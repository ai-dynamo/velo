// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The hub server: [`Hub`], its builder, startup, and shutdown.
//!
//! Startup, in order. A failure or panic at any step undoes every earlier step in
//! reverse, and the store's removal hook is installed only once everything
//! before it succeeded, so an injected store stays reusable:
//!
//! ```text
//! 1. validate the bundle and extension graph and config     (no I/O)
//! 2. bind both listeners
//! 3. build the hub's velo instance, with the store as its discovery
//! 4. put the hub's own pinned entry
//! 5. attach every extension, in dependency order
//! 6. start every extension, in dependency order; build both routers
//!    (extension route code, which can panic)
//! 7. install the removal hook; reserve what the store already holds
//! 8. spawn the reaper, heartbeat, lifecycle task and both HTTP servers;
//!    open admission
//! ```
//!
//! Shutdown runs as its own task, so a caller that stops waiting cannot
//! leave the hub half stopped: close admission; take the registration lock
//! (waits for the registration in flight); stop extension background work;
//! drain removal delivery; shut extensions down in reverse order; stop the
//! heartbeat and reaper; release the lock; drain both listeners, for at
//! most `shutdown_timeout_ms`; remove the pinned entry; shut velo down.

mod authority;
mod config;
mod context;
mod error;
mod extension;
mod graph;
mod heartbeat;
mod lifecycle;
mod registration;
mod routes;

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context as _;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerDiscovery, ShutdownPolicy, Velo, VeloBuilder};

pub use authority::{Authority, AuthorityError, Lease};
pub use config::{
    DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_MAX_FAILURES, DEFAULT_LOCK_TIMEOUT,
    DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_STEP_TIMEOUT, HubConfig, ServerConfig,
};
pub use context::{AttachContext, StartContext};
pub use error::HubError;
pub use extension::{Committed, Extension, RegistrationView, Rejection, Txn};
pub use graph::{Bundle, BundleInstaller, GraphError};
pub use routes::{Authorized, HubJson, HubPath, credential_from_headers};

use context::{Attached, Services};
use extension::DynExtension;
use graph::{HUB_OWNER, PendingBundle, PendingExtension};
use lifecycle::Job;

use crate::protocol::HubId;
use crate::store::{HubStore, InMemoryStore, Incarnation, RegistrationRecord, StoreError};

/// What every route and hub task shares.
pub(crate) struct Shared {
    pub(crate) hub_id: HubId,
    pub(crate) config: ServerConfig,
    pub(crate) store: Arc<dyn HubStore>,
    pub(crate) authority: Authority,
    pub(crate) velo: Option<Velo>,
    /// In start order.
    pub(crate) extensions: Vec<Arc<dyn DynExtension>>,
    pub(crate) by_name: HashMap<&'static str, Arc<dyn Any + Send + Sync>>,
    /// The hub-wide registration lock. Every extension mutation
    /// (`commit`, `on_registered`, `on_removed`) runs under it.
    pub(crate) lock: tokio::sync::Mutex<()>,
    admission: AtomicBool,
    pub(crate) jobs: mpsc::UnboundedSender<Job>,
}

impl Shared {
    pub(crate) fn admitting(&self) -> bool {
        self.admission.load(Ordering::Acquire)
    }

    /// A store remove under `step_timeout_ms`. Every remove that runs under
    /// the registration lock goes through this, so a backend that never
    /// answers cannot hold the lock. Expiry reads as `Indeterminate`: a
    /// remote remove can still apply after its future is dropped.
    pub(crate) async fn remove_bounded(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> Result<(), StoreError> {
        match tokio::time::timeout(self.config.step_timeout, self.store.remove(id, incarnation))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(StoreError::Indeterminate(format!(
                "remove did not complete within {:?}",
                self.config.step_timeout
            ))),
        }
    }
}

/// Builds a [`Hub`]. See the module docs for what [`serve`](Self::serve) does.
pub struct HubBuilder {
    config: HubConfig,
    store: Option<Arc<dyn HubStore>>,
    velo: Option<VeloBuilder>,
    bundles: Vec<PendingBundle>,
    direct: Vec<PendingExtension>,
}

impl HubBuilder {
    /// The store. Default: an [`InMemoryStore`] with the config's TTL.
    pub fn store(mut self, store: Arc<dyn HubStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Run a velo instance built from `builder`, with the store as its
    /// discovery. Without one the hub is discovery-only: it does not probe
    /// registrants, and liveness is the store's TTL alone.
    pub fn velo(mut self, builder: VeloBuilder) -> Self {
        self.velo = Some(builder);
        self
    }

    /// Add a bundle. Each bundle is added once; bundles that require it
    /// share it.
    pub fn bundle<B: Bundle>(mut self, bundle: B) -> Self {
        self.bundles.push(PendingBundle::new(bundle));
        self
    }

    /// Add one extension directly, without a bundle.
    pub fn extension<E: Extension>(mut self, extension: E) -> Self {
        self.direct
            .push(PendingExtension::new(Arc::new(extension), HUB_OWNER));
        self
    }

    /// Validate, bind, start, and serve. See the module docs.
    ///
    /// Startup runs as its own task. If this future is dropped part-way,
    /// startup still finishes, and the hub it built stops in order.
    pub async fn serve(self) -> anyhow::Result<Hub> {
        match tokio::spawn(self.serve_in_task()).await {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!("hub startup task failed: {error}")),
        }
    }

    async fn serve_in_task(self) -> anyhow::Result<Hub> {
        let HubBuilder {
            config,
            store,
            velo,
            bundles,
            direct,
        } = self;
        let server = config.server.clone();

        // 1. Graph and config, before any I/O.
        server.validate()?;
        let extensions = graph::plan(bundles, direct, &config.bundles)?;

        // 2. Listeners.
        let public = TcpListener::bind((server.bind_addr, server.public_port))
            .await
            .with_context(|| {
                format!(
                    "binding the public listener {}:{}",
                    server.bind_addr, server.public_port
                )
            })?;
        let control = TcpListener::bind((server.bind_addr, server.control_port))
            .await
            .with_context(|| {
                format!(
                    "binding the control listener {}:{}",
                    server.bind_addr, server.control_port
                )
            })?;
        let public_addr = public.local_addr().context("public listener address")?;
        let control_addr = control.local_addr().context("control listener address")?;

        let store: Arc<dyn HubStore> = store.unwrap_or_else(|| {
            Arc::new(
                InMemoryStore::builder()
                    .ttl(server.ttl)
                    .prune_interval(server.prune_interval)
                    .build(),
            )
        });
        let extension_cancel = CancellationToken::new();
        let mut unwind = Unwind {
            store: Arc::clone(&store),
            velo: None,
            pinned: None,
            attached: Vec::new(),
            cancel: extension_cancel.clone(),
            shutdown_timeout: server.shutdown_timeout,
        };

        // 3. Velo.
        if let Some(builder) = velo {
            let discovery: Arc<dyn PeerDiscovery> = Arc::clone(&store) as Arc<dyn PeerDiscovery>;
            match builder.discovery(discovery).build().await {
                Ok(velo) => unwind.velo = Some(velo),
                Err(error) => {
                    return Err(unwind
                        .run(error.context("building the hub's velo instance"))
                        .await);
                }
            }
        }

        // 4. The hub's own entry, so clients can find it over discovery.
        if let Some(velo) = &unwind.velo {
            let mut record = RegistrationRecord::bare(velo.peer_info());
            record.pinned = true;
            match store.put(record, None).await {
                Ok(incarnation) => unwind.pinned = Some((velo.instance_id(), incarnation)),
                Err(error) => {
                    return Err(unwind
                        .run(anyhow::Error::new(error).context("putting the hub's own entry"))
                        .await);
                }
            }
        }

        let authority = Authority::default();
        let services = Services {
            hub_id: HubId::new(),
            velo: unwind.velo.clone(),
            store: Arc::clone(&store),
            authority: authority.clone(),
            cancel: extension_cancel.clone(),
        };

        // 5. Attach, in dependency order.
        let mut attached: Attached = HashMap::new();
        for extension in &extensions {
            let ctx = AttachContext {
                name: extension.name(),
                requires: extension.requires(),
                attached: &attached,
                services: &services,
            };
            if let Err(error) = caught(extension.attach(&ctx)).await {
                let error = error.context(format!("attaching extension {:?}", extension.name()));
                return Err(unwind.run(error).await);
            }
            unwind.attached.push(Arc::clone(extension));
            attached.insert(extension.name(), extension.as_any());
        }

        // 6. Start, in dependency order.
        for extension in &extensions {
            let ctx = StartContext {
                attach: AttachContext {
                    name: extension.name(),
                    requires: extension.requires(),
                    attached: &attached,
                    services: &services,
                },
                public_addr,
                control_addr,
            };
            if let Err(error) = caught(extension.start(&ctx)).await {
                let error = error.context(format!("starting extension {:?}", extension.name()));
                return Err(unwind.run(error).await);
            }
        }

        // The shared state and both routers, before the hook: building a
        // router runs extension code (`public_routes`, `control_routes`)
        // that can panic, and nothing after the hook may fail.
        let (jobs, job_queue) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            hub_id: services.hub_id,
            config: server.clone(),
            store: Arc::clone(&store),
            authority: authority.clone(),
            velo: unwind.velo.clone(),
            by_name: attached,
            extensions,
            lock: tokio::sync::Mutex::new(()),
            admission: AtomicBool::new(false),
            jobs: jobs.clone(),
        });
        let request_cancel = CancellationToken::new();
        let routers = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (
                routes::public_router(&shared, &request_cancel),
                routes::control_router(&shared, &request_cancel),
            )
        }));
        let (public_routes, control_routes) = match routers {
            Ok(routers) => routers,
            Err(panic) => {
                let error =
                    anyhow::anyhow!("building the routes panicked: {}", panic_message(&panic));
                return Err(unwind.run(error).await);
            }
        };

        // 7. The removal hook: irreversible, so last. Its queue exists
        //    before the hook so no removal between here and the lifecycle
        //    task's spawn is lost.
        let mut continuity = store.continuity();
        authority.set_continuity(*continuity.borrow_and_update());
        match store.install_removal_hook(lifecycle::removal_hook(authority.clone(), jobs)) {
            Ok(snapshot) => authority.synchronize_reservations(&snapshot),
            Err(error) => {
                return Err(unwind
                    .run(anyhow::Error::new(error).context("installing the store's removal hook"))
                    .await);
            }
        }

        // 8. Serve.
        let Unwind { pinned, .. } = unwind;
        let core_cancel = CancellationToken::new();
        let lifecycle_cancel = CancellationToken::new();
        let http_cancel = CancellationToken::new();
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn({
            let authority = authority.clone();
            let cancel = core_cancel.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        changed = continuity.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            authority.set_continuity(*continuity.borrow_and_update());
                        }
                    }
                }
            }
        }));
        if let Some(reaper) = Arc::clone(&store).spawn_liveness_task(core_cancel.clone()) {
            tasks.push(reaper);
        }
        match &shared.velo {
            Some(velo) => tasks.push(heartbeat::spawn(
                velo.downgrade(),
                Arc::clone(&store),
                server.heartbeat_interval,
                server.heartbeat_max_failures,
                server.step_timeout,
                core_cancel.clone(),
            )),
            None => tracing::info!(
                "no velo instance: the hub does not probe registrants; liveness is the store's TTL alone"
            ),
        }
        let lifecycle = tokio::spawn(lifecycle::run(
            Arc::clone(&shared),
            job_queue,
            lifecycle_cancel.clone(),
        ));
        let servers = vec![
            spawn_server(public, public_routes, http_cancel.clone()),
            spawn_server(control, control_routes, http_cancel.clone()),
        ];
        shared.admission.store(true, Ordering::Release);
        tracing::info!(%public_addr, %control_addr, hub_id = %shared.hub_id, "hub serving");

        Ok(Hub {
            inner: Arc::new(HubInner {
                runtime: tokio::runtime::Handle::current(),
                shared,
                public_addr,
                control_addr,
                stage: tokio::sync::Mutex::new(Stage::Running(Running {
                    extension_cancel,
                    core_cancel,
                    lifecycle_cancel,
                    http_cancel,
                    request_cancel,
                    lifecycle,
                    tasks,
                    servers,
                    pinned,
                })),
            }),
        })
    }
}

/// Undoes a partial startup, in reverse.
struct Unwind {
    store: Arc<dyn HubStore>,
    velo: Option<Velo>,
    pinned: Option<(InstanceId, Incarnation)>,
    attached: Vec<Arc<dyn DynExtension>>,
    cancel: CancellationToken,
    shutdown_timeout: std::time::Duration,
}

impl Unwind {
    async fn run(self, error: anyhow::Error) -> anyhow::Error {
        self.cancel.cancel();
        for extension in self.attached.iter().rev() {
            shutdown_extension(extension.as_ref(), self.shutdown_timeout).await;
        }
        let mut error = error;
        if let Some((id, incarnation)) = self.pinned
            && let Err(cleanup) = match tokio::time::timeout(
                self.shutdown_timeout,
                remove_pinned(self.store.as_ref(), id, incarnation),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(StoreError::Indeterminate(format!(
                    "did not complete within {:?}",
                    self.shutdown_timeout
                ))),
            }
        {
            error = error.context(format!(
                "and removing the hub's own entry failed: {cleanup}"
            ));
        }
        if let Some(velo) = self.velo {
            velo.shutdown(ShutdownPolicy::Timeout(self.shutdown_timeout))
                .await;
        }
        error
    }
}

async fn remove_pinned(
    store: &dyn HubStore,
    id: InstanceId,
    incarnation: Incarnation,
) -> Result<(), StoreError> {
    match store.remove(id, incarnation).await {
        Ok(()) | Err(StoreError::NotFound(_) | StoreError::Stale { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Run `future`, turning a panic into an error, so a panicking extension
/// hook takes the same unwind path as one that returned an error.
async fn caught<T>(
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    use futures::FutureExt;
    match std::panic::AssertUnwindSafe(future).catch_unwind().await {
        Ok(result) => result,
        Err(panic) => Err(anyhow::anyhow!("panicked: {}", panic_message(&panic))),
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a non-text panic".to_string())
}

/// One extension's `shutdown`, under `within` and with its panic caught: a
/// misbehaving extension must not stop the rest of the shutdown.
async fn shutdown_extension(extension: &dyn DynExtension, within: std::time::Duration) {
    use futures::FutureExt;
    match tokio::time::timeout(
        within,
        std::panic::AssertUnwindSafe(extension.shutdown()).catch_unwind(),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => tracing::error!(
            extension = extension.name(),
            "shutdown panicked: {}",
            panic_message(&panic)
        ),
        Err(_) => tracing::error!(
            extension = extension.name(),
            "shutdown did not finish within {within:?}"
        ),
    }
}

/// Await each handle until `within` passes; true if all finished. A handle
/// that finished is taken out, so a second call does not poll it again.
async fn join_within(handles: &mut [Option<JoinHandle<()>>], within: std::time::Duration) -> bool {
    tokio::time::timeout(within, async {
        for slot in handles.iter_mut() {
            if let Some(handle) = slot.as_mut() {
                let _ = handle.await;
                *slot = None;
            }
        }
    })
    .await
    .is_ok()
}

fn spawn_server(
    listener: TcpListener,
    router: axum::Router,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let serve = axum::serve(listener, router)
            .with_graceful_shutdown(async move { cancel.cancelled().await });
        if let Err(error) = serve.await {
            tracing::error!(%error, "hub listener failed");
        }
    })
}

struct Running {
    extension_cancel: CancellationToken,
    core_cancel: CancellationToken,
    lifecycle_cancel: CancellationToken,
    http_cancel: CancellationToken,
    /// Ends the requests still in flight when the listener drain times out.
    request_cancel: CancellationToken,
    lifecycle: JoinHandle<(mpsc::UnboundedReceiver<Job>, Option<Job>)>,
    tasks: Vec<JoinHandle<()>>,
    servers: Vec<JoinHandle<()>>,
    pinned: Option<(InstanceId, Incarnation)>,
}

/// Where the hub is in its life.
enum Stage {
    Running(Running),
    /// A stop task runs or ran; its result arrives here.
    Stopping(tokio::sync::watch::Receiver<Option<Result<(), String>>>),
}

struct HubInner {
    shared: Arc<Shared>,
    /// The runtime that served the hub, so a drop on any thread can still
    /// run the ordered stop there.
    runtime: tokio::runtime::Handle,
    public_addr: SocketAddr,
    control_addr: SocketAddr,
    stage: tokio::sync::Mutex<Stage>,
}

impl HubInner {
    /// Start the stop task if none started yet, and return where its
    /// result arrives.
    fn begin_stop(
        stage: &mut Stage,
        shared: &Arc<Shared>,
        runtime: &tokio::runtime::Handle,
    ) -> tokio::sync::watch::Receiver<Option<Result<(), String>>> {
        if let Stage::Stopping(result) = stage {
            return result.clone();
        }
        let (done, result) = tokio::sync::watch::channel(None);
        let Stage::Running(running) = std::mem::replace(stage, Stage::Stopping(result.clone()))
        else {
            unreachable!("the stage was Running");
        };
        // Admission closes before the task is even scheduled.
        shared.admission.store(false, Ordering::Release);
        let shared = Arc::clone(shared);
        runtime.spawn(async move {
            let outcome = stop(&shared, running)
                .await
                .map_err(|error| format!("{error:#}"));
            let _ = done.send(Some(outcome));
        });
        result
    }
}

/// The ordered stop. Runs as its own task, so a caller that stops waiting
/// cannot leave the hub half stopped.
async fn stop(shared: &Shared, running: Running) -> anyhow::Result<()> {
    let config = &shared.config;
    let guard = shared.lock.lock().await;
    running.extension_cancel.cancel();
    running.lifecycle_cancel.cancel();
    if let Ok((mut queue, in_flight)) = running.lifecycle.await {
        if let Some(job) = in_flight {
            shared.run_job(job).await;
        }
        while let Ok(job) = queue.try_recv() {
            shared.run_job(job).await;
        }
    }
    for extension in shared.extensions.iter().rev() {
        shutdown_extension(extension.as_ref(), config.shutdown_timeout).await;
    }
    running.core_cancel.cancel();
    // Each core task stops at its next await once cancelled; the bound only
    // guards against a backend call that ignores its own deadline.
    for task in running.tasks {
        let abort = task.abort_handle();
        if tokio::time::timeout(config.shutdown_timeout, task)
            .await
            .is_err()
        {
            tracing::warn!(
                "a hub task did not stop within {:?}; aborting it",
                config.shutdown_timeout
            );
            abort.abort();
        }
    }
    // Release the lock before draining the listeners: a request queued on
    // it would otherwise hold the drain for its whole lock timeout. Admission
    // is closed, so it answers 503 as soon as it gets the lock.
    drop(guard);
    running.http_cancel.cancel();
    let mut servers: Vec<Option<JoinHandle<()>>> = running.servers.into_iter().map(Some).collect();
    if !join_within(&mut servers, config.shutdown_timeout).await {
        tracing::warn!(
            "listeners did not drain within {:?}; ending the requests still in flight",
            config.shutdown_timeout
        );
        // Each in-flight request answers 503 and its handler is dropped;
        // then the connections close and the servers finish.
        running.request_cancel.cancel();
        if !join_within(&mut servers, config.shutdown_timeout).await {
            for server in servers.iter().flatten() {
                server.abort();
            }
        }
    }
    let mut result = Ok(());
    if let Some((id, incarnation)) = running.pinned
        && let Err(error) = shared.remove_bounded(id, incarnation).await
        && !matches!(error, StoreError::NotFound(_) | StoreError::Stale { .. })
    {
        result = Err(anyhow::Error::new(error).context("removing the hub's own entry"));
    }
    if let Some(velo) = &shared.velo {
        velo.shutdown(ShutdownPolicy::Timeout(config.shutdown_timeout))
            .await;
    }
    result
}

/// A running hub. Cheap to clone; every clone is the same hub.
///
/// Call [`shutdown`](Self::shutdown) to stop it in order. Dropping the last
/// clone without it runs the same stop in the background, on the runtime
/// that served the hub.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("hub_id", &self.inner.shared.hub_id)
            .field("public_addr", &self.inner.public_addr)
            .field("control_addr", &self.inner.control_addr)
            .finish()
    }
}

impl Hub {
    /// A builder for a hub with `config`.
    pub fn builder(config: HubConfig) -> HubBuilder {
        HubBuilder {
            config,
            store: None,
            velo: None,
            bundles: Vec::new(),
            direct: Vec::new(),
        }
    }

    /// This hub process.
    pub fn hub_id(&self) -> HubId {
        self.inner.shared.hub_id
    }

    /// The public listener's bound address.
    pub fn public_addr(&self) -> SocketAddr {
        self.inner.public_addr
    }

    /// The control listener's bound address.
    pub fn control_addr(&self) -> SocketAddr {
        self.inner.control_addr
    }

    /// The hub's velo instance, if it runs one.
    pub fn velo(&self) -> Option<&Velo> {
        self.inner.shared.velo.as_ref()
    }

    /// The hub's store.
    pub fn store(&self) -> &Arc<dyn HubStore> {
        &self.inner.shared.store
    }

    /// The hub's credential authority.
    pub fn authority(&self) -> &Authority {
        &self.inner.shared.authority
    }

    /// A hosted extension, by type.
    pub fn extension<T: Extension>(&self) -> Option<Arc<T>> {
        let any = self.inner.shared.by_name.get(T::NAME)?;
        Arc::clone(any).downcast::<T>().ok()
    }

    /// Stop in order (see the module docs). A second call, also one made
    /// while the first runs, waits for the same stop and returns its result.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let mut result = {
            let mut stage = self.inner.stage.lock().await;
            HubInner::begin_stop(
                &mut stage,
                &self.inner.shared,
                &tokio::runtime::Handle::current(),
            )
        };
        let outcome = result
            .wait_for(Option::is_some)
            .await
            .map_err(|_| anyhow::anyhow!("the hub's stop task ended without a result"))?
            .clone()
            .expect("wait_for returned a result");
        outcome.map_err(|error| anyhow::anyhow!(error))
    }

    /// Serve until Ctrl-C, then [`shutdown`](Self::shutdown).
    pub async fn run_until_signal(self) -> anyhow::Result<()> {
        tokio::signal::ctrl_c()
            .await
            .context("waiting for Ctrl-C")?;
        self.shutdown().await
    }
}

impl Drop for HubInner {
    fn drop(&mut self) {
        let Stage::Running(_) = self.stage.get_mut() else {
            return;
        };
        // The same ordered stop as `shutdown`, in the background, on the
        // runtime that served the hub, whichever thread drops it. If that
        // runtime has shut down, the spawn does nothing, and its tasks,
        // which this would stop, are gone already.
        let runtime = self.runtime.clone();
        let _ = HubInner::begin_stop(self.stage.get_mut(), &self.shared, &runtime);
    }
}
