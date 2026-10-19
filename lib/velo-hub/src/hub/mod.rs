// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The hub server: [`Hub`], its builder, startup, and shutdown.
//!
//! Startup, in order. A failure at any step undoes every earlier step in
//! reverse, and the store's removal hook is installed only once everything
//! before it succeeded, so an injected store stays reusable:
//!
//! ```text
//! 1. validate the bundle and extension graph and config     (no I/O)
//! 2. bind both listeners
//! 3. build the hub's velo instance, with the store as its discovery
//! 4. put the hub's own pinned entry
//! 5. attach every extension, in dependency order
//! 6. start every extension, in dependency order
//! 7. install the removal hook; reserve what the store already holds
//! 8. spawn the reaper, heartbeat, lifecycle task and both HTTP servers;
//!    open admission
//! ```
//!
//! Shutdown: close admission; take the registration lock (waits for the
//! registration in flight); stop extension background work; drain removal
//! delivery; shut extensions down in reverse order; stop the heartbeat and
//! reaper; drain both listeners; remove the pinned entry; shut velo down.

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
pub use routes::{Authorized, credential_from_headers};

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
    pub async fn serve(self) -> anyhow::Result<Hub> {
        let HubBuilder {
            config,
            store,
            velo,
            bundles,
            direct,
        } = self;
        let server = config.server.clone();

        // 1. Graph and config, before any I/O.
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
            if let Err(error) = extension.attach(&ctx).await {
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
            if let Err(error) = extension.start(&ctx).await {
                let error = error.context(format!("starting extension {:?}", extension.name()));
                return Err(unwind.run(error).await);
            }
        }

        // 7. The removal hook: irreversible, so last. Its queue exists
        //    before the hook so no removal between here and the lifecycle
        //    task's spawn is lost.
        let mut continuity = store.continuity();
        authority.set_continuity(*continuity.borrow_and_update());
        let (jobs, job_queue) = mpsc::unbounded_channel();
        match store.install_removal_hook(lifecycle::removal_hook(authority.clone(), jobs.clone())) {
            Ok(snapshot) => authority.synchronize_reservations(&snapshot),
            Err(error) => {
                return Err(unwind
                    .run(anyhow::Error::new(error).context("installing the store's removal hook"))
                    .await);
            }
        }

        // 8. Serve.
        let Unwind { velo, pinned, .. } = unwind;
        let shared = Arc::new(Shared {
            hub_id: services.hub_id,
            config: server.clone(),
            store: Arc::clone(&store),
            authority: authority.clone(),
            velo,
            by_name: attached,
            extensions,
            lock: tokio::sync::Mutex::new(()),
            admission: AtomicBool::new(false),
            jobs,
        });
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
            spawn_server(public, routes::public_router(&shared), http_cancel.clone()),
            spawn_server(
                control,
                routes::control_router(&shared),
                http_cancel.clone(),
            ),
        ];
        shared.admission.store(true, Ordering::Release);
        tracing::info!(%public_addr, %control_addr, hub_id = %shared.hub_id, "hub serving");

        Ok(Hub {
            inner: Arc::new(HubInner {
                shared,
                public_addr,
                control_addr,
                running: tokio::sync::Mutex::new(Some(Running {
                    extension_cancel,
                    core_cancel,
                    lifecycle_cancel,
                    http_cancel,
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
            extension.shutdown().await;
        }
        let mut error = error;
        if let Some((id, incarnation)) = self.pinned
            && let Err(cleanup) = remove_pinned(self.store.as_ref(), id, incarnation).await
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
    lifecycle: JoinHandle<(mpsc::UnboundedReceiver<Job>, Option<Job>)>,
    tasks: Vec<JoinHandle<()>>,
    servers: Vec<JoinHandle<()>>,
    pinned: Option<(InstanceId, Incarnation)>,
}

struct HubInner {
    shared: Arc<Shared>,
    public_addr: SocketAddr,
    control_addr: SocketAddr,
    running: tokio::sync::Mutex<Option<Running>>,
}

/// A running hub. Cheap to clone; every clone is the same hub.
///
/// Call [`shutdown`](Self::shutdown) to stop it in order. Dropping the last
/// clone without it cancels everything at once and removes the hub's own
/// entry in the background.
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

    /// Stop in order (see the module docs). A second call returns at once.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let Some(running) = self.inner.running.lock().await.take() else {
            return Ok(());
        };
        let shared = &self.inner.shared;
        shared.admission.store(false, Ordering::Release);
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
            extension.shutdown().await;
        }
        running.core_cancel.cancel();
        for task in running.tasks {
            let _ = task.await;
        }
        running.http_cancel.cancel();
        for server in running.servers {
            let _ = server.await;
        }
        drop(guard);
        let mut result = Ok(());
        if let Some((id, incarnation)) = running.pinned
            && let Err(error) = remove_pinned(shared.store.as_ref(), id, incarnation).await
        {
            result = Err(anyhow::Error::new(error).context("removing the hub's own entry"));
        }
        if let Some(velo) = &shared.velo {
            velo.shutdown(ShutdownPolicy::Timeout(shared.config.shutdown_timeout))
                .await;
        }
        result
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
        let Some(running) = self.running.get_mut().take() else {
            return;
        };
        self.shared.admission.store(false, Ordering::Release);
        running.extension_cancel.cancel();
        running.lifecycle_cancel.cancel();
        running.core_cancel.cancel();
        running.http_cancel.cancel();
        if let Some((id, incarnation)) = running.pinned
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let store = Arc::clone(&self.shared.store);
            runtime.spawn(async move {
                let _ = remove_pinned(store.as_ref(), id, incarnation).await;
            });
        }
    }
}
