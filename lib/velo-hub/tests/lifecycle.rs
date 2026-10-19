// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Startup, shutdown, and the extension graph.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use parking_lot::Mutex;
use velo_hub::axum::Router;
use velo_hub::axum::routing::get;
use velo_hub::protocol::{HubDescriptor, paths};
use velo_hub::store::testing::FaultStore;
use velo_hub::store::{HubStore, Removal};
#[allow(unused_imports)]
use velo_hub::velo::PeerDiscovery;
use velo_hub::{
    AttachContext, Bundle, BundleInstaller, Committed, Extension, GraphError, Hub, Rejection,
    StartContext, Txn,
};

type Log = Arc<Mutex<Vec<String>>>;

/// An extension that records its lifecycle calls. `$requires` drives
/// order; `fail_start` makes `start` fail.
macro_rules! recorder {
    ($ty:ident, $name:literal, [$($requires:literal),*]) => {
        struct $ty {
            log: Log,
            fail_start: bool,
        }

        impl $ty {
            fn new(log: &Log) -> Self {
                Self { log: Arc::clone(log), fail_start: false }
            }

            #[allow(dead_code)]
            fn failing(log: &Log) -> Self {
                Self { log: Arc::clone(log), fail_start: true }
            }

            fn record(&self, event: &str) {
                self.log.lock().push(format!("{} {}", event, $name));
            }
        }

        impl Extension for $ty {
            const NAME: &'static str = $name;
            type Registration = ();
            type Staged = ();

            fn requires() -> &'static [&'static str] {
                &[$($requires),*]
            }

            async fn attach(&self, _ctx: &AttachContext<'_>) -> anyhow::Result<()> {
                self.record("attach");
                Ok(())
            }

            async fn start(&self, _ctx: &StartContext<'_>) -> anyhow::Result<()> {
                self.record("start");
                if self.fail_start {
                    anyhow::bail!("injected start failure");
                }
                Ok(())
            }

            async fn shutdown(&self) {
                self.record("shutdown");
            }

            fn descriptor(&self) -> serde_json::Value {
                serde_json::json!({ "name": $name })
            }

            async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
                self.record("prepare");
                Ok(())
            }

            fn commit(&self, _staged: (), _registration: &Committed<'_>) {
                self.record("commit");
            }

            fn on_removed(&self, _removal: Removal) {
                self.record("removed");
            }
        }
    };
}

recorder!(A, "a", []);
recorder!(B, "b", ["a"]);
recorder!(C, "c", ["b"]);
recorder!(X, "x", ["y"]);
recorder!(Y, "y", ["x"]);
recorder!(Lonely, "lonely", ["nope"]);

fn events(log: &Log, kind: &str) -> Vec<String> {
    log.lock()
        .iter()
        .filter(|event| event.starts_with(kind))
        .map(|event| event[kind.len() + 1..].to_string())
        .collect()
}

#[tokio::test]
async fn attach_and_start_follow_dependencies_and_shutdown_reverses() {
    let log = Log::default();
    let hub = builder()
        .extension(C::new(&log))
        .extension(B::new(&log))
        .extension(A::new(&log))
        .serve()
        .await
        .unwrap();
    assert_eq!(events(&log, "attach"), ["a", "b", "c"]);
    assert_eq!(events(&log, "start"), ["a", "b", "c"]);

    let peer = make_peer();
    let credential = register_plain(&hub, &peer, None).await.credential;
    assert_eq!(
        events(&log, "prepare"),
        ["a", "b", "c"],
        "every extension sees every registration, in start order"
    );
    assert_eq!(events(&log, "commit"), ["a", "b", "c"]);
    assert_eq!(
        unregister_status(&hub, peer.instance_id(), Some(&credential)).await,
        204
    );
    assert_eq!(
        &events(&log, "removed")[..3],
        ["c", "b", "a"],
        "removals go in reverse"
    );

    let described: HubDescriptor = http()
        .get(public(&hub, paths::HUB))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = described
        .extensions
        .iter()
        .map(|extension| extension.name.as_str())
        .collect();
    assert_eq!(names, ["a", "b", "c"]);
    assert_eq!(described.extensions[1].requires, ["a"]);
    assert_eq!(described.extensions[2].descriptor["name"], "c");
    assert_eq!(described.hub_id, hub.hub_id());

    hub.shutdown().await.unwrap();
    assert_eq!(events(&log, "shutdown"), ["c", "b", "a"]);
}

/// Each case would also fail to bind (the public port is taken). Getting a
/// `GraphError` instead proves the graph is checked before any socket.
#[tokio::test]
async fn bad_graphs_are_rejected_before_binding() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_config = || {
        let mut config = config();
        config.server.public_port = occupied.local_addr().unwrap().port();
        config
    };
    let log = Log::default();

    let cycle = Hub::builder(busy_config())
        .extension(X::new(&log))
        .extension(Y::new(&log))
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(cycle.downcast_ref::<GraphError>(), Some(GraphError::ExtensionCycle(names)) if names == &["x", "y"]),
        "{cycle}"
    );

    let missing = Hub::builder(busy_config())
        .extension(Lonely::new(&log))
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(
            missing.downcast_ref::<GraphError>(),
            Some(GraphError::MissingExtension {
                requires: "nope",
                ..
            })
        ),
        "{missing}"
    );

    let duplicate = Hub::builder(busy_config())
        .extension(A::new(&log))
        .extension(A::new(&log))
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(
            duplicate.downcast_ref::<GraphError>(),
            Some(GraphError::DuplicateExtension { name: "a", .. })
        ),
        "{duplicate}"
    );

    let mut unclaimed = busy_config();
    unclaimed
        .bundles
        .insert("ghost".to_string(), serde_json::json!({}));
    let unclaimed = Hub::builder(unclaimed).serve().await.unwrap_err();
    assert!(
        matches!(unclaimed.downcast_ref::<GraphError>(), Some(GraphError::UnclaimedConfig(section)) if section == "ghost"),
        "{unclaimed}"
    );

    let invalid = Hub::builder(busy_config())
        .extension(BadName)
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(
            invalid.downcast_ref::<GraphError>(),
            Some(GraphError::InvalidName(_))
        ),
        "{invalid}"
    );

    assert!(log.lock().is_empty(), "nothing attached");
}

struct BadName;

impl Extension for BadName {
    const NAME: &'static str = "../v1";
    type Registration = ();
    type Staged = ();

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

/// An extension that tries to claim a core path gets it under its own
/// prefix, and the core route still answers.
struct Shadow;

impl Extension for Shadow {
    const NAME: &'static str = "shadow";
    type Registration = ();
    type Staged = ();

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new().route("/v1/health", get(|| async { "shadow" }))
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

#[tokio::test]
async fn an_extension_cannot_shadow_core_routes() {
    let hub = builder().extension(Shadow).serve().await.unwrap();
    let core = http()
        .get(public(&hub, paths::HEALTH))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(core, "ok");
    let own = http()
        .get(public(&hub, "/v1/ext/shadow/v1/health"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(own, "shadow");
}

// ---- bundles -------------------------------------------------------------

#[derive(Default)]
struct Base {
    depth: Mutex<u32>,
}

impl Extension for Base {
    const NAME: &'static str = "base";
    type Registration = ();
    type Staged = ();

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

/// Holds what it found when it looked `base` up at attach.
macro_rules! dependent {
    ($ty:ident, $name:literal) => {
        #[derive(Default)]
        struct $ty {
            base: Mutex<Option<Arc<Base>>>,
        }

        impl Extension for $ty {
            const NAME: &'static str = $name;
            type Registration = ();
            type Staged = ();

            fn requires() -> &'static [&'static str] {
                &["base"]
            }

            async fn attach(&self, ctx: &AttachContext<'_>) -> anyhow::Result<()> {
                *self.base.lock() = Some(ctx.extension::<Base>()?);
                Ok(())
            }

            async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
                Ok(())
            }

            fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

            fn on_removed(&self, _removal: Removal) {}
        }
    };
}

dependent!(Left, "left");
dependent!(Right, "right");

#[derive(serde::Deserialize)]
struct BaseConfig {
    depth: u32,
}

struct BaseBundle;

impl Bundle for BaseBundle {
    const NAME: &'static str = "base-bundle";

    fn install(self, installer: &mut BundleInstaller<'_>) -> anyhow::Result<()> {
        let base = Base::default();
        if let Some(config) = installer.config::<BaseConfig>()? {
            *base.depth.lock() = config.depth;
        }
        installer.extension(base);
        Ok(())
    }
}

struct LeftBundle;

impl Bundle for LeftBundle {
    const NAME: &'static str = "left-bundle";

    fn requires() -> &'static [&'static str] {
        &["base-bundle"]
    }

    fn install(self, installer: &mut BundleInstaller<'_>) -> anyhow::Result<()> {
        installer.extension(Left::default());
        Ok(())
    }
}

struct RightBundle;

impl Bundle for RightBundle {
    const NAME: &'static str = "right-bundle";

    fn requires() -> &'static [&'static str] {
        &["base-bundle"]
    }

    fn install(self, installer: &mut BundleInstaller<'_>) -> anyhow::Result<()> {
        installer.extension(Right::default());
        Ok(())
    }
}

/// rhino and dynamo both require kvbm: the program adds kvbm once and
/// both resolve to the same instance, configured from its own section.
#[tokio::test]
async fn bundles_that_share_a_requirement_share_one_instance() {
    let mut config = config();
    config
        .bundles
        .insert("base-bundle".to_string(), serde_json::json!({ "depth": 3 }));
    let hub = Hub::builder(config)
        .bundle(RightBundle)
        .bundle(LeftBundle)
        .bundle(BaseBundle)
        .serve()
        .await
        .unwrap();
    let base = hub.extension::<Base>().unwrap();
    let left = hub
        .extension::<Left>()
        .unwrap()
        .base
        .lock()
        .clone()
        .unwrap();
    let right = hub
        .extension::<Right>()
        .unwrap()
        .base
        .lock()
        .clone()
        .unwrap();
    assert!(Arc::ptr_eq(&base, &left));
    assert!(Arc::ptr_eq(&base, &right));
    assert_eq!(
        *base.depth.lock(),
        3,
        "the bundle read its own config section"
    );
}

#[tokio::test]
async fn bundle_graph_errors() {
    let missing = builder().bundle(LeftBundle).serve().await.unwrap_err();
    assert!(
        matches!(
            missing.downcast_ref::<GraphError>(),
            Some(GraphError::MissingBundle {
                requires: "base-bundle",
                ..
            })
        ),
        "{missing}"
    );
    let duplicate = builder()
        .bundle(BaseBundle)
        .bundle(BaseBundle)
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(
            duplicate.downcast_ref::<GraphError>(),
            Some(GraphError::DuplicateBundle("base-bundle"))
        ),
        "{duplicate}"
    );
    let mut bad_config = config();
    bad_config.bundles.insert(
        "base-bundle".to_string(),
        serde_json::json!({ "depth": "deep" }),
    );
    let invalid = Hub::builder(bad_config)
        .bundle(BaseBundle)
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(
            invalid.downcast_ref::<GraphError>(),
            Some(GraphError::Install {
                bundle: "base-bundle",
                ..
            })
        ),
        "{invalid}"
    );
}

/// Looks up `base` without declaring it.
#[derive(Default)]
struct Sneaky;

impl Extension for Sneaky {
    const NAME: &'static str = "sneaky";
    type Registration = ();
    type Staged = ();

    async fn attach(&self, ctx: &AttachContext<'_>) -> anyhow::Result<()> {
        ctx.extension::<Base>().map(|_| ())
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

#[tokio::test]
async fn undeclared_extension_lookup_fails() {
    let error = builder()
        .extension(Base::default())
        .extension(Sneaky)
        .serve()
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("without declaring it in requires()"),
        "{error:#}"
    );
}

// ---- startup failure and shutdown ----------------------------------------

#[tokio::test]
async fn startup_failure_after_start_unwinds_and_leaves_the_store_reusable() {
    let store = Arc::new(FaultStore::default());
    let log = Log::default();
    let failed = builder()
        .store(store.clone())
        .velo(velo_builder())
        .extension(A::new(&log))
        .extension(B::failing(&log))
        .serve()
        .await;
    assert!(failed.is_err());
    assert_eq!(events(&log, "start"), ["a", "b"]);
    assert_eq!(
        events(&log, "shutdown"),
        ["b", "a"],
        "every attached extension shuts down, in reverse"
    );
    assert!(store.list().is_empty(), "the pinned entry is removed");

    let hub = builder()
        .store(store.clone())
        .serve()
        .await
        .expect("the store's hook was not consumed");
    let peer = make_peer();
    register_plain(&hub, &peer, None).await;
    store.expire(peer.instance_id());
    assert!(
        eventually(Duration::from_secs(1), || hub
            .authority()
            .authorize(peer.instance_id(), None)
            .is_err_and(|error| matches!(
                error,
                velo_hub::AuthorityError::NotFound { .. }
            )))
        .await,
        "the second hub's hook hears removals"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_an_in_flight_registration() {
    let hub = builder()
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let gate = ledger.gate_next_prepare();
    let peer = make_peer();
    let registration = tokio::spawn({
        let hub = hub.clone();
        let body = request(&peer);
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;

    let shutdown = tokio::spawn({
        let hub = hub.clone();
        async move { hub.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown must wait for the registration holding the lock"
    );
    gate.release();
    assert_eq!(
        registration.await.unwrap(),
        200,
        "a registration admitted before shutdown finishes"
    );
    shutdown.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_frees_ports_and_shuts_velo_down() {
    let velo_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let velo_addr = velo_listener.local_addr().unwrap();
    let transport = velo_hub::velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(velo_listener)
        .unwrap()
        .build()
        .unwrap();
    let hub = builder()
        .velo(velo_hub::velo::Velo::builder().add_transport(Arc::new(transport)))
        .serve()
        .await
        .unwrap();
    let (public_addr, control_addr) = (hub.public_addr(), hub.control_addr());
    hub.shutdown().await.unwrap();
    hub.shutdown().await.unwrap();

    for addr in [public_addr, control_addr, velo_addr] {
        std::net::TcpListener::bind(addr)
            .unwrap_or_else(|error| panic!("{addr} still bound after shutdown: {error}"));
    }
    assert!(hub.store().list().is_empty(), "the pinned entry is gone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registrations_are_refused_once_shutdown_closes_admission() {
    let hub = start().await;
    let control_url = control(&hub, paths::INSTANCES);
    hub.shutdown().await.unwrap();
    let after = http()
        .post(control_url)
        .json(&request(&make_peer()))
        .send()
        .await;
    assert!(after.is_err(), "the listener is closed");
}

// ---- shutdown under load ------------------------------------------------

/// A DELETE queued on the registration lock behind shutdown must not hold
/// shutdown for its lock timeout: shutdown releases the lock before it
/// drains the listeners, and the DELETE answers 503 at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_does_not_wait_out_the_lock_timeout_of_queued_requests() {
    let mut config = config();
    config.server.lock_timeout = Duration::from_secs(5);
    let hub = Hub::builder(config)
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let victim = make_peer();
    let victim_credential = register_plain(&hub, &victim, None).await.credential;
    let gate = ledger.gate_next_prepare();
    let held = tokio::spawn({
        let hub = hub.clone();
        let body = request(&make_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;
    let started = std::time::Instant::now();
    let shutdown = tokio::spawn({
        let hub = hub.clone();
        async move { hub.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let delete = tokio::spawn({
        let hub = hub.clone();
        async move { unregister_status(&hub, victim.instance_id(), Some(&victim_credential)).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let late_registration = tokio::spawn({
        let hub = hub.clone();
        let body = request(&make_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();
    assert_eq!(held.await.unwrap(), 200);
    shutdown.await.unwrap().unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shutdown took {:?}",
        started.elapsed()
    );
    assert_eq!(delete.await.unwrap(), 503);
    assert_eq!(
        late_registration.await.unwrap(),
        503,
        "admission closed when shutdown began"
    );
}

/// A request that never finishes (a probe to a peer that never answers)
/// cannot hold shutdown forever: the listener drain is bounded by
/// `shutdown_timeout_ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_does_not_wait_forever_for_a_stuck_request() {
    let mut config = config();
    config.server.shutdown_timeout = Duration::from_millis(500);
    config.server.heartbeat_interval = Duration::from_secs(60);
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let client = new_velo().await;
    install_wedged_heartbeat(&client, Duration::from_secs(120));
    wire_mutual(&hub, &client).await;
    let probe = tokio::spawn({
        let url = control(
            &hub,
            &velo_hub::protocol::instance_probe(client.instance_id()),
        );
        async move { http().post(url).send().await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(10), hub.shutdown())
        .await
        .expect("shutdown hung on a stuck request")
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown took {:?}",
        started.elapsed()
    );
    // The stuck request itself ends: it does not outlive the hub.
    let answer = tokio::time::timeout(Duration::from_secs(2), probe)
        .await
        .expect("the stuck request outlived shutdown")
        .unwrap();
    if let Ok(response) = answer {
        assert_eq!(response.status(), 503);
    }
}

/// The hub's own probe of a peer has a deadline: one heartbeat interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_to_a_wedged_peer_times_out() {
    let mut config = config();
    config.server.heartbeat_interval = Duration::from_millis(300);
    config.server.heartbeat_max_failures = 1000;
    let hub = Hub::builder(config)
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let client = new_velo().await;
    install_wedged_heartbeat(&client, Duration::from_secs(120));
    wire_mutual(&hub, &client).await;
    let started = std::time::Instant::now();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        http()
            .post(control(
                &hub,
                &velo_hub::protocol::instance_probe(client.instance_id()),
            ))
            .send(),
    )
    .await
    .expect("the probe has no deadline")
    .unwrap();
    assert_eq!(response.status(), 502);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "probe took {:?}",
        started.elapsed()
    );
}

/// Dropping a `shutdown()` future part-way does not leave the hub half
/// stopped: the shutdown finishes on its own, and a later call waits for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_shutdown_still_finishes() {
    let store = Arc::new(FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .velo(velo_builder())
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    let gate = ledger.gate_next_prepare();
    let held = tokio::spawn({
        let hub = hub.clone();
        // Decodes, so it reaches prepare on a hub with velo.
        let body = request(&unreachable_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), hub.shutdown())
            .await
            .is_err()
    );
    gate.release();
    let _ = held.await;
    hub.shutdown().await.unwrap();
    for addr in [hub.public_addr(), hub.control_addr()] {
        std::net::TcpListener::bind(addr)
            .unwrap_or_else(|error| panic!("{addr} still bound: {error}"));
    }
    assert!(
        store.list().iter().all(|stored| !stored.record.pinned),
        "the pinned entry is removed"
    );
}

/// Starts slowly, and records its shutdown.
struct SlowStart {
    log: Log,
}

impl Extension for SlowStart {
    const NAME: &'static str = "slow-start";
    type Registration = ();
    type Staged = ();

    async fn start(&self, _ctx: &StartContext<'_>) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    async fn shutdown(&self) {
        self.log.lock().push("shutdown slow-start".to_string());
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

/// Dropping a `serve()` future part-way does not strand what startup did:
/// the pinned entry is removed and started extensions shut down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_serve_still_cleans_up() {
    let store = Arc::new(FaultStore::default());
    let log = Log::default();
    let serving = builder()
        .store(store.clone())
        .velo(velo_builder())
        .extension(SlowStart {
            log: Arc::clone(&log),
        })
        .serve();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), serving)
            .await
            .is_err()
    );
    assert!(
        eventually(Duration::from_secs(3), || store.list().is_empty()
            && !log.lock().is_empty())
        .await,
        "entries: {}, log: {:?}",
        store.list().len(),
        log.lock()
    );
}

struct NeedsGhost;

impl Extension for NeedsGhost {
    const NAME: &'static str = "needs-ghost";
    type Registration = ();
    type Staged = ();

    fn registration_requires() -> &'static [&'static str] {
        &["ghost"]
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

struct CycleA;
struct CycleB;

impl Bundle for CycleA {
    const NAME: &'static str = "cycle-a";

    fn requires() -> &'static [&'static str] {
        &["cycle-b"]
    }

    fn install(self, _installer: &mut BundleInstaller<'_>) -> anyhow::Result<()> {
        Ok(())
    }
}

impl Bundle for CycleB {
    const NAME: &'static str = "cycle-b";

    fn requires() -> &'static [&'static str] {
        &["cycle-a"]
    }

    fn install(self, _installer: &mut BundleInstaller<'_>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn registration_requirements_and_bundle_cycles_are_checked() {
    let missing = builder().extension(NeedsGhost).serve().await.unwrap_err();
    assert!(
        matches!(
            missing.downcast_ref::<GraphError>(),
            Some(GraphError::MissingRegistrationRequirement {
                requires: "ghost",
                ..
            })
        ),
        "{missing}"
    );
    let cycle = builder()
        .bundle(CycleA)
        .bundle(CycleB)
        .serve()
        .await
        .unwrap_err();
    assert!(
        matches!(cycle.downcast_ref::<GraphError>(), Some(GraphError::BundleCycle(names)) if names == &["cycle-a", "cycle-b"]),
        "{cycle}"
    );
}

/// The pinned entry's removal at shutdown is bounded: a store that never
/// answers it does not hang shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_pinned_removal_does_not_hang_shutdown() {
    let store = Arc::new(FaultStore::default());
    let mut config = config();
    config.server.step_timeout = Duration::from_millis(200);
    let hub = Hub::builder(config)
        .store(store.clone())
        .velo(velo_builder())
        .serve()
        .await
        .unwrap();
    let _gate = store.gate_next_remove();
    let outcome = tokio::time::timeout(Duration::from_secs(3), hub.shutdown())
        .await
        .expect("shutdown hung on the pinned entry's removal");
    assert!(outcome.is_err(), "the unresolved removal is reported");
}

// ---- extension failures in startup and shutdown -----------------------------

#[derive(Clone, Copy, PartialEq)]
enum Misbehave {
    None,
    PanicInShutdown,
    HangInShutdown,
    PanicInStart,
    PanicInRoutes,
}

/// An extension that misbehaves in one place, and a witness that records
/// its own shutdown.
struct Unruly {
    how: Misbehave,
}

impl Extension for Unruly {
    const NAME: &'static str = "unruly";
    type Registration = ();
    type Staged = ();

    async fn start(&self, _ctx: &StartContext<'_>) -> anyhow::Result<()> {
        if self.how == Misbehave::PanicInStart {
            panic!("injected start panic");
        }
        Ok(())
    }

    async fn shutdown(&self) {
        match self.how {
            Misbehave::PanicInShutdown => panic!("injected shutdown panic"),
            Misbehave::HangInShutdown => std::future::pending::<()>().await,
            _ => {}
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        if self.how == Misbehave::PanicInRoutes {
            // axum rejects the old capture syntax with a panic.
            return Router::new().route("/a/:id", get(|| async { "x" }));
        }
        Router::new()
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

struct Witness {
    log: Log,
}

impl Extension for Witness {
    const NAME: &'static str = "witness";
    type Registration = ();
    type Staged = ();

    async fn shutdown(&self) {
        self.log.lock().push("shutdown witness".to_string());
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

fn unruly_builder(store: &Arc<FaultStore>, log: &Log, how: Misbehave) -> velo_hub::HubBuilder {
    let mut config = config();
    config.server.shutdown_timeout = Duration::from_millis(300);
    Hub::builder(config)
        .store(store.clone())
        .velo(velo_builder())
        .extension(Witness {
            log: Arc::clone(log),
        })
        .extension(Unruly { how })
}

/// What a finished stop leaves: ports free, no pinned entry, the witness
/// shut down.
fn assert_fully_stopped(hub: &Hub, store: &FaultStore, log: &Log) {
    for addr in [hub.public_addr(), hub.control_addr()] {
        std::net::TcpListener::bind(addr)
            .unwrap_or_else(|error| panic!("{addr} still bound: {error}"));
    }
    assert!(store.list().is_empty(), "the pinned entry is gone");
    assert!(
        log.lock().iter().any(|event| event == "shutdown witness"),
        "the witness shut down"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_extension_shutdown_does_not_leave_the_hub_half_stopped() {
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    let hub = unruly_builder(&store, &log, Misbehave::PanicInShutdown)
        .serve()
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), hub.shutdown())
        .await
        .expect("shutdown hung");
    assert_fully_stopped(&hub, &store, &log);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_extension_shutdown_is_bounded() {
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    let hub = unruly_builder(&store, &log, Misbehave::HangInShutdown)
        .serve()
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), hub.shutdown())
        .await
        .expect("shutdown hung");
    assert_fully_stopped(&hub, &store, &log);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panic_in_start_unwinds_like_an_error() {
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    assert!(
        unruly_builder(&store, &log, Misbehave::PanicInStart)
            .serve()
            .await
            .is_err()
    );
    assert!(store.list().is_empty(), "the pinned entry is removed");
    assert!(log.lock().iter().any(|event| event == "shutdown witness"));
    builder()
        .store(store.clone())
        .serve()
        .await
        .expect("the store is reusable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panic_while_building_routes_unwinds_and_leaves_the_store_reusable() {
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    assert!(
        unruly_builder(&store, &log, Misbehave::PanicInRoutes)
            .serve()
            .await
            .is_err()
    );
    assert!(store.list().is_empty(), "the pinned entry is removed");
    assert!(log.lock().iter().any(|event| event == "shutdown witness"));
    builder()
        .store(store.clone())
        .serve()
        .await
        .expect("the removal hook was not consumed");
}

/// Dropping the last `Hub` on a thread outside the runtime still stops it
/// in order, on the runtime that served it.
#[test]
fn dropping_the_hub_off_the_runtime_still_stops_it_in_order() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    let hub = runtime
        .block_on(unruly_builder(&store, &log, Misbehave::None).serve())
        .unwrap();
    assert!(tokio::runtime::Handle::try_current().is_err());
    drop(hub);
    let stopped = runtime.block_on(eventually(Duration::from_secs(3), || {
        store.list().is_empty() && log.lock().iter().any(|event| event == "shutdown witness")
    }));
    assert!(
        stopped,
        "entries: {}, log: {:?}",
        store.list().len(),
        log.lock()
    );
}

#[tokio::test]
async fn zero_intervals_are_rejected_before_binding() {
    for field in [
        "heartbeat_interval",
        "prune_interval",
        "step_timeout",
        "lock_timeout",
        "shutdown_timeout",
    ] {
        let mut config = config();
        match field {
            "heartbeat_interval" => config.server.heartbeat_interval = Duration::ZERO,
            "prune_interval" => config.server.prune_interval = Duration::ZERO,
            "step_timeout" => config.server.step_timeout = Duration::ZERO,
            "lock_timeout" => config.server.lock_timeout = Duration::ZERO,
            _ => config.server.shutdown_timeout = Duration::ZERO,
        }
        let error = Hub::builder(config).serve().await.unwrap_err();
        assert!(format!("{error:#}").contains(field), "{field}: {error:#}");
    }
}

/// A hub task that ignores its cancellation (a broken backend's liveness
/// task) is aborted after `shutdown_timeout_ms`, not waited on forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_that_ignores_cancellation_does_not_hang_shutdown() {
    let store = Arc::new(FaultStore::default());
    store.stubborn_liveness();
    let mut config = config();
    config.server.shutdown_timeout = Duration::from_millis(300);
    let hub = Hub::builder(config)
        .store(store.clone())
        .serve()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), hub.shutdown())
        .await
        .expect("shutdown waited on a task that ignores cancellation")
        .unwrap();
}

/// A removal queued while shutdown waits for the lock is still delivered:
/// shutdown drains the lifecycle queue before the extensions shut down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_delivers_the_removals_still_queued() {
    let store = Arc::new(FaultStore::default());
    let hub = builder()
        .store(store.clone())
        .extension(Ledger::default())
        .serve()
        .await
        .unwrap();
    let ledger = hub.extension::<Ledger>().unwrap();
    // Two: the lifecycle task holds one while it waits for the lock, and
    // the other stays in its queue.
    let victims = [make_peer(), make_peer()];
    for victim in &victims {
        register(&hub, &request_with(victim, &[ledger_opt_in()]), None).await;
    }
    let gate = ledger.gate_next_prepare();
    let held = tokio::spawn({
        let hub = hub.clone();
        let body = request(&make_peer());
        async move { post_register(&hub, &body, None).await.status() }
    });
    gate.entered().await;
    // Shutdown queues on the lock first, then the lifecycle task behind it.
    let shutdown = tokio::spawn({
        let hub = hub.clone();
        async move { hub.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    for victim in &victims {
        store.expire(victim.instance_id());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.release();
    let _ = held.await;
    shutdown.await.unwrap().unwrap();
    for victim in &victims {
        assert!(
            !ledger.has_owner(victim.instance_id()),
            "a queued removal was dropped"
        );
    }
}

/// A hub that outlives the runtime that served it: dropping it must not
/// panic. Its stop is spawned on a runtime that is gone, which does
/// nothing, and the tasks it would stop are gone with that runtime.
#[test]
fn dropping_a_hub_after_its_runtime_shut_down_does_not_panic() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    let hub = runtime
        .block_on(unruly_builder(&store, &log, Misbehave::None).serve())
        .unwrap();
    runtime.shutdown_background();
    drop(hub);
}

/// `shutdown` called from another runtime (a signal handler on the main
/// runtime stopping a hub served on a dedicated one) runs the stop on the
/// runtime that served the hub. Spawned on the caller's runtime, the stop
/// would die with it: the stage is already `Stopping`, so every later
/// `shutdown` would fail and the hub would never stop.
#[test]
fn a_shutdown_from_another_runtime_runs_on_the_serving_one() {
    let serving = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (store, log) = (Arc::new(FaultStore::default()), Log::default());
    let hub = serving
        .block_on(unruly_builder(&store, &log, Misbehave::None).serve())
        .unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Start the stop, give up at once, then lose the caller's runtime
    // before it ever runs another task.
    caller.block_on(async {
        let shutdown = std::pin::pin!(hub.shutdown());
        assert!(futures::poll!(shutdown).is_pending());
    });
    drop(caller);
    let outcome = serving
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), hub.shutdown()).await });
    assert!(
        matches!(outcome, Ok(Ok(()))),
        "{outcome:?}, log: {:?}",
        log.lock()
    );
    assert!(store.list().is_empty());
    assert!(log.lock().iter().any(|event| event == "shutdown witness"));
}

struct PanicsInDescriptor;

impl Extension for PanicsInDescriptor {
    const NAME: &'static str = "panics-in-descriptor";
    type Registration = ();
    type Staged = ();

    fn descriptor(&self) -> serde_json::Value {
        panic!("injected descriptor panic")
    }

    async fn prepare(&self, _: &velo_hub::Txn<'_>, _: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _: (), _: &velo_hub::Committed<'_>) {}

    fn on_removed(&self, _: Removal) {}
}

/// `GET /v1/hub` calls each extension's `descriptor`. A panic there must
/// answer an `ErrorBody` like every other core-route failure, not drop the
/// connection of an unauthenticated public route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_descriptor_answers_an_error_body() {
    let hub = builder()
        .extension(PanicsInDescriptor)
        .serve()
        .await
        .unwrap();
    let result = http()
        .get(public(&hub, velo_hub::protocol::paths::HUB))
        .send()
        .await;
    let response = result.expect("the connection was dropped instead of an ErrorBody");
    assert_eq!(response.status(), 500);
    assert_eq!(
        error_code(response).await,
        velo_hub::protocol::ErrorCode::Internal
    );
    hub.shutdown().await.unwrap();
}
