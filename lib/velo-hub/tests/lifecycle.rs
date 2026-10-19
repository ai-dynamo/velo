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
