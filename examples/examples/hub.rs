// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A hub service built from two bundles, configured from a file.
//!
//! - `counter` keeps a count per registered instance. Its control route
//!   increments the caller's own count (authorized by its credential, fenced
//!   by its registration's epoch); its public route reads any count.
//! - `greeter` requires `counter`: it reaches it through its attach context
//!   and greets an instance with its count.
//!
//! ```text
//! cargo run --example hub -- --config hub.json          # serve until Ctrl-C
//! cargo run --example hub -- --config hub.json --demo   # serve, run a client, stop
//! ```
//!
//! `--demo` serves on free ports, registers a client through `HubClient`,
//! serves a service, calls both bundles, checks the answers, and stops.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use serde::{Deserialize, Serialize};
use velo::transports::tcp::TcpTransportBuilder;
use velo_hub::axum::extract::State;
use velo_hub::axum::routing::{get, post};
use velo_hub::axum::{Json, Router};
use velo_hub::client::HubClient;
use velo_hub::protocol::RegistrationEpoch;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::discovery::ServiceDiscovery;
use velo_hub::velo::{InstanceId, Velo, VeloBuilder};
use velo_hub::{
    AttachContext, Authorized, Bundle, BundleInstaller, Committed, Extension, Hub, HubConfig,
    HubError, HubPath, Rejection, Txn,
};

#[derive(Parser, Debug)]
#[command(name = "hub", about = "A velo hub with two bundles")]
struct Args {
    /// The hub config (JSON): `server` settings and one section per bundle.
    #[arg(long)]
    config: PathBuf,
    /// Serve on free ports, run a client against the hub, check, and stop.
    #[arg(long)]
    demo: bool,
}

// ---- counter ---------------------------------------------------------------

/// The `counter` bundle's config section.
#[derive(Debug, Clone, Default, Deserialize)]
struct CounterConfig {
    /// The count a new registration starts at.
    #[serde(default)]
    start: u64,
}

struct CounterBundle;

impl Bundle for CounterBundle {
    const NAME: &'static str = "counter";

    fn install(self, installer: &mut BundleInstaller<'_>) -> Result<()> {
        let config: CounterConfig = installer.config()?.unwrap_or_default();
        installer.extension(Counter {
            start: config.start,
            counts: Mutex::new(HashMap::new()),
        });
        Ok(())
    }
}

/// One count per registered instance, with the registration it was
/// committed for: the epoch fences mutations, the incarnation fences
/// removals.
struct Counter {
    start: u64,
    counts: Mutex<HashMap<InstanceId, Owned>>,
}

struct Owned {
    incarnation: Incarnation,
    epoch: RegistrationEpoch,
    count: u64,
}

impl Counter {
    fn count(&self, owner: InstanceId) -> Option<u64> {
        self.counts
            .lock()
            .unwrap()
            .get(&owner)
            .map(|owned| owned.count)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Count {
    count: u64,
}

async fn increment(
    State(counter): State<Arc<Counter>>,
    auth: Authorized,
) -> Result<Json<Count>, HubError> {
    let mut counts = counter.counts.lock().unwrap();
    // The extension fence: apply only under the epoch this extension
    // committed for the owner, checked under its own lock with the write.
    match counts.get_mut(&auth.owner) {
        Some(owned) if owned.epoch == auth.lease.epoch => {
            owned.count += 1;
            Ok(Json(Count { count: owned.count }))
        }
        _ => Err(HubError::not_found("no count for this registration")),
    }
}

async fn read(
    State(counter): State<Arc<Counter>>,
    HubPath(owner): HubPath<InstanceId>,
) -> Result<Json<Count>, HubError> {
    counter
        .count(owner)
        .map(|count| Json(Count { count }))
        .ok_or_else(|| HubError::not_found("no count for this instance"))
}

impl Extension for Counter {
    const NAME: &'static str = "counter";
    type Registration = ();
    type Staged = ();

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, (): (), registration: &Committed<'_>) {
        let mut counts = self.counts.lock().unwrap();
        // A registrant that opts in keeps its count under the new
        // registration; one that does not drops what it had (the extension
        // rule: without a payload, `commit` drops the owner's state).
        if registration.opted_in(Self::NAME) {
            let count = counts
                .get(&registration.owner)
                .map_or(self.start, |owned| owned.count);
            counts.insert(
                registration.owner,
                Owned {
                    incarnation: registration.incarnation,
                    epoch: registration.epoch,
                    count,
                },
            );
        } else {
            counts.remove(&registration.owner);
        }
    }

    fn on_removed(&self, removal: Removal) {
        // A removal may arrive twice, or name an older incarnation than the
        // one committed since: only a removal of the current one or a later
        // one drops the count.
        let mut counts = self.counts.lock().unwrap();
        if counts
            .get(&removal.id)
            .is_some_and(|owned| owned.incarnation <= removal.incarnation)
        {
            counts.remove(&removal.id);
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route("/instances/{instance_id}", get(read))
            .with_state(self)
    }

    fn control_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route("/instances/{instance_id}/increment", post(increment))
            .with_state(self)
    }
}

// ---- greeter ---------------------------------------------------------------

/// The `greeter` bundle's config section.
#[derive(Debug, Clone, Deserialize)]
struct GreeterConfig {
    greeting: String,
}

struct GreeterBundle;

impl Bundle for GreeterBundle {
    const NAME: &'static str = "greeter";

    fn requires() -> &'static [&'static str] {
        &[CounterBundle::NAME]
    }

    fn install(self, installer: &mut BundleInstaller<'_>) -> Result<()> {
        let config: GreeterConfig = installer
            .config()?
            .context("the greeter bundle needs a `greeting` in its config section")?;
        installer.extension(Greeter {
            greeting: config.greeting,
            counter: OnceLock::new(),
        });
        Ok(())
    }
}

struct Greeter {
    greeting: String,
    /// Set at attach, from the hub, because `requires` names it.
    counter: OnceLock<Arc<Counter>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Greeting {
    text: String,
}

async fn greet(
    State(greeter): State<Arc<Greeter>>,
    HubPath(owner): HubPath<InstanceId>,
) -> Result<Json<Greeting>, HubError> {
    let counter = greeter.counter.get().expect("attached");
    let count = counter
        .count(owner)
        .ok_or_else(|| HubError::not_found("this instance has no count"))?;
    Ok(Json(Greeting {
        text: format!("{}, {owner}: your count is {count}", greeter.greeting),
    }))
}

impl Extension for Greeter {
    const NAME: &'static str = "greeter";
    type Registration = ();
    type Staged = ();

    fn requires() -> &'static [&'static str] {
        &[Counter::NAME]
    }

    async fn attach(&self, ctx: &AttachContext<'_>) -> Result<()> {
        if self.counter.set(ctx.extension::<Counter>()?).is_err() {
            unreachable!("the hub attaches an extension once");
        }
        Ok(())
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, (): (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route("/hello/{instance_id}", get(greet))
            .with_state(self)
    }
}

// ---- main ------------------------------------------------------------------

fn velo_builder() -> Result<VeloBuilder> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let tcp = TcpTransportBuilder::new()
        .from_listener(listener)?
        .build()?;
    Ok(Velo::builder().add_transport(Arc::new(tcp)))
}

async fn serve(config: HubConfig) -> Result<Hub> {
    Hub::builder(config)
        .velo(velo_builder()?)
        .bundle(CounterBundle)
        .bundle(GreeterBundle)
        .serve()
        .await
}

/// Register a client with the hub, use both bundles and a service, and
/// check the answers.
async fn demo(hub: &Hub) -> Result<()> {
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .build()?;
    let velo = client.velo_builder().add_transport(Arc::new(
        TcpTransportBuilder::new()
            .from_listener(std::net::TcpListener::bind("127.0.0.1:0")?)?
            .build()?,
    ));
    let velo = velo.build().await?;
    let mut extensions = velo_hub::client::Extensions::new();
    extensions.insert(Counter::NAME.to_string(), serde_json::Value::Null);
    let registration = client.register(&velo, extensions).await?;
    let me = registration.instance_id;
    println!("registered {me}, epoch {:?}", registration.epoch);

    let _service = client.register_service("demo-workers").await?;
    let workers = client.services().get_instances("demo-workers").await?;
    ensure!(workers == [me], "service lookup returned {workers:?}");
    println!("serving demo-workers: {workers:?}");

    let counter = client.ext(Counter::NAME);
    for _ in 0..3 {
        let Count { count } = counter
            .control_post(&format!("/instances/{me}/increment"), &())
            .await?;
        println!("incremented to {count}");
    }
    let Count { count } = counter.get(&format!("/instances/{me}")).await?;
    let Greeting { text } = client
        .ext(Greeter::NAME)
        .get(&format!("/hello/{me}"))
        .await?;
    println!("{text}");
    ensure!(
        text.ends_with(&format!("your count is {count}")),
        "greeting: {text}"
    );

    client.unregister().await?;
    velo.shutdown(velo::ShutdownPolicy::Timeout(
        std::time::Duration::from_secs(2),
    ))
    .await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let mut config: HubConfig = serde_json::from_str(&text).context("parsing the hub config")?;
    if args.demo {
        config.server.public_port = 0;
        config.server.control_port = 0;
    }
    let hub = serve(config).await?;
    println!(
        "hub {} serving: public {}, control {}",
        hub.hub_id(),
        hub.public_addr(),
        hub.control_addr()
    );
    if args.demo {
        let outcome = demo(&hub).await;
        hub.shutdown().await?;
        outcome?;
        println!("demo passed");
        Ok(())
    } else {
        hub.run_until_signal().await
    }
}
