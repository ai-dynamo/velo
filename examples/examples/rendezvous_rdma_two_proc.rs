// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two processes moving 8 MiB by RDMA GET, with a TCP control plane.
//!
//! ```text
//! cargo run --features ucx --example rendezvous_rdma_two_proc
//! ```
//!
//! That launches both roles as children and waits for them. To drive them by
//! hand — on two nodes, or under a profiler:
//!
//! ```text
//! cargo run --features ucx --example rendezvous_rdma_two_proc -- --role owner    --dir /tmp/rv
//! cargo run --features ucx --example rendezvous_rdma_two_proc -- --role consumer --dir /tmp/rv
//! ```
//!
//! # What it demonstrates
//!
//! * **UCX is not the control plane.** Both instances register a TCP transport
//!   *and* a UCX one. The acquire, the pull and the release are ordinary
//!   messages; only the payload moves over UCX. That is the deployment the
//!   eligibility rule was written for — a consumer offers the RDMA path because
//!   UCX is *registered* for the owner, not because it is primary.
//! * **Which path was actually taken.** Both sides print
//!   `velo_rendezvous_rdma_path_total` at the end. "It worked" is true on the
//!   chunked path too, so the metric is the only honest answer, and the
//!   consumer exits non-zero if it did not get the fast path.
//! * **What a fallback looks like.** Run it with `UCX_TLS` unset to something
//!   that cannot reach the peer, or with `VELO_RDMA_RENDEZVOUS_DISABLE=1`, and
//!   it still transfers correctly — the reason label changes and the exit code
//!   says the fast path was not used.
//!
//! # The rendezvous between the processes
//!
//! Each side writes a small JSON file and polls for the other's. Writes go to a
//! temporary name and are then `rename`d, which is atomic within a directory on
//! every filesystem this would run on — so a reader never sees a half-written
//! file and no lock file is needed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use clap::{Parser, ValueEnum};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use velo::observability::VeloMetrics;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{DataHandle, PeerInfo, ShutdownPolicy, Velo};

#[cfg(all(target_os = "linux", feature = "ucx"))]
use velo::transports::ucx::UcxTransportBuilder;

/// How long either side waits for the other to appear.
const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Role {
    /// Spawn both roles as children and wait for them. The default.
    Launch,
    /// Stage the payload and serve it.
    Owner,
    /// Pull the payload and check it.
    Consumer,
}

#[derive(Parser, Debug)]
#[command(about = "Move a large payload between two processes with an RDMA GET")]
struct Args {
    /// Which half to run.
    #[arg(long, value_enum, default_value_t = Role::Launch)]
    role: Role,

    /// Directory the two processes exchange their addresses through.
    #[arg(long, default_value = "/tmp/velo-rendezvous-rdma")]
    dir: PathBuf,

    /// Payload size in bytes.
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    size: usize,
}

/// What the owner publishes: how to reach it, and what to ask for.
#[derive(Serialize, Deserialize)]
struct OwnerCard {
    peer: PeerInfo,
    /// The staged payload's handle, as its `u128` decimal form.
    handle: String,
    size: usize,
}

/// What the consumer publishes: how to reach it, so the owner can answer.
#[derive(Serialize, Deserialize)]
struct ConsumerCard {
    peer: PeerInfo,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    if args.role == Role::Launch {
        return launch(&args);
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

/// Spawn the two halves and wait for them, so the whole demo is one command.
fn launch(args: &Args) -> Result<()> {
    let dir = fresh_dir(&args.dir)?;
    let exe = std::env::current_exe().context("locating this executable")?;
    let spawn = |role: &str| {
        std::process::Command::new(&exe)
            .arg("--role")
            .arg(role)
            .arg("--dir")
            .arg(&dir)
            .arg("--size")
            .arg(args.size.to_string())
            .spawn()
    };

    let mut owner = spawn("owner").context("spawning the owner")?;
    let mut consumer = spawn("consumer").context("spawning the consumer")?;

    let consumer_status = consumer.wait().context("waiting for the consumer")?;
    let owner_status = owner.wait().context("waiting for the owner")?;
    if !consumer_status.success() || !owner_status.success() {
        bail!("owner exited {owner_status}, consumer exited {consumer_status}");
    }
    println!("\nboth halves finished cleanly");
    Ok(())
}

async fn run(args: Args) -> Result<()> {
    std::fs::create_dir_all(&args.dir).context("creating the rendezvous directory")?;
    match args.role {
        Role::Owner => owner(args).await,
        Role::Consumer => consumer(args).await,
        Role::Launch => unreachable!("handled before the runtime is built"),
    }
}

// ---------------------------------------------------------------------------
// Owner
// ---------------------------------------------------------------------------

async fn owner(args: Args) -> Result<()> {
    let (velo, registry) = build().await?;

    // Staged in registered memory, so a capable consumer can read it with one
    // GET. This never fails: under pool pressure it stages in plain memory and
    // the consumer pulls chunks instead.
    let payload = pattern(args.size);
    let handle = velo.register_data_pinned(&payload).await;
    let staged_pinned = velo.metadata(handle).await?.pinned;
    println!(
        "owner: staged {} MiB, pinned={staged_pinned}, registered_bytes={}",
        args.size / (1024 * 1024),
        velo.rdma_registered_bytes()
    );

    publish(
        &args.dir.join("owner.json"),
        &OwnerCard {
            peer: velo.peer_info(),
            handle: handle.as_u128().to_string(),
            size: args.size,
        },
    )?;

    // The consumer's address, so replies have somewhere to go.
    let card: ConsumerCard = await_card(&args.dir.join("consumer.json")).await?;
    velo.register_peer(card.peer)?;

    // Wait for the consumer to say it is done rather than for a fixed time:
    // the owner must not tear its registrations down under a live transfer.
    await_file(&args.dir.join("done")).await?;

    println!("owner: {}", path_summary(&registry));
    // Read before shutting down, and asserted the same way the consumer does:
    // the owner is the side that *decides* whether a descriptor goes out, so an
    // owner that quietly answered chunked is exactly the failure this example
    // exists to make visible.
    let served_rdma = path_count(&registry, "ok") > 0;

    velo.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(30)))
        .await;

    if !served_rdma {
        bail!(
            "the owner answered chunked rather than with a descriptor; see the reason label \
             above"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Consumer
// ---------------------------------------------------------------------------

async fn consumer(args: Args) -> Result<()> {
    let card: OwnerCard = await_card(&args.dir.join("owner.json")).await?;
    let (velo, registry) = build().await?;

    let owner_instance = card.peer.instance_id();
    velo.register_peer(card.peer)?;
    publish(
        &args.dir.join("consumer.json"),
        &ConsumerCard {
            peer: velo.peer_info(),
        },
    )?;

    // Deterministic readiness: resolves when the owner's handler list actually
    // arrives, so there is nothing to sleep for.
    tokio::time::timeout(
        RENDEZVOUS_TIMEOUT,
        velo.wait_for_handler(owner_instance, "_rv_acquire"),
    )
    .await
    .context("the owner's handler list never arrived")??;

    let handle = DataHandle::from_u128(card.handle.parse()?);
    let started = Instant::now();
    let (data, lease) = velo.get(handle).await?;
    let elapsed = started.elapsed();
    velo.release(handle, lease).await?;

    if data.len() != card.size {
        bail!("expected {} bytes, got {}", card.size, data.len());
    }
    if data[..] != pattern(card.size)[..] {
        bail!("payload did not survive the transfer");
    }

    let mib = card.size as f64 / (1024.0 * 1024.0);
    println!(
        "consumer: {mib:.0} MiB verified in {elapsed:?} ({:.0} MiB/s)",
        mib / elapsed.as_secs_f64()
    );
    println!("consumer: {}", path_summary(&registry));

    let took_rdma = path_count(&registry, "ok") > 0;
    std::fs::write(args.dir.join("done"), b"done")?;
    velo.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(30)))
        .await;

    if !took_rdma {
        bail!(
            "the transfer was correct but did not use the RDMA path; see the \
             reason label above"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

/// A velo instance with a TCP control plane and a UCX transport beside it.
///
/// Both are registered. The messenger picks whichever it prefers for control
/// messages; the RDMA path only needs UCX to be *registered* for the peer,
/// which is what makes "control plane on TCP" a supported deployment rather
/// than a compromise.
async fn build() -> Result<(Arc<Velo>, Registry)> {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);

    let tcp = Arc::new(
        TcpTransportBuilder::new()
            .from_listener(std::net::TcpListener::bind("0.0.0.0:0")?)?
            .build()?,
    );

    #[cfg(all(target_os = "linux", feature = "ucx"))]
    let velo = {
        let ucx = Arc::new(UcxTransportBuilder::new().build()?);
        Velo::builder()
            .metrics(metrics)
            .add_transport(tcp)
            .add_ucx_transport(ucx)
            .build()
            .await?
    };
    #[cfg(not(all(target_os = "linux", feature = "ucx")))]
    let velo = {
        // Without the feature there is no RDMA path to demonstrate; the example
        // still runs and shows the chunked transfer it falls back to.
        eprintln!("note: built without the `ucx` feature — the chunked path is the only one here");
        Velo::builder()
            .metrics(metrics)
            .add_transport(tcp)
            .build()
            .await?
    };

    Ok((velo, registry))
}

// ---------------------------------------------------------------------------
// File rendezvous
// ---------------------------------------------------------------------------

/// Write atomically: a temporary name in the same directory, then a rename.
///
/// A reader therefore sees either nothing or the whole card, and neither side
/// needs a lock.
fn publish<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

async fn await_card<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = await_file(path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Poll for a file until it appears. Bounded, and says which file it gave up on.
async fn await_file(path: &Path) -> Result<Vec<u8>> {
    let deadline = Instant::now() + RENDEZVOUS_TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(bytes) = std::fs::read(path) {
            return Ok(bytes);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("timed out waiting for {}", path.display())
}

/// A run-scoped directory, so a stale card from a previous run cannot be
/// mistaken for this one's.
fn fresh_dir(base: &Path) -> Result<PathBuf> {
    let dir = base.join(format!("run-{}", std::process::id()));
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

// ---------------------------------------------------------------------------
// Payload and reporting
// ---------------------------------------------------------------------------

/// Every byte depends on its offset, so a transfer that lands shifted, short or
/// duplicated fails the check rather than passing on a lucky pattern.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(31).wrapping_add(i >> 8)) as u8)
        .collect()
}

/// Every non-zero `velo_rendezvous_rdma_path_total` series, as `path/reason=n`.
fn path_summary(registry: &Registry) -> String {
    let mut parts: Vec<String> = registry
        .gather()
        .iter()
        .filter(|family| family.name() == "velo_rendezvous_rdma_path_total")
        .flat_map(|family| family.get_metric())
        .filter(|metric| metric.get_counter().value() > 0.0)
        .map(|metric| {
            let label = |name: &str| {
                metric
                    .get_label()
                    .iter()
                    .find(|pair| pair.name() == name)
                    .map(|pair| pair.value().to_string())
                    .unwrap_or_default()
            };
            format!(
                "{}/{}={}",
                label("path"),
                label("reason"),
                metric.get_counter().value() as u64
            )
        })
        .collect();
    parts.sort();
    if parts.is_empty() {
        "no rendezvous path decisions recorded".to_string()
    } else {
        format!("path decisions: {}", parts.join(" "))
    }
}

fn path_count(registry: &Registry, reason: &str) -> u64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == "velo_rendezvous_rdma_path_total")
        .flat_map(|family| family.get_metric())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == "reason" && pair.value() == reason)
        })
        .map(|metric| metric.get_counter().value() as u64)
        .sum()
}
