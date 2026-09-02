// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Threshold sweep harness for the RDMA rendezvous hardware checkpoint.
//!
//! Same two-process shape as `rendezvous_rdma_two_proc`, but it stages one slot
//! per size, transfers each many times inside a single process pair, and lets
//! `rdma_min_bytes` be set from the command line so the crossover can be
//! bracketed from below. Not a shipped example — a measurement tool.
//!
//! Deliberately unwired: this file is evidence, not a build target. It calls
//! Phase-3 API that is not on `main` yet, so nothing compiles it here. See
//! `agent-docs/2026-08-29-rdma-phase3-hardware-checkpoint.md` §10 for what is
//! missing and for the `[[example]]` stanza to add once Phase 3 lands.

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
#[cfg(all(target_os = "linux", feature = "ucx"))]
use velo::{RdmaConfig, RdmaRendezvousConfig};

const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Role {
    Owner,
    Consumer,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, value_enum)]
    role: Role,
    #[arg(long)]
    dir: PathBuf,
    /// Comma-separated payload sizes in bytes.
    #[arg(long, default_value = "4096,8192,16384,32768,65536,131072,262144,524288,1048576")]
    sizes: String,
    /// Transfers per size.
    #[arg(long, default_value_t = 10)]
    reps: usize,
    /// `rdma_min_bytes` for both roles.
    #[arg(long, default_value_t = 65536)]
    min_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct OwnerCard {
    peer: PeerInfo,
    /// (size, handle as u128 decimal, pinned)
    slots: Vec<(usize, String, bool)>,
}

#[derive(Serialize, Deserialize)]
struct ConsumerCard {
    peer: PeerInfo,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    std::fs::create_dir_all(&args.dir).context("creating the rendezvous directory")?;
    let sizes: Vec<usize> = args
        .sizes
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<_, _>>()?;
    match args.role {
        Role::Owner => owner(args, sizes).await,
        Role::Consumer => consumer(args, sizes).await,
    }
}

async fn owner(args: Args, sizes: Vec<usize>) -> Result<()> {
    let (velo, _registry) = build(args.min_bytes).await?;
    // A rendezvous slot is single-use: `release` drops it. Stage one slot per
    // transfer so the pair can measure a warm endpoint rather than re-paying
    // wireup on every rep.
    let mut slots = Vec::new();
    for &size in &sizes {
        let payload = pattern(size);
        let mut pinned_all = true;
        for _ in 0..args.reps {
            let handle = velo.register_data_pinned(&payload).await;
            let pinned = velo.metadata(handle).await?.pinned;
            pinned_all &= pinned;
            slots.push((size, handle.as_u128().to_string(), pinned));
        }
        println!("OWNER staged size={size} x{} pinned={pinned_all}", args.reps);
    }
    println!("OWNER registered_bytes={}", velo.rdma_registered_bytes());

    publish(
        &args.dir.join("owner.json"),
        &OwnerCard {
            peer: velo.peer_info(),
            slots,
        },
    )?;
    let card: ConsumerCard = await_card(&args.dir.join("consumer.json")).await?;
    velo.register_peer(card.peer)?;
    await_file(&args.dir.join("done")).await?;
    println!("OWNER done");
    velo.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(30)))
        .await;
    Ok(())
}

async fn consumer(args: Args, _sizes: Vec<usize>) -> Result<()> {
    let card: OwnerCard = await_card(&args.dir.join("owner.json")).await?;
    let (velo, registry) = build(args.min_bytes).await?;
    let owner_instance = card.peer.instance_id();
    velo.register_peer(card.peer)?;
    publish(
        &args.dir.join("consumer.json"),
        &ConsumerCard {
            peer: velo.peer_info(),
        },
    )?;
    tokio::time::timeout(
        RENDEZVOUS_TIMEOUT,
        velo.wait_for_handler(owner_instance, "_rv_acquire"),
    )
    .await
    .context("the owner's handler list never arrived")??;

    let mut expected: std::collections::HashMap<usize, Vec<u8>> = Default::default();
    let mut reps_seen: std::collections::HashMap<usize, usize> = Default::default();
    for (size, handle_str, pinned) in &card.slots {
        let handle = DataHandle::from_u128(handle_str.parse()?);
        let want = expected.entry(*size).or_insert_with(|| pattern(*size));
        let rep = {
            let c = reps_seen.entry(*size).or_insert(0);
            let r = *c;
            *c += 1;
            r
        };
        let before = path_count(&registry, "ok");
        let started = Instant::now();
        let (data, lease) = velo.get(handle).await?;
        let elapsed = started.elapsed();
        velo.release(handle, lease).await?;
        let took_rdma = path_count(&registry, "ok") > before;
        if data.len() != *size {
            bail!("size {size}: expected {size} bytes, got {}", data.len());
        }
        if data[..] != want[..] {
            bail!("size {size}: payload did not survive the transfer");
        }
        println!(
            "SWEEP size={size} rep={rep} pinned={pinned} path={} get_us={}",
            if took_rdma { "rdma" } else { "chunked" },
            elapsed.as_micros()
        );
    }
    println!("CONSUMER {}", path_summary(&registry));
    std::fs::write(args.dir.join("done"), b"done")?;
    velo.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(30)))
        .await;
    Ok(())
}

#[allow(unused_variables)]
async fn build(min_bytes: u64) -> Result<(Arc<Velo>, Registry)> {
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
        let rdma = RdmaConfig {
            rendezvous: RdmaRendezvousConfig {
                rdma_min_bytes: min_bytes,
                ..RdmaRendezvousConfig::default()
            },
            ..RdmaConfig::default()
        };
        Velo::builder()
            .metrics(metrics)
            .add_transport(tcp)
            .add_ucx_transport(ucx)
            .rdma_config(rdma)
            .build()
            .await?
    };
    #[cfg(not(all(target_os = "linux", feature = "ucx")))]
    let velo = Velo::builder()
        .metrics(metrics)
        .add_transport(tcp)
        .build()
        .await?;

    Ok((velo, registry))
}

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

async fn await_file(path: &Path) -> Result<Vec<u8>> {
    let deadline = Instant::now() + RENDEZVOUS_TIMEOUT;
    while Instant::now() < deadline {
        // Re-reading the directory forces the NFS client to revalidate, which a
        // bare `read` of a name it has cached as absent will not do.
        let _ = path.parent().map(std::fs::read_dir);
        if let Ok(bytes) = std::fs::read(path) {
            return Ok(bytes);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("timed out waiting for {}", path.display())
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(31).wrapping_add(i >> 8)) as u8)
        .collect()
}

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
    format!("path decisions: {}", parts.join(" "))
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
