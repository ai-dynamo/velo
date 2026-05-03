// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-process NIXL/RDMA rendezvous demo.
//!
//! Demonstrates `velo-rendezvous` Phase 2 between **two distinct OS processes**
//! (sibling children of a launcher), validating that the same-host UCX SHM
//! transport (sysv/posix/cma) actually carries an `NIXL_READ` end-to-end.
//!
//! # Topology
//!
//! ```text
//!     launcher (this binary, no --role)
//!         │
//!         ├── owner    (this binary, --role owner)
//!         │     ├─ TCP listener for messenger control plane
//!         │     ├─ NIXL agent "velo-<owner_worker_id>", UCX backend
//!         │     └─ register_data_pinned(<payload>)
//!         │
//!         └── consumer (this binary, --role consumer)
//!               ├─ TCP listener for messenger control plane
//!               ├─ NIXL agent "velo-<consumer_worker_id>", UCX backend
//!               └─ get(handle) → NIXL_READ over UCX SHM
//! ```
//!
//! # Handshake
//!
//! Children rendezvous via files in a temp directory:
//!
//! - `owner.peer`     — owner's serialized [`PeerInfo`] (owner writes)
//! - `consumer.peer`  — consumer's serialized [`PeerInfo`] (consumer writes)
//! - `handle.bin`     — owner's `DataHandle` as little-endian `u128` (owner writes)
//! - `done`           — consumer signals success (consumer writes, owner waits)
//!
//! Each file is written via "write to `.tmp` + atomic rename" so the reader
//! never sees a partial file.
//!
//! # Required environment
//!
//! `LD_LIBRARY_PATH`, `NIXL_PLUGIN_DIR`, and `UCX_TLS=tcp,cma,shm,self` must
//! be set on the launcher (children inherit). See `velo-rendezvous`'s README.
//!
//! # Run
//!
//! ```bash
//! cargo run --example rendezvous_nixl_two_proc --features nixl
//! # or with explicit knobs:
//! cargo run --example rendezvous_nixl_two_proc --features nixl -- \
//!     --size-bytes 1048576
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use velo::{Velo, transports::tcp::TcpTransportBuilder};
use velo_examples::init_tracing;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Role {
    /// Default: spawn owner + consumer as children of this process.
    Launcher,
    /// Owner: stage RDMA-pinned data, hand the consumer a `DataHandle`.
    Owner,
    /// Consumer: fetch RDMA-pinned data via NIXL_READ.
    Consumer,
}

#[derive(Parser, Debug)]
#[command(name = "rendezvous_nixl_two_proc")]
#[command(about = "Cross-process NIXL/RDMA rendezvous demo")]
struct Args {
    /// Which role to play. Default `launcher` forks two children.
    #[arg(long, value_enum, default_value = "launcher")]
    role: Role,

    /// Handshake directory shared between owner and consumer. Required for
    /// `--role owner` / `--role consumer`; auto-created in a tempdir for the
    /// launcher.
    #[arg(long)]
    handshake_dir: Option<PathBuf>,

    /// Payload size to register on the owner (default 1 MiB).
    #[arg(long, default_value_t = 1024 * 1024)]
    size_bytes: usize,

    /// Per-step rendezvous-file timeout. Increase if you're driving a debugger.
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

// ---------------------------------------------------------------------------
// File-based rendezvous helpers
// ---------------------------------------------------------------------------

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents)
        .with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn wait_for_file(path: &Path, timeout: Duration) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    loop {
        match std::fs::read(path) {
            Ok(bytes) if !bytes.is_empty() => return Ok(bytes),
            Ok(_) => {} // empty — partial write race; retry
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for {} (>{:?})", path.display(), timeout);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_marker(path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        if Instant::now() >= deadline {
            bail!("timed out waiting for marker {}", path.display());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Owner role
// ---------------------------------------------------------------------------

async fn run_owner(handshake: &Path, size_bytes: usize, timeout: Duration) -> Result<()> {
    eprintln!("[owner pid={}] starting up", std::process::id());

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let local_addr = listener.local_addr()?;
    eprintln!("[owner pid={}] TCP listener on {}", std::process::id(), local_addr);

    let transport = Arc::new(TcpTransportBuilder::new().from_listener(listener)?.build()?);
    let velo = Velo::builder().add_transport(transport).build().await?;
    velo.enable_nixl().context("owner enable_nixl")?;
    eprintln!("[owner pid={}] velo built, NIXL agent ready", std::process::id());

    // Stage the payload now so subsequent handshakes capture an up-to-date
    // local_md (registrations are reflected in get_local_md per the
    // PLAN §8.4 / nixl_endpoint comments).
    let payload = make_pattern(size_bytes);
    let handle = velo.register_data_pinned(payload.clone())?;
    eprintln!(
        "[owner pid={}] register_data_pinned: {} bytes, handle = {handle}",
        std::process::id(),
        size_bytes
    );

    // Publish our peer info.
    let peer_info_bytes = rmp_serde::to_vec_named(&velo.peer_info())?;
    atomic_write(&handshake.join("owner.peer"), &peer_info_bytes)?;

    // Wait for consumer to publish its peer info, register it, then publish
    // the data handle.
    let consumer_peer_bytes = wait_for_file(&handshake.join("consumer.peer"), timeout)?;
    let consumer_peer: velo::PeerInfo = rmp_serde::from_slice(&consumer_peer_bytes)?;
    velo.register_peer(consumer_peer)?;
    eprintln!("[owner pid={}] registered consumer peer", std::process::id());

    // Give the messenger a moment to actually open the outbound TCP
    // connection to the peer before the consumer fires off a metadata RPC.
    // (Mirrors the 200ms sleep in `rendezvous_integration.rs`.)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Hand the consumer the handle as 16 bytes LE.
    atomic_write(&handshake.join("handle.bin"), &handle.as_u128().to_le_bytes())?;
    eprintln!("[owner pid={}] published handle", std::process::id());

    // Wait until the consumer signals it's done before tearing down the agent
    // (NIXL READ on the consumer dereferences our registered region).
    wait_for_marker(&handshake.join("done"), timeout)?;
    eprintln!("[owner pid={}] consumer signaled done; exiting cleanly", std::process::id());

    Ok(())
}

// ---------------------------------------------------------------------------
// Consumer role
// ---------------------------------------------------------------------------

async fn run_consumer(handshake: &Path, size_bytes: usize, timeout: Duration) -> Result<()> {
    eprintln!("[consumer pid={}] starting up", std::process::id());

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let local_addr = listener.local_addr()?;
    eprintln!("[consumer pid={}] TCP listener on {}", std::process::id(), local_addr);

    let transport = Arc::new(TcpTransportBuilder::new().from_listener(listener)?.build()?);
    let velo = Velo::builder().add_transport(transport).build().await?;
    velo.enable_nixl().context("consumer enable_nixl")?;
    eprintln!("[consumer pid={}] velo built, NIXL agent ready", std::process::id());

    // Wait for the owner's peer info, register it, then publish ours so the
    // owner can register us back. The two registrations bootstrap bi-directional
    // messenger routing.
    let owner_peer_bytes = wait_for_file(&handshake.join("owner.peer"), timeout)?;
    let owner_peer: velo::PeerInfo = rmp_serde::from_slice(&owner_peer_bytes)?;
    velo.register_peer(owner_peer)?;
    eprintln!("[consumer pid={}] registered owner peer", std::process::id());

    let consumer_peer_bytes = rmp_serde::to_vec_named(&velo.peer_info())?;
    atomic_write(&handshake.join("consumer.peer"), &consumer_peer_bytes)?;

    // Same TCP-warmup sleep as on the owner side.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Now wait for the data handle.
    let handle_bytes = wait_for_file(&handshake.join("handle.bin"), timeout)?;
    if handle_bytes.len() != 16 {
        bail!("handle.bin: expected 16 bytes, got {}", handle_bytes.len());
    }
    let handle_raw = u128::from_le_bytes(handle_bytes[..16].try_into().unwrap());
    let handle = velo::DataHandle::from_u128(handle_raw);
    eprintln!("[consumer pid={}] received handle = {handle}", std::process::id());

    // Sanity: metadata should report the slot as pinned.
    let meta = velo.metadata(handle).await?;
    eprintln!(
        "[consumer pid={}] metadata: total_len={} pinned={}",
        std::process::id(),
        meta.total_len,
        meta.pinned
    );
    if !meta.pinned {
        bail!("expected pinned slot, got chunked metadata");
    }
    if meta.total_len as usize != size_bytes {
        bail!(
            "metadata total_len mismatch: {} vs expected {}",
            meta.total_len,
            size_bytes
        );
    }

    // The actual NIXL_READ.
    let started = Instant::now();
    let (data, lease_id) = velo.get(handle).await.context("consumer get")?;
    let elapsed = started.elapsed();
    eprintln!(
        "[consumer pid={}] get returned {} bytes in {:?} ({:.2} MiB/s)",
        std::process::id(),
        data.len(),
        elapsed,
        (data.len() as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64()
    );

    // Verify byte pattern matches the owner's `make_pattern`.
    let expected = make_pattern(size_bytes);
    if data[..] != expected[..] {
        bail!("byte mismatch: data does not match expected pattern");
    }
    eprintln!("[consumer pid={}] byte pattern verified ✓", std::process::id());

    velo.release(handle, lease_id).await?;

    // Signal owner that we're done.
    atomic_write(&handshake.join("done"), b"ok")?;
    eprintln!("[consumer pid={}] wrote done marker; exiting cleanly", std::process::id());

    Ok(())
}

// ---------------------------------------------------------------------------
// Launcher role: spawn owner + consumer as siblings
// ---------------------------------------------------------------------------

fn run_launcher(args: &Args) -> Result<()> {
    use std::process::{Command, Stdio};

    let exe = std::env::current_exe().context("current_exe")?;

    let handshake_dir = match &args.handshake_dir {
        Some(d) => {
            std::fs::create_dir_all(d)?;
            d.clone()
        }
        None => {
            let d = std::env::temp_dir()
                .join(format!("velo-rv-nixl-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&d)?;
            d
        }
    };
    eprintln!("[launcher pid={}] handshake dir: {}", std::process::id(), handshake_dir.display());

    // Quick env sanity check: warn (don't fail) if NIXL env vars look unset.
    for var in ["LD_LIBRARY_PATH", "NIXL_PLUGIN_DIR", "UCX_TLS"] {
        match std::env::var(var) {
            Ok(v) if !v.is_empty() => eprintln!("[launcher] {var}={v}"),
            _ => eprintln!("[launcher] WARNING: {var} not set — children will likely fail at NIXL init"),
        }
    }

    let common_args = [
        "--handshake-dir".to_owned(),
        handshake_dir.to_string_lossy().into_owned(),
        "--size-bytes".to_owned(),
        args.size_bytes.to_string(),
        "--timeout-secs".to_owned(),
        args.timeout_secs.to_string(),
    ];

    let mut owner = Command::new(&exe)
        .arg("--role")
        .arg("owner")
        .args(&common_args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn owner")?;
    eprintln!("[launcher pid={}] spawned owner pid={}", std::process::id(), owner.id());

    // Tiny lead so the owner has its TCP listener up before the consumer
    // starts polling. Not strictly required (the consumer waits on
    // owner.peer), but reduces noise.
    std::thread::sleep(Duration::from_millis(200));

    let mut consumer = Command::new(&exe)
        .arg("--role")
        .arg("consumer")
        .args(&common_args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn consumer")?;
    eprintln!("[launcher pid={}] spawned consumer pid={}", std::process::id(), consumer.id());

    let consumer_status = consumer.wait()?;
    let owner_status = owner.wait()?;

    // Cleanup handshake dir on success.
    if consumer_status.success() && owner_status.success() {
        let _ = std::fs::remove_dir_all(&handshake_dir);
    } else {
        eprintln!("[launcher] handshake dir preserved at {} for inspection", handshake_dir.display());
    }

    if !owner_status.success() {
        bail!("owner exited with {:?}", owner_status.code());
    }
    if !consumer_status.success() {
        bail!("consumer exited with {:?}", consumer_status.code());
    }
    eprintln!("[launcher pid={}] both children exited 0; demo successful", std::process::id());
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deterministic byte pattern used by both owner and consumer to verify
/// the round-trip without exchanging the contents.
fn make_pattern(size: usize) -> Bytes {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push((i % 256) as u8);
    }
    Bytes::from(v)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();

    match args.role {
        Role::Launcher => run_launcher(&args),
        Role::Owner | Role::Consumer => {
            let handshake = args
                .handshake_dir
                .clone()
                .context("--handshake-dir is required for --role owner/consumer")?;
            let timeout = Duration::from_secs(args.timeout_secs);

            // Multi-threaded runtime so the synchronous file-polling helpers
            // (`wait_for_file` / `wait_for_marker`) don't block the same
            // thread the messenger's TCP listener and outbound writer tasks
            // are running on. With a current-thread runtime, a `std::thread::sleep`
            // in any awaited helper stalls the entire reactor and the listener
            // never accepts the inbound connection — which is the exact bug
            // that bit me on the first run of this example.
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                match args.role {
                    Role::Owner => run_owner(&handshake, args.size_bytes, timeout).await,
                    Role::Consumer => {
                        run_consumer(&handshake, args.size_bytes, timeout).await
                    }
                    Role::Launcher => unreachable!(),
                }
            })
        }
    }
}
