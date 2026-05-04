// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-process **GPU** NIXL/RDMA rendezvous demo.
//!
//! Mirrors `rendezvous_nixl_two_proc.rs` but stages the owner's payload in
//! VRAM (via [`velo::Velo::register_data_pinned_device`]) and the consumer
//! pulls into a VRAM arena buffer (via
//! [`velo::Velo::get_into_device_arena`]). End-to-end this exercises the
//! GPU↔GPU path through velo→NIXL→UCX with `cuda_ipc` (intra-node) — no
//! host bytes touched on the data plane apart from the verification
//! `cudaMemcpy` D2H at the very end.
//!
//! For the GPU↔CPU and CPU↔GPU crosses, swap one side: register host bytes
//! with `register_data_pinned` and consume with `get_into_device_arena`,
//! or vice versa.
//!
//! # Required environment
//!
//! Same as `rendezvous_nixl_two_proc.rs`, plus a CUDA-aware UCX (a UCX build
//! configured with `--with-cuda` against the CUDA toolkit you intend to use).
//!
//! ```bash
//! # Point these at your NIXL + UCX installs.
//! NIXL_PREFIX=/path/to/nixl-install \
//! NIXL_LIBS=$NIXL_PREFIX/lib/x86_64-linux-gnu \
//! UCX_LIBS=/path/to/ucx-install/lib \
//! LD_LIBRARY_PATH="$NIXL_LIBS:$UCX_LIBS:/usr/local/cuda/lib64:$LD_LIBRARY_PATH" \
//! NIXL_PLUGIN_DIR="$NIXL_LIBS/plugins" \
//! UCX_TLS=tcp,cma,shm,self,cuda_copy,cuda_ipc \
//! cargo run --example rendezvous_nixl_gpu_two_proc --features nixl
//! ```

use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use velo::{Velo, transports::tcp::TcpTransportBuilder};
use velo_examples::init_tracing;

#[link(name = "cudart")]
unsafe extern "C" {
    fn cudaMemcpy(dst: *mut c_void, src: *const c_void, count: usize, kind: i32) -> i32;
    fn cudaSetDevice(device: i32) -> i32;
    fn cudaDeviceSynchronize() -> i32;
}

const CUDA_MEMCPY_DEVICE_TO_HOST: i32 = 2;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Role {
    Launcher,
    Owner,
    Consumer,
}

/// Where each side puts its memory.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Side {
    /// Host (DRAM) — `register_data_pinned` / `get`.
    Host,
    /// Device (VRAM) — `register_data_pinned_device` / `get_into_device_arena`.
    Device,
}

#[derive(Parser, Debug)]
#[command(name = "rendezvous_nixl_gpu_two_proc")]
#[command(about = "Cross-process GPU NIXL/RDMA rendezvous demo (VRAM↔VRAM)")]
struct Args {
    #[arg(long, value_enum, default_value = "launcher")]
    role: Role,

    #[arg(long)]
    handshake_dir: Option<PathBuf>,

    /// Payload size to stage on owner's GPU.
    #[arg(long, default_value_t = 1024 * 1024)]
    size_bytes: usize,

    /// CUDA device for both owner staging and consumer destination.
    #[arg(long, default_value_t = 0)]
    device_id: u32,

    /// Where the owner stages the payload.
    #[arg(long, value_enum, default_value = "device")]
    owner_side: Side,

    /// Where the consumer pulls the payload to.
    #[arg(long, value_enum, default_value = "device")]
    consumer_side: Side,

    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,
}

// ---------------------------------------------------------------------------
// File-based rendezvous helpers (same shape as the host demo)
// ---------------------------------------------------------------------------

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn wait_for_file(path: &Path, timeout: Duration) -> Result<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    loop {
        match std::fs::read(path) {
            Ok(bytes) if !bytes.is_empty() => return Ok(bytes),
            Ok(_) => {}
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

/// Deterministic byte pattern shared by owner and consumer.
fn make_pattern(size: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push((i % 256) as u8);
    }
    v
}

// ---------------------------------------------------------------------------
// Owner role: stage on GPU
// ---------------------------------------------------------------------------

async fn run_owner(
    handshake: &Path,
    size_bytes: usize,
    device_id: u32,
    side: Side,
    timeout: Duration,
) -> Result<()> {
    eprintln!(
        "[gpu-owner pid={}] starting up (stage on {:?})",
        std::process::id(),
        side
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let local_addr = listener.local_addr()?;
    eprintln!(
        "[gpu-owner pid={}] TCP listener on {}",
        std::process::id(),
        local_addr
    );

    let transport = Arc::new(TcpTransportBuilder::new().from_listener(listener)?.build()?);
    let velo = Velo::builder().add_transport(transport).build().await?;
    velo.enable_nixl().context("owner enable_nixl")?;
    eprintln!(
        "[gpu-owner pid={}] velo built, NIXL agent ready",
        std::process::id()
    );

    let host_pattern = make_pattern(size_bytes);
    let handle = match side {
        Side::Host => velo.register_data_pinned(&host_pattern)?,
        Side::Device => velo.register_data_pinned_device(&host_pattern, device_id)?,
    };
    eprintln!(
        "[gpu-owner pid={}] staged {} bytes ({:?}, dev={device_id}), handle = {handle}",
        std::process::id(),
        size_bytes,
        side
    );

    let peer_info_bytes = rmp_serde::to_vec_named(&velo.peer_info())?;
    atomic_write(&handshake.join("owner.peer"), &peer_info_bytes)?;

    let consumer_peer_bytes = wait_for_file(&handshake.join("consumer.peer"), timeout)?;
    let consumer_peer: velo::PeerInfo = rmp_serde::from_slice(&consumer_peer_bytes)?;
    velo.register_peer(consumer_peer)?;
    eprintln!("[gpu-owner pid={}] registered consumer peer", std::process::id());

    tokio::time::sleep(Duration::from_millis(300)).await;

    atomic_write(&handshake.join("handle.bin"), &handle.as_u128().to_le_bytes())?;
    eprintln!("[gpu-owner pid={}] published handle", std::process::id());

    wait_for_marker(&handshake.join("done"), timeout)?;
    eprintln!(
        "[gpu-owner pid={}] consumer signaled done; exiting cleanly",
        std::process::id()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Consumer role: pull into GPU
// ---------------------------------------------------------------------------

async fn run_consumer(
    handshake: &Path,
    size_bytes: usize,
    device_id: u32,
    side: Side,
    timeout: Duration,
) -> Result<()> {
    use dynamo_memory::MemoryDescriptor as _;

    eprintln!(
        "[gpu-consumer pid={}] starting up (pull to {:?})",
        std::process::id(),
        side
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let local_addr = listener.local_addr()?;
    eprintln!(
        "[gpu-consumer pid={}] TCP listener on {}",
        std::process::id(),
        local_addr
    );

    let transport = Arc::new(TcpTransportBuilder::new().from_listener(listener)?.build()?);
    let velo = Velo::builder().add_transport(transport).build().await?;
    velo.enable_nixl().context("consumer enable_nixl")?;
    eprintln!(
        "[gpu-consumer pid={}] velo built, NIXL agent ready",
        std::process::id()
    );

    let owner_peer_bytes = wait_for_file(&handshake.join("owner.peer"), timeout)?;
    let owner_peer: velo::PeerInfo = rmp_serde::from_slice(&owner_peer_bytes)?;
    velo.register_peer(owner_peer)?;
    eprintln!(
        "[gpu-consumer pid={}] registered owner peer",
        std::process::id()
    );

    let consumer_peer_bytes = rmp_serde::to_vec_named(&velo.peer_info())?;
    atomic_write(&handshake.join("consumer.peer"), &consumer_peer_bytes)?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let handle_bytes = wait_for_file(&handshake.join("handle.bin"), timeout)?;
    if handle_bytes.len() != 16 {
        bail!("handle.bin: expected 16 bytes, got {}", handle_bytes.len());
    }
    let handle_raw = u128::from_le_bytes(handle_bytes[..16].try_into().unwrap());
    let handle = velo::DataHandle::from_u128(handle_raw);

    let meta = velo.metadata(handle).await?;
    eprintln!(
        "[gpu-consumer pid={}] metadata: total_len={} pinned={}",
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

    // Bind the consumer's CUDA context to this thread for the verification
    // cudaMemcpy below (only needed when destination is device memory).
    if side == Side::Device {
        unsafe {
            let rc = cudaSetDevice(device_id as i32);
            if rc != 0 {
                bail!("cudaSetDevice({device_id}) failed: rc={rc}");
            }
        }
    }

    let expected = make_pattern(size_bytes);
    let started = Instant::now();

    let lease_id = match side {
        Side::Host => {
            let (data, lease_id) = velo.get(handle).await.context("consumer get (host)")?;
            let elapsed = started.elapsed();
            eprintln!(
                "[gpu-consumer pid={}] get→host returned {} bytes in {:?} ({:.2} MiB/s)",
                std::process::id(),
                data.len(),
                elapsed,
                (data.len() as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64()
            );
            if data[..] != expected[..] {
                bail!(
                    "host byte mismatch: first byte got 0x{:02x}, expected 0x{:02x}",
                    data.first().copied().unwrap_or(0),
                    expected.first().copied().unwrap_or(0)
                );
            }
            eprintln!(
                "[gpu-consumer pid={}] host byte pattern verified ✓",
                std::process::id()
            );
            lease_id
        }
        Side::Device => {
            let (dev_buf, lease_id) = velo
                .get_into_device_arena(handle, device_id)
                .await
                .context("consumer get_into_device_arena")?;
            let elapsed = started.elapsed();
            eprintln!(
                "[gpu-consumer pid={}] get→device returned {} bytes in {:?} ({:.2} MiB/s)",
                std::process::id(),
                dev_buf.size(),
                elapsed,
                (dev_buf.size() as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64()
            );

            let mut host_check = vec![0u8; size_bytes];
            unsafe {
                let rc = cudaMemcpy(
                    host_check.as_mut_ptr() as *mut c_void,
                    dev_buf.addr() as *const c_void,
                    size_bytes,
                    CUDA_MEMCPY_DEVICE_TO_HOST,
                );
                if rc != 0 {
                    bail!("cudaMemcpy D2H for verification failed: rc={rc}");
                }
                let rc = cudaDeviceSynchronize();
                if rc != 0 {
                    bail!("cudaDeviceSynchronize failed: rc={rc}");
                }
            }
            if host_check[..] != expected[..] {
                bail!(
                    "device byte mismatch: first byte got 0x{:02x}, expected 0x{:02x}",
                    host_check.first().copied().unwrap_or(0),
                    expected.first().copied().unwrap_or(0)
                );
            }
            eprintln!(
                "[gpu-consumer pid={}] device byte pattern verified ✓",
                std::process::id()
            );
            drop(dev_buf);
            lease_id
        }
    };

    velo.release(handle, lease_id).await?;

    atomic_write(&handshake.join("done"), b"ok")?;
    eprintln!(
        "[gpu-consumer pid={}] wrote done marker; exiting cleanly",
        std::process::id()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Launcher
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
            let d = std::env::temp_dir().join(format!("velo-rv-nixl-gpu-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&d)?;
            d
        }
    };
    eprintln!(
        "[launcher pid={}] handshake dir: {}",
        std::process::id(),
        handshake_dir.display()
    );

    for var in ["LD_LIBRARY_PATH", "NIXL_PLUGIN_DIR", "UCX_TLS"] {
        match std::env::var(var) {
            Ok(v) if !v.is_empty() => eprintln!("[launcher] {var}={v}"),
            _ => eprintln!(
                "[launcher] WARNING: {var} not set — children will likely fail at NIXL init"
            ),
        }
    }

    let common_args = [
        "--handshake-dir".to_owned(),
        handshake_dir.to_string_lossy().into_owned(),
        "--size-bytes".to_owned(),
        args.size_bytes.to_string(),
        "--device-id".to_owned(),
        args.device_id.to_string(),
        "--owner-side".to_owned(),
        format!("{:?}", args.owner_side).to_lowercase(),
        "--consumer-side".to_owned(),
        format!("{:?}", args.consumer_side).to_lowercase(),
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
    eprintln!(
        "[launcher pid={}] spawned owner pid={}",
        std::process::id(),
        owner.id()
    );

    std::thread::sleep(Duration::from_millis(200));

    let mut consumer = Command::new(&exe)
        .arg("--role")
        .arg("consumer")
        .args(&common_args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawn consumer")?;
    eprintln!(
        "[launcher pid={}] spawned consumer pid={}",
        std::process::id(),
        consumer.id()
    );

    let consumer_status = consumer.wait()?;
    let owner_status = owner.wait()?;

    if consumer_status.success() && owner_status.success() {
        let _ = std::fs::remove_dir_all(&handshake_dir);
    } else {
        eprintln!(
            "[launcher] handshake dir preserved at {} for inspection",
            handshake_dir.display()
        );
    }

    if !owner_status.success() {
        bail!("owner exited with {:?}", owner_status.code());
    }
    if !consumer_status.success() {
        bail!("consumer exited with {:?}", consumer_status.code());
    }
    eprintln!(
        "[launcher pid={}] both children exited 0; demo successful",
        std::process::id()
    );
    Ok(())
}

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

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                match args.role {
                    Role::Owner => {
                        run_owner(
                            &handshake,
                            args.size_bytes,
                            args.device_id,
                            args.owner_side,
                            timeout,
                        )
                        .await
                    }
                    Role::Consumer => {
                        run_consumer(
                            &handshake,
                            args.size_bytes,
                            args.device_id,
                            args.consumer_side,
                            timeout,
                        )
                        .await
                    }
                    Role::Launcher => unreachable!(),
                }
            })
        }
    }
}
