// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Throughput benchmark for the Velo messaging facade.
//!
//! Measures messages/sec and bytes/sec across three send patterns:
//!
//! - **Sequential**: one message in-flight at a time (baseline per-message cost)
//! - **Concurrent**: N messages in-flight simultaneously (tests parallelism)
//! - **Pipeline**: fire-and-forget with no per-message await (ceiling throughput)
//!
//! Results are printed as a table with latency percentiles (p50/p95/p99) for
//! sequential and concurrent modes.
//!
//! By default both halves run in one process, over loopback. To measure a
//! network, run `--role server` on one host and `--role client` on another,
//! with the same `--peer-file` on a shared filesystem and `VELO_BIND_IP` set to
//! each host's address on the network under test.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use hdrhistogram::Histogram;
use tokio::task::JoinSet;
use tokio::time::sleep;
use velo::{Handler, InstanceId, Velo};
use velo_examples::{TransportArgs, TransportType, new_transport};

#[derive(Parser, Debug)]
#[command(name = "throughput")]
#[command(about = "Benchmark Velo throughput: msgs/sec, MB/sec, latency percentiles")]
struct Args {
    /// Messages per benchmark cell (warmup is 10% of this, minimum 100).
    #[arg(long, default_value = "10000")]
    count: u64,

    #[command(flatten)]
    tx: TransportArgs,

    /// Payload sizes in bytes (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [0usize, 1024, 65536])]
    payload_sizes: Vec<usize>,

    /// Concurrency levels for concurrent mode (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [1usize, 10, 100])]
    concurrency: Vec<usize>,

    /// Which half of the benchmark this process runs. `both` runs the server
    /// and the client in one process.
    #[arg(long, value_enum, default_value_t = Role::Both)]
    role: Role,

    /// Where the server writes its peer info and the client reads it, for
    /// `--role server` and `--role client`.
    #[arg(long)]
    peer_file: Option<std::path::PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Role {
    Both,
    Server,
    Client,
}

/// How the client learns that the server counted every pipelined message.
///
/// In one process the server's `count` handler signals a channel. Across
/// processes the client polls `count_done` once a millisecond, which is small
/// against a pipeline run of seconds.
#[derive(Clone)]
enum Done {
    Local(flume::Receiver<()>),
    Remote,
}

struct CellResult {
    mode: &'static str,
    payload_bytes: usize,
    concurrency: Option<usize>,
    msgs_per_sec: f64,
    mb_per_sec: f64,
    p50_us: Option<u64>,
    p95_us: Option<u64>,
    p99_us: Option<u64>,
    p99_9_us: Option<u64>,
}

// ---------------------------------------------------------------------------
// Sequential
// ---------------------------------------------------------------------------

async fn run_sequential(
    velo: &Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    for _ in 0..warmup {
        velo.unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();
    for _ in 0..count {
        let t = Instant::now();
        velo.unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
        let _ = hist.record(t.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();

    Ok(CellResult {
        mode: "sequential",
        payload_bytes: payload.len(),
        concurrency: Some(1),
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Concurrent
// ---------------------------------------------------------------------------

async fn run_concurrent(
    velo: Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
    concurrency: usize,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    {
        let mut set = JoinSet::new();
        for _ in 0..warmup {
            if set.len() >= concurrency
                && let Some(Ok(Err(e))) = set.join_next().await
            {
                return Err(e);
            }
            let v = Arc::clone(&velo);
            let p = payload.clone();
            set.spawn(async move {
                v.unary("echo")?
                    .raw_payload(p)
                    .instance(target)
                    .send()
                    .await
            });
        }
        while set.join_next().await.is_some() {}
    }

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut set: JoinSet<Result<u64>> = JoinSet::new();
    let start = Instant::now();

    for _ in 0..count {
        if set.len() >= concurrency {
            match set.join_next().await {
                Some(Ok(Ok(us))) => {
                    let _ = hist.record(us);
                }
                Some(Ok(Err(e))) => return Err(e),
                _ => {}
            }
        }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        set.spawn(async move {
            let t = Instant::now();
            v.unary("echo")?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(t.elapsed().as_micros() as u64)
        });
    }
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(us)) => {
                let _ = hist.record(us);
            }
            Ok(Err(e)) => return Err(e),
            _ => {}
        }
    }

    let elapsed = start.elapsed();
    Ok(CellResult {
        mode: "concurrent",
        payload_bytes: payload.len(),
        concurrency: Some(concurrency),
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Pipeline (fire-and-forget)
// ---------------------------------------------------------------------------

async fn set_target(velo: &Arc<Velo>, target: InstanceId, count: u64) -> Result<()> {
    let payload = Bytes::from(rmp_serde::to_vec(&count)?);
    velo.unary("set_target")?
        .raw_payload(payload)
        .instance(target)
        .send()
        .await?;
    Ok(())
}

async fn run_pipeline(
    velo: &Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
    done: &Done,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);

    set_target(velo, target, warmup).await?;
    for _ in 0..warmup {
        velo.am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    wait_done(velo, target, done, Duration::from_secs(30))
        .await
        .map_err(|e| anyhow::anyhow!("pipeline warmup: {e}"))?;

    set_target(velo, target, count).await?;
    let start = Instant::now();
    for _ in 0..count {
        velo.am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    wait_done(velo, target, done, Duration::from_secs(60))
        .await
        .map_err(|e| anyhow::anyhow!("pipeline run did not complete: {e}"))?;

    let elapsed = start.elapsed();
    Ok(CellResult {
        mode: "pipeline",
        payload_bytes: payload.len(),
        concurrency: None,
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: None,
        p95_us: None,
        p99_us: None,
        p99_9_us: None,
    })
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

async fn wait_done(
    velo: &Arc<Velo>,
    target: InstanceId,
    done: &Done,
    limit: Duration,
) -> Result<()> {
    match done {
        Done::Local(rx) => {
            tokio::time::timeout(limit, rx.recv_async())
                .await
                .map_err(|_| anyhow::anyhow!("timed out"))?
                .map_err(|e| anyhow::anyhow!("done channel error: {e}"))?;
        }
        Done::Remote => {
            let deadline = Instant::now() + limit;
            loop {
                let reply = velo
                    .unary("count_done")?
                    .raw_payload(Bytes::new())
                    .instance(target)
                    .send()
                    .await?;
                if reply.first() == Some(&1) {
                    break;
                }
                if Instant::now() > deadline {
                    anyhow::bail!("timed out");
                }
                sleep(Duration::from_millis(1)).await;
            }
        }
    }
    Ok(())
}

fn format_payload(bytes: usize) -> String {
    if bytes == 0 {
        "0 B".to_string()
    } else if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{} MB", bytes / (1024 * 1024))
    }
}

fn print_table(results: &[CellResult], transport: TransportType) {
    println!("\n=== Throughput Benchmark ({transport:?}) ===\n");
    println!(
        "{:<12} {:>8} {:>12} {:>14} {:>9} {:>8} {:>8} {:>8} {:>8}",
        "Mode",
        "Payload",
        "Concurrency",
        "Msgs/sec",
        "MB/sec",
        "p50 µs",
        "p95 µs",
        "p99 µs",
        "p99.9 µs"
    );
    println!("{}", "-".repeat(95));

    let mut last_payload = usize::MAX;
    for r in results {
        if r.payload_bytes != last_payload && last_payload != usize::MAX {
            println!();
        }
        last_payload = r.payload_bytes;

        let concurrency = r
            .concurrency
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let p50 = r
            .p50_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p95 = r
            .p95_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p99 = r
            .p99_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p999 = r
            .p99_9_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());

        println!(
            "{:<12} {:>8} {:>12} {:>14} {:>9.2} {:>8} {:>8} {:>8} {:>8}",
            r.mode,
            format_payload(r.payload_bytes),
            concurrency,
            format!("{:.0}", r.msgs_per_sec),
            r.mb_per_sec,
            p50,
            p95,
            p99,
            p999,
        );
    }
    println!();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Using {:?} transport", args.tx.transport);
    println!(
        "count={}, payload_sizes={:?}, concurrency={:?}, role={:?}",
        args.count, args.payload_sizes, args.concurrency, args.role
    );
    match args.role {
        Role::Both => run_both(args),
        Role::Server => {
            let path = args
                .peer_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--role server needs --peer-file"))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let (done_tx, _done_rx) = flume::bounded::<()>(1);
                let velo = serve(args.tx.transport, done_tx).await?;
                // Written under another name and renamed, so a client never
                // reads a half-written file.
                let staged = path.with_extension("staged");
                std::fs::write(&staged, rmp_serde::to_vec(&velo.peer_info())?)?;
                std::fs::rename(&staged, &path)?;
                println!("Server ready; peer info in {}", path.display());
                std::future::pending::<()>().await;
                Ok(())
            })
        }
        Role::Client => {
            let path = args
                .peer_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--role client needs --peer-file"))?;
            let deadline = Instant::now() + Duration::from_secs(120);
            let server_peer_info: velo::PeerInfo = loop {
                if let Ok(bytes) = std::fs::read(&path) {
                    break rmp_serde::from_slice(&bytes)?;
                }
                if Instant::now() > deadline {
                    anyhow::bail!("no peer info at {} after 120 s", path.display());
                }
                std::thread::sleep(Duration::from_millis(200));
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let transport = args.tx.transport;
            let results = runtime.block_on(run_client(&args, server_peer_info, Done::Remote))?;
            print_table(&results, transport);
            Ok(())
        }
    }
}

/// Both halves in one process, on two runtimes.
fn run_both(args: Args) -> Result<()> {
    let runtime_server = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let runtime_client = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let (peer_info_tx, peer_info_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = flume::bounded::<()>(1);

    let transport_type = args.tx.transport;
    let server_handle = std::thread::spawn(move || {
        runtime_server.block_on(async move {
            let velo = serve(transport_type, done_tx).await.expect("build server");
            peer_info_tx.send(velo.peer_info()).unwrap();
            println!("Server ready");
            std::future::pending::<()>().await;
        });
    });

    let server_peer_info = peer_info_rx.recv().unwrap();
    let client_handle = std::thread::spawn(move || -> Result<Vec<CellResult>> {
        runtime_client.block_on(run_client(&args, server_peer_info, Done::Local(done_rx)))
    });

    let results = client_handle.join().unwrap()?;
    print_table(&results, transport_type);
    drop(server_handle);
    Ok(())
}

/// Build the server and register its handlers.
async fn serve(transport_type: TransportType, done_tx: flume::Sender<()>) -> Result<Arc<Velo>> {
    let transport = new_transport(transport_type, "throughput").await?;
    let velo = Velo::builder().add_transport(transport).build().await?;

    sleep(Duration::from_millis(100)).await;

    let echo_handler = Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload.clone()))).build();
    velo.register_handler(echo_handler)?;

    let counter = Arc::new(AtomicU64::new(0));
    let target = Arc::new(AtomicU64::new(u64::MAX));
    {
        let counter_for_set = Arc::clone(&counter);
        let target_for_set = Arc::clone(&target);
        let set_target_handler = Handler::unary_handler("set_target", move |ctx| {
            let n: u64 = rmp_serde::from_slice(&ctx.payload)
                .map_err(|e| anyhow::anyhow!("deserialize: {e}"))?;
            counter_for_set.store(0, Ordering::SeqCst);
            target_for_set.store(n, Ordering::SeqCst);
            Ok(Some(Bytes::new()))
        })
        .build();
        velo.register_handler(set_target_handler)?;
    }
    {
        let counter = Arc::clone(&counter);
        let target = Arc::clone(&target);
        let count_handler = Handler::am_handler("count", move |_ctx| {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let t = target.load(Ordering::SeqCst);
            if n >= t {
                let _ = done_tx.try_send(());
            }
            Ok(())
        })
        .build();
        velo.register_handler(count_handler)?;
    }
    {
        let count_done_handler = Handler::unary_handler("count_done", move |_ctx| {
            let reached = counter.load(Ordering::SeqCst) >= target.load(Ordering::SeqCst);
            Ok(Some(Bytes::from(vec![u8::from(reached)])))
        })
        .build();
        velo.register_handler(count_done_handler)?;
    }
    Ok(velo)
}

/// Run every benchmark cell against the server.
async fn run_client(
    args: &Args,
    server_peer_info: velo::PeerInfo,
    done: Done,
) -> Result<Vec<CellResult>> {
    let transport = new_transport(args.tx.transport, "throughput").await?;
    let velo = Velo::builder().add_transport(transport).build().await?;

    sleep(Duration::from_millis(100)).await;

    velo.register_peer(server_peer_info.clone())?;
    let target = server_peer_info.instance_id();
    sleep(Duration::from_millis(500)).await;

    velo.unary("echo")?
        .raw_payload(Bytes::new())
        .instance(target)
        .send()
        .await?;

    let mut all_results = Vec::new();
    for &size in &args.payload_sizes {
        let payload = Bytes::from(vec![0u8; size]);

        println!("\nSequential, payload={}", format_payload(size));
        all_results.push(run_sequential(&velo, target, payload.clone(), args.count).await?);

        for &c in &args.concurrency {
            println!(
                "Concurrent concurrency={c}, payload={}",
                format_payload(size)
            );
            all_results.push(
                run_concurrent(Arc::clone(&velo), target, payload.clone(), args.count, c).await?,
            );
        }

        println!("Pipeline, payload={}", format_payload(size));
        all_results.push(run_pipeline(&velo, target, payload.clone(), args.count, &done).await?);
    }

    Ok(all_results)
}
