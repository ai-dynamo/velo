// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Throughput benchmark for Messenger active message system.
//!
//! Measures messages/sec and bytes/sec across three send patterns:
//!
//! - **Sequential**: one message in-flight at a time (baseline per-message cost)
//! - **Concurrent**: N messages in-flight simultaneously (tests parallelism)
//! - **Pipeline**: fire-and-forget with no per-message await (ceiling throughput)
//!
//! Results are printed as a table with latency percentiles (p50/p95/p99) for
//! sequential and concurrent modes.

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::sleep;
use velo_messenger::{Handler, Messenger};
use velo_transports::Transport;
use velo_transports::tcp::TcpTransportBuilder;
#[cfg(unix)]
use velo_transports::uds::UdsTransportBuilder;

/// Transport backends available for this benchmark.
#[derive(Debug, Clone, ValueEnum)]
enum TransportType {
    Tcp,
    #[cfg(unix)]
    Uds,
}

/// Create a transport based on the selected type.
async fn new_transport(transport_type: &TransportType) -> Arc<dyn Transport> {
    match transport_type {
        TransportType::Tcp => {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            Arc::new(
                TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        }
        #[cfg(unix)]
        TransportType::Uds => {
            let socket_path =
                std::env::temp_dir().join(format!("velo-throughput-{}.sock", uuid::Uuid::new_v4()));
            Arc::new(
                UdsTransportBuilder::new()
                    .socket_path(&socket_path)
                    .build()
                    .unwrap(),
            )
        }
    }
}

/// CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "throughput")]
#[command(about = "Benchmark Messenger throughput: msgs/sec, MB/sec, latency percentiles")]
struct Args {
    /// Messages per benchmark cell (warmup is 10% of this, minimum 100).
    #[arg(long, default_value = "10000")]
    count: u64,

    /// Transport backend.
    #[arg(long, default_value = "tcp")]
    transport: TransportType,

    /// Payload sizes in bytes (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [0usize, 1024, 65536])]
    payload_sizes: Vec<usize>,

    /// Concurrency levels for concurrent mode (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [1usize, 10, 100])]
    concurrency: Vec<usize>,
}

/// Results for one benchmark cell.
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
// Sequential benchmark
// ---------------------------------------------------------------------------

async fn run_sequential(
    messenger: &Arc<Messenger>,
    target: velo_common::InstanceId,
    payload: Bytes,
    count: u64,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    for _ in 0..warmup {
        messenger
            .unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();

    for _ in 0..count {
        let t = Instant::now();
        messenger
            .unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
        let us = t.elapsed().as_micros() as u64;
        let _ = hist.record(us);
    }

    let elapsed = start.elapsed();
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0;

    Ok(CellResult {
        mode: "sequential",
        payload_bytes: payload.len(),
        concurrency: Some(1),
        msgs_per_sec,
        mb_per_sec,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Concurrent benchmark
// ---------------------------------------------------------------------------

async fn run_concurrent(
    messenger: Arc<Messenger>,
    target: velo_common::InstanceId,
    payload: Bytes,
    count: u64,
    concurrency: usize,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    {
        let mut set = JoinSet::new();
        for _ in 0..warmup {
            if set.len() >= concurrency {
                set.join_next().await;
            }
            let m = Arc::clone(&messenger);
            let p = payload.clone();
            set.spawn(async move {
                m.unary("echo")?
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
        if set.len() >= concurrency
            && let Some(Ok(Ok(us))) = set.join_next().await
        {
            let _ = hist.record(us);
        }
        let m = Arc::clone(&messenger);
        let p = payload.clone();
        set.spawn(async move {
            let t = Instant::now();
            m.unary("echo")?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(t.elapsed().as_micros() as u64)
        });
    }
    while let Some(Ok(Ok(us))) = set.join_next().await {
        let _ = hist.record(us);
    }

    let elapsed = start.elapsed();
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0;

    Ok(CellResult {
        mode: "concurrent",
        payload_bytes: payload.len(),
        concurrency: Some(concurrency),
        msgs_per_sec,
        mb_per_sec,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Pipeline (fire-and-forget) benchmark
// ---------------------------------------------------------------------------

/// Call set_target on the server (unary) to atomically reset counter + set target.
/// This guarantees the server has processed the update before we start firing.
async fn set_target(
    messenger: &Arc<Messenger>,
    target_instance: velo_common::InstanceId,
    count: u64,
) -> Result<()> {
    let payload = Bytes::from(rmp_serde::to_vec(&count)?);
    messenger
        .unary("set_target")?
        .raw_payload(payload)
        .instance(target_instance)
        .send()
        .await?;
    Ok(())
}

async fn run_pipeline(
    messenger: &Arc<Messenger>,
    target: velo_common::InstanceId,
    payload: Bytes,
    count: u64,
    done_rx: flume::Receiver<()>,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);

    // Warmup pass — set_target waits for server ack before we fire.
    set_target(messenger, target, warmup).await?;
    for _ in 0..warmup {
        messenger
            .am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    tokio::time::timeout(Duration::from_secs(30), done_rx.recv_async())
        .await
        .map_err(|_| anyhow::anyhow!("pipeline warmup timed out"))?
        .ok();

    // Measured pass.
    set_target(messenger, target, count).await?;
    let start = Instant::now();
    for _ in 0..count {
        messenger
            .am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    tokio::time::timeout(Duration::from_secs(60), done_rx.recv_async())
        .await
        .map_err(|_| anyhow::anyhow!("pipeline run did not complete: timed out"))?
        .map_err(|e| anyhow::anyhow!("pipeline done channel error: {}", e))?;

    let elapsed = start.elapsed();
    let msgs_per_sec = count as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0;

    Ok(CellResult {
        mode: "pipeline",
        payload_bytes: payload.len(),
        concurrency: None,
        msgs_per_sec,
        mb_per_sec,
        p50_us: None,
        p95_us: None,
        p99_us: None,
        p99_9_us: None,
    })
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

fn format_payload(bytes: usize) -> String {
    if bytes == 0 {
        "0 B".to_string()
    } else if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{} MB", bytes / (1024 * 1024))
    }
}

fn print_table(results: &[CellResult], transport: &TransportType) {
    println!("\n=== Throughput Benchmark ({:?}) ===\n", transport);
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
            .unwrap_or_else(|| "-".to_string());
        let p95 = r
            .p95_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let p99 = r
            .p99_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let p999 = r
            .p99_9_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());

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

    println!("Using {:?} transport", args.transport);
    println!(
        "count={}, payload_sizes={:?}, concurrency={:?}",
        args.count, args.payload_sizes, args.concurrency
    );

    let runtime_server = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let runtime_client = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let (peer_info_tx, peer_info_rx) = std::sync::mpsc::channel();

    // Channel for pipeline coordination: server signals done when target reached.
    let (done_tx, done_rx) = flume::bounded::<()>(1);

    let transport_type = args.transport.clone();

    let server_handle = std::thread::spawn(move || {
        runtime_server.block_on(async move {
            let transport = new_transport(&transport_type).await;
            let messenger = Messenger::builder()
                .add_transport(transport)
                .build()
                .await
                .unwrap();

            sleep(Duration::from_millis(100)).await;

            // "echo" handler: returns received payload unchanged.
            let echo_handler =
                Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload.clone()))).build();
            messenger.register_handler(echo_handler).unwrap();

            // "set_target" handler: client calls this (unary) before each pipeline
            // run to atomically reset the counter and set the expected message count.
            // The unary response guarantees the server has updated state before the
            // client starts firing messages.
            let counter = Arc::new(AtomicU64::new(0));
            let target = Arc::new(AtomicU64::new(u64::MAX));
            {
                let counter_for_set = Arc::clone(&counter);
                let target_for_set = Arc::clone(&target);
                let set_target_handler = Handler::unary_handler("set_target", move |ctx| {
                    let n: u64 = rmp_serde::from_slice(&ctx.payload)
                        .map_err(|e| anyhow::anyhow!("deserialize: {}", e))?;
                    counter_for_set.store(0, Ordering::SeqCst);
                    target_for_set.store(n, Ordering::SeqCst);
                    Ok(Some(bytes::Bytes::new()))
                })
                .build();
                messenger.register_handler(set_target_handler).unwrap();
            }

            // "count" handler: counts fire-and-forget messages, signals done.
            {
                let counter = Arc::clone(&counter);
                let target = Arc::clone(&target);
                let done_tx = done_tx.clone();

                let count_handler = Handler::am_handler("count", move |_ctx| {
                    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                    let t = target.load(Ordering::SeqCst);
                    if n >= t {
                        let _ = done_tx.try_send(());
                    }
                    Ok(())
                })
                .build();
                messenger.register_handler(count_handler).unwrap();
            }

            peer_info_tx.send(messenger.peer_info()).unwrap();
            println!("Server ready");

            std::future::pending::<()>().await;
        });
    });

    let server_peer_info = peer_info_rx.recv().unwrap();
    let transport_type = args.transport.clone();

    let client_handle = std::thread::spawn(move || -> Result<Vec<CellResult>> {
        runtime_client.block_on(async move {
            let transport = new_transport(&transport_type).await;
            let messenger = Messenger::builder()
                .add_transport(transport)
                .build()
                .await?;

            sleep(Duration::from_millis(100)).await;

            messenger.register_peer(server_peer_info.clone())?;
            let target = server_peer_info.instance_id();

            // Wait for connection + handshake.
            sleep(Duration::from_millis(500)).await;

            // Verify echo handler is reachable.
            messenger
                .unary("echo")?
                .raw_payload(Bytes::new())
                .instance(target)
                .send()
                .await?;

            let mut all_results = Vec::new();

            for &size in &args.payload_sizes {
                let payload = Bytes::from(vec![0u8; size]);

                // --- Sequential ---
                println!("\nSequential, payload={}", format_payload(size));
                let r = run_sequential(&messenger, target, payload.clone(), args.count).await?;
                all_results.push(r);

                // --- Concurrent ---
                for &c in &args.concurrency {
                    println!(
                        "Concurrent concurrency={}, payload={}",
                        c,
                        format_payload(size)
                    );
                    let r = run_concurrent(
                        Arc::clone(&messenger),
                        target,
                        payload.clone(),
                        args.count,
                        c,
                    )
                    .await?;
                    all_results.push(r);
                }

                // --- Pipeline ---
                println!("Pipeline, payload={}", format_payload(size));
                let r = run_pipeline(
                    &messenger,
                    target,
                    payload.clone(),
                    args.count,
                    done_rx.clone(),
                )
                .await?;
                all_results.push(r);
            }

            Ok(all_results)
        })
    });

    let results = client_handle.join().unwrap()?;
    print_table(&results, &args.transport);
    drop(server_handle);

    Ok(())
}
