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
    done_rx: flume::Receiver<()>,
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
    tokio::time::timeout(Duration::from_secs(30), done_rx.recv_async())
        .await
        .map_err(|_| anyhow::anyhow!("pipeline warmup timed out"))?
        .ok();

    set_target(velo, target, count).await?;
    let start = Instant::now();
    for _ in 0..count {
        velo.am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    tokio::time::timeout(Duration::from_secs(60), done_rx.recv_async())
        .await
        .map_err(|_| anyhow::anyhow!("pipeline run did not complete: timed out"))?
        .map_err(|e| anyhow::anyhow!("pipeline done channel error: {e}"))?;

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
        "Mode", "Payload", "Concurrency", "Msgs/sec", "MB/sec", "p50 µs", "p95 µs", "p99 µs",
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
        let p50 = r.p50_us.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let p95 = r.p95_us.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
        let p99 = r.p99_us.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
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
        "count={}, payload_sizes={:?}, concurrency={:?}",
        args.count, args.payload_sizes, args.concurrency
    );

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
            let transport = new_transport(transport_type, "throughput")
                .await
                .expect("build server transport");
            let velo = Velo::builder()
                .add_transport(transport)
                .build()
                .await
                .expect("build server velo");

            sleep(Duration::from_millis(100)).await;

            let echo_handler =
                Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload.clone()))).build();
            velo.register_handler(echo_handler).unwrap();

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
                velo.register_handler(set_target_handler).unwrap();
            }

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
                velo.register_handler(count_handler).unwrap();
            }

            peer_info_tx.send(velo.peer_info()).unwrap();
            println!("Server ready");
            std::future::pending::<()>().await;
        });
    });

    let server_peer_info = peer_info_rx.recv().unwrap();
    let transport_type = args.tx.transport;

    let client_handle = std::thread::spawn(move || -> Result<Vec<CellResult>> {
        runtime_client.block_on(async move {
            let transport = new_transport(transport_type, "throughput").await?;
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
                all_results
                    .push(run_sequential(&velo, target, payload.clone(), args.count).await?);

                for &c in &args.concurrency {
                    println!(
                        "Concurrent concurrency={c}, payload={}",
                        format_payload(size)
                    );
                    all_results.push(
                        run_concurrent(
                            Arc::clone(&velo),
                            target,
                            payload.clone(),
                            args.count,
                            c,
                        )
                        .await?,
                    );
                }

                println!("Pipeline, payload={}", format_payload(size));
                all_results.push(
                    run_pipeline(&velo, target, payload.clone(), args.count, done_rx.clone())
                        .await?,
                );
            }

            Ok(all_results)
        })
    });

    let results = client_handle.join().unwrap()?;
    print_table(&results, args.tx.transport);
    drop(server_handle);
    Ok(())
}
