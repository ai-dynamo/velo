// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Anchor setup cost, in isolation.
//!
//! Drives only the anchor path: create an SPSC anchor, attach a sender on the
//! same worker, finalize, and read the terminal. One `Velo` node, and every
//! attach is local, so no frame crosses a transport. The numbers show what
//! anchor create, attach and finish cost, and what an unattached anchor holds
//! while it waits.
//!
//! ```bash
//! cargo run --release --example anchor_setup_bench -- --tasks 64 --streams 20000
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use velo::Velo;
use velo::StreamFrame;

#[derive(Parser, Debug)]
struct Args {
    /// Tokio worker threads.
    #[arg(long, default_value_t = 8)]
    threads: usize,
    /// Concurrent tasks that each run a loop of stream lifecycles.
    #[arg(long, default_value_t = 64)]
    tasks: usize,
    /// Stream lifecycles per task.
    #[arg(long, default_value_t = 20_000)]
    streams: usize,
    /// Unattached anchors to create and hold for the memory reading.
    #[arg(long, default_value_t = 200_000)]
    held: usize,
}

/// Resident set size of this process, in MiB.
///
/// Read from `VmRSS`, which the kernel reports in KiB. `/proc/self/statm`
/// counts pages, and a fixed page size misreads hosts with 64 KiB pages.
fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kib: f64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
        .unwrap_or(0.0);
    kib / 1024.0
}

async fn lifecycles(node: Velo, streams: usize) -> Result<Vec<u64>> {
    let mut nanos = Vec::with_capacity(streams);
    for _ in 0..streams {
        let start = Instant::now();
        let mut anchor = node.create_anchor::<u32>();
        let sender = node.attach_anchor::<u32>(anchor.handle()).await?;
        sender.finalize()?;
        match anchor.next().await {
            Some(Ok(StreamFrame::Finalized)) => {}
            other => anyhow::bail!("expected Finalized, got {other:?}"),
        }
        nanos.push(start.elapsed().as_nanos() as u64);
    }
    Ok(nanos)
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index] as f64 / 1000.0
}

fn runtime(threads: usize) -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?)
}

/// One node on a loopback TCP transport. The transport carries nothing
/// here: a `Velo` needs one to build.
async fn node() -> Result<Velo> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let tcp = velo::transports::tcp::TcpTransportBuilder::new()
        .from_listener(listener)?
        .build()?;
    Velo::builder().add_transport(Arc::new(tcp)).build().await
}

fn main() -> Result<()> {
    let args = Args::parse();

    // What unattached anchors hold while they wait. First, on a runtime and a
    // node of its own, so no task or allocation of the lifecycle phase is
    // counted.
    runtime(args.threads)?.block_on(async {
        let node = node().await?;
        let metrics = tokio::runtime::Handle::current().metrics();
        let tasks_before = metrics.num_alive_tasks();
        let rss_before = rss_mib();
        let start = Instant::now();
        let held: Vec<_> = (0..args.held)
            .map(|_| node.create_anchor::<u32>())
            .collect();
        let create = start.elapsed();
        // Let any spawned per-anchor tasks reach their first poll.
        tokio::time::sleep(Duration::from_millis(100)).await;
        println!(
            "unattached: {} anchors: create {:.0} ns each, +{} alive tasks, +{:.1} MiB RSS",
            held.len(),
            create.as_nanos() as f64 / held.len() as f64,
            metrics.num_alive_tasks() as i64 - tasks_before as i64,
            rss_mib() - rss_before,
        );
        Ok::<_, anyhow::Error>(())
    })?;

    // Lifecycle throughput and latency.
    runtime(args.threads)?.block_on(async {
        let node = node().await?;
        let start = Instant::now();
        let handles: Vec<_> = (0..args.tasks)
            .map(|_| tokio::spawn(lifecycles(node.clone(), args.streams)))
            .collect();
        let mut nanos = Vec::with_capacity(args.tasks * args.streams);
        for handle in handles {
            nanos.extend(handle.await??);
        }
        let elapsed = start.elapsed();
        nanos.sort_unstable();
        let total = nanos.len();
        println!(
            "lifecycle: {total} streams, {} tasks, {} threads: {:.0} streams/s, p50 {:.2} us, p99 {:.2} us, p99.9 {:.2} us, p99.99 {:.2} us",
            args.tasks,
            args.threads,
            total as f64 / elapsed.as_secs_f64(),
            percentile(&nanos, 0.50),
            percentile(&nanos, 0.99),
            percentile(&nanos, 0.999),
            percentile(&nanos, 0.9999),
        );
        Ok(())
    })
}
