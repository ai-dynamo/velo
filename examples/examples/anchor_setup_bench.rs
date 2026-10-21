// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Anchor setup cost, in isolation.
//!
//! Drives only the anchor path: create an SPSC anchor, attach a sender on the
//! same worker, finalize, and read the terminal. No network and no mux, so
//! the numbers show what anchor create, attach and finish cost, and what an
//! unattached anchor holds while it waits.
//!
//! ```bash
//! cargo run --release --example anchor_setup_bench -- --tasks 64 --streams 20000
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use futures::future::BoxFuture;
use velo::WorkerId;
use velo::ext::{TransportKey, WorkerAddress};
use velo::streaming::{AnchorManager, FrameTransport, StreamFrame};

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

/// A transport that is never used: every attach here is on the same worker.
struct NoTransport;

impl FrameTransport for NoTransport {
    fn key(&self) -> TransportKey {
        TransportKey::new("none")
    }

    fn address(&self) -> WorkerAddress {
        WorkerAddress::empty()
    }

    fn bind(&self, _: u64, _: u64) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
        Box::pin(async { anyhow::bail!("not used") })
    }

    fn connect(
        &self,
        _: WorkerId,
        _: u64,
        _: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async { anyhow::bail!("not used") })
    }
}

/// Resident set size of this process, in MiB.
fn rss_mib() -> f64 {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let pages: f64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0.0);
    pages * 4096.0 / (1024.0 * 1024.0)
}

async fn lifecycles(manager: Arc<AnchorManager>, streams: usize) -> Result<Vec<u64>> {
    let mut nanos = Vec::with_capacity(streams);
    for _ in 0..streams {
        let start = Instant::now();
        let mut anchor = manager.create_anchor::<u32>();
        let sender = manager.attach_stream_anchor::<u32>(anchor.handle()).await?;
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

fn manager() -> Arc<AnchorManager> {
    Arc::new(AnchorManager::new(
        WorkerId::from_u64(1),
        Arc::new(NoTransport),
    ))
}

fn main() -> Result<()> {
    let args = Args::parse();

    // What unattached anchors hold while they wait. First, on a runtime and a
    // manager of its own, so no task or allocation of the lifecycle phase is
    // counted.
    runtime(args.threads)?.block_on(async {
        let manager = manager();
        let metrics = tokio::runtime::Handle::current().metrics();
        let tasks_before = metrics.num_alive_tasks();
        let rss_before = rss_mib();
        let start = Instant::now();
        let held: Vec<_> = (0..args.held)
            .map(|_| manager.create_anchor::<u32>())
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
    });

    // Lifecycle throughput and latency.
    runtime(args.threads)?.block_on(async {
        let manager = manager();
        let start = Instant::now();
        let handles: Vec<_> = (0..args.tasks)
            .map(|_| tokio::spawn(lifecycles(Arc::clone(&manager), args.streams)))
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
