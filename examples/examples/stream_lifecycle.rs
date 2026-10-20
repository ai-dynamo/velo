// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What it costs a producer to learn that a finished stream's output has left
//! its node.
//!
//! Two `Velo` nodes in one process over loopback TCP, on the messenger mux. The
//! producer opens `--streams` short streams, `--concurrency` at a time, each
//! carrying `--items` items of 16 bytes, and finalizes each one. `--mode`
//! decides what the producer does after `finalize`:
//!
//! - `plain`: nothing. It moves on at once, and velo may still hold the
//!   stream's output.
//! - `retired`: it awaits `StreamSender::retired()`, which resolves once velo
//!   on the producer holds none of the stream's output.
//! - `ack`: the consumer sends one active message after it reads `Finalized`,
//!   and the producer waits for it. This is what an application builds when
//!   velo gives no signal: one more message per stream, and a table of waiters
//!   on the producer.
//!
//! It prints one `RESULT` line: streams per second, active messages per stream
//! from each side, and how long the producer held each stream after
//! `finalize` (p50, p90, p99). Wall time moves with whatever else the machine
//! runs, so compare modes and builds by the process's instruction count
//! (`/usr/bin/time -l` on macOS, `perf stat -e instructions` on Linux), divided
//! by `--streams`. `docs/src/operations/benchmarking.md` has the commands.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{MuxConfig, Retirement, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Handler, Velo};

#[derive(Parser, Debug)]
#[command(about = "Measure what it costs to learn that a stream's output has left the producer")]
struct Args {
    /// What the producer does after `finalize`.
    #[arg(long, value_enum, default_value_t = Mode::Plain)]
    mode: Mode,
    /// Streams in the measured run.
    #[arg(long, default_value_t = 200_000)]
    streams: usize,
    /// Streams open at once.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// Items per stream, 16 bytes each.
    #[arg(long, default_value_t = 4)]
    items: usize,
    /// Streams run before the measured run, to warm the connections and the
    /// batchers. Their messages are not counted.
    #[arg(long, default_value_t = 2_000)]
    warmup: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Mode {
    Plain,
    Retired,
    Ack,
}

const PATIENCE: Duration = Duration::from_secs(10);
const ACK_HANDLER: &str = "stream_lifecycle_ack";

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn node() -> anyhow::Result<(Velo, Registry)> {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let tcp = Arc::new(TcpTransportBuilder::new().from_listener(listener)?.build()?);
    let velo = Velo::builder()
        .add_transport(tcp)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics)
        .messenger_mux(MuxConfig::default())?
        .build()
        .await?;
    Ok((velo, registry))
}

/// Active messages this node sent, which is how the `ack` mode's extra message
/// shows up.
fn messages_sent(registry: &Registry) -> f64 {
    MetricSnapshot::from_registry(registry).counter(
        "velo_transport_frames_total",
        &[
            ("direction", "outbound"),
            ("message_type", "message"),
            ("outcome", "accepted"),
        ],
    )
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    sorted[((sorted.len() as f64 - 1.0) * p) as usize]
}

async fn run(args: Args) -> anyhow::Result<()> {
    let (producer, producer_metrics) = node().await?;
    let (consumer, consumer_metrics) = node().await?;
    producer.register_peer(consumer.peer_info())?;
    consumer.register_peer(producer.peer_info())?;

    // The `ack` mode's waiters. A table keyed by stream is what that mode
    // costs an application, so it is part of what is measured.
    let waiters = Arc::new(dashmap::DashMap::<u64, tokio::sync::oneshot::Sender<()>>::new());
    {
        let waiters = waiters.clone();
        producer.register_handler(
            Handler::am_handler(ACK_HANDLER, move |ctx| {
                let id = u64::from_be_bytes(ctx.payload[..8].try_into()?);
                if let Some((_, tx)) = waiters.remove(&id) {
                    let _ = tx.send(());
                }
                Ok(())
            })
            .build(),
        )?;
    }
    for (a, b) in [(&producer, &consumer), (&consumer, &producer)] {
        tokio::time::timeout(PATIENCE, a.wait_for_handler(b.instance_id(), "_anchor_attach"))
            .await??;
    }
    tokio::time::timeout(
        PATIENCE,
        consumer.wait_for_handler(producer.instance_id(), ACK_HANDLER),
    )
    .await??;

    let lane = Lane {
        producer: producer.clone(),
        consumer: consumer.clone(),
        waiters,
        mode: args.mode,
        items: args.items,
    };
    lane.run(args.warmup, args.concurrency).await?;
    let (p0, c0) = (
        messages_sent(&producer_metrics),
        messages_sent(&consumer_metrics),
    );
    let started = Instant::now();
    let mut held = lane.run(args.streams, args.concurrency).await?;
    let wall = started.elapsed();
    let (p1, c1) = (
        messages_sent(&producer_metrics),
        messages_sent(&consumer_metrics),
    );
    held.sort_unstable();
    println!(
        "RESULT mode={:?} streams={} concurrency={} items={} wall_ms={} streams_per_s={:.0} \
         producer_msgs_per_stream={:.3} consumer_msgs_per_stream={:.3} held_us p50={} p90={} p99={}",
        args.mode,
        args.streams,
        args.concurrency,
        args.items,
        wall.as_millis(),
        args.streams as f64 / wall.as_secs_f64(),
        (p1 - p0) / args.streams as f64,
        (c1 - c0) / args.streams as f64,
        percentile(&held, 0.5),
        percentile(&held, 0.9),
        percentile(&held, 0.99),
    );
    Ok(())
}

#[derive(Clone)]
struct Lane {
    producer: Velo,
    consumer: Velo,
    waiters: Arc<dashmap::DashMap<u64, tokio::sync::oneshot::Sender<()>>>,
    mode: Mode,
    items: usize,
}

impl Lane {
    /// Run `streams` streams, `concurrency` at a time, and return how long the
    /// producer held each one after `finalize`, in microseconds.
    async fn run(&self, streams: usize, concurrency: usize) -> anyhow::Result<Vec<u128>> {
        let permits = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut tasks = tokio::task::JoinSet::new();
        for id in 0..streams as u64 {
            let permit = permits.clone().acquire_owned().await?;
            let lane = self.clone();
            tasks.spawn(async move {
                let held = lane.stream(id).await;
                drop(permit);
                held
            });
        }
        let mut held = Vec::with_capacity(streams);
        while let Some(result) = tasks.join_next().await {
            held.push(result??);
        }
        Ok(held)
    }

    async fn stream(&self, id: u64) -> anyhow::Result<u128> {
        let payload = Bytes::from_static(b"0123456789abcdef");
        let mut anchor = self.consumer.create_anchor::<Bytes>();
        let handle = anchor.handle();
        let ticket = self
            .consumer
            .prebind_anchor(handle)
            .ok_or_else(|| anyhow::anyhow!("stream {id}: no ticket"))?;
        let consumer = self.consumer.clone();
        let ack_to = self.producer.instance_id();
        let mode = self.mode;
        let reader = tokio::spawn(async move {
            loop {
                match anchor.next_bytes().await {
                    Some(Ok(StreamFrame::Item(_))) => {}
                    Some(Ok(StreamFrame::Finalized)) => break,
                    other => anyhow::bail!("stream {id}: unexpected {other:?}"),
                }
            }
            if mode == Mode::Ack {
                consumer
                    .am_send(ACK_HANDLER)?
                    .raw_payload(Bytes::copy_from_slice(&id.to_be_bytes()))
                    .instance(ack_to)
                    .send()
                    .await?;
            }
            Ok(())
        });
        let ack = (mode == Mode::Ack).then(|| {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.waiters.insert(id, tx);
            rx
        });
        let sender = self
            .producer
            .open_anchor_stream::<Bytes>(handle, ticket)
            .await?;
        for _ in 0..self.items {
            sender.send_bytes(&payload).await?;
        }
        // Only the mode that waits asks for the future, so `plain` measures a
        // stream nobody watches.
        let retired = (mode == Mode::Retired).then(|| sender.retired());
        let finalized = Instant::now();
        sender.finalize()?;
        match mode {
            Mode::Plain => {}
            Mode::Retired => {
                let retired = retired.expect("retired mode asked for it");
                let outcome = tokio::time::timeout(PATIENCE, retired).await?;
                anyhow::ensure!(outcome == Retirement::Sent, "stream {id}: {outcome:?}");
            }
            Mode::Ack => {
                tokio::time::timeout(PATIENCE, ack.expect("ack mode has a waiter")).await??;
            }
        }
        let held = finalized.elapsed().as_micros();
        tokio::time::timeout(PATIENCE, reader).await???;
        Ok(held)
    }
}
