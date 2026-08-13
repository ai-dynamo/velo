// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batched streaming over the Messenger mux, in the shape an LLM serving stack
//! produces it.
//!
//! # The deployment
//!
//! Five `Velo` nodes in one process, all on loopback TCP:
//!
//! - **Three anchor hosts** — frontends. Every in-flight request's response
//!   stream is an SPSC `StreamAnchor` living on one of them.
//! - **`--engines` token producers** (default 2) — decode engines.
//!
//! Each engine runs continuous batching. It admits requests until it is holding
//! `--max-batch` of them, and every iteration is one **forward pass**: exactly
//! one token on every active request's stream. A request whose token budget is
//! spent is finalized, its slot leaves the batch, and the next queued request
//! joins on the following pass — so the batch composition churns for the whole
//! run, ending with both engines draining to empty.
//!
//! A request begins life as an anchor created on a host, and reaches an engine
//! as a `StreamAnchorHandle`. Here that hand-off is an in-process channel; in a
//! real deployment it is the RPC that dispatches the request to an engine, and
//! the handle is the same 128 bits either way.
//!
//! # What it demonstrates
//!
//! A forward pass is X sends spread across X *different* streams, never many
//! frames queued on one. That is the case `streaming/BATCHING.md` § "Measured
//! results" singles out: per-stream write coalescing can only pack frames
//! sitting on the same stream, so here it has nothing to work with and the
//! ratio is exactly **1.00 — one token, one write**.
//!
//! The mux batches on the other axis. `StreamAnchorHandle` packs the
//! destination worker in its upper bits, so every token heading for one host —
//! whichever request it belongs to, and whether or not that request was in the
//! batch a moment ago — rides in one `_stream_batch` active message.
//!
//! Running it every way is the whole argument. One developer machine, defaults:
//!
//! ```text
//! --legacy                    476 tokens, 476 per-stream egress flushes   1.00 : 1
//! --flush-policy auto         476 tokens, 217 _stream_batch AMs           2.19 : 1
//! --flush-policy manual       476 tokens, 217 _stream_batch AMs           2.19 : 1
//! ```
//!
//! The 1.00 is not a number that could have come out otherwise — with a gap
//! between passes, each stream's egress pump wakes holding exactly one frame.
//!
//! The two mux rows agreeing is the point rather than an anticlimax: with a
//! millisecond between passes the batcher always keeps up, so writing at every
//! wake and writing once per pass are the same writes. What separates them is
//! what happens when that stops being true. At a serving-shaped depth, five
//! runs each:
//!
//! ```text
//! --max-batch 32 --requests 96 --flush-policy auto     4.88  5.08  5.08  5.14  4.67
//! --max-batch 32 --requests 96 --flush-policy manual   5.38  5.38  5.38  5.38  5.38
//! ```
//!
//! `manual` is both higher and *the same number every time*. Higher because a
//! batcher writing at every wake sometimes wakes mid-pass and writes half of
//! one; the same every time because what a batch holds stops depending on how
//! the runtime scheduled the batcher against the engine. That determinism is
//! the reason to reach for it — a serving deployment can derive its batch size
//! from its own fan-out instead of measuring it.
//!
//! `auto` wins on raw packing in the opposite regime, and it is worth seeing:
//! at `--pass-delay-ms 0` the engine runs flat out, the batcher falls behind,
//! and a batch starts absorbing the pass after it — 6.61 to 7.44 over five
//! runs, against 3.47 to 4.41 for `manual`. That surplus is throughput bought
//! with per-token latency, which for a decode engine is the wrong trade.
//!
//! Every number above, with its command and its unedited output, is in
//! [`batched_streaming.evidence.md`](batched_streaming.evidence.md) beside
//! this file.
//!
//! Either way the ratio tracks how many of an engine's active requests live on
//! the same host, so it climbs with the batch as long as there are enough
//! requests to keep the batch full:
//!
//! ```text
//! --max-batch 4                    1.36 : 1
//! --max-batch 8                    2.18 : 1
//! --max-batch 16 --requests 48     3.30 : 1
//! --max-batch 32 --requests 96     5.38 : 1
//! --max-batch 64 --requests 192    7.20 : 1
//! ```
//!
//! Raising `--max-batch` alone plateaus around 2.8 here, because 24 requests
//! cannot fill a batch of 32. That plateau is the point rather than a caveat:
//! what the mux packs is *concurrent requests sharing a destination*, so the
//! win is a function of the deployment's X/Y — and it keeps growing toward the
//! 256–1024 requests a real decode engine holds against a handful of frontends.
//!
//! Nothing else changes. Each host checks that its streams arrived in order, so
//! the run also shows that sharing a batch — with other requests, and with a
//! second engine's traffic to the same host — never disturbs a single stream.
//! And because the negotiated transport is printed at attach time, the example
//! doubles as a negotiation demo: `--legacy` is the same five nodes with
//! `MuxConfig::enabled` set back to `false`, which is the documented rollback.
//!
//! The flush policy is the other configuration on show. `manual` is the default
//! here — not in `MuxConfig`, where the default stays `auto` — because it is
//! the serving-correct mode and because it is only half a configuration: the
//! other half is the engine calling `velo.flush_batch()` after each forward
//! pass, which is the line worth copying out of this file.
//!
//! Run: `cargo run --example batched_streaming -- --engines 2 --requests 24`

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{
    FlushPolicy, MuxConfig, StreamAnchor, StreamAnchorHandle, StreamFrame, StreamSender,
};
use velo::{Velo, VeloBuilder};
use velo_examples::{TransportType, new_transport};

/// Anchor hosts. Fixed rather than a flag because the interesting ratio is
/// requests-per-host, and `--max-batch` already moves it.
const HOSTS: usize = 3;

/// The streaming transport a node advertises only when the mux is switched on.
const MUX_KEY: &str = velo::streaming::MESSENGER_MUX_KEY;

/// The streaming transport every node has: one TCP connection per stream.
const LEGACY_KEY: &str = "tcp-stream";

/// A bound on the whole run, so a bug reports itself instead of hanging.
const PATIENCE: Duration = Duration::from_secs(60);

#[derive(Parser, Debug)]
#[command(name = "batched_streaming")]
#[command(about = "Batched streaming over the Messenger mux, in an LLM-serving shape")]
struct Args {
    /// Token producers. Each maintains its own active batch.
    #[arg(long, default_value = "2")]
    engines: usize,

    /// Requests to serve. Each is one response stream, start to terminal.
    #[arg(long, default_value = "24")]
    requests: u32,

    /// Requests one engine may hold in its active batch at once.
    #[arg(long = "max-batch", default_value = "8")]
    max_batch: usize,

    /// Longest response, in tokens. Each request's budget is derived from its
    /// index, so run-to-run composition is identical.
    #[arg(long, default_value = "40")]
    tokens: u32,

    /// Gap between forward passes, standing in for the compute a decode step
    /// spends on the GPU.
    ///
    /// This matters more than it looks. With no gap the engine runs arbitrarily
    /// far ahead, several passes' tokens pile up on every stream at once, and
    /// *per-stream* coalescing starts packing them — which flatters the legacy
    /// path and hides the effect this example is about. A real engine is never
    /// that far ahead. Set it to 0 to watch the difference.
    #[arg(long = "pass-delay-ms", default_value = "1")]
    pass_delay_ms: u64,

    /// Run the identical workload with the mux switched off, on the legacy
    /// one-TCP-connection-per-stream path.
    #[arg(long, default_value_t = false)]
    legacy: bool,

    /// Who decides when a batch is written. Ignored under `--legacy`, which
    /// has no batcher to decide anything.
    #[arg(long = "flush-policy", value_enum, default_value_t = Flush::Manual)]
    flush_policy: Flush,
}

/// The two flush policies, as the example exposes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Flush {
    /// The batcher writes at the end of every wake — velo's default, and what
    /// every mux did before the policy was configurable.
    Auto,
    /// The engine writes, once per forward pass. The serving-correct mode, and
    /// this example's default.
    Manual,
}

impl Flush {
    fn policy(self) -> FlushPolicy {
        match self {
            Self::Auto => FlushPolicy::default(),
            Self::Manual => FlushPolicy::Manual,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Manual => "manual",
        }
    }
}

/// One decoded token on one request's response stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Token {
    /// Position in this request's output. The host checks it, which is how the
    /// example shows that sharing a batch with other requests — and with
    /// another engine's traffic to the same host — does not disturb the order
    /// of any one stream.
    step: u32,
    /// Stand-in for the decoded text, sized like a real token.
    text: String,
}

/// A request handed to an engine.
///
/// In a real deployment this crosses an RPC: the frontend creates the anchor
/// and passes its [`StreamAnchorHandle`] to whichever engine will serve the
/// request. Here it is an in-process channel, and the handle is doing exactly
/// the same job.
struct Request {
    /// Which host owns this request's anchor. The engine never needs it to
    /// send — `StreamAnchorHandle` already packs the destination worker, which
    /// is exactly what the mux buckets on — but the example uses it to report
    /// how many distinct hosts an engine ended up talking to.
    host: usize,
    handle: StreamAnchorHandle,
    budget: u32,
}

/// The knobs one engine runs under.
#[derive(Clone, Copy)]
struct EngineConfig {
    max_batch: usize,
    pass_delay: Duration,
    /// Whether the mux is installed, and therefore whether there is anything
    /// for a flush to write.
    mux: bool,
    flush: Flush,
}

/// A request occupying a slot in an engine's active batch.
struct Active {
    sender: StreamSender<Token>,
    budget: u32,
    remaining: u32,
}

#[derive(Default)]
struct EngineStats {
    requests: u64,
    tokens: u64,
    passes: u64,
    /// Sum of the batch occupancy at each forward pass, for the mean.
    occupancy: u64,
    hosts_touched: usize,
    /// Whichever counter measures this run's wire writes.
    writes: f64,
    /// Records those writes carried: frames for the legacy pump, packed mux
    /// records under the mux. Both are this engine's own output — the mux
    /// histogram is read at `direction="sent"`, which is the half of it this
    /// node packed rather than the half its hosts sent back.
    frames: f64,
}

#[derive(Default)]
struct HostStats {
    completed: u64,
    tokens: u64,
    terminals: u64,
}

/// One node, plus the registry its collectors were installed into.
///
/// Per node rather than per run, because the summary attributes writes to the
/// engine that made them and attaches to the host that answered them — one
/// shared registry would sum both away.
struct Node {
    velo: Arc<Velo>,
    registry: Registry,
}

impl Node {
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.registry)
    }

    /// Attaches this node answered successfully over `transport`.
    fn attaches_over(&self, transport: &str) -> f64 {
        self.snapshot().counter(
            "velo_streaming_anchor_operations_total",
            &[
                ("operation", "attach"),
                ("outcome", "success"),
                ("transport_scheme", transport),
            ],
        )
    }

    /// `_stream_batch` active messages this node's batchers handed to the
    /// messenger — one per write, however many requests' tokens rode in it.
    fn mux_batches_sent(&self) -> f64 {
        self.snapshot()
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    }

    /// Records this node packed into those messages. Read at `sent` because
    /// every mux node is also a receiver — credit rides back on
    /// `_stream_batch` — and an unlabelled sum would credit an engine with the
    /// records its hosts packed for it.
    fn mux_records_sent(&self) -> f64 {
        self.snapshot().histogram_sum(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "sent")],
        )
    }

    /// Batches the legacy per-stream egress pump handed to its socket.
    fn egress_flushes(&self) -> f64 {
        self.snapshot()
            .counter("velo_streaming_egress_flushes_total", &[])
    }

    /// Frames those flushes carried.
    fn frames_written(&self) -> f64 {
        self.snapshot()
            .counter("velo_streaming_frames_written_total", &[])
    }
}

/// Build a node on loopback with the mux either installed or rolled back.
///
/// `enabled: false` is the documented rollback, and is the same node as never
/// calling `messenger_mux` at all: nothing is registered, nothing is
/// advertised, and every attach negotiates the legacy path.
async fn node(mux_enabled: bool, flush: Flush) -> Result<Arc<Node>> {
    // A registry per node. Two `VeloMetrics::register` calls against one
    // registry would collide on collector names, and per-node registries are
    // what let the summary attribute writes to the engine that made them.
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);
    let builder: VeloBuilder = Velo::builder()
        .add_transport(new_transport(TransportType::Tcp, "batched_streaming").await?)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics);
    let velo = builder
        .messenger_mux(MuxConfig {
            enabled: mux_enabled,
            flush_policy: flush.policy(),
            ..MuxConfig::default()
        })?
        .build()
        .await?;
    Ok(Arc::new(Node { velo, registry }))
}

/// How many tokens request `id` will emit.
///
/// Derived from the index so the batch composition — who retires when, and
/// therefore who shares a forward pass with whom — is identical on every run.
fn budget(id: u32, longest: u32) -> u32 {
    1 + (id.wrapping_mul(7)) % longest.max(1)
}

// ---------------------------------------------------------------------------
// Engine — a token producer running continuous batching
// ---------------------------------------------------------------------------

/// Serve requests until the queue is drained and the active batch is empty.
///
/// The loop is the shape a decode engine runs: admit up to the cap, emit one
/// token for every admitted request, retire whoever finished, repeat. What
/// matters for this example is the middle step — a forward pass is X sends
/// spread across a handful of destinations, and nothing about it is bursty on
/// any single stream.
async fn engine(
    index: usize,
    node: Arc<Node>,
    rx: flume::Receiver<Request>,
    config: EngineConfig,
) -> Result<EngineStats> {
    let EngineConfig {
        max_batch,
        pass_delay,
        mux,
        flush,
    } = config;
    let mut stats = EngineStats::default();
    let mut touched = BTreeSet::new();
    let mut active: Vec<Active> = Vec::new();
    let mut queue_open = true;
    let mut announced = false;

    // Attaching is the only handshake in the run: one active message to the
    // host, whose answer names the streaming transport both sides settled on.
    let admit = async |request: Request,
                       active: &mut Vec<Active>,
                       touched: &mut BTreeSet<usize>,
                       announced: &mut bool|
           -> Result<()> {
        let sender = node.velo.attach_anchor::<Token>(request.handle).await?;
        if !*announced {
            *announced = true;
            // The sender carries the answer, so the engine reads its own
            // outcome — as a deployed one would, having no access to the
            // frontend's metrics.
            let key = sender
                .negotiated_transport()
                .map_or("none (same worker)", |key| key.as_str());
            println!("[engine {index}] first attach negotiated {key}");
        }
        touched.insert(request.host);
        active.push(Active {
            sender,
            budget: request.budget,
            remaining: request.budget,
        });
        Ok(())
    };

    loop {
        // Admit whatever is waiting, up to the cap.
        while queue_open && active.len() < max_batch {
            match rx.try_recv() {
                Ok(request) => {
                    admit(request, &mut active, &mut touched, &mut announced).await?;
                    stats.requests += 1;
                }
                Err(flume::TryRecvError::Empty) => break,
                Err(flume::TryRecvError::Disconnected) => queue_open = false,
            }
        }

        if active.is_empty() {
            if !queue_open {
                break;
            }
            // Nothing admitted and nothing queued yet: wait for the generator.
            match rx.recv_async().await {
                Ok(request) => {
                    admit(request, &mut active, &mut touched, &mut announced).await?;
                    stats.requests += 1;
                }
                Err(_) => queue_open = false,
            }
            continue;
        }

        // One forward pass: exactly one token on every active request, issued
        // back to back with nothing awaited in between. Per-stream coalescing
        // has nothing to work with — each stream contributes one frame — so
        // whatever packing happens has to happen across streams, bucketed by
        // the host they are heading for.
        stats.passes += 1;
        stats.occupancy += active.len() as u64;
        for slot in active.iter_mut() {
            let step = slot.budget - slot.remaining;
            slot.sender
                .send(Token {
                    step,
                    text: format!("tok-{step:04}"),
                })
                .await?;
            slot.remaining -= 1;
            stats.tokens += 1;
        }

        // Retire whoever spent their budget. `finalize` is the terminal the
        // host is waiting for; letting the sender drop instead would deliver
        // `Dropped` and read as an engine crash.
        let mut still_running = Vec::with_capacity(active.len());
        for slot in active.drain(..) {
            if slot.remaining == 0 {
                slot.sender.finalize()?;
            } else {
                still_running.push(slot);
            }
        }
        active = still_running;

        // The pass is complete, so write it: one `_stream_batch` to each host
        // this engine touched, carrying every token that pass produced for it.
        //
        // After `finalize` rather than before, so a retiring request's terminal
        // rides in the same batch as the tokens beside it instead of chasing
        // them in one of its own.
        //
        // Only under `--flush-policy manual`. The call is valid under `auto`
        // too — it forces a write ahead of the conditions — but the point of
        // running `auto` here is to see what the batcher does when nobody tells
        // it anything, so the example leaves it alone.
        if mux && flush == Flush::Manual {
            node.velo.flush_batch();
        }

        // The gap a real forward pass spends computing. It keeps each pass a
        // distinct event on the wire, which is the only way the ratio below
        // measures cross-stream packing rather than an engine running ahead.
        if !pass_delay.is_zero() {
            tokio::time::sleep(pass_delay).await;
        }
    }

    stats.hosts_touched = touched.len();
    if mux {
        stats.writes = node.mux_batches_sent();
        stats.frames = node.mux_records_sent();
    } else {
        stats.writes = node.egress_flushes();
        stats.frames = node.frames_written();
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Host — a frontend consuming the response streams it owns
// ---------------------------------------------------------------------------

/// Drain one request's response stream, checking its order as it goes.
async fn drain(id: u32, mut anchor: StreamAnchor<Token>) -> Result<(u64, bool)> {
    let mut step = 0u32;
    while let Some(frame) = anchor.next().await {
        match frame? {
            StreamFrame::Item(token) => {
                if token.step != step {
                    bail!(
                        "request {id} delivered step {} where {step} was due — sharing a \
                         batch must never disturb a single stream's order",
                        token.step
                    );
                }
                step += 1;
            }
            StreamFrame::Finalized => return Ok((u64::from(step), true)),
            other => bail!("request {id} saw an unexpected frame: {other:?}"),
        }
    }
    Ok((u64::from(step), false))
}

/// Consume every response stream this host owns until the generator is done
/// and all of them have terminated.
async fn host(rx: flume::Receiver<(u32, StreamAnchor<Token>)>) -> Result<HostStats> {
    let mut streams = JoinSet::new();
    while let Ok((id, anchor)) = rx.recv_async().await {
        streams.spawn(drain(id, anchor));
    }

    let mut stats = HostStats::default();
    while let Some(joined) = streams.join_next().await {
        let (tokens, terminal) = joined??;
        stats.tokens += tokens;
        if terminal {
            stats.terminals += 1;
            stats.completed += 1;
        }
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mux = !args.legacy;
    if args.engines == 0 || args.max_batch == 0 || args.requests == 0 {
        bail!("--engines, --max-batch and --requests must all be non-zero");
    }

    let expected_tokens: u64 = (0..args.requests)
        .map(|id| u64::from(budget(id, args.tokens)))
        .sum();
    let mode = if mux {
        format!("mux, flush-policy {}", args.flush_policy.label())
    } else {
        "legacy (one TCP connection per stream)".to_string()
    };
    println!(
        "batched_streaming: {HOSTS} anchor hosts, {} engine(s), {} requests, \
         max-batch {}, {expected_tokens} tokens, {}ms between passes\n\
         mode: {mode}",
        args.engines, args.requests, args.max_batch, args.pass_delay_ms
    );

    // Build the deployment. Hosts own anchors; engines produce tokens.
    let mut hosts = Vec::with_capacity(HOSTS);
    for _ in 0..HOSTS {
        hosts.push(node(mux, args.flush_policy).await?);
    }
    let mut engines = Vec::with_capacity(args.engines);
    for _ in 0..args.engines {
        engines.push(node(mux, args.flush_policy).await?);
    }

    // Engines and hosts know each other; hosts never stream to each other and
    // neither do engines, so the mesh is only the pairs that talk.
    for e in &engines {
        for h in &hosts {
            e.velo.register_peer(h.velo.peer_info())?;
            h.velo.register_peer(e.velo.peer_info())?;
        }
    }
    // An attach is an active message to `_anchor_attach`. Wait for each host's
    // handler to be visible rather than sleeping — a fixed settle is a race on
    // a loaded machine.
    for e in &engines {
        for h in &hosts {
            e.velo
                .wait_for_handler(h.velo.instance_id(), "_anchor_attach")
                .await?;
        }
    }

    let hosts = Arc::new(hosts);

    // Each host consumes the streams it owns.
    let mut host_txs = Vec::with_capacity(HOSTS);
    let mut host_tasks = Vec::with_capacity(HOSTS);
    for _ in 0..HOSTS {
        let (tx, rx) = flume::unbounded::<(u32, StreamAnchor<Token>)>();
        host_txs.push(tx);
        host_tasks.push(tokio::spawn(host(rx)));
    }

    // Each engine serves the requests routed to it.
    let mut engine_txs = Vec::with_capacity(args.engines);
    let mut engine_tasks = Vec::with_capacity(args.engines);
    for (index, e) in engines.iter().enumerate() {
        let (tx, rx) = flume::unbounded::<Request>();
        engine_txs.push(tx);
        engine_tasks.push(tokio::spawn(engine(
            index,
            Arc::clone(e),
            rx,
            EngineConfig {
                max_batch: args.max_batch,
                pass_delay: Duration::from_millis(args.pass_delay_ms),
                mux,
                flush: args.flush_policy,
            },
        )));
    }

    let started = Instant::now();

    // The request generator. Every request's response stream is an anchor on
    // one of the hosts; the engine only ever sees the handle.
    for id in 0..args.requests {
        let host_index = id as usize % HOSTS;
        let anchor = hosts[host_index].velo.create_anchor::<Token>();
        let request = Request {
            host: host_index,
            handle: anchor.handle(),
            budget: budget(id, args.tokens),
        };
        host_txs[host_index].send((id, anchor))?;
        engine_txs[id as usize % args.engines].send(request)?;
    }
    drop(engine_txs);
    drop(host_txs);

    let engine_stats = tokio::time::timeout(PATIENCE, futures::future::try_join_all(engine_tasks))
        .await
        .map_err(|_| anyhow::anyhow!("the engines did not drain within {PATIENCE:?}"))??
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let host_stats = tokio::time::timeout(PATIENCE, futures::future::try_join_all(host_tasks))
        .await
        .map_err(|_| anyhow::anyhow!("the hosts did not see every terminal within {PATIENCE:?}"))??
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let elapsed = started.elapsed();

    // -----------------------------------------------------------------------
    // Summary
    // -----------------------------------------------------------------------

    let expected_key = if mux { MUX_KEY } else { LEGACY_KEY };
    let negotiated: f64 = hosts.iter().map(|h| h.attaches_over(expected_key)).sum();
    if negotiated != f64::from(args.requests) {
        bail!(
            "expected all {} attaches to negotiate {expected_key}, but only {negotiated:.0} did",
            args.requests
        );
    }

    // The engines' own packing, which is only separable because the record
    // histogram carries a direction: an engine also receives batches — that is
    // how credit comes back — and their records would otherwise be summed in
    // here. Every token is a record, plus the heartbeats, slot opens and
    // terminals that no token count sees.
    let packed: f64 = engine_stats.iter().map(|s| s.frames).sum();
    if mux && packed < expected_tokens as f64 {
        bail!(
            "the engines packed {packed:.0} records for {expected_tokens} tokens — \
             velo_streaming_mux_records_per_batch{{direction=\"sent\"}} is not \
             counting what this engine sent"
        );
    }

    let completed: u64 = host_stats.iter().map(|h| h.completed).sum();
    let received: u64 = host_stats.iter().map(|h| h.tokens).sum();
    if completed != u64::from(args.requests) || received != expected_tokens {
        bail!(
            "expected {} requests and {expected_tokens} tokens to complete, \
             saw {completed} and {received}",
            args.requests
        );
    }

    println!("\n=== engines ({mode}) ===");
    let write_unit = if mux {
        "_stream_batch AMs"
    } else {
        "egress flushes"
    };
    println!(
        "{:<8} {:>7} {:>8} {:>7} {:>9} {:>6} {:>18} {:>12}",
        "engine", "reqs", "tokens", "passes", "mean batch", "hosts", write_unit, "tokens/write"
    );
    for (index, s) in engine_stats.iter().enumerate() {
        println!(
            "{:<8} {:>7} {:>8} {:>7} {:>9.2} {:>6} {:>18.0} {:>12.2}",
            index,
            s.requests,
            s.tokens,
            s.passes,
            s.occupancy as f64 / s.passes.max(1) as f64,
            s.hosts_touched,
            s.writes,
            s.tokens as f64 / s.writes.max(1.0)
        );
    }

    println!("\n=== hosts ===");
    println!(
        "{:<8} {:>10} {:>8} {:>10}",
        "host", "completed", "tokens", "terminals"
    );
    for (index, s) in host_stats.iter().enumerate() {
        println!(
            "{:<8} {:>10} {:>8} {:>10}",
            index, s.completed, s.tokens, s.terminals
        );
    }

    let writes: f64 = engine_stats.iter().map(|s| s.writes).sum();
    let tokens: u64 = engine_stats.iter().map(|s| s.tokens).sum();
    let passes: u64 = engine_stats.iter().map(|s| s.passes).sum();
    let mean_batch = tokens as f64 / passes.max(1) as f64;
    let ratio = tokens as f64 / writes.max(1.0);

    let active_per_host = mean_batch / HOSTS as f64;
    println!("\n=== totals ===");
    println!("  mode                 {mode}");
    println!("  tokens streamed      {tokens}");
    println!("  forward passes       {passes}");
    println!("  wire writes          {writes:.0}  ({write_unit})");
    println!("  tokens per write     {ratio:.2} : 1");
    println!(
        "  active per host      {active_per_host:.2}  (mean batch {mean_batch:.2} over {HOSTS} hosts)"
    );
    println!(
        "  elapsed              {:.0} ms",
        elapsed.as_secs_f64() * 1e3
    );

    // -----------------------------------------------------------------------
    // What to notice
    // -----------------------------------------------------------------------

    if mux {
        // Zero here is the negative half of the claim: the legacy per-stream
        // TCP transport is still registered on every node — that is what keeps
        // a mux node able to serve peers without one — but no stream dialled
        // it, so it wrote nothing.
        let legacy: f64 = engines.iter().map(|e| e.egress_flushes()).sum();
        println!(
            "\n{tokens} tokens crossed the wire in {writes:.0} active messages — {ratio:.2} \
             tokens per write, and\nthe per-stream TCP path wrote {legacy:.0} times because no \
             stream ever dialled it. Every token\nstill arrived in order on its own stream: the \
             batching is on the destination axis, not\nthe stream axis, so it packs a forward \
             pass that per-stream coalescing cannot touch."
        );
        match args.flush_policy {
            Flush::Manual => println!(
                "\nThe engine wrote each pass itself: flush_batch() after the last send, one \
                 batch to\neach host it touched, carrying that host's whole share of the pass. \
                 Run it again and\nthe ratio comes out the same — what a batch holds is a \
                 property of the deployment,\nnot of how the runtime scheduled the batcher \
                 against the engine. No token waits for\nthe pass behind it, which is the part \
                 that matters for time-to-next-token.\nRun --flush-policy auto to let the \
                 batcher decide instead, and watch the number move."
            ),
            Flush::Auto => println!(
                "\nThe batcher wrote whenever the last batch was admitted, with nobody telling \
                 it to.\nThat number moves run to run, because what lands in a batch depends on \
                 how the\nruntime scheduled the batcher against the engine: it can wake \
                 mid-pass and write half\nof one, or fall behind and pack the pass after it. \
                 Try --pass-delay-ms 0 to see the\nsecond effect at its strongest — packing \
                 across passes is throughput bought with\nper-token latency. Run --flush-policy \
                 manual for one write per pass instead."
            ),
        }
        println!(
            "\n(active per host, {active_per_host:.2}, averages over all {HOSTS} hosts including \
             the ones a\ngiven pass had nothing for, so it reads a little under the tokens each \
             written batch\nactually carried.)"
        );
        println!("Run with --legacy for the same workload at one write per token.");
    } else {
        // BATCHING.md defines the batching ratio as
        // `frames_written / egress_flushes`, which is the canonical number and
        // sits a shade above the token ratio: the frame counter also sees the
        // heartbeats and the per-stream terminal, which are not tokens.
        let frames: f64 = engine_stats.iter().map(|s| s.frames).sum();
        println!(
            "\n{writes:.0} writes for {tokens} tokens — {ratio:.2} : 1. BATCHING.md's own \
             ratio,\nframes_written / egress_flushes, reads {:.2} : 1 here, higher only because \
             it counts\nthe heartbeats and terminals that a token count does not. Either way it \
             is the\nlimitation that document measures rather than a failure: a forward pass \
             puts one\nframe on each of many different streams, and per-stream coalescing can \
             only pack\nframes queued on the same one. Run without --legacy to bucket them by \
             destination.",
            frames / writes.max(1.0)
        );
    }

    Ok(())
}
