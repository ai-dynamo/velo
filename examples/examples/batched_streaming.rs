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
//! Running it both ways is the whole argument. One developer machine, defaults:
//!
//! ```text
//! --legacy    476 tokens, 476 per-stream egress flushes   1.00 : 1
//! (default)   476 tokens, 217 _stream_batch AMs           2.19 : 1
//! ```
//!
//! The 1.00 is not a number that could have come out otherwise — with a gap
//! between passes, each stream's egress pump wakes holding exactly one frame.
//!
//! The mux ratio tracks how many of an engine's active requests live on the
//! same host, so it climbs with the batch as long as there are enough requests
//! to keep the batch full:
//!
//! ```text
//! --max-batch 4                    1.36 : 1
//! --max-batch 8                    2.18 : 1
//! --max-batch 16 --requests 48     3.30 : 1
//! --max-batch 32 --requests 96     5.08 : 1
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
//! Run: `cargo run --example batched_streaming -- --engines 2 --requests 24`

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::Parser;
use futures::StreamExt;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{MuxConfig, StreamAnchor, StreamAnchorHandle, StreamFrame, StreamSender};
use velo::{Velo, VeloBuilder};
use velo_examples::{TransportType, new_transport};

/// Anchor hosts. Fixed rather than a flag because the interesting ratio is
/// requests-per-host, and `--max-batch` already moves it.
const HOSTS: usize = 3;

/// The streaming transport a node advertises only when the mux is switched on.
const MUX_KEY: &str = "messenger-mux-v1";

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
    /// Frames those writes carried. Legacy only — the mux's per-batch record
    /// histogram is not split by direction, so it cannot be read cleanly here.
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
/// The registry is here for one reason: **the negotiated transport is not
/// returned by any API**. `attach_anchor` hands back a `StreamSender` that says
/// nothing about the wire beneath it, so the only place the outcome surfaces is
/// a label on the receiving node's attach counter.
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
async fn node(mux_enabled: bool) -> Result<Arc<Node>> {
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
    hosts: Arc<Vec<Arc<Node>>>,
    rx: flume::Receiver<Request>,
    max_batch: usize,
    pass_delay: Duration,
    mux: bool,
) -> Result<EngineStats> {
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
            // The negotiated key, read off the host's attach counter because
            // nothing in the API hands it back.
            let key = if hosts[request.host].attaches_over(MUX_KEY) > 0.0 {
                MUX_KEY
            } else {
                LEGACY_KEY
            };
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
    println!(
        "batched_streaming: {HOSTS} anchor hosts, {} engine(s), {} requests, \
         max-batch {}, {expected_tokens} tokens, {}ms between passes, mux={mux}",
        args.engines, args.requests, args.max_batch, args.pass_delay_ms
    );

    // Build the deployment. Hosts own anchors; engines produce tokens.
    let mut hosts = Vec::with_capacity(HOSTS);
    for _ in 0..HOSTS {
        hosts.push(node(mux).await?);
    }
    let mut engines = Vec::with_capacity(args.engines);
    for _ in 0..args.engines {
        engines.push(node(mux).await?);
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
            Arc::clone(&hosts),
            rx,
            args.max_batch,
            Duration::from_millis(args.pass_delay_ms),
            mux,
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

    let completed: u64 = host_stats.iter().map(|h| h.completed).sum();
    let received: u64 = host_stats.iter().map(|h| h.tokens).sum();
    if completed != u64::from(args.requests) || received != expected_tokens {
        bail!(
            "expected {} requests and {expected_tokens} tokens to complete, \
             saw {completed} and {received}",
            args.requests
        );
    }

    println!("\n=== engines ({expected_key}) ===");
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

    println!("\n=== totals ===");
    println!("  tokens streamed      {tokens}");
    println!("  forward passes       {passes}");
    println!("  wire writes          {writes:.0}  ({write_unit})");
    println!("  tokens per write     {ratio:.2} : 1");
    println!(
        "  active per host      {:.2}  (mean batch {mean_batch:.2} over {HOSTS} hosts)",
        mean_batch / HOSTS as f64
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
             pass that per-stream coalescing cannot touch.\nRun with --legacy for the same \
             workload at one write per token."
        );
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
