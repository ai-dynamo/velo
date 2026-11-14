// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Response-plane load harness — the rig for measuring velo's streaming mux
//! against the shape Dynamo's response-plane PRs benchmark.
//!
//! # Why this exists
//!
//! `batched_streaming` demonstrates *that* the mux batches, and reports one
//! number: tokens per wire write. It is fixed at three anchor hosts and a few
//! dozen requests, and it measures no latency at all.
//!
//! That is not enough to settle the question this harness was built for.
//! Three frontend-side costs are claimed to scale as `O(peers x slots)`:
//!
//! 1. the credit sweep walking every slot of every peer at 500 Hz,
//! 2. per-stream tokio tasks and a per-frame timer in `reader_pump`,
//! 3. one extra runqueue traversal per token.
//!
//! Every one of those was derived by reading code, not by measurement, and an
//! attempt to measure the first with `batched_streaming` failed for a
//! structural reason: at three peers the sweep is free, so the claim rests
//! entirely on a scaling factor that example cannot reach. **This harness
//! exists to reach it.**
//!
//! # What it measures
//!
//! Per request, at the consumer: time-to-first-token and inter-token latency,
//! as HDR histograms, so p50/p95/p99 are reported rather than a mean. Per run:
//! completed requests per second, tokens per wire write, and process CPU
//! seconds split into user and system. Those are the columns Dynamo's table
//! reports, so the two can be read side by side.
//!
//! # The experiment it was built for
//!
//! `--credit-sweep-interval-ms` is the A/B. Hold everything else fixed, run it
//! at 2 (the pre-drain-hook cadence; velo's own default is 200) and again at
//! 500, and the CPU difference is the sweep's cost with nothing else moving.
//! That holds only while no stream outruns its credit window: the published
//! figures were taken at a window of 256 with streams of at most 64 tokens,
//! and `--initial-credit 256` reproduces that. At the default window of 32, streams longer than 32
//! tokens need grants, and the sweep also becomes a credit backstop.
//!
//! **Scale `--engines`, not `--anchor-hosts`.** The sweep walks a node's
//! *ingress* peers — the peers it receives batches from — so an anchor host's
//! peer count is the number of engines streaming to it, mirroring Dynamo's 512
//! workers against 2 frontends. `--anchor-hosts` moves the engines' own ingress
//! count, which is the smaller side of the same product.
//!
//! ```text
//! # Does the 500 Hz sweep cost anything at 128 ingress peers?
//! response_plane_bench --anchor-hosts 2 --engines 128 --requests 2000 --credit-sweep-interval-ms 2 --initial-credit 256
//! response_plane_bench --anchor-hosts 2 --engines 128 --requests 2000 --credit-sweep-interval-ms 500 --initial-credit 256
//! ```
//!
//! # Honest limits
//!
//! **One process, loopback.** Every node runs in this process, so reported CPU
//! is the whole topology's, not one frontend's. That is fine for the sweep A/B,
//! where only one frontend-side parameter moves and the delta is therefore
//! attributable, and it is *not* fine for a claim about absolute frontend CPU
//! per request. Dynamo's 36.56 ms/req is a number for a frontend process alone;
//! do not compare this harness's CPU column against it.
//!
//! **Loopback removes wire time**, which exaggerates the syscall term. That
//! cuts in the mux's favour, as `docs/src/concepts/batched-streaming.md` already says.
//!
//! **Request dispatch is an in-process channel**, standing in for the request
//! plane, exactly as `batched_streaming` does. The *response* path is real:
//! anchors live on different worker IDs from the engines, so every attach goes
//! over the wire and every token crosses a socket.
//!
//! Multi-process and multi-node are the natural next step and are deliberately
//! not built here — this harness answers the question that blocks the
//! optimisation work, and that question is answerable in one process.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use futures::StreamExt;
use hdrhistogram::Histogram;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{
    FlushPolicy, MuxConfig, StreamAnchor, StreamAnchorHandle, StreamFrame, StreamSender,
};
use velo::{Velo, VeloBuilder};
use velo_examples::arm::{ArmArgs, ProcStats, mux_counters};
use velo_examples::{TransportType, new_transport};

/// Longest response any request may ask for. Bounds the histograms.
const MAX_TOKENS: u32 = 4096;
/// How long the whole run may take before it is called hung.
const PATIENCE: Duration = Duration::from_secs(600);

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Response-plane load harness: TTFT/ITL percentiles, throughput and CPU for velo's streaming mux"
)]
struct Args {
    /// Anchor hosts — the frontends. Sets each engine's ingress peer count.
    #[arg(long, default_value_t = 3)]
    anchor_hosts: usize,

    /// Token producers. Each maintains its own active batch, and this is also
    /// each anchor host's ingress peer count — the axis that drives the
    /// `O(peers x slots)` costs under test, so it is the one to sweep.
    #[arg(long, default_value_t = 2)]
    engines: usize,

    /// Requests to serve. Each is one response stream, start to terminal.
    #[arg(long, default_value_t = 500)]
    requests: u32,

    /// Requests one engine may hold in its active batch at once.
    #[arg(long, default_value_t = 32)]
    max_batch: u32,

    /// Longest response, in tokens.
    #[arg(long, default_value_t = 64)]
    tokens: u32,

    /// Gap between forward passes, standing in for GPU compute time.
    #[arg(long, default_value_t = 1)]
    pass_delay_ms: u64,

    /// How often the mux credit sweep runs. **The A/B knob for gap 1.**
    /// velo's default is 200 ms; 2 ms here reproduces the pre-drain-hook
    /// cadence for the A/B.
    #[arg(long, default_value_t = 2)]
    credit_sweep_interval_ms: u64,

    /// Per-stream credit window (`MuxConfig::initial_credit`).
    #[arg(long, default_value_t = MuxConfig::default().initial_credit)]
    initial_credit: u32,

    /// Run on the legacy one-connection-per-stream path instead of the mux.
    #[arg(long)]
    legacy: bool,

    /// Who decides when a batch is written.
    #[arg(long, value_enum, default_value_t = Flush::Manual)]
    flush_policy: Flush,

    /// Emit one line of JSON instead of a human summary, for sweep scripting.
    #[arg(long)]
    json: bool,

    /// Discard this many requests' latencies before recording, so connection
    /// establishment and the first attaches do not land in the histograms.
    #[arg(long, default_value_t = 0)]
    warmup_requests: u32,

    /// Messenger transport that carries the mux batches.
    #[arg(long, value_enum, default_value_t = TransportType::Tcp)]
    transport: TransportType,

    /// Unattached-anchor timeout, in milliseconds. Hold it small so that
    /// unattached deadlines come due during the run, but above the attach
    /// latency: a request anchor that expires before its sender attaches
    /// fails the run.
    #[arg(long)]
    unattached_timeout_ms: Option<u64>,

    /// Scratch: `paced` is the engine model above. `closed`: `--streams`
    /// streams, each sends one record and waits for it to arrive before the
    /// next. `flood`: each stream sends its records back to back.
    #[arg(long, value_enum, default_value_t = Load::Paced)]
    load: Load,

    /// Streams for `closed` and `flood`.
    #[arg(long, default_value_t = 64)]
    streams: usize,

    /// Producer nodes for `closed` and `flood`; the streams are spread over
    /// them, so this is the consumer's count of busy peer lanes.
    #[arg(long, default_value_t = 1)]
    peers: usize,

    /// Measured records per stream for `closed` and `flood`.
    #[arg(long, default_value_t = 2000)]
    records: u32,

    /// Unmeasured records per stream before them.
    #[arg(long, default_value_t = 200)]
    warmup_records: u32,

    /// Scratch: arm, core sets, saturation.
    #[command(flatten)]
    arm: ArmArgs,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Load {
    Paced,
    Closed,
    Flood,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
enum Flush {
    /// The batcher writes at the end of every wake.
    Auto,
    /// The engine writes once per forward pass.
    Manual,
}

impl Flush {
    fn policy(self) -> FlushPolicy {
        match self {
            Flush::Auto => FlushPolicy::Auto(Default::default()),
            Flush::Manual => FlushPolicy::Manual,
        }
    }
}

/// One generated token. Sized like a real decoded token.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Token {
    /// Position in this request's output; the consumer asserts it, which is how
    /// the harness shows that sharing a batch never disturbs one stream's order.
    step: u32,
    /// Stand-in for the decoded text.
    text: String,
}

/// A request as it reaches an engine. The engine only ever sees the handle.
struct Request {
    id: u32,
    handle: StreamAnchorHandle,
    budget: u32,
}

/// A velo node plus the registry its counters live in.
struct Node {
    velo: Velo,
    registry: Registry,
}

impl Node {
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.registry)
    }

    /// Wire writes this node made: `_stream_batch` AMs under the mux (the
    /// histogram's sample count is one observation per batch), per-stream
    /// egress flushes on the legacy path.
    fn wire_writes(&self, mux: bool) -> f64 {
        if mux {
            self.snapshot().histogram_count(
                "velo_streaming_mux_records_per_batch",
                &[("direction", "sent")],
            ) as f64
        } else {
            self.snapshot()
                .counter("velo_streaming_egress_flushes_total", &[])
        }
    }
}

/// Build a node with the mux configured as the run asks for.
async fn node(args: &Args, mux_enabled: bool) -> Result<Arc<Node>> {
    // A registry per node: two `VeloMetrics::register` calls against one
    // registry collide on collector names.
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);
    let mut builder: VeloBuilder = Velo::builder()
        .add_transport(new_transport(args.transport, "response_plane_bench").await?)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics);
    if let Some(ms) = args.unattached_timeout_ms {
        let mut config = velo::streaming::ReservationConfig::default();
        config.unattached_timeout = Duration::from_millis(ms);
        builder = builder.reservation_config(config);
    }
    let velo = builder
        .messenger_mux(MuxConfig {
            enabled: mux_enabled,
            flush_policy: args.flush_policy.policy(),
            credit_sweep_interval: Duration::from_millis(args.credit_sweep_interval_ms),
            initial_credit: args.initial_credit,
            ..MuxConfig::default()
        })?
        .build()
        .await?;
    Ok(Arc::new(Node { velo, registry }))
}

/// How many tokens request `id` emits. Derived from the index so batch
/// composition is identical run to run.
fn budget(id: u32, longest: u32) -> u32 {
    1 + (id.wrapping_mul(7)) % longest.max(1)
}

/// Process CPU, split user/system, from `/proc/self/stat`.
///
/// Fields 14 and 15 (1-indexed) are utime and stime in clock ticks. Read after
/// the comm field, which may itself contain spaces inside parentheses — hence
/// splitting on the last `)` rather than on whitespace from the start.
#[cfg(target_os = "linux")]
fn cpu_seconds() -> (f64, f64) {
    let ticks = 100.0; // _SC_CLK_TCK is 100 on every Linux this runs on.
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return (0.0, 0.0);
    };
    let Some(after_comm) = stat.rsplit_once(')') else {
        return (0.0, 0.0);
    };
    let fields: Vec<&str> = after_comm.1.split_whitespace().collect();
    // after_comm.1 starts at field 3 (state), so utime is index 11, stime 12.
    let get = |i: usize| -> f64 {
        fields
            .get(i)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            / ticks
    };
    (get(11), get(12))
}

#[cfg(not(target_os = "linux"))]
fn cpu_seconds() -> (f64, f64) {
    (0.0, 0.0)
}

/// Latency samples for one run, in microseconds.
struct Latencies {
    ttft: Histogram<u64>,
    itl: Histogram<u64>,
}

impl Latencies {
    fn new() -> Result<Self> {
        Ok(Self {
            // 1 us to 100 s, three significant figures.
            ttft: Histogram::new_with_bounds(1, 100_000_000, 3)?,
            itl: Histogram::new_with_bounds(1, 100_000_000, 3)?,
        })
    }

    fn merge(&mut self, other: &Latencies) -> Result<()> {
        self.ttft.add(&other.ttft)?;
        self.itl.add(&other.itl)?;
        Ok(())
    }
}

/// What one consumed stream reported back.
struct StreamResult {
    tokens: u64,
    terminal: bool,
    ttft_us: Option<u64>,
    itl_us: Vec<u64>,
    recorded: bool,
}

/// Consume one response stream, timestamping the first token and each gap.
async fn drain(
    id: u32,
    dispatched: Instant,
    mut anchor: StreamAnchor<Token>,
    record: bool,
) -> Result<StreamResult> {
    let mut step = 0u32;
    let mut ttft_us = None;
    let mut itl_us = Vec::new();
    let mut previous: Option<Instant> = None;

    while let Some(frame) = anchor.next().await {
        match frame? {
            StreamFrame::Item(token) => {
                let now = Instant::now();
                if token.step != step {
                    bail!(
                        "request {id} delivered step {} where {step} was due — sharing a \
                         batch must never disturb a single stream's order",
                        token.step
                    );
                }
                if step == 0 {
                    ttft_us = Some(dispatched.elapsed().as_micros() as u64);
                } else if let Some(prev) = previous {
                    itl_us.push(now.duration_since(prev).as_micros() as u64);
                }
                previous = Some(now);
                step += 1;
            }
            StreamFrame::Finalized => {
                return Ok(StreamResult {
                    tokens: u64::from(step),
                    terminal: true,
                    ttft_us,
                    itl_us,
                    recorded: record,
                });
            }
            other => bail!("request {id} saw an unexpected frame: {other:?}"),
        }
    }
    Ok(StreamResult {
        tokens: u64::from(step),
        terminal: false,
        ttft_us,
        itl_us,
        recorded: record,
    })
}

#[derive(Default)]
struct HostStats {
    completed: u64,
    tokens: u64,
    terminals: u64,
}

/// One anchor host: accepts anchors and consumes each to its terminal.
async fn host(
    rx: flume::Receiver<(u32, Instant, StreamAnchor<Token>, bool)>,
) -> Result<(HostStats, Latencies)> {
    let mut streams = JoinSet::new();
    while let Ok((id, dispatched, anchor, record)) = rx.recv_async().await {
        streams.spawn(drain(id, dispatched, anchor, record));
    }

    let mut stats = HostStats::default();
    let mut lat = Latencies::new()?;
    while let Some(joined) = streams.join_next().await {
        let r = joined??;
        stats.tokens += r.tokens;
        if r.terminal {
            stats.terminals += 1;
            stats.completed += 1;
        }
        if r.recorded {
            if let Some(t) = r.ttft_us {
                lat.ttft.record(t.max(1))?;
            }
            for gap in r.itl_us {
                lat.itl.record(gap.max(1))?;
            }
        }
    }
    Ok((stats, lat))
}

#[derive(Default)]
struct EngineStats {
    requests: u32,
    tokens: u64,
    passes: u64,
}

/// One decode engine: continuous batching, one token per active request per pass.
async fn engine(
    index: usize,
    node: Arc<Node>,
    rx: flume::Receiver<Request>,
    args: Args,
    mux: bool,
) -> Result<EngineStats> {
    let mut stats = EngineStats::default();
    let mut active: Vec<(u32, StreamSender<Token>, u32, u32)> = Vec::new();
    let mut queue: Vec<Request> = Vec::new();
    let mut inbound_open = true;
    let pass_delay = Duration::from_millis(args.pass_delay_ms);

    loop {
        // Admit up to the batch ceiling, taking queued work first.
        while active.len() < args.max_batch as usize {
            let next = if !queue.is_empty() {
                Some(queue.remove(0))
            } else if inbound_open {
                match rx.try_recv() {
                    Ok(r) => Some(r),
                    Err(flume::TryRecvError::Empty) => None,
                    Err(flume::TryRecvError::Disconnected) => {
                        inbound_open = false;
                        None
                    }
                }
            } else {
                None
            };
            let Some(req) = next else { break };
            let sender = node
                .velo
                .attach_anchor::<Token>(req.handle)
                .await
                .map_err(|e| anyhow::anyhow!("engine {index} attach failed: {e}"))?;
            active.push((req.id, sender, 0, req.budget));
            stats.requests += 1;
        }

        // Nothing active: either wait for work or finish.
        if active.is_empty() {
            if !inbound_open && queue.is_empty() {
                break;
            }
            match rx.recv_async().await {
                Ok(r) => {
                    queue.push(r);
                    continue;
                }
                Err(_) => {
                    inbound_open = false;
                    if queue.is_empty() {
                        break;
                    }
                    continue;
                }
            }
        }

        // One forward pass: exactly one token on every active request.
        if !pass_delay.is_zero() {
            tokio::time::sleep(pass_delay).await;
        }
        stats.passes += 1;

        let mut retired = Vec::new();
        for (slot, (id, sender, step, budget)) in active.iter_mut().enumerate() {
            let token = Token {
                step: *step,
                text: format!("t{}", *step),
            };
            sender
                .send(token)
                .await
                .map_err(|e| anyhow::anyhow!("request {id} send failed: {e}"))?;

            *step += 1;
            stats.tokens += 1;
            if *step >= *budget {
                retired.push(slot);
            }
        }

        // Under Manual the engine writes the pass it just staged.
        if mux && args.flush_policy == Flush::Manual {
            node.velo.flush_batch();
        }

        // Finalize retired requests, highest slot first so indices stay valid.
        for slot in retired.into_iter().rev() {
            let (_, sender, _, _) = active.remove(slot);
            sender.finalize()?;
        }
    }

    Ok(stats)
}

/// The run's headline numbers, for `--json`.
#[derive(Serialize)]
struct RunReport {
    mode: String,
    anchor_hosts: usize,
    engines: usize,
    requests: u32,
    max_batch: u32,
    pass_delay_ms: u64,
    credit_sweep_interval_ms: u64,
    initial_credit: u32,
    flush_policy: String,
    elapsed_ms: u128,
    tokens: u64,
    wire_writes: f64,
    tokens_per_write: f64,
    requests_per_sec: f64,
    ttft_p50_us: u64,
    ttft_p95_us: u64,
    ttft_p99_us: u64,
    itl_p50_us: u64,
    itl_p95_us: u64,
    itl_p99_us: u64,
    cpu_user_s: f64,
    cpu_sys_s: f64,
    cpu_total_s: f64,
    cpu_ms_per_request: f64,
    samples_ttft: u64,
    samples_itl: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rt = args.arm.runtime()?;
    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
    if args.load != Load::Paced {
        return stream_load(args).await;
    }
    if args.tokens > MAX_TOKENS {
        bail!("--tokens above {MAX_TOKENS} is outside what this harness bounds");
    }
    if args.anchor_hosts == 0 || args.engines == 0 {
        bail!("--anchor-hosts and --engines must both be at least 1");
    }
    if args.warmup_requests >= args.requests {
        bail!("--warmup-requests must be below --requests");
    }
    let mux = !args.legacy;

    let mode = if mux {
        format!("mux, flush-policy {:?}", args.flush_policy).to_lowercase()
    } else {
        "legacy (one TCP connection per stream)".to_string()
    };
    if !args.json {
        println!(
            "response_plane_bench: {} anchor hosts, {} engine(s), {} requests, max-batch {}, \
             sweep {}ms, credit window {}, {}ms between passes",
            args.anchor_hosts,
            args.engines,
            args.requests,
            args.max_batch,
            args.credit_sweep_interval_ms,
            args.initial_credit,
            args.pass_delay_ms
        );
        println!("mode: {mode}");
    }

    // Build the topology.
    let mut hosts = Vec::with_capacity(args.anchor_hosts);
    for _ in 0..args.anchor_hosts {
        hosts.push(node(&args, mux).await?);
    }
    let mut engines = Vec::with_capacity(args.engines);
    for _ in 0..args.engines {
        engines.push(node(&args, mux).await?);
    }

    // Every engine must know every host to attach remotely.
    for e in &engines {
        for h in &hosts {
            e.velo.register_peer(h.velo.peer_info())?;
        }
    }
    for h in &hosts {
        for e in &engines {
            h.velo.register_peer(e.velo.peer_info())?;
        }
    }

    // Wire up host consumers and engine producers.
    let mut host_txs = Vec::with_capacity(hosts.len());
    let mut host_tasks = Vec::with_capacity(hosts.len());
    for _ in &hosts {
        let (tx, rx) = flume::unbounded();
        host_txs.push(tx);
        host_tasks.push(tokio::spawn(host(rx)));
    }

    let mut engine_txs = Vec::with_capacity(engines.len());
    let mut engine_tasks = Vec::with_capacity(engines.len());
    for (index, e) in engines.iter().enumerate() {
        let (tx, rx) = flume::unbounded::<Request>();
        engine_txs.push(tx);
        engine_tasks.push(tokio::spawn(engine(
            index,
            Arc::clone(e),
            rx,
            args.clone(),
            mux,
        )));
    }

    let saturation = args.arm.saturate();
    let engine_registries: Vec<&Registry> = engines.iter().map(|n| &n.registry).collect();
    let (wakes0, _, _) = mux_counters(&engine_registries);
    // CPU is sampled around the measured region only.
    let p0 = ProcStats::now();
    let (user0, sys0) = cpu_seconds();
    let started = Instant::now();

    let expected_tokens: u64 = (0..args.requests)
        .map(|id| u64::from(budget(id, args.tokens)))
        .sum();

    for id in 0..args.requests {
        let host_index = id as usize % args.anchor_hosts;
        let anchor = hosts[host_index].velo.create_anchor::<Token>();
        let dispatched = Instant::now();
        let record = id >= args.warmup_requests;
        let request = Request {
            id,
            handle: anchor.handle(),
            budget: budget(id, args.tokens),
        };
        host_txs[host_index].send((id, dispatched, anchor, record))?;
        engine_txs[id as usize % args.engines].send(request)?;
    }
    drop(engine_txs);
    drop(host_txs);

    let engine_stats = tokio::time::timeout(PATIENCE, futures::future::try_join_all(engine_tasks))
        .await
        .map_err(|_| anyhow::anyhow!("the engines did not drain within {PATIENCE:?}"))??
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    let host_results = tokio::time::timeout(PATIENCE, futures::future::try_join_all(host_tasks))
        .await
        .map_err(|_| anyhow::anyhow!("the hosts did not see every terminal within {PATIENCE:?}"))??
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    let elapsed = started.elapsed();
    let (user1, sys1) = cpu_seconds();
    let p1 = ProcStats::now();
    drop(saturation);
    let (_, vcsw, threads) = p1.since(&p0);
    let cpu_user = user1 - user0;
    let cpu_sys = sys1 - sys0;

    // Correctness gates: a fast wrong answer is not a result.
    let completed: u64 = host_results.iter().map(|(h, _)| h.completed).sum();
    let received: u64 = host_results.iter().map(|(h, _)| h.tokens).sum();
    if completed != u64::from(args.requests) || received != expected_tokens {
        bail!(
            "expected {} requests and {expected_tokens} tokens, saw {completed} and {received}",
            args.requests
        );
    }

    let mut lat = Latencies::new()?;
    for (_, l) in &host_results {
        lat.merge(l)?;
    }

    // Read after the hosts have every terminal, not when each engine's loop
    // exits: `finalize` only stages a terminal, so the last batch of a run is
    // still unwritten at that point. Sampling there undercounts writes and
    // inflates `tokens_per_write` — the one number this example exists to
    // report.
    let wire_writes: f64 = engines.iter().map(|node| node.wire_writes(mux)).sum();
    let tokens: u64 = engine_stats.iter().map(|s| s.tokens).sum();
    let tokens_per_write = if wire_writes > 0.0 {
        tokens as f64 / wire_writes
    } else {
        0.0
    };
    let secs = elapsed.as_secs_f64();
    let rps = if secs > 0.0 {
        f64::from(args.requests) / secs
    } else {
        0.0
    };
    let cpu_total = cpu_user + cpu_sys;
    let (wakes1, _, _) = mux_counters(&engine_registries);
    println!(
        "ARM,load=paced,arm={},engines={},hosts={},tokens={tokens},cpu_us_per_token={:.3},vcsw_per_token={:.3},wakes_per_token={:.3},tokens_per_write={tokens_per_write:.2},threads={threads},saturate={},ttft_p50_us={},ttft_p99_us={},itl_p50_us={},itl_p99_us={}",
        args.arm.label(),
        args.engines,
        args.anchor_hosts,
        cpu_total * 1e6 / tokens as f64,
        vcsw as f64 / tokens as f64,
        (wakes1 - wakes0) / tokens as f64,
        args.arm.saturate,
        lat.ttft.value_at_quantile(0.50),
        lat.ttft.value_at_quantile(0.99),
        lat.itl.value_at_quantile(0.50),
        lat.itl.value_at_quantile(0.99),
    );
    let cpu_ms_per_request = if args.requests > 0 {
        cpu_total * 1000.0 / f64::from(args.requests)
    } else {
        0.0
    };

    let report = RunReport {
        mode: mode.clone(),
        anchor_hosts: args.anchor_hosts,
        engines: args.engines,
        requests: args.requests,
        max_batch: args.max_batch,
        pass_delay_ms: args.pass_delay_ms,
        credit_sweep_interval_ms: args.credit_sweep_interval_ms,
        initial_credit: args.initial_credit,
        flush_policy: format!("{:?}", args.flush_policy).to_lowercase(),
        elapsed_ms: elapsed.as_millis(),
        tokens,
        wire_writes,
        tokens_per_write,
        requests_per_sec: rps,
        ttft_p50_us: lat.ttft.value_at_quantile(0.50),
        ttft_p95_us: lat.ttft.value_at_quantile(0.95),
        ttft_p99_us: lat.ttft.value_at_quantile(0.99),
        itl_p50_us: lat.itl.value_at_quantile(0.50),
        itl_p95_us: lat.itl.value_at_quantile(0.95),
        itl_p99_us: lat.itl.value_at_quantile(0.99),
        cpu_user_s: cpu_user,
        cpu_sys_s: cpu_sys,
        cpu_total_s: cpu_total,
        cpu_ms_per_request,
        samples_ttft: lat.ttft.len(),
        samples_itl: lat.itl.len(),
    };

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    println!("\n=== latency (us) ===");
    println!("{:<10} {:>12} {:>12} {:>12}", "", "p50", "p95", "p99");
    println!(
        "{:<10} {:>12} {:>12} {:>12}",
        "TTFT", report.ttft_p50_us, report.ttft_p95_us, report.ttft_p99_us
    );
    println!(
        "{:<10} {:>12} {:>12} {:>12}",
        "ITL", report.itl_p50_us, report.itl_p95_us, report.itl_p99_us
    );

    println!("\n=== throughput ===");
    println!("  requests            {}", report.requests);
    println!("  tokens streamed     {}", report.tokens);
    println!("  wire writes         {:.0}", report.wire_writes);
    println!("  tokens per write    {:.2} : 1", report.tokens_per_write);
    println!("  requests/sec        {:.1}", report.requests_per_sec);
    println!("  elapsed             {} ms", report.elapsed_ms);

    println!("\n=== cpu (whole process — see module docs) ===");
    println!("  user                {:.3} s", report.cpu_user_s);
    println!("  system              {:.3} s", report.cpu_sys_s);
    println!("  total               {:.3} s", report.cpu_total_s);
    println!("  per request         {:.3} ms", report.cpu_ms_per_request);

    Ok(())
}

// ---------------------------------------------------------------------------
// Scratch: closed-loop and flood loads
// ---------------------------------------------------------------------------

/// One record. `sent_ns` is when its producer sent it, from a shared base.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Rec {
    seq: u32,
    sent_ns: u64,
}

async fn stream_load(args: Args) -> Result<()> {
    if args.streams == 0 || args.peers == 0 || args.records == 0 {
        bail!("--streams, --peers and --records must be at least 1");
    }
    let closed = args.load == Load::Closed;
    let manual = args.flush_policy == Flush::Manual;
    let consumer = node(&args, true).await?;
    let mut producers = Vec::with_capacity(args.peers);
    for _ in 0..args.peers {
        producers.push(node(&args, true).await?);
    }
    for p in &producers {
        p.velo.register_peer(consumer.velo.peer_info())?;
        consumer.velo.register_peer(p.velo.peer_info())?;
    }
    for p in &producers {
        p.velo
            .wait_for_handler(consumer.velo.instance_id(), "_anchor_attach")
            .await?;
        consumer
            .velo
            .wait_for_handler(p.velo.instance_id(), "_anchor_attach")
            .await?;
    }

    let base = Instant::now();
    let warm = args.warmup_records;
    let total = warm + args.records;
    // Producers and main meet here after the warmup, and again before the end.
    let barrier = Arc::new(tokio::sync::Barrier::new(args.streams + 1));
    let mut consumers = Vec::with_capacity(args.streams);
    let mut senders = Vec::with_capacity(args.streams);
    for s in 0..args.streams {
        let producer = Arc::clone(&producers[s % args.peers]);
        let anchor = consumer.velo.create_anchor::<Rec>();
        let sender = producer.velo.attach_anchor::<Rec>(anchor.handle()).await?;
        // The consumer tells the producer what it has received.
        let (ack_tx, ack_rx) = flume::unbounded::<u32>();
        consumers.push(tokio::spawn(consume(anchor, base, warm, ack_tx)));
        senders.push((producer, sender, ack_rx));
    }

    let mut tasks = Vec::with_capacity(args.streams);
    for (producer, sender, ack_rx) in senders {
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let send = |seq: u32| {
                let sender = &sender;
                async move {
                    sender
                        .send(Rec {
                            seq,
                            sent_ns: base.elapsed().as_nanos() as u64,
                        })
                        .await
                }
            };
            let mut seq = 0u32;
            for phase_end in [warm, total] {
                while seq < phase_end {
                    send(seq).await?;
                    if manual {
                        producer.velo.flush_batch();
                    }
                    if closed {
                        // Wait for this record to arrive.
                        while ack_rx.recv_async().await? < seq {}
                    }
                    seq += 1;
                }
                if !closed {
                    while ack_rx.recv_async().await? + 1 < phase_end {}
                }
                barrier.wait().await;
            }
            sender.finalize()?;
            Ok::<_, anyhow::Error>(())
        }));
    }

    barrier.wait().await;
    // Measured phase.
    let saturation = args.arm.saturate();
    let producer_registries: Vec<&Registry> = producers.iter().map(|n| &n.registry).collect();
    let (w0, b0, r0) = mux_counters(&producer_registries);
    let p0 = ProcStats::now();
    barrier.wait().await;
    let p1 = ProcStats::now();
    let (w1, b1, r1) = mux_counters(&producer_registries);
    drop(saturation);

    for t in tasks {
        tokio::time::timeout(PATIENCE, t).await???;
    }
    let mut lat = Histogram::<u64>::new_with_bounds(1, 100_000_000_000, 3)?;
    for c in consumers {
        lat.add(&tokio::time::timeout(PATIENCE, c).await???)?;
    }
    let (cpu, vcsw, threads) = p1.since(&p0);
    let wall = p1.at.unwrap().duration_since(p0.at.unwrap()).as_secs_f64();
    let n = f64::from(args.records) * args.streams as f64;
    println!(
        "ARM,load={},arm={},streams={},peers={},records={},rec_per_s={:.0},p50_us={:.1},p99_us={:.1},p999_us={:.1},cpu_us_per_rec={:.3},vcsw_per_rec={:.3},wakes_per_rec={:.3},rec_per_batch={:.2},threads={},saturate={},flush={:?},samples={}",
        format!("{:?}", args.load).to_lowercase(),
        args.arm.label(),
        args.streams,
        args.peers,
        args.records,
        n / wall,
        lat.value_at_quantile(0.50) as f64 / 1e3,
        lat.value_at_quantile(0.99) as f64 / 1e3,
        lat.value_at_quantile(0.999) as f64 / 1e3,
        cpu * 1e6 / n,
        vcsw as f64 / n,
        (w1 - w0) / n,
        if b1 > b0 { (r1 - r0) / (b1 - b0) } else { 0.0 },
        threads,
        args.arm.saturate,
        args.flush_policy,
        lat.len(),
    );
    Ok(())
}

/// Read one stream to its end. Records the one-way latency of every record
/// after the warmup, in ns, and reports each sequence number received.
async fn consume(
    mut anchor: StreamAnchor<Rec>,
    base: Instant,
    warm: u32,
    ack: flume::Sender<u32>,
) -> Result<Histogram<u64>> {
    let mut h = Histogram::<u64>::new_with_bounds(1, 100_000_000_000, 3)?;
    let mut next = 0u32;
    while let Some(frame) = anchor.next().await {
        match frame? {
            StreamFrame::Item(rec) => {
                let now = base.elapsed().as_nanos() as u64;
                if rec.seq != next {
                    bail!("record {} arrived where {next} was due", rec.seq);
                }
                next += 1;
                if rec.seq >= warm {
                    h.record(now.saturating_sub(rec.sent_ns).max(1))?;
                }
                let _ = ack.send(rec.seq);
            }
            StreamFrame::Finalized => return Ok(h),
            other => bail!("unexpected frame {other:?}"),
        }
    }
    bail!("stream ended without a terminal")
}
