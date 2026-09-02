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
//! `agent-docs/dynamo-response-plane-competitive-plan.md` claims three
//! frontend-side costs that scale as `O(peers x slots)`:
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
//! at 2 (velo's default) and again at 500, and the CPU difference is the
//! sweep's cost with nothing else moving.
//!
//! **Scale `--engines`, not `--anchor-hosts`.** The sweep walks a node's
//! *ingress* peers — the peers it receives batches from — so an anchor host's
//! peer count is the number of engines streaming to it, mirroring Dynamo's 512
//! workers against 2 frontends. `--anchor-hosts` moves the engines' own ingress
//! count, which is the smaller side of the same product.
//!
//! ```text
//! # Does the 500 Hz sweep cost anything at 128 ingress peers?
//! response_plane_bench --anchor-hosts 2 --engines 128 --requests 2000 --credit-sweep-interval-ms 2
//! response_plane_bench --anchor-hosts 2 --engines 128 --requests 2000 --credit-sweep-interval-ms 500
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
//! cuts in the mux's favour, as `BATCHING.md` section V5 already says.
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
    /// velo's default is 2 ms.
    #[arg(long, default_value_t = 2)]
    credit_sweep_interval_ms: u64,

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
    velo: Arc<Velo>,
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
    let builder: VeloBuilder = Velo::builder()
        .add_transport(new_transport(TransportType::Tcp, "response_plane_bench").await?)
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics);
    let velo = builder
        .messenger_mux(MuxConfig {
            enabled: mux_enabled,
            flush_policy: args.flush_policy.policy(),
            credit_sweep_interval: Duration::from_millis(args.credit_sweep_interval_ms),
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
    writes: f64,
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

    stats.writes = node.wire_writes(mux);
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
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
             sweep {}ms, {}ms between passes",
            args.anchor_hosts,
            args.engines,
            args.requests,
            args.max_batch,
            args.credit_sweep_interval_ms,
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

    // CPU is sampled around the measured region only.
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

    let wire_writes: f64 = engine_stats.iter().map(|s| s.writes).sum();
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
