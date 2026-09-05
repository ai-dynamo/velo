<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Plan: make velo the winner in every response-plane category

Dated 2026-09-04. Goal: after the changes below, velo0 wins or ties every measured category against both the shipping per-request TCP plane and the ported PR 11918 mux plane, at the published rig shape. Nothing here is implemented yet. Evidence base: `ttft-gap-diagnosis.md` (the TTFT mechanism, adversarially verified) and `ucx-arm-instability-diagnosis.md` (the UCX defects). Both diagnoses point at the same two structural facts: velo's frontend ingest is a fixed-parallelism drain stage behind unbounded shared FIFOs, and velo's first-record path pays per-request setup that mux18p does not.

## Scoreboard today (matrix t3-m18p1, 3 reps, 512 workers, concurrency 8192)

| category | tcp | velo0 | mux18p | winner |
|---|---|---|---|---|
| throughput (req/s mean) | 2,511 | **3,019** | 2,746 | velo0 |
| throughput stability (rep spread) | 719 | **165** | 701 | velo0 |
| TTFT p50 (ms) | 106–1,342 | 1,098–1,123 | **86–168** | mux18p |
| TTFT p99 (ms) | 3,470 | 2,742 | **1,746** | mux18p |
| ITL p99 (ms) | 30–114 | **18–28** | 26–99 | velo0 (see caveat) |
| E2E p99 (s) | ~30 (rep2) | **6.8** | 26.1 | velo0 |
| frontend CPU (ms/req) | 9.07 | 9.19 | **6.51** | mux18p |
| errors | 0 | 0 | 0 | tie |
| ops surface (drain, credit, fallback, metrics) | — | **yes** | partial | velo0 |

Caveat on ITL: aiperf ITL = (latency − TTFT)/(tokens − 1), so velo0's high TTFT flatters it. The honest velo0 advantage in that row is E2E tail discipline, which is real (6.8 vs 26.1 s p99).

Two categories to take: **TTFT** (both percentiles) and **frontend CPU**. Two categories to not lose while taking them: throughput and the E2E tail. The mechanism ledger in `ttft-gap-diagnosis.md` says these are compatible: the TTFT second is a standing shared-FIFO backlog plus a per-request attach RTT, neither of which is what buys velo0 its throughput or its tail.

## Workstreams

Ordered by dependency, not by size. Each ships as its own PR from current `main`, tests first, one reviewable concern per PR.

### W0 — Instrumentation (prerequisite, no behavior change)

Wire the velo metrics registry into the dyn-pin frontend `/metrics` scrape and export worker-side velo metrics in the rig; add ordered-lane depth/wait gauges and a `message_rx` depth gauge; scrape the existing `WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS` histogram; record the git sha in `rig_run_meta.json`. Exit criterion: a tier-3 velo0 rep where the ~1.1 s is visibly split between `message_rx` wait and ordered-lane wait, and the attach RTT is a measured histogram. This closes the measurement hole both diagnoses hit and decides how much W1 vs W3/W4 must recover.

### W1 — Shard the frontend ingest drain (the dominant TTFT fix)

Today: one unbounded `message_rx` drained by one decode task, then one unbounded ordered lane per sender WorkerId (8 lanes for 512 workers). Change: dispatch `_stream_batch` on lanes keyed by slot (or slot-hash shards sized to available cores), and shard or inline the decode step. Per-slot ordering is the only ordering the protocol needs — per-slot `frame_seq` with `IngressSlot::park` already tolerates cross-slot reorder, so per-sender FIFO is stronger than required. Scope strictly to `_stream_batch`; other handlers keep their semantics. Expected effect: the standing backlog (~3,400 requests ≈ 1.1 s) collapses; TTFT moves toward the 100–200 ms floor the warmup-wave data already shows velo0 hitting whenever the backlog is absent. Risks: PeerIngress mutex contention, credit-reconcile races across shards. Tests: ordering-per-slot property test, credit conservation under concurrent shards, a saturation test that asserts bounded lane wait.

### W2 — Cut per-record frontend ingest cost (CPU category + compounds with W1)

Four verified line items: (a) per-frame `tokio::time::timeout` registration in `reader_pump` (`control.rs:336`) → coarse deadline check; (b) the Vec copy in `IngressSlot::deliver`; (c) ~4 task wakes + 2 allocations per record anchor delivery → one wake per decoded batch (slice handoff); (d) the per-attach 60 s accept-timeout task leak (~180k live timers at 3k attach/s) → cancel on OpenSlot. Target: close the 51% frontend CPU gap to mux18p (4,038 vs 2,671 CPU-s per run); 20–40% ingest CPU reduction is plausible from (a)–(c) alone. Because the drain stage runs with near-zero headroom, service-rate gains shrink the standing queue superlinearly — this is also a TTFT change. Low risk, mechanical, each item separately testable.

### W3 — Zero-RTT stream setup (remove the per-request attach round trip)

Today the worker awaits `_anchor_attach` before `generate()` starts; the attach crosses the backlogged `message_rx` once per request. Change: mint the stream identity at the frontend when the request is registered, carry streaming key, routing session, initial credit, and slot byte budget in the request envelope (peer-level defaults negotiated once in the hello), pre-bind the ingress slot at registration, and let the worker's first batch's OpenSlot claim it — bind-on-OpenSlot already exists frontend-side. This is PR 11918's zero-RTT shape expressed in velo's protocol, and it removes worker-side pre-generate blocking entirely. Expected: ~20 ms at tier-2 scale; up to several hundred ms at tier-3. Risks: accept-window semantics, credit negotiation moving to peer level, orphan-slot cleanup when a request dies before its first batch. Tests: golden handshake compatibility, orphan reclamation, credit accounting with pre-bound slots.

### W4 — Two-class ingest: urgent lane for stream-opening records

The worker batcher already stages `OpenSlot` urgent; the frontend has no receive-side equivalent, so a new stream's first records enter the shared FIFO at the tail. Change: an urgent class for OpenSlot/Prologue/first-data batches — a separate handler name dispatched off the lane path, or a priority queue inside the `_stream_batch` lane — mirroring mux18p's urgent lane. Urgent volume is bounded by design (~3 records/stream × ~3k opens/s ≈ 9k rec/s against ~820k data rec/s). This takes TTFT to the floor **without** giving up the admission discipline that produces velo0's 6.8 s E2E p99 — the established-stream backlog still paces the stream body. That is strictly better than mux18p's trade, which buys TTFT by letting 94–98% of requests stream concurrently and pays a 26.1 s E2E p99. Risks: urgency must be visible pre-decode (frame tag or handler name); cap urgent share to prevent inversion by short streams.

### W5 — Bounded ingest with upstream backpressure (hold until W3+W4 land)

Bound the `_stream_batch` lanes (depth or byte cap analogous to mux18p's 256 KiB per-connection budget) so the backlog moves upstream into per-stream slot inlets at the workers, where the batcher's SelectAll rotation naturally favors new streams. Highest risk in the set: backpressure crosses the shared per-connection admission gate, so control traffic can be head-of-line-blocked behind parked data — deadlock and throughput-collapse care required. Only worth doing if W1+W2+W4 leave a residual gap.

### W6 — UCX transport fixes (separate track, from `ucx-arm-instability-diagnosis.md`)

(a) gate `admit()` on in-flight ops or a per-peer ring share, returning `SendOutcome::Pending` at a cap; (b) heartbeat lane that cannot sit behind data, or a starved-vs-alive watchdog distinction; (c) instrument the UCX inbound path. Only after (a) and (c) is a ucx rerun worth cluster time. Note W4's urgent receive class and W6(b)'s heartbeat lane are the same concept at two layers; design them together.

## Isolation matrix (how we attribute each gain)

Each measurement is the standard tier-3 shape (3 reps, `t3-submit.sh`), arms interleaved, W0 metrics on. One variable per arm:

| arm | contents | question it answers |
|---|---|---|
| velo0 | baseline (current) | control |
| velo0+W1 | sharded drain only | how much of the 1.1 s is the drain stage |
| velo0+W2 | ingest cost only | CPU delta and its TTFT side effect |
| velo0+W1+W2 | both frontend fixes | do they compose superlinearly as predicted |
| velo0+W3 | zero-RTT setup only | the attach share of TTFT |
| velo0-full | W1+W2+W3+W4 | the ship candidate |
| mux18p | unchanged | the bar to beat |
| tcp | unchanged | the shipping baseline |

Decision points: after W0, the measured `message_rx`-vs-lane split sizes W1 (if the single decode task dominates, W1 starts there). After velo0+W1, if TTFT p50 is already ≤200 ms, W4 becomes optional polish and W5 is dropped. After velo0-full, if frontend CPU is still above mux18p, the remaining gap is adapter-side (anchor/consumer-task/spawn_blocking deltas enumerated in `tier2-adapter-brief.md`) and gets its own pass.

## Success criteria (all at the published shape, 3-rep means, zero errors required)

- TTFT p50 ≤ 200 ms and TTFT p99 ≤ 1,750 ms (beat mux18p's 1,746 or tie within noise).
- Throughput ≥ 3,000 req/s with rep spread ≤ 300 (hold today's win).
- E2E p99 ≤ 8 s (hold the admission-discipline advantage; mux18p sits at 26 s).
- Frontend CPU ≤ 6.5 ms/req (take the CPU category).
- ITL p99 ≤ 35 ms measured honestly (report alongside E2E p99, given the aiperf ITL arithmetic).

## What we deliberately do not do

- No tuning of flush/linger knobs: ruled out by the t3e control and by code (no timer exists on velo0's path).
- No ucx rerun before W6(a)+(c): ruled by the UCX diagnosis.
- No mux18p-style unbounded streaming concurrency: velo keeps admission discipline; W4 exists precisely so TTFT does not require giving up the E2E tail.

## Addendum 2026-09-04 (evening): W0 moved the seat of the backlog, so the order changes

W0's measurement (`ttft-gap-diagnosis.md`, addendum of the same date) found the frontend's two ingest FIFOs hold about 146 ms of the ~1,070 ms C segment (message_rx 36 ms by Little's law, ordered lanes 110 ms mean), while the worker-observed attach round trip averages 524 ms and every mocker process shows egress backpressure at concurrency 8192 and none at 2048. The standing backlog is in front of the per-connection writer on the worker's egress, above the wire. The workstreams keep their letters; their targets and order change:

1. **W0b (instrumentation, in flight)**: egress queue-wait histogram, frames-written counter and write-duration histogram on the connection writer, plus socket queue and node CPU sampling on both nodes, and `w6_egress.py`. Exit: the attach round trip is split between the worker egress queue, the socket, and the frontend egress queue.
2. **W3 (zero-RTT setup)** moves first among the fixes: it removes the largest single measured term (the attach round trip, 524 ms mean, 253 to 1,723 ms per process) and the worker-side pre-generate wait entirely.
3. **W4 (urgent class)** moves to the transport writer on the sending side: OpenSlot, prologue and attach frames bypass the per-connection data FIFO the way mux18p's writer drains its urgent lane before ordered data. Same concept as W6(b)'s heartbeat lane; design them as one mechanism in the `AdmissionGate` and the TCP writer.
4. **W2 (per-record cost)** unchanged in content, reduced in scope per `ingest-cost-ledger.md` to items (a) and (d); it is the frontend CPU category.
5. **W5 (bounded queue with backpressure)** now means bounding the per-connection admission queue in bytes, mux18p-style, so the backlog moves into per-stream slot inlets. It stays behind W3 and W4.
6. **W1 (frontend ingest)** drops to last: at most 110 ms of lane wait is available there, and the touched-slot reconcile in `ingest-cost-ledger.md` is the candidate only if a residual TTFT gap survives W3 and W4.

Arms for the next isolation matrix: `velo3` (W3), `velo4` (W4), `velo34`, `velo2` (W2), and `veloF` (W2+W3+W4), against `velo0`, `tcp`, `mux18p`, 3 reps, worker-side harvest on for every arm (three instrumented reps and one control showed no systematic perturbation; one outlier rep was router imbalance). Decision point after `velo34`: TTFT p50 at or under 200 ms with throughput at or above 3,000 req/s and E2E p99 at or under 8 s makes W5 and W1 optional. The success bar is unchanged.
