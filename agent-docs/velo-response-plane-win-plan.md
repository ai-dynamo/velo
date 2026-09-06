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

## Addendum 2026-09-04 (late night): the scoreboard is withdrawn; the bar is reset against a pinned baseline

`ttft-gap-diagnosis.md`'s late-night addendum records that the published first-token second was load-generator interference on the frontend node. Under core pinning velo0 posts TTFT p50 98 ms and p99 813 ms at 2,933 req/s, against mux18p's 49 ms, 768 ms at 2,762 req/s, with E2E p99 near 11.5 s for both and frontend CPU 5.85 against 5.27 ms/req. The rig now pins by default; `t3-base-pin` (three reps, tcp, velo0, mux18p) is the new scoreboard.

The plan's categories change accordingly. Throughput: velo0 holds it. E2E tail: a tie, no longer a velo0 win to protect; the ITL caveat becomes moot because both planes now stream the same population. First token: a gap of about 50 ms at p50 and about 45 ms at p99, whose measured components are the attach round trip (22 ms mean), the OpenSlot flush wait before the ack, the ordered-lane wait, and the anchor-to-SSE path. Frontend CPU: a gap of about 0.6 ms/req.

Order, unchanged in content, re-justified: W3 and W4a first (they remove the two setup waits and are already implemented on their branches), then W2 (a) and (d) for CPU, then W4b only if a residual first-token gap survives, and W1 only if the lane wait remains a visible term at the new scale. W5 is dropped: there is no standing backlog to bound. Success bar, to be fixed from the three-rep pinned means: TTFT p50 at or below mux18p's, TTFT p99 at or below mux18p's, throughput at or above 2,900 req/s with rep spread at or below 300, E2E p99 at or below mux18p's, frontend CPU at or below mux18p's, zero errors.

## Addendum 2026-09-05: success bar fixed from the pinned baseline

From `t3-base-pin` three-rep means (mux18p: 3,372 req/s, TTFT p50 47 ms, p99 753 ms, frontend CPU 2.76 ms/req; velo0: 3,210, 71, 833, 3.75), the bar for the ship candidate at the published shape with pinning, three-rep means, zero errors: TTFT p50 at or below 47 ms; TTFT p99 at or below 753 ms; throughput at or above 3,300 req/s with rep spread at or below 300; frontend CPU at or below 2.76 ms/req; E2E p99 at or below mux18p's on the same matrix. The gaps to close are 24 ms at p50, 80 ms at p99, and 0.99 ms/req of CPU. W3 and W4a (both implemented, PR #78 and the `w4-async-open-ack` branch) address the pre-generation waits (attach 13 ms mean, flush wait before the ack); the CPU gap is W2 (a) and (d) plus the per-batch reconcile walk from `ingest-cost-ledger.md`, now a first-class item. Status of the rest: W4b (control lane) only if a first-token residual survives W3 and W4a; W1 only if the lane wait is still visible; W5 dropped.

## Addendum 2026-09-05 (afternoon): `t3-iso1` verdict — W3 holds, W4a is blocked on a fix, the E2E criterion is withdrawn

Three-rep means from `t3-iso1` (table in `response-plane-benchmark-results.md`): mux18p 3,305 req/s, TTFT p50 48 ms, p99 764, CPU 2.84 ms/req; velo0 3,157, 85, 791, 3.93; velo3 3,303, 69, 851, 3.15; velo4a 3,408, 91, 820, 2.53, 16 errors; velo34 3,313, 59, 835, 3.01, 281 errors. TTFT p50 moves by about 10 ms between reps of one arm (velo0 74 to 95), so differences under that are noise.

Against the bar (p50 at or below 47, p99 at or below 753, at least 3,300 req/s with spread at most 300, CPU at or below 2.76, zero errors): no velo arm passes. velo3 recovers about 16 ms at p50 (69 against 85) with throughput on the bar and zero errors; its spread (389) and CPU (3.15) miss. velo34 posts the best velo p50 (59; 46 in rep 3) but fails on errors, and its numbers carry the leak. velo4a alone does not move p50 (91), so the detached ack recovers little by itself at this concurrency; what it buys shows only with W3 (velo34 against velo3, about 10 ms, at the noise edge). TTFT p99 does not move in any velo arm (791 to 851) and is likely set by the hot mocker process, not the plane.

Rulings.

1. The velo4a and velo34 errors are a defect in the control inbox (`ttft-gap-diagnosis.md`, afternoon addendum, section 1). The fix and its tests go on PR #79; velo4a and velo34 rerun after the wheel is rebuilt, three reps each, against the mux18p and velo3 numbers above from the same matrix.
2. The E2E p99 and ITL p99 criteria are withdrawn for this rig: they measure which mocker process holds the backlog (same addendum, section 2). The bar keeps TTFT p50 and p99, throughput and spread, CPU, and zero errors.
3. Order after the rerun: W2 (a) and (d) for CPU, then W4b only if a first-token residual survives; W1 stays last.
4. Follow-up for velo, outside the W4a PR: the control cap assumes about 1,024 live slots per peer and this rig runs one peer at 6,000. The refused entries are credit replies and closes, harmless only because `initial_credit` equals the output length here. A cap tied to live slots, or one that applies only to keys naming no live slot, is the candidate; it needs its own test and PR.
5. Rig follow-up: the backlog is a property of the packed mocker process (64 workers each). A per-process admission limit, or more processes with fewer workers each, would make the tail comparable, at the cost of comparability with the matrices to date. Not changed for the rerun.
6. New, ahead of W2: **W7, the zero-RTT request-path cost.** Section 3 of the diagnosis addendum shows W3 cuts the response leg (C) to mux18p's level while the request leg (A and B) grows from 4 to 20 to 24 ms, the frontend sends ten times more control batches to the workers, and its event-loop delay doubles. Recovering that leg is worth about 20 ms at p50 and would put velo34 at the bar. Order: instrument the batcher's sent records by type and its wakes by source (W0-class, one rep of velo3), name the reply that multiplies, fix it with a failing test, then rerun. W2 (a) and (d) follow, since the CPU gap may shrink with the same fix.

## Addendum 2026-09-05 (night): the CPU term is restated; iso2 clears the cap fix; iso3 carries the reply linger

Every frontend CPU per request number before tonight understated the truth about three-fold (results doc, night addendum: the summary read aiperf's local timestamps as UTC and measured the capture's idle tail). Corrected three-rep means on the pinned baseline: tcp 10.48, velo0 8.09, mux18p 7.61 ms per request; on `t3-iso1`: velo0 8.15, velo3 8.74, velo4a 7.66, velo34 8.45, mux18p 7.43. The CPU term of the bar is restated as **at or below mux18p's on the same matrix**; the gap to close is 0.5 to 0.7 ms per request, not 1.0, and W2's expected yield shrinks with it. The order does not change: W7 (the reply linger, PR #81, and the batch mix it fixes) is both the first-token lever and the likeliest CPU lever, since the frontend's 900,000 outbound batches and the workers' 4.5 million inbound ones are the work the linger and the data linger (`velo3f`) remove.

`t3-iso2` clears the control-cap fix: nine reps, zero errors, refusals without a fenced slot. Its first-token numbers are not compared against the bar (different nodes, a broad mocker backlog in several reps, one rep measured on aiperf's second pass). `t3-iso3` runs on the wheel with PR #81 merged: arms velo3 (linger on by default), velo3n (linger off, the control), velo3f (velo3 with a 500 us data linger on the workers), velo34, and mux18p, three reps each, with the corrected CPU summary and the PR #80 counters on every rep.

## Addendum 2026-09-06: iso3 verdict; the reply linger is a fix, not a lever; the next lever needs a profile and a fixed rig

`t3-iso3` (results doc, addendum of 2026-09-06) against the bar, same-mode pairs on one node pair: TTFT p50 velo3 55 to 62 ms against mux18p 48 to 49 (miss by 6 to 13); p99 855 against 789 (miss); throughput below 3,300 for every arm on these nodes (the bar's throughput term is node-dependent as written); CPU 9.76 against 7.48 (miss by 2.3 ms per request); errors zero (met). No velo arm passes.

Rulings.

1. PR #81 (reply linger) stays: it cuts the frontend's outbound batches five-fold and CPU by 0.4 ms per request, and the per-request join shows it costs nothing. It is not a first-token lever: A, B and C are unchanged at equal load. Section 3's forecast that recovering the batch inflation would recover the request path was wrong; the inflation was a symptom of the same contention, not its cause.
2. A 500 us data linger on the workers (velo3f) is not a ship setting: it saves 1.05 ms per request of frontend CPU and adds about 10 ms to the request path. A window near 100 us may trade better and can be tried as a rig arm without code.
3. The remaining gap is per-request frontend work on the response path under load: the frontend's own first-token time is 18 to 24 ms on velo3 against 12 on mux18p in the same mode, event-loop delay 2.3 to 3.5 ms against 1.1, CPU 31 percent higher. Next is a profile, not another mechanism: `perf record` of the frontend under velo3 and under mux18p on one node pair, one rep each, read against the W2 ledger (`ingest-cost-ledger.md`: the per-record reader-pump hop, the drain doorbell per record, the per-batch slot walk, the accept-window task) before any of W2's items is implemented. W1 (lane sharding) is judged on the same profile.
4. Rig prerequisites, now blocking rather than noted: (a) the mocker backlog draw decides throughput, ITL, E2E and, through load, TTFT p50; a per-process admission cap or more processes with fewer workers each is needed before a three-rep mean means anything; (b) node pairs differ by 15 to 20 percent on throughput and the bar's absolute throughput term does not survive that; restate it as at or above mux18p's on the same matrix; (c) the proc_stat capture must cover aiperf's whole run (start it with aiperf, not with the frontend) so the CPU column is never "not captured".
5. Order from here: rig fixes (a) and (c), then the profile, then W2 items chosen by the profile, each as its own PR with a rig arm; W4b and W1 wait on the profile.

## Addendum 2026-09-06: the profile names W2's order

`t3-prof2` (diagnosis section 5) profiled both frontends on one node pair. The plane-specific costs on velo's frontend are, in order of size: the per-batch walk of every slot in `handle_batch` (`collect_grants` and its per-slot channel-length lock, 2.1 percent of the frontend's cores), the per-frame `timeout` in `reader_pump` (timer-wheel lock contention, 1.6 percent), per-record delivery into anchor channels (0.9 percent, mostly inherent), and the active-anchor gauge recomputed per anchor create and retire (0.5 percent). They sum to about 1.3 ms per request, half the CPU gap to mux18p; the other half is spread across allocation and the kernel networking path and has no single name.

Rulings.

1. W2 is built in that order, each item its own PR with a rig arm and a failing test: (d) touched-slot reconcile (the ledger's W1-B: reconcile the slots a batch delivered into, plus a small cursor stride, with the doorbell and the sweep as backstops); (a) a last-frame instant checked from one interval tick in `reader_pump` instead of a timeout per frame; (c) an atomic active-anchor count. Expected together: about 1.3 ms per request of frontend CPU and the event-loop delay those locks add.
2. W1 (lane sharding) is not on the list: the lane's own cost is the slot walk, which (d) removes, and the profile shows no core-bound lane stage.
3. Rig: `nats-server` and `etcd` are pinned to aiperf's half of the node from here on; they were unpinned and took 12 to 38 percent of the frontend's cores across the two profiled reps. Every CPU number before this pinning includes whatever share the scheduler gave them.
4. The mocker backlog and the capture-window fixes stand as prerequisites for the first-token comparison; the profile does not need them, because it compares composition, not throughput.

## Addendum 2026-09-06 evening: the batching review, and W2 in build order

A read-only review of both planes' batching paths (four readers, three proposers, two refuters per proposal, one critic; the run's full output is in the session's task record) confirmed the profile's order and corrected two of its numbers. The per-frame timeout in the reader pump is four to five times larger than section 5 booked: the whole `Sleep` subtree under the pump is 7.3 percent of the frontend's cores plus 0.8 for the cancellation future, against 1.8 for the pump's own work. The two "credit return" ideas (a touched-slot visit list, and an exact drain counter fed by the pump) are one change at one call site. And the data linger was rejected for a mechanism the lane-wait series contradicts (diagnosis section 6).

Rulings.

1. Build, in this order, each its own PR with a failing test and a fail-before run: (d) touched-slot reconcile in `handle_batch`, keeping the doorbell and the sweep as the full-walk backstops, no cursor stride (0.50 ms/req expected); (a) one pinned timer per stream in the reader pump, at three sites (the mux pump, the mpsc pump, the messenger lane loop), 0.8 to 1.2 ms/req booked against a 1.9 ceiling; then a Data-class-only record cap on the batch writer that must not touch credit-reply batches; then the data linger retried after (d) and (a) at a matched draw; then a terminal-class bit in the record header. Cumulative: 1.4 to 1.9 ms/req against a 2.3 ms/req gap, discounted by about a third for sampling over-attribution.
2. No first-token claim is booked for any of them. TTFT is compared only at a matched backlog draw (diagnosis section 6); until such a comparison exists, arms are ordered by CPU per request.
3. Not built: a credit-grant threshold (refuted; `BATCHING.md` already rejects it), a connection pool per peer, lane sharding, a bigger initial credit, header reshaping. Each was measured or traced as not on the path.
4. Started 2026-09-06: (d) on `w2d-touched-slot-reconcile` off `w8-control-map-bound`, (a) on `w2a-pump-timer-hoist` off `w3-zero-rtt-attach` (the pump file is W3's).

## Addendum 2026-09-06 night: (d)'s first cut starved the credit tail

Ruling 1 above said the doorbell and the sweep are enough backstops for a slot a batch did not touch. Measured, they are not: every stream on this workload needs one grant for its last four records, and that grant must ride the peer's next inbound batch, not a rate-limited per-peer walk (see the handoff for the numbers: credit exhaustion 13 to 20,500 per worker process, throughput halved, lane wait 0.36 ms to 1.4 s). The corrected rule for (d): the pump names the slot it drained (an exact atomic count plus a per-peer lane of slot indexes, no lock), and the batch handler reconciles touched plus listed slots. The full walk survives only on the periodic tick, where it is now lock-free. (a) stands as built.

## Addendum 2026-09-06 afternoon: verdict with one runtime

With the frontend on one tokio runtime and both W2 changes, velo3 is ahead of mux18p on first-token p50 at every matched backlog draw (39 to 42 ms against 46, three reps; results addendum of this date). Zero errors. The bar's first-token clause is met at a matched draw; the p99 clause is within spread; the throughput clause is draw-bound for both arms; the CPU clause is not met (12.3 to 14.0 against 9.5 to 10.0 ms/req).

Rulings.

1. The one-runtime fix is the largest single lever found in this campaign and it is not velo's: it belongs in dynamo's Python bindings (`DistributedRuntime` initialising the pyo3 bridge with its own runtime). Until it lands upstream it lives in the rig-local adapter. Every velo measurement from here on is taken with it.
2. W2 (a) and (d) stand as built, second cuts. (d) must never leave credit to the doorbell; (a) must never fire under traffic. Both are pinned by tests.
3. Next on CPU: the per-subtree attribution of `t3-prof5` decides the order. The frontend worker-thread count is measured first because it moves both arms and may be most of the environment shift.

## Addendum 2026-09-06 evening: the CPU order after the profile with one runtime

Diagnosis section 8 partitions the one-runtime profile. Rulings.

1. Before any velo change, measure the frontend worker count with three reps at 72 and 32 (queued). If 32 holds first-token latency at a matched draw, the rig runs at 32 from then on and the CPU clause is judged there.
2. Then, in order: (i) deliver a batch's records to a slot's consumer with one wake instead of one per record, so hyper flushes once per burst (bounded 1.0 to 1.45 ms/req in velo3's HTTP path; the site is `IngressSlot::apply_data` through the reader pump into the adapter consumer); (ii) delete the reader pump hop for mux slots and deliver straight into the anchor's channel (0.5 to 0.9 ms/req plus a share of the scheduler residual; the credit count then moves to wherever the record leaves the mux buffer); (iii) the per-record channel cost on the surviving hop (0.2 to 0.4); (iv) the cancellation-token walks (0.1 to 0.17). Each its own PR with a failing test and a rig measurement.
3. Not velo's, worth raising with dynamo: axum's graceful-shutdown watch is re-polled on every connection wake and costs 1.4 (velo3) to 2.2 (mux18p) ms/req; and the two-runtime frontend (section 7), which the rig-local adapter now avoids.
4. Not to pursue: flattening the MessagePack envelope, the anchor, the adapter consumer, or the scheduler as a target of its own.

## Addendum 2026-09-06 night: worker count settled; the CPU work is the hop chain

The three-rep matrices at 72 and 32 workers (results addendum of this evening) settle ruling 1 of the evening addendum: 32 workers cuts velo's CPU per request by about 2 ms but costs its p95 about 130 ms and its p50 lead; the rig stays at 72. The CPU program is therefore the hop chain itself, in the evening addendum's order: one wake per burst into the consumer, then the reader pump hop, then the surviving channel and the cancellation-token walks. The first-token clause of the bar is met at a matched draw on the final tree (39 to 44 against 46); the p99 clause is 40 to 70 ms short; the CPU clause is 1.9 to 2.9 ms short.

## Addendum 2026-09-06, later: the order after the per-record trace

Diagnosis section 9 replaces the evening addendum's item (i): there is no SSE-flush lever. Rulings.

1. Build now, in parallel: W2(e), one credit grant per half window instead of one per drained record (velo, branch `w2e-grant-threshold` off `w2d-touched-slot-reconcile`; the threshold is derived from the negotiated window, the periodic sweep grants any remainder, and a dated `BATCHING.md` addendum supersedes the earlier ruling against a record threshold); and the adapter's inline receiver (rig-local `dyn-pin`, belongs upstream with the one-runtime fix).
2. Then carry `Bytes` end to end so the frontend stops allocating a `Vec` per record.
3. Not to build: merging the reader pump into the anchor channel (the sole-writer credit invariant forbids it); a linger anywhere on the response path.

## Addendum 2026-09-06, late: the CPU clause is no longer a gate; W2(e) is judged on the tail

The author's ruling of this evening: a smaller frontend CPU per request is welcome when it costs nothing, but it is not worth any latency or tail. The bar therefore keeps TTFT p50 and p99 at or below mux18p's at a matched draw, throughput at or above mux18p's on the same matrix, and zero errors. Frontend CPU at or below mux18p's is recorded, not required.

Consequences for the work in flight (results addendum of this night, `t3-w2e72` and `t3-prof6`):

1. W2(e), PR #85 (one grant per half window), cut credit updates 19-fold and frontend batches 30-fold and moved CPU per request by nothing measurable. On the same nodes it raised ITL p99 from mux18p's level to 68 and 76 ms and worker credit exhaustion to as much as 1,431 per rep. A change with no upside on the bar and a cost on the tail does not merge. Matrix `t3-now2e72` (the same wheel without #85) decides whether the tail belongs to #85 or to the adapter's inline receiver. If #85 owns it, the PR closes with the measurement in its body. A quarter-window threshold is worth a run only if it keeps ITL p99 at mux18p's, and then only as a batch-count tidy-up.
2. The adapter's inline receiver (rig-local `dyn-pin`) removed about 0.2 ms/req of adapter work. It stays only if `t3-now2e72` shows the tails at their `t3-final72` levels without #85. Otherwise the consumer task and its mailbox come back from the backup under `.research/logs/adapter-inline/baseline/`.
3. The CPU levers left in the hop chain (folding the reader pump into the anchor, the surviving channel costs, the cancellation-token walks) drop to optional. They are taken only with a same-matrix tail check.
4. The latency items move up: TTFT p99 is 40 to 70 ms behind mux18p at 72 workers on every matrix since the one-runtime fix, and the credit tail (a stream's last records wait for a grant) is the mechanism that turns a slow reader into an ITL spike. The starved-slot urgent grant on #83 is the next change with a claim on the tail.
