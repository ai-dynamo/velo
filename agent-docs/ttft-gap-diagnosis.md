<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# TTFT gap diagnosis: why mux18p's first token is 10x faster than velo0's

Dated 2026-09-04. This document records the root-cause diagnosis of the TTFT gap measured in matrix `t3-m18p1` (job 2721416): mux18p p50 TTFT 86–168 ms, velo0 1,098–1,123 ms, tcp 106–1,342 ms, at comparable throughput. Four parallel investigations (per-request data decomposition, velo first-record code path, mux18p and tcp first-record code paths, mocker timing model) ran first. An adversarial verify pass then spot-checked the load-bearing claims and reconciled the explanation against eight cross-arm facts. Confidence: high. Full structured reports: session workflow `wf_72ac5040-593`; consolidated data artifacts under the session scratchpad (`FINDINGS.txt` and per-analysis scripts).

## The two questions asked, answered directly

**"Are we not flushing the batch at the end of a forward pass?"** Flushing is not the problem. velo0 maps `DYN_VELO_RESPONSE_FLUSH_INTERVAL_US=0` to `AutoFlush { on_admission: true, max_linger: None }` (`velo_response.rs:307-316`, `messenger_mux/mod.rs:181-188`). `should_flush()` returns true on every admission and the linger timer arm is `pending` forever (`flush_gate.rs:125-139`, `peer_batcher/mod.rs:355`). The `OpenSlot` record additionally gets its own eager flush before any data stages (`peer_batcher/mod.rs:527-542`). No timer exists anywhere on velo0's path.

**"Maybe a debounce trigger?"** No. Three controls close this: the t3e matrix ran the 1 ms linger variant against write-on-admission and both landed at ~1.06–1.08 s TTFT p50 — the whole knob range (0–1 ms) is a thousandth of the effect. mux18p itself runs a 1 ms batch ceiling and posts ~130 ms. And the linger deadline is absolute from batch start, so it cannot compound with queue depth in either plane.

## Measurement method

A full per-request four-point join was built for every rep: aiperf `x_request_id` → frontend log `request received/completed` lines (which carry the inner request id, the frontend's own ttft_ms, and `decode_worker_id`) → mocker `push_handler` lines. 250,000/250,000 requests joined per rep. Cross-node clock skew bounded below 1 ms (per-process min one-way delta 0.6–0.9 ms). TTFT equals `http_req_waiting` to 0.3 ms at every percentile in every arm: the first HTTP byte is the first token; no arm emits an earlier ack or role chunk (mocker emits nothing before the first generated token — `mocker.rs:1079-1225`).

The request lifecycle splits into A = client → frontend HTTP ingress, B = frontend → worker ingress (the request plane), C = worker ingress → first token at the client. At p50, in ms:

| rep | A | B | C | TTFT |
|---|---|---|---|---|
| rep1-tcp | 31 | 4 | **1,293** | 1,342 |
| rep2-tcp | 19 | 3 | 70 | 106 |
| rep1-velo0 | 13 | 8 | **1,019** | 1,110 |
| rep1-mux18p | 16 | 5 | **60** | 86 |
| rep3-mux18p | 25 | 8 | 109 | 153 |

B's p90 is ≤56 ms in every arm: the request plane is exonerated. The whole gap lives in C, the response side.

## The mechanism, per arm

### velo0: a standing backlog in the frontend's shared ingest, paid from the tail

The frontend ingest for the velo plane is a fixed-parallelism drain stage: one node-global unbounded `message_rx` (`velo-ext/src/transport.rs:680`) drained by exactly one `create_message_handler` decode/dispatch task (`messenger/server/mod.rs:57,173`), feeding one unbounded ordered-by-sender lane per mocker process — 8 lanes, one task each, strict FIFO, a per-peer mutex held across each batch apply (`messenger_mux/mod.rs:492-506`, `handlers/mod.rs:236-238`, `ingress/mod.rs:322-360`). At ~820k records/s this stage is the pipeline's slowest, so in the closed loop at concurrency 8192 the spare concurrency pools in front of it: ~3,414 requests awaiting first token plus ~3,350 fully generated but undelivered responses at any instant. 3,414 / 3,084 req/s = 1.11 s = the measured 1,110 ms p50, to within a percent.

A new request pays this backlog roughly twice before its first token reaches the wire:

1. The per-request `_anchor_attach` round trip (`anchor.rs:1150-1190`), which the worker awaits **before** `generate()` starts (`push_handler.rs:968-988`). The attach request crosses the backlogged `message_rx` (it dispatches via `SpawnedDispatcher`, not the ordered lane, and its reply returns on the response lane — so it costs about one backlog traversal plus a wire round trip).
2. The `OpenSlot` + `Prologue` + first-`Data` batch, which enters the shared FIFO **at the tail**, behind the standing backlog of established streams' records, and then the sender's ordered lane.

Every per-stream buffer downstream starts empty and is exonerated (slot 257, anchor 256, mailbox 64). The queue sits above the transport, which is why the velo plane over UCX/InfiniBand shows the same-order 737–798 ms: swapping the wire changes nothing about the drain stage. The backlog forms during aiperf's 0.25 s thundering-herd ramp and never drains; it is an immediate plateau, identical across reps, uniform across all 512 workers (per-worker p50 spread max/min 1.29), and flat against per-process load. In the warmup wave, with frontend in-flight below ~2,048, velo0's TTFT is 43–230 ms and it is the only arm that rises monotonically with in-flight — the ~1.1 s carries no fixed per-request cost at all; it is pure load-induced queueing. velo0's frontend also burns 51% more CPU than mux18p's over the same run (4,038 vs 2,671 CPU-s): ~4 task wakes and 2 allocations per record downstream of decode set the drain-rate ceiling that sizes the backlog.

### tcp: a bistable per-request connect/accept loop

The shipping plane opens a TCP connection per response against one frontend accept loop. The loop is bistable. Jammed regime (rep1): a standing ~3,900 connected-but-unaccepted sockets (frontend fd census 13.0k vs the clear regime's 16.3k accepted-per-request census), TTFT ≈ backlog/accept-rate ≈ 1.4 s, worker handler spans stretched to 4.0 s. Clear regime (rep2): every socket accepted promptly, TTFT 106 ms. rep3 walks between regimes mid-run. The apparent load dependence is the regime, not the cause: low load and low TTFT are joint outcomes.

### mux18p: nothing on the first-record path scales with load

Three properties, all verified in code and counters: (1) zero per-request wire setup — the frontend mints `stream_id` locally at request registration and carries it in the request envelope; worker-side sender creation is two DashMap inserts and two semaphores, no connect, no RTT (`mux_response/mod.rs:565-604`, `client.rs:830-889`; frontend fd count dead flat at 8,272 all run). (2) The prologue is fire-and-forget on an urgent lane that the writer drains before ordered data, `biased` (`client.rs:363-395, 1103-1126`) — it jumps any queued data, however deep. (3) The only shared FIFO a first data frame crosses is bounded in bytes, not streams: a 256 KiB per-connection byte semaphore (~4 writev widths), constant whether 50 or 1,000 streams are live (`wire.rs:36`, `client.rs:1031-1063`). Flow control never engaged in the run (`write_calls_total{frontend}=0`). So TTFT sits at the ~100–150 ms floor at every load.

## The trade mux18p makes (and the ITL caveat)

aiperf's per-request ITL is `(request_latency − TTFT) / (tokens − 1)`, so velo0's high TTFT mechanically shrinks its ITL. The genuine difference is admission discipline: velo0 parks ~40% of the concurrency before the first token, so only ~58% of requests stream at once, versus mux18p's 94–98%. That gives velo0 the matrix-best end-to-end p99 (6.8 s vs mux18p's 26.1 s) and the smoothest established streams; mux18p moves the same closed-loop wait inside the token stream (request_latency p90 18.2 s, wide ITL). Control: tcp rep2 at 97% streaming concurrency posts the matrix-worst ITL p99 (114 ms) on the same wire format as tcp rep1's 30 ms — streaming concurrency, not wire format, drives ITL width. Neither discipline is free; velo0's wait is front-loaded in a shared FIFO, mux18p's is spread across streams.

## What this run cannot measure

The split of velo0's ~1 s between the single `message_rx` decode task and the 8 ordered lanes is unmeasurable from these artifacts: zero `velo_streaming_*` / `velo_transport_*` series exist in the matrix (the frontend scrape does not include the velo registry; worker-side metrics are not exported — same instrumentation hole the UCX diagnosis hit). Both queues are the same class (unbounded shared FIFO in front of a fixed-parallelism drain), so the explanation does not depend on the split, but sizing the fixes does. Instrumentation is therefore the first workstream in the plan.

## Change candidates (verified mechanisms only; ranked; feeds the win plan)

1. **Shard the frontend ingest drain** — `_stream_batch` dispatch keyed by slot (or slot-hash shards sized to cores) instead of one lane per sender; split or move the single decode task. Per-slot ordering is all the protocol needs (per-slot `frame_seq` with `IngressSlot::park` already tolerates cross-slot reorder). Dominant fix: the backlog collapses when drain headroom scales with cores (frontend used ~31 of 144).
2. **Cut per-record ingest cost** — remove the per-frame `tokio::time::timeout` registration in `reader_pump` (`control.rs:336`); kill the Vec copy in `IngressSlot::deliver`; deliver a decoded batch to the anchor in one wake instead of ~4 wakes + 2 allocations per record; cancel the per-attach 60 s accept-timeout task on successful OpenSlot (`messenger_mux/mod.rs:964-984`) instead of holding ~180k live timers. 20–40% ingest CPU; near-saturation queueing is hypersensitive to service rate, so this compounds with (1). Also directly attacks the CPU-per-request category.
3. **Zero-RTT stream setup** — mint stream identity at the frontend at request registration, carry it in the request envelope, pre-bind the ingress slot, let the worker's first batch's OpenSlot claim it. Removes one backlog traversal plus a wire RTT from every request and removes worker-side pre-generate blocking. PR 11918's shape, on velo's protocol.
4. **Two-class ingest** — an urgent class for OpenSlot/Prologue/first-data (the worker batcher already has `stage_urgent`) that bypasses the data backlog at the frontend. Gets TTFT to the floor while keeping the admission-discipline E2E advantage — unlike mux18p's trade.
5. **Bound the shared ingest queues** (later; riskiest) — push the standing backlog upstream into per-stream slot inlets at the workers. Backpressure crosses the shared per-connection admission gate, so control traffic can be head-of-line-blocked; land only after (3) and (4).
0. **Prerequisite: instrumentation** — velo registry into the frontend scrape, worker-side metrics export in the rig, ordered-lane depth/wait gauges, and scrape the existing `WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS` histogram (`push_handler.rs:794`). Decides the message_rx-vs-lane split and validates each change.

## Fact reconciliation (condensed)

- velo0 tight ~1.1 s at 2.9–3.1k req/s: closed-loop equilibrium; backlog/throughput pinned by fixed drain parallelism.
- tcp bistable: different mechanism (accept path), regime-dependent.
- mux18p flat 86–168 ms: no load-scaling term on its path.
- linger == write-on-admission: flush knob range is 1000x below the effect; neither touches attach or ingest backlog.
- Tier-2 small scale (velo0 60 ms, tcp 41 ms): drain stage has headroom, backlog ~0; the ~19 ms residual is the attach RTT plus mux overhead.
- velo0 best ITL / E2E p99: admission discipline (58% streaming concurrency) plus the ITL arithmetic artifact.
- UCX same order (737–798 ms): the queue is above the transport.
- TTFT == http_req_waiting everywhere: no pre-token byte in any arm; the wait is upstream of the SSE writer.

## Addendum 2026-09-04 (evening): W0 measured the split, and the seat of the backlog is not the frontend ingest

W0 landed the missing instruments (velo PR #77 on top of `drain-credit-return`: `velo_messenger_inbound_dequeued_total`, `velo_streaming_anchor_attach_rtt_seconds`, the ordered-lane series now covering `_stream_batch`; the rig now scrapes the frontend `/metrics` once a second and every mocker process every two seconds during the run, and writes `velo_sha`, `velo_dirty`, and `dynpin_describe` into `rig_run_meta.json`). The analysis is `.research/analysis/w0/` (`w5_report.py` on a rep directory). Three one-rep probes ran at the published shape (`t3-w0-probe`, `t3-w0-probe2` reps 1 and 2) and one at concurrency 2048 (`t3-w0-probe-c2048`), all on velo `cd8c076`. Every number below is a mean over the steady middle half of the profiling phase unless it says p50.

**What the frontend FIFOs hold.** Probe `t3-w0-probe` (2,748 req/s, TTFT p50 958 ms, C mean 1,072 ms, C p50 856 ms): the node-global `message_rx` queue held a mean depth of 945 messages at 26.4k messages/s, which is 36 ms of wait by Little's law. The `_stream_batch` ordered lanes (8 of them, mean depth 285 batches each) had a mean wait of 110 ms (p50 64 ms). No records dropped, no batch-sequence gaps. The two frontend FIFOs together account for about 146 ms of the 1,072 ms, not the whole second the mechanism section above attributes to them. The claim "the whole gap lives in the frontend's shared ingest" is refuted by measurement; the claim that the gap is load-induced queueing above the transport stands.

**Where the rest is.** The worker-side attach round trip (stamped by the sender around the `_anchor_attach` send only) averaged 524 ms (p50 310 ms) in that probe, and 422 and 448 ms (p50 382 and 408 ms) in the two `t3-w0-probe2` reps, whose frontend FIFOs read 31 and 52 ms (`message_rx`) and 138 and 122 ms (lanes) against C means of 1,084 and 1,124 ms. Set that against a frontend handler body of 30 us and a frontend `message_rx` wait of 36 ms. Per mocker process the mean was 253 to 445 ms for seven processes and 1,723 ms on the KV-router hot process, and it scaled with that process's live slots (330 slots gave 253 ms, 800 gave about 430 ms, 1,887 gave 1,723 ms). At the same time every mocker process reported `velo_transport_send_backpressure_total` at 150 to 213 Pending/s, `velo_streaming_slot_credit_exhausted_total` at 28 to 95/s, and 19 to 257 withheld records, and the frontend's own outbound reported 116 Pending/s. At concurrency 2048 (2,222 req/s, TTFT p50 113 ms, C mean 109 ms) all of those read zero, the attach round trip was 28 ms on every process, the lane wait 18 ms, and `message_rx` 0.5 ms. The standing backlog therefore sits in front of the per-connection writer on the worker's egress (the bounded channel plus the admission gate's pending queue, which every velo transport has and which the UCX diagnosis already found unbounded on UCX), with a possible share on the reply leg in the frontend's egress. That is still "above the transport", which is why the UCX arm showed the same order of TTFT, and it is exactly the queue that mux18p's urgent lane bypasses and its 256 KiB per-connection budget bounds. The exact split between the worker egress queue, the socket buffers, and the frontend egress queue is the next measurement (W0b: an egress queue-wait histogram, a frames-written counter, a write-duration histogram, plus socket queue and node CPU sampling on both nodes).

**Consequences for the plan.** W1 (shard the frontend ingest) targets about 110 ms of lane wait; it is real but secondary and moves after the egress work. W3 (zero-RTT setup) removes the attach round trip outright, which is the largest single term measured (524 ms mean). W4's urgent class belongs at the transport writer on the worker, not at the frontend's receive side: an OpenSlot or attach frame must bypass the per-connection data FIFO, the way mux18p's writer drains its urgent lane before ordered data. W5 (bound the queue with backpressure) applies to the per-connection admission queue, not to the frontend lanes. W2's CPU items are unchanged; the frontend CPU category still stands at 8.4 to 11.4 ms/req across the four instrumented reps against mux18p's 6.5.

**Instrument perturbation.** The first instrumented rep read TTFT p99 4,597 ms and 2,748 req/s, outside the historical velo0 band (2,107 to 2,853 ms, 2,684 to 3,084 req/s). Two further instrumented reps read p99 2,491 and 2,639 ms at 2,918 and 2,988 req/s, and a control rep with the worker-side harvest off read 2,349 ms at 2,930 req/s. The outlier was the hot process (1,887 live slots on one of eight), the same router-imbalance mechanism the UCX diagnosis describes, not the instrument. The worker-side harvest stays on by default so every arm carries the same series.

**Corrections to the text above.** The "What this run cannot measure" section said zero `velo_streaming_*` series existed in the matrix; the frontend teardown dump in `t3-m18p1` did carry them (the dump's prefix filter dropped only the `velo_messenger_*` lane series). And `velo_streaming_anchor_operation_duration_seconds{operation="attach"}` was never the round trip: it is the frontend handler body (mean 30 us in that matrix), which is why W0 added the sender-side histogram.

## Addendum 2026-09-04 (night): W0b measured the egress side, and the wait is spread, not seated in one queue

W0b (second commit on PR #77) added `velo_transport_egress_queue_wait_seconds`, `velo_transport_frames_written_total`, and `velo_transport_write_duration_seconds` on the TCP and UDS connection writers, and the rig now samples socket queues and node CPU on both nodes. One instrumented rep at the published shape (`t3-w0b-probe`, velo `0160fa1`, 3,076 req/s, TTFT p50 992 ms, p99 2,363 ms, frontend CPU 7.69 ms/req) gives, as means over the steady middle half:

| stage | mean | note |
|---|---|---|
| worker egress queue (admit to writer dequeue) | 79 ms pooled, 41 to 130 per process | p50 0.4 to 35 ms: the mean is carried by a tail |
| worker write duration | 0.8 ms | the socket send buffer is not full for long |
| socket queues, worker side | 2.9 MB sent-unacked across all connections, max 484 KB on one | about 25 ms of records at the per-connection rate |
| socket queues, frontend side | 2.0 MB unread across all connections, max 318 KB on one | the frontend's readers lag the wire |
| frontend `message_rx` | 26 ms | Little's law, depth 530 messages |
| frontend ordered lanes | 112 ms (p50 66 ms) | unchanged from the earlier probes |
| frontend egress queue (replies, credit returns) | 42 ms (p50 0.4 ms) | the attach reply's return leg |
| worker attach round trip | 390 ms (p50 342 ms) | measured by the sender |
| worker handler start to first response | 485 ms | attach plus the OpenSlot flush wait plus first-token generation |
| C, worker ingress to first token at the client | 982 ms | from the per-request join |

Summed, the measured queues on the attach's path (worker egress, sockets, `message_rx`, frontend egress) come to about 170 ms of the 390 ms round trip, and the queues on the first token's path (worker egress, sockets, `message_rx`, lanes) to about 240 ms of the roughly 500 ms between the worker's first response and the client's first token. The remainder in both legs is not in any queue velo owns: it is time between a wake and a run. The node CPU sampler explains where it goes: node A (frontend, etcd, nats, and aiperf) ran at 95% utilization with a load average of 159 on 144 cores, while the frontend process itself used 24 cores; node B (workers) ran at 57%. The load generator is starving the system under test. Every arm shares this rig, so the arm-versus-arm comparison stands, but the arm with the most frontend CPU per request (velo0 at 7.7 to 11.4 ms/req against mux18p's 6.5) pays the most scheduler latency, which makes W2's CPU cuts a first-token lever as well as a CPU-category one.

Two consequences. First, the plan's order holds: W3 removes the attach round trip outright, which is still the largest single measured term, and W4a removes the OpenSlot flush wait that sits between the attach and generation; both are correct regardless of where the remaining latency comes from. Second, the rig needs one experiment before the next matrix: pin aiperf and the frontend to disjoint core sets (`taskset` exists in the image) and rerun `velo0` and `mux18p` once each. If pinning moves velo0's first token materially, the published scoreboard has been measuring load-generator interference and the matrix shape changes for every arm; if it does not, the interference is not the lever and the order above stands unchanged. Either way the change is gated and default-off until the result is in.

The `EGRESS_SEAT` verdict from `w6_egress.py` is `undecided` by its own rule (no single term explains 60% of the attach round trip), which is the correct reading.

## Addendum 2026-09-04 (late night): the seat of the TTFT gap was the rig, and the scoreboard is withdrawn

The core-pinning experiment (`t3-pin1`, one rep each, frontend on cpus 0-71 and aiperf on 72-143 via `taskset`, everything else unchanged) settles it:

| arm | pinning | req/s | TTFT p50 | TTFT p99 | E2E p50 | E2E p90 | E2E p99 | ITL p99 | frontend CPU ms/req |
|---|---|---|---|---|---|---|---|---|---|
| velo0 | off (t3-w0b-probe) | 3,076 | 992 | 2,363 | 2.36 s | 4.14 s | 5.45 s | 16.6 | 7.69 |
| velo0 | on | 2,933 | 98 | 813 | 1.69 s | 6.84 s | 11.61 s | 43.9 | 5.85 |
| mux18p | off (t3-m18p1 rep1) | 2,360 | 86 | 1,697 | 0.54 s | 18.2 s | 26.1 s | 99.3 | 6.51 (3-rep) |
| mux18p | on | 2,762 | 49 | 768 | 0.44 s | 10.5 s | 11.25 s | 42.8 | 5.27 |

Every velo0 first-token number published before this date was load-generator interference: aiperf, with 64 worker processes and 64 record processors on the same node as the frontend, took the node to 95% utilization and starved the frontend's runtime of CPU. With disjoint cores velo0's first token is 98 ms at the same throughput, and its frontend CPU per request falls from 7.7 to 5.9 ms because the process stops paying for preemption. The mechanism sections above that place the second in velo's ingest FIFOs, and the evening addendum that moved it to the worker egress, both describe real queues whose measured occupancy was small; the wait between the queues was scheduler time on a saturated node.

The end-to-end tail moves the other way. velo0's E2E p99 rises from 5.5 to 6.8 s to 11.6 s under pinning, and mux18p's falls from 26 s to 11.3 s, and both ITL p99 values land near 43 ms. The "admission discipline" this document credited to velo0 was the starved frontend throttling how many streams ran at once. With the frontend fed, both planes stream about the same population and pay the same tail. That advantage is withdrawn along with the scoreboard.

What remains true and measured: at the published shape with pinning, velo0 leads throughput (2,933 against 2,762) and trails mux18p on first token (98 against 49 ms p50, 813 against 768 ms p99) and frontend CPU (5.85 against 5.27 ms/req). The remaining first-token gap has the shape the instruments now resolve: the attach round trip is 22 ms mean under pinning, the OpenSlot flush wait sits on top of it, and the ordered lanes and the anchor path carry the rest. W3 (no attach round trip) and W4a (no flush wait before the ack) target exactly those terms; W2 targets the CPU gap. The three-rep pinned baseline (`t3-base-pin`, tcp, velo0, mux18p) replaces `t3-m18p1` as the scoreboard, and `RIG_PIN_CORES` defaults to 1 in `t3-frontend.sh` from now on, with the 72/72 split recorded in `rig_run_meta.json`; a 48/96 split runs as a sensitivity check.

Corrections to the record: `response-plane-benchmark-results.md`'s t3-m18p1 table and the results page are superseded by `t3-base-pin`; the first-token and E2E rows of the plan's scoreboard are withdrawn; the isolation matrices for W2, W3 and W4 run under pinning, and their success bar is set against mux18p's pinned numbers once the three-rep baseline is in.

**Split sensitivity (`t3-pin2`, frontend on 48 cores, aiperf on 96, one rep each).** velo0: 3,008 req/s, TTFT p50 104 ms, p99 974 ms, E2E p99 8.1 s, ITL p99 30.5 ms. mux18p: 2,460 req/s, TTFT p50 49 ms, p99 937 ms, E2E p99 25.2 s, ITL p99 96 ms. The first-token result does not depend on the split (velo0 98 to 104 ms, mux18p 49 ms at both splits). The tail does: with the frontend held to 48 cores, mux18p's end-to-end p99 returns to 25 s and its throughput falls to 2,460, while velo0 holds 3,008 req/s and an 8.1 s p99. So velo0's tail discipline is real, but conditional: it shows when the frontend is CPU-constrained, and it disappears when the frontend has headroom (the 72/72 split, where both planes post about 11.5 s). The 72/72 split stays the default because it gives the system under test headroom; the conditional advantage is worth stating on its own line rather than as a category win.

## Addendum 2026-09-05 (afternoon): `t3-iso1` — the velo4a and velo34 errors are a control-cap leak, and the end-to-end tail is one mocker process

Matrix `t3-iso1` (job 2729436; arms velo0, velo3, velo4a, velo34, mux18p; three reps each; 512 workers, concurrency 8192, 250,000 requests per rep; pinned 72/72; velo `379240a`, the integration branch carrying W0, W3 and W4a, with rig-local modifications; dyn-pin `3a67ae2e6e` with rig-local mods). The per-rep table is in `response-plane-benchmark-results.md` and the verdict is in the plan. This addendum records the two mechanisms the matrix exposed.

### 1. The HTTP 500s on velo4a and velo34 are a leak in the peer batcher's control inbox, reachable only through a fenced slot

What the client saw: velo4a rep 1 had 16 errors and velo34 reps 2 and 3 had 95 and 186, every one `500 Failed to generate completions`. Every other rep of every arm had zero.

What the frontend logged, once per error: `reader_pump: heartbeat watchdog fired, injecting Dropped` with `anchor_frame_tx_len=0` and `transport_rx_len=0` (the anchor was idle, not saturated), then `dynamo_llm::migration: Creating new stream, retrying error=CannotConnect: ... velo response stream error: sender dropped`, then the 500. The watchdog counter equals the error count in each rep (16, 95, 186).

What the worker side showed: in each error rep exactly one mocker process ends the run with `velo_streaming_mux_control_refused_total` above zero (3,719; 4,787; 11,205), `velo_streaming_mux_withheld_records` stuck (4,191; 24,403; 44,563) and live slots that never close (16; 18; 69). The stuck records are the whole streams of the failed requests: 186 requests times about 240 records is 44,600, against 44,563 measured. On velo34 rep 3 the process's refusals begin at t=1788606923, when it holds 4,454 live slots; the frontend's first watchdog firing is at t=1788606939, 16 s later, which is the three missed 5 s heartbeat windows the watchdog needs (`DETECTION_MULTIPLIER` 3, default heartbeat interval 5 s) plus one scrape.

The mechanism, from `peer_batcher/control.rs` and `peer_batcher/mod.rs`. Control for a peer batcher is a pair of maps keyed by slot id, each capped at `MAX_PENDING_CONTROL` = 4,096 entries; a new key past the cap is refused and counted. The cap was sized for a peer with about 1,024 live slots. Under `async_open_ack` every open fences its slot and reports the `OpenSlot` admission back through `ControlInbox::singleton_resolved`, which went through the same capped entry as a peer's credit grant. With the map at its cap the resolution is refused, nothing else lifts the fence, and every record the slot ever queues is withheld until the consumer's watchdog gives up 15 s later. The same path serves over-budget rendezvous singletons (`send_singleton`) in the default configuration, so the hazard predates W4a; it was never hit because `velo_streaming_mux_rendezvous_singletons_total` is zero in every rep.

Why the map is at its cap: one mocker process per rep holds 4,000 to 6,700 live slots (section 2), and the frontend returns credit per drained record for each of them, so the owned-control map on that process legitimately exceeds 4,096 keys between two drains. Refusals then land on grants (harmless here: `initial_credit` 256 equals the output length, so a 256-token stream needs no returned credit), on closes, and on the W4a arms on open resolutions. The frontend's own control map for that peer refuses too (`velo_streaming_mux_control_refused_total` on the frontend: 792,305 on velo34 rep 3, 324,649 on velo3 rep 3, 7,200 on velo0 rep 3), which loses credit replies and closes for the hot process's slots. With `initial_credit` at the output length this costs nothing measurable, but it is the second half of the same assumption and is a follow-up in the plan.

Fix (on `w4-async-open-ack`, PR #79): a singleton resolution is exempt from the cap (`entry_mine_owed`). It is this side's own answer to a fence it raised, at most one per fenced slot, so growth past the cap is bounded by fenced slots. Tests: `a_singleton_resolution_is_never_refused_at_the_cap` (inbox) and `the_fence_lifts_when_the_admission_answers_into_a_full_control_map` (batcher; the map is filled past the cap while the batcher is parked at its test barrier, then the parked `OpenSlot` is admitted). Fail-before evidence: `.research/rig/failbefore-w4a-cap.sh` reverts the exemption and runs both tests plus two control cases beside them. In its first run (job 2729999) the inbox test failed as intended and the batcher test did not compile, because the review workflow was editing the same worktree during the run. The clean run on the integration tree (job 2730177, commit b68d94b) has the inbox test failing at its assertion, the batcher test failing with `timed out waiting for an admitted frame` (the fence never lifted), both control cases passing, and the pass-after gate green (fmt, clippy, the peer batcher, mux and credit test binaries).

What this means for the arm numbers: velo4a and velo34 as measured carry leaked slots (each failed request keeps a fenced slot, its withheld stream and its live-slot count until the process exits), so their E2E and ITL tails and their CPU are not clean. Their TTFT p50 is the healthy path and is reported with that caveat. Both arms rerun after the fix.

### 2. The end-to-end tail belongs to one mocker process, in every arm, and does not attribute to the response plane

Arrivals are equal: per 8 s bucket each of the eight mocker processes logs the same number of `request received` lines to within one (the frontend round-robins over 512 endpoints). In-flight requests (received minus completed, from the same logs) are not: seven processes sit at 150 to 200 for the whole run and one sits at 3,000 to 7,000. This holds for every rep of every arm, including tcp (`t3-base-pin` rep 2: proc 7 at 5,500) and mux18p (`t3-base-pin` rep 1: proc 7 at 6,900; `t3-iso1` rep 3: procs 6 and 7 at 2,600 to 3,000 and 3,500 to 4,300), and velo0 (`t3-base-pin` rep 1: proc 1 at 7,100). Which process it is varies by rep. Since the plane does not change it, it is the mocker process: eight Python processes packing 64 workers each, and a process that falls behind during the ramp stays behind because it completes exactly what it receives.

By Little's law the residence time on the hot process is 6,500 / 436 req/s, about 15 s, against 0.43 s on the others. The hot process is one eighth of all requests, so E2E p99 (9 to 21 s across reps) and ITL p99 (32 to 82 ms) are inside that population and measure which process got the backlog and how large it is. They do not measure the plane. TTFT p50 is the healthy seven eighths. TTFT p95 and p99 (165 to 235 ms and 707 to 869 ms in every arm, mux18p included) are probably the hot process's first tokens as well; the per-worker join (`extract.py`, `out-iso1`) can settle that and is queued.

This is also what puts velo34's live slots over the control cap in section 1: the hot process's 6,000 live slots are the backlog, not a velo effect. The E2E p99 criterion in the plan is withdrawn for this rig until the backlog is addressed on the rig side (fewer workers per mocker process, or a per-process admission limit), and the ITL caveat of the original diagnosis is withdrawn for the same reason.

### 3. Where velo3's first-token time went, and where it came back: the request path pays for the response path

The per-request join (`.research/analysis/ttft-join`, `out-iso1`) splits client TTFT into A (client send to the frontend's `request received` line), B (that line to the worker's `request received` line, skew-corrected per mocker process) and C (worker ingress to the first token at the client); the three sum to the client's TTFT per request. Medians per rep, then the three-rep mean, milliseconds:

| arm | A per rep | B per rep | C per rep | A mean | B mean | C mean | client TTFT p50 mean |
|---|---|---|---|---|---|---|---|
| velo0 | 2.0, 2.7, 2.4 | 1.9, 1.3, 1.2 | 79, 87, 67 | 2.4 | 1.5 | 78 | 85 |
| velo3 | 10.2, 13.1, 10.1 | 8.2, 11.1, 10.1 | 47, 47, 49 | 11.1 | 9.8 | 48 | 69 |
| velo4a | 3.1, 5.2, 4.4 | 1.3, 1.4, 1.4 | 72, 87, 86 | 4.2 | 1.4 | 82 | 91 |
| velo34 | 10.6, 10.6, 9.6 | 8.6, 8.1, 5.2 | 49, 43, 25 | 10.3 | 7.3 | 39 | 59 |
| mux18p | 5.3, 4.3, 3.6 | 0.7, 0.5, 0.5 | 45, 42, 43 | 4.4 | 0.6 | 43 | 48 |

Zero-RTT setup takes C from 78 ms to 48 (velo3) and, with the detached ack, to 39 (velo34; 25 in its best rep), at or under mux18p's 43. The client sees 16 to 26 ms of that, because A and B grow in every zero-RTT rep, by about 9 ms and 8 ms: the request path from the client to the worker goes from 4 ms to 18 to 21 ms. That growth is the remaining gap to mux18p (velo34 59 = 10.3 + 7.3 + 39 against mux18p 48 = 4.4 + 0.6 + 43). The detached ack on its own (velo4a) leaves C where velo0 has it (82 against 78), so what it buys shows only once the attach round trip is gone (velo34 against velo3, 9 ms on C).

What changed on the frontend under the gate, from the scrapes: it sends ten times more mux batches to the workers (velo3 rep 2: 1,027,872 outbound `_stream_batch` messages against 97,042 for velo0 rep 2; velo34 rep 1: 1,049,277) carrying 29 percent more records (22.4 M against 17.4 M); the sent-batch size distribution moves from a median near 100 records to 240,000 one-record batches and 740,000 of eight or fewer; the frontend's event-loop delay mean doubles (1.46 to 2.9 ms); node A stays at 47 to 49 percent busy in both. Drain visits are similar (127,000 to 142,000 against 178,000 to 181,000), so the credit sweep's cadence is not the multiplier, and the frontend records no drops, stalls, holds or refusals in the clean zero-RTT reps. Per stream the frontend goes from 0.4 to 4 control batches; every one is an inbound message on the worker and a wake on the frontend's batcher. The worker's TTFR (handler start to first response) is 3 ms under W3 against 20 ms, so the worker side of the request path is not where B grew; B and A grow together on the frontend, consistent with a runtime that is servicing far more small sends.

Which reply multiplies is not settled by the counters that exist: the batcher counts batches and records per direction, not records by type. The candidates, from the code, are the `CloseSlot` a pre-bound anchor's drop sends when its slot is still claimed (`PreBind::drop`, `close_claimed_slot`) and fragmentation of the credit replies across batcher wakes. The next instrument is a `velo_streaming_mux_records_sent_total{record_type}` counter and a batcher wake counter by source, one rep of velo3, and then the fix. This is the largest first-token lever left: recovering the request path returns velo34 to about 48 ms at p50, which is the bar.
