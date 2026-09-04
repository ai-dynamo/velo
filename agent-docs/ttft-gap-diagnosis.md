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
