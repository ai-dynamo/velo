# Velo vs Dynamo QUIC: where the frontend cost goes (2026-09-26)

Work branch `perf-streamline` from da848eb (the velo pin of dynamo PR 15231). Never merges; carry the lasting parts into the book.

## The gap being chased

Dynamo report (jthomson, 2026-09-23, 2 frontends at 72 cores each, 2,048 speedup-10 mockers, ~918 tokens/request): Velo TCP 4,241 req/s, 32.0 ms frontend CPU/req; Dynamo QUIC 4,582 req/s, 27.0 ms. The velo frontend ran at 136 of 144 cores, so CPU per record sets throughput. Velo RDMA cost the same CPU as velo TCP, so the transport is not the gap.

Config difference: jthomson's adapter uses `MuxConfig::default()` (`AutoFlush { on_admission: true, max_linger: None }`), so workers write small batches. Dynamo QUIC lingers bulk data 5 ms (`DYN_QUIC_RESPONSE_BATCH_INTERVAL_US=5000`) but sends each request's prologue and first data frame at once on a priority lane. QUIC has no per-request credit (16,384-deep channel, reset on overflow).

## Microbench

`.research/rpb` (rig-local): frontend process with prebound anchors polled like the adapter; engine process with N velo nodes, one step clock per node (per-stream timers capped the process near 200k ticks/s), `ResponseFrame::Data(Bytes)` of 160 B, 918 tokens per stream, jemalloc, report rustflags, run bare on a compute node so host `perf` works (`submit-bare.sh`).

Frontend cost per record (2,048 streams, ~1.25-1.55M records/s):

| setup | records/batch | fe us/record | notes |
|---|---:|---:|---|
| 12 peers, no linger | 23 | 6.8 | base |
| 12 peers, no linger, pump removed (prototype) | 22 | 6.0 | -12% |
| 48 peers, no linger | 5.8 | 9.2 | sys 2.0 us/record |
| 48 peers, 1 ms linger | 59 | 5.5 | |
| 48 peers, 5 ms linger | 154 | 5.2 | |
| 48 peers, 5 ms linger, pump removed | 158 | 4.3 | |

Per batch the frontend pays about 24 us (about 10 us system); per record about 4-5 us. Records per consumer wake stay at 1.0-1.2 in every setup: a slot's records are spread across a batch, so each record wakes the consumer.

## Profiles (perf, frame pointers)

12 peers, no linger: reader pump 28% (flume `try_send` into the anchor channel and its wake 11.5%, `recv_async` 10%, cancel future 3%), lane `handle_batch` 20% (slot `try_send` and pump wake 11%), consumer 18%, scheduler self 11%, TCP read 4%, memcpy 0.5%. Copies are not a lever.

48 peers, no linger: TCP listener 18% (recv syscall 11%, teardown future rebuilt per frame 5%), lane 20%, pump 21%, batcher 6% (credit replies), consumer 9%.

Pump-removed prototype: consumer 35%, of which `DrainSignal::drained` 13% (per-record listing on a per-peer flume lane shared by ~170 consumers plus a shared `pending` flag) and flume receive 14% (two channels polled per wake).

## Landed on the branch (test first)

- `7a691b7` sentinel check: an `Item` no longer runs a failing `rmp_serde` decode that formats a `String` per record (both ends). Fail-before: allocation count > 0. Under jemalloc the bench gain is within noise.
- `40172c5` TCP listener: teardown future pinned once per connection. Fail-before: 51 arms for 50 frames.

- `3867d3f` + `820025c` lever 3, credit posting: the per-peer flume dirty lane and per-slot `listed` flag became one lock-free `DirtySlots` bitmap; only the drain that newly lists a slot touches the shared `pending` flag; the drain wake lane is unbounded so a listing never loses its wake. Fail-before: a drain of a listed slot wrote `pending`; wake 1,024 was refused. Bench (3 reps, fe us/record median, ranges): 12 peers 6.66 (6.54-6.67) -> 5.82 (5.70-5.86), -12.5%; 48 peers 8.60 (8.43-9.03) -> 8.11 (7.67-8.13), -5.6%. Adversarial review (fable): no bug; the wake-full degraded mode it found is what `820025c` removes.

## Rig A/B, jthomson's adapter vs Dynamo QUIC (velo-base, 2026-09-25)

One frontend, 8 mocker processes x 64 speedup-10 workers, concurrency 8192, ISL 1024, OSL 900, jemalloc, report rustflags. Results in `.research/results/t3-pr15231-ab`.

| arm | req/s | ITL p50 / p99 ms | frontend cores | frontend CPU ms/req |
|---|---:|---|---:|---:|
| Dynamo QUIC | 2,296 / 2,661 | 1.34-1.37 / 3.0-3.1 | 30-35 | 13.1-13.3 |
| velo (jv) | 1,462 / 1,559 | 4.36-4.39 / 6.9-16.1 | 43-46 | 27.4-31.5 |

Velo loses by about 64% here, and the loss is latency-bound, not CPU-bound: the frontend has idle cores. Mean ordered-lane wait on the frontend is 1.25 ms per batch (2,820 s over 2.25M batches, about 104 records per batch). Worker credit exhaustion about 32k per rep over 250k streams. No second tokio runtime on the per-record path (the extra `tokio-rt-worker` threads are rayon's pool and the small etcd/NATS runtimes inheriting the thread name). **Resolved (2026-09-26): the rig is worker-node bound, and velo's worker-side cost grows with streams per process.**

- Both arms saturate the mocker node (about 134 of 144 cores, every mocker process at 16-17 cores in the 8-process shape, 8.2-8.4 cores in the 16-process shape).
- `peers16`, the same 512 workers as 16 processes x 32: velo 2,231 req/s, TTFT p50 94 ms, ITL p50 / p99 1.27 / 3.77 ms, fe CPU 20.3 ms/req; QUIC 2,247 req/s, 91 ms, 1.28 / 3.13 ms, 15.1 ms/req. Parity on throughput and latency.
- So at 8 processes (up to 2,645 streams per process) velo's worker-side CPU per request is about 1.6x QUIC's; at 16 processes it is equal. The frontend CPU gap (20.3 vs 15.1 ms/req) matches the published 32 vs 27.
- The frontend's ordered lanes are not CPU-bound on the rig (2.5% of 72 cores for 8 lanes); the 1.25 ms mean lane wait is scheduling delay behind a runtime 87% busy with Dynamo's HTTP pipeline. Velo is about 12% of frontend CPU on the rig (reader pump 5.4%, lane 2.5%, listener 0.4%, batcher 0.2%, anchor polling inside the HTTP body).
- Next: profile a mocker node in the 8-process shape (`wprof1`) to find what grows superlinearly with streams per process.

## Levers 1 and 4, and the frontend-bound rig (2026-09-26)

Commits: `a2b895a` direct feed, `d3de70e` withdraw fix (adversarial review), `55e1253` reader_pump cleanup, `3a2b366`/`d1a5d52` docs, `d63f112` lever 4 cheap half, `4df8aa5` wait_for_handler.

Bench (fe us/record median of 3): lever 1 5.87 -> 5.33 (12 peers), 7.71 -> 7.38 (48); lever 4 5.34 -> 5.20 (12), noise (48). Cumulative from da848eb: 6.66 -> 5.20 (-22%) at 12 peers, 8.60 -> 7.30 (-15%) at 48. Messenger per-batch allocations measured at ~0.35% of frontend CPU together: left alone.

Rig, 16 x 32 workers, OSL 900 (tables in `.research/results/t3-<tag>/ab-table.md`):

| tag | frontend | build | QUIC req/s | velo req/s | velo vs QUIC fe CPU | notes |
|---|---|---|---|---|---|---|
| fe32base | 32 cores | da848eb | 2,154 / 2,178 | 2,140 / 2,138 | 13.6-13.8 vs 12.0 | worker node 93-96% busy |
| fe32l1 | 32 cores | 55e1253 | 2,243 / 2,261 | 2,331 / 2,220 | 12.6-13.1 vs 11.7 | velo ITL p99 ~4 ms vs ~6 ms |
| fe24l1 | 24 cores (frontend-bound) | 55e1253 | 1,914 / 1,660 | 1,997 / 2,024 | 11.3-11.5 vs 11.5-13.7 | velo TTFT p50 219-640 ms vs QUIC 83-93 ms |

TTFT under frontend saturation: the frontend's `transport_roundtrip` stage (request-plane send to first response frame polled) was 468 / 220 ms mean for velo against 62 / 71 ms for QUIC. Frontend ordered-lane wait was ~25 ms per batch during load, inbound queue empty, so the lane explains a part only. Cause found: jthomson's adapter calls `Velo::wait_for_handler(peer, "_stream_stop")` before every generate, and `wait_for_handler` always refreshed with a full `_hello` round trip through the (saturated) frontend's messenger. `4df8aa5` returns at once when the known handler list names the handler. Rig A/B of that fix: `fe24l4` (before, d1a5d52) vs `fe24hf` (after, 4df8aa5), 4 reps each with QUIC as the fixed reference in each allocation.

## Where velo's first token waits under a saturated frontend (2026-09-26, `fe24dc`)

Rig-local instruments (Dynamo tree, `.research/rig/dyn-15231-rig-local.patch`): the worker observes handler entry to the engine's first item and to that item's `send` returning, next to the frontend's `request_plane_roundtrip_ttft`. `.research/rig/ttft-decomp.py` prints the split. Dynamo's own `time_to_first_response` stops at the prologue, not the first token, so the rig could not separate mocker queueing from the response path before this.

fe24dc: 24 frontend cores, 16 x 32 workers, arms dq, jv, and jl (jv with `on_admission: false, max_linger: 5 ms`), 3 reps each, interleaved.

| rep | req/s | TTFT p50 | ITL p99 | worker first token sent (ms) | response path (ms) | worker egress wait (ms) | FE CPU ms/req | node A irq cores |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rep2-dq | 1,814 | 80 | 27.8 | 31.8 | 22.8 | - | 12.0 | 5.9 |
| rep3-dq | 1,164 | 92 | 54.1 | 50.9 | 43.1 | - | 20.0 | 15.6 |
| rep2-jv | 1,444 | 815 | 20.9 | 44.2 | 666 | 194 | 16.2 | 12.9 |
| rep3-jv | 2,100 | 130 | 4.8 | 9.4 | 68 | 6.4 | 10.9 | 5.2 |
| rep2-jl | 2,036 | 158 | 5.8 | 8.9 | 114 | 80 | 10.6 | 5.1 |
| rep3-jl | 1,346 | 509 | 45.8 | 18.5 | 529 | 345 | 17.3 | 13.2 |

Findings:

- The mocker is not where velo's first token waits. The worker has it on the plane about 10 ms after the request arrives; it then spends 68-666 ms reaching the frontend.
- That time follows the worker's transport egress queue wait, and the egress queue is backpressure: in rep2-jv the frontend's receive queues on the 16 velo connections held 3.2 MB on average (440 KB in rep3-jv) and the worker's send queues 5.0 MB (686 KB). The frontend reader is not draining. A new stream's first record waits behind every bulk byte ahead of it in the same per-peer FIFO.
- The rig ran in two states. Bad reps, in every arm including QUIC, show 16-20 ms of frontend CPU per request against about 11, and 13-16 cores of irq/softirq on node A against 5-9. Three reps per arm that mix states average two regimes, so fe24dc gives no arm verdict. Per-CPU softirq capture is added to the rig to test whether NET_RX lands on the frontend's pinned cores.
- Under the same stress the planes fail differently: QUIC holds TTFT near 90 ms and pays in ITL p99 (54 ms); velo holds ITL p99 and pays in TTFT. QUIC's priority connection carries FirstData around the bulk lanes.
- The 5 ms window does not help: it raised records per batch to 217-238 and left egress wait and TTFT where the environment put them. The "eager first records" flush option (design question 2) cannot help either, since the first record is not waiting on the flush. Not built.
- Rig defect found on the way: `RIG_WORKER_METRICS_BASE_PORT` 9090 with 16 processes covered 9100, held by node_exporter, so proc10 was never scraped. Every summed worker metric in 16-process runs missed a sixteenth. Default moved to 19090.

Remaining unexplained: response path minus egress wait leaves 34-472 ms. The next build adds frontend adapter histograms (register to prologue received, register to first data polled) to place it.

## Open design questions (need a ruling)

1. Remove the reader pump from the mux data path: the consumer reads the slot buffer and posts drains; the pump survives only as a lifecycle/watchdog task stamped from ingress. Prototype (`proto-direct` branch, watchdog dropped) measured -12% to -18% frontend CPU. Reopens `batched-streaming-design.md:79-83` by a different mechanism; buffering per stream shrinks from C+1+256 to C+1.
2. Worker linger with an urgent first record: bulk data lingers (1-5 ms), a slot's first records flush at once, as QUIC does. Cuts per-batch cost 40% in the bench. The book rejected a plain 500 us data linger for TTFT; this variant keeps the first token eager.
3. Cheaper credit posting: `drained()` per record costs 13% of the prototype. Options: lock-free dirty set, or reconcile touched slots and post only past a threshold. Touches the #83 credit-on-next-batch design and the credit-tail memory.
4. Per-batch receive overhead (~24 us/batch): dispatcher and lane hops (2 wakes per batch), messenger allocations per batch, `with_label_values` per batch.
