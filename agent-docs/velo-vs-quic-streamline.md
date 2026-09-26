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

## Open design questions (need a ruling)

1. Remove the reader pump from the mux data path: the consumer reads the slot buffer and posts drains; the pump survives only as a lifecycle/watchdog task stamped from ingress. Prototype (`proto-direct` branch, watchdog dropped) measured -12% to -18% frontend CPU. Reopens `batched-streaming-design.md:79-83` by a different mechanism; buffering per stream shrinks from C+1+256 to C+1.
2. Worker linger with an urgent first record: bulk data lingers (1-5 ms), a slot's first records flush at once, as QUIC does. Cuts per-batch cost 40% in the bench. The book rejected a plain 500 us data linger for TTFT; this variant keeps the first token eager.
3. Cheaper credit posting: `drained()` per record costs 13% of the prototype. Options: lock-free dirty set, or reconcile touched slots and post only past a threshold. Touches the #83 credit-on-next-batch design and the credit-tail memory.
4. Per-batch receive overhead (~24 us/batch): dispatcher and lane hops (2 wakes per batch), messenger allocations per batch, `with_label_values` per batch.
