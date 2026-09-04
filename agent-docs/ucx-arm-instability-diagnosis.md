<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# UCX arm instability: diagnosis and ruling

Dated 2026-09-04. This document records the root-cause diagnosis of the `ucx` arm's instability in matrix `t3-ucx1` (job 2711829): 1,722 HTTP-500 errors in rep 1 and p99 TTFT swings of 9,151 / 2,016 / 7,678 ms across the three reps. Three independent investigations (error forensics, transport code read, environment resolution) ran in parallel. An adversarial verify pass then spot-checked their load-bearing claims against the logs and code before the ruling.

## Ruling

**Velo-side defect. Fix first, do not tune.** No tuned `ucx` rerun joins the final matrix. The three candidate knobs all fail the evidence bar: the flush interval addresses batch collapse, but the clean rep ran with batches just as collapsed. `UCX_TLS` pinning addresses nothing observed, because the UCX config was identical across all three reps and rep-to-rep variance tracks load placement, not transport selection. The two knobs that map onto the implicated mechanisms (`peer_byte_budget`, the heartbeat deadline) have no env surface in this deployment.

## The causal chain

1. **KV-router imbalance concentrates ~5,760 concurrent streams on one of the eight worker processes.** This imbalance is arm-independent: rep2-tcp put 7,529 on one process and rep3-velo0 put 5,350. Evidence: per-process in-flight counts from `mocker_*.log` push-handler lines.
2. **All 64 packed workers in a process share one velo node and one mux peer link** (dyn-pin `velo_response.rs:791`, `static PROCESS_VELO`). The blast radius is exactly 1/8 of the fleet. All 1,722 failures sit in `mocker_2`, spread uniformly (~27 each) over its 64 instance ids.
3. **Over UCX that one peer link cannot keep up.** Delivery lag (frontend HTTP completion minus worker handler completion) on the hot process grows monotonically to a median of 18.87 s. The controls isolate the transport: at comparable or higher hot-process concurrency, velo0-over-TCP peaks at 1.0-2.75 s of lag and native dynamo TCP at 0.04-1.02 s, with zero errors. rep3-tcp is decisive: 7,698 in-flight and 29.45 s of worker queueing, yet 0.04 s delivery lag.
4. **Streams whose first frame sat behind more than 15 s of backlog showed total silence at the frontend anchor.** Heartbeats ride the same per-stream channel and per-peer FIFO as data, so a saturated peer silences the liveness signal too.
5. **The reader-pump watchdog (3 x 5,000 ms, `streaming/control.rs`) killed every affected anchor.** `velo_streaming_heartbeat_watchdog_firings_total` = 1,722 in rep 1 and 0 in reps 2-3. All 1,722 WARN lines show `anchor_frame_tx_len=0, transport_rx_len=0`: every frontend-side queue was empty, so the silence was genuinely upstream. `closed_slot` drops = 439,110 = 1,722 x 255 exactly, so not one record had arrived before the kill.
6. **Dynamo surfaces each kill as HTTP 500** ("Failed to generate completions", latency band 15,007-16,448 ms, `input_tokens=0 output_tokens=0`). Migration is disabled (limit 0), so its retry cannot succeed.

In reps 2-3 the same backlog stayed below the 15 s watchdog threshold and appeared instead as the p99 TTFT excursion. The hot-process peaks rank the reps exactly: 5,741 / 2,961 / 5,546 in-flight for p99 9,151 / 2,016 / 7,678 ms.

The worker in `mocker_2` was alive and healthy the whole time: it completed all 27,702 of its requests and served new ones inside the failure window. The watchdog killed streams whose sender had already finished generating.

## The defect: no backpressure edge in the UCX send path

The TCP transport gives each connection a `flume::bounded(256)` queue and a blocking `write_all`, so a saturated peer parks the mux batcher behind velo's instrumented admission gate. The UCX transport has no equivalent edge anywhere:

- `ucp_am_send_nbx` never refuses. A resource-exhausted endpoint yields a request pointer, not an error (`transports/ucx/worker.rs:894-959`).
- `inflight_ops` is incremented on every post and read only at teardown. Nothing gates admission on it (`worker.rs:903` vs `worker.rs:1202-1208`).
- The per-peer `AdmissionGate` queue is an unbounded `VecDeque` (`velo-ext/src/admission.rs:445`).
- All peers share one `flume::bounded(1024)` ring into the progress thread (`transports/ucx/transport.rs:116`). TCP gives 256 per connection.

Because admission never blocks, the on-admission flush policy never parks, and the batcher de-clocks: 6.97-8.65 records per batch on ucx against 18-29 on the config-identical velo0 control (both ran `FLUSH_INTERVAL_US=0`). That is 2.6-4.1x the message rate for the same record volume. Past a per-peer saturation knee somewhere between ~3k and ~5.5k concurrent streams, the backlog accumulates below velo's last observable point. Which sub-queue held it (UCX endpoint pending, the admission gate, or the worker-side credit wait) cannot be located from this run's artifacts, because no worker process exported velo metrics and the UCX inbound path is uninstrumented. Every candidate location is one velo neither bounds, sees, nor signals.

## Fix shape (all velo-side)

1. **Backpressure**: in `transports/ucx/transport.rs` + `worker.rs`, gate `admit()` on outstanding ops (`shared.inflight_ops`) or on a per-peer share of the ring, and return `SendOutcome::Pending` at a cap. The batcher then parks and the backlog moves back behind velo's bounded, instrumented gate. This is the signal TCP's bounded queue plus blocking write provides and UCX lacks.
2. **Liveness**: in `streaming/sender.rs` + `control.rs`, give heartbeats a lane that cannot sit behind data, or teach the reader-pump watchdog to distinguish starved-but-alive from dead before injecting `Dropped`.
3. **Observability**: wire the observability handle into `recv_trampoline` (`transports/ucx/worker.rs:283-414`, mirror `ingress.rs::record_inbound_frame`), and export worker-side velo metrics in the rig. The instrumentation blackout is why this run could not be diagnosed past by-elimination.

A rerun is worth doing only after (1) and (3) land. Only then is a flush-interval comparison interpretable.

## Findings refuted along the way

- Send-backpressure counters do not track the failure: rep totals (3,903 / 2,709 / 662) are anti-correlated with instability across reps 2-3.
- The `peer_byte_budget` arithmetic (8 MiB over 5,760 slots) is transport-independent: rep3-velo0 carried 5,326 streams against the same budget over TCP and held p99 at 2.5 s. The transport is the differentiator.
- Batch collapse is an amplifier, not the trigger. The clean rep ran fully collapsed batches (8.65 rec/batch) and swung nothing. Stream concentration on one peer is the discriminator.
- No UCX scheduling unfairness is demonstrated or needed. The stalled peer carried 8x its siblings' load. Unequal load fully accounts for one peer saturating first.
- The nodes have 144 physical cores, not 72. There are 9 UCX progress threads total across both nodes (one per process). CPU starvation from spinning progress threads does not bite: 8/144 cores on the worker node.

## Environment facts (for the record)

- `UCX_TLS` and `UCX_NET_DEVICES` were unset in every rep. UCX auto-selected, and the selection is unrecorded because no `UCX_LOG_LEVEL` was set. The rig echo lines prove the vars were genuinely unset.
- UCX 1.22.0, vendored and statically built by `crates/ucx-rs` (version `0.1.0+ucx.1.22.0`), confirmed against both Cargo.lock files.
- dyn-pin builds the transport with pure defaults: `spin_us=20`, `channel_capacity=1024`, no tls/net_devices override (`velo_response.rs:581-593`).
- Provenance gap: `rig_run_meta.json` records no git sha, so whether the wheel contained the 2026-09-02 streaming commits is unverifiable from the artifacts. The rig must record a sha per run.

## Evidence pointers

Raw run artifacts: `.research/results/t3-ucx1/` (per-rep logs, prometheus teardown dumps, aiperf exports). The full investigation transcripts and structured reports are in the session workflow journal (run `wf_77836dba-6e1`). Key log signature, 1,722 occurrences: `WARN velo::streaming::control: reader_pump: heartbeat watchdog fired, injecting Dropped ... anchor_frame_tx_len=0 ... transport_rx_len=0`.
