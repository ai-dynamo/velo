<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Response-plane head-to-head: measured results

Dated 2026-09-03. This document reports the measured comparison the plan (`dynamo-response-plane-competitive-plan.md`) called Tier 2 and Tier 3: velo as a response plane inside Dynamo's own serving stack, benchmarked with Dynamo's own frontend, mocker engine, and aiperf, against Dynamo's shipping per-request TCP path, the PR 11996 QUIC plane, and the PR 11918 multiplexed-TCP build. All numbers are from exclusive Slurm allocations on this cluster (aarch64 GB200 nodes, 144 cores); raw artifacts are under `.research/results/` (gitignored), one directory per run, with `EVIDENCE.md` files beside the salvaged sets.

## The headline

**Velo beats Dynamo's shipping response plane on every axis that matters at scale, and it is the only plane besides bare per-request TCP that completes a full-scale run with zero errors.** Against upstream main's per-request TCP at the published rig shape (512 workers, concurrency 8192): +13–14% throughput, TTFT p99 lower by 24–31%, zero errors on both sides. The QUIC plane (PR 11996) cannot complete a clean full-scale run on this cluster at all. The PR 11918 mux build posts higher throughput than every current-base arm, but it runs on a stack two minor versions older with no possible control arm, so the response-plane contribution cannot be isolated (details below).

## Full scale (Tier 3): 2 nodes, 512 workers, concurrency 8192

Frontend + etcd + NATS + aiperf alone on node A; 8 mocker processes × 64 packed workers on node B; ISL 1024, OSL 256, mocker speedup 5; fixed 250,000 requests per run (the published rig's methodology — a fixed set — after wall-clock cutoff mode proved to wedge aiperf); arms interleaved, fresh processes per run. Matrix `t3e` (job 2709362) + mux18 matrix `m18d` (job 2710006).

| arm | reps | req/s (mean) | TTFT p50 ms | TTFT p99 ms | ITL p99 ms | frontend CPU ms/req | errors |
|---|---|---|---|---|---|---|---|
| `tcp` — per-request (upstream main) | 3 | 2,672 | 240–1,149 | **3,345** | 24–63 | **8.23** | 0 |
| `quic` — PR 11996 | 0 clean | — | — | — | — | — | never clean (below) |
| `velo` — mux, 1 ms linger | 3 | 3,026 | ~1,060 | 2,318 | 13–19 | 10.64 | 0 |
| `velo0` — mux, write-on-admission | 2 | **3,055** | ~1,077 | 2,538 | 19–23 | 8.85 | 0 |
| `mux18` — PR 11918 build (caveat!) | 3 | 4,614 | ~546 | 2,141 | 7–11 | invalid | 0 |

Readings, in order of confidence:

- **velo vs upstream main: velo wins.** +13.3% (velo) to +14.3% (velo0) requests per second, p99 TTFT 31% (velo) / 24% (velo0) lower, both zero errors across every rep. The per-request arm's tail (p99 3.2–3.5 s) reproduces the failure mode Dynamo's own published table showed (1,954 ms p95); velo's mux keeps the tail at 2.3–2.5 s while carrying more load.
- **velo0 is the configuration to ship for this shape.** Write-on-admission beats the 1 ms linger on CPU (8.85 vs 10.64 ms/req — within 8% of bare TCP) at equal-or-better throughput. The linger's batching gain does not pay for its timer work at this token size.
- **QUIC (PR 11996) is not usable at this scale on this cluster.** Six duration-mode reps (matrices `t3c`/`t3d`) all completed with mass errors — 4,947 to 84,525 per rep (2–30% of requests), HTTP 500s plus streams that delivered no content — and in fixed-count mode the last ~8,192 in-flight requests took 18.5 minutes to resolve (55,245 errors), blowing every budget. Its low TTFT medians in the duration-mode rows are survivorship: dropped requests do not queue. A likely cluster factor: `net.core.rmem_max` is 212,992 bytes and unraisable, so QUIC's UDP sockets are clamped ~30× below what quinn wants. This deserves its own investigation before quoting it as a QUIC verdict in general.
- **mux18 (PR 11918) posts 4,614 req/s — but the number cannot be attributed to its response plane.** That build is ai-dynamo 1.3.0 (the PR head, ~40 commits behind Dynamo main and two minor versions behind the 1.5.0 stack every other arm runs); the per-request path was deleted in that PR, so no control arm exists inside its own base. Every 1.5.0 arm sits at 2.3–3.1k while the 1.3.0 build hits 4.6k — the gap plausibly includes frontend-stack differences, not just the mux. Isolating it requires porting PR 11918 onto the current base (the plan's §3 option). Its frontend-CPU column is additionally invalid (the sampler read a near-idle process on the 1.3.0 build; not diagnosed further).
- velo pays **~2.4 ms/req more frontend CPU than bare per-request TCP** (10.6 vs 8.2; velo0 narrows it to ~0.6). The adapter's per-request work is the known headroom: anchor + consumer task per request, one `spawn_blocking` per retirement, and the hello/attach path — see the "known deltas" list in `tier2-adapter-brief.md`.

## Iteration scale (Tier 2): 1 node, 16 workers, concurrency 2048

Single exclusive node, everything colocated, 30 s duration mode, 3 interleaved reps (matrix `iter2`, job 2707028). Relative comparison only.

| arm | req/s (mean) | TTFT p50 ms | ITL p50 ms | CPU ms/req | errors |
|---|---|---|---|---|---|
| tcp | 2,142 | **41** | 3.28 | 16.06 | 0 |
| quic | 2,186 | 64 | **2.75** | **13.95** | 41 / 117 / 315 |
| velo | **2,260** | 61 | 3.02 | 14.03 | 0 |
| velo0 | 2,251 | 60 | 3.06 | 14.23 | 0 |

At this small shape the per-request path has no connection storm to lose, so it wins TTFT p50; velo ties QUIC on CPU and leads throughput with the tightest spread. QUIC's error pattern already shows here.

## Method notes and disclosed interventions

- **CPU per request** is computed by us (the shipped Dynamo tooling computes none): 1 Hz `/proc/<frontend_pid>/stat` deltas windowed to the aiperf measurement interval, divided by aiperf's completed-request count.
- **aiperf stall-recovery patch.** aiperf 0.10.0 wedges indefinitely when any record references an mmap-dataset entry that fails to decode (observed at ≥250k-request scale; it then writes no export at all). We patched `records_manager.py` in both venvs to finalize after 15 no-progress report ticks once credits are complete and ≥95% of records are processed. It fired on runs missing 2–397 records of 250,000 (≤0.16%), logged as `STALL RECOVERY` in each affected run's aiperf log, identically available to every arm. Healthy runs never reach it.
- **The mocker fleet must publish uniform model cards.** Packed workers (`--num-workers`) with a `DYN_SYSTEM_PORT` set produce mixed self-hosted/fallback cards, which Dynamo's discovery controller parks in a silent `Conflict` state — no log line at any level, the fleet never serves (dyn-pin `lib/llm/src/discovery/controller.rs:391-404`). Rig fix: `DYN_SELF_HOST_METADATA=0`, no `DYN_SYSTEM_PORT` (the upstream k8s template's shape). This silent-conflict behavior is worth an upstream issue.
- The velo arms asserted mux transit (`negotiated_transport == messenger-mux-v1`) per request, and the teardown metrics dump (`velo_streaming_mux_batches_total`) confirms batched transport.
- The velo arm's 2 MiB socket-buffer request is clamped to ~208 KiB by this cluster's unraisable `net.core.{r,w}mem_max`; arithmetic and the measured throughput both say it does not bind at these rates with ≥4 worker processes.
- Cross-node addressing pitfalls fixed along the way (recorded for reuse): compute-node NSS returns link-local IPv6 first (`getent ahostsv4` + 10.x filter required); PR 11918's `DYN_TCP_RESPONSE_STREAM_HOST` interface-name path resolves the same fe80 address and fails to bind (its auto-detect works).
- The UCX arm (velo mux over RDMA on the 6× 400 Gb NDR fabric) is deferred: `rhino-dev-260831.sqsh` has no rdma-core, `ucx-rs` hard-fails without the headers, and all competitor arms are TCP anyway. It needs a new container image and a bring-up run.

## What this answers, and what is open

1. **Is velo a better path than Dynamo's current response plane?** On these measurements, yes: more throughput, better tails, zero errors, at a small CPU premium that velo0 mostly closes — plus the qualitative advantages the plan already established (negotiated with fallback, graceful drain, credit flow control, observability).
2. **Does velo beat or reach parity with what Dynamo built in the PRs?** It strictly dominates PR 11996 QUIC on this cluster. Against PR 11918 the honest answer is *unresolved*: their build is faster end-to-end, but two minor versions of stack separate the arms and the response-plane share of the gap is unknowable without porting the PR onto the current base. That port is the single highest-value follow-up measurement. *(Resolved in the 2026-09-04 addendum below: the port was measured, and the 4,614 req/s was mostly stack, not plane.)*
3. **Velo adapter CPU headroom** is the highest-value follow-up optimization: the deltas are enumerated in `tier2-adapter-brief.md`'s addendum.
4. Everything is re-runnable: `bash .research/rig/t3-submit.sh <tag>` (current-base arms; `ARMS`/`REPS`/`REQUEST_COUNT` env-overridable) and `bash .research/rig/t3-submit-m18.sh <tag>` (PR 11918 build).

## Addendum 2026-09-04 — UCX measured and diagnosed; PR 11918 ported and isolated

Two follow-up matrices completed after the 2026-09-03 report. Both run the same shape as Tier 3 above (2 nodes, 512 workers, concurrency 8192, 250,000 fixed requests, 3 reps per arm, arms interleaved).

### UCX arm (matrix `t3-ucx1`, job 2711829)

The deferred UCX arm ran after a container rebuild added rdma-core (`rhino-dev-260903.sqsh`). The arm is velo0 over velo's UCX messenger transport on InfiniBand (2 mlx5 HCAs); every other arm rides 200G Ethernet TCP. That fabric difference is the arm's thesis: does RDMA buy anything here?

| arm | req/s (mean) | TTFT p99 ms per rep | CPU ms/req | errors |
|---|---|---|---|---|
| tcp | 2,394 | 3,191 / 3,272 / 3,197 | 7.6–11.6 | 0 |
| velo0 | 2,849 | 2,124 / 2,107 / 2,531 | 8.7–9.5 | 0 |
| ucx | 2,585 | **9,152 / 2,016 / 7,678** | 9.1–12.7 | **1,722** (rep 1) |

The answer is no. The ucx arm never beat velo0 and could not hold its tail. Rep 1 returned 1,722 HTTP-500s. The root-cause diagnosis is in `ucx-arm-instability-diagnosis.md`: the UCX send path has no backpressure edge, so a router-imbalance hot spot (arm-independent; tcp and velo0 drew the same imbalance and shrugged it off) accumulates an unbounded, uninstrumented per-peer backlog, heartbeats queue behind data, and the heartbeat watchdog kills streams whose sender is alive. The ruling is **fix velo, then rerun** — no tuning knob addresses the observed mechanism. The three fixes (admission gating on in-flight ops, a heartbeat lane that cannot sit behind data, inbound instrumentation) are enumerated there.

### PR 11918 ported onto the current base (matrix `t3-m18p1`, job 2721416)

The port (`mux_response/` in dyn-pin, arm `mux18p`, `DYN_RESPONSE_PLANE=mux-tcp`) is wire-protocol byte-identical to PR 11918, drift-ledgered A1–A15 in its module docs, and passed an independent adversarial fidelity review plus 11918's own golden pipelined-handshake test. Same wheel, same 1.5.0 stack, same sampler as every other arm — the CPU column is valid this time. Mux transit is confirmed per rep: 32 accepted `dynamo_tcp_response_mux` connections (8 processes × the pool of 4), ~250k stream setups, and zero mux-tcp series in the velo0 reps.

| arm | req/s (mean) | req/s range | TTFT p50 ms | TTFT p95 ms | TTFT p99 ms | ITL p99 ms | CPU ms/req | errors |
|---|---|---|---|---|---|---|---|---|
| tcp | 2,511 | 2,065–2,784 | 106–1,342 | 1,713 | 3,470 | 30–114 | 9.07 | 0 |
| velo0 | **3,019** | 2,919–3,084 | ~1,100 | 2,050 | 2,742 | **18–28** | 9.19 | 0 |
| mux18p | 2,746 | 2,360–3,061 | **86–168** | **548** | **1,746** | 26–99 | **6.51** | 0 |

Readings:

- **The 4,614 req/s was mostly stack, not plane.** On the common base the ported plane posts 2,746 req/s — velo0 leads it by 9.9% on throughput (3,019 vs 2,746) and by a wide margin on stability (velo0's rep spread is 165 req/s; mux18p's is 701).
- **mux18p wins first-token latency and frontend CPU decisively at this shape.** TTFT p50 86–168 ms vs velo0's ~1,100 ms, p95 548 vs 2,050 ms, p99 1,746 vs 2,742 ms, and 6.51 vs 9.19 CPU ms/req. Its ITL p99 is wider (26–99 vs velo0's 18–28), so the trade is first-token latency against stream smoothness and throughput. *(Diagnosed 2026-09-04: the TTFT gap is a standing backlog in velo's fixed-parallelism frontend ingest plus a per-request attach round trip — not flush policy. See `ttft-gap-diagnosis.md`; the fix plan is `velo-response-plane-win-plan.md`.)*
- **Both planes are clean.** Zero errors, all reps, both mux planes. The competitive picture is now two viable mux designs with different trade-offs, not one winner on every axis: velo0 for throughput, throughput stability, ITL tails, and the qualitative surface (negotiated fallback, drain, credit flow control, observability); mux18p's numbers say the first-token path and per-request frontend cost deserve a targeted look in the velo adapter (`tier2-adapter-brief.md` already enumerates the known deltas).
- The tcp arm reproduced its structural 3.4–3.5 s p99 tail in every rep, at every throughput draw.

## Addendum 2026-09-05: the pinned baseline replaces every table above

Every number above was measured with aiperf sharing the frontend node and starving it (`ttft-gap-diagnosis.md`, late-night addendum of 2026-09-04). The rig now pins the frontend to cpus 0-71 and aiperf to 72-143 by default (`RIG_PIN_CORES`, recorded as `pin_cores` in `rig_run_meta.json`). Matrix `t3-base-pin` (job 2725241, three reps per arm, 512 workers, concurrency 8192, 250,000 requests per rep, velo `0160fa1`, dyn-pin `3a67ae2e6e` with rig-local mods) is the scoreboard from here on.

| arm | rep | req/s | TTFT p50 ms | TTFT p95 ms | TTFT p99 ms | ITL p50 ms | ITL p99 ms | frontend CPU ms/req | E2E p50 s | E2E p90 s | E2E p99 s | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| tcp | 1 | 3,339 | 65 | 183 | 1,664 | 6.8 | 38.8 | 4.92 | 1.86 | 4.42 | 10.26 | 0 |
| tcp | 2 | 3,296 | 54 | 139 | 1,706 | 1.7 | 51.4 | 3.83 | 0.47 | 8.87 | 13.54 | 0 |
| tcp | 3 | 3,282 | 54 | 123 | 1,758 | 1.6 | 47.6 | 3.79 | 0.47 | 9.26 | 12.53 | 0 |
| velo0 | 1 | 3,060 | 57 | 192 | 850 | 1.7 | 71.5 | 4.04 | 0.49 | 13.71 | 18.78 | 0 |
| velo0 | 2 | 3,244 | 71 | 179 | 814 | 1.6 | 56.3 | 3.43 | 0.48 | 10.02 | 14.84 | 0 |
| velo0 | 3 | 3,327 | 85 | 180 | 834 | 1.6 | 36.0 | 3.78 | 0.48 | 8.44 | 9.51 | 0 |
| mux18p | 1 | 3,260 | 47 | 143 | 743 | 1.7 | 64.3 | 3.05 | 0.48 | 12.94 | 16.89 | 0 |
| mux18p | 2 | 3,318 | 47 | 109 | 765 | 1.5 | 43.6 | 3.00 | 0.42 | 8.98 | 11.47 | 0 |
| mux18p | 3 | 3,538 | 47 | 155 | 751 | 1.8 | 35.9 | 2.23 | 0.50 | 7.61 | 9.46 | 0 |

Three-rep means: tcp 3,306 req/s (spread 57), TTFT p50 58, p99 1,709, CPU 4.18; velo0 3,210 (266), 71, 833, 3.75; mux18p 3,372 (278), 47, 753, 2.76.

What this changes. The shipping tcp plane is not broken at p50 once the frontend is fed; its accept loop still costs a 1.7 s p99. velo0 and mux18p are within rep spread on throughput; mux18p leads first token by 24 ms at p50 and 80 ms at p99 and uses 26 percent less frontend CPU per request. The end-to-end tail is noisy and a tie at this split (both planes 9.5 to 18.8 s p99 across reps); at a 48/96 split velo0 holds 8.1 s while mux18p returns to 25 s (`t3-pin2`), so velo's tail discipline shows only when the frontend is CPU-constrained.

Where velo0's remaining first-token time is, from the W0 and W0b instruments on `t3-base-pin` rep 2 (means over the steady window): C 74 ms, worker handler start to first response 20 ms of which the attach round trip is 13 ms, ordered lanes 6 ms, inbound queue and egress queues under 2 ms each; the remainder is the delivery path from the anchor to the SSE writer and the client. W3 (PR #78, zero-RTT setup) and W4a (detached OpenSlot ack) remove the pre-generation waits; W2 targets the CPU gap.

The results page and the plan's scoreboard are updated to this table. `t3-m18p1`, `t3-ucx1`, and every earlier matrix remain on disk as pre-pinning history.

## Addendum 2026-09-05: isolation matrix `t3-iso1` (W3 and W4a against velo0 and mux18p)

Matrix `t3-iso1` (job 2729436; three reps per arm; 512 workers, concurrency 8192, 250,000 requests per rep; pinned 72/72; velo `379240a`, which is `drain-credit-return` plus W0, W3 and W4a, with rig-local modifications; dyn-pin `3a67ae2e6e` with rig-local mods; gate `DYN_VELO_RESPONSE_ZERO_RTT_ATTACH` on for velo3 and velo34, `DYN_VELO_RESPONSE_ASYNC_OPEN_ACK` on for velo4a and velo34, both asserted per rep from the resolved-config lines and the attach counters).

| arm | rep | req/s | TTFT p50 ms | TTFT p95 ms | TTFT p99 ms | ITL p50 ms | ITL p99 ms | frontend CPU ms/req | E2E p50 s | E2E p90 s | E2E p99 s | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| velo0 | 1 | 3,055 | 86 | 179 | 707 | 1.8 | 49.4 | 4.86 | 0.56 | 7.30 | 13.04 | 0 |
| velo0 | 2 | 3,302 | 95 | 199 | 823 | 1.6 | 34.6 | 3.09 | 0.50 | 8.28 | 9.15 | 0 |
| velo0 | 3 | 3,115 | 74 | 185 | 842 | 1.5 | 51.5 | 3.84 | 0.46 | 9.89 | 13.60 | 0 |
| velo3 | 1 | 3,263 | 65 | 177 | 869 | 1.6 | 43.9 | 3.34 | 0.48 | 8.76 | 11.59 | 0 |
| velo3 | 2 | 3,517 | 72 | 206 | 850 | 1.7 | 33.7 | 2.42 | 0.51 | 7.64 | 8.91 | 0 |
| velo3 | 3 | 3,128 | 69 | 177 | 832 | 1.6 | 56.0 | 3.69 | 0.47 | 7.94 | 14.77 | 0 |
| velo4a | 1 | 3,275 | 80 | 217 | 819 | 1.7 | 56.1 | 2.93 | 0.51 | 8.98 | 14.79 | 16 |
| velo4a | 2 | 3,502 | 98 | 235 | 828 | 1.7 | 31.6 | 2.22 | 0.55 | 7.50 | 8.37 | 0 |
| velo4a | 3 | 3,447 | 95 | 227 | 813 | 1.7 | 32.9 | 2.45 | 0.53 | 7.62 | 8.72 | 0 |
| velo34 | 1 | 3,386 | 68 | 189 | 868 | 1.6 | 34.7 | 2.79 | 0.48 | 7.98 | 9.16 | 0 |
| velo34 | 2 | 3,271 | 63 | 198 | 817 | 1.7 | 63.4 | 3.16 | 0.50 | 9.13 | 16.73 | 95 |
| velo34 | 3 | 3,281 | 46 | 202 | 822 | 1.7 | 81.6 | 3.09 | 0.50 | 9.17 | 21.46 | 186 |
| mux18p | 1 | 3,373 | 51 | 197 | 772 | 1.8 | 43.3 | 2.67 | 0.52 | 6.34 | 11.41 | 0 |
| mux18p | 2 | 3,320 | 47 | 178 | 756 | 1.7 | 49.5 | 2.75 | 0.49 | 8.87 | 13.04 | 0 |
| mux18p | 3 | 3,222 | 47 | 165 | 764 | 1.7 | 42.9 | 3.10 | 0.48 | 8.89 | 11.31 | 0 |

Three-rep means: velo0 3,157 req/s (spread 247), TTFT p50 85, p99 791, CPU 3.93, E2E p99 11.9 s; velo3 3,303 (389), 69, 851, 3.15, 11.8; velo4a 3,408 (227), 91, 820, 2.53, 10.6, 16 errors; velo34 3,313 (114), 59, 835, 3.01, 15.8, 281 errors; mux18p 3,305 (152), 48, 764, 2.84, 11.9.

Reading. W3 (zero-RTT setup) is worth about 16 ms at p50 with zero errors. W4a on its own is within noise; with W3 it reaches 59 ms mean and 46 ms in its best rep, but those reps carry a defect: every error is a stream whose `OpenSlot` admission answer was refused at the control inbox's cap, which left the slot fenced until the frontend's heartbeat watchdog gave up 15 s later (`ttft-gap-diagnosis.md`, afternoon addendum, section 1; fixed on PR #79; velo4a and velo34 rerun). The E2E and ITL p99 columns measure one mocker process's backlog in every arm, tcp and mux18p included (section 2 of the same addendum), and are no longer a criterion. The results page carries this table beside the pinned baseline.

## Addendum 2026-09-05 (night): every frontend CPU number above is corrected, and matrix `t3-iso2`

**The correction.** `summarize.py` computed frontend CPU per request inside aiperf's own measurement window, read from aiperf's `start_time` and `end_time`. Those strings carry no offset and are the machine's local clock (aiperf 0.10.0); the script read them as UTC. The window therefore landed seven hours past the capture, no sample fell inside it, and the script fell through to its second strategy, "the last `benchmark_duration` seconds of the capture", which on this rig is mostly the idle tail after aiperf has finished. Every frontend CPU per request number in this document, the plan, the results page and the diagnosis before this addendum came from that tail and understates the true figure about three-fold. The script now reads aiperf's naive timestamps in the capture's own offset (unit tests `dbg/test_summarize_cpu_window.py`), and the numbers below are recomputed from the same samples inside the real window. Throughput, first-token, ITL and end-to-end numbers were never affected; they come from aiperf.

Corrected frontend CPU per request, milliseconds, per rep and three-rep mean:

| matrix | arm | rep 1 | rep 2 | rep 3 | mean | recorded before |
|---|---|---|---|---|---|---|
| t3-base-pin | tcp | 11.12 | 10.30 | 10.01 | 10.48 | 4.18 |
| t3-base-pin | velo0 | 7.93 | 8.11 | 8.22 | 8.09 | 3.75 |
| t3-base-pin | mux18p | 7.69 | 7.63 | 7.52 | 7.61 | 2.76 |
| t3-iso1 | velo0 | 8.45 | 8.00 | 8.01 | 8.15 | 3.93 |
| t3-iso1 | velo3 | 8.70 | 8.64 | 8.87 | 8.74 | 3.15 |
| t3-iso1 | velo4a | 7.46 | 7.74 | 7.78 | 7.66 | 2.53 |
| t3-iso1 | velo34 | 8.73 | 8.54 | 8.09 | 8.45 | 3.01 |
| t3-iso1 | mux18p | 7.79 | 7.24 | 7.25 | 7.43 | 2.84 |

What changes in the reading: the frontend spends 25 to 30 cores on every plane at this load, not 10; mux18p's advantage over velo0 is 0.5 ms per request (6 percent), not 1.0 (26 percent); tcp costs 2.9 ms more than velo0, not 0.4; zero-RTT setup costs 0.6 ms per request over velo0 on the frontend (velo3 8.74 against 8.15), which is the request-path cost section 3 of the diagnosis addendum measures in time, and the detached ack alone saves 0.5 (velo4a 7.66). The CPU term of the success bar is restated in the plan as "at or below mux18p's on the same matrix". Earlier matrices (`t3-m18p1`, `t3-ucx1`, `t3-pin1`, `t3-pin2`, `t3-rt32`) are not recomputed; their CPU columns are wrong in the same way and their other columns stand.

**Matrix `t3-iso2`** (job 2730444; nodes ptyche0092 and ptyche0093; velo `f58de76`, the integration branch with the control-cap fix and PR #80's counters; three reps of velo3, velo4a, velo34; pinned 72/72; 250,000 requests per rep).

| arm | rep | req/s | TTFT p50 ms | TTFT p95 ms | TTFT p99 ms | ITL p50 ms | ITL p99 ms | frontend CPU ms/req | E2E p50 s | E2E p90 s | E2E p99 s | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| velo3 | 1 | 2,918 | 87 | 142 | 831 | 10.2 | 33.3 | 9.93 | 2.79 | 4.55 | 8.83 | 0 |
| velo3 | 2 | 2,824 | 60 | 119 | 161 | 1.4 | 46.3 | not captured | 0.42 | 10.26 | 12.18 | 0 |
| velo3 | 3 | 2,852 | 61 | 136 | 855 | 1.4 | 41.4 | 9.80 | 0.41 | 10.23 | 10.88 | 0 |
| velo4a | 1 | 3,029 | 111 | 209 | 841 | 1.5 | 29.8 | 8.46 | 0.49 | 6.54 | 7.91 | 0 |
| velo4a | 2 | 2,816 | 69 | 176 | 816 | 1.5 | 50.9 | 8.30 | 0.45 | 10.70 | 13.42 | 0 |
| velo4a | 3 | 2,271 | 56 | 252 | 820 | 1.7 | 105.1 | 8.90 | 0.49 | 19.61 | 27.62 | 0 |
| velo34 | 1 | 2,367 | 55 | 225 | 837 | 1.7 | 101.4 | 10.29 | 0.49 | 17.89 | 26.64 | 0 |
| velo34 | 2 | 2,763 | 61 | 140 | 836 | 1.4 | 46.8 | 9.91 | 0.41 | 10.58 | 12.36 | 0 |
| velo34 | 3 | 2,784 | 62 | 150 | 909 | 1.4 | 54.6 | 9.56 | 0.41 | 10.94 | 14.38 | 0 |

What it settles: zero errors in nine reps, zero heartbeat watchdog firings, and on three reps a worker refused control entries at the cap (24,865 on velo34 rep 1) with no slot left fenced and nothing withheld at the end, which is the control-cap fix doing exactly what its tests say. What it does not settle: these nodes ran every arm slower and hotter than `t3-iso1`'s (throughput 2,600 to 2,900 against 3,300; CPU 8.5 to 10 against 7.7 to 8.7), the mocker backlog fell across four to six processes in several reps (velo3 rep 1 has ITL p50 10 ms and E2E p50 2.8 s, which is the mocker, not the plane), and velo3 rep 2 is aiperf's second pass after its first hit a dataset decode error inside aiperf itself, so its capture missed the measured window. The per-request join on the clean reps (`out-iso2`) puts velo3 and velo34 at A 5 to 6 ms, B 5 to 6 ms, C 50 ms, against velo4a at A 1.8, B 1.5, C 64: the request-path cost of zero-RTT is half what `t3-iso1` measured on its nodes and the response leg is the same, which says the request-path cost is contention, not a fixed price. The first-token comparison against the bar waits for `t3-iso3` with the reply linger (PR #81) and a same-matrix mux18p.

## Addendum 2026-09-06: matrix `t3-iso3`, the reply linger measured

Matrix `t3-iso3` (job 2731049; nodes ptyche0196 and ptyche0197; velo `4afb407`, the integration branch with the control-cap fix, PR #80's counters and PR #81's reply linger; three reps each of velo3 (reply linger 1 ms, the new default), velo3n (velo3 with `DYN_VELO_RESPONSE_REPLY_LINGER_US=0`, the pre-linger behaviour), velo3f (velo3 with `DYN_VELO_RESPONSE_FLUSH_INTERVAL_US=500`, a data linger on the workers), velo34 and mux18p; pinned 72/72; 250,000 requests per rep; CPU per request from the corrected summary).

| arm | rep | req/s | TTFT p50 ms | TTFT p95 ms | TTFT p99 ms | ITL p50 ms | ITL p99 ms | frontend CPU ms/req | E2E p50 s | E2E p90 s | E2E p99 s | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| velo3 | 1 | 3,017 | 83 | 142 | 803 | 10.8 | 23.5 | 9.87 | 3.04 | 5.28 | 6.24 | 0 |
| velo3 | 2 | 2,254 | 54 | 231 | 871 | 1.8 | 106.7 | 9.91 | 0.51 | 19.37 | 28.03 | 0 |
| velo3 | 3 | 2,825 | 62 | 149 | 896 | 1.4 | 55.8 | 9.52 | 0.42 | 10.88 | 14.70 | 0 |
| velo3n | 1 | 2,353 | 55 | 225 | 864 | 1.7 | 101.5 | 10.15 | 0.48 | 18.78 | 26.63 | 0 |
| velo3n | 2 | 2,326 | 55 | 222 | 835 | 1.7 | 101.7 | 10.01 | 0.50 | 18.74 | 26.65 | 0 |
| velo3n | 3 | 2,314 | 55 | 231 | 868 | 1.7 | 102.3 | 10.29 | 0.51 | 18.99 | 26.86 | 0 |
| velo3f | 1 | 2,790 | 62 | 136 | 834 | 1.5 | 46.1 | 9.05 | 0.44 | 10.37 | 12.15 | 0 |
| velo3f | 2 | 3,077 | 75 | 132 | 805 | 1.4 | 27.0 | 9.15 | 0.43 | 6.57 | 7.13 | 0 |
| velo3f | 3 | 3,106 | 76 | 139 | 854 | 1.4 | 27.5 | 9.10 | 0.43 | 6.50 | 7.30 | 0 |
| velo34 | 1 | 2,815 | 61 | 138 | 842 | 1.5 | 44.9 | 9.72 | 0.43 | 10.34 | 11.83 | 0 |
| velo34 | 2 | 3,000 | 73 | 138 | 837 | 1.4 | 29.2 | 9.79 | 0.42 | 6.85 | 7.72 | 0 |
| velo34 | 3 | 2,352 | 54 | 229 | 847 | 1.6 | 101.6 | 9.88 | 0.46 | 18.71 | 26.70 | 0 |
| mux18p | 1 | 2,379 | 49 | 203 | 786 | 1.7 | 99.3 | 7.33 | 0.48 | 18.70 | 26.07 | 0 |
| mux18p | 2 | 2,928 | 48 | 115 | 788 | 1.4 | 51.2 | 7.62 | 0.41 | 10.18 | 13.46 | 0 |
| mux18p | 3 | 2,952 | 49 | 108 | 793 | 1.4 | 42.5 | 7.50 | 0.40 | 9.76 | 11.16 | 0 |

Three-rep means: velo3 2,699 req/s (spread 763), TTFT p50 66, p99 857, CPU 9.76; velo3n 2,331 (39), 55, 855, 10.15; velo3f 2,991 (316), 71, 831, 9.10; velo34 2,722 (648), 63, 842, 9.80; mux18p 2,753 (573), 49, 789, 7.48. Zero errors on all fifteen reps; zero heartbeat watchdog firings.

**Two load modes, set by the mocker.** Every rep of this matrix fell into one of two states that have nothing to do with the arm: a "bad" draw in which one mocker process holds about 7,300 of the 8,192 in-flight requests (velo3n's three reps, velo3 rep 2, velo34 rep 3, mux18p rep 1: about 2,330 req/s, ITL p99 about 100 ms, E2E p99 about 27 s) and a "good" draw in which the backlog is split over two or three processes (about 2,800 to 3,100 req/s, E2E p99 7 to 15 s). Throughput, ITL and E2E columns are the draw. TTFT p50 rises with the draw's throughput on the velo arms (54 to 55 ms at 2,300 req/s, 61 to 62 at 2,800, 73 to 76 at 3,000 to 3,100) and does not on mux18p (48 to 49 at both). Only same-mode pairs compare. The nodes also run every arm below `t3-iso1`'s (mux18p 2,753 against 3,305 there; CPU 7.48 against 7.43 is the one column that agrees).

**What the reply linger did.** From PR #80's counters on the frontend: with the linger off (velo3n) the frontend sent 1.58 to 1.69 million batches per rep, one per control wake, all credit replies; with it on (velo3, velo34) it sent 275,000 to 372,000, a five-fold cut, with 62,000 to 89,000 linger wakes carrying them. The per-request join (`out-iso3`) on the same-mode pair velo3 rep 2 against velo3n reps 1 and 2 shows no change in any segment: A 3.7 against 3.9 to 4.0 ms, B 3.3 against 3.3 to 3.5, C 47.7 against 47.8 to 47.9. CPU per request fell 0.4 ms (9.76 against 10.15). The linger removes the batch inflation and nothing else; the inflation was a symptom, not the cost.

**What the data linger did.** velo3f cuts the frontend's inbound batches from 7.5 to 8.6 million per rep to 1.0 million (the workers' data batches carry seven times more records) and CPU to 9.10 ms per request, the lowest velo figure, but its request path pays for it: A 8.4 to 9.1 ms and B 12 to 13 ms against velo3's 5.6 and 5.5 in the same mode, and its TTFT p50 is 72 to 76 ms. Bigger inbound batches hold the peer's ingress lock and the ordered lane longer per batch, and the requests waiting behind them pay. Not a ship setting at 500 us.

**Where velo stands against mux18p on these nodes, same mode.** Segment medians: mux18p A 1.4 to 1.6, B 0.5 to 0.7, C 45.6 to 45.9 ms, frontend-internal first-token time 12.3; velo3 A 3.7 to 5.6, B 3.3 to 5.5, C 47.7 to 50.4, frontend-internal 18 to 24. The response leg is within 2 to 4 ms of mux18p; the request leg costs 5 to 9 ms more; the frontend's own first-token time is 6 to 11 ms longer; frontend CPU is 2.3 ms per request (31 percent) higher, against 6 percent for velo0 on the pinned baseline. The residual is per-request frontend work under load, not batch counts.
