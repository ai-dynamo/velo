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
2. **Does velo beat or reach parity with what Dynamo built in the PRs?** It strictly dominates PR 11996 QUIC on this cluster. Against PR 11918 the honest answer is *unresolved*: their build is faster end-to-end, but two minor versions of stack separate the arms and the response-plane share of the gap is unknowable without porting the PR onto the current base. That port is the single highest-value follow-up measurement.
3. **Velo adapter CPU headroom** is the highest-value follow-up optimization: the deltas are enumerated in `tier2-adapter-brief.md`'s addendum.
4. Everything is re-runnable: `bash .research/rig/t3-submit.sh <tag>` (current-base arms; `ARMS`/`REPS`/`REQUEST_COUNT` env-overridable) and `bash .research/rig/t3-submit-m18.sh <tag>` (PR 11918 build).
