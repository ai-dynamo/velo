# Velo response plane: summary as of 2026-09-10

This document is the entry point. It carries the current result, what changed to get there, what the harness is, and what goes upstream. The dated record of every measurement is in `response-plane-benchmark-results.md`. The mechanisms are in `ttft-gap-diagnosis.md`. The plan and its rulings are in `velo-response-plane-win-plan.md`.

## Where it stands

Tree 94dc8eb (the PR stack through #83 and #84), the dynamo adapter with one tokio runtime and an inline receiver, a 72-worker frontend. Matrices `t3-final72` and `t3-now2e72`, three reps per arm. Arms are compared at a matched backlog draw (a holder is a mocker process with a share of the 8,192-way backlog). Steady state is the requests that started 10 s or later into the run.

| matched draw | arm | req/s | TTFT p50 ms | steady p90 | steady p99 | CPU ms/req |
|---|---|---|---|---|---|---|
| 1 holder | velo3 | 2,440 | 39.5 | 132 | 216 | 11.9 |
| 1 holder | mux18p | 2,274 / 2,323 | 46.4 / 46.2 | 137 / 134 | 226 / 217 | 10.1 / 10.0 |
| 2 holders | velo3 | 2,824 / 2,818 | 39.3 / 43.5 | 83 / 84 | 116 / 111 | 13.2 / 13.2 |
| 2 holders | mux18p | 2,835 | 45.7 | 80 | 107 | 10.3 |
| 3 holders | velo3 | 3,447 / 3,133 | 45.4 / 44.5 | 87 / 96 | 181 / 171 | 13.0 / 13.0 |
| 3 holders | mux18p | 3,211 | 48.3 | 103 | 229 | 11.4 |
| 5 to 6 holders | velo3 | 3,142 | 55.5 | 101 | 265 | 13.4 |
| 5 to 6 holders | mux18p | 3,330 | 55.1 | 116 | 308 | 12.2 |

Zero errors in eighteen reps of both arms.

1. **Velo meets the bar.** At a matched draw in steady state, first-token p50 is ahead by 2 to 7 ms, p90 and p99 are level or ahead, throughput is equal, errors are zero. Frontend CPU per request is 1.6 to 2.9 ms above mux18p's and is recorded, not required.
2. **The biggest lever was outside velo.** The dynamo frontend ran two 72-worker tokio runtimes on 72 cores. Every response record crossed between them through one contended mutex. The fix is 24 lines in dynamo's Python bindings and applies to every response plane.
3. **Three velo changes carried the first token**: zero-RTT stream setup (#78), credit returned on the next batch for every drained slot (#83), and one pinned timer per stream (#84). Two more are fixes those changes needed: the reply linger (#81) and the control-map bound (#82).
4. **What is not a lever**: grant coalescing (#85, closed: no CPU change, a worse tail), the credit tail (about 300 stalls per rep, under 2 ms each), a 32-worker frontend (costs velo about 130 ms of p95), and SSE flush coalescing (hyper flushes once per poll for both planes).
5. **The reported p99 was the run's opening burst.** Every request at or above the raw p99 (760 to 930 ms in both arms) started within 0.2 s of the profiling phase, when aiperf issues all 8,192 credits at once. The rig's summary now reports steady-state percentiles next to the raw ones.
6. **Velo's CPU surplus is task hops**: reader pump 1.0, anchor 0.85, ingress 0.5, adapter 0.5, transport and dispatch 0.3 ms per request, 3.2 together, against mux18p's reader and receiver at 2.0. Taking them is optional. Each needs a same-matrix tail check.

## What changed in velo

One draft PR, #86 (`response-plane` against `main`): `main` at a15f52d merged with the integration branch every rig matrix since 2026-09-06 ran on. The eight stacked draft PRs (#77 to #84) and the base branch `drain-credit-return` are folded into it and closed. #85 (one grant per half window) was measured and rejected and is not in it. Every commit is signed off. Tests landed with the code, each with a fail-before run (`.research/rig/failbefore-*.sh`).

| piece (former PR) | change | measured |
|---|---|---|
| drain-driven credit return (base branch) | Credit returns when the consumer drains, with a per-peer visit floor (breaking). Two fixes: the drain-visit heap bounded to one entry per peer, and no runtime worker blocks on a terminal sentinel. | The base every arm ran on. |
| instruments (#77, #80) | The inbound queue, the attach round trip, the egress writers, and the batcher's sent records by type and wakes by source. | Located the backlog draw, the lane wait and the reply that multiplied batches. |
| zero-RTT stream setup (#78) | The worker sends without waiting for the attach round trip; cancel is restored in band. | Worker-to-client segment from 78 to 48 ms at p50. |
| detached open ack (#79) | A stream open is acknowledged without waiting for its OpenSlot admission. | About 9 ms more on that segment with zero-RTT. |
| reply linger (#81) | Credit replies form a batch for 1 ms instead of each writing one. | 4 to 6 times fewer frontend batches, 0.4 ms/req. |
| control-map bound (#82) | The control maps are bounded by what the batcher allocated, not by a size cap. | Removed the HTTP 500s at the control cap. |
| credit on the next batch (#83) | Credit returns on the next batch for every slot that drained, named by the pump. The doorbell and the sweep are backstops. | With #84 and one runtime: p50 ahead of mux18p. |
| one timer per stream (#84) | One pinned timer per stream in the reader pump, re-armed from the receive path. | The pump's timer subtree from 7.3 to 0.15 percent of the frontend's samples. |

Versions: `velo` 0.13.0 (breaking on 0.12.0), `velo-ext` 0.5.1 with the pin `=0.5.1`; the semver gate passed against `origin/main`. Gate on the merge commit: fmt and clippy clean, 1,472 tests passed, 5 failed. The five are `main`'s own UCX idle-endpoint reaper tests (#69), which time out at 72 test threads and pass at 8 and serially; a run of `main` alone at the same parallelism settles whether they are `main`'s. The docs are PR #76.

## What changed in the harness

Everything here is rig-local: the dynamo checkout `.research/dyn-pin` (uncommitted), the rig under `.research/rig` and `.research/analysis` (gitignored), the container image, and two venvs.

### The dynamo adapter (`.research/dyn-pin`, base 3a67ae2e6e on dynamo's response-plane branch)

| piece | where | what it is |
|---|---|---|
| velo response plane | `lib/runtime/src/pipeline/network/velo_response.rs` (new, 3,501 lines, 19 tests) | One `velo::Velo` per process. A `StreamAnchor` per request on the frontend, a `StreamSender` on the worker. The messenger mux batches every stream to the same peer. Prebind for zero-RTT, the async open ack and the reply linger are threaded into `MuxConfig`. The inline receiver `VeloStreamReceiver` is polled by the connection task. The consumer task and its 64-deep mailbox are gone. |
| plane selection and wiring | `network.rs`, `egress/addressed_router.rs`, `ingress/push_handler.rs`, `distributed.rs`, `lib/runtime/Cargo.toml` | `ResponsePlaneMode::Velo`, a generic `decode_response_stream` over any `Stream<Item = Bytes>`, the `RecvProvider` and `RecvRegistration` arms, the velo `ResponsePublisher`, the process-wide server and client pool on `DistributedRuntime`. |
| knobs | `config/environment_names.rs` | `DYN_RESPONSE_PLANE=velo`. `DYN_VELO_RESPONSE_TRANSPORT` (tcp or ucx), `STREAM_HOST`, `STREAM_PORT`, `INITIAL_CREDIT` (256), `FLUSH_INTERVAL_US` (1000, the worker data linger), `REPLY_LINGER_US` (1000), `ZERO_RTT_ATTACH` (0), `ASYNC_OPEN_ACK` (0), `BUFFER_CAPACITY` (documented, no longer read). |
| one tokio runtime | `lib/bindings/python/rust/lib.rs` (+24/-17) | The pyo3 async bridge is initialised with dynamo's runtime behind `Worker::has_existing_runtime()`. Before, the bridge lazily built a second 72-worker runtime. |
| work-handler histograms | `metrics/work_handler_perf.rs` (+92/-18) | Registered per registry instead of once per process, so every mocker process exports `dynamo_work_handler_time_to_first_response_seconds`. The draw classifier reads it. |
| python surface | `runtime_args.py`, `frontend_args.py`, `mocker/args.py`, `utils/runtime.py`, three tests | `--response-plane` accepts `velo` and `mux-tcp`. Two more choice lists (`sample_engine.py`, `sample_diffusion_engine.py`) and the `Worker.run()` validator were not widened. |
| mux18p comparison arm | `pipeline/network/mux_response/` (new, 3,447 lines), `metrics/mux_response.rs`, `egress/tcp_client.rs` | A port of dynamo PR 11918 onto this base, so its numbers attribute to its plane rather than to the stack it was measured on. Not this campaign's product. |
| dependency | `Cargo.toml`, both lockfiles, two `kvbm` import paths | `velo = { path = "../../lib/velo", features = ["ucx"] }` against this repo's tree (0.13.0, unpublished). The kvbm imports move from `velo::backend` to `velo::transports`. |

### The rig (`.research/rig`)

- **Runs.** `t3-submit.sh TAG` submits a two-node job. `t3-matrix.sh` runs `ARMS` for `REPS`. `t3-frontend.sh` and `t3-workers.sh` bring up the frontend, eight mocker processes of 64 workers, etcd and nats, and aiperf at concurrency 8,192 for 250,000 requests after 8,192 warm-up requests. Arms: `tcp`, `quic`, `velo0` to `velo34`, `ucx`, `mux18p`. `smoke-all.sh` is the 256-request smoke.
- **Pinning.** `RIG_PIN_CORES=1` (default) puts the frontend and aiperf on disjoint cores. `RIG_AUX_PIN` moves etcd and nats onto aiperf's cores. `RIG_FRONTEND_WORKER_THREADS` sizes the frontend runtime (72 measured best). `RIG_PERF=1` with `RIG_PERF_BIN` and `RIG_EXTRA_MOUNTS` records a system-wide `perf` profile per rep.
- **Build and gates.** `build-wheel.sh` builds dyn-pin's bindings with `maturin develop`. That is an editable install: both venvs load one `_core.abi3.so`, so `RIG_VENV` selects the pure-Python side only, and A/B across trees is sequential. `check-tree-velo.sh` is the fmt, clippy and test gate for any velo worktree. `gate-then-wheel.sh` gates, then builds. `check-w0-adapter.sh` runs the adapter's suite. `failbefore-*.sh` record fail-before evidence for each PR.
- **Summary.** `summarize.py` writes one JSON line per rep: throughput, TTFT p50/p95/p99, steady-state TTFT p50/p90/p99 and count, ITL p50/p99, request and error counts, and frontend CPU per request over aiperf's window. aiperf's timestamps are naive local time. The earlier UTC reading understated CPU three-fold.
- **Analysis (`.research/analysis`).** `draw/draw.py` classifies the backlog draw per rep. `ttft-join/` joins aiperf, the frontend log and the mocker logs per request and splits TTFT into A, B and C (`extract.py`, `a2_join.py`, `a9_tail.py` for the tail). `itl/analyze_itl.py` counts long inter-token gaps per rep and per token position. The CPU partition of a `perf` profile by bucket is `part6.py` in the session scratchpad.
- **Image and venvs.** `rhino-dev-260903.sqsh` adds rdma-core headers so the wheel compiles velo's `ucx` feature. `aiperf-venv` and `aiperf-venv-b` hold aiperf 0.10.0 and the dynamo packages.

## What goes upstream

In the order that unblocks the most. Two dynamo fixes stand alone. The velo adapter waits on a velo release. The mux18p port is not ours.

| item | target | state | what blocks it |
|---|---|---|---|
| One tokio runtime for the pyo3 bridge | dynamo, `lib/bindings/python/rust/lib.rs` | Ready. Independent of any plane. The biggest single lever of the campaign. | A test that the process ends with one runtime (thread count or runtime identity). The regression is otherwise silent. |
| Work-handler histograms per registry | dynamo, `metrics/work_handler_perf.rs` | Ready with its test. Independent. | State the trade in the PR: K endpoints per runtime register the same histogram K times, de-duplicated with a warning on scrape. |
| `DYN_RESPONSE_PLANE` is latched on first read | dynamo, `environment_names.rs` | One doc hunk, true today. | Nothing. |
| The velo response plane | dynamo, `velo_response.rs` plus wiring, knobs and the python choice lists | Built and tested (681 runtime tests pass). One reviewable concern. | velo 0.13.0 and ucx-rs must be published (the adapter uses `prebind_anchor`, `StreamOpenTicket`, `MuxConfig::async_open_ack` and `reply_linger`). The `velo` dependency must become an optional cargo feature (the `ucx` feature drags in the vendored UCX build). Graceful shutdown no longer drains in-flight bodies (a `TaskTrackerToken` on the receiver restores it). `DYN_VELO_RESPONSE_BUFFER_CAPACITY` is documented and listed in the completeness test but unread. The knob docs cite rig paths. Two python choice lists and the `Worker.run()` validator still reject `velo`. The `distributed.rs` hold assumes a multi-thread primary. Drop the `lib.rs` smoke test and the `tolerate_finish_error_on_cancel` hook (only the mux port implements it). |
| axum's graceful-shutdown watch | dynamo, an issue with the profiles | 1.4 to 2.2 ms per request in both planes: one process-wide lock re-polled on every connection wake. | Not our code. The profile is the evidence. |
| mux18p plane | dynamo PR 11918 | A port of another author's PR with a drift ledger. | Hand the ledger back. Do not upstream from here. |
| velo: the base branch | velo `main` | `drain-credit-return`: four commits including a breaking change and the deadlock fix. | Its own PR first. `main` is at 0.12.0, the base at 0.11.0. |
| velo: the stack #77 to #84 | velo `main` | Eight draft PRs, each gated on a compute node, none tagged for human review. | A clean review pass per PR. The version and `velo-ext` pin reconciled on rebase (0.11 to 0.13, 0.5.0 to 0.5.1). Then publish 0.13.0 and ucx-rs, which unblocks the adapter. |
| aiperf: a concurrency ramp | aiperf | This version ramps request rate, not concurrency, so every run opens with an 8,192-request burst. | A feature request. The rig reports steady-state percentiles until then. |
| rig: `AIPERF_WORKERS_MAX` | dynamo `benchmarks/frontend/scripts/run_perf.sh` | Decouples aiperf's worker pool from `--concurrency`. | Record it in the results manifest in the same change. |

## How it was measured

Two GB200 nodes: the frontend, aiperf, etcd and nats on one, eight mocker processes of 64 workers on the other. Concurrency 8,192, 250,000 requests per rep after 8,192 warm-up requests, Qwen3-0.6B token shapes, three reps per arm per matrix. The frontend and the load generator are pinned to disjoint cores. Per rep the rig keeps aiperf's per-request export, the frontend and mocker logs, Prometheus scrapes of every process, and `/proc/stat` for the CPU window. Reps are compared only at the same backlog draw, because which mocker processes fall behind during the burst sets throughput and the tails for both arms. Percentile verdicts read the steady-state columns. What the rig cannot show: real engines, real token lengths, more than one frontend, and the burst itself as a fair comparison.
