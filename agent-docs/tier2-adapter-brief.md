<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Tier-2 velo response plane: design brief

Dated 2026-09-02. Synthesized from four reconnaissance reports (Dynamo seam, benchmark rig, velo readiness, cluster fabric) over `.research/dyn-pin` (Dynamo at PR 11996 head `3a67ae2e6e`, velo dep wired) and this repo's working tree. This brief carries settled rulings for the implementation stage. Supersede with a dated addendum, not a rewrite.

## Corrections to the competitive plan doc

1. **`dyn-pin` is PR 11996 alone, stacked on Dynamo main `f166f6ec03`.** PR 11918 (mux TCP) is a *divergent* branch (merge-base `ed0be1828d`, ~40 commits behind), not the base of 11996. There is no `tcp/mux.rs` in dyn-pin and no `DYN_RESPONSE_PLANE` value for the mux.
2. Therefore `DYN_RESPONSE_PLANE=tcp` in dyn-pin **is** the upstream-main per-request call-home path (fresh `TcpStream` per logical stream, `tcp/client.rs:114`). The control arm is free.
3. The plan's "`DYN_RESPONSE_PLANE` slots in the same way as `DYN_REQUEST_PLANE`" is wrong for the harness scripts (zero hits in `benchmarks/frontend/`), but irrelevant in practice: `run_perf.sh` and `LocalExecutor` inherit the caller's environment, so exporting the var works with zero harness edits.
4. The plan doc's `credit_sweep_interval` citation "2 ms (`mod.rs:274,292`)" is stale against the working tree (200 ms default after the drain-hook change).

## Benchmark matrix (ruling)

| arm | build | select | status |
|---|---|---|---|
| A `tcp-per-request` (upstream main control) | dyn-pin | `DYN_RESPONSE_PLANE=tcp` | free |
| C `quic` (PR 11996) | dyn-pin | `DYN_RESPONSE_PLANE=quic` | free |
| D `velo` (mux over TCP) | dyn-pin + adapter | `DYN_RESPONSE_PLANE=velo` | **this brief** |
| B `mux-tcp` (PR 11918) | separate build of `refs/dyn-pr/11918` at its own head | unconditional in that build | second priority; cross-base skew (~40 commits) stated in the writeup, not hidden |
| velo-over-UCX | — | — | **deferred**: `rhino-dev-260831.sqsh` has no rdma-core (4 independent checks); ucx-rs hard-fails without headers; every existing UCX test is `UCX_TLS=tcp`; and all Dynamo arms are TCP, so UCX is a fourth column, not the velo column. Needs a new container image (`libibverbs-dev librdmacm-dev` + runtime libs) and a bring-up run first. Fabric is ready: 6× IB NDR 400 Gb/s ACTIVE per node |

Primary metrics, per the published table: TTFT p50/p95/p99, req/s, errors, and **frontend CPU per request computed by us** — 1 Hz `/proc/<frontend_pid>/stat` deltas windowed to the aiperf measurement interval, divided by aiperf's request count. The shipped Dynamo analysis tooling computes none of this.

## Adapter design (rulings)

New module `lib/runtime/src/pipeline/network/velo_response.rs` (+`velo_response/` submodules if it grows). As a descendant of `pipeline::network` it can construct `StreamSender { tx, prologue }`, `StreamReceiver { rx }`, and use `RegisteredStream::{new, with_registration_id, with_cleanup}` with **zero visibility changes** (precedent: `quic_response.rs:1185`, `push_handler.rs:1075`). The one visibility edit: `trait ResponsePublisher` (`push_handler.rs:143`) becomes `pub(crate)`.

- `TRANSPORT_NAME = "velo-response"`; `ResponsePlaneMode::Velo`; `from_config_value` accepts `"velo"`; `name() = "velo"`. Python: widen `choices` in `runtime_args.py:205`, `frontend_args.py:480`, `mocker/args.py:608`, and the set in `common/utils/runtime.py:79`.
- **Stream identity and addressing travel in the request payload**, like both existing planes: `VeloResponseConnectionInfo { version, frontend_instance_id: Uuid, frontend_worker_address_b64: String, anchor_handle: {hi: u64, lo: u64}, request_id: String }` serialized as JSON into `ConnectionInfo.info`. Nothing goes into etcd/NATS discovery; `VeloBuilder::discovery` stays unset.
- **Frontend** (`VeloResponseServer`, owned by `DistributedRuntime` behind a `OnceCell` like QUIC): one `Arc<Velo>` per process — TCP transport, `stream_bind_addr` from `DYN_VELO_RESPONSE_STREAM_HOST`/`_PORT` (defaults mirroring the TCP server's auto-detect), `.metrics(VeloMetrics)` on its own `prometheus::Registry`, `.messenger_mux(MuxConfig { enabled: true, initial_credit, credit_sweep_interval: 200ms explicit, flush_policy, ..Default::default() })`. `register_response(ctx)` creates `StreamAnchor<VeloResponseFrame>`, spawns a consumer task, returns `RegisteredStream` whose oneshot resolves on the prologue frame.
- **Frame type** (velo streams are typed; Dynamo's `rx` carries encoded bodies only):
  ```rust
  enum VeloResponseFrame {
      Prologue { error: Option<String> },
      Data(#[serde(with = "serde_bytes")] Vec<u8>),
      Aborted,
  }
  ```
  Consumer task: first frame must be `Prologue` — `error: None` resolves the oneshot with `StreamReceiver { rx }`; `error: Some(e)` resolves `Err(e)`. `Data` forwards into `rx`; `Aborted` or `Finalized` ends the task (drop of the mpsc tx closes `rx`). `strict_prologue() = true` (QUIC's choice — a swallowed prologue failure would surface as tail latency, polluting the measured metric).
- **Worker** (`VeloResponseClientPool`): one `Arc<Velo>` per process (same builder shape, ephemeral bind). Per frontend first-seen: `register_peer(PeerInfo::new(instance_id, WorkerAddress::from_encoded(bytes)))`, memoized in a `DashMap`. Per request: `attach_anchor::<VeloResponseFrame>(handle)`, then **assert `negotiated_transport() == Some(MESSENGER_MUX_KEY)` and fail the request otherwise** — a silent legacy fallback would benchmark the per-stream path while labelled velo.
- **`VeloResponseSender` implements `ResponsePublisher`**: `send_prologue` sends `Prologue` (tracks `prologue_sent`, errors on double-send instead of panicking); `send` wraps bytes in `Data`; `finish()` calls `finalize()` (consumes the sender); `abort()` sends `Aborted` then finalizes.
- **Drop safety (deadlock-adjacent, non-negotiable):** `velo::StreamSender::{finalize, detach, Drop}` do a *synchronous* `flume::Sender::send` into a bounded inlet — the documented teardown-deadlock path (`agent-docs/mux-negotiation-hang.md`, reproduced live this session on the exact pinned tree). The adapter must never let a velo sender hit `Drop` on an async worker: hold it in `Option`, and on `Drop` of `VeloResponseSender` without an explicit finish, `tokio::task::spawn_blocking` the finalize. Keep `initial_credit` at the 256 default; never below 64.
- **Flush cadence:** the Dynamo engine loop cannot call `velo.flush_batch()`. Ruling: use the mux `AutoFlush` linger if `MuxConfig` expresses it; otherwise spawn one flusher task per process calling `flush_batch()` every `DYN_VELO_RESPONSE_FLUSH_INTERVAL_US` (default 1000 — parity with PR 11918's 1 ms batch interval). Implementer verifies `FlushPolicy::Auto` semantics in `messenger_mux/mod.rs` and picks the cheaper mechanism; either way the interval must be env-tunable.
- **Cancellation (v1 ruling):** frontend stop/kill drops the anchor; the worker's next `send` gets a `SendError`, the pump breaks and calls `context.stop_generating()` — the same observable behavior as a dead per-request socket. No reverse control frames in v1 (the benchmark runs `ignore_eos` with fixed OSL; aborts are not on the measured path). Record this as a known delta in the writeup.
- **Teardown ordering** (worker and frontend both): stop producing → explicit `finalize()` per sender → drain anchors to `Finalized` under a wall-clock guard → `velo.graceful_shutdown(ShutdownPolicy::Timeout(..))` → drop `Arc<Velo>`. Never `WaitForever`.
- **Env surface** (new `environment_names.rs` module): `DYN_VELO_RESPONSE_STREAM_HOST/PORT`, `DYN_VELO_RESPONSE_BUFFER_CAPACITY` (mailbox, default 64 — parity), `DYN_VELO_RESPONSE_INITIAL_CREDIT` (256), `DYN_VELO_RESPONSE_FLUSH_INTERVAL_US` (1000).
- **Metrics v1:** skip the Prometheus mirror module; at teardown, gather velo's own registry and log `velo_streaming_mux_records_per_batch` etc. so tokens-per-write is auditable. A `metrics/velo_response.rs` mirror is follow-up.
- **Sequencing note:** if PR 11918 is ever ported onto this seam, its `StreamSender`/`StreamReceiver` refactor breaks every construction site this adapter adds. Land the adapter first; do not interleave.

## Tests (land with the code)

- Third plane in `response_plane_mismatch_reports_error_before_generate` (`push_handler.rs:975`) and `response_plane_mode_parses_supported_values` (`network.rs:549`).
- `VeloResponseConnectionInfo` JSON round-trip; prologue-exactly-once (double-send errors); data-before-prologue rejected.
- An in-process loopback test: register → attach → prologue → N data → finalize → receiver sees N bodies then closed channel; and the mux-negotiated assertion failing when the frontend velo has `enabled: false`.
- Python `choices` tests: `test_config.py`, `test_runtime_args.py` third value.

## Rig plan

- Iteration config (from PR 11918's own qualification): 16 workers, concurrency 2048, ISL 1024, OSL 256, speedup 10, `--benchmark-duration 30`; single node, `run_perf.sh` driven, `DYN_RESPONSE_PLANE` exported. Under 2 min per run.
- Full config: 2 nodes (`--segment=2` confirmed working), frontend alone on node A, workers on node B via `--num-workers` process-packing; etcd+NATS on node A; aiperf on node A against the frontend. 256–512 workers, concurrency 4096–8192, speedup 5, 120 s, ≥3 interleaved reps per arm.
- Prereqs staged by job 2704276: `.research/bin/{etcd,nats-server}` (arm64), `.research/aiperf-venv` (aiperf 0.10.0 + maturin), Qwen3-0.6B tokenizer in `/work/hf_cache`. The `ai-dynamo` wheel needs `maturin` against `lib/bindings/python` in the container — first build is its own job.
- Slurm discipline: every step `--cpus-per-task=144` (without it, `--exclusive` still cgroups the step to 1 CPU — verified); short jobs (≤30 min) schedule in ~1 min even under load; `RUSTUP_HOME=/work/.rustup-aarch64 CARGO_HOME=/work/.cargo-aarch64`.
- Reproducibility gate: **commit the velo streaming work before the first measured run** — dyn-pin pins this working tree by path, and today's tree differs from HEAD (200 ms sweep default).
- Cluster confound, checked and resolved: velo requests 2 MiB socket buffers (`lib/velo/src/transports/tcp/listener.rs:513`, `transport.rs:645`); this cluster's unraisable `net.core.{r,w}mem_max = 212992` clamps that to ~208 KiB and the explicit setsockopt locks out autotune. At 0.5 ms RTT that caps one connection near 400 MB/s. Worst-case mux traffic at the published rig's rate is ~200 MB/s *total*, so the clamp does not bite provided workers are packed into **≥4 mocker processes** (≥4 mux connections). Rule: use ≥4 worker processes per node in every velo-arm config, and state the clamp in the writeup.
