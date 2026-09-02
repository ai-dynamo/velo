<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Velo against Dynamo's new response plane — findings and plan

Status: **draft for review**. Dated 2026-08-31. Written after reading both Dynamo pull requests and the velo source, and after checking what this cluster can actually run.

This document answers three questions. What did Dynamo build, and what is it worth? Where does velo stand against it? How do we measure the two side by side and settle it with numbers rather than opinion?

---

## 1. What Dynamo built

The work people are calling "the QUIC PR" is really two stacked pull requests against `lib/runtime`, both by the same author.

| PR | What it does | State |
|---|---|---|
| [11918](https://github.com/ai-dynamo/dynamo/pull/11918) | Multiplexes TCP response streams. Deletes the old one-connection-per-request path. Made **mandatory** in its second commit. | Open, labelled Stale |
| [11996](https://github.com/ai-dynamo/dynamo/pull/11996) | Adds a fixed-lane QUIC response plane on top, opt-in behind `DYN_RESPONSE_PLANE=quic`. | Open |

Both change the **response plane** only — the path a worker uses to stream generated tokens back to a frontend. The request plane and the request callbacks stay on TCP.

### Their published numbers

Rig: two NUMA-local frontends, 512 mock workers over two nodes, the Dynamo `mocker` engine at speedup 5, Rust AIPerf at concurrency 8192, 120 seconds, a fixed set of 269,135 requests.

| Variant | req/s | TTFT p50 | TTFT p95 | TTFT p99 | Frontend CPU per req | Errors |
|---|---|---|---|---|---|---|
| Upstream main — one TCP connection per request | 1807.3 | 53.6 ms | 1954 ms | 4314 ms | — | 1089 |
| PR 11918 — multiplexed TCP | 1803.0 | 48.0 ms | **102 ms** | **286 ms** | 36.6 ms | **0** |
| PR 11996 — batched QUIC | **1936.4** | **32.8 ms** | 79 ms | 277 ms | 38.8 ms | 0 |

### The reading that matters

**Almost the whole win is the multiplexing, not QUIC.** PR 11918 alone takes the tail from 1954 ms to 102 ms and the errors from 1089 to zero. QUIC then adds 7.4% throughput and about 15 ms of median time-to-first-token, and costs 6.2% more frontend CPU per request.

So the thing velo is really competing with is PR 11918. And PR 11918 is the same idea as velo's Messenger mux: stop opening a connection per stream, carry many logical streams over shared connectivity, and batch the frames. Velo shipped that idea first. QUIC is a second-order question.

Throughput barely moved across all three rows (1807 → 1803 → 1936). That says the bottleneck in their rig is not the wire. It is frontend CPU. Any change we make should be judged on frontend CPU per request and on the latency tail, not on raw bandwidth.

---

## 2. Where velo stands

Velo's `messenger_mux` buckets stream frames by destination peer into one `_stream_batch` active message, negotiated per attach as `messenger-mux-v1`. Design doc: `lib/velo/src/streaming/BATCHING.md`. Measured evidence: `examples/examples/batched_streaming.evidence.md`.

### Where velo is ahead

These are real advantages, not consolation prizes.

- **Per-slot credit flow control.** Velo issues HTTP/2-style credit per slot against a mux-owned buffer, with a reserved terminal credit (`lib/velo/src/streaming/messenger_mux/flow_control/mod.rs:229`). Dynamo's is a per-stream byte window, coarser.
- **The protocol is negotiated, with a fallback.** Velo intersects supported transport keys per attach and falls back to `tcp-stream`. Dynamo's mux is **mandatory with no negotiation** — commit `06d22825e9` deleted the kill switch and the old path together. A mixed-version fleet during a rollout fails every cross-version request.
- **Graceful shutdown.** Velo has a three-phase Gate → Drain → Teardown with a dedicated shutdown lane. Dynamo's mux has **no graceful drain at all** — no Drain, GoAway or Shutdown frame kind exists. On writer exit it fails every queued frame and kills every stream on the connection.
- **Observability breadth.** 48 velo series against Dynamo's 12.
- **Ordered dispatch lanes** keyed on the sending peer, so one slow sender does not reorder another's traffic.

### Where velo has a real gap

This section was rewritten after the evidence came in. My first draft listed eight gaps from reading the code. I then had every one of them judged against velo's source and then adversarially re-checked by a second pass whose job was to refute the claim. **Most did not survive.**

Of 20 head-to-head comparisons against PR 11918, the scoreboard is: 14 **equivalent**, 1 **velo better**, 2 **not applicable**, and **3 where Dynamo is genuinely ahead**. Of 16 comparisons against the QUIC PR, 9 came back "velo already has it" and the rest were declines.

The three real gaps are all the same thing wearing different clothes: **frontend-side per-stream and per-frame overhead at 4096 live streams.** That matters because the frontend is exactly where Dynamo's bottleneck is, and because the metric PR 11918 actually moved was queueing latency, not bandwidth.

> **Measurement status. Read this before quoting any number from this document.**
>
> **The "+21% CPU per token" figure previously reported here is retracted.** It
> was measured on a shared login node with about eleven other users on it, not
> under an exclusive allocation. A paired re-measurement — same workload, both
> binaries interleaved run by run — showed within-cell spread of up to
> 5.5 us/token against an effect of roughly 2 us/token. The noise was as large
> as the signal.
>
> The failure is worth recording precisely because nothing about the rig was
> wrong. The harness reproduces `batched_streaming`'s ratio to within noise, the
> workload is the right shape, the normalization (CPU per token, identical token
> counts across arms) is sound. It was still worthless, because of *where it
> ran*. Every benchmark from here on goes through `srun --exclusive`;
> `.research/gap1-node.sh` is that run, and this section will be rewritten from
> its output.
>
> **Gap 1 is still real as a defect**, and that part does not rest on any
> timing. `lib/velo/tests/streaming/mux_credit.rs` shows the sweep was the only
> thing returning credit to a quiet peer: with the sweep unreachable, a draining
> consumer's stream dies at frame 4 of a 4-credit window. That is what made 2 ms
> load-bearing. What it *cost* is the open question.
>
> **Gaps 2 and 3 remain unmeasured**, as before. Neither has an A/B knob.
>
> The earlier attempt with `batched_streaming` failed for a structural reason worth keeping: at three anchor hosts the sweep is free, so that example could never have shown this. The axis is ingress peers, and ingress peers are *engines*, not anchor hosts.

**Gap 1 — the credit sweep does 2 million no-op slot visits per second.** *(2026-09-02: fixed on branch `drain-credit-return` — drain-driven return with a per-peer `drain_visit_floor`, sweep relaxed to 200 ms as backstop; the line numbers below describe the pre-fix code.)* `MuxCore::sweep` (`messenger_mux/mod.rs:498`) ran at 500 Hz by default (`credit_sweep_interval` 2 ms). Per tick it allocates a `Vec<WorkerId>` of every peer (`ingress/mod.rs:153`), takes each peer's mutex — the same one `handle_batch` takes on the ingress hot path (`ingress/mod.rs:167`) — and walks every slot. At 512 peers and 4096 slots per frontend that is **256,000 mutex acquisitions and 2.05 million slot visits per second**, nearly all of which early-return doing nothing (`ingress/slot.rs:180`). Dynamo does zero periodic work for the same job: credit return is a branch inside a `poll_next` the consumer already runs (`network.rs:522-531`). Velo's own `BATCHING.md` specifies the drain-driven return that would fix this; it was never wired.

**Gap 2 — three tokio tasks per logical stream, where Dynamo has zero.** `reader_pump` is spawned per attach (`control.rs:552`) and rebuilds a `tokio::time::timeout` on **every single frame** (`control.rs:316`), so each token costs a timer insert, a timer cancel, a task wake and a second channel hop. At 4096 streams per frontend that is 4096 tasks contending the timer wheel where Dynamo runs 4 pooled reader tasks and no timers. Separately, the 60-second bind-expiry task (`mod.rs:594`) is never cancelled when the bind is claimed, accumulating roughly **58,000 sleeping tasks per frontend** at steady state.

**Gap 3 — one extra runqueue traversal per token.** Velo: ingress `try_send` wakes `reader_pump`, which `try_send`s and wakes the consumer. Two wakes. Dynamo deleted exactly this hop in PR 11918 and now has one. The CPU cost is small (under 1% of frontend CPU) — but the cost lands in **queueing latency on a saturated frontend**, which is precisely the metric that went from 1954 ms to 102 ms. Related and free to fix alongside: `Record::body_range` is documented for zero-copy `Bytes` slicing (`protocol/mod.rs:367`) and is unused outside tests, so ingress does a `body.to_vec()` copy per record (`ingress/mod.rs:332`).

**Gap 4 (operational, not perf) — no stream-setup latency metric and no physical-write counter.** Dynamo's `setup_seconds` is what makes their tail-latency claim auditable, and `frames_per_write` is what proves the mux works at all. Velo's `velo_transport_frames_total{direction="outbound"}` counts admissions, not wire writes, and the mux's `_stream_batch` path has no flush observer at all. **We cannot make Dynamo's argument without these, regardless of how fast we are.**

### Claims from my first draft that the evidence killed

Recording these because being wrong in public is cheaper than quietly dropping them.

- **`SO_REUSEPORT`** — verdict `not_applicable`, upheld under adversarial review with high confidence. Dynamo needs 8 reuse-port endpoints because one `quinn::Endpoint` is one UDP socket with one demux driver for all 512 workers. TCP gets per-connection demux from the kernel for free, so velo's frontend already has 512 sockets, 512 receive queues and 512 reader tasks. Dynamo's *own TCP mux* has one listener and no reuse-port. The mechanism is cheap but buys velo nothing. **Caveat that matters: this flips if we ship a QUIC transport.** Then it becomes mandatory, and the exclusive-first-bind ordering has to come with it.
- **One connection per peer** — `velo_equivalent`. Both stacks use long-lived process-lifetime connections shared by many streams. They differ in cardinality (4 vs 1), and the ingress-parallelism comparison came back equivalent because velo's per-peer batchers already spread across the tokio pool.
- **Priority lane** — `velo_equivalent`, and the QUIC lane-split came back "deliberately decline". Velo reaches the same outcome by a different mechanism: liveness records bypass batching, data does not. Dynamo's urgent lane exists to let a Prologue overtake a queue their design deliberately builds; velo does not build that queue.
- **Batch-cap misalignment (60 vs 64 KiB)** — not upheld as a real cost. Velo's flatten-threshold and one-writev-per-batch behaviour came back equivalent to Dynamo's.
- **Mux off by default** — still true, still a decision to make, but it is a flag rather than a missing mechanism.

### Still open, and still worth doing

**The mux is not in `velo-ext`** (`BATCHING.md` P11, unimplemented). Out-of-tree `FrameTransport` implementors get no multiplexing. Since modularity is half of what we are claiming, this is awkward — we are asking people to write transports against a surface that excludes our best feature.

---

## 3. How we test

The honest problem: velo's evidence today is five nodes in one process on loopback, 24 to 96 requests. Dynamo's is 512 workers on two nodes at concurrency 8192. Those do not compare.

I propose a ladder. Each rung produces a number, and each rung is useful even if we stop there.

### Tier 1 — velo-native load harness — **built**

`examples/examples/response_plane_bench.rs`, registered in `examples/Cargo.toml`. It models the serving shape `batched_streaming` established — N frontends, M engines, continuous batching, one token per active request per forward pass — and adds what was needed to measure rather than demonstrate:

- **TTFT and inter-token latency as HDR histograms**, reported as p50/p95/p99.
- **Requests per second, tokens per wire write, and process CPU** split user/system from `/proc/self/stat`.
- **`--engines` scalable to 256**, which is the ingress-peer axis the frontend-side costs scale on.
- **`--credit-sweep-interval-ms`**, the A/B knob that made gap 1 measurable.
- **`--json`** for sweep scripting; `--warmup-requests` to keep connection setup out of the histograms.
- Correctness gates that fail the run if any request, token or ordering is wrong, so a fast wrong answer cannot be reported as a result.

Validated against the known-good example: it reports 5.41 : 1 tokens per write where `batched_streaming` reports 5.38 : 1 at the same configuration.

**Known limits.** One process on loopback, so reported CPU is the whole topology's rather than one frontend's — sound for an A/B where only a frontend-side parameter moves, not sound for comparison against Dynamo's absolute 36.56 ms/req. It OOMs above roughly 256 nodes in a process, so Dynamo's full 512-per-frontend shape needs a multi-process extension that is not built. Absolute latencies are queueing-dominated and should be read as relative comparisons between arms.

Value: fast iteration, and it is the only rig where a velo change can be A/B'd in minutes. Weakness unchanged: it is our benchmark measuring our code.

### Tier 2 — velo as a third response plane inside Dynamo (decisive)

Run Dynamo's own rig three ways on the same box: `--response-plane tcp`, `--response-plane quic`, `--response-plane velo`. Same mocker, same AIPerf, same frontend. This is the only experiment that settles the argument, because every variable except the response plane is held fixed.

Dynamo already has the harness. `benchmarks/frontend/scripts/sweep_runner.py` has a local mode that starts mocker and frontend, runs AIPerf, and tears down between runs. Its documented "transport saturation sweep" is `--concurrency 4096 --num-requests 16384,32768 --workers 1,2,4,8` — our shape almost exactly. It already threads `DYN_REQUEST_PLANE` through as an environment variable, so `DYN_RESPONSE_PLANE` slots in the same way. It collects `/proc/<pid>/stat`, `perf stat`, flamegraphs and bpf context-switch traces, which is where their frontend-CPU number comes from.

What it costs us — measured from the QUIC arm, not guessed:
- About 250 to 350 lines of plumbing across the four files that match on `ResponsePlaneMode`, plus the adapter module itself.
- **The adapter has to live inside `dynamo-runtime`.** The seam is not extensible from outside: `RegisteredStream::{new, with_registration_id, with_cleanup}` are `pub(crate)`, and `StreamReceiver.rx` and `StreamSender{tx, prologue}` are private fields. A third response plane cannot be an out-of-tree crate implementing a trait. It must be a sibling module.

### What Tier 2 is actually built on, and why that is uncomfortable

Both Dynamo PRs are **unmerged**, and PR 11918 — the one that carries nearly all the win — is labelled **Stale**. A third response plane written against them inherits their fate. Three things follow and the plan should say them out loud rather than discover them later.

1. **The baseline may move.** If 11918 is reworked or abandoned, the "multiplexed TCP" column we are measuring against changes or disappears. Our velo arm sits on the same `ResponsePlaneMode` seam that PR 11996 introduced, so it moves with it.
2. **We should measure against three arms, not two.** Include upstream `main` (the per-request call-home path) as a control. It is the only column that exists in a shipped release today, and it is the one a Dynamo user actually experiences. If velo beats upstream main by the margin 11918 does, that is a publishable result regardless of whether 11918 ever lands.
3. **Prefer landing the seam over landing the transport.** The cheapest durable outcome is for the `ResponsePlaneMode` seam to become extensible — the constructors it needs are `pub(crate)` today — so a velo arm is additive rather than a fork. That is a smaller, more reviewable ask than "take our transport", and it survives either PR being reworked.

There is a real possibility worth naming: the reason 11918 went stale may be reviewer resistance to the size of the change (it deletes 1251 lines of the existing client and makes the new path mandatory). A velo arm that is *opt-in and negotiated* is a strictly easier review than the thing that stalled.

### Mux-off-by-default is a Tier 2 blocker, not a footnote

`MuxConfig::enabled = false` (`messenger_mux/mod.rs:287`) is listed above as a decision to make. For Tier 2 it is a precondition. A velo response plane that runs with the mux off is the *legacy per-stream socket path* — one connection per stream, which is precisely the upstream-main architecture that produced 1954 ms tail latency and 1089 errors. We would be benchmarking the thing Dynamo already beat.

So the Tier 2 adapter must turn the mux on explicitly, and the benchmark matrix should include mux-off as a deliberate control column — it is the closest analogue of upstream main and makes the comparison legible.

### Tier 3 — multi-node at their scale

Two nodes, 512 mockers, concurrency 8192, on Slurm. Same rig as Tier 2, more nodes. This is what we publish.

---

## 4. Blockers, checked rather than assumed

**The velo version pin is real, but much smaller than it first looks.** Dynamo's root `Cargo.toml` reads `velo = { version = "0.1.0" }`, resolved from crates.io, consumed by `kvbm-config`, `kvbm-engine` and `kvbm-physical`. This repository is at 0.10.0.

A `[patch.crates-io]` entry **cannot work**: 0.10.0 is semver-incompatible with a `"0.1.0"` requirement, so Cargo refuses to apply the patch and says so. The pin has to be edited directly, which means the three kvbm crates get the new velo whether they are ready or not.

So I counted what those crates actually use. It is 41 references across 26 files, and the surface is narrow:

| Symbol | Count | Status in 0.10.0 |
|---|---|---|
| `velo::Messenger` | 10 | unchanged |
| `velo::{Event, EventManager, EventHandle, EventAwaiter, EventStatus}` | 17 | unchanged — the events re-export list is byte-identical between the two versions |
| `velo::TypedUnaryResult` | 2 | unchanged |
| `velo::discovery::FilesystemPeerDiscovery` | 1 | unchanged, same path |
| `velo::backend::Transport` | 1 | **renamed** to `velo::transports::Transport` |
| `velo::backend::tcp::TcpTransportBuilder` | 2 | **renamed** to `velo::transports::tcp::TcpTransportBuilder` |

velo 0.1.0 was the facade crate over the nine siblings — it re-exported `velo_transports as backend`. The collapse renamed that module. The messenger re-export list in 0.10.0 is a strict superset of 0.1.0's: it adds names and removes none.

**At the name level the whole break is three import lines.** I initially flagged `velo::VeloLeaderService` as a fourth break; it is not. That is kvbm's own local module `kvbm-engine/src/leader/velo/`, not the velo crate.

**I then ran the compile, and it passes.** Inside `rhino-dev-260831.sqsh` on an aarch64 compute node, with the pin repointed at this repository and the three imports rewritten:

```
=== EXIT kvbm-config:   0 ===
=== EXIT kvbm-physical: 0 ===
=== EXIT kvbm-engine:   0 ===
```

Zero errors across all three crates. Signatures did not drift underneath the names. **The velo version pin is not a blocker — it is three import lines and a `Cargo.toml` edit.** Reproduce with `.research/pin-gate.sh`; the worktree is `.research/dyn-pin/`.

**That check proved less than it looked, so I ran the one that matters.** Those three kvbm crates already depended on velo; showing they survive a version bump says nothing about Tier 2. Tier 2 needs **`dynamo-runtime`** — which has no velo dependency at all today — to take a new one. That is the actual gate. Adding `velo = { workspace = true }` to `lib/runtime/Cargo.toml` and a smoke test that names `velo::Messenger`, `velo::transports::tcp::TcpTransportBuilder` and `velo::streaming::StreamAnchor<Vec<u8>>` from inside the crate:

```
=== EXIT dupes:                 0 ===   # cargo tree -p dynamo-runtime -d → empty
=== EXIT dynamo-runtime:        0 ===
=== EXIT dynamo-runtime-tests:  0 ===
velo-ext prometheus count:      0
```

Four things this establishes, each of which could have sunk Tier 2:

- `dynamo-runtime` compiles with velo linked, and velo 0.10.0 genuinely builds into that tree (not merely declared).
- **`cargo tree -d` is empty.** No duplicate crates — the dual-copy semver bug class that motivated velo's own workspace collapse does not reproduce inside Dynamo's tree.
- The dependency versions align exactly where it counts: `prometheus` 0.14, `dashmap` 6.1, `parking_lot` 0.12.5, `uuid` 1.18.1 on both sides, and Dynamo's hard `tokio = "=1.48.0"` satisfies velo's `"1"`.
- **`velo-ext` pulls zero `prometheus`** even inside Dynamo's tree, so the trait-crate boundary acceptance test from `CLAUDE.md` still holds.

Reproduce with `.research/runtime-gate.sh`. **Tier 2 is unblocked at the dependency level.** What remains is the adapter itself and the seam's `pub(crate)` constructors, which are code to write, not risks to discover.

(The check had to run in a container: the login node has no `libclang`, so `nixl-sys`'s build script fails before reaching any velo code. It also has no `protoc` or `cmake`, which is why `cargo build -p velo` fails there on default features while `--no-default-features` succeeds.)

**Good news on the container.** `enroot_images/rhino-dev-260831.sqsh`, built today, has protobuf-compiler, cmake, libclang-dev, ninja and Rust 1.96.1. That is the full toolchain for both velo and Dynamo. Verified by running it.

**The cluster is aarch64. This login node is x86_64.** Compute nodes have 144 cores. Dynamo's published numbers do not state their architecture, and are probably x86. So we cannot chase their absolute numbers. What we can do — and what is actually more rigorous — is run all three response planes on our own hardware and compare them against each other.

**AIPerf is out of tree**, `aiperf==0.10.0` from PyPI, and is not installed here. Neither is the `dynamo` Python package.

**The mocker needs no GPU.** It is a simulated engine; `--speedup-ratio 5` runs mock execution five times faster than the modelled latency. This is a CPU-only benchmark, which is why 512 workers on two nodes is affordable.

---

## 5. What I recommend

**Step zero is done and it passed.** The pin is not a blocker. Tier 2 is therefore the priority, and it is much cheaper than I first assessed.

### The work, in order

**A. Observability first, because it gates every claim we make.** Add a stream-setup latency histogram (dispatch to first frame) and a real physical-write counter on the `_stream_batch` path. Today `velo_transport_frames_total{direction="outbound"}` counts admissions rather than wire writes, and the mux path has no flush observer at all. Without these we cannot make Dynamo's argument even if we win. Small, and it unblocks measurement of everything below.

**B. The frontend overhead cluster.** All three real gaps are the same problem and share one fix shape:

1. **Wire the drain-driven credit return** that `BATCHING.md` already specifies — add an optional drain hook to `reader_pump`, have the mux slot return credit at a byte threshold (Dynamo uses 64 KiB), then relax `credit_sweep_interval` from 2 ms toward hundreds of milliseconds. Keep the sweep task; batcher eviction free-rides on it and matters more to velo than to Dynamo, which reaps nothing. Medium effort because `reader_pump` is shared with the TCP and gRPC paths, so the hook must be optional and keep those allocation-free.
2. **Move the per-stream timers onto the sweep.** Give `IngressSlot` a last-arrival `Instant` and let the sweep fire the watchdog, so `reader_pump` stops building a timer per frame. Age out unclaimed binds the same way, deleting ~58,000 sleeping tasks per frontend. Small, purely additive on machinery that already runs.
3. **Collapse the extra hop** by letting `StreamAnchor` poll the mux-owned buffer directly, giving the other writers a second lane the anchor selects over. This preserves the credit proof (the mux buffer must stay single-writer) while removing a runqueue traversal per token, and fixes the uncounted-256 double-buffer problem in the same change. Take the zero-copy `body_range` win alongside it. Medium, and worth doing after 1 and 2 land.

**C. QUIC transport.** Port PR 24 to the current layout. Note the interaction: **shipping QUIC makes `SO_REUSEPORT` mandatory**, because one `quinn::Endpoint` is one UDP socket with one demux driver — the exact chokepoint Dynamo hit and fixed with 8 endpoints. Port their exclusive-first-bind ordering with it.

**D. Tier 1 harness** in parallel with B, as the daily iteration loop for measuring whether B actually moved anything.

### What I would not copy

- **`SO_REUSEPORT` for the TCP path.** Verdict was `not_applicable` and it survived adversarial review at high confidence. TCP gets per-connection demux from the kernel; Dynamo's own TCP mux has one listener too. Only revisit under C.
- **Mandatory-with-no-negotiation rollout.** A mixed-version fleet fails 100% of cross-version requests. Velo's per-attach negotiation is better and should stay.
- **No graceful drain.** Velo's Gate → Drain → Teardown is a genuine advantage.
- **Per-frame metric deletion.** Dynamo deleted theirs to save two `Instant::now()` reads per frame. Velo's counters are already per-batch and per-direction, so we are past their endpoint.

---

## 6. Decisions taken, and what is still open

### Taken

- **Tier 2 first.** Pursue velo as a third response plane inside Dynamo and measure it under Dynamo's own rig. The concern that motivated asking — the velo version pin — turned out to be three import lines, and the compile passes on all three kvbm crates.
- **Benchmark on multi-node Slurm**, matching their rig shape rather than a single box.
- **Revive the QUIC transport** (velo PR 24) rather than deferring it. I had argued for deprioritising it on the grounds that QUIC is worth only 7.4% over a good mux; the decision went the other way, and there is a fair case for it — it removes "they have QUIC and we do not" as a talking point regardless of the measured delta. Note the consequence recorded in §5: QUIC makes `SO_REUSEPORT` mandatory rather than optional.
- **Skip the GitHub review threads.** Work from the code and the published numbers.

### Open

1. **`SO_REUSEPORT` needs re-confirming.** It was selected for the steal list on my recommendation, and I have since retracted that recommendation — the evidence says `not_applicable` for the TCP path, upheld adversarially at high confidence. It becomes mandatory only under the QUIC work, which is now in scope. So it is still on the list, but for a different reason and at a different time. Flagging rather than silently reinterpreting the choice.
2. **How closely do we match their exact rig?** 512 workers, 8192 concurrency and 269,135 requests is what we would publish, but it is not a loop we can iterate on daily. A smaller sweep for development and the full rig for the final number is the obvious split; worth confirming that is acceptable.
3. **Does the mux become the default?** Still a live decision, independent of everything above. Dynamo made theirs mandatory; ours ships off.
4. **Does the mux surface go into `velo-ext`** (`BATCHING.md` P11)? This is the modularity half of the goal and nothing above addresses it.

---

## Appendix — where the sources are staged

- Dynamo PR 11918 files and diff: `.research/dynamo-pr11918/`
- Dynamo PR 11996 files and diff: `.research/dynamo-pr11996/`
- Velo PR 24 (QUIC) staged copies: `.research/velo-pr24/`
- A Dynamo worktree at the PR 11996 head with the velo pin edited: `.research/dyn-pin/`
- Fetched refs in the Dynamo checkout: `refs/dyn-pr/11918`, `refs/dyn-pr/11996`
- Fetched refs here: `refs/velo-pr/24`, `refs/velo-pr/66` through `69`
