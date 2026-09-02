<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Session handoff — velo vs Dynamo response plane

Written 2026-09-02. Everything below is uncommitted work in the working tree at
`/lustre/fsw/core_dlfw_ci/ryan/velo`, branch `main` at `19921ca`.

Read this file, then `dynamo-response-plane-competitive-plan.md` beside it.

---

## The original ask

Dynamo opened [PR 11996](https://github.com/ai-dynamo/dynamo/pull/11996), a
batched QUIC response plane. The question was whether velo is a better path
forward, whether we should steal any of their optimisations, and how to test the
two side by side.

## What was found

**The framing in the ask was off, and this is the single most important thing to
carry forward.** PR 11996 is the *second* of two stacked PRs.
[PR 11918](https://github.com/ai-dynamo/dynamo/pull/11918) multiplexes TCP
response streams and is where nearly all the win is:

| variant | req/s | TTFT p50 | TTFT p95 | errors |
|---|---|---|---|---|
| upstream main (one conn per request) | 1807 | 54 ms | **1954 ms** | **1089** |
| PR 11918 — multiplexed TCP | 1803 | 48 ms | **102 ms** | **0** |
| PR 11996 — batched QUIC | 1936 | 33 ms | 79 ms | 0 |

Multiplexing does it all; QUIC adds 7.4% throughput for 6% more frontend CPU.
**Velo's Messenger mux is the same idea and shipped first.** So the competitor
is 11918, not the QUIC PR, and their bottleneck is frontend CPU rather than the
wire (throughput barely moves across all three rows).

Head-to-head, from a workflow that judged each design decision and then had a
second adversarial pass try to refute it: **14 equivalent, 1 velo better, 2 not
applicable, 3 dynamo better** against PR 11918. Against the QUIC PR, 9 of 16
came back "velo already has it."

Velo is ahead on: negotiated-with-fallback (theirs is mandatory, so a mixed
fleet fails every cross-version request during rollout), graceful drain (theirs
has no drain frame kind at all), per-slot credit with a reserved terminal
credit, and 48 metric series against 12.

Dynamo is ahead on frontend per-stream overhead: 0 tasks per logical stream
against velo's 3, no per-frame timer, one runqueue hop per token against two.

---

## State of the working tree

### New files (all mine, none committed)

| path | what |
|---|---|
| `examples/examples/response_plane_bench.rs` | Tier-1 load harness — TTFT/ITL HDR histograms, req/s, CPU from `/proc/self/stat`, `--engines` to 256, `--credit-sweep-interval-ms` A/B knob, `--json` |
| `examples/examples/response_plane_bench.evidence.md` | measured results, **including a retraction banner — read it** |
| `lib/velo/tests/streaming/mux_credit.rs` | 4 tests for drain-driven credit return |
| `agent-docs/dynamo-response-plane-competitive-plan.md` | the plan and the full comparison |
| `agent-docs/mux-negotiation-hang.md` | **a deadlock on `main`, diagnosed** |
| `agent-docs/SESSION-HANDOFF.md` | this file |

### Modified by me

`lib/velo/src/streaming/{anchor.rs, control.rs, control/tests.rs,
messenger_mux/mod.rs, messenger_mux/ingress/mod.rs,
messenger_mux/ingress/tests.rs, mpsc/control.rs, BATCHING.md}` and
`lib/velo/Cargo.toml` (registers the new test). Plus `examples/Cargo.toml` and
`Cargo.lock` (registers the new example, adds `serde_json`).

### Modified but NOT by me — do not attribute or revert

`examples/examples/{throughput.rs, mpsc_fanin.rs, soak/**}` and
`examples/src/lib.rs` were **already dirty when this session started**. Leave
them alone. There is also a pre-existing `cargo fmt` diff in
`examples/examples/mpsc_fanin.rs` that is not mine.

### Scratch (gitignored, safe to delete)

`.research/` holds staged Dynamo sources, benchmark scripts, raw JSONL, and
build logs. `target-drain/` and `examples/target-drain/` are isolated build dirs
used because the shared `target/` was lock-contended.

---

## Work item 1: drain-driven credit return — COMPLETE, needs review

`BATCHING.md:391` specified that `reader_pump` return credit on each handoff.
It was never wired; the mux reconciled buffer occupancy on a 500 Hz sweep
instead, and `BATCHING.md:956` records that deviation as "the same effect".
It is not the same effect. This change wires it.

**Design, as landed:**

- The hook is a **doorbell, not a ledger** — it carries no quantity. It posts
  the peer on a bounded lane; `IngressSlot::reconcile` remains the only thing
  that decides how much credit was freed. That is what makes it safe to run
  concurrently with the surviving sweep: a redundant visit recomputes the same
  answer, where a delta would double-count.
- Wakes coalesce on a **per-peer `AtomicBool`**, cleared *before* the reconcile
  so a drain landing mid-visit posts a fresh wake instead of being swallowed.
- **Both pumps are hooked.** MPSC negotiates the mux in the same version as
  SPSC, so `mpsc_reader_pump` needed it too; without that the relaxed default is
  a silent 100x regression for MPSC streams.
- `PumpContext` replaces three positional args, because the eighth argument
  would trip `clippy::too_many_arguments` and `CLAUDE.md` forbids the `allow`.
- **`velo-ext` is untouched.** `AnchorManager` already holds a concrete
  `OnceLock<Arc<MessengerMuxTransport>>`, so no trait change and no coordinated
  version bump.
- `credit_sweep_interval` default 2 ms -> 200 ms.

**Verification, all green:**

```
mux_credit                4 passed   (the target test failed before the change)
velo lib                749 passed, 0 failed
cargo fmt --check       clean
cargo clippy            zero warnings
```

**Performance, measured on an exclusive node, twice:**

Net **−4.5% CPU per token** at 256 ingress peers, and the accounting matters:
relaxing the sweep 2 ms -> 200 ms is worth ~7.5%, the hook is what makes that
relaxation *safe*, and the hook itself costs ~2% back. In 34 of 42 paired runs
the hook alone made CPU slightly worse. **Ship this on correctness, not on the
performance number.**

---

## Work item 2: a deadlock on `main` — DIAGNOSED, NOT FIXED

See `agent-docs/mux-negotiation-hang.md`. This is arguably the most valuable
output of the session and it is **not caused by any of the work above** — it
reproduces on a pristine tree.

`concurrent_streams_to_one_peer_share_the_batch_flow`
(`lib/velo/tests/streaming/mux_negotiation/mod.rs:363`) never completes. Bisect:

| streams | credit | outcome |
|---|---|---|
| 2, 4, 5 | 8 | ok, ~0.05 s |
| **6, 8** | **8** | **deadlock** |
| **6** | **512** | **ok, 0.04 s** |

The last row is decisive: the same six streams pass with a wider window, so
**credit exhaustion is the variable, not stream count**. Three threads parked in
`futex_wait`, no CPU burned, runtime workers already gone — the block is
synchronous, on a worker thread. `slot_stream.rs`'s own module docs describe
exactly this hazard: `finalize`/`detach`/`Drop` reach the inlet through a
synchronous `flume::Sender::send` that blocks when full, and the inlet is sized
`initial_credit + 1`.

Matters beyond a test fix: many streams to one peer is the shape the mux exists
for, and CI runs `--all-features --all-targets`, so this should be visible there.

Open design question: should the terminal path reserve an inlet slot the way the
credit ledger already reserves one for the terminal record?

---

## Work item 3: Tier 2 — NOT STARTED, dependency-unblocked

The decisive experiment is running Dynamo's own rig three ways — `tcp`, `quic`,
`velo` — so every variable except the response plane is fixed. **We never ran a
head-to-head; the whole comparison above is code-reading plus adversarial
review.**

The dependency gate **passed** (reproduce with `.research/runtime-gate.sh`):

```
cargo tree -p dynamo-runtime -d   empty — no duplicate crates
cargo check -p dynamo-runtime     0
dynamo-runtime --tests            0     (smoke test names velo types from inside)
velo-ext prometheus count         0
```

The velo pin in Dynamo was the feared blocker and is **three import lines** —
`velo::backend::*` was renamed `velo::transports::*`. All three kvbm crates
compile against velo 0.10.0. Worktree: `.research/dyn-pin/`.

What remains is the adapter itself, ~250-350 lines, and it **must live inside
`dynamo-runtime`** — the seam is not extensible from outside, because
`RegisteredStream`'s constructors are `pub(crate)` and `StreamReceiver.rx` /
`StreamSender{tx,prologue}` are private fields.

Two things to design in from the start:
1. **Measure against upstream `main` as a third arm.** Both Dynamo PRs are
   unmerged and 11918 is labelled Stale; upstream main is the only column that
   ships today, and beating it is publishable regardless.
2. **The adapter must enable the mux explicitly.** `MuxConfig::enabled` is
   `false` by default, and a velo response plane with the mux off *is* the
   one-socket-per-stream architecture that produced their 1954 ms tail.

---

## Errors made this session — read before trusting any number

Four, and the pattern is worth knowing.

1. **The "+21% CPU" figure was measured on a shared login node** with ~11 other
   users. Caught only because the user asked whether runs went through Slurm.
   Retracted across seven files. **All benchmarks must run under
   `srun --exclusive`** — `.research/gap1-node.sh` is the correct pattern.
2. **A `biased` select in my own change starved its drain arm** — the ticker was
   polled first, so at short intervals the event path barely ran. Fixed. It did
   *not* change the A/B result, so the theory that it explained the first
   negative result was also wrong.
3. **A negative result was reported before verifying the code did what was
   claimed.** Twice, in fact — once from the environment, once from my own bug.
4. **Five test call sites were rewritten with a regex**, producing code that
   compiled and passed but failed `fmt` and clippy. Tests hid it. Run the gates
   *during* iteration, not at the end.

The compounding lesson: a number was trusted faster than the code that produced
it, which is the opposite of the order `CLAUDE.md` prescribes.

**Related and still standing:** gaps 2 and 3 in the plan document (per-stream
tokio tasks, the extra runqueue hop) come from the *same* style of reasoning
that produced the retracted 21%. They have no A/B knob and were never measured.
Treat them as arithmetic, not findings.

---

## Environment

- Login node is **x86_64**; all compute nodes are **aarch64**, 144 cores, 940 GB.
  Binaries built here will not run there.
- `cargo build -p velo` fails on the login node — no `protoc` (grpc) and no
  `cmake` (zmq). `--no-default-features` works. `protoc` is at
  `/lustre/fsw/core_dlfw_ci/ryan/.cache/kvbm-build-tools/protoc-29.3/bin/protoc`.
- **The container has everything**: `enroot_images/rhino-dev-260831.sqsh` —
  protoc, cmake, libclang-18, Rust 1.96.1.
- Slurm needs `--account=core_dlfw_ci` and a job name shaped
  `core_dlfw_ci-<subproject>.<detail>`.
- `ptrace` is restricted on the login node, so `gdb -p` cannot attach.
- `gh` is **not** authenticated; PR bodies were read over the public web.

---

## Suggested next steps, in order

1. **Review the drain-driven credit change** and decide whether it ships. It is
   complete, tested and gate-clean; the case is correctness, with ~4.5% as a
   secondary benefit.
2. **Fix the deadlock.** It is on `main`, in the workload shape the mux targets,
   and the bisect makes it cheap to pick up.
3. **Decide whether the mux becomes the default.** Currently opt-in, which means
   stock velo ships the architecture Dynamo already beat.
4. **Build the Tier-2 adapter** and get a real head-to-head.
5. Lower priority: QUIC transport revival (PR 24 predates the crate collapse),
   and lifting the mux surface into `velo-ext` (`BATCHING.md` P11), which is the
   modularity half of the original goal and currently unaddressed.

---

## Addendum 2026-09-02 — review verdict on work item 1

A 45-agent adversarial review (five lenses, three-refuter panel per finding)
upheld the design claims and returned four real findings. All four are fixed
on branch `drain-credit-return`:

1. **Doorbell visit rate had no floor.** On a peer that drains continuously,
   the sweep task ran back-to-back O(slots) walks under the peer mutex —
   measured 3,560 walks/s at window 8 against ~40 µs per walk. Fix:
   `MuxConfig::drain_visit_floor` (default 2 ms) defers a wake that lands
   inside the floor; the armed flag makes later drains coalesce into the
   scheduled visit. Measured after: 328 walks/s. New metric:
   `velo_streaming_mux_drain_visits_total`. A second defect (a one-visit
   floor overshoot when a deferred peer also got a periodic sweep) was found
   and fixed the same way, failing test first.
2. **`draining_and_sweeping_together_never_overspend_the_window` promised
   `assert_no_reader_stall` and never called it.** The helper is now ported
   onto the fixture and called; `streaming_mux_credit` now declares
   `required-features = ["test-helpers"]` like its siblings.
3. `take_mux_drain_signal` sat inside `flush_mux_batches`'s doc comment.
   Each function has its own doc now.
4. `DrainSignal::drained()`'s doc contradicted the failure path. It now
   states what the code does: a full lane puts the flag back down.

The "work proportional to drains" wording in the module docs and BATCHING.md
overclaimed and is corrected to floored-per-peer, bounded-by-drains.

Gates after the fixes: fmt clean, clippy clean, `streaming_mux_credit` 5/5,
`streaming_mux_negotiation` 16/16 (known hang skipped), lib 721/721.
`MuxConfig` gained a public field, so the workspace version is 0.11.0.

The pre-existing teardown deadlock (work item 2) was re-confirmed live on
this tree — the drain hook does not fix it, as expected.
