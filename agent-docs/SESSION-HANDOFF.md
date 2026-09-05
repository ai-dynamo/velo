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
used because the shared `target/` was lock-contended. Note they are untracked but
**not** gitignored, so a `git add -A` would sweep them into a commit; stage by
path until someone adds them to `.gitignore`.

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

## Work item 2: a deadlock on `main` — FIXED 2026-09-02

> Fixed on branch `drain-credit-return-teardown-fix` (on top of `3eab5c9`). The
> cause was **not** the missing terminal reserve this section guesses at below:
> the batcher drains fine and never parks on admission, and the inlet was full of
> data records. The synchronous terminal send blocked the runtime's only worker,
> which starved the batcher that would have released it. `finalize`, `detach` and
> `Drop` now escalate a full channel to a task that awaits the space instead of a
> thread that blocks on it, so the invariant no longer depends on a buffer size.
> Full mechanism, stack evidence and gate results are in the "FIXED" section of
> `agent-docs/mux-negotiation-hang.md`. The original diagnosis below is kept as
> written.
>
> One more hang of the same class turned up and was **left alone**:
> `test_mpsc_local_drop_preserved_under_backpressure` blocks on `MpscSender`'s
> `Drop` (`mpsc/sender.rs:259`). It hangs with the fix stashed too, so it predates
> this work, and its own assertion demands the blocking behaviour — so it needs a
> ruling rather than a repeat of the patch.

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
2. ~~**Fix the deadlock.**~~ Done — see work item 2. Review it: it changes when
   `detach` clears the attachment flag, which is the only behaviour change a
   caller can observe.
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

---

## Addendum 2026-09-03 — measured head-to-head complete

The Tier-2 adapter was implemented, adversarially reviewed, and benchmarked
at both scales. **Results: `agent-docs/response-plane-benchmark-results.md`.**
Headline: velo beats upstream main's per-request plane by 13–14% req/s with
24–31% lower TTFT p99 and zero errors at the 512-worker/8192-concurrency
shape; PR 11996 QUIC cannot complete a clean full-scale run on this cluster;
PR 11918's build is faster end-to-end but cross-base (ai-dynamo 1.3.0 vs
1.5.0) with no control arm, so its response-plane share is unresolved.

Adapter code lives uncommitted in `.research/dyn-pin` (velo_response.rs +
glue + Python choices); the rig is `.research/rig/` (t3-submit.sh /
t3-submit-m18.sh). Design rulings and known deltas:
`tier2-adapter-brief.md` addendum.

Open items needing a ruling or follow-up:
1. **Port PR 11918 onto the current Dynamo base** — the one measurement that
   would isolate its mux against velo's fairly.
2. **May `MpscSender::Drop` block a runtime worker?** Its own test asserts
   the blocking (`tests/streaming/mpsc_integration.rs:288`); same defect
   class as the fixed teardown deadlock. Needs a design ruling.
3. Velo adapter CPU headroom (~2.4 ms/req vs bare TCP; velo0 closes most).
4. Upstream Dynamo issue-worthy finds: silent discovery `Conflict`
   (`controller.rs:391-404`, no logging, permanently non-serving fleet) and
   aiperf 0.10.0's unbounded record wait on mmap decode failures.
5. UCX arm: needs a container image with rdma-core; fabric verified healthy.
6. Branch `drain-credit-return` (5 commits) is PR-ready; `gh` is not
   authenticated here, so nothing was pushed.

### Second-pass review, 2026-09-02

A second adversarial pass over the fix itself returned one high finding and two
low ones.

1. **The deferral queue ratcheted.** A periodic tick calls `sweep_peer` on every
   peer, including one whose walk was already queued, and that clears its wake;
   the consumer's next drain re-arms and posts a second wake inside the same
   floor. The old `admit` queued a second entry for it, and `due` then re-queued
   whichever entry lost — permanent residue, one entry per tick, with the sweep
   task's queue work growing to match. Confirmed twice before fixing: the
   reviewer's live probe saw 59 floor-spaced walks continue after the traffic had
   provably stopped, and a unit-level replay of the pattern grew the queue by
   exactly one entry per round, monotone, 64 rounds to 64 entries. Fixed by
   bounding the queue to one entry per peer: a queued walk is the authoritative
   next one, so `admit` answers a wake with it rather than queueing a second and
   never walks a queued peer out of band, `due` clears the membership on pop, and
   `forget_stale` keeps a peer whose walk is queued regardless of its age.
2. **`drain_visit_floor` could overflow the deferral deadline.** `last + floor`
   panics for an absurd `Duration`. The floor is now clamped to an hour, which is
   already "the doorbell is off".
3. **`drain_pending` never shrinks across peer churn — known, not fixed.**
   Recorded on the field. It mirrors the `peers` map beside it, and naive removal
   is a footgun rather than a cleanup: a pump holds its peer's flag as an `Arc`
   for the life of its stream, so removing the map entry while that pump lives
   leaves it setting a flag nothing reads — permanently true, permanently
   coalescing, and that peer's credit back on the periodic sweep for the rest of
   the stream. It may only be removed under the same visibility that retires
   slots and binds.

Deviation from the review's fix ruling, deliberate: the ruling said a redundant
pop should be *dropped* rather than re-queued. Under the bound as implemented a
redundant pop is unreachable, because an entry exists only for a peer `admit` has
refused to walk since it was queued. Taking the ruling literally would have left
`admit` free to walk a queued peer once the floor elapsed, which strands that
entry and reopens the residue by a second route — and opens a narrow lost-wake
race along the way. Refusing that walk is what makes the bound structural.

## Addendum 2026-09-03 (afternoon) — UCX arm measured, PR 11918 ported, session paused mid-pipeline

Session paused deliberately by the user with work in a known-good, resumable state. Everything below is uncommitted (dyn-pin and rig changes live under gitignored `.research/`; the velo repo tracked tree is unchanged apart from the pre-existing `examples/` dirty files that are not ours).

### Completed since the last addendum

- **Container**: `enroot_images/rhino-dev-260903.sqsh` = rhino-dev-260831 + rdma-core dev/runtime + mlx5 provider (`ibverbs-providers`) + `libnuma-dev`. Built by `.research/rig/build-ucx-image.sh` (job 2711035). In-container `ibv_devinfo` shows 2 ACTIVE mlx5 HCAs, link_layer InfiniBand. Old image untouched; `RIG_IMG`/`IN_CTR_IMG` env overrides select the image (rig defaults still point at 260831).
- **UCX transport selection in the adapter** (dyn-pin, uncommitted): `DYN_VELO_RESPONSE_TRANSPORT=tcp|ucx` (hard error otherwise), exactly one transport registered per node, dyn-pin's velo dep now `features = ["ucx"]`. Teardown metric dump widened to include `velo_transport*`. Rig: `ucx` arm (= velo0 + the env var) across single-node and t3 scripts; `.research/rig/assert-ucx-transit.sh` fails a run unless `velo_transport_frames_total{transport="ucx"}` > 0 AND the tcp label is untouched.
- **Wheel**: rebuilt in the new image with UCX compiled in (job 2711115, provenance-checked). Smoke `smoke-ucx1`: velo0 + ucx both green, ucx moved 4,971 frames over the ucx transport.
- **UCX measured** (matrix `t3-ucx1`, job 2711829, COMPLETED 34 min, `.research/results/t3-ucx1/summary.jsonl`): tcp 2798/2187/2196 req/s (p99 TTFT ~3.2 s, 0 err); velo0 3017/2847/2684 (p99 2.1–2.5 s, 0 err); **ucx over IB 2526/2875/2355 with p99 TTFT 9151/2016/7678 ms and 1,722 HTTP-500 errors in rep1** (reps 2–3 clean). velo0-over-TCP remains the best arm. The ucx errors are frontend 500 "Failed to generate completions"; frontend-side `velo_transport_rejections_total{transport="ucx"}` all zero. Diagnosis was launched and then stopped for the pause — no findings yet.
- **PR 11918 ported onto the current base** (plan `.research/m18-port-plan.md`, executed): new `mux_response/` module in dyn-pin behind `DYN_RESPONSE_PLANE=mux-tcp`, arm `mux18p`, wire protocol byte-identical, drift ledger A1–A15 in `mux_response/mod.rs` module docs, `dynamo_tcp_response_mux_*` metrics preserved. Check gate green before the fidelity fixes: clippy clean, 23 mux tests, 716-test runtime suite, 11918's golden pipelined-handshake test passing. Control arms untouched (`tcp/`, `quic_response.rs` not in git status).
- **Adversarial fidelity review of the port** (independent model): faithful on the measured path. F1 fixed by us afterwards: mux listener now sets `SO_LINGER(0)` on accepts (ledger A15) + env doc comment names `mux-tcp`. F2 is a results-doc caveat, not a defect: 1.5.0's shared pump downgrades final-marker publish failures to debug when stopped-not-killed (all four arms share it; differs from 11918's unconditional error!). 
- **velo issue filed**: https://github.com/ai-dynamo/velo/issues/75 (may MpscSender::Drop block a runtime worker). gh is authenticated now — the drain-credit-return draft PR is unblocked but not yet created.

### In flight at pause

- Slurm job **2712163** (`m18p-linger` check gate: check-m18p.sh re-run validating the SO_LINGER fix) was RUNNING at pause and left to complete on its own. Log: `.research/logs/inctr-m18p-linger-8749.log`. Expect `FINAL: green=true` with 5 zero exit codes; the fix is 6 lines + docs, low risk.
- The ucx-arm diagnosis agent was stopped before producing findings. The brief to re-issue: (1) what produced rep1's 1,722 500s (`.research/results/t3-ucx1/rep1-ucx/logs/`, clean reps 2–3 as controls); (2) mechanism for the p99 swings — read `lib/velo/src/transports/ucx/` progress model (spin_us, worker progression, starvation with 8 packed processes) and what UCX_TLS resolved to (run logs echo it); (3) at most three tuning knobs for one rerun, or "velo-side defect, fix first" if honest.

### Restart sequence (the user's stated order: UCX number → shareable doc → 11918 measurement → doc update)

1. Read `.research/logs/inctr-m18p-linger-8749.log`; if not green, fix and re-gate (`IN_CTR_IMG=.../rhino-dev-260903.sqsh bash .research/in-ctr.sh --label m18p-check --time 00:50:00 /work/velo/.research/rig/check-m18p.sh`).
2. Re-launch the ucx diagnosis (brief above). Its ruling decides whether the final matrix carries a tuned `ucx` rerun or the doc reports the measured instability as-is with tuning as an open item.
3. Rebuild the wheel with the port in it (`IN_CTR_IMG=... bash .research/in-ctr.sh --label wheel-m18p --time 01:30:00 /work/velo/.research/rig/build-wheel.sh`) — this is the point of no return for the t3-ucx1 venv; the matrix is done, so it is safe.
4. Smoke: `IN_CTR_IMG=... ARMS='velo0 mux18p' bash .research/in-ctr.sh --label smoke-m18p --time 00:30:00 /work/velo/.research/rig/smoke-all.sh m18p1`.
5. Final matrix: `RIG_IMG=... ARMS="tcp velo0 mux18p" REPS=3 bash .research/rig/t3-submit.sh m18p1` (append ` ucx` with tuned env if step 2 justifies it; a 4-arm × 3-rep matrix ran in ~45 min last time).
6. Build the shareable results page (artifact) from `agent-docs/response-plane-benchmark-results.md` + t3-ucx1 numbers; update both after the mux18p matrix. Caveats to carry: ucx arm rides IB while all others ride 200G Ethernet (that is the arm's thesis — say it, don't bury it); F2 pump-classification note; mux18p CPU column IS valid this time (same wheel, same sampler).
7. Parked for the user: draft PR for drain-credit-return (gh now works); mux default-on decision; velo-ext mux surface (BATCHING.md P11).

## Addendum 2026-09-04 — restart sequence completed end to end

Every step of the 2026-09-03 restart sequence ran to completion. The measurement campaign is done.

### What happened, in order

1. **m18p-linger gate green** (job 2712163): `FINAL: green=true`, all five exit codes zero. The SO_LINGER fix holds.
2. **UCX diagnosis completed and adversarially verified** (workflow `wf_77836dba-6e1`: 2 opus investigators + 1 sonnet env agent, then an adversarial verify pass on the combined reports). Ruling: **velo-defect-fix-first, zero tuning knobs** — the final matrix carried no ucx arm. Full causal chain, refuted alternatives, and fix shape recorded in `agent-docs/ucx-arm-instability-diagnosis.md`. Short form: router imbalance (arm-independent) parks ~5.7k streams on one worker process; all 64 packed workers share one UCX peer link; the UCX send path has no backpressure edge (`ucp_am_send_nbx` never refuses, `inflight_ops` write-only until teardown, unbounded AdmissionGate queue, one shared 1024 ring vs TCP's 256 per connection); the batcher never parks (7–9 rec/batch vs 18–29 on velo0); >15 s backlog accumulates below velo's last instrumented point; heartbeats queue behind data; the watchdog kills 1,722 live streams → HTTP 500s. Fixes before any rerun: (a) gate UCX admission on in-flight ops / per-peer ring share, (b) heartbeat lane that cannot sit behind data or a starved-vs-dead watchdog distinction, (c) instrument the UCX inbound path + export worker-side metrics in the rig. Also: the rig records no git sha per run (rig_run_meta.json) — a provenance gap worth closing.
3. **Wheel rebuilt** with the mux18p port (job 2721367, provenance-checked, entry points OK). **Smoke green** (job 2721408, `smoke-m18p1`): velo0 1,113 req/s and mux18p 1,141 req/s, 256/256 each, zero errors, mux transit confirmed.
4. **Final matrix `t3-m18p1` COMPLETED** (job 2721416, `ARMS="tcp velo0 mux18p"` REPS=3, overall_rc=0, zero errors on all nine reps). Means: tcp 2,511 req/s (p99 TTFT 3,470 ms, CPU 9.07), velo0 3,019 (p99 2,742, ITL p99 18–28, CPU 9.19), mux18p 2,746 (p99 1,746, TTFT p50 86–168 ms, CPU 6.51). velo0 leads throughput +9.9% over mux18p with a 165 req/s rep spread vs mux18p's 701; mux18p wins first-token latency and frontend CPU decisively. The 1.3.0 build's 4,614 req/s is now attributable mostly to stack, not plane. Mux transit verified per rep (32 accepted `dynamo_tcp_response_mux` connections, ~250k stream setups; no mux series in velo0 reps).
5. **Docs updated**: `response-plane-benchmark-results.md` got an Addendum 2026-09-04 with both matrices (t3-ucx1 + t3-m18p1); the original Q2 ("unresolved") is superseded in place with a pointer. New deep-dive doc: `ucx-arm-instability-diagnosis.md`.
6. **Shareable results page published and current**: https://claude.ai/code/artifact/13c13139-9672-4f0e-9ab2-748aa21701fb (private until shared from its share menu). Carries the verdict stat row, both full-scale matrices with per-rep charts/tables, the ucx diagnosis callout, the mux18 caveat, tier-2 table, and all method notes (F2 pump classification included).

### Parked for the user (unchanged plus new)

- Draft PR for drain-credit-return (gh authenticated, branch ready).
- Mux default-on decision; velo-ext mux surface (BATCHING.md P11).
- The three UCX fixes above — and whether to file a velo issue for the missing UCX backpressure edge (the diagnosis doc is written to be lifted into one).
- mux18p's first-token-latency and frontend-CPU win suggests a targeted look at the velo adapter's first-token path (`tier2-adapter-brief.md` deltas).
- Rig: record a git sha in rig_run_meta.json per run.

## Addendum 2026-09-04 (later) — TTFT gap diagnosed, results page rewritten, win plan drafted

The user asked why mux18p's TTFT is so much better and whether velo fails to flush at the end of a forward pass. Answered with a verified diagnosis (workflow `wf_72ac5040-593`: 3 opus + 1 sonnet investigators, adversarial verify; a 250k-request four-point log join per rep, clock skew < 1 ms).

- **Flush ruled out**: velo0 has no timer on its path (AutoFlush on_admission, OpenSlot eagerly flushed); t3e's 1 ms-linger control was TTFT-identical; mux18p itself debounces 1 ms and is fast.
- **Mechanism**: the whole gap is in the response side (request plane exonerated, B p90 ≤ 56 ms). velo0's ~1.1 s = a standing backlog in the frontend's fixed-parallelism ingest (one unbounded message_rx + one decode task + 8 unbounded per-sender ordered lanes; ~3,414 requests awaiting first token / 3,084 req/s = 1.11 s), paid from the tail roughly twice per request (awaited pre-generate `_anchor_attach` RTT + first data batch). tcp's 1.2–1.4 s is a different mechanism: bistable per-request connect/accept (~3,900 unaccepted sockets when jammed; rep2 ran clear at 106 ms). mux18p is flat because nothing on its first-record path scales with load (frontend-minted stream id in the request envelope, fire-and-forget prologue on an urgent lane, 256 KiB bounded per-connection queue). ITL caveat: aiperf ITL arithmetic flatters high-TTFT arms; velo0's real advantage is E2E p99 6.8 s vs mux18p's 26.1 s via admission discipline (~58% vs 94–98% streaming concurrency).
- **Docs**: `agent-docs/ttft-gap-diagnosis.md` (evidence, fact reconciliation, ranked change candidates); `agent-docs/velo-response-plane-win-plan.md` (W0 instrumentation → W1 shard frontend ingest → W2 per-record cost cuts → W3 zero-RTT stream setup → W4 urgent ingest class; W5 bounded ingest held back; W6 = UCX fixes; isolation matrix with one-variable arms; success criteria). Nothing implemented — plan only, per the user's instruction.
- **Results page rewritten** (same URL, version `ttft-explained-rewrite`): plain-English problem statement and onboarding, t3-m18p1 as the canonical matrix with a scoreboard and a TTFT-p50 chart, the mechanism section with a three-lane diagram, the plan summary, condensed RDMA section. Superseded t3e/tier-2 tables flushed to prose control findings (QUIC verdict, linger control, small-scale control, mux18 1.3.0 history).
- Benchmark doc got a pointer to the diagnosis and plan in its 2026-09-04 addendum readings.

Next when resumed: the user wants velo to win every category — execute the win plan starting at W0 (instrumentation) and the isolation matrix. Data artifacts from the join live in the session scratchpad (FINDINGS.txt and scripts) and the workflow journals.

## Restart 2026-09-04 — win-plan execution: the detailed sequence

The next session executes `agent-docs/velo-response-plane-win-plan.md`. This section is the operational script for it. The mechanisms it builds on are in `ttft-gap-diagnosis.md` (TTFT) and `ucx-arm-instability-diagnosis.md` (UCX); do not re-litigate them — re-verify only where a step's own gate demands it.

### Ground rules (all sessions so far obeyed these; keep them)

- Edit on the login node. Build, test, and benchmark ONLY on compute nodes through `.research/in-ctr.sh` and `.research/rig/*` (aarch64). Use `IN_CTR_IMG`/`RIG_IMG=/lustre/fsw/core_dlfw_ci/ryan/enroot_images/rhino-dev-260903.sqsh` explicitly for every launch — script defaults still point at the old 260831 image, and only 260903 has rdma-core for the `ucx` feature.
- `.research/dyn-pin` is a gitignored working copy of dynamo-runtime 1.5.0 carrying uncommitted local mods (velo response-plane adapter env mapping, the `mux_response/` port, transport selection). Never clean, reset, or re-pin it. The wheel (`.research/rig/build-wheel.sh`) builds it against the velo working tree into `.research/aiperf-venv`.
- Pre-existing dirty `examples/` files and `target-drain/` are NOT ours. Never touch or revert them.
- `gh` is the only GitHub credential (`gh auth status` first; if it fails, ask the user to run `gh auth login`). Draft PRs; one reviewable concern per PR; tests land with the code they cover, written first; no assistant brand in commits/PRs, no Co-Authored-By lines. Branch each PR from current `main`.
- velo repo discipline per CLAUDE.md: clippy `-D warnings` all-features, `cargo fmt`, semver gate; anything touching `velo-ext` needs default impls and a coordinated `=`-pin bump. Test suites under coreutils `timeout` (900 full / 300 targeted) inside in-ctr check scripts — model them on `.research/rig/check-m18p.sh`.
- Subagents: opus/sonnet are the heavy lifters (research, implementation, log work); fable orchestrates, synthesizes, and runs adversarial verification. Before human review of any PR, run `wills-mega-review`.
- The shareable results page is https://claude.ai/code/artifact/13c13139-9672-4f0e-9ab2-748aa21701fb — a new session updates it by passing that URL as `url` to the Artifact tool (publishing without `url` forks a new page; don't).

### Step 0 — orientation and working-tree hygiene

1. Read, in order: `velo-response-plane-win-plan.md`, `ttft-gap-diagnosis.md`, `ucx-arm-instability-diagnosis.md`, this addendum. Reusable measurement tooling: `.research/analysis/ttft-join/` (the 250k-request four-point join scripts, FINDINGS.txt, first-record path notes). Raw matrices: `.research/results/t3-m18p1/` and `t3-ucx1/`. Full investigation journals: `~/.claude/projects/-lustre-fsw-core-dlfw-ci-ryan-velo/*/subagents/workflows/` runs `wf_72ac5040-593` (TTFT) and `wf_77836dba-6e1` (UCX).
2. The tree sits on branch `drain-credit-return` with uncommitted agent-docs changes (ours). First action: `git checkout -b response-plane-docs main`, commit ONLY the agent-docs paths (`SESSION-HANDOFF.md`, `response-plane-benchmark-results.md`, `ttft-gap-diagnosis.md`, `ucx-arm-instability-diagnosis.md`, `velo-response-plane-win-plan.md`), open a draft docs-only PR. Then return to per-workstream branches off `main` for code. Never commit `examples/` or `target-drain/`.

### Step 1 — W0: instrumentation (velo PR + rig-local dyn-pin work)

- velo PR: a depth gauge on the node-global `message_rx`; per-ordered-lane depth and wait-time metrics on the `_stream_batch` dispatch path; an attach round-trip histogram if no existing series covers it. Tests first (registration + depth accounting under dispatch).
- dyn-pin (rig-local, uncommitted, same pattern as the transport-selection work): register velo's Prometheus registry into the frontend `/metrics`; extend the teardown metric dump to every mocker process so worker-side velo series exist; scrape the existing `WORK_HANDLER_TIME_TO_FIRST_RESPONSE_SECONDS` histogram (`push_handler.rs:794`).
- rig: write the velo git sha (`git -C /work/velo rev-parse HEAD`) and a dyn-pin describe into `rig_run_meta.json` — this closes the provenance gap both diagnoses flagged.
- Gate (check script via in-ctr) → wheel (`--label wheel-w0`) → smoke (`ARMS='velo0'`) → probe: `RIG_IMG=... REPS=1 ARMS=velo0 bash .research/rig/t3-submit.sh w0-probe`.
- Exit criterion: the ~1.1 s is measurably split between message_rx wait and ordered-lane wait, and the attach RTT is a histogram. Record the split as a dated addendum in `ttft-gap-diagnosis.md`; it decides whether W1 starts at the decode task or the lanes.

### Step 2 — W1 (shard the ingest) and W2 (per-record cost), one velo PR each

- Implement both behind env-selectable velo config so ONE wheel runs baseline and variants interleaved in a single matrix. Add rig arms exactly the way `mux18p` was added (arm cases in `t3-workers.sh:115`, `t3-frontend.sh:122`, `smoke-arm.sh:50`, plus each script's valid-arm list): `velo1` = velo0+W1, `velo2` = velo0+W2, `velo12` = both. The env gates are experiment instruments: after the isolation matrix picks winners, a follow-up PR removes the losing path and the gate — no `legacy` names, per the compatibility policy.
- W1 tests first: per-slot ordering property test (cross-slot reorder is legal, per-slot `frame_seq` order is not), credit conservation under concurrent shards, a saturation test asserting bounded lane wait. W2's four line items (reader_pump per-frame timer, IngressSlot::deliver copy, one-wake-per-batch anchor delivery, attach-timeout task cancellation) each get their own test.
- Gate → wheel → smoke `ARMS='velo0 velo1 velo12'` → matrix: `RIG_IMG=... ARMS="tcp velo0 velo1 velo12 mux18p" REPS=3 bash .research/rig/t3-submit.sh w12` (~75 min for 5 arms).
- Decision point: if velo12 hits TTFT p50 <= 200 ms at >= 3,000 req/s with E2E p99 <= 8 s, W4 becomes optional polish and W5 is dropped.

### Step 3 — W3 (zero-RTT setup) and W4 (urgent ingest class)

- W3 spans velo protocol (pre-bound ingress slots, envelope-carried stream identity, peer-level credit defaults in the hello) and the dyn-pin adapter (carry the identity in the request envelope; drop the awaited attach). Tests: handshake compatibility golden test, orphan-slot reclamation when a request dies before its first batch, credit accounting with pre-bound slots. Watch the velo-ext boundary — any new public type referenced by trait signatures forces the coordinated bump.
- W4: an urgent class visible pre-decode (handler name or frame tag) for OpenSlot/Prologue/first-data; cap urgent share; property test that urgent cannot starve data. Design it together with W6(b)'s heartbeat lane — same concept, two layers.
- Arms `velo3` (W3 only) and `veloF` (W1+W2+W3+W4). Final matrix: `ARMS="tcp velo0 velo3 veloF mux18p" REPS=3` tag `win1`.

### Step 4 — verdict, docs, page

- Success bar (from the plan): TTFT p50 <= 200 ms, TTFT p99 <= 1,750 ms, throughput >= 3,000 req/s with rep spread <= 300, E2E p99 <= 8 s, frontend CPU <= 6.5 ms/req, zero errors. E2E p99 is not in `summary.jsonl` — compute it from `aiperf/profile_export.jsonl` with the join scripts in `.research/analysis/ttft-join/`.
- Update `response-plane-benchmark-results.md` (dated addendum), the artifact page (pass the URL above as `url`), and this handoff. Run `wills-mega-review` on each open PR, then hand to human review.

### Parked (carried forward)

Draft PR for the `drain-credit-return` branch; mux default-on decision; velo-ext mux surface (BATCHING.md P11); W6 UCX fixes (and the possible velo issue lifted from `ucx-arm-instability-diagnosis.md`); velo issue #75 follow-up; W5 bounded ingest only if a residual TTFT gap survives W1+W2+W4.

## Addendum 2026-09-04 (evening): Step 0 and Step 1 done, the seat moved, W0b in flight

### State on disk

- Branches and PRs: `response-plane-docs` off `main` is draft PR #76 (docs only: the five campaign docs, `ingest-cost-ledger.md`, and the dated addenda; every commit signed off, the DCO check requires `git commit -s`). `drain-credit-return` is pushed to origin with no PR of its own (still parked for the user). `w0-ingest-metrics` is draft PR #77 with base `drain-credit-return` (one commit, cd8c076, the W0 instruments; W0b becomes its second commit). Code PRs stack on `drain-credit-return` because `main` lacks af58539 (the terminal-sentinel deadlock fix) and the measured velo0 baseline. CI (`ci.yml`) runs only on `main` and mirrored `pull-request/N` branches, so the compute-node gates are the check for stacked PRs. The working tree at `/lustre/fsw/core_dlfw_ci/ryan/velo` is on `w0-ingest-metrics`; `/lustre/fsw/core_dlfw_ci/ryan/velo-docs` is a worktree on `response-plane-docs`. Docs edited in the worktree are mirrored as untracked copies in the main tree so a reader there sees the current text.
- Gates: `.research/rig/check-w0-velo.sh` (velo: fmt, clippy all-features, the touched test targets, and in `full` mode etcd plus a JetStream nats-server and the whole suite with `--skip test_mpsc_local_drop_preserved_under_backpressure`), `.research/rig/check-w0-adapter.sh` (dyn-pin), `.research/rig/dbg/w0-precheck.sh` (single node at 8x64: worker ports, velo and TTFR series on every page, readiness), `.research/rig/failbefore-w0-velo.sh` (fail-before evidence by reverting one decision at a time). All green on 2026-09-04.
- Wheel: built from velo cd8c076 plus the dyn-pin rig-local mods (`VeloResponseHold::attach_metrics` puts velo's registry on every DistributedRuntime's `/metrics`; `work_handler_perf` registers per registry; `log_velo_metrics` widened to the `velo_` prefix). Rebuild after W0b lands.
- Rig: `t3-workers.sh` has `RIG_WORKER_METRICS` (default 1: `DYN_SYSTEM_PORT=9090+p` on the same line as `DYN_SELF_HOST_METADATA=0`, and a 2 s harvester into `prometheus/workers/proc<p>.txt`); `t3-frontend.sh` scrapes `/metrics` about once a second into `prometheus/timeseries.txt` (about 27 MB per rep); all four `rig_run_meta.json` writers carry `velo_sha`, `velo_dirty`, `dynpin_describe`. If the W0b agent flipped the harvest default to 0, flip it back to 1: three instrumented reps and one control showed no systematic perturbation.
- Analysis: `.research/analysis/w0/` (`w1_queue.py` to `w5_report.py`, `lib_w0.py`, 50 unit tests) and the parameterized join (`T3_RESULTS_ROOT`, `T3_OUT`; `extract.py <rep>` then `w5_report.py <rep_dir>`). The frontend log is ANSI-coloured; prefilters must not span the colon after `metrics`.
- Results: `t3-w0-probe` (rep1, the outlier: p99 4.6 s, hot process 1,887 slots), `t3-w0-probe2` (two clean instrumented reps), `t3-w0-ctrl-nowm` (worker harvest off), `t3-w0-probe-c2048` (concurrency 2048 discriminator). Join outputs under `.research/analysis/ttft-join/out-<tag>/`.
- Memory notes for future sessions: GitHub over https through `gh` (no SSH agent on the login node), the branch-base decision, the DCO sign-off, and the Slurm queue facts.

### Findings that changed the plan

See the evening addenda in `ttft-gap-diagnosis.md` and `velo-response-plane-win-plan.md`: the frontend FIFOs hold about 146 ms; the attach round trip is 524 ms mean and scales with the mocker process's live slots; worker egress backpressure is present at 8192 and absent at 2048. W3 and a transport-level W4 come first; W1 drops to last. `ingest-cost-ledger.md` records the W1 and W2 design research (W2 reduced to items (a) and (d)).

### Next, in order

1. Land W0b (workflow `wf_eee734d1-73e`): review its diff, commit with sign-off as the second commit on `w0-ingest-metrics`, rebuild the wheel, run one instrumented velo0 rep, run `w6_egress.py`, and record the egress split as a further addendum. Then `wills-mega-review` on PR #77.
2. W3 design and implementation (velo protocol plus the dyn-pin adapter), tests first, behind `DYN_VELO_RESPONSE_*` gates parsed with hard errors, arm `velo3`; then W4 at the transport writer, arm `velo4`, and `velo34`. Rig arm edits exactly as `mux18p` was added, with every existing arm explicitly unsetting the new gates.
3. W2 (a) and (d) as its own PR, arm `velo2`.
4. Matrix `win1`: `ARMS="tcp velo0 velo2 velo3 velo34 veloF mux18p" REPS=3`, then the verdict against the success bar, the benchmark doc, the results page (pass the artifact URL as `url`), and this handoff.

### Update 2026-09-04 (night): W0b landed, egress split measured, core-pinning experiment next

- PR #77 now has two signed-off commits: cd8c076 (inbound instruments) and 0160fa1 (egress instruments; velo-ext 0.5.0 to 0.5.1 with three defaulted `TransportObservability` methods, velo 0.11.1, pin and CLAUDE.md quote updated; `scripts/check-semver.sh` green for both crates via `.research/rig/check-semver-ctr.sh`, which must run with a target dir outside the repo or cargo sees two `velo-ext` manifests). `examples/Cargo.lock` is stale against the bump and deliberately untouched.
- The wheel in `.research/aiperf-venv` is built from the W0b code (the build log names HEAD cd8c076 with dirty files because the build ran before the commit; the code equals 0160fa1). `t3-w0b-probe` is the instrumented rep; `w6_egress.py` is the egress analysis; the worker-side harvest default is 1 again (four reps showed no systematic perturbation).
- Findings: see the night addendum in `ttft-gap-diagnosis.md`. Node A runs at 95% CPU with aiperf sharing it; the remaining latency is scheduler time, not a velo queue.
- In flight: W3 (`velo-w3` worktree, branch `w3-zero-rtt-attach`, workflow `wf_70982ebd-fb2`) and W4a (`velo-w4a`, branch `w4-async-open-ack`, workflow `wf_4119ef8a-1a1`), velo side only; their adapter halves and the rig arms follow in one stage against an integration branch. `.research/rig/check-tree-velo.sh` is the per-worktree gate (`VELO_TREE`, `VELO_TEST_TARGETS` semicolon-separated).
- Next: (1) core-pinning experiment (`RIG_PIN_CORES`, default off) with `velo0` and `mux18p`, one rep each; (2) commit W3 and W4a, open their draft PRs against `drain-credit-return`; (3) adapter and arm stage; (4) integration branch, wheel, smoke of each new arm, then the matrix.
