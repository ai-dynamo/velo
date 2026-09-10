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

### Update 2026-09-04 (late night): pinning experiment

The first-token second was load-generator interference; see the late-night addenda in `ttft-gap-diagnosis.md` and the plan. `RIG_PIN_CORES` now defaults to 1 in `t3-frontend.sh` (frontend cpus 0-71, aiperf 72-143; `pin_cores` is written into `rig_run_meta.json`). `t3-base-pin` (tcp, velo0, mux18p, 3 reps) is running as the new scoreboard; `t3-pin2` checks a 48/96 split. Every number in `response-plane-benchmark-results.md`, the results page, and the plan scoreboard predates pinning and is superseded.

### Update 2026-09-05: pinned baseline is the scoreboard; W3 is PR #78

`t3-base-pin` (3 reps, pinned 72/72) is recorded in `response-plane-benchmark-results.md`; the plan carries the reset success bar. W3 velo side is committed as a2e7365 on `w3-zero-rtt-attach`, draft PR #78 against `drain-credit-return`. W4a is implemented on `w4-async-open-ack` (worktree `velo-w4a`) with a follow-up in flight (fence-aware close lane, velo 0.12.0 bump for the new public `MuxConfig` field, semver run). Next: commit W4a and open its PR; the adapter and rig-arm stage for both against an integration branch; `t3-rt32` tests a 32-worker frontend runtime as the precursor to any loom-rs (per-thread pinning) integration.

### Update 2026-09-05: runtime-sizing experiment (t3-rt32)

Frontend tokio runtime forced to 32 workers under the default 72/72 pinning, one rep each: velo0 3,055 req/s, TTFT p50 89 ms, p99 769 ms, CPU 5.48 ms/req (default runtime: 3,060 to 3,327 req/s, 57 to 85 ms, 3.4 to 4.0 ms/req); mux18p 2,824 req/s, 49 ms, 782 ms. A smaller runtime costs velo0 CPU and first-token time, so per-thread pinning with fewer threads (the loom-rs shape) is not a lever now; the frontend wants the parallelism. loom-rs stays a candidate only if a later profile shows migration or cache effects, and it would enter dynamo through `Runtime::from_handle` in the Python binding entry.

### Update 2026-09-05: integration branch carries W0 and W3

`integration/response-plane-wheel` in the main checkout = `drain-credit-return` + `w0-ingest-metrics` (dfc4948) + `w3-zero-rtt-attach` (ba68907; the `anchor.rs` conflict between W0's round-trip histogram and W3's shared attach tail was resolved so the histogram brackets the send only and the zero-RTT path observes nothing). Gated green in quick and full mode on the compute node (1,244 tests). W3's own branch was re-gated on an isolated target dir (`target-velo-w3`) and stayed green; `check-tree-velo.sh` now gives every worktree its own target dir because a shared one can let one tree's build satisfy another's freshness check. Worktree gitfiles are relative (`gitdir: ../velo/.git/worktrees/<name>`) so git resolves inside the container. In flight: the W3 adapter half and `velo3` arm (workflow `wf_59f67d30-bc2`: adapter, rig arms, wheel, smoke of velo0 and velo3, review, fix) and W4a's second fix round (`wf_08f86741-26e`: immediate producer disconnect at overflow kill with the wire close deferred, gate-off test arm, full gate on its own target dir).

### Update 2026-09-05: W4a committed, PR #79

`w4-async-open-ack` is committed as b3b9988 (signed off; velo 0.12.0 for the new public `MuxConfig::async_open_ack` field; semver green against the base) and open as draft PR #79 against `drain-credit-return`. Its second fix round separated the overflow kill's producer disconnect (immediate) from the wire `CloseSlot` (deferred behind the fence) and added default-config coverage. Merge it into `integration/response-plane-wheel` only after the W3 adapter-and-arms workflow (`wf_59f67d30-bc2`) finishes, because that stage builds velo and the wheel from the main tree. Then add `DYN_VELO_RESPONSE_ASYNC_OPEN_ACK`, arms `velo4a` and `velo34`, rebuild the wheel, smoke, and run the isolation matrix `ARMS="velo0 velo3 velo4a velo34 mux18p" REPS=3` under the default pinning.

### Update 2026-09-05: W3 adapter and arms landed (rig-local), W4a merging into the wheel branch

The W3 adapter half is in `.research/dyn-pin` (gate `DYN_VELO_RESPONSE_ZERO_RTT_ATTACH`, ticket in the envelope with `skip_serializing_if`, three-way open on the worker, every resolved field on the `velo response plane listening` line; `check-w0-adapter.sh` green, fail-before evidence in `.research/rig/failbefore-w3-adapter.sh`). The rig has arm `velo3`, `assert-zero-rtt.sh` (liveness-controlled, absolute-max based, `--gate-only` in the smoke), `dbg/arm-parity-check.sh` (mapping and validity cases), and the `summarize.py` length-descending rule with a real test. The wheel built during that stage predates the adapter fixes and must be rebuilt. `w4-async-open-ack` is being merged into `integration/response-plane-wheel` (conflicts resolved: version 0.12.0, pin `=0.5.1`, both appended test blocks in `messenger_mux/tests.rs`); the full gate is queued and the merge commits only if green. Next: the W4a adapter gate and arms `velo4a`, `velo34` (brief in the scratchpad `w34/BRIEF-adapter-arms-w4a.md`), rebuild, smoke all four velo arms, then `ARMS="velo0 velo3 velo4a velo34 mux18p" REPS=3`.

### Update 2026-09-05: W4a merged into the wheel branch

`integration/response-plane-wheel` = W0 + W3 + W4a (379240a; full gate green on the merged tree, 18 targeted binaries plus the whole suite). The W4a adapter gate and arms `velo4a`, `velo34` are in flight (workflow `wf_c9c6f3fb-bf3`: adapter, rig arms, wheel rebuild, four-arm smoke, review, fix). Next after it: `RIG_IMG=... ARMS="velo0 velo3 velo4a velo34 mux18p" REPS=3 bash .research/rig/t3-submit.sh iso1` (pinning is the default), then `w5_report.py` on the velo reps and the verdict against the reset bar.

### Update 2026-09-05: isolation matrix submitted

W4a adapter gate and arms landed (rig-local): `DYN_VELO_RESPONSE_ASYNC_OPEN_ACK`, arms `velo4a` and `velo34`, `assert-async-open-ack.sh` (configuration assertion on both roles, with a 9-case fixture harness), parity check extended. Wheel rebuilt from 379240a (job 2729211); four-arm smoke green with assertions. Matrix `t3-iso1`: `ARMS="velo0 velo3 velo4a velo34 mux18p" REPS=3`, pinned by default. Next: per-arm means against the bar in the plan addendum of 2026-09-05, `w5_report.py` on the velo reps, docs and page update, then `wills-mega-review` on PRs #77, #78, #79 (each from its own worktree, gates through `in-ctr.sh`).

### Update 2026-09-05 (afternoon): iso1 analyzed; W4a control-cap fix in the gate

`t3-iso1` (job 2729436) is recorded in the results doc, the plan (verdict addendum) and the diagnosis (afternoon addendum). Two findings. (1) The velo4a and velo34 HTTP 500s are a leak in `ControlInbox`: a refused `singleton_resolved` leaves a fenced slot fenced forever, reachable on every open under `async_open_ack` once a peer holds more than 4,096 live slots. The fix (`entry_mine_owed`) and two tests are on `velo-w4a`, uncommitted until the gate returns; `failbefore-w4a-cap.sh` (job 2729999) confirmed fail-before, pass-after pending. (2) The E2E and ITL tail is one mocker process's backlog in every arm and plane, so the E2E criterion is withdrawn. Next: commit the fix with `-s`, push PR #79, re-merge into `integration/response-plane-wheel`, rebuild the wheel (`build-wheel.sh`), smoke velo4a and velo34, rerun `ARMS="velo4a velo34" REPS=3` as `t3-iso2`. The mega-review workflow (`wf_92ee7ae2-e88`, task `w000qd17x`) over PRs #77, #78 and #79 is still running; its fixes go through the same gates. The results page still needs the iso1 table. Analysis queued: per-worker TTFT join on iso1 (`extract.py` and `w5_report.py` into `out-iso1`) to test whether TTFT p95 and p99 sit in the hot process.

### Update 2026-09-05 (late afternoon): per-request join on iso1; cap fix on the integration branch; jobs in flight

The join (`out-iso1`; `w1_queue.py` and `w3_worker.py` now read a missing attach series as zero under `zero_rtt_attach`, 108 analysis tests green) shows zero-RTT cuts the response leg to mux18p's level and the request leg grows by 20 ms, with the frontend sending ten times more control batches; recorded as section 3 of the diagnosis addendum and as W7 in the plan, ahead of W2. The control-cap fix is committed on `integration/response-plane-wheel` as b68d94b (ahead of the PR #79 update, which waits for the review workflow to release `velo-w4a`; the patch is in the scratchpad as `w34/cap-fix-backup.patch`). In flight: `failbefore-w4a-cap.sh` against the integration tree (`VELO_TREE=/work/velo`, label `w4a-cap-int`) and `build-wheel.sh` from b68d94b (label `wheel-cap`). Next: smoke velo4a and velo34 on the new wheel, then `ARMS="velo3 velo4a velo34" REPS=3` as `t3-iso2`; on the review workflow's return, land the cap fix on `w4-async-open-ack` with `-s`, push, and re-create the integration branch from the PR heads. `w5_report.py` fails on the errored rep3-velo34 (an empty TTFT field from a failed request reaches `float()`); the extractor should skip failed rows.

### Update 2026-09-05 (evening): control-cap fix proven on the integration tree; W7 instruments drafted

`failbefore-w4a-cap.sh` on the integration tree (job 2730177, `VELO_TREE=/work/velo`): fail-before clean for both new tests (the batcher test times out waiting for the fenced record), control cases pass, pass-after gate green. The wheel from b68d94b is building (label `wheel-cap`). W7's first step is drafted on worktree `velo-w7` (branch `w7-batcher-instruments` off `w0-ingest-metrics`): `velo_streaming_mux_records_sent_total{record_type}` and `velo_streaming_mux_batcher_wakes_total{source}` with two tests in `peer_batcher/tests/instruments.rs` and rows in `BATCHING.md`; its gate has not run yet (cold target dir `target-velo-w7`). Land order: gate W7, commit with `-s`, open a draft PR against `w0-ingest-metrics`, merge into the wheel branch after `t3-iso2`, one velo3 rep with the instruments, then the fix.

### Update 2026-09-05 (evening): W7 instruments are PR #80; wheel with the cap fix built; smoke running

`w7-batcher-instruments` (5713955, signed off; gate green: fmt, clippy, peer batcher 51, mux 23, observability 11, credit 5) is draft PR #80 against `w0-ingest-metrics`. The wheel from integration b68d94b (control-cap fix) built with provenance verified (job 2730185); `smoke-all.sh cap` with `ARMS='velo4a velo34'` is running (job 2730431). Next in order: `RIG_IMG=... REPS=3 ARMS="velo3 velo4a velo34" bash .research/rig/t3-submit.sh iso2`; when the review workflow returns, commit its fixes and the cap fix on the PR branches with `-s`, push, run the review loop again on PR #80, and re-create the integration branch from the PR heads (note the version split it will hit: #77 now bumps velo to 0.12.1 against main's 0.12.0, #79 sits at 0.12.0); then merge W7 into the wheel branch and run one velo3 rep to name the multiplying reply.

### Update 2026-09-05 (evening): segment medians for every iso1 rep; join handles failed requests

The join now runs on all fifteen iso1 reps (`out-iso1`); `a2_join.py` skips and counts requests with no first token (`skipped_failed`, unit test `test_a2_join_failed_rows.py`), so the errored velo34 reps join too. Three-rep segment means are in the diagnosis (section 3): zero-RTT adds about 9 ms to A and 8 ms to B in every rep while C falls to mux18p's level or below. `t3-iso2` is job 2730444 (queued on priority at submission).

### Update 2026-09-05 (evening): W7 counters folded into the iso2 wheel

`t3-iso2` (job 2730444) will not start before about 09:54, so the W7 counters go into the wheel it runs on: `integration/response-plane-wheel` is now f58de76 = b68d94b + `w7-batcher-instruments` (conflicts: `records.rs` keeps the rendezvous meter beside the sent-record counts; `tests/mod.rs` lists both modules; the detached open counts its `OpenSlot` after `dispatch_singleton`, a line W7's base lacks and PR #80 must gain when it rebases onto W4a). Sequence in flight: quick gate on the merged tree (label `int-w7-gate`), then `build-wheel.sh`, then `smoke-all.sh` for velo3 and velo34, all before the matrix starts. iso2 then carries `velo_streaming_mux_records_sent_total{record_type}` and `velo_streaming_mux_batcher_wakes_total{source}` on every rep, so the multiplying reply can be read from the velo3 reps without a separate run.

### Update 2026-09-05 (night): the multiplying reply is credit, the reply linger is PR #81, iso2 half done

`t3-iso2` rep 1 (velo3, on the wheel with PR #80's counters) shows the frontend's outbound is 100 percent `CreditUpdate` records at one batch per batcher wake (914,572 control wakes, 912,329 batches, 28.5 M records); the diagnosis section 3 now says so and names the mechanism (the coalescing window of an urgent reply is the previous flush's admission wait, which zero-RTT made zero). Fix: `w7-reply-linger` (221d049, draft PR #81 against `w7-batcher-instruments`): `MuxConfig::reply_linger`, 1 ms default, credit replies form a batch for the window; closes stay urgent. Gate green on its own target dir; fail-before clean on the rerun (job 2730827: both batcher discriminators fail with `push_reply` reverted, the zero-window and gate unit cases pass). Rig-local and uncommitted, waiting on iso2 to finish before the wheel changes: the adapter knob `DYN_VELO_RESPONSE_REPLY_LINGER_US` (parsed, threaded into `MuxConfig::reply_linger`, printed on the resolved-config line, two tests; the adapter gate needs the integration tree to carry PR #81 first) and arms `velo3n` (linger off) and `velo3f` (500 us data linger), with the unset discipline on every branch of every arm script, parity check green. iso2 rep 1: zero errors on all three arms, no refusals, no stuck withheld records; the first velo3 rep drew a six-process mocker backlog (ITL p50 10 ms, E2E p50 2.8 s, CPU 6.8 ms/req), so its first-token numbers are the mocker's, not the plane's. Next: when iso2 ends, merge PR #81 into the integration branch, gate the adapter (`check-w0-adapter.sh`), rebuild the wheel, smoke velo3 velo3n velo3f, and run `ARMS="velo3 velo3n velo3f velo34 mux18p" REPS=3` as `t3-iso3`.

### Update 2026-09-05 (night): CPU numbers corrected everywhere; iso2 read; PR #81 merged into the wheel branch; gates in flight

`summarize.py` read aiperf's naive local `start_time`/`end_time` as UTC and measured the capture's idle tail; every frontend CPU per request on record was about a third of the truth. Fixed (`_parse_local_ts` keeps the capture offset, `_parse_aiperf_time` reads naive strings in it; `dbg/test_summarize_cpu_window.py`), tables recomputed in the results doc's night addendum, the plan's CPU term restated (at or below mux18p's on the same matrix; base-pin mux18p 7.61, velo0 8.09, tcp 10.48). `t3-iso2` (job 2730444): zero errors, the cap fix holds under refusals; performance numbers not comparable (nodes, backlog draws, one double aiperf pass); recorded in the results doc. `integration/response-plane-wheel` is now 4afb407 = f58de76 + `w7-reply-linger` (conflicts: both new `MuxConfig` fields kept, the batcher builds its gate with the reply window). In flight: velo quick gate on the merged tree (`int-w7b-gate`) and the adapter gate with `DYN_VELO_RESPONSE_REPLY_LINGER_US` (`adapter-linger`). Next: wheel rebuild, `smoke-all.sh` for velo3 velo3n velo3f velo34, then `RIG_IMG=... REPS=3 ARMS="velo3 velo3n velo3f velo34 mux18p" bash .research/rig/t3-submit.sh iso3`; the results page is republished with the corrected CPU figures and a correction note.

### Update 2026-09-05 (night): gates green on the reply-linger wheel branch; wheel building

Integration 4afb407 (cap fix + PR #80 counters + PR #81 reply linger): velo quick gate green (job 2730908; peer batcher 70, mux 30, observability, negotiation, credit, flush, negotiation-integration), adapter gate green (job 2730909; the two `DYN_VELO_RESPONSE_REPLY_LINGER_US` tests and the whole `dynamo-runtime` suite, 675). `build-wheel.sh` is running (label `wheel-w7b`). Next in order: `ARMS='velo3 velo3n velo3f velo34' bash smoke-all.sh w7b`, then `RIG_IMG=... REPS=3 ARMS="velo3 velo3n velo3f velo34 mux18p" bash .research/rig/t3-submit.sh iso3`. The rig-local adapter knob and the arms are uncommitted (dyn-pin and `.research/rig` are rig-local by design); PR #81 is where the velo side lives.

### Update 2026-09-05 (night): reply-linger wheel smoked; iso3 submitted

Wheel from 4afb407 built (job 2730985). `smoke-all.sh w7b` (velo3, velo3n, velo3f, velo34): 256 of 256 on every arm, zero errors, gate assertions green, resolved-config lines show `reply_linger_us=1000` (0 on velo3n; `flush_interval_us=500` on velo3f). Already at smoke scale the counters show the linger working: the frontend sent 104 batches on velo3 against 1,440 on velo3n for the same 8,000 to 9,000 credit replies, and 102 on velo3f with only 3,035 replies (the workers' data linger makes bigger inbound batches, so fewer drain visits). `t3-iso3` is job 2731049: `ARMS="velo3 velo3n velo3f velo34 mux18p" REPS=3`, pinned, corrected CPU summary, PR #80 counters on every rep. Read it with `iso-table.py` (scratchpad), `w7_batches.py`, and the join (`out-iso3`); the bar is TTFT p50 and p99 at or below the same-matrix mux18p, throughput at or above 3,300 with spread at most 300, CPU at or below mux18p's, zero errors.

### Update 2026-09-05 (night): review loop launched on PRs #80 and #81

`wills-mega-review` over PRs #80 (`velo-w7`) and #81 (`velo-w7b`) is running as workflow `wf_0afcf4b7-613` (task `wl32umymu`), the same script as the loop over #77, #78 and #79 (`wf_92ee7ae2-e88`, still in its second pass). Those four worktrees are the workflows' to edit until they return; on return, commit each worktree's fixes with `-s`, push, re-create the integration branch from the PR heads, and only then add `human-review`. iso3 (job 2731049) is queued.

### Update 2026-09-05 (night): review loop over PRs #77, #78, #79 ran three passes; fixes committed; loop resumed

Workflow `wf_92ee7ae2-e88` finished its three passes per PR (18 agents); every third-pass review still had actionable findings, so the PRs are not tagged `human-review` yet. The worktrees' review fixes are committed and pushed: `w0-ingest-metrics` 882e8ca (velo 0.12.1 against main's 0.12.0; NATS and ZMQ outbound double count removed; egress families bound only for tcp and uds; MPSC attach recorded; shared EgressObserver; `agent-docs/w0-egress-instrumentation-cost.md`), `w3-zero-rtt-attach` d143506 (watchdog does not count an unclaimed pre-bind; detach clears the pre-bind; orphaned pump cancelled; `control/ticket.rs`), `w4-async-open-ack` 8a8b001 (the control-cap fix, its own commit) and 90d49cd (docs, vocabulary, the admission-failure test, `agent-docs/w4a-async-open-ack-status.md`). PR #77 and #79 bodies carry the review notes the fixers asked for. The loop is resumed with a cap of five passes (`resumeFromRunId` replays the three cached ones). Open decisions for the author: the stack's version split (0.12.1 on #77, 0.12.0 on #79, base 0.11.0) and a rebase onto main; the design questions listed in the two new agent-docs. The integration branch (4afb407) predates these commits and must be re-created from the PR heads before the next wheel; iso3 (job 2731049) runs on the 4afb407 wheel and is not affected.

### Update 2026-09-06: iso3 read; the reply linger is a fix, not a lever; review loops on all five PRs resumed

`t3-iso3` (job 2731049, nodes ptyche0196/0197): zero errors on fifteen reps; the reply linger cuts the frontend's outbound batches five-fold and 0.4 ms/req of CPU with no change in the per-request segments; the 500 us data linger (velo3f) cuts inbound batches eight-fold and 1 ms/req of CPU but adds about 10 ms to the request path; same-mode velo3 against mux18p is 55 to 62 against 48 to 49 ms at p50, CPU 9.76 against 7.48. Two mocker load modes decide everything else (one process holding 7,300 in flight, or two to three sharing it). Recorded in the results doc (addendum 2026-09-06), the plan (verdict addendum: profile the two frontends on one node pair before any W2 item; rig fixes are prerequisites), the diagnosis (section 4) and the page. PR branches: `w7-batcher-instruments` c8f322a and `w7-reply-linger` 0e50639 carry the review-pass fixes (records counted per batch through pre-bound counters; the wake counter counts select-loop wakes; the reply window is a property of the pending reply, so a reply is never held for good under Manual once data joins it); their loop (`wf_0afcf4b7-613`, task `wsvzdrbqw`) and the #77–79 loop (`wf_92ee7ae2-e88`, task `w2qxxo6ma`) are resumed for passes four and five. No PR is tagged `human-review` yet. The integration branch (4afb407) predates every review commit; re-create it from the PR heads (drain-credit-return, 882e8ca, d143506, 90d49cd, c8f322a, 0e50639) before the next wheel. The iso3 wheel's counters have the pre-review semantics (wakes counted per dispatched item), which is why `ctrl_wakes` equals batches sent on velo3n there.

### Update 2026-09-06: control-map bound (W8) in the gate; profile run queued behind the review loops

On the user's direction: `w8-control-map-bound` (worktree `velo-w8`, off `w4-async-open-ack` at 90d49cd) removes `MAX_PENDING_CONTROL`. `mine` refuses only a key whose index the batcher never allocated (the batcher calls `ControlInbox::note_allocated` on every open, so the bound is its own allocation high-water mark); `peers` refuses nothing (this side's own ingress writes it for slots it holds); resolutions keep their own lane. `velo_streaming_mux_control_refused_total` now means "a peer named a slot that never existed here". Tests: a grant past the bound is refused; 5,000 live slots lose no grant; 10,000 replies are never refused; a flood of bogus grants is refused while the admission answer still lifts the fence. `failbefore-w8-bound.sh` puts a 4,096 cap back and expects the two live-slot tests to fail; it and the gate are running (label `w8-bound`, cold target dir `target-velo-w8`). On green: commit with `-s`, push, open a draft PR against `w4-async-open-ack`, run the review loop on it. The frontend profile run (perf on both frontends on one node pair) is set up after the review loops on #77 through #81 return, per the same direction.

### Update 2026-09-06: control-map bound is PR #82

`w8-control-map-bound` fd1fde3 (signed off; gate green: fmt, clippy, peer batcher 65, mux 24, observability 7, credit 5, flush 3, negotiation 18; fail-before job `w8-bound`: with a 4,096 cap put back the two live-slot tests fail at 904 and 5,904 refusals) is draft PR #82 against `w4-async-open-ack`. Its review loop is launched (task from the `mega-review-pr-82` workflow). Three review loops now run: #77–79 (passes four and five), #80–81 (passes four and five), #82 (from pass one). When all three return: commit each worktree's fixes with `-s`, push, tag `human-review` only where the last pass reported nothing actionable, re-create the integration branch from the six PR heads, then set up the frontend profile run.

### Update 2026-09-06: PR #80/#81 loop finished five passes; #81 merged onto #80's tip

Workflow `wf_0afcf4b7-613` ran five review/fix passes per PR (30 agents). Committed: `w7-batcher-instruments` c8f322a then 532f450 (records counted per batch from the encoder's tally through pre-bound counters; the wake counter counts select-loop wakes; drift guard; the pass-five review's one medium, a tautological conservation assertion, is already replaced by a wire-total comparison in 532f450), `w7-reply-linger` 0e50639 then 886550f (the reply window is a property of the pending reply under every policy; two agent-docs record the credit-loss mechanism of a discarded batch and the iso3 measurement). Pass five still listed low documentation findings on both and one high on #81: its merge-base was #80's first commit while #80's tip had changed `push_reply`, so the stacked tree did not compile. Fixed by merging `w7-batcher-instruments` @ 532f450 into `w7-reply-linger` (three conflicts: the wake help string keeps the reply-window sentence; BATCHING.md takes the records row from #80 and the wakes row from #81; `push_reply` keeps the window and drops the per-record count); the merge gated green (label `w7b-merge-gate2`, after restoring the record kind on `push_reply` that the instruments tip had dropped) and is pushed as 1a80642. Residual low findings on #80 and #81 are documentation precision items; neither PR is tagged `human-review` yet. The same stale-base hazard applies to #82 (`w8-control-map-bound` off `w4-async-open-ack` @ 90d49cd) once the #77–79 loop commits more on `w4-async-open-ack`: merge the base in, never rebase (no force-push).

### Update 2026-09-06: PR #77–79 loop finished five passes; fixes committed; no PR tagged yet

Workflow `wf_92ee7ae2-e88` ran five review/fix passes per PR (30 agents). Committed and pushed: `w0-ingest-metrics` bc36be1 (velo is now 0.13.0 on this branch, what cargo-semver-checks asks for against origin/main given the stack's breaking commit; #79 and the W7 branches sit at 0.12.0, so the stack's version is an author decision before merge), `w3-zero-rtt-attach` 3680ef8 (an adopted pre-bind whose sender never delivers its OpenSlot is now reaped; zero-RTT setup records its own operation), `w4-async-open-ack` e641278 (docs and a test assertion). Every pass-five review still listed medium and low findings, none high: for #77 the inbound-record ordering on TCP and UDS and README wording; for #78 the Detached-arm spawn placement, control.rs crossing 1,000 lines (a split into its own PR), and the MuxConfig::enabled rollback claim in BATCHING.md; for #79 the deferred CloseSlot's driver and doc narration. The loops do not converge under this reviewer, so no PR carries `human-review`; the residuals are in the journals under `subagents/workflows/wf_92ee7ae2-e88/` and `wf_0afcf4b7-613/`. Next: when the #82 loop returns, merge `w4-async-open-ack` @ e641278 into `w8-control-map-bound` (its base moved), gate, push; re-create `integration/response-plane-wheel` from the PR heads (drain-credit-return, bc36be1, 3680ef8, e641278, w8's tip, 532f450, 1a80642), resolve the version split there, gate full, rebuild the wheel. The frontend profile run is being set up now (`dbg/perf-probe.sh` checks the image for perf, kernel limits and extension symbols).

### Update 2026-09-06: profile tooling on the rig

Rig-local, uncommitted by design (`.research/`): `t3-frontend.sh`'s CPU capture now follows aiperf (a stop file touched three seconds after aiperf exits, `CAPTURE_MAX` as the backstop) instead of a fixed count from the frontend's launch, so the corrected CPU column can never be "not captured" again; `RIG_PERF=1` runs `perf record -F 199 -g` on the frontend PID for `RIG_PERF_SECONDS` (30) starting `RIG_PERF_DELAY` (25 s) after aiperf launches, then writes `system/perf-flat.txt`, `system/perf-callers.txt` and `system/perf-folded.txt` (`dbg/perf-collapse.py` folds `perf script` output); `RIG_EXTRA_MOUNTS` (t3-matrix.sh) and `IN_CTR_EXTRA_MOUNTS` (in-ctr.sh) add pyxis mounts. perf is not in the image; the host has it at `/usr/lib/linux-nvidia-6.14-tools-6.14.0-1015/perf` (kernel allows profiling: perf_event_paranoid -1; the installed extension has a full .symtab). `dbg/perf-probe-libs.sh` is checking which of its shared libraries the image lacks; those get bind-mounted from the host's `/lib/aarch64-linux-gnu`. The profile run is then `RIG_IMG=... RIG_PERF=1 RIG_PERF_BIN=/opt/hostperf/perf RIG_EXTRA_MOUNTS="/usr/lib/linux-nvidia-6.14-tools-6.14.0-1015:/opt/hostperf:ro,<libs>" REPS=1 ARMS="velo3 mux18p" bash .research/rig/t3-submit.sh prof1`, read against `ingest-cost-ledger.md`.

### Update 2026-09-06: profile run submitted

The host's perf runs inside the image once its directory and five shared libraries are bind-mounted (`libelf`, `libdebuginfod`, `libdw`, `libslang`, `libtraceevent`, all from `/lib/aarch64-linux-gnu`; the mount list is in the scratchpad as `perf-mounts.txt` and in this note). `t3-prof1` is job 2734444: `RIG_PERF=1 RIG_PERF_BIN=/opt/hostperf/perf RIG_PERF_DELAY=45 RIG_PERF_SECONDS=30 REPS=1 ARMS="velo3 mux18p"`, on the 4afb407 wheel (cap fix, counters, reply linger). It writes `system/perf-flat.txt`, `system/perf-callers.txt` and `system/perf-folded.txt` per rep. Read the flat report first (by DSO and symbol, 0.3 percent floor), then the callers of the top velo symbols against `ingest-cost-ledger.md`, and compare against mux18p's frontend on the same node pair; the mocker draw does not matter for a CPU profile of the frontend's per-request path as long as both reps are in the same mode, so check the in-flight tables before comparing.

### Update 2026-09-06: frontend profiled; W2 has an order; nats-server pinned

`t3-prof2` (job 2734467; `RIG_PERF=1`, host perf bind-mounted; `t3-prof1` failed because `perf record -p` opens one event per thread per CPU, so the step now samples the frontend's pinned cores system-wide with a flat pass and a DWARF call-graph pass) profiled velo3 and mux18p on one node pair. Diagnosis section 5 and the plan addendum of 2026-09-06 carry the result: velo's plane-specific frontend costs are the per-batch slot walk in `handle_batch` (`collect_grants` calling `reconcile` and a flume `len()` lock on every slot of the peer per inbound batch, 2.1 percent of the cores), the per-frame `tokio::time::timeout` in `reader_pump` (timer-wheel lock contention, 1.6 percent), per-record channel delivery (0.9), and `set_active_anchor_gauge` per create and retire (0.5); together about 1.3 ms/req, half the CPU gap to mux18p. W2 is ordered (d) touched-slot reconcile, (a) interval watchdog, (c) atomic anchor count; W1 is off the list. Rig: `nats-server` and `etcd` are now launched under `taskset -c $RIG_AIPERF_CPUS` in `t3-frontend.sh` (they were unpinned and took 12 to 38 percent of the frontend's cores in the two profiled reps). Reports: `results/t3-prof2/rep*/system/perf-flat.txt`, `perf-by-dso.txt`, `perf-callers.txt`, `perf-folded.txt` (the `.data` files are 23 MB and 400 to 460 MB). Still open: the #82 review loop; then the integration branch rebuild from the six PR heads and the version decision.

### Update 2026-09-06 evening: #82 reviewed and pushed; W7 branches caught up; integration rebuilt

- **PR #82 (`w8-control-map-bound`)**: five review passes. Pass one found that `peers` had no bound at all: the ingress pushes a close reply for every `OpenSlot` it rejects, keyed by the id the peer put on the wire, so a bogus-open flood grew it one entry per record while the batcher was parked on admission; and that `mine`'s index-only bound let a peer pin 256 entries per index by naming generations. Both fixed on the branch: rejections travel as `ReplyRecord::RejectSlot` (same wire frame) into a lane capped at `MAX_PENDING_REJECTS`, merged into `peers` at drain; `mine` keeps the live generation per allocated index and drops other generations silently. Committed as b453d73, then `w4-async-open-ack` @ e641278 merged in (46a5ac5; five conflicts resolved by hand: status doc, metric help, SATURATION, the `async_open_ack` doc, the open_ack test). Gate green (job 2734555, with `lib-ingress` and `lib-peer-batcher` now in `check-tree-velo.sh`'s default list: 22 and 71 tests). Pushed; PR body rewritten. Residual, not taken: `drain` discards map capacity each cycle (needs a drain-cycle measurement first). No PR carries the `human-review` tag: no loop ever returned "no actionable findings".
- **W7 branches**: `w7-batcher-instruments` and `w7-reply-linger` lacked their base's tip (bc36be1, which carries velo 0.13.0 and velo-ext 0.5.1). Merged: 7cf4874 and 3f1fa2d, both clean. Gate running (`gate-w7-pair.sh`, one job, both trees); push after it is green.
- **Integration branch**: rebuilt by merging the reviewed tips into the old tip 4afb407 (kept as `integration/response-plane-wheel-pre-review`): w3 @ 3680ef8 clean, w8 @ 46a5ac5 (seven conflicts: the old cap fix against the new bound, `record_sent` calls against the per-batch tally), w7b @ 3f1fa2d (eight: versions, the reply-linger docs against the detached-open docs, the writer's singleton path). Now f25b0d8, workspace version 0.13.0 (the one that goes with the `=0.5.1` velo-ext pin; the per-PR version split is still the author's call). `gate-then-wheel.sh` is running the quick gate and, if green, the wheel build. Fmt runs in place in the gate, so diff the tree after the job before trusting the commit.
- **Rig**: `t3-frontend.sh` pins `etcd` and `nats-server` to aiperf's cores (`AUX_PIN`) under `RIG_PIN_CORES=1`; the pin block moved above the infrastructure step. Nothing has been measured with this yet.
- **Done since**: W7 gates green (job 2734570) and both branches pushed (7cf4874, 3f1fa2d). Integration gate green and the wheel rebuilt from f25b0d8 (job 2734571; `build-wheel-tree-20260905_160532.json`). `smoke-w8` (job 2734614): velo0, velo3, velo4a, velo34 each 256/256 with zero errors and their gate assertions passing. The rig's `nats-server`/`etcd` pinning was live for that smoke.
- **Next**: W2 (d) touched-slot reconcile as the first PR of the profile's order (branch off `w8-control-map-bound`, rig arm, failing test first); then a t3 matrix on this wheel for a bar verdict once the mocker backlog cap is in.

### Update 2026-09-06 night: W2 built; the first cut of (d) starved the workers; second cut in progress

- **Built and gated**: (d) touched-slot reconcile on `w2d-touched-slot-reconcile` (1f8c2d8, PR #83, base `w8-control-map-bound`) and (a) one pinned timer per stream in the reader pump, the mpsc pump and the messenger lane loop on `w2a-pump-timer-hoist` (844956d, PR #84, base `w3-zero-rtt-attach`). Both have fail-before scripts (`failbefore-w2d-reconcile.sh`, `failbefore-w2a-timer.sh`) and green quick gates. Merged into the integration branch (be8f6f4), wheel rebuilt, smoke green. A review loop over #83 and #84 is running.
- **The profile run with both (`t3-t3-prof3b`, job 2736065) exposed a defect in (d)'s ruling.** velo3 fell to 1,516 req/s, TTFT p50 331 ms, frontend CPU 15.85 ms/req; mux18p on the same nodes was normal (2,923 req/s, 44 ms, 9.7 ms/req). The workers' `velo_streaming_slot_credit_exhausted_total` went from 13 to about 20,500 per process: every stream sends about 260 data records against an initial credit of 256, so its last four records need a grant, and the grant used to ride the peer's next inbound batch (microseconds). The first cut left slots the batch did not touch to the drain doorbell, which is a per-peer, rate-limited, single-task walk; visits fell to about 80 per peer per second and the frontend's ordered lane oscillated between 9,000 and 305,000 batches per second with a 1.4 s mean wait. The frontend sent 2.35 million credit updates instead of 28.8 million. The profile itself confirms (a): the `Sleep` subtree under the reader pump is 0.15 percent (was 7.3), `flume::Shared::len` 0.14 (was 2.1); but the rep is not a valid CPU-per-request measurement because of the starvation. `set_active_anchor_gauge` is now the visible velo-only symbol (0.7 flat, 1.0 inclusive).
- **Second cut of (d)** (`w2d-slot-named-credit`, worktree `velo-w2d2`, brief `scratchpad/w2/BRIEF-w2d2-slot-named-credit.md`): the reader pump counts drains exactly in `DrainSignal` (an atomic per slot) and lists the slot on a per-peer bounded lane; `handle_batch` reconciles the touched slots plus the listed ones, so credit returns on the next batch again without the full walk; the doorbell visit reconciles listed slots only; the periodic tick keeps the full walk (now an atomic swap per slot, no channel lock). `frame_tx.len()` leaves the hot path. Implementer running.
- **Matrix `t3-t3-w2`** (job 2736061, velo3 and mux18p, three reps, the wheel with both first cuts) is running; its velo3 reps will show the starvation and its mux18p reps are the draw-matched controls for the next wheel. The draw classifier is `.research/analysis/draw/draw.py <matrix dir>` (holders from per-process first-response time, live-slot peaks for velo).
- **Rig note**: `t3-submit.sh` prefixes `t3-` to the tag, so `t3-w2` lands in `results/t3-t3-w2`. A profile run needs `RIG_PERF_BIN=/opt/hostperf/perf` with the mounts in `scratchpad/perf-mounts.txt`; the first `t3-prof3` submission lacked it and was cancelled.

### Update 2026-09-06 small hours: the second cut did not clear the collapse; isolating W2(a) from W2(d)

- **Second cut of (d)** committed on `w2d-slot-named-credit` (10a8e53; tests, fail-before `failbefore-w2d2-credit.sh`, gate green). Merged into the integration branch with the zero-RTT claim cell carrying both the slot id and the dirty lane (37242a9, 0ef7370, fmt 065c545), wheel rebuilt, and profiled (`t3-t3-prof4`, job 2737433): velo3 still collapsed (1,481 req/s, TTFT p50 479 ms, CPU 16 ms/req, worker credit exhaustion about 21,500 per process, frontend lane wait 1.6 s mean). Credit updates sent went 28.8M (pre-W2) to 3.0M; the frontend's batcher flushed a credit batch about every 20 ms per peer instead of every 2.4 ms; grants coalesce about 22 records each, so the reader pumps drain in bursts about 28 ms apart. The profile shows `flume::Shared::len` and the pump's `Sleep` subtree gone, and `AsyncSignal::fire` from `deliver` into `push_remote_task` up from 0.27 to 2.4 percent with parking_lot contention at 6.8 percent inclusive: pump wakes cross a runtime boundary through tokio's injection queue and now contend.
- **Not the rig**: `t3-t3-w2c-nopin` (job 2737873, `RIG_AUX_PIN=0`, etcd and nats-server unpinned as before 2026-09-06) collapsed the same way (293 ms, 1,363 req/s, credit exhaustion 22k, lane wait 1.9 s); mux18p was normal. The `RIG_AUX_PIN` knob stays in `t3-frontend.sh`.
- **Isolating the two W2 changes.** The rig now takes `RIG_VENV` (build-wheel, t3-frontend, t3-workers, smoke-arm) and a second venv `aiperf-venv-b` exists (a relocated copy). `integration/w2a-only` (f25b0d8 plus the timer hoist, d21e733, checked out in the main tree) builds into venv-b (job 2738016) and then runs `t3-w2a-only`. `integration/w2d-only` (f25b0d8 plus the first cut, 1b761e8, worktree `velo-w2donly`) is next, into venv-a, once the main tree is free. Whichever arm collapses names the culprit; if neither does alone, the interaction is the finding.
- **Review loop over #83 and #84 finished** (four passes each). #83's corrections are committed (5b65813, pushed); a fold agent is merging that branch into the second cut in `velo-w2d2` (gate `w2d-fold` queued). #84's fixes are uncommitted in `velo-w2a` (stamp `last_frame` after the forward so backpressure is not charged to the sender, a lower bound on the arm-count tests, a bounded await, a firing-timer test, a wider lane margin) and wait on gate `w2a-fb4` (job 2737789) before the commit. Open from the reviews: a shared re-arm helper across the three sites (design call), the un-biased lane select can abandon a queued item during router teardown (behaviour call), the mpsc pump's blocking `send_async` on sentinel injection (pre-existing).
- `t3-w2b` was cancelled: it would have measured the second cut with both changes, which prof4 already did.

### Update 2026-09-06 morning: the collapse mechanism, and one runtime for the frontend

Two read-only investigations (a collapse hunt with two investigators and a refuter per hypothesis, then a runtime-topology check with a refuter) settled the W2 collapse: see diagnosis section 7. The dynamo frontend runs two 72-worker tokio runtimes (dynamo's, holding the velo node; pyo3-async-runtimes', holding the HTTP handlers, the reader pumps and the adapter consumers), and every record's lane-to-pump wake is a `push_remote_task` on the second runtime's injection mutex. W2 removed the accidental pacing and the convoy starved runtime A's timers. Neither W2 change is wrong on its own; the discriminating wheels (`integration/w2a-only`, `integration/w2d-only`) were cancelled in favour of testing the fix. The adapter fix (`dyn-pin` `lib/bindings/python/rust/lib.rs`: `init_with_runtime(primary)` behind `has_existing_runtime`) is built into venv-a with the full W2 integration tree (065c545) and runs as `t3-w2-onert`. Also worth taking from the hunt: the pinned heartbeat timer now fires once per deadline per stream under traffic (harmless at 5 s, but the intent was "never fires under traffic"; a bounded re-arm from the receive arm restores it), and a starved slot's grant should be staged urgent rather than waiting the reply linger. Both go to PRs #84 and #83 respectively.

### Update 2026-09-06 midday: one runtime clears the collapse; velo3 at mux18p's first-token level

`t3-t3-w2-onert` (job 2741018; the W2 integration tree 065c545, the adapter initialising the pyo3 bridge with dynamo's runtime, thread count 154 instead of 242, no "already initialised" warning):

| arm | draw (holders) | req/s | TTFT p50 | p95 | p99 | ITL p99 | CPU ms/req | worker credit exhaustion |
|---|---|---|---|---|---|---|---|---|
| velo3 | 6 | 3,116 | 46.1 | 168 | 823 | 38.6 | 13.6 | 68 |
| mux18p | 1 | 3,014 | 44.6 | 157 | 827 | 75.8 | 10.4 | n/a |

One rep. The lane wait is back to 0.42 ms per batch. At a six-holder draw, velo3 before this ran 73 to 83 ms; mux18p at a one-holder draw is the same 44 to 48 it always was. CPU per request is the open question: both arms are higher than in iso3 (mux18p 7.5 then, 10.4 now), so the environment shifted with the pinning and runtime changes, and the velo3 to mux18p ratio (1.31) is what it was. A three-rep matrix (`t3-w2-onert3`, job 2741140) and a profile (`t3-prof5`, job 2741141) are queued. The 621 client-disconnect errors on velo3's frontend log are the same kind iso3 showed (186 to 284 per rep) and aiperf reported zero errors.

Open on the branches: PR #83 is the folded second cut (1567d2d, pushed, body rewritten); a fresh review pass is running. PR #84's tree carries uncommitted review fixes and one red test; the second cut of (a) (bounded re-arm from the receive arm, so the timer never fires under traffic) is being implemented in `velo-w2a` and will commit both. The starved-slot urgent grant is a follow-up for #83. The adapter change is rig-local (`dyn-pin`), uncommitted like the other adapter changes, and belongs upstream in dynamo's Python bindings.

### Update 2026-09-06 afternoon: three reps with one runtime; velo3 ahead at a matched draw

`t3-t3-w2-onert3` (results addendum): velo3 39.1 to 41.5 ms p50 at one- and two-holder draws against mux18p 45.6 to 46.1, 57.4 at a six-holder draw; zero errors; credit exhaustion 25 to 74; CPU 12.3 to 14.0 against 9.5 to 10.0. PR #84 now carries the timer's second cut (94030b6, 10231f9; never fires under traffic; pushed, body rewritten). The integration branch is 3834c9b (both second cuts) and is being gated and built into `aiperf-venv-b` (`int-w2c-gate-wheel`); venv-a holds the 065c545 wheel the three reps used. Queued: `t3-w2-wt32` (both arms, `RIG_FRONTEND_WORKER_THREADS=32`, venv-a). Running: the PR #83 review loop, the `t3-prof5` CPU attribution. The adapter's one-runtime change is uncommitted in `dyn-pin` like the other adapter changes; it needs an upstream PR against dynamo.

### Update 2026-09-06 evening: CPU attribution done; final matrices queued

Diagnosis section 8 and the plan addendum of this evening carry the one-runtime CPU partition (`t3-prof5`) and the 32-worker run. Queued on `aiperf-venv-b` (tree 3834c9b, both second cuts): `t3-final72` (job 2741447) and `t3-final32` (job 2741448, `RIG_FRONTEND_WORKER_THREADS=32`), three reps each of velo3 and mux18p. The PR #83 review loop is still running; its fixes, when it returns, are committed with `-s` and pushed like the others. Rig state: venv-a holds the 065c545 wheel plus the one-runtime adapter, venv-b holds 3834c9b plus the same adapter; both venvs are `RIG_VENV` targets. The `integration/w2a-only` and `integration/w2d-only` branches and the `velo-w2donly` worktree are leftovers of the isolation plan and can be deleted.

### Update 2026-09-06 night: final matrices on 3834c9b

`t3-t3-final72` and `t3-t3-final32` are in the results addendum of this evening. Verdict on the final tree with one runtime: velo3 ahead on p50 at a matched draw (39.3 to 43.5 against 45.7 to 46.4), p99 40 to 70 ms behind, CPU 1.9 to 2.9 ms/req above, zero errors, six reps each. 32 workers is not a lever: it trades 2 ms/req of CPU for velo's tails. Still running: the PR #83 review loop (pass 2 fixer); commit and push its fixes when it returns. The results page carries the headline and the conclusions at the top.

### Update 2026-09-06 night: all loops returned; state at rest

- **PRs**: #83 (`w2d-touched-slot-reconcile`, tip d384115: first cut, corrections, second cut, fold, review corrections; `DrainSignal` now lives in `ingress/drain.rs`), #84 (`w2a-pump-timer-hoist`, tip 10231f9: first cut, review corrections, never-fires second cut), #82 (46a5ac5), #80 (7cf4874), #81 (3f1fa2d), #77, #78, #79 unchanged. All drafts. No `human-review` tag anywhere: every loop ended with residual low or doc findings rather than a clean pass. The version split (0.11 to 0.13 across the stack) is the author's call.
- **Integration branch**: `integration/response-plane-wheel` is 94dc8eb: 3834c9b plus the #83 review corrections (the drain signal moved to `ingress/drain.rs`; the merge keeps the zero-RTT claim shape and the registry's test accessors), quick gate green (job 2742028). The wheel in `aiperf-venv-b` is 3834c9b and was not rebuilt for that doc-and-sweep commit. `integration/w2a-only`, `integration/w2d-only` and the worktree `velo-w2donly` are leftovers and can be deleted; `velo-w2d2` is a second checkout of #83's branch name and can go too.
- **Rig**: venv-a = 065c545 wheel, venv-b = 3834c9b wheel, both with the one-runtime adapter change (rig-local `dyn-pin`, uncommitted, belongs upstream in dynamo's Python bindings). Knobs added this session: `RIG_VENV`, `RIG_AUX_PIN`, `RIG_FRONTEND_WORKER_THREADS` (existing, now measured: keep 72). `analysis/draw/draw.py` classifies the backlog draw per rep.
- **Results page**: headline and conclusions at the top, then the measurements in order. Docs: diagnosis sections 6 to 8, plan addenda through the night of 2026-09-06, results addenda.
- **Next work, in the plan's order**: one wake per burst into the consumer (hyper flush coalescing), delete the reader pump hop, the surviving channel cost, the cancellation-token walks; the starved-slot urgent grant on #83; upstream the one-runtime fix and raise axum's shutdown-watch re-poll with dynamo.

### Update 2026-09-06, later: "one wake per burst" reframed; two implementers running

The per-record trace (diagnosis section 9) found no flush lever; the levers are the adapter's consumer hop and the per-record credit grants. Running: an implementer on `velo-w2e` (`w2e-grant-threshold`, brief `scratchpad/w2/BRIEF-w2e-grant-threshold.md`, will commit with `-s`, not push) and one on `dyn-pin` (`BRIEF-adapter-inline-stream.md`, uncommitted by rule, gate `check-w0-adapter.sh`). When both are green: push `w2e`, open its draft PR on `w2d-touched-slot-reconcile`, merge into the integration branch, rebuild the wheel into `aiperf-venv-b`, run `velo3 mux18p` three reps at 72 workers, and read `velo_streaming_mux_records_sent_total{credit_update}` on the frontend (expect about a hundredfold fewer) and `slot_credit_exhausted_total` on the workers (expect tens).

### Update 2026-09-06, later: W2(e) and the adapter's inline receiver are built

- **PR #85** (`w2e-grant-threshold`, 5b87d6b, base `w2d-touched-slot-reconcile`): one credit grant per half window; fail-before `failbefore-w2e-threshold.sh`; gate green (jobs 2742121, 2742124). Draft, pushed, not reviewed by a loop yet.
- **Adapter inline receiver** (rig-local `dyn-pin`, uncommitted): `VeloStreamReceiver` polled by the connection task replaces the consumer task and its mailbox; the discriminator (eight frames in the anchor visible without another task running) went 0 to 8; the whole dynamo-runtime suite passed (681) before and after (`.research/logs/adapter-inline/`). Two notes from the implementer: graceful shutdown no longer drains in-flight bodies (the setup task returns at the prologue; a `TaskTrackerToken` on the receiver would restore it), and `DYN_VELO_RESPONSE_BUFFER_CAPACITY` is documented in `environment_names.rs` but no longer read.
- **Next**: merge #85 into the integration branch, gate and build the wheel into venv-b (the adapter change rides along), run `velo3 mux18p` three reps at 72 workers and one profiled rep; read `records_sent_total{credit_update}` on the frontend (expect about 1/128 of the data records) and worker credit exhaustion (expect tens).

### Update 2026-09-06, late: #85 merged into the integration branch; gate, wheel and review loop in flight

- **Integration branch**: `integration/response-plane-wheel` is f4dccc6 (94dc8eb plus #85 at 5b87d6b; two conflicts resolved: the `drain_visit_floor` doc in `messenger_mux/mod.rs` and the `producer_worker` test field). The quick gate and the wheel build into `aiperf-venv-b` run as `int-w2e-gate-wheel` (job 2742212). The adapter's inline receiver rides along from `dyn-pin`.
- **Queued after a green wheel**: `t3-w2e72` (`velo3 mux18p`, three reps, 72 workers, venv-b) and `t3-prof6` (one profiled rep of both arms). Read on the frontend `velo_streaming_mux_records_sent_total{record_type="credit_update"}` (expect about 1/128 of the data records) and on the workers `velo_streaming_slot_credit_exhausted_total` (expect tens). Then update the results addendum, the results page and this file.
- **Running**: the review loop over PR #85 (fresh opus reviewer, sonnet fixer, up to three passes, worktree `velo-w2e`). Commit its fixes with `-s` and push when it returns. If it changes code, merge the new tip into the integration branch before the next wheel.
- **Cleanup done**: worktrees `velo-w2donly` and `velo-w2d2` removed; branches `integration/w2a-only` and `integration/w2d-only` deleted. `integration/response-plane-wheel-pre-review` (4afb407) stays as the backup ref.

### Update 2026-09-06, late: W2(e) measured; grants fell 19x, CPU did not move, the ITL tail got worse

- **`t3-t3-w2e72`** (wheel from f4dccc6 with the inline receiver, 72 workers, three reps each): velo3 TTFT p50 41.8 ms at a one-holder draw, 47.2 and 50.4 at two-holder draws, against mux18p 47.1 to 48.0 (all two-holder); CPU 12.4 to 13.2 against 10.3 to 10.6 ms/req; zero errors. Credit updates 2.8 to 3.1 million for 67.7 million records (1 per 21 to 25 records, was 1 per 1.2), frontend batches sent 7,800 to 10,200 (was 307,000 to 484,000). The 1/128 target was not reached because the 200 ms sweep grants every sub-threshold remainder and a stream lives about 2.7 s under the 8,192-way backlog, so the sweep visits each slot about 13 times.
- **The tail moved the wrong way.** ITL p99 68 and 76 ms in two reps against mux18p 40 and 42 on the same nodes (the third rep 44 against 35); ITL p50 3.5 ms in the heaviest-draw rep (1.5 to 1.7 elsewhere). Worker credit exhaustion, summed over the eight processes (the earlier per-rep figures were proc0 alone): 133, 1,431 and 835, against 368, 310 and 246 on `t3-final72`. The likely mechanism: with a half-window threshold the sender's usable window is between 128 and 256 records instead of 256, so a slow HTTP reader exhausts the sender sooner. Whether W2(e) or the inline receiver (which removed the 64-deep mailbox ahead of the body) causes it is being isolated.
- **`t3-t3-prof6`** (one profiled rep each): velo-only buckets 3.20 ms/req (was 3.71 on `t3-prof5`): pump 1.01, anchor 0.85, ingress 0.51 (was 0.65), adapter 0.49 (was 0.66), tcp 0.18, dispatch 0.14, batcher 0.01 (was 0.12). The aggregate did not move (13.5 ms/req at a three-holder draw); the axum shutdown-watch bucket was 1.89 on velo3 against 0.43 on mux18p in this rep (it was 1.36 against 2.21 on `t3-prof5`; it swings with the draw). Grants were not a CPU lever: the batcher bucket was already 0.12.
- **Rig finding**: both venvs load the same editable `_core.abi3.so` from `dyn-pin` (`ai_dynamo_runtime.pth`), so `RIG_VENV` never selected a wheel; every matrix ran the last build, which matches what each was meant to run. A/B across trees is sequential from now on.
- **In flight**: `integration/no-w2e` (94dc8eb, the integration tip without #85) is checked out in the main tree and building (`now2e-wheel`); then `t3-now2e72` (three reps, both arms) runs on it, then the main tree goes back to `integration/response-plane-wheel`. If the ITL tail and exhaustion return to the `t3-final72` levels, #85 is the cause and its threshold needs to shrink (a quarter window, as mux18p's 64 KiB of 256 KiB) or the PR closes. The PR #85 review loop is still running.

### Update 2026-09-06, late: priority ruling

The author ruled that CPU per request is a bonus, not a bar: cut it only when latency and tails do not pay for it (plan addendum of this night). PR #85 and the inline receiver are judged on `t3-now2e72` (running, job 2742370, tree 94dc8eb in the main tree until the job ends, then back to `integration/response-plane-wheel`). Whichever owns the ITL tail is dropped; #85 closes with the measurement if it is the one.

### Update 2026-09-06 night: W2(e) closed on the isolation matrix

- **`t3-now2e72`** (94dc8eb with the inline receiver, same nodes as `t3-w2e72`): exhaustion 276 to 377 per rep, ITL p99 inside mux18p's range, CPU unchanged. PR #85 owns the tail and bought no CPU: closed with the measurement in a comment. The review loop over #85 was stopped.
- **Integration branch**: `integration/response-plane-wheel` is f4dccc6 plus a signed-off revert of the #85 merge, so its content is 94dc8eb. The installed wheel (both venvs share one editable extension, see the earlier update) is 94dc8eb with the inline receiver. `integration/no-w2e` (94dc8eb) can be deleted.
- **Adapter**: the inline receiver stays in `dyn-pin` (uncommitted, with the one-runtime fix); both belong upstream in dynamo. The graceful-shutdown note and the unread `DYN_VELO_RESPONSE_BUFFER_CAPACITY` still apply.
- **Next**: the starved-slot urgent grant on #83, then TTFT p99 at 72 workers. Every CPU change gets a same-matrix tail check (plan addendum of this night).
- **Worktree `velo-w2e`** holds the review loop's uncommitted pass-1 and pass-2 edits (`BATCHING.md`, `ingress/slot.rs`, `ingress/tests.rs`, `messenger_mux/mod.rs`, `peer_batcher/flush_gate.rs`). The loop was stopped in pass 3 when #85 closed. The reviewer's pass-1 finding matched the measurement: the no-stall argument missed the in-flight records and the anchor channel's 256 cap, so a sender can wait on the threshold. Nothing there is needed; the worktree and the branch can go once the user agrees.
- **Results page** republished with the headline, the W2(e) section and the re-ordered plan.

### Update 2026-09-06 night: the urgent grant sized from the per-token data and not built

Three read-only readers (receiver grant path, sender stall path, aiperf per-token gaps) fed `scratchpad/w2/BRIEF-w9-urgent-grant.md`. Ruling: not built (plan and results addenda of this night). The analysis script is `.research/analysis/itl/analyze_itl.py <matrix dir>`. The `velo-ug` worktree and `w9-urgent-grant` branch were created and removed. Next: decompose first-token p99 per request at a matched draw on `t3-now2e72` before proposing a mechanism.

### Update 2026-09-10: first-token p99 decomposed; the bar is met in steady state

`a9_tail.py` (in `.research/analysis/ttft-join`, joined data under `out-now2e72`, output `tail-now2e72.txt`) shows the reported TTFT p99 is the 8,192-request burst that opens the profiling phase (every p99 request in every rep started within 0.2 s; the excess is B, 400 to 593 ms, in both arms). Steady-state p99 (started 10 s or later): velo3 181, 171, 265 against mux18p 229, 308, 158; ahead at both matched draws. Results addendum, diagnosis section 10 and plan addendum of this date carry it. `summarize.py` now emits `ttft_ss_p50_ms`, `ttft_ss_p90_ms`, `ttft_ss_p99_ms` and `ttft_ss_count` (profiling-phase requests started 10 s or later; checked against `a9_tail.py` on two reps, identical), and the scratchpad read script prints them. Next matrices carry the columns; every p90 or p99 verdict reads them. Then the optional items in the plan addendum.

### Update 2026-09-10: summary document and PR #76 message

`agent-docs/response-plane-summary.md` is the entry point: the current result, the PR stack, the harness map, and the upstream list, with no history. The results page carries the same content. PR #76's title and body now match it (`scratchpad/PR76-body.md` in the session scratchpad). The upstream map came from two read-only readers over `.research/dyn-pin` (`wf_9444f7ea-3a5`): the velo adapter module and the mux18p port are untracked there, so the whole adapter is new against dynamo's response-plane branch.

### Update 2026-09-10: the stack is being consolidated into one PR

The author asked for the stack to land as one PR unless a logical break point says otherwise. Branch `response-plane` (worktree `velo-rp`) is `main` at a15f52d merged with `integration/response-plane-wheel` (the tested content). Eight files conflicted: the manifests and lockfiles, `peer_batcher/tests/{support,flush_policy}.rs` (main's #74 counters onto the stack's `StalledHarness`) and `transports/ucx/{transport,worker}.rs` (main's RDMA state plus the stack's observability slot on `WorkerShared`). A workflow (`wf_d6124c2c-e1f`) resolves them, runs the full gate and the semver gate on a compute node, commits the merge signed off, and verifies it with a fresh reader. Then: push, open one draft PR against `main` (body in `scratchpad/PR-response-plane-body.md`), close #77 to #84 with a pointer, and update the summary. The base branch alone merges into `main` with only manifest conflicts, so splitting it out as a first PR stays a ten-minute option.

### Update 2026-09-10: one PR, #86

`response-plane` (5b2986a, worktree `velo-rp`) is pushed and is draft PR #86 against `main`. #77 to #84 are closed with a pointer. Gate on the merge commit: fmt and clippy clean, targeted 374 green, semver green against `origin/main`, full suite 1,472 passed and 5 failed, all five `main`'s UCX idle-endpoint reaper tests (#69) at 72 test threads (job 2785863; diagnostic job 2785995: pass serially and at 8 threads, fail identically with `main`'s pristine UCX files in the merged tree). `check-main-ucx-parallel.sh` runs the same tests on a pristine `main` checkout (`velo-main` worktree) to settle it; the PR body carries the result. Rig finding: a worktree's `.git` pointer is host-absolute, so git inside the container fails for worktrees (`head=unknown` in gate logs; `check-semver-ctr.sh` needs `BASE_REF=origin/main` and a relative pointer). Fix in the rig scripts, not done yet. The integration branch, the closed PR branches and their worktrees can go once #86 merges.
- `main` alone (a15f52d, pristine `velo-main` checkout, job 2786677, log `inctr-main-ucx-par-2485.log`) fails the same five UCX reaper tests at 72 threads (37 passed, 5 failed; whole lib suite 917 passed, 4 failed) and passes at 8 and 1 threads. They are `main`'s. PR #86's body says so. The DCO check on #86 is red: the six base-branch commits of 2026-09-02 have no sign-off. `response-plane-squash` (b237103, worktree `velo-rp-sq`) is the same tree as one signed-off commit; moving #86 onto it is a force-push and waits for the author's word.
- Review pass 1 on #86 (fable reviewer and fixer, the author's ruling for this session; the earlier opus pass's eight findings were adjudicated too): nine signed-off commits on `response-plane`, tip 3c07bee, pushed. Quick gate green (`inctr-rp-review-fix-11895.log`); a full-suite run on 3c07bee is `rp-full2`; review pass 2 runs with its fixer waiting for that gate. `response-plane-squash` is rebuilt as 36b3dd9 (identical tree, one signed-off commit). The velo-ext 0.5.1 bump is correct under cargo's 0.y.z reading and the semver gate. The file split (3c07bee) is the last commit and can move to its own PR if the author prefers.
- Full suite on 3c07bee (`rp-full2`, job 2788763, log `inctr-rp-full2-559.log`, step log `velo-rp-gate/test-full.log`): fmt and clippy clean, 1,758 passed, 4 failed, all four `main`'s UCX reaper tests at 72 threads. PR #86's body carries it. Review pass 2 (fable) is running (`wf_c76c92ff-58d`).
- Review pass 2 (fable): one medium defect at a seam (a zero-RTT prompt close lost to the batcher-eviction sweep) fixed test-first as 2307d37 (`ControlInbox::close` on the last drain; fail-before log `inctr-rp-fix2-defect-4373.log`; gate `inctr-rp-review-fix2-30262.log` green), pushed. `response-plane-squash` rebuilt on it. Pass 3 (fable) runs on 2307d37; the rule is a clean pass before the tag.
- Review pass 3 (fable): one medium (a zero `credit_sweep_interval` built a mux whose sweep task panicked; now refused at build) and a teardown order (a cancelled batcher unregisters before its inbox closes; 64-attempt race test), plus a mux-level re-post test and three doc items. The fixer agent ran out before committing; the tree was read, gated green (`inctr-rp-review-fix3-16245.log`; fail-before `inctr-rp-review-fix3-pre-28180.log`) and committed as 5e137c6, pushed. `response-plane-squash` rebuilt as ed4676e. Pass 4 (fable) runs on 5e137c6.
- Review pass 4 (fable): nothing structural; a test synchronisation (f4af960) and two doc drifts (7ca3ec5), gate green (`inctr-rp-review-fix4-26532.log`), pushed. `response-plane-squash` rebuilt on 7ca3ec5. Pass 5 is the confirmation pass.
