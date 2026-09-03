<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# A test on `main` hangs deterministically

> **Fixed 2026-09-02 — see "FIXED" at the end of this file.** Everything between here and there is the original report, kept as written. Its hypothesis was half right, and where it was wrong is the useful part.

Found 2026-09-01 while validating unrelated work. Reported here rather than fixed, because the fix is not obviously mine to choose and the cause is not yet established.

## The finding

`concurrent_streams_to_one_peer_share_the_batch_flow` (`lib/velo/tests/streaming/mux_negotiation/mod.rs:363`) **never completes** on a clean checkout of `main` at `19921ca`, with no local modifications.

Evidence, all on a tree with `git status --short lib/velo/src/` reporting zero files:

| run | outcome |
|---|---|
| the test alone, 900 s bound | killed at the bound, no output past the test name |
| the test alone, 300 s bound | killed at the bound |
| **the other 16 tests in the same file** | **16 passed in 0.89 s** |

So it is one test, not the fixture and not the suite. It is also not the harness's own 30 s `PATIENCE` timeout firing — that would produce `timed out collecting items` and a failure. The process sits there instead.

## What the test does

Six concurrent streams to **one** peer, 100 frames each, sent round-robin across the six senders, then all six finalized. Consumers collect concurrently through `join_all`. Config (`mux_negotiation/mod.rs:49`):

```rust
MuxConfig { enabled: true, initial_credit: 8, credit_sweep_interval: 1ms, .. }
```

Two things stand out about that configuration relative to the rest of the suite:

- **Six slots on one peer** is the highest per-peer slot concurrency anywhere in the mux tests. Most others use one or two.
- **`initial_credit: 8` against 100 frames per stream** means every stream exhausts its window roughly twelve times over, so the run depends entirely on credit being returned, repeatedly, while five other slots contend for the same peer state.

That combination points at credit return under multi-slot contention on a single peer. It is a hypothesis, not a diagnosis — I did not attach a debugger or capture a stack dump.

## Why this matters more than a skipped test

`.github/workflows/ci.yml:158` runs:

```
cargo test --locked --all-features --all-targets
```

`--all-features` includes `test-helpers`, which is this target's `required-features` (`lib/velo/Cargo.toml:170`). **So CI should be running this test.** Either it hangs there too and the job is being killed by a runner-level timeout, or something about the CI environment changes the interleaving enough to let it through. Both are worth knowing; the second would make this a flake that has been getting away with it.

## Reproducing

```bash
cargo test --features test-helpers --test streaming_mux_negotiation -- \
    --test-threads=1 concurrent_streams_to_one_peer_share_the_batch_flow
```

Always run it under `timeout`. It does not come back.

To confirm the rest of the file is healthy:

```bash
cargo test --features test-helpers --test streaming_mux_negotiation -- \
    --skip concurrent_streams_to_one_peer_share_the_batch_flow
```

## Not caused by the drain-driven credit work

This was found while validating `lib/velo/tests/streaming/mux_credit.rs` and the drain hook. The hang reproduces on a pristine tree with that work absent, which is how it was cleared. Recording that explicitly so the two are not conflated later: the drain change is a separate question, and its own tests pass.

## What the process looks like while hung

Running the test binary directly and inspecting `/proc` after 45 s:

```
State:   S (sleeping)
Threads: 3
  tid ...400: S futex_wait_queue
  tid ...402: S futex_wait_queue
  tid ...403: S futex_wait_queue
```

Three threads, all parked in `futex_wait`, nothing spinning. Two things follow.

**It is a deadlock, not a livelock or a slow test.** No thread is runnable; no CPU is being burned. Waiting longer will never help, which matches the 300 s and 900 s bounds both expiring with no further output.

**Three threads is the surprising part.** This is `#[tokio::test(flavor = "multi_thread")]`, which by default sizes its worker pool to the core count — 128 here. Three live threads means the runtime's workers are already gone, so the hang is very likely *after* the async body has finished or unwound, in the shutdown path, rather than inside the sends and collects. That shifts suspicion away from "consumers parked on credit that never returns" and toward teardown: the test drops six senders and their anchors, and `peer_batcher/slot_stream.rs`'s module docs describe exactly such a hazard — `finalize`, `detach` and `Drop` reach the inlet through a *synchronous* `flume::Sender::send`, which blocks when the channel is full, and under mux a starved slot's channel may never drain.

That is a stronger hypothesis than the earlier one, and the bisect below confirms it.

## Bisect: it is credit exhaustion, not stream count

Copying the test at other stream counts, on the same pristine tree:

| streams | `initial_credit` | outcome |
|---|---|---|
| 2 | 8 | ok, 0.05 s |
| 4 | 8 | ok, 0.06 s |
| 5 | 8 | ok, 0.06 s |
| **6** | **8** | **deadlock** |
| 8 | 8 | deadlock |
| **6** | **512** | **ok, 0.04 s** |

Two things fall out. It is a **cliff, not a slope** — everything that passes does so in about 50 ms, so nothing is merely getting slower. And the last row is decisive: the *same six streams* pass when the credit window is widened. **The stream count was never the variable; credit exhaustion was.** Six streams is simply where 100 frames each against a window of 8 first starves a slot for long enough.

## The mechanism

`slot_stream.rs` names it, in the module docs of the very code that is supposed to prevent it:

> `finalize`, `detach` and `Drop` reach the inlet through a **synchronous**
> `flume::Sender::send`, which blocks when the channel is full — and under mux a
> starved slot's channel would never drain, so the block would be permanent, on
> a runtime worker thread, from inside a `Drop` in async context.

The withheld queue exists to keep that from happening. The bisect says it does not fully succeed. The inlet channel is sized `slot_buffer_depth()` — `initial_credit + 1`, so **9** in this test (`messenger_mux/mod.rs:811`) — and widening exactly that number is what makes the hang go away.

This is consistent with the three-`futex_wait`-threads picture: the block is synchronous, on a worker thread, so the runtime cannot make progress or shut down.

## Why this is more than a test bug

A window of 8 is small, but **many streams to one peer is the shape the mux was built for**. `BATCHING.md`'s motivation is a decode engine holding 256-1024 requests against 4-16 frontends. Six is not a large number in that setting, and `initial_credit` is an operator-visible knob with no documented lower bound and no guard against picking one that deadlocks teardown. The default is 256, which is why this is not being hit in normal use — but nothing says 8 is invalid, and the test suite itself picks it.

The interesting question for whoever fixes it: should `finalize`/`detach`/`Drop` be reaching a bounded channel synchronously at all, or should the terminal path have a reserved slot the way the credit ledger reserves one for the terminal record?

## Next step

A stack would settle it, and `ptrace` is restricted on this host (`gdb -p` fails with `Inappropriate ioctl for device`; see `/proc/sys/kernel/yama/ptrace_scope`), so it has to be done somewhere with `ptrace` allowed, or from inside the process. Cheapest options, in order:

1. Build the test with `tokio_unstable` and `console-subscriber`, which names parked tasks without needing `ptrace`.
2. Run it in a container or on a node where `ptrace_scope` permits attaching, then `rust-gdb -p` and `thread apply all bt`.
3. Bisect the test body: cut it to two streams, then four, and see where it starts hanging. Cheap, needs no tooling, and would confirm or kill the multi-slot-contention theory on its own.

---

# FIXED, 2026-09-02

Fixed on branch `drain-credit-return-teardown-fix`, on top of `3eab5c9`. The test passes un-skipped, ten consecutive runs, and a new test beside it pins the invariant at the tightest window the protocol allows.

`ptrace` was still restricted, so the stack was taken from *inside* the process: the three synchronous sends were temporarily swapped for `send_timeout` plus `Backtrace::force_capture`, and the batcher grew a position marker and a plain-OS-thread watchdog. All of that instrumentation was thrown away before the commit; what it produced is below.

## What was actually wrong

Three facts, each measured rather than argued.

**1. `finalize` blocks on a full inlet, and the receiver is alive.**

```
DIAG [finalize] BLOCKED 3s len=9 cap=Some(9) disconnected=false receivers=1 thread=Some("tokio-rt-worker")
   0: velo::streaming::sender::diag_send
   1: velo::streaming::sender::StreamSender<T>::finalize   (sender.rs:347)
   2: concurrent_streams_to_one_peer_share_the_batch_flow  (mux_negotiation/mod.rs:390)
```

Nine of nine seats taken, nobody disconnected, and the blocked call is on a runtime worker. It never comes back — the same line reprints at 6 s, 9 s, and every three seconds to the end of the run.

**2. The batcher is idle, not stuck, and not parked on admission.**

```
DIAG [watchdog] loops=17 (+0) pos=1 polls=616      <- repeated forever
```

`pos=1` is the `tokio::select!` at the top of the run loop. `loops` is the iteration counter and `polls` counts `SlotStream::poll_next`. Both freeze. The batcher's last live iteration held **all six inlets empty** and ~84 records withheld per slot, which is exactly the healthy starved state: it had drained everything offered and was waiting for credit. A timeout probe wrapped around `writer.flush()` never fired once, so the "batcher parked on admission" branch of the earlier hypothesis is ruled out directly.

**3. Nothing on the runtime runs at all.**

Adding a 100 ms `tokio::time::sleep` arm to the batcher's select did not wake it. A `tokio::spawn`ed one-second heartbeat task never printed a single tick. That is not a lost wakeup — the executor itself has stopped. And then:

```
DIAG [runtime] flavor=MultiThread workers=1
```

`std::thread::available_parallelism()` returns `Ok(1)` on this host, verified with a standalone binary. `#[tokio::test(flavor = "multi_thread")]` leaves `worker_threads` at that default, so the test runtime has **one** worker. (`nproc` reports 128 and is not the number tokio uses.)

### The mechanism, in order

1. `initial_credit: 8` sizes each per-slot egress inlet `flume::bounded(9)`.
2. Six slots against 100 frames each exhaust credit. The batcher pulls every inlet record into the withheld queue — correctly — and parks waiting for credit to come back over the wire.
3. The producer's data path, `StreamSender::send`, is `async`. A full inlet parks the *task*, which is harmless and is how the two sides hand off.
4. `finalize` / `detach` / `Drop` used a **synchronous** `flume::Sender::send`. On a full inlet that blocks the *thread*.
5. That thread is the only worker. The batcher — the one thing that can drain the inlet and release the send — can never be polled again. The runtime cannot even shut down, because shutdown waits on the worker that is blocked, which is why the process hangs instead of failing.

## The hypothesis above was partly right

Right: the call site (`finalize`/`detach`/`Drop` reaching the inlet through a synchronous send), the resource (the `C + 1` inlet), and the variable (credit exhaustion is what fills it — the `6 streams @ 512` row is genuinely decisive).

Wrong in two places, and both matter for the fix:

- **"A starved slot's channel would never drain."** It drains fine. The batcher is willing and idle. What stops it is the blocking send itself, which starves the executor that would have released it. The causality is the other way round from what the module docs assumed.
- **"The `+ 1` reserve is insufficient."** The reserve was never the hole. The inlet was full of nine **data** records; the `+ 1` in `slot_buffer_depth` is a *credit-ledger* reservation on the receive side, and it never was a reservation of egress channel capacity. Nothing about a reserved terminal seat is wrong, but building one would have fixed this test and left the defect standing: *any* synchronous send on a bounded, task-drained channel from async context wedges a worker, terminal or not.

The one-worker runtime is what makes this deterministic here rather than a flake, and it answers the open question above about CI: with `W` workers you need `W` concurrent blocking terminal sends to wedge the executor, so a CI runner with many cores would usually get away with it. **That does not make it environmental.** A decode engine finalizing many starved streams at once is precisely `W` concurrent blocking sends, and it is the workload the mux exists for.

## The fix

`lib/velo/src/streaming/sender.rs` gains `send_terminal`, and `finalize`, `detach` and `Drop` all go through it:

1. `try_send`. Almost always succeeds, and nothing changes on that path.
2. `Disconnected` -> `SendError::ChannelClosed`, as before.
3. `Full` -> hand the bytes to a task that `await`s `send_async`. The record still arrives; what changes is that a *task* waits for the space instead of a *thread*.
4. No runtime under the calling thread -> block, as before. There is no worker to starve, and blocking is what the caller asked for.

Two details carry weight:

- **The task takes a clone of the sender.** That keeps the channel open until the sentinel is in it, so the receiver cannot see the inlet's EOF before the record that explains it.
- **`detach` clears the attachment flag from `on_delivered`, not inline.** A sender that re-attached before the deferred `Detached` landed would put its records ahead of the sentinel that ends the previous attachment, and frame order is the one thing a re-attach may not disturb. Until delivery a re-attach sees the anchor as still attached and can retry. On the fast path this is inline and identical to the old behaviour.

### Why this is structural

The invariant is now a property of the code path rather than of a number: **no terminal send blocks a thread that a runtime owns.** It holds at any `initial_credit`, at any stream count, on a runtime of any size, and it cannot be re-broken by tuning a buffer, because no buffer size appears in it. Contrast the shape it replaces, where safety depended on the inlet happening to have a free seat at the moment a producer finished.

The one remaining loss window is a runtime shutting down before the queued task runs. Nothing is owed then: the receiver is being torn down by the same shutdown, so no consumer is left to tell `Finalized` from `Dropped`, and the sender clone dies with the task so the inlet reaches EOF rather than hanging. `tokio::task::block_in_place` would close even that window, at the price of panicking on a `current_thread` runtime — a worse failure than the one it fixes.

### What proves it

`a_terminal_lands_even_when_its_inlet_is_full`, beside the original test. Eight streams at `initial_credit: 1` — the smallest window a peer may advertise, since zero is `NegotiationError::LegacyPeer` — so every inlet is two records deep and every `finalize` is offered to a full channel. It asserts each consumer sees all its items *and* its `Finalized`.

It is a real regression test, not a passing assertion: with the pre-fix blocking send restored behind a temporary env var, it hangs and is killed at the bound (exit 124); with the fix it passes in 0.07 s. That control was removed before the commit.

Gates, all green: the target test 10/10 consecutive under `timeout 120`, the new test 10/10, `streaming_mux_negotiation` 18/18 with nothing skipped, `streaming_mux_credit` 5/5, `velo` lib 724/724, `velo-ext` lib 57/57, `cargo fmt --check` clean, `cargo clippy --all-targets -- -D warnings` clean.

## A second instance of the same defect, left alone

`test_mpsc_local_drop_preserved_under_backpressure`
(`lib/velo/tests/streaming/mpsc_integration.rs:288`) hangs on this host for the same reason, in the MPSC sender rather than in `StreamSender`. `MpscSender`'s `Drop` sends through a synchronous `tx.send` at `lib/velo/src/streaming/mpsc/sender.rs:259`, the test fills the channel first, and the one-worker runtime then has nothing left to drain it.

It is **not** caused by this change and was not fixed here. Verified by running it alone with the fix stashed: it is killed at the bound either way (exit 124 both times).

Fixing it is a design decision rather than a repeat of this patch, because the test asserts the blocking behaviour as its contract:

```rust
assert!(
    !drop_task.is_finished(),
    "drop should block while the bounded channel is full"
);
```

Someone has to rule on whether an MPSC `Drop` may block a runtime worker. If the answer is no — and the argument above says it is — then `send_terminal` is the shape to reuse, and that assertion is the thing to replace.
