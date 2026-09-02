<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# A test on `main` hangs deterministically

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
