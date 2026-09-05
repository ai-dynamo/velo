<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# `MuxConfig::async_open_ack` (PR #79) — measured, not yet a win

This branch (`w4-async-open-ack`) ships the flag; the only A/B measurement of it
was taken on the `integration/response-plane-wheel` branch, after this PR's
head (`b3b9988`) was already merged into that integration build. Neither the
numbers nor the defect they exposed had landed here. This file is that landing
spot; the full diagnosis (mechanism, per-mocker-process backlog, the rig
methodology) lives in `ttft-gap-diagnosis.md` and `velo-response-plane-win-plan.md`
on `integration/response-plane-wheel`, addenda dated 2026-09-05 — this is a
pointer and a summary, not a duplicate of that content.

## The measurement: `t3-iso1`, three reps, velo0 vs velo4a

Same build (`379240a`, which contains this PR's head) with
`DYN_VELO_RESPONSE_ASYNC_OPEN_ACK` as the only difference between the two arms.

| | TTFT p50 | TTFT p95 | TTFT p99 | errors |
|---|---|---|---|---|
| velo0 (flag off) | 85 ms | 188 ms | 791 ms | 0 / 0 / 0 |
| velo4a (flag on) | 91 ms | 227 ms | 820 ms | 16 / 0 / 0 |

p95 is worse in 3 of 3 reps (179→217, 199→235, 185→227 ms), against a
within-arm velo0 p95 spread of about 20 ms — this is not noise. p50 does not
improve. velo4a is the only arm besides velo34 (this flag stacked with W3) with
any request errors in the whole matrix; velo34 had 95 and 186 in its two error
reps.

**Verdict: the flag alone does not move first-token latency at this
concurrency, and the errors are a real defect, not noise.** It is not a merge
blocker for the mechanism (the awaited-ack default is unchanged and every
existing gate-off test still passes), but it is a blocker for the doc claiming
a latency win: `MuxConfig::async_open_ack`'s doc and `BATCHING.md` say what
this flag is *for* without saying what was measured when it was tried.

## The defect the errors traced to, and its status here

Every velo4a/velo34 error was one stream whose `OpenSlot` admission answer got
refused at the peer batcher's control inbox (`MAX_PENDING_CONTROL` = 4,096,
sized against roughly 1,024 live slots per peer; the rig's mocker processes
hold 4,000–6,700). A refused resolution leaves the slot fenced with no second
answer coming, so every record it ever queues sits withheld until the
consumer's heartbeat watchdog gives up 15 s later — the "500 Failed to
generate completions" the frontend logged.

This worktree carries the fix, uncommitted: `entry_mine_owed`
(`peer_batcher/control.rs`) exempts a singleton resolution from the cap by
keeping it in its own map (`ControlState::resolutions`), merged into `mine`
only at drain time. A first pass exempted it by inserting straight into `mine`
instead — same intent, wrong mechanism, since it shares `mine`'s cap headroom
with `entry_mine`'s ordinary grants and closes. A peer with more live slots
than `MAX_PENDING_CONTROL` generates that many resolutions too, and sharing one
map let the exemption alone push `mine` past the cap and refuse every grant
behind it — which is unrecoverable, because the receiver has already zeroed
the credit it sent by the time its `CreditUpdate` reached us. The separate map
removes that one contributor; it does not close the underlying gap.
`entry_mine`'s grants and closes still refuse once *their own* entries reach
the cap, and on a peer with more live slots than `MAX_PENDING_CONTROL` that is
the same legitimate-entries case above, not the bogus-id case the cap was
sized for — still real, still unrecoverable, and still open. A cap keyed to
live slots, or one that refuses only keys naming no live slot, is a follow-up
outside this PR. Tests:
`a_singleton_resolution_is_never_refused_at_the_cap`,
`resolutions_alone_must_not_exhaust_the_grant_lane` (`peer_batcher/control.rs`)
and `the_fence_lifts_when_the_admission_answers_into_a_full_control_map`
(`peer_batcher/tests/open_ack.rs`).

**A second defect in the same worktree, likely the p95 cause above**:
`fire_singleton` fenced every singleton unconditionally, without reading
`FireResult::admission_state()` — synchronous, and already `Admitted` for the
fast path `_stream_batch` takes on a registered peer. On an uncongested peer
that fence bought no order (the frame was already on the transport's send
channel) and instead made the first record of every stream wait for the
resolution round trip to land — the spawn, the control-inbox insert, the
`Notify` wake, the `release_withheld` pass, all of which run unconditionally
either way — before it could be staged, which is the TTFT path the flag
exists to shorten. Fixed by fencing only when
`admission_state() != Admitted`; ordering is unaffected because `Admitted`
means the frame already entered the target's FIFO admission gate, and the
batcher dispatches one record at a time so every later record for the same
slot necessarily enters behind it. Test: `an_admitted_open_slot_is_never_fenced`
(`peer_batcher/tests/open_ack.rs`).

## Merge precondition

The exemption has not been re-measured. `t3-iso1`'s velo4a and velo34 reps
predate it and their tails, live-slot counts and CPU are not clean because of
the leak this fixes — their TTFT p50 is reported above with that caveat, but
the error columns and everything downstream of them are not a clean read on
the flag. **A `t3-iso2` rerun of velo4a and velo34 against the same velo0 and
velo3 baseline is a precondition for calling this flag a win**, not merely
correct. Until that rerun lands, `MuxConfig::async_open_ack`'s doc and
`BATCHING.md` describe the mechanism and its known-negative p95 result rather
than asserting a benefit.
