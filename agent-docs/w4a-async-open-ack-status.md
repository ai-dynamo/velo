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

## Addendum 2026-09-05: the size cap is gone

The follow-up above is done on `w8-control-map-bound`: `MAX_PENDING_CONTROL` is removed. `mine` refuses only a key whose index the batcher never allocated (the batcher publishes its allocation high-water mark to the inbox on every open); `peers` refuses nothing, being this side's own writes about slots its ingress holds. A peer with any number of live slots loses no grant, no close and no resolution; `velo_streaming_mux_control_refused_total` now means exactly "a peer named a slot that never existed here". Tests: a grant past the allocation bound is refused; 5,000 live slots lose no grant; 10,000 replies are never refused; a flood of bogus grants is refused while the admission answer still lifts the fence.

## Addendum 2026-09-05 (review pass): `peers` was not actually unbounded-safe

The addendum above overstated what landed. `open_slot` (`ingress/mod.rs`) pushes a close reply for a wire-supplied `SlotId` at three sites *before* any table lookup — an out-of-range index, a slot collision, and an `OpenSlot` for a bind that never existed or expired — so "`peers` refuses nothing, being this side's own writes about slots its ingress holds" was false for exactly those three: a peer could grow `peers` by one entry per bogus `OpenSlot`, unbounded, on the ingress task, while the batcher was parked on admission. Separately, `mine`'s bound was index-only while the map keys on the whole `SlotId` (index plus an 8-bit generation), so the real ceiling was the allocation high-water mark times 256, not "the indices in use".

Both are fixed on the same branch, by splitting each map along the property the original fix already relied on:

- `mine` now checks the *live* generation per index (`ControlState::live_generations`, published by `ControlInbox::note_allocated` on every open, including a reopen) rather than only the index. A grant for an index never allocated is refused and counted, as before; a grant for an index that was allocated but at a generation that has since retired — an ordinary close-then-reopen race — is dropped silently, matching what the pre-fix generation check at apply time already did, just enforced one hop earlier. The check only gates *future* writes, though — it does not evict the entry a reopen's predecessor generation already left in `mine` — so the map's real bound between two drains is one live entry per index plus one stale leftover per reopen since the last drain, up to the same 256-per-index ceiling the check exists to keep a bogus peer from reaching on its own.
- `peers` splits by whether the ingress ever held the slot the reply names. `collect_grants` and `fail_slot` name a slot in the ingress's own table and stay refused-never — but the table's own slot limit does not cap `peers`: a peer that closes and reopens the same index keeps minting a new key here, one per generation the ingress admitted for it since the last drain, up to 256 per index, because nothing removes an entry except `drain`. `open_slot`'s three outright-rejection sites now produce a distinct `ReplyRecord::RejectSlot` (identical wire frame to `CloseSlot`; the peer cannot tell them apart) that lands in a new capped sub-lane, `ControlState::rejects` (`MAX_PENDING_REJECTS = 8,192`), merged into `peers` at drain. Dropping a reject past the cap costs no credit — the sender's slot just keeps streaming into one the receiver already discarded until its own producer finishes — unlike a credit or close reply, where the receiver has already zeroed `ungranted` for the delta; that asymmetry is the reason it is safe to cap where a credit or close reply is not.

`velo_streaming_mux_control_refused_total` now counts both refusal reasons (never-allocated index, reject-lane cap) and does not count the ordinary stale-generation race. Tests: `a_stale_generation_is_dropped_and_never_credits_the_live_entry` (control.rs), `a_flood_of_bogus_open_rejections_is_capped` (control.rs), `a_bogus_open_slot_flood_reaches_control_as_a_bounded_reject` (`peer_batcher/tests/reject_lane.rs`, the ingress-to-control seam).
