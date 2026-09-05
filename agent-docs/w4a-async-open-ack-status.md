<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# `MuxConfig::async_open_ack` (PR #79) — measured, still not a win

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

This branch carries the fix as `8a8b001`: `entry_mine_owed`
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

## The rerun: `t3-iso2`, three reps, velo3 vs velo4a vs velo34

`t3-iso1`'s velo4a and velo34 reps predated both fixes above, and their tails,
live-slot counts and CPU were not clean because of the control-cap leak — their
TTFT numbers are reported above with that caveat. `t3-iso2` reran the same
shape (three reps, 512 workers, 8,192-way concurrency) with both fixes applied,
against `velo3` as the unflagged baseline (`velo0`'s peer from that matrix).
Results: `.research/results/t3-iso2/summary.jsonl`.

| | TTFT p50 (mean of 3 reps) | TTFT p95 (mean of 3 reps) | errors (per rep) |
|---|---|---|---|
| velo3 (baseline) | 69 ms | 132 ms | 0 / 0 / 0 |
| velo4a (flag on) | 79 ms | 212 ms | 0 / 0 / 0 |
| velo34 (flag on, stacked with W3) | 59 ms | 172 ms | 0 / 0 / 0 |

**The control-cap defect is fixed: zero errors across every arm and every
rep**, where `t3-iso1` had 16 on velo4a and errors on both velo34 reps.

**The latency verdict is unchanged.** p95 is worse than baseline in every rep
for both flagged arms — velo4a: 209/176/252 ms against velo3's 142/119/136 ms;
velo34: 225/140/150 ms against the same baseline — the same shape `t3-iso1`
measured, not narrowed by the unconditional-fence fix. p50 is mixed: velo34
(which stacks W3) comes in under baseline, velo4a alone does not. `t3-iso1`'s
own p50 delta (85→91 ms) does not repeat in the same direction here, which
given the per-rep spread on both sides (e.g. velo3's own p50 ranges 60–87 ms
across its three reps) reads as noise at this concurrency rather than a
reversal.

## Merge precondition

**Met.** The rerun landed and the fix is correct: it removes the error mode it
targeted without changing the flag's latency shape. `MuxConfig::async_open_ack`'s
doc and `BATCHING.md` describe that shape — a mechanism with a known-negative
p95 result at this concurrency — rather than asserting a benefit.

## Addendum, 2026-09-05 (pass-3 fixer)

The "second defect" fix above was applied inside the caller-shared
`fire_singleton` helper, so the `admission_state() != Admitted` skip covered
both `open_detached`'s `OpenSlot` dispatch and `send_singleton`'s unrelated,
always-on rendezvous-record path — silently changing the shipped default's
existing rendezvous behavior (an over-budget record no longer fenced its slot
when synchronously admitted), with no test on that arm and a rationale
("per-target FIFO already orders anything dispatched after it") that does not
hold for it: a rendezvous record's bytes are resolved by the receiver's
ordered dispatcher in a detached task before dispatch, so the sender's
admission order says nothing about the order the receiver applies it in.
Narrowed to `open_detached` alone (`fire_singleton` now takes a `FenceSkip`
argument); `send_singleton` is back to fencing unconditionally for its
non-terminal case, matching pre-PR behavior there. On a terminal it now
fences too, which the pre-PR code did not (base `send_singleton` fenced only
in the non-terminal `else` arm); that is inert here because the immediately
following `close_local` removes the slot's table entry before anything can
observe the fence. Test:
`a_synchronously_admitted_rendezvous_record_still_fences_its_slot`
(`peer_batcher/tests/egress.rs`). This does not revise the numbers above:
oversized records are rare in the measured workload and the `OpenSlot` fence
is what they are attributed to.

## Addendum, 2026-09-05 (pass-6 fixer): known-scoped follow-up, `batch_seq` inversion

`open_detached` and any later flush to the same peer are independently
scheduled tasks for as long as that peer's sends still take
`spawn_slow_path` (i.e. before `can_send_directly` registers it), so their
two `batch_seq` values can admit to the wire out of the order they were
issued in. Nothing about per-slot delivery order depends on `batch_seq` — the
fence is what orders a slot's own records — so this is not a correctness
defect; it inverts a *counter*. The receiver's gap meter
(`ingress/mod.rs`'s `note_batch_seq`, which only calls
`metrics.batch_seq_gap(gap)` and updates `state.last_batch_seq`) reads an
inverted pair as one batch that went missing and
`velo_streaming_mux_batch_seq_gaps_total` jumps by `u32::MAX` on the
wraparound. Latent, not observed: every peer in the response-plane rig is
registered well before its first stream opens, so the two tasks never race in
practice there. The fix is a per-class `batch_seq` — one counter per
registration state rather than one per peer — which is out of scope for this
PR and not yet filed as its own issue; this addendum is that follow-up's
record until it is.

## Addendum 2026-09-05: the size cap is gone

The follow-up above is done on `w8-control-map-bound`: `MAX_PENDING_CONTROL` is removed. `mine` refuses only a key whose index the batcher never allocated (the batcher publishes its allocation high-water mark to the inbox on every open); `peers` refuses nothing, being this side's own writes about slots its ingress holds. A peer with any number of live slots loses no grant, no close and no resolution; `velo_streaming_mux_control_refused_total` now means exactly "a peer named a slot that never existed here". Tests: a grant past the allocation bound is refused; 5,000 live slots lose no grant; 10,000 replies are never refused; a flood of bogus grants is refused while the admission answer still lifts the fence.

## Addendum 2026-09-05 (review pass): `peers` was not actually unbounded-safe

The addendum above overstated what landed. `open_slot` (`ingress/mod.rs`) pushes a close reply for a wire-supplied `SlotId` at three sites *before* any table lookup — an out-of-range index, a slot collision, and an `OpenSlot` for a bind that never existed or expired — so "`peers` refuses nothing, being this side's own writes about slots its ingress holds" was false for exactly those three: a peer could grow `peers` by one entry per bogus `OpenSlot`, unbounded, on the ingress task, while the batcher was parked on admission. Separately, `mine`'s bound was index-only while the map keys on the whole `SlotId` (index plus an 8-bit generation), so the real ceiling was the allocation high-water mark times 256, not "the indices in use".

Both are fixed on the same branch, by splitting each map along the property the original fix already relied on:

- `mine` now checks the *live* generation per index (`ControlState::live_generations`, published by `ControlInbox::note_allocated` on every open, including a reopen) rather than only the index. A grant for an index never allocated is refused and counted, as before; a grant for an index that was allocated but at a generation that has since retired — an ordinary close-then-reopen race — is dropped silently, matching what the pre-fix generation check at apply time already did, just enforced one hop earlier. The check only gates *future* writes, though — it does not evict the entry a reopen's predecessor generation already left in `mine` — so the map's real bound between two drains is one live entry per index plus one stale leftover per reopen since the last drain, up to the same 256-per-index ceiling the check exists to keep a bogus peer from reaching on its own.
- `peers` splits by whether the ingress ever held the slot the reply names. `collect_grants` and `fail_slot` name a slot in the ingress's own table and stay refused-never — but the table's own slot limit does not cap `peers`: a peer that closes and reopens the same index keeps minting a new key here, one per generation the ingress admitted for it since the last drain, up to 256 per index, because nothing removes an entry except `drain`. `open_slot`'s three outright-rejection sites now produce a distinct `ReplyRecord::RejectSlot` (identical wire frame to `CloseSlot`; the peer cannot tell them apart) that lands in a new capped sub-lane, `ControlState::rejects` (`MAX_PENDING_REJECTS = 8,192`), merged into `peers` at drain. Dropping a reject past the cap costs no credit — the sender's slot just keeps streaming into one the receiver already discarded until its own producer finishes — unlike a credit or close reply, where the receiver has already zeroed `ungranted` for the delta; that asymmetry is the reason it is safe to cap where a credit or close reply is not.

`velo_streaming_mux_control_refused_total` now counts both refusal reasons (never-allocated index, reject-lane cap) and does not count the ordinary stale-generation race. Tests: `a_stale_generation_is_dropped_and_never_credits_the_live_entry` (control.rs), `a_flood_of_bogus_open_rejections_is_capped` (control.rs), `a_bogus_open_slot_flood_reaches_control_as_a_bounded_reject` (`peer_batcher/tests/reject_lane.rs`, the ingress-to-control seam).
