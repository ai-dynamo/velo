<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# W7: two ways a staged credit reply can be lost, and why `reply_linger` widens one of them

Written 2026-09-05, PR #81 (`w7-reply-linger`), pass-4 review response.

This is where `FlushGate::discarded`'s enumeration moved to. `flush_gate.rs`
knows nothing about slots, credit or epochs by design (its module doc says
so), so a paragraph about `take_pending_grant`, `on_owned_control`'s drain
order, and `Batcher::flush`'s call order does not belong inside it — every
sentence below is a fact about a different file, and three review passes each
had to re-derive or re-correct it in place because nothing there could check
it. `discarded()` itself keeps two or three sentences and a pointer here.

## The credit is already unrecoverable once a reply record is minted

A `CreditUpdate` record's `ungranted` delta is zeroed at mint time, in
[`FlowControl::take_pending_grant`](../lib/velo/src/streaming/messenger_mux/flow_control/mod.rs)
(`self.ungranted = 0`), not when the batch carrying it is admitted. From that
point, the batch holding the record carries the *only* copy of that credit.
If the batch never reaches the wire, the credit is gone — nothing later
re-derives it at the same occupancy.

## Loss path 1: discarded within one control drain (pre-dates this PR, needs no window)

`on_control` applies `drained.peers` before `drained.mine`. A
`CreditUpdate` staged while applying a `peers` entry (via
`FlushGate::stage_reply`) is still sitting in the open batch when a `mine`
entry's failed singleton send calls `epoch_death`, which discards the batch
with no flush in between. This needs no reply window at all — it existed
before `reply_linger` did, with `stage_urgent` in `stage_reply`'s place.

## What `reply_linger` adds: reach, not a new mechanism

Before this PR, a credit reply was staged urgent and left the batcher's hands
by the end of the same wake, so the window in which loss path 1 could catch
it was one control drain. `reply_linger` lets a reply sit staged, unwritten,
across wakes for up to the configured window — so a discard *anywhere in that
interval*, not just in the same drain the reply arrived in, now loses it too.
The window widens an existing hole; it does not open a new one.

## Loss path 2: any batch that fails to write, windowed or not

Independent of `reply_linger`: a batch carrying credit replies that fails to
write loses them, whether it sat out the window or was flushed immediately.
[`Batcher::flush`](../lib/velo/src/streaming/messenger_mux/peer_batcher/mod.rs)
calls `FlushGate::cleared` *before* attempting the write, so if the write
then fails, the staged-records gauge already went down as if the batch had
succeeded — the failure is invisible to the gate. `discarded()` (reached
through `epoch_death` at the failure site) finds nothing staged by the time it
runs, and is a no-op there. Neither the gate nor the gauge marks a failed
write as the loss it is; this is read from the mint-time zeroing and
`Batcher::flush`'s statement order, not exercised by a dedicated test the way
loss path 1 is.

## Where `epoch_death` runs without a flush first

Of `epoch_death`'s three call sites, only `on_owned_control`'s reaches
`discarded()` with the open batch still holding whatever was staged (loss path
1, above). The other two discard nothing *new* at that call: `Batcher::flush`
calls it only after the write it just attempted already failed (loss path 2,
already accounted for by the time this call happens), and both calls to
`send_singleton` are preceded by a flush inside `emit_data`, the function that
calls `send_singleton`.

Outside `epoch_death`, the run loop has two exits that skip the per-wake flush
check and fall straight through to `teardown(true)`: cancellation, fired only
from `MuxCore::drop` (the whole mux going away, not a per-peer event, so the
credit is moot — nothing is left to send it to), and a closed open-slot
receiver, which cannot fire in production since the sole `Sender` lives inside
the same `Arc<BatcherHandle>` the task holds for its own lifetime. The
`stopping` branch (peer retirement) is not among these exits: it forces a
write before it tears down, so retirement loses no staged credit.

## What fixing this would need

Neither loss path is `flush_gate.rs`'s to close, and neither is in scope for
`w7-reply-linger`. A fix would re-post the pending grant on discard, or defer
zeroing `ungranted` until the batch is actually admitted rather than at mint
time. Pinned today by
`peer_batcher::tests::reply_linger::epoch_death_discards_a_staged_reply_and_the_credit_is_lost`,
which characterizes loss path 1 under a live `reply_linger` window and is
deliberately not a passing invariant — it is the hole, kept green so a change
to it is a deliberate decision rather than a silent regression.

## Addendum, 2026-09-11: loss path 1 is closed, loss path 2 is left open on purpose

The body above stands as the analysis. What changed is the answer to "what
fixing this would need", and only for loss path 1.

`Batcher` now keeps its own copy of the credit it encodes into the batch the
writer has open (`staged_credit`, one `(SlotId, u32)` per `CreditUpdate`).
`epoch_death` hands that copy back to the control state through
`ControlInbox::reply`, which is where the drained reply came from, so the next
batch re-advertises it. This is the first of the two remedies the body named —
re-post on discard — chosen over deferring the `ungranted` zeroing. The second remedy must carry
write-confirmation from the batcher back into `PeerIngress` per slot, across
the boundary `flush_gate.rs` states it keeps.

Why the hand-back is correct rather than a double grant: the slots this credit
belongs to are *ingress* slots, the peer's egress into us. `epoch_death`'s
`close_all` closes this side's *egress* slots, so the ingress slots outlive the
epoch and their sender is still waiting on a window nothing else re-derives —
`take_pending_grant` zeroed `ungranted` at mint time and no later reconcile
reaches the same occupancy.

**Loss path 2 stays open, deliberately.** `Batcher::flush` clears
`staged_credit` before it attempts the write, so a refused batch's credit is
not handed back. A transport that refused this batch will not take the one a
re-post rebuilds either. The re-post also wakes the task, so closing path 2
turns a permanently failing transport into an unbounded retry paced by
`reply_linger`. The batcher calls `mark_active` on every wake, so the idle
reaper never reaches it. Losing the credit of a peer whose writes are failing
costs nothing the failing epoch has not already cost. Spinning does. If that
judgement is revisited, bound the retry rather than remove the clear.

Two counters make both arms visible:
`velo_streaming_mux_credit_reposted_total` for credit handed back, and
`velo_streaming_mux_credit_lost_total` for a hand-back refused because the
batcher had already taken its last drain. The second must stay at zero.

Pinned by
`peer_batcher::tests::reply_linger::epoch_death_returns_the_credit_its_discarded_batch_carried`,
which replaces the characterization test the body names. That test asserted
the loss; it was removed rather than inverted in place, because what it pinned
no longer happens.
