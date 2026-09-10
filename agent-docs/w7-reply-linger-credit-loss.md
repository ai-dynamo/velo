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
