// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The per-slot drain signal: what a reader pump counts and posts against,
//! on its own task, without ever taking the peer mutex.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use velo_ext::WorkerId;

/// Told when the consumer takes a record out of the buffer credit is issued
/// against, so credit comes back by draining instead of by a timer.
///
/// `BATCHING.md` § P8 specifies this: `reader_pump` "gains an
/// `Option<CreditReturn>` and calls `credit.release(1)` after each successful
/// handoff to `frame_tx` — exact, O(1), and immediate", leaving the sweep to
/// reclaim only for slots whose pump died. Two halves of that landed and one
/// did not, deliberately.
///
/// **The pump counts, and it names its slot.** `drained` is the exact number
/// of records this slot's pump has taken out of the buffer since the last
/// reconcile, and `listed` says whether the slot is already on its peer's
/// dirty lane waiting for one. That is what lets a reconcile be exact without
/// reading the slot channel's length — a read that takes that channel's lock,
/// which is what made the arrival path's whole-table walk expensive enough to
/// narrow in the first place.
///
/// **The pump does not release credit.** Releasing needs the peer's mutex —
/// the same one the inbound batch path takes — and taking it per record would
/// trade a periodic cost for a worse per-record one. Two paths each releasing
/// an amount for one drained record would also double-count, and the periodic
/// sweep is still there. So the pump posts and the reconcile decides, which
/// keeps every visit idempotent: a redundant one recomputes zero.
///
/// The peer is not known when `bind` creates this: a bind belongs to whoever
/// claims it, and the claim arrives later as an `OpenSlot`. Until then the
/// signal is inert, which is correct — nothing has been delivered, so nothing
/// has drained.
pub(crate) struct DrainSignal {
    /// Whose bind this turned out to be, and where its drains are posted. All
    /// of it arrives together when an `OpenSlot` claims the bind.
    claim: OnceLock<SlotClaim>,
    /// Records this slot's pump has taken out of the buffer since the last
    /// [`IngressSlot::reconcile`](super::slot::IngressSlot::reconcile) swapped
    /// it to zero.
    drained: AtomicU32,
    /// Whether this slot's index is already sitting on the peer's dirty lane.
    listed: AtomicBool,
    wake: flume::Sender<WorkerId>,
}

/// What an `OpenSlot` tells a bind's [`DrainSignal`] when it claims it.
struct SlotClaim {
    peer: WorkerId,
    /// The peer's "a credit-return visit is already queued" flag.
    pending: Arc<AtomicBool>,
    /// The peer's dirty-slot lane, for naming this slot as having drained.
    lane: flume::Sender<u32>,
    /// This slot's index in the peer's table.
    index: u32,
}

impl DrainSignal {
    pub(crate) fn new(wake: flume::Sender<WorkerId>) -> Self {
        Self {
            claim: OnceLock::new(),
            drained: AtomicU32::new(0),
            listed: AtomicBool::new(false),
            wake,
        }
    }

    /// Name the peer this bind turned out to belong to, its slot index, that
    /// peer's dirty lane and its pending-wake flag. Called once, when an
    /// `OpenSlot` claims the bind.
    pub(crate) fn claimed_by(
        &self,
        peer: WorkerId,
        pending: Arc<AtomicBool>,
        lane: flume::Sender<u32>,
        index: u32,
    ) {
        let _ = self.claim.set(SlotClaim {
            peer,
            pending,
            lane,
            index,
        });
    }

    /// One record left the buffer: count it, name the slot, ring the doorbell.
    ///
    /// The count comes first and is unconditional, because it is the only
    /// record of the drain that survives — the listing and the wake are both
    /// best-effort hints about *when* to look, and a reconcile that arrives by
    /// any route reads the same number.
    ///
    /// The listing is per *slot* and the wake is per *peer*, which is the
    /// granularity each does its work at: one lane entry is all a reconcile
    /// needs to find this slot, and one wake is all the sweep task needs to
    /// come and drain the lane. `listed` and the peer's `pending` flag are the
    /// two coalescers, and each is taken down by the visit it summoned.
    ///
    /// `try_send` rather than an await on both: this runs on the pump's task,
    /// in the path of every frame, and must never park it. **A full lane puts
    /// `listed` back down**, and a full wake lane puts `pending` back down, for
    /// the same reason: leaving either up claims a visit is coming when none
    /// is, and every later drain would coalesce into something that was
    /// dropped. Clearing costs this one drain its hint and lets the next one
    /// try again; the periodic sweep's whole-table walk is what bounds the gap
    /// if no next one comes, and the count is still there when it arrives.
    ///
    /// A per-slot record threshold was the alternative to the wake and is
    /// worse on both counts: it withholds credit for the first `T` records of
    /// every slot, which is latency on the path this change exists to speed up,
    /// and with a thousand slots on one peer it still posts a thousand times.
    ///
    /// Both flag updates below are RMWs (`swap`), never a load followed by a
    /// conditional swap: a plain `listed.load` could return a stale `true`
    /// while a concurrent [`take_drained`](Self::take_drained) has already
    /// cleared it but not yet finished swapping the count out, and a pump that
    /// trusts that stale read declines to list — stranding this drain's count
    /// until the periodic walk finds it, which is the tail-of-stream stall
    /// this whole mechanism exists to remove. An RMW has no such window: it is
    /// guaranteed to observe the value immediately preceding it in `listed`'s
    /// own modification order, so it always sees a concurrent clear. The same
    /// argument is why `claim.pending.swap` below is a swap and not a load.
    pub(crate) fn drained(&self) {
        let Some(claim) = self.claim.get() else {
            // Nothing has been delivered on this bind yet, so nothing drained.
            return;
        };
        self.drained.fetch_add(1, Ordering::Relaxed);
        if !self.listed.swap(true, Ordering::AcqRel) && claim.lane.try_send(claim.index).is_err() {
            self.listed.store(false, Ordering::Release);
        }
        if claim.pending.swap(true, Ordering::AcqRel) {
            return; // a wake for this peer is already outstanding
        }
        if self.wake.try_send(claim.peer).is_err() {
            // Nobody will take the flag down, so let the next drain try again
            // rather than leaving this peer permanently marked as pending.
            claim.pending.store(false, Ordering::Release);
        }
    }

    /// Clear the listing, then take the drain count.
    ///
    /// The order is what makes a concurrent drain safe, and it is the reverse
    /// of [`drained`](Self::drained)'s. A drain landing between the two steps
    /// finds `listed` down and lists the slot again, so the next pass sees
    /// either a count of zero (this pass had already taken its record) or the
    /// new drain — never a count with nothing to come and fetch it. Swapping
    /// first and clearing after loses exactly that case: the drain would find
    /// `listed` still up, decline to list, and its credit would wait for the
    /// periodic walk.
    ///
    /// That guarantee is not a consequence of the abstract Rust/C++ memory
    /// model on its own: `listed`'s `store` here and `drained`'s `fetch_add`
    /// in [`drained`](Self::drained) are on different atomics joined only by
    /// program order on each side, which is the store-buffering shape that
    /// model permits between `Release`/`Acquire` operations on different
    /// locations, closed only by making every operation on both sides
    /// `SeqCst`. Where the model permits it, the failure is bounded rather
    /// than a lost drain: the count stays on `drained` and the slot is not
    /// relisted, which is the same degradation as a lane entry lost to a full
    /// lane, and the periodic whole-table walk already covers that within one
    /// `credit_sweep_interval`. Not observed on the x86-64 and AArch64
    /// targets this crate is built and measured for; a target that needs the
    /// tighter guarantee promotes this pair to `SeqCst` instead.
    pub(super) fn take_drained(&self) -> u32 {
        self.listed.store(false, Ordering::Release);
        self.drained.swap(0, Ordering::AcqRel)
    }
}
