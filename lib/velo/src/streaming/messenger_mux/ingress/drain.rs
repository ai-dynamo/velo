// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The per-slot drain signal: what a reader pump counts and posts against,
//! on its own task, without ever taking the peer mutex.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use velo_ext::WorkerId;

use super::super::protocol::SlotId;
use super::dirty::DirtySlots;

/// Told when the consumer takes a record out of the buffer credit is issued
/// against, so credit comes back by draining instead of by a timer.
///
/// `docs/src/development/batched-streaming-design.md` specifies this: `reader_pump` "gains an
/// `Option<CreditReturn>` and calls `credit.release(1)` after each successful
/// handoff to `frame_tx` — exact, O(1), and immediate", leaving the sweep to
/// reclaim only for slots whose pump died. Two halves of that landed and one
/// did not, deliberately.
///
/// **The pump counts, and it names its slot.** `drained` is the exact number
/// of records this slot's pump has taken out of the buffer since the last
/// reconcile, and the slot's bit in its peer's [`DirtySlots`] says whether it
/// is already listed waiting for one. That is what lets a reconcile be exact
/// without reading the slot channel's length — a read that takes that
/// channel's lock, which is what made the arrival path's whole-table walk
/// expensive enough to narrow in the first place.
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
    // Serializes claim with early stop/cancel; neither side can miss the other.
    lifecycle: std::sync::Mutex<u8>,
    /// Records this slot's pump has taken out of the buffer since the last
    /// [`IngressSlot::reconcile`](super::slot::IngressSlot::reconcile) swapped
    /// it to zero.
    drained: AtomicU32,
    /// Batches that delivered into this slot, bumped once per batch by the
    /// ingress. The direct feed's watchdog reads it as the sender's liveness:
    /// it never sees a frame itself.
    arrivals: AtomicU64,
    /// Fired when the mux lets go of this bind's buffer: an unclaimed bind
    /// released or expired, or a claimed slot retired. The direct feed's
    /// watchdog never receives from the buffer, so it cannot see the close
    /// the way a receiver does; this is how it learns in time to reap an
    /// unclaimed bind before its consumer notices anything.
    closed: tokio_util::sync::CancellationToken,
    wake: flume::Sender<WorkerId>,
}

/// What an `OpenSlot` tells a bind's [`DrainSignal`] when it claims it.
struct SlotClaim {
    peer: WorkerId,
    /// The peer's "a credit-return visit is already queued" flag.
    pending: Arc<AtomicBool>,
    /// The slot this bind became, whose index names it in the dirty set.
    slot: SlotId,
    /// The peer's dirty-slot set, for naming this slot as having drained.
    dirty: Arc<DirtySlots>,
}

impl DrainSignal {
    pub(crate) fn new(wake: flume::Sender<WorkerId>) -> Self {
        Self {
            claim: OnceLock::new(),
            lifecycle: std::sync::Mutex::new(0),
            drained: AtomicU32::new(0),
            arrivals: AtomicU64::new(0),
            closed: tokio_util::sync::CancellationToken::new(),
            wake,
        }
    }

    /// Name the peer this bind turned out to belong to, its slot index, that
    /// peer's dirty-slot set and its pending-wake flag. Called once, when an
    /// `OpenSlot` claims the bind.
    pub(crate) fn claimed_by(
        &self,
        peer: WorkerId,
        slot: SlotId,
        pending: Arc<AtomicBool>,
        dirty: Arc<DirtySlots>,
    ) -> u8 {
        let lifecycle = self.lifecycle.lock().unwrap();
        let _ = self.claim.set(SlotClaim {
            peer,
            slot,
            pending,
            dirty,
        });
        *lifecycle
    }

    pub(crate) fn request_stop(&self) -> Option<(WorkerId, SlotId)> {
        let mut state = self.lifecycle.lock().unwrap();
        if *state != 0 {
            return None;
        }
        *state = 1;
        self.claimed()
    }

    pub(crate) fn cancel(&self) -> Option<(WorkerId, SlotId)> {
        *self.lifecycle.lock().unwrap() = 2;
        self.claimed()
    }

    /// The peer and slot that claimed this bind, once one has.
    ///
    /// `None` is "no `OpenSlot` has arrived", which is what tells a pre-bind's
    /// owner that giving up means releasing a bind rather than closing a slot.
    pub(crate) fn claimed(&self) -> Option<(WorkerId, SlotId)> {
        self.claim.get().map(|claim| (claim.peer, claim.slot))
    }

    /// One record left the buffer: count it, name the slot, ring the doorbell.
    ///
    /// The count comes first and is unconditional, because it is the only
    /// record of the drain that survives — the listing and the wake are both
    /// hints about *when* to look, and a reconcile that arrives by any route
    /// reads the same number.
    ///
    /// The listing is per *slot* and the wake is per *peer*, which is the
    /// granularity each does its work at: one bit in the peer's
    /// [`DirtySlots`] is all a reconcile needs to find this slot, and one wake
    /// is all the sweep task needs to come and take the set. The set's own
    /// dedup and the peer's `pending` flag are the two coalescers, and each is
    /// taken down by the visit it summoned.
    ///
    /// **Only the drain that lists the slot touches `pending`.** Every
    /// consumer of the peer's streams drains into that one flag, so a write
    /// per record bounces its cache line between every thread running one of
    /// them. A drain that finds its slot already listed has nothing to add: the
    /// drain that listed it either posted a wake or found one already
    /// outstanding, a visit takes `pending` down *before* it takes the set
    /// (`MuxCore::visit_drained_peer`, `MuxCore::sweep_peer`), and the arrival
    /// path's own take leaves the slot unlisted, so the next drain lists it
    /// again and does the `pending` step itself. Either way the listing this
    /// drain rode on is answered by a take that also collects its count.
    ///
    /// `try_send` rather than an await on the wake: this runs on the
    /// consumer's path for every record and must never park it. The wake lane
    /// is unbounded (`drain_wake_lane` has why), so the send fails only once
    /// the sweep task is gone; `pending` goes back down then, since leaving it
    /// up would claim a visit is coming when none is.
    ///
    /// A per-slot record threshold was the alternative to the wake and is
    /// worse on both counts: it withholds credit for the first `T` records of
    /// every slot, which is latency on the path this change exists to speed up,
    /// and with a thousand slots on one peer it still posts a thousand times.
    ///
    /// Orderings: the count's `fetch_add` is `Relaxed` and the listing's
    /// `fetch_or` is `AcqRel`, so a take whose swap reads the listing (or any
    /// later RMW on its word) sees the count. `pending` is a `swap`, never a
    /// load followed by a store, so a concurrent clear is always observed.
    pub(crate) fn drained(&self) {
        let Some(claim) = self.claim.get() else {
            // Nothing has been delivered on this bind yet, so nothing drained.
            return;
        };
        self.drained.fetch_add(1, Ordering::Relaxed);
        if !claim.dirty.mark(claim.slot.index()) {
            return; // already listed; that listing's visit collects this count
        }
        if claim.pending.swap(true, Ordering::AcqRel) {
            return; // a wake for this peer is already outstanding
        }
        if self.wake.try_send(claim.peer).is_err() {
            // The sweep task is gone; nobody will take the flag down.
            claim.pending.store(false, Ordering::Release);
        }
    }

    /// Take the drain count.
    ///
    /// Called only for a slot whose listing the caller already took out of the
    /// peer's [`DirtySlots`] (or, on the whole-table walk, after taking every
    /// listing): clearing the listing first is what makes a concurrent drain
    /// safe. A drain landing after the take finds its bit clear and lists the
    /// slot again, so the next pass sees either a count of zero (this call
    /// already took its record) or the new drain — never a count with nothing
    /// to come and fetch it. Both sides are RMWs (`fetch_or` against `swap` on
    /// the slot's word, then `fetch_add` against this `swap`), so unlike the
    /// store-then-swap this replaced there is no store-buffering window.
    /// A batch delivered into this slot. Once per batch, not per record: the
    /// watchdog only asks whether the count moved during its window.
    pub(super) fn note_arrival(&self) {
        self.arrivals.fetch_add(1, Ordering::Relaxed);
    }

    /// The mux let go of this bind's buffer. Idempotent.
    pub(crate) fn close(&self) {
        self.closed.cancel();
    }

    /// Fires once the mux has let go of this bind's buffer.
    pub(crate) fn closed(&self) -> tokio_util::sync::CancellationToken {
        self.closed.clone()
    }

    /// How many batches have delivered into this slot.
    pub(crate) fn arrivals(&self) -> u64 {
        self.arrivals.load(Ordering::Relaxed)
    }

    pub(super) fn take_drained(&self) -> u32 {
        self.drained.swap(0, Ordering::AcqRel)
    }
}
