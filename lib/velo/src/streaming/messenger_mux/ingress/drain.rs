// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The per-slot drain signal: what the consumer (a mux-fed `StreamAnchor`,
//! or an MPSC anchor's pump) counts and posts against, on its own task,
//! without ever taking the peer mutex.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use super::super::PeerLane;
use super::super::protocol::SlotId;
use super::dirty::DirtySlots;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    Active,
    StopRequested,
    Cancelled,
}

/// Told when the consumer takes a record out of the buffer credit is issued
/// against, so credit comes back by draining instead of by a timer.
///
/// The first design had `reader_pump` call `credit.release(1)` after each
/// handoff to `frame_tx` — exact, O(1), and immediate — leaving the sweep to
/// reclaim only for slots whose consumer was gone
/// (`docs/src/development/batched-streaming-design.md` has the history). Two
/// halves of that landed and one did not, deliberately. A mux bind now has no
/// reader pump: its consumer reads the buffer itself and counts here.
///
/// **The consumer counts, and it names its slot.** `drained` is the exact number
/// of records this slot's consumer has taken out of the buffer since the last
/// reconcile, and the slot's bit in its peer's [`DirtySlots`] says whether it
/// is already listed waiting for one. That is what lets a reconcile be exact
/// without reading the slot channel's length — a read that takes that
/// channel's lock, which is what made the arrival path's whole-table walk
/// expensive enough to narrow in the first place.
///
/// **The consumer does not release credit.** Releasing needs the peer's mutex —
/// the same one the inbound batch path takes — and taking it per record would
/// trade a periodic cost for a worse per-record one. Two paths each releasing
/// an amount for one drained record would also double-count, and the periodic
/// sweep is still there. So the consumer posts and the reconcile decides, which
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
    lifecycle: parking_lot::Mutex<Lifecycle>,
    /// Records this slot's consumer has taken out of the buffer since the last
    /// [`IngressSlot::reconcile`](super::slot::IngressSlot::reconcile) swapped
    /// it to zero.
    drained: AtomicU32,
    /// Records delivered into this slot's buffer, bumped once per delivered
    /// record by the ingress (records parked in the reorder hold do not
    /// count). The direct feed's watchdog reads it as the sender's liveness:
    /// it never sees a frame itself.
    arrivals: AtomicU64,
    /// Whether the sender holds no data credit and no records wait in the
    /// reorder hold, published by the slot. The watchdog's exemption: a
    /// sender without credit cannot heartbeat.
    sender_parked: AtomicBool,
    /// Set with `closed`, readable without a lock; see `is_released`.
    released: AtomicBool,
    /// Fired when the mux lets go of this bind's buffer: an unclaimed bind
    /// released or expired, or a claimed slot retired. The direct feed's
    /// watchdog never receives from the buffer, so it cannot see the close
    /// the way a receiver does; this is how it learns in time to reap an
    /// unclaimed bind before its consumer notices anything.
    closed: tokio_util::sync::CancellationToken,
    wake: flume::Sender<PeerLane>,
}

/// What an `OpenSlot` tells a bind's [`DrainSignal`] when it claims it.
struct SlotClaim {
    /// The (peer, lane) the `OpenSlot` arrived on. A stop, cancel or close of
    /// this slot goes back through that lane's batcher and no other: slot ids
    /// are unique only within one lane.
    key: PeerLane,
    /// The peer's "a credit-return visit is already queued" flag.
    pending: Arc<AtomicBool>,
    /// The slot this bind became, whose index names it in the dirty set.
    slot: SlotId,
    /// The peer's dirty-slot set, for naming this slot as having drained.
    dirty: Arc<DirtySlots>,
}

impl DrainSignal {
    pub(crate) fn new(wake: flume::Sender<PeerLane>) -> Self {
        Self {
            claim: OnceLock::new(),
            lifecycle: parking_lot::Mutex::new(Lifecycle::Active),
            drained: AtomicU32::new(0),
            arrivals: AtomicU64::new(0),
            sender_parked: AtomicBool::new(false),
            released: AtomicBool::new(false),
            closed: tokio_util::sync::CancellationToken::new(),
            wake,
        }
    }

    /// Name the (peer, lane) this bind turned out to belong to, its slot
    /// index, that table's dirty-slot set and its pending-wake flag. Called
    /// once, when an `OpenSlot` claims the bind.
    pub(crate) fn claimed_by(
        &self,
        key: PeerLane,
        slot: SlotId,
        pending: Arc<AtomicBool>,
        dirty: Arc<DirtySlots>,
    ) -> Lifecycle {
        let lifecycle = self.lifecycle.lock();
        let _ = self.claim.set(SlotClaim {
            key,
            slot,
            pending,
            dirty,
        });
        *lifecycle
    }

    pub(crate) fn request_stop(&self) -> Option<(PeerLane, SlotId)> {
        let mut state = self.lifecycle.lock();
        if *state != Lifecycle::Active {
            return None;
        }
        *state = Lifecycle::StopRequested;
        self.claimed()
    }

    pub(crate) fn cancel(&self) -> Option<(PeerLane, SlotId)> {
        *self.lifecycle.lock() = Lifecycle::Cancelled;
        self.claimed()
    }

    /// The (peer, lane) and slot that claimed this bind, once one has.
    ///
    /// `None` is "no `OpenSlot` has arrived", which is what tells a pre-bind's
    /// owner that giving up means releasing a bind rather than closing a slot.
    pub(crate) fn claimed(&self) -> Option<(PeerLane, SlotId)> {
        self.claim.get().map(|claim| (claim.key, claim.slot))
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
    /// drain rode on is answered by a take that also collects its count. What
    /// this saves depends on the shape: with about one record per stream per
    /// batch, the arrival path's take unlists nearly every slot between
    /// drains, so nearly every drain lists and reaches `pending`; the `swap`
    /// usually finds it already up and posts nothing.
    ///
    /// `try_send` rather than an await on the wake: this runs on the
    /// consumer's path for every record and must never park it. The wake lane
    /// is unbounded (`drain_wake_lane` has why) and the mux core holds its
    /// receiver, so the send fails only once the core itself is gone. If it
    /// does, `pending` goes back down, since leaving it up would claim a visit
    /// is coming when none is.
    ///
    /// A per-slot record threshold was the alternative to the wake and is
    /// worse on both counts: it withholds credit for the first `T` records of
    /// every slot, which is latency on the path this change exists to speed up,
    /// and with a thousand slots on one peer it still posts a thousand times.
    ///
    /// Orderings: the count's `fetch_add` is `Relaxed` and the listing's
    /// `fetch_or` is `AcqRel`, so a take whose swap reads the listing (or any
    /// later RMW on its word) sees the count. Both writers of `pending` are
    /// RMWs -- this `swap(true)` and the visit's `swap(false)` in
    /// `IngressRegistry::clear_pending_wake` -- so whichever comes second reads
    /// the first; see there for why the clear cannot be a store.
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
        if self.wake.try_send(claim.key).is_err() {
            // The sweep task is gone; nobody will take the flag down.
            claim.pending.store(false, Ordering::Release);
        }
    }

    /// A record delivered into this slot's buffer. The watchdog only asks
    /// whether the count moved during its window; the ingress counts
    /// deliveries, not records parked in its reorder hold.
    pub(super) fn note_arrival(&self) {
        self.arrivals.fetch_add(1, Ordering::Relaxed);
    }

    /// Record whether the sender holds any data credit, as the slot's account
    /// sees it. Written under the peer's mutex on admit, reconcile, grant and
    /// the reorder hold, so per record; the store is skipped when nothing
    /// changed, which keeps it a load in steady state.
    pub(super) fn set_sender_parked(&self, parked: bool) {
        if self.sender_parked.load(Ordering::Relaxed) != parked {
            self.sender_parked.store(parked, Ordering::Relaxed);
        }
    }

    /// Whether the sender holds no data credit, so cannot send a heartbeat,
    /// and no records wait in the reorder hold (see `IngressSlot`'s
    /// `publish_credit`). Read by the stream watchdog, which may be up to a credit round trip
    /// behind; its detection window is several heartbeats long.
    pub(crate) fn sender_parked(&self) -> bool {
        self.sender_parked.load(Ordering::Relaxed)
    }

    /// The mux let go of this bind's buffer. Idempotent.
    pub(crate) fn close(&self) {
        self.released.store(true, Ordering::Release);
        self.closed.cancel();
    }

    /// Record that the slot is retiring on its sender's terminal, from a
    /// consumer that has just read that terminal off the buffer: the ingress
    /// applied it in the same step that retires the slot, and this lands the
    /// release a moment before that step does.
    pub(crate) fn mark_released(&self) {
        self.released.store(true, Ordering::Release);
    }

    /// Whether the mux has already let go of this bind's buffer: its slot
    /// retired (on the sender's terminal, among others) or its bind released.
    ///
    /// A plain load, where `closed().is_cancelled()` would lock the token: a
    /// consumer that ends a stream asks this so an ordinary end, whose slot
    /// the terminal already retired, never takes the peer's ingress lock.
    pub(crate) fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    /// Fires once the mux has let go of this bind's buffer.
    pub(crate) fn closed(&self) -> tokio_util::sync::CancellationToken {
        self.closed.clone()
    }

    /// How many records have been delivered into this slot.
    pub(crate) fn arrivals(&self) -> u64 {
        self.arrivals.load(Ordering::Relaxed)
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
    pub(super) fn take_drained(&self) -> u32 {
        self.drained.swap(0, Ordering::AcqRel)
    }
}
