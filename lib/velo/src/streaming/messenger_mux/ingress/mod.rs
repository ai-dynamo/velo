// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The `_stream_batch` handler body — the receive side of the mux.
//!
//! The handler is registered with **ordered per-sender dispatch**, so batches
//! from one peer are handled on that peer's lane, by one task, in arrival order.
//! That is the guarantee the deleted `VeloFrameTransport` lacked: it layered a
//! 4096-deep reorder buffer over a dispatcher that spawns a task per inbound
//! message, and under cross-stream contention the window overflowed and
//! deadlocked the consumer. With the lane in place the general reordering
//! problem does not arise and needs no window to solve it.
//!
//! Holding the lane is also the constraint everything here is written against.
//! **Nothing in this module awaits.** State sits behind a `std::sync::Mutex`
//! taken and released with no await point in between, and the reply records a
//! pass produces are handed to the peer's batcher afterwards over an unbounded
//! channel that cannot block either.
//!
//! One narrow exception to lane order survives, and it is self-inflicted:
//! rendezvous payloads resolve in a detached task *before* dispatch, so an
//! oversized record routed that way is not ordered against the eager batches
//! around it. `frame_seq` carries the order proof and the per-slot hold is where
//! an early record waits — bounded by the credit already granted and by the byte
//! budget behind it. Overflow closes **that** slot and nothing else.

mod slot;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use velo_ext::WorkerId;

use self::slot::{Applied, IngressSlot, heartbeat_frame};
use super::MuxConfig;
use super::flow_control::ByteBudget;
use super::peer_batcher::ReplyRecord;
use super::protocol::{
    BatchDecoder, BatchHeader, CloseReason, Record, RecordBody, SlotId, batch_seq_gap,
};
use crate::observability::{MuxDirection, MuxDropReason, MuxMetricsHandle};

/// Ceiling on the dense slot table one peer may make this node allocate.
///
/// A sender allocates from a free list starting at zero, so its indices stay
/// within a small multiple of its live slot count; a jump past this is a
/// misbehaving or hostile peer sizing a `Vec` on this node from a wire field.
/// 64 Ki is two orders of magnitude above any real fan-in — a decode engine's
/// 1024 concurrent streams to one peer use indices 0..1024 — and keeps the
/// worst case one `OpenSlot` can force to a few megabytes rather than a few
/// hundred, which is the same amplification the batch decoder refuses.
const MAX_INGRESS_SLOTS_PER_PEER: usize = 1 << 16;

/// A `bind()` waiting for the `OpenSlot` that will claim it.
struct BindEntry {
    /// The mux-owned `C + 1` buffer whose receiver went to the anchor.
    frame_tx: flume::Sender<Vec<u8>>,
    /// Handed to `reader_pump` at attach; told which peer it belongs to here,
    /// when an `OpenSlot` claims this bind.
    drain: Arc<DrainSignal>,
}

/// Registry of binds and per-peer slot tables.
#[derive(Default)]
pub(crate) struct IngressRegistry {
    /// `(anchor_id, session_id)` → the buffer a matching `OpenSlot` claims.
    binds: DashMap<(u64, u64), BindEntry>,
    /// Per-peer slot tables. One `Mutex` per peer, uncontended in steady state
    /// because the peer's ordering lane is its only writer; the credit sweep is
    /// the sole other visitor.
    peers: DashMap<WorkerId, Mutex<PeerIngress>>,
    /// Per-peer "a credit-return visit is already queued" flags, read and set by
    /// draining pumps without taking the peer mutex. See [`DrainSignal`].
    ///
    /// **Grows with distinct peers and is never pruned**, which mirrors `peers`
    /// above and costs a pointer and a bool per peer this node has ever received
    /// a slot from. Removing an entry is not a matter of picking a moment: a
    /// pump holds its peer's flag as an `Arc` for the life of its stream, so a
    /// removal while any such pump lives leaves that pump setting a flag nothing
    /// reads — permanently true, permanently coalescing, and that peer's credit
    /// falls back to the periodic sweep for the rest of the stream. So it may
    /// only be removed under the same visibility that retires slots and binds,
    /// and until that is worth building, unbounded-but-tiny is the honest trade.
    drain_pending: DashMap<WorkerId, Arc<AtomicBool>>,
}

/// Receive-side state for one peer.
struct PeerIngress {
    /// The sender epoch this table belongs to. `None` until the first batch.
    epoch: Option<u64>,
    last_batch_seq: Option<u32>,
    slots: Vec<Option<IngressSlot>>,
    peer_bytes: ByteBudget,
}

impl PeerIngress {
    fn new(peer_byte_budget: u64) -> Self {
        Self {
            epoch: None,
            last_batch_seq: None,
            slots: Vec::new(),
            peer_bytes: ByteBudget::new(peer_byte_budget),
        }
    }

    fn live(&self) -> usize {
        self.slots.iter().filter(|entry| entry.is_some()).count()
    }
}

/// What one batch produced, acted on after the peer lock is released.
#[derive(Default)]
pub(crate) struct BatchOutcome {
    /// Control records to send back to this peer.
    pub(crate) replies: Vec<ReplyRecord>,
    /// `CreditUpdate`s addressed to slots *we* own, for the egress batcher.
    pub(crate) grants: Vec<(SlotId, u32)>,
    /// `CloseSlot`s addressed to slots *we* own, likewise.
    pub(crate) peer_closes: Vec<(SlotId, CloseReason)>,
    /// Slots this batch created.
    pub(crate) opened: usize,
    /// Slots this batch retired.
    pub(crate) closed: usize,
}

/// The read-only context an apply pass needs.
struct ApplyCtx<'a> {
    registry: &'a IngressRegistry,
    config: &'a MuxConfig,
    metrics: Option<&'a MuxMetricsHandle>,
    /// Whose batch this is. Carried so `open_slot` can tell the bind's
    /// [`DrainSignal`] which peer it turned out to belong to.
    peer: WorkerId,
}

/// Told when the consumer takes a record out of the buffer credit is issued
/// against, so credit can be returned by draining instead of by a timer.
///
/// `BATCHING.md` § P8 specifies this: `reader_pump` "gains an
/// `Option<CreditReturn>` and calls `credit.release(1)` after each successful
/// handoff to `frame_tx` — exact, O(1), and immediate", leaving the sweep to
/// reclaim only for slots whose pump died. What shipped instead reconciled
/// occupancy on a 500 Hz sweep that walks every slot of every ingress peer,
/// so its cost grows as `O(peers x slots)` while the credit it finds does not.
///
/// The signal does not touch the credit ledger itself. Releasing credit needs
/// the peer's mutex — the same one the inbound batch path takes — and taking it
/// per record would trade a periodic cost for a worse per-record one. It posts
/// the peer instead, and the sweep task does the reconcile it already knows how
/// to do. That turns work proportional to *time × peers* into work a peer only
/// pays when its consumer drains — bounded above by the drains and below by
/// [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor), which
/// is what stops a consumer that keeps up from turning the doorbell into a spin
/// over that peer's slot table.
///
/// The peer is not known when `bind` creates this: a bind belongs to whoever
/// claims it, and the claim arrives later as an `OpenSlot`. Until then the
/// signal is inert, which is correct — nothing has been delivered, so nothing
/// has drained.
pub(crate) struct DrainSignal {
    /// Whose bind this turned out to be, and that peer's pending-wake flag.
    /// Both arrive together when an `OpenSlot` claims the bind.
    claim: std::sync::OnceLock<(WorkerId, Arc<AtomicBool>)>,
    wake: flume::Sender<WorkerId>,
}

impl DrainSignal {
    pub(crate) fn new(wake: flume::Sender<WorkerId>) -> Self {
        Self {
            claim: std::sync::OnceLock::new(),
            wake,
        }
    }

    /// Name the peer this bind turned out to belong to, and hand it that peer's
    /// pending-wake flag. Called once, when an `OpenSlot` claims the bind.
    pub(crate) fn claimed_by(&self, peer: WorkerId, pending: Arc<AtomicBool>) {
        let _ = self.claim.set((peer, pending));
    }

    /// One record left the buffer.
    ///
    /// Coalesced per *peer*, which is the granularity the work happens at: one
    /// `sweep_peer` reconciles every slot of that peer, so a second wake while
    /// one is outstanding would buy nothing. The flag makes that exact — the
    /// first drain posts, the rest are free until the sweep task takes it down.
    /// The sweep task keeps the flag up while it holds a visit back under
    /// [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor), so
    /// the drains arriving during that hold coalesce into the visit it has
    /// already scheduled.
    ///
    /// A per-slot record threshold was the alternative and is worse on both
    /// counts: it withholds credit for the first `T` records of every slot,
    /// which is latency on the path this change exists to speed up, and with a
    /// thousand slots on one peer it still posts a thousand times.
    ///
    /// `try_send` rather than an await: this runs on the pump's task, in the
    /// path of every frame, and must never park it. **A full lane puts the flag
    /// back down.** Leaving it up would be a claim that a visit is queued when
    /// none is, and every later drain would coalesce into a wake that was
    /// dropped — the peer would be stuck on the periodic sweep for the rest of
    /// the stream. Clearing it costs this one drain its wake and lets the next
    /// one try again; the periodic sweep is what bounds the gap if no next one
    /// comes.
    pub(crate) fn drained(&self) {
        let Some((peer, pending)) = self.claim.get() else {
            // Nothing has been delivered on this bind yet, so nothing drained.
            return;
        };
        if pending.swap(true, Ordering::AcqRel) {
            return; // a wake for this peer is already outstanding
        }
        if self.wake.try_send(*peer).is_err() {
            // Nobody will take the flag down, so let the next drain try again
            // rather than leaving this peer permanently marked as pending.
            pending.store(false, Ordering::Release);
        }
    }
}

impl IngressRegistry {
    /// This peer's pending-wake flag, created on first use.
    ///
    /// Lives on the registry rather than in `PeerIngress` so a draining pump
    /// can reach it without taking the peer mutex — taking that mutex per
    /// record is the cost this whole change exists to avoid.
    pub(crate) fn pending_wake(&self, peer: WorkerId) -> Arc<AtomicBool> {
        Arc::clone(
            self.drain_pending
                .entry(peer)
                .or_insert_with(|| Arc::new(AtomicBool::new(false)))
                .value(),
        )
    }

    /// Take this peer's wake down, so drains landing during the visit post a
    /// fresh one rather than being swallowed by it.
    pub(crate) fn clear_pending_wake(&self, peer: WorkerId) {
        if let Some(flag) = self.drain_pending.get(&peer) {
            flag.store(false, Ordering::Release);
        }
    }

    /// Register the buffer a `bind()` created, keyed by `(anchor, session)`.
    pub(crate) fn register_bind(
        &self,
        anchor_id: u64,
        session_id: u64,
        frame_tx: flume::Sender<Vec<u8>>,
        drain: Arc<DrainSignal>,
    ) {
        self.binds
            .insert((anchor_id, session_id), BindEntry { frame_tx, drain });
    }

    /// Drop an unclaimed bind, reporting whether one was there.
    pub(crate) fn expire_bind(&self, anchor_id: u64, session_id: u64) -> bool {
        self.binds.remove(&(anchor_id, session_id)).is_some()
    }

    /// Bytes `peer`'s ahead-of-sequence holds have reserved between them.
    #[cfg(test)]
    pub(crate) fn peer_bytes_used(&self, peer: WorkerId) -> u64 {
        self.peers
            .get(&peer)
            .map_or(0, |entry| lock(entry.value()).peer_bytes.used())
    }

    /// Live receive-side slots for `peer`.
    pub(crate) fn live_slots(&self, peer: WorkerId) -> usize {
        self.peers
            .get(&peer)
            .map_or(0, |entry| lock(entry.value()).live())
    }

    /// Every peer with receive-side state, for the credit sweep.
    pub(crate) fn peers(&self) -> Vec<WorkerId> {
        self.peers.iter().map(|entry| *entry.key()).collect()
    }

    /// Reconcile every slot of `peer` and collect the credit now returnable.
    ///
    /// The sweep is load-bearing rather than a backstop: a peer whose only slot
    /// has parked out of credit sends nothing more, so no further batch arrives
    /// to drive reconciliation on the arrival path, and without this the pair
    /// deadlocks with the consumer drained and the sender parked.
    pub(crate) fn sweep_credit(&self, peer: WorkerId) -> Vec<ReplyRecord> {
        let Some(entry) = self.peers.get(&peer) else {
            return Vec::new();
        };
        let mut state = lock(entry.value());
        let mut replies = Vec::new();
        collect_grants(&mut state, &mut replies);
        replies
    }

    /// Tear down every slot of every peer, injecting `Dropped` into each.
    ///
    /// Used when the transport itself goes away, so a consumer never waits out
    /// its heartbeat watchdog for a sender that has already been dismantled.
    pub(crate) fn shutdown(&self) -> usize {
        let mut closed = 0;
        for entry in self.peers.iter() {
            let mut state = lock(entry.value());
            closed += retire_epoch(&mut state, None);
        }
        self.binds.clear();
        closed
    }
}

/// Handle one `_stream_batch` payload from `peer`.
///
/// Returns the replies to send back, the records addressed to our own egress
/// slots, and the slot-count deltas the caller feeds the `live_slots` gauge.
/// Never blocks, never awaits.
pub(crate) fn handle_batch(
    registry: &IngressRegistry,
    config: &MuxConfig,
    metrics: Option<&MuxMetricsHandle>,
    peer: WorkerId,
    payload: &Bytes,
) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();

    let header = match BatchHeader::decode(payload) {
        Ok(header) => header,
        Err(error) => {
            tracing::warn!(peer = %peer, %error, "messenger mux: undecodable batch header");
            return outcome;
        }
    };

    if !registry.peers.contains_key(&peer) {
        registry
            .peers
            .entry(peer)
            .or_insert_with(|| Mutex::new(PeerIngress::new(config.peer_byte_budget)));
    }
    // A read guard, not `entry`'s write guard: the `Mutex` inside already
    // serialises writers, and holding the shard for writing would block the
    // credit sweep on an unrelated peer in the same shard.
    let Some(entry) = registry.peers.get(&peer) else {
        return outcome;
    };
    let mut state = lock(entry.value());

    if !accept_epoch(&mut state, &header, metrics, &mut outcome) {
        return outcome;
    }
    note_batch_seq(&mut state, &header, metrics);

    let decoder = match BatchDecoder::new(payload) {
        Ok(decoder) => decoder,
        Err(error) => {
            tracing::warn!(peer = %peer, %error, "messenger mux: undecodable batch");
            return outcome;
        }
    };
    if let Some(metrics) = metrics {
        metrics.batch(MuxDirection::Received, usize::from(header.record_count));
    }

    let ctx = ApplyCtx {
        registry,
        config,
        metrics,
        peer,
    };
    for decoded in decoder {
        match decoded {
            Ok(record) => apply_record(&mut state, &ctx, &record, &mut outcome),
            Err(error) => {
                tracing::warn!(
                    peer = %peer,
                    %error,
                    "messenger mux: malformed record; the rest of the batch is skipped"
                );
                break;
            }
        }
    }

    // One reconcile pass per batch: the consumer has had the whole batch to
    // drain, and doing it here keeps credit returns off the per-record path.
    collect_grants(&mut state, &mut outcome.replies);
    outcome
}

/// Decide what to do with a batch's epoch. `false` means discard the batch.
fn accept_epoch(
    state: &mut PeerIngress,
    header: &BatchHeader,
    metrics: Option<&MuxMetricsHandle>,
    outcome: &mut BatchOutcome,
) -> bool {
    match state.epoch {
        // First batch from this peer: adopt whatever epoch it names.
        None => state.epoch = Some(header.peer_epoch),
        Some(current) if header.peer_epoch < current => {
            // Discarded wholesale by header inspection rather than drained
            // record by record against state that has moved on.
            if let Some(metrics) = metrics {
                metrics.records_dropped(MuxDropReason::StaleEpoch, u64::from(header.record_count));
            }
            return false;
        }
        Some(current) if header.peer_epoch > current => {
            // The reconnect, seen from the receive side. Egress learns of epoch
            // death from a failed admission; the receiver's only signal is this
            // header, and without acting on it the old epoch's slots leak for
            // the life of the process and `live_slots` never returns to zero.
            outcome.closed += retire_epoch(state, metrics);
            state.epoch = Some(header.peer_epoch);
            state.last_batch_seq = None;
        }
        Some(_) => {}
    }
    true
}

fn note_batch_seq(
    state: &mut PeerIngress,
    header: &BatchHeader,
    metrics: Option<&MuxMetricsHandle>,
) {
    if let (Some(metrics), Some(last)) = (metrics, state.last_batch_seq) {
        let gap = batch_seq_gap(last.wrapping_add(1), header.batch_seq);
        if gap > 0 {
            metrics.batch_seq_gap(gap);
        }
    }
    state.last_batch_seq = Some(header.batch_seq);
}

/// Apply one record to the peer's slot table.
fn apply_record(
    state: &mut PeerIngress,
    ctx: &ApplyCtx<'_>,
    record: &Record<'_>,
    outcome: &mut BatchOutcome,
) {
    match record.body {
        RecordBody::OpenSlot {
            anchor_id,
            session_id,
        } => open_slot(state, ctx, record, anchor_id, session_id, outcome),
        RecordBody::CreditUpdate { delta } => {
            // Addressed to a slot *we* opened, so it has no entry in this table
            // and must not be looked up in it. The caller routes it to the
            // egress batcher.
            outcome.grants.push((record.slot, delta));
        }
        RecordBody::CloseSlot { reason } => {
            close_slot(state, ctx, record.slot, record.frame_seq, reason, outcome);
        }
        RecordBody::Data(body) => deliver(state, ctx, record, body.to_vec(), outcome),
        RecordBody::SlotHeartbeat => deliver(state, ctx, record, heartbeat_frame(), outcome),
    }
}

fn open_slot(
    state: &mut PeerIngress,
    ctx: &ApplyCtx<'_>,
    record: &Record<'_>,
    anchor_id: u64,
    session_id: u64,
    outcome: &mut BatchOutcome,
) {
    let id = record.slot;
    let index = id.index() as usize;
    if index >= MAX_INGRESS_SLOTS_PER_PEER {
        outcome.replies.push(ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::ProtocolError,
        });
        return;
    }
    // Checked *before* the bind is consumed, and before anything is written.
    //
    // A live slot at this index means the sender opened over an occupant this
    // side has not seen closed — it cannot have come from the free list, which
    // only yields an index after its `CloseSlot`. Retiring the incumbent to make
    // room would silently kill a healthy stream: its consumer would see the
    // channel end with no `Dropped`, its held bytes would stay charged to the
    // peer budget, and the collision would be invisible. So the *newcomer* is
    // rejected instead, the incumbent is untouched, and the bind stays
    // registered for the opener that is entitled to it.
    if state
        .slots
        .get(index)
        .and_then(Option::as_ref)
        .is_some_and(|incumbent| incumbent.id != id)
    {
        outcome.replies.push(ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::ProtocolError,
        });
        if let Some(metrics) = ctx.metrics {
            metrics.record_dropped(MuxDropReason::SlotCollision);
        }
        return;
    }

    let Some((_, bind)) = ctx.registry.binds.remove(&(anchor_id, session_id)) else {
        // The reverse race: an `OpenSlot` for a pair that was never registered,
        // or whose accept window expired. It must **not** fail the peer — reply
        // and discard that slot's records.
        outcome.replies.push(ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::UnknownSlot,
        });
        if let Some(metrics) = ctx.metrics {
            metrics.record_dropped(MuxDropReason::UnknownSlot);
        }
        return;
    };

    if state.slots.len() <= index {
        state.slots.resize_with(index + 1, || None);
    }
    // A re-`OpenSlot` for the *same* id is a duplicate, not a collision: the
    // guard above let it through, so it replaces the incumbent. It goes through
    // the ordinary close first, though — taking the slot out directly would skip
    // the held-byte release and leave the consumer's channel ending without the
    // `Dropped` that tells it why.
    if state.slots.get(index).and_then(Option::as_ref).is_some() {
        finish_close(state, id, CloseReason::PeerGone, ctx.metrics, outcome);
    }

    // No `CreditUpdate` reply: the window was advertised on the attach
    // response, and the sender opened its slot already holding it. Granting it
    // again here would hand the sender `2C` against a `C + 1` buffer — the
    // reader stall the credit invariant exists to make impossible. Credit
    // returns from here on are the ordinary reconciliation ones.
    // The bind now has an owner, so its drain signal can start posting wakes.
    // Before this point it is inert: nothing has been delivered on this slot,
    // so nothing can have drained.
    bind.drain
        .claimed_by(ctx.peer, ctx.registry.pending_wake(ctx.peer));

    let slot = IngressSlot::new(
        id,
        bind.frame_tx,
        ctx.config.initial_credit,
        ctx.config.slot_byte_budget,
        record.frame_seq.saturating_add(1),
    );
    state.slots[index] = Some(slot);
    outcome.opened += 1;
}

fn close_slot(
    state: &mut PeerIngress,
    ctx: &ApplyCtx<'_>,
    id: SlotId,
    frame_seq: u32,
    reason: CloseReason,
    outcome: &mut BatchOutcome,
) {
    // Direction is carried by the reason, not by a wire bit. `TerminalSent` and
    // `PeerGone` come from the slot's owner and act on this table; `UnknownSlot`
    // and `ProtocolError` come from a receiver rejecting a slot *we* opened, and
    // belong to the batcher. The partition matters because both sides may hold a
    // slot at the same dense index.
    if matches!(
        reason,
        CloseReason::UnknownSlot | CloseReason::ProtocolError
    ) {
        outcome.peer_closes.push((id, reason));
        return;
    }

    let due = match checked_slot(state, ctx.metrics, id) {
        Some(slot) => slot.apply_close(frame_seq, reason),
        None => return,
    };
    if due {
        finish_close(state, id, reason, ctx.metrics, outcome);
    }
}

/// Retire a slot, injecting `Dropped` unless its owner said it sent a terminal.
fn finish_close(
    state: &mut PeerIngress,
    id: SlotId,
    reason: CloseReason,
    metrics: Option<&MuxMetricsHandle>,
    outcome: &mut BatchOutcome,
) {
    let index = id.index() as usize;
    let Some(mut slot) = state.slots.get_mut(index).and_then(Option::take) else {
        return;
    };
    state.peer_bytes.release(slot.hold_bytes_used() as usize);
    if let Some(metrics) = metrics
        && slot.held() > 0
    {
        metrics.held_records_delta(-(slot.held() as i64));
    }
    if reason != CloseReason::TerminalSent {
        slot.inject_dropped();
    }
    // Dropping the mux-side sender is what makes `reader_pump` exit through the
    // same `Err` branch it uses today when a socket closes. Identical path.
    drop(slot);
    outcome.closed += 1;
}

fn deliver(
    state: &mut PeerIngress,
    ctx: &ApplyCtx<'_>,
    record: &Record<'_>,
    body: Vec<u8>,
    outcome: &mut BatchOutcome,
) {
    let id = record.slot;
    let index = id.index() as usize;
    if checked_slot(state, ctx.metrics, id).is_none() {
        return;
    }

    // Split the borrow by field: `apply_data` needs the peer budget alongside
    // the slot, and both live in `state`.
    let peer_bytes = &mut state.peer_bytes;
    let Some(slot) = state.slots[index].as_mut() else {
        return;
    };
    let held_before = slot.held();
    let applied = slot.apply_data(record.frame_seq, body, peer_bytes);
    let held_after = slot.held();
    let due = slot.due_close();

    if let Some(metrics) = ctx.metrics
        && held_after != held_before
    {
        metrics.held_records_delta(held_after as i64 - held_before as i64);
    }

    match applied {
        Applied::Delivered | Applied::Held => {
            if let Some(reason) = due {
                finish_close(state, id, reason, ctx.metrics, outcome);
            }
        }
        Applied::Duplicate => {
            if let Some(metrics) = ctx.metrics {
                metrics.record_dropped(MuxDropReason::Duplicate);
            }
        }
        Applied::ReaderStall => {
            if let Some(metrics) = ctx.metrics {
                metrics.reader_stall();
            }
            fail_slot(state, ctx, id, CloseReason::ProtocolError, outcome);
        }
        Applied::Fault(reason) => {
            if let Some(metrics) = ctx.metrics
                && reason == CloseReason::ProtocolError
            {
                metrics.hold_overflow();
            }
            fail_slot(state, ctx, id, reason, outcome);
        }
    }
}

/// Close a slot the receiver is rejecting, and tell its owner.
fn fail_slot(
    state: &mut PeerIngress,
    ctx: &ApplyCtx<'_>,
    id: SlotId,
    reason: CloseReason,
    outcome: &mut BatchOutcome,
) {
    finish_close(state, id, reason, ctx.metrics, outcome);
    outcome
        .replies
        .push(ReplyRecord::CloseSlot { slot: id, reason });
}

/// Look up a slot, rejecting a stale generation and an index that never opened.
fn checked_slot<'a>(
    state: &'a mut PeerIngress,
    metrics: Option<&MuxMetricsHandle>,
    id: SlotId,
) -> Option<&'a mut IngressSlot> {
    let index = id.index() as usize;
    match state.slots.get_mut(index).and_then(Option::as_mut) {
        Some(slot) if slot.id == id => Some(slot),
        Some(_) => {
            // Dense slot reuse caught by the generation tag. Without it this
            // record would surface inside whichever stream now holds the index.
            if let Some(metrics) = metrics {
                metrics.record_dropped(MuxDropReason::Generation);
            }
            None
        }
        None => {
            if let Some(metrics) = metrics {
                metrics.record_dropped(MuxDropReason::ClosedSlot);
            }
            None
        }
    }
}

/// Retire every slot of a dying epoch, injecting exactly one `Dropped` each.
fn retire_epoch(state: &mut PeerIngress, metrics: Option<&MuxMetricsHandle>) -> usize {
    let mut closed = 0;
    for index in 0..state.slots.len() {
        if let Some(mut slot) = state.slots[index].take() {
            if let Some(metrics) = metrics
                && slot.held() > 0
            {
                metrics.held_records_delta(-(slot.held() as i64));
            }
            slot.inject_dropped();
            closed += 1;
        }
    }
    state.slots.clear();
    state.peer_bytes = ByteBudget::new(state.peer_bytes.limit());
    closed
}

fn collect_grants(state: &mut PeerIngress, replies: &mut Vec<ReplyRecord>) {
    for entry in &mut state.slots {
        let Some(slot) = entry.as_mut() else {
            continue;
        };
        slot.reconcile();
        if let Some(delta) = slot.take_grant() {
            replies.push(ReplyRecord::CreditUpdate {
                slot: slot.id,
                delta,
            });
        }
    }
}

/// Take a lock, ignoring poisoning.
///
/// The critical section is a slot-table walk with no user code in it, so a
/// poisoned lock means a panic elsewhere rather than torn state; propagating it
/// would take down every stream from the peer.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
