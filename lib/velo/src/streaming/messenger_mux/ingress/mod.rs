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

mod drain;
mod reconcile;
mod slot;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use velo_ext::WorkerId;

pub(crate) use self::drain::DrainSignal;
use self::reconcile::{collect_grants, collect_touched_grants, list_drained_slots};
use self::slot::{Applied, IngressSlot, heartbeat_frame};
use super::MuxConfig;
use super::flow_control::ByteBudget;
use super::peer_batcher::ReplyRecord;
use super::protocol::{
    BatchDecoder, BatchHeader, CloseReason, Record, RecordBody, SlotId, batch_seq_gap,
    batch_seq_is_newer,
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
pub(crate) const MAX_INGRESS_SLOTS_PER_PEER: usize = 1 << 16;

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
    /// Slot indexes the pass being run must reconcile, in arrival order.
    ///
    /// Scratch, reused across passes so the steady state allocates nothing: a
    /// batch pushes the few indexes it delivered into, [`list_drained_slots`]
    /// adds the ones the pump named on the dirty lane, and
    /// [`collect_touched_grants`] drains the list, keeping the capacity. It is
    /// meant to be empty whenever the peer's mutex is free, which is what
    /// makes [`IngressSlot::mark_touched`]'s flag mean "already listed for the
    /// pass in flight" and nothing wider — a panic between a push and the
    /// drain can leave a stale entry here instead, and it self-heals on the
    /// next pass that visits it (see the flag's own doc on [`IngressSlot`]).
    touched: Vec<u32>,
    /// The peer's dirty-slot lane: indexes a draining pump named, waiting for
    /// the next pass to reconcile them.
    ///
    /// Both ends live here rather than beside `drain_pending` on the registry
    /// because everything that touches either end already holds this mutex —
    /// [`open_slot`] hands the sender to the claiming slot's [`DrainSignal`],
    /// and [`list_drained_slots`] and [`collect_grants`] are the readers, one
    /// per pass. A pump reaches its clone of the sender through the signal it
    /// already owns, so nothing needs the lane without the lock and nothing
    /// needs the lock to list.
    ///
    /// Capacity is [`MAX_INGRESS_SLOTS_PER_PEER`] — a bound on live slots,
    /// not a promise that a slot never has more than one entry outstanding.
    /// The arrival path and the doorbell both drain this lane in
    /// [`list_drained_slots`] *before* [`collect_touched_grants`] clears any
    /// slot's `listed` flag, and `collect_grants` drains it itself before its
    /// own walk clears every flag it visits — in every case, a drain racing
    /// between that lane drain and the flag clear relists a slot the walk is
    /// about to (or just did) clear, leaving a stale entry for the next pass.
    /// That entry costs its slot one visit that reads whatever that slot's
    /// own [`DrainSignal`] has accumulated since — it cannot misplace or lose
    /// credit, because the quantity lives there and not in the lane — and it
    /// cannot survive whichever pass visits this peer next, since every pass
    /// drains the lane before it reconciles.
    /// `flume` allocates the queue as it fills, so an idle peer pays nothing
    /// for it.
    drained_tx: flume::Sender<u32>,
    drained_rx: flume::Receiver<u32>,
    /// Reconcile visits this peer's slots have taken. Counts exactly what the
    /// narrowed scope removes — one slot visited under this mutex — which no
    /// reply or ledger value reveals, because a visit that finds nothing is
    /// indistinguishable from a visit that never happened.
    #[cfg(test)]
    reconcile_visits: u64,
}

impl PeerIngress {
    fn new(peer_byte_budget: u64) -> Self {
        let (drained_tx, drained_rx) = flume::bounded(MAX_INGRESS_SLOTS_PER_PEER);
        Self {
            epoch: None,
            last_batch_seq: None,
            slots: Vec::new(),
            peer_bytes: ByteBudget::new(peer_byte_budget),
            touched: Vec::new(),
            drained_tx,
            drained_rx,
            #[cfg(test)]
            reconcile_visits: 0,
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

impl IngressRegistry {
    /// Retire a live slot whose consumer has gone, reporting the close its
    /// owner is owed.
    ///
    /// The same verdict the arrival path reaches for a slot whose consumer
    /// dropped the receiver — `CloseReason::UnknownSlot`, "this side has
    /// nowhere to put your records", the answer `fault_reason` gives for
    /// `ConsumerGone` — on a trigger that does not need a record to arrive.
    ///
    /// `None` when no such slot is there, which is what makes it idempotent:
    /// a second call finds the table entry gone and asks for nothing.
    pub(crate) fn close_consumer_gone(
        &self,
        peer: WorkerId,
        id: SlotId,
        metrics: Option<&MuxMetricsHandle>,
    ) -> Option<ReplyRecord> {
        let entry = self.peers.get(&peer)?;
        let mut state = lock(entry.value());
        // Generation-checked, because `finish_close` is not: it takes the slot
        // by dense index alone, and the peer may have recycled this one under a
        // new generation since the caller learned the id. Retiring by index
        // would then kill whichever healthy stream holds it now.
        if state
            .slots
            .get(id.index() as usize)
            .and_then(Option::as_ref)
            .map(|slot| slot.id)
            != Some(id)
        {
            return None;
        }
        let mut outcome = BatchOutcome::default();
        finish_close(
            &mut state,
            id,
            CloseReason::UnknownSlot,
            metrics,
            &mut outcome,
        );
        (outcome.closed > 0).then_some(ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::UnknownSlot,
        })
    }

    /// Binds registered and neither claimed nor released.
    #[cfg(test)]
    pub(crate) fn bind_count(&self) -> usize {
        self.binds.len()
    }

    /// The window one of `peer`'s live slots opened holding.
    #[cfg(test)]
    pub(crate) fn slot_open_terms(&self, peer: WorkerId, id: SlotId) -> Option<(u32, u64)> {
        let entry = self.peers.get(&peer)?;
        let state = lock(entry.value());
        state
            .slots
            .get(id.index() as usize)
            .and_then(Option::as_ref)
            .filter(|slot| slot.id == id)
            .map(|slot| slot.open_terms())
    }

    /// The ids of `peer`'s live slots.
    #[cfg(test)]
    pub(crate) fn live_slot_ids(&self, peer: WorkerId) -> Vec<SlotId> {
        self.peers.get(&peer).map_or_else(Vec::new, |entry| {
            lock(entry.value())
                .slots
                .iter()
                .filter_map(|slot| slot.as_ref().map(|slot| slot.id))
                .collect()
        })
    }

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

    /// Reconcile visits `peer`'s slots have taken since its table opened.
    #[cfg(test)]
    pub(crate) fn reconcile_visits(&self, peer: WorkerId) -> u64 {
        self.peers
            .get(&peer)
            .map_or(0, |entry| lock(entry.value()).reconcile_visits)
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
    /// The periodic sweep's walk, and the only path that visits a slot nobody
    /// named. It is load-bearing rather than a backstop for one case: a peer
    /// whose only slot has parked out of credit sends nothing more, so no
    /// further batch arrives to drive reconciliation on the arrival path, and
    /// without this the pair deadlocks with the consumer drained and the sender
    /// parked. It is the backstop for one more: a drain whose listing found the
    /// dirty lane full, which neither [`handle_batch`] nor [`sweep_drained`]
    /// can see.
    ///
    /// A visit is now an atomic swap per slot rather than a slot-channel length
    /// read, so what this walk costs is bounded by the tick's own interval.
    ///
    /// [`sweep_drained`]: Self::sweep_drained
    pub(crate) fn sweep_credit(&self, peer: WorkerId) -> Vec<ReplyRecord> {
        let Some(entry) = self.peers.get(&peer) else {
            return Vec::new();
        };
        let mut state = lock(entry.value());
        let mut replies = Vec::new();
        collect_grants(&mut state, &mut replies);
        replies
    }

    /// Reconcile the slots of `peer` that a pump named on the dirty lane.
    ///
    /// The drain doorbell's visit. It answers a wake, and a wake means some
    /// slot of this peer drained — the lane says which, so the walk is over
    /// those and not over the peer's whole table. That matters because this
    /// runs under the same mutex the inbound batch path takes, at up to one
    /// visit per
    /// [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor)
    /// per peer.
    pub(crate) fn sweep_drained(&self, peer: WorkerId) -> Vec<ReplyRecord> {
        let Some(entry) = self.peers.get(&peer) else {
            return Vec::new();
        };
        let mut state = lock(entry.value());
        let mut replies = Vec::new();
        list_drained_slots(&mut state);
        collect_touched_grants(&mut state, &mut replies);
        replies
    }

    /// Both ends of `peer`'s dirty-slot lane, for the test that fills it.
    #[cfg(test)]
    pub(crate) fn drained_lane(
        &self,
        peer: WorkerId,
    ) -> (flume::Sender<u32>, flume::Receiver<u32>) {
        let entry = self.peers.get(&peer).expect("peer has a slot table");
        let state = lock(entry.value());
        (state.drained_tx.clone(), state.drained_rx.clone())
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

    // Reconcile the slots this batch delivered into *and* the slots a pump
    // named on the dirty lane, and no others. Both sets are proportional to
    // what moved; the whole-table walk they replace read one slot buffer's
    // length — under that channel's lock — per live slot per batch, so a peer
    // holding a thousand slots paid a thousand lock acquisitions for the eleven
    // a batch delivers into at the measured serving shape, whatever the load.
    //
    // The lane is why this pass carries the drained slots too, and leaving them
    // to the doorbell was measured and is not an option: the doorbell is a
    // per-peer, rate-limited, single-task walk, and every stream in the serving
    // shape sends about four records more than its initial window, so the tail
    // of every stream waited on it. `BATCHING.md` § "Addendum, 2026-09-05: the
    // arrival path also returns the credit of every slot that drained" has the
    // numbers. Credit still does not come back *from* the pump, for the reason
    // the 2026-09-01 addendum gives under "The pump rings a doorbell; it does
    // not release credit": releasing there means taking this peer's mutex per
    // record.
    list_drained_slots(&mut state);
    collect_touched_grants(&mut state, &mut outcome.replies);
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

/// Meter the batch's sequence against the newest one seen from this peer.
///
/// The mark only moves forward. A batch behind it — a duplicate, or one that
/// arrived after its successor — is not a gap and does not move the mark.
/// Metering it would add the wrapped difference, near `u32::MAX`, to a counter
/// that means "batches missing", and moving the mark back would count its
/// successor's gap a second time when the sequence resumes past it. A detached
/// open under `MuxConfig::async_open_ack` can invert a pair this way (see
/// `Batcher::open_detached` in `peer_batcher`); what the meter reports for
/// one is the single batch its later half looked like when it arrived first,
/// and nothing more.
fn note_batch_seq(
    state: &mut PeerIngress,
    header: &BatchHeader,
    metrics: Option<&MuxMetricsHandle>,
) {
    let received = header.batch_seq;
    if let Some(last) = state.last_batch_seq {
        if !batch_seq_is_newer(received, last) {
            return;
        }
        let gap = batch_seq_gap(last.wrapping_add(1), received);
        if gap > 0
            && let Some(metrics) = metrics
        {
            metrics.batch_seq_gap(gap);
        }
    }
    state.last_batch_seq = Some(received);
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
        // Never held and never will be: this index is out of the table's
        // range entirely, so the reject lane carries it, not `peers`.
        outcome.replies.push(ReplyRecord::RejectSlot {
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
        // The incumbent, not `id`, occupies this index — `id` itself names no
        // entry in the table, so it goes to the reject lane.
        outcome.replies.push(ReplyRecord::RejectSlot {
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
        // and discard that slot's records. This `OpenSlot` was never admitted
        // — that is what puts it in the reject lane, not that `id` is absent
        // from the table: a same-id duplicate passes the collision guard
        // above and can reach here with `id` still the live incumbent. That
        // incumbent survives only up to this reply's delivery: once the
        // sender's `on_peer_closed` acts on it, it closes its own live slot
        // for `id` with no reply of its own, leaving this incumbent running
        // with no producer behind it. Pre-existing, unchanged by this lane's
        // split from `CloseSlot`.
        outcome.replies.push(ReplyRecord::RejectSlot {
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
    // The bind now has an owner, so its drain signal can start counting and
    // posting. Before this point it is inert: nothing has been delivered on
    // this slot, so nothing can have drained.
    bind.drain.claimed_by(
        ctx.peer,
        id,
        ctx.registry.pending_wake(ctx.peer),
        state.drained_tx.clone(),
    );

    let slot = IngressSlot::new(
        id,
        bind.frame_tx,
        Arc::clone(&bind.drain),
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
    let touched = &mut state.touched;
    let Some(slot) = state.slots[index].as_mut() else {
        return;
    };
    // Marked before the record is applied rather than after, so any record
    // that reaches the slot counts, not only the ones that spend credit: one
    // that parks in the reorder hold has spent credit that only a reconcile
    // gives back and may release the whole hold later in this same batch,
    // while a duplicate spends none — marking it anyway costs at worst one
    // visit that takes a drain count of zero and returns.
    if slot.mark_touched() {
        touched.push(id.index());
    }
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
    // The entries here name slots of the epoch being retired. On the ordinary
    // path the list is already empty at this point — the epoch check runs
    // before any record is applied, and `shutdown` runs with no batch in
    // flight — so this clears the poison path `touched`'s own doc names (a
    // panic between a push and the drain), plus any future caller that
    // retires mid-batch. The table is cleared and regrows from index zero, so
    // a left-behind entry would send the next pass to whatever slot takes that
    // index back — a reconcile of a slot neither the batch nor a pump named.
    // Clearing keeps every entry meaning what the pass assumes it means.
    //
    // The dirty lane gets no matching clear. A retired index's own pump can
    // still post to it — draining what was already in the C + 1 buffer at
    // close — and that entry outlives this function with nothing here to name
    // it. Left alone, it costs whatever visits the index next one empty visit
    // (nothing, if the index stays closed) or one spurious visit of a
    // replacement (harmless per `collect_touched_grants`'s doc: the visit
    // reads the replacement's own count, whatever its own pump has drained
    // since, and that count is always its own — `bind` makes one
    // `DrainSignal` per bind and `open_slot` claims it, so it can never be the
    // retired slot's). `collect_grants`'s periodic walk also drains the lane
    // outright, so such an entry cannot outlive one tick.
    state.touched.clear();
    state.peer_bytes = ByteBudget::new(state.peer_bytes.limit());
    closed
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
