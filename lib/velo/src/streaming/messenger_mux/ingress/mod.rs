// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The batch handlers' body — the receive side of the mux.
//!
//! Each lane's handler (`_stream_batch`, `_stream_batch.1`, ...) is registered
//! with **ordered per-sender dispatch**, so batches from one peer on one mux
//! lane are handled by one task, in arrival order.
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

mod dirty;
mod drain;
mod epoch;
mod reconcile;
mod slot;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use velo_ext::WorkerId;

pub(crate) use self::dirty::DirtySlots;
pub(crate) use self::drain::DrainSignal;
use self::epoch::{accept_epoch, note_batch_seq, retire_epoch};
use self::reconcile::{collect_grants, collect_touched_grants, list_drained_slots};
use self::slot::{Applied, IngressSlot, LiveCounts, heartbeat_frame};
use super::flow_control::SharedByteBudget;
use super::lane_choice::{LaneCounts, LaneReservation, PeerLaneCounts};
use super::peer_batcher::ReplyRecord;
use super::protocol::{BatchDecoder, BatchHeader, CloseReason, Record, RecordBody, SlotId};
use super::{LaneIndex, MuxConfig, PeerLane};
use crate::observability::{MuxDirection, MuxDropReason, MuxMetricsHandle};

/// Ceiling on the dense slot table one (peer, lane) may make this node
/// allocate.
///
/// A sender allocates from a free list starting at zero, so its indices stay
/// within a small multiple of its live slot count; a jump past this is a
/// misbehaving or hostile peer sizing a `Vec` on this node from a wire field.
/// 64 Ki is two orders of magnitude above any real fan-in — a decode engine's
/// 1024 concurrent streams to one peer use indices 0..1024 — and keeps the
/// worst case one `OpenSlot` can force to a few megabytes rather than a few
/// hundred, which is the same amplification the batch decoder refuses.
///
/// Every lane's table gets the whole range, so the worst case one peer can
/// force is [`MAX_LANES`](super::lane::MAX_LANES) tables of it. The range is
/// not split over the lanes, because the sender picks the lane a stream rides
/// and this node cannot know how the sender's streams will fall: a sender with
/// one lane puts all of them in one table, and a share sized from this node's
/// lane count (4 Ki at 16 lanes) refused streams the sender had every right to
/// open.
pub(crate) const MAX_INGRESS_SLOTS_PER_PEER: usize = 1 << 16;

/// A `bind()` waiting for the `OpenSlot` that will claim it.
struct BindEntry {
    /// The mux-owned `C + 1` buffer whose receiver went to the anchor.
    frame_tx: flume::Sender<Vec<u8>>,
    /// Handed to the anchor's direct feed and stream watchdog at attach (or to
    /// `mpsc_reader_pump` for an MPSC anchor); told which peer it belongs to
    /// here, when an `OpenSlot` claims this bind.
    drain: Arc<DrainSignal>,
    /// The lane the consumer placed this bind on, counted toward that lane's
    /// load until the bind leaves this table by any path. A claim hands the
    /// count over to the slot's own, on the lane the `OpenSlot` arrived on.
    _lane: LaneReservation,
}

impl Drop for BindEntry {
    /// A bind leaving the table unclaimed — released, expired, refused or
    /// torn down — closes its buffer for good. A claimed one lives on as an
    /// `IngressSlot`, which closes it when it retires.
    fn drop(&mut self) {
        if self.drain.claimed().is_none() {
            self.drain.close();
        }
    }
}

/// Registry of binds and per-peer slot tables.
#[derive(Default)]
pub(crate) struct IngressRegistry {
    /// `(anchor_id, session_id)` → the buffer a matching `OpenSlot` claims.
    binds: DashMap<(u64, u64), BindEntry>,
    /// Slot tables, one per (peer, lane). One `Mutex` per table, uncontended
    /// in steady state because the lane's ordered handler is its only writer;
    /// the credit sweep is the sole other visitor.
    peers: DashMap<PeerLane, Mutex<PeerIngress>>,
    /// Calls into `close_consumer_gone`, each of which takes a peer's lock.
    #[cfg(test)]
    consumer_gone_calls: std::sync::atomic::AtomicUsize,
    /// Per-table "a credit-return visit is already queued" flags, read and set
    /// by draining consumers without taking the table's mutex. See
    /// [`DrainSignal`].
    ///
    /// **Grows with distinct (peer, lane)s and is never pruned**, which mirrors
    /// `peers` above and costs a pointer and a bool per (peer, lane) this node
    /// has ever received a slot on. Removing an entry is not a matter of picking a moment: a
    /// claimed [`DrainSignal`] holds its peer's flag as an `Arc` for the life of
    /// its stream, so a removal while any such stream lives leaves its consumer
    /// setting a flag nothing reads — permanently true, permanently
    /// coalescing, and that peer's credit falls back to the periodic sweep for
    /// the rest of the stream. So it may
    /// only be removed under the same visibility that retires slots and binds,
    /// and until that is worth building, unbounded-but-tiny is the honest trade.
    drain_pending: DashMap<PeerLane, Arc<AtomicBool>>,
    /// Live slots per lane, summed over every peer.
    ///
    /// Each [`IngressSlot`] holds one count on its arrival lane, so no retire
    /// path can forget it. A pre-bind does not know its peer and reads this to
    /// place its stream; summing the tables instead would lock every
    /// (peer, lane) table on a frontend's per-request path.
    live: LaneCounts,
    /// Live slots per (peer, lane), held by each [`IngressSlot`] like `live`.
    ///
    /// What an unkeyed attach reads to place its stream. Reading the tables
    /// instead would wait on the mutex the ordered batch handler holds through
    /// a whole batch. [`live_slots`](Self::live_slots) stays the exact answer
    /// for the one reader that needs it, batcher eviction.
    live_per_peer: PeerLaneCounts,
    /// One byte budget per peer, shared by all of that peer's lane tables.
    ///
    /// Shared rather than split, so the peer's bound is `peer_byte_budget`
    /// whatever lane count either side keeps and whenever each table was
    /// made. Grows with distinct peers and is never pruned, like `peers`.
    peer_bytes: DashMap<WorkerId, Arc<SharedByteBudget>>,
}

/// Receive-side state for one (peer, lane).
///
/// Per lane, not per peer, because everything here depends on order and order
/// holds only within a lane: the epoch and `batch_seq` a lane's batcher
/// stamps, and slot ids, which are unique only within one batcher.
struct PeerIngress {
    /// The sender epoch this table belongs to. `None` until the first batch.
    epoch: Option<u64>,
    last_batch_seq: Option<u32>,
    slots: Vec<Option<IngressSlot>>,
    /// The peer's byte budget, shared with its other lanes' tables.
    peer_bytes: Arc<SharedByteBudget>,
    /// This (peer, lane)'s live-slot count, taken once here so opening a
    /// slot does not look it up in a map while this table's mutex is held.
    live: Arc<AtomicUsize>,
    /// Slot indexes the pass being run must reconcile, in arrival order.
    ///
    /// Scratch, reused across passes so the steady state allocates nothing: a
    /// batch pushes the few indexes it delivered into, [`list_drained_slots`]
    /// adds the ones a consumer listed in the dirty set, and
    /// [`collect_touched_grants`] drains the list, keeping the capacity. It is
    /// meant to be empty whenever the peer's mutex is free, which is what
    /// makes [`IngressSlot::mark_touched`]'s flag mean "already listed for the
    /// pass in flight" and nothing wider — a panic between a push and the
    /// drain can leave a stale entry here instead, and it self-heals on the
    /// next pass that visits it (see the flag's own doc on [`IngressSlot`]).
    touched: Vec<u32>,
    /// The peer's dirty-slot set: slots a draining consumer listed, waiting
    /// for the next pass to reconcile them.
    ///
    /// Shared, not behind this mutex: every [`DrainSignal`] this peer's slots
    /// claimed holds a clone and lists into it from its own task, per record,
    /// with no lock. The readers are the passes that already hold this mutex —
    /// [`list_drained_slots`] and [`collect_grants`], one per pass — so a take
    /// never races another take. A listing that lands after a pass took the
    /// set is the next pass's; it cannot misplace or lose credit, because the
    /// quantity lives in the slot's own [`DrainSignal`] and not in the set.
    /// It cannot be full, so no drain ever gives up its listing.
    dirty: Arc<DirtySlots>,
    /// Reconcile visits this peer's slots have taken. Counts exactly what the
    /// narrowed scope removes — one slot visited under this mutex — which no
    /// reply or ledger value reveals, because a visit that finds nothing is
    /// indistinguishable from a visit that never happened.
    #[cfg(test)]
    reconcile_visits: u64,
}

impl PeerIngress {
    fn new(peer_bytes: Arc<SharedByteBudget>, live: Arc<AtomicUsize>) -> Self {
        Self {
            epoch: None,
            last_batch_seq: None,
            slots: Vec::new(),
            peer_bytes,
            live,
            touched: Vec::new(),
            dirty: Arc::new(DirtySlots::new()),
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
    pub(crate) peer_stops: Vec<(SlotId, u64, bool)>,
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
    /// Whose batch this is, and on which lane. Carried so `open_slot` can
    /// tell the bind's [`DrainSignal`] which (peer, lane) it turned out to
    /// belong to.
    key: PeerLane,
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
        key: PeerLane,
        id: SlotId,
        metrics: Option<&MuxMetricsHandle>,
        session_id: Option<u64>,
    ) -> Option<ReplyRecord> {
        #[cfg(test)]
        self.consumer_gone_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = self.peers.get(&key)?;
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
        let live = state.slots[id.index() as usize].as_ref()?;
        if session_id.is_some_and(|session| live.session_id != session) {
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
        (outcome.closed > 0).then_some(match session_id {
            Some(session_id) => ReplyRecord::LifecycleSlot {
                slot: id,
                session_id,
                cancel: true,
            },
            None => ReplyRecord::CloseSlot {
                slot: id,
                reason: CloseReason::UnknownSlot,
            },
        })
    }

    /// This (peer, lane)'s pending-wake flag, created on first use.
    ///
    /// Lives on the registry rather than in `PeerIngress` so a draining consumer
    /// can reach it without taking the table's mutex — taking that mutex per
    /// record is the cost this whole change exists to avoid.
    pub(crate) fn pending_wake(&self, key: PeerLane) -> Arc<AtomicBool> {
        Arc::clone(
            self.drain_pending
                .entry(key)
                .or_insert_with(|| Arc::new(AtomicBool::new(false)))
                .value(),
        )
    }

    /// Take this peer's wake down, so drains landing during the visit post a
    /// fresh one rather than being swallowed by it.
    ///
    /// A `swap`, not a store. A drain lists its slot and then `swap`s the flag
    /// up; if this clear were a plain store, it could land after that set with
    /// nothing ordering the drain's listing before this visit's take, so the
    /// take could miss the listing and the drain's wake would be gone with the
    /// flag -- the slot then waits for the periodic tick. As an `AcqRel` RMW
    /// the clear either comes before the drain's set, and the drain posts a
    /// wake, or after it and acquires it, and then the take below sees the
    /// listing.
    pub(crate) fn clear_pending_wake(&self, key: PeerLane) {
        if let Some(flag) = self.drain_pending.get(&key) {
            flag.swap(false, Ordering::AcqRel);
        }
    }

    /// Register the buffer a `bind()` created, keyed by `(anchor, session)`,
    /// with the lane the consumer placed it on.
    pub(crate) fn register_bind(
        &self,
        anchor_id: u64,
        session_id: u64,
        frame_tx: flume::Sender<Vec<u8>>,
        drain: Arc<DrainSignal>,
        lane: LaneReservation,
    ) {
        self.binds.insert(
            (anchor_id, session_id),
            BindEntry {
                frame_tx,
                drain,
                _lane: lane,
            },
        );
    }

    /// Drop an unclaimed bind, reporting whether one was there.
    pub(crate) fn expire_bind(&self, anchor_id: u64, session_id: u64) -> bool {
        self.binds.remove(&(anchor_id, session_id)).is_some()
    }

    /// `peer`'s byte budget, created at `limit` on first use.
    fn peer_budget(&self, peer: WorkerId, limit: u64) -> Arc<SharedByteBudget> {
        Arc::clone(
            self.peer_bytes
                .entry(peer)
                .or_insert_with(|| Arc::new(SharedByteBudget::new(limit)))
                .value(),
        )
    }

    /// Live receive-side slots for one (peer, lane), counted by walking its
    /// table under the table's mutex. Exact, and it waits behind a batch in
    /// the ordered handler; [`live_count`](Self::live_count) does neither.
    pub(crate) fn live_slots(&self, key: PeerLane) -> usize {
        self.peers
            .get(&key)
            .map_or(0, |entry| lock(entry.value()).live())
    }

    /// Live receive-side slots on `lane`, from every peer.
    pub(crate) fn live_on_lane(&self, lane: LaneIndex) -> usize {
        self.live.get(lane)
    }

    /// Live receive-side slots for one (peer, lane), read from an atomic and
    /// taking no table's mutex.
    ///
    /// A slot counts from the moment its `OpenSlot` is applied until it
    /// drops, so a reader racing a batch may see it a moment early or late.
    pub(crate) fn live_count(&self, key: PeerLane) -> usize {
        self.live_per_peer.get(key)
    }

    /// Every (peer, lane) with receive-side state, for the credit sweep.
    pub(crate) fn peers(&self) -> Vec<PeerLane> {
        self.peers.iter().map(|entry| *entry.key()).collect()
    }

    /// Reconcile every slot of `key` and collect the credit now returnable.
    ///
    /// The periodic sweep's walk, and the only path that visits a slot nobody
    /// named. It is load-bearing rather than a backstop for one case: a peer
    /// whose only slot has parked out of credit sends nothing more, so no
    /// further batch arrives to drive reconciliation on the arrival path, and
    /// without this the pair deadlocks with the consumer drained and the sender
    /// parked.
    ///
    /// A visit is now an atomic swap per slot rather than a slot-channel length
    /// read, so what this walk costs is bounded by the tick's own interval.
    ///
    /// [`sweep_drained`]: Self::sweep_drained
    pub(crate) fn sweep_credit(&self, key: PeerLane) -> Vec<ReplyRecord> {
        let Some(entry) = self.peers.get(&key) else {
            return Vec::new();
        };
        let mut state = lock(entry.value());
        let mut replies = Vec::new();
        collect_grants(&mut state, &mut replies);
        replies
    }

    /// Reconcile the slots of `key` that a consumer listed in the dirty set.
    ///
    /// The drain doorbell's visit. It answers a wake, and a wake means some
    /// slot of this peer drained — the set says which, so the walk is over
    /// those and not over the peer's whole table. That matters because this
    /// runs under the same mutex the inbound batch path takes, at up to one
    /// visit per
    /// [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor)
    /// per (peer, lane).
    pub(crate) fn sweep_drained(&self, key: PeerLane) -> Vec<ReplyRecord> {
        let Some(entry) = self.peers.get(&key) else {
            return Vec::new();
        };
        let mut state = lock(entry.value());
        let mut replies = Vec::new();
        list_drained_slots(&mut state);
        collect_touched_grants(&mut state, &mut replies);
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

/// Handle one batch from one (peer, lane).
///
/// `key.lane` is the lane of the handler the batch arrived on. A batch whose
/// header names another lane is dropped whole: a sender stamps the lane it
/// sends on, so a mismatch is a sender bug or a corrupt batch, and its slot
/// ids belong to another lane's batcher, where applying them would retire or
/// feed the wrong streams.
///
/// Returns the replies to send back, the records addressed to our own egress
/// slots, and the slot-count deltas the caller feeds the `live_slots` gauge.
/// Never blocks, never awaits.
pub(crate) fn handle_batch(
    registry: &IngressRegistry,
    config: &MuxConfig,
    metrics: Option<&MuxMetricsHandle>,
    key: PeerLane,
    payload: &Bytes,
) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();

    let header = match BatchHeader::decode(payload) {
        Ok(header) => header,
        Err(error) => {
            tracing::warn!(
                peer = %key.peer,
                lane = %key.lane,
                %error,
                "messenger mux: undecodable batch header"
            );
            return outcome;
        }
    };

    // Before the table lookup, so a batch on the wrong lane cannot create a
    // table for a lane the peer never used.
    if header.lane() != key.lane.get() {
        tracing::warn!(
            peer = %key.peer,
            lane = %key.lane,
            header_lane = header.lane(),
            "messenger mux: batch header names another lane; dropped"
        );
        if let Some(metrics) = metrics {
            metrics.records_dropped(MuxDropReason::LaneMismatch, u64::from(header.record_count));
        }
        return outcome;
    }

    if !registry.peers.contains_key(&key) {
        let peer_bytes = registry.peer_budget(key.peer, config.peer_byte_budget);
        let live = registry.live_per_peer.counter(key);
        registry
            .peers
            .entry(key)
            .or_insert_with(|| Mutex::new(PeerIngress::new(peer_bytes, live)));
    }
    // A read guard, not `entry`'s write guard: the `Mutex` inside already
    // serialises writers, and holding the shard for writing would block the
    // credit sweep on an unrelated peer in the same shard.
    let Some(entry) = registry.peers.get(&key) else {
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
            tracing::warn!(
                peer = %key.peer,
                lane = %key.lane,
                %error,
                "messenger mux: undecodable batch"
            );
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
        key,
    };
    for decoded in decoder {
        match decoded {
            Ok(record) => apply_record(&mut state, &ctx, &record, &mut outcome),
            Err(error) => {
                tracing::warn!(
                    peer = %key.peer,
                    lane = %key.lane,
                    %error,
                    "messenger mux: malformed record; the rest of the batch is skipped"
                );
                break;
            }
        }
    }

    // Reconcile the slots this batch delivered into *and* the slots a
    // consumer listed in the dirty set, and no others. Both sets are proportional to
    // what moved; the whole-table walk they replace read one slot buffer's
    // length — under that channel's lock — per live slot per batch, so a peer
    // holding a thousand slots paid a thousand lock acquisitions for the eleven
    // a batch delivers into at the measured serving shape, whatever the load.
    //
    // The set is why this pass carries the drained slots too, and leaving them
    // to the doorbell was measured and is not an option: the doorbell is a
    // per-peer, rate-limited, single-task walk, and every stream in the serving
    // shape sent about four records more than its then-default 256-record
    // window, so the tail of every stream waited on it. `docs/src/development/batched-streaming-design.md`
    // has the numbers for why the arrival path also returns the credit of
    // every slot that drained. Credit still does not come back *from* the
    // consumer: releasing there means taking this peer's mutex per record.
    list_drained_slots(&mut state);
    collect_touched_grants(&mut state, &mut outcome.replies);
    outcome
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
        RecordBody::LifecycleSlot { session_id, cancel } => {
            outcome.peer_stops.push((record.slot, session_id, cancel))
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
    let lifecycle = bind.drain.claimed_by(
        ctx.key,
        id,
        ctx.registry.pending_wake(ctx.key),
        Arc::clone(&state.dirty),
    );

    // Counted before `bind` drops at the end of this function, so the stream
    // is counted twice for a moment rather than not at all. On the arrival
    // lane, not the lane the bind was placed on: a sender with fewer lanes
    // clamps, and the arrival lane is where the stream's load is.
    let mut slot = IngressSlot::new(
        id,
        LiveCounts::new(
            ctx.registry.live.take(ctx.key.lane),
            LaneReservation::take(ctx.key.lane, Arc::clone(&state.live)),
        ),
        bind.frame_tx.clone(),
        Arc::clone(&bind.drain),
        ctx.config.initial_credit,
        ctx.config.slot_byte_budget,
        record.frame_seq.saturating_add(1),
    );
    slot.session_id = session_id;
    state.slots[index] = Some(slot);
    outcome.opened += 1;
    if lifecycle == 2 {
        finish_close(state, id, CloseReason::UnknownSlot, ctx.metrics, outcome);
        outcome.replies.push(ReplyRecord::LifecycleSlot {
            slot: id,
            session_id,
            cancel: true,
        });
    } else if lifecycle == 1 {
        outcome.replies.push(ReplyRecord::LifecycleSlot {
            slot: id,
            session_id,
            cancel: false,
        });
    }
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
    // Dropping the slot drops the mux-side sender and fires the drain signal's
    // `closed`: the consumer's feed ends as a receiver does when a socket
    // closes, the stream watchdog exits, and an MPSC pump takes its `Err`
    // branch.
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
    let peer_bytes = &*state.peer_bytes;
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
