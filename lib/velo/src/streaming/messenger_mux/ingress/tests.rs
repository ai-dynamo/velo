// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ingress unit tests.
//!
//! `handle_batch` is a pure function over an [`IngressRegistry`], so every
//! receive-side property is testable without a messenger, a runtime, or a
//! socket — the batch bytes go in and the replies come out.

use std::collections::BTreeMap;

use bytes::Bytes;
use velo_ext::WorkerId;

use super::*;
use crate::streaming::messenger_mux::LaneIndex;
use crate::streaming::messenger_mux::protocol::{BatchEncoder, RecordType, SlotId};
use crate::streaming::sender::{cached_dropped, cached_finalized};

/// Test-only views into the registry. Here rather than beside the registry,
/// so the receive path's file holds only the receive path.
impl IngressRegistry {
    /// Binds registered and neither claimed nor released.
    pub(crate) fn bind_count(&self) -> usize {
        self.binds.len()
    }

    /// The window one of `key`'s live slots opened holding.
    pub(crate) fn slot_open_terms(&self, key: PeerLane, id: SlotId) -> Option<(u32, u64)> {
        let entry = self.peers.get(&key)?;
        let state = lock(entry.value());
        state
            .slots
            .get(id.index() as usize)
            .and_then(Option::as_ref)
            .filter(|slot| slot.id == id)
            .map(|slot| slot.open_terms())
    }

    /// The ids of `key`'s live slots.
    pub(crate) fn live_slot_ids(&self, key: PeerLane) -> Vec<SlotId> {
        self.peers.get(&key).map_or_else(Vec::new, |entry| {
            lock(entry.value())
                .slots
                .iter()
                .filter_map(|slot| slot.as_ref().map(|slot| slot.id))
                .collect()
        })
    }

    /// Calls into `close_consumer_gone` so far.
    pub(crate) fn consumer_gone_calls(&self) -> usize {
        self.consumer_gone_calls
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Bytes `peer`'s ahead-of-sequence holds have reserved between them, on
    /// every lane.
    pub(crate) fn peer_bytes_used(&self, peer: WorkerId) -> u64 {
        self.peer_bytes.get(&peer).map_or(0, |budget| budget.used())
    }

    /// Reconcile visits `key`'s slots have taken since its table opened.
    pub(crate) fn reconcile_visits(&self, key: PeerLane) -> u64 {
        self.peers
            .get(&key)
            .map_or(0, |entry| lock(entry.value()).reconcile_visits)
    }

    /// Live slots placed on `lane`, from `peer` or from every peer, counted by
    /// walking every table: the exact answer the lane counts must match.
    pub(crate) fn live_placed_on(&self, peer: Option<WorkerId>, lane: LaneIndex) -> usize {
        self.peers
            .iter()
            .filter(|entry| peer.is_none_or(|peer| entry.key().peer == peer))
            .map(|entry| {
                lock(entry.value())
                    .slots
                    .iter()
                    .flatten()
                    .filter(|slot| slot.placed_lane() == lane)
                    .count()
            })
            .sum()
    }

    /// Run `f` while holding `key`'s table mutex, as the ordered batch
    /// handler does through a decode. `None` when `key` has no table.
    #[cfg(feature = "quic")]
    pub(crate) fn with_table_locked<R>(&self, key: PeerLane, f: impl FnOnce() -> R) -> Option<R> {
        let entry = self.peers.get(&key)?;
        let _state = lock(entry.value());
        Some(f())
    }

    /// `key`'s dirty-slot set, for the tests that inspect it.
    pub(crate) fn dirty_slots(&self, key: PeerLane) -> Arc<DirtySlots> {
        let entry = self.peers.get(&key).expect("peer has a slot table");
        Arc::clone(&lock(entry.value()).dirty)
    }
}

/// A drain signal whose wakes go nowhere, for tests that drive the registry
/// directly. The claim path still runs, so `open_slot` naming the peer, the
/// slot index and the dirty set is covered; nothing consumes the wake lane
/// because these tests have no sweep task.
fn test_drain() -> Arc<DrainSignal> {
    let (tx, _rx) = flume::bounded(16);
    Arc::new(DrainSignal::new(tx))
}

/// The consumer side of one bound slot: the receiver `bind` handed the anchor,
/// and the drain signal its direct feed holds.
///
/// Both are needed because credit is returned against what the consumer
/// *counted*. Taking a frame out of `rx` without telling the signal is what a
/// gone consumer looks like, not what a draining one looks like, and
/// reconciles nothing. (`pump` is named for the reader pump that used to do
/// this for a mux bind; the `StreamAnchor` now reads the buffer itself.)
struct Consumer {
    rx: flume::Receiver<Vec<u8>>,
    drain: Arc<DrainSignal>,
}

impl Consumer {
    /// Take everything available, counting each record the way a mux-fed
    /// `StreamAnchor` does.
    fn pump(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while let Ok(frame) = self.rx.try_recv() {
            self.drain.drained();
            out.push(frame);
        }
        out
    }

    /// Take exactly `n` records, counting each.
    fn pump_n(&self, n: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let frame = self.rx.try_recv().expect("a record to take");
            self.drain.drained();
            out.push(frame);
        }
        out
    }
}

/// Register a bind for `(ANCHOR, session)` and return its consumer side.
fn register(registry: &IngressRegistry, config: &MuxConfig, session: u64) -> Consumer {
    register_on(registry, config, session, LaneIndex::ZERO)
}

/// As [`register`], with the bind placed on `lane`.
fn register_on(
    registry: &IngressRegistry,
    config: &MuxConfig,
    session: u64,
    lane: LaneIndex,
) -> Consumer {
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    let drain = test_drain();
    registry.register_bind(
        ANCHOR,
        session,
        tx,
        Arc::clone(&drain),
        LaneReservation::uncounted(lane),
    );
    Consumer { rx, drain }
}

const PEER: u64 = 0xABCD;
const ANCHOR: u64 = 7;
const SESSION: u64 = 11;

/// The peer's lane 0, where every test here runs unless it names a lane.
fn peer() -> PeerLane {
    PeerLane::new(WorkerId::from_u64(PEER), LaneIndex::ZERO)
}

fn config() -> MuxConfig {
    MuxConfig {
        initial_credit: 4,
        slot_byte_budget: 256,
        peer_byte_budget: 4096,
        ..MuxConfig::default()
    }
}

fn slot(index: u32, generation: u8) -> SlotId {
    SlotId::new(index, generation).expect("index fits u24")
}

/// Build a lane-0 batch payload from a closure that pushes its records.
fn batch(epoch: u64, batch_seq: u32, build: impl FnOnce(&mut BatchEncoder)) -> Bytes {
    batch_on(LaneIndex::ZERO, epoch, batch_seq, build)
}

/// As [`batch`], stamped with `lane`.
fn batch_on(
    lane: LaneIndex,
    epoch: u64,
    batch_seq: u32,
    build: impl FnOnce(&mut BatchEncoder),
) -> Bytes {
    let mut encoder = BatchEncoder::new(epoch, batch_seq, lane);
    build(&mut encoder);
    encoder.finish().freeze()
}

fn item(n: u8) -> Vec<u8> {
    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(n)).expect("encode item")
}

/// A registry with one bound anchor, plus that anchor's consumer side.
fn bound() -> (IngressRegistry, Consumer, MuxConfig) {
    let config = config();
    let registry = IngressRegistry::default();
    let consumer = register(&registry, &config, SESSION);
    (registry, consumer, config)
}

/// Open slot `id` at `frame_seq = 0` and return the resulting outcome.
fn open(registry: &IngressRegistry, config: &MuxConfig, id: SlotId, epoch: u64) -> BatchOutcome {
    let payload = batch(epoch, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    handle_batch(registry, config, None, peer(), &payload)
}

/// Take everything the consumer can see, without counting it on a drain
/// signal. For the tests that assert on frames rather than on credit — a
/// record taken this way is one whose consumer is gone, as far as the ledger
/// knows.
fn drain(rx: &flume::Receiver<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        out.push(frame);
    }
    out
}

// ---------------------------------------------------------------------------
// OpenSlot
// ---------------------------------------------------------------------------

#[test]
fn open_slot_claims_the_matching_bind_and_grants_no_credit() {
    let (registry, _consumer, config) = bound();
    let id = slot(0, 0);

    let outcome = open(&registry, &config, id, 1);

    assert_eq!(outcome.opened, 1);
    assert_eq!(registry.live_slots(peer()), 1);
    assert!(
        outcome.replies.is_empty(),
        "the window was advertised on the attach response and the sender \
         opened already holding it; granting it again here would hand the \
         sender 2C against a C + 1 buffer, which is the reader stall the \
         credit invariant exists to make impossible"
    );
}

#[test]
fn open_slot_for_an_unregistered_anchor_rejects_that_slot_only() {
    let (registry, _consumer, config) = bound();
    let id = slot(3, 0);

    let payload = batch(1, 0, |encoder| {
        // A pair nobody bound.
        encoder.push_open_slot(id, 0, 999, 999).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 0);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::RejectSlot {
            slot: id,
            reason: CloseReason::UnknownSlot
        }],
        "the reverse race must not fail the peer, and names no slot this side \
         ever held — the reject lane, not the held-slot close lane"
    );
    // The bind that *was* registered is untouched and still claimable.
    let outcome = open(&registry, &config, slot(0, 0), 1);
    assert_eq!(outcome.opened, 1);
}

/// An `OpenSlot` past the table's index ceiling is rejected before any table
/// lookup — it names no entry this table ever had or ever will.
#[test]
fn an_out_of_range_open_slot_is_rejected_without_touching_the_table() {
    let (registry, _consumer, config) = bound();
    let id = slot(MAX_INGRESS_SLOTS_PER_PEER as u32, 0);

    let payload = batch(1, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 0);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::RejectSlot {
            slot: id,
            reason: CloseReason::ProtocolError
        }],
        "out of range names no table entry, so it is a rejection, not a close"
    );
    // The bind is untouched: an out-of-range `OpenSlot` must not consume it.
    let outcome = open(&registry, &config, slot(0, 0), 1);
    assert_eq!(outcome.opened, 1);
}

/// A colliding `OpenSlot` is rejected; the incumbent keeps running.
///
/// The sender's free list only yields an index after that slot's `CloseSlot`,
/// so an open over a live occupant is a protocol violation however it arose.
/// Retiring the occupant to make room would kill a healthy stream silently: no
/// `Dropped` for its consumer, and its held bytes left charged to the peer
/// budget forever.
#[test]
fn a_colliding_open_slot_is_rejected_and_the_incumbent_survives() {
    let config = config();
    let registry = IngressRegistry::default();
    let depth =
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit);
    let (incumbent_tx, incumbent_rx) = flume::bounded(depth);
    registry.register_bind(
        ANCHOR,
        SESSION,
        incumbent_tx,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );
    // A second bind, for the collider to try to claim.
    let (rival_tx, rival_rx) = flume::bounded(depth);
    registry.register_bind(
        ANCHOR,
        SESSION + 1,
        rival_tx,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );

    let incumbent = slot(0, 0);
    open(&registry, &config, incumbent, 1);

    // Give the incumbent something in its ahead-of-sequence hold, so a silent
    // eviction would leak peer byte budget as well as the stream.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(incumbent, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    let held_bytes = registry.peer_bytes_used(peer().peer);
    assert!(
        held_bytes > 0,
        "the hold has to be charged for this to test anything"
    );

    // Same dense index, next generation — a live occupant is there either way.
    let collider = slot(0, 1);
    let payload = batch(1, 2, |encoder| {
        encoder
            .push_open_slot(collider, 0, ANCHOR, SESSION + 1)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 0);
    assert_eq!(outcome.closed, 0, "the incumbent must not be retired");
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::RejectSlot {
            slot: collider,
            reason: CloseReason::ProtocolError
        }],
        "the newcomer is what fails, and it is told which slot id failed; the \
         newcomer's id names no entry in the table, so it is a rejection, not \
         a close"
    );
    assert_eq!(registry.live_slots(peer()), 1);
    assert!(
        !incumbent_rx.is_disconnected(),
        "the incumbent's consumer must not see its channel end"
    );
    assert_eq!(
        registry.peer_bytes_used(peer().peer),
        held_bytes,
        "the incumbent's hold must still be charged to the peer budget — a \
         silent eviction would have leaked it for the life of the epoch"
    );

    // The incumbent still works: closing its gap delivers both records, which
    // it could not do if its hold or its byte accounting had been disturbed.
    let payload = batch(1, 3, |encoder| {
        encoder.push_data(incumbent, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(drain(&incumbent_rx), vec![item(1), item(2)]);

    // The rejected open did not consume the bind it named, so the opener that
    // is entitled to it can still claim it.
    let rightful = slot(1, 0);
    let payload = batch(1, 4, |encoder| {
        encoder
            .push_open_slot(rightful, 0, ANCHOR, SESSION + 1)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(outcome.opened, 1);
    let payload = batch(1, 5, |encoder| {
        encoder.push_data(rightful, 1, &item(9)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(drain(&rival_rx), vec![item(9)]);
}

/// A duplicate `OpenSlot` replaces the incumbent, but through the front door.
///
/// Replacement is right — the same slot id reopening is a retransmitted open,
/// not a collision — but taking the slot out directly would skip the close: the
/// consumer's channel would simply end, with no `Dropped` to say why, and the
/// incumbent's held bytes would stay charged to the peer budget for the life of
/// the epoch.
#[test]
fn a_duplicate_open_retires_the_incumbent_through_the_ordinary_close() {
    let config = config();
    let registry = IngressRegistry::default();
    let depth =
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit);
    let (first_tx, first_rx) = flume::bounded(depth);
    let (second_tx, second_rx) = flume::bounded(depth);
    registry.register_bind(
        ANCHOR,
        SESSION,
        first_tx,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );
    registry.register_bind(
        ANCHOR,
        SESSION + 1,
        second_tx,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );

    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // Something ahead of sequence, so the hold has bytes charged against the
    // peer budget that only a proper close gives back.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(registry.peer_bytes_used(peer().peer) > 0);

    // The same slot id opens again, against a different bind.
    let payload = batch(1, 2, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION + 1).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 1);
    assert_eq!(
        outcome.closed, 1,
        "the incumbent was retired, not dropped on the floor"
    );
    assert_eq!(
        registry.peer_bytes_used(peer().peer),
        0,
        "the incumbent's held bytes go back to the peer budget"
    );
    assert_eq!(
        drain(&first_rx),
        vec![cached_dropped().clone()],
        "the incumbent's consumer is told why its stream ended"
    );

    // The replacement is a working slot on the bind it named.
    let payload = batch(1, 3, |encoder| {
        encoder.push_data(id, 1, &item(9)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(drain(&second_rx), vec![item(9)]);
    assert_eq!(registry.live_slots(peer()), 1);
}

/// A same-id duplicate `OpenSlot` with no bind registered for its pair is
/// rejected without disturbing the live incumbent it names — locally, and
/// only up to this function's own return. The collision guard only fires
/// when the incoming id differs from the incumbent's
/// (`a_colliding_open_slot_is_rejected_and_the_incumbent_survives`); a
/// matching id passes it as an ordinary duplicate and falls straight to the
/// bind lookup below. When that lookup misses, the rejection this produces
/// names an id that is still live in the table right now — the reject lane is
/// not "the id was never entered", it is "this `OpenSlot` was never admitted".
///
/// Once that `RejectSlot` reply is delivered, though, the sender's
/// `on_peer_closed` closes its own live egress slot for this id with no
/// reply of its own (`close_local`, unlike `finish_close`, emits no
/// `CloseSlot`) — so the incumbent this test pins as surviving is left
/// running with no producer behind it. Pre-existing, byte-identical before
/// this lane split; not chased here.
#[test]
fn a_same_id_duplicate_with_no_bind_is_rejected_without_disturbing_the_incumbent() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);
    assert_eq!(registry.live_slots(peer()), 1);

    // The exact same id, but naming a pair nobody bound.
    let payload = batch(1, 1, |encoder| {
        encoder.push_open_slot(id, 0, 999, 999).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 0);
    assert_eq!(outcome.closed, 0, "the incumbent must not be retired");
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::RejectSlot {
            slot: id,
            reason: CloseReason::UnknownSlot
        }],
        "the rejection names the incumbent's own id, which is still live in \
         the table — the collision guard let it through because the ids \
         match, not because the slot is absent"
    );
    assert_eq!(
        registry.live_slots(peer()),
        1,
        "the incumbent survives untouched"
    );

    // The incumbent still works.
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump(), vec![item(1)]);
}

#[test]
fn records_for_a_slot_that_never_opened_are_dropped() {
    let (registry, consumer, config) = bound();

    let payload = batch(1, 0, |encoder| {
        encoder.push_data(slot(5, 0), 0, &item(1)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert!(outcome.replies.is_empty());
    assert!(consumer.pump().is_empty());
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[test]
fn data_applies_in_frame_seq_order() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for n in 0..4u8 {
            encoder
                .push_data(id, u32::from(n) + 1, &item(n))
                .expect("push data");
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    let frames = consumer.pump();
    assert_eq!(frames.len(), 4);
    for (n, frame) in frames.iter().enumerate() {
        assert_eq!(frame, &item(n as u8), "frame {n} out of order");
    }
}

#[test]
fn ahead_of_sequence_records_are_held_until_the_gap_closes() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // Seq 1 is the rendezvous singleton that has not resolved yet; 2 and 3 are
    // the eager successors that overtook it.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
        encoder.push_data(id, 3, &item(3)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(
        consumer.pump().is_empty(),
        "nothing may be delivered while the gap is open"
    );

    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    let frames = consumer.pump();
    assert_eq!(frames, vec![item(1), item(2), item(3)]);
}

/// Records waiting behind a gap are neither arrivals nor a parked sender.
///
/// The stream watchdog reads two things off the drain signal: whether
/// anything arrived for the consumer, and whether the sender holds no credit
/// and so cannot heartbeat. Records parked ahead of a gap reach neither the
/// consumer nor the buffer, and a gap that never closes -- a batch lost on its
/// way, a rendezvous payload that failed to resolve -- would stop the stream
/// for good. If held records counted as arrivals, or a window spent into the
/// hold counted as a parked sender, that stuck stream would be exempt from the
/// watchdog forever, with its slot, held bytes and peer batcher kept alive.
///
/// Control: the same window delivered in order does park the sender.
#[test]
fn records_held_behind_a_gap_do_not_keep_the_stream_alive() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // The whole window (4) spent past a gap at seq 1.
    let payload = batch(1, 1, |encoder| {
        for seq in 2..=5 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(
        consumer.pump().is_empty(),
        "the gap is open, so nothing is delivered"
    );
    assert_eq!(
        consumer.drain.arrivals(),
        0,
        "records held behind a gap never reached the consumer"
    );
    assert!(
        !consumer.drain.sender_parked(),
        "a window spent into the hold is a stuck stream, not a sender waiting on its consumer"
    );
}

#[test]
fn a_window_delivered_in_order_parks_the_sender() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=4 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(consumer.drain.arrivals() > 0);
    assert!(
        consumer.drain.sender_parked(),
        "the window sits unread in the buffer, so the sender holds no credit"
    );
}

#[test]
fn records_behind_the_sequence_are_dropped_as_duplicates() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
        encoder.push_data(id, 1, &item(9)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(consumer.pump(), vec![item(1)]);
    assert_eq!(registry.live_slots(peer()), 1, "a duplicate is not a fault");
}

#[test]
fn hold_overflow_closes_that_slot_and_leaves_the_others_alone() {
    let config = config();
    let registry = IngressRegistry::default();
    let depth =
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit);
    let (tx_a, rx_a) = flume::bounded(depth);
    let (tx_b, rx_b) = flume::bounded(depth);
    registry.register_bind(
        ANCHOR,
        SESSION,
        tx_a,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );
    registry.register_bind(
        ANCHOR,
        SESSION + 1,
        tx_b,
        test_drain(),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );

    let a = slot(0, 0);
    let b = slot(1, 0);
    let payload = batch(1, 0, |encoder| {
        encoder.push_open_slot(a, 0, ANCHOR, SESSION).unwrap();
        encoder.push_open_slot(b, 0, ANCHOR, SESSION + 1).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(registry.live_slots(peer()), 2);

    // Slot A holds more than its `C` credits ahead of sequence: seq 1 never
    // arrives, so 2..=6 pile up and the fifth overspends the grant.
    let payload = batch(1, 1, |encoder| {
        for seq in 2..=6u32 {
            encoder.push_data(a, seq, &item(seq as u8)).unwrap();
        }
        encoder.push_data(b, 1, &item(42)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(registry.live_slots(peer()), 1, "only slot A may close");
    assert!(
        outcome.replies.contains(&ReplyRecord::CloseSlot {
            slot: a,
            reason: CloseReason::ProtocolError
        }),
        "the owner is told which slot failed: {:?}",
        outcome.replies
    );
    assert_eq!(
        drain(&rx_a),
        vec![cached_dropped().clone()],
        "the consumer of the failed slot sees Dropped"
    );
    assert_eq!(drain(&rx_b), vec![item(42)], "slot B is untouched");
}

// ---------------------------------------------------------------------------
// Generations and epochs
// ---------------------------------------------------------------------------

#[test]
fn a_record_at_the_wrong_generation_is_dropped_and_metered() {
    let registry_metrics = prometheus::Registry::new();
    let metrics = crate::observability::VeloMetrics::register(&registry_metrics).unwrap();
    let mux_metrics = metrics.bind_mux();

    let (registry, consumer, config) = bound();
    let id = slot(0, 3);
    let payload = batch(1, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);

    // The same index at the previous generation: a record still in flight for a
    // slot that has since been recycled.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(slot(0, 2), 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);

    assert!(
        consumer.pump().is_empty(),
        "a stale generation must never surface inside the stream now holding the index"
    );
    let snapshot =
        crate::observability::test_helpers::MetricSnapshot::from_registry(&registry_metrics);
    assert_eq!(
        snapshot.counter("velo_streaming_mux_generation_mismatch_total", &[]),
        1.0
    );
}

#[test]
fn a_stale_epoch_batch_is_discarded_wholesale() {
    let registry_metrics = prometheus::Registry::new();
    let metrics = crate::observability::VeloMetrics::register(&registry_metrics).unwrap();
    let mux_metrics = metrics.bind_mux();

    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 5);

    let payload = batch(4, 0, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);

    assert!(consumer.pump().is_empty());
    let snapshot =
        crate::observability::test_helpers::MetricSnapshot::from_registry(&registry_metrics);
    assert_eq!(
        snapshot.counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "stale_epoch")]
        ),
        2.0,
        "the whole batch is dropped by header inspection, record count and all"
    );
}

#[test]
fn a_newer_epoch_retires_the_old_slots_with_exactly_one_dropped() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    // The sender reconnected. Its first batch under the new epoch is what tells
    // this side; nothing else can.
    let payload = batch(2, 0, |encoder| {
        encoder.push_data(slot(0, 0), 0, &item(2)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.closed, 1);
    assert_eq!(
        registry.live_slots(peer()),
        0,
        "slots do not survive an epoch — that is what makes exactly-one-Dropped provable"
    );
    assert_eq!(consumer.pump(), vec![item(1), cached_dropped().clone()]);
}

/// The gap meter against a reordered pair. A batch that lands after its
/// successor is behind the mark, not a gap: `received - expected` wrapped
/// would add nearly `u32::MAX` to a counter that means "batches missing", and
/// one inverted pair — which a detached open under `async_open_ack` can
/// produce — would end the counter's usefulness for the life of the process.
/// The mark must not move back for it either, or the successor's gap is
/// counted a second time when the sequence resumes past it.
#[test]
fn a_batch_arriving_after_its_successor_is_not_metered_as_a_gap() {
    let registry_metrics = prometheus::Registry::new();
    let metrics = crate::observability::VeloMetrics::register(&registry_metrics).unwrap();
    let mux_metrics = metrics.bind_mux();
    let gaps = || {
        crate::observability::test_helpers::MetricSnapshot::from_registry(&registry_metrics)
            .counter("velo_streaming_mux_batch_seq_gaps_total", &[])
    };

    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    let payload = batch(1, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);

    // Batch 2 lands before batch 1. As far as the meter can tell here, one
    // batch is missing, and that is the one count it may make for the pair.
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);
    assert_eq!(
        gaps(),
        1.0,
        "a batch whose predecessor has not arrived reads as one missing"
    );

    // Then batch 1 arrives: behind the mark, so no gap at all.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);
    assert_eq!(
        gaps(),
        1.0,
        "a batch that arrives after its successor must not be metered as a wrapped gap"
    );

    // And the mark stayed at 2, so batch 3 is simply the next in order.
    let payload = batch(1, 3, |encoder| {
        encoder.push_data(id, 3, &item(3)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);
    assert_eq!(
        gaps(),
        1.0,
        "the mark must stay at the newest sequence seen, or the successor's gap is counted twice"
    );

    // Control: the reorder is the meter's problem alone. Delivery holds
    // record 2 until record 1 closes the hold, then releases in order.
    assert_eq!(consumer.pump(), vec![item(1), item(2), item(3)]);
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

#[test]
fn terminal_then_close_delivers_the_terminal_and_injects_nothing() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, cached_finalized()).unwrap();
        encoder
            .push_close_slot(id, 2, CloseReason::TerminalSent)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.closed, 1);
    assert_eq!(registry.live_slots(peer()), 0);
    assert_eq!(
        consumer.pump(),
        vec![cached_finalized().clone()],
        "a terminal spends the reserve and closes without a spurious Dropped"
    );
    assert!(
        consumer.rx.is_disconnected(),
        "dropping the mux-side sender is what ends the consumer's feed"
    );
}

#[test]
fn a_terminal_gets_through_after_the_data_credit_is_spent() {
    // `C = 1`: one data record exhausts the window, so the terminal can only
    // land on the reserve held back for it. One reserved credit is provably
    // enough — `sent_terminal` guarantees at most one terminal per slot, after
    // which the slot closes.
    let config = MuxConfig {
        initial_credit: 1,
        ..config()
    };
    let registry = IngressRegistry::default();
    let consumer = register(&registry, &config, SESSION);
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
        encoder.push_data(id, 2, cached_finalized()).unwrap();
        encoder
            .push_close_slot(id, 3, CloseReason::TerminalSent)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.closed, 1);
    assert_eq!(
        consumer.pump(),
        vec![item(1), cached_finalized().clone()],
        "data exhaustion must never be what a slot fails to deliver its terminal on"
    );
}

#[test]
fn a_terminal_close_defers_behind_records_still_in_the_hold() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // The terminal and its close arrive while seq 1 is still outstanding — the
    // shape a rendezvous singleton produces, since it resolves outside the
    // ordered lane.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, cached_finalized()).unwrap();
        encoder
            .push_close_slot(id, 3, CloseReason::TerminalSent)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(outcome.closed, 0, "the close waits for the gap to close");
    assert_eq!(registry.live_slots(peer()), 1);

    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(
        consumer.pump(),
        vec![item(1), cached_finalized().clone()],
        "the consumer sees Finalized, not the Dropped an early close would have injected"
    );
    assert_eq!(registry.live_slots(peer()), 0);
}

#[test]
fn a_non_terminal_close_from_the_receiver_is_routed_to_the_batcher() {
    let (registry, _consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder
            .push_close_slot(id, 0, CloseReason::UnknownSlot)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(
        outcome.peer_closes,
        vec![(id, CloseReason::UnknownSlot)],
        "direction is carried by the reason: anything but TerminalSent is about a slot we opened"
    );
    assert_eq!(
        registry.live_slots(peer()),
        1,
        "our ingress slot is untouched"
    );
}

// ---------------------------------------------------------------------------
// Credit
// ---------------------------------------------------------------------------

#[test]
fn credit_is_returned_as_the_consumer_drains() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=4u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert!(
        outcome.replies.is_empty(),
        "nothing has drained yet, so there is nothing to grant back"
    );

    assert_eq!(consumer.pump().len(), 4);
    let replies = registry.sweep_credit(peer());
    assert_eq!(
        replies,
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 4 }],
        "the sweep is what un-parks a sender whose peer has gone quiet"
    );
}

#[test]
fn credit_is_withheld_while_the_slot_is_over_its_byte_watermark() {
    let config = MuxConfig {
        initial_credit: 4,
        // One item is well over this, so the first delivered record puts the
        // slot over its watermark.
        slot_byte_budget: 1,
        ..config()
    };
    let registry = IngressRegistry::default();
    let consumer = register(&registry, &config, SESSION);
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump().len(), 2);

    // Occupancy is back to zero, so the watermark no longer binds and the
    // credit flows.
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }]
    );
}

// ---------------------------------------------------------------------------
// Reconcile scope
// ---------------------------------------------------------------------------

/// Live slots one peer holds in these tests. The shape measured on the tier-3
/// rig is about a thousand slots per peer against the eleven a batch delivers
/// into.
const MANY_SLOTS: u32 = 1_000;

/// Open `count` slots on one peer in a single batch, at indexes `0..count`.
///
/// The consumers come back so the caller can keep them alive: dropping one
/// turns the next record for that slot into a `ConsumerGone` fault and retires
/// the slot these tests are counting.
fn open_many(registry: &IngressRegistry, config: &MuxConfig, count: u32) -> Vec<Consumer> {
    let mut consumers = Vec::with_capacity(count as usize);
    for index in 0..count {
        consumers.push(register(registry, config, SESSION + u64::from(index)));
    }

    let payload = batch(1, 0, |encoder| {
        for index in 0..count {
            encoder
                .push_open_slot(slot(index, 0), 0, ANCHOR, SESSION + u64::from(index))
                .unwrap();
        }
    });
    handle_batch(registry, config, None, peer(), &payload);
    assert_eq!(registry.live_slots(peer()), count as usize);
    consumers
}

/// The cost this scope exists to remove: a visit per live slot per batch, on a
/// peer holding a thousand of them, under the mutex the batch path needs.
///
/// Named for the surviving rule, not the first cut's: a batch reconciles the
/// slots it delivered into *and* the slots in the dirty set, and this case
/// has none of the latter, so the assertion below only exercises the first
/// half. [`a_batch_returns_the_credit_of_every_slot_that_drained`] is the
/// counterpart that exercises the dirty-set half.
#[test]
fn a_batch_reconciles_the_slots_it_delivered_into_and_no_others_when_nothing_drained() {
    let config = config();
    let registry = IngressRegistry::default();
    let _consumers = open_many(&registry, &config, MANY_SLOTS);

    let before = registry.reconcile_visits(peer());
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(slot(7, 0), 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    let visits = registry.reconcile_visits(peer()) - before;

    assert_eq!(
        visits, 1,
        "nothing has drained, so the peer's dirty set is empty and a batch \
         delivering into 1 of its {MANY_SLOTS} slots must reconcile that one; \
         it reconciled {visits}"
    );
}

/// Control: the periodic sweep keeps the whole-table walk, because it is the
/// backstop for a slot nothing named — one parked with nothing arriving and
/// nothing being taken out.
#[test]
fn the_sweep_reconciles_every_slot() {
    let config = config();
    let registry = IngressRegistry::default();
    let _consumers = open_many(&registry, &config, MANY_SLOTS);

    let before = registry.reconcile_visits(peer());
    assert!(
        registry.sweep_credit(peer()).is_empty(),
        "nothing has drained, so there is nothing to grant back"
    );
    let visits = registry.reconcile_visits(peer()) - before;

    assert_eq!(
        visits,
        u64::from(MANY_SLOTS),
        "the sweep visited {visits} of the peer's {MANY_SLOTS} slots"
    );
}

/// The discriminator: a batch returns the credit of every slot that drained,
/// not only of the slots it delivered into.
///
/// This replaces a test whose premise was the defect. That test asserted a
/// batch "says nothing about the slot it did not touch", and leaving those
/// slots to the doorbell was measured on the tier-3 rig: every stream sends
/// about four records more than its then-default 256-record window, so the tail of every
/// stream waited on a per-peer, rate-limited walk instead of the peer's next
/// inbound batch. Slot-credit exhaustion went from 13 to about 20,500 per
/// worker process and throughput halved.
#[test]
fn a_batch_returns_the_credit_of_every_slot_that_drained() {
    let config = config();
    let registry = IngressRegistry::default();
    let consumers = open_many(&registry, &config, 2);
    let a = slot(0, 0);
    let b = slot(1, 0);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(a, seq, &item(seq as u8)).unwrap();
            encoder.push_data(b, seq, &item(seq as u8)).unwrap();
        }
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert!(outcome.replies.is_empty(), "nothing has drained yet");

    // Only B's consumer drains. A's records are still in its buffer, so A has
    // nothing to give back and every grant below is B's.
    assert_eq!(consumers[1].pump().len(), 2);

    // A second batch that delivers into A alone.
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(a, 3, &item(3)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CreditUpdate { slot: b, delta: 2 }],
        "B's consumer counted two records out and listed B in the peer's dirty \
         set, so this batch must carry B's grant even though it delivered \
         only into A; without it B's sender waits for a doorbell visit"
    );

    assert!(
        registry.sweep_credit(peer()).is_empty(),
        "and carries it once: the batch already took B's count, and A has \
         drained nothing"
    );

    // A's turn, by the same route.
    assert_eq!(consumers[0].pump().len(), 3);
    let payload = batch(1, 3, |encoder| {
        encoder.push_data(b, 3, &item(3)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CreditUpdate { slot: a, delta: 3 }],
        "the rule is symmetric: the batch into B returns A's credit"
    );
}

/// A doorbell visit walks the slots that drained, not the peer's whole table.
///
/// The visit holds the same per-peer mutex the inbound batch path takes, and
/// runs up to once per `MuxConfig::drain_visit_floor` per peer, so what it
/// walks is hot-path cost.
#[test]
fn a_doorbell_visit_reconciles_only_the_slots_that_drained() {
    let config = config();
    let registry = IngressRegistry::default();
    let consumers = open_many(&registry, &config, MANY_SLOTS);
    let id = slot(7, 0);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumers[7].pump().len(), 1);

    let before = registry.reconcile_visits(peer());
    assert_eq!(
        registry.sweep_drained(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 1 }],
        "the visit answers the drain that rang for it"
    );
    let visits = registry.reconcile_visits(peer()) - before;

    assert_eq!(
        visits, 1,
        "1 of the peer's {MANY_SLOTS} slots drained, and the set names it, so \
         the doorbell must visit 1; it visited {visits}"
    );
}

/// The grant is what the consumer counted, not what the slot buffer holds —
/// and staying exact holds even once that count outruns `sizes`, the one
/// case R6 asked to pin: `inject_dropped` is the only producer of a channel
/// entry with no `sizes` entry, and no live slot reaches it, but a record
/// pushed straight into the buffer behind the mux's back (below) is the same
/// shape without needing that path.
///
/// Occupancy was only ever a proxy for the drain, and only right while the mux
/// was the buffer's sole writer — which the ledger had no way to check. Here a
/// record goes into the buffer behind the mux's back, so the two answers
/// differ and only the counted one is correct.
#[test]
fn the_grant_is_what_the_pump_counted_not_what_the_channel_holds() {
    let config = config();
    let registry = IngressRegistry::default();
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    let drain = test_drain();
    registry.register_bind(
        ANCHOR,
        SESSION,
        tx.clone(),
        Arc::clone(&drain),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );
    let consumer = Consumer { rx, drain };
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // Three delivered, two taken out.
    let payload = batch(1, 1, |encoder| {
        for seq in 1..=3u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump_n(2).len(), 2);

    // A fourth record the mux never admitted. The buffer now holds two, and
    // occupancy says one record drained where two did.
    tx.try_send(item(9)).expect("room in the C + 1 buffer");

    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }],
        "two records were counted out of the buffer, so two credits come back \
         however many records the buffer happens to hold"
    );

    // Take the rest out: the real seq-3 record, whose `sizes` entry the first
    // reconcile above left behind, plus the injected one that never had one.
    // The consumer counts both, so the next reconcile's drain count (2) outruns
    // `sizes` (1 entry) — the `drained > sizes.len()` case R6 asked to decide
    // and test. `reconcile`'s pop loop stops at the one entry `sizes` has, and
    // `SlotCreditAccount::release` clamps the unbounded count against what the
    // account itself still shows buffered (1, not 2), so the grant is 1: the
    // account's clamp is what keeps this exact, not the `sizes` bound, which
    // exists only to stop an underflow that no live slot can actually reach.
    assert_eq!(consumer.pump().len(), 2);
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 1 }],
        "the consumer counted 2 drains but `sizes` and the account both show only \
         1 record still outstanding, so the grant is 1, clamped by the \
         account rather than inflated by the count"
    );
}

/// The periodic walk takes the whole dirty set, not just the slots.
///
/// The walk reconciles every live slot regardless of what is listed, so it
/// discards the listings too: one left behind would send the next pass to an
/// index whose credit was already returned. After the walk the set is empty,
/// and the next drain lists its slot again rather than finding a stale bit
/// and assuming a visit is on its way.
#[test]
fn the_periodic_walk_takes_the_dirty_set_and_the_next_drain_lists_again() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump().len(), 2);

    let dirty = registry.dirty_slots(peer());
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }],
        "the whole-table walk returns what the consumer drained"
    );
    let mut left = Vec::new();
    dirty.take(|index| left.push(index));
    assert!(left.is_empty(), "the walk must take the set: {left:?}");

    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 3, &item(3)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump().len(), 1);
    let mut listed = Vec::new();
    dirty.take(|index| listed.push(index));
    assert_eq!(listed, vec![id.index()], "the slot is listed again");
}

/// A held record earns its credit when it leaves the buffer, never when it
/// enters the hold.
///
/// The hold is ahead-of-sequence storage on this side of the buffer: the
/// record has spent the peer's credit but has been handed to nobody, so
/// returning credit for it would let the peer hold more than `C` records
/// against a `C + 1` buffer.
#[test]
fn a_held_record_earns_its_credit_only_when_the_consumer_takes_it() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // seq 2 arrives first and parks in the hold.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(
        registry.sweep_credit(peer()).is_empty(),
        "a record in the hold has been taken out of nothing"
    );

    // seq 1 closes the gap and the hold releases behind it: both are in the
    // buffer now, and neither has been taken out.
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(
        registry.sweep_credit(peer()).is_empty(),
        "in the buffer is not drained either"
    );

    assert_eq!(consumer.pump_n(1), vec![item(1)]);
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 1 }]
    );
    assert_eq!(consumer.pump_n(1), vec![item(2)]);
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 1 }],
        "the held record is counted once, on its way out, and not again"
    );
}

/// The ledger is unchanged by the scope: every credit a consumer drained comes
/// back exactly once, whichever pass mints it.
///
/// Passes whatever the batch pass's scope is, because it tallies every pass
/// together — which is what makes it a control on the arithmetic rather than
/// on the scope.
#[test]
fn no_grant_is_lost_or_double_counted_when_a_batch_touches_one_of_two_slots() {
    let config = config();
    let registry = IngressRegistry::default();
    let consumers = open_many(&registry, &config, 2);
    let a = slot(0, 0);
    let b = slot(1, 0);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(a, seq, &item(seq as u8)).unwrap();
            encoder.push_data(b, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumers[0].pump().len(), 2);
    assert_eq!(consumers[1].pump().len(), 2);

    let payload = batch(1, 2, |encoder| {
        encoder.push_data(a, 3, &item(3)).unwrap();
    });
    let mut granted: BTreeMap<SlotId, u32> = BTreeMap::new();
    let mut tally = |replies: Vec<ReplyRecord>| {
        for reply in replies {
            match reply {
                ReplyRecord::CreditUpdate { slot, delta } => {
                    *granted.entry(slot).or_insert(0) += delta;
                }
                other => panic!("unexpected reply: {other:?}"),
            }
        }
    };
    tally(handle_batch(&registry, &config, None, peer(), &payload).replies);
    tally(registry.sweep_credit(peer()));
    tally(registry.sweep_credit(peer()));

    let expected: BTreeMap<SlotId, u32> = [(a, 2), (b, 2)].into_iter().collect();
    assert_eq!(
        granted, expected,
        "two records drained on each slot, so each gets two credits back, once"
    );
}

/// A record that parks in the hold marks its slot, and the release the next
/// record triggers is reconciled inside that same batch.
///
/// One slot open on purpose: with the hold marking nothing, the batch would
/// reconcile no slot at all, which is the regression this pins.
#[test]
fn a_held_record_marks_its_slot_and_its_release_is_reconciled_in_the_same_batch() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // Ahead of sequence: parked in the hold, delivered to nobody.
    let before = registry.reconcile_visits(peer());
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    let visits = registry.reconcile_visits(peer()) - before;
    assert_eq!(
        visits, 1,
        "a held record has spent credit only a reconcile gives back, so its \
         slot is visited: {visits} visits"
    );
    assert!(consumer.pump().is_empty(), "seq 2 waits for seq 1");

    // The gap closes, and the hold releases behind it, in one batch.
    let before = registry.reconcile_visits(peer());
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    let visits = registry.reconcile_visits(peer()) - before;
    assert_eq!(visits, 1, "the releasing batch visits its slot: {visits}");
    assert_eq!(
        consumer.pump(),
        vec![item(1), item(2)],
        "the release hands the consumer both records, in sequence"
    );

    let payload = batch(1, 3, |encoder| {
        encoder.push_data(id, 3, &item(3)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }],
        "the held record is accounted exactly once, when it drains"
    );
}

/// A dense index closed and reopened, with a drain of the retired slot still
/// listed: the listing names the index, so the pass finds the
/// *replacement* there instead. That visit is spurious but grants the
/// replacement nothing, because the count a reconcile reads belongs to the
/// slot and not to the index — the replacement claimed its own bind's
/// `DrainSignal`, and no consumer has taken anything out of that one.
#[test]
fn a_reused_index_reconciles_its_replacement_and_grants_it_nothing() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // A drain of the slot that is about to be retired. It lists index 0, so
    // the pass below has an entry for an index whose occupant has changed —
    // and a credit the replacement must not be handed.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(
        consumer.pump(),
        vec![item(1)],
        "the retired slot drained one"
    );

    let replacement = register(&registry, &config, SESSION + 1);
    let new_id = slot(0, 1);

    // Close before open: with the open first, `open_slot`'s collision guard
    // would reject the newcomer, since the old occupant is still live.
    let before = registry.reconcile_visits(peer());
    let payload = batch(1, 2, |encoder| {
        encoder
            .push_close_slot(id, 2, CloseReason::TerminalSent)
            .unwrap();
        encoder
            .push_open_slot(new_id, 0, ANCHOR, SESSION + 1)
            .unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);
    let visits = registry.reconcile_visits(peer()) - before;

    assert_eq!(outcome.closed, 1);
    assert_eq!(
        registry.live_slots(peer()),
        1,
        "index 0 is live again, under the new generation"
    );
    assert!(
        outcome.replies.is_empty(),
        "the drain belonged to the slot that is gone, and the replacement's \
         own signal has counted nothing: {:?}",
        outcome.replies
    );
    assert_eq!(
        visits, 1,
        "the pass visits whatever now occupies the listed index, not the \
         slot that listed it: {visits} visits"
    );
    assert!(
        replacement.pump().is_empty(),
        "the replacement never received a record in this batch"
    );
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

#[test]
fn shutdown_retires_every_slot() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    assert_eq!(registry.shutdown(), 1);
    assert_eq!(registry.live_slots(peer()), 0);
    assert_eq!(consumer.pump(), vec![cached_dropped().clone()]);
}

#[test]
fn a_heartbeat_record_reaches_the_consumer_as_a_heartbeat_frame() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_heartbeat(id, 1).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(
        consumer.pump(),
        vec![crate::streaming::sender::cached_heartbeat().clone()],
        "a heartbeat is a Data-class record: dropping one under saturation is \
         the per-slot saturation signal the stream watchdog watches for"
    );
    assert_eq!(RecordType::SlotHeartbeat.as_u8(), 4);
}

/// Only the drain that lists its slot touches the peer's shared pending flag.
///
/// Every consumer of a peer's streams drains into that one flag, so a write
/// per record bounces its cache line between every thread running one of the
/// peer's streams. A drain of a slot that is already listed has nothing to
/// add: the drain that listed it already made sure a visit is coming, and a
/// visit takes the flag down before it takes the listings, so it collects
/// this drain's count too.
#[test]
fn a_drain_of_a_listed_slot_leaves_the_pending_flag_alone() {
    let (wake_tx, wake_rx) = flume::bounded(8);
    let drain = DrainSignal::new(wake_tx);
    let pending = Arc::new(AtomicBool::new(false));
    drain.claimed_by(
        peer(),
        slot(3, 0),
        Arc::clone(&pending),
        Arc::new(DirtySlots::new()),
    );

    drain.drained();
    assert!(
        pending.load(Ordering::Acquire),
        "the listing drain arms the wake"
    );
    assert_eq!(wake_rx.len(), 1);

    // A visit takes the flag down first, then takes the listings.
    pending.store(false, Ordering::Release);
    drain.drained();
    assert!(
        !pending.load(Ordering::Acquire),
        "a drain of a slot still listed must not write the shared flag"
    );
    assert_eq!(wake_rx.len(), 1, "and must not post a second wake");
    assert_eq!(
        drain.take_drained(),
        2,
        "the visit the first listing arranged collects both drains"
    );
}

/// The claim is write-once, and the first `OpenSlot` is the one that counts.
///
/// Both readers of this cell depend on that. `drained` posts wakes to the peer
/// it names, and a pre-bind's owner closes the slot it names; a second claim
/// overwriting either would send credit to the wrong peer's set, or close a
/// slot belonging to a stream that is still running.
#[test]
fn drain_signal_claim_stays_write_once() {
    let drain = test_drain();
    assert_eq!(drain.claimed(), None, "an unclaimed bind names no slot");

    let first = SlotId::new(3, 0).expect("slot id");
    let second = SlotId::new(9, 1).expect("slot id");
    let other_peer = PeerLane::new(WorkerId::from_u64(PEER + 1), LaneIndex::ZERO);

    let dirty = Arc::new(DirtySlots::new());
    drain.claimed_by(
        peer(),
        first,
        Arc::new(AtomicBool::new(false)),
        Arc::clone(&dirty),
    );
    drain.claimed_by(other_peer, second, Arc::new(AtomicBool::new(false)), dirty);

    assert_eq!(
        drain.claimed(),
        Some((peer(), first)),
        "the second claim must be dropped, not applied"
    );
}

// ---------------------------------------------------------------------------
// Lanes
// ---------------------------------------------------------------------------

/// Two lanes of one peer keep separate tables.
///
/// Slot ids are unique only within one sender batcher, and each lane has its
/// own batcher, so the same id may be live on two lanes at once. A table
/// shared by the lanes would read the second `OpenSlot` as a collision and
/// reject it, and a new epoch on one lane would retire the other lane's
/// streams. Every other test here runs on lane 0 alone and cannot see either.
#[test]
fn lanes_of_one_peer_keep_separate_tables() {
    let config = config();
    let registry = IngressRegistry::default();
    let on_zero = register(&registry, &config, SESSION);
    let on_one = register(&registry, &config, SESSION + 1);
    let zero = peer();
    let one = PeerLane::new(zero.peer, LaneIndex::new(1));
    let id = slot(0, 0);

    let payload = batch(5, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, zero, &payload);
    assert_eq!(outcome.opened, 1);

    // The same slot id on lane 1, under that lane's own epoch.
    let payload = batch_on(one.lane, 9, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION + 1).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, one, &payload);
    assert_eq!(outcome.opened, 1);
    assert!(
        outcome.replies.is_empty(),
        "the same id on another lane is not a collision"
    );
    assert_eq!(registry.live_slots(zero), 1);
    assert_eq!(registry.live_slots(one), 1);

    // Records reach the stream of the lane they arrived on.
    let payload = batch_on(one.lane, 9, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, one, &payload);
    assert_eq!(on_one.pump(), vec![item(1)]);
    assert!(on_zero.pump().is_empty());

    // A new epoch on lane 1 retires lane 1's slot and nothing on lane 0.
    let payload = batch_on(one.lane, 10, 0, |_| {});
    let outcome = handle_batch(&registry, &config, None, one, &payload);
    assert_eq!(outcome.closed, 1);
    assert_eq!(registry.live_slots(one), 0);
    assert_eq!(
        registry.live_slots(zero),
        1,
        "an epoch change on one lane must leave the other lane's streams alone"
    );
}

/// A claim names the lane its `OpenSlot` arrived on, and so does the wake its
/// drains post.
///
/// Stop, cancel and close of a claimed slot go through the batcher the claim
/// names, and credit comes back through the lane the wake names. Either one
/// landing on another lane would reach a batcher whose slot ids mean other
/// streams.
#[test]
fn a_claim_and_its_wake_name_the_arrival_lane() {
    let config = config();
    let registry = IngressRegistry::default();
    let (wake_tx, wake_rx) = flume::unbounded();
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    let drain = Arc::new(DrainSignal::new(wake_tx));
    registry.register_bind(
        ANCHOR,
        SESSION,
        tx,
        Arc::clone(&drain),
        LaneReservation::uncounted(LaneIndex::ZERO),
    );
    let consumer = Consumer { rx, drain };
    let one = PeerLane::new(peer().peer, LaneIndex::new(1));
    let id = slot(4, 2);

    let payload = batch_on(one.lane, 3, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
        encoder.push_data(id, 1, &item(7)).unwrap();
    });
    handle_batch(&registry, &config, None, one, &payload);

    assert_eq!(consumer.drain.claimed(), Some((one, id)));
    assert_eq!(consumer.pump(), vec![item(7)]);
    assert_eq!(
        wake_rx.try_recv(),
        Ok(one),
        "the wake names the arrival lane"
    );
    assert_eq!(consumer.drain.cancel(), Some((one, id)));
}

/// A batch whose header names another lane than its handler's is dropped
/// whole and metered, and creates no table.
///
/// The control is the same batch stamped with the handler's lane, which
/// opens its slot; without the check the mismatched one would too, on a table
/// for a lane its slot ids do not belong to.
#[test]
fn a_batch_on_the_wrong_lane_is_dropped_and_metered() {
    let registry_metrics = prometheus::Registry::new();
    let metrics = crate::observability::VeloMetrics::register(&registry_metrics).unwrap();
    let mux_metrics = metrics.bind_mux();
    let (registry, _consumer, config) = bound();
    let three = PeerLane::new(peer().peer, LaneIndex::new(3));
    let open_slot = |encoder: &mut BatchEncoder| {
        encoder
            .push_open_slot(slot(0, 0), 0, ANCHOR, SESSION)
            .unwrap();
    };

    // Stamped lane 0, arriving on lane 3's handler.
    let payload = batch(1, 0, open_slot);
    let outcome = handle_batch(&registry, &config, Some(&mux_metrics), three, &payload);
    assert_eq!(outcome.opened, 0);
    assert!(outcome.replies.is_empty());
    assert!(
        !registry.peers().contains(&three),
        "a dropped batch must not create a table"
    );
    let snapshot =
        crate::observability::test_helpers::MetricSnapshot::from_registry(&registry_metrics);
    assert_eq!(
        snapshot.counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "lane_mismatch")]
        ),
        1.0
    );

    // The control: stamped with the handler's lane, the same batch opens.
    let payload = batch_on(three.lane, 1, 0, open_slot);
    let outcome = handle_batch(&registry, &config, Some(&mux_metrics), three, &payload);
    assert_eq!(outcome.opened, 1);
    assert_eq!(registry.live_slots(three), 1);
}

/// Every lane's table takes the whole slot index range.
///
/// The sender picks the lane a stream rides, clamped to its own lane count,
/// so one lane's table may hold all of a peer's streams. A range split over
/// this node's lanes refused `OpenSlot`s a one-lane sender had every right to
/// send.
#[test]
fn every_lane_table_takes_the_whole_slot_range() {
    let (registry, _consumer, config) = bound();
    let last = PeerLane::new(peer().peer, LaneIndex::new(15));
    let top = slot(MAX_INGRESS_SLOTS_PER_PEER as u32 - 1, 0);
    let payload = batch_on(last.lane, 1, 0, |encoder| {
        encoder.push_open_slot(top, 0, ANCHOR, SESSION).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, last, &payload);
    assert_eq!(outcome.opened, 1);
    assert!(outcome.replies.is_empty());

    // The ceiling itself is still refused.
    let past = slot(MAX_INGRESS_SLOTS_PER_PEER as u32, 0);
    let payload = batch_on(last.lane, 1, 1, |encoder| {
        encoder.push_open_slot(past, 0, ANCHOR, SESSION).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, last, &payload);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::RejectSlot {
            slot: past,
            reason: CloseReason::ProtocolError
        }]
    );
}

/// The node's live count per lane, and the count per (peer, lane), match the
/// slots in its tables, by the lane each slot was placed on, after every way a
/// slot opens or leaves.
///
/// Unkeyed pre-binds are placed by the first and unkeyed attaches by the
/// second, both read without locking any table. Each slot holds its own
/// count, so the two can drift only if a slot is kept somewhere after it
/// leaves its table, or a count is taken with no slot behind it.
///
/// A slot counts on the lane its bind was placed on, which is not always the
/// lane its batches arrive on: a sender with fewer lanes sends lane k on k
/// modulo its count. Here binds placed on lanes 5 and 6 arrive on lane 1, and
/// one placed on lane 2 arrives on lane 0. The table walk reads each slot's
/// lane from the slot's own count, so it agrees with a count taken on the wrong
/// lane; the expected per-lane counts after each step are what catch that, so
/// keep them. The steps cover each exit (a duplicate open, a cancel that lands
/// before the claim, a close from the consumer side, a new epoch, and
/// shutdown) on two peers and two arrival lanes.
#[test]
fn the_live_count_per_lane_matches_the_slot_tables() {
    let config = config();
    let registry = IngressRegistry::default();
    let a = WorkerId::from_u64(PEER);
    let b = WorkerId::from_u64(PEER + 1);
    let a0 = PeerLane::new(a, LaneIndex::ZERO);
    let a1 = PeerLane::new(a, LaneIndex::new(1));
    let b1 = PeerLane::new(b, LaneIndex::new(1));
    // Session n's bind is placed on `placed[n - 1]`.
    let placed = [0, 5, 1, 6, 2, 5].map(LaneIndex::new);
    let consumers: Vec<Consumer> = (1..=6u64)
        .map(|session| register_on(&registry, &config, session, placed[session as usize - 1]))
        .collect();
    let open_on = |key: PeerLane, epoch: u64, batch_seq: u32, id: SlotId, session: u64| {
        let payload = batch_on(key.lane, epoch, batch_seq, |encoder| {
            encoder.push_open_slot(id, 0, ANCHOR, session).unwrap();
        });
        handle_batch(&registry, &config, None, key, &payload)
    };
    // Every count against a walk of the tables, and the live slots per
    // placed lane, 0 to 6, for the step to be checked against.
    let counts = |step: &str| -> [usize; 7] {
        LaneIndex::all().take(7).for_each(|lane| {
            for peer in [a, b] {
                assert_eq!(
                    registry.live_count(PeerLane::new(peer, lane)),
                    registry.live_placed_on(Some(peer), lane),
                    "{step}: the count of ({peer:?}, {lane:?}) must match the tables"
                );
            }
            assert_eq!(
                registry.live_on_lane(lane),
                registry.live_placed_on(None, lane),
                "{step}: the node's count on {lane:?} must match the tables"
            );
        });
        std::array::from_fn(|lane| registry.live_on_lane(LaneIndex::new(lane as u16)))
    };

    open_on(a0, 1, 0, slot(0, 0), 1);
    open_on(a1, 5, 0, slot(0, 0), 2);
    open_on(b1, 7, 0, slot(0, 0), 3);
    open_on(b1, 7, 1, slot(1, 0), 4);
    assert_eq!(counts("after the opens"), [1, 1, 0, 0, 0, 1, 1]);
    assert_eq!(
        (
            registry.live_slots(a0),
            registry.live_slots(a1),
            registry.live_slots(b1)
        ),
        (1, 1, 2),
        "the tables stay keyed by the lane batches arrive on"
    );

    // The same id opens again on a0: the incumbent retires, the new one opens
    // placed on lane 2.
    let outcome = open_on(a0, 1, 1, slot(0, 0), 5);
    assert_eq!((outcome.opened, outcome.closed), (1, 1));
    assert_eq!(counts("after a duplicate open"), [0, 1, 1, 0, 0, 1, 1]);

    // The consumer cancels before the `OpenSlot` lands: the slot opens and
    // closes in one pass.
    assert_eq!(consumers[5].drain.cancel(), None);
    let outcome = open_on(b1, 7, 2, slot(2, 0), 6);
    assert_eq!((outcome.opened, outcome.closed), (1, 1));
    assert_eq!(
        counts("after a cancel before the claim"),
        [0, 1, 1, 0, 0, 1, 1]
    );

    assert!(
        registry
            .close_consumer_gone(b1, slot(1, 0), None, None)
            .is_some()
    );
    assert_eq!(counts("after a consumer-side close"), [0, 1, 1, 0, 0, 1, 0]);

    let payload = batch_on(a1.lane, 6, 0, |_| {});
    let outcome = handle_batch(&registry, &config, None, a1, &payload);
    assert_eq!(outcome.closed, 1);
    assert_eq!(counts("after a new epoch on a1"), [0, 1, 1, 0, 0, 0, 0]);

    assert_eq!(registry.shutdown(), 2);
    assert_eq!(counts("after shutdown"), [0; 7]);
}

/// One peer's lanes share one byte budget, so the per-peer bound is
/// `peer_byte_budget` whatever lane count either side keeps.
///
/// Each lane's table used to take its own share of the budget, sized from the
/// lane count this node read when the table was made. A count read as 1 (the
/// peer not yet known to discovery) gave a table the whole budget, and later
/// lanes a share each, so the peer could hold more than the budget. Here each
/// table is made while the count reads 1, and a second lane's hold must still
/// be refused once the first lane's hold has taken most of the budget.
///
/// The budget is below two slots' windows, so the shared bound is what bites,
/// not a slot's own cap. After a new epoch retires lane 0's slot, its bytes
/// come back to the shared budget and lane 1 can hold again.
#[test]
fn the_lanes_of_one_peer_share_one_byte_budget() {
    let config = MuxConfig {
        peer_byte_budget: 300,
        ..config()
    };
    assert!(config.peer_byte_budget < 2 * u64::from(config.slot_byte_budget));
    let registry = IngressRegistry::default();
    let _consumers: Vec<Consumer> = (1..=3)
        .map(|session| register(&registry, &config, session))
        .collect();
    let a0 = peer();
    let a1 = PeerLane::new(a0.peer, LaneIndex::new(1));
    let big = |n: u8| {
        rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![n; 180]))
            .expect("encode item")
    };
    let id = slot(0, 0);
    let run = |key: PeerLane, epoch: u64, batch_seq: u32, build: &dyn Fn(&mut BatchEncoder)| {
        let payload = batch_on(key.lane, epoch, batch_seq, |encoder| build(encoder));
        handle_batch(&registry, &config, None, key, &payload)
    };

    run(a0, 1, 0, &|encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, 1).unwrap();
    });
    run(a1, 1, 0, &|encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, 2).unwrap();
    });
    // Ahead of sequence (the next is 1), so each record is held and charged.
    let outcome = run(a0, 1, 1, &|encoder| {
        encoder.push_data(id, 2, &big(1)).unwrap();
    });
    assert!(outcome.replies.is_empty(), "lane 0's hold fits the budget");
    let outcome = run(a1, 1, 1, &|encoder| {
        encoder.push_data(id, 2, &big(2)).unwrap();
    });
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::ProtocolError
        }],
        "lane 1's hold must be refused: with lane 0's it would pass the peer's budget"
    );
    let held = registry.peer_bytes_used(a0.peer);
    assert!(
        (150..=config.peer_byte_budget).contains(&held),
        "lane 0's hold alone is charged to the peer: {held}"
    );

    // A new epoch on lane 0 retires its slot and gives its bytes back.
    let outcome = run(a0, 2, 0, &|_| {});
    assert_eq!(outcome.closed, 1);
    assert_eq!(registry.peer_bytes_used(a0.peer), 0);
    run(a1, 1, 2, &|encoder| {
        encoder.push_open_slot(slot(1, 0), 0, ANCHOR, 3).unwrap();
    });
    let outcome = run(a1, 1, 3, &|encoder| {
        encoder.push_data(slot(1, 0), 2, &big(3)).unwrap();
    });
    assert!(
        outcome.replies.is_empty(),
        "a retired epoch's holds must go back to the shared budget"
    );
}
