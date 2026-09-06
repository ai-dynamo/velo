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
use crate::streaming::messenger_mux::protocol::{BatchEncoder, RecordType, SlotId};
use crate::streaming::sender::{cached_dropped, cached_finalized};

/// A drain signal whose wakes go nowhere, for tests that drive the registry
/// directly. The claim path still runs, so `open_slot` naming the peer, the
/// slot index and the dirty lane is covered; nothing consumes the wake lane
/// because these tests have no sweep task.
fn test_drain() -> Arc<DrainSignal> {
    let (tx, _rx) = flume::bounded(16);
    Arc::new(DrainSignal::new(tx))
}

/// The consumer side of one bound slot: the receiver `bind` handed the anchor,
/// and the drain signal `reader_pump` would hold.
///
/// Both are needed because credit is returned against what the pump *counted*.
/// Taking a frame out of `rx` without telling the signal is what a dead pump
/// looks like, not what a draining consumer looks like, and reconciles nothing.
struct Consumer {
    rx: flume::Receiver<Vec<u8>>,
    drain: Arc<DrainSignal>,
}

impl Consumer {
    /// Take everything available, counting each record the way `reader_pump`
    /// does.
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
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    let drain = test_drain();
    registry.register_bind(ANCHOR, session, tx, Arc::clone(&drain));
    Consumer { rx, drain }
}

const PEER: u64 = 0xABCD;
const ANCHOR: u64 = 7;
const SESSION: u64 = 11;

fn peer() -> WorkerId {
    WorkerId::from_u64(PEER)
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

/// Build a batch payload from a closure that pushes its records.
fn batch(epoch: u64, batch_seq: u32, build: impl FnOnce(&mut BatchEncoder)) -> Bytes {
    let mut encoder = BatchEncoder::new(epoch, batch_seq);
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
/// record taken this way is one whose pump died, as far as the ledger knows.
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
    registry.register_bind(ANCHOR, SESSION, incumbent_tx, test_drain());
    // A second bind, for the collider to try to claim.
    let (rival_tx, rival_rx) = flume::bounded(depth);
    registry.register_bind(ANCHOR, SESSION + 1, rival_tx, test_drain());

    let incumbent = slot(0, 0);
    open(&registry, &config, incumbent, 1);

    // Give the incumbent something in its ahead-of-sequence hold, so a silent
    // eviction would leak peer byte budget as well as the stream.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(incumbent, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    let held_bytes = registry.peer_bytes_used(peer());
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
        registry.peer_bytes_used(peer()),
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
    registry.register_bind(ANCHOR, SESSION, first_tx, test_drain());
    registry.register_bind(ANCHOR, SESSION + 1, second_tx, test_drain());

    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    // Something ahead of sequence, so the hold has bytes charged against the
    // peer budget that only a proper close gives back.
    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert!(registry.peer_bytes_used(peer()) > 0);

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
        registry.peer_bytes_used(peer()),
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
    registry.register_bind(ANCHOR, SESSION, tx_a, test_drain());
    registry.register_bind(ANCHOR, SESSION + 1, tx_b, test_drain());

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
        "dropping the mux-side sender is what makes reader_pump exit its usual Err branch"
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
// Grant threshold
// ---------------------------------------------------------------------------

/// A window big enough for the threshold to be a rule rather than a rounding
/// case, and the one the mux ships with.
const FULL_WINDOW: u32 = 256;

/// The default window against a slot byte budget wide enough to hold it, so
/// the byte watermark never binds and the only rule under test is the
/// threshold.
fn wide_window_config() -> MuxConfig {
    MuxConfig {
        initial_credit: FULL_WINDOW,
        slot_byte_budget: 64 * 1024,
        ..config()
    }
}

/// Deliver a full window into one slot and hand back its consumer.
fn full_window(registry: &IngressRegistry, config: &MuxConfig, id: SlotId) -> Consumer {
    let consumer = register(registry, config, SESSION);
    open(registry, config, id, 1);
    let payload = batch(1, 1, |encoder| {
        for seq in 1..=FULL_WINDOW {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(registry, config, None, peer(), &payload);
    consumer
}

/// The discriminator: a grant is minted once per half window, not once per
/// drained record.
///
/// Since the arrival path learned to answer the dirty lane, a slot is
/// reconciled on every batch that delivered into it or that its pump listed,
/// which is about once per record — so almost every drained record became its
/// own `CreditUpdate`. On the tier-3 rig's frontend that was 61.7 million of
/// them for about 66 million data records received, against 28.8 million for
/// the same work before. Each one is a reply staged into a batcher, a batch on
/// the wire, and a control record the peer decodes and applies.
#[test]
fn a_grant_is_minted_once_per_half_window_not_once_per_drained_record() {
    let config = wide_window_config();
    let registry = IngressRegistry::default();
    let id = slot(0, 0);
    let consumer = full_window(&registry, &config, id);

    // One drain, one reconcile pass, all the way through the window.
    let mut grants: Vec<(u32, u32)> = Vec::new();
    for drained in 1..=FULL_WINDOW {
        assert_eq!(consumer.pump_n(1).len(), 1);
        for reply in registry.sweep_drained(peer()) {
            match reply {
                ReplyRecord::CreditUpdate {
                    slot: granted,
                    delta,
                } => {
                    assert_eq!(granted, id, "the grant names the slot that drained");
                    grants.push((drained, delta));
                }
                other => panic!("unexpected reply: {other:?}"),
            }
        }
    }

    let threshold = FULL_WINDOW / 2;
    assert_eq!(
        grants,
        vec![(threshold, threshold), (FULL_WINDOW, threshold)],
        "a {FULL_WINDOW}-record window has a threshold of {threshold}, so \
         draining it one record at a time must mint 2 grants of {threshold}, \
         at drains {threshold} and {FULL_WINDOW}; it minted {} of them",
        grants.len()
    );
}

/// The periodic sweep grants what the threshold withheld, so a remainder waits
/// at most one `MuxConfig::credit_sweep_interval` and never longer.
///
/// Nothing is waiting on it — a sender with a window in hand cannot reach the
/// threshold's withholding, per `IngressSlot::take_grant` — but a slot that
/// goes quiet mid-window must not carry the remainder for the rest of its life
/// either, because the next window's arithmetic starts from it.
#[test]
fn the_periodic_sweep_grants_the_remainder_the_threshold_withholds() {
    const DRAINED: u32 = 10;

    let config = wide_window_config();
    let registry = IngressRegistry::default();
    let id = slot(0, 0);
    let consumer = full_window(&registry, &config, id);

    assert_eq!(consumer.pump_n(DRAINED as usize).len(), DRAINED as usize);
    assert!(
        registry.sweep_drained(peer()).is_empty(),
        "{DRAINED} records drained against a threshold of {}, so the doorbell \
         has nothing to advertise yet",
        FULL_WINDOW / 2
    );
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate {
            slot: id,
            delta: DRAINED
        }],
        "the periodic tick is what bounds how long a sub-threshold remainder \
         waits, so it grants whatever is pending"
    );
}

/// The byte watermark withholds a grant the threshold would allow.
///
/// The two rules are independent and both must hold: the threshold is about
/// how *often* credit is advertised, the watermark about whether the slot has
/// room for what advertising it would let in.
#[test]
fn the_byte_watermark_withholds_a_grant_the_threshold_would_allow() {
    let config = MuxConfig {
        initial_credit: 4,
        // One item is well over this, so any record left in the buffer keeps
        // the slot over its watermark.
        slot_byte_budget: 1,
        ..config()
    };
    let registry = IngressRegistry::default();
    let consumer = register(&registry, &config, SESSION);
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=4u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(consumer.pump_n(2).len(), 2);
    assert!(
        registry.sweep_drained(peer()).is_empty(),
        "2 of a 4-record window drained, which meets the threshold, but two \
         records still occupy the buffer and the watermark is what decides"
    );

    assert_eq!(consumer.pump_n(2).len(), 2);
    assert_eq!(
        registry.sweep_drained(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 4 }],
        "occupancy is back to zero, so both rules are satisfied and the whole \
         window comes back at once"
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
/// slots it delivered into *and* the slots on the dirty lane, and this case
/// has none of the latter, so the assertion below only exercises the first
/// half. [`a_batch_returns_the_credit_of_every_slot_that_drained`] is the
/// counterpart that exercises the dirty-lane half.
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
        "nothing has drained, so the peer's dirty lane is empty and a batch \
         delivering into 1 of its {MANY_SLOTS} slots must reconcile that one; \
         it reconciled {visits}"
    );
}

/// Control: the periodic sweep keeps the whole-table walk, because it is the
/// backstop for a slot nothing named — one parked with nothing arriving and
/// nothing being taken out, and one whose drain found the dirty lane full.
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
/// about four records more than its 256-record window, so the tail of every
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
        "B's pump counted two records out and named B on the peer's dirty \
         lane, so this batch must carry B's grant even though it delivered \
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
///
/// Two records drained rather than one, because the doorbell mints a grant
/// only once the slot's pending credit has reached half its window and the
/// window here is [`config`]'s 4. The count under test is the *visit* count,
/// which one drain would have measured just as well; the grant is asserted
/// alongside it so a visit that walked the right slot and returned nothing
/// cannot pass, and that assertion is what needs the threshold met.
#[test]
fn a_doorbell_visit_reconciles_only_the_slots_that_drained() {
    let config = config();
    let registry = IngressRegistry::default();
    let consumers = open_many(&registry, &config, MANY_SLOTS);
    let id = slot(7, 0);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumers[7].pump().len(), 2);

    let before = registry.reconcile_visits(peer());
    assert_eq!(
        registry.sweep_drained(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }],
        "the visit answers the drain that rang for it"
    );
    let visits = registry.reconcile_visits(peer()) - before;

    assert_eq!(
        visits, 1,
        "1 of the peer's {MANY_SLOTS} slots drained, and the lane names it, so \
         the doorbell must visit 1; it visited {visits}"
    );
}

/// The grant is what the pump counted, not what the slot buffer holds — and
/// staying exact holds even once the pump's count outruns `sizes`, the one
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
    registry.register_bind(ANCHOR, SESSION, tx.clone(), Arc::clone(&drain));
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
    // The pump counts both, so the next reconcile's drain count (2) outruns
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
        "the pump counted 2 drains but `sizes` and the account both show only \
         1 record still outstanding, so the grant is 1, clamped by the \
         account rather than inflated by the count"
    );
}

/// A full dirty lane costs a listing, not the credit — and the periodic walk
/// that answers it also empties the lane, not just the slots.
///
/// The lane is the fast path and the periodic walk is what stands behind it.
/// The walk reconciles every live slot regardless of what is on the lane, so
/// it drains the lane too: leaving an entry behind would let the next real
/// listing for the same index queue a second one, and "one entry per slot
/// with something outstanding" would stop being true. Filling and emptying a
/// lane of `MAX_INGRESS_SLOTS_PER_PEER` entries is why this test runs in tens
/// of milliseconds rather than microseconds.
#[test]
fn a_drain_that_cannot_list_still_gets_its_credit_from_the_periodic_walk() {
    let (registry, consumer, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    // Fill the lane after the batch, because the batch pass drains it.
    let (lane_tx, lane_rx) = registry.drained_lane(peer());
    for _ in 0..MAX_INGRESS_SLOTS_PER_PEER {
        lane_tx.try_send(u32::MAX).expect("the lane has room");
    }
    assert!(lane_tx.try_send(u32::MAX).is_err(), "the lane is full");

    assert_eq!(consumer.pump().len(), 2);
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }],
        "the listing had nowhere to go, so the whole-table walk is what finds \
         the count the pump left on the slot"
    );

    assert!(
        lane_rx.try_recv().is_err(),
        "the walk that just reconciled this slot must also have drained the \
         fill entries it invalidated; one left behind here is what lets a \
         later drain of the same slot queue a second entry for it"
    );

    // With room again, the next drain lists: a failed listing puts the flag
    // back down rather than claiming a visit that is not coming.
    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 3, &item(3)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(consumer.pump().len(), 1);
    assert_eq!(
        lane_rx.try_recv().expect("the slot is listed again"),
        id.index()
    );
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
/// on the lane: the entry names the index, so the pass finds the
/// *replacement* there instead. That visit is spurious but grants the
/// replacement nothing, because the count a reconcile reads belongs to the
/// slot and not to the index — the replacement claimed its own bind's
/// `DrainSignal`, and no pump has taken anything out of that one.
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
         the per-slot saturation signal reader_pump's watchdog watches for"
    );
    assert_eq!(RecordType::SlotHeartbeat.as_u8(), 4);
}
