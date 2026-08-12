// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ingress unit tests.
//!
//! `handle_batch` is a pure function over an [`IngressRegistry`], so every
//! receive-side property is testable without a messenger, a runtime, or a
//! socket — the batch bytes go in and the replies come out.

use bytes::Bytes;
use velo_ext::WorkerId;

use super::*;
use crate::streaming::messenger_mux::protocol::{BatchEncoder, RecordType, SlotId};
use crate::streaming::sender::{cached_dropped, cached_finalized};

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

/// A registry with one bound anchor, plus the receiver the consumer would hold.
fn bound() -> (IngressRegistry, flume::Receiver<Vec<u8>>, MuxConfig) {
    let config = config();
    let registry = IngressRegistry::default();
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    registry.register_bind(ANCHOR, SESSION, tx);
    (registry, rx, config)
}

/// Open slot `id` at `frame_seq = 0` and return the resulting outcome.
fn open(registry: &IngressRegistry, config: &MuxConfig, id: SlotId, epoch: u64) -> BatchOutcome {
    let payload = batch(epoch, 0, |encoder| {
        encoder.push_open_slot(id, 0, ANCHOR, SESSION).unwrap();
    });
    handle_batch(registry, config, None, peer(), &payload)
}

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
fn open_slot_claims_the_matching_bind_and_grants_credit() {
    let (registry, _rx, config) = bound();
    let id = slot(0, 0);

    let outcome = open(&registry, &config, id, 1);

    assert_eq!(outcome.opened, 1);
    assert_eq!(registry.live_slots(peer()), 1);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CreditUpdate {
            slot: id,
            delta: config.initial_credit
        }],
        "the window has to be advertised: until negotiation lands there is \
         nowhere else to say it, and a sender that guessed would push into a \
         buffer this side never sized"
    );
}

#[test]
fn open_slot_for_an_unregistered_anchor_rejects_that_slot_only() {
    let (registry, _rx, config) = bound();
    let id = slot(3, 0);

    let payload = batch(1, 0, |encoder| {
        // A pair nobody bound.
        encoder.push_open_slot(id, 0, 999, 999).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(outcome.opened, 0);
    assert_eq!(
        outcome.replies,
        vec![ReplyRecord::CloseSlot {
            slot: id,
            reason: CloseReason::UnknownSlot
        }],
        "the reverse race must not fail the peer"
    );
    // The bind that *was* registered is untouched and still claimable.
    let outcome = open(&registry, &config, slot(0, 0), 1);
    assert_eq!(outcome.opened, 1);
}

#[test]
fn records_for_a_slot_that_never_opened_are_dropped() {
    let (registry, rx, config) = bound();

    let payload = batch(1, 0, |encoder| {
        encoder.push_data(slot(5, 0), 0, &item(1)).unwrap();
    });
    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert!(outcome.replies.is_empty());
    assert!(drain(&rx).is_empty());
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[test]
fn data_applies_in_frame_seq_order() {
    let (registry, rx, config) = bound();
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

    let frames = drain(&rx);
    assert_eq!(frames.len(), 4);
    for (n, frame) in frames.iter().enumerate() {
        assert_eq!(frame, &item(n as u8), "frame {n} out of order");
    }
}

#[test]
fn ahead_of_sequence_records_are_held_until_the_gap_closes() {
    let (registry, rx, config) = bound();
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
        drain(&rx).is_empty(),
        "nothing may be delivered while the gap is open"
    );

    let payload = batch(1, 2, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    let frames = drain(&rx);
    assert_eq!(frames, vec![item(1), item(2), item(3)]);
}

#[test]
fn records_behind_the_sequence_are_dropped_as_duplicates() {
    let (registry, rx, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
        encoder.push_data(id, 1, &item(9)).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(drain(&rx), vec![item(1)]);
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
    registry.register_bind(ANCHOR, SESSION, tx_a);
    registry.register_bind(ANCHOR, SESSION + 1, tx_b);

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

    let (registry, rx, config) = bound();
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
        drain(&rx).is_empty(),
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

    let (registry, rx, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 5);

    let payload = batch(4, 0, |encoder| {
        encoder.push_data(id, 1, &item(1)).unwrap();
        encoder.push_data(id, 2, &item(2)).unwrap();
    });
    handle_batch(&registry, &config, Some(&mux_metrics), peer(), &payload);

    assert!(drain(&rx).is_empty());
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
    let (registry, rx, config) = bound();
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
    assert_eq!(drain(&rx), vec![item(1), cached_dropped().clone()]);
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

#[test]
fn terminal_then_close_delivers_the_terminal_and_injects_nothing() {
    let (registry, rx, config) = bound();
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
        drain(&rx),
        vec![cached_finalized().clone()],
        "a terminal spends the reserve and closes without a spurious Dropped"
    );
    assert!(
        rx.is_disconnected(),
        "dropping the mux-side sender is what makes reader_pump exit its usual Err branch"
    );
}

#[test]
fn a_terminal_close_defers_behind_records_still_in_the_hold() {
    let (registry, rx, config) = bound();
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
        drain(&rx),
        vec![item(1), cached_finalized().clone()],
        "the consumer sees Finalized, not the Dropped an early close would have injected"
    );
    assert_eq!(registry.live_slots(peer()), 0);
}

#[test]
fn a_non_terminal_close_from_the_receiver_is_routed_to_the_batcher() {
    let (registry, _rx, config) = bound();
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
    let (registry, rx, config) = bound();
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

    assert_eq!(drain(&rx).len(), 4);
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
    let (tx, rx) = flume::bounded(
        crate::streaming::messenger_mux::flow_control::slot_buffer_depth(config.initial_credit),
    );
    registry.register_bind(ANCHOR, SESSION, tx);
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        for seq in 1..=2u32 {
            encoder.push_data(id, seq, &item(seq as u8)).unwrap();
        }
    });
    handle_batch(&registry, &config, None, peer(), &payload);
    assert_eq!(drain(&rx).len(), 2);

    // Occupancy is back to zero, so the watermark no longer binds and the
    // credit flows.
    assert_eq!(
        registry.sweep_credit(peer()),
        vec![ReplyRecord::CreditUpdate { slot: id, delta: 2 }]
    );
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

#[test]
fn shutdown_retires_every_slot() {
    let (registry, rx, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    assert_eq!(registry.shutdown(), 1);
    assert_eq!(registry.live_slots(peer()), 0);
    assert_eq!(drain(&rx), vec![cached_dropped().clone()]);
}

#[test]
fn a_heartbeat_record_reaches_the_consumer_as_a_heartbeat_frame() {
    let (registry, rx, config) = bound();
    let id = slot(0, 0);
    open(&registry, &config, id, 1);

    let payload = batch(1, 1, |encoder| {
        encoder.push_heartbeat(id, 1).unwrap();
    });
    handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(
        drain(&rx),
        vec![crate::streaming::sender::cached_heartbeat().clone()],
        "a heartbeat is a Data-class record: dropping one under saturation is \
         the per-slot saturation signal reader_pump's watchdog watches for"
    );
    assert_eq!(RecordType::SlotHeartbeat.as_u8(), 4);
}
