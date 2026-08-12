// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Packing, clamps, credit, terminals, oversized records, and the
//! unconditional inlet drain.

use std::time::Duration;

use super::super::*;
use super::support::*;
use crate::streaming::messenger_mux::protocol::RecordType;
use crate::streaming::sender::cached_finalized;
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

// ---------------------------------------------------------------------------
// Packing
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn one_batch_carries_records_from_several_slots() {
    let harness = harness(MuxConfig::default()).await;

    let mut slots = Vec::new();
    for session in 0..3u64 {
        slots.push(harness.open(1, session).await);
    }

    // Queue on parked slots: with no credit the batcher pulls one record per
    // slot, withholds it and stops draining. Everything else sits in the inlet.
    for (inlet, _) in &slots {
        for n in 0..4u32 {
            inlet.send(item(n)).expect("queue record");
        }
    }
    eventually(|| harness.try_next_batch().is_none()).await;

    // One grant per slot, all queued before the batcher can flush the first:
    // the opportunistic drain takes all three plus every queued record.
    for (_, id) in &slots {
        harness.grant(*id, 8);
    }

    let mut seen = std::collections::BTreeMap::<u32, usize>::new();
    let mut multi_slot_batches = 0;
    while seen.values().sum::<usize>() < 12 {
        let batch = harness.next_batch().await;
        if batch.slots().len() > 1 {
            multi_slot_batches += 1;
        }
        for record in &batch.records {
            if record.kind == RecordType::Data {
                *seen.entry(record.slot.index()).or_default() += 1;
            }
        }
    }

    assert_eq!(seen.len(), 3, "every slot must have been drained");
    assert!(
        seen.values().all(|count| *count == 4),
        "records lost or duplicated: {seen:?}"
    );
    assert!(
        multi_slot_batches > 0,
        "the point of bucketing by destination is that one batch carries several streams"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_configured_cap_bounds_every_batch() {
    // Small enough that a handful of records fills it, large enough that a
    // single record still fits eagerly.
    let cap = 128;
    let harness = harness(MuxConfig {
        max_batch_bytes: cap,
        ..MuxConfig::default()
    })
    .await;

    let (inlet, id) = harness.open(1, 1).await;
    for n in 0..12u32 {
        inlet.send(item(n)).expect("queue record");
    }
    eventually(|| harness.try_next_batch().is_none()).await;
    harness.grant(id, 32);

    let mut delivered = 0;
    while delivered < 12 {
        let batch = harness.next_batch().await;
        assert!(
            batch.encoded_len <= cap,
            "batch of {} bytes exceeds the {cap}-byte cap",
            batch.encoded_len
        );
        delivered += batch.records.len();
    }
    assert_eq!(delivered, 12);
}

/// The coalescing threshold is the clamp that binds when nothing else does.
///
/// It is the packing *target*, not merely a ceiling: the shared coalescing
/// writer stages a frame into one buffered `write_all` only while
/// `header + payload` fits under it, so a batch above the threshold gives back
/// exactly what batching bought. With a configured cap far above it and a
/// transport reporting megabytes of eager budget, this is the arm that has to
/// hold — and the default configuration sits just under the threshold, which
/// would hide a regression here forever.
#[tokio::test(flavor = "multi_thread")]
async fn the_coalescing_threshold_bounds_a_batch_when_the_configured_cap_does_not() {
    let harness = harness(MuxConfig {
        max_batch_bytes: 1 << 20,
        ..MuxConfig::default()
    })
    .await;

    let (inlet, id) = harness.open(1, 1).await;
    let payload = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![7u8; 1000]))
        .expect("encode payload");
    const RECORDS: usize = 100;
    for _ in 0..RECORDS {
        inlet.send(payload.clone()).expect("queue record");
    }
    eventually(|| harness.try_next_batch().is_none()).await;
    harness.grant(id, 256);

    let mut delivered = 0;
    let mut batches = 0;
    let mut largest = 0;
    while delivered < RECORDS {
        let batch = harness.next_batch().await;
        assert!(
            batch.encoded_len <= COALESCE_THRESHOLD,
            "batch of {} bytes is over the {COALESCE_THRESHOLD}-byte coalescing threshold",
            batch.encoded_len
        );
        largest = largest.max(batch.encoded_len);
        batches += 1;
        delivered += batch.records.len();
    }
    assert_eq!(delivered, RECORDS);
    assert!(
        batches > 1,
        "100 KiB of records has to be cut into more than one batch"
    );
    assert!(
        largest > COALESCE_THRESHOLD / 2,
        "the threshold, not some smaller clamp, is what bound these batches: \
         largest was {largest} bytes"
    );
}

// ---------------------------------------------------------------------------
// The inlet is drained unconditionally
// ---------------------------------------------------------------------------

/// A starved slot must not be able to block a producer's *synchronous* send.
///
/// `finalize`, `detach` and `Drop` reach the inlet through `flume::Sender::send`,
/// which blocks on a full channel — and `Drop` does it from inside async
/// context, on a runtime worker thread. Under TCP a full channel drains at
/// socket speed, so the block is transient. Under mux a slot with no credit
/// would never drain at all, so it would be permanent. The withheld queue is
/// what keeps the channel moving.
#[tokio::test(flavor = "multi_thread")]
async fn a_starved_slot_keeps_draining_so_a_synchronous_terminal_never_blocks() {
    let harness = harness(MuxConfig::default()).await;
    // A shallow inlet, so a producer meets the channel's limit almost at once.
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 4).await;

    // No credit is ever granted, so nothing this slot holds may be sent.
    const RECORDS: u32 = 32;
    for n in 0..RECORDS {
        tokio::time::timeout(RECV_TIMEOUT, inlet.send_async(item(n)))
            .await
            .expect("a starved slot must not stall its producer")
            .expect("inlet open");
    }

    // The blocking call the hazard is about. On a blocking pool so that a
    // regression stalls this test rather than the whole runtime.
    let terminal_inlet = inlet.clone();
    let sent = tokio::time::timeout(
        RECV_TIMEOUT,
        tokio::task::spawn_blocking(move || terminal_inlet.send(cached_finalized().clone())),
    )
    .await
    .expect("a synchronous terminal send must not block on a starved slot")
    .expect("blocking task");
    assert!(sent.is_ok());

    assert!(
        harness.try_next_batch().is_none(),
        "nothing may reach the wire without credit"
    );

    // Everything the producer handed over is still there, in order, and comes
    // out the moment credit does.
    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < RECORDS as usize + 2 {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().take(RECORDS as usize).enumerate() {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    assert_eq!(records[RECORDS as usize].data, *cached_finalized());
    assert_eq!(records[RECORDS as usize + 1].kind, RecordType::CloseSlot);
}

/// Run-ahead past the byte cap kills that slot, and only that slot.
///
/// This is the per-slot slow-consumer kill `BATCHING.md` prefers to the
/// heartbeat watchdog: deterministic, scoped, and metered.
#[tokio::test(flavor = "multi_thread")]
async fn withheld_overflow_closes_the_starved_slot_and_leaves_the_others_alone() {
    let harness = harness(MuxConfig {
        slot_byte_budget: 256,
        ..MuxConfig::default()
    })
    .await;

    let (starved_inlet, (starved, _)) = harness.open_with_inlet(1, 1, 256).await;
    let (flowing_inlet, (flowing, _)) = harness.open_with_inlet(1, 2, 256).await;
    harness.grant(flowing, 64);

    // Never granted, so everything piles into the withheld queue until the cap.
    for n in 0..64u32 {
        let _ = starved_inlet.send(item(n));
    }
    eventually(|| starved_inlet.is_disconnected()).await;

    let mut closed = None;
    let mut flowing_seen = 0;
    for n in 0..4u32 {
        flowing_inlet.send(item(100 + n)).expect("flowing send");
    }
    while closed.is_none() || flowing_seen < 4 {
        let batch = harness.next_batch().await;
        for record in batch.records {
            if record.slot == starved && record.kind == RecordType::CloseSlot {
                closed = Some(record);
            } else if record.slot == flowing {
                flowing_seen += 1;
            }
        }
    }
    assert!(
        closed.is_some(),
        "the consumer has to be told, or it waits out its heartbeat watchdog"
    );
    assert!(
        !flowing_inlet.is_disconnected(),
        "the peer's other slots are untouched"
    );
    assert!(
        harness.snapshot().counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "withheld_overflow")]
        ) > 0.0
    );
}

/// A producer that goes while records are still withheld owes them first.
#[tokio::test(flavor = "multi_thread")]
async fn a_departed_producer_s_withheld_records_go_before_its_close() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 64).await;

    for n in 0..4u32 {
        inlet.send(item(n)).expect("queue record");
    }
    drop(inlet);
    // The records are parked, not merely un-sent: the inlet drained even though
    // the slot has no credit, which is the property the close has to wait on.
    harness.await_withheld(4).await;
    assert!(harness.try_next_batch().is_none());

    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < 5 {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().take(4).enumerate() {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    assert_eq!(
        records[4].kind,
        RecordType::CloseSlot,
        "the close a departed producer owes waits behind what it enqueued"
    );
}

/// A terminal already in the queue closes the slot; no `PeerGone` follows it.
#[tokio::test(flavor = "multi_thread")]
async fn a_withheld_terminal_closes_the_slot_without_a_second_close() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, (id, _)) = harness.open_with_inlet(1, 1, 64).await;

    inlet.send(item(1)).expect("queue item");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    drop(inlet);
    // Both parked: the terminal is behind a predecessor, so its reserve does
    // not apply and it waits its turn.
    harness.await_withheld(2).await;
    assert!(harness.try_next_batch().is_none());

    harness.grant(id, 64);
    let mut records = Vec::new();
    while records.len() < 3 {
        records.extend(harness.next_batch().await.records);
    }
    assert_eq!(records[0].data, item(1));
    assert_eq!(records[1].data, *cached_finalized());
    assert_eq!(records[2].kind, RecordType::CloseSlot);
    assert_eq!(
        records.len(),
        3,
        "the terminal already closed the slot, so the departed inlet adds nothing"
    );
    // Nothing further: a second close would tell the consumer to inject
    // `Dropped` behind a `Finalized` it has already seen.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(harness.try_next_batch().is_none());
}

/// A terminal at the head of the queue spends the reserve without a grant.
///
/// The reserve exists for exactly this: a consumer that has stopped draining
/// will never return credit, so a terminal that waited for one would wait
/// forever and the stream would end on the heartbeat watchdog instead of on the
/// `Finalized` its producer actually sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_spends_the_reserve_when_data_credit_is_gone() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open(1, 1).await;

    // Exactly one data credit, spent by the first record.
    harness.grant(id, 1);
    inlet.send(item(0)).expect("queue item");
    let first = harness.next_batch().await;
    assert_eq!(first.records[0].data, item(0));

    // No predecessors queued, no further grant coming: the terminal must go out
    // on the reserve alone.
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    let mut records = Vec::new();
    while records.len() < 2 {
        records.extend(harness.next_batch().await.records);
    }
    assert_eq!(records[0].data, *cached_finalized());
    assert_eq!(
        records[1].kind,
        RecordType::CloseSlot,
        "the terminal's close still rides the same batch"
    );
    assert_eq!(harness.withheld(), 0.0, "the terminal was sent, not parked");
}

/// The reserve is for the queue *head*, so a terminal behind stuck predecessors
/// stays stuck with them.
///
/// Pinned because it is the limit of what the reserve buys and it is easy to
/// mistake for a bug. Letting the terminal past would reorder the stream — the
/// consumer would see the end before records it is owed — so the terminal waits,
/// and what ends the stream is `reader_pump`'s heartbeat watchdog on the
/// consumer's side, which is the mechanism `SATURATION.md` documents for a
/// consumer that stopped draining. The other exit is the byte cap: a producer
/// that keeps sending gets the per-slot kill instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_behind_starved_predecessors_waits_for_them() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open(1, 1).await;

    // No credit at all: the data records cannot move, so neither can what is
    // behind them.
    for n in 0..3u32 {
        inlet.send(item(n)).expect("queue record");
    }
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    harness.await_withheld(4).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        harness.try_next_batch().is_none(),
        "the terminal must not overtake records the consumer is owed"
    );
    assert_eq!(harness.withheld(), 4.0, "all four are still parked");
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        1.0,
        "the slot stays open; it is the consumer's watchdog that ends this stream"
    );

    // The moment the consumer resumes, the whole queue drains in order and the
    // terminal closes the slot behind it.
    harness.grant(id, 8);
    let mut records = Vec::new();
    while records.len() < 5 {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().take(3).enumerate() {
        assert_eq!(record.data, item(n as u32));
    }
    assert_eq!(records[3].data, *cached_finalized());
    assert_eq!(records[4].kind, RecordType::CloseSlot);
}

/// A grant that covers only part of the queue drains only that part, in order.
///
/// The path this pins is `release_withheld`'s peek-then-pop: a record inspected
/// and found unaffordable must stay where it is. Popping it and putting it back
/// would send it to the *back* of the queue, which is a reordering the wire has
/// no way to detect.
#[tokio::test(flavor = "multi_thread")]
async fn a_partial_grant_drains_the_queue_head_and_leaves_the_rest_in_order() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open_with_inlet(1, 1, 64).await;
    let id = id.0;

    const RECORDS: u32 = 8;
    for n in 0..RECORDS {
        inlet.send(item(n)).expect("queue record");
    }
    harness.await_withheld(RECORDS as usize).await;

    // Three credits against eight records: the fourth peek misses and the
    // remaining five have to stay put.
    harness.grant(id, 3);
    let mut records = Vec::new();
    while records.len() < 3 {
        records.extend(harness.next_batch().await.records);
    }
    harness.await_withheld((RECORDS - 3) as usize).await;
    assert!(
        harness.try_next_batch().is_none(),
        "a partial grant must not drain past what it paid for"
    );

    harness.grant(id, 16);
    while records.len() < RECORDS as usize {
        records.extend(harness.next_batch().await.records);
    }
    for (n, record) in records.iter().enumerate() {
        assert_eq!(
            record.data,
            item(n as u32),
            "record {n} out of order across the credit miss"
        );
    }
}

// ---------------------------------------------------------------------------
// Credit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_starved_slot_is_withheld_while_the_others_keep_flowing() {
    let harness = harness(MuxConfig::default()).await;

    let (starved_inlet, starved) = harness.open(1, 1).await;
    let (flowing_inlet, flowing) = harness.open(1, 2).await;

    for n in 0..4u32 {
        starved_inlet.send(item(n)).expect("queue starved");
        flowing_inlet.send(item(100 + n)).expect("queue flowing");
    }

    // Only the second slot gets credit. The starved slot's records are pulled
    // all the same — they wait in its withheld queue, not in its inlet.
    harness.grant(flowing, 8);

    let mut flowing_records = 0;
    while flowing_records < 4 {
        let batch = harness.next_batch().await;
        for record in &batch.records {
            assert_eq!(
                record.slot, flowing,
                "a slot with no credit must not put anything on the wire"
            );
            flowing_records += 1;
        }
    }

    assert!(
        harness.try_next_batch().is_none(),
        "the starved slot has nothing admissible"
    );
    assert!(
        harness
            .snapshot()
            .counter("velo_streaming_slot_credit_exhausted_total", &[])
            > 0.0,
        "starvation is a per-slot event worth an operator's attention"
    );

    // The grant is the un-park: the withheld record and everything behind it
    // flow without the producer having done anything.
    harness.grant(starved, 8);
    let mut starved_records = 0;
    while starved_records < 4 {
        let batch = harness.next_batch().await;
        starved_records += batch
            .records
            .iter()
            .filter(|record| record.slot == starved)
            .count();
    }
}

// ---------------------------------------------------------------------------
// Terminals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_and_its_close_ride_the_same_batch() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, id) = harness.open(1, 1).await;

    inlet.send(item(1)).expect("queue item");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    // Frames queued behind a terminal are discarded — today's semantics,
    // correctly scoped to the one slot.
    inlet.send(item(2)).expect("queue after terminal");
    harness.grant(id, 8);

    let mut records = Vec::new();
    while !records
        .iter()
        .any(|r: &OwnedRecord| r.kind == RecordType::CloseSlot)
    {
        records.extend(harness.next_batch().await.records);
    }

    let close_at = records
        .iter()
        .position(|r| r.kind == RecordType::CloseSlot)
        .expect("close present");
    assert_eq!(
        records[close_at - 1].data,
        *cached_finalized(),
        "the close must sit immediately behind its terminal"
    );
    assert_eq!(records[close_at].slot, id);
    assert!(
        !records[close_at + 1..]
            .iter()
            .any(|r| r.slot == id && r.kind == RecordType::Data),
        "nothing queued behind the terminal may reach the wire"
    );

    eventually(|| inlet.is_disconnected()).await;
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        0.0,
        "a terminal frees its slot"
    );
}

// ---------------------------------------------------------------------------
// Oversized records
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_record_goes_alone_and_fences_only_its_slot() {
    let cap = 256;
    let harness = harness(MuxConfig {
        max_batch_bytes: cap,
        ..MuxConfig::default()
    })
    .await;

    let (big_inlet, big) = harness.open(1, 1).await;
    let (small_inlet, small) = harness.open(1, 2).await;
    harness.grant(big, 8);
    harness.grant(small, 8);

    let oversized = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![
        7u8;
        cap * 2
    ]))
    .expect("encode oversized");
    big_inlet.send(oversized.clone()).expect("queue oversized");
    for n in 0..4u32 {
        small_inlet.send(item(n)).expect("queue small");
    }

    let mut small_seen = 0;
    let mut singleton_seen = false;
    while small_seen < 4 || !singleton_seen {
        let batch = harness.next_batch().await;
        if batch.records.iter().any(|r| r.slot == big) {
            assert_eq!(
                batch.records.len(),
                1,
                "an over-budget record travels alone so the rendezvous round trip \
                 is not charged to unrelated slots"
            );
            assert_eq!(batch.records[0].data, oversized);
            singleton_seen = true;
        }
        small_seen += batch.records.iter().filter(|r| r.slot == small).count();
    }

    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_rendezvous_singletons_total", &[]),
        1.0
    );

    // The fence lifts once the singleton's admission resolves, and the slot's
    // later records follow it in order.
    big_inlet.send(item(9)).expect("queue successor");
    let mut successor = None;
    while successor.is_none() {
        let batch = harness.next_batch().await;
        successor = batch.records.into_iter().find(|r| r.slot == big);
    }
    let successor = successor.expect("successor delivered");
    assert_eq!(successor.data, item(9));
    assert_eq!(
        successor.frame_seq, 2,
        "frame_seq carries the order proof past a resolve that is not lane-ordered"
    );
}
