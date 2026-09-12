// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What a batcher's outbound batches are made of, and what wakes it.
//!
//! `velo_streaming_mux_batches_total{direction="sent"}` counts batches and
//! `velo_streaming_mux_records_per_batch` counts records, and neither says
//! which records. A node whose batch count multiplies has either data arriving
//! one record at a time or control being flushed as it comes, and telling those
//! apart is what `velo_streaming_mux_records_sent_total{record_type}` and
//! `velo_streaming_mux_batcher_wakes_total{source}` are for.
//!
//! These tests pin two things beyond "the label matches the record": that a
//! record staged but never handed to the messenger — an epoch death, or the
//! task tearing down with a batch still open — is never counted as sent, and
//! that `batcher_wakes_total` counts the task's real wakes rather than the
//! items its drain loop pulls in behind one of them.

use std::sync::Arc;
use std::time::Duration;

use super::super::super::{AutoFlush, FlushPolicy};
use super::super::test_hooks::TestHooks;
use super::super::*;
use super::support::*;
use crate::streaming::messenger_mux::protocol::RecordType;
use crate::streaming::sender::cached_finalized;

const CREDIT: u32 = 64;

fn sent(harness: &Harness, kind: &str) -> f64 {
    harness.snapshot().counter(
        "velo_streaming_mux_records_sent_total",
        &[("record_type", kind)],
    )
}

fn wakes(harness: &Harness, source: &str) -> f64 {
    harness.snapshot().counter(
        "velo_streaming_mux_batcher_wakes_total",
        &[("source", source)],
    )
}

fn manual() -> MuxConfig {
    MuxConfig {
        flush_policy: FlushPolicy::Manual,
        ..MuxConfig::default()
    }
}

fn auto(on_admission: bool, max_linger: Option<Duration>) -> MuxConfig {
    MuxConfig {
        flush_policy: FlushPolicy::Auto(AutoFlush {
            on_admission,
            max_linger,
        }),
        ..MuxConfig::default()
    }
}

// ---------------------------------------------------------------------------
// velo_streaming_mux_records_sent_total
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sent_records_are_counted_by_type() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, slot) = harness.open_credited(1, 1, CREDIT).await;
    assert_eq!(sent(&harness, "open_slot"), 1.0, "the open went out alone");

    // Wire-decoded tally, independent of `records_sent_total`: every record
    // counted here comes from `OwnedBatch::decode`'s `record.kind`, which
    // reads the wire byte `RecordType::as_u8` writes; `records_sent_total`
    // is read out of `BatchEncoder::record_type_counts`, an array
    // `RecordType::count_index` indexes, in the same `push` call. Seeded at
    // 1 for the `OpenSlot` batch `open_credited` already decoded and pinned
    // above.
    let mut wire_total: u64 = 1;

    inlet.send(item(0)).expect("queue record");
    inlet.send(item(1)).expect("queue record");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    let mut data = 0;
    let mut closed = false;
    while !closed {
        for record in harness.next_batch().await.records {
            wire_total += 1;
            match record.kind {
                RecordType::Data => data += 1,
                RecordType::CloseSlot => closed = true,
                other => panic!("unexpected {other:?} on the wire"),
            }
        }
    }
    assert_eq!(data, 3, "two items and the terminal");
    assert_eq!(sent(&harness, "data"), 3.0);
    assert_eq!(sent(&harness, "close_slot"), 1.0, "the terminal's close");

    // A reply names a slot the peer owns; it is control this side sends back.
    harness
        .handle
        .reply(&[ReplyRecord::CreditUpdate { slot, delta: 5 }]);
    let batch = harness.next_batch().await;
    wire_total += batch.records.len() as u64;
    assert_eq!(batch.records[0].kind, RecordType::CreditUpdate);
    assert_eq!(sent(&harness, "credit_update"), 1.0);
    assert_eq!(
        sent(&harness, "open_slot"),
        1.0,
        "nothing else is filed as an open"
    );

    // The per-type assertions above already pair a wire-decoded kind with
    // its registry counter, which is what catches a `count_index` bug: a
    // wrong or colliding index mislabels the registry series while leaving
    // the wire byte -- written from the same `record_type` argument, by
    // `RecordType::as_u8`, in the same `push` call -- correct. This closes
    // the total: comparing the registry's summed count to `wire_total`,
    // decoded independently of `record_type_counts`, catches a `push` call
    // site that reaches the wire without incrementing `record_type_counts`
    // at all (or the reverse). Comparing this counter's sum to
    // `records_per_batch` instead, as an earlier version of this assertion
    // did, cannot fail: both are derived from the same `counts` array inside
    // `batch_sent`.
    let total_sent = harness
        .snapshot()
        .counter_sum("velo_streaming_mux_records_sent_total", &[]);
    assert_eq!(
        total_sent, wire_total as f64,
        "every record decoded off the wire is counted, in total, by type"
    );
}

/// An over-budget record rides the singleton path (`writer::dispatch_singleton`)
/// rather than the packed-batch one (`writer::flush`) — the two places that
/// read a batch's per-type counts out of its encoder, so both need the count.
#[tokio::test(flavor = "multi_thread")]
async fn singleton_records_are_counted_by_type() {
    let cap = 256;
    let harness = harness(MuxConfig {
        max_batch_bytes: cap,
        ..MuxConfig::default()
    })
    .await;
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    let before = sent(&harness, "data");
    let singletons_before = harness
        .snapshot()
        .counter("velo_streaming_mux_rendezvous_singletons_total", &[]);

    let oversized = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![
        7u8;
        cap * 2
    ]))
    .expect("encode oversized");
    inlet.send(oversized.clone()).expect("queue oversized");

    let batch = harness.next_batch().await;
    assert_eq!(
        batch.records.len(),
        1,
        "an over-budget record travels alone through rendezvous"
    );
    assert_eq!(batch.records[0].data, oversized);
    assert_eq!(
        sent(&harness, "data") - before,
        1.0,
        "the singleton path packs a record too, and must count it"
    );
    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_rendezvous_singletons_total", &[])
            - singletons_before,
        1.0,
        "pins that this record actually rode dispatch_singleton, the only \
         caller of MuxMetricsHandle::rendezvous_singleton, rather than a \
         one-record BatchWriter::flush that happened to look the same"
    );
}

/// A record staged into an open batch is counted only when that batch is
/// handed to the messenger — never when it is staged. Cancelling the batcher
/// while a record sits staged and unflushed exercises the same discard
/// `FlushGate::discarded` names for the staged-records gauge: the batch dies
/// with the task, and nothing reached the wire.
#[tokio::test(flavor = "multi_thread")]
async fn sent_records_exclude_batches_discarded_before_flush() {
    let harness = harness(manual()).await;
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    let before = sent(&harness, "data");

    inlet
        .send(item(0))
        .expect("stage a record that is never flushed");
    harness.await_staged(1).await;

    harness.cancel.cancel();
    harness.await_staged(0).await;

    assert_eq!(
        sent(&harness, "data") - before,
        0.0,
        "a record staged but discarded at teardown must never show up as sent"
    );
}

/// The other discard this module's doc names: a staged batch whose epoch
/// dies underneath it. `Batcher::epoch_death` calls `BatchWriter::reset_epoch`
/// (`writer.rs`), which drops the encoder along with everything staged —
/// the same mechanism `sent_records_exclude_batches_discarded_before_flush`
/// pins for teardown, triggered here by the resolution a failed rendezvous
/// admission delivers, instead of by cancellation.
#[tokio::test(flavor = "multi_thread")]
async fn sent_records_exclude_batches_discarded_by_epoch_death() {
    let harness = harness(manual()).await;
    let (inlet, id) = harness.open_credited(1, 1, CREDIT).await;
    let before = sent(&harness, "data");

    inlet
        .send(item(0))
        .expect("stage a record that is never flushed");
    harness.await_staged(1).await;

    harness.handle.control.singleton_resolved(id, false);
    harness.await_staged(0).await;

    assert_eq!(
        sent(&harness, "data") - before,
        0.0,
        "a record staged but discarded by an epoch death must never show up as sent"
    );
}

// ---------------------------------------------------------------------------
// velo_streaming_mux_batcher_wakes_total
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn wakes_are_counted_by_source() {
    let harness = harness(MuxConfig::default()).await;
    let open_before = wakes(&harness, "open");
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    assert_eq!(
        wakes(&harness, "open") - open_before,
        1.0,
        "the open woke the batcher exactly once"
    );

    let frame_before = wakes(&harness, "frame");
    inlet.send(item(0)).expect("queue record");
    let _ = harness.next_batch().await;
    assert_eq!(
        wakes(&harness, "frame") - frame_before,
        1.0,
        "a queued record woke it exactly once"
    );

    let control_before = wakes(&harness, "control");
    harness.flush_batch();
    eventually(|| wakes(&harness, "control") - control_before >= 1.0).await;
    assert_eq!(
        wakes(&harness, "control") - control_before,
        1.0,
        "one kick is one coalesced control wake, not one per caller"
    );

    let inlet_closed_before = wakes(&harness, "inlet_closed");
    drop(inlet);
    let close = harness.next_batch().await;
    assert_eq!(close.records[0].kind, RecordType::CloseSlot);
    assert_eq!(
        wakes(&harness, "inlet_closed") - inlet_closed_before,
        1.0,
        "a departed producer woke it exactly once"
    );
}

/// The fifth source: nothing arrives, and the linger window itself is what
/// wakes the batcher's select.
#[tokio::test(flavor = "multi_thread")]
async fn linger_wake_is_counted() {
    let harness = harness(auto(false, Some(Duration::from_millis(50)))).await;
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    let before = wakes(&harness, "linger");

    inlet.send(item(0)).expect("stage a record");
    // `next_batch` already times out at `RECV_TIMEOUT` — the window must fire
    // on its own for this to return at all — so the assertion below is what
    // proves it was the linger deadline and not just any wake.
    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        wakes(&harness, "linger") - before,
        1.0,
        "the linger deadline is what woke the batcher, not the record's arrival"
    );
}

/// `dispatch` is called both from the loop's `select!` — a real wake — and
/// from `drain_once`, which pulls whatever is already queued without waiting
/// for another one. Three records queued while the loop is parked mid-wake
/// (via the barrier `test_hooks` offers) land in one batch from one wake; a
/// counter that fires once per dispatched item instead reports three.
#[tokio::test(flavor = "multi_thread")]
async fn wakes_count_true_wakes_not_drained_items() {
    let hooks = Arc::new(TestHooks::default());
    let harness = harness_with_hooks(MuxConfig::default(), Some(Arc::clone(&hooks))).await;
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    let before = wakes(&harness, "frame");

    hooks.pause();
    inlet
        .send(item(0))
        .expect("the record that wakes the batcher");
    hooks.wait_until_parked().await;

    // Queued while the loop sits at the barrier, after dispatching item 0's
    // wake and before its drain loop runs — the shape a real wake's drain
    // pulls in without a second poll of the select.
    inlet.send(item(1)).expect("queue a second record");
    inlet.send(item(2)).expect("queue a third record");
    hooks.release();

    let batch = harness.next_batch().await;
    assert_eq!(
        batch.records.len(),
        3,
        "the drain loop carries all three into the wake's batch"
    );
    assert_eq!(
        wakes(&harness, "frame") - before,
        1.0,
        "one select wake produced this batch; drain_once pulling the other \
         two records is not a second and third wake"
    );
}
