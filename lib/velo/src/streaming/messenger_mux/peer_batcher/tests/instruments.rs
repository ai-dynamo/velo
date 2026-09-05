// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What a batcher's outbound batches are made of, and what wakes it.
//!
//! `velo_streaming_mux_batches_total{direction="sent"}` counts batches and
//! `velo_streaming_mux_records_per_batch` counts records, and neither says
//! which records. A node whose batch count multiplies has either data arriving
//! one record at a time or control being flushed as it comes, and telling those
//! apart is what `velo_streaming_mux_records_sent_total{record_type}` and
//! `velo_streaming_mux_batcher_wakes_total{source}` are for. These tests pin
//! that every record the batcher packs is filed under its type, and that every
//! way into `dispatch` is filed under its source.

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

#[tokio::test(flavor = "multi_thread")]
async fn sent_records_are_counted_by_type() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, slot) = harness.open_credited(1, 1, CREDIT).await;
    assert_eq!(sent(&harness, "open_slot"), 1.0, "the open went out alone");

    inlet.send(item(0)).expect("queue record");
    inlet.send(item(1)).expect("queue record");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    let mut data = 0;
    let mut closed = false;
    while !closed {
        for record in harness.next_batch().await.records {
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
    assert_eq!(batch.records[0].kind, RecordType::CreditUpdate);
    assert_eq!(sent(&harness, "credit_update"), 1.0);
    assert_eq!(
        sent(&harness, "open_slot"),
        1.0,
        "nothing else is filed as an open"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn wakes_are_counted_by_source() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;
    assert!(wakes(&harness, "open") >= 1.0, "the open woke the batcher");

    inlet.send(item(0)).expect("queue record");
    let _ = harness.next_batch().await;
    assert!(wakes(&harness, "frame") >= 1.0, "a queued record woke it");

    harness.flush_batch();
    eventually(|| wakes(&harness, "control") >= 1.0).await;

    drop(inlet);
    let close = harness.next_batch().await;
    assert_eq!(close.records[0].kind, RecordType::CloseSlot);
    assert!(
        wakes(&harness, "inlet_closed") >= 1.0,
        "a departed producer woke it"
    );
}
