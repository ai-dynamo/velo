// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Credit replies form a batch for [`MuxConfig::reply_linger`] instead of each
//! writing one.
//!
//! What is pinned: replies alone wait for the window and go out together; a
//! close reply does not wait and carries the replies staged before it; a zero
//! window is the urgent flush the batcher had before. The windows here are
//! long (a second) and the waits short (tens of milliseconds), so the assertions
//! hold on a loaded runner without depending on how fast the peer admits.

use std::time::Duration;

use super::super::*;
use super::support::*;
use crate::streaming::messenger_mux::protocol::{CloseReason, RecordType};

const CREDIT: u32 = 64;
const WINDOW: Duration = Duration::from_secs(1);
const SOON: Duration = Duration::from_millis(250);

fn with_reply_linger(window: Duration) -> MuxConfig {
    MuxConfig {
        reply_linger: window,
        ..MuxConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn credit_replies_alone_form_one_batch_for_the_window() {
    let harness = harness(with_reply_linger(WINDOW)).await;
    let (_inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    for delta in 1..=3 {
        harness
            .handle
            .reply(&[ReplyRecord::CreditUpdate { slot, delta }]);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        harness.try_next_batch().is_none(),
        "nothing goes out before the window: three replies, no batch yet"
    );

    let batch = harness.next_batch().await;
    let credits: Vec<u32> = batch
        .records
        .iter()
        .filter(|r| r.kind == RecordType::CreditUpdate)
        .map(|r| r.credit)
        .collect();
    assert_eq!(
        credits,
        vec![1, 2, 3],
        "the window's replies go out together, in order"
    );
    assert_eq!(batch.records.len(), 3, "and nothing else rode along");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_close_reply_does_not_wait_and_carries_the_replies_before_it() {
    let harness = harness(with_reply_linger(WINDOW)).await;
    let (_inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    harness
        .handle
        .reply(&[ReplyRecord::CreditUpdate { slot, delta: 7 }]);
    tokio::time::sleep(Duration::from_millis(20)).await;
    harness.handle.reply(&[ReplyRecord::CloseSlot {
        slot,
        reason: CloseReason::UnknownSlot,
    }]);

    let batch = tokio::time::timeout(SOON, harness.next_batch())
        .await
        .expect("a close is liveness: it must not wait for the reply window");
    let kinds: Vec<RecordType> = batch.records.iter().map(|r| r.kind).collect();
    assert_eq!(
        kinds,
        vec![RecordType::CreditUpdate, RecordType::CloseSlot],
        "the close moves the reply staged before it, in the order they were owed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zero_window_writes_a_reply_at_once() {
    let harness = harness(with_reply_linger(Duration::ZERO)).await;
    let (_inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    harness
        .handle
        .reply(&[ReplyRecord::CreditUpdate { slot, delta: 7 }]);
    let batch = tokio::time::timeout(SOON, harness.next_batch())
        .await
        .expect("with the window off a reply is urgent, as before");
    assert_eq!(batch.records[0].kind, RecordType::CreditUpdate);
}
