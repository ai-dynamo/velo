// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Credit replies form a batch for [`MuxConfig::reply_linger`] instead of each
//! writing one.
//!
//! What is pinned: replies alone wait for the window and go out together
//! through the `linger` timer, not some incidental wake; a close reply does
//! not wait and carries the replies staged before it; a zero window is the
//! urgent flush the batcher had before. The windows here are long (a second)
//! and the waits short (tens of milliseconds), so the assertions hold on a
//! loaded runner without depending on how fast the peer admits.

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

fn wakes(harness: &Harness, source: &str) -> f64 {
    harness.snapshot().counter(
        "velo_streaming_mux_batcher_wakes_total",
        &[("source", source)],
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn credit_replies_alone_form_one_batch_for_the_window() {
    let harness = harness(with_reply_linger(WINDOW)).await;
    let (_inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    // `await_staged` after each reply is the positive fact: it proves this
    // reply was already staged as its own record before the next one is sent,
    // so two replies to the same slot cannot coalesce in the control inbox
    // into one record with a merged delta before the batcher ever sees them
    // separately.
    for (staged, delta) in (1..=3u32).enumerate() {
        harness
            .handle
            .reply(&[ReplyRecord::CreditUpdate { slot, delta }]);
        harness.await_staged(staged + 1).await;
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
    assert!(
        wakes(&harness, "linger") >= 1.0,
        "the batch must have arrived through the reply window's own timer, \
         not some incidental wake"
    );
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

/// The credit a discarded batch was carrying comes back rather than dying
/// with the batch.
///
/// `take_pending_grant` zeroes the ingress account's `ungranted` the moment
/// the reply is minted, so from then until the write the batch *is* the
/// credit's only copy. `epoch_death` throws that batch away, which is why the
/// batcher hands the credit back to its own control state on the way out: the
/// slot it belongs to is an ingress slot, and `close_all` does not close those
/// — it closes this side's egress slots — so the sender is still waiting on a
/// window that nothing else re-derives.
///
/// Kicked each turn rather than waited out: the window here is five seconds,
/// chosen so the staging above cannot flush on its own clock, and the
/// re-posted credit would otherwise sit in it.
#[tokio::test(flavor = "multi_thread")]
async fn epoch_death_returns_the_credit_its_discarded_batch_carried() {
    let harness = harness(with_reply_linger(Duration::from_secs(5))).await;
    let (_inlet, slot) = harness.open(1, 1).await;

    harness
        .handle
        .reply(&[ReplyRecord::CreditUpdate { slot, delta: 7 }]);
    harness.await_staged(1).await;

    harness.handle.control.singleton_resolved(slot, false);

    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    let mut returned = None;
    while tokio::time::Instant::now() < deadline && returned.is_none() {
        harness.flush_batch();
        if let Some(batch) = harness.try_next_batch() {
            returned = batch
                .records
                .iter()
                .find(|r| r.kind == RecordType::CreditUpdate)
                .map(|r| r.credit);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[]),
        1.0,
        "the failed singleton must have failed the epoch — otherwise this test \
         never exercised a discard at all"
    );
    assert_eq!(
        returned,
        Some(7),
        "the discarded batch's credit must reach the wire on a later one"
    );
    let snapshot = harness.snapshot();
    assert_eq!(
        snapshot.counter("velo_streaming_mux_credit_reposted_total", &[]),
        7.0,
        "the recovery must be visible, and for the whole delta"
    );
    assert_eq!(
        snapshot.counter("velo_streaming_mux_credit_lost_total", &[]),
        0.0,
        "a live batcher's inbox takes the re-post, so nothing is lost here"
    );
}
