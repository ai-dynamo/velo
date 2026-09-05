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

/// Characterizes a known gap, not desired behaviour: a reply staged inside the
/// window this test exercises can be discarded before it ever reaches the
/// wire, and the credit it carried does not come back.
///
/// `on_control` applies `drained.peers` (which can stage a `CreditUpdate`
/// through `FlushGate::stage_reply`) before `drained.mine` (whose
/// failed-singleton arm calls `epoch_death`), and `epoch_death` discards
/// whatever is staged with no flush in between — see `FlushGate::discarded`'s
/// doc comment for the full enumeration of why this is the one reachable
/// hole. `reply_linger` is what makes the hole span more than one drain: the
/// reply can sit staged, unwritten, for up to the window configured here.
///
/// The credit is not recoverable elsewhere either: `take_pending_grant`
/// already zeroed the ingress account's `ungranted` delta the moment this
/// reply was minted, and nothing later re-derives it at the same occupancy.
/// Fixing that is a larger, separate change (re-post the pending grant on
/// discard, or don't zero `ungranted` until the batch is admitted) — this
/// test only pins what happens today, so a change to it is deliberate rather
/// than a silent regression.
#[tokio::test(flavor = "multi_thread")]
async fn epoch_death_discards_a_staged_reply_and_the_credit_is_lost() {
    // Long enough that nothing here can flush the reply on its own clock
    // before `singleton_resolved` reaches it.
    let harness = harness(with_reply_linger(Duration::from_secs(5))).await;
    let (_inlet, slot) = harness.open(1, 1).await;

    harness
        .handle
        .reply(&[ReplyRecord::CreditUpdate { slot, delta: 7 }]);
    harness.await_staged(1).await;

    harness.handle.control.singleton_resolved(slot, false);
    harness.await_staged(0).await;

    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[]),
        1.0,
        "the failed singleton must have failed the epoch, which is what \
         discards the batch the reply was staged into"
    );

    let mut sent_a_credit_update = false;
    while let Some(batch) = harness.try_next_batch() {
        sent_a_credit_update |= batch
            .records
            .iter()
            .any(|r| r.kind == RecordType::CreditUpdate);
    }
    assert!(
        !sent_a_credit_update,
        "the staged reply must never have reached the wire — this is the \
         loss the doc comment describes, not proof it was avoided"
    );
}
