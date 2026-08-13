// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What each [`FlushPolicy`] does with a staged batch.
//!
//! Read against `egress.rs`, which pins the same batcher under the default
//! policy: everything there is the regression that `Auto` did not change. What
//! is asserted here is the difference — that `Manual` holds ordinary records
//! and only ordinary records, that a kick moves them, and that neither policy
//! can hold back the records something else is waiting on.
//!
//! Every test drives the real batcher against a real messenger, so the
//! assertions are the decoded wire bytes rather than an accounting mirror.

use std::sync::Arc;
use std::time::Duration;

use super::super::super::{AutoFlush, FlushPolicy};
use super::support::{Harness, RECV_TIMEOUT, harness, harness_with_hooks, item};
use crate::streaming::messenger_mux::MuxConfig;
use crate::streaming::messenger_mux::protocol::{CloseReason, RecordType, SlotId};
use crate::streaming::sender::cached_finalized;

/// Credit generous enough that nothing in this file parks on it — these tests
/// are about the flush decision, and a starved slot never reaches one.
const CREDIT: u32 = 4096;

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

/// Nothing arrived within a window several times the batcher's own latency.
///
/// A negative fact, so it is worth being explicit about what makes it sound:
/// every caller first establishes a *positive* one — `await_staged`, or the
/// receipt of the eager `OpenSlot` batch — proving the batcher has already run
/// and made its decision. Without that, this would pass against a batcher that
/// had simply not woken yet.
async fn assert_nothing_written(harness: &Harness) {
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        harness.try_next_batch().is_none(),
        "the policy said hold, so nothing may reach the wire"
    );
}

// ---------------------------------------------------------------------------
// Manual — the application owns the flush
// ---------------------------------------------------------------------------

/// The headline property: one kick, one batch, everything in it.
///
/// This is the determinism a serving loop is after. A forward pass sends one
/// token on each of M streams to a peer and then flushes; what should reach the
/// wire is exactly one `_stream_batch` carrying all M, not M messages and not
/// some prefix of them. Under the default policy the count depends on how the
/// runtime happened to schedule the batcher against the producer; under
/// `Manual` it is a property of the code.
#[tokio::test(flavor = "multi_thread")]
async fn one_kick_writes_one_batch_carrying_every_staged_record() {
    const SLOTS: u64 = 16;
    let harness = harness(manual()).await;

    let mut inlets = Vec::new();
    for id in 0..SLOTS {
        let (inlet, slot) = harness.open_credited(id, id, CREDIT).await;
        inlets.push((inlet, slot));
    }

    // One "forward pass": one record per slot, back to back.
    for (index, (inlet, _)) in inlets.iter().enumerate() {
        inlet.send(item(index as u32)).expect("stage a record");
    }
    harness.await_staged(SLOTS as usize).await;
    assert_nothing_written(&harness).await;

    harness.flush_batch();

    let batch = harness.next_batch().await;
    assert_eq!(
        batch.records.len(),
        SLOTS as usize,
        "the pass must arrive as one batch, not several"
    );
    assert_eq!(
        batch.slots().len(),
        SLOTS as usize,
        "and must carry every slot that was staged"
    );
    for record in &batch.records {
        assert_eq!(record.kind, RecordType::Data);
    }
    assert_eq!(
        harness.staged(),
        0.0,
        "and the write leaves nothing behind it"
    );
    assert!(
        harness.try_next_batch().is_none(),
        "one kick is one batch: nothing follows it"
    );
}

/// A record queued *after* the loop drained and *before* it saw the kick still
/// goes out in that kick's batch.
///
/// The property every other test here misses, because they all stage their
/// records before kicking. What carries it is that the drain is a *loop*: a
/// kick is coalesced control, so observing it costs one `drain_once`, and the
/// loop then keeps going and polls the slot streams. Reaching that ordering
/// from outside is impossible — it needs work queued while the loop is
/// mid-wake — so the batcher offers a barrier:
///
/// 1. record A arrives; the loop wakes and stages it;
/// 2. **barrier** — the loop has not started draining;
/// 3. record B is queued and `flush_batch()` kicks;
/// 4. released, the loop drains: the kick first, then B, then writes both.
///
/// A caller that did this had `send` return before it called `flush_batch`, so
/// B is a record it sent and expects that flush to carry. Collapsing the drain
/// loop to a single pass takes the kick and leaves B behind — which is the
/// mutation this test is checked against.
#[tokio::test(flavor = "multi_thread")]
async fn a_record_queued_between_the_drain_and_the_kick_still_makes_that_batch() {
    let hooks = Arc::new(super::super::test_hooks::TestHooks::default());
    let harness = harness_with_hooks(manual(), Some(Arc::clone(&hooks))).await;
    let (inlet, _) = harness.open_credited(1, 1, CREDIT).await;

    hooks.pause();
    inlet.send(item(0)).expect("stage record A");
    hooks.wait_until_parked().await;

    // The loop is parked before its drain runs. Both of these land before the
    // drain observes the kick, so the drain loop must carry them both out.
    inlet.send(item(1)).expect("queue record B");
    harness.flush_batch();
    hooks.release();

    let batch = harness.next_batch().await;
    assert_eq!(
        batch.records.len(),
        2,
        "the first batch after the kick must carry both records — B was queued \
         before the flush, so the flush owes it"
    );
    assert_eq!(batch.records[0].data, item(0));
    assert_eq!(batch.records[1].data, item(1));
    assert_eq!(harness.staged(), 0.0, "and nothing is left behind");
}

/// Records staged before a kick stay staged, with no timer behind them.
///
/// This is the contract `Manual` trades for that determinism, stated as a test
/// so it cannot be quietly softened into a window later: nothing rescues a
/// producer that stops flushing. `velo_streaming_mux_staged_records` is the
/// only thing that notices, which is why it exists.
#[tokio::test(flavor = "multi_thread")]
async fn manual_has_no_timer_behind_it() {
    let harness = harness(manual()).await;
    let (inlet, _) = harness.open_credited(1, 1, CREDIT).await;

    for n in 0..4u32 {
        inlet.send(item(n)).expect("stage a record");
    }
    harness.await_staged(4).await;

    // Several times any plausible window. Nothing comes, because there is none.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        harness.try_next_batch().is_none(),
        "manual means manual: a forgotten flush is not rescued by a timer"
    );
    assert_eq!(
        harness.staged(),
        4.0,
        "and the gauge is where an operator sees the producer that forgot"
    );

    harness.flush_batch();
    assert_eq!(harness.next_batch().await.records.len(), 4);
}

/// A flush with nothing staged costs a wake and produces no traffic.
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_with_nothing_staged_writes_nothing() {
    let harness = harness(manual()).await;
    // The eager `OpenSlot` is the last thing on the wire.
    let (_inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;

    for _ in 0..10 {
        harness.flush_batch();
    }
    assert_nothing_written(&harness).await;
}

// ---------------------------------------------------------------------------
// What no policy may hold back
// ---------------------------------------------------------------------------

/// A `CreditUpdate` owed to a peer goes without waiting to be flushed.
///
/// The sharpest case of the rule, and the reason it is correctness rather than
/// polish: the peer's sender is parked waiting for this window, and no
/// application on *this* side knows it owes that peer anything. If credit could
/// wait for `flush_batch`, a node with a quiet producer would starve a busy
/// one.
#[tokio::test(flavor = "multi_thread")]
async fn a_credit_reply_moves_under_manual() {
    let harness = harness(manual()).await;
    let peer_slot = SlotId::new(7, 0).expect("index fits u24");

    harness
        .handle
        .reply(&[super::super::ReplyRecord::CreditUpdate {
            slot: peer_slot,
            delta: 32,
        }]);

    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].kind, RecordType::CreditUpdate);
    assert_eq!(batch.records[0].slot, peer_slot);
    assert_eq!(batch.records[0].credit, 32);
}

/// So does a close the peer is waiting on.
#[tokio::test(flavor = "multi_thread")]
async fn a_close_reply_moves_under_manual() {
    let harness = harness(manual()).await;
    let peer_slot = SlotId::new(9, 1).expect("index fits u24");

    harness
        .handle
        .reply(&[super::super::ReplyRecord::CloseSlot {
            slot: peer_slot,
            reason: CloseReason::UnknownSlot,
        }]);

    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].kind, RecordType::CloseSlot);
    assert_eq!(batch.records[0].slot, peer_slot);
}

/// A terminal ends a stream, so it does not wait for a flush the finalizing
/// producer may never make — and it still leaves with its `CloseSlot`.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_and_its_close_move_under_manual() {
    let harness = harness(manual()).await;
    let (inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    inlet.send(item(0)).expect("stage a record");
    inlet
        .send(cached_finalized().clone())
        .expect("stage the terminal");

    let batch = harness.next_batch().await;
    assert_eq!(
        batch.records.len(),
        3,
        "the record staged ahead of it rides along: it was owed to the consumer first"
    );
    assert_eq!(batch.records[0].kind, RecordType::Data);
    assert_eq!(batch.records[1].kind, RecordType::Data);
    assert_eq!(
        batch.records[2].kind,
        RecordType::CloseSlot,
        "terminal-then-close is atomic in one batch"
    );
    assert_eq!(batch.records[2].slot, slot);
}

/// A batch at its byte cap goes out unasked, because holding a full batch buys
/// nothing — there is no room left to batch into.
///
/// The cap is dropped to something a test can fill without generating 60 KiB.
/// It is the clamp a real batch meets, so it is the one worth pinning: the
/// record count would need 65 535 records to bind.
#[tokio::test(flavor = "multi_thread")]
async fn a_byte_clamp_still_splits_a_batch_under_manual() {
    const CAP: usize = 1024;
    let harness = harness(MuxConfig {
        max_batch_bytes: CAP,
        ..manual()
    })
    .await;
    let (inlet, _) = harness.open_credited(1, 1, CREDIT).await;

    // Comfortably more than one capped batch holds.
    for n in 0..256u32 {
        inlet.send(item(n)).expect("stage a record");
    }

    let batch = harness.next_batch().await;
    assert!(
        batch.encoded_len <= CAP,
        "the clamp cut the batch without anyone asking it to: {} bytes over a {CAP} cap",
        batch.encoded_len
    );
    assert!(
        !batch.records.is_empty() && batch.records.len() < 256,
        "a cut, not the whole burst and not nothing: {} records",
        batch.records.len()
    );

    // Whatever the clamps did not force out is still staged, still waiting on
    // the application — the kick is what finishes the burst.
    let staged_before = harness.staged();
    assert!(staged_before > 0.0, "the tail of the burst is still staged");
    harness.flush_batch();
    let mut seen = batch.records.len();
    while seen < 256 {
        seen += harness.next_batch().await.records.len();
    }
    assert_eq!(
        seen, 256,
        "every record arrives, across however many batches"
    );
    assert_eq!(harness.staged(), 0.0);
}

// ---------------------------------------------------------------------------
// Auto
// ---------------------------------------------------------------------------

/// A kick is valid under `Auto` too, and writes ahead of the conditions.
///
/// The policy here is the windowed one — no end-of-wake write, a window far
/// longer than the test's patience — so the only thing that can produce a batch
/// inside it is the flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_kick_beats_an_auto_window_that_has_not_elapsed() {
    let harness = harness(auto(false, Some(Duration::from_secs(30)))).await;
    let (inlet, _) = harness.open_credited(1, 1, CREDIT).await;

    for n in 0..5u32 {
        inlet.send(item(n)).expect("stage a record");
    }
    harness.await_staged(5).await;
    assert_nothing_written(&harness).await;

    harness.flush_batch();
    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 5);
}

// ---------------------------------------------------------------------------
// Auto
// ---------------------------------------------------------------------------

/// The window writes the batch on its own, with nobody flushing.
#[tokio::test(flavor = "multi_thread")]
async fn an_auto_window_writes_without_a_kick() {
    let harness = harness(auto(false, Some(Duration::from_millis(50)))).await;
    let (inlet, _) = harness.open_credited(1, 1, CREDIT).await;

    for n in 0..3u32 {
        inlet.send(item(n)).expect("stage a record");
    }

    let batch = tokio::time::timeout(RECV_TIMEOUT, async { harness.next_batch().await })
        .await
        .expect("the window must fire on its own");
    assert_eq!(batch.records.len(), 3);
    assert_eq!(harness.staged(), 0.0);
}

// ---------------------------------------------------------------------------
// Flushing a batcher that is parked on admission
// ---------------------------------------------------------------------------

/// Kicks arriving while the peer's gate is full neither escape it nor corrupt
/// what does.
///
/// This is the window a serving loop actually meets: the peer is congested, the
/// batcher is suspended inside `flush().await`, and the producer keeps calling
/// `flush_batch` every pass because it has no way to know that. A kick is
/// coalesced state, so a thousand of them are one bit — nothing is queued and
/// nothing is written early. What matters most is what comes out the far side
/// when the gate opens: every record exactly once, in `frame_seq` order.
#[tokio::test(flavor = "multi_thread")]
async fn kicks_during_an_admission_park_neither_double_send_nor_reorder() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use dashmap::DashMap;
    use tokio_util::sync::CancellationToken;

    use super::super::{BatcherContext, OpenSlotRequest, spawn};
    use super::support::{OwnedBatch, StallingTransport, eventually};
    use crate::messenger::Messenger;
    use crate::observability::VeloMetrics;
    use crate::streaming::messenger_mux::flow_control::SlotCredit;

    const QUEUED: u32 = 120;

    let (transport, wire) = StallingTransport::new(tokio::runtime::Handle::current());
    let sender = Messenger::builder()
        .add_transport(transport)
        .build()
        .await
        .expect("sender messenger");
    let peer_instance = velo_ext::InstanceId::new_v4();
    sender
        .register_peer(velo_ext::PeerInfo::new(
            peer_instance,
            velo_ext::WorkerAddress::from_encoded(
                rmp_serde::to_vec(&std::collections::HashMap::from([(
                    "stalling".to_string(),
                    b"stalling".to_vec(),
                )]))
                .expect("encode"),
            ),
        ))
        .expect("register peer");

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let handle = spawn(
        peer_instance.worker_id(),
        BatcherContext {
            messenger: Arc::clone(&sender),
            config: manual(),
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
            hooks: None,
        },
    );

    let (inlet, inlet_rx) = flume::bounded::<Vec<u8>>(512);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    handle
        .open_slot(OpenSlotRequest {
            anchor_id: 1,
            session_id: 1,
            inlet: inlet_rx,
            credit: SlotCredit::new(CREDIT),
            slot_byte_budget: MuxConfig::default().slot_byte_budget,
            ack: ack_tx,
        })
        .await
        .expect("queue OpenSlot");
    tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("ack")
        .expect("ack delivered")
        .expect("slot allocated");

    // The eager `OpenSlot` flush is the first write; it takes the gate's one
    // free place, so every flush after it parks.
    let open = OwnedBatch::decode(&{
        let (_, payload) = tokio::time::timeout(RECV_TIMEOUT, wire.recv_async())
            .await
            .expect("the OpenSlot flush must reach the wire")
            .expect("wire open");
        payload
    });
    assert_eq!(open.records[0].kind, RecordType::OpenSlot);

    let batches = |registry: &prometheus::Registry| {
        crate::observability::test_helpers::MetricSnapshot::from_registry(registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    };
    // The counter increments *after* a batch lands in the channel, so wait for
    // the OpenSlot flush to be accounted before building on the number.
    eventually(|| batches(&registry) == 1.0).await;

    // Stage a record and flush it: that write fills the gate and parks there.
    // Wait for BOTH facts — the gate being full and the batch being counted —
    // or the baseline below races the increment (seen under llvm-cov, whose
    // instrumentation widens the land-then-count window).
    inlet.send(item(0)).expect("stage a record");
    handle.kick_flush();
    eventually(|| wire.is_full() && batches(&registry) == 2.0).await;
    let parked_at = batches(&registry);

    // The producer keeps going: more of the stream, and a flush per pass, all
    // while the batcher is suspended.
    for n in 1..QUEUED {
        inlet.send(item(n)).expect("stage a record");
        handle.kick_flush();
    }
    assert_eq!(
        batches(&registry),
        parked_at,
        "nothing may reach the messenger while the peer's gate is full, \
         however many times the application asks"
    );

    // Open the gate and collect everything, in the order it was written.
    let mut seen: Vec<u32> = Vec::new();
    while (seen.len() as u32) < QUEUED {
        let (_, payload) = tokio::time::timeout(RECV_TIMEOUT, wire.recv_async())
            .await
            .expect("the batcher must resume once the gate drains")
            .expect("wire open");
        for record in OwnedBatch::decode(&payload).records {
            assert_eq!(record.kind, RecordType::Data);
            seen.push(record.frame_seq);
        }
    }
    cancel.cancel();

    // `OpenSlot` took frame_seq 0, so the data records are 1..=QUEUED.
    let expected: Vec<u32> = (1..=QUEUED).collect();
    assert_eq!(
        seen, expected,
        "every record exactly once and in order: a kick during a park must not \
         re-send what the parked flush already took, nor let anything overtake it"
    );
}
