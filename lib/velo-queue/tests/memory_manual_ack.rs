// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the in-memory backend under [`AckPolicy::Manual`].

use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use velo_queue::backends::memory::InMemoryBackend;
use velo_queue::{
    AckPolicy, WorkQueueBackend, WorkQueueRecvError, receiver, receiver_auto, sender,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Job {
    id: u64,
}

async fn setup(
    capacity: usize,
    visibility_timeout: Duration,
) -> (
    InMemoryBackend,
    velo_queue::WorkQueueSender<Job>,
    velo_queue::WorkQueueReceiver<Job>,
) {
    let backend = InMemoryBackend::with_visibility_timeout(capacity, visibility_timeout);
    let tx = sender::<Job>(&backend, "q").await.unwrap();
    let rx = receiver::<Job>(&backend, "q", AckPolicy::Manual)
        .await
        .unwrap();
    (backend, tx, rx)
}

#[tokio::test(flavor = "multi_thread")]
async fn basic_manual_ack() {
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    tx.enqueue(&Job { id: 1 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 1);
    item.ack().await.unwrap();

    // Queue is drained; try_next returns None.
    assert!(rx.try_next().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_zero_redelivers_immediately() {
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    tx.enqueue(&Job { id: 42 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 42);
    item.nack(Duration::ZERO).await.unwrap();

    // Redelivery should land within a reasonable window on a multi-thread runtime.
    let again = tokio::time::timeout(Duration::from_secs(1), rx.next())
        .await
        .expect("redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 42);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_with_delay_waits() {
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    tx.enqueue(&Job { id: 7 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.nack(Duration::from_millis(400)).await.unwrap();

    // Poll before the delay elapses — must not yet be available.
    assert!(rx.try_next().unwrap().is_none());

    // After the delay, the item should reappear.
    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("delayed redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 7);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn visibility_timeout_redelivers() {
    let (_backend, tx, rx) = setup(16, Duration::from_millis(300)).await;
    tx.enqueue(&Job { id: 99 }).await.unwrap();

    // Receive and hold the item without calling ack/nack — keep it alive via mem::forget
    // so Drop doesn't trigger an early nack(0). We want the visibility timeout to fire.
    let item = rx.next().await.unwrap().unwrap();
    let (_job, handle) = item.into_parts();
    std::mem::forget(handle); // Simulate a stuck worker — handle never drops.

    // Within a bit over the visibility timeout, the item must be redelivered.
    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("visibility-timeout redelivery did not fire")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 99);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_without_outcome_redelivers_via_spawn() {
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    tx.enqueue(&Job { id: 123 }).await.unwrap();

    {
        // Drop the WorkItem without an explicit ack — Drop should spawn nack(0).
        let _item = rx.next().await.unwrap().unwrap();
    }

    let again = tokio::time::timeout(Duration::from_secs(1), rx.next())
        .await
        .expect("drop-nack spawn did not fire")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 123);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn in_progress_extends_deadline() {
    // Short visibility timeout (200ms); heartbeat every 80ms for ~500ms total.
    let (_backend, tx, rx) = setup(16, Duration::from_millis(200)).await;
    tx.enqueue(&Job { id: 2024 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();

    // Heartbeat for longer than the raw timeout would allow.
    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        item.in_progress().await.unwrap();
    }

    // After 480ms of heartbeats, no redelivery should have happened — the queue is still empty.
    assert!(
        rx.try_next().unwrap().is_none(),
        "item was redelivered despite in_progress heartbeats"
    );

    item.ack().await.unwrap();
    assert!(rx.try_next().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn term_goes_to_dlq_not_redelivered() {
    let backend = InMemoryBackend::with_visibility_timeout(16, Duration::from_secs(30));
    let tx = sender::<Job>(&backend, "q").await.unwrap();
    let rx = receiver::<Job>(&backend, "q", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 555 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.term().await.unwrap();

    // Wait briefly; no redelivery should happen.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rx.try_next().unwrap().is_none());

    // DLQ should have the item.
    let dlq = backend.dlq_receiver("q").unwrap();
    let dlq_bytes = dlq.recv_async().await.unwrap();
    let dlq_job: Job = rmp_serde::from_slice(&dlq_bytes).unwrap();
    assert_eq!(dlq_job.id, 555);
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_partial_ack_redelivers_nacked() {
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    for i in 0u64..5 {
        tx.enqueue(&Job { id: i }).await.unwrap();
    }

    let opts = velo_queue::NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_millis(500));
    let mut batch = rx.next_with_options(opts).await.unwrap();
    assert_eq!(batch.len(), 5);

    // Ack first three, nack last two.
    let b4 = batch.pop().unwrap();
    let b3 = batch.pop().unwrap();
    for item in batch {
        item.ack().await.unwrap();
    }
    b3.nack(Duration::ZERO).await.unwrap();
    b4.nack(Duration::ZERO).await.unwrap();

    // Only the nacked items should come back.
    let opts2 = velo_queue::NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_millis(500));
    let redelivered = rx.next_with_options(opts2).await.unwrap();
    assert_eq!(redelivered.len(), 2);
    for item in redelivered {
        item.ack().await.unwrap();
    }
}

#[test]
fn drop_outside_runtime_does_not_panic() {
    // Build a WorkItem on a tokio runtime, then drop it on a plain thread with no runtime.
    use std::sync::mpsc;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let item_holder = rt.block_on(async {
        let backend = InMemoryBackend::with_visibility_timeout(16, Duration::from_secs(30));
        let tx = sender::<Job>(&backend, "q").await.unwrap();
        let rx = receiver::<Job>(&backend, "q", AckPolicy::Manual)
            .await
            .unwrap();
        tx.enqueue(&Job { id: 1 }).await.unwrap();
        // Keep backend alive for the duration of this test by leaking it — we only care
        // about the Drop-without-runtime behavior here.
        Box::leak(Box::new(backend));
        rx.next().await.unwrap().unwrap()
    });

    // Now drop the item outside any async context — must not panic.
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        drop(item_holder);
        done_tx.send(()).unwrap();
    })
    .join()
    .unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker thread did not complete");
}

// ============================================================================
// Coverage backfill: Auto-policy no-ops, receiver_auto helper, malformed
// payload error path, and the 24-hour nack-delay clamp.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn auto_policy_methods_return_ok_without_panicking() {
    // Under AckPolicy::Auto, WorkItem::handle is None and every method on the
    // wrapper takes the None branch. This test pins the public contract that
    // each method returns Ok(()) and does not panic. It does NOT verify that
    // no queue-side action happens — that property comes from inspecting the
    // source's `None => Ok(())` arm. To verify no-op behavior directly would
    // require either a recording backend or a queue-internal lease counter.
    let backend = InMemoryBackend::new(16);
    let tx = sender::<Job>(&backend, "q").await.unwrap();
    let rx = receiver::<Job>(&backend, "q", AckPolicy::Auto)
        .await
        .unwrap();

    // Send four items so each method gets its own to consume.
    for i in 0..4 {
        tx.enqueue(&Job { id: i }).await.unwrap();
    }

    // ack: no-op under Auto.
    let item = rx.next().await.unwrap().unwrap();
    item.ack().await.unwrap();

    // nack: clamp_delay still runs but the inner is None.
    let item = rx.next().await.unwrap().unwrap();
    item.nack(Duration::from_millis(100)).await.unwrap();

    // in_progress: borrows self, no-op for Auto. ack to consume.
    let item = rx.next().await.unwrap().unwrap();
    item.in_progress().await.unwrap();
    item.ack().await.unwrap();

    // term: no-op under Auto.
    let item = rx.next().await.unwrap().unwrap();
    item.term().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn receiver_auto_helper_works() {
    // Smoke-tests the receiver_auto<T>() free function (the Auto-policy
    // shortcut). End-to-end equivalent to receiver<T>(backend, name, Auto).
    let backend = InMemoryBackend::new(16);
    let tx = sender::<Job>(&backend, "q").await.unwrap();
    let rx = receiver_auto::<Job>(&backend, "q").await.unwrap();

    tx.enqueue(&Job { id: 99 }).await.unwrap();
    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 99);
    item.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_payload_returns_deserialize_error() {
    // Inject garbage bytes via the raw byte-level API and observe the typed
    // receiver propagate WorkQueueRecvError::Deserialization.
    let backend = InMemoryBackend::new(16);
    let raw_sender = backend.sender("q").await.unwrap();
    let rx = receiver::<Job>(&backend, "q", AckPolicy::Auto)
        .await
        .unwrap();

    // Bytes that don't form a valid MessagePack-encoded `Job`.
    raw_sender
        .send(Bytes::from_static(b"\xFF\xFF garbage"))
        .await
        .unwrap();

    let result = rx.next().await;
    assert!(
        matches!(result, Err(WorkQueueRecvError::Deserialization(_))),
        "expected Deserialization error, got {:?}",
        result
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn nack_with_huge_delay_clamps_and_does_not_block() {
    // Pass 48h to nack(). clamp_delay caps it at 24h and logs a warning.
    // The nack call itself returns promptly (the actual sleep happens in
    // the reaper task, which we abandon when the test ends).
    let (_backend, tx, rx) = setup(16, Duration::from_secs(30)).await;
    tx.enqueue(&Job { id: 1 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();

    // 48 hours — well past MAX_NACK_DELAY (24h).
    let huge = Duration::from_secs(48 * 60 * 60);
    tokio::time::timeout(Duration::from_millis(500), item.nack(huge))
        .await
        .expect("nack call took too long; clamp didn't trigger?")
        .unwrap();
}
