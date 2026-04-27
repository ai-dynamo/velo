// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the messenger backend under [`AckPolicy::Manual`].

#![cfg(feature = "messenger")]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use velo_messenger::Messenger;
use velo_transports::uds::UdsTransportBuilder;

use velo_queue::backends::messenger::{MessengerQueueBackend, MessengerQueueConfig};
use velo_queue::{AckPolicy, NextOptions, receiver, sender};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Job {
    id: u64,
}

/// Set up two messenger instances connected via UDS.
async fn setup_two_messengers() -> (Arc<Messenger>, Arc<Messenger>) {
    let socket_a =
        std::env::temp_dir().join(format!("velo-queue-test-{}.sock", uuid::Uuid::new_v4()));
    let socket_b =
        std::env::temp_dir().join(format!("velo-queue-test-{}.sock", uuid::Uuid::new_v4()));

    let transport_a = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&socket_a)
            .build()
            .expect("build transport A"),
    );
    let transport_b = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&socket_b)
            .build()
            .expect("build transport B"),
    );

    let m_a = Messenger::builder()
        .add_transport(transport_a)
        .build()
        .await
        .expect("build messenger A");

    let m_b = Messenger::builder()
        .add_transport(transport_b)
        .build()
        .await
        .expect("build messenger B");

    let peer_a = m_a.peer_info();
    let peer_b = m_b.peer_info();
    m_a.register_peer(peer_b).expect("register B on A");
    m_b.register_peer(peer_a).expect("register A on B");

    tokio::time::sleep(Duration::from_millis(200)).await;

    (m_a, m_b)
}

fn local_config(visibility_timeout: Duration) -> MessengerQueueConfig {
    MessengerQueueConfig {
        capacity: None,
        visibility_timeout,
    }
}

// ============================================================================
// Local path tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn local_basic_manual_ack() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend, "local-basic").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-basic", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 1 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 1);
    item.ack().await.unwrap();

    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(200));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(leftover.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn local_nack_zero_redelivers() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend, "local-nack").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-nack", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 42 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.nack(Duration::ZERO).await.unwrap();

    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 42);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_nack_with_delay() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend, "local-delay").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-delay", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 7 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.nack(Duration::from_millis(400)).await.unwrap();

    // Should not be there yet.
    assert!(rx.try_next().unwrap().is_none());

    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("delayed redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 7);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_visibility_timeout_redelivers() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend, "local-vis").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-vis", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 99 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    let (_job, handle) = item.into_parts();
    std::mem::forget(handle);

    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("visibility-timeout redelivery did not fire")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 99);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_sweep_to_waiter_uses_real_visibility_timeout() {
    // Regression: when sweep_expired_leases re-enqueues a payload AND there's
    // a pending receiver at sweep time, deliver_to_waiter mints a new lease.
    // It must use the actor's real visibility_timeout — passing 0 would
    // create an already-expired lease and a self-perpetuating sweep loop.
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend, "local-sweep-waiter").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-sweep-waiter", AckPolicy::Manual)
        .await
        .unwrap();

    // Step 1: enqueue, recv, leak handle to simulate stuck worker.
    tx.enqueue(&Job { id: 1 }).await.unwrap();
    let item1 = rx.next().await.unwrap().unwrap();
    let (_, h1) = item1.into_parts();
    std::mem::forget(h1);

    // Step 2: spawn a recv that will block (queue empty, lease 1 still held).
    // This puts a Command::Recv on the actor's pending_receivers list.
    let rx_clone = rx.clone();
    let waiter_task =
        tokio::spawn(
            async move { tokio::time::timeout(Duration::from_secs(2), rx_clone.next()).await },
        );

    // Give the spawned recv a moment to register as a pending receiver.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 3: lease 1 expires (~300ms after step 1). Sweeper finds it, pushes
    // payload back to items, and the waiter-flush loop delivers to the
    // pending receiver via deliver_to_waiter — this mints lease 2.
    let item2 = waiter_task
        .await
        .unwrap()
        .expect("waiter timed out — sweeper didn't deliver")
        .unwrap()
        .unwrap();
    assert_eq!(item2.id, 1);

    // Step 4: hold item2 without acking, well within the visibility timeout.
    // With the fix, item2's lease deadline is ~now + 300ms, still active.
    // Without the fix, item2's lease deadline was now+0; the next sweeper
    // tick (a few ms later) re-expires it and pushes the payload back to
    // items, where try_next would find it.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(100));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(
        leftover.is_empty(),
        "lease delivered via sweep had zero timeout — re-redelivered immediately"
    );

    item2.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_drop_without_outcome_redelivers() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend, "local-drop").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-drop", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 123 }).await.unwrap();

    {
        let _item = rx.next().await.unwrap().unwrap();
    }

    let again = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .expect("drop-nack did not redeliver")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 123);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_in_progress_extends_deadline() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_millis(200)),
    );
    let tx = sender::<Job>(&backend, "local-progress").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-progress", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 2024 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();

    for _ in 0..6 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        item.in_progress().await.unwrap();
    }

    assert!(
        rx.try_next().unwrap().is_none(),
        "item redelivered despite in_progress heartbeats"
    );
    item.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn local_term_not_redelivered() {
    let (m_a, _m_b) = setup_two_messengers().await;
    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend, "local-term").await.unwrap();
    let rx = receiver::<Job>(&backend, "local-term", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 555 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.term().await.unwrap();

    // Past the visibility timeout — no redelivery.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(rx.try_next().unwrap().is_none());
}

// ============================================================================
// Remote path tests
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn remote_basic_manual_ack() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-basic", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend_a, "remote-basic").await.unwrap();

    tx.enqueue(&Job { id: 1 }).await.unwrap();

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("no item arrived")
        .unwrap()
        .unwrap();
    assert_eq!(item.id, 1);
    item.ack().await.unwrap();

    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(300));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(leftover.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_nack_redelivers() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-nack", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend_a, "remote-nack").await.unwrap();

    tx.enqueue(&Job { id: 42 }).await.unwrap();

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("no item arrived")
        .unwrap()
        .unwrap();
    item.nack(Duration::ZERO).await.unwrap();

    let again = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .expect("redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 42);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_visibility_timeout_redelivers() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-vis", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend_a, "remote-vis").await.unwrap();

    tx.enqueue(&Job { id: 99 }).await.unwrap();

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("no item arrived")
        .unwrap()
        .unwrap();
    let (_job, handle) = item.into_parts();
    std::mem::forget(handle);

    let again = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .expect("visibility-timeout redelivery did not fire")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 99);
    again.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_batch_partial_ack() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-batch", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend_a, "remote-batch").await.unwrap();

    for i in 0u64..5 {
        tx.enqueue(&Job { id: i }).await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(3));
    let mut batch = rx.next_with_options(opts).await.unwrap();
    assert_eq!(batch.len(), 5);

    let b4 = batch.pop().unwrap();
    let b3 = batch.pop().unwrap();
    for item in batch {
        item.ack().await.unwrap();
    }
    b3.nack(Duration::ZERO).await.unwrap();
    b4.nack(Duration::ZERO).await.unwrap();

    let opts2 = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(3));
    let redelivered = rx.next_with_options(opts2).await.unwrap();
    assert_eq!(redelivered.len(), 2);
    for item in redelivered {
        item.ack().await.unwrap();
    }
}

// ============================================================================
// Remote-path coverage backfill: in_progress, term, and drop-without-ack
// across two messenger instances. These exercise the Remote variant of
// MessengerAckHandle and the corresponding RPC routes.
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn remote_in_progress_extends_deadline() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Tight 300ms visibility timeout so heartbeats are doing observable work.
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-progress", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend_a, "remote-progress").await.unwrap();

    tx.enqueue(&Job { id: 2024 }).await.unwrap();

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("no item arrived")
        .unwrap()
        .unwrap();

    // Heartbeat for ~600ms (well past 300ms visibility timeout) via Remote
    // Progress RPCs. If they're not actually extending the deadline on the
    // actor, the item would be redelivered.
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(120)).await;
        item.in_progress().await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(300));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(
        leftover.is_empty(),
        "remote in_progress did not extend deadline; item was redelivered"
    );

    item.ack().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_term_does_not_redeliver() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-term", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_millis(300)),
    );
    let tx = sender::<Job>(&backend_a, "remote-term").await.unwrap();

    tx.enqueue(&Job { id: 555 }).await.unwrap();

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("no item arrived")
        .unwrap()
        .unwrap();
    item.term().await.unwrap();

    // Wait past the visibility timeout — confirm the actor did NOT redeliver.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(300));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(
        leftover.is_empty(),
        "remote term did not suppress redelivery"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_drop_without_outcome_redelivers() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let rx = receiver::<Job>(&backend_b, "remote-drop", AckPolicy::Manual)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        local_config(Duration::from_secs(30)),
    );
    let tx = sender::<Job>(&backend_a, "remote-drop").await.unwrap();

    tx.enqueue(&Job { id: 123 }).await.unwrap();

    {
        // Drop the WorkItem without ack/nack/term. AckHandle::Drop spawns a
        // Remote Nack RPC, which the actor handles as nack(0).
        let _item = tokio::time::timeout(Duration::from_secs(5), rx.next())
            .await
            .expect("no item arrived")
            .unwrap()
            .unwrap();
    }

    let again = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("drop-nack did not redeliver across the wire")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 123);
    again.ack().await.unwrap();
}
