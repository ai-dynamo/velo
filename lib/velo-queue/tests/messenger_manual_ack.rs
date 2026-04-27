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
