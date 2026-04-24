// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the messenger queue backend using UDS transport.

#![cfg(feature = "messenger")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo_messenger::Messenger;
use velo_transports::uds::UdsTransportBuilder;

use velo_queue::WorkQueueBackend;
use velo_queue::backends::messenger::{MessengerQueueBackend, MessengerQueueConfig};
use velo_queue::options::{AckPolicy, NextOptions};

fn bytes(msg: Option<velo_queue::DeliveredMessage>) -> Option<Bytes> {
    msg.map(|m| m.bytes)
}

fn batch_bytes(batch: Vec<velo_queue::DeliveredMessage>) -> Vec<Bytes> {
    batch.into_iter().map(|m| m.bytes).collect()
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

    // Exchange peer info
    let peer_a = m_a.peer_info();
    let peer_b = m_b.peer_info();
    m_a.register_peer(peer_b).expect("register B on A");
    m_b.register_peer(peer_a).expect("register A on B");

    // Wait for connections to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    (m_a, m_b)
}

// ============================================================================
// Local path tests (sender + receiver on same instance)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_local_send_recv() {
    let (m_a, _m_b) = setup_two_messengers().await;

    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        MessengerQueueConfig::default(),
    );

    let sender = backend.sender("local-test").await.unwrap();
    let receiver = backend
        .receiver("local-test", AckPolicy::Auto)
        .await
        .unwrap();

    let data = Bytes::from_static(b"hello local");
    sender.send(data.clone()).await.unwrap();

    let received = receiver.recv().await.unwrap();
    assert_eq!(bytes(received), Some(data));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_local_batch() {
    let (m_a, _m_b) = setup_two_messengers().await;

    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        MessengerQueueConfig::default(),
    );

    let sender = backend.sender("local-batch").await.unwrap();
    let receiver = backend
        .receiver("local-batch", AckPolicy::Auto)
        .await
        .unwrap();

    for i in 0u8..5 {
        sender.send(Bytes::from(vec![i])).await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(3)
        .timeout(Duration::from_millis(500));
    let batch = batch_bytes(receiver.recv_batch(&opts).await.unwrap());
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0], Bytes::from(vec![0u8]));
    assert_eq!(batch[1], Bytes::from(vec![1u8]));
    assert_eq!(batch[2], Bytes::from(vec![2u8]));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_try_send_delivers() {
    let (m_a, _m_b) = setup_two_messengers().await;

    let backend = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_a.instance_id(),
        MessengerQueueConfig::default(),
    );

    let sender = backend.sender("try-send-test").await.unwrap();
    let receiver = backend
        .receiver("try-send-test", AckPolicy::Auto)
        .await
        .unwrap();

    sender.try_send(Bytes::from_static(b"try-sent")).unwrap();

    // try_send is async under the hood — give it a moment to deliver
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_secs(2));
    let batch = batch_bytes(receiver.recv_batch(&opts).await.unwrap());
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0], Bytes::from_static(b"try-sent"));
}

// ============================================================================
// Remote path tests (sender on A, receiver on B)
// ============================================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_send_recv() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Backend on B: actor lives on B
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    // Create receiver first so the actor+handlers are registered on B
    let receiver = backend_b
        .receiver("remote-test", AckPolicy::Auto)
        .await
        .unwrap();

    // Backend on A: sends to B's actor
    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("remote-test").await.unwrap();

    let data = Bytes::from_static(b"hello remote");
    sender.send(data.clone()).await.unwrap();

    // Remote recv should block until the item arrives (not return None)
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_secs(5));
    let batch = batch_bytes(receiver.recv_batch(&opts).await.unwrap());
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0], data);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_send_after_stale_handler_cache_refreshes() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Force an initial handshake before the queue service exists on B.
    let handlers = m_a.available_handlers(m_b.instance_id()).await.unwrap();
    assert!(
        !handlers.contains(&"velo.queue.rpc".to_string()),
        "queue service should not be registered before the backend is created"
    );

    // Now create the queue backend on B, which registers the fixed queue RPC handler.
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let receiver = backend_b
        .receiver("stale-cache", AckPolicy::Auto)
        .await
        .unwrap();

    // A still has stale cached handlers from the first handshake. Sending should
    // succeed because messenger refreshes on handler miss.
    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("stale-cache").await.unwrap();

    sender.send(Bytes::from_static(b"refresh")).await.unwrap();
    assert_eq!(
        bytes(receiver.recv().await.unwrap()),
        Some(Bytes::from_static(b"refresh"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_recv_waits() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Actor on B
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let receiver = backend_b
        .receiver("remote-wait", AckPolicy::Auto)
        .await
        .unwrap();

    // Sender on A → B
    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("remote-wait").await.unwrap();

    // Send item after a delay — recv should wait for it, not return None
    let sender_clone = Arc::clone(&sender);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        sender_clone
            .send(Bytes::from_static(b"delayed"))
            .await
            .unwrap();
    });

    let result = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("recv should not timeout waiting 5s for a 200ms delayed item");
    assert_eq!(bytes(result.unwrap()), Some(Bytes::from_static(b"delayed")));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_batch() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Actor on B
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let receiver = backend_b
        .receiver("remote-batch", AckPolicy::Auto)
        .await
        .unwrap();

    // Sender on A → B
    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("remote-batch").await.unwrap();

    for i in 0u8..3 {
        sender.send(Bytes::from(vec![i])).await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(3)
        .timeout(Duration::from_secs(5));
    let batch = receiver.recv_batch(&opts).await.unwrap();
    assert_eq!(batch.len(), 3);
}

/// Regression test: recv_batch must keep polling until the deadline, not give
/// up after the first empty poll window. Here the queue starts empty and items
/// arrive after the first poll cycle (250ms) but well before the batch timeout.
#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_batch_delayed_arrival() {
    let (m_a, m_b) = setup_two_messengers().await;

    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let receiver = backend_b
        .receiver("remote-delayed-batch", AckPolicy::Auto)
        .await
        .unwrap();

    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("remote-delayed-batch").await.unwrap();

    // Start recv_batch BEFORE any items exist — first polls will return empty.
    let recv_handle = tokio::spawn({
        let receiver = Arc::clone(&receiver);
        async move {
            let opts = NextOptions::new()
                .batch_size(3)
                .timeout(Duration::from_secs(5));
            receiver.recv_batch(&opts).await
        }
    });

    // Send items after 500ms — past the first poll window (250ms) but well
    // within the 5s batch timeout.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for i in 0u8..3 {
        sender.send(Bytes::from(vec![i])).await.unwrap();
    }

    let batch = recv_handle.await.unwrap().unwrap();
    assert_eq!(
        batch.len(),
        3,
        "batch should collect all 3 items despite initial empty polls"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_messenger_remote_batch_timeout_partial() {
    let (m_a, m_b) = setup_two_messengers().await;

    // Actor on B
    let backend_b = MessengerQueueBackend::new(
        Arc::clone(&m_b),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let receiver = backend_b
        .receiver("remote-partial", AckPolicy::Auto)
        .await
        .unwrap();

    // Sender on A → B
    let backend_a = MessengerQueueBackend::new(
        Arc::clone(&m_a),
        m_b.instance_id(),
        MessengerQueueConfig::default(),
    );
    let sender = backend_a.sender("remote-partial").await.unwrap();

    // Send 2 items but request batch of 5
    sender.send(Bytes::from_static(b"a")).await.unwrap();
    sender.send(Bytes::from_static(b"b")).await.unwrap();

    let opts = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(2));
    let batch = receiver.recv_batch(&opts).await.unwrap();
    // Should get 2 items before timeout
    assert_eq!(batch.len(), 2);
}
