// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the NATS JetStream queue backend.
//!
//! Requires a NATS server with JetStream enabled.
//! Set `NATS_URL` env var to override the default `nats://localhost:4222`.

#![cfg(feature = "nats")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use velo_queue::WorkQueueBackend;
use velo_queue::backends::nats::NatsQueueBackend;
use velo_queue::options::NextOptions;

fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_owned())
}

/// RAII guard that deletes the JetStream stream on drop.
struct TestNatsStream {
    stream_name: String,
    jetstream: async_nats::jetstream::Context,
}

impl TestNatsStream {
    async fn new(client: &async_nats::Client, cluster_id: &str, queue_name: &str) -> Self {
        let stream_name = format!("{}_{}", cluster_id, queue_name).replace('.', "_");
        let jetstream = async_nats::jetstream::new(client.clone());
        Self {
            stream_name,
            jetstream,
        }
    }
}

impl Drop for TestNatsStream {
    fn drop(&mut self) {
        // Best-effort cleanup: try to delete the stream synchronously.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let js = self.jetstream.clone();
            let name = self.stream_name.clone();
            handle.spawn(async move {
                if let Err(e) = js.delete_stream(&name).await {
                    eprintln!("test cleanup: failed to delete stream {name}: {e}");
                }
            });
        }
    }
}

async fn setup(queue_name: &str) -> (NatsQueueBackend, TestNatsStream) {
    let client = async_nats::connect(nats_url())
        .await
        .expect("NATS connection failed — is NATS running with JetStream?");
    let cluster_id = format!("test_{}", uuid::Uuid::new_v4().simple());
    let guard = TestNatsStream::new(&client, &cluster_id, queue_name).await;
    let backend = NatsQueueBackend::new(Arc::new(client), cluster_id);
    (backend, guard)
}

#[tokio::test]
async fn test_nats_basic_send_recv() {
    let (backend, _guard) = setup("basic").await;

    let sender = backend.sender("basic").await.unwrap();
    let receiver = backend.receiver("basic").await.unwrap();

    let data = Bytes::from_static(b"hello nats");
    sender.send(data.clone()).await.unwrap();

    let received = receiver.recv().await.unwrap();
    assert_eq!(received, Some(data));
}

#[tokio::test]
async fn test_nats_ordering() {
    let (backend, _guard) = setup("ordering").await;

    let sender = backend.sender("ordering").await.unwrap();
    let receiver = backend.receiver("ordering").await.unwrap();

    for i in 0u8..10 {
        sender.send(Bytes::from(vec![i])).await.unwrap();
    }

    for i in 0u8..10 {
        let received = receiver.recv().await.unwrap();
        assert_eq!(received, Some(Bytes::from(vec![i])));
    }
}

#[tokio::test]
async fn test_nats_batch_send_recv() {
    let (backend, _guard) = setup("batch").await;

    let sender = backend.sender("batch").await.unwrap();
    let receiver = backend.receiver("batch").await.unwrap();

    for i in 0u8..5 {
        sender.send(Bytes::from(vec![i])).await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(5));
    let batch = receiver.recv_batch(&opts).await.unwrap();
    assert_eq!(batch.len(), 5);
    for (idx, item) in batch.iter().enumerate() {
        assert_eq!(*item, Bytes::from(vec![idx as u8]));
    }
}

#[tokio::test]
async fn test_nats_batch_timeout_partial() {
    let (backend, _guard) = setup("partial").await;

    let sender = backend.sender("partial").await.unwrap();
    let receiver = backend.receiver("partial").await.unwrap();

    // Send 2 items but request batch of 5
    sender.send(Bytes::from_static(b"a")).await.unwrap();
    sender.send(Bytes::from_static(b"b")).await.unwrap();

    let opts = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(2));
    let batch = receiver.recv_batch(&opts).await.unwrap();
    assert_eq!(batch.len(), 2);
}

#[tokio::test]
async fn test_nats_try_send() {
    let (backend, _guard) = setup("trysend").await;

    let sender = backend.sender("trysend").await.unwrap();
    let receiver = backend.receiver("trysend").await.unwrap();

    sender.try_send(Bytes::from_static(b"try")).unwrap();

    // try_send is fire-and-forget async — give it time to deliver
    tokio::time::sleep(Duration::from_millis(500)).await;

    let received = receiver.recv().await.unwrap();
    assert_eq!(received, Some(Bytes::from_static(b"try")));
}

#[tokio::test]
async fn test_nats_multiple_queues_independent() {
    let client = async_nats::connect(nats_url())
        .await
        .expect("NATS connection failed");
    let cluster_id = format!("test_{}", uuid::Uuid::new_v4().simple());

    let _guard_a = TestNatsStream::new(&client, &cluster_id, "queue_a").await;
    let _guard_b = TestNatsStream::new(&client, &cluster_id, "queue_b").await;

    let backend = NatsQueueBackend::new(Arc::new(client), cluster_id);

    let sender_a = backend.sender("queue_a").await.unwrap();
    let sender_b = backend.sender("queue_b").await.unwrap();
    let receiver_a = backend.receiver("queue_a").await.unwrap();
    let receiver_b = backend.receiver("queue_b").await.unwrap();

    sender_a.send(Bytes::from_static(b"aa")).await.unwrap();
    sender_b.send(Bytes::from_static(b"bb")).await.unwrap();

    assert_eq!(
        receiver_a.recv().await.unwrap(),
        Some(Bytes::from_static(b"aa"))
    );
    assert_eq!(
        receiver_b.recv().await.unwrap(),
        Some(Bytes::from_static(b"bb"))
    );
}

/// Verify that closing the backend causes recv() to return Ok(None).
///
/// This exercises the ReceiverBackend contract:
/// "Returns Ok(None) when the queue is closed and drained."
#[tokio::test]
async fn test_nats_recv_returns_none_on_close() {
    let (backend, _guard) = setup("close_signal").await;
    let backend = Arc::new(backend);

    // Obtain a receiver before closing the backend.
    let receiver = backend
        .receiver("close_signal")
        .await
        .expect("receiver creation failed");

    // Spawn a task that blocks on recv().
    let recv_task = tokio::spawn(async move { receiver.recv().await });

    // Give the recv task a moment to enter the fetch loop.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Close the backend — this should unblock recv() with Ok(None).
    backend.close();

    // The task should complete promptly (well within 5 seconds).
    let result = tokio::time::timeout(Duration::from_secs(5), recv_task)
        .await
        .expect("recv() did not return within 5 seconds after close()")
        .expect("task panicked");

    assert_eq!(result.unwrap(), None, "expected Ok(None) after close()");
}
