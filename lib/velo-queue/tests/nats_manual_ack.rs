// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the NATS JetStream backend under [`AckPolicy::Manual`].
//!
//! Requires a NATS server with JetStream enabled on `localhost:4222`.

#![cfg(feature = "nats")]

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use velo_queue::backends::nats::NatsQueueBackend;
use velo_queue::{AckPolicy, NextOptions, receiver, sender};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Job {
    id: u64,
}

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

async fn setup(queue_name: &str, ack_wait: Duration) -> (NatsQueueBackend, TestNatsStream) {
    let client = async_nats::connect(nats_url())
        .await
        .expect("NATS connection failed — is NATS running with JetStream?");
    let cluster_id = format!("test_{}", uuid::Uuid::new_v4().simple());
    let guard = TestNatsStream::new(&client, &cluster_id, queue_name).await;
    let backend = NatsQueueBackend::new(Arc::new(client), cluster_id).with_ack_wait(ack_wait);
    (backend, guard)
}

#[tokio::test]
async fn basic_manual_ack() {
    let (backend, _guard) = setup("basic", Duration::from_secs(5)).await;
    let tx = sender::<Job>(&backend, "basic").await.unwrap();
    let rx = receiver::<Job>(&backend, "basic", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 1 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 1);
    item.ack().await.unwrap();

    // Nothing left — recv_batch with a short timeout should return empty.
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(500));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(leftover.is_empty());
}

#[tokio::test]
async fn nack_redelivers() {
    let (backend, _guard) = setup("nack", Duration::from_secs(5)).await;
    let tx = sender::<Job>(&backend, "nack").await.unwrap();
    let rx = receiver::<Job>(&backend, "nack", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 42 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    assert_eq!(item.id, 42);
    item.nack(Duration::ZERO).await.unwrap();

    let again = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .expect("redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 42);
    again.ack().await.unwrap();
}

#[tokio::test]
async fn visibility_timeout_redelivers() {
    // Tight ack_wait so the test completes quickly.
    let (backend, _guard) = setup("vistimeout", Duration::from_secs(1)).await;
    let tx = sender::<Job>(&backend, "vistimeout").await.unwrap();
    let rx = receiver::<Job>(&backend, "vistimeout", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 7 }).await.unwrap();

    // Forget the handle so it can't ack — simulate a stuck worker.
    let item = rx.next().await.unwrap().unwrap();
    let (_job, handle) = item.into_parts();
    std::mem::forget(handle);

    // NATS should redeliver after ~1s AckWait.
    let again = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .expect("visibility-timeout redelivery timed out")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 7);
    again.ack().await.unwrap();
}

#[tokio::test]
async fn in_progress_extends_deadline() {
    // 1s AckWait; heartbeat every ~300ms for ~1.5s total to prove we're past the raw timeout.
    let (backend, _guard) = setup("progress", Duration::from_secs(1)).await;
    let tx = sender::<Job>(&backend, "progress").await.unwrap();
    let rx = receiver::<Job>(&backend, "progress", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 99 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();

    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        item.in_progress().await.unwrap();
    }

    // Total elapsed ~1.5s > 1s AckWait. If heartbeats worked, nothing was redelivered.
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(500));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(
        leftover.is_empty(),
        "item redelivered despite in_progress heartbeats"
    );

    item.ack().await.unwrap();
}

#[tokio::test]
async fn term_does_not_redeliver() {
    let (backend, _guard) = setup("term", Duration::from_secs(1)).await;
    let tx = sender::<Job>(&backend, "term").await.unwrap();
    let rx = receiver::<Job>(&backend, "term", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 555 }).await.unwrap();

    let item = rx.next().await.unwrap().unwrap();
    item.term().await.unwrap();

    // Well past AckWait — still nothing.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let opts = NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(500));
    let leftover = rx.next_with_options(opts).await.unwrap();
    assert!(leftover.is_empty(), "terminated item was redelivered");
}

#[tokio::test]
async fn drop_without_outcome_redelivers() {
    let (backend, _guard) = setup("dropnack", Duration::from_secs(5)).await;
    let tx = sender::<Job>(&backend, "dropnack").await.unwrap();
    let rx = receiver::<Job>(&backend, "dropnack", AckPolicy::Manual)
        .await
        .unwrap();

    tx.enqueue(&Job { id: 123 }).await.unwrap();

    {
        let _item = rx.next().await.unwrap().unwrap();
        // Drop without ack — the spawned nack(0) should publish Nak to NATS.
    }

    let again = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .expect("drop-nack did not trigger redelivery")
        .unwrap()
        .unwrap();
    assert_eq!(again.id, 123);
    again.ack().await.unwrap();
}

#[tokio::test]
async fn batch_partial_ack() {
    let (backend, _guard) = setup("batchack", Duration::from_secs(5)).await;
    let tx = sender::<Job>(&backend, "batchack").await.unwrap();
    let rx = receiver::<Job>(&backend, "batchack", AckPolicy::Manual)
        .await
        .unwrap();

    for i in 0u64..5 {
        tx.enqueue(&Job { id: i }).await.unwrap();
    }

    let opts = NextOptions::new()
        .batch_size(5)
        .timeout(Duration::from_secs(2));
    let mut batch = rx.next_with_options(opts).await.unwrap();
    assert_eq!(batch.len(), 5);

    // Ack the first three, nack the last two.
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
