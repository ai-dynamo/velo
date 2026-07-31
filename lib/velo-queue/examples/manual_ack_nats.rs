// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Manual-ack example for the NATS JetStream backend.
//!
//! Run with: `cargo run --example manual_ack_nats --features nats`
//!
//! Requires a NATS server with JetStream enabled on `nats://localhost:4222`
//! (override with the `NATS_URL` env var). Start one locally with:
//!
//! ```sh
//! nats-server -js
//! ```
//!
//! Each scenario uses a unique `cluster_id` so streams don't collide across
//! runs; streams are best-effort deleted on scenario exit.
//!
//! The user-facing API is identical to the in-memory example (`manual_ack`).
//! The differences live in backend construction:
//! - `NatsQueueBackend::new(client, cluster_id)` instead of `InMemoryBackend::new(cap)`.
//! - `with_ack_wait(d)` overrides the JetStream consumer's `AckWait` (visibility
//!   timeout). Default is 30s; set lower for snappy demos.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use velo_queue::backends::nats::NatsQueueBackend;
use velo_queue::{AckPolicy, receiver, sender};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PersistJob {
    block_id: u64,
}

fn nats_url() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_owned())
}

/// Scenario-scoped NATS context. Holds the backend plus enough state to
/// best-effort delete the stream on exit.
struct NatsScope {
    backend: NatsQueueBackend,
    client: async_nats::Client,
    cluster_id: String,
    queue: String,
}

impl NatsScope {
    async fn new(queue: &str, ack_wait: Duration) -> Result<Self, Box<dyn std::error::Error>> {
        let client = async_nats::connect(nats_url())
            .await
            .map_err(|e| format!("NATS connect failed: {e} — is nats-server -js running?"))?;
        let cluster_id = format!("example_{}", uuid::Uuid::new_v4().simple());
        let backend = NatsQueueBackend::new(Arc::new(client.clone()), cluster_id.clone())
            .with_ack_wait(ack_wait);
        Ok(Self {
            backend,
            client,
            cluster_id,
            queue: queue.to_owned(),
        })
    }

    async fn cleanup(self) {
        drop(self.backend); // releases the shutdown token; no-op for stream state
        let stream_name = format!("{}_{}", self.cluster_id, self.queue).replace('.', "_");
        let js = async_nats::jetstream::new(self.client);
        let _ = js.delete_stream(&stream_name).await;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("velo-queue NATS manual-ack example");
    eprintln!("connecting to {}\n", nats_url());

    scenario_basic_ack().await?;
    scenario_nack_with_delay().await?;
    scenario_term().await?;
    scenario_in_progress_heartbeat().await?;
    scenario_drop_redelivers().await?;

    eprintln!("\nAll scenarios complete.");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 1: happy path — receive, process, ack.
// ----------------------------------------------------------------------------

async fn scenario_basic_ack() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 1: basic ack");
    eprintln!("---------------------");

    let scope = NatsScope::new("jobs", Duration::from_secs(5)).await?;
    let tx = sender::<PersistJob>(&scope.backend, &scope.queue).await?;
    let rx = receiver::<PersistJob>(&scope.backend, &scope.queue, AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 42 }).await?;
    eprintln!("  enqueued block 42 to JetStream");

    let item = rx.next().await?.expect("queue closed");
    eprintln!("  received block {}", item.block_id);
    item.ack().await?;
    eprintln!("  acked (ack_with sent to NATS)");

    scope.cleanup().await;
    eprintln!("  stream deleted\n");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 2: transient failure — nack with delay maps to AckKind::Nak(Some(d)).
// ----------------------------------------------------------------------------

async fn scenario_nack_with_delay() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 2: nack with delay");
    eprintln!("---------------------------");

    let scope = NatsScope::new("jobs", Duration::from_secs(10)).await?;
    let tx = sender::<PersistJob>(&scope.backend, &scope.queue).await?;
    let rx = receiver::<PersistJob>(&scope.backend, &scope.queue, AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 7 }).await?;

    let item = rx.next().await?.unwrap();
    eprintln!(
        "  attempt 1: simulated transient failure on block {}",
        item.block_id
    );
    item.nack(Duration::from_millis(500)).await?;
    eprintln!("  nacked with 500ms delay (AckKind::Nak(Some(500ms)))");

    let item = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .map_err(|_| "redelivery timed out")??
        .unwrap();
    eprintln!("  attempt 2: success on block {}", item.block_id);
    item.ack().await?;
    eprintln!("  acked");

    scope.cleanup().await;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 3: permanent failure — term maps to AckKind::Term.
// ----------------------------------------------------------------------------

async fn scenario_term() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 3: term (no DLQ exposure on NATS yet)");
    eprintln!("----------------------------------------------");

    let scope = NatsScope::new("jobs", Duration::from_secs(2)).await?;
    let tx = sender::<PersistJob>(&scope.backend, &scope.queue).await?;
    let rx = receiver::<PersistJob>(&scope.backend, &scope.queue, AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 999 }).await?;

    let item = rx.next().await?.unwrap();
    eprintln!("  block {} unrecoverable; calling term()", item.block_id);
    item.term().await?;
    eprintln!("  termed (AckKind::Term sent to NATS — server will not redeliver)");

    // Wait past ack_wait — confirm no redelivery.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let opts = velo_queue::NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(500));
    let leftover = rx.next_with_options(opts).await?;
    assert!(leftover.is_empty(), "termed item was redelivered");
    eprintln!("  not redelivered (NATS suppressed it server-side)");
    eprintln!(
        "  note: a server-side DLQ would require stream-level config; not exposed by velo-queue today"
    );

    scope.cleanup().await;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 4: long-running work — in_progress maps to AckKind::Progress and
// extends the server's AckWait deadline.
// ----------------------------------------------------------------------------

async fn scenario_in_progress_heartbeat() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 4: in_progress heartbeat");
    eprintln!("---------------------------------");

    // 1s ack_wait. Heartbeat every 300ms for 1.5s — past the raw timeout.
    let scope = NatsScope::new("jobs", Duration::from_secs(1)).await?;
    let tx = sender::<PersistJob>(&scope.backend, &scope.queue).await?;
    let rx = receiver::<PersistJob>(&scope.backend, &scope.queue, AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 2024 }).await?;
    let item = rx.next().await?.unwrap();
    eprintln!("  received block {}; ack_wait = 1s", item.block_id);

    for tick in 1..=5 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        item.in_progress().await?;
        eprintln!("  heartbeat {tick} sent (AckKind::Progress, deadline reset)");
    }

    // After 1.5s elapsed (>1s raw ack_wait) — confirm not redelivered.
    let opts = velo_queue::NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(500));
    let leftover = rx.next_with_options(opts).await?;
    assert!(leftover.is_empty(), "redelivered despite heartbeats");
    eprintln!("  no premature redelivery despite ~1.5s elapsed");

    item.ack().await?;
    eprintln!("  acked");

    scope.cleanup().await;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 5: drop without explicit outcome — AckHandle::Drop spawns nack(0).
// ----------------------------------------------------------------------------

async fn scenario_drop_redelivers() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 5: drop without ack");
    eprintln!("----------------------------");

    let scope = NatsScope::new("jobs", Duration::from_secs(10)).await?;
    let tx = sender::<PersistJob>(&scope.backend, &scope.queue).await?;
    let rx = receiver::<PersistJob>(&scope.backend, &scope.queue, AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 555 }).await?;

    {
        let _item = rx.next().await?.unwrap();
        eprintln!("  received block 555, dropping without explicit outcome");
        // Drop happens here. AckHandle::Drop spawns a nack(0) on the runtime.
    }

    let item = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .map_err(|_| "redelivery timed out")??
        .unwrap();
    eprintln!("  block {} redelivered via spawned nack", item.block_id);
    item.ack().await?;
    eprintln!("  acked");

    scope.cleanup().await;
    eprintln!();
    Ok(())
}
