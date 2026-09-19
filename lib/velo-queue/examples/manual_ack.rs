// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Manual-ack example for velo-queue.
//!
//! Run with: `cargo run --example manual_ack`
//!
//! Demonstrates the canonical user-facing API:
//! - Typed `WorkQueueSender<T>` / `WorkQueueReceiver<T>` via the `sender` /
//!   `receiver` free functions.
//! - `WorkItem<T>` returned from `rx.next()`, with `ack` / `nack(delay)` /
//!   `in_progress` / `term` for explicit lifecycle.
//! - In-memory backend with the dead-letter queue exposed via
//!   `InMemoryBackend::dlq_receiver`.
//!
//! For at-least-once processing across worker crashes, prefer
//! `AckPolicy::Manual`. Under `AckPolicy::Auto`, items are acked on receipt
//! and a worker crash loses the in-flight job.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use velo_queue::backends::memory::InMemoryBackend;
use velo_queue::{AckPolicy, receiver, sender};

/// A simulated KV-block-persistence job — the kind of thing KVBM might
/// dispatch to a G4 (object-storage) worker.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct PersistJob {
    block_id: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("velo-queue manual-ack example\n");

    scenario_basic_ack().await?;
    scenario_nack_with_delay().await?;
    scenario_term_to_dlq().await?;
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

    // Default visibility timeout (30s); plenty of headroom for in-process work.
    let backend = InMemoryBackend::new(16);
    let tx = sender::<PersistJob>(&backend, "jobs").await?;
    let rx = receiver::<PersistJob>(&backend, "jobs", AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 42 }).await?;
    eprintln!("  enqueued block 42");

    // `rx.next()` returns Result<Option<WorkItem<PersistJob>>, _>.
    let item = rx.next().await?.expect("queue closed unexpectedly");
    eprintln!("  received block {} (Deref accessor)", item.block_id);

    // Pretend we did the actual work (an S3 PUT, a transfer, etc.).

    // Ack consumes self; the future must be awaited.
    item.ack().await?;
    eprintln!("  acked");

    // Queue is empty — try_next returns None (Ok-wrapped).
    assert!(rx.try_next()?.is_none());
    eprintln!("  queue confirmed empty\n");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 2: transient failure — nack with a delay, item redelivers later.
// ----------------------------------------------------------------------------

async fn scenario_nack_with_delay() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 2: nack with delay");
    eprintln!("---------------------------");

    let backend = InMemoryBackend::new(16);
    let tx = sender::<PersistJob>(&backend, "jobs").await?;
    let rx = receiver::<PersistJob>(&backend, "jobs", AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 7 }).await?;

    // First delivery: simulate a transient failure (network blip, S3 5xx, etc.).
    let item = rx.next().await?.unwrap();
    eprintln!(
        "  attempt 1: simulated transient failure on block {}",
        item.block_id
    );
    item.nack(Duration::from_millis(200)).await?;
    eprintln!("  nacked with 200ms delay");

    // Within the delay window, the item is not visible.
    assert!(rx.try_next()?.is_none());
    eprintln!("  not visible immediately (delay holding it)");

    // After the delay, the item reappears for a retry.
    let item = rx.next().await?.unwrap();
    eprintln!("  attempt 2: success on block {}", item.block_id);
    item.ack().await?;
    eprintln!("  acked\n");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 3: permanent failure — term routes to the dead-letter queue.
// ----------------------------------------------------------------------------

async fn scenario_term_to_dlq() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 3: term → DLQ");
    eprintln!("----------------------");

    let backend = InMemoryBackend::new(16);
    let tx = sender::<PersistJob>(&backend, "jobs").await?;
    let rx = receiver::<PersistJob>(&backend, "jobs", AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 999 }).await?;

    // Pretend we determined the payload is unrecoverable — corrupted block,
    // missing bucket permission, references a deleted object, etc.
    let item = rx.next().await?.unwrap();
    eprintln!("  block {} unrecoverable; calling term()", item.block_id);
    item.term().await?;

    // The live queue is empty; the item is not redelivered.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(rx.try_next()?.is_none());
    eprintln!("  not redelivered");

    // The DLQ has the payload.
    let dlq = backend
        .dlq_receiver("jobs")
        .expect("queue must exist by now");
    let payload = dlq.recv_async().await?;
    let recovered: PersistJob = rmp_serde::from_slice(&payload)?;
    eprintln!("  DLQ holds block {}\n", recovered.block_id);
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 4: long-running work — in_progress heartbeat extends the deadline.
// ----------------------------------------------------------------------------

async fn scenario_in_progress_heartbeat() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 4: in_progress heartbeat");
    eprintln!("---------------------------------");

    // Tight visibility timeout (200ms) with heartbeats every 80ms — we'll keep
    // the lease alive past what the raw timeout would allow.
    let backend = InMemoryBackend::with_visibility_timeout(16, Duration::from_millis(200));
    let tx = sender::<PersistJob>(&backend, "jobs").await?;
    let rx = receiver::<PersistJob>(&backend, "jobs", AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 2024 }).await?;
    let item = rx.next().await?.unwrap();
    eprintln!(
        "  received block {}; visibility timeout = 200ms",
        item.block_id
    );

    // Simulate a 480ms job, heartbeating every 80ms.
    for tick in 1..=6 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        item.in_progress().await?;
        eprintln!("  heartbeat {tick} sent (deadline reset)");
    }

    // No redelivery happened — the queue is still empty for someone else.
    assert!(rx.try_next()?.is_none());
    eprintln!("  no premature redelivery during ~480ms of heartbeats");

    item.ack().await?;
    eprintln!("  acked\n");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 5: forgotten outcome — drop triggers nack(0), redelivery happens.
// ----------------------------------------------------------------------------

async fn scenario_drop_redelivers() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 5: drop without ack");
    eprintln!("----------------------------");

    let backend = InMemoryBackend::new(16);
    let tx = sender::<PersistJob>(&backend, "jobs").await?;
    let rx = receiver::<PersistJob>(&backend, "jobs", AckPolicy::Manual).await?;

    tx.enqueue(&PersistJob { block_id: 555 }).await?;

    // Receive but deliberately drop without calling ack/nack/term — this
    // simulates a worker crash mid-process or a forgotten ack.
    {
        let _item = rx.next().await?.unwrap();
        eprintln!("  received block 555, dropping without explicit outcome");
        // _item drops here. AckHandle::Drop spawns a best-effort nack(0).
    }

    // The spawned nack runs on the multi-thread runtime; redelivery is fast.
    let item = tokio::time::timeout(Duration::from_secs(1), rx.next())
        .await
        .map_err(|_| "redelivery timed out")??
        .unwrap();
    eprintln!("  block {} redelivered via drop-nack", item.block_id);
    item.ack().await?;
    eprintln!("  acked\n");
    Ok(())
}
