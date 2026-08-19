// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Manual-ack example for the messenger backend.
//!
//! Run with: `cargo run --example manual_ack_messenger --features messenger`
//!
//! On macOS the default temp directory path is too long for UDS sockets
//! (`SUN_LEN` limit, ~108 bytes). Use a short TMPDIR:
//!
//! ```sh
//! TMPDIR=/tmp cargo run --example manual_ack_messenger --features messenger
//! ```
//!
//! ## What this demonstrates
//!
//! Two `Messenger` instances connected over UDS. The actor (queue service)
//! lives on instance B. Senders on instance A enqueue work; receivers on
//! instance B pull work. ack/nack/term/in_progress flow back through the
//! actor via the `velo.queue.rpc` RPC handler.
//!
//! The user-facing API is identical to the in-memory example. The only
//! difference is backend construction: `MessengerQueueBackend::new(messenger,
//! target_instance, config)` where `target_instance` is the instance ID of the
//! messenger that should own the queue actor.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use velo_messenger::Messenger;
use velo_queue::backends::messenger::{MessengerQueueBackend, MessengerQueueConfig};
use velo_queue::{AckPolicy, receiver, sender};
use velo_transports::uds::UdsTransportBuilder;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct PersistJob {
    block_id: u64,
}

/// Spin up two messengers connected over UDS. Returns (instance_a, instance_b).
async fn setup_two_messengers()
-> Result<(Arc<Messenger>, Arc<Messenger>), Box<dyn std::error::Error>> {
    let socket_a =
        std::env::temp_dir().join(format!("velo-queue-example-{}.sock", uuid::Uuid::new_v4()));
    let socket_b =
        std::env::temp_dir().join(format!("velo-queue-example-{}.sock", uuid::Uuid::new_v4()));

    let transport_a = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&socket_a)
            .build()
            .map_err(|e| format!("transport A: {e} (try TMPDIR=/tmp on macOS)"))?,
    );
    let transport_b = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&socket_b)
            .build()
            .map_err(|e| format!("transport B: {e} (try TMPDIR=/tmp on macOS)"))?,
    );

    let m_a = Messenger::builder()
        .add_transport(transport_a)
        .build()
        .await?;
    let m_b = Messenger::builder()
        .add_transport(transport_b)
        .build()
        .await?;

    m_a.register_peer(m_b.peer_info())?;
    m_b.register_peer(m_a.peer_info())?;

    // Give the connections a moment to settle.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok((m_a, m_b))
}

/// A scenario-scoped pair of backends: A (sender side) and B (receiver/actor side).
struct MessengerScope {
    _m_a: Arc<Messenger>,
    _m_b: Arc<Messenger>,
    backend_a: MessengerQueueBackend,
    backend_b: MessengerQueueBackend,
    queue: String,
}

impl MessengerScope {
    async fn new(
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (m_a, m_b) = setup_two_messengers().await?;
        let config = MessengerQueueConfig {
            capacity: None,
            visibility_timeout,
        };
        // Backend B owns the actor (target_instance == m_b.instance_id()).
        let backend_b = MessengerQueueBackend::new(
            Arc::clone(&m_b),
            m_b.instance_id(),
            MessengerQueueConfig {
                capacity: config.capacity,
                visibility_timeout: config.visibility_timeout,
            },
        );
        // Backend A sends to B (target_instance == m_b.instance_id()).
        let backend_a = MessengerQueueBackend::new(Arc::clone(&m_a), m_b.instance_id(), config);
        Ok(Self {
            _m_a: m_a,
            _m_b: m_b,
            backend_a,
            backend_b,
            queue: queue.to_owned(),
        })
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("velo-queue messenger manual-ack example");
    eprintln!("(two messengers, UDS, cross-instance ack)\n");

    scenario_basic_ack().await?;
    scenario_nack_with_delay().await?;
    scenario_visibility_timeout().await?;
    scenario_in_progress_heartbeat().await?;
    scenario_drop_redelivers().await?;

    eprintln!("\nAll scenarios complete.");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 1: A enqueues, B receives + acks. Cross-instance happy path.
// ----------------------------------------------------------------------------

async fn scenario_basic_ack() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 1: cross-instance basic ack");
    eprintln!("------------------------------------");

    let scope = MessengerScope::new("jobs", Duration::from_secs(30)).await?;
    // Receiver on B (where the actor lives) — must be created first so the
    // `velo.queue.rpc` handler is registered before A tries to send.
    let rx = receiver::<PersistJob>(&scope.backend_b, &scope.queue, AckPolicy::Manual).await?;
    let tx = sender::<PersistJob>(&scope.backend_a, &scope.queue).await?;

    tx.enqueue(&PersistJob { block_id: 42 }).await?;
    eprintln!("  A → B: enqueued block 42 (Enqueue RPC)");

    let item = tokio::time::timeout(Duration::from_secs(5), rx.next())
        .await
        .map_err(|_| "no item arrived")??
        .expect("queue closed");
    eprintln!(
        "  B received block {} (RecvPoll RPC → Item with token)",
        item.block_id
    );
    item.ack().await?;
    eprintln!("  B acked (Ack RPC)\n");
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 2: nack with delay — actor schedules an internal Enqueue after the delay.
// ----------------------------------------------------------------------------

async fn scenario_nack_with_delay() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 2: nack with delay");
    eprintln!("---------------------------");

    let scope = MessengerScope::new("jobs", Duration::from_secs(30)).await?;
    let rx = receiver::<PersistJob>(&scope.backend_b, &scope.queue, AckPolicy::Manual).await?;
    let tx = sender::<PersistJob>(&scope.backend_a, &scope.queue).await?;

    tx.enqueue(&PersistJob { block_id: 7 }).await?;

    let item = rx.next().await?.unwrap();
    eprintln!(
        "  attempt 1: simulated transient failure on block {}",
        item.block_id
    );
    item.nack(Duration::from_millis(400)).await?;
    eprintln!("  nacked with 400ms delay (actor will requeue after sleep)");

    let item = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .map_err(|_| "redelivery timed out")??
        .unwrap();
    eprintln!("  attempt 2: success on block {}", item.block_id);
    item.ack().await?;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 3: visibility-timeout redelivery — actor's deadline sweeper fires.
// ----------------------------------------------------------------------------

async fn scenario_visibility_timeout() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 3: visibility-timeout redelivery");
    eprintln!("-----------------------------------------");

    // Tight 300ms timeout for a snappy demo.
    let scope = MessengerScope::new("jobs", Duration::from_millis(300)).await?;
    let rx = receiver::<PersistJob>(&scope.backend_b, &scope.queue, AckPolicy::Manual).await?;
    let tx = sender::<PersistJob>(&scope.backend_a, &scope.queue).await?;

    tx.enqueue(&PersistJob { block_id: 99 }).await?;

    // Receive but never ack/nack — simulate a stuck worker by leaking the handle.
    let item = rx.next().await?.unwrap();
    eprintln!(
        "  received block {}, leaking handle to simulate stuck worker",
        item.block_id
    );
    let (_payload, handle) = item.into_parts();
    std::mem::forget(handle);

    // Actor's sweeper detects expired lease, requeues the payload.
    let item = tokio::time::timeout(Duration::from_secs(2), rx.next())
        .await
        .map_err(|_| "visibility timeout did not fire")??
        .unwrap();
    eprintln!(
        "  block {} redelivered after deadline expiry",
        item.block_id
    );
    item.ack().await?;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 4: in_progress heartbeat — keeps the lease alive past raw timeout.
// ----------------------------------------------------------------------------

async fn scenario_in_progress_heartbeat() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 4: in_progress heartbeat");
    eprintln!("---------------------------------");

    let scope = MessengerScope::new("jobs", Duration::from_millis(300)).await?;
    let rx = receiver::<PersistJob>(&scope.backend_b, &scope.queue, AckPolicy::Manual).await?;
    let tx = sender::<PersistJob>(&scope.backend_a, &scope.queue).await?;

    tx.enqueue(&PersistJob { block_id: 2024 }).await?;
    let item = rx.next().await?.unwrap();
    eprintln!(
        "  received block {}; visibility timeout = 300ms",
        item.block_id
    );

    for tick in 1..=5 {
        tokio::time::sleep(Duration::from_millis(120)).await;
        item.in_progress().await?;
        eprintln!("  heartbeat {tick} sent (Progress RPC, lease deadline reset)");
    }

    // ~600ms elapsed, well past 300ms raw timeout. Item must still be ours.
    let opts = velo_queue::NextOptions::new()
        .batch_size(1)
        .timeout(Duration::from_millis(300));
    let leftover = rx.next_with_options(opts).await?;
    assert!(leftover.is_empty(), "redelivered despite heartbeats");
    eprintln!("  no premature redelivery despite ~600ms elapsed");

    item.ack().await?;
    eprintln!();
    Ok(())
}

// ----------------------------------------------------------------------------
// Scenario 5: drop without explicit outcome — AckHandle::Drop spawns a remote
// Nack RPC, which the actor handles as nack(0).
// ----------------------------------------------------------------------------

async fn scenario_drop_redelivers() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Scenario 5: drop without ack");
    eprintln!("----------------------------");

    let scope = MessengerScope::new("jobs", Duration::from_secs(30)).await?;
    let rx = receiver::<PersistJob>(&scope.backend_b, &scope.queue, AckPolicy::Manual).await?;
    let tx = sender::<PersistJob>(&scope.backend_a, &scope.queue).await?;

    tx.enqueue(&PersistJob { block_id: 555 }).await?;

    {
        let _item = rx.next().await?.unwrap();
        eprintln!("  received block 555, dropping without explicit outcome");
        // Drop fires AckHandle::Drop, which spawns a Nack RPC to the actor.
    }

    let item = tokio::time::timeout(Duration::from_secs(3), rx.next())
        .await
        .map_err(|_| "redelivery timed out")??
        .unwrap();
    eprintln!("  block {} redelivered via spawned Nack RPC", item.block_id);
    item.ack().await?;
    eprintln!();
    Ok(())
}
