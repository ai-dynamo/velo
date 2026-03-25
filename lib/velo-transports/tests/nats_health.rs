// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS health check and max_payload enforcement tests.
//!
//! Requires a running NATS server at nats://127.0.0.1:4222.
//! Locally: `docker run -d -p 4222:4222 nats:latest`

#![cfg(feature = "nats")]

mod common;

use std::sync::Arc;
use std::time::Duration;
use velo_transports::HealthCheckError;
use velo_transports::nats::{NatsTransport, NatsTransportBuilder};
use velo_transports::{InstanceId, MessageType, Transport, make_channels};

use bytes::Bytes;

/// Create a started NATS transport with a given cluster_id.
///
/// This is a local helper because `TestTransportHandle::new_nats()` is not yet
/// wired up in `common/mod.rs`. We build the transport directly.
async fn make_nats_transport(
    client: Arc<async_nats::Client>,
    cluster_id: &str,
) -> anyhow::Result<(NatsTransport, InstanceId)> {
    let transport = NatsTransportBuilder::new(client, cluster_id).build();
    let instance_id = InstanceId::new_v4();
    let (adapter, _streams) = make_channels();
    let rt = tokio::runtime::Handle::current();
    transport.start(instance_id, adapter, rt).await?;
    // Give subscriptions a moment to become live
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok((transport, instance_id))
}

/// TEST-04: Healthy peer — check_health returns Ok when peer is alive.
#[tokio::test]
async fn test_check_health_healthy_peer() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let client_b = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());

    let (transport_a, _id_a) = make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo_transports::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b).unwrap();

    // A checks health of B — B is alive, should return Ok
    let result = transport_a.check_health(id_b, Duration::from_secs(2)).await;
    assert!(
        result.is_ok(),
        "Health check to alive peer must return Ok, got: {:?}",
        result
    );

    transport_b.shutdown();
    transport_a.shutdown();
}

/// TEST-04: Unreachable peer — check_health returns ConnectionFailed when peer has shut down.
#[tokio::test]
async fn test_check_health_unreachable_peer() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let client_b = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());

    let (transport_a, _id_a) = make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo_transports::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b).unwrap();

    // Shutdown B so its health subscriber goes away
    transport_b.shutdown();
    // Wait for NATS to propagate the unsubscribe
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A checks health of B — NATS returns NoResponders, maps to ConnectionFailed
    let result = transport_a.check_health(id_b, Duration::from_secs(2)).await;
    assert!(
        matches!(result, Err(HealthCheckError::ConnectionFailed)),
        "Health check to unreachable peer must return ConnectionFailed, got: {:?}",
        result
    );

    transport_a.shutdown();
}

/// TEST-04: Timeout — check_health returns Timeout when peer absorbs request but never replies.
///
/// The trick: we subscribe to B's health subject with a raw client that receives
/// the request but never publishes on the reply subject, so the requester's
/// `client.request()` future never resolves and the tokio timeout fires.
#[tokio::test]
async fn test_check_health_timeout() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let client_b = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());

    let (transport_a, _id_a) = make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, id_b) = make_nats_transport(client_b.clone(), &cluster_id)
        .await
        .unwrap();

    // A registers B as a peer (so A knows B's inbound subject)
    use velo_transports::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b.clone()).unwrap();

    // Extract B's inbound subject from B's address: the "nats" entry is the inbound subject bytes
    let inbound_bytes = transport_b.address().get_entry("nats").unwrap().unwrap();
    let inbound_subject = String::from_utf8(inbound_bytes.to_vec()).unwrap();
    let health_subject = format!("{}.health", inbound_subject);

    // Shutdown B so its real health subscriber is gone
    transport_b.shutdown();
    // Wait for unsubscribe to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Subscribe to B's health subject with a raw absorber that never replies
    let raw_client = async_nats::connect("nats://127.0.0.1:4222").await.unwrap();
    let _absorber = raw_client.subscribe(health_subject.clone()).await.unwrap();

    // A checks health of B with a short timeout — absorber receives the request but never replies
    let result = transport_a
        .check_health(id_b, Duration::from_millis(200))
        .await;

    // IMPORTANT: keep _absorber alive until after check_health returns
    drop(_absorber);

    assert!(
        matches!(result, Err(HealthCheckError::Timeout)),
        "Health check with non-replying absorber must return Timeout, got: {:?}",
        result
    );

    transport_a.shutdown();
}

/// TEST-05: Max payload enforcement — oversized frames trigger the on_error callback.
///
/// Default NATS max_payload is 1_048_576 bytes. With 64 bytes overhead, a payload
/// of 1_048_576 bytes will exceed the limit.
#[tokio::test]
async fn test_nats_max_payload_enforcement() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());
    let client_b = Arc::new(async_nats::connect("nats://127.0.0.1:4222").await.unwrap());

    let error_handler = Arc::new(common::TestErrorHandler::new());

    let (transport_a, _id_a) = make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo_transports::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b).unwrap();

    // Send an oversized payload: 1MB payload + 64 bytes overhead exceeds 1MB max_payload
    let oversized = vec![0u8; 1_048_576]; // 1MB payload + 64 overhead > 1MB limit
    transport_a.send_message(
        id_b,
        Bytes::from(b"test-header".to_vec()),
        Bytes::from(oversized),
        MessageType::Message,
        error_handler.clone(),
    );

    // Wait for the synchronous error callback to fire (send_message is synchronous here
    // because the max_payload check happens before spawning the async task)
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert!(
        error_handler.error_count() >= 1,
        "Oversized frame must trigger at least one error callback"
    );

    let errors = error_handler.get_errors();
    let error_msg = &errors[0].2;
    assert!(
        error_msg.contains("exceeds NATS max_payload"),
        "Error message must contain 'exceeds NATS max_payload', got: {}",
        error_msg
    );

    transport_b.shutdown();
    transport_a.shutdown();
}
