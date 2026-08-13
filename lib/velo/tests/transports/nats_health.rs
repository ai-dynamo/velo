// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS health check and max_payload enforcement tests.
//!
//! Requires a running NATS server at nats://127.0.0.1:4222 (or $NATS_URL).
//! Locally: `docker run -d -p 4222:4222 nats:latest`

#![cfg(feature = "nats-transport")]

mod common;

use std::sync::Arc;
use std::time::Duration;
use velo::transports::HealthCheckError;
use velo::transports::nats::{NatsTransport, NatsTransportBuilder};
use velo::transports::{DataStreams, MessageType, Transport, make_channels};
use velo_ext::InstanceId;

use bytes::Bytes;

/// Create a started NATS transport with a given cluster_id.
///
/// This is a local helper because `TestTransportHandle::new_nats()` is not yet
/// wired up in `common/mod.rs`. We build the transport directly.
async fn make_nats_transport(
    client: Arc<async_nats::Client>,
    cluster_id: &str,
) -> anyhow::Result<(NatsTransport, DataStreams, InstanceId)> {
    let transport = NatsTransportBuilder::new(client, cluster_id).build();
    let instance_id = InstanceId::new_v4();
    let (adapter, streams) = make_channels();
    let rt = tokio::runtime::Handle::current();
    transport.start(instance_id, adapter, rt).await?;
    // Give subscriptions a moment to become live
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok((transport, streams, instance_id))
}

/// What one request on a health subject met, which is what `check_health`
/// reports on: the three states are distinguishable and each maps to one
/// verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Health {
    /// A responder replied → `Ok`.
    Answered,
    /// No interest at all, so the server bounced the request with
    /// `NoResponders` → `ConnectionFailed`.
    Bounced,
    /// Interest, but no reply within the probe's own deadline → `Timeout`.
    Absorbed,
}

/// Ask the subject once, from `client`'s connection.
///
/// `PROBE_TIMEOUT` is what separates `Absorbed` from `Answered`; it only has to
/// exceed a loopback round trip, which it does by two orders of magnitude.
async fn probe(client: &async_nats::Client, subject: &str) -> Health {
    const PROBE_TIMEOUT: Duration = Duration::from_millis(100);

    match tokio::time::timeout(
        PROBE_TIMEOUT,
        client.request(subject.to_string(), Bytes::new()),
    )
    .await
    {
        Err(_elapsed) => Health::Absorbed,
        Ok(Ok(_reply)) => Health::Answered,
        Ok(Err(_no_responders)) => Health::Bounced,
    }
}

/// Block until `subject` is in `want`, or fail saying what it was instead.
///
/// The probe runs on the *requester's own connection*, so what it observes is
/// what `check_health` will observe rather than a proxy for it. That is the
/// whole point: a subscription's arrival and a shutdown's unsubscribe are both
/// asynchronous, and a sleep long enough to usually cover them is evidence of
/// nothing. The bound is wall-clock rather than a number of attempts, because
/// both transient answers come back immediately and a count of them would
/// expire in microseconds.
async fn wait_for(client: &async_nats::Client, subject: &str, want: Health) {
    const PATIENCE: Duration = Duration::from_secs(5);

    // Both states worth waiting out — a responder still winding down, an
    // absorber whose SUB has not landed — resolve in milliseconds, and both
    // report back immediately, so probing without a pause would spend the
    // whole budget hammering the server rather than waiting for it.
    const BETWEEN_PROBES: Duration = Duration::from_millis(20);

    let deadline = tokio::time::Instant::now() + PATIENCE;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        last = Some(probe(client, subject).await);
        if last == Some(want) {
            return;
        }
        tokio::time::sleep(BETWEEN_PROBES).await;
    }

    panic!(
        "{subject} was {last:?} rather than {want:?} for {PATIENCE:?} — \
         check_health cannot be reaching its verdict for the reason this test \
         claims"
    );
}

/// TEST-04: Healthy peer — check_health returns Ok when peer is alive.
#[tokio::test]
async fn test_check_health_healthy_peer() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let client_b = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());

    let (transport_a, _streams_a, _id_a) =
        make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, _streams_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo::PeerInfo;
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

    let client_a = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let client_b = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());

    let (transport_a, _streams_a, _id_a) = make_nats_transport(client_a.clone(), &cluster_id)
        .await
        .unwrap();
    let (transport_b, _streams_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b).unwrap();

    let inbound_bytes = transport_b.address().get_entry("nats").unwrap().unwrap();
    let health_subject = format!(
        "{}.health",
        String::from_utf8(inbound_bytes.to_vec()).unwrap()
    );

    // Shutdown B so its health subscriber goes away, and wait for the subject
    // to actually lose its interest rather than for a duration that usually
    // covers it — this is the state the assertion below is about.
    transport_b.shutdown();
    wait_for(&client_a, &health_subject, Health::Bounced).await;

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
///
/// The absorber subscribes *before* B shuts down, which is load-bearing rather
/// than tidy: the subject must never be left with zero interest, or the server
/// answers the next request with `NoResponders` and `check_health` reports
/// `ConnectionFailed` — the verdict of the test one function up, reached for
/// the wrong reason.
#[tokio::test]
async fn test_check_health_timeout() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let client_b = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());

    let (transport_a, _streams_a, _id_a) = make_nats_transport(client_a.clone(), &cluster_id)
        .await
        .unwrap();
    let (transport_b, _streams_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer (so A knows B's inbound subject)
    use velo::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b.clone()).unwrap();

    // Extract B's inbound subject from B's address: the "nats" entry is the inbound subject bytes
    let inbound_bytes = transport_b.address().get_entry("nats").unwrap().unwrap();
    let inbound_subject = String::from_utf8(inbound_bytes.to_vec()).unwrap();
    let health_subject = format!("{}.health", inbound_subject);

    // Absorber first: it holds interest in the subject across B's shutdown, so
    // there is no window in which the subject is unsubscribed.
    let raw_client = async_nats::connect(&common::nats_url()).await.unwrap();
    let _absorber = raw_client.subscribe(health_subject.clone()).await.unwrap();
    raw_client.flush().await.unwrap();

    // Now B can go: the absorber is what the subject has left.
    transport_b.shutdown();
    wait_for(&client_a, &health_subject, Health::Absorbed).await;

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

    let client_a = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let client_b = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());

    let error_handler = Arc::new(common::TestErrorHandler::new());

    let (transport_a, _streams_a, _id_a) =
        make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, _streams_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    // A registers B as a peer
    use velo::PeerInfo;
    let peer_b = PeerInfo::new(id_b, transport_b.address());
    transport_a.register(peer_b).unwrap();

    // Send an oversized payload: 1MB payload + 64 bytes overhead exceeds 1MB max_payload
    let oversized = vec![0u8; 1_048_576]; // 1MB payload + 64 overhead > 1MB limit
    assert!(
        transport_a
            .send_message(
                id_b,
                Bytes::from(b"test-header".to_vec()),
                Bytes::from(oversized),
                MessageType::Message,
                error_handler.clone(),
            )
            .is_admitted(),
        "the oversized path reports via on_error and leaves nothing to wait on"
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

/// The negotiated capacity report: `max_message_size` is the connection's own
/// `max_payload`, less the frame overhead this transport charges against it.
///
/// Every expected number here is derived from the client's live
/// `max_payload()` rather than written as a literal, because `max_payload`
/// belongs to the server, not to us — a NATS started with a different
/// `--max_payload` must move the report with it. This is the one transport
/// whose capacity is genuinely negotiated.
///
/// What proves the report is that server's number rather than arithmetic on a
/// constant is the boundary, pinned from both sides: one byte past the reported
/// capacity is rejected before the wire, and exactly the reported capacity is
/// carried end to end. That is also the report and the pre-wire check agreeing,
/// which they cannot stop doing now that both read one accessor.
///
/// A transport that has never been started reports the same number as one that
/// has, and that is the part that pins the staleness: the capacity is read from
/// the connection at every use, so there is no `start()`-time snapshot left to
/// go stale across a reconnect. This assertion used to be `None` — exactly what
/// a snapshot that had not been taken yet looked like.
#[tokio::test]
async fn test_nats_max_message_size_reflects_negotiated_max_payload() {
    let cluster_id = format!("test-{}", InstanceId::new_v4());

    let client_a = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let client_b = Arc::new(async_nats::connect(&common::nats_url()).await.unwrap());
    let server_max = client_a.max_payload();
    assert_eq!(
        server_max,
        client_a.server_info().max_payload,
        "the client's live max_payload is the server's negotiated one"
    );

    let unstarted = NatsTransportBuilder::new(client_a.clone(), &cluster_id).build();
    let unstarted_capacity = unstarted
        .max_message_size(InstanceId::new_v4())
        .expect("capacity comes from the connection, which is already up");
    assert!(
        unstarted_capacity < server_max,
        "the transport discounts its own framing from the connection's limit"
    );

    let error_handler = Arc::new(common::TestErrorHandler::new());
    let (transport_a, _streams_a, _id_a) =
        make_nats_transport(client_a, &cluster_id).await.unwrap();
    let (transport_b, streams_b, id_b) = make_nats_transport(client_b, &cluster_id).await.unwrap();

    use velo::PeerInfo;
    transport_a
        .register(PeerInfo::new(id_b, transport_b.address()))
        .unwrap();

    let capacity = transport_a
        .max_message_size(id_b)
        .expect("a started NATS transport has been told its max_payload");
    assert_eq!(
        capacity, unstarted_capacity,
        "starting the transport must not be what establishes the capacity"
    );

    // One byte past the report is rejected by the send gate, pre-wire.
    let header = Bytes::from_static(b"h");
    let oversized = vec![0u8; capacity + 1 - header.len()];
    transport_a.send_message(
        id_b,
        header.clone(),
        Bytes::from(oversized),
        MessageType::Message,
        error_handler.clone(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        error_handler.error_count(),
        1,
        "capacity + 1 must be rejected: {:?}",
        error_handler.get_errors()
    );
    assert!(
        error_handler.get_errors()[0]
            .2
            .contains("exceeds NATS max_payload")
    );

    // Exactly the report is not rejected, and reaches the peer.
    error_handler.clear();
    let sized = vec![7u8; capacity - header.len()];
    transport_a.send_message(
        id_b,
        header.clone(),
        Bytes::from(sized),
        MessageType::Message,
        error_handler.clone(),
    );
    let (rx_header, rx_payload) = tokio::time::timeout(
        Duration::from_secs(10),
        streams_b.message_stream.recv_async(),
    )
    .await
    .expect("a frame of exactly the reported capacity must reach the peer")
    .expect("message stream stays open");
    assert_eq!(rx_header.len() + rx_payload.len(), capacity);
    assert_eq!(
        error_handler.error_count(),
        0,
        "a frame of exactly the reported capacity must not error: {:?}",
        error_handler.get_errors()
    );

    transport_b.shutdown();
    transport_a.shutdown();
}
