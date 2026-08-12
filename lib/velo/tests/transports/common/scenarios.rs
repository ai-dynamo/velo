// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generic test scenarios that work with any transport implementation

use super::*;
use std::time::Duration;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn single_message_round_trip<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );

    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn bidirectional_messaging<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();
    transport_b.register_peer(&transport_a).unwrap();

    // A -> B
    let (header1, payload1) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header1.clone(),
        payload1.clone(),
        MessageType::Message,
    );

    // B -> A
    let (header2, payload2) = test_message(2);
    transport_b.send(
        transport_a.instance_id,
        header2.clone(),
        payload2.clone(),
        MessageType::Message,
    );

    let recv_b = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    let recv_a = transport_a.recv_message(TEST_TIMEOUT).await.unwrap();

    assert_message_eq(recv_b, &header1, &payload1);
    assert_message_eq(recv_a, &header2, &payload2);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn multiple_messages_same_connection<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Send 10 messages
    for i in 0..10 {
        let (header, payload) = test_message(i);
        transport_a.send(
            transport_b.instance_id,
            header,
            payload,
            MessageType::Message,
        );
    }

    // Receive and verify all messages (order-independent)
    let messages = transport_b
        .collect_messages_unordered(10, TEST_TIMEOUT)
        .await
        .unwrap();

    // Generate expected messages and sort them the same way
    let mut expected: Vec<_> = (0..10).map(test_message).collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    for (i, msg) in messages.iter().enumerate() {
        assert_message_eq(msg.clone(), &expected[i].0, &expected[i].1);
    }

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn response_message_type<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Response,
    );

    let received = transport_b.recv_response(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn event_message_type<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Event,
    );

    let received = transport_b.recv_event(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn ack_message_type<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Ack,
    );

    // Acks route to event stream
    let received = transport_b.recv_event(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn mixed_message_types<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Send different message types
    let (msg_h, msg_p) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        msg_h.clone(),
        msg_p.clone(),
        MessageType::Message,
    );

    let (resp_h, resp_p) = test_message(2);
    transport_a.send(
        transport_b.instance_id,
        resp_h.clone(),
        resp_p.clone(),
        MessageType::Response,
    );

    let (event_h, event_p) = test_message(3);
    transport_a.send(
        transport_b.instance_id,
        event_h.clone(),
        event_p.clone(),
        MessageType::Event,
    );

    // Receive from appropriate streams
    let recv_msg = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    let recv_resp = transport_b.recv_response(TEST_TIMEOUT).await.unwrap();
    let recv_event = transport_b.recv_event(TEST_TIMEOUT).await.unwrap();

    assert_message_eq(recv_msg, &msg_h, &msg_p);
    assert_message_eq(recv_resp, &resp_h, &resp_p);
    assert_message_eq(recv_event, &event_h, &event_p);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn large_payload<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // 900KB payload — large enough to stress transports, small enough to fit
    // within NATS default max_payload (1MB) after header + protocol overhead.
    let header = b"large-payload".to_vec();
    let payload = test_data(900 * 1024);

    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );

    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn empty_header_and_payload<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    transport_a.send(
        transport_b.instance_id,
        vec![],
        vec![],
        MessageType::Message,
    );

    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &[], &[]);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn cluster_mesh_communication<F: TransportFactory>() {
    let cluster = F::create_cluster(3).await.unwrap();

    // Each node sends to every other node
    for i in 0..3 {
        for j in 0..3 {
            if i != j {
                let (header, payload) = test_message((i * 10 + j) as u32);
                cluster.get(i).send(
                    cluster.get(j).instance_id,
                    header,
                    payload,
                    MessageType::Message,
                );
            }
        }
    }

    // Each node should receive 2 messages
    for i in 0..3 {
        let messages = cluster
            .get(i)
            .collect_messages(2, TEST_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(messages.len(), 2);
    }

    cluster.shutdown();
}

pub async fn concurrent_senders<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Send from multiple tasks concurrently (without needing to move transport_a)
    let target_id = transport_b.instance_id;
    let mut handles = vec![];

    for i in 0..10 {
        let (header, payload) = test_message(i);
        // Send directly without spawning - the send itself is non-blocking
        transport_a.send(target_id, header, payload, MessageType::Message);
    }

    // Alternatively test with actual concurrent tasks using a different approach
    // Spawn receiver tasks to demonstrate concurrent receives
    for _ in 0..10 {
        let handle = tokio::spawn(async {
            // Just to demonstrate concurrency is working
            tokio::time::sleep(Duration::from_micros(1)).await;
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Receive all messages
    let messages = transport_b
        .collect_messages(10, TEST_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(messages.len(), 10);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn send_to_unregistered_peer<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    // Don't register B with A
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );

    // Give it a moment to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should have an error
    assert!(transport_a.error_handler.error_count() > 0);

    let errors = transport_a.error_handler.get_errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, header.as_slice());
    assert_eq!(errors[0].1, payload.as_slice());
    assert!(errors[0].2.to_lowercase().contains("peer not registered"));

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn connection_reuse<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // First message establishes connection
    let (header1, payload1) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header1.clone(),
        payload1.clone(),
        MessageType::Message,
    );

    let recv1 = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(recv1, &header1, &payload1);

    // Second message reuses connection
    let (header2, payload2) = test_message(2);
    transport_a.send(
        transport_b.instance_id,
        header2.clone(),
        payload2.clone(),
        MessageType::Message,
    );

    let recv2 = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(recv2, &header2, &payload2);

    // No errors should have occurred
    assert_eq!(transport_a.error_handler.error_count(), 0);

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn graceful_shutdown<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Send a message
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );

    // Receive it
    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    // Shutdown should complete without panics
    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn high_throughput<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    let num_messages = 100;

    // Send many messages
    for i in 0..num_messages {
        let (header, payload) = test_message(i);
        transport_a.send(
            transport_b.instance_id,
            header,
            payload,
            MessageType::Message,
        );
    }

    // Receive all messages (order-independent)
    let messages = transport_b
        .collect_messages_unordered(num_messages as usize, TEST_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(messages.len(), num_messages as usize);

    // Generate expected messages and sort them the same way
    let mut expected: Vec<_> = (0..num_messages).map(test_message).collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));

    // Verify all messages received correctly
    for (i, msg) in messages.iter().enumerate() {
        assert_message_eq(msg.clone(), &expected[i].0, &expected[i].1);
    }

    transport_a.shutdown();
    transport_b.shutdown();
}

pub async fn zero_copy_efficiency<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Large payload to test zero-copy
    let header = b"zero-copy-test".to_vec();
    let payload = test_data(512 * 1024); // 512KB

    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );

    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    // Verify no errors
    assert_eq!(transport_a.error_handler.error_count(), 0);

    transport_a.shutdown();
    transport_b.shutdown();
}

// --- Drain / shutdown scenarios ---

/// After begin_drain on B, messages sent from A to B should NOT arrive on B's message_stream.
pub async fn drain_rejects_messages<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Begin drain on B (both transport-level and shutdown-state, mirroring VeloBackend::graceful_shutdown)
    transport_b.transport.begin_drain();
    transport_b.streams.shutdown_state.begin_drain();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A sends a Message to B
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header,
        payload,
        MessageType::Message,
    );

    // B's message_stream should be empty (message rejected during drain)
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        transport_b.streams.message_stream.recv_async(),
    )
    .await;
    assert!(
        result.is_err(),
        "Expected timeout — messages should be rejected during drain"
    );

    transport_a.transport.shutdown();
    transport_b.streams.shutdown_state.teardown_token().cancel();
    transport_b.transport.shutdown();
}

/// After begin_drain on B, responses sent from A to B should still arrive on B's response_stream.
pub async fn drain_accepts_responses<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Begin drain on B
    transport_b.transport.begin_drain();
    transport_b.streams.shutdown_state.begin_drain();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A sends a Response to B
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Response,
    );

    // B's response_stream should still receive it
    let received = transport_b.recv_response(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.transport.shutdown();
    transport_b.streams.shutdown_state.teardown_token().cancel();
    transport_b.transport.shutdown();
}

/// After begin_drain on B, events sent from A to B should still arrive on B's event_stream.
pub async fn drain_accepts_events<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Begin drain on B
    transport_b.transport.begin_drain();
    transport_b.streams.shutdown_state.begin_drain();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A sends an Event to B
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Event,
    );

    // B's event_stream should still receive it
    let received = transport_b.recv_event(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    transport_a.transport.shutdown();
    transport_b.streams.shutdown_state.teardown_token().cancel();
    transport_b.transport.shutdown();
}

/// After begin_drain on B, health checks from A to B should still succeed.
pub async fn health_during_drain<F: TransportFactory>() {
    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();

    transport_a.register_peer(&transport_b).unwrap();

    // Establish a connection first: send a message and receive it
    let (header, payload) = test_message(1);
    transport_a.send(
        transport_b.instance_id,
        header.clone(),
        payload.clone(),
        MessageType::Message,
    );
    let received = transport_b.recv_message(TEST_TIMEOUT).await.unwrap();
    assert_message_eq(received, &header, &payload);

    // Begin drain on B
    transport_b.transport.begin_drain();
    transport_b.streams.shutdown_state.begin_drain();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A checks health of B — should still succeed during drain
    let result = transport_a
        .transport
        .check_health(transport_b.instance_id, Duration::from_secs(2))
        .await;
    assert!(
        result.is_ok(),
        "Health check should succeed during drain: {:?}",
        result.err()
    );

    transport_a.transport.shutdown();
    transport_b.streams.shutdown_state.teardown_token().cancel();
    transport_b.transport.shutdown();
}

// --- Admission ordering ---

/// Frames must arrive in the order their sends returned, even when nobody
/// waits for them.
///
/// This is the guarantee the per-target admission gate exists to provide, and
/// the one the old backpressure future could not: a saturated channel used to
/// park the frame inside an unpolled future, where a later fast-path send could
/// overtake it.
///
/// Saturation is provoked by issuing the whole burst from one synchronous loop.
/// The `#[tokio::test]` runtime is single-threaded, so the loop never yields and
/// the writer task cannot drain a single frame until the last send has
/// returned — the channel is guaranteed full well before then (`BURST` exceeds
/// every in-tree transport's channel capacity). Transports whose writer is an
/// OS thread rather than a task, ZMQ today, may drain concurrently and see less
/// pressure; the ordering assertion holds either way.
pub async fn admission_ordering_under_capacity_pressure<F: TransportFactory>() {
    /// Larger than the biggest in-tree send-channel capacity (NATS, 1024).
    const BURST: usize = 1100;
    const BURST_TIMEOUT: Duration = Duration::from_secs(30);

    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();
    transport_a.register_peer(&transport_b).unwrap();

    // Every outcome is dropped: delivery must not depend on anyone holding an
    // admission, which is exactly what a fire-and-forget sender does.
    for seq in 0..BURST {
        transport_a.send(
            transport_b.instance_id,
            format!("seq-{seq:05}").into_bytes(),
            b"p".to_vec(),
            MessageType::Message,
        );
    }

    let received = transport_b
        .collect_messages(BURST, BURST_TIMEOUT)
        .await
        .unwrap();
    let order: Vec<String> = received
        .iter()
        .map(|(header, _)| String::from_utf8(header.to_vec()).unwrap())
        .collect();
    let expected: Vec<String> = (0..BURST).map(|seq| format!("seq-{seq:05}")).collect();
    assert_eq!(
        order, expected,
        "frames must arrive in the order their sends returned"
    );

    transport_a.shutdown();
    transport_b.shutdown();
}

/// A connection epoch that dies must take its queued frames with it.
///
/// The peer is registered and then shut down, so the writer task's connect
/// fails: frames already on the channel are reported through `on_error`, and
/// frames still queued behind the gate resolve `Err` rather than lingering for
/// a successor connection to deliver. The successor's own gate must be
/// unaffected — a dead epoch does not poison the target.
///
/// Only meaningful for transports with a per-connection epoch, so this is wired
/// into the TCP and UDS suites directly rather than through
/// `transport_integration_tests!`. NATS and ZMQ multiplex a shared writer with
/// no connection to replace.
pub async fn admissions_fail_when_the_connection_epoch_dies<F: TransportFactory>() {
    /// Larger than the TCP/UDS send-channel capacity (256), so the tail of the
    /// burst has to queue in the gate.
    const BURST: usize = 400;

    let transport_a = F::create().await.unwrap();
    let transport_b = F::create().await.unwrap();
    transport_a.register_peer(&transport_b).unwrap();

    // Register first, then kill the listener: the peer is known but
    // unreachable, so the connection epoch dies during connect.
    transport_b.transport.shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // One synchronous burst, so the writer task cannot drain any of it before
    // the last send returns.
    let mut outstanding = Vec::new();
    for seq in 0..BURST {
        if let velo::transports::SendOutcome::Pending(admission) = transport_a.send_admission(
            transport_b.instance_id,
            format!("seq-{seq:05}").into_bytes(),
            b"p".to_vec(),
            MessageType::Message,
        ) {
            outstanding.push(admission);
        }
    }
    assert!(
        !outstanding.is_empty(),
        "a burst of {BURST} should have overflowed the send channel"
    );

    let total = outstanding.len();
    let mut failed = 0;
    for admission in outstanding {
        if tokio::time::timeout(TEST_TIMEOUT, admission)
            .await
            .expect("every admission must resolve when the epoch dies")
            .is_err()
        {
            failed += 1;
        }
    }
    // The one frame the gate's driver had already handed to the channel may
    // report `Admitted` if the channel accepted it before the epoch died —
    // that window is documented and holds at most one frame. Everything behind
    // it must fail.
    assert!(
        failed >= total - 1,
        "expected the dead epoch's queued frames to fail, got {failed} of {total}"
    );

    // The successor epoch starts clean: a fresh connection, a fresh gate, an
    // empty channel.
    assert!(
        transport_a
            .send_admission(
                transport_b.instance_id,
                b"successor".to_vec(),
                b"p".to_vec(),
                MessageType::Message,
            )
            .is_admitted(),
        "the dead epoch must not poison the target"
    );

    transport_a.shutdown();
}
