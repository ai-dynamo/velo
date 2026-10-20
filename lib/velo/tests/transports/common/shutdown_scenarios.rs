// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generic shutdown test scenarios parameterized on ShutdownTestClient

use super::*;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use velo::transports::InboundMessage;

pub async fn drain_rejects_messages<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();
    handle.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    let mut stream =
        C::connect_and_send_frame(&handle, MessageType::Message, b"req-header", b"req-payload")
            .await;
    let (msg_type, header, payload) = C::read_one_frame(&mut stream).await;
    assert_eq!(msg_type, MessageType::ShuttingDown);
    assert_eq!(&header[..], b"req-header");
    assert_eq!(payload.len(), 0);

    handle.streams.shutdown_state.teardown_token().cancel();
}

pub async fn drain_accepts_responses<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();
    handle.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    C::connect_and_send_frame(
        &handle,
        MessageType::Response,
        b"resp-header",
        b"resp-payload",
    )
    .await;

    let (header, payload) = timeout(
        Duration::from_secs(2),
        handle.streams.response_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&header[..], b"resp-header");
    assert_eq!(&payload[..], b"resp-payload");

    handle.streams.shutdown_state.teardown_token().cancel();
}

pub async fn drain_accepts_events<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();
    handle.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    C::connect_and_send_frame(&handle, MessageType::Event, b"evt-header", b"evt-payload").await;

    let (header, payload) = timeout(
        Duration::from_secs(2),
        handle.streams.event_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&header[..], b"evt-header");
    assert_eq!(&payload[..], b"evt-payload");

    handle.streams.shutdown_state.teardown_token().cancel();
}

pub async fn new_connection_during_drain<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();
    handle.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    C::connect_and_send_frame(&handle, MessageType::Response, b"new-resp", b"new-payload").await;

    let (header, payload) = timeout(
        Duration::from_secs(2),
        handle.streams.response_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&header[..], b"new-resp");
    assert_eq!(&payload[..], b"new-payload");

    handle.streams.shutdown_state.teardown_token().cancel();
}

pub async fn graceful_shutdown_lifecycle<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();

    // Verify normal operation
    C::connect_and_send_frame(&handle, MessageType::Message, b"normal-msg", b"normal-pay").await;
    let InboundMessage { header, .. } = timeout(
        Duration::from_secs(2),
        handle.streams.message_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&header[..], b"normal-msg");

    // Acquire in-flight guard
    let guard = handle.streams.shutdown_state.acquire();
    assert_eq!(handle.streams.shutdown_state.in_flight_count(), 1);

    // Phase 1: begin drain
    handle.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    // Verify rejection
    let mut stream =
        C::connect_and_send_frame(&handle, MessageType::Message, b"reject-me", b"").await;
    let (msg_type, _, _) = C::read_one_frame(&mut stream).await;
    assert_eq!(msg_type, MessageType::ShuttingDown);

    // Verify responses still flow
    C::connect_and_send_frame(&handle, MessageType::Response, b"still-ok", b"data").await;
    let (header, _) = timeout(
        Duration::from_secs(2),
        handle.streams.response_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&header[..], b"still-ok");

    // Phase 2+3: wait for drain then teardown
    let shutdown_state = handle.streams.shutdown_state.clone();
    let shutdown_handle = tokio::spawn(async move {
        shutdown_state.wait_for_drain().await;
        shutdown_state.teardown_token().cancel();
    });

    sleep(Duration::from_millis(100)).await;
    assert!(!shutdown_handle.is_finished());

    drop(guard);

    timeout(Duration::from_secs(2), shutdown_handle)
        .await
        .expect("shutdown should complete")
        .unwrap();

    assert!(
        handle
            .streams
            .shutdown_state
            .teardown_token()
            .is_cancelled()
    );
}

/// A `Message` that reached the inbound queue but has not been consumed yet is
/// counted work: `wait_for_drain` must stay parked until somebody takes it off
/// the queue and drops it.
///
/// This is the transport half of the drain invariant. Producers have always
/// gated at admission, but the in-flight guard used to be acquired by the
/// *consumer* after the dequeue, which left the queued-but-unconsumed window
/// invisible to phase 2 — `wait_for_drain` returned, teardown ran, and the
/// message was dispatched into an instance that had already declared itself
/// dead. There is no dispatcher in this harness, so `in_flight_count() == 1`
/// here means exactly "queued and counted".
pub async fn queued_message_defers_drain_completion<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();

    // Keep the client socket open: the frame is admitted from the connection
    // reader, and closing under it would race the assertions below.
    let _stream =
        C::connect_and_send_frame(&handle, MessageType::Message, b"queued-hdr", b"queued-pay")
            .await;

    // Bounded poll for arrival rather than a fixed sleep — the queue is the
    // thing under test, so it is also the arrival signal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while handle.streams.message_stream.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "frame never reached the inbound queue"
        );
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        handle.streams.shutdown_state.in_flight_count(),
        1,
        "a queued but unconsumed Message must be counted work"
    );

    handle.streams.shutdown_state.begin_drain();

    let shutdown_state = handle.streams.shutdown_state.clone();
    let drain = tokio::spawn(async move { shutdown_state.wait_for_drain().await });

    sleep(Duration::from_millis(100)).await;
    assert!(
        !drain.is_finished(),
        "wait_for_drain completed with a Message still sitting on the inbound queue"
    );

    let queued = timeout(
        Duration::from_secs(2),
        handle.streams.message_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");
    assert_eq!(&queued.header[..], b"queued-hdr");
    assert_eq!(&queued.payload[..], b"queued-pay");

    // Dropping the message drops its guard: this is what the real consumer
    // does once the handler it dispatched has finished.
    drop(queued);

    timeout(Duration::from_secs(2), drain)
        .await
        .expect("wait_for_drain must complete once the queued Message is released")
        .unwrap();
    assert_eq!(handle.streams.shutdown_state.in_flight_count(), 0);

    handle.streams.shutdown_state.teardown_token().cancel();
}

pub async fn shutdown_timeout_forces_teardown<C: ShutdownTestClient>() {
    let handle = C::new_handle().await.unwrap();
    let _guard = handle.streams.shutdown_state.acquire();

    let shutdown_state = handle.streams.shutdown_state.clone();
    let shutdown_handle = tokio::spawn(async move {
        shutdown_state.begin_drain();
        let _ =
            tokio::time::timeout(Duration::from_millis(100), shutdown_state.wait_for_drain()).await;
        shutdown_state.teardown_token().cancel();
    });

    timeout(Duration::from_secs(2), shutdown_handle)
        .await
        .expect("shutdown should complete via timeout")
        .unwrap();

    assert!(
        handle
            .streams
            .shutdown_state
            .teardown_token()
            .is_cancelled()
    );
    assert_eq!(handle.streams.shutdown_state.in_flight_count(), 1);
}

pub async fn outbound_sends_during_drain<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();

    handle_a.register_peer(&handle_b).unwrap();
    handle_b.register_peer(&handle_a).unwrap();

    handle_a.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    handle_a.send(
        handle_b.instance_id,
        b"response-hdr".to_vec(),
        b"response-pay".to_vec(),
        MessageType::Response,
    );

    let (header, payload) = timeout(
        Duration::from_secs(2),
        handle_b.streams.response_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");

    assert_eq!(&header[..], b"response-hdr");
    assert_eq!(&payload[..], b"response-pay");

    handle_a.streams.shutdown_state.teardown_token().cancel();
    handle_b.streams.shutdown_state.teardown_token().cancel();
}

/// A drain rejection must reach the *sender's* `shutdown_stream`.
///
/// The listener replies to a Message received during drain by writing a
/// ShuttingDown frame back on the same socket the Message arrived on — the
/// socket the sender's connection writer dialed. The sender must read that
/// socket and route the frame to its own shutdown_stream, or it never learns
/// the message was rejected.
pub async fn drain_rejection_reaches_sender<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();

    handle_a.register_peer(&handle_b).unwrap();

    handle_b.streams.shutdown_state.begin_drain();
    sleep(Duration::from_millis(50)).await;

    handle_a.send(
        handle_b.instance_id,
        b"drain-reject-hdr".to_vec(),
        b"drain-reject-pay".to_vec(),
        MessageType::Message,
    );

    // B's listener echoes the header back in a ShuttingDown frame; A must
    // surface it as a correlation entry on its shutdown stream.
    let (header, payload) = timeout(
        Duration::from_secs(2),
        handle_a.streams.shutdown_stream.recv_async(),
    )
    .await
    .expect("sender never saw the ShuttingDown rejection")
    .expect("recv");
    assert_eq!(&header[..], b"drain-reject-hdr");
    assert_eq!(payload.len(), 0);

    handle_a.streams.shutdown_state.teardown_token().cancel();
    handle_b.streams.shutdown_state.teardown_token().cancel();
}

pub async fn connection_writer_exits_on_teardown<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();

    handle_a.register_peer(&handle_b).unwrap();

    handle_a.send(
        handle_b.instance_id,
        b"setup".to_vec(),
        b"data".to_vec(),
        MessageType::Message,
    );

    timeout(
        Duration::from_secs(2),
        handle_b.streams.message_stream.recv_async(),
    )
    .await
    .expect("timeout")
    .expect("recv");

    handle_a.transport.shutdown();
    sleep(Duration::from_millis(200)).await;

    handle_a.error_handler.clear();
    handle_a.send(
        handle_b.instance_id,
        b"should-fail".to_vec(),
        b"data".to_vec(),
        MessageType::Message,
    );

    sleep(Duration::from_millis(100)).await;

    assert!(
        handle_a.error_handler.error_count() >= 1,
        "error handler should be invoked for post-shutdown send"
    );

    let not_delivered = timeout(
        Duration::from_millis(100),
        handle_b.streams.message_stream.recv_async(),
    )
    .await;
    assert!(
        not_delivered.is_err(),
        "post-shutdown message must not arrive at handle_b"
    );

    handle_b.streams.shutdown_state.teardown_token().cancel();
}

/// Every frame handed to the transport before teardown either reaches the peer
/// or comes back through `on_error`. Nothing may vanish between the two: a
/// frame the writer counted as written that the peer never saw is a silent
/// loss, and its caller waits out a response timeout instead of failing fast.
///
/// Large frames keep data in flight when teardown lands, which is the window
/// this is about. The coalescing writer checks cancellation only between
/// batches, so a batch it started is written in full; on TCP and UDS the
/// kernel then delivers it after the socket closes, and whatever was still
/// queued fails through `on_error`. A transport that closes its connection
/// without waiting for the peer to acknowledge what it wrote loses the tail.
///
/// Limits: the frame count equals the default channel capacity, so no frame
/// waits in the admission gate, and the test does not call `closed()`. The
/// transports' own unit tests cover both.
pub async fn every_frame_is_delivered_or_failed_across_teardown<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    const FRAMES: usize = 256;
    const PAYLOAD: usize = 64 * 1024;
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();
    handle_a.register_peer(&handle_b).unwrap();

    for i in 0..FRAMES {
        handle_a.send(
            handle_b.instance_id,
            (i as u32).to_be_bytes().to_vec(),
            vec![0u8; PAYLOAD],
            MessageType::Response,
        );
    }
    // Tear down once frames are flowing, so it lands mid-stream.
    let (first, _) = timeout(
        Duration::from_secs(5),
        handle_b.streams.response_stream.recv_async(),
    )
    .await
    .expect("the first frame arrives")
    .expect("recv");
    let mut delivered = vec![first];
    handle_a.streams.shutdown_state.teardown_token().cancel();
    handle_a.transport.shutdown();

    while let Ok(Ok((header, _))) = timeout(
        Duration::from_secs(3),
        handle_b.streams.response_stream.recv_async(),
    )
    .await
    {
        delivered.push(header);
    }
    let failed: Vec<_> = handle_a
        .error_handler
        .get_errors()
        .into_iter()
        .map(|(header, _, _)| header)
        .collect();

    // Each frame accounted for exactly once, by its header. Matching counts
    // alone would let a frame reported twice (delivered and failed, or
    // delivered twice) hide one that vanished.
    let mut seen = vec![0usize; FRAMES];
    for header in delivered.iter().chain(&failed) {
        let index = u32::from_be_bytes(header[..].try_into().expect("a 4-byte header")) as usize;
        seen[index] += 1;
    }
    let missing: Vec<_> = (0..FRAMES).filter(|&i| seen[i] == 0).collect();
    let twice: Vec<_> = (0..FRAMES).filter(|&i| seen[i] > 1).collect();
    assert!(
        missing.is_empty() && twice.is_empty(),
        "{} delivered, {} failed: frames {missing:?} vanished across teardown, frames {twice:?} were reported more than once",
        delivered.len(),
        failed.len()
    );
    handle_b.streams.shutdown_state.teardown_token().cancel();
}

/// Every frame the transport admitted before teardown reaches the peer, and
/// none fails: teardown writes what its writers already queued before it
/// closes them, and `closed()` waits for those writes.
///
/// Admission is the promise a sender acts on. The messenger mux, for one,
/// reports a stream sent once the batch that ends it is admitted, and its
/// producer may shut down right after. A writer that dropped its queue at
/// teardown broke that promise for whatever it had not yet written, which on
/// a busy connection is most of the queue.
///
/// The frames are large, so most of them still wait in the writer's channel
/// when teardown lands. They are no more than the channel holds, so every one
/// was admitted and none waits in the admission gate. TCP and UDS only: QUIC
/// still fails frames queued at teardown.
pub async fn admitted_frames_are_written_across_teardown<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    const FRAMES: usize = 256;
    const PAYLOAD: usize = 64 * 1024;
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();
    handle_a.register_peer(&handle_b).unwrap();

    for i in 0..FRAMES {
        handle_a.send(
            handle_b.instance_id,
            (i as u32).to_be_bytes().to_vec(),
            vec![0u8; PAYLOAD],
            MessageType::Response,
        );
    }
    // Tear down once frames are flowing, so it lands mid-stream.
    let (first, _) = timeout(
        Duration::from_secs(5),
        handle_b.streams.response_stream.recv_async(),
    )
    .await
    .expect("the first frame arrives")
    .expect("recv");
    let mut delivered = vec![first];
    handle_a.streams.shutdown_state.teardown_token().cancel();
    handle_a.transport.shutdown();
    timeout(Duration::from_secs(5), handle_a.transport.closed())
        .await
        .expect("closed() bounds its wait");

    while delivered.len() < FRAMES {
        match timeout(
            Duration::from_secs(3),
            handle_b.streams.response_stream.recv_async(),
        )
        .await
        {
            Ok(Ok((header, _))) => delivered.push(header),
            _ => break,
        }
    }
    let failed = handle_a.error_handler.get_errors();
    assert!(
        delivered.len() == FRAMES && failed.is_empty(),
        "{} of {FRAMES} admitted frames delivered, {} failed at teardown: {:?}",
        delivered.len(),
        failed.len(),
        failed.first().map(|(_, _, error)| error)
    );
    for (i, header) in delivered.iter().enumerate() {
        assert_eq!(
            u32::from_be_bytes(header[..].try_into().expect("a 4-byte header")) as usize,
            i,
            "frames arrive in the order they were admitted"
        );
    }
    handle_b.streams.shutdown_state.teardown_token().cancel();
}

/// A frame admitted to a connection that is still dialing reaches the peer
/// when teardown lands before the socket exists. The first send to a peer
/// creates the connection, and the writer dials; a sender may act on the
/// admission at once (the messenger mux reports a stream sent when its batch
/// is admitted, which on a new lane is before any socket). A writer that gave
/// the dial up at teardown failed the frame instead.
pub async fn a_frame_admitted_while_dialing_survives_teardown<C: ShutdownTestClient>()
where
    C::Transport: 'static,
{
    let handle_a = C::new_handle().await.unwrap();
    let handle_b = C::new_handle().await.unwrap();
    handle_a.register_peer(&handle_b).unwrap();

    // No earlier traffic: this send creates the connection.
    handle_a.send(
        handle_b.instance_id,
        b"cold".to_vec(),
        vec![0u8; 64],
        MessageType::Response,
    );
    handle_a.streams.shutdown_state.teardown_token().cancel();
    handle_a.transport.shutdown();
    timeout(Duration::from_secs(5), handle_a.transport.closed())
        .await
        .expect("closed() bounds its wait");

    let (header, _) = timeout(
        Duration::from_secs(3),
        handle_b.streams.response_stream.recv_async(),
    )
    .await
    .expect("the frame admitted while dialing arrives")
    .expect("recv");
    assert_eq!(&header[..], b"cold");
    let failed = handle_a.error_handler.get_errors();
    assert!(failed.is_empty(), "{:?}", failed.first().map(|(_, _, e)| e));
    handle_b.streams.shutdown_state.teardown_token().cancel();
}
