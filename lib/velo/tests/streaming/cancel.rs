// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for upstream cancellation (Phase 11).
//!
//! TEST-CANCEL-01: Drop StreamAnchor with attached sender — token fires, send() fails.
//! TEST-CANCEL-02: StreamController::cancel() with attached sender — same effect.
//! TEST-CANCEL-03: Remote cross-worker cancel via _stream_cancel AM over TCP.
//! TEST-CANCEL-04: Drop StreamAnchor with no sender — registry cleared, attach fails.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use velo::streaming::{AnchorManager, AnchorManagerBuilder, AttachError};
use velo_ext::{TransportKey, WorkerAddress, WorkerId};

// ---------------------------------------------------------------------------
// LocalMockTransport — in-memory transport for local (non-TCP) cancel tests
// ---------------------------------------------------------------------------

struct LocalMockTransport;

impl velo::streaming::FrameTransport for LocalMockTransport {
    fn key(&self) -> TransportKey {
        TransportKey::new("mock-stream")
    }

    fn address(&self) -> WorkerAddress {
        WorkerAddress::empty()
    }

    fn bind(
        &self,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, anyhow::Result<flume::Receiver<Vec<u8>>>> {
        Box::pin(async move {
            let (_tx, rx) = flume::bounded::<Vec<u8>>(256);
            Ok(rx)
        })
    }

    fn connect(
        &self,
        _peer: WorkerId,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async move {
            let (tx, _rx) = flume::bounded::<Vec<u8>>(1);
            Ok(tx)
        })
    }
}

fn make_local_manager() -> Arc<AnchorManager> {
    let worker_id = WorkerId::from_u64(1);
    Arc::new(AnchorManager::new(worker_id, Arc::new(LocalMockTransport)))
}

// ---------------------------------------------------------------------------
// TEST-CANCEL-01: Drop StreamAnchor with attached sender — token fires, send() fails
// ---------------------------------------------------------------------------

/// Drop StreamAnchor (with an attached sender) triggers:
///   - impl Drop -> controller.cancel() -> SenderRegistry lookup -> cancel_token fires
///   - subsequent send() returns Err(SendError::ChannelClosed)
#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_01_drop_with_sender() {
    let mgr = make_local_manager();
    let anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();

    // Attach sender
    let sender = mgr
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("attach must succeed");

    // Get cancellation token before any drop
    let cancel_token = sender.cancellation_token();
    assert!(
        !cancel_token.is_cancelled(),
        "token must not be cancelled initially"
    );

    // Drop the StreamAnchor — triggers impl Drop -> controller.cancel()
    // which removes anchor from registry, fires token via SenderRegistry
    drop(anchor);

    // Give the async cancellation a moment to propagate
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Token must have fired
    assert!(
        cancel_token.is_cancelled(),
        "cancel_token must fire after StreamAnchor is dropped"
    );

    // send() must return Err(ChannelClosed)
    let result = sender.send(42u32).await;
    assert!(
        matches!(result, Err(velo::streaming::SendError::ChannelClosed)),
        "send() must return ChannelClosed after anchor dropped, got {:?}",
        result
    );
}

// ---------------------------------------------------------------------------
// TEST-CANCEL-02: StreamController::cancel() with attached sender
// ---------------------------------------------------------------------------

/// StreamController::cancel() triggers the same effect as dropping StreamAnchor.
#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_02_controller_cancel() {
    let mgr = make_local_manager();
    let anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();

    // Obtain the controller BEFORE moving the stream
    let ctrl = anchor.controller();

    // Attach sender
    let sender = mgr
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("attach must succeed");

    // Get cancel token from sender
    let cancel_token = sender.cancellation_token();
    assert!(
        !cancel_token.is_cancelled(),
        "token must not be cancelled initially"
    );

    // Explicitly cancel via the controller (anchor still alive, so Drop hasn't fired yet)
    ctrl.cancel();

    // Drop anchor after — Drop's cancel() will no-op due to AtomicBool gate
    drop(anchor);

    // Wait for async propagation
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Token must have fired
    assert!(
        cancel_token.is_cancelled(),
        "cancel_token must fire after StreamController::cancel()"
    );

    // send() must return Err
    let result = sender.send(42u32).await;
    assert!(
        matches!(result, Err(velo::streaming::SendError::ChannelClosed)),
        "send() must return ChannelClosed after controller cancel, got {:?}",
        result
    );
}

// ---------------------------------------------------------------------------
// TEST-CANCEL-04: Drop StreamAnchor with no sender — registry cleared, attach fails
// ---------------------------------------------------------------------------

/// Dropping StreamAnchor without attaching a sender:
///   - removes anchor from registry (via impl Drop -> controller.cancel())
///   - subsequent attach returns AnchorNotFound
#[tokio::test]
async fn test_cancel_04_drop_no_sender() {
    let mgr = make_local_manager();
    let anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();

    // Drop without attaching any sender
    drop(anchor);

    // Give any async operations a moment to flush
    tokio::task::yield_now().await;

    // Subsequent attach must return AnchorNotFound — proves registry was cleared
    let result = mgr.attach_stream_anchor::<u32>(handle).await;
    assert!(
        matches!(result, Err(AttachError::AnchorNotFound { .. })),
        "attach after no-sender drop must return AnchorNotFound, got {:?}",
        result
    );
}

// ---------------------------------------------------------------------------
// TCP helpers + TEST-CANCEL-03 (remote two-worker cancel)
// ---------------------------------------------------------------------------

use velo::messenger::Messenger;
use velo::streaming::control::{StreamCancelRequest, create_stream_cancel_handler};
use velo::transports::tcp::TcpTransportBuilder;

// serde_json is needed for AM payload serialization (typed_unary_async handlers use JSON)

/// Create a TcpTransport bound to an OS-assigned port.
fn new_tcp_transport() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

/// Set up two Messenger instances connected over TCP loopback.
async fn make_two_messengers() -> (Arc<Messenger>, Arc<Messenger>) {
    let t1 = new_tcp_transport();
    let t2 = new_tcp_transport();

    let m1 = Messenger::builder()
        .add_transport(t1)
        .build()
        .await
        .expect("m1");
    let m2 = Messenger::builder()
        .add_transport(t2)
        .build()
        .await
        .expect("m2");

    m2.register_peer(m1.peer_info()).expect("register m1 on m2");
    m1.register_peer(m2.peer_info()).expect("register m2 on m1");

    tokio::time::sleep(Duration::from_millis(200)).await;
    (m1, m2)
}

/// TEST-CANCEL-03: Remote cross-worker cancel via _stream_cancel AM over TCP.
///
/// Worker A (consumer side) sends a `_stream_cancel` active message to Worker B
/// (sender side). Worker B's handler fires the CancellationToken for the active
/// StreamSender identified by `sender_stream_id`.
#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_03_remote_cancel() {
    let (messenger_a, messenger_b) = make_two_messengers().await;
    let worker_id_b = messenger_b.instance_id().worker_id();

    // Worker B: create AnchorManager with sender_registry + register _stream_cancel handler
    let mock_transport_b = Arc::new(LocalMockTransport);
    let am_b = Arc::new(
        AnchorManagerBuilder::default()
            .worker_id(worker_id_b)
            .transport(mock_transport_b)
            .build()
            .expect("am_b"),
    );
    let _ = messenger_b.register_streaming_handler(create_stream_cancel_handler(Arc::clone(
        &am_b.sender_registry,
    )));

    // Worker B: create a local anchor + attach local sender (to populate SenderRegistry)
    let anchor_b = am_b.create_anchor::<u32>();
    let handle_b = anchor_b.handle();
    let sender_b = am_b
        .attach_stream_anchor::<u32>(handle_b)
        .await
        .expect("attach sender B");
    let cancel_token = sender_b.cancellation_token();

    // sender_stream_id is 1 (first attach on am_b)
    let sender_stream_id = 1u64;
    assert!(
        am_b.sender_registry.senders.contains_key(&sender_stream_id),
        "SenderEntry must be in registry before cancel"
    );

    // Worker A: send _stream_cancel AM to worker B with sender_stream_id
    // NOTE: typed_unary_async handlers deserialize with serde_json, not rmp_serde.
    let payload = serde_json::to_vec(&StreamCancelRequest { sender_stream_id })
        .expect("serialize StreamCancelRequest");
    messenger_a
        .am_send_streaming("_stream_cancel")
        .expect("am_send_streaming builder")
        .raw_payload(bytes::Bytes::from(payload))
        .worker(worker_id_b)
        .send()
        .await
        .expect("send _stream_cancel AM");

    // Wait for handler to process
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Assert: cancel_token fired on worker B
    assert!(
        cancel_token.is_cancelled(),
        "sender cancel_token must fire after _stream_cancel AM received"
    );
}

// ---------------------------------------------------------------------------
// Cancel over the mux: the two ways a dead consumer reaches its producer
// ---------------------------------------------------------------------------
//
// A producer learns its consumer is gone through one of two paths, and which
// one is available depends on how the stream was opened:
//
// - **Attach.** `_anchor_attach` carries a `StreamCancelHandle`, so the anchor
//   can name the producer's `SenderEntry` and poison it directly. This works
//   whether or not the producer is sending.
// - **Ingress.** The next record to arrive for a slot whose consumer dropped
//   its receiver faults with `CloseReason::UnknownSlot`
//   (`messenger_mux/ingress/slot.rs:292`), which closes the producer's egress
//   slot. This works only while records keep arriving.
//
// Zero-RTT setup never sends an attach, so it never learns the cancel handle
// and the first path is unavailable to it. The pair of tests below is that
// contrast: the control fixes the second path in place, and the behavioural
// test asks for an idle producer, which only the second path cannot serve.

use futures::StreamExt;
use velo::Velo;
use velo::streaming::control::StreamOpenTicket;
use velo::streaming::{MuxConfig, StreamAnchorHandle, StreamFrame};

/// Long enough to absorb a loaded machine, short enough that a hang fails the
/// test rather than the runner.
const CANCEL_BOUND: Duration = Duration::from_secs(15);

/// How long a producer stays silent before the send that must fail.
///
/// The close travels batcher flush, then TCP, then the peer's `_stream_batch`
/// handler. Loopback needs milliseconds; the margin is for a node running the
/// whole 41-binary suite at once, where no retry is available — retrying the
/// send would feed the reactive ingress-fault path and make the test
/// tautological.
const SETTLE: Duration = Duration::from_secs(5);

/// A window small enough that credit has to move, and a sweep fast enough that
/// a parked producer resumes inside `CANCEL_BOUND`.
fn cancel_mux_config() -> MuxConfig {
    MuxConfig {
        enabled: true,
        initial_credit: 8,
        credit_sweep_interval: Duration::from_millis(1),
        ..MuxConfig::default()
    }
}

async fn velo_node() -> Arc<Velo> {
    Velo::builder()
        .add_transport(new_tcp_transport())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .messenger_mux(cancel_mux_config())
        .expect("install mux")
        .build()
        .await
        .expect("build velo")
}

/// Two cross-registered nodes: `consumer` owns the anchors, `producer` sends.
async fn velo_pair() -> (Arc<Velo>, Arc<Velo>) {
    let consumer = velo_node().await;
    let producer = velo_node().await;
    consumer
        .register_peer(producer.peer_info())
        .expect("register producer on consumer");
    producer
        .register_peer(consumer.peer_info())
        .expect("register consumer on producer");
    for (node, peer) in [
        (&producer, consumer.instance_id()),
        (&consumer, producer.instance_id()),
    ] {
        tokio::time::timeout(CANCEL_BOUND, node.wait_for_handler(peer, "_anchor_attach"))
            .await
            .expect("timed out waiting for the peer's control plane")
            .expect("peer never advertised the handler");
    }
    (consumer, producer)
}

fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

/// Control. A producer that keeps sending already learns its consumer is gone,
/// on every setup path, because the ingress slot faults on the next record.
///
/// This is what makes the idle-producer test next door non-tautological: the
/// fixture, the bound and the mux config are identical, so a failure there is
/// about the producer being idle and not about any of those.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_with_a_record_in_flight_already_errors() {
    let (consumer, producer) = velo_pair().await;

    let mut anchor = consumer.create_anchor::<u32>();
    let sender = producer
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    let send = tokio::spawn(async move {
        for n in 0..u32::MAX {
            if sender.send(n).await.is_err() {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        unreachable!("the send loop must end on an error");
    });

    // One item proves the stream is live before the anchor dies.
    let first = tokio::time::timeout(CANCEL_BOUND, anchor.next())
        .await
        .expect("timed out waiting for the first item")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(matches!(first, StreamFrame::Item(0)));

    drop(anchor);

    let sent = tokio::time::timeout(CANCEL_BOUND, send)
        .await
        .expect("the producer never learned its consumer was gone")
        .expect("send task");
    assert!(sent > 0, "the failure must come after a delivered record");
}

/// Behavioural. Under zero-RTT setup there is no attach, so the anchor never
/// learns a [`velo::streaming::control::StreamCancelHandle`] and cannot poison
/// its producer directly. The producer is idle, so the ingress fault that rides
/// on the next arriving record cannot reach it either. The close the dying
/// pre-bind posts is the only thing left, and this is the test of it.
///
/// One send, after a settle the producer spends silent: a send that succeeds
/// here is the slot still open and the producer still blind.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_reaches_an_idle_worker_within_a_bound() {
    let (consumer, producer) = velo_pair().await;

    let mut anchor = consumer.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = consumer
        .prebind_anchor(handle)
        .expect("both nodes run the mux, so a ticket is minted");
    // Through serde, because that is how a ticket reaches a worker: in the
    // request envelope the application already sends, never by reference.
    let ticket: StreamOpenTicket =
        serde_json::from_slice(&serde_json::to_vec(&ticket).expect("encode ticket"))
            .expect("decode ticket");

    let sender = producer
        .open_anchor_stream::<u32>(handle, ticket)
        .await
        .expect("zero-RTT open");
    sender.send(0).await.expect("first item");

    let first = tokio::time::timeout(CANCEL_BOUND, anchor.next())
        .await
        .expect("timed out waiting for the first item")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(matches!(first, StreamFrame::Item(0)));
    assert!(
        !sender.cancellation_token().is_cancelled(),
        "no attach happened, so nothing can have routed a cancel AM to this sender"
    );

    drop(anchor);

    tokio::time::sleep(SETTLE).await;
    assert!(
        sender.send(1).await.is_err(),
        "the first send after the consumer died must already fail"
    );
}
