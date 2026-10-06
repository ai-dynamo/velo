// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the TCP transport's connection lifecycle and builder.

use super::*;
use crate::transports::AdmissionState;
use crate::transports::address::WorkerAddressBuilder;
use crate::transports::tcp::TcpFrameCodec;
use std::sync::atomic::{AtomicUsize, Ordering};
use velo_ext::PeerInfo;

/// Error handler that discards errors (for tests that don't need to track them).
struct NullErrorHandler;
impl TransportErrorHandler for NullErrorHandler {
    fn on_error(&self, _: Bytes, _: Bytes, _: String) {}
}

/// Error handler that counts errors (for tests that verify error routing).
struct TrackingErrorHandler {
    count: AtomicUsize,
    reasons: Mutex<Vec<String>>,
}

impl TrackingErrorHandler {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            reasons: Mutex::new(Vec::new()),
        }
    }

    fn error_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl TransportErrorHandler for TrackingErrorHandler {
    fn on_error(&self, _: Bytes, _: Bytes, reason: String) {
        self.reasons.lock().unwrap().push(reason);
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

/// Build a `PeerInfo` whose TCP endpoint points at `addr` using legacy format.
fn make_tcp_peer(addr: SocketAddr) -> PeerInfo {
    let instance_id = crate::InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    builder
        .add_entry("tcp", format!("tcp://{}", addr).into_bytes())
        .unwrap();
    PeerInfo::new(instance_id, builder.build().unwrap())
}

/// Build a `PeerInfo` whose TCP endpoint uses the new multi-endpoint format.
fn make_tcp_peer_multi(endpoints: Vec<InterfaceEndpoint>) -> PeerInfo {
    let instance_id = crate::InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    let encoded = rmp_serde::to_vec(&endpoints).unwrap();
    builder.add_entry("tcp", encoded).unwrap();
    PeerInfo::new(instance_id, builder.build().unwrap())
}

/// Build a `TcpTransport` with its runtime set, bound to a real listener.
/// Returns `(transport, listener_addr)`.
fn make_transport() -> (TcpTransport, SocketAddr) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();
    // Set the runtime handle so `get_or_create_connection` can spawn tasks.
    transport
        .runtime
        .set(tokio::runtime::Handle::current())
        .ok();
    (transport, addr)
}

/// Build a `ConnectionHandle` over a channel of the given capacity.
fn make_handle(capacity: usize) -> (ConnectionHandle, flume::Receiver<SendTask>) {
    let (tx, rx) = flume::bounded::<SendTask>(capacity);
    let handle = ConnectionHandle {
        gate: AdmissionGate::new(tx.clone(), tokio::runtime::Handle::current()),
        tx,
    };
    (handle, rx)
}

/// Insert a stale `ConnectionHandle` into the transport's connections map.
/// A "stale" handle is one whose receiver has been dropped.
fn insert_stale_handle(transport: &TcpTransport, instance_id: crate::InstanceId) {
    let (handle, _rx) = make_handle(1);
    // Drop _rx immediately so tx.is_disconnected() == true
    transport.connections.insert((instance_id, 0), handle);
}

/// A `SendTask` whose error handler is the given one.
fn task(on_error: Arc<dyn TransportErrorHandler>) -> SendTask {
    SendTask {
        msg_type: MessageType::Message,
        header: Bytes::from_static(b"hdr"),
        payload: Bytes::from_static(b"pay"),
        on_error,
        queued_at: None,
    }
}

/// The egress instruments reach the real Prometheus collectors through the
/// observer the connection writer is handed. That seam — pre-bound handle into
/// `TcpWriterObserver`, tally out of the coalescing loop, counters in a
/// registry — is the one an end-to-end scenario cannot isolate, because it
/// cannot say which of the two ends failed.
#[tokio::test]
async fn writer_observer_publishes_egress_into_the_bound_handle() {
    use crate::observability::VeloMetrics;
    use crate::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = VeloMetrics::register(&registry).expect("register metrics");
    let handle: Arc<dyn velo_ext::TransportObservability> = Arc::new(metrics.bind_transport("tcp"));

    let frame = |msg_type| SendTask {
        msg_type,
        header: Bytes::from_static(b"hdr"),
        payload: Bytes::from_static(b"pay"),
        on_error: Arc::new(NullErrorHandler),
        queued_at: Some(Instant::now()),
    };

    let (tx, rx) = flume::unbounded::<SendTask>();
    for _ in 0..3 {
        tx.send(frame(MessageType::Message)).unwrap();
    }
    tx.send(frame(MessageType::Response)).unwrap();
    drop(tx);

    // A `Vec<u8>` stands in for the socket: this test is about the observer,
    // and `coalesce::tests` already owns the wire-format assertions.
    let mut sink: Vec<u8> = Vec::new();
    run_coalescing_writer(
        &mut sink,
        &rx,
        std::convert::identity,
        None,
        &TcpWriterObserver {
            instance_id: crate::InstanceId::new_v4(),
            lane: 0,
            addr: "127.0.0.1:1".parse().unwrap(),
            egress: Some(EgressMetrics::new(handle)),
        },
    )
    .await;

    let snap = MetricSnapshot::from_registry(&registry);
    assert_eq!(
        snap.counter(
            "velo_transport_frames_written_total",
            &[("transport", "tcp"), ("message_type", "message")]
        ),
        3.0
    );
    assert_eq!(
        snap.counter(
            "velo_transport_frames_written_total",
            &[("transport", "tcp"), ("message_type", "response")]
        ),
        1.0
    );
    assert_eq!(
        snap.histogram_count(
            "velo_transport_egress_queue_wait_seconds",
            &[("transport", "tcp")]
        ),
        4,
        "one queue-wait observation per frame"
    );
    assert_eq!(
        snap.histogram_count(
            "velo_transport_write_duration_seconds",
            &[("transport", "tcp")]
        ),
        1,
        "the four frames coalesced into one write"
    );
}

/// The wait clock starts *before* the frame is offered to the connection's
/// `AdmissionGate`, so a frame the gate holds is charged for the whole time it
/// was held. That placement is why this histogram exists beside the derived
/// depth rather than duplicating it: `accepted - written` is structurally blind
/// to the gate, because a gate-held frame has not been counted accepted yet.
///
/// Capacity 1 pins the split — one frame in the bounded channel, one blocked in
/// the gate's driver, one in the gate's pending queue — and the writer starts
/// only after the hold, so all three waited it out. A stamp taken behind the
/// gate would report one observation of one hold instead of three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_egress_queue_wait_spans_the_admission_gate() {
    use crate::observability::VeloMetrics;
    use crate::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = VeloMetrics::register(&registry).expect("register metrics");
    let handle: Arc<dyn velo_ext::TransportObservability> = Arc::new(metrics.bind_transport("tcp"));
    let (transport, _addr) = make_transport();
    transport.set_observability(Arc::clone(&handle));

    // A connection whose writer has not started: the channel takes one frame
    // and everything after it queues behind the gate.
    let instance_id = crate::InstanceId::new_v4();
    let (conn, rx) = make_handle(1);
    transport.connections.insert((instance_id, 0), conn);

    let outcomes: Vec<SendOutcome> = (0..3)
        .map(|_| {
            transport.send_message(
                instance_id,
                Bytes::from_static(b"hdr"),
                Bytes::from_static(b"pay"),
                MessageType::Message,
                Arc::new(NullErrorHandler),
            )
        })
        .collect();
    assert!(
        outcomes[0].is_admitted(),
        "the bounded channel takes the first frame"
    );
    assert!(
        !outcomes[1].is_admitted() && !outcomes[2].is_admitted(),
        "the other two are the gate's, which is the whole point of the test"
    );

    let hold = Duration::from_millis(200);
    tokio::time::sleep(hold).await;

    // Only now start the writer. A `Vec<u8>` stands in for the socket: this
    // test is about where the clock starts, and `coalesce::tests` owns the
    // wire format.
    let cancel = CancellationToken::new();
    let writer = {
        let cancel = cancel.clone();
        let handle = Arc::clone(&handle);
        tokio::spawn(async move {
            let mut sink: Vec<u8> = Vec::new();
            run_coalescing_writer(
                &mut sink,
                &rx,
                std::convert::identity,
                Some(&cancel),
                &TcpWriterObserver {
                    instance_id,
                    lane: 0,
                    addr: "127.0.0.1:1".parse().unwrap(),
                    egress: Some(EgressMetrics::new(handle)),
                },
            )
            .await;
        })
    };

    // The channel stays open for as long as the gate holds a sender, so the
    // writer is stopped by cancellation rather than by disconnection.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let observed = MetricSnapshot::from_registry(&registry).histogram_count(
            "velo_transport_egress_queue_wait_seconds",
            &[("transport", "tcp")],
        );
        if observed == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "only {observed} of three frames were observed waiting — a frame stamped \
                 behind the gate is never observed at all"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("the writer stopped on cancellation")
        .expect("writer task");

    let snap = MetricSnapshot::from_registry(&registry);
    let waited = snap.histogram_sum(
        "velo_transport_egress_queue_wait_seconds",
        &[("transport", "tcp")],
    );
    assert!(
        waited >= 2.0 * hold.as_secs_f64(),
        "the two gate-held frames carry the hold too: waited={waited}s over a {hold:?} hold"
    );
    assert_eq!(
        snap.counter_sum("velo_transport_frames_written_total", &[]),
        3.0,
        "and all three reached the writer"
    );
}

#[test]
fn test_parse_tcp_endpoint() {
    // With tcp:// prefix
    let addr = parse_tcp_endpoint(b"tcp://127.0.0.1:5555").unwrap();
    assert_eq!(addr.port(), 5555);

    // Without prefix
    let addr = parse_tcp_endpoint(b"127.0.0.1:6666").unwrap();
    assert_eq!(addr.port(), 6666);

    // Invalid
    assert!(parse_tcp_endpoint(b"invalid").is_err());
}

#[test]
fn test_builder_default_prebinds() {
    // Builder without explicit bind_addr should pre-bind to 0.0.0.0:0
    let result = TcpTransportBuilder::new().build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_with_bind_addr() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let result = TcpTransportBuilder::new().bind_addr(addr).build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_with_listener() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let result = TcpTransportBuilder::new().from_listener(listener);
    assert!(result.is_ok());
    let result = result.unwrap().build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_bind_addr_and_listener_mutually_exclusive() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let result = TcpTransportBuilder::new()
        .bind_addr(addr)
        .from_listener(listener);
    assert!(result.is_err());
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("mutually exclusive"));
}

#[test]
fn test_builder_multi_endpoint_format() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();

    // The address should contain msgpack-encoded endpoints
    let wa = transport.address();
    let raw = wa.get_entry("tcp").unwrap().unwrap();
    let endpoints: Vec<InterfaceEndpoint> = rmp_serde::from_slice(&raw).unwrap();
    assert!(!endpoints.is_empty());
    // All endpoints should have the correct port
    for ep in &endpoints {
        assert_eq!(ep.port, addr.port());
    }
}

#[tokio::test]
async fn test_register_legacy_format() {
    let (transport, _our_addr) = make_transport();
    let peer_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let peer = make_tcp_peer(peer_addr);
    let iid = peer.instance_id();
    // Legacy "tcp://host:port" format should still work
    transport.register(peer).unwrap();
    assert!(transport.peers.contains_key(&iid));
}

#[tokio::test]
async fn test_register_multi_endpoint_format() {
    let (transport, _our_addr) = make_transport();
    let endpoints = vec![InterfaceEndpoint {
        name: "eth0".to_string(),
        ip: "127.0.0.1".to_string(),
        port: 9999,
        prefix_len: 8,
        numa_node: None,
    }];
    let peer = make_tcp_peer_multi(endpoints);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();
    assert!(transport.peers.contains_key(&iid));
}

#[tokio::test]
async fn test_get_or_create_connection_replaces_stale_handle() {
    let (transport, _our_addr) = make_transport();

    // Start a listener that the transport can connect to
    let peer_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();

    let peer = make_tcp_peer(peer_addr);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();

    // Insert a stale handle
    insert_stale_handle(&transport, iid);
    assert!(
        transport
            .connections
            .get(&(iid, 0))
            .unwrap()
            .tx
            .is_disconnected()
    );

    // get_or_create_connection should replace the stale handle with a live one
    let handle = transport.get_or_create_connection((iid, 0)).unwrap();
    assert!(!handle.tx.is_disconnected());

    // The map entry should also be live
    let entry = transport.connections.get(&(iid, 0)).unwrap();
    assert!(!entry.tx.is_disconnected());
}

#[tokio::test]
async fn test_check_health_removes_stale_entry() {
    let (transport, _our_addr) = make_transport();

    // Start a listener so the peer is "reachable"
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();

    let peer = make_tcp_peer(peer_addr);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();

    // Insert stale handle — simulates a dead writer task
    insert_stale_handle(&transport, iid);
    assert!(transport.connections.contains_key(&(iid, 0)));

    // check_health should remove the stale entry and verify the peer is reachable
    let result = transport.check_health(iid, Duration::from_secs(2)).await;

    // Stale entry should be gone
    assert!(!transport.connections.contains_key(&(iid, 0)));

    // Since there WAS a previous connection entry, check_health returns Ok
    // (the peer is reachable via our test listener)
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_writer_task_cleans_up_on_write_error() {
    // Bind a listener, accept once, then drop everything to cause a write error
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let iid = crate::InstanceId::new_v4();
    let (handle, rx) = make_handle(8);
    let tx = handle.tx.clone();

    let connections: Arc<DashMap<LaneKey, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert((iid, 0), handle);

    let conns = Arc::clone(&connections);
    let cancel = CancellationToken::new();

    // Spawn the writer task
    let writer = tokio::spawn(connection_writer_task(
        addr,
        (iid, 0),
        rx,
        tx.clone(),
        WriterTaskContext {
            connections: conns,
            cancel_token: cancel,
            connect_timeout: Duration::from_secs(5),
            reader_ctx: None,
            metrics: None,
            socket_buffers: None,
        },
    ));

    // Accept the connection, then immediately drop it + the listener
    let (stream, _) = listener.accept().await.unwrap();
    drop(stream);
    drop(listener);

    // Send messages until the writer's rx is dropped. A single small write
    // can land entirely in the kernel send buffer before the peer's RST is
    // observed; the EPIPE is then surfaced on the *next* write. We loop
    // (with yields) so the broken-pipe path is exercised deterministically.
    for _ in 0..256 {
        if tx
            .send(SendTask {
                msg_type: MessageType::Message,
                header: Bytes::from_static(b"hdr"),
                payload: Bytes::from_static(b"pay"),
                on_error: Arc::new(NullErrorHandler),
                queued_at: None,
            })
            .is_err()
        {
            break; // writer's rx dropped — it has already exited
        }
        tokio::task::yield_now().await;
    }

    drop(tx);

    // Wait for writer task to finish, bounded so a stuck test fails loudly.
    let join_result = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("writer task did not exit within 5s of peer disconnect")
        .expect("writer task panicked");
    // The writer returns Ok(()) once its inner loop has cleanly exited;
    // a write error inside the loop is handled (logged + on_error) and
    // doesn't propagate, so this assertion mostly guards against a future
    // refactor that surfaces the error through the join.
    join_result.expect("writer task returned an error");

    // The writer should have removed the stale entry from the map
    assert!(
        !connections.contains_key(&(iid, 0)),
        "writer task should clean up its DashMap entry on write error"
    );
}

#[tokio::test]
async fn test_send_message_does_not_fail_on_stale_handle() {
    let (transport, _our_addr) = make_transport();

    // Start a listener that accepts connections (simulates a healthy peer)
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();

    let peer = make_tcp_peer(peer_addr);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();

    // Insert a stale handle
    insert_stale_handle(&transport, iid);

    // send_message should detect the stale handle and create a new one,
    // NOT immediately call on_error. This exercises the slow path
    // (get_or_create_connection + try_send on a freshly-created handle).
    let error_handler = Arc::new(TrackingErrorHandler::new());
    assert!(
        transport
            .send_message(
                iid,
                Bytes::from_static(b"test-header"),
                Bytes::from_static(b"test-payload"),
                MessageType::Message,
                error_handler.clone(),
            )
            .is_admitted(),
        "a fresh connection's channel is empty, so the send admits immediately"
    );

    // Accept the connection that the new writer task will establish
    let (mut stream, _) = peer_listener.accept().await.unwrap();

    // Read the framed message from the stream to confirm delivery
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 256];
    // Give the async writer a moment to flush the frame
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("timed out waiting for data")
        .expect("read error");
    assert!(n > 0, "expected data from the writer task");

    // No errors should have been reported
    assert_eq!(
        error_handler.error_count(),
        0,
        "send_message should retry on stale handle, not fail"
    );

    // The connections map should now contain a live handle
    let entry = transport.connections.get(&(iid, 0)).unwrap();
    assert!(
        !entry.tx.is_disconnected(),
        "stale handle should have been replaced with a live one"
    );
}

#[tokio::test]
async fn test_writer_task_drains_on_connect_failure() {
    // Use an address where nothing is listening so connect will fail.
    // Binding then immediately dropping gives us a port that is guaranteed closed.
    let tmp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = tmp.local_addr().unwrap();
    drop(tmp);

    let iid = crate::InstanceId::new_v4();
    let (handle, rx) = make_handle(8);
    let tx = handle.tx.clone();

    let connections: Arc<DashMap<LaneKey, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert((iid, 0), handle);

    // Queue a message *before* the writer task even starts — this simulates
    // the race between create_connection returning and connect completing.
    let error_handler = Arc::new(TrackingErrorHandler::new());
    tx.send(SendTask {
        msg_type: MessageType::Message,
        header: Bytes::from_static(b"hdr"),
        payload: Bytes::from_static(b"pay"),
        on_error: error_handler.clone(),
        queued_at: None,
    })
    .unwrap();

    let conns = Arc::clone(&connections);
    let cancel = CancellationToken::new();

    let writer = tokio::spawn(connection_writer_task(
        addr,
        (iid, 0),
        rx,
        tx.clone(),
        WriterTaskContext {
            connections: conns,
            cancel_token: cancel,
            connect_timeout: Duration::from_secs(5),
            reader_ctx: None,
            metrics: None,
            socket_buffers: None,
        },
    ));
    drop(tx);
    let error = writer.await.unwrap().unwrap_err();
    assert!(format!("{error:#}").contains("connect failed"));
    assert!(error_handler.reasons.lock().unwrap()[0].contains("connect failed"));

    assert_eq!(
        error_handler.error_count(),
        1,
        "queued message should have its on_error called when connect fails"
    );

    assert!(
        !connections.contains_key(&(iid, 0)),
        "writer task should clean up its DashMap entry on connect failure"
    );
}

/// Replacing a stale connection must kill the old epoch's queued frames.
///
/// The gate is per connection, so a frame queued behind a connection that has
/// since died must never be handed to its successor — it was addressed to a
/// socket that no longer exists.
#[tokio::test]
async fn stale_replacement_fails_the_old_epoch_and_admits_on_the_successor() {
    let (transport, _our_addr) = make_transport();

    let peer_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();
    let peer = make_tcp_peer(peer_addr);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();

    // A live one-slot connection, filled and then queued behind.
    let (handle, rx) = make_handle(1);
    transport.connections.insert((iid, 0), handle.clone());
    let errors = Arc::new(TrackingErrorHandler::new());
    assert!(handle.gate.send(task(errors.clone())).is_admitted());
    let queued = match handle.gate.send(task(errors.clone())) {
        SendOutcome::Pending(admission) => admission,
        SendOutcome::Admitted => panic!("a full channel must not admit"),
    };

    // Kill the connection. There is deliberately no await between here and the
    // assertions: the gate's driver has never run, so the queued frame is still
    // in the gate and `fail_all` resolves it synchronously.
    drop(rx);
    assert!(handle.tx.is_disconnected());
    let fresh = transport.get_or_create_connection((iid, 0)).unwrap();

    assert_eq!(
        queued.state(),
        AdmissionState::Failed,
        "the old epoch's queued frame must not survive the replacement"
    );
    assert!(!fresh.tx.is_disconnected(), "the successor should be live");
    assert!(
        fresh.gate.send(task(errors)).is_admitted(),
        "the successor's gate is unaffected by the dead epoch"
    );
}

/// The reported capacity is exactly the codec's encode ceiling, not an
/// approximation of it: a frame whose `header + payload` sums to it builds a
/// preamble, and one byte more does not.
///
/// Nothing is subtracted for the 11-byte preamble because
/// `validate_lengths_limit` never counts it — it caps the two content lengths
/// alone. That is the whole derivation, and this test is what keeps it true.
#[tokio::test]
async fn max_message_size_is_exactly_what_the_codec_will_encode() {
    let (transport, _addr) = make_transport();

    let capacity = transport
        .max_message_size(crate::InstanceId::new_v4())
        .expect("TCP always knows its framed limit");
    assert_eq!(capacity, 16 * 1024 * 1024);

    // The codec caps the sum, so the split across header/payload is arbitrary.
    let header_len = 1024u32;
    let payload_len = capacity as u32 - header_len;
    assert!(
        TcpFrameCodec::build_preamble(MessageType::Message, header_len, payload_len).is_ok(),
        "a frame of exactly the reported capacity must encode",
    );
    assert!(
        TcpFrameCodec::build_preamble(MessageType::Message, header_len, payload_len + 1).is_err(),
        "one byte past the reported capacity must not",
    );
}

/// A size below the common `rmem_max` of 212,992 and far from the 2 MiB
/// default, so a test that uses it tells the setting apart from the default.
const SMALL_BUFFERS: usize = 98_304;

/// What a new TCP socket reports for its receive and send buffers: the
/// kernel's defaults, and the values once `bytes` is set on it. Linux doubles
/// a set value for bookkeeping and clamps it, so the pairs differ on common
/// hosts. On a host where one pair is equal, the tests that use this cannot
/// tell a sized socket from an unsized one, so the helper fails there instead
/// of letting them pass with no effect.
struct BufferSizes {
    default_recv: usize,
    sized_recv: usize,
    default_send: usize,
    sized_send: usize,
}

fn buffer_sizes(bytes: usize) -> BufferSizes {
    let fresh =
        || socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
    let default = fresh();
    let sized = fresh();
    sized.set_recv_buffer_size(bytes).unwrap();
    sized.set_send_buffer_size(bytes).unwrap();
    let sizes = BufferSizes {
        default_recv: default.recv_buffer_size().unwrap(),
        sized_recv: sized.recv_buffer_size().unwrap(),
        default_send: default.send_buffer_size().unwrap(),
        sized_send: sized.send_buffer_size().unwrap(),
    };
    assert_ne!(
        sizes.default_recv, sizes.sized_recv,
        "a sized socket reads as the default on this host"
    );
    assert_ne!(
        sizes.default_send, sizes.sized_send,
        "a sized socket reads as the default on this host"
    );
    sizes
}

fn recv_and_send<'a>(sock: impl Into<socket2::SockRef<'a>>) -> (usize, usize) {
    let sock = sock.into();
    (
        sock.recv_buffer_size().unwrap(),
        sock.send_buffer_size().unwrap(),
    )
}

fn listener_buffers(transport: &TcpTransport) -> (usize, usize) {
    let guard = transport.listener.lock().unwrap();
    recv_and_send(guard.as_ref().expect("the builder binds the listener"))
}

/// By default the listening socket is sized, as it always was, so accepted
/// sockets inherit the size. With `socket_buffers(None)` it keeps the kernel's
/// default, so accepted sockets autotune. Both ways of giving the builder a
/// listener are covered: `bind_addr`, where the builder binds its own socket,
/// and `from_listener`, which the examples use.
///
/// An explicit size turns autotuning off and is clamped to `rmem_max`; on
/// hosts where that is 212,992 it caps one connection's window near 208 KiB.
#[test]
fn socket_buffers_sizes_the_listener_or_leaves_it_to_the_kernel() {
    // The book and the builder doc state 2 MiB.
    assert_eq!(
        super::super::listener::DEFAULT_SOCKET_BUFFERS,
        2 * 1024 * 1024
    );
    let default_sizes = buffer_sizes(super::super::listener::DEFAULT_SOCKET_BUFFERS);
    let small = buffer_sizes(SMALL_BUFFERS);
    let bound = |builder: TcpTransportBuilder| {
        builder
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .build()
            .unwrap()
    };
    let given = |builder: TcpTransportBuilder| {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        builder.from_listener(listener).unwrap().build().unwrap()
    };
    for (name, build) in [
        (
            "bind_addr",
            &bound as &dyn Fn(TcpTransportBuilder) -> TcpTransport,
        ),
        ("from_listener", &given),
    ] {
        let by_default = build(TcpTransportBuilder::new());
        assert_eq!(
            listener_buffers(&by_default),
            (default_sizes.sized_recv, default_sizes.sized_send),
            "{name}, default"
        );
        let small_transport = build(TcpTransportBuilder::new().socket_buffers(Some(SMALL_BUFFERS)));
        assert_eq!(
            listener_buffers(&small_transport),
            (small.sized_recv, small.sized_send),
            "{name}, {SMALL_BUFFERS}"
        );
        let autotuned = build(TcpTransportBuilder::new().socket_buffers(None));
        assert_eq!(
            listener_buffers(&autotuned),
            (small.default_recv, small.default_send),
            "{name}, None"
        );
    }
}

/// `start()` builds the listener that serves the socket, and that listener
/// sizes a socket it is given once more. So `start()` must hand it the
/// transport's setting, or `socket_buffers(None)` would turn into 2 MiB there
/// and every accepted socket would lose autotuning. The listener is read
/// through a clone once a frame has arrived, which proves that the accept loop
/// (it runs after that second sizing) has started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_keeps_the_listener_setting() {
    let small = buffer_sizes(SMALL_BUFFERS);
    for (setting, expected) in [
        (Some(SMALL_BUFFERS), (small.sized_recv, small.sized_send)),
        (None, (small.default_recv, small.default_send)),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let probe = listener.try_clone().unwrap();
        let server = TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .socket_buffers(setting)
            .build()
            .unwrap();
        let (adapter, streams) = crate::transports::make_channels();
        let server_id = crate::InstanceId::new_v4();
        server
            .start(server_id, adapter, tokio::runtime::Handle::current())
            .await
            .unwrap();

        let (client, _) = make_transport();
        client
            .register(PeerInfo::new(server_id, server.address()))
            .unwrap();
        let _ = client.send_message(
            server_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Event,
            Arc::new(NullErrorHandler),
        );
        tokio::time::timeout(Duration::from_secs(5), streams.event_stream.recv_async())
            .await
            .expect("the frame arrives")
            .expect("event stream open");

        assert_eq!(recv_and_send(&probe), expected, "{setting:?}");
        client.shutdown();
        server.shutdown();
    }
}

/// A dialed socket follows the same setting. The transport builds each
/// connection writer's context with `writer_context`, and `dial` is the step
/// of the writer that connects and sets up the socket, so this reads the
/// socket that the transport writes to. With `None`, the socket must read the
/// same as one dialed with no setting at all.
#[tokio::test]
async fn dial_sizes_the_socket_or_leaves_it_to_the_kernel() {
    let small = buffer_sizes(SMALL_BUFFERS);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (unsized_dial, _accepted) =
        tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
    let untouched = recv_and_send(&unsized_dial.unwrap());

    for (setting, expected) in [
        (Some(SMALL_BUFFERS), (small.sized_recv, small.sized_send)),
        (None, untouched),
    ] {
        let transport = TcpTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .socket_buffers(setting)
            .build()
            .unwrap();
        let ctx = transport.writer_context();
        let (dialed, _accepted) = tokio::join!(super::dial(addr, &ctx), listener.accept());
        let dialed = dialed.unwrap().expect("not cancelled");
        assert_eq!(recv_and_send(&dialed), expected, "{setting:?}");
    }
}

/// Replacing a dead connection must not update the connection gauge while the
/// map entry is held. The gauge reads `len()`, which read-locks every shard,
/// and the shard that the entry holds for writing is not reentrant: the
/// thread waits on itself, and every later operation on that shard waits
/// behind it. The dead entry is seeded directly because in normal use it only
/// appears in a race between `reap_stale_connection` and `entry()`.
///
/// The test owns its runtime and drops it in the background on a timeout, so
/// the fault fails the test instead of hanging the run. With the fault, a
/// task that later touches the map blocks a runtime worker on the held shard,
/// and dropping a `#[tokio::test]` runtime would then wait for it forever.
#[test]
fn replacing_a_dead_connection_does_not_deadlock() {
    use crate::observability::VeloMetrics;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let guard = rt.enter();

    let registry = prometheus::Registry::new();
    let metrics = VeloMetrics::register(&registry).expect("register metrics");
    let (transport, _addr) = make_transport();
    // Observed, so the gauge update runs.
    transport.set_observability(Arc::new(metrics.bind_transport("tcp")));

    let peer_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = make_tcp_peer(peer_listener.local_addr().unwrap());
    let iid = peer.instance_id();
    transport.register(peer).unwrap();
    insert_stale_handle(&transport, iid);

    let transport = Arc::new(transport);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn({
        let transport = transport.clone();
        let handle = rt.handle().clone();
        move || {
            let installed = transport.install_connection((iid, 0), &handle).is_ok();
            let _ = done_tx.send(installed);
        }
    });
    let Ok(installed) = done_rx.recv_timeout(Duration::from_secs(5)) else {
        drop(guard);
        rt.shutdown_background();
        panic!("install_connection deadlocked replacing a dead connection");
    };
    assert!(installed);
    assert!(
        !transport
            .connections
            .get(&(iid, 0))
            .unwrap()
            .tx
            .is_disconnected()
    );
    transport.shutdown();
}

/// A frame as a raw peer saw it: the index of the connection it came on, in
/// accept order, and the frame, or `None` when that connection ended.
type RawFrame = (usize, Option<(MessageType, Bytes, Bytes)>);

/// A plain TCP listener standing in for the peer, so a test can see which
/// connection each frame came on. The listener of the transport routes the
/// frames of every connection into one stream, which hides that.
struct RawPeer {
    info: PeerInfo,
    frames: flume::Receiver<RawFrame>,
    accepted: Arc<AtomicUsize>,
}

impl RawPeer {
    async fn start() -> Self {
        use futures::StreamExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let info = make_tcp_peer(listener.local_addr().unwrap());
        let (tx, frames) = flume::unbounded();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let index = count.fetch_add(1, Ordering::SeqCst);
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut decoded =
                        tokio_util::codec::FramedRead::new(stream, TcpFrameCodec::new());
                    while let Some(Ok(frame)) = decoded.next().await {
                        let _ = tx.send((index, Some(frame)));
                    }
                    let _ = tx.send((index, None));
                });
            }
        });
        Self {
            info,
            frames,
            accepted,
        }
    }

    /// The next frame, which must arrive within 5 s.
    async fn next(&self) -> RawFrame {
        tokio::time::timeout(Duration::from_secs(5), self.frames.recv_async())
            .await
            .expect("the frame arrives")
            .unwrap()
    }
}

/// A started-enough transport with `lanes` lanes, registered with a raw peer.
async fn laned_pair(lanes: u16) -> (Arc<TcpTransport>, RawPeer, crate::InstanceId) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .lanes(lanes)
        .build()
        .unwrap();
    transport
        .runtime
        .set(tokio::runtime::Handle::current())
        .ok();
    let peer = RawPeer::start().await;
    let peer_id = peer.info.instance_id();
    transport.register(peer.info.clone()).unwrap();
    (Arc::new(transport), peer, peer_id)
}

#[test]
fn the_builder_keeps_at_least_one_lane() {
    let id = crate::InstanceId::new_v4();
    let lanes = |builder: TcpTransportBuilder| builder.build().unwrap().lanes(id).get();
    assert_eq!(lanes(TcpTransportBuilder::new()), 1, "the default");
    assert_eq!(lanes(TcpTransportBuilder::new().lanes(0)), 1, "0 means 1");
    assert_eq!(lanes(TcpTransportBuilder::new().lanes(4)), 4);
}

/// Each lane is its own TCP connection, and keeps its own order while frames
/// queue in its admission gate.
///
/// The header carries `(lane, seq)`. The peer sees the lanes interleaved,
/// which is allowed, and each lane's sequence in order, which is the contract.
/// Lanes that share one connection also pass the order check, so the test
/// also checks that the frames of each lane all came on one connection, and
/// that no two lanes shared one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_lane_is_its_own_connection_and_keeps_its_own_order() {
    const LANES: u16 = 4;
    const FRAMES: u32 = 5_000;
    let (transport, peer, peer_id) = laned_pair(LANES).await;
    assert_eq!(transport.lanes(peer_id).get(), LANES);

    let errors = Arc::new(TrackingErrorHandler::new());
    let mut senders = Vec::new();
    for lane in 0..LANES {
        let transport = transport.clone();
        let errors = errors.clone();
        senders.push(tokio::spawn(async move {
            // Send everything before awaiting any admission, so most frames
            // queue in the gate behind a full channel, and the order check
            // below covers the gate's queue as well as the channel. It does
            // not catch a send that skips the gate: flume hands a waiting
            // sender's frame into the channel as soon as a slot frees, so a
            // skipping send almost never finds room (measured: a `try_send`
            // ahead of the gate passed this test 5 of 5 times).
            let mut pending = Vec::new();
            for seq in 0..FRAMES {
                let mut header = lane.to_le_bytes().to_vec();
                header.extend_from_slice(&seq.to_le_bytes());
                let outcome = transport.send_message_on_lane(
                    peer_id,
                    lane,
                    Bytes::from(header),
                    Bytes::from_static(b"token"),
                    MessageType::Event,
                    errors.clone(),
                );
                if let SendOutcome::Pending(admission) = outcome {
                    pending.push(admission);
                }
            }
            assert!(!pending.is_empty(), "no send queued in the gate");
            for admission in pending {
                admission.await.unwrap();
            }
        }));
    }

    let mut next = vec![0u32; usize::from(LANES)];
    let mut connection_of: Vec<Option<usize>> = vec![None; usize::from(LANES)];
    for _ in 0..FRAMES * u32::from(LANES) {
        let (connection, frame) = peer.next().await;
        let (_, header, _) = frame.expect("no connection ends while the lanes send");
        let lane = usize::from(u16::from_le_bytes(header[..2].try_into().unwrap()));
        let seq = u32::from_le_bytes(header[2..6].try_into().unwrap());
        assert_eq!(seq, next[lane], "lane {lane} out of order");
        next[lane] += 1;
        let first = *connection_of[lane].get_or_insert(connection);
        assert_eq!(first, connection, "lane {lane} moved to another connection");
    }
    for sender in senders {
        sender.await.unwrap();
    }
    assert_eq!(errors.error_count(), 0);

    let mut distinct: Vec<usize> = connection_of.iter().map(|c| c.unwrap()).collect();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        usize::from(LANES),
        "each lane has its own connection: {connection_of:?}"
    );
    assert_eq!(peer.accepted.load(Ordering::SeqCst), usize::from(LANES));
    for lane in 0..LANES {
        assert!(transport.connections.contains_key(&(peer_id, lane)));
    }
    transport.shutdown();
}

/// `send_message` is lane 0, and a lane at or past the count wraps.
///
/// Ordinary traffic must keep one ordered channel, so a transport built with
/// several lanes must not open a second connection for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_message_is_lane_zero_and_lanes_wrap() {
    let (transport, peer, peer_id) = laned_pair(3).await;
    let errors = Arc::new(TrackingErrorHandler::new());

    // Several sends, so that a send_message that rotates over lanes opens a
    // second connection.
    for _ in 0..3 {
        let _ = transport.send_message(
            peer_id,
            Bytes::from_static(b"plain"),
            Bytes::new(),
            MessageType::Event,
            errors.clone(),
        );
    }
    let mut plain_connection = None;
    for _ in 0..3 {
        let (connection, frame) = peer.next().await;
        assert_eq!(frame.unwrap().1, Bytes::from_static(b"plain"));
        assert_eq!(*plain_connection.get_or_insert(connection), connection);
    }
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(transport.connections.len(), 1);
    assert!(transport.connections.contains_key(&(peer_id, 0)));

    // Lane 4 of 3: modulo gives lane 1, where a clamp would give lane 2.
    let _ = transport.send_message_on_lane(
        peer_id,
        4,
        Bytes::from_static(b"wrapped"),
        Bytes::new(),
        MessageType::Event,
        errors.clone(),
    );
    let (connection, frame) = peer.next().await;
    assert_eq!(frame.unwrap().1, Bytes::from_static(b"wrapped"));
    assert_ne!(Some(connection), plain_connection, "lane 1 is not lane 0");
    assert!(
        transport.connections.contains_key(&(peer_id, 1)),
        "lane 4 of 3 is lane 1"
    );
    assert_eq!(transport.connections.len(), 2);
    assert_eq!(errors.error_count(), 0);
    transport.shutdown();
}

/// A peer reached only on a lane other than 0 is healthy.
///
/// A caller can put a flow on any lane, so a peer can have live connections
/// with none on lane 0. A health check that looks at lane 0 alone dials a
/// throwaway probe and calls such a peer `NeverConnected`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_live_only_on_a_later_lane_is_healthy() {
    let (transport, peer, peer_id) = laned_pair(3).await;
    let errors = Arc::new(TrackingErrorHandler::new());
    let _ = transport.send_message_on_lane(
        peer_id,
        2,
        Bytes::from_static(b"lane two"),
        Bytes::new(),
        MessageType::Event,
        errors.clone(),
    );
    assert!(peer.next().await.1.is_some());
    assert!(!transport.connections.contains_key(&(peer_id, 0)));

    let health = transport
        .check_health(peer_id, Duration::from_secs(1))
        .await;
    assert!(health.is_ok(), "{health:?}");
    assert_eq!(errors.error_count(), 0);
    transport.shutdown();
}

/// `shutdown()` closes every lane's connection, and every frame sent on any
/// lane is delivered or failed.
///
/// Every lane is connected, and has delivered a bulk frame, before shutdown
/// lands. On loopback the rest of the bulk often arrives before the writers
/// see the cancel, so the test does not count on any frame failing. It proves
/// that every lane closes and that no frame is lost or counted twice. Each
/// send is admitted at once (the bulk fits the send channel), so a frame is
/// either on the wire or reported through `on_error`. None can be dropped in
/// the gate. The writer's drain of unsent frames has its own tests:
/// `test_writer_task_cleans_up_on_write_error` and
/// `test_writer_task_drains_on_connect_failure`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_every_lane() {
    const LANES: u16 = 4;
    const FRAMES: usize = 128;
    const PAYLOAD: usize = 64 * 1024;
    let (transport, peer, peer_id) = laned_pair(LANES).await;
    let errors = Arc::new(TrackingErrorHandler::new());

    let send = |lane: u16, bytes: usize| {
        let outcome = transport.send_message_on_lane(
            peer_id,
            lane,
            Bytes::copy_from_slice(&lane.to_be_bytes()),
            Bytes::from(vec![0u8; bytes]),
            MessageType::Response,
            errors.clone(),
        );
        assert!(outcome.is_admitted(), "the bulk must fit the send channel");
    };
    for lane in 0..LANES {
        send(lane, 0);
    }
    for _ in 0..LANES {
        assert!(peer.next().await.1.is_some(), "every lane connects");
    }
    assert_eq!(peer.accepted.load(Ordering::SeqCst), usize::from(LANES));

    for i in 0..FRAMES {
        send((i % usize::from(LANES)) as u16, PAYLOAD);
    }
    // Shut down only once every lane has delivered a bulk frame, so shutdown
    // lands on writers that are streaming, not on writers yet to start.
    let mut per_lane = vec![0usize; usize::from(LANES)];
    let mut ended = 0;
    while per_lane.contains(&0) {
        let (_, frame) = peer.next().await;
        let (_, header, _) = frame.expect("no connection ends before shutdown");
        per_lane[usize::from(u16::from_be_bytes([header[0], header[1]]))] += 1;
    }
    transport.shutdown();

    while ended < usize::from(LANES) {
        match peer.next().await {
            (_, Some((_, header, _))) => {
                per_lane[usize::from(u16::from_be_bytes([header[0], header[1]]))] += 1;
            }
            (_, None) => ended += 1,
        }
    }
    assert!(transport.connections.is_empty());

    // A writer reports the frames it did not write after its socket closes, so
    // the count can trail the last end of stream by a moment.
    let delivered: usize = per_lane.iter().sum();
    let deadline = Instant::now() + Duration::from_secs(5);
    while delivered + errors.error_count() < FRAMES && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let failed = errors.error_count();
    assert_eq!(
        delivered + failed,
        FRAMES,
        "{delivered} delivered + {failed} failed != {FRAMES} sent"
    );
}

/// A sender can retain an old epoch while its writer retires. Its accepted
/// frames must fail, and cleanup must leave a replacement entry intact.
#[tokio::test]
async fn retirement_accounts_for_late_sends_without_removing_the_successor() {
    let key = (crate::InstanceId::new_v4(), 0);
    let connections = Arc::new(DashMap::new());
    let (old, rx) = make_handle(4);
    connections.insert(key, old.clone());
    let errors = Arc::new(TrackingErrorHandler::new());
    let closing = tokio::spawn({
        let connections = connections.clone();
        let tx = old.tx.clone();
        async move { retire_connection(key, tx, rx, &connections, "retired").await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while connections.contains_key(&key) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (next, _next_rx) = make_handle(4);
    connections.insert(key, next.clone());
    for _ in 0..2 {
        assert!(old.gate.send(task(errors.clone())).is_admitted());
    }
    drop(old);
    tokio::time::timeout(Duration::from_secs(2), closing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(errors.error_count(), 2);
    assert!(connections.get(&key).unwrap().tx.same_channel(&next.tx));
}
