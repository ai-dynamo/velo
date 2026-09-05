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
}

impl TrackingErrorHandler {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }

    fn error_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl TransportErrorHandler for TrackingErrorHandler {
    fn on_error(&self, _: Bytes, _: Bytes, _: String) {
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
    transport.connections.insert(instance_id, handle);
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
            addr: "127.0.0.1:1".parse().unwrap(),
            egress: EgressMetrics::new(Some(handle)),
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
    transport.connections.insert(instance_id, conn);

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
                    addr: "127.0.0.1:1".parse().unwrap(),
                    egress: EgressMetrics::new(Some(handle)),
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
            .get(&iid)
            .unwrap()
            .tx
            .is_disconnected()
    );

    // get_or_create_connection should replace the stale handle with a live one
    let handle = transport.get_or_create_connection(iid).unwrap();
    assert!(!handle.tx.is_disconnected());

    // The map entry should also be live
    let entry = transport.connections.get(&iid).unwrap();
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
    assert!(transport.connections.contains_key(&iid));

    // check_health should remove the stale entry and verify the peer is reachable
    let result = transport.check_health(iid, Duration::from_secs(2)).await;

    // Stale entry should be gone
    assert!(!transport.connections.contains_key(&iid));

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

    let connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert(iid, handle);

    let conns = Arc::clone(&connections);
    let cancel = CancellationToken::new();

    // Spawn the writer task
    let writer = tokio::spawn(connection_writer_task(
        addr,
        iid,
        rx,
        WriterTaskContext {
            connections: conns,
            cancel_token: cancel,
            connect_timeout: Duration::from_secs(5),
            reader_ctx: None,
            metrics: None,
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
        !connections.contains_key(&iid),
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
    let entry = transport.connections.get(&iid).unwrap();
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

    let connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert(iid, handle);

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
        iid,
        rx,
        WriterTaskContext {
            connections: conns,
            cancel_token: cancel,
            connect_timeout: Duration::from_secs(5),
            reader_ctx: None,
            metrics: None,
        },
    ));
    let _ = writer.await;

    assert_eq!(
        error_handler.error_count(),
        1,
        "queued message should have its on_error called when connect fails"
    );

    assert!(
        !connections.contains_key(&iid),
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
    transport.connections.insert(iid, handle.clone());
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
    let fresh = transport.get_or_create_connection(iid).unwrap();

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
