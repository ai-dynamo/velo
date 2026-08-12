// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the UDS transport's endpoint parsing, connection lifecycle and builder.

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

/// Build a `PeerInfo` whose UDS endpoint points at `path`.
fn make_uds_peer(path: &Path) -> PeerInfo {
    let instance_id = crate::InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    builder
        .add_entry("uds", format!("uds://{}", path.display()).into_bytes())
        .unwrap();
    PeerInfo::new(instance_id, builder.build().unwrap())
}

/// Build a `UdsTransport` with its runtime set, bound to a temp socket path.
/// Returns `(transport, socket_path)`.
fn make_transport() -> (UdsTransport, PathBuf) {
    let dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("test.sock");
    let transport = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();
    transport
        .runtime
        .set(tokio::runtime::Handle::current())
        .ok();
    (transport, socket_path)
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
fn insert_stale_handle(transport: &UdsTransport, instance_id: crate::InstanceId) {
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
    }
}

#[test]
fn test_parse_uds_endpoint() {
    // With uds:// prefix
    let path = parse_uds_endpoint(b"uds:///tmp/test.sock").unwrap();
    assert_eq!(path, PathBuf::from("/tmp/test.sock"));

    // Without prefix
    let path = parse_uds_endpoint(b"/var/run/anvil.sock").unwrap();
    assert_eq!(path, PathBuf::from("/var/run/anvil.sock"));

    // Empty path
    assert!(parse_uds_endpoint(b"").is_err());
}

#[test]
fn test_builder_requires_socket_path() {
    let result = UdsTransportBuilder::new().build();
    assert!(result.is_err());
}

#[test]
fn test_builder_with_socket_path() {
    let result = UdsTransportBuilder::new()
        .socket_path("/tmp/test.sock")
        .build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_custom_key() {
    let transport = UdsTransportBuilder::new()
        .socket_path("/tmp/test.sock")
        .key(TransportKey::from("custom-uds"))
        .build()
        .unwrap();
    assert_eq!(transport.key(), TransportKey::from("custom-uds"));
}

#[test]
fn test_transport_socket_path() {
    let transport = UdsTransportBuilder::new()
        .socket_path("/tmp/test.sock")
        .build()
        .unwrap();
    assert_eq!(transport.socket_path(), Path::new("/tmp/test.sock"));
}

#[tokio::test]
async fn test_get_or_create_connection_replaces_stale_handle() {
    let (transport, _socket_path) = make_transport();

    // Start a UDS listener that the transport can connect to
    let dir = std::env::temp_dir().join(format!("uds-peer-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let peer_socket = dir.join("peer.sock");
    let peer_listener = tokio::net::UnixListener::bind(&peer_socket).unwrap();

    let peer = make_uds_peer(&peer_socket);
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

    // Cleanup
    drop(peer_listener);
    std::fs::remove_file(&peer_socket).ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_check_health_removes_stale_entry() {
    let (transport, _socket_path) = make_transport();

    // Start a UDS listener so the peer is "reachable"
    let dir = std::env::temp_dir().join(format!("uds-peer-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let peer_socket = dir.join("peer.sock");
    let _peer_listener = tokio::net::UnixListener::bind(&peer_socket).unwrap();

    let peer = make_uds_peer(&peer_socket);
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
    assert!(result.is_ok());

    // Cleanup
    std::fs::remove_file(&peer_socket).ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_writer_task_cleans_up_on_write_error() {
    // Bind a UDS listener, accept once, then drop everything to cause a write error
    let dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("writer-test.sock");
    let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();

    let iid = crate::InstanceId::new_v4();
    let (handle, rx) = make_handle(8);
    let tx = handle.tx.clone();

    let connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert(iid, handle);

    let conns = Arc::clone(&connections);
    let cancel = CancellationToken::new();

    // Spawn the writer task
    let writer = tokio::spawn(connection_writer_task(
        socket_path.clone(),
        iid,
        rx,
        conns,
        cancel,
        Duration::from_secs(5),
        None,
    ));

    // Accept the connection, then immediately drop it + the listener
    let (stream, _) = listener.accept().await.unwrap();
    drop(stream);
    drop(listener);

    // Send a message — the writer should hit a broken-pipe error
    tx.send(SendTask {
        msg_type: MessageType::Message,
        header: Bytes::from_static(b"hdr"),
        payload: Bytes::from_static(b"pay"),
        on_error: Arc::new(NullErrorHandler),
    })
    .unwrap();

    // Wait for writer task to finish
    let _ = writer.await;

    // The writer should have removed the stale entry from the map
    assert!(
        !connections.contains_key(&iid),
        "writer task should clean up its DashMap entry on write error"
    );

    // Cleanup
    std::fs::remove_file(&socket_path).ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_send_message_does_not_fail_on_stale_handle() {
    let (transport, _socket_path) = make_transport();

    // Start a UDS listener that accepts connections (simulates a healthy peer)
    let dir = std::env::temp_dir().join(format!("uds-peer-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let peer_socket = dir.join("peer.sock");
    let peer_listener = tokio::net::UnixListener::bind(&peer_socket).unwrap();

    let peer = make_uds_peer(&peer_socket);
    let iid = peer.instance_id();
    transport.register(peer).unwrap();

    // Insert a stale handle
    insert_stale_handle(&transport, iid);

    // send_message should detect the stale handle and create a new one. This
    // exercises the slow path (get_or_create_connection, then the fresh
    // connection's gate) — the fresh channel is empty, so the frame admits
    // immediately rather than queueing.
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

    // Cleanup
    std::fs::remove_file(&peer_socket).ok();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_double_bind_returns_err() {
    use crate::transports::transport::make_channels;

    let dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("double-bind.sock");

    let transport1 = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();

    let instance_id = crate::InstanceId::new_v4();
    let (adapter1, _streams1) = make_channels();
    let rt = tokio::runtime::Handle::current();

    // First bind must succeed.
    transport1
        .start(instance_id, adapter1, rt.clone())
        .await
        .unwrap();

    // Second transport on the same path must fail.
    let transport2 = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();
    let (adapter2, _streams2) = make_channels();
    let result = transport2.start(instance_id, adapter2, rt).await;
    assert!(
        result.is_err(),
        "start() should return Err when a live listener already owns the socket"
    );

    // Cleanup
    transport1.shutdown();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_begin_drain_activates_draining_flag() {
    use crate::transports::transport::make_channels;

    let dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("drain-test.sock");

    let transport = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();

    let instance_id = crate::InstanceId::new_v4();
    let (adapter, _streams) = make_channels();
    let rt = tokio::runtime::Handle::current();

    transport.start(instance_id, adapter, rt).await.unwrap();

    assert!(
        !transport.shutdown_state.get().unwrap().is_draining(),
        "should not be draining before begin_drain()"
    );

    transport.begin_drain();

    assert!(
        transport.shutdown_state.get().unwrap().is_draining(),
        "should be draining after begin_drain()"
    );

    // Cleanup
    transport.shutdown();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_writer_task_drains_on_connect_failure() {
    // Use a socket path where nothing is listening so connect will fail.
    let dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let dead_socket = dir.join("dead.sock");

    let iid = crate::InstanceId::new_v4();
    let (handle, rx) = make_handle(8);
    let tx = handle.tx.clone();

    let connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>> = Arc::new(DashMap::new());
    connections.insert(iid, handle);

    // Queue a message before the writer task starts
    let error_handler = Arc::new(TrackingErrorHandler::new());
    tx.send(SendTask {
        msg_type: MessageType::Message,
        header: Bytes::from_static(b"hdr"),
        payload: Bytes::from_static(b"pay"),
        on_error: error_handler.clone(),
    })
    .unwrap();

    let conns = Arc::clone(&connections);
    let cancel = CancellationToken::new();

    let writer = tokio::spawn(connection_writer_task(
        dead_socket,
        iid,
        rx,
        conns,
        cancel,
        Duration::from_secs(5),
        None,
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

    // Cleanup
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_register_rejects_missing_path() {
    let dir = std::env::temp_dir().join(format!("uds-reject-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("self.sock");
    let transport = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();

    // Peer path does not exist at all.
    let missing =
        std::env::temp_dir().join(format!("uds-missing-{}.sock", crate::InstanceId::new_v4()));
    assert!(!missing.exists());
    let peer = make_uds_peer(&missing);
    let peer_id = peer.instance_id();

    let result = transport.register(peer);
    assert!(matches!(result, Err(TransportError::NoEndpoint)));
    assert!(!transport.peers.contains_key(&peer_id));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_register_rejects_non_socket_file() {
    let dir = std::env::temp_dir().join(format!("uds-nonsock-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("self.sock");
    let transport = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();

    // Create a regular file at the peer path.
    let regular_file = dir.join("not-a-socket");
    std::fs::write(&regular_file, b"I am not a socket").unwrap();

    let peer = make_uds_peer(&regular_file);
    let peer_id = peer.instance_id();

    let result = transport.register(peer);
    assert!(matches!(result, Err(TransportError::NoEndpoint)));
    assert!(!transport.peers.contains_key(&peer_id));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn test_register_accepts_bound_socket() {
    let dir = std::env::temp_dir().join(format!("uds-accept-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket_path = dir.join("self.sock");
    let transport = UdsTransportBuilder::new()
        .socket_path(&socket_path)
        .build()
        .unwrap();

    let peer_socket = dir.join("peer.sock");
    let _peer_listener = tokio::net::UnixListener::bind(&peer_socket).unwrap();

    let peer = make_uds_peer(&peer_socket);
    let peer_id = peer.instance_id();

    transport.register(peer).expect("register should succeed");
    assert!(transport.peers.contains_key(&peer_id));

    std::fs::remove_dir_all(&dir).ok();
}

/// Replacing a stale connection must kill the old epoch's queued frames.
///
/// Mirror of the TCP test: the gate is per connection, so a frame queued behind
/// a connection that has since died must never be handed to its successor.
#[tokio::test]
async fn stale_replacement_fails_the_old_epoch_and_admits_on_the_successor() {
    let (transport, _socket_path) = make_transport();

    let peer_dir = std::env::temp_dir().join(format!("uds-test-{}", crate::InstanceId::new_v4()));
    std::fs::create_dir_all(&peer_dir).unwrap();
    let peer_path = peer_dir.join("peer.sock");
    let _peer_listener = tokio::net::UnixListener::bind(&peer_path).unwrap();
    let peer = make_uds_peer(&peer_path);
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

/// UDS reports the same ceiling as TCP because it is the same codec, and the
/// same pin applies: exactly the reported capacity encodes, one byte more does
/// not. A Unix socket adds no message limit of its own.
#[tokio::test]
async fn max_message_size_is_exactly_what_the_codec_will_encode() {
    let (transport, _socket_path) = make_transport();

    let capacity = transport
        .max_message_size(crate::InstanceId::new_v4())
        .expect("UDS always knows its framed limit");
    assert_eq!(capacity, 16 * 1024 * 1024);

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
