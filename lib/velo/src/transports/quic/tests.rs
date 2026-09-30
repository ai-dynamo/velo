// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use velo_ext::{
    DataStreams, InstanceId, MessageType, PeerInfo, Transport, TransportErrorHandler, make_channels,
};

use super::{ConnectionHandle, QuicEndpointInfo, QuicTransport, QuicTransportBuilder};
use crate::transports::address::WorkerAddressBuilder;

#[derive(Default)]
struct Errors(Mutex<Vec<String>>);

impl TransportErrorHandler for Errors {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        self.0.lock().unwrap().push(error);
    }
}

/// Log lines captured for one test, through `tracing::subscriber::set_default`.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn contains(&self, needle: &str) -> bool {
        String::from_utf8_lossy(&self.0.lock().unwrap()).contains(needle)
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn started() -> (QuicTransport, DataStreams, InstanceId) {
    let transport = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, streams) = make_channels();
    let id = InstanceId::new_v4();
    transport
        .start(id, adapter, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (transport, streams, id)
}

/// `target`'s address, but naming `fingerprint` as its certificate.
fn peer_with_fingerprint(
    target: &QuicTransport,
    id: InstanceId,
    fingerprint: super::tls::Fingerprint,
) -> PeerInfo {
    let key = target.key();
    let raw = target.address().get_entry(&key).unwrap().unwrap();
    let mut info = QuicEndpointInfo::decode(&raw).unwrap();
    info.fingerprint = fingerprint;
    let mut builder = WorkerAddressBuilder::new();
    builder.add_entry(key, info.encode().unwrap()).unwrap();
    PeerInfo::new(id, builder.build().unwrap())
}

/// A dialer accepts only the certificate that the peer's address names. A
/// listener with another certificate on that address fails the handshake,
/// and the frame goes to the error handler instead of the listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_another_certificate_is_refused() {
    let (client, _client_streams, _) = started().await;
    let (server, server_streams, server_id) = started().await;
    let (stranger, _stranger_streams, _) = started().await;

    // Control: with the right fingerprint, the frame arrives.
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();
    let errors = Arc::new(Errors::default());
    let _ = client.send_message(
        server_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"trusted"),
        MessageType::Event,
        errors.clone(),
    );
    let (_, payload) = tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("a trusted peer receives the frame")
    .unwrap();
    assert_eq!(&payload[..], b"trusted");

    // The same address, pinned to another certificate.
    let impostor_id = InstanceId::new_v4();
    client
        .register(peer_with_fingerprint(
            &server,
            impostor_id,
            stranger.fingerprint(),
        ))
        .unwrap();
    let _ = client.send_message(
        impostor_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"untrusted"),
        MessageType::Event,
        errors.clone(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while errors.0.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the refused frame reaches the error handler");
    assert!(
        server_streams.event_stream.try_recv().is_err(),
        "nothing reaches a listener whose certificate is not the pinned one"
    );

    for t in [&client, &server, &stranger] {
        t.shutdown();
    }
}

/// Many small frames on one stream all arrive, in order. This load shape
/// hit "too many gaps in stream buffer" in quinn-proto 0.11.17 in Dynamo's
/// QUIC response plane; 0.11.18 is the floor in `Cargo.toml`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_of_small_frames_arrives_in_order() {
    const FRAMES: u32 = 50_000;
    let (client, _client_streams, _) = started().await;
    let (server, server_streams, server_id) = started().await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();

    let errors = Arc::new(Errors::default());
    let sender = tokio::spawn({
        let errors = errors.clone();
        async move {
            for i in 0..FRAMES {
                let outcome = client.send_message(
                    server_id,
                    Bytes::from(i.to_le_bytes().to_vec()),
                    Bytes::from_static(b"token"),
                    MessageType::Event,
                    errors.clone(),
                );
                if let velo_ext::SendOutcome::Pending(admission) = outcome {
                    admission.await.unwrap();
                }
            }
            client
        }
    });

    let received = tokio::time::timeout(Duration::from_secs(60), async {
        for expected in 0..FRAMES {
            let (header, _) = server_streams.event_stream.recv_async().await.unwrap();
            let got = u32::from_le_bytes(header[..4].try_into().unwrap());
            assert_eq!(got, expected, "frames arrive in send order");
        }
    })
    .await;
    received.expect("every frame arrives");
    assert!(errors.0.lock().unwrap().is_empty());

    let client = sender.await.unwrap();
    client.shutdown();
    server.shutdown();
}

async fn started_with(builder: QuicTransportBuilder) -> (QuicTransport, DataStreams, InstanceId) {
    let transport = builder
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, streams) = make_channels();
    let id = InstanceId::new_v4();
    transport
        .start(id, adapter, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (transport, streams, id)
}

async fn large_frames_round_trip(max_mtu: Option<u16>) {
    let configure = |b: QuicTransportBuilder| match max_mtu {
        Some(m) => b.max_mtu(m),
        None => b,
    };
    let (a, a_streams, a_id) = started_with(configure(QuicTransportBuilder::new())).await;
    let (b, b_streams, b_id) = started_with(configure(QuicTransportBuilder::new())).await;
    a.register(peer_with_fingerprint(&b, b_id, b.fingerprint()))
        .unwrap();
    b.register(peer_with_fingerprint(&a, a_id, a.fingerprint()))
        .unwrap();
    let errors = Arc::new(Errors::default());
    let payload = Bytes::from(vec![7u8; 64 * 1024]);
    let started_at = std::time::Instant::now();
    for i in 0..200u32 {
        let _ = a.send_message(
            b_id,
            Bytes::from(i.to_le_bytes().to_vec()),
            payload.clone(),
            MessageType::Event,
            errors.clone(),
        );
        let (h, p) =
            tokio::time::timeout(Duration::from_secs(5), b_streams.event_stream.recv_async())
                .await
                .unwrap_or_else(|_| panic!("request {i} stalled (max_mtu {max_mtu:?})"))
                .unwrap();
        let _ = b.send_message(a_id, h, p, MessageType::Response, errors.clone());
        tokio::time::timeout(
            Duration::from_secs(5),
            a_streams.response_stream.recv_async(),
        )
        .await
        .unwrap_or_else(|_| panic!("response {i} stalled (max_mtu {max_mtu:?})"))
        .unwrap();
    }
    // Each round trip takes well under a millisecond on loopback. A lost
    // send batch costs a loss-probe timeout per flight, tens of ms each, and
    // 200 round trips then take seconds.
    let elapsed = started_at.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "200 round trips of 64 KiB took {elapsed:?} (max_mtu {max_mtu:?}); \
         a send batch above the UDP datagram limit is lost on every flight"
    );
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_frames_round_trip_at_the_default_mtu() {
    large_frames_round_trip(None).await;
}

/// A jumbo-frame MTU (8952) is lowered to `GSO_SAFE_MAX_MTU`. Unclamped, a
/// 10-packet send batch exceeds the UDP datagram limit, the kernel refuses
/// it, and quinn-udp reports success, so every flight is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_frames_round_trip_at_a_jumbo_mtu() {
    large_frames_round_trip(Some(8952)).await;
}

#[test]
fn a_max_mtu_above_the_gso_batch_limit_is_lowered() {
    use super::builder::GSO_SAFE_MAX_MTU;
    assert_eq!(GSO_SAFE_MAX_MTU, 6550);
    assert_eq!(super::builder::clamp_to_gso_batch(8952), 6550);
    assert_eq!(super::builder::clamp_to_gso_batch(1452), 1452);
}

/// Records rejections, for the one test that needs to count them.
#[derive(Default)]
struct Rejections(Mutex<Vec<velo_ext::TransportRejection>>);

impl velo_ext::TransportObservability for Rejections {
    fn record_frame(&self, _: velo_ext::Direction, _: &str, _: usize) {}
    fn record_rejection(&self, reason: velo_ext::TransportRejection) {
        self.0.lock().unwrap().push(reason);
    }
    fn set_registered_peers(&self, _: usize) {}
    fn set_active_connections(&self, _: usize) {}
    fn record_send_backpressure(&self) {}
}

impl Rejections {
    fn decode_errors(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|r| **r == velo_ext::TransportRejection::DecodeError)
            .count()
    }
}

/// A started transport whose observability handle is in place before
/// `start`, which is when the accept loops take it.
async fn started_observed(observed: Arc<Rejections>) -> (QuicTransport, DataStreams, InstanceId) {
    let transport = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    transport.set_observability(observed);
    let (adapter, streams) = make_channels();
    let id = InstanceId::new_v4();
    transport
        .start(id, adapter, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (transport, streams, id)
}

/// A server that shuts down ends its connections in the ordinary way; it is
/// not a malformed frame, and no frame was lost. The dialer must neither count
/// it as `DecodeError` nor warn of lost frames. `DecodeError`
/// on TCP means a frame failed to parse. Without this a single restart bumps
/// the counter, and logs a warning, on every peer, and both stop meaning
/// anything.
///
/// The server shuts down first, while the dialer's reader is still live, so
/// the reader sees the close. The other order proves nothing: the dialer's own
/// shutdown stops its reader before the server's close arrives.
///
/// The runtime is `current_thread`, so the log subscriber, which is set for
/// this thread only, sees the writer's end.
#[tokio::test]
async fn a_dialer_does_not_count_the_servers_shutdown_as_a_decode_error() {
    let client_seen = Arc::new(Rejections::default());
    let (client, _client_streams, _) = started_observed(client_seen.clone()).await;
    let (server, server_streams, server_id) = started().await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();

    let errors = Arc::new(Errors::default());
    let _ = client.send_message(
        server_id,
        Bytes::from_static(b"hdr"),
        Bytes::from_static(b"pay"),
        MessageType::Event,
        errors.clone(),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("the frame arrives")
    .unwrap();

    let logs = CapturedLogs::default();
    let _logging = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish(),
    );
    server.shutdown();
    server.closed().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        client_seen.decode_errors(),
        0,
        "the dialer counted the server's shutdown as a decode error"
    );
    assert!(
        client.connections.is_empty(),
        "control: the dialer's writer never saw the close, so the log check proves nothing"
    );
    assert!(
        !logs.contains("can be lost"),
        "the dialer warned of lost frames on an orderly peer shutdown"
    );
    assert!(errors.0.lock().unwrap().is_empty());
    client.shutdown();
}

/// The accept side of the same rule. The peer is a raw quinn client that sends
/// one whole frame and then closes the connection without finishing its
/// stream, so the listener's read ends on the close rather than on a clean
/// end of stream. That is an orderly end, not a decode error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_does_not_count_a_peer_closing_as_a_decode_error() {
    let server_seen = Arc::new(Rejections::default());
    let (server, server_streams, _) = started_observed(server_seen.clone()).await;
    let raw = server.address().get_entry(server.key()).unwrap().unwrap();
    let addr = QuicEndpointInfo::decode(&raw).unwrap().endpoints[0]
        .socket_addr()
        .unwrap();

    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint
        .set_default_client_config(super::tls::pinned_client_config(server.fingerprint()).unwrap());
    let connection = endpoint
        .connect(addr, super::tls::SERVER_NAME)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = connection.open_bi().await.unwrap();
    let mut frame = Vec::new();
    crate::transports::tcp::TcpFrameCodec::encode_frame_sync(
        &mut frame,
        MessageType::Event,
        b"hdr",
        b"pay",
    )
    .unwrap();
    send.write_all(&frame).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("the frame arrives")
    .unwrap();

    connection.close(quinn::VarInt::from_u32(0), b"bye");
    endpoint.wait_idle().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        server_seen.decode_errors(),
        0,
        "the listener counted the peer's close as a decode error"
    );
    server.shutdown();
}

/// Replacing a dead connection must not update the connection gauge while the
/// map entry is held. The gauge reads `len()`, which read-locks every shard,
/// and the shard that the entry holds for writing is not reentrant: the
/// thread would wait on itself, and every later operation on that shard
/// behind it. The dead entry is seeded directly because in normal use it only
/// appears in a race between `reap_stale_connection` and `entry()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacing_a_dead_connection_does_not_deadlock() {
    // Observed, so the gauge update runs.
    let (client, _client_streams, _) = started_observed(Arc::new(Rejections::default())).await;
    let (server, _server_streams, server_id) = started().await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();

    let rt = tokio::runtime::Handle::current();
    let (tx, rx) = flume::bounded(1);
    drop(rx);
    client.connections.insert(
        (server_id, 0),
        ConnectionHandle {
            gate: crate::transports::transport::AdmissionGate::new(tx.clone(), rt.clone()),
            tx,
        },
    );

    let client = Arc::new(client);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn({
        let client = client.clone();
        move || {
            let installed = client.install_connection((server_id, 0), &rt).is_ok();
            let _ = done_tx.send(installed);
        }
    });
    let installed = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("install_connection deadlocked replacing a dead connection");
    assert!(installed);
    assert!(
        !client
            .connections
            .get(&(server_id, 0))
            .unwrap()
            .tx
            .is_disconnected()
    );
    client.shutdown();
    server.shutdown();
}

/// A process that exits right after graceful shutdown must not discard the
/// frames its QUIC writers already wrote. TCP gets this for free: the kernel
/// delivers the tail after the process exits. On QUIC the tail is in user
/// space, so `closed()` must finish it before the runtime goes away. That the
/// connection is also closed on the wire is
/// `the_senders_exit_closes_its_connection`.
///
/// The sender runs on its own runtime, which is dropped right after
/// `shutdown()` and `closed()`.
#[test]
fn frames_survive_the_runtime_ending_right_after_close() {
    const FRAMES: usize = 256;
    const PAYLOAD: usize = 64 * 1024;
    let rx_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (server, server_streams, server_id) = rx_rt.block_on(started());
    let server_peer = peer_with_fingerprint(&server, server_id, server.fingerprint());

    let errors = Arc::new(Errors::default());
    let tx_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let first = tx_rt.block_on(async {
        let (client, _client_streams, _) = started().await;
        client.register(server_peer).unwrap();
        for i in 0..FRAMES {
            let _ = client.send_message(
                server_id,
                Bytes::from((i as u32).to_be_bytes().to_vec()),
                Bytes::from(vec![0u8; PAYLOAD]),
                MessageType::Response,
                errors.clone(),
            );
        }
        // Close once frames are flowing, so it lands mid-stream.
        let first = tokio::time::timeout(
            Duration::from_secs(5),
            server_streams.response_stream.recv_async(),
        )
        .await
        .expect("the first frame arrives")
        .is_ok();
        client.shutdown();
        client.closed().await;
        first
    });
    // The process exits: nothing on this runtime runs again.
    drop(tx_rt);
    assert!(first);

    let rest = rx_rt.block_on(async {
        let mut rest = 0;
        while let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_secs(3),
            server_streams.response_stream.recv_async(),
        )
        .await
        {
            rest += 1;
        }
        rest
    });
    let delivered = 1 + rest;
    let failed = errors.0.lock().unwrap().len();
    assert_eq!(
        delivered + failed,
        FRAMES,
        "{delivered} delivered + {failed} failed != {FRAMES} sent: frames vanished when the runtime ended"
    );
    rx_rt.block_on(async { server.shutdown() });
}

/// A raw quinn server with this transport's certificate and `window` as its
/// stream receive window, and the peer that names it. `serve` gets each
/// accepted connection.
fn raw_server<F, Fut>(window: u32, idle: Duration, serve: F) -> (PeerInfo, InstanceId)
where
    F: FnOnce(quinn::Connection) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let identity = super::tls::Identity::generate().unwrap();
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(
            super::tls::server_crypto(&identity).unwrap(),
        )
        .unwrap(),
    ));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.stream_receive_window(quinn::VarInt::from_u32(window));
    transport_config.max_idle_timeout(Some(idle.try_into().unwrap()));
    server_config.transport_config(Arc::new(transport_config));
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        serve(connection).await;
        drop(endpoint);
    });

    let info = QuicEndpointInfo {
        endpoints: crate::transports::utils::interfaces::resolve_advertise_endpoints(
            addr,
            &crate::transports::utils::interfaces::InterfaceFilter::All,
        )
        .unwrap(),
        fingerprint: identity.fingerprint,
        ports: vec![],
    };
    let mut builder = WorkerAddressBuilder::new();
    builder.add_entry("quic", info.encode().unwrap()).unwrap();
    let peer_id = InstanceId::new_v4();
    (PeerInfo::new(peer_id, builder.build().unwrap()), peer_id)
}

/// A sender that exits right after `closed()` closes its connection on the
/// wire; its peer sees an application close, not an idle timeout. The peer is
/// a raw quinn server, so the test reads the close reason directly.
#[test]
fn the_senders_exit_closes_its_connection() {
    let rx_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (reason_tx, reason_rx) = std::sync::mpsc::channel();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (peer, peer_id) = rx_rt.block_on(async {
        raw_server(
            1 << 20,
            Duration::from_millis(1500),
            move |connection| async move {
                let (_send, mut recv) = connection.accept_bi().await.unwrap();
                let _ = accepted_tx.send(());
                let _ = recv.read_to_end(usize::MAX).await;
                let _ = reason_tx.send(connection.closed().await);
            },
        )
    });

    let tx_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tx_rt.block_on(async {
        let (client, _client_streams, _) = started().await;
        client.register(peer).unwrap();
        let _ = client.send_message(
            peer_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Event,
            Arc::new(Errors::default()),
        );
        // The peer sees the stream only once the frame arrives, so the
        // connection is up before the shutdown under test.
        tokio::time::timeout(Duration::from_secs(5), accepted_rx)
            .await
            .expect("the frame never reached the peer")
            .unwrap();
        client.shutdown();
        client.closed().await;
        assert_eq!(
            client.client_endpoints.get().unwrap()[0].open_connections(),
            0,
            "closed() returned with a dialed connection still open"
        );
    });
    drop(tx_rt);

    let reason = reason_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the connection ends");
    assert!(
        matches!(reason, quinn::ConnectionError::ApplicationClosed(_)),
        "the sender exited without closing its connection: {reason:?}"
    );
}

/// Frames still waiting in the admission gate when the transport shuts down
/// are failed by the time `closed()` returns. The runtime reports a failed
/// admission to the sender's error handler, so a process that exits on
/// `closed()`'s return must not leave any pending.
///
/// A one-slot channel and a peer that never reads keep the frames in the
/// gate. The gate is failed by its own driver: the writer drops its receiver
/// when it ends, which wakes the driver. On a `current_thread` runtime that
/// wake is queued ahead of `closed()`'s next poll, so the driver resolves
/// every ticket before `closed()` returns. On a multi-threaded runtime the two
/// race, and nothing in `closed()` orders them.
#[tokio::test]
async fn closed_fails_frames_still_waiting_in_the_gate() {
    const FRAMES: usize = 32;
    let (peer, peer_id) = raw_server(
        64 * 1024,
        Duration::from_secs(30),
        |connection| async move {
            let _stream = connection.accept_bi().await.unwrap();
            std::future::pending::<()>().await;
        },
    );
    let client = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .channel_capacity(1)
        .build()
        .unwrap();
    let (adapter, _streams) = make_channels();
    client
        .start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
    client.register(peer).unwrap();

    let errors = Arc::new(Errors::default());
    let mut pending = Vec::new();
    for i in 0..FRAMES {
        if let velo_ext::SendOutcome::Pending(admission) = client.send_message(
            peer_id,
            Bytes::from((i as u32).to_be_bytes().to_vec()),
            Bytes::from(vec![0u8; 64 * 1024]),
            MessageType::Response,
            errors.clone(),
        ) {
            pending.push(admission);
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!pending.is_empty(), "the gate never queued a frame");
    client.shutdown();
    client.closed().await;
    let still = pending
        .iter()
        .filter(|a| a.state() == velo_ext::AdmissionState::Pending)
        .count();
    assert_eq!(
        still,
        0,
        "{still} of {} gated frames were still pending when closed() returned",
        pending.len()
    );
}

/// A node that only accepts connections must also close them on the wire
/// before `closed()` returns. Its CONNECTION_CLOSE leaves on the connection
/// driver's next poll; if the process exits first, each peer learns of the
/// close only at its idle timeout and counts that as a decode error.
///
/// The receiver runs on its own runtime and is dropped right after
/// `shutdown()` and `closed()`. The dialer has a short idle timeout, so a
/// missing close would time out within the test.
#[test]
fn an_accept_only_node_closes_its_connections_before_it_exits() {
    accept_only_node_closes_before_it_exits(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );
}

/// The same on a `current_thread` runtime, where nothing else runs while
/// `closed()` is being polled. If `closed()` is ready on its first poll, the
/// connection drivers never run again, and no CONNECTION_CLOSE leaves.
#[test]
fn an_accept_only_node_on_a_current_thread_runtime_closes_its_connections() {
    accept_only_node_closes_before_it_exits(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
}

fn accept_only_node_closes_before_it_exits(server_rt: tokio::runtime::Runtime) {
    let dialer_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let seen = Arc::new(Rejections::default());
    let client = dialer_rt.block_on(async {
        let client = QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .idle_timeout(Duration::from_millis(1500))
            .build()
            .unwrap();
        client.set_observability(seen.clone());
        let (adapter, _streams) = make_channels();
        client
            .start(
                InstanceId::new_v4(),
                adapter,
                tokio::runtime::Handle::current(),
            )
            .await
            .unwrap();
        client
    });

    server_rt.block_on(async {
        let (server, server_streams, server_id) = started().await;
        client
            .register(peer_with_fingerprint(
                &server,
                server_id,
                server.fingerprint(),
            ))
            .unwrap();
        let _ = client.send_message(
            server_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Event,
            Arc::new(Errors::default()),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            server_streams.event_stream.recv_async(),
        )
        .await
        .expect("the frame arrives")
        .unwrap();
        server.shutdown();
        server.closed().await;
        for endpoint in server.server_endpoints.get().into_iter().flatten() {
            assert_eq!(
                endpoint.open_connections(),
                0,
                "closed() returned with a server connection still open"
            );
        }
    });
    // The receiver's process exits.
    drop(server_rt);

    dialer_rt.block_on(async { tokio::time::sleep(Duration::from_secs(3)).await });
    assert_eq!(
        seen.decode_errors(),
        0,
        "the dialer timed out on a connection the receiver closed without telling it"
    );
    dialer_rt.block_on(async { client.shutdown() });
}

/// When `closed()` gives up on a peer that stopped reading, every frame must
/// be accounted for by the time it returns: failed through its error handler,
/// since none was delivered. A writer parked on the peer's flow-control
/// window fails only once the connection closes; a process that exits on
/// `closed()`'s return must not beat those failures.
///
/// The peer is a raw quinn server with this transport's certificate that
/// accepts the stream and never reads it, with a 64 KiB stream window. The
/// window is smaller than one frame, so no write ever completes and every
/// frame stays with the writer. With a larger window some frames would count
/// as written, never be read, and never be reported: that loss is the same as
/// a TCP peer that stops reading, and the writer only warns about it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_accounts_for_every_frame_when_the_peer_stops_reading() {
    const FRAMES: usize = 64;
    const PAYLOAD: usize = 64 * 1024;
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (peer, peer_id) = raw_server(
        64 * 1024,
        Duration::from_secs(30),
        move |connection| async move {
            let _stream = connection.accept_bi().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        },
    );
    let (client, _client_streams, _) = started().await;
    client.register(peer).unwrap();

    let errors = Arc::new(Errors::default());
    for i in 0..FRAMES {
        let _ = client.send_message(
            peer_id,
            Bytes::from((i as u32).to_be_bytes().to_vec()),
            Bytes::from(vec![0u8; PAYLOAD]),
            MessageType::Response,
            errors.clone(),
        );
    }
    // The peer sees the stream only once the writer's first bytes arrive, so
    // the handshake is done and the writer is writing its first frame into a
    // window smaller than that frame: it is parked, which is the case under
    // test. Without this, a slow handshake would let shutdown fail every
    // frame before any writer existed.
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("the writer never reached the peer")
        .unwrap();
    assert!(
        errors.0.lock().unwrap().is_empty(),
        "a frame failed before shutdown"
    );
    client.shutdown();
    client.closed().await;
    let failed = errors.0.lock().unwrap().len();
    assert_eq!(
        failed, FRAMES,
        "{failed} of {FRAMES} frames failed when closed() returned; the rest are unaccounted for"
    );
}

/// A writer whose connection ends for any reason but a peer close warns that
/// frames can be lost. Here the dialer closes by force at CLOSE_WAIT, because
/// the peer stopped reading: frames sit in the peer's window, written but not
/// read. Only a peer close is an orderly end; a forced close, an idle timeout
/// or a reset is not.
///
/// The runtime is `current_thread`, so the log subscriber, which is set for
/// this thread only, sees the writer's end.
#[tokio::test]
async fn a_forced_close_warns_of_frames_the_peer_did_not_read() {
    const FRAMES: usize = 64;
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (peer, peer_id) = raw_server(
        1 << 20,
        Duration::from_secs(30),
        move |connection| async move {
            let _stream = connection.accept_bi().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        },
    );
    let (client, _client_streams, _) = started().await;
    client.register(peer).unwrap();
    let errors = Arc::new(Errors::default());
    for i in 0..FRAMES {
        let _ = client.send_message(
            peer_id,
            Bytes::from((i as u32).to_be_bytes().to_vec()),
            Bytes::from(vec![0u8; 64 * 1024]),
            MessageType::Response,
            errors.clone(),
        );
    }
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("the writer never reached the peer")
        .unwrap();

    let logs = CapturedLogs::default();
    let _logging = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish(),
    );
    client.shutdown();
    client.closed().await;
    let failed = errors.0.lock().unwrap().len();
    assert!(
        failed < FRAMES,
        "control: no frame fit the peer's window, so none could be lost unreported"
    );
    assert!(
        logs.contains("can be lost"),
        "{} frames were written into a window the peer never read, and nothing warned",
        FRAMES - failed
    );
}

/// A listener that tears down ends its connections with an application close,
/// not by stopping the stream first. A STOP_SENDING that reached the dialer
/// before the close would read as a stream the peer stopped with frames
/// unread, and the dialer would warn once per connection on every peer
/// restart.
///
/// The listener does not call `close()` itself. Its connection task and its
/// stream task drop their handles at teardown, and quinn closes the
/// connection when the last one goes; a closed connection sends only
/// CONNECTION_CLOSE, which supersedes the STOP_SENDING the dropped stream
/// queued. The runtime is `current_thread`, so the driver runs only after both
/// tasks have dropped their handles, which makes that order deterministic.
///
/// Only the teardown token is cancelled, as `graceful_shutdown` does before it
/// calls `shutdown()`, so the order under test is the listener's own.
#[tokio::test]
async fn a_listener_in_teardown_closes_the_connection_before_its_streams() {
    let server = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, server_streams) = make_channels();
    let teardown = adapter.shutdown_state.teardown_token().clone();
    server
        .start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
    let raw = server.address().get_entry(server.key()).unwrap().unwrap();
    let addr = QuicEndpointInfo::decode(&raw).unwrap().endpoints[0]
        .socket_addr()
        .unwrap();

    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint
        .set_default_client_config(super::tls::pinned_client_config(server.fingerprint()).unwrap());
    let connection = endpoint
        .connect(addr, super::tls::SERVER_NAME)
        .unwrap()
        .await
        .unwrap();
    let (mut send, _recv) = connection.open_bi().await.unwrap();
    let mut frame = Vec::new();
    crate::transports::tcp::TcpFrameCodec::encode_frame_sync(
        &mut frame,
        MessageType::Event,
        b"hdr",
        b"pay",
    )
    .unwrap();
    send.write_all(&frame).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("the frame arrives")
    .unwrap();

    teardown.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(5), send.stopped())
        .await
        .expect("the listener neither stopped the stream nor closed the connection");
    assert!(
        matches!(
            outcome,
            Err(quinn::StoppedError::ConnectionLost(
                quinn::ConnectionError::ApplicationClosed(_)
            ))
        ),
        "the listener stopped the stream before it closed the connection: {outcome:?}"
    );
    server.shutdown();
}

/// A client with `lanes` lanes, registered with a fresh server.
async fn laned_pair(
    lanes: u16,
) -> (
    Arc<QuicTransport>,
    DataStreams,
    QuicTransport,
    DataStreams,
    InstanceId,
) {
    let (client, client_streams, _) = started_with(QuicTransportBuilder::new().lanes(lanes)).await;
    let (server, server_streams, server_id) = started().await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();
    (
        Arc::new(client),
        client_streams,
        server,
        server_streams,
        server_id,
    )
}

/// Each lane is its own connection from its own socket, and keeps its own
/// order under load from concurrent senders.
///
/// The header carries `(lane, seq)`. The receiver sees the lanes interleaved,
/// which is allowed, and each lane's sequence in order, which is the contract.
/// A lane that shared a connection with another would still pass the order
/// check, so the test also counts connections per dial endpoint: one lane, one
/// socket, one connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_lane_is_its_own_connection_and_keeps_its_own_order() {
    const LANES: u16 = 4;
    const FRAMES: u32 = 10_000;
    let (client, _client_streams, server, server_streams, server_id) = laned_pair(LANES).await;
    assert_eq!(client.lanes(server_id).get(), LANES);

    let errors = Arc::new(Errors::default());
    let mut senders = Vec::new();
    for lane in 0..LANES {
        let client = client.clone();
        let errors = errors.clone();
        senders.push(tokio::spawn(async move {
            for seq in 0..FRAMES {
                let mut header = lane.to_le_bytes().to_vec();
                header.extend_from_slice(&seq.to_le_bytes());
                let outcome = client.send_message_on_lane(
                    server_id,
                    lane,
                    Bytes::from(header),
                    Bytes::from_static(b"token"),
                    MessageType::Event,
                    errors.clone(),
                );
                if let velo_ext::SendOutcome::Pending(admission) = outcome {
                    admission.await.unwrap();
                }
            }
        }));
    }

    let mut next = vec![0u32; usize::from(LANES)];
    tokio::time::timeout(Duration::from_secs(60), async {
        for _ in 0..FRAMES * u32::from(LANES) {
            let (header, _) = server_streams.event_stream.recv_async().await.unwrap();
            let lane = u16::from_le_bytes(header[..2].try_into().unwrap());
            let seq = u32::from_le_bytes(header[2..6].try_into().unwrap());
            assert_eq!(seq, next[usize::from(lane)], "lane {lane} out of order");
            next[usize::from(lane)] += 1;
        }
    })
    .await
    .expect("every frame arrives");
    for sender in senders {
        sender.await.unwrap();
    }
    assert!(errors.0.lock().unwrap().is_empty());

    for lane in 0..LANES {
        assert!(client.connections.contains_key(&(server_id, lane)));
    }
    let endpoints = client.client_endpoints.get().unwrap();
    assert_eq!(endpoints.len(), usize::from(LANES));
    for (lane, endpoint) in endpoints.iter().enumerate() {
        assert_eq!(
            endpoint.open_connections(),
            1,
            "lane {lane} dials from its own socket"
        );
    }
    client.shutdown();
    server.shutdown();
}

/// `send_message` is lane 0, and a lane at or past the count wraps.
///
/// Ordinary traffic must keep the one ordered channel it had before lanes, so
/// a transport built with several lanes must not open a second connection for
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_message_is_lane_zero_and_lanes_wrap() {
    let (client, _client_streams, server, server_streams, server_id) = laned_pair(3).await;
    let errors = Arc::new(Errors::default());

    // Several sends, so a send_message that rotated over lanes would open a
    // second connection.
    for _ in 0..3 {
        let _ = client.send_message(
            server_id,
            Bytes::from_static(b"plain"),
            Bytes::new(),
            MessageType::Event,
            errors.clone(),
        );
    }
    for _ in 0..3 {
        let frame = tokio::time::timeout(
            Duration::from_secs(5),
            server_streams.event_stream.recv_async(),
        )
        .await
        .expect("the frame arrives")
        .unwrap();
        assert_eq!(frame.0, Bytes::from_static(b"plain"));
    }
    assert_eq!(client.connections.len(), 1);
    assert!(client.connections.contains_key(&(server_id, 0)));

    // Lane 4 of 3: modulo gives lane 1, where a clamp would give lane 2.
    let _ = client.send_message_on_lane(
        server_id,
        4,
        Bytes::from_static(b"wrapped"),
        Bytes::new(),
        MessageType::Event,
        errors.clone(),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("the frame arrives")
    .unwrap();
    assert!(
        client.connections.contains_key(&(server_id, 1)),
        "lane 4 of 3 is lane 1"
    );
    assert_eq!(client.connections.len(), 2);
    assert!(errors.0.lock().unwrap().is_empty());
    client.shutdown();
    server.shutdown();
}

/// `closed()` covers every lane: after it returns, no dial endpoint has a
/// connection open, and every frame sent on any lane was delivered or failed.
///
/// Every lane is connected before the close, and bulk frames are flowing on
/// lanes 1 and up when it lands, so the close meets writers that are mid-stream
/// on endpoints other than lane 0's. Each send is admitted at once (the bulk
/// fits the send channel), so a frame is either on the wire or reported through
/// `on_error`; none can be dropped in the gate.
///
/// This pins the state after `closed()` returns; the force close of a stuck
/// lane other than 0 is what `closed_force_closes_a_stuck_lane_other_than_zero`
/// covers. The endpoint check needs every connection to finish draining within
/// `CLOSE_WAIT`, 2 s.
///
/// The accounting also needs the peer to acknowledge what the writers wrote
/// within `FINISH_GRACE`, 1 s. Past that the writer warns that the stream end
/// was not acknowledged and closes, and those frames are neither delivered nor
/// failed. On loopback that is milliseconds of work, so a failure here next to
/// that warning points at the grace, not at lanes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_waits_for_every_lane() {
    const LANES: u16 = 4;
    const FRAMES: usize = 256;
    const PAYLOAD: usize = 64 * 1024;
    let (client, _client_streams, server, server_streams, server_id) = laned_pair(LANES).await;
    let errors = Arc::new(Errors::default());

    let send = |lane: u16, seq: usize, bytes: usize| {
        let mut header = lane.to_be_bytes().to_vec();
        header.extend_from_slice(&(seq as u32).to_be_bytes());
        let outcome = client.send_message_on_lane(
            server_id,
            lane,
            Bytes::from(header),
            Bytes::from(vec![0u8; bytes]),
            MessageType::Response,
            errors.clone(),
        );
        assert!(outcome.is_admitted(), "the bulk must fit the send channel");
    };
    for lane in 0..LANES {
        send(lane, 0, 0);
    }
    for _ in 0..LANES {
        tokio::time::timeout(
            Duration::from_secs(5),
            server_streams.response_stream.recv_async(),
        )
        .await
        .expect("every lane connects")
        .unwrap();
    }

    for i in 0..FRAMES {
        send(1 + (i % usize::from(LANES - 1)) as u16, i + 1, PAYLOAD);
    }
    // Close only once every bulk lane has delivered a frame, so the close
    // lands on writers that are streaming, not on writers yet to start.
    let mut per_lane = vec![0usize; usize::from(LANES)];
    let mut delivered = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        while per_lane[1..].contains(&0) {
            let (header, _) = server_streams.response_stream.recv_async().await.unwrap();
            per_lane[usize::from(u16::from_be_bytes([header[0], header[1]]))] += 1;
            delivered += 1;
        }
    })
    .await
    .expect("every bulk lane delivers");
    client.shutdown();
    client.closed().await;
    for (lane, endpoint) in client.client_endpoints.get().unwrap().iter().enumerate() {
        assert_eq!(
            endpoint.open_connections(),
            0,
            "closed() returned with lane {lane}'s connection still open"
        );
    }

    while let Ok(Ok((header, _))) = tokio::time::timeout(
        Duration::from_secs(3),
        server_streams.response_stream.recv_async(),
    )
    .await
    {
        per_lane[usize::from(u16::from_be_bytes([header[0], header[1]]))] += 1;
        delivered += 1;
    }
    let failed = errors.0.lock().unwrap().len();
    assert_eq!(
        delivered + failed,
        FRAMES,
        "{delivered} delivered + {failed} failed != {FRAMES} sent"
    );
    server.shutdown();
}

/// A peer reached only on a lane other than 0 is healthy.
///
/// The mux will put streams on any lane, so a peer can have live connections
/// with none on lane 0. A health check that looked at lane 0 alone would dial a
/// throwaway probe and call such a peer `NeverConnected`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_live_only_on_a_later_lane_is_healthy() {
    let (client, _client_streams, server, server_streams, server_id) = laned_pair(3).await;
    let errors = Arc::new(Errors::default());
    let _ = client.send_message_on_lane(
        server_id,
        2,
        Bytes::from_static(b"lane two"),
        Bytes::new(),
        MessageType::Event,
        errors.clone(),
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("the frame arrives")
    .unwrap();
    assert!(!client.connections.contains_key(&(server_id, 0)));

    let health = client.check_health(server_id, Duration::from_secs(1)).await;
    assert!(health.is_ok(), "{health:?}");
    client.shutdown();
    server.shutdown();
}

/// `closed()` force-closes a lane other than 0 whose peer stopped reading, and
/// every frame on it is accounted for.
///
/// The laned form of `closed_accounts_for_every_frame_when_the_peer_stops_reading`.
/// Its peer never reads, so the writer on lane 2 cannot finish its stream, and
/// only a force close of its dial endpoint ends it. `shutdown()` schedules that
/// close for every lane's endpoint, and `closed()` does it again after
/// `CLOSE_WAIT`. If neither reached lane 2's endpoint, its writer would still be
/// blocked, and its frames unreported, when `closed()` returns.
///
/// The test does not require the connection to be gone from its endpoint. A
/// force-closed connection stays counted while it drains, on lane 0 too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_force_closes_a_stuck_lane_other_than_zero() {
    let lane: u16 = 2;
    const FRAMES: usize = 64;
    const PAYLOAD: usize = 64 * 1024;
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (peer, peer_id) = raw_server(
        64 * 1024,
        Duration::from_secs(30),
        move |connection| async move {
            let _stream = connection.accept_bi().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        },
    );
    let (client, _client_streams, _) = started_with(QuicTransportBuilder::new().lanes(3)).await;
    client.register(peer).unwrap();

    let errors = Arc::new(Errors::default());
    for i in 0..FRAMES {
        let _ = client.send_message_on_lane(
            peer_id,
            lane,
            Bytes::from((i as u32).to_be_bytes().to_vec()),
            Bytes::from(vec![0u8; PAYLOAD]),
            MessageType::Response,
            errors.clone(),
        );
    }
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("the writer never reached the peer")
        .unwrap();
    assert!(client.connections.contains_key(&(peer_id, lane)));
    assert_eq!(client.connections.len(), 1, "only lane {lane} was dialed");
    // Otherwise every frame could have failed for another reason, and the
    // force close under test would never have run.
    assert!(
        errors.0.lock().unwrap().is_empty(),
        "a frame failed before shutdown"
    );

    client.shutdown();
    client.closed().await;
    let failed = errors.0.lock().unwrap().len();
    assert_eq!(
        failed, FRAMES,
        "{failed} of {FRAMES} frames failed when closed() returned; the rest are unaccounted for"
    );
}

/// Dial `lanes` lanes into a server with `sockets` server sockets, one frame
/// per lane, and return how many connections each server socket holds.
async fn connections_per_server_socket(lanes: u16, sockets: usize) -> Vec<usize> {
    let (client, _client_streams, _) = started_with(QuicTransportBuilder::new().lanes(lanes)).await;
    let (server, server_streams, server_id) =
        started_with(QuicTransportBuilder::new().server_endpoints(sockets)).await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();
    let errors = Arc::new(Errors::default());
    for lane in 0..lanes {
        let _ = client.send_message_on_lane(
            server_id,
            lane,
            Bytes::from(lane.to_le_bytes().to_vec()),
            Bytes::new(),
            MessageType::Event,
            errors.clone(),
        );
    }
    for _ in 0..lanes {
        tokio::time::timeout(
            Duration::from_secs(5),
            server_streams.event_stream.recv_async(),
        )
        .await
        .expect("every lane delivers")
        .unwrap();
    }
    assert!(errors.0.lock().unwrap().is_empty());
    let counts = server
        .server_endpoints
        .get()
        .unwrap()
        .iter()
        .map(|endpoint| endpoint.open_connections())
        .collect();
    client.shutdown();
    server.shutdown();
    counts
}

/// The lanes of one dialer land on different server sockets of the peer.
///
/// Each server socket has its own port and its own endpoint driver, and the
/// dialer spreads its lanes over the ports. Two lanes on one socket would
/// share one driver, which is the limit lanes exist to lift.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_lane_lands_on_its_own_server_socket() {
    assert_eq!(connections_per_server_socket(4, 4).await, vec![1, 1, 1, 1]);
}

/// With more lanes than server sockets, the lanes wrap and spread evenly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_lanes_than_sockets_spread_evenly() {
    assert_eq!(connections_per_server_socket(4, 2).await, vec![2, 2]);
}

/// A lane's address: socket `(offset + lane) % n` of the peer, or the one
/// advertised address for a peer that lists no ports.
#[test]
fn lane_addresses_spread_over_the_peers_sockets() {
    let client_config = super::tls::pinned_client_config([0; 32]).unwrap();
    let addr: std::net::SocketAddr = "10.0.0.1:5000".parse().unwrap();
    let peer = super::PeerEntry {
        addr,
        ports: vec![5000, 5001, 5002, 5003],
        client_config: client_config.clone(),
    };
    let ports: Vec<u16> = (0..4).map(|lane| peer.lane_addr(lane, 6).port()).collect();
    assert_eq!(ports, vec![5002, 5003, 5000, 5001]);
    assert!((0..4).all(|lane| peer.lane_addr(lane, 6).ip() == addr.ip()));

    let before_lanes = super::PeerEntry {
        addr,
        ports: vec![],
        client_config,
    };
    assert!((0..4).all(|lane| before_lanes.lane_addr(lane, 6) == addr));
}
