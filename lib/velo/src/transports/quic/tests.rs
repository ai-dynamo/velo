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
    use super::GSO_SAFE_MAX_MTU;
    assert_eq!(GSO_SAFE_MAX_MTU, 6550);
    assert_eq!(super::clamp_to_gso_batch(8952), 6550);
    assert_eq!(super::clamp_to_gso_batch(1452), 1452);
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

/// A peer that shuts down ends its connection in the ordinary way; it is not
/// a malformed frame. Neither side may count it as `DecodeError`, which on
/// TCP means a frame failed to parse. Without this a single restart bumps the
/// counter on every peer, and the counter stops meaning anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_shutting_down_is_not_a_decode_error() {
    let client_seen = Arc::new(Rejections::default());
    let server_seen = Arc::new(Rejections::default());
    let (client, _client_streams, _) = started_observed(client_seen.clone()).await;
    let (server, server_streams, server_id) = started_observed(server_seen.clone()).await;
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

    // The server's accepted stream, and then the client's dialed reader, each
    // see the other end go away.
    client.shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;
    server.shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        server_seen.decode_errors(),
        0,
        "the server counted the client's shutdown as a decode error"
    );
    assert_eq!(
        client_seen.decode_errors(),
        0,
        "the client counted the server's shutdown as a decode error"
    );
    assert!(errors.0.lock().unwrap().is_empty());
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
        server_id,
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
            let installed = client.install_connection(server_id, &rt).is_ok();
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
            .get(&server_id)
            .unwrap()
            .tx
            .is_disconnected()
    );
    client.shutdown();
    server.shutdown();
}

/// A process that exits right after graceful shutdown must not discard the
/// frames its QUIC writers already wrote, and must close its connections so
/// its peers see an orderly end. TCP gets both for free: the kernel delivers
/// the tail and sends FIN after the process exits. On QUIC both are in user
/// space, so `closed()` must finish them before the runtime goes away.
///
/// The sender runs on its own runtime, which is dropped right after
/// `shutdown()` and `closed()`. The receiver has a short idle timeout, so a
/// connection that died without closing times out within the test and would
/// be counted as a decode error.
#[test]
fn frames_survive_the_runtime_ending_right_after_close() {
    const FRAMES: usize = 256;
    const PAYLOAD: usize = 64 * 1024;
    let rx_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let seen = Arc::new(Rejections::default());
    let (server, server_streams, server_id) = rx_rt.block_on(async {
        let server = QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .idle_timeout(Duration::from_millis(1500))
            .build()
            .unwrap();
        server.set_observability(seen.clone());
        let (adapter, streams) = make_channels();
        let id = InstanceId::new_v4();
        server
            .start(id, adapter, tokio::runtime::Handle::current())
            .await
            .unwrap();
        (server, streams, id)
    });
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
    assert_eq!(
        seen.decode_errors(),
        0,
        "the receiver counted the sender's exit as a decode error: its connection was not closed"
    );
    rx_rt.block_on(async { server.shutdown() });
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

    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
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
/// accepts the stream and never reads it, with a 64 KiB stream window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_accounts_for_every_frame_when_the_peer_stops_reading() {
    const FRAMES: usize = 64;
    const PAYLOAD: usize = 64 * 1024;
    let identity = super::tls::Identity::generate().unwrap();
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(
            super::tls::server_crypto(&identity).unwrap(),
        )
        .unwrap(),
    ));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.stream_receive_window(quinn::VarInt::from_u32(64 * 1024));
    server_config.transport_config(Arc::new(transport_config));
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    let stalled = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let _stream = connection.accept_bi().await.unwrap();
        std::future::pending::<()>().await;
    });

    let info = QuicEndpointInfo {
        endpoints: crate::transports::utils::interfaces::resolve_advertise_endpoints(
            addr,
            &crate::transports::utils::interfaces::InterfaceFilter::All,
        )
        .unwrap(),
        fingerprint: identity.fingerprint,
    };
    let mut builder = WorkerAddressBuilder::new();
    builder.add_entry("quic", info.encode().unwrap()).unwrap();
    let peer_id = InstanceId::new_v4();
    let (client, _client_streams, _) = started().await;
    client
        .register(PeerInfo::new(peer_id, builder.build().unwrap()))
        .unwrap();

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
    // Let the writer fill the window and park.
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.shutdown();
    client.closed().await;
    let failed = errors.0.lock().unwrap().len();
    assert_eq!(
        failed, FRAMES,
        "{failed} of {FRAMES} frames failed when closed() returned; the rest are unaccounted for"
    );
    stalled.abort();
}
