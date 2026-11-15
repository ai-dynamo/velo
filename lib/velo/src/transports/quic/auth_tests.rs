// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC with certificates from a private CA, and the authorizer.
//!
//! Each refusal test has a positive control that uses the same client, so a
//! refusal cannot pass because the client never worked. "Nothing is
//! dispatched" is read off the listener's inbound streams, which every frame
//! the transport routes reaches; the Velo tests also count handler calls.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use velo_ext::{
    DataStreams, InstanceId, MessageType, PeerInfo, Transport, TransportErrorHandler, make_channels,
};

use super::test_ca::{Ca, Leaf};
use super::{ConnectionOrigin, PeerIdentity, QuicTls};
use crate::transports::quic::{QuicEndpointInfo, QuicTransport, QuicTransportBuilder, tls};
use crate::transports::tcp::TcpFrameCodec;

const QUIET: Duration = Duration::from_millis(500);
const BOUND: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Errors(Mutex<Vec<String>>);

impl TransportErrorHandler for Errors {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        self.0.lock().push(error);
    }
}

impl Errors {
    async fn wait_for(&self, count: usize) {
        tokio::time::timeout(BOUND, async {
            while self.0.lock().len() < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {count} failed frames, got {:?}", self.0.lock()));
    }
}

/// One authorizer call: the peer's URIs and the connection.
type Call = (Vec<String>, ConnectionOrigin);

/// Every authorizer call, in order.
#[derive(Clone, Default)]
struct Calls(Arc<Mutex<Vec<Call>>>);

impl Calls {
    /// An authorizer that records each call and accepts only peers whose
    /// URIs include `allowed`.
    fn allowing(
        &self,
        allowed: &'static str,
    ) -> impl Fn(&PeerIdentity, &ConnectionOrigin) -> bool + Send + Sync + 'static + use<> {
        let calls = self.clone();
        move |peer, origin| {
            calls.0.lock().push((peer.uris().to_vec(), *origin));
            peer.uris().iter().any(|uri| uri == allowed)
        }
    }

    fn snapshot(&self) -> Vec<Call> {
        self.0.lock().clone()
    }
}

async fn started(tls: QuicTls, lanes: u16) -> (QuicTransport, DataStreams, InstanceId) {
    let transport = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .lanes(lanes)
        .tls(tls)
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

fn peer_info(transport: &QuicTransport, id: InstanceId) -> PeerInfo {
    PeerInfo::new(id, transport.address())
}

fn listen_addr(transport: &QuicTransport) -> SocketAddr {
    let raw = transport
        .address()
        .get_entry(transport.key())
        .unwrap()
        .unwrap();
    QuicEndpointInfo::decode(&raw).unwrap().endpoints[0]
        .socket_addr()
        .unwrap()
}

/// Nothing reached any inbound stream of the listener within [`QUIET`].
async fn assert_nothing_dispatched(streams: &DataStreams) {
    tokio::time::sleep(QUIET).await;
    assert!(
        streams.message_stream.is_empty(),
        "a Message was dispatched"
    );
    assert!(
        streams.response_stream.is_empty(),
        "a Response was dispatched"
    );
    assert!(streams.event_stream.is_empty(), "an Event was dispatched");
    assert!(
        streams.shutdown_stream.is_empty(),
        "a ShuttingDown was dispatched"
    );
}

async fn expect_event(streams: &DataStreams, payload: &[u8]) {
    let (_, got) = tokio::time::timeout(BOUND, streams.event_stream.recv_async())
        .await
        .expect("the frame arrives")
        .unwrap();
    assert_eq!(&got[..], payload);
}

/// Accepts any server certificate: the raw client tests the listener, not
/// the dialer's verification.
#[derive(Debug)]
struct AnyServer;

impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Presents a fixed certificate and key without checking that they match,
/// as a peer that holds a stolen chain but not its key would.
#[derive(Debug)]
struct Unchecked(Arc<rustls::sign::CertifiedKey>);

impl Unchecked {
    fn new(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Arc<Self> {
        let key = rustls::crypto::ring::default_provider()
            .key_provider
            .load_private_key(key)
            .unwrap();
        Arc::new(Self(Arc::new(rustls::sign::CertifiedKey::new(chain, key))))
    }
}

impl rustls::client::ResolvesClientCert for Unchecked {
    fn resolve(
        &self,
        _hints: &[&[u8]],
        _schemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

impl rustls::server::ResolvesServerCert for Unchecked {
    fn resolve(
        &self,
        _hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }
}

/// What a raw client presents.
enum ClientCert {
    None,
    Leaf(Leaf),
    /// The chain of the first, with the key of the second.
    WrongKey(Leaf, Leaf),
}

/// Dial `addr` with a raw quinn client, write one Event frame with
/// `payload`, finish the stream and wait briefly for the peer. Errors
/// anywhere are ignored: the listener's streams are the observable.
async fn raw_send(addr: SocketAddr, cert: ClientCert, payload: &'static [u8]) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyServer));
    let mut crypto = match cert {
        ClientCert::None => builder.with_no_client_auth(),
        ClientCert::Leaf(leaf) => builder.with_client_auth_cert(leaf.chain, leaf.key).unwrap(),
        ClientCert::WrongKey(chain, key) => {
            builder.with_client_cert_resolver(Unchecked::new(chain.chain, key.key))
        }
    };
    crypto.alpn_protocols = vec![tls::ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));
    let endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let attempt = async {
        let connection = endpoint
            .connect_with(config, addr, "velo")
            .ok()?
            .await
            .ok()?;
        let (mut send, _recv) = connection.open_bi().await.ok()?;
        TcpFrameCodec::encode_frame(&mut send, MessageType::Event, b"h", payload)
            .await
            .ok()?;
        send.finish().ok()?;
        let _ = tokio::time::timeout(Duration::from_secs(2), send.stopped()).await;
        Some(connection)
    };
    let connection = tokio::time::timeout(Duration::from_secs(5), attempt)
        .await
        .ok()
        .flatten();
    if let Some(connection) = connection {
        connection.close(0u32.into(), b"done");
    }
    endpoint.wait_idle().await;
}

/// No client certificate: the handshake fails, and no frame is routed.
/// Control: the same client with a certificate from the roots gets through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialer_without_a_certificate_is_refused_before_any_frame() {
    let ca = Ca::new();
    let (server, streams, _) = started(ca.tls("spiffe://d/server/c1"), 1).await;
    let addr = listen_addr(&server);

    raw_send(
        addr,
        ClientCert::Leaf(ca.leaf("spiffe://d/worker/c1")),
        b"control",
    )
    .await;
    expect_event(&streams, b"control").await;

    raw_send(addr, ClientCert::None, b"anonymous").await;
    assert_nothing_dispatched(&streams).await;
}

/// A certificate from another CA is refused in the handshake, from a raw
/// client and from a velo transport. Control: the same raw client with a
/// certificate from the listener's CA gets through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_from_another_root_is_refused_before_any_frame() {
    let ca = Ca::new();
    let other = Ca::new();
    let (server, streams, server_id) = started(ca.tls("spiffe://d/server/c1"), 1).await;
    let addr = listen_addr(&server);

    raw_send(
        addr,
        ClientCert::Leaf(ca.leaf("spiffe://d/worker/c1")),
        b"control",
    )
    .await;
    expect_event(&streams, b"control").await;

    raw_send(
        addr,
        ClientCert::Leaf(other.leaf("spiffe://d/worker/c1")),
        b"foreign",
    )
    .await;
    assert_nothing_dispatched(&streams).await;

    // A velo transport from the other CA: its own verifier refuses the
    // server, and the server would refuse it. Its frame fails.
    let (stranger, _, _) = started(other.tls("spiffe://d/worker/c1"), 1).await;
    stranger.register(peer_info(&server, server_id)).unwrap();
    let errors = Arc::new(Errors::default());
    let _ = stranger.send_message(
        server_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"foreign transport"),
        MessageType::Event,
        errors.clone(),
    );
    errors.wait_for(1).await;
    assert_nothing_dispatched(&streams).await;
}

/// A default transport (pinned self-signed certificate, no client
/// certificate) cannot dial a CA-mode transport.
///
/// The dialer's frames are lost without a report: in TLS 1.3 the client
/// finishes its handshake and writes before the server refuses the missing
/// certificate. So the test asserts only what the listener dispatched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_default_transport_cannot_dial_a_ca_transport() {
    let ca = Ca::new();
    let (server, streams, server_id) = started(ca.tls("spiffe://d/server/c1"), 1).await;
    let plain = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, _plain_streams) = make_channels();
    plain
        .start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        )
        .await
        .unwrap();
    plain.register(peer_info(&server, server_id)).unwrap();
    let _ = plain.send_message(
        server_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"unauthenticated"),
        MessageType::Event,
        Arc::new(Errors::default()),
    );
    assert_nothing_dispatched(&streams).await;
}

/// A chain from the right CA, presented with another key, fails the
/// handshake signature check. Control: the same chain with its own key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_chain_without_its_key_is_refused() {
    let ca = Ca::new();
    let (server, streams, _) = started(ca.tls("spiffe://d/server/c1"), 1).await;
    let addr = listen_addr(&server);

    raw_send(
        addr,
        ClientCert::Leaf(ca.leaf("spiffe://d/worker/c1")),
        b"control",
    )
    .await;
    expect_event(&streams, b"control").await;

    let stolen = ca.leaf("spiffe://d/worker/c1");
    let mine = ca.leaf("spiffe://d/thief/c1");
    raw_send(addr, ClientCert::WrongKey(stolen, mine), b"stolen").await;
    assert_nothing_dispatched(&streams).await;
}

/// A raw QUIC server that presents `chain` with `key`, unchecked, and
/// forwards the payload of each Event frame it reads.
async fn raw_server(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> (SocketAddr, flume::Receiver<Bytes>, quinn::Endpoint) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(Unchecked::new(chain, key));
    crypto.alpn_protocols = vec![tls::ALPN.to_vec()];
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto).unwrap(),
    ));
    let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    let (tx, rx) = flume::unbounded();
    let accept = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accept.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.await else {
                    return;
                };
                let Ok((_send, recv)) = connection.accept_bi().await else {
                    return;
                };
                use futures::StreamExt;
                let mut framed = tokio_util::codec::FramedRead::new(recv, TcpFrameCodec::new());
                while let Some(Ok((_, _, payload))) = framed.next().await {
                    let _ = tx.send(payload);
                }
            });
        }
    });
    (addr, rx, endpoint)
}

fn peer_at(addr: SocketAddr, id: InstanceId) -> PeerInfo {
    let info = QuicEndpointInfo {
        endpoints: vec![crate::transports::utils::interfaces::InterfaceEndpoint {
            name: String::new(),
            ip: addr.ip().to_string(),
            port: addr.port(),
            prefix_len: 32,
            numa_node: None,
        }],
        fingerprint: [0; 32],
        ports: vec![],
    };
    let mut builder = crate::transports::address::WorkerAddressBuilder::new();
    builder
        .add_entry(velo_ext::TransportKey::from("quic"), info.encode().unwrap())
        .unwrap();
    PeerInfo::new(id, builder.build().unwrap())
}

/// The dialer checks the server's handshake signature against the chain:
/// a server with the right chain and another key is refused, and gets no
/// frame. Control: a server with the chain's own key gets the frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_chain_without_its_key_is_refused() {
    let ca = Ca::new();
    let (client, _, _) = started(ca.tls("spiffe://d/worker/c1"), 1).await;

    let honest = ca.leaf("spiffe://d/server/c1");
    let (addr, frames, _honest_endpoint) = raw_server(honest.chain, honest.key).await;
    let honest_id = InstanceId::new_v4();
    client.register(peer_at(addr, honest_id)).unwrap();
    let errors = Arc::new(Errors::default());
    let _ = client.send_message(
        honest_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"control"),
        MessageType::Event,
        errors.clone(),
    );
    let got = tokio::time::timeout(BOUND, frames.recv_async())
        .await
        .expect("the honest server gets the frame")
        .unwrap();
    assert_eq!(&got[..], b"control");

    let stolen = ca.leaf("spiffe://d/server/c1");
    let other_key = ca.leaf("spiffe://d/thief/c1");
    let (addr, frames, _thief_endpoint) = raw_server(stolen.chain, other_key.key).await;
    let thief_id = InstanceId::new_v4();
    client.register(peer_at(addr, thief_id)).unwrap();
    let _ = client.send_message(
        thief_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"secret"),
        MessageType::Event,
        errors.clone(),
    );
    errors.wait_for(1).await;
    tokio::time::sleep(QUIET).await;
    assert!(frames.is_empty(), "a server without the key got a frame");
}

/// The listener's authorizer refuses a certificate the CA did issue. It
/// was asked, with the peer's URI, and nothing was dispatched. Control: an
/// allowed URI from the same CA, through the same client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_listeners_authorizer_refuses_a_valid_certificate() {
    let ca = Ca::new();
    let calls = Calls::default();
    let tls = ca
        .tls("spiffe://d/server/c1")
        .authorizer(calls.allowing("spiffe://d/worker/c1"));
    let (server, streams, _) = started(tls, 1).await;
    let addr = listen_addr(&server);

    raw_send(
        addr,
        ClientCert::Leaf(ca.leaf("spiffe://d/worker/c1")),
        b"control",
    )
    .await;
    expect_event(&streams, b"control").await;

    raw_send(
        addr,
        ClientCert::Leaf(ca.leaf("spiffe://d/worker/c9")),
        b"refused",
    )
    .await;
    assert_nothing_dispatched(&streams).await;

    let calls = calls.snapshot();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(calls[1].0, vec!["spiffe://d/worker/c9".to_string()]);
    assert!(matches!(calls[1].1, ConnectionOrigin::Accepted { .. }));
}

/// The dialer's authorizer refuses the listener: no frame is written, the
/// queued frames fail, and nothing reaches the listener. It was asked with
/// the instance id and lane that it dialed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_dialers_authorizer_refuses_a_listener() {
    let ca = Ca::new();
    let (server, streams, server_id) = started(ca.tls("spiffe://d/server/c9"), 1).await;
    let calls = Calls::default();
    let tls = ca
        .tls("spiffe://d/worker/c1")
        .authorizer(calls.allowing("spiffe://d/server/c1"));
    let (client, _, _) = started(tls, 1).await;
    client.register(peer_info(&server, server_id)).unwrap();

    let errors = Arc::new(Errors::default());
    for _ in 0..3 {
        let _ = client.send_message(
            server_id,
            Bytes::from_static(b"h"),
            Bytes::from_static(b"to an impostor"),
            MessageType::Event,
            errors.clone(),
        );
    }
    errors.wait_for(3).await;
    assert_nothing_dispatched(&streams).await;
    let calls = calls.snapshot();
    assert!(!calls.is_empty());
    assert!(
        matches!(calls[0].1, ConnectionOrigin::Dialed { peer, lane: 0, .. } if peer == server_id),
        "{calls:?}"
    );
    assert_eq!(calls[0].0, vec!["spiffe://d/server/c9".to_string()]);

    // A health check is a dial too: it reports the refused peer as failed.
    assert!(client.check_health(server_id, BOUND).await.is_err());
}

/// Positive control: a connection whose first frame is a Response. A sends a
/// Message to B; B's first frame on the connection it dials back to A is
/// the Response. Both authorizers saw both connections, each from its side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_begins_with_a_response_is_authorized_and_delivered() {
    let ca = Ca::new();
    let a_calls = Calls::default();
    let b_calls = Calls::default();
    let (a, a_streams, a_id) = started(
        ca.tls("spiffe://d/a/c1")
            .authorizer(a_calls.allowing("spiffe://d/b/c2")),
        1,
    )
    .await;
    let (b, b_streams, b_id) = started(
        ca.tls("spiffe://d/b/c2")
            .authorizer(b_calls.allowing("spiffe://d/a/c1")),
        1,
    )
    .await;
    a.register(peer_info(&b, b_id)).unwrap();
    b.register(peer_info(&a, a_id)).unwrap();

    let errors = Arc::new(Errors::default());
    let _ = a.send_message(
        b_id,
        Bytes::from_static(b"req"),
        Bytes::from_static(b"request"),
        MessageType::Message,
        errors.clone(),
    );
    let inbound = tokio::time::timeout(BOUND, b_streams.message_stream.recv_async())
        .await
        .expect("the request arrives")
        .unwrap();
    drop(inbound);
    let _ = b.send_message(
        a_id,
        Bytes::from_static(b"resp"),
        Bytes::from_static(b"response"),
        MessageType::Response,
        errors.clone(),
    );
    let (_, payload) = tokio::time::timeout(BOUND, a_streams.response_stream.recv_async())
        .await
        .expect("the response arrives")
        .unwrap();
    assert_eq!(&payload[..], b"response");
    assert!(errors.0.lock().is_empty());

    let accepted = |calls: &Calls| {
        calls
            .snapshot()
            .iter()
            .filter(|(_, origin)| matches!(origin, ConnectionOrigin::Accepted { .. }))
            .count()
    };
    let dialed = |calls: &Calls| {
        calls
            .snapshot()
            .iter()
            .filter(|(_, origin)| matches!(origin, ConnectionOrigin::Dialed { .. }))
            .count()
    };
    // A dialed B for the request; B dialed A for the response.
    assert_eq!((dialed(&a_calls), accepted(&a_calls)), (1, 1));
    assert_eq!((dialed(&b_calls), accepted(&b_calls)), (1, 1));
}

/// Rotation without a restart. The peer is registered before each change,
/// and a new connection (another lane) picks the change up:
/// - after `rotate`, the dialer presents the new certificate;
/// - after `set_roots` drops the old CA, a certificate from it is refused,
///   while a connection that is already open keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certificates_and_roots_rotate_without_a_restart() {
    let ca = Ca::new();
    let next_ca = Ca::new();
    let calls = Calls::default();
    let recorder = calls.clone();
    let server_tls = ca.tls("spiffe://d/server/c1").authorizer(
        move |peer: &PeerIdentity, origin: &ConnectionOrigin| {
            recorder.0.lock().push((peer.uris().to_vec(), *origin));
            true
        },
    );
    let (server, streams, server_id) = started(server_tls.clone(), 1).await;
    let client_tls = ca.tls("spiffe://d/worker/old");
    let (client, _, _) = started(client_tls.clone(), 3).await;
    client.register(peer_info(&server, server_id)).unwrap();
    let errors = Arc::new(Errors::default());
    let send = |lane: u16, payload: &'static [u8]| {
        let _ = client.send_message_on_lane(
            server_id,
            lane,
            Bytes::from_static(b"h"),
            Bytes::from_static(payload),
            MessageType::Event,
            errors.clone(),
        );
    };

    send(0, b"lane 0 before");
    expect_event(&streams, b"lane 0 before").await;

    let rotated = ca.leaf("spiffe://d/worker/new");
    client_tls.rotate(rotated.chain, rotated.key).unwrap();
    send(1, b"lane 1 after rotate");
    expect_event(&streams, b"lane 1 after rotate").await;
    let uris: Vec<Vec<String>> = calls.snapshot().into_iter().map(|(uris, _)| uris).collect();
    assert_eq!(
        uris,
        vec![
            vec!["spiffe://d/worker/old".to_string()],
            vec!["spiffe://d/worker/new".to_string()],
        ]
    );

    // The server now trusts only the next CA. The client's certificate is
    // from the old one, so its next new connection is refused.
    server_tls.set_roots([next_ca.root()]).unwrap();
    // The frame is lost without a report (see
    // `a_default_transport_cannot_dial_a_ca_transport`).
    send(2, b"lane 2 after set_roots");
    assert_nothing_dispatched(&streams).await;
    // The open lane 0 connection was authenticated before the change.
    send(0, b"lane 0 still open");
    expect_event(&streams, b"lane 0 still open").await;

    // A client certificate from the next CA is accepted, and the server,
    // which still presents its old-CA certificate, is trusted by a client
    // that keeps both CAs.
    let next = next_ca.leaf("spiffe://d/worker/next");
    client_tls.rotate(next.chain, next.key).unwrap();
    client_tls.set_roots([ca.root(), next_ca.root()]).unwrap();
    client.unregister(server_id);
    client.register(peer_info(&server, server_id)).unwrap();
    send(0, b"next CA");
    expect_event(&streams, b"next CA").await;
}

#[test]
fn a_key_that_does_not_match_the_leaf_is_refused() {
    let ca = Ca::new();
    let leaf = ca.leaf("spiffe://d/worker/c1");
    let other = ca.leaf("spiffe://d/worker/c2");
    assert!(QuicTls::new([ca.root()], leaf.chain.clone(), other.key).is_err());
    let tls = QuicTls::new([ca.root()], leaf.chain, leaf.key).unwrap();
    let again = ca.leaf("spiffe://d/worker/c3");
    let mismatch = ca.leaf("spiffe://d/worker/c4");
    assert!(tls.rotate(again.chain, mismatch.key).is_err());
    assert!(
        QuicTls::new(
            [],
            vec![],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(vec![]))
        )
        .is_err()
    );
}

mod velo_level {
    //! The same rules through `Velo`: a counting handler is the observable.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{Handler, Velo};

    async fn node(tls: QuicTls) -> Velo {
        let transport = QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .tls(tls)
            .build()
            .unwrap();
        Velo::builder()
            .add_transport(Arc::new(transport))
            .build()
            .await
            .unwrap()
    }

    /// B accepts A and refuses C, both with certificates from B's CA. A's
    /// request reaches B's handler and its response comes back on the
    /// connection B dials to A. C's messages reach no handler.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn only_authorized_peers_reach_a_handler() {
        let ca = Ca::new();
        let allowed = |peer: &PeerIdentity, _: &ConnectionOrigin| {
            peer.uris()
                .iter()
                .any(|uri| uri.starts_with("spiffe://d/") && !uri.ends_with("/evil"))
        };
        let b = node(ca.tls("spiffe://d/b/c1").authorizer(allowed)).await;
        let a = node(ca.tls("spiffe://d/a/c1").authorizer(allowed)).await;
        let c = node(ca.tls("spiffe://d/c/evil")).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        b.register_handler(
            Handler::unary_handler("ping", move |ctx| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(Some(ctx.payload))
            })
            .build(),
        )
        .unwrap();
        for (x, y) in [(&a, &b), (&c, &b)] {
            x.register_peer(y.peer_info()).unwrap();
            y.register_peer(x.peer_info()).unwrap();
        }

        let echoed: Bytes = a
            .unary("ping")
            .unwrap()
            .raw_payload(Bytes::from_static(b"from a"))
            .instance(b.instance_id())
            .timeout(BOUND)
            .send()
            .await
            .expect("an authorized peer gets its answer");
        assert_eq!(&echoed[..], b"from a");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let refused = c
            .unary("ping")
            .unwrap()
            .raw_payload(Bytes::from_static(b"from c"))
            .instance(b.instance_id())
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        assert!(refused.is_err(), "a refused peer got an answer");
        tokio::time::sleep(QUIET).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a refused peer reached the handler"
        );
    }
}

/// Whether each direction's verifier accepts a leaf from `ca`.
fn trusts(tls: &QuicTls, leaf: &CertificateDer<'_>) -> (bool, bool) {
    use rustls::client::danger::ServerCertVerifier;
    use rustls::server::danger::ClientCertVerifier;
    let now = rustls::pki_types::UnixTime::now();
    let name = rustls::pki_types::ServerName::try_from("velo").unwrap();
    let outbound = super::RootsServerVerifier(tls.state.clone())
        .verify_server_cert(leaf, &[], &name, &[], now)
        .is_ok();
    let inbound = super::CurrentClientVerifier(tls.state.clone())
        .verify_client_cert(leaf, &[], now)
        .is_ok();
    (outbound, inbound)
}

/// Two `set_roots` calls at once leave both directions trusting the same
/// roots. With the roots and the client verifier behind separate locks, the
/// writes could interleave as A.roots, B.roots, B.verifier, A.verifier: the
/// dialer then trusted B and the listener A, after both calls returned.
#[test]
fn concurrent_root_updates_leave_both_directions_agreeing() {
    let a = Ca::new();
    let b = Ca::new();
    let leaf_a = a.leaf("spiffe://d/x/a").chain[0].clone();
    let leaf_b = b.leaf("spiffe://d/x/b").chain[0].clone();
    let tls = a.tls("spiffe://d/self/c1");
    for round in 0..2000 {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            for root in [a.root(), b.root()] {
                let (tls, barrier) = (&tls, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    tls.set_roots([root]).unwrap();
                });
            }
        });
        let (out_a, in_a) = trusts(&tls, &leaf_a);
        let (out_b, in_b) = trusts(&tls, &leaf_b);
        assert_eq!(
            (out_a, out_b),
            (in_a, in_b),
            "round {round}: the dialer and the listener trust different roots"
        );
    }
}
