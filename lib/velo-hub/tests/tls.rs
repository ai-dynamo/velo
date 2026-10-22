// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TLS on the hub's listeners: the existing client over a configured
//! `reqwest::Client`, client certificates, the client identity in routes and
//! registration hooks, the SSE watch, and rotation.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use futures::StreamExt;
use parking_lot::Mutex;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use velo_hub::axum::Router;
use velo_hub::axum::routing::get;
use velo_hub::client::HubClient;
use velo_hub::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use velo_hub::protocol::ErrorCode;
use velo_hub::store::Removal;
use velo_hub::velo::discovery::{PeerDiscovery, ServiceDiscovery, ServiceEvent};
use velo_hub::{
    ClientCerts, ClientIdentity, Committed, Extension, Hub, HubTls, RegistrationView, Rejection,
    Txn,
};

struct Ca {
    issuer: CertifiedIssuer<'static, KeyPair>,
}

struct Leaf {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Ca {
    fn new() -> Self {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(DnType::CommonName, "hub test CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        Self {
            issuer: CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap(),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.issuer.der().clone()
    }

    fn issue(&self, sans: Vec<SanType>, usage: ExtendedKeyUsagePurpose) -> Leaf {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.subject_alt_names = sans;
        params.extended_key_usages = vec![usage];
        let key = KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            chain: vec![cert.der().clone(), self.root()],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        }
    }

    /// The hub's certificate, for 127.0.0.1.
    fn server(&self) -> Leaf {
        self.issue(
            vec![SanType::IpAddress([127, 0, 0, 1].into())],
            ExtendedKeyUsagePurpose::ServerAuth,
        )
    }

    /// A client certificate with `uri` as its SAN.
    fn client(&self, uri: &str) -> Leaf {
        self.issue(
            vec![SanType::URI(uri.try_into().unwrap())],
            ExtendedKeyUsagePurpose::ClientAuth,
        )
    }

    fn hub_tls(&self) -> HubTls {
        let leaf = self.server();
        HubTls::new(leaf.chain, leaf.key).unwrap()
    }
}

fn rustls_client(ca: &Ca, cert: Option<Leaf>) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.root()).unwrap();
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots);
    match cert {
        Some(leaf) => builder.with_client_auth_cert(leaf.chain, leaf.key).unwrap(),
        None => builder.with_no_client_auth(),
    }
}

/// A `reqwest::Client` that trusts `ca`, presenting `cert` if given.
fn https(ca: &Ca, cert: Option<Leaf>) -> reqwest::Client {
    reqwest::Client::builder()
        .use_preconfigured_tls(rustls_client(ca, cert))
        .build()
        .unwrap()
}

/// The existing hub client, over https with `http`.
fn client_for(hub: &Hub, http: reqwest::Client) -> HubClient {
    HubClient::builder()
        .scheme("https")
        .host("127.0.0.1")
        .public_port(hub.public_addr().port())
        .control_port(hub.control_addr().port())
        .http_client(http)
        .build()
        .unwrap()
}

fn none() -> BTreeMap<String, serde_json::Value> {
    BTreeMap::new()
}

/// Register, discover and unregister through the hub client, over TLS on
/// both listeners. A plain-HTTP client gets no answer from a TLS listener.
#[tokio::test]
async fn the_hub_client_works_over_tls() {
    let ca = Ca::new();
    let tls = ca.hub_tls();
    let hub = builder()
        .public_tls(tls.clone())
        .control_tls(tls)
        .serve()
        .await
        .unwrap();
    let client = client_for(&hub, https(&ca, None));
    let peer = make_peer();
    client.register_peer(peer.clone(), none()).await.unwrap();
    let found = client
        .discovery()
        .discover_by_instance_id(peer.instance_id())
        .await
        .unwrap();
    assert_eq!(found.instance_id(), peer.instance_id());

    let plain = reqwest::get(format!("http://{}/v1/health", hub.public_addr())).await;
    assert!(
        plain.is_err(),
        "plain HTTP got an answer from a TLS listener"
    );

    client.unregister().await.unwrap();
    hub.shutdown().await.unwrap();
}

/// With required client certificates, a client without one fails the
/// handshake, and so does one from another CA. Control: the same client
/// with a certificate from the configured CA registers.
#[tokio::test]
async fn a_client_without_a_certificate_is_refused_when_verification_is_on() {
    let ca = Ca::new();
    let other = Ca::new();
    let tls = ca
        .hub_tls()
        .client_certs([ca.root()], ClientCerts::Required)
        .unwrap();
    let hub = builder().control_tls(tls).serve().await.unwrap();
    let control = |http: reqwest::Client| {
        HubClient::builder()
            .host("127.0.0.1")
            .public_port(hub.public_addr().port())
            .control_port(hub.control_addr().port())
            .scheme("https")
            .http_client(http)
            .build()
            .unwrap()
    };

    let anonymous = control(https(&ca, None));
    assert!(anonymous.register_peer(make_peer(), none()).await.is_err());
    let foreign = control(https(&ca, Some(other.client("spiffe://d/worker/c1"))));
    assert!(foreign.register_peer(make_peer(), none()).await.is_err());
    assert!(hub.store().list().is_empty());

    let known = control(https(&ca, Some(ca.client("spiffe://d/worker/c1"))));
    known.register_peer(make_peer(), none()).await.unwrap();
    assert_eq!(hub.store().list().len(), 1);
    hub.shutdown().await.unwrap();
}

/// Admits a registration only from `spiffe://d/worker/allowed`, and records
/// the identity each hook saw. Its public route returns the caller's URIs.
#[derive(Default)]
struct Gate {
    seen: Mutex<Vec<Seen>>,
}

/// A hook, and the client URIs it saw.
type Seen = (&'static str, Option<Vec<String>>);

const ALLOWED: &str = "spiffe://d/worker/allowed";

fn uris(client: Option<&ClientIdentity>) -> Option<Vec<String>> {
    client.map(|identity| identity.uris().to_vec())
}

impl Extension for Gate {
    const NAME: &'static str = "gate";
    type Registration = ();
    type Staged = ();

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route(
                "/me",
                get(|identity: ClientIdentity| async move { identity.uris().join(",") }),
            )
            .route(
                "/maybe",
                get(|identity: Option<ClientIdentity>| async move {
                    identity.map_or("anonymous".to_string(), |id| id.uris().join(","))
                }),
            )
    }

    fn admit(&self, view: &RegistrationView<'_>, payload: Option<&()>) -> Result<(), Rejection> {
        self.seen.lock().push(("admit", uris(view.client)));
        if payload.is_some()
            && !view
                .client
                .is_some_and(|c| c.uris().iter().any(|u| u == ALLOWED))
        {
            return Err(Rejection::bad_request(
                "this identity may not join the gate",
            ));
        }
        Ok(())
    }

    async fn prepare(&self, txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        self.seen.lock().push(("prepare", uris(txn.client)));
        Ok(())
    }

    fn commit(&self, _staged: (), registration: &Committed<'_>) {
        self.seen.lock().push(("commit", uris(registration.client)));
    }

    fn on_removed(&self, _removal: Removal) {}
}

/// An extension route reads the verified client identity, and a registration
/// hook admits only the identity it authorizes. Anonymous clients are
/// allowed on the public listener (`Optional`), and get 401 from a route
/// that needs an identity.
#[tokio::test]
async fn an_extension_sees_the_client_identity() {
    let ca = Ca::new();
    let gate = Arc::new(Gate::default());
    let hub = builder()
        .extension(Arc::clone(&gate))
        .public_tls(
            ca.hub_tls()
                .client_certs([ca.root()], ClientCerts::Optional)
                .unwrap(),
        )
        .control_tls(
            ca.hub_tls()
                .client_certs([ca.root()], ClientCerts::Required)
                .unwrap(),
        )
        .serve()
        .await
        .unwrap();
    let route = |path: &str| {
        format!(
            "https://127.0.0.1:{}/v1/ext/gate{path}",
            hub.public_addr().port()
        )
    };

    let allowed = https(&ca, Some(ca.client(ALLOWED)));
    let me = allowed.get(route("/me")).send().await.unwrap();
    assert_eq!(me.status(), 200);
    assert_eq!(me.text().await.unwrap(), ALLOWED);
    let anonymous = https(&ca, None);
    let me = anonymous.get(route("/me")).send().await.unwrap();
    assert_eq!(me.status(), 401);
    assert_eq!(error_code(me).await, ErrorCode::Unauthorized);
    let maybe = anonymous.get(route("/maybe")).send().await.unwrap();
    assert_eq!(maybe.text().await.unwrap(), "anonymous");

    let opt_in = BTreeMap::from([("gate".to_string(), serde_json::Value::Null)]);
    let stranger = client_for(&hub, https(&ca, Some(ca.client("spiffe://d/worker/other"))));
    let refused = stranger.register_peer(make_peer(), opt_in.clone()).await;
    assert!(
        matches!(&refused, Err(error) if error.code() == Some(ErrorCode::BadRequest)),
        "{refused:?}"
    );
    let member = client_for(&hub, allowed);
    member.register_peer(make_peer(), opt_in).await.unwrap();

    let seen = gate.seen.lock().clone();
    let allowed_uris = Some(vec![ALLOWED.to_string()]);
    assert_eq!(
        seen,
        vec![
            ("admit", Some(vec!["spiffe://d/worker/other".to_string()])),
            ("admit", allowed_uris.clone()),
            ("prepare", allowed_uris.clone()),
            ("commit", allowed_uris),
        ]
    );
    hub.shutdown().await.unwrap();
}

async fn next_event(
    watch: &mut (impl futures::Stream<Item = ServiceEvent> + Unpin),
) -> ServiceEvent {
    tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("a watch event within 5 s")
        .expect("the watch is open")
}

/// The service watch (SSE) over TLS reports the initial set and a change.
#[tokio::test]
async fn a_service_watch_works_over_tls() {
    let ca = Ca::new();
    let tls = ca.hub_tls();
    let hub = builder()
        .public_tls(tls.clone())
        .control_tls(tls)
        .serve()
        .await
        .unwrap();
    let watcher = client_for(&hub, https(&ca, None));
    let mut watch = watcher.services().watch_instances("svc").await.unwrap();
    assert_eq!(next_event(&mut watch).await, ServiceEvent::Initial(vec![]));

    let member = client_for(&hub, https(&ca, None));
    let peer = make_peer();
    member.register_peer(peer.clone(), none()).await.unwrap();
    let _guard = member.register_service("svc").await.unwrap();
    assert_eq!(
        next_event(&mut watch).await,
        ServiceEvent::Added(peer.instance_id())
    );
    drop(watch);
    hub.shutdown().await.unwrap();
}

fn connector(ca: &Ca, cert: Leaf) -> tokio_rustls::TlsConnector {
    tokio_rustls::TlsConnector::from(Arc::new(rustls_client(ca, Some(cert))))
}

/// The certificate that a new TLS connection to `addr` is served, if the
/// hub answers a request on it. A connector keeps its session cache, so a
/// second call with the same one would resume, if the hub allowed it.
async fn handshake(
    addr: std::net::SocketAddr,
    connector: &tokio_rustls::TlsConnector,
) -> Result<CertificateDer<'static>, std::io::Error> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let tcp = tokio::net::TcpStream::connect(addr).await?;
    let mut stream = connector
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await?;
    // TLS 1.3: the server checks the client certificate after the client's
    // handshake ends. A refused client learns it at its first read.
    stream
        .write_all(b"GET /v1/health HTTP/1.1\r\nhost: hub\r\nconnection: close\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    if !response.starts_with(b"HTTP/1.1 200") {
        return Err(std::io::Error::other(
            String::from_utf8_lossy(&response).to_string(),
        ));
    }
    Ok(stream.get_ref().1.peer_certificates().unwrap()[0].clone())
}

/// `rotate` and `set_client_roots` take effect on the next connection,
/// without a restart. One client keeps its connector throughout, so a
/// resumed session would present the first certificate and skip the new
/// roots; the hub does not resume.
#[tokio::test]
async fn certificates_and_client_roots_rotate_without_a_restart() {
    let ca = Ca::new();
    let next_ca = Ca::new();
    let first = ca.server();
    let first_cert = first.chain[0].clone();
    let tls = HubTls::new(first.chain, first.key)
        .unwrap()
        .client_certs([ca.root()], ClientCerts::Required)
        .unwrap();
    let hub = builder().control_tls(tls.clone()).serve().await.unwrap();
    let addr = hub.control_addr();
    let old_client = connector(&ca, ca.client("spiffe://d/c/1"));

    assert_eq!(handshake(addr, &old_client).await.unwrap(), first_cert);

    let second = ca.server();
    let second_cert = second.chain[0].clone();
    tls.rotate(second.chain, second.key).unwrap();
    assert_eq!(handshake(addr, &old_client).await.unwrap(), second_cert);

    tls.set_client_roots([next_ca.root()]).unwrap();
    assert!(
        handshake(addr, &old_client).await.is_err(),
        "a client certificate from the old CA was accepted"
    );
    let new_client = connector(&ca, next_ca.client("spiffe://d/c/2"));
    handshake(addr, &new_client).await.unwrap();
    hub.shutdown().await.unwrap();
}

/// Handshakes run in their own tasks: clients that connect and send
/// nothing do not hold up `accept` for the client behind them.
#[tokio::test]
async fn a_silent_client_does_not_block_other_handshakes() {
    let ca = Ca::new();
    let hub = builder().public_tls(ca.hub_tls()).serve().await.unwrap();
    let mut silent = Vec::new();
    for _ in 0..8 {
        silent.push(
            tokio::net::TcpStream::connect(hub.public_addr())
                .await
                .unwrap(),
        );
    }
    let health = https(&ca, None)
        .get(format!(
            "https://127.0.0.1:{}/v1/health",
            hub.public_addr().port()
        ))
        .send();
    let response = tokio::time::timeout(Duration::from_secs(2), health)
        .await
        .expect("a handshake behind silent clients finishes at once")
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(silent);
    hub.shutdown().await.unwrap();
}

#[test]
fn a_key_that_does_not_match_the_certificate_is_refused() {
    let ca = Ca::new();
    let leaf = ca.server();
    let other = ca.server();
    assert!(HubTls::new(leaf.chain.clone(), other.key).is_err());
    let tls = HubTls::new(leaf.chain, leaf.key).unwrap();
    let again = ca.server();
    let mismatch = ca.server();
    assert!(tls.rotate(again.chain, mismatch.key).is_err());
    assert!(
        tls.set_client_roots([ca.root()]).is_err(),
        "set_client_roots without client_certs"
    );
}

/// Opens TLS only when it is told to start.
struct SlowStart(tokio::sync::Semaphore);

impl Extension for SlowStart {
    const NAME: &'static str = "slow-start";
    type Registration = ();
    type Staged = ();

    async fn start(&self, _ctx: &velo_hub::StartContext<'_>) -> anyhow::Result<()> {
        let _ = self.0.acquire().await;
        Ok(())
    }

    async fn prepare(&self, _txn: &Txn<'_>, _payload: Option<()>) -> Result<(), Rejection> {
        Ok(())
    }

    fn commit(&self, _staged: (), _registration: &Committed<'_>) {}

    fn on_removed(&self, _removal: Removal) {}
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A TLS listener takes no connection before the hub serves. Each one it
/// took during a slow startup would hold a task and a socket until the hub
/// served, with no bound: the kernel backlog is the only queue then.
#[tokio::test]
async fn no_tls_handshake_completes_before_the_hub_serves() {
    let ca = Ca::new();
    let gate = Arc::new(SlowStart(tokio::sync::Semaphore::new(0)));
    let mut config = config();
    config.server.public_port = free_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.server.public_port));
    let serving = tokio::spawn(
        Hub::builder(config)
            .extension(Arc::clone(&gate))
            .public_tls(ca.hub_tls())
            .serve(),
    );
    let client = connector(&ca, ca.client("spiffe://d/c/1"));
    // Wait for the bind, then try a handshake while start() is held.
    let early = async {
        loop {
            if let Ok(tcp) = tokio::net::TcpStream::connect(addr).await {
                let name = ServerName::try_from("127.0.0.1").unwrap();
                return client.connect(name, tcp).await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(1), early)
            .await
            .is_err(),
        "a TLS handshake completed while the hub was still starting"
    );
    gate.0.add_permits(1);
    let hub = serving.await.unwrap().unwrap();
    assert!(handshake(addr, &client).await.is_ok());
    hub.shutdown().await.unwrap();
}

/// The listener offers HTTP/1.1 only by ALPN: the hub does not build
/// axum's HTTP/2 support, so offering `h2` broke clients that prefer it.
#[tokio::test]
async fn the_listener_negotiates_http_1_1() {
    let ca = Ca::new();
    let hub = builder().public_tls(ca.hub_tls()).serve().await.unwrap();
    let mut config = rustls_client(&ca, None);
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tcp = tokio::net::TcpStream::connect(hub.public_addr())
        .await
        .unwrap();
    let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    assert_eq!(stream.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
    hub.shutdown().await.unwrap();
}

/// When `shutdown` returns, the TLS listeners have closed their sockets,
/// as the plain ones have: the ports bind again at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_releases_the_tls_ports() {
    let ca = Ca::new();
    for round in 0..20 {
        let tls = ca.hub_tls();
        let hub = builder()
            .public_tls(tls.clone())
            .control_tls(tls)
            .serve()
            .await
            .unwrap();
        let (public, control) = (hub.public_addr(), hub.control_addr());
        hub.shutdown().await.unwrap();
        for addr in [public, control] {
            assert!(
                std::net::TcpListener::bind(addr).is_ok(),
                "round {round}: {addr} is still bound after shutdown"
            );
        }
    }
}
