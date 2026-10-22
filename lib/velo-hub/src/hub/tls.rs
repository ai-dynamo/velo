// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TLS on the hub's listeners, and the client identity it verifies.
//!
//! Each listener serves plain HTTP unless the program gives it a
//! [`HubTls`] (`HubBuilder::public_tls`, `HubBuilder::control_tls`). The
//! library reads no files: the program passes DER certificates and keys.
//! With [`HubTls::client_certs`], the listener asks clients for a
//! certificate and verifies it against the given roots. Routes read the
//! verified [`ClientIdentity`] with its extractor, and registration hooks
//! read it from `RegistrationView::client` and `Txn::client`.
//!
//! TLS handshakes run in their own tasks, each under
//! [`HANDSHAKE_TIMEOUT`], so a slow or silent client cannot hold up
//! `accept` for the clients behind it. At most `MAX_PENDING_TLS`
//! connections wait in a handshake or for axum; past that the listener
//! stops accepting.

use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use axum::extract::connect_info::{ConnectInfo, Connected};
use axum::extract::{FromRequestParts, OptionalFromRequestParts};
use axum::http::request::Parts;
use axum::serve::IncomingStream;
use parking_lot::RwLock;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::either::Either;

use super::error::HubError;
use crate::protocol::ErrorCode;

/// How long a client has to finish its TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether a listener with [`HubTls::client_certs`] requires a client
/// certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCerts {
    /// A client without a certificate fails the handshake.
    Required,
    /// A client may connect without a certificate. A certificate that it
    /// presents must verify, and its routes see a [`ClientIdentity`].
    Optional,
}

/// The verified certificate of the client on one connection.
#[derive(Clone, Debug)]
pub struct ClientIdentity {
    chain: Arc<[CertificateDer<'static>]>,
    uris: Arc<[String]>,
}

impl ClientIdentity {
    fn from_chain(chain: &[CertificateDer<'_>]) -> Option<Self> {
        let leaf = chain.first()?;
        // The handshake verified this certificate, so it parses.
        let uris = webpki::EndEntityCert::try_from(leaf)
            .map(|cert| cert.valid_uri_names().map(str::to_string).collect())
            .unwrap_or_default();
        Some(Self {
            chain: chain.iter().map(|cert| cert.clone().into_owned()).collect(),
            uris,
        })
    }

    /// The client's leaf certificate, in DER.
    pub fn certificate(&self) -> &CertificateDer<'static> {
        &self.chain[0]
    }

    /// The chain that the client presented, leaf first.
    pub fn chain(&self) -> &[CertificateDer<'static>] {
        &self.chain
    }

    /// The URI SANs of the leaf, such as `spiffe://deployment/class/cell`.
    pub fn uris(&self) -> &[String] {
        &self.uris
    }
}

/// Rejects a request without a verified client certificate with 401. Use
/// `Option<ClientIdentity>` on a route that also serves anonymous clients.
impl<S: Send + Sync> FromRequestParts<S> for ClientIdentity {
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Ok(identity) =
            <Self as OptionalFromRequestParts<S>>::from_request_parts(parts, state).await;
        identity.ok_or_else(|| {
            HubError::new(
                ErrorCode::Unauthorized,
                "this route needs a verified client certificate",
            )
        })
    }
}

impl<S: Send + Sync> OptionalFromRequestParts<S> for ClientIdentity {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<ConnectInfo<Connection>>()
            .and_then(|ConnectInfo(connection)| connection.identity.clone()))
    }
}

/// TLS for one or both hub listeners: the hub's certificate, and
/// optionally the roots that client certificates must verify against.
///
/// Clones share their state: keep one to [`rotate`](Self::rotate) the
/// certificate or [`set_client_roots`](Self::set_client_roots) while the hub
/// runs. New handshakes use the new values; open connections keep theirs.
#[derive(Clone)]
pub struct HubTls {
    state: Arc<State>,
}

struct State {
    provider: Arc<CryptoProvider>,
    certified: RwLock<Arc<CertifiedKey>>,
    client: RwLock<Option<ClientAuth>>,
}

struct ClientAuth {
    mode: ClientCerts,
    verifier: Arc<dyn ClientCertVerifier>,
}

impl fmt::Debug for HubTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HubTls")
            .field(
                "client_certs",
                &self.state.client.read().as_ref().map(|auth| auth.mode),
            )
            .finish_non_exhaustive()
    }
}

impl HubTls {
    /// Serve `chain` (leaf first) with its private `key`. Clients are not
    /// asked for a certificate. Fails if `key` does not match the leaf.
    pub fn new(chain: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Result<Self> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let certified = certified_key(chain, key, &provider)?;
        Ok(Self {
            state: Arc::new(State {
                provider,
                certified: RwLock::new(certified),
                client: RwLock::new(None),
            }),
        })
    }

    /// Ask clients for a certificate, and verify it against `roots`.
    pub fn client_certs(
        self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
        mode: ClientCerts,
    ) -> Result<Self> {
        let verifier = client_verifier(roots, mode, &self.state.provider)?;
        *self.state.client.write() = Some(ClientAuth { mode, verifier });
        Ok(self)
    }

    /// Serve `chain` with `key` on every new handshake. Fails, and keeps
    /// the current certificate, if `key` does not match the leaf.
    pub fn rotate(
        &self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<()> {
        let certified = certified_key(chain, key, &self.state.provider)?;
        *self.state.certified.write() = certified;
        Ok(())
    }

    /// Verify client certificates against `roots` on every new handshake.
    /// Fails if this `HubTls` does not ask for client certificates, or a
    /// root does not parse.
    pub fn set_client_roots(
        &self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Result<()> {
        let mut client = self.state.client.write();
        let Some(auth) = client.as_mut() else {
            bail!("set_client_roots needs client_certs first");
        };
        auth.verifier = client_verifier(roots, auth.mode, &self.state.provider)?;
        Ok(())
    }

    /// The rustls config of a listener. TLS 1.3 only; HTTP/1.1 by ALPN. No session resumption: a resumed session skips client
    /// certificate verification and keeps the first identity, so it would
    /// outlive `rotate` and `set_client_roots`.
    fn server_config(&self) -> Result<Arc<rustls::ServerConfig>> {
        let mut config = rustls::ServerConfig::builder_with_provider(self.state.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("failed to select TLS 1.3")?
            .with_client_cert_verifier(Arc::new(CurrentClientVerifier(self.state.clone())))
            .with_cert_resolver(Arc::new(CurrentCert(self.state.clone())));
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        // HTTP/1.1 only: the hub does not build axum's HTTP/2 support, so a
        // client that got `h2` would send a preface the server cannot read.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }
}

fn certified_key(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    provider: &CryptoProvider,
) -> Result<Arc<CertifiedKey>> {
    if chain.is_empty() {
        bail!("hub TLS needs a certificate chain");
    }
    CertifiedKey::from_der(chain, key, provider)
        .map(Arc::new)
        .context("the hub's private key does not load or does not match its certificate")
}

fn client_verifier(
    roots: impl IntoIterator<Item = CertificateDer<'static>>,
    mode: ClientCerts,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ClientCertVerifier>> {
    let mut store = RootCertStore::empty();
    for root in roots {
        store
            .add(root)
            .context("failed to add a client root certificate")?;
    }
    if store.is_empty() {
        bail!("client certificate verification needs at least one root");
    }
    let builder = WebPkiClientVerifier::builder_with_provider(Arc::new(store), provider.clone());
    let builder = match mode {
        ClientCerts::Required => builder,
        ClientCerts::Optional => builder.allow_unauthenticated(),
    };
    builder
        .build()
        .context("failed to build the client certificate verifier")
}

#[derive(Debug)]
struct CurrentCert(Arc<State>);

impl ResolvesServerCert for CurrentCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.certified.read().clone())
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("State").finish_non_exhaustive()
    }
}

/// Asks for a client certificate only when client certs are configured,
/// and verifies with the current roots. Read on every handshake, so
/// `client_certs` and `set_client_roots` take effect without a restart.
#[derive(Debug)]
struct CurrentClientVerifier(Arc<State>);

impl CurrentClientVerifier {
    fn current(&self) -> Option<Arc<dyn ClientCertVerifier>> {
        self.0
            .client
            .read()
            .as_ref()
            .map(|auth| auth.verifier.clone())
    }
}

impl ClientCertVerifier for CurrentClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.current().is_some()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.current()
            .is_some_and(|verifier| verifier.client_auth_mandatory())
    }

    /// No hints: a hint list borrowed from roots that can change cannot be
    /// returned. A client then sends the certificate it has.
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        match self.current() {
            Some(verifier) => verifier.verify_client_cert(end_entity, intermediates, now),
            // Not asked for, so a certificate here is unexpected; refuse it.
            None => Err(rustls::Error::General(
                "this listener does not take client certificates".into(),
            )),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0
            .provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// What a route learns about the connection it is served on: the client's
/// verified certificate, if it presented one.
#[derive(Clone, Debug)]
pub(crate) struct Connection {
    pub(crate) identity: Option<ClientIdentity>,
}

impl Connected<IncomingStream<'_, HubListener>> for Connection {
    fn connect_info(stream: IncomingStream<'_, HubListener>) -> Self {
        stream.remote_addr().clone()
    }
}

/// Most TLS connections a listener holds before axum takes them: in their
/// handshake, or done and waiting. At the limit it stops accepting, and the
/// kernel backlog queues the rest, as for a plain listener.
pub(crate) const MAX_PENDING_TLS: usize = 256;

/// A finished handshake, with the slot it holds until axum takes it.
type Ready = (
    TlsStream<TcpStream>,
    Connection,
    tokio::sync::OwnedSemaphorePermit,
);

/// A hub listener: plain TCP, or TCP with TLS.
///
/// The TLS listener owns its socket and accepts only while axum asks for a
/// connection, so it takes nothing before the hub serves, and its socket
/// closes when `axum::serve` drops it at shutdown. Each handshake runs in a
/// task of its own under [`HANDSHAKE_TIMEOUT`], so a client that sends
/// nothing does not hold up the next one.
pub(crate) enum HubListener {
    Plain(TcpListener),
    Tls(Box<TlsListener>),
}

pub(crate) struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    /// One per connection in its handshake or waiting for axum.
    slots: Arc<tokio::sync::Semaphore>,
    ready_tx: mpsc::Sender<Ready>,
    ready: mpsc::Receiver<Ready>,
}

impl HubListener {
    /// Wrap `listener`, with TLS if `tls` is given. Accepts nothing yet.
    pub(crate) fn new(listener: TcpListener, tls: Option<&HubTls>) -> Result<Self> {
        Self::with_limit(listener, tls, MAX_PENDING_TLS)
    }

    fn with_limit(listener: TcpListener, tls: Option<&HubTls>, limit: usize) -> Result<Self> {
        let Some(tls) = tls else {
            return Ok(Self::Plain(listener));
        };
        // As many as there are slots, so a finished handshake never waits
        // to send.
        let (ready_tx, ready) = mpsc::channel(limit);
        Ok(Self::Tls(Box::new(TlsListener {
            listener,
            acceptor: TlsAcceptor::from(tls.server_config()?),
            slots: Arc::new(tokio::sync::Semaphore::new(limit)),
            ready_tx,
            ready,
        })))
    }
}

impl TlsListener {
    async fn accept(&mut self) -> (TlsStream<TcpStream>, Connection) {
        let Self {
            listener,
            acceptor,
            slots,
            ready_tx,
            ready,
        } = self;
        loop {
            // Both arms are cancel-safe: a dropped slot future frees its
            // permit, and `TcpListener::accept` loses no connection.
            let accepted = tokio::select! {
                biased;
                done = ready.recv() => {
                    let (stream, connection, _slot) =
                        done.expect("the listener holds a sender");
                    return (stream, connection);
                }
                accepted = async {
                    let slot = Arc::clone(slots)
                        .acquire_owned()
                        .await
                        .expect("the semaphore is never closed");
                    (listener.accept().await, slot)
                } => accepted,
            };
            match accepted {
                (Ok((stream, _)), slot) => {
                    tokio::spawn(handshake(stream, acceptor.clone(), ready_tx.clone(), slot));
                }
                (Err(error), _) => accept_error(&error).await,
            }
        }
    }
}

/// One handshake, under [`HANDSHAKE_TIMEOUT`]. Ends early when the listener
/// is dropped. Holds `slot` until axum takes the connection.
async fn handshake(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    ready: mpsc::Sender<Ready>,
    slot: tokio::sync::OwnedSemaphorePermit,
) {
    let remote = stream.peer_addr().ok();
    let handshake = tokio::select! {
        _ = ready.closed() => return,
        handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)) => handshake,
    };
    match handshake {
        Ok(Ok(stream)) => {
            let identity = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(ClientIdentity::from_chain);
            // Capacity equals the slots, and this holds one: never waits.
            let _ = ready.try_send((stream, Connection { identity }, slot));
        }
        Ok(Err(error)) => tracing::debug!(?remote, %error, "hub TLS handshake failed"),
        Err(_) => tracing::debug!(?remote, "hub TLS handshake timed out"),
    }
}

/// As `axum::serve` does for its own listener: a connection error is the
/// peer's, so retry at once; anything else (out of file descriptors) waits
/// a second, so the loop does not spin.
async fn accept_error(error: &std::io::Error) {
    use std::io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if matches!(
        error.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    ) {
        return;
    }
    tracing::error!(%error, "hub listener accept failed");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

impl axum::serve::Listener for HubListener {
    type Io = Either<TcpStream, TlsStream<TcpStream>>;
    type Addr = Connection;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self {
            // axum's own listener, with its handling of accept errors.
            Self::Plain(listener) => {
                let (stream, _) = axum::serve::Listener::accept(listener).await;
                (Either::Left(stream), Connection { identity: None })
            }
            Self::Tls(tls) => {
                let (stream, connection) = tls.accept().await;
                (Either::Right(stream), connection)
            }
        }
    }

    /// axum reads this for its logs only; the hub reports its addresses
    /// itself.
    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(Connection { identity: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed certificate for 127.0.0.1, enough to build a listener.
    fn hub_tls() -> HubTls {
        let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        HubTls::new(
            vec![certified.cert.der().clone()],
            PrivateKeyDer::Pkcs8(certified.signing_key.serialize_der().into()),
        )
        .unwrap()
    }

    /// `axum::serve` drops its listener when it stops. The TLS listener's
    /// socket must close with it, before any other task runs: the hub's
    /// shutdown joins the server task and then returns, and a socket held by
    /// a detached task outlived it (for up to a second after an accept
    /// error, while that task slept).
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_a_tls_listener_closes_its_socket_at_once() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let listener = HubListener::new(tcp, Some(&hub_tls())).unwrap();
        tokio::task::yield_now().await;
        drop(listener);
        assert!(
            std::net::TcpListener::bind(addr).is_ok(),
            "the TLS listener's socket outlived the listener"
        );
    }

    /// A client that trusts any certificate, to finish handshakes.
    fn client() -> tokio_rustls::TlsConnector {
        #[derive(Debug)]
        struct Any;
        impl rustls::client::danger::ServerCertVerifier for Any {
            fn verify_server_cert(
                &self,
                _: &CertificateDer<'_>,
                _: &[CertificateDer<'_>],
                _: &rustls::pki_types::ServerName<'_>,
                _: &[u8],
                _: UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            fn verify_tls12_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }
            fn verify_tls13_signature(
                &self,
                _: &[u8],
                _: &CertificateDer<'_>,
                _: &DigitallySignedStruct,
            ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
            {
                Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
            }
            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                rustls::crypto::ring::default_provider()
                    .signature_verification_algorithms
                    .supported_schemes()
            }
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Any))
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    async fn connect(
        addr: std::net::SocketAddr,
    ) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let tcp = TcpStream::connect(addr).await?;
        client()
            .connect(
                rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap(),
                tcp,
            )
            .await
    }

    /// Connections in a handshake or waiting for axum are bounded. With a
    /// limit of 2, two clients that connect and send nothing hold both slots,
    /// so the listener accepts no third connection, and a real client waits
    /// in the kernel backlog. When one silent client leaves, its slot frees,
    /// and the real client gets through. Without the bound, every accepted
    /// socket held a task of its own, with no limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_tls_connections_are_bounded() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let HubListener::Tls(mut listener) =
            HubListener::with_limit(tcp, Some(&hub_tls()), 2).unwrap()
        else {
            panic!("a TLS listener");
        };
        let slots = Arc::clone(&listener.slots);
        // axum's loop: take every connection that is ready.
        let (taken_tx, taken) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await;
                let _ = taken_tx.send(stream);
            }
        });
        let silent_a = TcpStream::connect(addr).await.unwrap();
        let _silent_b = TcpStream::connect(addr).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while slots.available_permits() > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the silent clients take both slots");

        let real = tokio::spawn(connect(addr));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !real.is_finished(),
            "a handshake finished while two silent clients held every slot"
        );
        drop(silent_a);
        tokio::time::timeout(Duration::from_secs(5), real)
            .await
            .expect("the real client gets a slot once a silent one leaves")
            .unwrap()
            .unwrap();
        let mut taken = taken;
        assert!(taken.recv().await.is_some(), "axum got the real connection");
        server.abort();
    }
}
