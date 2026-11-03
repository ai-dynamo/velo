// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Authenticated QUIC: certificates from a private CA, verified in both
//! directions, and an authorization callback per connection.
//!
//! The default transport pins a self-signed certificate per transport, and
//! the listener does not authenticate the dialer. With [`QuicTls`], both ends
//! present a certificate chain from a configured CA and verify the other's in
//! the TLS handshake. The listener requires a client certificate.
//!
//! The application's [`QuicAuthorizer`] then sees the verified
//! [`PeerIdentity`] of each connection, inbound and dialed, on every lane,
//! before the transport reads or writes a frame on it. A refused connection
//! is closed with [`REFUSED`] and dispatches nothing. The messenger's
//! `_hello` cannot be this check: system handlers skip it, and a reverse
//! connection can begin with a response.
//!
//! Peers are identified by their certificate, not by their name: every
//! dialer presents the fixed server name `velo`, so the server certificate is
//! checked against the roots and not against a DNS name. A CA that issues
//! SPIFFE-style IDs (`spiffe://<deployment>/<class>/<cell>`) puts them in a
//! URI SAN, which [`PeerIdentity::uris`] returns.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use parking_lot::RwLock;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{ResolvesClientCert, verify_server_cert_signed_by_trust_anchor};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ParsedCertificate, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use tracing::warn;

use crate::InstanceId;

/// The QUIC application close code for a connection that the authorizer
/// refused. A dialer that sees it logs that the peer refused its identity.
pub const REFUSED: u32 = 0x1;

/// The verified identity of the peer on one connection.
#[derive(Clone, Debug)]
pub struct PeerIdentity {
    chain: Arc<[CertificateDer<'static>]>,
    uris: Arc<[String]>,
}

impl PeerIdentity {
    /// Read the identity from the certificate chain that the handshake
    /// verified, leaf first. `None` for an empty chain.
    fn from_chain(chain: Vec<CertificateDer<'static>>) -> Option<Self> {
        let leaf = chain.first()?;
        // The handshake already parsed and verified this certificate, so
        // parsing does not fail here; an unparsable one has no URIs.
        let uris = webpki::EndEntityCert::try_from(leaf)
            .map(|cert| cert.valid_uri_names().map(str::to_string).collect())
            .unwrap_or_default();
        Some(Self {
            chain: chain.into(),
            uris,
        })
    }

    /// The peer's leaf certificate, in DER.
    pub fn certificate(&self) -> &CertificateDer<'static> {
        &self.chain[0]
    }

    /// The chain that the peer presented, leaf first.
    pub fn chain(&self) -> &[CertificateDer<'static>] {
        &self.chain
    }

    /// The URI SANs of the leaf, such as `spiffe://deployment/class/cell`.
    pub fn uris(&self) -> &[String] {
        &self.uris
    }
}

/// Which connection an [`QuicAuthorizer`] decides on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionOrigin {
    /// A peer dialed this transport from `remote`.
    Accepted {
        /// The peer's UDP address.
        remote: SocketAddr,
    },
    /// This transport dialed `peer`, registered under that instance id, on
    /// `lane`. A health check dials lane 0.
    Dialed {
        /// The instance that discovery registered at this address.
        peer: InstanceId,
        /// The lane of the connection.
        lane: u16,
        /// The peer's UDP address.
        remote: SocketAddr,
    },
}

/// Decides whether a connection with a verified peer may carry frames.
///
/// It runs once per connection, after the TLS handshake and before the
/// transport reads or writes a frame on it, on the connection's task. It
/// must not block. A panic counts as a refusal.
pub trait QuicAuthorizer: Send + Sync + 'static {
    /// `true` accepts the connection.
    fn authorize(&self, peer: &PeerIdentity, origin: &ConnectionOrigin) -> bool;
}

impl<F> QuicAuthorizer for F
where
    F: Fn(&PeerIdentity, &ConnectionOrigin) -> bool + Send + Sync + 'static,
{
    fn authorize(&self, peer: &PeerIdentity, origin: &ConnectionOrigin) -> bool {
        self(peer, origin)
    }
}

/// TLS for a QUIC transport whose peers hold certificates from a private CA.
///
/// Pass it to `QuicTransportBuilder::tls`. Every transport both dials and
/// listens, so each leaf certificate needs both the `serverAuth` and the
/// `clientAuth` extended key usage.
///
/// Clones share their state: keep one to rotate the certificate or the roots
/// while the transport runs. New handshakes use the new values; connections
/// already open keep the identity they authenticated with.
#[derive(Clone)]
pub struct QuicTls {
    state: Arc<TlsState>,
    authorizer: Option<Arc<dyn QuicAuthorizer>>,
}

struct TlsState {
    provider: Arc<CryptoProvider>,
    certified: RwLock<Arc<CertifiedKey>>,
    /// One lock for both directions: two `set_roots` calls at once must not
    /// leave the dialer trusting one set and the listener another.
    trust: RwLock<Arc<Trust>>,
}

/// The roots, and the client verifier built from them.
struct Trust {
    roots: Arc<RootCertStore>,
    client_verifier: Arc<dyn ClientCertVerifier>,
}

impl Trust {
    fn new(
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
        provider: &Arc<CryptoProvider>,
    ) -> Result<Arc<Self>> {
        let roots = root_store(roots)?;
        let client_verifier = client_verifier(&roots, provider)?;
        Ok(Arc::new(Self {
            roots,
            client_verifier,
        }))
    }
}

impl fmt::Debug for TlsState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsState")
            .field("roots", &self.trust.read().roots.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for QuicTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicTls")
            .field("state", &self.state)
            .field("authorizer", &self.authorizer.is_some())
            .finish()
    }
}

impl QuicTls {
    /// Trust the CA certificates in `roots`, and present `chain` (leaf
    /// first) with its private `key`. Fails if a root does not parse, there
    /// is no root, or `key` does not match the leaf.
    pub fn new(
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let trust = Trust::new(roots, &provider)?;
        let certified = certified_key(chain, key, &provider)?;
        Ok(Self {
            state: Arc::new(TlsState {
                provider,
                certified: RwLock::new(certified),
                trust: RwLock::new(trust),
            }),
            authorizer: None,
        })
    }

    /// Decide on each connection after its handshake. Without an authorizer,
    /// every peer with a certificate from the roots is accepted.
    pub fn authorizer(mut self, authorizer: impl QuicAuthorizer) -> Self {
        self.authorizer = Some(Arc::new(authorizer));
        self
    }

    /// Present `chain` with `key` on every new handshake. Fails, and keeps
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

    /// Trust `roots` on every new handshake, in both directions. Fails, and
    /// keeps the current roots, if a root does not parse or there is none.
    pub fn set_roots(
        &self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Result<()> {
        *self.state.trust.write() = Trust::new(roots, &self.state.provider)?;
        Ok(())
    }

    /// The leaf certificate that new handshakes present.
    pub fn certificate(&self) -> CertificateDer<'static> {
        self.state.certified.read().cert[0].clone()
    }

    /// The listener's rustls config: requires a client certificate.
    ///
    /// No 0-RTT (`max_early_data_size` stays 0): early data would arrive
    /// before the client certificate is verified. No session resumption: a
    /// resumed session skips certificate verification and carries the
    /// identity of the session it resumes, so it would outlive `rotate` and
    /// `set_roots`.
    pub(super) fn server_crypto(&self, alpn: &[u8]) -> Result<rustls::ServerConfig> {
        let mut crypto = rustls::ServerConfig::builder_with_provider(self.state.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("failed to select TLS 1.3")?
            .with_client_cert_verifier(Arc::new(CurrentClientVerifier(self.state.clone())))
            .with_cert_resolver(Arc::new(CurrentCert(self.state.clone())));
        crypto.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        crypto.send_tls13_tickets = 0;
        crypto.alpn_protocols = vec![alpn.to_vec()];
        Ok(crypto)
    }

    /// The dialer's rustls config: verifies the server against the roots
    /// and presents the current certificate. No session resumption, for the
    /// reason given on `server_crypto`.
    pub(super) fn client_crypto(&self, alpn: &[u8]) -> Result<rustls::ClientConfig> {
        let mut crypto = rustls::ClientConfig::builder_with_provider(self.state.provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("failed to select TLS 1.3")?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(RootsServerVerifier(self.state.clone())))
            .with_client_cert_resolver(Arc::new(CurrentCert(self.state.clone())));
        crypto.resumption = rustls::client::Resumption::disabled();
        crypto.alpn_protocols = vec![alpn.to_vec()];
        Ok(crypto)
    }

    /// Run the authorizer on a connection whose handshake completed. Logs a
    /// refusal. Fails closed: no certificate, or a panic, is a refusal.
    pub(super) fn admit(&self, connection: &quinn::Connection, origin: ConnectionOrigin) -> bool {
        let Some(peer) = connection
            .peer_identity()
            .and_then(|any| any.downcast::<Vec<CertificateDer<'static>>>().ok())
            .and_then(|chain| PeerIdentity::from_chain(*chain))
        else {
            warn!(
                ?origin,
                "QUIC: refused a connection without a peer certificate"
            );
            return false;
        };
        let Some(authorizer) = &self.authorizer else {
            return true;
        };
        let accepted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            authorizer.authorize(&peer, &origin)
        }))
        .unwrap_or_else(|_| {
            warn!(
                ?origin,
                "QUIC: the authorizer panicked; refusing the connection"
            );
            false
        });
        if !accepted {
            warn!(?origin, uris = ?peer.uris(), "QUIC: the authorizer refused a peer");
        }
        accepted
    }
}

fn root_store(
    roots: impl IntoIterator<Item = CertificateDer<'static>>,
) -> Result<Arc<RootCertStore>> {
    let mut store = RootCertStore::empty();
    for root in roots {
        store
            .add(root)
            .context("failed to add a QUIC root certificate")?;
    }
    if store.is_empty() {
        bail!("QUIC TLS needs at least one root certificate");
    }
    Ok(Arc::new(store))
}

fn certified_key(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    provider: &CryptoProvider,
) -> Result<Arc<CertifiedKey>> {
    if chain.is_empty() {
        bail!("QUIC TLS needs a certificate chain");
    }
    CertifiedKey::from_der(chain, key, provider)
        .map(Arc::new)
        .context("the QUIC private key does not load or does not match the certificate")
}

fn client_verifier(
    roots: &Arc<RootCertStore>,
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<dyn ClientCertVerifier>> {
    WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
        .build()
        .context("failed to build the QUIC client certificate verifier")
}

/// Presents the current certificate, on both sides of the handshake.
#[derive(Debug)]
struct CurrentCert(Arc<TlsState>);

impl ResolvesServerCert for CurrentCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.certified.read().clone())
    }
}

impl ResolvesClientCert for CurrentCert {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.0.certified.read().clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// Verifies a client chain against the current roots. Client auth is
/// mandatory.
#[derive(Debug)]
struct CurrentClientVerifier(Arc<TlsState>);

impl ClientCertVerifier for CurrentClientVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    /// No hints. The roots can change, and a hint list borrowed from them
    /// cannot. Each transport holds one certificate, so it has nothing to
    /// choose between.
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let trust = self.0.trust.read().clone();
        trust
            .client_verifier
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
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
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
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

/// Verifies a server chain against the current roots, with the
/// `serverAuth` usage, and not against a name: every dialer presents the
/// name `velo`, and the authorizer decides who the peer is.
#[derive(Debug)]
struct RootsServerVerifier(Arc<TlsState>);

impl ServerCertVerifier for RootsServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        let trust = self.0.trust.read().clone();
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &trust.roots,
            intermediates,
            now,
            self.0.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
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
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
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

/// A private CA and the leaf certificates it issues, for tests.
#[cfg(test)]
pub(crate) mod test_ca {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose, SanType,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    use super::QuicTls;

    pub(crate) struct Ca {
        issuer: CertifiedIssuer<'static, KeyPair>,
    }

    /// A leaf chain (leaf, then the CA) and its key.
    pub(crate) struct Leaf {
        pub(crate) chain: Vec<CertificateDer<'static>>,
        pub(crate) key: PrivateKeyDer<'static>,
    }

    impl Ca {
        pub(crate) fn new() -> Self {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params
                .distinguished_name
                .push(DnType::CommonName, "velo test CA");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            let issuer =
                CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
            Self { issuer }
        }

        pub(crate) fn root(&self) -> CertificateDer<'static> {
            self.issuer.der().clone()
        }

        /// A leaf with `uri` as its only SAN, valid for both server and
        /// client authentication, as a transport needs.
        pub(crate) fn leaf(&self, uri: &str) -> Leaf {
            let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
            params.subject_alt_names = vec![SanType::URI(uri.try_into().unwrap())];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &self.issuer).unwrap();
            Leaf {
                chain: vec![cert.der().clone(), self.root()],
                key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
            }
        }

        /// TLS for a transport that trusts this CA and presents a leaf for
        /// `uri`.
        pub(crate) fn tls(&self, uri: &str) -> QuicTls {
            let leaf = self.leaf(uri);
            QuicTls::new([self.root()], leaf.chain, leaf.key).unwrap()
        }
    }
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
