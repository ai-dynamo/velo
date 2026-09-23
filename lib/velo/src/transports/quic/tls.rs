// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TLS for the QUIC transport: a self-signed certificate per transport, pinned
//! by its SHA-256 fingerprint.
//!
//! QUIC requires TLS 1.3. A cluster has no CA for its workers, so each
//! transport makes its own certificate and puts the fingerprint in its
//! `WorkerAddress` entry. A dialer accepts only the certificate that the
//! address named, and still checks the TLS 1.3 handshake signature. That
//! rejects a different listener on a reused port, and a peer that holds no
//! matching key. Skipping verification entirely would accept both.

use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

/// ALPN protocol id. A peer that speaks another protocol fails the handshake.
pub(super) const ALPN: &[u8] = b"velo-quic/1";

/// The server name every dialer presents. The pinned fingerprint, not the
/// name, is what identifies the peer.
pub(crate) const SERVER_NAME: &str = "velo";

/// A SHA-256 certificate fingerprint.
pub type Fingerprint = [u8; 32];

/// A transport's certificate and key, and the fingerprint that peers pin.
pub(crate) struct Identity {
    pub(super) cert: CertificateDer<'static>,
    pub(super) key: PrivatePkcs8KeyDer<'static>,
    pub(crate) fingerprint: Fingerprint,
}

impl Identity {
    /// Generate a fresh self-signed certificate.
    pub(crate) fn generate() -> Result<Self> {
        let certified = rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()])
            .context("failed to generate the QUIC certificate")?;
        let cert = certified.cert.der().clone();
        let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let fingerprint = fingerprint(&cert);
        Ok(Self {
            cert,
            key,
            fingerprint,
        })
    }
}

/// SHA-256 over the DER certificate.
pub(super) fn fingerprint(cert: &CertificateDer<'_>) -> Fingerprint {
    let digest = ring::digest::digest(&ring::digest::SHA256, cert.as_ref());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build the rustls server config for `identity`, with the velo ALPN.
pub(crate) fn server_crypto(identity: &Identity) -> Result<rustls::ServerConfig> {
    let mut crypto = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("failed to select TLS 1.3")?
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.clone()],
            PrivateKeyDer::Pkcs8(identity.key.clone_key()),
        )
        .context("failed to load the QUIC certificate")?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    Ok(crypto)
}

/// Build a quinn client config that accepts only the certificate with
/// `expected` as its SHA-256 fingerprint.
pub fn pinned_client_config(expected: Fingerprint) -> Result<quinn::ClientConfig> {
    let provider = provider();
    let verifier = Arc::new(PinnedVerifier {
        expected,
        algorithms: provider.signature_verification_algorithms,
    });
    let mut crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("failed to select TLS 1.3")?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    let quic = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .context("failed to build the QUIC client TLS config")?;
    Ok(quinn::ClientConfig::new(Arc::new(quic)))
}

/// Accepts exactly one certificate, by fingerprint, and verifies the
/// handshake signature against it.
#[derive(Debug)]
struct PinnedVerifier {
    expected: Fingerprint,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if fingerprint(end_entity) == self.expected {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pinned_verifier_accepts_only_its_certificate() {
        let ours = Identity::generate().unwrap();
        let other = Identity::generate().unwrap();
        let verifier = PinnedVerifier {
            expected: ours.fingerprint,
            algorithms: provider().signature_verification_algorithms,
        };
        let name = ServerName::try_from(SERVER_NAME).unwrap();
        assert!(
            verifier
                .verify_server_cert(&ours.cert, &[], &name, &[], UnixTime::now())
                .is_ok()
        );
        assert!(
            verifier
                .verify_server_cert(&other.cert, &[], &name, &[], UnixTime::now())
                .is_err(),
            "a certificate with another fingerprint must be refused"
        );
    }
}
