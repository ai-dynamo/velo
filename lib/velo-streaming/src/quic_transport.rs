// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC-based [`FrameTransport`] implementation for dedicated per-stream connections.
//!
//! [`QuicFrameTransport`] gives each stream its own QUIC connection with a
//! token-based handshake for validation. QUIC provides TLS 1.3 encryption,
//! multiplexed streams, and built-in congestion control.
//!
//! TLS configuration is reused from [`velo_transports::quic::tls`], which
//! provides self-signed certificate generation and QUIC endpoint configuration.
//!
//! # Endpoint Format
//!
//! ```text
//! quic://{ip}:{port}/{uuid_token}           (IPv4)
//! quic://[{ip}]:{port}/{uuid_token}         (IPv6)
//! ```
//!
//! # Connection Lifecycle
//!
//! 1. **bind()**: Creates a QUIC endpoint on an ephemeral UDP port, generates a
//!    UUID token, spawns an accept-once task, and returns the endpoint + receiver.
//! 2. **connect()**: Parses the endpoint, connects via QUIC, opens a bidirectional
//!    stream, sends the 16-byte token for validation, and spawns a pump task that
//!    bridges the flume channel to the QUIC stream using `TcpFrameCodec`.
//! 3. **Accept-once**: The listener validates the token, accepts exactly one
//!    connection, then closes. Invalid tokens are rejected; the listener
//!    continues waiting (up to a 30-second timeout).

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio_util::codec::Framed;
use uuid::Uuid;
use velo_transports::MessageType;
use velo_transports::tcp::TcpFrameCodec;

use crate::transport::FrameTransport;

/// Timeout for the accept-once listener to receive a valid connection.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(30);

/// QUIC-based [`FrameTransport`] providing dedicated per-stream connections.
///
/// Each `bind()` call creates an independent QUIC endpoint on an ephemeral UDP
/// port with a unique UUID token and its own self-signed TLS certificate.
/// The corresponding `connect()` establishes the QUIC connection, validates via
/// the token handshake, and returns a `flume::Sender` for frame delivery.
pub struct QuicFrameTransport {
    bind_addr: IpAddr,
}

impl QuicFrameTransport {
    /// Create a new `QuicFrameTransport` with the given bind address.
    pub fn new(bind_addr: IpAddr) -> Self {
        Self { bind_addr }
    }
}

impl Default for QuicFrameTransport {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        }
    }
}

/// Parse a `quic://` endpoint into `(SocketAddr, Uuid)`.
///
/// Expected format: `quic://{host}:{port}/{uuid_token}`
pub fn parse_quic_endpoint(endpoint: &str) -> Result<(std::net::SocketAddr, Uuid)> {
    let stripped = endpoint
        .strip_prefix("quic://")
        .ok_or_else(|| anyhow::anyhow!("missing quic:// prefix: {}", endpoint))?;
    let slash_pos = stripped
        .rfind('/')
        .ok_or_else(|| anyhow::anyhow!("missing token in endpoint: {}", endpoint))?;
    let addr_str = &stripped[..slash_pos];
    let token_str = &stripped[slash_pos + 1..];
    let addr: std::net::SocketAddr = addr_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid address '{}': {}", addr_str, e))?;
    let token: Uuid = token_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid token '{}': {}", token_str, e))?;
    Ok((addr, token))
}

/// Check if the given raw frame bytes represent a terminal sentinel.
fn is_terminal_sentinel(bytes: &[u8]) -> bool {
    use crate::sender::{cached_detached, cached_dropped, cached_finalized};

    if bytes == cached_dropped().as_slice()
        || bytes == cached_detached().as_slice()
        || bytes == cached_finalized().as_slice()
    {
        return true;
    }

    if let Ok(frame) = rmp_serde::from_slice::<crate::frame::StreamFrame<()>>(bytes) {
        matches!(frame, crate::frame::StreamFrame::TransportError(_))
    } else {
        false
    }
}

/// A combined read/write wrapper around QUIC's separate send and recv streams.
///
/// This allows using `tokio_util::codec::Framed` which requires a single
/// `AsyncRead + AsyncWrite` type.
struct QuicBiStream {
    send: quinn::SendStream,
    recv: tokio::io::BufReader<quinn::RecvStream>,
}

impl tokio::io::AsyncRead for QuicBiStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for QuicBiStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        <quinn::SendStream as tokio::io::AsyncWrite>::poll_write(
            std::pin::Pin::new(&mut self.send),
            cx,
            buf,
        )
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as tokio::io::AsyncWrite>::poll_flush(
            std::pin::Pin::new(&mut self.send),
            cx,
        )
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        <quinn::SendStream as tokio::io::AsyncWrite>::poll_shutdown(
            std::pin::Pin::new(&mut self.send),
            cx,
        )
    }
}

impl FrameTransport for QuicFrameTransport {
    fn bind(
        &self,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, Result<(String, flume::Receiver<Vec<u8>>)>> {
        let bind_addr = self.bind_addr;
        Box::pin(async move {
            // Install the rustls crypto provider (no-op if already installed)
            let _ = rustls::crypto::ring::default_provider().install_default();

            // Generate self-signed TLS cert using the shared utility
            let (certs, key) = velo_transports::quic::tls::generate_self_signed_cert()?;
            let server_config = velo_transports::quic::tls::make_server_config(certs, key)?;

            // Create QUIC endpoint on ephemeral UDP port
            let bind_socket_addr = std::net::SocketAddr::new(bind_addr, 0);
            let endpoint = quinn::Endpoint::server(server_config, bind_socket_addr)?;
            let port = endpoint.local_addr()?.port();

            let token = Uuid::new_v4();

            // Frame delivery channel
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);

            // Resolve unspecified bind addresses to routable loopback
            let advertise_ip = crate::util::resolve_advertise_ip(bind_addr);
            let sock_addr = std::net::SocketAddr::new(advertise_ip, port);
            let endpoint_str = format!("quic://{}/{}", sock_addr, token);

            // Spawn accept-once task
            let expected_token = token;
            tokio::spawn(async move {
                let accept_result = tokio::time::timeout(ACCEPT_TIMEOUT, async {
                    loop {
                        match endpoint.accept().await {
                            Some(connecting) => {
                                let connection = match connecting.await {
                                    Ok(conn) => conn,
                                    Err(e) => {
                                        tracing::warn!(
                                            "QUIC streaming handshake failed: {}",
                                            e
                                        );
                                        continue;
                                    }
                                };

                                let (send, recv) = match connection.accept_bi().await {
                                    Ok(streams) => streams,
                                    Err(e) => {
                                        tracing::warn!(
                                            "QUIC streaming: failed to accept bi stream: {}",
                                            e
                                        );
                                        continue;
                                    }
                                };

                                // Read 16-byte token
                                let mut token_buf = [0u8; 16];
                                let mut recv_reader = tokio::io::BufReader::new(recv);
                                if let Err(e) =
                                    tokio::io::AsyncReadExt::read_exact(&mut recv_reader, &mut token_buf)
                                        .await
                                {
                                    tracing::warn!(
                                        "QUIC streaming handshake read failed: {}",
                                        e
                                    );
                                    continue;
                                }

                                let received_token = Uuid::from_bytes(token_buf);
                                if received_token != expected_token {
                                    tracing::warn!(
                                        "QUIC streaming handshake: invalid token (expected {}, got {})",
                                        expected_token,
                                        received_token
                                    );
                                    continue;
                                }

                                // Valid connection
                                return Some((send, recv_reader));
                            }
                            None => {
                                tracing::warn!("QUIC streaming: endpoint closed");
                                return None;
                            }
                        }
                    }
                })
                .await;

                let (send, recv_reader) = match accept_result {
                    Ok(Some(streams)) => streams,
                    Ok(None) => {
                        tracing::warn!("QUIC streaming: accept failed, closing channel");
                        return;
                    }
                    Err(_timeout) => {
                        tracing::warn!(
                            "QUIC streaming: accept timed out after {:?}",
                            ACCEPT_TIMEOUT
                        );
                        return;
                    }
                };

                // Read frames via TcpFrameCodec and forward to frame_tx
                let rw = QuicBiStream {
                    send,
                    recv: recv_reader,
                };
                let mut framed = Framed::new(rw, TcpFrameCodec::new());
                let mut last_was_terminal = false;

                while let Some(result) = framed.next().await {
                    match result {
                        Ok((_msg_type, _header, payload)) => {
                            let payload_vec = payload.to_vec();
                            last_was_terminal = is_terminal_sentinel(&payload_vec);
                            if frame_tx.send_async(payload_vec).await.is_err() {
                                break; // consumer dropped
                            }
                        }
                        Err(e) => {
                            tracing::warn!("QUIC streaming read error: {}", e);
                            break;
                        }
                    }
                }

                // If no terminal sentinel was the last frame, inject Dropped
                if !last_was_terminal {
                    let _ = frame_tx
                        .send_async(crate::sender::cached_dropped().clone())
                        .await;
                }
            });

            Ok((endpoint_str, frame_rx))
        })
    }

    fn connect(
        &self,
        endpoint: &str,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let endpoint_str = endpoint.to_string();
        Box::pin(async move {
            let (addr, token) = parse_quic_endpoint(&endpoint_str)?;

            // Install the rustls crypto provider (no-op if already installed)
            let _ = rustls::crypto::ring::default_provider().install_default();

            let client_config = velo_transports::quic::tls::make_client_config_insecure()?;

            // Create a client-only QUIC endpoint, matching address family to remote
            let bind_addr: std::net::SocketAddr = if addr.is_ipv6() {
                "[::]:0".parse()?
            } else {
                "0.0.0.0:0".parse()?
            };
            let mut client_endpoint = quinn::Endpoint::client(bind_addr)?;
            client_endpoint.set_default_client_config(client_config);

            // Connect to the server
            let connection = client_endpoint.connect(addr, "localhost")?.await?;

            // Open a bidirectional stream
            let (mut send, _recv) = connection.open_bi().await?;

            // Send 16-byte token for handshake validation
            tokio::io::AsyncWriteExt::write_all(&mut send, token.as_bytes()).await?;

            // Create channel for frame pump
            let (tx, rx) = flume::bounded::<Vec<u8>>(256);

            // Spawn pump task: reads from channel, writes to QUIC stream.
            // Move connection and client_endpoint into the task to keep them alive.
            tokio::spawn(async move {
                let _client_endpoint = client_endpoint;

                while let Ok(frame_bytes) = rx.recv_async().await {
                    if let Err(e) = TcpFrameCodec::encode_frame(
                        &mut send,
                        MessageType::Message,
                        &[], // empty header for streaming
                        &frame_bytes,
                    )
                    .await
                    {
                        tracing::error!("QUIC streaming write error: {}", e);
                        break;
                    }
                }

                // Finish the send stream (queues FIN) and drop it
                if let Err(e) = send.finish() {
                    tracing::debug!("QUIC streaming finish on close: {}", e);
                }
                drop(send);

                // Keep the connection alive until the peer closes it (after reading
                // the FIN and all buffered data). Without this, dropping the
                // connection immediately can race with QUIC delivering the final
                // packets, causing data loss on the receiver side.
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(5), connection.closed())
                        .await;
            });

            Ok(tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind_returns_quic_endpoint() {
        let transport = QuicFrameTransport::default();
        let (endpoint, _rx) = transport.bind(1, 0).await.unwrap();

        assert!(
            endpoint.starts_with("quic://"),
            "endpoint should start with quic://: {}",
            endpoint
        );

        let (addr, token) = parse_quic_endpoint(&endpoint).unwrap();
        assert!(addr.port() > 0, "port should be non-zero");
        assert_ne!(token, Uuid::nil(), "token should not be nil UUID");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect_handshake() {
        let transport = QuicFrameTransport::default();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();

        let frame_bytes = rmp_serde::to_vec(&crate::frame::StreamFrame::<String>::Item(
            "hello world".to_string(),
        ))
        .unwrap();

        sender.send_async(frame_bytes.clone()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout waiting for frame")
            .expect("channel closed unexpectedly");

        assert_eq!(
            received, frame_bytes,
            "received frame should match sent frame"
        );

        drop(sender);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect_round_trip_multiple_frames() {
        let transport = QuicFrameTransport::default();
        let (endpoint, rx) = transport.bind(42, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 42, 1).await.unwrap();

        let mut expected_frames = Vec::new();
        for i in 0..10 {
            let frame = rmp_serde::to_vec(&crate::frame::StreamFrame::<u32>::Item(i)).unwrap();
            expected_frames.push(frame.clone());
            sender.send_async(frame).await.unwrap();
        }

        for (i, expected) in expected_frames.iter().enumerate() {
            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for frame {}", i))
                .unwrap_or_else(|_| panic!("channel closed at frame {}", i));
            assert_eq!(&received, expected, "frame {} mismatch", i);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_invalid_token_rejected() {
        let transport = QuicFrameTransport::default();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        let (addr, _valid_token) = parse_quic_endpoint(&endpoint).unwrap();

        // Install crypto provider and create client config
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client_config = velo_transports::quic::tls::make_client_config_insecure().unwrap();
        let mut client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
        client_endpoint.set_default_client_config(client_config);

        // Connect with wrong token
        let connection = client_endpoint
            .connect(addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        let wrong_token = Uuid::new_v4();
        tokio::io::AsyncWriteExt::write_all(&mut send, wrong_token.as_bytes())
            .await
            .unwrap();

        // Should timeout (no frames from invalid token connection)
        let result = tokio::time::timeout(Duration::from_millis(500), rx.recv_async()).await;
        assert!(
            result.is_err(),
            "should not receive frames from invalid token connection"
        );

        drop(send);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dropped_sentinel_injected_on_abrupt_close() {
        let transport = QuicFrameTransport::default();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();
        let frame = rmp_serde::to_vec(&crate::frame::StreamFrame::<String>::Item(
            "data".to_string(),
        ))
        .unwrap();
        sender.send_async(frame.clone()).await.unwrap();

        // Drop sender to trigger abrupt close
        drop(sender);

        // Should receive the data frame first
        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(received, frame);

        // Then the injected Dropped sentinel
        let sentinel = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout waiting for Dropped sentinel")
            .expect("channel closed before Dropped sentinel");

        assert_eq!(
            sentinel.as_slice(),
            crate::sender::cached_dropped().as_slice(),
            "should receive Dropped sentinel after abrupt close"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_extra_dropped_after_finalized() {
        let transport = QuicFrameTransport::default();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();

        let finalized = rmp_serde::to_vec(&crate::frame::StreamFrame::<()>::Finalized).unwrap();
        sender.send_async(finalized.clone()).await.unwrap();

        drop(sender);

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(received, finalized);

        // Should NOT receive an extra Dropped sentinel
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv_async()).await;
        match result {
            Ok(Ok(extra)) => {
                assert_ne!(
                    extra.as_slice(),
                    crate::sender::cached_dropped().as_slice(),
                    "should not inject Dropped after Finalized"
                );
            }
            Ok(Err(_)) => {} // channel closed -- expected
            Err(_) => {}     // timeout -- also fine
        }
    }

    #[test]
    fn test_parse_quic_endpoint_valid() {
        let endpoint = "quic://127.0.0.1:8080/550e8400-e29b-41d4-a716-446655440000";
        let (addr, token) = parse_quic_endpoint(endpoint).unwrap();
        assert_eq!(addr.port(), 8080);
        assert_eq!(token.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_parse_quic_endpoint_invalid_no_prefix() {
        assert!(parse_quic_endpoint("http://127.0.0.1:8080/token").is_err());
    }

    #[test]
    fn test_parse_quic_endpoint_invalid_no_token() {
        assert!(parse_quic_endpoint("quic://127.0.0.1:8080").is_err());
    }

    #[test]
    fn test_parse_quic_endpoint_invalid_bad_uuid() {
        assert!(parse_quic_endpoint("quic://127.0.0.1:8080/not-a-uuid").is_err());
    }

    #[test]
    fn test_parse_quic_endpoint_invalid_bad_addr() {
        assert!(
            parse_quic_endpoint("quic://not-an-addr/550e8400-e29b-41d4-a716-446655440000").is_err()
        );
    }

    #[test]
    fn test_parse_quic_endpoint_ipv6() {
        let endpoint = "quic://[::1]:8080/550e8400-e29b-41d4-a716-446655440000";
        let (addr, token) = parse_quic_endpoint(endpoint).unwrap();
        assert_eq!(addr, "[::1]:8080".parse::<std::net::SocketAddr>().unwrap());
        assert_eq!(token.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind_unspecified_resolves_to_loopback() {
        let transport = QuicFrameTransport::default(); // 0.0.0.0
        let (endpoint, _rx) = transport.bind(1, 0).await.unwrap();
        let (addr, _token) = parse_quic_endpoint(&endpoint).unwrap();
        assert!(
            addr.ip().is_loopback(),
            "unspecified bind should resolve to loopback in endpoint: {}",
            endpoint
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_ipv6_round_trip() {
        // Skip if IPv6 is not available (CI runners, containers without IPv6)
        if std::net::UdpSocket::bind(std::net::SocketAddr::from((
            std::net::Ipv6Addr::LOCALHOST,
            0,
        )))
        .is_err()
        {
            eprintln!("Skipping test_ipv6_round_trip: IPv6 not available");
            return;
        }

        let transport = QuicFrameTransport::new(std::net::Ipv6Addr::LOCALHOST.into());
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        // Verify endpoint uses bracketed IPv6
        assert!(
            endpoint.contains("[::1]"),
            "IPv6 endpoint must bracket the address: {}",
            endpoint
        );

        // Connect and round-trip a frame
        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();
        let frame_bytes = rmp_serde::to_vec(&crate::frame::StreamFrame::<String>::Item(
            "ipv6 hello".to_string(),
        ))
        .unwrap();

        sender.send_async(frame_bytes.clone()).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout waiting for frame")
            .expect("channel closed unexpectedly");

        assert_eq!(received, frame_bytes);
        drop(sender);
    }
}
