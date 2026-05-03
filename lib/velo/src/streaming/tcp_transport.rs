// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TCP-based [`FrameTransport`] implementation backed by a single shared listener.
//!
//! [`TcpFrameTransport`] opens **one** TCP listener at construction time and
//! demuxes incoming connections to per-stream readers via a 16-byte UUID token
//! sent on the wire by the connecting peer. This bypasses the AM (Active
//! Message) fire-and-forget overhead of
//! [`crate::streaming::velo_transport::VeloFrameTransport`] without the per-stream
//! listener / port / fd cost of an accept-once design.
//!
//! # Endpoint Format
//!
//! ```text
//! tcp://{ip}:{port}/{uuid_token}           (IPv4)
//! tcp://[{ip}]:{port}/{uuid_token}         (IPv6)
//! ```
//!
//! The `{ip}:{port}` is the shared listener address (identical for all streams
//! served by a single transport instance); the `{uuid_token}` identifies which
//! stream the connecting socket belongs to.
//!
//! # Connection Lifecycle
//!
//! 1. **`new()`**: Binds one [`TcpListener`] on the given IP + ephemeral port
//!    and spawns a single accept loop that demuxes connections by token.
//! 2. **`bind(anchor, session)`**: Allocates a fresh UUID, registers a frame
//!    sender keyed by that UUID, returns the shared endpoint + the matching
//!    receiver. No new listener, no new accept task.
//! 3. **`connect(endpoint)`**: Parses the endpoint, opens a TCP connection to
//!    the shared listener, writes the 16-byte token, and pumps frames.
//! 4. **Accept loop**: For each incoming socket, reads the token, looks up the
//!    registered frame sender, and pumps the framed stream to it. Unknown
//!    tokens are dropped with a warning. Tokens registered but never connected
//!    expire after [`ACCEPT_TIMEOUT`] and free their slot.
//!
//! # Socket Configuration
//!
//! All sockets are configured with TCP_NODELAY, keepalive (60s time / 10s
//! interval), and 1MB send/recv buffers, matching the `TcpTransport` pattern
//! from `velo-transports`.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::transports::MessageType;
use crate::transports::tcp::TcpFrameCodec;
use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::streaming::transport::FrameTransport;

/// Maximum time a registered token may sit unused before its slot is freed.
/// After expiry, an arriving connection with the token is treated as unknown.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum time the accept loop will wait for a connecting peer to send its
/// 16-byte token. Bounds attacker-induced fd holding from open-but-silent
/// connections.
const TOKEN_READ_TIMEOUT: Duration = Duration::from_secs(20);

/// One slot in the per-transport token registry. The deliverer-side
/// [`flume::Sender`] is removed from the map and given to the per-stream
/// reader task on a successful handshake.
struct PendingStream {
    frame_tx: flume::Sender<Vec<u8>>,
}

type TokenRegistry = DashMap<Uuid, PendingStream>;

/// TCP-based [`FrameTransport`] backed by a single shared listener.
///
/// One listener + one accept loop per transport instance, regardless of how
/// many concurrent streams are active. `bind()` is O(1): it allocates a token,
/// registers a sender, and returns; no per-stream port, listener, or accept
/// task is created.
pub struct TcpFrameTransport {
    advertise_addr: SocketAddr,
    registry: Arc<TokenRegistry>,
    /// Cancellation handle for the accept loop. Tripped on `Drop` so the
    /// spawned accept task exits, dropping its `Arc<TcpListener>` clone and
    /// allowing the kernel to release the bound port. Without this, dropping
    /// the transport would leak the listener for the lifetime of the process.
    cancel: CancellationToken,
}

impl TcpFrameTransport {
    /// Bind a single TCP listener on `(bind_addr, 0)` and spawn the accept
    /// loop. Returns an [`Arc<Self>`] because the accept loop holds clones of
    /// the registry; sharing the transport via [`Arc`] lets callers continue
    /// using it after construction.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the listener cannot be opened (port exhaustion, bind
    /// permission, etc.).
    pub async fn new(bind_addr: IpAddr) -> Result<Arc<Self>> {
        let listener = TcpListener::bind((bind_addr, 0u16)).await?;
        let local_port = listener.local_addr()?.port();
        let advertise_ip = crate::streaming::util::resolve_advertise_ip(bind_addr);
        let advertise_addr = SocketAddr::new(advertise_ip, local_port);

        let registry: Arc<TokenRegistry> = Arc::new(DashMap::new());
        let cancel = CancellationToken::new();

        tokio::spawn(run_accept_loop(
            Arc::new(listener),
            registry.clone(),
            cancel.clone(),
        ));

        Ok(Arc::new(Self {
            advertise_addr,
            registry,
            cancel,
        }))
    }

    /// Bind on `0.0.0.0:0` (loopback-resolved for the advertised address).
    pub async fn default_bound() -> Result<Arc<Self>> {
        Self::new(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)).await
    }
}

impl Drop for TcpFrameTransport {
    fn drop(&mut self) {
        // Stop the accept loop so it drops its listener clone; the kernel
        // releases the bound port once all clones are gone (in-flight
        // per-connection tasks may briefly outlive Drop, which is fine).
        self.cancel.cancel();
    }
}

/// Single accept loop per transport. Pulls connections off the shared
/// listener and hands each off to a per-connection handler that does the
/// token handshake and pumps frames. Exits when `cancel` fires.
async fn run_accept_loop(
    listener: Arc<TcpListener>,
    registry: Arc<TokenRegistry>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("TCP streaming accept loop cancelled, exiting");
                return;
            }
            res = listener.accept() => match res {
                Ok((stream, _peer)) => {
                    tokio::spawn(handle_one_connection(stream, registry.clone()));
                }
                Err(e) => {
                    // Transient errors (e.g., EMFILE under fd pressure) are
                    // logged and the loop continues; a single failure must not
                    // kill streaming for the lifetime of the transport.
                    tracing::warn!("TCP streaming accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }
}

/// Per-connection task: token handshake + frame pump.
async fn handle_one_connection(mut stream: TcpStream, registry: Arc<TokenRegistry>) {
    let mut token_buf = [0u8; 16];
    let read_result = tokio::time::timeout(
        TOKEN_READ_TIMEOUT,
        AsyncReadExt::read_exact(&mut stream, &mut token_buf),
    )
    .await;

    let token = match read_result {
        Ok(Ok(_)) => Uuid::from_bytes(token_buf),
        Ok(Err(e)) => {
            tracing::warn!("TCP streaming handshake read failed: {}", e);
            return;
        }
        Err(_) => {
            tracing::warn!(
                "TCP streaming handshake timed out after {:?}",
                TOKEN_READ_TIMEOUT
            );
            return;
        }
    };

    let pending = match registry.remove(&token) {
        Some((_, p)) => p,
        None => {
            // Unknown or expired token. Drop the socket; do not let the peer
            // probe for valid tokens at zero cost.
            tracing::warn!(
                "TCP streaming: rejecting unknown token {} (expired or never registered)",
                token
            );
            return;
        }
    };

    configure_socket(&stream);
    pump_frames(stream, pending.frame_tx).await;
}

/// Read frames off the stream and forward them to the per-stream channel.
/// On abrupt close (no terminal sentinel observed), inject a `Dropped`
/// sentinel so the consumer sees a clean termination signal.
///
/// Always sends FIN explicitly via `shutdown()` before returning, rather than
/// relying on Drop to close the fd. This signals end-of-stream to the producer
/// proactively when *we* are the side initiating close (consumer dropped
/// mid-stream); otherwise the producer wouldn't notice until its next write.
async fn pump_frames(stream: TcpStream, frame_tx: flume::Sender<Vec<u8>>) {
    let mut framed = Framed::new(stream, TcpFrameCodec::new());
    let mut last_was_terminal = false;
    let mut consumer_dropped = false;

    while let Some(result) = framed.next().await {
        match result {
            Ok((_msg_type, _header, payload)) => {
                let payload_vec = payload.to_vec();
                last_was_terminal = is_terminal_sentinel(&payload_vec);
                if frame_tx.send_async(payload_vec).await.is_err() {
                    consumer_dropped = true;
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("TCP streaming read error: {}", e);
                break;
            }
        }
    }

    if !last_was_terminal && !consumer_dropped {
        let _ = frame_tx
            .send_async(crate::streaming::sender::cached_dropped().clone())
            .await;
    }

    // Proactively half-close: send FIN to the producer so it learns we're done
    // immediately, instead of waiting for its next failed write. Errors here
    // are normal (peer may have closed first); log at debug.
    let mut stream = framed.into_inner();
    if let Err(e) = stream.shutdown().await {
        tracing::debug!("TCP streaming receiver shutdown: {}", e);
    }
}

/// Configure TCP socket options matching TcpTransport patterns.
///
/// Sets TCP_NODELAY, keepalive (60s time / 10s interval), 1MB send/recv buffers,
/// and TCP_USER_TIMEOUT (30s) on both accept-side and connect-side sockets.
/// Errors are logged as warnings (non-fatal) since socket options are
/// best-effort optimizations.
///
/// `TCP_USER_TIMEOUT` bounds how long an unacked write may block before the
/// kernel returns an error. Without it, Linux's default retransmit window
/// (~15 min) means a write to a wedged peer blocks far longer than keepalive
/// (~150s) detection. Setting it to 30s gives writes a deterministic deadline
/// and matches the keepalive-driven dead-peer detection budget.
fn configure_socket(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::warn!("Failed to set TCP_NODELAY: {}", e);
    }
    let sock = socket2::SockRef::from(stream);
    if let Err(e) = sock.set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10)),
    ) {
        tracing::warn!("Failed to set TCP keepalive: {}", e);
    }
    if let Err(e) = sock.set_send_buffer_size(1_048_576) {
        tracing::warn!("Failed to set send buffer size: {}", e);
    }
    if let Err(e) = sock.set_recv_buffer_size(1_048_576) {
        tracing::warn!("Failed to set recv buffer size: {}", e);
    }
    if let Err(e) = sock.set_tcp_user_timeout(Some(Duration::from_secs(30))) {
        tracing::warn!("Failed to set TCP_USER_TIMEOUT: {}", e);
    }
}

/// Parse a `tcp://` endpoint into `(SocketAddr, Uuid)`.
///
/// Expected format: `tcp://{host}:{port}/{uuid_token}`
///
/// # Errors
///
/// Returns `Err` on malformed URIs (missing prefix, invalid address, invalid UUID).
///
/// # Examples
///
/// ```
/// # use crate::streaming::tcp_transport::parse_tcp_endpoint;
/// let (addr, token) = parse_tcp_endpoint("tcp://127.0.0.1:8080/550e8400-e29b-41d4-a716-446655440000").unwrap();
/// assert_eq!(addr.port(), 8080);
/// ```
pub fn parse_tcp_endpoint(endpoint: &str) -> Result<(std::net::SocketAddr, Uuid)> {
    let stripped = endpoint
        .strip_prefix("tcp://")
        .ok_or_else(|| anyhow::anyhow!("missing tcp:// prefix: {}", endpoint))?;
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
///
/// Terminal sentinels are Dropped, Detached, Finalized, and TransportError.
/// We compare against cached sentinel bytes for the common cases and fall
/// back to deserialization for TransportError (which carries a string payload).
fn is_terminal_sentinel(bytes: &[u8]) -> bool {
    use crate::streaming::sender::{cached_detached, cached_dropped, cached_finalized};

    if bytes == cached_dropped().as_slice()
        || bytes == cached_detached().as_slice()
        || bytes == cached_finalized().as_slice()
    {
        return true;
    }

    // TransportError carries a dynamic string, so we must try to deserialize
    if let Ok(frame) = rmp_serde::from_slice::<crate::streaming::frame::StreamFrame<()>>(bytes) {
        matches!(
            frame,
            crate::streaming::frame::StreamFrame::TransportError(_)
        )
    } else {
        false
    }
}

impl FrameTransport for TcpFrameTransport {
    fn bind(
        &self,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, Result<(String, flume::Receiver<Vec<u8>>)>> {
        let advertise_addr = self.advertise_addr;
        let registry = self.registry.clone();
        Box::pin(async move {
            let token = Uuid::new_v4();
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
            // The registry owns the *only* Sender clone we hold internally.
            // After bind() returns, the channel's lifetime is governed solely
            // by (a) the consumer's frame_rx, and (b) the registry entry
            // (which is moved into the per-connection pump on a successful
            // handshake or removed by the expiry task on timeout).
            registry.insert(token, PendingStream { frame_tx });
            // Token expiry: if no peer connects within ACCEPT_TIMEOUT, drop
            // the registry entry. That drops the Sender, which closes the
            // consumer's receiver promptly — no extra clones held past the
            // natural close point.
            let expiry_registry = registry.clone();
            tokio::spawn(async move {
                tokio::time::sleep(ACCEPT_TIMEOUT).await;
                if expiry_registry.remove(&token).is_some() {
                    tracing::warn!(
                        "TCP streaming: token {} expired before peer connected",
                        token
                    );
                }
            });
            let endpoint = format!("tcp://{}/{}", advertise_addr, token);
            Ok((endpoint, frame_rx))
        })
    }

    fn connect(
        &self,
        endpoint: &str,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let endpoint = endpoint.to_string();
        Box::pin(async move {
            let (addr, token) = parse_tcp_endpoint(&endpoint)?;

            // Connect to the listener and configure socket options before the
            // token write so TCP_NODELAY is in effect for the handshake.
            let mut stream = TcpStream::connect(addr).await?;
            configure_socket(&stream);
            stream.write_all(token.as_bytes()).await?;

            let (tx, rx) = flume::bounded::<Vec<u8>>(256);

            // Pump task: drain the flume channel, encode frames over TCP.
            // We keep the stream unsplit so shutdown() at the end half-closes
            // the *write* side via SHUT_WR (sending FIN) without leaving an
            // orphan read half holding kernel state. close(fd) at task end
            // releases the rest.
            tokio::spawn(async move {
                while let Ok(frame_bytes) = rx.recv_async().await {
                    if let Err(e) = TcpFrameCodec::encode_frame(
                        &mut stream,
                        MessageType::Message,
                        &[], // empty header for streaming
                        &frame_bytes,
                    )
                    .await
                    {
                        tracing::error!("TCP streaming write error: {}", e);
                        break;
                    }
                }

                // Sender-initiated graceful close: flush queued bytes, then
                // SHUT_WR to send FIN. TIME_WAIT lands on this (producer/client)
                // side, which is the right place — distributes the 2*MSL state
                // across producer hosts instead of accumulating on the
                // listener. Default SO_LINGER (off) is intentional: setting it
                // would either block close (positive timeout) or send RST and
                // truncate in-flight bytes (zero timeout) — both worse.
                if let Err(e) = stream.flush().await {
                    tracing::debug!("TCP streaming flush on close: {}", e);
                }
                if let Err(e) = stream.shutdown().await {
                    tracing::debug!("TCP streaming shutdown on close: {}", e);
                }
            });

            Ok(tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind_returns_tcp_endpoint() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, _rx) = transport.bind(1, 0).await.unwrap();

        // Verify endpoint format: tcp://addr:port/uuid
        assert!(
            endpoint.starts_with("tcp://"),
            "endpoint should start with tcp://: {}",
            endpoint
        );

        let (addr, token) = parse_tcp_endpoint(&endpoint).unwrap();
        assert!(addr.port() > 0, "port should be non-zero");
        assert_ne!(token, Uuid::nil(), "token should not be nil UUID");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect_handshake() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        // Connect and send some data
        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();

        let frame_bytes = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<String>::Item(
            "hello world".to_string(),
        ))
        .unwrap();

        sender.send_async(frame_bytes.clone()).await.unwrap();

        // Receive and verify
        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout waiting for frame")
            .expect("channel closed unexpectedly");

        assert_eq!(
            received, frame_bytes,
            "received frame should match sent frame"
        );

        // Drop sender to trigger cleanup
        drop(sender);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_invalid_token_rejected() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        // Parse endpoint to get addr but use a different token
        let (addr, _valid_token) = parse_tcp_endpoint(&endpoint).unwrap();

        // Connect manually with wrong token
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let wrong_token = Uuid::new_v4();
        stream.write_all(wrong_token.as_bytes()).await.unwrap();

        // The connection should be rejected. The listener should keep waiting
        // and eventually time out. We can't connect with the valid token after
        // the wrong one since the listener is still waiting. Instead, let's
        // verify that the wrong token doesn't produce frames.
        //
        // Give a short window to ensure no frames arrive from the wrong connection.
        let result = tokio::time::timeout(Duration::from_millis(500), rx.recv_async()).await;

        // Should timeout (no frames from invalid token connection)
        assert!(
            result.is_err(),
            "should not receive frames from invalid token connection"
        );

        // Clean up -- write some data on the rejected stream and verify it's not forwarded
        drop(stream);
    }

    #[test]
    fn test_parse_tcp_endpoint_valid() {
        let endpoint = "tcp://127.0.0.1:8080/550e8400-e29b-41d4-a716-446655440000";
        let (addr, token) = parse_tcp_endpoint(endpoint).unwrap();
        assert_eq!(addr.port(), 8080);
        assert_eq!(token.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_parse_tcp_endpoint_invalid_no_prefix() {
        assert!(parse_tcp_endpoint("http://127.0.0.1:8080/token").is_err());
    }

    #[test]
    fn test_parse_tcp_endpoint_invalid_no_token() {
        assert!(parse_tcp_endpoint("tcp://127.0.0.1:8080").is_err());
    }

    #[test]
    fn test_parse_tcp_endpoint_invalid_bad_uuid() {
        assert!(parse_tcp_endpoint("tcp://127.0.0.1:8080/not-a-uuid").is_err());
    }

    #[test]
    fn test_parse_tcp_endpoint_invalid_bad_addr() {
        assert!(
            parse_tcp_endpoint("tcp://not-an-addr/550e8400-e29b-41d4-a716-446655440000").is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect_round_trip_multiple_frames() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, rx) = transport.bind(42, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 42, 1).await.unwrap();

        // Send multiple frames
        let mut expected_frames = Vec::new();
        for i in 0..10 {
            let frame =
                rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<u32>::Item(i)).unwrap();
            expected_frames.push(frame.clone());
            sender.send_async(frame).await.unwrap();
        }

        // Receive and verify order
        for (i, expected) in expected_frames.iter().enumerate() {
            let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for frame {}", i))
                .unwrap_or_else(|_| panic!("channel closed at frame {}", i));
            assert_eq!(&received, expected, "frame {} mismatch", i);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dropped_sentinel_injected_on_abrupt_close() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        // Connect and send one frame, then drop sender without finalize
        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();
        let frame = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<String>::Item(
            "data".to_string(),
        ))
        .unwrap();
        sender.send_async(frame.clone()).await.unwrap();

        // Drop sender to trigger abrupt close (no terminal sentinel sent by user)
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
            crate::streaming::sender::cached_dropped().as_slice(),
            "should receive Dropped sentinel after abrupt close"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_extra_dropped_after_finalized() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();

        let sender = transport.connect(&endpoint, 1, 1).await.unwrap();

        // Send a Finalized sentinel explicitly
        let finalized =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Finalized).unwrap();
        sender.send_async(finalized.clone()).await.unwrap();

        // Drop sender
        drop(sender);

        // Should receive Finalized
        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(received, finalized);

        // Should NOT receive an extra Dropped sentinel -- channel should close
        let result = tokio::time::timeout(Duration::from_secs(2), rx.recv_async()).await;
        match result {
            Ok(Ok(extra)) => {
                // If we get something, it should NOT be a Dropped sentinel
                assert_ne!(
                    extra.as_slice(),
                    crate::streaming::sender::cached_dropped().as_slice(),
                    "should not inject Dropped after Finalized"
                );
            }
            Ok(Err(_)) => {} // channel closed -- expected
            Err(_) => {}     // timeout -- also fine
        }
    }

    #[test]
    fn test_parse_tcp_endpoint_ipv6() {
        let endpoint = "tcp://[::1]:8080/550e8400-e29b-41d4-a716-446655440000";
        let (addr, token) = parse_tcp_endpoint(endpoint).unwrap();
        assert_eq!(addr, "[::1]:8080".parse::<std::net::SocketAddr>().unwrap());
        assert_eq!(token.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind_ipv6_endpoint_format() {
        // Skip if IPv6 is not available (CI runners, containers without IPv6)
        if std::net::TcpListener::bind(std::net::SocketAddr::from((
            std::net::Ipv6Addr::LOCALHOST,
            0,
        )))
        .is_err()
        {
            eprintln!("Skipping test_bind_ipv6_endpoint_format: IPv6 not available");
            return;
        }

        let transport = TcpFrameTransport::new(std::net::Ipv6Addr::LOCALHOST.into())
            .await
            .unwrap();
        let (endpoint, _rx) = transport.bind(1, 0).await.unwrap();
        assert!(
            endpoint.contains("[::1]"),
            "IPv6 endpoint must bracket the address: {}",
            endpoint
        );
        let (addr, _token) = parse_tcp_endpoint(&endpoint).unwrap();
        assert!(addr.ip().is_loopback());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bind_unspecified_resolves_to_loopback() {
        let transport = TcpFrameTransport::default_bound().await.unwrap(); // 0.0.0.0
        let (endpoint, _rx) = transport.bind(1, 0).await.unwrap();
        let (addr, _token) = parse_tcp_endpoint(&endpoint).unwrap();
        assert!(
            addr.ip().is_loopback(),
            "unspecified bind should resolve to loopback in endpoint: {}",
            endpoint
        );
    }

    // ------------------------------------------------------------------
    // Shared-listener invariants (single bind per transport, token demux).
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_listener_serves_many_binds_on_one_port() {
        // Construct one transport, call bind() many times, assert every
        // returned endpoint advertises the same socket address.
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (endpoint0, _rx0) = transport.bind(0, 0).await.unwrap();
        let (addr0, _) = parse_tcp_endpoint(&endpoint0).unwrap();

        let mut tokens = std::collections::HashSet::new();
        for i in 1u64..50 {
            let (endpoint, _rx) = transport.bind(i, 0).await.unwrap();
            let (addr, token) = parse_tcp_endpoint(&endpoint).unwrap();
            assert_eq!(addr, addr0, "bind #{i} advertised a different address");
            assert!(tokens.insert(token), "duplicate token {token} on bind #{i}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_token_is_rejected_and_does_not_disrupt_others() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        // Register one valid stream.
        let (endpoint, rx) = transport.bind(1, 0).await.unwrap();
        let (addr, valid_token) = parse_tcp_endpoint(&endpoint).unwrap();

        // Connect with a wrong token; the accept loop should drop it without
        // touching the registered stream.
        let mut bad = TcpStream::connect(addr).await.unwrap();
        bad.write_all(Uuid::new_v4().as_bytes()).await.unwrap();
        // Give the accept loop a moment to process and reject.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The valid stream is still usable and the listener still serves us.
        let mut good = TcpStream::connect(addr).await.unwrap();
        good.write_all(valid_token.as_bytes()).await.unwrap();
        // Send a frame via TcpFrameCodec.
        let payload =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<u32>::Item(7)).unwrap();
        TcpFrameCodec::encode_frame(&mut good, MessageType::Message, &[], &payload)
            .await
            .unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), rx.recv_async())
            .await
            .expect("recv timeout — bad token disrupted the registered stream")
            .expect("channel closed unexpectedly");
        assert_eq!(received, payload);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn token_expires_when_no_peer_connects() {
        // Use tokio's paused virtual clock so we don't actually wait
        // ACCEPT_TIMEOUT (30s wall) — `tokio::time::sleep` inside the expiry
        // task observes virtual time and fires immediately when we advance.
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let (_endpoint, rx) = transport.bind(99, 0).await.unwrap();
        assert_eq!(transport.registry.len(), 1, "token should be registered");

        // Sleep past the timeout on the paused clock. `sleep` auto-advances
        // virtual time AND yields, so the expiry task gets a chance to run.
        // After it runs: registry entry removed, Sender dropped, channel
        // closed, recv returns Err. Wrap recv in a virtual-time timeout so
        // the test fails fast if an extra Sender clone leaks past expiry.
        tokio::time::sleep(ACCEPT_TIMEOUT + Duration::from_secs(1)).await;

        match tokio::time::timeout(Duration::from_secs(120), rx.recv_async()).await {
            Ok(Err(_)) => {} // channel closed — expected
            Ok(Ok(b)) => panic!("expected closed channel, got frame: {b:?}"),
            Err(_) => panic!(
                "consumer channel did not close after expiry — an extra Sender \
                 clone is leaking past the timeout and pinning the channel open"
            ),
        }
        assert_eq!(
            transport.registry.len(),
            0,
            "expired token should be removed from registry"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_binds_all_succeed_through_one_listener() {
        let transport = TcpFrameTransport::default_bound().await.unwrap();
        let mut handles = Vec::new();
        for i in 0u64..32 {
            let t = transport.clone();
            handles.push(tokio::spawn(async move {
                let (endpoint, rx) = t.bind(i, 0).await.unwrap();
                let sender = t.connect(&endpoint, i, 0).await.unwrap();
                let payload =
                    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<u64>::Item(i))
                        .unwrap();
                sender.send_async(payload.clone()).await.unwrap();
                let received = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                    .await
                    .expect("recv timeout")
                    .expect("channel closed");
                assert_eq!(received, payload);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }
}
