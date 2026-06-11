// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Allow dead_code during phased development: transport.rs (companion module being
// developed concurrently) will consume BoundTipcListener and the helper functions
// once the full TIPC module lands.  Remove this attribute when that module ships.
#![allow(dead_code)]

//! Two-phase bind, accept/serve/route/drain loop, and graceful-close handling
//! (ECONNRESET with empty partial-frame buffer treated as graceful peer close).
//!
//! ## Two-phase bind protocol
//!
//! The TIPC transport pre-binds its socket at `TipcTransportBuilder::build()` time:
//! `socket → bind_service_range → listen → getsockname`. This makes
//! `Transport::address()` final before `start()` is called and means `start()`
//! cannot fail on bind.
//!
//! [`BoundTipcListener`] receives the already-bound [`socket2::Socket`] and wraps
//! it in an `AsyncFd` at `serve()` time (when the tokio reactor is guaranteed to be
//! running).
//!
//! ## TIPC-specific close handling (invariant 2)
//!
//! A plain `close()` on the peer side surfaces as `ECONNRESET` at this end, never
//! as `EOF`.  Only an explicit `shutdown(SHUT_RDWR)` before drop produces a clean
//! `EOF` (`recv() == 0`) at the peer.
//!
//! The listener therefore treats `io::ErrorKind::ConnectionReset` with an **empty**
//! partial-frame decode buffer as a graceful close — no `DecodeError` metric, no
//! error log.  `ConnectionReset` with a non-empty buffer (partial frame in flight)
//! is a real error and is recorded as `DecodeError`.
//!
//! ## Drain gating
//!
//! During drain (`ShutdownState::is_draining()`), new `Message` frames are rejected
//! with a `ShuttingDown` response echoing the request header.  `Response`, `Ack`, and
//! `Event` frames always pass through so in-flight requests can complete.

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use std::io;
use std::sync::Arc;
use tokio::io::unix::AsyncFd;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use crate::observability::{Direction, TransportRejection};
use crate::transports::tcp::TcpFrameCodec;
use crate::transports::tcp::framing::{maybe_shrink_read_buffer, parse_shrink_threshold};
use velo_ext::{MessageType, ShutdownState, TransportAdapter, TransportErrorHandler};

use super::socket::set_rcvbuf;
use super::stream::TipcStream;

/// `SO_RCVBUF` size set on each accepted TIPC connection (2 MiB).
///
/// TIPC `SOCK_STREAM` flow-control is driven exclusively by `SO_RCVBUF` on the
/// receiving side — `SO_SNDBUF` is a no-op (§2.2).  A 2 MiB receive buffer gives
/// each accepted connection a deeper in-flight window before backpressure engages,
/// matching the initial sysctl default (`tipc_rmem = 2 MiB` on this host).
const TIPC_RCVBUF_SIZE: usize = 2 * 1024 * 1024;

// ── Per-connection context ────────────────────────────────────────────────────

/// Per-connection configuration passed to [`handle_connection`].
struct TipcConnectionContext {
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    shrink_threshold: usize,
}

// ── BoundTipcListenerConfig ───────────────────────────────────────────────────

/// Configuration bundle for [`BoundTipcListener::new`].
///
/// Avoids a many-argument constructor while keeping the listener construction
/// readable at the call site in `transport.rs`.
pub(super) struct BoundTipcListenerConfig {
    pub(super) adapter: TransportAdapter,
    pub(super) error_handler: Arc<dyn TransportErrorHandler>,
    pub(super) shutdown_state: ShutdownState,
    pub(super) transport_key: String,
    pub(super) metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    pub(super) shrink_threshold: usize,
}

// ── BoundTipcListener ─────────────────────────────────────────────────────────

/// A pre-bound TIPC `SOCK_STREAM` listener, ready to accept connections.
///
/// Created by [`BoundTipcListener::new`] with an already-bound [`socket2::Socket`]
/// (the transport builder performs bind+listen at `build()` time).  Call
/// [`BoundTipcListener::serve`] from within the tokio runtime (spawned task) to run
/// the accept loop.
pub struct BoundTipcListener {
    /// Pre-bound, listening `AF_TIPC SOCK_STREAM` socket.
    ///
    /// Transferred to an `AsyncFd` inside `serve()`, when the tokio reactor is
    /// guaranteed to be running.
    listener: socket2::Socket,
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    /// Read-buffer shrink threshold for each per-connection `Framed` instance.
    shrink_threshold: usize,
}

impl BoundTipcListener {
    /// Wrap an already-bound-and-listening [`socket2::Socket`] into a listener.
    ///
    /// The caller is responsible for having previously called
    /// `socket::bind_service_range_and_listen` on `listener`.  This constructor
    /// does not touch the socket; it is stored and registered with the tokio
    /// reactor when [`serve`][`BoundTipcListener::serve`] is called.
    pub(super) fn new(listener: socket2::Socket, config: BoundTipcListenerConfig) -> Self {
        Self {
            listener,
            adapter: config.adapter,
            error_handler: config.error_handler,
            shutdown_state: config.shutdown_state,
            transport_key: config.transport_key,
            metrics: config.metrics,
            shrink_threshold: config.shrink_threshold,
        }
    }

    /// Accept connections until the teardown token is cancelled.
    ///
    /// Registers the pre-bound socket with the tokio reactor (edge-triggered epoll),
    /// then loops:
    /// - Prioritises the teardown token so a flood of connects cannot starve shutdown.
    /// - Accepts each new socket, sets `SO_RCVBUF`, wraps it in [`TipcStream`], and
    ///   spawns a per-connection [`handle_connection`] task.
    ///
    /// Returns `Ok(())` when the teardown token fires.  Returns `Err` only if the
    /// `AsyncFd` constructor fails (e.g. the socket fd limit is exhausted).
    pub async fn serve(self) -> io::Result<()> {
        let teardown_token = self.shutdown_state.teardown_token().clone();

        // Switch to non-blocking mode and hand the fd to the tokio reactor.
        // This must happen inside an async context (i.e., when a tokio runtime
        // is available) so the `AsyncFd` registration succeeds.
        self.listener.set_nonblocking(true)?;
        let async_listener = AsyncFd::new(self.listener)?;

        info!("TIPC listener accepting connections");

        loop {
            tokio::select! {
                // Prioritise teardown so the accept loop exits promptly.
                biased;
                _ = teardown_token.cancelled() => {
                    info!("TIPC listener shutting down (teardown token cancelled)");
                    break;
                }
                ready = async_listener.readable() => {
                    let mut guard = match ready {
                        Ok(g) => g,
                        Err(e) => {
                            error!("TIPC listener: readiness error: {e}");
                            break;
                        }
                    };

                    match guard.try_io(|inner| inner.get_ref().accept()) {
                        Ok(Ok((sock, _addr))) => {
                            // Set SO_RCVBUF on the accepted socket.
                            // TIPC STREAM flow-control window = SO_RCVBUF on the
                            // receiving side; no SO_SNDBUF (flow-control no-op, §2.2).
                            if let Err(e) = set_rcvbuf(&sock, TIPC_RCVBUF_SIZE) {
                                warn!("TIPC listener: failed to set SO_RCVBUF: {e}");
                            }

                            let stream = match TipcStream::from_socket(sock) {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!("TIPC listener: failed to wrap accepted socket: {e}");
                                    continue;
                                }
                            };

                            debug!("TIPC listener: accepted new connection");

                            let ctx = TipcConnectionContext {
                                adapter: self.adapter.clone(),
                                error_handler: self.error_handler.clone(),
                                shutdown_state: self.shutdown_state.clone(),
                                transport_key: self.transport_key.clone(),
                                metrics: self.metrics.clone(),
                                shrink_threshold: self.shrink_threshold,
                            };

                            tokio::spawn(async move {
                                handle_connection(stream, ctx).await;
                            });
                        }
                        Ok(Err(e)) => {
                            // A transient accept error (e.g. ENOBUFS) should not kill
                            // the listener — log and continue.
                            error!("TIPC listener: accept error: {e}");
                        }
                        // Edge-triggered: spurious readiness → guard cleared, loop back.
                        Err(_would_block) => {}
                    }
                }
            }
        }

        Ok(())
    }
}

// ── Shrink-threshold helper ───────────────────────────────────────────────────

/// Resolve the TIPC read-buffer shrink threshold from `VELO_TIPC_SHRINK_THRESHOLD`
/// (bytes), falling back to the codec-level default (8 MiB).
///
/// Called by the transport builder so the threshold can be set once and passed to
/// every accepted connection.
pub(super) fn default_shrink_threshold() -> usize {
    parse_shrink_threshold(std::env::var("VELO_TIPC_SHRINK_THRESHOLD").ok().as_deref())
}

// ── Per-connection handler ────────────────────────────────────────────────────

/// Serve a single accepted TIPC connection until it closes or the teardown token
/// fires.
///
/// Decodes frames using [`TcpFrameCodec`] (the same codec as TCP and UDS), applies
/// drain gating, routes each frame to the appropriate [`TransportAdapter`] stream,
/// and records observability metrics.
///
/// ## TIPC close handling (invariant 2)
///
/// `ECONNRESET` with an empty partial-frame buffer is silently treated as graceful
/// close (peer called plain `close()` instead of `shutdown(Both)`).  Only
/// `ECONNRESET` mid-frame — a non-empty buffer — is recorded as a `DecodeError`.
async fn handle_connection(stream: TipcStream, ctx: TipcConnectionContext) {
    let TipcConnectionContext {
        adapter,
        error_handler,
        shutdown_state,
        transport_key,
        metrics,
        shrink_threshold,
    } = ctx;

    debug!("TIPC connection: frame loop starting");

    let mut framed = Framed::new(stream, TcpFrameCodec::new());
    let teardown_token = shutdown_state.teardown_token().clone();

    loop {
        tokio::select! {
            // Prioritise teardown so a saturated connection cannot starve shutdown.
            biased;
            _ = teardown_token.cancelled() => {
                debug!("TIPC connection: torn down via teardown token");
                break;
            }
            frame_result = framed.next() => {
                match frame_result {
                    Some(Ok((msg_type, header, payload))) => {
                        // Drain gate: reject new Message frames during drain;
                        // Response/Ack/Event always pass through.
                        if shutdown_state.is_draining() && msg_type == MessageType::Message {
                            if let Some(m) = metrics.as_ref() {
                                m.record_rejection(TransportRejection::DrainRejected);
                            }
                            debug!(
                                "TIPC connection: rejecting Message frame during drain \
                                 (sending ShuttingDown)"
                            );
                            // Echo original header for correlation; empty payload.
                            if let Err(e) = TcpFrameCodec::encode_frame(
                                framed.get_mut(),
                                MessageType::ShuttingDown,
                                &header,
                                &[],
                            )
                            .await
                            {
                                warn!("TIPC connection: failed to send ShuttingDown frame: {e}");
                            }
                            continue;
                        }

                        let frame_size = header.len() + payload.len();
                        if let Err(e) = route_frame(
                            msg_type,
                            header,
                            payload,
                            &adapter,
                            &error_handler,
                            &transport_key,
                            metrics.as_ref(),
                        )
                        .await
                        {
                            warn!("TIPC connection: failed to route {:?} frame: {e}", msg_type);
                        }

                        maybe_shrink_read_buffer(&mut framed, shrink_threshold, frame_size);
                    }

                    Some(Err(e)) => {
                        // Invariant 2: plain close() → ECONNRESET at peer.
                        // Empty buffer means no partial frame was in flight — all data
                        // was delivered before the close signal.  Treat as graceful.
                        // Non-empty buffer → partial frame = real decode error.
                        if e.kind() == io::ErrorKind::ConnectionReset
                            && framed.read_buffer().is_empty()
                        {
                            debug!(
                                "TIPC connection: graceful close \
                                 (ECONNRESET + empty buffer — peer used plain close())"
                            );
                        } else {
                            if let Some(m) = metrics.as_ref() {
                                m.record_rejection(TransportRejection::DecodeError);
                            }
                            error!("TIPC connection: frame decode error: {e}");
                        }
                        break;
                    }

                    None => {
                        // Clean EOF from explicit shutdown(Both) by the peer.
                        debug!("TIPC connection: clean EOF (peer called shutdown(Both))");
                        break;
                    }
                }
            }
        }
    }
}

// ── Frame router ─────────────────────────────────────────────────────────────

/// Route a decoded frame to the appropriate [`TransportAdapter`] stream.
///
/// Records an inbound observability event before sending.  On channel send failure,
/// records a `RouteFailed` rejection and calls the error handler with the original
/// header and payload.
async fn route_frame(
    msg_type: MessageType,
    header: Bytes,
    payload: Bytes,
    adapter: &TransportAdapter,
    error_handler: &Arc<dyn TransportErrorHandler>,
    transport_key: &str,
    metrics: Option<&Arc<dyn velo_ext::TransportObservability>>,
) -> Result<()> {
    #[cfg(not(feature = "distributed-tracing"))]
    let _ = transport_key;

    let sender = match msg_type {
        MessageType::Message => &adapter.message_stream,
        MessageType::Response => &adapter.response_stream,
        MessageType::Ack | MessageType::Event => &adapter.event_stream,
        // ShuttingDown is outbound-only; receiving it means a remote peer rejected
        // our request.  Route to the response stream for correlation.
        MessageType::ShuttingDown => &adapter.response_stream,
    };

    if let Some(m) = metrics {
        #[cfg(feature = "distributed-tracing")]
        let span = tracing::debug_span!(
            "velo.transport.receive",
            transport = transport_key,
            message_type = crate::transports::message_type_label(msg_type),
            bytes = header.len() + payload.len()
        );
        #[cfg(feature = "distributed-tracing")]
        let _entered = span.enter();

        m.record_frame(
            Direction::Inbound,
            crate::transports::message_type_label(msg_type),
            header.len() + payload.len(),
        );
    }

    match sender.send_async((header, payload)).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if let Some(m) = metrics {
                m.record_rejection(TransportRejection::RouteFailed);
            }
            error_handler.on_error(e.0.0, e.0.1, format!("Failed to route {:?}", msg_type));
            Err(anyhow::anyhow!("Failed to send to stream"))
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::transport::make_channels;

    struct NullErrorHandler;
    impl TransportErrorHandler for NullErrorHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {}
    }

    /// [`BoundTipcListener::new`] must compile and store its fields without
    /// panicking — a zero-cost sanity check that the constructor signature is
    /// correct without needing the TIPC kernel module.
    ///
    /// We can't call `serve()` here because:
    /// 1. We do not have a real pre-bound TIPC socket in a pure unit test.
    /// 2. `serve()` registers the socket with the tokio reactor and would fail
    ///    immediately on an invalid fd.
    ///
    /// Full accept-loop coverage lives in `tipc_integration.rs` (requires the
    /// TIPC kernel module: gated `#[cfg(velo_tipc)]`).
    #[test]
    fn bound_tipc_listener_constructs() {
        let (adapter, _streams) = make_channels();
        let error_handler: Arc<dyn TransportErrorHandler> = Arc::new(NullErrorHandler);

        // socket2::Socket::from_raw_fd would be unsafe; use a Unix-domain socket
        // as a stand-in for the shape test.  We are only checking that the struct
        // construction compiles and does not panic.
        //
        // Note: this does NOT exercise the TIPC-specific accept path; that lives in
        // the integration tests.
        let (sock_a, _sock_b) = {
            // Use std::os::unix::net::UnixStream to get a live fd pair.
            let (a, b) = std::os::unix::net::UnixStream::pair().expect("UnixStream::pair");
            (a, b)
        };

        // Wrap in socket2::Socket via RawFd.
        use std::os::fd::{FromRawFd, IntoRawFd};
        let raw = sock_a.into_raw_fd();
        // SAFETY: raw is a valid, owned fd from UnixStream::pair.
        let s2 = unsafe { socket2::Socket::from_raw_fd(raw) };

        let _listener = BoundTipcListener::new(
            s2,
            BoundTipcListenerConfig {
                adapter,
                error_handler,
                shutdown_state: ShutdownState::default(),
                transport_key: "tipc".to_string(),
                metrics: None,
                shrink_threshold: default_shrink_threshold(),
            },
        );
        // If we got here without panicking, the constructor is correct.
    }

    /// `default_shrink_threshold` returns a non-zero value (the env var is
    /// unlikely to be set to 0 in the test environment).
    #[test]
    fn shrink_threshold_default_is_nonzero() {
        let t = default_shrink_threshold();
        assert!(t > 0, "default shrink threshold must be positive");
    }
}
