// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC listener for inbound connections.
//!
//! Accepts incoming QUIC connections, reads framed messages from bidirectional
//! streams, and routes them to the appropriate transport streams.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};

use velo_observability::{Direction, TransportRejection};

use crate::tcp::TcpFrameCodec;
use crate::{MessageType, ShutdownState, TransportAdapter, TransportErrorHandler};

/// QUIC listener that accepts inbound connections and routes frames.
pub(crate) struct QuicListener {
    endpoint: quinn::Endpoint,
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<velo_observability::TransportMetricsHandle>,
}

impl QuicListener {
    /// Create a new QUIC listener.
    pub(crate) fn new(
        endpoint: quinn::Endpoint,
        adapter: TransportAdapter,
        error_handler: Arc<dyn TransportErrorHandler>,
        shutdown_state: ShutdownState,
        transport_key: String,
        metrics: Option<velo_observability::TransportMetricsHandle>,
    ) -> Self {
        Self {
            endpoint,
            adapter,
            error_handler,
            shutdown_state,
            transport_key,
            metrics,
        }
    }

    /// Run the QUIC listener, accepting connections until teardown.
    pub(crate) async fn run(self) -> Result<()> {
        let local_addr = self.endpoint.local_addr()?;
        info!("QUIC listener bound to {}", local_addr);

        let teardown_token = self.shutdown_state.teardown_token().clone();

        loop {
            tokio::select! {
                incoming = self.endpoint.accept() => {
                    match incoming {
                        Some(connecting) => {
                            let remote = connecting.remote_address();
                            debug!("Accepted QUIC connection from {}", remote);

                            let adapter = self.adapter.clone();
                            let error_handler = self.error_handler.clone();
                            let shutdown_state = self.shutdown_state.clone();
                            let transport_key = self.transport_key.clone();
                            let metrics = self.metrics.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(
                                    connecting,
                                    adapter,
                                    error_handler,
                                    shutdown_state,
                                    transport_key,
                                    metrics,
                                ).await {
                                    warn!("Error handling QUIC connection from {}: {}", remote, e);
                                }
                            });
                        }
                        None => {
                            info!("QUIC endpoint closed");
                            break;
                        }
                    }
                }
                _ = teardown_token.cancelled() => {
                    info!("QUIC listener shutting down (teardown)");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single inbound QUIC connection.
    async fn handle_connection(
        connecting: quinn::Incoming,
        adapter: TransportAdapter,
        error_handler: Arc<dyn TransportErrorHandler>,
        shutdown_state: ShutdownState,
        transport_key: String,
        metrics: Option<velo_observability::TransportMetricsHandle>,
    ) -> Result<()> {
        let connection = connecting.await.context("QUIC handshake failed")?;
        let remote = connection.remote_address();
        debug!("QUIC handshake complete with {}", remote);

        let teardown_token = shutdown_state.teardown_token().clone();

        // Accept bidirectional streams from the peer
        loop {
            let stream = tokio::select! {
                result = connection.accept_bi() => {
                    match result {
                        Ok(stream) => stream,
                        Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                            debug!("QUIC connection from {} closed by peer", remote);
                            break;
                        }
                        Err(e) => {
                            debug!("QUIC connection from {} error: {}", remote, e);
                            break;
                        }
                    }
                }
                _ = teardown_token.cancelled() => {
                    debug!("QUIC connection handler for {} torn down", remote);
                    break;
                }
            };

            let adapter = adapter.clone();
            let error_handler = error_handler.clone();
            let shutdown_state = shutdown_state.clone();
            let transport_key = transport_key.clone();
            let metrics = metrics.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_stream(
                    stream,
                    remote,
                    adapter,
                    error_handler,
                    shutdown_state,
                    transport_key,
                    metrics,
                )
                .await
                {
                    debug!("QUIC stream from {} ended: {}", remote, e);
                }
            });
        }

        Ok(())
    }

    /// Handle a single bidirectional QUIC stream.
    async fn handle_stream(
        (send, recv): (quinn::SendStream, quinn::RecvStream),
        remote: SocketAddr,
        adapter: TransportAdapter,
        error_handler: Arc<dyn TransportErrorHandler>,
        shutdown_state: ShutdownState,
        transport_key: String,
        metrics: Option<velo_observability::TransportMetricsHandle>,
    ) -> Result<()> {
        let rw = QuicBiStream {
            send,
            recv: tokio::io::BufReader::new(recv),
        };
        let mut framed = Framed::new(rw, TcpFrameCodec::new());

        let teardown_token = shutdown_state.teardown_token().clone();

        loop {
            tokio::select! {
                frame_result = framed.next() => {
                    match frame_result {
                        Some(Ok((msg_type, header, payload))) => {
                            // Drain-aware rejection
                            if shutdown_state.is_draining() && msg_type == MessageType::Message {
                                if let Some(metrics) = metrics.as_ref() {
                                    metrics.record_rejection(TransportRejection::DrainRejected);
                                }
                                debug!(
                                    "Rejecting Message frame from {} during drain (sending ShuttingDown)",
                                    remote
                                );
                                if let Err(e) = TcpFrameCodec::encode_frame(
                                    framed.get_mut(),
                                    MessageType::ShuttingDown,
                                    &header,
                                    &[],
                                ).await {
                                    warn!(
                                        "Failed to send ShuttingDown frame to {}: {}",
                                        remote, e
                                    );
                                }
                                continue;
                            }

                            if let Err(e) = Self::route_frame(
                                msg_type,
                                header,
                                payload,
                                &adapter,
                                &error_handler,
                                &transport_key,
                                metrics.as_ref(),
                            ).await {
                                warn!(
                                    "Failed to route {:?} frame from {}: {}",
                                    msg_type, remote, e
                                );
                            }
                        }
                        Some(Err(e)) => {
                            if let Some(metrics) = metrics.as_ref() {
                                metrics.record_rejection(TransportRejection::DecodeError);
                            }
                            error!("Frame decode error from {}: {}", remote, e);
                            break;
                        }
                        None => {
                            debug!("QUIC stream from {} closed", remote);
                            break;
                        }
                    }
                }
                _ = teardown_token.cancelled() => {
                    debug!("QUIC stream handler for {} torn down", remote);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Route a decoded frame to the appropriate transport stream.
    async fn route_frame(
        msg_type: MessageType,
        header: Bytes,
        payload: Bytes,
        adapter: &TransportAdapter,
        error_handler: &Arc<dyn TransportErrorHandler>,
        transport_key: &str,
        metrics: Option<&velo_observability::TransportMetricsHandle>,
    ) -> Result<()> {
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = transport_key;
        let sender = match msg_type {
            MessageType::Message => &adapter.message_stream,
            MessageType::Response => &adapter.response_stream,
            MessageType::Ack | MessageType::Event => &adapter.event_stream,
            MessageType::ShuttingDown => &adapter.response_stream,
        };

        if let Some(metrics) = metrics {
            #[cfg(feature = "distributed-tracing")]
            let span = tracing::debug_span!(
                "velo.transport.receive",
                transport = transport_key,
                message_type = crate::message_type_label(msg_type),
                bytes = header.len() + payload.len()
            );
            #[cfg(feature = "distributed-tracing")]
            let _entered = span.enter();

            metrics.record_frame(
                Direction::Inbound,
                crate::message_type_label(msg_type),
                header.len() + payload.len(),
            );
        }

        match sender.send_async((header, payload)).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(metrics) = metrics {
                    metrics.record_rejection(TransportRejection::RouteFailed);
                }
                error_handler.on_error(e.0.0, e.0.1, format!("Failed to route {:?}", msg_type));
                Err(anyhow::anyhow!("Failed to send to stream"))
            }
        }
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
        // quinn::SendStream implements tokio::io::AsyncWrite
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
