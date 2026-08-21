// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Inbound frame routing shared by the messenger TCP/UDS listeners and the
//! read half of their dialed connections.
//!
//! Accepted sockets have always been read — that is what the listeners do.
//! Dialed sockets used to be write-only: the connection writer pushed frames
//! at the peer and nothing ever read the reverse direction. But the peer's
//! listener *does* write one thing back on that socket — the ShuttingDown
//! correlation reply it sends when it rejects a `Message` during drain.
//! Unread, those replies rot in the dialing side's kernel receive buffer and
//! the sender never learns its message was dropped.
//!
//! [`run_dialed_reader`] is the missing read half: it decodes frames off a
//! dialed socket and routes them through the same [`route_frame`] path the
//! listeners use, so a drain rejection lands on the sender's response stream
//! like any other correlated reply. It changes nothing on the wire — it only
//! reads bytes the peer was already sending.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use velo_ext::{MessageType, TransportAdapter, TransportErrorHandler};

use crate::observability::{Direction, TransportRejection};

use super::tcp::framing::{TcpFrameCodec, maybe_shrink_read_buffer};

/// Route a decoded frame to the appropriate stream
///
/// This function performs zero-copy routing by transferring ownership of
/// the Bytes to the flume channel. On error, it invokes the error callback
/// with the original data (requiring a clone).
pub(crate) async fn route_frame(
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
        MessageType::ShuttingDown => {
            // ShuttingDown is an outbound-only frame type; receiving it here
            // means a remote peer rejected our request. Route to the response
            // stream so higher layers can handle the rejection via correlation.
            &adapter.response_stream
        }
    };

    if let Some(metrics) = metrics {
        #[cfg(feature = "distributed-tracing")]
        let span = tracing::debug_span!(
            "velo.transport.receive",
            transport = transport_key,
            message_type = crate::transports::message_type_label(msg_type),
            bytes = header.len() + payload.len()
        );
        #[cfg(feature = "distributed-tracing")]
        let _entered = span.enter();

        metrics.record_frame(
            Direction::Inbound,
            crate::transports::message_type_label(msg_type),
            header.len() + payload.len(),
        );
    }

    // Try to send with ownership transfer (zero-copy)
    match sender.send_async((header, payload)).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if let Some(metrics) = metrics {
                metrics.record_rejection(TransportRejection::RouteFailed);
            }
            // Send failed - invoke error callback with the data
            error_handler.on_error(
                e.0.0, // header
                e.0.1, // payload
                format!("Failed to route {:?}", msg_type),
            );
            Err(anyhow::anyhow!("Failed to send to stream"))
        }
    }
}

/// Everything a dialed connection's read loop needs besides the socket.
///
/// Built once per transport in `start()` (that is when the adapter exists) and
/// cloned into each connection writer task.
#[derive(Clone)]
pub(crate) struct DialedReaderContext {
    pub adapter: TransportAdapter,
    pub error_handler: Arc<dyn TransportErrorHandler>,
    pub transport_key: String,
    pub shrink_threshold: usize,
}

/// Read frames off the read half of a dialed connection until the peer closes
/// it, a frame fails to decode, or `conn_cancel` fires.
///
/// On exit it cancels `conn_cancel` — a socket whose read side is finished is
/// a socket the peer has abandoned, so the connection's writer (which selects
/// on the same token) should stop instead of pushing frames at it until a
/// write finally errors.
///
/// No drain gate here: frames arriving on a dialed socket are the peer's
/// replies to our own sends (ShuttingDown correlations), not new inbound work,
/// so they must flow even while this side is draining.
pub(crate) async fn run_dialed_reader<R>(
    read_half: R,
    ctx: DialedReaderContext,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    conn_cancel: CancellationToken,
    peer: String,
) where
    R: AsyncRead + Unpin,
{
    let mut framed = FramedRead::new(read_half, TcpFrameCodec::new());

    loop {
        tokio::select! {
            // Prioritize cancellation so a saturated connection cannot starve
            // shutdown.
            biased;
            _ = conn_cancel.cancelled() => break,
            frame_result = framed.next() => {
                match frame_result {
                    Some(Ok((msg_type, header, payload))) => {
                        let frame_size = header.len() + payload.len();
                        if let Err(e) = route_frame(
                            msg_type,
                            header,
                            payload,
                            &ctx.adapter,
                            &ctx.error_handler,
                            &ctx.transport_key,
                            metrics.as_ref(),
                        )
                        .await
                        {
                            warn!(
                                "Failed to route {:?} frame from dialed connection to {}: {}",
                                msg_type, peer, e
                            );
                        }
                        maybe_shrink_read_buffer(
                            framed.read_buffer_mut(),
                            ctx.shrink_threshold,
                            frame_size,
                        );
                    }
                    Some(Err(e)) => {
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.record_rejection(TransportRejection::DecodeError);
                        }
                        warn!("Frame decode error on dialed connection to {}: {}", peer, e);
                        break;
                    }
                    None => {
                        debug!("Dialed connection to {} closed by peer", peer);
                        break;
                    }
                }
            }
        }
    }

    conn_cancel.cancel();
}
