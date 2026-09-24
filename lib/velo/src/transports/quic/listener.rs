// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Accept side of the QUIC transport: read the frames that peers send, and
//! answer a request refused during drain.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use velo_ext::{MessageType, TransportAdapter, TransportErrorHandler};

use crate::observability::TransportRejection;
use crate::transports::ingress::{Routed, route_frame};
use crate::transports::tcp::TcpFrameCodec;
use crate::transports::tcp::framing::maybe_shrink_read_buffer;

/// Everything an accepted stream needs besides the stream itself.
#[derive(Clone)]
pub(super) struct AcceptContext {
    pub(super) adapter: TransportAdapter,
    pub(super) error_handler: Arc<dyn TransportErrorHandler>,
    pub(super) transport_key: String,
    pub(super) metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    /// The shutdown teardown token. It stops accept loops and stream readers.
    pub(super) teardown: CancellationToken,
    /// See `QuicTransportBuilder::shrink_threshold`.
    pub(super) shrink_threshold: usize,
}

/// A QUIC receive stream that reads an orderly close as the end of the stream.
///
/// quinn reports a connection that the peer closed as an `io::Error`
/// (`NotConnected`), where TCP gives the reader a clean end of stream. Read as
/// an error, it would be counted as `DecodeError` on every peer on each
/// restart. A close with an application code, or a local close, is an
/// ordinary end. A lost connection or a reset stream stays an error, and a
/// frame cut off by the close is still a decode error, which the codec reports
/// at the end of the stream.
pub(super) struct OrderlyEnd(pub(super) quinn::RecvStream);

impl tokio::io::AsyncRead for OrderlyEnd {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match std::pin::Pin::new(&mut self.0).poll_read(cx, buf) {
            std::task::Poll::Ready(Err(e)) if is_orderly_close(&e) => {
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

fn is_orderly_close(error: &std::io::Error) -> bool {
    matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<quinn::ReadError>()),
        Some(quinn::ReadError::ConnectionLost(
            quinn::ConnectionError::ApplicationClosed(_) | quinn::ConnectionError::LocallyClosed
        ))
    )
}

/// Accept connections on one server endpoint until teardown.
pub(super) async fn run_accept_loop(endpoint: quinn::Endpoint, ctx: AcceptContext) {
    loop {
        let incoming = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => break,
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => break,
            },
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => serve_connection(connection, ctx).await,
                Err(e) => debug!("QUIC: inbound handshake failed: {e:#}"),
            }
        });
    }
}

/// Serve the streams that one peer opens on a connection.
async fn serve_connection(connection: quinn::Connection, ctx: AcceptContext) {
    let peer = connection.remote_address();
    loop {
        let stream = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => break,
            stream = connection.accept_bi() => stream,
        };
        match stream {
            Ok((send, recv)) => {
                tokio::spawn(serve_stream(send, recv, ctx.clone(), peer));
            }
            Err(e) => {
                debug!(
                    "QUIC: connection from {peer} ended: {e:#} (close reason: {:?})",
                    connection.close_reason()
                );
                break;
            }
        }
    }
}

/// Read frames from one stream and route them. A `Message` refused during
/// drain gets a `ShuttingDown` frame back on the same stream, with the
/// request header echoed, so the sender fails it at once.
async fn serve_stream(
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
    ctx: AcceptContext,
    peer: std::net::SocketAddr,
) {
    let mut framed = FramedRead::new(OrderlyEnd(recv), TcpFrameCodec::new());
    loop {
        let frame = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => break,
            frame = framed.next() => frame,
        };
        match frame {
            Some(Ok((msg_type, header, payload))) => {
                let frame_size = header.len() + payload.len();
                match route_frame(
                    msg_type,
                    header,
                    payload,
                    &ctx.adapter,
                    &ctx.error_handler,
                    &ctx.transport_key,
                    ctx.metrics.as_ref(),
                )
                .await
                {
                    Ok(Routed::Delivered) => {}
                    Ok(Routed::DrainRejected { header }) => {
                        debug!("QUIC: rejecting Message from {peer} during drain");
                        if let Err(e) = echo_shutting_down(&mut send, &header).await {
                            warn!("QUIC: failed to send ShuttingDown to {peer}: {e:#}");
                        }
                    }
                    Err(e) => warn!("QUIC: failed to route {msg_type:?} from {peer}: {e:#}"),
                }
                maybe_shrink_read_buffer(
                    framed.read_buffer_mut(),
                    ctx.shrink_threshold,
                    frame_size,
                );
            }
            Some(Err(e)) => {
                if let Some(metrics) = ctx.metrics.as_ref() {
                    metrics.record_rejection(TransportRejection::DecodeError);
                }
                warn!("QUIC: frame decode error from {peer}: {e:#}");
                break;
            }
            None => {
                debug!("QUIC: stream from {peer} finished");
                break;
            }
        }
    }
}

async fn echo_shutting_down(send: &mut quinn::SendStream, header: &Bytes) -> std::io::Result<()> {
    TcpFrameCodec::encode_frame(send, MessageType::ShuttingDown, header, &[]).await
}
