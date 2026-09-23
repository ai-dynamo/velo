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

/// Everything an accepted stream needs besides the stream itself.
#[derive(Clone)]
pub(super) struct AcceptContext {
    pub(super) adapter: TransportAdapter,
    pub(super) error_handler: Arc<dyn TransportErrorHandler>,
    pub(super) transport_key: String,
    pub(super) metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
    /// The shutdown teardown token. It stops accept loops and stream readers.
    pub(super) teardown: CancellationToken,
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
    let mut framed = FramedRead::new(recv, TcpFrameCodec::new());
    loop {
        let frame = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => break,
            frame = framed.next() => frame,
        };
        match frame {
            Some(Ok((msg_type, header, payload))) => {
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
