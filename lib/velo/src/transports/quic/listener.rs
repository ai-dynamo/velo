// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Accept side of the QUIC transport: read the frames that peers send, and
//! answer a request refused during drain.

use std::sync::Arc;

use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use velo_ext::{TransportAdapter, TransportErrorHandler};

use crate::transports::ingress::{EchoShuttingDown, FrameRoute, serve_frames};
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
async fn serve_connection(connection: quinn::Connection, mut ctx: AcceptContext) {
    let peer = connection.remote_address();
    // The stream readers' teardown tokens are children of this connection's
    // token, so a cancel wakes this task before its readers. On the node-wide
    // token the readers' tokens would be its children, and would wake first.
    // The peer must see the connection close, not a STOP_SENDING from a reader
    // that stopped first. The wake order makes that certain only on a
    // current-thread runtime; on a multi-thread one a reader can still run
    // first, as it could when every task waited on the node-wide token.
    ctx.teardown = ctx.teardown.child_token();
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
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    ctx: AcceptContext,
    peer: std::net::SocketAddr,
) {
    serve_frames(
        FramedRead::new(OrderlyEnd(recv), TcpFrameCodec::new()),
        EchoShuttingDown(send),
        &ctx.teardown,
        FrameRoute {
            adapter: &ctx.adapter,
            error_handler: &ctx.error_handler,
            transport_key: &ctx.transport_key,
            metrics: ctx.metrics.as_ref(),
            shrink_threshold: ctx.shrink_threshold,
        },
        peer,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::super::tls;
    use super::*;
    use crate::transports::ingress::TEARDOWN_ARMS;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use velo_ext::MessageType;

    struct FailErrors;
    impl TransportErrorHandler for FailErrors {
        fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
            panic!("{error}");
        }
    }

    /// A stream builds its teardown future once, not once per frame: the QUIC
    /// twin of `tcp::listener::a_connection_arms_its_teardown_once`, which
    /// says why. QUIC used to rebuild it per frame on the node-wide token (65
    /// builds for these 64 frames).
    #[tokio::test]
    async fn a_stream_arms_its_teardown_once() {
        let identity = tls::Identity::generate().unwrap();
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(
            tls::server_crypto(&identity).unwrap(),
        )
        .unwrap();
        let server = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(crypto)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(tls::pinned_client_config(identity.fingerprint).unwrap());
        let accept = tokio::spawn({
            let server = server.clone();
            async move {
                let conn = server.accept().await.unwrap().await.unwrap();
                let (send, recv) = conn.accept_bi().await.unwrap();
                (conn, send, recv)
            }
        });
        let conn = client
            .connect(server.local_addr().unwrap(), "velo")
            .unwrap()
            .await
            .unwrap();
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        for _ in 0..64 {
            TcpFrameCodec::encode_frame(&mut send, MessageType::Response, b"h", b"p")
                .await
                .unwrap();
        }
        let (_server_conn, server_send, server_recv) =
            tokio::time::timeout(Duration::from_secs(5), accept)
                .await
                .unwrap()
                .unwrap();
        let (adapter, streams) = velo_ext::make_channels();
        let teardown = CancellationToken::new();
        let ctx = AcceptContext {
            adapter,
            error_handler: Arc::new(FailErrors),
            transport_key: "quic".into(),
            metrics: None,
            teardown: teardown.clone(),
            shrink_threshold: usize::MAX,
        };
        let arms = Arc::new(AtomicU64::new(0));
        let handler = tokio::spawn(TEARDOWN_ARMS.scope(
            arms.clone(),
            serve_stream(server_send, server_recv, ctx, conn.remote_address()),
        ));
        for _ in 0..64 {
            tokio::time::timeout(Duration::from_secs(5), streams.response_stream.recv_async())
                .await
                .unwrap()
                .unwrap();
        }
        teardown.cancel();
        tokio::time::timeout(Duration::from_secs(5), handler)
            .await
            .unwrap()
            .unwrap();
        let count = arms.load(Ordering::Relaxed);
        conn.close(0u32.into(), b"done");
        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
        assert_eq!(count, 1);
    }
}
