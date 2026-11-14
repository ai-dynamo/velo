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
    /// The shutdown teardown token. It stops the accept loop and the
    /// connection tasks; a connection task then stops its stream readers.
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
///
/// At teardown this task closes the connection before any of its stream
/// readers drops a stream. A reader that dropped its stream on an open
/// connection would send STOP_SENDING, which the dialer reads as frames left
/// unread, and it would warn on every peer restart. After `close()`, quinn
/// sends only CONNECTION_CLOSE: a stream dropped on a closed connection sends
/// nothing.
///
/// So the readers do not stop on the teardown token. A wake order is not a run
/// order: on a multi-thread runtime a reader that woke after this task can
/// still run first. They stop on `readers`, which this task cancels only after
/// it has closed the connection, so the order is certain on any runtime.
async fn serve_connection(connection: quinn::Connection, ctx: AcceptContext) {
    let peer = connection.remote_address();
    // Not a child of the teardown token: a node-wide cancel must not reach the
    // readers before `close()`. The guard also stops them when this task ends
    // any other way: the connection is lost, or the task is dropped.
    let readers = CancellationToken::new();
    let _stop_readers = readers.clone().drop_guard();
    loop {
        let stream = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => {
                connection.close(quinn::VarInt::from_u32(0), b"shutdown");
                break;
            }
            stream = connection.accept_bi() => stream,
        };
        match stream {
            Ok((send, recv)) => {
                tokio::spawn(serve_stream(send, recv, ctx.clone(), readers.clone(), peer));
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
/// request header echoed, so the sender fails it at once. Stops when `stop`
/// is cancelled; see `serve_connection` for why that is not the teardown
/// token.
async fn serve_stream(
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    ctx: AcceptContext,
    stop: CancellationToken,
    peer: std::net::SocketAddr,
) {
    serve_frames(
        FramedRead::new(OrderlyEnd(recv), TcpFrameCodec::new()),
        EchoShuttingDown(send),
        &stop,
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

    /// At teardown the peer sees the connection close, never a STOP_SENDING
    /// from a stream reader, even when the scheduler runs the readers before
    /// the connection task.
    ///
    /// A wake order is not a run order: on a multi-thread runtime a reader
    /// that woke after the connection task can still run first. A reader that
    /// stops on the teardown token can then drop its stream while the
    /// connection is open, and quinn sends STOP_SENDING. The dialer reads that
    /// as a peer that left frames unread, and warns on every peer restart.
    ///
    /// The test holds the connection task back by not polling it after the
    /// cancel, which is the worst order a scheduler can choose. The readers
    /// must not act until the connection task has run and closed the
    /// connection. When this order was only likely, this test failed on every
    /// run with `Ok(Some(0))`: the reader stopped its stream at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn teardown_stops_no_stream_before_the_connection_task_closes() {
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
            async move { server.accept().await.unwrap().await.unwrap() }
        });
        let connection = client
            .connect(server.local_addr().unwrap(), "velo")
            .unwrap()
            .await
            .unwrap();
        let server_connection = accept.await.unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
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
        let mut connection_task = Box::pin(serve_connection(server_connection, ctx));
        TcpFrameCodec::encode_frame(&mut send, MessageType::Event, b"h", b"p")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut connection_task => panic!("the connection must stay open"),
                frame = streams.event_stream.recv_async() => {
                    assert_eq!(frame.unwrap(), (Bytes::from_static(b"h"), Bytes::from_static(b"p")));
                }
            }
        })
        .await
        .expect("the connection task has spawned a reader, and the reader has routed a frame");

        teardown.cancel();
        // The connection task is not polled here; the spawned reader is free
        // to run on either worker.
        let early = tokio::time::timeout(Duration::from_secs(1), send.stopped()).await;
        assert!(
            early.is_err(),
            "a reader stopped its stream before the connection task ran: {early:?}"
        );
        tokio::time::timeout(Duration::from_secs(5), &mut connection_task)
            .await
            .expect("the connection task ends at teardown");
        let outcome = tokio::time::timeout(Duration::from_secs(5), send.stopped())
            .await
            .expect("the connection task closed the connection");
        server.close(0u32.into(), b"done");
        client.close(0u32.into(), b"done");
        assert!(
            matches!(
                outcome,
                Err(quinn::StoppedError::ConnectionLost(
                    quinn::ConnectionError::ApplicationClosed(_)
                ))
            ),
            "the peer saw something other than the connection close: {outcome:?}"
        );
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
            serve_stream(
                server_send,
                server_recv,
                ctx,
                teardown.clone(),
                conn.remote_address(),
            ),
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
