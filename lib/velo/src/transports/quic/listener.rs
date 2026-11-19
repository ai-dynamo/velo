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
/// still run first. They stop on a token that [`CloseThenStop`] cancels only
/// after it has closed the connection, so the order is certain on any runtime.
async fn serve_connection(connection: quinn::Connection, ctx: AcceptContext) {
    let peer = connection.remote_address();
    let guard = CloseThenStop {
        connection: connection.clone(),
        readers: CancellationToken::new(),
    };
    loop {
        let stream = tokio::select! {
            biased;
            _ = ctx.teardown.cancelled() => break,
            stream = connection.accept_bi() => stream,
        };
        match stream {
            Ok((send, recv)) => {
                tokio::spawn(serve_stream(
                    send,
                    recv,
                    ctx.clone(),
                    guard.readers.clone(),
                    peer,
                ));
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

/// Closes a connection and then stops its stream readers when the connection
/// task ends a connection that is still open: at teardown, on a panic, or
/// when the task is dropped.
///
/// The readers' token is not a child of the teardown token, so a node-wide
/// cancel cannot reach them before the close.
///
/// A connection that has ended without us, closed by the peer or lost, keeps
/// its readers, however the task ends. The peer may close as soon as we
/// acknowledge its last frames, and a reader can still have those frames in
/// its buffer. Stopped, it would lose frames the peer counts as delivered. On
/// a closed connection a read returns the buffered bytes and then the end, so
/// each reader ends on its own. `accept_bi` fails only once the connection has
/// ended, so its error path is this case too.
struct CloseThenStop {
    connection: quinn::Connection,
    readers: CancellationToken,
}

impl Drop for CloseThenStop {
    fn drop(&mut self) {
        // Also keeps us from calling `close()` on an ended connection, which
        // would replace its error with `LocallyClosed`.
        if self.connection.close_reason().is_some() {
            return;
        }
        self.connection
            .close(quinn::VarInt::from_u32(0), b"shutdown");
        self.readers.cancel();
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

    /// A connection task that is dropped, not torn down, still closes the
    /// connection before it stops its readers.
    ///
    /// Any one reader keeps the connection open while it holds its streams. A
    /// reader blocked on a `ShuttingDown` reply that the peer's one-byte
    /// window cannot take does not see its stop at once. If the dropped task
    /// only stopped the readers, an idle reader would drop its stream on that
    /// open connection, and the peer would get STOP_SENDING. Run once through
    /// teardown as the control, and once with the task dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_a_connection_task_closes_before_stopping_its_readers() {
        async fn check(drop_task: bool) {
            struct RejectingMessage(Arc<tokio::sync::Notify>);
            impl velo_ext::TransportObservability for RejectingMessage {
                fn record_frame(&self, _: velo_ext::Direction, _: &str, _: usize) {}
                fn record_rejection(&self, reason: velo_ext::TransportRejection) {
                    if matches!(reason, velo_ext::TransportRejection::DrainRejected) {
                        self.0.notify_one();
                    }
                }
                fn set_registered_peers(&self, _: usize) {}
                fn set_active_connections(&self, _: usize) {}
                fn record_send_backpressure(&self) {}
            }

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
            let mut client_config = tls::pinned_client_config(identity.fingerprint).unwrap();
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.stream_receive_window(1u32.into());
            client_config.transport_config(Arc::new(transport_config));
            client.set_default_client_config(client_config);
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
            adapter.shutdown_state.begin_drain();
            let rejecting_message = Arc::new(tokio::sync::Notify::new());
            let teardown = CancellationToken::new();
            let ctx = AcceptContext {
                adapter,
                error_handler: Arc::new(FailErrors),
                transport_key: "quic".into(),
                metrics: Some(Arc::new(RejectingMessage(rejecting_message.clone()))),
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
                    frame = streams.event_stream.recv_async() => { frame.unwrap(); }
                }
            })
            .await
            .expect("the idle reader has routed a frame");

            // The peer's one-byte window cannot fit a ShuttingDown reply.
            let (mut blocked_send, _blocked_recv) = connection.open_bi().await.unwrap();
            TcpFrameCodec::encode_frame(&mut blocked_send, MessageType::Message, b"h", b"p")
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = &mut connection_task => panic!("the connection must stay open"),
                    _ = rejecting_message.notified() => {}
                }
            })
            .await
            .expect("the second reader is replying to a drain rejection");
            assert!(connection.close_reason().is_none());

            teardown.cancel();
            if !drop_task {
                tokio::time::timeout(Duration::from_secs(5), &mut connection_task)
                    .await
                    .expect("the connection task handles teardown");
            }
            drop(connection_task);
            let outcome = tokio::time::timeout(Duration::from_secs(5), send.stopped()).await;
            server.close(0u32.into(), b"done");
            client.close(0u32.into(), b"done");
            assert!(
                matches!(
                    outcome,
                    Ok(Err(quinn::StoppedError::ConnectionLost(
                        quinn::ConnectionError::ApplicationClosed(_)
                    )))
                ),
                "connection task dropped={drop_task} stopped a stream before closing: {outcome:?}"
            );
        }

        check(false).await;
        check(true).await;
    }

    /// Dropping a task after the peer closes must preserve acknowledged frames.
    ///
    /// The control observes the peer close, then tears down while the reader is paused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_a_peer_closed_connection_task_preserves_acknowledged_frames() {
        struct PausedReader {
            entered: tokio::sync::Notify,
            first: std::sync::atomic::AtomicBool,
            released: parking_lot::Mutex<bool>,
            release: parking_lot::Condvar,
        }
        impl velo_ext::TransportObservability for PausedReader {
            fn record_frame(&self, _: velo_ext::Direction, _: &str, _: usize) {
                if !self.first.swap(true, Ordering::SeqCst) {
                    tokio::task::block_in_place(|| {
                        self.entered.notify_one();
                        let mut released = self.released.lock();
                        while !*released {
                            self.release.wait(&mut released);
                        }
                    });
                }
            }
            fn record_rejection(&self, _: velo_ext::TransportRejection) {}
            fn set_registered_peers(&self, _: usize) {}
            fn set_active_connections(&self, _: usize) {}
            fn record_send_backpressure(&self) {}
        }
        struct ReleaseReader(Arc<PausedReader>);
        impl Drop for ReleaseReader {
            fn drop(&mut self) {
                *self.0.released.lock() = true;
                self.0.release.notify_all();
            }
        }

        async fn check(drop_task: bool) {
            const FRAMES: usize = 8;
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
            client.set_default_client_config(
                tls::pinned_client_config(identity.fingerprint).unwrap(),
            );
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
            let paused = Arc::new(PausedReader {
                entered: tokio::sync::Notify::new(),
                first: std::sync::atomic::AtomicBool::new(false),
                released: parking_lot::Mutex::new(false),
                release: parking_lot::Condvar::new(),
            });
            let release = ReleaseReader(paused.clone());
            let teardown = CancellationToken::new();
            let ctx = AcceptContext {
                adapter,
                error_handler: Arc::new(FailErrors),
                transport_key: "quic".into(),
                metrics: Some(paused.clone()),
                teardown: teardown.clone(),
                shrink_threshold: usize::MAX,
            };
            let mut task = Box::pin(serve_connection(server_connection.clone(), ctx));
            TcpFrameCodec::encode_frame(&mut send, MessageType::Event, b"0", b"payload")
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = &mut task => panic!("the connection must stay open"),
                    _ = paused.entered.notified() => {}
                }
            })
            .await
            .expect("the reader is paused while routing its first frame");
            for seq in 1..FRAMES {
                TcpFrameCodec::encode_frame(
                    &mut send,
                    MessageType::Event,
                    &[seq as u8],
                    b"payload",
                )
                .await
                .unwrap();
            }
            send.finish().unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), send.stopped())
                    .await
                    .unwrap()
                    .unwrap(),
                None,
                "the listener acknowledged every byte and the FIN"
            );
            connection.close(0u32.into(), b"done");
            let reason = tokio::time::timeout(Duration::from_secs(5), server_connection.closed())
                .await
                .expect("quinn observes the peer's close while the connection task is held back");
            assert!(matches!(
                reason,
                quinn::ConnectionError::ApplicationClosed(_)
            ));
            assert!(!teardown.is_cancelled());
            if !drop_task {
                tokio::time::timeout(Duration::from_secs(5), &mut task)
                    .await
                    .expect("control: the task observes the peer's close");
            }
            drop(task);
            if !drop_task {
                teardown.cancel();
            }
            drop(release);

            let mut delivered = 0;
            while tokio::time::timeout(Duration::from_secs(5), streams.event_stream.recv_async())
                .await
                .expect("all readers must finish")
                .is_ok()
            {
                delivered += 1;
            }
            server.close(0u32.into(), b"done");
            client.close(0u32.into(), b"done");
            assert_eq!(
                delivered, FRAMES,
                "connection task dropped={drop_task}: acknowledged frames were discarded outside teardown"
            );
        }

        check(false).await;
        check(true).await;
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

    /// A peer that finishes and closes leaves its acknowledged frames readable.
    ///
    /// Pause the reader after its first decode, let quinn acknowledge every
    /// frame and the FIN, and let the connection task observe the peer's close.
    /// Resuming the reader must deliver all eight frames. The control resumes
    /// it before the peer closes.
    ///
    /// The pause waits in `block_in_place`; see the comment in `record_frame`.
    /// With one worker thread and a plain wait, this test hangs every run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_close_preserves_acknowledged_frames() {
        struct PausedReader {
            entered: tokio::sync::Notify,
            first: std::sync::atomic::AtomicBool,
            released: parking_lot::Mutex<bool>,
            release: parking_lot::Condvar,
        }
        impl velo_ext::TransportObservability for PausedReader {
            fn record_frame(&self, _: velo_ext::Direction, _: &str, _: usize) {
                if !self.first.swap(true, Ordering::SeqCst) {
                    self.entered.notify_one();
                    // The wait holds this worker thread. Tasks the reader woke
                    // just before, such as quinn's connection driver, can sit
                    // in this worker's LIFO slot, which no other worker takes:
                    // with no ACK from the driver, `send.stopped()` below
                    // timed out. `block_in_place` hands the worker's queue to
                    // a new thread for the wait.
                    tokio::task::block_in_place(|| {
                        let mut released = self.released.lock();
                        while !*released {
                            self.release.wait(&mut released);
                        }
                    });
                }
            }
            fn record_rejection(&self, _: velo_ext::TransportRejection) {}
            fn set_registered_peers(&self, _: usize) {}
            fn set_active_connections(&self, _: usize) {}
            fn record_send_backpressure(&self) {}
        }
        struct ReleaseReader(Arc<PausedReader>);
        impl Drop for ReleaseReader {
            fn drop(&mut self) {
                *self.0.released.lock() = true;
                self.0.release.notify_all();
            }
        }

        async fn check(close_before_reader_resumes: bool) {
            const FRAMES: usize = 8;
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
            client.set_default_client_config(
                tls::pinned_client_config(identity.fingerprint).unwrap(),
            );
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
            let paused = Arc::new(PausedReader {
                entered: tokio::sync::Notify::new(),
                first: std::sync::atomic::AtomicBool::new(false),
                released: parking_lot::Mutex::new(false),
                release: parking_lot::Condvar::new(),
            });
            let release = ReleaseReader(paused.clone());
            let teardown = CancellationToken::new();
            let ctx = AcceptContext {
                adapter,
                error_handler: Arc::new(FailErrors),
                transport_key: "quic".into(),
                metrics: Some(paused.clone()),
                teardown: teardown.clone(),
                shrink_threshold: usize::MAX,
            };
            let mut task = tokio::spawn(serve_connection(server_connection, ctx));
            TcpFrameCodec::encode_frame(&mut send, MessageType::Event, b"0", b"payload")
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), paused.entered.notified())
                .await
                .expect("the reader is paused while routing its first frame");
            for seq in 1..FRAMES {
                TcpFrameCodec::encode_frame(
                    &mut send,
                    MessageType::Event,
                    &[seq as u8],
                    b"payload",
                )
                .await
                .unwrap();
            }
            send.finish().unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), send.stopped())
                    .await
                    .unwrap()
                    .unwrap(),
                None,
                "the listener acknowledged every byte and the FIN"
            );
            assert!(!teardown.is_cancelled());
            if close_before_reader_resumes {
                connection.close(0u32.into(), b"done");
                tokio::time::timeout(Duration::from_secs(5), &mut task)
                    .await
                    .expect("the connection task observes the peer's close")
                    .unwrap();
            }
            drop(release);

            let mut delivered = 0;
            if close_before_reader_resumes {
                while tokio::time::timeout(
                    Duration::from_secs(5),
                    streams.event_stream.recv_async(),
                )
                .await
                .expect("all readers must finish")
                .is_ok()
                {
                    delivered += 1;
                }
            } else {
                for _ in 0..FRAMES {
                    tokio::time::timeout(Duration::from_secs(5), streams.event_stream.recv_async())
                        .await
                        .expect("control: all frames reach the adapter")
                        .unwrap();
                    delivered += 1;
                }
                connection.close(0u32.into(), b"done");
                tokio::time::timeout(Duration::from_secs(5), &mut task)
                    .await
                    .unwrap()
                    .unwrap();
            }
            server.close(0u32.into(), b"done");
            client.close(0u32.into(), b"done");
            assert_eq!(
                delivered, FRAMES,
                "peer closed before reader resumed={close_before_reader_resumes}: acknowledged frames were discarded"
            );
        }

        check(false).await;
        check(true).await;
    }
}
