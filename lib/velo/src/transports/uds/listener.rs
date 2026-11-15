// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! UDS listener for ActiveMessage transport
//!
//! Mirrors `tcp/listener.rs` but uses `UnixListener`/`UnixStream`.
//! Reuses `TcpFrameCodec` for framing. Supports drain-aware frame handling
//! via `ShutdownState`.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UnixListener as TokioUnixListener;
use tokio::net::UnixStream;
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, warn};

use crate::transports::ingress::{EchoShuttingDown, FrameRoute, serve_frames};

use velo_ext::{ShutdownState, TransportAdapter, TransportErrorHandler};

use crate::transports::tcp::TcpFrameCodec;
use crate::transports::tcp::framing::parse_shrink_threshold;

/// Per-connection configuration handed to [`UdsListener::handle_connection`].
struct UdsConnectionContext {
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    shrink_threshold: usize,
}

/// Resolve the UDS listener shrink threshold from `VELO_UDS_SHRINK_THRESHOLD`
/// (in bytes), falling back to the codec-level default.
pub(super) fn default_shrink_threshold() -> usize {
    parse_shrink_threshold(std::env::var("VELO_UDS_SHRINK_THRESHOLD").ok().as_deref())
}

/// UDS listener for ActiveMessage transport
///
/// Accepts incoming Unix domain socket connections and routes decoded frames
/// to the appropriate transport streams. Supports graceful drain: during drain,
/// new `Message` frames are rejected with a `ShuttingDown` response while
/// `Response`/`Event`/`Ack` frames continue to flow.
pub struct UdsListener {
    socket_path: PathBuf,
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    transport_key: String,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    /// If the per-connection `Framed` read buffer grows past this many bytes,
    /// it is reset to a small fresh `BytesMut` the next time it fully drains.
    /// Bounds long-lived memory after a single oversized frame.
    shrink_threshold: usize,
}

/// UDS listener that has been bound to a socket path, ready to accept connections.
///
/// Created by [`UdsListener::bind`]. Holding this value proves the OS-level bind
/// succeeded, so callers can detect failures before spawning a task.
pub struct BoundUdsListener {
    socket_path: PathBuf,
    adapter: TransportAdapter,
    error_handler: Arc<dyn TransportErrorHandler>,
    shutdown_state: ShutdownState,
    listener: TokioUnixListener,
    transport_key: String,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    shrink_threshold: usize,
}

impl UdsListener {
    /// Create a new builder for UdsListener
    pub fn builder() -> UdsListenerBuilder {
        UdsListenerBuilder::new()
    }

    /// Bind to the socket path and return a [`BoundUdsListener`] ready to serve.
    ///
    /// `TokioUnixListener::bind` is synchronous, so this method is also
    /// synchronous. Callers that need to propagate bind failures before spawning
    /// a task should call `bind()` first, then spawn `bound.serve()`.
    pub fn bind(self) -> Result<BoundUdsListener> {
        let listener = TokioUnixListener::bind(&self.socket_path)
            .with_context(|| format!("Failed to bind UDS listener to {:?}", self.socket_path))?;
        info!("UDS listener bound to {:?}", self.socket_path);
        Ok(BoundUdsListener {
            socket_path: self.socket_path,
            adapter: self.adapter,
            error_handler: self.error_handler,
            shutdown_state: self.shutdown_state,
            listener,
            transport_key: self.transport_key,
            metrics: self.metrics,
            shrink_threshold: self.shrink_threshold,
        })
    }

    /// Convenience shim: bind and serve in one call.
    pub async fn serve(self) -> Result<()> {
        self.bind()?.serve().await
    }

    /// Handle a single UDS connection
    async fn handle_connection(mut stream: UnixStream, ctx: UdsConnectionContext) -> Result<()> {
        let UdsConnectionContext {
            adapter,
            error_handler,
            shutdown_state,
            transport_key,
            metrics,
            shrink_threshold,
        } = ctx;

        // Borrowed halves: no `Arc`, and no write shutdown when they drop.
        let (read_half, write_half) = stream.split();
        serve_frames(
            FramedRead::new(read_half, TcpFrameCodec::new()),
            EchoShuttingDown(write_half),
            shutdown_state.teardown_token(),
            FrameRoute {
                adapter: &adapter,
                error_handler: &error_handler,
                transport_key: &transport_key,
                metrics: metrics.as_ref(),
                shrink_threshold,
            },
            "a UDS peer",
        )
        .await;

        Ok(())
    }
}

impl BoundUdsListener {
    /// Accept connections until the teardown token is cancelled.
    pub async fn serve(self) -> Result<()> {
        let teardown_token = self.shutdown_state.teardown_token().clone();

        loop {
            tokio::select! {
                // Prioritize teardown so the accept loop exits promptly under shutdown.
                biased;
                _ = teardown_token.cancelled() => {
                    info!("UDS listener shutting down (teardown)");
                    break;
                }
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((stream, _addr)) => {
                            debug!("Accepted UDS connection");

                            let ctx = UdsConnectionContext {
                                adapter: self.adapter.clone(),
                                error_handler: self.error_handler.clone(),
                                shutdown_state: self.shutdown_state.clone(),
                                transport_key: self.transport_key.clone(),
                                metrics: self.metrics.clone(),
                                shrink_threshold: self.shrink_threshold,
                            };

                            tokio::spawn(async move {
                                if let Err(e) = UdsListener::handle_connection(stream, ctx).await {
                                    warn!("Error handling UDS connection: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept UDS connection: {}", e);
                        }
                    }
                }
            }
        }

        // Clean up socket file
        std::fs::remove_file(&self.socket_path).ok();

        Ok(())
    }
}

/// Builder for UdsListener
pub struct UdsListenerBuilder {
    socket_path: Option<PathBuf>,
    adapter: Option<TransportAdapter>,
    error_handler: Option<Arc<dyn TransportErrorHandler>>,
    shutdown_state: Option<ShutdownState>,
    transport_key: Option<String>,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    shrink_threshold: Option<usize>,
}

impl UdsListenerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            socket_path: None,
            adapter: None,
            error_handler: None,
            shutdown_state: None,
            transport_key: None,
            metrics: None,
            shrink_threshold: None,
        }
    }

    /// Set the socket path
    pub fn socket_path(mut self, path: PathBuf) -> Self {
        self.socket_path = Some(path);
        self
    }

    /// Set the transport adapter
    pub fn adapter(mut self, adapter: TransportAdapter) -> Self {
        self.adapter = Some(adapter);
        self
    }

    /// Set the error handler
    pub fn error_handler(mut self, handler: Arc<dyn TransportErrorHandler>) -> Self {
        self.error_handler = Some(handler);
        self
    }

    /// Set the shutdown state for graceful drain coordination
    pub fn shutdown_state(mut self, state: ShutdownState) -> Self {
        self.shutdown_state = Some(state);
        self
    }

    /// Set the transport key used for metrics labels.
    pub fn transport_key(mut self, transport_key: impl Into<String>) -> Self {
        self.transport_key = Some(transport_key.into());
        self
    }

    /// Install transport-scoped observability handles.
    pub fn metrics(
        mut self,
        metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Override the read-buffer shrink threshold (default resolves from
    /// `VELO_UDS_SHRINK_THRESHOLD`, falling back to the codec-level default).
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = Some(bytes);
        self
    }

    /// Build the UdsListener
    pub fn build(self) -> Result<UdsListener> {
        let socket_path = self
            .socket_path
            .ok_or_else(|| anyhow::anyhow!("socket_path is required"))?;
        let adapter = self
            .adapter
            .ok_or_else(|| anyhow::anyhow!("adapter is required"))?;
        let error_handler = self
            .error_handler
            .ok_or_else(|| anyhow::anyhow!("error_handler is required"))?;
        let shutdown_state = self.shutdown_state.unwrap_or_default();

        Ok(UdsListener {
            socket_path,
            adapter,
            error_handler,
            shutdown_state,
            transport_key: self.transport_key.unwrap_or_else(|| "uds".to_string()),
            metrics: self.metrics,
            shrink_threshold: self
                .shrink_threshold
                .unwrap_or_else(default_shrink_threshold),
        })
    }
}

impl Default for UdsListenerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::ingress::TEARDOWN_ARMS;
    use crate::transports::transport::make_channels;
    use bytes::Bytes;
    use velo_ext::MessageType;

    struct TestErrorHandler;

    impl TransportErrorHandler for TestErrorHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
            eprintln!("Test error handler: {}", error);
        }
    }

    /// A connection builds its teardown future once, not once per frame: the
    /// UDS twin of `tcp::listener::a_connection_arms_its_teardown_once`, which
    /// says why. UDS used to rebuild it per frame on the node-wide token (128
    /// builds here).
    #[tokio::test]
    async fn a_connection_arms_its_teardown_once() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("review.sock");
        let listener = TokioUnixListener::bind(&path).unwrap();
        let client_path = path.clone();
        let client = tokio::spawn(async move {
            let mut s = UnixStream::connect(client_path).await.unwrap();
            while TcpFrameCodec::encode_frame(&mut s, MessageType::Response, b"h", b"p")
                .await
                .is_ok()
            {}
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (adapter, streams) = make_channels();
        let shutdown_state = ShutdownState::new();
        let ctx = UdsConnectionContext {
            adapter,
            error_handler: Arc::new(TestErrorHandler),
            shutdown_state: shutdown_state.clone(),
            transport_key: "uds".to_string(),
            metrics: None,
            shrink_threshold: usize::MAX,
        };
        let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handler = tokio::spawn(
            TEARDOWN_ARMS.scope(arms.clone(), UdsListener::handle_connection(stream, ctx)),
        );
        for _ in 0..50 {
            tokio::time::timeout(Duration::from_secs(5), streams.response_stream.recv_async())
                .await
                .expect("frame routed")
                .unwrap();
        }
        let responses = streams.response_stream.clone();
        let drainer = tokio::spawn(async move { while responses.recv_async().await.is_ok() {} });
        assert!(!client.is_finished(), "the client must still be sending");
        shutdown_state.teardown_token().cancel();
        tokio::time::timeout(Duration::from_secs(5), handler)
            .await
            .expect("teardown ends the loop")
            .unwrap()
            .unwrap();
        assert_eq!(arms.load(std::sync::atomic::Ordering::Relaxed), 1);
        drop(streams);
        client.abort();
        drainer.abort();
    }

    #[test]
    fn test_builder_requires_fields() {
        let result = UdsListener::builder().build();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_builder_with_all_fields() {
        let (adapter, _streams) = make_channels();
        let error_handler = Arc::new(TestErrorHandler);

        let result = UdsListener::builder()
            .socket_path(PathBuf::from("/tmp/test-uds-listener.sock"))
            .adapter(adapter)
            .error_handler(error_handler)
            .build();

        assert!(result.is_ok());
    }
}
