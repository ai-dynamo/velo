// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! UDS transport implementation
//!
//! Structural mirror of the TCP transport (`tcp/transport.rs`), replacing
//! `TcpStream`/`TcpListener` with `UnixStream`/`UnixListener`.
//! Reuses `TcpFrameCodec` for framing since it operates on any `AsyncRead + AsyncWrite`.

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::transports::transport::{
    HealthCheckError, SendBackpressure, ShutdownState, TransportError, TransportErrorHandler,
    try_send_or_backpressure,
};
use velo_ext::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use super::listener::{UdsListener, default_shrink_threshold};
use crate::transports::coalesce::{
    Coalescable, WriterFailure, WriterObserver, run_coalescing_writer,
};

/// UDS transport with lock-free concurrent access
///
/// Mirrors `TcpTransport` but uses Unix domain sockets.
pub struct UdsTransport {
    key: TransportKey,
    socket_path: PathBuf,
    local_address: WorkerAddress,

    // Shared mutable state with DashMap (lock-free)
    peers: Arc<DashMap<crate::InstanceId, PathBuf>>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,

    // Runtime handle for spawning tasks
    runtime: OnceLock<tokio::runtime::Handle>,

    // Shutdown coordination
    cancel_token: CancellationToken,
    shutdown_state: OnceLock<ShutdownState>,

    // Send channel capacity for backpressure
    channel_capacity: usize,

    // Connect timeout for outbound connections
    connect_timeout: Duration,
    metrics: OnceLock<std::sync::Arc<dyn velo_ext::TransportObservability>>,

    // Listener read-buffer shrink threshold (bytes). Plumbed into UdsListener
    // at start() time. Resolved from env or default in new().
    shrink_threshold: usize,
}

/// Handle to a connection's writer task
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
}

/// Task sent to writer task containing pre-encoded frame
struct SendTask {
    msg_type: MessageType,
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
}

impl SendTask {
    fn on_error(self, error: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, error.into());
    }
}

impl UdsTransport {
    /// Create a new UDS transport
    pub fn new(
        socket_path: PathBuf,
        key: TransportKey,
        local_address: WorkerAddress,
        channel_capacity: usize,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            key,
            socket_path,
            local_address,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity,
            connect_timeout,
            metrics: OnceLock::new(),
            shrink_threshold: default_shrink_threshold(),
        }
    }

    /// Get the socket path this transport is bound to
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Optional: Pre-establish connection after registration
    pub fn ensure_connected(&self, instance_id: crate::InstanceId) -> Result<()> {
        self.get_or_create_connection(instance_id)?;
        Ok(())
    }

    /// Get or create a connection to a peer (lazy initialization)
    fn get_or_create_connection(&self, instance_id: crate::InstanceId) -> Result<ConnectionHandle> {
        // Fast path: connection already exists and is alive
        if let Some(handle) = self.connections.get(&instance_id) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            // Stale — drop guard before mutating the map
            drop(handle);
            self.connections
                .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
            self.update_connection_gauge();
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;

        // Atomic check-and-insert via entry API
        let handle = match self.connections.entry(instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !entry.get().tx.is_disconnected() {
                    entry.get().clone()
                } else {
                    // Stale entry — replace in-place with a fresh connection
                    let handle = self.create_connection(instance_id, rt)?;
                    entry.insert(handle.clone());
                    self.update_connection_gauge();
                    handle
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let handle = self.create_connection(instance_id, rt)?;
                entry.insert(handle.clone());
                self.update_connection_gauge();
                handle
            }
        };

        Ok(handle)
    }

    /// Create a new connection handle and spawn the writer task.
    fn create_connection(
        &self,
        instance_id: crate::InstanceId,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let path = self
            .peers
            .get(&instance_id)
            .ok_or(TransportError::PeerNotRegistered(instance_id))?
            .value()
            .clone();

        let (tx, rx) = flume::bounded(self.channel_capacity);
        let handle = ConnectionHandle { tx };

        let cancel = self.cancel_token.clone();
        let conns = Arc::clone(&self.connections);
        let connect_timeout = self.connect_timeout;
        let metrics = self.metrics.get().cloned();
        debug!("Created new UDS connection to {} ({:?})", instance_id, path);
        rt.spawn(connection_writer_task(
            path,
            instance_id,
            rx,
            conns,
            cancel,
            connect_timeout,
            metrics,
        ));
        Ok(handle)
    }

    fn update_peer_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_registered_peers(self.peers.len());
        }
    }

    fn update_connection_gauge(&self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.set_active_connections(self.connections.len());
        }
    }

    /// Slow path: establish (or reuse) a connection, then enqueue via the
    /// shared backpressure helper.
    fn slow_path_send(
        &self,
        instance_id: crate::InstanceId,
        send_msg: SendTask,
    ) -> Result<(), SendBackpressure> {
        if self.runtime.get().is_none() {
            send_msg.on_error("Transport not started");
            return Ok(());
        }
        let handle = match self.get_or_create_connection(instance_id) {
            Ok(h) => h,
            Err(e) => {
                send_msg.on_error(format!("Failed to create connection: {}", e));
                return Ok(());
            }
        };
        let r = try_send_or_backpressure(
            &handle.tx,
            send_msg,
            |msg| msg.on_error("Connection closed immediately"),
            |msg| msg.on_error("Connection closed"),
        );
        if let Some(m) = self.metrics.get()
            && r.is_err()
        {
            m.record_send_backpressure();
        }
        r
    }
}

impl Transport for UdsTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        // Get endpoint from peer's address
        let endpoint = peer_info
            .worker_address()
            .get_entry(&self.key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;

        // Parse UDS endpoint (expected format: "uds:///path/to/socket" or "/path/to/socket")
        let path = parse_uds_endpoint(&endpoint).map_err(|e| {
            error!("Failed to parse UDS endpoint: {}", e);
            TransportError::InvalidEndpoint
        })?;

        // Visibility gate: UDS is only usable if the peer's socket is reachable
        // in our mount namespace. A missing path is the normal cross-host case;
        // a non-socket file means the path is in use by something else (stale
        // regular file, directory). Reject with NoEndpoint so the backend's
        // priority sort can promote a different transport (e.g. TCP).
        match std::fs::metadata(&path) {
            Ok(m) if m.file_type().is_socket() => {}
            Ok(_) => {
                debug!(
                    "UDS path {:?} exists but is not a socket; rejecting UDS for peer {}",
                    path,
                    peer_info.instance_id()
                );
                return Err(TransportError::NoEndpoint);
            }
            Err(_) => {
                debug!(
                    "UDS path {:?} not visible on this host; rejecting UDS for peer {}",
                    path,
                    peer_info.instance_id()
                );
                return Err(TransportError::NoEndpoint);
            }
        }

        // Store peer path
        self.peers.insert(peer_info.instance_id(), path.clone());
        self.update_peer_gauge();

        debug!("Registered peer {} at {:?}", peer_info.instance_id(), path);

        Ok(())
    }

    #[inline]
    fn send_message(
        &self,
        instance_id: crate::InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure> {
        let send_msg = SendTask {
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        // Fast path: try existing connection.
        if let Some(handle) = self.connections.get(&instance_id) {
            match handle.tx.try_send(send_msg) {
                Ok(()) => return Ok(()),
                Err(flume::TrySendError::Full(send_msg)) => {
                    if let Some(m) = self.metrics.get() {
                        m.record_send_backpressure();
                    }
                    let tx = handle.tx.clone();
                    return Err(SendBackpressure::new(Box::pin(async move {
                        if let Err(flume::SendError(m)) = tx.send_async(send_msg).await {
                            m.on_error("Connection closed");
                        }
                    })));
                }
                Err(flume::TrySendError::Disconnected(send_msg_out)) => {
                    drop(handle);
                    self.connections
                        .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
                    self.update_connection_gauge();
                    return self.slow_path_send(instance_id, send_msg_out);
                }
            }
        }
        self.slow_path_send(instance_id, send_msg)
    }

    fn start(
        &self,
        _instance_id: crate::InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        // Store runtime handle for use in send_message
        self.runtime.set(rt.clone()).ok();

        // Capture shutdown state from the adapter
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();

        let socket_path = self.socket_path.clone();
        let shutdown_state = channels.shutdown_state.clone();

        Box::pin(async move {
            struct DefaultErrorHandler;
            impl TransportErrorHandler for DefaultErrorHandler {
                fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
                    warn!("UDS transport error: {}", error);
                }
            }

            // Remove a stale socket file only when it is safe to do so.
            if socket_path.exists() {
                let is_socket = std::fs::metadata(&socket_path)
                    .map(|m| m.file_type().is_socket())
                    .unwrap_or(false);
                if !is_socket {
                    anyhow::bail!(
                        "path {:?} exists and is not a Unix domain socket",
                        socket_path
                    );
                }
                // Probe liveness: a successful connect means a live listener owns it.
                match tokio::time::timeout(
                    Duration::from_millis(100),
                    UnixStream::connect(&socket_path),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        anyhow::bail!(
                            "a live UDS listener is already running at {:?}",
                            socket_path
                        );
                    }
                    _ => {
                        // Stale (connection refused / timeout) — safe to unlink.
                        std::fs::remove_file(&socket_path).ok();
                    }
                }
            }

            // Build and bind before spawning so that start() only returns Ok
            // after the OS-level bind succeeds.
            let uds_listener = UdsListener::builder()
                .socket_path(socket_path.clone())
                .adapter(channels)
                .error_handler(Arc::new(DefaultErrorHandler))
                .shutdown_state(shutdown_state)
                .transport_key(self.key.as_str())
                .metrics(self.metrics.get().cloned())
                .shrink_threshold(self.shrink_threshold)
                .build()?;

            let bound_listener = uds_listener.bind()?;

            rt.spawn(async move {
                if let Err(e) = bound_listener.serve().await {
                    error!("UDS listener error: {}", e);
                }
            });

            info!("UDS transport started on {:?}", socket_path);

            Ok(())
        })
    }

    fn begin_drain(&self) {
        if let Some(state) = self.shutdown_state.get() {
            state.begin_drain();
        }
    }

    fn shutdown(&self) {
        info!("Shutting down UDS transport");

        // Cancel the teardown token (Phase 3) to stop the listener and connection handlers
        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        self.cancel_token.cancel();

        // Clear connections
        self.connections.clear();
        self.update_connection_gauge();
    }

    fn set_observability(
        &self,
        observability: std::sync::Arc<dyn velo_ext::TransportObservability>,
    ) {
        let _ = self.metrics.set(observability);
        self.update_peer_gauge();
        self.update_connection_gauge();
    }

    fn check_health(
        &self,
        instance_id: crate::InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            let connection_exists = self.connections.contains_key(&instance_id);

            if let Some(handle) = self.connections.get(&instance_id) {
                if !handle.tx.is_disconnected() {
                    return Ok(());
                }
                // Channel is disconnected — drop guard and remove stale entry
                drop(handle);
                self.connections
                    .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
            }

            // No existing connection or connection is dead - verify peer is reachable
            let path = self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value()
                .clone();

            // Try to connect (and immediately drop) to verify peer is reachable
            match tokio::time::timeout(timeout, UnixStream::connect(&path)).await {
                Ok(Ok(_stream)) => {
                    if connection_exists {
                        Ok(())
                    } else {
                        Err(HealthCheckError::NeverConnected)
                    }
                }
                Ok(Err(_)) => Err(HealthCheckError::ConnectionFailed),
                Err(_) => Err(HealthCheckError::Timeout),
            }
        })
    }
}

/// Connection writer task for UDS
///
/// Mirrors the TCP connection_writer_task. Cleanup (draining queued messages
/// and removing the stale map entry) always runs, even if the initial connect fails.
async fn connection_writer_task(
    path: PathBuf,
    instance_id: crate::InstanceId,
    rx: flume::Receiver<SendTask>,
    connections: Arc<DashMap<crate::InstanceId, ConnectionHandle>>,
    cancel_token: CancellationToken,
    connect_timeout: Duration,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
) -> Result<()> {
    let result =
        connection_writer_inner(&path, instance_id, &rx, &cancel_token, connect_timeout).await;

    // Always drain queued messages and notify their error handlers.
    while let Ok(msg) = rx.try_recv() {
        msg.on_error("Connection closed");
    }

    // Drop the receiver so our sender half becomes disconnected, then remove
    // the stale entry. The predicate ensures we only remove our own entry —
    // a replacement connection's tx will still be connected.
    drop(rx);
    connections.remove_if(&instance_id, |_, h| h.tx.is_disconnected());
    if let Some(metrics) = metrics.as_ref() {
        metrics.set_active_connections(connections.len());
    }

    debug!("UDS connection to {} ({:?}) closed", instance_id, path);

    result
}

/// Inner loop: connect and send frames until the channel closes or a write error occurs.
async fn connection_writer_inner(
    path: &Path,
    instance_id: crate::InstanceId,
    rx: &flume::Receiver<SendTask>,
    cancel_token: &CancellationToken,
    connect_timeout: Duration,
) -> Result<()> {
    debug!("Connecting to UDS {:?}", path);

    let mut stream = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = tokio::time::timeout(connect_timeout, UnixStream::connect(path)) => {
            res.context("UDS connect timeout")?.context("UDS connect failed")?
        },
    };

    // Set large buffers for high throughput (2MB each)
    let sock = socket2::SockRef::from(&stream);
    if let Err(e) = sock.set_send_buffer_size(2_097_152) {
        warn!("Failed to set UDS send buffer size: {}", e);
    }
    if let Err(e) = sock.set_recv_buffer_size(2_097_152) {
        warn!("Failed to set UDS recv buffer size: {}", e);
    }

    debug!("Connected to UDS {:?}", path);

    // Main send loop. Coalescing writer, identical in behaviour to the TCP one
    // because it *is* the TCP one — see `crate::transports::coalesce`. Messages
    // still queued when this returns are reported by the caller's drain.
    run_coalescing_writer(
        &mut stream,
        rx,
        // The channel already carries the writer's item type.
        std::convert::identity,
        Some(cancel_token),
        &UdsWriterObserver { instance_id, path },
    )
    .await;

    Ok(())
}

impl Coalescable for SendTask {
    /// A staged task *is* its own failure token: what
    /// [`TransportErrorHandler::on_error`] needs — the header, the payload,
    /// and the handler — is every field but a one-byte `Copy` enum, and all
    /// three are refcounted handles. Splitting them into a second struct would
    /// have the same footprint, so the writer just keeps the task. Retaining
    /// it holds no payload bytes beyond the ones the sender already owns.
    type FailureToken = Self;

    fn msg_type(&self) -> MessageType {
        self.msg_type
    }

    fn header(&self) -> &[u8] {
        &self.header
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn into_failure_token(self) -> Self {
        self
    }

    fn fail(token: Self, reason: &str) {
        token.on_error(format!("Failed to write to UDS stream: {}", reason));
    }
}

/// Attaches the connection's identity to the writer loop's log lines.
struct UdsWriterObserver<'a> {
    instance_id: crate::InstanceId,
    path: &'a Path,
}

impl WriterObserver for UdsWriterObserver<'_> {
    fn on_failure(&self, kind: WriterFailure, err: &std::io::Error, frames: usize) {
        match kind {
            WriterFailure::Write => error!(
                "Write error to {} ({:?}): {} ({} message(s) in batch)",
                self.instance_id, self.path, err, frames
            ),
            WriterFailure::Encode => error!(
                "Encode error to {} ({:?}): {}",
                self.instance_id, self.path, err
            ),
        }
    }
}

/// Parse a UDS endpoint string into a PathBuf
///
/// Accepts formats:
/// - "uds:///path/to/socket"
/// - "/path/to/socket"
fn parse_uds_endpoint(endpoint: &[u8]) -> Result<PathBuf> {
    let endpoint_str = std::str::from_utf8(endpoint).context("endpoint is not valid UTF-8")?;

    // Strip "uds://" prefix if present
    let path_str = endpoint_str.strip_prefix("uds://").unwrap_or(endpoint_str);

    if path_str.is_empty() {
        anyhow::bail!("empty UDS socket path");
    }

    Ok(PathBuf::from(path_str))
}

/// Builder for UdsTransport
pub struct UdsTransportBuilder {
    socket_path: Option<PathBuf>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    shrink_threshold: Option<usize>,
}

impl UdsTransportBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            socket_path: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            shrink_threshold: None,
        }
    }

    /// Set the socket path
    pub fn socket_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.socket_path = Some(path.into());
        self
    }

    /// Set the transport key
    pub fn key(mut self, key: TransportKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the channel capacity for backpressure (default: 256)
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the connect timeout for outbound connections (default: 5s)
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the per-connection read-buffer shrink threshold (bytes).
    ///
    /// If a single oversized inbound frame causes the listener's `BytesMut`
    /// read buffer to grow past this many bytes, the buffer will be reset back
    /// to a small capacity the next time it fully drains. Defaults to 8 MB,
    /// overridable at process start via `VELO_UDS_SHRINK_THRESHOLD`.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = Some(bytes);
        self
    }

    /// Build the UdsTransport
    pub fn build(self) -> Result<UdsTransport> {
        let socket_path = self
            .socket_path
            .ok_or_else(|| anyhow::anyhow!("socket_path is required"))?;
        let key = self.key.unwrap_or_else(|| TransportKey::from("uds"));

        let local_endpoint = format!("uds://{}", socket_path.display());
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), local_endpoint.as_bytes().to_vec())?;
        let local_address = addr_builder.build()?;

        let mut transport = UdsTransport::new(
            socket_path,
            key,
            local_address,
            self.channel_capacity,
            self.connect_timeout,
        );
        if let Some(t) = self.shrink_threshold {
            transport.shrink_threshold = t;
        }
        Ok(transport)
    }
}

impl Default for UdsTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// `#[path]` keeps the tests beside their siblings as `uds/tests.rs`; the
// default resolution would bury them in a one-file `uds/transport/` directory.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
