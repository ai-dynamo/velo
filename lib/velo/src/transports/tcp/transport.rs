// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! High-performance TCP transport with single-threaded optimizations
//!
//! This implementation uses Rc+RefCell+LocalSet for maximum performance on a single CPU core.
//! All operations run on the same thread as the TCP listener for optimal cache locality.

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::transports::transport::{
    AdmissionError, AdmissionGate, HealthCheckError, SendOutcome, ShutdownState, TransportError,
    TransportErrorHandler,
};
use crate::transports::utils::interfaces::{
    InterfaceEndpoint, InterfaceFilter, parse_endpoints, resolve_advertise_endpoints,
    select_best_endpoint,
};
use velo_ext::{MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress};

use super::framing::DEFAULT_MAX_FRAME_SIZE;
use super::listener::TcpListener;
use super::writer::TcpWriterObserver;
use crate::transports::coalesce::{EgressMetrics, run_coalescing_writer};
use crate::transports::ingress::{DialedReaderContext, run_dialed_reader};

/// One connection per peer and lane. Lane 0 is the only lane unless the
/// builder asked for more.
type LaneKey = (crate::InstanceId, u16);

/// High-performance TCP transport with lock-free concurrent access
///
/// This transport uses `DashMap` for lock-free concurrent access to connection state.
/// Tasks are spawned using `tokio::spawn` for compatibility with the `Transport` trait.
/// For single-threaded performance, run the entire transport in a `LocalSet` context.
///
/// It keeps up to [`TcpTransportBuilder::lanes`] connections to each peer, one
/// for each lane used.
pub struct TcpTransport {
    // Identity (immutable, no wrapper needed)
    key: TransportKey,
    bind_addr: SocketAddr,
    local_address: WorkerAddress,

    // Shared mutable state with DashMap (lock-free)
    peers: Arc<DashMap<crate::InstanceId, SocketAddr>>,
    connections: Arc<DashMap<LaneKey, ConnectionHandle>>,

    // Ordered channels to each peer, each its own connection, dialed on the
    // first send on that lane. Set by the builder; `new()` gives 1.
    lanes: NonZeroU16,

    // Runtime handle for spawning tasks
    runtime: OnceLock<tokio::runtime::Handle>,

    // Shutdown coordination
    cancel_token: CancellationToken,
    shutdown_state: OnceLock<ShutdownState>,

    // Send channel capacity for backpressure
    channel_capacity: usize,

    // Connect timeout for outbound connections
    connect_timeout: Duration,

    // Optional pre-bound listener (used for tests to avoid port races)
    listener: Mutex<Option<std::net::TcpListener>>,

    // Cached local interfaces for endpoint selection
    local_interfaces: OnceLock<Vec<InterfaceEndpoint>>,

    // NUMA hint for topology-aware NIC selection
    numa_hint: Option<u32>,

    // Optional shared metrics.
    metrics: OnceLock<std::sync::Arc<dyn velo_ext::TransportObservability>>,

    // Listener read-buffer shrink threshold (bytes). Plumbed into TcpListener
    // at start() time. Resolved from env or default in new().
    shrink_threshold: usize,

    // Context for each dialed connection's read loop (the path that surfaces
    // the peer's ShuttingDown drain rejections). Set in start(), before
    // `runtime` — send paths gate on `runtime`, so every connection writer
    // observes this as set.
    dialed_ctx: OnceLock<DialedReaderContext>,
}

/// Handle to a connection's writer task.
///
/// One handle is one connection epoch. The gate is the only way frames enter
/// `tx`, so per-connection FIFO holds no matter how many tasks are sending;
/// `tx` is retained purely for the liveness probes (`is_disconnected`) that
/// decide when an epoch is stale.
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
    gate: AdmissionGate<SendTask>,
}

impl ConnectionHandle {
    /// Kill this epoch: every frame still queued behind the gate belonged to a
    /// connection that no longer exists, so none of them may ride the
    /// successor.
    fn retire(&self) {
        self.gate.fail_all(AdmissionError::ConnectionReplaced);
    }
}

/// Task sent to writer task containing pre-encoded frame
///
/// Fields are `pub(super)`: `writer.rs`'s `impl Coalescable for SendTask`
/// reads them directly, and constructs nothing here, so `tcp` is the visibility
/// this type needs rather than a public one.
pub(super) struct SendTask {
    pub(super) msg_type: MessageType,
    pub(super) header: Bytes,
    pub(super) payload: Bytes,
    pub(super) on_error: Arc<dyn TransportErrorHandler>,
    /// When the transport accepted this frame, or `None` when nothing is
    /// watching.
    ///
    /// Stamped in `send_message`, before the frame is offered to the gate, so
    /// the writer's `velo_transport_egress_queue_wait_seconds` covers the
    /// gate's pending queue as well as the bounded channel — under load the
    /// gate is where the frames actually are. The clock read is skipped
    /// entirely when the transport has no observability handle, because the
    /// writer would have nowhere to report it.
    pub(super) queued_at: Option<Instant>,
}

impl SendTask {
    pub(super) fn on_error(self, error: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, error.into());
    }
}

impl TcpTransport {
    /// Create a new TCP transport bound to `bind_addr` with the given transport key.
    ///
    /// An optional pre-bound `listener` can be provided (useful for tests binding
    /// to port 0). `channel_capacity` controls backpressure on per-connection
    /// writer channels (default 256).
    pub fn new(
        bind_addr: SocketAddr,
        key: TransportKey,
        local_address: WorkerAddress,
        channel_capacity: usize,
        connect_timeout: Duration,
        listener: Option<std::net::TcpListener>,
        numa_hint: Option<u32>,
    ) -> Self {
        Self {
            key,
            bind_addr,
            local_address,
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            lanes: NonZeroU16::MIN,
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity,
            connect_timeout,
            listener: Mutex::new(listener),
            local_interfaces: OnceLock::new(),
            numa_hint,
            metrics: OnceLock::new(),
            shrink_threshold: super::listener::default_shrink_threshold(),
            dialed_ctx: OnceLock::new(),
        }
    }

    /// Optional: Pre-establish connection after registration
    ///
    /// This can be called after `register()` to eagerly establish the TCP connection
    /// instead of waiting for the first `send_message()` call. It connects
    /// lane 0, the lane `send_message()` uses.
    pub fn ensure_connected(&self, instance_id: crate::InstanceId) -> Result<()> {
        self.get_or_create_connection((instance_id, 0))?;
        Ok(())
    }

    /// Drop a dead connection's map entry, retiring its epoch first.
    ///
    /// The predicate keeps us from evicting a successor that another task
    /// installed in the meantime; retiring before the entry disappears is what
    /// guarantees the old epoch's queued frames fail rather than linger.
    fn reap_stale_connection(&self, key: LaneKey) {
        if let Some((_, stale)) = self
            .connections
            .remove_if(&key, |_, h| h.tx.is_disconnected())
        {
            stale.retire();
            self.update_connection_gauge();
        }
    }

    /// Get or create the connection for one peer and lane (lazy
    /// initialization)
    fn get_or_create_connection(&self, key: LaneKey) -> Result<ConnectionHandle> {
        // Fast path: connection already exists and is alive
        if let Some(handle) = self.connections.get(&key) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            // Stale — drop guard before mutating the map
            drop(handle);
            self.reap_stale_connection(key);
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;
        self.install_connection(key, rt)
    }

    /// Put a live connection in the map for `key`: the one already there if
    /// it is live, else a new one.
    fn install_connection(
        &self,
        key: LaneKey,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        // Atomic check-and-insert via entry API
        let handle = match self.connections.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if !entry.get().tx.is_disconnected() {
                    entry.get().clone()
                } else {
                    // Stale entry — retire the dead epoch before the successor
                    // is installed, so no frame from the old connection can be
                    // observed as pending on the new one.
                    entry.get().retire();
                    let handle = self.create_connection(key, rt)?;
                    entry.insert(handle.clone());
                    handle
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let handle = self.create_connection(key, rt)?;
                entry.insert(handle.clone());
                handle
            }
        };
        // After the match, not inside it: the gauge reads `len()`, which
        // read-locks every shard, and an occupied entry still holds its
        // shard's write lock, which is not reentrant. The thread then waits
        // on itself.
        self.update_connection_gauge();
        Ok(handle)
    }

    /// Create a new connection handle and spawn the writer task.
    fn create_connection(
        &self,
        key: LaneKey,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let (instance_id, lane) = key;
        let addr = *self
            .peers
            .get(&instance_id)
            .ok_or(TransportError::PeerNotRegistered(instance_id))?
            .value();

        let (tx, rx) = flume::bounded(self.channel_capacity);
        let handle = ConnectionHandle {
            gate: AdmissionGate::new(tx.clone(), rt.clone()),
            tx,
        };

        rt.spawn(connection_writer_task(
            addr,
            key,
            rx,
            WriterTaskContext {
                connections: Arc::clone(&self.connections),
                cancel_token: self.cancel_token.clone(),
                connect_timeout: self.connect_timeout,
                reader_ctx: self.dialed_ctx.get().cloned(),
                metrics: self.metrics.get().cloned(),
            },
        ));

        debug!(
            "Created new connection to {} lane {} ({})",
            instance_id, lane, addr
        );
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

    /// Slow path: establish (or reuse) a connection, then offer the frame to
    /// its gate.
    ///
    /// A failure here is terminal for the frame, so it is reported through
    /// `on_error` and the send reports [`SendOutcome::Admitted`] — there is
    /// nothing for the caller to wait on.
    fn slow_path_send(&self, key: LaneKey, send_msg: SendTask) -> SendOutcome {
        if self.runtime.get().is_none() {
            send_msg.on_error("Transport not started");
            return SendOutcome::Admitted;
        }
        let handle = match self.get_or_create_connection(key) {
            Ok(h) => h,
            Err(e) => {
                send_msg.on_error(format!("Failed to create connection: {}", e));
                return SendOutcome::Admitted;
            }
        };
        self.admit(&handle, send_msg)
    }

    /// Offer one frame to a connection's gate, counting the saturated case.
    fn admit(&self, handle: &ConnectionHandle, send_msg: SendTask) -> SendOutcome {
        let outcome = handle.gate.send(send_msg);
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
    }
}

impl Transport for TcpTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    /// The codec's frame ceiling, which is already stated in the units this
    /// method wants: `TcpFrameCodec::validate_lengths_limit` caps
    /// `header_len + payload_len`, and the 11-byte preamble is written
    /// *outside* that sum. There is nothing to subtract.
    ///
    /// Static and identical for every peer — `build_preamble` validates
    /// against [`DEFAULT_MAX_FRAME_SIZE`] itself, so a codec constructed with
    /// `with_max_frame_size` moves only what this process will *decode*, never
    /// what it will encode.
    fn max_message_size(&self, _target: crate::InstanceId) -> Option<usize> {
        Some(DEFAULT_MAX_FRAME_SIZE as usize)
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        // Get endpoint from peer's address
        let endpoint = peer_info
            .worker_address()
            .get_entry(&self.key)
            .map_err(|_| TransportError::NoEndpoint)?
            .ok_or(TransportError::NoEndpoint)?;

        // Parse endpoints (supports both new multi-endpoint and legacy formats)
        let remote_endpoints = parse_endpoints(&endpoint).map_err(|e| {
            error!("Failed to parse TCP endpoint: {}", e);
            TransportError::InvalidEndpoint
        })?;

        // Lazy-init local interfaces for endpoint selection
        let local = self.local_interfaces.get_or_init(|| {
            resolve_advertise_endpoints(self.bind_addr, &InterfaceFilter::All).unwrap_or_default()
        });

        // Select best endpoint based on NUMA + subnet affinity
        let addr = select_best_endpoint(&remote_endpoints, local, self.numa_hint)
            .ok_or(TransportError::InvalidEndpoint)?;

        // Store peer address
        self.peers.insert(peer_info.instance_id(), addr);
        self.update_peer_gauge();

        debug!("Registered peer {} at {}", peer_info.instance_id(), addr);

        Ok(())
    }

    fn lanes(&self, _target: crate::InstanceId) -> NonZeroU16 {
        self.lanes
    }

    #[inline]
    fn send_message(
        &self,
        instance_id: crate::InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: std::sync::Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        self.send_message_on_lane(instance_id, 0, header, payload, message_type, on_error)
    }

    /// Each lane is its own connection with its own gate. If the connection
    /// of a lane is dead, the next send on that lane dials a new one. A lane
    /// never uses the connection of another lane, because that can reorder
    /// it.
    #[inline]
    fn send_message_on_lane(
        &self,
        instance_id: crate::InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: std::sync::Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        // Modulo, not a clamp: a caller that maps flows with `flow % lanes`
        // and one that passes a raw flow number land on the same lane.
        let key: LaneKey = (instance_id, lane % self.lanes);
        let send_msg = SendTask {
            msg_type: message_type,
            header,
            payload,
            on_error,
            // One clock read per send, and only when a writer will report it.
            queued_at: self.metrics.get().map(|_| Instant::now()),
        };

        // Fast path: an established connection. The liveness probe comes first
        // because a dead epoch's gate would swallow the frame; the gate itself
        // then decides admitted-vs-queued, so there is no `try_send` here that
        // could overtake a frame already queued behind it.
        if let Some(handle) = self.connections.get(&key) {
            let live = (!handle.tx.is_disconnected()).then(|| handle.clone());
            // Release the shard guard before either admitting (which may spawn
            // a driver) or mutating the map.
            drop(handle);
            match live {
                Some(handle) => return self.admit(&handle, send_msg),
                None => self.reap_stale_connection(key),
            }
        }
        self.slow_path_send(key, send_msg)
    }

    fn start(
        &self,
        _instance_id: crate::InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        // Dialed-reader context first: send paths gate on `runtime`, so
        // setting this before `runtime` guarantees every connection writer
        // sees it.
        self.dialed_ctx
            .set(DialedReaderContext {
                adapter: channels.clone(),
                error_handler: std::sync::Arc::new(DialedReaderErrorHandler),
                transport_key: self.key.as_str().to_string(),
                shrink_threshold: self.shrink_threshold,
            })
            .ok();

        // Store runtime handle for use in send_message
        self.runtime.set(rt.clone()).ok();

        // Capture shutdown state from the adapter
        self.shutdown_state
            .set(channels.shutdown_state.clone())
            .ok();

        let bind_addr = self.bind_addr;
        let shutdown_state = channels.shutdown_state.clone();
        // Take ownership of the listener (if present) - we can only start once
        let listener = self
            .listener
            .lock()
            .expect("Listener mutex poisoned")
            .take();

        Box::pin(async move {
            // Create error handler that routes to the transport error handler
            struct DefaultErrorHandler;
            impl TransportErrorHandler for DefaultErrorHandler {
                fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
                    warn!("Transport error: {}", error);
                }
            }

            // Start TCP listener
            let tcp_listener = TcpListener::builder()
                .bind_addr(bind_addr)
                .adapter(channels)
                .error_handler(std::sync::Arc::new(DefaultErrorHandler))
                .shutdown_state(shutdown_state)
                .listener(listener)
                .transport_key(self.key.as_str())
                .metrics(self.metrics.get().cloned())
                .shrink_threshold(self.shrink_threshold)
                .build()?;

            rt.spawn(async move {
                if let Err(e) = tcp_listener.serve().await {
                    error!("TCP listener error: {}", e);
                }
            });

            info!("TCP transport started on {}", bind_addr);

            Ok(())
        })
    }

    fn begin_drain(&self) {
        // Per-frame gate in the listener handles drain — no-op here.
    }

    fn shutdown(&self) {
        info!("Shutting down TCP transport");

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
            // Any live lane shows that the peer is reachable. A caller can
            // send on any lane, so a peer can have no connection on lane 0.
            // A lane whose writer has exited (socket closed) has a
            // disconnected channel. Reap it: its next send dials it again.
            let mut connection_exists = false;
            for lane in 0..self.lanes.get() {
                let key: LaneKey = (instance_id, lane);
                let Some(handle) = self.connections.get(&key) else {
                    continue;
                };
                connection_exists = true;
                if !handle.tx.is_disconnected() {
                    return Ok(());
                }
                drop(handle);
                self.reap_stale_connection(key);
            }

            // No existing connection or connection is dead - verify peer is reachable
            let addr = *self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value();

            // Try to connect (and immediately drop) to verify peer is reachable
            match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
                Ok(Ok(_stream)) => {
                    // Connection successful, drop immediately
                    // If we never had a connection before, report NeverConnected
                    // If we had one before that failed, report Ok (peer is reachable now)
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

/// Per-connection configuration handed to [`connection_writer_task`].
struct WriterTaskContext {
    connections: Arc<DashMap<LaneKey, ConnectionHandle>>,
    cancel_token: CancellationToken,
    connect_timeout: Duration,
    reader_ctx: Option<DialedReaderContext>,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

/// Connection writer task
///
/// This task runs on the LocalSet and handles writing framed bytes to the TCP stream.
/// It receives pre-encoded frames via a flume channel and writes them to the socket.
///
/// Cleanup (draining queued messages and removing the stale map entry) always runs,
/// even if the initial TCP connect fails.
async fn connection_writer_task(
    addr: SocketAddr,
    key: LaneKey,
    rx: flume::Receiver<SendTask>,
    ctx: WriterTaskContext,
) -> Result<()> {
    let WriterTaskContext {
        connections,
        cancel_token,
        connect_timeout,
        reader_ctx,
        metrics,
    } = ctx;
    let result = connection_writer_inner(
        addr,
        key,
        &rx,
        &cancel_token,
        connect_timeout,
        reader_ctx,
        metrics.clone(),
    )
    .await;

    // Always drain queued messages and notify their error handlers.
    //
    // TODO: There is a tiny race between the drain finishing and `drop(rx)`:
    // a sender on another thread could `try_send` successfully in that window,
    // and the message would be silently dropped when rx is destroyed. Closing
    // this fully would require swapping the map entry with a "poisoned" handle
    // (a disconnected tx) before draining, so fast-path senders see a failure
    // instead. Not worth the complexity today — at most one message is affected,
    // and async senders already get `SendError` once rx is dropped.
    while let Ok(msg) = rx.try_recv() {
        msg.on_error("Connection closed");
    }

    // Drop the receiver so our sender half becomes disconnected, then remove
    // the stale entry. The predicate ensures we only remove our own entry —
    // a replacement connection's tx will still be connected.
    //
    // Retiring the gate is what fails frames still queued behind it. Dropping
    // `rx` would eventually fail them too (the driver's `send_async` sees a
    // closed channel), but `ConnectionReplaced` names the cause and lands
    // without waiting on the driver. If the entry was already replaced, the
    // successor's gate is a different one and the old gate's frames take the
    // closed-channel route instead.
    drop(rx);
    if let Some((_, stale)) = connections.remove_if(&key, |_, h| h.tx.is_disconnected()) {
        stale.retire();
    }
    if let Some(metrics) = metrics.as_ref() {
        metrics.set_active_connections(connections.len());
    }

    debug!("Connection to {} lane {} ({}) closed", key.0, key.1, addr);

    result
}

/// Inner loop: connect, configure the socket, and send frames until the channel
/// closes, a write error occurs, or the reader sees the peer close the socket.
async fn connection_writer_inner(
    addr: SocketAddr,
    (instance_id, lane): LaneKey,
    rx: &flume::Receiver<SendTask>,
    cancel_token: &CancellationToken,
    connect_timeout: Duration,
    reader_ctx: Option<DialedReaderContext>,
    metrics: Option<std::sync::Arc<dyn velo_ext::TransportObservability>>,
) -> Result<()> {
    debug!("Connecting to {} lane {} ({})", instance_id, lane, addr);

    let stream = tokio::select! {
        _ = cancel_token.cancelled() => return Ok(()),
        res = tokio::time::timeout(connect_timeout, TcpStream::connect(addr)) => {
            res.context("connect timeout")?.context("connect failed")?
        },
    };

    if let Err(e) = stream.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY: {}", e);
    }

    let sock = socket2::SockRef::from(&stream);
    if let Err(e) = sock.set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(10)),
    ) {
        warn!("Failed to set keepalive: {}", e);
    }

    // Safe to size buffers here: this side dialed the connection and has not
    // written a byte yet, so unlike the accept path there is no in-flight data
    // to race (see the listener for why that race collapses the window).
    if let Err(e) = sock.set_send_buffer_size(2_097_152) {
        warn!("Failed to set send buffer size: {}", e);
    }

    if let Err(e) = sock.set_recv_buffer_size(2_097_152) {
        warn!("Failed to set recv buffer size: {}", e);
    }

    debug!("Connected to {} lane {} ({})", instance_id, lane, addr);

    // The peer's listener replies on THIS socket when it rejects a Message
    // during drain (a ShuttingDown frame echoing the header). Split the
    // stream so those replies are read and routed into the adapter streams
    // instead of rotting unread in the kernel receive buffer. The reader and
    // writer share a child token: the reader cancels it on EOF/decode error
    // so the writer stops instead of pushing frames at a dead socket, and the
    // transport-level `cancel_token` still stops both through propagation.
    let (read_half, mut write_half) = stream.into_split();
    let conn_cancel = cancel_token.child_token();
    let reader = reader_ctx.map(|ctx| {
        tokio::spawn(run_dialed_reader(
            read_half,
            ctx,
            metrics.clone(),
            conn_cancel.clone(),
            format!("{} lane {} ({})", instance_id, lane, addr),
        ))
    });

    // Coalescing writer: several queued messages become one `write_all`. See
    // `crate::transports::coalesce` for why that is wire-compatible with an
    // unmodified peer and adds no latency, and `docs/src/concepts/batched-streaming.md`
    // for the wider rationale. Messages still queued when this returns are reported by
    // `connection_writer_task`'s drain.
    run_coalescing_writer(
        &mut write_half,
        rx,
        // The channel already carries the writer's item type.
        std::convert::identity,
        Some(&conn_cancel),
        &TcpWriterObserver {
            instance_id,
            lane,
            addr,
            egress: metrics.map(EgressMetrics::new),
        },
    )
    .await;

    // The writer ending is what ends the connection; abort the reader rather
    // than joining it so a frame mid-route cannot pin the socket open.
    if let Some(reader) = reader {
        reader.abort();
    }

    Ok(())
}

/// Routing failures on a dialed connection's read half have no per-send error
/// handler to invoke, so they are logged — the same policy as the listener's
/// default handler.
struct DialedReaderErrorHandler;

impl TransportErrorHandler for DialedReaderErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        warn!("Transport error: {}", error);
    }
}

/// Parse a TCP endpoint string into a SocketAddr (legacy format, used in tests).
#[cfg(test)]
fn parse_tcp_endpoint(endpoint: &[u8]) -> Result<SocketAddr> {
    use std::net::ToSocketAddrs;

    let endpoint_str = std::str::from_utf8(endpoint).context("endpoint is not valid UTF-8")?;

    // Strip "tcp://" prefix if present
    let addr_str = endpoint_str.strip_prefix("tcp://").unwrap_or(endpoint_str);

    // Parse as socket address
    let mut addrs = addr_str
        .to_socket_addrs()
        .context("failed to parse socket address")?;

    addrs
        .next()
        .ok_or_else(|| anyhow::anyhow!("no addresses resolved"))
}

/// Builder for TcpTransport
pub struct TcpTransportBuilder {
    bind_addr: Option<SocketAddr>,
    key: Option<TransportKey>,
    channel_capacity: usize,
    connect_timeout: Duration,
    listener: Option<std::net::TcpListener>,
    interface_filter: InterfaceFilter,
    numa_hint: Option<u32>,
    shrink_threshold: Option<usize>,
    lanes: NonZeroU16,
}

impl TcpTransportBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            bind_addr: None,
            key: None,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            listener: None,
            interface_filter: InterfaceFilter::default(),
            numa_hint: None,
            shrink_threshold: None,
            lanes: NonZeroU16::MIN,
        }
    }

    /// Set the bind address
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = Some(addr);
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

    /// Set the interface selection filter for multi-NIC environments.
    pub fn interface_filter(mut self, filter: InterfaceFilter) -> Self {
        self.interface_filter = filter;
        self
    }

    /// Set the NUMA node hint for topology-aware NIC selection.
    ///
    /// Callers typically resolve this via `dynamo_memory::numa::get_device_numa_node(gpu_id)`.
    pub fn numa_hint(mut self, node: u32) -> Self {
        self.numa_hint = Some(node);
        self
    }

    /// Override the per-connection read-buffer shrink threshold (bytes).
    ///
    /// If a single oversized inbound frame causes the listener's `BytesMut`
    /// read buffer to grow past this many bytes, the buffer will be reset back
    /// to a small capacity the next time it fully drains. Defaults to 8 MB,
    /// overridable at process start via `VELO_TCP_SHRINK_THRESHOLD`.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = Some(bytes);
        self
    }

    /// Lanes to each peer: up to this many connections, one for each lane
    /// used, dialed on the first send on that lane (default 1, at least 1).
    ///
    /// One connection is limited by its receiver: one reader task does the
    /// whole receive copy, and fixed socket buffers cap the TCP window. Each
    /// lane is its own connection, read by its own task, so N lanes spread
    /// that work over up to N cores. Order holds
    /// within a lane only, so a caller that uses lanes must keep each ordered
    /// flow on one lane (see `Transport::send_message_on_lane`).
    /// `send_message` uses lane 0.
    ///
    /// Only the dialing side's count matters: the listener accepts however
    /// many connections a peer opens, and reads each on its own task.
    pub fn lanes(mut self, lanes: u16) -> Self {
        self.lanes = NonZeroU16::new(lanes).unwrap_or(NonZeroU16::MIN);
        self
    }

    /// Use a pre-bound TcpListener instead of binding to a specific address
    ///
    /// This is useful for tests where you want to bind to port 0 and get an OS-assigned
    /// port without creating a race condition between binding and starting the transport.
    ///
    /// Note: This is mutually exclusive with `bind_addr()`. Using both will result in an error.
    pub fn from_listener(mut self, listener: std::net::TcpListener) -> Result<Self> {
        // Validate mutual exclusivity: can't use both bind_addr() and from_listener()
        if self.bind_addr.is_some() {
            anyhow::bail!(
                "Cannot use both bind_addr() and from_listener() - they are mutually exclusive"
            );
        }

        let addr = listener
            .local_addr()
            .context("Failed to get local address from listener")?;
        self.bind_addr = Some(addr);
        self.listener = Some(listener);
        Ok(self)
    }

    /// Build the TcpTransport
    pub fn build(self) -> Result<TcpTransport> {
        let key = self.key.unwrap_or_else(|| TransportKey::from("tcp"));

        // If we have a listener, use its address; otherwise pre-bind to resolve port 0.
        let (bind_addr, listener) = if let Some(listener) = self.listener {
            // Caller-provided listener: it is already live, so this is best
            // effort — connections whose handshake completed before this point
            // keep kernel-default autotuned buffers, which is safe.
            super::listener::size_listener_buffers(&listener);
            let addr = listener.local_addr()?;
            (addr, Some(listener))
        } else {
            let requested = self
                .bind_addr
                .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
            // Built by hand instead of std::net::TcpListener::bind so the
            // socket buffers are sized before listen() — accepted sockets
            // inherit them at handshake time (see `size_listener_buffers`).
            let domain = if requested.is_ipv4() {
                socket2::Domain::IPV4
            } else {
                socket2::Domain::IPV6
            };
            let socket =
                socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
                    .context("Failed to create TCP listener socket")?;
            // std::net::TcpListener::bind sets SO_REUSEADDR on Unix; keep that.
            socket
                .set_reuse_address(true)
                .context("Failed to set SO_REUSEADDR")?;
            super::listener::size_listener_buffers(&socket);
            socket
                .bind(&requested.into())
                .context("Failed to pre-bind TCP listener")?;
            // 128 matches std::net::TcpListener::bind's backlog.
            socket.listen(128).context("Failed to listen")?;
            let std_listener: std::net::TcpListener = socket.into();
            let actual = std_listener.local_addr()?;
            (actual, Some(std_listener))
        };

        // Resolve advertise endpoints (multi-interface discovery)
        let endpoints = resolve_advertise_endpoints(bind_addr, &self.interface_filter)?;

        // Warn if NUMA hint conflicts with interface filter
        if let (Some(numa), InterfaceFilter::ByName(name)) =
            (self.numa_hint, &self.interface_filter)
        {
            for ep in &endpoints {
                if let Some(ep_numa) = ep.numa_node
                    && ep_numa != numa as i32
                {
                    warn!(
                        "NIC {} is on NUMA node {} but GPU NUMA hint is {}",
                        name, ep_numa, numa
                    );
                }
            }
        }

        let encoded =
            rmp_serde::to_vec(&endpoints).context("Failed to encode interface endpoints")?;
        let mut addr_builder = crate::transports::address::WorkerAddressBuilder::new();
        addr_builder.add_entry(key.clone(), encoded)?;
        let local_address = addr_builder.build()?;

        let mut transport = TcpTransport::new(
            bind_addr,
            key,
            local_address,
            self.channel_capacity,
            self.connect_timeout,
            listener,
            self.numa_hint,
        );
        if let Some(t) = self.shrink_threshold {
            transport.shrink_threshold = t;
        }
        transport.lanes = self.lanes;
        Ok(transport)
    }
}

impl Default for TcpTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// `#[path]` keeps the tests beside their siblings as `tcp/tests.rs`; the
// default resolution would bury them in a one-file `tcp/transport/` directory.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
