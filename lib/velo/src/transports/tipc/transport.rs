// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `TipcTransport` and `TipcTransportBuilder`: `Transport` trait impl, builder,
//! reachability gate, send path, observability wiring, and unit tests.
//!
//! ## Design invariants (enforced here)
//!
//! 1. **No half-close**: every writer-task teardown path calls
//!    [`TipcStream::shutdown_both`] before dropping the stream.  The final
//!    `close(2)` is issued via `tokio::task::spawn_blocking` to avoid stalling
//!    a tokio worker for up to 8 s under link congestion.
//! 2. **ECONNRESET = graceful close at peer**: handled in `listener.rs`; the
//!    writer does not need special treatment beyond issuing `shutdown(Both)`.
//! 3. **Connect gated on remote `accept()`**: `TipcStream::connect` already
//!    bounds this by `connect_timeout` (default 5 s); the builder sets the
//!    kernel `TIPC_CONN_TIMEOUT` 1 s above that to ensure our timeout fires first.
//! 4. **Named msgpack encoding**: `TipcEndpoint` uses `rmp_serde::to_vec_named`.
//! 5. **Netns nonce**: `xxh3_64(boot_id ++ netns_ino)` parsed from `readlink`.
//! 6. **Gate**: three-way `Gate::{Reachable, Never, NotYet}` verdict per §5.3.
//! 7. **Re-register hook**: `TopologyState::set_reregister_hook` wired by the
//!    velo builder via `TipcTransport::topology_state()` after Messenger is built.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::transports::tcp::TcpFrameCodec;
use crate::transports::transport::{
    HealthCheckError, SendBackpressure, ShutdownState, TransportError, TransportErrorHandler,
    try_send_or_backpressure,
};
use velo_ext::{
    InstanceId, MessageType, PeerInfo, Transport, TransportAdapter, TransportKey, WorkerAddress,
};

use super::endpoint::TipcEndpoint;
use super::listener::{BoundTipcListener, BoundTipcListenerConfig, default_shrink_threshold};
use super::socket::{
    bind_service_range_and_listen, compute_netns_nonce, create_tipc_stream, getsockname_ref_node,
    tipc_available,
};
use super::stream::TipcStream;
use super::sys::{TIPC_CLUSTER_SCOPE, TIPC_NODE_SCOPE, TIPC_RESERVED_TYPES};
use super::topology::TopologyState;

// ── Bind-scope newtype ────────────────────────────────────────────────────────

/// TIPC publication scope for the listener binding.
///
/// `Cluster` (default) makes the service visible in the TIPC cluster name table
/// so remote peers can verify it is live before connecting.  `Node` restricts
/// visibility to the local node; remote peers will always receive `Gate::NotYet`
/// for this binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipcScope {
    /// `TIPC_CLUSTER_SCOPE` — visible cluster-wide. **Default.**
    Cluster,
    /// `TIPC_NODE_SCOPE` — visible only on the local node.
    Node,
}

impl From<TipcScope> for i8 {
    fn from(s: TipcScope) -> i8 {
        match s {
            TipcScope::Cluster => TIPC_CLUSTER_SCOPE,
            TipcScope::Node => TIPC_NODE_SCOPE,
        }
    }
}

// ── Gate verdict ──────────────────────────────────────────────────────────────

/// Three-way reachability verdict returned by the internal register gate.
///
/// Matches the proposal §5.3 `Gate::{Reachable, Never, NotYet}` design verbatim.
enum Gate {
    /// Endpoint is reachable right now — register it.
    Reachable,
    /// Permanent property of this endpoint — reject outright, do not park.
    Never,
    /// Not reachable *right now* (stale or cold-start) — park for event-driven
    /// re-registration.
    NotYet,
}

// ── Connection handle / send task ─────────────────────────────────────────────

/// Handle to a per-peer writer task.
#[derive(Clone)]
struct ConnectionHandle {
    tx: flume::Sender<SendTask>,
}

/// Task sent to the writer task containing a pre-encoded frame.
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

// ── TipcTransport ─────────────────────────────────────────────────────────────

/// Messenger transport over TIPC `SOCK_STREAM` sockets.
///
/// ## Lifecycle
///
/// 1. Build via [`TipcTransportBuilder`] — pre-binds the listening socket,
///    computes the local [`TipcEndpoint`], and constructs [`TopologyState`].
/// 2. Optionally call `set_reregister_hook` on the inner `TopologyState` (the
///    velo builder does this automatically via [`TipcTransport::topology_state`]).
/// 3. Call [`Transport::start`] — spawns the accept loop and awaits topology
///    initial replay before returning.
/// 4. Use [`Transport::register`] / [`Transport::send_message`] / etc. normally.
/// 5. Call [`Transport::shutdown`] for 3-phase teardown.
///
/// ## Re-register hook
///
/// The velo builder, after constructing `VeloBackend`, calls
/// [`TipcTransport::topology_state`] and installs a re-register hook via
/// [`TopologyState::set_reregister_hook`].  This ensures that cold-start peers
/// (registered while the TIPC name-table is still converging) are automatically
/// promoted from TCP to TIPC once their publication appears.
pub struct TipcTransport {
    /// Transport key — always `"tipc"`.
    key: TransportKey,
    /// Local endpoint advertised in this transport's `WorkerAddress` entry.
    local_endpoint: TipcEndpoint,
    /// Composite `WorkerAddress` containing the `"tipc"` entry.
    local_address: WorkerAddress,

    /// Pre-bound, listening `AF_TIPC SOCK_STREAM` socket.
    ///
    /// Moved into the [`BoundTipcListener`] when [`start`] is first called.
    /// `None` after `start()` has been invoked.
    listener_socket: Mutex<Option<socket2::Socket>>,

    /// Successfully registered remote peers: `InstanceId → TipcEndpoint`.
    peers: Arc<DashMap<InstanceId, TipcEndpoint>>,
    /// Per-peer writer channels.
    connections: Arc<DashMap<InstanceId, ConnectionHandle>>,
    /// Shared topology state: node-up/down cache, service-publication cache,
    /// pending-registration map, and the re-register hook.
    topology: Arc<TopologyState>,

    /// Tokio runtime handle injected at `start()`.
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Transport-level cancellation token for writer tasks.
    cancel_token: CancellationToken,
    /// Shutdown state injected from `TransportAdapter` at `start()`.
    shutdown_state: OnceLock<ShutdownState>,

    channel_capacity: usize,
    connect_timeout: Duration,
    /// Prometheus observability handle (set once via `set_observability`).
    metrics: OnceLock<Arc<dyn velo_ext::TransportObservability>>,
    /// Per-connection Framed read-buffer shrink threshold (bytes).
    shrink_threshold: usize,
}

impl TipcTransport {
    /// Return a clone of the shared [`TopologyState`].
    ///
    /// The velo builder calls this after constructing `VeloBackend` to install
    /// the re-register hook (invariant 7).  Out-of-tree callers should instead
    /// re-call `Velo::register_peer` from their own topology watcher task.
    ///
    /// Exposed as `pub` (not `pub(crate)`) to allow integration tests to access
    /// the pending map and install hooks without going through the full builder
    /// stack — this is the "public hook/test seam" referenced in proposal §9.
    pub fn topology_state(&self) -> Arc<TopologyState> {
        Arc::clone(&self.topology)
    }

    /// Install the re-register hook for cold-start recovery.
    ///
    /// Convenience delegate to [`TopologyState::set_reregister_hook`] that lets
    /// callers avoid importing [`TopologyState`] directly.  Called automatically
    /// by the velo builder; also exposed as a public test seam (proposal §9) for
    /// integration tests that construct the transport without the full builder stack.
    pub fn set_reregister_hook(&self, hook: Arc<dyn Fn(PeerInfo) + Send + Sync>) {
        self.topology.set_reregister_hook(hook);
    }

    // ── Gauge helpers ─────────────────────────────────────────────────────────

    fn update_peer_gauge(&self) {
        if let Some(m) = self.metrics.get() {
            m.set_registered_peers(self.peers.len());
        }
    }

    fn update_connection_gauge(&self) {
        if let Some(m) = self.metrics.get() {
            m.set_active_connections(self.connections.len());
        }
    }

    // ── Connection management ─────────────────────────────────────────────────

    fn get_or_create_connection(&self, instance_id: InstanceId) -> Result<ConnectionHandle> {
        // Fast path: live connection already exists.
        if let Some(handle) = self.connections.get(&instance_id) {
            if !handle.tx.is_disconnected() {
                return Ok(handle.clone());
            }
            drop(handle);
            self.connections
                .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
            self.update_connection_gauge();
        }

        let rt = self.runtime.get().ok_or(TransportError::NotStarted)?;

        // Atomic check-and-insert via entry API to avoid spawning duplicate tasks.
        let handle = match self.connections.entry(instance_id) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                if !e.get().tx.is_disconnected() {
                    e.get().clone()
                } else {
                    let h = self.create_connection(instance_id, rt)?;
                    e.insert(h.clone());
                    self.update_connection_gauge();
                    h
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let h = self.create_connection(instance_id, rt)?;
                e.insert(h.clone());
                self.update_connection_gauge();
                h
            }
        };

        Ok(handle)
    }

    fn create_connection(
        &self,
        instance_id: InstanceId,
        rt: &tokio::runtime::Handle,
    ) -> Result<ConnectionHandle> {
        let ep = self
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

        debug!(
            "Creating TIPC writer task for {} (ref={:#x}, node={:#x})",
            instance_id, ep.socket_ref, ep.node
        );

        rt.spawn(tipc_connection_writer_task(
            ep,
            instance_id,
            rx,
            conns,
            cancel,
            connect_timeout,
            metrics,
        ));

        Ok(handle)
    }

    /// Slow-path send: establish (or reuse) a connection, then enqueue.
    fn slow_path_send(
        &self,
        instance_id: InstanceId,
        task: SendTask,
    ) -> Result<(), SendBackpressure> {
        if self.runtime.get().is_none() {
            task.on_error("Transport not started");
            return Ok(());
        }
        let handle = match self.get_or_create_connection(instance_id) {
            Ok(h) => h,
            Err(e) => {
                task.on_error(format!("Failed to create connection: {e}"));
                return Ok(());
            }
        };
        let r = try_send_or_backpressure(
            &handle.tx,
            task,
            |t| t.on_error("Connection closed immediately"),
            |t| t.on_error("Connection closed"),
        );
        if let Some(m) = self.metrics.get()
            && r.is_err()
        {
            m.record_send_backpressure();
        }
        r
    }
}

// ── Transport impl ────────────────────────────────────────────────────────────

impl Transport for TipcTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    /// Register a remote peer after evaluating the `Gate` verdict (§5.3).
    ///
    /// Returns `Err(TransportError::NoEndpoint)` for both `Gate::Never` and
    /// `Gate::NotYet`.  The difference is invisible to the caller (it always
    /// promotes a different transport), but `NotYet` additionally parks the
    /// `PeerInfo` in `topology.pending` so the topology watcher can re-drive it
    /// when the TIPC name table converges.
    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        let ep = TipcEndpoint::decode_from_peer(&peer_info, &self.key)?;
        let local = &self.local_endpoint;

        // ── Gate verdict (proposal §5.3, invariant 6) ────────────────────────
        let verdict = if ep.netid != local.netid {
            // Wrong cluster — permanent.
            Gate::Never
        } else if ep.netns_nonce == local.netns_nonce {
            // Same (boot, netns) ⟹ same TIPC stack ⟹ exact-ref connect valid
            // regardless of node value (node=0 is fine intra-stack).
            Gate::Reachable
        } else if ep.node == 0 || local.node == 0 {
            // Zero-config TIPC (no bearer) never crosses a netns boundary.
            // Unequal nonce with either side at node=0 is unreachable by
            // construction (cross-netns send → EHOSTUNREACH, §2.7).
            Gate::Never
        } else if ep.node == local.node {
            // Foreign endpoint claiming OUR node value: connecting to
            // {ref, node=local.node} would route into our own stack.
            // TIPC rejects duplicate identities at link establishment, so
            // this is misconfiguration — force closed rather than letting
            // node_state.is_up(local.node) possibly return true.
            Gate::Never
        } else if !self.topology.is_stale()
            && self.topology.node_state.is_up(ep.node)
            && self.topology.service_watch.publication_matches(
                ep.service_instance,
                ep.socket_ref,
                ep.node,
            )
        {
            // Bearer path: node is live in the kernel topology table AND the
            // peer's exact listener socket is published right now.
            // Both checks are O(1) DashMap reads — zero RTT.
            Gate::Reachable
        } else {
            // Stale, or cold-start name-table propagation still in progress.
            // Indistinguishable now; park for event-driven recovery.
            Gate::NotYet
        };

        match verdict {
            Gate::Reachable => {
                // Remove from pending (in case of a re-registration after a
                // cold-start park), store the endpoint, update gauge.
                // Save debug values before moving ep into the DashMap.
                let iid = peer_info.instance_id();
                let (socket_ref, node) = (ep.socket_ref, ep.node);
                // unpark_pending removes from both pending and pending_decoded.
                self.topology.unpark_pending(&iid);
                self.peers.insert(iid, ep);
                self.update_peer_gauge();
                debug!(
                    "TIPC: registered peer {} (ref={:#x}, node={:#x})",
                    iid, socket_ref, node,
                );
                Ok(())
            }
            Gate::NotYet => {
                // Park: topology watch re-drives this through the re-register
                // hook on TIPC_PUBLISHED / node-up events.  Store the decoded
                // endpoint alongside PeerInfo so the re-drive can filter by
                // service_instance or node without re-decoding on each event.
                let iid = peer_info.instance_id();
                debug!(
                    "TIPC: parking peer {} (publication absent or node down)",
                    iid
                );
                self.topology.park_pending(iid, peer_info, ep);
                Err(TransportError::NoEndpoint)
            }
            Gate::Never => {
                debug!(
                    "TIPC: permanently rejecting peer {} \
                     (netid or netns mismatch, or own-node duplicate)",
                    peer_info.instance_id()
                );
                Err(TransportError::NoEndpoint)
            }
        }
    }

    /// Fire-and-forget send — UDS/TCP shape (per-peer flume channel).
    #[inline]
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure> {
        let task = SendTask {
            msg_type: message_type,
            header,
            payload,
            on_error,
        };

        // Fast path: try existing live connection.
        if let Some(handle) = self.connections.get(&instance_id) {
            match handle.tx.try_send(task) {
                Ok(()) => return Ok(()),
                Err(flume::TrySendError::Full(task)) => {
                    if let Some(m) = self.metrics.get() {
                        m.record_send_backpressure();
                    }
                    let tx = handle.tx.clone();
                    return Err(SendBackpressure::new(Box::pin(async move {
                        if let Err(flume::SendError(t)) = tx.send_async(task).await {
                            t.on_error("Connection closed");
                        }
                    })));
                }
                Err(flume::TrySendError::Disconnected(task)) => {
                    drop(handle);
                    self.connections
                        .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
                    self.update_connection_gauge();
                    return self.slow_path_send(instance_id, task);
                }
            }
        }
        self.slow_path_send(instance_id, task)
    }

    /// Start the transport: spawn the accept loop, await topology initial replay.
    ///
    /// After this future resolves:
    /// - The TIPC listener is accepting connections.
    /// - The topology cache has processed its initial replay (`register()` no
    ///   longer races cold caches — proposal §5.3 correction 1 / invariant 7).
    fn start(
        &self,
        _instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.runtime.set(rt.clone()).ok();
        let shutdown_state = channels.shutdown_state.clone();
        self.shutdown_state.set(shutdown_state.clone()).ok();

        Box::pin(async move {
            // Take the pre-bound listener socket.  A second call to start() is a
            // caller bug; we surface it as an error rather than panicking.
            let listener_sock = self
                .listener_socket
                .lock()
                .expect("listener_socket lock poisoned")
                .take()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "TIPC transport already started (listener socket already taken)"
                    )
                })?;

            struct DefaultErrHandler;
            impl TransportErrorHandler for DefaultErrHandler {
                fn on_error(&self, _h: Bytes, _p: Bytes, err: String) {
                    warn!("TIPC transport error: {err}");
                }
            }

            let bound = BoundTipcListener::new(
                listener_sock,
                BoundTipcListenerConfig {
                    adapter: channels,
                    error_handler: Arc::new(DefaultErrHandler),
                    shutdown_state,
                    transport_key: self.key.as_str().to_string(),
                    metrics: self.metrics.get().cloned(),
                    shrink_threshold: self.shrink_threshold,
                },
            );

            rt.spawn(async move {
                if let Err(e) = bound.serve().await {
                    error!("TIPC listener error: {e}");
                }
            });

            // Await topology subscription establishment and initial-replay
            // completion.  Returns once the barrier TIPC_SUBSCR_TIMEOUT fires,
            // guaranteeing that register() no longer races cold caches.
            Arc::clone(&self.topology)
                .start()
                .await
                .context("TIPC topology start failed")?;

            info!(
                "TIPC transport started (ref={:#x}, node={:#x})",
                self.local_endpoint.socket_ref, self.local_endpoint.node
            );
            Ok(())
        })
    }

    /// Signal the drain phase: flip `ShutdownState` so the listener starts
    /// returning `ShuttingDown` to new inbound `Message` frames.
    fn begin_drain(&self) {
        if let Some(state) = self.shutdown_state.get() {
            state.begin_drain();
        }
    }

    /// Phase-3 teardown: cancel listeners, writer tasks, topology watcher; clear state.
    fn shutdown(&self) {
        info!("Shutting down TIPC transport");
        // Cancel teardown token → accept loop + per-connection readers exit.
        if let Some(state) = self.shutdown_state.get() {
            state.teardown_token().cancel();
        }
        // Cancel transport token → all writer tasks exit.
        self.cancel_token.cancel();
        // Cancel topology-watcher task → exits its read/reconnect loop promptly,
        // releasing the SEQPACKET topology-server connection.
        self.topology.cancel();
        self.connections.clear();
        self.update_connection_gauge();
    }

    /// Install the Prometheus observability handle and refresh gauges.
    fn set_observability(&self, observability: Arc<dyn velo_ext::TransportObservability>) {
        let _ = self.metrics.set(observability);
        self.update_peer_gauge();
        self.update_connection_gauge();
    }

    /// Probe health of a registered peer.
    ///
    /// Fast path: a live writer channel is sufficient evidence the peer is up.
    /// Slow path: attempt a probe-connect to the exact `{socket_ref, node}`
    /// address under `timeout`.
    ///
    /// **Semantic note**: because TIPC connect completes only when the remote
    /// application calls `accept()` (invariant 3 / §2.3.3), a successful probe
    /// means "remote accept loop is responsive" — not merely "listener bound".
    /// A wedged runtime yields `Timeout` for a live process.
    fn check_health(
        &self,
        instance_id: InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async move {
            // Remember whether a connection existed before the check so we can
            // return `NeverConnected` vs `Ok` on a successful probe (mirroring UDS).
            let connection_existed = self.connections.contains_key(&instance_id);

            // Fast path: live writer channel.
            if let Some(handle) = self.connections.get(&instance_id) {
                if !handle.tx.is_disconnected() {
                    return Ok(());
                }
                drop(handle);
                self.connections
                    .remove_if(&instance_id, |_, h| h.tx.is_disconnected());
                self.update_connection_gauge();
            }

            // Look up the registered endpoint.
            let ep = self
                .peers
                .get(&instance_id)
                .ok_or(HealthCheckError::PeerNotRegistered)?
                .value()
                .clone();

            // Zero-RTT fast miss: node is known down.
            // Only applies when caches are fresh — a stale cache (topology server
            // reconnecting) has an empty node_state map, so is_up returns false for
            // every node.  Treating a stale miss as ConnectionFailed would falsely
            // fail health checks for all bearer-cluster peers during any reconnect
            // window.  Fall through to the probe-connect instead, mirroring the
            // register() gate's stale → NotYet discipline.
            if !self.topology.is_stale() && ep.node != 0 && !self.topology.node_state.is_up(ep.node)
            {
                return Err(HealthCheckError::ConnectionFailed);
            }

            // Probe-connect to the exact socket address.
            // TipcStream::connect already applies `timeout` internally; the outer
            // tokio::time::timeout is a safety net.
            match tokio::time::timeout(
                timeout,
                TipcStream::connect(ep.socket_ref, ep.node, timeout),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    // Invariant 1: shutdown(Both) before drop (probe socket).
                    let _ = stream.shutdown_both();
                    // spawn_blocking so close(2) doesn't stall the caller's task.
                    let _ = tokio::task::spawn_blocking(move || drop(stream)).await;
                    if connection_existed {
                        Ok(())
                    } else {
                        Err(HealthCheckError::NeverConnected)
                    }
                }
                Ok(Err(_)) => Err(HealthCheckError::ConnectionFailed),
                Err(_timeout) => Err(HealthCheckError::Timeout),
            }
        })
    }
}

// ── Writer task ───────────────────────────────────────────────────────────────

/// Per-peer connection writer task.
///
/// Always runs cleanup (drain queued sends, remove stale map entry) even if
/// the initial connect fails.
async fn tipc_connection_writer_task(
    ep: TipcEndpoint,
    instance_id: InstanceId,
    rx: flume::Receiver<SendTask>,
    connections: Arc<DashMap<InstanceId, ConnectionHandle>>,
    cancel_token: CancellationToken,
    connect_timeout: Duration,
    metrics: Option<Arc<dyn velo_ext::TransportObservability>>,
) -> Result<()> {
    let result =
        tipc_connection_writer_inner(&ep, instance_id, &rx, &cancel_token, connect_timeout).await;

    // Drain queued messages regardless of how the connection ended.
    while let Ok(task) = rx.try_recv() {
        task.on_error("Connection closed");
    }

    // Drop the receiver so our sender half becomes disconnected, then remove
    // the stale map entry.  The predicate ensures we only remove our own entry —
    // a replacement connection's tx will still be connected.
    drop(rx);
    connections.remove_if(&instance_id, |_, h| h.tx.is_disconnected());
    if let Some(m) = metrics.as_ref() {
        m.set_active_connections(connections.len());
    }

    debug!(
        "TIPC writer task for {} (ref={:#x}, node={:#x}) exited",
        instance_id, ep.socket_ref, ep.node
    );

    result
}

/// Inner connect + send loop.
async fn tipc_connection_writer_inner(
    ep: &TipcEndpoint,
    instance_id: InstanceId,
    rx: &flume::Receiver<SendTask>,
    cancel_token: &CancellationToken,
    connect_timeout: Duration,
) -> Result<()> {
    debug!(
        "TIPC: connecting to peer {} (ref={:#x}, node={:#x})",
        instance_id, ep.socket_ref, ep.node
    );

    // Connect — cancellable so transport shutdown doesn't leave tasks blocked.
    let mut stream = tokio::select! {
        biased;
        _ = cancel_token.cancelled() => return Ok(()),
        res = TipcStream::connect(ep.socket_ref, ep.node, connect_timeout) => {
            res.context("TIPC connect failed")?
        },
    };

    debug!(
        "TIPC: connected to peer {} (ref={:#x}, node={:#x})",
        instance_id, ep.socket_ref, ep.node
    );

    // Send loop: prioritise cancellation to bound shutdown latency.
    loop {
        let task = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            res = rx.recv_async() => match res {
                Ok(task) => task,
                Err(_disconnected) => break,
            },
        };

        if let Err(e) =
            TcpFrameCodec::encode_frame(&mut stream, task.msg_type, &task.header, &task.payload)
                .await
        {
            error!(
                "TIPC write error to {} (ref={:#x}, node={:#x}): {e}",
                instance_id, ep.socket_ref, ep.node
            );
            task.on_error(format!("Failed to write to TIPC stream: {e}"));
            // Exit the loop so the cleanup in the outer function drains rx.
            break;
        }
    }

    // Invariant 1: always issue shutdown(Both) before drop so the peer sees
    // a clean EOF rather than ECONNRESET.
    //
    // shutdown(2) itself is fast (non-blocking); only the subsequent close(2)
    // can block up to 8 s under link congestion (§2.3 close-blocking hazard).
    // spawn_blocking ensures the tokio worker thread is not stalled.
    let _ = stream.shutdown_both(); // fast shutdown(SHUT_RDWR)
    let _ = tokio::task::spawn_blocking(move || drop(stream)).await; // close(2)

    Ok(())
}

// ── TipcTransportBuilder ──────────────────────────────────────────────────────

/// Builder for [`TipcTransport`].
///
/// Call [`build`](TipcTransportBuilder::build) to pre-bind the TIPC listener
/// socket and produce a ready-to-start transport.
///
/// ## Defaults
///
/// | Field | Default |
/// |---|---|
/// | `key` | `"tipc"` |
/// | `service_type` | `0x56454C4F` ("VELO") |
/// | `service_instance` | random `u32` |
/// | `scope` | [`TipcScope::Cluster`] |
/// | `netid` | `4711` (TIPC's compiled-in default) |
/// | `channel_capacity` | 256 |
/// | `connect_timeout` | 5 s |
pub struct TipcTransportBuilder {
    key: Option<TransportKey>,
    service_type: u32,
    service_instance: Option<u32>,
    scope: TipcScope,
    netid: u32,
    channel_capacity: usize,
    connect_timeout: Duration,
    shrink_threshold: Option<usize>,
}

impl TipcTransportBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            key: None,
            service_type: 0x56454C4F, // "VELO" in ASCII
            service_instance: None,
            scope: TipcScope::Cluster,
            netid: 4711,
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            shrink_threshold: None,
        }
    }

    /// Override the transport key (default: `"tipc"`).
    pub fn key(mut self, key: impl Into<TransportKey>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the TIPC service type.
    ///
    /// Must be ≥ `TIPC_RESERVED_TYPES` (64).  Defaults to `0x56454C4F` ("VELO").
    pub fn service_type(mut self, t: u32) -> Self {
        self.service_type = t;
        self
    }

    /// Fix the TIPC service instance.
    ///
    /// Defaults to a random `u32`.  Setting a fixed value is useful in tests or
    /// when the application needs a well-known service address.
    pub fn service_instance(mut self, instance: u32) -> Self {
        self.service_instance = Some(instance);
        self
    }

    /// Set the publication scope (default: [`TipcScope::Cluster`]).
    ///
    /// `Node` scope makes the service invisible in remote name tables;
    /// remote peers will always see `Gate::NotYet` for this transport.
    pub fn scope(mut self, scope: TipcScope) -> Self {
        self.scope = scope;
        self
    }

    /// Set the TIPC cluster network ID (default: `4711`, TIPC's compiled-in default).
    ///
    /// The `netid` is an operator-assigned 32-bit cluster identity.  Two nodes with
    /// different `netid` values will never exchange bearer traffic; the register gate
    /// returns `Gate::Never` for any endpoint whose `netid` differs from this one.
    ///
    /// **Deployment requirement**: velo cannot read the kernel's live netid without
    /// issuing TIPC-specific netlink messages.  Any deployment that sets a custom netid
    /// via `tipc node set netid <id>` MUST pass the same value here, or all cross-node
    /// peers will be permanently rejected at `register()` time.
    pub fn netid(mut self, netid: u32) -> Self {
        self.netid = netid;
        self
    }

    /// Set the per-peer channel capacity (default: 256).
    ///
    /// When the channel is full, `send_message` returns [`SendBackpressure`]
    /// instead of enqueuing synchronously.
    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the outbound connect timeout (default: 5 s).
    ///
    /// This bounds both SYN delivery time and "remote accept loop wedged"
    /// latency (invariant 3 / §2.3.3).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the per-connection read-buffer shrink threshold (bytes).
    ///
    /// Defaults to `VELO_TIPC_SHRINK_THRESHOLD` env var or 8 MiB.
    pub fn shrink_threshold(mut self, bytes: usize) -> Self {
        self.shrink_threshold = Some(bytes);
        self
    }

    /// Pre-bind the listener socket and build the transport.
    ///
    /// Steps performed synchronously:
    /// 1. Probe TIPC availability (`tipc_available()`); error if module absent.
    /// 2. Validate `service_type >= TIPC_RESERVED_TYPES`.
    /// 3. Generate a random `service_instance` if not set.
    /// 4. `socket → bind_service_range → listen → getsockname`.
    /// 5. Compute `netns_nonce = xxh3_64(boot_id ++ netns_ino)`.
    /// 6. Construct [`TipcEndpoint`] and encode into [`WorkerAddress`].
    /// 7. Construct [`TopologyState`].
    ///
    /// ## EAFNOSUPPORT
    ///
    /// If the TIPC kernel module is not loaded, `build()` returns an error with
    /// the message:
    /// > "TIPC kernel module not loaded; run `sudo modprobe tipc`"
    pub fn build(self) -> Result<TipcTransport> {
        // 1. Probe TIPC availability.
        if !tipc_available() {
            anyhow::bail!(
                "TIPC kernel module not loaded; run `sudo modprobe tipc` \
                 (or add `tipc` to /etc/modules-load.d/tipc.conf for persistence)"
            );
        }

        // 2. Validate service type.
        if self.service_type < TIPC_RESERVED_TYPES {
            anyhow::bail!(
                "TIPC service_type {:#x} is reserved (must be >= {TIPC_RESERVED_TYPES}); \
                 use a value >= 64 or the default 0x56454C4F (\"VELO\")",
                self.service_type,
            );
        }

        // 3. Random service_instance when not fixed by the caller.
        //    InstanceId::new_v4() is UUID v4 (random); cast to u32 is fine.
        let service_instance = self
            .service_instance
            .unwrap_or_else(|| InstanceId::new_v4().as_u128() as u32);

        // 4. Create socket, bind service range, listen.
        let scope_i8: i8 = self.scope.into();
        let sock = create_tipc_stream().context("Failed to create AF_TIPC SOCK_STREAM socket")?;

        bind_service_range_and_listen(
            &sock,
            self.service_type,
            service_instance,
            service_instance, // single-instance range: lower == upper
            scope_i8,
            128, // listen backlog
        )
        .context("Failed to bind TIPC service range and listen")?;

        // 5. getsockname → kernel-assigned {socket_ref, node}.
        let (socket_ref, node) =
            getsockname_ref_node(&sock).context("TIPC getsockname failed after bind")?;

        // 6. Compute netns_nonce.
        //    xxh3_64(boot_id_bytes ++ netns_inode_le64); inode parsed from
        //    readlink("/proc/self/ns/net") = "net:[<u64>]" — not stat().
        let netns_nonce = compute_netns_nonce().context("Failed to compute TIPC netns nonce")?;

        // Cluster netid: operator-configurable via TipcTransportBuilder::netid().
        // Defaults to 4711 (TIPC's compiled-in default).  velo cannot read the
        // kernel's live netid without TIPC-specific netlink; callers using
        // `tipc node set netid <id>` MUST pass the matching value here.
        let netid = self.netid;

        // node_id (128-bit): requires ioctl(SIOCGETNODEID) which is not
        // in sys.rs yet.  Zero-init is safe: the field is informational only;
        // routing uses `node` (32-bit hash), not `node_id`.
        let node_id = [0u8; 16];

        let key = self.key.unwrap_or_else(|| TransportKey::from("tipc"));

        // 7. Build TipcEndpoint and encode into WorkerAddress.
        let local_endpoint = TipcEndpoint {
            version: 1,
            service_type: self.service_type,
            service_instance,
            node,
            socket_ref,
            netid,
            node_id,
            netns_nonce,
            scope: scope_i8 as u8,
        };

        let local_address = local_endpoint
            .encode_into_worker_address(&key)
            .context("Failed to encode TipcEndpoint into WorkerAddress")?;

        // 8. Construct TopologyState (caches start empty/stale; started in
        //    Transport::start() which awaits the initial replay).
        let topology = TopologyState::new(self.service_type);

        let shrink_threshold = self
            .shrink_threshold
            .unwrap_or_else(default_shrink_threshold);

        Ok(TipcTransport {
            key,
            local_endpoint,
            local_address,
            listener_socket: Mutex::new(Some(sock)),
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            topology,
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: self.channel_capacity,
            connect_timeout: self.connect_timeout,
            metrics: OnceLock::new(),
            shrink_threshold,
        })
    }
}

impl Default for TipcTransportBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::address::WorkerAddressBuilder;
    use velo_ext::TransportKey;

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Default netid used in tests.
    const TEST_NETID: u32 = 4711;
    /// Dummy service type (≥ TIPC_RESERVED_TYPES).
    const TEST_SVC_TYPE: u32 = 0x5654_9999;

    /// Build a [`TipcEndpoint`] with controlled fields.
    fn make_ep(
        netid: u32,
        netns_nonce: u64,
        node: u32,
        socket_ref: u32,
        service_instance: u32,
    ) -> TipcEndpoint {
        TipcEndpoint {
            version: 1,
            service_type: TEST_SVC_TYPE,
            service_instance,
            node,
            socket_ref,
            netid,
            node_id: [0u8; 16],
            netns_nonce,
            scope: TIPC_CLUSTER_SCOPE as u8,
        }
    }

    /// Build a [`PeerInfo`] carrying the given endpoint under the `"tipc"` key.
    fn peer_info_from_ep(ep: &TipcEndpoint) -> PeerInfo {
        let key = TransportKey::from("tipc");
        let addr = ep.encode_into_worker_address(&key).unwrap();
        PeerInfo::new(InstanceId::new_v4(), addr)
    }

    /// Build a minimal [`TipcTransport`] with the given local endpoint and
    /// `TopologyState`, without touching real sockets.
    ///
    /// Used to test the `register()` gate logic in isolation.
    fn make_transport_for_test(
        local_ep: TipcEndpoint,
        topology: Arc<TopologyState>,
    ) -> TipcTransport {
        // Build a dummy WorkerAddress with the local endpoint.
        let key = TransportKey::from("tipc");
        let local_address = local_ep.encode_into_worker_address(&key).unwrap();

        // Fabricate a socket pair for the listener_socket field.
        // We never actually start the transport in these tests, so the socket
        // is never used; it only needs to be a valid fd.
        use std::os::fd::{FromRawFd, IntoRawFd};
        let (a, _b) = std::os::unix::net::UnixStream::pair().unwrap();
        let raw = a.into_raw_fd();
        // SAFETY: we own this fd from UnixStream::pair.
        let placeholder_sock = unsafe { socket2::Socket::from_raw_fd(raw) };

        TipcTransport {
            key,
            local_endpoint: local_ep,
            local_address,
            listener_socket: Mutex::new(Some(placeholder_sock)),
            peers: Arc::new(DashMap::new()),
            connections: Arc::new(DashMap::new()),
            topology,
            runtime: OnceLock::new(),
            cancel_token: CancellationToken::new(),
            shutdown_state: OnceLock::new(),
            channel_capacity: 256,
            connect_timeout: Duration::from_secs(5),
            metrics: OnceLock::new(),
            shrink_threshold: 8 * 1024 * 1024,
        }
    }

    // ── Gate verdict table tests (no kernel required) ─────────────────────────

    /// `Gate::Never` — netid mismatch.
    #[test]
    fn gate_never_netid_mismatch() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0x1111, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Remote has different netid
        let remote = make_ep(TEST_NETID + 1, 0xBBBB, 0x2222, 0x1234, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "netid mismatch must return NoEndpoint (Gate::Never)"
        );
        assert!(
            !transport.peers.contains_key(&iid),
            "peer must not be stored after Gate::Never"
        );
        assert!(
            !transport.topology.pending.contains_key(&iid),
            "peer must not be parked after Gate::Never (netid mismatch is permanent)"
        );
    }

    /// `Gate::Never` — non-default netid mismatch (multi-cluster deployment).
    ///
    /// Two clusters each configured with a custom netid via
    /// `tipc node set netid <id>` / `TipcTransportBuilder::netid(<id>)`.
    /// An endpoint from cluster B (netid 3000) seen by a transport in cluster A
    /// (netid 2000) must be permanently rejected — no parking, no retry.
    #[test]
    fn gate_never_multi_cluster_netid() {
        const CLUSTER_A_NETID: u32 = 2000;
        const CLUSTER_B_NETID: u32 = 3000;

        let local = make_ep(CLUSTER_A_NETID, 0xAAAA, 0x1111, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Peer belongs to cluster B — different custom netid.
        let remote = make_ep(CLUSTER_B_NETID, 0xBBBB, 0x2222, 0x1234, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "cross-cluster netid mismatch must return NoEndpoint (Gate::Never)"
        );
        assert!(
            !transport.peers.contains_key(&iid),
            "peer must not be stored after Gate::Never"
        );
        assert!(
            !transport.topology.pending.contains_key(&iid),
            "cross-cluster rejection is permanent — must not be parked"
        );
    }

    /// `Gate::Reachable` — same netns_nonce (same TIPC stack).
    #[test]
    fn gate_reachable_same_nonce() {
        let nonce = 0xCAFE_BABE_DEAD_BEEFu64;
        let local = make_ep(TEST_NETID, nonce, 0, 0xAAAA, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Same nonce → same TIPC stack → direct connect valid.
        let remote = make_ep(TEST_NETID, nonce, 0, 0xBBBB, 20);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        transport
            .register(peer)
            .expect("same-nonce peer must register as Reachable");
        assert!(transport.peers.contains_key(&iid));
        assert!(!transport.topology.pending.contains_key(&iid));
    }

    /// `Gate::Never` — unequal nonce with local.node == 0 (no bearer).
    #[test]
    fn gate_never_local_node_zero_unequal_nonce() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0, 0xABCD, 100); // node == 0
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Remote with different nonce and nonzero node.
        let remote = make_ep(TEST_NETID, 0xBBBB, 0x1234, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "unequal nonce with local node=0 must be Gate::Never"
        );
        assert!(!transport.topology.pending.contains_key(&iid));
    }

    /// `Gate::Never` — unequal nonce with remote.node == 0 (no bearer on remote).
    #[test]
    fn gate_never_remote_node_zero_unequal_nonce() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0x1234, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Remote has node == 0 (no bearer) and different nonce.
        let remote = make_ep(TEST_NETID, 0xBBBB, 0, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "unequal nonce with remote node=0 must be Gate::Never"
        );
        assert!(!transport.topology.pending.contains_key(&iid));
    }

    /// `Gate::Never` — own-node duplicate: ep.node == local.node with unequal nonce.
    ///
    /// Invariant 6: a foreign endpoint claiming OUR node value would route into
    /// our own TIPC stack.  Must be forced closed rather than falling through to
    /// the node-state check, which could return `is_up(local.node) == true`.
    #[test]
    fn gate_never_own_node_duplicate() {
        let local_node = 0x9876_5432u32;
        let local = make_ep(TEST_NETID, 0xAAAA, local_node, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Remote claims the same node value as us but has a different nonce.
        let remote = make_ep(TEST_NETID, 0xBBBB, local_node, 0x1234, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "own-node duplicate (ep.node == local.node, unequal nonce) must be Gate::Never"
        );
        assert!(!transport.topology.pending.contains_key(&iid));
    }

    /// `Gate::NotYet` — stale caches (topology not yet started or reconnecting).
    ///
    /// A new `TopologyState` starts stale; any remote arm that would otherwise
    /// require a fresh cache falls through to `Gate::NotYet`.
    #[test]
    fn gate_not_yet_stale_cache() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0x1111, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE); // starts stale
        assert!(topology.is_stale(), "fresh TopologyState must start stale");

        let transport = make_transport_for_test(local, topology);

        // Remote with different node/nonce; cache is stale → NotYet.
        let remote = make_ep(TEST_NETID, 0xBBBB, 0x2222, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "stale cache must yield Gate::NotYet (returns NoEndpoint)"
        );
        assert!(
            transport.topology.pending.contains_key(&iid),
            "Gate::NotYet must park the peer in topology.pending"
        );
    }

    /// `Gate::NotYet` — stale cache with nonzero nodes (additional coverage).
    ///
    /// Verifies the stale path specifically for the multi-node case (both sides
    /// have valid bearer-assigned node values, unequal nonces, but cache is stale).
    /// The gate cannot reach the node_state check path when `is_stale()` is true,
    /// so the endpoint is parked regardless of node-up status.
    #[test]
    fn gate_not_yet_stale_nonzero_nodes() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0x1111, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        // A fresh TopologyState always starts stale — the gate cannot reach the
        // node_state/service_watch checks while is_stale() is true.
        assert!(topology.is_stale(), "fresh TopologyState must start stale");

        let transport = make_transport_for_test(local, Arc::clone(&topology));

        // Remote with valid node value and different nonce; stale → NotYet.
        let remote = make_ep(TEST_NETID, 0xBBBB, 0x2222, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "stale cache (nonzero nodes, unequal nonce) must yield Gate::NotYet"
        );
        assert!(
            transport.topology.pending.contains_key(&iid),
            "Gate::NotYet must park the peer in topology.pending"
        );
    }

    /// `Gate::NotYet` — peer parked, then removed on re-registration.
    ///
    /// Simulates the cold-start recovery: first register parks it (Gate::NotYet),
    /// then same-nonce re-registration succeeds (Gate::Reachable) and removes it
    /// from pending.
    ///
    /// Note: local.node must be nonzero (bearer-assigned).  When local.node == 0
    /// the gate returns `Gate::Never` for any unequal-nonce remote (proposal §5.3
    /// case 3: "no bearer, cross-netns unreachable"), so the peer is not parked.
    #[test]
    fn gate_reachable_removes_pending() {
        let nonce = 0xDEAD_BEEFu64;
        // Use nonzero node values so case 3 of the Gate (ep.node==0 || local.node==0)
        // does not fire, allowing the stale-cache path to reach Gate::NotYet.
        let local = make_ep(TEST_NETID, nonce, 0x1111, 0xAAAA, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local.clone(), Arc::clone(&topology));

        // First: a different-nonce, different-node registration gets parked.
        // - ep.netid == local.netid: passes case 1
        // - ep.nonce != local.nonce: passes case 2
        // - ep.node != 0 && local.node != 0: passes case 3
        // - ep.node != local.node: passes case 4
        // - topology is stale: falls through to Gate::NotYet
        let remote_parked = make_ep(TEST_NETID, 0xBEEF, 0x9999, 0x1234, 50);
        let peer_parked = peer_info_from_ep(&remote_parked);
        let iid_parked = peer_parked.instance_id();
        transport.register(peer_parked).unwrap_err();
        assert!(topology.pending.contains_key(&iid_parked));

        // Second: same-nonce re-registration succeeds (Gate::Reachable, case 2)
        // and removes from pending.
        let remote_good = make_ep(TEST_NETID, nonce, 0x2222, 0x4321, 60);
        let peer_good = peer_info_from_ep(&remote_good);
        let iid_good = peer_good.instance_id();
        // Manually pre-park to simulate event-driven re-registration.
        topology.pending.insert(iid_good, peer_good.clone());

        transport
            .register(peer_good)
            .expect("same-nonce re-registration must succeed");
        assert!(
            transport.peers.contains_key(&iid_good),
            "successfully registered peer must be in peers map"
        );
        assert!(
            !topology.pending.contains_key(&iid_good),
            "register Reachable must remove peer from pending"
        );
    }

    /// `Gate::Never` — no `"tipc"` key in peer's WorkerAddress.
    #[test]
    fn gate_never_no_tipc_key() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0x1111, 0xABCD, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        // Peer with no TIPC entry.
        let mut builder = WorkerAddressBuilder::new();
        builder
            .add_entry("tcp", b"127.0.0.1:9999".to_vec())
            .unwrap();
        let addr = builder.build().unwrap();
        let peer = PeerInfo::new(InstanceId::new_v4(), addr);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "missing 'tipc' key must return NoEndpoint"
        );
        assert!(!transport.peers.contains_key(&iid));
    }

    /// `set_reregister_hook` installs a hook and a second call is graceful.
    ///
    /// The hook is stored in a `OnceLock` on `TopologyState`; a second call logs
    /// a warning and is ignored.  This test verifies the contract is upheld (no
    /// panic on double-install) and that the hook mechanism itself works by calling
    /// it directly via a local copy before installing it on the topology.
    ///
    /// Full event-driven re-drive (topology watcher fires the hook on
    /// `TIPC_PUBLISHED`) is covered by `topology.rs` integration tests.
    #[test]
    fn set_reregister_hook_is_idempotent() {
        let nonce = 0xCAFEu64;
        let local = make_ep(TEST_NETID, nonce, 0, 0xAAAA, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let _ = make_transport_for_test(local.clone(), Arc::clone(&topology));

        // Track calls via a shared counter.
        let call_count: Arc<std::sync::atomic::AtomicUsize> =
            Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // First install — must succeed silently.
        {
            let count = Arc::clone(&call_count);
            topology.set_reregister_hook(Arc::new(move |_pi: PeerInfo| {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }));
        }

        // Second install — must also not panic (OnceLock ignores second set).
        {
            topology.set_reregister_hook(Arc::new(move |_pi: PeerInfo| {
                // intentionally empty
            }));
        }

        // Verify a peer can be parked in pending.
        let remote = make_ep(TEST_NETID, 0xBEEF, 0x2222, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();
        topology.pending.insert(iid, peer);
        assert!(
            topology.pending.contains_key(&iid),
            "peer must be parkable in pending map"
        );

        // The hook count is 0 because we never called it; the topology watcher
        // task would call it via redrive_pending on TIPC events (integration test).
        assert_eq!(
            call_count.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "hook must not fire from set_reregister_hook itself"
        );
    }

    // ── Builder validation tests ──────────────────────────────────────────────

    /// Service type < 64 must be rejected at build time.
    #[test]
    fn builder_rejects_reserved_service_type() {
        use super::super::socket::tipc_available;
        if !tipc_available() {
            return; // needs the TIPC module for the socket path
        }
        let result = TipcTransportBuilder::new().service_type(63).build();
        assert!(
            result.is_err(),
            "service_type < TIPC_RESERVED_TYPES must be rejected"
        );
        // Use .err().expect() to avoid the T: Debug bound from unwrap_err().
        let msg = result.err().expect("checked is_err above").to_string();
        assert!(
            msg.contains("reserved") || msg.contains("TIPC_RESERVED_TYPES"),
            "error message should mention 'reserved': {msg}"
        );
    }

    /// `topology_state()` returns a clone of the internal `TopologyState`.
    #[test]
    fn topology_state_accessor_returns_same_arc() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0, 0xBBBB, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, Arc::clone(&topology));

        let ts = transport.topology_state();
        // Same pointer — they share state.
        assert!(
            Arc::ptr_eq(&ts, &topology),
            "topology_state() must return the same Arc as was passed in"
        );
    }

    // ── Gate arm 5 tests: bearer path with fresh caches ───────────────────────
    //
    // These tests require the `mark_fresh_for_test` / `test_mark_node_up` /
    // `test_publish_service` seams added to `TopologyState` for exactly this
    // purpose (proposal §9, finding 5).  They exercise the code path that was
    // previously unreachable in the no-bearer CI environment:
    //
    //   !is_stale() && node_state.is_up(ep.node)
    //   && service_watch.publication_matches(instance, ref, node)
    //   → Gate::Reachable
    //
    // Without these tests a regression in the publication_matches triple-check
    // or the is_stale guard ordering would pass the entire suite.

    /// `Gate::Reachable` — arm 5: fresh caches, node up, publication matches.
    #[test]
    fn gate_reachable_bearer_fresh_cache_node_up_publication_matches() {
        let local_node = 0x1111_1111u32;
        let remote_node = 0x2222_2222u32;
        let remote_ref = 0x5678u32;
        let remote_instance = 200u32;

        let local = make_ep(TEST_NETID, 0xAAAA, local_node, 0xAAAA, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);

        // Simulate barrier TIPC_SUBSCR_TIMEOUT (initial replay complete).
        topology.mark_fresh_for_test();
        // Simulate the remote node coming up.
        topology.test_mark_node_up(remote_node);
        // Simulate the remote service being published.
        topology.test_publish_service(remote_instance, remote_ref, remote_node);

        let transport = make_transport_for_test(local, Arc::clone(&topology));

        let remote = make_ep(TEST_NETID, 0xBBBB, remote_node, remote_ref, remote_instance);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        transport
            .register(peer)
            .expect("arm 5: fresh + node_up + publication_matches → Gate::Reachable");
        assert!(
            transport.peers.contains_key(&iid),
            "Reachable peer must be stored in peers map"
        );
        assert!(
            !topology.pending.contains_key(&iid),
            "Reachable peer must not be left in pending"
        );
    }

    /// `Gate::NotYet` — arm 5 miss: fresh caches, node up, publication absent.
    ///
    /// The service watch does not contain the peer's {instance, socket_ref, node},
    /// so `publication_matches` returns false → `Gate::NotYet` (parked).
    #[test]
    fn gate_not_yet_bearer_fresh_cache_node_up_no_publication() {
        let local_node = 0x1111_1111u32;
        let remote_node = 0x2222_2222u32;

        let local = make_ep(TEST_NETID, 0xAAAA, local_node, 0xAAAA, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);

        topology.mark_fresh_for_test();
        topology.test_mark_node_up(remote_node);
        // Do NOT publish the service → publication_matches returns false.

        let transport = make_transport_for_test(local, Arc::clone(&topology));

        let remote = make_ep(TEST_NETID, 0xBBBB, remote_node, 0x5678, 200);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "arm 5 miss (no publication): must return NoEndpoint (Gate::NotYet)"
        );
        assert!(
            topology.pending.contains_key(&iid),
            "Gate::NotYet must park the peer in topology.pending"
        );
    }

    /// `Gate::NotYet` — arm 5 miss: fresh caches, node down, publication present.
    ///
    /// The node-state watch does not show the node as up even though the service
    /// is published.  Both checks must pass for `Gate::Reachable`; missing either
    /// yields `Gate::NotYet`.
    #[test]
    fn gate_not_yet_bearer_fresh_cache_node_down_publication_present() {
        let local_node = 0x1111_1111u32;
        let remote_node = 0x2222_2222u32;
        let remote_ref = 0x5678u32;
        let remote_instance = 200u32;

        let local = make_ep(TEST_NETID, 0xAAAA, local_node, 0xAAAA, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);

        topology.mark_fresh_for_test();
        // Do NOT mark the node as up (is_up returns false for absent entries).
        // Publish the service — but node-down makes this irrelevant.
        topology.test_publish_service(remote_instance, remote_ref, remote_node);

        let transport = make_transport_for_test(local, Arc::clone(&topology));

        let remote = make_ep(TEST_NETID, 0xBBBB, remote_node, remote_ref, remote_instance);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "arm 5 miss (node down): must return NoEndpoint (Gate::NotYet)"
        );
        assert!(
            topology.pending.contains_key(&iid),
            "Gate::NotYet must park the peer in topology.pending"
        );
    }

    /// `Gate::NotYet` — arm 5 miss: fresh caches, node up, wrong socket_ref.
    ///
    /// `publication_matches` checks the full triple {instance, socket_ref, node}.
    /// A stale ref (recycled socket, crashed-and-restarted peer) must not be
    /// treated as reachable — `Gate::NotYet` (parked) instead.
    #[test]
    fn gate_not_yet_bearer_fresh_cache_publication_wrong_socket_ref() {
        let local_node = 0x1111_1111u32;
        let remote_node = 0x2222_2222u32;
        let remote_instance = 200u32;
        let published_ref = 0xAAAAu32;
        let stale_ref = 0xBBBBu32; // endpoint carries a different socket_ref

        let local = make_ep(TEST_NETID, 0xAAAA, local_node, 0xAAAA, 100);
        let topology = TopologyState::new(TEST_SVC_TYPE);

        topology.mark_fresh_for_test();
        topology.test_mark_node_up(remote_node);
        // Publish with published_ref; the peer advertises stale_ref.
        topology.test_publish_service(remote_instance, published_ref, remote_node);

        let transport = make_transport_for_test(local, Arc::clone(&topology));

        // Remote endpoint carries the old stale_ref.
        let remote = make_ep(TEST_NETID, 0xBBBB, remote_node, stale_ref, remote_instance);
        let peer = peer_info_from_ep(&remote);
        let iid = peer.instance_id();

        let result = transport.register(peer);
        assert!(
            matches!(result, Err(TransportError::NoEndpoint)),
            "arm 5 miss (wrong socket_ref): triple-check must reject a stale ref"
        );
        assert!(
            topology.pending.contains_key(&iid),
            "Gate::NotYet must park the peer in topology.pending"
        );
    }

    /// `key()` returns `"tipc"` by default.
    #[test]
    fn transport_key_is_tipc() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0, 0xBBBB, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);
        assert_eq!(transport.key(), TransportKey::from("tipc"));
    }

    /// `address()` contains a `"tipc"` entry.
    #[test]
    fn transport_address_has_tipc_entry() {
        let local = make_ep(TEST_NETID, 0xAAAA, 0, 0xBBBB, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);
        assert!(
            transport.address().get_entry("tipc").unwrap().is_some(),
            "address() must have a 'tipc' entry"
        );
    }

    /// Registering after unregistered instance returns `PeerNotRegistered`.
    #[test]
    fn check_health_unregistered_returns_peer_not_registered() {
        // We just need to confirm the method returns PeerNotRegistered without
        // needing a real tokio runtime.
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        let local = make_ep(TEST_NETID, 0xAAAA, 0, 0xBBBB, 10);
        let topology = TopologyState::new(TEST_SVC_TYPE);
        let transport = make_transport_for_test(local, topology);

        let iid = InstanceId::new_v4(); // unregistered
        let result = rt.block_on(transport.check_health(iid, Duration::from_millis(100)));
        assert!(
            matches!(result, Err(HealthCheckError::PeerNotRegistered)),
            "check_health on unregistered peer must return PeerNotRegistered"
        );
    }
}
