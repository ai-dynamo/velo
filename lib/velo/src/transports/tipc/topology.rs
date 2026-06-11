// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Topology-server watch: `NodeStateWatch` (node up/down) and `VeloServiceWatch`
//! (live Velo service publications), plus the shared `TopologyState` that wires them
//! into the register()-time reachability gate and the event-driven re-registration
//! path (invariants 6 and 7 from the proposal).
//!
//! ## Architecture
//!
//! `TopologyState` is created at builder time (`TipcTransportBuilder::build`) and
//! started (`start()`) during `Transport::start`.  A long-running tokio task:
//!
//! 1. Opens a `SOCK_SEQPACKET` connection to the TIPC topology server
//!    (`{TIPC_TOP_SRV, TIPC_TOP_SRV}` = `{1, 1}`).
//! 2. Writes two permanent subscriptions: `TIPC_NODE_STATE` and the Velo service
//!    type (both with `TIPC_SUB_PORTS | TIPC_WAIT_FOREVER`).
//! 3. Writes a barrier subscription (same range, `BARRIER_TIMEOUT_MS` = 10 ms).
//! 4. Reads 48-byte `tipc_event` frames, updating `node_state` and `service_watch`.
//! 5. On `TIPC_PUBLISHED` / node-up: re-drives the `pending` map through the
//!    re-register hook so cold-start TCP demotions self-heal.
//! 6. On `TIPC_SUBSCR_TIMEOUT` for the barrier: marks caches fresh and signals the
//!    `start()` future that initial replay is complete.
//!
//! ## Ready semantics — dual-subscription barrier
//!
//! The topology server processes subscriptions on an ordered `SEQPACKET` connection.
//! Writing node-state sub → velo-service sub → barrier sub (10 ms timeout) guarantees
//! that the barrier's `TIPC_SUBSCR_TIMEOUT` event arrives only after all initial-replay
//! `TIPC_PUBLISHED` events from the preceding two subscriptions have been delivered.
//! `start()` returns once `TIPC_SUBSCR_TIMEOUT` is received; `register()` therefore
//! never races a cold cache in the normal lifecycle (proposal §5.3, correction 1).
//!
//! ## Reconnect behaviour
//!
//! On connection loss: caches are marked stale and cleared; the gate returns
//! `Gate::NotYet` for any registration attempted while stale; the task reconnects
//! with exponential backoff (100 ms → 5 s); after the first successful replay on
//! reconnect, caches are marked fresh and all pending entries are re-driven.

use std::io;
use std::mem::{MaybeUninit, size_of};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use tokio::io::unix::AsyncFd;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use velo_ext::{InstanceId, PeerInfo};

use super::endpoint::TipcEndpoint;
use super::socket::create_tipc_seqpacket;
use super::sys::{
    AF_TIPC, SockaddrTipc, TIPC_NODE_STATE, TIPC_PUBLISHED, TIPC_SERVICE_ADDR, TIPC_SUB_PORTS,
    TIPC_SUBSCR_TIMEOUT, TIPC_TOP_SRV, TIPC_WAIT_FOREVER, TIPC_WITHDRAWN, TipcAddrUnion, TipcEvent,
    TipcServiceAddr, TipcServiceName, TipcServiceRange, TipcSubscr, tipc_to_sockaddr,
};

// ── Subscription handle identifiers ──────────────────────────────────────────
//
// Stored in `TipcSubscr::usr_handle[0]` and echoed back in every `TipcEvent::s`.
// We use this to identify which subscription generated a given event.

/// Node-state subscription (`TIPC_NODE_STATE`, `TIPC_WAIT_FOREVER`).
const HANDLE_NODE_STATE: u8 = 1;
/// Velo service subscription (velo service type, `TIPC_WAIT_FOREVER`).
const HANDLE_VELO_SERVICE: u8 = 2;
/// Barrier subscription (same range as velo service, short timeout).
const HANDLE_BARRIER: u8 = 0xff;

/// Barrier subscription timeout in milliseconds.
///
/// After writing the two permanent subscriptions, a third subscription with this
/// timeout is written.  The topology server processes subscriptions in SEQPACKET
/// order, so the barrier's `TIPC_SUBSCR_TIMEOUT` event arrives only after every
/// initial-replay `TIPC_PUBLISHED` event from subscriptions 1 and 2 has been
/// delivered.  10 ms comfortably exceeds any kernel scheduling jitter while
/// remaining invisible to `start()` latency.
const BARRIER_TIMEOUT_MS: u32 = 10;

/// Initial reconnect back-off delay (milliseconds).
const RECONNECT_INITIAL_MS: u64 = 100;
/// Maximum reconnect back-off delay (milliseconds).
const RECONNECT_MAX_MS: u64 = 5_000;

// ── NodeStateWatch ────────────────────────────────────────────────────────────

/// Cache of live node states from a `TIPC_NODE_STATE` topology subscription.
///
/// `TIPC_PUBLISHED` events mark a node as up; `TIPC_WITHDRAWN` marks it down.
/// An absent entry is treated as down by `is_up`.  The cache is cleared on
/// topology-connection loss and rebuilt from the initial replay on reconnect.
pub struct NodeStateWatch {
    map: DashMap<u32, bool>, // node_id → is_up
}

impl NodeStateWatch {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Returns `true` if `node` has been seen up and not subsequently seen down.
    ///
    /// O(1), lock-free read via `DashMap`.
    pub fn is_up(&self, node: u32) -> bool {
        self.map.get(&node).is_some_and(|r| *r)
    }

    fn set(&self, node: u32, up: bool) {
        self.map.insert(node, up);
    }

    fn clear(&self) {
        self.map.clear();
    }
}

// ── VeloServiceWatch ──────────────────────────────────────────────────────────

/// Cache of live Velo TIPC service publications from a `TIPC_SUB_PORTS` subscription.
///
/// Maintains `instance → (socket_ref, node)` as reported by `TIPC_PUBLISHED` events
/// for the configured Velo service type.  Entries are removed on `TIPC_WITHDRAWN`.
/// The cache is cleared on topology-connection loss and rebuilt on reconnect.
pub struct VeloServiceWatch {
    map: DashMap<u32, (u32, u32)>, // service_instance → (socket_ref, node)
}

impl VeloServiceWatch {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Returns `true` if `{instance, socket_ref, node}` is currently published
    /// in the Velo service name table.
    ///
    /// O(1), lock-free read via `DashMap`.  The triple check prevents a recycled
    /// `socket_ref` at the same instance from appearing reachable.
    pub fn publication_matches(&self, instance: u32, socket_ref: u32, node: u32) -> bool {
        self.map
            .get(&instance)
            .is_some_and(|r| *r == (socket_ref, node))
    }

    fn set(&self, instance: u32, socket_ref: u32, node: u32) {
        self.map.insert(instance, (socket_ref, node));
    }

    fn remove(&self, instance: u32) {
        self.map.remove(&instance);
    }

    fn clear(&self) {
        self.map.clear();
    }
}

// ── TopologyState ─────────────────────────────────────────────────────────────

/// Shared state between the TIPC transport and the topology-watcher task.
///
/// Created at builder time via [`TopologyState::new`].  Started (task spawned +
/// initial replay awaited) via [`TopologyState::start`].
///
/// # Usage in the register() gate
///
/// ```rust,ignore
/// // from transport.rs Gate evaluation:
/// if state.is_stale() {
///     return Gate::NotYet;        // topology connection not yet ready
/// }
/// if state.node_state.is_up(ep.node)
///     && state.service_watch.publication_matches(ep.service_instance, ep.socket_ref, ep.node)
/// {
///     return Gate::Reachable;
/// }
/// // else: insert into state.pending and return Gate::NotYet
/// state.pending.insert(peer_info.instance_id(), peer_info);
/// Gate::NotYet
/// ```
pub struct TopologyState {
    /// TIPC service type subscribed to for Velo listener publications.
    service_type: u32,
    /// Node-up/down cache from a `TIPC_NODE_STATE` subscription.
    pub node_state: NodeStateWatch,
    /// Live service-publication cache from a `TIPC_SUB_PORTS` subscription.
    pub service_watch: VeloServiceWatch,
    /// Pending peer registrations awaiting event-driven retry.
    ///
    /// Populated by `register()` on `Gate::NotYet`; re-driven through
    /// `reregister_hook` on every `TIPC_PUBLISHED` / node-up event.
    /// A fresh `register()` for the same `InstanceId` overwrites the entry.
    /// Bounded by the number of known peers.
    pub pending: DashMap<InstanceId, PeerInfo>,
    /// Decoded TIPC endpoints for pending registrations, stored at park time.
    ///
    /// Mirrors `pending` but holds the pre-decoded [`TipcEndpoint`] so the
    /// hot-path re-drive helpers can filter by `service_instance` or `node`
    /// without decoding the `WorkerAddress` bytes on every topology event.
    /// Absent for entries inserted directly into `pending` by test code (those
    /// are conservatively re-driven on every event).
    pending_decoded: DashMap<InstanceId, TipcEndpoint>,
    /// `true` while the topology-server connection is absent or being re-established.
    ///
    /// The `register()` gate treats stale caches as `Gate::NotYet`.  Set to `true`
    /// immediately on connection loss; cleared to `false` when the barrier
    /// `TIPC_SUBSCR_TIMEOUT` is received after a (re)connect.
    cache_stale: AtomicBool,
    /// Re-register hook called when a topology event suggests a pending peer
    /// registration might now succeed.
    ///
    /// Typically wires to `VeloBackend::register_peer` (invariant 7).
    /// Stored in `OnceLock` for lock-free reads on the topology-event hot path.
    /// Set by the velo builder after constructing `VeloBackend`; calling
    /// [`set_reregister_hook`] immediately re-drives any already-pending entries,
    /// so the install order relative to `start()` does not create a lost-event window.
    reregister_hook: OnceLock<Arc<dyn Fn(PeerInfo) + Send + Sync>>,
    /// Cancellation token for the topology-watcher task.
    ///
    /// Cancelled by [`cancel`] (called from `TipcTransport::shutdown()`).
    /// The topology task selects on this token in its reconnect-backoff loop and
    /// in its per-event read loop so that shutdown does not leave an orphaned task
    /// or a live SEQPACKET connection to the topology server.
    cancel_token: CancellationToken,
}

impl TopologyState {
    /// Create a new `TopologyState` for the given Velo service type.
    ///
    /// Both caches start empty and marked stale (no topology connection yet).
    /// Call [`TopologyState::start`] during `Transport::start` to establish the
    /// watch and drain the initial replay.
    pub fn new(service_type: u32) -> Arc<Self> {
        Arc::new(Self {
            service_type,
            node_state: NodeStateWatch::new(),
            service_watch: VeloServiceWatch::new(),
            pending: DashMap::new(),
            pending_decoded: DashMap::new(),
            cache_stale: AtomicBool::new(true),
            reregister_hook: OnceLock::new(),
            cancel_token: CancellationToken::new(),
        })
    }

    /// Returns `true` if the topology caches are stale.
    ///
    /// Stale means the topology-server connection is absent or the initial replay
    /// has not yet completed.  The `register()` gate returns `Gate::NotYet` when
    /// stale so that speculative `Gate::Reachable` verdicts on empty caches are
    /// avoided.
    pub fn is_stale(&self) -> bool {
        self.cache_stale.load(Ordering::Acquire)
    }

    /// Set the re-register hook.
    ///
    /// The hook is called with a `PeerInfo` when a topology event (node up or
    /// service published) suggests that a previously-rejected registration might
    /// now succeed.  In-tree usage: wires to `VeloBackend::register_peer`.
    ///
    /// A second call is a caller bug; it logs a warning and is ignored.
    ///
    /// After installing the hook, any entries already in `pending` are immediately
    /// re-driven through it.  This closes the window that would otherwise exist
    /// between `Transport::start()` returning (which seeds pending via topology
    /// replay) and `VeloBuilder::build()` calling this method: registrations
    /// attempted in that window are re-tried synchronously on install.
    pub fn set_reregister_hook(&self, hook: Arc<dyn Fn(PeerInfo) + Send + Sync>) {
        if self.reregister_hook.set(hook).is_err() {
            warn!("TIPC topology: set_reregister_hook called more than once; second call ignored");
            return;
        }
        // Immediately re-drive any pending entries that accumulated before the hook
        // was installed.  The OnceLock was just populated above; get() cannot fail.
        let hook = self
            .reregister_hook
            .get()
            .expect("hook was just installed via OnceLock::set");
        let entries: Vec<PeerInfo> = self.pending.iter().map(|r| r.value().clone()).collect();
        for peer_info in entries {
            debug!(
                "TIPC topology: re-driving pending peer {} on hook install",
                peer_info.instance_id()
            );
            hook(peer_info);
        }
    }

    /// Cancel the topology-watcher task.
    ///
    /// Called from `TipcTransport::shutdown()`.  Signals the long-running
    /// `topology_task` to exit its read loop and reconnect-backoff loop so the
    /// task — and the SEQPACKET connection it holds — are released promptly.
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Park a pending registration together with its pre-decoded endpoint.
    ///
    /// Stores the `PeerInfo` in [`Self::pending`] (for the re-register hook)
    /// and the decoded [`TipcEndpoint`] in the internal `pending_decoded` index
    /// (for selective filtering in [`redrive_pending_for_service`] and
    /// [`redrive_pending_for_node`]).
    ///
    /// A second call for the same `iid` overwrites the previous entry (e.g. on
    /// repeated cold-start re-registration attempts).
    pub fn park_pending(&self, iid: InstanceId, peer_info: PeerInfo, ep: TipcEndpoint) {
        self.pending_decoded.insert(iid, ep);
        self.pending.insert(iid, peer_info);
    }

    /// Remove a peer from all pending registration maps.
    ///
    /// Called by the transport when a previously-parked peer's registration
    /// succeeds (`Gate::Reachable` after an event-driven retry), so the entry
    /// is not re-driven on subsequent topology events.
    pub fn unpark_pending(&self, iid: &InstanceId) {
        self.pending.remove(iid);
        self.pending_decoded.remove(iid);
    }

    /// Mark the topology caches as fresh (test seam only).
    ///
    /// Simulates the barrier `TIPC_SUBSCR_TIMEOUT` arriving — i.e., the initial
    /// topology replay is complete and `is_stale()` returns `false`.
    ///
    /// Used in unit tests to drive the bearer-path arm of the `register()` gate
    /// (arm 5: fresh + node_up + publication_matches → `Gate::Reachable`) without
    /// a live topology-server connection.
    #[cfg(test)]
    pub fn mark_fresh_for_test(&self) {
        self.cache_stale.store(false, Ordering::Release);
    }

    /// Mark a node as up in the node-state cache (test seam only).
    #[cfg(test)]
    pub fn test_mark_node_up(&self, node: u32) {
        self.node_state.set(node, true);
    }

    /// Publish a service in the service-watch cache (test seam only).
    #[cfg(test)]
    pub fn test_publish_service(&self, instance: u32, socket_ref: u32, node: u32) {
        self.service_watch.set(instance, socket_ref, node);
    }

    /// Start the topology watcher.
    ///
    /// Connects to the TIPC topology server (`{TIPC_TOP_SRV, TIPC_TOP_SRV}`),
    /// writes the node-state and Velo-service subscriptions plus a barrier
    /// subscription, then waits until the barrier's `TIPC_SUBSCR_TIMEOUT` is
    /// received (confirming the initial replay is complete).  Returns `Ok(())`
    /// once caches are coherent; `register()` can then be called without racing
    /// cold caches.
    ///
    /// A long-running tokio task continues to maintain the caches after this
    /// call returns.  On connection loss the task marks caches stale, clears
    /// them, and reconnects with exponential back-off.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the initial connection to the topology server fails —
    /// typically because the TIPC kernel module is not loaded.
    pub async fn start(self: Arc<Self>) -> io::Result<()> {
        // Initial connect: verify TIPC is available before spawning the task.
        // Failures here surface "module not loaded" to the caller immediately.
        let afd = connect_and_subscribe(self.service_type).await?;

        // One-shot channel: the topology task fires this when the barrier
        // TIPC_SUBSCR_TIMEOUT is received, signalling that initial replay is done.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        let state = Arc::clone(&self);
        let ready_tx_task = Arc::clone(&ready_tx);

        tokio::spawn(async move {
            topology_task(state, afd, ready_tx_task).await;
        });

        ready_rx.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TIPC topology task exited before signalling ready",
            )
        })
    }
}

// ── Topology task ─────────────────────────────────────────────────────────────

/// Long-running task body.  Runs the reader loop, reconnects on loss.
async fn topology_task(
    state: Arc<TopologyState>,
    initial_afd: AsyncFd<socket2::Socket>,
    ready_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
) {
    let mut maybe_afd: Option<AsyncFd<socket2::Socket>> = Some(initial_afd);
    let mut backoff_ms = RECONNECT_INITIAL_MS;

    loop {
        // Either use the pre-connected afd from start() or reconnect.
        let afd = if let Some(a) = maybe_afd.take() {
            a
        } else {
            // Backoff before reconnect — cancelled immediately on transport shutdown
            // so the task does not hang for up to RECONNECT_MAX_MS (5 s) after the
            // transport is torn down.
            tokio::select! {
                biased;
                _ = state.cancel_token.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
            }
            match connect_and_subscribe(state.service_type).await {
                Ok(a) => {
                    backoff_ms = RECONNECT_INITIAL_MS;
                    info!("TIPC topology: reconnected to topology server");
                    a
                }
                Err(e) => {
                    warn!("TIPC topology: reconnect failed: {e}; retry in {backoff_ms} ms");
                    backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
                    continue;
                }
            }
        };

        // Read events until the connection is lost or the task is cancelled.
        reader_loop(&state, afd, &ready_tx).await;

        // reader_loop returns on connection loss OR on cancel.
        // Check cancellation before marking caches stale / attempting reconnect.
        if state.cancel_token.is_cancelled() {
            return;
        }

        // Mark caches stale and clear them so the gate returns Gate::NotYet
        // rather than serving stale data from a dead topology connection.
        state.cache_stale.store(true, Ordering::Release);
        state.node_state.clear();
        state.service_watch.clear();
        warn!(
            "TIPC topology: connection to topology server lost; \
             caches cleared, reconnecting with back-off"
        );
    }
}

/// Read events from `afd` until an error occurs (connection lost) or the
/// topology watcher is cancelled.
async fn reader_loop(
    state: &Arc<TopologyState>,
    afd: AsyncFd<socket2::Socket>,
    ready_tx: &Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
) {
    loop {
        tokio::select! {
            // Prioritise cancellation so transport shutdown exits promptly even
            // when the topology server is sending a flood of events.
            biased;
            _ = state.cancel_token.cancelled() => return,
            result = read_event(&afd) => {
                match result {
                    Ok(event) => process_event(state, &event, ready_tx),
                    Err(e) => {
                        debug!("TIPC topology: read error (connection lost): {e}");
                        return;
                    }
                }
            }
        }
    }
}

/// Dispatch a single `TipcEvent` to the appropriate cache update and side-effects.
fn process_event(
    state: &Arc<TopologyState>,
    event: &TipcEvent,
    ready_tx: &Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
) {
    let handle = event.s.usr_handle[0];

    match event.event {
        TIPC_PUBLISHED => match handle {
            HANDLE_NODE_STATE => {
                // Node-state PUBLISHED = node came up.
                // found_lower == found_upper == 32-bit node ID for NODE_STATE events.
                let node = event.found_lower;
                state.node_state.set(node, true);
                debug!("TIPC topology: node {node:#010x} up");
                // Re-drive only peers parked for this specific node, not all pending.
                redrive_pending_for_node(state, node);
            }
            HANDLE_VELO_SERVICE => {
                // Velo service binding appeared.  For TIPC_SUB_PORTS on a
                // single-instance bind, found_lower == found_upper == instance.
                let instance = event.found_lower;
                let socket_ref = event.port.ref_;
                let node = event.port.node;
                state.service_watch.set(instance, socket_ref, node);
                debug!(
                    "TIPC topology: velo instance {instance:#010x} published by \
                     {{ref={socket_ref:#010x}, node={node:#010x}}}"
                );
                // Re-drive only the peer whose service_instance matches this event.
                redrive_pending_for_service(state, instance);
            }
            HANDLE_BARRIER => {
                // The barrier subscription covers the same service range; its
                // PUBLISHED events duplicate what HANDLE_VELO_SERVICE already
                // delivered.  Update the cache (idempotent) but do not re-drive
                // pending a second time — that would cause spurious register() calls.
                let instance = event.found_lower;
                let socket_ref = event.port.ref_;
                let node = event.port.node;
                state.service_watch.set(instance, socket_ref, node);
            }
            _ => {}
        },

        TIPC_WITHDRAWN => match handle {
            HANDLE_NODE_STATE => {
                let node = event.found_lower;
                state.node_state.set(node, false);
                debug!("TIPC topology: node {node:#010x} down");
            }
            HANDLE_VELO_SERVICE | HANDLE_BARRIER => {
                let instance = event.found_lower;
                state.service_watch.remove(instance);
                debug!("TIPC topology: velo instance {instance:#010x} withdrawn");
            }
            _ => {}
        },

        TIPC_SUBSCR_TIMEOUT => {
            if handle == HANDLE_BARRIER {
                // All initial-replay PUBLISHED events from subscriptions 1 and 2 have
                // been delivered (ordered SEQPACKET guarantees this).  Mark caches fresh,
                // re-drive ALL pending entries (safety valve: initial replay or reconnect
                // replay is complete, so we retry every parked peer unconditionally).
                state.cache_stale.store(false, Ordering::Release);
                debug!("TIPC topology: initial replay complete; caches are now fresh");
                redrive_pending_all(state);
                // Signal fires only once; subsequent reconnects find ready_tx empty.
                if let Ok(mut guard) = ready_tx.lock()
                    && let Some(tx) = guard.take()
                {
                    let _ = tx.send(());
                }
            }
            // Permanent subscriptions (HANDLE_NODE_STATE, HANDLE_VELO_SERVICE) use
            // TIPC_WAIT_FOREVER and will never produce SUBSCR_TIMEOUT.
        }

        _ => {
            debug!("TIPC topology: unknown event type {}", event.event);
        }
    }
}

/// Re-drive all pending `PeerInfo`s through the re-register hook.
///
/// Safety valve: called on barrier `TIPC_SUBSCR_TIMEOUT` (initial replay complete)
/// and after a topology reconnect replay, when every pending peer should be retried
/// regardless of which event arrived.
fn redrive_pending_all(state: &Arc<TopologyState>) {
    let hook = match state.reregister_hook.get() {
        Some(h) => h,
        None => return,
    };
    // Collect before calling hook to avoid holding the DashMap shard lock across
    // a potentially slow register() call.
    let entries: Vec<PeerInfo> = state.pending.iter().map(|r| r.value().clone()).collect();
    for peer_info in entries {
        debug!(
            "TIPC topology: re-driving pending peer {} (full sweep)",
            peer_info.instance_id()
        );
        hook(peer_info);
    }
}

/// Re-drive only the pending peer whose `TipcEndpoint.service_instance` matches
/// `service_instance`.
///
/// Called on `TIPC_PUBLISHED` for the Velo service subscription so that a single
/// publication event triggers at most one register() call, not O(pending) calls.
///
/// Entries without a pre-decoded endpoint in `pending_decoded` (e.g., inserted
/// directly by test code) are re-driven conservatively.
fn redrive_pending_for_service(state: &Arc<TopologyState>, service_instance: u32) {
    let hook = match state.reregister_hook.get() {
        Some(h) => h,
        None => return,
    };
    let entries: Vec<PeerInfo> = state
        .pending
        .iter()
        .filter(|r| {
            state
                .pending_decoded
                .get(r.key())
                .map(|ep| ep.service_instance == service_instance)
                .unwrap_or(true) // conservative: re-drive if no decoded endpoint cached
        })
        .map(|r| r.value().clone())
        .collect();
    for peer_info in entries {
        debug!(
            "TIPC topology: re-driving pending peer {} (service_instance={:#010x})",
            peer_info.instance_id(),
            service_instance
        );
        hook(peer_info);
    }
}

/// Re-drive only pending peers whose `TipcEndpoint.node` matches `node`.
///
/// Called on `TIPC_PUBLISHED` for the node-state subscription (node up) so that
/// a node-up event only re-tries peers on that specific node.
///
/// Entries without a pre-decoded endpoint are re-driven conservatively.
fn redrive_pending_for_node(state: &Arc<TopologyState>, node: u32) {
    let hook = match state.reregister_hook.get() {
        Some(h) => h,
        None => return,
    };
    let entries: Vec<PeerInfo> = state
        .pending
        .iter()
        .filter(|r| {
            state
                .pending_decoded
                .get(r.key())
                .map(|ep| ep.node == node)
                .unwrap_or(true) // conservative: re-drive if no decoded endpoint cached
        })
        .map(|r| r.value().clone())
        .collect();
    for peer_info in entries {
        debug!(
            "TIPC topology: re-driving pending peer {} (node={:#010x} up)",
            peer_info.instance_id(),
            node
        );
        hook(peer_info);
    }
}

// ── Socket helpers ────────────────────────────────────────────────────────────

/// Create an `AsyncFd<socket2::Socket>` connected to the TIPC topology server
/// (`{TIPC_SERVICE_ADDR, type=TIPC_TOP_SRV, instance=TIPC_TOP_SRV}`) with all
/// three subscriptions written.
async fn connect_and_subscribe(service_type: u32) -> io::Result<AsyncFd<socket2::Socket>> {
    let afd = connect_to_topsrv().await?;
    write_subscriptions(&afd, service_type).await?;
    Ok(afd)
}

/// Open a non-blocking `SOCK_SEQPACKET` socket and connect it to
/// `{TIPC_SERVICE_ADDR, type=TIPC_TOP_SRV=1, instance=TIPC_TOP_SRV=1}`.
///
/// Returns an `AsyncFd` ready for writes after the connection completes.
async fn connect_to_topsrv() -> io::Result<AsyncFd<socket2::Socket>> {
    let sock = create_tipc_seqpacket()?;

    // Service-address connect to {type=1, instance=1} — the topology server.
    let addr = SockaddrTipc {
        family: AF_TIPC,
        addrtype: TIPC_SERVICE_ADDR,
        scope: 0, // ignored for connect
        addr: TipcAddrUnion {
            service_name: TipcServiceName {
                name: TipcServiceAddr {
                    type_: TIPC_TOP_SRV,
                    instance: TIPC_TOP_SRV,
                },
                domain: 0,
            },
        },
    };
    let sa = tipc_to_sockaddr(&addr);

    match sock.connect(&sa) {
        Ok(()) => {}
        Err(e)
            if e.raw_os_error() == Some(libc::EINPROGRESS)
                || e.kind() == io::ErrorKind::WouldBlock =>
        {
            // Non-blocking connect in progress — wait for writability below.
        }
        Err(e) => return Err(e),
    }

    let afd = AsyncFd::new(sock)?;

    // Wait for the connect to complete (SEQPACKET to a local service is fast).
    //
    // IMPORTANT: drop the guard WITHOUT calling clear_ready().  This retains
    // write readiness in AsyncFd so that the immediately following
    // write_subscription() calls can issue their sends without waiting for a
    // fresh EPOLLOUT event.  With edge-triggered epoll (EPOLLET), calling
    // clear_ready() here would wipe AsyncFd's internal state without consuming
    // writability at the kernel level — no state change occurs, so no new EPOLLOUT
    // is delivered, and the first write_subscription().await hangs indefinitely.
    let _connect_guard = afd.writable().await?;

    // Check SO_ERROR to confirm the connect succeeded.  Uses getsockopt directly,
    // independent of epoll readiness.
    if let Some(err) = afd.get_ref().take_error()? {
        return Err(err);
    }

    // _connect_guard dropped here without clear_ready() — write readiness retained.
    Ok(afd)
}

/// Write the three topology subscriptions to `afd`.
///
/// Order matters: node-state (1) → velo-service (2) → barrier (3).
/// The topology server processes them in SEQPACKET order, so all initial-replay
/// events from (1) and (2) arrive before the barrier's `TIPC_SUBSCR_TIMEOUT`.
async fn write_subscriptions(afd: &AsyncFd<socket2::Socket>, service_type: u32) -> io::Result<()> {
    // Subscription 1: node up/down events.
    write_subscription(
        afd,
        &make_subscription(
            TIPC_NODE_STATE,
            0,
            0xffff_ffff,
            TIPC_WAIT_FOREVER,
            HANDLE_NODE_STATE,
        ),
    )
    .await?;

    // Subscription 2: Velo service publications (one event per binding, per §2.5).
    write_subscription(
        afd,
        &make_subscription(
            service_type,
            0,
            0xffff_ffff,
            TIPC_WAIT_FOREVER,
            HANDLE_VELO_SERVICE,
        ),
    )
    .await?;

    // Subscription 3: Barrier — same range, short timeout.  SUBSCR_TIMEOUT arrives
    // only after all initial-replay events from (1) and (2) have been delivered.
    write_subscription(
        afd,
        &make_subscription(
            service_type,
            0,
            0xffff_ffff,
            BARRIER_TIMEOUT_MS,
            HANDLE_BARRIER,
        ),
    )
    .await?;

    Ok(())
}

/// Build a `TipcSubscr` with the given parameters.
fn make_subscription(type_: u32, lower: u32, upper: u32, timeout: u32, handle: u8) -> TipcSubscr {
    let mut usr_handle = [0u8; 8];
    usr_handle[0] = handle;
    TipcSubscr {
        seq: TipcServiceRange {
            type_,
            lower,
            upper,
        },
        timeout,
        filter: TIPC_SUB_PORTS,
        usr_handle,
    }
}

/// Write a single 28-byte `TipcSubscr` to the topology-server socket.
///
/// Loops on `WouldBlock` (edge-triggered `AsyncFd`).
async fn write_subscription(afd: &AsyncFd<socket2::Socket>, sub: &TipcSubscr) -> io::Result<()> {
    // SAFETY: TipcSubscr is #[repr(C)] POD; the byte slice is valid for its lifetime.
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            sub as *const TipcSubscr as *const u8,
            size_of::<TipcSubscr>(),
        )
    };
    loop {
        let mut guard = afd.writable().await?;
        match guard.try_io(|inner| {
            let fd = inner.get_ref().as_raw_fd();
            // SAFETY: `bytes` is valid for `bytes.len()` bytes; fd is a live socket.
            let n = unsafe {
                libc::send(
                    fd,
                    bytes.as_ptr().cast::<libc::c_void>(),
                    bytes.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }) {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(e)) => return Err(e),
            Err(_would_block) => continue, // edge-triggered: clear and retry
        }
    }
}

/// Read a single 48-byte `TipcEvent` from the topology-server socket.
///
/// `SOCK_SEQPACKET` preserves message boundaries; each `recv` call returns exactly
/// one event.  Returns `Err` if the connection is lost or delivers a malformed frame.
async fn read_event(afd: &AsyncFd<socket2::Socket>) -> io::Result<TipcEvent> {
    let mut event = MaybeUninit::<TipcEvent>::uninit();
    loop {
        let mut guard = afd.readable().await?;
        match guard.try_io(|inner| {
            let fd = inner.get_ref().as_raw_fd();
            // SAFETY: MaybeUninit<TipcEvent> has correct size/alignment;
            // `recv` will write exactly `size_of::<TipcEvent>()` bytes on success.
            let n = unsafe {
                libc::recv(
                    fd,
                    event.as_mut_ptr().cast::<libc::c_void>(),
                    size_of::<TipcEvent>(),
                    0,
                )
            };
            if n < 0 {
                Err(io::Error::last_os_error())
            } else if n == 0 {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "TIPC topology server closed connection",
                ))
            } else if n as usize != size_of::<TipcEvent>() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "expected {}-byte tipc_event, got {n} bytes",
                        size_of::<TipcEvent>()
                    ),
                ))
            } else {
                // SAFETY: recv returned exactly size_of::<TipcEvent>() bytes.
                Ok(unsafe { event.assume_init() })
            }
        }) {
            Ok(Ok(ev)) => return Ok(ev),
            Ok(Err(e)) => return Err(e),
            Err(_would_block) => continue, // edge-triggered: clear and retry
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    use super::super::endpoint::TipcEndpoint;
    use super::super::socket::{
        bind_single_instance_and_listen, create_tipc_stream, getsockname_ref_node, tipc_available,
    };
    use velo_ext::{TransportKey, WorkerAddress};

    // ── Service type used by all topology unit tests ──────────────────────────
    //
    // Must be ≥ TIPC_RESERVED_TYPES (64) and distinct from types used by
    // other test modules to avoid inter-test cross-talk.
    const TOPO_TEST_TYPE: u32 = 0x5654_0300;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Build a `PeerInfo` whose `TipcEndpoint` matches the given socket binding.
    fn make_peer_info_for_binding(
        service_instance: u32,
        socket_ref: u32,
        node: u32,
    ) -> (InstanceId, PeerInfo) {
        let ep = TipcEndpoint {
            version: 1,
            service_type: TOPO_TEST_TYPE,
            service_instance,
            node,
            socket_ref,
            netid: 4711,
            node_id: [0u8; 16],
            netns_nonce: 0,
            scope: 2,
        };
        let key = TransportKey::from(TipcEndpoint::KEY);
        let addr = ep.encode_into_worker_address(&key).unwrap();
        let iid = InstanceId::new_v4();
        let peer_info = PeerInfo::new(iid, addr);
        (iid, peer_info)
    }

    // ── Test: service_watch updated on PUBLISHED / cleared on WITHDRAWN ───────

    /// Verify that after `start()`, a service bound before topology startup appears
    /// in `service_watch` and is removed after the socket is dropped.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn topology_service_watch_published_and_withdrawn() {
        if !tipc_available() {
            eprintln!(
                "topology_service_watch_published_and_withdrawn: TIPC not available; \
                 run `sudo modprobe tipc`"
            );
            return;
        }

        const INSTANCE: u32 = 0xDEAD_0301;

        // Bind a service socket BEFORE starting topology so it appears in
        // the initial-replay PUBLISHED events.
        let listener = create_tipc_stream().unwrap();
        bind_single_instance_and_listen(&listener, TOPO_TEST_TYPE, INSTANCE, 4).unwrap();
        let (socket_ref, node) = getsockname_ref_node(&listener).unwrap();

        // Start topology watch and await initial replay.
        let state = TopologyState::new(TOPO_TEST_TYPE);
        Arc::clone(&state)
            .start()
            .await
            .expect("topology start should succeed");

        // After start() returns the initial replay is complete: the binding must be
        // in service_watch.
        assert!(
            state
                .service_watch
                .publication_matches(INSTANCE, socket_ref, node),
            "service_watch must contain the pre-existing binding after initial replay"
        );

        assert!(
            !state.is_stale(),
            "caches must be marked fresh after initial replay"
        );

        // Drop the listener → TIPC topology server sends TIPC_WITHDRAWN within ~0.3 ms
        // (§2.5, [verified]).  Wait 100 ms for the event to arrive and be processed.
        drop(listener);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            !state
                .service_watch
                .publication_matches(INSTANCE, socket_ref, node),
            "service_watch must be cleared after TIPC_WITHDRAWN"
        );
    }

    // ── Test: pending entry is re-driven and hook fires on PUBLISHED event ────

    /// Verify the cold-start recovery path:
    ///
    /// 1. Bind a service socket before starting topology.
    /// 2. Insert a matching `PeerInfo` into `pending` before `start()`.
    /// 3. Set a hook that records which `InstanceId`s it was called with.
    /// 4. Call `start()`.
    /// 5. By the time `start()` returns the initial replay is done, the
    ///    `TIPC_PUBLISHED` event has fired the hook, and `hook_called` is true.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn topology_pending_hook_fires_on_published() {
        if !tipc_available() {
            eprintln!(
                "topology_pending_hook_fires_on_published: TIPC not available; \
                 run `sudo modprobe tipc`"
            );
            return;
        }

        const INSTANCE: u32 = 0xDEAD_0302;

        // Bind so the initial replay delivers a PUBLISHED event for this instance.
        let listener = create_tipc_stream().unwrap();
        bind_single_instance_and_listen(&listener, TOPO_TEST_TYPE, INSTANCE, 4).unwrap();
        let (socket_ref, node) = getsockname_ref_node(&listener).unwrap();

        let state = TopologyState::new(TOPO_TEST_TYPE);

        // Insert the matching PeerInfo into pending BEFORE start() so it is found
        // when the PUBLISHED event fires during the initial replay.
        let (iid, peer_info) = make_peer_info_for_binding(INSTANCE, socket_ref, node);
        state.pending.insert(iid, peer_info);

        // Install a hook that records the InstanceId it was called with.
        let hook_called = Arc::new(AtomicBool::new(false));
        {
            let hook_called = Arc::clone(&hook_called);
            let expected_iid = iid;
            state.set_reregister_hook(Arc::new(move |pi: PeerInfo| {
                if pi.instance_id() == expected_iid {
                    hook_called.store(true, Ordering::SeqCst);
                }
            }));
        }

        // start() awaits the barrier TIPC_SUBSCR_TIMEOUT.  By that point the
        // TIPC_PUBLISHED event (step 5 in the module sequence) has already fired
        // the hook, so hook_called is visible here without any extra sleep.
        Arc::clone(&state)
            .start()
            .await
            .expect("topology start should succeed");

        assert!(
            hook_called.load(Ordering::SeqCst),
            "re-register hook must have been called for the pending peer \
             when its TIPC_PUBLISHED event arrived during initial replay"
        );

        // Cleanup: drop listener, give event a moment to arrive (not required for
        // correctness of this test, but avoids leaving the subscription live).
        drop(listener);
    }

    // ── Test: hook fires for a publication that appears AFTER start() ─────────

    /// Verify the post-start recovery path: a new service binding fires the hook
    /// for a pending entry that was inserted after initial replay.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn topology_pending_hook_fires_on_post_start_publish() {
        if !tipc_available() {
            eprintln!(
                "topology_pending_hook_fires_on_post_start_publish: TIPC not available; \
                 run `sudo modprobe tipc`"
            );
            return;
        }

        const INSTANCE: u32 = 0xDEAD_0303;

        // Start with no pre-existing service of this instance.
        let state = TopologyState::new(TOPO_TEST_TYPE);

        let hook_called = Arc::new(AtomicBool::new(false));
        {
            let hook_called = Arc::clone(&hook_called);
            state.set_reregister_hook(Arc::new(move |_pi: PeerInfo| {
                hook_called.store(true, Ordering::SeqCst);
            }));
        }

        Arc::clone(&state)
            .start()
            .await
            .expect("topology start should succeed");

        // Bind after start().  This generates a live TIPC_PUBLISHED event (not a
        // replay event).  The topology task will call redrive_pending when it arrives.
        let listener = create_tipc_stream().unwrap();
        bind_single_instance_and_listen(&listener, TOPO_TEST_TYPE, INSTANCE, 4).unwrap();
        let (socket_ref, node) = getsockname_ref_node(&listener).unwrap();

        // Insert into pending; the topology task will re-drive it when the
        // TIPC_PUBLISHED event fires.
        let (iid, peer_info) = make_peer_info_for_binding(INSTANCE, socket_ref, node);
        state.pending.insert(iid, peer_info);

        // Wait for the PUBLISHED event (arrives within ~0.331 ms per §2.5; 100 ms
        // is well beyond that).
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            hook_called.load(Ordering::SeqCst),
            "re-register hook must fire when a live TIPC_PUBLISHED event \
             arrives for an entry already in pending"
        );

        assert!(
            state
                .service_watch
                .publication_matches(INSTANCE, socket_ref, node),
            "service_watch must reflect the newly published binding"
        );

        drop(listener);
    }

    // ── Unit tests: no kernel required ────────────────────────────────────────

    #[test]
    fn node_state_watch_is_up_absent_returns_false() {
        let w = NodeStateWatch::new();
        assert!(!w.is_up(0x0102_0304), "absent node must be treated as down");
    }

    #[test]
    fn node_state_watch_set_and_read() {
        let w = NodeStateWatch::new();
        w.set(42, true);
        assert!(w.is_up(42));
        w.set(42, false);
        assert!(!w.is_up(42));
    }

    #[test]
    fn node_state_watch_clear_removes_all() {
        let w = NodeStateWatch::new();
        w.set(1, true);
        w.set(2, true);
        w.clear();
        assert!(!w.is_up(1));
        assert!(!w.is_up(2));
    }

    #[test]
    fn velo_service_watch_publication_matches_absent_returns_false() {
        let w = VeloServiceWatch::new();
        assert!(
            !w.publication_matches(100, 0xabcd, 0x1234),
            "absent entry must not match"
        );
    }

    #[test]
    fn velo_service_watch_set_and_exact_triple_matches() {
        let w = VeloServiceWatch::new();
        w.set(100, 0xabcd, 0x1234);
        assert!(w.publication_matches(100, 0xabcd, 0x1234));
        // Wrong ref
        assert!(!w.publication_matches(100, 0x0000, 0x1234));
        // Wrong node
        assert!(!w.publication_matches(100, 0xabcd, 0x0000));
        // Wrong instance
        assert!(!w.publication_matches(101, 0xabcd, 0x1234));
    }

    #[test]
    fn velo_service_watch_remove_clears_entry() {
        let w = VeloServiceWatch::new();
        w.set(100, 0xabcd, 0x1234);
        w.remove(100);
        assert!(!w.publication_matches(100, 0xabcd, 0x1234));
    }

    #[test]
    fn velo_service_watch_clear_removes_all() {
        let w = VeloServiceWatch::new();
        w.set(1, 0xa, 0xb);
        w.set(2, 0xc, 0xd);
        w.clear();
        assert!(!w.publication_matches(1, 0xa, 0xb));
        assert!(!w.publication_matches(2, 0xc, 0xd));
    }

    #[test]
    fn topology_state_starts_stale() {
        let state = TopologyState::new(0x5645_4c4f);
        assert!(state.is_stale(), "new TopologyState must start stale");
    }

    #[test]
    fn topology_state_pending_insert_and_lookup() {
        let state = TopologyState::new(0x5645_4c4f);
        let iid = InstanceId::new_v4();
        let addr = WorkerAddress::empty();
        let peer_info = PeerInfo::new(iid, addr);
        state.pending.insert(iid, peer_info.clone());
        assert!(state.pending.contains_key(&iid));
        state.pending.remove(&iid);
        assert!(!state.pending.contains_key(&iid));
    }

    #[test]
    fn make_subscription_fields_correct() {
        let sub = make_subscription(42, 0, 0xffff_ffff, TIPC_WAIT_FOREVER, HANDLE_NODE_STATE);
        assert_eq!(sub.seq.type_, 42);
        assert_eq!(sub.seq.lower, 0);
        assert_eq!(sub.seq.upper, 0xffff_ffff);
        assert_eq!(sub.timeout, TIPC_WAIT_FOREVER);
        assert_eq!(sub.filter, TIPC_SUB_PORTS);
        assert_eq!(sub.usr_handle[0], HANDLE_NODE_STATE);
        // Remaining bytes must be zero.
        assert_eq!(&sub.usr_handle[1..], &[0u8; 7]);
    }
}
