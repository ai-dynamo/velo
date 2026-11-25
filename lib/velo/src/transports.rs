// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs)]

//! Multi-transport active message routing framework.
//!
//! This module abstracts TCP, UDS, QUIC, HTTP, NATS, gRPC, ZMQ, and UCX behind
//! a unified [`Transport`] trait with zero-copy [`bytes::Bytes`],
//! fire-and-forget error callbacks, priority-based peer routing, and 4-phase
//! graceful shutdown.
//!
//! # Architecture
//!
//! The backend inside each [`Messenger`](crate::Messenger) is the central
//! orchestrator. It holds a set of transports, each identified by a
//! [`TransportKey`]. When a peer registers, the backend
//! selects a *primary* transport (highest-priority compatible transport) and
//! records any alternatives. Outbound messages are routed through the primary
//! transport by default, or through an explicit alternative.
//!
//! Inbound messages arrive via [`DataStreams`] — four independent channels
//! for messages, responses, events, and drain rejections (`ShuttingDown`).
//!
//! # Shutdown
//!
//! Graceful shutdown follows four phases:
//! 1. **Gate** — flip the draining flag; transports reject new inbound requests.
//! 2. **Drain** — wait for all in-flight requests to complete.
//! 3. **Teardown** — cancel listeners/writers and call `shutdown()` on each transport.
//! 4. **Close** — await `closed()` on each transport, so written data reaches the peer.

pub(crate) mod address;

/// Write coalescing shared by the TCP, UDS, QUIC, and streaming writer loops.
pub(crate) mod coalesce;

/// Inbound frame routing shared by the TCP, UDS, and QUIC listeners and the
/// read half of their dialed connections.
pub(crate) mod ingress;

/// What the TCP and UDS connection writers keep doing after their transport
/// tears down, and for how long.
pub(crate) mod teardown_grace;

pub mod tcp;

/// Shared utility functions for transport implementations.
pub mod utils;

#[cfg(unix)]
pub mod uds;

#[cfg(all(target_os = "linux", feature = "ucx"))]
pub mod ucx;

// #[cfg(feature = "http")]
// pub mod http;

#[cfg(feature = "nats-transport")]
pub mod nats;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "zmq")]
pub mod zmq;

#[cfg(feature = "quic")]
pub mod quic;

pub(crate) mod teardown;
mod transport;

use std::sync::OnceLock;
use std::{collections::HashMap, sync::Arc};

use crate::messenger::PeerDiscovery;
use crate::messenger::client::{LookupTimedOut, PeerKey, PeerRegistry};
use crate::observability::{ClientResolution, Direction, TransportRejection, VeloMetrics};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;

// Identity / address types from velo-ext are reachable as `velo::InstanceId`,
// `velo::PeerInfo`, etc. and via the `velo_ext` crate root. Pulling them in
// here too is just noise.
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress, WorkerId};

// Internal builder for address construction
use address::WorkerAddressBuilder;

// Re-export interface discovery types (used by host-affinity tests + ZMQ NUMA hints)

// Trait surface — fully defined in `velo-ext`. Re-exported here as a
// convenience for callers reaching into `velo::transports::*` for transport
// orchestration, but `velo_ext::*` is the canonical source.
pub use transport::{
    AdmissionError, AdmissionGate, AdmitOutcome, DataStreams, HealthCheckError, InFlightGuard,
    InboundMessage, MessageType, SendAdmission, SendOutcome, ShutdownState, TransportAdapter,
    TransportError, TransportErrorHandler, make_channels,
};
// Their public path is the crate root (`velo::Transport`, ...).
pub(crate) use transport::{AdmissionState, ShutdownPolicy, Transport};

/// How long a reply waits for discovery to find the peer it answers, unless
/// the builder sets another bound. Enough for a healthy discovery backend and
/// short enough that a lost reply does not hold the handler that owes it.
pub(crate) const DEFAULT_REPLY_LOOKUP_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(5);

/// The most discovery lookups one node runs at once. Each miss runs its own
/// lookup, so a burst of misses would otherwise send a burst of requests to
/// the discovery backend. A miss past the cap waits for a permit inside its
/// own bound. 64 is far above what a healthy node needs, since a lookup is one
/// short round trip and a miss that gets its permit after the peer was
/// registered asks nothing; it is low enough that one node cannot flood a
/// shared discovery service.
pub(crate) const MAX_CONCURRENT_LOOKUPS: usize = 64;

/// The most misses that wait for one of those permits. A miss past this is
/// refused at once. 1024 holds a large burst of replies to one forgotten
/// peer, which the first lookups then answer from the table, and it bounds
/// what a peer can make this node hold: worker ids come off the wire.
pub(crate) const MAX_PENDING_LOOKUPS: usize = 1024;

/// The bound on the lookup of a miss that has no deadline of its own: a send
/// with no timeout. The same 30 s such a send's handshake has, so no lookup
/// holds its permit for ever.
pub(crate) const UNTIMED_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Errors from the transport backend that routes a messenger's sends. A send
/// that fails for one of these reasons carries it in its `anyhow::Error`.
#[derive(Debug, thiserror::Error)]
pub enum VeloBackendError {
    /// No transport could accept the peer's address.
    #[error("No compatible transports found")]
    NoCompatibleTransports,

    /// The target instance was never registered, or was unregistered.
    #[error("Transport not found for instance: {0}")]
    InstanceNotRegistered(InstanceId),

    /// The worker ID is not in the fast-path cache.
    #[error("Worker not found: {0}")]
    WorkerNotRegistered(WorkerId),
}

/// Central orchestrator that aggregates multiple transports and routes messages
/// to peers via priority-based transport selection.
///
/// Each peer is registered with all compatible transports; the highest-priority
/// compatible transport becomes the *primary* for that peer. Worker IDs are
/// cached for fast-path routing without discovery lookups.
///
/// Crate-private: a [`Messenger`](crate::Messenger) owns one, and applications
/// reach it through `Velo`.
pub(crate) struct VeloBackend {
    instance_id: InstanceId,
    address: WorkerAddress,
    priorities: Mutex<Vec<TransportKey>>,
    transports: HashMap<TransportKey, Arc<dyn Transport>>,
    transport_metrics: HashMap<TransportKey, Arc<crate::observability::TransportMetricsHandle>>,
    primary_transport: DashMap<InstanceId, Arc<dyn Transport>>,
    alternative_transports: DashMap<InstanceId, Vec<TransportKey>>,
    workers: DashMap<WorkerId, InstanceId>,
    shutdown_state: ShutdownState,
    teardown: OnceLock<teardown::Completion>,
    teardown_outcome: teardown::Outcome,
    runtime: tokio::runtime::Handle,
    observability: Option<Arc<VeloMetrics>>,
    /// Each registered peer's `PeerInfo`, so that an observer installed
    /// late hears of it. Written under the peer's `primary_transport` entry.
    peers: DashMap<InstanceId, PeerInfo>,
    peer_observer: OnceLock<Arc<dyn PeerTableObserver>>,
    advertised: OnceLock<PeerInfo>,
    /// Discovery for a peer the table missed, and the client's handler
    /// registry that a discovered peer joins. Unset without a discovery
    /// backend, and then a miss fails as it always has.
    peer_discovery: OnceLock<(Arc<dyn PeerDiscovery>, Arc<PeerRegistry>)>,
    /// One permit per discovery lookup in flight. See
    /// [`MAX_CONCURRENT_LOOKUPS`].
    lookup_permits: tokio::sync::Semaphore,
    /// One place per miss that runs or waits for a lookup. See
    /// [`MAX_PENDING_LOOKUPS`].
    lookup_places: tokio::sync::Semaphore,
    /// Ends every lookup. A child of the teardown token, cancelled by
    /// `request_teardown` itself: the teardown token is cancelled by the
    /// teardown worker, after the transports' `begin_drain` hooks, and a hook
    /// that takes its time would leave lookups registering peers meanwhile.
    lookup_cancel: tokio_util::sync::CancellationToken,
    /// See [`VeloBuilder::reply_lookup_timeout`](crate::VeloBuilder::reply_lookup_timeout).
    reply_lookup_timeout: OnceLock<std::time::Duration>,
}

/// A frame [`VeloBackend::send_routed`] found no route for, handed back
/// unsent.
struct Unsent {
    header: Bytes,
    payload: Bytes,
    on_error: Arc<dyn TransportErrorHandler>,
}

/// Wait until the frame is on its send channel.
async fn admitted(outcome: SendOutcome) -> anyhow::Result<()> {
    if let SendOutcome::Pending(admission) = outcome {
        admission.await?;
    }
    Ok(())
}

/// Told when the backend's peer table changes.
///
/// The backend's table is the one every way of learning a peer goes
/// through: `Velo::register_peer`, `Messenger::register_peer`, `_hello`,
/// and discovery when a send names an unknown worker. `VeloBuilder::build`
/// installs one observer that keeps the per-stream transport's own peer
/// table in step, so that no path can fill one table and not the other.
///
/// The backend calls it while it holds the peer's `primary_transport`
/// entry, so that a register and an unregister of one peer reach it in the
/// order they change the route. It must not block, and it must not call
/// back into the backend.
pub(crate) trait PeerTableObserver: Send + Sync {
    /// The peer was added: at least one messenger transport accepted it.
    /// Infallible, because the messenger's registration stands either way;
    /// an observer logs what it could not do.
    fn registered(&self, peer: &PeerInfo);

    /// The peer was dropped. Called for every `unregister_peer`, also for a
    /// peer never registered, so it must be cheap and idempotent.
    fn unregistered(&self, instance_id: InstanceId);
}

/// Stop completed transports if construction fails or is cancelled.
struct StartupTransports {
    transports: HashMap<TransportKey, Arc<dyn Transport>>,
    shutdown: Option<ShutdownState>,
}

impl StartupTransports {
    fn stop(&mut self) -> Result<(), Arc<str>> {
        let Some(shutdown) = self.shutdown.take() else {
            return Ok(());
        };
        // Construction has no caller to drain work for. Use a zero budget.
        stop_transports(&shutdown, &self.transports)
    }

    fn finish(mut self) -> HashMap<TransportKey, Arc<dyn Transport>> {
        self.shutdown = None;
        std::mem::take(&mut self.transports)
    }
}

/// Gate, tear down, and shut down every transport. Both `begin_drain` calls
/// are idempotent, so this is safe after a graceful drain already ran.
///
/// Teardown runs once, so a panicking hook must not skip the hooks after it:
/// a skipped hook would leave its threads and memory for the life of the
/// process. Each hook runs in its own `catch_unwind`; the first panic is
/// returned after all of them have run.
fn stop_transports(
    state: &ShutdownState,
    transports: &HashMap<TransportKey, Arc<dyn Transport>>,
) -> Result<(), Arc<str>> {
    let mut failure = None;
    // Logged here, so every path that runs the hooks reports the same way.
    let mut run = |key: &TransportKey, hook: &dyn Fn()| {
        if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(hook)) {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("transport shutdown hook panicked");
            tracing::error!(transport = %key.as_str(), error = message, "Transport teardown failed");
            failure.get_or_insert_with(|| Arc::<str>::from(message));
        }
    };
    state.begin_drain();
    for (key, transport) in transports {
        run(key, &|| transport.begin_drain());
    }
    state.teardown_token().cancel();
    for (key, transport) in transports {
        run(key, &|| transport.shutdown());
    }
    failure.map_or(Ok(()), Err)
}

impl Drop for StartupTransports {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl VeloBackend {
    /// Create a new backend from a list of transports.
    ///
    /// Each transport is started (bound, listening) and its address is merged
    /// into a composite [`WorkerAddress`]. Returns the backend and the
    /// [`DataStreams`] receivers for inbound messages.
    pub async fn new(
        backend_transports: Vec<Arc<dyn Transport>>,
        observability: Option<Arc<VeloMetrics>>,
    ) -> anyhow::Result<(Self, DataStreams)> {
        let instance_id = InstanceId::new_v4();

        // build worker address
        let mut priorities = Vec::new();
        let mut builder = WorkerAddressBuilder::new();
        let mut transport_metrics = HashMap::new();

        let (adapter, data_streams) = transport::make_channels();
        let shutdown_state = adapter.shutdown_state.clone();
        let mut started = StartupTransports {
            transports: HashMap::new(),
            shutdown: Some(shutdown_state.clone()),
        };

        let runtime = tokio::runtime::Handle::current();

        let startup = async {
            for transport in backend_transports {
                let key = transport.key();
                anyhow::ensure!(
                    !started.transports.contains_key(&key),
                    "Duplicate transport key: {key}"
                );
                if let Some(metrics) = observability.as_ref() {
                    let handle = Arc::new(metrics.bind_transport(key.as_str()));
                    transport.set_observability(
                        handle.clone() as Arc<dyn velo_ext::TransportObservability>
                    );
                    transport_metrics.insert(key.clone(), handle);
                }
                transport
                    .start(instance_id, adapter.clone(), runtime.clone())
                    .await?;
                started
                    .transports
                    .insert(key.clone(), Arc::clone(&transport));
                builder.merge(&transport.address())?;
                priorities.push(key.clone());
            }
            Ok::<_, anyhow::Error>(builder.build()?)
        }
        .await;
        let address = match startup {
            Ok(address) => address,
            Err(error) => {
                // A hook that panicked may never set up what its `closed`
                // waits on, so wait only after a clean teardown, as
                // `finish_shutdown` does.
                if started.stop().is_ok() {
                    futures::future::join_all(started.transports.values().map(|t| t.closed()))
                        .await;
                }
                return Err(error);
            }
        };

        Ok((
            Self {
                instance_id,
                address,
                transports: started.finish(),
                transport_metrics,
                priorities: Mutex::new(priorities),
                primary_transport: DashMap::new(),
                alternative_transports: DashMap::new(),
                workers: DashMap::new(),
                lookup_cancel: shutdown_state.teardown_token().child_token(),
                shutdown_state,
                teardown: OnceLock::new(),
                teardown_outcome: Default::default(),
                runtime,
                observability,
                peers: DashMap::new(),
                peer_observer: OnceLock::new(),
                advertised: OnceLock::new(),
                peer_discovery: OnceLock::new(),
                lookup_permits: tokio::sync::Semaphore::new(MAX_CONCURRENT_LOOKUPS),
                lookup_places: tokio::sync::Semaphore::new(
                    MAX_CONCURRENT_LOOKUPS + MAX_PENDING_LOOKUPS,
                ),
                reply_lookup_timeout: OnceLock::new(),
            },
            data_streams,
        ))
    }

    /// The metrics this backend was built with. Borrowed, so a reader on a
    /// per-message path pays for no reference count unless it keeps one.
    pub(crate) fn observability(&self) -> Option<&Arc<VeloMetrics>> {
        self.observability.as_ref()
    }

    /// Returns this backend's unique instance identifier.
    pub fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    /// Returns a [`PeerInfo`] describing this backend (instance ID + composite address).
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo::new(self.instance_id, self.address.clone())
    }

    /// What this node sends to a peer in `_hello`: its complete address.
    ///
    /// [`peer_info`](Self::peer_info) has the messenger transports only. A
    /// `Velo` node also listens for streams on an endpoint of its own, and a
    /// peer that learns the node by `_hello` must get that endpoint too, or
    /// it cannot attach to the node's anchors. `VeloBuilder::build` sets the
    /// complete address; a plain messenger has nothing to add.
    pub(crate) fn advertised_peer_info(&self) -> PeerInfo {
        match self.advertised.get() {
            Some(peer_info) => peer_info.clone(),
            None => self.peer_info(),
        }
    }

    /// Set the address [`advertised_peer_info`](Self::advertised_peer_info)
    /// returns. Once only: a node's address does not change.
    pub(crate) fn set_advertised_peer_info(&self, peer_info: PeerInfo) -> anyhow::Result<()> {
        anyhow::ensure!(
            peer_info.instance_id() == self.instance_id,
            "advertised address belongs to another instance"
        );
        self.advertised
            .set(peer_info)
            .map_err(|_| anyhow::anyhow!("advertised address already set"))
    }

    /// Install the one [`PeerTableObserver`]. Fails if one is installed.
    ///
    /// The observer first hears of every peer already registered: the
    /// transports start before `VeloBuilder::build` can install it, and a
    /// `_hello` can register a peer in between.
    pub(crate) fn set_peer_observer(
        &self,
        observer: Arc<dyn PeerTableObserver>,
    ) -> anyhow::Result<()> {
        self.peer_observer
            .set(Arc::clone(&observer))
            .map_err(|_| anyhow::anyhow!("a peer table observer is already installed"))?;
        // Installed first, so a register from here on tells it, and one that
        // the walk also finds is told twice, which is one register. The walk
        // holds each shard while it calls, so a register or an unregister of
        // that peer comes after it.
        for peer in self.peers.iter() {
            observer.registered(peer.value());
        }
        Ok(())
    }

    /// Returns `true` if the given instance has been registered via [`register_peer`](Self::register_peer).
    pub fn is_registered(&self, instance_id: InstanceId) -> bool {
        self.primary_transport.contains_key(&instance_id)
    }

    /// Fast-path lookup of worker_id -> instance_id from cache.
    ///
    /// Returns `WorkerNotRegistered` if the worker is not in the cache.
    /// Higher layers (Velo, VeloEvents, ActiveMessageClient) should handle
    /// discovery fallback when this returns an error.
    ///
    /// # Example
    /// ```ignore
    /// match backend.try_translate_worker_id(worker_id) {
    ///     Ok(instance_id) => { /* fast path: send immediately */ }
    ///     Err(VeloBackendError::WorkerNotRegistered(_)) => {
    ///         /* slow path: query discovery, then register_peer() */
    ///     }
    /// }
    /// ```
    pub fn try_translate_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> Result<InstanceId, VeloBackendError> {
        self.workers
            .get(&worker_id)
            .map(|entry| *entry)
            .ok_or(VeloBackendError::WorkerNotRegistered(worker_id))
    }

    /// Returns the [`TransportKey`] of the primary transport selected for `target`,
    /// or `None` if the peer has not been registered. For the RDMA path's
    /// eligibility check, and for tests.
    #[cfg(any(
        test,
        feature = "test-helpers",
        all(target_os = "linux", feature = "ucx")
    ))]
    #[doc(hidden)]
    pub fn primary_transport_key(&self, target: InstanceId) -> Option<TransportKey> {
        self.primary_transport
            .get(&target)
            .map(|entry| entry.value().key())
    }

    /// Probe the selected transport without changing its priority or fallback.
    pub(crate) async fn check_peer_health(
        &self,
        target: InstanceId,
        timeout: std::time::Duration,
    ) -> Result<(), transport::HealthCheckError> {
        let transport = self
            .primary_transport
            .get(&target)
            .map(|entry| entry.value().clone())
            .ok_or(transport::HealthCheckError::PeerNotRegistered)?;
        transport.check_health(target, timeout).await
    }

    /// Largest `header + payload` the peer's primary transport will carry to
    /// `target` in one send, or `None` when that cannot be established.
    ///
    /// `None` covers two cases that are the same answer to a caller: the
    /// transport does not know its own limit, and `target` was never
    /// registered so there is no transport to ask. Either way there is no
    /// capacity to plan against and the caller falls back to its own
    /// conservative budget.
    ///
    /// Reported for the *primary* transport only, the one every send in the
    /// library goes through. An alternative transport can have a different
    /// limit.
    pub(crate) fn max_message_size(&self, target: InstanceId) -> Option<usize> {
        self.primary_transport
            .get(&target)
            .and_then(|transport| transport.value().max_message_size(target))
    }

    /// Returns the ordered list of alternative [`TransportKey`]s for `target`,
    /// or `None` if the peer has not been registered.
    #[cfg(any(test, all(target_os = "linux", feature = "ucx")))]
    pub fn alternative_transport_keys(&self, target: InstanceId) -> Option<Vec<TransportKey>> {
        self.alternative_transports
            .get(&target)
            .map(|entry| entry.value().clone())
    }

    /// [`send_message_on_lane`](Self::send_message_on_lane) on lane 0, for
    /// the backend's tests.
    #[cfg(test)]
    pub fn send_message(
        &self,
        target: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> anyhow::Result<SendOutcome> {
        self.send_message_on_lane(target, 0, header, payload, message_type, on_error)
    }

    /// How many lanes the primary transport to `target` keeps. See
    /// [`Transport::lanes`].
    ///
    /// Returns [`VeloBackendError::InstanceNotRegistered`] if the peer has not
    /// been registered.
    pub fn lanes(&self, target: InstanceId) -> anyhow::Result<std::num::NonZeroU16> {
        let transport = self
            .primary_transport
            .get(&target)
            .ok_or(VeloBackendError::InstanceNotRegistered(target))?;
        Ok(transport.value().lanes(target))
    }

    /// The most lanes any installed transport keeps, for a choice made before
    /// the peer is known.
    ///
    /// Asks each transport about this node's own instance, because there is
    /// no peer to name. That is right only while `lanes()` ignores its target,
    /// which every in-tree transport does today; one that answered per peer
    /// would need a peer here.
    pub(crate) fn max_lanes(&self) -> std::num::NonZeroU16 {
        self.transports
            .values()
            .map(|transport| transport.lanes(self.instance_id))
            .max()
            .unwrap_or(std::num::NonZeroU16::MIN)
    }

    /// Send a message to a registered peer on one of its primary transport's
    /// lanes.
    ///
    /// Frames sent on one `(target, lane)` arrive in order, and nothing is
    /// ordered across lanes. So `Message` frames for an ordered handler must
    /// stay on one lane: the messenger's own traffic uses lane 0. See
    /// [`Transport::send_message_on_lane`] for the contract.
    ///
    /// Returns [`VeloBackendError::InstanceNotRegistered`] if the peer has not
    /// been registered with [`register_peer`](Self::register_peer).
    ///
    /// The [`SendOutcome`] distinguishes synchronous admission
    /// ([`SendOutcome::Admitted`]) from a saturated channel for the lane
    /// ([`SendOutcome::Pending`]), where the frame is queued behind its
    /// predecessors and the contained [`SendAdmission`] reports when it lands.
    pub fn send_message_on_lane(
        &self,
        target: InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> anyhow::Result<SendOutcome> {
        self.send_routed(target, lane, header, payload, message_type, on_error)
            .map_err(|_| VeloBackendError::InstanceNotRegistered(target).into())
    }

    /// [`send_message_on_lane`](Self::send_message_on_lane), handing the frame
    /// back when `target` has no route. Nothing was sent then, so a caller
    /// that can find the peer again may send it once more.
    fn send_routed(
        &self,
        target: InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<SendOutcome, Unsent> {
        let Some(transport) = self.primary_transport.get(&target) else {
            return Err(Unsent {
                header,
                payload,
                on_error,
            });
        };
        let transport_key = transport.value().key();
        // Only the span below needs this; `finalize_send_outcome` recomputes it
        // from the report's own frame handles.
        #[cfg(feature = "distributed-tracing")]
        let bytes = header.len() + payload.len();
        let metrics = self.transport_metrics.get(&transport_key);

        let error_handler = instrument_transport_error_handler(metrics.cloned(), on_error);
        let report = SendReport {
            metrics: metrics.cloned(),
            on_error: error_handler.clone(),
            message_type,
            header: header.clone(),
            payload: payload.clone(),
        };

        #[cfg(feature = "distributed-tracing")]
        let outcome = {
            let span = tracing::info_span!(
                "velo.transport.send",
                transport = transport_key.as_str(),
                message_type = message_type_label(message_type),
                bytes
            );
            let _entered = span.enter();
            transport.send_message_on_lane(
                target,
                lane,
                header,
                payload,
                message_type,
                error_handler,
            )
        };

        #[cfg(not(feature = "distributed-tracing"))]
        let outcome = transport.send_message_on_lane(
            target,
            lane,
            header,
            payload,
            message_type,
            error_handler,
        );

        Ok(finalize_send_outcome(outcome, report))
    }

    /// Send a reply to the worker it answers, and wait for its admission.
    ///
    /// A table hit sends at once, as a send to a known instance does: no
    /// timer, no task. On a miss the worker is resolved through discovery,
    /// bounded by [`reply_lookup_timeout`](crate::VeloBuilder::reply_lookup_timeout),
    /// then the reply is sent. A route that `unregister_peer` removed after
    /// the worker lookup is a miss too: nothing was sent, so the reply is sent
    /// at most once. Without discovery a miss returns the table's error.
    pub(crate) async fn send_reply(
        &self,
        worker: WorkerId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> anyhow::Result<()> {
        let (unsent, miss) = match self.try_translate_worker_id(worker) {
            Ok(target) => {
                match self.send_routed(target, 0, header, payload, message_type, on_error) {
                    Ok(outcome) => return admitted(outcome).await,
                    Err(unsent) => (unsent, VeloBackendError::InstanceNotRegistered(target)),
                }
            }
            Err(miss) => (
                Unsent {
                    header,
                    payload,
                    on_error,
                },
                miss,
            ),
        };
        // Without discovery a miss fails as it always has: the caller that
        // owes the reply counts and logs it, and nothing else does.
        if !self.has_peer_discovery() {
            return Err(miss.into());
        }
        let bound = self.reply_lookup_timeout();
        // A bound too large to add to the clock leaves the lookup its
        // fallback bound.
        let deadline = tokio::time::Instant::now().checked_add(bound);
        // Boxed, so a hit's future does not carry the lookup's state.
        let target = Box::pin(self.resolve_peer(PeerKey::Worker(worker), deadline)).await?;
        let outcome = self
            .send_routed(
                target,
                0,
                unsent.header,
                unsent.payload,
                message_type,
                unsent.on_error,
            )
            .map_err(|_| VeloBackendError::InstanceNotRegistered(target))?;
        admitted(outcome).await
    }

    /// Resolve a peer the table missed through discovery, giving up at
    /// `deadline` with [`LookupTimedOut`]. With no deadline the lookup gives
    /// up after [`UNTIMED_LOOKUP_TIMEOUT`].
    ///
    /// Each miss runs its own lookup, in the caller's future: dropping the
    /// future ends the lookup, and nothing is shared or left behind. The
    /// backend's teardown ends it too, so a detached send does not hold a
    /// lookup past its node. Replies and sends both resolve here, so this is
    /// the one place a miss is counted and logged.
    pub(crate) async fn resolve_peer(
        &self,
        key: PeerKey,
        deadline: Option<tokio::time::Instant>,
    ) -> anyhow::Result<InstanceId> {
        let deadline =
            deadline.unwrap_or_else(|| tokio::time::Instant::now() + UNTIMED_LOOKUP_TIMEOUT);
        let result = tokio::select! {
            biased;
            () = self.lookup_cancel.cancelled() => Err(anyhow::anyhow!(
                "the node is shutting down; discovery for {key} was cancelled"
            )),
            found = tokio::time::timeout_at(deadline, self.discover(key, deadline)) => {
                found.unwrap_or_else(|_| Err(LookupTimedOut(key).into()))
            }
        };
        if let Some(metrics) = self.observability.as_ref() {
            metrics.record_client_resolution(match result {
                Ok(_) => ClientResolution::DiscoverySuccess,
                Err(_) => ClientResolution::DiscoveryError,
            });
        }
        if let Err(error) = &result {
            tracing::error!(target: "crate::transports", peer = %key, error = %error, "Discovery failed");
        }
        result
    }

    /// One lookup: wait for a permit, ask discovery, check the answer, and
    /// register the peer in both tables `Messenger::register_peer` fills.
    ///
    /// `timeout_at` polls this future before its timer, so a step that ends
    /// at a late poll would pass as in time. The caller has given up by then.
    /// So the deadline is read again after each wait, before anything is
    /// returned or registered.
    async fn discover(
        &self,
        key: PeerKey,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<InstanceId> {
        let Some((discovery, registry)) = self.peer_discovery.get() else {
            anyhow::bail!("No discovery backend configured. Cannot resolve {key}");
        };
        let timed_out = || tokio::time::Instant::now() >= deadline;
        let teardown = &self.lookup_cancel;
        let Ok(_place) = self.lookup_places.try_acquire() else {
            anyhow::bail!(
                "{} discovery lookups are running or pending; not starting one for {key}",
                MAX_CONCURRENT_LOOKUPS + MAX_PENDING_LOOKUPS
            );
        };
        let _permit = self.lookup_permits.acquire().await?;
        if timed_out() {
            return Err(LookupTimedOut(key).into());
        }
        // The table may hold the peer by now: an earlier miss of the same
        // burst, a `_hello` or a `register_peer` can land while this one
        // waits. Then discovery has nothing to add, and may hold no record.
        if let Some(instance) = key.registered_in(self) {
            return Ok(instance);
        }
        let found = match key {
            PeerKey::Worker(worker) => discovery.discover_by_worker_id(worker).await,
            PeerKey::Instance(instance) => discovery.discover_by_instance_id(instance).await,
        };
        // Before the answer is read: a refusal that comes late is a timeout.
        if timed_out() {
            return Err(LookupTimedOut(key).into());
        }
        let peer =
            found.map_err(|error| anyhow::anyhow!("Discovery failed for {key}: {error:#}"))?;
        let instance = peer.instance_id();
        // Discovery is trusted for which instance holds a worker id, but an
        // answer for another id would deliver one peer's frame to another.
        anyhow::ensure!(
            key.names(instance),
            "Discovery answered {key} with {instance}"
        );
        // No route goes into a node that is tearing down. Teardown can also
        // begin during the registration, which no lock covers; then the peer
        // is taken out again before anything is sent to it.
        anyhow::ensure!(
            !teardown.is_cancelled(),
            "the node is shutting down; not registering {instance}"
        );
        self.register_peer(peer).map_err(|error| {
            anyhow::anyhow!("Could not register discovered {instance}: {error}")
        })?;
        registry.register_peer(instance);
        if teardown.is_cancelled() {
            self.unregister_peer(instance);
            registry.unregister_peer(instance);
            anyhow::bail!("the node is shutting down; not registering {instance}");
        }
        // A transport can take its time to register. The peer stays, but the
        // caller has given up: nothing is sent for it.
        if timed_out() {
            return Err(LookupTimedOut(key).into());
        }
        Ok(instance)
    }

    /// Misses that run or wait for a lookup now.
    #[cfg(test)]
    pub(crate) fn lookups_held(&self) -> usize {
        MAX_CONCURRENT_LOOKUPS + MAX_PENDING_LOOKUPS - self.lookup_places.available_permits()
    }

    /// Whether a discovery backend is configured.
    pub(crate) fn has_peer_discovery(&self) -> bool {
        self.peer_discovery.get().is_some()
    }

    /// Install discovery and the client's handler registry. Once, by the
    /// messenger's client.
    pub(crate) fn set_peer_discovery(
        &self,
        discovery: Arc<dyn PeerDiscovery>,
        registry: Arc<PeerRegistry>,
    ) {
        let _ = self.peer_discovery.set((discovery, registry));
    }

    /// The bound on a reply's discovery lookup.
    pub(crate) fn reply_lookup_timeout(&self) -> std::time::Duration {
        self.reply_lookup_timeout
            .get()
            .copied()
            .unwrap_or(DEFAULT_REPLY_LOOKUP_TIMEOUT)
    }

    /// Set the bound on a reply's discovery lookup, once.
    pub(crate) fn set_reply_lookup_timeout(&self, timeout: std::time::Duration) {
        let _ = self.reply_lookup_timeout.set(timeout);
    }

    /// Register a remote peer with all compatible transports.
    ///
    /// The highest-priority compatible transport becomes the peer's *primary*.
    /// Returns [`VeloBackendError::NoCompatibleTransports`] if no transport
    /// can accept the peer's address.
    pub fn register_peer(&self, peer: PeerInfo) -> Result<(), VeloBackendError> {
        let instance_id = peer.instance_id();
        let mut compatible_transports = Vec::new();
        let mut failures = Vec::new();
        for (key, transport) in self.transports.iter() {
            match transport.register(peer.clone()) {
                Ok(()) => compatible_transports.push(key.clone()),
                Err(error) => failures.push((key, error)),
            }
        }
        if compatible_transports.is_empty() {
            tracing::warn!(peer = %instance_id, ?failures, "No transport could register peer");
            return Err(VeloBackendError::NoCompatibleTransports);
        }

        // sort against the preferred transports
        let sorted_transports = self
            .priorities
            .lock()
            .iter()
            .filter(|key| compatible_transports.contains(key))
            .cloned()
            .collect::<Vec<TransportKey>>();

        assert!(
            !sorted_transports.is_empty(),
            "failed to properly sort compatible transports"
        );

        let primary_transport_key = sorted_transports[0].clone();
        let alternative_transport_keys = sorted_transports[1..].to_vec();
        tracing::debug!(peer = %instance_id, transport = %primary_transport_key, "Registered peer");

        let primary_transport = self.transports.get(&primary_transport_key).unwrap();

        // The route's entry orders this against an unregister of the same
        // peer, so the observer's table ends as the route does. No lock of
        // the backend's own: a send's error handler can register a peer.
        let route = self
            .primary_transport
            .entry(instance_id)
            .insert(primary_transport.clone());
        self.peers.insert(instance_id, peer.clone());
        if let Some(observer) = self.peer_observer.get() {
            observer.registered(&peer);
        }
        drop(route);
        self.alternative_transports
            .insert(instance_id, alternative_transport_keys);
        self.workers.insert(instance_id.worker_id(), instance_id);
        Ok(())
    }

    /// Forget a peer that [`register_peer`](Self::register_peer) added.
    ///
    /// Drops the peer's routes, then has every transport drop its state for
    /// the peer and close its connections (see [`Transport::unregister`]).
    /// A send to it then fails with [`VeloBackendError::InstanceNotRegistered`].
    /// Idempotent; other peers are not affected.
    pub fn unregister_peer(&self, instance_id: InstanceId) {
        // Routes first, so a new send fails here and does not reach a
        // transport that is closing the peer's connections. The observer is
        // told under the route's entry, as `register_peer` tells it.
        let route = self.primary_transport.entry(instance_id);
        self.peers.remove(&instance_id);
        if let Some(observer) = self.peer_observer.get() {
            observer.unregistered(instance_id);
        }
        // A vacant entry holds the shard too: drop it here, not at return.
        match route {
            dashmap::Entry::Occupied(route) => drop(route.remove()),
            dashmap::Entry::Vacant(route) => drop(route),
        }
        self.alternative_transports.remove(&instance_id);
        self.workers
            .remove_if(&instance_id.worker_id(), |_, id| *id == instance_id);
        // A transport fails the peer's queued sends here and runs their
        // error handlers, so it runs under no entry.
        // Every transport, not only the routes: a re-registration that one
        // transport refused leaves that transport out of the routes, but it
        // can still hold the peer from before.
        for transport in self.transports.values() {
            transport.unregister(instance_id);
        }
        tracing::debug!(peer = %instance_id, "Unregistered peer");
    }

    /// `unregister_peer` stopped after its first step: the route is gone and
    /// the worker entry is not.
    #[cfg(test)]
    pub(crate) fn drop_route(&self, instance_id: InstanceId) {
        self.primary_transport.remove(&instance_id);
    }

    /// Get the available transports.
    #[cfg(test)]
    pub fn available_transports(&self) -> Vec<TransportKey> {
        self.transports.keys().cloned().collect()
    }

    /// Set the order in which `register_peer` picks a peer's primary
    /// transport. Every available transport must occur exactly once.
    ///
    /// Test-only: the library keeps the order the transports were added in.
    #[cfg(test)]
    pub(crate) fn set_transport_priority(&self, priorities: Vec<TransportKey>) {
        let mut available = self.available_transports();
        let mut given = priorities.clone();
        available.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        given.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(
            given, available,
            "a priority list names each available transport once"
        );
        *self.priorities.lock() = priorities;
    }

    /// Get the shared shutdown state.
    pub fn shutdown_state(&self) -> &ShutdownState {
        &self.shutdown_state
    }

    /// Begin Phase 1 (Gate) of graceful shutdown: flip the shared drain flag
    /// and notify each transport via `begin_drain()`.
    ///
    /// Listeners then reject new `Message` frames with ShuttingDown
    /// correlation replies while responses, acks, and events keep flowing.
    /// Idempotent. Phase 2 is [`drain`](Self::drain)'s job, and phases 3–4
    /// are [`finish_shutdown`](Self::finish_shutdown)'s; calling this alone
    /// leaves the instance serving in-flight work indefinitely.
    pub fn begin_drain(&self) {
        self.shutdown_state.begin_drain();
        for transport in self.transports.values() {
            transport.begin_drain();
        }
    }

    /// Request transport cleanup without waiting for native thread joins.
    pub(crate) fn shutdown_now(&self) {
        // The worker starts immediately; polling the completion is not required.
        drop(self.request_teardown());
    }

    /// The error of a teardown that has already run and failed. Teardown runs
    /// once, so the failure is final.
    ///
    /// Reads the result the teardown wrote, not the completion: `Shared::peek`
    /// sees only a result some waiter already took, and polling a clone can
    /// say "not yet" for a finished teardown when the task has spent its
    /// cooperative budget. Either way a retry would drain again.
    pub(crate) fn teardown_failure(&self) -> Option<Arc<str>> {
        self.teardown_outcome.get()?.clone().err()
    }

    /// Whether this backend has started its own teardown. Not the shared
    /// teardown token, which an out-of-tree transport may cancel itself.
    pub(crate) fn teardown_requested(&self) -> bool {
        self.teardown.get().is_some()
    }

    pub(crate) fn request_teardown(&self) -> teardown::Completion {
        self.shutdown_state.begin_drain();
        // Here, not on the teardown worker: no lookup registers a peer once
        // this has returned, whatever the transports' hooks then do.
        self.lookup_cancel.cancel();
        self.teardown
            .get_or_init(|| {
                teardown::start(
                    self.shutdown_state.clone(),
                    self.transports.clone(),
                    self.runtime.clone(),
                    Arc::clone(&self.teardown_outcome),
                )
            })
            .clone()
    }

    /// Perform a graceful 4-phase shutdown.
    ///
    /// 1. **Gate**: Flip the draining flag and notify each transport via `begin_drain()`.
    /// 2. **Drain**: Wait for all in-flight requests to complete (per `policy`).
    /// 3. **Teardown**: Cancel the teardown token and call `shutdown()` on each transport.
    /// 4. **Close**: Await each transport's `closed()`, so what it wrote reaches the peer
    ///    before this returns.
    ///
    /// # Panics
    ///
    /// Panics if a transport's shutdown hook panicked. The other hooks still
    /// ran, but shutdown cannot report the instance as stopped. A later call
    /// panics at once, without draining again.
    ///
    /// Test-only: `Velo::shutdown` runs these phases itself, to join the mux
    /// senders between drain and teardown.
    #[cfg(test)]
    pub async fn graceful_shutdown(&self, policy: ShutdownPolicy) {
        // A failed teardown is final: draining again would only spend the
        // caller's budget against torn-down transports.
        if let Some(error) = self.teardown_failure() {
            panic!("transport teardown failed: {error}");
        }
        self.drain(policy).await;
        self.finish_shutdown().await;
    }

    /// Gate and drain while transports still serve accepted work.
    pub(crate) async fn drain(&self, policy: ShutdownPolicy) {
        // Phase 1: Gate
        self.begin_drain();

        // Phase 2: Drain
        match policy {
            ShutdownPolicy::WaitForever => {
                self.shutdown_state.wait_for_drain().await;
            }
            ShutdownPolicy::Timeout(duration) => {
                let _ = tokio::time::timeout(duration, self.shutdown_state.wait_for_drain()).await;
            }
        }
    }

    /// Tear down after services that send through these transports have stopped.
    pub(crate) async fn finish_shutdown(&self) {
        // Phase 3: All callers wait for the same hooks before inspecting close.
        if let Err(error) = self.request_teardown().await {
            // Returning success would let Velo report RDMA memory as released.
            panic!("transport teardown failed: {error}");
        }

        // Phase 4: Wait for each transport's close to finish on the wire, so a
        // process that exits right after this returns does not discard frames
        // a transport keeps in user space (QUIC). Each transport bounds its
        // own wait; the default returns at once.
        futures::future::join_all(self.transports.values().map(|t| t.closed())).await;
    }
}

pub(crate) fn message_type_label(message_type: MessageType) -> &'static str {
    match message_type {
        MessageType::Message => "message",
        MessageType::Response => "response",
        MessageType::Ack => "ack",
        MessageType::Event => "event",
        MessageType::ShuttingDown => "shutting_down",
    }
}

struct InstrumentedTransportErrorHandler {
    metrics: Arc<crate::observability::TransportMetricsHandle>,
    inner: Arc<dyn TransportErrorHandler>,
}

impl TransportErrorHandler for InstrumentedTransportErrorHandler {
    fn on_error(&self, header: Bytes, payload: Bytes, error: String) {
        self.metrics.record_rejection(TransportRejection::SendError);
        self.inner.on_error(header, payload, error);
    }
}

fn instrument_transport_error_handler(
    metrics: Option<Arc<crate::observability::TransportMetricsHandle>>,
    inner: Arc<dyn TransportErrorHandler>,
) -> Arc<dyn TransportErrorHandler> {
    match metrics {
        Some(metrics) => Arc::new(InstrumentedTransportErrorHandler { metrics, inner }),
        None => inner,
    }
}

/// What [`finalize_send_outcome`] needs to close the loop on one send.
///
/// The frame handles are `Bytes` clones taken before the transport consumed
/// them — two refcount bumps, so that an admission that fails can still hand
/// the original frame to `on_error` the way a wire failure does.
struct SendReport {
    metrics: Option<Arc<crate::observability::TransportMetricsHandle>>,
    on_error: Arc<dyn TransportErrorHandler>,
    message_type: MessageType,
    header: Bytes,
    payload: Bytes,
}

/// Attach the backend's bookkeeping to a transport's [`SendOutcome`].
///
/// The outbound-frame metric must count frames that reached the send channel,
/// not frames that were offered, so:
///
/// - [`SendOutcome::Admitted`] records immediately — the frame is on the
///   channel by the time the transport returned.
/// - [`SendOutcome::Pending`] records from a completion hook, and only if the
///   admission resolves `Ok`. A hook rather than a wrapper future because
///   fire-and-forget senders drop the admission unpolled; the frame is still
///   delivered, so the metric still has to fire.
///
/// A failed admission is a frame that never reached the wire, which is what
/// `on_error` reports, so it is routed there — through the same instrumented
/// handler the transport was given, so the rejection counter sees it too.
fn finalize_send_outcome(outcome: SendOutcome, report: SendReport) -> SendOutcome {
    let SendReport {
        metrics,
        on_error,
        message_type,
        header,
        payload,
    } = report;
    let label = message_type_label(message_type);
    let bytes = header.len() + payload.len();

    match outcome {
        SendOutcome::Admitted => {
            if let Some(metrics) = metrics {
                metrics.record_frame(Direction::Outbound, label, bytes);
            }
            SendOutcome::Admitted
        }
        SendOutcome::Pending(admission) => {
            SendOutcome::Pending(admission.on_resolved(move |result| match result {
                Ok(()) => {
                    if let Some(metrics) = metrics {
                        metrics.record_frame(Direction::Outbound, label, bytes);
                    }
                }
                Err(error) => {
                    on_error.on_error(header, payload, format!("Send not admitted: {error}"));
                }
            }))
        }
    }
}

#[cfg(test)]
mod tests;
