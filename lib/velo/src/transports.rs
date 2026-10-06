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
//! [`VeloBackend`] is the central orchestrator. It holds a set of transports,
//! each identified by a [`TransportKey`]. When a peer registers, the backend
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

use crate::observability::{Direction, TransportRejection, VeloMetrics};
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
pub use utils::interfaces::{InterfaceEndpoint, InterfaceFilter};

// Trait surface — fully defined in `velo-ext`. Re-exported here as a
// convenience for callers reaching into `velo::transports::*` for transport
// orchestration, but `velo_ext::*` is the canonical source.
pub use transport::{
    AdmissionError, AdmissionGate, AdmissionState, AdmitOutcome, DataStreams, HealthCheckError,
    InFlightGuard, InboundMessage, MessageType, SendAdmission, SendOutcome, ShutdownPolicy,
    ShutdownState, Transport, TransportAdapter, TransportError, TransportErrorHandler,
    make_channels,
};

/// Errors returned by [`VeloBackend`] operations.
#[derive(Debug, thiserror::Error)]
pub enum VeloBackendError {
    /// No transport could accept the peer's address.
    #[error("No compatible transports found")]
    NoCompatibleTransports,

    /// The target instance was never registered via [`VeloBackend::register_peer`].
    #[error("Transport not found for instance: {0}")]
    InstanceNotRegistered(InstanceId),

    /// The worker ID is not in the fast-path cache.
    #[error("Worker not found: {0}")]
    WorkerNotRegistered(WorkerId),

    /// The requested [`TransportKey`] does not match any loaded transport.
    #[error("Transport not found: {0}")]
    TransportNotFound(TransportKey),

    /// The priority list does not match the set of available transports.
    #[error("Invalid transport priority: {0}")]
    InvalidTransportPriority(String),
}

/// Central orchestrator that aggregates multiple transports and routes messages
/// to peers via priority-based transport selection.
///
/// Each peer is registered with all compatible transports; the highest-priority
/// compatible transport becomes the *primary* for that peer. Worker IDs are
/// cached for fast-path routing without discovery lookups.
pub struct VeloBackend {
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
    runtime: tokio::runtime::Handle,
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
                shutdown_state,
                teardown: OnceLock::new(),
                runtime,
            },
            data_streams,
        ))
    }

    /// Returns this backend's unique instance identifier.
    pub fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    /// Returns a [`PeerInfo`] describing this backend (instance ID + composite address).
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo::new(self.instance_id, self.address.clone())
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

    /// Deprecated: Use `try_translate_worker_id()` for explicit fast-path semantics.
    #[deprecated(since = "0.7.0", note = "Use try_translate_worker_id() instead")]
    pub fn translate_worker_id(&self, worker_id: WorkerId) -> Result<InstanceId, VeloBackendError> {
        self.try_translate_worker_id(worker_id)
    }

    /// Check if an instance_id is registered.
    pub fn has_instance(&self, instance_id: InstanceId) -> bool {
        self.primary_transport.contains_key(&instance_id)
    }

    /// Returns the [`TransportKey`] of the primary transport selected for `target`,
    /// or `None` if the peer has not been registered.
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
    /// Reported for the *primary* transport only. An alternative reached
    /// through [`send_message_with_transport`](Self::send_message_with_transport)
    /// can have a different limit; sizing a send against this number and then
    /// routing it elsewhere is the caller's business to avoid.
    pub(crate) fn max_message_size(&self, target: InstanceId) -> Option<usize> {
        self.primary_transport
            .get(&target)
            .and_then(|transport| transport.value().max_message_size(target))
    }

    /// Returns the ordered list of alternative [`TransportKey`]s for `target`,
    /// or `None` if the peer has not been registered.
    pub fn alternative_transport_keys(&self, target: InstanceId) -> Option<Vec<TransportKey>> {
        self.alternative_transports
            .get(&target)
            .map(|entry| entry.value().clone())
    }

    /// Send a message to a registered peer via its primary transport.
    ///
    /// Returns [`VeloBackendError::InstanceNotRegistered`] if the peer has not
    /// been registered with [`register_peer`](Self::register_peer).
    ///
    /// The [`SendOutcome`] distinguishes synchronous admission
    /// ([`SendOutcome::Admitted`]) from a saturated channel for the target's
    /// lane 0
    /// ([`SendOutcome::Pending`]), where the frame is queued behind its
    /// predecessors and the contained [`SendAdmission`] reports when it lands.
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
    /// [`Transport::send_message_on_lane`] for the contract, and
    /// [`send_message`](Self::send_message) for everything else, which is the
    /// same.
    pub fn send_message_on_lane(
        &self,
        target: InstanceId,
        lane: u16,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> anyhow::Result<SendOutcome> {
        let transport = self
            .primary_transport
            .get(&target)
            .ok_or(VeloBackendError::InstanceNotRegistered(target))?;
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

    /// Send a message to a registered peer via a specific transport.
    ///
    /// If `transport_key` matches the peer's primary transport, the message is
    /// sent directly. Otherwise, the alternative transports are searched.
    /// Returns [`VeloBackendError::NoCompatibleTransports`] if the requested
    /// transport is not available for this peer.
    pub fn send_message_with_transport(
        &self,
        target: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
        transport_key: TransportKey,
    ) -> anyhow::Result<SendOutcome> {
        let transport = self
            .primary_transport
            .get(&target)
            .ok_or(VeloBackendError::InstanceNotRegistered(target))?;

        if transport.value().key() == transport_key {
            let metrics = self.transport_metrics.get(&transport_key);

            let error_handler = instrument_transport_error_handler(metrics.cloned(), on_error);
            let report = SendReport {
                metrics: metrics.cloned(),
                on_error: error_handler.clone(),
                message_type,
                header: header.clone(),
                payload: payload.clone(),
            };
            let outcome =
                transport.send_message(target, header, payload, message_type, error_handler);

            return Ok(finalize_send_outcome(outcome, report));
        } else {
            // if we got here, we can unwrap because there is an entry in the alternative_transports map
            let alternative_transports = self
                .alternative_transports
                .get(&target)
                .ok_or(VeloBackendError::InstanceNotRegistered(target))?;

            for alternative_transport in alternative_transports.iter() {
                if *alternative_transport == transport_key
                    && let Some(transport) = self.transports.get(alternative_transport)
                {
                    let metrics = self.transport_metrics.get(alternative_transport);

                    let error_handler =
                        instrument_transport_error_handler(metrics.cloned(), on_error);
                    let report = SendReport {
                        metrics: metrics.cloned(),
                        on_error: error_handler.clone(),
                        message_type,
                        header: header.clone(),
                        payload: payload.clone(),
                    };
                    let outcome = transport.send_message(
                        target,
                        header,
                        payload,
                        message_type,
                        error_handler,
                    );

                    return Ok(finalize_send_outcome(outcome, report));
                }
            }
        }

        Err(VeloBackendError::NoCompatibleTransports)?
    }

    /// Send message to a worker (fast-path only).
    ///
    /// This method uses `try_translate_worker_id()` for fast-path lookup.
    /// Returns `WorkerNotRegistered` error if the worker is not in the cache.
    ///
    /// For automatic discovery, use the two-phase pattern:
    /// ```ignore
    /// match backend.send_message_to_worker(...) {
    ///     Ok(SendOutcome::Admitted) => { /* already on the send channel */ }
    ///     Ok(SendOutcome::Pending(admission)) => { admission.await?; }
    ///     Err(e) if matches_worker_not_registered(&e) => {
    ///         tokio::spawn(async move {
    ///             let instance_id = backend.resolve_and_register_worker(worker_id).await?;
    ///             if let SendOutcome::Pending(admission) =
    ///                 backend.send_message(instance_id, ...)?
    ///             {
    ///                 admission.await?;
    ///             }
    ///         });
    ///     }
    /// }
    /// ```
    pub fn send_message_to_worker(
        &self,
        worker_id: WorkerId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> anyhow::Result<SendOutcome> {
        let instance_id = self.try_translate_worker_id(worker_id)?;
        self.send_message(instance_id, header, payload, message_type, on_error)
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

        self.primary_transport
            .insert(instance_id, primary_transport.clone());
        self.alternative_transports
            .insert(instance_id, alternative_transport_keys);
        self.workers.insert(instance_id.worker_id(), instance_id);

        Ok(())
    }

    /// Get the available transports.
    pub fn available_transports(&self) -> Vec<TransportKey> {
        self.transports.keys().cloned().collect()
    }

    /// Set the priority of the transports.
    ///
    /// Every available transport must occur exactly once.
    pub fn set_transport_priority(
        &self,
        priorities: Vec<TransportKey>,
    ) -> Result<(), VeloBackendError> {
        let required_transports = self.available_transports();
        if required_transports.len() != priorities.len() {
            return Err(VeloBackendError::InvalidTransportPriority(format!(
                "Required transports: {:?}, provided priorities: {:?}",
                required_transports, priorities
            )));
        }

        for (index, priority) in priorities.iter().enumerate() {
            if !required_transports.contains(priority) {
                return Err(VeloBackendError::InvalidTransportPriority(format!(
                    "Priority transport not found: {:?}",
                    priority
                )));
            }
            if priorities[..index].contains(priority) {
                return Err(VeloBackendError::InvalidTransportPriority(format!(
                    "Duplicate priority transport: {priority}"
                )));
            }
        }

        let mut guard = self.priorities.lock();
        *guard = priorities;
        Ok(())
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
    /// Idempotent. Phases 2–3 are [`graceful_shutdown`](Self::graceful_shutdown)'s
    /// job; calling this alone leaves the instance serving in-flight work
    /// indefinitely.
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

    pub(crate) fn request_teardown(&self) -> teardown::Completion {
        self.shutdown_state.begin_drain();
        self.teardown
            .get_or_init(|| {
                teardown::start(
                    self.shutdown_state.clone(),
                    self.transports.clone(),
                    self.runtime.clone(),
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
    /// ran, but shutdown cannot report the instance as stopped.
    pub async fn graceful_shutdown(&self, policy: ShutdownPolicy) {
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
