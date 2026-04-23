// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs)]

//! Multi-transport active message routing framework.
//!
//! `velo-transports` abstracts TCP, HTTP, NATS, gRPC, and UCX behind a unified
//! [`Transport`] trait with zero-copy [`bytes::Bytes`], fire-and-forget error
//! callbacks, priority-based peer routing, and 3-phase graceful shutdown.
//!
//! # Architecture
//!
//! [`VeloBackend`] is the central orchestrator. It holds a set of transports,
//! each identified by a [`TransportKey`]. When a peer registers, the backend
//! selects a *primary* transport (highest-priority compatible transport) and
//! records any alternatives. Outbound messages are routed through the primary
//! transport by default, or through an explicit alternative.
//!
//! Inbound messages arrive via [`DataStreams`] — three independent channels
//! for messages, responses, and events.
//!
//! # Shutdown
//!
//! Graceful shutdown follows three phases:
//! 1. **Gate** — flip the draining flag; transports reject new inbound requests.
//! 2. **Drain** — wait for all in-flight requests to complete.
//! 3. **Teardown** — cancel listeners/writers and call `shutdown()` on each transport.

mod address;

pub mod tcp;

/// Shared utility functions for transport implementations.
pub mod utils;

#[cfg(unix)]
pub mod uds;

// #[cfg(feature = "ucx")]
// pub mod ucx;

// #[cfg(feature = "http")]
// pub mod http;

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "zmq")]
pub mod zmq;

mod transport;

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use velo_observability::{Direction, TransportRejection, VeloMetrics};

// Public re-exports from velo-common
pub use velo_common::{
    InstanceId, PeerInfo, TransportKey, WorkerAddress, WorkerAddressError, WorkerId,
};

// Internal builder for address construction
use address::WorkerAddressBuilder;

// Re-export interface discovery types
pub use utils::interfaces::{InterfaceEndpoint, InterfaceFilter};

// Re-export transport types
pub use transport::{
    DataStreams, HealthCheckError, InFlightGuard, MessageType, SendBackpressure, SendOutcome,
    ShutdownPolicy, ShutdownState, Transport, TransportAdapter, TransportError,
    TransportErrorHandler, make_channels, try_send_or_backpressure,
};
pub use velo_observability::VeloMetrics as TransportMetrics;

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
    transport_metrics: HashMap<TransportKey, velo_observability::TransportMetricsHandle>,
    primary_transport: DashMap<InstanceId, Arc<dyn Transport>>,
    alternative_transports: DashMap<InstanceId, Vec<TransportKey>>,
    workers: DashMap<WorkerId, InstanceId>,
    shutdown_state: ShutdownState,

    #[allow(dead_code)]
    runtime: tokio::runtime::Handle,
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
        let mut transports = HashMap::new();
        let mut transport_metrics = HashMap::new();

        let (adapter, data_streams) = transport::make_channels();
        let shutdown_state = adapter.shutdown_state.clone();

        let runtime = tokio::runtime::Handle::current();

        for transport in backend_transports {
            let key = transport.key();
            if let Some(metrics) = observability.as_ref() {
                transport.set_observability(metrics.clone());
                transport_metrics.insert(key.clone(), metrics.bind_transport(key.as_str()));
            }
            transport
                .start(instance_id, adapter.clone(), runtime.clone())
                .await?;
            builder.merge(&transport.address())?;
            priorities.push(key.clone());
            transports.insert(key, transport);
        }
        let address = builder.build()?;

        Ok((
            Self {
                instance_id,
                address,
                transports,
                transport_metrics,
                priorities: Mutex::new(priorities),
                primary_transport: DashMap::new(),
                alternative_transports: DashMap::new(),
                workers: DashMap::new(),
                shutdown_state,
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
    /// The inner [`SendOutcome`] distinguishes synchronous enqueue
    /// ([`SendOutcome::Enqueued`]) from saturated channels
    /// ([`SendOutcome::Backpressured`]) where the caller must `.await` the
    /// returned future to complete the send.
    pub fn send_message(
        &self,
        target: InstanceId,
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
        let transport_name = transport_key.to_string();
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = &transport_name;
        let bytes = header.len() + payload.len();
        let metrics = self.transport_metrics.get(&transport_key);

        let error_handler = instrument_transport_error_handler(metrics.cloned(), on_error);

        #[cfg(feature = "distributed-tracing")]
        let send_result = {
            let span = tracing::info_span!(
                "velo.transport.send",
                transport = transport_name.as_str(),
                message_type = message_type_label(message_type),
                bytes
            );
            let _entered = span.enter();
            transport.send_message(target, header, payload, message_type, error_handler)
        };

        #[cfg(not(feature = "distributed-tracing"))]
        let send_result =
            transport.send_message(target, header, payload, message_type, error_handler);

        Ok(finalize_send_outcome(
            send_result,
            metrics.cloned(),
            message_type,
            bytes,
        ))
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
            let _transport_name = transport_key.to_string();
            let bytes = header.len() + payload.len();
            let metrics = self.transport_metrics.get(&transport_key);

            let error_handler = instrument_transport_error_handler(metrics.cloned(), on_error);
            let send_result =
                transport.send_message(target, header, payload, message_type, error_handler);

            return Ok(finalize_send_outcome(
                send_result,
                metrics.cloned(),
                message_type,
                bytes,
            ));
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
                    let _transport_name = alternative_transport.to_string();
                    let bytes = header.len() + payload.len();
                    let metrics = self.transport_metrics.get(alternative_transport);

                    let error_handler =
                        instrument_transport_error_handler(metrics.cloned(), on_error);
                    let send_result = transport.send_message(
                        target,
                        header,
                        payload,
                        message_type,
                        error_handler,
                    );

                    return Ok(finalize_send_outcome(
                        send_result,
                        metrics.cloned(),
                        message_type,
                        bytes,
                    ));
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
    ///     Ok(SendOutcome::Enqueued) => { /* synchronous enqueue */ }
    ///     Ok(SendOutcome::Backpressured(bp)) => { bp.await; }
    ///     Err(e) if matches_worker_not_registered(&e) => {
    ///         tokio::spawn(async move {
    ///             let instance_id = backend.resolve_and_register_worker(worker_id).await?;
    ///             if let SendOutcome::Backpressured(bp) =
    ///                 backend.send_message(instance_id, ...)?
    ///             {
    ///                 bp.await;
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
        // try to register the peer with each transport
        // we must have at least one compatible transport; otherwise, return an error
        let instance_id = peer.instance_id();
        let mut compatible_transports = Vec::new();
        for (key, transport) in self.transports.iter() {
            if transport.register(peer.clone()).is_ok() {
                compatible_transports.push(key.clone());
            }
        }
        if compatible_transports.is_empty() {
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
    /// The list of [`TransportKey`]s must be an order set of the available transports.
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

        for priority in &priorities {
            if !required_transports.contains(priority) {
                return Err(VeloBackendError::InvalidTransportPriority(format!(
                    "Priority transport not found: {:?}",
                    priority
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

    /// Perform a graceful 3-phase shutdown.
    ///
    /// 1. **Gate**: Flip the draining flag and notify each transport via `begin_drain()`.
    /// 2. **Drain**: Wait for all in-flight requests to complete (per `policy`).
    /// 3. **Teardown**: Cancel the teardown token and call `shutdown()` on each transport.
    pub async fn graceful_shutdown(&self, policy: ShutdownPolicy) {
        // Phase 1: Gate
        self.shutdown_state.begin_drain();
        for transport in self.transports.values() {
            transport.begin_drain();
        }

        // Phase 2: Drain
        match policy {
            ShutdownPolicy::WaitForever => {
                self.shutdown_state.wait_for_drain().await;
            }
            ShutdownPolicy::Timeout(duration) => {
                let _ = tokio::time::timeout(duration, self.shutdown_state.wait_for_drain()).await;
            }
        }

        // Phase 3: Teardown
        self.shutdown_state.teardown_token().cancel();
        for transport in self.transports.values() {
            transport.shutdown();
        }
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
    metrics: Option<velo_observability::TransportMetricsHandle>,
    inner: Arc<dyn TransportErrorHandler>,
}

impl TransportErrorHandler for InstrumentedTransportErrorHandler {
    fn on_error(&self, header: Bytes, payload: Bytes, error: String) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.record_rejection(TransportRejection::SendError);
        }
        self.inner.on_error(header, payload, error);
    }
}

fn instrument_transport_error_handler(
    metrics: Option<velo_observability::TransportMetricsHandle>,
    inner: Arc<dyn TransportErrorHandler>,
) -> Arc<dyn TransportErrorHandler> {
    Arc::new(InstrumentedTransportErrorHandler { metrics, inner })
}

/// Turn a transport `send_message` result into a [`SendOutcome`], recording
/// the outbound-frame metric at the moment the frame is actually enqueued.
///
/// - On `Ok(())` (synchronous enqueue) the metric is recorded immediately.
/// - On `Err(bp)` (backpressure) the bp future is wrapped so the metric is
///   recorded only when the caller awaits it to completion. If the caller
///   drops the future without awaiting, the frame was never enqueued and no
///   outbound-frame count is recorded.
fn finalize_send_outcome(
    send_result: Result<(), SendBackpressure>,
    metrics: Option<velo_observability::TransportMetricsHandle>,
    message_type: MessageType,
    bytes: usize,
) -> SendOutcome {
    match send_result {
        Ok(()) => {
            if let Some(metrics) = metrics {
                metrics.record_frame(Direction::Outbound, message_type_label(message_type), bytes);
            }
            SendOutcome::Enqueued
        }
        Err(bp) => {
            let label = message_type_label(message_type);
            SendOutcome::Backpressured(SendBackpressure::new(Box::pin(async move {
                bp.await;
                if let Some(metrics) = metrics {
                    metrics.record_frame(Direction::Outbound, label, bytes);
                }
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Mock transport for testing VeloBackend logic without real networking.
    struct MockTransport {
        key: TransportKey,
        address: WorkerAddress,
        accept_register: bool,
        started: AtomicBool,
        drained: AtomicBool,
        shut_down: AtomicBool,
        send_count: AtomicUsize,
        /// When true, `send_message` returns `Err(SendBackpressure::new(...))`
        /// whose inner future is immediately ready. Lets tests exercise the
        /// backend's Backpressured path.
        always_backpressure: bool,
    }

    impl MockTransport {
        fn new(key: &str, accept_register: bool) -> Arc<Self> {
            let mut builder = WorkerAddressBuilder::new();
            builder
                .add_entry(key, format!("mock://{}", key).into_bytes())
                .unwrap();
            let address = builder.build().unwrap();

            Arc::new(Self {
                key: TransportKey::from(key),
                address,
                accept_register,
                started: AtomicBool::new(false),
                drained: AtomicBool::new(false),
                shut_down: AtomicBool::new(false),
                send_count: AtomicUsize::new(0),
                always_backpressure: false,
            })
        }

        fn new_backpressured(key: &str) -> Arc<Self> {
            let mut builder = WorkerAddressBuilder::new();
            builder
                .add_entry(key, format!("mock://{}", key).into_bytes())
                .unwrap();
            let address = builder.build().unwrap();

            Arc::new(Self {
                key: TransportKey::from(key),
                address,
                accept_register: true,
                started: AtomicBool::new(false),
                drained: AtomicBool::new(false),
                shut_down: AtomicBool::new(false),
                send_count: AtomicUsize::new(0),
                always_backpressure: true,
            })
        }
    }

    impl Transport for MockTransport {
        fn key(&self) -> TransportKey {
            self.key.clone()
        }
        fn address(&self) -> WorkerAddress {
            self.address.clone()
        }
        fn register(&self, _peer_info: PeerInfo) -> Result<(), TransportError> {
            if self.accept_register {
                Ok(())
            } else {
                Err(TransportError::NoEndpoint)
            }
        }
        fn send_message(
            &self,
            _instance_id: InstanceId,
            _header: Bytes,
            _payload: Bytes,
            _message_type: MessageType,
            _on_error: Arc<dyn TransportErrorHandler>,
        ) -> Result<(), SendBackpressure> {
            self.send_count.fetch_add(1, Ordering::Relaxed);
            if self.always_backpressure {
                Err(SendBackpressure::new(Box::pin(async {})))
            } else {
                Ok(())
            }
        }
        fn start(
            &self,
            _instance_id: InstanceId,
            _channels: TransportAdapter,
            _rt: tokio::runtime::Handle,
        ) -> BoxFuture<'_, anyhow::Result<()>> {
            self.started.store(true, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }
        fn shutdown(&self) {
            self.shut_down.store(true, Ordering::Relaxed);
        }
        fn begin_drain(&self) {
            self.drained.store(true, Ordering::Relaxed);
        }
        fn check_health(
            &self,
            _instance_id: InstanceId,
            _timeout: Duration,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<(), transport::HealthCheckError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    struct NoopErrorHandler;
    impl TransportErrorHandler for NoopErrorHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {}
    }

    /// Helper: build a PeerInfo with entries for specified transport keys.
    fn make_peer_info(keys: &[&str]) -> PeerInfo {
        let instance_id = InstanceId::new_v4();
        let mut builder = WorkerAddressBuilder::new();
        for key in keys {
            builder
                .add_entry(*key, format!("mock://{}", key).into_bytes())
                .unwrap();
        }
        let address = builder.build().unwrap();
        PeerInfo::new(instance_id, address)
    }

    #[tokio::test]
    async fn test_new_single_transport() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
            .await
            .unwrap();

        assert!(t.started.load(Ordering::Relaxed));
        // instance_id should be a valid v4 UUID (non-zero)
        assert!(!backend.instance_id().as_bytes().iter().all(|&b| b == 0));
        assert_eq!(backend.available_transports().len(), 1);
    }

    #[tokio::test]
    async fn test_new_multiple_transports() {
        let t1 = MockTransport::new("tcp", true);
        let t2 = MockTransport::new("http", true);
        let (backend, _streams) = VeloBackend::new(
            vec![
                t1.clone() as Arc<dyn Transport>,
                t2.clone() as Arc<dyn Transport>,
            ],
            None,
        )
        .await
        .unwrap();

        assert!(t1.started.load(Ordering::Relaxed));
        assert!(t2.started.load(Ordering::Relaxed));
        assert_eq!(backend.available_transports().len(), 2);
    }

    #[tokio::test]
    async fn test_register_peer_selects_primary_by_priority() {
        let t1 = MockTransport::new("tcp", true);
        let t2 = MockTransport::new("http", true);
        let (backend, _streams) = VeloBackend::new(
            vec![
                t1.clone() as Arc<dyn Transport>,
                t2.clone() as Arc<dyn Transport>,
            ],
            None,
        )
        .await
        .unwrap();

        let peer = make_peer_info(&["tcp", "http"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        assert!(backend.is_registered(peer_id));
        // Primary should be "tcp" (first in priority)
        let primary = backend.primary_transport.get(&peer_id).unwrap();
        assert_eq!(primary.value().key(), TransportKey::from("tcp"));
    }

    #[tokio::test]
    async fn test_register_peer_no_compatible_transports() {
        // Transport rejects all registrations
        let t = MockTransport::new("tcp", false);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let result = backend.register_peer(peer);
        assert!(matches!(
            result,
            Err(VeloBackendError::NoCompatibleTransports)
        ));
    }

    #[tokio::test]
    async fn test_register_peer_stores_worker_mapping() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let peer_id = peer.instance_id();
        let worker_id = peer_id.worker_id();
        backend.register_peer(peer).unwrap();

        let resolved = backend.try_translate_worker_id(worker_id).unwrap();
        assert_eq!(resolved, peer_id);
    }

    #[tokio::test]
    async fn test_send_message_routes_to_primary() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        let outcome = backend
            .send_message(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
            )
            .unwrap();

        assert!(
            matches!(outcome, SendOutcome::Enqueued),
            "MockTransport returns Ok(()) so backend should report Enqueued"
        );
        assert_eq!(t.send_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_send_message_backpressured() {
        // A transport that always returns Err(SendBackpressure) should cause
        // the backend to surface SendOutcome::Backpressured; awaiting the
        // future must resolve cleanly.
        let t = MockTransport::new_backpressured("tcp");
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        let outcome = backend
            .send_message(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
            )
            .unwrap();

        match outcome {
            SendOutcome::Backpressured(bp) => {
                tokio::time::timeout(Duration::from_secs(1), bp)
                    .await
                    .expect("bp should resolve when inner future completes");
            }
            SendOutcome::Enqueued => {
                panic!("backpressured mock should surface SendOutcome::Backpressured")
            }
        }
    }

    #[tokio::test]
    async fn test_send_message_unregistered_peer() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let result = backend.send_message(
            InstanceId::new_v4(),
            Bytes::new(),
            Bytes::new(),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_message_with_transport_primary_match() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t.clone() as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        backend
            .send_message_with_transport(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
                TransportKey::from("tcp"),
            )
            .unwrap();

        assert_eq!(t.send_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_send_message_with_transport_alternative() {
        let t1 = MockTransport::new("tcp", true);
        let t2 = MockTransport::new("http", true);
        let (backend, _streams) = VeloBackend::new(
            vec![
                t1.clone() as Arc<dyn Transport>,
                t2.clone() as Arc<dyn Transport>,
            ],
            None,
        )
        .await
        .unwrap();

        let peer = make_peer_info(&["tcp", "http"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        // Send via "http" (the alternative transport)
        backend
            .send_message_with_transport(
                peer_id,
                Bytes::from_static(&[1]),
                Bytes::from_static(&[2]),
                MessageType::Message,
                Arc::new(NoopErrorHandler),
                TransportKey::from("http"),
            )
            .unwrap();

        assert_eq!(t2.send_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_send_message_with_transport_not_found() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let peer = make_peer_info(&["tcp"]);
        let peer_id = peer.instance_id();
        backend.register_peer(peer).unwrap();

        let result = backend.send_message_with_transport(
            peer_id,
            Bytes::new(),
            Bytes::new(),
            MessageType::Message,
            Arc::new(NoopErrorHandler),
            TransportKey::from("grpc"),
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_try_translate_worker_id_not_found() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let result = backend.try_translate_worker_id(InstanceId::new_v4().worker_id());
        assert!(matches!(
            result,
            Err(VeloBackendError::WorkerNotRegistered(_))
        ));
    }

    #[tokio::test]
    async fn test_set_transport_priority_valid() {
        let t1 = MockTransport::new("tcp", true);
        let t2 = MockTransport::new("http", true);
        let (backend, _streams) = VeloBackend::new(
            vec![t1 as Arc<dyn Transport>, t2 as Arc<dyn Transport>],
            None,
        )
        .await
        .unwrap();

        // Reverse the priority
        backend
            .set_transport_priority(vec![TransportKey::from("http"), TransportKey::from("tcp")])
            .unwrap();
    }

    #[tokio::test]
    async fn test_set_transport_priority_wrong_length() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let result = backend
            .set_transport_priority(vec![TransportKey::from("tcp"), TransportKey::from("http")]);
        assert!(matches!(
            result,
            Err(VeloBackendError::InvalidTransportPriority(_))
        ));
    }

    #[tokio::test]
    async fn test_set_transport_priority_unknown_key() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let result = backend.set_transport_priority(vec![TransportKey::from("unknown")]);
        assert!(matches!(
            result,
            Err(VeloBackendError::InvalidTransportPriority(_))
        ));
    }

    #[tokio::test]
    async fn test_graceful_shutdown_calls_all_transports() {
        let t1 = MockTransport::new("tcp", true);
        let t2 = MockTransport::new("http", true);
        let (backend, _streams) = VeloBackend::new(
            vec![
                t1.clone() as Arc<dyn Transport>,
                t2.clone() as Arc<dyn Transport>,
            ],
            None,
        )
        .await
        .unwrap();

        backend
            .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(100)))
            .await;

        assert!(t1.drained.load(Ordering::Relaxed));
        assert!(t2.drained.load(Ordering::Relaxed));
        assert!(t1.shut_down.load(Ordering::Relaxed));
        assert!(t2.shut_down.load(Ordering::Relaxed));
        assert!(backend.shutdown_state().is_draining());
        assert!(backend.shutdown_state().teardown_token().is_cancelled());
    }

    #[tokio::test]
    async fn test_peer_info_roundtrip() {
        let t = MockTransport::new("tcp", true);
        let (backend, _streams) = VeloBackend::new(vec![t as Arc<dyn Transport>], None)
            .await
            .unwrap();

        let info = backend.peer_info();
        assert_eq!(info.instance_id(), backend.instance_id());
    }
}
