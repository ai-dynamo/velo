// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Simulated network fabric that owns transfer state and delivery scheduling.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use loom_rs::sim::SimHandle;
use parking_lot::Mutex;

use crate::transports::{MessageType, TransportAdapter, TransportErrorHandler};
use velo_ext::{InstanceId, PeerInfo, WorkerId};

use crate::simulation::network::NetworkModel;

/// In-flight message transfer through the simulated fabric.
pub struct Transfer {
    pub source: InstanceId,
    pub target: InstanceId,
    pub header: Bytes,
    pub payload: Bytes,
    pub message_type: MessageType,
    pub total_bytes: usize,
    /// Bytes successfully transferred so far (fractional for precision).
    pub bytes_transferred: f64,
    /// Virtual time when this transfer was enqueued.
    pub enqueue_time: Duration,
    /// Virtual time when progress was last updated.
    pub last_update_time: Duration,
    /// Error callback for late delivery failures after enqueue succeeds.
    pub on_error: Arc<dyn TransportErrorHandler>,
}

struct FabricState {
    network: Box<dyn NetworkModel>,
    transfers: Vec<Transfer>,
    /// Generation counter: incremented when the schedule changes.
    /// Stale completion events compare their generation and become no-ops.
    generation: u64,
}

/// Simulated network fabric.
///
/// The fabric owns the network model and all in-flight transfers. It decides
/// **when** messages complete delivery — the cost is dynamic, depending on
/// what else is in-flight at the moment.
///
/// All `SimTransport` and `SimDiscovery` instances share an `Arc<SimFabric>`.
pub struct SimFabric {
    sim_handle: SimHandle,
    state: Mutex<FabricState>,
    /// Per-instance transport adapters for delivering completed transfers.
    pub(crate) adapters: DashMap<InstanceId, TransportAdapter>,
    /// Peer info registry for discovery.
    pub(crate) peers: DashMap<InstanceId, PeerInfo>,
    /// WorkerId → InstanceId fast-path lookup.
    pub(crate) workers: DashMap<WorkerId, InstanceId>,
}

impl SimFabric {
    /// Create a new fabric with the given DES handle and network model.
    pub fn new(sim_handle: SimHandle, network: impl NetworkModel) -> Self {
        Self {
            sim_handle,
            state: Mutex::new(FabricState {
                network: Box::new(network),
                transfers: Vec::new(),
                generation: 0,
            }),
            adapters: DashMap::new(),
            peers: DashMap::new(),
            workers: DashMap::new(),
        }
    }

    /// Register a velo instance's transport adapter for message delivery.
    pub fn register_adapter(&self, instance_id: InstanceId, adapter: TransportAdapter) {
        self.adapters.insert(instance_id, adapter);
    }

    /// Remove a transport adapter from the fabric.
    pub fn unregister_adapter(&self, instance_id: InstanceId) {
        self.adapters.remove(&instance_id);
    }

    /// Register a peer for discovery.
    pub fn register_peer(&self, peer_info: PeerInfo) {
        let instance_id = peer_info.instance_id();
        let worker_id = instance_id.worker_id();
        self.workers.insert(worker_id, instance_id);
        self.peers.insert(instance_id, peer_info);
    }

    /// Remove a peer from discovery.
    pub fn unregister_peer(&self, instance_id: InstanceId) {
        self.peers.remove(&instance_id);
        self.workers
            .remove_if(&instance_id.worker_id(), |_, existing| {
                *existing == instance_id
            });
    }

    /// Clear all adapters, discovery state, and in-flight transfers.
    ///
    /// Pending DES callbacks become stale because the generation counter is
    /// bumped as part of the reset.
    pub fn reset(&self) {
        self.adapters.clear();
        self.peers.clear();
        self.workers.clear();

        let mut state = self.state.lock();
        state.transfers.clear();
        state.generation += 1;
    }

    /// Enqueue a message for delivery through the simulated fabric.
    ///
    /// The fabric computes when the message will arrive based on the network
    /// model and all currently in-flight transfers. Adding a new transfer may
    /// change the completion time of existing transfers (congestion).
    pub fn enqueue(
        self: &Arc<Self>,
        source: InstanceId,
        target: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) {
        let now = self.sim_handle.now();
        let total_bytes = header.len() + payload.len();

        let mut state = self.state.lock();
        self.update_progress(&mut state, now);

        state.transfers.push(Transfer {
            source,
            target,
            header,
            payload,
            message_type,
            total_bytes,
            bytes_transferred: 0.0,
            enqueue_time: now,
            last_update_time: now,
            on_error,
        });

        self.reschedule(&mut state, now);
    }

    /// Update transfer progress to `now` according to the active network model.
    fn update_progress(&self, state: &mut FabricState, now: Duration) {
        state.network.advance_to(&mut state.transfers, now);
    }

    /// Reschedule the next completion event based on current transfers.
    fn reschedule(self: &Arc<Self>, state: &mut FabricState, now: Duration) {
        state.generation += 1;
        let generation = state.generation;

        if let Some(next) = state.network.next_completion(&state.transfers, now) {
            let fabric = Arc::clone(self);
            let delay = next.completion_time.saturating_sub(now);
            self.sim_handle.schedule(delay, move || {
                fabric.on_completion(generation);
            });
        }
    }

    /// Called by the DES scheduler when a completion event fires.
    fn on_completion(self: &Arc<Self>, expected_generation: u64) {
        let now = self.sim_handle.now();
        let mut state = self.state.lock();

        if state.generation != expected_generation {
            return;
        }

        self.update_progress(&mut state, now);
        let mut completed_indices = self.completed_indices(&state, now);
        if completed_indices.is_empty() {
            self.reschedule(&mut state, now);
            return;
        }

        completed_indices.sort_unstable();
        let mut completed = Vec::with_capacity(completed_indices.len());
        for idx in completed_indices.into_iter().rev() {
            if idx < state.transfers.len() {
                completed.push(state.transfers.swap_remove(idx));
            }
        }
        completed.reverse();

        self.reschedule(&mut state, now);
        drop(state);

        for transfer in completed {
            self.deliver_message(transfer);
        }
    }

    fn completed_indices(&self, state: &FabricState, now: Duration) -> Vec<usize> {
        let mut completed = Vec::new();

        for (idx, transfer) in state.transfers.iter().enumerate() {
            if state.network.is_complete(transfer, now) {
                completed.push(idx);
            }
        }

        if completed.is_empty()
            && let Some(next) = state.network.next_completion(&state.transfers, now)
            && next.completion_time <= now
            && next.transfer_index < state.transfers.len()
        {
            completed.push(next.transfer_index);
        }

        completed
    }

    /// Push a completed transfer's bytes into the appropriate transport stream.
    fn deliver_message(&self, transfer: Transfer) {
        let target_draining = self
            .adapters
            .get(&transfer.target)
            .map(|adapter| adapter.shutdown_state.is_draining())
            .unwrap_or(false);

        if transfer.message_type == MessageType::Message && target_draining {
            self.reject_due_to_drain(transfer);
            return;
        }

        let header_for_error = transfer.header.clone();
        let payload_for_error = transfer.payload.clone();
        let on_error = Arc::clone(&transfer.on_error);
        let target = transfer.target;

        let Some(adapter) = self.adapters.get(&target) else {
            tracing::warn!(target = %target, "SimFabric: no adapter registered for target instance");
            on_error.on_error(
                header_for_error,
                payload_for_error,
                format!("SimFabric: no adapter registered for target instance {target}"),
            );
            return;
        };

        let result = match transfer.message_type {
            MessageType::Message => adapter
                .message_stream
                .send((transfer.header, transfer.payload)),
            MessageType::Response | MessageType::ShuttingDown => adapter
                .response_stream
                .send((transfer.header, transfer.payload)),
            MessageType::Ack | MessageType::Event => adapter
                .event_stream
                .send((transfer.header, transfer.payload)),
        };

        if let Err(e) = result {
            tracing::warn!(target = %target, "SimFabric: failed to deliver message: {e}");
            on_error.on_error(
                header_for_error,
                payload_for_error,
                format!("SimFabric: failed to deliver message to {target}: {e}"),
            );
        }
    }

    fn reject_due_to_drain(&self, transfer: Transfer) {
        let header_for_error = transfer.header.clone();
        let payload_for_error = transfer.payload.clone();
        let on_error = Arc::clone(&transfer.on_error);
        let source = transfer.source;
        let target = transfer.target;

        let Some(source_adapter) = self.adapters.get(&source) else {
            tracing::warn!(
                source = %source,
                target = %target,
                "SimFabric: source adapter missing while sending ShuttingDown"
            );
            on_error.on_error(
                header_for_error,
                payload_for_error,
                format!(
                    "SimFabric: source adapter missing while rejecting drained target {target}"
                ),
            );
            return;
        };

        if let Err(e) = source_adapter
            .response_stream
            .send((transfer.header, Bytes::new()))
        {
            tracing::warn!(
                source = %source,
                target = %target,
                "SimFabric: failed to deliver ShuttingDown response: {e}"
            );
            on_error.on_error(
                header_for_error,
                payload_for_error,
                format!("SimFabric: failed to deliver ShuttingDown response from {target}: {e}"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::simulation::network::BisectionBandwidth;
    use crate::transports::TransportErrorHandler;
    use loom_rs::sim::SimulationRuntime;
    use parking_lot::Mutex;

    struct NoopErrorHandler;

    impl TransportErrorHandler for NoopErrorHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {}
    }

    #[derive(Clone)]
    struct RecordingErrorHandler {
        errors: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingErrorHandler {
        fn new() -> Self {
            Self {
                errors: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn errors(&self) -> Arc<Mutex<Vec<String>>> {
            Arc::clone(&self.errors)
        }
    }

    impl TransportErrorHandler for RecordingErrorHandler {
        fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
            self.errors.lock().push(error);
        }
    }

    fn noop_handler() -> Arc<dyn TransportErrorHandler> {
        Arc::new(NoopErrorHandler)
    }

    #[test]
    fn fabric_delivers_message() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();

        let fabric = Arc::new(SimFabric::new(
            handle.clone(),
            BisectionBandwidth {
                link_gbps: 200.0,
                bisection_gbps: 12_800.0,
                base_latency: Duration::from_micros(10),
            },
        ));

        let (adapter, streams) = crate::transports::make_channels();
        let target_id = InstanceId::new_v4();
        let source_id = InstanceId::new_v4();
        fabric.register_adapter(target_id, adapter);

        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Message,
            noop_handler(),
        );

        sim.run().unwrap();

        let (header, payload) = streams.message_stream.try_recv().unwrap();
        assert_eq!(&header[..], b"hdr");
        assert_eq!(&payload[..], b"pay");
        assert!(sim.now() >= Duration::from_micros(10));
    }

    #[test]
    fn simultaneous_completions_finish_in_same_tick() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(
            handle.clone(),
            BisectionBandwidth {
                link_gbps: 8.0,
                bisection_gbps: 8.0,
                base_latency: Duration::ZERO,
            },
        ));

        let target_id = InstanceId::new_v4();
        let source_id = InstanceId::new_v4();
        let (adapter, streams) = crate::transports::make_channels();
        fabric.register_adapter(target_id, adapter);

        let payload = Bytes::from(vec![0u8; 998]);
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"m1"),
            payload.clone(),
            MessageType::Message,
            noop_handler(),
        );
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"m2"),
            payload,
            MessageType::Message,
            noop_handler(),
        );

        let final_time = sim.run().unwrap();
        assert_eq!(final_time, Duration::from_micros(2));

        let (h1, _) = streams.message_stream.try_recv().unwrap();
        let (h2, _) = streams.message_stream.try_recv().unwrap();
        let headers: Vec<&[u8]> = vec![&h1[..], &h2[..]];
        assert!(headers.contains(&b"m1".as_slice()));
        assert!(headers.contains(&b"m2".as_slice()));
    }

    #[test]
    fn mid_flight_enqueue_preserves_progress() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(
            handle.clone(),
            BisectionBandwidth {
                link_gbps: 8.0,
                bisection_gbps: 8.0,
                base_latency: Duration::ZERO,
            },
        ));

        let target_id = InstanceId::new_v4();
        let source_id = InstanceId::new_v4();
        let (adapter, streams) = crate::transports::make_channels();
        fabric.register_adapter(target_id, adapter);

        let payload = Bytes::from(vec![0u8; 999]);
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"a"),
            payload.clone(),
            MessageType::Message,
            noop_handler(),
        );

        let delayed_fabric = Arc::clone(&fabric);
        handle.schedule(Duration::from_nanos(500), move || {
            delayed_fabric.enqueue(
                source_id,
                target_id,
                Bytes::from_static(b"b"),
                Bytes::from(vec![0u8; 999]),
                MessageType::Message,
                noop_handler(),
            );
        });

        let final_time = sim.run().unwrap();
        assert_eq!(final_time, Duration::from_micros(2));

        let (h1, _) = streams.message_stream.try_recv().unwrap();
        let (h2, _) = streams.message_stream.try_recv().unwrap();
        let headers: Vec<&[u8]> = vec![&h1[..], &h2[..]];
        assert!(headers.contains(&b"a".as_slice()));
        assert!(headers.contains(&b"b".as_slice()));
    }

    #[test]
    fn drain_rejects_messages_with_shutting_down() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));

        let source_id = InstanceId::new_v4();
        let target_id = InstanceId::new_v4();
        let (source_adapter, source_streams) = crate::transports::make_channels();
        let (target_adapter, target_streams) = crate::transports::make_channels();
        fabric.register_adapter(source_id, source_adapter);
        fabric.register_adapter(target_id, target_adapter);
        target_streams.shutdown_state.begin_drain();

        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Message,
            noop_handler(),
        );

        sim.run().unwrap();

        assert!(target_streams.message_stream.try_recv().is_err());
        let (header, payload) = source_streams.response_stream.try_recv().unwrap();
        assert_eq!(&header[..], b"hdr");
        assert!(payload.is_empty());
    }

    #[test]
    fn missing_adapter_reports_error_handler() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));

        let handler = RecordingErrorHandler::new();
        let errors = handler.errors();
        fabric.enqueue(
            InstanceId::new_v4(),
            InstanceId::new_v4(),
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Message,
            Arc::new(handler),
        );

        sim.run().unwrap();
        let errors = errors.lock();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("no adapter registered"));
    }

    #[test]
    fn reset_invalidates_pending_events_and_clears_state() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));

        let target_id = InstanceId::new_v4();
        let peer = PeerInfo::new(
            target_id,
            velo_ext::WorkerAddress::from_encoded(Bytes::new()),
        );
        let (adapter, streams) = crate::transports::make_channels();
        fabric.register_adapter(target_id, adapter);
        fabric.register_peer(peer);
        fabric.enqueue(
            InstanceId::new_v4(),
            target_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Message,
            noop_handler(),
        );

        fabric.reset();
        sim.run().unwrap();

        assert!(streams.message_stream.try_recv().is_err());
        assert!(fabric.adapters.is_empty());
        assert!(fabric.peers.is_empty());
        assert!(fabric.workers.is_empty());
    }
}
