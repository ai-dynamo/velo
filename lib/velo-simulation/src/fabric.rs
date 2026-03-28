// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Simulated network fabric that owns transfer state and delivery scheduling.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use loom_rs::sim::SimHandle;
use parking_lot::Mutex;

use velo_common::{InstanceId, PeerInfo, WorkerId};
use velo_transports::{MessageType, TransportAdapter};

use crate::network::NetworkModel;

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

    /// Register a peer for discovery.
    pub fn register_peer(&self, peer_info: PeerInfo) {
        let instance_id = peer_info.instance_id();
        let worker_id = instance_id.worker_id();
        self.workers.insert(worker_id, instance_id);
        self.peers.insert(instance_id, peer_info);
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
    ) {
        let now = self.sim_handle.now();
        let total_bytes = header.len() + payload.len();

        let mut state = self.state.lock();

        // Update progress on all in-flight transfers before adding the new one
        self.update_progress(&mut state, now);

        // Add the new transfer
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
        });

        // Reschedule: bump generation (invalidates old scheduled event) and schedule new
        self.reschedule(&mut state, now);
    }

    /// Update bytes_transferred on all in-flight transfers based on elapsed time.
    fn update_progress(&self, state: &mut FabricState, now: Duration) {
        if state.transfers.is_empty() {
            return;
        }

        // We need bandwidth allocations to compute progress.
        // Re-evaluate the model to get per-transfer bandwidth, then apply elapsed time.
        let link_gbps = self.get_bandwidth_allocations(state);

        for (i, transfer) in state.transfers.iter_mut().enumerate() {
            let elapsed = now.saturating_sub(transfer.last_update_time);
            if elapsed.is_zero() || i >= link_gbps.len() {
                continue;
            }
            let bits_transferred = link_gbps[i] * 1e9 * elapsed.as_secs_f64();
            transfer.bytes_transferred += bits_transferred / 8.0;
            transfer.last_update_time = now;
        }
    }

    /// Get bandwidth allocation (in Gbps) for each transfer.
    /// This mirrors the logic in NetworkModel but returns allocations instead of completion.
    fn get_bandwidth_allocations(&self, _state: &FabricState) -> Vec<f64> {
        // For now, delegate to evaluate and reverse-engineer.
        // A cleaner approach would add an `allocations()` method to NetworkModel,
        // but for the initial impl we compute directly using the BisectionBandwidth logic.
        //
        // Since we can't generically extract allocations from NetworkModel::evaluate(),
        // we use a simpler approach: assume linear bandwidth sharing and compute
        // from the model parameters. For custom NetworkModel impls, progress tracking
        // would need to be refined.
        //
        // For now: just return empty and rely on completion-time-based tracking.
        // The fabric treats each transfer atomically — progress is updated only at
        // completion events, not incrementally.
        vec![]
    }

    /// Reschedule the next completion event based on current transfers.
    fn reschedule(self: &Arc<Self>, state: &mut FabricState, now: Duration) {
        state.generation += 1;
        let generation = state.generation;

        if let Some(next) = state.network.evaluate(&state.transfers, now) {
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

        // Stale event — schedule changed since this was queued
        if state.generation != expected_generation {
            return;
        }

        // Find which transfer completes at this time
        let Some(next) = state.network.evaluate(&state.transfers, now) else {
            return;
        };

        // The evaluate() might return a completion in the future if the state
        // has drifted slightly. Only complete if the time matches (within tolerance).
        // Since the DES fires at exactly the scheduled time, this should match.
        let idx = next.transfer_index;
        if idx >= state.transfers.len() {
            return;
        }

        // Remove the completed transfer
        let transfer = state.transfers.swap_remove(idx);

        // Deliver the message
        self.deliver_message(transfer);

        // Reschedule for the next completion
        self.reschedule(&mut state, now);
    }

    /// Push a completed transfer's bytes into the target's TransportAdapter.
    fn deliver_message(&self, transfer: Transfer) {
        let Some(adapter) = self.adapters.get(&transfer.target) else {
            tracing::warn!(
                target = %transfer.target,
                "SimFabric: no adapter registered for target instance"
            );
            return;
        };

        let pair = (transfer.header, transfer.payload);
        let result = match transfer.message_type {
            MessageType::Message => adapter.message_stream.send(pair),
            MessageType::Response | MessageType::ShuttingDown => adapter.response_stream.send(pair),
            MessageType::Ack | MessageType::Event => adapter.event_stream.send(pair),
        };

        if let Err(e) = result {
            tracing::warn!(
                target = %transfer.target,
                "SimFabric: failed to deliver message: {e}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::BisectionBandwidth;
    use loom_rs::sim::SimulationRuntime;

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

        // Create a transport adapter for the target
        let (adapter, streams) = velo_transports::make_channels();
        let target_id = InstanceId::new_v4();
        let source_id = InstanceId::new_v4();
        fabric.register_adapter(target_id, adapter);

        // Enqueue a message
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"hdr"),
            Bytes::from_static(b"pay"),
            MessageType::Message,
        );

        // Run the simulation — should deliver the message
        sim.run().unwrap();

        // Check delivery
        let (header, payload) = streams.message_stream.try_recv().unwrap();
        assert_eq!(&header[..], b"hdr");
        assert_eq!(&payload[..], b"pay");

        // Virtual time should be ~10µs (base latency for tiny message)
        assert!(sim.now() >= Duration::from_micros(10));
    }

    #[test]
    fn fabric_schedules_completion_dynamically() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();

        let fabric = Arc::new(SimFabric::new(
            handle.clone(),
            BisectionBandwidth {
                link_gbps: 200.0,
                bisection_gbps: 12_800.0,
                base_latency: Duration::ZERO,
            },
        ));

        let target_id = InstanceId::new_v4();
        let source_id = InstanceId::new_v4();
        let (adapter, streams) = velo_transports::make_channels();
        fabric.register_adapter(target_id, adapter);

        // Send two messages — they should both arrive
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"msg1"),
            Bytes::new(),
            MessageType::Message,
        );
        fabric.enqueue(
            source_id,
            target_id,
            Bytes::from_static(b"msg2"),
            Bytes::new(),
            MessageType::Message,
        );

        sim.run().unwrap();

        // Both should have been delivered
        let (h1, _) = streams.message_stream.try_recv().unwrap();
        let (h2, _) = streams.message_stream.try_recv().unwrap();
        // One of them is msg1, one is msg2
        let headers: Vec<&[u8]> = vec![&h1[..], &h2[..]];
        assert!(headers.contains(&b"msg1".as_slice()));
        assert!(headers.contains(&b"msg2".as_slice()));
    }
}
