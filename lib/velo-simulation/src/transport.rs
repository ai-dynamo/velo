// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Simulated transport that routes messages through the fabric's DES queue.

use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use futures::future::BoxFuture;

use velo_common::{InstanceId, PeerInfo, TransportKey, WorkerAddress};
use velo_observability::VeloMetrics;
use velo_transports::{
    HealthCheckError, MessageType, Transport, TransportAdapter, TransportError,
    TransportErrorHandler,
};

use crate::fabric::SimFabric;

/// Simulated transport that delivers messages through the [`SimFabric`].
///
/// Instead of real network I/O, `send_message()` enqueues the message in the
/// fabric, which schedules delivery as a DES event at a future virtual time
/// determined by the network model and current congestion.
pub struct SimTransport {
    key: TransportKey,
    local_address: WorkerAddress,
    fabric: Arc<SimFabric>,
    instance_id: OnceLock<InstanceId>,
    adapter: OnceLock<TransportAdapter>,
    peers: DashMap<InstanceId, ()>,
}

impl SimTransport {
    /// Create a new simulated transport backed by the given fabric.
    pub fn new(fabric: Arc<SimFabric>) -> Self {
        // Build a WorkerAddress with a "sim" entry.
        // The value is a placeholder — SimTransport doesn't use real addresses.
        let address_map: std::collections::HashMap<String, Vec<u8>> =
            [("sim".to_string(), b"sim".to_vec())].into_iter().collect();
        let encoded = rmp_serde::to_vec(&address_map).expect("msgpack encode");
        let local_address = WorkerAddress::from_encoded(Bytes::from(encoded));

        Self {
            key: TransportKey::from("sim"),
            local_address,
            fabric,
            instance_id: OnceLock::new(),
            adapter: OnceLock::new(),
            peers: DashMap::new(),
        }
    }
}

impl Transport for SimTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        self.local_address.clone()
    }

    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
        // Check that the peer has a "sim" transport entry
        let addr = &peer_info.worker_address;
        if addr.get_entry("sim").ok().flatten().is_none() {
            return Err(TransportError::NoEndpoint);
        }
        self.peers.insert(peer_info.instance_id(), ());
        Ok(())
    }

    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) {
        let Some(source_id) = self.instance_id.get().copied() else {
            on_error.on_error(header, payload, "SimTransport not started".into());
            return;
        };

        if !self.peers.contains_key(&instance_id) {
            on_error.on_error(
                header,
                payload,
                format!("Peer not registered: {instance_id}"),
            );
            return;
        }

        self.fabric
            .enqueue(source_id, instance_id, header, payload, message_type);
    }

    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.instance_id
            .set(instance_id)
            .map_err(|_| anyhow::anyhow!("SimTransport already started"))
            .ok();
        let adapter = channels.clone();
        self.adapter
            .set(channels)
            .map_err(|_| anyhow::anyhow!("SimTransport adapter already set"))
            .ok();

        // Register adapter with fabric so it can deliver messages to this instance
        self.fabric.register_adapter(instance_id, adapter);

        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {
        // No resources to clean up in simulation
    }

    fn set_observability(&self, _observability: Arc<VeloMetrics>) {
        // No-op for simulation
    }

    fn check_health(
        &self,
        instance_id: InstanceId,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>> {
        let registered = self.peers.contains_key(&instance_id);
        Box::pin(async move {
            if registered {
                Ok(())
            } else {
                Err(HealthCheckError::PeerNotRegistered)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::BisectionBandwidth;
    use loom_rs::sim::SimulationRuntime;

    #[test]
    fn sim_transport_key_and_address() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = SimTransport::new(fabric);

        assert_eq!(transport.key(), TransportKey::from("sim"));
        assert!(transport.address().get_entry("sim").unwrap().is_some());
    }

    #[test]
    fn register_requires_sim_entry() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = SimTransport::new(fabric.clone());

        // Build a peer with "sim" entry
        let other = SimTransport::new(fabric);
        let peer_addr = other.address();
        let peer_info = PeerInfo::new(InstanceId::new_v4(), peer_addr);

        assert!(transport.register(peer_info).is_ok());
    }
}
