// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-memory peer discovery for simulation.

use std::sync::Arc;

use anyhow::Result;
use futures::future::BoxFuture;

use velo_common::{InstanceId, PeerInfo, WorkerId};
use velo_discovery::PeerDiscovery;

use crate::fabric::SimFabric;

/// In-memory peer discovery backed by the shared [`SimFabric`].
///
/// Instances register themselves via [`register()`](SimDiscovery::register),
/// and other instances discover them via the [`PeerDiscovery`] trait.
pub struct SimDiscovery {
    fabric: Arc<SimFabric>,
}

impl SimDiscovery {
    /// Create a new discovery service backed by the given fabric.
    pub fn new(fabric: Arc<SimFabric>) -> Self {
        Self { fabric }
    }

    /// Register a peer for discovery by other instances.
    pub fn register(&self, peer_info: PeerInfo) {
        self.fabric.register_peer(peer_info);
    }
}

impl PeerDiscovery for SimDiscovery {
    fn discover_by_worker_id(&self, worker_id: WorkerId) -> BoxFuture<'_, Result<PeerInfo>> {
        Box::pin(async move {
            self.fabric
                .workers
                .get(&worker_id)
                .and_then(|instance_id| self.fabric.peers.get(&instance_id).map(|p| p.clone()))
                .ok_or_else(|| anyhow::anyhow!("Worker {worker_id:?} not found in simulation"))
        })
    }

    fn discover_by_instance_id(&self, instance_id: InstanceId) -> BoxFuture<'_, Result<PeerInfo>> {
        Box::pin(async move {
            self.fabric
                .peers
                .get(&instance_id)
                .map(|p| p.clone())
                .ok_or_else(|| anyhow::anyhow!("Instance {instance_id} not found in simulation"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::BisectionBandwidth;
    use loom_rs::sim::SimulationRuntime;
    use velo_common::WorkerAddress;

    fn make_peer_info() -> PeerInfo {
        let id = InstanceId::new_v4();
        let addr_map: std::collections::HashMap<String, Vec<u8>> =
            [("sim".to_string(), b"sim".to_vec())].into_iter().collect();
        let encoded = rmp_serde::to_vec(&addr_map).unwrap();
        PeerInfo::new(id, WorkerAddress::from_encoded(bytes::Bytes::from(encoded)))
    }

    #[test]
    fn register_and_discover_by_instance() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let discovery = SimDiscovery::new(fabric);

        let peer = make_peer_info();
        let id = peer.instance_id();
        discovery.register(peer.clone());

        let found = sim
            .loom()
            .block_on(discovery.discover_by_instance_id(id))
            .unwrap();
        assert_eq!(found.instance_id(), id);
    }

    #[test]
    fn register_and_discover_by_worker() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let discovery = SimDiscovery::new(fabric);

        let peer = make_peer_info();
        let id = peer.instance_id();
        let worker_id = id.worker_id();
        discovery.register(peer.clone());

        let found = sim
            .loom()
            .block_on(discovery.discover_by_worker_id(worker_id))
            .unwrap();
        assert_eq!(found.instance_id(), id);
    }

    #[test]
    fn discover_unknown_returns_error() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let discovery = SimDiscovery::new(fabric);

        let result = sim
            .loom()
            .block_on(discovery.discover_by_instance_id(InstanceId::new_v4()));
        assert!(result.is_err());
    }
}
