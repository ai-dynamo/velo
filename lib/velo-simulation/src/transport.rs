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
    HealthCheckError, MessageType, ShutdownState, Transport, TransportAdapter, TransportError,
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
    shutdown_state: OnceLock<ShutdownState>,
    peers: DashMap<InstanceId, ()>,
}

impl SimTransport {
    /// Create a new simulated transport backed by the given fabric.
    pub fn new(fabric: Arc<SimFabric>) -> anyhow::Result<Self> {
        let address_map: std::collections::HashMap<String, Vec<u8>> =
            [("sim".to_string(), b"sim".to_vec())].into_iter().collect();
        let encoded = rmp_serde::to_vec(&address_map)?;
        let local_address = WorkerAddress::from_encoded(Bytes::from(encoded));

        Ok(Self {
            key: TransportKey::from("sim"),
            local_address,
            fabric,
            instance_id: OnceLock::new(),
            adapter: OnceLock::new(),
            shutdown_state: OnceLock::new(),
            peers: DashMap::new(),
        })
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

        self.fabric.enqueue(
            source_id,
            instance_id,
            header,
            payload,
            message_type,
            on_error,
        );
    }

    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let result = self
            .instance_id
            .set(instance_id)
            .map_err(|_| anyhow::anyhow!("SimTransport already started"))
            .and_then(|_| {
                self.adapter
                    .set(channels.clone())
                    .map_err(|_| anyhow::anyhow!("SimTransport adapter already set"))
            })
            .and_then(|_| {
                self.shutdown_state
                    .set(channels.shutdown_state.clone())
                    .map_err(|_| anyhow::anyhow!("SimTransport shutdown state already set"))
            });

        match result {
            Ok(()) => {
                self.fabric.register_adapter(instance_id, channels);
                Box::pin(async { Ok(()) })
            }
            Err(e) => Box::pin(async { Err(e) }),
        }
    }

    fn shutdown(&self) {
        if let Some(instance_id) = self.instance_id.get().copied() {
            self.fabric.unregister_adapter(instance_id);
        }
        self.peers.clear();
    }

    fn set_observability(&self, _observability: Arc<VeloMetrics>) {
        // No-op for simulation
    }

    fn begin_drain(&self) {
        if let Some(state) = self.shutdown_state.get() {
            state.begin_drain();
        }
    }

    fn check_health(
        &self,
        instance_id: InstanceId,
        _timeout: Duration,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>> {
        let started = self.instance_id.get().is_some();
        let registered = self.peers.contains_key(&instance_id);
        Box::pin(async move {
            if !started {
                Err(HealthCheckError::TransportNotStarted)
            } else if registered {
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
        let transport = SimTransport::new(fabric).unwrap();

        assert_eq!(transport.key(), TransportKey::from("sim"));
        assert!(transport.address().get_entry("sim").unwrap().is_some());
    }

    #[test]
    fn register_requires_sim_entry() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = SimTransport::new(fabric.clone()).unwrap();

        let other = SimTransport::new(fabric).unwrap();
        let peer_addr = other.address();
        let peer_info = PeerInfo::new(InstanceId::new_v4(), peer_addr);

        assert!(transport.register(peer_info).is_ok());
    }

    #[test]
    fn begin_drain_sets_shared_shutdown_state() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = Arc::new(SimTransport::new(fabric).unwrap());
        let instance_id = InstanceId::new_v4();
        let (adapter, streams) = velo_transports::make_channels();
        let transport_start = Arc::clone(&transport);

        sim.run_until_complete(async move {
            transport_start
                .start(instance_id, adapter, tokio::runtime::Handle::current())
                .await
                .unwrap();
        })
        .unwrap();

        transport.begin_drain();
        assert!(streams.shutdown_state.is_draining());
    }

    #[test]
    fn shutdown_unregisters_adapter() {
        let mut sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = Arc::new(SimTransport::new(Arc::clone(&fabric)).unwrap());
        let instance_id = InstanceId::new_v4();
        let (adapter, _streams) = velo_transports::make_channels();
        let transport_start = Arc::clone(&transport);

        sim.run_until_complete(async move {
            transport_start
                .start(instance_id, adapter, tokio::runtime::Handle::current())
                .await
                .unwrap();
        })
        .unwrap();

        assert!(fabric.adapters.contains_key(&instance_id));
        transport.shutdown();
        assert!(!fabric.adapters.contains_key(&instance_id));
    }

    #[test]
    fn check_health_requires_start() {
        let sim = SimulationRuntime::new().unwrap();
        let handle = sim.handle();
        let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
        let transport = SimTransport::new(fabric).unwrap();

        let result = sim
            .loom()
            .block_on(transport.check_health(InstanceId::new_v4(), Duration::from_secs(1)));
        assert_eq!(result, Err(HealthCheckError::TransportNotStarted));
    }
}
