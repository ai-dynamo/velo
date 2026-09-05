// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Fixtures shared by the mux's unit tests.
//!
//! Only what more than one test module needs. The batcher's own harness stays
//! in [`peer_batcher::tests::support`](super::peer_batcher), because it is about
//! driving a batcher; what is here is about the *messenger underneath* one, and
//! the transport-level tests need the same thing.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

/// A transport whose per-target send channel this test owns.
///
/// One admission gate over a `bounded(1)` channel nobody drains: the first send
/// takes the fast path, every send after it parks in the gate. That is the shape
/// of a congested peer, produced deterministically instead of waited for.
pub(super) struct StallingTransport {
    key: velo_ext::TransportKey,
    address: velo_ext::WorkerAddress,
    gate: velo_ext::AdmissionGate<(Bytes, Bytes)>,
    peers: Mutex<HashSet<velo_ext::InstanceId>>,
}

impl StallingTransport {
    pub(super) fn new(rt: tokio::runtime::Handle) -> (Arc<Self>, flume::Receiver<(Bytes, Bytes)>) {
        let (tx, rx) = flume::bounded::<(Bytes, Bytes)>(1);
        let key = velo_ext::TransportKey::new("stalling");
        let transport = Arc::new(Self {
            key,
            address: stalling_address(),
            gate: velo_ext::AdmissionGate::new(tx, rt),
            peers: Mutex::new(HashSet::new()),
        });
        (transport, rx)
    }
}

/// The address every peer of a [`StallingTransport`] is registered under.
///
/// A peer this transport accepts, so a test can name a target that resolves and
/// never answers. Nothing reads the far end; the gate is the only thing under
/// test.
pub(super) fn stalling_address() -> velo_ext::WorkerAddress {
    let entries = HashMap::from([("stalling".to_string(), b"stalling".to_vec())]);
    velo_ext::WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).expect("encode"))
}

impl velo_ext::Transport for StallingTransport {
    fn key(&self) -> velo_ext::TransportKey {
        self.key.clone()
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        self.address.clone()
    }

    fn register(&self, peer_info: velo_ext::PeerInfo) -> Result<(), velo_ext::TransportError> {
        self.peers
            .lock()
            .expect("peer set poisoned")
            .insert(peer_info.instance_id());
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: velo_ext::InstanceId,
        header: Bytes,
        payload: Bytes,
        _message_type: velo_ext::MessageType,
        _on_error: Arc<dyn velo_ext::TransportErrorHandler>,
    ) -> velo_ext::SendOutcome {
        self.gate.send((header, payload))
    }

    fn start(
        &self,
        _instance_id: velo_ext::InstanceId,
        _channels: velo_ext::TransportAdapter,
        _rt: tokio::runtime::Handle,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn check_health(
        &self,
        _instance_id: velo_ext::InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), velo_ext::HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}
