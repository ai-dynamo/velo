// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS-based peer discovery for Velo distributed systems.
//!
//! [`NatsPeerDiscovery`] uses NATS request-reply for peer discovery.
//! Registration subscribes to per-instance and per-worker subjects and
//! spawns a responder task that replies with MessagePack-serialized [`PeerInfo`].

pub mod subjects;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use velo_common::{InstanceId, PeerInfo, WorkerId};

use super::{PeerDiscovery, RegistrationGuard};
use subjects::{discovery_instance_subject, discovery_worker_subject};

/// Default timeout for discovery queries.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// NATS-based peer discovery backend.
///
/// Constructed via [`NatsPeerDiscoveryBuilder`]. Implements [`PeerDiscovery`]
/// for discovering peers by instance ID or worker ID over NATS request-reply.
pub struct NatsPeerDiscovery {
    client: Arc<async_nats::Client>,
    cluster_id: String,
}

impl NatsPeerDiscovery {
    /// Register a peer by subscribing to discovery subjects and spawning a responder task.
    ///
    /// Pre-serializes `peer_info` to MessagePack bytes once, then spawns a task that
    /// replies to all incoming discovery requests with those bytes. The returned
    /// [`NatsRegistrationGuard`] cancels the responder task when dropped or when
    /// [`RegistrationGuard::unregister`] is called.
    pub async fn register(&self, peer_info: &PeerInfo) -> anyhow::Result<NatsRegistrationGuard> {
        let peer_info_bytes = Bytes::from(
            rmp_serde::to_vec(peer_info)
                .context("Failed to serialize PeerInfo for registration")?,
        );

        let instance_sub = self
            .client
            .subscribe(discovery_instance_subject(
                &self.cluster_id,
                peer_info.instance_id(),
            ))
            .await
            .context("Failed to subscribe to instance discovery subject")?;

        let worker_sub = self
            .client
            .subscribe(discovery_worker_subject(
                &self.cluster_id,
                peer_info.worker_id(),
            ))
            .await
            .context("Failed to subscribe to worker discovery subject")?;

        let cancel = CancellationToken::new();

        tokio::spawn(run_responder_loop(
            instance_sub,
            worker_sub,
            peer_info_bytes,
            self.client.clone(),
            cancel.clone(),
        ));

        Ok(NatsRegistrationGuard { cancel })
    }

    /// Issue a NATS request to the given subject and deserialize the `PeerInfo` response.
    ///
    /// Maps NoResponders and timeout errors to "peer not found" errors.
    async fn query_peer(&self, subject: String) -> anyhow::Result<PeerInfo> {
        match tokio::time::timeout(
            DISCOVERY_TIMEOUT,
            self.client.request(subject, Bytes::new()),
        )
        .await
        {
            Ok(Ok(response)) => rmp_serde::from_slice(&response.payload)
                .context("Failed to deserialize PeerInfo from discovery response"),
            Ok(Err(_e)) => anyhow::bail!("Peer not found (no responder)"),
            Err(_elapsed) => anyhow::bail!("Peer not found (timeout)"),
        }
    }
}

impl PeerDiscovery for NatsPeerDiscovery {
    fn discover_by_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let subject = discovery_worker_subject(&self.cluster_id, worker_id);
        Box::pin(self.query_peer(subject))
    }

    fn discover_by_instance_id(
        &self,
        instance_id: InstanceId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        let subject = discovery_instance_subject(&self.cluster_id, instance_id);
        Box::pin(self.query_peer(subject))
    }
}

/// Builder for [`NatsPeerDiscovery`].
///
/// Requires a pre-instantiated `Arc<async_nats::Client>` and a cluster ID.
pub struct NatsPeerDiscoveryBuilder {
    client: Arc<async_nats::Client>,
    cluster_id: String,
}

impl NatsPeerDiscoveryBuilder {
    /// Create a new builder with the given NATS client and cluster ID.
    pub fn new(client: Arc<async_nats::Client>, cluster_id: impl Into<String>) -> Self {
        Self {
            client,
            cluster_id: cluster_id.into(),
        }
    }

    /// Consume the builder and produce a [`NatsPeerDiscovery`].
    pub fn build(self) -> NatsPeerDiscovery {
        NatsPeerDiscovery {
            client: self.client,
            cluster_id: self.cluster_id,
        }
    }
}

/// RAII guard that cancels the discovery responder task when dropped.
///
/// Obtained from [`NatsPeerDiscovery::register`]. Callers that can await
/// should prefer [`unregister()`](RegistrationGuard::unregister) over
/// relying on `Drop` (per D-10).
pub struct NatsRegistrationGuard {
    cancel: CancellationToken,
}

impl RegistrationGuard for NatsRegistrationGuard {
    fn unregister(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.cancel.cancel();
            Ok(())
        })
    }
}

impl Drop for NatsRegistrationGuard {
    fn drop(&mut self) {
        // Sync safety net: cancel the responder task token.
        // The task's select! arm will fire, unsubscribing both subscribers.
        self.cancel.cancel();
    }
}

/// Responder loop that replies to discovery requests with pre-serialized PeerInfo.
///
/// Runs until the cancellation token fires or a subscription stream ends.
/// On cancellation, unsubscribes both subscribers before exiting.
async fn run_responder_loop(
    mut instance_sub: async_nats::Subscriber,
    mut worker_sub: async_nats::Subscriber,
    peer_info_bytes: Bytes,
    client: Arc<async_nats::Client>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("Discovery responder cancelled, unsubscribing");
                let _ = instance_sub.unsubscribe().await;
                let _ = worker_sub.unsubscribe().await;
                break;
            }
            msg = instance_sub.next() => {
                match msg {
                    Some(msg) => reply_peer_info(&client, &msg, &peer_info_bytes).await,
                    None => break,
                }
            }
            msg = worker_sub.next() => {
                match msg {
                    Some(msg) => reply_peer_info(&client, &msg, &peer_info_bytes).await,
                    None => break,
                }
            }
        }
    }
    // Fallback unsubscribe if loop exited via stream-end rather than cancel.
    let _ = instance_sub.unsubscribe().await;
    let _ = worker_sub.unsubscribe().await;
    tracing::debug!("Discovery responder loop exited");
}

/// Reply to a single discovery request with pre-serialized PeerInfo bytes.
async fn reply_peer_info(
    client: &async_nats::Client,
    msg: &async_nats::Message,
    peer_info_bytes: &Bytes,
) {
    if let Some(reply) = &msg.reply {
        if let Err(e) = client.publish(reply.clone(), peer_info_bytes.clone()).await {
            tracing::warn!(error = %e, "Failed to reply to discovery request");
        }
    }
    // Requests with no reply inbox are silently ignored
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_test_address(data: &[u8]) -> velo_common::WorkerAddress {
        let map: HashMap<String, Vec<u8>> =
            [("nats".to_string(), data.to_vec())].into_iter().collect();
        let encoded = rmp_serde::to_vec(&map).unwrap();
        velo_common::WorkerAddress::from_encoded(encoded)
    }

    #[test]
    fn test_peer_info_msgpack_round_trip() {
        let instance_id = InstanceId::new_v4();
        let worker_address = make_test_address(b"nats://localhost:4222/test");
        let peer_info = PeerInfo::new(instance_id, worker_address.clone());

        let serialized = rmp_serde::to_vec(&peer_info).expect("Serialization must succeed");
        let deserialized: PeerInfo =
            rmp_serde::from_slice(&serialized).expect("Deserialization must succeed");

        assert_eq!(
            deserialized.instance_id(),
            peer_info.instance_id(),
            "Round-trip must preserve instance_id"
        );
        assert_eq!(
            deserialized.worker_address(),
            peer_info.worker_address(),
            "Round-trip must preserve worker_address"
        );
    }

    #[test]
    fn test_nats_registration_guard_cancel_on_drop() {
        let token = CancellationToken::new();
        let guard = NatsRegistrationGuard {
            cancel: token.clone(),
        };
        assert!(
            !token.is_cancelled(),
            "Token must not be cancelled before drop"
        );
        drop(guard);
        assert!(token.is_cancelled(), "Token must be cancelled after drop");
    }

    #[tokio::test]
    async fn test_nats_registration_guard_unregister_cancels() {
        let token = CancellationToken::new();
        let mut guard = NatsRegistrationGuard {
            cancel: token.clone(),
        };
        assert!(
            !token.is_cancelled(),
            "Token must not be cancelled before unregister"
        );
        guard.unregister().await.expect("unregister must succeed");
        assert!(
            token.is_cancelled(),
            "Token must be cancelled after unregister()"
        );
    }
}
