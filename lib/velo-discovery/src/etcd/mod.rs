// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Etcd-based service discovery for Velo distributed systems.
//!
//! Uses etcd KV with leases for service registration and etcd watch for
//! real-time change notification. Registration keys follow the pattern:
//!
//! ```text
//! /velo/{cluster_id}/services/{service_name}/{instance_id}
//! ```
//!
//! Each registration is tied to an etcd lease with a configurable TTL.
//! A background keep-alive task maintains the lease; if the process dies,
//! the lease expires and the key is automatically deleted by etcd.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use etcd_client::{Client, EventType, GetOptions, PutOptions, WatchOptions};
use futures::Stream;
use futures::future::BoxFuture;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use velo_common::InstanceId;

use super::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};

/// Default lease TTL in seconds.
const DEFAULT_LEASE_TTL: i64 = 30;

/// Default keep-alive interval: TTL / 3, so we renew well before expiry.
const KEEPALIVE_DIVISOR: u64 = 3;

/// Etcd-based service discovery backend.
///
/// Constructed via [`EtcdServiceDiscoveryBuilder`]. Implements [`ServiceDiscovery`]
/// for discovering service instances via etcd KV prefix scans and watches.
pub struct EtcdServiceDiscovery {
    client: Arc<AsyncMutex<Client>>,
    cluster_id: String,
    lease_ttl: i64,
}

impl EtcdServiceDiscovery {
    /// Register an instance for a named service.
    ///
    /// Creates an etcd lease, writes a key under the service prefix, and spawns
    /// a background keep-alive task. The returned guard revokes the lease on drop
    /// or explicit [`ServiceRegistrationGuard::unregister`].
    pub async fn register_service(
        &self,
        service_name: &str,
        instance_id: InstanceId,
    ) -> Result<EtcdServiceRegistrationGuard> {
        let key = service_key(&self.cluster_id, service_name, instance_id);
        let value = instance_id.to_string();

        let lease_id = {
            let mut client = self.client.lock().await;
            let lease = client
                .lease_grant(self.lease_ttl, None)
                .await
                .context("Failed to grant etcd lease")?;
            let lease_id = lease.id();

            client
                .put(
                    key.as_bytes(),
                    value.as_bytes(),
                    Some(PutOptions::new().with_lease(lease_id)),
                )
                .await
                .context("Failed to put service registration key")?;

            lease_id
        };

        // Spawn keep-alive task
        let cancel = CancellationToken::new();
        let keepalive_interval =
            Duration::from_secs((self.lease_ttl as u64 / KEEPALIVE_DIVISOR).max(1));
        let client = self.client.clone();
        let cancel_clone = cancel.clone();

        tokio::spawn(async move {
            run_keepalive_loop(client, lease_id, keepalive_interval, cancel_clone).await;
        });

        Ok(EtcdServiceRegistrationGuard {
            client: self.client.clone(),
            lease_id,
            cancel,
        })
    }

    /// Parse instance IDs from etcd KV pairs under a service prefix.
    fn parse_instances_from_kvs(kvs: &[etcd_client::KeyValue], prefix: &str) -> Vec<InstanceId> {
        kvs.iter()
            .filter_map(|kv| {
                let key_str = kv.key_str().ok()?;
                let id_str = key_str.strip_prefix(prefix)?;
                uuid::Uuid::parse_str(id_str).ok().map(InstanceId::from)
            })
            .collect()
    }
}

impl ServiceDiscovery for EtcdServiceDiscovery {
    fn list_services(&self) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            let prefix = services_prefix(&self.cluster_id);
            let mut client = self.client.lock().await;
            let resp = client
                .get(prefix.as_bytes(), Some(GetOptions::new().with_prefix()))
                .await
                .context("Failed to list services from etcd")?;

            let mut service_names: Vec<String> = resp
                .kvs()
                .iter()
                .filter_map(|kv| {
                    let key_str = kv.key_str().ok()?;
                    // Key format: /velo/{cluster_id}/services/{service_name}/{instance_id}
                    let suffix = key_str.strip_prefix(&prefix)?;
                    let service_name = suffix.split('/').next()?;
                    Some(service_name.to_string())
                })
                .collect();

            service_names.sort();
            service_names.dedup();
            Ok(service_names)
        })
    }

    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, Result<Vec<InstanceId>>> {
        let prefix = service_prefix(&self.cluster_id, service_name);
        Box::pin(async move {
            let mut client = self.client.lock().await;
            let resp = client
                .get(prefix.as_bytes(), Some(GetOptions::new().with_prefix()))
                .await
                .context("Failed to get service instances from etcd")?;

            Ok(Self::parse_instances_from_kvs(resp.kvs(), &prefix))
        })
    }

    fn watch_instances(
        &self,
        service_name: &str,
    ) -> BoxFuture<'_, Result<Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>>> {
        let prefix = service_prefix(&self.cluster_id, service_name);
        let client = self.client.clone();

        Box::pin(async move {
            // Get initial snapshot and capture the revision for gap-free watching.
            // The revision from the GET response header tells us the exact point-in-time
            // of the snapshot. Starting the watch at revision+1 ensures we don't miss
            // events between the GET and the watch, and don't re-process existing keys.
            let (initial, start_revision) = {
                let mut c = client.lock().await;
                let resp = c
                    .get(prefix.as_bytes(), Some(GetOptions::new().with_prefix()))
                    .await
                    .context("Failed to get initial service instances")?;
                let revision = resp.header().map(|h| h.revision()).unwrap_or(0);
                (
                    Self::parse_instances_from_kvs(resp.kvs(), &prefix),
                    revision,
                )
            };

            // Start watch at revision+1 so we only see new changes
            let (_watcher, watch_stream) = {
                let mut c = client.lock().await;
                c.watch(
                    prefix.as_bytes(),
                    Some(
                        WatchOptions::new()
                            .with_prefix()
                            .with_start_revision(start_revision + 1),
                    ),
                )
                .await
                .context("Failed to start etcd watch")?
            };

            let prefix_owned = prefix;
            let stream = async_stream::stream! {
                yield ServiceEvent::Initial(initial);

                let mut watch_stream = watch_stream;
                loop {
                    match watch_stream.message().await {
                        Ok(Some(resp)) => {
                            for event in resp.events() {
                                if let Some(kv) = event.kv()
                                    && let Ok(key_str) = kv.key_str()
                                    && let Some(id_str) = key_str.strip_prefix(&prefix_owned)
                                    && let Ok(uuid) = uuid::Uuid::parse_str(id_str)
                                {
                                    let instance_id = InstanceId::from(uuid);
                                    match event.event_type() {
                                        EventType::Put => {
                                            yield ServiceEvent::Added(instance_id);
                                        }
                                        EventType::Delete => {
                                            yield ServiceEvent::Removed(instance_id);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::warn!(prefix = %prefix_owned, "etcd watch stream closed by server");
                            yield ServiceEvent::Disconnected;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(prefix = %prefix_owned, error = %e, "etcd watch stream error");
                            yield ServiceEvent::Disconnected;
                            break;
                        }
                    }
                }
            };

            Ok(Box::pin(stream) as Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>)
        })
    }
}

/// Builder for [`EtcdServiceDiscovery`].
pub struct EtcdServiceDiscoveryBuilder {
    client: Client,
    cluster_id: String,
    lease_ttl: i64,
}

impl EtcdServiceDiscoveryBuilder {
    /// Create a new builder with the given etcd client and cluster ID.
    pub fn new(client: Client, cluster_id: impl Into<String>) -> Self {
        Self {
            client,
            cluster_id: cluster_id.into(),
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }

    /// Set the lease TTL in seconds (default: 30).
    pub fn lease_ttl(mut self, ttl: i64) -> Self {
        self.lease_ttl = ttl;
        self
    }

    /// Consume the builder and produce an [`EtcdServiceDiscovery`].
    pub fn build(self) -> EtcdServiceDiscovery {
        EtcdServiceDiscovery {
            client: Arc::new(AsyncMutex::new(self.client)),
            cluster_id: self.cluster_id,
            lease_ttl: self.lease_ttl,
        }
    }
}

/// RAII guard that revokes an etcd lease when dropped.
///
/// Obtained from [`EtcdServiceDiscovery::register_service`]. When the lease is
/// revoked, etcd automatically deletes the associated key, removing the service
/// registration.
pub struct EtcdServiceRegistrationGuard {
    client: Arc<AsyncMutex<Client>>,
    lease_id: i64,
    cancel: CancellationToken,
}

impl ServiceRegistrationGuard for EtcdServiceRegistrationGuard {
    fn unregister(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.cancel.cancel();
            let mut client = self.client.lock().await;
            client
                .lease_revoke(self.lease_id)
                .await
                .context("Failed to revoke etcd lease")?;
            Ok(())
        })
    }
}

impl Drop for EtcdServiceRegistrationGuard {
    fn drop(&mut self) {
        // Cancel keep-alive task. The lease will expire via TTL as a safety net.
        self.cancel.cancel();

        // Best-effort async lease revocation.
        let client = self.client.clone();
        let lease_id = self.lease_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut client = client.lock().await;
                let _ = client.lease_revoke(lease_id).await;
            });
        }
    }
}

/// Background task that sends periodic lease keep-alive messages.
async fn run_keepalive_loop(
    client: Arc<AsyncMutex<Client>>,
    lease_id: i64,
    interval: Duration,
    cancel: CancellationToken,
) {
    let (mut keeper, mut stream) = {
        let mut c = client.lock().await;
        match c.lease_keep_alive(lease_id).await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(lease_id, error = %e, "Failed to start lease keep-alive");
                return;
            }
        }
    };

    let mut tick = tokio::time::interval(interval);
    // Skip immediate first tick
    tick.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!(lease_id, "Keep-alive cancelled");
                break;
            }
            _ = tick.tick() => {
                if let Err(e) = keeper.keep_alive().await {
                    tracing::warn!(lease_id, error = %e, "Lease keep-alive failed");
                    break;
                }
                // Drain the response to avoid backpressure
                let _ = stream.message().await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Prefix for all services in a cluster: `/velo/{cluster_id}/services/`
fn services_prefix(cluster_id: &str) -> String {
    format!("/velo/{cluster_id}/services/")
}

/// Prefix for a specific service: `/velo/{cluster_id}/services/{service_name}/`
fn service_prefix(cluster_id: &str, service_name: &str) -> String {
    format!("/velo/{cluster_id}/services/{service_name}/")
}

/// Full key for a service instance: `/velo/{cluster_id}/services/{service_name}/{instance_id}`
fn service_key(cluster_id: &str, service_name: &str, instance_id: InstanceId) -> String {
    format!("/velo/{cluster_id}/services/{service_name}/{instance_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_key_format() {
        let id = InstanceId::new_v4();
        let key = service_key("my-cluster", "rhino-router", id);
        assert!(key.starts_with("/velo/my-cluster/services/rhino-router/"));
        assert!(key.ends_with(&id.to_string()));
    }

    #[test]
    fn test_service_prefix_format() {
        let prefix = service_prefix("prod", "data-loader");
        assert_eq!(prefix, "/velo/prod/services/data-loader/");
    }

    #[test]
    fn test_services_prefix_format() {
        let prefix = services_prefix("prod");
        assert_eq!(prefix, "/velo/prod/services/");
    }

    #[test]
    fn test_cluster_isolation() {
        let id = InstanceId::new_v4();
        let key1 = service_key("cluster-a", "svc", id);
        let key2 = service_key("cluster-b", "svc", id);
        assert_ne!(key1, key2, "Different clusters must produce different keys");
    }
}
