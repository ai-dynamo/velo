// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Filesystem-based service discovery for Velo distributed systems.
//!
//! Stores service-to-instance mappings in a JSON file on disk. Suitable for
//! development, testing, and lightweight single-host deployments.
//!
//! # File Format
//!
//! ```json
//! {
//!   "services": {
//!     "rhino-router": ["uuid-1", "uuid-2"],
//!     "data-loader": ["uuid-3"]
//!   }
//! }
//! ```
//!
//! # Watch
//!
//! [`ServiceDiscovery::watch_instances`] uses polling with a configurable
//! interval to detect changes on disk.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use fs4::fs_std::FileExt;
use futures::Stream;
use futures::future::BoxFuture;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use velo_ext::InstanceId;

use super::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};

/// Default polling interval for watch streams.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Filesystem-based service discovery backend.
///
/// Stores service registrations in a JSON file. Provides simple persistence
/// for testing scenarios where external dependencies (etcd) are not desired.
#[derive(Clone)]
pub struct FilesystemServiceDiscovery {
    file_path: PathBuf,
    inner: Arc<RwLock<ServiceRegistry>>,
    write_mutex: Arc<AsyncMutex<()>>,
    poll_interval: Duration,
}

impl std::fmt::Debug for FilesystemServiceDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilesystemServiceDiscovery")
            .field("file_path", &self.file_path)
            .finish()
    }
}

/// In-memory service registry.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ServiceRegistry {
    services: HashMap<String, Vec<String>>,
}

impl FilesystemServiceDiscovery {
    /// Create a new filesystem-based service discovery at the specified path.
    pub fn new(file_path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            file_path: file_path.into(),
            inner: Arc::new(RwLock::new(ServiceRegistry::default())),
            write_mutex: Arc::new(AsyncMutex::new(())),
            poll_interval: DEFAULT_POLL_INTERVAL,
        })
    }

    /// Create a new instance in a temporary directory. Useful for testing.
    pub fn new_temp() -> Result<Self> {
        let temp_dir = std::env::temp_dir();
        let file_name = format!("velo-service-discovery-{}.json", uuid::Uuid::new_v4());
        Self::new(temp_dir.join(file_name))
    }

    /// Set the polling interval for watch streams.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Register an instance for a named service.
    pub async fn register_service(
        &self,
        service_name: &str,
        instance_id: InstanceId,
    ) -> Result<FilesystemServiceRegistrationGuard> {
        let _guard = self.write_mutex.lock().await;
        self.load_from_disk().await?;

        {
            let mut state = self.inner.write();
            let instances = state.services.entry(service_name.to_string()).or_default();
            let id_str = instance_id.to_string();
            if instances.contains(&id_str) {
                anyhow::bail!(
                    "Instance {} is already registered for service '{}'",
                    instance_id,
                    service_name
                );
            }
            instances.push(id_str);
        }

        self.save_to_disk().await?;
        Ok(FilesystemServiceRegistrationGuard::new(
            self.clone(),
            service_name.to_string(),
            instance_id,
        ))
    }

    /// Unregister an instance from a named service.
    async fn unregister_service(&self, service_name: &str, instance_id: InstanceId) -> Result<()> {
        let _guard = self.write_mutex.lock().await;
        self.load_from_disk().await?;

        {
            let mut state = self.inner.write();
            let id_str = instance_id.to_string();
            if let Some(instances) = state.services.get_mut(service_name) {
                instances.retain(|id| id != &id_str);
                if instances.is_empty() {
                    state.services.remove(service_name);
                }
            }
        }

        self.save_to_disk().await?;
        Ok(())
    }

    /// Load the service registry from disk with shared file locking.
    async fn load_from_disk(&self) -> Result<()> {
        if !self.file_path.exists() {
            return Ok(());
        }

        let file_path = self.file_path.clone();
        let content = tokio::task::spawn_blocking(move || -> Result<String> {
            let mut file =
                std::fs::File::open(&file_path).context("Failed to open service discovery file")?;
            file.lock_shared()
                .context("Failed to acquire shared lock")?;
            let mut content = String::new();
            std::io::Read::read_to_string(&mut file, &mut content)
                .context("Failed to read service discovery file")?;
            Ok(content)
        })
        .await
        .context("spawn_blocking failed")??;

        let registry: ServiceRegistry =
            serde_json::from_str(&content).context("Failed to parse service discovery file")?;

        *self.inner.write() = registry;
        Ok(())
    }

    /// Save the service registry to disk with atomic write.
    async fn save_to_disk(&self) -> Result<()> {
        let content = {
            let inner = self.inner.read();
            serde_json::to_string_pretty(&*inner).context("Failed to serialize service registry")?
        };

        let file_path = self.file_path.clone();
        let temp_file_name = format!(
            "{}.tmp.{}",
            self.file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy(),
            uuid::Uuid::new_v4()
        );
        let temp_path = self
            .file_path
            .parent()
            .map(|p| p.join(&temp_file_name))
            .unwrap_or_else(|| PathBuf::from(&temp_file_name));

        tokio::task::spawn_blocking(move || -> Result<()> {
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            std::fs::write(&temp_path, &content).context("Failed to write temp file")?;

            let file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)
                .context("Failed to open service discovery file")?;
            file.lock_exclusive()
                .context("Failed to acquire exclusive lock")?;

            std::fs::rename(&temp_path, &file_path).context("Failed to rename file")?;
            Ok(())
        })
        .await
        .context("spawn_blocking failed")??;

        Ok(())
    }

    /// Parse instance IDs for a given service from the in-memory registry.
    fn parse_instances(state: &ServiceRegistry, service_name: &str) -> Vec<InstanceId> {
        state
            .services
            .get(service_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id_str| uuid::Uuid::parse_str(id_str).ok().map(InstanceId::from))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl ServiceDiscovery for FilesystemServiceDiscovery {
    fn list_services(&self) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            self.load_from_disk().await?;
            let state = self.inner.read();
            Ok(state
                .services
                .keys()
                .filter(|k| state.services.get(*k).is_some_and(|v| !v.is_empty()))
                .cloned()
                .collect())
        })
    }

    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, Result<Vec<InstanceId>>> {
        let service_name = service_name.to_string();
        Box::pin(async move {
            self.load_from_disk().await?;
            let state = self.inner.read();
            Ok(Self::parse_instances(&state, &service_name))
        })
    }

    fn watch_instances(
        &self,
        service_name: &str,
    ) -> BoxFuture<'_, Result<Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>>> {
        let service_name = service_name.to_string();
        let discovery = self.clone();
        let poll_interval = self.poll_interval;

        Box::pin(async move {
            // Load initial state
            discovery.load_from_disk().await?;
            let initial = {
                let state = discovery.inner.read();
                Self::parse_instances(&state, &service_name)
            };

            let stream = async_stream::stream! {
                let mut known: HashSet<InstanceId> = initial.iter().copied().collect();
                yield ServiceEvent::Initial(initial);

                let mut interval = tokio::time::interval(poll_interval);
                // Skip the first tick (fires immediately)
                interval.tick().await;

                loop {
                    interval.tick().await;

                    if let Err(e) = discovery.load_from_disk().await {
                        tracing::warn!(error = %e, "Failed to reload service discovery file");
                        continue;
                    }

                    let current: HashSet<InstanceId> = {
                        let state = discovery.inner.read();
                        Self::parse_instances(&state, &service_name)
                            .into_iter()
                            .collect()
                    };

                    // Detect added instances
                    for &id in &current {
                        if !known.contains(&id) {
                            yield ServiceEvent::Added(id);
                        }
                    }

                    // Detect removed instances
                    for &id in &known {
                        if !current.contains(&id) {
                            yield ServiceEvent::Removed(id);
                        }
                    }

                    known = current;
                }

                // Unreachable: the loop above runs until the stream is dropped.
                // This explicit yield satisfies async_stream's type inference.
                #[allow(unreachable_code)]
                {
                    yield ServiceEvent::Initial(vec![]);
                }
            };

            Ok(Box::pin(stream) as Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>)
        })
    }
}

/// RAII guard that unregisters a service instance when dropped.
#[derive(Debug)]
pub struct FilesystemServiceRegistrationGuard {
    discovery: FilesystemServiceDiscovery,
    registrations: Vec<(String, InstanceId)>,
}

impl FilesystemServiceRegistrationGuard {
    fn new(
        discovery: FilesystemServiceDiscovery,
        service_name: String,
        instance_id: InstanceId,
    ) -> Self {
        Self {
            discovery,
            registrations: vec![(service_name, instance_id)],
        }
    }
}

impl ServiceRegistrationGuard for FilesystemServiceRegistrationGuard {
    fn unregister(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let registrations = std::mem::take(&mut self.registrations);
            for (service_name, instance_id) in registrations {
                self.discovery
                    .unregister_service(&service_name, instance_id)
                    .await?;
            }
            Ok(())
        })
    }
}

impl Drop for FilesystemServiceRegistrationGuard {
    fn drop(&mut self) {
        let discovery = self.discovery.clone();
        let registrations = std::mem::take(&mut self.registrations);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for (service_name, instance_id) in registrations {
                    let _ = discovery
                        .unregister_service(&service_name, instance_id)
                        .await;
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_register_and_get_instances() {
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let id1 = InstanceId::new_v4();
        let id2 = InstanceId::new_v4();

        let _g1 = discovery.register_service("my-service", id1).await.unwrap();
        let _g2 = discovery.register_service("my-service", id2).await.unwrap();

        let instances = discovery.get_instances("my-service").await.unwrap();
        assert_eq!(instances.len(), 2);
        assert!(instances.contains(&id1));
        assert!(instances.contains(&id2));
    }

    #[tokio::test]
    async fn test_list_services() {
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let id = InstanceId::new_v4();

        let _g1 = discovery.register_service("svc-a", id).await.unwrap();
        let _g2 = discovery
            .register_service("svc-b", InstanceId::new_v4())
            .await
            .unwrap();

        let mut services = discovery.list_services().await.unwrap();
        services.sort();
        assert_eq!(services, vec!["svc-a", "svc-b"]);
    }

    #[tokio::test]
    async fn test_unregister() {
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let id = InstanceId::new_v4();

        let mut guard = discovery.register_service("svc", id).await.unwrap();
        assert_eq!(discovery.get_instances("svc").await.unwrap().len(), 1);

        guard.unregister().await.unwrap();
        assert!(discovery.get_instances("svc").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_duplicate_registration_fails() {
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let id = InstanceId::new_v4();

        let _g = discovery.register_service("svc", id).await.unwrap();
        let result = discovery.register_service("svc", id).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already registered")
        );
    }

    #[tokio::test]
    async fn test_empty_service_returns_empty() {
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let instances = discovery.get_instances("nonexistent").await.unwrap();
        assert!(instances.is_empty());
    }

    #[tokio::test]
    async fn test_watch_initial_event() {
        let discovery = FilesystemServiceDiscovery::new_temp()
            .unwrap()
            .with_poll_interval(Duration::from_millis(50));
        let id = InstanceId::new_v4();
        let _g = discovery.register_service("svc", id).await.unwrap();

        let mut stream = discovery.watch_instances("svc").await.unwrap();
        let event = stream.next().await.unwrap();
        assert_eq!(event, ServiceEvent::Initial(vec![id]));
    }

    #[tokio::test]
    async fn test_watch_added_removed() {
        let discovery = FilesystemServiceDiscovery::new_temp()
            .unwrap()
            .with_poll_interval(Duration::from_millis(50));

        let mut stream = discovery.watch_instances("svc").await.unwrap();

        // Initial (empty)
        let event = stream.next().await.unwrap();
        assert_eq!(event, ServiceEvent::Initial(vec![]));

        // Register → should see Added
        let id = InstanceId::new_v4();
        let mut guard = discovery.register_service("svc", id).await.unwrap();

        let event = stream.next().await.unwrap();
        assert_eq!(event, ServiceEvent::Added(id));

        // Unregister → should see Removed
        guard.unregister().await.unwrap();
        let event = stream.next().await.unwrap();
        assert_eq!(event, ServiceEvent::Removed(id));
    }

    #[tokio::test]
    async fn test_persistence_across_instances() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!(
            "test-velo-service-discovery-{}.json",
            uuid::Uuid::new_v4()
        ));

        let id = InstanceId::new_v4();
        let _guard;

        {
            let discovery = FilesystemServiceDiscovery::new(&file_path).unwrap();
            _guard = discovery.register_service("svc", id).await.unwrap();
        }

        {
            let discovery = FilesystemServiceDiscovery::new(&file_path).unwrap();
            let instances = discovery.get_instances("svc").await.unwrap();
            assert_eq!(instances, vec![id]);
        }

        let _ = std::fs::remove_file(&file_path);
    }

    #[tokio::test]
    async fn test_service_registration_guard_trait() {
        use crate::discovery::ServiceRegistrationGuard as _;
        let discovery = FilesystemServiceDiscovery::new_temp().unwrap();
        let id = InstanceId::new_v4();

        let mut guard = discovery.register_service("svc", id).await.unwrap();
        assert_eq!(discovery.get_instances("svc").await.unwrap().len(), 1);

        guard.unregister().await.unwrap();
        assert!(discovery.get_instances("svc").await.unwrap().is_empty());
    }
}
