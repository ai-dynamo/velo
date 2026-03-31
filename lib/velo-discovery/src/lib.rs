// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Discovery traits for abstracting peer and service discovery backends.

pub mod filesystem;
pub use filesystem::{FilesystemPeerDiscovery, FilesystemRegistrationGuard};

pub mod filesystem_service;
pub use filesystem_service::{FilesystemServiceDiscovery, FilesystemServiceRegistrationGuard};

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(feature = "etcd")]
pub mod etcd;

use std::pin::Pin;

use anyhow::Result;
use futures::Stream;
use futures::future::BoxFuture;
use velo_common::{InstanceId, PeerInfo, WorkerId};

// ---------------------------------------------------------------------------
// Peer discovery
// ---------------------------------------------------------------------------

/// Abstraction over peer discovery mechanisms.
///
/// Higher-level crates implement this trait to integrate with concrete discovery
/// backends (e.g., etcd, consul) without pulling those dependencies into the
/// messenger layer.
pub trait PeerDiscovery: Send + Sync {
    /// Discover a peer by its worker ID.
    fn discover_by_worker_id(&self, worker_id: WorkerId) -> BoxFuture<'_, Result<PeerInfo>>;

    /// Discover a peer by its instance ID.
    fn discover_by_instance_id(&self, instance_id: InstanceId) -> BoxFuture<'_, Result<PeerInfo>>;
}

/// RAII guard that unregisters a peer when dropped or explicitly unregistered.
///
/// Backends implement this trait to provide async cleanup via `unregister()`.
/// Callers that can await should prefer `unregister()` over relying on `Drop`.
pub trait PeerRegistrationGuard: Send {
    /// Explicitly unregister and clean up resources.
    fn unregister(&mut self) -> BoxFuture<'_, Result<()>>;
}

/// Deprecated: Use [`PeerRegistrationGuard`] instead.
#[deprecated(note = "Renamed to PeerRegistrationGuard")]
pub type RegistrationGuard = dyn PeerRegistrationGuard;

// ---------------------------------------------------------------------------
// Service discovery
// ---------------------------------------------------------------------------

/// Event emitted by a service instance watch stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceEvent {
    /// Initial snapshot of all known instances for the service.
    Initial(Vec<InstanceId>),
    /// A new instance registered for the service.
    Added(InstanceId),
    /// An instance was removed from the service.
    Removed(InstanceId),
}

/// Abstraction over service discovery mechanisms.
///
/// Maps named services to sets of [`InstanceId`]s. For example, finding all
/// instances that expose a "rhino-router" service. Clients should treat results
/// as best-effort — returned instance IDs may refer to instances that have
/// already departed. The caller is responsible for handling failures when
/// communicating with discovered instances.
pub trait ServiceDiscovery: Send + Sync {
    /// List all service names that have at least one registered instance.
    fn list_services(&self) -> BoxFuture<'_, Result<Vec<String>>>;

    /// Get all instances currently registered for a service.
    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, Result<Vec<InstanceId>>>;

    /// Watch for changes to instances registered for a service.
    ///
    /// The stream emits an [`ServiceEvent::Initial`] event with the current set
    /// of instances, followed by [`ServiceEvent::Added`] / [`ServiceEvent::Removed`]
    /// events as instances come and go.
    fn watch_instances(
        &self,
        service_name: &str,
    ) -> BoxFuture<'_, Result<Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>>>;
}

/// RAII guard that unregisters a service instance when dropped or explicitly unregistered.
///
/// Backends implement this trait to provide async cleanup via `unregister()`.
/// Callers that can await should prefer `unregister()` over relying on `Drop`.
pub trait ServiceRegistrationGuard: Send {
    /// Explicitly unregister and clean up resources.
    fn unregister(&mut self) -> BoxFuture<'_, Result<()>>;
}
