// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Peer discovery trait for abstracting discovery backends.

pub mod filesystem;
pub use filesystem::{FilesystemPeerDiscovery, FilesystemRegistrationGuard};

#[cfg(feature = "nats")]
pub mod nats;

use anyhow::Result;
use futures::future::BoxFuture;
use velo_common::{InstanceId, PeerInfo, WorkerId};

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
pub trait RegistrationGuard: Send {
    /// Explicitly unregister and clean up resources.
    fn unregister(&mut self) -> futures::future::BoxFuture<'_, anyhow::Result<()>>;
}
