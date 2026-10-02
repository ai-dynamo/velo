// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Discovery backends. Trait definitions live in [`velo_ext::discovery`] and
//! are re-exported here for backwards compatibility while the workspace
//! migrates to the two-crate (`velo` + `velo-ext`) layout.

#[cfg(feature = "services")]
pub mod filesystem;
#[cfg(feature = "services")]
pub use filesystem::{FilesystemPeerDiscovery, FilesystemRegistrationGuard};

#[cfg(feature = "services")]
pub mod filesystem_service;
#[cfg(feature = "services")]
pub use filesystem_service::{FilesystemServiceDiscovery, FilesystemServiceRegistrationGuard};

#[cfg(feature = "nats-discovery")]
pub mod nats;

#[cfg(feature = "etcd")]
pub mod etcd;

pub use velo_ext::discovery::{
    PeerDiscovery, PeerRegistrationGuard, ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard,
};
