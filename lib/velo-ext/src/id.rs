// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Identity, address, and transport-key types referenced by the extension
//! traits. Anything that appears in a [`Transport`](crate::transport::Transport),
//! [`PeerDiscovery`](crate::discovery::PeerDiscovery), or
//! [`ServiceDiscovery`](crate::discovery::ServiceDiscovery) signature lives
//! here.

mod address;
mod identity;
mod transport;

pub use address::{PeerInfo, WorkerAddress, WorkerAddressError};
pub use identity::{InstanceId, WorkerId};
pub use transport::TransportKey;
