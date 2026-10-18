// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::{Result, WorkerAddress};

/// A peer's location before its instance identity is known.
///
/// Endpoint connect learns a self-reported identity; it does not authenticate it.
#[derive(Debug, Clone)]
pub enum Endpoint {
    /// A TCP socket address using the default `tcp` transport key.
    Tcp(std::net::SocketAddr),
    /// A Unix socket path using the default `uds` transport key.
    #[cfg(unix)]
    Uds(std::path::PathBuf),
}

impl Endpoint {
    pub(crate) fn worker_address(&self) -> Result<WorkerAddress> {
        match self {
            Self::Tcp(address) => crate::transports::tcp::TcpTransport::worker_address(*address),
            #[cfg(unix)]
            Self::Uds(path) => crate::transports::uds::UdsTransport::worker_address(path),
        }
    }
}
