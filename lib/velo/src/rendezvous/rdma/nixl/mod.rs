// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The NIXL backend: [`RdmaBackend`] over a NIXL agent.
//!
//! NIXL is not a messenger transport here. It only moves rendezvous payloads:
//! the owner registers memory with its agent, the descriptor carries what a
//! consumer's agent needs to read it, and the consumer posts a NIXL READ. The
//! control plane stays on whatever transports the instance has.
//!
//! [`RdmaBackend`]: super::RdmaBackend

mod agent;
mod backend;
mod driver;
#[cfg(any(test, feature = "test-helpers"))]
mod fake;
mod key;
mod peers;
mod sys;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use velo_ext::InstanceId;

use super::{RdmaBackend, RdmaError};

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::NixlLoopback;

/// Tuning for the NIXL backend. Pass it to `VeloBuilder::add_nixl_rdma`.
#[derive(Debug, Clone)]
pub struct NixlRdmaConfig {
    /// UCX workers inside the NIXL agent. Each READ is pinned to one of them,
    /// in turn, so concurrent READs spread over the workers. Each worker makes
    /// its own connection to a peer on first use.
    pub num_workers: u32,
    /// Network devices for NIXL's UCX plugin, as UCX names them
    /// (`mlx5_0:1,mlx5_1:1`). Empty: NIXL's default.
    pub devices: String,
    /// How long the driver thread spins after it posts a READ, or sees one
    /// finish, before it sleeps between polls. A READ that outlasts the spin
    /// is noticed up to 1 ms late.
    pub spin: Duration,
}

impl Default for NixlRdmaConfig {
    fn default() -> Self {
        Self {
            num_workers: 1,
            devices: String::new(),
            spin: driver::DEFAULT_SPIN,
        }
    }
}

impl NixlRdmaConfig {
    fn driver(&self) -> driver::DriverConfig {
        driver::DriverConfig {
            workers: self.num_workers.max(1) as usize,
            spin: self.spin,
            ..Default::default()
        }
    }
}

/// Which agent the backend runs on.
pub(crate) enum NixlSetup {
    /// A real NIXL agent.
    Real(NixlRdmaConfig),
    /// An agent that lives in this process, joined to others over `net`.
    #[cfg(any(test, feature = "test-helpers"))]
    Loopback(NixlLoopback, NixlRdmaConfig),
}

/// Start a NIXL backend for the instance `instance`.
pub(crate) async fn start(
    setup: NixlSetup,
    instance: InstanceId,
) -> Result<Arc<dyn RdmaBackend>, RdmaError> {
    let name = key::agent_name(instance);
    let backend = match setup {
        NixlSetup::Real(cfg) => {
            // `nixl-sys` is built as a stub that loads `libnixl_capi.so` at run
            // time. With no library its first real call aborts the process,
            // and only `is_stub` answers without one. So ask first.
            if nixl_sys::is_stub() {
                return Err(RdmaError::Backend(
                    "nixl: libnixl_capi.so was not found. Put the NIXL library directory on \
                     the loader path (LD_LIBRARY_PATH) and set NIXL_PLUGIN_DIR."
                        .to_string(),
                ));
            }
            let driver = cfg.driver();
            backend::NixlBackend::start(move || sys::SysAgent::create(&name, &cfg), driver).await?
        }
        #[cfg(any(test, feature = "test-helpers"))]
        NixlSetup::Loopback(net, cfg) => {
            let make = move || Ok(fake::FakeAgent::new(&name, net));
            backend::NixlBackend::start(make, cfg.driver()).await?
        }
    };
    Ok(backend)
}
