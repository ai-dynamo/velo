// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Velo's NIXL integration layer (Phase 2).
//!
//! Thin wrapper around `nixl-sys` that bundles the boilerplate every velo
//! caller would otherwise reproduce:
//!
//! - [`agent_with_ucx`] — create a `nixl_sys::Agent`, instantiate the UCX
//!   backend, and return a pre-built [`OptArgs`] that already references the
//!   backend (needed for `register_memory` / `create_xfer_req`).
//! - [`wait_xfer`] — async-friendly poll loop on a posted transfer request.
//!
//! Everything else is a re-export from `nixl-sys` so callers don't need to
//! depend on it directly.

pub use nixl_sys::{
    Agent, Backend, MemType, MemoryRegion, NixlDescriptor, NixlError, NixlRegistration, OptArgs,
    RegDescList, RegistrationHandle, SystemStorage, XferDescList, XferOp, XferRequest, XferStatus,
};

use std::time::Duration;

/// Errors that can arise while setting up or waiting for a NIXL transfer.
#[derive(Debug, thiserror::Error)]
pub enum VeloNixlError {
    #[error("nixl error: {0}")]
    Nixl(#[from] NixlError),
    #[error("transfer timed out after {0:?}")]
    Timeout(Duration),
}

/// Create a NIXL agent with the UCX backend pre-attached.
///
/// Returns the agent, the UCX backend handle (kept alive by the agent
/// internally — but exposed here in case the caller wants it directly), and
/// an [`OptArgs`] with the backend already added. Pass `Some(&opt_args)` to
/// `register_memory` / `create_xfer_req` to ensure UCX is selected.
pub fn agent_with_ucx(name: &str) -> Result<(Agent, Backend, OptArgs), VeloNixlError> {
    tracing::trace!(agent.name = %name, "Creating NIXL agent with UCX backend");
    let agent = Agent::new(name)?;
    let (_mems, params) = agent.get_plugin_params("UCX")?;
    let backend = agent.create_backend("UCX", &params)?;
    let mut opt_args = OptArgs::new()?;
    opt_args.add_backend(&backend)?;
    Ok((agent, backend, opt_args))
}

/// Default timeout for `wait_xfer`. CMA same-node transfers complete in
/// microseconds, so 30s is a generous upper bound that still catches hangs.
pub const DEFAULT_XFER_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll the status of a posted transfer request until it succeeds.
///
/// Yields to the tokio scheduler between polls so we don't burn a worker
/// thread. UCX/CMA same-node transfers usually complete on the first poll.
pub async fn wait_xfer(agent: &Agent, req: &XferRequest) -> Result<(), VeloNixlError> {
    wait_xfer_with_timeout(agent, req, DEFAULT_XFER_TIMEOUT).await
}

/// Like [`wait_xfer`] but with a caller-supplied timeout.
pub async fn wait_xfer_with_timeout(
    agent: &Agent,
    req: &XferRequest,
    timeout: Duration,
) -> Result<(), VeloNixlError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match agent.get_xfer_status(req)? {
            XferStatus::Success => return Ok(()),
            XferStatus::InProgress => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(VeloNixlError::Timeout(timeout));
                }
                tokio::task::yield_now().await;
            }
        }
    }
}
