// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The UCX backend: [`RdmaBackend`] over the UCX transport's RMA plumbing.
//!
//! This is the only file of the registration layer that names UCX. It is
//! compiled with the `ucx` feature; the rest of the layer is compiled when any
//! RDMA backend is.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::backend::{BackendGet, BackendRegion, RdmaBackend, RdmaError};

/// [`RdmaBackend`] over the UCX transport's Phase-1 RMA plumbing.
///
/// Holds an [`RdmaEndpoint`](crate::transports::ucx::rma::RdmaEndpoint), which
/// is two `Arc`s and no UCX state — every `ucp_*` handle stays on the progress
/// thread. Constructing one before
/// [`Transport::start`](velo_ext::Transport::start) has resolved is harmless:
/// every method answers `NotStarted` until the transport marks itself started,
/// which this projects to [`RdmaError::Backend`].
pub(crate) struct UcxBackend {
    endpoint: crate::transports::ucx::rma::RdmaEndpoint,
}

impl UcxBackend {
    /// Wrap a transport's RMA endpoint.
    pub(crate) fn new(endpoint: crate::transports::ucx::rma::RdmaEndpoint) -> Arc<Self> {
        Arc::new(Self { endpoint })
    }
}

impl RdmaBackend for UcxBackend {
    fn key(&self) -> &str {
        "ucx"
    }

    fn map(&self, ptr: usize, len: usize) -> BoxFuture<'_, Result<BackendRegion, RdmaError>> {
        Box::pin(async move {
            let region = self
                .endpoint
                .map_region(ptr, len)
                .await
                .map_err(rma_error)?;
            Ok(BackendRegion {
                backend_region_id: region.region_id,
                effective_addr: region.effective_addr,
                effective_len: region.effective_len,
                packed_key: region.packed_rkey,
            })
        })
    }

    fn unmap(&self, backend_region_id: u64) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move {
            self.endpoint
                .unmap_region(backend_region_id)
                .await
                .map_err(rma_error)
        })
    }

    /// The progress thread's own region count.
    ///
    /// Decremented on unmap *attempt*, not on success — see the trait docs.
    /// Sound as a precondition on UCX 1.22, where `ucp_mem_unmap` cannot fail
    /// for a handle UCX itself issued; a UCX bump has to re-check that.
    fn live_registrations(&self) -> Option<usize> {
        Some(self.endpoint.live_regions())
    }

    fn get(&self, req: BackendGet) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move {
            self.endpoint
                .get(crate::transports::ucx::rma::RmaGetRequest {
                    peer: req.peer,
                    remote_addr: req.remote_addr,
                    packed_rkey: req.packed_key,
                    local_region: req.local_region_id,
                    local_offset: req.local_offset,
                    len: req.len,
                })
                .await
                .map_err(rma_error)
        })
    }
}

/// Project a UCX-level RMA failure onto the backend-agnostic error.
///
/// The three "the plumbing is not there" conditions — not started, no progress
/// thread, a `ucp_*` call failing — all collapse into
/// [`RdmaError::Backend`], because nothing above this trait can act on the
/// difference. The message keeps the distinction so a human can.
fn rma_error(e: crate::transports::ucx::rma::RmaError) -> RdmaError {
    use crate::transports::ucx::rma::RmaError as E;
    match e {
        E::ShuttingDown => RdmaError::ShuttingDown,
        E::OutOfRange => RdmaError::OutOfRange,
        E::RegionNotFound => RdmaError::RegionNotFound,
        E::InvalidRkey => RdmaError::InvalidKey,
        other => RdmaError::Backend(format!("ucx: {other}")),
    }
}
