// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The backend seam: what the registration layer needs from an RDMA provider.
//!
//! [`RdmaBackend`] is the whole of D12. Everything above it — arenas,
//! [`RegionGuard`](super::RegionGuard), the registry — is written against this
//! trait, so a later NIXL or libfabric implementation is an additional impl
//! rather than a reshaping of the registration layer. Nothing backend-specific
//! (a `ucp_mem_h`, a `ucs_memory_type_t`, a NIXL `MemType`) crosses it: what
//! comes back from [`RdmaBackend::map`] is an opaque id, a byte range, and a
//! *packed* key as plain [`Bytes`] — which is exactly what the Phase-3 wire
//! descriptor carries.
//!
//! The trait is `pub(crate)` and lives in `velo`, not `velo-ext` (D10). It is
//! object-safe by returning [`BoxFuture`] rather than using RPITIT, because the
//! registry holds one as `Arc<dyn RdmaBackend>`.
//!
//! # Contract an implementation must honour
//!
//! These are the properties the layers above depend on, stated here so a second
//! backend has something to implement against rather than inferring them from
//! [`UcxBackend`]:
//!
//! * **`map` rolls back if its future is dropped.** A cancelled `map` must
//!   leave no registration behind under the id it was minting. The caller
//!   treats a dropped `map` exactly like a returned error and frees the pages,
//!   so a backend that leaves the range pinned after cancellation turns that
//!   into a use-after-free.
//! * **`unmap` is idempotent, and proceeds once submitted.** An id that names
//!   nothing — never mapped, or already unmapped — answers `Ok(())`. Dropping
//!   the future abandons the notification only.
//! * **`Ok(())` from `unmap` means the pages are no longer pinned**, and is the
//!   only thing that lets the layer above resolve
//!   [`RegionGuard::deregistered`](super::RegionGuard::deregistered). Any error
//!   — [`RdmaError::ShuttingDown`] in particular — means *unknown*, not
//!   *unmapped*.
//! * **`get` keeps the destination region alive for the transfer's duration**,
//!   whether or not anyone is still awaiting the future.

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use velo_ext::InstanceId;

/// Why a registration-layer operation could not be performed.
///
/// Backend-agnostic by construction: the UCX-specific
/// [`RmaError`](crate::transports::ucx::rma::RmaError) is projected onto these
/// variants by [`UcxBackend`], with the original diagnosis preserved in
/// [`RdmaError::Backend`]'s string. Callers switch on the variant; humans read
/// the string.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum RdmaError {
    /// The registry, or the transport under it, is shutting down. New
    /// registrations are refused; an in-progress deregistration reached no
    /// conclusion.
    ///
    /// **Not a statement that memory was unmapped.** A caller holding a
    /// [`RegionGuard`](super::RegionGuard) that sees this must keep its
    /// allocation alive until
    /// [`deregistered()`](super::RegionGuard::deregistered) resolves — which it
    /// will, at the latest when velo shutdown completes.
    #[error("rdma registration layer is shutting down")]
    ShuttingDown,

    /// The registered-bytes budget would be exceeded by this registration.
    ///
    /// Phase 3's callers treat this as "stage chunked instead" — pool
    /// exhaustion falls back to the active-message path and is never a hard
    /// failure of the staging operation (D4).
    #[error(
        "rdma registered-bytes budget exceeded: {requested} B requested, {registered} B registered, {budget} B budget"
    )]
    BudgetExceeded {
        /// Bytes the refused registration asked for.
        requested: u64,
        /// Bytes already registered when the request was refused.
        registered: u64,
        /// The configured ceiling.
        budget: u64,
    },

    /// The named region is not registered — never was, or has already been
    /// deregistered.
    #[error("rdma region not found")]
    RegionNotFound,

    /// A length or offset falls outside the region it names, or the request
    /// was degenerate (null pointer, zero length).
    #[error("rdma range outside the registered region")]
    OutOfRange,

    /// A packed remote key was empty, oversized, or could not be parsed within
    /// its own bytes.
    #[error("malformed packed remote key")]
    InvalidKey,

    /// The backend failed. The string names which backend-level condition it
    /// was, so a mis-ordered construction (an endpoint used before its
    /// transport started, say) is diagnosable from the message alone.
    ///
    /// Transient or environmental by nature — a caller may reasonably retry.
    /// Conditions a retry can never fix have their own variants below, so that
    /// a retry loop written against this one cannot spin forever on a
    /// configuration mistake.
    #[error("rdma backend: {0}")]
    Backend(String),

    /// This instance has no RDMA backend: the UCX transport was never installed
    /// through `VeloBuilder::add_ucx_transport`.
    ///
    /// A deployment fact, not a failure. Retrying cannot change it, and a
    /// caller with a chunked fallback should take it permanently.
    #[error("no rdma backend configured for this instance")]
    NotConfigured,

    /// The region was registered from a caller-owned pointer, so velo has no
    /// buffer to hand back. Retrying cannot change it either.
    #[error("region owns no buffer")]
    NotOwned,

    /// The operation did not finish inside the caller's budget.
    #[error("rdma operation timed out")]
    Timeout,
}

/// One registered range, as the backend reports it.
#[derive(Debug, Clone)]
pub(crate) struct BackendRegion {
    /// Names the registration in a later [`RdmaBackend::unmap`]. Opaque above
    /// this trait.
    pub backend_region_id: u64,
    /// Start of the range the backend actually pinned. May be *below* the
    /// pointer that was mapped — UCX rounds outward to page boundaries — so it
    /// is a fact to report, never a base for offset arithmetic.
    pub effective_addr: u64,
    /// Length of the pinned range; always covers the requested range.
    pub effective_len: u64,
    /// Endpoint-independent key material. Opaque bytes: it may be cached, sent
    /// on the wire, or held anywhere.
    pub packed_key: Bytes,
}

/// One remote read: `len` bytes at `remote_addr` on `peer`, landing at
/// `local_offset` inside a locally registered region.
///
/// Phase 3 builds these from an owner-authored wire descriptor. The consumer
/// never computes `remote_addr` itself — that is the property that makes
/// software-emulated RMA (which validates nothing) safe over `UCX_TLS=tcp`.
#[derive(Debug, Clone)]
pub(crate) struct BackendGet {
    /// Instance to read from.
    pub peer: InstanceId,
    /// Absolute address in the peer's address space, authored by the peer.
    pub remote_addr: u64,
    /// The peer's packed key covering `remote_addr`.
    pub packed_key: Bytes,
    /// Destination region, from a previous [`RdmaBackend::map`] on this side.
    pub local_region_id: u64,
    /// Destination offset, measured from the pointer that was mapped — not
    /// from [`BackendRegion::effective_addr`].
    pub local_offset: u64,
    /// Bytes to read.
    pub len: u64,
}

/// An RDMA provider the registration layer can register memory with and pull
/// remote memory through. See the module docs for the contract.
pub(crate) trait RdmaBackend: Send + Sync {
    /// Wire-level discriminator for this backend (`"ucx"`). Phase 3 puts it in
    /// the descriptor and in the consumer's capability offer.
    fn key(&self) -> &str;

    /// Register `[ptr, ptr + len)` and pack a key for it.
    fn map(&self, ptr: usize, len: usize) -> BoxFuture<'_, Result<BackendRegion, RdmaError>>;

    /// Deregister a region. Idempotent; `Ok(())` means "no longer pinned".
    fn unmap(&self, backend_region_id: u64) -> BoxFuture<'_, Result<(), RdmaError>>;

    /// Read remote memory into a locally registered region.
    fn get(&self, req: BackendGet) -> BoxFuture<'_, Result<(), RdmaError>>;

    /// How many registrations this backend still holds, if it can say.
    ///
    /// Evidence, not bookkeeping: the layer above tracks its own registrations,
    /// and asking it whether they are gone would just be asking it to agree
    /// with itself. This is the backend's own count, and the registration layer
    /// uses it as the precondition for declaring memory released at the end of
    /// shutdown — the one moment it makes that claim without having seen an
    /// unmap confirmed.
    ///
    /// `None` means the backend does not track it. A backend that answers
    /// `None` gives up that check, and the layer above falls back to trusting
    /// its call-site ordering.
    fn live_registrations(&self) -> Option<usize> {
        None
    }
}

/// [`RdmaBackend`] over the UCX transport's Phase-1 RMA plumbing.
///
/// Holds an [`RdmaEndpoint`], which is two `Arc`s and no UCX state — every
/// `ucp_*` handle stays on the progress thread. Constructing one before
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
