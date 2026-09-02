// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Control-plane handler constructors for the rendezvous protocol.
//!
//! Seven handlers are registered on the owner side:
//! - [`create_rv_metadata_handler`]: lock-free metadata query
//! - [`create_rv_acquire_handler`]: acquires read lock, returns an RDMA
//!   descriptor or chunked transfer metadata
//! - [`create_rv_pull_handler`]: returns a specific chunk
//! - [`create_rv_ref_handler`]: increments refcount
//! - [`create_rv_detach_handler`]: releases read lock only
//! - [`create_rv_release_handler`]: releases read lock + decrements refcount
//! - [`create_rv_lease_renew_handler`]: pushes an RDMA lease's deadline out

use std::sync::Arc;

use crate::rendezvous::protocol::{
    AcquireResponse, RvAcquireRequest, RvDetachRequest, RvLeaseRenewRequest, RvMetadataRequest,
    RvPullRequest, RvRefRequest, RvReleaseRequest,
};
use crate::rendezvous::store::{DEFAULT_CHUNK_SIZE, DataStore, LeaseOutcome};

/// Build the `_rv_metadata` handler: returns [`DataMetadata`](crate::rendezvous::protocol::DataMetadata)
/// without acquiring a read lock.
pub fn create_rv_metadata_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary(
        "_rv_metadata",
        move |ctx: crate::messenger::TypedContext<RvMetadataRequest>| {
            let handle = ctx.input.handle.to_handle();
            let (_, local_id) = handle.unpack();
            match store.metadata(local_id) {
                Some(meta) => Ok(meta),
                None => anyhow::bail!("rendezvous handle not found: {handle}"),
            }
        },
    )
    .build()
}

/// Build the `_rv_acquire` handler: acquires a read lock and answers with
/// either an RDMA descriptor or chunked transfer metadata.
///
/// The read lock is taken *first*, before either answer is composed, so the
/// slot cannot be freed between the decision and the response. Which answer the
/// consumer gets never changes what it must do afterwards: both carry a lease,
/// and both end in a detach or a release.
pub fn create_rv_acquire_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary(
        "_rv_acquire",
        move |ctx: crate::messenger::TypedContext<RvAcquireRequest>| {
            let handle = ctx.input.handle.to_handle();
            let (_, local_id) = handle.unpack();

            // Acquire read lock
            let lease_id = store
                .acquire_read_lock(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;

            let total_len = store
                .get_total_len(local_id)
                .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;

            #[cfg(all(target_os = "linux", feature = "ucx"))]
            if let Some(response) = rdma_response(
                &store,
                local_id,
                lease_id,
                total_len,
                ctx.input.rdma.as_ref(),
            ) {
                return Ok(response);
            }

            // Always use chunked transfer (even for 1 chunk) to avoid
            // JSON-encoding binary data in the typed-unary response.
            let (transfer_id, chunk_size, chunk_count) = store
                .create_transfer(local_id, lease_id, DEFAULT_CHUNK_SIZE)
                .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
            Ok(AcquireResponse::Ready {
                lease_id,
                transfer_id,
                total_len,
                chunk_size,
                chunk_count,
            })
        },
    )
    .build()
}

/// Decide whether this acquire can be answered with an RDMA descriptor.
///
/// `None` means "serve it chunked", and every `None` has recorded *why* on
/// `velo_rendezvous_rdma_path_total` before returning — the series that makes a
/// path nobody can see answerable in production. The checks run cheapest-first,
/// and each one is a plain fact about this acquire:
///
/// 1. The consumer offered nothing, so it could not decode a descriptor.
/// 2. This instance has no registry, so it has no pinned slots either.
/// 3. The kill switch is off (D6): rollback without a rebuild.
/// 4. The slot is plain heap bytes.
/// 5. The payload is below `rdma_min_bytes`, where one GET does not pay for
///    itself against the single pull it would replace (D11).
/// 6. The consumer's offer does not name the backend this owner serves (D12) —
///    a NIXL-only consumer talking to a UCX owner is well-formed and simply
///    unservable.
/// 7. The staging is gone: an external region was deregistered under the slot,
///    or the descriptor would not encode. Counted as `not_pinned`, because from
///    the acquire's point of view that is what it now is.
///
/// A lease that *is* answered with a descriptor gets a deadline, and that is
/// the only place one is set. Chunked leases stay deadline-free.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn rdma_response(
    store: &Arc<DataStore>,
    local_id: u64,
    lease_id: u64,
    total_len: u64,
    offer: Option<&crate::rendezvous::protocol::RdmaOffer>,
) -> Option<AcquireResponse> {
    use crate::observability::RdmaPathReason;
    use crate::rendezvous::store::StageMode;

    let decline = |reason: RdmaPathReason| -> Option<AcquireResponse> {
        store.record_path(reason);
        None
    };

    let Some(offer) = offer else {
        return decline(RdmaPathReason::NoOffer);
    };
    let Some(rdma) = store.rdma() else {
        // No registry at all: a `ucx` build whose instance never got
        // `add_ucx_transport`. That is a transport-configuration fact, not a
        // staging one, and `NotPinned` below means something else entirely —
        // the slot exists and is heap-staged. The consumer records
        // `NotConfigured` for this same condition, so labelling it anything
        // else makes the two sides disagree about one fact.
        return decline(RdmaPathReason::NotConfigured);
    };
    if !rdma.config.enabled {
        return decline(RdmaPathReason::KillSwitch);
    }
    if store.stage_mode(local_id) != Some(StageMode::Pinned) {
        return decline(RdmaPathReason::NotPinned);
    }
    if total_len < rdma.config.rdma_min_bytes {
        return decline(RdmaPathReason::BelowMin);
    }
    let backend = rdma.backend;
    if !offer.backends.iter().any(|name| name == backend.key()) {
        return decline(RdmaPathReason::NoOffer);
    }

    // Built under the slot's map guard and encoded outside it. `None` here is a
    // slot that vanished, a slot staged for a different backend, or an external
    // region that has been deregistered underneath it — all of which mean the
    // memory is not readable by a peer's NIC any more.
    let descriptor = store
        .with_pinned(local_id, |slot| {
            (slot.backend() == backend).then(|| slot.descriptor())?
        })
        .flatten();
    let Some(bytes) = descriptor.and_then(|d| d.encode()) else {
        return decline(RdmaPathReason::NotPinned);
    };

    // Derived first, and the deadline armed only if the consumer is being told
    // about it. `set_rdma_context` normalises the timeout so this cannot be
    // zero; the check is here because the alternative — a deadline the consumer
    // was told did not exist, so never renews — force-releases a lease while
    // the peer's NIC is still reading, and that is silent wrong data rather
    // than a visible failure.
    let lease_timeout_ms = u64::try_from(rdma.config.lease_timeout.as_millis()).unwrap_or(u64::MAX);
    if lease_timeout_ms != 0 {
        // The lease is now the owner's only handle on a transfer it cannot
        // observe.
        store.set_lease_deadline(lease_id, local_id, rdma.config.lease_timeout);
    }
    store.record_path(RdmaPathReason::Ok);
    Some(AcquireResponse::Rdma {
        lease_id,
        descriptor: bytes,
        lease_timeout_ms,
    })
}

/// Build the `_rv_pull` handler: returns chunk bytes for a given transfer + index.
pub fn create_rv_pull_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::unary_handler(
        "_rv_pull",
        move |ctx: crate::messenger::Context| -> crate::messenger::UnifiedResponse {
            let req: RvPullRequest = serde_json::from_slice(&ctx.payload)?;
            match store.get_chunk(req.transfer_id, req.chunk_index) {
                Some(chunk) => Ok(Some(chunk)),
                None => anyhow::bail!(
                    "chunk not found: transfer_id={}, chunk_index={}",
                    req.transfer_id,
                    req.chunk_index
                ),
            }
        },
    )
    .build()
}

/// Build the `_rv_ref` handler: increments refcount.
///
/// Returns an empty ack so the caller can confirm the increment completed
/// before proceeding (avoids races between fire-and-forget and metadata queries).
pub fn create_rv_ref_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::unary_handler(
        "_rv_ref",
        move |ctx: crate::messenger::Context| -> crate::messenger::UnifiedResponse {
            let req: RvRefRequest = serde_json::from_slice(&ctx.payload)?;
            let handle = req.handle.to_handle();
            let (_, local_id) = handle.unpack();
            if !store.ref_increment(local_id) {
                anyhow::bail!("_rv_ref: handle not found: {handle}");
            }
            Ok(None)
        },
    )
    .build()
}

/// Build the `_rv_lease_renew` handler: pushes an RDMA lease's deadline out by
/// the timeout it was granted under (D8).
///
/// Fire-and-forget, and forgiving by design. A renewal that names a lease which
/// has already been detached, released or reaped does nothing and says so at
/// `debug`: the keepalive races the release that ends the transfer, and a
/// consumer whose last renewal crossed its own release has done nothing wrong.
/// Anything louder would make the normal end of every renewed transfer look
/// like a fault.
///
/// The `handle` is checked against the lease the same way detach and release
/// check theirs, so a renewal cannot extend a lease on a slot the caller does
/// not name — a lease id is a small integer, and a confused or hostile peer
/// guessing one must not be able to keep somebody else's slot alive.
pub fn create_rv_lease_renew_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler(
        "_rv_lease_renew",
        move |ctx: crate::messenger::Context| {
            let req: RvLeaseRenewRequest = serde_json::from_slice(&ctx.payload)?;
            let handle = req.handle.to_handle();
            let (_, local_id) = handle.unpack();

            match store.lease_slot(req.lease_id) {
                Some(expected_local_id) if expected_local_id == local_id => {
                    if !store.renew_lease(req.lease_id) {
                        tracing::debug!(
                            lease = req.lease_id,
                            %handle,
                            "_rv_lease_renew: lease carries no deadline to renew"
                        );
                    }
                }
                Some(expected_local_id) => {
                    tracing::warn!(
                        "_rv_lease_renew: lease {} maps to slot {}, not {}",
                        req.lease_id,
                        expected_local_id,
                        local_id,
                    );
                }
                None => {
                    tracing::debug!(
                        lease = req.lease_id,
                        %handle,
                        "_rv_lease_renew: lease already ended"
                    );
                }
            }
            Ok(())
        },
    )
    .build()
}

/// Build the `_rv_detach` handler: releases read lock without decrementing refcount.
pub fn create_rv_detach_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler("_rv_detach", move |ctx: crate::messenger::Context| {
        let req: RvDetachRequest = serde_json::from_slice(&ctx.payload)?;
        let handle = req.handle.to_handle();
        let (_, local_id) = handle.unpack();

        match store.consume_lease(req.lease_id, local_id) {
            LeaseOutcome::Consumed => {
                store.release_read_lock(local_id);
                store.remove_transfers_by_lease(req.lease_id);
            }
            // Nothing was consumed, so nothing is released and the lease keeps
            // its deadline — a mismatched detach cannot strand the slot it
            // names *or* the slot it does not.
            LeaseOutcome::Mismatch { actual } => {
                tracing::warn!(
                    "_rv_detach: lease {} maps to slot {}, not {}; ignored",
                    req.lease_id,
                    actual,
                    local_id,
                );
            }
            LeaseOutcome::Unknown => {
                tracing::warn!(
                    "_rv_detach: invalid or already-consumed lease {} for {handle}",
                    req.lease_id,
                );
            }
        }
        Ok(())
    })
    .build()
}

/// Build the `_rv_release` handler: releases read lock AND decrements refcount.
/// Frees the slot if both reach zero.
pub fn create_rv_release_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler("_rv_release", move |ctx: crate::messenger::Context| {
        let req: RvReleaseRequest = serde_json::from_slice(&ctx.payload)?;
        let handle = req.handle.to_handle();
        let (_, local_id) = handle.unpack();

        match store.consume_lease(req.lease_id, local_id) {
            LeaseOutcome::Consumed => {
                store.release_read_lock(local_id);
                store.remove_transfers_by_lease(req.lease_id);
                let should_free = store.ref_decrement(local_id);
                if should_free {
                    store.try_free(local_id);
                    tracing::debug!("_rv_release: freed slot for {handle}");
                }
            }
            LeaseOutcome::Mismatch { actual } => {
                tracing::warn!(
                    "_rv_release: lease {} maps to slot {}, not {}; ignored",
                    req.lease_id,
                    actual,
                    local_id,
                );
            }
            LeaseOutcome::Unknown => {
                tracing::warn!(
                    "_rv_release: invalid or already-consumed lease {} for {handle}",
                    req.lease_id,
                );
            }
        }
        Ok(())
    })
    .build()
}
