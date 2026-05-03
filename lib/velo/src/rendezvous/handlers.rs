// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Control-plane handler constructors for the rendezvous protocol.
//!
//! Six handlers are registered on the owner side:
//! - [`create_rv_metadata_handler`]: lock-free metadata query
//! - [`create_rv_acquire_handler`]: acquires read lock, returns inline or chunked
//! - [`create_rv_pull_handler`]: returns a specific chunk
//! - [`create_rv_ref_handler`]: increments refcount
//! - [`create_rv_detach_handler`]: releases read lock only
//! - [`create_rv_release_handler`]: releases read lock + decrements refcount

use std::sync::Arc;

use crate::rendezvous::protocol::{
    AcquireResponse, RvAcquireRequest, RvDetachRequest, RvMetadataRequest, RvPullRequest,
    RvRefRequest, RvReleaseRequest,
};
use crate::rendezvous::store::{DEFAULT_CHUNK_SIZE, DataStore};

#[cfg(feature = "nixl")]
use std::sync::OnceLock;

#[cfg(feature = "nixl")]
use crate::rendezvous::nixl_endpoint::NixlEndpoint;
#[cfg(feature = "nixl")]
use crate::rendezvous::protocol::{
    NixlAddrDescriptor, RvNixlHandshakeRequest, RvNixlHandshakeResponse,
};
#[cfg(feature = "nixl")]
use crate::rendezvous::store::StageMode;

/// Shared, lazily-initialized NIXL endpoint slot.
///
/// `RendezvousManager` holds one of these and clones the `Arc` into every
/// handler closure that needs to observe `enable_nixl()`.
#[cfg(feature = "nixl")]
pub(crate) type NixlEndpointSlot = Arc<OnceLock<Arc<NixlEndpoint>>>;

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

/// Build the `_rv_acquire` handler: acquires a read lock and returns data
/// inline (small) or chunked transfer metadata (large).
///
/// When the `nixl` feature is enabled, this handler also takes the manager's
/// [`NixlEndpoint`] so it can answer pinned slots with an
/// [`AcquireResponse::Rdma`] descriptor pointing at the registered region.
#[cfg(not(feature = "nixl"))]
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

/// `nixl`-feature variant: branches on slot mode and returns
/// [`AcquireResponse::Rdma`] for pinned slots when the slot is shared with an
/// initialized [`NixlEndpoint`].
#[cfg(feature = "nixl")]
pub fn create_rv_acquire_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    create_rv_acquire_handler_with_slot(store, Arc::new(OnceLock::new()))
}

/// Build the acquire handler with a shared NIXL endpoint slot.
///
/// `nixl_slot` is read on every invocation, so callers may set it after
/// handler registration via
/// [`RendezvousManager::enable_nixl`](crate::RendezvousManager::enable_nixl).
/// If unset when a pinned slot's acquire arrives, the request fails with a
/// clear error.
#[cfg(feature = "nixl")]
pub(crate) fn create_rv_acquire_handler_with_slot(
    store: Arc<DataStore>,
    nixl_slot: NixlEndpointSlot,
) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary(
        "_rv_acquire",
        move |ctx: crate::messenger::TypedContext<RvAcquireRequest>| {
            let handle = ctx.input.handle.to_handle();
            let (_, local_id) = handle.unpack();

            let mode = store
                .stage_mode(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;

            let lease_id = store
                .acquire_read_lock(local_id)
                .ok_or_else(|| anyhow::anyhow!("rendezvous handle not found: {handle}"))?;

            match mode {
                StageMode::InMemory => {
                    let total_len = store
                        .get_total_len(local_id)
                        .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
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
                }
                StageMode::Pinned => {
                    let endpoint = nixl_slot.get().ok_or_else(|| {
                        anyhow::anyhow!(
                            "pinned slot {handle} requested but NIXL endpoint is not enabled \
                             on this owner"
                        )
                    })?;
                    let slot_data = store
                        .get_data(local_id)
                        .ok_or_else(|| anyhow::anyhow!("slot vanished after lock acquire"))?;
                    let descriptor = NixlAddrDescriptor {
                        agent: endpoint.agent_name.clone(),
                        addr: slot_data.as_ptr() as u64,
                        size: slot_data.len() as u64,
                        mem_type: velo_nixl::MemType::Dram,
                        device_id: 0,
                    };
                    let bytes = rmp_serde::to_vec(&descriptor).map_err(|e| {
                        anyhow::anyhow!("failed to serialize NixlAddrDescriptor: {e}")
                    })?;
                    Ok(AcquireResponse::Rdma {
                        lease_id,
                        descriptor: bytes,
                    })
                }
            }
        },
    )
    .build()
}

/// Build the `_rv_nixl_handshake` handler: returns the local agent name and
/// `get_local_md()` blob for a peer to call `Agent::load_remote_md` on.
///
/// Reads the endpoint from a shared slot so the handler stays valid whether
/// `enable_nixl()` was called before or after handler registration.
#[cfg(feature = "nixl")]
pub(crate) fn create_rv_nixl_handshake_handler(
    nixl_slot: NixlEndpointSlot,
) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary(
        "_rv_nixl_handshake",
        move |_ctx: crate::messenger::TypedContext<RvNixlHandshakeRequest>| {
            let endpoint = nixl_slot.get().ok_or_else(|| {
                anyhow::anyhow!("NIXL handshake requested but enable_nixl was not called")
            })?;
            // Re-snapshot local_md per request so it includes registrations
            // that happened between `enable_nixl()` and now.
            let local_md = endpoint.current_local_md()?;
            Ok(RvNixlHandshakeResponse {
                agent: endpoint.agent_name.clone(),
                local_md,
            })
        },
    )
    .build()
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

/// Build the `_rv_detach` handler: releases read lock without decrementing refcount.
pub fn create_rv_detach_handler(store: Arc<DataStore>) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler("_rv_detach", move |ctx: crate::messenger::Context| {
        let req: RvDetachRequest = serde_json::from_slice(&ctx.payload)?;
        let handle = req.handle.to_handle();
        let (_, local_id) = handle.unpack();

        match store.consume_lease(req.lease_id) {
            Some(expected_local_id) if expected_local_id == local_id => {
                store.release_read_lock(local_id);
                store.remove_transfers_by_lease(req.lease_id);
            }
            Some(expected_local_id) => {
                tracing::warn!(
                    "_rv_detach: lease {} maps to slot {}, not {}",
                    req.lease_id,
                    expected_local_id,
                    local_id,
                );
            }
            None => {
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

        match store.consume_lease(req.lease_id) {
            Some(expected_local_id) if expected_local_id == local_id => {
                store.release_read_lock(local_id);
                store.remove_transfers_by_lease(req.lease_id);
                let should_free = store.ref_decrement(local_id);
                if should_free {
                    store.try_free(local_id);
                    tracing::debug!("_rv_release: freed slot for {handle}");
                }
            }
            Some(expected_local_id) => {
                tracing::warn!(
                    "_rv_release: lease {} maps to slot {}, not {}",
                    req.lease_id,
                    expected_local_id,
                    local_id,
                );
            }
            None => {
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
