// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Consumer-side logic for pulling rendezvous data.
//!
//! Handles the receiver-driven pull protocol: acquire read lock, then either
//! receive inline data or pull chunks one-by-one (with optional pipelining).

use std::sync::Arc;

use crate::messenger::Messenger;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use velo_ext::WorkerId;

use crate::rendezvous::handle::DataHandle;
use crate::rendezvous::protocol::{
    AcquireResponse, DataMetadata, RvAcquireRequest, RvDetachRequest, RvHandleWire,
    RvMetadataRequest, RvPullRequest, RvRefRequest, RvReleaseRequest,
};
use crate::rendezvous::write::RendezvousWrite;

#[cfg(feature = "nixl")]
use crate::rendezvous::nixl_endpoint::NixlEndpoint;
#[cfg(feature = "nixl")]
use crate::rendezvous::protocol::{
    NixlAddrDescriptor, RvNixlHandshakeRequest, RvNixlHandshakeResponse,
};

/// Consumer-side operations for the rendezvous protocol.
///
/// These are free functions that take the necessary components as arguments,
/// called by [`crate::RendezvousManager`] which holds the state.
pub struct Consumer;

impl Consumer {
    /// Query metadata about remote data (no read lock acquired).
    pub async fn metadata(messenger: &Arc<Messenger>, handle: DataHandle) -> Result<DataMetadata> {
        let target_worker = handle.worker_id();

        let meta: DataMetadata = messenger
            .typed_unary_streaming::<DataMetadata>("_rv_metadata")
            .payload(&RvMetadataRequest {
                handle: RvHandleWire::from_handle(handle),
            })?
            .worker(target_worker)
            .send()
            .await?;

        Ok(meta)
    }

    /// Pull data from a remote handle into a new `Bytes`.
    ///
    /// Acquires a read lock, transfers data (inline or chunked), returns owned bytes.
    /// The read lock remains held until `detach()` or `release()` is called.
    pub async fn get(messenger: &Arc<Messenger>, handle: DataHandle) -> Result<(Bytes, u64)> {
        Self::get_with_nixl(messenger, handle, None).await
    }

    /// Pull data from a remote handle, optionally fulfilling RDMA acquires
    /// via the supplied [`NixlEndpoint`].
    ///
    /// Phase 2 entry point used by [`crate::RendezvousManager::get`] when the
    /// `nixl` feature is enabled. If `endpoint_with_remote` is `None` and the
    /// owner returns an `Rdma` response, the call fails with a clear error.
    pub(crate) async fn get_with_nixl(
        messenger: &Arc<Messenger>,
        handle: DataHandle,
        #[cfg(feature = "nixl")] endpoint_with_remote: Option<(&Arc<NixlEndpoint>, &str)>,
        #[cfg(not(feature = "nixl"))] endpoint_with_remote: Option<()>,
    ) -> Result<(Bytes, u64)> {
        let target_worker = handle.worker_id();

        let response: AcquireResponse = messenger
            .typed_unary_streaming::<AcquireResponse>("_rv_acquire")
            .payload(&RvAcquireRequest {
                handle: RvHandleWire::from_handle(handle),
            })?
            .worker(target_worker)
            .send()
            .await?;

        match response {
            AcquireResponse::Ready {
                lease_id,
                transfer_id,
                total_len,
                chunk_size,
                chunk_count,
            } => {
                match pull_chunks(
                    messenger,
                    target_worker,
                    transfer_id,
                    total_len,
                    chunk_size,
                    chunk_count,
                )
                .await
                {
                    Ok(data) => Ok((data.freeze(), lease_id)),
                    Err(e) => {
                        // Best-effort cleanup: release read lock to prevent owner-side leak
                        if let Err(cleanup_err) =
                            Consumer::detach(messenger, handle, lease_id).await
                        {
                            tracing::warn!(
                                "Failed to detach lease {lease_id} after pull failure: {cleanup_err}"
                            );
                        }
                        Err(e)
                    }
                }
            }
            #[cfg(feature = "nixl")]
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
            } => {
                let (endpoint, remote_agent) = endpoint_with_remote.ok_or_else(|| {
                    anyhow::anyhow!(
                        "received Rdma acquire response but consumer has not enabled NIXL"
                    )
                })?;
                match nixl_read(endpoint, remote_agent, &descriptor).await {
                    Ok(data) => Ok((data, lease_id)),
                    Err(e) => {
                        if let Err(cleanup_err) =
                            Consumer::detach(messenger, handle, lease_id).await
                        {
                            tracing::warn!(
                                "Failed to detach lease {lease_id} after RDMA read failure: \
                                 {cleanup_err}"
                            );
                        }
                        Err(e)
                    }
                }
            }
            #[cfg(not(feature = "nixl"))]
            AcquireResponse::Rdma { .. } => {
                let _ = endpoint_with_remote;
                anyhow::bail!(
                    "received Rdma acquire response but velo-rendezvous was built without \
                     the `nixl` feature"
                )
            }
        }
    }

    /// Pull data from a remote handle into an explicit destination buffer.
    ///
    /// Acquires a read lock, transfers data chunk-by-chunk into `dest`.
    /// The read lock remains held until `detach()` or `release()` is called.
    pub async fn get_into(
        messenger: &Arc<Messenger>,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
    ) -> Result<u64> {
        Self::get_into_with_nixl(messenger, handle, dest, None).await
    }

    /// `get_into` variant that can also fulfill RDMA acquires.
    pub(crate) async fn get_into_with_nixl(
        messenger: &Arc<Messenger>,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
        #[cfg(feature = "nixl")] endpoint_with_remote: Option<(&Arc<NixlEndpoint>, &str)>,
        #[cfg(not(feature = "nixl"))] endpoint_with_remote: Option<()>,
    ) -> Result<u64> {
        let target_worker = handle.worker_id();

        let response: AcquireResponse = messenger
            .typed_unary_streaming::<AcquireResponse>("_rv_acquire")
            .payload(&RvAcquireRequest {
                handle: RvHandleWire::from_handle(handle),
            })?
            .worker(target_worker)
            .send()
            .await?;

        match response {
            AcquireResponse::Ready {
                lease_id,
                transfer_id,
                total_len,
                chunk_size,
                chunk_count,
            } => {
                match pull_chunks_into(
                    messenger,
                    target_worker,
                    transfer_id,
                    total_len,
                    chunk_size,
                    chunk_count,
                    dest,
                )
                .await
                {
                    Ok(()) => Ok(lease_id),
                    Err(e) => {
                        // Best-effort cleanup: release read lock to prevent owner-side leak
                        if let Err(cleanup_err) =
                            Consumer::detach(messenger, handle, lease_id).await
                        {
                            tracing::warn!(
                                "Failed to detach lease {lease_id} after pull failure: {cleanup_err}"
                            );
                        }
                        Err(e)
                    }
                }
            }
            #[cfg(feature = "nixl")]
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
            } => {
                let (endpoint, remote_agent) = endpoint_with_remote.ok_or_else(|| {
                    anyhow::anyhow!(
                        "received Rdma acquire response but consumer has not enabled NIXL"
                    )
                })?;
                match nixl_read(endpoint, remote_agent, &descriptor).await {
                    Ok(data) => {
                        dest.write_chunk(0, &data)?;
                        Ok(lease_id)
                    }
                    Err(e) => {
                        if let Err(cleanup_err) =
                            Consumer::detach(messenger, handle, lease_id).await
                        {
                            tracing::warn!(
                                "Failed to detach lease {lease_id} after RDMA read failure: \
                                 {cleanup_err}"
                            );
                        }
                        Err(e)
                    }
                }
            }
            #[cfg(not(feature = "nixl"))]
            AcquireResponse::Rdma { .. } => {
                let _ = endpoint_with_remote;
                anyhow::bail!(
                    "received Rdma acquire response but velo-rendezvous was built without \
                     the `nixl` feature"
                )
            }
        }
    }

    /// Send `_rv_nixl_handshake` to the owner and return their (agent_name,
    /// local_md). The caller is responsible for `Agent::load_remote_md` and
    /// caching.
    #[cfg(feature = "nixl")]
    pub(crate) async fn nixl_handshake(
        messenger: &Arc<Messenger>,
        target_worker: WorkerId,
    ) -> Result<RvNixlHandshakeResponse> {
        let resp: RvNixlHandshakeResponse = messenger
            .typed_unary_streaming::<RvNixlHandshakeResponse>("_rv_nixl_handshake")
            .payload(&RvNixlHandshakeRequest)?
            .worker(target_worker)
            .send()
            .await?;
        Ok(resp)
    }

    /// Increment the refcount on a remote handle.
    ///
    /// Waits for an ack from the owner confirming the increment completed,
    /// so callers can safely read metadata or pass the handle to another
    /// consumer immediately after this returns.
    pub async fn ref_handle(messenger: &Arc<Messenger>, handle: DataHandle) -> Result<()> {
        let target_worker = handle.worker_id();

        messenger
            .unary_streaming("_rv_ref")
            .raw_payload(Bytes::from(serde_json::to_vec(&RvRefRequest {
                handle: RvHandleWire::from_handle(handle),
            })?))
            .worker(target_worker)
            .send()
            .await?;

        Ok(())
    }

    /// Release the read lock without decrementing refcount (can get again).
    pub async fn detach(
        messenger: &Arc<Messenger>,
        handle: DataHandle,
        lease_id: u64,
    ) -> Result<()> {
        let target_worker = handle.worker_id();

        messenger
            .am_send_streaming("_rv_detach")?
            .raw_payload(Bytes::from(serde_json::to_vec(&RvDetachRequest {
                handle: RvHandleWire::from_handle(handle),
                lease_id,
            })?))
            .worker(target_worker)
            .send()
            .await?;

        Ok(())
    }

    /// Release the read lock AND decrement refcount. Frees data when both hit 0.
    pub async fn release(
        messenger: &Arc<Messenger>,
        handle: DataHandle,
        lease_id: u64,
    ) -> Result<()> {
        let target_worker = handle.worker_id();

        messenger
            .am_send_streaming("_rv_release")?
            .raw_payload(Bytes::from(serde_json::to_vec(&RvReleaseRequest {
                handle: RvHandleWire::from_handle(handle),
                lease_id,
            })?))
            .worker(target_worker)
            .send()
            .await?;

        Ok(())
    }
}

/// Pull all chunks for a chunked transfer into a `BytesMut` buffer.
async fn pull_chunks(
    messenger: &Arc<Messenger>,
    target_worker: WorkerId,
    transfer_id: u64,
    total_len: u64,
    chunk_size: u32,
    chunk_count: u32,
) -> Result<BytesMut> {
    let mut buf = BytesMut::with_capacity(total_len as usize);
    buf.resize(total_len as usize, 0);

    // Pull chunks sequentially. Pipelining can be added as an optimization.
    for chunk_index in 0..chunk_count {
        let req = RvPullRequest {
            transfer_id,
            chunk_index,
        };
        let payload = serde_json::to_vec(&req)?;

        let chunk_bytes: Bytes = messenger
            .unary_streaming("_rv_pull")
            .raw_payload(Bytes::from(payload))
            .worker(target_worker)
            .send()
            .await?;

        let offset = chunk_index as usize * chunk_size as usize;
        let end = (offset + chunk_bytes.len()).min(total_len as usize);
        buf[offset..end].copy_from_slice(&chunk_bytes[..end - offset]);
    }

    Ok(buf)
}

/// Perform a NIXL_READ from a remote-registered region into a host arena
/// slice, then copy the result into a returned [`Bytes`].
///
/// The destination is allocated from the endpoint's pre-registered host
/// arena (`alloc_host_dst`), so no per-call `register_memory` is paid — the
/// arena's backing `SystemStorage` was registered once at endpoint creation.
/// The remote descriptor's `mem_type` and `device_id` are honored verbatim,
/// so VRAM-source transfers work as long as both sides have CUDA-aware UCX.
#[cfg(feature = "nixl")]
async fn nixl_read(
    endpoint: &Arc<NixlEndpoint>,
    remote_agent: &str,
    descriptor: &[u8],
) -> Result<Bytes> {
    use velo_nixl::{XferDescList, XferOp};

    let desc: NixlAddrDescriptor = rmp_serde::from_slice(descriptor)
        .map_err(|e| anyhow::anyhow!("failed to deserialize NixlAddrDescriptor: {e}"))?;

    let size = desc.size as usize;

    // Validate that the descriptor's agent matches the loaded remote name.
    if desc.agent != remote_agent {
        anyhow::bail!(
            "NIXL Rdma descriptor agent {:?} does not match handshake-loaded agent {:?}",
            desc.agent,
            remote_agent,
        );
    }

    let dst = endpoint.alloc_host_dst(size)?;
    let local_descriptor = dst.registered_descriptor();

    // Build descriptor lists in a sub-scope so they drop before the `.await`
    // below — `XferDescList` is not `Send` (its inner C handle is a raw
    // pointer), but the `XferRequest` returned by `create_xfer_req` *is*
    // `Send + Sync` and copies all needed state into libnixl.
    let req = {
        let mut local = XferDescList::new(local_descriptor.mem_type)
            .map_err(|e| anyhow::anyhow!("velo-nixl: local XferDescList::new failed: {e}"))?;
        local.add_desc(
            local_descriptor.addr as usize,
            size,
            local_descriptor.device_id,
        );

        let mut remote = XferDescList::new(desc.mem_type)
            .map_err(|e| anyhow::anyhow!("velo-nixl: remote XferDescList::new failed: {e}"))?;
        remote.add_desc(desc.addr as usize, size, desc.device_id);

        endpoint
            .agent
            .create_xfer_req(
                XferOp::Read,
                &local,
                &remote,
                remote_agent,
                Some(endpoint.opt_args()),
            )
            .map_err(|e| anyhow::anyhow!("velo-nixl: create_xfer_req failed: {e}"))?
    };

    endpoint
        .agent
        .post_xfer_req(&req, Some(endpoint.opt_args()))
        .map_err(|e| anyhow::anyhow!("velo-nixl: post_xfer_req failed: {e}"))?;
    velo_nixl::wait_xfer(&endpoint.agent, &req)
        .await
        .map_err(|e| anyhow::anyhow!("velo-nixl: wait_xfer failed: {e}"))?;

    // Copy out of the arena slice into a freshly-allocated Bytes (existing
    // contract). The arena buffer drops at end of function, releasing the
    // slice back to the arena.
    //
    // SAFETY: `local_descriptor.addr` is the base pointer of the arena
    // slice, valid for `size` bytes for the lifetime of `dst`.
    let host_slice =
        unsafe { std::slice::from_raw_parts(local_descriptor.addr as *const u8, size) };
    let out = Bytes::copy_from_slice(host_slice);
    drop(dst);
    Ok(out)
}

/// Perform a NIXL_READ from a remote-registered region into a device arena
/// slice on the specified CUDA device. Returns the arena buffer; the caller
/// is responsible for keeping it alive while the data is in use, and may
/// `cudaMemcpy` it to host or pass its `registered_descriptor()` on for
/// further GPU-side use.
///
/// Both DRAM-source (`MemType::Dram` in the wire descriptor) and VRAM-source
/// (`MemType::Vram`) transfers are supported — UCX's `cuda_copy` /
/// `cuda_ipc` transports handle the cross. Requires a UCX build with
/// `--with-cuda` against the CUDA toolkit.
#[cfg(feature = "nixl")]
pub(crate) async fn nixl_read_into_device(
    endpoint: &Arc<NixlEndpoint>,
    remote_agent: &str,
    descriptor: &[u8],
    device_id: u32,
) -> Result<crate::rendezvous::nixl_endpoint::DeviceArenaBuffer> {
    use velo_nixl::{MemType, XferDescList, XferOp};

    let desc: NixlAddrDescriptor = rmp_serde::from_slice(descriptor)
        .map_err(|e| anyhow::anyhow!("failed to deserialize NixlAddrDescriptor: {e}"))?;

    let size = desc.size as usize;

    if desc.agent != remote_agent {
        anyhow::bail!(
            "NIXL Rdma descriptor agent {:?} does not match handshake-loaded agent {:?}",
            desc.agent,
            remote_agent,
        );
    }

    let dst = endpoint.alloc_device_dst(size, device_id)?;
    let local_descriptor = dst.registered_descriptor();
    debug_assert_eq!(local_descriptor.mem_type, MemType::Vram);

    let req = {
        let mut local = XferDescList::new(local_descriptor.mem_type)
            .map_err(|e| anyhow::anyhow!("velo-nixl: local XferDescList::new failed: {e}"))?;
        local.add_desc(
            local_descriptor.addr as usize,
            size,
            local_descriptor.device_id,
        );

        let mut remote = XferDescList::new(desc.mem_type)
            .map_err(|e| anyhow::anyhow!("velo-nixl: remote XferDescList::new failed: {e}"))?;
        remote.add_desc(desc.addr as usize, size, desc.device_id);

        endpoint
            .agent
            .create_xfer_req(
                XferOp::Read,
                &local,
                &remote,
                remote_agent,
                Some(endpoint.opt_args()),
            )
            .map_err(|e| anyhow::anyhow!("velo-nixl: create_xfer_req failed: {e}"))?
    };

    endpoint
        .agent
        .post_xfer_req(&req, Some(endpoint.opt_args()))
        .map_err(|e| anyhow::anyhow!("velo-nixl: post_xfer_req failed: {e}"))?;
    velo_nixl::wait_xfer(&endpoint.agent, &req)
        .await
        .map_err(|e| anyhow::anyhow!("velo-nixl: wait_xfer failed: {e}"))?;

    Ok(dst)
}

/// Pull all chunks for a chunked transfer into an explicit destination.
async fn pull_chunks_into(
    messenger: &Arc<Messenger>,
    target_worker: WorkerId,
    transfer_id: u64,
    _total_len: u64,
    chunk_size: u32,
    chunk_count: u32,
    dest: &mut impl RendezvousWrite,
) -> Result<()> {
    for chunk_index in 0..chunk_count {
        let req = RvPullRequest {
            transfer_id,
            chunk_index,
        };
        let payload = serde_json::to_vec(&req)?;

        let chunk_bytes: Bytes = messenger
            .unary_streaming("_rv_pull")
            .raw_payload(Bytes::from(payload))
            .worker(target_worker)
            .send()
            .await?;

        let offset = chunk_index as usize * chunk_size as usize;
        dest.write_chunk(offset, &chunk_bytes)?;
    }

    Ok(())
}
