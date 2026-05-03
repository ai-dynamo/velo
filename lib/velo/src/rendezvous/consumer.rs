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
            AcquireResponse::Rdma { .. } => {
                anyhow::bail!("RDMA rendezvous not yet implemented (Phase 2)")
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
            AcquireResponse::Rdma { .. } => {
                anyhow::bail!("RDMA rendezvous not yet implemented (Phase 2)")
            }
        }
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
