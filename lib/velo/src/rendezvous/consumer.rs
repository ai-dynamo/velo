// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Consumer-side logic for pulling rendezvous data.
//!
//! The receiver drives everything: `_rv_acquire` takes a read lock and settles
//! how the bytes will travel, and then either chunks are pulled or a single
//! RDMA GET is issued. Either way the caller ends holding a lease to detach or
//! release.
//!
//! # Eligibility is decided here, locally, before the acquire (D6)
//!
//! The offer that rides the acquire is built from three facts this side already
//! knows: whether this instance has an RDMA registry, whether its kill switch
//! is off, and whether the owner is registered on a transport whose key matches
//! that registry's backend. The last is a lookup in the messenger's own peer
//! table — the GET rides *this* consumer's UCX endpoint to the owner, so "UCX
//! is registered for that peer" is the whole condition. UCX does not have to be
//! the primary transport; a control plane on TCP with UCX beside it is the
//! expected deployment.
//!
//! Nothing is asked of the owner, which is what makes the scheme skew-safe: an
//! owner that does not understand the offer ignores it and answers chunked.
//!
//! The cold-start consequence is worth stating plainly: a peer that has not
//! been registered yet translates to nothing, so the *first* get to an unknown
//! owner offers nothing and pulls chunks. Every later one takes the fast path.
//!
//! # Falling back is a routing decision, never an error
//!
//! Four things can go wrong once the owner has answered with a descriptor: it
//! does not decode, it names a backend this consumer does not have, no
//! registered destination can be had, or the GET itself fails. All four detach
//! the lease, re-acquire with no offer, and pull chunks — which always works,
//! because a pinned slot serves the chunked path too.
//!
//! **Exactly one attempt.** The re-acquire carries no offer, so an owner that
//! answers it with another descriptor is broken; that is an error rather than a
//! second fallback, because a loop here would turn one misbehaving owner into
//! an unbounded storm of round trips.

use std::sync::Arc;

use crate::messenger::Messenger;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use velo_ext::WorkerId;

use crate::rendezvous::RendezvousManager;
use crate::rendezvous::handle::DataHandle;
use crate::rendezvous::protocol::{
    AcquireResponse, DataMetadata, RdmaOffer, RvAcquireRequest, RvDetachRequest, RvHandleWire,
    RvMetadataRequest, RvPullRequest, RvRefRequest, RvReleaseRequest,
};
use crate::rendezvous::write::RendezvousWrite;

#[cfg(all(target_os = "linux", feature = "ucx"))]
use crate::observability::RdmaPathReason;
#[cfg(all(target_os = "linux", feature = "ucx"))]
use crate::rendezvous::protocol::RvLeaseRenewRequest;

/// Consumer-side operations for the rendezvous protocol.
///
/// Free functions over the [`RendezvousManager`] that holds the state, called
/// by that manager once it has decided a handle is remote.
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
    /// Acquires a read lock, moves the data by whichever path the owner
    /// offered, and returns owned bytes. The read lock remains held until
    /// `detach()` or `release()` is called.
    pub async fn get(manager: &RendezvousManager, handle: DataHandle) -> Result<(Bytes, u64)> {
        let messenger = manager.messenger();
        let target_worker = handle.worker_id();
        let response = acquire(manager, handle, rdma_offer(manager, target_worker)).await?;

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
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
                lease_timeout_ms,
            } => {
                let descriptor = with_descriptor_hook(manager, descriptor);
                match rdma_pull(manager, handle, lease_id, &descriptor, lease_timeout_ms).await {
                    // One copy out of registered memory, so the caller holds an
                    // ordinary `Bytes` with no relationship to the pool and the
                    // space goes back immediately. `get_pinned` is the version
                    // that skips this copy.
                    Ok(buf) => Ok((Bytes::copy_from_slice(&buf), lease_id)),
                    Err(reason) => fallback_chunked(manager, handle, lease_id, reason).await,
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "ucx")))]
            AcquireResponse::Rdma { lease_id, .. } => {
                unsolicited_rdma(manager, handle, lease_id).await
            }
        }
    }

    /// Pull data from a remote handle straight into registered memory, with no
    /// copy out.
    ///
    /// When the owner answers with a descriptor the NIC writes into the
    /// returned buffer and nothing is copied at all. When it answers chunked —
    /// an in-memory slot, a payload under the threshold, a kill switch — the
    /// chunks are pulled and then copied into a pooled buffer, so the return
    /// type is the same either way and the caller does not branch.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub async fn get_pinned(
        manager: &RendezvousManager,
        handle: DataHandle,
    ) -> Result<(crate::rendezvous::rdma::PinnedBuf, u64)> {
        let messenger = manager.messenger();
        let target_worker = handle.worker_id();
        let response = acquire(manager, handle, rdma_offer(manager, target_worker)).await?;

        match response {
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
                lease_timeout_ms,
            } => {
                let descriptor = with_descriptor_hook(manager, descriptor);
                match rdma_pull(manager, handle, lease_id, &descriptor, lease_timeout_ms).await {
                    Ok(buf) => Ok((buf, lease_id)),
                    Err(reason) => {
                        // The fallback returns a *fresh* lease, and the copy
                        // below can still fail — a pool under pressure, a
                        // registry that has gone away. Guard it, or the failure
                        // strands a chunked lease the reaper cannot reclaim.
                        let (data, lease_id) =
                            fallback_chunked(manager, handle, lease_id, reason).await?;
                        let lease = manager.lease_guard(handle, lease_id);
                        let buf = copy_into_pool(manager, &data).await?;
                        Ok((buf, lease.disarm()))
                    }
                }
            }
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
                    Ok(data) => {
                        let lease = manager.lease_guard(handle, lease_id);
                        let buf = copy_into_pool(manager, &data).await?;
                        Ok((buf, lease.disarm()))
                    }
                    Err(e) => {
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
        }
    }

    /// Pull data from a remote handle into an explicit destination buffer.
    ///
    /// Acquires a read lock and transfers the data into `dest`. The read lock
    /// remains held until `detach()` or `release()` is called.
    pub async fn get_into(
        manager: &RendezvousManager,
        handle: DataHandle,
        dest: &mut impl RendezvousWrite,
    ) -> Result<u64> {
        let messenger = manager.messenger();
        let target_worker = handle.worker_id();
        let response = acquire(manager, handle, rdma_offer(manager, target_worker)).await?;

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
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
                lease_timeout_ms,
            } => {
                let descriptor = with_descriptor_hook(manager, descriptor);
                match rdma_pull_into(
                    manager,
                    handle,
                    lease_id,
                    &descriptor,
                    lease_timeout_ms,
                    dest,
                )
                .await
                {
                    Ok(()) => Ok(lease_id),
                    Err(reason) => {
                        let (data, lease_id) =
                            fallback_chunked(manager, handle, lease_id, reason).await?;
                        // `write_chunk` refuses a destination too small for the
                        // payload, and that refusal must not take the fresh
                        // lease with it.
                        let lease = manager.lease_guard(handle, lease_id);
                        dest.write_chunk(0, &data)?;
                        Ok(lease.disarm())
                    }
                }
            }
            #[cfg(not(all(target_os = "linux", feature = "ucx")))]
            AcquireResponse::Rdma { lease_id, .. } => {
                let (data, lease_id) = unsolicited_rdma(manager, handle, lease_id).await?;
                let lease = manager.lease_guard(handle, lease_id);
                dest.write_chunk(0, &data)?;
                Ok(lease.disarm())
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

/// Send one `_rv_acquire`, carrying `offer` if there is one.
async fn acquire(
    manager: &RendezvousManager,
    handle: DataHandle,
    offer: Option<RdmaOffer>,
) -> Result<AcquireResponse> {
    let response: AcquireResponse = manager
        .messenger()
        .typed_unary_streaming::<AcquireResponse>("_rv_acquire")
        .payload(&RvAcquireRequest {
            handle: RvHandleWire::from_handle(handle),
            rdma: offer,
        })?
        .worker(handle.worker_id())
        .send()
        .await?;
    Ok(response)
}

/// A build without the RDMA path never offers. The function exists so the call
/// sites read the same either way.
#[cfg(not(all(target_os = "linux", feature = "ucx")))]
fn rdma_offer(_manager: &RendezvousManager, _target: WorkerId) -> Option<RdmaOffer> {
    None
}

/// What this consumer can pull with, for an acquire aimed at `target` (D6).
///
/// Computed per get rather than cached: peer registration changes over a
/// process's life, and an offer that outlived the endpoint it was based on
/// would send the owner down a path this side can no longer walk. See the
/// module docs for why the three conditions are the whole of eligibility.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn rdma_offer(manager: &RendezvousManager, target: WorkerId) -> Option<RdmaOffer> {
    let store = manager.data_store();
    let Some(ctx) = store.rdma() else {
        store.record_path(RdmaPathReason::NotConfigured);
        return None;
    };
    if !ctx.config.enabled {
        store.record_path(RdmaPathReason::KillSwitch);
        return None;
    }
    let key = ctx.backend.key();
    let backend = manager.messenger().backend();
    // An owner this instance has never registered has no endpoint to GET over,
    // whatever transports it advertises.
    let Ok(instance) = backend.try_translate_worker_id(target) else {
        store.record_path(RdmaPathReason::NoOffer);
        return None;
    };
    // Registered is enough; primary is not required.
    let serves = backend
        .primary_transport_key(instance)
        .is_some_and(|k| k.as_str() == key)
        || backend
            .alternative_transport_keys(instance)
            .is_some_and(|keys| keys.iter().any(|k| k.as_str() == key));
    if !serves {
        store.record_path(RdmaPathReason::NoOffer);
        return None;
    }
    Some(RdmaOffer {
        backends: vec![key.to_string()],
    })
}

/// Apply an armed descriptor fault, if there is one. Identity in a release
/// build, where the hook does not exist.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn with_descriptor_hook(manager: &RendezvousManager, descriptor: Vec<u8>) -> Vec<u8> {
    #[cfg(feature = "test-helpers")]
    if let Some(hook) = manager.take_descriptor_hook() {
        return hook.corrupt(descriptor);
    }
    let _ = manager;
    descriptor
}

/// What an armed transfer fault asks of the GET. Always `None` in a build
/// without `test-helpers`, where nothing can arm one.
#[cfg(all(target_os = "linux", feature = "ucx"))]
#[cfg_attr(not(feature = "test-helpers"), allow(dead_code))]
enum TransferHook {
    /// Report the transfer as failed without issuing it.
    Fail,
    /// Issue it, but not yet.
    Delay(std::time::Duration),
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
fn take_transfer_hook(manager: &RendezvousManager) -> Option<TransferHook> {
    #[cfg(feature = "test-helpers")]
    {
        use crate::rendezvous::RdmaTestHook;
        match manager.take_get_hook() {
            Some(RdmaTestHook::FailGet) => Some(TransferHook::Fail),
            Some(RdmaTestHook::SlowGet(delay)) => Some(TransferHook::Delay(delay)),
            _ => None,
        }
    }
    #[cfg(not(feature = "test-helpers"))]
    {
        let _ = manager;
        None
    }
}

/// Why an RDMA transfer could not be completed.
///
/// Every variant means the same thing to the caller — detach, re-acquire
/// chunked, once — so this exists to carry the *reason* to the metric and the
/// log rather than to be matched on.
#[cfg(all(target_os = "linux", feature = "ucx"))]
struct RdmaFallback {
    reason: RdmaPathReason,
    detail: String,
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl RdmaFallback {
    fn new(reason: RdmaPathReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

/// Read the range the owner's descriptor names into a pooled buffer.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn rdma_pull(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
    descriptor: &[u8],
    lease_timeout_ms: u64,
) -> Result<crate::rendezvous::rdma::PinnedBuf, RdmaFallback> {
    use crate::rendezvous::rdma::RdmaError;

    let (ctx, desc) = decode_for(manager, descriptor)?;
    let len = usize::try_from(desc.len).map_err(|_| {
        RdmaFallback::new(
            RdmaPathReason::DecodeError,
            format!(
                "descriptor length {} does not fit this address space",
                desc.len
            ),
        )
    })?;

    let buf = ctx.registry.alloc_pinned(len).await.map_err(|e| {
        let reason = match e {
            RdmaError::BudgetExceeded { .. } => RdmaPathReason::Budget,
            _ => RdmaPathReason::PoolExhausted,
        };
        RdmaFallback::new(reason, e.to_string())
    })?;

    let dest = crate::rendezvous::write::RdmaDestination::held(
        buf.backend_region_id(),
        buf.arena_offset(),
        buf.len() as u64,
        // The transfer's claim on the pool space. If this future is dropped
        // mid-flight, `buf` goes with it but the space does not come back until
        // the backend has finished writing into it.
        buf.hold(),
    );
    run_get(manager, handle, lease_id, &desc, dest, lease_timeout_ms).await?;
    Ok(buf)
}

/// Read the descriptor into the caller's destination, using the destination's
/// own registered memory when it has some.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn rdma_pull_into(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
    descriptor: &[u8],
    lease_timeout_ms: u64,
    dest: &mut impl RendezvousWrite,
) -> Result<(), RdmaFallback> {
    let (_ctx, desc) = decode_for(manager, descriptor)?;
    let len = usize::try_from(desc.len).map_err(|_| {
        RdmaFallback::new(
            RdmaPathReason::DecodeError,
            format!(
                "descriptor length {} does not fit this address space",
                desc.len
            ),
        )
    })?;

    // The zero-copy case: the destination is itself registered memory, so the
    // NIC writes into it and nothing is copied at all.
    if let Some(target) = dest.rdma_destination() {
        if target.capacity() < desc.len {
            return Err(RdmaFallback::new(
                RdmaPathReason::DecodeError,
                format!(
                    "destination holds {} bytes, descriptor names {}",
                    target.capacity(),
                    desc.len
                ),
            ));
        }
        return run_get(manager, handle, lease_id, &desc, target, lease_timeout_ms).await;
    }

    // Otherwise the GET lands in a pooled buffer and is copied once. This is
    // still the RDMA path — one memcpy against the several `_rv_pull` round
    // trips the chunked path would have cost — and the destination's own
    // semantics are exactly what they were. A `Vec`, a `BytesMut` and a
    // `&mut [u8]` all take this branch.
    //
    // Deliberately *no* capacity check here: `write_chunk` is what decides
    // whether a destination can take the bytes, and the growable destinations
    // answer that by resizing. Pre-empting it with `capacity()` would send a
    // `Vec::with_capacity(0)` down the fallback path over a number that says
    // nothing about whether the write would have succeeded. The registered
    // branch above does need its check, because the NIC writes without asking.
    let _ = len;
    let buf = rdma_pull(manager, handle, lease_id, descriptor, lease_timeout_ms).await?;
    // A destination that refuses the write refuses it on the chunked path too,
    // so this reaches the caller as an error either way — one extra round trip
    // for a destination that was never going to work.
    dest.write_chunk(0, &buf)
        .map_err(|e| RdmaFallback::new(RdmaPathReason::GetFailed, e.to_string()))
}

/// Decode a descriptor and check it against this instance's backend.
#[cfg(all(target_os = "linux", feature = "ucx"))]
fn decode_for<'a>(
    manager: &'a RendezvousManager,
    descriptor: &[u8],
) -> Result<
    (
        &'a crate::rendezvous::RdmaContext,
        crate::rendezvous::descriptor::RdmaDescriptor,
    ),
    RdmaFallback,
> {
    let ctx = manager.data_store().rdma().ok_or_else(|| {
        RdmaFallback::new(
            RdmaPathReason::PoolExhausted,
            "no rdma registry on this instance",
        )
    })?;
    let desc = crate::rendezvous::descriptor::RdmaDescriptor::decode(descriptor)
        .map_err(|e| RdmaFallback::new(RdmaPathReason::DecodeError, e.to_string()))?;
    if desc.backend != ctx.backend {
        return Err(RdmaFallback::new(
            RdmaPathReason::DecodeError,
            format!(
                "descriptor names backend {:?}, this instance serves {:?}",
                desc.backend, ctx.backend
            ),
        ));
    }
    Ok((ctx, desc))
}

/// Issue the GET, keeping the lease alive while it runs.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn run_get(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
    desc: &crate::rendezvous::descriptor::RdmaDescriptor,
    mut dest: crate::rendezvous::write::RdmaDestination<'_>,
    lease_timeout_ms: u64,
) -> Result<(), RdmaFallback> {
    let store = manager.data_store();
    let ctx = store.rdma().ok_or_else(|| {
        RdmaFallback::new(
            RdmaPathReason::PoolExhausted,
            "no rdma registry on this instance",
        )
    })?;
    let peer = manager
        .messenger()
        .backend()
        .try_translate_worker_id(handle.worker_id())
        .map_err(|e| {
            RdmaFallback::new(
                RdmaPathReason::GetFailed,
                format!("owner is not registered: {e}"),
            )
        })?;

    let req = crate::rendezvous::rdma::BackendGet {
        peer,
        remote_addr: desc.addr,
        packed_key: desc.packed_key.clone(),
        local_region_id: dest.region_id(),
        local_offset: dest.offset(),
        len: desc.len,
    };

    let delay = match take_transfer_hook(manager) {
        Some(TransferHook::Fail) => {
            return Err(RdmaFallback::new(
                RdmaPathReason::GetFailed,
                "injected transfer failure",
            ));
        }
        Some(TransferHook::Delay(delay)) => Some(delay),
        None => None,
    };

    // The transfer is *spawned*, not awaited in place, and the destination's
    // reservation moves into it.
    //
    // Dropping a `get_pinned` or `get_into` future must not free the
    // destination's granules: the backend's cancel-safety is arena-granular —
    // the transfer completes and the arena stays mapped — but the
    // suballocation would go back on the free list and the next allocation
    // would be handed memory a still-running NIC write is about to overwrite.
    //
    // A detached task is exactly the tool for that. Dropping a `JoinHandle`
    // detaches the task rather than cancelling it, so the caller going away
    // leaves the transfer running with its reservation intact, and the space
    // comes back when the backend says the write is over. The Phase-1 contract
    // is what makes that terminate: every submitted RMA op completes or is
    // cancelled, and either way the completion fires.
    let hold = dest.take_hold();
    let registry = Arc::clone(&ctx.registry);
    let transfer = ctx.registry.runtime().spawn(async move {
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let outcome = registry.get(req).await;
        // Released here and nowhere else: after the backend has finished with
        // the range, whether it succeeded or failed.
        drop(hold);
        outcome
    });

    let started = std::time::Instant::now();
    let outcome = with_lease_renewal(manager, handle, lease_id, lease_timeout_ms, async move {
        match transfer.await {
            Ok(outcome) => outcome,
            // The task panicked or the runtime is going down. The reservation
            // went with it either way, so this is a failed transfer and not a
            // leak.
            Err(join) => Err(crate::rendezvous::rdma::RdmaError::Backend(format!(
                "rdma transfer task: {join}"
            ))),
        }
    })
    .await;

    match outcome {
        Ok(()) => {
            if let Some(m) = store.metrics() {
                m.record_rendezvous_rdma_get(started.elapsed());
            }
            store.record_path(RdmaPathReason::Ok);
            Ok(())
        }
        Err(e) => Err(RdmaFallback::new(RdmaPathReason::GetFailed, e.to_string())),
    }
}

/// Run `transfer` while keeping its lease alive with `_rv_lease_renew` (D8).
///
/// The renewal is transfer-scoped and nothing else. It exists so a *slow link*
/// is not mistaken for a dead consumer, and it stops the moment the transfer
/// resolves — by completion, by failure, or by this future being dropped, since
/// the ticker is a `select!` arm rather than a spawned task and so cannot
/// outlive the thing it is renewing for.
///
/// **Holding a lease idle is not supported in v1.** Once the transfer is done
/// the standing deadline applies and the reaper is the backstop, so a caller
/// that wants to hold a lease across a long pause should release and re-acquire
/// instead. Renewing for as long as a caller holds a lease would leave the
/// reaper unable to tell a live holder from a crashed one, which is the entire
/// reason it exists.
///
/// A `lease_timeout_ms` of zero is an owner that set no deadline — there is
/// nothing to renew, and the transfer runs unwrapped.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn with_lease_renewal<F, T>(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
    lease_timeout_ms: u64,
    transfer: F,
) -> T
where
    F: std::future::Future<Output = T>,
{
    if lease_timeout_ms == 0 {
        return transfer.await;
    }
    // Half the deadline, floored so a tiny timeout does not spin. The owner
    // scans at the same cadence, so two renewals fit inside every deadline even
    // if one is lost.
    let period = std::time::Duration::from_millis(lease_timeout_ms / 2)
        .max(std::time::Duration::from_millis(5));
    let mut transfer = std::pin::pin!(transfer);
    loop {
        tokio::select! {
            out = &mut transfer => return out,
            _ = tokio::time::sleep(period) => {
                send_lease_renewal(manager, handle, lease_id).await;
            }
        }
    }
}

/// Fire one keepalive. Losing it is benign — the standing deadline still
/// applies and the next tick is half a deadline away — so a failure is a debug
/// line rather than something that interrupts the transfer.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn send_lease_renewal(manager: &RendezvousManager, handle: DataHandle, lease_id: u64) {
    let payload = match serde_json::to_vec(&RvLeaseRenewRequest {
        handle: RvHandleWire::from_handle(handle),
        lease_id,
    }) {
        Ok(payload) => payload,
        Err(e) => {
            tracing::debug!(error = %e, "rendezvous: could not encode a lease renewal");
            return;
        }
    };
    let sent = async {
        manager
            .messenger()
            .am_send_streaming("_rv_lease_renew")?
            .raw_payload(Bytes::from(payload))
            .worker(handle.worker_id())
            .send()
            .await
    }
    .await;
    if let Err(e) = sent {
        tracing::debug!(
            lease = lease_id,
            error = %e,
            "rendezvous: lease renewal not delivered; the standing deadline still applies"
        );
    }
}

/// Detach the lease and pull the same handle chunked instead. **One attempt.**
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn fallback_chunked(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
    fallback: RdmaFallback,
) -> Result<(Bytes, u64)> {
    let store = manager.data_store();
    store.record_path(fallback.reason);
    tracing::warn!(
        %handle,
        lease = lease_id,
        reason = fallback.reason.as_str(),
        detail = %fallback.detail,
        "rendezvous: falling back to the chunked path for this transfer"
    );

    // The owner's lease is tied to a transfer that will never happen. Detach it
    // before asking for another, or the slot carries two read locks and the
    // first is released only when its deadline passes.
    if let Err(e) = Consumer::detach(manager.messenger(), handle, lease_id).await {
        tracing::warn!(%handle, error = %e, "rendezvous: could not detach before falling back");
    }
    chunked_only(manager, handle).await
}

/// An owner answered `Rdma` to an acquire that carried no offer.
///
/// Reachable only from a build without the RDMA path, or from an owner that
/// ignored a missing offer. Either way the lease names a transfer this side
/// cannot perform.
#[cfg(not(all(target_os = "linux", feature = "ucx")))]
async fn unsolicited_rdma(
    manager: &RendezvousManager,
    handle: DataHandle,
    lease_id: u64,
) -> Result<(Bytes, u64)> {
    tracing::warn!(
        %handle,
        "rendezvous: owner answered with an RDMA descriptor for an acquire that offered \
         nothing; falling back to the chunked path"
    );
    if let Err(e) = Consumer::detach(manager.messenger(), handle, lease_id).await {
        tracing::warn!(%handle, error = %e, "rendezvous: could not detach before falling back");
    }
    chunked_only(manager, handle).await
}

/// Acquire with no offer and pull the chunks.
///
/// The terminal path: it does not fall back to anything. An owner that answers
/// a no-offer acquire with a descriptor is broken, and this reports that rather
/// than recursing — a retry loop here would turn one such owner into an
/// unbounded storm of round trips.
async fn chunked_only(manager: &RendezvousManager, handle: DataHandle) -> Result<(Bytes, u64)> {
    let messenger = manager.messenger();
    let target_worker = handle.worker_id();
    match acquire(manager, handle, None).await? {
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
                    if let Err(cleanup_err) = Consumer::detach(messenger, handle, lease_id).await {
                        tracing::warn!(
                            "Failed to detach lease {lease_id} after pull failure: {cleanup_err}"
                        );
                    }
                    Err(e)
                }
            }
        }
        AcquireResponse::Rdma { lease_id, .. } => {
            if let Err(e) = Consumer::detach(messenger, handle, lease_id).await {
                tracing::warn!(
                    %handle,
                    error = %e,
                    "rendezvous: could not detach an unusable lease"
                );
            }
            anyhow::bail!(
                "rendezvous owner answered {handle} with an RDMA descriptor for an acquire that \
                 carried no offer; refusing to retry"
            )
        }
    }
}

/// Copy bytes the chunked path produced into a pooled buffer, so
/// [`Consumer::get_pinned`] returns the same type whichever way it got them.
#[cfg(all(target_os = "linux", feature = "ucx"))]
async fn copy_into_pool(
    manager: &RendezvousManager,
    data: &[u8],
) -> Result<crate::rendezvous::rdma::PinnedBuf> {
    let ctx = manager
        .data_store()
        .rdma()
        .ok_or_else(|| anyhow::anyhow!("get_pinned needs an rdma registry on this instance"))?;
    let mut buf = ctx.registry.alloc_pinned(data.len()).await?;
    buf.copy_from_slice(data);
    Ok(buf)
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
