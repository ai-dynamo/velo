// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Remote memory access (RMA) plumbing on the UCX progress thread.
//!
//! [`RdmaEndpoint`] is the cheap-clone handle the rest of the crate uses to
//! register memory and to pull remote memory with `ucp_get_nbx`. It carries no
//! UCX state: every `ucp_*` handle (`ucp_mem_h`, `ucp_rkey_h`) is created,
//! used and destroyed inside [`worker`](super::worker), which owns the single
//! `UCS_THREAD_MODE_SINGLE` worker. What crosses this boundary is a region id,
//! a byte range, and a *packed* rkey as plain [`Bytes`].
//!
//! ## Submission path
//!
//! Commands ride the progress thread's existing bounded ring
//! (`send_async` + [`Doorbell::ring`](super::worker::Doorbell::ring)) and are
//! answered by a `oneshot`. They deliberately do **not** go through
//! `Transport::send_message` or an `AdmissionGate`: those implement Active
//! Message frame semantics (eager caps, drain rejection, `SendOutcome`) which
//! have nothing to say about an RMA operation. The bounded ring is the
//! backpressure mechanism, exactly as for `check_health`.
//!
//! ## Why the not-started and shutting-down checks come first
//!
//! Before `Transport::start` the ring exists but has no consumer, so a push
//! *succeeds* and the reply oneshot is never resolved — a hang, not an error.
//! After `shutdown()` the receiver is dropped, which does surface as an error,
//! but only once the drop has happened. Both states are therefore checked
//! before the push, so every method resolves promptly with a diagnosis.
//!
//! ## Offsets are relative to the pointer the caller mapped
//!
//! `ucp_mem_map` rounds the pinned range outward to page boundaries, so the
//! range `ucp_mem_query` reports can start below and end above the caller's
//! allocation. [`MappedRegion`] reports that effective range as a fact, but
//! [`RmaGetRequest::local_offset`] is measured from the pointer passed to
//! [`RdmaEndpoint::map_region`] and bounds-checked against that length. A
//! caller can therefore never name a byte inside the registration but outside
//! its own allocation, which would be a silent write into unrelated heap.
//!
//! ## Cancellation
//!
//! Every method here is a future the caller may drop (a `select!` arm, a
//! `timeout`). Dropping one must never orphan a pinned region — the caller that
//! saw a failure is entitled by [`map_region`](RdmaEndpoint::map_region)'s own
//! contract to free the buffer, and freeing memory UCX still has pinned is the
//! whole hazard class this module exists to contain. So:
//!
//! * **`map_region`** — the region id is minted *before* the push, so a dropped
//!   future can compensate: a `Drop` guard pushes an [`Cmd::UnmapRegion`] for
//!   that id. The progress thread independently rolls back when it finds the
//!   reply channel already closed at send time. Between them, cancelling
//!   `map_region` means "no region exists", with a full ring the only gap.
//! * **`unmap_region`** — idempotent and fire-and-forget once pushed: the
//!   progress thread performs the unmap whether or not anyone is still waiting,
//!   and a repeat call for an id that is already gone answers `Ok(())`. The
//!   advisory range entry is dropped *before* the push, so a cancelled unmap
//!   still gates the region against new GETs.
//! * **`get`** — dropping it abandons only the notification. The operation keeps
//!   running and keeps the region's in-flight count raised, which is exactly
//!   what stops the destination being unmapped underneath UCX.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use dashmap::DashMap;
use velo_ext::InstanceId;

use super::worker::{Cmd, WorkerShared};

/// Largest packed rkey this side will accept.
///
/// Bounds the copy and the pre-parse loop. Descriptors are velo-authored and a
/// real packed rkey is tens of bytes even with several memory domains, so a blob
/// past this size is malformed by definition.
pub(crate) const MAX_PACKED_RKEY: usize = 1024;

/// Slack in the buffer `prepare_get` unpacks from, filled with `0xFF`.
///
/// Belt-and-braces only — [`preparse_packed_rkey`] is the containment. See its
/// docs for why a length bound cannot be one, and why the filler is `0xFF`
/// rather than zero.
pub(crate) const RKEY_UNPACK_PAD: usize = 512;

/// `UCS_SYS_DEVICE_ID_UNKNOWN`: the value that terminates UCX's distance walk.
const SYS_DEV_UNKNOWN: u8 = u8::MAX;

/// `sizeof(ucp_rkey_packed_distance_t)` — `{ u8 sys_dev, fp8 latency, fp8
/// bandwidth }`, `UCS_S_PACKED` (`ucp_rkey.c:27`).
const PACKED_DISTANCE_LEN: usize = 3;

/// Why an RMA operation could not be performed.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RmaError {
    /// `Transport::start` has not completed, so no progress thread exists.
    #[error("ucx transport not started")]
    NotStarted,

    /// The GET target was never registered with this transport.
    #[error("peer not registered: {0}")]
    PeerNotRegistered(InstanceId),

    /// No endpoint could be created for the peer.
    #[error("ucx endpoint unavailable: {0}")]
    EndpointUnavailable(String),

    /// The requested local range falls outside the mapped region.
    #[error("rma range outside the mapped region")]
    OutOfRange,

    /// The region id names no region on this transport — it was never minted,
    /// or its unmap has already been performed.
    ///
    /// Only [`RdmaEndpoint::get`] produces this;
    /// [`unmap_region`](RdmaEndpoint::unmap_region) is idempotent and answers
    /// `Ok(())` for an id that is already gone.
    #[error("rma region not found")]
    RegionNotFound,

    /// The packed rkey is empty or implausibly large; see [`MAX_PACKED_RKEY`].
    #[error("malformed packed rkey")]
    InvalidRkey,

    /// A `ucp_*` call failed.
    #[error("ucx: {status_name}")]
    Ucx {
        /// `ucs_status_string` of the failing status.
        status_name: &'static str,
    },

    /// The transport is shutting down; the operation was refused or abandoned.
    #[error("ucx transport shutting down")]
    ShuttingDown,

    /// The progress thread is gone and the command could not be delivered.
    #[error("ucx progress thread unavailable")]
    ChannelClosed,
}

/// A locally registered memory region, as reported by the progress thread.
#[derive(Debug, Clone)]
pub(crate) struct MappedRegion {
    /// Names the region in subsequent [`RdmaEndpoint`] calls.
    pub region_id: u64,
    /// Start of the range `ucp_mem_query` reports UCX actually pinned. Always
    /// `<=` the pointer that was mapped (UCX rounds outward).
    pub effective_addr: u64,
    /// Length of the pinned range; always covers the requested range.
    pub effective_len: u64,
    /// `ucp_rkey_pack` output, copied out before `ucp_rkey_buffer_release`.
    /// Endpoint-independent, so it may be cached, sent on the wire, or held
    /// anywhere — unlike an unpacked `ucp_rkey_h`.
    pub packed_rkey: Bytes,
}

/// One remote read: `len` bytes at `remote_addr` on `peer`, landing at
/// `local_offset` inside `local_region`.
#[derive(Debug, Clone)]
pub(crate) struct RmaGetRequest {
    /// Instance to read from. Must be registered with this transport.
    pub peer: InstanceId,
    /// Absolute address in the peer's address space, authored by the peer.
    pub remote_addr: u64,
    /// The peer's packed rkey covering `remote_addr`.
    pub packed_rkey: Bytes,
    /// Region id from a previous [`RdmaEndpoint::map_region`] on this side.
    pub local_region: u64,
    /// Destination offset, measured from the pointer that was mapped — not
    /// from [`MappedRegion::effective_addr`]. See the module docs.
    pub local_offset: u64,
    /// Bytes to read. Zero completes successfully without touching UCX.
    pub len: u64,
}

/// Submit-side state shared by every [`RdmaEndpoint`] clone of one transport.
pub(crate) struct RmaState {
    /// Set once `Transport::start` has produced a running progress thread.
    started: AtomicBool,
    /// The transport's runtime, so a cancelled `map_region` can await its
    /// compensating unmap onto a full ring instead of dropping it. `Drop` cannot
    /// block, but it can spawn.
    runtime: OnceLock<tokio::runtime::Handle>,
    /// Mints [`MappedRegion::region_id`]; monotonic, never reused.
    next_region_id: AtomicU64,
    /// `region_id -> (mapped pointer, mapped length)`.
    ///
    /// Advisory only: it exists so an out-of-range or unknown-region request is
    /// rejected without occupying a ring slot. The progress thread's own
    /// `regions` map is authoritative and re-checks everything.
    ranges: DashMap<u64, (u64, u64)>,
}

impl RmaState {
    pub(crate) fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            runtime: OnceLock::new(),
            next_region_id: AtomicU64::new(1),
            ranges: DashMap::new(),
        }
    }

    /// Called by `UcxTransport::start` once the progress thread is consuming
    /// the ring.
    pub(crate) fn mark_started(&self, runtime: Option<tokio::runtime::Handle>) {
        if let Some(handle) = runtime {
            let _ = self.runtime.set(handle);
        }
        self.started.store(true, Ordering::Release);
    }
}

/// Handle for RMA operations on a started [`UcxTransport`](super::UcxTransport).
///
/// Obtained from `UcxTransport::rdma_endpoint`. Cloning is two atomic
/// increments; every clone addresses the same progress thread and the same
/// region-id space.
pub(crate) struct RdmaEndpoint {
    shared: Arc<WorkerShared>,
    state: Arc<RmaState>,
}

impl Clone for RdmaEndpoint {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            state: Arc::clone(&self.state),
        }
    }
}

impl RdmaEndpoint {
    pub(crate) fn new(shared: Arc<WorkerShared>, state: Arc<RmaState>) -> Self {
        Self { shared, state }
    }

    /// Register `[ptr, ptr + len)` for RMA and pack a remote key for it.
    ///
    /// The memory must stay allocated and un-freed until the matching
    /// [`unmap_region`](Self::unmap_region) resolves (or the transport has shut
    /// down): UCX holds the pages pinned and a peer holding the packed rkey can
    /// read them at any time. Phase 2's registration layer owns that contract
    /// for callers outside this module; nothing here can enforce it.
    ///
    /// **Cancellation rolls the registration back.** Dropping this future means
    /// no region exists under the id it was minting — the progress thread either
    /// never created one, or unmaps it again (see the module docs). The caller
    /// may therefore treat a dropped `map_region` exactly like a returned error
    /// and free the buffer.
    pub(crate) async fn map_region(
        &self,
        ptr: usize,
        len: usize,
    ) -> Result<MappedRegion, RmaError> {
        if ptr == 0 || len == 0 || ptr.checked_add(len).is_none() {
            return Err(RmaError::OutOfRange);
        }
        self.ready()?;

        let region_id = self.state.next_region_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared
            .ring_tx
            .send_async(Cmd::MapRegion {
                ptr,
                len,
                region_id,
                reply: tx,
            })
            .await
            .map_err(|_| self.gone())?;
        self.shared.doorbell.ring();

        // Armed across the only await that can be cancelled while a region may
        // already exist. Disarmed the instant the outcome is in hand, after
        // which there is no suspension point before the caller owns it.
        let mut rollback = MapRollback {
            endpoint: self,
            region_id,
            armed: true,
        };
        // Disarmed only by an answer the worker actually sent. A `RecvError`
        // means the sender was dropped without one — the progress thread died
        // between inserting the region and replying — which is precisely when a
        // compensating unmap is still owed.
        let Ok(outcome) = rx.await else {
            return Err(self.gone());
        };
        rollback.armed = false;

        let region = outcome?;
        self.state
            .ranges
            .insert(region_id, (ptr as u64, len as u64));
        Ok(region)
    }

    /// Deregister a region. Idempotent.
    ///
    /// Resolves once the region has no local RMA operation left in flight and
    /// `ucp_mem_unmap` has returned. An id that names no region — never minted,
    /// or already unmapped — answers `Ok(())`: "nothing is mapped here" is the
    /// state the caller asked for. From the moment the command reaches the
    /// progress thread, new GETs into the region are refused.
    ///
    /// Cancellation has two outcomes, split at the moment the command is
    /// enqueued. Dropped **before** the push completes — which a full ring can
    /// make into a real wait — nothing happens at all: the region stays mapped
    /// and usable, and the caller must retry. Dropped **after**, the unmap
    /// proceeds regardless and only the notification is lost. The advisory range
    /// entry is removed at exactly that boundary, so it never claims a region is
    /// gone while the progress thread still has it mapped and ungated.
    ///
    /// Concurrent callers for the same id all resolve together.
    pub(crate) async fn unmap_region(&self, region_id: u64) -> Result<(), RmaError> {
        self.ready()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared
            .ring_tx
            .send_async(Cmd::UnmapRegion {
                region_id,
                reply: tx,
            })
            .await
            .map_err(|_| self.gone())?;
        // Enqueued: from here the unmap happens whatever becomes of this future,
        // so the id stops being usable now and not a moment earlier.
        self.state.ranges.remove(&region_id);
        self.shared.doorbell.ring();
        rx.await.unwrap_or_else(|_| Err(self.gone()))
    }

    /// Read `req.len` bytes of `req.peer`'s memory into a local region.
    ///
    /// Resolves when the GET has completed at *both* ends — `ucp_get_nbx`
    /// completion is authoritative for the remote side too, so no flush or
    /// fence follows.
    ///
    /// Dropping this future abandons the notification only. The transfer runs to
    /// completion and holds the destination region against unmapping until it
    /// does, which is what keeps UCX from writing into a deregistered range.
    pub(crate) async fn get(&self, req: RmaGetRequest) -> Result<(), RmaError> {
        let (_, mapped_len) = self
            .state
            .ranges
            .get(&req.local_region)
            .map(|entry| *entry.value())
            .ok_or(RmaError::RegionNotFound)?;
        let end = req
            .local_offset
            .checked_add(req.len)
            .ok_or(RmaError::OutOfRange)?;
        if end > mapped_len {
            return Err(RmaError::OutOfRange);
        }
        // Ahead of the rkey check, so the documented "zero bytes touches no UCX"
        // holds even for a request carrying no usable key.
        if req.len == 0 {
            return Ok(());
        }
        validate_packed_rkey(&req.packed_rkey)?;
        self.submit(|reply| Cmd::RmaGet { req, reply }).await
    }

    /// Push one command and await its reply. See the module docs for why the
    /// two state checks precede the push.
    async fn submit<T>(
        &self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<Result<T, RmaError>>) -> Cmd,
    ) -> Result<T, RmaError> {
        self.ready()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.shared
            .ring_tx
            .send_async(build(tx))
            .await
            .map_err(|_| self.gone())?;
        self.shared.doorbell.ring();
        rx.await.unwrap_or_else(|_| Err(self.gone()))
    }

    /// The two states in which a push would never be answered.
    fn ready(&self) -> Result<(), RmaError> {
        if !self.state.started.load(Ordering::Acquire) {
            return Err(RmaError::NotStarted);
        }
        if self.shared.shutdown_requested.load(Ordering::Acquire) {
            return Err(RmaError::ShuttingDown);
        }
        Ok(())
    }

    /// Diagnosis for a command that could not be delivered or answered.
    fn gone(&self) -> RmaError {
        if self.shared.shutdown_requested.load(Ordering::Acquire) {
            RmaError::ShuttingDown
        } else {
            RmaError::ChannelClosed
        }
    }
}

/// Compensating unmap for a [`RdmaEndpoint::map_region`] whose caller went away.
///
/// The region id is minted before the push, so the id of a region that may now
/// exist is known even though the reply never arrived. Pushing an idempotent
/// `UnmapRegion` for it is therefore always safe: the progress thread either
/// unmaps a real region, or answers `Ok(())` for one that was never created (or
/// that its own rollback already removed).
///
/// `try_send` rather than an await, because `Drop` cannot block — but it can
/// spawn, so a full ring falls back to an awaited push on the transport's
/// runtime. What is left after that is a *closed* ring, which only happens once
/// the progress thread has begun tearing down, and teardown force-unmaps every
/// region it still holds.
struct MapRollback<'a> {
    endpoint: &'a RdmaEndpoint,
    region_id: u64,
    armed: bool,
}

impl Drop for MapRollback<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let region_id = self.region_id;
        let shared = Arc::clone(&self.endpoint.shared);
        match shared.ring_tx.try_send(Cmd::UnmapRegion {
            region_id,
            reply: tx,
        }) {
            Ok(()) => shared.doorbell.ring(),
            Err(flume::TrySendError::Full(cmd)) => match self.endpoint.state.runtime.get() {
                Some(runtime) => {
                    runtime.spawn(async move {
                        if shared.ring_tx.send_async(cmd).await.is_ok() {
                            shared.doorbell.ring();
                        }
                    });
                }
                None => tracing::warn!(
                    "ucx: ring full and no runtime to retry on; region {region_id} stays \
                         mapped until teardown"
                ),
            },
            // A disconnected ring means the progress thread is already tearing
            // down, and teardown unmaps whatever it still holds.
            Err(flume::TrySendError::Disconnected(_)) => {}
        }
    }
}

/// Refuse a packed rkey `ucp_ep_rkey_unpack` could not parse within its own
/// bytes. Bounds first, then [`preparse_packed_rkey`].
pub(crate) fn validate_packed_rkey(packed: &[u8]) -> Result<(), RmaError> {
    if packed.is_empty() || packed.len() > MAX_PACKED_RKEY {
        return Err(RmaError::InvalidRkey);
    }
    preparse_packed_rkey(packed)
}

/// Prove that UCX's parse of `packed` terminates inside `packed`.
///
/// `ucp_ep_rkey_unpack` takes no length — the public API has no parameter for
/// one — so nothing stops a malformed blob walking off the end of whatever
/// buffer it lives in, and UCX's internal assertions are compiled out of a
/// release build. Neither a length check nor trailing padding can fix that, for
/// two separate reasons found in the 1.22.0 source:
///
/// * **Stage 1** (`ucp_rkey_pack_memh`'s format, read back in
///   `ucp_ep_rkey_unpack_internal`) is `md_map: u64le`, `mem_type: u8`, then one
///   `len: u8` + `len` bytes per set bit of `md_map`, then `sys_dev: u8` iff
///   `md_map != 0`. The walk is driven by `md_map`, never by a buffer end, and a
///   length byte sitting at the last content byte may declare 255 — so no fixed
///   pad size is provably enough.
/// * **Stage 2** (`ucp_rkey_unpack_lanes_distance`, reached through
///   `ucp_rkey_proto_resolve` whenever the peer is UCX >= 1.20 and the blob's
///   `sys_dev` is not `UNKNOWN`) sets `buffer_end = UINTPTR_MAX`
///   (`ucp_rkey.c:897`) and walks 3-byte records, breaking **only** on a `0xFF`
///   byte (`ucp_rkey.c:820`). Zero padding does not stop it; it feeds it.
///
/// So containment has to come from the blob itself. This walks the same format
/// and requires every read UCX will perform to land inside `packed`: the stage-1
/// entries, the `sys_dev` byte, and — when that byte is not `0xFF` and stage 2
/// therefore runs — a `0xFF` terminator reachable on the 3-byte stride. UCX's
/// own packer always produces such a blob (`ucp_rkey_pack_memh` writes the
/// terminator at `ucp_rkey.c:264`), so nothing legitimate is refused.
pub(crate) fn preparse_packed_rkey(packed: &[u8]) -> Result<(), RmaError> {
    let bad = || RmaError::InvalidRkey;

    let md_map = u64::from_le_bytes(
        packed
            .get(..8)
            .ok_or_else(bad)?
            .try_into()
            .map_err(|_| bad())?,
    );
    // `mem_type`.
    let mut at = 9usize;
    if at > packed.len() {
        return Err(bad());
    }

    for _ in 0..md_map.count_ones() {
        let tl_len = *packed.get(at).ok_or_else(bad)? as usize;
        at = at
            .checked_add(1)
            .and_then(|a| a.checked_add(tl_len))
            .ok_or_else(bad)?;
        if at > packed.len() {
            return Err(bad());
        }
    }

    if md_map == 0 {
        // No `sys_dev` byte is read and stage 2 never runs.
        return Ok(());
    }

    let sys_dev = *packed.get(at).ok_or_else(bad)?;
    at += 1;
    if sys_dev == SYS_DEV_UNKNOWN {
        // Stage 2 is skipped for an unknown system device.
        return Ok(());
    }

    // Stage 2 will run unbounded from here until it reads a `0xFF` byte. Prove
    // it reaches one without leaving the blob.
    loop {
        match packed.get(at) {
            Some(&SYS_DEV_UNKNOWN) => return Ok(()),
            // A record it will consume whole must fit.
            Some(_) if at + PACKED_DISTANCE_LEN <= packed.len() => at += PACKED_DISTANCE_LEN,
            _ => return Err(bad()),
        }
    }
}
