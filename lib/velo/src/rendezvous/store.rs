// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Data store: the owner-side registry of staged rendezvous data.
//!
//! [`DataStore`] holds a [`DashMap`] of [`DataSlot`] entries keyed by local ID,
//! a [`DashMap`] of [`TransferState`] entries for active chunked transfers, and
//! the deadlines of the leases that carry one.
//!
//! # One fact, one field
//!
//! A slot's body is a [`SlotBody`], not a `Bytes` plus a mode flag plus an
//! optional descriptor. The three-field shape makes states representable that
//! cannot be true — pinned with no registration, in-memory with a descriptor —
//! and every reader then has to decide which field to believe. The enum makes
//! the mode a *consequence* of the body: [`StageMode`] is derived, never
//! stored, so it cannot disagree with what is actually staged.
//!
//! # Pinned slots still answer the chunked path
//!
//! Every read path here serves both bodies. A pinned slot is host memory that a
//! peer *may* read with an RDMA GET, not memory that only an RDMA consumer can
//! reach: an old consumer, a consumer without the UCX transport, one whose GET
//! failed, and one below the RDMA size threshold all pull it chunk by chunk.
//! Bifurcating the two — a pinned slot that refuses non-RDMA readers — was PR
//! #40's worst property and is explicitly excluded by the plan.
//!
//! # Lease deadlines are for RDMA leases only
//!
//! A chunked transfer is visible to the owner: every chunk is an inbound
//! request, and an abandoned transfer stops making them. An RDMA GET is issued
//! by the *consumer's* NIC and the owner sees nothing at all, so an RDMA lease
//! carries a deadline and a reaper force-releases it (D8). Chunked leases keep
//! their existing no-deadline behaviour deliberately: giving them one would
//! change the semantics of a path this phase is not otherwise touching.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;

use crate::observability::VeloMetrics;
use crate::rendezvous::protocol::DataMetadata;

/// Default chunk size for chunked transfers (512 KiB).
pub const DEFAULT_CHUNK_SIZE: u32 = 512 * 1024;

/// How data is staged in memory.
///
/// Reporting only. Derived from [`SlotBody`] on every query rather than stored
/// beside it, so the two cannot drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageMode {
    /// Plain heap bytes, served via chunked pull.
    InMemory,
    /// RDMA-registered memory: served by an RDMA GET to a consumer that can do
    /// one, and by chunked pull to everybody else.
    Pinned,
}

/// The staged payload of a [`DataSlot`].
///
/// # Why the pinned variant is feature-gated rather than always present
///
/// `PinnedSlot` owns a pool allocation or an external-region guard, both of
/// which only exist when there is an RDMA backend to have registered them. A
/// variant that could never be constructed would still force every match here
/// to carry an arm for it, and every one of those arms would be dead code
/// asserting something impossible. Gating the variant instead means the
/// non-`ucx` build's match arms are the honest ones: there is one body, and it
/// is bytes. The cost is `#[cfg]` on a handful of match arms below, which is
/// the smaller and more local price.
pub(crate) enum SlotBody {
    /// Plain heap bytes.
    InMemory(Bytes),
    /// Registered memory a peer may read directly.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    Pinned(super::pinned::PinnedSlot),
}

impl SlotBody {
    /// Reporting mode for this body.
    pub(crate) fn stage_mode(&self) -> StageMode {
        match self {
            Self::InMemory(_) => StageMode::InMemory,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            Self::Pinned(slot) => slot.stage_mode(),
        }
    }

    /// Staged length in bytes.
    pub(crate) fn total_len(&self) -> u64 {
        match self {
            Self::InMemory(data) => data.len() as u64,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            Self::Pinned(slot) => slot.len(),
        }
    }

    /// Copy `len` bytes from `offset` into fresh memory.
    ///
    /// `None` for a range outside the body, or for pinned memory whose
    /// registration has gone away underneath it.
    fn read_at(&self, offset: u64, len: usize) -> Option<Bytes> {
        match self {
            Self::InMemory(data) => {
                let start = usize::try_from(offset).ok()?;
                let end = start.checked_add(len)?;
                if end > data.len() {
                    return None;
                }
                // Cheap: a `Bytes` slice is a refcount bump, not a copy.
                Some(data.slice(start..end))
            }
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            Self::Pinned(slot) => slot.read_at(offset, len),
        }
    }

    /// Copy the whole body out.
    fn to_bytes(&self) -> Option<Bytes> {
        match self {
            Self::InMemory(data) => Some(data.clone()),
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            Self::Pinned(slot) => slot.to_bytes(),
        }
    }
}

/// A single slot in the data store registry.
pub(crate) struct DataSlot {
    /// The staged payload, and with it how it is staged.
    pub body: SlotBody,
    /// Reference count. Defaults to 1. Decremented by release, freed at 0.
    pub refcount: AtomicU32,
    /// Active read lock count. Prevents cleanup while transfers are in flight.
    pub read_lock_count: AtomicU32,
    /// Cached total length for metadata queries.
    pub total_len: u64,
    /// When this slot was created.
    #[allow(dead_code)]
    pub created_at: Instant,
    /// Optional time-to-live. Data is eligible for reaping after this duration.
    #[allow(dead_code)]
    pub ttl: Option<Duration>,
}

/// State for an active chunked transfer.
#[allow(dead_code)]
pub(crate) struct TransferState {
    /// Which DataSlot this transfer reads from.
    pub slot_local_id: u64,
    /// The lease ID associated with the read lock.
    pub lease_id: u64,
    /// Size of each chunk in bytes.
    pub chunk_size: u32,
    /// Total number of chunks.
    pub chunk_count: u32,
    /// When this transfer was created.
    pub created_at: Instant,
}

/// A lease that must be renewed or it will be reaped.
#[derive(Clone, Copy, Debug)]
struct LeaseDeadline {
    /// The slot the lease holds a read lock on.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    local_id: u64,
    /// When the owner stops waiting.
    expires_at: Instant,
    /// How far a renewal pushes the deadline out.
    ///
    /// Carried on the entry rather than read from config at renewal time, so
    /// the `_rv_lease_renew` handler needs nothing but the store: the lease was
    /// granted under a timeout the consumer was told about in the acquire
    /// response, and renewing it under a *different* one — a config reloaded
    /// between grant and renewal — would silently change the contract the
    /// consumer is pacing its keepalives against.
    timeout: Duration,
}

/// What [`DataStore::consume_lease`] found.
///
/// Distinguished so a caller can log the difference, and so the two failing
/// cases cannot be collapsed into one by accident: a lease held by another slot
/// is still live and still has a deadline, while an unknown one has neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseOutcome {
    /// The lease named the expected slot, and has been consumed. The caller
    /// owns the release that follows.
    Consumed,
    /// The lease exists but holds a different slot. **Nothing was consumed**:
    /// the lease and its deadline are untouched, so the reaper still backstops
    /// it.
    Mismatch {
        /// The slot the lease actually holds.
        actual: u64,
    },
    /// No such lease — never issued, or already ended.
    Unknown,
}

/// Options for registering data.
#[derive(Debug, Clone)]
pub struct RegisterOptions {
    /// Optional time-to-live for the staged data.
    pub ttl: Option<Duration>,
}

impl RegisterOptions {
    pub fn new() -> Self {
        Self { ttl: None }
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl Default for RegisterOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Owner-side registry of staged rendezvous data.
///
/// Holds data slots keyed by local ID and active chunked transfer state.
/// Thread-safe via [`DashMap`] and atomic counters.
pub struct DataStore {
    /// Monotonically increasing slot ID counter. Starts at 1 (0 is reserved).
    next_id: AtomicU64,
    /// Active data slots.
    pub(crate) slots: DashMap<u64, DataSlot>,
    /// Active chunked transfers keyed by transfer_id.
    pub(crate) transfers: DashMap<u64, TransferState>,
    /// Monotonically increasing transfer ID counter.
    next_transfer_id: AtomicU64,
    /// Monotonically increasing lease ID counter.
    next_lease_id: AtomicU64,
    /// Outstanding leases: lease_id → local_id. Consumed on detach/release.
    active_leases: DashMap<u64, u64>,
    /// Deadlines for the leases that carry one — RDMA leases only. Entries are
    /// removed by the same [`consume_lease`](Self::consume_lease) that ends the
    /// lease, so a deadline never outlives the thing it bounds.
    lease_deadlines: DashMap<u64, LeaseDeadline>,
    /// Observability, for the decisions taken inside the handlers this store
    /// backs. `None` when the instance was built without metrics.
    ///
    /// Only the RDMA path reads it today, hence the conditional allow.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    metrics: Option<Arc<VeloMetrics>>,
    /// The RDMA registry and policy, once they exist.
    ///
    /// # Why this lives on the store rather than beside it
    ///
    /// The `_rv_acquire` handler is a closure built at `register_handlers`
    /// time, which is *before* `VeloBuilder::build` has constructed the
    /// registry — the registry wraps an RMA endpoint on a transport that must
    /// already have started. So the handler cannot capture the context by
    /// value, and capturing the `RendezvousManager` instead would make a
    /// reference cycle through the messenger that holds the handler, which
    /// leaks the store for the life of the process.
    ///
    /// The store is the one thing the handler already holds an `Arc` to, and it
    /// is per-instance, so it is where the late-bound context goes. Set once,
    /// by `RendezvousManager::set_rdma_context`.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    rdma: std::sync::OnceLock<super::RdmaContext>,
}

impl DataStore {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            slots: DashMap::new(),
            transfers: DashMap::new(),
            next_transfer_id: AtomicU64::new(1),
            next_lease_id: AtomicU64::new(1),
            active_leases: DashMap::new(),
            lease_deadlines: DashMap::new(),
            metrics: None,
            #[cfg(all(target_os = "linux", feature = "ucx"))]
            rdma: std::sync::OnceLock::new(),
        }
    }

    /// A store that emits into `metrics`.
    pub(crate) fn with_metrics(metrics: Option<Arc<VeloMetrics>>) -> Self {
        Self {
            metrics,
            ..Self::new()
        }
    }

    /// The metrics handle, if this instance was built with one.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    pub(crate) fn metrics(&self) -> Option<&Arc<VeloMetrics>> {
        self.metrics.as_ref()
    }

    /// Bind the RDMA registry and policy. Called once, from
    /// `RendezvousManager::set_rdma_context`.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn set_rdma(&self, ctx: super::RdmaContext) -> Result<(), super::RdmaContext> {
        self.rdma.set(ctx)
    }

    /// The RDMA registry and policy, if this instance has them.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn rdma(&self) -> Option<&super::RdmaContext> {
        self.rdma.get()
    }

    /// Count one path decision. A no-op without metrics.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn record_path(&self, reason: crate::observability::RdmaPathReason) {
        if let Some(m) = &self.metrics {
            m.record_rendezvous_rdma_path(reason);
        }
    }

    /// Register data and return the local slot ID.
    pub fn register(&self, data: Bytes, opts: Option<RegisterOptions>) -> u64 {
        self.register_body(SlotBody::InMemory(data), opts)
    }

    /// Register an already-built body and return the local slot ID.
    ///
    /// The staging path — pool, external region, or plain bytes — decides the
    /// body; everything after that is the same slot.
    pub(crate) fn register_body(&self, body: SlotBody, opts: Option<RegisterOptions>) -> u64 {
        let local_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let total_len = body.total_len();
        let ttl = opts.as_ref().and_then(|o| o.ttl);
        self.slots.insert(
            local_id,
            DataSlot {
                body,
                refcount: AtomicU32::new(1),
                read_lock_count: AtomicU32::new(0),
                total_len,
                created_at: Instant::now(),
                ttl,
            },
        );
        local_id
    }

    /// Query metadata for a slot (no lock acquired).
    pub fn metadata(&self, local_id: u64) -> Option<DataMetadata> {
        self.slots.get(&local_id).map(|slot| DataMetadata {
            total_len: slot.total_len,
            refcount: slot.refcount.load(Ordering::Relaxed),
            pinned: slot.body.stage_mode() == StageMode::Pinned,
        })
    }

    /// How a slot is staged, or `None` if it does not exist.
    pub fn stage_mode(&self, local_id: u64) -> Option<StageMode> {
        self.slots.get(&local_id).map(|slot| slot.body.stage_mode())
    }

    /// Run `f` against a slot's pinned body, if it has one.
    ///
    /// Scoped rather than returning the body so the `DashMap` guard is released
    /// at a point the caller cannot get wrong: a descriptor built from a slot
    /// must not be held while the map shard is locked, and the read paths that
    /// copy out of pinned memory must not be reentered from inside one.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn with_pinned<R>(
        &self,
        local_id: u64,
        f: impl FnOnce(&super::pinned::PinnedSlot) -> R,
    ) -> Option<R> {
        let slot = self.slots.get(&local_id)?;
        match &slot.body {
            SlotBody::Pinned(pinned) => Some(f(pinned)),
            SlotBody::InMemory(_) => None,
        }
    }

    /// Acquire a read lock on a slot. Returns a lease ID, or None if the slot doesn't exist.
    pub fn acquire_read_lock(&self, local_id: u64) -> Option<u64> {
        let slot = self.slots.get(&local_id)?;
        slot.read_lock_count.fetch_add(1, Ordering::Relaxed);
        let lease_id = self.next_lease_id.fetch_add(1, Ordering::Relaxed);
        self.active_leases.insert(lease_id, local_id);
        Some(lease_id)
    }

    /// Consume a lease **only** if it holds the slot the caller named.
    ///
    /// Each lease can be consumed once, which is what prevents a double detach
    /// or release. The `expected_local_id` check is part of the same atomic
    /// step rather than something the caller does afterwards, and that is the
    /// point of the signature:
    ///
    /// A lease id is a small integer travelling on the wire beside a handle
    /// that names a slot. A request pairing a real lease with the *wrong*
    /// handle — a confused peer, a replayed frame, a guess — must change
    /// nothing. Consuming first and checking afterwards left the slot's read
    /// lock permanently elevated (the lock is only released on the matching
    /// arm) and, worse, silently discarded the lease's reaper deadline, so the
    /// backstop that exists precisely for leases nobody ends could no longer
    /// see it. One mismatched frame turned a recoverable leak into an immortal
    /// slot.
    ///
    /// So: nothing is consumed unless it matches, the deadline survives a
    /// mismatch, and the reaper still reclaims the lease when it expires.
    ///
    /// This is also the single point where a lease's deadline is discarded, so
    /// every path that ends a lease — detach, release, and the reaper's own
    /// forced release — drops the deadline with it.
    pub fn consume_lease(&self, lease_id: u64, expected_local_id: u64) -> LeaseOutcome {
        // `remove_if` decides and removes under one shard lock: a concurrent
        // detach for the same lease cannot slip between the comparison and the
        // removal, so exactly one caller can be told `Consumed`.
        if self
            .active_leases
            .remove_if(&lease_id, |_, held| *held == expected_local_id)
            .is_some()
        {
            self.lease_deadlines.remove(&lease_id);
            return LeaseOutcome::Consumed;
        }
        // Diagnosis only, and deliberately a second lookup: the lease was not
        // consumed either way, and whether it is absent or merely held by
        // another slot only changes the log line.
        match self
            .active_leases
            .get(&lease_id)
            .map(|entry| *entry.value())
        {
            Some(actual) => LeaseOutcome::Mismatch { actual },
            None => LeaseOutcome::Unknown,
        }
    }

    /// Which slot a lease holds a read lock on, without consuming it.
    ///
    /// For the one caller that must check a lease's identity but must *not* end
    /// it: `_rv_lease_renew`. Detach and release check identity as a side
    /// effect of consuming, which is not an option for a keepalive.
    pub(crate) fn lease_slot(&self, lease_id: u64) -> Option<u64> {
        self.active_leases
            .get(&lease_id)
            .map(|entry| *entry.value())
    }

    /// Give a lease a deadline, after which the reaper force-releases it.
    ///
    /// Only RDMA leases get one: see the module docs.
    // Without the RDMA path nothing ever sets a deadline, so this is unused
    // there. Kept unconditional rather than scattered with `#[cfg]`:
    // `_rv_lease_renew` is registered in *every* build, so the store has to be
    // able to answer it in every build, and a store whose shape depended on a
    // feature flag is the readability cost this refactor exists to remove.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    pub(crate) fn set_lease_deadline(&self, lease_id: u64, local_id: u64, timeout: Duration) {
        self.lease_deadlines.insert(
            lease_id,
            LeaseDeadline {
                local_id,
                expires_at: Instant::now() + timeout,
                timeout,
            },
        );
    }

    /// Push a lease's deadline out by the timeout it was granted under.
    ///
    /// Returns whether the lease still had a deadline to push. A renewal for a
    /// lease that has already been released or reaped is not an error — the
    /// keepalive is fire-and-forget and races the release it is renewing past
    /// by construction — so the caller logs at most a debug line.
    pub(crate) fn renew_lease(&self, lease_id: u64) -> bool {
        match self.lease_deadlines.get_mut(&lease_id) {
            Some(mut entry) => {
                entry.expires_at = Instant::now() + entry.timeout;
                true
            }
            None => false,
        }
    }

    /// Leases whose deadline has passed, as `(lease_id, local_id)`.
    ///
    /// Collected into a `Vec` and returned rather than acted on in place. The
    /// forced release that follows removes from `lease_deadlines`,
    /// `active_leases`, `transfers` and `slots`, and doing any of that while a
    /// `DashMap` iterator holds a shard lock deadlocks rather than fails.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    pub(crate) fn expired_leases(&self, now: Instant) -> Vec<(u64, u64)> {
        self.lease_deadlines
            .iter()
            .filter(|entry| entry.value().expires_at <= now)
            .map(|entry| (*entry.key(), entry.value().local_id))
            .collect()
    }

    /// Leases currently carrying a deadline.
    #[cfg(test)]
    pub(crate) fn deadline_count(&self) -> usize {
        self.lease_deadlines.len()
    }

    /// End a lease the consumer never ended itself.
    ///
    /// Full release semantics, not a detach: a consumer that vanished
    /// mid-transfer also never sent `_rv_release`, so its refcount contribution
    /// would leak with the read lock and the slot would be immortal. That
    /// compounding leak is what the reaper exists to prevent (D8).
    ///
    /// Returns the slot the lease held, or `None` if the lease had already been
    /// ended by a detach or release that raced the reaper.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    pub(crate) fn force_release_lease(&self, lease_id: u64, local_id: u64) -> bool {
        if self.consume_lease(lease_id, local_id) != LeaseOutcome::Consumed {
            return false;
        }
        self.release_read_lock(local_id);
        self.remove_transfers_by_lease(lease_id);
        if self.ref_decrement(local_id) {
            self.try_free(local_id);
        }
        true
    }

    /// Release a read lock on a slot. Returns true if the slot should be freed
    /// (refcount == 0 AND read_lock_count == 0).
    ///
    /// Uses checked arithmetic: returns `false` if the count is already zero
    /// instead of underflowing.
    pub fn release_read_lock(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            let result =
                slot.read_lock_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        if v > 0 { Some(v - 1) } else { None }
                    });
            match result {
                Ok(prev) => {
                    let read_locks = prev - 1;
                    let refcount = slot.refcount.load(Ordering::Relaxed);
                    read_locks == 0 && refcount == 0
                }
                Err(_) => {
                    tracing::warn!(
                        "release_read_lock: read_lock_count already 0 for slot {local_id}"
                    );
                    false
                }
            }
        } else {
            false
        }
    }

    /// Increment the reference count for a slot.
    pub fn ref_increment(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            slot.refcount.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Decrement the reference count. Returns true if the slot should be freed
    /// (refcount == 0 AND read_lock_count == 0).
    ///
    /// Uses checked arithmetic: returns `false` if the count is already zero
    /// instead of underflowing.
    pub fn ref_decrement(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            let result = slot
                .refcount
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    if v > 0 { Some(v - 1) } else { None }
                });
            match result {
                Ok(prev) => {
                    let refcount = prev - 1;
                    let read_locks = slot.read_lock_count.load(Ordering::Relaxed);
                    refcount == 0 && read_locks == 0
                }
                Err(_) => {
                    tracing::warn!("ref_decrement: refcount already 0 for slot {local_id}");
                    false
                }
            }
        } else {
            false
        }
    }

    /// Remove a slot from the registry and return a copy of its data.
    ///
    /// A copy, not the staging: a pinned slot's pages belong to the pool or to
    /// a caller's region, and handing them out under a `Bytes` would make the
    /// registration's lifetime depend on where that `Bytes` ended up. Removing
    /// the slot drops the staging, which is the whole point of removing it.
    pub fn remove(&self, local_id: u64) -> Option<Bytes> {
        self.slots
            .remove(&local_id)
            .and_then(|(_, slot)| slot.body.to_bytes())
    }

    /// Try to free a slot if both refcount and read_lock_count are zero.
    pub fn try_free(&self, local_id: u64) {
        // Use entry API to avoid TOCTOU: only remove if both counters are zero.
        self.slots.remove_if(&local_id, |_, slot| {
            slot.refcount.load(Ordering::Relaxed) == 0
                && slot.read_lock_count.load(Ordering::Relaxed) == 0
        });
    }

    /// Get the data bytes for a slot (for inline responses or local fast-path).
    ///
    /// Free for an in-memory slot (a refcount bump) and a copy for a pinned
    /// one, because the caller gets bytes it may hold indefinitely and pinned
    /// staging cannot promise to outlive them.
    pub fn get_data(&self, local_id: u64) -> Option<Bytes> {
        self.slots
            .get(&local_id)
            .and_then(|slot| slot.body.to_bytes())
    }

    /// Get the total length of data in a slot.
    pub fn get_total_len(&self, local_id: u64) -> Option<u64> {
        self.slots.get(&local_id).map(|slot| slot.total_len)
    }

    /// Create a new chunked transfer for a slot.
    /// Returns (transfer_id, chunk_size, chunk_count).
    pub fn create_transfer(
        &self,
        local_id: u64,
        lease_id: u64,
        max_chunk_size: u32,
    ) -> Option<(u64, u32, u32)> {
        let slot = self.slots.get(&local_id)?;
        let total_len = slot.total_len;
        let chunk_size = max_chunk_size.min(DEFAULT_CHUNK_SIZE);
        let chunk_count = total_len.div_ceil(chunk_size as u64) as u32;

        let transfer_id = self.next_transfer_id.fetch_add(1, Ordering::Relaxed);
        self.transfers.insert(
            transfer_id,
            TransferState {
                slot_local_id: local_id,
                lease_id,
                chunk_size,
                chunk_count,
                created_at: Instant::now(),
            },
        );

        Some((transfer_id, chunk_size, chunk_count))
    }

    /// Get a specific chunk from an active transfer.
    ///
    /// Serves both bodies. A pinned slot answers here exactly as an in-memory
    /// one does — see the module docs for why that is not optional.
    pub fn get_chunk(&self, transfer_id: u64, chunk_index: u32) -> Option<Bytes> {
        let transfer = self.transfers.get(&transfer_id)?;
        let slot = self.slots.get(&transfer.slot_local_id)?;

        let offset = chunk_index as u64 * transfer.chunk_size as u64;
        if offset >= slot.total_len {
            return None;
        }
        let end = (offset + transfer.chunk_size as u64).min(slot.total_len);

        slot.body.read_at(offset, (end - offset) as usize)
    }

    /// Remove a completed transfer.
    pub fn remove_transfer(&self, transfer_id: u64) {
        self.transfers.remove(&transfer_id);
    }

    /// Remove all transfers associated with a given lease ID.
    pub fn remove_transfers_by_lease(&self, lease_id: u64) {
        self.transfers.retain(|_, state| state.lease_id != lease_id);
    }
}

impl Default for DataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let store = DataStore::new();
        let data = Bytes::from(vec![1u8, 2, 3, 4]);
        let id = store.register(data.clone(), None);
        assert_eq!(id, 1);
        assert_eq!(store.get_data(id).unwrap(), data);
    }

    #[test]
    fn test_metadata() {
        let store = DataStore::new();
        let data = Bytes::from(vec![0u8; 1024]);
        let id = store.register(data, None);
        let meta = store.metadata(id).unwrap();
        assert_eq!(meta.total_len, 1024);
        assert_eq!(meta.refcount, 1);
        assert!(!meta.pinned);
        assert_eq!(store.stage_mode(id), Some(StageMode::InMemory));
    }

    #[test]
    fn test_ref_counting() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("hello"), None);

        // Initial refcount is 1
        assert_eq!(store.metadata(id).unwrap().refcount, 1);

        // Increment
        assert!(store.ref_increment(id));
        assert_eq!(store.metadata(id).unwrap().refcount, 2);

        // Decrement (not yet free: refcount=1)
        assert!(!store.ref_decrement(id));

        // Decrement (free: refcount=0, no read locks)
        assert!(store.ref_decrement(id));

        // try_free should remove it
        store.try_free(id);
        assert!(store.metadata(id).is_none());
    }

    #[test]
    fn test_read_lock_prevents_free() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("data"), None);

        // Acquire read lock
        let _lease = store.acquire_read_lock(id).unwrap();

        // Decrement refcount to 0
        let should_free = store.ref_decrement(id);
        // Should NOT free because read lock is held
        assert!(!should_free);

        // Release read lock — now should free
        let should_free = store.release_read_lock(id);
        assert!(should_free);
    }

    #[test]
    fn test_chunked_transfer() {
        let store = DataStore::new();
        let data = Bytes::from(vec![0xAA; 2000]);
        let id = store.register(data, None);
        let lease_id = store.acquire_read_lock(id).unwrap();

        let (transfer_id, chunk_size, chunk_count) =
            store.create_transfer(id, lease_id, 1024).unwrap();
        assert_eq!(chunk_size, 1024);
        assert_eq!(chunk_count, 2);

        // Get chunk 0
        let chunk0 = store.get_chunk(transfer_id, 0).unwrap();
        assert_eq!(chunk0.len(), 1024);
        assert!(chunk0.iter().all(|&b| b == 0xAA));

        // Get chunk 1 (partial)
        let chunk1 = store.get_chunk(transfer_id, 1).unwrap();
        assert_eq!(chunk1.len(), 976); // 2000 - 1024
        assert!(chunk1.iter().all(|&b| b == 0xAA));

        // Chunk 2 doesn't exist
        assert!(store.get_chunk(transfer_id, 2).is_none());

        // Cleanup
        store.remove_transfer(transfer_id);
        assert!(store.transfers.get(&transfer_id).is_none());
    }

    /// A lease with no deadline is invisible to the reaper, however long it is
    /// held. Chunked leases must keep exactly the behaviour they had.
    #[test]
    fn chunked_leases_never_expire() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("data"), None);
        let lease = store.acquire_read_lock(id).unwrap();

        assert_eq!(store.deadline_count(), 0);
        assert!(
            store
                .expired_leases(Instant::now() + Duration::from_secs(3600))
                .is_empty(),
            "a lease with no deadline must never be reported expired"
        );
        assert!(!store.renew_lease(lease));
    }

    #[test]
    fn a_deadline_expires_and_renewal_pushes_it_out() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("data"), None);
        let lease = store.acquire_read_lock(id).unwrap();
        store.set_lease_deadline(lease, id, Duration::from_secs(3600));
        assert!(
            store.expired_leases(Instant::now()).is_empty(),
            "a fresh deadline is not expired"
        );

        // Expire it by re-granting under a zero timeout, which is what a
        // deadline that has passed looks like from the reaper's side.
        store.set_lease_deadline(lease, id, Duration::from_millis(0));
        assert_eq!(store.expired_leases(Instant::now()), vec![(lease, id)]);

        // A renewal pushes the deadline out by the timeout the lease was
        // granted under, which is now zero — so re-grant under a real one and
        // check that renewal keeps it alive.
        store.set_lease_deadline(lease, id, Duration::from_secs(3600));
        assert!(store.renew_lease(lease));
        assert!(
            store.expired_leases(Instant::now()).is_empty(),
            "renewal must push the deadline past now"
        );
    }

    /// Ending a lease drops its deadline, so the reaper cannot resurrect a
    /// lease id that a detach or release already consumed.
    #[test]
    fn consuming_a_lease_drops_its_deadline() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("data"), None);
        let lease = store.acquire_read_lock(id).unwrap();
        store.set_lease_deadline(lease, id, Duration::from_secs(30));
        assert_eq!(store.deadline_count(), 1);

        assert_eq!(store.consume_lease(lease, id), LeaseOutcome::Consumed);
        assert_eq!(store.deadline_count(), 0);
        assert!(!store.renew_lease(lease));
    }

    /// A detach or release that names the right lease and the wrong slot must
    /// change nothing at all.
    ///
    /// The dangerous half is the deadline. Consuming first and checking the
    /// handle afterwards discarded it, which blinded the reaper to the one kind
    /// of lease it exists for; the read lock was then elevated forever and the
    /// slot became immortal. So the assertion is not only "the lease survived"
    /// but "the reaper can still reclaim it".
    #[test]
    fn a_mismatched_lease_is_not_consumed_and_stays_reapable() {
        let store = DataStore::new();
        let mine = store.register(Bytes::from(vec![0u8; 4096]), None);
        let theirs = store.register(Bytes::from(vec![1u8; 4096]), None);
        let lease = store.acquire_read_lock(mine).unwrap();
        store.set_lease_deadline(lease, mine, Duration::from_millis(0));

        assert_eq!(
            store.consume_lease(lease, theirs),
            LeaseOutcome::Mismatch { actual: mine },
            "a lease held by another slot must not be consumed"
        );
        assert_eq!(
            store.deadline_count(),
            1,
            "the mismatch discarded the deadline, blinding the reaper"
        );
        assert_eq!(
            store.lease_slot(lease),
            Some(mine),
            "the lease must survive"
        );

        // The backstop still works, which is the property the whole fix is for.
        assert_eq!(store.expired_leases(Instant::now()), vec![(lease, mine)]);
        assert!(store.force_release_lease(lease, mine));
        assert!(
            store.metadata(mine).is_none(),
            "the reaper must free the slot"
        );

        // And an unknown lease is distinguishable from a mismatched one.
        assert_eq!(store.consume_lease(lease, mine), LeaseOutcome::Unknown);
        assert_eq!(store.consume_lease(9_999, mine), LeaseOutcome::Unknown);
    }

    /// A forced release is a release, not a detach: the refcount the vanished
    /// consumer was holding goes with the read lock, so the slot can be freed.
    #[test]
    fn force_release_frees_a_transparent_style_slot() {
        let store = DataStore::new();
        let id = store.register(Bytes::from(vec![0u8; 4096]), None);
        let lease = store.acquire_read_lock(id).unwrap();
        let (transfer_id, _, _) = store.create_transfer(id, lease, 1024).unwrap();
        store.set_lease_deadline(lease, id, Duration::from_millis(0));

        assert!(store.force_release_lease(lease, id));
        assert!(store.metadata(id).is_none(), "the slot must be freed");
        assert!(store.get_chunk(transfer_id, 0).is_none());
        assert_eq!(store.deadline_count(), 0);
        assert!(
            !store.force_release_lease(lease, id),
            "a lease is force-released at most once"
        );
    }
}
