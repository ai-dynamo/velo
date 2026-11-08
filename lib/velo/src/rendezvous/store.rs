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
//! Bifurcating the two — a pinned slot that refuses non-RDMA readers — was an
//! earlier version's worst property, and this store never reintroduces it.
//!
//! # Which leases have a deadline
//!
//! An RDMA GET is issued by the *consumer's* NIC and the owner sees nothing at
//! all, so an RDMA lease carries a deadline and a reaper force-releases it
//! (D8). A chunked lease has one only when its consumer asks for one in its
//! acquire, and each pull renews it. The messenger's resolver asks when its
//! fetch has a bound: it gives up at that bound, sends nothing to say so, and
//! the lease would otherwise hold the slot until the owner tears down. A
//! consumer that does not ask, such as one of the public `get` calls, keeps
//! the no-deadline lease it always had, so a lease it holds after its last
//! pull is never reaped.
//!
//! # Time-to-live
//!
//! A slot registered with [`RegisterOptions::ttl`] is freed when its time is
//! up, with every lease on it ended, whether or not a consumer is still
//! reading. A staged message whose call has a timeout uses it: once the caller
//! has given up, the payload has no reader worth waiting for.
//!
//! Both are enforced by [`ensure_reaper`], which runs only while a deadline or
//! a time-to-live is pending.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
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
    /// Optional time-to-live. Enforced through [`DataStore::expiring`].
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
    /// Optional time-to-live for the staged data. When it is up, the slot is
    /// freed and every lease on it ended, whether or not a consumer is still
    /// reading. Enforced by the instance's reaper, which a registration
    /// through [`RendezvousManager`](crate::RendezvousManager) starts; a
    /// [`DataStore`] used on its own enforces it only through
    /// [`DataStore::sweep_expired`].
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
    /// Deadlines for the leases that carry one: RDMA leases, and chunked
    /// leases whose consumer asked for one. Entries are removed by the same
    /// [`consume_lease`](Self::consume_lease) that ends the lease, so a
    /// deadline never outlives the thing it bounds.
    lease_deadlines: DashMap<u64, LeaseDeadline>,
    /// When each slot registered with a time-to-live expires. Only those
    /// slots are here, so the reaper never walks the whole table.
    expiring: DashMap<u64, Instant>,
    /// Whether [`ensure_reaper`]'s sweep is running, and what stops it.
    reaper: Reaper,
    /// Observability, for the decisions taken inside the handlers this store
    /// backs. `None` when the instance was built without metrics.
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
            expiring: DashMap::new(),
            reaper: Reaper::default(),
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
        // Before the slot, so the sweep never sees a slot that should expire
        // and is not listed.
        if let Some(ttl) = ttl {
            self.expiring.insert(local_id, Instant::now() + ttl);
        }
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
    /// RDMA leases, and chunked leases whose consumer asked: see the module
    /// docs.
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
        let (_, slot) = self.slots.remove(&local_id)?;
        if slot.ttl.is_some() {
            self.expiring.remove(&local_id);
        }
        slot.body.to_bytes()
    }

    /// Try to free a slot if both refcount and read_lock_count are zero.
    pub fn try_free(&self, local_id: u64) {
        // Use entry API to avoid TOCTOU: only remove if both counters are zero.
        let freed = self.slots.remove_if(&local_id, |_, slot| {
            slot.refcount.load(Ordering::Relaxed) == 0
                && slot.read_lock_count.load(Ordering::Relaxed) == 0
        });
        if let Some((_, slot)) = freed
            && slot.ttl.is_some()
        {
            self.expiring.remove(&local_id);
        }
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

    /// Serve one chunk to a consumer, and renew its lease's deadline if the
    /// lease has one: a consumer that is still pulling is still reading.
    pub(crate) fn pull_chunk(&self, transfer_id: u64, chunk_index: u32) -> Option<Bytes> {
        let chunk = self.get_chunk(transfer_id, chunk_index)?;
        if let Some(lease_id) = self.transfers.get(&transfer_id).map(|t| t.lease_id) {
            self.renew_lease(lease_id);
        }
        Some(chunk)
    }

    /// Remove a completed transfer.
    pub fn remove_transfer(&self, transfer_id: u64) {
        self.transfers.remove(&transfer_id);
    }

    /// Move every slot out of registered memory and into the heap, releasing
    /// what the registration layer is waiting on.
    ///
    /// Called once, from `RendezvousManager::shutdown`, before the registration
    /// sweep. A pinned body holds a pool suballocation and, for an anchor
    /// inside a caller's region, the in-flight guard that region's
    /// deregistration waits on. Without releasing those, a single long-lived
    /// anchor burns the *entire* shutdown budget on a drain that can never
    /// finish, and every unmap behind it — and the messenger phase after that —
    /// is starved.
    ///
    /// # Demoted, not dropped
    ///
    /// The body is replaced with a heap copy of the same bytes rather than
    /// removed. The messenger has only just gated *new* inbound requests; a
    /// chunked pull admitted before that gate is still entitled to finish, and
    /// dropping the slot underneath it would turn a completion into "chunk not
    /// found" mid-transfer. Demotion keeps the slot, its transfer state and its
    /// data exactly where the puller expects them, and still hands the sweep
    /// back everything it was waiting for.
    ///
    /// A slot whose region has already gone is dropped, because there is
    /// nothing left to copy — an in-flight pull against it was going to fail
    /// either way.
    ///
    /// # It costs memory, once
    ///
    /// Every staged pinned byte is copied to the heap here. That is the price
    /// of not breaking an admitted pull, it is bounded by what was staged, and
    /// it is transient: the copies go when the slots do.
    ///
    /// Returns `(demoted, dropped)`.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn demote_pinned_slots(&self) -> (usize, usize) {
        // Collected first: the copy below happens under a *read* guard on the
        // slot, and taking that while iterating the same map would hold a shard
        // lock across every copy in it.
        let pinned: Vec<u64> = self
            .slots
            .iter()
            .filter(|entry| matches!(entry.value().body, SlotBody::Pinned(_)))
            .map(|entry| *entry.key())
            .collect();

        let (mut demoted, mut dropped) = (0usize, 0usize);
        for local_id in pinned {
            // Read under a shared guard, so a concurrent `get_chunk` on the
            // same shard is not blocked by the copy. Chunk-gated, so a large
            // anchor does not park a deregistration either.
            let copied = self.slots.get(&local_id).and_then(|slot| match &slot.body {
                SlotBody::Pinned(pinned) => Some(pinned.to_bytes()),
                SlotBody::InMemory(_) => None,
            });
            match copied {
                Some(Some(bytes)) => {
                    if let Some(mut slot) = self.slots.get_mut(&local_id)
                        && matches!(slot.body, SlotBody::Pinned(_))
                    {
                        // The pinned body drops here, which is what releases
                        // the pool suballocation and the region's in-flight
                        // guard.
                        slot.body = SlotBody::InMemory(bytes);
                        demoted += 1;
                    }
                }
                // The region behind it is already released, so there is nothing
                // to preserve and nothing an in-flight pull could have read.
                Some(None) => {
                    if self.slots.remove(&local_id).is_some() {
                        dropped += 1;
                    }
                }
                None => {}
            }
        }
        (demoted, dropped)
    }

    /// Remove all transfers associated with a given lease ID.
    pub fn remove_transfers_by_lease(&self, lease_id: u64) {
        self.transfers.retain(|_, state| state.lease_id != lease_id);
    }

    /// End what is past its time at `now`: leases past their deadline, and
    /// slots past their time-to-live.
    ///
    /// Returns `(leases, slots)`, the leases force-released and the slots
    /// freed by expiry. A slot that a lease's release frees counts as a lease.
    pub fn sweep_expired(&self, now: Instant) -> (usize, usize) {
        let mut leases = 0;
        for (lease_id, local_id) in self.expired_leases(now) {
            if self.force_release_lease(lease_id, local_id) {
                leases += 1;
            }
        }
        // Collected first, as `expired_leases` is: freeing a slot touches
        // the maps this iterator would be holding a shard of.
        let due: Vec<u64> = self
            .expiring
            .iter()
            .filter(|entry| *entry.value() <= now)
            .map(|entry| *entry.key())
            .collect();
        let mut slots = 0;
        for local_id in due {
            if self.expire_slot(local_id) {
                slots += 1;
            }
        }
        (leases, slots)
    }

    /// Free a slot whose time-to-live is up, and end every lease on it.
    ///
    /// The leases go with it, deadlines and transfers included, so that a
    /// lease granted to a consumer that has since given up holds nothing. A
    /// consumer still reading gets "chunk not found", and its later release
    /// finds no lease, which is a no-op.
    fn expire_slot(&self, local_id: u64) -> bool {
        self.expiring.remove(&local_id);
        if self.slots.remove(&local_id).is_none() {
            return false;
        }
        let mut ended = Vec::new();
        self.active_leases.retain(|lease_id, held| {
            if *held == local_id {
                ended.push(*lease_id);
                false
            } else {
                true
            }
        });
        for lease_id in ended {
            self.lease_deadlines.remove(&lease_id);
            self.remove_transfers_by_lease(lease_id);
        }
        true
    }

    /// Whether a lease deadline or a time-to-live is pending.
    fn has_pending_expiry(&self) -> bool {
        !self.lease_deadlines.is_empty() || !self.expiring.is_empty()
    }

    /// Stop the reaper, for good. Called when the instance shuts down.
    #[cfg_attr(not(all(target_os = "linux", feature = "ucx")), allow(dead_code))]
    pub(crate) fn stop_reaper(&self) {
        self.reaper.stop.cancel();
    }
}

/// How often the reaper sweeps while something is pending: how long past its
/// deadline a lease or a slot can live.
const SWEEP_PERIOD: Duration = Duration::from_millis(100);

/// Whether the sweep task is running, and what stops it.
#[derive(Default)]
struct Reaper {
    running: AtomicBool,
    stop: tokio_util::sync::CancellationToken,
}

/// Make sure a task sweeps `store` while a lease deadline or a time-to-live
/// is pending. Call it after setting either.
///
/// The task exits when nothing is pending, so an instance that stages nothing
/// with a deadline arms no timer at all. It holds the store weakly, so it
/// never keeps a dropped instance alive. Outside a Tokio runtime it does
/// nothing, and the deadline waits for the next call made inside one.
///
/// In a UCX build with RDMA configured, the RDMA reaper sweeps the same
/// deadlines; the two racing is harmless, because a lease is consumed once.
pub(crate) fn ensure_reaper(store: &Arc<DataStore>) {
    // Pairs with the fence in `sweep_while_pending`: either that sweep sees
    // the deadline just set, or this swap sees the sweep has stopped.
    std::sync::atomic::fence(Ordering::SeqCst);
    if store.reaper.running.swap(true, Ordering::AcqRel) {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        store.reaper.running.store(false, Ordering::Release);
        return;
    };
    let weak = Arc::downgrade(store);
    let stop = store.reaper.stop.clone();
    runtime.spawn(sweep_while_pending(weak, stop));
}

async fn sweep_while_pending(store: Weak<DataStore>, stop: tokio_util::sync::CancellationToken) {
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(SWEEP_PERIOD) => {}
        }
        let Some(store) = store.upgrade() else { return };
        let (leases, slots) = store.sweep_expired(Instant::now());
        if leases + slots != 0 {
            tracing::debug!(
                leases,
                slots,
                "rendezvous: ended staged payloads past their deadline"
            );
            if let Some(m) = store.metrics() {
                m.set_rendezvous_active_slots(store.slots.len());
            }
        }
        if store.has_pending_expiry() {
            continue;
        }
        store.reaper.running.store(false, Ordering::Release);
        // A deadline set between the check above and the store, by a caller
        // that saw the flag still up, would wait for nobody. Look once more,
        // and keep running if one did; if that caller already started a new
        // sweep, this one leaves it to that. The fence pairs with the one in
        // `ensure_reaper`: without both, this look and that caller's swap
        // can each miss the other's write.
        std::sync::atomic::fence(Ordering::SeqCst);
        if !store.has_pending_expiry() || store.reaper.running.swap(true, Ordering::AcqRel) {
            return;
        }
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

    /// A chunked lease with a deadline lives while its consumer pulls, and is
    /// reaped once the pulls stop. This is the lease of a fetch that gave up
    /// at its bound without a word: without the deadline, it held the slot
    /// until the owner tore down.
    #[test]
    fn a_chunked_lease_deadline_is_renewed_by_pulls_and_reaped_without_them() {
        let store = DataStore::new();
        let id = store.register(Bytes::from(vec![7u8; 4096]), None);
        let lease = store.acquire_read_lock(id).unwrap();
        let (transfer, _, _) = store.create_transfer(id, lease, 1024).unwrap();
        let granted = Instant::now();
        store.set_lease_deadline(lease, id, Duration::from_millis(100));

        // A pull at least 60 ms after the grant pushes the deadline past
        // granted + 160 ms, so a sweep at granted + 130 ms, which would reap
        // the lease as granted, finds nothing due.
        std::thread::sleep(Duration::from_millis(60));
        assert!(store.pull_chunk(transfer, 0).is_some());
        assert_eq!(
            store.sweep_expired(granted + Duration::from_millis(130)),
            (0, 0),
            "a pull renews the deadline"
        );

        let (leases, slots) = store.sweep_expired(Instant::now() + Duration::from_secs(1));
        assert_eq!((leases, slots), (1, 0));
        assert!(
            store.metadata(id).is_none(),
            "the reaped lease freed the slot"
        );
        assert!(
            store.get_chunk(transfer, 1).is_none(),
            "its transfer is gone"
        );
        assert!(!store.has_pending_expiry());
    }

    /// A lease without a deadline, as a public `get` takes, is never reaped,
    /// however long its holder keeps it after the last pull.
    #[test]
    fn a_lease_without_a_deadline_is_never_reaped() {
        let store = DataStore::new();
        let id = store.register(Bytes::from(vec![7u8; 4096]), None);
        let lease = store.acquire_read_lock(id).unwrap();
        let (transfer, _, _) = store.create_transfer(id, lease, 4096).unwrap();
        assert!(store.pull_chunk(transfer, 0).is_some());
        assert_eq!(
            store.sweep_expired(Instant::now() + Duration::from_secs(3600)),
            (0, 0)
        );
        assert_eq!(store.lease_slot(lease), Some(id));
    }

    /// A slot past its time-to-live is freed with every lease on it, deadlines
    /// and transfers included, whether or not anyone fetched it. The late
    /// release of a lease it ended is a no-op.
    #[test]
    fn a_slot_past_its_time_to_live_is_freed_with_every_lease_on_it() {
        let store = DataStore::new();
        let ttl = RegisterOptions::new().ttl(Duration::from_millis(100));
        let unread = store.register(Bytes::from(vec![1u8; 4096]), Some(ttl.clone()));
        let read = store.register(Bytes::from(vec![2u8; 4096]), Some(ttl));
        let kept = store.register(Bytes::from(vec![3u8; 4096]), None);
        let lease = store.acquire_read_lock(read).unwrap();
        let (transfer, _, _) = store.create_transfer(read, lease, 1024).unwrap();
        store.set_lease_deadline(lease, read, Duration::from_secs(3600));

        assert_eq!(store.sweep_expired(Instant::now()), (0, 0), "not yet due");
        let due = Instant::now() + Duration::from_millis(200);
        assert_eq!(store.sweep_expired(due), (0, 2));
        assert!(store.metadata(unread).is_none());
        assert!(store.metadata(read).is_none());
        assert!(store.metadata(kept).is_some(), "a slot without a ttl stays");
        assert_eq!(store.lease_slot(lease), None, "the lease on it ended");
        assert_eq!(store.deadline_count(), 0, "with its deadline");
        assert!(store.get_chunk(transfer, 0).is_none(), "and its transfer");
        assert_eq!(store.consume_lease(lease, read), LeaseOutcome::Unknown);
        assert!(!store.has_pending_expiry());
    }

    /// A slot freed by its release, before its time-to-live, leaves nothing
    /// for the reaper to look at, so the reaper can stop.
    #[test]
    fn a_slot_released_before_its_time_to_live_leaves_no_expiry_behind() {
        let store = DataStore::new();
        let ttl = RegisterOptions::new().ttl(Duration::from_secs(3600));
        let id = store.register(Bytes::from(vec![1u8; 4096]), Some(ttl));
        assert!(store.has_pending_expiry());
        let lease = store.acquire_read_lock(id).unwrap();
        assert_eq!(store.consume_lease(lease, id), LeaseOutcome::Consumed);
        store.release_read_lock(id);
        assert!(store.ref_decrement(id));
        store.try_free(id);
        assert!(store.metadata(id).is_none());
        assert!(!store.has_pending_expiry());

        let removed = store.register(
            Bytes::from(vec![1u8; 16]),
            Some(RegisterOptions::new().ttl(Duration::from_secs(3600))),
        );
        assert!(store.remove(removed).is_some());
        assert!(!store.has_pending_expiry());
    }

    /// Poll `done` until it holds, for at most five seconds.
    async fn eventually(done: impl Fn() -> bool) -> bool {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    }

    /// The reaper sweeps while something is pending and stops when nothing
    /// is, so an instance that stages nothing with a deadline arms no timer.
    /// A deadline set after it stopped starts it again.
    ///
    /// Real time, not a paused clock: expiry is measured on `std`'s clock,
    /// which a paused Tokio clock does not move.
    #[tokio::test]
    async fn the_reaper_runs_only_while_something_is_pending() {
        let store = Arc::new(DataStore::new());
        let ttl = || Some(RegisterOptions::new().ttl(Duration::from_millis(20)));
        let first = store.register(Bytes::from(vec![1u8; 16]), ttl());
        ensure_reaper(&store);
        assert!(store.reaper.running.load(Ordering::Acquire));
        assert!(
            eventually(|| store.metadata(first).is_none()).await,
            "reaped at its ttl"
        );
        assert!(
            eventually(|| !store.reaper.running.load(Ordering::Acquire)).await,
            "the reaper stopped with nothing pending"
        );

        let second = store.register(Bytes::from(vec![2u8; 16]), ttl());
        ensure_reaper(&store);
        assert!(
            eventually(|| store.metadata(second).is_none()).await,
            "a second reaper ran"
        );
        assert!(eventually(|| !store.reaper.running.load(Ordering::Acquire)).await);
    }

    /// A stopped reaper does not run again, which is what shutdown needs.
    #[tokio::test]
    async fn a_stopped_reaper_stays_stopped() {
        let store = Arc::new(DataStore::new());
        let id = store.register(
            Bytes::from(vec![1u8; 16]),
            Some(RegisterOptions::new().ttl(Duration::from_millis(1))),
        );
        store.stop_reaper();
        ensure_reaper(&store);
        tokio::time::sleep(SWEEP_PERIOD * 3).await;
        assert!(store.metadata(id).is_some(), "a stopped reaper reaped");
    }
}
