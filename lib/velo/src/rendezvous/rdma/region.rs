// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! External registration and its RAII guard (D5).
//!
//! A caller who already owns memory — a model weight arena, an inference KV
//! block, a buffer from another allocator — registers it with velo and gets a
//! [`RegionGuard`] back. The guard does not borrow: it is an independent value
//! the caller holds for as long as the registration must live.
//!
//! # The one contract that matters
//!
//! Registered pages must stay allocated until velo says otherwise. "Otherwise"
//! is exactly two events:
//!
//! * [`RegionGuard::deregistered`] resolves — the backend confirmed the unmap.
//! * Velo's own shutdown completes — transport teardown force-unmaps whatever
//!   is still registered, so everything is released by the time it returns.
//!
//! Neither an error from [`RegionGuard::unregister`] nor dropping the guard is
//! one of them. `Drop` cannot block (blocking in a `Drop` inside a tokio
//! runtime deadlocks the worker), so it starts the deregistration in the
//! background and returns immediately — the memory is still pinned when `Drop`
//! finishes. Dropping the guard early is therefore a liveness bug for the
//! caller, not a soundness bug for anyone else, and it is logged at `warn`.
//!
//! # Registered means remotely writable
//!
//! UCP's `prot` field is dead code: a region registered for RMA can be
//! *written* by any holder of its key, not just read, whatever the velo
//! protocol above it does. So a `&mut [u8]`-shaped API would be an aliasing
//! lie, and this one does not offer it. The assumption is a trust domain — key
//! material only ever reaches peers this instance already talks to — and it is
//! stated here rather than implied.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use velo_ext::ShutdownState;

use super::RegistryShared;
use super::arena::RemoteRef;
use super::backend::RdmaError;

/// Shared state behind one external registration.
///
/// Held by the [`RegionGuard`], by every [`RegionWatch`], by the registry, and
/// by a background deregistration task — so any of them may outlive the others
/// and still resolve the latch honestly.
pub(crate) struct RegionInner {
    /// Registry-local id; the key in the registry map.
    pub(super) id: u64,
    /// Distinguishes registrations of the same address over time.
    pub(super) generation: u64,
    /// Id for the matching backend unmap.
    pub(super) backend_region_id: u64,
    /// The pointer the caller asked to register.
    pub(super) ptr: usize,
    /// The length the caller asked to register.
    pub(super) len: usize,
    /// Packed key covering the region.
    pub(super) packed_key: Bytes,
    /// Start of the range the backend actually pinned. Reported, never used for
    /// offset arithmetic: the backend rounds outward to page boundaries, so it
    /// can name bytes the caller does not own.
    pub(super) effective_addr: u64,
    /// Length of the pinned range.
    pub(super) effective_len: u64,
    /// In-flight operations against this region. Phase 3 acquires a guard per
    /// RDMA lease; here the counter exists and `unregister` drains it.
    ///
    /// Reused verbatim from `velo_ext` rather than hand-rolled: the SeqCst plus
    /// register-notified-first discipline in there has been paid for twice
    /// already.
    pub(super) in_flight: ShutdownState,
    /// Latched once the backend confirms the unmap. Monotonic.
    deregistered: AtomicBool,
    dereg_notify: Notify,
    /// Serialises deregistration attempts so exactly one of them does the work
    /// and the rest observe the outcome.
    pub(super) dereg_lock: tokio::sync::Mutex<()>,
    /// The buffer velo took ownership of, for `register_owned`.
    pub(super) owned: parking_lot::Mutex<Option<Box<[u8]>>>,
    /// Cancelled when the registry begins shutting down.
    pub(super) shutdown: CancellationToken,
}

/// Everything a [`RegionInner`] is built from, so the constructor stays a
/// single argument instead of ten positional ones.
pub(super) struct RegionParts {
    pub id: u64,
    pub generation: u64,
    pub backend_region_id: u64,
    pub ptr: usize,
    pub len: usize,
    pub packed_key: Bytes,
    pub effective_addr: u64,
    pub effective_len: u64,
    /// `Some` only for `register_owned`, where velo holds the allocation.
    pub owned: Option<Box<[u8]>>,
    pub shutdown: CancellationToken,
}

impl RegionInner {
    pub(super) fn new(parts: RegionParts) -> Self {
        Self {
            id: parts.id,
            generation: parts.generation,
            backend_region_id: parts.backend_region_id,
            ptr: parts.ptr,
            len: parts.len,
            packed_key: parts.packed_key,
            effective_addr: parts.effective_addr,
            effective_len: parts.effective_len,
            in_flight: ShutdownState::new(),
            deregistered: AtomicBool::new(false),
            dereg_notify: Notify::new(),
            dereg_lock: tokio::sync::Mutex::new(()),
            owned: parking_lot::Mutex::new(parts.owned),
            shutdown: parts.shutdown,
        }
    }

    /// Whether the backend has confirmed the unmap.
    pub(super) fn is_deregistered(&self) -> bool {
        self.deregistered.load(Ordering::SeqCst)
    }

    /// Latch the region as deregistered and wake every waiter.
    ///
    /// Only ever called after a backend unmap answered `Ok`. An error — a
    /// timeout, a shutting-down backend — means *unknown*, and latching on
    /// unknown would tell a caller it may free memory that is still pinned.
    pub(super) fn latch_deregistered(&self) {
        self.deregistered.store(true, Ordering::SeqCst);
        self.dereg_notify.notify_waiters();
    }

    /// Resolve once the registration is gone for good.
    ///
    /// The waiter is created *before* the flag is read. `notify_waiters` stores
    /// no permit, so a future built after the latch would never hear it; tokio
    /// wakes futures that merely exist at notify time, so this ordering closes
    /// the window rather than narrowing it. Same discipline as
    /// `ShutdownState::wait_for_drain`.
    pub(super) async fn wait_deregistered(&self) {
        loop {
            let notified = self.dereg_notify.notified();
            if self.is_deregistered() {
                return;
            }
            notified.await;
        }
    }
}

/// Gate, drain, unmap, latch — the whole deregistration of one region.
///
/// Every path that ends a registration goes through here: an explicit
/// `unregister`, a dropped guard, and the registry shutdown sweep. They are
/// serialised by `dereg_lock` and made idempotent by the latch, so a guard
/// dropped while shutdown is sweeping does the work once and both callers see
/// the same outcome.
///
/// `budget` bounds the whole sequence, not each step. What it buys per step:
///
/// * **Drain.** Exceeding it is not fatal — the unmap still runs, because the
///   backend does its own local in-flight accounting and parks the unmap until
///   its own operations finish. What a drain timeout leaves exposed is a
///   *remote* straggler, which is D8's documented accepted risk.
/// * **Unmap.** Exceeding it loses only the notification: the backend contract
///   says a submitted unmap proceeds regardless. The latch stays unset, the
///   region stays in the registry, and the shutdown sweep will ask again and
///   get the idempotent `Ok`.
pub(super) async fn deregister(
    shared: &Arc<RegistryShared>,
    inner: &Arc<RegionInner>,
    budget: Duration,
) -> Result<(), RdmaError> {
    let deadline = Instant::now() + budget;
    let remaining = |deadline: Instant| deadline.saturating_duration_since(Instant::now());

    let Ok(_lock) = tokio::time::timeout(remaining(deadline), inner.dereg_lock.lock()).await else {
        return Err(RdmaError::Timeout);
    };
    if inner.is_deregistered() {
        return Ok(());
    }

    // Step 1 (gate): no new operation may join this region.
    inner.in_flight.begin_drain();

    // Step 2 (drain): bounded wait for the ones already counted.
    let drained = tokio::time::timeout(remaining(deadline), inner.in_flight.wait_for_drain())
        .await
        .is_ok();
    if !drained {
        let waiting = inner.in_flight.in_flight_count();
        let region = inner.id;
        tracing::warn!(
            region,
            waiting,
            "rdma: region drain timed out; unmapping anyway"
        );
    }

    // Step 3 (deregister): only a confirmed unmap may latch.
    let unmap = shared.backend.unmap(inner.backend_region_id);
    let outcome = match tokio::time::timeout(remaining(deadline), unmap).await {
        Ok(result) => result,
        Err(_) => Err(RdmaError::Timeout),
    };
    match outcome {
        Ok(()) => {
            shared.forget_region(inner);
            inner.latch_deregistered();
            if drained {
                Ok(())
            } else {
                Err(RdmaError::Timeout)
            }
        }
        Err(e) => Err(e),
    }
}

/// Keeps one externally-supplied memory region registered for RDMA.
///
/// Obtained from `Velo::register_external_memory` or `Velo::register_owned`.
/// The registration lasts until [`unregister`](Self::unregister) resolves, the
/// guard is dropped (which starts a background deregistration), or velo shuts
/// down — see the module docs for what each of those actually promises.
///
/// Not `Clone`: exactly one value is responsible for ending the registration.
/// For observation without responsibility, take a [`watch`](Self::watch).
pub struct RegionGuard {
    inner: Arc<RegionInner>,
    shared: Arc<RegistryShared>,
}

impl RegionGuard {
    pub(super) fn new(inner: Arc<RegionInner>, shared: Arc<RegistryShared>) -> Self {
        Self { inner, shared }
    }

    /// Address of the first registered byte, as the caller supplied it.
    pub fn addr(&self) -> u64 {
        self.inner.ptr as u64
    }

    /// Length the caller asked to register.
    pub fn len(&self) -> usize {
        self.inner.len
    }

    /// Whether the registered range is empty. Never true: a zero-length
    /// registration is refused.
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// Generation of this registration. Rides every descriptor cut from the
    /// region so a stale one is detectable rather than silently wrong.
    pub fn generation(&self) -> u64 {
        self.inner.generation
    }

    /// The range the backend actually pinned, which may extend past the
    /// caller's allocation in both directions: registration rounds outward to
    /// page boundaries. Reported for diagnostics; never an offset base.
    pub fn effective_range(&self) -> (u64, u64) {
        (self.inner.effective_addr, self.inner.effective_len)
    }

    /// Whether velo has begun shutting the registration layer down. Monotonic,
    /// so `true` is always authoritative.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Resolve when velo begins shutting the registration layer down, so a
    /// holder can start releasing before the forced sweep reaches it.
    pub async fn shutdown_initiated(&self) {
        self.inner.shutdown.cancelled().await;
    }

    /// Whether the backend has confirmed the unmap.
    pub fn is_deregistered(&self) -> bool {
        self.inner.is_deregistered()
    }

    /// Resolve once the memory is no longer registered — the point at which
    /// the caller may free it.
    ///
    /// Latched: it stays resolved forever after, so awaiting it late is fine.
    /// It resolves only on a *confirmed* unmap. If velo is torn down in a way
    /// that leaves the confirmation unobtainable, this future simply never
    /// resolves; the caller is then covered by the other clause of the
    /// contract — velo shutdown having completed — not by this one.
    pub async fn deregistered(&self) {
        self.inner.wait_deregistered().await;
    }

    /// An observational handle. Clonable, and holding one neither keeps the
    /// registration alive nor obliges the holder to end it.
    pub fn watch(&self) -> RegionWatch {
        RegionWatch {
            inner: Arc::clone(&self.inner),
        }
    }

    /// How a peer would address this region. Phase 3 builds the wire
    /// descriptor from it.
    pub(crate) fn remote(&self) -> RemoteRef {
        RemoteRef {
            addr: self.inner.ptr as u64,
            len: self.inner.len as u64,
            packed_key: self.inner.packed_key.clone(),
            generation: self.inner.generation,
        }
    }

    /// The region's in-flight accounting. Phase 3 acquires a guard from it per
    /// RDMA lease, which is what makes `unregister` wait for outstanding
    /// transfers rather than pulling the registration out from under them.
    pub(crate) fn in_flight(&self) -> &ShutdownState {
        &self.inner.in_flight
    }

    /// Deregister the memory, waiting up to `timeout` for the whole sequence.
    ///
    /// `Ok(())` is the only answer that means the memory is free to release.
    /// [`RdmaError::Timeout`] can mean either "in-flight operations outlasted
    /// the budget" or "the unmap was submitted but not confirmed in time"; the
    /// first still latches [`deregistered`](Self::deregistered), the second
    /// does not. Either way the caller may await `deregistered()` to find out.
    pub async fn unregister(self, timeout: Duration) -> Result<(), RdmaError> {
        deregister(&self.shared, &self.inner, timeout).await
    }

    /// Deregister and take back the buffer velo was holding.
    ///
    /// Only meaningful for a guard from `register_owned`. On any error the
    /// buffer stays with velo, because the pages may still be pinned — handing
    /// a `Box` back that the caller could drop is exactly the free-while-mapped
    /// hazard this layer exists to prevent.
    pub async fn unregister_owned(self, timeout: Duration) -> Result<Box<[u8]>, RdmaError> {
        deregister(&self.shared, &self.inner, timeout).await?;
        let taken = self.inner.owned.lock().take();
        taken.ok_or_else(|| RdmaError::Backend("region owns no buffer".into()))
    }
}

impl Drop for RegionGuard {
    /// Start a background deregistration if one has not already happened.
    ///
    /// Never blocks and never aborts. Blocking here would park a runtime worker
    /// inside a `Drop`, which deadlocks the moment the deregistration needs
    /// that worker to make progress; aborting would take the process down over
    /// a caller mistake. So the work is spawned and the memory stays pinned
    /// until it finishes — which is why this is a `warn` and not a shrug.
    ///
    /// The runtime handle is the one captured when the registry was built, not
    /// `Handle::try_current`: a guard dropped on a plain thread has no ambient
    /// runtime, and reading one from the environment would silently turn
    /// "deregister in the background" into "leak".
    fn drop(&mut self) {
        if self.inner.is_deregistered() {
            return;
        }
        let region = self.inner.id;
        let bytes = self.inner.len;
        tracing::warn!(
            region,
            bytes,
            "rdma: RegionGuard dropped before deregistration; deregistering in the \
             background. The memory stays pinned until that finishes, so it is not safe \
             to free yet — await `deregistered()` on a watch, or velo shutdown."
        );
        let shared = Arc::clone(&self.shared);
        let inner = Arc::clone(&self.inner);
        let budget = shared.cfg.drop_dereg_timeout;
        shared.runtime.clone().spawn(async move {
            if let Err(e) = deregister(&shared, &inner, budget).await {
                tracing::error!(
                    region,
                    error = %e,
                    "rdma: background deregistration did not confirm; the region stays \
                     registered until velo shuts down"
                );
            }
        });
    }
}

/// A clonable, observational view of a registration.
///
/// Holding one does not keep the registration alive and does not oblige the
/// holder to end it — that responsibility belongs to the single
/// [`RegionGuard`]. Useful for a task that must know when the memory has been
/// released without owning the release.
#[derive(Clone)]
pub struct RegionWatch {
    inner: Arc<RegionInner>,
}

impl RegionWatch {
    /// Whether velo has begun shutting the registration layer down.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutdown.is_cancelled()
    }

    /// Resolve when velo begins shutting the registration layer down.
    pub async fn shutdown_initiated(&self) {
        self.inner.shutdown.cancelled().await;
    }

    /// Whether the backend has confirmed the unmap.
    pub fn is_deregistered(&self) -> bool {
        self.inner.is_deregistered()
    }

    /// Resolve once the memory is no longer registered. Same latched semantics
    /// as the guard's.
    pub async fn deregistered(&self) {
        self.inner.wait_deregistered().await;
    }
}
