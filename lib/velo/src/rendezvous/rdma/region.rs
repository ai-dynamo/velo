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
//! Registered pages must stay allocated until velo says otherwise, and
//! "otherwise" is one event: [`RegionGuard::deregistered`] resolves.
//!
//! It resolves on a confirmed unmap, or at the end of velo's own shutdown —
//! transport teardown force-unmaps whatever is still registered, so a region
//! the backend never confirmed is nonetheless released by the time shutdown
//! returns, and the latch is closed for it there. One thing to await, whatever
//! happened underneath.
//!
//! The exception is an abnormal teardown: if the progress thread dies and the
//! backend still reports registrations, the latch refuses and the future never
//! resolves. The memory is then leaked deliberately, because velo cannot
//! establish it was released. See [`RegionGuard::deregistered`].
//!
//! Neither an error from [`RegionGuard::unregister`] nor dropping the guard
//! releases anything. `Drop` cannot block (blocking in a `Drop` inside a tokio
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
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
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
    /// Packed key covering the region. Read by `RegionGuard::remote`, which
    /// becomes the wire descriptor of an anchor staged inside it.
    pub(super) packed_key: Bytes,
    /// Start of the range the backend actually pinned. Reported, never used for
    /// offset arithmetic: the backend rounds outward to page boundaries, so it
    /// can name bytes the caller does not own.
    pub(super) effective_addr: u64,
    /// Length of the pinned range.
    pub(super) effective_len: u64,
    /// In-flight operations against this region: one guard per staged anchor,
    /// which `unregister` drains before it unmaps.
    ///
    /// Reused verbatim from `velo_ext` rather than hand-rolled: the SeqCst plus
    /// register-notified-first discipline in there has been paid for twice
    /// already.
    pub(super) in_flight: ShutdownState,
    /// Latched once the backend confirms the unmap. Monotonic.
    deregistered: AtomicBool,
    /// Serialises the latch against readers copying out of this region.
    ///
    /// # What it orders, and what it deliberately does not
    ///
    /// A rendezvous anchor staged inside this region serves the chunked path by
    /// copying bytes out through a raw pointer. It checks
    /// [`is_deregistered`](Self::is_deregistered) first, but a check followed by
    /// a copy is a check-then-act: without this gate nothing stops the latch
    /// closing between them, and the latch is precisely the moment the caller
    /// is told it may free the memory.
    ///
    /// So readers take this for read, check the flag, and copy while holding
    /// it; [`latch_deregistered`](Self::latch_deregistered) takes it for write.
    /// A copy in progress therefore delays the latch, and a copy that starts
    /// after the latch sees the flag. Both orderings are what the caller's
    /// free-after-`deregistered()` contract needs.
    ///
    /// It is deliberately **not** held across the backend unmap. Unmapping
    /// deregisters; it does not free. A copy overlapping the unmap reads memory
    /// that is still allocated — by the caller, who may not free until the
    /// latch resolves, or by `owned` here, which the reader's own `RegionWatch`
    /// keeps alive. Holding a lock across that `await` would also make every
    /// deregistration future non-`Send` for no gain.
    ///
    /// Hold times are bounded by one chunk copy, and that is enforced rather
    /// than assumed: readers take the gate per `DEFAULT_CHUNK_SIZE` and
    /// re-check the latch between chunks, so a request for a multi-gigabyte
    /// anchor is many short acquisitions rather than one long one. A
    /// deregistration waiting behind live readers therefore waits for a memcpy
    /// of at most that size — not for a whole anchor, and not for a transfer.
    copy_gate: parking_lot::RwLock<()>,
    dereg_notify: Notify,
    /// Serialises deregistration attempts so exactly one of them does the work
    /// and the rest observe the outcome.
    pub(super) dereg_lock: tokio::sync::Mutex<()>,
    /// The buffer velo took ownership of, for `register_owned`.
    pub(super) owned: parking_lot::Mutex<Option<Box<[u8]>>>,
    /// Bytes charged against the budget for this region.
    charged: u64,
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
    /// Bytes charged against the budget — the backend page-enclosing range.
    pub charged: u64,
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
            copy_gate: parking_lot::RwLock::new(()),
            dereg_notify: Notify::new(),
            dereg_lock: tokio::sync::Mutex::new(()),
            owned: parking_lot::Mutex::new(parts.owned),
            charged: parts.charged,
            shutdown: parts.shutdown,
        }
    }

    /// Whether the backend has confirmed the unmap.
    pub(super) fn is_deregistered(&self) -> bool {
        self.deregistered.load(Ordering::SeqCst)
    }

    /// Latch the region as deregistered and wake every waiter.
    ///
    /// Only ever called after a backend unmap answered `Ok`, or at the end of
    /// velo shutdown once the backend reports it holds nothing. An error — a
    /// timeout, a shutting-down backend — means *unknown*, and latching on
    /// unknown would tell a caller it may free memory that is still pinned.
    ///
    /// Taken under the [`copy_gate`](Self::copy_gate) write lock, which is what
    /// makes the flag safe to act on: no reader can be mid-copy when it flips,
    /// and no reader that starts afterwards can miss it. The guard covers a
    /// store and a notify and crosses no `await`.
    pub(super) fn latch_deregistered(&self) {
        let _closing = self.copy_gate.write();
        self.deregistered.store(true, Ordering::SeqCst);
        self.dereg_notify.notify_waiters();
    }

    /// Run `read` against this region's memory, or answer `None` if it is gone.
    ///
    /// The whole check-then-copy sequence, held together by the gate so it
    /// cannot be taken apart at the call site. `read` runs only when the region
    /// is still registered, and the latch cannot close underneath it.
    pub(super) fn with_live<R>(&self, read: impl FnOnce() -> R) -> Option<R> {
        let _open = self.copy_gate.read();
        if self.is_deregistered() {
            return None;
        }
        Some(read())
    }

    /// Bytes charged against the registered-bytes budget for this region.
    ///
    /// The backend page-encloses the requested range, so this is generally
    /// larger than `len`; releasing anything else would skew the budget.
    pub(super) fn charged(&self) -> u64 {
        self.charged
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

/// How a deregistration ended.
///
/// Both variants mean the same load-bearing thing: **the unmap was confirmed
/// and the memory is released**. They differ only in whether the in-flight
/// drain finished first. That distinction used to ride on `Err(Timeout)`, which
/// conflated "released, but we did not wait" with "not released" — and a caller
/// reading the error as the latter would hold memory forever, or, for an owned
/// buffer, never get it back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a DrainTimedOut deregistration released the memory without waiting for in-flight work"]
pub enum Deregistered {
    /// Every in-flight operation finished, and then the unmap was confirmed.
    Drained,
    /// The unmap was confirmed and the memory is released, but in-flight
    /// operations outlasted the budget and were not waited for. On RDMA
    /// hardware a straggling remote transfer now fails at its own end; over
    /// `UCX_TLS=tcp` it is silently lost. D8 documents this as the accepted
    /// cost of bounding shutdown.
    DrainTimedOut,
}

/// Buffers leaked by [`RegionInner::drop`] because the registration was never
/// confirmed gone.
///
/// The leak is the *correct* behaviour, and it is invisible: a test cannot
/// observe that a `Box` was not freed without reading freed memory, which is
/// the very thing being prevented. So the decision is recorded instead, which
/// makes "leaked rather than freed" an assertable fact rather than something
/// only Miri or ASan could catch.
#[cfg(test)]
pub(crate) static LEAKED_BUFFERS: AtomicUsize = AtomicUsize::new(0);

impl Drop for RegionInner {
    /// Leak the owned buffer unless the registration is confirmed gone.
    ///
    /// Plain drop glue here would be a use-after-free with extra steps. Drop a
    /// `Velo` without `graceful_shutdown` — a panic, a forgotten teardown, a
    /// test that just lets it fall out of scope — and the registry map drops,
    /// which drops these, which would hand the allocator back pages the NIC
    /// still has pinned and a peer still holds a key for. The peer then reads,
    /// or writes, freed heap.
    ///
    /// So the same discipline as `PageMemory`: leak unless the deregistration
    /// was confirmed. A leak costs memory; the alternative corrupts it. The
    /// gate also opens for free at the end of `Velo::graceful_shutdown`, where
    /// `latch_all_deregistered` marks every survivor released once transport
    /// teardown has force-unmapped everything — so an orderly shutdown frees
    /// these buffers normally and only a genuinely abandoned runtime leaks.
    fn drop(&mut self) {
        let Some(buffer) = self.owned.get_mut().take() else {
            return;
        };
        if self.is_deregistered() {
            // Confirmed released: the ordinary drop is correct.
            return;
        }
        let bytes = buffer.len();
        let region = self.id;
        // Deliberately never freed. `Box::leak` is the whole point.
        let _ = Box::leak(buffer);
        #[cfg(test)]
        LEAKED_BUFFERS.fetch_add(1, Ordering::SeqCst);
        tracing::error!(
            region,
            bytes,
            "rdma: an owned registration was dropped without a confirmed deregistration; \
             leaking its buffer rather than freeing memory the backend may still have \
             pinned. The registry was torn down without `RdmaRegistry::shutdown`."
        );
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
///
/// `Ok` therefore always means "confirmed released", and the variant says
/// whether the drain finished. `Err` always means "not confirmed" — the caller
/// must keep the memory alive.
pub(super) async fn deregister(
    shared: &Arc<RegistryShared>,
    inner: &Arc<RegionInner>,
    budget: Duration,
) -> Result<Deregistered, RdmaError> {
    let deadline = Instant::now() + budget;
    let remaining = |deadline: Instant| deadline.saturating_duration_since(Instant::now());

    let Ok(_lock) = tokio::time::timeout(remaining(deadline), inner.dereg_lock.lock()).await else {
        return Err(RdmaError::Timeout);
    };
    if inner.is_deregistered() {
        return Ok(Deregistered::Drained);
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
                Ok(Deregistered::Drained)
            } else {
                Ok(Deregistered::DrainTimedOut)
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
#[must_use = "the registration lasts as long as this guard; dropping it immediately starts a background deregistration"]
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
    ///
    /// `u64` to match [`addr`](Self::addr) and
    /// [`effective_range`](Self::effective_range): these three describe one
    /// range, and a caller doing arithmetic across them should not have to cast
    /// between widths in the middle of it.
    pub fn len(&self) -> u64 {
        self.inner.len as u64
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

    /// Resolve once the memory is no longer registered — the point at which the
    /// caller may free it.
    ///
    /// **This is the release contract.** It resolves when the unmap is
    /// confirmed, or when velo shutdown has fully completed, whichever comes
    /// first; after it resolves the memory is safe to free. In a process that
    /// shuts down normally there is nothing else to reason about.
    ///
    /// The shutdown half is what makes it dependable. A region whose unmap
    /// could not be confirmed — a wedged backend, a transport that went down
    /// first — is nonetheless genuinely released once
    /// `Velo::graceful_shutdown` returns, because transport teardown
    /// force-unmaps everything the progress thread still holds, and that is
    /// where the latch is closed for any survivor.
    ///
    /// # The third outcome
    ///
    /// If teardown itself fails abnormally — the progress thread panics, the
    /// join reports an error, and the backend still says it holds regions —
    /// the latch **refuses to close** and this future stays pending forever.
    /// That is deliberate, and it is the same leak-rather-than-free policy the
    /// rest of the layer follows: velo cannot establish that the pages were
    /// released, so it will not say they were. The memory is leaked for the
    /// life of the process, which is the survivable failure; the alternative is
    /// telling a caller to free memory a dead progress thread may still have
    /// pinned. A caller that must bound its wait should
    /// [`is_shutting_down`](Self::is_shutting_down) or time out and then leak
    /// deliberately, never free on a timeout.
    ///
    /// Latched, so awaiting it late is fine: it stays resolved forever after.
    ///
    /// Note what does *not* release the memory: dropping the
    /// [`RegionGuard`] merely starts a background deregistration, and an
    /// `unregister` that returns `Err` reached no conclusion at all.
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

    /// How a peer would address this region. The wire descriptor for an
    /// anchor staged inside it is cut from this.
    pub(crate) fn remote(&self) -> RemoteRef {
        RemoteRef {
            addr: self.inner.ptr as u64,
            len: self.inner.len as u64,
            packed_key: self.inner.packed_key.clone(),
            generation: self.inner.generation,
        }
    }

    /// The region's in-flight accounting.
    ///
    /// `register_data_in_region` takes a guard from it for every staged anchor,
    /// which is what makes `unregister` wait for those anchors rather than
    /// pulling the registration out from under them.
    pub(crate) fn in_flight(&self) -> &ShutdownState {
        &self.inner.in_flight
    }

    /// Deregister the memory, waiting up to `timeout` for the whole sequence.
    ///
    /// `Ok` means the unmap was confirmed and the memory is free to release;
    /// the [`Deregistered`] variant says whether in-flight work was waited for.
    /// `Err` means the deregistration reached no conclusion — the memory may
    /// still be pinned, and the caller must keep it alive until either a later
    /// attempt confirms it or velo shutdown completes.
    pub async fn unregister(self, timeout: Duration) -> Result<Deregistered, RdmaError> {
        deregister(&self.shared, &self.inner, timeout).await
    }

    /// Deregister and take back the buffer velo was holding.
    ///
    /// Only meaningful for a guard from `register_owned`. A guard over
    /// caller-owned memory answers [`RdmaError::NotOwned`] **without
    /// deregistering anything** — the check comes first, so the error never
    /// describes the opposite of what happened. (The guard is still consumed,
    /// so its ordinary `Drop` applies afterwards: a background deregistration
    /// starts, exactly as it would have on any other drop.)
    ///
    /// **A confirmed unmap always returns the buffer**, including when the
    /// in-flight drain timed out — the memory is released either way, and the
    /// [`Deregistered`] variant carries the distinction. Returning `Err` there
    /// would have destroyed the buffer with the guard while telling the caller
    /// velo had kept it.
    ///
    /// On `Err` the buffer stays with velo, because the pages may still be
    /// pinned: handing back a `Box` the caller could drop is exactly the
    /// free-while-mapped hazard this layer exists to prevent. It is not lost —
    /// velo frees it once shutdown confirms the release.
    pub async fn unregister_owned(
        self,
        timeout: Duration,
    ) -> Result<(Box<[u8]>, Deregistered), RdmaError> {
        // Before any side effect: a caller asking the wrong guard for a buffer
        // must not have its region deregistered and then be told the call did
        // nothing.
        if self.inner.owned.lock().is_none() {
            return Err(RdmaError::NotOwned);
        }
        let outcome = deregister(&self.shared, &self.inner, timeout).await?;
        let taken = self.inner.owned.lock().take();
        taken
            .map(|buffer| (buffer, outcome))
            .ok_or(RdmaError::NotOwned)
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

impl std::fmt::Debug for RegionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionGuard")
            .field("region", &self.inner.id)
            .field("addr", &self.inner.ptr)
            .field("len", &self.inner.len)
            .field("generation", &self.inner.generation)
            .field("deregistered", &self.inner.is_deregistered())
            .finish()
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

impl std::fmt::Debug for RegionWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionWatch")
            .field("region", &self.inner.id)
            .field("deregistered", &self.inner.is_deregistered())
            .finish()
    }
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

    /// Resolve once the memory is no longer registered — confirmed unmap or
    /// completed velo shutdown, whichever comes first. Same contract and same
    /// latched semantics as [`RegionGuard::deregistered`], including its third
    /// outcome: after an abnormal teardown this never resolves, and the memory
    /// is leaked deliberately rather than declared safe on no evidence.
    pub async fn deregistered(&self) {
        self.inner.wait_deregistered().await;
    }

    /// Run `read` against the region's memory while holding it open, or answer
    /// `None` once it has been released.
    ///
    /// The only sound way to read a range this watch describes: it takes the
    /// region's copy gate, checks the latch under it, and runs `read` before
    /// releasing — so a caller cannot accidentally split the check from the
    /// access it licenses. See
    /// [`RegionInner::copy_gate`](super::region::RegionInner::copy_gate) for
    /// what that ordering buys.
    ///
    /// `read` should be short: it delays any deregistration of this region for
    /// as long as it runs.
    pub(crate) fn with_live<R>(&self, read: impl FnOnce() -> R) -> Option<R> {
        self.inner.with_live(read)
    }

    /// Build a watch over a region directly.
    ///
    /// Production watches come from `RegionGuard::watch`, which needs a
    /// registry and a backend behind it; the ordering properties a watch
    /// carries are velo's own and are better tested without either.
    #[cfg(test)]
    pub(crate) fn for_test(inner: Arc<RegionInner>) -> Self {
        Self { inner }
    }

    /// Close the latch directly, for the adversarial wakeup scan.
    ///
    /// The scan needs to fire the latch from a plain thread at a precise moment
    /// relative to a waiter, which no production path offers — every real
    /// latch is the tail of an async deregistration.
    #[cfg(test)]
    pub(crate) fn latch_for_test(&self) {
        self.inner.latch_deregistered();
    }
}
