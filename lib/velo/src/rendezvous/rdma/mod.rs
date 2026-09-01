// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! RDMA memory registration for `velo::rendezvous`.
//!
//! Two ways in, one lifecycle out of both:
//!
//! * **Pool** ([`arena`]) — velo owns the pages, registers them in big arenas,
//!   and hands out suballocated [`PinnedBuf`]s. Phase 3 stages slot data here.
//! * **External** ([`region`]) — the caller owns the pages and holds a
//!   [`RegionGuard`] for as long as they stay registered.
//!
//! Everything is programmed against [`RdmaBackend`] (D12), so a later NIXL or
//! libfabric provider is an additional impl rather than a reshaping of this
//! layer. The whole module is `pub(crate)` except the guard types, and nothing
//! here touches `velo-ext` (D10).
//!
//! # Shutdown is an ordering, not a flag (D8)
//!
//! [`RdmaRegistry::shutdown`] implements steps 1–3, and `Velo::graceful_shutdown`
//! runs it *before* messenger teardown, because an RDMA GET is issued by the
//! peer's NIC and is therefore invisible to this instance's in-flight counters.
//! Unmapping after transport teardown would be unmapping under a transfer
//! nobody local can see.
//!
//! 1. **Gate** — new registrations and pool allocations are refused.
//! 2. **Drain** — registrations already in progress finish, then each region's
//!    own in-flight count is drained under a bounded budget.
//! 3. **Deregister** — every external region and every arena is unmapped, and
//!    each confirmed unmap resolves that region's `deregistered()` latch.
//!
//! The gate is not a `CancellationToken` check. A token read is a check-then-act
//! race with the map that follows it: a registration that passes the check and
//! lands after step 3 has enumerated leaves pinned memory with no tracking
//! entry and no latch. So admission acquires an in-flight guard *first* and
//! re-reads the gate with `SeqCst` afterwards, exactly as
//! `TransportAdapter::admit_message` does — the token is kept only as the
//! observable a `RegionGuard` holder awaits.

// The pool is fully exercised by this module's tests, but Phase 3 is its
// production consumer: nothing in the runtime stages a pinned slot yet. The
// allow is scoped to this module rather than sprinkled per item so it is one
// decision to revisit when `StageMode::Pinned` becomes real, and it is stated
// here rather than left as an unexplained attribute.
#[allow(dead_code)]
pub(crate) mod arena;
pub(crate) mod backend;
pub(crate) mod region;

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use velo_ext::ShutdownState;

use crate::observability::VeloMetrics;

pub use arena::RdmaPoolConfig;
pub use backend::RdmaError;
pub use region::{Deregistered, RegionGuard, RegionWatch};

pub(crate) use arena::{ArenaSet, Budget, PinnedBuf};
pub(crate) use backend::{BackendGet, RdmaBackend, UcxBackend};
use region::{RegionInner, RegionParts};

/// Tuning for the whole registration layer.
#[derive(Debug, Clone)]
pub struct RdmaConfig {
    /// Arena pool sizing and the registered-bytes budget.
    pub pool: RdmaPoolConfig,
    /// Bound on the shutdown sweep when the runtime shutdown policy names no
    /// deadline of its own. Exceeding it warns and force-unmaps (D8).
    pub shutdown_timeout: Duration,
    /// Bound on a background deregistration started by a dropped
    /// [`RegionGuard`].
    pub drop_dereg_timeout: Duration,
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            pool: RdmaPoolConfig::default(),
            shutdown_timeout: Duration::from_secs(30),
            drop_dereg_timeout: Duration::from_secs(30),
        }
    }
}

/// A [`register_owned`](RdmaRegistry::register_owned) that failed, with the
/// buffer handed back.
///
/// The buffer is the point. `BudgetExceeded` is a *routine* refusal — the
/// documented signal to stage chunked instead — and an error that swallowed the
/// allocation on the way would make the fallback path more expensive than the
/// fast one it falls back from.
///
/// `buffer` is `Option` because the same machinery serves the caller-owned
/// registration path, where there was never a buffer to return.
///
/// `#[non_exhaustive]`: velo constructs these, callers only read them. Leaving
/// it open would let a later field — which registration path refused, how much
/// budget was short — become a breaking change.
#[non_exhaustive]
pub struct RegisterOwnedError {
    /// The buffer velo did not take. `None` for a caller-owned registration.
    pub buffer: Option<Box<[u8]>>,
    /// Why the registration was refused.
    pub cause: RdmaError,
}

impl RegisterOwnedError {
    /// Split into the buffer and the reason.
    pub fn into_parts(self) -> (Option<Box<[u8]>>, RdmaError) {
        (self.buffer, self.cause)
    }
}

impl std::fmt::Debug for RegisterOwnedError {
    /// Prints the buffer length, never its contents: these are routinely
    /// gigabytes, and a `{:?}` in a log line should not try to render them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisterOwnedError")
            .field("buffer_len", &self.buffer.as_ref().map(|b| b.len()))
            .field("cause", &self.cause)
            .finish()
    }
}

impl std::fmt::Display for RegisterOwnedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.cause, f)
    }
}

impl std::error::Error for RegisterOwnedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

/// State shared by the registry, every [`RegionGuard`], and every background
/// deregistration task.
pub(crate) struct RegistryShared {
    /// The provider. One per instance in v1; the trait is what makes a second
    /// one additive.
    pub(crate) backend: Arc<dyn RdmaBackend>,
    pub(crate) cfg: RdmaConfig,
    /// Captured at construction so a `RegionGuard` dropped on a non-runtime
    /// thread still has somewhere to run its deregistration.
    pub(crate) runtime: tokio::runtime::Handle,
    /// Live external registrations, keyed by registry id.
    regions: DashMap<u64, Arc<RegionInner>>,
    /// Mapped bytes across the pool and external regions.
    budget: Arc<Budget>,
    metrics: Option<Arc<VeloMetrics>>,
    /// Admission gate for registrations. See the module docs for why this is a
    /// counter plus a `SeqCst` flag and not a token read.
    admission: ShutdownState,
    gate_closed: AtomicBool,
    /// Observable for `RegionGuard::shutdown_initiated`. Not the gate.
    shutdown_token: CancellationToken,
    next_region_id: AtomicU64,
    generations: Arc<AtomicU64>,
    /// How many times the shutdown sweep has run.
    ///
    /// It should run once. `Velo::graceful_shutdown` serialises its callers, so
    /// this is how a test observes that the serialisation is real rather than
    /// hoping to catch the narrow interleaving it prevents.
    sweeps: std::sync::atomic::AtomicUsize,
}

impl RegistryShared {
    /// Drop the registry tracking of a region and give its bytes back.
    ///
    /// Called exactly once per region, from the single deregistration that saw
    /// a confirmed unmap while holding that region's `dereg_lock`. Keying the
    /// budget release off the map removal keeps a racing guard-drop and
    /// shutdown sweep from double-crediting it.
    pub(crate) fn forget_region(&self, inner: &Arc<RegionInner>) {
        if self.regions.remove(&inner.id).is_some() {
            // `charged`, not `len`: the claim was taken on the page-enclosing
            // range the backend actually pinned, and releasing a smaller number
            // would skew the budget permanently upward.
            self.budget.release(inner.charged());
        }
    }
}

/// Owns the arena pool and every external registration for one velo instance.
///
/// Constructed by `VeloBuilder::build` once the transports have started, and
/// hung off [`Velo`](crate::Velo). Phase 3 reaches it through the runtime-
/// internal accessor to stage pinned slots and to issue GETs.
pub(crate) struct RdmaRegistry {
    shared: Arc<RegistryShared>,
    pool: ArenaSet,
}

impl RdmaRegistry {
    /// Build a registry over `backend`.
    ///
    /// `runtime` is stored, not sampled later: a `RegionGuard` may be dropped
    /// on any thread, and its background deregistration needs somewhere to run.
    pub(crate) fn new(
        backend: Arc<dyn RdmaBackend>,
        cfg: RdmaConfig,
        runtime: tokio::runtime::Handle,
        metrics: Option<Arc<VeloMetrics>>,
    ) -> Self {
        let budget = Arc::new(Budget::new(
            cfg.pool.registered_bytes_budget,
            metrics.clone(),
        ));
        let generations = Arc::new(AtomicU64::new(1));
        let pool = ArenaSet::new(
            Arc::clone(&backend),
            cfg.pool.clone(),
            Arc::clone(&budget),
            Arc::clone(&generations),
            metrics.clone(),
        );
        let shared = Arc::new(RegistryShared {
            backend,
            cfg,
            runtime,
            regions: DashMap::new(),
            budget,
            metrics,
            admission: ShutdownState::new(),
            gate_closed: AtomicBool::new(false),
            shutdown_token: CancellationToken::new(),
            next_region_id: AtomicU64::new(1),
            generations,
            sweeps: std::sync::atomic::AtomicUsize::new(0),
        });
        Self { shared, pool }
    }

    /// Take an admission ticket for one registration, or refuse.
    ///
    /// Acquire first, then re-read the gate with `SeqCst`. The two sides are a
    /// store-buffer litmus: this one does `fetch_add(in_flight)` then
    /// `load(gate)`, the shutdown side does `store(gate)` then
    /// `load(in_flight)`, and all four are `SeqCst`, so at least one must see
    /// the other. Checking the gate first and registering second would be a
    /// plain check-then-act race, and no ordering can fix that one: the whole
    /// of gate, drain and sweep can run inside the gap.
    fn admit(&self) -> Result<velo_ext::InFlightGuard, RdmaError> {
        let ticket = self.shared.admission.acquire();
        if self.shared.gate_closed.load(Ordering::SeqCst) {
            drop(ticket);
            return Err(RdmaError::ShuttingDown);
        }
        Ok(ticket)
    }

    /// Register memory velo does not own.
    ///
    /// # Safety
    ///
    /// For the entire lifetime of the registration — which ends only when
    /// [`RegionGuard::deregistered`] resolves, and **not** when the guard is
    /// dropped or an `unregister` returns `Err` — all of the following must
    /// hold.
    ///
    /// * `ptr` is valid for **both reads and writes** of `len` bytes. Read
    ///   validity is not enough: registering a range for RMA makes it remotely
    ///   writable by any holder of its key, because UCP carries no enforceable
    ///   protection field and the GET-only shape of the protocol above is a
    ///   convention, not an enforcement. Registering a read-only mapping is
    ///   undefined behaviour even though velo never writes to it itself.
    /// * `ptr + len` does not wrap the address space.
    /// * The allocation is not freed, moved, remapped, or reallocated —
    ///   `realloc` included, whether or not it happens to grow in place.
    /// * **No Rust reference into the range exists.** Not `&[u8]`, not
    ///   `&mut [u8]`, not a reference to anything stored inside it. A peer may
    ///   write at any moment, which contradicts the guarantees of a shared
    ///   reference and the exclusivity of a mutable one. Access the range
    ///   through raw pointers only.
    ///
    /// Registering is therefore a trust decision about the peers this instance
    /// talks to, not merely a performance one.
    ///
    /// Registration pins whole pages, so bytes adjacent to the allocation share
    /// its pinning and its remote writability;
    /// [`RegionGuard::effective_range`] reports what was actually pinned.
    pub(crate) async unsafe fn register_external(
        &self,
        ptr: NonNull<u8>,
        len: usize,
    ) -> Result<RegionGuard, RdmaError> {
        self.register(ptr.as_ptr() as usize, len, None)
            .await
            .map_err(|e| e.cause)
    }

    /// Register a buffer velo takes ownership of.
    ///
    /// Safe because velo, not the caller, decides when the allocation is
    /// dropped: it is held until a confirmed deregistration, and handed back by
    /// [`RegionGuard::unregister_owned`].
    ///
    /// The error carries the buffer back. [`RdmaError::BudgetExceeded`] is a
    /// documented routine refusal that a caller answers by staging chunked —
    /// destroying a possibly-gigabytes allocation on the way would make the
    /// fallback path cost more than the fast one.
    pub(crate) async fn register_owned(
        &self,
        buf: Box<[u8]>,
    ) -> Result<RegionGuard, RegisterOwnedError> {
        let ptr = buf.as_ptr() as usize;
        let len = buf.len();
        self.register(ptr, len, Some(buf)).await
    }

    /// The shared body of both registration paths.
    ///
    /// The admission ticket is held across the map, so the shutdown sweep
    /// cannot reach step 3 while a registration is still in flight. The budget
    /// claim is a [`Reservation`](arena::Reservation) rather than a manual
    /// release on the error arm, because this future may be *cancelled* at the
    /// map — a `timeout` around a registration is an ordinary thing for a
    /// caller to write — and no error arm runs then.
    ///
    /// The claim is taken on the page-enclosing range, not on `len`: the kernel
    /// pins whole pages, so charging the requested length would let the budget,
    /// whose whole job is to be the `RLIMIT_MEMLOCK` valve, undercount by
    /// nearly half on a small unaligned buffer. It is reconciled to the
    /// backend report once the map returns.
    async fn register(
        &self,
        ptr: usize,
        len: usize,
        owned: Option<Box<[u8]>>,
    ) -> Result<RegionGuard, RegisterOwnedError> {
        macro_rules! refuse {
            ($cause:expr) => {
                return Err(RegisterOwnedError {
                    buffer: owned,
                    cause: $cause,
                })
            };
        }

        if ptr == 0 || len == 0 {
            refuse!(RdmaError::OutOfRange);
        }
        let Some(enclosing) = arena::page_enclosing_len(ptr, len) else {
            refuse!(RdmaError::OutOfRange);
        };
        let ticket = match self.admit() {
            Ok(ticket) => ticket,
            Err(e) => refuse!(e),
        };
        let mut reservation = match self.shared.budget.try_reserve(enclosing) {
            Ok(reservation) => reservation,
            Err(e) => refuse!(e),
        };
        let mapped = match self.shared.backend.map(ptr, len).await {
            Ok(mapped) => mapped,
            Err(e) => refuse!(e),
        };
        // What the backend says it pinned wins over the local estimate.
        reservation.raise_to(mapped.effective_len.max(enclosing));

        let inner = Arc::new(RegionInner::new(RegionParts {
            id: self.shared.next_region_id.fetch_add(1, Ordering::Relaxed),
            generation: self.shared.generations.fetch_add(1, Ordering::Relaxed),
            backend_region_id: mapped.backend_region_id,
            ptr,
            len,
            packed_key: mapped.packed_key,
            effective_addr: mapped.effective_addr,
            effective_len: mapped.effective_len,
            owned,
            charged: reservation.bytes(),
            shutdown: self.shared.shutdown_token.clone(),
        }));
        self.shared.regions.insert(inner.id, Arc::clone(&inner));
        // Tracked now, so `forget_region` owns the matching release, and it
        // releases `inner.charged` — the same number claimed above.
        reservation.commit();
        drop(ticket);
        if let Some(m) = &self.shared.metrics {
            m.record_rdma_registration(crate::observability::RdmaRegistrationKind::External);
        }
        Ok(RegionGuard::new(inner, Arc::clone(&self.shared)))
    }

    // Phase 3 stages pinned slots and issues GETs through these; the tests
    // below are their only caller today.
    #[allow(dead_code)]
    /// Cut `len` registered bytes out of the pool.
    ///
    /// Goes through the same admission gate as an external registration, so a
    /// staging that starts during shutdown is refused rather than mapping an
    /// arena the sweep has already walked past. It is the *only* gated way into
    /// the pool — see [`pool`](Self::pool) for why there is no other.
    pub(crate) async fn alloc_pinned(&self, len: usize) -> Result<PinnedBuf, RdmaError> {
        let _ticket = self.admit()?;
        self.pool.alloc(len).await
    }

    /// Read remote memory into a locally registered destination. Phase 3 drives
    /// this from an owner-authored descriptor.
    #[allow(dead_code)]
    pub(crate) async fn get(&self, req: BackendGet) -> Result<(), RdmaError> {
        self.shared.backend.get(req).await
    }

    /// D8 steps 1 to 3: gate, drain, deregister.
    ///
    /// Idempotent, and must run *before* messenger and transport teardown —
    /// see the module docs for why unmapping after teardown is unsound rather
    /// than merely untidy.
    ///
    /// `budget` bounds the whole sweep. Overrunning it warns and force-unmaps:
    /// a straggling remote GET then fails at its own end on RDMA hardware, and
    /// is silently lost over `UCX_TLS=tcp`. That is the documented accepted
    /// risk, and the alternative — waiting forever on a peer that may have
    /// crashed — is worse.
    pub(crate) async fn shutdown(&self, budget: Duration) {
        self.shared.sweeps.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + budget;
        let remaining = |deadline: Instant| deadline.saturating_duration_since(Instant::now());

        // Step 1 (gate). The `SeqCst` store is the half of the admission
        // litmus that `admit` re-reads; `begin_drain` and the token are the
        // reporting halves.
        self.shared.gate_closed.store(true, Ordering::SeqCst);
        self.shared.admission.begin_drain();
        self.shared.shutdown_token.cancel();

        // Step 2 (drain). Registrations already past the gate must land before
        // the sweep enumerates, or one could insert itself behind the snapshot
        // and be missed entirely.
        let admitted = self.shared.admission.wait_for_drain();
        let landed = tokio::time::timeout(remaining(deadline), admitted).await;
        if landed.is_err() {
            tracing::warn!(
                "rdma: registrations in progress outlasted the shutdown budget; one may \
                 register memory this sweep will not unmap"
            );
        }

        // Step 3 (deregister). External regions first, then the arenas, each
        // through the same `deregister` the guards use — so a guard dropped
        // mid-sweep and this loop cannot both do the work, and whichever wins
        // resolves the latch.
        let regions: Vec<Arc<RegionInner>> = self
            .shared
            .regions
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect();
        for inner in regions {
            let region = inner.id;
            match region::deregister(&self.shared, &inner, remaining(deadline)).await {
                Ok(region::Deregistered::Drained) => {}
                Ok(region::Deregistered::DrainTimedOut) => {
                    tracing::warn!(
                        region,
                        "rdma: region unmapped without waiting out its in-flight work; the \
                         memory is released and the latch resolved, but a straggling remote \
                         transfer may now fail at its own end"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        region,
                        %e,
                        "rdma: region unmap unconfirmed; the deregistered() latch stays \
                         unresolved until the end of velo shutdown"
                    );
                }
            }
        }

        let leaked = self.pool.unmap_all(deadline).await;
        if leaked != 0 {
            tracing::warn!(
                arenas = leaked,
                "rdma: arenas left pinned at shutdown; their pages are freed once velo \
                 shutdown completes"
            );
        }
    }

    /// Declare every surviving registration released, at the end of velo
    /// shutdown.
    ///
    /// This is what makes [`RegionGuard::deregistered`] a signal a caller can
    /// depend on. `shutdown` above resolves the latch only for regions whose
    /// unmap the backend confirmed; anything else — a wedged unmap, a transport
    /// that went down first — would otherwise leave the future pending forever,
    /// and a caller holding memory until it resolved would hold it for the life
    /// of the process.
    ///
    /// # The latch is a claim, so it is checked
    ///
    /// Closing it says two things at once: callers may free their memory, and
    /// `RegionInner::drop` may free the buffers velo owns. Both are false if
    /// anything is still pinned, so this does not simply trust its call site.
    /// It asks the backend how many registrations it still holds and refuses to
    /// latch anything unless the answer is none.
    ///
    /// The call site is nonetheless what *makes* the answer none:
    /// `Velo::graceful_shutdown` runs this after transport teardown, and
    /// teardown force-unmaps every region the progress thread holds and joins
    /// that thread before returning. The check is there so a future caller
    /// cannot quietly move this earlier — under a `Timeout` policy the
    /// messenger phase can be handed a budget of zero and return without
    /// finishing — and turn a latch into a use-after-free.
    ///
    /// A backend that cannot report its count (`None`) gives up the check; the
    /// ordering is then the only guarantee, which is what it was before.
    ///
    /// # A refusal covers the pool too
    ///
    /// When the check refuses, the arenas whose unmap the sweep could not
    /// confirm stay leaked as well — `release_unconfirmed` is not called. That
    /// coupling is deliberate and it is the same rule in both directions: those
    /// pages are freed only on evidence that nothing is pinned, and a backend
    /// still reporting registrations is the evidence being absent. Freeing
    /// arena pages while the progress thread may still be alive is precisely
    /// the hazard the leak-by-default discipline exists for.
    pub(crate) fn latch_all_deregistered(&self) {
        match self.shared.backend.live_registrations() {
            Some(0) | None => {}
            Some(live) => {
                tracing::error!(
                    live,
                    "rdma: refusing to declare registrations released while the backend still \
                     holds {live} of them; their memory stays pinned and their deregistered() \
                     latches stay unresolved. Shutdown ran out of order."
                );
                return;
            }
        }

        let regions: Vec<Arc<RegionInner>> = self
            .shared
            .regions
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect();
        for inner in regions {
            self.shared.forget_region(&inner);
            inner.latch_deregistered();
        }
        self.pool.release_unconfirmed();
    }

    /// Bytes currently registered with the backend, pool and external together.
    ///
    /// Counts what the backend reports it pinned — the page-enclosing ranges —
    /// not the lengths callers asked for, because that is what the kernel and
    /// `RLIMIT_MEMLOCK` are holding.
    pub(crate) fn registered_bytes(&self) -> u64 {
        self.shared.budget.registered()
    }

    /// Live external registrations.
    #[allow(dead_code)]
    pub(crate) fn region_count(&self) -> usize {
        self.shared.regions.len()
    }

    /// The arena pool.
    ///
    /// `cfg(test)` deliberately. Handing this out crate-wide would be an
    /// admission escape hatch: `pool().alloc()` skips [`admit`](Self::admit),
    /// so a Phase-3 caller racing shutdown could map an arena after the sweep
    /// had already walked the set — pinned memory with nothing left to unmap
    /// it. [`alloc_pinned`](Self::alloc_pinned) is the only gated entry, and
    /// with this restricted it is also the only entry.
    #[cfg(test)]
    pub(crate) fn pool(&self) -> &ArenaSet {
        &self.pool
    }

    /// How many times the shutdown sweep has run.
    #[cfg(test)]
    pub(crate) fn sweep_count(&self) -> usize {
        self.shared.sweeps.load(Ordering::Relaxed)
    }

    /// The sweep budget to use when the runtime shutdown policy names none.
    pub(crate) fn shutdown_timeout(&self) -> Duration {
        self.shared.cfg.shutdown_timeout
    }

    /// Wire discriminator of the backend behind this registry. Phase 3 puts it
    /// in the descriptor and matches it against the consumer offer.
    #[allow(dead_code)]
    pub(crate) fn backend_key(&self) -> &str {
        self.shared.backend.key()
    }
}

// The tests live beside their siblings as `rdma/tests.rs`.
#[cfg(test)]
#[path = "tests.rs"]
mod tests;
