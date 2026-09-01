// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The arena pool (D4): few big registrations, many cheap suballocations.
//!
//! Velo runs UCX with `UCX_RCACHE_ENABLE=n` and `UCX_MEM_EVENTS=n` — no malloc
//! hooks in the process, deliberately. Under that configuration every
//! `ucp_mem_map` is a fresh `ibv_reg_mr` whose cost is linear in the size
//! registered, and UCX caches nothing on the way. Pre-registered arenas are
//! therefore load-bearing rather than an optimisation: without them every
//! staged buffer would pay a pin.
//!
//! So the pool registers a handful of large [`Arena`]s and suballocates inside
//! them in 4 KiB [`GRANULE`]s using `offset-allocator` (MIT, zero `unsafe`,
//! O(1) alloc and free). That crate allocates *offsets*, never memory: the
//! pages come from `PageMemory` here, so a bug in the suballocator is a wrong
//! offset, never a bad pointer.
//!
//! # A remote descriptor is not a free token
//!
//! The two are deliberately different values and must never be conflated. The
//! free token is the private `offset_allocator::Allocation` held inside
//! [`PinnedBuf`]; returning it is what makes the space reusable. The wire-side
//! [`RemoteRef`] — address, length, packed key, generation — is a *description*
//! of where some bytes currently live. A peer holding one can read the range;
//! it cannot free it, and holding one does not keep it alive.
//!
//! # Pages outlive their registration, always
//!
//! `PageMemory` defaults to leaking rather than freeing: the flag that lets it
//! call `dealloc` is set only once the backend has *confirmed* the unmap.
//! Freeing pages UCX still has pinned is the hazard this whole module exists to
//! contain, and a leak is the strictly safer failure. A registry torn down
//! without [`shutdown`](super::RdmaRegistry::shutdown) therefore leaks its
//! arenas, and says so at `error` level.
//!
//! # Registered means remotely writable
//!
//! Registering a range for RMA makes it remotely *writable* by any holder of
//! its key, not merely readable: UCP carries no enforceable protection field,
//! so the GET-only shape of the protocol above is a convention rather than an
//! enforcement. Every byte in every arena is therefore exposed to peers this
//! instance has keys out to, and the safety of a [`PinnedBuf`] rests on that
//! trust domain — not on the borrow checker, which cannot see the NIC.
//!
//! A consequence for Phase 3: a GET destination should be expressed as
//! `&mut PinnedBuf`, so the exclusion against *local* readers is carried by the
//! borrow checker for the duration of the transfer, instead of by a convention
//! about who holds the buffer. Handing a shared reference to a range the NIC is
//! actively filling would be a race Rust would otherwise let through silently.
//!
//! # Reclamation is Phase 4
//!
//! Arenas are append-only and live until registry shutdown. Empty-arena
//! reclamation under a low-water mark — the only kind that is safe, since
//! nothing references an empty arena — is deliberately not in this phase.

use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use offset_allocator::{Allocation, Allocator};
use parking_lot::{Mutex, RwLock};

use super::backend::{RdmaBackend, RdmaError};

/// Suballocation unit. Matches the page size UCX pins at, so a granule never
/// straddles a page and the float-bin round-up stays bounded at ~12.5%.
pub(crate) const GRANULE: usize = 4096;

/// Tuning for the arena pool (D11's new knobs).
#[derive(Debug, Clone)]
pub struct RdmaPoolConfig {
    /// Size of the first pooled arena. Later arenas grow geometrically from
    /// here up to [`max_arena_bytes`](Self::max_arena_bytes).
    ///
    /// Each arena carries suballocator metadata sized to its granule count —
    /// roughly 28 bytes per 4 KiB granule, about 0.7% of the arena, so ~7 MiB
    /// for a 1 GiB arena. That is deliberate: sizing the node pool below the
    /// granule count trades a visible fixed cost for an invisible one, where a
    /// fragmented arena reports "full" while it still has room.
    pub initial_arena_bytes: u64,
    /// Ceiling on a single pooled arena.
    pub max_arena_bytes: u64,
    /// Requests at or above this size get an arena of their own, sized to the
    /// request. Without it the float-bin round-up would cost up to 12.5% — 128
    /// MiB on a 1 GiB object.
    ///
    /// A dedicated arena is never offered to the general search, and this phase
    /// reclaims no arenas, so *every* oversize request maps a new one and holds
    /// it until shutdown — dropping the `PinnedBuf` does not give the arena
    /// back. A workload that repeatedly stages at or above this size therefore
    /// walks into [`registered_bytes_budget`](Self::registered_bytes_budget)
    /// within a single session (16 allocations at the defaults) and falls back
    /// to chunked from then on. Empty-arena reclamation is Phase 4; until then,
    /// raise this threshold above the sizes a hot path actually stages.
    pub dedicated_arena_min: u64,
    /// Ceiling on *mapped* bytes across the pool and external regions together.
    ///
    /// Note the axis: this counts what is registered with the backend, not what
    /// is suballocated, because that is what the NIC and `RLIMIT_MEMLOCK` see.
    /// A mostly-empty arena still costs its full size.
    ///
    /// Over budget, [`ArenaSet::alloc`] answers [`RdmaError::BudgetExceeded`]
    /// and Phase 3's callers stage chunked instead — pool exhaustion is never a
    /// hard failure of the staging operation (D4).
    pub registered_bytes_budget: u64,
}

impl Default for RdmaPoolConfig {
    fn default() -> Self {
        Self {
            initial_arena_bytes: 64 << 20,
            max_arena_bytes: 1 << 30,
            dedicated_arena_min: 64 << 20,
            registered_bytes_budget: 1 << 30,
        }
    }
}

/// Where some registered bytes currently live, as a peer would address them.
///
/// Deliberately **not** `Serialize`: the wire descriptor is D7's business and
/// ships in Phase 3. Deriving an encoding here would pre-commit a wire shape
/// before the version / flags / backend framing exists.
#[derive(Debug, Clone)]
pub(crate) struct RemoteRef {
    /// Absolute address in this process's address space.
    pub addr: u64,
    /// Length of the described range.
    pub len: u64,
    /// The backend's packed key covering `addr`.
    pub packed_key: Bytes,
    /// Generation of the registration behind `addr`, so a descriptor that
    /// outlived its registration is detectable rather than silently wrong.
    pub generation: u64,
}

// ---------------------------------------------------------------------------
// Backing pages
// ---------------------------------------------------------------------------

/// A page-aligned allocation that will not be freed while it might be pinned.
///
/// `freeable` starts `false` and is set exactly once, after the backend
/// confirms the unmap. Every other exit — a registry dropped without
/// `shutdown`, an unmap that answered `ShuttingDown`, a panic — leaks the
/// pages, which is the safe direction: a leak costs memory, a premature free
/// hands the NIC a dangling range with a peer's key still outstanding.
struct PageMemory {
    ptr: *mut u8,
    len: usize,
    freeable: AtomicBool,
}

// SAFETY: `ptr` is a unique heap allocation owned by this value for its whole
// lifetime, with no thread affinity. Access to its contents is handed out one
// level up, where the suballocator guarantees the ranges are non-overlapping
// (see `PinnedBuf`). So moving a `PageMemory` between threads, and sharing
// `&PageMemory` across them, are both sound.
unsafe impl Send for PageMemory {}
// SAFETY: as above.
unsafe impl Sync for PageMemory {}

impl PageMemory {
    /// Allocate `len` bytes (rounded up to whole [`GRANULE`]s), page-aligned
    /// and zeroed.
    fn new(len: usize) -> Result<Self, RdmaError> {
        let len = len
            .checked_next_multiple_of(GRANULE)
            .filter(|n| *n > 0)
            .ok_or(RdmaError::OutOfRange)?;
        let layout = Layout::from_size_align(len, GRANULE).map_err(|_| RdmaError::OutOfRange)?;
        // SAFETY: `layout` has non-zero size (rounded up to at least one
        // granule above) and a power-of-two alignment, so it is a valid
        // argument to the global allocator.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(RdmaError::Backend(format!(
                "allocating a {len} B rdma arena failed"
            )));
        }
        Ok(Self {
            ptr,
            len,
            freeable: AtomicBool::new(false),
        })
    }

    fn addr(&self) -> usize {
        self.ptr as usize
    }

    /// Record that the pages are not (or no longer) pinned, so `Drop` may free.
    ///
    /// Two callers, both meaning the same thing: the backend confirmed an
    /// unmap, or the map has not been issued yet and a cancellation between
    /// here and its completion would leave nothing pinned.
    fn mark_unmapped(&self) {
        self.freeable.store(true, Ordering::Release);
    }

    /// Record that the backend now holds these pages, so `Drop` must leak them.
    fn mark_pinned(&self) {
        self.freeable.store(false, Ordering::Release);
    }
}

impl Drop for PageMemory {
    fn drop(&mut self) {
        if !self.freeable.load(Ordering::Acquire) {
            tracing::error!(
                bytes = self.len,
                "rdma arena dropped without a confirmed unmap; leaking its pages rather than \
                 freeing memory the backend may still have pinned. The registry was torn down \
                 without `RdmaRegistry::shutdown`."
            );
            return;
        }
        let layout = Layout::from_size_align(self.len, GRANULE).expect("layout was valid to alloc");
        // SAFETY: same pointer and layout the allocation was made with, freed
        // exactly once (this is `Drop`), and only after the backend confirmed
        // the range is no longer pinned.
        unsafe { std::alloc::dealloc(self.ptr, layout) };
    }
}

// ---------------------------------------------------------------------------
// Arena
// ---------------------------------------------------------------------------

/// One backend registration plus the suballocator over it.
pub(crate) struct Arena {
    memory: PageMemory,
    /// Id for the matching [`RdmaBackend::unmap`].
    backend_region_id: u64,
    /// Packed key covering the whole arena; every [`RemoteRef`] cut from it
    /// carries a clone (a refcount bump, not a copy).
    packed_key: Bytes,
    /// Distinguishes registrations, so a stale descriptor is detectable.
    generation: u64,
    /// Bytes charged against the budget for this arena — the backend's
    /// effective range, which is what `unmap_all` must release. Kept separate
    /// from `memory.len` so the claim and the release cannot drift apart.
    charged: u64,
    /// Arena length in [`GRANULE`]s — the unit the suballocator works in.
    granules: u32,
    free: Mutex<Allocator<u32>>,
    /// Live [`PinnedBuf`]s cut from this arena. Phase 4's reclamation reads it;
    /// here it makes "the pool really did give the space back" assertable.
    live: AtomicUsize,
    /// Dedicated arenas back exactly one oversize request and are never offered
    /// to the general search, so the request that motivated them cannot be
    /// crowded out by later small ones.
    dedicated: bool,
}

impl Arena {
    fn base(&self) -> *mut u8 {
        self.memory.ptr
    }

    /// Arena length in bytes, as mapped.
    pub(crate) fn len(&self) -> usize {
        self.memory.len
    }

    /// Live suballocations.
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// Try to cut `len` bytes out of this arena.
    ///
    /// `None` means "not from here" — no space, or the request does not fit at
    /// all. Never an error: the caller moves on to the next arena or grows the
    /// set.
    fn try_alloc(self: &Arc<Self>, len: usize) -> Option<PinnedBuf> {
        let granules = u32::try_from(len.div_ceil(GRANULE)).ok()?;
        if granules == 0 || granules > self.granules {
            return None;
        }
        let allocation = self.free.lock().allocate(granules)?;
        self.live.fetch_add(1, Ordering::AcqRel);
        Some(PinnedBuf {
            offset: allocation.offset as usize * GRANULE,
            len,
            allocation,
            arena: Arc::clone(self),
        })
    }

    /// Return a suballocation's space to the pool. Never touches the backend —
    /// the arena stays registered, which is the point of pooling.
    fn release(&self, allocation: Allocation<u32>) {
        self.free.lock().free(allocation);
        self.live.fetch_sub(1, Ordering::AcqRel);
    }
}

// ---------------------------------------------------------------------------
// PinnedBuf
// ---------------------------------------------------------------------------

/// An owned, registered byte range cut from an [`Arena`].
///
/// Derefs to its bytes, so it is usable as a plain buffer. Dropping it returns
/// the space to the pool and nothing else: the arena stays registered, so the
/// next allocation of that space costs no pin.
pub(crate) struct PinnedBuf {
    arena: Arc<Arena>,
    /// The free token. Private, and the only thing that can return the space.
    allocation: Allocation<u32>,
    /// Byte offset of this range within the arena.
    offset: usize,
    /// Exactly what the caller asked for. The suballocator rounds up to a float
    /// bin, so the reserved space is at least `len`; the extra bytes belong to
    /// nobody and are never exposed.
    len: usize,
}

impl PinnedBuf {
    /// How a peer would address these bytes.
    pub(crate) fn remote(&self) -> RemoteRef {
        RemoteRef {
            addr: self.addr(),
            len: self.len as u64,
            packed_key: self.arena.packed_key.clone(),
            generation: self.arena.generation,
        }
    }

    /// Absolute address of the first byte.
    pub(crate) fn addr(&self) -> u64 {
        self.arena.base() as u64 + self.offset as u64
    }

    /// Offset of this range inside its arena — what a backend GET wants when
    /// the arena is the destination.
    pub(crate) fn arena_offset(&self) -> u64 {
        self.offset as u64
    }

    /// Backend region id of the arena backing these bytes.
    pub(crate) fn backend_region_id(&self) -> u64 {
        self.arena.backend_region_id
    }

    /// Length in bytes, exactly as requested.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Whether the range is empty. Never true — the pool refuses zero-length
    /// requests — but clippy wants it beside `len`.
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Deref for PinnedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the range `offset .. offset + len` lies inside the arena's
        // allocation — `try_alloc` sized the request in granules against the
        // arena's own granule count — the arena is kept alive by the `Arc`, and
        // the suballocator hands out non-overlapping ranges, so no other
        // `PinnedBuf` aliases these bytes *from this process*.
        //
        // What this cannot establish is that nothing else is writing. The arena
        // is registered for RMA, so any peer holding its key can write into
        // this range at any moment, and `prot` is dead code in UCP — the
        // GET-only shape of the protocol above is a convention, not an
        // enforcement. A concurrent remote write is a data race the Rust
        // abstract machine has no vocabulary for. It is admitted as the
        // module's trust-domain assumption, not proved away here.
        unsafe { std::slice::from_raw_parts(self.arena.base().add(self.offset), self.len) }
    }
}

impl DerefMut for PinnedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as for `Deref`. `&mut self` proves this is the only *Rust*
        // handle to the range — it proves nothing about the NIC, which is a
        // writer the borrow checker cannot see and which needs no handle from
        // us to write here. Exclusivity against remote writers is a property of
        // the protocol and the trust domain, and the trust domain alone is what
        // makes this sound; claiming `&mut self` settles it would be a lie
        // about which writers exist.
        unsafe { std::slice::from_raw_parts_mut(self.arena.base().add(self.offset), self.len) }
    }
}

impl Drop for PinnedBuf {
    fn drop(&mut self) {
        self.arena.release(self.allocation);
    }
}

// ---------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------

/// Registered-bytes accounting shared by the pool and external regions (D9).
///
/// The axis is *mapped* bytes — what the backend has pinned — not suballocated
/// bytes, for the reason given on the config field it reads.
pub(crate) struct Budget {
    registered: AtomicU64,
    limit: u64,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
}

impl Budget {
    pub(crate) fn new(limit: u64, metrics: Option<Arc<crate::observability::VeloMetrics>>) -> Self {
        Self {
            registered: AtomicU64::new(0),
            limit,
            metrics,
        }
    }

    /// Claim `bytes` against the budget, or refuse.
    ///
    /// A compare-exchange loop rather than "read, compare, add": two concurrent
    /// registrations that each read an under-budget total and then both added
    /// would overshoot the ceiling by construction.
    ///
    /// Returns a [`Reservation`] that gives the bytes back unless it is
    /// committed. The registration it belongs to has an `await` between the
    /// claim and the commit, and that future may simply be dropped — a
    /// `timeout` around a registration, a `select!` arm. A manual release on
    /// the error path does not run then, and a budget that leaks on
    /// cancellation eventually refuses every registration for the life of the
    /// process while reporting nothing wrong.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, RdmaError> {
        let mut current = self.registered.load(Ordering::Acquire);
        loop {
            let exceeded = || RdmaError::BudgetExceeded {
                requested: bytes,
                registered: current,
                budget: self.limit,
            };
            let next = current.checked_add(bytes).ok_or_else(exceeded)?;
            if next > self.limit {
                return Err(exceeded());
            }
            match self.registered.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.publish(next);
                    return Ok(Reservation {
                        budget: Arc::clone(self),
                        bytes,
                        committed: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Give `bytes` back after a confirmed unmap.
    ///
    /// Saturating rather than a bare `fetch_sub`: an accounting mistake that
    /// released more than it claimed would wrap the counter to near `u64::MAX`
    /// and refuse every future registration permanently, while `publish`'s own
    /// saturation showed a reassuring zero on the gauge. One defensive
    /// `fetch_update` turns a silent permanent-failure class into an
    /// over-credit that self-corrects.
    pub(crate) fn release(&self, bytes: u64) {
        let next = self
            .registered
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .unwrap_or(0);
        self.publish(next.saturating_sub(bytes));
    }

    /// Add `bytes` to the running total whether or not it fits.
    ///
    /// For reconciliation only: the backend has already pinned the pages, so
    /// the choice is between an accurate total that briefly exceeds the ceiling
    /// and an inaccurate one that does not. Refusing here would mean unmapping
    /// a region that just mapped successfully over a page-rounding delta, which
    /// is strictly worse — the overshoot is bounded by one page per
    /// registration. Returns whether the total stayed within the ceiling.
    pub(crate) fn charge(&self, bytes: u64) -> bool {
        let next = self
            .registered
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(bytes))
            })
            .unwrap_or(0)
            .saturating_add(bytes);
        self.publish(next);
        next <= self.limit
    }

    /// Bytes currently registered.
    pub(crate) fn registered(&self) -> u64 {
        self.registered.load(Ordering::Acquire)
    }

    fn publish(&self, bytes: u64) {
        if let Some(m) = &self.metrics {
            m.set_rdma_registered_bytes(bytes);
        }
    }
}

/// A claim on the registered-bytes budget that is given back unless committed.
///
/// The point is cancellation. A registration claims budget, then awaits the
/// backend map; if that future is dropped mid-await, no error arm runs, and a
/// manually-released design leaks the claim silently and permanently. A guard
/// releases on the drop that cancellation actually performs.
///
/// [`commit`](Self::commit) is called only once the bytes are genuinely
/// registered, after which the matching [`Budget::release`] belongs to the
/// unmap.
#[must_use = "an uncommitted Reservation releases its claim when dropped"]
pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
    committed: bool,
}

impl Reservation {
    /// The bytes are registered: stop tracking them here, and leave the
    /// matching release to the unmap.
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }

    /// How many bytes this claim covers. The unmap must release exactly this
    /// number, which is why callers round *before* reserving.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Raise this claim to `bytes`, the size the backend actually pinned.
    ///
    /// The claim is taken before the map on a locally computed page-enclosing
    /// estimate, because the budget has to be held against a concurrent
    /// registration while the map runs. The backend then reports the range it
    /// really pinned, which may be larger — a bigger page size, an
    /// implementation that rounds further out. This settles the difference so
    /// the number released later is the number the kernel is holding.
    ///
    /// **Raises only.** Call sites pass `reported.max(local_estimate)`, so a
    /// backend that under-reports cannot shrink a claim below what this side
    /// believes is pinned — releasing less than was charged is a permanent
    /// upward skew, and releasing *more* than the kernel holds is worse still.
    /// There is deliberately no lowering path rather than an unreachable one.
    ///
    /// A top-up past the ceiling is accepted with a warning; see
    /// [`Budget::charge`].
    pub(crate) fn raise_to(&mut self, bytes: u64) {
        debug_assert!(
            bytes >= self.bytes,
            "raise_to must never shrink a claim: {} to {bytes}",
            self.bytes
        );
        let Some(extra) = bytes.checked_sub(self.bytes).filter(|e| *e != 0) else {
            return;
        };
        if !self.budget.charge(extra) {
            tracing::warn!(
                extra,
                registered = self.budget.registered(),
                budget = self.budget.limit,
                "rdma: the backend pinned more than the registered-bytes budget allows; \
                 accepting the overshoot rather than unmapping a live registration"
            );
        }
        self.bytes = bytes;
    }
}

/// The page-enclosing range a registration of `[ptr, ptr + len)` will pin.
///
/// Registration pins whole pages, so the kernel charges `RLIMIT_MEMLOCK` for
/// the enclosing range, not for what the caller asked for. A 4097-byte buffer
/// straddling a page boundary costs three pages, not two — so charging `len`
/// would let the budget, whose entire job is to be that valve, undercount by
/// most of a factor of two on small unaligned registrations.
pub(crate) fn page_enclosing_len(ptr: usize, len: usize) -> Option<u64> {
    let start = ptr & !(GRANULE - 1);
    let end = ptr.checked_add(len)?.checked_next_multiple_of(GRANULE)?;
    u64::try_from(end.checked_sub(start)?).ok()
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            self.budget.release(self.bytes);
        }
    }
}

/// `initial` doubled once per pooled arena, saturated at `max`.
///
/// The doubling is a multiply, not a shift, because `checked_shl` refuses only
/// an out-of-range *shift amount* — never a shifted-out *value*. Shifting
/// `initial` by the arena count answers `Some(0)` as soon as the last
/// significant bit leaves the word, and `0` read as a size collapses growth to
/// one request-sized arena per allocation. `checked_mul` refuses that overflow,
/// so the cap is reached instead.
///
/// Extracted from the growth path so the saturation can be tested directly:
/// reaching the interesting arena counts through `alloc` would mean mapping
/// tens of arenas.
pub(crate) fn pool_arena_target(initial: u64, max: u64, pooled: usize) -> u64 {
    u32::try_from(pooled)
        .ok()
        .and_then(|shift| 1u64.checked_shl(shift))
        .and_then(|factor| initial.checked_mul(factor))
        .unwrap_or(max)
        .min(max)
}

// ---------------------------------------------------------------------------
// ArenaSet
// ---------------------------------------------------------------------------

/// The pool: an append-only set of arenas with geometric growth.
pub(crate) struct ArenaSet {
    backend: Arc<dyn RdmaBackend>,
    cfg: RdmaPoolConfig,
    budget: Arc<Budget>,
    generations: Arc<AtomicU64>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
    /// Append-only until shutdown. Read on every allocation, written only by
    /// the task holding `grow`.
    arenas: RwLock<Vec<Arc<Arena>>>,
    /// Serialises growth so two concurrent misses map one arena, not two. An
    /// async mutex because what it guards is an await point.
    grow: tokio::sync::Mutex<()>,
    /// Arenas the sweep could not confirm an unmap for, held so their pages can
    /// be freed once velo shutdown completes rather than leaked for good.
    unconfirmed: Mutex<Vec<Arc<Arena>>>,
}

impl ArenaSet {
    pub(crate) fn new(
        backend: Arc<dyn RdmaBackend>,
        cfg: RdmaPoolConfig,
        budget: Arc<Budget>,
        generations: Arc<AtomicU64>,
        metrics: Option<Arc<crate::observability::VeloMetrics>>,
    ) -> Self {
        Self {
            backend,
            cfg,
            budget,
            generations,
            metrics,
            arenas: RwLock::new(Vec::new()),
            grow: tokio::sync::Mutex::new(()),
            unconfirmed: Mutex::new(Vec::new()),
        }
    }

    /// Cut `len` registered bytes out of the pool, growing it if needed.
    ///
    /// [`RdmaError::BudgetExceeded`] is the expected refusal under pressure;
    /// Phase 3's callers answer it by staging chunked, so it is a routing
    /// decision rather than a failure.
    pub(crate) async fn alloc(&self, len: usize) -> Result<PinnedBuf, RdmaError> {
        if len == 0 {
            return Err(RdmaError::OutOfRange);
        }
        let dedicated = len as u64 >= self.cfg.dedicated_arena_min;
        if !dedicated && let Some(buf) = self.try_existing(len) {
            return Ok(buf);
        }
        // One grower at a time. The re-check inside is not belt-and-braces:
        // whoever held the lock before us may have mapped exactly the arena
        // this request needs, and mapping a second one would double the pin.
        let _grow = self.grow.lock().await;
        if !dedicated && let Some(buf) = self.try_existing(len) {
            return Ok(buf);
        }
        let arena_bytes = if dedicated {
            (len as u64)
                .checked_next_multiple_of(GRANULE as u64)
                .ok_or(RdmaError::OutOfRange)?
        } else {
            self.next_pool_arena_bytes(len)?
        };
        let arena = self.map_arena(arena_bytes, dedicated).await?;
        let buf = arena
            .try_alloc(len)
            .ok_or_else(|| RdmaError::Backend("a fresh arena refused its own request".into()))?;
        self.arenas.write().push(arena);
        Ok(buf)
    }

    /// Walk the existing pooled arenas, first fit. They are few and ordered by
    /// creation, so the earlier (smaller) ones are tried first and the big tail
    /// stays available for big requests.
    fn try_existing(&self, len: usize) -> Option<PinnedBuf> {
        let arenas = self.arenas.read();
        arenas
            .iter()
            .filter(|a| !a.dedicated)
            .find_map(|arena| arena.try_alloc(len))
    }

    /// Geometric growth from `initial_arena_bytes`, doubling per pooled arena,
    /// capped at `max_arena_bytes` — and never smaller than the request that
    /// forced the growth.
    fn next_pool_arena_bytes(&self, len: usize) -> Result<u64, RdmaError> {
        let pooled = self.arenas.read().iter().filter(|a| !a.dedicated).count();
        let grown = pool_arena_target(
            self.cfg.initial_arena_bytes,
            self.cfg.max_arena_bytes,
            pooled,
        );
        let needed = (len as u64)
            .checked_next_multiple_of(GRANULE as u64)
            .ok_or(RdmaError::OutOfRange)?;
        Ok(grown.max(needed).max(GRANULE as u64))
    }

    /// Allocate pages, register them, and build the arena around them.
    ///
    /// The budget is claimed *before* the map, so a concurrent registration
    /// cannot slip in against bytes this one is about to consume, and the claim
    /// is a [`Reservation`] so a cancelled registration returns it.
    ///
    /// The rounding happens before the reservation, not inside `PageMemory`,
    /// so the number claimed here is the same number [`unmap_all`](Self::unmap_all)
    /// later releases. Reserving a caller-shaped length and releasing a
    /// page-rounded one would under-release on every arena — and since the
    /// counter is unsigned, the drift compounds until the pool refuses
    /// everything.
    async fn map_arena(&self, bytes: u64, dedicated: bool) -> Result<Arc<Arena>, RdmaError> {
        let len = usize::try_from(bytes)
            .ok()
            .and_then(|len| len.checked_next_multiple_of(GRANULE))
            .ok_or(RdmaError::OutOfRange)?;
        // Before anything is claimed or mapped. The suballocator indexes
        // granules with a `u32`, so an arena above 16 TiB cannot be described;
        // finding that out *after* a successful map would strand a backend
        // registration with nothing left holding its id to unmap it. This is
        // also the size cap a dedicated arena is subject to, and it now fails
        // with a reason rather than by leaking.
        let granules = u32::try_from(len / GRANULE).map_err(|_| RdmaError::OutOfRange)?;
        let mut reservation = self.budget.try_reserve(len as u64)?;

        let memory = PageMemory::new(len)?;
        debug_assert_eq!(
            memory.len as u64,
            reservation.bytes(),
            "the budget claim and the mapped length must be the same number"
        );
        // A failed *or cancelled* map leaves nothing registered (the backend
        // contract), so the pages here were never pinned and freeing them is
        // correct — but `PageMemory` leaks by default, and cancellation drops it
        // without running any arm of this match. So the flag is set for the
        // cancellation case *before* the await and cleared once the map has
        // actually succeeded, after which leak-by-default takes over for real.
        memory.mark_unmapped();
        let region = self.backend.map(memory.addr(), memory.len).await?;
        memory.mark_pinned();
        // Charge what the backend says it pinned, not what we asked for, so the
        // number released at unmap is the number the kernel is holding.
        reservation.raise_to(region.effective_len.max(memory.len as u64));
        debug_assert_eq!(
            memory.len / GRANULE,
            granules as usize,
            "the granule count was validated against a different length than was mapped"
        );
        let arena = Arc::new(Arena {
            backend_region_id: region.backend_region_id,
            packed_key: region.packed_key,
            generation: self.generations.fetch_add(1, Ordering::Relaxed),
            charged: reservation.bytes(),
            granules,
            // `max_allocs` is the granule count, not a cap below it. Node
            // metadata is what the allocator hands out alongside space, and
            // sizing it smaller means a fully-fragmented arena runs out of
            // *nodes* while it still has room — which surfaces as `allocate`
            // returning `None`, indistinguishable from "no space". The pool
            // then maps another arena and burns budget it did not need.
            // `Allocator::new`'s own default (128 Ki) is exactly that trap for
            // any arena above 512 MiB.
            free: Mutex::new(Allocator::with_max_allocs(granules, granules)),
            live: AtomicUsize::new(0),
            dedicated,
            memory,
        });
        if let Some(m) = &self.metrics {
            m.record_rdma_registration(crate::observability::RdmaRegistrationKind::Arena);
        }
        tracing::debug!(
            bytes = arena.len(),
            dedicated,
            generation = arena.generation,
            "rdma: mapped a new arena"
        );
        // Registered and tracked: the matching release now belongs to
        // `unmap_all`, and releases exactly the length claimed above.
        reservation.commit();
        Ok(arena)
    }

    /// Unmap every arena, each bounded by `deadline`.
    ///
    /// The deadline is not decoration: this runs inside
    /// `Velo::graceful_shutdown`, and a backend whose unmap never answers would
    /// otherwise park shutdown forever even under a `Timeout` policy — this was
    /// the one step of the sweep with no bound. On timeout the arena joins the
    /// unconfirmed list and the sweep moves on; the transport's own force-unmap
    /// at teardown is the backstop.
    ///
    /// Returns the number of arenas whose unmap was not confirmed. Their pages
    /// stay leaked *for now* — [`release_unconfirmed`](Self::release_unconfirmed)
    /// frees them once velo shutdown has completed and nothing can still be
    /// pinned.
    pub(crate) async fn unmap_all(&self, deadline: Instant) -> usize {
        let arenas: Vec<Arc<Arena>> = std::mem::take(&mut *self.arenas.write());
        let mut unconfirmed = Vec::new();
        for arena in arenas {
            let live = arena.live();
            if live != 0 {
                tracing::warn!(
                    live,
                    bytes = arena.len(),
                    "rdma: unmapping an arena that still has live suballocations"
                );
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let unmap = self.backend.unmap(arena.backend_region_id);
            let outcome = match tokio::time::timeout(remaining, unmap).await {
                Ok(result) => result,
                Err(_) => Err(RdmaError::Timeout),
            };
            match outcome {
                Ok(()) => {
                    arena.memory.mark_unmapped();
                    self.budget.release(arena.charged);
                }
                Err(e) => {
                    let bytes = arena.len();
                    tracing::warn!(
                        %e,
                        bytes,
                        "rdma: arena unmap unconfirmed; its pages stay leaked until velo \
                         shutdown completes"
                    );
                    unconfirmed.push(arena);
                }
            }
        }
        let count = unconfirmed.len();
        self.unconfirmed.lock().extend(unconfirmed);
        count
    }

    /// Release the arenas whose unmap could not be confirmed.
    ///
    /// Called only once velo shutdown has fully completed, at which point the
    /// transport teardown has force-unmapped everything it still held — so
    /// nothing is pinned any more, whatever the unmap replies said. Without
    /// this, an unconfirmed unmap would leak the arena for the life of the
    /// process rather than for the length of the shutdown.
    pub(crate) fn release_unconfirmed(&self) {
        for arena in self.unconfirmed.lock().drain(..) {
            arena.memory.mark_unmapped();
            self.budget.release(arena.charged);
        }
    }

    /// Arenas currently mapped. Test and diagnostics surface.
    pub(crate) fn arena_count(&self) -> usize {
        self.arenas.read().len()
    }

    /// Live suballocations across every arena.
    pub(crate) fn live_allocations(&self) -> usize {
        self.arenas.read().iter().map(|a| a.live()).sum()
    }
}
