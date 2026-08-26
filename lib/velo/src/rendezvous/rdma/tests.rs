// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the RDMA registration layer.
//!
//! Two harnesses, deliberately:
//!
//! * [`MockBackend`] — an in-process [`RdmaBackend`] that mints ids and fake
//!   keys. Everything about the pool, the budget, the guard lifecycle and the
//!   shutdown ordering is *velo* logic, and testing it against a mock makes
//!   those tests fast, deterministic, and able to inject failures UCX would
//!   never produce on demand (a `ShuttingDown` unmap, a slow map).
//! * A real [`UcxTransport`] pair over `UCX_TLS=tcp` — the same lane CI runs
//!   without RDMA hardware. These prove the wiring: that the projection of
//!   `RmaError`, the region ids, and the shutdown ordering hold against the
//!   actual progress thread, asserted through its own `live_regions` count
//!   rather than through bookkeeping this module also owns.
//!
//! A mock-only suite would test the layer against its own assumptions; a
//! UCX-only suite could not reach the failure paths. Both, then.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use velo_ext::Transport;

use super::arena::{ArenaSet, Budget, GRANULE, RdmaPoolConfig};
use super::backend::{BackendGet, BackendRegion, RdmaBackend, RdmaError, UcxBackend};
use super::region::Deregistered;
use super::{RdmaConfig, RdmaRegistry};

/// Generous ceiling for anything that should resolve promptly.
const T: Duration = Duration::from_secs(10);

/// An [`RdmaBackend`] that registers nothing and remembers everything.
///
/// `map` mints an id and a plausible packed key; `unmap` records the id. The
/// injectable knobs exist because the interesting registry behaviour is what it
/// does when the backend misbehaves, and UCX cannot be asked to misbehave on
/// cue.
struct MockBackend {
    next_id: AtomicU64,
    mapped: dashmap::DashMap<u64, (usize, usize)>,
    unmapped: AtomicUsize,
    /// When set, every `unmap` answers `ShuttingDown` — the case where the
    /// pages are *not* known to be released.
    refuse_unmap: AtomicBool,
    /// Artificial delay on `map`, for racing a registration against shutdown.
    map_delay: parking_lot::Mutex<Option<Duration>>,
}

impl MockBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            mapped: dashmap::DashMap::new(),
            unmapped: AtomicUsize::new(0),
            refuse_unmap: AtomicBool::new(false),
            map_delay: parking_lot::Mutex::new(None),
        })
    }

    /// Registrations the backend still believes it holds.
    fn live(&self) -> usize {
        self.mapped.len()
    }

    fn unmap_calls(&self) -> usize {
        self.unmapped.load(Ordering::SeqCst)
    }
}

impl RdmaBackend for MockBackend {
    fn key(&self) -> &str {
        "mock"
    }

    fn map(&self, ptr: usize, len: usize) -> BoxFuture<'_, Result<BackendRegion, RdmaError>> {
        Box::pin(async move {
            let delay = *self.map_delay.lock();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            self.mapped.insert(id, (ptr, len));
            Ok(BackendRegion {
                backend_region_id: id,
                // Rounded outward, exactly as a real registration would be, so
                // anything that mistakes the effective range for the requested
                // one is visible here rather than only on hardware.
                effective_addr: (ptr & !(GRANULE - 1)) as u64,
                effective_len: len.next_multiple_of(GRANULE) as u64,
                packed_key: Bytes::from_static(b"mock-packed-key"),
            })
        })
    }

    fn unmap(&self, backend_region_id: u64) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move {
            self.unmapped.fetch_add(1, Ordering::SeqCst);
            if self.refuse_unmap.load(Ordering::SeqCst) {
                return Err(RdmaError::ShuttingDown);
            }
            // Idempotent, per the trait contract: an id that names nothing is
            // the state the caller asked for.
            self.mapped.remove(&backend_region_id);
            Ok(())
        })
    }

    fn get(&self, _req: BackendGet) -> BoxFuture<'_, Result<(), RdmaError>> {
        Box::pin(async move { Ok(()) })
    }
}

/// A pool over a mock backend with test-sized arenas.
fn mock_pool(cfg: RdmaPoolConfig) -> (Arc<MockBackend>, ArenaSet) {
    let backend = MockBackend::new();
    let budget = Arc::new(Budget::new(cfg.registered_bytes_budget, None));
    let pool = ArenaSet::new(
        Arc::clone(&backend) as Arc<dyn RdmaBackend>,
        cfg,
        Arc::clone(&budget),
        Arc::new(AtomicU64::new(1)),
        None,
    );
    (backend, pool)
}

/// Arena sizes small enough that growth and exhaustion happen in a test rather
/// than after 64 MiB of allocation.
fn small_pool_config() -> RdmaPoolConfig {
    RdmaPoolConfig {
        initial_arena_bytes: 64 * GRANULE as u64,
        max_arena_bytes: 256 * GRANULE as u64,
        dedicated_arena_min: 128 * GRANULE as u64,
        registered_bytes_budget: 1024 * GRANULE as u64,
    }
}

/// A registry over a mock backend, for lifecycle tests that need no UCX.
fn mock_registry(cfg: RdmaConfig) -> (Arc<MockBackend>, Arc<RdmaRegistry>) {
    let backend = MockBackend::new();
    let registry = Arc::new(RdmaRegistry::new(
        Arc::clone(&backend) as Arc<dyn RdmaBackend>,
        cfg,
        tokio::runtime::Handle::current(),
        None,
    ));
    (backend, registry)
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

/// A round trip through the pool: the buffer is writable, addresses inside the
/// arena, and describes itself consistently.
#[tokio::test]
async fn pool_alloc_roundtrip() {
    let (backend, pool) = mock_pool(small_pool_config());

    let mut buf = pool.alloc(4000).await.expect("alloc");
    assert_eq!(
        buf.len(),
        4000,
        "the exact requested length is what is handed out"
    );
    buf.fill(0xAB);
    assert!(buf.iter().all(|b| *b == 0xAB));

    let remote = buf.remote();
    assert_eq!(remote.addr, buf.addr());
    assert_eq!(remote.len, 4000);
    assert_eq!(&remote.packed_key[..], b"mock-packed-key");

    assert_eq!(
        backend.live(),
        1,
        "one arena maps once, however many buffers come out of it"
    );
    assert_eq!(pool.arena_count(), 1);
    assert_eq!(pool.live_allocations(), 1);

    drop(buf);
    assert_eq!(pool.live_allocations(), 0);
    assert_eq!(
        backend.unmap_calls(),
        0,
        "returning a suballocation must never touch the backend; the arena stays registered"
    );
}

/// Sub-granule requests still consume a whole granule, and the reported length
/// stays the requested one — the rounding must not leak into the descriptor.
#[tokio::test]
async fn pool_rounds_to_granules() {
    let (_backend, pool) = mock_pool(small_pool_config());

    let a = pool.alloc(1).await.expect("alloc a");
    let b = pool.alloc(1).await.expect("alloc b");
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    let gap = b.addr().abs_diff(a.addr());
    assert!(
        gap >= GRANULE as u64,
        "two live buffers shared a granule: {gap} bytes apart"
    );
}

/// Zero-length allocations are refused at the boundary rather than producing a
/// buffer that names no bytes.
#[tokio::test]
async fn pool_refuses_zero_length() {
    let (_backend, pool) = mock_pool(small_pool_config());
    assert_eq!(pool.alloc(0).await.err(), Some(RdmaError::OutOfRange));
}

/// The set grows geometrically rather than mapping one arena per request, and
/// each new arena is at least as big as the last.
#[tokio::test]
async fn pool_grows_geometrically() {
    let cfg = small_pool_config();
    let (backend, pool) = mock_pool(cfg.clone());

    // Fill well past the first arena. Each buffer is a quarter of it, so the
    // fifth is the one that cannot fit.
    let quarter = cfg.initial_arena_bytes as usize / 4;
    let mut held = Vec::new();
    for _ in 0..12 {
        held.push(pool.alloc(quarter).await.expect("alloc"));
    }

    let arenas = pool.arena_count();
    assert!(arenas >= 2, "the pool never grew: {arenas} arenas");
    assert!(
        arenas < 12,
        "the pool mapped an arena per allocation ({arenas}); growth is not geometric"
    );
    assert_eq!(
        backend.live(),
        arenas,
        "every arena is one backend registration"
    );
}

/// A request at or above `dedicated_arena_min` gets an arena sized to it,
/// instead of forcing the pool up a growth step and wasting the round-up.
#[tokio::test]
async fn pool_dedicates_an_arena_to_oversize_requests() {
    let cfg = small_pool_config();
    let (backend, pool) = mock_pool(cfg.clone());

    let small = pool.alloc(GRANULE).await.expect("small alloc");
    let arenas_before = pool.arena_count();

    let big = pool
        .alloc(cfg.dedicated_arena_min as usize)
        .await
        .expect("oversize alloc");
    assert_eq!(
        pool.arena_count(),
        arenas_before + 1,
        "an oversize request must get its own arena"
    );
    assert_eq!(backend.live(), arenas_before + 1);

    // The dedicated arena is not offered to the general search, so a later
    // small request cannot land inside it and strand the big one.
    let another_small = pool.alloc(GRANULE).await.expect("second small alloc");
    let big_end = big.addr() + big.len() as u64;
    assert!(
        another_small.addr() < big.addr() || another_small.addr() >= big_end,
        "a pooled allocation landed inside the dedicated arena"
    );
    drop((small, big, another_small));
}

/// Over the registered-bytes ceiling the pool refuses with `BudgetExceeded`,
/// which is the signal Phase 3 turns into "stage chunked instead" — never a
/// panic and never a hard failure of the staging operation (D4).
#[tokio::test]
async fn pool_budget_exhaustion_is_a_refusal() {
    let cfg = RdmaPoolConfig {
        initial_arena_bytes: 16 * GRANULE as u64,
        max_arena_bytes: 16 * GRANULE as u64,
        dedicated_arena_min: 1024 * GRANULE as u64,
        // Room for exactly two arenas.
        registered_bytes_budget: 32 * GRANULE as u64,
    };
    let (backend, pool) = mock_pool(cfg);

    let mut held = Vec::new();
    let mut refusal = None;
    for _ in 0..64 {
        match pool.alloc(8 * GRANULE).await {
            Ok(buf) => held.push(buf),
            Err(e) => {
                refusal = Some(e);
                break;
            }
        }
    }

    match refusal {
        Some(RdmaError::BudgetExceeded {
            registered, budget, ..
        }) => {
            assert_eq!(budget, 32 * GRANULE as u64);
            assert!(
                registered <= budget,
                "the budget was overshot before it was enforced: {registered} over {budget}"
            );
        }
        other => panic!("expected a budget refusal, got {other:?}"),
    }
    assert_eq!(
        backend.live(),
        2,
        "the pool mapped past its own ceiling before refusing"
    );
}

/// Space really comes back: fill the pool to its budget, drop everything, and
/// allocate again.
///
/// The assertion is that a subsequent allocation *succeeds*, not that some byte
/// arithmetic balances. `offset-allocator` rounds a request up to a float bin,
/// so capacity is not the sum of the requested lengths and an exact-capacity
/// assertion would be testing the allocator, not the pool.
#[tokio::test]
async fn pool_reuses_space_after_drop() {
    let cfg = RdmaPoolConfig {
        initial_arena_bytes: 16 * GRANULE as u64,
        max_arena_bytes: 16 * GRANULE as u64,
        dedicated_arena_min: 1024 * GRANULE as u64,
        registered_bytes_budget: 32 * GRANULE as u64,
    };
    let (backend, pool) = mock_pool(cfg);

    let mut held = Vec::new();
    while let Ok(buf) = pool.alloc(4 * GRANULE).await {
        held.push(buf);
        assert!(held.len() < 64, "the pool never filled up");
    }
    let filled = held.len();
    assert!(filled > 0, "nothing could be allocated at all");
    let arenas_when_full = pool.arena_count();

    drop(held);
    assert_eq!(pool.live_allocations(), 0);

    let again = pool.alloc(4 * GRANULE).await;
    assert!(
        again.is_ok(),
        "space returned by a dropped PinnedBuf was not reusable: {:?}",
        again.err()
    );
    assert_eq!(
        pool.arena_count(),
        arenas_when_full,
        "reuse mapped a new arena instead of using the space that came back"
    );
    assert_eq!(backend.live(), arenas_when_full);
}

/// Concurrent allocation and release: no double-issued range, no lost space,
/// and the growth path serialises so the arena count stays sane.
///
/// Each task writes its own byte value across its whole buffer and reads it
/// back after a yield. Two buffers overlapping would show up as a mismatch,
/// which is the property that actually matters — an offset bug in the
/// suballocator is invisible to a count-based assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_concurrent_alloc_and_free() {
    const TASKS: usize = 16;
    const ROUNDS: usize = 24;

    let cfg = small_pool_config();
    let (backend, pool) = mock_pool(cfg);
    let pool = Arc::new(pool);

    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let pool = Arc::clone(&pool);
        tasks.push(tokio::spawn(async move {
            let tag = (task % 251) as u8;
            for round in 0..ROUNDS {
                let len = GRANULE * (1 + (round % 3));
                let mut buf = pool.alloc(len).await.expect("concurrent alloc");
                buf.fill(tag);
                tokio::task::yield_now().await;
                assert!(
                    buf.iter().all(|b| *b == tag),
                    "another allocation wrote into this range: task {task}, round {round}"
                );
            }
        }));
    }
    for task in tasks {
        task.await.expect("task panicked");
    }

    assert_eq!(pool.live_allocations(), 0, "a suballocation leaked");
    assert_eq!(
        backend.live(),
        pool.arena_count(),
        "the arena set and the backend disagree about what is mapped"
    );
    assert_eq!(
        backend.unmap_calls(),
        0,
        "no arena should have been unmapped"
    );
}

// ---------------------------------------------------------------------------
// RegionGuard lifecycle
// ---------------------------------------------------------------------------

/// The happy path: register, observe, unregister, and only then does
/// `deregistered()` resolve.
#[tokio::test]
async fn region_unregister_latches_deregistered() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 8192].into_boxed_slice())
        .await
        .expect("register");

    assert_eq!(backend.live(), 1);
    assert_eq!(registry.region_count(), 1);
    // The budget charges the page-enclosing range, not the requested length: a
    // heap `Box` is byte-aligned, so 8192 bytes generally straddles three pages
    // and that is what the kernel pins.
    let charged = registry.registered_bytes();
    assert!(
        charged >= 8192 && charged % GRANULE as u64 == 0,
        "expected a page-enclosing charge, got {charged}"
    );
    assert!(
        !guard.is_deregistered(),
        "a live registration must not claim to be deregistered"
    );

    // The latch is not resolved yet, so awaiting it must not complete.
    let watch = guard.watch();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), watch.deregistered())
            .await
            .is_err(),
        "deregistered() resolved while the memory was still registered"
    );

    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained
    );

    assert_eq!(backend.live(), 0);
    assert_eq!(registry.region_count(), 0);
    assert_eq!(
        registry.registered_bytes(),
        0,
        "the budget was not credited back"
    );
    assert!(watch.is_deregistered());
    tokio::time::timeout(T, watch.deregistered())
        .await
        .expect("the latch must be resolved for every observer, not just the unregisterer");
}

/// An unmap the backend could not confirm must **not** latch.
///
/// This is the whole point of the two-clause contract. `ShuttingDown` from an
/// unmap means *unknown*, not *unmapped*; latching on it would tell the caller
/// it may free memory that is still pinned. The caller is instead covered by
/// the other clause — velo shutdown having completed.
#[tokio::test]
async fn unconfirmed_unmap_does_not_latch() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();
    let charged = registry.registered_bytes();

    backend.refuse_unmap.store(true, Ordering::SeqCst);
    let err = guard
        .unregister(T)
        .await
        .expect_err("the unmap was refused");
    assert_eq!(err, RdmaError::ShuttingDown);

    assert!(
        !watch.is_deregistered(),
        "an unconfirmed unmap latched deregistered(): a caller would now free pinned memory"
    );
    assert_eq!(
        registry.region_count(),
        1,
        "an unconfirmed region must stay tracked so the shutdown sweep asks again"
    );
    assert_eq!(
        registry.registered_bytes(),
        charged,
        "the budget was credited back for memory that may still be pinned"
    );

    // And once the backend cooperates, the sweep does resolve it.
    backend.refuse_unmap.store(false, Ordering::SeqCst);
    registry.shutdown(T).await;
    assert!(watch.is_deregistered());
    assert_eq!(registry.registered_bytes(), 0);
}

/// Dropping the guard without awaiting deregisters in the background — it does
/// not leak, and it does not block the dropping thread either.
///
/// The observable is the backend actually being asked to unmap, reached through
/// a `RegionWatch` taken before the drop. `Drop` returning is explicitly *not*
/// the point at which the memory is free.
#[tokio::test]
async fn dropped_guard_deregisters_in_the_background() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();

    drop(guard);

    tokio::time::timeout(T, watch.deregistered())
        .await
        .expect("a dropped guard must still deregister");
    assert_eq!(
        backend.live(),
        0,
        "the backend still holds the registration"
    );
    assert_eq!(registry.region_count(), 0);
    assert_eq!(registry.registered_bytes(), 0);
}

/// Dropping a guard on a plain thread with no ambient runtime still works,
/// because the registry captured a runtime handle at construction rather than
/// reading one from the environment at drop time.
#[tokio::test(flavor = "multi_thread")]
async fn guard_dropped_off_runtime_still_deregisters() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();

    std::thread::spawn(move || drop(guard))
        .join()
        .expect("dropper thread panicked");

    tokio::time::timeout(T, watch.deregistered())
        .await
        .expect("a guard dropped off the runtime must still deregister");
    assert_eq!(backend.live(), 0);
}

/// `unregister` waits for the region in-flight count to drain before unmapping.
///
/// Phase 3 acquires one of these guards per RDMA lease; this is the mechanism
/// that stops a registration being pulled out from under a transfer. Asserted
/// as "pending while held, resolves after release" rather than by sleeping past
/// a guessed interval.
#[tokio::test(flavor = "multi_thread")]
async fn unregister_waits_for_in_flight() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");

    let lease = guard.in_flight().acquire();
    let watch = guard.watch();

    let mut unregistering = tokio::spawn(async move { guard.unregister(T).await });

    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut unregistering)
            .await
            .is_err(),
        "unregister completed while an operation was still in flight"
    );
    assert_eq!(
        backend.unmap_calls(),
        0,
        "the backend was asked to unmap before the region had drained"
    );

    drop(lease);

    let outcome = tokio::time::timeout(T, unregistering)
        .await
        .expect("unregister must resolve once the last in-flight guard is released")
        .expect("task panicked")
        .expect("unregister");
    assert_eq!(
        outcome,
        Deregistered::Drained,
        "the drain completed, so this is not a timed-out deregistration"
    );
    assert!(watch.is_deregistered());
    assert_eq!(backend.live(), 0);
}

/// A drain that outlasts the budget still unmaps, and says so with `Timeout`.
///
/// Waiting forever on a peer that may have crashed is the worse failure, so the
/// bounded wait force-unmaps — and the latch does resolve, because the unmap
/// itself was confirmed. Only the *drain* was cut short.
#[tokio::test(flavor = "multi_thread")]
async fn unregister_timeout_still_unmaps() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();

    // Never released: this stands in for a lease whose holder has gone away.
    let _stuck = guard.in_flight().acquire();

    let outcome = guard
        .unregister(Duration::from_millis(100))
        .await
        .expect("a confirmed unmap is Ok even when the drain was cut short");
    assert_eq!(
        outcome,
        Deregistered::DrainTimedOut,
        "the caller must be able to tell that in-flight work was not waited for"
    );

    assert!(
        watch.is_deregistered(),
        "the unmap was confirmed, so the latch must resolve even though the drain timed out"
    );
    assert_eq!(
        backend.live(),
        0,
        "a timed-out drain must still force the unmap"
    );
}

/// `watch()` observes the same states as the guard, and keeps working after the
/// guard is gone. Holding one neither keeps the registration alive nor ends it.
#[tokio::test]
async fn watch_observes_without_owning() {
    let (_backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");

    let watch = guard.watch();
    let second = watch.clone();
    assert!(!watch.is_shutting_down());
    assert!(!watch.is_deregistered());

    registry.shutdown(T).await;

    assert!(watch.is_shutting_down(), "a watch must see shutdown begin");
    assert!(second.is_deregistered());
    tokio::time::timeout(T, second.shutdown_initiated())
        .await
        .expect("shutdown_initiated must resolve once shutdown has begun");
    drop(guard);
}

/// `register_owned` hands the buffer back on a confirmed deregistration, and
/// keeps it otherwise — a `Box` returned while the pages may still be pinned is
/// exactly the free-while-mapped hazard.
#[tokio::test]
async fn register_owned_returns_the_buffer() {
    let (backend, registry) = mock_registry(RdmaConfig::default());

    let mut buf = vec![0u8; 4096].into_boxed_slice();
    buf[0] = 0x5A;
    let guard = registry.register_owned(buf).await.expect("register");
    assert_eq!(guard.len(), 4096);

    let (returned, outcome) = guard.unregister_owned(T).await.expect("unregister_owned");
    assert_eq!(outcome, Deregistered::Drained);
    assert_eq!(returned.len(), 4096);
    assert_eq!(
        returned[0], 0x5A,
        "the buffer that came back is not the one that went in"
    );
    assert_eq!(backend.live(), 0);
}

/// The unsafe path, exercised against an allocation deliberately leaked for the
/// duration — which is the honest way to satisfy the safety contract in a test.
#[tokio::test]
async fn register_external_memory_smoke() {
    let (backend, registry) = mock_registry(RdmaConfig::default());

    // Leaked on purpose: the contract requires the allocation to outlive the
    // registration, and this test asserts on the registration, not on reclaim.
    let leaked: &'static mut [u8] = Box::leak(vec![7u8; 8192].into_boxed_slice());
    let ptr = std::ptr::NonNull::new(leaked.as_mut_ptr()).expect("non-null");

    // SAFETY: `leaked` is a live 8192-byte allocation that is never freed, so it
    // outlives the registration unconditionally.
    let guard = unsafe { registry.register_external(ptr, leaked.len()) }
        .await
        .expect("register external");

    assert_eq!(guard.addr(), ptr.as_ptr() as u64);
    assert_eq!(guard.len(), 8192);
    let (eff_addr, eff_len) = guard.effective_range();
    assert!(
        eff_addr <= guard.addr() && eff_len >= guard.len() as u64,
        "the effective range must cover the requested one"
    );
    assert_ne!(
        guard.generation(),
        0,
        "every registration gets a generation"
    );

    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained
    );
    assert_eq!(backend.live(), 0);
}

/// Degenerate arguments are refused at the boundary rather than reaching the
/// backend as a map of nothing.
#[tokio::test]
async fn register_external_refuses_degenerate_ranges() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let mut byte = 0u8;
    let ptr = std::ptr::NonNull::new(&mut byte as *mut u8).expect("non-null");

    // SAFETY: `ptr` is valid; the call is refused before it is ever used.
    let err = unsafe { registry.register_external(ptr, 0) }
        .await
        .unwrap_err();
    assert_eq!(err, RdmaError::OutOfRange);
    assert_eq!(
        backend.live(),
        0,
        "a refused registration must not reach the backend"
    );
}

// ---------------------------------------------------------------------------
// Registry shutdown (D8 steps 1 to 3)
// ---------------------------------------------------------------------------

/// The sweep unmaps everything — external regions and pool arenas alike — and
/// resolves every latch.
#[tokio::test]
async fn shutdown_deregisters_regions_and_arenas() {
    let cfg = RdmaConfig {
        pool: small_pool_config(),
        ..RdmaConfig::default()
    };
    let (backend, registry) = mock_registry(cfg);

    let guard = registry
        .register_owned(vec![0u8; 8192].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();
    let buf = registry.alloc_pinned(4096).await.expect("alloc pinned");
    assert!(
        backend.live() >= 2,
        "expected an external region and an arena"
    );

    // The buffer outlives the sweep on purpose: shutdown must not depend on
    // every caller having tidied up first.
    registry.shutdown(T).await;

    assert_eq!(backend.live(), 0, "shutdown left something registered");
    assert!(
        watch.is_deregistered(),
        "shutdown did not resolve the latch"
    );
    assert_eq!(registry.registered_bytes(), 0);
    assert_eq!(registry.pool().arena_count(), 0);
    drop(buf);
    drop(guard);
}

/// After the gate closes, both registration paths refuse. This is what stops a
/// registration landing behind the sweep with no tracking entry and no latch.
#[tokio::test]
async fn shutdown_gates_new_registrations() {
    let (_backend, registry) = mock_registry(RdmaConfig::default());
    registry.shutdown(T).await;

    let refused = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect_err("a gated registry must refuse");
    assert_eq!(refused.cause, RdmaError::ShuttingDown);
    assert_eq!(
        refused.buffer.map(|b| b.len()),
        Some(4096),
        "a refused registration must hand the caller buffer back"
    );
    assert_eq!(
        registry.alloc_pinned(4096).await.err(),
        Some(RdmaError::ShuttingDown),
        "pool allocation must go through the same gate as external registration"
    );
}

/// Shutdown is idempotent: a second sweep over an empty registry is a no-op
/// rather than a double-unmap or a double budget credit.
#[tokio::test]
async fn shutdown_is_idempotent() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect("register");
    drop(guard);

    registry.shutdown(T).await;
    let calls = backend.unmap_calls();
    registry.shutdown(T).await;
    assert_eq!(
        backend.unmap_calls(),
        calls,
        "a second shutdown re-issued unmaps"
    );
    assert_eq!(registry.registered_bytes(), 0);
}

/// A registration already past the gate must land before the sweep enumerates.
///
/// This is the race the admission counter exists for: a token-check gate would
/// let this registration pass the check, map after step 3 had walked the
/// region map, and leave pinned memory with no entry and no latch. The
/// backend delay widens the window from instructions to milliseconds so the
/// test is a detector rather than a coin flip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_in_flight_is_not_missed_by_shutdown() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    *backend.map_delay.lock() = Some(Duration::from_millis(200));

    let registering = {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            registry
                .register_owned(vec![0u8; 4096].into_boxed_slice())
                .await
        })
    };

    // Let the registration get past the gate and into the backend map.
    tokio::time::sleep(Duration::from_millis(50)).await;
    registry.shutdown(T).await;

    let outcome = tokio::time::timeout(T, registering)
        .await
        .expect("the registration must resolve")
        .expect("task panicked");

    match outcome {
        Ok(guard) => {
            // It got in. The sweep must have waited for it and unmapped it.
            assert!(
                guard.is_deregistered(),
                "a registration admitted before the gate closed was missed by the sweep"
            );
        }
        Err(e) if e.cause == RdmaError::ShuttingDown => {
            // It was refused at the gate, which is equally correct.
            assert!(e.buffer.is_some(), "a refusal must return the buffer");
        }
        Err(e) => panic!("unexpected registration failure: {}", e.cause),
    }
    assert_eq!(
        backend.live(),
        0,
        "shutdown returned with memory still registered"
    );
    assert_eq!(registry.registered_bytes(), 0);
}

// ---------------------------------------------------------------------------
// Against a real UCX transport, over UCX_TLS=tcp
// ---------------------------------------------------------------------------

/// A started [`UcxTransport`] plus a registry over its RMA endpoint.
///
/// `_streams` is held because dropping the receivers would tear the inbound
/// channels down under a transport that is still running.
struct UcxHarness {
    transport: Arc<crate::transports::ucx::UcxTransport>,
    registry: Arc<RdmaRegistry>,
    _streams: crate::transports::DataStreams,
}

impl UcxHarness {
    async fn start(cfg: RdmaConfig) -> Self {
        use velo_ext::InstanceId;

        let transport = Arc::new(
            crate::transports::ucx::UcxTransportBuilder::new()
                .tls("tcp")
                .build()
                .expect("build ucx transport"),
        );
        let (adapter, streams) = crate::transports::make_channels();
        tokio::time::timeout(
            T,
            transport.start(
                InstanceId::new_v4(),
                adapter,
                tokio::runtime::Handle::current(),
            ),
        )
        .await
        .expect("ucx startup must not hang")
        .expect("start ucx transport");

        let registry = Arc::new(RdmaRegistry::new(
            UcxBackend::new(transport.rdma_endpoint()),
            cfg,
            tokio::runtime::Handle::current(),
            None,
        ));
        Self {
            transport,
            registry,
            _streams: streams,
        }
    }

    /// Regions the progress thread itself believes it holds. The authoritative
    /// count: this module cannot fake it, which is the point of asserting on it
    /// rather than on `registry.region_count()`.
    fn live_regions(&self) -> usize {
        self.transport.live_regions()
    }
}

/// The backend really registers with UCX, and really releases it.
///
/// Asserted through the progress thread's own `live_regions`, so a registration
/// the registry has forgotten but UCX still holds cannot pass.
#[tokio::test(flavor = "multi_thread")]
async fn ucx_backend_maps_and_unmaps() {
    let harness = UcxHarness::start(RdmaConfig::default()).await;
    assert_eq!(harness.live_regions(), 0);
    assert_eq!(harness.registry.backend_key(), "ucx");

    let guard = harness
        .registry
        .register_owned(vec![0u8; 256 * 1024].into_boxed_slice())
        .await
        .expect("register with ucx");

    assert_eq!(harness.live_regions(), 1, "ucx did not register the range");
    let remote = guard.remote();
    assert!(
        !remote.packed_key.is_empty(),
        "a real registration must produce a packed key"
    );
    let (eff_addr, eff_len) = guard.effective_range();
    assert!(
        eff_addr <= guard.addr() && eff_len >= guard.len() as u64,
        "ucx reported an effective range that does not cover the request"
    );

    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained
    );
    assert_eq!(harness.live_regions(), 0, "ucx still holds the region");
    assert_eq!(harness.registry.registered_bytes(), 0);
    harness.transport.shutdown();
}

/// The pool over a real backend: an arena is one UCX registration however many
/// buffers come out of it, and the shutdown sweep releases it.
#[tokio::test(flavor = "multi_thread")]
async fn ucx_pool_arena_is_one_registration() {
    let cfg = RdmaConfig {
        pool: small_pool_config(),
        ..RdmaConfig::default()
    };
    let harness = UcxHarness::start(cfg).await;

    let a = harness.registry.alloc_pinned(4096).await.expect("alloc a");
    let b = harness.registry.alloc_pinned(4096).await.expect("alloc b");
    assert_eq!(
        harness.live_regions(),
        1,
        "two suballocations from one arena must be one ucx registration"
    );
    assert_eq!(a.backend_region_id(), b.backend_region_id());
    assert_ne!(a.arena_offset(), b.arena_offset());
    drop((a, b));

    harness.registry.shutdown(T).await;
    assert_eq!(harness.live_regions(), 0, "the arena was not unmapped");
    harness.transport.shutdown();
}

/// The ordering D8 exists for, asserted end to end through the real
/// `Velo::graceful_shutdown`.
///
/// The load-bearing claim is not "the memory is eventually released" but
/// "it is released *before* graceful_shutdown returns, and before the transport
/// is torn down". So the assertion is taken at the moment shutdown returns,
/// against the progress thread's own region count — the one number that cannot be
/// satisfied by bookkeeping in the layer under test. If the registry sweep ran
/// after transport teardown, or not at all, `live_regions` would be non-zero
/// here or the unmap would have been a forced one from teardown rather than an
/// orderly one from the sweep.
#[tokio::test(flavor = "multi_thread")]
async fn velo_graceful_shutdown_deregisters_before_transport_teardown() {
    let transport = Arc::new(
        crate::transports::ucx::UcxTransportBuilder::new()
            .tls("tcp")
            .build()
            .expect("build ucx transport"),
    );
    let velo = crate::Velo::builder()
        .add_ucx_transport(Arc::clone(&transport))
        .build()
        .await
        .expect("build velo");

    let guard = velo
        .register_owned(vec![0u8; 128 * 1024].into_boxed_slice())
        .await
        .expect("register through the velo facade");
    let watch = guard.watch();
    assert_eq!(transport.live_regions(), 1);
    assert!(
        velo.rdma_registered_bytes() >= 128 * 1024,
        "the budget must charge at least the requested length"
    );

    velo.graceful_shutdown(velo_ext::ShutdownPolicy::Timeout(T))
        .await;

    assert!(
        watch.is_deregistered(),
        "graceful_shutdown returned without resolving the deregistered() latch"
    );
    assert_eq!(
        transport.live_regions(),
        0,
        "graceful_shutdown returned with memory still registered with ucx"
    );
    assert_eq!(
        transport.live_rkeys(),
        0,
        "an unpacked rkey outlived shutdown"
    );
    assert_eq!(velo.rdma_registered_bytes(), 0);
    drop(guard);
}

/// A guard dropped without awaiting is deregistered in the background against
/// the real backend too — the failure mode being ruled out is a warn that
/// announces a deregistration nobody actually performs.
#[tokio::test(flavor = "multi_thread")]
async fn ucx_dropped_guard_deregisters() {
    let harness = UcxHarness::start(RdmaConfig::default()).await;
    let guard = harness
        .registry
        .register_owned(vec![0u8; 64 * 1024].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();
    assert_eq!(harness.live_regions(), 1);

    drop(guard);

    tokio::time::timeout(T, watch.deregistered())
        .await
        .expect("a dropped guard must deregister against ucx too");
    assert_eq!(
        harness.live_regions(),
        0,
        "the warn claimed a background deregistration that never reached ucx"
    );
    harness.transport.shutdown();
}

/// Without a UCX transport the facade refuses rather than panicking, and the
/// rest of Velo is unaffected. Adding the transport with `add_transport`
/// instead of `add_ucx_transport` is a legitimate messaging-only setup.
#[tokio::test(flavor = "multi_thread")]
async fn velo_without_ucx_transport_refuses_registration() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let transport = Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("listener")
            .build()
            .expect("build tcp transport"),
    );
    let velo = crate::Velo::builder()
        .add_transport(transport)
        .build()
        .await
        .expect("build velo");

    let err = velo
        .register_owned(vec![0u8; 4096].into_boxed_slice())
        .await
        .expect_err("registration must be refused without a ucx transport");
    assert_eq!(
        err.cause,
        RdmaError::NotConfigured,
        "a missing backend is a permanent configuration fact, not a retryable backend error"
    );
    assert!(
        err.buffer.is_some(),
        "the caller buffer must come back from a refusal"
    );
    assert_eq!(velo.rdma_registered_bytes(), 0);

    // And shutdown still works, with no registry to sweep.
    velo.graceful_shutdown(velo_ext::ShutdownPolicy::Timeout(T))
        .await;
}

// ---------------------------------------------------------------------------
// Budget accounting under cancellation and awkward sizes
// ---------------------------------------------------------------------------

/// A registration whose future is dropped at the map must give its budget claim
/// back.
///
/// Wrapping a registration in a `timeout` is an ordinary thing for a caller to
/// write, and a claim released only on the error arm survives it: the arm never
/// runs. The failure is silent and permanent — enough cancellations and every
/// later registration answers `BudgetExceeded` for the life of the process,
/// which Phase 3 reads as "stage chunked" and never reports as broken.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_registration_returns_its_budget() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    *backend.map_delay.lock() = Some(Duration::from_millis(500));

    for _ in 0..4 {
        let attempt = registry.register_owned(vec![0u8; 64 * 1024].into_boxed_slice());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), attempt)
                .await
                .is_err(),
            "the registration was supposed to be cancelled mid-map"
        );
    }

    assert_eq!(
        registry.registered_bytes(),
        0,
        "cancelled registrations leaked their budget claim"
    );

    // And the budget is genuinely usable again, not merely reported as zero.
    *backend.map_delay.lock() = None;
    let guard = registry
        .register_owned(vec![0u8; 64 * 1024].into_boxed_slice())
        .await
        .expect("a registration after cancellations must still be admitted");
    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained
    );
    assert_eq!(registry.registered_bytes(), 0);
}

/// The same property for the pool path, which claims its budget in
/// `map_arena`.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_pool_alloc_returns_its_budget() {
    let cfg = RdmaConfig {
        pool: small_pool_config(),
        ..RdmaConfig::default()
    };
    let (backend, registry) = mock_registry(cfg);
    *backend.map_delay.lock() = Some(Duration::from_millis(500));

    for _ in 0..4 {
        let attempt = registry.alloc_pinned(4096);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), attempt)
                .await
                .is_err(),
            "the allocation was supposed to be cancelled mid-map"
        );
    }

    assert_eq!(
        registry.registered_bytes(),
        0,
        "cancelled pool allocations leaked their budget claim"
    );
    assert_eq!(
        registry.pool().arena_count(),
        0,
        "a cancelled map left an arena in the set"
    );

    *backend.map_delay.lock() = None;
    let buf = registry
        .alloc_pinned(4096)
        .await
        .expect("the pool must still be usable after cancellations");
    drop(buf);
}

/// Arena sizes that are not granule multiples must still balance.
///
/// `initial_arena_bytes` is a public field wired through
/// `VeloBuilder::rdma_config`, so nothing stops a caller passing 100_000. If
/// the claim were taken on the requested size and the release on the
/// page-rounded one, every arena would under-release by the difference; the
/// counter is unsigned, so the drift accumulates until the pool refuses
/// everything, and `publish` saturates so the gauge would keep reading zero.
/// Every other test in this file uses granule multiples, which is exactly why
/// this one does not.
#[tokio::test]
async fn unaligned_arena_sizes_balance_the_budget() {
    let cfg = RdmaConfig {
        pool: RdmaPoolConfig {
            initial_arena_bytes: 100_000,
            max_arena_bytes: 300_000,
            dedicated_arena_min: 1 << 30,
            registered_bytes_budget: 4_000_000,
        },
        ..RdmaConfig::default()
    };
    let (_backend, registry) = mock_registry(cfg);

    let mut held = Vec::new();
    for _ in 0..6 {
        held.push(registry.alloc_pinned(30_000).await.expect("alloc"));
    }
    let registered = registry.registered_bytes();
    assert!(registered > 0, "nothing was accounted as registered");
    assert_eq!(
        registered % GRANULE as u64,
        0,
        "the budget claim must be the page-rounded length that is actually mapped"
    );

    drop(held);
    registry.shutdown(T).await;
    assert_eq!(
        registry.registered_bytes(),
        0,
        "reserve and release disagreed on the length; the budget is now permanently skewed"
    );
}

/// The budget ceiling holds under concurrent pressure.
///
/// The CAS loop in `try_reserve` is the only thing standing between N tasks
/// each reading an under-budget total and each concluding it has room. A plain
/// "read, compare, add" passes every sequential test in this file.
///
/// The assertion is on **how many registrations were admitted**, not on the
/// counter. A lost update makes the counter under-report, so a bare
/// "counter stays under the ceiling" check passes with the bug in place — the
/// counter is exactly the thing the bug corrupts. What cannot be faked is the
/// number of registrations simultaneously alive: every admitted registration
/// pins at least its requested length, so admitting more than the ceiling
/// allows means the ceiling did not hold, whatever the counter says.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_registration_never_overshoots_the_budget() {
    const TASKS: usize = 24;
    const ROUNDS: usize = 8;
    const CHUNK: usize = 16 * GRANULE;
    /// Room for eight concurrent registrations, so tasks genuinely race at the
    /// ceiling rather than all fitting or all being refused.
    const BUDGET: u64 = (8 * 16 * GRANULE) as u64;

    let cfg = RdmaConfig {
        pool: RdmaPoolConfig {
            registered_bytes_budget: BUDGET,
            ..small_pool_config()
        },
        ..RdmaConfig::default()
    };
    let (_backend, registry) = mock_registry(cfg);
    let admitted = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let registry = Arc::clone(&registry);
        let admitted = Arc::clone(&admitted);
        tasks.push(tokio::spawn(async move {
            for _ in 0..ROUNDS {
                // Guards are kept, never released, so the peak is the total.
                if let Ok(guard) = registry
                    .register_owned(vec![0u8; CHUNK].into_boxed_slice())
                    .await
                {
                    admitted.lock().push(guard);
                }
                tokio::task::yield_now().await;
            }
        }));
    }
    for task in tasks {
        task.await.expect("task panicked");
    }

    let live = admitted.lock().len();
    assert!(
        live > 0,
        "nothing was admitted at all; the test proves nothing"
    );
    assert!(
        (live * CHUNK) as u64 <= BUDGET,
        "{live} concurrent registrations of {CHUNK} B were admitted against a {BUDGET} B \
         budget: the ceiling did not hold under concurrency"
    );

    admitted.lock().clear();
    registry.shutdown(T).await;
    assert_eq!(registry.registered_bytes(), 0, "the budget did not balance");
}

/// An unaligned, non-granule external registration charges the enclosing pages.
///
/// The budget exists to be the `RLIMIT_MEMLOCK` valve, so it has to count what
/// the kernel pins. A 4097-byte buffer at an arbitrary heap address spans three
/// pages; charging its requested length would undercount by most of a factor of
/// two, and the ceiling would let through roughly twice the memory the operator
/// asked it to allow.
#[tokio::test]
async fn external_registration_charges_page_enclosing_bytes() {
    let (_backend, registry) = mock_registry(RdmaConfig::default());

    // Deliberately odd, and deliberately from the heap so it is not page-aligned.
    let leaked: &'static mut [u8] = Box::leak(vec![0u8; 4097].into_boxed_slice());
    let ptr = std::ptr::NonNull::new(leaked.as_mut_ptr()).expect("non-null");

    // SAFETY: a leaked allocation is never freed, so it outlives the
    // registration unconditionally.
    let guard = unsafe { registry.register_external(ptr, leaked.len()) }
        .await
        .expect("register");

    let charged = registry.registered_bytes();
    assert!(
        charged >= 4097,
        "the charge must cover at least the requested range: {charged}"
    );
    assert_eq!(
        charged % GRANULE as u64,
        0,
        "the charge must be a whole number of pages: {charged}"
    );

    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained
    );
    assert_eq!(
        registry.registered_bytes(),
        0,
        "reserve and release disagreed; the budget is now permanently skewed"
    );
}

/// `deregistered()` resolves at the end of velo shutdown even when the unmap
/// itself was never confirmed.
///
/// This is what makes the future a signal a caller can actually wait on. A
/// backend that answers `ShuttingDown` — a transport already going down, a
/// wedged progress thread — leaves the sweep unable to latch honestly; but by
/// the time `graceful_shutdown` returns, transport teardown has force-unmapped
/// everything, so the region really is released. Without the final latch the
/// future would stay pending forever and a caller waiting on it before freeing
/// would wait for the life of the process.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_latches_regions_whose_unmap_was_never_confirmed() {
    let (backend, registry) = mock_registry(RdmaConfig::default());
    let guard = registry
        .register_owned(vec![0u8; 8192].into_boxed_slice())
        .await
        .expect("register");
    let watch = guard.watch();

    // The sweep will not be able to confirm anything.
    backend.refuse_unmap.store(true, Ordering::SeqCst);
    registry.shutdown(Duration::from_millis(200)).await;
    assert!(
        !watch.is_deregistered(),
        "an unconfirmed unmap must not latch during the sweep; that is the point of the sweep \
         being honest about what it knows"
    );

    // Standing in for the transport teardown that force-unmaps everything.
    registry.latch_all_deregistered();

    assert!(
        watch.is_deregistered(),
        "the end of velo shutdown must resolve every surviving latch"
    );
    tokio::time::timeout(T, watch.deregistered())
        .await
        .expect("deregistered() must resolve once shutdown has completed");
    assert_eq!(
        registry.registered_bytes(),
        0,
        "regions released at the end of shutdown must give their budget back"
    );
    drop(guard);
}

/// The owned buffer survives a runtime abandoned without `shutdown`.
///
/// Plain drop glue on `RegionInner` would free the `Box` here while the backend
/// still had the pages mapped, and a peer holding the key then reads or writes
/// freed heap. The leak is the correct outcome.
///
/// Reproducing it faithfully needs the *last* `Arc<RegionInner>` to go while
/// the registration is unconfirmed. Dropping the guard spawns a background
/// deregistration that holds one, so the scenario is a runtime that dies before
/// that task ever runs — a panicking process, a `Runtime` dropped out from
/// under its tasks. Dropping the runtime cancels the task, releases the last
/// reference, and runs the destructor under test.
///
/// The assertion is on the recorded decision rather than on the memory: proving
/// a `Box` was not freed by reading it is the very use-after-free being
/// prevented, so only Miri or ASan could see it directly.
#[test]
fn abandoned_runtime_leaks_owned_buffers_rather_than_freeing_them() {
    let before = super::region::LEAKED_BUFFERS.load(Ordering::SeqCst);
    let backend = MockBackend::new();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on({
        let backend = Arc::clone(&backend);
        async move {
            let registry = Arc::new(RdmaRegistry::new(
                backend as Arc<dyn RdmaBackend>,
                RdmaConfig::default(),
                tokio::runtime::Handle::current(),
                None,
            ));
            let guard = registry
                .register_owned(vec![0xC5u8; 8192].into_boxed_slice())
                .await
                .expect("register");
            // The background deregistration is spawned and never polled.
            drop(guard);
            drop(registry);
        }
    });
    // Cancels the pending deregistration, releasing the last reference.
    drop(runtime);

    assert_eq!(
        backend.unmap_calls(),
        0,
        "the deregistration was supposed to never run; the scenario is not what it claims"
    );
    assert_eq!(
        backend.live(),
        1,
        "the backend still holds the registration, so the pages are still pinned"
    );
    assert!(
        super::region::LEAKED_BUFFERS.load(Ordering::SeqCst) > before,
        "the owned buffer was freed while its pages were still pinned"
    );
}

/// `wait_deregistered` must not lose the wakeup when the latch closes while it
/// is between reading the flag and parking.
///
/// `notify_waiters()` stores no permit, so a `Notified` created *after* the
/// latch never hears it — the future would hang forever on a region that is
/// already released, and a caller waiting before freeing would wait for the
/// life of the process. The fix is to create the future before reading the
/// flag; this scans the window rather than hoping to hit it, mirroring
/// `velo_ext`'s `wait_for_drain_survives_guard_dropped_at_the_check`, whose
/// discipline the implementation cites.
///
/// A lost wakeup is permanent, so the per-iteration bound is short and the
/// first hit fails. It is a detector, not a deadline: a runner that fails to
/// schedule the latcher inside it looks identical from here, so the latcher is
/// joined — making the latch a fact — and the wait re-awaited under a generous
/// grace window, which costs no detection power.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_deregistered_survives_a_latch_at_the_check() {
    const ITERATIONS: usize = 4096;
    const SPIN_SWEEP: usize = 512;
    const WAITER_LEAD: usize = 600;
    const GRACE: Duration = Duration::from_secs(2);

    // `black_box` keeps LLVM from folding the busy-wait away and deleting the
    // delay the scan depends on.
    fn burn(rounds: usize) {
        let mut sink = 0usize;
        for k in 0..rounds {
            sink = std::hint::black_box(sink.wrapping_add(k));
        }
    }

    let (_backend, registry) = mock_registry(RdmaConfig::default());

    for iteration in 0..ITERATIONS {
        let guard = registry
            .register_owned(vec![0u8; GRANULE].into_boxed_slice())
            .await
            .expect("register");
        let watch = guard.watch();
        let inner = guard.watch();

        let armed = Arc::new(AtomicBool::new(false));
        let spins = iteration % SPIN_SWEEP;

        let latcher_armed = Arc::clone(&armed);
        let latcher = std::thread::spawn(move || {
            while !latcher_armed.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            burn(spins);
            inner.latch_for_test();
        });

        let mut waiter = tokio::spawn(async move {
            armed.store(true, Ordering::Release);
            burn(WAITER_LEAD);
            watch.deregistered().await;
        });

        let finished = tokio::time::timeout(Duration::from_millis(200), &mut waiter).await;
        latcher.join().expect("latcher thread panicked");
        let joined = match finished {
            Ok(joined) => joined,
            Err(_) => tokio::time::timeout(GRACE, &mut waiter)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "wait_deregistered lost the latch wakeup (iteration {iteration}, \
                         spins {spins})"
                    )
                }),
        };
        joined.expect("waiter task panicked");
        std::mem::forget(guard);
    }
    registry.shutdown(T).await;
}

/// A dedicated arena is not reused after its buffer is dropped.
///
/// The other direction of `pool_dedicates_an_arena_to_oversize_requests`: not
/// only is a dedicated arena kept out of the general search, it is also not
/// recycled for the next oversize request. That is Phase 4 work, and pinning it
/// here means the behaviour change lands as a failing test rather than as a
/// silent improvement nobody notices — and documents why
/// `dedicated_arena_min` carries the warning it does.
#[tokio::test]
async fn dedicated_arenas_are_not_reused() {
    let cfg = small_pool_config();
    let (backend, pool) = mock_pool(cfg.clone());
    let size = cfg.dedicated_arena_min as usize;

    let first = pool.alloc(size).await.expect("first oversize");
    assert_eq!(pool.arena_count(), 1);
    let first_arena = backend.live();
    drop(first);
    assert_eq!(
        pool.live_allocations(),
        0,
        "the suballocation was returned to its arena"
    );
    assert_eq!(
        backend.live(),
        first_arena,
        "dropping a PinnedBuf must never unmap its arena"
    );

    let second = pool.alloc(size).await.expect("second oversize");
    assert_eq!(
        pool.arena_count(),
        2,
        "the freed dedicated arena was reused; Phase 4 reclamation has landed and \
         `dedicated_arena_min` docs need updating"
    );
    assert_eq!(
        backend.live(),
        2,
        "each oversize request maps its own arena"
    );
    drop(second);
}
