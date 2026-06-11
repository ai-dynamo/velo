// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`velo::sync::PendingMap`].
//!
//! Test IDs covered:
//! - TEST-SYNC-RACE
//! - TEST-SYNC-INTERLEAVE-A
//! - TEST-SYNC-INTERLEAVE-B
//! - TEST-SYNC-RESOLVE-VS-CLOSE
//! - TEST-SYNC-OFF-RUNTIME

use std::sync::{Arc, Barrier};
use std::time::Duration;

use velo::sync::{PendingMap, RegisterError};

/// TEST-SYNC-RACE
///
/// 8-worker-thread runtime; 200 tasks each call `register(id)` then
/// `timeout(100 ms, waiter)`.  One task calls `close("torn down")` after a few
/// `yield_now` points.
///
/// INVARIANT: zero timeouts.  Every `register()` either returns `Ok(waiter)`
/// that resolves within 100 ms (`Ok(())` or `Err(Closed)`) or returns
/// `Err(Closed)` synchronously.
///
/// NOTE ON COVERAGE: This test *stochastically* samples the register-vs-close
/// race by interleaving tasks via `yield_now`; it does not deterministically
/// explore every possible scheduling interleaving.  A reintroduced two-lock
/// design (sharded map + separate closed flag) might pass this test by chance.
/// The structural guarantee — that an entry can never be inserted into an
/// already-closed map — comes from the single `parking_lot::Mutex` that
/// serializes `register` and `close` in production code, not from this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sync_race() {
    const N: usize = 200;
    let pending: PendingMap<usize, ()> = PendingMap::new();

    let mut handles = Vec::with_capacity(N + 1);

    // Close task: yield a few times to let worker tasks interleave, then close.
    {
        let p = pending.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            p.close("torn down");
        }));
    }

    // Worker tasks: register then timeout-await.
    for i in 0..N {
        let p = pending.clone();
        handles.push(tokio::spawn(async move {
            match p.register(i) {
                Ok(waiter) => {
                    // Waiter MUST resolve within 100 ms — either Ok(()) or Err(Closed).
                    // A timeout here means an entry was stranded in the map with no one
                    // left to drain it, which would violate the single-lock invariant.
                    // The inner Ok/Err value is intentionally discarded: the invariant
                    // under test is "no timeout", not which branch the waiter took.
                    let _ = tokio::time::timeout(Duration::from_millis(100), waiter)
                        .await
                        .unwrap_or_else(|_| {
                            panic!(
                                "TEST-SYNC-RACE: waiter for key {i} timed out — \
                                 structural invariant violated (entry stranded in closed map)"
                            )
                        });
                }
                // register() returned Err synchronously — no waiter was created.
                Err(RegisterError::Closed(_)) => {}
                // Unique keys (0..N) per task — Occupied is impossible.
                Err(e) => panic!("TEST-SYNC-RACE: unexpected register error for key {i}: {e}"),
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

/// TEST-SYNC-INTERLEAVE-A
///
/// `std::sync::Barrier` (count = 2) pins `register()` strictly before
/// `close()`.  The close thread spawns first but blocks at the barrier;
/// `register()` runs in the calling thread; the barrier is released after
/// registration, allowing close to proceed.  The waiter must yield
/// `Err(Closed)` carrying the close reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sync_interleave_a() {
    let barrier = Arc::new(Barrier::new(2));
    let pending: PendingMap<u32, ()> = PendingMap::new();

    // Close thread blocks at the barrier until register() completes.
    let p = pending.clone();
    let b_close = barrier.clone();
    let jh = std::thread::spawn(move || {
        b_close.wait(); // blocked until the register side releases the barrier
        p.close("interleave-a");
    });

    // register() runs here — before we release the barrier.
    let waiter = pending
        .register(1)
        .expect("TEST-SYNC-INTERLEAVE-A: register must succeed before close");

    // Release: register is done; close thread may now proceed.
    // Brief blocking of one tokio worker thread is acceptable given worker_threads = 4.
    barrier.wait();

    let result = tokio::time::timeout(Duration::from_millis(500), waiter)
        .await
        .expect("TEST-SYNC-INTERLEAVE-A: waiter must not timeout after close");

    jh.join().unwrap();

    assert_eq!(
        result.unwrap_err().reason(),
        "interleave-a",
        "TEST-SYNC-INTERLEAVE-A: waiter must carry the close reason"
    );
}

/// TEST-SYNC-INTERLEAVE-B
///
/// `close()` completes before `register()` is called.  `register()` must
/// return `Err(RegisterError::Closed)` synchronously and `len()` must be 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sync_interleave_b() {
    let pending: PendingMap<u32, ()> = PendingMap::new();

    // Close first.
    pending.close("already closed");

    // register after close must return Err(RegisterError::Closed) synchronously.
    match pending.register(99) {
        Err(RegisterError::Closed(c)) => {
            assert_eq!(
                c.reason(),
                "already closed",
                "TEST-SYNC-INTERLEAVE-B: Closed reason must match"
            );
        }
        Ok(_) => panic!("TEST-SYNC-INTERLEAVE-B: expected Err(Closed), got Ok"),
        Err(e) => panic!("TEST-SYNC-INTERLEAVE-B: expected Err(Closed), got other error: {e}"),
    }

    assert_eq!(
        pending.len(),
        0,
        "TEST-SYNC-INTERLEAVE-B: len must be 0 after close"
    );
}

/// TEST-SYNC-RESOLVE-VS-CLOSE
///
/// 100 keys registered; 50 resolver tasks and one close task race against each
/// other.  Each waiter sees exactly one of `Ok(v)` / `Err(Closed)` — none
/// strand.  Every waiter resolves within a 500 ms timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sync_resolve_vs_close() {
    const N: usize = 100;
    let pending: PendingMap<usize, usize> = PendingMap::new();

    // Register all N keys before spawning any tasks.
    let mut waiters = Vec::with_capacity(N);
    for i in 0..N {
        waiters.push(
            pending.register(i).unwrap_or_else(|e| {
                panic!("TEST-SYNC-RESOLVE-VS-CLOSE: register({i}) failed: {e}")
            }),
        );
    }

    // Spawn N/2 resolver tasks, one per key in 0..N/2.
    let mut resolve_handles = Vec::with_capacity(N / 2);
    for i in 0..(N / 2) {
        let p = pending.clone();
        resolve_handles.push(tokio::spawn(async move {
            tokio::task::yield_now().await;
            p.resolve(&i, i * 2);
        }));
    }

    // Spawn one close task.
    let close_handle = {
        let p = pending.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            p.close("race shutdown");
        })
    };

    // Collect waiter results; none may strand.
    let mut ok_count = 0usize;
    let mut closed_count = 0usize;
    for (i, waiter) in waiters.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "TEST-SYNC-RESOLVE-VS-CLOSE: waiter {i} timed out — \
                     entry was stranded with no resolver or close"
                )
            });
        match result {
            Ok(_) => ok_count += 1,
            Err(_) => closed_count += 1,
        }
    }

    assert_eq!(
        ok_count + closed_count,
        N,
        "TEST-SYNC-RESOLVE-VS-CLOSE: all {N} waiters must have resolved (ok={ok_count} closed={closed_count})"
    );

    for h in resolve_handles {
        h.await.unwrap();
    }
    close_handle.await.unwrap();
}

/// TEST-SYNC-OFF-RUNTIME
///
/// Plain `#[test]` — no Tokio runtime is constructed before the key operations.
/// `register()` and `close()` are called from `std::thread`s.  The resulting
/// `Waiter` is then awaited on a runtime built afterward.
///
/// This validates that `PendingMap` is usable in environments that build a
/// runtime late (e.g., a PyO3/vLLM entry point that spawns a runtime only
/// after the Python interpreter has set up its own thread pool).
#[test]
fn test_sync_off_runtime() {
    let pending: PendingMap<u32, u32> = PendingMap::new();

    // Register from the current (non-Tokio) thread.
    let waiter = pending
        .register(1)
        .expect("TEST-SYNC-OFF-RUNTIME: register must succeed without a runtime");

    // Close from a separate std thread — still no runtime.
    let p = pending.clone();
    std::thread::spawn(move || {
        p.close("off-runtime close");
    })
    .join()
    .unwrap();

    // By the time join() returns, close() has sent Err(Closed) on the oneshot
    // channel.  The waiter is already resolved.  Building a runtime afterward
    // and awaiting should complete immediately.
    let rt =
        tokio::runtime::Runtime::new().expect("TEST-SYNC-OFF-RUNTIME: failed to build runtime");

    let result =
        rt.block_on(async move { tokio::time::timeout(Duration::from_millis(100), waiter).await });

    let inner = result
        .expect("TEST-SYNC-OFF-RUNTIME: waiter must not timeout — it was already resolved")
        .unwrap_err();

    assert_eq!(
        inner.reason(),
        "off-runtime close",
        "TEST-SYNC-OFF-RUNTIME: waiter must carry the reason set by the std thread"
    );
}
