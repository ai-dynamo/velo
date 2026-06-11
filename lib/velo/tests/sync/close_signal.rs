// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`velo::sync::CloseSignal`] +
//! [`velo::sync::PendingMap`] composition.
//!
//! Test IDs covered:
//! - TEST-SYNC-HOOK

use std::time::Duration;

use velo::sync::{CloseSignal, PendingMap};

/// TEST-SYNC-HOOK — substrate proof of `CloseSignal` + `PendingMap`
/// composition.
///
/// Wires a `PendingMap` drain into a `CloseSignal` subscriber:
///
/// ```text
/// signal.on_close(move |r| { pending.close(r.clone()); });
/// ```
///
/// Parks `M` waiters.  Fires `signal.close("boom")`.
///
/// Assertions:
/// - All `M` waiters yield `Err(Closed)` carrying `"boom"`.
/// - The `CancellationToken` is cancelled after `close()`.
/// - **Drain-before-cancel ordering**: a task parked on `signal.cancelled()`
///   that inspects the map immediately after waking observes
///   `is_closed() == true` and `len() == 0`.  This works because
///   `CloseSignal::close()` fires subscribers in step 2 (which drains the
///   `PendingMap`) and cancels the token in step 3 (which wakes async awaiters)
///   — so by the time the parked task runs, the map is already drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sync_hook() {
    const M: usize = 50;

    let signal = CloseSignal::new();
    let pending: PendingMap<usize, ()> = PendingMap::new();

    // Wire signal → PendingMap drain via on_close subscriber.
    signal.on_close({
        let p = pending.clone();
        move |r| {
            p.close(r.clone());
        }
    });

    // Park M waiters.
    let mut waiters = Vec::with_capacity(M);
    for i in 0..M {
        waiters.push(
            pending
                .register(i)
                .unwrap_or_else(|e| panic!("TEST-SYNC-HOOK: register({i}) failed: {e}")),
        );
    }

    // Spawn a task that parks on the cancellation token, then verifies the
    // drain-before-cancel ordering contract.
    //
    // CloseSignal::close() ordering:
    //   Step 2 → subscribers fire (our on_close drains the PendingMap).
    //   Step 3 → token cancelled (wakes this task).
    //
    // So when this task wakes, the PendingMap is already drained.
    let p2 = pending.clone();
    let s2 = signal.clone();
    let ordering_handle = tokio::spawn(async move {
        s2.cancelled().await;
        assert!(
            p2.is_closed(),
            "TEST-SYNC-HOOK: map must be closed when token fires \
             (drain-before-cancel ordering violated)"
        );
        assert_eq!(
            p2.len(),
            0,
            "TEST-SYNC-HOOK: map must be empty when token fires \
             (drain-before-cancel ordering violated)"
        );
    });

    // Fire the signal.
    let won = signal.close("boom");
    assert!(won, "TEST-SYNC-HOOK: first close() must return true");

    // Token must be cancelled synchronously as part of close().
    assert!(
        signal.closed().is_cancelled(),
        "TEST-SYNC-HOOK: token must be cancelled after close()"
    );

    // All M waiters must resolve with Err(Closed { reason: "boom" }); none may strand.
    for (i, waiter) in waiters.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .unwrap_or_else(|_| panic!("TEST-SYNC-HOOK: waiter {i} timed out — stranded"));

        match result {
            Err(closed) => assert_eq!(
                closed.reason(),
                "boom",
                "TEST-SYNC-HOOK: waiter {i} carries wrong reason (expected \"boom\")"
            ),
            Ok(_) => panic!("TEST-SYNC-HOOK: waiter {i} resolved Ok — expected Err(Closed)"),
        }
    }

    ordering_handle.await.unwrap();
}
