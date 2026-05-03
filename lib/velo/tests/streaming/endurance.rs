// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(velo_endurance)]

//! Endurance and fault-injection tests for velo-streaming.
//!
//! Gated behind the `velo_endurance` rustc cfg (NOT a Cargo feature), so CI's
//! `cargo test --all-features --all-targets` never compiles or runs them.
//! Opt in locally with:
//!
//! ```text
//! RUSTFLAGS="--cfg velo_endurance" cargo test -p velo-streaming --test endurance
//! ```
//!
//! Test inventory
//! ─────────────
//! E-01  sustained_10k_frames         — 10 000-frame single-stream, zero loss
//! E-02  concurrent_100_streams       — 100 anchors × 100 frames, no corruption
//! E-03  rapid_create_destroy_anchors — 1 000 create/finalize cycles, no leak
//! E-04  sender_panic_drop            — sender task panics; Dropped sentinel arrives
//! E-05  consumer_slow_sustained      — 1 000 items, 1 ms delay per item; zero loss
//! E-06  five_cycle_reattach_endurance — 5 detach/re-attach × 100 items; 500 total
//! E-07  cancel_storm                 — 50 concurrent cancel() calls; idempotent

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use velo::streaming::{AnchorManager, StreamController, StreamFrame};
use velo_ext::WorkerId;

use common::MockFrameTransport;

fn make_manager() -> Arc<AnchorManager> {
    Arc::new(AnchorManager::new(
        WorkerId::from_u64(1),
        Arc::new(MockFrameTransport::new()),
    ))
}

// ============================================================================
// E-01: sustained_10k_frames
// ============================================================================

/// Send 10 000 u64 frames on a single anchor as fast as possible.
/// Verify zero frame loss and strict ordering.
#[tokio::test(flavor = "multi_thread")]
async fn test_e01_sustained_10k_frames() {
    const TOTAL: u64 = 10_000;
    let mgr = make_manager();
    let mut anchor = mgr.create_anchor::<u64>();
    let handle = anchor.handle();
    let sender = mgr
        .attach_stream_anchor::<u64>(handle)
        .await
        .expect("attach");

    let send_task = tokio::spawn(async move {
        for i in 0..TOTAL {
            sender.send(i).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });

    let mut received: Vec<u64> = Vec::with_capacity(TOTAL as usize);
    while let Some(frame) = anchor.next().await {
        match frame {
            Ok(StreamFrame::Item(v)) => received.push(v),
            Ok(StreamFrame::Finalized) => break,
            other => panic!("unexpected: {other:?}"),
        }
    }
    send_task.await.expect("send task");

    assert_eq!(received.len(), TOTAL as usize, "zero frame loss");
    assert_eq!(
        received,
        (0..TOTAL).collect::<Vec<_>>(),
        "frames must be in order"
    );
    assert!(anchor.next().await.is_none());
}

// ============================================================================
// E-02: concurrent_100_streams
// ============================================================================

/// 100 anchors × 100 frames concurrently. Verify all 10 000 frames arrive,
/// each anchor receives exactly its own items, registry is clean at the end.
#[tokio::test(flavor = "multi_thread")]
async fn test_e02_concurrent_100_streams() {
    const STREAMS: usize = 100;
    const ITEMS: u32 = 100;

    let mgr = make_manager();

    // Create all anchors + attach all senders
    let mut anchors: Vec<_> = Vec::new();
    let mut senders: Vec<_> = Vec::new();
    for _ in 0..STREAMS {
        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();
        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("attach");
        anchors.push(anchor);
        senders.push(sender);
    }

    // Spawn one task per sender
    let send_tasks: Vec<_> = senders
        .into_iter()
        .enumerate()
        .map(|(idx, sender)| {
            tokio::spawn(async move {
                let base = idx as u32 * 1000;
                for j in 0..ITEMS {
                    sender.send(base + j).await.expect("send");
                }
                sender.finalize().expect("finalize");
            })
        })
        .collect();

    // Spawn one task per anchor and collect
    let recv_tasks: Vec<_> = anchors
        .into_iter()
        .enumerate()
        .map(|(idx, mut anchor)| {
            tokio::spawn(async move {
                let base = idx as u32 * 1000;
                let mut got: Vec<u32> = Vec::new();
                while let Some(frame) = anchor.next().await {
                    match frame {
                        Ok(StreamFrame::Item(v)) => got.push(v),
                        Ok(StreamFrame::Finalized) => break,
                        other => panic!("stream {idx}: unexpected {other:?}"),
                    }
                }
                (idx, got, base)
            })
        })
        .collect();

    // Wait for all senders
    for t in send_tasks {
        t.await.expect("send task");
    }

    // Validate all receivers
    for t in recv_tasks {
        let (idx, got, base) = t.await.expect("recv task");
        let expected: Vec<u32> = (0..ITEMS).map(|j| base + j).collect();
        assert_eq!(got, expected, "stream {idx}: received wrong items");
    }

    // Registry must be empty
    assert_eq!(
        mgr.active_anchor_count(),
        0,
        "all anchors must be removed after finalize"
    );
}

// ============================================================================
// E-03: rapid_create_destroy_anchors
// ============================================================================

/// Create and finalize 1 000 anchors sequentially.
/// After each finalize the registry count must return to 0 (no anchor leak).
#[tokio::test(flavor = "multi_thread")]
async fn test_e03_rapid_create_destroy_anchors() {
    const ROUNDS: usize = 1_000;
    let mgr = make_manager();

    for round in 0..ROUNDS {
        let mut anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();
        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .unwrap_or_else(|e| panic!("round {round} attach: {e}"));
        sender
            .finalize()
            .unwrap_or_else(|e| panic!("round {round} finalize: {e}"));
        // Drain Finalized sentinel
        while let Some(f) = anchor.next().await {
            if matches!(f, Ok(StreamFrame::Finalized)) {
                break;
            }
        }
        assert!(anchor.next().await.is_none());
        assert_eq!(
            mgr.active_anchor_count(),
            0,
            "round {round}: registry must be empty after finalize"
        );
    }
}

// ============================================================================
// E-04: sender_panic_drop
// ============================================================================

/// Spawn a sender task that panics after sending 500 items.
/// The `impl Drop` on `StreamSender` sends the Dropped sentinel synchronously,
/// even when the thread is unwinding due to a panic.
/// The consumer must receive all 500 items followed by Err(SenderDropped).
#[tokio::test(flavor = "multi_thread")]
async fn test_e04_sender_panic_drop() {
    use velo::streaming::StreamError;

    let mgr = make_manager();
    let mut anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();
    let sender = mgr
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("attach");

    // Spawn a task that sends 500 items then panics.
    // `catch_unwind` keeps the test runtime alive; the sender's Drop fires on panic.
    let send_task = tokio::spawn(async move {
        for i in 0u32..500 {
            sender.send(i).await.expect("send");
        }
        // Panic without explicit finalize/detach — Drop sends Dropped.
        panic!("deliberate sender panic for E-04");
    });

    // Collect everything until stream closes
    let mut items: Vec<u32> = Vec::new();
    let mut saw_dropped = false;
    while let Some(frame) = anchor.next().await {
        match frame {
            Ok(StreamFrame::Item(v)) => items.push(v),
            Err(StreamError::SenderDropped) => {
                saw_dropped = true;
                break;
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // The spawn task will have panicked — just ignore the join error
    let _ = send_task.await;

    assert_eq!(items.len(), 500, "must receive all 500 items before panic");
    assert!(saw_dropped, "Dropped sentinel must arrive after panic-drop");
    assert!(anchor.next().await.is_none());
}

// ============================================================================
// E-05: consumer_slow_sustained
// ============================================================================

/// Send 1 000 items with the consumer sleeping 1 ms per item.
/// The sender will block on the bounded channel (capacity 256).
/// All 1 000 items must arrive; no ChannelClosed or frame loss.
#[tokio::test(flavor = "multi_thread")]
async fn test_e05_consumer_slow_sustained() {
    const TOTAL: u32 = 1_000;

    let mgr = make_manager();
    let mut anchor = mgr.create_anchor::<u32>();
    let handle = anchor.handle();
    let sender = mgr
        .attach_stream_anchor::<u32>(handle)
        .await
        .expect("attach");

    let send_task = tokio::spawn(async move {
        for i in 0..TOTAL {
            sender.send(i).await.expect("send");
        }
        sender.finalize().expect("finalize");
    });

    let mut items: Vec<u32> = Vec::new();
    while let Some(frame) = anchor.next().await {
        match frame {
            Ok(StreamFrame::Item(v)) => {
                items.push(v);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(StreamFrame::Finalized) => break,
            other => panic!("unexpected: {other:?}"),
        }
    }

    send_task.await.expect("send task");
    assert_eq!(
        items.len(),
        TOTAL as usize,
        "zero frame loss under slow consumer"
    );
    assert_eq!(items, (0..TOTAL).collect::<Vec<_>>(), "items in order");
    assert!(anchor.next().await.is_none());
}

// ============================================================================
// E-06: five_cycle_reattach_endurance
// ============================================================================

/// 5 detach/re-attach cycles × 100 items per batch = 500 total items.
/// All 500 items must arrive in strict order across all 6 attach sessions
/// (5 detach + 1 final finalize), with exactly 5 Detached sentinels between batches.
#[tokio::test(flavor = "multi_thread")]
async fn test_e06_five_cycle_reattach_endurance() {
    const CYCLES: u32 = 5;
    const BATCH: u32 = 100;

    let mgr = make_manager();
    let mut anchor = mgr.create_anchor::<u32>();
    let mut current_handle = anchor.handle();
    let mut all_items: Vec<u32> = Vec::new();
    let mut detached_count: u32 = 0;
    let mut offset: u32 = 0;

    for cycle in 0..CYCLES {
        let sender = mgr
            .attach_stream_anchor::<u32>(current_handle)
            .await
            .unwrap_or_else(|e| panic!("cycle {cycle} attach: {e}"));
        for j in 0..BATCH {
            sender.send(offset + j).await.expect("send");
        }
        offset += BATCH;
        current_handle = sender.detach().expect("detach");

        loop {
            let f = tokio::time::timeout(Duration::from_secs(10), anchor.next())
                .await
                .expect("timeout")
                .expect("stream open");
            match f {
                Ok(StreamFrame::Item(v)) => all_items.push(v),
                Ok(StreamFrame::Detached) => {
                    detached_count += 1;
                    break;
                }
                other => panic!("cycle {cycle}: unexpected {other:?}"),
            }
        }
    }

    // Final attach + finalize
    let last_sender = mgr
        .attach_stream_anchor::<u32>(current_handle)
        .await
        .expect("final attach");
    for j in 0..BATCH {
        last_sender.send(offset + j).await.expect("send");
    }
    last_sender.finalize().expect("finalize");

    loop {
        let f = tokio::time::timeout(Duration::from_secs(10), anchor.next())
            .await
            .expect("timeout")
            .expect("stream open");
        match f {
            Ok(StreamFrame::Item(v)) => all_items.push(v),
            Ok(StreamFrame::Finalized) => break,
            other => panic!("final batch: unexpected {other:?}"),
        }
    }
    assert!(anchor.next().await.is_none());

    let total = (CYCLES + 1) * BATCH;
    assert_eq!(
        all_items.len(),
        total as usize,
        "must receive all {total} items"
    );
    assert_eq!(
        all_items,
        (0..total).collect::<Vec<_>>(),
        "items must be in strict order"
    );
    assert_eq!(
        detached_count, CYCLES,
        "must see exactly {CYCLES} Detached sentinels"
    );
}

// ============================================================================
// E-07: cancel_storm
// ============================================================================

/// 50 goroutines call `StreamController::cancel()` concurrently.
/// Only one should remove the anchor from the registry (AtomicBool gate).
/// The rest should be no-ops. The final anchor count must be 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_e07_cancel_storm() {
    const STORMERS: usize = 50;

    let mgr = make_manager();
    let anchor = mgr.create_anchor::<u32>();
    let ctrl: StreamController = anchor.controller();

    // Spawn all cancellers concurrently — all have a clone of the same controller
    let tasks: Vec<_> = (0..STORMERS)
        .map(|_| {
            let c = ctrl.clone();
            tokio::spawn(async move { c.cancel() })
        })
        .collect();

    for t in tasks {
        t.await.expect("task");
    }

    // Registry must be empty — exactly one cancel() removes the entry
    assert_eq!(
        mgr.active_anchor_count(),
        0,
        "registry must be empty after cancel storm"
    );
    // The anchor was consumed by .controller() — just verify the count is 0
}
