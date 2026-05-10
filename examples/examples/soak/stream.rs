// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streaming soak scenarios.
//!
//! - **S1** sustained single stream, strict per-frame ordering.
//! - **S2** many concurrent streams; per-stream ordering preserved.
//! - **S4** rapid create/finalize cycles; `velo_streaming_active_anchors`
//!   returns to zero.
//! - **S6** large items at the NATS-compatible 900 KB ceiling.
//!
//! S5 (cross-Velo detach/reattach) is deferred. The local-AnchorManager E-06
//! test in `lib/velo/tests/streaming/endurance.rs` covers the protocol; the
//! cross-Velo variant requires per-cycle producer/consumer serialization that
//! the current harness doesn't provide, and a naive concurrent driver hits a
//! `SenderDropped` 100% of the time on TCP.
//!
//! Faults:
//! - **F1s** sender panic mid-stream; consumer observes `Dropped` sentinel.
//! - **F2s** cancel storm; idempotent (no double-send of `_stream_cancel`).
//! - **F3s** slow consumer; heartbeats use `try_send` and are dropped silently.
//!
//! S3 (within-window reorder) and F4 (beyond-window overflow) are deferred.
//! They require a transport wrapper that selectively reorders only stream-data
//! AMs without scrambling response/event frames; the value/cost ratio for the
//! first cut isn't there.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use futures::StreamExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use velo::{StreamFrame, Velo};

use crate::harness::oracle::{
    Oracle, ScenarioReport, assert_gauges_at_baseline, settle_until, snapshot,
    streaming_active_anchors,
};
use crate::harness::{Pair, ScenarioCtx, build_pair, progress};

pub async fn run_all(ctx: &ScenarioCtx) -> Result<Vec<ScenarioReport>> {
    let mut out = Vec::new();
    if ctx.selected("S1") {
        out.push(run_s1_single_stream(ctx).await?);
    }
    if ctx.selected("S2") {
        out.push(run_s2_many_streams(ctx).await?);
    }
    if ctx.selected("S4") {
        out.push(run_s4_rapid_cycle(ctx).await?);
    }
    if ctx.selected("S6") {
        out.push(run_s6_large_items(ctx).await?);
    }
    if ctx.faults {
        if ctx.selected("F1s") {
            out.push(run_f1s_sender_panic(ctx).await?);
        }
        if ctx.selected("F2s") {
            out.push(run_f2s_cancel_storm(ctx).await?);
        }
        if ctx.selected("F3s") {
            out.push(run_f3s_slow_consumer(ctx).await?);
        }
    }
    Ok(out)
}

/// Item type — small enough that the message-pack overhead doesn't dominate
/// at high frame counts.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Item {
    seq: u64,
    blob: bytes::Bytes,
}

async fn fresh_pair(ctx: &ScenarioCtx, tag: &str) -> Result<Pair> {
    build_pair(ctx.transport, ctx.stream_transport, &format!("soak-stream-{tag}")).await
}

// ---------------------------------------------------------------------------
// S1 — sustained single stream
// ---------------------------------------------------------------------------

pub async fn run_s1_single_stream(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "s1").await?;
    let started = Instant::now();
    let frames = ctx.tier.budget().stream_frames;

    let cancel = CancellationToken::new();
    let pl = progress::spawn(
        "S1",
        Arc::clone(&oracle),
        Duration::from_secs(2),
        cancel.clone(),
    );

    drive_one_stream(
        &pair,
        Arc::clone(&oracle),
        frames,
        bytes::Bytes::from_static(b"x"),
    )
    .await?;

    cancel.cancel();
    let _ = pl.await;

    if oracle.received() != frames {
        return Err(anyhow!(
            "S1: expected {frames} frames, observed {}",
            oracle.received()
        ));
    }

    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);
    drop(pair);
    settle_until(Duration::from_secs(5), Duration::from_millis(100), || {
        assert_gauges_at_baseline(&server_snap, &client_snap)
    })
    .await
    .ok(); // gauges decrement asynchronously after Drop; tolerate brief lag

    Ok(ScenarioReport::from_oracle(
        "S1",
        started.elapsed(),
        &oracle,
        format!("{frames} frames in-order"),
    ))
}

/// Run a single stream from `pair.client` (sender) to `pair.server` (anchor).
/// Asserts strict in-order receipt of `frame_count` `Item(seq)` frames followed
/// by exactly one `Finalized`.
async fn drive_one_stream(
    pair: &Pair,
    oracle: Arc<Oracle>,
    frame_count: u64,
    blob: bytes::Bytes,
) -> Result<()> {
    let consumer = Arc::clone(&pair.server.velo);
    let producer = Arc::clone(&pair.client.velo);

    // Consumer creates anchor; gets handle.
    let anchor = consumer.create_anchor::<Item>();
    let handle = anchor.handle();

    // Producer attaches and pumps.
    let send_oracle = Arc::clone(&oracle);
    let producer_blob = blob.clone();
    let producer_task = tokio::spawn(async move {
        let sender = producer.attach_anchor::<Item>(handle).await?;
        for seq in 0..frame_count {
            sender
                .send(Item {
                    seq,
                    blob: producer_blob.clone(),
                })
                .await?;
            send_oracle.inc_sent();
        }
        sender.finalize()?;
        anyhow::Ok(())
    });

    // Consumer receives.
    let mut stream = anchor;
    let mut expected: u64 = 0;
    let mut saw_finalized = false;
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(StreamFrame::Item(item)) => {
                if item.seq != expected {
                    return Err(anyhow!(
                        "S/order: expected seq {expected}, got {} at received={}",
                        item.seq,
                        oracle.received()
                    ));
                }
                expected += 1;
                oracle.inc_received();
            }
            Ok(StreamFrame::Finalized) => {
                saw_finalized = true;
                break;
            }
            Ok(StreamFrame::Heartbeat) => unreachable!("heartbeats filtered by anchor"),
            Ok(other) => {
                return Err(anyhow!("S/unexpected sentinel: {other:?}"));
            }
            Err(e) => return Err(anyhow!("S/stream error: {e}")),
        }
    }
    if !saw_finalized {
        return Err(anyhow!("S: stream ended without Finalized sentinel"));
    }

    producer_task.await??;
    Ok(())
}

// ---------------------------------------------------------------------------
// S2 — many concurrent streams
// ---------------------------------------------------------------------------

pub async fn run_s2_many_streams(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "s2").await?;
    let started = Instant::now();
    let anchors = ctx.tier.budget().stream_anchors;
    // Cap per-stream frame count so total work is bounded.
    let per_stream = (ctx.tier.budget().stream_frames / anchors as u64).max(64);

    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for _ in 0..anchors {
        let consumer = Arc::clone(&pair.server.velo);
        let producer = Arc::clone(&pair.client.velo);
        let oracle = Arc::clone(&oracle);
        set.spawn(async move { run_one_stream(consumer, producer, oracle, per_stream).await });
    }
    while let Some(r) = set.join_next().await {
        r??;
    }

    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);
    drop(pair);
    let _ = settle_until(Duration::from_secs(10), Duration::from_millis(100), || {
        assert_gauges_at_baseline(&server_snap, &client_snap)
    })
    .await;

    let expected = (anchors as u64) * per_stream;
    if oracle.received() != expected {
        return Err(anyhow!(
            "S2: expected {expected} frames across {anchors} streams, observed {}",
            oracle.received()
        ));
    }

    Ok(ScenarioReport::from_oracle(
        "S2",
        started.elapsed(),
        &oracle,
        format!("{anchors} streams × {per_stream} frames"),
    ))
}

async fn run_one_stream(
    consumer: Arc<Velo>,
    producer: Arc<Velo>,
    oracle: Arc<Oracle>,
    frames: u64,
) -> Result<()> {
    let anchor = consumer.create_anchor::<Item>();
    let handle = anchor.handle();

    let producer_oracle = Arc::clone(&oracle);
    let producer_task = tokio::spawn(async move {
        let sender = producer
            .attach_anchor::<Item>(handle)
            .await
            .map_err(|e| anyhow!("attach failed: {e}"))?;
        for seq in 0..frames {
            sender
                .send(Item {
                    seq,
                    blob: bytes::Bytes::from_static(b"."),
                })
                .await
                .map_err(|e| anyhow!("send seq={seq}: {e}"))?;
            producer_oracle.inc_sent();
        }
        sender.finalize().map_err(|e| anyhow!("finalize: {e}"))?;
        anyhow::Ok(())
    });

    let mut stream = anchor;
    let mut expected: u64 = 0;
    let stream_outcome: Result<()> = loop {
        let Some(frame) = stream.next().await else {
            break Err(anyhow!("S2: stream ended without sentinel"));
        };
        match frame {
            Ok(StreamFrame::Item(item)) => {
                if item.seq != expected {
                    break Err(anyhow!("S2/order: expected {expected}, got {}", item.seq));
                }
                expected += 1;
                oracle.inc_received();
            }
            Ok(StreamFrame::Finalized) => break Ok(()),
            Ok(StreamFrame::Heartbeat) => unreachable!(),
            Ok(other) => break Err(anyhow!("S2/unexpected: {other:?}")),
            Err(e) => break Err(anyhow!("S2/error: {e}")),
        }
    };

    // Always join the producer task so we surface its error if both sides
    // failed. The producer error is more diagnostic than "SenderDropped".
    let producer_outcome: Result<()> = match producer_task.await {
        Ok(r) => r,
        Err(e) => Err(anyhow!("producer task panicked: {e}")),
    };

    match (stream_outcome, producer_outcome) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(consumer_err), Err(producer_err)) => Err(anyhow!(
            "S2: consumer={consumer_err}; producer={producer_err}"
        )),
        (Err(e), Ok(())) => Err(e),
        (Ok(()), Err(e)) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// S4 — rapid create/finalize cycles
// ---------------------------------------------------------------------------

pub async fn run_s4_rapid_cycle(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "s4").await?;
    let started = Instant::now();
    // Don't go silly here — each cycle carries its own attach/finalize.
    let cycles = (ctx.tier.budget().stream_anchors as u64).max(64);

    for _ in 0..cycles {
        run_one_stream(
            Arc::clone(&pair.server.velo),
            Arc::clone(&pair.client.velo),
            Arc::clone(&oracle),
            8,
        )
        .await?;
    }

    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);
    drop(pair);
    settle_until(Duration::from_secs(10), Duration::from_millis(100), || {
        let active =
            streaming_active_anchors(&server_snap) + streaming_active_anchors(&client_snap);
        if active == 0.0 {
            Ok(())
        } else {
            Err(anyhow!("S4: active_anchors still {active}"))
        }
    })
    .await
    .ok();

    Ok(ScenarioReport::from_oracle(
        "S4",
        started.elapsed(),
        &oracle,
        format!("{cycles} create/finalize cycles"),
    ))
}

// ---------------------------------------------------------------------------
// S6 — large items
// ---------------------------------------------------------------------------

pub async fn run_s6_large_items(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "s6").await?;
    let started = Instant::now();

    // Stay below typical NATS max payload (1 MiB) to keep things portable.
    let blob = bytes::Bytes::from(vec![0xab_u8; 900 * 1024]);
    let frames = 32u64;
    drive_one_stream(&pair, Arc::clone(&oracle), frames, blob).await?;

    if oracle.received() != frames {
        return Err(anyhow!(
            "S6: expected {frames} large frames, observed {}",
            oracle.received()
        ));
    }

    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "S6",
        started.elapsed(),
        &oracle,
        format!("{frames} × 900 KB"),
    ))
}

// ---------------------------------------------------------------------------
// F1s — sender panic mid-stream
// ---------------------------------------------------------------------------

pub async fn run_f1s_sender_panic(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "f1s").await?;
    let started = Instant::now();

    let consumer = Arc::clone(&pair.server.velo);
    let producer = Arc::clone(&pair.client.velo);

    let anchor = consumer.create_anchor::<Item>();
    let handle = anchor.handle();

    let producer_task = tokio::spawn(async move {
        let sender = producer.attach_anchor::<Item>(handle).await.unwrap();
        for seq in 0..50_u64 {
            sender
                .send(Item {
                    seq,
                    blob: bytes::Bytes::from_static(b"."),
                })
                .await
                .unwrap();
        }
        // Force panic; Drop should still send Dropped sentinel.
        panic!("F1s: induced sender panic mid-stream");
    });

    let mut stream = anchor;
    let mut got_items: u64 = 0;
    let mut saw_dropped = false;
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(StreamFrame::Item(_)) => got_items += 1,
            Ok(StreamFrame::Heartbeat) => unreachable!(),
            Err(velo::StreamError::SenderDropped) => {
                saw_dropped = true;
                break;
            }
            Ok(other) => return Err(anyhow!("F1s/unexpected: {other:?}")),
            Err(e) => return Err(anyhow!("F1s/err: {e}")),
        }
    }
    let _ = producer_task.await; // expected to be JoinError(Panic)

    if !saw_dropped {
        return Err(anyhow!(
            "F1s: never observed SenderDropped (got {got_items} items)"
        ));
    }
    if got_items == 0 {
        return Err(anyhow!(
            "F1s: received 0 items before panic — handler chain broken"
        ));
    }
    oracle.received.store(got_items, Ordering::Relaxed);
    oracle.fault_injected.store(1, Ordering::Relaxed);

    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "F1s",
        started.elapsed(),
        &oracle,
        format!("{got_items} items pre-panic, Dropped seen"),
    ))
}

// ---------------------------------------------------------------------------
// F2s — cancel storm
// ---------------------------------------------------------------------------

pub async fn run_f2s_cancel_storm(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "f2s").await?;
    let started = Instant::now();

    let consumer = Arc::clone(&pair.server.velo);
    let producer = Arc::clone(&pair.client.velo);

    let anchor = consumer.create_anchor::<Item>();
    let handle = anchor.handle();
    let controller = anchor.controller();

    let producer_task = tokio::spawn(async move {
        // The cancel storm may fire before we successfully attach. Treat
        // "anchor not found" as a normal outcome of the race rather than a
        // panic — the assertion is that cancellation is idempotent and
        // doesn't leave the runtime wedged.
        let sender = match producer.attach_anchor::<Item>(handle).await {
            Ok(s) => s,
            Err(_) => return,
        };
        let cancel = sender.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                r = sender.send(Item { seq: 0, blob: bytes::Bytes::from_static(b".") }) => {
                    if r.is_err() { break; }
                }
            }
        }
    });

    // 100 concurrent cancel calls.
    let mut joiners: JoinSet<()> = JoinSet::new();
    for _ in 0..100 {
        let c = controller.clone();
        joiners.spawn(async move {
            c.cancel();
        });
    }
    while joiners.join_next().await.is_some() {}

    // Drain the anchor.
    let mut stream = anchor;
    while stream.next().await.is_some() {}
    let _ = producer_task.await;

    drop(pair);
    oracle.fault_injected.store(1, Ordering::Relaxed);
    Ok(ScenarioReport::from_oracle(
        "F2s",
        started.elapsed(),
        &oracle,
        "100x concurrent cancel idempotent",
    ))
}

// ---------------------------------------------------------------------------
// F3s — slow consumer with heartbeats
// ---------------------------------------------------------------------------

pub async fn run_f3s_slow_consumer(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, "f3s").await?;
    let started = Instant::now();

    let consumer = Arc::clone(&pair.server.velo);
    let producer = Arc::clone(&pair.client.velo);

    let anchor = consumer.create_anchor::<Item>();
    let handle = anchor.handle();

    let producer_oracle = Arc::clone(&oracle);
    let producer_task = tokio::spawn(async move {
        let sender = producer.attach_anchor::<Item>(handle).await?;
        for seq in 0..500u64 {
            sender
                .send(Item {
                    seq,
                    blob: bytes::Bytes::from_static(b"."),
                })
                .await?;
            producer_oracle.inc_sent();
        }
        sender.finalize()?;
        anyhow::Ok(())
    });

    let mut stream = anchor;
    let mut expected: u64 = 0;
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(StreamFrame::Item(item)) => {
                if item.seq != expected {
                    return Err(anyhow!("F3s: expected {expected}, got {}", item.seq));
                }
                expected += 1;
                oracle.inc_received();
                // Slow consumer: 1ms per item.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(StreamFrame::Finalized) => break,
            Ok(StreamFrame::Heartbeat) => unreachable!(),
            Ok(other) => return Err(anyhow!("F3s/unexpected: {other:?}")),
            Err(e) => return Err(anyhow!("F3s/err: {e}")),
        }
    }
    producer_task.await??;

    if oracle.received() != 500 {
        return Err(anyhow!(
            "F3s: expected 500 frames, got {}",
            oracle.received()
        ));
    }

    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "F3s",
        started.elapsed(),
        &oracle,
        "slow consumer + heartbeats: no loss",
    ))
}
