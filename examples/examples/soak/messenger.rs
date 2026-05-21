// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Messenger soak scenarios.
//!
//! - **M1** sustained `am_send` fire-and-forget; oracle sent==handler invocations.
//! - **M2** sustained `unary` request/response; pending_responses bounded;
//!   `response_slot_exhausted_total == 0`.
//! - **M3** mixed traffic (am_send + unary interleaved).
//! - **M5** drain under load: ramp to N in-flight, drop client, gauges reset.
//!
//! Faults (run when `--faults` is set):
//! - **F1** panicking handler: 1 in N requests panic; on_error count tracks injection.
//! - **F2** peer kill mid-flight: drop server Arc; client unary calls error out.
//! - **F3** slow handler: handler sleeps > scenario budget for a fraction of requests.
//!
//! M4 (backpressure storm) and the more involved drain assertions are deferred —
//! the public messenger surface does not currently expose `peer_channel_capacity`
//! tuning, so we cannot reliably saturate the per-peer channel from the example.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use velo::{Handler, InstanceId, Velo};

use crate::faults::Dice;
use crate::harness::oracle::{
    Oracle, ScenarioReport, assert_gauges_at_baseline, messenger_pending_responses,
    messenger_response_slot_exhausted_total, settle_until, snapshot,
};
use crate::harness::{Pair, ScenarioCtx, build_pair, progress};

const HANDLER_AM_COUNT: &str = "soak_count";
const HANDLER_UNARY_ECHO: &str = "soak_echo";
const HANDLER_AM_SLOW: &str = "soak_slow";

/// Wire up handlers used across messenger scenarios. Idempotent per-Velo
/// because we register them once at pair-build time on the server side.
fn register_handlers(
    velo: &Arc<Velo>,
    oracle: Arc<Oracle>,
    slow_dice: Arc<Dice>,
    slow_for: Duration,
) -> Result<()> {
    {
        let oracle = Arc::clone(&oracle);
        let h = Handler::am_handler(HANDLER_AM_COUNT, move |_ctx| {
            oracle.inc_handler_invocations();
            oracle.inc_received();
            Ok(())
        })
        .build();
        velo.register_handler(h)?;
    }
    {
        let oracle = Arc::clone(&oracle);
        let h = Handler::unary_handler(HANDLER_UNARY_ECHO, move |ctx| {
            oracle.inc_handler_invocations();
            oracle.inc_received();
            Ok(Some(ctx.payload.clone()))
        })
        .build();
        velo.register_handler(h)?;
    }
    {
        let oracle = Arc::clone(&oracle);
        let dice = Arc::clone(&slow_dice);
        // Async handler so the sleep doesn't block other handler dispatches.
        let h = Handler::am_handler_async(HANDLER_AM_SLOW, move |_ctx| {
            let oracle = Arc::clone(&oracle);
            let dice = Arc::clone(&dice);
            let slow_for = slow_for;
            async move {
                if dice.hit() {
                    tokio::time::sleep(slow_for).await;
                }
                oracle.inc_handler_invocations();
                oracle.inc_received();
                Ok(())
            }
        })
        .build();
        velo.register_handler(h)?;
    }
    Ok(())
}

pub async fn run_all(ctx: &ScenarioCtx) -> Result<Vec<ScenarioReport>> {
    let mut out = Vec::new();
    if ctx.selected("M1") {
        out.push(run_m1(ctx).await?);
    }
    if ctx.selected("M2") {
        out.push(run_m2(ctx).await?);
    }
    if ctx.selected("M3") {
        out.push(run_m3(ctx).await?);
    }
    if ctx.selected("M5") {
        out.push(run_m5(ctx).await?);
    }
    if ctx.faults {
        if ctx.selected("F1") {
            out.push(run_f1_panicking_handler(ctx).await?);
        }
        if ctx.selected("F2") {
            out.push(run_f2_peer_kill(ctx).await?);
        }
        if ctx.selected("F3") {
            out.push(run_f3_slow_handler(ctx).await?);
        }
    }
    Ok(out)
}

/// Kick off a server-side handler set on a freshly-built pair.
async fn fresh_pair(ctx: &ScenarioCtx, oracle: Arc<Oracle>) -> Result<Pair> {
    fresh_pair_with_slow(ctx, oracle, Arc::new(Dice::new(0)), Duration::from_secs(0)).await
}

async fn fresh_pair_with_slow(
    ctx: &ScenarioCtx,
    oracle: Arc<Oracle>,
    slow_dice: Arc<Dice>,
    slow_for: Duration,
) -> Result<Pair> {
    let pair = build_pair(ctx.transport, ctx.stream_transport, "soak-msg").await?;
    register_handlers(&pair.server.velo, oracle, slow_dice, slow_for)?;
    // Settle so the client knows about the new handlers.
    pair.client
        .velo
        .wait_for_handler(pair.server.instance_id, HANDLER_AM_COUNT)
        .await?;
    Ok(pair)
}

// ---------------------------------------------------------------------------
// M1 — sustained am_send
// ---------------------------------------------------------------------------

pub async fn run_m1(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, Arc::clone(&oracle)).await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let cancel = CancellationToken::new();
    let pl = progress::spawn(
        "M1",
        Arc::clone(&oracle),
        Duration::from_secs(2),
        cancel.clone(),
    );

    let started = Instant::now();
    let send_fut = drive_am_send(
        velo,
        target,
        Arc::clone(&oracle),
        ctx.scenario_duration(),
        ctx.tier.budget().messenger_rate,
    );
    send_fut.await?;

    // Allow a settle window for in-flight sends to be received.
    settle_until(Duration::from_secs(10), Duration::from_millis(100), || {
        let sent = oracle.sent();
        let recv = oracle.received();
        if sent == recv {
            Ok(())
        } else {
            Err(anyhow!("sent={sent} recv={recv}"))
        }
    })
    .await?;

    cancel.cancel();
    let _ = pl.await;

    oracle.assert_clean_throughput()?;
    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);
    drop(pair);
    assert_gauges_at_baseline(&server_snap, &client_snap)?;

    Ok(ScenarioReport::from_oracle(
        "M1",
        started.elapsed(),
        &oracle,
        "am_send sustained",
    ))
}

async fn drive_am_send(
    velo: Arc<Velo>,
    target: InstanceId,
    oracle: Arc<Oracle>,
    duration: Duration,
    rate: u64,
) -> Result<()> {
    let payload = Bytes::from(vec![0u8; 64]);
    let interval = if rate == 0 {
        Duration::from_nanos(1)
    } else {
        Duration::from_secs_f64(1.0 / rate as f64)
    };
    let mut next = Instant::now();
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        match velo
            .am_send(HANDLER_AM_COUNT)?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await
        {
            Ok(()) => oracle.inc_sent(),
            Err(e) => return Err(e),
        }
        next += interval;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// M2 — sustained unary
// ---------------------------------------------------------------------------

pub async fn run_m2(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, Arc::clone(&oracle)).await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let cancel = CancellationToken::new();
    let pl = progress::spawn(
        "M2",
        Arc::clone(&oracle),
        Duration::from_secs(2),
        cancel.clone(),
    );

    let started = Instant::now();
    let payload = Bytes::from(vec![0u8; 256]);
    let concurrency = ctx.tier.budget().messenger_concurrency;
    let deadline = Instant::now() + ctx.scenario_duration();

    let mut set: JoinSet<Result<()>> = JoinSet::new();
    while Instant::now() < deadline {
        if set.len() >= concurrency
            && let Some(r) = set.join_next().await {
                r??;
            }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        let oracle = Arc::clone(&oracle);
        oracle.inc_sent();
        set.spawn(async move {
            v.unary(HANDLER_UNARY_ECHO)?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(())
        });
    }
    while let Some(r) = set.join_next().await {
        r??;
    }

    cancel.cancel();
    let _ = pl.await;

    oracle.assert_clean_throughput()?;

    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);

    // Pending response gauge MUST be zero post-run.
    let pending =
        messenger_pending_responses(&server_snap) + messenger_pending_responses(&client_snap);
    if pending != 0.0 {
        return Err(anyhow!(
            "M2: velo_messenger_pending_responses={pending} after run (expected 0)"
        ));
    }
    let exhausted = messenger_response_slot_exhausted_total(&server_snap)
        + messenger_response_slot_exhausted_total(&client_snap);
    if exhausted != 0.0 {
        return Err(anyhow!(
            "M2: velo_messenger_response_slot_exhausted_total={exhausted} (expected 0 in non-saturating run)"
        ));
    }

    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "M2",
        started.elapsed(),
        &oracle,
        "unary sustained, no slot exhaustion",
    ))
}

// ---------------------------------------------------------------------------
// M3 — mixed traffic
// ---------------------------------------------------------------------------

pub async fn run_m3(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, Arc::clone(&oracle)).await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let cancel = CancellationToken::new();
    let pl = progress::spawn(
        "M3",
        Arc::clone(&oracle),
        Duration::from_secs(2),
        cancel.clone(),
    );

    let started = Instant::now();
    let duration = ctx.scenario_duration();
    let am_rate = ctx.tier.budget().messenger_rate;

    // AM driver in the background, unary driver in the foreground.
    let am_oracle = Arc::clone(&oracle);
    let v = Arc::clone(&velo);
    let am_handle =
        tokio::spawn(
            async move { drive_am_send(v, target, am_oracle, duration, am_rate / 2).await },
        );

    let payload = Bytes::from(vec![0u8; 64]);
    let concurrency = ctx.tier.budget().messenger_concurrency.max(8);
    let deadline = Instant::now() + duration;
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    while Instant::now() < deadline {
        if set.len() >= concurrency
            && let Some(r) = set.join_next().await {
                r??;
            }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        let oracle = Arc::clone(&oracle);
        oracle.inc_sent();
        set.spawn(async move {
            v.unary(HANDLER_UNARY_ECHO)?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(())
        });
    }
    while let Some(r) = set.join_next().await {
        r??;
    }
    am_handle.await??;

    settle_until(Duration::from_secs(10), Duration::from_millis(100), || {
        let s = oracle.sent();
        let r = oracle.received();
        if s == r {
            Ok(())
        } else {
            Err(anyhow!("M3: sent={s} recv={r}"))
        }
    })
    .await?;

    cancel.cancel();
    let _ = pl.await;

    oracle.assert_clean_throughput()?;
    let server_snap = snapshot(&pair.server.registry);
    let client_snap = snapshot(&pair.client.registry);
    drop(pair);
    assert_gauges_at_baseline(&server_snap, &client_snap)?;

    Ok(ScenarioReport::from_oracle(
        "M3",
        started.elapsed(),
        &oracle,
        "mixed am+unary",
    ))
}

// ---------------------------------------------------------------------------
// M5 — drain under load (best-effort)
// ---------------------------------------------------------------------------

pub async fn run_m5(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, Arc::clone(&oracle)).await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let started = Instant::now();
    let duration = ctx.scenario_duration().min(Duration::from_secs(10));
    let payload = Bytes::from(vec![0u8; 128]);

    // Ramp in-flight unaries.
    let concurrency = ctx.tier.budget().messenger_concurrency;
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    let stop_at = Instant::now() + duration;
    while Instant::now() < stop_at && oracle.sent() < concurrency as u64 * 10 {
        if set.len() >= concurrency
            && let Some(r) = set.join_next().await {
                let _ = r?; // tolerate errors; drain may produce them
            }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        let oracle = Arc::clone(&oracle);
        oracle.inc_sent();
        set.spawn(async move {
            match v
                .unary(HANDLER_UNARY_ECHO)?
                .raw_payload(p)
                .instance(target)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    // Drain may reject in-flight; that's acceptable here.
                    Ok(())
                }
            }
        });
    }

    // Drop the server side. Its transports tear down; pending responses on the
    // client should error out cleanly.
    drop(pair.server);

    while let Some(r) = set.join_next().await {
        let _ = r?;
    }

    let client_snap = snapshot(&pair.client.registry);
    let pending = messenger_pending_responses(&client_snap);
    if pending != 0.0 {
        return Err(anyhow!(
            "M5: client pending_responses={pending} after server drop (expected 0)"
        ));
    }

    Ok(ScenarioReport::from_oracle(
        "M5",
        started.elapsed(),
        &oracle,
        "server drop drains pending responses",
    ))
}

// ---------------------------------------------------------------------------
// F1 — panicking handler
// ---------------------------------------------------------------------------

pub async fn run_f1_panicking_handler(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = build_pair(ctx.transport, ctx.stream_transport, "soak-msg-f1").await?;

    // Custom registration: AM that panics every Nth call.
    let panic_dice = Arc::new(Dice::new(100));
    {
        let oracle = Arc::clone(&oracle);
        let dice = Arc::clone(&panic_dice);
        let h = Handler::am_handler(HANDLER_AM_COUNT, move |_ctx| {
            if dice.hit() {
                oracle.handler_panics.fetch_add(1, Ordering::Relaxed);
                panic!("F1: induced handler panic");
            }
            oracle.inc_handler_invocations();
            oracle.inc_received();
            Ok(())
        })
        .build();
        pair.server.velo.register_handler(h)?;
    }
    pair.client
        .velo
        .wait_for_handler(pair.server.instance_id, HANDLER_AM_COUNT)
        .await?;

    let started = Instant::now();
    let velo = Arc::clone(&pair.client.velo);
    let target = pair.server.instance_id;
    let payload = Bytes::from(vec![0u8; 64]);
    let count = ctx.tier.budget().messenger_rate / 4;
    for _ in 0..count {
        velo.am_send(HANDLER_AM_COUNT)?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
        oracle.inc_sent();
    }

    // Settle a moment for inbound to land.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let panics = oracle.handler_panics.load(Ordering::Relaxed);
    oracle.fault_injected.store(panics, Ordering::Relaxed);

    // Sent should equal received + panics. Fire-and-forget AMs that hit a
    // panicking handler do not return an error to the client (the AM is a
    // void send), so we account for them in fault_injected and assert
    // received + faults == sent.
    settle_until(Duration::from_secs(5), Duration::from_millis(100), || {
        let s = oracle.sent();
        let r = oracle.received();
        let p = oracle.handler_panics.load(Ordering::Relaxed);
        if r + p == s {
            Ok(())
        } else {
            Err(anyhow!("F1: sent={s} recv={r} panics={p}"))
        }
    })
    .await?;

    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "F1",
        started.elapsed(),
        &oracle,
        "panicking handler accounted",
    ))
}

// ---------------------------------------------------------------------------
// F2 — peer kill mid-flight
// ---------------------------------------------------------------------------

pub async fn run_f2_peer_kill(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let pair = fresh_pair(ctx, Arc::clone(&oracle)).await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let started = Instant::now();
    let payload = Bytes::from(vec![0u8; 64]);
    let concurrency = ctx.tier.budget().messenger_concurrency;
    let mut set: JoinSet<Result<bool>> = JoinSet::new();

    // Spawn a wave of unaries; mid-flight, drop the server side.
    for i in 0..concurrency * 4 {
        let v = Arc::clone(&velo);
        let p = payload.clone();
        let oracle = Arc::clone(&oracle);
        oracle.inc_sent();
        set.spawn(async move {
            // Slight stagger so half hit pre-kill, half post.
            if i % 2 == 1 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            match v
                .unary(HANDLER_UNARY_ECHO)?
                .raw_payload(p)
                .instance(target)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        });
    }

    // Kill the server.
    tokio::time::sleep(Duration::from_millis(40)).await;
    drop(pair.server);

    let mut ok = 0u64;
    let mut err = 0u64;
    while let Some(r) = set.join_next().await {
        match r? {
            Ok(true) => ok += 1,
            Ok(false) => err += 1,
            Err(_) => err += 1,
        }
    }
    oracle.fault_injected.store(err, Ordering::Relaxed);
    oracle.received.store(ok, Ordering::Relaxed);

    if ok + err != oracle.sent() {
        return Err(anyhow!(
            "F2: ok={ok} + err={err} != sent={} ",
            oracle.sent()
        ));
    }

    let client_snap = snapshot(&pair.client.registry);
    let pending = messenger_pending_responses(&client_snap);
    if pending != 0.0 {
        return Err(anyhow!(
            "F2: client pending_responses={pending} (expected 0 after server kill settled)"
        ));
    }

    Ok(ScenarioReport::from_oracle(
        "F2",
        started.elapsed(),
        &oracle,
        format!("peer kill: {ok} ok / {err} err"),
    ))
}

// ---------------------------------------------------------------------------
// F3 — slow handler
// ---------------------------------------------------------------------------

pub async fn run_f3_slow_handler(ctx: &ScenarioCtx) -> Result<ScenarioReport> {
    let oracle = Oracle::arc();
    let dice = Arc::new(Dice::new(50));
    let pair = fresh_pair_with_slow(
        ctx,
        Arc::clone(&oracle),
        Arc::clone(&dice),
        Duration::from_millis(50),
    )
    .await?;
    let target = pair.server.instance_id;
    let velo = Arc::clone(&pair.client.velo);

    let started = Instant::now();
    let payload = Bytes::from(vec![0u8; 64]);
    let count = ctx.tier.budget().messenger_rate / 4;
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    let concurrency = ctx.tier.budget().messenger_concurrency;
    for _ in 0..count {
        if set.len() >= concurrency
            && let Some(r) = set.join_next().await {
                r??;
            }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        let oracle = Arc::clone(&oracle);
        oracle.inc_sent();
        set.spawn(async move {
            v.am_send(HANDLER_AM_SLOW)?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(())
        });
    }
    while let Some(r) = set.join_next().await {
        r??;
    }

    settle_until(Duration::from_secs(15), Duration::from_millis(100), || {
        let s = oracle.sent();
        let r = oracle.received();
        if s == r {
            Ok(())
        } else {
            Err(anyhow!("F3: sent={s} recv={r}"))
        }
    })
    .await?;

    oracle.assert_clean_throughput()?;
    drop(pair);
    Ok(ScenarioReport::from_oracle(
        "F3",
        started.elapsed(),
        &oracle,
        "slow handler kept up",
    ))
}
