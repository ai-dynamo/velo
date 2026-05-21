// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Rolling progress logger. Prints `[scenario] elapsed=Xs sent=Y rate=Z/s`
//! every `interval` so a multi-hour soak is observable from the terminal /
//! CI log without requiring a separate dashboard.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::oracle::Oracle;

/// Spawn a background printer. Drop the returned handle (via `cancel.cancel()`
/// then `.await`) to stop printing.
pub fn spawn(
    scenario: &'static str,
    oracle: Arc<Oracle>,
    interval: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let start = Instant::now();
        let mut last_sent: u64 = 0;
        let mut last_tick = start;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
            let now = Instant::now();
            let sent = oracle.sent.load(Ordering::Relaxed);
            let recv = oracle.received.load(Ordering::Relaxed);
            let bp = oracle.backpressure_seen.load(Ordering::Relaxed);
            let dt = now.duration_since(last_tick).as_secs_f64().max(0.001);
            let rate = (sent.saturating_sub(last_sent) as f64) / dt;
            println!(
                "[{scenario}] t={:>6.1}s sent={sent} recv={recv} bp={bp} rate={rate:>8.0}/s",
                now.duration_since(start).as_secs_f64(),
            );
            last_sent = sent;
            last_tick = now;
        }
    })
}
