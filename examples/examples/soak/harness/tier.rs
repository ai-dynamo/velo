// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tier sizing tables.
//!
//! `ci` is the only tier that runs unconditionally on every PR — it must finish
//! `soak all` in under two minutes. `nightly` runs on a cron and gets ~30 min.
//! `long` is operator-driven with no upper bound; defaults are placeholders.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lower")]
pub enum Tier {
    Ci,
    Nightly,
    Long,
}

#[derive(Debug, Clone, Copy)]
pub struct TierBudget {
    /// Wall-clock budget for any single scenario.
    pub per_scenario: Duration,
    /// Target offered load for messenger throughput scenarios (msg/s).
    pub messenger_rate: u64,
    /// Target in-flight concurrency for messenger request/response scenarios.
    pub messenger_concurrency: usize,
    /// Number of frames for the single-stream high-throughput scenario (S1).
    pub stream_frames: u64,
    /// Number of concurrent anchors for the multi-stream scenario (S2).
    pub stream_anchors: usize,
    /// Number of register/get/release cycles for rendezvous scenarios.
    pub rendezvous_cycles: u64,
    /// Largest payload pulled in chunked-rendezvous mode.
    pub rendezvous_max_bytes: usize,
    /// Whether to engage the RSS slope check (skip on short runs — too noisy).
    pub rss_check: bool,
}

impl Tier {
    pub fn budget(self) -> TierBudget {
        match self {
            // <2 min total for `soak all`. Each scenario must clear in seconds,
            // not minutes. Keep loads modest so even a slow CI box stays inside
            // the budget.
            //
            // `stream_anchors` is 64. Previously capped at 4 to work around a
            // `VeloFrameTransport` reorder-buffer bug that deadlocked S2 at
            // 16 anchors; after the WorkerAddress streaming refactor (default
            // streaming = TCP with multi-interface advertise) S2 passes cleanly
            // at 256+ anchors.
            //
            // Above ~256 anchors S2 starts to flake — but the failure mode is
            // *environmental*, not a runtime bug: the test producer pumps as
            // fast as it can, and at high anchor counts the consumer side
            // can't drain its bounded(256) per-anchor frame channels fast
            // enough. That backs up TCP receive → kernel zero-window → producer
            // TCP write stalls → no frames flow → reader_pump's heartbeat
            // watchdog fires after 15s. A 200µs sleep between sender.send()
            // calls in the test eliminates the failure entirely (verified
            // 30/30 pass at 1024 anchors). For sustained higher-anchor stress
            // testing add throttling to S2 or run on a less contended host.
            Tier::Ci => TierBudget {
                per_scenario: Duration::from_secs(8),
                messenger_rate: 1_000,
                messenger_concurrency: 16,
                stream_frames: 20_000,
                stream_anchors: 64,
                rendezvous_cycles: 500,
                rendezvous_max_bytes: 256 * 1024,
                rss_check: false,
            },
            // ~30 min total. Saturate within reason; this is where we expect
            // counter parity drift / leaks to surface if they exist.
            Tier::Nightly => TierBudget {
                per_scenario: Duration::from_secs(120),
                messenger_rate: 20_000,
                messenger_concurrency: 256,
                stream_frames: 1_000_000,
                stream_anchors: 256,
                rendezvous_cycles: 10_000,
                rendezvous_max_bytes: 64 * 1024 * 1024,
                rss_check: true,
            },
            // Operator-driven. Defaults are sensible starting points; expect
            // the operator to override --duration / --rate / --frames.
            Tier::Long => TierBudget {
                per_scenario: Duration::from_secs(3600),
                messenger_rate: 50_000,
                messenger_concurrency: 512,
                stream_frames: 10_000_000,
                stream_anchors: 512,
                rendezvous_cycles: 100_000,
                rendezvous_max_bytes: 256 * 1024 * 1024,
                rss_check: true,
            },
        }
    }
}
