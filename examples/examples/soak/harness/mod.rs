// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared infrastructure for soak scenarios.
//!
//! - [`tier`] — sizing tables (ci / nightly / long).
//! - [`builder`] — two-instance Velo bring-up over TCP or gRPC.
//! - [`oracle`] — independent counters + Prometheus snapshot diff.
//! - [`progress`] — rolling rate logger for long runs.
//! - [`rss`] — `/proc/self/statm` sampler with linear-regression slope check.

pub mod builder;
pub mod oracle;
pub mod progress;
pub mod rss;
pub mod tier;

pub use builder::{Pair, build_pair};
pub use oracle::ScenarioReport;
pub use tier::Tier;

use std::time::Duration;

/// Result type for scenario closures. Errors propagate up to main and yield non-zero exit.
pub type ScenarioResult = anyhow::Result<ScenarioReport>;

/// Streaming-transport selection plumbed through every scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTransportKind {
    Tcp,
    Grpc,
}

/// Common knobs every scenario inherits from the CLI.
#[derive(Debug, Clone)]
pub struct ScenarioCtx {
    /// Selected runtime budget tier.
    pub tier: Tier,
    /// Override of per-scenario duration. `None` ⇒ use tier default.
    pub duration: Option<Duration>,
    /// Whether to run fault-injection variants.
    pub faults: bool,
    /// Subset of scenario IDs to run, e.g. `["M1", "M5"]`. Empty = all.
    pub scenarios: Vec<String>,
    /// Messenger transport backend.
    pub transport: velo_examples::TransportType,
    /// Streaming transport backend. Independent of `transport`.
    pub stream_transport: StreamTransportKind,
}

impl ScenarioCtx {
    /// Resolved per-scenario duration (CLI override or tier default).
    pub fn scenario_duration(&self) -> Duration {
        self.duration.unwrap_or(self.tier.budget().per_scenario)
    }

    /// True if `id` is selected (or no filter active).
    pub fn selected(&self, id: &str) -> bool {
        self.scenarios.is_empty() || self.scenarios.iter().any(|s| s.eq_ignore_ascii_case(id))
    }
}
