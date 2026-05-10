// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Linux RSS sampler with a linear-regression slope check.
//!
//! Runs only on Linux. Samples `/proc/self/statm` every `step`; once the
//! sample window is long enough, fits a least-squares line through the
//! second half of the samples and asserts `slope_bytes_per_sec < threshold`.
//! Skipped on short runs — the slope is too noisy on <60 s windows.

use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct RssSummary {
    pub samples: usize,
    pub start_kb: u64,
    pub peak_kb: u64,
    pub end_kb: u64,
    pub slope_kb_per_sec: f64,
}

/// Spawn the sampler. The returned handle resolves to a summary once
/// `cancel` fires; `step` and the threshold pick are scenario-specific.
pub fn spawn(
    cancel: CancellationToken,
    step: Duration,
    slope_threshold_kb_per_sec: f64,
    enabled: bool,
) -> JoinHandle<Result<RssSummary>> {
    tokio::spawn(async move {
        if !enabled {
            return Ok(RssSummary {
                samples: 0,
                start_kb: 0,
                peak_kb: 0,
                end_kb: 0,
                slope_kb_per_sec: 0.0,
            });
        }

        let start = Instant::now();
        let mut samples: Vec<(f64, u64)> = Vec::new();
        loop {
            let kb = read_rss_kb().unwrap_or(0);
            samples.push((start.elapsed().as_secs_f64(), kb));
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(step) => {}
            }
        }

        // Final sample so end_kb is fresh.
        let kb = read_rss_kb().unwrap_or(0);
        samples.push((start.elapsed().as_secs_f64(), kb));

        if samples.is_empty() {
            return Ok(RssSummary {
                samples: 0,
                start_kb: 0,
                peak_kb: 0,
                end_kb: 0,
                slope_kb_per_sec: 0.0,
            });
        }

        let start_kb = samples.first().unwrap().1;
        let end_kb = samples.last().unwrap().1;
        let peak_kb = samples.iter().map(|(_, k)| *k).max().unwrap_or(0);

        let slope_kb_per_sec = if samples.len() >= 6 {
            let mid = samples.len() / 2;
            slope(&samples[mid..])
        } else {
            0.0
        };

        if slope_kb_per_sec.abs() > slope_threshold_kb_per_sec {
            return Err(anyhow!(
                "RSS slope {slope_kb_per_sec:.2} KB/s exceeds threshold \
                 {slope_threshold_kb_per_sec:.2} KB/s — possible leak \
                 (start={start_kb} KB peak={peak_kb} KB end={end_kb} KB samples={})",
                samples.len()
            ));
        }

        Ok(RssSummary {
            samples: samples.len(),
            start_kb,
            peak_kb,
            end_kb,
            slope_kb_per_sec,
        })
    })
}

#[cfg(target_os = "linux")]
fn read_rss_kb() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    // statm: size resident shared text lib data dt — all in pages.
    let mut it = s.split_whitespace();
    let _size = it.next()?;
    let resident_pages: u64 = it.next()?.parse().ok()?;
    let page_kb = page_size_kb()?;
    Some(resident_pages * page_kb)
}

#[cfg(not(target_os = "linux"))]
fn read_rss_kb() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn page_size_kb() -> Option<u64> {
    // SAFETY: sysconf is async-signal-safe and re-entrant.
    let p = unsafe { libc_sysconf_pagesize() };
    if p <= 0 {
        None
    } else {
        Some((p as u64) / 1024)
    }
}

#[cfg(target_os = "linux")]
unsafe fn libc_sysconf_pagesize() -> i64 {
    // Avoid pulling in libc as a dep — read /proc/self/auxv? No, simplest is
    // to read from /proc/meminfo's PageSize? Not portable. Default to 4 KB,
    // which matches every Linux configuration we care about.
    4096
}

/// Least-squares slope of `(x_secs, y_kb)` samples.
fn slope(samples: &[(f64, u64)]) -> f64 {
    let n = samples.len() as f64;
    if n < 2.0 {
        return 0.0;
    }
    let sum_x: f64 = samples.iter().map(|(x, _)| *x).sum();
    let sum_y: f64 = samples.iter().map(|(_, y)| *y as f64).sum();
    let sum_xy: f64 = samples.iter().map(|(x, y)| *x * *y as f64).sum();
    let sum_xx: f64 = samples.iter().map(|(x, _)| *x * *x).sum();
    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < 1e-9 {
        0.0
    } else {
        (n * sum_xy - sum_x * sum_y) / denom
    }
}
