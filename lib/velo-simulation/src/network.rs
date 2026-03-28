// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pluggable network models for the simulation fabric.

use std::time::Duration;

use velo_common::InstanceId;

use crate::fabric::Transfer;

/// Result of evaluating the network model: which transfer completes next.
pub struct NextCompletion {
    /// Index into the transfers slice.
    pub transfer_index: usize,
    /// Absolute virtual time when this transfer completes.
    pub completion_time: Duration,
}

/// Pluggable network model that determines transfer completion times.
///
/// Given all active transfers and the current virtual time, the model computes
/// which transfer finishes next and when. The fabric calls `evaluate()` every
/// time a transfer is added or completed.
pub trait NetworkModel: Send + Sync + 'static {
    /// Evaluate all active transfers and return the next completion.
    ///
    /// Returns `None` if no transfers are active.
    fn evaluate(&self, transfers: &[Transfer], now: Duration) -> Option<NextCompletion>;
}

/// Per-adapter link bandwidth with a global bisection cap.
///
/// Models modern HPC/AI network fabric:
/// - Each (source, target) pair has `link_gbps` shared among concurrent transfers on that link
/// - Aggregate of all transfers is capped at `bisection_gbps`
/// - Every message pays a fixed `base_latency` (propagation + switching)
///
/// # Defaults
///
/// ```ignore
/// BisectionBandwidth {
///     link_gbps: 200.0,                          // 200 Gbps adapter (CX-7 class)
///     bisection_gbps: 12_800.0,                  // 128-port × 200G = 12.8 Tbps
///     base_latency: Duration::from_micros(10),   // ~10µs under no contention
/// }
/// ```
pub struct BisectionBandwidth {
    /// Per-adapter link rate in gigabits per second (e.g. 200.0 for 200 Gbps).
    pub link_gbps: f64,
    /// Aggregate cross-fabric cap in gigabits per second (e.g. 12_800.0 for 12.8 Tbps).
    pub bisection_gbps: f64,
    /// Fixed per-message latency (propagation + switching).
    pub base_latency: Duration,
}

impl Default for BisectionBandwidth {
    fn default() -> Self {
        Self {
            link_gbps: 200.0,
            bisection_gbps: 12_800.0,
            base_latency: Duration::from_micros(10),
        }
    }
}

impl NetworkModel for BisectionBandwidth {
    fn evaluate(&self, transfers: &[Transfer], now: Duration) -> Option<NextCompletion> {
        if transfers.is_empty() {
            return None;
        }

        let link_bps = self.link_gbps * 1e9;
        let bisection_bps = self.bisection_gbps * 1e9;

        // Step 1: Compute per-link share for each transfer.
        // Group by (source, target) and divide link bandwidth equally.
        let mut per_transfer_bps = vec![0.0f64; transfers.len()];

        // Count transfers per link
        let mut link_counts: Vec<((InstanceId, InstanceId), usize)> = Vec::new();
        for t in transfers {
            let key = (t.source, t.target);
            if let Some(entry) = link_counts.iter_mut().find(|(k, _)| *k == key) {
                entry.1 += 1;
            } else {
                link_counts.push((key, 1));
            }
        }

        // Assign per-link share
        for (i, t) in transfers.iter().enumerate() {
            let key = (t.source, t.target);
            let count = link_counts.iter().find(|(k, _)| *k == key).unwrap().1;
            per_transfer_bps[i] = link_bps / count as f64;
        }

        // Step 2: Apply bisection cap.
        // If sum of all allocations exceeds bisection, scale down proportionally.
        let total_bps: f64 = per_transfer_bps.iter().sum();
        if total_bps > bisection_bps {
            let scale = bisection_bps / total_bps;
            for bps in &mut per_transfer_bps {
                *bps *= scale;
            }
        }

        // Step 3: Find which transfer completes first.
        let mut earliest_idx = 0;
        let mut earliest_time = Duration::MAX;

        for (i, t) in transfers.iter().enumerate() {
            let bits_remaining = (t.total_bytes as f64 - t.bytes_transferred) * 8.0;
            let bps = per_transfer_bps[i];
            if bps <= 0.0 {
                continue;
            }
            let transfer_secs = bits_remaining / bps;
            let base_latency_remaining = if t.bytes_transferred == 0.0 {
                self.base_latency
            } else {
                Duration::ZERO // latency already paid
            };
            let completion = now + base_latency_remaining + Duration::from_secs_f64(transfer_secs);

            if completion < earliest_time {
                earliest_time = completion;
                earliest_idx = i;
            }
        }

        if earliest_time == Duration::MAX {
            return None;
        }

        Some(NextCompletion {
            transfer_index: earliest_idx,
            completion_time: earliest_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_common::InstanceId;

    fn make_transfer(source: InstanceId, target: InstanceId, bytes: usize) -> Transfer {
        Transfer {
            source,
            target,
            header: bytes::Bytes::new(),
            payload: bytes::Bytes::new(),
            message_type: velo_transports::MessageType::Message,
            total_bytes: bytes,
            bytes_transferred: 0.0,
            enqueue_time: Duration::ZERO,
            last_update_time: Duration::ZERO,
        }
    }

    #[test]
    fn single_transfer_no_contention() {
        let model = BisectionBandwidth {
            link_gbps: 200.0,
            bisection_gbps: 12_800.0,
            base_latency: Duration::from_micros(10),
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();

        // 1 KB message at 200 Gbps = 8000 bits / 200e9 bps = 40ns + 10µs latency
        let transfers = vec![make_transfer(a, b, 1024)];
        let result = model.evaluate(&transfers, Duration::ZERO).unwrap();

        assert_eq!(result.transfer_index, 0);
        // Should be ~10.04µs (10µs latency + 40ns transfer)
        let expected_ns = 10_000 + 40; // approximate
        let actual_ns = result.completion_time.as_nanos();
        assert!(
            (actual_ns as i128 - expected_ns as i128).unsigned_abs() < 5,
            "expected ~{expected_ns}ns, got {actual_ns}ns"
        );
    }

    #[test]
    fn two_transfers_same_link_share_bandwidth() {
        let model = BisectionBandwidth {
            link_gbps: 200.0,
            bisection_gbps: 12_800.0,
            base_latency: Duration::ZERO,
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();

        // Two identical transfers on same link: each gets 100 Gbps
        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, b, 1024)];
        let result = model.evaluate(&transfers, Duration::ZERO).unwrap();

        // 8192 bits / 100e9 bps = 81.92ns — either finishes at same time
        let expected_ns = 81;
        let actual_ns = result.completion_time.as_nanos();
        assert!(
            (actual_ns as i128 - expected_ns as i128).unsigned_abs() < 5,
            "expected ~{expected_ns}ns, got {actual_ns}ns"
        );
    }

    #[test]
    fn different_links_no_contention() {
        let model = BisectionBandwidth {
            link_gbps: 200.0,
            bisection_gbps: 12_800.0,
            base_latency: Duration::ZERO,
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();
        let c = InstanceId::new_v4();

        // Two transfers on different links: each gets full 200 Gbps
        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, c, 1024)];
        let result = model.evaluate(&transfers, Duration::ZERO).unwrap();

        // 8192 bits / 200e9 bps = 40.96ns
        let expected_ns = 40;
        let actual_ns = result.completion_time.as_nanos();
        assert!(
            (actual_ns as i128 - expected_ns as i128).unsigned_abs() < 5,
            "expected ~{expected_ns}ns, got {actual_ns}ns"
        );
    }

    #[test]
    fn bisection_cap_throttles() {
        let model = BisectionBandwidth {
            link_gbps: 200.0,
            bisection_gbps: 200.0, // very low bisection = only 200 Gbps total
            base_latency: Duration::ZERO,
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();
        let c = InstanceId::new_v4();

        // Two transfers on different links, but bisection caps at 200 Gbps total
        // Each gets 100 Gbps
        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, c, 1024)];
        let result = model.evaluate(&transfers, Duration::ZERO).unwrap();

        // 8192 bits / 100e9 bps = 81.92ns
        let expected_ns = 81;
        let actual_ns = result.completion_time.as_nanos();
        assert!(
            (actual_ns as i128 - expected_ns as i128).unsigned_abs() < 5,
            "expected ~{expected_ns}ns, got {actual_ns}ns"
        );
    }
}
