// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pluggable network models for the simulation fabric.

use std::collections::HashMap;
use std::time::Duration;

use velo_common::InstanceId;

use crate::fabric::Transfer;

const COMPLETION_EPSILON_BYTES: f64 = 1e-9;

/// Result of evaluating the network model: which transfer completes next.
pub struct NextCompletion {
    /// Index into the transfers slice.
    pub transfer_index: usize,
    /// Absolute virtual time when this transfer completes.
    pub completion_time: Duration,
}

/// Pluggable network model that determines transfer completion times.
///
/// The model owns both the rules for advancing transfer progress over virtual
/// time and the selection of the next completion under the current contention.
pub trait NetworkModel: Send + Sync + 'static {
    /// Advance all active transfers to `now`.
    fn advance_to(&self, transfers: &mut [Transfer], now: Duration);

    /// Evaluate all active transfers and return the next completion.
    ///
    /// Returns `None` if no transfers are active.
    fn next_completion(&self, transfers: &[Transfer], now: Duration) -> Option<NextCompletion>;

    /// Returns `true` if the transfer is complete at `now`.
    fn is_complete(&self, transfer: &Transfer, now: Duration) -> bool;
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
    fn advance_to(&self, transfers: &mut [Transfer], now: Duration) {
        if transfers.is_empty() {
            return;
        }

        let per_transfer_bps = self.per_transfer_bps(transfers, now);

        for (transfer, bps) in transfers.iter_mut().zip(per_transfer_bps) {
            let active_after = transfer.enqueue_time + self.base_latency;
            let active_start = std::cmp::max(transfer.last_update_time, active_after);
            let active_elapsed = now.saturating_sub(active_start);

            if !active_elapsed.is_zero() && bps > 0.0 {
                let bits_transferred = bps * active_elapsed.as_secs_f64();
                transfer.bytes_transferred = (transfer.bytes_transferred
                    + (bits_transferred / 8.0))
                    .min(transfer.total_bytes as f64);
            }

            transfer.last_update_time = now;
        }
    }

    fn next_completion(&self, transfers: &[Transfer], now: Duration) -> Option<NextCompletion> {
        if transfers.is_empty() {
            return None;
        }

        let per_transfer_bps = self.per_transfer_bps(transfers, now);
        let mut earliest_idx = 0;
        let mut earliest_time = Duration::MAX;

        for (i, transfer) in transfers.iter().enumerate() {
            let bytes_remaining =
                (transfer.total_bytes as f64 - transfer.bytes_transferred).max(0.0);
            let latency_ready_at = transfer.enqueue_time + self.base_latency;
            let latency_remaining = latency_ready_at.saturating_sub(now);

            if bytes_remaining <= COMPLETION_EPSILON_BYTES {
                let completion = now + latency_remaining;
                if completion < earliest_time {
                    earliest_time = completion;
                    earliest_idx = i;
                }
                continue;
            }

            let bps = per_transfer_bps[i];
            if bps <= 0.0 {
                continue;
            }

            let bits_remaining = bytes_remaining * 8.0;
            let transfer_secs = bits_remaining / bps;
            let completion = now + latency_remaining + Duration::from_secs_f64(transfer_secs);

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

    fn is_complete(&self, transfer: &Transfer, now: Duration) -> bool {
        now >= transfer.enqueue_time + self.base_latency
            && transfer.bytes_transferred + COMPLETION_EPSILON_BYTES >= transfer.total_bytes as f64
    }
}

impl BisectionBandwidth {
    fn per_transfer_bps(&self, transfers: &[Transfer], now: Duration) -> Vec<f64> {
        let link_bps = self.link_gbps * 1e9;
        let bisection_bps = self.bisection_gbps * 1e9;

        // Count only active transfers (past base_latency) per link for contention
        let mut active_link_counts: HashMap<(InstanceId, InstanceId), usize> = HashMap::new();
        for transfer in transfers {
            if now >= transfer.enqueue_time + self.base_latency {
                *active_link_counts
                    .entry((transfer.source, transfer.target))
                    .or_insert(0) += 1;
            }
        }

        let mut per_transfer_bps = Vec::with_capacity(transfers.len());
        for transfer in transfers {
            let active = now >= transfer.enqueue_time + self.base_latency;
            let count = active_link_counts
                .get(&(transfer.source, transfer.target))
                .copied()
                .unwrap_or(if active { 1 } else { 0 });
            // Active transfers share link bandwidth among themselves.
            // Inactive transfers (still in base_latency) get the full link rate
            // as a prediction for next_completion — they don't reduce active
            // transfers' share because they aren't counted in active_link_counts.
            let effective_count = if active { count } else { 1 };
            per_transfer_bps.push(link_bps / effective_count as f64);
        }

        // Bisection cap applies only to active transfers' aggregate
        let active_total_bps: f64 = transfers
            .iter()
            .zip(per_transfer_bps.iter())
            .filter(|(t, _)| now >= t.enqueue_time + self.base_latency)
            .map(|(_, bps)| bps)
            .sum();
        if active_total_bps > bisection_bps {
            let scale = bisection_bps / active_total_bps;
            for (transfer, bps) in transfers.iter().zip(per_transfer_bps.iter_mut()) {
                if now >= transfer.enqueue_time + self.base_latency {
                    *bps *= scale;
                }
            }
        }

        per_transfer_bps
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use velo_common::InstanceId;
    use velo_transports::TransportErrorHandler;

    struct NoopErrorHandler;

    impl TransportErrorHandler for NoopErrorHandler {
        fn on_error(&self, _header: bytes::Bytes, _payload: bytes::Bytes, _error: String) {}
    }

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
            on_error: Arc::new(NoopErrorHandler),
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
        let result = model.next_completion(&transfers, Duration::ZERO).unwrap();

        assert_eq!(result.transfer_index, 0);
        // Should be ~10.04µs (10µs latency + 40ns transfer)
        let expected_ns = 10_000 + 40;
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

        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, b, 1024)];
        let result = model.next_completion(&transfers, Duration::ZERO).unwrap();

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

        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, c, 1024)];
        let result = model.next_completion(&transfers, Duration::ZERO).unwrap();

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
            bisection_gbps: 200.0,
            base_latency: Duration::ZERO,
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();
        let c = InstanceId::new_v4();

        let transfers = vec![make_transfer(a, b, 1024), make_transfer(a, c, 1024)];
        let result = model.next_completion(&transfers, Duration::ZERO).unwrap();

        let expected_ns = 81;
        let actual_ns = result.completion_time.as_nanos();
        assert!(
            (actual_ns as i128 - expected_ns as i128).unsigned_abs() < 5,
            "expected ~{expected_ns}ns, got {actual_ns}ns"
        );
    }

    #[test]
    fn advance_to_respects_base_latency() {
        let model = BisectionBandwidth {
            link_gbps: 8.0,
            bisection_gbps: 8.0,
            base_latency: Duration::from_nanos(100),
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();
        let mut transfers = vec![make_transfer(a, b, 1000)];

        model.advance_to(&mut transfers, Duration::from_nanos(50));
        assert_eq!(transfers[0].bytes_transferred, 0.0);

        model.advance_to(&mut transfers, Duration::from_nanos(600));
        assert!((transfers[0].bytes_transferred - 500.0).abs() < 0.001);
    }

    #[test]
    fn next_completion_uses_remaining_progress() {
        let model = BisectionBandwidth {
            link_gbps: 8.0,
            bisection_gbps: 8.0,
            base_latency: Duration::ZERO,
        };

        let a = InstanceId::new_v4();
        let b = InstanceId::new_v4();
        let mut transfers = vec![make_transfer(a, b, 1000)];

        model.advance_to(&mut transfers, Duration::from_nanos(500));
        let result = model
            .next_completion(&transfers, Duration::from_nanos(500))
            .unwrap();

        assert_eq!(result.transfer_index, 0);
        assert_eq!(result.completion_time, Duration::from_nanos(1_000));
    }
}
