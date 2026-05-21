// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Independent counters + Prometheus diff. The whole point of the oracle is
//! that it does not trust the runtime's bookkeeping: every scenario keeps its
//! own atomic tallies, then asserts those tallies match the metric snapshot
//! at the end. A runtime regression that broke `record_frame` would still be
//! caught by oracle.received != oracle.sent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use prometheus::Registry;
use velo::observability::test_helpers::MetricSnapshot;

/// Independent oracle counters. All scenarios use this surface; the assertion
/// helpers know which fields to check for which scenario family.
pub struct Oracle {
    /// Things the scenario asked the runtime to send.
    pub sent: AtomicU64,
    /// Things the receiver actually observed.
    pub received: AtomicU64,
    /// `on_error` callback invocations (transport-side delivery failure).
    pub on_error_seen: AtomicU64,
    /// SendBackpressure futures observed (resolved or dropped).
    pub backpressure_seen: AtomicU64,
    /// Faults the scenario *intentionally* injected. Used to back-out from
    /// strict equality on counters.
    pub fault_injected: AtomicU64,
    /// Local-side handler invocations (set by handler closures themselves).
    pub handler_invocations: AtomicU64,
    /// Local-side handler panics observed.
    pub handler_panics: AtomicU64,
}

impl Default for Oracle {
    fn default() -> Self {
        Self::new()
    }
}

impl Oracle {
    pub fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            on_error_seen: AtomicU64::new(0),
            backpressure_seen: AtomicU64::new(0),
            fault_injected: AtomicU64::new(0),
            handler_invocations: AtomicU64::new(0),
            handler_panics: AtomicU64::new(0),
        }
    }

    pub fn arc() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }
    pub fn on_error_seen(&self) -> u64 {
        self.on_error_seen.load(Ordering::Relaxed)
    }
    pub fn backpressure_seen(&self) -> u64 {
        self.backpressure_seen.load(Ordering::Relaxed)
    }
    pub fn fault_injected(&self) -> u64 {
        self.fault_injected.load(Ordering::Relaxed)
    }
    pub fn handler_invocations(&self) -> u64 {
        self.handler_invocations.load(Ordering::Relaxed)
    }

    pub fn inc_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_on_error(&self) {
        self.on_error_seen.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_backpressure(&self) {
        self.backpressure_seen.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_fault(&self) {
        self.fault_injected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_handler_invocations(&self) {
        self.handler_invocations.fetch_add(1, Ordering::Relaxed);
    }

    /// `sent` minus the faults we expected to lose. The harness scenario
    /// reports the difference; gauges and counters are sanity-checked against it.
    pub fn expected_received(&self) -> u64 {
        self.sent().saturating_sub(self.fault_injected())
    }

    /// Strict throughput-only check: every send must produce a receive.
    pub fn assert_clean_throughput(&self) -> Result<()> {
        let sent = self.sent();
        let recv = self.received();
        if sent != recv {
            return Err(anyhow!(
                "clean-throughput parity broken: sent={sent} received={recv} (delta={})",
                sent.abs_diff(recv)
            ));
        }
        Ok(())
    }

    /// Allow `fault_injected` to absorb the gap. We accept that some sends
    /// were intentionally lost; we still require received + faults == sent.
    pub fn assert_with_faults(&self) -> Result<()> {
        let sent = self.sent();
        let recv = self.received();
        let faults = self.fault_injected();
        if recv + faults != sent {
            return Err(anyhow!(
                "fault-mode parity broken: sent={sent} received={recv} fault_injected={faults} \
                 (received + fault_injected should equal sent)"
            ));
        }
        Ok(())
    }
}

/// What a scenario reports back at the end of `run()`.
pub struct ScenarioReport {
    pub id: &'static str,
    pub elapsed: Duration,
    pub sent: u64,
    pub received: u64,
    pub fault_injected: u64,
    pub on_error_seen: u64,
    pub backpressure_seen: u64,
    pub note: String,
}

impl ScenarioReport {
    pub fn from_oracle(
        id: &'static str,
        elapsed: Duration,
        oracle: &Oracle,
        note: impl Into<String>,
    ) -> Self {
        Self {
            id,
            elapsed,
            sent: oracle.sent(),
            received: oracle.received(),
            fault_injected: oracle.fault_injected(),
            on_error_seen: oracle.on_error_seen(),
            backpressure_seen: oracle.backpressure_seen(),
            note: note.into(),
        }
    }
}

/// Read a counter scoped to a transport label. The runtime emits frame counts
/// per `(direction, message_type, outcome)` — we sum the `outcome=ok`
/// inbound rows for a quick "messages routed" estimate. Useful as a lower bound
/// on what the runtime saw, but the oracle-side `received` counter is the
/// authoritative receipt count (it's incremented inside handler closures, not
/// in the transport adapter).
pub fn frames_total_inbound(snap: &MetricSnapshot, transport: &str) -> f64 {
    snap.counter(
        "velo_transport_frames_total",
        &[
            ("transport", transport),
            ("direction", "inbound"),
            ("message_type", "message"),
            ("outcome", "ok"),
        ],
    )
}

/// Active-anchors gauge — must return to zero after teardown.
pub fn streaming_active_anchors(snap: &MetricSnapshot) -> f64 {
    snap.gauge("velo_streaming_active_anchors", &[])
}

/// Active-rendezvous-slots gauge — must return to zero after teardown.
pub fn rendezvous_active_slots(snap: &MetricSnapshot) -> f64 {
    snap.gauge("velo_rendezvous_active_slots", &[])
}

/// Pending-responses gauge — must stay bounded, not grow unbounded.
pub fn messenger_pending_responses(snap: &MetricSnapshot) -> f64 {
    snap.gauge("velo_messenger_pending_responses", &[])
}

/// Saturation indicator — we expect this to be exactly zero in non-saturating
/// runs (M2/M3); a non-zero value means the response slot table ran out and
/// the scenario should be re-tuned.
pub fn messenger_response_slot_exhausted_total(snap: &MetricSnapshot) -> f64 {
    snap.counter("velo_messenger_response_slot_exhausted_total", &[])
}

/// Snapshot a registry. Always succeeds — the runtime's metric families are
/// gathered into a `Vec<MetricFamily>` we can then query by name+label.
pub fn snapshot(registry: &Registry) -> MetricSnapshot {
    MetricSnapshot::from_registry(registry)
}

/// Standard post-run check: runtime gauges have returned to baseline.
///
/// Caller passes both server and client snapshots — gauges live on whichever
/// side did the work, so we sum both.
pub fn assert_gauges_at_baseline(server: &MetricSnapshot, client: &MetricSnapshot) -> Result<()> {
    let active_anchors = streaming_active_anchors(server) + streaming_active_anchors(client);
    let active_slots = rendezvous_active_slots(server) + rendezvous_active_slots(client);
    let pending_resp = messenger_pending_responses(server) + messenger_pending_responses(client);

    if active_anchors > 0.0 {
        return Err(anyhow!(
            "post-run: velo_streaming_active_anchors={active_anchors} (expected 0)"
        ));
    }
    if active_slots > 0.0 {
        return Err(anyhow!(
            "post-run: velo_rendezvous_active_slots={active_slots} (expected 0)"
        ));
    }
    if pending_resp > 0.0 {
        return Err(anyhow!(
            "post-run: velo_messenger_pending_responses={pending_resp} (expected 0)"
        ));
    }
    Ok(())
}

/// Settle with retries — runtime gauges decrement asynchronously when
/// senders/anchors finalize. Polls up to `total` with `step` between tries.
pub async fn settle_until<F: Fn() -> Result<()>>(
    total: Duration,
    step: Duration,
    check: F,
) -> Result<()> {
    let deadline = std::time::Instant::now() + total;
    let mut last = check();
    while last.is_err() && std::time::Instant::now() < deadline {
        tokio::time::sleep(step).await;
        last = check();
    }
    last.with_context(|| "settle_until: condition never held within budget")
}
