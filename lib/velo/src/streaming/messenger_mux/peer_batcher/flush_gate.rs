// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! When the staged batch goes out — the whole of [`FlushPolicy`] in one place.
//!
//! The batcher stages records; this decides whether the batch it has is written
//! at the end of the current wake. It is a separate component for the same
//! reason [`super::writer`] is: it knows nothing about slots, credit or epochs,
//! only about what has been staged and what the policy says about that, so the
//! policy cannot quietly acquire a second implementation in a branch somewhere
//! on the data path.
//!
//! ## The two things that are not policy
//!
//! Two reasons to write survive every policy, and neither is asked about here
//! because neither is a decision:
//!
//! - **A batch at a clamp goes.** `emit_data`, `push_close` and `push_reply`
//!   flush inline when a record will not fit, because holding a full batch buys
//!   nothing — there is no room left to batch into.
//! - **Records that carry liveness go.** `OpenSlot` has its own eager flush, and
//!   a `CloseSlot` or a terminal staged into the batch marks it
//!   [`urgent`](FlushGate::stage_urgent).
//!
//! A `CreditUpdate` is the third liveness record, and it gets its own, bounded,
//! treatment: [`stage_reply`](FlushGate::stage_reply) lets a batch that holds
//! nothing but credit replies form for [`MuxConfig::reply_linger`] before it
//! goes, whatever the policy says otherwise. Holding a reply for good would
//! starve a peer's sender with nothing left to rescue it, which is why no
//! policy may hold one; holding it for a millisecond costs that sender
//! `reply_linger / initial_credit` per record and buys the receiver one batch
//! per sweep visit instead of one per reply. `Duration::ZERO` makes a reply
//! urgent again.
//!
//! [`MuxConfig::reply_linger`]: super::super::MuxConfig::reply_linger
//!
//! ## The kick, and why it outlives a clamp
//!
//! `flush_batch()` arrives as coalesced control and sets [`FlushGate::kick`].
//! That flag is **not** cleared by [`FlushGate::cleared`], and the asymmetry is
//! load-bearing: a wake that stages 100 records may hit a clamp on record 60 and
//! flush inline, and if that flush consumed the kick the remaining 40 would sit
//! staged until the *next* one. The kick is consumed once, by the batcher, at
//! the end of the wake — after which every record the application queued before
//! it has been drained and written.
//!
//! ## Manual has no timer
//!
//! Under [`FlushPolicy::Manual`] the application owns the flush, and there is no
//! window that eventually rescues it. A producer that stops calling
//! `flush_batch` leaves its last records staged; they are bounded by the same
//! clamps as any batch, so this costs latency and not memory, and
//! `velo_streaming_mux_staged_records` is where an operator sees it. The linger
//! window lives in [`AutoFlush::max_linger`](super::super::AutoFlush) instead,
//! where it is a condition the batcher was *asked* to apply rather than a net
//! under one it was told not to.

use std::time::Duration;

use tokio::time::Instant;

use super::super::FlushPolicy;
use crate::observability::MuxMetricsHandle;

/// The flush decision for one peer batcher.
pub(super) struct FlushGate {
    policy: FlushPolicy,
    metrics: Option<MuxMetricsHandle>,
    /// Records in the batch the writer has open.
    staged: usize,
    /// One of those records must move regardless of policy.
    urgent: bool,
    /// An application flush arrived and has not been served.
    kicked: bool,
    /// When the open batch's oldest record was staged.
    ///
    /// Only tracked under a linger window: taking a timestamp per record would
    /// otherwise be a clock read on the hot path for a number nothing reads.
    since: Option<Instant>,
    /// How long a batch of nothing but credit replies may form.
    reply_linger: Duration,
    /// When the open batch's first credit reply was staged, while every record
    /// in it is one. `None` once anything else joins, and whenever the batch
    /// is empty.
    replies_since: Option<Instant>,
}

impl FlushGate {
    pub(super) fn new(
        policy: FlushPolicy,
        reply_linger: Duration,
        metrics: Option<MuxMetricsHandle>,
    ) -> Self {
        Self {
            policy,
            metrics,
            staged: 0,
            urgent: false,
            kicked: false,
            since: None,
            reply_linger,
            replies_since: None,
        }
    }

    /// Note `count` records appended to the open batch.
    pub(super) fn stage(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        // Anything that is not a credit reply ends the replies-only hold: the
        // batch now carries a record the policy has its own answer for.
        self.replies_since = None;
        self.stage_records(count);
    }

    /// As [`Self::stage`], for records that must move whatever the policy says:
    /// a close, a terminal.
    pub(super) fn stage_urgent(&mut self, count: usize) {
        self.stage(count);
        self.urgent = true;
    }

    /// Note `count` credit replies appended to the open batch.
    ///
    /// A batch that holds nothing else may form for the reply window (module
    /// docs); with the window at zero this is [`Self::stage_urgent`].
    pub(super) fn stage_reply(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        if self.reply_linger.is_zero() {
            self.stage_urgent(count);
            return;
        }
        if self.staged == 0 {
            self.replies_since = Some(Instant::now());
        }
        self.stage_records(count);
    }

    fn stage_records(&mut self, count: usize) {
        if self.staged == 0 && self.linger_window().is_some() {
            self.since = Some(Instant::now());
        }
        self.staged += count;
        if let Some(metrics) = &self.metrics {
            metrics.staged_records_delta(count as i64);
        }
    }

    /// An application called `flush_batch`.
    pub(super) fn kick(&mut self) {
        self.kicked = true;
    }

    /// Take the pending kick, if there is one.
    ///
    /// Separate from [`Self::cleared`] on purpose — see the module docs: an
    /// inline clamp flush must not be able to swallow an application's flush and
    /// strand whatever was staged after it.
    pub(super) fn take_kick(&mut self) -> bool {
        std::mem::take(&mut self.kicked)
    }

    /// Whether the policy itself asks for the open batch to be written now.
    ///
    /// The linger arm compares *state* rather than reacting to a timer firing:
    /// a wake from a slot record can easily arrive after the deadline passed
    /// without the timer arm ever being selected, and a batch that is late is
    /// late however the batcher happened to wake up.
    pub(super) fn should_flush(&self) -> bool {
        if self.urgent {
            return true;
        }
        if let Some(since) = self.replies_since {
            return since.elapsed() >= self.reply_linger;
        }
        if self.policy.on_admission() {
            return true;
        }
        match (self.since, self.linger_window()) {
            (Some(since), Some(window)) => since.elapsed() >= window,
            _ => false,
        }
    }

    /// When the open batch is due, for the batcher's select to park on.
    ///
    /// `None` means nothing is running: either no window is configured or
    /// nothing is staged to run one against.
    pub(super) fn deadline(&self) -> Option<Instant> {
        if let Some(since) = self.replies_since {
            return Some(since + self.reply_linger);
        }
        match (self.since, self.linger_window()) {
            (Some(since), Some(window)) => Some(since + window),
            _ => None,
        }
    }

    /// The open batch was written.
    pub(super) fn cleared(&mut self) {
        self.forget_staged();
    }

    /// The open batch was thrown away — an epoch death, or the task exiting.
    ///
    /// Distinct from [`Self::cleared`] only in what it means: nothing reached
    /// the wire. It matters because the staged gauge *is* the forgotten-flush
    /// signal, so a discarded batch that kept its count would read as an
    /// application that stopped flushing.
    pub(super) fn discarded(&mut self) {
        self.forget_staged();
    }

    fn forget_staged(&mut self) {
        if self.staged > 0
            && let Some(metrics) = &self.metrics
        {
            metrics.staged_records_delta(-(self.staged as i64));
        }
        self.staged = 0;
        self.urgent = false;
        self.since = None;
        self.replies_since = None;
    }

    const fn linger_window(&self) -> Option<Duration> {
        self.policy.max_linger()
    }
}

/// Park until `deadline`, or forever when there is none.
///
/// The `None` arm is [`std::future::pending`] rather than a far-future sleep so
/// a batcher under a policy with no window registers no timer at all.
pub(super) async fn linger_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::AutoFlush;
    use super::*;

    fn gate(policy: FlushPolicy) -> FlushGate {
        FlushGate::new(policy, Duration::ZERO, None)
    }

    fn gate_with_reply_linger(policy: FlushPolicy, window: Duration) -> FlushGate {
        FlushGate::new(policy, window, None)
    }

    fn auto(on_admission: bool, max_linger: Option<Duration>) -> FlushPolicy {
        FlushPolicy::Auto(AutoFlush {
            on_admission,
            max_linger,
        })
    }

    #[test]
    fn auto_on_admission_writes_whatever_it_has() {
        let mut gate = gate(FlushPolicy::default());
        assert!(gate.should_flush(), "an empty wake still ends in a flush");
        gate.stage(1);
        assert!(gate.should_flush());
        assert_eq!(gate.deadline(), None, "and never runs a timer");
    }

    #[test]
    fn manual_holds_ordinary_records() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage(32);
        assert!(!gate.should_flush(), "manual means the application decides");
        assert_eq!(gate.deadline(), None, "and there is no window to rescue it");
    }

    #[test]
    fn manual_never_holds_a_record_that_carries_liveness() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage(4);
        gate.stage_urgent(1);
        assert!(
            gate.should_flush(),
            "a close or a terminal moves whatever the policy says"
        );
    }

    #[test]
    fn credit_replies_alone_wait_for_the_reply_window() {
        let window = Duration::from_millis(50);
        let mut gate = gate_with_reply_linger(FlushPolicy::default(), window);
        gate.stage_reply(1);
        assert!(
            !gate.should_flush(),
            "on_admission does not write a batch that holds only credit replies"
        );
        let due = gate.deadline().expect("the reply window is running");
        gate.stage_reply(3);
        assert_eq!(
            gate.deadline(),
            Some(due),
            "later replies join the window the first started"
        );
    }

    #[test]
    fn anything_else_ends_the_reply_hold() {
        let mut gate = gate_with_reply_linger(FlushPolicy::default(), Duration::from_millis(50));
        gate.stage_reply(2);
        gate.stage(1);
        assert!(
            gate.should_flush(),
            "a data record puts the batch back under the policy"
        );
        assert_eq!(gate.deadline(), None, "and the reply timer is gone with it");

        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(50));
        gate.stage_reply(2);
        gate.stage_urgent(1);
        assert!(
            gate.should_flush(),
            "a close moves the replies staged before it"
        );
    }

    #[test]
    fn a_zero_reply_window_makes_a_reply_urgent() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage_reply(1);
        assert!(
            gate.should_flush(),
            "the pre-window behaviour: a reply moves at once"
        );
        assert_eq!(gate.deadline(), None);
    }

    #[test]
    fn a_written_batch_ends_the_reply_window() {
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(50));
        gate.stage_reply(1);
        gate.cleared();
        assert_eq!(gate.deadline(), None);
        assert!(!gate.should_flush());
    }

    #[test]
    fn a_kick_survives_an_inline_clamp_flush() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage(60);
        gate.kick();
        // The clamp bound mid-wake and the writer cut the batch.
        gate.cleared();
        gate.stage(40);
        assert!(
            gate.take_kick(),
            "the tail of the pass must not be stranded by a flush the app did not ask for"
        );
        assert!(!gate.take_kick(), "and one kick is served once");
    }

    #[test]
    fn a_linger_window_runs_from_the_oldest_staged_record() {
        let mut gate = gate(auto(false, Some(Duration::from_millis(50))));
        assert_eq!(gate.deadline(), None, "nothing staged, nothing due");
        gate.stage(1);
        let first = gate.deadline().expect("a window is running");
        gate.stage(1);
        assert_eq!(
            gate.deadline(),
            Some(first),
            "the second record does not restart the window the first started"
        );
        assert!(!gate.should_flush(), "and it has not elapsed yet");
    }

    #[test]
    fn a_written_batch_starts_the_window_again() {
        let mut gate = gate(auto(false, Some(Duration::from_millis(50))));
        gate.stage(1);
        gate.cleared();
        assert_eq!(gate.deadline(), None);
        assert!(!gate.should_flush(), "an empty batch is never due");
    }

    #[test]
    fn a_discarded_batch_leaves_nothing_staged() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage_urgent(8);
        gate.discarded();
        assert!(
            !gate.should_flush(),
            "an epoch death takes the urgency with the records it applied to"
        );
    }
}
