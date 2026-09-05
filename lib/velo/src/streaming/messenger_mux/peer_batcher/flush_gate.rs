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
//! treatment: [`stage_reply`](FlushGate::stage_reply) starts a window of
//! [`MuxConfig::reply_linger`] the moment a reply with no window already
//! running joins the batch, and it is a property of *that reply*, not of the
//! batch staying replies-only. What a data record, a close or a terminal
//! joining afterward does to that window depends on the policy: under
//! [`AutoFlush::on_admission`](super::super::AutoFlush::on_admission), it ends
//! the wait at once, data included, because the batch is no longer
//! replies-only and `on_admission` already has its own reason to write
//! anything non-empty; under [`FlushPolicy::Manual`] and
//! `Auto { on_admission: false }`, nothing about ordinary staging cuts the
//! wait short, so the joining record rides out in the same write the reply's
//! own window ends. Neither case resets or cancels the window itself — only
//! [`Self::cleared`] or [`Self::discarded`] do that.
//! Holding a reply for good would starve a peer's sender with nothing left to
//! rescue it, which is why no policy may hold one past its window; holding it
//! for a millisecond costs that sender `reply_linger / initial_credit` per
//! record and buys the receiver one batch per sweep visit instead of one per
//! reply. A policy's own linger window keeps running underneath and can still
//! cut the wait short — [`FlushGate::deadline`] takes whichever due time comes
//! first. `Duration::ZERO` makes the window already due, which is
//! [`stage_urgent`](FlushGate::stage_urgent) in effect without a second code
//! path for it.
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
//! ## Manual has no timer for the application's own records
//!
//! Under [`FlushPolicy::Manual`] the application owns the flush for its own
//! records, and there is no window that eventually rescues them. A producer
//! that stops calling `flush_batch` leaves its last records staged; they are
//! bounded by the same clamps as any batch, so this costs latency and not
//! memory, and `velo_streaming_mux_staged_records` is where an operator sees
//! it. The linger window lives in
//! [`AutoFlush::max_linger`](super::super::AutoFlush) instead, where it is a
//! condition the batcher was *asked* to apply rather than a net under one it
//! was told not to. The one record `Manual` does not leave to the
//! application is still the credit reply: [`stage_reply`](FlushGate::stage_reply)
//! gives it [`MuxConfig::reply_linger`] regardless of policy, and that window
//! carries out whatever else is staged alongside it too — a data record
//! joining a pending reply under `Manual` is written with it at the reply's
//! deadline rather than waiting on the application, because nothing on this
//! side of the reply knows it is owed.

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
    /// How long a pending credit reply may wait for a batch to form around it.
    reply_linger: Duration,
    /// When the *oldest unwritten* credit reply was staged.
    ///
    /// Tracks the reply itself, not "the batch holds nothing else" — a data
    /// record, a close or a terminal joining afterwards must not cancel a
    /// pending reply's own bound. Set only while `None` (so a later reply
    /// joins the window the first one started) and cleared only by
    /// [`Self::forget_staged`], on a write or a discard.
    replies_since: Option<Instant>,
    /// Whether anything other than a credit reply has been staged since the
    /// last clear — a data record, a close or a terminal alike, since
    /// [`Self::stage_urgent`] delegates to [`Self::stage`].
    ///
    /// `replies_since.is_some() && !non_reply_staged` is "replies-only": the
    /// one case `on_admission` does not treat as reason enough to write,
    /// because the reply window already owns that decision. A plain `bool`
    /// rather than a second count alongside `staged`, because nothing here
    /// needs the magnitude — only whether the batch is still pure.
    non_reply_staged: bool,
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
            non_reply_staged: false,
        }
    }

    /// Note `count` records appended to the open batch.
    ///
    /// Does not touch the reply window: whether the batch is *still*
    /// replies-only is read back from `non_reply_staged`, set here rather
    /// than invalidated here, so a reply staged before this record keeps the
    /// bound it was given.
    pub(super) fn stage(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        self.non_reply_staged = true;
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
    /// Starts the reply window if none is running yet (module docs). No
    /// special case for a zero window: [`Self::deadline`] and
    /// [`Self::should_flush`] read it as already due, which is
    /// [`Self::stage_urgent`] in effect without a second code path for it.
    pub(super) fn stage_reply(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        if self.replies_since.is_none() {
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

    /// Whether the open batch is due, by policy or by a pending reply's
    /// window.
    ///
    /// The linger arm compares *state* rather than reacting to a timer firing:
    /// a wake from a slot record can easily arrive after the deadline passed
    /// without the timer arm ever being selected, and a batch that is late is
    /// late however the batcher happened to wake up.
    pub(super) fn should_flush(&self) -> bool {
        if self.urgent {
            return true;
        }
        if let Some(since) = self.replies_since
            && since.elapsed() >= self.reply_linger
        {
            return true;
        }
        if self.policy.on_admission() && !self.replies_only() {
            return true;
        }
        match (self.since, self.linger_window()) {
            (Some(since), Some(window)) => since.elapsed() >= window,
            _ => false,
        }
    }

    /// When the open batch is due, for the batcher's select to park on.
    ///
    /// `None` means nothing is running: no reply is pending and either no
    /// linger window is configured or nothing is staged to run one against.
    /// A reply's own due time and the policy window are independent clocks —
    /// a batch may be running both at once — so this is the earlier of the
    /// two that are actually armed, not one replacing the other.
    pub(super) fn deadline(&self) -> Option<Instant> {
        let reply_due = self.replies_since.map(|since| since + self.reply_linger);
        let policy_due = match (self.since, self.linger_window()) {
            (Some(since), Some(window)) => Some(since + window),
            _ => None,
        };
        match (reply_due, policy_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(due), None) | (None, Some(due)) => Some(due),
            (None, None) => None,
        }
    }

    /// Whether every record currently staged is a credit reply.
    ///
    /// `false` on an empty batch: `on_admission` still has to write an empty
    /// wake (module docs), and that decision must not depend on whether the
    /// batch happens to have last held only replies. `replies_since` being
    /// set is what makes an empty batch not count — it is `None` whenever
    /// nothing is staged — so this needs no separate emptiness check.
    fn replies_only(&self) -> bool {
        self.replies_since.is_some() && !self.non_reply_staged
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
    ///
    /// A pending credit reply discarded here is credit lost for good: the
    /// ingress account already zeroed its ungranted delta the moment the
    /// reply record was minted (`take_pending_grant`), and a later reconcile
    /// at the same occupancy has nothing left to re-derive it from. That hole
    /// predates this file and is out of scope here.
    ///
    /// A reply's window is what makes that reachable: a staged reply can sit
    /// unwritten across wakes for up to `reply_linger`, so a discard in that
    /// interval loses it. Every exit but one flushes first — the `stopping`
    /// branch forces a write before it tears down, and `epoch_death` only
    /// runs from a flush that already lost its batch. The exception is
    /// `cancel.cancelled()`, which breaks the run loop straight to
    /// [`Self::discarded`] when the transport goes away; the credit is moot
    /// there, since nothing is left to send it to.
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
        self.non_reply_staged = false;
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

    /// A record joining a batch that already holds a pending reply must not
    /// cancel that reply's window, in either arrival order.
    ///
    /// Regression for a state model that keyed the window on "the batch holds
    /// nothing but credit replies" instead of "a reply is pending": under
    /// `Manual`, a reply that lost its window this way had nothing left to
    /// rescue it — no policy, no timer — which is exactly the unbounded credit
    /// starvation this file's own docs say cannot happen.
    #[test]
    fn a_reply_keeps_its_window_when_a_record_joins_it() {
        // Reply first, so the old code cleared `replies_since` the moment
        // `stage` ran (it unconditionally set it to `None`).
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(1));
        gate.stage_reply(1);
        gate.stage(1);
        assert!(
            gate.deadline().is_some(),
            "a record joining must not cancel the reply's window"
        );

        // Data first, so the old code's `if self.staged == 0` guard on
        // `stage_reply` never fired and the window never started at all.
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(1));
        gate.stage(1);
        gate.stage_reply(1);
        assert!(
            gate.deadline().is_some(),
            "a reply joining a non-empty batch must still start one"
        );
    }

    /// Under `on_admission`, a data record joining a batch that holds a
    /// pending reply ends the wait at once — the reply's own window keeps
    /// running underneath (it is not cancelled), but the batch is no longer
    /// replies-only, so `on_admission`'s own reason to write applies to it
    /// like any other non-empty batch.
    ///
    /// This is the split the module docs (and `MuxConfig::reply_linger`) have
    /// to state explicitly: "data joining does not cut the wait short" holds
    /// under `Manual` and `Auto { on_admission: false }` only.
    #[test]
    fn on_admission_writes_a_mixed_batch_at_once() {
        let mut gate = gate_with_reply_linger(FlushPolicy::default(), Duration::from_millis(50));
        gate.stage_reply(1);
        assert!(!gate.should_flush(), "replies-only still waits");
        let reply_due = gate.deadline().expect("the reply window is running");

        gate.stage(1);
        assert!(
            gate.should_flush(),
            "on_admission writes the mixed batch at once, reply included"
        );
        assert_eq!(
            gate.deadline(),
            Some(reply_due),
            "the reply's own window is not cancelled by the data record — \
             should_flush is true for an independent reason (on_admission), \
             not because the window expired"
        );
    }

    /// Under `Manual`, the same data record must NOT end the wait: with no
    /// `on_admission` and no `max_linger`, the pending reply's own window is
    /// the only thing standing between this batch and unbounded starvation.
    #[test]
    fn manual_does_not_write_a_mixed_batch_before_the_reply_window() {
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(50));
        gate.stage_reply(1);
        gate.stage(1);
        assert!(
            !gate.should_flush(),
            "data joining under Manual must not cut the reply's wait short"
        );
    }

    /// A close or terminal still moves the replies staged before it at once —
    /// unaffected by the reply window surviving other records now.
    #[test]
    fn a_close_still_moves_the_replies_staged_before_it() {
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(50));
        gate.stage_reply(2);
        gate.stage_urgent(1);
        assert!(
            gate.should_flush(),
            "a close moves the replies staged before it"
        );
    }

    /// A policy window shorter than the reply window must still bind a
    /// replies-only batch — the reply's own deadline is a floor, not a
    /// replacement for the policy's.
    #[test]
    fn a_shorter_policy_window_still_binds_a_replies_only_batch() {
        let policy_window = Duration::from_micros(200);
        let reply_linger = Duration::from_millis(50);
        let before = Instant::now();
        let mut gate = gate_with_reply_linger(auto(false, Some(policy_window)), reply_linger);
        gate.stage_reply(1);
        let due = gate.deadline().expect("a window is running");
        assert!(
            due <= before + Duration::from_millis(25),
            "deadline() must take the shorter of the two windows, not just the \
             reply one: got a due time consistent with the {reply_linger:?} \
             reply window instead of the {policy_window:?} policy window"
        );
    }

    #[test]
    fn a_zero_reply_window_flushes_at_once() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage_reply(1);
        assert!(
            gate.should_flush(),
            "the pre-window behaviour: a reply moves at once"
        );
        assert!(
            gate.deadline().is_some_and(|due| due <= Instant::now()),
            "a zero window is due immediately rather than unset — should_flush \
             is already true either way, so the batcher never waits on this"
        );
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
