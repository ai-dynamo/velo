// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! When the staged batch goes out — the whole of [`FlushPolicy`] in one place.
//!
//! The batcher stages records; this decides whether the batch it has is written
//! at the end of the current wake. It is a separate component for the same
//! reason [`super::writer`] is: it knows nothing about slots or epochs, only
//! about what has been staged, one record class it tracks by name — a pending
//! `CreditUpdate` reply, because it alone gets its own bounded wait — and what
//! the policy says about the rest, so the policy cannot quietly acquire a
//! second implementation in a branch somewhere on the data path.
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
//! `MuxConfig::async_open_ack`'s detached open bypasses this gate entirely — it
//! dispatches straight to the transport, so nothing here decides when it goes.
//!
//! [`MuxConfig::reply_linger`]: super::super::MuxConfig::reply_linger
//!
//! ## Going before the window: a complete batch, a full batch
//!
//! A linger window runs from the batch's oldest record, so a record that is
//! alone on its lane waits the whole window for company that will not come. Two
//! opt-in conditions end the wait when waiting cannot help:
//!
//! - [`AutoFlush::on_complete`]: every live slot already has a data record in
//!   the batch. More waiting could only add a second record from one of them.
//!   The batch then goes at the later of its oldest record and one window
//!   after the last write, which keeps a single fast stream at one write per
//!   window. Counting slots needs the batcher's slot table, which this gate
//!   does not have: the batcher marks each slot with [`FlushGate::batch_gen`]
//!   when it stages for it, and passes the live count in a [`FlushContext`].
//! - [`AutoFlush::at_bytes`]: the batch has reached a size past which batching
//!   saves no packets.
//!
//! Neither is a reason for the idle-lane rule "write at once when the lane has
//! been quiet": a forward pass's records reach the batcher over a fraction of a
//! millisecond, so that rule writes the first of them alone and the rest a
//! window later, which is two writes per pass where the window made one.
//!
//! [`AutoFlush::on_complete`]: super::super::AutoFlush::on_complete
//! [`AutoFlush::at_bytes`]: super::super::AutoFlush::at_bytes
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
    /// When this batcher last wrote a batch.
    ///
    /// Only tracked under [`AutoFlush::on_complete`] with a linger window,
    /// the one place that reads it: a complete batch may go early, but no
    /// sooner than one window after this.
    ///
    /// [`AutoFlush::on_complete`]: super::super::AutoFlush::on_complete
    last_write: Option<Instant>,
    /// Names the open batch, so a slot can tell whether it already has a record
    /// in it ([`Self::batch_gen`]). Starts at 1 so that a new slot, marked 0,
    /// is never taken to be in the first batch.
    batch_gen: u64,
    /// Distinct slots with a data record in the open batch.
    slots_in_batch: usize,
}

/// What the batcher knows at the moment of a flush decision and this gate
/// does not: how many slots could still add to the batch, and how big it is.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct FlushContext {
    /// Slots open on this (peer, lane).
    pub(super) live_slots: usize,
    /// Encoded size of the open batch, header included.
    pub(super) batch_bytes: usize,
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
            last_write: None,
            batch_gen: 1,
            slots_in_batch: 0,
        }
    }

    /// The open batch's name, for [`Self::stage_data`]'s caller to mark the
    /// slot it stages for.
    pub(super) const fn batch_gen(&self) -> u64 {
        self.batch_gen
    }

    /// A slot with a data record in the open batch closed. It no longer counts
    /// toward a complete batch: it is not live, and the batch must not go
    /// early because of a record from a slot that is gone.
    pub(super) fn slot_left_batch(&mut self) {
        self.slots_in_batch = self.slots_in_batch.saturating_sub(1);
    }

    /// As [`Self::stage`], for one slot's data records. `first_for_slot` says
    /// the slot had no record in this batch yet, which is what
    /// [`AutoFlush::on_complete`](super::super::AutoFlush::on_complete) counts.
    pub(super) fn stage_data(&mut self, count: usize, first_for_slot: bool) {
        if count == 0 {
            return;
        }
        if first_for_slot {
            self.slots_in_batch += 1;
        }
        self.stage(count);
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
    /// The deadline arm compares *state* against [`Self::deadline`] rather
    /// than reacting to a timer firing: a wake from a slot record can easily
    /// arrive after the deadline passed without the timer arm ever being
    /// selected, and a batch that is late goes out on whichever wake notices,
    /// not only a `linger` one. Written as `deadline().is_some_and(..)`
    /// rather than re-deriving the two clocks by hand, so the two functions
    /// cannot drift out of agreement about which one binds.
    pub(super) fn should_flush(&self, ctx: FlushContext) -> bool {
        self.urgent
            || (self.policy.on_admission() && !self.replies_only())
            || self
                .policy
                .at_bytes()
                .is_some_and(|bytes| self.staged > 0 && ctx.batch_bytes >= bytes)
            || self.deadline(ctx).is_some_and(|due| due <= Instant::now())
    }

    /// When the open batch is due, for the batcher's select to park on.
    ///
    /// `None` means nothing is running: no reply is pending and either no
    /// linger window is configured or nothing is staged to run one against.
    /// A reply's own due time and the policy window are independent clocks —
    /// a batch may be running both at once — so this is the earlier of the
    /// two that are actually armed, not one replacing the other.
    ///
    /// A complete batch (see [`Self::complete`]) is due at the later of its
    /// oldest record and one window after the last write. That is never later
    /// than the plain window, because the last write came before the oldest
    /// record.
    pub(super) fn deadline(&self, ctx: FlushContext) -> Option<Instant> {
        let reply_due = self.replies_since.map(|since| since + self.reply_linger);
        let policy_due = match (self.since, self.linger_window()) {
            (Some(since), Some(window)) if self.complete(ctx) => Some(
                self.last_write
                    .map_or(since, |last| since.max(last + window)),
            ),
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
    /// `false` on an empty batch, and for free: `replies_since` is `None`
    /// whenever nothing is staged, so an empty batch never reads as
    /// replies-only without a separate emptiness check. Not that it would
    /// matter if it did — an empty batch writes nothing either way
    /// ([`writer::flush`](super::writer::BatchWriter::flush) no-ops on an
    /// absent or empty encoder) — but a decision this close to the reply
    /// window should not depend on an accident of what the batch last held.
    fn replies_only(&self) -> bool {
        self.replies_since.is_some() && !self.non_reply_staged
    }

    /// Whether the batch holds a data record from every live slot, under a
    /// policy that lets such a batch go early.
    ///
    /// Credit replies and closes do not count, so a replies-only batch is never
    /// complete. A slot that closes leaves the count
    /// ([`Self::slot_left_batch`]), so the two sides stay equal; `>=` only
    /// keeps a miscount from holding a batch back.
    fn complete(&self, ctx: FlushContext) -> bool {
        self.policy.on_complete() && ctx.live_slots > 0 && self.slots_in_batch >= ctx.live_slots
    }

    /// The open batch was written.
    pub(super) fn cleared(&mut self) {
        if self.policy.on_complete() && self.linger_window().is_some() {
            self.last_write = Some(Instant::now());
        }
        self.forget_staged();
    }

    /// The open batch was thrown away — an epoch death, or the task exiting.
    ///
    /// Distinct from [`Self::cleared`] only in what it means: nothing reached
    /// the wire. It matters because the staged gauge *is* the forgotten-flush
    /// signal, so a discarded batch that kept its count would read as an
    /// application that stopped flushing.
    ///
    /// A pending credit reply discarded here used to be credit lost for good.
    /// It is not any more: this file knows nothing about slots, credit or
    /// epochs, so the caller keeps its own copy of what it staged and hands it
    /// back — see [`epoch_death`](super::Batcher::epoch_death) and
    /// `FlowControl::take_pending_grant`. Pinned by
    /// `super::tests::reply_linger::epoch_death_returns_the_credit_its_discarded_batch_carried`.
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
        self.slots_in_batch = 0;
        self.batch_gen = self.batch_gen.wrapping_add(1);
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

    /// Context for the tests of rules that do not read it.
    const CTX: FlushContext = FlushContext {
        live_slots: 0,
        batch_bytes: 0,
    };

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
            ..AutoFlush::default()
        })
    }

    #[test]
    fn auto_on_admission_writes_whatever_it_has() {
        let mut gate = gate(FlushPolicy::default());
        assert!(
            gate.should_flush(CTX),
            "an empty wake still ends in a flush"
        );
        gate.stage(1);
        assert!(gate.should_flush(CTX));
        assert_eq!(gate.deadline(CTX), None, "and never runs a timer");
    }

    #[test]
    fn manual_holds_ordinary_records() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage(32);
        assert!(
            !gate.should_flush(CTX),
            "manual means the application decides"
        );
        assert_eq!(
            gate.deadline(CTX),
            None,
            "and there is no window to rescue it"
        );
    }

    #[test]
    fn manual_never_holds_a_record_that_carries_liveness() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage(4);
        gate.stage_urgent(1);
        assert!(
            gate.should_flush(CTX),
            "a close or a terminal moves whatever the policy says"
        );
    }

    #[test]
    fn credit_replies_alone_wait_for_the_reply_window() {
        let window = Duration::from_millis(50);
        let mut gate = gate_with_reply_linger(FlushPolicy::default(), window);
        gate.stage_reply(1);
        assert!(
            !gate.should_flush(CTX),
            "on_admission does not write a batch that holds only credit replies"
        );
        let due = gate.deadline(CTX).expect("the reply window is running");
        gate.stage_reply(3);
        assert_eq!(
            gate.deadline(CTX),
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
            gate.deadline(CTX).is_some(),
            "a record joining must not cancel the reply's window"
        );

        // Data first, so the old code's `if self.staged == 0` guard on
        // `stage_reply` never fired and the window never started at all.
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(1));
        gate.stage(1);
        gate.stage_reply(1);
        assert!(
            gate.deadline(CTX).is_some(),
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
        assert!(!gate.should_flush(CTX), "replies-only still waits");
        let reply_due = gate.deadline(CTX).expect("the reply window is running");

        gate.stage(1);
        assert!(
            gate.should_flush(CTX),
            "on_admission writes the mixed batch at once, reply included"
        );
        assert_eq!(
            gate.deadline(CTX),
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
            !gate.should_flush(CTX),
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
            gate.should_flush(CTX),
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
        let due = gate.deadline(CTX).expect("a window is running");
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
            gate.should_flush(CTX),
            "the pre-window behaviour: a reply moves at once"
        );
        assert!(
            gate.deadline(CTX).is_some_and(|due| due <= Instant::now()),
            "a zero window is due immediately rather than unset — should_flush \
             is already true either way, so the batcher never waits on this"
        );
    }

    #[test]
    fn a_written_batch_ends_the_reply_window() {
        let mut gate = gate_with_reply_linger(FlushPolicy::Manual, Duration::from_millis(50));
        gate.stage_reply(1);
        gate.cleared();
        assert_eq!(gate.deadline(CTX), None);
        assert!(!gate.should_flush(CTX));
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
        assert_eq!(gate.deadline(CTX), None, "nothing staged, nothing due");
        gate.stage(1);
        let first = gate.deadline(CTX).expect("a window is running");
        gate.stage(1);
        assert_eq!(
            gate.deadline(CTX),
            Some(first),
            "the second record does not restart the window the first started"
        );
        assert!(!gate.should_flush(CTX), "and it has not elapsed yet");
    }

    #[test]
    fn a_written_batch_starts_the_window_again() {
        let mut gate = gate(auto(false, Some(Duration::from_millis(50))));
        gate.stage(1);
        gate.cleared();
        assert_eq!(gate.deadline(CTX), None);
        assert!(!gate.should_flush(CTX), "an empty batch is never due");
    }

    #[test]
    fn a_discarded_batch_leaves_nothing_staged() {
        let mut gate = gate(FlushPolicy::Manual);
        gate.stage_urgent(8);
        gate.discarded();
        assert!(
            !gate.should_flush(CTX),
            "an epoch death takes the urgency with the records it applied to"
        );
    }

    // -----------------------------------------------------------------------
    // on_complete and at_bytes
    // -----------------------------------------------------------------------

    const WINDOW: Duration = Duration::from_millis(50);

    fn windowed(on_complete: bool, at_bytes: Option<usize>) -> FlushPolicy {
        FlushPolicy::Auto(AutoFlush {
            on_admission: false,
            max_linger: Some(WINDOW),
            on_complete,
            at_bytes,
        })
    }

    const fn live(live_slots: usize) -> FlushContext {
        FlushContext {
            live_slots,
            batch_bytes: 0,
        }
    }

    /// A gate that wrote a batch and then saw nothing for `quiet`.
    async fn written_then_quiet(policy: FlushPolicy, quiet: Duration) -> FlushGate {
        let mut gate = gate(policy);
        gate.stage_data(1, true);
        gate.cleared();
        tokio::time::advance(quiet).await;
        gate
    }

    /// The c1 case: one stream on the lane, and its record arrives long after
    /// the last write. Nothing else can join the batch, so the record goes at
    /// once instead of waiting the window alone.
    #[tokio::test(start_paused = true)]
    async fn a_complete_batch_on_a_quiet_lane_is_due_at_once() {
        let mut gate = written_then_quiet(windowed(true, None), WINDOW * 3).await;
        gate.stage_data(1, true);
        assert!(
            gate.should_flush(live(1)),
            "the only live slot is in the batch"
        );
        assert_eq!(gate.deadline(live(1)), Some(Instant::now()));
    }

    /// Before the first write there is no spacing to keep.
    #[tokio::test(start_paused = true)]
    async fn a_complete_batch_before_any_write_is_due_at_once() {
        let mut gate = gate(windowed(true, None));
        gate.stage_data(1, true);
        assert!(gate.should_flush(live(1)));
    }

    /// A stream that sends faster than the window still gets one write per
    /// window: a complete batch staged right behind a write waits until one
    /// window after that write. That is still sooner than the plain window,
    /// which would run from this batch's own oldest record.
    #[tokio::test(start_paused = true)]
    async fn a_complete_batch_waits_one_window_from_the_last_write() {
        let mut gate = gate(windowed(true, None));
        gate.stage_data(1, true);
        gate.cleared();
        let written = Instant::now();
        tokio::time::advance(WINDOW / 5).await;

        gate.stage_data(1, true);
        let staged = Instant::now();
        assert!(!gate.should_flush(live(1)), "one write per window, at most");
        assert_eq!(gate.deadline(live(1)), Some(written + WINDOW));
        assert!(
            written + WINDOW < staged + WINDOW,
            "never later than the plain window"
        );

        tokio::time::advance(WINDOW - WINDOW / 5).await;
        assert!(gate.should_flush(live(1)));
    }

    /// Many streams on a lane: while one has nothing staged, the batch is not
    /// complete and the window runs from the oldest record, however quiet the
    /// lane was. This is what keeps a busy lane, where some streams are always
    /// in prefill or between turns, at one write per window. A second record
    /// from a slot already in the batch does not count as another slot.
    #[tokio::test(start_paused = true)]
    async fn an_incomplete_batch_keeps_the_full_window() {
        let mut gate = written_then_quiet(windowed(true, None), WINDOW * 3).await;
        gate.stage_data(1, true);
        let staged = Instant::now();
        gate.stage_data(1, false);
        assert!(
            !gate.should_flush(live(2)),
            "one of two live slots has nothing staged"
        );
        assert_eq!(gate.deadline(live(2)), Some(staged + WINDOW));
    }

    /// The condition is opt-in: without it, a lone record waits the window as it
    /// always did.
    #[tokio::test(start_paused = true)]
    async fn without_on_complete_a_lone_record_waits_the_window() {
        let mut gate = written_then_quiet(windowed(false, None), WINDOW * 3).await;
        gate.stage_data(1, true);
        let staged = Instant::now();
        assert!(!gate.should_flush(live(1)));
        assert_eq!(gate.deadline(live(1)), Some(staged + WINDOW));
    }

    /// A credit reply is not a slot's record, so a batch holding only replies is
    /// never complete; it keeps its own reply window.
    #[tokio::test(start_paused = true)]
    async fn credit_replies_alone_never_make_a_batch_complete() {
        let reply_linger = Duration::from_millis(20);
        let mut gate = gate_with_reply_linger(windowed(true, None), reply_linger);
        gate.stage_reply(1);
        let staged = Instant::now();
        assert!(!gate.should_flush(live(1)));
        assert_eq!(gate.deadline(live(1)), Some(staged + reply_linger));
    }

    /// Past a segment's worth of bytes, batching saves no packets, so a batch at
    /// `at_bytes` goes without waiting for the window. An empty batch is never
    /// due by size, even though its header alone has a length.
    #[test]
    fn a_batch_at_the_byte_threshold_is_due_at_once() {
        let at = |batch_bytes| FlushContext {
            live_slots: 4,
            batch_bytes,
        };
        let mut gate = gate(windowed(false, Some(1000)));
        assert!(
            !gate.should_flush(at(1000)),
            "nothing staged, nothing to write"
        );
        gate.stage_data(1, true);
        assert!(
            !gate.should_flush(at(999)),
            "below the threshold the window applies"
        );
        assert!(gate.should_flush(at(1000)));
    }
}
