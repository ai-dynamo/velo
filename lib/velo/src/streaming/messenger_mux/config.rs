// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Construction-time tuning for the mux: [`MuxConfig`] and the flush-policy
//! types it carries.
//!
//! Split out of the transport module because nothing here touches the
//! runtime. These are the values an operator sets and the batchers read, and
//! their documentation is most of what a reader of `MuxConfig` comes for.

use std::time::Duration;

use super::flow_control::{DEFAULT_PEER_BYTE_BUDGET, DEFAULT_SLOT_BYTE_BUDGET};

/// Conditions on which an [`FlushPolicy::Auto`] batcher writes itself.
///
/// A struct rather than more enum variants because these compose: a batcher may
/// hold both, and `BATCHING.md`'s original "opportunistic" and "windowed"
/// policies are the two of them taken one at a time.
///
/// Deliberately **not** `#[non_exhaustive]`, for the same reason [`MuxConfig`]
/// is not: that attribute forbids `AutoFlush { on_admission: false,
/// ..Default::default() }` outside this crate, and the update idiom is worth
/// more than the bump a future condition costs. The version gate is what makes
/// such a bump a decision rather than an accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoFlush {
    /// Write whatever is staged at the end of every wake, having first taken
    /// everything already queued.
    ///
    /// The historical default, and on its own it cannot make anything worse: the batcher
    /// never waits for work that has not arrived, it only notices that more is
    /// *already* there and takes all of it. The name is the mechanism — a flush
    /// parks until the transport admits it, so "at the end of every wake" is in
    /// practice "as soon as the peer admitted the last batch". The one
    /// exception is a batch holding nothing but pending credit replies, which
    /// waits for [`MuxConfig::reply_linger`] under every policy — see that
    /// field for why, and for what changes the moment anything else joins the
    /// batch.
    pub on_admission: bool,
    /// Also write once this long has passed since the oldest staged record.
    ///
    /// `Some(w)` with `on_admission: false` is the windowed policy
    /// `BATCHING.md` specifies: a batch forms for up to `w` and then goes,
    /// trading up to `w` of latency for packing. `None` is no timer at all.
    pub max_linger: Option<Duration>,
}

impl Default for AutoFlush {
    fn default() -> Self {
        Self {
            on_admission: true,
            max_linger: None,
        }
    }
}

impl AutoFlush {
    /// Add a linger window, so a batch also goes out `window` after its oldest
    /// record was staged.
    #[must_use]
    pub const fn with_max_linger(mut self, window: Duration) -> Self {
        self.max_linger = Some(window);
        self
    }
}

/// When a peer batcher writes what it has staged.
///
/// See `BATCHING.md` § "Flush policy". Both policies obey the same two
/// overrides — a batch at its size clamp goes, and the records that carry
/// liveness go (a close or a terminal at once, a credit reply within
/// [`MuxConfig::reply_linger`]) — and under both,
/// [`Velo::flush_batch`](crate::Velo::flush_batch) writes immediately. What
/// they differ on is whether anything *else* does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushPolicy {
    /// The batcher decides, on the conditions in [`AutoFlush`].
    ///
    /// The default is `AutoFlush::default()`, which reproduces the *policy*
    /// every mux had before this knob existed. Reproducing the pre-knob
    /// *behaviour* also needs [`MuxConfig::reply_linger`] at `Duration::ZERO`
    /// — the default is 1 ms, so a credit reply now waits up to that long for
    /// company before this policy would have written it at once.
    Auto(AutoFlush),
    /// The application decides, through
    /// [`Velo::flush_batch`](crate::Velo::flush_batch).
    ///
    /// The policy a serving loop wants: one write per forward pass carrying
    /// that pass's whole fan-out to each peer, and nothing lingering into the
    /// next pass. **There is no timer for the application's own records.** A
    /// producer that stops calling `flush_batch` leaves its last records
    /// staged until something else moves them;
    /// `velo_streaming_mux_staged_records` is where that shows. The one record
    /// this does not apply to is a pending credit reply, which carries
    /// whatever it is staged with out after [`MuxConfig::reply_linger`] —
    /// nothing on this side of the reply knows it is owed, so `Manual` cannot
    /// leave it to the application the way it leaves everything else.
    Manual,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::Auto(AutoFlush::default())
    }
}

impl FlushPolicy {
    /// The window a staged batch is running against, if any.
    pub(crate) const fn max_linger(self) -> Option<Duration> {
        match self {
            Self::Auto(auto) => auto.max_linger,
            Self::Manual => None,
        }
    }

    /// Whether reaching the end of a wake is itself a reason to write.
    pub(crate) const fn on_admission(self) -> bool {
        match self {
            Self::Auto(auto) => auto.on_admission,
            Self::Manual => false,
        }
    }
}

/// Construction-time tuning for the mux, and the switch that installs one.
///
/// Reached from the `Velo` builder as `.messenger_mux(MuxConfig { enabled: true,
/// ..Default::default() })`. Defaults are chosen so `enabled` is the only
/// decision an operator has to make.
#[derive(Debug, Clone)]
pub struct MuxConfig {
    /// Whether to install the mux at all.
    ///
    /// **Defaults to `false`, and stays that way** — the mux is opt-in, not the
    /// default transport. This flag is also the rollback: set it back to
    /// `false` and the node stops registering `messenger-mux-v1` and stops
    /// advertising it on attach, so the next attach negotiates the legacy path
    /// with no code change and no wire change. That is what makes a canary
    /// safe, and why activation is config-only.
    ///
    /// Complete on the node that mints zero-RTT tickets — with no mux there is
    /// no ticket, and every stream attaches the ordinary way — but not
    /// symmetric. A producer rolled back alone, against a consumer that still
    /// pre-binds for it, is refused at attach rather than served over the
    /// default transport: `adopt_prebind` will not adopt a pre-bind whose key
    /// the sender no longer offers. Roll the minting side back first, or both
    /// together; `BATCHING.md`'s 2026-09-04 addendum has the whole argument.
    pub enabled: bool,
    /// Configured ceiling on one batch. Further clamped at flush time by the
    /// effective eager budget and by `COALESCE_THRESHOLD`, whichever binds
    /// first.
    pub max_batch_bytes: usize,
    /// Data credit `C` granted to each new slot, and therefore the depth of the
    /// `C + 1` buffer `bind` hands the anchor.
    ///
    /// Advertised verbatim as the attach response's `initial_credit`, so it
    /// must never be zero: zero on the wire means *this peer is not offering
    /// the mux*. Building a mux refuses a zero rather than letting a node
    /// install one it then tells every peer to ignore.
    pub initial_credit: u32,
    /// Bytes one slot may hold in flight — the replacement for the ~1 MiB the
    /// kernel socket used to enforce per stream for free. Zero means the
    /// default, which is the same thing it means on the wire.
    pub slot_byte_budget: u32,
    /// Bytes all of one peer's slots may hold in flight between them.
    pub peer_byte_budget: u64,
    /// How often the credit sweep runs.
    ///
    /// A backstop, not the primary mechanism. Credit comes back from the
    /// arrival path on every inbound batch — for the slots that batch
    /// delivered into and the slots a draining pump named on the peer's dirty
    /// lane — and from the doorbell over that same lane. This covers only what
    /// neither reaches: a slot parked with nothing further arriving *and*
    /// nothing being taken out, and one whose drain found the lane full. It
    /// also carries batcher eviction, whose granularity it sets.
    ///
    /// It was 2 ms when the sweep was the only way credit came back, which is
    /// what made that interval load-bearing rather than a tuning choice. Every
    /// tick walks every slot of every ingress peer to find the few with
    /// anything to do, so the cost grows as `O(peers x slots)` while the useful
    /// work does not. Now that draining returns credit, the interval only has
    /// to bound the cases draining cannot reach.
    ///
    /// The magnitude of what the old interval cost is not currently a
    /// measured number: the figures first quoted here were taken on a shared
    /// login node and are retracted. See the banner in
    /// `examples/examples/response_plane_bench.evidence.md`.
    ///
    /// Must be non-zero. The sweep ticks on a `tokio::time::interval`, which
    /// has no zero period, so building a mux refuses a zero here the way it
    /// refuses a zero `initial_credit` — unlike
    /// [`drain_visit_floor`](Self::drain_visit_floor) and
    /// [`reply_linger`](Self::reply_linger), where `Duration::ZERO` is a
    /// valid "off".
    pub credit_sweep_interval: Duration,
    /// Shortest gap between two doorbell-driven reconciles of the *same* peer —
    /// so a ceiling of `1 / drain_visit_floor` visits per second per peer.
    ///
    /// Coalescing alone does not bound this. A visit takes the peer's wake down
    /// before it walks, because a drain landing mid-walk must be able to post a
    /// fresh one; on a peer whose consumer is keeping up, the first record
    /// drained during the walk does exactly that, and the sweep task turns
    /// wake -> clear -> walk as fast as it can. Each of those walks holds the
    /// mutex the inbound batch path takes, so on the shape this mux exists for
    /// — one peer, hundreds to thousands of slots, a consumer that keeps up —
    /// the doorbell becomes hot-path contention. It walked every slot of the
    /// peer when this floor was added; it now walks the dirty lane's slots
    /// alone, which shortens each visit but does not change what the rate needs
    /// bounding for.
    ///
    /// Under the floor a wake arriving too soon is *not* cleared and *not*
    /// walked: it is scheduled for when the peer next comes due. The flag stays
    /// armed meanwhile, so every further drain coalesces into that one visit
    /// rather than queueing another, and no wake is lost — only delayed, by at
    /// most this long.
    ///
    /// The price is latency, and it is worth being exact about who pays it: a
    /// producer parked out of credit on a peer sending this side no further
    /// batch waits up to this long for the return its consumer's drain has
    /// already earned. Only that producer — any inbound batch from the peer
    /// reconciles the slots its pumps named on the dirty lane, so a peer that
    /// keeps sending never reaches this floor at all. A drain whose listing
    /// found the lane full is not on the lane and so not on this path either;
    /// `credit_sweep_interval` is what covers it. That is one wait per window,
    /// so what it costs per record is `floor / initial_credit` — negligible at
    /// the default 256-record window, and visible at the small windows the
    /// credit tests use deliberately. It stacks with
    /// [`reply_linger`](Self::reply_linger) rather than replacing it: the
    /// return still has to cross the receiver's egress batcher once it is
    /// reconciled here.
    ///
    /// Defaults to 2 ms, which is the interval the sweep itself ran at while it
    /// was the only way credit came back. That cadence was enough to keep every
    /// peer's credit moving then, so it is enough as a per-peer floor now, and
    /// it is a shipped number rather than a fresh guess. `Duration::ZERO` turns
    /// the floor off: every wake is walked, which is the behaviour this field
    /// was added to bound. At the other end it is clamped to an hour, past which
    /// every value means the same thing — the doorbell is off and the periodic
    /// sweep owns credit return — and the deadline arithmetic would overflow.
    pub drain_visit_floor: Duration,
    /// How long a batcher may sit idle with no slots before it is evicted.
    pub batcher_idle_ttl: Duration,
    /// When a batcher writes what it has staged.
    ///
    /// Defaults to [`FlushPolicy::Auto`] on [`AutoFlush::default`], which
    /// reproduces the pre-knob flush *policy* — reproducing the pre-knob
    /// *behaviour* also needs [`Self::reply_linger`] at `Duration::ZERO`; see
    /// that field's doc for what the 1 ms default costs instead.
    pub flush_policy: FlushPolicy,
    /// Whether opening a slot acks before the peer has admitted its `OpenSlot`.
    ///
    /// **Defaults to `false`**, the awaited ack every mux shipped with: the
    /// `OpenSlot` is written into this peer's batch — joining whatever is
    /// already staged, or opening a fresh one if nothing is — and `connect`
    /// returns once the transport has taken it. That couples opening a
    /// stream to the depth of the per-connection send queue, and on a
    /// congested peer that queue is full — so a worker cannot start
    /// producing until a place in it comes free, behind every batch already
    /// there.
    ///
    /// Set to `true` and the `OpenSlot` is still cut into a batch of its own
    /// and still handed to the transport before the ack, so `bind()`'s accept
    /// window keeps measuring "time until a batch bearing this `OpenSlot`
    /// arrives". What stops is *waiting* for the transport to take it. The
    /// slot is fenced until that admission resolves, so its first data record
    /// cannot overtake the `OpenSlot` that claims the buffer it lands in, and a
    /// failed admission is epoch death exactly as an awaited one is.
    ///
    /// The wait it removes is the open's own, and only that one. Cutting the
    /// `OpenSlot` into a batch of its own means first writing whatever this
    /// peer already had staged, and *that* write still parks on admission — so
    /// the ack is wait-free only when nothing is staged. With a batch staged
    /// the two paths wait the same once and this one costs a frame more,
    /// because the awaited path packs the `OpenSlot` into that staged batch
    /// rather than sending a second. Read as a rate, what this buys in
    /// principle is the open of an idle peer, which is the open a queued
    /// request waits on.
    ///
    /// **Measured, that benefit did not show at load.** Two reruns at 512
    /// workers and 8,192-way concurrency, the second past a control-cap fix
    /// this branch also carries: TTFT p95 is worse than baseline in every rep
    /// for every flagged arm, both times; p50 does not improve. See
    /// `agent-docs/w4a-async-open-ack-status.md` for the numbers.
    ///
    /// **The per-open cost is not amortized the way a packed flush is, and is
    /// paid whether or not the wait it removes was on the critical path.**
    /// Every call spends one `tokio::spawn` and an `Arc` clone to watch the
    /// admission. On a peer whose admission is not already behind it — the
    /// congested case, where the fence below actually goes up — that watcher
    /// also costs a `ControlInbox` mutex-guarded map insert plus a `Notify`
    /// wake to report the answer back, and — once the fence lifts — an
    /// unfence and a full `release_withheld` pass; a synchronously `Admitted`
    /// admission reports nothing, since there is no fence for it to lift.
    /// Every record queued during the fence window pays a withheld-queue push
    /// and pop plus a second `is_terminal_sentinel` decode that the unfenced
    /// fast path never runs.
    ///
    /// **The lift itself can queue behind other opens to the same peer.** The
    /// run loop's `select!` and its `drain_once` fast-forward both poll the
    /// opens channel ahead of coalesced control, so while that channel has
    /// anything queued for this peer, the resolution that lifts a fence —
    /// carried on the control lane — does not get drained. Before this flag a
    /// slot's first record never depended on the control lane at all; with it,
    /// every congested-peer open now does.
    ///
    /// **The fence this trades in is a second way to lose a stream.** On a
    /// peer whose admission is not already behind it, the fence goes up
    /// before the ack, unconditionally of credit, so the slot's first
    /// record — and every one behind it — withholds from record #1 whether or
    /// not the slot has room to spend. On a peer whose send queue is the
    /// congested one this flag exists to route around, a producer that starts
    /// generating right away can fill the slot's byte cap before its own
    /// `OpenSlot` is admitted, and the slow-consumer kill (`SATURATION.md`)
    /// destroys the stream on that basis — a healthy consumer never entered
    /// into it. Because `peer_byte_budget` bounds ingress only, nothing caps
    /// how many slots may be open this way against one congested peer at once:
    /// each can withhold up to `slot_byte_budget`, so the aggregate egress
    /// buffer a stalled peer can hold grows with concurrent opens where the
    /// awaited ack bounded it to one wait at a time.
    pub async_open_ack: bool,
    /// How long a pending credit reply may wait for a batch to form around it.
    ///
    /// A `CreditUpdate` used to mark its batch urgent, so a batcher whose peer
    /// admits at once wrote one batch per wake — and a receiver's batcher wakes
    /// once per reply the sweep hands it. Measured on the tier-3 rig, arms that
    /// differ only in this window with zero-RTT attach on both (so nothing
    /// else in the path is waiting behind a round trip), turning that
    /// per-reply batch into one per sweep visit cut outbound batches by
    /// 4.3x-5.9x across 3 reps. Records per batch moved too (roughly 20-22 to
    /// roughly 96-105), but not by the same ratio, because the two arms did
    /// not carry equal total `CreditUpdate` volume — see
    /// `agent-docs/w7-reply-linger-measurement.md` for the numbers, why the
    /// two ratios do not have to match, and why that document's request-level
    /// numbers (throughput, TTFT, ITL) do not establish a benefit from this
    /// window on their own. What the receiver's own instrumentation does
    /// show, band-independent across the same reps: with the window on,
    /// `velo_transport_write_duration_seconds_sum` ran 2.82-3.88 s against
    /// 13.52-14.30 s with it off, and
    /// `velo_transport_egress_queue_wait_seconds_sum` ran 3.53-6.56 s against
    /// 25.82-27.44 s — disjoint in both series.
    ///
    /// What ends the wait early depends on the policy, because `on_admission`
    /// already has its own reason to write and this window does not override
    /// it:
    ///
    /// - Under [`AutoFlush::on_admission`], any record that is not a credit
    ///   reply ends the wait at once — data included — because the batch is no
    ///   longer replies-only and `on_admission` writes any non-empty batch
    ///   that isn't. A batch holding nothing but replies still waits out the
    ///   window (or a close, a terminal or an application flush, whichever is
    ///   sooner).
    /// - Under [`FlushPolicy::Manual`] and `Auto { on_admission: false }`,
    ///   nothing about admission or ordinary staging cuts the wait short: data
    ///   joins the batch without disturbing it, because the window is a
    ///   property of the pending reply and not of the batch staying
    ///   replies-only. The reply keeps its bound and the data rides out with
    ///   it when the window (or a close, a terminal, or `flush_batch`) ends
    ///   it.
    /// - Under every policy, opening a slot on this peer ends the wait: the
    ///   `OpenSlot` has its own eager flush, and whatever the batch already
    ///   held — a pending reply included — is written ahead of or alongside
    ///   it.
    ///
    /// A shorter `max_linger` on the same batcher can still cut either case
    /// short, since the two windows run independently and the earlier due time
    /// wins.
    ///
    /// The return a sender is owed is delayed by at most this long, once per
    /// window, which per record is `reply_linger / initial_credit`: nothing at
    /// the default 256-record window. On the drain-driven return path this
    /// stacks with [`drain_visit_floor`](Self::drain_visit_floor) rather than
    /// replacing it — a producer parked out of credit can wait for both, in
    /// series.
    /// `Duration::ZERO` makes the window already due, so a reply goes out at
    /// the end of the wake that staged it — the same urgent flush the
    /// batcher had before this knob existed.
    ///
    /// Defaults to 1 ms.
    pub reply_linger: Duration,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_batch_bytes: 60 * 1024,
            initial_credit: 256,
            slot_byte_budget: DEFAULT_SLOT_BYTE_BUDGET,
            peer_byte_budget: DEFAULT_PEER_BYTE_BUDGET,
            credit_sweep_interval: Duration::from_millis(200),
            drain_visit_floor: Duration::from_millis(2),
            batcher_idle_ttl: Duration::from_secs(60),
            flush_policy: FlushPolicy::Auto(AutoFlush::default()),
            async_open_ack: false,
            reply_linger: Duration::from_millis(1),
        }
    }
}

impl MuxConfig {
    /// Sweep ticks a batcher must sit idle through before it may be evicted.
    pub(super) fn idle_ticks(&self) -> u32 {
        let interval = self.credit_sweep_interval.as_millis().max(1);
        let ttl = self.batcher_idle_ttl.as_millis();
        u32::try_from(ttl / interval).unwrap_or(u32::MAX).max(1)
    }
}
