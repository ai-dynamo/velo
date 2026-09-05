// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batched, multiplexed streaming over the Messenger — the `messenger-mux-v1`
//! transport described by `BATCHING.md`.
//!
//! Today one stream owns one connection: X concurrent streams to one peer means
//! X sockets, X egress pumps, X heartbeat timers, and one `write` syscall per
//! token. The mux collapses that to **one batcher per peer** and packs every
//! stream's records into `_stream_batch` active messages that ride the
//! Messenger's existing connectivity. There is no dial, no listener, no
//! acceptor and no connection lifecycle, because there is no connection: the
//! sender's identity arrives in the AM envelope, so credit has somewhere to be
//! routed without a handshake invented to learn it.
//!
//! The cost is that streaming no longer owns its wire. It shares queues,
//! framing and backpressure with control traffic, and ordering stops being a
//! TCP guarantee and becomes an explicit protocol obligation. Everything in
//! this module follows from that one trade:
//!
//! - [`protocol`] — the wire. A 16-byte batch header carrying the peer epoch
//!   and a modulo-compared batch sequence, then records tagged with a
//!   `(u24 index, u8 generation)` slot and a per-slot `frame_seq` that is the
//!   authority on stream order.
//! - [`flow_control`] — the credit. Multiplexing means the shared resource is
//!   the peer's *ordering lane*, and a handler that awaits holds it against
//!   every slot from that peer. So ingress is bounded and nonblocking on
//!   per-slot credit, with one reserved terminal credit, control records that
//!   data exhaustion cannot block, and byte budgets standing in for the
//!   per-stream socket limit the kernel used to enforce for free.
//! - [`peer_batcher`] — egress. One task per peer, packing every slot's records
//!   and parking on send admission when the peer is congested.
//! - [`ingress`] — receive. The `_stream_batch` handler body, ordered per
//!   sender and nonblocking by construction.
//!
//! ## How a deployment reaches it
//!
//! Opt-in, through `Velo::builder().messenger_mux(MuxConfig { enabled: true,
//! ..Default::default() })`. That registers the transport beside the configured
//! legacy one and lets this node advertise [`MESSENGER_MUX_KEY`] on its attach
//! requests; [`crate::streaming::negotiation`] is where an attach then picks
//! between the two, and picks the mux only when both peers named it. Setting
//! `enabled` back to `false` stops the advertisement and is therefore the whole
//! rollback.
//!
//! ## The producer's contract under the mux
//!
//! `StreamSender::send` documents itself in transport-neutral terms — a bounded
//! channel, an awaited send when it fills — and for TCP and gRPC that is exactly
//! what happens, because those egress pumps drain at socket speed and a full
//! channel is transient. The mux behaves differently and it is worth stating
//! where a reader will meet it:
//!
//! - **`send` does not become the backpressure point.** The batcher drains a
//!   slot's inlet whether or not the slot may send — it has to, because
//!   `finalize`, `detach` and `Drop` reach that same channel through a
//!   *synchronous* send, and a slot parked on credit would block one of them
//!   forever. Records the slot cannot send wait in a mux-owned withheld queue
//!   instead.
//! - **The bound is bytes, and overrunning it ends the stream.** That queue is
//!   capped at the per-slot byte budget (1 MiB by default). A producer that runs
//!   further ahead than that on a slot nobody is draining has its slot closed:
//!   the consumer receives `Dropped`, the producer's channel starts erroring,
//!   and the peer's other slots are untouched. `SATURATION.md` describes it from
//!   the operator's side.
//!
//! The exception is a batcher parked on *admission* rather than on credit. That
//! suspends the task, inlet drain included — but it is bounded by the
//! transport's own progress, which is the position a socket was always in. It is
//! credit starvation, which nothing but the consumer can end, that the withheld
//! queue exists for.
//!
//! Credit comes back from three places, and which one gets there first decides
//! what the sweep interval costs. The arrival path reconciles on every inbound
//! batch. A draining consumer posts its peer through
//! [`ingress::DrainSignal`], and the sweep task reconciles that peer alone — no
//! more often than once per [`MuxConfig::drain_visit_floor`], because clearing
//! the wake before the walk means a consumer that keeps up re-arms it
//! immediately and would otherwise have the task spin over its slot table.
//! The periodic tick is the backstop for what neither reaches — a slot parked
//! with nothing arriving *and* nothing being taken out — and it carries batcher
//! eviction.
//!
//! The drain path is a doorbell, not a ledger: it carries no quantity, and
//! `IngressSlot::reconcile` remains the only thing that decides how much credit
//! was freed. That is what lets it run concurrently with the sweep — a
//! redundant visit recomputes the same answer, where a delta would double-count.
//!
//! It still differs from `BATCHING.md` § P8, which specifies an exact
//! `credit.release(1)` per handoff. Releasing an amount from the pump is the
//! part that was not adopted, for the reason above. See the dated addendum at
//! the end of that document.

pub(crate) mod flow_control;
pub(crate) mod ingress;
pub(crate) mod peer_batcher;
pub(crate) mod protocol;
#[cfg(test)]
mod tests;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use velo_ext::{TransportKey, WorkerAddress, WorkerId};

use self::flow_control::{DEFAULT_PEER_BYTE_BUDGET, DEFAULT_SLOT_BYTE_BUDGET, NegotiatedLimits};
use self::ingress::IngressRegistry;
use self::peer_batcher::{
    BatcherContext, BatcherHandle, BatcherMap, OpenRejected, OpenSlotRequest,
};
use crate::messenger::{Context, Handler, Messenger};
use crate::observability::{MuxMetricsHandle, VeloMetrics};
use crate::streaming::transport::FrameTransport;

/// The streaming-transport key this mux answers to.
///
/// Versioned in the key rather than only in the batch header: negotiation
/// matches on the key, so an incompatible wire change is a new key and two
/// versions simply never pair up.
///
/// Public because it is what
/// [`StreamSender::negotiated_transport`](crate::streaming::StreamSender::negotiated_transport)
/// is compared against — a caller that had to spell the string itself would be
/// re-deriving the one value negotiation is keyed on.
pub const MESSENGER_MUX_KEY: &str = "messenger-mux-v1";

/// The active-message handler every batch travels through.
pub(crate) const STREAM_BATCH_HANDLER: &str = "_stream_batch";

/// How long a `bind()` waits for the `OpenSlot` that claims it.
///
/// Deliberately the same 60 s the TCP transport gives a pending session, and
/// deliberately measuring the same thing: "time until a batch bearing this
/// `OpenSlot` arrives". `OpenSlot` is eager precisely so this cannot quietly
/// become "time until the producer produces its first token" and expire a queued
/// request with a long prefill.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Attempts `connect` makes before giving up on a batcher that keeps retiring
/// underneath it. Two is already generous — losing the race twice requires two
/// eviction sweeps inside one attach.
const CONNECT_ATTEMPTS: usize = 3;

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
    /// The historical default, and it cannot make anything worse: the batcher
    /// never waits for work that has not arrived, it only notices that more is
    /// *already* there and takes all of it. The name is the mechanism — a flush
    /// parks until the transport admits it, so "at the end of every wake" is in
    /// practice "as soon as the peer admitted the last batch".
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
/// liveness go — and under both,
/// [`Velo::flush_batch`](crate::Velo::flush_batch) writes immediately. What
/// they differ on is whether anything *else* does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushPolicy {
    /// The batcher decides, on the conditions in [`AutoFlush`].
    ///
    /// The default is `AutoFlush::default()`, which is byte-for-byte the
    /// behaviour every mux had before this knob existed.
    Auto(AutoFlush),
    /// The application decides, through
    /// [`Velo::flush_batch`](crate::Velo::flush_batch).
    ///
    /// The policy a serving loop wants: one write per forward pass carrying
    /// that pass's whole fan-out to each peer, and nothing lingering into the
    /// next pass. **There is no timer.** A producer that stops calling
    /// `flush_batch` leaves its last records staged until something else moves
    /// them; `velo_streaming_mux_staged_records` is where that shows.
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
    /// arrival path on every inbound batch and from the consumer draining
    /// (`ingress::DrainSignal`); this covers only what neither reaches — a slot
    /// parked with nothing further arriving *and* nothing being taken out — and
    /// carries batcher eviction, whose granularity it also sets.
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
    pub credit_sweep_interval: Duration,
    /// Shortest gap between two doorbell-driven reconciles of the *same* peer —
    /// so a ceiling of `1 / drain_visit_floor` visits per second per peer.
    ///
    /// Coalescing alone does not bound this. A visit takes the peer's wake down
    /// before it walks, because a drain landing mid-walk must be able to post a
    /// fresh one; on a peer whose consumer is keeping up, the first record
    /// drained during the walk does exactly that, and the sweep task turns
    /// wake -> clear -> walk as fast as it can. Each of those walks iterates
    /// every slot of the peer and holds the mutex the inbound batch path takes,
    /// so on the shape this mux exists for — one peer, hundreds to thousands of
    /// slots, a consumer that keeps up — the doorbell becomes hot-path
    /// contention.
    ///
    /// Under the floor a wake arriving too soon is *not* cleared and *not*
    /// walked: it is scheduled for when the peer next comes due. The flag stays
    /// armed meanwhile, so every further drain coalesces into that one visit
    /// rather than queueing another, and no wake is lost — only delayed, by at
    /// most this long.
    ///
    /// The price is latency, and it is worth being exact about who pays it: a
    /// producer parked out of credit, with no further batch arriving to
    /// reconcile it on the arrival path, waits up to this long for the return
    /// its consumer's drain has already earned. That is one wait per window, so
    /// what it costs per record is `floor / initial_credit` — negligible at the
    /// default 256-record window, and visible at the small windows the credit
    /// tests use deliberately.
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
    /// Defaults to [`FlushPolicy::Auto`] on [`AutoFlush::default`], which is
    /// the behaviour every mux had before this knob existed.
    pub flush_policy: FlushPolicy,
    /// How long a batch holding only credit replies may form before it goes.
    ///
    /// A `CreditUpdate` used to mark its batch urgent, so a batcher whose peer
    /// admits at once wrote one batch per wake — and a receiver's batcher wakes
    /// once per reply the sweep hands it. Measured on the tier-3 rig with the
    /// attach round trip gone (nothing else left in the receiver's egress to
    /// wait behind), that was ten times the outbound batches of the same load
    /// with it, most of them one to eight records, and the request path paid
    /// for every one as a wake on this side and an inbound message on the
    /// other.
    ///
    /// Under this window a batch that holds nothing but credit replies waits
    /// for the window, or for the next record that does not wait — a close, a
    /// terminal, data, an application flush — whichever comes first. The
    /// return a sender is owed is delayed by at most this long, once per
    /// window, which per record is `reply_linger / initial_credit`: nothing at
    /// the default 256-record window. `Duration::ZERO` restores the urgent
    /// flush.
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
            reply_linger: Duration::from_millis(1),
        }
    }
}

impl MuxConfig {
    /// Sweep ticks a batcher must sit idle through before it may be evicted.
    fn idle_ticks(&self) -> u32 {
        let interval = self.credit_sweep_interval.as_millis().max(1);
        let ttl = self.batcher_idle_ttl.as_millis();
        u32::try_from(ttl / interval).unwrap_or(u32::MAX).max(1)
    }
}

/// Peer wakes the drain lane holds before it starts dropping them.
///
/// Wakes coalesce naturally — many slots of one peer post the same `WorkerId`,
/// and one reconcile of that peer serves all of them — so this does not need to
/// scale with slot count. It needs to absorb a burst across distinct peers.
const DRAIN_WAKE_CAPACITY: usize = 1024;

/// The `messenger-mux-v1` [`FrameTransport`].
///
/// Holds no listener and no connections. `connect` allocates a slot on the
/// peer's batcher; `bind` registers a buffer the peer's `OpenSlot` will claim.
/// Everything else is the two subsystems this type wires together.
pub(crate) struct MessengerMuxTransport {
    core: Arc<MuxCore>,
    key: TransportKey,
}

/// State shared between the transport, its batchers and the ingress handler.
struct MuxCore {
    messenger: Arc<Messenger>,
    config: MuxConfig,
    /// This node's own window, resolved once at construction.
    ///
    /// What a receiver advertises on attach and what a sender falls back to
    /// when it opens a slot without having negotiated — the two are the same
    /// numbers, so they are resolved in one place through the same
    /// [`NegotiatedLimits::from_wire`] the wire path uses.
    limits: NegotiatedLimits,
    metrics: Option<MuxMetricsHandle>,
    batchers: Arc<BatcherMap>,
    ingress: Arc<IngressRegistry>,
    /// Process-wide monotonic epoch source.
    ///
    /// Per transport rather than per batcher on purpose: a batcher evicted and
    /// lazily recreated must not restart its epoch, or the peer would read every
    /// batch of the new one as stale and discard it wholesale.
    epochs: Arc<AtomicU64>,
    cancel: CancellationToken,
    /// Peers with credit to return, posted by draining consumers. See
    /// [`ingress::DrainSignal`].
    drain_tx: flume::Sender<WorkerId>,
    drain_rx: flume::Receiver<WorkerId>,
    /// Drain signals waiting to be collected by the attach that will spawn the
    /// pump holding them.
    ///
    /// `bind` cannot hand this back directly — `FrameTransport::bind` returns a
    /// receiver and nothing else, and widening that trait would be a breaking
    /// change to `velo-ext` for every out-of-tree implementor. So the signal is
    /// parked here for the attach path to take, which it does a few lines after
    /// `bind` returns. Take-once: whoever collects it owns it, and the bind
    /// expiry that already exists drops any that was never collected.
    drains: DashMap<(u64, u64), Arc<ingress::DrainSignal>>,
}

impl MessengerMuxTransport {
    /// Take the [`ingress::DrainSignal`] `bind` parked for this pair.
    ///
    /// Called once by the attach path, between `bind` returning and the pump
    /// being spawned. Returns `None` for a pair this transport did not bind,
    /// which is the honest answer for the legacy per-stream transports — they
    /// have no mux credit to return.
    pub(crate) fn take_drain_signal(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> Option<Arc<ingress::DrainSignal>> {
        self.core
            .drains
            .remove(&(anchor_id, session_id))
            .map(|(_, signal)| signal)
    }

    /// Build a mux over `messenger` and register its `_stream_batch` handler.
    ///
    /// Registration is for the messenger's lifetime: there is no
    /// handler-deregistration hook, and `register_streaming_handler` refuses a
    /// duplicate name, so at most one mux may be installed per messenger — a
    /// second attempt fails here rather than producing two batchers racing for
    /// one handler name.
    ///
    /// Fails on `initial_credit = 0`, which is not a small window but the wire
    /// encoding of *"not offering the mux"*. A node that installed one would
    /// advertise a key and then tell every peer to ignore it.
    pub(crate) fn new(
        messenger: Arc<Messenger>,
        config: MuxConfig,
        metrics: Option<Arc<VeloMetrics>>,
    ) -> Result<Arc<Self>> {
        let limits = NegotiatedLimits::from_wire(config.initial_credit, config.slot_byte_budget)
            .map_err(|error| anyhow!("messenger mux: {error}"))?;
        // Normalised so every reader of the config sees the effective budget
        // rather than the "use the default" zero.
        let config = MuxConfig {
            slot_byte_budget: limits.slot_byte_budget(),
            ..config
        };
        // Bounded, and deliberately lossy on overflow: a wake is a hint that a
        // peer has credit to return, and a dropped hint costs latency the
        // periodic sweep still bounds. Sized so a burst across many peers does
        // not discard wakes it could have kept.
        let (drain_tx, drain_rx) = flume::bounded::<WorkerId>(DRAIN_WAKE_CAPACITY);
        let core = Arc::new(MuxCore {
            messenger: Arc::clone(&messenger),
            config,
            limits,
            metrics: metrics.as_ref().map(|metrics| metrics.bind_mux()),
            batchers: Arc::new(DashMap::new()),
            ingress: Arc::new(IngressRegistry::default()),
            // Epochs start at 1 so zero is never a live epoch, which keeps a
            // zeroed header from reading as a legitimate one.
            epochs: Arc::new(AtomicU64::new(1)),
            cancel: CancellationToken::new(),
            drain_tx,
            drain_rx,
            drains: DashMap::new(),
        });

        let handler_core = Arc::downgrade(&core);
        let handler = Handler::am_handler_async(STREAM_BATCH_HANDLER, move |ctx: Context| {
            let handler_core = handler_core.clone();
            async move {
                if let Some(core) = handler_core.upgrade() {
                    core.deliver_batch(ctx.sender_worker_id(), &ctx.payload);
                }
                Ok(())
            }
        })
        // Ordered per sender. This is the whole reason the mux can drop the
        // reorder window the deprecated AM transport needed: batches from one
        // peer are handled on that peer's lane, by one task, in arrival order.
        .ordered()
        .build();
        messenger.register_streaming_handler(handler)?;

        spawn_sweep(&core);

        Ok(Arc::new(Self {
            core,
            key: TransportKey::new(MESSENGER_MUX_KEY),
        }))
    }
}

impl MuxCore {
    /// The batcher for `peer`, created on first use.
    fn batcher(&self, peer: WorkerId) -> Arc<BatcherHandle> {
        if let Some(existing) = self.batchers.get(&peer) {
            return Arc::clone(existing.value());
        }
        Arc::clone(
            self.batchers
                .entry(peer)
                .or_insert_with(|| {
                    peer_batcher::spawn(
                        peer,
                        BatcherContext {
                            messenger: Arc::clone(&self.messenger),
                            config: self.config.clone(),
                            metrics: self.metrics.clone(),
                            epochs: Arc::clone(&self.epochs),
                            batchers: Arc::clone(&self.batchers),
                            cancel: self.cancel.clone(),
                            #[cfg(test)]
                            hooks: None,
                        },
                    )
                })
                .value(),
        )
    }

    /// Hand one decoded batch to the ingress lane and act on what it produced.
    fn deliver_batch(&self, peer: WorkerId, payload: &bytes::Bytes) {
        let outcome = ingress::handle_batch(
            &self.ingress,
            &self.config,
            self.metrics.as_ref(),
            peer,
            payload,
        );

        if let Some(metrics) = &self.metrics {
            for _ in 0..outcome.opened {
                metrics.slot_opened();
            }
            for _ in 0..outcome.closed {
                metrics.slot_closed();
            }
        }

        if outcome.replies.is_empty() && outcome.grants.is_empty() && outcome.peer_closes.is_empty()
        {
            return;
        }

        let batcher = self.batcher(peer);
        for (slot, delta) in outcome.grants {
            batcher.grant(slot, delta);
        }
        for (slot, reason) in outcome.peer_closes {
            batcher.peer_closed(slot, reason);
        }
        if !outcome.replies.is_empty() {
            self.send_replies(&batcher, peer, &outcome.replies);
        }
    }

    /// Kick every live batcher into writing what it has staged.
    fn flush_batches(&self) {
        for entry in self.batchers.iter() {
            entry.value().kick_flush();
        }
    }

    /// Queue control records back to `peer`, re-resolving once if the batcher
    /// exited between resolution and the write.
    ///
    /// Control is coalesced state rather than a queue, so nothing here can fail
    /// on the write — the liveness check is what stands in for a `SendError`.
    /// Losing the race costs nothing: a batcher exits on cancellation, which is
    /// the transport going away, or on retirement, which requires zero live
    /// slots on both sides and so no credit anyone is waiting for.
    fn send_replies(
        &self,
        batcher: &Arc<BatcherHandle>,
        peer: WorkerId,
        replies: &[peer_batcher::ReplyRecord],
    ) {
        if batcher.is_alive() {
            batcher.reply(replies);
            return;
        }
        self.batcher(peer).reply(replies);
    }

    /// Return whatever credit one peer has to return.
    ///
    /// The drain-driven path: a consumer took records out of a slot's buffer,
    /// so that peer — and only that peer — has credit worth reconciling. The
    /// periodic sweep runs the same body across every peer.
    fn sweep_peer(&self, peer: WorkerId) {
        // Taken down before the reconcile, not after: a record drained while
        // this visit is in progress must be able to post a fresh wake, or its
        // credit waits for the periodic backstop.
        self.ingress.clear_pending_wake(peer);
        let replies = self.ingress.sweep_credit(peer);
        if !replies.is_empty() {
            let batcher = self.batcher(peer);
            self.send_replies(&batcher, peer, &replies);
        }
    }

    /// One doorbell-driven visit: reconcile the peer that rang, and count it.
    ///
    /// Counted here rather than where the wake is received, so the series
    /// measures walks and not wakes — a wake the floor deferred is counted once,
    /// on the visit it coalesced into.
    fn visit_drained_peer(&self, peer: WorkerId) {
        if let Some(metrics) = &self.metrics {
            metrics.drain_visit();
        }
        self.sweep_peer(peer);
    }

    /// One sweep tick: return credit, then age out idle batchers.
    fn sweep(&self) {
        for peer in self.ingress.peers() {
            self.sweep_peer(peer);
        }

        let threshold = self.config.idle_ticks();
        let peers: Vec<WorkerId> = self.batchers.iter().map(|entry| *entry.key()).collect();
        for peer in peers {
            let Some(handle) = self.batchers.get(&peer) else {
                continue;
            };
            let idle = handle.tick_idle();
            drop(handle);
            if idle < threshold || self.ingress.live_slots(peer) > 0 {
                continue;
            }
            // The claim is made under the registry's shard lock, so a `connect`
            // resolving the same peer either sees the entry gone and creates a
            // fresh batcher, or gets this one and has its `OpenSlot` refused.
            if let Some((_, handle)) = self
                .batchers
                .remove_if(&peer, |_, handle| handle.try_retire(threshold))
            {
                handle.retire();
            }
        }
    }
}

impl Drop for MuxCore {
    fn drop(&mut self) {
        self.cancel.cancel();
        let closed = self.ingress.shutdown();
        if let Some(metrics) = &self.metrics {
            for _ in 0..closed {
                metrics.slot_closed();
            }
        }
    }
}

/// Ceiling on an operator's [`MuxConfig::drain_visit_floor`].
///
/// Not a tuning limit — a floor this long already means "the doorbell is off and
/// the periodic sweep owns credit return", and every value past it means the
/// same thing. It exists so `last + floor` is total arithmetic: a `Duration`
/// near the maximum would overflow the `Instant` and panic the sweep task, which
/// is a poor way to answer a misconfiguration.
const MAX_DRAIN_VISIT_FLOOR: Duration = Duration::from_secs(3600);

/// One peer's doorbell state.
struct PeerVisits {
    /// When the doorbell last walked this peer.
    last: tokio::time::Instant,
    /// Whether a deferred walk for it is already queued.
    ///
    /// Exactly one entry per peer is ever in the queue, and this is what says
    /// so. See [`DrainVisits`] for what a second one costs.
    queued: bool,
}

/// The per-peer floor on doorbell-driven visits, and the queue it defers into.
///
/// Coalescing alone leaves the visit rate a property of the traffic: a visit
/// takes the peer's wake down before it walks, so the next record drained arms
/// it again and the sweep task turns wake -> clear -> walk back to back. What
/// bounds it is this — a peer visited less than [`MuxConfig::drain_visit_floor`]
/// ago is *not* walked and its wake is *not* cleared. It goes into the deferred
/// queue instead, and because the flag stays armed, every drain until then
/// coalesces into that one scheduled visit rather than posting another wake. So
/// deferral never loses a wake; it delays one by at most the floor.
///
/// **One queue entry per peer, and the reason is a ratchet.** A queued walk is
/// the authoritative next one, so a wake arriving while one is queued is
/// answered by it rather than queueing a second — and, past the floor or not, is
/// never walked out of band. Letting either happen leaves the queued entry
/// behind as residue, and the periodic tick supplies a steady source of them: it
/// calls `sweep_peer` on every peer, clearing the wake of one that is
/// *currently deferred*, whose consumer's next drain then re-arms and posts
/// again inside the same floor. One entry per tick, permanently, with the sweep
/// task's queue work growing to match. Measured before the bound: 64 rounds of
/// that pattern left 64 entries for a single peer, monotone, and a live probe
/// saw 59 floor-spaced walks continue after the traffic had provably stopped.
struct DrainVisits {
    floor: Duration,
    peers: HashMap<WorkerId, PeerVisits>,
    /// Deferred walks, ordered by when they come due, at most one per peer.
    deferred: BinaryHeap<Reverse<(tokio::time::Instant, WorkerId)>>,
}

impl DrainVisits {
    fn new(floor: Duration) -> Self {
        Self {
            floor: floor.min(MAX_DRAIN_VISIT_FLOOR),
            peers: HashMap::new(),
            deferred: BinaryHeap::new(),
        }
    }

    /// When the next deferred peer comes due, if any is waiting.
    fn next_due(&self) -> Option<tokio::time::Instant> {
        self.deferred.peek().map(|Reverse((due, _))| *due)
    }

    /// Walks currently queued.
    ///
    /// The bound this type owes its caller is one entry per peer, so the size
    /// of the queue is the property worth asserting on and `next_due` alone
    /// cannot see it — a queue holding residue reports the same next deadline
    /// as one holding a single live entry.
    #[cfg(test)]
    fn queued(&self) -> usize {
        self.deferred.len()
    }

    /// Answer a wake: `Some(peer)` to walk it now, `None` when the wake was
    /// deferred into a queued walk instead.
    ///
    /// A peer handed back is stamped here rather than by the caller after the
    /// walk. The invariant is what makes the floor hold: *everything this
    /// returns has already been counted as visited*, so two wakes for one peer
    /// cannot both be admitted, and the interval the floor measures is
    /// walk-start to walk-start — which is what the rate it bounds means.
    fn admit(&mut self, peer: WorkerId, now: tokio::time::Instant) -> Option<WorkerId> {
        let Some(state) = self.peers.get_mut(&peer) else {
            self.peers.insert(
                peer,
                PeerVisits {
                    last: now,
                    queued: false,
                },
            );
            return Some(peer);
        };
        // Checked before the floor, not after. A queued walk answers this wake
        // whether or not the floor has since elapsed, and walking here instead
        // would strand that entry in the queue as residue — which is the whole
        // of the ratchet described on this type.
        if state.queued {
            return None;
        }
        if now.saturating_duration_since(state.last) < self.floor {
            state.queued = true;
            self.deferred.push(Reverse((state.last + self.floor, peer)));
            return None;
        }
        state.last = now;
        Some(peer)
    }

    /// Peers whose deferred walk has come due.
    fn due(&mut self, now: tokio::time::Instant) -> Vec<WorkerId> {
        let mut ready = Vec::new();
        while self.next_due().is_some_and(|due| due <= now) {
            let Reverse((_, peer)) = self.deferred.pop().expect("peeked a moment ago");
            if let Some(state) = self.peers.get_mut(&peer) {
                state.queued = false;
            }
            // Back through `admit`, which is the one place the floor is decided.
            // An entry exists only for a peer `admit` has refused to walk since
            // it was queued, and its due instant is one floor past that peer's
            // last walk, so this always comes back `Some` and never re-queues.
            ready.extend(self.admit(peer, now));
        }
        ready
    }

    /// Drop per-peer state that can no longer defer anything.
    ///
    /// A last walk older than the floor admits the next wake immediately, so
    /// forgetting it changes no decision. It only keeps this map to the peers
    /// currently draining rather than to every peer the node has ever received
    /// from.
    ///
    /// A peer with a walk still queued is kept regardless of its age. Its state
    /// is what records that the walk is queued, and dropping it would let the
    /// next wake walk immediately and queue a second entry behind the one still
    /// sitting there — the residue this type exists to not accumulate.
    fn forget_stale(&mut self, now: tokio::time::Instant) {
        let floor = self.floor;
        self.peers
            .retain(|_, state| state.queued || now.saturating_duration_since(state.last) < floor);
    }
}

/// Park until `due`, or forever when nothing is deferred.
///
/// `pending` rather than a zero-length sleep: an empty queue must leave the
/// timer arm silent, or the loop spins on a deadline that is always in the past
/// and becomes a worse version of the rate this floor exists to bound.
async fn deferred_visit_due(due: Option<tokio::time::Instant>) {
    match due {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Spawn the credit-return and eviction sweep.
///
/// Two sources, and which one does the work matters for cost. A draining
/// consumer posts its peer on the drain lane, and this reconciles **that peer
/// only** — bounded above by the drains, and bounded below by
/// [`MuxConfig::drain_visit_floor`], which is what keeps a consumer that keeps
/// up from turning the doorbell into a spin over the peer's slot table. The
/// ticker is the backstop for what draining cannot reach: a slot parked with
/// nothing further arriving and no consumer taking anything out, and batcher
/// eviction, which free-rides on the same tick.
///
/// Before this, the ticker was the only source and ran at 500 Hz, walking every
/// slot of every peer to find the few with credit to return — work that scales
/// with peers and slots while the credit actually returned does not.
///
/// A tick does not stamp the peers it swept as visited. It could, and the cost
/// of not doing it is at most one extra doorbell walk per peer per tick period —
/// against a default interval a hundred times the floor, that is noise, and it
/// keeps the periodic path from having to report which peers it touched.
///
/// The tick does something subtler that is *not* noise, and [`DrainVisits`] is
/// where it is answered: `sweep_peer` clears the wake of every peer, including
/// one whose walk is already queued, so that peer's next drain re-arms and posts
/// a second wake inside the same floor. Deferring that wake into a second queue
/// entry is what used to leave one entry behind per tick, for good.
fn spawn_sweep(core: &Arc<MuxCore>) {
    let weak = Arc::downgrade(core);
    let cancel = core.cancel.clone();
    let interval = core.config.credit_sweep_interval;
    let floor = core.config.drain_visit_floor;
    let drain_rx = core.drain_rx.clone();
    tokio::spawn(async move {
        enum Wake {
            Tick,
            Peer(WorkerId),
            Due,
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut visits = DrainVisits::new(floor);
        loop {
            // `biased` puts cancellation first, so teardown is never starved
            // by a busy lane. The remaining arms are deliberately *not* biased
            // against each other: an earlier version polled the ticker first,
            // which at a short interval left it almost always ready and starved
            // the drain arm — the event-driven path barely ran, and the periodic
            // walk did the work it was meant to replace. The deferred-visit
            // timer joins them on the same footing for the same reason.
            let deferred_until = visits.next_due();
            let wake = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                wake = async {
                    tokio::select! {
                        _ = ticker.tick() => Wake::Tick,
                        () = deferred_visit_due(deferred_until) => Wake::Due,
                        drained = drain_rx.recv_async() => match drained {
                            Ok(peer) => Wake::Peer(peer),
                            // Every sender is gone with the transport. Fall back
                            // to ticking so eviction still runs.
                            Err(_) => {
                                ticker.tick().await;
                                Wake::Tick
                            }
                        },
                    }
                } => wake,
            };
            let Some(core) = weak.upgrade() else {
                return;
            };
            let now = tokio::time::Instant::now();
            match wake {
                Wake::Tick => {
                    core.sweep();
                    visits.forget_stale(now);
                }
                Wake::Peer(peer) => {
                    if let Some(peer) = visits.admit(peer, now) {
                        core.visit_drained_peer(peer);
                    }
                }
                Wake::Due => {
                    for peer in visits.due(now) {
                        core.visit_drained_peer(peer);
                    }
                }
            }
        }
    });
}

impl FrameTransport for MessengerMuxTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    /// Empty: the mux piggybacks on the Messenger's connectivity and opens no
    /// listener, so it has no endpoint to advertise. The trait anticipates
    /// exactly this case.
    fn address(&self) -> WorkerAddress {
        WorkerAddress::empty()
    }

    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            // `C + 1`: `C` data credits plus the one reserved terminal credit.
            // Credit is issued against *this* buffer and never against the
            // anchor's `frame_tx`, which has writers other than the mux.
            let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(core.limits.slot_buffer_depth());
            let drain = Arc::new(ingress::DrainSignal::new(core.drain_tx.clone()));
            core.drains
                .insert((anchor_id, session_id), Arc::clone(&drain));
            core.ingress
                .register_bind(anchor_id, session_id, frame_tx, drain);

            // `Weak`, and cancellable. A strong handle here would pin the whole
            // transport alive for the full accept window after the last owner
            // dropped it — a minute of leaked slots, batcher tasks and ingress
            // state per outstanding bind, and a `live_slots` gauge that only
            // comes back to zero when the timers do.
            let expiry = Arc::downgrade(&core);
            let cancel = core.cancel.clone();
            tokio::spawn(async move {
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(ACCEPT_TIMEOUT) => {}
                }
                let Some(core) = expiry.upgrade() else {
                    return;
                };
                // Whether or not the bind was still there, drop any drain
                // signal no attach collected. Without this an attach that
                // failed between `bind` and `take_drain_signal` would leak one
                // entry per attempt for the process's life.
                core.drains.remove(&(anchor_id, session_id));
                if core.ingress.expire_bind(anchor_id, session_id) {
                    tracing::warn!(
                        anchor_id,
                        session_id,
                        "messenger mux: no OpenSlot arrived before the accept window closed"
                    );
                }
            });

            Ok(frame_rx)
        })
    }

    /// Opens a slot at *this node's* limits.
    ///
    /// Only correct where both ends are configured alike, which is why the
    /// attach path never takes it: it calls
    /// [`connect_negotiated`](Self::connect_negotiated) with the window the
    /// receiver actually advertised. This exists so the transport is still
    /// usable through the bare [`FrameTransport`] trait — a mux wired in
    /// directly, with no anchor manager between the two ends, has no attach
    /// response to learn a window from.
    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let limits = self.core.limits;
        self.connect_negotiated(peer, anchor_id, session_id, limits)
    }
}

impl MessengerMuxTransport {
    /// The window this node advertises to a peer negotiating an attach.
    pub(crate) fn advertised_limits(&self) -> NegotiatedLimits {
        self.core.limits
    }

    /// Write what every batcher has staged, to every peer.
    ///
    /// All of them rather than one, because the caller cannot know the
    /// bucketing: a producer holds `StreamSender`s, and which peer each one
    /// lands on is a property of the anchor handle it attached to, resolved
    /// several layers below. A per-peer flush would be an API whose correct use
    /// requires knowing something the API deliberately hides.
    pub(crate) fn flush_batches(&self) {
        self.core.flush_batches();
    }

    /// Open a slot to `peer` at the limits its attach response advertised.
    ///
    /// The negotiated window is what lets the slot open *already granted*: the
    /// receiver sized its buffer from the same numbers it put on the wire, so
    /// there is nothing left for it to tell the sender and no round trip in
    /// which to tell it. Before negotiation the slot opened at zero credit and
    /// waited for a `CreditUpdate` the receiver emitted on `OpenSlot`, which
    /// cost one round trip per stream open.
    pub(crate) fn connect_negotiated(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
        limits: NegotiatedLimits,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            for _ in 0..CONNECT_ATTEMPTS {
                let batcher = core.batcher(peer);
                // Sized to the credit window for symmetry with the receive
                // buffer. It is *not* where credit starvation backpressures a
                // producer — the batcher drains this channel whether or not the
                // slot may send — but it **is** where a batcher parked on
                // admission does, because that park suspends the whole task,
                // inlet drain included. See
                // [the producer contract](self#the-producers-contract-under-the-mux).
                let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(limits.slot_buffer_depth());
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                if batcher
                    .open_slot(OpenSlotRequest {
                        anchor_id,
                        session_id,
                        inlet: inlet_rx,
                        credit: limits.open_credit(),
                        slot_byte_budget: limits.slot_byte_budget(),
                        ack: ack_tx,
                    })
                    .await
                    .is_err()
                {
                    continue;
                }
                match ack_rx.await {
                    Ok(Ok(())) => return Ok(inlet_tx),
                    // Evicted between resolution and delivery. A fresh batcher
                    // is one loop away.
                    Ok(Err(OpenRejected::Retired)) | Err(_) => continue,
                    Ok(Err(error)) => return Err(error.into()),
                }
            }
            Err(anyhow!(
                "messenger mux: could not open a slot to peer {peer} after {CONNECT_ATTEMPTS} attempts"
            ))
        })
    }
}
