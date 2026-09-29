// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batched, multiplexed streaming over the Messenger — the `messenger-mux-v2`
//! transport described by `docs/src/concepts/batched-streaming.md`.
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
//! On by default: `Velo::builder()` installs `MuxConfig::default()` unless
//! `.messenger_mux(...)` passes another. That registers the transport beside
//! the configured per-stream one and lets this node advertise
//! [`MESSENGER_MUX_KEY`] on its attach requests;
//! [`crate::streaming::negotiation`] is where an attach then picks between the
//! two, and picks the mux only when both peers named it. Setting
//! `enabled` back to `false` stops the advertisement, and on the node that
//! mints zero-RTT tickets that is the whole rollback; a producer rolled back
//! alone is refused by a consumer that still pre-binds for it, so the minting
//! side goes first or both go together — see [`MuxConfig::enabled`].
//!
//! ## The producer's contract under the mux
//!
//! `StreamSender::send` documents itself in transport-neutral terms — a bounded
//! channel, an awaited send when it fills — and for TCP and gRPC that is exactly
//! what happens, because those egress pumps drain at socket speed and a full
//! channel is transient. The mux behaves differently and it is worth stating
//! where a reader will meet it:
//!
//! - **`send` waits at the byte cap, not at the credit window.** The batcher
//!   pulls a slot's inlet whether or not the slot may send, into a mux-owned
//!   withheld queue, until that queue holds the per-slot byte budget (1 MiB by
//!   default). Then the slot pauses its inlet, the inlet fills, and `send`
//!   waits until credit drains the queue below the budget. So a producer runs
//!   up to the byte budget ahead of its credit, not up to the channel depth.
//! - **Terminals do not block.** `finalize`, `detach` and `Drop` reach the same
//!   inlet synchronously. On a full inlet the terminal waits in a task on the
//!   sender's runtime, so no caller thread blocks, and the terminal still goes
//!   after the records ahead of it. `docs/src/operations/saturation.md`
//!   describes the pause from the operator's side.
//!
//! A batcher parked on *admission* rather than on credit suspends the whole
//! task, inlet pulls included. That is bounded by the transport's own
//! progress, which is the position a socket was always in.
//!
//! Credit comes back from three places. Two of them visit only slots that
//! something named; the third is the whole-table backstop. A draining
//! consumer (the `StreamAnchor` reading its slot buffer directly, or an MPSC
//! anchor's pump) counts the record on that slot's [`ingress::DrainSignal`], lists the slot in its peer's
//! [`ingress::DirtySlots`], and posts the peer if the listing is new. The
//! **arrival path** then reconciles, on every inbound batch, the slots that
//! batch delivered into together with the slots in that set — so the credit a
//! stream's tail waits on rides the peer's next batch, which arrives in tens of
//! microseconds. The **doorbell** takes the same set when the sweep task
//! answers a wake, no more often than once per
//! [`MuxConfig::drain_visit_floor`]; it is what covers a peer that has gone
//! quiet. The **periodic tick** walks the whole table, for the slot nothing
//! named — one parked with nothing arriving *and* nothing being taken out —
//! and it carries batcher eviction.
//!
//! The set is a doorbell, not a ledger: a listing names a slot and carries no
//! quantity. The quantity is the count on that slot's own signal, and
//! `IngressSlot::reconcile` taking it is the only thing that decides how much
//! credit was freed. That is what lets the three paths run concurrently — a
//! redundant visit finds a count of zero, where a delta would double-count.
//!
//! The first design released an exact `credit.release(1)` per handoff.
//! Releasing an amount from the consumer is the part that was not adopted:
//! releasing needs the peer's mutex, and taking it per record would trade a
//! periodic cost for a worse per-record one. See "Credit return" in
//! `docs/src/development/batched-streaming-design.md`.

mod config;
pub(crate) mod flow_control;
pub(crate) mod ingress;
pub(crate) mod peer_batcher;
pub(crate) mod protocol;
mod sweep;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use velo_ext::{TransportKey, WorkerAddress, WorkerId};

use self::flow_control::NegotiatedLimits;
use self::ingress::IngressRegistry;
use self::peer_batcher::{
    BatcherContext, BatcherHandle, BatcherMap, OpenRejected, OpenSlotRequest,
};
use crate::messenger::{Context, Handler, Messenger};
use crate::observability::{MuxMetricsHandle, VeloMetrics};
use crate::streaming::transport::FrameTransport;

pub use self::config::{AutoFlush, FlushPolicy, MuxConfig};

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
pub const MESSENGER_MUX_KEY: &str = "messenger-mux-v2";

/// The active-message handler every batch travels through.
pub(crate) const STREAM_BATCH_HANDLER: &str = "_stream_batch";

/// How long a bind waits for the `OpenSlot` that claims it.
///
/// Deliberately the same 60 s the TCP transport gives a pending session, and
/// deliberately measuring the same thing: "time until a batch bearing this
/// `OpenSlot` arrives". That sentence holds without qualification for
/// `FrameTransport::bind`'s attach-path caller: a sender has already asked by
/// the time the bind exists, so the window is one response leg plus one batch
/// leg, and `OpenSlot` is eager precisely so it cannot quietly become "time
/// until the producer produces its first token" there.
///
/// It does not hold for `MessengerMuxTransport::prebind`'s zero-RTT caller,
/// where the same clock starts before any sender has asked at all: the window
/// there is envelope transit plus however long the ticket sits in a request
/// envelope before its worker calls `open_anchor_stream`, which can be exactly
/// the producer-side wait the paragraph above rules out for `bind`. See
/// `AnchorManager::prebind_anchor`'s doc for that bound. An attach that adopts
/// an existing pre-bind does not restart this timer either way — adoption
/// takes over the bind `prebind` already registered rather than calling
/// `bind` again, so it inherits whatever is left of the 60 s, not a fresh one.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Attempts `connect` makes before giving up on a batcher that keeps retiring
/// underneath it. Two is already generous — losing the race twice requires two
/// eviction sweeps inside one attach.
const CONNECT_ATTEMPTS: usize = 3;

/// The lane drain signals post their peer on and the sweep task answers.
///
/// Unbounded, because a refused wake strands credit. Only the drain that newly
/// lists a slot posts its peer; every later drain of that slot rides the
/// listing and posts nothing, so a wake dropped here leaves the slot listed
/// with no visit coming until a batch or the periodic tick arrives -- and a
/// peer whose sender is parked out of credit sends no batch.
///
/// Occupancy is bounded without a capacity. A post needs the peer's `pending`
/// flag to go from down to up, and only a visit to that peer or the periodic
/// tick takes it down; each doorbell visit consumes the entry that summoned it,
/// so the lane holds about one entry per peer, plus at most one more per peer
/// per tick while the sweep task is behind.
fn drain_wake_lane() -> (flume::Sender<WorkerId>, flume::Receiver<WorkerId>) {
    flume::unbounded::<WorkerId>()
}

/// The `messenger-mux-v2` [`FrameTransport`].
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
    /// The runtime the mux was built on, for work that has to spawn when the
    /// caller has none: a slot close from an anchor dropped off-runtime.
    runtime: Option<tokio::runtime::Handle>,
    /// Peers with credit to return, posted by draining consumers. See
    /// [`ingress::DrainSignal`].
    drain_tx: flume::Sender<WorkerId>,
    drain_rx: flume::Receiver<WorkerId>,
    /// Drain signals waiting to be collected by the attach that will start the
    /// feed and watchdog (or MPSC pump) holding them.
    ///
    /// `bind` cannot hand this back directly — `FrameTransport::bind` returns a
    /// receiver and nothing else, and widening that trait would be a breaking
    /// change to `velo-ext` for every out-of-tree implementor. So the signal is
    /// parked here for the attach path to take, which it does a few lines after
    /// `bind` returns. Take-once: whoever collects it owns it, and the bind
    /// expiry that already exists drops any that was never collected.
    drains: DashMap<(u64, u64), Arc<ingress::DrainSignal>>,
    /// Accept-window deadlines, one per bind, oldest first.
    ///
    /// Every bind gets the same `ACCEPT_TIMEOUT`, so pushing at bind time keeps
    /// the queue in deadline order and the sweep only ever pops its front. A
    /// bind claimed or released early stays queued and costs a failed lookup
    /// when its deadline comes; that is cheaper than finding and removing it.
    /// It used to be one timer task per bind, which nothing but shutdown
    /// cancelled: a claimed bind's task lived out the full minute, so a
    /// frontend opening 2,000 streams a second carried about 120,000 idle
    /// tasks.
    bind_deadlines: std::sync::Mutex<VecDeque<(tokio::time::Instant, (u64, u64))>>,
    /// A barrier handed to every batcher this core spawns, installed by the
    /// tests that need one held mid-wake. See [`peer_batcher::test_hooks`].
    #[cfg(test)]
    hooks: std::sync::OnceLock<Arc<peer_batcher::test_hooks::TestHooks>>,
}

impl MessengerMuxTransport {
    pub(crate) fn request_stop(&self, peer: WorkerId, slot: protocol::SlotId, session_id: u64) {
        self.core.return_credit(
            peer,
            vec![peer_batcher::ReplyRecord::LifecycleSlot {
                slot,
                session_id,
                cancel: false,
            }],
        );
    }

    /// Take the [`ingress::DrainSignal`] `bind` parked for this pair.
    ///
    /// Called once by the attach path, between `bind` returning and the
    /// consumer side being started. Returns `None` for a pair this transport did not bind,
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
    /// handler-deregistration hook. The messenger does not refuse a duplicate
    /// name either; a second registration silently replaces the first handler.
    /// So the one-mux rule lives in `VeloBuilder::messenger_mux`, which fails
    /// when a mux config is already set, and `VeloBuilder::build` calls this
    /// once, so no two muxes race for one handler name.
    ///
    /// Fails on `initial_credit = 0`, which is not a small window but the wire
    /// encoding of *"not offering the mux"*. A node that installed one would
    /// advertise a key and then tell every peer to ignore it.
    ///
    /// Fails on `credit_sweep_interval = 0` too. The sweep ticks on a
    /// `tokio::time::interval`, which panics on a zero period — inside the
    /// spawned sweep task, so without this check the build returned `Ok` and
    /// the mux ran with no periodic credit backstop, no batcher eviction, and
    /// nobody reading the drain doorbell, which that task alone consumes.
    pub(crate) fn new(
        messenger: Arc<Messenger>,
        config: MuxConfig,
        metrics: Option<Arc<VeloMetrics>>,
    ) -> Result<Arc<Self>> {
        let limits = NegotiatedLimits::from_wire(config.initial_credit, config.slot_byte_budget)
            .map_err(|error| anyhow!("messenger mux: {error}"))?;
        if config.credit_sweep_interval.is_zero() {
            return Err(anyhow!(
                "messenger mux: credit_sweep_interval must be non-zero; the sweep ticks on it"
            ));
        }
        // Normalised so every reader of the config sees the effective budget
        // rather than the "use the default" zero.
        let config = MuxConfig {
            slot_byte_budget: limits.slot_byte_budget(),
            ..config
        };
        let (drain_tx, drain_rx) = drain_wake_lane();
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
            runtime: tokio::runtime::Handle::try_current().ok(),
            bind_deadlines: std::sync::Mutex::default(),
            #[cfg(test)]
            hooks: std::sync::OnceLock::new(),
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
        // Records and credit of open streams: see `register_drain_exempt_handler`.
        messenger.register_drain_exempt_handler(handler)?;

        sweep::spawn_sweep(&core);

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
                            hooks: self.hooks.get().cloned(),
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

        if outcome.replies.is_empty()
            && outcome.grants.is_empty()
            && outcome.peer_closes.is_empty()
            && outcome.peer_stops.is_empty()
        {
            return;
        }

        let batcher = self.batcher(peer);
        for (slot, session_id, cancel) in outcome.peer_stops {
            batcher.peer_stopped(slot, session_id, cancel);
        }
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

    /// Queue control records back to `peer`, re-resolving while the batcher
    /// in hand has stopped reading.
    ///
    /// Control is coalesced state rather than a queue, so nothing here can fail
    /// on the write — `reply`'s answer is what stands in for a `SendError`, and
    /// it is decided under the inbox lock the batcher's last drain also takes.
    /// So a reply either rode that drain or comes back refused, with no third
    /// case, and a refused one goes to whatever batcher now owns the peer. A
    /// liveness check *before* the write, which this used to be, cannot see a
    /// task that has already taken its last drain, and a reply posted there
    /// was applied by nobody. That is not always credit nobody wanted:
    /// eviction needs zero live slots on both sides, and the `CloseSlot`
    /// `close_claimed_slot` posts is what brought the ingress side to zero —
    /// the idle producer it names has no other way to learn, because every
    /// later record it sends is dropped here as `ClosedSlot` with no reply.
    ///
    /// The loop runs once in practice. A further turn needs the batcher
    /// `batcher()` just handed back to be evicted inside this call, which is
    /// a sweep tick landing in microseconds of straight-line code; it cannot
    /// spin, because a tick is what each turn waits for.
    fn send_replies(
        &self,
        batcher: &Arc<BatcherHandle>,
        peer: WorkerId,
        replies: &[peer_batcher::ReplyRecord],
    ) {
        if batcher.reply(replies) {
            return;
        }
        while !self.batcher(peer).reply(replies) {}
    }

    /// Reconcile every slot of one peer, on the periodic tick.
    ///
    /// The whole-table walk, and the only visitor of a slot nobody named — the
    /// one parked with nothing arriving and nothing being taken out.
    fn sweep_peer(&self, peer: WorkerId) {
        // Taken down before the reconcile, not after: a record drained while
        // this visit is in progress must be able to post a fresh wake, or its
        // credit waits for the periodic backstop.
        self.ingress.clear_pending_wake(peer);
        self.return_credit(peer, self.ingress.sweep_credit(peer));
    }

    /// One doorbell-driven visit: reconcile the slots of the peer that rang.
    ///
    /// Scoped to the slots listed in that peer's dirty set, because a
    /// wake means those slots drained and says nothing about the rest — and
    /// this walk holds the mutex the inbound batch path takes.
    ///
    /// Counted here rather than where the wake is received, so the series
    /// measures walks and not wakes — a wake the floor deferred is counted once,
    /// on the visit it coalesced into.
    fn visit_drained_peer(&self, peer: WorkerId) {
        if let Some(metrics) = &self.metrics {
            metrics.drain_visit();
        }
        self.ingress.clear_pending_wake(peer);
        self.return_credit(peer, self.ingress.sweep_drained(peer));
    }

    /// Hand a reconcile pass's grants to the peer's batcher.
    fn return_credit(&self, peer: WorkerId, replies: Vec<peer_batcher::ReplyRecord>) {
        if replies.is_empty() {
            return;
        }
        let batcher = self.batcher(peer);
        self.send_replies(&batcher, peer, &replies);
    }

    /// Retire a slot whose consumer has gone and tell its owner.
    ///
    /// Two halves, and both are needed. The local retire is what returns
    /// `live_slots` to zero — nothing else does, because the sweep reads
    /// only what the slot's own consumer counted drained, and a consumer that
    /// is gone counts nothing; the next record to arrive would
    /// close the slot by finding its receiver gone, but an idle producer
    /// sends none. The reply is what that idle producer needs, since the
    /// fault that carries the same news to it otherwise rides on the next
    /// record it sends.
    fn close_claimed_slot(&self, peer: WorkerId, slot: protocol::SlotId, session_id: Option<u64>) {
        // Resolving a batcher may spawn its task, and this runs from a `Drop`
        // that can land on a thread with no runtime under it. Enter the runtime
        // the mux was built on in that case: waiting for the slot's next record
        // is no answer, because a dead or parked sender sends none. Checked
        // before touching the ingress table, not after, so a mux with no
        // runtime at all leaves the slot as it found it rather than retiring
        // it with nowhere to post the reply.
        let _entered = match tokio::runtime::Handle::try_current() {
            Ok(_) => None,
            // Only for a thread with no runtime. A destroyed thread-local (a
            // drop from a TLS destructor) would make `enter` panic inside a
            // `Drop`, so that case keeps the quiet return below.
            Err(error) if !error.is_missing_context() => {
                tracing::debug!(
                    peer = %peer,
                    "messenger mux: runtime context unavailable here; the peer learns on its next record"
                );
                return;
            }
            Err(_) => match self.runtime.as_ref() {
                Some(runtime) => Some(runtime.enter()),
                None => {
                    tracing::debug!(
                        peer = %peer,
                        "messenger mux: no runtime to post a slot close on; the peer learns on its next record"
                    );
                    return;
                }
            },
        };
        let Some(reply) =
            self.ingress
                .close_consumer_gone(peer, slot, self.metrics.as_ref(), session_id)
        else {
            return;
        };
        if let Some(metrics) = &self.metrics {
            metrics.slot_closed();
        }
        let batcher = self.batcher(peer);
        self.send_replies(&batcher, peer, &[reply]);
    }

    /// Close the accept window on every bind whose deadline has passed.
    ///
    /// Runs on the sweep tick, so a window closes up to one
    /// `credit_sweep_interval` late -- 200 ms on a 60 s window by default.
    fn expire_binds(&self, now: tokio::time::Instant) {
        let due: Vec<(u64, u64)> = {
            let mut deadlines = self
                .bind_deadlines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ready = deadlines.partition_point(|(deadline, _)| *deadline <= now);
            let due = deadlines.drain(..ready).map(|(_, key)| key).collect();
            // A burst of binds leaves the queue's capacity behind it. Give it
            // back only once the queue is nearly empty: the shrink copies what
            // is left while holding the lock every bind takes, so shrinking a
            // queue still tens of thousands deep would stall new streams.
            if deadlines.len() <= 1024 && deadlines.capacity() > 8192 {
                deadlines.shrink_to(2048);
            }
            due
        };
        for (anchor_id, session_id) in due {
            // Whether or not the bind was still there, drop any drain signal
            // no attach collected. Without this an attach that failed between
            // `bind` and `take_drain_signal` would leak one entry per attempt
            // for the process's life.
            self.drains.remove(&(anchor_id, session_id));
            if self.ingress.expire_bind(anchor_id, session_id) {
                tracing::warn!(
                    anchor_id,
                    session_id,
                    "messenger mux: no OpenSlot arrived before the accept window closed"
                );
            }
        }
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

/// Register the receive buffer for one `(anchor_id, session_id)` and open the
/// accept window on it.
///
/// The body [`FrameTransport::bind`] and
/// [`MessengerMuxTransport::prebind`] share. `bind` is async because the trait
/// is; **nothing in here awaits**, and that is what lets the zero-RTT path call
/// it synchronously while registering a request. The accept window is a
/// deadline the sweep expires (`MuxCore::expire_binds`), queued here, once,
/// rather than in two places that would drift.
fn open_bind(core: &Arc<MuxCore>, anchor_id: u64, session_id: u64) -> flume::Receiver<Vec<u8>> {
    // `C + 1`: `C` data credits plus the one reserved terminal credit.
    // Credit is issued against *this* buffer and never against the
    // anchor's `frame_tx`, which has writers other than the mux.
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(core.limits.slot_buffer_depth());
    let drain = Arc::new(ingress::DrainSignal::new(core.drain_tx.clone()));
    core.drains
        .insert((anchor_id, session_id), Arc::clone(&drain));
    core.ingress
        .register_bind(anchor_id, session_id, frame_tx, drain);

    // A deadline, not a task: the sweep expires it (`MuxCore::expire_binds`).
    // Nothing here may pin the core either, which a task holding a strong
    // handle would, for the full window after the last owner dropped it.
    // The clock is read under the lock, so pushes land in deadline order.
    let mut deadlines = core
        .bind_deadlines
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    deadlines.push_back((
        tokio::time::Instant::now() + ACCEPT_TIMEOUT,
        (anchor_id, session_id),
    ));
    drop(deadlines);

    frame_rx
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
        Box::pin(async move { Ok(open_bind(&core, anchor_id, session_id)) })
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

    /// Bind a slot before any sender has asked for one.
    ///
    /// The synchronous twin of [`FrameTransport::bind`], and identical to it:
    /// the trait's `bind` is async only because the trait is, and its body has
    /// no await in it. Zero-RTT setup needs the receiver *now*, while
    /// registering a request, so it takes this door instead of paying a future
    /// for nothing.
    ///
    /// Nothing about the resulting bind is special. A peer's `OpenSlot` claims
    /// it by the same `(anchor_id, session_id)` lookup, the accept window runs
    /// the same 60 s, and [`release_bind`](Self::release_bind) is what an owner
    /// that gives up before then calls.
    pub(crate) fn prebind(&self, anchor_id: u64, session_id: u64) -> flume::Receiver<Vec<u8>> {
        open_bind(&self.core, anchor_id, session_id)
    }

    /// Give back a bind nobody claimed, along with the drain signal parked with
    /// it.
    ///
    /// The accept window does the same thing when it expires, and stays as the
    /// backstop. This is for the owner that already knows: a pre-bound anchor
    /// whose request died before its first token knows a minute earlier than
    /// the timer does, and at the rate a frontend registers requests that
    /// minute is thousands of leaked binds.
    ///
    /// Idempotent: a bind already claimed or already released is not there to
    /// remove, and removing nothing is the right answer for both.
    pub(crate) fn release_bind(&self, anchor_id: u64, session_id: u64) {
        self.core.drains.remove(&(anchor_id, session_id));
        self.core.ingress.expire_bind(anchor_id, session_id);
    }

    /// Retire a live slot whose consumer has gone, and tell the peer that owns
    /// it to abandon its end.
    ///
    /// The receive side already reaches this verdict on its own — a record
    /// arriving for a slot whose consumer dropped the receiver faults with
    /// `CloseReason::UnknownSlot` — but only on the *next* record, and an idle
    /// producer sends none. This is the same close on a different trigger.
    ///
    /// Idempotent, and silent where there is nothing to close: a stream that
    /// ended on its own terminal retired the slot then, so the ordinary end of
    /// a stream costs no extra record on the wire.
    pub(crate) fn cancel_claimed_session(
        &self,
        peer: WorkerId,
        slot: protocol::SlotId,
        session_id: u64,
    ) {
        self.core.close_claimed_slot(peer, slot, Some(session_id));
    }

    #[cfg(test)]
    pub(crate) fn close_claimed_slot(&self, peer: WorkerId, slot: protocol::SlotId) {
        self.core.close_claimed_slot(peer, slot, None);
    }

    /// Slot closes that went as far as taking a peer's ingress lock.
    #[cfg(test)]
    pub(crate) fn consumer_gone_calls(&self) -> usize {
        self.core.ingress.consumer_gone_calls()
    }

    /// Binds registered and neither claimed nor released.
    #[cfg(test)]
    pub(crate) fn pending_binds(&self) -> usize {
        self.core.ingress.bind_count()
    }

    /// Drain signals a bind parked and no attach has collected.
    #[cfg(test)]
    pub(crate) fn parked_drains(&self) -> usize {
        self.core.drains.len()
    }

    /// Live receive-side slots for `peer`.
    #[cfg(test)]
    pub(crate) fn live_ingress_slots(&self, peer: WorkerId) -> usize {
        self.core.ingress.live_slots(peer)
    }

    /// The ids of `peer`'s live receive-side slots.
    ///
    /// A test that has to name a slot would otherwise have to re-derive the
    /// sender's allocation order, which is the allocator's business and not the
    /// test's.
    #[cfg(test)]
    pub(crate) fn live_slot_ids(&self, peer: WorkerId) -> Vec<protocol::SlotId> {
        self.core.ingress.live_slot_ids(peer)
    }

    /// The window one of `peer`'s live receive-side slots opened holding.
    #[cfg(test)]
    pub(crate) fn slot_open_terms(
        &self,
        peer: WorkerId,
        id: protocol::SlotId,
    ) -> Option<(u32, u64)> {
        self.core.ingress.slot_open_terms(peer, id)
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
        self.connect_controlled(peer, anchor_id, session_id, limits, None)
    }

    pub(crate) fn connect_controlled(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
        limits: NegotiatedLimits,
        lifecycle: Option<(CancellationToken, CancellationToken)>,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            for _ in 0..CONNECT_ATTEMPTS {
                let batcher = core.batcher(peer);
                // Sized to the credit window for symmetry with the receive
                // buffer. A producer waits on it once its slot pauses at the
                // byte cap, or while the batcher is parked on admission. See
                // [the producer contract](self#the-producers-contract-under-the-mux).
                let (inlet_tx, inlet_rx) = flume::bounded::<Vec<u8>>(limits.slot_buffer_depth());
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                if batcher
                    .open_slot(OpenSlotRequest {
                        anchor_id,
                        session_id,
                        inlet: inlet_rx,
                        lifecycle: lifecycle.clone(),
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
