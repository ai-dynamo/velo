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
//! One deviation from `BATCHING.md` survives: `reader_pump` returns credit by
//! *reconciliation* — the mux compares the buffer's occupancy against what it
//! admitted — rather than through the exact `credit.release(1)` hook the
//! document describes. The effect is the same and the sweep bounds the latency;
//! what it costs is that a return is one sweep tick late when no further batch
//! arrives to drive reconciliation on the arrival path.

pub(crate) mod flow_control;
pub(crate) mod ingress;
pub(crate) mod peer_batcher;
pub(crate) mod protocol;
#[cfg(test)]
mod tests;

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
pub(crate) const MESSENGER_MUX_KEY: &str = "messenger-mux-v1";

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
    /// The arrival path returns credit on every inbound batch, so this only
    /// matters for a slot that has parked with nothing further arriving to
    /// trigger reconciliation — where it is the difference between resuming and
    /// deadlocking, at one interval of added latency.
    pub credit_sweep_interval: Duration,
    /// How long a batcher may sit idle with no slots before it is evicted.
    pub batcher_idle_ttl: Duration,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_batch_bytes: 60 * 1024,
            initial_credit: 256,
            slot_byte_budget: DEFAULT_SLOT_BYTE_BUDGET,
            peer_byte_budget: DEFAULT_PEER_BYTE_BUDGET,
            credit_sweep_interval: Duration::from_millis(2),
            batcher_idle_ttl: Duration::from_secs(60),
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
}

impl MessengerMuxTransport {
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

    /// One sweep tick: return credit, then age out idle batchers.
    fn sweep(&self) {
        for peer in self.ingress.peers() {
            let replies = self.ingress.sweep_credit(peer);
            if !replies.is_empty() {
                let batcher = self.batcher(peer);
                self.send_replies(&batcher, peer, &replies);
            }
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

/// Spawn the periodic credit-return and eviction sweep.
fn spawn_sweep(core: &Arc<MuxCore>) {
    let weak = Arc::downgrade(core);
    let cancel = core.cancel.clone();
    let interval = core.config.credit_sweep_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let Some(core) = weak.upgrade() else {
                return;
            };
            core.sweep();
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
            core.ingress.register_bind(anchor_id, session_id, frame_tx);

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
