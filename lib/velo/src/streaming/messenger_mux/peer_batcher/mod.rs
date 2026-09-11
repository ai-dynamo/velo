// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The per-peer egress batcher — one task, one peer, every stream to it.
//!
//! A node talking to Y peers holds Y of these however many streams it holds,
//! which is the whole O(X) → O(Y) argument in `BATCHING.md` § "Riding the
//! Messenger" made concrete. The batcher owns the slot table for its peer, packs
//! records from every slot into `_stream_batch` active messages, and is the one
//! place that decides when a batch is cut.
//!
//! ## What backpressure means here
//!
//! Control reaching it is **coalesced state**, not a queue — see [`control`] for
//! why an unbounded mailbox is unbounded memory the moment a flush parks.
//!
//! There is no socket to fill, so the batcher learns its peer is congested the
//! only way a messenger user can: **admission**. A fire send completes at
//! admission, so awaiting the [`FireResult`] of a flush parks the batcher — not
//! a runtime worker — until the frame reaches the transport's send channel, and
//! parks it *in order*, because the target's admission gate is FIFO. That is why
//! the flush is awaited rather than fired and forgotten.
//!
//! The other direction of the same coin: **any** failed admission is epoch
//! death. `FireResult` erases `AdmissionError` into a string, so the
//! `{ConnectionReplaced, ChannelClosed}` pair `BATCHING.md` names cannot be
//! matched on — but the superset is the correct rule anyway. A batch that never
//! reached the wire leaves a `frame_seq` gap in every slot packed into it, and
//! the mux does not retransmit, so those slots can never make progress again.
//! Failing them and bumping the epoch is what makes "exactly one `Dropped` per
//! failed live slot" provable.
//!
//! ## When the batch is cut
//!
//! [`flush_gate`] owns that decision and nothing else does. The loop below
//! stages work, drains everything already queued behind it, and then asks the
//! gate once. Under the default policy the answer is yes unless the batch
//! holds nothing but pending credit replies, in which case `flush_gate` holds
//! it for [`MuxConfig::reply_linger`](super::MuxConfig::reply_linger)
//! instead. Under [`FlushPolicy::Manual`](super::FlushPolicy::Manual) the
//! answer is no until the application says otherwise, except that a pending
//! reply still ages out after `reply_linger` and takes whatever else is
//! staged with it. The kick the application says otherwise with arrives as
//! coalesced control for the reason everything else does.
//!
//! ## Draining X channels from one task
//!
//! [`slot_stream`] explains the `SelectAll` arrangement and why every inlet is
//! drained unconditionally, credit or no credit: a slot parked on credit would
//! otherwise leave its producer's terminal waiting on a channel that never makes
//! room. The batcher's half of that contract is the per-slot withheld queue —
//! where a record waits when the slot cannot send it — and the byte cap on that
//! queue, which is what bounds the memory the arrangement costs.

mod control;
mod flush_gate;
mod records;
mod slot_stream;
#[cfg(test)]
pub(crate) mod test_hooks;
#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use futures::future::FutureExt;
use futures::stream::{SelectAll, StreamExt};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use velo_ext::WorkerId;

use self::control::{ControlInbox, DrainedControl, OwnedControl, PeerControl};
use self::flush_gate::{FlushGate, linger_until};
pub(crate) use self::slot_stream::AllocError;
use self::slot_stream::{EgressSlots, SlotItem, SlotStream};
#[cfg(test)]
use self::test_hooks::TestHooks;
use self::writer::BatchWriter;
use super::MuxConfig;
use super::protocol::{
    BATCH_HEADER_LEN, BatchEncoder, CloseReason, EncodeError, RecordType, SlotId,
    record_encoded_len,
};
use crate::messenger::Messenger;
use crate::observability::{BatcherWake, MuxDropReason, MuxMetricsHandle};
use crate::streaming::messenger_mux::flow_control::{CreditClass, SlotCredit};
use crate::streaming::sender::is_terminal_sentinel;
use crate::transports::AdmissionState;

/// The per-peer batcher registry, keyed by the batching key from `BATCHING.md`
/// § "Why bucketing by destination is free".
pub(crate) type BatcherMap = DashMap<WorkerId, Arc<BatcherHandle>>;

/// Attach requests queued for a batcher.
///
/// The one thing that cannot coalesce — each carries its own channel and its own
/// caller waiting on an ack — so it keeps a queue, and a **bounded** one. A full
/// queue makes an attach wait rather than fail, which is the right answer: the
/// caller is already `await`ing an ack, and there are only ever as many in
/// flight as there are concurrent `connect` calls.
pub(crate) struct OpenSlotRequest {
    pub(crate) anchor_id: u64,
    pub(crate) session_id: u64,
    pub(crate) inlet: flume::Receiver<Vec<u8>>,
    /// The ledger the slot opens with — the window the receiver advertised on
    /// its attach response, already granted.
    ///
    /// Per slot rather than per batcher because a batcher serves every stream
    /// to one peer and each was negotiated separately; an MPSC anchor and an
    /// SPSC one on the same peer need not agree.
    pub(crate) credit: SlotCredit,
    /// Bytes this slot may withhold, likewise negotiated.
    pub(crate) slot_byte_budget: u32,
    pub(crate) ack: oneshot::Sender<Result<(), OpenRejected>>,
}

/// Attach requests one batcher may have queued at once.
const OPEN_QUEUE_DEPTH: usize = 64;

/// A control record the receiving side sends back to a slot's owner.
///
/// `CloseSlot` is bidirectional on the wire and carries no direction bit, so the
/// reason supplies one: `TerminalSent` and `PeerGone` only ever travel owner →
/// receiver, `UnknownSlot` and `ProtocolError` only ever travel receiver →
/// owner. That partition is what lets a node tell "close the slot I opened" from
/// "close the slot you opened" when both sides may hold a slot at the same dense
/// index.
///
/// `RejectSlot` writes the identical wire frame as `CloseSlot` — the peer
/// cannot tell them apart and does not need to. The split is internal: a
/// `CloseSlot` answers an `OpenSlot` this side admitted, for a slot still in
/// its table (bounded by that table, so keeping it is safe); a `RejectSlot`
/// answers one this side never admitted (out of range, a collision, or a bind
/// that never existed), so keeping every one a hostile peer can name is not —
/// even the rare rejection whose id happens to match a slot admitted under a
/// different `OpenSlot`. See `ControlState` for where that distinction is
/// enforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyRecord {
    /// Additional data credit for the peer's slot.
    CreditUpdate { slot: SlotId, delta: u32 },
    /// Tell the peer to abandon a slot the ingress holds and is closing.
    CloseSlot { slot: SlotId, reason: CloseReason },
    /// Tell the peer to abandon an `OpenSlot` the ingress never admitted.
    RejectSlot { slot: SlotId, reason: CloseReason },
}

/// Why an `OpenSlot` command was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OpenRejected {
    /// This batcher was evicted between the caller finding it and the command
    /// arriving. The caller re-resolves and gets a fresh batcher.
    #[error("peer batcher was retired; retry with a fresh one")]
    Retired,
    /// The slot table is full.
    #[error("{0}")]
    Alloc(#[from] AllocError),
}

/// Registry-side handle to a running batcher.
///
/// Carries the counters the eviction sweep reads, so the sweep never has to talk
/// to the task to decide whether it is idle.
pub(crate) struct BatcherHandle {
    opens: flume::Sender<OpenSlotRequest>,
    control: Arc<ControlInbox>,
    live_slots: AtomicUsize,
    idle_ticks: AtomicU32,
    retired: AtomicBool,
}

impl BatcherHandle {
    /// Queue an attach, waiting if this batcher already has `OPEN_QUEUE_DEPTH`
    /// of them outstanding.
    pub(crate) async fn open_slot(
        &self,
        request: OpenSlotRequest,
    ) -> Result<(), flume::SendError<OpenSlotRequest>> {
        self.opens.send_async(request).await
    }

    /// An inbound `CreditUpdate` for one of this peer's slots.
    pub(crate) fn grant(&self, slot: SlotId, delta: u32) {
        self.control.grant(slot, delta);
    }

    /// The receiver asked us to abandon one of our slots.
    pub(crate) fn peer_closed(&self, slot: SlotId, reason: CloseReason) {
        self.control.peer_closed(slot, reason);
    }

    /// Queue control records to send back to this peer.
    ///
    /// `false` means the task has taken its last drain and queued nothing;
    /// the caller re-resolves the peer's batcher and posts there. Control is
    /// state rather than a queue, so a writer cannot learn from the send that
    /// nobody will read it — this answer is what stands in for that, and it
    /// is decided under the inbox lock the last drain also takes
    /// ([`ControlInbox::close`]), so there is no window in which a reply is
    /// accepted and never read. A liveness flag checked *before* the write
    /// cannot see a task that has already drained for the last time, and the
    /// reply that lands there is not always credit nobody wanted: the
    /// `CloseSlot` the mux's `close_claimed_slot` posts is what made this
    /// batcher evictable, and the idle producer it names learns no other way.
    pub(crate) fn reply(&self, records: &[ReplyRecord]) -> bool {
        self.control.reply(records)
    }

    /// The sweep evicted this batcher from the registry.
    pub(crate) fn retire(&self) {
        self.control.retire();
    }

    /// Write whatever this batcher has staged.
    ///
    /// Sync and non-blocking: it sets the coalesced kick and returns, leaving
    /// the write — and any wait for the peer to admit it — on the batcher's own
    /// task. A producer loop calling this every forward pass therefore never
    /// blocks on a congested peer, which stays credit and admission's job.
    pub(crate) fn kick_flush(&self) {
        self.control.kick_flush();
    }

    /// Control entries pending, for the bound the stalled-admission test pins.
    #[cfg(test)]
    pub(crate) fn pending_control(&self) -> usize {
        self.control.pending_len()
    }

    /// Whether the task has taken its last drain, so a reply posted now is
    /// refused rather than taken.
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.control.is_closed()
    }

    /// Advance the idle counter and report the new value.
    pub(crate) fn tick_idle(&self) -> u32 {
        self.idle_ticks
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    /// Claim this batcher for eviction, under the registry's shard lock.
    ///
    /// The `swap` is what makes the claim exclusive: the sweep removes the entry
    /// and the task observes `retired` on its next wake, so the decision is made
    /// in one place and acted on in another without a second round trip.
    pub(crate) fn try_retire(&self, idle_threshold: u32) -> bool {
        self.live_slots.load(Ordering::Relaxed) == 0
            && self.idle_ticks.load(Ordering::Relaxed) >= idle_threshold
            && !self.retired.swap(true, Ordering::AcqRel)
    }

    fn mark_active(&self) {
        self.idle_ticks.store(0, Ordering::Relaxed);
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
}

/// Everything a batcher task needs that is not per-peer state.
pub(crate) struct BatcherContext {
    pub(crate) messenger: Arc<Messenger>,
    pub(crate) config: MuxConfig,
    pub(crate) metrics: Option<MuxMetricsHandle>,
    pub(crate) epochs: Arc<AtomicU64>,
    pub(crate) batchers: Arc<BatcherMap>,
    pub(crate) cancel: CancellationToken,
    /// A barrier in the run loop, installed only by the tests that need to stop
    /// it mid-wake. See [`test_hooks`].
    #[cfg(test)]
    pub(crate) hooks: Option<Arc<TestHooks>>,
}

/// Spawn a batcher for `peer` and return its registry handle.
pub(crate) fn spawn(peer: WorkerId, ctx: BatcherContext) -> Arc<BatcherHandle> {
    let (opens, open_rx) = flume::bounded(OPEN_QUEUE_DEPTH);
    let control = Arc::new(ControlInbox::new(ctx.metrics.clone()));
    let handle = Arc::new(BatcherHandle {
        opens,
        control: Arc::clone(&control),
        live_slots: AtomicUsize::new(0),
        idle_ticks: AtomicU32::new(0),
        retired: AtomicBool::new(false),
    });
    let epoch = ctx.epochs.fetch_add(1, Ordering::Relaxed);
    let gate = FlushGate::new(
        ctx.config.flush_policy,
        ctx.config.reply_linger,
        ctx.metrics.clone(),
    );
    let async_open_ack = ctx.config.async_open_ack;
    let writer = BatchWriter::new(
        Arc::clone(&ctx.messenger),
        peer,
        ctx.config,
        ctx.metrics.clone(),
        epoch,
    );
    let batcher = Batcher {
        peer,
        metrics: ctx.metrics,
        handle: Arc::clone(&handle),
        epochs: ctx.epochs,
        batchers: ctx.batchers,
        cancel: ctx.cancel,
        control,
        gate,
        writer,
        slots: EgressSlots::default(),
        streams: SelectAll::new(),
        stopping: false,
        async_open_ack,
        staged_credit: Vec::new(),
        #[cfg(test)]
        hooks: ctx.hooks,
    };
    tokio::spawn(batcher.run(open_rx));
    handle
}

/// One unit of work pulled by the main loop.
enum Work {
    Open(OpenSlotRequest),
    Control(DrainedControl),
    Slot(u32, SlotItem),
    /// A linger window elapsed. Carries nothing: it exists to end the park, and
    /// the decision it leads to is [`FlushGate::should_flush`] reading the
    /// deadline as state.
    Linger,
}

/// Whether [`Batcher::fire_singleton`] may skip the fence on a synchronously
/// admitted dispatch.
///
/// Only `open_detached`'s `OpenSlot` qualifies — see `fire_singleton`'s doc
/// for why `send_singleton`'s rendezvous records never do.
enum FenceSkip {
    /// Skip the fence when [`AdmissionState::Admitted`] already applies.
    IfAdmitted,
    /// Always fence, regardless of how the admission resolved.
    Never,
}

struct Batcher {
    peer: WorkerId,
    metrics: Option<MuxMetricsHandle>,
    handle: Arc<BatcherHandle>,
    epochs: Arc<AtomicU64>,
    batchers: Arc<BatcherMap>,
    cancel: CancellationToken,
    /// The coalesced control state this task drains.
    control: Arc<ControlInbox>,
    /// Whether the staged batch is written at the end of this wake.
    gate: FlushGate,
    writer: BatchWriter,
    slots: EgressSlots,
    streams: SelectAll<SlotStream>,
    /// Set once the task has decided to exit, so the drain loop stops pulling
    /// work it will never flush.
    stopping: bool,
    /// Whether an open acks before its `OpenSlot` is admitted. See
    /// [`MuxConfig::async_open_ack`].
    async_open_ack: bool,
    /// Credit replies encoded into the batch the writer currently has open.
    ///
    /// `FlowControl::take_pending_grant` zeroes the ingress account's
    /// `ungranted` when the reply is minted, so between that point and the
    /// write the open batch holds the only copy of that credit. This is that
    /// copy, kept so a batch thrown away rather than written can hand it back
    /// — see [`Batcher::repost_staged_credit`].
    staged_credit: Vec<(SlotId, u32)>,
    #[cfg(test)]
    hooks: Option<Arc<TestHooks>>,
}

impl Batcher {
    async fn run(mut self, opens: flume::Receiver<OpenSlotRequest>) {
        let cancel = self.cancel.clone();
        let control = Arc::clone(&self.control);
        loop {
            let deadline = self.gate.deadline();
            let work = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                open = opens.recv_async() => match open {
                    Ok(open) => Work::Open(open),
                    Err(_) => break,
                },
                () = control.wait() => match control.take() {
                    Some(drained) => Work::Control(drained),
                    // Drained by the pass below between the wake and the take.
                    None => continue,
                },
                Some((index, item)) = self.streams.next() => Work::Slot(index, item),
                () = linger_until(deadline) => Work::Linger,
            };
            self.handle.mark_active();
            if let Some(metrics) = &self.metrics {
                metrics.batcher_wake(match &work {
                    Work::Slot(_, SlotItem::Frame(_)) => BatcherWake::Frame,
                    Work::Slot(_, SlotItem::InletClosed) => BatcherWake::InletClosed,
                    Work::Open(_) => BatcherWake::Open,
                    Work::Control(_) => BatcherWake::Control,
                    Work::Linger => BatcherWake::Linger,
                });
            }
            self.dispatch(work).await;

            // The one point a test can stop the loop at, so a record can be
            // queued mid-wake. See [`test_hooks`].
            #[cfg(test)]
            if let Some(hooks) = self.hooks.clone() {
                hooks.barrier().await;
            }

            // Take everything already queued before deciding to write. Under
            // every policy: this is what turns a forward pass's X back-to-back
            // sends into one batch, and it never waits for work that has not
            // arrived.
            //
            // It is also what makes an application's flush *exact*, and the
            // argument is worth writing down because it is not obvious. A kick
            // is coalesced control, so the only way `gate.kicked` becomes true
            // is `on_control`, reached through `dispatch` — from the select arm
            // above, or from `drain_once` below. Either way the loop keeps
            // draining afterwards, and `drain_once` polls the slot streams
            // after the control state. So every record queued before the kick
            // was queued before the drain that follows the kick's observation,
            // and is therefore in the batch the kick writes. The loop is what
            // carries that; a single pass would not.
            while !self.stopping && self.drain_once(&opens).await {}

            if self.stopping {
                // The last read, and the one that closes the inbox. A reply
                // that landed since the drain that carried `retire` comes
                // back here and rides the final flush below; one that lands
                // after is refused, and its writer re-resolves onto the
                // batcher that replaces this one. Without this the drain that
                // carried `retire` was the last read and nothing told a
                // writer so: the mux's `close_claimed_slot` posts its
                // `CloseSlot` right after retiring the ingress slot that made
                // this batcher evictable, and one landing in between was
                // applied by nobody. `retire` cannot be in what comes back —
                // the sweep posts it only through a registry entry, and this
                // batcher's is gone.
                if let Some(leftover) = self.control.close() {
                    self.on_control(leftover).await;
                }
            }

            let kicked = self.gate.take_kick();

            if kicked || self.stopping || self.gate.should_flush() {
                self.flush().await;
            }
            if self.stopping {
                // The sweep already removed the registry entry.
                return self.teardown(false);
            }
        }
        self.teardown(true);
    }

    /// Pull one already-available item, returning whether there was one.
    async fn drain_once(&mut self, opens: &flume::Receiver<OpenSlotRequest>) -> bool {
        if let Ok(open) = opens.try_recv() {
            self.dispatch(Work::Open(open)).await;
            return true;
        }
        if let Some(drained) = self.control.take() {
            self.dispatch(Work::Control(drained)).await;
            return true;
        }
        match self.streams.next().now_or_never() {
            Some(Some((index, item))) => {
                self.dispatch(Work::Slot(index, item)).await;
                true
            }
            _ => false,
        }
    }

    async fn dispatch(&mut self, work: Work) {
        match work {
            Work::Slot(index, SlotItem::Frame(bytes)) => self.on_frame(index, bytes).await,
            Work::Slot(index, SlotItem::InletClosed) => self.on_inlet_closed(index).await,
            Work::Open(request) => self.on_open_slot(request).await,
            Work::Control(drained) => self.on_control(drained).await,
            Work::Linger => {}
        }
    }

    /// Apply one drain's worth of coalesced control.
    ///
    /// Order within a drain is by kind rather than by arrival, because arrival
    /// order is what coalescing gave up and none of these depend on it: replies
    /// name the peer's slots, grants and closes name ours, and a close makes its
    /// slot's grant moot either way.
    async fn on_control(&mut self, drained: DrainedControl) {
        if drained.flush {
            self.gate.kick();
        }
        for (raw, entry) in drained.peers {
            self.on_reply(SlotId::from_raw(raw), entry).await;
        }
        for (raw, entry) in drained.mine {
            self.on_owned_control(SlotId::from_raw(raw), entry).await;
        }
        if drained.retire {
            self.on_retire();
        }
    }

    /// Apply the coalesced control for one slot this side owns.
    ///
    /// The generation check comes first, and that ordering is the whole point:
    /// a singleton's resolution carries the `SlotId` it was sent under, and a
    /// close-then-reopen recycles that dense index under a new generation while
    /// the resolution is still in flight. Acting on a stale failure would fail
    /// the epoch — every live slot on the peer — over a stream that ended
    /// cleanly before the answer arrived.
    async fn on_owned_control(&mut self, slot: SlotId, entry: OwnedControl) {
        if self.slots.get_mut_checked(slot).is_none() {
            // The slot is gone, so there is no `frame_seq` gap left to protect:
            // its records are nobody's problem and its consumer has already
            // been told. If the admission failed for a connection-level reason
            // rather than a slot-level one, the very next batch to this peer
            // meets the same failure and fails the epoch then — deferring to
            // that signal costs a batch and loses nothing.
            if entry.singleton == Some(false)
                && let Some(metrics) = &self.metrics
            {
                metrics.record_dropped(MuxDropReason::StaleSingleton);
            }
            return;
        }

        // A failed singleton is epoch death; nothing else about the slot
        // matters afterwards, because the slot does not survive the epoch.
        if entry.singleton == Some(false) {
            self.epoch_death();
            return;
        }
        if let Some(reason) = entry.close {
            self.on_peer_closed(slot, reason);
            return;
        }
        let mut touched = false;
        if let Some(live) = self.slots.get_mut_checked(slot) {
            if entry.credit > 0 {
                live.credit.grant(entry.credit);
                touched = true;
            }
            if entry.singleton == Some(true) {
                live.unfence();
                touched = true;
            }
        }
        if touched {
            self.release_withheld(slot.index()).await;
        }
    }

    // -----------------------------------------------------------------------
    // Control path
    // -----------------------------------------------------------------------

    async fn on_open_slot(&mut self, request: OpenSlotRequest) {
        let OpenSlotRequest {
            anchor_id,
            session_id,
            inlet,
            credit,
            slot_byte_budget,
            ack,
        } = request;
        if self.handle.is_retired() {
            let _ = ack.send(Err(OpenRejected::Retired));
            return;
        }
        let (id, stream) = match self.slots.allocate(inlet, credit, slot_byte_budget) {
            Ok(allocated) => allocated,
            Err(error) => {
                let _ = ack.send(Err(error.into()));
                return;
            }
        };
        // Not a redundant copy of `self.slots`: `id.generation()` is
        // information the control inbox has no other way to see, published
        // here because `entry_mine`'s bound needs it and nothing shorter than
        // this call site can hand it over.
        self.control.note_allocated(id);
        self.streams.push(stream);
        self.publish_live_slots();
        if let Some(metrics) = &self.metrics {
            metrics.slot_opened();
        }

        // Eager, and written before the ack either way: `bind()`'s accept
        // timeout measures "time until a batch bearing this OpenSlot arrives",
        // and piggybacking it on the first data record would quietly redefine
        // that as "time until the producer produces its first token" — expiring
        // a queued request with a long prefill.
        if self.async_open_ack {
            self.open_detached(id, anchor_id, session_id, ack).await;
        } else {
            self.open_awaited(id, anchor_id, session_id, ack).await;
        }
    }

    /// The `frame_seq` the slot's `OpenSlot` is stamped with — its first.
    fn open_seq(&mut self, id: SlotId) -> u32 {
        self.slots
            .get_mut(id.index())
            .map_or(0, |entry| entry.take_seq())
    }

    /// Write the `OpenSlot` and ack once the transport has taken it.
    async fn open_awaited(
        &mut self,
        id: SlotId,
        anchor_id: u64,
        session_id: u64,
        ack: oneshot::Sender<Result<(), OpenRejected>>,
    ) {
        let seq = self.open_seq(id);
        self.ensure_batch();
        if let Some(encoder) = self.writer.encoder() {
            let _ = encoder.push_open_slot(id, seq, anchor_id, session_id);
            self.gate.stage_urgent(1);
        }
        self.flush().await;
        let _ = ack.send(Ok(()));
    }

    /// Hand the `OpenSlot` to the transport and ack without waiting for it.
    ///
    /// The `send_singleton` shape, applied to an open: the batch is dispatched,
    /// the slot is fenced behind its admission, and the answer comes back as
    /// coalesced control rather than by parking this task. What it buys is the
    /// wait it does not take — on a congested peer the awaited ack costs a place
    /// in the send queue that is already full, which is the queue a worker's
    /// first token sits behind.
    async fn open_detached(
        &mut self,
        id: SlotId,
        anchor_id: u64,
        session_id: u64,
        ack: oneshot::Sender<Result<(), OpenRejected>>,
    ) {
        // Cut whatever is staged first, so the open's own batch cannot overtake
        // records already packed for this peer: `batch_seq` is the receiver's
        // gap meter and it reads a reordered pair as a batch that went missing.
        // It costs no wait that was not already owed — that batch was going out
        // at the end of this wake anyway — and it keeps "a batch that was never
        // admitted is epoch death" a decision made in the one place that makes
        // it.
        self.flush().await;
        // That cut can be the failure that kills the epoch, and the slot
        // allocated a moment ago goes with it. Opening it on the wire now would
        // bind a receiver to a stream nothing can ever send on.
        if self.slots.get_mut_checked(id).is_none() {
            let _ = ack.send(Ok(()));
            return;
        }
        // Fenced before the ack, because the ack is what hands the producer its
        // inlet: the slot's first record must wait for the `OpenSlot` that
        // claims its buffer at the receiver. The receiver binds a slot from its
        // own `OpenSlot`'s frame_seq, so a data record that arrives first names
        // a slot it has never bound and is dropped outright — there is no
        // reordering buffer on the other side that would let it wait, the way
        // `apply_data`'s own does for records that merely arrive out of order
        // once the slot exists. Per-target FIFO cannot substitute for the fence
        // on its own: nothing about this send path guarantees the `OpenSlot`
        // itself is what the peer's transport admits first — its fire can take
        // the direct send while a later data batch for the same slot takes
        // `spawn_slow_path`, or the reverse, and only the fence orders the two
        // regardless of which one the transport admits first.
        //
        // `fire_singleton` (below) fails the epoch on a dispatch that never
        // reached the transport and acks `Ok` either way: the awaited path
        // reaches the same place through a failed flush and answers the caller
        // the same way, and the producer learns from the inlet that the epoch
        // death just closed under it.
        //
        // The fence above orders a slot against its own later records, not one
        // detached dispatch on this peer against another. A detached open and
        // *any* later batch to the same peer — another open, or an ordinary
        // flush — are each an independently scheduled task the moment that
        // peer's sends still take `spawn_slow_path` (not yet registered):
        // `can_send_directly` gates inline-vs-detached identically for both,
        // so it takes only one detached open racing one later flush, not two
        // opens, for their `spawn_slow_path` tasks to admit out of the order
        // they were issued in and their `batch_seq` to invert on the wire.
        // Nothing reads that as data loss — per-slot order is the fence's
        // job, and `note_batch_seq` only meters — but the receiver's gap
        // meter reads the later batch, arriving first, as one batch that went
        // missing, and `velo_streaming_mux_batch_seq_gaps_total` gains one
        // per inverted pair (the earlier batch, arriving second, is behind
        // the meter's mark and counts nothing). Latent, not observed — every
        // peer in the rig is registered long before its first stream opens —
        // and removing even that one belongs to a per-class `batch_seq`, a
        // follow-up scoped and tracked in
        // `agent-docs/w4a-async-open-ack-status.md` rather than a fix owed
        // here.
        let seq = self.open_seq(id);
        self.fire_singleton(id, FenceSkip::IfAdmitted, |encoder| {
            encoder.push_open_slot(id, seq, anchor_id, session_id)
        });
        let _ = ack.send(Ok(()));
    }

    /// Dispatch one record outside the packed batch, fence its slot unless
    /// `skip` says the caller's admission already made the fence pointless,
    /// and watch the result from a detached task.
    ///
    /// The shared tail of `send_singleton` and `open_detached`: both need to
    /// fence the slot, dispatch outside the packed batch, and watch the same
    /// admission, so it lives once here rather than in each caller.
    /// `metrics.rendezvous_singleton()` is counted by the caller rather than
    /// here because it counts a rendezvous transfer, not an open, and this
    /// seam is the one place both call from. Returns `false` after failing the
    /// epoch when nothing reached the transport; the caller still owes its own
    /// record whatever answer it owes on that arm (an ack, a close), which is
    /// why this does not do it itself.
    ///
    /// Only [`FenceSkip::IfAdmitted`] ever skips the fence, and only
    /// `open_detached` passes it: an `OpenSlot` [`AdmissionState::Admitted`]
    /// synchronously has already entered the transport's send channel, so
    /// every record this batcher dispatches after this call enters that same
    /// FIFO channel behind it — there is nothing left for a fence to order.
    /// `send_singleton` passes [`FenceSkip::Never`] and always fences,
    /// including on a terminal. That is inert on this call path: `close_local`
    /// removes the slot's table entry (`EgressSlots::close`'s `Option::take`)
    /// with no `.await` between the fence and the close, so nothing observes
    /// the fence before it goes with the entry — except the `cfg(test)` hook
    /// just below, which does record it. The always-fences behavior itself is
    /// unconditional because a rendezvous record's bytes are resolved by the
    /// receiver's ordered dispatcher in a detached task before dispatch
    /// (`BATCHING.md` § "Slots"), so nothing about the *sender's* admission
    /// order says anything about the order the receiver applies it in.
    ///
    /// The `tokio::spawn` below always watches the admission — even an
    /// unfenced dispatch has to learn of a *failure*, which is epoch death
    /// whether or not the fence was ever raised. But it reports *success* to
    /// `singleton_resolved` only when this call actually fenced: an unfenced
    /// admission's own resolution has no fence of its own to lift, and
    /// [`ControlState::resolutions`] is keyed by [`SlotId`] alone, with no way
    /// to tell *which* dispatch a `true` is answering. Reporting one anyway
    /// would let it coalesce with a later, genuinely outstanding fenced
    /// singleton's entry and release a fence that has not actually resolved —
    /// the exact race
    /// `an_admitted_singletons_resolution_does_not_lift_a_different_fence`
    /// pins shut. The fence itself, raised synchronously above, is what makes
    /// the slot's own records *wait* before `on_frame` will stage them; the
    /// report only decides whether anything will ever lift that wait back
    /// off. Raising the fence
    /// when nothing needs ordering buys no order and only makes the first
    /// record of every stream wait out hops that were never on the critical
    /// path — on an uncongested peer, exactly the wait
    /// `MuxConfig::async_open_ack` exists to remove.
    fn fire_singleton(
        &mut self,
        id: SlotId,
        skip: FenceSkip,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) -> bool {
        let Some(fire) = self.writer.dispatch_singleton(write) else {
            self.epoch_death();
            return false;
        };
        let needs_fence = match skip {
            FenceSkip::IfAdmitted => fire.admission_state() != AdmissionState::Admitted,
            FenceSkip::Never => true,
        };
        if needs_fence && let Some(slot) = self.slots.get_mut(id.index()) {
            slot.fence();
        }
        #[cfg(test)]
        if needs_fence && let Some(hooks) = &self.hooks {
            hooks.note_fenced();
        }
        let control = Arc::clone(&self.control);
        #[cfg(test)]
        let hooks = self.hooks.clone();
        tokio::spawn(async move {
            let admitted = fire.await.is_ok();
            #[cfg(test)]
            if let Some(hooks) = &hooks {
                hooks.await_resolutions_release().await;
            }
            // A failure is epoch death regardless of whether this dispatch
            // fenced, so it is always reported. A success is reported only
            // when it fenced: an unfenced dispatch's own resolution has no
            // fence of its own to lift, and reporting one anyway would let it
            // coalesce with a *different*, still-outstanding fenced
            // singleton's entry under the same `SlotId` key and release a
            // fence that has not actually resolved (see `fire_singleton`'s
            // doc above).
            if needs_fence || !admitted {
                control.singleton_resolved(id, admitted);
            }
        });
        true
    }

    fn on_peer_closed(&mut self, slot: SlotId, reason: CloseReason) {
        if self.slots.get_mut_checked(slot).is_some() {
            tracing::debug!(slot = ?slot, ?reason, "messenger mux: peer closed our egress slot");
            self.close_local(slot.index());
        }
    }

    /// Emit the coalesced control owed back for one of the peer's slots.
    ///
    /// These reference the *peer's* slot ids and carry `frame_seq = 0`: they do
    /// not belong to that slot's outbound counter, and their order comes from
    /// batch position.
    async fn on_reply(&mut self, slot: SlotId, entry: PeerControl) {
        if entry.credit > 0
            && self
                .push_reply(RecordType::CreditUpdate, |encoder| {
                    encoder.push_credit_update(slot, 0, entry.credit)
                })
                .await
        {
            // Recorded after the await, so a `push_reply` that had to flush
            // the previous batch first attributes this credit to the batch it
            // actually landed in rather than to the one already gone.
            self.staged_credit.push((slot, entry.credit));
        }
        if let Some(reason) = entry.close {
            self.push_reply(RecordType::CloseSlot, |encoder| {
                encoder.push_close_slot(slot, 0, reason)
            })
            .await;
        }
    }

    /// `kind` is what the reply is, and decides how long the batch may hold
    /// it: a credit reply rides the reply window, a close goes now.
    async fn push_reply(
        &mut self,
        kind: RecordType,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) -> bool {
        let needed = record_encoded_len(4).unwrap_or(usize::MAX);
        self.ensure_batch();
        if !self.fits(needed, 1) {
            self.flush().await;
            self.ensure_batch();
        }
        if let Some(encoder) = self.writer.encoder() {
            let _ = write(encoder);
            // A close is liveness and goes now. A credit reply is liveness too,
            // but held for at most the reply window rather than at once: no
            // application on this side knows it owes the peer a flush, so the
            // window is the batcher's own and never the policy's — see
            // `flush_gate`'s module docs for what the urgent flush cost.
            if kind == RecordType::CreditUpdate {
                self.gate.stage_reply(1);
            } else {
                self.gate.stage_urgent(1);
            }
            return true;
        }
        false
    }

    fn on_retire(&mut self) {
        if self.slots.live() == 0 {
            self.stopping = true;
            return;
        }
        // A `connect()` won the race with the sweep: its `OpenSlot` was queued
        // before the eviction claim and processed after it. Take the registry
        // entry back rather than serve a peer nobody can find.
        match self.batchers.entry(self.peer) {
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                self.handle.retired.store(false, Ordering::Release);
                self.handle.mark_active();
                vacant.insert(Arc::clone(&self.handle));
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                // A newer batcher already owns this peer. Fail our slots so
                // their producers see a closed channel and re-attach onto it.
                self.stopping = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Batch assembly — see [`writer`]
    // -----------------------------------------------------------------------

    fn ensure_batch(&mut self) -> usize {
        self.writer.ensure_batch()
    }

    fn fits(&self, bytes: usize, records: u16) -> bool {
        self.writer.fits(bytes, records)
    }

    /// Write the staged batch, failing the epoch if it is never admitted.
    ///
    /// The writer reports the failure and this decides what it means: a batch
    /// that never reached the wire leaves a `frame_seq` gap in every slot packed
    /// into it, and the mux does not retransmit, so those slots can never make
    /// progress again.
    async fn flush(&mut self) {
        self.gate.cleared();
        // Whatever credit this batch carries goes with the write either way.
        // Admitted, it is the peer's. Refused, it dies with the epoch the
        // refusal kills, and deliberately: a transport that refused this
        // batch will not take the one a re-post rebuilds either, so re-posting
        // here is an unbounded retry at the reply window's cadence rather than
        // a recovery. `epoch_death` therefore finds nothing to hand back on
        // this path, and everything to hand back on its other two.
        self.staged_credit.clear();
        if let Err(writer::FlushFailed(error)) = self.writer.flush().await {
            tracing::warn!(
                peer = %self.peer,
                epoch = self.writer.epoch(),
                %error,
                "messenger mux: batch was never admitted; failing the peer epoch"
            );
            self.epoch_death();
        }
    }

    // -----------------------------------------------------------------------
    // Slot lifecycle
    // -----------------------------------------------------------------------

    fn close_local(&mut self, index: u32) {
        if self.slots.close(index) {
            if let Some(metrics) = &self.metrics {
                metrics.slot_closed();
            }
            self.publish_live_slots();
        }
    }

    fn publish_live_slots(&self) {
        self.handle
            .live_slots
            .store(self.slots.live(), Ordering::Relaxed);
    }

    /// Fail every live slot and move to a fresh epoch.
    ///
    /// Slots do not survive an epoch. That is what makes "exactly one failure
    /// per live slot" provable, and it is why generations only have to be unique
    /// within an epoch. Dropping the `SelectAll` drops every slot's
    /// `flume::Receiver`, which is the death signal a producer sees.
    fn epoch_death(&mut self) {
        let closed = self.slots.close_all();
        self.streams = SelectAll::new();
        if let Some(metrics) = &self.metrics {
            metrics.epoch_death();
            for _ in 0..closed {
                metrics.slot_closed();
            }
        }
        self.publish_live_slots();
        // The staged batch goes with the epoch, so the gate must forget it too.
        // Otherwise the staged gauge — the one signal a forgotten flush shows up
        // in — drifts up by a batch per epoch death and cries wolf. The credit
        // that batch was carrying does not go with it: `repost_staged_credit`
        // below hands it back, because the ingress slots it belongs to outlive
        // the epoch. See agent-docs/w7-reply-linger-credit-loss.md and its
        // 2026-09-11 addendum.
        self.gate.discarded();
        self.repost_staged_credit();
        self.writer
            .reset_epoch(self.epochs.fetch_add(1, Ordering::Relaxed));
    }

    /// Hand back the credit a batch was carrying when it was thrown away.
    ///
    /// The slots this credit belongs to are *ingress* slots — the peer's
    /// egress into us — and `close_all` above closes this side's egress slots,
    /// so they outlive the epoch and their sender is still waiting on a window
    /// nothing else re-derives. Posting it back to the control state puts it
    /// where the drained reply came from, so the next batch re-advertises it.
    ///
    /// The inbox refuses this once the task has taken its last drain, which
    /// `on_retire`'s `Occupied` arm makes reachable: it stops a batcher that
    /// still holds live slots, so the drain that `close` takes can still reach
    /// `epoch_death`. The credit then goes to whichever batcher took the peer
    /// over, the same answer `MuxCore::send_replies` gives a refused writer.
    /// One attempt, not `send_replies`' loop: that loop terminates because it
    /// can spawn a batcher, and this side can only read the registry.
    ///
    /// Safe to take the inbox lock here: all three callers reach this with the
    /// drained control already released, never mid-`mutate`.
    fn repost_staged_credit(&mut self) {
        if self.staged_credit.is_empty() {
            return;
        }
        let staged = std::mem::take(&mut self.staged_credit);
        let replies: Vec<ReplyRecord> = staged
            .iter()
            .map(|&(slot, delta)| ReplyRecord::CreditUpdate { slot, delta })
            .collect();
        let recovered = self.control.reply(&replies)
            || self
                .replacement()
                .is_some_and(|handle| handle.reply(&replies));
        if let Some(metrics) = &self.metrics {
            for (_, delta) in staged {
                if recovered {
                    metrics.credit_reposted(delta);
                } else {
                    metrics.credit_lost(delta);
                }
            }
        }
    }

    /// The batcher that has taken this peer over, when it is not this one.
    ///
    /// Cloned out rather than used through the guard: holding a `DashMap` shard
    /// lock across `reply`'s own mutex buys nothing and orders two locks.
    fn replacement(&self) -> Option<Arc<BatcherHandle>> {
        let entry = self.batchers.get(&self.peer)?;
        (!Arc::ptr_eq(entry.value(), &self.handle)).then(|| Arc::clone(entry.value()))
    }

    /// Close every slot on the way out, so producers learn immediately.
    fn teardown(&mut self, unregister: bool) {
        // Unregistered before the inbox closes. `send_replies` re-resolves a
        // refused reply through the registry until a batcher takes it, and
        // that terminates only if a closed batcher is never the registered
        // one. The retire path holds it because the sweep removes the entry
        // before posting `retire`; this order is what holds it on the other
        // exit. Nothing writes after cancel today — it comes only from
        // `MuxCore::drop` — which is why the invariant is kept structural
        // rather than argued from the callers.
        if unregister {
            let handle = Arc::clone(&self.handle);
            self.batchers
                .remove_if(&self.peer, |_, entry| Arc::ptr_eq(entry, &handle));
        }
        // Already closed on the retirement path, where what it handed back
        // rode the final flush. On cancellation whatever is still pending
        // dies with the transport, and a writer that comes later is refused
        // and re-resolves — onto a batcher on the same cancelled token, which
        // exits the same way.
        self.control.close();
        // Anything still staged dies with the task: the slots it belongs to are
        // being closed in the next line, so their consumers learn through
        // `Dropped` rather than through a batch nobody is left to admit.
        //
        // No `repost_staged_credit` here, and it is not an omission. The
        // retirement path forces a write before it reaches this line, so it
        // arrives with nothing staged. The two exits that skip that write both
        // come from the whole mux going away, where there is no later batch to
        // re-advertise into — and the inbox is closed one line above, so a
        // re-post would be refused anyway.
        self.gate.discarded();
        let closed = self.slots.close_all();
        self.streams = SelectAll::new();
        if let Some(metrics) = &self.metrics {
            for _ in 0..closed {
                metrics.slot_closed();
            }
        }
        self.publish_live_slots();
    }
}
