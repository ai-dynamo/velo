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

use crate::observability::BatcherWake;
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
use crate::observability::{MuxDropReason, MuxMetricsHandle};
use crate::streaming::messenger_mux::flow_control::{CreditClass, SlotCredit};
use crate::streaming::sender::is_terminal_sentinel;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplyRecord {
    /// Additional data credit for the peer's slot.
    CreditUpdate { slot: SlotId, delta: u32 },
    /// Tell the peer to abandon its slot.
    CloseSlot { slot: SlotId, reason: CloseReason },
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
    alive: AtomicBool,
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

    /// Whether the task is still running.
    ///
    /// Control is state rather than a queue, so a writer cannot learn from the
    /// send that nobody will read it. Callers that care — the reply path — check
    /// this and re-resolve. Nothing is lost when it races: a batcher only exits
    /// on cancellation, which is the transport going away, or on retirement,
    /// which requires zero live slots on both sides and therefore no credit to
    /// return.
    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
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
    pub(crate) fn reply(&self, records: &[ReplyRecord]) {
        for record in records {
            match *record {
                ReplyRecord::CreditUpdate { slot, delta } => self.control.reply_credit(slot, delta),
                ReplyRecord::CloseSlot { slot, reason } => self.control.reply_close(slot, reason),
            }
        }
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
        alive: AtomicBool::new(true),
    });
    let epoch = ctx.epochs.fetch_add(1, Ordering::Relaxed);
    let gate = FlushGate::new(
        ctx.config.flush_policy,
        ctx.config.reply_linger,
        ctx.metrics.clone(),
    );
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
        if let Some(metrics) = &self.metrics {
            metrics.batcher_wake(match &work {
                Work::Slot(_, SlotItem::Frame(_)) => BatcherWake::Frame,
                Work::Slot(_, SlotItem::InletClosed) => BatcherWake::InletClosed,
                Work::Open(_) => BatcherWake::Open,
                Work::Control(_) => BatcherWake::Control,
                Work::Linger => BatcherWake::Linger,
            });
        }
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
        self.streams.push(stream);
        self.publish_live_slots();
        if let Some(metrics) = &self.metrics {
            metrics.slot_opened();
        }

        let seq = self
            .slots
            .get_mut(id.index())
            .map_or(0, |entry| entry.take_seq());
        self.ensure_batch();
        if let Some(encoder) = self.writer.encoder() {
            let _ = encoder.push_open_slot(id, seq, anchor_id, session_id);
            if let Some(metrics) = &self.metrics {
                metrics.record_sent(RecordType::OpenSlot);
            }
            self.gate.stage_urgent(1);
        }
        // Eager, in its own flush: `bind()`'s accept timeout measures "time
        // until a batch bearing this OpenSlot arrives", and piggybacking it on
        // the first data record would quietly redefine that as "time until the
        // producer produces its first token" — expiring a queued request with a
        // long prefill.
        self.flush().await;
        let _ = ack.send(Ok(()));
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
        if entry.credit > 0 {
            self.push_reply(RecordType::CreditUpdate, |encoder| {
                encoder.push_credit_update(slot, 0, entry.credit)
            })
            .await;
        }
        if let Some(reason) = entry.close {
            self.push_reply(RecordType::CloseSlot, |encoder| {
                encoder.push_close_slot(slot, 0, reason)
            })
            .await;
        }
    }

    async fn push_reply(
        &mut self,
        kind: RecordType,
        write: impl FnOnce(&mut BatchEncoder) -> Result<(), EncodeError>,
    ) {
        let needed = record_encoded_len(4).unwrap_or(usize::MAX);
        self.ensure_batch();
        if !self.fits(needed, 1) {
            self.flush().await;
            self.ensure_batch();
        }
        if let Some(encoder) = self.writer.encoder() {
            let _ = write(encoder);
            if let Some(metrics) = &self.metrics {
                metrics.record_sent(kind);
            }
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
        }
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
        // in — drifts up by a batch per epoch death and cries wolf.
        self.gate.discarded();
        self.writer
            .reset_epoch(self.epochs.fetch_add(1, Ordering::Relaxed));
    }

    /// Close every slot on the way out, so producers learn immediately.
    fn teardown(&mut self, unregister: bool) {
        self.handle.alive.store(false, Ordering::Release);
        // Anything still staged dies with the task: the slots it belongs to are
        // being closed in the next line, so their consumers learn through
        // `Dropped` rather than through a batch nobody is left to admit.
        self.gate.discarded();
        let closed = self.slots.close_all();
        self.streams = SelectAll::new();
        if let Some(metrics) = &self.metrics {
            for _ in 0..closed {
                metrics.slot_closed();
            }
        }
        self.publish_live_slots();
        if unregister {
            let handle = Arc::clone(&self.handle);
            self.batchers
                .remove_if(&self.peer, |_, entry| Arc::ptr_eq(entry, &handle));
        }
    }
}
