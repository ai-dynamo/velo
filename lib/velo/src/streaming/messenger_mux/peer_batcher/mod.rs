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
//! ## Draining X channels from one task
//!
//! [`slot_stream`] explains the `SelectAll` arrangement and why every inlet is
//! drained unconditionally, credit or no credit: `finalize`, `detach` and `Drop`
//! reach the inlet through a *synchronous* send, and a slot parked on credit
//! would otherwise block one of them on a full channel forever. The batcher's
//! half of that contract is the per-slot withheld queue — where a record waits
//! when the slot cannot send it — and the byte cap on that queue, which is what
//! bounds the memory the arrangement costs.

mod slot_stream;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::future::FutureExt;
use futures::stream::{SelectAll, StreamExt};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use velo_ext::{InstanceId, WorkerId};

pub(crate) use self::slot_stream::AllocError;
use self::slot_stream::{EgressSlots, SlotItem, SlotStream};
use super::MuxConfig;
use super::protocol::{
    BATCH_HEADER_LEN, BatchEncoder, CloseReason, MAX_RECORDS_PER_BATCH, SlotId, record_encoded_len,
};
use crate::messenger::Messenger;
use crate::observability::{MuxDirection, MuxDropReason, MuxMetricsHandle};
use crate::streaming::messenger_mux::STREAM_BATCH_HANDLER;
use crate::streaming::messenger_mux::flow_control::CreditClass;
use crate::streaming::sender::is_terminal_sentinel;
use crate::transports::tcp::framing::COALESCE_THRESHOLD;

/// Smallest batch a clamp may produce: the header plus one empty record.
///
/// A transport that reports a tiny eager budget must not clamp the cap to
/// nothing, or the batcher would route every record — including the 13-byte
/// control ones — through rendezvous and never make progress. Records that do
/// not fit above this floor still take the singleton path, which is the correct
/// answer for them.
const MIN_BATCH_CAP: usize = BATCH_HEADER_LEN + 13;

/// The per-peer batcher registry, keyed by the batching key from `BATCHING.md`
/// § "Why bucketing by destination is free".
pub(crate) type BatcherMap = DashMap<WorkerId, Arc<BatcherHandle>>;

/// Work handed to a batcher task from outside it.
///
/// Everything that is not a data record arrives this way, on an **unbounded**
/// channel: control volume is O(live slots), which credit already bounds, and a
/// bounded control lane would let data starvation block the `CloseSlot` that
/// ends a stream.
pub(crate) enum Command {
    /// Allocate a slot for a `connect()` and send its eager `OpenSlot`.
    OpenSlot {
        anchor_id: u64,
        session_id: u64,
        inlet: flume::Receiver<Vec<u8>>,
        ack: oneshot::Sender<Result<(), OpenRejected>>,
    },
    /// An inbound `CreditUpdate` for one of this peer's slots.
    Grant { slot: SlotId, delta: u32 },
    /// The receiver asked us to abandon one of our slots.
    PeerClosed { slot: SlotId, reason: CloseReason },
    /// Control records the ingress lane wants sent back to this peer.
    Reply(Vec<ReplyRecord>),
    /// A rendezvous singleton finished resolving its admission.
    SingletonResolved { slot: SlotId, admitted: bool },
    /// The sweep evicted this batcher from the registry.
    Retire,
}

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
    commands: flume::Sender<Command>,
    live_slots: AtomicUsize,
    idle_ticks: AtomicU32,
    retired: AtomicBool,
}

impl BatcherHandle {
    /// Queue `command`. Fails only once the task has exited.
    pub(crate) fn send(&self, command: Command) -> Result<(), flume::SendError<Command>> {
        self.commands.send(command)
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
}

/// Spawn a batcher for `peer` and return its registry handle.
pub(crate) fn spawn(peer: WorkerId, ctx: BatcherContext) -> Arc<BatcherHandle> {
    let (commands, inbox) = flume::unbounded();
    let handle = Arc::new(BatcherHandle {
        commands: commands.clone(),
        live_slots: AtomicUsize::new(0),
        idle_ticks: AtomicU32::new(0),
        retired: AtomicBool::new(false),
    });
    let epoch = ctx.epochs.fetch_add(1, Ordering::Relaxed);
    let batcher = Batcher {
        peer,
        peer_instance: None,
        messenger: ctx.messenger,
        config: ctx.config,
        metrics: ctx.metrics,
        handle: Arc::clone(&handle),
        epochs: ctx.epochs,
        batchers: ctx.batchers,
        cancel: ctx.cancel,
        commands,
        epoch,
        next_batch_seq: 0,
        cap: MIN_BATCH_CAP,
        slots: EgressSlots::default(),
        streams: SelectAll::new(),
        encoder: None,
        buffer: BytesMut::new(),
        stopping: false,
    };
    tokio::spawn(batcher.run(inbox));
    handle
}

/// One unit of work pulled by the main loop.
enum Work {
    Command(Command),
    Slot(u32, SlotItem),
}

struct Batcher {
    peer: WorkerId,
    peer_instance: Option<InstanceId>,
    messenger: Arc<Messenger>,
    config: MuxConfig,
    metrics: Option<MuxMetricsHandle>,
    handle: Arc<BatcherHandle>,
    epochs: Arc<AtomicU64>,
    batchers: Arc<BatcherMap>,
    cancel: CancellationToken,
    /// Our own command sender, so a detached singleton watcher has somewhere to
    /// report. Keeping it here means the inbox never disconnects, which is why
    /// the loop exits on `Retire` or cancellation rather than on channel close.
    commands: flume::Sender<Command>,
    epoch: u64,
    next_batch_seq: u32,
    cap: usize,
    slots: EgressSlots,
    streams: SelectAll<SlotStream>,
    encoder: Option<BatchEncoder>,
    buffer: BytesMut,
    /// Set once the task has decided to exit, so the drain loop stops pulling
    /// work it will never flush.
    stopping: bool,
}

impl Batcher {
    async fn run(mut self, inbox: flume::Receiver<Command>) {
        let cancel = self.cancel.clone();
        loop {
            let work = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                command = inbox.recv_async() => match command {
                    Ok(command) => Work::Command(command),
                    Err(_) => break,
                },
                Some((index, item)) = self.streams.next() => Work::Slot(index, item),
            };
            self.handle.mark_active();
            self.dispatch(work).await;

            // Opportunistic drain: take everything already queued before
            // writing. This is what turns a forward pass's X back-to-back sends
            // into one batch, and it never waits for work that has not arrived.
            while !self.stopping && self.drain_once(&inbox).await {}

            self.flush().await;
            if self.stopping {
                // The sweep already removed the registry entry.
                return self.teardown(false);
            }
        }
        self.teardown(true);
    }

    /// Pull one already-available item, returning whether there was one.
    async fn drain_once(&mut self, inbox: &flume::Receiver<Command>) -> bool {
        if let Ok(command) = inbox.try_recv() {
            self.dispatch(Work::Command(command)).await;
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
            Work::Command(Command::OpenSlot {
                anchor_id,
                session_id,
                inlet,
                ack,
            }) => self.on_open_slot(anchor_id, session_id, inlet, ack).await,
            Work::Command(Command::Grant { slot, delta }) => self.on_grant(slot, delta).await,
            Work::Command(Command::PeerClosed { slot, reason }) => {
                self.on_peer_closed(slot, reason)
            }
            Work::Command(Command::Reply(records)) => self.on_reply(records).await,
            Work::Command(Command::SingletonResolved { slot, admitted }) => {
                self.on_singleton_resolved(slot, admitted).await
            }
            Work::Command(Command::Retire) => self.on_retire(),
        }
    }

    // -----------------------------------------------------------------------
    // Data path
    // -----------------------------------------------------------------------

    async fn on_frame(&mut self, index: u32, bytes: Vec<u8>) {
        let Some(slot) = self.slots.get_mut(index) else {
            // The slot closed between the stream yielding and this dispatch —
            // a terminal in the same drain, or a peer-side close. Today's
            // egress semantics: frames queued behind a terminal are discarded.
            return;
        };

        // Terminal-ness costs an `rmp_serde` decode attempt on anything that is
        // not one of the three cached sentinels, so it is asked only on the
        // branch that needs it. Under starvation the withhold branch is the hot
        // one, and it does not care.
        if slot.must_withhold(CreditClass::Data) {
            let starved = slot.credit.data_available() == 0;
            match slot.withheld.push(bytes) {
                Ok(()) => {
                    if starved
                        && slot.note_starved()
                        && let Some(metrics) = &self.metrics
                    {
                        metrics.credit_exhausted();
                    }
                }
                Err(error) => self.overflow_kill(index, error).await,
            }
            return;
        }

        let terminal = is_terminal_sentinel(&bytes);
        self.emit_data(index, bytes, terminal).await;
    }

    /// The producer ran past the byte cap on a slot that cannot send.
    ///
    /// This is the per-slot slow-consumer kill, and it is deliberately *not* the
    /// heartbeat watchdog: the slot dies, its consumer sees `Dropped` through the
    /// `CloseSlot{PeerGone}` sent here, and the peer's other slots never notice.
    /// Anything withheld goes with it — including a queued terminal, so a
    /// consumer that would have seen `Finalized` sees `Dropped` instead. That is
    /// the cost of not blocking the producer's synchronous terminal send, and it
    /// is bounded by a megabyte of run-ahead on a stream nobody is draining.
    async fn overflow_kill(&mut self, index: u32, error: slot_stream::WithheldOverflow) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        let id = slot.id;
        let seq = slot.take_seq();
        let discarded = slot.withheld.len();
        tracing::warn!(
            slot = ?id,
            %error,
            discarded,
            "messenger mux: producer outran a starved slot's byte cap; closing the slot"
        );
        if let Some(metrics) = &self.metrics {
            metrics.records_dropped(MuxDropReason::WithheldOverflow, discarded as u64 + 1);
        }
        self.push_close(id, seq, CloseReason::PeerGone).await;
        self.close_local(index);
    }

    /// Append a `CloseSlot` for a slot this side owns, cutting the batch first
    /// if it will not fit.
    async fn push_close(&mut self, id: SlotId, seq: u32, reason: CloseReason) {
        let needed = record_encoded_len(1).unwrap_or(usize::MAX);
        self.ensure_batch();
        if !self.fits(needed, 1) {
            self.flush().await;
            self.ensure_batch();
        }
        if let Some(encoder) = self.encoder.as_mut() {
            let _ = encoder.push_close_slot(id, seq, reason);
        }
    }

    /// Every producer handle for a slot has gone without a terminal going out.
    ///
    /// The consumer has to be told, or it waits out its heartbeat watchdog for a
    /// sender that no longer exists. `CloseSlot{PeerGone}` is that telling, and
    /// it is what makes the receive side inject `Dropped` — reproducing, per
    /// slot, what the TCP receive pump does on an EOF with no terminal behind
    /// it.
    async fn on_inlet_closed(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        slot.inlet_closed = true;
        if !slot.withheld.is_empty() {
            // Records the producer enqueued before it went are still owed to the
            // consumer. The close waits behind them; `release_withheld` fires it
            // when the queue empties.
            return;
        }
        self.finish_inlet_close(index).await;
    }

    /// Emit the `CloseSlot{PeerGone}` a departed producer owes its consumer.
    async fn finish_inlet_close(&mut self, index: u32) {
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        let id = slot.id;
        let seq = slot.take_seq();
        self.push_close(id, seq, CloseReason::PeerGone).await;
        self.close_local(index);
    }

    /// Pack one data record, cutting or bypassing the batch as its size demands.
    async fn emit_data(&mut self, index: u32, bytes: Vec<u8>, terminal: bool) {
        let record_len = record_encoded_len(bytes.len()).unwrap_or(usize::MAX);
        // A terminal and its `CloseSlot` are atomic: same batch, adjacent. Room
        // is therefore checked for the pair, never for the terminal alone —
        // splitting them across batches is a silent protocol violation that
        // only shows up at a cap boundary.
        let close_len = record_encoded_len(1).unwrap_or(usize::MAX);
        let needed = if terminal {
            record_len.saturating_add(close_len)
        } else {
            record_len
        };
        let records = if terminal { 2 } else { 1 };

        let mut cap = self.ensure_batch();
        if BATCH_HEADER_LEN.saturating_add(needed) > cap {
            // Larger than any eager batch to this peer. Flush first so the
            // record keeps its place in the peer's send order, then send it
            // alone and let the messenger stage it through rendezvous.
            self.flush().await;
            self.send_singleton(index, bytes, terminal).await;
            return;
        }
        if !self.fits(needed, records) {
            self.flush().await;
            cap = self.ensure_batch();
            if BATCH_HEADER_LEN.saturating_add(needed) > cap {
                self.send_singleton(index, bytes, terminal).await;
                return;
            }
        }

        let class = if terminal {
            CreditClass::Terminal
        } else {
            CreditClass::Data
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        if slot.credit.try_spend(class).is_err() {
            return;
        }
        let id = slot.id;
        let seq = slot.take_seq();
        let close_seq = terminal.then(|| slot.take_seq());

        if let Some(encoder) = self.encoder.as_mut() {
            if let Err(error) = encoder.push_data(id, seq, &bytes) {
                tracing::error!(slot = ?id, %error, "messenger mux: dropping unencodable record");
                return;
            }
            if let Some(close_seq) = close_seq {
                let _ = encoder.push_close_slot(id, close_seq, CloseReason::TerminalSent);
            }
        }

        if terminal {
            self.close_local(index);
        }
    }

    /// Send one record alone, over the eager budget and therefore through
    /// rendezvous, and fence its slot until the admission resolves.
    async fn send_singleton(&mut self, index: u32, bytes: Vec<u8>, terminal: bool) {
        let class = if terminal {
            CreditClass::Terminal
        } else {
            CreditClass::Data
        };
        let Some(slot) = self.slots.get_mut(index) else {
            return;
        };
        if slot.credit.try_spend(class).is_err() {
            return;
        }
        let id = slot.id;
        let seq = slot.take_seq();
        let close_seq = terminal.then(|| slot.take_seq());

        let batch_seq = self.take_batch_seq();
        let mut encoder = BatchEncoder::new(self.epoch, batch_seq);
        if let Err(error) = encoder.push_data(id, seq, &bytes) {
            tracing::error!(slot = ?id, %error, "messenger mux: dropping unencodable record");
            return;
        }
        if let Some(close_seq) = close_seq {
            let _ = encoder.push_close_slot(id, close_seq, CloseReason::TerminalSent);
        }
        let records = usize::from(encoder.record_count());
        let payload = encoder.finish().freeze();

        if let Some(metrics) = &self.metrics {
            metrics.rendezvous_singleton();
            metrics.batch(MuxDirection::Sent, records);
        }

        let fire = match self.messenger.am_send_streaming(STREAM_BATCH_HANDLER) {
            Ok(builder) => builder.raw_payload(payload).worker(self.peer).send(),
            Err(error) => {
                tracing::error!(%error, "messenger mux: could not build a singleton send");
                self.epoch_death();
                return;
            }
        };

        if terminal {
            self.close_local(index);
        } else if let Some(slot) = self.slots.get_mut(index) {
            slot.fence();
        }

        // Deliberately not awaited here: fencing is per slot, so every other
        // slot on this peer keeps flowing while the staged transfer is in
        // flight. The resolution comes back as a command.
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let admitted = fire.await.is_ok();
            let _ = commands.send(Command::SingletonResolved { slot: id, admitted });
        });
    }

    // -----------------------------------------------------------------------
    // Control path
    // -----------------------------------------------------------------------

    async fn on_open_slot(
        &mut self,
        anchor_id: u64,
        session_id: u64,
        inlet: flume::Receiver<Vec<u8>>,
        ack: oneshot::Sender<Result<(), OpenRejected>>,
    ) {
        if self.handle.is_retired() {
            let _ = ack.send(Err(OpenRejected::Retired));
            return;
        }
        let (id, stream) = match self.slots.allocate(inlet, self.config.slot_byte_budget) {
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
        if let Some(encoder) = self.encoder.as_mut() {
            let _ = encoder.push_open_slot(id, seq, anchor_id, session_id);
        }
        // Eager, in its own flush: `bind()`'s accept timeout measures "time
        // until a batch bearing this OpenSlot arrives", and piggybacking it on
        // the first data record would quietly redefine that as "time until the
        // producer produces its first token" — expiring a queued request with a
        // long prefill.
        self.flush().await;
        let _ = ack.send(Ok(()));
    }

    async fn on_grant(&mut self, slot: SlotId, delta: u32) {
        let Some(entry) = self.slots.get_mut_checked(slot) else {
            return;
        };
        entry.credit.grant(delta);
        self.release_withheld(slot.index()).await;
    }

    fn on_peer_closed(&mut self, slot: SlotId, reason: CloseReason) {
        if self.slots.get_mut_checked(slot).is_some() {
            tracing::debug!(slot = ?slot, ?reason, "messenger mux: peer closed our egress slot");
            self.close_local(slot.index());
        }
    }

    async fn on_reply(&mut self, records: Vec<ReplyRecord>) {
        for record in records {
            // Control records reference the *peer's* slot ids and carry
            // `frame_seq = 0`: they do not belong to that slot's outbound
            // counter, and their order comes from batch position.
            let needed = record_encoded_len(4).unwrap_or(usize::MAX);
            self.ensure_batch();
            if !self.fits(needed, 1) {
                self.flush().await;
                self.ensure_batch();
            }
            let Some(encoder) = self.encoder.as_mut() else {
                continue;
            };
            let _ = match record {
                ReplyRecord::CreditUpdate { slot, delta } => {
                    encoder.push_credit_update(slot, 0, delta)
                }
                ReplyRecord::CloseSlot { slot, reason } => encoder.push_close_slot(slot, 0, reason),
            };
        }
    }

    async fn on_singleton_resolved(&mut self, slot: SlotId, admitted: bool) {
        if !admitted {
            self.epoch_death();
            return;
        }
        if let Some(entry) = self.slots.get_mut_checked(slot) {
            entry.unfence();
        }
        self.release_withheld(slot.index()).await;
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

    /// Drain a slot's withheld queue as far as credit and the fence allow.
    ///
    /// Loops rather than releasing one record, because a grant may cover several
    /// and the queue is the only thing holding `frame_seq` order. Re-reads the
    /// slot each turn: `emit_data` can close it (a terminal) or fence it (an
    /// oversized singleton).
    async fn release_withheld(&mut self, index: u32) {
        loop {
            let next = {
                let Some(slot) = self.slots.get_mut(index) else {
                    return;
                };
                if slot.is_fenced() {
                    return;
                }
                match slot.withheld.front() {
                    Some(front) => {
                        let terminal = is_terminal_sentinel(front);
                        let class = if terminal {
                            CreditClass::Terminal
                        } else {
                            CreditClass::Data
                        };
                        if !slot.credit.can_spend(class) {
                            return;
                        }
                        slot.withheld.pop().map(|bytes| (bytes, terminal))
                    }
                    None => {
                        slot.note_flowing();
                        None
                    }
                }
            };
            match next {
                Some((bytes, terminal)) => self.emit_data(index, bytes, terminal).await,
                // The queue is empty. A producer that left while records were
                // still owed gets its close now.
                None => {
                    if self
                        .slots
                        .get_mut(index)
                        .is_some_and(|slot| slot.inlet_closed)
                    {
                        self.finish_inlet_close(index).await;
                    }
                    return;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Batch assembly
    // -----------------------------------------------------------------------

    /// Open a batch if none is staged, and report the byte cap it must respect.
    fn ensure_batch(&mut self) -> usize {
        if self.encoder.is_none() {
            self.cap = self.compute_cap();
            let batch_seq = self.take_batch_seq();
            let buffer = std::mem::take(&mut self.buffer);
            self.encoder = Some(BatchEncoder::with_buffer(buffer, self.epoch, batch_seq));
        }
        self.cap
    }

    /// Whether the staged batch can take `bytes` more in `records` more records.
    fn fits(&self, bytes: usize, records: u16) -> bool {
        let Some(encoder) = self.encoder.as_ref() else {
            return true;
        };
        encoder.encoded_len().saturating_add(bytes) <= self.cap
            && u32::from(encoder.record_count()) + u32::from(records)
                <= u32::from(MAX_RECORDS_PER_BATCH)
    }

    /// `min(configured cap, effective eager budget, COALESCE_THRESHOLD)`.
    ///
    /// The threshold is the packing *target*: the shared coalescing writer
    /// stages a frame into one buffered `write_all` only while it fits, so a
    /// batch above it gives back what batching bought. The eager budget is the
    /// ceiling above it — exceed it and the batch quietly becomes a rendezvous
    /// transfer, paying a round trip on behalf of every slot packed into it.
    ///
    /// Asked here, in the batcher task, because `effective_eager_payload`
    /// accounts for the ambient trace context the send will inject. An
    /// unresolved peer costs the conservative clamp rather than a failed flush.
    fn compute_cap(&mut self) -> usize {
        let eager = self.peer_instance().map_or(usize::MAX, |instance| {
            self.messenger
                .effective_eager_payload(instance, STREAM_BATCH_HANDLER, None)
        });
        let clamped = self
            .config
            .max_batch_bytes
            .min(eager)
            .min(COALESCE_THRESHOLD);
        // Not `clamp`: the floor is applied *after* the three ceilings, and a
        // configured cap below the floor is a legitimate (if useless) setting
        // rather than the panic `clamp` would give it.
        clamped.max(MIN_BATCH_CAP)
    }

    fn peer_instance(&mut self) -> Option<InstanceId> {
        if self.peer_instance.is_none() {
            self.peer_instance = self
                .messenger
                .backend()
                .try_translate_worker_id(self.peer)
                .ok();
        }
        self.peer_instance
    }

    fn take_batch_seq(&mut self) -> u32 {
        let seq = self.next_batch_seq;
        self.next_batch_seq = self.next_batch_seq.wrapping_add(1);
        seq
    }

    /// Write the staged batch, parking on admission until it is the transport's
    /// problem. A failed admission is epoch death — see the module docs.
    async fn flush(&mut self) {
        let Some(encoder) = self.encoder.take() else {
            return;
        };
        if encoder.is_empty() {
            // Nothing went out, so the sequence it reserved is not a gap.
            self.next_batch_seq = self.next_batch_seq.wrapping_sub(1);
            self.buffer = encoder.finish();
            return;
        }
        let records = usize::from(encoder.record_count());
        let mut finished = encoder.finish();
        let payload = finished.split().freeze();
        self.buffer = finished;

        if let Some(metrics) = &self.metrics {
            metrics.batch(MuxDirection::Sent, records);
        }
        if let Err(error) = self.send_batch(payload).await {
            tracing::warn!(
                peer = %self.peer,
                epoch = self.epoch,
                %error,
                "messenger mux: batch was never admitted; failing the peer epoch"
            );
            self.epoch_death();
        }
    }

    async fn send_batch(&self, payload: Bytes) -> anyhow::Result<()> {
        self.messenger
            .am_send_streaming(STREAM_BATCH_HANDLER)?
            .raw_payload(payload)
            .worker(self.peer)
            .send()
            .await
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
        self.encoder = None;
        if let Some(metrics) = &self.metrics {
            metrics.epoch_death();
            for _ in 0..closed {
                metrics.slot_closed();
            }
        }
        self.publish_live_slots();
        self.epoch = self.epochs.fetch_add(1, Ordering::Relaxed);
        self.next_batch_seq = 0;
    }

    /// Close every slot on the way out, so producers learn immediately.
    fn teardown(&mut self, unregister: bool) {
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
