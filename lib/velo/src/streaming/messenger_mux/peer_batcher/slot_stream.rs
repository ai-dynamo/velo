// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The egress slot table and the pausable per-slot inlet stream.
//!
//! `FrameTransport::connect` has to hand its caller a `flume::Sender<Vec<u8>>`,
//! so every slot arrives with its own channel and the batcher's problem is
//! draining X of them from one task. [`futures::stream::SelectAll`] is the right
//! shape for that — it allocates one node per slot at `connect` time, not one
//! per record, and only polls the streams that were actually woken — but it
//! offers no way to stop pulling from one member.
//!
//! The stream is written here rather than composed for two reasons. It has to
//! carry the slot index, because `SelectAll` erases provenance and the bytes do
//! not carry it. And it has to end on demand: [`SlotGate::close`] terminates the
//! stream, `SelectAll` drops it, the `flume::Receiver` goes with it, and the
//! producer's `Sender` starts erroring — the whole consumer-visible death
//! contract, reached by dropping a receiver exactly as the TCP egress pump does.
//!
//! > **The inlet is drained unconditionally.** A slot that cannot *send* — out
//! > of credit, or fencing a rendezvous singleton — still has its records
//! > pulled, into [`EgressSlot`]'s withheld queue.
//!
//! That is not an optimisation. `finalize`, `detach` and `Drop` reach the inlet
//! through a **synchronous** `flume::Sender::send`, which blocks when the
//! channel is full — and under mux a starved slot's channel would never drain,
//! so the block would be permanent, on a runtime worker thread, from inside a
//! `Drop` in async context. TCP never had this failure mode: its egress pump
//! drains at socket speed, so a full channel is transient. Credit can park a
//! slot indefinitely, so it is not. The withheld queue is where the backpressure
//! goes instead, bounded by the slot's byte cap rather than by a channel that
//! control traffic has to get through.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures::Stream;
use futures::task::AtomicWaker;

use super::super::protocol::{MAX_SLOT_INDEX, SlotId};
use crate::streaming::messenger_mux::flow_control::{CreditClass, SlotCredit};

/// Close signalling for one slot's inlet, shared between the batcher task and
/// the stream it polls.
///
/// One flag, because there is only one thing to say: draining never stops for
/// any reason short of the slot ending.
pub(super) struct SlotGate {
    closed: AtomicBool,
    waker: AtomicWaker,
}

impl SlotGate {
    fn new() -> Self {
        Self {
            closed: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    /// End the slot. The stream terminates on its next poll and takes the
    /// `flume::Receiver` with it.
    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker.wake();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// What a slot's inlet yields.
#[derive(Debug)]
pub(super) enum SlotItem {
    /// A frame the producer enqueued.
    Frame(Vec<u8>),
    /// Every producer handle for this slot has been dropped, and no terminal
    /// went out ahead of it.
    ///
    /// The socket-era equivalent is a peer closing its connection: the receive
    /// pump sees EOF, notices the last frame was not terminal, and injects
    /// `Dropped`. The mux has no EOF, so the inlet's own end has to be turned
    /// into a record — otherwise a producer that simply vanished would leave its
    /// consumer waiting out the heartbeat watchdog.
    InletClosed,
}

/// One slot's inlet, as `SelectAll` sees it.
///
/// Yields `(slot index, item)` so the batcher can recover which slot a record
/// came from — `SelectAll` erases provenance, and re-deriving it from the bytes
/// is not possible.
pub(super) struct SlotStream {
    index: u32,
    gate: Arc<SlotGate>,
    inner: flume::r#async::RecvStream<'static, Vec<u8>>,
    /// Set once [`SlotItem::InletClosed`] has been yielded, so the stream ends
    /// on the poll after it rather than repeating.
    announced_close: bool,
}

impl Stream for SlotStream {
    type Item = (u32, SlotItem);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.announced_close {
            return Poll::Ready(None);
        }

        // Registered on *every* poll. When the stream is parked on the inner
        // receiver it is the receiver's waker that is armed, so a `close()`
        // racing that park would otherwise be lost and the slot would linger
        // until its producer happened to send — which, for a producer being torn
        // down by epoch death, is never.
        this.gate.waker.register(cx.waker());

        if this.gate.is_closed() {
            // The batcher closed this slot itself, so it needs no telling.
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some((this.index, SlotItem::Frame(bytes)))),
            Poll::Ready(None) => {
                this.announced_close = true;
                Poll::Ready(Some((this.index, SlotItem::InletClosed)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The records a slot has pulled but may not send yet.
///
/// FIFO, because `frame_seq` order is the whole protocol obligation the mux took
/// on when it gave up a private TCP connection. Bounded by **bytes**, not by
/// records: this is the memory bound that stands in for the ~1 MiB the kernel
/// socket used to enforce per stream for free, and riding the Messenger deleted
/// exactly that protection.
pub(super) struct WithheldQueue {
    records: VecDeque<Vec<u8>>,
    bytes: u64,
    cap: u64,
}

impl WithheldQueue {
    fn new(cap: u32) -> Self {
        Self {
            records: VecDeque::new(),
            bytes: 0,
            cap: u64::from(cap),
        }
    }

    /// Whether anything is waiting. A non-empty queue is itself a reason to
    /// withhold, since a record overtaking one already parked would reorder the
    /// stream.
    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Records waiting.
    pub(super) fn len(&self) -> usize {
        self.records.len()
    }

    /// Park a record, or report that the slot has run past its byte cap.
    ///
    /// The bound is **cap plus one frame**, deliberately, and it is the same
    /// shape as the `C + 1` slot buffer on the receive side: the cap governs how
    /// far a producer may run ahead, and the `+ 1` is there so a single record
    /// larger than the whole cap is never what kills a stream. Such a record
    /// leaves as an oversized singleton, which is a supported path; refusing it
    /// would mean a stream dying for sending one large frame, which nothing else
    /// in the protocol does.
    pub(super) fn push(&mut self, record: Vec<u8>) -> Result<(), WithheldOverflow> {
        let len = record.len() as u64;
        if !self.records.is_empty() && self.bytes.saturating_add(len) > self.cap {
            return Err(WithheldOverflow {
                queued: self.bytes,
                cap: self.cap,
            });
        }
        self.bytes = self.bytes.saturating_add(len);
        self.records.push_back(record);
        Ok(())
    }

    /// The oldest record, without removing it.
    ///
    /// Peek-then-pop rather than pop-then-return: a record put back would land
    /// at the *back* of the queue, which is the one thing this type exists to
    /// prevent.
    pub(super) fn front(&self) -> Option<&[u8]> {
        self.records.front().map(Vec::as_slice)
    }

    /// Discard everything queued. Used when the slot is being killed.
    pub(super) fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }

    /// Take the oldest record.
    pub(super) fn pop(&mut self) -> Option<Vec<u8>> {
        let record = self.records.pop_front()?;
        self.bytes = self.bytes.saturating_sub(record.len() as u64);
        Some(record)
    }
}

/// The producer ran further ahead of a slot that cannot send than the slot's
/// byte cap allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("withheld {queued} bytes on a slot capped at {cap}")]
pub(super) struct WithheldOverflow {
    queued: u64,
    cap: u64,
}

/// One live egress slot.
pub(super) struct EgressSlot {
    /// Index and generation as they travel on the wire.
    pub(super) id: SlotId,
    /// What this side may still send. Opens empty — see
    /// [`EgressSlots::allocate`].
    pub(super) credit: SlotCredit,
    /// Next `frame_seq` to stamp. Advances on every record this side emits for
    /// the slot, control included, so a gap is detectable across control too.
    pub(super) next_seq: u32,
    /// Records pulled from the inlet that the slot may not send yet.
    pub(super) withheld: WithheldQueue,
    /// Set once the producer has gone, so the `CloseSlot{PeerGone}` that tells
    /// the consumer can wait behind whatever is still withheld.
    pub(super) inlet_closed: bool,
    gate: Arc<SlotGate>,
    /// A rendezvous singleton is outstanding. `BATCHING.md` § "Slots": at most
    /// one per slot, and the slot's later records wait for its admission so
    /// `frame_seq` order survives the unordered resolve.
    fenced: bool,
    /// Whether the slot is currently withholding for want of credit, so the
    /// starvation meter ticks once per episode rather than once per record.
    starved: bool,
}

impl EgressSlot {
    /// Whether a rendezvous singleton is outstanding for this slot.
    pub(super) const fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Whether a record offered now has to be withheld rather than sent.
    ///
    /// Order first — anything already queued goes before a newcomer — then the
    /// fence, then credit.
    pub(super) fn must_withhold(&self, class: CreditClass) -> bool {
        !self.withheld.is_empty() || self.fenced || !self.credit.can_spend(class)
    }

    /// Note that the slot is withholding for want of credit, reporting whether
    /// this is the start of an episode.
    pub(super) fn note_starved(&mut self) -> bool {
        !std::mem::replace(&mut self.starved, true)
    }

    /// Clear the starvation flag once the queue has drained.
    pub(super) fn note_flowing(&mut self) {
        self.starved = false;
    }

    /// Fence the slot behind an outstanding rendezvous singleton.
    pub(super) fn fence(&mut self) {
        self.fenced = true;
    }

    /// Release the rendezvous fence.
    pub(super) fn unfence(&mut self) {
        self.fenced = false;
    }

    /// Take the next `frame_seq` for a record this side is emitting.
    ///
    /// Saturating rather than wrapping: `u32` per slot is unreachable in
    /// practice (`BATCHING.md` says so explicitly), and wrapping to zero would
    /// silently tell the receiver that every subsequent record is a stale
    /// duplicate.
    pub(super) fn take_seq(&mut self) -> u32 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        seq
    }
}

/// The dense slot table for one peer epoch.
///
/// Dense because ingress demuxes by `Vec` index rather than by hash — at 60 KiB
/// batches that lookup runs roughly 1100 times per batch. The generation ride
/// along the index is what makes reuse safe; it is bumped on free, so a record
/// still in flight for a recycled index is rejected instead of being delivered
/// into whichever stream now occupies it.
#[derive(Default)]
pub(super) struct EgressSlots {
    entries: Vec<Option<EgressSlot>>,
    /// Current generation per index. Outlives the entry — that is the point.
    generations: Vec<u8>,
    free: Vec<u32>,
    live: usize,
}

/// Why a slot could not be allocated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AllocError {
    /// The `u24` index space for this peer epoch is full: 16 777 216 concurrent
    /// slots to one peer. Reachable only by a leak.
    #[error("slot index space exhausted for this peer epoch")]
    IndexSpaceExhausted,
}

impl EgressSlots {
    /// Slots currently allocated.
    pub(super) const fn live(&self) -> usize {
        self.live
    }

    /// Allocate a slot for `(anchor_id, session_id)` and hand back its stream.
    ///
    /// The slot opens holding whatever ledger it is handed. On the attach path
    /// that is the **negotiated** window rather than zero: the receiver
    /// advertised the same numbers it sized its own buffer from, so the first
    /// record may go out immediately — no `CreditUpdate` on `OpenSlot`, and no
    /// round trip before the first token. A window is never *guessed*;
    /// `NegotiationError::LegacyPeer` is what makes a peer that advertised none
    /// unreachable through this path at all.
    pub(super) fn allocate(
        &mut self,
        rx: flume::Receiver<Vec<u8>>,
        credit: SlotCredit,
        slot_byte_budget: u32,
    ) -> Result<(SlotId, SlotStream), AllocError> {
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                let index = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
                if index > MAX_SLOT_INDEX {
                    return Err(AllocError::IndexSpaceExhausted);
                }
                self.entries.push(None);
                self.generations.push(0);
                index
            }
        };

        let generation = self.generations[index as usize];
        let id = SlotId::new(index, generation).ok_or(AllocError::IndexSpaceExhausted)?;
        let gate = Arc::new(SlotGate::new());
        let stream = SlotStream {
            index,
            gate: Arc::clone(&gate),
            inner: rx.into_stream(),
            announced_close: false,
        };

        self.entries[index as usize] = Some(EgressSlot {
            id,
            credit,
            next_seq: 0,
            withheld: WithheldQueue::new(slot_byte_budget),
            inlet_closed: false,
            gate,
            fenced: false,
            starved: false,
        });
        self.live += 1;
        Ok((id, stream))
    }

    /// The live slot at `index`, if any.
    pub(super) fn get_mut(&mut self, index: u32) -> Option<&mut EgressSlot> {
        self.entries.get_mut(index as usize)?.as_mut()
    }

    /// The live slot named by `id`, rejecting a stale generation.
    pub(super) fn get_mut_checked(&mut self, id: SlotId) -> Option<&mut EgressSlot> {
        let slot = self.get_mut(id.index())?;
        (slot.id == id).then_some(slot)
    }

    /// Close the slot at `index`, ending its stream and bumping its generation.
    ///
    /// Returns `true` when a slot was actually there, so the caller can keep the
    /// `live_slots` gauge honest without double-counting a repeated close.
    pub(super) fn close(&mut self, index: u32) -> bool {
        let Some(slot) = self.entries.get_mut(index as usize).and_then(Option::take) else {
            return false;
        };
        slot.gate.close();
        self.generations[index as usize] = slot.id.generation().wrapping_add(1);
        self.free.push(index);
        self.live -= 1;
        true
    }

    /// Close every live slot, returning how many there were.
    ///
    /// Used by epoch death, where "exactly one failure per live slot" is the
    /// property being preserved: slots do not survive an epoch, so this runs
    /// once and the table is empty afterwards.
    pub(super) fn close_all(&mut self) -> usize {
        let closed = self.live;
        for index in 0..self.entries.len() {
            let index = index as u32;
            self.close(index);
        }
        closed
    }
}
