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
//! That gap is exactly what multiplexing needs. A slot that has run out of
//! credit, or that is fencing a rendezvous singleton, must stop being drained so
//! its producer feels backpressure on its own channel rather than piling into a
//! mux-side buffer; meanwhile every other slot on the peer keeps flowing. So the
//! stream is written here rather than composed: [`SlotStream`] consults a
//! [`SlotGate`] before touching its receiver, and the gate is also how a closed
//! slot terminates — the stream ends, `SelectAll` drops it, the `flume::Receiver`
//! goes with it, and the producer's `Sender` starts erroring. That last step is
//! the whole consumer-visible death contract, reached by dropping a receiver
//! exactly as the TCP egress pump does.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};

use futures::Stream;
use futures::task::AtomicWaker;

use super::super::protocol::{MAX_SLOT_INDEX, SlotId};
use crate::streaming::messenger_mux::flow_control::SlotCredit;

/// Set while the batcher does not want records from this slot.
const PAUSED: u8 = 0b01;
/// Set once, permanently: the slot is gone.
const CLOSED: u8 = 0b10;

/// Pause / close signalling for one slot's inlet, shared between the batcher
/// task and the stream it polls.
///
/// Two flags rather than two channels because both answers are needed on the
/// poll path and neither can afford an allocation there.
pub(super) struct SlotGate {
    state: AtomicU8,
    waker: AtomicWaker,
}

impl SlotGate {
    /// An open, unpaused gate.
    fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
            waker: AtomicWaker::new(),
        }
    }

    /// Stop draining this slot. Records already pulled are the caller's problem.
    fn pause(&self) {
        self.state.fetch_or(PAUSED, Ordering::Release);
    }

    /// Resume draining and wake the stream.
    fn resume(&self) {
        self.state.fetch_and(!PAUSED, Ordering::Release);
        self.waker.wake();
    }

    /// End the slot. The stream terminates on its next poll and takes the
    /// `flume::Receiver` with it.
    fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::Release);
        self.waker.wake();
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
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

        // Registered on *every* poll, not only while paused. When the stream is
        // parked on the inner receiver it is the receiver's waker that is armed,
        // so a `close()` racing that park would otherwise be lost and the slot
        // would linger until its producer happened to send — which, for a
        // producer being torn down by epoch death, is never.
        this.gate.waker.register(cx.waker());

        let state = this.gate.state();
        if state & CLOSED != 0 {
            // The batcher closed this slot itself, so it needs no telling.
            return Poll::Ready(None);
        }
        if state & PAUSED != 0 {
            return Poll::Pending;
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

/// Why a slot is not being drained right now.
///
/// Both reasons share one gate so there is exactly one place that can leave a
/// slot wedged, and one place that decides it may run again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PauseReasons {
    /// The slot has no credit for the record it is holding.
    starved: bool,
    /// A rendezvous singleton for this slot is outstanding. `BATCHING.md`
    /// § "Slots": at most one per slot, and the slot's later records wait for
    /// its admission so `frame_seq` order survives the unordered resolve.
    fenced: bool,
}

impl PauseReasons {
    const fn any(self) -> bool {
        self.starved || self.fenced
    }
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
    /// The single record pulled before the slot lost the right to send it.
    ///
    /// Bounded at one by construction: the batcher pauses the gate in the same
    /// step that it withholds, and processes stream items one at a time, so the
    /// stream cannot yield a second record before the pause takes effect.
    pub(super) withheld: Option<Vec<u8>>,
    gate: Arc<SlotGate>,
    pause: PauseReasons,
}

impl EgressSlot {
    /// Whether a record of this slot may be pulled and packed right now.
    pub(super) const fn is_paused(&self) -> bool {
        self.pause.any()
    }

    /// Whether a rendezvous singleton is outstanding for this slot.
    pub(super) const fn is_fenced(&self) -> bool {
        self.pause.fenced
    }

    /// Park the slot on credit.
    pub(super) fn park_starved(&mut self) {
        self.pause.starved = true;
        self.sync_gate();
    }

    /// Release the credit park.
    pub(super) fn unpark_starved(&mut self) {
        self.pause.starved = false;
        self.sync_gate();
    }

    /// Fence the slot behind an outstanding rendezvous singleton.
    pub(super) fn fence(&mut self) {
        self.pause.fenced = true;
        self.sync_gate();
    }

    /// Release the rendezvous fence.
    pub(super) fn unfence(&mut self) {
        self.pause.fenced = false;
        self.sync_gate();
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

    fn sync_gate(&self) {
        if self.pause.any() {
            self.gate.pause();
        } else {
            self.gate.resume();
        }
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
    /// Credit opens at **zero**. Until attach-time negotiation lands (Stage F)
    /// there is nowhere for the receiver to advertise its window ahead of time,
    /// so the sender waits for the `CreditUpdate` the receiver emits on
    /// `OpenSlot` rather than guessing a window the receiver never sized. A
    /// guess is the one thing `NegotiationError::LegacyPeer` exists to forbid.
    pub(super) fn allocate(
        &mut self,
        rx: flume::Receiver<Vec<u8>>,
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
            credit: SlotCredit::new(0),
            next_seq: 0,
            withheld: None,
            gate,
            pause: PauseReasons::default(),
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
