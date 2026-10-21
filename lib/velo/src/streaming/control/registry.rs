// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The sender side of the control plane: the registry of open senders, and
//! the routes by which a consumer stops or cancels one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

use super::{StreamCancelHandle, StreamCancelRequest};
use crate::streaming::sender::Retirement;

/// Whether a sender is cancelled, readable on every send without a lock.
///
/// `CancellationToken::is_cancelled` locks a mutex (tokio-util 0.7.18 and
/// 0.7.19): 8.7 ns per call against 0.3 ns for this load, paid on every
/// record. [`SenderEntry::cancel`] sets the flag before it cancels the token,
/// so a send after it fails at once. Nothing else cancels the stream token:
/// the token a sender hands out is a child that only observes it.
///
/// It is also where the stream's retirement is recorded: the sender and its
/// transport slot already share this allocation, so
/// [`StreamSender::retired`](crate::streaming::StreamSender::retired) costs a
/// stream nothing until the stream ends.
#[derive(Clone, Debug, Default)]
pub struct SenderClosed(Arc<SenderState>);

/// The state behind [`SenderClosed`]: one allocation per stream, made when the
/// stream opens.
#[derive(Debug, Default)]
struct SenderState {
    closed: AtomicBool,
    /// The retirement outcome in the low bits ([`SENT`] or [`DISCARDED`], or
    /// none yet), and [`WATCHED`] once a [`Retired`] future has registered a
    /// waker. The first outcome stands.
    ///
    /// [`Retired`]: crate::streaming::Retired
    retirement: AtomicU8,
    /// One waker per [`Retired`] future of this stream that waits. A sender
    /// can hand out any number of them, so one stored waker is not enough: the
    /// last registration would replace the others, and a waiter that gave up
    /// would strand the rest. Each future keeps its own entry, replaces its
    /// waker when polled from another task, and removes the entry when it is
    /// dropped, so an abandoned wait leaves nothing behind. Touched only by a
    /// poll, a drop, and the retirement of a watched stream, never on the item
    /// path.
    ///
    /// [`Retired`]: crate::streaming::Retired
    wakers: Mutex<Wakers>,
}

#[derive(Debug, Default)]
struct Wakers {
    next: u64,
    waiting: Vec<(u64, Waker)>,
}

const SENT: u8 = 1;
const DISCARDED: u8 = 2;
const OUTCOME: u8 = SENT | DISCARDED;
/// Set by a waiting [`Retired`](crate::streaming::Retired). Without it,
/// retirement skips the waker, so a stream nobody watches pays one
/// uncontended compare-exchange when it ends.
const WATCHED: u8 = 4;

fn outcome(state: u8) -> Option<Retirement> {
    match state & OUTCOME {
        SENT => Some(Retirement::Sent),
        DISCARDED => Some(Retirement::Discarded),
        _ => None,
    }
}

impl SenderClosed {
    /// Mark the sender cancelled.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
    }

    /// Whether the sender is cancelled.
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    /// Record how the stream's output left this node, and wake a waiting
    /// [`Retired`](crate::streaming::Retired). The first outcome stands, so a
    /// later report of the same stream changes nothing.
    pub(crate) fn retire(&self, outcome: Retirement) {
        let code = match outcome {
            Retirement::Sent => SENT,
            Retirement::Discarded => DISCARDED,
        };
        let state = &self.0.retirement;
        // Expect no watcher first: that is the common case, and it costs one
        // compare-exchange. A failure that shows only `WATCHED` retries once
        // with it.
        let mut current = 0;
        loop {
            match state.compare_exchange(
                current,
                current | code,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if current & WATCHED != 0 {
                        // Taken under the lock a poll registers under, so a
                        // waker registered before the mark is in the list by
                        // the time the mark is seen. Woken outside it.
                        let wakers = std::mem::take(&mut self.0.wakers.lock().waiting);
                        for (_, waker) in wakers {
                            waker.wake();
                        }
                    }
                    return;
                }
                Err(actual) if actual & OUTCOME == 0 => current = actual,
                Err(_) => return,
            }
        }
    }

    /// Wakers registered and not yet woken.
    #[cfg(test)]
    pub(crate) fn waiting(&self) -> usize {
        self.0.wakers.lock().waiting.len()
    }

    /// Remove the waker a dropped [`Retired`](crate::streaming::Retired)
    /// registered, if retirement has not taken it already.
    pub(crate) fn unregister_retired(&self, id: u64) {
        let mut wakers = self.0.wakers.lock();
        if let Some(index) = wakers.waiting.iter().position(|(entry, _)| *entry == id) {
            wakers.waiting.swap_remove(index);
        }
    }

    /// Poll the stream's retirement for [`Retired`](crate::streaming::Retired).
    ///
    /// `registration` is the future's own entry in the waker list: `None`
    /// until its first pending poll.
    pub(crate) fn poll_retired(
        &self,
        cx: &mut Context<'_>,
        registration: &mut Option<u64>,
    ) -> Poll<Retirement> {
        let state = &self.0.retirement;
        if let Some(outcome) = outcome(state.load(Ordering::Acquire)) {
            return Poll::Ready(outcome);
        }
        // Register before marking, under the lock: a `retire` that sees the
        // mark takes the lock after this and finds the waker, and one that
        // came first shows in what `fetch_or` returns.
        let mut wakers = self.0.wakers.lock();
        let entry =
            registration.and_then(|id| wakers.waiting.iter_mut().find(|(entry, _)| *entry == id));
        match entry {
            Some((_, waker)) => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            }
            None => {
                let id = *registration.get_or_insert_with(|| {
                    wakers.next += 1;
                    wakers.next
                });
                wakers.waiting.push((id, cx.waker().clone()));
            }
        }
        drop(wakers);
        match outcome(state.fetch_or(WATCHED, Ordering::AcqRel)) {
            Some(outcome) => Poll::Ready(outcome),
            None => Poll::Pending,
        }
    }
}

/// Reports a stream [`Discarded`](Retirement::Discarded) when dropped, unless
/// [`sent`](Self::sent) consumed it first.
///
/// Whatever holds a stream's terminal on its way out holds one of these: the
/// task that waits for channel space, the batch that carries the terminal, the
/// task that watches a rendezvous admission. When that holder dies with the
/// terminal (an aborted task, a batch thrown away), the stream still retires,
/// so a [`Retired`](crate::streaming::Retired) never waits forever.
pub(crate) struct RetireGuard(Option<SenderClosed>);

impl RetireGuard {
    pub(crate) fn new(closed: SenderClosed) -> Self {
        Self(Some(closed))
    }

    /// The terminal left: report the stream sent.
    pub(crate) fn sent(mut self) {
        if let Some(closed) = self.0.take() {
            closed.retire(Retirement::Sent);
        }
    }
}

impl Drop for RetireGuard {
    fn drop(&mut self) {
        if let Some(closed) = self.0.take() {
            closed.retire(Retirement::Discarded);
        }
    }
}

/// A single slot in the sender-side registry, representing an active [`crate::streaming::sender::StreamSender`].
///
/// Stored per active stream. The `_stream_cancel` handler removes the entry
/// and calls [`cancel`](Self::cancel). The token also wakes blocked sends.
/// A mux slot holds a clone, so it cancels its sender the same way.
#[derive(Clone)]
pub struct SenderEntry {
    /// Fires when `_stream_cancel` is received — user-facing via `cancellation_token()`.
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Graceful stop leaves the response channel open.
    pub stop_token: tokio_util::sync::CancellationToken,
    /// What the sender checks per record. Set by [`cancel`](Self::cancel).
    pub closed: SenderClosed,
}

impl SenderEntry {
    /// Cancel the sender: later sends fail at once, and blocked sends wake.
    pub fn cancel(&self) {
        self.closed.close();
        self.cancel_token.cancel();
    }
}

/// Sender-side registry of active [`SenderEntry`] slots.
///
/// Keyed by the sender's local stream ID (`u64`). Mirrored in structure to the
/// anchor registry (`DashMap<u64, AnchorEntry>`) on the receiver side.
///
/// `pub` so that [`create_stream_cancel_handler`] can accept `Arc<SenderRegistry>`
/// at its public function signature. Callers outside this crate hold it via `Arc`.
#[derive(Default)]
pub struct SenderRegistry {
    pub senders: dashmap::DashMap<u64, SenderEntry>,
}

impl SenderRegistry {
    /// Remove a sender and signal cancellation. Graceful cleanup only
    /// removes the entry, because it must leave queued output usable.
    pub(crate) fn cancel(&self, sender_stream_id: u64) {
        if let Some((_, entry)) = self.senders.remove(&sender_stream_id) {
            entry.cancel();
        }
    }
}

/// Route cancellation on the messenger runtime, including calls from plain threads.
pub(crate) fn request_sender_cancel(
    handle: StreamCancelHandle,
    local_worker: velo_ext::WorkerId,
    registry: &SenderRegistry,
    messenger: Option<&Arc<crate::messenger::Messenger>>,
) {
    let (worker, sender_stream_id) = handle.unpack();
    if worker == local_worker {
        registry.cancel(sender_stream_id);
        return;
    }
    if let Some(messenger) = messenger {
        let rt = messenger.runtime().clone();
        let payload =
            serde_json::to_vec(&StreamCancelRequest { sender_stream_id }).expect("stream identity");
        // Built here, so the task holds the client and not the Messenger. A
        // send to a peer that stopped reading can wait on admission forever;
        // holding the Messenger there would keep its final drop, and so its
        // transport teardown, from ever happening.
        let send = messenger
            .am_send_streaming("_stream_cancel")
            .expect("stream cancel handler")
            .raw_payload(bytes::Bytes::from(payload))
            .worker(worker);
        rt.spawn(async move {
            let _ = send.send().await;
        });
    }
}

// ---------------------------------------------------------------------------
// create_stream_cancel_handler
// ---------------------------------------------------------------------------

/// Build the `_stream_cancel` handler.
///
/// When the consumer-side anchor receives a cancel request, it sends a
/// `_stream_cancel` active message to the sender's worker. This handler:
/// 1. Looks up the [`SenderEntry`] by `sender_stream_id`.
/// 2. Removes the entry and cancels its token.
///
/// Idempotent: if the entry is absent the handler returns `Ok(())` silently.
pub fn create_stream_cancel_handler(
    sender_registry: Arc<SenderRegistry>,
) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler(
        "_stream_cancel",
        move |ctx: crate::messenger::Context| {
            let req = serde_json::from_slice::<StreamCancelRequest>(&ctx.payload)?;
            sender_registry.cancel(req.sender_stream_id);
            Ok(())
        },
    )
    .build()
}

/// Send a graceful stop through the identity established by attach.
///
/// A sender on `local_worker` is stopped through the local registry, as
/// `StreamController::cancel` does: a same-worker attach registers it there,
/// and an active message to the local worker is not guaranteed to resolve.
pub(crate) fn request_sender_stop(
    handle: StreamCancelHandle,
    local_worker: velo_ext::WorkerId,
    registry: &SenderRegistry,
    messenger: Option<&Arc<crate::messenger::Messenger>>,
) {
    let (worker, sender_stream_id) = handle.unpack();
    if worker == local_worker {
        if let Some(entry) = registry.senders.get(&sender_stream_id) {
            entry.stop_token.cancel();
        }
        return;
    }
    // Stream IDs are local to a worker. Do not cancel an unrelated local sender.
    if let Some(messenger) = messenger {
        // On the messenger's runtime, not the caller's: `request_stop` is
        // synchronous and can run on a thread with no runtime, where the stop
        // would be dropped after the anchor had already recorded it.
        let rt = messenger.runtime().clone();
        let payload =
            serde_json::to_vec(&StreamCancelRequest { sender_stream_id }).expect("stream identity");
        // Built here, so the task holds the client and not the Messenger, for
        // the reason `request_sender_cancel` gives.
        let send = messenger
            .am_send_streaming("_stream_stop")
            .expect("stream stop handler")
            .raw_payload(bytes::Bytes::from(payload))
            .worker(worker);
        rt.spawn(async move {
            let _ = send.send().await;
        });
    } else if let Some(entry) = registry.senders.get(&sender_stream_id) {
        entry.stop_token.cancel();
    }
}

pub(crate) fn create_stream_stop_handler(
    registry: Arc<SenderRegistry>,
) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler("_stream_stop", move |ctx: crate::messenger::Context| {
        let req = serde_json::from_slice::<StreamCancelRequest>(&ctx.payload)?;
        if let Some(entry) = registry.senders.get(&req.sender_stream_id) {
            entry.stop_token.cancel();
        }
        Ok(())
    })
    .build()
}
