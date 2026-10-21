// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! MPSC anchor registry: [`MpscAnchorEntry`], [`MpscStreamAnchor`],
//! [`MpscStreamController`], and the per-anchor channel plumbing.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::streaming::sweep::{DeadlineSweep, Due, Expiring, SweepTarget, expire_due};
use dashmap::DashMap;
use futures::Stream;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::streaming::anchor::AnchorContext;
use crate::streaming::frame::{StreamError, StreamFrame};
use crate::streaming::handle::StreamAnchorHandle;

use super::frame::MpscFrame;
use super::types::SenderId;

/// One attached sender's tracking state inside an [`MpscAnchorEntry`].
pub(crate) struct MpscSenderSlot {
    /// Child of `MpscAnchorEntry::cancel_token`. `Some` for remote senders
    /// (used to stop the per-sender reader pump without cancelling the parent);
    /// `None` for local senders (no pump).
    pub pump_token: Option<CancellationToken>,
    /// The sender's `StreamCancelHandle` — used by
    /// [`MpscStreamController::cancel`] to route `_stream_cancel` upstream.
    /// `None` means same-worker cancel via [`crate::streaming::control::SenderRegistry`]
    /// is sufficient.
    pub stream_cancel_handle: Option<crate::streaming::control::StreamCancelHandle>,
}

/// One entry in the MPSC anchor registry.
///
/// Lives in `Arc<DashMap<u64, MpscAnchorEntry>>`. Every mutation of
/// `senders` runs under the DashMap shard lock and must not be held across an
/// `.await` point — follow the bind-then-lock precedent in
/// the `_anchor_attach` handler.
#[allow(dead_code)] // `max_senders` / `heartbeat_interval` accessed via direct field read in attach paths
pub(crate) struct MpscAnchorEntry {
    /// Shared `(sender_id, bytes)` delivery channel. Every attached sender
    /// (local) or per-sender reader pump (remote) holds a clone of this
    /// [`flume::Sender`] and writes tagged frames into it.
    pub frame_tx: flume::Sender<(u64, Vec<u8>)>,
    /// Anchor-lifetime parent cancel token. Cancelled only by
    /// finalize / remove / cancel paths.
    pub cancel_token: CancellationToken,
    /// All currently attached senders, keyed by sender_id.
    pub senders: HashMap<u64, MpscSenderSlot>,
    /// Monotonic sender-id allocator; starts at 1. Each attach increments it.
    pub next_sender_id: u64,
    /// Stored duration used to re-arm the unattached timeout after the last
    /// sender departs.
    pub unattached_timeout: Option<Duration>,
    /// When the anchor expires with no sender. `None` while at least one
    /// sender is attached, and when there is no timeout. Renewal does not
    /// reach an MPSC anchor, so this moves only when the last sender leaves.
    pub expires: Option<tokio::time::Instant>,
    /// When this anchor's live entry in the deadline queue comes due. The
    /// sweep drops a popped entry whose instant differs, as a stale copy.
    pub queued: Option<tokio::time::Instant>,
    /// The manager's MPSC deadline queue, held here so that each path that
    /// removes the last sender can re-arm the timeout.
    pub deadlines: Arc<MpscDeadlines>,
    /// Negotiated heartbeat cadence echoed to each attaching sender.
    pub heartbeat_interval: Duration,
    /// Optional cap on concurrent attached senders.
    pub max_senders: Option<usize>,
}

// ---------------------------------------------------------------------------
// MpscStreamController
// ---------------------------------------------------------------------------

struct MpscStreamControllerInner {
    worker_id: velo_ext::WorkerId,
    local_id: u64,
    registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    messenger: Option<Arc<crate::messenger::Messenger>>,
    cancel_wake: flume::Sender<()>,
    cancelled: AtomicBool,
}

/// Cloneable cancel handle for an [`MpscStreamAnchor`].
///
/// Unlike [`crate::StreamController`] (single-sender), this iterates every
/// attached sender and cancels each in turn. Idempotent: the first
/// caller wins via [`AtomicBool::compare_exchange`]; subsequent calls return
/// immediately.
#[derive(Clone)]
pub struct MpscStreamController {
    inner: Arc<MpscStreamControllerInner>,
}

impl MpscStreamController {
    /// Close the anchor. Removes it from the MPSC registry, cancels every
    /// attached sender (same-worker via [`crate::streaming::control::SenderRegistry`]
    /// cancellation, cross-worker via fire-and-forget `_stream_cancel` AM), and
    /// cancels the anchor's parent [`CancellationToken`] (which in turn
    /// cancels every remote pump).
    pub fn cancel(&self) {
        if self
            .inner
            .cancelled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let Some((_, entry)) = self.inner.registry.remove(&self.inner.local_id) else {
            return;
        };

        entry.cancel_token.cancel();
        let _ = self.inner.cancel_wake.try_send(());

        cancel_all_senders(
            &entry,
            self.inner.worker_id,
            &self.inner.sender_registry,
            self.inner.messenger.as_ref(),
        );
    }
}

// ---------------------------------------------------------------------------
// MpscStreamAnchor<T>
// ---------------------------------------------------------------------------

/// Consumer-side receive stream for an MPSC anchor.
///
/// Implements [`Stream`] yielding `Result<(SenderId, MpscFrame<T>), StreamError>`.
/// Unlike the SPSC [`crate::StreamAnchor`], no wire frame is terminal: the
/// stream only yields `None` when every sender has left *and* the channel has
/// drained, or when [`MpscStreamController::cancel`] is called, or when the
/// anchor is dropped.
pub struct MpscStreamAnchor<T> {
    handle: StreamAnchorHandle,
    inner_stream: flume::r#async::RecvStream<'static, (u64, Vec<u8>)>,
    cancel_stream: flume::r#async::RecvStream<'static, ()>,
    terminated: bool,
    local_id: u64,
    registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    controller: MpscStreamController,
    _phantom: PhantomData<T>,
}

impl<T> MpscStreamAnchor<T> {
    pub(crate) fn new(
        handle: StreamAnchorHandle,
        rx: flume::Receiver<(u64, Vec<u8>)>,
        local_id: u64,
        ctx: AnchorContext,
        sender_registry: Arc<crate::streaming::control::SenderRegistry>,
        messenger: Option<Arc<crate::messenger::Messenger>>,
    ) -> Self {
        let AnchorContext {
            mpsc_registry: registry,
            ..
        } = ctx;
        let (cancel_wake, cancel_rx) = flume::bounded::<()>(1);
        let inner = Arc::new(MpscStreamControllerInner {
            worker_id: handle.unpack().0,
            local_id,
            registry: registry.clone(),
            sender_registry,
            messenger,
            cancel_wake,
            cancelled: AtomicBool::new(false),
        });
        let controller = MpscStreamController { inner };
        Self {
            handle,
            inner_stream: rx.into_stream(),
            cancel_stream: cancel_rx.into_stream(),
            terminated: false,
            local_id,
            registry,
            controller,
            _phantom: PhantomData,
        }
    }

    /// Return the anchor handle; pass to senders to attach via
    /// [`crate::AnchorManager::attach_mpsc_stream_anchor`].
    pub fn handle(&self) -> StreamAnchorHandle {
        self.handle
    }

    /// Return a cloneable controller for cancelling the anchor from outside
    /// the stream.
    pub fn controller(&self) -> MpscStreamController {
        self.controller.clone()
    }

    /// Cancel the anchor and consume `self`.
    pub fn cancel(mut self) -> MpscStreamController {
        self.terminated = true;
        self.controller.cancel();
        self.controller.clone()
    }
}

impl<T> Unpin for MpscStreamAnchor<T> {}

impl<T> Drop for MpscStreamAnchor<T> {
    fn drop(&mut self) {
        if !self.terminated {
            self.controller.cancel();
        }
    }
}

impl<T: DeserializeOwned> Stream for MpscStreamAnchor<T> {
    type Item = Result<(SenderId, MpscFrame<T>), StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.cancel_stream).poll_next(cx) {
                Poll::Ready(Some(_)) | Poll::Ready(None) => {
                    this.terminated = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
            match Pin::new(&mut this.inner_stream).poll_next(cx) {
                Poll::Ready(Some((sender_id, bytes))) => {
                    match rmp_serde::from_slice::<StreamFrame<T>>(&bytes) {
                        Ok(frame) => {
                            let Some(mpsc_frame) = MpscFrame::from_stream_frame(frame) else {
                                continue; // heartbeat filtered
                            };
                            if mpsc_frame.is_sender_exit() {
                                remove_sender_slot(&this.registry, this.local_id, sender_id);
                            }
                            return Poll::Ready(Some(Ok((SenderId(sender_id), mpsc_frame))));
                        }
                        Err(e) => {
                            // Attribute to the sender but keep the anchor alive.
                            return Poll::Ready(Some(Err(StreamError::DeserializationError(
                                format!("sender {}: {}", sender_id, e),
                            ))));
                        }
                    }
                }
                Poll::Ready(None) => {
                    this.terminated = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Cancel every sender in an [`MpscAnchorEntry`].
///
/// For each slot:
/// 1. Cancel the pump_token (remote senders — stops the per-sender reader pump).
/// 2. Cancel same-worker senders via [`crate::streaming::control::SenderRegistry`].
/// 3. Fire-and-forget `_stream_cancel` AM for cross-worker senders.
///
/// Shared by [`MpscStreamController::cancel`] and the `_mpsc_anchor_cancel`
/// handler so both paths give attached senders the same cleanup.
pub(crate) fn cancel_all_senders(
    entry: &MpscAnchorEntry,
    local_worker: velo_ext::WorkerId,
    sender_registry: &Arc<crate::streaming::control::SenderRegistry>,
    messenger: Option<&Arc<crate::messenger::Messenger>>,
) {
    for (_sender_id, slot) in entry.senders.iter() {
        if let Some(pump_token) = &slot.pump_token {
            pump_token.cancel();
        }

        let Some(handle) = &slot.stream_cancel_handle else {
            continue;
        };
        crate::streaming::control::request_sender_cancel(
            *handle,
            local_worker,
            sender_registry,
            messenger,
        );
    }
}

/// Remove a sender slot from an MPSC anchor entry.
///
/// Returns the removed [`MpscSenderSlot`] (so the caller can cancel its
/// `pump_token`) or `None` if the slot was not present.
///
/// If the entry goes to zero senders and has an `unattached_timeout`
/// configured, the timeout is re-armed inside the same critical section.
///
/// Safe to call on a non-existent entry (no-op) and safe to call repeatedly
/// for the same sender_id.
pub(crate) fn remove_sender_slot(
    registry: &Arc<DashMap<u64, MpscAnchorEntry>>,
    local_id: u64,
    sender_id: u64,
) -> Option<MpscSenderSlot> {
    let mut entry = registry.get_mut(&local_id)?;
    let slot = entry.senders.remove(&sender_id)?;
    if entry.senders.is_empty()
        && let Some(duration) = entry.unattached_timeout
    {
        let expires = crate::streaming::deadline::deadline_after(duration);
        entry.expires = Some(expires);
        // Under the shard lock. That is safe: the push takes only a queue
        // shard lock, and the sweep reads `expires` and `queued` after this
        // guard is gone.
        let entry = &mut *entry;
        entry
            .deadlines
            .arm(registry, &mut entry.queued, expires, local_id);
    }
    Some(slot)
}

/// The unattached deadlines of one manager's MPSC anchors, swept by one
/// task. See [`crate::streaming::anchor::AnchorDeadlineQueue`].
pub(crate) type MpscDeadlines = DeadlineSweep<DashMap<u64, MpscAnchorEntry>>;

impl Expiring for MpscAnchorEntry {
    fn due(&self, now: tokio::time::Instant) -> Due {
        if !self.senders.is_empty() {
            return Due::Drop;
        }
        Due::at(self.expires, now)
    }

    fn queued(&self) -> Option<tokio::time::Instant> {
        self.queued
    }

    fn set_queued(&mut self, at: tokio::time::Instant) {
        self.queued = Some(at);
    }

    /// Dropping the entry drops its `frame_tx` clone. Remaining sender
    /// clones keep the receiver alive until they drop.
    fn expire(self) {
        self.cancel_token.cancel();
    }
}

impl SweepTarget for DashMap<u64, MpscAnchorEntry> {
    const FIRST_LOOK: Duration = crate::streaming::anchor::ANCHOR_FIRST_LOOK;
    const RECHECK: Duration = crate::streaming::anchor::ANCHOR_RECHECK;

    fn on_due(
        &self,
        due: Vec<(tokio::time::Instant, u64)>,
        now: tokio::time::Instant,
        renewed: &mut Vec<(tokio::time::Instant, u64)>,
    ) {
        expire_due(self, due, now, renewed);
    }
}
