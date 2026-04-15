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

use dashmap::DashMap;
use futures::Stream;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::frame::{StreamError, StreamFrame};
use crate::handle::StreamAnchorHandle;

use super::frame::MpscFrame;
use super::types::SenderId;

/// One attached sender's tracking state inside an [`MpscAnchorEntry`].
pub(crate) struct MpscSenderSlot {
    /// Child of `MpscAnchorEntry::cancel_token`. `Some` for remote senders
    /// (used to stop the per-sender reader pump without poisoning the parent);
    /// `None` for local senders (no pump).
    pub pump_token: Option<CancellationToken>,
    /// The sender's `StreamCancelHandle` — used by
    /// [`MpscStreamController::cancel`] to route `_stream_cancel` upstream.
    /// `None` means same-worker cancel via [`crate::control::SenderRegistry`]
    /// is sufficient.
    pub stream_cancel_handle: Option<crate::control::StreamCancelHandle>,
}

/// One entry in the MPSC anchor registry.
///
/// Lives in `Arc<DashMap<u64, MpscAnchorEntry>>`. Every mutation of
/// `senders` runs under the DashMap shard lock and must not be held across an
/// `.await` point — follow the bind-then-lock precedent in
/// [`crate::control::create_anchor_attach_handler`].
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
    /// Stored duration used to respawn the unattached timeout after the
    /// last sender departs.
    pub unattached_timeout: Option<Duration>,
    /// Currently-armed timeout task cancel token. `None` while at least one
    /// sender is attached.
    pub timeout_cancel: Option<CancellationToken>,
    /// Negotiated heartbeat cadence echoed to each attaching sender.
    pub heartbeat_interval: Duration,
    /// Optional cap on concurrent attached senders.
    pub max_senders: Option<usize>,
}

// ---------------------------------------------------------------------------
// MpscStreamController
// ---------------------------------------------------------------------------

struct MpscStreamControllerInner {
    local_id: u64,
    registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    sender_registry: Arc<crate::control::SenderRegistry>,
    messenger: Option<Arc<velo_messenger::Messenger>>,
    cancelled: AtomicBool,
}

/// Cloneable cancel handle for an [`MpscStreamAnchor`].
///
/// Unlike [`crate::StreamController`] (single-sender), this iterates every
/// attached sender and poisons/cancels each in turn. Idempotent: the first
/// caller wins via [`AtomicBool::compare_exchange`]; subsequent calls return
/// immediately.
#[derive(Clone)]
pub struct MpscStreamController {
    inner: Arc<MpscStreamControllerInner>,
}

impl MpscStreamController {
    /// Close the anchor. Removes it from the MPSC registry, cancels every
    /// attached sender (same-worker via [`crate::control::SenderRegistry`]
    /// poisoning, cross-worker via fire-and-forget `_stream_cancel` AM), and
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
        if let Some(tc) = entry.timeout_cancel {
            tc.cancel();
        }

        for (_sender_id, slot) in entry.senders.into_iter() {
            if let Some(pump_token) = slot.pump_token {
                pump_token.cancel();
            }

            let Some(handle) = slot.stream_cancel_handle else {
                continue;
            };
            let (sender_worker_id, sender_stream_id) = handle.unpack();

            // Same-worker fast path: poison the sender's poison channel
            // directly so `send()` starts returning `ChannelClosed` without an
            // AM round-trip. Idempotent.
            if let Some((_, sender_entry)) =
                self.inner.sender_registry.senders.remove(&sender_stream_id)
            {
                drop(sender_entry.rx_closer.lock().unwrap().take());
                sender_entry.cancel_token.cancel();
            }

            // Cross-worker: fire-and-forget `_stream_cancel` AM.
            if let Some(messenger) = self.inner.messenger.clone() {
                let payload =
                    serde_json::to_vec(&crate::control::StreamCancelRequest { sender_stream_id })
                        .expect("serialize StreamCancelRequest");
                if let Ok(rt) = tokio::runtime::Handle::try_current() {
                    rt.spawn(async move {
                        let _ = messenger
                            .am_send_streaming("_stream_cancel")
                            .expect("am_send_streaming builder")
                            .raw_payload(bytes::Bytes::from(payload))
                            .worker(sender_worker_id)
                            .send()
                            .await;
                    });
                }
            }
        }
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
        registry: Arc<DashMap<u64, MpscAnchorEntry>>,
        sender_registry: Arc<crate::control::SenderRegistry>,
        messenger: Option<Arc<velo_messenger::Messenger>>,
    ) -> Self {
        let inner = Arc::new(MpscStreamControllerInner {
            local_id,
            registry: registry.clone(),
            sender_registry,
            messenger,
            cancelled: AtomicBool::new(false),
        });
        let controller = MpscStreamController { inner };
        Self {
            handle,
            inner_stream: rx.into_stream(),
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

/// Remove a sender slot from an MPSC anchor entry.
///
/// If the entry goes to zero senders and has an `unattached_timeout`
/// configured, a fresh timeout task is spawned.
///
/// Safe to call on a non-existent entry (no-op) and safe to call repeatedly
/// for the same sender_id.
pub(crate) fn remove_sender_slot(
    registry: &Arc<DashMap<u64, MpscAnchorEntry>>,
    local_id: u64,
    sender_id: u64,
) {
    if let Some(mut entry) = registry.get_mut(&local_id) {
        if entry.senders.remove(&sender_id).is_none() {
            return;
        }
        if entry.senders.is_empty()
            && let Some(duration) = entry.unattached_timeout
        {
            let parent = entry.cancel_token.clone();
            let tc = spawn_mpsc_timeout_task(registry.clone(), local_id, duration, &parent);
            entry.timeout_cancel = Some(tc);
        }
    }
}

/// Spawn a background task that removes the anchor after `timeout` elapses,
/// cancelled by either explicit cancel or a new sender attaching.
pub(crate) fn spawn_mpsc_timeout_task(
    registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    local_id: u64,
    timeout: Duration,
    parent_cancel: &CancellationToken,
) -> CancellationToken {
    let tc = parent_cancel.child_token();
    let tc_clone = tc.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = tc_clone.cancelled() => {}
            _ = tokio::time::sleep(timeout) => {
                if let Some((_, entry)) = registry.remove(&local_id) {
                    entry.cancel_token.cancel();
                    // Dropping entry drops the stored frame_tx clone. Any remaining
                    // sender clones keep the receiver alive until they're dropped.
                }
            }
        }
    });
    tc
}
