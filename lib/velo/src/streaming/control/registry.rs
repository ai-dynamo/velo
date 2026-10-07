// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The sender side of the control plane: the registry of open senders, and
//! the routes by which a consumer stops or cancels one.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{StreamCancelHandle, StreamCancelRequest};

/// Whether a sender is cancelled, readable on every send without a lock.
///
/// `CancellationToken::is_cancelled` locks a mutex (tokio-util 0.7.18 and
/// 0.7.19): 8.7 ns per call against 0.3 ns for this load, paid on every
/// record. [`SenderEntry::cancel`] sets the flag before it cancels the token,
/// so a send after it fails at once. A token cancelled any other way sets the
/// flag from the sender's heartbeat task, one scheduler hop later, while the
/// runtime that built the sender is alive.
#[derive(Clone, Debug, Default)]
pub struct SenderClosed(Arc<AtomicBool>);

impl SenderClosed {
    /// Mark the sender cancelled.
    pub fn close(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether the sender is cancelled.
    pub fn is_closed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Held by a sender's heartbeat task, which ends when the sender's token is
/// cancelled. A token cancelled other than through [`SenderEntry::cancel`]
/// reaches the per-record check here. A drop guard, so a cancel that came
/// before the task was dropped with its runtime still sets the flag. A direct
/// cancel after that runtime is gone has no task to see it, and `send` keeps
/// accepting records; [`SenderEntry::cancel`] still works then.
pub(crate) struct CloseOnCancel {
    token: tokio_util::sync::CancellationToken,
    closed: SenderClosed,
}

impl CloseOnCancel {
    /// Built with the sender. A token already cancelled by then closes the
    /// flag at once: no task needs to run for it.
    pub(crate) fn new(token: &tokio_util::sync::CancellationToken, closed: &SenderClosed) -> Self {
        if token.is_cancelled() {
            closed.close();
        }
        Self {
            token: token.clone(),
            closed: closed.clone(),
        }
    }
}

impl Drop for CloseOnCancel {
    fn drop(&mut self) {
        if self.token.is_cancelled() {
            self.closed.close();
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
