// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`MpscStreamSender<T>`]: typed sender for an MPSC anchor.
//!
//! Structurally mirrors [`crate::StreamSender`] with two differences:
//!
//! 1. **Channel dispatch**: local senders write `(sender_id, bytes)` directly
//!    into the anchor's shared `flume::Sender<(u64, Vec<u8>)>`; remote senders
//!    write raw `Vec<u8>` through the transport and the anchor-side
//!    per-sender pump tags each frame with the originating `sender_id`.
//! 2. **No `finalize`**: MPSC senders cannot close the anchor. Finalization
//!    is owned by the consumer via [`crate::mpsc::MpscStreamController::cancel`].

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::frame::{SendError, StreamFrame};
use crate::handle::StreamAnchorHandle;
use crate::sender::{StreamSenderCancelInfo, cached_detached, cached_dropped, cached_heartbeat};

use super::anchor::MpscAnchorEntry;
use super::types::SenderId;

/// Local vs. remote frame delivery channel.
///
/// Cloneable because the heartbeat background task holds its own clone and
/// the main sender keeps another for per-item `send()` calls.
#[derive(Clone)]
pub(crate) enum SenderChannel {
    /// Same-worker write path: push `(sender_id, bytes)` directly into the
    /// anchor's shared frame channel.
    Local(flume::Sender<(u64, Vec<u8>)>),
    /// Cross-worker write path: push raw bytes through a transport
    /// [`flume::Sender`]; the anchor-side `mpsc_reader_pump` tags them with
    /// the originating `sender_id` before forwarding to the shared channel.
    Remote(flume::Sender<Vec<u8>>),
}

/// Typed sender for an MPSC anchor.
///
/// Holds one of two channels ([`SenderChannel`]) plus the usual heartbeat
/// task and cancel/poison plumbing. Created by
/// [`crate::AnchorManager::attach_mpsc_stream_anchor`] (local) or by the
/// `_mpsc_anchor_attach` handler round-trip (remote).
pub struct MpscStreamSender<T> {
    sender_id: SenderId,
    channel: SenderChannel,
    handle: StreamAnchorHandle,
    heartbeat_cancel: CancellationToken,
    sent_terminal: bool,
    mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    cancel_token: CancellationToken,
    sender_stream_id: u64,
    sender_registry: Arc<crate::control::SenderRegistry>,
    poison_tx: flume::Sender<()>,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for MpscStreamSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MpscStreamSender")
            .field("sender_id", &self.sender_id)
            .field("handle", &self.handle)
            .field("sent_terminal", &self.sent_terminal)
            .finish_non_exhaustive()
    }
}

impl<T: Serialize> MpscStreamSender<T> {
    /// Construct a new MPSC sender and spawn its heartbeat task.
    ///
    /// All arguments except `cancel` are distinct because bundling them
    /// further would obscure the local/remote dispatch. `cancel` is the same
    /// [`StreamSenderCancelInfo`] bundle used by SPSC senders so the
    /// `_stream_cancel` handler can cancel both kinds uniformly.
    #[allow(clippy::too_many_arguments)] // mirrors SPSC sender.rs::new; five ctor args plus cancel bundle
    pub(crate) fn new(
        sender_id: SenderId,
        channel: SenderChannel,
        handle: StreamAnchorHandle,
        mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
        cancel: StreamSenderCancelInfo,
        heartbeat_interval: Duration,
    ) -> Self {
        let StreamSenderCancelInfo {
            cancel_token,
            sender_stream_id,
            sender_registry,
            poison_tx,
        } = cancel;
        let heartbeat_cancel = CancellationToken::new();

        // Heartbeat task — skip first immediate tick, then emit cached
        // heartbeat bytes via non-blocking `try_send`. Matches the SPSC
        // heartbeat shape in sender.rs:158-177 but dispatches on the channel
        // enum.
        let hb_cancel = heartbeat_cancel.clone();
        let hb_channel = channel.clone();
        let hb_sender_id = sender_id.0;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.tick().await; // drop the first immediate tick
            loop {
                tokio::select! {
                    _ = hb_cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let bytes = cached_heartbeat().clone();
                        match &hb_channel {
                            SenderChannel::Local(tx) => {
                                let _ = tx.try_send((hb_sender_id, bytes));
                            }
                            SenderChannel::Remote(tx) => {
                                let _ = tx.try_send(bytes);
                            }
                        }
                    }
                }
            }
        });

        Self {
            sender_id,
            channel,
            handle,
            heartbeat_cancel,
            sent_terminal: false,
            mpsc_registry,
            cancel_token,
            sender_stream_id,
            sender_registry,
            poison_tx,
            _phantom: PhantomData,
        }
    }

    /// This sender's assigned identifier. Unique within the anchor's lifetime.
    pub fn sender_id(&self) -> SenderId {
        self.sender_id
    }

    /// Return a cloneable cancellation token that fires when the consumer
    /// cancels the stream (`MpscStreamController::cancel`).
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Send a typed item through the channel.
    pub async fn send(&self, item: T) -> Result<(), SendError> {
        if self.poison_tx.is_disconnected() {
            return Err(SendError::ChannelClosed);
        }
        let bytes = rmp_serde::to_vec(&StreamFrame::Item(item))
            .map_err(|e| SendError::SerializationError(e.to_string()))?;
        match &self.channel {
            SenderChannel::Local(tx) => tx
                .send_async((self.sender_id.0, bytes))
                .await
                .map_err(|_| SendError::ChannelClosed),
            SenderChannel::Remote(tx) => tx
                .send_async(bytes)
                .await
                .map_err(|_| SendError::ChannelClosed),
        }
    }

    /// Send a soft error string. Does not terminate the sender.
    pub async fn send_err(&self, msg: impl ToString) -> Result<(), SendError> {
        let bytes = rmp_serde::to_vec(&StreamFrame::<()>::SenderError(msg.to_string()))
            .expect("SenderError serializes infallibly");
        match &self.channel {
            SenderChannel::Local(tx) => tx
                .send_async((self.sender_id.0, bytes))
                .await
                .map_err(|_| SendError::ChannelClosed),
            SenderChannel::Remote(tx) => tx
                .send_async(bytes)
                .await
                .map_err(|_| SendError::ChannelClosed),
        }
    }

    /// Detach the sender cleanly, returning the anchor handle for reattach.
    ///
    /// Reattaching via [`crate::AnchorManager::attach_mpsc_stream_anchor`]
    /// allocates a fresh [`SenderId`].
    pub fn detach(mut self) -> Result<StreamAnchorHandle, SendError> {
        self.heartbeat_cancel.cancel();
        self.sent_terminal = true;
        let bytes = cached_detached().clone();
        let result = match &self.channel {
            SenderChannel::Local(tx) => tx
                .send((self.sender_id.0, bytes))
                .map_err(|_| SendError::ChannelClosed),
            SenderChannel::Remote(tx) => tx.send(bytes).map_err(|_| SendError::ChannelClosed),
        };

        // Same-worker: remove the slot locally so reattach can reuse capacity
        // immediately. Cross-worker: the remote pump forwards the Detached
        // sentinel and the anchor's `poll_next` removes the slot on its side.
        let (_, local_id) = self.handle.unpack();
        crate::mpsc::anchor::remove_sender_slot(&self.mpsc_registry, local_id, self.sender_id.0);

        self.sender_registry.senders.remove(&self.sender_stream_id);
        result?;
        Ok(self.handle)
    }
}

impl<T> Drop for MpscStreamSender<T> {
    fn drop(&mut self) {
        // Idempotent sender-registry cleanup.
        self.sender_registry.senders.remove(&self.sender_stream_id);
        if !self.sent_terminal {
            self.heartbeat_cancel.cancel();
            let bytes = cached_dropped().clone();
            match &self.channel {
                SenderChannel::Local(tx) => {
                    let _ = tx.send((self.sender_id.0, bytes));
                }
                SenderChannel::Remote(tx) => {
                    let _ = tx.send(bytes);
                }
            }
            // Local: drop slot so an unattached-timeout can re-arm when last sender leaves.
            let (_, local_id) = self.handle.unpack();
            crate::mpsc::anchor::remove_sender_slot(
                &self.mpsc_registry,
                local_id,
                self.sender_id.0,
            );
        }
    }
}
