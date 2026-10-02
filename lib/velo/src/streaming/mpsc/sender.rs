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
//!    is owned by the consumer via [`crate::streaming::mpsc::MpscStreamController::cancel`].

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::streaming::frame::{SendError, StreamFrame};
use crate::streaming::handle::StreamAnchorHandle;
use crate::streaming::sender::{
    StreamSenderCancelInfo, cached_detached, cached_dropped, cached_heartbeat,
};

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
///
/// Local drop queues `Dropped` after buffered items without blocking the caller.
/// A full queue needs a live runtime to deliver that event. Consumer cancellation
/// stops the wait; a runtime that has stopped cannot deliver the event.
pub struct MpscStreamSender<T> {
    sender_id: SenderId,
    channel: SenderChannel,
    handle: StreamAnchorHandle,
    heartbeat_cancel: CancellationToken,
    runtime: tokio::runtime::Handle,
    sent_terminal: bool,
    mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    cancel_token: CancellationToken,
    sender_stream_id: u64,
    sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    poison_tx: flume::Sender<()>,
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
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
    pub(crate) fn new(
        sender_id: SenderId,
        channel: SenderChannel,
        handle: StreamAnchorHandle,
        mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
        cancel: StreamSenderCancelInfo,
        heartbeat_interval: Duration,
        metrics: Option<Arc<crate::observability::VeloMetrics>>,
    ) -> Self {
        let StreamSenderCancelInfo {
            cancel_token,
            sender_stream_id,
            sender_registry,
            poison_tx,
        } = cancel;
        let heartbeat_cancel = cancel_token.child_token();

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
            runtime: tokio::runtime::Handle::current(),
            sent_terminal: false,
            mpsc_registry,
            cancel_token,
            sender_stream_id,
            sender_registry,
            poison_tx,
            metrics,
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

    /// Await `send`, giving up when the consumer cancels the stream.
    ///
    /// Over the mux the channel stays full for as long as the slot is paused
    /// at its byte cap, which lasts as long as the consumer does not read. A
    /// consumer that cancels, or drops its anchor, must still wake a sender
    /// parked here. Mirrors `StreamSender::send`.
    async fn until_cancelled<E>(
        &self,
        send: impl std::future::Future<Output = Result<(), E>>,
    ) -> Result<(), SendError> {
        tokio::select! {
            biased;
            _ = self.cancel_token.cancelled() => Err(SendError::ChannelClosed),
            result = send => result.map_err(|_| SendError::ChannelClosed),
        }
    }

    /// Send a typed item through the channel.
    pub async fn send(&self, item: T) -> Result<(), SendError> {
        if self.poison_tx.is_disconnected() {
            return Err(SendError::ChannelClosed);
        }
        let bytes = rmp_serde::to_vec(&StreamFrame::Item(item))
            .map_err(|e| SendError::SerializationError(e.to_string()))?;
        // try_send first so producer-side backpressure ticks before the
        // awaited send. Mirrors SPSC StreamSender::send.
        match &self.channel {
            SenderChannel::Local(tx) => match tx.try_send((self.sender_id.0, bytes)) {
                Ok(()) => Ok(()),
                Err(flume::TrySendError::Full(b)) => {
                    if let Some(m) = self.metrics.as_ref() {
                        m.record_producer_send_backpressure();
                    }
                    self.until_cancelled(tx.send_async(b)).await
                }
                Err(flume::TrySendError::Disconnected(_)) => Err(SendError::ChannelClosed),
            },
            SenderChannel::Remote(tx) => match tx.try_send(bytes) {
                Ok(()) => Ok(()),
                Err(flume::TrySendError::Full(b)) => {
                    if let Some(m) = self.metrics.as_ref() {
                        m.record_producer_send_backpressure();
                    }
                    self.until_cancelled(tx.send_async(b)).await
                }
                Err(flume::TrySendError::Disconnected(_)) => Err(SendError::ChannelClosed),
            },
        }
    }

    /// Send a soft error string. Does not terminate the sender.
    pub async fn send_err(&self, msg: impl ToString) -> Result<(), SendError> {
        if self.poison_tx.is_disconnected() {
            return Err(SendError::ChannelClosed);
        }
        let bytes = rmp_serde::to_vec(&StreamFrame::<()>::SenderError(msg.to_string()))
            .expect("SenderError serializes infallibly");
        match &self.channel {
            SenderChannel::Local(tx) => {
                self.until_cancelled(tx.send_async((self.sender_id.0, bytes)))
                    .await
            }
            SenderChannel::Remote(tx) => self.until_cancelled(tx.send_async(bytes)).await,
        }
    }

    /// Detach the sender cleanly, returning the anchor handle for reattach.
    ///
    /// Reattaching via [`crate::AnchorManager::attach_mpsc_stream_anchor`]
    /// allocates a fresh [`SenderId`]. Once polled, detach runs to completion
    /// on the sender's runtime even if this future is dropped. Consumer
    /// cancellation stops a detach that is waiting for channel space.
    pub async fn detach(mut self) -> Result<StreamAnchorHandle, SendError> {
        self.heartbeat_cancel.cancel();
        let cleanup = self.cleanup_guard();
        let channel = self.channel.clone();
        let cancel = self.cancel_token.clone();
        let sender_id = self.sender_id.0;
        // Flume can enqueue a frame before its send future is polled Ready.
        // Keep one task responsible for the terminal event and cleanup, so
        // abandoning this caller cannot send both Detached and Dropped.
        // Capture only transport state: T does not need a Send bound.
        let task = self.runtime.spawn(async move {
            let _cleanup = cleanup;
            let send = async {
                let bytes = cached_detached().clone();
                match channel {
                    SenderChannel::Local(tx) => tx
                        .send_async((sender_id, bytes))
                        .await
                        .map_err(|_| SendError::ChannelClosed),
                    SenderChannel::Remote(tx) => tx
                        .send_async(bytes)
                        .await
                        .map_err(|_| SendError::ChannelClosed),
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(SendError::ChannelClosed),
                result = send => result,
            }
        });
        self.sent_terminal = true;
        task.await.map_err(|_| SendError::ChannelClosed)??;
        Ok(self.handle)
    }
}

/// Cleanup belongs to the committed detach task, or to synchronous sender Drop.
/// The task guard also runs if its runtime shuts down.
struct SenderCleanup {
    local: bool,
    local_id: u64,
    sender_id: u64,
    mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    sender_stream_id: u64,
    sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    runtime: tokio::runtime::Handle,
}

impl Drop for SenderCleanup {
    fn drop(&mut self) {
        self.sender_registry.senders.remove(&self.sender_stream_id);
        // The last local slot may start an unattached timeout. Drop can run
        // on a plain thread, so provide the runtime captured at construction.
        let _entered = self.runtime.enter();
        if self.local
            && let Some(slot) = super::anchor::remove_sender_slot(
                &self.mpsc_registry,
                self.local_id,
                self.sender_id,
            )
            && let Some(token) = slot.pump_token
        {
            token.cancel();
        }
    }
}

impl<T> MpscStreamSender<T> {
    fn cleanup_guard(&self) -> SenderCleanup {
        SenderCleanup {
            local: matches!(self.channel, SenderChannel::Local(_)),
            local_id: self.handle.unpack().1,
            sender_id: self.sender_id.0,
            mpsc_registry: self.mpsc_registry.clone(),
            sender_stream_id: self.sender_stream_id,
            sender_registry: self.sender_registry.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<T> Drop for MpscStreamSender<T> {
    fn drop(&mut self) {
        if !self.sent_terminal {
            let _cleanup = self.cleanup_guard();
            self.heartbeat_cancel.cancel();
            let bytes = cached_dropped().clone();
            match &self.channel {
                SenderChannel::Local(tx) => {
                    let (_, local_id) = self.handle.unpack();
                    let consumer_cancel = self
                        .mpsc_registry
                        .get(&local_id)
                        .map(|entry| entry.cancel_token.clone());
                    if let Err(flume::TrySendError::Full(frame)) =
                        tx.try_send((self.sender_id.0, bytes))
                        && let Some(consumer_cancel) = consumer_cancel
                    {
                        // Keep the exit after queued items without blocking a
                        // runtime worker. Drop removes the sender registry
                        // entry, so only the anchor token can stop this wait.
                        let tx = tx.clone();
                        self.runtime.spawn(async move {
                            tokio::select! {
                                biased;
                                _ = consumer_cancel.cancelled() => {}
                                _ = tx.send_async(frame) => {}
                            }
                        });
                    }
                }
                SenderChannel::Remote(tx) => {
                    // Keep remote drop non-blocking; transport close is the
                    // fallback if we cannot enqueue the explicit sentinel.
                    let _ = tx.try_send(bytes);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_heartbeats_while_the_sender_is_retained() {
        let (tx, rx) = flume::bounded(16);
        let (poison_tx, _poison_rx) = flume::bounded(1);
        let cancel_token = CancellationToken::new();
        let heartbeat = Duration::from_secs(5);
        let sender = MpscStreamSender::<u32>::new(
            SenderId(1),
            SenderChannel::Remote(tx),
            StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1),
            Arc::new(DashMap::new()),
            StreamSenderCancelInfo {
                cancel_token: cancel_token.clone(),
                sender_stream_id: 1,
                sender_registry: Arc::new(crate::streaming::control::SenderRegistry::default()),
                poison_tx,
            },
            heartbeat,
            None,
        );
        assert_eq!(rx.recv_async().await.unwrap(), *cached_heartbeat());
        cancel_token.cancel();
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(heartbeat * 2, rx.recv_async())
                .await
                .is_err()
        );
        drop(sender);
    }
}
