// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`StreamSender<T>`]: typed sender for pushing frames with heartbeat and drop safety.
//!
//! `StreamSender` is the primary write-side abstraction for the streaming protocol.
//! It serializes typed items into [`StreamFrame`] via `rmp_serde`, pushes them through
//! a [`flume::Sender<Vec<u8>>`], manages a background heartbeat task, and guarantees
//! a [`StreamFrame::Dropped`] sentinel on abnormal exit via `impl Drop`.
//!
//! # Lifecycle
//!
//! A `StreamSender<T>` is created by [`crate::streaming::anchor::AnchorManager::attach_stream_anchor`] and
//! must be terminated via one of three paths:
//!
//! 1. **[`finalize(self)`](StreamSender::finalize)** — clean close, sends `Finalized` sentinel.
//! 2. **[`detach(self)`](StreamSender::detach)** — clean detach, sends `Detached` sentinel, returns handle for re-attach.
//! 3. **Drop** — abnormal exit, sends `Dropped` sentinel.
//!
//! The heartbeat background task is cancelled in all three paths before the
//! terminal sentinel is sent.
//!
//! None of the three blocks the calling thread on a full channel. `send_terminal`
//! is where that is arranged, and why it has to be.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::streaming::anchor::AnchorEntry;
use crate::streaming::frame::{SendError, StreamFrame};
use crate::streaming::handle::StreamAnchorHandle;

/// Cancellation state for a [`StreamSender`].
///
/// Bundled to keep [`StreamSender::new`] under the argument-count threshold
/// while still keeping the call sites readable. All fields are required
/// — there are no defaults.
pub(crate) struct StreamSenderCancelInfo {
    /// User-facing cancellation signal: fires when `_stream_cancel` is received.
    pub cancel_token: CancellationToken,
    /// Sender-side registry key, also used by the `_stream_cancel` handler.
    pub sender_stream_id: u64,
    /// Sender-side registry shared with `_stream_cancel`.
    pub sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    /// The flag the registry entry's cancel sets. Passed in, not looked up:
    /// a cancel that arrives before the sender is built removes the entry.
    pub closed: crate::streaming::control::SenderClosed,
}

// ---------------------------------------------------------------------------
// Cached sentinel bytes (OnceLock)
// ---------------------------------------------------------------------------

/// Cached serialized bytes for `StreamFrame::<()>::Heartbeat`.
/// Avoids re-serializing on every heartbeat tick.
pub(crate) fn cached_heartbeat() -> &'static Vec<u8> {
    static HEARTBEAT: OnceLock<Vec<u8>> = OnceLock::new();
    HEARTBEAT.get_or_init(|| {
        rmp_serde::to_vec(&StreamFrame::<()>::Heartbeat).expect("Heartbeat serializes infallibly")
    })
}

/// Cached serialized bytes for `StreamFrame::<()>::Dropped`.
/// Used in Drop impl and by the watchdogs (`reader_pump`, `stream_watchdog`)
/// and the unclaimed-bind reap when they inject `Dropped`.
pub(crate) fn cached_dropped() -> &'static Vec<u8> {
    static DROPPED: OnceLock<Vec<u8>> = OnceLock::new();
    DROPPED.get_or_init(|| {
        rmp_serde::to_vec(&StreamFrame::<()>::Dropped).expect("Dropped serializes infallibly")
    })
}

/// Cached serialized bytes for `StreamFrame::<()>::Finalized`.
pub(crate) fn cached_finalized() -> &'static Vec<u8> {
    static FINALIZED: OnceLock<Vec<u8>> = OnceLock::new();
    FINALIZED.get_or_init(|| {
        rmp_serde::to_vec(&StreamFrame::<()>::Finalized).expect("Finalized serializes infallibly")
    })
}

/// Cached serialized bytes for `StreamFrame::<()>::Detached`.
pub(crate) fn cached_detached() -> &'static Vec<u8> {
    static DETACHED: OnceLock<Vec<u8>> = OnceLock::new();
    DETACHED.get_or_init(|| {
        rmp_serde::to_vec(&StreamFrame::<()>::Detached).expect("Detached serializes infallibly")
    })
}

/// How `rmp_serde` encodes every `StreamFrame::Item`: a one-entry map (`0x81`)
/// keyed by the fixstr `"Item"` (`0xa4` plus four bytes), then the payload.
pub(crate) const ITEM_PREFIX: &[u8] = &[0x81, 0xa4, b'I', b't', b'e', b'm'];

/// Whether these raw frame bytes are a terminal sentinel.
///
/// Terminal means the stream ends here: `Dropped`, `Detached`, `Finalized` and
/// `TransportError`. Every transport needs this verdict — the TCP and gRPC
/// egress pumps stop after one, and the mux spends a slot's reserved terminal
/// credit on one and closes the slot in the same batch — so it lives beside the
/// cached sentinel bytes it compares against rather than being reimplemented per
/// transport.
///
/// The three no-payload sentinels are a byte comparison against their cached
/// encodings. `TransportError` carries a `String`, so it has no cached form and
/// costs a decode; that decode is also why an ordinary `Item` payload is not
/// mistaken for one — a payload that happens to deserialize as `StreamFrame<()>`
/// can only do so as a variant this function then rejects.
///
/// Both ends of the mux classify every data record, so an `Item` never reaches
/// that decode: a frame keyed `"Item"` can only decode as `Item` or fail, never
/// as `TransportError`, so [`ITEM_PREFIX`] decides it exactly. The failed decode
/// it replaces formatted an error `String` per record.
pub(crate) fn is_terminal_sentinel(bytes: &[u8]) -> bool {
    if bytes.starts_with(ITEM_PREFIX) {
        return false;
    }
    if bytes == cached_dropped().as_slice()
        || bytes == cached_detached().as_slice()
        || bytes == cached_finalized().as_slice()
    {
        return true;
    }

    matches!(
        rmp_serde::from_slice::<StreamFrame<()>>(bytes),
        Ok(StreamFrame::TransportError(_))
    )
}

/// Typed sender for pushing frames through the streaming channel.
///
/// Holds a [`flume::Sender<Vec<u8>>`] for serialized frame bytes, a
/// [`StreamAnchorHandle`] identifying the anchor, a [`CancellationToken`]
/// to stop the background heartbeat task, and a reference to the anchor
/// registry so that [`detach`](StreamSender::detach) can clear the attachment
/// flag once its sentinel is in the channel.
///
/// `T` is the user-defined item payload type. The `Serialize` bound is required
/// for [`send`](StreamSender::send) to serialize `StreamFrame::Item(T)`.
/// Sentinel methods (`finalize`, `detach`, Drop) use `StreamFrame::<()>` to
/// avoid the `Serialize` bound (sentinel variants carry no `T` data).
pub struct StreamSender<T> {
    tx: flume::Sender<Vec<u8>>,
    handle: StreamAnchorHandle,
    heartbeat_cancel: CancellationToken,
    sent_terminal: bool,
    /// Registry reference for clearing the attachment flag on detach.
    local_registry: Option<Arc<DashMap<u64, AnchorEntry>>>,
    /// User-facing cancellation signal: fires when _stream_cancel is received.
    cancel_token: CancellationToken,
    /// Checked per record instead of the token, which takes a lock.
    closed: crate::streaming::control::SenderClosed,
    stop_token: CancellationToken,
    /// Key in the sender-side registry for cleanup and for the _stream_cancel handler.
    sender_stream_id: u64,
    /// Sender-side registry shared with the _stream_cancel handler.
    sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    /// Optional metrics handle for producer-side backpressure observability.
    /// `None` for in-process AnchorManager constructions that skip metrics
    /// (test fixtures and direct AnchorManagerBuilder users).
    metrics: Option<Arc<crate::observability::VeloMetrics>>,
    /// The streaming transport the attach settled on, or `None` when the
    /// attach never negotiated one. See
    /// [`negotiated_transport`](StreamSender::negotiated_transport).
    negotiated_transport: Option<velo_ext::TransportKey>,
    /// The runtime the sender was made on. A terminal that meets a full
    /// channel on a thread with no runtime waits in a task here.
    runtime: tokio::runtime::Handle,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> std::fmt::Debug for StreamSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSender")
            .field("handle", &self.handle)
            .field("sent_terminal", &self.sent_terminal)
            .finish_non_exhaustive()
    }
}

impl<T: Serialize> StreamSender<T> {
    /// Create a new `StreamSender` and spawn the background heartbeat task.
    ///
    /// The heartbeat task emits [`StreamFrame::Heartbeat`] at `heartbeat_interval`
    /// via non-blocking `try_send`. It is cancelled when the sender is finalized,
    /// detached, or dropped. `heartbeat_interval` is negotiated by the consumer
    /// via [`crate::streaming::control::AnchorAttachResponse::Ok::heartbeat_interval_ms`]
    /// — both sides must agree so the consumer's watchdog deadline matches.
    ///
    /// `registry` is a shared reference to the anchor registry so that
    /// [`detach`](StreamSender::detach) can atomically clear the attachment flag.
    ///
    /// `negotiated_transport` is the key the receiver answered the attach with,
    /// and `None` on the same-worker path, which negotiates nothing. It is
    /// carried rather than derived because the answer is the receiver's to give
    /// and this side has no second copy of it.
    pub(crate) fn new(
        tx: flume::Sender<Vec<u8>>,
        handle: StreamAnchorHandle,
        registry: Arc<DashMap<u64, AnchorEntry>>,
        cancel: StreamSenderCancelInfo,
        heartbeat_interval: Duration,
        metrics: Option<Arc<crate::observability::VeloMetrics>>,
        negotiated_transport: Option<velo_ext::TransportKey>,
    ) -> Self {
        let StreamSenderCancelInfo {
            cancel_token,
            sender_stream_id,
            sender_registry,
            closed,
        } = cancel;
        let stop_token = sender_registry
            .senders
            .get(&sender_stream_id)
            .map(|entry| entry.stop_token.clone())
            .unwrap_or_else(|| cancel_token.child_token());
        let heartbeat_cancel = cancel_token.child_token();

        // Spawn heartbeat background task
        let cancel = heartbeat_cancel.clone();
        let tx_clone = tx.clone();
        let hb_closed = crate::streaming::control::CloseOnCancel::new(&cancel_token, &closed);
        tokio::spawn(async move {
            let _close = hb_closed;
            // First tick one period out. Waiting for an immediate first
            // tick would delay seeing a cancelled token by a timer turn.
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + heartbeat_interval,
                heartbeat_interval,
            );

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        // Use cached heartbeat bytes — avoids rmp_serde::to_vec per tick.
                        let bytes = cached_heartbeat().clone();
                        // Non-blocking try_send: if the channel is full we silently
                        // drop the heartbeat rather than stalling the sender.
                        let _ = tx_clone.try_send(bytes);
                    }
                }
            }
        });

        Self {
            tx,
            handle,
            runtime: tokio::runtime::Handle::current(),
            heartbeat_cancel,
            stop_token,
            sent_terminal: false,
            local_registry: negotiated_transport.is_none().then_some(registry),
            cancel_token,
            closed,
            sender_stream_id,
            sender_registry,
            metrics,
            negotiated_transport,
            _phantom: std::marker::PhantomData,
        }
    }

    /// The streaming transport this sender's frames cross, as the receiver
    /// named it when it answered the attach.
    ///
    /// `Some(`[`MESSENGER_MUX_KEY`]`)` means the frames are multiplexed onto
    /// the messenger alongside every other stream heading for the same worker;
    /// any other key is a per-stream connection on that transport. `None` means
    /// the attach was same-worker: the frames go straight into the anchor's
    /// channel and no transport was negotiated because none is involved.
    ///
    /// This is the receiver's answer, not a lookup of what runs locally. The
    /// two agree except on the empty-registry convenience path, where any key
    /// resolves to this node's default transport.
    ///
    /// [`MESSENGER_MUX_KEY`]: crate::streaming::MESSENGER_MUX_KEY
    pub fn negotiated_transport(&self) -> Option<&velo_ext::TransportKey> {
        self.negotiated_transport.as_ref()
    }

    /// Returns a cloneable `CancellationToken` that fires when the consumer
    /// cancels the stream (e.g. drops `StreamAnchor` or calls `StreamController::cancel()`).
    ///
    /// Use in a `tokio::select!` to stop production proactively:
    /// ```no_run
    /// # async fn example() {
    /// # let mut sender: velo::streaming::StreamSender<u32> = todo!();
    /// # async fn produce() -> u32 { 0 }
    /// loop {
    ///     tokio::select! {
    ///         _ = sender.cancellation_token().cancelled() => break,
    ///         val = produce() => { if sender.send(val).await.is_err() { break; } }
    ///     }
    /// }
    /// # }
    /// ```
    ///
    /// Also fires for ticket opens, including an idle producer whose slot closes.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Fires when the consumer requests graceful stop or cancels the stream.
    /// The producer can send buffered output and finalize after graceful stop.
    pub fn stop_token(&self) -> CancellationToken {
        self.stop_token.clone()
    }

    /// Send a typed item through the channel.
    ///
    /// Serializes `StreamFrame::Item(item)` via `rmp_serde` and pushes the
    /// resulting bytes through the flume channel asynchronously.
    ///
    /// # Errors
    ///
    /// - [`SendError::SerializationError`] if `rmp_serde::to_vec` fails.
    /// - [`SendError::ChannelClosed`] if the receiver has been dropped or the
    ///   stream has been cancelled. A cancel through `SenderEntry::cancel` is
    ///   seen at once; a direct cancel of [`Self::cancellation_token`] once
    ///   the sender's heartbeat task has run, which needs the runtime that
    ///   built the sender to be alive.
    pub async fn send(&self, item: T) -> Result<(), SendError> {
        if self.closed.is_closed() {
            return Err(SendError::ChannelClosed);
        }
        let bytes = rmp_serde::to_vec(&StreamFrame::Item(item))
            .map_err(|e| SendError::SerializationError(e.to_string()))?;
        // Try non-blocking first so we can record producer-side backpressure
        // before falling through to the awaited send. The connect-side channel
        // is 4096 deep on the per-stream path and C+1 deep under the mux.
        match self.tx.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(b)) => {
                if let Some(m) = self.metrics.as_ref() {
                    m.record_producer_send_backpressure();
                }
                tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => Err(SendError::ChannelClosed),
                    result = self.tx.send_async(b) => result.map_err(|_| SendError::ChannelClosed),
                }
            }
            Err(flume::TrySendError::Disconnected(_)) => Err(SendError::ChannelClosed),
        }
    }

    /// Send a soft error through the channel.
    ///
    /// Serializes `StreamFrame::SenderError(msg)` and pushes via `send_async`.
    /// Uses `StreamFrame::<()>::SenderError` to avoid requiring `T: Serialize`
    /// just for error sending -- the SenderError variant carries only a String,
    /// so the msgpack encoding is identical regardless of the phantom type `T`.
    ///
    /// # Errors
    ///
    /// - [`SendError::ChannelClosed`] if the receiver has been dropped or the
    ///   stream has been cancelled, under the same rule as [`Self::send`].
    pub async fn send_err(&self, msg: impl ToString) -> Result<(), SendError> {
        if self.closed.is_closed() {
            return Err(SendError::ChannelClosed);
        }
        // Safe to use StreamFrame::<()> here: the SenderError variant carries
        // only a String and its msgpack encoding is identical for any T.
        let bytes = rmp_serde::to_vec(&StreamFrame::<()>::SenderError(msg.to_string()))
            .expect("SenderError serializes infallibly");
        // The same shape as `send`, so the two agree on a cancel.
        match self.tx.try_send(bytes) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(b)) => {
                tokio::select! {
                    biased;
                    _ = self.cancel_token.cancelled() => Err(SendError::ChannelClosed),
                    result = self.tx.send_async(b) => result.map_err(|_| SendError::ChannelClosed),
                }
            }
            Err(flume::TrySendError::Disconnected(_)) => Err(SendError::ChannelClosed),
        }
    }

    /// Permanently close the stream by sending a `Finalized` sentinel.
    ///
    /// Cancels the heartbeat task, hands `StreamFrame::Finalized` to the channel
    /// through `send_terminal`, and consumes `self`. The subsequent `Drop` will
    /// see `sent_terminal = true` and skip the `Dropped` sentinel.
    ///
    /// Ordering holds even when the channel is full and the sentinel is queued by
    /// a task: every record this sender produced is already in the channel ahead
    /// of it, and this sender can produce no more.
    ///
    /// # Errors
    ///
    /// - [`SendError::ChannelClosed`] if the receiver has already been dropped.
    pub fn finalize(mut self) -> Result<(), SendError> {
        self.heartbeat_cancel.cancel();
        let bytes = cached_finalized().clone();
        self.sent_terminal = true;
        // Clean up sender registry entry before returning
        self.sender_registry.senders.remove(&self.sender_stream_id);
        send_terminal(&self.runtime, &self.tx, bytes, || {})
    }

    /// Detach the sender from the anchor by sending a `Detached` sentinel.
    ///
    /// Cancels the heartbeat task, hands `StreamFrame::Detached` to the channel
    /// through `send_terminal`, clears the attachment flag in the registry (so a
    /// new sender can attach), and returns the [`StreamAnchorHandle`] for
    /// re-attachment. The subsequent `Drop` will see `sent_terminal = true` and
    /// skip `Dropped`.
    ///
    /// The attachment flag is cleared **after** the sentinel reaches the channel,
    /// which on a full channel means after the queuing task drains into it. A
    /// sender that attached earlier would put its records ahead of the `Detached`
    /// that ends the previous attachment, and the stream's frame order is the one
    /// thing a re-attach may not disturb. Until then a re-attach sees the anchor
    /// as still attached and can retry.
    ///
    /// # Errors
    ///
    /// - [`SendError::ChannelClosed`] if the receiver has already been dropped.
    pub fn detach(mut self) -> Result<StreamAnchorHandle, SendError> {
        self.heartbeat_cancel.cancel();
        let bytes = cached_detached().clone();
        self.sent_terminal = true;
        let registry = self.local_registry.clone();
        let (worker_id, local_id) = self.handle.unpack();
        let sender_stream_id = self.sender_stream_id;
        let runtime = self.runtime.clone();
        send_terminal(&self.runtime, &self.tx, bytes, move || {
            // The fast path runs this on the caller's thread, which may have
            // no runtime; the unattached timeout must still be armed.
            let _runtime = crate::streaming::tasks::enter_if_outside_runtime(&runtime);
            if let Some(registry) = registry
                && let Some(mut entry) = registry.get_mut(&local_id)
                && entry
                    .stream_cancel_handle
                    .is_some_and(|handle| handle.unpack() == (worker_id, sender_stream_id))
            {
                entry.attachment = false;
                entry.restart_unattached_timeout(&registry, local_id);
            }
        })?;
        // Clean up sender registry entry
        self.sender_registry.senders.remove(&self.sender_stream_id);
        Ok(self.handle)
    }
}

/// Hand a terminal sentinel to the frame channel without blocking a runtime worker.
///
/// [`flume::Sender::send`] blocks the calling **thread** while the channel is
/// full. Under the messenger mux that channel is a per-slot inlet drained by one
/// tokio task, so blocking a worker here starves the very task that would make
/// room: a runtime with `W` workers wedges on `W` concurrent terminal sends, and a
/// one-worker runtime on the first. A slot paused at its byte cap fills the inlet,
/// and so does a batcher parked on admission, so this is reachable whenever a
/// producer finalizes a stream whose consumer is behind — not an exotic state.
///
/// Dropping the record instead is not open to us: it is what tells the consumer
/// `Finalized` from `Dropped`, and losing it strands a reader on the heartbeat
/// watchdog. So a full channel escalates to a task that **awaits** the space
/// rather than a thread that waits for it. The task takes a clone of the sender,
/// which is what keeps the channel open until the sentinel is in it — otherwise
/// the receiver would see the inlet's EOF before the record that explains it.
///
/// `on_delivered` runs once the record is queued: inline on the fast path, on the
/// task otherwise. It carries work that must not become visible to anyone before
/// the sentinel is in the channel.
///
/// The task runs on the caller's runtime when the caller has one, and on the
/// runtime the sender was made on when it does not. A caller on a thread with no
/// runtime (a sender moved to a language binding's thread, say) would otherwise
/// have to block that thread, and under the mux a slot at its byte cap can keep
/// the inlet full for as long as its consumer does not read. The caller's
/// runtime goes first because the sender can outlive the runtime it was made on.
///
/// A `current_thread` runtime that nobody drives never runs the task, so the
/// terminal waits there with no end: the task's sender clone keeps the inlet
/// open, and the consumer sees neither `Finalized` nor `Dropped` until that
/// runtime runs. That is the price of never blocking the caller's thread.
///
/// The record is still lost when the runtime chosen is shutting down before or
/// during the wait, and for `detach` the attachment flag then stays set. The
/// sender clone dies with the task, so the inlet reaches EOF rather than
/// hanging, and the consumer sees `Dropped`.
/// [`tokio::task::block_in_place`] would avoid even that, at the price of
/// panicking on a `current_thread` runtime, which is a worse failure than the
/// one it fixes.
fn send_terminal(
    runtime: &tokio::runtime::Handle,
    tx: &flume::Sender<Vec<u8>>,
    bytes: Vec<u8>,
    on_delivered: impl FnOnce() + Send + 'static,
) -> Result<(), SendError> {
    let bytes = match tx.try_send(bytes) {
        Ok(()) => {
            on_delivered();
            return Ok(());
        }
        Err(flume::TrySendError::Disconnected(_)) => return Err(SendError::ChannelClosed),
        Err(flume::TrySendError::Full(bytes)) => bytes,
    };

    let runtime = tokio::runtime::Handle::try_current().unwrap_or_else(|_| runtime.clone());
    let tx = tx.clone();
    runtime.spawn(async move {
        if tx.send_async(bytes).await.is_ok() {
            on_delivered();
        }
    });
    Ok(())
}

/// Drop safety: hands `StreamFrame::Dropped` to the channel if no terminal was sent.
///
/// This `impl` block has no `T: Serialize` bound because sentinel serialization
/// uses `StreamFrame::<()>` — Rust forbids trait bounds on `Drop` impls.
impl<T> Drop for StreamSender<T> {
    fn drop(&mut self) {
        // Always clean up sender registry entry to prevent memory leak.
        // This is idempotent: if finalize/detach already removed it, this is a no-op.
        self.sender_registry.senders.remove(&self.sender_stream_id);
        if !self.sent_terminal {
            self.heartbeat_cancel.cancel();
            let bytes = cached_dropped().clone();
            // Never blocks — see `send_terminal`. A `Drop` that parks a runtime
            // worker is the one thing this path may not do. Errors ignored: the
            // channel may already be closed if the receiver was dropped first.
            let _ = send_terminal(&self.runtime, &self.tx, bytes, || {});
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use dashmap::DashMap;

    use crate::streaming::anchor::AnchorEntry;
    use crate::streaming::frame::{SendError, StreamFrame};
    use crate::streaming::handle::StreamAnchorHandle;

    use super::{StreamSender, StreamSenderCancelInfo};

    /// Every variant, encoded the way a sender encodes it, gets the terminal
    /// verdict the mux's credit classes depend on: exactly `Dropped`,
    /// `Detached`, `Finalized` and `TransportError` spend the terminal reserve.
    /// An `Item` whose payload is itself a sentinel's encoding stays data.
    #[test]
    fn terminal_classification_covers_every_variant() {
        let enc = |f: &StreamFrame<String>| rmp_serde::to_vec(f).unwrap();
        let cases = [
            (StreamFrame::Item("x".to_string()), false),
            (StreamFrame::Item("Finalized".to_string()), false),
            (StreamFrame::SenderError("e".to_string()), false),
            (StreamFrame::Heartbeat, false),
            (StreamFrame::Dropped, true),
            (StreamFrame::Detached, true),
            (StreamFrame::Finalized, true),
            (StreamFrame::TransportError("t".to_string()), true),
        ];
        for (frame, terminal) in cases {
            assert_eq!(
                super::is_terminal_sentinel(&enc(&frame)),
                terminal,
                "{frame:?}"
            );
        }
        let nested =
            rmp_serde::to_vec(&StreamFrame::Item(super::cached_finalized().clone())).unwrap();
        assert!(!super::is_terminal_sentinel(&nested));
    }

    /// The fast path rests on the encoding: an `Item` is a one-entry map keyed
    /// `"Item"`, whatever its payload.
    #[test]
    fn item_frames_start_with_the_item_prefix() {
        for bytes in [
            rmp_serde::to_vec(&StreamFrame::Item(())).unwrap(),
            rmp_serde::to_vec(&StreamFrame::Item(vec![7u8; 300])).unwrap(),
            rmp_serde::to_vec(&StreamFrame::Item(bytes::Bytes::from_static(b"data"))).unwrap(),
        ] {
            assert!(bytes.starts_with(super::ITEM_PREFIX), "{bytes:02x?}");
        }
    }

    /// Every data record on both ends of the mux is classified, so the check
    /// must not allocate. It used to decode each `Item` as `StreamFrame<()>`
    /// to rule out `TransportError`, and the failed decode formatted an error
    /// `String` per record.
    #[test]
    fn classifying_an_item_does_not_allocate() {
        let bytes = rmp_serde::to_vec(&StreamFrame::Item(vec![1u8; 160])).unwrap();
        // Warm the cached sentinels, which allocate once per process.
        super::is_terminal_sentinel(&bytes);
        let (terminal, allocations) =
            crate::test_alloc::allocations_in(|| super::is_terminal_sentinel(&bytes));
        assert!(!terminal);
        assert_eq!(allocations, 0);
    }

    /// Create an empty registry for use in unit tests (no real anchors needed).
    fn empty_registry() -> Arc<DashMap<u64, AnchorEntry>> {
        Arc::new(DashMap::new())
    }

    /// Create a test sender with a bounded(256) channel and a dummy handle.
    fn make_sender() -> (StreamSender<u32>, flume::Receiver<Vec<u8>>) {
        let (tx, rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let sender_registry =
            std::sync::Arc::new(crate::streaming::control::SenderRegistry::default());
        let sender = StreamSender::new(
            tx,
            handle,
            empty_registry(),
            StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id: 1,
                sender_registry,
                closed: Default::default(),
            },
            Duration::from_secs(5),
            None,
            None,
        );
        (sender, rx)
    }

    /// Create a sender entry and insert it into a registry. Returns (registry, sender_stream_id).
    fn make_sender_with_registry(
        tx: flume::Sender<Vec<u8>>,
        handle: StreamAnchorHandle,
        sender_stream_id: u64,
    ) -> (
        StreamSender<u32>,
        std::sync::Arc<crate::streaming::control::SenderRegistry>,
    ) {
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let sender_registry =
            std::sync::Arc::new(crate::streaming::control::SenderRegistry::default());

        // Insert the SenderEntry into the registry (simulating what attach_stream_anchor does)
        let closed = crate::streaming::control::SenderClosed::default();
        let entry = crate::streaming::control::SenderEntry {
            stop_token: cancel_token.child_token(),
            cancel_token: cancel_token.clone(),
            closed: closed.clone(),
        };
        sender_registry.senders.insert(sender_stream_id, entry);

        let sender = StreamSender::new(
            tx,
            handle,
            empty_registry(),
            StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id,
                sender_registry: sender_registry.clone(),
                closed,
            },
            Duration::from_secs(5),
            None,
            None,
        );
        (sender, sender_registry)
    }

    /// A terminal sent from a thread with no runtime does not block that
    /// thread on a full channel.
    ///
    /// Under the mux a slot at its byte cap stops pulling from its inlet, and
    /// the inlet stays full for as long as the consumer does not read. A sender
    /// moved to a plain thread, a language binding's thread for one, and dropped
    /// there must not hang that thread until the consumer reads. The terminal
    /// waits in a task on the runtime the sender was made on instead, and
    /// arrives, in order, once there is room.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_terminal_from_a_thread_without_a_runtime_does_not_block_it() {
        let (tx, rx) = flume::bounded::<Vec<u8>>(1);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let (sender, _registry) = make_sender_with_registry(tx.clone(), handle, 1);
        tx.send(b"filler".to_vec()).expect("fill the channel");

        let dropper = std::thread::spawn(move || drop(sender));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !dropper.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "dropping the sender blocked its thread on the full channel"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        dropper.join().expect("dropping thread panicked");

        assert_eq!(rx.recv_async().await.unwrap(), b"filler".to_vec());
        let bytes = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
            .await
            .expect("the terminal never arrived")
            .expect("channel closed before the terminal");
        assert!(matches!(decode::<u32>(&bytes), StreamFrame::Dropped));
    }

    /// A terminal sent after the sender's own runtime has ended still goes
    /// out, on the caller's live runtime.
    ///
    /// The sender can outlive the runtime it was made on, while the batcher or
    /// the consumer lives on another. There, a terminal that meets a full
    /// channel must wait on the runtime that is still running, or the
    /// consumer sees `Dropped` where it was owed `Finalized`. This pins that
    /// the caller's runtime goes first, ahead of the one the sender stores.
    #[test]
    fn a_terminal_after_the_senders_runtime_ends_goes_out_on_the_callers() {
        let (tx, rx) = flume::bounded::<Vec<u8>>(1);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let first = tokio::runtime::Runtime::new().unwrap();
        let (sender, _registry) =
            first.block_on(async { make_sender_with_registry(tx.clone(), handle, 1) });
        tx.send(b"filler".to_vec()).expect("fill the channel");
        drop(first);

        let second = tokio::runtime::Runtime::new().unwrap();
        second.block_on(async move {
            sender.finalize().expect("finalize");
            assert_eq!(rx.recv_async().await.unwrap(), b"filler".to_vec());
            let bytes = tokio::time::timeout(Duration::from_secs(5), rx.recv_async())
                .await
                .expect("the terminal never arrived")
                .expect("channel closed before the terminal");
            assert!(matches!(decode::<u32>(&bytes), StreamFrame::Finalized));
        });
    }

    /// Helper: deserialize raw bytes into StreamFrame<T>.
    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> StreamFrame<T> {
        rmp_serde::from_slice(bytes).expect("deserialize StreamFrame")
    }

    // -----------------------------------------------------------------------
    // Test 1: StreamSender::new() spawns a heartbeat task
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_heartbeat_emits() {
        tokio::time::pause();

        let (sender, rx) = make_sender();

        // Advance time past one heartbeat interval (5 seconds).
        // Use sleep rather than advance+yield so the interval task gets polled.
        tokio::time::sleep(Duration::from_secs(6)).await;

        // Should have received at least one heartbeat
        let bytes = rx.try_recv().expect("should receive heartbeat frame");
        let frame: StreamFrame<u32> = decode(&bytes);
        assert!(
            matches!(frame, StreamFrame::Heartbeat),
            "expected Heartbeat, got {:?}",
            frame
        );

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Test 2: send(item) serializes StreamFrame::Item(item) and sends
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_send_item() {
        let (sender, rx) = make_sender();

        sender.send(42u32).await.expect("send should succeed");

        let bytes = rx.recv_async().await.expect("should receive item");
        let frame: StreamFrame<u32> = decode(&bytes);
        match frame {
            StreamFrame::Item(val) => assert_eq!(val, 42),
            other => panic!("expected Item(42), got {:?}", other),
        }

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Test 3: send_err("msg") serializes StreamFrame::SenderError
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_send_err() {
        let (sender, rx) = make_sender();

        sender
            .send_err("something went wrong")
            .await
            .expect("send_err should succeed");

        let bytes = rx.recv_async().await.expect("should receive error frame");
        let frame: StreamFrame<u32> = decode(&bytes);
        match frame {
            StreamFrame::SenderError(msg) => assert_eq!(msg, "something went wrong"),
            other => panic!("expected SenderError, got {:?}", other),
        }

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Test 4: finalize(self) sends Finalized, cancels heartbeat, no Dropped
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_finalize() {
        let (sender, rx) = make_sender();

        sender.finalize().expect("finalize should succeed");

        let bytes = rx.recv_async().await.expect("should receive Finalized");
        let frame: StreamFrame<u32> = decode(&bytes);
        assert!(
            matches!(frame, StreamFrame::Finalized),
            "expected Finalized, got {:?}",
            frame
        );

        // No Dropped should follow — drain any heartbeats and check
        while let Ok(bytes) = rx.try_recv() {
            let frame: StreamFrame<u32> = decode(&bytes);
            assert!(
                !matches!(frame, StreamFrame::Dropped),
                "should NOT receive Dropped after finalize"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: detach(self) sends Detached, returns handle, no Dropped
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_detach() {
        let (sender, rx) = make_sender();
        let expected_handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);

        let returned_handle = sender.detach().expect("detach should succeed");
        assert_eq!(returned_handle, expected_handle);

        let bytes = rx.recv_async().await.expect("should receive Detached");
        let frame: StreamFrame<u32> = decode(&bytes);
        assert!(
            matches!(frame, StreamFrame::Detached),
            "expected Detached, got {:?}",
            frame
        );

        // No Dropped should follow
        while let Ok(bytes) = rx.try_recv() {
            let frame: StreamFrame<u32> = decode(&bytes);
            assert!(
                !matches!(frame, StreamFrame::Dropped),
                "should NOT receive Dropped after detach"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test 6: dropping StreamSender without terminal sends Dropped
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_sends_dropped() {
        let (sender, rx) = make_sender();

        // Drop without finalize or detach
        drop(sender);

        // Should receive Dropped
        let bytes = rx.recv_async().await.expect("should receive Dropped");
        let frame: StreamFrame<u32> = decode(&bytes);
        assert!(
            matches!(frame, StreamFrame::Dropped),
            "expected Dropped, got {:?}",
            frame
        );
    }

    // -----------------------------------------------------------------------
    // Test 7: heartbeat uses try_send (non-blocking) -- doesn't block on full channel
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_heartbeat_non_blocking_on_full_channel() {
        tokio::time::pause();

        // Create a channel with capacity 1
        let (tx, rx) = flume::bounded::<Vec<u8>>(1);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let sender_registry =
            std::sync::Arc::new(crate::streaming::control::SenderRegistry::default());
        let sender = StreamSender::new(
            tx,
            handle,
            empty_registry(),
            StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id: 1,
                sender_registry,
                closed: Default::default(),
            },
            Duration::from_secs(5),
            None,
            None,
        );

        // Put an item in the channel to fill it
        sender.send(99u32).await.expect("send should succeed");
        // Channel is now full (capacity=1)

        // Advance past heartbeat interval — heartbeat should try_send and not block.
        // Use sleep so the interval task gets polled under paused time.
        tokio::time::sleep(Duration::from_secs(6)).await;

        // The test passes if we get here without hanging — heartbeat didn't block
        // Verify channel still has the original item
        let bytes = rx.try_recv().expect("should have the sent item");
        let frame: StreamFrame<u32> = decode(&bytes);
        assert!(matches!(frame, StreamFrame::Item(99)));

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Test 8: send() on a closed channel returns SendError::ChannelClosed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_send_on_closed_channel() {
        let (tx, rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let sender_registry =
            std::sync::Arc::new(crate::streaming::control::SenderRegistry::default());
        let sender = StreamSender::new(
            tx,
            handle,
            empty_registry(),
            StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id: 1,
                sender_registry,
                closed: Default::default(),
            },
            Duration::from_secs(5),
            None,
            None,
        );

        // Drop the receiver to close the channel
        drop(rx);

        let result = sender.send(42u32).await;
        assert!(
            matches!(result, Err(SendError::ChannelClosed)),
            "expected ChannelClosed, got {:?}",
            result
        );

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Test 9: heartbeat task stops after cancel (CancellationToken)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_heartbeat_stops_after_cancel() {
        tokio::time::pause();

        let (sender, rx) = make_sender();

        // Finalize cancels the heartbeat
        sender.finalize().expect("finalize should succeed");

        // Drain any frames already sent
        while rx.try_recv().is_ok() {}

        // Advance time — no more heartbeats should arrive.
        // Use sleep so any pending tasks get polled under paused time.
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Channel should be empty (no heartbeats after cancel)
        assert!(
            rx.try_recv().is_err(),
            "should NOT receive any frames after heartbeat cancel"
        );
    }

    // -----------------------------------------------------------------------
    // Test 10: cancellation_token() returns a cloneable token
    // -----------------------------------------------------------------------

    #[test]
    fn test_cancellation_token() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (sender, _rx) = make_sender();
            let token = sender.cancellation_token();
            // Token should not be cancelled yet
            assert!(!token.is_cancelled(), "token should not start cancelled");
            // Cancelling the token externally should be reflected
            token.cancel();
            assert!(
                token.is_cancelled(),
                "token should be cancelled after cancel()"
            );
            // A clone should also be cancelled
            let clone = sender.cancellation_token();
            assert!(
                clone.is_cancelled(),
                "cloned token should reflect cancellation"
            );
            drop(sender);
        });
    }

    // -----------------------------------------------------------------------
    // Test 11: cancellation rejects sends while the receiver remains open
    // -----------------------------------------------------------------------

    /// A cancel through the registry is seen by the very next send: the
    /// sender checks a flag that the cancel sets first. `_stream_cancel` and
    /// shutdown cancel this way.
    #[tokio::test]
    async fn registry_cancel_fails_the_next_send_at_once() {
        let (tx, rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let (sender, registry) = make_sender_with_registry(tx, handle, 7);
        registry.cancel(7);
        assert!(matches!(
            sender.send(42).await,
            Err(SendError::ChannelClosed)
        ));
        assert!(matches!(
            sender.send_err("late error").await,
            Err(SendError::ChannelClosed)
        ));
        assert!(rx.is_empty());
    }

    /// The registry cancel needs no task: it sets the flag itself. So it
    /// still stops a sender whose building runtime is gone, when a direct
    /// token cancel no longer can (no heartbeat task is left to see it).
    #[test]
    fn registry_cancel_works_after_the_senders_runtime_is_gone() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (tx, _rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let (sender, registry) =
            runtime.block_on(async { make_sender_with_registry(tx, handle, 7) });
        drop(runtime);
        registry.cancel(7);
        assert!(matches!(
            futures::executor::block_on(sender.send(42)),
            Err(SendError::ChannelClosed)
        ));
    }

    /// A token cancelled directly, not through the registry, reaches the
    /// per-record flag from the heartbeat task, one scheduler hop later.
    /// Checking the token itself per record would take its mutex every time.
    #[tokio::test]
    async fn test_send_after_cancel() {
        let (sender, rx) = make_sender();
        sender.cancellation_token().cancel();
        // Bounded, not one hop: on a multi-thread runtime the heartbeat task
        // may run on another worker.
        for _ in 0..1000 {
            if sender.closed.is_closed() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            sender.send(42).await,
            Err(SendError::ChannelClosed)
        ));
        assert!(matches!(
            sender.send_err("late error").await,
            Err(SendError::ChannelClosed)
        ));
        assert!(rx.is_empty());
    }

    #[tokio::test]
    async fn cancellation_wakes_item_and_error_sends_on_a_full_channel() {
        let (sender, rx) = make_sender();
        for _ in 0..rx.capacity().unwrap() {
            sender.send(0).await.unwrap();
        }
        let item = sender.send(42);
        let error = sender.send_err("late error");
        tokio::pin!(item, error);
        assert!(futures::poll!(&mut item).is_pending());
        assert!(futures::poll!(&mut error).is_pending());

        sender.cancellation_token().cancel();
        let (item, error) =
            tokio::time::timeout(Duration::from_secs(1), async { (item.await, error.await) })
                .await
                .expect("blocked sends did not wake after cancellation");
        assert!(matches!(item, Err(SendError::ChannelClosed)));
        assert!(matches!(error, Err(SendError::ChannelClosed)));
        assert!(rx.is_full(), "the receiver stayed open and did not drain");
    }

    // -----------------------------------------------------------------------
    // Test 12: SenderRegistry entry removed after finalize()
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_registry_cleanup_on_finalize() {
        let (tx, _rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let (sender, registry) = make_sender_with_registry(tx, handle, 1);

        // Entry should be present before finalize
        assert!(
            registry.senders.contains_key(&1),
            "entry should be present before finalize"
        );

        sender.finalize().expect("finalize should succeed");

        // Entry should be removed after finalize
        assert!(
            !registry.senders.contains_key(&1),
            "entry should be removed after finalize"
        );
    }

    // -----------------------------------------------------------------------
    // Test 13: SenderRegistry entry removed after detach()
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_registry_cleanup_on_detach() {
        let (tx, _rx) = flume::bounded::<Vec<u8>>(256);
        let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
        let (sender, registry) = make_sender_with_registry(tx, handle, 1);

        // Entry should be present before detach
        assert!(
            registry.senders.contains_key(&1),
            "entry should be present before detach"
        );

        sender.detach().expect("detach should succeed");

        // Entry should be removed after detach
        assert!(
            !registry.senders.contains_key(&1),
            "entry should be removed after detach"
        );
    }

    // -----------------------------------------------------------------------
    // Test 14: SenderRegistry entry removed after drop
    // -----------------------------------------------------------------------

    #[test]
    fn test_registry_cleanup_on_drop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, _rx) = flume::bounded::<Vec<u8>>(256);
            let handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 1);
            let (sender, registry) = make_sender_with_registry(tx, handle, 1);

            // Entry should be present before drop
            assert!(
                registry.senders.contains_key(&1),
                "entry should be present before drop"
            );

            // Drop without finalize or detach
            drop(sender);

            // Entry should be removed after drop
            assert!(
                !registry.senders.contains_key(&1),
                "entry should be removed after drop"
            );
        });
    }
}
