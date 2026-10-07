// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Anchor registry layer: [`AnchorManager`], `AnchorEntry`, [`StreamAnchor`], and [`AttachError`].
//!
//! The anchor registry is the core coordination point for the streaming protocol.
//! Each anchor represents a single exclusive-attachment stream slot:
//!
//! - [`AnchorManager::create_anchor`] allocates a registry slot and returns a
//!   [`StreamAnchor<T>`] that embeds the [`crate::streaming::handle::StreamAnchorHandle`]
//!   (obtainable via [`.handle()`](StreamAnchor::handle)) for the consumer.
//! - Exactly one [`flume::Sender`] may be attached at a time;
//!   the attach check is performed atomically via [`dashmap::DashMap::entry`].
//! - Each entry holds a [`tokio_util::sync::CancellationToken`] created at anchor
//!   creation so that whichever cleanup path fires first cancels the token; subsequent
//!   cancellations are no-ops.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::observability::{HandlerOutcome, StreamingOp, VeloMetrics};
use dashmap::DashMap;
use derive_builder::Builder;
use futures::Stream;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::streaming::frame::{StreamError, StreamFrame};
use crate::streaming::handle::StreamAnchorHandle;

/// Grouped handles needed by anchor constructors and background tasks to
/// keep both registries and the metrics collector in a single parameter.
/// Cheap to clone (all `Arc`s).
#[derive(Clone)]
pub(crate) struct AnchorContext {
    pub registry: Arc<DashMap<u64, AnchorEntry>>,
    pub mpsc_registry: Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,
    pub metrics: Option<Arc<VeloMetrics>>,
}

// ---------------------------------------------------------------------------
// AttachError
// ---------------------------------------------------------------------------

/// Errors that can occur when attempting to attach a sender to an anchor.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AttachError {
    /// The requested anchor handle was not found in the registry.
    #[error("anchor {handle} not found in registry")]
    AnchorNotFound { handle: StreamAnchorHandle },

    /// Another sender is already attached to this anchor.
    #[error("anchor {handle} is already attached")]
    AlreadyAttached { handle: StreamAnchorHandle },

    /// The MPSC anchor has reached its configured `max_senders` cap.
    #[error("anchor {handle} reached max_senders limit of {limit}")]
    MaxSendersReached {
        handle: StreamAnchorHandle,
        limit: usize,
    },

    /// The handle was produced for a different anchor kind than the attach
    /// method expected (e.g. an MPSC handle passed to `attach_stream_anchor`,
    /// or an SPSC handle passed to `attach_mpsc_stream_anchor`).
    ///
    /// Detected client-side from [`crate::streaming::handle::StreamAnchorHandle::kind`]
    /// so no AM round-trip is wasted.
    #[error("anchor {handle} is of wrong kind: expected {expected}")]
    WrongHandleKind {
        handle: StreamAnchorHandle,
        expected: crate::streaming::handle::AnchorKind,
    },

    /// The underlying transport failed during bind/connect.
    #[error("transport bind failed: {0}")]
    TransportError(#[from] anyhow::Error),
}

// ---------------------------------------------------------------------------
// AnchorConfig
// ---------------------------------------------------------------------------

/// Per-anchor overrides for the two liveness knobs.
///
/// Both fields are `Option`: `None` means "inherit the manager-level default"
/// (`AnchorManager::default_unattached_timeout` and
/// `AnchorManager::default_heartbeat_interval`); `Some(d)` overrides it for the
/// single anchor created via [`AnchorManager::create_anchor_with_config`].
///
/// `AnchorConfig::default()` inherits everything, making it equivalent to the
/// zero-arg [`AnchorManager::create_anchor`] path.
#[derive(Debug, Clone, Default)]
pub struct AnchorConfig {
    /// How long an unattached anchor may live before being auto-removed.
    /// `Some(None)` is not expressible — pass `None` to inherit the manager
    /// default (which itself may be `None` to disable the timeout entirely).
    pub unattached_timeout: Option<Duration>,

    /// The heartbeat cadence the attached sender must emit at, and the
    /// per-window deadline the consumer side (reader pump or mux stream
    /// watchdog) applies. Total tolerance before `Dropped` injection is about
    /// `crate::streaming::control::DETECTION_MULTIPLIER` times this value.
    pub heartbeat_interval: Option<Duration>,
}

// ---------------------------------------------------------------------------
// AnchorEntry
// ---------------------------------------------------------------------------

/// A single slot in the anchor registry.
///
/// Non-generic by design: [`AnchorManager`] stores `DashMap<u64, AnchorEntry>`
/// which avoids propagating a type parameter throughout the registry.
///
/// The `attachment` flag indicates whether a sender is currently attached.
/// The check-and-set is performed atomically via [`dashmap::mapref::entry::Entry`]
/// to prevent TOCTOU races. A reader pump takes ownership of a per-stream
/// transport's receiver rather than storing it in the entry; a mux bind's
/// receiver sits in `feed`, where the consumer reads it.
// Fields are consumed by the control handlers, the pump and the watchdog.
#[allow(dead_code)]
pub(crate) struct AnchorEntry {
    /// The mux slot buffer this anchor's consumer reads directly, when a mux
    /// bind is feeding it. Shared with the [`StreamAnchor<T>`]; see
    /// [`FeedCell`](crate::streaming::control::FeedCell).
    pub feed: Arc<crate::streaming::control::FeedCell>,

    /// Raw-bytes frame delivery channel to the [`StreamAnchor<T>`] consumer.
    ///
    /// Non-generic so `DashMap<u64, AnchorEntry>` requires no type parameters.
    pub frame_tx: flume::Sender<Vec<u8>>,

    /// Anchor-lifetime parent token. Created at anchor creation; cancelled only
    /// by finalize/remove/cancel. Child tokens are derived for transient tasks
    /// (reader pump or stream watchdog, timeout) so that stopping a child never
    /// cancels the parent.
    pub cancel_token: CancellationToken,

    /// Child token for the currently active reader pump or, for a mux bind,
    /// stream watchdog (`None` when no sender is attached). Created via
    /// `cancel_token.child_token()` on each attach or pre-bind. Cancelling it
    /// stops that task without affecting the parent, but for a mux bind it no
    /// longer cuts the data path: use [`AnchorEntry::retire_pump`], which also
    /// withdraws `feed`.
    pub active_pump_token: Option<CancellationToken>,

    /// `true` iff a sender is currently attached. The transport receiver is
    /// held by the reader pump, or by `feed` for a mux bind, not here.
    pub attachment: bool,

    /// Cancels the inactivity timeout task when a sender attaches.
    /// `None` if no timeout is configured for this anchor.
    pub timeout_cancel: Option<CancellationToken>,

    /// The configured unattached timeout for this anchor. Stored so that
    /// `detach` can respawn the timeout task with the same duration.
    /// `None` means the anchor never auto-removes while unattached.
    pub unattached_timeout: Option<Duration>,

    /// The negotiated heartbeat cadence for this anchor. The reader pump or
    /// stream watchdog uses this as its per-window deadline; the producer's
    /// `StreamSender` uses it as its emit interval. Resolved at create-time from per-anchor config or
    /// the manager-level default and echoed to the sender via
    /// [`crate::streaming::control::AnchorAttachResponse::Ok::heartbeat_interval_ms`].
    pub heartbeat_interval: Duration,

    /// Populated on successful attach from [`crate::streaming::control::AnchorAttachRequest::stream_cancel_handle`].
    /// Encodes the sender's WorkerId + stream ID so the anchor can route `_stream_cancel`
    /// active messages to the correct sender worker when the consumer cancels upstream.
    /// `None` until a sender attaches.
    pub stream_cancel_handle: Option<crate::streaming::control::StreamCancelHandle>,

    /// The mux slot bound and fed to this anchor ahead of any sender, when
    /// [`AnchorManager::prebind_anchor`] minted a ticket for it. `None` on the
    /// ordinary attach path, which is every anchor whose application sends no
    /// ticket.
    pub prebind: Option<PreBind>,
    pub stop_requested: bool,
}

// ---------------------------------------------------------------------------
// PreBind
// ---------------------------------------------------------------------------

/// A mux slot bound and fed to an anchor before any sender asked for one.
///
/// Zero-RTT stream setup does at request registration what the attach handler
/// does on the round trip: allocate the routing session, bind the slot, take
/// the drain signal, and start the direct feed and its watchdog. What is left
/// over is this — the terms that were minted, the claim token that says whether
/// anyone took them up, and the means to give the slot back.
///
/// [`Drop`] is the whole reclamation story, and deliberately not a new call
/// site: every path that kills an anchor already removes its registry entry, so
/// every one of them drops this. What it does depends on whether an `OpenSlot`
/// has claimed the bind:
///
/// - **Unclaimed** — release the bind. The 60 s accept window would collect it
///   eventually and stays as the backstop, but a request that dies before its
///   first token knows a minute sooner than that timer does.
/// - **Claimed** — tell the peer to abandon its egress slot. Without this a
///   producer that is not sending never learns its consumer is gone: the
///   ingress fault carrying that news rides on the next record to arrive, and
///   an idle producer sends none. Zero-RTT has no `_anchor_attach`, so it never
///   learns a `StreamCancelHandle` either, and this is the only prompt path
///   left.
///
/// Claim and lifecycle changes are serialized by `DrainSignal`. An early stop
/// or cancel is therefore applied when OpenSlot claims the bind. Adoption still
/// checks the claim before it transfers ownership; a competing open is refused.
pub(crate) struct PreBind {
    /// The anchor this slot was bound for. Half of the bind's key; the other
    /// half is `ticket.routing_session_id`.
    anchor_id: u64,
    ticket: crate::streaming::control::StreamOpenTicket,
    drain: Arc<crate::streaming::messenger_mux::ingress::DrainSignal>,
    /// `Weak` because a strong handle inside a registry entry would keep the
    /// transport, its batchers and its ingress state alive for as long as
    /// anything holds the anchor registry.
    ///
    /// It is also how [`PreBind::adopt`] defuses `Drop` — see there.
    mux: std::sync::Weak<crate::streaming::messenger_mux::MessengerMuxTransport>,
    /// Shared with the [`WatchdogContext`](crate::streaming::control::WatchdogContext)
    /// of this bind's stream watchdog. `true` until [`PreBind::adopt`] clears
    /// it — the one transition from "no sender yet" to "a sender exists"
    /// that a sender opening on its ticket the ordinary way (an `OpenSlot`
    /// claiming `drain` directly) never needs a writer for, because
    /// `drain.claimed()` already answers it.
    prebound: Arc<AtomicBool>,
}

impl PreBind {
    /// Whether an `OpenSlot` has claimed this bind.
    fn is_claimed(&self) -> bool {
        self.drain.claimed().is_some()
    }

    /// The terms this slot was minted on.
    fn ticket(&self) -> &crate::streaming::control::StreamOpenTicket {
        &self.ticket
    }

    /// Hand the slot to a sender that asked for it the long way round, and stop
    /// owning it.
    ///
    /// Clearing the transport handle is what defuses [`Drop`]: from here the
    /// slot is an ordinary attached stream, reclaimed by the paths that reclaim
    /// those -- the anchor entry's own `Drop` closes the slot on removal, and
    /// the attach carried a `StreamCancelHandle`, so the anchor can reach its
    /// producer directly as well.
    ///
    /// The ticket is cloned rather than moved out because this type has a
    /// `Drop`; the clone is one `Arc<str>` bump on a path that runs once per
    /// adopted stream.
    fn adopt(mut self) -> crate::streaming::control::StreamOpenTicket {
        self.mux = std::sync::Weak::new();
        // This pre-bind's watchdog still reads `prebound` as `true` -- it
        // has no other way to learn a sender just showed up by this door
        // rather than by its own `OpenSlot`, which may still be seconds or
        // minutes away. Clearing it here, in the same place that already
        // defuses `Drop`, is what stops that watchdog from exempting a
        // now-live stream from heartbeat detection. That is the only
        // exemption it lifts: the unclaimed-bind reap gates on the claim
        // alone (`bind_unclaimed`, never `prebound`), so an adopted attach
        // whose `OpenSlot` never arrives is still reaped by the bind's accept
        // window, exactly as a pre-bind nobody adopted is -- see
        // `control::reap_unclaimed`.
        self.prebound.store(false, Ordering::Relaxed);
        self.ticket.clone()
    }
}

impl Drop for AnchorEntry {
    /// A removed entry closes its mux slot, tells the sender, and lets go of
    /// its feed.
    ///
    /// Every way a stream leaves the registry passes through here: the
    /// consumer's own terminal, a watchdog firing, a cancel through the
    /// controller or the anchor's drop, the accept window. Waiting for a
    /// delivery to find no receiver is not enough, because a sender that is
    /// dead, or parked on its window, sends nothing to fail on; the slot and its
    /// peer batcher would stay live. The close is idempotent and checks the
    /// slot's generation and session, and a stream that ended on its sender's
    /// terminal has no slot left to close.
    fn drop(&mut self) {
        // Withdrawn first, so a consumer still polling moves off the slot
        // buffer before the close can inject `Dropped` into it and end the
        // stream with an error instead of cleanly.
        if let Some(feed) = self.feed.withdraw() {
            feed.release_slot();
        }
    }
}

impl AnchorEntry {
    pub(crate) fn restart_unattached_timeout(
        &mut self,
        registry: &Arc<DashMap<u64, AnchorEntry>>,
        local_id: u64,
    ) {
        if let Some(previous) = self.timeout_cancel.take() {
            previous.cancel();
        }
        if !self.attachment
            && self.prebind.is_none()
            && let Some(duration) = self.unattached_timeout
        {
            self.timeout_cancel = Some(AnchorManager::spawn_timeout_task(
                Arc::clone(registry),
                local_id,
                duration,
                &self.cancel_token,
            ));
        }
    }

    /// Stop the pump or watchdog serving this anchor, and withdraw the direct
    /// feed with it.
    ///
    /// For a per-stream transport the token is enough, because the pump is the
    /// only route from a bind to the consumer. A mux bind's consumer reads the
    /// slot buffer itself, so the feed has to come out as well: a bind retired
    /// here can still be claimed and delivered into by a racing `OpenSlot`,
    /// and the consumer must not read that as its own stream. The token is
    /// returned for the caller that still needs it.
    pub(crate) fn retire_pump(&mut self) -> Option<CancellationToken> {
        self.feed.withdraw();
        let token = self.active_pump_token.take();
        if let Some(ref token) = token {
            token.cancel();
        }
        token
    }

    /// Retire the pump if `feed` is still the installed one: a mux stream's
    /// own `Detached` ended it. The watchdog stops with the feed, so it
    /// cannot fire later on the entry a re-attach reuses. A newer feed is not
    /// this stream's to stop.
    pub(crate) fn retire_ended_feed(
        &mut self,
        feed: &Arc<crate::streaming::control::DirectFeed>,
    ) -> bool {
        if self
            .feed
            .current()
            .is_some_and(|current| Arc::ptr_eq(&current, feed))
        {
            self.retire_pump();
            true
        } else {
            false
        }
    }

    /// Whether a pre-bound slot on this anchor already has a sender.
    ///
    /// `attachment` does not answer this. Nothing on the zero-RTT path sets it
    /// — there is no attach to set it — so a stream running through a claimed
    /// pre-bind leaves it `false` for the stream's whole life. Any guard that
    /// means *this anchor already has a sender* has to ask both, which is why
    /// [`AnchorManager::adopt_prebind`] refuses a claimed pre-bind rather than
    /// treating an unattached anchor as a free one.
    fn prebind_is_claimed(&self) -> bool {
        self.prebind.as_ref().is_some_and(PreBind::is_claimed)
    }
}

impl Drop for PreBind {
    fn drop(&mut self) {
        crate::streaming::control::SlotRelease {
            mux: self.mux.clone(),
            anchor_id: self.anchor_id,
            session_id: self.ticket.routing_session_id,
        }
        .release(&self.drain);
    }
}

/// The sender-side identity one stream is opened under.
///
/// Registered before publishing the identity to an anchor. The guard removes
/// it if attach fails or its future is dropped; a constructed sender takes
/// responsibility for the entry.
struct SenderIdentity {
    sender_stream_id: u64,
    /// A clone of the registered entry: the same tokens and flag.
    entry: crate::streaming::control::SenderEntry,
    registry: Arc<crate::streaming::control::SenderRegistry>,
    armed: bool,
}

impl Drop for SenderIdentity {
    fn drop(&mut self) {
        if self.armed {
            self.registry.senders.remove(&self.sender_stream_id);
        }
    }
}

/// What an incoming `_anchor_attach` may do with a pre-bound slot.
pub(crate) enum PrebindAdoption {
    /// No pre-bound slot on this anchor; the ordinary bind path applies.
    None,
    /// The sender may have the slot already waiting for it, on these terms.
    Adopted(crate::streaming::control::StreamOpenTicket),
    /// A pre-bound slot exists but this sender cannot take it.
    Refused(String),
}

// ---------------------------------------------------------------------------
// StreamController
// ---------------------------------------------------------------------------

/// Shared inner state between [`StreamAnchor`] and [`StreamController`].
///
/// Wrapped in `Arc` so `StreamController` can outlive `StreamAnchor` being
/// moved into StreamExt combinators.
struct StreamControllerInner {
    worker_id: velo_ext::WorkerId,
    local_id: u64,
    registry: Arc<DashMap<u64, AnchorEntry>>,
    metrics: Option<Arc<VeloMetrics>>,
    /// Sender-side registry: used to directly cancel the [`crate::streaming::control::SenderEntry`]
    /// when the anchor is cancelled (same-worker path without AM round-trip).
    sender_registry: Arc<crate::streaming::control::SenderRegistry>,
    /// Optional messenger for sending `_stream_cancel` AM to the sender's worker.
    /// `None` for local-only (MockFrameTransport) scenarios.
    messenger: Option<Arc<crate::messenger::Messenger>>,
    /// AtomicBool gate: compare_exchange(false, true) to ensure AM is sent at most once.
    cancelled: AtomicBool,
}

/// Cloneable handle to cancel a [`StreamAnchor`] from outside the stream.
///
/// Obtain via [`StreamAnchor::controller`]. Required for the StreamExt combinator
/// use-case where the `StreamAnchor` is moved into `.map()` / `.take_while()` etc.
/// and the caller loses direct access to it.
#[derive(Clone)]
pub struct StreamController {
    inner: Arc<StreamControllerInner>,
}

impl StreamController {
    /// Request graceful stop without closing the response stream.
    /// An early request is retained until a producer attaches or opens its ticket.
    pub fn request_stop(&self) {
        let Some(mut entry) = self.inner.registry.get_mut(&self.inner.local_id) else {
            return;
        };
        if entry.stop_requested {
            return;
        }
        entry.stop_requested = true;
        if let Some(prebind) = &entry.prebind {
            if let Some((key, slot)) = prebind.drain.request_stop()
                && let Some(mux) = prebind.mux.upgrade()
            {
                mux.request_stop(key, slot, prebind.ticket.routing_session_id);
            }
        } else if let Some(handle) = entry.stream_cancel_handle {
            crate::streaming::control::request_sender_stop(
                handle,
                self.inner.worker_id,
                &self.inner.sender_registry,
                self.inner.messenger.as_ref(),
            );
        }
    }

    /// Cancel the stream: remove the anchor from the registry and send a
    /// `_stream_cancel` AM to the sender's worker (fire-and-forget).
    ///
    /// Idempotent: the AM is sent at most once regardless of how many clones
    /// call `cancel()` concurrently.
    pub fn cancel(&self) {
        // AtomicBool gate: only the first caller proceeds
        if self
            .inner
            .cancelled
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return; // already cancelled
        }

        let started = Instant::now();

        // Remove anchor from registry and extract stream_cancel_handle
        let stream_cancel_handle =
            self.inner
                .registry
                .remove(&self.inner.local_id)
                .and_then(|(_, entry)| {
                    entry.cancel_token.cancel();
                    entry.stream_cancel_handle
                });
        if let Some(metrics) = self.inner.metrics.as_ref() {
            metrics.record_streaming_operation(
                StreamingOp::Cancel,
                HandlerOutcome::Success,
                "velo",
                started.elapsed(),
            );
        }

        if let Some(handle) = stream_cancel_handle {
            crate::streaming::control::request_sender_cancel(
                handle,
                self.inner.worker_id,
                &self.inner.sender_registry,
                self.inner.messenger.as_ref(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// StreamAnchor<T>
// ---------------------------------------------------------------------------

/// Consumer-side receive stream for an anchor.
///
/// Implements [`futures::Stream`] yielding `Result<StreamFrame<T>, StreamError>`.
/// Heartbeat frames are filtered out and never exposed to the consumer.
/// `Finalized`, `Dropped`, and `TransportError` end the stream.
/// `Detached` ends one attachment; the consumer can read a later attachment.
///
/// Use [`StreamExt::next()`](futures::StreamExt::next) for async iteration.
///
/// # Example
///
/// ```rust,no_run
/// use futures::StreamExt;
/// use velo::streaming::{AnchorManager, StreamFrame};
///
/// # async fn example(mgr: &AnchorManager) -> anyhow::Result<()> {
/// // Consumer creates an anchor
/// let mut anchor = mgr.create_anchor::<String>();
/// let handle = anchor.handle();
///
/// // Producer attaches (could be on a different worker)
/// let sender = mgr.attach_stream_anchor::<String>(handle).await?;
///
/// // Send items
/// sender.send("hello".into()).await?;
/// sender.send("world".into()).await?;
/// sender.finalize()?;
///
/// // Consume the stream
/// while let Some(frame) = anchor.next().await {
///     match frame {
///         Ok(StreamFrame::Item(s)) => println!("{s}"),
///         Ok(StreamFrame::Finalized) => break,
///         Err(e) => eprintln!("stream error: {e}"),
///         _ => {}
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// For upstream cancellation, see [`StreamController`].
pub struct StreamAnchor<T> {
    /// The anchor handle — pass to a sender for attachment via
    /// [`AnchorManager::attach_stream_anchor`].
    handle: StreamAnchorHandle,
    /// Async stream obtained from consuming the flume::Receiver via `into_stream()`.
    inner_stream: flume::r#async::RecvStream<'static, Vec<u8>>,
    /// Where a mux bind installs its slot buffer for this consumer to read.
    feed_cell: Arc<crate::streaming::control::FeedCell>,
    /// The generation of `feed_cell` that `feed` was taken from.
    feed_generation: u64,
    /// The mux slot buffer being read, polled ahead of `inner_stream`. `None`
    /// before a mux bind and after its buffer closes.
    feed: Option<(
        Arc<crate::streaming::control::DirectFeed>,
        flume::r#async::RecvStream<'static, Vec<u8>>,
    )>,
    /// Whether the last frame `poll_frame` returned came from `feed` rather
    /// than the anchor channel.
    from_feed: bool,
    /// Read the anchor channel before a newly installed feed. Anything already
    /// there was sent before the new sender attached -- a co-located sender's
    /// tail and its `Detached` -- and must not be overtaken.
    channel_first: bool,
    /// Set to true after a terminal sentinel; prevents further polling.
    terminated: bool,
    /// The local ID of the anchor in the registry (for cancel).
    local_id: u64,
    /// Arc clone of the AnchorManager's registry (for cancel).
    registry: Arc<DashMap<u64, AnchorEntry>>,
    /// Shared cancel handle — also held by any [`StreamController`] clones.
    controller: StreamController,
    metrics: Option<Arc<VeloMetrics>>,
    /// Runs after a frame is read and before it is handled, so a test can land
    /// work in that gap on the same thread.
    #[cfg(test)]
    pub(crate) after_frame_hook: Option<Box<dyn FnMut() + Send>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> StreamAnchor<T> {
    pub(crate) fn new(
        handle: StreamAnchorHandle,
        rx: flume::Receiver<Vec<u8>>,
        feed_cell: Arc<crate::streaming::control::FeedCell>,
        local_id: u64,
        ctx: AnchorContext,
        sender_registry: Arc<crate::streaming::control::SenderRegistry>,
        messenger: Option<Arc<crate::messenger::Messenger>>,
    ) -> Self {
        let AnchorContext {
            registry,
            mpsc_registry: _,
            metrics,
        } = ctx;
        let inner = Arc::new(StreamControllerInner {
            worker_id: handle.unpack().0,
            local_id,
            registry: registry.clone(),
            metrics: metrics.clone(),
            sender_registry,
            messenger,
            cancelled: AtomicBool::new(false),
        });
        let controller = StreamController { inner };
        Self {
            handle,
            inner_stream: rx.into_stream(),
            feed_cell,
            feed_generation: 0,
            feed: None,
            from_feed: false,
            channel_first: false,
            terminated: false,
            local_id,
            registry,
            controller,
            metrics,
            #[cfg(test)]
            after_frame_hook: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Return the anchor handle. Pass to a sender (possibly on another worker)
    /// for attachment via [`AnchorManager::attach_stream_anchor`].
    pub fn handle(&self) -> StreamAnchorHandle {
        self.handle
    }

    /// Return a cloneable [`StreamController`] that can cancel this anchor
    /// even after `self` is moved into a StreamExt combinator.
    pub fn controller(&self) -> StreamController {
        self.controller.clone()
    }

    /// The next raw frame: the mux slot buffer first, then the anchor channel.
    ///
    /// Data reaches a mux-fed anchor only through the slot buffer, which the
    /// mux alone writes; the anchor channel carries sentinels injected by the
    /// runtime (cancel, reap, watchdog) and the heartbeat that announces a new
    /// feed. While a feed is installed the buffer is read first, so nothing on
    /// the anchor channel jumps ahead of a record the consumer can still read.
    /// A feed that is withdrawn (retire, entry removal) is dropped on the next
    /// poll, before its buffer is read again: records still unread there are
    /// discarded, not delivered. Every record taken from the buffer is counted
    /// on its drain signal, because that count is the credit the sender gets
    /// back.
    fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Option<Vec<u8>>> {
        loop {
            let generation = self.feed_cell.generation();
            if generation != self.feed_generation {
                let (generation, feed) = self.feed_cell.snapshot();
                self.feed_generation = generation;
                self.channel_first = feed.is_some();
                self.feed = feed.map(|feed| {
                    let rx = feed.rx.clone().into_stream();
                    (feed, rx)
                });
            }
            if self.channel_first {
                match Pin::new(&mut self.inner_stream).poll_next(cx) {
                    Poll::Pending => self.channel_first = false,
                    ready => {
                        self.from_feed = false;
                        return ready;
                    }
                }
            }
            if let Some((feed, rx)) = self.feed.as_mut() {
                match Pin::new(rx).poll_next(cx) {
                    Poll::Ready(Some(bytes)) => {
                        // Withdrawn while this poll was reading it: the record
                        // belongs to a stream this consumer has left -- a
                        // `Dropped` a racing cancel's close injected, say --
                        // so it is discarded like the rest of that buffer.
                        if self.feed_cell.generation() != self.feed_generation {
                            continue;
                        }
                        feed.drain.drained();
                        self.from_feed = true;
                        return Poll::Ready(Some(bytes));
                    }
                    Poll::Ready(None) => {
                        // The stream ended, or its bind was released or
                        // reaped; anything left to say is on the anchor
                        // channel.
                        let (feed, _) = self.feed.take().expect("matched Some above");
                        crate::streaming::control::reap_unclaimed(
                            &feed,
                            self.local_id,
                            &self.registry,
                            self.metrics.as_deref(),
                        );
                        continue;
                    }
                    Poll::Pending => {}
                }
            }
            self.from_feed = false;
            return Pin::new(&mut self.inner_stream).poll_next(cx);
        }
    }

    /// End the stream on this side: let go of the slot buffer this consumer
    /// reads, close its slot, and remove the anchor's registry entry.
    ///
    /// Every terminal frame owes the registry this, and [`Drop`] cannot be the
    /// one to pay it: it short-circuits on `terminated`, which a terminal frame
    /// has just set. An entry left behind costs the frame channel, a permanent
    /// reading in `velo_streaming_active_anchors`, and the pump or watchdog
    /// serving it, with nothing telling either to stop.
    ///
    /// Called from `poll_next` on every terminal arm, including
    /// `TransportError` and a frame that fails to deserialize. Removing the
    /// entry closes the mux slot and tells the sender (`AnchorEntry`'s
    /// `Drop`); the receiver clones this consumer holds are dropped here, or
    /// they would outlive the stream while the application holds the
    /// anchor.
    fn retire(&mut self) {
        // The entry's drop closes the slot; the receiver clones this consumer
        // took for itself go here.
        self.feed = None;
        if let Some((_, entry)) = self.registry.remove(&self.local_id) {
            entry.cancel_token.cancel();
        }
    }

    /// Consume the stream and cancel the anchor.
    ///
    /// Removes the anchor from the registry and sends `_stream_cancel` AM to
    /// the sender's worker if a sender is attached. Same effect as
    /// [`StreamController::cancel`] but consumes `self` to signal intent.
    pub fn cancel(mut self) -> StreamController {
        self.terminated = true; // prevent Drop from re-cancelling
        self.controller.cancel();
        self.controller.clone()
    }

    /// Configure or override the inactivity timeout for this anchor.
    ///
    /// - `Some(duration)`: anchor will be auto-removed if no sender attaches
    ///   within `duration`. If the anchor is currently unattached, a new timeout
    ///   task is spawned immediately (replacing any existing one).
    /// - `None`: disable timeout for this anchor. Any running timeout task is
    ///   cancelled.
    ///
    /// If the anchor is currently attached, the new duration is stored and will
    /// take effect on the next detach (no immediate spawn since the timer is
    /// paused while attached).
    pub fn set_timeout(&self, timeout: Option<Duration>) {
        if let Some(mut entry) = self.registry.get_mut(&self.local_id) {
            entry.unattached_timeout = timeout;
            entry.restart_unattached_timeout(&self.registry, self.local_id);
        }
    }
}

// SAFETY: StreamAnchor does not use structural pinning. Its `inner_stream`
// (flume::r#async::RecvStream) is Unpin, and all other fields are trivially Unpin.
// PhantomData<T> should not prevent Unpin, but we assert it explicitly.
impl<T> Unpin for StreamAnchor<T> {}

impl<T> Drop for StreamAnchor<T> {
    fn drop(&mut self) {
        if !self.terminated {
            // Delegate to the shared controller — AtomicBool prevents double-cancel.
            self.controller.cancel();
        }
    }
}

impl<T: DeserializeOwned> Stream for StreamAnchor<T> {
    type Item = Result<StreamFrame<T>, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        loop {
            match this.poll_frame(cx) {
                Poll::Ready(Some(bytes)) => {
                    #[cfg(test)]
                    if let Some(hook) = this.after_frame_hook.as_mut() {
                        hook();
                    }
                    match rmp_serde::from_slice::<StreamFrame<T>>(&bytes) {
                        Ok(StreamFrame::Heartbeat) => continue, // filter heartbeats
                        Ok(StreamFrame::Item(data)) => {
                            return Poll::Ready(Some(Ok(StreamFrame::Item(data))));
                        }
                        Ok(StreamFrame::SenderError(msg)) => {
                            // Soft error -- stream continues (not terminated)
                            return Poll::Ready(Some(Err(StreamError::SenderError(msg))));
                        }
                        Ok(StreamFrame::Finalized) => {
                            this.terminated = true;
                            // A terminal read off the slot buffer was applied
                            // in the same step that retires the slot, so there
                            // is nothing left to close. Saying so now saves the
                            // close its lock trip when the consumer gets here
                            // before that step's release lands.
                            if this.from_feed
                                && let Some((feed, _)) = &this.feed
                            {
                                feed.drain.mark_released();
                            }
                            // Anchor is permanently closed.
                            this.retire();
                            return Poll::Ready(Some(Ok(StreamFrame::Finalized)));
                        }
                        Ok(StreamFrame::Detached) => {
                            // Channel writers clear their own attachment after
                            // enqueueing Detached. Reading that old sentinel
                            // must not change a later attachment. A direct feed
                            // is retired here only while it is still current.
                            let from_feed = this.from_feed;
                            let ended_feed = this
                                .feed
                                .as_ref()
                                .filter(|_| from_feed)
                                .map(|(feed, _)| Arc::clone(feed));
                            let released_prebind = ended_feed.and_then(|feed| {
                                this.registry.get_mut(&this.local_id).and_then(|mut entry| {
                                    if !entry.retire_ended_feed(&feed) {
                                        return None;
                                    }
                                    entry.attachment = false;
                                    let released = entry.prebind.take();
                                    entry.restart_unattached_timeout(&this.registry, this.local_id);
                                    released
                                })
                            });
                            drop(released_prebind);
                            return Poll::Ready(Some(Ok(StreamFrame::Detached)));
                        }
                        Ok(StreamFrame::Dropped) => {
                            this.terminated = true;
                            // Sender dropped without an explicit close.
                            this.retire();
                            return Poll::Ready(Some(Err(StreamError::SenderDropped)));
                        }
                        Ok(StreamFrame::TransportError(msg)) => {
                            this.terminated = true;
                            this.retire();
                            return Poll::Ready(Some(Err(StreamError::TransportError(msg))));
                        }
                        Err(e) => {
                            this.terminated = true;
                            this.retire();
                            return Poll::Ready(Some(Err(StreamError::DeserializationError(
                                e.to_string(),
                            ))));
                        }
                    }
                }
                Poll::Ready(None) => {
                    this.terminated = true;
                    this.retire();
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AnchorManager
// ---------------------------------------------------------------------------

/// Central registry that creates and tracks streaming anchors.
///
/// `worker_id` is stamped into every [`StreamAnchorHandle`] so that remote
/// peers can route responses back to the correct worker. `next_local_id`
/// starts at 0 and is incremented with `fetch_add(1)` -- the *result + 1*
/// is the first valid local ID (i.e., IDs start at 1; 0 is reserved).
///
/// The `registry` is wrapped in an `Arc` so that control handlers and the
/// consumer-side tasks can hold a cheap clone of the registry reference without
/// holding a reference to the whole `AnchorManager`.
///
/// Use [`AnchorManagerBuilder`] for optional configuration (e.g. `default_unattached_timeout`,
/// `default_heartbeat_interval`), or [`AnchorManager::new`] as a convenience constructor
/// with no unattached timeout and the protocol default heartbeat interval (5s).
#[derive(Builder)]
#[builder(pattern = "owned", build_fn(name = "build_inner", private))]
pub struct AnchorManager {
    worker_id: velo_ext::WorkerId,

    #[builder(setter(skip), default = "AtomicU64::new(0)")]
    next_local_id: AtomicU64,

    #[builder(default = "Arc::new(DashMap::new())")]
    pub(crate) registry: Arc<DashMap<u64, AnchorEntry>>,

    /// MPSC-variant registry. Separate from `registry` so existing SPSC
    /// handler code paths do not need enum-matching. Local IDs are still
    /// allocated from the shared `next_local_id` counter so the two
    /// namespaces never collide.
    #[builder(default = "Arc::new(DashMap::new())")]
    pub(crate) mpsc_registry: Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,

    pub transport: Arc<dyn crate::streaming::transport::FrameTransport>,

    /// Transport registry: maps scheme (e.g., "tcp", "velo") to the FrameTransport
    /// that handles endpoints with that scheme. Populated at build time via
    /// `AnchorManagerBuilder::transport_registry()`. Read-only after construction.
    /// Used by `attach_remote` to resolve the correct transport for `connect()`.
    #[builder(default = "Arc::new(HashMap::new())")]
    pub transport_registry:
        Arc<HashMap<String, Arc<dyn crate::streaming::transport::FrameTransport>>>,

    /// Default inactivity timeout for newly created anchors.
    /// When set, `create_anchor` spawns a timeout task that auto-removes the
    /// anchor if no sender attaches within this duration. Per-anchor overrides
    /// are supported via [`AnchorConfig::unattached_timeout`] +
    /// [`AnchorManager::create_anchor_with_config`].
    #[builder(default, setter(into, strip_option))]
    pub default_unattached_timeout: Option<Duration>,

    /// Default heartbeat cadence negotiated with senders attached to anchors
    /// created by this manager. Per-anchor overrides are supported via
    /// [`AnchorConfig::heartbeat_interval`] +
    /// [`AnchorManager::create_anchor_with_config`]. Defaults to 5 seconds,
    /// matching the historical hardcoded value.
    #[builder(default = "Duration::from_secs(5)")]
    pub default_heartbeat_interval: Duration,

    /// Optional messenger for sending `_stream_cancel` AM from the consumer side.
    /// Set whenever the anchor has a remote counterpart; `None` for local /
    /// mock-transport scenarios.
    #[builder(default)]
    pub messenger: Option<Arc<crate::messenger::Messenger>>,

    /// Shared Prometheus collectors for streaming control-plane metrics.
    ///
    /// Set it through the builder. `build` registers this manager as a source
    /// of `velo_streaming_active_anchors` with these collectors, so collectors
    /// assigned to the field afterwards never see the gauge.
    #[builder(default)]
    pub metrics: Option<Arc<VeloMetrics>>,

    /// Monotonically increasing counter for sender_stream_id values.
    /// Separate from next_local_id to keep anchor-side and sender-side namespaces distinct.
    #[builder(setter(skip), default = "AtomicU64::new(0)")]
    next_sender_stream_id: AtomicU64,

    /// Receiver-allocated counter for transport routing session ids. Each
    /// remote attach reserves a unique routing slot from this counter so the
    /// `(anchor_id, session_id)` pair used by the transport layer cannot
    /// collide across senders from different worker_ids (their local
    /// `next_sender_stream_id` counters are independent and both start at 0).
    /// See the cross-worker MPSC attach regression test for the bug class.
    #[builder(setter(skip), default = "AtomicU64::new(0)")]
    pub(crate) next_routing_session_id: AtomicU64,

    /// Sender-side registry: maps sender_stream_id -> SenderEntry.
    /// Shared with the _stream_cancel handler registered on this AnchorManager.
    /// Also accessed by StreamSender::Drop / finalize / detach for cleanup.
    #[builder(default = "Arc::new(crate::streaming::control::SenderRegistry::default())")]
    pub sender_registry: Arc<crate::streaming::control::SenderRegistry>,

    /// Write-once lock storing the live Messenger after `register_handlers` is called.
    /// `None` until `register_handlers` succeeds; subsequent calls return `Err`.
    #[builder(setter(skip), default = "std::sync::OnceLock::new()")]
    pub(crate) messenger_lock: std::sync::OnceLock<Arc<crate::messenger::Messenger>>,

    /// The `messenger-mux-v2` transport, when one is installed.
    ///
    /// Held as its concrete type rather than only as a registry entry because
    /// negotiation needs things the `FrameTransport` trait does not carry: the
    /// window to advertise on an attach response, a `connect` that takes the
    /// window a peer advertised back, and — for zero-RTT setup — a synchronous
    /// `prebind` plus the `release_bind` and `close_claimed_slot` that give a
    /// pre-bound slot back. Keeping all of that off the trait is deliberate:
    /// `FrameTransport` lives in `velo-ext` and out-of-tree implementors should
    /// not grow methods about one in-tree transport's credit protocol.
    ///
    /// Write-once, like `messenger_lock`, and skipped by the builder: it is a
    /// crate-internal type, and a public setter naming it would leak it.
    #[builder(setter(skip), default = "std::sync::OnceLock::new()")]
    mux: std::sync::OnceLock<Arc<crate::streaming::messenger_mux::MessengerMuxTransport>>,
}

impl AnchorManagerBuilder {
    /// Build the [`AnchorManager`].
    pub fn build(self) -> Result<AnchorManager, AnchorManagerBuilderError> {
        let manager = self.build_inner()?;
        if let Some(metrics) = manager.metrics.as_ref() {
            // Weak, so the metrics registry does not keep a dropped manager's
            // anchors alive; a dropped manager counts zero.
            let spsc = Arc::downgrade(&manager.registry);
            let mpsc = Arc::downgrade(&manager.mpsc_registry);
            // Each registry counts on its own: a `StreamAnchor` keeps the SPSC
            // registry alive past the manager and the MPSC one does not, so
            // the source is spent only once both are gone.
            metrics.add_active_anchor_source(move || {
                let spsc = spsc.upgrade();
                let mpsc = mpsc.upgrade();
                if spsc.is_none() && mpsc.is_none() {
                    return None;
                }
                Some(spsc.map_or(0, |r| r.len()) + mpsc.map_or(0, |r| r.len()))
            });
        }
        Ok(manager)
    }
}

impl AnchorManager {
    /// Stop and join mux sends while their messenger transports still exist.
    /// Readers stay attached until full shutdown withdraws their feeds.
    pub(crate) async fn stop_mux_sending(&self) {
        if let Some(mux) = self.mux.get() {
            mux.stop_sending_and_wait().await;
        }
    }

    /// Detach streams before retiring mux slots. Explicit shutdown calls this
    /// after messenger teardown; final owner Drop uses it without a drain.
    ///
    /// After step 1, the mux accepts no new sends. No mux slot is retired
    /// while a stream still reads from it, because retirement injects
    /// `Dropped` that the reader would
    /// take as its sender's. Hence the steps:
    ///
    /// 1. Stop the mux's tasks, if the transports are already gone (explicit
    ///    shutdown). A slot close after this finds no batcher and sends
    ///    nothing. The slots stay open.
    /// 2. Take streams off their slots: SPSC feeds withdrawn, MPSC pumps
    ///    cancelled.
    /// 3. Remove anchors and MPSC entries, and cancel their senders: local
    ///    ones directly, remote ones by `_stream_cancel` while the messenger's
    ///    transports are still up (final drop).
    ///
    /// On final drop the caller stops the mux next, so each batcher sends the
    /// slot closes step 3 queued before it exits; a zero-RTT producer learns
    /// that its stream ended only from that close.
    ///
    /// The caller then retires mux slots, after joining tasks if it can wait.
    ///
    /// Steps 1-3 do not await. A caller that drops the shutdown future (a timeout
    /// around `Velo::shutdown`) still leaves no stream waiting on a slot or an
    /// anchor. The slots then stay open, with no reader, until the mux drops.
    ///
    /// `Velo::graceful_shutdown` stops and joins mux sends before transport
    /// teardown. A direct call to this method is safe because slots are
    /// retired only after the streams leave them, never by this method: step
    /// 1 runs only when the transports are already gone, so it is not what
    /// keeps a direct call safe.
    fn prepare_stop(&self) {
        // Remote senders hear of the end only from this node: the mux is
        // stopping, and a retained Messenger drops their batches once the mux
        // is gone. Without word they fill the window and wait forever. Only
        // while the transports are up: explicit shutdown calls this after
        // teardown, and must send nothing.
        let peers = self
            .messenger_lock
            .get()
            .filter(|m| !m.backend().teardown_requested());
        let mux = self.mux.get();
        // With transports up, the caller stops the mux after the removals
        // below have queued their slot closes: a zero-RTT producer has no
        // other signal, and a stopping batcher sends what is already queued.
        if peers.is_none()
            && let Some(mux) = mux
        {
            mux.stop_sending();
        }
        for mut entry in self.registry.iter_mut() {
            entry.retire_pump();
        }
        for entry in self.mpsc_registry.iter() {
            for slot in entry.senders.values() {
                if let Some(pump) = &slot.pump_token {
                    pump.cancel();
                }
            }
        }
        // Remove entries outside shard guards: their Drop may close a mux slot.
        // An attached sender is also told by `_stream_cancel`.
        let ids: Vec<_> = self.registry.iter().map(|entry| *entry.key()).collect();
        for id in ids {
            if let Some(handle) = self
                .remove_anchor(id)
                .and_then(|entry| entry.stream_cancel_handle)
            {
                crate::streaming::control::request_sender_cancel(
                    handle,
                    self.worker_id,
                    &self.sender_registry,
                    peers,
                );
            }
        }
        let ids: Vec<_> = self
            .mpsc_registry
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for id in ids {
            if let Some((_, entry)) = self.mpsc_registry.remove(&id) {
                crate::streaming::mpsc::anchor::cancel_all_senders(
                    &entry,
                    self.worker_id,
                    &self.sender_registry,
                    peers,
                );
                entry.cancel_token.cancel();
            }
        }
        let ids: Vec<_> = self
            .sender_registry
            .senders
            .iter()
            .map(|entry| *entry.key())
            .collect();
        for id in ids {
            self.sender_registry.cancel(id);
        }
    }

    pub(crate) fn stop(&self) {
        self.prepare_stop();
        if let Some(mux) = self.mux.get() {
            mux.stop();
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.prepare_stop();
        if let Some(mux) = self.mux.get() {
            mux.shutdown().await;
        }
    }

    /// Convenience constructor with no default timeout.
    ///
    /// Equivalent to `AnchorManagerBuilder::default().worker_id(id).transport(t).build()`.
    pub fn new(
        worker_id: velo_ext::WorkerId,
        transport: Arc<dyn crate::streaming::transport::FrameTransport>,
    ) -> Self {
        AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .build()
            .expect("required fields provided")
    }

    /// Allocate a new anchor with the manager's default liveness configuration.
    ///
    /// Equivalent to [`create_anchor_with_config`](Self::create_anchor_with_config)
    /// called with `AnchorConfig::default()` — i.e. inherits both
    /// `default_unattached_timeout` and `default_heartbeat_interval`.
    ///
    /// The returned `StreamAnchor` embeds the [`StreamAnchorHandle`]; obtain it via
    /// [`.handle()`](StreamAnchor::handle) to pass to a sender for attachment.
    ///
    /// Local IDs start at 1 and increment monotonically; ID 0 is reserved.
    /// A flume bounded channel (capacity 256) is created per anchor to deliver raw frame bytes.
    pub fn create_anchor<T>(&self) -> StreamAnchor<T> {
        self.create_anchor_with_config(AnchorConfig::default())
    }

    /// Allocate a new anchor with per-anchor liveness overrides.
    ///
    /// `config.unattached_timeout` and `config.heartbeat_interval` each override
    /// the corresponding manager default when `Some`; `None` inherits.
    /// The resolved `heartbeat_interval` is later echoed to the attaching sender
    /// via [`crate::streaming::control::AnchorAttachResponse`] so both sides agree without
    /// hardcoded constants.
    pub fn create_anchor_with_config<T>(&self, config: AnchorConfig) -> StreamAnchor<T> {
        // fetch_add returns the *old* value (starts at 0), so +1 gives us IDs starting at 1.
        let local_id = self.next_local_id.fetch_add(1, Ordering::Relaxed) + 1;

        let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
        let cancel_token = CancellationToken::new();

        // Resolve liveness knobs: per-anchor override > manager default.
        let unattached_timeout = config
            .unattached_timeout
            .or(self.default_unattached_timeout);
        let heartbeat_interval = config
            .heartbeat_interval
            .unwrap_or(self.default_heartbeat_interval);

        // Spawn timeout task if configured — derive child from the anchor's parent token
        // so that finalize/remove auto-cancels it.
        let timeout_cancel = unattached_timeout.map(|timeout| {
            Self::spawn_timeout_task(self.registry.clone(), local_id, timeout, &cancel_token)
        });

        let feed = Arc::new(crate::streaming::control::FeedCell::default());
        let entry = AnchorEntry {
            feed: Arc::clone(&feed),
            frame_tx,
            cancel_token,
            active_pump_token: None,
            attachment: false,
            timeout_cancel,
            unattached_timeout,
            heartbeat_interval,
            stream_cancel_handle: None, // populated on attach
            prebind: None,              // populated by `prebind_anchor`
            stop_requested: false,
        };

        self.registry.insert(local_id, entry);

        let handle = StreamAnchorHandle::pack(self.worker_id, local_id);
        StreamAnchor::new(
            handle,
            frame_rx,
            feed,
            local_id,
            self.anchor_context(),
            self.sender_registry.clone(),
            self.messenger.clone(),
        )
    }

    /// Spawn a background task that removes the anchor after `timeout` elapses.
    ///
    /// Returns a [`CancellationToken`] that cancels the task when triggered
    /// (e.g. on attach, or when `set_timeout(None)` is called). The returned
    /// token is safe to store and cancel unconditionally even when no task
    /// was actually spawned (see the guard below) — every reader of
    /// `AnchorEntry::timeout_cancel` only ever calls `.cancel()` on it, which
    /// is a no-op with nothing listening.
    ///
    /// Guarded by `Handle::try_current()` for the same reason
    /// `close_claimed_slot` (`messenger_mux/mod.rs`) and `StreamController::cancel`
    /// (below) are: this is reachable from `StreamAnchor::poll_next`'s `Detached`
    /// arm, which runs under whatever executor the *consumer* chose, not
    /// necessarily tokio. Degrading to "the timer never fires" is the same
    /// trade those two make — the alternative is a bare `tokio::spawn` panic
    /// in the middle of a consumer's poll.
    pub(crate) fn spawn_timeout_task(
        registry: Arc<DashMap<u64, AnchorEntry>>,
        local_id: u64,
        timeout: Duration,
        parent_cancel: &CancellationToken,
    ) -> CancellationToken {
        let tc = parent_cancel.child_token();
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            tracing::debug!(
                local_id,
                "anchor: no runtime to arm the unattached timeout on; it will not fire"
            );
            return tc;
        };
        let tc_clone = tc.clone();
        rt.spawn(async move {
            tokio::select! {
                _ = tc_clone.cancelled() => {
                    // Attach or explicit cancel -- do nothing
                }
                _ = tokio::time::sleep(timeout) => {
                    // Timeout expired -- remove anchor
                    if let Some((_, entry)) = registry.remove_if(&local_id, |_, entry| {
                        !tc_clone.is_cancelled() && !entry.attachment && entry.prebind.is_none()
                    }) {
                        entry.cancel_token.cancel();
                    }
                }
            }
        });
        tc
    }

    /// Remove an anchor from the registry and return its entry (if present).
    ///
    /// Cancels the entry's token before returning. Used by control-path cleanup
    /// handlers and drop impls.
    pub(crate) fn remove_anchor(&self, local_id: u64) -> Option<AnchorEntry> {
        self.registry.remove(&local_id).map(|(_, entry)| {
            entry.cancel_token.cancel();
            entry
        })
    }

    /// Install the mux this manager negotiates with, once.
    ///
    /// Separate from the transport registry, which the mux also joins: the
    /// registry answers "can I `connect()` on this key", while this answers
    /// "may I offer, and drive, `messenger-mux-v2`". Both are needed and they
    /// are set together by the builder.
    pub(crate) fn install_mux(
        &self,
        mux: Arc<crate::streaming::messenger_mux::MessengerMuxTransport>,
    ) -> anyhow::Result<()> {
        self.mux
            .set(mux)
            .map_err(|_| anyhow::anyhow!("a messenger mux is already installed on this manager"))
    }

    /// Bind a mux slot for `handle` and start its feed now, so its sender
    /// never has to ask for one.
    ///
    /// Returns the terms that sender must open on, to be carried to it in
    /// whatever envelope the application already sends — see
    /// [`crate::streaming::control::StreamOpenTicket`] and its counterpart
    /// [`open_anchor_stream`](Self::open_anchor_stream). Everything the
    /// `_anchor_attach` handler would have done on the round trip happens here
    /// instead, so the sender's first record is its first message.
    ///
    /// `None` means *no ticket was minted; attach the ordinary way*, and as a
    /// return it is never an error. With no mux installed nothing here can run
    /// and every stream takes the per-stream path — which is what keeps
    /// `MuxConfig::enabled` the complete rollback for this path too, and why
    /// that `None` alone is silent.
    ///
    /// `None` is also the answer for a handle this manager cannot pre-bind: one
    /// belonging to another worker, an MPSC anchor, an anchor already attached
    /// or already pre-bound, or one that has since been removed. Those are
    /// logged at debug and recorded as `outcome="error"` under
    /// `velo_streaming_anchor_operations_total{operation="prebind"}`, because
    /// unlike the rollback they are a caller mistake rather than a
    /// configuration.
    ///
    /// Must be called from a runtime context: it spawns the stream watchdog,
    /// exactly as the attach handler does for a mux bind.
    ///
    /// The ticket may sit in a request envelope for up to the mux's 60 s
    /// accept window before its worker opens it: heartbeat detection does not
    /// start until an `OpenSlot` claims the bind, so nothing shorter can reap
    /// it, and the configured `unattached_timeout` does not apply here either
    /// — it stays paused for the whole pre-bind phase, same as at attach. A
    /// worker that may be queued longer than that needs a longer-lived
    /// rendezvous than this call provides.
    ///
    /// The ticket names the mux lane with the least load on this node: its
    /// live slots from every peer, plus its pre-binds not yet claimed,
    /// released or expired. [`prebind_anchor_keyed`](Self::prebind_anchor_keyed)
    /// places it by a key instead.
    pub fn prebind_anchor(
        &self,
        handle: StreamAnchorHandle,
    ) -> Option<crate::streaming::control::StreamOpenTicket> {
        self.prebind_anchor_on(handle, None)
    }

    /// As [`prebind_anchor`](Self::prebind_anchor), with the stream placed on
    /// the mux lane `key` hashes to.
    ///
    /// The hash is fixed across builds and processes, so one key gives the
    /// same lane index on every node that keeps the same lane count. With one
    /// lane (the default transport setup) every key is on lane 0. The key is a
    /// placement hint, not an ordering guarantee: the mux never orders records
    /// across streams, even on one lane.
    pub fn prebind_anchor_keyed(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Option<crate::streaming::control::StreamOpenTicket> {
        self.prebind_anchor_on(handle, Some(key))
    }

    fn prebind_anchor_on(
        &self,
        handle: StreamAnchorHandle,
        lane_key: Option<u64>,
    ) -> Option<crate::streaming::control::StreamOpenTicket> {
        // The rollback, and the only `None` that is not a mistake -- zero-RTT
        // is simply off, so there is no operation to record.
        let mux = self.mux.get()?;
        let started = Instant::now();

        let (handle_worker_id, local_id) = handle.unpack();
        if handle.is_mpsc_stream() || handle_worker_id != self.worker_id {
            tracing::debug!(
                %handle,
                "prebind_anchor: not a local SPSC anchor; no ticket minted"
            );
            self.record_streaming_operation(
                StreamingOp::Prebind,
                HandlerOutcome::Error,
                "unknown",
                started,
            );
            return None;
        }

        // Fail fast on the common "not pre-bindable" case before minting a
        // session id or registering a bind: `mux.bind_on_lane` queues a 60 s
        // accept-window deadline, and a bind registered for nothing holds its
        // buffer until that deadline passes. This is a plain read, not a lock the mutate-and-check
        // below still has to redo -- an entry can change between the two, and
        // that race is the same bounded one every other caller of this check
        // already accepts (see the `Entry::Occupied` arm below).
        match self.registry.get(&local_id) {
            None => {
                tracing::debug!(%handle, "prebind_anchor: anchor is missing; no ticket minted");
                self.record_streaming_operation(
                    StreamingOp::Prebind,
                    HandlerOutcome::Error,
                    "unknown",
                    started,
                );
                return None;
            }
            Some(entry) if entry.attachment || entry.prebind.is_some() => {
                tracing::debug!(
                    %handle,
                    "prebind_anchor: anchor is attached or already pre-bound; no ticket minted"
                );
                self.record_streaming_operation(
                    StreamingOp::Prebind,
                    HandlerOutcome::Error,
                    "unknown",
                    started,
                );
                return None;
            }
            Some(_) => {}
        }

        // Receiver-allocated, for the reason the attach handler allocates one:
        // two senders reusing their own local counters would collide on the
        // transport's `(anchor_id, session_id)` routing key.
        let routing_session_id = self.next_routing_session_id.fetch_add(1, Ordering::Relaxed) + 1;
        // No sender is known yet, so the lane is placed against this node's
        // own pre-binds. Chosen before the bind so the bind is counted on it.
        let lane = mux.choose_lane(None, lane_key);
        let lane_index = lane.lane();
        // The bind fails once the mux is stopped. Its drain signal is missing
        // only when mux shutdown cleared it after the bind, which is the same
        // case, a moment later.
        let bound = mux
            .bind_on_lane(local_id, routing_session_id, lane)
            .ok()
            .and_then(
                |receiver| match mux.take_drain_signal(local_id, routing_session_id) {
                    Some(drain) => Some((receiver, drain)),
                    None => {
                        mux.release_bind(local_id, routing_session_id);
                        None
                    }
                },
            );
        let Some((receiver, drain)) = bound else {
            tracing::debug!(%handle, "prebind_anchor: the mux is shut down; no ticket minted");
            self.record_streaming_operation(
                StreamingOp::Prebind,
                HandlerOutcome::Error,
                "unknown",
                started,
            );
            return None;
        };

        // Negotiated against the local mux alone. There is no peer here to
        // intersect with — that is the whole point — so the terms are this
        // node's own window, which is exactly what `select` would have answered
        // a sender that named the mux.
        let key = velo_ext::TransportKey::new(crate::streaming::MESSENGER_MUX_KEY);
        let prepared = {
            use dashmap::mapref::entry::Entry;
            match self.registry.entry(local_id) {
                Entry::Vacant(_) => None,
                Entry::Occupied(mut occ) => {
                    let entry = occ.get_mut();
                    if entry.attachment || entry.prebind.is_some() {
                        None
                    } else {
                        let ticket = crate::streaming::control::StreamOpenTicket::from_limits(
                            key,
                            entry.heartbeat_interval,
                            routing_session_id,
                            mux.advertised_limits(),
                            lane_index,
                        );
                        // A child of the anchor's token, as at attach: finalize,
                        // cancel and detach stop the watchdog without cancelling
                        // the parent.
                        let pump_cancel = entry.cancel_token.child_token();
                        entry.active_pump_token = Some(pump_cancel.clone());
                        // The unattached timer measures "no sender is coming".
                        // A pre-bound anchor has a slot waiting for one, so the
                        // timer is measuring nothing and would remove a live
                        // stream. Attach pauses it for the same reason.
                        if let Some(ref tc) = entry.timeout_cancel {
                            tc.cancel();
                        }
                        // Shared with the watchdog spawned below and cleared by
                        // `PreBind::adopt` the moment an attach hands this
                        // slot to a sender that used it instead of the
                        // ticket -- see `WatchdogContext::prebound`.
                        let prebound = Arc::new(AtomicBool::new(true));
                        if entry.stop_requested {
                            drain.request_stop();
                        }
                        entry.prebind = Some(PreBind {
                            anchor_id: local_id,
                            ticket: ticket.clone(),
                            drain: Arc::clone(&drain),
                            mux: Arc::downgrade(mux),
                            prebound: Arc::clone(&prebound),
                        });
                        // Installed under the shard lock, so a retire or a
                        // removal cannot land between this and the install.
                        let (feed, replaced) = crate::streaming::control::install_direct_feed(
                            entry,
                            crate::streaming::control::DirectFeed {
                                rx: receiver.clone(),
                                drain: Arc::clone(&drain),
                                pump_token: pump_cancel,
                                release: Some(crate::streaming::control::SlotRelease {
                                    mux: Arc::downgrade(mux),
                                    anchor_id: local_id,
                                    session_id: routing_session_id,
                                }),
                            },
                        );
                        Some((
                            ticket,
                            entry.frame_tx.clone(),
                            entry.heartbeat_interval,
                            prebound,
                            feed,
                            replaced,
                        ))
                    }
                }
            }
        };

        let Some((ticket, frame_tx, heartbeat_interval, prebound, feed, replaced)) = prepared
        else {
            // Nothing took ownership of the bind, so give it straight back
            // rather than leaving the accept window to find it in a minute.
            mux.release_bind(local_id, routing_session_id);
            tracing::debug!(
                %handle,
                "prebind_anchor: anchor is missing, attached or already pre-bound; no ticket minted"
            );
            self.record_streaming_operation(
                StreamingOp::Prebind,
                HandlerOutcome::Error,
                "unknown",
                started,
            );
            return None;
        };

        if let Some(replaced) = replaced {
            replaced.release_slot();
        }
        drop(receiver);
        crate::streaming::control::launch_direct_stream(
            feed,
            frame_tx,
            self.anchor_context(),
            crate::streaming::control::WatchdogContext {
                local_id,
                heartbeat_deadline: heartbeat_interval,
                // The one genuine pre-bind spawn site: no sender has shown up
                // yet. The `Arc` is the same cell stored in `PreBind` above,
                // so `PreBind::adopt` can flip it the moment one does. See
                // `WatchdogContext::prebound`.
                prebound,
            },
        );

        self.record_streaming_operation(
            StreamingOp::Prebind,
            HandlerOutcome::Success,
            ticket.streaming_transport_key.as_str(),
            started,
        );
        Some(ticket)
    }

    /// Decide what an incoming `_anchor_attach` may do with a pre-bound slot.
    ///
    /// Called before the handler's own already-attached check, because a
    /// pre-bound anchor is *not* attached — nothing has claimed it, which is
    /// what makes it adoptable. The exactly-once token is unchanged either way:
    /// on the attach path it is the `attachment` flip this sets, and under
    /// zero-RTT it is the `binds.remove` an `OpenSlot` performs. Adoption
    /// consumes neither more nor less than one of them.
    pub(crate) fn adopt_prebind(
        &self,
        local_id: u64,
        req: &crate::streaming::control::AnchorAttachRequest,
    ) -> PrebindAdoption {
        /// What the pre-bind, if any, allows — decided from a shared borrow so
        /// the arm that acts on it can take a unique one.
        enum Verdict {
            None,
            Claimed,
            Mismatch(velo_ext::TransportKey),
            Adopt,
        }

        // The verdict is reached under the shard lock; a pre-bind to release
        // leaves with it and is dropped after, so a `PreBind::drop` that has to
        // talk to the mux never runs with a registry shard held.
        let (verdict, released) = {
            use dashmap::mapref::entry::Entry;
            let Entry::Occupied(mut occ) = self.registry.entry(local_id) else {
                return PrebindAdoption::None;
            };
            let entry = occ.get_mut();
            let verdict = match entry.prebind.as_ref() {
                None => Verdict::None,
                // Claimed means an `OpenSlot` already opened this slot with the
                // ticket's own session id, so the stream is running and this
                // attach is a second opener. Binding it a fresh slot would give
                // the anchor two senders; adopting would hand out terms already
                // in use. Refusing leaves the live stream alone, which is the
                // only answer that does.
                Some(prebind) if prebind.is_claimed() => Verdict::Claimed,
                Some(prebind)
                    if !req
                        .supported_transport_keys
                        .contains(&prebind.ticket().streaming_transport_key) =>
                {
                    Verdict::Mismatch(prebind.ticket().streaming_transport_key.clone())
                }
                Some(_) => Verdict::Adopt,
            };
            match verdict {
                Verdict::None => (PrebindAdoption::None, None),
                Verdict::Claimed => (
                    PrebindAdoption::Refused(format!(
                        "anchor {} is already streaming through a pre-bound slot",
                        req.handle
                    )),
                    None,
                ),
                Verdict::Mismatch(key) => {
                    // Released rather than left to the accept window: the sender
                    // is being told the attach failed, so it will never send the
                    // `OpenSlot` that would claim it.
                    let released = entry.prebind.take();
                    // The watchdog and feed this prebind's own bind started
                    // have to stop with it. `released`'s `Drop` calls
                    // `release_bind` below, which closes the unclaimed bind;
                    // left running, the watchdog (on `DrainSignal::closed`) or
                    // the consumer (on its feed's buffer closing) would run
                    // `control::reap_unclaimed`, which for an *unclaimed* drain
                    // removes the registry entry so an abandoned pre-bind is
                    // actually reaped. That entry is this one, still very much
                    // alive for the next attach to use. `retire_pump` cancels
                    // the token, which the reap checks, and withdraws the
                    // feed, so neither can tear down whatever wins this race.
                    // Left `None` on purpose, matching `_anchor_detach`: the
                    // next attach or `prebind_anchor` creates its own.
                    entry.retire_pump();
                    // And the anchor is unattached again, so the timer that
                    // measures exactly that has to come back. `prebind_anchor`
                    // cancelled it because a pre-bind is a sender on its way;
                    // this is that sender turning back. Without the re-arm the
                    // anchor has no reaper left for the consumer that holds its
                    // `StreamAnchor` open and never gets a replacement sender —
                    // `Drop` only reaps a `StreamAnchor` that is actually
                    // dropped un-terminated (see `retire`'s doc above), and one
                    // the application is still holding is never dropped.
                    //
                    // Called under the `Entry::Occupied` guard held above:
                    // safe now that `spawn_timeout_task` guards its own
                    // `tokio::spawn` — the call never runs synchronously and
                    // never touches this registry, so there is nothing here
                    // for it to deadlock against.
                    entry.restart_unattached_timeout(&self.registry, local_id);
                    (
                        PrebindAdoption::Refused(format!(
                            "anchor {} was pre-bound on {key}, which this sender does not support",
                            req.handle
                        )),
                        released,
                    )
                }
                Verdict::Adopt => {
                    let prebind = entry.prebind.take().expect("the verdict says it is there");
                    entry.attachment = true;
                    entry.stream_cancel_handle = Some(req.stream_cancel_handle);
                    if entry.stop_requested {
                        crate::streaming::control::request_sender_stop(
                            req.stream_cancel_handle,
                            self.worker_id,
                            &self.sender_registry,
                            self.messenger_lock.get(),
                        );
                    }
                    (PrebindAdoption::Adopted(prebind.adopt()), None)
                }
            }
        };
        drop(released);
        verdict
    }

    /// A weak handle to the installed mux, for a consumer that has to close
    /// its own slot. Weak for the reason `PreBind` holds one.
    pub(crate) fn mux_handle(
        &self,
    ) -> Option<std::sync::Weak<crate::streaming::messenger_mux::MessengerMuxTransport>> {
        self.mux.get().map(Arc::downgrade)
    }

    /// Take the drain signal the mux parked for a bind, if the mux made it.
    ///
    /// `None` for the legacy per-stream transports, which is the honest answer:
    /// they issue no credit over that seam, so there is nothing to return.
    pub(crate) fn take_mux_drain_signal(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> Option<Arc<crate::streaming::messenger_mux::ingress::DrainSignal>> {
        self.mux
            .get()
            .and_then(|mux| mux.take_drain_signal(anchor_id, session_id))
    }

    /// Write what the mux's batchers have staged, if a mux is installed.
    ///
    /// A no-op without one, which is the honest answer rather than an error:
    /// the legacy per-stream transports have nothing staged to write, since
    /// their egress pumps hand every frame straight to a socket.
    pub(crate) fn flush_mux_batches(&self) {
        if let Some(mux) = self.mux.get() {
            mux.flush_batches();
        }
    }

    /// The transports this node advertises when it attaches to a remote anchor.
    fn supported_transport_keys(&self) -> Vec<velo_ext::TransportKey> {
        crate::streaming::negotiation::advertised_keys(
            &self.transport_registry,
            &self.transport,
            self.mux.get(),
        )
    }

    /// Pick the transport to bind for an incoming attach from `peer`, and
    /// the mux lane, by `lane_key` if the sender gave one.
    ///
    /// Called by both attach handlers, which differ only in the response type
    /// they pour the answer into.
    pub(crate) fn select_streaming_transport(
        &self,
        offered: &[velo_ext::TransportKey],
        peer: velo_ext::WorkerId,
        lane_key: Option<u64>,
    ) -> crate::streaming::negotiation::Selection {
        crate::streaming::negotiation::select(
            offered,
            self.mux.get(),
            &self.transport,
            peer,
            lane_key,
        )
    }

    /// Give back a bind an attach handler made and then could not commit.
    ///
    /// For the mux only: its bind counts on the lane it was placed on, and
    /// left to the 60 s accept window it would skew every unkeyed choice for
    /// that peer meanwhile. The same release `prebind_anchor` makes when
    /// nothing took its bind. It touches only the mux's own maps, so a caller
    /// may hold an anchor registry entry. Other transports' binds keep their
    /// own accept windows.
    pub(crate) fn release_unused_bind(
        &self,
        key: &velo_ext::TransportKey,
        anchor_id: u64,
        session_id: u64,
    ) {
        if key.as_str() == crate::streaming::messenger_mux::MESSENGER_MUX_KEY
            && let Some(mux) = self.mux.get()
        {
            mux.release_bind(anchor_id, session_id);
        }
    }

    /// Connect the transport the receiver's attach response named.
    ///
    /// The mux arm is why this is not just `resolve_transport(...).connect(...)`:
    /// a negotiated slot opens already holding the window the receiver
    /// advertised, which is what removes the round trip an `OpenSlot`-time
    /// `CreditUpdate` used to cost.
    async fn connect_streaming(
        &self,
        ticket: &crate::streaming::control::StreamOpenTicket,
        peer: velo_ext::WorkerId,
        anchor_id: u64,
        lifecycle: Option<crate::streaming::control::SenderEntry>,
    ) -> Result<flume::Sender<Vec<u8>>, AttachError> {
        let key = &ticket.streaming_transport_key;
        let session_id = ticket.routing_session_id;
        let initial_credit = ticket.initial_credit;
        let slot_byte_budget = ticket.slot_byte_budget;
        match crate::streaming::negotiation::choose(key, initial_credit, slot_byte_budget) {
            Ok(crate::streaming::negotiation::Connect::Mux(limits)) => {
                let mux = self.mux.get().ok_or_else(|| {
                    AttachError::TransportError(anyhow::anyhow!(
                        "peer answered with {key} but no messenger mux is installed here; \
                         it can only have learned that key from an advertisement this node made"
                    ))
                })?;
                // The lane the receiver named, clamped to the lanes this node
                // keeps to it. A receiver from before lanes names none: lane 0.
                let key = mux.sender_lane(peer, ticket.lane);
                Ok(mux
                    .connect_controlled(key, anchor_id, session_id, limits, lifecycle)
                    .await?)
            }
            Ok(crate::streaming::negotiation::Connect::Legacy) => {
                let transport = self.resolve_transport(key)?;
                Ok(transport.connect(peer, anchor_id, session_id).await?)
            }
            Err(error) => Err(AttachError::TransportError(anyhow::anyhow!(
                "peer answered with {key} but {error}"
            ))),
        }
    }

    /// Resolve a FrameTransport by streaming-transport key.
    ///
    /// Looks up `key` in the transport registry. Falls back to `self.transport`
    /// if the registry is empty (test/legacy convenience for callers that don't
    /// populate the registry).
    ///
    /// Returns `Err(AttachError::TransportError)` if the key is not found in a
    /// non-empty registry.
    fn resolve_transport(
        &self,
        key: &velo_ext::TransportKey,
    ) -> Result<Arc<dyn crate::streaming::transport::FrameTransport>, AttachError> {
        if let Some(transport) = self.transport_registry.get(key.as_str()) {
            return Ok(Arc::clone(transport));
        }
        if self.transport_registry.is_empty() {
            return Ok(Arc::clone(&self.transport));
        }
        Err(AttachError::TransportError(anyhow::anyhow!(
            "unsupported streaming transport key: {}",
            key
        )))
    }

    /// Returns the number of anchors currently registered.
    ///
    /// Intended for testing and observability. Counts SPSC anchors only; the
    /// Prometheus `velo_streaming_active_anchors` gauge counts SPSC and MPSC
    /// anchors of every manager sharing the metrics registry.
    pub fn active_anchor_count(&self) -> usize {
        self.registry.len()
    }

    /// Bundle the SPSC registry, MPSC registry, and metrics collector into
    /// a cheap `Arc`-cloneable context. Used to keep anchor constructors
    /// and background tasks under clippy's argument threshold.
    pub(crate) fn anchor_context(&self) -> AnchorContext {
        AnchorContext {
            registry: self.registry.clone(),
            mpsc_registry: self.mpsc_registry.clone(),
            metrics: self.metrics.clone(),
        }
    }

    pub(crate) fn record_streaming_operation(
        &self,
        operation: StreamingOp,
        outcome: HandlerOutcome,
        transport_scheme: &str,
        started: Instant,
    ) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.record_streaming_operation(
                operation,
                outcome,
                transport_scheme,
                started.elapsed(),
            );
        }
    }

    /// Observe one remote attach round trip, from the sender's side.
    ///
    /// The single place both the SPSC (`attach_remote`) and MPSC
    /// (`attach_mpsc_remote`) paths stamp the RTT, so the two cannot drift into
    /// bracketing different spans of work. `started` must be taken immediately
    /// before the attach request's send — to `_anchor_attach` or
    /// `_mpsc_anchor_attach`, whichever this call is for — and observed
    /// immediately after it: the transport connect that follows a successful
    /// attach is the sender's own work, not the round trip, and folding it in
    /// would make the excess over the receiver's handler timing stop meaning
    /// "ingest queueing".
    ///
    /// Two paths observe nothing, for the same reason. The co-located attach
    /// never leaves the process, and the zero-RTT open
    /// ([`open_anchor_stream`](Self::open_anchor_stream)) is handed the terms
    /// the round trip existed to fetch. Neither has a round trip to report.
    pub(crate) fn record_attach_rtt(
        &self,
        started: Instant,
        outcome: HandlerOutcome,
        transport_scheme: &str,
    ) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.record_attach_rtt(outcome, transport_scheme, started.elapsed());
        }
    }

    /// Fold the transport key a peer answered an attach with into the closed
    /// set this node advertised, or `"unknown"`.
    ///
    /// `streaming_transport_key` on an attach response is a string the *remote*
    /// worker put on the wire, and `transport_scheme` is a metric label:
    /// recorded raw, a version-skewed or hostile peer mints one histogram child
    /// per distinct string, each alive for the life of the process. That is the
    /// unbounded cardinality a fixed label set is supposed to make impossible.
    ///
    /// The advertisement is the closed set to fold onto, and it is the right
    /// one: `resolve_transport` succeeds only on a key in this node's registry,
    /// and the mux key takes `Connect::Mux`, so every key that can lead to a
    /// working attach is in here. A key outside it fails `connect_streaming` a
    /// few lines later, and `"unknown"` is the honest label for it — nothing was
    /// negotiated. Normalising here rather than after the connect is what keeps
    /// the RTT bracket over the send alone.
    fn attach_scheme_label<'a>(
        advertised: &'a [velo_ext::TransportKey],
        answered: &velo_ext::TransportKey,
    ) -> &'a str {
        advertised
            .iter()
            .find(|key| key.as_str() == answered.as_str())
            .map_or("unknown", |key| key.as_str())
    }

    /// Register all five control-plane AM handlers on a live Messenger.
    ///
    /// Registers: `_anchor_attach`, `_anchor_detach`, `_anchor_finalize`,
    /// `_anchor_cancel` (each holding this manager weakly), and
    /// `_stream_cancel` (on `self.sender_registry`).
    ///
    /// Stores the messenger strongly in `messenger_lock` (write-once) for use
    /// by `attach_remote`. The handlers hold this manager weakly, so the
    /// caller must keep its own `Arc` for as long as the handlers should
    /// serve. The manager keeps the messenger alive: drop the manager too
    /// before expecting final Messenger drop to start teardown.
    ///
    /// # Errors
    ///
    /// Returns `Err` if called twice (OnceLock already set) or if any
    /// handler registration fails.
    ///
    /// # Panics
    ///
    /// Does not panic. Caller must hold an `Arc<AnchorManager>`.
    pub fn register_handlers(
        self: &Arc<Self>,
        messenger: Arc<crate::messenger::Messenger>,
    ) -> anyhow::Result<()> {
        // The manager holds the messenger, so its handlers must not hold the
        // manager: that cycle would keep both alive after their owners drop.
        let manager = Arc::downgrade(self);
        use crate::streaming::control::{
            anchor_attach_handler, anchor_cancel_handler, anchor_detach_handler,
            anchor_finalize_handler, create_stream_cancel_handler,
        };

        messenger.register_streaming_handler(anchor_attach_handler(manager.clone()))?;
        // Everything but the attaches serves a stream already open, so the
        // drain gate lets it through; an attach opens a new stream.
        messenger.register_drain_exempt_handler(anchor_detach_handler(manager.clone()))?;
        messenger.register_drain_exempt_handler(anchor_finalize_handler(manager.clone()))?;
        messenger.register_drain_exempt_handler(anchor_cancel_handler(manager.clone()))?;
        messenger.register_drain_exempt_handler(create_stream_cancel_handler(Arc::clone(
            &self.sender_registry,
        )))?;

        messenger.register_drain_exempt_handler(
            crate::streaming::control::create_stream_stop_handler(Arc::clone(
                &self.sender_registry,
            )),
        )?;

        // MPSC handlers — share the same SenderRegistry so `_stream_cancel`
        // covers both SPSC and MPSC senders uniformly.
        messenger.register_streaming_handler(
            crate::streaming::mpsc::control::mpsc_anchor_attach_handler(manager.clone()),
        )?;
        messenger.register_drain_exempt_handler(
            crate::streaming::mpsc::control::mpsc_anchor_detach_handler(manager.clone()),
        )?;
        messenger.register_drain_exempt_handler(
            crate::streaming::mpsc::control::mpsc_anchor_cancel_handler(manager),
        )?;

        self.messenger_lock
            .set(messenger)
            .map_err(|_| anyhow::anyhow!("register_handlers called twice"))?;

        Ok(())
    }

    /// Attach a sender to an existing anchor via the remote control-plane path.
    ///
    /// Called when `attach_stream_anchor` detects that `handle.worker_id != self.worker_id`.
    ///
    /// Sends an `_anchor_attach` AM to the remote worker, receives the stream endpoint,
    /// calls `transport.connect()` to establish the write channel, and returns a
    /// [`StreamSender<T>`](crate::streaming::sender::StreamSender) that writes directly into the
    /// transport bridge (which the remote reader pump forwards to the anchor's frame channel,
    /// or which, over the mux, the remote consumer reads directly).
    ///
    /// # Errors
    /// - [`AttachError::TransportError`] if `messenger_lock` is not set (register_handlers not called)
    /// - [`AttachError::TransportError`] if the AM send or transport connect fails
    /// - [`AttachError::TransportError`] if the remote worker returns `AnchorAttachResponse::Err`
    async fn attach_remote<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        lane_key: Option<u64>,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        let (handle_worker_id, _) = handle.unpack();

        // Require messenger_lock to be set (register_handlers must have been called)
        let messenger = self.messenger_lock.get().ok_or_else(|| {
            AttachError::TransportError(anyhow::anyhow!(
                "register_handlers not called — messenger unavailable for remote attach"
            ))
        })?;

        // Allocated before the terms are known, because the request has to name
        // it: the receiver stores the cancel handle built from it, and the
        // registry this side keys the resulting `SenderEntry` by the same id.
        let identity = self.new_sender_identity();
        let stream_cancel_handle = crate::streaming::control::StreamCancelHandle::pack(
            self.worker_id,
            identity.sender_stream_id,
        );

        // Build request payload (serde_json — typed_unary_async handlers use JSON)
        // Use sender_stream_id as the session_id for the remote attach request.
        let req = crate::streaming::control::AnchorAttachRequest {
            handle,
            session_id: identity.sender_stream_id,
            stream_cancel_handle,
            supported_transport_keys: self.supported_transport_keys(),
            lane_key,
        };

        // Send _anchor_attach AM to the remote worker (typed request-response).
        //
        // The RTT timer brackets the send and nothing else: the request is
        // built before it starts and `connect_streaming` below runs after it
        // stops, so what the histogram reports is exactly the wire round trip
        // the caller was blocked on.
        let request = messenger
            .typed_unary_streaming::<crate::streaming::control::AnchorAttachResponse>(
                "_anchor_attach",
            )
            .payload(&req)
            .map_err(AttachError::TransportError)?
            .worker(handle_worker_id);
        let started = Instant::now();
        let response: crate::streaming::control::AnchorAttachResponse = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                self.record_attach_rtt(started, HandlerOutcome::Error, "unknown");
                return Err(AttachError::TransportError(error));
            }
        };

        match response {
            crate::streaming::control::AnchorAttachResponse::Ok {
                streaming_transport_key,
                heartbeat_interval_ms,
                routing_session_id,
                initial_credit,
                slot_byte_budget,
                lane,
            } => {
                self.record_attach_rtt(
                    started,
                    HandlerOutcome::Success,
                    Self::attach_scheme_label(
                        &req.supported_transport_keys,
                        &streaming_transport_key,
                    ),
                );
                // Use the receiver-allocated routing_session_id so the
                // transport-layer routing slot is unique across senders from
                // different worker_ids (legacy senders set the field to 0 via
                // serde-default and fall back to the collision-prone
                // sender_stream_id). Resolved *here*, before the terms are
                // gathered up, so everything downstream reads one field that
                // always means "the session id to open on", whichever of the
                // two it turned out to be.
                let routing_session_id = if routing_session_id != 0 {
                    routing_session_id
                } else {
                    identity.sender_stream_id
                };
                // The response's six fields *are* the terms a stream opens on,
                // which is what a ticket carries, so the shared tail below takes
                // one shape rather than two. The credit fields are whatever the
                // peer answered, including the legacy zeros — `negotiation::choose`
                // is what interprets them, unchanged.
                let ticket = crate::streaming::control::StreamOpenTicket {
                    streaming_transport_key,
                    heartbeat_interval_ms,
                    routing_session_id,
                    initial_credit,
                    slot_byte_budget,
                    lane,
                };
                self.open_stream_sender::<T>(handle, &ticket, identity)
                    .await
            }
            crate::streaming::control::AnchorAttachResponse::Err { reason } => {
                // A refusal is still a round trip the caller waited on, so it
                // is observed. No scheme was ever negotiated, hence "unknown".
                self.record_attach_rtt(started, HandlerOutcome::Error, "unknown");
                Err(AttachError::TransportError(anyhow::anyhow!("{}", reason)))
            }
        }
    }

    /// Open the sender half of a stream whose slot the receiver already bound.
    ///
    /// The zero-RTT twin of [`attach_stream_anchor`](Self::attach_stream_anchor):
    /// the same tail with the `_anchor_attach` round trip cut out, because
    /// `ticket` already carries the answer that round trip existed to fetch.
    /// The receiver minted it with [`prebind_anchor`](Self::prebind_anchor);
    /// the application carried it here.
    ///
    /// The worker's first batch opens the pre-bound slot by the ticket's
    /// routing session id, which is the claim — bind-on-`OpenSlot` is how a mux
    /// bind has always been taken up, and nothing about that changes.
    ///
    /// `Ok` means the local egress slot is staged, not that the receiver has
    /// accepted the ticket: the `OpenSlot` this call stages is eager and
    /// unacked, the same as an ordinary attach's, so a ticket whose bind was
    /// already released or has expired still returns `Ok` here and only
    /// surfaces later, as a send failure on the returned `StreamSender`.
    ///
    /// Slot lifecycle signals reach the producer even before its first send.
    ///
    /// # Errors
    /// - [`AttachError::TransportError`] if `handle` names this node's own
    ///   worker id (a ticket is for a *different* worker to open; a
    ///   same-worker sender should use
    ///   [`attach_stream_anchor`](Self::attach_stream_anchor) instead), if
    ///   the ticket names `messenger-mux-v2` but this node has no mux
    ///   installed, if the ticket names a transport key this node cannot
    ///   resolve, or if staging the local slot fails after retrying a
    ///   batcher that keeps retiring underneath it.
    pub async fn open_anchor_stream<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        ticket: crate::streaming::control::StreamOpenTicket,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        // Mirrors `attach_stream_anchor`'s guard: `prebind_anchor` never mints
        // a ticket for an MPSC handle, so a ticket paired with one here can
        // only be a caller's mismatch, not a real zero-RTT slot to open.
        if handle.is_mpsc_stream() {
            return Err(AttachError::WrongHandleKind {
                handle,
                expected: crate::streaming::handle::AnchorKind::Spsc,
            });
        }
        // `prebind_anchor` only checks that `handle` names an anchor this
        // node owns -- it says nothing about which worker ends up opening
        // the ticket, so nothing upstream of here rules out this node
        // opening its own ticket. Unlike `attach_stream_anchor`'s same-worker
        // case, there is no co-located path to take instead: zero-RTT
        // deliberately never sets `attachment` for a claimed slot (see
        // `docs/src/concepts/batched-streaming.md`), so a co-located write here would have to
        // either invent a second claim representation or reuse `attachment`
        // with a meaning that depends on where the producer landed -- the
        // "two spellings of one rule" this crate's `CLAUDE.md` singles out.
        // Refusing outright also matches every real caller: a ticket exists
        // to travel to a *different* worker in the application's own request
        // envelope, which is what `prebind_anchor` and the README's example
        // both assume. Without this guard the call silently took the network
        // path instead: `open_anchor_stream` returned `Ok(sender)`, and the
        // very first `send` on it failed with `ChannelClosed` -- confirmed by
        // running it before this guard existed.
        let (handle_worker_id, _) = handle.unpack();
        if handle_worker_id == self.worker_id {
            return Err(AttachError::TransportError(anyhow::anyhow!(
                "open_anchor_stream: handle {handle} names this node's own worker id, so \
                 the ticket it carries was minted here too -- a ticket is for a *different* \
                 worker to open, the one the application's request envelope is addressed \
                 to. A same-worker sender should use attach_stream_anchor instead, whose \
                 co-located path writes directly into the anchor's channel"
            )));
        }
        self.open_stream_sender::<T>(handle, &ticket, self.new_sender_identity())
            .await
    }

    /// Allocate the sender-side identity one stream is opened under.
    fn new_sender_identity(&self) -> SenderIdentity {
        let sender_stream_id = self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel_token = CancellationToken::new();
        let entry = crate::streaming::control::SenderEntry {
            stop_token: cancel_token.child_token(),
            cancel_token,
            closed: Default::default(),
        };
        self.sender_registry
            .senders
            .insert(sender_stream_id, entry.clone());
        SenderIdentity {
            sender_stream_id,
            entry,
            registry: self.sender_registry.clone(),
            armed: true,
        }
    }

    /// Connect the transport a set of terms names, and register the sender.
    ///
    /// The tail both remote open paths share: everything after the terms are
    /// known, whether they arrived in an attach response or in a ticket. Kept
    /// as one body on purpose — the two differ only in how the terms were
    /// learned, and a fork here would be two copies of the credit handling, the
    /// cancel registration and the heartbeat cadence.
    async fn open_stream_sender<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        ticket: &crate::streaming::control::StreamOpenTicket,
        mut identity: SenderIdentity,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        let started = Instant::now();
        let (handle_worker_id, local_id) = handle.unpack();

        // Resolve the local FrameTransport that matches the remote worker's
        // bound streaming transport, then connect by WorkerId.
        let frame_tx = match self
            .connect_streaming(
                ticket,
                handle_worker_id,
                local_id,
                Some(identity.entry.clone()),
            )
            .await
        {
            Ok(frame_tx) => frame_tx,
            Err(err) => {
                self.record_streaming_operation(
                    StreamingOp::Open,
                    HandlerOutcome::Error,
                    ticket.streaming_transport_key.as_str(),
                    started,
                );
                return Err(err);
            }
        };

        let sender_stream_id = identity.sender_stream_id;
        let cancel_token = identity.entry.cancel_token.clone();
        identity.armed = false;

        // Build StreamSender: frame_tx from the transport (not a local registry
        // frame_tx). No local AnchorEntry is created for the remote anchor.
        let sender = crate::streaming::sender::StreamSender::new(
            frame_tx,
            handle,
            self.registry.clone(), // this worker's registry (no entry for this handle — correct)
            crate::streaming::sender::StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id,
                sender_registry: self.sender_registry.clone(),
                closed: identity.entry.closed.clone(),
            },
            Duration::from_millis(ticket.heartbeat_interval_ms),
            self.metrics.clone(),
            Some(ticket.streaming_transport_key.clone()),
        );
        self.record_streaming_operation(
            StreamingOp::Open,
            HandlerOutcome::Success,
            ticket.streaming_transport_key.as_str(),
            started,
        );
        Ok(sender)
    }

    /// Attach a sender to an existing anchor, establishing the transport connection.
    ///
    /// This is the primary sender-side entry point (API-05). It:
    /// 1. Detects remote handles (`handle.worker_id != self.worker_id`) and routes through
    ///    the remote attach path for cross-worker AM dispatch.
    /// 2. For local handles: validates the anchor exists and is unattached,
    ///    atomically marks the anchor as attached, and returns a
    ///    [`StreamSender<T>`](crate::streaming::sender::StreamSender) for pushing typed frames.
    ///
    /// The StreamSender writes to the entry's `frame_tx` so items flow directly
    /// to the [`StreamAnchor<T>`] consumer. The transport connection is used for
    /// cross-worker flows.
    ///
    /// # Errors
    /// - [`AttachError::AnchorNotFound`] if the handle is not in the registry (local path)
    /// - [`AttachError::AlreadyAttached`] if another sender is already connected, which
    ///   includes a pre-bound slot an `OpenSlot` has already claimed (local path)
    /// - [`AttachError::TransportError`] for all remote path errors (messenger unavailable,
    ///   AM send failed, remote error response)
    ///
    /// Over the mux, the consumer places the stream on the lane with the
    /// fewest streams from this worker;
    /// [`attach_stream_anchor_keyed`](Self::attach_stream_anchor_keyed) places
    /// it by a key instead.
    pub async fn attach_stream_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        self.attach_stream_anchor_on(handle, None).await
    }

    /// As [`attach_stream_anchor`](Self::attach_stream_anchor), with the
    /// stream placed on the mux lane `key` hashes to.
    ///
    /// The consumer hashes the key, with a hash fixed across builds and
    /// processes, over the lanes its transport keeps to this worker. One key
    /// therefore gives the same lane index on every consumer that keeps the
    /// same lane count, and with one lane every key is on lane 0. The key is a
    /// placement hint, not an ordering guarantee: the mux never orders records
    /// across streams, even on one lane. A local anchor has no lane and
    /// ignores the key.
    pub async fn attach_stream_anchor_keyed<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        self.attach_stream_anchor_on(handle, Some(key)).await
    }

    async fn attach_stream_anchor_on<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        lane_key: Option<u64>,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        // Fail fast if the caller passed an MPSC handle: the SPSC registry
        // will never contain it, and the remote path would waste an AM
        // round-trip to discover the same thing.
        if handle.is_mpsc_stream() {
            return Err(AttachError::WrongHandleKind {
                handle,
                expected: crate::streaming::handle::AnchorKind::Spsc,
            });
        }

        let (handle_worker_id, local_id) = handle.unpack();

        // Remote path: handle belongs to a different worker — send _anchor_attach AM
        if handle_worker_id != self.worker_id {
            return self.attach_remote::<T>(handle, lane_key).await;
        }

        // Step 1: Quick check anchor exists and is unattached (drop ref before async)
        {
            let entry = self.registry.get(&local_id);
            match entry {
                None => return Err(AttachError::AnchorNotFound { handle }),
                Some(e) if e.attachment || e.prebind_is_claimed() => {
                    return Err(AttachError::AlreadyAttached { handle });
                }
                _ => {} // looks good, proceed
            }
        } // DashMap ref dropped here

        // Step 2: Atomically set attachment under shard lock.
        // Re-check under the entry guard to prevent TOCTOU.
        //
        // The whole match evaluates to `(result, released_prebind)` rather
        // than returning `result` directly, so the block below can hoist a
        // released `PreBind` out before dropping it -- `PreBind::drop`
        // reaches into mux state (ingress registry, a batcher), and the two
        // other call sites that take one out from under this same shard lock
        // (`_anchor_detach`, `adopt_prebind`'s `Mismatch` arm) both drop it
        // only after the guard is gone, never while it is held.
        use dashmap::mapref::entry::Entry;
        let (result, released_prebind) = match self.registry.entry(local_id) {
            Entry::Vacant(_) => (Err(AttachError::AnchorNotFound { handle }), None),
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if entry.attachment || entry.prebind_is_claimed() {
                    (Err(AttachError::AlreadyAttached { handle }), None)
                } else {
                    // Clone the frame_tx so the StreamSender can write items
                    // directly to the StreamAnchor consumer.
                    let frame_tx = entry.frame_tx.clone();
                    // Snapshot the negotiated heartbeat cadence for the sender.
                    let heartbeat_interval = entry.heartbeat_interval;

                    // Mark as attached. The co-located sender needs no reader
                    // pump or feed: it writes into `frame_tx` itself.
                    entry.attachment = true;

                    // Cancel the timeout task while attached (pause timer)
                    if let Some(ref tc) = entry.timeout_cancel {
                        tc.cancel();
                    }

                    // A pre-bound slot on this anchor was minted for a sender
                    // on another worker. This one is on ours and writes
                    // straight into the anchor's channel, so that slot has no
                    // sender and never will: releasing it here is what keeps
                    // the accept window from holding a bind for a minute that
                    // nothing can claim.
                    //
                    // The guard above is a best-effort read: `prebind_is_claimed`
                    // resolves through an unlocked `OnceLock` the mux ingress
                    // writes from `open_slot`, under that peer's own mutex, not
                    // this registry's shard lock -- so a remote `OpenSlot` can
                    // still claim the bind in the gap between that read and the
                    // release below. That is not a leak: `retire_pump` a few
                    // lines down, before the prebind is taken, cancels the
                    // watchdog and withdraws the feed, so the consumer stops
                    // reading that slot buffer on its next poll (the test
                    // `a_retired_pump_withdraws_its_feed` pins this). A claim
                    // landing in the gap can still have a record read by a
                    // consumer poll that runs before the withdrawal; the
                    // reader pump this replaced forwarded such records until
                    // its own cancel landed, so the window is not new.
                    // `PreBind::drop` sees the claim and posts
                    // `CloseSlot{UnknownSlot}` to that producer, which is the
                    // true state of a stream whose consumer side just went
                    // away, delivered promptly rather than on the producer's
                    // next record. It is returned out of this block and
                    // dropped once the shard guard is gone -- `PreBind`'s drop
                    // reaches into mux state (the ingress registry, a
                    // batcher), which is exactly what
                    // the two other call sites that release one from under this
                    // same lock (`_anchor_detach`, `adopt_prebind`'s `Mismatch`
                    // arm) exist to avoid doing while holding it.
                    //
                    // Its watchdog and feed have to stop with it, and this
                    // branch starts no replacement — the co-located sender
                    // writes straight into `frame_tx` below, no transport in
                    // between. Left running, the watchdog would see the bind
                    // close once the released prebind drops, and
                    // `control::reap_unclaimed` would remove, for an
                    // *unclaimed* drain, the registry entry this co-located
                    // sender is about to become. `retire_pump` cancels the
                    // token the reap checks and withdraws the feed.
                    entry.retire_pump();
                    let released_prebind = entry.prebind.take();

                    // Allocate sender_stream_id and build SenderEntry
                    let sender_stream_id =
                        self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1;
                    let cancel_token = tokio_util::sync::CancellationToken::new();

                    let closed = crate::streaming::control::SenderClosed::default();
                    let sender_entry = crate::streaming::control::SenderEntry {
                        stop_token: cancel_token.child_token(),
                        cancel_token: cancel_token.clone(),
                        closed: closed.clone(),
                    };
                    if entry.stop_requested {
                        sender_entry.stop_token.cancel();
                    }
                    self.sender_registry
                        .senders
                        .insert(sender_stream_id, sender_entry);

                    // Store stream_cancel_handle in AnchorEntry (already under DashMap lock)
                    entry.stream_cancel_handle =
                        Some(crate::streaming::control::StreamCancelHandle::pack(
                            self.worker_id,
                            sender_stream_id,
                        ));

                    // Return sender with all new fields
                    let sender = crate::streaming::sender::StreamSender::new(
                        frame_tx,
                        handle,
                        self.registry.clone(),
                        crate::streaming::sender::StreamSenderCancelInfo {
                            cancel_token,
                            sender_stream_id,
                            sender_registry: self.sender_registry.clone(),
                            closed,
                        },
                        heartbeat_interval,
                        self.metrics.clone(),
                        // Same worker: the frames go straight into the anchor's
                        // channel, so there was no transport to negotiate.
                        None,
                    );
                    (Ok(sender), released_prebind)
                }
            }
        };
        drop(released_prebind);
        result
    }

    // -----------------------------------------------------------------------
    // MPSC anchor API
    // -----------------------------------------------------------------------

    /// Create a new MPSC anchor using only manager-level defaults.
    ///
    /// See [`AnchorManager::create_mpsc_anchor_with_config`] for per-anchor
    /// overrides (channel capacity, unattached timeout, heartbeat cadence,
    /// `max_senders`).
    pub fn create_mpsc_anchor<T>(&self) -> crate::streaming::mpsc::MpscStreamAnchor<T> {
        self.create_mpsc_anchor_with_config(crate::streaming::mpsc::MpscAnchorConfig::default())
    }

    /// Create a new MPSC anchor with per-anchor config overrides.
    ///
    /// Shares `next_local_id` with the SPSC registry so handles are unique
    /// across both kinds; the two DashMaps never see the same key.
    pub fn create_mpsc_anchor_with_config<T>(
        &self,
        config: crate::streaming::mpsc::MpscAnchorConfig,
    ) -> crate::streaming::mpsc::MpscStreamAnchor<T> {
        // Raw 63-bit counter; the MPSC discriminator bit is applied at handle
        // pack time (and is stored with the entry in `mpsc_registry` so
        // registry keys match `handle.unpack().1` exactly).
        let raw_local = self.next_local_id.fetch_add(1, Ordering::Relaxed) + 1;
        let handle = StreamAnchorHandle::pack_mpsc(self.worker_id, raw_local);
        let (_, local_id) = handle.unpack();

        let capacity = config.channel_capacity.unwrap_or(256);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(capacity);
        let cancel_token = CancellationToken::new();

        let unattached_timeout = config
            .unattached_timeout
            .or(self.default_unattached_timeout);
        let heartbeat_interval = config
            .heartbeat_interval
            .unwrap_or(self.default_heartbeat_interval);

        let timeout_cancel = unattached_timeout.map(|timeout| {
            crate::streaming::mpsc::anchor::spawn_mpsc_timeout_task(
                self.mpsc_registry.clone(),
                local_id,
                timeout,
                &cancel_token,
            )
        });

        let entry = crate::streaming::mpsc::anchor::MpscAnchorEntry {
            frame_tx,
            cancel_token,
            senders: HashMap::new(),
            next_sender_id: 1,
            unattached_timeout,
            timeout_cancel,
            heartbeat_interval,
            max_senders: config.max_senders,
        };

        self.mpsc_registry.insert(local_id, entry);

        crate::streaming::mpsc::MpscStreamAnchor::new(
            handle,
            frame_rx,
            local_id,
            self.anchor_context(),
            self.sender_registry.clone(),
            self.messenger.clone(),
        )
    }

    /// Attach a sender to an MPSC anchor. Like [`Self::attach_stream_anchor`] but
    /// targets the MPSC registry: multiple senders may attach concurrently,
    /// and each attach allocates a fresh [`crate::streaming::mpsc::SenderId`].
    ///
    /// Over the mux, the consumer places the sender on the lane with the fewest
    /// streams from this worker;
    /// [`attach_mpsc_stream_anchor_keyed`](Self::attach_mpsc_stream_anchor_keyed)
    /// places it by a key instead.
    pub async fn attach_mpsc_stream_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
    ) -> Result<crate::streaming::mpsc::MpscStreamSender<T>, AttachError> {
        self.attach_mpsc_stream_anchor_on(handle, None).await
    }

    /// As [`attach_mpsc_stream_anchor`](Self::attach_mpsc_stream_anchor), with
    /// the sender placed on the mux lane `key` hashes to. See
    /// [`attach_stream_anchor_keyed`](Self::attach_stream_anchor_keyed).
    pub async fn attach_mpsc_stream_anchor_keyed<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        key: u64,
    ) -> Result<crate::streaming::mpsc::MpscStreamSender<T>, AttachError> {
        self.attach_mpsc_stream_anchor_on(handle, Some(key)).await
    }

    async fn attach_mpsc_stream_anchor_on<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        lane_key: Option<u64>,
    ) -> Result<crate::streaming::mpsc::MpscStreamSender<T>, AttachError> {
        // Fail fast if the caller passed an SPSC handle.
        if handle.is_spsc_stream() {
            return Err(AttachError::WrongHandleKind {
                handle,
                expected: crate::streaming::handle::AnchorKind::Mpsc,
            });
        }

        let (handle_worker_id, local_id) = handle.unpack();

        if handle_worker_id != self.worker_id {
            return self.attach_mpsc_remote::<T>(handle, lane_key).await;
        }

        // Local path: reserve a slot under the shard lock, then construct
        // the sender after the lock is released.
        use dashmap::mapref::entry::Entry;
        let mut identity = self.new_sender_identity();
        let (sender_id, frame_tx, heartbeat_interval) = match self.mpsc_registry.entry(local_id) {
            Entry::Vacant(_) => return Err(AttachError::AnchorNotFound { handle }),
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if let Some(limit) = entry.max_senders
                    && entry.senders.len() >= limit
                {
                    return Err(AttachError::MaxSendersReached { handle, limit });
                }

                let sender_id = entry.next_sender_id;
                entry.next_sender_id += 1;

                // Pause the unattached timeout the moment we have a sender.
                if let Some(ref tc) = entry.timeout_cancel {
                    tc.cancel();
                }
                entry.timeout_cancel = None;

                let frame_tx = entry.frame_tx.clone();
                let heartbeat_interval = entry.heartbeat_interval;

                let slot = crate::streaming::mpsc::anchor::MpscSenderSlot {
                    pump_token: None,
                    stream_cancel_handle: Some(
                        crate::streaming::control::StreamCancelHandle::pack(
                            self.worker_id,
                            identity.sender_stream_id,
                        ),
                    ),
                };
                entry.senders.insert(sender_id, slot);

                (sender_id, frame_tx, heartbeat_interval)
            }
        };

        identity.armed = false;
        Ok(crate::streaming::mpsc::MpscStreamSender::new(
            crate::streaming::mpsc::SenderId(sender_id),
            crate::streaming::mpsc::sender::SenderChannel::Local(frame_tx),
            handle,
            self.mpsc_registry.clone(),
            crate::streaming::sender::StreamSenderCancelInfo {
                cancel_token: identity.entry.cancel_token.clone(),
                sender_stream_id: identity.sender_stream_id,
                sender_registry: self.sender_registry.clone(),
                closed: identity.entry.closed.clone(),
            },
            heartbeat_interval,
            self.metrics.clone(),
        ))
    }

    async fn attach_mpsc_remote<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        lane_key: Option<u64>,
    ) -> Result<crate::streaming::mpsc::MpscStreamSender<T>, AttachError> {
        let (handle_worker_id, _) = handle.unpack();

        let messenger = self.messenger_lock.get().ok_or_else(|| {
            AttachError::TransportError(anyhow::anyhow!(
                "register_handlers not called — messenger unavailable for remote mpsc attach"
            ))
        })?;

        let mut identity = self.new_sender_identity();
        let sender_stream_id = identity.sender_stream_id;
        let stream_cancel_handle =
            crate::streaming::control::StreamCancelHandle::pack(self.worker_id, sender_stream_id);

        let req = crate::streaming::mpsc::control::MpscAnchorAttachRequest {
            handle,
            session_id: sender_stream_id,
            stream_cancel_handle,
            supported_transport_keys: self.supported_transport_keys(),
            lane_key,
        };

        // Same bracket as the SPSC path above, through the same helper: the
        // timer covers the send and nothing on either side of it.
        let request = messenger
            .typed_unary_streaming::<crate::streaming::mpsc::control::MpscAnchorAttachResponse>(
                "_mpsc_anchor_attach",
            )
            .payload(&req)
            .map_err(AttachError::TransportError)?
            .worker(handle_worker_id);
        let started = Instant::now();
        let response: crate::streaming::mpsc::control::MpscAnchorAttachResponse =
            match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    self.record_attach_rtt(started, HandlerOutcome::Error, "unknown");
                    return Err(AttachError::TransportError(error));
                }
            };

        match response {
            crate::streaming::mpsc::control::MpscAnchorAttachResponse::Ok {
                streaming_transport_key,
                heartbeat_interval_ms,
                sender_id,
                routing_session_id,
                initial_credit,
                slot_byte_budget,
                lane,
            } => {
                self.record_attach_rtt(
                    started,
                    HandlerOutcome::Success,
                    Self::attach_scheme_label(
                        &req.supported_transport_keys,
                        &streaming_transport_key,
                    ),
                );
                let (_, local_id) = handle.unpack();
                // See the SPSC remote attach above for routing_session_id
                // rationale and the legacy-zero fallback.
                let connect_session_id = if routing_session_id != 0 {
                    routing_session_id
                } else {
                    sender_stream_id
                };
                let frame_tx = self
                    .connect_streaming(
                        &crate::streaming::control::StreamOpenTicket {
                            streaming_transport_key: streaming_transport_key.clone(),
                            heartbeat_interval_ms,
                            routing_session_id: connect_session_id,
                            initial_credit,
                            slot_byte_budget,
                            lane,
                        },
                        handle_worker_id,
                        local_id,
                        Some(identity.entry.clone()),
                    )
                    .await?;

                identity.armed = false;
                Ok(crate::streaming::mpsc::MpscStreamSender::new(
                    crate::streaming::mpsc::SenderId(sender_id),
                    crate::streaming::mpsc::sender::SenderChannel::Remote(frame_tx),
                    handle,
                    self.mpsc_registry.clone(),
                    crate::streaming::sender::StreamSenderCancelInfo {
                        cancel_token: identity.entry.cancel_token.clone(),
                        sender_stream_id,
                        sender_registry: self.sender_registry.clone(),
                        closed: identity.entry.closed.clone(),
                    },
                    Duration::from_millis(heartbeat_interval_ms),
                    self.metrics.clone(),
                ))
            }
            crate::streaming::mpsc::control::MpscAnchorAttachResponse::Err { reason } => {
                self.record_attach_rtt(started, HandlerOutcome::Error, "unknown");
                Err(AttachError::TransportError(anyhow::anyhow!("{}", reason)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::frame::{StreamError, StreamFrame};
    use anyhow::Result as AnyhowResult;
    use futures::StreamExt;
    use futures::future::BoxFuture;
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Mock transport for unit tests
    // -----------------------------------------------------------------------

    struct MockTransport;

    impl crate::streaming::transport::FrameTransport for MockTransport {
        fn key(&self) -> velo_ext::TransportKey {
            velo_ext::TransportKey::new("mock-stream")
        }

        fn address(&self) -> velo_ext::WorkerAddress {
            velo_ext::WorkerAddress::empty()
        }

        fn bind(
            &self,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Receiver<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).1) })
        }

        fn connect(
            &self,
            _peer: velo_ext::WorkerId,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Sender<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).0) })
        }
    }

    /// A deferred local exit must release its channel when the consumer
    /// cancels, even if the consumer retains the full anchor queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn off_runtime_mpsc_drop_stops_waiting_when_the_consumer_cancels() {
        let manager = make_manager();
        let anchor = manager.create_mpsc_anchor_with_config::<u32>(
            crate::streaming::mpsc::MpscAnchorConfig {
                channel_capacity: Some(1),
                unattached_timeout: Some(Duration::from_secs(3600)),
                ..Default::default()
            },
        );
        let handle = anchor.handle();
        let frame_tx = manager
            .mpsc_registry
            .get(&handle.unpack().1)
            .unwrap()
            .frame_tx
            .clone();
        let sender = manager
            .attach_mpsc_stream_anchor::<u32>(handle)
            .await
            .unwrap();
        sender.send(1).await.unwrap();
        let (done, finished) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            drop(sender);
            let _ = done.send(());
        });
        if tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .is_err()
        {
            // Release a blocking old implementation before failing the test.
            drop(anchor);
            thread.join().unwrap();
            panic!("drop blocked on the full queue");
        }
        thread.join().unwrap();
        assert!(manager.sender_registry.senders.is_empty());
        assert!(
            manager
                .mpsc_registry
                .get(&handle.unpack().1)
                .unwrap()
                .senders
                .is_empty()
        );
        anchor.controller().cancel();
        tokio::time::timeout(Duration::from_secs(2), async {
            while frame_tx.sender_count() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deferred exit retained the cancelled anchor channel");
        drop(anchor);
    }

    /// `velo_streaming_active_anchors` reads the registries when scraped, so
    /// it follows creates and retires without either touching the gauge.
    #[tokio::test]
    async fn the_active_anchor_gauge_follows_the_registries_when_scraped() {
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
        let mgr = AnchorManagerBuilder::default()
            .worker_id(velo_ext::WorkerId::from_u64(1))
            .transport(
                Arc::new(MockTransport) as Arc<dyn crate::streaming::transport::FrameTransport>
            )
            .metrics(Some(metrics))
            .build()
            .unwrap();
        let gauge = || {
            crate::observability::test_helpers::MetricSnapshot::from_registry(&registry)
                .gauge("velo_streaming_active_anchors", &[])
        };
        assert_eq!(gauge(), 0.0);
        let spsc = mgr.create_anchor::<u32>();
        let mpsc = mgr.create_mpsc_anchor::<u32>();
        assert_eq!(gauge(), 2.0, "one SPSC and one MPSC anchor");
        spsc.cancel();
        assert_eq!(gauge(), 1.0);
        drop(mpsc);
        drop(mgr);
        assert_eq!(gauge(), 0.0, "a dropped manager counts nothing");
    }

    /// The gauge counts each registry on its own.
    ///
    /// A `StreamAnchor` holds the SPSC registry and not the MPSC one, so the
    /// manager can be gone while SPSC anchors still drain. A source that gave
    /// up when either registry was gone would be pruned then, for good, and
    /// the gauge would read zero with anchors alive.
    #[tokio::test]
    async fn the_active_anchor_gauge_outlives_one_registry() {
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
        let mgr = AnchorManagerBuilder::default()
            .worker_id(velo_ext::WorkerId::from_u64(1))
            .transport(
                Arc::new(MockTransport) as Arc<dyn crate::streaming::transport::FrameTransport>
            )
            .metrics(Some(metrics))
            .build()
            .unwrap();
        let gauge = || {
            crate::observability::test_helpers::MetricSnapshot::from_registry(&registry)
                .gauge("velo_streaming_active_anchors", &[])
        };
        let spsc = mgr.create_anchor::<u32>();
        drop(mgr);
        assert_eq!(
            gauge(),
            1.0,
            "the MPSC registry went with the manager; the SPSC anchor still counts"
        );
        drop(spsc);
        assert_eq!(gauge(), 0.0);
    }

    /// A consumer that reads its sender's `Finalized` off the slot buffer marks
    /// the slot released.
    ///
    /// The ingress applied that terminal in the same step that retires the
    /// slot, and its own release lands a moment after the consumer can read
    /// the terminal. Marking it here is what keeps an ordinary end from taking
    /// the ingress lock in that gap. The drain here belongs to no mux, so
    /// nothing else ever marks it: only the consumer can.
    #[tokio::test]
    async fn reading_finalized_off_the_feed_marks_the_slot_released() {
        let mgr = make_manager();
        let mut anchor = mgr.create_anchor::<u32>();
        let (_, local_id) = anchor.handle().unpack();
        let (tx, rx) = flume::bounded::<Vec<u8>>(4);
        let (wake, _wake_rx) = flume::unbounded();
        let drain = Arc::new(crate::streaming::messenger_mux::ingress::DrainSignal::new(
            wake,
        ));
        {
            let mut entry = mgr.registry.get_mut(&local_id).expect("entry");
            let _installed = crate::streaming::control::install_direct_feed(
                &mut entry,
                crate::streaming::control::DirectFeed {
                    rx,
                    drain: Arc::clone(&drain),
                    pump_token: CancellationToken::new(),
                    release: None,
                },
            );
        }
        tx.send(crate::streaming::sender::cached_finalized().clone())
            .unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), anchor.next())
            .await
            .expect("timed out waiting for Finalized")
            .expect("stream ended early")
            .expect("frame decodes");
        assert!(matches!(frame, StreamFrame::Finalized));
        assert!(
            drain.is_released(),
            "the consumer read the terminal off the buffer, so the slot is released"
        );
    }

    fn make_manager() -> AnchorManager {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport = Arc::new(MockTransport);
        AnchorManager::new(worker_id, transport)
    }

    // -----------------------------------------------------------------------
    // Test 1: Monotonic local IDs starting at 1
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_anchor_monotonic_ids() {
        let mgr = make_manager();

        let a1 = mgr.create_anchor::<u8>();
        let a2 = mgr.create_anchor::<u8>();
        let a3 = mgr.create_anchor::<u8>();

        let (_, id1) = a1.handle().unpack();
        let (_, id2) = a2.handle().unpack();
        let (_, id3) = a3.handle().unpack();

        assert_eq!(id1, 1, "first local_id must be 1");
        assert_eq!(id2, 2, "second local_id must be 2");
        assert_eq!(id3, 3, "third local_id must be 3");
    }

    // -----------------------------------------------------------------------
    // Test 2: Registry contains entry after create_anchor
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_anchor_registry_insert() {
        let mgr = make_manager();

        let anchor = mgr.create_anchor::<u8>();
        let (_, local_id) = anchor.handle().unpack();

        assert!(
            mgr.registry.contains_key(&local_id),
            "entry must be present in registry after create_anchor"
        );
    }

    // -----------------------------------------------------------------------
    // Test 3: Exclusive attach -- second attach while attached returns AlreadyAttached
    // -----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn test_exclusive_attach() {
        let mgr = make_manager();
        let mut anchor = mgr.create_anchor::<u8>();
        anchor.set_timeout(Some(Duration::from_secs(1)));
        let handle = anchor.handle();
        let sender = mgr.attach_stream_anchor::<u8>(handle).await.unwrap();
        assert!(matches!(
            mgr.attach_stream_anchor::<u8>(handle).await,
            Err(AttachError::AlreadyAttached { .. })
        ));
        sender.detach().unwrap();
        let sender = mgr.attach_stream_anchor::<u8>(handle).await.unwrap();
        assert!(matches!(
            anchor.next().await,
            Some(Ok(StreamFrame::Detached))
        ));
        // Reading the prior sender's Detached must not expire this attachment.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(matches!(
            mgr.attach_stream_anchor::<u8>(handle).await,
            Err(AttachError::AlreadyAttached { .. })
        ));
        sender.send(42).await.unwrap();
        assert!(matches!(
            anchor.next().await,
            Some(Ok(StreamFrame::Item(42)))
        ));
    }

    /// A sender may be moved to a thread with no runtime (a language
    /// binding's thread). Detaching there must still arm the unattached
    /// timeout; otherwise an anchor nobody re-attaches is never reaped.
    #[tokio::test(flavor = "multi_thread")]
    async fn detach_from_plain_thread_arms_unattached_timeout() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u8>();
        anchor.set_timeout(Some(Duration::from_millis(100)));
        let handle = anchor.handle();
        let (_, local_id) = handle.unpack();
        let sender = mgr.attach_stream_anchor::<u8>(handle).await.unwrap();
        std::thread::spawn(move || sender.detach().unwrap())
            .join()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while mgr.registry.contains_key(&local_id) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("detached anchor was never reaped");
        drop(anchor);
    }

    // -----------------------------------------------------------------------
    // Test 4: CancellationToken is idempotent across multiple cancel() calls
    // -----------------------------------------------------------------------

    #[test]
    fn test_cancel_token_idempotent() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u8>();
        let (_, local_id) = anchor.handle().unpack();

        // Retrieve a clone of the token before removing the entry.
        let token = mgr
            .registry
            .get(&local_id)
            .map(|e| e.cancel_token.clone())
            .expect("entry must exist");

        // First cancel -- should not panic.
        token.cancel();
        assert!(
            token.is_cancelled(),
            "token must be cancelled after first cancel()"
        );

        // Second cancel -- must not panic and must still report cancelled.
        token.cancel();
        assert!(
            token.is_cancelled(),
            "token must still be cancelled after second cancel()"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5: remove_anchor removes the entry from the registry
    // -----------------------------------------------------------------------

    #[test]
    fn test_registry_cleanup() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u8>();
        let (_, local_id) = anchor.handle().unpack();

        assert!(
            mgr.registry.contains_key(&local_id),
            "entry must exist before cleanup"
        );

        let removed = mgr.remove_anchor(local_id);
        assert!(removed.is_some(), "remove_anchor must return the entry");
        assert!(
            !mgr.registry.contains_key(&local_id),
            "entry must be absent after remove_anchor"
        );
    }

    // -----------------------------------------------------------------------
    // attach_stream_anchor tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_attach_stream_anchor_success() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();

        let result = mgr.attach_stream_anchor::<u32>(handle).await;

        assert!(
            result.is_ok(),
            "attach_stream_anchor should succeed: {:?}",
            result.err()
        );

        // The returned StreamSender should be usable
        let sender = result.unwrap();
        sender.finalize().expect("finalize should succeed");
    }

    #[tokio::test]
    async fn test_attach_stream_anchor_not_found() {
        let mgr = make_manager();
        // Create a handle for a non-existent anchor
        let fake_handle = crate::streaming::handle::StreamAnchorHandle::pack(
            velo_ext::WorkerId::from_u64(42),
            999,
        );

        let result = mgr.attach_stream_anchor::<u32>(fake_handle).await;

        match result {
            Err(AttachError::AnchorNotFound { .. }) => {}
            other => panic!("expected AnchorNotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_attach_stream_anchor_already_attached() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();

        // First attach should succeed
        let sender1 = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("first attach should succeed");

        // Second attach should fail with AlreadyAttached
        let result = mgr.attach_stream_anchor::<u32>(handle).await;

        match result {
            Err(AttachError::AlreadyAttached { .. }) => {}
            other => panic!("expected AlreadyAttached, got {:?}", other),
        }

        drop(sender1);
    }

    #[tokio::test]
    async fn test_attach_stream_anchor_sender_can_send() {
        let mgr = make_manager();
        let mut anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();

        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("attach should succeed");

        // Send an item through the StreamSender
        sender.send(42u32).await.expect("send should succeed");

        // The item should arrive via the Stream interface
        let result = anchor.next().await;
        match result {
            Some(Ok(StreamFrame::Item(val))) => assert_eq!(val, 42),
            other => panic!("expected Item(42), got {:?}", other),
        }

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // StreamAnchor<T> Stream impl tests
    // -----------------------------------------------------------------------

    /// Helper: create a raw channel pair + StreamAnchor for testing Stream impl.
    /// Returns (sender for pushing raw bytes, StreamAnchor<T>).
    fn make_test_stream<T>() -> (flume::Sender<Vec<u8>>, StreamAnchor<T>) {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<T>();
        let (_, local_id) = anchor.handle().unpack();
        // Get the frame_tx from the registry for pushing raw bytes
        let frame_tx = mgr
            .registry
            .get(&local_id)
            .map(|e| e.frame_tx.clone())
            .expect("entry must exist");
        (frame_tx, anchor)
    }

    #[tokio::test]
    async fn test_stream_yields_item() {
        let (tx, mut stream) = make_test_stream::<u32>();

        // Send serialized Item frame
        let bytes = rmp_serde::to_vec(&StreamFrame::Item(42u32)).unwrap();
        tx.send(bytes).unwrap();

        let result = stream.next().await;
        match result {
            Some(Ok(StreamFrame::Item(val))) => assert_eq!(val, 42),
            other => panic!("expected Some(Ok(Item(42))), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stream_yields_sender_error_and_continues() {
        let (tx, mut stream) = make_test_stream::<u32>();

        // Send SenderError
        let err_bytes =
            rmp_serde::to_vec(&StreamFrame::<u32>::SenderError("oops".to_string())).unwrap();
        tx.send(err_bytes).unwrap();

        // Should yield Err(StreamError::SenderError)
        let result = stream.next().await;
        match result {
            Some(Err(StreamError::SenderError(msg))) => assert_eq!(msg, "oops"),
            other => panic!("expected SenderError, got {:?}", other),
        }

        // Stream should continue -- send another item
        let item_bytes = rmp_serde::to_vec(&StreamFrame::Item(99u32)).unwrap();
        tx.send(item_bytes).unwrap();

        let result2 = stream.next().await;
        match result2 {
            Some(Ok(StreamFrame::Item(val))) => assert_eq!(val, 99),
            other => panic!("expected Item(99) after SenderError, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stream_finalized_then_none() {
        let (tx, mut stream) = make_test_stream::<u32>();

        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Finalized).unwrap();
        tx.send(bytes).unwrap();

        // Should yield Ok(Finalized)
        let result = stream.next().await;
        assert!(
            matches!(result, Some(Ok(StreamFrame::Finalized))),
            "expected Finalized, got {:?}",
            result
        );

        // Next call should yield None
        let result2 = stream.next().await;
        assert!(
            result2.is_none(),
            "expected None after Finalized, got {:?}",
            result2
        );
    }

    #[tokio::test]
    async fn test_stream_detached_then_none() {
        // Detached is non-terminal — a new sender may reattach.
        // After Detached, the stream continues polling. Simulate no reattach
        // by sending Dropped, which IS terminal.
        let (tx, mut stream) = make_test_stream::<u32>();

        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Detached).unwrap();
        tx.send(bytes).unwrap();

        let result = stream.next().await;
        assert!(
            matches!(result, Some(Ok(StreamFrame::Detached))),
            "expected Detached, got {:?}",
            result
        );

        // Send Dropped to signal no reattach — terminal sentinel.
        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Dropped).unwrap();
        tx.send(bytes).unwrap();

        let result2 = stream.next().await;
        assert!(
            matches!(result2, Some(Err(StreamError::SenderDropped))),
            "expected SenderDropped after Detached, got {:?}",
            result2
        );

        let result3 = stream.next().await;
        assert!(
            result3.is_none(),
            "expected None after SenderDropped, got {:?}",
            result3
        );
    }

    #[test]
    fn test_detached_arm_off_runtime_does_not_panic() {
        // The `Detached` arm's `released.is_some()` branch spawns a timeout
        // task, but `poll_next` runs under whatever executor the *consumer*
        // chose -- `StreamAnchor`'s inner stream is a plain
        // `flume::r#async::RecvStream`, executor-agnostic, with no tokio
        // requirement documented anywhere on the consumer side. A bare
        // `tokio::spawn` reachable from here therefore panics mid-poll on a
        // thread with no runtime under it, exactly the hazard
        // `close_claimed_slot` and `StreamController::cancel` already guard
        // against for the same reason.
        //
        // Setup needs a runtime of its own (`create_anchor_with_config`'s
        // `spawn_timeout_task` call does too), so build one, use it, and
        // drop it before polling on a bare `std::thread`.
        let setup_rt = tokio::runtime::Runtime::new().expect("build setup runtime");
        let (frame_tx, mut anchor) = setup_rt.block_on(async {
            let mgr = make_manager();
            let anchor = mgr.create_anchor_with_config::<u32>(AnchorConfig {
                unattached_timeout: Some(Duration::from_secs(30)),
                heartbeat_interval: None,
            });
            let (_, local_id) = anchor.handle().unpack();
            let frame_tx = mgr
                .registry
                .get(&local_id)
                .map(|e| e.frame_tx.clone())
                .expect("entry must exist");

            // Give the anchor a *claimed* pre-bind -- the only shape that
            // reaches `released.is_some()` in the Detached arm: a zero-RTT
            // producer that called `StreamSender::detach()` after its
            // `OpenSlot` claimed the pre-bind.
            let (wake_tx, _wake_rx) = flume::bounded(1);
            let drain = Arc::new(crate::streaming::messenger_mux::ingress::DrainSignal::new(
                wake_tx,
            ));
            drain.claimed_by(
                crate::streaming::messenger_mux::PeerLane::new(
                    velo_ext::WorkerId::from_u64(7),
                    crate::streaming::messenger_mux::LaneIndex::ZERO,
                ),
                crate::streaming::messenger_mux::protocol::SlotId::from_raw(0),
                Arc::new(AtomicBool::new(false)),
                Arc::new(crate::streaming::messenger_mux::ingress::DirtySlots::new()),
            );
            let ticket = crate::streaming::control::StreamOpenTicket {
                streaming_transport_key: velo_ext::TransportKey::new("mock-stream"),
                heartbeat_interval_ms: 5000,
                routing_session_id: 1,
                initial_credit: 1,
                slot_byte_budget: 1,
                lane: 0,
            };
            let prebind = PreBind {
                anchor_id: local_id,
                ticket,
                drain,
                // Dangling on purpose: `PreBind::drop`'s `mux.upgrade()`
                // then returns `None` and does nothing, which is exactly
                // right with no real mux transport under this unit test.
                mux: std::sync::Weak::new(),
                prebound: Arc::new(AtomicBool::new(false)),
            };
            if let Some(mut entry) = mgr.registry.get_mut(&local_id) {
                entry.prebind = Some(prebind);
            }

            (frame_tx, anchor)
        });
        drop(setup_rt);

        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Detached).unwrap();
        frame_tx.send(bytes).expect("send Detached frame");

        // The hazard itself: poll from a thread with no tokio runtime.
        let result = std::thread::spawn(move || futures::executor::block_on(anchor.next()))
            .join()
            .expect("poll_next must not panic off-runtime");

        assert!(
            matches!(result, Some(Ok(StreamFrame::Detached))),
            "expected Detached, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_stream_dropped_then_none() {
        let (tx, mut stream) = make_test_stream::<u32>();

        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Dropped).unwrap();
        tx.send(bytes).unwrap();

        let result = stream.next().await;
        match result {
            Some(Err(StreamError::SenderDropped)) => {}
            other => panic!("expected SenderDropped, got {:?}", other),
        }

        let result2 = stream.next().await;
        assert!(
            result2.is_none(),
            "expected None after Dropped, got {:?}",
            result2
        );
    }

    #[tokio::test]
    async fn test_stream_transport_error_then_none() {
        let (tx, mut stream) = make_test_stream::<u32>();

        let bytes = rmp_serde::to_vec(&StreamFrame::<u32>::TransportError(
            "conn reset".to_string(),
        ))
        .unwrap();
        tx.send(bytes).unwrap();

        let result = stream.next().await;
        match result {
            Some(Err(StreamError::TransportError(msg))) => assert_eq!(msg, "conn reset"),
            other => panic!("expected TransportError, got {:?}", other),
        }

        let result2 = stream.next().await;
        assert!(
            result2.is_none(),
            "expected None after TransportError, got {:?}",
            result2
        );
    }

    #[tokio::test]
    async fn test_stream_filters_heartbeat() {
        let (tx, mut stream) = make_test_stream::<u32>();

        // Send heartbeat then an item
        let hb_bytes = rmp_serde::to_vec(&StreamFrame::<u32>::Heartbeat).unwrap();
        tx.send(hb_bytes).unwrap();

        let item_bytes = rmp_serde::to_vec(&StreamFrame::Item(7u32)).unwrap();
        tx.send(item_bytes).unwrap();

        // Consumer should never see Heartbeat -- should get Item directly
        let result = stream.next().await;
        match result {
            Some(Ok(StreamFrame::Item(val))) => assert_eq!(val, 7),
            other => panic!("expected Item(7) (heartbeat filtered), got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_stream_deserialization_error_then_none() {
        let (tx, mut stream) = make_test_stream::<u32>();

        // Send invalid bytes
        tx.send(vec![0xFF, 0xFE, 0xFD]).unwrap();

        let result = stream.next().await;
        match result {
            Some(Err(StreamError::DeserializationError(_))) => {}
            other => panic!("expected DeserializationError, got {:?}", other),
        }

        let result2 = stream.next().await;
        assert!(
            result2.is_none(),
            "expected None after DeserializationError, got {:?}",
            result2
        );
    }

    #[tokio::test]
    async fn test_stream_none_when_sender_dropped() {
        let mgr = make_manager();
        let mut stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        // Remove the anchor from the registry to drop the frame_tx sender,
        // then drop the returned entry so ALL senders are gone.
        let entry = mgr.remove_anchor(local_id);
        drop(entry); // drops frame_tx -> channel closes

        let result = stream.next().await;
        assert!(
            result.is_none(),
            "expected None when channel sender dropped, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_cancel_removes_anchor_from_registry() {
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist before cancel"
        );

        // cancel(self) consumes the stream and removes anchor from registry
        stream.cancel();

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after cancel(self)"
        );
    }

    // -----------------------------------------------------------------------
    // AnchorManagerBuilder + default_unattached_timeout tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_creates_manager_no_timeout() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .build()
            .expect("builder with required fields should succeed");
        assert!(
            mgr.default_unattached_timeout.is_none(),
            "default_unattached_timeout must be None when not set"
        );
    }

    #[test]
    fn test_builder_creates_manager_with_timeout() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("builder with timeout should succeed");
        assert_eq!(
            mgr.default_unattached_timeout,
            Some(std::time::Duration::from_secs(10)),
            "default_unattached_timeout must match configured value"
        );
    }

    #[test]
    fn test_convenience_new_still_works() {
        // AnchorManager::new must still compile and create a manager with no timeout
        let mgr = make_manager();
        assert!(
            mgr.default_unattached_timeout.is_none(),
            "AnchorManager::new must produce None default_unattached_timeout"
        );
    }

    #[tokio::test]
    async fn test_timeout_removes_unattached_anchor() {
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(1))
            .build()
            .expect("builder should succeed");

        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();
        let (_, local_id) = handle.unpack();

        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist after create"
        );

        // Advance past the timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after timeout expires"
        );
    }

    #[tokio::test]
    async fn test_expired_anchor_returns_not_found() {
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(1))
            .build()
            .expect("builder should succeed");

        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();

        // Advance past timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Try to attach -- should get AnchorNotFound
        let result = mgr.attach_stream_anchor::<u32>(handle).await;
        match result {
            Err(AttachError::AnchorNotFound { .. }) => {}
            other => panic!("expected AnchorNotFound after timeout, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_timeout_pauses_on_attach_resumes_on_detach() {
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("builder should succeed");

        let mut anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();
        let (_, local_id) = handle.unpack();

        // Advance 1s (less than 2s timeout)
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist before timeout"
        );

        // Attach -- should cancel the timeout task
        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("attach should succeed");

        // Advance well past the original deadline
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must still exist while attached (timeout paused)"
        );

        // Detach -- should respawn the timeout task
        sender.detach().expect("detach");
        assert!(matches!(
            anchor.next().await,
            Some(Ok(StreamFrame::Detached))
        ));

        // Advance past the new timeout (2s from detach)
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after detach + timeout"
        );
    }

    // -----------------------------------------------------------------------
    // StreamAnchor::set_timeout tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_set_timeout_starts_timeout_on_no_default() {
        tokio::time::pause();

        // Manager with NO default timeout
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        // set_timeout starts a timeout task even though manager had no default
        stream.set_timeout(Some(std::time::Duration::from_secs(1)));

        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist before timeout"
        );

        // Advance past the timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after set_timeout expires"
        );
    }

    #[tokio::test]
    async fn test_set_timeout_none_disables_timeout() {
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(2))
            .build()
            .expect("builder should succeed");

        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        // Disable the timeout
        stream.set_timeout(None);

        // Advance well past the original deadline
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must still exist after disabling timeout"
        );
    }

    #[tokio::test]
    async fn test_set_timeout_overrides_default() {
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("builder should succeed");

        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        // Override with a shorter timeout
        stream.set_timeout(Some(std::time::Duration::from_secs(1)));

        // Advance 2s -- should trigger the 1s override, not the 10s default
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed by overridden 1s timeout, not waiting for 10s default"
        );
    }

    #[tokio::test]
    async fn test_set_timeout_while_attached_no_immediate_effect() {
        tokio::time::pause();

        let mgr = make_manager();
        let mut stream = mgr.create_anchor::<u32>();
        let handle = stream.handle();
        let (_, local_id) = handle.unpack();

        // Attach the anchor
        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("attach should succeed");

        // Set a timeout while attached -- should NOT spawn a task immediately
        stream.set_timeout(Some(std::time::Duration::from_secs(1)));

        // Advance well past the timeout
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Anchor must still exist (attached, timeout only takes effect on detach)
        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must still exist while attached even with set_timeout"
        );

        // Detach -- now the timeout should kick in (stored duration from set_timeout)
        sender.detach().expect("detach");
        assert!(matches!(
            stream.next().await,
            Some(Ok(StreamFrame::Detached))
        ));

        // Advance past the timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after detach with stored set_timeout duration"
        );
    }

    // -----------------------------------------------------------------------
    // Registry injection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_with_external_registry() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let external_registry: Arc<DashMap<u64, AnchorEntry>> = Arc::new(DashMap::new());

        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .registry(external_registry.clone())
            .build()
            .expect("builder with external registry should succeed");

        // Verify the manager uses the injected registry (same Arc)
        assert!(
            Arc::ptr_eq(&mgr.registry, &external_registry),
            "manager must use the externally provided registry Arc"
        );
    }

    #[test]
    fn test_builder_without_registry_creates_own() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);

        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .build()
            .expect("builder without registry should succeed");

        // Registry should exist and be empty
        assert_eq!(mgr.registry.len(), 0, "auto-created registry must be empty");
    }

    #[test]
    fn test_create_anchor_inserts_into_shared_registry() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let shared_registry: Arc<DashMap<u64, AnchorEntry>> = Arc::new(DashMap::new());

        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .registry(shared_registry.clone())
            .build()
            .expect("builder should succeed");

        assert_eq!(
            shared_registry.len(),
            0,
            "shared registry must be empty before create_anchor"
        );

        let anchor = mgr.create_anchor::<u32>();
        let (_, local_id) = anchor.handle().unpack();

        // Verify the entry was inserted into the shared registry (accessible outside mgr)
        assert_eq!(
            shared_registry.len(),
            1,
            "shared registry must have 1 entry after create_anchor"
        );
        assert!(
            shared_registry.contains_key(&local_id),
            "shared registry must contain the created anchor"
        );
    }

    // -----------------------------------------------------------------------
    // StreamController tests (Plan 11-02, Task 1)
    // -----------------------------------------------------------------------

    #[test]
    fn test_controller_clone() {
        // controller() returns a Clone-able type; multiple clones all refer to same anchor.
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        let ctrl1 = stream.controller();
        let ctrl2 = ctrl1.clone();

        // Both point to the same local_id — cancelling via ctrl2 removes the anchor.
        ctrl2.cancel();
        assert!(
            !mgr.registry.contains_key(&local_id),
            "ctrl2.cancel() must remove anchor from registry"
        );

        // ctrl1 is now a no-op (AtomicBool already set), double-cancel must not panic.
        ctrl1.cancel();
    }

    #[test]
    fn test_cancel_self_removes_registry() {
        // StreamAnchor::cancel(self) removes anchor from registry.
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist before cancel"
        );

        stream.cancel();

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after cancel(self)"
        );
    }

    #[test]
    fn test_controller_cancel_removes_registry() {
        // StreamController::cancel() removes anchor from registry.
        // Test the drop path: get controller, drop StreamAnchor, verify controller still no-panics.
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        let ctrl = stream.controller();
        // Drop the stream — Drop impl fires, removes anchor via controller.cancel()
        drop(stream);

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed by Drop"
        );

        // ctrl.cancel() should be idempotent — anchor already gone, no panic.
        ctrl.cancel();
    }

    #[test]
    fn test_double_cancel_idempotent() {
        // cancel twice does not panic, registry entry absent after first cancel.
        let mgr = make_manager();
        let stream = mgr.create_anchor::<u32>();
        let (_, local_id) = stream.handle().unpack();

        let ctrl = stream.controller();
        ctrl.cancel();
        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be absent after first cancel"
        );

        // Second cancel — must not panic.
        ctrl.cancel();
    }

    // -----------------------------------------------------------------------
    // register_handlers tests (Plan 12-01, Task 2)
    // -----------------------------------------------------------------------

    #[test]
    fn test_register_handlers_stores_messenger_in_lock() {
        // Verify that after register_handlers, messenger_lock.get() is Some
        // and that a second call returns Err.
        // Note: We use Messenger::builder().build() which requires tokio runtime.
        // This is a compile + behavior test using a real Messenger (no-transport).
        // The test is sync to avoid needing #[tokio::test] but uses a runtime.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let messenger = crate::messenger::Messenger::builder()
                .build()
                .await
                .expect("messenger");
            let worker_id = velo_ext::WorkerId::from_u64(99);
            let transport = Arc::new(MockTransport);
            let am = Arc::new(AnchorManager::new(worker_id, transport));

            // First call succeeds
            am.register_handlers(Arc::clone(&messenger))
                .expect("first register_handlers must succeed");

            // messenger_lock is set
            assert!(
                am.messenger_lock.get().is_some(),
                "messenger_lock must be Some after register_handlers"
            );
        });
    }

    #[test]
    fn test_register_handlers_second_call_errors() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let m1 = crate::messenger::Messenger::builder()
                .build()
                .await
                .unwrap();
            let m2 = crate::messenger::Messenger::builder()
                .build()
                .await
                .unwrap();

            let worker_id = velo_ext::WorkerId::from_u64(100);
            let transport = Arc::new(MockTransport);
            let am = Arc::new(AnchorManager::new(worker_id, transport));

            am.register_handlers(Arc::clone(&m1))
                .expect("first call ok");
            let result = am.register_handlers(Arc::clone(&m2));
            assert!(result.is_err(), "second call must return Err");
        });
    }

    // -----------------------------------------------------------------------
    // Transport registry tests (Plan 16-02, Task 1)
    // -----------------------------------------------------------------------

    /// Minimal no-op transport used for registry resolution tests.
    /// Different from MockTransport so we can distinguish registered
    /// transports by type via pointer identity.
    struct NoopTransport;

    impl crate::streaming::transport::FrameTransport for NoopTransport {
        fn key(&self) -> velo_ext::TransportKey {
            velo_ext::TransportKey::new("noop-stream")
        }

        fn address(&self) -> velo_ext::WorkerAddress {
            velo_ext::WorkerAddress::empty()
        }

        fn bind(
            &self,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Receiver<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded(1).1) })
        }

        fn connect(
            &self,
            _peer: velo_ext::WorkerId,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Sender<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded(1).0) })
        }
    }

    #[test]
    fn test_transport_registry_resolution() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let default_transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let tcp_transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(NoopTransport);

        let mut registry = HashMap::new();
        registry.insert("noop-stream".to_string(), Arc::clone(&tcp_transport));

        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(default_transport)
            .transport_registry(Arc::new(registry))
            .build()
            .expect("builder should succeed");

        // "noop-stream" key should resolve to the registered NoopTransport
        let resolved = mgr
            .resolve_transport(&velo_ext::TransportKey::new("noop-stream"))
            .expect("noop-stream key must resolve");
        assert!(
            Arc::ptr_eq(&resolved, &tcp_transport),
            "resolved transport must be the registered noop transport"
        );

        // Unregistered key in a non-empty registry must error (no fallback).
        let err = match mgr.resolve_transport(&velo_ext::TransportKey::new("missing-stream")) {
            Err(e) => e,
            Ok(_) => panic!("unregistered key in non-empty registry must error"),
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("unsupported streaming transport key"),
            "error message must mention unsupported key, got: {}",
            msg
        );
    }

    #[test]
    fn test_unsupported_key() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let default_transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);

        let mut registry = HashMap::new();
        registry.insert(
            "noop-stream".to_string(),
            Arc::new(NoopTransport) as Arc<dyn crate::streaming::transport::FrameTransport>,
        );

        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(default_transport)
            .transport_registry(Arc::new(registry))
            .build()
            .expect("builder should succeed");

        let err = match mgr.resolve_transport(&velo_ext::TransportKey::new("unknown")) {
            Err(e) => e,
            Ok(_) => panic!("unknown key must return error"),
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("unknown"),
            "error must name the unsupported key, got: {}",
            msg
        );
    }

    #[test]
    fn test_empty_registry_fallback() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let default_transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let default_clone = Arc::clone(&default_transport);

        // Empty registry -- backward compat: resolve_transport falls back to self.transport.
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(default_transport)
            .build()
            .expect("builder should succeed");

        let resolved = mgr
            .resolve_transport(&velo_ext::TransportKey::new("anything"))
            .expect("empty registry must fall back to default transport");
        assert!(
            Arc::ptr_eq(&resolved, &default_clone),
            "resolved transport must be the default transport when registry is empty"
        );
    }

    // -----------------------------------------------------------------------
    // Per-anchor liveness configuration tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_heartbeat_interval_is_5s() {
        // AnchorManager::new must produce the protocol default of 5s so that
        // existing callers see no behavior change.
        let mgr = make_manager();
        assert_eq!(
            mgr.default_heartbeat_interval,
            std::time::Duration::from_secs(5),
            "AnchorManager::new must default heartbeat_interval to 5s"
        );
    }

    #[test]
    fn test_builder_overrides_default_heartbeat_interval() {
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_heartbeat_interval(std::time::Duration::from_millis(750))
            .build()
            .expect("builder should succeed");
        assert_eq!(
            mgr.default_heartbeat_interval,
            std::time::Duration::from_millis(750),
            "builder must accept default_heartbeat_interval override"
        );
    }

    #[test]
    fn test_create_anchor_uses_manager_heartbeat_default() {
        // create_anchor() (no config) must inherit the manager's default cadence.
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_heartbeat_interval(std::time::Duration::from_millis(250))
            .build()
            .expect("builder should succeed");

        let anchor = mgr.create_anchor::<u32>();
        let (_, local_id) = anchor.handle().unpack();

        let entry = mgr
            .registry
            .get(&local_id)
            .expect("entry must exist after create_anchor");
        assert_eq!(
            entry.heartbeat_interval,
            std::time::Duration::from_millis(250),
            "create_anchor must inherit manager-level default_heartbeat_interval"
        );
    }

    #[test]
    fn test_create_anchor_with_config_overrides_heartbeat() {
        // Per-anchor override beats the manager default.
        let mgr = make_manager(); // 5s default heartbeat
        let cfg = AnchorConfig {
            unattached_timeout: None,
            heartbeat_interval: Some(std::time::Duration::from_millis(123)),
        };
        let anchor = mgr.create_anchor_with_config::<u32>(cfg);
        let (_, local_id) = anchor.handle().unpack();

        let entry = mgr.registry.get(&local_id).expect("entry exists");
        assert_eq!(
            entry.heartbeat_interval,
            std::time::Duration::from_millis(123),
            "AnchorConfig::heartbeat_interval must override the manager default"
        );
    }

    #[tokio::test]
    async fn test_create_anchor_with_config_overrides_unattached_timeout() {
        // Per-anchor override beats the manager default for the unattached TTL too.
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("builder should succeed");

        let cfg = AnchorConfig {
            unattached_timeout: Some(std::time::Duration::from_millis(50)),
            heartbeat_interval: None,
        };
        let anchor = mgr.create_anchor_with_config::<u32>(cfg);
        let (_, local_id) = anchor.handle().unpack();

        let entry = mgr.registry.get(&local_id).expect("entry exists");
        assert_eq!(
            entry.unattached_timeout,
            Some(std::time::Duration::from_millis(50)),
            "AnchorConfig::unattached_timeout must override the manager default"
        );
    }

    #[tokio::test]
    async fn test_create_anchor_with_default_config_inherits_both() {
        // AnchorConfig::default() inherits both fields — equivalent to create_anchor().
        let worker_id = velo_ext::WorkerId::from_u64(42);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .default_unattached_timeout(std::time::Duration::from_secs(7))
            .default_heartbeat_interval(std::time::Duration::from_millis(800))
            .build()
            .expect("builder should succeed");

        let anchor = mgr.create_anchor_with_config::<u32>(AnchorConfig::default());
        let (_, local_id) = anchor.handle().unpack();

        let entry = mgr.registry.get(&local_id).expect("entry exists");
        assert_eq!(
            entry.heartbeat_interval,
            std::time::Duration::from_millis(800)
        );
        assert_eq!(
            entry.unattached_timeout,
            Some(std::time::Duration::from_secs(7))
        );
    }

    #[tokio::test]
    async fn test_per_anchor_heartbeat_propagates_through_attach_response() {
        // End-to-end: create an anchor with a non-default heartbeat interval,
        // attach via the local path, observe the sender ticks at the configured
        // cadence (proves AnchorEntry → StreamSender plumbing works).
        tokio::time::pause();

        let worker_id = velo_ext::WorkerId::from_u64(7);
        let transport: Arc<dyn crate::streaming::transport::FrameTransport> =
            Arc::new(MockTransport);
        let mgr = AnchorManagerBuilder::default()
            .worker_id(worker_id)
            .transport(transport)
            .build()
            .expect("builder should succeed");

        let cfg = AnchorConfig {
            unattached_timeout: None,
            heartbeat_interval: Some(std::time::Duration::from_millis(200)),
        };
        let anchor = mgr.create_anchor_with_config::<u32>(cfg);
        let handle = anchor.handle();

        let sender = mgr
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("local attach should succeed");

        // Drain the consumer-side stream concurrently so the bounded channel
        // doesn't block the sender's heartbeat task.
        let collected: Arc<DashMap<usize, crate::streaming::frame::StreamFrame<u32>>> =
            Arc::new(DashMap::new());
        let collected_clone = collected.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            let mut anchor = anchor;
            let mut idx = 0usize;
            while let Some(frame) = anchor.next().await {
                if let Ok(f) = frame {
                    collected_clone.insert(idx, f);
                    idx += 1;
                }
            }
        });

        // Advance ~1 full second: at 200ms cadence we expect at least 4 heartbeats
        // emitted by the producer (the consumer filters them out, but the registry
        // entry is what we care about — it must hold the configured interval).
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        let (_, local_id) = handle.unpack();
        let entry = mgr.registry.get(&local_id).expect("entry exists");
        assert_eq!(
            entry.heartbeat_interval,
            std::time::Duration::from_millis(200),
            "AnchorEntry must store the per-anchor cadence after attach"
        );

        drop(sender);
    }

    // -----------------------------------------------------------------------
    // Attach-RTT label cardinality
    // -----------------------------------------------------------------------

    /// A `FrameTransport` whose key the test chooses, so the two sides of an
    /// attach can disagree about it.
    struct KeyedMockTransport(&'static str);

    impl crate::streaming::transport::FrameTransport for KeyedMockTransport {
        fn key(&self) -> velo_ext::TransportKey {
            velo_ext::TransportKey::new(self.0)
        }

        fn address(&self) -> velo_ext::WorkerAddress {
            velo_ext::WorkerAddress::empty()
        }

        fn bind(
            &self,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Receiver<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).1) })
        }

        fn connect(
            &self,
            _peer: velo_ext::WorkerId,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, AnyhowResult<flume::Sender<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).0) })
        }
    }

    fn tcp_messenger_transport() -> Arc<crate::transports::tcp::TcpTransport> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .expect("from_listener")
                .build()
                .expect("build transport"),
        )
    }

    /// The `transport_scheme` label never carries a string the peer chose.
    ///
    /// The key in an attach response comes off the wire, and the sender records
    /// it as a label before anything has resolved it — `connect_streaming` runs
    /// afterwards, and moving the observation there would fold the transport
    /// dial into the round trip the histogram is supposed to bracket. A label
    /// value a peer can pick is unbounded cardinality: one histogram child per
    /// distinct string, each alive for the life of the process.
    ///
    /// The disagreement is the ordinary mixed-deployment one, not a contrived
    /// hostile peer: `negotiation::select` falls through to the *receiver's*
    /// own default transport key whenever the sender did not advertise the mux,
    /// so a receiver configured with a different streaming transport answers
    /// with a key this sender never named. Here the sender's registry is empty,
    /// which is the documented convenience path where `resolve_transport` falls
    /// back to the default transport — so the attach also succeeds, and the
    /// label is recorded on the success arm rather than an error one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_peer_chosen_transport_key_never_becomes_a_label_value() {
        use crate::observability::test_helpers::MetricSnapshot;

        const RTT: &str = "velo_streaming_anchor_attach_rtt_seconds";
        const PEER_CHOSEN: &str = "a-key-this-sender-never-advertised";

        let m_consumer = crate::messenger::Messenger::builder()
            .add_transport(tcp_messenger_transport())
            .build()
            .await
            .expect("consumer messenger");
        let m_producer = crate::messenger::Messenger::builder()
            .add_transport(tcp_messenger_transport())
            .build()
            .await
            .expect("producer messenger");
        m_consumer
            .register_peer(m_producer.peer_info())
            .expect("register producer on consumer");
        m_producer
            .register_peer(m_consumer.peer_info())
            .expect("register consumer on producer");
        tokio::time::sleep(Duration::from_millis(200)).await;

        let producer_reg = prometheus::Registry::new();
        let producer_metrics =
            Arc::new(VeloMetrics::register(&producer_reg).expect("register metrics"));

        let am_consumer = Arc::new(
            AnchorManagerBuilder::default()
                .worker_id(m_consumer.instance_id().worker_id())
                .transport(Arc::new(KeyedMockTransport(PEER_CHOSEN))
                    as Arc<dyn crate::streaming::transport::FrameTransport>)
                .messenger(Some(Arc::clone(&m_consumer)))
                .build()
                .expect("consumer anchor manager"),
        );
        let am_producer = Arc::new(
            AnchorManagerBuilder::default()
                .worker_id(m_producer.instance_id().worker_id())
                .transport(Arc::new(KeyedMockTransport("this-senders-own-stream"))
                    as Arc<dyn crate::streaming::transport::FrameTransport>)
                .messenger(Some(Arc::clone(&m_producer)))
                .metrics(Some(Arc::clone(&producer_metrics)))
                .build()
                .expect("producer anchor manager"),
        );
        am_consumer
            .register_handlers(Arc::clone(&m_consumer))
            .expect("consumer handlers");
        am_producer
            .register_handlers(Arc::clone(&m_producer))
            .expect("producer handlers");

        let anchor = am_consumer.create_anchor::<u32>();
        let handle = anchor.handle();
        let _sender = am_producer
            .attach_stream_anchor::<u32>(handle)
            .await
            .expect("the attach must succeed: an empty registry resolves any key");

        let snap = MetricSnapshot::from_registry(&producer_reg);
        assert_eq!(
            snap.histogram_count(RTT, &[("transport_scheme", PEER_CHOSEN)]),
            0,
            "the peer named the label child; every distinct string a peer sends \
             would mint one that lives as long as the process"
        );
        assert_eq!(
            snap.histogram_count(
                RTT,
                &[("outcome", "success"), ("transport_scheme", "unknown")]
            ),
            1,
            "a key outside this node's advertisement is not a negotiated scheme"
        );

        // Held to the end: dropping the anchor tears the consumer side down and
        // races the handler that produced the observation above.
        drop(anchor);
    }

    // -----------------------------------------------------------------------
    // Terminal error frames retire the registry entry
    // -----------------------------------------------------------------------

    /// A terminal error frame removes the anchor's registry entry, the way
    /// `Finalized` and `Dropped` do.
    ///
    /// `Drop` cannot do it afterwards. It short-circuits on `terminated`, which
    /// these arms have just set, so an anchor that ends on an error and is then
    /// dropped leaves its entry behind for the life of the process: the frame
    /// channel, the anchor's place in `velo_streaming_active_anchors`, and —
    /// under zero-RTT — the `PreBind` whose `Drop` is the only thing that gives
    /// the mux slot back. The unattached timer used to be the backstop for
    /// that, and a pre-bound anchor no longer has one.
    #[tokio::test]
    async fn a_terminal_error_frame_retires_the_registry_entry() {
        let mgr = make_manager();

        // The two shapes: one the producer wrote, and one this side could not
        // decode. `0xc1` is msgpack's never-used byte, so it can only fail.
        let produced = rmp_serde::to_vec(&StreamFrame::<u32>::TransportError("socket gone".into()))
            .expect("encode TransportError");
        for bytes in [produced, vec![0xc1u8]] {
            let mut anchor = mgr.create_anchor::<u32>();
            let (_, local_id) = anchor.handle().unpack();
            mgr.registry
                .get(&local_id)
                .expect("entry exists")
                .frame_tx
                .send(bytes)
                .expect("inject the frame");

            let frame = anchor.next().await.expect("a frame");
            assert!(
                frame.is_err(),
                "both shapes must surface as a terminal error, got {frame:?}"
            );
            assert!(
                !mgr.registry.contains_key(&local_id),
                "a terminal error frame must retire the entry; the drop that follows will not, \
                 because the frame already terminated the anchor"
            );
            drop(anchor);
        }
    }
}
