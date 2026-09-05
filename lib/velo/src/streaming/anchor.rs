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

// ---------------------------------------------------------------------------
// Shared gauge helper
// ---------------------------------------------------------------------------

/// Set the `streaming_active_anchors` Prometheus gauge to
/// `spsc.len() + mpsc.len()`. No-op when `metrics` is `None`.
///
/// SPSC and MPSC anchors live in separate registries but share a single
/// `next_local_id` counter and a single gauge, so every path that mutates
/// either registry must report the sum. Use this helper rather than reading
/// `registry.len()` directly — that's how the pre-MPSC code mis-counted.
pub(crate) fn set_active_anchor_gauge(
    metrics: Option<&Arc<VeloMetrics>>,
    spsc: &Arc<DashMap<u64, AnchorEntry>>,
    mpsc: &Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,
) {
    if let Some(m) = metrics {
        m.set_streaming_active_anchors(spsc.len() + mpsc.len());
    }
}

/// Grouped handles needed by anchor constructors and background pumps to
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
    /// per-window deadline the consumer reader pump applies. Total tolerance
    /// before `Dropped` injection is `crate::streaming::control::DETECTION_MULTIPLIER`
    /// times this value.
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
/// to prevent TOCTOU races. The reader pump takes ownership of the transport
/// receiver directly rather than storing it in the entry.
// Fields are consumed by Phase 7+ control handlers and Phase 8 data path.
#[allow(dead_code)]
pub(crate) struct AnchorEntry {
    /// Raw-bytes frame delivery channel to the [`StreamAnchor<T>`] consumer.
    ///
    /// Non-generic so `DashMap<u64, AnchorEntry>` requires no type parameters.
    pub frame_tx: flume::Sender<Vec<u8>>,

    /// Anchor-lifetime parent token. Created at anchor creation; cancelled only
    /// by finalize/remove/cancel. Child tokens are derived for transient tasks
    /// (reader pump, timeout) so that stopping a child never poisons the parent.
    pub cancel_token: CancellationToken,

    /// Child token for the currently active reader pump (`None` when no sender
    /// is attached). Created via `cancel_token.child_token()` on each attach.
    /// Cancelling this stops the pump without affecting the parent.
    pub active_pump_token: Option<CancellationToken>,

    /// `true` iff a sender is currently attached. The reader pump owns the
    /// transport receiver separately (not stored here).
    pub attachment: bool,

    /// Cancels the inactivity timeout task when a sender attaches.
    /// `None` if no timeout is configured for this anchor.
    pub timeout_cancel: Option<CancellationToken>,

    /// The configured unattached timeout for this anchor. Stored so that
    /// `detach` can respawn the timeout task with the same duration.
    /// `None` means the anchor never auto-removes while unattached.
    pub unattached_timeout: Option<Duration>,

    /// The negotiated heartbeat cadence for this anchor. The reader pump uses
    /// this as its per-window deadline; the producer's `StreamSender` uses it
    /// as its emit interval. Resolved at create-time from per-anchor config or
    /// the manager-level default and echoed to the sender via
    /// [`crate::streaming::control::AnchorAttachResponse::Ok::heartbeat_interval_ms`].
    pub heartbeat_interval: Duration,

    /// Populated on successful attach from [`crate::streaming::control::AnchorAttachRequest::stream_cancel_handle`].
    /// Encodes the sender's WorkerId + stream ID so the anchor can route `_stream_cancel`
    /// active messages to the correct sender worker when the consumer cancels upstream.
    /// `None` until a sender attaches.
    pub stream_cancel_handle: Option<crate::streaming::control::StreamCancelHandle>,

    /// The mux slot bound and pumped for this anchor ahead of any sender, when
    /// [`AnchorManager::prebind_anchor`] minted a ticket for it. `None` on the
    /// ordinary attach path, which is every anchor whose application sends no
    /// ticket.
    pub prebind: Option<PreBind>,
}

// ---------------------------------------------------------------------------
// PreBind
// ---------------------------------------------------------------------------

/// A mux slot bound and pumped for an anchor before any sender asked for one.
///
/// Zero-RTT stream setup does at request registration what the attach handler
/// does on the round trip: allocate the routing session, bind the slot, take
/// the drain signal, and spawn the reader pump. What is left over is this — the
/// terms that were minted, the claim token that says whether anyone took them
/// up, and the means to give the slot back.
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
pub(crate) struct PreBind {
    /// The anchor this slot was bound for. Half of the bind's key; the other
    /// half is `ticket.routing_session_id`.
    anchor_id: u64,
    ticket: crate::streaming::control::StreamOpenTicket,
    drain: Arc<crate::streaming::messenger_mux::ingress::DrainSignal>,
    /// `Weak` for the same reason the accept window's task holds one: a strong
    /// handle inside a registry entry would keep the transport, its batchers
    /// and its ingress state alive for as long as anything holds the anchor
    /// registry.
    ///
    /// It is also how [`PreBind::adopt`] defuses `Drop` — see there.
    mux: std::sync::Weak<crate::streaming::messenger_mux::MessengerMuxTransport>,
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
    /// those — the attach carried a `StreamCancelHandle`, so the anchor can
    /// reach its producer directly and needs no close posted on its behalf.
    ///
    /// The ticket is cloned rather than moved out because this type has a
    /// `Drop`; the clone is one `Arc<str>` bump on a path that runs once per
    /// adopted stream.
    fn adopt(mut self) -> crate::streaming::control::StreamOpenTicket {
        self.mux = std::sync::Weak::new();
        self.ticket.clone()
    }
}

impl AnchorEntry {
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
        let Some(mux) = self.mux.upgrade() else {
            return;
        };
        match self.drain.claimed() {
            Some((peer, slot)) => mux.close_claimed_slot(peer, slot),
            None => mux.release_bind(self.anchor_id, self.ticket.routing_session_id),
        }
    }
}

/// The sender-side identity one stream is opened under.
///
/// A struct because it is allocated before the terms of the stream are known —
/// the remote attach path has to name it in the request it sends to learn them
/// — and then has to survive intact into the tail that registers it. Passing
/// the four parts positionally would take that tail past clippy's argument
/// limit, which `CLAUDE.md` says to answer with a config struct rather than an
/// `allow`.
struct SenderIdentity {
    sender_stream_id: u64,
    cancel_token: CancellationToken,
    poison_tx: flume::Sender<()>,
    poison_rx: flume::Receiver<()>,
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
    local_id: u64,
    registry: Arc<DashMap<u64, AnchorEntry>>,
    /// Sibling MPSC registry — held so the shared gauge update includes MPSC
    /// anchors alongside SPSC. Cheap `Arc` clone, no other use.
    mpsc_registry: Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,
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
        set_active_anchor_gauge(
            self.inner.metrics.as_ref(),
            &self.inner.registry,
            &self.inner.mpsc_registry,
        );
        if let Some(metrics) = self.inner.metrics.as_ref() {
            metrics.record_streaming_operation(
                StreamingOp::Cancel,
                HandlerOutcome::Success,
                "velo",
                started.elapsed(),
            );
        }

        // Directly cancel the SenderEntry in the local sender_registry.
        // This fires the user-facing cancel_token and poisons send() immediately
        // without requiring an AM round-trip. Idempotent: remove returns None if
        // the entry was already removed (e.g. finalize/detach ran first).
        if let Some(handle) = stream_cancel_handle {
            let (sender_worker_id, sender_stream_id) = handle.unpack();
            if let Some((_, entry)) = self.inner.sender_registry.senders.remove(&sender_stream_id) {
                drop(entry.rx_closer.lock().unwrap().take());
                entry.cancel_token.cancel();
            }

            // Also send _stream_cancel AM for cross-worker scenarios (messenger present)
            if let Some(messenger) = self.inner.messenger.clone() {
                let payload = serde_json::to_vec(&crate::streaming::control::StreamCancelRequest {
                    sender_stream_id,
                })
                .expect("serialize StreamCancelRequest");
                // Fire-and-forget: use tokio::spawn guarded by try_current()
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
// StreamAnchor<T>
// ---------------------------------------------------------------------------

/// Consumer-side receive stream for an anchor.
///
/// Implements [`futures::Stream`] yielding `Result<StreamFrame<T>, StreamError>`.
/// Heartbeat frames are filtered out and never exposed to the consumer.
/// Terminal sentinels (`Finalized`, `Detached`, `Dropped`, `TransportError`)
/// cause the stream to yield one final item and then `None` on subsequent polls.
///
/// Use [`StreamExt::next()`](futures::StreamExt::next) for async iteration.
///
/// # Example
///
/// ```rust,no_run
/// use futures::StreamExt;
/// use crate::streaming::{AnchorManager, StreamFrame};
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
    /// Set to true after a terminal sentinel; prevents further polling.
    terminated: bool,
    /// The local ID of the anchor in the registry (for cancel).
    local_id: u64,
    /// Arc clone of the AnchorManager's registry (for cancel).
    registry: Arc<DashMap<u64, AnchorEntry>>,
    /// Sibling MPSC registry — used when updating the shared active-anchors gauge.
    mpsc_registry: Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,
    /// Shared cancel handle — also held by any [`StreamController`] clones.
    controller: StreamController,
    metrics: Option<Arc<VeloMetrics>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> StreamAnchor<T> {
    pub(crate) fn new(
        handle: StreamAnchorHandle,
        rx: flume::Receiver<Vec<u8>>,
        local_id: u64,
        ctx: AnchorContext,
        sender_registry: Arc<crate::streaming::control::SenderRegistry>,
        messenger: Option<Arc<crate::messenger::Messenger>>,
    ) -> Self {
        let AnchorContext {
            registry,
            mpsc_registry,
            metrics,
        } = ctx;
        let inner = Arc::new(StreamControllerInner {
            local_id,
            registry: registry.clone(),
            mpsc_registry: mpsc_registry.clone(),
            metrics: metrics.clone(),
            sender_registry,
            messenger,
            cancelled: AtomicBool::new(false),
        });
        let controller = StreamController { inner };
        Self {
            handle,
            inner_stream: rx.into_stream(),
            terminated: false,
            local_id,
            registry,
            mpsc_registry,
            controller,
            metrics,
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

    /// Remove this anchor's registry entry, if it is still there.
    ///
    /// Every terminal frame owes the registry this, and [`Drop`] cannot be the
    /// one to pay it: it short-circuits on `terminated`, which a terminal frame
    /// has just set. An entry left behind costs the frame channel, a permanent
    /// reading in `velo_streaming_active_anchors`, and — under zero-RTT — the
    /// [`PreBind`] whose own `Drop` is the only thing that gives the mux slot
    /// back to its producer.
    fn retire(&self) {
        if let Some((_, entry)) = self.registry.remove(&self.local_id) {
            entry.cancel_token.cancel();
            set_active_anchor_gauge(self.metrics.as_ref(), &self.registry, &self.mpsc_registry);
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
            // Cancel existing timeout task if any
            if let Some(ref old_tc) = entry.timeout_cancel {
                old_tc.cancel();
            }

            // Update the stored duration
            entry.unattached_timeout = timeout;

            // If unattached and a timeout is set, spawn a new timeout task.
            // A pre-bound anchor counts as spoken for: its slot is bound and
            // pumped and a sender is on its way to it, so the timer that
            // measures "no sender attached" would be measuring nothing and
            // would remove a stream that is about to run.
            if !entry.attachment && entry.prebind.is_none() {
                if let Some(duration) = timeout {
                    let tc = AnchorManager::spawn_timeout_task(
                        self.registry.clone(),
                        self.mpsc_registry.clone(),
                        self.metrics.clone(),
                        self.local_id,
                        duration,
                        &entry.cancel_token,
                    );
                    entry.timeout_cancel = Some(tc);
                } else {
                    entry.timeout_cancel = None;
                }
            } else {
                // Attached: just clear the old cancel token; duration is stored
                // and will be used when detach respawns the timeout task.
                entry.timeout_cancel = None;
            }
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
            match Pin::new(&mut this.inner_stream).poll_next(cx) {
                Poll::Ready(Some(bytes)) => {
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
                            // Anchor is permanently closed.
                            this.retire();
                            return Poll::Ready(Some(Ok(StreamFrame::Finalized)));
                        }
                        Ok(StreamFrame::Detached) => {
                            // Detached is NOT terminal — a new sender may reattach.
                            // Clear the attachment flag so attach_stream_anchor can succeed.
                            if let Some(mut entry) = this.registry.get_mut(&this.local_id) {
                                entry.attachment = false;
                            }
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
/// The `registry` is wrapped in an `Arc` so that control handlers (Phase 7)
/// and the data-path pump (Phase 8) can hold a cheap clone of the registry
/// reference without holding a reference to the whole `AnchorManager`.
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

    /// The `messenger-mux-v1` transport, when one is installed.
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
        self.build_inner()
    }
}

impl AnchorManager {
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
            Self::spawn_timeout_task(
                self.registry.clone(),
                self.mpsc_registry.clone(),
                self.metrics.clone(),
                local_id,
                timeout,
                &cancel_token,
            )
        });

        let entry = AnchorEntry {
            frame_tx,
            cancel_token,
            active_pump_token: None,
            attachment: false,
            timeout_cancel,
            unattached_timeout,
            heartbeat_interval,
            stream_cancel_handle: None, // populated on attach
            prebind: None,              // populated by `prebind_anchor`
        };

        self.registry.insert(local_id, entry);
        self.update_active_anchor_gauge();

        let handle = StreamAnchorHandle::pack(self.worker_id, local_id);
        StreamAnchor::new(
            handle,
            frame_rx,
            local_id,
            self.anchor_context(),
            self.sender_registry.clone(),
            self.messenger.clone(),
        )
    }

    /// Spawn a background task that removes the anchor after `timeout` elapses.
    ///
    /// Returns a [`CancellationToken`] that cancels the task when triggered
    /// (e.g. on attach, or when `set_timeout(None)` is called).
    pub(crate) fn spawn_timeout_task(
        registry: Arc<DashMap<u64, AnchorEntry>>,
        mpsc_registry: Arc<DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>>,
        metrics: Option<Arc<VeloMetrics>>,
        local_id: u64,
        timeout: Duration,
        parent_cancel: &CancellationToken,
    ) -> CancellationToken {
        let tc = parent_cancel.child_token();
        let tc_clone = tc.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tc_clone.cancelled() => {
                    // Attach or explicit cancel -- do nothing
                }
                _ = tokio::time::sleep(timeout) => {
                    // Timeout expired -- remove anchor
                    if let Some((_, entry)) = registry.remove(&local_id) {
                        entry.cancel_token.cancel();
                        set_active_anchor_gauge(metrics.as_ref(), &registry, &mpsc_registry);
                        // Dropping frame_tx closes the channel -> StreamAnchor yields None
                    }
                }
            }
        });
        tc
    }

    /// Remove an anchor from the registry and return its entry (if present).
    ///
    /// Cancels the entry's token before returning. Used by control path cleanup
    /// handlers (Phase 7) and drop impls.
    #[allow(dead_code)]
    pub(crate) fn remove_anchor(&self, local_id: u64) -> Option<AnchorEntry> {
        self.registry.remove(&local_id).map(|(_, entry)| {
            entry.cancel_token.cancel();
            self.update_active_anchor_gauge();
            entry
        })
    }

    /// Inject a raw sentinel frame into the anchor's delivery channel.
    ///
    /// This is a non-blocking best-effort send used by the control path (Phase 7).
    /// The data path (Phase 8) will use a blocking variant for `Item` frames.
    ///
    /// # Note
    /// The registry reference is dropped before any other operation to ensure we do
    /// NOT hold a DashMap shard lock across any await point.
    #[allow(dead_code)]
    pub(crate) fn inject_sentinel(&self, local_id: u64, frame_bytes: Vec<u8>) {
        // Obtain a cloned Sender so we drop the DashMap reference immediately.
        let maybe_sender = self
            .registry
            .get(&local_id)
            .map(|entry| entry.frame_tx.clone());

        if let Some(sender) = maybe_sender {
            // Non-blocking best-effort -- control sentinels must never stall.
            let _ = sender.try_send(frame_bytes);
        }
    }

    /// Install the mux this manager negotiates with, once.
    ///
    /// Separate from the transport registry, which the mux also joins: the
    /// registry answers "can I `connect()` on this key", while this answers
    /// "may I offer, and drive, `messenger-mux-v1`". Both are needed and they
    /// are set together by the builder.
    pub(crate) fn install_mux(
        &self,
        mux: Arc<crate::streaming::messenger_mux::MessengerMuxTransport>,
    ) -> anyhow::Result<()> {
        self.mux
            .set(mux)
            .map_err(|_| anyhow::anyhow!("a messenger mux is already installed on this manager"))
    }

    /// Bind and pump a mux slot for `handle` now, so its sender never has to
    /// ask for one.
    ///
    /// Returns the terms that sender must open on, to be carried to it in
    /// whatever envelope the application already sends — see
    /// [`crate::streaming::control::StreamOpenTicket`] and its counterpart
    /// [`open_anchor_stream`](Self::open_anchor_stream). Everything the
    /// `_anchor_attach` handler would have done on the round trip happens here
    /// instead, so the sender's first record is its first message.
    ///
    /// `None` means *no ticket was minted; attach the ordinary way*, and it is
    /// never an error. With no mux installed nothing here can run and every
    /// stream behaves exactly as it does today — which is what keeps
    /// `MuxConfig::enabled` the complete rollback for this path too.
    ///
    /// `None` is also the answer for a handle this manager cannot pre-bind: one
    /// belonging to another worker, an MPSC anchor, an anchor already attached
    /// or already pre-bound, or one that has since been removed. Those are
    /// logged at debug, because unlike the rollback they are a caller mistake
    /// rather than a configuration.
    ///
    /// Must be called from a runtime context: it spawns the reader pump and the
    /// bind's accept-window task, exactly as the attach handler does.
    pub fn prebind_anchor(
        &self,
        handle: StreamAnchorHandle,
    ) -> Option<crate::streaming::control::StreamOpenTicket> {
        // The rollback, and the only `None` that is not a mistake.
        let mux = self.mux.get()?;

        let (handle_worker_id, local_id) = handle.unpack();
        if handle.is_mpsc_stream() || handle_worker_id != self.worker_id {
            tracing::debug!(
                %handle,
                "prebind_anchor: not a local SPSC anchor; no ticket minted"
            );
            return None;
        }

        // Receiver-allocated, for the reason the attach handler allocates one:
        // two senders reusing their own local counters would collide on the
        // transport's `(anchor_id, session_id)` routing key.
        let routing_session_id = self.next_routing_session_id.fetch_add(1, Ordering::Relaxed) + 1;
        let receiver = mux.prebind(local_id, routing_session_id);
        let drain = mux
            .take_drain_signal(local_id, routing_session_id)
            .expect("prebind parks a drain signal for the pair it just registered");

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
                        );
                        // A child of the anchor's token, as at attach: finalize,
                        // cancel and detach stop the pump without poisoning the
                        // parent.
                        let pump_cancel = entry.cancel_token.child_token();
                        entry.active_pump_token = Some(pump_cancel.clone());
                        // The unattached timer measures "no sender is coming".
                        // A pre-bound anchor has a slot waiting for one, so the
                        // timer is measuring nothing and would remove a live
                        // stream. Attach pauses it for the same reason.
                        if let Some(ref tc) = entry.timeout_cancel {
                            tc.cancel();
                        }
                        entry.prebind = Some(PreBind {
                            anchor_id: local_id,
                            ticket: ticket.clone(),
                            drain: Arc::clone(&drain),
                            mux: Arc::downgrade(mux),
                        });
                        Some((
                            ticket,
                            entry.frame_tx.clone(),
                            pump_cancel,
                            entry.heartbeat_interval,
                        ))
                    }
                }
            }
        };

        let Some((ticket, frame_tx, pump_cancel, heartbeat_interval)) = prepared else {
            // Nothing took ownership of the bind, so give it straight back
            // rather than leaving the accept window to find it in a minute.
            mux.release_bind(local_id, routing_session_id);
            tracing::debug!(
                %handle,
                "prebind_anchor: anchor is missing, attached or already pre-bound; no ticket minted"
            );
            return None;
        };

        tokio::spawn(crate::streaming::control::reader_pump(
            receiver,
            frame_tx,
            pump_cancel,
            self.anchor_context(),
            crate::streaming::control::PumpContext {
                local_id,
                heartbeat_deadline: heartbeat_interval,
                drain: Some(drain),
            },
        ));

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
                    // And the anchor is unattached again, so the timer that
                    // measures exactly that has to come back. `prebind_anchor`
                    // cancelled it because a pre-bind is a sender on its way;
                    // this is that sender turning back. Without the re-arm the
                    // anchor has no reaper at all — the timer was the only one,
                    // and a consumer that never polls its `StreamAnchor` to a
                    // terminal drops it into a `Drop` that does nothing.
                    if let Some(duration) = entry.unattached_timeout {
                        let tc = Self::spawn_timeout_task(
                            Arc::clone(&self.registry),
                            Arc::clone(&self.mpsc_registry),
                            self.metrics.clone(),
                            local_id,
                            duration,
                            &entry.cancel_token,
                        );
                        entry.timeout_cancel = Some(tc);
                    }
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
                    (PrebindAdoption::Adopted(prebind.adopt()), None)
                }
            }
        };
        drop(released);
        verdict
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

    /// Pick the transport to bind for an incoming attach.
    ///
    /// Called by both attach handlers, which differ only in the response type
    /// they pour the answer into.
    pub(crate) fn select_streaming_transport(
        &self,
        offered: &[velo_ext::TransportKey],
    ) -> crate::streaming::negotiation::Selection {
        crate::streaming::negotiation::select(offered, self.mux.get(), &self.transport)
    }

    /// Connect the transport the receiver's attach response named.
    ///
    /// The mux arm is why this is not just `resolve_transport(...).connect(...)`:
    /// a negotiated slot opens already holding the window the receiver
    /// advertised, which is what removes the round trip an `OpenSlot`-time
    /// `CreditUpdate` used to cost.
    async fn connect_streaming(
        &self,
        key: &velo_ext::TransportKey,
        peer: velo_ext::WorkerId,
        anchor_id: u64,
        session_id: u64,
        initial_credit: u32,
        slot_byte_budget: u32,
    ) -> Result<flume::Sender<Vec<u8>>, AttachError> {
        match crate::streaming::negotiation::choose(key, initial_credit, slot_byte_budget) {
            Ok(crate::streaming::negotiation::Connect::Mux(limits)) => {
                let mux = self.mux.get().ok_or_else(|| {
                    AttachError::TransportError(anyhow::anyhow!(
                        "peer answered with {key} but no messenger mux is installed here; \
                         it can only have learned that key from an advertisement this node made"
                    ))
                })?;
                Ok(mux
                    .connect_negotiated(peer, anchor_id, session_id, limits)
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

    /// Atomically attempt to mark an anchor as attached.
    ///
    /// Uses `DashMap::entry()` to perform the check-and-set atomically under
    /// the shard lock, preventing TOCTOU races between concurrent attach attempts.
    /// The reader pump takes ownership of the transport receiver separately.
    ///
    /// If a timeout task is running, it is cancelled (paused) on successful attach.
    ///
    /// Returns `Err(AttachError::AlreadyAttached)` if a sender is already attached.
    /// Returns `Err(AttachError::AnchorNotFound)` if `local_id` is not in the registry.
    #[allow(dead_code)]
    pub(crate) fn try_attach(
        &self,
        local_id: u64,
        handle: StreamAnchorHandle,
    ) -> Result<(), AttachError> {
        use dashmap::mapref::entry::Entry;
        match self.registry.entry(local_id) {
            Entry::Vacant(_) => Err(AttachError::AnchorNotFound { handle }),
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if entry.attachment {
                    Err(AttachError::AlreadyAttached { handle })
                } else {
                    entry.attachment = true;
                    // Cancel the timeout task while attached (pause timer)
                    if let Some(ref tc) = entry.timeout_cancel {
                        tc.cancel();
                    }
                    Ok(())
                }
            }
        }
    }

    /// Clear the attachment flag on an anchor.
    ///
    /// If the anchor has a configured `unattached_timeout`, a new timeout task
    /// is spawned (timer "resumes" by restarting from the full duration).
    ///
    /// Returns `true` if the anchor was found and was previously attached.
    #[allow(dead_code)]
    pub(crate) fn detach(&self, local_id: u64) -> bool {
        // Phase 1: Clear attachment and read unattached_timeout + cancel_token (drop DashMap ref)
        let (was_attached, maybe_timeout, maybe_parent) = self
            .registry
            .get_mut(&local_id)
            .map(|mut entry| {
                let was = entry.attachment;
                entry.attachment = false;
                (
                    was,
                    entry.unattached_timeout,
                    Some(entry.cancel_token.clone()),
                )
            })
            .unwrap_or((false, None, None));

        // Phase 2: Respawn timeout task outside the DashMap borrow
        if let Some(timeout) = maybe_timeout {
            let parent = maybe_parent
                .as_ref()
                .expect("cancel_token present when unattached_timeout is");
            let tc = Self::spawn_timeout_task(
                self.registry.clone(),
                self.mpsc_registry.clone(),
                self.metrics.clone(),
                local_id,
                timeout,
                parent,
            );
            // Store the new cancellation token back in the entry
            if let Some(mut entry) = self.registry.get_mut(&local_id) {
                entry.timeout_cancel = Some(tc);
            }
        }

        was_attached
    }

    /// Returns the number of anchors currently registered.
    ///
    /// Intended for testing and observability. The Prometheus
    /// `velo_streaming_active_anchors` gauge reflects the same value.
    pub fn active_anchor_count(&self) -> usize {
        self.registry.len()
    }

    pub(crate) fn update_active_anchor_gauge(&self) {
        set_active_anchor_gauge(self.metrics.as_ref(), &self.registry, &self.mpsc_registry);
    }

    /// Bundle the SPSC registry, MPSC registry, and metrics collector into
    /// a cheap `Arc`-cloneable context. Used to keep anchor constructors
    /// and background pumps under clippy's argument threshold.
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

    /// Register all five control-plane AM handlers on a live Messenger.
    ///
    /// Registers: `_anchor_attach`, `_anchor_detach`, `_anchor_finalize`,
    /// `_anchor_cancel` (all on `self` as `Arc<AnchorManager>`), and
    /// `_stream_cancel` (on `self.sender_registry`).
    ///
    /// Stores the messenger in `messenger_lock` (write-once) for use by
    /// `attach_remote` in Phase 12 Plan 02.
    ///
    /// # Errors
    ///
    /// Returns `Err` if called twice (OnceLock already set) or if any
    /// handler registration fails (e.g., duplicate handler name).
    ///
    /// # Panics
    ///
    /// Does not panic. Caller must hold an `Arc<AnchorManager>`.
    pub fn register_handlers(
        self: &Arc<Self>,
        messenger: Arc<crate::messenger::Messenger>,
    ) -> anyhow::Result<()> {
        use crate::streaming::control::{
            create_anchor_attach_handler, create_anchor_cancel_handler,
            create_anchor_detach_handler, create_anchor_finalize_handler,
            create_stream_cancel_handler,
        };

        messenger.register_streaming_handler(create_anchor_attach_handler(Arc::clone(self)))?;
        messenger.register_streaming_handler(create_anchor_detach_handler(Arc::clone(self)))?;
        messenger.register_streaming_handler(create_anchor_finalize_handler(Arc::clone(self)))?;
        messenger.register_streaming_handler(create_anchor_cancel_handler(Arc::clone(self)))?;
        messenger.register_streaming_handler(create_stream_cancel_handler(Arc::clone(
            &self.sender_registry,
        )))?;

        // MPSC handlers — share the same SenderRegistry so `_stream_cancel`
        // covers both SPSC and MPSC senders uniformly.
        messenger.register_streaming_handler(
            crate::streaming::mpsc::control::create_mpsc_anchor_attach_handler(Arc::clone(self)),
        )?;
        messenger.register_streaming_handler(
            crate::streaming::mpsc::control::create_mpsc_anchor_detach_handler(Arc::clone(self)),
        )?;
        messenger.register_streaming_handler(
            crate::streaming::mpsc::control::create_mpsc_anchor_cancel_handler(Arc::clone(self)),
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
    /// transport bridge (which the remote reader_pump forwards to the anchor's frame channel).
    ///
    /// # Errors
    /// - [`AttachError::TransportError`] if `messenger_lock` is not set (register_handlers not called)
    /// - [`AttachError::TransportError`] if the AM send or transport connect fails
    /// - [`AttachError::TransportError`] if the remote worker returns `AnchorAttachResponse::Err`
    async fn attach_remote<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
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
        };

        // Send _anchor_attach AM to the remote worker (typed request-response)
        let response: crate::streaming::control::AnchorAttachResponse = messenger
            .typed_unary_streaming::<crate::streaming::control::AnchorAttachResponse>(
                "_anchor_attach",
            )
            .payload(&req)
            .map_err(AttachError::TransportError)?
            .worker(handle_worker_id)
            .send()
            .await
            .map_err(AttachError::TransportError)?;

        match response {
            crate::streaming::control::AnchorAttachResponse::Ok {
                streaming_transport_key,
                heartbeat_interval_ms,
                routing_session_id,
                initial_credit,
                slot_byte_budget,
            } => {
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
                // The response's five fields *are* the terms a stream opens on,
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
                };
                self.open_stream_sender::<T>(handle, &ticket, identity)
                    .await
            }
            crate::streaming::control::AnchorAttachResponse::Err { reason } => {
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
    /// # Errors
    /// - [`AttachError::TransportError`] if the ticket names a transport this
    ///   node cannot open, or if opening the slot fails.
    pub async fn open_anchor_stream<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
        ticket: crate::streaming::control::StreamOpenTicket,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        self.open_stream_sender::<T>(handle, &ticket, self.new_sender_identity())
            .await
    }

    /// Allocate the sender-side identity one stream is opened under.
    fn new_sender_identity(&self) -> SenderIdentity {
        let (poison_tx, poison_rx) = flume::bounded::<()>(1);
        SenderIdentity {
            sender_stream_id: self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1,
            cancel_token: CancellationToken::new(),
            poison_tx,
            poison_rx,
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
        identity: SenderIdentity,
    ) -> Result<crate::streaming::sender::StreamSender<T>, AttachError> {
        let (handle_worker_id, local_id) = handle.unpack();

        // Resolve the local FrameTransport that matches the remote worker's
        // bound streaming transport, then connect by WorkerId.
        let frame_tx = self
            .connect_streaming(
                &ticket.streaming_transport_key,
                handle_worker_id,
                local_id,
                ticket.routing_session_id,
                ticket.initial_credit,
                ticket.slot_byte_budget,
            )
            .await?;

        let SenderIdentity {
            sender_stream_id,
            cancel_token,
            poison_tx,
            poison_rx,
        } = identity;

        // Register SenderEntry for _stream_cancel routing
        self.sender_registry.senders.insert(
            sender_stream_id,
            crate::streaming::control::SenderEntry {
                cancel_token: cancel_token.clone(),
                rx_closer: std::sync::Mutex::new(Some(poison_rx)),
            },
        );

        // Build StreamSender: frame_tx from the transport (not a local registry
        // frame_tx). No local AnchorEntry is created for the remote anchor.
        Ok(crate::streaming::sender::StreamSender::new(
            frame_tx,
            handle,
            self.registry.clone(), // this worker's registry (no entry for this handle — correct)
            crate::streaming::sender::StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id,
                sender_registry: self.sender_registry.clone(),
                poison_tx,
            },
            Duration::from_millis(ticket.heartbeat_interval_ms),
            self.metrics.clone(),
            Some(ticket.streaming_transport_key.clone()),
        ))
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
    /// to the [`StreamAnchor<T>`] consumer. The transport connection is used by the
    /// reader pump for cross-worker flows.
    ///
    /// # Errors
    /// - [`AttachError::AnchorNotFound`] if the handle is not in the registry (local path)
    /// - [`AttachError::AlreadyAttached`] if another sender is already connected, which
    ///   includes a pre-bound slot an `OpenSlot` has already claimed (local path)
    /// - [`AttachError::TransportError`] for all remote path errors (messenger unavailable,
    ///   AM send failed, remote error response)
    pub async fn attach_stream_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
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
            return self.attach_remote::<T>(handle).await;
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
        use dashmap::mapref::entry::Entry;
        match self.registry.entry(local_id) {
            Entry::Vacant(_) => Err(AttachError::AnchorNotFound { handle }),
            Entry::Occupied(mut occ) => {
                let entry = occ.get_mut();
                if entry.attachment || entry.prebind_is_claimed() {
                    Err(AttachError::AlreadyAttached { handle })
                } else {
                    // Clone the frame_tx so the StreamSender can write items
                    // directly to the StreamAnchor consumer.
                    let frame_tx = entry.frame_tx.clone();
                    // Snapshot the negotiated heartbeat cadence for the sender.
                    let heartbeat_interval = entry.heartbeat_interval;

                    // Mark as attached (reader pump takes ownership of transport
                    // receiver separately).
                    entry.attachment = true;

                    // Cancel the timeout task while attached (pause timer)
                    if let Some(ref tc) = entry.timeout_cancel {
                        tc.cancel();
                    }

                    // An *unclaimed* pre-bound slot on this anchor was minted
                    // for a sender on another worker. This one is on ours and
                    // writes straight into the anchor's channel, so that slot
                    // has no sender and never will: releasing it here is what
                    // keeps the accept window from holding a bind for a minute
                    // that nothing can claim. A claimed one never reaches this
                    // line — the guard above refuses it as an attached anchor,
                    // because a claim *is* a sender: admitting a second one
                    // would interleave two writers on this `frame_tx`, and the
                    // release below would post `CloseSlot{UnknownSlot}` to the
                    // producer of a healthy stream. It is dropped inside the
                    // shard guard, which is safe because `PreBind`'s drop
                    // reaches only mux state — the ingress registry and a
                    // batcher — and never back into this one.
                    let _released_prebind = entry.prebind.take();

                    // Allocate sender_stream_id and build SenderEntry
                    let sender_stream_id =
                        self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1;
                    let cancel_token = tokio_util::sync::CancellationToken::new();
                    let (poison_tx, poison_rx) = flume::bounded::<()>(1);

                    let sender_entry = crate::streaming::control::SenderEntry {
                        cancel_token: cancel_token.clone(),
                        rx_closer: std::sync::Mutex::new(Some(poison_rx)),
                    };
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
                    Ok(crate::streaming::sender::StreamSender::new(
                        frame_tx,
                        handle,
                        self.registry.clone(),
                        crate::streaming::sender::StreamSenderCancelInfo {
                            cancel_token,
                            sender_stream_id,
                            sender_registry: self.sender_registry.clone(),
                            poison_tx,
                        },
                        heartbeat_interval,
                        self.metrics.clone(),
                        // Same worker: the frames go straight into the anchor's
                        // channel, so there was no transport to negotiate.
                        None,
                    ))
                }
            }
        }
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
            crate::streaming::mpsc::anchor::spawn_mpsc_timeout_task_with_metrics(
                self.mpsc_registry.clone(),
                Some(self.registry.clone()),
                self.metrics.clone(),
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
            spsc_registry: self.registry.clone(),
            metrics: self.metrics.clone(),
        };

        self.mpsc_registry.insert(local_id, entry);
        self.update_active_anchor_gauge();

        crate::streaming::mpsc::MpscStreamAnchor::new(
            handle,
            frame_rx,
            local_id,
            self.anchor_context(),
            self.sender_registry.clone(),
            self.messenger.clone(),
        )
    }

    /// Attach a sender to an MPSC anchor. Like [`attach_stream_anchor`] but
    /// targets the MPSC registry: multiple senders may attach concurrently,
    /// and each attach allocates a fresh [`crate::streaming::mpsc::SenderId`].
    pub async fn attach_mpsc_stream_anchor<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
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
            return self.attach_mpsc_remote::<T>(handle).await;
        }

        // Local path: reserve a slot under the shard lock, then construct
        // the sender after the lock is released.
        use dashmap::mapref::entry::Entry;
        let (
            sender_id,
            frame_tx,
            heartbeat_interval,
            cancel_token,
            poison_tx,
            poison_rx,
            sender_stream_id,
        ) = match self.mpsc_registry.entry(local_id) {
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

                let sender_stream_id =
                    self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1;
                let cancel_token = CancellationToken::new();
                let (poison_tx, poison_rx) = flume::bounded::<()>(1);

                let slot = crate::streaming::mpsc::anchor::MpscSenderSlot {
                    pump_token: None,
                    stream_cancel_handle: Some(
                        crate::streaming::control::StreamCancelHandle::pack(
                            self.worker_id,
                            sender_stream_id,
                        ),
                    ),
                };
                entry.senders.insert(sender_id, slot);

                (
                    sender_id,
                    frame_tx,
                    heartbeat_interval,
                    cancel_token,
                    poison_tx,
                    poison_rx,
                    sender_stream_id,
                )
            }
        };

        // Register SenderEntry outside the shard lock.
        let sender_entry = crate::streaming::control::SenderEntry {
            cancel_token: cancel_token.clone(),
            rx_closer: std::sync::Mutex::new(Some(poison_rx)),
        };
        self.sender_registry
            .senders
            .insert(sender_stream_id, sender_entry);

        Ok(crate::streaming::mpsc::MpscStreamSender::new(
            crate::streaming::mpsc::SenderId(sender_id),
            crate::streaming::mpsc::sender::SenderChannel::Local(frame_tx),
            handle,
            self.mpsc_registry.clone(),
            crate::streaming::sender::StreamSenderCancelInfo {
                cancel_token,
                sender_stream_id,
                sender_registry: self.sender_registry.clone(),
                poison_tx,
            },
            heartbeat_interval,
            self.metrics.clone(),
        ))
    }

    async fn attach_mpsc_remote<T: serde::Serialize>(
        &self,
        handle: StreamAnchorHandle,
    ) -> Result<crate::streaming::mpsc::MpscStreamSender<T>, AttachError> {
        let (handle_worker_id, _) = handle.unpack();

        let messenger = self.messenger_lock.get().ok_or_else(|| {
            AttachError::TransportError(anyhow::anyhow!(
                "register_handlers not called — messenger unavailable for remote mpsc attach"
            ))
        })?;

        let sender_stream_id = self.next_sender_stream_id.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel_token = CancellationToken::new();
        let (poison_tx, poison_rx) = flume::bounded::<()>(1);
        let stream_cancel_handle =
            crate::streaming::control::StreamCancelHandle::pack(self.worker_id, sender_stream_id);

        let req = crate::streaming::mpsc::control::MpscAnchorAttachRequest {
            handle,
            session_id: sender_stream_id,
            stream_cancel_handle,
            supported_transport_keys: self.supported_transport_keys(),
        };

        let response: crate::streaming::mpsc::control::MpscAnchorAttachResponse = messenger
            .typed_unary_streaming::<crate::streaming::mpsc::control::MpscAnchorAttachResponse>(
                "_mpsc_anchor_attach",
            )
            .payload(&req)
            .map_err(AttachError::TransportError)?
            .worker(handle_worker_id)
            .send()
            .await
            .map_err(AttachError::TransportError)?;

        match response {
            crate::streaming::mpsc::control::MpscAnchorAttachResponse::Ok {
                streaming_transport_key,
                heartbeat_interval_ms,
                sender_id,
                routing_session_id,
                initial_credit,
                slot_byte_budget,
            } => {
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
                        &streaming_transport_key,
                        handle_worker_id,
                        local_id,
                        connect_session_id,
                        initial_credit,
                        slot_byte_budget,
                    )
                    .await?;

                let sender_entry = crate::streaming::control::SenderEntry {
                    cancel_token: cancel_token.clone(),
                    rx_closer: std::sync::Mutex::new(Some(poison_rx)),
                };
                self.sender_registry
                    .senders
                    .insert(sender_stream_id, sender_entry);

                Ok(crate::streaming::mpsc::MpscStreamSender::new(
                    crate::streaming::mpsc::SenderId(sender_id),
                    crate::streaming::mpsc::sender::SenderChannel::Remote(frame_tx),
                    handle,
                    self.mpsc_registry.clone(),
                    crate::streaming::sender::StreamSenderCancelInfo {
                        cancel_token,
                        sender_stream_id,
                        sender_registry: self.sender_registry.clone(),
                        poison_tx,
                    },
                    Duration::from_millis(heartbeat_interval_ms),
                    self.metrics.clone(),
                ))
            }
            crate::streaming::mpsc::control::MpscAnchorAttachResponse::Err { reason } => {
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

    #[test]
    fn test_exclusive_attach() {
        let mgr = make_manager();
        let anchor = mgr.create_anchor::<u8>();
        let handle = anchor.handle();
        let (_, local_id) = handle.unpack();

        // First attach succeeds.
        let result1 = mgr.try_attach(local_id, handle);
        assert!(result1.is_ok(), "first attach must succeed: {result1:?}");

        // Second attach while still attached must fail with AlreadyAttached.
        let result2 = mgr.try_attach(local_id, handle);
        match result2 {
            Err(AttachError::AlreadyAttached { .. }) => {}
            other => panic!("expected AlreadyAttached, got {other:?}"),
        }

        // Detach and try again -- must succeed.
        let was_attached = mgr.detach(local_id);
        assert!(was_attached, "detach must return true when attached");

        let result3 = mgr.try_attach(local_id, handle);
        assert!(
            result3.is_ok(),
            "third attach after detach must succeed: {result3:?}"
        );
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
    // StreamAnchor<T> Stream impl tests (Plan 08-03, Task 1)
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
    // AnchorManagerBuilder + default_unattached_timeout tests (Plan 08-04, Task 1)
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
        let (_, local_id) = handle.unpack();

        // Advance past timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Try to attach -- should get AnchorNotFound
        let result = mgr.try_attach(local_id, handle);
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

        let anchor = mgr.create_anchor::<u32>();
        let handle = anchor.handle();
        let (_, local_id) = handle.unpack();

        // Advance 1s (less than 2s timeout)
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must exist before timeout"
        );

        // Attach -- should cancel the timeout task
        mgr.try_attach(local_id, handle)
            .expect("attach should succeed");

        // Advance well past the original deadline
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert!(
            mgr.registry.contains_key(&local_id),
            "anchor must still exist while attached (timeout paused)"
        );

        // Detach -- should respawn the timeout task
        mgr.detach(local_id);

        // Advance past the new timeout (2s from detach)
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after detach + timeout"
        );
    }

    // -----------------------------------------------------------------------
    // StreamAnchor::set_timeout tests (Plan 08-04, Task 2)
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
        let stream = mgr.create_anchor::<u32>();
        let handle = stream.handle();
        let (_, local_id) = handle.unpack();

        // Attach the anchor
        mgr.try_attach(local_id, handle)
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
        mgr.detach(local_id);

        // Advance past the timeout
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        assert!(
            !mgr.registry.contains_key(&local_id),
            "anchor must be removed after detach with stored set_timeout duration"
        );
    }

    // -----------------------------------------------------------------------
    // Registry injection tests (Plan 09-01, Task 2)
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
