// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Control-plane handler constructors for the anchor lifecycle.
//!
//! This module provides four [`crate::messenger::Handler`] constructors:
//! - [`create_anchor_attach_handler`]: validates anchor existence, calls
//!   `transport.bind().await` (outside shard lock), then atomically stores
//!   the [`flume::Receiver`] in the anchor entry.
//! - [`create_anchor_detach_handler`]: clears attachment, cancels CancellationToken,
//!   injects [`crate::streaming::frame::StreamFrame::Detached`] sentinel; anchor stays in registry.
//! - [`create_anchor_finalize_handler`]: injects [`crate::streaming::frame::StreamFrame::Finalized`]
//!   sentinel, then removes anchor from registry.
//! - [`create_anchor_cancel_handler`]: removes anchor from registry with no sentinel injection.
//!
//! It also re-exports [`StreamOpenTicket`] (minted by
//! [`crate::streaming::anchor::AnchorManager::prebind_anchor`] for zero-RTT
//! stream setup), and holds the two ways a bind reaches its consumer, both
//! `pub(crate)` because every caller is in-crate: the reader pump ([`pump`])
//! that the attach handler spawns for a per-stream transport, and the direct
//! feed ([`feed`]) that the attach handler and the zero-RTT open path start
//! for a mux bind.

use crate::observability::{HandlerOutcome, StreamingOp};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Weak};
use std::time::Instant;

use crate::streaming::anchor::AnchorManager;
use crate::streaming::handle::StreamAnchorHandle;

/// Public factories own their manager. Internal handlers borrow it to avoid
/// the manager -> messenger -> handler -> manager ownership cycle.
#[derive(Clone)]
pub(crate) enum AnchorManagerRef {
    Strong(Arc<AnchorManager>),
    Weak(Weak<AnchorManager>),
}

impl AnchorManagerRef {
    pub(crate) fn upgrade(&self) -> Option<Arc<AnchorManager>> {
        match self {
            Self::Strong(manager) => Some(Arc::clone(manager)),
            Self::Weak(manager) => manager.upgrade(),
        }
    }
}

/// Number of consecutive missed heartbeat windows that trigger `Dropped` injection.
///
/// The consumer tolerates about `DETECTION_MULTIPLIER * heartbeat_interval` of total
/// silence before declaring the sender dead. Two tasks measure it, each with one timer per
/// stream:
///
/// - [`reader_pump`], for a per-stream transport, pushes its timer forward on a received
///   frame only once it is inside half a window, and otherwise re-arms it from its own fire,
///   comparing each fire against the last frame's instant. The misses it counts are
///   consecutive windows of silence, a stream still carrying frames never fires it, and
///   detection lands exactly `DETECTION_MULTIPLIER` windows after the last frame. See
///   [`reader_pump`] for why.
/// - The mux's stream watchdog (`feed::stream_watchdog`) never sees a frame. It wakes once
///   per window on its own clock and counts a window as dead when the ingress delivered
///   nothing to the slot while its sender still held data credit, and so could have
///   sent a heartbeat. Detection lands between
///   `DETECTION_MULTIPLIER` and `DETECTION_MULTIPLIER + 1` windows after the last arrival.
///
/// Both the producer (`StreamSender`) heartbeat cadence and the consumer's per-window
/// deadline are negotiated via `AnchorAttachResponse::heartbeat_interval_ms`, but the
/// multiplier itself is a protocol constant agreed by both sides.
pub const DETECTION_MULTIPLIER: u8 = 3;

/// Default heartbeat interval (milliseconds) used when `AnchorAttachResponse::Ok` is
/// deserialized from a wire payload that predates the `heartbeat_interval_ms` field.
/// Matches the historical hardcoded 5s constant.
fn default_heartbeat_interval_ms() -> u64 {
    5_000
}

// ---------------------------------------------------------------------------
// StreamCancelHandle
// ---------------------------------------------------------------------------

/// Compact wire handle encoding the sender's [`velo_ext::WorkerId`] (upper 64 bits)
/// and the sender's local stream ID (lower 64 bits) into a single `u128`.
///
/// Serializes via rmp-serde as a two-field struct `{hi: u64, lo: u64}` — not as raw
/// binary bytes — to guarantee correct round-tripping across msgpack boundaries.
/// Identical encoding to [`StreamAnchorHandle`] but scoped to the sender side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamCancelHandle(u128);

/// Private wire representation for rmp-serde serialization.
///
/// rmp-serde encodes a raw `u128` as a MessagePack binary blob (`bin8`), which
/// cannot be decoded back to a struct. By delegating to this two-field struct we
/// encode as a fixmap with named fields that round-trip correctly.
#[derive(Serialize, Deserialize)]
struct StreamCancelHandleWire {
    hi: u64,
    lo: u64,
}

impl StreamCancelHandle {
    /// Encode a sender [`velo_ext::WorkerId`] and stream ID into a [`StreamCancelHandle`].
    pub fn pack(worker_id: velo_ext::WorkerId, stream_id: u64) -> Self {
        Self(((worker_id.as_u64() as u128) << 64) | (stream_id as u128))
    }

    /// Decode the sender [`velo_ext::WorkerId`] and stream ID from this handle.
    pub fn unpack(self) -> (velo_ext::WorkerId, u64) {
        let hi = (self.0 >> 64) as u64;
        let lo = self.0 as u64;
        (velo_ext::WorkerId::from_u64(hi), lo)
    }
}

impl Serialize for StreamCancelHandle {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        StreamCancelHandleWire {
            hi: (self.0 >> 64) as u64,
            lo: self.0 as u64,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StreamCancelHandle {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = StreamCancelHandleWire::deserialize(deserializer)?;
        Ok(Self(((wire.hi as u128) << 64) | (wire.lo as u128)))
    }
}

// ---------------------------------------------------------------------------
// StreamCancelRequest
// ---------------------------------------------------------------------------

/// Payload for the `_stream_cancel` active message.
///
/// The receiver (sender-side worker) looks up `sender_stream_id` in the
/// [`SenderRegistry`] to find and cancel the corresponding [`SenderEntry`].
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamCancelRequest {
    pub sender_stream_id: u64,
}

// ---------------------------------------------------------------------------
// SenderEntry + SenderRegistry
// ---------------------------------------------------------------------------

/// A single slot in the sender-side registry, representing an active [`crate::streaming::sender::StreamSender`].
///
/// Stored per active stream. The `_stream_cancel` handler retrieves and removes
/// the entry then cancels its token. The token also wakes blocked sends.
pub struct SenderEntry {
    /// Fires when `_stream_cancel` is received — user-facing via `cancellation_token()`.
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Graceful stop leaves the response channel open.
    pub stop_token: tokio_util::sync::CancellationToken,
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
            entry.cancel_token.cancel();
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
        let messenger = Arc::clone(messenger);
        rt.spawn(async move {
            let payload = serde_json::to_vec(&StreamCancelRequest { sender_stream_id })
                .expect("stream identity");
            let _ = messenger
                .am_send_streaming("_stream_cancel")
                .expect("stream cancel handler")
                .raw_payload(bytes::Bytes::from(payload))
                .worker(worker)
                .send()
                .await;
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
        let messenger = Arc::clone(messenger);
        rt.spawn(async move {
            let payload = serde_json::to_vec(&StreamCancelRequest { sender_stream_id })
                .expect("stream identity");
            let _ = messenger
                .am_send_streaming("_stream_stop")
                .expect("stream stop handler")
                .raw_payload(bytes::Bytes::from(payload))
                .worker(worker)
                .send()
                .await;
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

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// Request to attach a transport sender to an anchor.
///
/// `session_id` is an opaque caller-assigned identifier that may be forwarded
/// to the transport layer for logging and routing purposes.
///
/// `stream_cancel_handle` encodes the sender's worker ID and local stream ID so that
/// the anchor can route `_stream_cancel` active messages back to the correct sender.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnchorAttachRequest {
    pub handle: StreamAnchorHandle,
    pub session_id: u64,
    /// Encodes the sender's WorkerId + sender_stream_id. Stored in the anchor entry
    /// on successful attach so the anchor knows where to route upstream cancel AMs.
    pub stream_cancel_handle: StreamCancelHandle,
    /// Streaming transports this sender has installed and can therefore be
    /// asked to `connect()` on.
    ///
    /// The receiver intersects this with its own installed set and prefers
    /// `messenger-mux-v2` when it appears in both. `#[serde(default)]` means a
    /// sender that predates negotiation deserializes as one advertising
    /// nothing, which is exactly right: an empty list can never intersect, so
    /// absent a pre-bound slot on the anchor, such a sender is always answered
    /// with the receiver's default transport key — the behaviour it already
    /// expects. When the anchor holds an unclaimed pre-bind whose key this
    /// sender does not advertise, `adopt_prebind` refuses the attach instead
    /// of falling through to that default.
    #[serde(default)]
    pub supported_transport_keys: Vec<velo_ext::TransportKey>,
    /// A key for the receiver to place the stream's mux lane by, so streams
    /// with one key share a lane. `None` lets the receiver choose the lane
    /// with the fewest streams from this sender.
    ///
    /// Left out when `None`, so a request without a key is the same bytes as
    /// one from before lanes. A receiver from before lanes ignores it and
    /// places every stream on lane 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane_key: Option<u64>,
}

/// Whether a lane on the wire is lane 0, which is left out of what is sent so
/// that lane-0 terms are the same bytes they were before lanes.
pub(crate) fn is_zero_lane(lane: &u16) -> bool {
    *lane == 0
}

/// Response from the attach handler.
#[derive(Debug, Serialize, Deserialize)]
pub enum AnchorAttachResponse {
    /// Attach succeeded.
    ///
    /// `streaming_transport_key` tells the client which `FrameTransport` it
    /// should call `connect()` on (looked up in the local
    /// [`crate::streaming::AnchorManager`]'s transport registry). Endpoint
    /// resolution is no longer string-based — the chosen transport extracts
    /// the peer's listener address from the cached WorkerAddress entry it
    /// stored on `register()`.
    ///
    /// `heartbeat_interval_ms` tells the sender how often it must emit a
    /// [`crate::streaming::frame::StreamFrame::Heartbeat`] when no data frames are flowing.
    /// The consumer (its reader pump, or the mux's stream watchdog) will tolerate about
    /// `DETECTION_MULTIPLIER * heartbeat_interval_ms` of total silence before injecting
    /// `Dropped`. The field is carried as `u64` ms (rather than `Duration`) for stable
    /// msgpack encoding, and defaults to 5000ms when absent.
    Ok {
        streaming_transport_key: velo_ext::TransportKey,
        #[serde(default = "default_heartbeat_interval_ms")]
        heartbeat_interval_ms: u64,
        /// Receiver-allocated routing slot id. The sender uses this for the
        /// `transport.connect(...)` call so the transport-layer
        /// `(anchor_id, session_id)` routing key is globally unique on the
        /// receiver, independent of the sender's local stream counter.
        /// `#[serde(default)]` so older senders (which never set it) still
        /// deserialize; in that case the sender falls back to its local
        /// `sender_stream_id` (the legacy collision-prone behavior).
        #[serde(default)]
        routing_session_id: u64,
        /// Data credit the receiver grants each new mux slot — and therefore
        /// the depth of the buffer it sized behind that slot.
        ///
        /// Zero is **not** a small window. It means *this peer is not offering
        /// the mux*, and the sender must not drive one even if
        /// `streaming_transport_key` matched: a sender that guessed a window
        /// would push into a buffer the receiver never sized. Every older peer
        /// deserializes as exactly that, because `#[serde(default)]` fills the
        /// absent field with zero.
        #[serde(default)]
        initial_credit: u32,
        /// Bytes one mux slot may hold in flight.
        ///
        /// Zero here means something different from zero above: *use the
        /// default*. The asymmetry is deliberate and
        /// `docs/src/concepts/batched-streaming.md` is its authority — a
        /// credit window cannot be defaulted safely because only
        /// the receiver knows what it allocated, whereas the byte cap is a
        /// memory bound both sides can agree on without being told. The mux's
        /// internal `NegotiatedLimits::from_wire` is the one place that split
        /// is encoded.
        #[serde(default)]
        slot_byte_budget: u32,
        /// The mux lane the receiver put the slot on. The sender opens the
        /// slot on `lane % (lanes it keeps to the receiver)`.
        ///
        /// An older receiver sends no lane, which reads as lane 0, the one lane
        /// it has. Left out when zero, so a lane-0 answer is the same bytes as
        /// before lanes. Always zero for a transport other than the mux.
        #[serde(default, skip_serializing_if = "is_zero_lane")]
        lane: u16,
    },
    /// Attach failed; `reason` describes why.
    Err { reason: String },
}

mod feed;
mod pump;
mod ticket;
#[cfg(test)]
pub(crate) use feed::stream_watchdog;
pub(crate) use feed::{
    DirectFeed, FeedCell, SlotRelease, WatchdogContext, install_direct_feed, launch_direct_stream,
    reap_unclaimed,
};
pub(crate) use pump::{PumpContext, note_timer_arm, note_timer_fire, reader_pump};
#[cfg(test)]
pub(crate) use pump::{TIMER_ARMS, TIMER_FIRES};
pub use ticket::StreamOpenTicket;

/// Request to detach the current sender from an anchor without closing it.
///
/// After detach the anchor remains in the registry so a new sender may attach.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnchorDetachRequest {
    pub handle: StreamAnchorHandle,
}

/// Request to finalize (permanently close) an anchor.
///
/// After finalize the anchor is removed from the registry.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnchorFinalizeRequest {
    pub handle: StreamAnchorHandle,
}

/// Request to cancel an anchor with no sentinel injection.
///
/// Used when a sender exits before attaching or when an explicit abort is needed.
/// After cancel the anchor is removed from the registry.
#[derive(Debug, Serialize, Deserialize)]
pub struct AnchorCancelRequest {
    pub handle: StreamAnchorHandle,
}

// ---------------------------------------------------------------------------
// Handler constructors
// ---------------------------------------------------------------------------

/// Build the `_anchor_attach` handler.
///
/// Uses the bind-then-lock pattern: calls `transport.bind().await` OUTSIDE the
/// DashMap shard lock, then atomically checks and sets the attachment under the lock.
/// This avoids holding the shard lock across an async `.await` point.
///
/// Returns [`AnchorAttachResponse::Ok`] on success or [`AnchorAttachResponse::Err`] on
/// any failure (not found, already attached, transport error).
pub fn create_anchor_attach_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    anchor_attach_handler(AnchorManagerRef::Strong(manager))
}

pub(crate) fn anchor_attach_handler(manager: AnchorManagerRef) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_attach",
        move |ctx: crate::messenger::TypedContext<AnchorAttachRequest>| {
            let manager = manager.clone();
            async move {
                let manager = manager
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("anchor manager shut down"))?;
                let started = Instant::now();
                // The worker whose batches will carry this stream: the lane is placed
                // against its load. From the envelope, not from the request body.
                let sender = ctx.sender_worker_id();
                let req = ctx.input;

                // Defence-in-depth: reject MPSC handles at the SPSC attach
                // endpoint. The client-side `attach_stream_anchor` already
                // rejects these before the AM, but misbehaving or older
                // clients may still hit the wire.
                if req.handle.is_mpsc_stream() {
                    manager.record_streaming_operation(
                        StreamingOp::Attach,
                        HandlerOutcome::Error,
                        "unknown",
                        started,
                    );
                    return Ok(AnchorAttachResponse::Err {
                        reason: format!("anchor {} is mpsc; use _mpsc_anchor_attach", req.handle),
                    });
                }

                let (_, local_id) = req.handle.unpack();

                // Step 0: adopt a pre-bound slot, if this anchor is holding one.
                //
                // Zero-RTT setup does at request registration what the rest of
                // this handler does on the round trip: negotiate, bind, and
                // start the consumer side. A sender that speaks the attach
                // protocol anyway — an older worker, or one whose envelope
                // carried no ticket — must be given *that* slot rather than a
                // second one: binding again would leave the first bind
                // unclaimed with a watchdog waiting on a sender that never
                // comes, and the sender would open against a routing session
                // the pre-bind never expects an OpenSlot for.
                //
                // Ahead of the already-attached check because a pre-bound
                // anchor is not attached; nothing has claimed it yet, which is
                // exactly what makes it adoptable.
                match manager.adopt_prebind(local_id, &req) {
                    crate::streaming::anchor::PrebindAdoption::None => {}
                    crate::streaming::anchor::PrebindAdoption::Adopted(ticket) => {
                        manager.record_streaming_operation(
                            StreamingOp::Attach,
                            HandlerOutcome::Success,
                            ticket.streaming_transport_key.as_str(),
                            started,
                        );
                        return Ok(AnchorAttachResponse::Ok {
                            streaming_transport_key: ticket.streaming_transport_key,
                            heartbeat_interval_ms: ticket.heartbeat_interval_ms,
                            routing_session_id: ticket.routing_session_id,
                            initial_credit: ticket.initial_credit,
                            slot_byte_budget: ticket.slot_byte_budget,
                            // The lane the pre-bind was minted on: the
                            // adopting sender must open where the ticket
                            // would have.
                            lane: ticket.lane,
                        });
                    }
                    crate::streaming::anchor::PrebindAdoption::Refused(reason) => {
                        manager.record_streaming_operation(
                            StreamingOp::Attach,
                            HandlerOutcome::Error,
                            "unknown",
                            started,
                        );
                        return Ok(AnchorAttachResponse::Err { reason });
                    }
                }

                // Step 1: Quick check -- anchor exists and is unattached (drop lock)
                {
                    let entry = manager.registry.get(&local_id);
                    match entry {
                        None => {
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            return Ok(AnchorAttachResponse::Err {
                                reason: format!("anchor {} not found", req.handle),
                            });
                        }
                        Some(e) if e.attachment => {
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            return Ok(AnchorAttachResponse::Err {
                                reason: format!("anchor {} already attached", req.handle),
                            });
                        }
                        _ => {} // looks good, proceed
                    }
                } // DashMap ref dropped here

                // Step 2: Async bind OUTSIDE shard lock.
                //
                // Allocate a receiver-side routing_session_id rather than
                // reusing the sender's local stream counter (req.session_id):
                // two senders from different workers both attaching to the
                // same anchor would otherwise hit the same `(local_id,
                // session_id)` routing slot and silently overwrite each
                // other at the transport layer. See
                // [`crate::streaming::AnchorManager::next_routing_session_id`].
                let routing_session_id = manager
                    .next_routing_session_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                // Which transport this attach rides is decided here, from what
                // the sender advertised: `messenger-mux-v2` when both sides
                // named it. Otherwise use the per-stream default, or reject
                // the peer if this instance is mux-only.
                let selection = manager.select_streaming_transport(
                    &req.supported_transport_keys,
                    sender,
                    req.lane_key,
                );
                let bound = match selection {
                    Ok(selection) => selection.bind(local_id, routing_session_id).await,
                    Err(error) => Err(error),
                };
                let (receiver, terms) = match bound {
                    Ok(bound) => bound,
                    Err(e) => {
                        manager.record_streaming_operation(
                            StreamingOp::Attach,
                            HandlerOutcome::Error,
                            "unknown",
                            started,
                        );
                        return Ok(AnchorAttachResponse::Err {
                            reason: format!("transport error: {}", e),
                        });
                    }
                };
                let streaming_transport_key = terms.key;

                // Step 3: Atomically set attachment under shard lock
                use dashmap::mapref::entry::Entry;
                match manager.registry.entry(local_id) {
                    // The three arms that fail after the bind give it back
                    // at once rather than to the accept window.
                    Entry::Vacant(_) => {
                        manager.release_unused_bind(
                            &streaming_transport_key,
                            local_id,
                            routing_session_id,
                        );
                        manager.record_streaming_operation(
                            StreamingOp::Attach,
                            HandlerOutcome::Error,
                            "unknown",
                            started,
                        );
                        Ok(AnchorAttachResponse::Err {
                            reason: format!("anchor {} removed during bind", req.handle),
                        })
                    }
                    Entry::Occupied(mut occ) => {
                        let entry = occ.get_mut();
                        if entry.attachment {
                            manager.release_unused_bind(
                                &streaming_transport_key,
                                local_id,
                                routing_session_id,
                            );
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            Ok(AnchorAttachResponse::Err {
                                reason: format!("anchor {} already attached", req.handle),
                            })
                        } else if entry.prebind.is_some() {
                            // A pre-bind that landed while this handler was
                            // awaiting its bind. Step 0 looked before the await
                            // and found none, and `attachment` never says so —
                            // which is why this asks the same pair
                            // `prebind_anchor` asks (`anchor.rs`). Binding over
                            // it would leave two tasks serving one anchor
                            // and two live routing sessions, with
                            // `active_pump_token` naming only the newer, so
                            // nothing could ever cancel the older.
                            manager.release_unused_bind(
                                &streaming_transport_key,
                                local_id,
                                routing_session_id,
                            );
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            Ok(AnchorAttachResponse::Err {
                                reason: format!(
                                    "anchor {} was pre-bound while this attach was binding",
                                    req.handle
                                ),
                            })
                        } else {
                            // Derive a child token for this pump or watchdog so detach can cancel
                            // it without cancelling the parent (which lives for the anchor's lifetime).
                            let pump_cancel = entry.cancel_token.child_token();
                            entry.active_pump_token = Some(pump_cancel.clone());
                            let pump_frame_tx = entry.frame_tx.clone();
                            // Snapshot the negotiated heartbeat interval before dropping the lock.
                            let heartbeat_interval = entry.heartbeat_interval;

                            // Mark as attached and store cancel handle for upstream cancel routing
                            entry.attachment = true;
                            entry.stream_cancel_handle = Some(req.stream_cancel_handle);
                            if entry.stop_requested {
                                request_sender_stop(
                                    req.stream_cancel_handle,
                                    // The anchor's worker is this worker.
                                    req.handle.unpack().0,
                                    &manager.sender_registry,
                                    manager.messenger_lock.get(),
                                );
                            }

                            // Start the stream's consumer side. Only the mux
                            // parks a drain signal, and only for the pair it
                            // just bound: a mux bind's consumer reads the slot
                            // buffer itself (see `control::feed`), and its feed
                            // is installed before the shard lock drops, so a
                            // retire or a removal cannot land in between. Every
                            // other transport gets a reader pump.
                            let (_, local_id) = req.handle.unpack();
                            let direct = manager
                                .take_mux_drain_signal(local_id, routing_session_id)
                                .map(|drain| {
                                    install_direct_feed(
                                        entry,
                                        DirectFeed {
                                            rx: receiver.clone(),
                                            drain,
                                            pump_token: pump_cancel.clone(),
                                            release: manager.mux_handle().map(|mux| SlotRelease {
                                                mux,
                                                anchor_id: local_id,
                                                session_id: routing_session_id,
                                            }),
                                        },
                                    )
                                });

                            // Drop shard lock before spawning
                            drop(occ);

                            if let Some((feed, replaced)) = direct {
                                if let Some(replaced) = replaced {
                                    replaced.release_slot();
                                }
                                drop(receiver);
                                launch_direct_stream(
                                    feed,
                                    pump_frame_tx,
                                    manager.anchor_context(),
                                    WatchdogContext {
                                        local_id,
                                        heartbeat_deadline: heartbeat_interval,
                                        // This is the ordinary attach path: a
                                        // sender is already on the wire, not a
                                        // zero-RTT pre-bind waiting for one. No
                                        // `PreBind` exists to share a flag with,
                                        // so this one starts and stays `false`.
                                        prebound: std::sync::Arc::new(
                                            std::sync::atomic::AtomicBool::new(false),
                                        ),
                                    },
                                );
                            } else {
                                tokio::spawn(reader_pump(
                                    receiver,      // transport receiver from bind
                                    pump_frame_tx, // cloned from entry
                                    pump_cancel,   // cloned from entry
                                    manager.anchor_context(),
                                    local_id,
                                    heartbeat_interval,
                                ));
                            }

                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Success,
                                streaming_transport_key.as_str(),
                                started,
                            );

                            Ok(AnchorAttachResponse::Ok {
                                streaming_transport_key,
                                heartbeat_interval_ms: heartbeat_interval.as_millis() as u64,
                                routing_session_id,
                                initial_credit: terms.initial_credit,
                                slot_byte_budget: terms.slot_byte_budget,
                                lane: terms.lane,
                            })
                        }
                    }
                }
            }
        },
    )
    .spawn()
    .build()
}

/// Build the `_anchor_detach` handler.
///
/// Atomically clears `attachment`, releases and re-arms around any pre-bind,
/// and retires the active pump or watchdog (`AnchorEntry::retire_pump`:
/// cancel its token, withdraw any direct feed), all via one
/// `DashMap::entry()` -- the retire has to happen before the shard lock drops
/// and before the released `PreBind` is dropped, so the task this handler is
/// retiring is already told before anything closes the buffer it watches.
/// Only after the lock drops does it inject a
/// [`crate::streaming::frame::StreamFrame::Detached`] sentinel into the frame
/// channel. The anchor remains in the registry so a new sender may re-attach.
///
/// **Not for mux streams.** A mux stream's records sit in the slot buffer the
/// consumer reads directly, apart from the anchor channel this handler writes
/// its sentinel into. Retiring or removing the entry withdraws that feed before the
/// consumer has read what is already buffered, so up to `C` records are lost,
/// and a sender that re-attaches quickly can have its records read ahead of
/// the sentinel. No in-tree sender sends this message: a mux stream ends with
/// its terminal record and `CloseSlot`, in order, on the slot itself.
///
/// Idempotent: if the anchor is not found, returns `Ok(())`.
pub fn create_anchor_detach_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    anchor_detach_handler(AnchorManagerRef::Strong(manager))
}

pub(crate) fn anchor_detach_handler(manager: AnchorManagerRef) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_detach",
        move |ctx: crate::messenger::TypedContext<AnchorDetachRequest>| {
            let manager = manager.clone();
            async move {
                let manager = manager
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("anchor manager shut down"))?;
                let started = Instant::now();
                let req = ctx.input;
                let (_, local_id) = req.handle.unpack();

                use dashmap::mapref::entry::Entry;
                // Atomically clear attachment, release any pre-bind, and clone
                // cancel_token + frame_tx before dropping the shard lock (never
                // hold DashMap ref across channel ops or into `PreBind::drop`,
                // which reaches into mux state).
                let (maybe_entry_info, released_prebind) = match manager.registry.entry(local_id) {
                    Entry::Vacant(_) => (None, None),
                    Entry::Occupied(mut occ) => {
                        let entry = occ.get_mut();
                        // Clear the attachment flag
                        entry.attachment = false;
                        // Release any pre-bind the way `adopt_prebind`'s
                        // `Verdict::Mismatch` arm releases one: under
                        // zero-RTT `attachment` is never set, so a claimed
                        // pre-bind is the only thing that would otherwise
                        // refuse every later attach forever, with nothing
                        // left to reap it. Re-arm the unattached timer for
                        // the same reason Mismatch does: the anchor is
                        // genuinely unattached again.
                        let released = entry.prebind.take();
                        // Called under the `Entry::Occupied` guard held
                        // above: safe now that `spawn_timeout_task` guards
                        // its own `tokio::spawn` — the call never runs
                        // synchronously and never touches this registry, so
                        // there is nothing here for it to deadlock against.
                        entry.restart_unattached_timeout(&manager.registry, local_id);
                        // Take the child token (leaves None) so the next attach creates a fresh one,
                        // cancel it and withdraw the feed here, before the shard lock drops and
                        // before the `released_prebind` below is dropped. Retiring first is
                        // load-bearing for a released pre-bind: `PreBind::drop` closes the unclaimed
                        // bind, and `control::reap_unclaimed` -- run by the watchdog on
                        // `DrainSignal::closed` and by the consumer when its feed's buffer closes --
                        // treats an unclaimed, *uncancelled* close as "the accept window reclaimed
                        // an abandoned pre-bind" and removes the registry entry, which this handler
                        // has just re-armed for reattachment, not abandoned. The cancelled token
                        // stops the watchdog's reap; the withdrawn feed means the consumer never
                        // sees the close. Both are what `retire_pump` does, exactly as
                        // `adopt_prebind`'s `Verdict::Mismatch` arm and the co-located branch of
                        // `attach_stream_anchor` already do.
                        let retired_feed = entry.feed.current();
                        let pump_token = entry.retire_pump();
                        (
                            Some((pump_token, entry.frame_tx.clone(), retired_feed)),
                            released,
                        )
                    }
                };
                // shard lock is now dropped
                drop(released_prebind);

                if let Some((_pump_token, frame_tx, retired_feed)) = maybe_entry_info {
                    // The entry stays for a re-attach, so its `Drop` will not
                    // close this stream's mux slot; with no `PreBind` (an
                    // ordinary attach) nothing else would tell a parked sender.
                    // Outside the shard lock, as every other close is.
                    if let Some(feed) = retired_feed {
                        feed.release_slot();
                    }
                    let sentinel_bytes = crate::streaming::sender::cached_detached().clone();
                    let _ = frame_tx.try_send(sentinel_bytes);
                    manager.record_streaming_operation(
                        StreamingOp::Detach,
                        HandlerOutcome::Success,
                        "velo",
                        started,
                    );
                } else {
                    manager.record_streaming_operation(
                        StreamingOp::Detach,
                        HandlerOutcome::Error,
                        "velo",
                        started,
                    );
                }

                Ok(())
            }
        },
    )
    .spawn()
    .build()
}

/// A failed connect may undo only the attachment established by this identity.
/// Keep this separate from the legacy, unqualified detach request.
#[derive(Serialize, Deserialize)]
pub(crate) struct AnchorAbortAttachRequest {
    pub handle: StreamAnchorHandle,
    pub stream_cancel_handle: StreamCancelHandle,
}

pub(crate) fn create_anchor_abort_attach_handler(
    manager: AnchorManagerRef,
) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_abort_attach",
        move |ctx: crate::messenger::TypedContext<AnchorAbortAttachRequest>| {
            let manager = manager.clone();
            async move {
                let manager = manager
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("anchor manager shut down"))?;
                let req = ctx.input;
                let (worker, local_id) = req.handle.unpack();
                if worker != ctx.msg.instance_id().worker_id() || req.handle.is_mpsc_stream() {
                    anyhow::bail!("abort attach requires a local SPSC anchor");
                }
                let (feed, prebind) = {
                    let Some(mut entry) = manager.registry.get_mut(&local_id) else {
                        return Ok(());
                    };
                    if !entry.attachment
                        || entry.stream_cancel_handle != Some(req.stream_cancel_handle)
                    {
                        return Ok(());
                    }
                    entry.attachment = false;
                    entry.stream_cancel_handle = None;
                    let feed = entry.feed.current();
                    entry.retire_pump();
                    let prebind = entry.prebind.take();
                    entry.restart_unattached_timeout(&manager.registry, local_id);
                    (feed, prebind)
                };
                // Mux release enters other locks. Release the anchor guard first.
                drop(prebind);
                if let Some(feed) = feed {
                    feed.release_slot();
                }
                // No sender was returned, so there is no Detached event to emit.
                Ok(())
            }
        },
    )
    .spawn()
    .build()
}

/// Build the `_anchor_finalize` handler.
///
/// Atomically removes the anchor from the registry via `remove_anchor()`, injects a
/// [`crate::streaming::frame::StreamFrame::Finalized`] sentinel, and cancels the `CancellationToken`.
///
/// **Not for mux streams.** A mux stream's records sit in the slot buffer the
/// consumer reads directly, apart from the anchor channel this handler writes
/// its sentinel into. Retiring or removing the entry withdraws that feed before the
/// consumer has read what is already buffered, so up to `C` records are lost,
/// and a sender that re-attaches quickly can have its records read ahead of
/// the sentinel. No in-tree sender sends this message: a mux stream ends with
/// its terminal record and `CloseSlot`, in order, on the slot itself.
///
/// Idempotent: if the anchor is already absent, returns `Ok(())`.
pub fn create_anchor_finalize_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    anchor_finalize_handler(AnchorManagerRef::Strong(manager))
}

pub(crate) fn anchor_finalize_handler(manager: AnchorManagerRef) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_finalize",
        move |ctx: crate::messenger::TypedContext<AnchorFinalizeRequest>| {
            let manager = manager.clone();
            async move {
                let manager = manager
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("anchor manager shut down"))?;
                let started = Instant::now();
                let req = ctx.input;
                let (_, local_id) = req.handle.unpack();

                // remove_anchor cancels the token and returns the entry
                if let Some(entry) = manager.remove_anchor(local_id) {
                    let sentinel_bytes = crate::streaming::sender::cached_finalized().clone();
                    let _ = entry.frame_tx.try_send(sentinel_bytes);
                    manager.record_streaming_operation(
                        StreamingOp::Finalize,
                        HandlerOutcome::Success,
                        "velo",
                        started,
                    );
                } else {
                    manager.record_streaming_operation(
                        StreamingOp::Finalize,
                        HandlerOutcome::Error,
                        "velo",
                        started,
                    );
                }

                Ok(())
            }
        },
    )
    .spawn()
    .build()
}

/// Build the `_anchor_cancel` handler.
///
/// Removes the anchor from the registry with no sentinel injection.
/// Used when a sender aborts before or during attachment.
///
/// Idempotent: calling cancel on an already-absent anchor does not panic.
pub fn create_anchor_cancel_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    anchor_cancel_handler(AnchorManagerRef::Strong(manager))
}

pub(crate) fn anchor_cancel_handler(manager: AnchorManagerRef) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_cancel",
        move |ctx: crate::messenger::TypedContext<AnchorCancelRequest>| {
            let manager = manager.clone();
            async move {
                let manager = manager
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("anchor manager shut down"))?;
                let started = Instant::now();
                let req = ctx.input;
                let (_, local_id) = req.handle.unpack();

                // remove_anchor is a no-op (returns None) if anchor absent -- idempotent
                if let Some(entry) = manager.remove_anchor(local_id) {
                    entry.cancel_token.cancel();
                    manager.record_streaming_operation(
                        StreamingOp::Cancel,
                        HandlerOutcome::Success,
                        "velo",
                        started,
                    );
                } else {
                    manager.record_streaming_operation(
                        StreamingOp::Cancel,
                        HandlerOutcome::Error,
                        "velo",
                        started,
                    );
                }

                Ok(())
            }
        },
    )
    .spawn()
    .build()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
