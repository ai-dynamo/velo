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
//! stream setup) and the reader pump ([`pump`], `pub(crate)` only — every
//! caller is in-crate) that all four handlers and the zero-RTT open path
//! spawn onto.

use crate::observability::{HandlerOutcome, StreamingOp};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

use crate::streaming::anchor::AnchorManager;
use crate::streaming::handle::StreamAnchorHandle;

/// Number of consecutive missed heartbeat windows that trigger `Dropped` injection.
///
/// The reader pump tolerates `DETECTION_MULTIPLIER * heartbeat_interval` of total silence
/// (each window of length `heartbeat_interval`) before declaring the sender dead.
/// Both the producer (`StreamSender`) heartbeat cadence and the consumer (`reader_pump`)
/// per-window deadline are negotiated via `AnchorAttachResponse::heartbeat_interval_ms`,
/// but the multiplier itself is a protocol constant agreed by both sides.
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
/// the entry then triggers both the user-facing cancellation token and the
/// poison-drop mechanism via `rx_closer`.
pub struct SenderEntry {
    /// Fires when `_stream_cancel` is received — user-facing via `cancellation_token()`.
    pub cancel_token: tokio_util::sync::CancellationToken,

    /// Drop this to signal cancellation to `StreamSender::send()` via
    /// `poison_tx.is_disconnected()`. Wrapped in `Mutex<Option<...>>` so the
    /// cancel handler can take it exactly once.
    pub rx_closer: std::sync::Mutex<Option<flume::Receiver<()>>>,
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

// ---------------------------------------------------------------------------
// create_stream_cancel_handler
// ---------------------------------------------------------------------------

/// Build the `_stream_cancel` handler.
///
/// When the consumer-side anchor receives a cancel request, it sends a
/// `_stream_cancel` active message to the sender's worker. This handler:
/// 1. Looks up the [`SenderEntry`] by `sender_stream_id`.
/// 2. Drops the `rx_closer` to poison the sender channel.
/// 3. Cancels the user-facing `cancel_token`.
///
/// Idempotent: if the entry is absent the handler returns `Ok(())` silently.
pub fn create_stream_cancel_handler(
    sender_registry: Arc<SenderRegistry>,
) -> crate::messenger::Handler {
    crate::messenger::Handler::am_handler(
        "_stream_cancel",
        move |ctx: crate::messenger::Context| {
            let req = serde_json::from_slice::<StreamCancelRequest>(&ctx.payload)?;
            if let Some((_, entry)) = sender_registry.senders.remove(&req.sender_stream_id) {
                // Poison the tx channel: drop the receiver end so
                // poison_tx.is_disconnected() is true in StreamSender::send()
                drop(entry.rx_closer.lock().unwrap().take());
                // Signal the token so user code can react proactively
                entry.cancel_token.cancel();
            }
            Ok(())
        },
    )
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
    /// `messenger-mux-v1` when it appears in both. `#[serde(default)]` means a
    /// sender that predates negotiation deserializes as one advertising
    /// nothing, which is exactly right: an empty list can never intersect, so
    /// absent a pre-bound slot on the anchor, such a sender is always answered
    /// with the receiver's default transport key — the behaviour it already
    /// expects. When the anchor holds an unclaimed pre-bind whose key this
    /// sender does not advertise, `adopt_prebind` refuses the attach instead
    /// of falling through to that default.
    #[serde(default)]
    pub supported_transport_keys: Vec<velo_ext::TransportKey>,
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
    /// The consumer's reader pump will tolerate `DETECTION_MULTIPLIER * heartbeat_interval_ms`
    /// of total silence before injecting `Dropped`. The field is carried as `u64` ms
    /// (rather than `Duration`) for stable msgpack encoding, and defaults to 5000ms
    /// when absent.
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
        /// default*. The asymmetry is deliberate and `BATCHING.md` is its
        /// authority — a credit window cannot be defaulted safely because only
        /// the receiver knows what it allocated, whereas the byte cap is a
        /// memory bound both sides can agree on without being told. The mux's
        /// internal `NegotiatedLimits::from_wire` is the one place that split
        /// is encoded.
        #[serde(default)]
        slot_byte_budget: u32,
    },
    /// Attach failed; `reason` describes why.
    Err { reason: String },
}

mod pump;
mod ticket;
pub(crate) use pump::{PumpContext, reader_pump};
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
    crate::messenger::Handler::typed_unary_async(
        "_anchor_attach",
        move |ctx: crate::messenger::TypedContext<AnchorAttachRequest>| {
            let manager = manager.clone();
            async move {
                let started = Instant::now();
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
                // spawn the pump. A sender that speaks the attach protocol
                // anyway — an older worker, or one whose envelope carried no
                // ticket — must be given *that* slot rather than a second one:
                // binding again would leave the first bind unclaimed with a
                // pump nobody feeds, and the sender would open against a
                // routing session the pre-bind never expects an OpenSlot for.
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
                // the sender advertised: `messenger-mux-v1` when both sides
                // named it, and otherwise exactly the local default this
                // handler answered with before negotiation existed.
                let selection = manager.select_streaming_transport(&req.supported_transport_keys);
                let receiver = match selection.transport.bind(local_id, routing_session_id).await {
                    Ok(rx) => rx,
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
                let streaming_transport_key = selection.key;

                // Step 3: Atomically set attachment under shard lock
                use dashmap::mapref::entry::Entry;
                match manager.registry.entry(local_id) {
                    Entry::Vacant(_) => {
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
                            // it would leave two pumps feeding one `frame_tx`
                            // and two live routing sessions, with
                            // `active_pump_token` naming only the newer, so
                            // nothing could ever cancel the older. The bind just
                            // made is left to the accept window, as it is on the
                            // two arms above.
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
                            // Derive a child token for this pump so detach can cancel it
                            // without poisoning the parent (which lives for the anchor's lifetime).
                            let pump_cancel = entry.cancel_token.child_token();
                            entry.active_pump_token = Some(pump_cancel.clone());
                            let pump_frame_tx = entry.frame_tx.clone();
                            // Snapshot the negotiated heartbeat interval before dropping the lock.
                            let heartbeat_interval = entry.heartbeat_interval;

                            // Mark as attached and store cancel handle for upstream cancel routing
                            entry.attachment = true;
                            entry.stream_cancel_handle = Some(req.stream_cancel_handle);

                            // Drop shard lock before spawning
                            drop(occ);

                            // Spawn reader pump as background task
                            let (_, local_id) = req.handle.unpack();
                            // Only the mux parks a drain signal, and only for
                            // the pair it just bound. Every other transport
                            // returns `None` and the pump's per-frame cost is
                            // one `Option` check.
                            let drain = manager.take_mux_drain_signal(local_id, routing_session_id);
                            tokio::spawn(reader_pump(
                                receiver,      // transport receiver from bind
                                pump_frame_tx, // cloned from entry
                                pump_cancel,   // cloned from entry
                                manager.anchor_context(),
                                PumpContext {
                                    local_id,
                                    heartbeat_deadline: heartbeat_interval,
                                    drain,
                                    // This is the ordinary attach path: a
                                    // sender is already on the wire, not a
                                    // zero-RTT pre-bind waiting for one. No
                                    // `PreBind` exists to share a flag with,
                                    // so this one starts and stays `false`.
                                    prebound: std::sync::Arc::new(
                                        std::sync::atomic::AtomicBool::new(false),
                                    ),
                                },
                            ));

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
                                initial_credit: selection.initial_credit,
                                slot_byte_budget: selection.slot_byte_budget,
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
/// and cancels the active pump's `CancellationToken`, all via one
/// `DashMap::entry()` -- the cancel has to happen before the shard lock drops
/// and before the released `PreBind` is dropped, so the pump this handler is
/// retiring is already told before anything closes the channel it reads from.
/// Only after the lock drops does it inject a
/// [`crate::streaming::frame::StreamFrame::Detached`] sentinel into the frame
/// channel. The anchor remains in the registry so a new sender may re-attach.
///
/// Idempotent: if the anchor is not found, returns `Ok(())`.
pub fn create_anchor_detach_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_detach",
        move |ctx: crate::messenger::TypedContext<AnchorDetachRequest>| {
            let manager = manager.clone();
            async move {
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
                        if released.is_some()
                            && let Some(duration) = entry.unattached_timeout
                        {
                            let tc = AnchorManager::spawn_timeout_task(
                                Arc::clone(&manager.registry),
                                Arc::clone(&manager.mpsc_registry),
                                manager.metrics.clone(),
                                local_id,
                                duration,
                                &entry.cancel_token,
                            );
                            entry.timeout_cancel = Some(tc);
                        }
                        // Take the child token (leaves None) so the next attach creates a fresh one,
                        // and cancel it here, before the shard lock drops and before the
                        // `released_prebind` below is dropped. Cancelling first is load-bearing for
                        // a released pre-bind: `PreBind::drop` closes the bind's `frame_tx` (the
                        // other end of this pump's `transport_rx`) when unclaimed, and the pump's
                        // own `Ok(Err(_))` arm treats an unclaimed, *uncancelled* transport close as
                        // "the accept window reclaimed an abandoned pre-bind" and removes the
                        // registry entry -- which this handler has just re-armed for reattachment,
                        // not abandoned. Cancelling first is what tells that arm this pump is being
                        // retired on purpose, exactly as `adopt_prebind`'s `Verdict::Mismatch` arm
                        // and the co-located branch of `attach_stream_anchor` already do.
                        let pump_token = entry.active_pump_token.take();
                        if let Some(ref token) = pump_token {
                            token.cancel();
                        }
                        (Some((pump_token, entry.frame_tx.clone())), released)
                    }
                };
                // shard lock is now dropped
                drop(released_prebind);

                if let Some((_pump_token, frame_tx)) = maybe_entry_info {
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

/// Build the `_anchor_finalize` handler.
///
/// Atomically removes the anchor from the registry via `remove_anchor()`, injects a
/// [`crate::streaming::frame::StreamFrame::Finalized`] sentinel, and cancels the `CancellationToken`.
///
/// Idempotent: if the anchor is already absent, returns `Ok(())`.
pub fn create_anchor_finalize_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_anchor_finalize",
        move |ctx: crate::messenger::TypedContext<AnchorFinalizeRequest>| {
            let manager = manager.clone();
            async move {
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
    crate::messenger::Handler::typed_unary_async(
        "_anchor_cancel",
        move |ctx: crate::messenger::TypedContext<AnchorCancelRequest>| {
            let manager = manager.clone();
            async move {
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
