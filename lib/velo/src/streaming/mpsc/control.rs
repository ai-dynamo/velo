// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Control-plane handlers and per-sender reader pump for the MPSC anchor
//! protocol.
//!
//! Three active-message handlers are defined here:
//! - [`create_mpsc_anchor_attach_handler`]: allocates a sender_id, binds the
//!   transport, and spawns a per-sender reader pump.
//! - [`create_mpsc_anchor_detach_handler`]: removes one sender from an
//!   entry; re-arms the unattached timeout if it was the last one.
//! - [`create_mpsc_anchor_cancel_handler`]: removes the whole anchor silently.
//!
//! `_stream_cancel` is **not** duplicated — the existing SPSC handler at
//! `control.rs:152` is keyed off `sender_stream_id` and works for MPSC
//! senders unchanged (they register in the same [`SenderRegistry`]).

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::streaming::anchor::AnchorManager;
use crate::streaming::control::{DETECTION_MULTIPLIER, StreamCancelHandle};
use crate::streaming::handle::StreamAnchorHandle;

use super::anchor::{MpscAnchorEntry, MpscSenderSlot};

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// Request to attach a new sender to an existing MPSC anchor.
///
/// Mirrors [`crate::streaming::control::AnchorAttachRequest`] but target semantics and
/// response shape differ.
#[derive(Debug, Serialize, Deserialize)]
pub struct MpscAnchorAttachRequest {
    pub handle: StreamAnchorHandle,
    pub session_id: u64,
    pub stream_cancel_handle: StreamCancelHandle,
}

/// Response from the MPSC attach handler.
#[derive(Debug, Serialize, Deserialize)]
pub enum MpscAnchorAttachResponse {
    Ok {
        stream_endpoint: String,
        heartbeat_interval_ms: u64,
        /// Newly allocated sender-id within the anchor's MPSC set.
        sender_id: u64,
    },
    Err {
        reason: String,
    },
}

/// Request to detach a specific sender from an MPSC anchor.
#[derive(Debug, Serialize, Deserialize)]
pub struct MpscAnchorDetachRequest {
    pub handle: StreamAnchorHandle,
    pub sender_id: u64,
}

/// Request to cancel an entire MPSC anchor from the sender side.
#[derive(Debug, Serialize, Deserialize)]
pub struct MpscAnchorCancelRequest {
    pub handle: StreamAnchorHandle,
}

// ---------------------------------------------------------------------------
// Per-sender reader pump
// ---------------------------------------------------------------------------

/// Per-sender reader pump for the MPSC anchor.
///
/// Reads raw bytes from a remote sender's transport receiver, tags each
/// frame with this sender's `sender_id`, and forwards to the anchor's
/// shared `(u64, Vec<u8>)` channel. On 3 missed heartbeats (or transport
/// close with no explicit terminal) it injects a `Dropped` sentinel for
/// **this sender only** — not for the whole anchor — and removes the sender
/// slot from the MPSC entry. Explicit in-band terminal sentinels
/// (`Detached`, `Dropped`, `Finalized`) are treated as authoritative and are
/// forwarded exactly once.
pub(crate) async fn mpsc_reader_pump(
    sender_id: u64,
    transport_rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<(u64, Vec<u8>)>,
    cancel_token: CancellationToken,
    mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    local_id: u64,
    heartbeat_deadline: Duration,
) {
    let mut missed_heartbeats: u8 = 0;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            result = tokio::time::timeout(heartbeat_deadline, transport_rx.recv_async()) => {
                match result {
                    Ok(Ok(bytes)) => {
                        missed_heartbeats = 0;
                        let explicit_terminal = bytes == *crate::streaming::sender::cached_detached()
                            || bytes == *crate::streaming::sender::cached_dropped()
                            || bytes == *crate::streaming::sender::cached_finalized();
                        if frame_tx.send_async((sender_id, bytes)).await.is_err() {
                            break;
                        }
                        if explicit_terminal {
                            if let Some(slot) =
                                super::anchor::remove_sender_slot(&mpsc_registry, local_id, sender_id)
                                && let Some(pt) = slot.pump_token
                            {
                                pt.cancel();
                            }
                            break;
                        }
                    }
                    Ok(Err(_)) => {
                        let dropped = crate::streaming::sender::cached_dropped().clone();
                        let _ = frame_tx.send_async((sender_id, dropped)).await;
                        super::anchor::remove_sender_slot(
                            &mpsc_registry,
                            local_id,
                            sender_id,
                        );
                        break;
                    }
                    Err(_timeout) => {
                        missed_heartbeats += 1;
                        if missed_heartbeats >= DETECTION_MULTIPLIER {
                            let dropped = crate::streaming::sender::cached_dropped().clone();
                            let _ = frame_tx.send_async((sender_id, dropped)).await;
                            super::anchor::remove_sender_slot(
                                &mpsc_registry,
                                local_id,
                                sender_id,
                            );
                            break;
                        }
                    }
                }
            }
        }
    }
    cancel_token.cancel();
}

// ---------------------------------------------------------------------------
// Handler constructors
// ---------------------------------------------------------------------------

/// Build the `_mpsc_anchor_attach` handler.
///
/// Uses the bind-then-lock pattern from
/// [`crate::streaming::control::create_anchor_attach_handler`]: quick existence check,
/// async `transport.bind().await` outside the shard lock, then atomic slot
/// insertion under the lock.
pub fn create_mpsc_anchor_attach_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_mpsc_anchor_attach",
        move |ctx: crate::messenger::TypedContext<MpscAnchorAttachRequest>| {
            let manager = manager.clone();
            async move {
                let req = ctx.input;

                // Defence-in-depth: reject SPSC handles at the MPSC attach
                // endpoint. Mirrors the symmetric check in
                // `create_anchor_attach_handler`.
                if req.handle.is_spsc_stream() {
                    return Ok(MpscAnchorAttachResponse::Err {
                        reason: format!("anchor {} is spsc; use _anchor_attach", req.handle),
                    });
                }

                let (_, local_id) = req.handle.unpack();

                // Step 1: quick existence / capacity check.
                let heartbeat_interval = {
                    let entry = manager.mpsc_registry.get(&local_id);
                    match entry {
                        None => {
                            return Ok(MpscAnchorAttachResponse::Err {
                                reason: format!("mpsc anchor {} not found", req.handle),
                            });
                        }
                        Some(e) => {
                            if let Some(limit) = e.max_senders
                                && e.senders.len() >= limit
                            {
                                return Ok(MpscAnchorAttachResponse::Err {
                                    reason: format!(
                                        "mpsc anchor {} reached max_senders limit {}",
                                        req.handle, limit
                                    ),
                                });
                            }
                            e.heartbeat_interval
                        }
                    }
                };

                // Step 2: async bind outside the shard lock.
                let (endpoint, transport_rx) =
                    match manager.transport.bind(local_id, req.session_id).await {
                        Ok(pair) => pair,
                        Err(e) => {
                            return Ok(MpscAnchorAttachResponse::Err {
                                reason: format!("transport error: {}", e),
                            });
                        }
                    };

                // Step 3: atomic slot insertion.
                use dashmap::mapref::entry::Entry;
                let (frame_tx, pump_cancel, sender_id) = match manager.mpsc_registry.entry(local_id)
                {
                    Entry::Vacant(_) => {
                        return Ok(MpscAnchorAttachResponse::Err {
                            reason: format!("mpsc anchor {} removed during bind", req.handle),
                        });
                    }
                    Entry::Occupied(mut occ) => {
                        let entry = occ.get_mut();
                        if let Some(limit) = entry.max_senders
                            && entry.senders.len() >= limit
                        {
                            return Ok(MpscAnchorAttachResponse::Err {
                                reason: format!(
                                    "mpsc anchor {} reached max_senders limit {}",
                                    req.handle, limit
                                ),
                            });
                        }

                        let sender_id = entry.next_sender_id;
                        entry.next_sender_id += 1;

                        let pump_cancel = entry.cancel_token.child_token();
                        let slot = MpscSenderSlot {
                            pump_token: Some(pump_cancel.clone()),
                            stream_cancel_handle: Some(req.stream_cancel_handle),
                        };
                        entry.senders.insert(sender_id, slot);

                        // Cancel unattached timeout now that we have a sender again.
                        if let Some(ref tc) = entry.timeout_cancel {
                            tc.cancel();
                        }
                        entry.timeout_cancel = None;

                        (entry.frame_tx.clone(), pump_cancel, sender_id)
                    }
                };

                // Spawn the per-sender pump outside the shard lock.
                let pump_registry = manager.mpsc_registry.clone();
                tokio::spawn(mpsc_reader_pump(
                    sender_id,
                    transport_rx,
                    frame_tx,
                    pump_cancel,
                    pump_registry,
                    local_id,
                    heartbeat_interval,
                ));

                Ok(MpscAnchorAttachResponse::Ok {
                    stream_endpoint: endpoint,
                    heartbeat_interval_ms: heartbeat_interval.as_millis() as u64,
                    sender_id,
                })
            }
        },
    )
    .spawn()
    .build()
}

/// Build the `_mpsc_anchor_detach` handler.
///
/// Removes one sender slot from the entry and cancels its pump. Anchor
/// remains in the registry; the consumer will eventually see a `Detached`
/// frame for this sender_id via the pump forwarding or via slot removal.
pub fn create_mpsc_anchor_detach_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_mpsc_anchor_detach",
        move |ctx: crate::messenger::TypedContext<MpscAnchorDetachRequest>| {
            let manager = manager.clone();
            async move {
                let req = ctx.input;
                let (_, local_id) = req.handle.unpack();

                // Remove the slot (re-arms the unattached timeout if this was
                // the last sender) and cancel its pump_token, if any.
                if let Some(slot) = super::anchor::remove_sender_slot(
                    &manager.mpsc_registry,
                    local_id,
                    req.sender_id,
                ) && let Some(pt) = slot.pump_token
                {
                    pt.cancel();
                }

                Ok(())
            }
        },
    )
    .spawn()
    .build()
}

/// Build the `_mpsc_anchor_cancel` handler — remove the whole anchor silently.
pub fn create_mpsc_anchor_cancel_handler(manager: Arc<AnchorManager>) -> crate::messenger::Handler {
    crate::messenger::Handler::typed_unary_async(
        "_mpsc_anchor_cancel",
        move |ctx: crate::messenger::TypedContext<MpscAnchorCancelRequest>| {
            let manager = manager.clone();
            async move {
                let req = ctx.input;
                let (_, local_id) = req.handle.unpack();

                if let Some((_, entry)) = manager.mpsc_registry.remove(&local_id) {
                    entry.cancel_token.cancel();
                    if let Some(ref tc) = entry.timeout_cancel {
                        tc.cancel();
                    }
                    super::anchor::cancel_all_senders(
                        &entry,
                        &manager.sender_registry,
                        manager.messenger_lock.get(),
                    );
                    manager.update_active_anchor_gauge();
                }

                Ok(())
            }
        },
    )
    .spawn()
    .build()
}
