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
use std::time::Instant;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::observability::{HandlerOutcome, StreamingOp};
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
    /// Streaming transports this sender can drive; see
    /// [`crate::streaming::control::AnchorAttachRequest::supported_transport_keys`].
    /// MPSC negotiates in the same version as SPSC so there is no
    /// half-migrated state where one anchor kind rides the mux and the other
    /// does not.
    #[serde(default)]
    pub supported_transport_keys: Vec<velo_ext::TransportKey>,
}

/// Response from the MPSC attach handler.
#[derive(Debug, Serialize, Deserialize)]
pub enum MpscAnchorAttachResponse {
    Ok {
        streaming_transport_key: velo_ext::TransportKey,
        heartbeat_interval_ms: u64,
        /// Newly allocated sender-id within the anchor's MPSC set.
        sender_id: u64,
        /// Receiver-allocated routing slot id; see
        /// [`crate::streaming::control::AnchorAttachResponse::Ok`] for
        /// rationale. `#[serde(default)]` for backwards compatibility with
        /// senders that haven't been updated.
        #[serde(default)]
        routing_session_id: u64,
        /// Mux credit window; zero means *not offering the mux*. See
        /// [`crate::streaming::control::AnchorAttachResponse::Ok`] — the two
        /// zeros mean different things and that variant documents which.
        #[serde(default)]
        initial_credit: u32,
        /// Mux per-slot byte cap; zero means *use the default*.
        #[serde(default)]
        slot_byte_budget: u32,
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
///
/// Heartbeat loss is watched with one timer per sender: armed once before the
/// loop, pushed forward from the receive arm only once it is inside half a
/// window, and otherwise re-armed from its own fire. Same shape and same
/// reason as [`crate::streaming::control::reader_pump`], whose doc carries
/// both arguments -- that a sender still sending never fires this timer, and
/// that detection still lands `DETECTION_MULTIPLIER * heartbeat_deadline`
/// after the last frame. MPSC has no pre-bind, so there is no
/// `awaiting_sender` exemption here.
pub(crate) async fn mpsc_reader_pump(
    sender_id: u64,
    transport_rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<(u64, Vec<u8>)>,
    cancel_token: CancellationToken,
    mpsc_registry: Arc<DashMap<u64, MpscAnchorEntry>>,
    pump: crate::streaming::control::PumpContext,
) {
    let crate::streaming::control::PumpContext {
        local_id,
        heartbeat_deadline,
        drain,
        // MPSC has no zero-RTT pre-bind path (`AnchorManager::prebind_anchor`
        // refuses `is_mpsc_stream()`), so every spawn here is an ordinary
        // attach and there is nothing to gate on.
        prebound: _,
    } = pump;
    let mut missed_heartbeats: u8 = 0;
    // One timer per sender, not one per record: see
    // `crate::streaming::control::reader_pump` for what rebuilding it per
    // record cost on the driver's lock. Kept in the same shape as that pump
    // and as `messenger::server::lanes` so a reader recognises all three.
    let mut last_frame = tokio::time::Instant::now();
    // Hoisted out of the per-record path; see the SPSC pump for the rule.
    let rearm_threshold = heartbeat_deadline / 2;
    let mut armed_until = last_frame + heartbeat_deadline;
    let sleep = tokio::time::sleep_until(armed_until);
    tokio::pin!(sleep);
    crate::streaming::control::note_timer_arm();
    let cancelled = cancel_token.cancelled();
    tokio::pin!(cancelled);

    loop {
        tokio::select! {
            // `tokio::time::timeout`, which this replaced, always polled the
            // receive first and only checked its own deadline if that was
            // Pending -- a ready receive could never lose. `biased` restores
            // that exact priority; see `crate::streaming::control::reader_pump`
            // for why an unbiased select over a deadline that lands on the
            // sender's own cadence is a real, not theoretical, race.
            biased;
            _ = &mut cancelled => break,
            received = transport_rx.recv_async() => {
                match received {
                    Ok(bytes) => {
                        let explicit_terminal = bytes == *crate::streaming::sender::cached_detached()
                            || bytes == *crate::streaming::sender::cached_dropped()
                            || bytes == *crate::streaming::sender::cached_finalized();
                        if frame_tx.send_async((sender_id, bytes)).await.is_err() {
                            break;
                        }
                        // Any frame proves liveness -- but only once it is
                        // actually forwarded. `mpsc_reader_pump` has no
                        // `try_send` fast path, so every frame blocks here
                        // for as long as the consumer takes to free a slot;
                        // that is the pump doing real work, not the sender
                        // going silent. Stamping on arrival instead of here
                        // would charge that block against the sender's
                        // heartbeat budget -- see the SPSC counterpart of
                        // this stamp in `crate::streaming::control::reader_pump`
                        // for the full argument.
                        missed_heartbeats = 0;
                        last_frame = tokio::time::Instant::now();
                        // Push the deadline out only once it is inside half a
                        // window, so a sender that keeps sending never fires
                        // this timer and moves it at most twice per deadline;
                        // same rule and same reason as the SPSC pump.
                        if armed_until.saturating_duration_since(last_frame)
                            < rearm_threshold
                        {
                            armed_until = last_frame + heartbeat_deadline;
                            sleep.as_mut().reset(armed_until);
                            crate::streaming::control::note_timer_arm();
                        }
                        // MPSC negotiates the mux in the same version as SPSC
                        // (see `MpscAnchorAttachRequest::supported_transport_keys`),
                        // so an MPSC stream over the mux needs its credit
                        // returned by draining for exactly the same reason.
                        if let Some(drain) = drain.as_deref() {
                            drain.drained();
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
                    Err(_) => {
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
            _ = &mut sleep => {
                crate::streaming::control::note_timer_fire();
                // Every path out of this arm re-arms the sleep first: a fired
                // `Sleep` stays ready until it is reset, so a `continue` past
                // one would spin this task instead of waiting.
                let idle = last_frame.elapsed();
                if idle < heartbeat_deadline {
                    armed_until = last_frame + heartbeat_deadline;
                    sleep.as_mut().reset(armed_until);
                    crate::streaming::control::note_timer_arm();
                    continue;
                }
                armed_until = tokio::time::Instant::now() + heartbeat_deadline;
                sleep.as_mut().reset(armed_until);
                crate::streaming::control::note_timer_arm();
                missed_heartbeats += 1;
                if missed_heartbeats >= DETECTION_MULTIPLIER {
                    let dropped = crate::streaming::sender::cached_dropped().clone();
                    let _ = frame_tx.send_async((sender_id, dropped)).await;
                    super::anchor::remove_sender_slot(&mpsc_registry, local_id, sender_id);
                    break;
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
                let started = Instant::now();
                let req = ctx.input;

                // Defence-in-depth: reject SPSC handles at the MPSC attach
                // endpoint. Mirrors the symmetric check in
                // `create_anchor_attach_handler`.
                if req.handle.is_spsc_stream() {
                    manager.record_streaming_operation(
                        StreamingOp::Attach,
                        HandlerOutcome::Error,
                        "unknown",
                        started,
                    );
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
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            return Ok(MpscAnchorAttachResponse::Err {
                                reason: format!("mpsc anchor {} not found", req.handle),
                            });
                        }
                        Some(e) => {
                            if let Some(limit) = e.max_senders
                                && e.senders.len() >= limit
                            {
                                manager.record_streaming_operation(
                                    StreamingOp::Attach,
                                    HandlerOutcome::Error,
                                    "unknown",
                                    started,
                                );
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
                //
                // Allocate a receiver-side routing_session_id rather than
                // reusing req.session_id (sender's local stream counter):
                // two MPSC senders from different workers both start at 1
                // and would otherwise collide on the same `(local_id,
                // session_id)` transport routing slot. See
                // [`crate::streaming::AnchorManager::next_routing_session_id`].
                let routing_session_id = manager
                    .next_routing_session_id
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                // Same intersection the SPSC handler makes; MPSC negotiates in
                // the same version so there is no half-migrated state.
                let selection = manager.select_streaming_transport(&req.supported_transport_keys);
                let transport_rx =
                    match selection.transport.bind(local_id, routing_session_id).await {
                        Ok(rx) => rx,
                        Err(e) => {
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
                            return Ok(MpscAnchorAttachResponse::Err {
                                reason: format!("transport error: {}", e),
                            });
                        }
                    };
                let streaming_transport_key = selection.key;

                // Step 3: atomic slot insertion.
                use dashmap::mapref::entry::Entry;
                let (frame_tx, pump_cancel, sender_id) = match manager.mpsc_registry.entry(local_id)
                {
                    Entry::Vacant(_) => {
                        manager.record_streaming_operation(
                            StreamingOp::Attach,
                            HandlerOutcome::Error,
                            "unknown",
                            started,
                        );
                        return Ok(MpscAnchorAttachResponse::Err {
                            reason: format!("mpsc anchor {} removed during bind", req.handle),
                        });
                    }
                    Entry::Occupied(mut occ) => {
                        let entry = occ.get_mut();
                        if let Some(limit) = entry.max_senders
                            && entry.senders.len() >= limit
                        {
                            manager.record_streaming_operation(
                                StreamingOp::Attach,
                                HandlerOutcome::Error,
                                "unknown",
                                started,
                            );
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
                let drain = manager.take_mux_drain_signal(local_id, routing_session_id);
                tokio::spawn(mpsc_reader_pump(
                    sender_id,
                    transport_rx,
                    frame_tx,
                    pump_cancel,
                    pump_registry,
                    crate::streaming::control::PumpContext {
                        local_id,
                        heartbeat_deadline: heartbeat_interval,
                        drain,
                        // Always an ordinary attach; see the destructure in
                        // `mpsc_reader_pump`.
                        prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    },
                ));

                manager.record_streaming_operation(
                    StreamingOp::Attach,
                    HandlerOutcome::Success,
                    streaming_transport_key.as_str(),
                    started,
                );

                Ok(MpscAnchorAttachResponse::Ok {
                    streaming_transport_key,
                    heartbeat_interval_ms: heartbeat_interval.as_millis() as u64,
                    sender_id,
                    routing_session_id,
                    initial_credit: selection.initial_credit,
                    slot_byte_budget: selection.slot_byte_budget,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::control::{PumpContext, TIMER_ARMS, TIMER_FIRES};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;
    use velo_ext::{TransportKey, WorkerId};

    /// The MPSC half of the property `streaming::control`'s
    /// `a_thousand_records_arm_the_heartbeat_timer_a_handful_of_times` pins:
    /// this pump watches one sender, and it must arm one timer for that
    /// sender rather than one per record it carries. The hour-long deadline
    /// keeps the timer from ever firing here;
    /// `an_mpsc_sender_under_traffic_never_fires_its_heartbeat_timer` is what
    /// pins the firing case.
    #[tokio::test]
    async fn a_thousand_records_arm_the_mpsc_heartbeat_timer_a_handful_of_times() {
        tokio::time::pause();

        const RECORDS: usize = 1_000;
        // One arm before the loop, plus headroom for a paused-clock
        // auto-advance if the runtime goes idle between records. The bound
        // does not scale with `RECORDS`; that is the whole assertion.
        const MAX_ARMS: u64 = 4;

        let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(256);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(256);
        let arms = Arc::new(AtomicU64::new(0));

        tokio::spawn(TIMER_ARMS.scope(
            Arc::clone(&arms),
            mpsc_reader_pump(
                7,
                transport_rx,
                frame_tx,
                CancellationToken::new(),
                Arc::new(DashMap::new()),
                PumpContext {
                    local_id: 1,
                    heartbeat_deadline: Duration::from_secs(3600),
                    drain: None,
                    prebound: Arc::new(AtomicBool::new(false)),
                },
            ),
        ));

        let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
        for i in 0..RECORDS {
            transport_tx
                .send_async(record.clone())
                .await
                .expect("the pump must still be reading");
            // Drained in step with the send: a backlog would park both this
            // task and the pump, and an idle runtime under
            // `tokio::time::pause` auto-advances to the sleep's deadline.
            let (sender_id, forwarded) = frame_rx
                .recv_async()
                .await
                .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
            assert_eq!(sender_id, 7, "record {i} must carry its sender's id");
            assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
        }

        let armed = arms.load(Ordering::Relaxed);
        // Without this, deleting every `note_timer_arm()` call site leaves
        // `armed` at 0 and the bound below still passes -- an upper bound
        // alone does not prove the seam is wired to anything. The pre-loop
        // arm is unconditional and this point is reached only after a record
        // has been forwarded, so `>= 1` is exact and cannot flake.
        assert!(
            armed >= 1,
            "the pre-loop arm must have counted at least once"
        );
        assert!(
            armed <= MAX_ARMS,
            "mpsc_reader_pump must arm its heartbeat timer a bounded number of times, \
             not once per record: {RECORDS} records armed it {armed} times (bound {MAX_ARMS})"
        );

        drop(transport_tx);
    }

    /// The MPSC half of `streaming::control`'s
    /// `a_stream_under_traffic_never_fires_its_heartbeat_timer`: a sender that
    /// keeps sending must not let its pump's heartbeat timer fire at all.
    /// That test carries the derivation of both bounds.
    #[tokio::test]
    async fn an_mpsc_sender_under_traffic_never_fires_its_heartbeat_timer() {
        tokio::time::pause();

        const DEADLINE_MS: u64 = 50;
        const STEP_MS: u64 = 5;
        const TRAFFIC_MS: u64 = 500;
        const RECORDS: usize = (TRAFFIC_MS / STEP_MS) as usize + 1;
        const MAX_ARMS: u64 = 1 + TRAFFIC_MS / (DEADLINE_MS / 2) + 1;

        let deadline = Duration::from_millis(DEADLINE_MS);
        let step = Duration::from_millis(STEP_MS);

        let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(4);
        let arms = Arc::new(AtomicU64::new(0));
        let fires = Arc::new(AtomicU64::new(0));

        tokio::spawn(TIMER_FIRES.scope(
            Arc::clone(&fires),
            TIMER_ARMS.scope(
                Arc::clone(&arms),
                mpsc_reader_pump(
                    7,
                    transport_rx,
                    frame_tx,
                    CancellationToken::new(),
                    Arc::new(DashMap::new()),
                    PumpContext {
                        local_id: 1,
                        heartbeat_deadline: deadline,
                        drain: None,
                        prebound: Arc::new(AtomicBool::new(false)),
                    },
                ),
            ),
        ));

        let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
        for i in 0..RECORDS {
            // Advance between records, never after the last one, so the clock
            // still reads the last frame's instant when the loop ends.
            if i > 0 {
                tokio::time::advance(step).await;
            }
            transport_tx
                .send_async(record.clone())
                .await
                .unwrap_or_else(|_| panic!("the pump must still be reading at record {i}"));
            let (sender_id, forwarded) = frame_rx
                .recv_async()
                .await
                .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
            assert_eq!(sender_id, 7, "record {i} must carry its sender's id");
            assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
        }

        let fired = fires.load(Ordering::Relaxed);
        assert_eq!(
            fired, 0,
            "mpsc_reader_pump must not fire its timer under traffic: {RECORDS} records \
             {STEP_MS} ms apart under a {DEADLINE_MS} ms deadline fired it {fired} times"
        );

        let armed = arms.load(Ordering::Relaxed);
        // Without this, deleting every `note_timer_arm()` call site leaves
        // `armed` at 0 and the bound below still passes.
        assert!(
            armed >= 1,
            "the pre-loop arm must have counted at least once"
        );
        assert!(
            armed <= MAX_ARMS,
            "mpsc_reader_pump must re-arm its heartbeat timer at a bounded rate, not \
             once per record: {RECORDS} records armed it {armed} times (bound {MAX_ARMS})"
        );

        drop(transport_tx);
    }

    /// The MPSC half of `streaming::control`'s
    /// `a_stream_that_stops_is_caught_a_detection_window_after_its_last_record`,
    /// which carries the schedule this asserts. It is also the anti-tautology
    /// control for this pump's own `note_timer_fire()` call site -- a separate
    /// site from the SPSC pump's, so the SPSC control says nothing about it,
    /// and without a `fired >= DETECTION_MULTIPLIER` assertion here the
    /// zero-fires test above would pass on a seam wired to nothing.
    #[tokio::test]
    async fn an_mpsc_sender_that_stops_is_dropped_a_detection_window_after_its_last_record() {
        tokio::time::pause();

        const DEADLINE_MS: u64 = 50;
        const STEP_MS: u64 = 5;
        const TRAFFIC_MS: u64 = 500;
        const RECORDS: usize = (TRAFFIC_MS / STEP_MS) as usize + 1;

        let deadline = Duration::from_millis(DEADLINE_MS);
        let step = Duration::from_millis(STEP_MS);

        let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(4);
        let fires = Arc::new(AtomicU64::new(0));

        let pump = tokio::spawn(TIMER_FIRES.scope(
            Arc::clone(&fires),
            mpsc_reader_pump(
                7,
                transport_rx,
                frame_tx,
                CancellationToken::new(),
                Arc::new(DashMap::new()),
                PumpContext {
                    local_id: 1,
                    heartbeat_deadline: deadline,
                    drain: None,
                    prebound: Arc::new(AtomicBool::new(false)),
                },
            ),
        ));

        let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
        for i in 0..RECORDS {
            if i > 0 {
                tokio::time::advance(step).await;
            }
            transport_tx
                .send_async(record.clone())
                .await
                .unwrap_or_else(|_| panic!("the pump must still be reading at record {i}"));
            let (_, forwarded) = frame_rx
                .recv_async()
                .await
                .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
            assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
        }

        // The pump stamps `last_frame` right after the forward this loop just
        // drained, and no advance separates the two, so the paused clock reads
        // that same instant here.
        let last_frame = tokio::time::Instant::now();
        let fires_under_traffic = fires.load(Ordering::Relaxed);

        // Bounded rather than a bare await: a watchdog that never fired would
        // park this test on a paused clock with nothing left to advance to,
        // hanging the runner instead of going red.
        tokio::time::timeout(deadline * 20, pump)
            .await
            .expect("the watchdog must drop a sender that stopped")
            .expect("the pump task must not panic");

        let caught_after = last_frame.elapsed();
        let window = deadline * u32::from(DETECTION_MULTIPLIER);
        assert!(
            caught_after >= window && caught_after < window + deadline,
            "a sender that stops must be dropped {DETECTION_MULTIPLIER} deadlines after \
             its last record: dropped after {caught_after:?}, want {window:?} within one \
             {deadline:?} deadline"
        );

        let fired = fires.load(Ordering::Relaxed) - fires_under_traffic;
        let misses = u64::from(DETECTION_MULTIPLIER);
        assert!(
            fired == misses || fired == misses + 1,
            "a sender that stops must fire its pump's timer once per window of silence \
             (plus at most one no-miss re-arm): fired {fired} times, want {misses} or {}",
            misses + 1
        );

        let (sender_id, bytes) = frame_rx
            .try_recv()
            .expect("the watchdog must inject a Dropped sentinel for the silent sender");
        assert_eq!(sender_id, 7, "the sentinel must name the sender that died");
        assert_eq!(
            bytes,
            *crate::streaming::sender::cached_dropped(),
            "the sentinel must be Dropped"
        );

        drop(transport_tx);
    }

    /// Hoisting the timer must not cost the watchdog: a sender that goes
    /// silent is still dropped after `DETECTION_MULTIPLIER` windows, and the
    /// sentinel still carries that sender's id so the anchor drops one sender
    /// rather than the whole stream.
    #[tokio::test]
    async fn a_silent_mpsc_sender_is_dropped_after_the_detection_window() {
        tokio::time::pause();

        // Held so the transport channel stays open: this must be the
        // watchdog's doing, not the closed-channel arm's.
        let (_transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(16);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(16);
        let heartbeat = Duration::from_millis(50);

        let pump = tokio::spawn(mpsc_reader_pump(
            7,
            transport_rx,
            frame_tx,
            CancellationToken::new(),
            Arc::new(DashMap::new()),
            PumpContext {
                local_id: 1,
                heartbeat_deadline: heartbeat,
                drain: None,
                prebound: Arc::new(AtomicBool::new(false)),
            },
        ));

        // Bounded even under a paused clock: if the hoisted timer were ever
        // silently disarmed (the seam this PR is built around), the pump
        // would park forever on a channel with no other pending timer, and a
        // paused clock has nothing left to auto-advance to -- the test would
        // hang the runner instead of failing red. This timeout is that
        // remaining timer: it gives the paused clock something to advance to
        // regardless of the pump's own state, so a dead watchdog fails on a
        // named assertion here instead.
        tokio::time::timeout(heartbeat * (DETECTION_MULTIPLIER as u32 + 2), pump)
            .await
            .expect("the pump must exit once the detection window passes")
            .expect("the pump task must not panic");

        let (sender_id, bytes) = frame_rx
            .try_recv()
            .expect("a silent sender must be dropped");
        assert_eq!(
            sender_id, 7,
            "the Dropped sentinel must name the sender that went silent"
        );
        assert_eq!(
            bytes,
            *crate::streaming::sender::cached_dropped(),
            "the sentinel must be Dropped"
        );
    }

    /// The other half: a sender that keeps sending is never dropped, however
    /// many times its timer fires in between. This is what the compare
    /// against the last frame's instant buys -- the timer is no longer pushed
    /// forward by each record, so a fire that lands mid-stream must count
    /// nothing rather than a miss.
    #[tokio::test]
    async fn an_mpsc_sender_that_keeps_sending_is_never_dropped() {
        tokio::time::pause();

        let heartbeat = Duration::from_millis(50);
        let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(16);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(16);

        let pump = tokio::spawn(mpsc_reader_pump(
            7,
            transport_rx,
            frame_tx,
            CancellationToken::new(),
            Arc::new(DashMap::new()),
            PumpContext {
                local_id: 1,
                heartbeat_deadline: heartbeat,
                drain: None,
                prebound: Arc::new(AtomicBool::new(false)),
            },
        ));

        let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(1u32)).unwrap();
        // Ten detection windows of streaming, one record every half window.
        for i in 0..(10 * DETECTION_MULTIPLIER as u32) {
            transport_tx
                .send_async(record.clone())
                .await
                .expect("the pump must still be reading");
            let (_, forwarded) = frame_rx
                .recv_async()
                .await
                .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
            assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
            tokio::time::sleep(heartbeat / 2).await;
        }

        assert!(
            !pump.is_finished(),
            "a sender streaming inside its heartbeat deadline must never be dropped"
        );
        drop(transport_tx);
    }

    /// A forward blocked on a saturated `frame_tx` is the pump doing real
    /// work, not the sender going silent, so it must not count against the
    /// heartbeat budget -- see the SPSC counterpart of this test on
    /// `crate::streaming::control::reader_pump` for the full argument.
    /// `mpsc_reader_pump` has no `try_send` fast path at all, so this is the
    /// ordinary saturated case for it, not an edge one.
    #[tokio::test]
    async fn mpsc_reader_pump_does_not_count_a_blocked_forward_as_heartbeat_silence() {
        tokio::time::pause();

        let heartbeat = Duration::from_millis(50);
        let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
        let (frame_tx, frame_rx) = flume::bounded::<(u64, Vec<u8>)>(1);
        // Fill the one slot so the send always blocks.
        frame_tx.try_send((0, b"placeholder".to_vec())).unwrap();

        let pump = tokio::spawn(mpsc_reader_pump(
            7,
            transport_rx,
            frame_tx,
            CancellationToken::new(),
            Arc::new(DashMap::new()),
            PumpContext {
                local_id: 1,
                heartbeat_deadline: heartbeat,
                drain: None,
                prebound: Arc::new(AtomicBool::new(false)),
            },
        ));

        let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(1u32)).unwrap();
        transport_tx.send_async(record.clone()).await.unwrap();

        // Let the forward stay blocked for slightly over one deadline before
        // anything drains it -- time spent moving the frame, not silence.
        tokio::time::sleep(heartbeat + Duration::from_millis(10)).await;

        let (placeholder_id, placeholder) = frame_rx.recv_async().await.unwrap();
        assert_eq!(placeholder_id, 0);
        assert_eq!(placeholder, b"placeholder".to_vec());
        let (sender_id, forwarded) = frame_rx.recv_async().await.unwrap();
        assert_eq!(sender_id, 7);
        assert_eq!(forwarded, record, "the blocked record must still land");

        // No further frames. Two more full windows of genuine silence --
        // three in total from the moment the forward actually finished --
        // must still be needed before the sender is dropped. A half-window
        // margin keeps this proportional to `heartbeat`: the buggy stamp
        // drops the sender at 2 windows past the moment the forward landed,
        // the correct one at 3, and this sleep lands squarely between them.
        tokio::time::sleep(heartbeat * 2 + heartbeat / 2).await;

        assert!(
            !pump.is_finished(),
            "mpsc_reader_pump dropped the sender one window early: a forward \
             blocked by backpressure was counted as heartbeat silence because \
             `last_frame` was stamped when the frame arrived instead of when \
             the forward finished"
        );
    }

    /// MPSC negotiates in the same version as SPSC, so its request carries the
    /// same key list and its response the same credit fields. What this pins is
    /// that an older MPSC sender — which sends neither — is still understood.
    #[test]
    fn an_mpsc_attach_request_from_before_negotiation_advertises_nothing() {
        let legacy_json = r#"{
            "handle": {"hi": 1, "lo": 2},
            "session_id": 3,
            "stream_cancel_handle": {"hi": 4, "lo": 5}
        }"#;
        let decoded: MpscAnchorAttachRequest =
            serde_json::from_str(legacy_json).expect("legacy mpsc request must deserialize");
        assert!(decoded.supported_transport_keys.is_empty());
    }

    #[test]
    fn an_mpsc_attach_response_from_before_negotiation_offers_no_mux() {
        let legacy_json = r#"{"Ok":{
            "streaming_transport_key": "tcp-stream",
            "heartbeat_interval_ms": 5000,
            "sender_id": 2
        }}"#;
        let decoded: MpscAnchorAttachResponse =
            serde_json::from_str(legacy_json).expect("legacy mpsc response must deserialize");
        match decoded {
            MpscAnchorAttachResponse::Ok {
                initial_credit,
                slot_byte_budget,
                ..
            } => {
                assert_eq!(
                    initial_credit, 0,
                    "an absent credit window is a peer not offering the mux"
                );
                assert_eq!(
                    slot_byte_budget, 0,
                    "an absent byte cap means the default, not a refusal"
                );
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn an_mpsc_attach_exchange_round_trips_its_negotiation_fields() {
        let req = MpscAnchorAttachRequest {
            handle: StreamAnchorHandle::pack_mpsc(WorkerId::from_u64(1), 2),
            session_id: 3,
            stream_cancel_handle: StreamCancelHandle::pack(WorkerId::from_u64(4), 5),
            supported_transport_keys: vec![TransportKey::new("messenger-mux-v1")],
        };
        let decoded: MpscAnchorAttachRequest =
            rmp_serde::from_slice(&rmp_serde::to_vec(&req).expect("encode")).expect("decode");
        assert_eq!(
            decoded
                .supported_transport_keys
                .iter()
                .map(TransportKey::as_str)
                .collect::<Vec<_>>(),
            ["messenger-mux-v1"],
        );

        let resp = MpscAnchorAttachResponse::Ok {
            streaming_transport_key: TransportKey::new("messenger-mux-v1"),
            heartbeat_interval_ms: 5000,
            sender_id: 7,
            routing_session_id: 8,
            initial_credit: 256,
            slot_byte_budget: 1024,
        };
        let decoded: MpscAnchorAttachResponse =
            rmp_serde::from_slice(&rmp_serde::to_vec(&resp).expect("encode")).expect("decode");
        match decoded {
            MpscAnchorAttachResponse::Ok {
                initial_credit,
                slot_byte_budget,
                ..
            } => {
                assert_eq!(initial_credit, 256);
                assert_eq!(slot_byte_budget, 1024);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}
