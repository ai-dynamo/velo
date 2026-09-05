// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The reader pump: bridges transport frames to an anchor's delivery channel.
//!
//! Split out of `control.rs` (the file's own `// --- Reader pump ---` banner
//! marked the seam before the split): [`PumpContext`], [`bind_unclaimed`],
//! [`awaiting_sender`] and [`reader_pump`] are one self-contained unit with a
//! single external dependency, [`super::DETECTION_MULTIPLIER`]. Spawned from
//! two call sites in `anchor.rs` (an ordinary or adopted attach) and one in
//! `mpsc/control.rs` (`mpsc_reader_pump`, which reuses [`PumpContext`] rather
//! than duplicating it) — none of them live here, which is why this stays a
//! plain function rather than growing a home for its callers too.

use std::time::Duration;

use super::DETECTION_MULTIPLIER;

/// What a reader pump needs beyond its channels.
///
/// A struct rather than three more parameters: `mpsc_reader_pump` already
/// carries a sender id and a registry, and adding the drain hook positionally
/// would take it past the argument limit — which `CLAUDE.md` says to answer
/// with a config struct rather than an `allow`.
pub(crate) struct PumpContext {
    /// The anchor's local id, for registry removal on heartbeat loss.
    pub(crate) local_id: u64,
    /// The anchor's configured cadence, carried to the sender on the attach
    /// response or the ticket. `DETECTION_MULTIPLIER` misses is a dead
    /// stream -- except while [`awaiting_sender`] holds, which every window a
    /// pre-bind spends with no sender yet does by construction.
    pub(crate) heartbeat_deadline: Duration,
    /// Told when a record leaves the buffer credit is issued against. `None`
    /// for every transport that does not do flow control over this seam.
    pub(crate) drain: Option<std::sync::Arc<crate::streaming::messenger_mux::ingress::DrainSignal>>,
    /// Whether this pump's slot still has no sender, other than through its
    /// own `OpenSlot`.
    ///
    /// `true` only at the one genuine pre-bind spawn site
    /// (`AnchorManager::prebind_anchor`); every ordinary attach spawn passes
    /// a fresh `Arc::new(AtomicBool::new(false))`. Shared with the
    /// `PreBind` the pump was spawned for, and cleared by
    /// [`PreBind::adopt`](crate::streaming::anchor::PreBind::adopt) the
    /// moment a sender attaches the long way round instead of opening on its
    /// ticket -- the one other door through which a sender can show up, and
    /// the one transition an `Arc<AtomicBool>` exists to carry immediately
    /// rather than the pump learning it only once that sender's own
    /// `OpenSlot` lands.
    ///
    /// `drain.claimed().is_none()` is not a proxy for this on its own: the
    /// mux parks a `DrainSignal` for *every* bind, including an ordinary
    /// attach's, and that signal stays unclaimed until the peer's `OpenSlot`
    /// arrives -- which is necessarily after the attach response already
    /// returned. See [`awaiting_sender`] for the combined read the
    /// heartbeat-exemption branch below needs; the transport-closed branch
    /// needs only the claim, see [`bind_unclaimed`].
    pub(crate) prebound: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Whether an `OpenSlot` has claimed the mux bind this pump reads from.
///
/// `None` (a transport with no `DrainSignal`) answers `false`: without a
/// drain there is no accept window racing to reclaim the bind either, so a
/// closed transport there has always meant a sender that genuinely departed,
/// not one still on its way in. This is the condition the reap arm below
/// gates on -- it does not care which of the two doors in [`awaiting_sender`]
/// a sender was expected to come through, only whether one has.
fn bind_unclaimed(drain: Option<&crate::streaming::messenger_mux::ingress::DrainSignal>) -> bool {
    drain.is_some_and(|d| d.claimed().is_none())
}

/// Whether a pump's slot still has no sender -- the state that must not count
/// against the heartbeat watchdog.
///
/// Two different doors let a sender show up, and this has to watch both:
/// `prebound` answers for the one that opens on its ticket directly (an
/// `OpenSlot` claims `drain`, no attach in between) by being cleared the
/// instant [`PreBind::adopt`](crate::streaming::anchor::PreBind::adopt)
/// reassigns the slot to a sender that attached the ordinary way instead --
/// `drain` alone cannot see that sender, since its own `OpenSlot` may still
/// be seconds or minutes away. `prebound` alone is not enough either: an
/// ordinary attach's pump starts with an unclaimed `drain` too, for as long
/// as the peer's own `OpenSlot` is still in flight, and that pump is spawned
/// with `prebound` already `false` for exactly that reason.
fn awaiting_sender(
    prebound: &std::sync::atomic::AtomicBool,
    drain: Option<&crate::streaming::messenger_mux::ingress::DrainSignal>,
) -> bool {
    prebound.load(std::sync::atomic::Ordering::Relaxed) && bind_unclaimed(drain)
}

/// Reader pump: bridges transport frames to the anchor's delivery channel.
///
/// Spawned from one of two sites: the attach handler, after a sender has
/// already asked for the stream, or
/// [`AnchorManager::prebind_anchor`](crate::streaming::anchor::AnchorManager::prebind_anchor),
/// before any sender has. Reads from the transport receiver, forwards to the
/// anchor's frame_tx. Monitors for heartbeat timeouts: `DETECTION_MULTIPLIER`
/// consecutive `heartbeat_deadline` windows with no frames trigger Dropped
/// sentinel injection, registry removal (LIVE-02), and cleanup -- but only
/// once a sender exists. While [`awaiting_sender`] holds, a pre-bind pump
/// times out every window by construction (there is no producer yet to be
/// silent), so the timeout branch gates on it instead of counting those
/// windows as misses. The transport-closed branch gates on the narrower
/// [`bind_unclaimed`] instead: a bind nobody has claimed has no sender to
/// speak of regardless of which door it was expecting one through, and the
/// mux's accept window closing it is the only reaper such a bind has left,
/// while a claimed bind's producer going silent is the watchdog's job. The
/// deadline is the anchor's configured cadence, carried to the sender on
/// `AnchorAttachResponse::heartbeat_interval_ms` for an attach or on
/// `StreamOpenTicket::heartbeat_interval_ms` for a ticket -- either way it is
/// `entry.heartbeat_interval` at the moment this pump was spawned.
pub(crate) async fn reader_pump(
    transport_rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<Vec<u8>>,
    cancel_token: tokio_util::sync::CancellationToken,
    ctx: crate::streaming::anchor::AnchorContext,
    pump: PumpContext,
) {
    let PumpContext {
        local_id,
        heartbeat_deadline,
        drain,
        prebound,
    } = pump;
    let crate::streaming::anchor::AnchorContext {
        registry,
        mpsc_registry,
        metrics,
    } = ctx;
    let mut missed_heartbeats: u8 = 0;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            result = tokio::time::timeout(heartbeat_deadline, transport_rx.recv_async()) => {
                match result {
                    Ok(Ok(bytes)) => {
                        // Any frame (data or heartbeat) proves liveness
                        missed_heartbeats = 0;
                        // Forward to anchor's frame channel.
                        //
                        // The per-anchor frame_tx is bounded(256) — the smallest
                        // channel in the saturation cascade and the first to fill
                        // when the consumer can't keep up. We try_send first so we
                        // can record a leading-indicator counter on the slow path
                        // before falling through to the awaited send.
                        match frame_tx.try_send(bytes) {
                            Ok(()) => {}
                            Err(flume::TrySendError::Full(b)) => {
                                if let Some(m) = metrics.as_ref() {
                                    m.record_reader_pump_backpressure();
                                }
                                if frame_tx.send_async(b).await.is_err() {
                                    break; // consumer dropped
                                }
                            }
                            Err(flume::TrySendError::Disconnected(_)) => break,
                        }
                        // The record is out of the buffer the mux issues credit
                        // against, so that credit is free. Telling the mux here
                        // is what lets its sweep interval be a backstop rather
                        // than the only way credit comes back — see
                        // `messenger_mux::ingress::DrainSignal`. `None` for
                        // every transport that does not do flow control over
                        // this seam, which pays one `Option` check per frame.
                        if let Some(drain) = drain.as_deref() {
                            drain.drained();
                        }
                    }
                    Ok(Err(_)) => {
                        // Transport channel closed -- the mux's accept window
                        // reclaiming an unclaimed bind (`release_bind` /
                        // `expire_bind` drop the bind's `frame_tx`, the other
                        // end of this `transport_rx`), since a claimed bind's
                        // channel does not otherwise close out from under a
                        // live producer.
                        //
                        // Gated on `bind_unclaimed`, not `awaiting_sender`:
                        // the accept window is a fixed 60 s from bind
                        // creation and does not care which of the two doors
                        // in `awaiting_sender` a sender was expected through,
                        // only whether one has actually claimed the bind. A
                        // real pre-bind and an ordinary or adopted attach
                        // whose peer never sent its `OpenSlot` both leave the
                        // registry entry with a live consumer and no other
                        // reaper once this channel closes -- the heartbeat
                        // watchdog only gets to run again if this arm defers
                        // to it, and its next fire, restarting from whenever
                        // `awaiting_sender` stopped exempting this pump, can
                        // land after the accept window that just closed (see
                        // `ACCEPT_TIMEOUT`'s doc on the residual window an
                        // adopted attach inherits). Once claimed, this branch
                        // never removes the entry -- something else already
                        // owns telling the registry about a live stream going
                        // away (finalize, cancel, or the watchdog).
                        //
                        // `!cancel_token.is_cancelled()` excludes the pump
                        // being *retired* rather than abandoned: releasing an
                        // unclaimed pre-bind (a mismatched-transport refusal,
                        // say) cancels this same token before dropping the
                        // bind, precisely so the entry it would otherwise
                        // remove -- reused by whatever attach wins next -- is
                        // never touched by a pump that no longer speaks for
                        // it. `tokio::select!` does not guarantee the cancel
                        // branch wins a tie, so the check has to be a state
                        // read here, not an ordering assumption.
                        if bind_unclaimed(drain.as_deref())
                            && !cancel_token.is_cancelled()
                            && let Some((_, entry)) = registry.remove(&local_id)
                        {
                            // The consumer must see `SenderDropped`, not a
                            // bare channel close: a bind reclaimed unclaimed
                            // means whatever sender it was minted for never
                            // showed up. Best-effort, unlike the
                            // watchdog-fired injection below only in that it
                            // runs after `registry.remove` rather than
                            // before -- there is nothing to race here, since
                            // an entry a concurrent finalize or cancel
                            // already removed makes `remove` return `None`
                            // and this whole block does not run, so a
                            // `Finalized` frame already in `frame_tx` never
                            // gets a spurious `Dropped` appended after it. A
                            // bind that was never claimed has forwarded no
                            // data, so `frame_tx` cannot be full here either
                            // way.
                            if let Some(m) = metrics.as_ref() {
                                m.record_unclaimed_bind_reaped();
                            }
                            let _ =
                                frame_tx.try_send(crate::streaming::sender::cached_dropped().clone());
                            entry.cancel_token.cancel();
                            crate::streaming::anchor::set_active_anchor_gauge(
                                metrics.as_ref(),
                                &registry,
                                &mpsc_registry,
                            );
                        }
                        break;
                    }
                    Err(_timeout) => {
                        // A slot still `awaiting_sender` times out on every
                        // window by construction -- there is no producer to
                        // be silent yet. Counting that as a miss is the bug:
                        // it arms this watchdog against a sender that has not
                        // shown up, capping how long a zero-RTT request may
                        // wait in a queue, or an adopted attach may wait for
                        // its own `OpenSlot`, at a bound nothing documents.
                        // Once a sender exists -- an `OpenSlot` claims the
                        // bind, or an attach adopts it -- every miss counts
                        // exactly as it always has, with the same
                        // `DETECTION_MULTIPLIER` margin the ordinary attach
                        // path has always given it.
                        if awaiting_sender(&prebound, drain.as_deref()) {
                            continue;
                        }
                        missed_heartbeats += 1;
                        if missed_heartbeats >= DETECTION_MULTIPLIER {
                            if let Some(m) = metrics.as_ref() {
                                m.record_heartbeat_watchdog_firing();
                            }
                            // Inject Dropped sentinel -- sender is dead.
                            // The diagnostic context here is what the saturation
                            // runbook tells operators to grep for: anchor channel
                            // depth at the moment of firing tells you whether the
                            // session was sitting at the bound (cascade) or empty
                            // (real producer crash).
                            tracing::warn!(
                                local_id,
                                anchor_frame_tx_len = frame_tx.len(),
                                anchor_frame_tx_cap = frame_tx.capacity().unwrap_or_default(),
                                transport_rx_len = transport_rx.len(),
                                transport_rx_cap = transport_rx.capacity().unwrap_or_default(),
                                heartbeat_deadline_ms = heartbeat_deadline.as_millis() as u64,
                                detection_multiplier = DETECTION_MULTIPLIER,
                                "reader_pump: heartbeat watchdog fired, injecting Dropped \
                                 (saturation indicator: see velo_streaming_*_backpressure_total)"
                            );
                            let dropped_bytes = crate::streaming::sender::cached_dropped().clone();
                            // Non-blocking: an anchor channel that is already
                            // full when the watchdog fires would deadlock a
                            // blocking await here -- registry cleanup and the
                            // cancel_token would never run, leaking a dead
                            // anchor. We accept that the consumer may see a
                            // plain channel-close (EOF) instead of an explicit
                            // SenderDropped in the saturated edge case; the
                            // watchdog firing metric + the warn! above are the
                            // authoritative signal for operators.
                            if frame_tx.try_send(dropped_bytes).is_err() {
                                tracing::warn!(
                                    local_id,
                                    "reader_pump: anchor channel saturated at watchdog-fire; \
                                     Dropped sentinel could not be injected, consumer will see \
                                     channel close (EOF) -- watchdog firing counter is the \
                                     authoritative signal here"
                                );
                            }
                            // LIVE-02: Full anchor cleanup -- remove from registry
                            // so no stale entry remains (ANCR-04)
                            if let Some((_, entry)) = registry.remove(&local_id) {
                                entry.cancel_token.cancel();
                                crate::streaming::anchor::set_active_anchor_gauge(
                                    metrics.as_ref(),
                                    &registry,
                                    &mpsc_registry,
                                );
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    // Cleanup: cancel token so other paths know the pump exited
    cancel_token.cancel();
}
