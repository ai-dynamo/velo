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

#[cfg(test)]
tokio::task_local! {
    /// Counts how many times a pump has armed or re-armed its heartbeat
    /// timer.
    ///
    /// A task-local rather than a field on [`PumpContext`]: the property it
    /// exists to pin -- that a pump arms one timer for its stream instead of
    /// one per record -- belongs to the pump's own loop, not to anything a
    /// caller hands it, so a field would have had to be threaded through all
    /// three production spawn sites to observe something none of them decide.
    /// Scoping it per task also keeps each test's count its own while the
    /// suite runs them in parallel, which a process-wide counter could not.
    pub(crate) static TIMER_ARMS: std::sync::Arc<std::sync::atomic::AtomicU64>;
}

/// Record one timer arm. Nothing observes it outside a test scope.
#[cfg(test)]
pub(crate) fn note_timer_arm() {
    let _ = TIMER_ARMS.try_with(|arms| arms.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
}

/// Compiles away entirely: the seam must cost the shipped pump nothing.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn note_timer_arm() {}

#[cfg(test)]
tokio::task_local! {
    /// Counts how many times a pump's heartbeat timer has actually elapsed.
    ///
    /// A second counter rather than a flag on [`TIMER_ARMS`] because the two
    /// answer different questions and the receive-arm re-arm below moves them
    /// in opposite directions: a stream under traffic pushes its deadline
    /// forward roughly twice per deadline, so its arm count *rises*, while its
    /// fire count goes to zero. An arm count alone cannot tell a timer that
    /// never fires from one that fires every window.
    pub(crate) static TIMER_FIRES: std::sync::Arc<std::sync::atomic::AtomicU64>;
}

/// Record one timer fire. Nothing observes it outside a test scope.
#[cfg(test)]
pub(crate) fn note_timer_fire() {
    let _ = TIMER_FIRES.try_with(|fires| fires.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
}

/// Compiles away entirely: the seam must cost the shipped pump nothing.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn note_timer_fire() {}

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
/// anchor's frame_tx. Monitors for heartbeat loss with one timer for the
/// whole stream: `DETECTION_MULTIPLIER * heartbeat_deadline` of silence
/// triggers Dropped sentinel injection, registry removal (LIVE-02), and
/// cleanup -- but only once a sender exists.
///
/// That timer is armed once before the loop and moved by two rules. A
/// received frame pushes it forward only when its deadline is already inside
/// half a window, setting it a full `heartbeat_deadline` past the frame;
/// otherwise the frame leaves it alone. A fire compares against `last_frame`
/// and re-arms from there. Under steady traffic the first rule keeps the
/// deadline at least half a window ahead of the clock, so the timer never
/// fires and is moved at most twice per deadline.
///
/// Detection still lands where a timer rebuilt per record would have put it.
/// Write `L` for the instant of the last frame and `d` for
/// `heartbeat_deadline`. When frames stop, the deadline sits somewhere in
/// `[L + d/2, L + d]` -- the receive rule only ever sets it a full `d` past a
/// frame, and only from inside `d/2`. A deadline at `L + d` fires there, sees
/// exactly `d` of silence and counts the first miss; a deadline short of it
/// fires early, finds `L.elapsed() < d`, counts no miss and re-arms to
/// `L + d`, where that first miss is counted instead. Either way the misses
/// land at `L + d`, `L + 2d`, `L + 3d`, and the `DETECTION_MULTIPLIER`th at
/// `L + DETECTION_MULTIPLIER * d`. The only fire that finds `L.elapsed() >= d`
/// on its first look is one whose `L` predates the arm -- the arm at spawn,
/// or the re-arm an [`awaiting_sender`] window takes -- and a per-record
/// timeout restarted its window at exactly those points too.
///
/// While [`awaiting_sender`] holds, a pre-bind pump times out every window by
/// construction (there is no producer yet to be silent), so the timer branch
/// gates on it instead of counting those windows as misses. The
/// transport-closed branch gates on the narrower
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
    // One timer for the stream, not one per record. `tokio::time::timeout`
    // builds a fresh `Sleep` every trip: its first poll registers a timer
    // entry with the driver and its drop deregisters it, both under the
    // driver's lock, so a stream carrying N records took 2N turns of that
    // lock -- 7.3 percent of the frontend's cores on the tier-3 profile, for
    // a deadline that almost never fires. A pinned `Sleep` that is already
    // registered polls without the lock (tokio's `poll_elapsed` takes its
    // `registered` branch), which leaves one clock read per record in its
    // place. The cancellation future is hoisted for the same reason: rebuilt
    // per trip it adds and removes a waiter on the token each time.
    //
    // The timer is moved by two rules, and neither runs per record. The
    // receive arm below pushes the deadline forward only once it is inside
    // half a window; the fired arm re-arms from `last_frame`. Under steady
    // traffic the first keeps the deadline at least half a window ahead of
    // the clock, so this timer never fires and moves at most twice per
    // deadline -- where resetting it on every frame would put the
    // deregister/register pair back, and re-arming it only from its own fire
    // (the shape this replaced) left every live stream firing once per
    // window, thousands of them in phase.
    let mut last_frame = tokio::time::Instant::now();
    // Hoisted out of the per-record path: the check below is then one
    // comparison against an `Instant` already stamped, no division and no
    // second clock read.
    let rearm_threshold = heartbeat_deadline / 2;
    let mut armed_until = last_frame + heartbeat_deadline;
    let sleep = tokio::time::sleep_until(armed_until);
    tokio::pin!(sleep);
    note_timer_arm();
    let cancelled = cancel_token.cancelled();
    tokio::pin!(cancelled);

    loop {
        tokio::select! {
            // `tokio::time::timeout`, which this replaced, always polled the
            // receive first and only checked its own deadline if that was
            // Pending -- a ready receive could never lose. An unbiased
            // `select!` instead starts each poll from a pseudo-random branch,
            // so a heartbeat landing right at the deadline boundary (it is
            // sent on the same cadence as this pump's own deadline) can find
            // both the receive and the fired sleep ready at once and let the
            // sleep win the coin flip, discarding a frame that just proved
            // liveness -- an explicit terminal, worst case, reported to the
            // consumer as a watchdog-driven `Dropped` instead of the real
            // sentinel. `biased` restores `timeout`'s exact priority.
            biased;
            _ = &mut cancelled => break,
            received = transport_rx.recv_async() => {
                match received {
                    Ok(bytes) => {
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
                        // Any frame (data or heartbeat) proves liveness -- but
                        // only once it is actually forwarded. A `frame_tx` that
                        // is full makes the `send_async` above block for as
                        // long as the consumer takes to free a slot; that is
                        // the pump doing real work, not the sender going
                        // silent, and stamping on arrival instead of here would
                        // charge the block against the sender's heartbeat
                        // budget, so a watchdog window that should need
                        // `DETECTION_MULTIPLIER * heartbeat_deadline` of actual
                        // silence would instead fire one window early --
                        // `messenger::server::lanes` takes the same stance on
                        // its own `last_item` for the same reason.
                        missed_heartbeats = 0;
                        last_frame = tokio::time::Instant::now();
                        // Push the deadline out only once it is inside half a
                        // window. That bounds this to twice per deadline
                        // under any record rate, so the timer never fires on
                        // a live stream, and it costs a comparison against
                        // the instant just stamped. Resetting on every record
                        // instead would be a deregister/register pair on the
                        // time driver's lock per record -- the cost the one
                        // timer per stream was hoisted to avoid.
                        if armed_until.saturating_duration_since(last_frame)
                            < rearm_threshold
                        {
                            armed_until = last_frame + heartbeat_deadline;
                            sleep.as_mut().reset(armed_until);
                            note_timer_arm();
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
                    Err(_) => {
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
                        // it. The `biased` cancel branch above always wins a
                        // poll where both it and this receive are ready, but
                        // cancellation can still land in the gap after this
                        // poll already committed to this arm's body, so the
                        // check stays a state read rather than an ordering
                        // assumption.
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
                }
            }
            _ = &mut sleep => {
                note_timer_fire();
                // Every path out of this arm re-arms the sleep first. A fired
                // `Sleep` stays ready until it is reset, so a `continue` past
                // one turns the pump into a hot loop instead of a wait --
                // which is exactly what a pre-bind pump, exempt from the
                // miss count below, would otherwise do on every window.
                let idle = last_frame.elapsed();
                if idle < heartbeat_deadline {
                    // A frame landed inside this window, so the deadline that
                    // frame implies has not arrived yet. Reached only when
                    // the last frame fell in the first half of a window, or
                    // when there is no sender yet -- the receive arm's push
                    // keeps a stream carrying records out of this arm
                    // entirely.
                    armed_until = last_frame + heartbeat_deadline;
                    sleep.as_mut().reset(armed_until);
                    note_timer_arm();
                    continue;
                }
                armed_until = tokio::time::Instant::now() + heartbeat_deadline;
                sleep.as_mut().reset(armed_until);
                note_timer_arm();
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
    // Cleanup: cancel token so other paths know the pump exited
    cancel_token.cancel();
}
