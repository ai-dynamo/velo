// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The reader pump: bridges transport frames to an anchor's delivery channel.
//!
//! Split out of `control.rs` (the file's own `// --- Reader pump ---` banner
//! marked the seam before the split). [`reader_pump`] serves every
//! per-stream transport and is spawned by the attach handler. A mux bind has
//! no reader pump: its consumer reads the slot buffer itself, and
//! [`super::feed`] holds what the pump did besides moving data.
//! [`PumpContext`], [`bind_unclaimed`] and [`awaiting_sender`] live here for
//! the two users that still need a bind's drain signal: that feed's watchdog,
//! and `mpsc/control.rs`'s `mpsc_reader_pump`, which reuses [`PumpContext`]
//! rather than duplicating it.

use std::time::Duration;

use super::DETECTION_MULTIPLIER;

#[cfg(test)]
tokio::task_local! {
    /// Counts how many times a pump or a stream watchdog has armed or
    /// re-armed its heartbeat timer.
    ///
    /// A task-local rather than a field on [`PumpContext`]: the property it
    /// exists to pin -- that a task arms one timer for its stream instead of
    /// one per record -- belongs to the task's own loop, not to anything a
    /// caller hands it, so a field would have had to be threaded through every
    /// production spawn site to observe something none of them decide.
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
    /// Counts how many times a pump's or watchdog's heartbeat timer has
    /// actually elapsed.
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

/// What a mux-fed task needs beyond its channels: the direct feed's
/// [`stream_watchdog`](super::feed::stream_watchdog) and `mpsc_reader_pump`.
/// [`reader_pump`] reads no drain signal and does not take one.
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
    /// The mux bind's drain signal. `mpsc_reader_pump` tells it when a record
    /// leaves the buffer credit is issued against; the watchdog reads its
    /// arrival count and close. `None` for every transport that does not do
    /// flow control over this seam.
    pub(crate) drain: Option<std::sync::Arc<crate::streaming::messenger_mux::ingress::DrainSignal>>,
    /// Whether this task's slot still has no sender, other than through its
    /// own `OpenSlot`.
    ///
    /// `true` only at the one genuine pre-bind spawn site
    /// (`AnchorManager::prebind_anchor`); every ordinary attach spawn passes
    /// a fresh `Arc::new(AtomicBool::new(false))`. Shared with the
    /// `PreBind` the task was spawned for, and cleared by
    /// [`PreBind::adopt`](crate::streaming::anchor::PreBind::adopt) the
    /// moment a sender attaches the long way round instead of opening on its
    /// ticket -- the one other door through which a sender can show up, and
    /// the one transition an `Arc<AtomicBool>` exists to carry immediately
    /// rather than the task learning it only once that sender's own
    /// `OpenSlot` lands.
    ///
    /// `drain.claimed().is_none()` is not a proxy for this on its own: the
    /// mux parks a `DrainSignal` for *every* bind, including an ordinary
    /// attach's, and that signal stays unclaimed until the peer's `OpenSlot`
    /// arrives -- which is necessarily after the attach response already
    /// returned. See [`awaiting_sender`] for the combined read the
    /// watchdog's heartbeat exemption needs; the unclaimed-bind reap
    /// (`feed::reap_unclaimed`) needs only the claim, see [`bind_unclaimed`].
    pub(crate) prebound: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Whether an `OpenSlot` has claimed the mux bind this task reads from.
///
/// `None` (a transport with no `DrainSignal`) answers `false`: without a
/// drain there is no accept window racing to reclaim the bind either, so a
/// closed transport there has always meant a sender that genuinely departed,
/// not one still on its way in. This is the condition
/// `feed::reap_unclaimed` gates on -- it does not care which of the two doors
/// in [`awaiting_sender`] a sender was expected to come through, only whether
/// one has.
pub(super) fn bind_unclaimed(
    drain: Option<&crate::streaming::messenger_mux::ingress::DrainSignal>,
) -> bool {
    drain.is_some_and(|d| d.claimed().is_none())
}

/// Whether a mux bind still has no sender -- the state that must not count
/// against the heartbeat watchdog.
///
/// Two different doors let a sender show up, and this has to watch both:
/// `prebound` answers for the one that opens on its ticket directly (an
/// `OpenSlot` claims `drain`, no attach in between) by being cleared the
/// instant [`PreBind::adopt`](crate::streaming::anchor::PreBind::adopt)
/// reassigns the slot to a sender that attached the ordinary way instead --
/// `drain` alone cannot see that sender, since its own `OpenSlot` may still
/// be seconds or minutes away. `prebound` alone is not enough either: an
/// ordinary attach's watchdog starts with an unclaimed `drain` too, for as
/// long as the peer's own `OpenSlot` is still in flight, and that watchdog is
/// spawned with `prebound` already `false` for exactly that reason.
pub(super) fn awaiting_sender(
    prebound: &std::sync::atomic::AtomicBool,
    drain: Option<&crate::streaming::messenger_mux::ingress::DrainSignal>,
) -> bool {
    prebound.load(std::sync::atomic::Ordering::Relaxed) && bind_unclaimed(drain)
}

/// Reader pump: bridges transport frames to the anchor's delivery channel.
///
/// Spawned by the attach handler for every per-stream transport. A mux bind
/// does not get one: its consumer reads the slot buffer itself and
/// [`stream_watchdog`](super::feed::stream_watchdog) keeps the lifecycle
/// duties. Reads from the transport receiver, forwards to the anchor's
/// frame_tx. Monitors for heartbeat loss with one timer for the whole stream:
/// `DETECTION_MULTIPLIER * heartbeat_deadline` of silence triggers Dropped
/// sentinel injection, registry removal (LIVE-02), and cleanup.
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
/// on its first look is one whose `L` predates the arm -- the arm at spawn
/// -- and a per-record timeout restarted its window there too.
///
/// The deadline is the anchor's configured cadence, carried to the sender on
/// `AnchorAttachResponse::heartbeat_interval_ms` -- `entry.heartbeat_interval`
/// at the moment this pump was spawned.
pub(crate) async fn reader_pump(
    transport_rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<Vec<u8>>,
    cancel_token: tokio_util::sync::CancellationToken,
    ctx: crate::streaming::anchor::AnchorContext,
    local_id: u64,
    heartbeat_deadline: Duration,
) {
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
                    }
                    Err(_) => {
                        // The transport let go of the stream: the sender is
                        // gone, and the consumer sees the channel close.
                        break;
                    }
                }
            }
            _ = &mut sleep => {
                note_timer_fire();
                // Every path out of this arm re-arms the sleep first. A fired
                // `Sleep` stays ready until it is reset, so a `continue` past
                // one turns the pump into a hot loop instead of a wait on
                // every window.
                let idle = last_frame.elapsed();
                if idle < heartbeat_deadline {
                    // A frame landed inside this window, so the deadline that
                    // frame implies has not arrived yet. Reached only when
                    // the last frame fell in the first half of a window --
                    // the receive arm's push keeps a stream carrying records
                    // out of this arm entirely.
                    armed_until = last_frame + heartbeat_deadline;
                    sleep.as_mut().reset(armed_until);
                    note_timer_arm();
                    continue;
                }
                armed_until = tokio::time::Instant::now() + heartbeat_deadline;
                sleep.as_mut().reset(armed_until);
                note_timer_arm();
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
