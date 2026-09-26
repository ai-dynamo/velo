// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The mux's direct feed: the consumer reads its slot buffer itself.
//!
//! Every other transport hands a bind's receiver to [`reader_pump`], which
//! moves each frame into the anchor's channel. For a mux bind that hop cost a
//! task wake, a channel send and receive, and a hook allocation per record on
//! the frontend — the largest single stage of its profile — and it made the
//! credit window wrong: the pump counted each move as a drain, so a consumer
//! that stopped reading still earned its sender the anchor channel's 256
//! records of credit on top of the window.
//!
//! So for a mux bind the [`StreamAnchor`] polls the slot buffer directly,
//! ahead of its own channel, and tells the [`DrainSignal`] when *it* takes a
//! record. What the pump did besides moving data survives in
//! [`stream_watchdog`], a task that owns no data and wakes only on its timer:
//! it reaps a bind nobody claimed and injects `Dropped` when a sender goes
//! silent.
//!
//! The rejected alternative was merging the pump into the anchor channel
//! (`docs/src/development/batched-streaming-design.md`): credit is issued
//! against the slot buffer because the mux is its only writer, and the anchor
//! channel has other writers. That still holds here — the consumer drains the
//! mux-owned buffer, and the anchor channel carries only sentinels.
//!
//! [`reader_pump`]: super::reader_pump
//! [`StreamAnchor`]: crate::streaming::anchor::StreamAnchor
//! [`DrainSignal`]: crate::streaming::messenger_mux::ingress::DrainSignal

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio_util::sync::CancellationToken;

use super::DETECTION_MULTIPLIER;
use super::pump::{PumpContext, awaiting_sender, bind_unclaimed, note_timer_arm, note_timer_fire};
use crate::streaming::messenger_mux::ingress::DrainSignal;

/// One mux bind's slot buffer, as the consumer reads it.
pub(crate) struct DirectFeed {
    /// The bind's receiver: the `C + 1` buffer credit is issued against.
    pub(crate) rx: flume::Receiver<Vec<u8>>,
    /// Told each time the consumer takes a record out of `rx`.
    pub(crate) drain: Arc<DrainSignal>,
    /// The token the pump this feed replaced would have carried. Cancelled
    /// when the bind is retired rather than abandoned (a released pre-bind),
    /// which is what tells a closed `rx` apart from an unclaimed bind reaped
    /// by the accept window.
    pub(crate) pump_token: CancellationToken,
}

/// The anchor's current feed, replaceable across detach and reattach.
///
/// A generation counter lets the consumer check for a new feed with one atomic
/// load per poll and take the lock only when one was installed.
#[derive(Default)]
pub(crate) struct FeedCell {
    generation: AtomicU64,
    feed: parking_lot::Mutex<Option<Arc<DirectFeed>>>,
}

impl FeedCell {
    fn install(&self, feed: Arc<DirectFeed>) {
        *self.feed.lock() = Some(feed);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Take the feed out, so the consumer stops reading that slot buffer on
    /// its next poll. Called wherever the feed's pump token is cancelled and
    /// when the anchor entry is removed: the token alone no longer cuts the
    /// data path, because the consumer is the data path.
    pub(crate) fn withdraw(&self) {
        if self.feed.lock().take().is_some() {
            self.generation.fetch_add(1, Ordering::Release);
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn current(&self) -> Option<Arc<DirectFeed>> {
        self.feed.lock().clone()
    }
}

/// Start a mux bind's stream: install the feed, wake the consumer, spawn the
/// watchdog.
///
/// Called at the two places a mux bind gets its consumer side — the attach
/// handler and `AnchorManager::prebind_anchor` — where every other transport
/// spawns [`reader_pump`](super::reader_pump).
///
/// The wake is a heartbeat frame on the anchor's own channel: a consumer
/// already parked there (an ordinary attach lands while the application is
/// waiting for the first frame) has no other way to learn it should look at the
/// feed, and the frame is queued, so it cannot be lost to a race with the
/// consumer's next poll. The consumer drops heartbeats before they reach the
/// application.
pub(crate) fn start_direct_stream(
    cell: &FeedCell,
    rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<Vec<u8>>,
    pump_token: CancellationToken,
    ctx: crate::streaming::anchor::AnchorContext,
    pump: PumpContext,
) {
    let drain = pump
        .drain
        .clone()
        .expect("a direct feed exists only for a mux bind, which always has a drain signal");
    cell.install(Arc::new(DirectFeed {
        rx: rx.clone(),
        drain,
        pump_token: pump_token.clone(),
    }));
    let _ = frame_tx.try_send(crate::streaming::sender::cached_heartbeat().clone());
    tokio::spawn(stream_watchdog(rx, frame_tx, pump_token, ctx, pump));
}

/// Reap a bind nobody claimed whose buffer just closed, exactly once.
///
/// The accept window closing an unclaimed bind drops its sender, so the
/// consumer sees `SenderDropped` rather than waiting forever. Shared by the
/// consumer, which sees the close first when it is polling, and
/// [`stream_watchdog`], which sees it when nobody is; `registry.remove` makes
/// whichever comes second a no-op.
///
/// A cancelled `pump_token` means the bind was *retired*, not abandoned — a
/// pre-bind released on a transport mismatch cancels it before dropping the
/// bind, so the entry it would remove, reused by whatever attach wins next, is
/// left alone. `reader_pump` carries the same guard for the same reason.
pub(crate) fn reap_unclaimed(
    feed: &DirectFeed,
    local_id: u64,
    ctx: &crate::streaming::anchor::AnchorContext,
) -> bool {
    if !bind_unclaimed(Some(&feed.drain)) || feed.pump_token.is_cancelled() {
        return false;
    }
    let Some((_, entry)) = ctx.registry.remove(&local_id) else {
        return false;
    };
    if let Some(m) = ctx.metrics.as_ref() {
        m.record_unclaimed_bind_reaped();
    }
    let _ = entry
        .frame_tx
        .try_send(crate::streaming::sender::cached_dropped().clone());
    entry.cancel_token.cancel();
    crate::streaming::anchor::set_active_anchor_gauge(
        ctx.metrics.as_ref(),
        &ctx.registry,
        &ctx.mpsc_registry,
    );
    true
}

/// The pump's lifecycle duties for a mux bind, with no data on its path.
///
/// Wakes once per `heartbeat_deadline`, never per record. A window counts as
/// live if the ingress delivered anything to the slot during it (the slot's
/// arrival count moved) or if the buffer still holds records: a consumer that
/// is behind leaves the sender without credit, and a sender without credit
/// cannot heartbeat, so that silence is not the sender's. After
/// `DETECTION_MULTIPLIER` windows with neither, and once a sender exists, the
/// watchdog injects `Dropped` and removes the anchor, as the pump did.
///
/// Detection lands between `DETECTION_MULTIPLIER` and one more window after the
/// last arrival: windows run on the watchdog's own clock, not from the last
/// frame, because the watchdog never sees a frame.
///
/// Exits when its token is cancelled, when the mux closes the bind's buffer
/// (the stream ended, or an unclaimed bind was released — which it reaps on
/// the spot, as the pump did on seeing its receiver close), or when it fires.
async fn stream_watchdog(
    rx: flume::Receiver<Vec<u8>>,
    frame_tx: flume::Sender<Vec<u8>>,
    cancel_token: CancellationToken,
    ctx: crate::streaming::anchor::AnchorContext,
    pump: PumpContext,
) {
    let PumpContext {
        local_id,
        heartbeat_deadline,
        drain,
        prebound,
    } = pump;
    let drain = drain.expect("a direct feed exists only for a mux bind");
    let feed = DirectFeed {
        rx,
        drain,
        pump_token: cancel_token.clone(),
    };
    let mut seen = feed.drain.arrivals();
    let mut missed: u8 = 0;
    let sleep = tokio::time::sleep(heartbeat_deadline);
    tokio::pin!(sleep);
    note_timer_arm();
    let cancelled = cancel_token.cancelled();
    tokio::pin!(cancelled);
    let bind_closed = feed.drain.closed();
    let closed = bind_closed.cancelled();
    tokio::pin!(closed);

    loop {
        tokio::select! {
            biased;
            _ = &mut cancelled => break,
            _ = &mut closed => {
                reap_unclaimed(&feed, local_id, &ctx);
                break;
            }
            _ = &mut sleep => {
                note_timer_fire();
                sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat_deadline);
                note_timer_arm();
                let arrivals = feed.drain.arrivals();
                if arrivals != seen || !feed.rx.is_empty() {
                    seen = arrivals;
                    missed = 0;
                    continue;
                }
                // A slot with no sender yet is silent by construction; see
                // `awaiting_sender`.
                if awaiting_sender(&prebound, Some(&feed.drain)) {
                    continue;
                }
                missed += 1;
                if missed >= DETECTION_MULTIPLIER {
                    if let Some(m) = ctx.metrics.as_ref() {
                        m.record_heartbeat_watchdog_firing();
                    }
                    tracing::warn!(
                        local_id,
                        anchor_frame_tx_len = frame_tx.len(),
                        heartbeat_deadline_ms = heartbeat_deadline.as_millis() as u64,
                        detection_multiplier = DETECTION_MULTIPLIER,
                        "stream_watchdog: no arrivals and an empty slot buffer for \
                         the detection window, injecting Dropped"
                    );
                    let _ = frame_tx.try_send(crate::streaming::sender::cached_dropped().clone());
                    if let Some((_, entry)) = ctx.registry.remove(&local_id) {
                        entry.cancel_token.cancel();
                        crate::streaming::anchor::set_active_anchor_gauge(
                            ctx.metrics.as_ref(),
                            &ctx.registry,
                            &ctx.mpsc_registry,
                        );
                    }
                    break;
                }
            }
        }
    }
    cancel_token.cancel();
}
