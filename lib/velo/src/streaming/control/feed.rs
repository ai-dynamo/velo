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
//! [`stream_watchdog`], a task that owns no data and wakes only on its timer
//! or when the mux closes the bind: it reaps a bind nobody claimed and injects
//! `Dropped` when a sender goes silent.
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::DETECTION_MULTIPLIER;
use super::pump::{awaiting_sender, bind_unclaimed, note_timer_arm, note_timer_fire};
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

/// What [`stream_watchdog`] needs besides the feed it watches.
pub(crate) struct WatchdogContext {
    pub(crate) local_id: u64,
    pub(crate) heartbeat_deadline: Duration,
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
    pub(crate) prebound: Arc<AtomicBool>,
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
///
/// Both callers start the feed after dropping the registry's shard lock, so a
/// cancel can remove the entry in between; the entry's drop then withdraws a
/// feed that is not installed yet. Checking the registry after the install
/// closes that gap: either the install lands first and the removal withdraws
/// it, or the check sees the entry gone and withdraws it here. Anchor ids are
/// never reused, so a present entry is this anchor's.
pub(crate) fn start_direct_stream(
    cell: &FeedCell,
    feed: DirectFeed,
    frame_tx: flume::Sender<Vec<u8>>,
    ctx: crate::streaming::anchor::AnchorContext,
    watch: WatchdogContext,
) {
    let feed = Arc::new(feed);
    cell.install(Arc::clone(&feed));
    if !ctx.registry.contains_key(&watch.local_id) {
        cell.withdraw();
        return;
    }
    let _ = frame_tx.try_send(crate::streaming::sender::cached_heartbeat().clone());
    tokio::spawn(stream_watchdog(feed, frame_tx, ctx, watch));
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
/// left alone.
pub(crate) fn reap_unclaimed(
    feed: &DirectFeed,
    local_id: u64,
    registry: &dashmap::DashMap<u64, crate::streaming::anchor::AnchorEntry>,
    metrics: Option<&crate::observability::VeloMetrics>,
) -> bool {
    if !bind_unclaimed(Some(&feed.drain)) || feed.pump_token.is_cancelled() {
        return false;
    }
    let Some((_, entry)) = registry.remove(&local_id) else {
        return false;
    };
    if let Some(m) = metrics {
        m.record_unclaimed_bind_reaped();
    }
    let _ = entry
        .frame_tx
        .try_send(crate::streaming::sender::cached_dropped().clone());
    entry.cancel_token.cancel();
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
pub(crate) async fn stream_watchdog(
    feed: Arc<DirectFeed>,
    frame_tx: flume::Sender<Vec<u8>>,
    ctx: crate::streaming::anchor::AnchorContext,
    watch: WatchdogContext,
) {
    let WatchdogContext {
        local_id,
        heartbeat_deadline,
        prebound,
    } = watch;
    let cancel_token = feed.pump_token.clone();
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
                reap_unclaimed(&feed, local_id, &ctx.registry, ctx.metrics.as_deref());
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
                    }
                    break;
                }
            }
        }
    }
    cancel_token.cancel();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry() -> crate::streaming::anchor::AnchorEntry {
        crate::streaming::anchor::AnchorEntry {
            feed: Default::default(),
            frame_tx: flume::bounded(4).0,
            cancel_token: CancellationToken::new(),
            active_pump_token: None,
            attachment: false,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: std::time::Duration::from_secs(5),
            stream_cancel_handle: None,
            prebind: None,
            stop_requested: false,
        }
    }

    fn unclaimed_feed(pump_token: CancellationToken) -> DirectFeed {
        let (wake, _) = flume::unbounded();
        DirectFeed {
            rx: flume::bounded(1).1,
            drain: Arc::new(DrainSignal::new(wake)),
            pump_token,
        }
    }

    /// A retired feed never reaps; an abandoned one reaps exactly once.
    ///
    /// Releasing a pre-bind on a transport mismatch cancels its pump token and
    /// then drops the bind, which closes it exactly as the accept window does.
    /// The entry that close would reap is the one the winning attach is about
    /// to reuse, so the cancelled token is what must stop the reap -- whichever
    /// of the consumer and the watchdog sees the close.
    /// A feed started for an anchor that is no longer registered is withdrawn.
    ///
    /// The attach handler and `prebind_anchor` start the feed after they drop
    /// the registry's shard lock. A cancel landing in that gap removes the
    /// entry first, and the entry's drop withdraws a feed that is not there
    /// yet; the install that follows would then leave the consumer reading a
    /// cancelled stream's slot buffer. The pump this replaced was spawned with
    /// the already-cancelled token and exited at once, so the feed has to check
    /// the same thing.
    #[tokio::test]
    async fn a_feed_started_for_a_removed_anchor_is_withdrawn() {
        let ctx = crate::streaming::anchor::AnchorContext {
            registry: Arc::new(dashmap::DashMap::new()),
            mpsc_registry: Arc::new(dashmap::DashMap::new()),
            metrics: None,
        };
        let cell = FeedCell::default();
        let (_tx, rx) = flume::bounded::<Vec<u8>>(4);
        let (frame_tx, _frame_rx) = flume::bounded::<Vec<u8>>(4);
        let (wake, _) = flume::unbounded();
        start_direct_stream(
            &cell,
            DirectFeed {
                rx,
                drain: Arc::new(DrainSignal::new(wake)),
                pump_token: CancellationToken::new(),
            },
            frame_tx,
            ctx,
            WatchdogContext {
                local_id: 1,
                heartbeat_deadline: Duration::from_secs(5),
                prebound: Arc::new(AtomicBool::new(false)),
            },
        );
        assert!(
            cell.current().is_none(),
            "anchor 1 is not registered, so its feed must not stay installed"
        );
    }

    #[test]
    fn a_retired_feed_does_not_reap_and_an_abandoned_one_reaps_once() {
        let ctx = crate::streaming::anchor::AnchorContext {
            registry: Arc::new(dashmap::DashMap::new()),
            mpsc_registry: Arc::new(dashmap::DashMap::new()),
            metrics: None,
        };
        ctx.registry.insert(1, entry());

        let retired = CancellationToken::new();
        retired.cancel();
        assert!(!reap_unclaimed(
            &unclaimed_feed(retired),
            1,
            &ctx.registry,
            None
        ));
        assert!(
            ctx.registry.contains_key(&1),
            "a retired feed must leave the entry"
        );

        let abandoned = unclaimed_feed(CancellationToken::new());
        assert!(reap_unclaimed(&abandoned, 1, &ctx.registry, None));
        assert!(!ctx.registry.contains_key(&1));
        assert!(
            !reap_unclaimed(&abandoned, 1, &ctx.registry, None),
            "the second sight of the close is a no-op"
        );
    }
}
