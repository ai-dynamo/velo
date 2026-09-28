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
    /// How to close this feed's slot when the consumer ends the stream itself.
    /// `None` only in tests that build a feed without a mux.
    pub(crate) release: Option<SlotRelease>,
}

/// What a consumer needs to close its own slot.
///
/// A consumer that ends the stream on its side -- a record it cannot decode, a
/// transport error -- leaves a sender that is still sending. The mux learns a
/// consumer is gone when a delivery finds no receiver, but the consumer's
/// receiver clones may outlive the stream, and a sender that fills its window
/// and parks sends nothing more for a delivery to fail on. So the consumer
/// closes the slot directly and tells the sender, the way a pre-bind's owner
/// does when it gives up (`PreBind`'s `Drop`).
pub(crate) struct SlotRelease {
    pub(crate) mux: std::sync::Weak<crate::streaming::messenger_mux::MessengerMuxTransport>,
    pub(crate) anchor_id: u64,
    pub(crate) session_id: u64,
}

impl DirectFeed {
    /// Close this feed's slot and tell its sender to abandon its end. A no-op
    /// where there is nothing to close, including a stream that already ended
    /// on its own terminal.
    pub(crate) fn release_slot(&self) {
        let Some(release) = &self.release else {
            return;
        };
        // The ordinary end: the sender's terminal retired the slot already,
        // so there is nothing to close and no reason to take the peer's lock.
        if self.drain.is_released() {
            return;
        }
        let Some(mux) = release.mux.upgrade() else {
            return;
        };
        // `cancel`, not `claimed`: it marks the bind cancelled under the lock
        // an `OpenSlot`'s claim takes, so a claim that lands after it finds
        // the mark and closes the slot itself (`open_slot`'s cancelled arm)
        // rather than opening one nobody reads.
        match self.drain.cancel() {
            Some((peer, slot)) => mux.cancel_claimed_session(peer, slot, release.session_id),
            None => mux.release_bind(release.anchor_id, release.session_id),
        }
    }
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
    /// Install `feed`, returning the one it replaces, if any.
    ///
    /// The generation moves under the same lock as the feed, so a
    /// [`snapshot`](Self::snapshot) never pairs a feed with another feed's
    /// generation.
    fn install(&self, feed: Arc<DirectFeed>) -> Option<Arc<DirectFeed>> {
        let mut slot = self.feed.lock();
        let replaced = slot.replace(feed);
        self.generation.fetch_add(1, Ordering::Release);
        replaced
    }

    /// Take the feed out, so the consumer stops reading that slot buffer on
    /// its next poll. Called wherever the feed's pump token is cancelled and
    /// when the anchor entry is removed: the token alone no longer cuts the
    /// data path, because the consumer is the data path.
    pub(crate) fn withdraw(&self) -> Option<Arc<DirectFeed>> {
        let mut slot = self.feed.lock();
        let feed = slot.take();
        if feed.is_some() {
            self.generation.fetch_add(1, Ordering::Release);
        }
        feed
    }

    /// Take `feed` out if it is still the installed one, so the feed of a
    /// stream that ended does not outlive it, and leave a newer feed alone.
    pub(crate) fn withdraw_if(&self, feed: &Arc<DirectFeed>) -> Option<Arc<DirectFeed>> {
        let mut slot = self.feed.lock();
        if !slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, feed))
        {
            return None;
        }
        self.generation.fetch_add(1, Ordering::Release);
        slot.take()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn current(&self) -> Option<Arc<DirectFeed>> {
        self.feed.lock().clone()
    }

    /// The feed together with the generation it was installed at.
    pub(crate) fn snapshot(&self) -> (u64, Option<Arc<DirectFeed>>) {
        let slot = self.feed.lock();
        (self.generation.load(Ordering::Acquire), slot.clone())
    }
}

/// Install a mux bind's feed for its consumer. Call with the anchor entry's
/// shard lock held, then [`launch_direct_stream`] once it is dropped.
///
/// Under the lock because a retire (`AnchorEntry::retire_pump`) and a removal
/// both take that lock too: installing after it dropped could put back a feed
/// that was retired in the gap, over the feed of the attach that replaced it.
///
/// Takes the entry itself, not its `Arc<FeedCell>`, so a caller can only
/// reach it through a registry guard: cloning the cell out and installing
/// after the lock drops does not type-check.
///
/// Returns the new feed and the one it replaced, if any. A feed still
/// installed belongs to a stream this anchor has moved on from, and nothing
/// else holds it to close its slot, so the caller calls
/// [`DirectFeed::release_slot`] on it once the shard lock drops: the close
/// takes the peer's ingress lock.
#[must_use = "the replaced feed's slot must be closed once the shard lock drops"]
pub(crate) fn install_direct_feed(
    entry: &mut crate::streaming::anchor::AnchorEntry,
    feed: DirectFeed,
) -> (Arc<DirectFeed>, Option<Arc<DirectFeed>>) {
    let feed = Arc::new(feed);
    let replaced = entry.feed.install(Arc::clone(&feed));
    if let Some(replaced) = &replaced {
        // Cancelled under the shard lock, the lock a firing watchdog reads
        // the token under (`fire_watchdog`), so the replaced feed's watchdog
        // cannot remove the entry, now the new feed's.
        replaced.pump_token.cancel();
    }
    (feed, replaced)
}

/// Start a mux bind's stream: wake the consumer, spawn the watchdog.
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
pub(crate) fn launch_direct_stream(
    feed: Arc<DirectFeed>,
    frame_tx: flume::Sender<Vec<u8>>,
    ctx: crate::streaming::anchor::AnchorContext,
    watch: WatchdogContext,
) {
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
/// arrival count moved) or if the sender holds no data credit. Heartbeats
/// spend data credit, so a sender whose consumer is behind -- the record
/// window unread, or its credit withheld because the buffer holds its byte
/// budget -- cannot send one, and that silence is not the sender's. A sender
/// that still holds credit can heartbeat, so its silence is its own even with
/// records unread. After `DETECTION_MULTIPLIER` windows with neither, and once
/// a sender exists, the watchdog injects `Dropped` and removes the anchor, as
/// the pump did.
///
/// Detection lands between `DETECTION_MULTIPLIER` and one more window after the
/// last arrival: windows run on the watchdog's own clock, not from the last
/// frame, because the watchdog never sees a frame.
///
/// A firing removes the anchor, which withdraws the feed, so records still in
/// the slot buffer are not delivered: the consumer's next poll reads
/// `SenderDropped`. The reader pump delivered them first. Keeping them would
/// mean leaving the entry in place until the consumer drained, for records
/// from a sender already judged dead on a stream that ends in an error either
/// way.
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
                if arrivals != seen {
                    seen = arrivals;
                    missed = 0;
                    continue;
                }
                // Heartbeats spend data credit, so a sender holding none
                // cannot send one; that silence is its consumer's, not its own.
                if feed.drain.sender_parked() {
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
                    if !fire_watchdog(&cancel_token, &frame_tx, &ctx.registry, local_id) {
                        break;
                    }
                    if let Some(m) = ctx.metrics.as_ref() {
                        m.record_heartbeat_watchdog_firing();
                    }
                    tracing::warn!(
                        local_id,
                        slot_buffer_len = feed.rx.len(),
                        arrivals = seen,
                        heartbeat_deadline_ms = heartbeat_deadline.as_millis() as u64,
                        detection_multiplier = DETECTION_MULTIPLIER,
                        "stream_watchdog: nothing arrived from a sender holding credit \
                         for the detection window, injected Dropped"
                    );
                    break;
                }
            }
        }
    }
    cancel_token.cancel();
}

/// Remove the anchor a watchdog judged dead, and tell its consumer. Returns
/// whether it fired.
///
/// The token is read under the entry's shard lock, the lock
/// [`install_direct_feed`] cancels a replaced feed's token under. A watchdog
/// whose `select!` took the timer arm just before an attach replaced its feed
/// would otherwise remove the new stream's entry and put `Dropped` on its
/// channel.
///
/// Removing the entry closes the slot and tells the sender (`AnchorEntry`'s
/// `Drop`): a sender judged dead sends nothing a delivery could fail on.
fn fire_watchdog(
    cancel_token: &CancellationToken,
    frame_tx: &flume::Sender<Vec<u8>>,
    registry: &dashmap::DashMap<u64, crate::streaming::anchor::AnchorEntry>,
    local_id: u64,
) -> bool {
    let Some((_, entry)) = registry.remove_if(&local_id, |_, _| !cancel_token.is_cancelled())
    else {
        return false;
    };
    let _ = frame_tx.try_send(crate::streaming::sender::cached_dropped().clone());
    entry.cancel_token.cancel();
    true
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
            release: None,
        }
    }

    /// A watchdog retired while it was firing leaves the entry and its channel
    /// alone: both belong to the stream that replaced its feed.
    #[test]
    fn a_watchdog_retired_mid_firing_leaves_the_new_stream_alone() {
        let registry = dashmap::DashMap::new();
        registry.insert(1, entry());
        let (frame_tx, frame_rx) = flume::unbounded();

        let retired = CancellationToken::new();
        retired.cancel();
        assert!(!fire_watchdog(&retired, &frame_tx, &registry, 1));
        assert!(registry.contains_key(&1), "the new stream's entry stays");
        assert!(frame_rx.is_empty(), "and its consumer sees no Dropped");

        assert!(fire_watchdog(
            &CancellationToken::new(),
            &frame_tx,
            &registry,
            1
        ));
        assert!(!registry.contains_key(&1));
        assert_eq!(
            frame_rx.try_recv().expect("Dropped"),
            *crate::streaming::sender::cached_dropped()
        );
    }

    /// A retired feed never reaps; an abandoned one reaps exactly once.
    ///
    /// Releasing a pre-bind on a transport mismatch cancels its pump token and
    /// then drops the bind, which closes it exactly as the accept window does.
    /// The entry that close would reap is the one the winning attach is about
    /// to reuse, so the cancelled token is what must stop the reap -- whichever
    /// of the consumer and the watchdog sees the close.

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
