// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Sender heartbeats, sent by one task per manager.
//!
//! A remote stream's receiver ends the stream when nothing arrives for
//! `DETECTION_MULTIPLIER` heartbeat intervals, so an idle sender must beat.
//! Each sender used to spawn a task that beat on its own `interval` for as
//! long as the stream was attached. Now a sender registers its channel here,
//! and one [`DeadlineSweep`] beats every registered channel when its time
//! comes. A same-worker sender does not register: nothing on the anchor
//! side reads its beats.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use crate::streaming::control::SenderClosed;
use crate::streaming::deadline::deadline_after;
use crate::streaming::sweep::{DeadlineSweep, SweepOwner, SweepTarget};

/// What one registered sender needs for a beat.
struct Beat {
    /// The sender's frame channel. Held only while the sender lives: its
    /// [`BeatGuard`] removes the entry when the sender finishes, so the
    /// channel closes when the sender's own handle drops, not a beat later.
    sink: flume::Sender<Vec<u8>>,
    interval: Duration,
    /// When the next beat is due. The queue entry only says when to look.
    next: Instant,
    /// Set when the stream is cancelled. A cancelled sender beats no more.
    closed: SenderClosed,
}

/// The registered senders of one manager.
#[derive(Default)]
pub(crate) struct Heartbeats {
    beats: DashMap<u64, Beat>,
    next_key: AtomicU64,
}

impl SweepTarget for Heartbeats {
    /// A sender that finishes within its first second leaves its entry for at
    /// most this long, so the queue holds about sender rate x 1 s of stale
    /// entries. A live sender costs one extra look in its first interval.
    const FIRST_LOOK: Duration = Duration::from_secs(1);
    /// A requeued entry is queued at the sender's next beat, which is never
    /// more than one interval ahead.
    const RECHECK: Duration = Duration::MAX;

    fn on_due(&self, due: Vec<u64>, now: Instant, renewed: &mut Vec<(Instant, u64)>) {
        for key in due {
            let Some(mut beat) = self.beats.get_mut(&key) else {
                continue; // the sender finished
            };
            if beat.closed.is_closed() {
                drop(beat);
                self.beats.remove(&key);
                continue;
            }
            if beat.next > now {
                renewed.push((beat.next, key));
                continue;
            }
            // A full channel drops the beat rather than stall the sweep. A
            // channel full of data needs no beat.
            let _ = beat
                .sink
                .try_send(crate::streaming::sender::cached_heartbeat().clone());
            beat.next = now + beat.interval;
            renewed.push((beat.next, key));
        }
    }
}

/// What a sender holds to register for heartbeats. Cheap to clone.
#[derive(Clone)]
pub(crate) struct HeartbeatHandle {
    beats: Arc<Heartbeats>,
    sweep: Arc<DeadlineSweep<Heartbeats>>,
}

impl HeartbeatHandle {
    /// Beat into `sink` every `interval`, from one interval after now, until
    /// the returned guard drops or `closed` is set.
    pub(crate) fn register(
        &self,
        sink: flume::Sender<Vec<u8>>,
        interval: Duration,
        closed: SenderClosed,
    ) -> BeatGuard {
        let key = self.beats.next_key.fetch_add(1, Ordering::Relaxed);
        let next = deadline_after(interval);
        self.beats.beats.insert(
            key,
            Beat {
                sink,
                interval,
                next,
                closed,
            },
        );
        self.sweep.push(&self.beats, next, key);
        BeatGuard {
            beats: Arc::clone(&self.beats),
            key,
        }
    }

    /// How many senders are registered.
    #[cfg(test)]
    pub(crate) fn registered(&self) -> usize {
        self.beats.beats.len()
    }
}

/// Stops a sender's heartbeats when dropped. Held by the sender, and dropped
/// when it finalizes, detaches, or drops.
pub(crate) struct BeatGuard {
    beats: Arc<Heartbeats>,
    key: u64,
}

impl Drop for BeatGuard {
    fn drop(&mut self) {
        self.beats.beats.remove(&self.key);
    }
}

/// The manager's heartbeats. Stops the sweep when the manager drops.
#[derive(Default)]
pub(crate) struct ManagerHeartbeats {
    beats: Arc<Heartbeats>,
    sweep: SweepOwner<Heartbeats>,
}

impl ManagerHeartbeats {
    pub(crate) fn handle(&self) -> HeartbeatHandle {
        HeartbeatHandle {
            beats: Arc::clone(&self.beats),
            sweep: Arc::clone(&self.sweep),
        }
    }

    /// End the sweep. Registered senders beat no more.
    pub(crate) fn stop(&self) {
        self.sweep.stop();
    }

    /// Sweep on `runtime`: the messenger's, which carries the beats.
    pub(crate) fn pin_runtime(&self, runtime: tokio::runtime::Handle) {
        self.sweep.pin_runtime(runtime);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(5);

    fn beats_in(rx: &flume::Receiver<Vec<u8>>) -> usize {
        rx.drain()
            .filter(|frame| frame == crate::streaming::sender::cached_heartbeat())
            .count()
    }

    /// Senders share one heartbeat task, however many there are.
    ///
    /// Each sender used to spawn a task that lived for the whole stream. This
    /// counts tasks, so it stays as the regression guard for that cost.
    #[tokio::test(start_paused = true)]
    async fn senders_share_one_heartbeat_task() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        let metrics = tokio::runtime::Handle::current().metrics();
        let before = metrics.num_alive_tasks();
        let senders: Vec<_> = (0..100)
            .map(|_| {
                let (tx, rx) = flume::bounded(16);
                let guard = handle.register(tx, INTERVAL, SenderClosed::default());
                (rx, guard)
            })
            .collect();
        assert_eq!(
            metrics.num_alive_tasks(),
            before + 1,
            "one task for 100 senders"
        );
        tokio::time::sleep(INTERVAL + Duration::from_millis(10)).await;
        for (rx, _guard) in &senders {
            assert_eq!(beats_in(rx), 1, "each sender beats once per interval");
        }
        tokio::time::sleep(INTERVAL).await;
        for (rx, _guard) in &senders {
            assert_eq!(beats_in(rx), 1);
        }
    }

    /// A finished sender's channel closes when the sender drops, not when
    /// its next beat comes due.
    ///
    /// The beat entry holds a clone of the channel. If the entry stayed until
    /// the sweep found it, a sender that dropped without a terminal would
    /// hold its stream's channel open for up to one interval.
    #[tokio::test(start_paused = true)]
    async fn a_finished_sender_releases_its_channel_at_once() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        let (tx, rx) = flume::bounded::<Vec<u8>>(16);
        let guard = handle.register(tx.clone(), INTERVAL, SenderClosed::default());
        drop(tx);
        assert!(!rx.is_disconnected());
        drop(guard);
        assert!(rx.is_disconnected(), "no beat entry holds the channel");
        assert_eq!(handle.registered(), 0);
    }

    /// A cancelled stream beats no more, and its entry leaves.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_sender_beats_no_more() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        let (tx, rx) = flume::bounded::<Vec<u8>>(16);
        let closed = SenderClosed::default();
        let _guard = handle.register(tx, INTERVAL, closed.clone());
        tokio::time::sleep(INTERVAL + Duration::from_millis(10)).await;
        assert_eq!(beats_in(&rx), 1);
        closed.close();
        tokio::time::sleep(INTERVAL * 3).await;
        assert_eq!(beats_in(&rx), 0);
        assert_eq!(handle.registered(), 0);
    }

    /// Senders keep beating after the runtime that first swept them is gone.
    ///
    /// The entries live in the manager, not in the task. A new sweep, started
    /// by the next registration on a live runtime, beats them all.
    #[test]
    fn beats_outlive_the_runtime_that_first_swept_them() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        let (early_tx, early_rx) = flume::bounded::<Vec<u8>>(16);
        let setup = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _early = setup.block_on(async {
            handle.register(
                early_tx,
                Duration::from_millis(100),
                SenderClosed::default(),
            )
        });
        drop(setup);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(async {
            let (late_tx, late_rx) = flume::bounded::<Vec<u8>>(16);
            let _late =
                handle.register(late_tx, Duration::from_millis(100), SenderClosed::default());
            tokio::time::sleep(Duration::from_millis(1050)).await;
            assert!(
                beats_in(&late_rx) >= 1,
                "the new sweep beats the new sender"
            );
            assert!(
                beats_in(&early_rx) >= 1,
                "and the sender of the dropped runtime"
            );
        });
    }

    /// A pinned sweep keeps beating every sender when a runtime that
    /// registered one is gone, with no later registration.
    ///
    /// Unpinned, the sweep ran on the runtime of the first registration. A
    /// sender registered from another runtime while that sweep ran did not
    /// start one of its own. When the first runtime dropped the sweep,
    /// nothing beat that sender until some later registration, and its
    /// remote stream was reaped. A task per sender ran on its own runtime
    /// and kept beating.
    #[test]
    fn a_pinned_sweep_outlives_the_runtime_that_started_it() {
        let main = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let heartbeats = ManagerHeartbeats::default();
        heartbeats.pin_runtime(main.handle().clone());
        let handle = heartbeats.handle();
        let interval = Duration::from_millis(20);
        let first = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (early_tx, early_rx) = flume::bounded::<Vec<u8>>(64);
        let _early =
            first.block_on(async { handle.register(early_tx, interval, SenderClosed::default()) });
        let (late_tx, late_rx) = flume::bounded::<Vec<u8>>(64);
        let _late =
            main.block_on(async { handle.register(late_tx, interval, SenderClosed::default()) });
        drop(first);
        std::thread::sleep(Duration::from_millis(300));
        assert!(beats_in(&early_rx) >= 1, "the early sender still beats");
        assert!(beats_in(&late_rx) >= 1, "the late sender still beats");
    }

    /// The entries of senders that finished leave the queue within the first
    /// look.
    #[tokio::test(start_paused = true)]
    async fn finished_senders_leave_the_queue_within_the_first_look() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        for _ in 0..1000 {
            let (tx, _rx) = flume::bounded::<Vec<u8>>(1);
            drop(handle.register(tx, INTERVAL, SenderClosed::default()));
        }
        assert_eq!(handle.sweep.len(), 1000);
        tokio::time::sleep(Heartbeats::FIRST_LOOK + Duration::from_millis(10)).await;
        assert_eq!(handle.sweep.len(), 0);
    }

    /// Stop ends the sweep, as manager shutdown does.
    #[tokio::test(start_paused = true)]
    async fn stop_ends_the_sweep() {
        let heartbeats = ManagerHeartbeats::default();
        let handle = heartbeats.handle();
        let (tx, _rx) = flume::bounded::<Vec<u8>>(16);
        let _guard = handle.register(tx, INTERVAL, SenderClosed::default());
        tokio::task::yield_now().await;
        assert!(handle.sweep.sweep_running());
        heartbeats.stop();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!handle.sweep.sweep_running());
    }
}
