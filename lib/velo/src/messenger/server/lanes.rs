// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Keyed ordering lanes for the ordered dispatch mode.
//!
//! A [`LaneRouter`] partitions items by key. Each key gets its own unbounded
//! channel and a single consumer task, so items sharing a key are handled
//! strictly in the order they were routed, while distinct keys run in parallel.
//!
//! This module is deliberately generic over the key and item types and knows
//! nothing about [`HandlerContext`](super::dispatcher::HandlerContext),
//! `prometheus`, or the messenger. That keeps the lane-lifecycle races — which
//! are the hard part — unit-testable without a transport or a `Messenger`.
//!
//! # Lane lifecycle
//!
//! Lanes are created on first use and reaped after `idle_ttl` with no work. The
//! reap runs *inside the lane task itself*, so [`LaneState::pending`] has
//! exactly one decrementer and the reaper can never race the consumer.
//!
//! Soundness rests on two invariants:
//!
//! 1. **`DashMap::entry` and `DashMap::remove_if` take the same shard write
//!    lock.** The producer increments `pending` and sends *while holding the
//!    entry guard*, so a producer can never be observed mid-send by the reap
//!    predicate. Either the reaper sees `pending > 0` and declines, or it
//!    removes an entry no producer has touched.
//! 2. **`pending` is decremented after the consumer future resolves**, not when
//!    the item is dequeued. A lane running a long handler over an empty queue
//!    still reports `pending > 0` and is never reaped mid-flight.
//!
//! Together these mean at most one lane task per key can ever hold work: the
//! map holds exactly one sender per key, and a task whose entry was removed
//! provably has an empty queue and can never receive again.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use futures::future::BoxFuture;
use tokio_util::task::TaskTracker;
use tracing::trace;

#[cfg(test)]
tokio::task_local! {
    /// Counts how many times a lane task has armed or re-armed its idle
    /// timer.
    ///
    /// Lane-local rather than shared with `streaming::control::TIMER_ARMS`:
    /// the module doc above claims this module knows nothing about the
    /// streaming crate, and importing that seam to save one counter type
    /// would make the claim false for a test-only reason. Scoping it per
    /// task keeps each test's count its own while the suite runs them in
    /// parallel, which a process-wide counter could not.
    pub(crate) static TIMER_ARMS: std::sync::Arc<std::sync::atomic::AtomicU64>;
}

/// Record one timer arm. Nothing observes it outside a test scope.
#[cfg(test)]
pub(crate) fn note_timer_arm() {
    let _ = TIMER_ARMS.try_with(|arms| arms.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
}

/// Compiles away entirely: the seam must cost the shipped lane task nothing.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn note_timer_arm() {}

#[cfg(test)]
tokio::task_local! {
    /// Counts how many times a lane task's idle timer has actually elapsed.
    ///
    /// Lane-local for the same reason as [`TIMER_ARMS`], and a second counter
    /// rather than a flag on it because the two move in opposite directions
    /// under the receive-arm re-arm below: a lane under traffic pushes its
    /// deadline forward roughly twice per TTL, so its arm count rises, while
    /// its fire count goes to zero.
    pub(crate) static TIMER_FIRES: std::sync::Arc<std::sync::atomic::AtomicU64>;
}

/// Record one timer fire. Nothing observes it outside a test scope.
#[cfg(test)]
pub(crate) fn note_timer_fire() {
    let _ = TIMER_FIRES.try_with(|fires| fires.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
}

/// Compiles away entirely: the seam must cost the shipped lane task nothing.
#[cfg(not(test))]
#[inline(always)]
pub(crate) fn note_timer_fire() {}

/// Hooks fired as lanes are created, reaped, and drained.
///
/// Kept as a trait so this module carries no `prometheus` dependency and tests
/// can install a counting mock.
pub(crate) trait LaneObserver: Send + Sync {
    /// A lane was spawned for a key that had none.
    fn lane_created(&self);
    /// A lane task exited, either reaped for idleness or because the router was
    /// dropped.
    fn lane_closed(&self);
}

/// Per-lane state shared between the producer and the lane task.
struct LaneState {
    /// Items routed to this lane that have not finished being handled.
    ///
    /// Incremented by the producer under the shard write lock; decremented by
    /// the lane task *after* the consumer future resolves. Observing `0` under
    /// that same lock proves the lane is idle and that no producer is mid-send.
    pending: AtomicUsize,
}

struct Lane<T> {
    tx: flume::Sender<T>,
    state: Arc<LaneState>,
}

/// Configuration for a [`LaneRouter`].
pub(crate) struct LaneRouterConfig {
    /// How long a lane may sit idle before it reaps itself. `None` disables
    /// reaping, so lanes live until the router is dropped.
    pub idle_ttl: Option<Duration>,
    /// Runtime used to spawn lane tasks. Passing an explicit handle means
    /// [`LaneRouter::route`] does not require ambient runtime context.
    pub runtime: tokio::runtime::Handle,
    /// Tracker lane tasks are registered with, so a future graceful shutdown
    /// can drain them.
    pub tracker: TaskTracker,
    /// Optional lifecycle hooks.
    pub observer: Option<Arc<dyn LaneObserver>>,
}

type Consumer<T> = Arc<dyn Fn(T) -> BoxFuture<'static, ()> + Send + Sync>;

/// Routes items to per-key ordering lanes.
///
/// Items sharing a key are handed to `consumer` one at a time, in route order.
/// Items with different keys run concurrently.
pub(crate) struct LaneRouter<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    lanes: Arc<DashMap<K, Lane<T>>>,
    consumer: Consumer<T>,
    config: LaneRouterConfig,
}

impl<K, T> LaneRouter<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    /// Create a router that hands each item to `consumer` on its key's lane.
    pub(crate) fn new(consumer: Consumer<T>, config: LaneRouterConfig) -> Self {
        Self {
            lanes: Arc::new(DashMap::new()),
            consumer,
            config,
        }
    }

    /// Enqueue `item` on `key`'s lane, creating and spawning the lane if needed.
    ///
    /// Returns the lane's depth *before* this item, or gives `item` back when
    /// `capacity` is `Some(n)` and the lane already holds `n` unhandled items.
    /// With `capacity: None` this never rejects. Never blocks: lane channels
    /// are unbounded, and `capacity` is an admission check, not a channel
    /// bound.
    pub(crate) fn route(&self, key: K, item: T, capacity: Option<usize>) -> Result<usize, T> {
        // Everything below runs while the shard's write lock is held by the
        // entry guard. That is what makes the reap predicate sound — see the
        // module docs. In particular, do NOT clone the sender out and drop the
        // guard before sending: the clone keeps the channel alive so the send
        // still succeeds, but a concurrent producer may have already built a
        // replacement lane, leaving two lanes holding items for one key with no
        // ordering between them.
        let lane = match self.lanes.entry(key.clone()) {
            Entry::Occupied(occupied) => occupied.into_ref(),
            Entry::Vacant(vacant) => {
                let (tx, rx) = flume::unbounded();
                let state = Arc::new(LaneState {
                    pending: AtomicUsize::new(0),
                });
                self.spawn_lane(key.clone(), rx, Arc::clone(&state));
                vacant.insert(Lane { tx, state })
            }
        };

        // The entry guard serialises every producer for this key, so a plain
        // load/compare is sufficient here — no CAS loop. The lane task may
        // decrement concurrently, which only frees capacity, so a `depth` read
        // here can be stale-high but never stale-low: we can reject a shade
        // early, never admit past `capacity`.
        let depth = lane.state.pending.load(Ordering::Acquire);
        if capacity.is_some_and(|capacity| depth >= capacity) {
            return Err(item);
        }

        lane.state.pending.fetch_add(1, Ordering::AcqRel);
        // Unbounded: `send` only fails if every receiver is gone, which cannot
        // happen while we hold the entry guard that owns the sender.
        let _ = lane.tx.send(item);
        Ok(depth)
    }

    /// Number of live lanes. Test and metrics support.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn lane_count(&self) -> usize {
        self.lanes.len()
    }

    fn spawn_lane(&self, key: K, rx: flume::Receiver<T>, state: Arc<LaneState>) {
        if let Some(observer) = self.config.observer.as_ref() {
            observer.lane_created();
        }

        let task = LaneTask {
            key,
            rx,
            state,
            // Weak, so a live lane task never keeps the router's map alive.
            // Dropping the router drops every sender, and each lane exits on
            // the resulting disconnect.
            lanes: Arc::downgrade(&self.lanes),
            consumer: Arc::clone(&self.consumer),
            idle_ttl: self.config.idle_ttl,
            observer: self.config.observer.clone(),
        };

        // `track_future` + `Handle::spawn` rather than `TaskTracker::spawn`, so
        // `route` works without ambient runtime context.
        let tracked = self.config.tracker.track_future(task.run());
        self.config.runtime.spawn(tracked);
    }
}

struct LaneTask<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    key: K,
    rx: flume::Receiver<T>,
    state: Arc<LaneState>,
    lanes: std::sync::Weak<DashMap<K, Lane<T>>>,
    consumer: Consumer<T>,
    idle_ttl: Option<Duration>,
    observer: Option<Arc<dyn LaneObserver>>,
}

impl<K, T> LaneTask<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    async fn run(self) {
        // Removes this lane's map entry however the task exits — reap, router
        // teardown, or an unforeseen panic in the loop. Without it a dead task
        // could leave its sender in the map, silently black-holing every
        // subsequent item for the key.
        let _guard = LaneExitGuard {
            key: self.key.clone(),
            state: Arc::clone(&self.state),
            lanes: self.lanes.clone(),
            observer: self.observer.clone(),
        };

        let idle_ttl = self.idle_ttl;
        let Some(deadline) = idle_ttl else {
            // No idle timeout: this lane never reaps itself, so it must
            // never touch the time driver. `LaneRouterConfig::runtime` is
            // whatever handle the caller passes in, and `tokio::time::sleep`
            // panics immediately -- not lazily on first poll -- on a handle
            // whose runtime was not built with `enable_time`. The pre-hoist
            // code took this same shortcut (`None => self.rx.recv_async().await`,
            // no timer built at all); the one-timer-for-the-lane rewrite
            // below must keep it, not trade it for a `Sleep` pinned to a
            // value nobody reads.
            loop {
                let Ok(item) = self.rx.recv_async().await else {
                    // Sender dropped: the router was torn down, or this lane
                    // was replaced after a reap. Either way there is no more
                    // work.
                    break;
                };
                (self.consumer)(item).await;
                self.state.pending.fetch_sub(1, Ordering::AcqRel);
            }
            return;
        };

        // One timer for the lane, not one per item. A fresh
        // `tokio::time::timeout` registers a timer entry with the driver on
        // its first poll and deregisters it on drop, both under the driver's
        // lock, so every item on every live lane took two turns of that lock
        // for a deadline that almost never fires. A pinned `Sleep` that is
        // already registered polls without it, and comparing each fire
        // against `last_item` leaves the reap where it was: after `idle_ttl`
        // of real silence.
        //
        // An item pushes that deadline forward only once it is inside half a
        // TTL, so a lane that keeps receiving moves its timer at most twice
        // per TTL and never lets it fire, while a lane that stops still reaps
        // one TTL after its last item: the deadline then sits in
        // `[last_item + ttl/2, last_item + ttl]`, and a fire short of
        // `last_item + ttl` re-arms to exactly that instant rather than
        // reaping. Same shape as the two streaming reader pumps
        // (`streaming::control::reader_pump` carries the timing argument) --
        // but not `biased`, unlike those two: `biased` there fixes a real
        // race, a heartbeat frame and a fired sleep landing on the same poll
        // with the receive needing to win the tie. An item and a fired sleep
        // landing together here is harmless while the router is alive:
        // `route`'s entry guard increments `pending` and sends under the
        // same shard lock `try_reap`'s predicate runs inside (see the module
        // doc), so an item already in the channel implies `pending > 0` and
        // a sleep that wins the coin flip only wastes one wake. The
        // exception is router teardown: `try_reap`'s `upgrade()` failure
        // returns `true` before `pending` is ever read (see its own comment
        // below), so a sleep that wins the tie there can reap a lane with an
        // item still queued, which the un-`biased` select made reachable by
        // a coin flip instead of only by the narrower race the pre-hoist
        // `timeout` left (see `try_reap`).

        // The idle window starts when the handler returns, not when the item
        // was dequeued: the timeout this replaces wrapped `recv_async` alone,
        // so a handler slower than `idle_ttl` never put its own lane a window
        // behind. Stamping this at dequeue instead would let such a handler
        // hand back a lane that is already past its deadline with an empty
        // queue -- reaped the moment it finished work.
        let mut last_item = tokio::time::Instant::now();
        // Hoisted out of the per-item path; see the two reader pumps for the
        // rule.
        let rearm_threshold = deadline / 2;
        let mut armed_until = last_item + deadline;
        let sleep = tokio::time::sleep_until(armed_until);
        tokio::pin!(sleep);
        note_timer_arm();

        loop {
            let received = tokio::select! {
                _ = &mut sleep => {
                    note_timer_fire();
                    // Every path out of this arm re-arms the sleep first: a
                    // fired `Sleep` stays ready until it is reset, so a
                    // `continue` past one would spin the lane task.
                    let idle = last_item.elapsed();
                    if idle < deadline {
                        armed_until = last_item + deadline;
                        sleep.as_mut().reset(armed_until);
                        note_timer_arm();
                        continue;
                    }
                    armed_until = tokio::time::Instant::now() + deadline;
                    sleep.as_mut().reset(armed_until);
                    note_timer_arm();
                    if self.try_reap() {
                        trace!(
                            target: "crate::messenger::lanes",
                            key = ?self.key,
                            "Reaping idle ordering lane"
                        );
                        break;
                    }
                    continue;
                }
                received = self.rx.recv_async() => received,
            };

            let Ok(item) = received else {
                // Sender dropped: the router was torn down, or this lane was
                // replaced after a reap. Either way there is no more work.
                break;
            };

            (self.consumer)(item).await;

            // Decrement only now, so a lane running a long handler over an
            // empty queue is never mistaken for idle.
            self.state.pending.fetch_sub(1, Ordering::AcqRel);
            last_item = tokio::time::Instant::now();
            // Push the deadline out only once it is inside half a TTL, so a
            // busy lane moves its timer at most twice per TTL instead of once
            // per item. Resetting on every item would put back the driver
            // lock pair this loop was rewritten to avoid.
            if armed_until.saturating_duration_since(last_item) < rearm_threshold {
                armed_until = last_item + deadline;
                sleep.as_mut().reset(armed_until);
                note_timer_arm();
            }
        }
    }

    /// Try to remove this lane from the map. Returns `true` when the lane was
    /// removed and the task should exit.
    fn try_reap(&self) -> bool {
        let Some(lanes) = self.lanes.upgrade() else {
            // Router is gone; nothing left to remove it from. This does not
            // check `pending` first, so an item already sent into `self.rx`
            // before the router dropped is not drained here -- unlike the
            // `idle_ttl: None` loop above, which keeps receiving until the
            // channel disconnects. See the caller's comment on why that gap
            // is a live teardown case now, not only a theoretical one.
            return true;
        };

        lanes
            .remove_if(&self.key, |_, lane| {
                // `ptr_eq` guards against removing a successor lane's entry:
                // the exit guard can fire after this lane was already replaced.
                Arc::ptr_eq(&lane.state, &self.state)
                    && lane.state.pending.load(Ordering::Acquire) == 0
            })
            .is_some()
    }
}

struct LaneExitGuard<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    key: K,
    state: Arc<LaneState>,
    lanes: std::sync::Weak<DashMap<K, Lane<T>>>,
    observer: Option<Arc<dyn LaneObserver>>,
}

impl<K, T> Drop for LaneExitGuard<K, T>
where
    K: Eq + Hash + Clone + Debug + Send + Sync + 'static,
    T: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(lanes) = self.lanes.upgrade() {
            // Identity-checked so a lane that already reaped itself — and was
            // then replaced by a fresh lane for the same key — cannot evict its
            // successor. Unlike the idle reap this ignores `pending`: the task
            // is over, so leaving the entry would black-hole anything queued.
            lanes.remove_if(&self.key, |_, lane| Arc::ptr_eq(&lane.state, &self.state));
        }
        if let Some(observer) = self.observer.as_ref() {
            observer.lane_closed();
        }
    }
}

/// Reports the observed lane generation for each item, so tests can assert that
/// no two lanes for one key were ever concurrently live.
#[cfg(test)]
#[derive(Default)]
struct CountingObserver {
    created: AtomicUsize,
    closed: AtomicUsize,
}

#[cfg(test)]
impl LaneObserver for CountingObserver {
    fn lane_created(&self) {
        self.created.fetch_add(1, Ordering::AcqRel);
    }
    fn lane_closed(&self) {
        self.closed.fetch_add(1, Ordering::AcqRel);
    }
}

/// Groups a flat `(key, value)` observation log by key, preserving order.
#[cfg(test)]
fn group_by_key<K: Eq + Hash + Clone, V: Clone>(
    log: &[(K, V)],
) -> std::collections::HashMap<K, Vec<V>> {
    let mut grouped: std::collections::HashMap<K, Vec<V>> = std::collections::HashMap::new();
    for (key, value) in log {
        grouped.entry(key.clone()).or_default().push(value.clone());
    }
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    fn config(idle_ttl: Option<Duration>, observer: Arc<CountingObserver>) -> LaneRouterConfig {
        LaneRouterConfig {
            idle_ttl,
            runtime: tokio::runtime::Handle::current(),
            tracker: TaskTracker::new(),
            observer: Some(observer),
        }
    }

    /// Waits for `predicate` to hold, polling every millisecond. Fails the test
    /// rather than hanging if it never does.
    async fn wait_for(label: &str, mut predicate: impl FnMut() -> bool) {
        let deadline = Duration::from_secs(5);
        timeout(deadline, async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for: {label}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lane_preserves_per_key_order() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let observer = Arc::new(CountingObserver::default());

        let sink = Arc::clone(&log);
        let router: LaneRouter<u64, (u64, u32)> = LaneRouter::new(
            Arc::new(move |(key, seq)| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    // Yield so the runtime has every chance to reorder us.
                    tokio::task::yield_now().await;
                    sink.lock().push((key, seq));
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        const KEYS: u64 = 4;
        const PER_KEY: u32 = 250;
        for seq in 0..PER_KEY {
            for key in 0..KEYS {
                let _ = router.route(key, (key, seq), None);
            }
        }

        wait_for("all items handled", || {
            log.lock().len() == (KEYS as usize) * (PER_KEY as usize)
        })
        .await;

        let grouped = group_by_key(&log.lock().clone());
        for key in 0..KEYS {
            let observed: Vec<u32> = grouped[&key].clone();
            let expected: Vec<u32> = (0..PER_KEY).collect();
            assert_eq!(observed, expected, "key {key} was reordered");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_keys_run_concurrently() {
        // Both consumers wait on the same 2-party barrier. If the two keys
        // shared a lane task this deadlocks, so completion *is* the assertion —
        // no timing heuristics, no flake.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let observer = Arc::new(CountingObserver::default());
        let done = Arc::new(AtomicUsize::new(0));

        let consumer_barrier = Arc::clone(&barrier);
        let consumer_done = Arc::clone(&done);
        let router: LaneRouter<u64, ()> = LaneRouter::new(
            Arc::new(move |()| {
                let barrier = Arc::clone(&consumer_barrier);
                let done = Arc::clone(&consumer_done);
                Box::pin(async move {
                    barrier.wait().await;
                    done.fetch_add(1, Ordering::AcqRel);
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        let _ = router.route(1, (), None);
        let _ = router.route(2, (), None);

        wait_for("both lanes cleared the barrier", || {
            done.load(Ordering::Acquire) == 2
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_lane_is_reaped() {
        let observer = Arc::new(CountingObserver::default());
        let handled = Arc::new(AtomicUsize::new(0));

        let consumer_handled = Arc::clone(&handled);
        let router: LaneRouter<u64, ()> = LaneRouter::new(
            Arc::new(move |()| {
                let handled = Arc::clone(&consumer_handled);
                Box::pin(async move {
                    handled.fetch_add(1, Ordering::AcqRel);
                })
            }),
            config(Some(Duration::from_millis(50)), Arc::clone(&observer)),
        );

        let _ = router.route(7, (), None);
        wait_for("item handled", || handled.load(Ordering::Acquire) == 1).await;
        wait_for("lane reaped", || router.lane_count() == 0).await;
        // The map entry goes first, from inside `try_reap`; `lane_closed`
        // fires from the exit guard once the task has actually returned.
        // Asserting on the observer straight after the map read races that
        // gap and fails with `closed` still at 0.
        wait_for("lane close observed", || {
            observer.closed.load(Ordering::Acquire) == 1
        })
        .await;

        assert_eq!(observer.created.load(Ordering::Acquire), 1);
        assert_eq!(observer.closed.load(Ordering::Acquire), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lane_is_not_reaped_while_items_keep_arriving() {
        // The sharp edge of the hoisted idle timer: it is armed once and
        // re-armed only when it fires, so a fire that lands mid-stream must
        // compare against the last item rather than reap on the spot. Unlike
        // `no_reap_while_pending` this lane is genuinely empty and idle each
        // time the timer could fire -- what keeps it alive is that it was fed
        // inside its TTL, not that it had a backlog.
        let observer = Arc::new(CountingObserver::default());
        let handled = Arc::new(AtomicUsize::new(0));

        let consumer_handled = Arc::clone(&handled);
        let router: LaneRouter<u64, ()> = LaneRouter::new(
            Arc::new(move |()| {
                let handled = Arc::clone(&consumer_handled);
                Box::pin(async move {
                    handled.fetch_add(1, Ordering::AcqRel);
                })
            }),
            config(Some(Duration::from_millis(100)), Arc::clone(&observer)),
        );

        // Forty items 5 ms apart is a 20x margin against the 100 ms TTL --
        // wide enough that a single scheduling stall on a loaded runner does
        // not reap the lane and flip the assertions below (a real-clock test
        // with no invariant to fall back on, unlike `no_reap_while_pending`'s
        // `pending > 0` or `no_reap_while_handler_in_flight`'s handler still
        // running). At that rate the receive-arm re-arm keeps the deadline
        // half a TTL ahead and the timer never fires, so what this asserts is
        // the outcome -- a lane fed inside its TTL stays live -- for whichever
        // of the two arms ends up moving the timer on a loaded runner.
        const ITEMS: usize = 40;
        for handled_so_far in 1..=ITEMS {
            let _ = router.route(9, (), None);
            wait_for("item handled", || {
                handled.load(Ordering::Acquire) == handled_so_far
            })
            .await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(
            router.lane_count(),
            1,
            "a lane fed every 5 ms must still be live under a 100 ms TTL"
        );
        assert_eq!(
            observer.created.load(Ordering::Acquire),
            1,
            "the lane must never have been reaped and recreated"
        );
        assert_eq!(
            observer.closed.load(Ordering::Acquire),
            0,
            "no lane may have closed while items kept arriving"
        );
    }

    /// A behavioural test cannot see the difference between one timer per
    /// lane and one per item -- the same items come out either way -- so
    /// this counts the arms directly, the same way
    /// `a_thousand_records_arm_the_heartbeat_timer_a_handful_of_times` pins
    /// it for `streaming::control::reader_pump`. The hour-long TTL keeps the
    /// timer from ever firing here;
    /// `a_lane_under_traffic_never_fires_its_idle_timer_and_reaps_once_it_stops`
    /// is what pins the firing case. Built from a bare
    /// [`LaneTask`] rather than through [`LaneRouter`]: the task-local scope
    /// below has to wrap the exact future that gets spawned, and
    /// `LaneRouter::route` spawns that future itself, several calls removed
    /// from any scope this test could establish around it.
    #[tokio::test]
    async fn lane_arms_its_idle_timer_a_handful_of_times_not_once_per_item() {
        tokio::time::pause();

        const ITEMS: usize = 1_000;
        // The arm before the loop is the whole steady state here: the
        // deadline is an hour and the test never advances the clock that
        // far, so the timer has no reason to fire. The headroom covers the
        // paused clock auto-advancing to its own nearest pending timer (this
        // test's `wait_for` polls, always far nearer than the lane's) if the
        // runtime does go idle between items. What the bound asserts is that
        // it does not scale with `ITEMS`.
        const MAX_ARMS: u64 = 4;

        let (tx, rx) = flume::unbounded::<()>();
        let state = Arc::new(LaneState {
            pending: AtomicUsize::new(0),
        });
        let handled = Arc::new(AtomicUsize::new(0));
        let consumer_handled = Arc::clone(&handled);
        let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // No router (a dangling `Weak`), so if the hour-long `idle_ttl`
        // below ever did fire, `try_reap`'s `upgrade()` would fail and it
        // would answer `true` unconditionally -- reaping the lane and
        // ending the test on a confusing `wait_for` timeout instead of on
        // its arm-count assertion. What actually keeps that from happening
        // is the hour itself: the test never advances the clock anywhere
        // near it, so the timer has no occasion to fire.
        let task: LaneTask<u64, ()> = LaneTask {
            key: 1,
            rx,
            state: Arc::clone(&state),
            lanes: std::sync::Weak::new(),
            consumer: Arc::new(move |()| {
                let handled = Arc::clone(&consumer_handled);
                Box::pin(async move {
                    handled.fetch_add(1, Ordering::AcqRel);
                })
            }),
            idle_ttl: Some(Duration::from_secs(3600)),
            observer: None,
        };

        tokio::spawn(TIMER_ARMS.scope(Arc::clone(&arms), task.run()));

        for i in 0..ITEMS {
            // Mirrors what `LaneRouter::route` does under its entry guard.
            state.pending.fetch_add(1, Ordering::AcqRel);
            tx.send(()).expect("lane task must still be receiving");
            wait_for("item handled", || handled.load(Ordering::Acquire) == i + 1).await;
        }

        let armed = arms.load(std::sync::atomic::Ordering::Relaxed);
        // Without this, deleting the `note_timer_arm()` call sites leaves
        // `armed` at 0 and the bound below still passes -- an upper bound
        // alone does not prove the seam is wired to anything. The pre-loop
        // arm is unconditional and this point is reached only after an item
        // has been handled, so `>= 1` is exact and cannot flake.
        assert!(
            armed >= 1,
            "the pre-loop arm must have counted at least once"
        );
        assert!(
            armed <= MAX_ARMS,
            "the lane task must arm its idle timer a bounded number of times, \
             not once per item: {ITEMS} items armed it {armed} times (bound {MAX_ARMS})"
        );
    }

    /// The lane half of `streaming::control`'s
    /// `a_stream_under_traffic_never_fires_its_heartbeat_timer`, which carries
    /// the derivation of both bounds: a lane that keeps receiving items must
    /// not let its idle timer fire, and a lane that stops must still reap one
    /// TTL after its last item.
    ///
    /// Built from a bare [`LaneTask`] rather than through [`LaneRouter`] for
    /// the reason `lane_arms_its_idle_timer_a_handful_of_times_not_once_per_item`
    /// gives: the task-local scopes have to wrap the exact future that gets
    /// spawned, and `LaneRouter::route` spawns that future itself. The map it
    /// holds is real rather than a dangling `Weak`, so the reap below goes
    /// through `try_reap`'s actual predicate -- this lane's own entry, with
    /// `pending == 0` -- and not through its router-is-gone shortcut.
    ///
    /// The consumer signals each item on a channel instead of the `wait_for`
    /// poll the router tests use: a poll loop sleeps, and under
    /// `tokio::time::pause` a sleeping test task with a parked lane task lets
    /// the runtime auto-advance to the lane's own timer -- firing the very
    /// timer this test says must not fire.
    #[tokio::test]
    async fn a_lane_under_traffic_never_fires_its_idle_timer_and_reaps_once_it_stops() {
        tokio::time::pause();

        const TTL_MS: u64 = 100;
        const STEP_MS: u64 = 5;
        const TRAFFIC_MS: u64 = 500;
        const ITEMS: usize = (TRAFFIC_MS / STEP_MS) as usize + 1;
        const MAX_ARMS: u64 = 1 + TRAFFIC_MS / (TTL_MS / 2) + 1;

        let ttl = Duration::from_millis(TTL_MS);
        let step = Duration::from_millis(STEP_MS);

        let (tx, rx) = flume::unbounded::<()>();
        let (done_tx, done_rx) = flume::unbounded::<()>();
        let state = Arc::new(LaneState {
            pending: AtomicUsize::new(0),
        });
        let lanes: Arc<DashMap<u64, Lane<()>>> = Arc::new(DashMap::new());
        lanes.insert(
            1,
            Lane {
                tx: tx.clone(),
                state: Arc::clone(&state),
            },
        );
        let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let fires = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let task: LaneTask<u64, ()> = LaneTask {
            key: 1,
            rx,
            state: Arc::clone(&state),
            lanes: Arc::downgrade(&lanes),
            consumer: Arc::new(move |()| {
                let done = done_tx.clone();
                Box::pin(async move {
                    let _ = done.send(());
                })
            }),
            idle_ttl: Some(ttl),
            observer: None,
        };

        let lane = tokio::spawn(TIMER_FIRES.scope(
            Arc::clone(&fires),
            TIMER_ARMS.scope(Arc::clone(&arms), task.run()),
        ));

        for i in 0..ITEMS {
            // Advance between items, never after the last one, so the clock
            // still reads the last item's instant when the loop ends.
            if i > 0 {
                tokio::time::advance(step).await;
            }
            // Mirrors what `LaneRouter::route` does under its entry guard.
            state.pending.fetch_add(1, Ordering::AcqRel);
            tx.send(()).expect("the lane task must still be receiving");
            done_rx
                .recv_async()
                .await
                .unwrap_or_else(|_| panic!("the lane task must handle item {i}"));
        }

        let fired = fires.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            fired, 0,
            "a lane task must not fire its timer under traffic: {ITEMS} items \
             {STEP_MS} ms apart under a {TTL_MS} ms TTL fired it {fired} times"
        );

        let armed = arms.load(std::sync::atomic::Ordering::Relaxed);
        // Without this, deleting every `note_timer_arm()` call site leaves
        // `armed` at 0 and the bound below still passes.
        assert!(
            armed >= 1,
            "the pre-loop arm must have counted at least once"
        );
        assert!(
            armed <= MAX_ARMS,
            "the lane task must re-arm its idle timer at a bounded rate, not once \
             per item: {ITEMS} items armed it {armed} times (bound {MAX_ARMS})"
        );

        // The lane stamps `last_item` right after the handler this loop just
        // drained, and no advance separates the two, so the paused clock reads
        // that same instant here.
        let last_item = tokio::time::Instant::now();

        // Bounded rather than a bare await: a lane whose timer never fired
        // would park this test on a paused clock with nothing left to advance
        // to, hanging the runner instead of going red.
        tokio::time::timeout(ttl * 10, lane)
            .await
            .expect("a lane left idle must reap itself")
            .expect("the lane task must not panic");

        let reaped_after = last_item.elapsed();
        assert!(
            reaped_after >= ttl && reaped_after < ttl * 2,
            "a lane that stops must reap one TTL after its last item, no sooner: \
             reaped after {reaped_after:?} under a {ttl:?} TTL"
        );
        assert!(
            fires.load(std::sync::atomic::Ordering::Relaxed) > fired,
            "the reap must have come from a timer fire, not from anything else"
        );
        assert_eq!(lanes.len(), 0, "a reaped lane must leave no map entry");
    }

    /// `idle_ttl: None` disables reaping, and the pre-hoist code never called
    /// into the time driver on that path (`None => self.rx.recv_async().await`,
    /// no timer of any kind). A `LaneRouter` is built with whatever
    /// `tokio::runtime::Handle` its caller passes in `LaneRouterConfig`; a
    /// runtime without `enable_time` panics the instant anything calls
    /// `tokio::time::sleep`, so a lane task must not construct one on this
    /// path either. This does not use `#[tokio::test]`, which enables time by
    /// default and so could not tell a hoisted `sleep(...)` call apart from
    /// its absence -- it builds the bare runtime by hand instead.
    #[test]
    fn lane_task_with_no_idle_ttl_never_touches_the_time_driver() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime with no drivers enabled must still build");

        let (tx, rx) = flume::unbounded::<()>();
        let state = Arc::new(LaneState {
            pending: AtomicUsize::new(0),
        });
        let handled = Arc::new(AtomicUsize::new(0));
        let consumer_handled = Arc::clone(&handled);

        let task: LaneTask<u64, ()> = LaneTask {
            key: 1,
            rx,
            state: Arc::clone(&state),
            lanes: std::sync::Weak::new(),
            consumer: Arc::new(move |()| {
                let handled = Arc::clone(&consumer_handled);
                Box::pin(async move {
                    handled.fetch_add(1, Ordering::AcqRel);
                })
            }),
            idle_ttl: None,
            observer: None,
        };

        let join = rt.spawn(task.run());
        tx.send(()).expect("lane task must still be receiving");

        rt.block_on(async {
            // No timer is available to bound this wait with a deadline, so
            // this polls a fixed number of scheduler turns instead. Each
            // `yield_now` touches only the task queue, never the time driver.
            for _ in 0..10_000 {
                if handled.load(Ordering::Acquire) == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                handled.load(Ordering::Acquire),
                1,
                "lane task with idle_ttl: None never handled the routed item \
                 (a panic constructing its timer would look like this: the \
                 task dies before the consumer ever runs)"
            );

            drop(tx);
            for _ in 0..10_000 {
                if join.is_finished() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let result = join.await;
            assert!(
                result.is_ok(),
                "lane task with idle_ttl: None panicked instead of running: {result:?}"
            );
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_reap_while_pending() {
        // A backlog must keep the lane alive even though each individual
        // handler takes far longer than the idle TTL.
        let observer = Arc::new(CountingObserver::default());
        let log = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&log);
        let router: LaneRouter<u64, u32> = LaneRouter::new(
            Arc::new(move |seq| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    sink.lock().push(seq);
                })
            }),
            config(Some(Duration::from_millis(1)), Arc::clone(&observer)),
        );

        for seq in 0..5 {
            let _ = router.route(1, seq, None);
        }

        wait_for("all items handled", || log.lock().len() == 5).await;
        assert_eq!(*log.lock(), vec![0, 1, 2, 3, 4]);
        assert_eq!(
            observer.created.load(Ordering::Acquire),
            1,
            "the lane must not be reaped and recreated while work is queued"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_reap_while_handler_in_flight() {
        // The sharp edge of the previous test: a single item whose handler runs
        // far longer than the idle TTL, with an empty queue behind it. This is
        // what forces `pending` to be decremented *after* the future resolves
        // rather than at dequeue.
        let observer = Arc::new(CountingObserver::default());
        let finished = Arc::new(AtomicUsize::new(0));

        let consumer_finished = Arc::clone(&finished);
        let router: LaneRouter<u64, ()> = LaneRouter::new(
            Arc::new(move |()| {
                let finished = Arc::clone(&consumer_finished);
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    finished.fetch_add(1, Ordering::AcqRel);
                })
            }),
            config(Some(Duration::from_millis(5)), Arc::clone(&observer)),
        );

        let _ = router.route(1, (), None);

        // Well past the idle TTL but well before the handler finishes.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            router.lane_count(),
            1,
            "a lane with a handler in flight must not be reaped"
        );
        assert_eq!(finished.load(Ordering::Acquire), 0);

        wait_for("handler finished", || finished.load(Ordering::Acquire) == 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reap_then_reuse_preserves_order() {
        let observer = Arc::new(CountingObserver::default());
        let log = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&log);
        let router: LaneRouter<u64, u32> = LaneRouter::new(
            Arc::new(move |seq| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    sink.lock().push(seq);
                })
            }),
            config(Some(Duration::from_millis(30)), Arc::clone(&observer)),
        );

        for seq in 0..5 {
            let _ = router.route(1, seq, None);
        }
        wait_for("first burst handled", || log.lock().len() == 5).await;
        wait_for("lane reaped", || router.lane_count() == 0).await;

        for seq in 5..10 {
            let _ = router.route(1, seq, None);
        }
        wait_for("second burst handled", || log.lock().len() == 10).await;

        assert_eq!(*log.lock(), (0..10).collect::<Vec<_>>());
        assert_eq!(
            observer.created.load(Ordering::Acquire),
            2,
            "the reaped lane should have been rebuilt exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_producers_single_lane() {
        // Hammer one key from many tasks while reaping aggressively. Every item
        // must be handled exactly once, and — because at most one lane per key
        // may hold work — the sequence of lane generations observed by the
        // consumer must be non-decreasing.
        let observer = Arc::new(CountingObserver::default());
        let log = Arc::new(Mutex::new(Vec::new()));
        let generation = Arc::new(AtomicUsize::new(0));

        let sink = Arc::clone(&log);
        let consumer_generation = Arc::clone(&generation);
        let router: Arc<LaneRouter<u64, u32>> = Arc::new(LaneRouter::new(
            Arc::new(move |item| {
                let sink = Arc::clone(&sink);
                // `created` at handling time identifies which lane generation
                // is currently running.
                let lane_gen = consumer_generation.load(Ordering::Acquire);
                Box::pin(async move {
                    sink.lock().push((lane_gen, item));
                })
            }),
            config(Some(Duration::from_millis(1)), Arc::clone(&observer)),
        ));

        // Keep `generation` in step with lane creations.
        let generation_tracker = Arc::clone(&generation);
        let observer_for_tracker = Arc::clone(&observer);
        tokio::spawn(async move {
            loop {
                generation_tracker.store(
                    observer_for_tracker.created.load(Ordering::Acquire),
                    Ordering::Release,
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        const PRODUCERS: u32 = 8;
        const PER_PRODUCER: u32 = 200;
        let mut handles = Vec::new();
        for producer in 0..PRODUCERS {
            let router = Arc::clone(&router);
            handles.push(tokio::spawn(async move {
                for i in 0..PER_PRODUCER {
                    let _ = router.route(1, producer * PER_PRODUCER + i, None);
                    if i % 32 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.expect("producer task");
        }

        wait_for("all items handled", || {
            log.lock().len() == (PRODUCERS * PER_PRODUCER) as usize
        })
        .await;

        let observed = log.lock().clone();
        let generations: Vec<usize> = observed.iter().map(|(lane_gen, _)| *lane_gen).collect();
        assert!(
            generations.windows(2).all(|w| w[0] <= w[1]),
            "two lanes for one key were live at the same time"
        );

        let mut items: Vec<u32> = observed.iter().map(|(_, item)| *item).collect();
        items.sort_unstable();
        assert_eq!(
            items,
            (0..PRODUCERS * PER_PRODUCER).collect::<Vec<_>>(),
            "every item must be handled exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_router_stops_lanes() {
        let observer = Arc::new(CountingObserver::default());
        let handled = Arc::new(AtomicUsize::new(0));

        let consumer_handled = Arc::clone(&handled);
        let router: LaneRouter<u64, ()> = LaneRouter::new(
            Arc::new(move |()| {
                let handled = Arc::clone(&consumer_handled);
                Box::pin(async move {
                    handled.fetch_add(1, Ordering::AcqRel);
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        for key in 0..3 {
            let _ = router.route(key, (), None);
        }
        wait_for("items handled", || handled.load(Ordering::Acquire) == 3).await;
        assert_eq!(observer.created.load(Ordering::Acquire), 3);

        drop(router);

        wait_for("all lanes exited", || {
            observer.closed.load(Ordering::Acquire) == 3
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_is_enforced_per_key() {
        // Admission is per lane, not per router: filling one key's lane must
        // not affect any other key's.
        let observer = Arc::new(CountingObserver::default());
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handled = Arc::new(Mutex::new(Vec::new()));

        let consumer_release = Arc::clone(&release);
        let sink = Arc::clone(&handled);
        let router: LaneRouter<u64, u32> = LaneRouter::new(
            Arc::new(move |item| {
                let release = Arc::clone(&consumer_release);
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    while !release.load(Ordering::Acquire) {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    sink.lock().push(item);
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        // Key 1: one item enters the consumer and wedges, two more fill the
        // queue to the cap, the fourth is refused.
        assert_eq!(router.route(1, 10, Some(3)), Ok(0));
        assert_eq!(router.route(1, 11, Some(3)), Ok(1));
        assert_eq!(router.route(1, 12, Some(3)), Ok(2));
        assert_eq!(
            router.route(1, 13, Some(3)),
            Err(13),
            "a full lane must hand the item back"
        );

        // Key 2 is untouched by key 1's backlog.
        assert_eq!(router.route(2, 20, Some(3)), Ok(0));

        // `capacity: None` never refuses, even on the saturated lane.
        assert_eq!(router.route(1, 14, None), Ok(3));

        release.store(true, Ordering::Release);
        wait_for("all admitted items handled", || handled.lock().len() == 5).await;

        let grouped = group_by_key(
            &handled
                .lock()
                .iter()
                .map(|item| (item / 10, *item))
                .collect::<Vec<_>>(),
        );
        assert_eq!(grouped[&1], vec![10, 11, 12, 14], "key 1 lost or reordered");
        assert_eq!(grouped[&2], vec![20]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capacity_frees_as_the_lane_drains() {
        // The cap bounds *unhandled* items, so it must recover once the
        // consumer works through the backlog rather than latching shut.
        let observer = Arc::new(CountingObserver::default());
        let handled = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&handled);
        let router: LaneRouter<u64, u32> = LaneRouter::new(
            Arc::new(move |item| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    sink.lock().push(item);
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        assert!(router.route(1, 0, Some(1)).is_ok());
        wait_for("first item handled", || handled.lock().len() == 1).await;
        assert!(
            router.route(1, 1, Some(1)).is_ok(),
            "capacity must be reusable once the lane drains"
        );
        wait_for("second item handled", || handled.lock().len() == 2).await;
        assert_eq!(*handled.lock(), vec![0, 1]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lane_survives_consumer_panic() {
        // The router itself does not catch panics — `OrderedDispatcher` wraps
        // the handler in `catch_unwind` before handing it over. This test pins
        // the contract that a consumer which handles its own panics keeps the
        // lane alive and ordered.
        let observer = Arc::new(CountingObserver::default());
        let log = Arc::new(Mutex::new(Vec::new()));

        let sink = Arc::clone(&log);
        let router: LaneRouter<u64, u32> = LaneRouter::new(
            Arc::new(move |seq| {
                let sink = Arc::clone(&sink);
                Box::pin(async move {
                    let result =
                        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
                            assert_ne!(seq, 3, "deliberate panic");
                            seq
                        }))
                        .await;
                    if let Ok(seq) = result {
                        sink.lock().push(seq);
                    }
                })
            }),
            config(None, Arc::clone(&observer)),
        );

        for seq in 0..8 {
            let _ = router.route(1, seq, None);
        }

        wait_for("surviving items handled", || log.lock().len() == 7).await;
        assert_eq!(*log.lock(), vec![0, 1, 2, 4, 5, 6, 7]);
        assert_eq!(
            observer.created.load(Ordering::Acquire),
            1,
            "the lane must survive a panicking item"
        );
    }
}
