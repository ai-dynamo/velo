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
    /// Never blocks and never fails: lane channels are unbounded.
    pub(crate) fn route(&self, key: K, item: T) {
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

        lane.state.pending.fetch_add(1, Ordering::AcqRel);
        // Unbounded: `send` only fails if every receiver is gone, which cannot
        // happen while we hold the entry guard that owns the sender.
        let _ = lane.tx.send(item);
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

        loop {
            let received = match self.idle_ttl {
                Some(ttl) => match tokio::time::timeout(ttl, self.rx.recv_async()).await {
                    Ok(received) => received,
                    Err(_elapsed) => {
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
                },
                None => self.rx.recv_async().await,
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
        }
    }

    /// Try to remove this lane from the map. Returns `true` when the lane was
    /// removed and the task should exit.
    fn try_reap(&self) -> bool {
        let Some(lanes) = self.lanes.upgrade() else {
            // Router is gone; nothing to remove and nothing left to receive.
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
                router.route(key, (key, seq));
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

        router.route(1, ());
        router.route(2, ());

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

        router.route(7, ());
        wait_for("item handled", || handled.load(Ordering::Acquire) == 1).await;
        wait_for("lane reaped", || router.lane_count() == 0).await;

        assert_eq!(observer.created.load(Ordering::Acquire), 1);
        assert_eq!(observer.closed.load(Ordering::Acquire), 1);
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
            router.route(1, seq);
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

        router.route(1, ());

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
            router.route(1, seq);
        }
        wait_for("first burst handled", || log.lock().len() == 5).await;
        wait_for("lane reaped", || router.lane_count() == 0).await;

        for seq in 5..10 {
            router.route(1, seq);
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
                    router.route(1, producer * PER_PRODUCER + i);
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
            router.route(key, ());
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
            router.route(1, seq);
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
