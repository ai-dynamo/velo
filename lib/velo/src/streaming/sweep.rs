// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! One task that sweeps a sharded deadline queue, in place of one task per
//! stream.
//!
//! The anchor unattached timeout and the sender heartbeat both use it. A
//! stream pushes a key and a time; the sweep wakes at the earliest time and
//! hands the due keys to the [`SweepTarget`], which keeps the state that
//! counts. The queue holds only when to look again.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::streaming::deadline::{DeadlineQueue, requeue_at};

/// How many independently locked parts the queue has. A key's entries always
/// go to the part that the key selects.
///
/// Every push takes a lock on the queue, and an anchor create pushes. With
/// one lock, creates on different worker threads waited on each other: in
/// `anchor_setup_bench` at 8 threads, the p99.9 of a create-attach-finish
/// cycle was 110 us, against 25 us with a task per anchor. With 1 or 2
/// threads, the single lock was faster than the task at every percentile.
const SHARDS: usize = 16;

/// The most entries one hold of a shard lock pops. Every push takes a shard
/// lock.
const BATCH: usize = 4096;

/// The state behind the keys of a [`DeadlineSweep`], and what to do when a
/// key comes due.
pub(crate) trait SweepTarget: Send + Sync + 'static {
    /// The longest a pushed entry waits before the sweep first looks at it.
    const FIRST_LOOK: Duration;
    /// The longest a requeued entry waits before the sweep looks again.
    const RECHECK: Duration;

    /// Act on the entries that came due by `now`, each a key with the
    /// instant it was queued at. Push into `renewed` each key to look at
    /// again, with a time after `now`.
    ///
    /// Called with no queue lock held, so it can take the target's own
    /// locks. A key may be stale: its stream may have ended or moved on. A
    /// target that keeps one live entry per key tells its own entry from a
    /// stale copy by the instant. Such a target records the instant of a
    /// requeue from [`DeadlineSweep::requeue_at`], which is the instant the
    /// queue holds.
    fn on_due(&self, due: Vec<(Instant, u64)>, now: Instant, renewed: &mut Vec<(Instant, u64)>);
}

/// A sharded deadline queue and the one task that sweeps it.
///
/// The sweep starts on the first push that finds none running. It runs on
/// the runtime given to [`pin_runtime`](Self::pin_runtime), or else on the
/// runtime of that push. The entries live in the queue, not in the task:
/// when a runtime drops the sweep, the next push starts a new one, and
/// nothing queued is lost. A push from a thread with no runtime still queues
/// its entry for the next sweep.
///
/// Without a pinned runtime, entries that are already queued wait for the
/// next push after their sweep's runtime is gone. Pin the runtime that the
/// owner's work runs on, when there is one, so the sweep lives as long as
/// that work.
pub(crate) struct DeadlineSweep<T> {
    shards: [DeadlineQueue<u64>; SHARDS],
    /// Signalled by `push` when the new entry is the earliest of its shard,
    /// so the sweep retargets its wait.
    earlier: tokio::sync::Notify,
    /// Cancelled by [`stop`](Self::stop). It ends the sweep, and no sweep
    /// starts after it.
    stopped: CancellationToken,
    /// Whether a sweep task exists. Cleared by [`Alive`] when that task ends
    /// or is dropped.
    alive: AtomicBool,
    /// The runtime to sweep on, once the owner knows one.
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
    target: PhantomData<fn(&T)>,
}

/// Clears [`DeadlineSweep::alive`] when the sweep ends.
///
/// Built before the spawn and moved into the task, not created inside it: a
/// runtime that is shutting down can drop a spawned task before its first
/// poll. A guard made in the task body would then never exist, the flag would
/// stay set, and no sweep would start again.
struct Alive<T: SweepTarget>(Arc<DeadlineSweep<T>>);

impl<T: SweepTarget> Drop for Alive<T> {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::Release);
    }
}

/// The owner's handle on a [`DeadlineSweep`]: it stops the sweep when the
/// owner drops.
///
/// Streams hold the queue too, so the sweep cannot learn from the queue
/// itself that its owner is gone. A field of this type stops it without a
/// `Drop` impl on the owner, which would stop callers from moving the owner's
/// public fields out.
pub(crate) struct SweepOwner<T: SweepTarget>(Arc<DeadlineSweep<T>>);

impl<T: SweepTarget> Default for SweepOwner<T> {
    fn default() -> Self {
        Self(DeadlineSweep::new())
    }
}

impl<T: SweepTarget> std::ops::Deref for SweepOwner<T> {
    type Target = Arc<DeadlineSweep<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: SweepTarget> Drop for SweepOwner<T> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

impl<T: SweepTarget> DeadlineSweep<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            shards: std::array::from_fn(|_| DeadlineQueue::new(T::FIRST_LOOK, T::RECHECK)),
            earlier: tokio::sync::Notify::new(),
            stopped: CancellationToken::new(),
            alive: AtomicBool::new(false),
            runtime: std::sync::OnceLock::new(),
            target: PhantomData,
        })
    }

    fn shard(&self, key: u64) -> &DeadlineQueue<u64> {
        &self.shards[(key % SHARDS as u64) as usize]
    }

    /// When the earliest entry of any shard comes due.
    fn next_due(&self) -> Option<Instant> {
        self.shards.iter().filter_map(DeadlineQueue::next_due).min()
    }

    /// How many entries are queued, stale ones included.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.heap().len()).sum()
    }

    /// Whether a sweep task exists on this queue.
    #[cfg(test)]
    pub(crate) fn sweep_running(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// End the sweep, and start no other.
    pub(crate) fn stop(&self) {
        self.stopped.cancel();
    }

    /// Start every later sweep on `runtime`. The first call wins.
    pub(crate) fn pin_runtime(&self, runtime: tokio::runtime::Handle) {
        let _ = self.runtime.set(runtime);
    }

    /// Queue a look at `key` by `deadline`, and start a sweep if none runs.
    ///
    /// Callers can hold a lock of the target: this takes only a queue shard
    /// lock, and the sweep never holds a queue shard lock while it calls the
    /// target. A spawn does not run the task in place.
    ///
    /// The sweep is woken when the entry is the earliest of its shard. That
    /// is not always the earliest of the queue, and then the wake only costs
    /// the sweep a fresh look at the shards.
    ///
    /// Returns when the entry comes due, after the [`SweepTarget::FIRST_LOOK`]
    /// cap. A target that keeps one live entry per key records it, under the
    /// target lock it holds, before the sweep can pop the entry.
    pub(crate) fn push(self: &Arc<Self>, target: &Arc<T>, deadline: Instant, key: u64) -> Instant {
        let pushed = self.shard(key).push(deadline, key);
        if pushed.earliest {
            self.earlier.notify_one();
        }
        if !self.alive.load(Ordering::Acquire) {
            self.start(target);
        }
        pushed.at
    }

    /// When the sweep, requeuing at `now`, queues an entry for `deadline`:
    /// at most [`SweepTarget::RECHECK`] after `now`.
    pub(crate) fn requeue_at(now: Instant, deadline: Instant) -> Instant {
        requeue_at(now, deadline, T::RECHECK)
    }

    /// Arm `key` to come due by `expires`, for a target that keeps one live
    /// entry per key and records its instant in `queued`.
    ///
    /// Queues nothing while the live entry is still ahead and not after
    /// `expires`: that entry reads the new deadline from the target when it
    /// comes due. Otherwise queues a new entry and records it. Without this,
    /// each re-arm queued one more entry, and the sweep requeued every copy.
    ///
    /// The caller holds the target lock that guards `queued`, so the sweep
    /// cannot pop the new entry before `queued` names it.
    pub(crate) fn arm(
        self: &Arc<Self>,
        target: &Arc<T>,
        queued: &mut Option<Instant>,
        expires: Instant,
        key: u64,
    ) {
        let live = queued.is_some_and(|at| Instant::now() < at && at <= expires);
        if !live {
            *queued = Some(self.push(target, expires, key));
        }
    }

    #[cold]
    fn start(self: &Arc<Self>, target: &Arc<T>) {
        if self.stopped.is_cancelled() {
            return;
        }
        // The caller may run on an executor that is not tokio. With no pinned
        // runtime, the entry stays queued for the next push that has one.
        let Some(rt) = self
            .runtime
            .get()
            .cloned()
            .or_else(|| tokio::runtime::Handle::try_current().ok())
        else {
            return;
        };
        if self
            .alive
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        rt.spawn(Self::run(Alive(Arc::clone(self)), Arc::downgrade(target)));
    }

    /// Park until the earliest entry is due, act on what is due, repeat.
    ///
    /// Holds the target weakly, so a parked sweep does not keep the state of
    /// a dropped owner alive.
    async fn run(alive: Alive<T>, target: Weak<T>) {
        let sweep = &alive.0;
        loop {
            let next = sweep.next_due();
            tokio::select! {
                biased;
                () = sweep.stopped.cancelled() => return,
                // An earlier entry was pushed: retarget the wait.
                () = sweep.earlier.notified() => continue,
                () = async {
                    match next {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        // Empty: only a push, through `earlier`, wakes it.
                        None => std::future::pending().await,
                    }
                } => {}
            }
            let Some(target) = target.upgrade() else {
                return;
            };
            let now = Instant::now();
            for shard in &sweep.shards {
                loop {
                    let due = shard.pop_due(now, BATCH);
                    // Nothing due in this shard: skip the requeue and its
                    // lock. After a burst, the requeue of its last chunk
                    // shrinks.
                    if due.is_empty() {
                        break;
                    }
                    let more = due.len() == BATCH;
                    let mut renewed = Vec::new();
                    target.on_due(due, now, &mut renewed);
                    // Every renewed time is after `now`, so this loop never
                    // pops its own requeues. A key goes back to the shard it
                    // came from, because the shard is a function of the key.
                    shard.requeue(now, renewed);
                    if !more {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

/// What a sweep does with a due key, decided from the state of its entry.
pub(crate) enum Due {
    /// Nothing waits on the deadline (a sender is attached, or there is no
    /// timeout), or the queue entry is a stale copy: forget the queue entry.
    Drop,
    /// A renewal moved the deadline past now: look again then.
    Requeue(Instant),
    /// The deadline has passed: remove the entry.
    Expire,
}

impl Due {
    /// The verdict for an entry that waits on `expires`.
    pub(crate) fn at(expires: Option<Instant>, now: Instant) -> Self {
        match expires {
            Some(deadline) if deadline > now => Self::Requeue(deadline),
            Some(_) => Self::Expire,
            None => Self::Drop,
        }
    }

    /// The verdict for the queue entry of `entry` that came due `at`.
    fn of<V: Expiring>(entry: &V, at: Instant, now: Instant) -> Self {
        // The entry queued a later copy: that one does the work. Acting on
        // both would requeue each, and the copies would pile up.
        if entry.queued() != Some(at) {
            return Self::Drop;
        }
        entry.due(now)
    }
}

/// An entry of a registry whose deadline a [`DeadlineSweep`] keeps.
///
/// The entry has one live entry in the queue, and records when it comes due,
/// so the sweep can tell it from a stale copy. [`DeadlineSweep::arm`] queues
/// it, and [`expire_due`] requeues it.
pub(crate) trait Expiring {
    /// What to do with the entry at `now`, from its own state.
    fn due(&self, now: Instant) -> Due;
    /// When the entry's live queue entry comes due.
    fn queued(&self) -> Option<Instant>;
    /// Record the instant of a requeued entry.
    fn set_queued(&mut self, at: Instant);
    /// End an entry that the sweep removed. Runs after the shard lock is
    /// released.
    fn expire(self);
}

/// Act on the due keys of a registry whose entries expire.
///
/// A read lock decides each key: most belong to entries that a sender took,
/// that ended, or that are stale copies, and a write lock per key would
/// contend with attach and detach. A requeue records its new instant in the
/// entry, and an expiry removes it, so both take the write lock and decide
/// again: a renewal, attach or re-arm can land in between.
pub(crate) fn expire_due<V: Expiring>(
    registry: &DashMap<u64, V>,
    due: Vec<(Instant, u64)>,
    now: Instant,
    renewed: &mut Vec<(Instant, u64)>,
) where
    DashMap<u64, V>: SweepTarget,
{
    use dashmap::mapref::entry::Entry;
    for (at, key) in due {
        let Some(verdict) = registry.get(&key).map(|entry| Due::of(&*entry, at, now)) else {
            continue; // already removed
        };
        if matches!(verdict, Due::Drop) {
            continue;
        }
        let Entry::Occupied(mut entry) = registry.entry(key) else {
            continue;
        };
        match Due::of(entry.get(), at, now) {
            Due::Expire => entry.remove().expire(),
            Due::Requeue(deadline) => {
                let requeue_at = DeadlineSweep::<DashMap<u64, V>>::requeue_at(now, deadline);
                entry.get_mut().set_queued(requeue_at);
                renewed.push((requeue_at, key));
            }
            Due::Drop => {}
        }
    }
}
