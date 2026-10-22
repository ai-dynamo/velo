// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Renewable deadlines, shared by anchor reservations and mux binds.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// `now + lifetime`, saturating 30 years out as `tokio::time::sleep` does.
///
/// `Instant + Duration` panics on overflow, and a caller that passes
/// `Duration::MAX` as a timeout means "never".
pub(crate) fn deadline_after(lifetime: Duration) -> Instant {
    const FAR_FUTURE: Duration = Duration::from_secs(86_400 * 365 * 30);
    let now = Instant::now();
    now.checked_add(lifetime)
        .unwrap_or_else(|| now + FAR_FUTURE)
}

/// When [`DeadlineQueue::requeue`] at `now` queues an entry for `deadline`:
/// at most `recheck` after `now`.
///
/// An owner that records the instant of its entry computes it here, so the
/// instant it records is the one the queue holds.
pub(crate) fn requeue_at(now: Instant, deadline: Instant, recheck: Duration) -> Instant {
    now.checked_add(recheck)
        .map_or(deadline, |cap| deadline.min(cap))
}

/// Move `expires` to a full `lifetime` from now, unless it already passed.
///
/// A passed deadline stays passed: renewal never revives what expiry is about
/// to remove.
pub(crate) fn renew_if_live(expires: &mut Instant, lifetime: Duration) {
    if *expires > Instant::now() {
        *expires = deadline_after(lifetime);
    }
}

/// When to look at each key again, earliest first.
///
/// An entry is a hint, not the deadline: the owner stores the deadline that
/// counts with the key, and renewal moves that one. When an entry comes due,
/// the owner reads the stored deadline and requeues the key if it moved. So
/// an entry is never removed early. A key that leaves its owner (claimed,
/// attached, released) leaves its entry behind, and that entry costs one
/// failed lookup when it comes due. That is cheaper than finding it in the
/// heap on the claim path, which every stream takes.
///
/// What bounds the stale entries is the cap: no entry is first queued more
/// than `first_look` ahead, and no entry is queued again more than `recheck`
/// ahead. Queued at the key's own deadline instead, a `Duration::MAX` timeout
/// came due in 30 years, and every key ever queued stayed in the heap for the
/// life of the process.
/// What [`DeadlineQueue::push`] did.
pub(crate) struct Pushed {
    /// When the entry comes due.
    pub(crate) at: Instant,
    /// Whether the entry is now the earliest in the queue.
    pub(crate) earliest: bool,
}

pub(crate) struct DeadlineQueue<K> {
    heap: Mutex<BinaryHeap<Reverse<(Instant, K)>>>,
    first_look: Duration,
    recheck: Duration,
}

impl<K: Ord> DeadlineQueue<K> {
    /// A queue that holds a pushed entry at most `first_look` ahead, and a
    /// requeued one at most `recheck` ahead.
    pub(crate) const fn new(first_look: Duration, recheck: Duration) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            first_look,
            recheck,
        }
    }

    /// Queue a look at `key` by `deadline`, at most `first_look` from now.
    ///
    /// Returns when the entry comes due, after the cap, so an owner can tell
    /// its own entry from a stale one when it pops. Also returns whether the
    /// entry is now the earliest, so a task parked on the old head can wake
    /// and retarget. A push behind the head needs no wake.
    pub(crate) fn push(&self, deadline: Instant, key: K) -> Pushed {
        let at = deadline.min(deadline_after(self.first_look));
        let mut heap = self.heap.lock();
        let earliest = heap.peek().is_none_or(|head| at < head.0.0);
        heap.push(Reverse((at, key)));
        Pushed { at, earliest }
    }

    /// Pop at most `max` entries that are due by `now`, each with the instant
    /// it was queued at.
    ///
    /// Bounded because every push takes this lock: a sweep that pops a whole
    /// burst in one hold stalls every new stream for as long as that takes.
    /// A result of `max` entries means more can be due; call again.
    pub(crate) fn pop_due(&self, now: Instant, max: usize) -> Vec<(Instant, K)> {
        let mut heap = self.heap.lock();
        let mut due = Vec::new();
        while due.len() < max && heap.peek().is_some_and(|entry| entry.0.0 <= now) {
            due.push(heap.pop().expect("due entry").0);
        }
        due
    }

    /// Queue every key in `renewed` again, each at most `recheck` after
    /// `now`, under one lock.
    ///
    /// Then give back the capacity a burst left behind, but only once the
    /// queue is nearly empty. The shrink copies what is left while holding
    /// the lock every push takes, so shrinking a queue still tens of
    /// thousands deep would stall new streams. It is decided after the
    /// requeue, so a sweep never shrinks a queue it is about to refill.
    pub(crate) fn requeue(&self, now: Instant, renewed: impl IntoIterator<Item = (Instant, K)>) {
        let mut heap = self.heap.lock();
        heap.extend(
            renewed
                .into_iter()
                .map(|(deadline, key)| Reverse((requeue_at(now, deadline, self.recheck), key))),
        );
        if heap.len() <= 1024 && heap.capacity() > 8192 {
            heap.shrink_to(2048);
        }
    }

    /// When the earliest entry comes due, or `None` when the queue is empty.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.heap.lock().peek().map(|head| head.0.0)
    }

    /// The heap itself, for tests that read its length or capacity, or plant
    /// entries directly.
    #[cfg(test)]
    pub(crate) fn heap(&self) -> parking_lot::MutexGuard<'_, BinaryHeap<Reverse<(Instant, K)>>> {
        self.heap.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST_LOOK: Duration = Duration::from_secs(1);
    const RECHECK: Duration = Duration::from_secs(60);

    fn queue() -> DeadlineQueue<u64> {
        DeadlineQueue::new(FIRST_LOOK, RECHECK)
    }

    fn head(q: &DeadlineQueue<u64>) -> Option<Instant> {
        q.next_due()
    }

    /// A key pushed with a far deadline comes due at the first look, not at
    /// its deadline. This is what bounds how long a stale entry lives.
    #[tokio::test(start_paused = true)]
    async fn push_caps_an_entry_at_the_first_look() {
        let q = queue();
        let now = Instant::now();
        let pushed = q.push(deadline_after(Duration::MAX), 1);
        assert_eq!(
            pushed.at,
            now + FIRST_LOOK,
            "push reports the capped instant"
        );
        assert_eq!(head(&q), Some(now + FIRST_LOOK));
        q.push(now + Duration::from_millis(10), 2);
        assert_eq!(head(&q), Some(now + Duration::from_millis(10)));
    }

    /// A requeued key is capped at the recheck after the sweep's `now`.
    #[tokio::test(start_paused = true)]
    async fn requeue_caps_at_the_recheck() {
        let q = queue();
        let now = Instant::now();
        q.requeue(now, [(deadline_after(Duration::MAX), 1)]);
        assert_eq!(head(&q), Some(now + RECHECK));
        q.requeue(now, [(now + Duration::from_secs(5), 2)]);
        assert_eq!(head(&q), Some(now + Duration::from_secs(5)));
    }

    /// The return value says whether the push became the head, which is when
    /// a parked sweep must wake. Pushes behind the head do not wake it.
    #[tokio::test(start_paused = true)]
    async fn push_reports_only_a_new_head() {
        let q = queue();
        let now = Instant::now();
        assert!(
            q.push(now + Duration::from_millis(500), 1).earliest,
            "empty queue"
        );
        assert!(
            !q.push(now + Duration::from_millis(800), 2).earliest,
            "behind the head"
        );
        assert!(
            !q.push(now + Duration::from_millis(500), 3).earliest,
            "tied with the head"
        );
        assert!(
            q.push(now + Duration::from_millis(100), 4).earliest,
            "ahead of the head"
        );
    }

    /// A sweep pops at most `max` keys per call, earliest first, and leaves
    /// entries that are not due.
    #[tokio::test(start_paused = true)]
    async fn pop_due_is_bounded_and_ordered() {
        let q = queue();
        let now = Instant::now();
        for key in (0..10).rev() {
            q.push(now + Duration::from_millis(key), key);
        }
        q.push(now + Duration::from_millis(900), 99);
        let later = now + Duration::from_millis(100);
        let keys = |due: Vec<(Instant, u64)>| -> Vec<u64> {
            due.into_iter()
                .map(|(at, key)| {
                    assert_eq!(
                        at,
                        now + Duration::from_millis(key),
                        "popped with its instant"
                    );
                    key
                })
                .collect()
        };
        assert_eq!(keys(q.pop_due(later, 4)), vec![0, 1, 2, 3]);
        assert_eq!(keys(q.pop_due(later, 100)), vec![4, 5, 6, 7, 8, 9]);
        assert!(q.pop_due(later, 100).is_empty(), "99 is not due");
        assert_eq!(q.heap().len(), 1);
    }

    /// After a burst drains, the queue gives its capacity back.
    #[tokio::test(start_paused = true)]
    async fn a_drained_burst_gives_its_capacity_back() {
        let q = queue();
        let now = Instant::now();
        for key in 0..20_000 {
            q.push(now, key);
        }
        while !q.pop_due(now, 4096).is_empty() {}
        assert!(q.heap().capacity() > 8192);
        q.requeue(now, []);
        assert!(q.heap().capacity() <= 4096, "{}", q.heap().capacity());
    }
}
