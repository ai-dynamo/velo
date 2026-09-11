// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A pause point the run loop offers its own tests.
//!
//! One property of the loop cannot be reached from outside it. The kick is
//! taken *after* the first drain and the loop then drains **again**, which is
//! what makes "flush what I sent" exact: a record queued between the first
//! drain finding nothing and the kick being observed would otherwise sit staged
//! until the next flush. Every way of driving the batcher from outside stages
//! the records before kicking, so the second drain is unreachable — delete it
//! and nothing fails.
//!
//! Reproducing it needs the loop stopped at exactly that point, which is what
//! this is: a barrier the loop offers only when a test installed one, and a
//! no-op — one `Option` check per wake, in `cfg(test)` builds only — otherwise.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

/// How long [`TestHooks::wait_until_parked`] waits before calling it a bug.
const PATIENCE: Duration = Duration::from_secs(5);

/// The barrier, plus enough state for a test to know the loop reached it.
#[derive(Default)]
pub(crate) struct TestHooks {
    /// The next arrival at the barrier waits.
    paused: AtomicBool,
    /// The loop is sitting at the barrier right now.
    parked: AtomicBool,
    resume: Notify,
    /// Times `fire_singleton` has raised the fence.
    ///
    /// Whether a singleton's admission resolves before a test can observe the
    /// slot's withheld queue is a genuine race against the watcher task
    /// `fire_singleton` spawns — on an uncongested peer that race can go
    /// either way, which makes the withheld gauge alone unfit for pinning "no
    /// fence was raised at all". The fence itself, in contrast, is decided
    /// synchronously on the batcher's own task before that task ever yields,
    /// so counting it is race-free.
    fenced: AtomicU64,
    /// The next singleton watcher to finish holds its report at the gate
    /// below instead of calling `singleton_resolved` right away.
    ///
    /// Exists to pin the one property racing the real watcher cannot show:
    /// that an admission which never fenced writes nothing for a test to
    /// later mistake for a *different*, still-outstanding singleton's answer.
    /// Without this, "wait for the unfenced watcher's write, then fence a
    /// second singleton on the same slot, then see which lands first" is a
    /// coin flip against the scheduler — this makes the interleaving a
    /// decision the test makes instead of one it hopes for.
    resolutions_held: AtomicBool,
    resolutions_gate: Notify,
}

impl TestHooks {
    /// Hold the loop at the barrier the next time it reaches one.
    pub(crate) fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Let it continue.
    pub(crate) fn release(&self) {
        self.paused.store(false, Ordering::Release);
        self.resume.notify_waiters();
    }

    /// Wait until the loop is parked at the barrier.
    ///
    /// A positive fact, so a test that arranges state "while it is parked"
    /// really does. Polled rather than notified because the alternative is a
    /// wakeup protocol whose own races would need testing.
    pub(crate) async fn wait_until_parked(&self) {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        while tokio::time::Instant::now() < deadline {
            if self.parked.load(Ordering::Acquire) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the batcher never reached the barrier within {PATIENCE:?}");
    }

    /// Record that `fire_singleton` raised the fence.
    pub(super) fn note_fenced(&self) {
        self.fenced.fetch_add(1, Ordering::Relaxed);
    }

    /// Times the fence has been raised so far.
    pub(super) fn fenced_count(&self) -> u64 {
        self.fenced.load(Ordering::Relaxed)
    }

    /// Hold every singleton watcher's report from here on, until released.
    pub(super) fn hold_resolutions(&self) {
        self.resolutions_held.store(true, Ordering::Release);
    }

    /// Let every held report through.
    pub(super) fn release_resolutions(&self) {
        self.resolutions_held.store(false, Ordering::Release);
        self.resolutions_gate.notify_waiters();
    }

    /// Called by a singleton's watcher task once its admission has answered,
    /// before it reports that answer onward. A no-op when nothing is holding.
    pub(super) async fn await_resolutions_release(&self) {
        loop {
            // Constructed before the check, for the same reason `barrier`
            // constructs its own wait first. `notified` snapshots the
            // `notify_waiters` generation counter, and the future's first poll
            // completes on a mismatch — so a release landing between the two
            // is carried by this future. Construct it after the check instead
            // and the release has nothing to land on: `notify_waiters` stores
            // no permit. Both halves are pinned in this file's `tests`.
            let released = self.resolutions_gate.notified();
            if !self.resolutions_held.load(Ordering::Acquire) {
                return;
            }
            released.await;
        }
    }

    /// Called by the run loop on each wake, before its drain loop runs.
    pub(super) async fn barrier(&self) {
        if !self.paused.load(Ordering::Acquire) {
            return;
        }
        self.parked.store(true, Ordering::Release);
        loop {
            // Constructed before the check, so a release landing between the
            // two is carried by this future rather than lost — see
            // `await_resolutions_release` for the mechanism.
            let resumed = self.resume.notified();
            if !self.paused.load(Ordering::Acquire) {
                break;
            }
            resumed.await;
        }
        self.parked.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property both waits above are built on, stated on the primitive
    /// itself: a `Notified` constructed *before* a `notify_waiters` still
    /// completes on its first poll.
    ///
    /// `Notify::notified` reads the `notify_waiters` generation counter into
    /// the future at construction, and the first poll compares it against the
    /// live one before anything else. The release is therefore carried by the
    /// future, not held on the `Notify` — which is what makes constructing
    /// before the flag read sufficient, with no registration and no permit
    /// involved.
    #[tokio::test]
    async fn a_release_after_construction_completes_the_wait() {
        let gate = Notify::new();
        let waiter = gate.notified();
        gate.notify_waiters();
        tokio::time::timeout(PATIENCE, waiter)
            .await
            .expect("a release after construction must complete the wait");
    }

    /// The other half, and why the order is load-bearing rather than
    /// stylistic: a `notify_waiters` landing *before* the future exists is
    /// gone, because there is no permit for a later future to find.
    ///
    /// Constructing after the flag read — the shape this file deliberately
    /// does not use — puts the release in exactly this window, and the wait
    /// never ends.
    #[tokio::test(start_paused = true)]
    async fn a_release_before_construction_is_lost() {
        let gate = Notify::new();
        gate.notify_waiters();
        let waiter = gate.notified();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), waiter)
                .await
                .is_err(),
            "notify_waiters stores no permit: a wait constructed after it has \
             nothing to find, which is the bug the construct-first order avoids"
        );
    }
}
