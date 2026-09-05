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
}

impl TestHooks {
    /// Hold the loop at the barrier the next time it reaches one.
    pub(super) fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    /// Let it continue.
    pub(super) fn release(&self) {
        self.paused.store(false, Ordering::Release);
        self.resume.notify_waiters();
    }

    /// Wait until the loop is parked at the barrier.
    ///
    /// A positive fact, so a test that arranges state "while it is parked"
    /// really does. Polled rather than notified because the alternative is a
    /// wakeup protocol whose own races would need testing.
    pub(super) async fn wait_until_parked(&self) {
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

    /// Called by the run loop on each wake, before its drain loop runs.
    pub(super) async fn barrier(&self) {
        if !self.paused.load(Ordering::Acquire) {
            return;
        }
        self.parked.store(true, Ordering::Release);
        loop {
            // Registered before the check, so a release landing between the two
            // is held as a permit rather than missed.
            let resumed = self.resume.notified();
            if !self.paused.load(Ordering::Acquire) {
                break;
            }
            resumed.await;
        }
        self.parked.store(false, Ordering::Release);
    }
}
