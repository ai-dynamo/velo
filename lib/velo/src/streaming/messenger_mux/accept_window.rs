// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The accept window: how long a bind waits for the `OpenSlot` that claims
//! it, and the deadline queues the sweep closes it from.
//!
//! An attach bind waits the fixed [`ACCEPT_TIMEOUT`](super::ACCEPT_TIMEOUT).
//! A pre-bind waits [`MuxConfig::prebind_accept_timeout`](super::MuxConfig::prebind_accept_timeout),
//! which may be another length or no limit. Each kind has its own queue,
//! because a queue stays in deadline order only while every entry in it has
//! the same timeout.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

/// Which accept window a bind gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BindKind {
    /// An attach, or a bind through the bare `FrameTransport::bind`. A sender
    /// has already asked, so the window is the fixed
    /// [`ACCEPT_TIMEOUT`](super::ACCEPT_TIMEOUT).
    Attach,
    /// A zero-RTT pre-bind. No sender has asked yet, so the window is
    /// [`MuxConfig::prebind_accept_timeout`](super::MuxConfig::prebind_accept_timeout).
    PreBind,
}

/// Accept-window deadlines for binds that share one timeout, oldest first.
///
/// Every entry has the same timeout, so pushing at bind time keeps the queue
/// in deadline order and the sweep only ever pops its front. A bind claimed
/// or released early stays queued and costs a failed lookup when its deadline
/// comes; that is cheaper than finding and removing it. It used to be one
/// timer task per bind, which nothing but shutdown cancelled: a claimed
/// bind's task lived out the full minute, so a frontend opening 2,000 streams
/// a second carried about 120,000 idle tasks.
pub(super) struct BindDeadlines {
    timeout: Duration,
    queue: Mutex<VecDeque<(tokio::time::Instant, (u64, u64))>>,
}

impl BindDeadlines {
    pub(super) fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            queue: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<(tokio::time::Instant, (u64, u64))>> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Start the window for one `(anchor_id, session_id)`.
    pub(super) fn push(&self, key: (u64, u64)) {
        let mut queue = self.lock();
        // The clock is read under the lock, so pushes land in deadline order.
        // A deadline past the clock's range never comes, so it needs no entry,
        // and `+` would panic on it.
        if let Some(deadline) = tokio::time::Instant::now().checked_add(self.timeout) {
            queue.push_back((deadline, key));
        }
    }

    /// Remove and return every bind whose deadline is at or before `now`.
    pub(super) fn take_due(&self, now: tokio::time::Instant) -> Vec<(u64, u64)> {
        let mut queue = self.lock();
        let ready = queue.partition_point(|(deadline, _)| *deadline <= now);
        let due = queue.drain(..ready).map(|(_, key)| key).collect();
        // A burst of binds leaves the queue's capacity behind it. Give it
        // back only once the queue is nearly empty: the shrink copies what is
        // left while holding the lock every bind of this kind takes, so
        // shrinking a queue still tens of thousands deep would stall new
        // streams.
        if queue.len() <= 1024 && queue.capacity() > 8192 {
            queue.shrink_to(2048);
        }
        due
    }

    /// Deadlines queued, whether or not their bind is still there.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().len()
    }
}
