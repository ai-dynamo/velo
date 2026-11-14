// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The idle reaper's clock.
//!
//! Every time the reaper reads goes through [`ReaperClock::now`]: the pass
//! clock, the stamp after `ucp_ep_create`, and the completion time of an
//! endpoint's last send in flight. In a normal build the type has no fields
//! and `now` is `Instant::now()`, so the send-completion path pays nothing for
//! it.
//!
//! With `test` or the `test-helpers` feature, a test can also freeze the clock
//! and move it by hand ([`ManualClock`]). The reaper tests use that to decide
//! when time passes: an endpoint then ages only when the test says so, and a
//! thread starved on a loaded host cannot age it. A build with `test-helpers`
//! that never freezes the clock pays one untaken branch per read.

use std::time::Instant;

#[cfg(any(test, feature = "test-helpers"))]
use std::sync::Arc;
#[cfg(any(test, feature = "test-helpers"))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(any(test, feature = "test-helpers"))]
use std::time::Duration;

/// The clock the idle reaper reads. Cheap to clone: no fields in a normal
/// build, one `Arc` with test helpers.
#[derive(Clone, Default)]
pub(crate) struct ReaperClock {
    #[cfg(any(test, feature = "test-helpers"))]
    manual: Arc<ManualClock>,
}

impl ReaperClock {
    /// The current time, or the frozen time while a test holds the clock.
    #[inline(always)]
    pub(crate) fn now(&self) -> Instant {
        #[cfg(any(test, feature = "test-helpers"))]
        if let Some(now) = self.manual.frozen_now() {
            return now;
        }
        Instant::now()
    }

    /// The settable side, for tests.
    #[cfg(any(test, feature = "test-helpers"))]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn manual(&self) -> &ManualClock {
        &self.manual
    }
}

/// A clock a test can freeze and then move forward by hand.
///
/// Frozen, it reads `base + offset`, and only [`ManualClock::advance`] moves
/// it. It never moves backward: freezing starts from the real time.
#[cfg(any(test, feature = "test-helpers"))]
pub(crate) struct ManualClock {
    frozen: AtomicBool,
    base: Instant,
    offset_ns: AtomicU64,
}

#[cfg(any(test, feature = "test-helpers"))]
impl Default for ManualClock {
    fn default() -> Self {
        Self {
            frozen: AtomicBool::new(false),
            base: Instant::now(),
            offset_ns: AtomicU64::new(0),
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[cfg_attr(not(test), allow(dead_code))]
impl ManualClock {
    fn frozen_now(&self) -> Option<Instant> {
        if !self.frozen.load(Ordering::Acquire) {
            return None;
        }
        Some(self.base + Duration::from_nanos(self.offset_ns.load(Ordering::Acquire)))
    }

    /// Stop the clock at the current real time.
    pub(crate) fn freeze(&self) {
        let ns = self.base.elapsed().as_nanos() as u64;
        self.offset_ns.store(ns, Ordering::Release);
        self.frozen.store(true, Ordering::Release);
    }

    /// Move a frozen clock forward.
    pub(crate) fn advance(&self, by: Duration) {
        self.offset_ns
            .fetch_add(by.as_nanos() as u64, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frozen_clock_moves_only_when_advanced() {
        let clock = ReaperClock::default();
        clock.manual().freeze();
        let t0 = clock.now();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(clock.now(), t0, "a frozen clock moved by itself");
        clock.manual().advance(Duration::from_millis(500));
        assert_eq!(clock.now(), t0 + Duration::from_millis(500));
    }

    #[test]
    fn freezing_does_not_move_the_clock_backward() {
        let clock = ReaperClock::default();
        let before = clock.now();
        clock.manual().freeze();
        assert!(clock.now() >= before);
    }
}
