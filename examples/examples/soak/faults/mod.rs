// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Application-layer fault helpers.
//!
//! A previous design called for a `Transport` wrapper that could drop, delay,
//! or reorder frames. Implementing that requires a full `Transport` impl plus
//! AM-type-aware filtering (otherwise response frames would also get scrambled,
//! breaking unrelated request/response paths). For the first cut we inject
//! faults from the scenario side instead — same correctness coverage, much
//! smaller blast radius.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cheap deterministic-ish dice. We seed from `tag` so repeat runs of the same
/// scenario hit the same modulo pattern; that way "drop ~1%" becomes "drop
/// every 100th item" rather than a real RNG. The point of soak is bulk volume,
/// not statistical purity.
pub struct Dice {
    counter: AtomicU64,
    period: u64,
}

impl Dice {
    /// `period_inv = 100` means "1 in 100" hits. `0` disables.
    pub fn new(period_inv: u64) -> Self {
        Self {
            counter: AtomicU64::new(0),
            period: period_inv,
        }
    }

    pub fn hit(&self) -> bool {
        if self.period == 0 {
            return false;
        }
        // fetch_add returns the pre-increment value; use the post-increment
        // value so the first hit lands on the configured period instead of
        // immediately (counter starts at 0).
        let n = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        n.is_multiple_of(self.period)
    }
}
