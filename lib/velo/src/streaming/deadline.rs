// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Renewable deadlines, shared by anchor reservations and mux binds.

use std::time::Duration;

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

/// Move `expires` to a full `lifetime` from now, unless it already passed.
///
/// A passed deadline stays passed: renewal never revives what expiry is about
/// to remove.
pub(crate) fn renew_if_live(expires: &mut Instant, lifetime: Duration) {
    if *expires > Instant::now() {
        *expires = deadline_after(lifetime);
    }
}
