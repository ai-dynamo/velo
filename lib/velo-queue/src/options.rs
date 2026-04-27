// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Options for batch receive operations and acknowledgment policy.

use std::time::Duration;

/// Controls whether received items must be explicitly acknowledged.
///
/// - [`AckPolicy::Auto`] (default): the backend acknowledges each item on
///   receipt. Items leave the queue immediately; worker crashes lose in-flight
///   work. Suitable for fire-and-forget workloads.
/// - [`AckPolicy::Manual`]: the caller must call [`ack`], [`nack`], or
///   [`term`] on each received item. Dropping a [`WorkItem`] without an
///   explicit outcome is treated as an implicit `nack(0)` (redeliver). Items
///   that are nack'd or time out (visibility timeout) are redelivered by the
///   backend. Required for at-least-once processing.
///
/// [`WorkItem`]: crate::WorkItem
/// [`ack`]: crate::WorkItem::ack
/// [`nack`]: crate::WorkItem::nack
/// [`term`]: crate::WorkItem::term
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum AckPolicy {
    /// Backend acknowledges items on receipt. Default.
    #[default]
    Auto,
    /// Caller must explicitly acknowledge each item.
    Manual,
}

/// Options for [`WorkQueueReceiver::next_with_options`](crate::WorkQueueReceiver::next_with_options).
///
/// Controls batch fetching behavior: the receiver will block until `batch_size`
/// items are collected **or** `timeout` elapses, whichever comes first.
#[derive(Debug, Clone)]
pub struct NextOptions {
    /// Maximum number of items to return in one batch. Defaults to 1.
    pub batch_size: usize,
    /// Maximum time to wait for the batch to fill. Defaults to 30 seconds.
    pub timeout: Duration,
}

impl Default for NextOptions {
    fn default() -> Self {
        Self {
            batch_size: 1,
            timeout: Duration::from_secs(30),
        }
    }
}

impl NextOptions {
    /// Create new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum batch size. Must be at least 1.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    pub fn batch_size(mut self, n: usize) -> Self {
        assert!(n > 0, "batch_size must be at least 1");
        self.batch_size = n;
        self
    }

    /// Set the timeout for batch collection.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }
}
