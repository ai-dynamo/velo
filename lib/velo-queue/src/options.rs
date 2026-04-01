// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Options for batch receive operations.

use std::time::Duration;

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

    /// Set the maximum batch size.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Set the timeout for batch collection.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }
}
