// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed wrapper for a received work item with optional acknowledgment semantics.

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::queue::ack::AckHandle;
use crate::queue::error::WorkQueueRecvError;

/// Maximum nack delay accepted before clamping. Matches typical queue server caps.
const MAX_NACK_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

/// A received work item wrapping a deserialized value of type `T`.
///
/// Under [`AckPolicy::Auto`](crate::queue::AckPolicy::Auto) the backend already
/// acknowledged the item on receipt; `handle` is `None`.
///
/// Under [`AckPolicy::Manual`](crate::queue::AckPolicy::Manual) the caller must
/// explicitly `ack`, `nack`, or `term` each item. Dropping a `WorkItem`
/// without an explicit outcome triggers a best-effort `nack(0)` via
/// [`AckHandle`]'s own [`Drop`] impl; if no tokio runtime is active at drop
/// time, the backend's visibility timeout is the fallback.
#[must_use = "WorkItem must be ack()'d, nack()'d, term()'d, or explicitly dropped to trigger redelivery"]
pub struct WorkItem<T> {
    inner: T,
    handle: Option<AckHandle>,
}

impl<T> WorkItem<T> {
    /// Construct a WorkItem. `handle` is `None` under Auto policy.
    pub(crate) fn new(inner: T, handle: Option<AckHandle>) -> Self {
        Self { inner, handle }
    }

    /// Consume the wrapper and return the inner value.
    ///
    /// Under Manual policy this preserves nack-on-drop semantics: the
    /// [`AckHandle`] is dropped at the end of this call, which spawns a
    /// best-effort `nack(0)`. Use [`into_parts`](Self::into_parts) instead if
    /// you want to take ownership of the handle.
    pub fn into_inner(self) -> T {
        self.inner
        // self.handle drops here — AckHandle::Drop fires nack-on-drop if Manual.
    }

    /// Consume the wrapper and return both the value and the handle.
    ///
    /// The caller becomes responsible for acking/nacking via the returned
    /// handle. Useful for passing ownership across task boundaries.
    pub fn into_parts(self) -> (T, Option<AckHandle>) {
        (self.inner, self.handle)
    }

    /// Acknowledge the item — remove it from the queue permanently.
    ///
    /// Under Auto policy this is a no-op.
    pub async fn ack(self) -> Result<(), WorkQueueRecvError> {
        match self.handle {
            Some(h) => h.ack().await,
            None => Ok(()),
        }
    }

    /// Negatively acknowledge the item — redeliver after `delay`.
    ///
    /// `delay` is clamped to 24h. Under Auto policy this is a no-op because
    /// the backend already acked the item on receipt.
    pub async fn nack(self, delay: Duration) -> Result<(), WorkQueueRecvError> {
        let delay = clamp_delay(delay);
        match self.handle {
            Some(h) => h.nack(delay).await,
            None => Ok(()),
        }
    }

    /// Extend the visibility timeout — signal that processing is still in progress.
    ///
    /// May be called repeatedly as a heartbeat. Under Auto policy this is a no-op.
    pub async fn in_progress(&self) -> Result<(), WorkQueueRecvError> {
        match &self.handle {
            Some(h) => h.in_progress().await,
            None => Ok(()),
        }
    }

    /// Terminate the item — do not redeliver, optionally move to dead-letter.
    ///
    /// Under Auto policy this is a no-op.
    pub async fn term(self) -> Result<(), WorkQueueRecvError> {
        match self.handle {
            Some(h) => h.term().await,
            None => Ok(()),
        }
    }
}

impl<T> Deref for WorkItem<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for WorkItem<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for WorkItem<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("inner", &self.inner)
            .field("manual", &self.handle.is_some())
            .finish()
    }
}

fn clamp_delay(d: Duration) -> Duration {
    if d > MAX_NACK_DELAY {
        tracing::warn!(
            "nack delay {:?} exceeds max {:?}; clamping",
            d,
            MAX_NACK_DELAY
        );
        MAX_NACK_DELAY
    } else {
        d
    }
}
