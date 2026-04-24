// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed wrapper for a received work item with optional acknowledgment semantics.

use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::ack::AckHandle;
use crate::error::WorkQueueRecvError;

/// Maximum nack delay accepted before clamping. Matches typical queue server caps.
const MAX_NACK_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

/// A received work item wrapping a deserialized value of type `T`.
///
/// Under [`AckPolicy::Auto`](crate::AckPolicy::Auto) the backend already
/// acknowledged the item on receipt; `handle` is `None` and drop is a no-op.
///
/// Under [`AckPolicy::Manual`](crate::AckPolicy::Manual) the caller must
/// explicitly `ack`, `nack`, or `term` each item. Dropping without an
/// explicit outcome triggers a best-effort `nack(0)` (redeliver) via a
/// spawned task; if no tokio runtime is active at drop time, the backend's
/// visibility timeout is the fallback.
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

    /// Consume the wrapper and return the inner value without ack'ing.
    ///
    /// Under Manual policy this behaves like drop: best-effort `nack(0)` runs
    /// via the spawned task. Use this when you want the value but intend to
    /// defer the ack decision — rare; prefer [`ack`](Self::ack) or
    /// [`into_parts`](Self::into_parts) for explicit control.
    pub fn into_inner(mut self) -> T {
        // Leave handle in place so Drop fires its spawn logic.
        // SAFETY: move out of `self.inner` via take-and-replace-with-zeroed is not valid;
        // use std::ptr::read after preventing the original drop of inner.
        let handle = self.handle.take();
        // Reconstruct a drop guard that owns the handle but not T.
        // Simpler: build a forgettable move.
        let value = unsafe { std::ptr::read(&self.inner) };
        // Put the handle back into a minimal drop guard to preserve nack-on-drop semantics.
        let _guard = DropNackGuard { handle };
        std::mem::forget(self);
        value
    }

    /// Consume the wrapper and return both the value and the handle.
    ///
    /// The caller becomes responsible for acking/nacking via the returned
    /// handle. Useful for passing ownership across task boundaries.
    pub fn into_parts(mut self) -> (T, Option<AckHandle>) {
        let handle = self.handle.take();
        let value = unsafe { std::ptr::read(&self.inner) };
        std::mem::forget(self);
        (value, handle)
    }

    /// Acknowledge the item — remove it from the queue permanently.
    ///
    /// Under Auto policy this is a no-op.
    pub async fn ack(mut self) -> Result<(), WorkQueueRecvError> {
        match self.handle.take() {
            Some(h) => h.ack().await,
            None => Ok(()),
        }
    }

    /// Negatively acknowledge the item — redeliver after `delay`.
    ///
    /// `delay` is clamped to 24h. Under Auto policy this is a no-op because
    /// the backend already acked the item on receipt.
    pub async fn nack(mut self, delay: Duration) -> Result<(), WorkQueueRecvError> {
        let delay = clamp_delay(delay);
        match self.handle.take() {
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
    pub async fn term(mut self) -> Result<(), WorkQueueRecvError> {
        match self.handle.take() {
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
            .field("acked", &self.handle.is_none())
            .finish()
    }
}

impl<T> Drop for WorkItem<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            nack_on_drop(handle);
        }
    }
}

/// Minimal drop guard used by `into_inner` to preserve nack-on-drop
/// semantics after the value has been moved out of the WorkItem.
struct DropNackGuard {
    handle: Option<AckHandle>,
}

impl Drop for DropNackGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            nack_on_drop(handle);
        }
    }
}

fn nack_on_drop(handle: AckHandle) {
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            rt.spawn(async move {
                if let Err(e) = handle.nack(Duration::ZERO).await {
                    tracing::warn!("WorkItem dropped without explicit outcome; nack failed: {e}");
                }
            });
        }
        Err(_) => {
            tracing::warn!(
                "WorkItem dropped without active tokio runtime; relying on backend visibility timeout for redelivery"
            );
        }
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
