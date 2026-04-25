// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Acknowledgment handle for manually-acked work items.
//!
//! Each backend provides its own [`AckHandleInner`] implementation, erased
//! behind [`AckHandle`]. The typed [`WorkItem`](crate::WorkItem) wrapper owns
//! the handle and exposes `ack`/`nack`/`in_progress`/`term` methods that
//! delegate through it.
//!
//! [`AckHandle`] owns the "drop = nack(0)" semantic: a handle dropped without
//! `ack` / `nack` / `term` having been called fires a best-effort spawned
//! `nack(0)` on Drop. Consuming methods take the inner via `Option::take`,
//! leaving `None` behind so Drop is a no-op when the handle was resolved
//! explicitly.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::error::WorkQueueRecvError;

/// Boxed future returned by [`AckHandleInner`] methods.
pub type AckFuture<'a> = Pin<Box<dyn Future<Output = Result<(), WorkQueueRecvError>> + Send + 'a>>;

/// Per-backend acknowledgment behavior.
///
/// Consuming methods take `Box<Self>` so double-ack is a compile error.
/// `in_progress` takes `&self` because heartbeat extensions can be sent
/// repeatedly for a single lease.
pub trait AckHandleInner: Send + Sync {
    /// Acknowledge the item — remove it from the queue permanently.
    fn ack(self: Box<Self>) -> AckFuture<'static>;

    /// Negatively acknowledge the item — return it to the queue for redelivery.
    ///
    /// If `delay` is non-zero, the item is held before becoming visible again.
    fn nack(self: Box<Self>, delay: Duration) -> AckFuture<'static>;

    /// Extend the visibility timeout — signal that the item is still being processed.
    ///
    /// Can be called repeatedly as a heartbeat.
    fn in_progress<'a>(&'a self) -> AckFuture<'a>;

    /// Terminate the item — do not redeliver. Optionally moved to a dead-letter queue.
    fn term(self: Box<Self>) -> AckFuture<'static>;
}

/// Type-erased acknowledgment handle for a single received work item.
///
/// Produced by [`ReceiverBackend`](crate::ReceiverBackend) implementations
/// under [`AckPolicy::Manual`](crate::AckPolicy::Manual) and consumed by the
/// methods on [`WorkItem`](crate::WorkItem).
///
/// Wraps `Option<Box<dyn AckHandleInner>>`. The terminal methods (`ack`,
/// `nack`, `term`) take the inner via `Option::take`, leaving `None`. The
/// [`Drop`] impl checks for `Some` and fires a best-effort `nack(0)`; when a
/// terminal method has already run, Drop sees `None` and is a no-op.
pub struct AckHandle(Option<Box<dyn AckHandleInner>>);

impl AckHandle {
    /// Wrap a backend-specific handle.
    pub fn new<H: AckHandleInner + 'static>(inner: H) -> Self {
        Self(Some(Box::new(inner)))
    }

    pub(crate) async fn ack(mut self) -> Result<(), WorkQueueRecvError> {
        match self.0.take() {
            Some(inner) => inner.ack().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn nack(mut self, delay: Duration) -> Result<(), WorkQueueRecvError> {
        match self.0.take() {
            Some(inner) => inner.nack(delay).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn in_progress(&self) -> Result<(), WorkQueueRecvError> {
        match &self.0 {
            Some(inner) => inner.in_progress().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn term(mut self) -> Result<(), WorkQueueRecvError> {
        match self.0.take() {
            Some(inner) => inner.term().await,
            None => Ok(()),
        }
    }
}

impl Drop for AckHandle {
    fn drop(&mut self) {
        let Some(inner) = self.0.take() else { return };
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    if let Err(e) = inner.nack(Duration::ZERO).await {
                        tracing::warn!(
                            "AckHandle dropped without explicit outcome; nack failed: {e}"
                        );
                    }
                });
            }
            Err(_) => {
                tracing::warn!(
                    "AckHandle dropped without active tokio runtime; relying on backend visibility timeout for redelivery"
                );
            }
        }
    }
}

impl std::fmt::Debug for AckHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckHandle")
            .field("resolved", &self.0.is_none())
            .finish()
    }
}
