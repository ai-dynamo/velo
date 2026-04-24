// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Acknowledgment handle for manually-acked work items.
//!
//! Each backend provides its own [`AckHandleInner`] implementation, erased
//! behind [`AckHandle`]. The typed [`WorkItem`](crate::WorkItem) wrapper owns
//! the handle and exposes `ack`/`nack`/`in_progress`/`term` methods that
//! delegate through it.

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
pub struct AckHandle(Box<dyn AckHandleInner>);

impl AckHandle {
    /// Wrap a backend-specific handle.
    pub fn new<H: AckHandleInner + 'static>(inner: H) -> Self {
        Self(Box::new(inner))
    }

    pub(crate) async fn ack(self) -> Result<(), WorkQueueRecvError> {
        self.0.ack().await
    }

    pub(crate) async fn nack(self, delay: Duration) -> Result<(), WorkQueueRecvError> {
        self.0.nack(delay).await
    }

    pub(crate) async fn in_progress(&self) -> Result<(), WorkQueueRecvError> {
        self.0.in_progress().await
    }

    pub(crate) async fn term(self) -> Result<(), WorkQueueRecvError> {
        self.0.term().await
    }
}

impl std::fmt::Debug for AckHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckHandle").finish_non_exhaustive()
    }
}
