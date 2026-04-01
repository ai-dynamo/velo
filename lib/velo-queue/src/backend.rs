// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Backend trait definitions for work queue implementations.
//!
//! Backend traits operate on raw [`Bytes`] — serialization and deserialization
//! are handled by the typed [`WorkQueueSender`](crate::WorkQueueSender) and
//! [`WorkQueueReceiver`](crate::WorkQueueReceiver) wrappers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::NextOptions;

/// Boxed future type alias for sender backend creation.
pub type SenderFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<dyn SenderBackend>, WorkQueueError>> + Send + 'a>>;

/// Boxed future type alias for receiver backend creation.
pub type ReceiverFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + 'a>>;

/// A backend that manages named work queues.
///
/// Implementations map queue names to backend-specific resources:
/// - **In-memory**: named entries in a `DashMap`
/// - **NATS JetStream**: durable streams with work-queue retention
/// - **Messenger**: actor-based handlers on a target velo instance
pub trait WorkQueueBackend: Send + Sync {
    /// Create or connect to a named queue and return a sender handle.
    fn sender(&self, name: &str) -> SenderFuture<'_>;

    /// Create or connect to a named queue and return a receiver handle.
    fn receiver(&self, name: &str) -> ReceiverFuture<'_>;
}

/// Raw-bytes sender interface for a single named queue.
///
/// Implementations handle the actual transport of serialized bytes to the
/// queue's backing storage.
pub trait SenderBackend: Send + Sync {
    /// Send serialized data to the queue. May block on backpressure.
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>>;

    /// Try to send without blocking. Returns [`WorkQueueSendError::Full`] on backpressure.
    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError>;

    /// Close the sender side of the queue.
    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Raw-bytes receiver interface for a single named queue.
///
/// Implementations handle fetching serialized bytes from the queue's backing
/// storage.
///
/// # TODO: Acknowledgment Support
///
/// Currently items are auto-acknowledged on receipt. A future iteration will add:
/// - `WorkItem<T>` wrapper with `ack()`, `nack(delay)`, `in_progress()`, `term()` methods
/// - `AckPolicy` configuration: `None` (auto-ack, current) vs `Manual` (explicit ack)
/// - Redelivery for nack'd or timed-out items
pub trait ReceiverBackend: Send + Sync {
    /// Receive the next item. Returns `Ok(None)` when the queue is closed and drained.
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>>;

    /// Receive a batch of items according to the given options.
    ///
    /// Blocks until `batch_size` items are collected **or** `timeout` elapses,
    /// whichever comes first. May return fewer than `batch_size` items,
    /// including an empty `Vec` on timeout.
    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>, WorkQueueRecvError>> + Send + '_>>;

    /// Try to receive without blocking. Returns `Ok(None)` if no items are available.
    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError>;
}
