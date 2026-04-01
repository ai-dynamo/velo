// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed work queue sender.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;

use crate::backend::SenderBackend;
use crate::error::WorkQueueSendError;

/// A typed sender handle for a named work queue.
///
/// Serializes items of type `T` using MessagePack and delegates to the
/// underlying [`SenderBackend`].
///
/// `WorkQueueSender` is cheaply cloneable (the backend is behind an `Arc`).
pub struct WorkQueueSender<T: Serialize> {
    backend: Arc<dyn SenderBackend>,
    _marker: PhantomData<fn(T)>,
}

impl<T: Serialize> Clone for WorkQueueSender<T> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            _marker: PhantomData,
        }
    }
}

impl<T: Serialize> WorkQueueSender<T> {
    /// Create a new sender wrapping the given backend.
    pub(crate) fn new(backend: Arc<dyn SenderBackend>) -> Self {
        Self {
            backend,
            _marker: PhantomData,
        }
    }

    /// Enqueue an item. Waits if the backend applies backpressure.
    pub async fn enqueue(&self, data: &T) -> Result<(), WorkQueueSendError> {
        let bytes = serialize(data)?;
        self.backend.send(bytes).await
    }

    /// Try to enqueue an item without blocking.
    ///
    /// Returns [`WorkQueueSendError::Full`] if the queue is at capacity.
    pub fn try_enqueue(&self, data: &T) -> Result<(), WorkQueueSendError> {
        let bytes = serialize(data)?;
        self.backend.try_send(bytes)
    }

    /// Request that the sender side be closed.
    ///
    /// This delegates to the underlying [`SenderBackend`] and the exact
    /// effect is backend-dependent. Callers should treat the sender as
    /// logically closed and avoid further enqueue operations after calling
    /// this method, but backends may not be able to *strictly* prevent
    /// additional sends in all cases.
    pub async fn close(&self) {
        self.backend.close().await;
    }
}

fn serialize<T: Serialize>(data: &T) -> Result<Bytes, WorkQueueSendError> {
    rmp_serde::to_vec(data)
        .map(Bytes::from)
        .map_err(|e| WorkQueueSendError::Serialization(e.to_string()))
}
