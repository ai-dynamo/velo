// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed work queue receiver.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use serde::de::DeserializeOwned;

use crate::backend::ReceiverBackend;
use crate::error::WorkQueueRecvError;
use crate::options::NextOptions;

/// A typed receiver handle for a named work queue.
///
/// Deserializes items of type `T` from MessagePack bytes received from the
/// underlying [`ReceiverBackend`].
///
/// # TODO: Acknowledgment Support
///
/// Currently items are auto-acknowledged on receipt. A future version will
/// return `WorkItem<T>` wrappers with explicit `ack()` / `nack()` /
/// `in_progress()` methods, controlled by an `AckPolicy` configuration.
pub struct WorkQueueReceiver<T: DeserializeOwned> {
    backend: Arc<dyn ReceiverBackend>,
    _marker: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> Clone for WorkQueueReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            _marker: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> WorkQueueReceiver<T> {
    /// Create a new receiver wrapping the given backend.
    pub(crate) fn new(backend: Arc<dyn ReceiverBackend>) -> Self {
        Self {
            backend,
            _marker: PhantomData,
        }
    }

    /// Get the next item, blocking until one is available.
    ///
    /// Returns `Ok(None)` when the queue is closed and fully drained.
    pub async fn next(&self) -> Result<Option<T>, WorkQueueRecvError> {
        match self.backend.recv().await? {
            Some(bytes) => Ok(Some(deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Get the next batch of items according to the given options.
    ///
    /// Blocks until `batch_size` items are collected **or** `timeout` elapses,
    /// whichever comes first. Returns an empty `Vec` only when the queue is
    /// closed and drained.
    pub async fn next_with_options(
        &self,
        options: NextOptions,
    ) -> Result<Vec<T>, WorkQueueRecvError> {
        let raw = self.backend.recv_batch(&options).await?;
        raw.iter().map(|b| deserialize(b)).collect()
    }

    /// Try to get the next item without blocking.
    ///
    /// Returns `Ok(None)` if no items are currently available.
    pub fn try_next(&self) -> Result<Option<T>, WorkQueueRecvError> {
        match self.backend.try_recv()? {
            Some(bytes) => Ok(Some(deserialize(&bytes)?)),
            None => Ok(None),
        }
    }
}

fn deserialize<T: DeserializeOwned>(bytes: &Bytes) -> Result<T, WorkQueueRecvError> {
    rmp_serde::from_slice(bytes).map_err(|e| WorkQueueRecvError::Deserialization(e.to_string()))
}
