// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Typed work queue receiver.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use serde::de::DeserializeOwned;

use crate::queue::backend::{DeliveredMessage, ReceiverBackend};
use crate::queue::error::WorkQueueRecvError;
use crate::queue::options::NextOptions;
use crate::queue::work_item::WorkItem;

/// A typed receiver handle for a named work queue.
///
/// Deserializes items of type `T` from MessagePack bytes and returns them
/// wrapped in [`WorkItem<T>`]. Under
/// [`AckPolicy::Auto`](crate::queue::AckPolicy::Auto) the wrapper is effectively
/// transparent (drop is a no-op); under
/// [`AckPolicy::Manual`](crate::queue::AckPolicy::Manual) the caller must call
/// [`WorkItem::ack`], [`nack`](WorkItem::nack), or [`term`](WorkItem::term)
/// on each item.
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
    pub async fn next(&self) -> Result<Option<WorkItem<T>>, WorkQueueRecvError> {
        match self.backend.recv().await? {
            Some(msg) => Ok(Some(build_work_item(msg)?)),
            None => Ok(None),
        }
    }

    /// Get the next batch of items according to the given options.
    ///
    /// Blocks until `batch_size` items are collected **or** `timeout` elapses,
    /// whichever comes first. May return fewer than `batch_size` items,
    /// including an empty `Vec`, for example when the timeout expires or when
    /// the queue has been closed and fully drained. Callers MUST NOT treat an
    /// empty batch as a definitive signal that the queue is closed.
    pub async fn next_with_options(
        &self,
        options: NextOptions,
    ) -> Result<Vec<WorkItem<T>>, WorkQueueRecvError> {
        let raw = self.backend.recv_batch(&options).await?;
        raw.into_iter().map(build_work_item).collect()
    }

    /// Try to get the next item without blocking.
    ///
    /// Returns `Ok(None)` if no items are currently available.
    pub fn try_next(&self) -> Result<Option<WorkItem<T>>, WorkQueueRecvError> {
        match self.backend.try_recv()? {
            Some(msg) => Ok(Some(build_work_item(msg)?)),
            None => Ok(None),
        }
    }
}

/// Deserialize the payload and wrap into a `WorkItem`. On deserialization
/// failure under Manual policy, fire-and-forget a term on the handle so the
/// poison bytes don't loop back via the implicit nack-on-drop path.
fn build_work_item<T: DeserializeOwned>(
    msg: DeliveredMessage,
) -> Result<WorkItem<T>, WorkQueueRecvError> {
    match deserialize(&msg.bytes) {
        Ok(value) => Ok(WorkItem::new(value, msg.handle)),
        Err(e) => {
            // Under Auto, msg.handle is None and this branch is skipped.
            // Under Manual, term removes the message so it isn't redelivered;
            // best-effort spawn so we don't block the caller's error path.
            if let Some(handle) = msg.handle
                && let Ok(rt) = tokio::runtime::Handle::try_current()
            {
                rt.spawn(async move {
                    let _ = handle.term().await;
                });
            }
            Err(e)
        }
    }
}

fn deserialize<T: DeserializeOwned>(bytes: &Bytes) -> Result<T, WorkQueueRecvError> {
    rmp_serde::from_slice(bytes).map_err(|e| WorkQueueRecvError::Deserialization(e.to_string()))
}
