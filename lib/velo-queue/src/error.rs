// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Error types for velo-queue.

use std::fmt;

/// Error returned when creating or connecting to a named queue.
#[derive(thiserror::Error, Debug)]
pub enum WorkQueueError {
    /// Failed to create or connect to a queue.
    #[error("failed to create queue '{name}': {source}")]
    Creation {
        name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A backend-specific error.
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Error returned by send/enqueue operations.
#[derive(thiserror::Error, Debug)]
pub enum WorkQueueSendError {
    /// The queue has been closed; no more items can be enqueued.
    #[error("queue is closed")]
    Closed,

    /// The queue is full and the operation would block (returned by `try_enqueue`).
    #[error("queue is full")]
    Full,

    /// Serialization of the item failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A backend-specific error.
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Error returned by receive/next operations.
#[derive(thiserror::Error)]
pub enum WorkQueueRecvError {
    /// The queue has been closed and drained.
    #[error("queue is closed")]
    Closed,

    /// Deserialization of the item failed.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// A backend-specific error.
    #[error("backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
}

// Manual Debug impl to avoid requiring Debug on the boxed error source
// while still providing useful output.
impl fmt::Debug for WorkQueueRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "Closed"),
            Self::Deserialization(msg) => f.debug_tuple("Deserialization").field(msg).finish(),
            Self::Backend(e) => f.debug_tuple("Backend").field(&e.to_string()).finish(),
        }
    }
}
