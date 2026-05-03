// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # velo-queue
//!
//! Named work queue abstraction for Velo distributed systems.
//!
//! A work queue is a named entity that you create or connect to, then get
//! typed sender/receiver handles for enqueuing and consuming work items.
//!
//! ## Backends
//!
//! | Backend | Feature | Description |
//! |---------|---------|-------------|
//! | [`InMemoryBackend`](backends::memory::InMemoryBackend) | *(always)* | `DashMap` + `flume` channels, for testing |
//! | [`MessengerQueueBackend`](backends::messenger::MessengerQueueBackend) | `messenger` | Actor on a velo instance via active messages |
//! | [`NatsQueueBackend`](backends::nats::NatsQueueBackend) | `nats` | NATS JetStream with WorkQueue retention |
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use serde::{Serialize, Deserialize};
//! use velo::queue::{sender, receiver_auto, backends::memory::InMemoryBackend};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct Job { id: u64, payload: String }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = InMemoryBackend::new(1024);
//!
//! let tx = sender::<Job>(&backend, "my-jobs").await?;
//! let rx = receiver_auto::<Job>(&backend, "my-jobs").await?;
//!
//! tx.enqueue(&Job { id: 1, payload: "work".into() }).await?;
//! let item = rx.next().await?.unwrap();
//! assert_eq!(item.id, 1);
//! // Auto-ack: the item is already gone from the queue.
//! # Ok(())
//! # }
//! ```
//!
//! ## Acknowledgments
//!
//! By default ([`AckPolicy::Auto`]) items are acknowledged on receipt — if a
//! worker crashes mid-job, the item is lost. For at-least-once processing,
//! create the receiver with [`AckPolicy::Manual`] and acknowledge each item
//! explicitly:
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use serde::{Serialize, Deserialize};
//! use velo::queue::{sender, receiver, AckPolicy, backends::memory::InMemoryBackend};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Job { id: u64 }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = InMemoryBackend::new(1024);
//! let rx = receiver::<Job>(&backend, "my-jobs", AckPolicy::Manual).await?;
//!
//! let item = rx.next().await?.unwrap();
//! match do_work(&item).await {
//!     Ok(()) => item.ack().await?,
//!     Err(_) => item.nack(Duration::from_secs(5)).await?,
//! }
//! # Ok(())
//! # }
//! # async fn do_work<T>(_: &T) -> Result<(), ()> { Ok(()) }
//! ```
//!
//! Nack'd items are redelivered after the delay. Items that time out
//! (visibility timeout, default 30s) or whose [`WorkItem`] is dropped without
//! an explicit outcome are also redelivered.

pub mod ack;
pub mod backend;
pub mod backends;
pub mod error;
pub mod options;
pub mod receiver;
pub mod sender;
pub mod work_item;

// Re-export primary types at crate root for convenience.
pub use ack::{AckHandle, AckHandleInner};
pub use backend::{DeliveredMessage, ReceiverBackend, SenderBackend, WorkQueueBackend};
pub use error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
pub use options::{AckPolicy, NextOptions};
pub use receiver::WorkQueueReceiver;
pub use sender::WorkQueueSender;
pub use work_item::WorkItem;

/// Create a typed sender for a named queue from the given backend.
pub async fn sender<T: serde::Serialize>(
    backend: &dyn WorkQueueBackend,
    name: &str,
) -> Result<WorkQueueSender<T>, WorkQueueError> {
    let raw = backend.sender(name).await?;
    Ok(WorkQueueSender::new(raw))
}

/// Create a typed receiver for a named queue with the given acknowledgment policy.
pub async fn receiver<T: serde::de::DeserializeOwned>(
    backend: &dyn WorkQueueBackend,
    name: &str,
    policy: AckPolicy,
) -> Result<WorkQueueReceiver<T>, WorkQueueError> {
    let raw = backend.receiver(name, policy).await?;
    Ok(WorkQueueReceiver::new(raw))
}

/// Create a typed receiver with [`AckPolicy::Auto`] — items are acked on receipt.
///
/// Convenience shortcut for the common case. For at-least-once processing use
/// [`receiver`] with [`AckPolicy::Manual`].
pub async fn receiver_auto<T: serde::de::DeserializeOwned>(
    backend: &dyn WorkQueueBackend,
    name: &str,
) -> Result<WorkQueueReceiver<T>, WorkQueueError> {
    receiver(backend, name, AckPolicy::Auto).await
}
