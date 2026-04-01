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
//! use velo_queue::{sender, receiver, backends::memory::InMemoryBackend};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct Job { id: u64, payload: String }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = InMemoryBackend::new(1024);
//!
//! let tx = sender::<Job>(&backend, "my-jobs").await?;
//! let rx = receiver::<Job>(&backend, "my-jobs").await?;
//!
//! tx.enqueue(&Job { id: 1, payload: "work".into() }).await?;
//! let job = rx.next().await?.unwrap();
//! assert_eq!(job.id, 1);
//! # Ok(())
//! # }
//! ```
//!
//! ## TODO: Acknowledgment Support
//!
//! Currently items are auto-acknowledged on receipt. A future iteration will add:
//! - `WorkItem<T>` wrapper with `ack()`, `nack(delay)`, `in_progress()`, `term()`
//! - `AckPolicy` config: `None` (auto-ack) vs `Manual` (explicit acknowledgment)
//! - Redelivery for nack'd or timed-out items

pub mod backend;
pub mod backends;
pub mod error;
pub mod options;
pub mod receiver;
pub mod sender;

// Re-export primary types at crate root for convenience.
pub use backend::{ReceiverBackend, SenderBackend, WorkQueueBackend};
pub use error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
pub use options::NextOptions;
pub use receiver::WorkQueueReceiver;
pub use sender::WorkQueueSender;

/// Create a typed sender for a named queue from the given backend.
pub async fn sender<T: serde::Serialize>(
    backend: &dyn WorkQueueBackend,
    name: &str,
) -> Result<WorkQueueSender<T>, WorkQueueError> {
    let raw = backend.sender(name).await?;
    Ok(WorkQueueSender::new(raw))
}

/// Create a typed receiver for a named queue from the given backend.
pub async fn receiver<T: serde::de::DeserializeOwned>(
    backend: &dyn WorkQueueBackend,
    name: &str,
) -> Result<WorkQueueReceiver<T>, WorkQueueError> {
    let raw = backend.receiver(name).await?;
    Ok(WorkQueueReceiver::new(raw))
}
