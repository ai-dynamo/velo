// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-memory work queue backend backed by `flume` channels.
//!
//! Named queues are stored in a [`DashMap`]. Calling `sender("foo")` and
//! `receiver("foo")` both create-or-connect to the same underlying channel.
//!
//! Useful for testing and single-process work distribution.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;

use crate::backend::{ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::NextOptions;

/// An in-memory work queue backend.
///
/// Each named queue is a bounded `flume` channel. Multiple senders and
/// receivers can share the same named queue.
pub struct InMemoryBackend {
    queues: DashMap<String, Arc<InMemoryQueue>>,
    capacity: usize,
}

impl InMemoryBackend {
    /// Create a new in-memory backend where each queue has the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            queues: DashMap::new(),
            capacity,
        }
    }

    fn get_or_create(&self, name: &str) -> Arc<InMemoryQueue> {
        self.queues
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(InMemoryQueue::new(self.capacity)))
            .clone()
    }
}

impl WorkQueueBackend for InMemoryBackend {
    fn sender(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn SenderBackend>, WorkQueueError>> + Send + '_>>
    {
        let queue = self.get_or_create(name);
        Box::pin(async move { Ok(queue as Arc<dyn SenderBackend>) })
    }

    fn receiver(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let queue = self.get_or_create(name);
        Box::pin(async move { Ok(queue as Arc<dyn ReceiverBackend>) })
    }
}

/// A single named in-memory queue backed by a bounded flume channel.
struct InMemoryQueue {
    tx: flume::Sender<Bytes>,
    rx: flume::Receiver<Bytes>,
}

impl InMemoryQueue {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        Self { tx, rx }
    }
}

impl SenderBackend for InMemoryQueue {
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>> {
        Box::pin(async move {
            self.tx
                .send_async(data)
                .await
                .map_err(|_| WorkQueueSendError::Closed)
        })
    }

    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError> {
        match self.tx.try_send(data) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(_)) => Err(WorkQueueSendError::Full),
            Err(flume::TrySendError::Disconnected(_)) => Err(WorkQueueSendError::Closed),
        }
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // Dropping all senders closes the channel, but since InMemoryQueue
        // holds both tx and rx we can't truly drop just the sender side.
        // For in-memory, closing is a no-op — the queue drains naturally
        // when all sender handles are dropped.
        Box::pin(async {})
    }
}

impl ReceiverBackend for InMemoryQueue {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        Box::pin(async move {
            match self.rx.recv_async().await {
                Ok(data) => Ok(Some(data)),
                Err(flume::RecvError::Disconnected) => Ok(None),
            }
        })
    }

    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        let batch_size = opts.batch_size;
        let timeout = opts.timeout;
        Box::pin(async move {
            let mut batch = Vec::with_capacity(batch_size);

            // Wait for the first item (or timeout).
            match tokio::time::timeout(timeout, self.rx.recv_async()).await {
                Ok(Ok(data)) => batch.push(data),
                Ok(Err(flume::RecvError::Disconnected)) => return Ok(batch),
                Err(_timeout) => return Ok(batch),
            }

            // Try to fill the rest of the batch without blocking.
            while batch.len() < batch_size {
                match self.rx.try_recv() {
                    Ok(data) => batch.push(data),
                    Err(flume::TryRecvError::Empty | flume::TryRecvError::Disconnected) => break,
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        match self.rx.try_recv() {
            Ok(data) => Ok(Some(data)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_send_recv() {
        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("test").await.unwrap();
        let receiver = backend.receiver("test").await.unwrap();

        let data = Bytes::from_static(b"hello");
        sender.send(data.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received, Some(data));
    }

    #[tokio::test]
    async fn test_try_send_full() {
        let backend = InMemoryBackend::new(1);
        let sender = backend.sender("test").await.unwrap();

        sender.send(Bytes::from_static(b"first")).await.unwrap();

        let result = sender.try_send(Bytes::from_static(b"second"));
        assert!(matches!(result, Err(WorkQueueSendError::Full)));
    }

    #[tokio::test]
    async fn test_try_recv_empty() {
        let backend = InMemoryBackend::new(16);
        let receiver = backend.receiver("test").await.unwrap();

        let result = receiver.try_recv().unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_named_queues_are_independent() {
        let backend = InMemoryBackend::new(16);

        let sender_a = backend.sender("a").await.unwrap();
        let sender_b = backend.sender("b").await.unwrap();
        let receiver_a = backend.receiver("a").await.unwrap();
        let receiver_b = backend.receiver("b").await.unwrap();

        sender_a.send(Bytes::from_static(b"aa")).await.unwrap();
        sender_b.send(Bytes::from_static(b"bb")).await.unwrap();

        assert_eq!(
            receiver_a.recv().await.unwrap(),
            Some(Bytes::from_static(b"aa"))
        );
        assert_eq!(
            receiver_b.recv().await.unwrap(),
            Some(Bytes::from_static(b"bb"))
        );
    }

    #[tokio::test]
    async fn test_same_name_shares_channel() {
        let backend = InMemoryBackend::new(16);

        let sender = backend.sender("shared").await.unwrap();
        let receiver = backend.receiver("shared").await.unwrap();

        sender.send(Bytes::from_static(b"data")).await.unwrap();
        assert_eq!(
            receiver.recv().await.unwrap(),
            Some(Bytes::from_static(b"data"))
        );
    }

    #[tokio::test]
    async fn test_batch_recv() {
        use std::time::Duration;

        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("batch").await.unwrap();
        let receiver = backend.receiver("batch").await.unwrap();

        for i in 0u8..5 {
            sender.send(Bytes::from(vec![i])).await.unwrap();
        }

        let opts = NextOptions::new()
            .batch_size(3)
            .timeout(Duration::from_millis(100));

        let batch = receiver.recv_batch(&opts).await.unwrap();
        assert_eq!(batch.len(), 3);
    }

    #[tokio::test]
    async fn test_batch_recv_timeout_partial() {
        use std::time::Duration;

        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("batch-partial").await.unwrap();
        let receiver = backend.receiver("batch-partial").await.unwrap();

        // Only send 2 items but request batch of 5
        sender.send(Bytes::from_static(b"a")).await.unwrap();
        sender.send(Bytes::from_static(b"b")).await.unwrap();

        let opts = NextOptions::new()
            .batch_size(5)
            .timeout(Duration::from_millis(50));

        let batch = receiver.recv_batch(&opts).await.unwrap();
        // Should get 2 items (all available) before timeout
        assert_eq!(batch.len(), 2);
    }
}
