// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-memory work queue backend backed by `flume` channels.
//!
//! Named queues are stored in a [`DashMap`]. Calling `sender("foo")` and
//! `receiver("foo")` both create-or-connect to the same underlying channel.
//!
//! Supports both [`AckPolicy::Auto`] and [`AckPolicy::Manual`]. Under Manual,
//! each received item holds a lease tracked by a per-lease reaper task that
//! redelivers on visibility-timeout expiry or dropped-without-ack.
//!
//! Useful for testing and single-process work distribution.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;

use crate::queue::ack::{AckFuture, AckHandle, AckHandleInner};
use crate::queue::backend::{DeliveredMessage, ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::queue::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::queue::options::{AckPolicy, NextOptions};

/// Default visibility timeout for manually-acked in-flight items.
pub const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

/// An in-memory work queue backend.
///
/// Each named queue is a bounded `flume` channel. Multiple senders and
/// receivers can share the same named queue.
pub struct InMemoryBackend {
    queues: DashMap<String, Arc<InMemoryQueue>>,
    capacity: usize,
    visibility_timeout: Duration,
    /// `None` = unbounded DLQ (default); `Some(n)` = bounded.
    dlq_capacity: Option<usize>,
}

impl InMemoryBackend {
    /// Create a new in-memory backend where each queue has the given capacity.
    ///
    /// Uses [`DEFAULT_VISIBILITY_TIMEOUT`] for manually-acked items. The
    /// dead-letter queue is unbounded by default; opt in to a bounded DLQ via
    /// [`with_dlq_capacity`](Self::with_dlq_capacity).
    pub fn new(capacity: usize) -> Self {
        Self::with_visibility_timeout(capacity, DEFAULT_VISIBILITY_TIMEOUT)
    }

    /// Create a new in-memory backend with a custom visibility timeout.
    pub fn with_visibility_timeout(capacity: usize, visibility_timeout: Duration) -> Self {
        Self {
            queues: DashMap::new(),
            capacity,
            visibility_timeout,
            dlq_capacity: None,
        }
    }

    /// Bound the dead-letter queue to `dlq_capacity` items.
    ///
    /// Defaults to unbounded. When bounded and full, reaper tasks handling
    /// `term()` calls back-pressure via `send_async` until a consumer drains
    /// via [`dlq_receiver`](Self::dlq_receiver). The user-facing `term()`
    /// call is not affected — it returns once the control signal reaches the
    /// reaper; the DLQ insertion happens off the hot path.
    ///
    /// Only the reaper for the offending lease blocks; other leases and the
    /// main queue flow are unaffected.
    pub fn with_dlq_capacity(mut self, dlq_capacity: usize) -> Self {
        self.dlq_capacity = Some(dlq_capacity);
        self
    }

    fn get_or_create(&self, name: &str) -> Arc<InMemoryQueue> {
        self.queues
            .entry(name.to_owned())
            .or_insert_with(|| {
                Arc::new(InMemoryQueue::new(
                    self.capacity,
                    self.visibility_timeout,
                    self.dlq_capacity,
                ))
            })
            .clone()
    }

    /// Get a receiver for the dead-letter queue associated with `name`.
    ///
    /// Items terminated via [`WorkItem::term`](crate::WorkItem::term) appear here.
    /// Returns `None` if no queue named `name` exists yet.
    ///
    /// The DLQ is unbounded by default; see
    /// [`with_dlq_capacity`](Self::with_dlq_capacity) to bound it.
    pub fn dlq_receiver(&self, name: &str) -> Option<flume::Receiver<Bytes>> {
        self.queues.get(name).map(|q| q.dlq_rx.clone())
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
        policy: AckPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let queue = self.get_or_create(name);
        Box::pin(async move {
            Ok(Arc::new(InMemoryReceiver { queue, policy }) as Arc<dyn ReceiverBackend>)
        })
    }
}

/// A single named in-memory queue backed by a bounded flume channel.
pub(crate) struct InMemoryQueue {
    tx: flume::Sender<Bytes>,
    rx: flume::Receiver<Bytes>,
    /// Per-lease control channels — signal Ack/Nack/Progress/Term to a lease's reaper.
    leases: DashMap<u64, flume::Sender<LeaseSignal>>,
    next_lease_id: AtomicU64,
    visibility_timeout: Duration,
    /// Dead-letter channel — items terminated via `term()` are pushed here.
    dlq_tx: flume::Sender<Bytes>,
    dlq_rx: flume::Receiver<Bytes>,
}

impl InMemoryQueue {
    fn new(capacity: usize, visibility_timeout: Duration, dlq_capacity: Option<usize>) -> Self {
        let (tx, rx) = flume::bounded(capacity);
        let (dlq_tx, dlq_rx) = match dlq_capacity {
            Some(c) => flume::bounded(c),
            None => flume::unbounded(),
        };
        Self {
            tx,
            rx,
            leases: DashMap::new(),
            next_lease_id: AtomicU64::new(1),
            visibility_timeout,
            dlq_tx,
            dlq_rx,
        }
    }

    /// Pop an item from the queue, registering a lease under Manual policy.
    ///
    /// Under Manual policy this requires an active tokio runtime to spawn the
    /// reaper. If no runtime is available (e.g., `try_recv` called from a
    /// non-tokio thread), the payload is best-effort re-enqueued and an error
    /// is returned — the caller is told explicitly rather than handed a dead
    /// handle that silently swallows ack/nack/term calls.
    fn pop_with_policy(
        self: &Arc<Self>,
        payload: Bytes,
        policy: AckPolicy,
    ) -> Result<DeliveredMessage, WorkQueueRecvError> {
        match policy {
            AckPolicy::Auto => Ok(DeliveredMessage::auto(payload)),
            AckPolicy::Manual => {
                // Manual requires a runtime. Detect early and bail out with the
                // payload re-enqueued, rather than handing back a handle whose
                // control channel has no listener.
                let rt = match tokio::runtime::Handle::try_current() {
                    Ok(rt) => rt,
                    Err(_) => {
                        // Blocking send: waits for a slot if a concurrent
                        // producer raced us into the bounded channel. try_send
                        // would return Err(Full) and silently drop the
                        // payload. send only fails if the channel is closed,
                        // which can't happen here (InMemoryQueue holds both
                        // tx and rx) outside of teardown.
                        let payload_back = self.tx.send(payload).is_ok();
                        let msg = if payload_back {
                            "AckPolicy::Manual requires an active tokio runtime; payload re-enqueued"
                        } else {
                            "AckPolicy::Manual requires an active tokio runtime; payload lost (queue closed)"
                        };
                        return Err(WorkQueueRecvError::Backend(msg.into()));
                    }
                };

                let lease_id = self.next_lease_id.fetch_add(1, Ordering::Relaxed);
                let (control_tx, control_rx) = flume::bounded(1);
                self.leases.insert(lease_id, control_tx.clone());

                // Spawn reaper. Clone payload so the reaper owns the redelivery copy.
                let queue = Arc::clone(self);
                let reaper_payload = payload.clone();
                let timeout = self.visibility_timeout;
                rt.spawn(async move {
                    lease_reaper(queue, lease_id, reaper_payload, control_rx, timeout).await;
                });

                let handle = AckHandle::new(MemoryAckHandle {
                    lease_id,
                    control: control_tx,
                });
                Ok(DeliveredMessage::manual(payload, handle))
            }
        }
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

/// Receiver wrapping an `Arc<InMemoryQueue>` with a fixed policy.
struct InMemoryReceiver {
    queue: Arc<InMemoryQueue>,
    policy: AckPolicy,
}

impl ReceiverBackend for InMemoryReceiver {
    fn recv(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>,
    > {
        Box::pin(async move {
            match self.queue.rx.recv_async().await {
                Ok(data) => Ok(Some(self.queue.pop_with_policy(data, self.policy)?)),
                Err(flume::RecvError::Disconnected) => Ok(None),
            }
        })
    }

    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>>
    {
        let batch_size = opts.batch_size;
        let timeout = opts.timeout;
        Box::pin(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let deadline = tokio::time::Instant::now() + timeout;

            while batch.len() < batch_size {
                // Try non-blocking drain first.
                match self.queue.rx.try_recv() {
                    Ok(data) => {
                        batch.push(self.queue.pop_with_policy(data, self.policy)?);
                        continue;
                    }
                    Err(flume::TryRecvError::Disconnected) => return Ok(batch),
                    Err(flume::TryRecvError::Empty) => {}
                }

                // Wait for the next item until the deadline.
                match tokio::time::timeout_at(deadline, self.queue.rx.recv_async()).await {
                    Ok(Ok(data)) => batch.push(self.queue.pop_with_policy(data, self.policy)?),
                    Ok(Err(flume::RecvError::Disconnected)) => return Ok(batch),
                    Err(_timeout) => return Ok(batch),
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<DeliveredMessage>, WorkQueueRecvError> {
        match self.queue.rx.try_recv() {
            Ok(data) => Ok(Some(self.queue.pop_with_policy(data, self.policy)?)),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Ok(None),
        }
    }
}

/// Signal sent from a [`MemoryAckHandle`] to the reaper task.
enum LeaseSignal {
    Ack,
    Nack(Duration),
    Progress,
    Term,
}

/// Per-lease reaper task. Owns the redelivery payload until resolution.
async fn lease_reaper(
    queue: Arc<InMemoryQueue>,
    lease_id: u64,
    payload: Bytes,
    control: flume::Receiver<LeaseSignal>,
    timeout: Duration,
) {
    let mut deadline = tokio::time::Instant::now() + timeout;
    loop {
        tokio::select! {
            biased;
            signal = control.recv_async() => {
                match signal {
                    Ok(LeaseSignal::Ack) => {
                        queue.leases.remove(&lease_id);
                        return;
                    }
                    Ok(LeaseSignal::Nack(delay)) => {
                        queue.leases.remove(&lease_id);
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        // Ignore send errors — queue closed means nothing to redeliver to.
                        let _ = queue.tx.send_async(payload).await;
                        return;
                    }
                    Ok(LeaseSignal::Progress) => {
                        deadline = tokio::time::Instant::now() + timeout;
                    }
                    Ok(LeaseSignal::Term) => {
                        queue.leases.remove(&lease_id);
                        let _ = queue.dlq_tx.send_async(payload).await;
                        return;
                    }
                    Err(_) => {
                        // Control channel closed without a signal — the AckHandle was
                        // dropped. Drop semantics on WorkItem should have sent Nack first,
                        // but if we got here redeliver via timeout path.
                        queue.leases.remove(&lease_id);
                        let _ = queue.tx.send_async(payload).await;
                        return;
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                // Visibility timeout — redeliver.
                queue.leases.remove(&lease_id);
                let _ = queue.tx.send_async(payload).await;
                return;
            }
        }
    }
}

/// In-memory ack handle. Sends signals to a per-lease reaper task.
struct MemoryAckHandle {
    lease_id: u64,
    control: flume::Sender<LeaseSignal>,
}

impl MemoryAckHandle {
    fn send_signal(&self, signal: LeaseSignal) {
        // send_async takes a Sender<T> by reference implicitly via try_send;
        // a closed reaper (already resolved) is not an error — just log.
        if self.control.try_send(signal).is_err() {
            tracing::debug!(
                "MemoryAckHandle: lease {} already resolved (reaper task exited)",
                self.lease_id
            );
        }
    }
}

impl AckHandleInner for MemoryAckHandle {
    fn ack(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move {
            self.send_signal(LeaseSignal::Ack);
            Ok(())
        })
    }

    fn nack(self: Box<Self>, delay: Duration) -> AckFuture<'static> {
        Box::pin(async move {
            self.send_signal(LeaseSignal::Nack(delay));
            Ok(())
        })
    }

    fn in_progress(&self) -> AckFuture<'_> {
        Box::pin(async move {
            self.send_signal(LeaseSignal::Progress);
            Ok(())
        })
    }

    fn term(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move {
            self.send_signal(LeaseSignal::Term);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn auto_receiver(backend: &InMemoryBackend, name: &str) -> Arc<dyn ReceiverBackend> {
        backend
            .receiver(name, AckPolicy::Auto)
            .await
            .expect("receiver")
    }

    fn bytes(msg: Option<DeliveredMessage>) -> Option<Bytes> {
        msg.map(|m| m.bytes)
    }

    #[tokio::test]
    async fn test_basic_send_recv() {
        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("test").await.unwrap();
        let receiver = auto_receiver(&backend, "test").await;

        let data = Bytes::from_static(b"hello");
        sender.send(data.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(bytes(received), Some(data));
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
        let receiver = auto_receiver(&backend, "test").await;

        let result = receiver.try_recv().unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_named_queues_are_independent() {
        let backend = InMemoryBackend::new(16);

        let sender_a = backend.sender("a").await.unwrap();
        let sender_b = backend.sender("b").await.unwrap();
        let receiver_a = auto_receiver(&backend, "a").await;
        let receiver_b = auto_receiver(&backend, "b").await;

        sender_a.send(Bytes::from_static(b"aa")).await.unwrap();
        sender_b.send(Bytes::from_static(b"bb")).await.unwrap();

        assert_eq!(
            bytes(receiver_a.recv().await.unwrap()),
            Some(Bytes::from_static(b"aa"))
        );
        assert_eq!(
            bytes(receiver_b.recv().await.unwrap()),
            Some(Bytes::from_static(b"bb"))
        );
    }

    #[tokio::test]
    async fn test_same_name_shares_channel() {
        let backend = InMemoryBackend::new(16);

        let sender = backend.sender("shared").await.unwrap();
        let receiver = auto_receiver(&backend, "shared").await;

        sender.send(Bytes::from_static(b"data")).await.unwrap();
        assert_eq!(
            bytes(receiver.recv().await.unwrap()),
            Some(Bytes::from_static(b"data"))
        );
    }

    #[tokio::test]
    async fn test_batch_recv() {
        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("batch").await.unwrap();
        let receiver = auto_receiver(&backend, "batch").await;

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
        let backend = InMemoryBackend::new(16);
        let sender = backend.sender("batch-partial").await.unwrap();
        let receiver = auto_receiver(&backend, "batch-partial").await;

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
