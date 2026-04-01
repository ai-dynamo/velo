// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Velo-messenger-based work queue backend.
//!
//! Uses an **actor pattern**: the receiver side registers handlers on a target
//! velo instance that hold an internal buffer (`VecDeque`). Senders enqueue
//! items by sending active messages to the actor; receivers pull items via
//! typed unary requests.
//!
//! ## Handler Names
//!
//! For a queue named `"my-queue"`, the following handlers are registered:
//! - `queue.my-queue.enqueue` — fire-and-forget AM handler that pushes to the deque
//! - `queue.my-queue.next` — typed unary handler that pops from the deque and returns

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Notify;
use velo_common::InstanceId;
use velo_messenger::Messenger;

use crate::backend::{ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::NextOptions;

/// Configuration for the messenger queue backend.
#[derive(Default)]
pub struct MessengerQueueConfig {
    /// Capacity of the internal buffer on the actor side.
    /// If `None`, the buffer is unbounded.
    pub capacity: Option<usize>,
}

/// A work queue backend that uses velo-messenger active messages.
///
/// The queue actor lives on `target_instance`. Senders route `am_send` messages
/// to it; receivers send `typed_unary` requests to pull items.
pub struct MessengerQueueBackend {
    messenger: Arc<Messenger>,
    target_instance: InstanceId,
    config: MessengerQueueConfig,
    /// Tracks which queue names have had their actor handlers registered on
    /// this instance (to avoid double-registration).
    registered_actors: DashMap<String, Arc<QueueActor>>,
}

impl MessengerQueueBackend {
    /// Create a new messenger queue backend.
    ///
    /// - `messenger`: the local messenger instance
    /// - `target_instance`: the instance where queue actors live (can be self)
    pub fn new(
        messenger: Arc<Messenger>,
        target_instance: InstanceId,
        config: MessengerQueueConfig,
    ) -> Self {
        Self {
            messenger,
            target_instance,
            config,
            registered_actors: DashMap::new(),
        }
    }

    /// Get or create the local actor for a queue name.
    ///
    /// If the target instance is the local messenger, this registers the
    /// handler and creates the backing buffer. If the target is remote,
    /// we assume the actor already exists there.
    fn get_or_create_actor(&self, name: &str) -> Result<Arc<QueueActor>, WorkQueueError> {
        // Use entry API to avoid TOCTOU race on concurrent calls for the same name.
        // The `or_try_insert_with` closure runs exactly once per name.
        let entry = self.registered_actors.entry(name.to_owned());
        match entry {
            dashmap::Entry::Occupied(e) => Ok(e.get().clone()),
            dashmap::Entry::Vacant(e) => {
                let actor = Arc::new(QueueActor::new(self.config.capacity));
                let enqueue_name = format!("queue.{name}.enqueue");
                let next_name = format!("queue.{name}.next");

                // Register the enqueue handler (fire-and-forget)
                let actor_for_enqueue = Arc::clone(&actor);
                let enqueue_handler =
                    velo_messenger::Handler::am_handler_async(enqueue_name, move |ctx| {
                        let actor = Arc::clone(&actor_for_enqueue);
                        async move {
                            actor.push(ctx.payload);
                            Ok(())
                        }
                    })
                    .spawn()
                    .build();

                self.messenger
                    .register_handler(enqueue_handler)
                    .map_err(|e| WorkQueueError::Creation {
                        name: name.to_owned(),
                        source: e.into(),
                    })?;

                // Register the next handler (unary, returns raw bytes or empty)
                let actor_for_next = Arc::clone(&actor);
                let next_handler =
                    velo_messenger::Handler::unary_handler_async(next_name, move |_ctx| {
                        let actor = Arc::clone(&actor_for_next);
                        async move { Ok(actor.pop()) }
                    })
                    .spawn()
                    .build();

                self.messenger.register_handler(next_handler).map_err(|e| {
                    WorkQueueError::Creation {
                        name: name.to_owned(),
                        source: e.into(),
                    }
                })?;

                e.insert(Arc::clone(&actor));
                Ok(actor)
            }
        }
    }
}

impl WorkQueueBackend for MessengerQueueBackend {
    fn sender(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn SenderBackend>, WorkQueueError>> + Send + '_>>
    {
        let result = (|| {
            // If we are the target, ensure the actor is registered locally.
            if self.target_instance == self.messenger.instance_id() {
                self.get_or_create_actor(name)?;
            }

            Ok(Arc::new(MessengerSender {
                messenger: Arc::clone(&self.messenger),
                target: self.target_instance,
                enqueue_handler: format!("queue.{name}.enqueue"),
            }) as Arc<dyn SenderBackend>)
        })();
        Box::pin(async move { result })
    }

    fn receiver(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let result = (|| {
            // For local target, register the actor and return a local receiver
            // that pulls directly from the deque (avoiding network round-trips).
            if self.target_instance == self.messenger.instance_id() {
                let actor = self.get_or_create_actor(name)?;
                return Ok(Arc::new(LocalMessengerReceiver { actor }) as Arc<dyn ReceiverBackend>);
            }

            // For remote target, use typed_unary requests to pull items.
            Ok(Arc::new(RemoteMessengerReceiver {
                messenger: Arc::clone(&self.messenger),
                target: self.target_instance,
                next_handler: format!("queue.{name}.next"),
            }) as Arc<dyn ReceiverBackend>)
        })();
        Box::pin(async move { result })
    }
}

// ============================================================================
// Queue Actor — the in-process buffer that backs a single named queue
// ============================================================================

struct QueueActor {
    buffer: parking_lot::Mutex<VecDeque<Bytes>>,
    capacity: Option<usize>,
    notify: Notify,
}

impl QueueActor {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            buffer: parking_lot::Mutex::new(VecDeque::new()),
            capacity,
            notify: Notify::new(),
        }
    }

    fn push(&self, data: Bytes) {
        let mut buf = self.buffer.lock();
        if let Some(cap) = self.capacity
            && buf.len() >= cap
        {
            // Drop oldest if at capacity (work queue semantics: prefer
            // freshness over completeness). Callers that need backpressure
            // should use bounded in-memory or NATS backends instead.
            buf.pop_front();
        }
        buf.push_back(data);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<Bytes> {
        self.buffer.lock().pop_front()
    }

    async fn pop_async(&self) -> Option<Bytes> {
        loop {
            if let Some(data) = self.pop() {
                return Some(data);
            }
            self.notify.notified().await;
        }
    }
}

// ============================================================================
// Sender: sends AM messages to the target instance's enqueue handler
// ============================================================================

struct MessengerSender {
    messenger: Arc<Messenger>,
    target: InstanceId,
    enqueue_handler: String,
}

impl SenderBackend for MessengerSender {
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>> {
        Box::pin(async move {
            self.messenger
                .am_send(&self.enqueue_handler)
                .map_err(|e| WorkQueueSendError::Backend(e.into()))?
                .raw_payload(data)
                .send_to(self.target)
                .await
                .map_err(|e| WorkQueueSendError::Backend(e.into()))
        })
    }

    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError> {
        // For messenger, try_send is best-effort async. We spawn the actual
        // send on the runtime if one is available.
        let messenger = Arc::clone(&self.messenger);
        let target = self.target;
        let handler = self.enqueue_handler.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let res: Result<(), WorkQueueSendError> = async {
                    messenger
                        .am_send(&handler)
                        .map_err(|e| WorkQueueSendError::Backend(e.into()))?
                        .raw_payload(data)
                        .send_to(target)
                        .await
                        .map_err(|e| WorkQueueSendError::Backend(e.into()))
                }
                .await;
                if let Err(e) = res {
                    tracing::warn!("messenger queue try_send failed: {e}");
                }
            });
        } else {
            tracing::warn!(
                "messenger queue try_send called without an active Tokio runtime; message will be dropped"
            );
        }
        Ok(())
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

// ============================================================================
// Local Receiver: pulls directly from the QueueActor (same instance)
// ============================================================================

struct LocalMessengerReceiver {
    actor: Arc<QueueActor>,
}

impl ReceiverBackend for LocalMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        Box::pin(async move { Ok(self.actor.pop_async().await) })
    }

    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        let batch_size = opts.batch_size;
        let timeout = opts.timeout;
        Box::pin(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let deadline = tokio::time::Instant::now() + timeout;

            while batch.len() < batch_size {
                // Try non-blocking drain first.
                if let Some(data) = self.actor.pop() {
                    batch.push(data);
                    continue;
                }

                // Wait for the next item until the deadline.
                match tokio::time::timeout_at(deadline, self.actor.pop_async()).await {
                    Ok(Some(data)) => batch.push(data),
                    Ok(None) => return Ok(batch),
                    Err(_timeout) => return Ok(batch),
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        Ok(self.actor.pop())
    }
}

// ============================================================================
// Remote Receiver: sends typed_unary requests to the actor on another instance
// ============================================================================

struct RemoteMessengerReceiver {
    messenger: Arc<Messenger>,
    target: InstanceId,
    next_handler: String,
}

impl ReceiverBackend for RemoteMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        Box::pin(async move {
            let response = self
                .messenger
                .unary(&self.next_handler)
                .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
                .send_to(self.target)
                .await
                .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;

            if response.is_empty() {
                Ok(None)
            } else {
                Ok(Some(response))
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

            match tokio::time::timeout(timeout, self.recv()).await {
                Ok(Ok(Some(data))) => batch.push(data),
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return Ok(batch),
            }

            // Try to fill the rest (non-blocking requests in quick succession)
            for _ in 1..batch_size {
                match self.recv().await {
                    Ok(Some(data)) => batch.push(data),
                    _ => break,
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        // Can't do true non-blocking over the network — return None.
        Ok(None)
    }
}
