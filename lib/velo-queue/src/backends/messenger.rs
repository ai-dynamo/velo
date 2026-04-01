// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Velo-messenger-based work queue backend.
//!
//! Uses a single queue service actor per target messenger instance. Queue names
//! are part of the request payload rather than the handler name, so the remote
//! handler surface stays fixed even as queues are created dynamically.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use velo_common::InstanceId;
use velo_messenger::{Messenger, TypedContext};

use crate::backend::{ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::NextOptions;

const QUEUE_RPC_HANDLER: &str = "velo.queue.rpc";
const REMOTE_RECV_POLL_INTERVAL: Duration = Duration::from_millis(25);
const REMOTE_RECV_POLL_TIMEOUT: Duration = Duration::from_millis(250);

/// Configuration for the messenger queue backend.
#[derive(Default)]
pub struct MessengerQueueConfig {
    /// Capacity of the internal buffer on the actor side.
    /// If `None`, the buffer is unbounded.
    pub capacity: Option<usize>,
}

/// A work queue backend that uses a queue service actor behind fixed messenger handlers.
///
/// The service is created lazily on first use and lives as long as this backend
/// (plus any outstanding sender/receiver handles). When the backend and all its
/// handles are dropped, the background service task shuts down automatically.
pub struct MessengerQueueBackend {
    messenger: Arc<Messenger>,
    target_instance: InstanceId,
    config: MessengerQueueConfig,
    /// Lazily-initialized local queue service (only used when target == self).
    local_service: Mutex<Option<Arc<QueueService>>>,
}

impl MessengerQueueBackend {
    /// Create a new messenger queue backend.
    pub fn new(
        messenger: Arc<Messenger>,
        target_instance: InstanceId,
        config: MessengerQueueConfig,
    ) -> Self {
        Self {
            messenger,
            target_instance,
            config,
            local_service: Mutex::new(None),
        }
    }

    fn local_service(&self) -> Result<Arc<QueueService>, WorkQueueError> {
        let mut guard = self.local_service.lock();
        if let Some(service) = guard.as_ref() {
            return Ok(Arc::clone(service));
        }

        let service = QueueService::spawn();
        let handler = build_queue_rpc_handler(Arc::clone(&service));
        self.messenger
            .register_handler(handler)
            .map_err(|e| WorkQueueError::Creation {
                name: QUEUE_RPC_HANDLER.to_owned(),
                source: e.into(),
            })?;

        *guard = Some(Arc::clone(&service));
        Ok(service)
    }
}

impl WorkQueueBackend for MessengerQueueBackend {
    fn sender(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn SenderBackend>, WorkQueueError>> + Send + '_>>
    {
        let result = (|| {
            if self.target_instance == self.messenger.instance_id() {
                let service = self.local_service()?;
                return Ok(Arc::new(LocalMessengerSender {
                    service,
                    queue: name.to_owned(),
                    capacity: self.config.capacity,
                }) as Arc<dyn SenderBackend>);
            }

            Ok(Arc::new(RemoteMessengerSender {
                messenger: Arc::clone(&self.messenger),
                target: self.target_instance,
                queue: name.to_owned(),
                capacity: self.config.capacity,
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
            if self.target_instance == self.messenger.instance_id() {
                let service = self.local_service()?;
                return Ok(Arc::new(LocalMessengerReceiver {
                    service,
                    queue: name.to_owned(),
                    capacity: self.config.capacity,
                }) as Arc<dyn ReceiverBackend>);
            }

            Ok(Arc::new(RemoteMessengerReceiver {
                messenger: Arc::clone(&self.messenger),
                target: self.target_instance,
                queue: name.to_owned(),
                capacity: self.config.capacity,
            }) as Arc<dyn ReceiverBackend>)
        })();
        Box::pin(async move { result })
    }
}

#[derive(Serialize, Deserialize)]
enum QueueRpcRequest {
    Enqueue {
        queue: String,
        data: Vec<u8>,
        capacity: Option<usize>,
    },
    RecvPoll {
        queue: String,
        capacity: Option<usize>,
        poll_timeout_ms: u64,
    },
}

#[derive(Serialize, Deserialize)]
enum QueueRpcResponse {
    Ack,
    Item(Vec<u8>),
    Empty,
}

fn build_queue_rpc_handler(service: Arc<QueueService>) -> velo_messenger::Handler {
    velo_messenger::Handler::typed_unary_async(
        QUEUE_RPC_HANDLER,
        move |ctx: TypedContext<QueueRpcRequest>| {
            let service = Arc::clone(&service);
            async move {
                match ctx.input {
                    QueueRpcRequest::Enqueue {
                        queue,
                        data,
                        capacity,
                    } => {
                        service
                            .enqueue_async(queue, data, capacity)
                            .await
                            .map_err(|_| std::io::Error::other("queue service closed"))?;
                        Ok(QueueRpcResponse::Ack)
                    }
                    QueueRpcRequest::RecvPoll {
                        queue,
                        capacity,
                        poll_timeout_ms,
                    } => {
                        let deadline =
                            tokio::time::Instant::now() + Duration::from_millis(poll_timeout_ms);
                        loop {
                            if let Some(data) = service
                                .try_recv_async(queue.clone(), capacity)
                                .await
                                .map_err(|_| std::io::Error::other("queue service closed"))?
                            {
                                return Ok(QueueRpcResponse::Item(data));
                            }

                            let now = tokio::time::Instant::now();
                            if now >= deadline {
                                return Ok(QueueRpcResponse::Empty);
                            }

                            tokio::time::sleep((deadline - now).min(REMOTE_RECV_POLL_INTERVAL))
                                .await;
                        }
                    }
                }
            }
        },
    )
    .spawn()
    .build()
}

enum Command {
    Enqueue {
        queue: String,
        data: Vec<u8>,
        capacity: Option<usize>,
        reply: flume::Sender<Result<(), QueueServiceError>>,
    },
    Recv {
        queue: String,
        capacity: Option<usize>,
        reply: flume::Sender<Vec<u8>>,
    },
    TryRecv {
        queue: String,
        capacity: Option<usize>,
        reply: flume::Sender<Option<Vec<u8>>>,
    },
}

#[derive(Debug, Clone, Copy)]
enum QueueServiceError {
    Closed,
}

struct QueueState {
    items: VecDeque<Vec<u8>>,
    capacity: Option<usize>,
    pending_receivers: VecDeque<flume::Sender<Vec<u8>>>,
}

impl QueueState {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
            pending_receivers: VecDeque::new(),
        }
    }
}

struct QueueService {
    commands: flume::Sender<Command>,
}

impl QueueService {
    fn spawn() -> Arc<Self> {
        let (tx, rx) = flume::unbounded();
        let service = Arc::new(Self { commands: tx });
        tokio::spawn(async move {
            run_queue_service(rx).await;
        });
        service
    }

    async fn enqueue_async(
        &self,
        queue: String,
        data: Vec<u8>,
        capacity: Option<usize>,
    ) -> Result<(), QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send_async(Command::Enqueue {
                queue,
                data,
                capacity,
                reply: reply_tx,
            })
            .await
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| QueueServiceError::Closed)?
    }

    fn enqueue_sync(
        &self,
        queue: String,
        data: Vec<u8>,
        capacity: Option<usize>,
    ) -> Result<(), QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send(Command::Enqueue {
                queue,
                data,
                capacity,
                reply: reply_tx,
            })
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx.recv().map_err(|_| QueueServiceError::Closed)?
    }

    async fn recv_async(
        &self,
        queue: String,
        capacity: Option<usize>,
    ) -> Result<Vec<u8>, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send_async(Command::Recv {
                queue,
                capacity,
                reply: reply_tx,
            })
            .await
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| QueueServiceError::Closed)
    }

    async fn try_recv_async(
        &self,
        queue: String,
        capacity: Option<usize>,
    ) -> Result<Option<Vec<u8>>, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send_async(Command::TryRecv {
                queue,
                capacity,
                reply: reply_tx,
            })
            .await
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx
            .recv_async()
            .await
            .map_err(|_| QueueServiceError::Closed)
    }

    fn try_recv_sync(
        &self,
        queue: String,
        capacity: Option<usize>,
    ) -> Result<Option<Vec<u8>>, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send(Command::TryRecv {
                queue,
                capacity,
                reply: reply_tx,
            })
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx.recv().map_err(|_| QueueServiceError::Closed)
    }
}

async fn run_queue_service(rx: flume::Receiver<Command>) {
    let mut queues = HashMap::<String, QueueState>::new();

    while let Ok(command) = rx.recv_async().await {
        match command {
            Command::Enqueue {
                queue,
                data,
                capacity,
                reply,
            } => {
                let state = queues
                    .entry(queue)
                    .or_insert_with(|| QueueState::new(capacity));
                deliver_or_buffer(state, data);
                let _ = reply.send(Ok(()));
            }
            Command::Recv {
                queue,
                capacity,
                reply,
            } => {
                let state = queues
                    .entry(queue)
                    .or_insert_with(|| QueueState::new(capacity));
                if let Some(data) = state.items.pop_front() {
                    if let Err(flume::SendError(data)) = reply.send(data) {
                        state.items.push_front(data);
                    }
                } else {
                    state.pending_receivers.push_back(reply);
                }
            }
            Command::TryRecv {
                queue,
                capacity,
                reply,
            } => {
                let state = queues
                    .entry(queue)
                    .or_insert_with(|| QueueState::new(capacity));
                let data = state.items.pop_front();
                let _ = reply.send(data);
            }
        }
    }
}

fn deliver_or_buffer(state: &mut QueueState, mut data: Vec<u8>) {
    while let Some(waiter) = state.pending_receivers.pop_front() {
        match waiter.send(data) {
            Ok(()) => return,
            Err(flume::SendError(returned)) => {
                data = returned;
            }
        }
    }

    if let Some(capacity) = state.capacity
        && state.items.len() >= capacity
    {
        state.items.pop_front();
    }
    state.items.push_back(data);
}

fn map_service_send_error(err: QueueServiceError) -> WorkQueueSendError {
    match err {
        QueueServiceError::Closed => WorkQueueSendError::Closed,
    }
}

fn map_service_recv_error(err: QueueServiceError) -> WorkQueueRecvError {
    match err {
        QueueServiceError::Closed => WorkQueueRecvError::Closed,
    }
}

struct LocalMessengerSender {
    service: Arc<QueueService>,
    queue: String,
    capacity: Option<usize>,
}

impl SenderBackend for LocalMessengerSender {
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>> {
        let service = Arc::clone(&self.service);
        let queue = self.queue.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            service
                .enqueue_async(queue, data.to_vec(), capacity)
                .await
                .map_err(map_service_send_error)
        })
    }

    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError> {
        self.service
            .enqueue_sync(self.queue.clone(), data.to_vec(), self.capacity)
            .map_err(map_service_send_error)
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

struct RemoteMessengerSender {
    messenger: Arc<Messenger>,
    target: InstanceId,
    queue: String,
    capacity: Option<usize>,
}

impl RemoteMessengerSender {
    async fn send_rpc(&self, data: Vec<u8>) -> Result<(), WorkQueueSendError> {
        let response: QueueRpcResponse = self
            .messenger
            .typed_unary(QUEUE_RPC_HANDLER)
            .map_err(|e| WorkQueueSendError::Backend(e.into()))?
            .payload(QueueRpcRequest::Enqueue {
                queue: self.queue.clone(),
                data,
                capacity: self.capacity,
            })
            .map_err(|e| WorkQueueSendError::Backend(e.into()))?
            .send_to(self.target)
            .await
            .map_err(|e| WorkQueueSendError::Backend(e.into()))?;

        match response {
            QueueRpcResponse::Ack => Ok(()),
            QueueRpcResponse::Item(_) | QueueRpcResponse::Empty => Err(
                WorkQueueSendError::Backend("unexpected queue RPC response".into()),
            ),
        }
    }
}

impl SenderBackend for RemoteMessengerSender {
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>> {
        Box::pin(async move { self.send_rpc(data.to_vec()).await })
    }

    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError> {
        let messenger = Arc::clone(&self.messenger);
        let target = self.target;
        let queue = self.queue.clone();
        let capacity = self.capacity;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let response = messenger
                    .typed_unary::<QueueRpcResponse>(QUEUE_RPC_HANDLER)
                    .and_then(|builder| {
                        builder.payload(QueueRpcRequest::Enqueue {
                            queue,
                            data: data.to_vec(),
                            capacity,
                        })
                    });

                match response {
                    Ok(builder) => {
                        if let Err(e) = builder.send_to(target).await {
                            tracing::warn!("messenger queue try_send failed: {e}");
                        }
                    }
                    Err(e) => tracing::warn!("messenger queue try_send failed: {e}"),
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

struct LocalMessengerReceiver {
    service: Arc<QueueService>,
    queue: String,
    capacity: Option<usize>,
}

impl ReceiverBackend for LocalMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        let service = Arc::clone(&self.service);
        let queue = self.queue.clone();
        let capacity = self.capacity;
        Box::pin(async move {
            service
                .recv_async(queue, capacity)
                .await
                .map(Bytes::from)
                .map(Some)
                .map_err(map_service_recv_error)
        })
    }

    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        let service = Arc::clone(&self.service);
        let queue = self.queue.clone();
        let capacity = self.capacity;
        let batch_size = opts.batch_size;
        let timeout = opts.timeout;
        Box::pin(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let deadline = tokio::time::Instant::now() + timeout;

            while batch.len() < batch_size {
                if let Some(data) = service
                    .try_recv_async(queue.clone(), capacity)
                    .await
                    .map_err(map_service_recv_error)?
                {
                    batch.push(Bytes::from(data));
                    continue;
                }

                match tokio::time::timeout_at(deadline, service.recv_async(queue.clone(), capacity))
                    .await
                {
                    Ok(Ok(data)) => batch.push(Bytes::from(data)),
                    Ok(Err(err)) => return Err(map_service_recv_error(err)),
                    Err(_timeout) => return Ok(batch),
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        self.service
            .try_recv_sync(self.queue.clone(), self.capacity)
            .map(|data| data.map(Bytes::from))
            .map_err(map_service_recv_error)
    }
}

struct RemoteMessengerReceiver {
    messenger: Arc<Messenger>,
    target: InstanceId,
    queue: String,
    capacity: Option<usize>,
}

impl RemoteMessengerReceiver {
    async fn poll_once(&self, timeout: Duration) -> Result<Option<Bytes>, WorkQueueRecvError> {
        let response: QueueRpcResponse = self
            .messenger
            .typed_unary(QUEUE_RPC_HANDLER)
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .payload(QueueRpcRequest::RecvPoll {
                queue: self.queue.clone(),
                capacity: self.capacity,
                poll_timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
            })
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .send_to(self.target)
            .await
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;

        match response {
            QueueRpcResponse::Item(data) => Ok(Some(Bytes::from(data))),
            QueueRpcResponse::Empty => Ok(None),
            QueueRpcResponse::Ack => Err(WorkQueueRecvError::Backend(
                "unexpected queue RPC response".into(),
            )),
        }
    }
}

impl ReceiverBackend for RemoteMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        Box::pin(async move {
            loop {
                if let Some(data) = self.poll_once(REMOTE_RECV_POLL_TIMEOUT).await? {
                    return Ok(Some(data));
                }
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
            let deadline = tokio::time::Instant::now() + timeout;

            while batch.len() < batch_size {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Ok(batch);
                }

                let poll_timeout = (deadline - now).min(REMOTE_RECV_POLL_TIMEOUT);
                if let Some(data) = self.poll_once(poll_timeout).await? {
                    batch.push(data);
                }
                // On Empty, continue polling until the deadline expires.
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        Ok(None)
    }
}
