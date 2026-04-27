// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Velo-messenger-based work queue backend.
//!
//! Uses a single queue service actor per target messenger instance. Queue names
//! are part of the request payload rather than the handler name, so the remote
//! handler surface stays fixed even as queues are created dynamically.
//!
//! Supports both [`AckPolicy::Auto`] (actor pops on recv and the item is gone)
//! and [`AckPolicy::Manual`] (actor mints a lease with a visibility timeout;
//! the caller drives `ack` / `nack(delay)` / `in_progress` / `term` via the
//! [`AckHandle`](crate::AckHandle) attached to each delivered item). The
//! actor runs a single deadline-driven sweeper that redelivers expired leases
//! back to the queue.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use velo_common::InstanceId;
use velo_messenger::{Messenger, TypedContext};

use crate::ack::{AckFuture, AckHandle, AckHandleInner};
use crate::backend::{DeliveredMessage, ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::{AckPolicy, NextOptions};

const QUEUE_RPC_HANDLER: &str = "velo.queue.rpc";
const REMOTE_RECV_POLL_INTERVAL: Duration = Duration::from_millis(25);
const REMOTE_RECV_POLL_TIMEOUT: Duration = Duration::from_millis(250);

/// Default visibility timeout for manually-acked items on the messenger backend.
pub const DEFAULT_MESSENGER_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Configuration for the messenger queue backend.
pub struct MessengerQueueConfig {
    /// Capacity of the internal buffer on the actor side.
    /// If `None`, the buffer is unbounded.
    pub capacity: Option<usize>,
    /// Visibility timeout for in-flight items under [`AckPolicy::Manual`].
    pub visibility_timeout: Duration,
}

impl Default for MessengerQueueConfig {
    fn default() -> Self {
        Self {
            capacity: None,
            visibility_timeout: DEFAULT_MESSENGER_VISIBILITY_TIMEOUT,
        }
    }
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

        let service = QueueService::spawn(self.config.visibility_timeout);
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
        policy: AckPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let result = (|| {
            if self.target_instance == self.messenger.instance_id() {
                let service = self.local_service()?;
                return Ok(Arc::new(LocalMessengerReceiver {
                    service,
                    queue: name.to_owned(),
                    capacity: self.config.capacity,
                    policy,
                }) as Arc<dyn ReceiverBackend>);
            }

            Ok(Arc::new(RemoteMessengerReceiver {
                messenger: Arc::clone(&self.messenger),
                target: self.target_instance,
                queue: name.to_owned(),
                capacity: self.config.capacity,
                policy,
            }) as Arc<dyn ReceiverBackend>)
        })();
        Box::pin(async move { result })
    }
}

// ============================================================================
// Wire protocol (MessagePack via active-messenger RPC)
// ============================================================================

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
        /// `Manual` mints a lease and returns a token; `Auto` pops the item.
        policy: AckPolicy,
    },
    Ack {
        queue: String,
        token: u64,
    },
    Nack {
        queue: String,
        token: u64,
        delay_ms: u64,
    },
    Progress {
        queue: String,
        token: u64,
    },
    Term {
        queue: String,
        token: u64,
    },
}

#[derive(Serialize, Deserialize)]
enum QueueRpcResponse {
    /// Enqueue acknowledgment.
    Ack,
    /// Delivered item; `token` is `Some` under Manual.
    Item { data: Vec<u8>, token: Option<u64> },
    /// No item available within the poll window.
    Empty,
    /// Ack/Nack/Progress/Term acknowledgment.
    AckOk,
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
                        policy,
                    } => {
                        let deadline = Instant::now() + Duration::from_millis(poll_timeout_ms);
                        loop {
                            if let Some(reply) = service
                                .try_recv_async(queue.clone(), capacity, policy)
                                .await
                                .map_err(|_| std::io::Error::other("queue service closed"))?
                            {
                                return Ok(QueueRpcResponse::Item {
                                    data: reply.data,
                                    token: reply.token,
                                });
                            }

                            let now = Instant::now();
                            if now >= deadline {
                                return Ok(QueueRpcResponse::Empty);
                            }

                            tokio::time::sleep((deadline - now).min(REMOTE_RECV_POLL_INTERVAL))
                                .await;
                        }
                    }
                    QueueRpcRequest::Ack { queue, token } => {
                        service.ack_async(queue, token).await.ok();
                        Ok(QueueRpcResponse::AckOk)
                    }
                    QueueRpcRequest::Nack {
                        queue,
                        token,
                        delay_ms,
                    } => {
                        service
                            .nack_async(queue, token, Duration::from_millis(delay_ms))
                            .await
                            .ok();
                        Ok(QueueRpcResponse::AckOk)
                    }
                    QueueRpcRequest::Progress { queue, token } => {
                        service.progress_async(queue, token).await.ok();
                        Ok(QueueRpcResponse::AckOk)
                    }
                    QueueRpcRequest::Term { queue, token } => {
                        service.term_async(queue, token).await.ok();
                        Ok(QueueRpcResponse::AckOk)
                    }
                }
            }
        },
    )
    .spawn()
    .build()
}

// ============================================================================
// Actor command protocol (in-process, between handles and the service loop)
// ============================================================================

/// A received item plus an optional lease token (Some under Manual).
struct RecvReply {
    data: Vec<u8>,
    token: Option<u64>,
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
        policy: AckPolicy,
        reply: flume::Sender<RecvReply>,
    },
    TryRecv {
        queue: String,
        capacity: Option<usize>,
        policy: AckPolicy,
        reply: flume::Sender<Option<RecvReply>>,
    },
    Ack {
        queue: String,
        token: u64,
    },
    Nack {
        queue: String,
        token: u64,
        delay: Duration,
    },
    Progress {
        queue: String,
        token: u64,
    },
    Term {
        queue: String,
        token: u64,
    },
}

#[derive(Debug, Clone, Copy)]
enum QueueServiceError {
    Closed,
}

/// State for a single named queue on the actor.
struct QueueState {
    items: VecDeque<Vec<u8>>,
    capacity: Option<usize>,
    /// Pending receivers waiting for items, tagged with their policy so we
    /// know whether to mint a lease at delivery time.
    pending_receivers: VecDeque<(flume::Sender<RecvReply>, AckPolicy)>,
    /// Outstanding leases for manually-acked items, keyed by token.
    leases: HashMap<u64, Lease>,
    next_token: u64,
}

struct Lease {
    payload: Vec<u8>,
    deadline: Instant,
}

impl QueueState {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
            pending_receivers: VecDeque::new(),
            leases: HashMap::new(),
            next_token: 1,
        }
    }
}

struct QueueService {
    commands: flume::Sender<Command>,
}

impl QueueService {
    fn spawn(visibility_timeout: Duration) -> Arc<Self> {
        let (tx, rx) = flume::unbounded();
        let service = Arc::new(Self { commands: tx });
        let internal_tx = service.commands.clone();
        tokio::spawn(async move {
            run_queue_service(rx, internal_tx, visibility_timeout).await;
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
        policy: AckPolicy,
    ) -> Result<RecvReply, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send_async(Command::Recv {
                queue,
                capacity,
                policy,
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
        policy: AckPolicy,
    ) -> Result<Option<RecvReply>, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send_async(Command::TryRecv {
                queue,
                capacity,
                policy,
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
        policy: AckPolicy,
    ) -> Result<Option<RecvReply>, QueueServiceError> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.commands
            .send(Command::TryRecv {
                queue,
                capacity,
                policy,
                reply: reply_tx,
            })
            .map_err(|_| QueueServiceError::Closed)?;
        reply_rx.recv().map_err(|_| QueueServiceError::Closed)
    }

    async fn ack_async(&self, queue: String, token: u64) -> Result<(), QueueServiceError> {
        self.commands
            .send_async(Command::Ack { queue, token })
            .await
            .map_err(|_| QueueServiceError::Closed)
    }

    async fn nack_async(
        &self,
        queue: String,
        token: u64,
        delay: Duration,
    ) -> Result<(), QueueServiceError> {
        self.commands
            .send_async(Command::Nack {
                queue,
                token,
                delay,
            })
            .await
            .map_err(|_| QueueServiceError::Closed)
    }

    async fn progress_async(&self, queue: String, token: u64) -> Result<(), QueueServiceError> {
        self.commands
            .send_async(Command::Progress { queue, token })
            .await
            .map_err(|_| QueueServiceError::Closed)
    }

    async fn term_async(&self, queue: String, token: u64) -> Result<(), QueueServiceError> {
        self.commands
            .send_async(Command::Term { queue, token })
            .await
            .map_err(|_| QueueServiceError::Closed)
    }
}

/// Main actor loop. Also runs a deadline-driven sweeper that redelivers
/// expired manual-ack leases back to their queues.
async fn run_queue_service(
    rx: flume::Receiver<Command>,
    internal_tx: flume::Sender<Command>,
    visibility_timeout: Duration,
) {
    let mut queues = HashMap::<String, QueueState>::new();

    loop {
        // Compute the nearest lease deadline across all queues.
        let next_deadline = queues
            .values()
            .flat_map(|q| q.leases.values().map(|l| l.deadline))
            .min();

        let command = if let Some(deadline) = next_deadline {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    sweep_expired_leases(&mut queues, visibility_timeout);
                    continue;
                }
                cmd = rx.recv_async() => cmd,
            }
        } else {
            rx.recv_async().await
        };

        let Ok(command) = command else { return };

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
                deliver_or_buffer(state, data, visibility_timeout);
                let _ = reply.send(Ok(()));
            }
            Command::Recv {
                queue,
                capacity,
                policy,
                reply,
            } => {
                let state = queues
                    .entry(queue)
                    .or_insert_with(|| QueueState::new(capacity));
                if let Some(data) = state.items.pop_front() {
                    let recv_reply = build_recv_reply(state, data, policy, visibility_timeout);
                    if let Err(flume::SendError(rr)) = reply.send(recv_reply) {
                        // Receiver went away before we could deliver — put back.
                        if let Some(token) = rr.token {
                            // Remove the lease we just minted, since it's not being taken.
                            if let Some(lease) = state.leases.remove(&token) {
                                state.items.push_front(lease.payload);
                            }
                        } else {
                            state.items.push_front(rr.data);
                        }
                    }
                } else {
                    state.pending_receivers.push_back((reply, policy));
                }
            }
            Command::TryRecv {
                queue,
                capacity,
                policy,
                reply,
            } => {
                let state = queues
                    .entry(queue)
                    .or_insert_with(|| QueueState::new(capacity));
                let out = state
                    .items
                    .pop_front()
                    .map(|data| build_recv_reply(state, data, policy, visibility_timeout));
                let _ = reply.send(out);
            }
            Command::Ack { queue, token } => {
                if let Some(state) = queues.get_mut(&queue) {
                    state.leases.remove(&token);
                }
            }
            Command::Nack {
                queue,
                token,
                delay,
            } => {
                if let Some(state) = queues.get_mut(&queue)
                    && let Some(lease) = state.leases.remove(&token)
                {
                    if delay.is_zero() {
                        deliver_or_buffer(state, lease.payload, visibility_timeout);
                    } else {
                        // Schedule a delayed re-enqueue via an internal Enqueue command.
                        let tx = internal_tx.clone();
                        let capacity = state.capacity;
                        let queue_name = queue.clone();
                        let payload = lease.payload;
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let (reply_tx, _) = flume::bounded(1);
                            let _ = tx
                                .send_async(Command::Enqueue {
                                    queue: queue_name,
                                    data: payload,
                                    capacity,
                                    reply: reply_tx,
                                })
                                .await;
                        });
                    }
                }
            }
            Command::Progress { queue, token } => {
                if let Some(state) = queues.get_mut(&queue)
                    && let Some(lease) = state.leases.get_mut(&token)
                {
                    lease.deadline = Instant::now() + visibility_timeout;
                }
            }
            Command::Term { queue, token } => {
                if let Some(state) = queues.get_mut(&queue) {
                    state.leases.remove(&token);
                }
                // Dead-lettering not exposed for messenger yet; just drop.
            }
        }
    }
}

/// Walk all queues' leases, re-enqueue any whose deadline has passed.
///
/// `visibility_timeout` is forwarded to `deliver_to_waiter` so that leases
/// minted via the waiter-flush path (when a `Command::Recv` is pending at
/// sweep time) get a real deadline rather than `now + 0`.
fn sweep_expired_leases(queues: &mut HashMap<String, QueueState>, visibility_timeout: Duration) {
    let now = Instant::now();
    // Collect expired (queue, token) pairs first to avoid borrow conflicts.
    let mut expired: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (name, state) in queues.iter() {
        for (&token, lease) in state.leases.iter() {
            if lease.deadline <= now {
                expired.entry(name.clone()).or_default().push(token);
            }
        }
    }
    for (name, tokens) in expired {
        if let Some(state) = queues.get_mut(&name) {
            // Re-enqueue: payload goes back into items and is minted with a
            // fresh lease (and real timeout) on the next recv.
            for token in tokens {
                if let Some(lease) = state.leases.remove(&token) {
                    if let Some(capacity) = state.capacity
                        && state.items.len() >= capacity
                    {
                        state.items.pop_front();
                    }
                    state.items.push_back(lease.payload);
                }
            }
            // Flush to any pending receivers. deliver_to_waiter mints a new
            // lease here, so we must pass the actor's real visibility_timeout
            // — passing 0 would create immediately-expired leases and a
            // self-perpetuating sweep loop.
            while !state.pending_receivers.is_empty() && !state.items.is_empty() {
                let data = state.items.pop_front().unwrap();
                let (waiter, policy) = state.pending_receivers.pop_front().unwrap();
                deliver_to_waiter(state, data, waiter, policy, visibility_timeout);
            }
        }
    }
}

/// Build the `RecvReply` for `data` popped from `state.items`, minting a lease under Manual.
fn build_recv_reply(
    state: &mut QueueState,
    data: Vec<u8>,
    policy: AckPolicy,
    visibility_timeout: Duration,
) -> RecvReply {
    match policy {
        AckPolicy::Auto => RecvReply { data, token: None },
        AckPolicy::Manual => {
            let token = state.next_token;
            state.next_token += 1;
            state.leases.insert(
                token,
                Lease {
                    payload: data.clone(),
                    deadline: Instant::now() + visibility_timeout,
                },
            );
            RecvReply {
                data,
                token: Some(token),
            }
        }
    }
}

/// Deliver a payload to a waiting receiver; if the receiver has dropped, push back to the queue.
fn deliver_to_waiter(
    state: &mut QueueState,
    data: Vec<u8>,
    waiter: flume::Sender<RecvReply>,
    policy: AckPolicy,
    visibility_timeout: Duration,
) {
    let reply = build_recv_reply(state, data, policy, visibility_timeout);
    if let Err(flume::SendError(rr)) = waiter.send(reply) {
        // Receiver gone — undo the lease we just minted and requeue the payload.
        if let Some(token) = rr.token
            && let Some(lease) = state.leases.remove(&token)
        {
            state.items.push_front(lease.payload);
        } else {
            state.items.push_front(rr.data);
        }
    }
}

fn deliver_or_buffer(state: &mut QueueState, mut data: Vec<u8>, visibility_timeout: Duration) {
    while let Some((waiter, policy)) = state.pending_receivers.pop_front() {
        // Satisfy the waiter — mint a lease if they asked for Manual.
        let reply = build_recv_reply(state, data, policy, visibility_timeout);
        match waiter.send(reply) {
            Ok(()) => return,
            Err(flume::SendError(rr)) => {
                // Receiver went away; undo the lease if we minted one.
                if let Some(token) = rr.token
                    && let Some(lease) = state.leases.remove(&token)
                {
                    data = lease.payload;
                } else {
                    data = rr.data;
                }
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

// ============================================================================
// Senders (unchanged — enqueue is policy-agnostic).
// ============================================================================

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
            _ => Err(WorkQueueSendError::Backend(
                "unexpected queue RPC response".into(),
            )),
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

// ============================================================================
// Receivers
// ============================================================================

struct LocalMessengerReceiver {
    service: Arc<QueueService>,
    queue: String,
    capacity: Option<usize>,
    policy: AckPolicy,
}

impl LocalMessengerReceiver {
    fn wrap_reply(&self, reply: RecvReply) -> DeliveredMessage {
        match reply.token {
            Some(token) => DeliveredMessage::manual(
                Bytes::from(reply.data),
                AckHandle::new(MessengerAckHandle::Local {
                    service: Arc::clone(&self.service),
                    queue: self.queue.clone(),
                    token,
                }),
            ),
            None => DeliveredMessage::auto(Bytes::from(reply.data)),
        }
    }
}

impl ReceiverBackend for LocalMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>,
    > {
        let service = Arc::clone(&self.service);
        let queue = self.queue.clone();
        let capacity = self.capacity;
        let policy = self.policy;
        Box::pin(async move {
            service
                .recv_async(queue, capacity, policy)
                .await
                .map(|r| Some(self.wrap_reply(r)))
                .map_err(map_service_recv_error)
        })
    }

    fn recv_batch(
        &self,
        opts: &NextOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>>
    {
        let service = Arc::clone(&self.service);
        let queue = self.queue.clone();
        let capacity = self.capacity;
        let policy = self.policy;
        let batch_size = opts.batch_size;
        let timeout = opts.timeout;
        Box::pin(async move {
            let mut batch = Vec::with_capacity(batch_size);
            let deadline = Instant::now() + timeout;

            while batch.len() < batch_size {
                if let Some(reply) = service
                    .try_recv_async(queue.clone(), capacity, policy)
                    .await
                    .map_err(map_service_recv_error)?
                {
                    batch.push(self.wrap_reply(reply));
                    continue;
                }

                match tokio::time::timeout_at(
                    deadline,
                    service.recv_async(queue.clone(), capacity, policy),
                )
                .await
                {
                    Ok(Ok(reply)) => batch.push(self.wrap_reply(reply)),
                    Ok(Err(err)) => return Err(map_service_recv_error(err)),
                    Err(_timeout) => return Ok(batch),
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<DeliveredMessage>, WorkQueueRecvError> {
        self.service
            .try_recv_sync(self.queue.clone(), self.capacity, self.policy)
            .map(|r| r.map(|rr| self.wrap_reply(rr)))
            .map_err(map_service_recv_error)
    }
}

struct RemoteMessengerReceiver {
    messenger: Arc<Messenger>,
    target: InstanceId,
    queue: String,
    capacity: Option<usize>,
    policy: AckPolicy,
}

impl RemoteMessengerReceiver {
    async fn poll_once(
        &self,
        timeout: Duration,
    ) -> Result<Option<DeliveredMessage>, WorkQueueRecvError> {
        let response: QueueRpcResponse = self
            .messenger
            .typed_unary(QUEUE_RPC_HANDLER)
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .payload(QueueRpcRequest::RecvPoll {
                queue: self.queue.clone(),
                capacity: self.capacity,
                poll_timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                policy: self.policy,
            })
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .send_to(self.target)
            .await
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;

        match response {
            QueueRpcResponse::Item { data, token } => Ok(Some(self.wrap_item(data, token))),
            QueueRpcResponse::Empty => Ok(None),
            QueueRpcResponse::Ack | QueueRpcResponse::AckOk => Err(WorkQueueRecvError::Backend(
                "unexpected queue RPC response".into(),
            )),
        }
    }

    fn wrap_item(&self, data: Vec<u8>, token: Option<u64>) -> DeliveredMessage {
        match token {
            Some(token) => DeliveredMessage::manual(
                Bytes::from(data),
                AckHandle::new(MessengerAckHandle::Remote {
                    messenger: Arc::clone(&self.messenger),
                    target: self.target,
                    queue: self.queue.clone(),
                    token,
                }),
            ),
            None => DeliveredMessage::auto(Bytes::from(data)),
        }
    }
}

impl ReceiverBackend for RemoteMessengerReceiver {
    fn recv(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>,
    > {
        Box::pin(async move {
            loop {
                if let Some(msg) = self.poll_once(REMOTE_RECV_POLL_TIMEOUT).await? {
                    return Ok(Some(msg));
                }
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
            let deadline = Instant::now() + timeout;

            while batch.len() < batch_size {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(batch);
                }

                let poll_timeout = (deadline - now).min(REMOTE_RECV_POLL_TIMEOUT);
                if let Some(msg) = self.poll_once(poll_timeout).await? {
                    batch.push(msg);
                }
                // On Empty, continue polling until the deadline expires.
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<DeliveredMessage>, WorkQueueRecvError> {
        Ok(None)
    }
}

// ============================================================================
// AckHandle
// ============================================================================

enum MessengerAckHandle {
    Local {
        service: Arc<QueueService>,
        queue: String,
        token: u64,
    },
    Remote {
        messenger: Arc<Messenger>,
        target: InstanceId,
        queue: String,
        token: u64,
    },
}

impl MessengerAckHandle {
    async fn send_rpc(
        messenger: &Messenger,
        target: InstanceId,
        request: QueueRpcRequest,
    ) -> Result<(), WorkQueueRecvError> {
        let response: QueueRpcResponse = messenger
            .typed_unary(QUEUE_RPC_HANDLER)
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .payload(request)
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?
            .send_to(target)
            .await
            .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;
        match response {
            QueueRpcResponse::AckOk => Ok(()),
            _ => Err(WorkQueueRecvError::Backend(
                "unexpected ack RPC response".into(),
            )),
        }
    }
}

impl AckHandleInner for MessengerAckHandle {
    fn ack(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move {
            match *self {
                MessengerAckHandle::Local {
                    service,
                    queue,
                    token,
                } => service
                    .ack_async(queue, token)
                    .await
                    .map_err(|_| WorkQueueRecvError::Closed),
                MessengerAckHandle::Remote {
                    messenger,
                    target,
                    queue,
                    token,
                } => {
                    MessengerAckHandle::send_rpc(
                        &messenger,
                        target,
                        QueueRpcRequest::Ack { queue, token },
                    )
                    .await
                }
            }
        })
    }

    fn nack(self: Box<Self>, delay: Duration) -> AckFuture<'static> {
        Box::pin(async move {
            match *self {
                MessengerAckHandle::Local {
                    service,
                    queue,
                    token,
                } => service
                    .nack_async(queue, token, delay)
                    .await
                    .map_err(|_| WorkQueueRecvError::Closed),
                MessengerAckHandle::Remote {
                    messenger,
                    target,
                    queue,
                    token,
                } => {
                    let delay_ms = delay.as_millis().min(u128::from(u64::MAX)) as u64;
                    MessengerAckHandle::send_rpc(
                        &messenger,
                        target,
                        QueueRpcRequest::Nack {
                            queue,
                            token,
                            delay_ms,
                        },
                    )
                    .await
                }
            }
        })
    }

    fn in_progress(&self) -> AckFuture<'_> {
        Box::pin(async move {
            match self {
                MessengerAckHandle::Local {
                    service,
                    queue,
                    token,
                } => service
                    .progress_async(queue.clone(), *token)
                    .await
                    .map_err(|_| WorkQueueRecvError::Closed),
                MessengerAckHandle::Remote {
                    messenger,
                    target,
                    queue,
                    token,
                } => {
                    MessengerAckHandle::send_rpc(
                        messenger,
                        *target,
                        QueueRpcRequest::Progress {
                            queue: queue.clone(),
                            token: *token,
                        },
                    )
                    .await
                }
            }
        })
    }

    fn term(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move {
            match *self {
                MessengerAckHandle::Local {
                    service,
                    queue,
                    token,
                } => service
                    .term_async(queue, token)
                    .await
                    .map_err(|_| WorkQueueRecvError::Closed),
                MessengerAckHandle::Remote {
                    messenger,
                    target,
                    queue,
                    token,
                } => {
                    MessengerAckHandle::send_rpc(
                        &messenger,
                        target,
                        QueueRpcRequest::Term { queue, token },
                    )
                    .await
                }
            }
        })
    }
}
