// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS JetStream work queue backend.
//!
//! Maps named queues to JetStream streams with **WorkQueue** retention policy.
//! Each queue creates:
//! - A stream named `{cluster_id}_{name}` with a single subject
//!   `{cluster_id}.queue.{name}`
//! - A durable pull consumer named `{cluster_id}_{name}_worker`
//!
//! Supports both [`AckPolicy::Auto`] (backend acks on receipt) and
//! [`AckPolicy::Manual`] (caller drives ack / nack(delay) / in_progress / term
//! via the [`AckHandle`](crate::queue::AckHandle) attached to each delivered item).
//! Visibility timeout is driven by the consumer's `AckWait`
//! ([`DEFAULT_NATS_ACK_WAIT`] by default, override via
//! [`NatsQueueBackend::with_ack_wait`]).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::message::Acker;
use async_nats::jetstream::stream::RetentionPolicy;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;

use tokio_util::sync::CancellationToken;

use crate::queue::ack::{AckFuture, AckHandle, AckHandleInner};
use crate::queue::backend::{DeliveredMessage, ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::queue::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::queue::options::{AckPolicy, NextOptions};

/// Default visibility timeout (server-side `AckWait`) for unacked messages.
pub const DEFAULT_NATS_ACK_WAIT: Duration = Duration::from_secs(30);

/// NATS JetStream work queue backend.
///
/// Accepts an existing `async_nats::Client` so the connection can be shared
/// with other velo subsystems (e.g., `NatsTransport`).
pub struct NatsQueueBackend {
    jetstream: jetstream::Context,
    cluster_id: String,
    ack_wait: Duration,
    /// Cache of created stream/consumer resources per queue name.
    resources: DashMap<String, Arc<NatsQueueResources>>,
    /// Cancellation token shared with all receivers created from this backend.
    /// Cancelling it causes all active recv() calls to return Ok(None).
    shutdown_token: CancellationToken,
}

struct NatsQueueResources {
    subject: String,
    js: jetstream::Context,
    consumer: PullConsumer,
}

impl NatsQueueBackend {
    /// Create a new NATS queue backend from an existing client.
    ///
    /// Uses [`DEFAULT_NATS_ACK_WAIT`] for the consumer's visibility timeout.
    /// Override with [`with_ack_wait`](Self::with_ack_wait).
    pub fn new(client: Arc<async_nats::Client>, cluster_id: String) -> Self {
        let jetstream = jetstream::new((*client).clone());
        Self {
            jetstream,
            cluster_id,
            ack_wait: DEFAULT_NATS_ACK_WAIT,
            resources: DashMap::new(),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Convenience: connect to a NATS server and create the backend.
    pub async fn from_url(url: &str, cluster_id: String) -> Result<Self, WorkQueueError> {
        let client = async_nats::connect(url)
            .await
            .map_err(|e| WorkQueueError::Backend(e.into()))?;
        let jetstream = jetstream::new(client);
        Ok(Self {
            jetstream,
            cluster_id,
            ack_wait: DEFAULT_NATS_ACK_WAIT,
            resources: DashMap::new(),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// Override the consumer's `AckWait` (visibility timeout) before the first queue is used.
    ///
    /// The value takes effect at consumer-creation time; for already-created
    /// queues the existing consumer's `AckWait` persists.
    pub fn with_ack_wait(mut self, ack_wait: Duration) -> Self {
        self.ack_wait = ack_wait;
        self
    }

    fn stream_name(&self, queue_name: &str) -> String {
        // JetStream stream names cannot contain dots
        format!("{}_{}", self.cluster_id, queue_name).replace('.', "_")
    }

    fn subject(&self, queue_name: &str) -> String {
        format!("{}.queue.{queue_name}", self.cluster_id)
    }

    fn consumer_name(&self, queue_name: &str) -> String {
        format!("{}_worker", self.stream_name(queue_name))
    }

    async fn get_or_create_resources(
        &self,
        name: &str,
    ) -> Result<Arc<NatsQueueResources>, WorkQueueError> {
        if let Some(res) = self.resources.get(name) {
            return Ok(res.clone());
        }

        let stream_name = self.stream_name(name);
        let subject = self.subject(name);
        let consumer_name = self.consumer_name(name);

        // Create or get the stream with WorkQueue retention
        let stream = self
            .jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: stream_name,
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                ..Default::default()
            })
            .await
            .map_err(|e| WorkQueueError::Creation {
                name: name.to_owned(),
                source: e.into(),
            })?;

        // Create or get the durable pull consumer
        let consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: self.ack_wait,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| WorkQueueError::Creation {
                name: name.to_owned(),
                source: e.into(),
            })?;

        let resources = Arc::new(NatsQueueResources {
            subject,
            js: self.jetstream.clone(),
            consumer,
        });
        self.resources
            .insert(name.to_owned(), Arc::clone(&resources));
        Ok(resources)
    }

    /// Signal all receivers created from this backend to stop blocking and
    /// return `Ok(None)`. Idempotent — safe to call multiple times.
    pub fn close(&self) {
        self.shutdown_token.cancel();
    }
}

impl Drop for NatsQueueBackend {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
    }
}

impl WorkQueueBackend for NatsQueueBackend {
    fn sender(
        &self,
        name: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn SenderBackend>, WorkQueueError>> + Send + '_>>
    {
        let name = name.to_owned();
        Box::pin(async move {
            let resources = self.get_or_create_resources(&name).await?;
            Ok(Arc::new(NatsSender {
                subject: resources.subject.clone(),
                js: resources.js.clone(),
            }) as Arc<dyn SenderBackend>)
        })
    }

    fn receiver(
        &self,
        name: &str,
        policy: AckPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let name = name.to_owned();
        let shutdown = self.shutdown_token.child_token();
        Box::pin(async move {
            let resources = self.get_or_create_resources(&name).await?;
            Ok(Arc::new(NatsReceiver {
                consumer: resources.consumer.clone(),
                shutdown,
                policy,
            }) as Arc<dyn ReceiverBackend>)
        })
    }
}

// ============================================================================
// Sender: publishes to JetStream subject
// ============================================================================

struct NatsSender {
    subject: String,
    js: jetstream::Context,
}

impl SenderBackend for NatsSender {
    fn send(
        &self,
        data: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), WorkQueueSendError>> + Send + '_>> {
        Box::pin(async move {
            self.js
                .publish(self.subject.clone(), data)
                .await
                .map_err(|e| WorkQueueSendError::Backend(e.into()))?
                .await
                .map_err(|e| WorkQueueSendError::Backend(e.into()))?;
            Ok(())
        })
    }

    fn try_send(&self, data: Bytes) -> Result<(), WorkQueueSendError> {
        let js = self.js.clone();
        let subject = self.subject.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(e) = js.publish(subject, data).await {
                    tracing::warn!("NATS queue try_send publish failed: {e}");
                }
            });
        } else {
            tracing::warn!(
                "NATS queue try_send called without an active Tokio runtime; message will be dropped"
            );
        }
        Ok(())
    }

    fn close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // No explicit close for NATS-backed queues — the stream persists.
        Box::pin(async {})
    }
}

// ============================================================================
// Receiver: fetches from JetStream pull consumer. Under Auto the receiver
// acks on receipt; under Manual it hands the Acker to a WorkItem via AckHandle.
// ============================================================================

struct NatsReceiver {
    consumer: PullConsumer,
    /// Child token of NatsQueueBackend::shutdown_token.
    /// Cancelled when the parent backend closes or drops.
    shutdown: CancellationToken,
    policy: AckPolicy,
}

/// Split a fetched JetStream message into a `DeliveredMessage` per the policy.
///
/// Under Auto: ack immediately and return a handle-less delivery.
/// Under Manual: split off an `Acker` for the caller to drive.
async fn deliver_message(
    msg: jetstream::Message,
    policy: AckPolicy,
) -> Result<DeliveredMessage, WorkQueueRecvError> {
    match policy {
        AckPolicy::Auto => {
            msg.ack().await.map_err(WorkQueueRecvError::Backend)?;
            Ok(DeliveredMessage::auto(msg.payload.clone()))
        }
        AckPolicy::Manual => {
            let (inner, acker) = msg.split();
            let handle = AckHandle::new(NatsAckHandle { acker });
            Ok(DeliveredMessage::manual(inner.payload, handle))
        }
    }
}

impl ReceiverBackend for NatsReceiver {
    fn recv(
        &self,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<DeliveredMessage>, WorkQueueRecvError>> + Send + '_>,
    > {
        Box::pin(async move {
            loop {
                // Check shutdown before issuing the next fetch.
                if self.shutdown.is_cancelled() {
                    return Ok(None);
                }

                let fetch_fut = async {
                    let mut batch = self
                        .consumer
                        .fetch()
                        .max_messages(1)
                        .expires(Duration::from_secs(30))
                        .messages()
                        .await
                        .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;
                    Ok::<_, WorkQueueRecvError>(batch.next().await)
                };

                tokio::select! {
                    biased;
                    _ = self.shutdown.cancelled() => return Ok(None),
                    result = fetch_fut => {
                        match result? {
                            Some(Ok(msg)) => {
                                return Ok(Some(deliver_message(msg, self.policy).await?));
                            }
                            Some(Err(e)) => return Err(WorkQueueRecvError::Backend(e)),
                            // Fetch expired with no message; retry
                            None => continue,
                        }
                    }
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
            // Check shutdown before issuing the fetch.
            if self.shutdown.is_cancelled() {
                return Ok(Vec::new());
            }

            let fetch_fut = async {
                let mut messages = self
                    .consumer
                    .fetch()
                    .max_messages(batch_size)
                    .expires(timeout)
                    .messages()
                    .await
                    .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;

                let mut batch = Vec::with_capacity(batch_size);

                while let Some(msg_result) = messages.next().await {
                    match msg_result {
                        Ok(msg) => {
                            batch.push(deliver_message(msg, self.policy).await?);
                        }
                        Err(e) => {
                            tracing::warn!("NATS queue recv_batch message error: {e}");
                            break;
                        }
                    }
                }

                Ok::<_, WorkQueueRecvError>(batch)
            };

            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => Ok(Vec::new()),
                result = fetch_fut => result,
            }
        })
    }

    fn try_recv(&self) -> Result<Option<DeliveredMessage>, WorkQueueRecvError> {
        // JetStream fetch is inherently async — cannot do true non-blocking.
        Ok(None)
    }
}

// ============================================================================
// AckHandle: maps to JetStream AckKind variants via `Acker`.
// ============================================================================

/// Backend-specific ack handle wrapping a JetStream `Acker`.
struct NatsAckHandle {
    acker: Acker,
}

impl NatsAckHandle {
    async fn send(&self, kind: AckKind) -> Result<(), WorkQueueRecvError> {
        self.acker
            .ack_with(kind)
            .await
            .map_err(WorkQueueRecvError::Backend)
    }
}

impl AckHandleInner for NatsAckHandle {
    fn ack(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move { self.acker.ack().await.map_err(WorkQueueRecvError::Backend) })
    }

    fn nack(self: Box<Self>, delay: Duration) -> AckFuture<'static> {
        let kind = if delay.is_zero() {
            AckKind::Nak(None)
        } else {
            AckKind::Nak(Some(delay))
        };
        Box::pin(async move { self.send(kind).await })
    }

    fn in_progress(&self) -> AckFuture<'_> {
        Box::pin(async move { self.send(AckKind::Progress).await })
    }

    fn term(self: Box<Self>) -> AckFuture<'static> {
        Box::pin(async move { self.send(AckKind::Term).await })
    }
}
