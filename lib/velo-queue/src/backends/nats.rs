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
//! Items are auto-acknowledged on receipt (consumer uses `AckPolicy::Explicit`
//! and the receiver acks immediately after fetching).
//!
//! # TODO: Acknowledgment Support
//!
//! A future iteration will expose `WorkItem<T>` with manual ack/nack,
//! controlled by an `AckPolicy` config.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_nats::jetstream;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::stream::RetentionPolicy;
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;

use crate::backend::{ReceiverBackend, SenderBackend, WorkQueueBackend};
use crate::error::{WorkQueueError, WorkQueueRecvError, WorkQueueSendError};
use crate::options::NextOptions;

/// NATS JetStream work queue backend.
///
/// Accepts an existing `async_nats::Client` so the connection can be shared
/// with other velo subsystems (e.g., `NatsTransport`).
pub struct NatsQueueBackend {
    jetstream: jetstream::Context,
    cluster_id: String,
    /// Cache of created stream/consumer resources per queue name.
    resources: DashMap<String, Arc<NatsQueueResources>>,
}

struct NatsQueueResources {
    subject: String,
    js: jetstream::Context,
    consumer: PullConsumer,
}

impl NatsQueueBackend {
    /// Create a new NATS queue backend from an existing client.
    pub fn new(client: Arc<async_nats::Client>, cluster_id: String) -> Self {
        let jetstream = jetstream::new((*client).clone());
        Self {
            jetstream,
            cluster_id,
            resources: DashMap::new(),
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
            resources: DashMap::new(),
        })
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
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ReceiverBackend>, WorkQueueError>> + Send + '_>>
    {
        let name = name.to_owned();
        Box::pin(async move {
            let resources = self.get_or_create_resources(&name).await?;
            Ok(Arc::new(NatsReceiver {
                consumer: resources.consumer.clone(),
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
// Receiver: fetches from JetStream pull consumer and auto-acks
// ============================================================================

struct NatsReceiver {
    consumer: PullConsumer,
}

impl ReceiverBackend for NatsReceiver {
    fn recv(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, WorkQueueRecvError>> + Send + '_>> {
        Box::pin(async move {
            let mut batch = self
                .consumer
                .fetch()
                .max_messages(1)
                .messages()
                .await
                .map_err(|e| WorkQueueRecvError::Backend(e.into()))?;

            match batch.next().await {
                Some(Ok(msg)) => {
                    // Auto-ack on receipt
                    msg.ack().await.map_err(WorkQueueRecvError::Backend)?;
                    Ok(Some(msg.payload.clone()))
                }
                Some(Err(e)) => Err(WorkQueueRecvError::Backend(e)),
                None => Ok(None),
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
                        msg.ack().await.map_err(WorkQueueRecvError::Backend)?;
                        batch.push(msg.payload.clone());
                    }
                    Err(e) => {
                        tracing::warn!("NATS queue recv_batch message error: {e}");
                        break;
                    }
                }
            }

            Ok(batch)
        })
    }

    fn try_recv(&self) -> Result<Option<Bytes>, WorkQueueRecvError> {
        // JetStream fetch is inherently async — cannot do true non-blocking.
        Ok(None)
    }
}
