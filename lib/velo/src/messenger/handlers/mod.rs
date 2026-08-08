// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Clean, builder-based handler API for active message patterns.
//!
//! ## Handler Types
//!
//! ### Active Message Handlers
//! - **`am_handler()`** - Sync AM handler: `Fn(Context) -> anyhow::Result<()>`
//! - **`am_handler_async()`** - Async AM handler: `Fn(Context) -> Future<anyhow::Result<()>>`
//!
//! ### Request-Response Handlers
//! - **`unary_handler()`** - Sync unary: `Fn(Context) -> UnifiedResponse`
//! - **`unary_handler_async()`** - Async unary: `Fn(Context) -> Future<UnifiedResponse>`
//!
//! ### Typed Request-Response Handlers
//! - **`typed_unary()`** - Sync typed: `Fn(TypedContext<I>) -> anyhow::Result<O>`
//! - **`typed_unary_async()`** - Async typed: `Fn(TypedContext<I>) -> Future<anyhow::Result<O>>`
//!
//! ## Context Objects
//!
//! All context objects include:
//! - **`message_id: MessageId`** - Unique, compact identifier for this message
//! - **`msg: Arc<Messenger>`** - The messenger API for sending messages, querying handlers, etc.

mod manager;
pub(crate) use manager::HandlerManager;

use crate::observability::{DispatchFailure, HandlerOutcome, HandlerResponseType};
use anyhow::Result;
use bytes::Bytes;
use futures::future::{BoxFuture, Ready, ready};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
#[cfg(feature = "distributed-tracing")]
use tracing::Instrument;
use tracing::{debug, error};
use velo_ext::{InstanceId, WorkerId};

use crate::messenger::common::events::{EventType, Outcome, encode_event_header};
use crate::messenger::common::messages::ResponseType;
use crate::messenger::common::responses::{ResponseId, encode_response_header};
use crate::messenger::server::dispatcher::{
    ActiveMessageDispatcher, ActiveMessageHandler, HandlerContext, InlineDispatcher,
    OrderedDispatcher, SpawnedDispatcher,
};
use crate::transports::{MessageType, SendOutcome, VeloBackend};
use derive_getters::Dissolve;
use tokio_util::task::TaskTracker;

#[inline]
async fn drive_bp(outcome: SendOutcome) {
    if let SendOutcome::Backpressured(bp) = outcome {
        bp.await;
    }
}

// ============================================================================
// Opaque Handles
// ============================================================================

pub struct Handler {
    pub(crate) dispatcher: Arc<dyn ActiveMessageDispatcher>,
}

impl Handler {
    pub fn name(&self) -> &str {
        self.dispatcher.as_ref().name()
    }

    /// Create a synchronous active message handler
    pub fn am_handler<F>(
        name: impl Into<String>,
        f: F,
    ) -> AmHandlerBuilder<SyncExecutor<F, Context, ()>>
    where
        F: Fn(Context) -> Result<()> + Send + Sync + 'static,
    {
        am_handler(name, f)
    }

    /// Create an asynchronous active message handler
    pub fn am_handler_async<F, Fut>(
        name: impl Into<String>,
        f: F,
    ) -> AmHandlerBuilder<AsyncExecutor<F, Context, ()>>
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        am_handler_async(name, f)
    }

    /// Create a synchronous unary (request-response) handler
    pub fn unary_handler<F>(
        name: impl Into<String>,
        f: F,
    ) -> UnaryHandlerBuilder<SyncExecutor<F, Context, Option<Bytes>>>
    where
        F: Fn(Context) -> UnifiedResponse + Send + Sync + 'static,
    {
        unary_handler(name, f)
    }

    /// Create an asynchronous unary (request-response) handler
    pub fn unary_handler_async<F, Fut>(
        name: impl Into<String>,
        f: F,
    ) -> UnaryHandlerBuilder<AsyncExecutor<F, Context, Option<Bytes>>>
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = UnifiedResponse> + Send + 'static,
    {
        unary_handler_async(name, f)
    }

    /// Create a synchronous typed unary handler with automatic serialization
    pub fn typed_unary<I, O, F>(
        name: impl Into<String>,
        f: F,
    ) -> TypedUnaryHandlerBuilder<SyncExecutor<F, TypedContext<I>, O>, I, O>
    where
        I: serde::de::DeserializeOwned + Send + Sync + 'static,
        O: serde::Serialize + Send + Sync + 'static,
        F: Fn(TypedContext<I>) -> Result<O> + Send + Sync + 'static,
    {
        typed_unary(name, f)
    }

    /// Create an asynchronous typed unary handler with automatic serialization
    pub fn typed_unary_async<I, O, F, Fut>(
        name: impl Into<String>,
        f: F,
    ) -> TypedUnaryHandlerBuilder<AsyncExecutor<F, TypedContext<I>, O>, I, O>
    where
        I: serde::de::DeserializeOwned + Send + Sync + 'static,
        O: serde::Serialize + Send + Sync + 'static,
        F: Fn(TypedContext<I>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        typed_unary_async(name, f)
    }
}

// ============================================================================
// Type Definitions
// ============================================================================

/// Unified response type for request-response handlers.
pub type UnifiedResponse = Result<Option<Bytes>>;

/// Dispatch mode for handlers
///
/// Marked `#[non_exhaustive]` so future modes can be added without a breaking
/// change. Match with a `_` arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DispatchMode {
    /// Execute handler inline on dispatcher task (minimal latency)
    Inline,
    /// Spawn handler on separate task (default, safer)
    Spawn,
    /// Queue onto an ordering lane drained by a single task, so messages
    /// sharing a lane key are handled in arrival order.
    ///
    /// Configured via [`OrderedConfig`]; see [`AmHandlerBuilder::ordered`].
    Ordered,
}

/// How an ordered handler partitions inbound messages into lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OrderingKey {
    /// One lane per sending instance.
    ///
    /// Messages from a single peer are handled in arrival order; messages from
    /// different peers run in parallel. This is the guarantee the transport
    /// layer actually provides — one connection per peer, read sequentially —
    /// so it is what [`AmHandlerBuilder::ordered`] selects.
    #[default]
    Sender,
    /// A single lane for the whole handler.
    ///
    /// Total arrival order across every peer, at the cost of all cross-peer
    /// parallelism: one slow sender blocks everyone.
    Global,
}

/// What an ordered handler does when a lane exceeds
/// [`OrderedConfig::max_queue_depth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OverflowPolicy {
    /// Log and count, but keep enqueuing. Visibility with no behaviour change.
    #[default]
    Warn,
    /// Drop the message and, for `AckNack`/`Unary`, send an error response so
    /// the caller fails fast instead of waiting for its own timeout.
    Reject,
}

/// Tuning for [`DispatchMode::Ordered`].
///
/// Fields are crate-private so new options can be added without breaking
/// struct literals downstream; build with [`OrderedConfig::by_sender`] /
/// [`OrderedConfig::global`] and the consuming setters.
#[derive(Debug, Clone)]
pub struct OrderedConfig {
    pub(crate) key: OrderingKey,
    pub(crate) idle_lane_ttl: Option<Duration>,
    pub(crate) max_concurrent: Option<usize>,
    pub(crate) max_queue_depth: Option<usize>,
    pub(crate) overflow: OverflowPolicy,
}

impl Default for OrderedConfig {
    fn default() -> Self {
        Self {
            key: OrderingKey::Sender,
            // Reaping matters for churn, not steady state: ephemeral clients
            // that connect, send once, and never return would otherwise leave a
            // parked task and a channel behind forever.
            idle_lane_ttl: Some(Duration::from_secs(30)),
            max_concurrent: None,
            max_queue_depth: None,
            overflow: OverflowPolicy::Warn,
        }
    }
}

impl OrderedConfig {
    /// Per-sender lanes (the default).
    pub fn by_sender() -> Self {
        Self::default()
    }

    /// A single lane for the whole handler.
    pub fn global() -> Self {
        Self {
            key: OrderingKey::Global,
            ..Self::default()
        }
    }

    /// Set the lane partitioning key.
    pub fn with_key(mut self, key: OrderingKey) -> Self {
        self.key = key;
        self
    }

    /// How long a lane may sit idle before it reaps itself.
    ///
    /// `None` keeps lanes alive for the lifetime of the handler.
    pub fn with_idle_lane_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.idle_lane_ttl = ttl;
        self
    }

    /// Cap how many lanes may be running the handler at the same instant.
    ///
    /// The permit is taken per message *inside* the lane, so per-lane ordering
    /// is unaffected: a lane that cannot get a permit parks with its queue
    /// intact. `None` (the default) means cross-lane parallelism is bounded
    /// only by the number of senders with queued work. `Some(0)` is rejected
    /// immediately because it would park every lane forever.
    pub fn with_max_concurrent(mut self, limit: Option<usize>) -> Self {
        assert!(
            limit.is_none_or(|limit| limit > 0),
            "max_concurrent must be greater than zero"
        );
        self.max_concurrent = limit;
        self
    }

    /// Soft cap on queued-but-unhandled messages, evaluated per handler.
    ///
    /// Lane channels are unbounded regardless; this only drives
    /// [`OrderedConfig::with_overflow`]. `None` (the default) disables the check.
    pub fn with_max_queue_depth(mut self, depth: Option<usize>) -> Self {
        self.max_queue_depth = depth;
        self
    }

    /// What to do once [`OrderedConfig::with_max_queue_depth`] is exceeded.
    pub fn with_overflow(mut self, policy: OverflowPolicy) -> Self {
        self.overflow = policy;
        self
    }
}

// ============================================================================
// Context Objects
// ============================================================================

/// Context passed to active message handlers
#[derive(Clone, Dissolve)]
pub struct Context {
    /// Unique identifier for this message (compact, human-readable)
    pub message_id: crate::messenger::common::MessageId,
    /// The message payload
    pub payload: Bytes,
    /// Optional user headers (for tracing, metadata, etc.)
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// The messenger API
    pub msg: Arc<crate::Messenger>,
}

/// Context passed to typed handlers (already deserialized input)
#[derive(Clone, Dissolve)]
pub struct TypedContext<I> {
    /// Unique identifier for this message (compact, human-readable)
    pub message_id: crate::messenger::common::MessageId,
    /// The deserialized input
    pub input: I,
    /// Optional user headers (for tracing, metadata, etc.)
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// The messenger API
    pub msg: Arc<crate::Messenger>,
}

/// Emits the sender-provenance accessors shared by [`Context`] and
/// [`TypedContext`].
///
/// These are methods rather than fields on purpose: both contexts derive
/// `Dissolve` and expose every field publicly, so adding one would change the
/// `.dissolve()` tuple arity *and* trip `constructible_struct_adds_field`.
macro_rules! impl_sender_accessors {
    ($ty:ident $(<$generic:ident>)?) => {
        impl $(<$generic>)? $ty $(<$generic>)? {
            /// [`WorkerId`] of the instance that sent this message.
            ///
            /// Always available: the sender mints the message id from its own
            /// response-slot arena, which bit-packs its worker id. This is the
            /// lane key used by [`OrderingKey::Sender`].
            pub fn sender_worker_id(&self) -> WorkerId {
                self.message_id.worker_id()
            }

            /// [`InstanceId`] of the sender, if known locally.
            ///
            /// Resolved from the peer registry, which is populated by the
            /// `_hello` handshake — so this can be `None` for a peer whose
            /// handshake has not landed yet. `WorkerId` is a deterministic
            /// bijection of `InstanceId`, so prefer
            /// [`sender_worker_id`](Self::sender_worker_id) when you only need
            /// a stable partition key.
            pub fn sender_instance_id(&self) -> Option<InstanceId> {
                self.msg
                    .backend()
                    .try_translate_worker_id(self.sender_worker_id())
                    .ok()
            }
        }
    };
}

impl_sender_accessors!(Context);
impl_sender_accessors!(TypedContext<I>);

// ============================================================================
// Core HandlerExecutor Trait (GAT-based, avoids async_trait)
// ============================================================================

/// Core trait for handler execution with GAT to support both sync and async
pub trait HandlerExecutor<C, T>: Send + Sync {
    type Future<'a>: Future<Output = Result<T>> + Send + 'a
    where
        Self: 'a,
        C: 'a,
        T: 'a;

    fn execute<'a>(&'a self, ctx: C) -> Self::Future<'a>
    where
        C: 'a;

    fn is_async(&self) -> bool;
}

// ============================================================================
// Sync Executor Implementation
// ============================================================================

pub struct SyncExecutor<F, C, T> {
    f: F,
    _phantom: PhantomData<fn(C) -> T>,
}

impl<F, C, T> SyncExecutor<F, C, T> {
    fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

impl<F, C, T> HandlerExecutor<C, T> for SyncExecutor<F, C, T>
where
    F: Fn(C) -> Result<T> + Send + Sync,
    C: Send + 'static,
    T: Send + 'static,
{
    type Future<'a>
        = Ready<Result<T>>
    where
        Self: 'a,
        C: 'a,
        T: 'a;

    fn execute<'a>(&'a self, ctx: C) -> Self::Future<'a>
    where
        C: 'a,
    {
        ready((self.f)(ctx))
    }

    fn is_async(&self) -> bool {
        false
    }
}

// ============================================================================
// Async Executor Implementation
// ============================================================================

pub struct AsyncExecutor<F, C, T> {
    f: F,
    _phantom: PhantomData<fn(C) -> T>,
}

impl<F, C, T> AsyncExecutor<F, C, T> {
    fn new(f: F) -> Self {
        Self {
            f,
            _phantom: PhantomData,
        }
    }
}

impl<F, Fut, C, T> HandlerExecutor<C, T> for AsyncExecutor<F, C, T>
where
    F: Fn(C) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T>> + Send + 'static,
    C: Send + 'static,
    T: Send + 'static,
{
    type Future<'a>
        = BoxFuture<'a, Result<T>>
    where
        Self: 'a,
        C: 'a,
        T: 'a;

    fn execute<'a>(&'a self, ctx: C) -> Self::Future<'a>
    where
        C: 'a,
    {
        Box::pin((self.f)(ctx))
    }

    fn is_async(&self) -> bool {
        true
    }
}

// ============================================================================
// Adapter: HandlerExecutor -> ActiveMessageHandler
// ============================================================================

struct AmExecutorAdapter<E> {
    executor: Arc<E>,
    name: String,
    metrics: OnceLock<Option<crate::observability::HandlerMetricsHandle>>,
}

impl<E> AmExecutorAdapter<E> {
    fn new(executor: E, name: String) -> Self {
        Self {
            executor: Arc::new(executor),
            name,
            metrics: OnceLock::new(),
        }
    }
}

impl<E> ActiveMessageHandler for AmExecutorAdapter<E>
where
    E: HandlerExecutor<Context, ()> + 'static,
{
    fn handle(&self, ctx: HandlerContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let request_bytes = ctx.payload.len();
        let am_ctx = Context {
            message_id: crate::messenger::common::MessageId::new(ctx.message_id),
            payload: ctx.payload,
            headers: ctx.headers.clone(),
            msg: ctx.system.clone(),
        };

        let executor = self.executor.clone();
        let name = self.name.clone();
        #[cfg(feature = "distributed-tracing")]
        let span_name = name.clone();

        let backend = ctx.system.backend().clone();
        let response_id = ctx.message_id;
        let response_type = ctx.response_type;
        let headers = ctx.headers.clone();
        #[cfg(feature = "distributed-tracing")]
        let trace_headers = headers.clone();
        let observability = ctx.system.observability();
        let handler_metrics = self
            .metrics
            .get_or_init(|| {
                ctx.system
                    .observability()
                    .as_ref()
                    .and_then(|metrics| metrics.bind_handler(&self.name))
            })
            .clone();
        let future = async move {
            let _in_flight = handler_metrics.as_ref().map(|m| m.start());
            let started = Instant::now();
            let result = executor.execute(am_ctx).await;
            let mut outcome = HandlerOutcome::Success;
            let mut response_bytes = 0usize;

            match response_type {
                ResponseType::FireAndForget => {
                    if let Err(e) = result {
                        error!("AM handler '{}' failed: {}", name, e);
                        outcome = HandlerOutcome::Error;
                    }
                }
                ResponseType::AckNack => {
                    let send_result = match result {
                        Ok(()) => send_ack(backend, response_id).await,
                        Err(err) => {
                            error!("AM handler '{}' failed: {}", name, err);
                            response_bytes = err.to_string().len();
                            outcome = HandlerOutcome::Error;
                            send_nack(backend, response_id, err.to_string()).await
                        }
                    };
                    if let Err(e) = send_result {
                        if let Some(metrics) = observability.as_ref() {
                            metrics.record_dispatch_failure(DispatchFailure::ResponseSendAckNack);
                        }
                        debug!("Failed to send ACK/NACK response: {}", e);
                    }
                }
                ResponseType::Unary => {
                    let error_message = match result {
                        Ok(()) => {
                            format!("Unary message incorrectly routed to AM handler '{}'", name)
                        }
                        Err(ref e) => {
                            format!(
                                "Unary message incorrectly routed to AM handler '{}': {}",
                                name, e
                            )
                        }
                    };
                    error!("{}", error_message);
                    outcome = HandlerOutcome::Error;
                    response_bytes = error_message.len();
                    let send_result =
                        send_response_error(backend, response_id, headers, error_message).await;
                    if let Err(e) = send_result {
                        if let Some(metrics) = observability.as_ref() {
                            metrics
                                .record_dispatch_failure(DispatchFailure::ResponseSendUnaryError);
                        }
                        debug!("Failed to send unary error response: {}", e);
                    }
                }
            }

            if let Some(metrics) = handler_metrics.as_ref() {
                metrics.finish(
                    handler_response_type(response_type),
                    outcome,
                    started.elapsed(),
                    request_bytes,
                    response_bytes,
                );
            }
        };

        #[cfg(feature = "distributed-tracing")]
        {
            let span = tracing::info_span!(
                "velo.messenger.handler",
                handler = %span_name,
                response_type = response_type_label(response_type),
                request_bytes
            );
            crate::observability::apply_remote_parent(&span, trace_headers.as_ref());
            Box::pin(future.instrument(span))
        }

        #[cfg(not(feature = "distributed-tracing"))]
        Box::pin(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

struct UnaryExecutorAdapter<E> {
    executor: Arc<E>,
    name: String,
    metrics: OnceLock<Option<crate::observability::HandlerMetricsHandle>>,
}

impl<E> UnaryExecutorAdapter<E> {
    fn new(executor: E, name: String) -> Self {
        Self {
            executor: Arc::new(executor),
            name,
            metrics: OnceLock::new(),
        }
    }
}

impl<E> ActiveMessageHandler for UnaryExecutorAdapter<E>
where
    E: HandlerExecutor<Context, Option<Bytes>> + 'static,
{
    fn handle(&self, ctx: HandlerContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let request_bytes = ctx.payload.len();
        let unary_ctx = Context {
            message_id: crate::messenger::common::MessageId::new(ctx.message_id),
            payload: ctx.payload,
            headers: ctx.headers.clone(),
            msg: ctx.system.clone(),
        };

        let executor = self.executor.clone();
        let backend = ctx.system.backend().clone();
        let response_id = ctx.message_id;
        let response_type = ctx.response_type;
        let headers = ctx.headers.clone();
        #[cfg(feature = "distributed-tracing")]
        let trace_headers = headers.clone();
        let observability = ctx.system.observability();
        let handler_name = self.name.clone();
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = &handler_name;
        #[cfg(feature = "distributed-tracing")]
        let span_handler_name = handler_name.clone();
        let handler_metrics = self
            .metrics
            .get_or_init(|| {
                ctx.system
                    .observability()
                    .as_ref()
                    .and_then(|metrics| metrics.bind_handler(&self.name))
            })
            .clone();

        let future = async move {
            let _in_flight = handler_metrics.as_ref().map(|m| m.start());
            let started = Instant::now();
            let result = executor.execute(unary_ctx).await;
            let mut outcome = HandlerOutcome::Success;
            let mut response_bytes = 0usize;

            let send_result = match (response_type, result) {
                (ResponseType::AckNack, Ok(None)) => send_ack(backend, response_id).await,
                (ResponseType::AckNack, Ok(Some(_))) => {
                    // AckNack response carries no payload on wire.
                    send_ack(backend, response_id).await
                }
                (ResponseType::AckNack, Err(err)) => {
                    let error_msg = err.to_string();
                    outcome = HandlerOutcome::Error;
                    response_bytes = error_msg.len();
                    send_nack(backend, response_id, error_msg).await
                }
                (ResponseType::Unary, Ok(None)) => {
                    send_response_ok(backend, response_id, headers.clone()).await
                }
                (ResponseType::Unary, Ok(Some(bytes))) => {
                    response_bytes = bytes.len();
                    send_response(backend, response_id, headers.clone(), bytes).await
                }
                (ResponseType::Unary, Err(err)) => {
                    let error_msg = err.to_string();
                    outcome = HandlerOutcome::Error;
                    response_bytes = error_msg.len();
                    send_response_error(backend, response_id, headers.clone(), error_msg).await
                }
                (ResponseType::FireAndForget, _) => {
                    outcome = HandlerOutcome::Error;
                    error!("FireAndForget message incorrectly routed to unary handler");
                    Ok(())
                }
            };

            if let Err(e) = send_result {
                if let Some(metrics) = observability.as_ref() {
                    metrics.record_dispatch_failure(DispatchFailure::ResponseSendUnary);
                }
                debug!("Failed to send response: {}", e);
            }

            if let Some(metrics) = handler_metrics.as_ref() {
                metrics.finish(
                    handler_response_type(response_type),
                    outcome,
                    started.elapsed(),
                    request_bytes,
                    response_bytes,
                );
            }
        };

        #[cfg(feature = "distributed-tracing")]
        {
            let span = tracing::info_span!(
                "velo.messenger.handler",
                handler = %span_handler_name,
                response_type = response_type_label(response_type),
                request_bytes
            );
            crate::observability::apply_remote_parent(&span, trace_headers.as_ref());
            Box::pin(future.instrument(span))
        }

        #[cfg(not(feature = "distributed-tracing"))]
        Box::pin(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

struct TypedUnaryExecutorAdapter<E, I, O> {
    executor: Arc<E>,
    name: String,
    metrics: OnceLock<Option<crate::observability::HandlerMetricsHandle>>,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<E, I, O> TypedUnaryExecutorAdapter<E, I, O> {
    fn new(executor: E, name: String) -> Self {
        Self {
            executor: Arc::new(executor),
            name,
            metrics: OnceLock::new(),
            _phantom: PhantomData,
        }
    }
}

impl<E, I, O> ActiveMessageHandler for TypedUnaryExecutorAdapter<E, I, O>
where
    E: HandlerExecutor<TypedContext<I>, O> + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
{
    fn handle(&self, ctx: HandlerContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let request_bytes = ctx.payload.len();
        let payload = ctx.payload;
        let system = ctx.system.clone();
        let msg_id = crate::messenger::common::MessageId::new(ctx.message_id);
        let headers = ctx.headers.clone();
        #[cfg(feature = "distributed-tracing")]
        let trace_headers = headers.clone();
        let backend = ctx.system.backend().clone();
        let response_id = ctx.message_id;
        let response_type = ctx.response_type;
        let executor = self.executor.clone();
        let handler_name = self.name.clone();
        #[cfg(not(feature = "distributed-tracing"))]
        let _ = &handler_name;
        #[cfg(feature = "distributed-tracing")]
        let span_handler_name = handler_name.clone();
        let observability = ctx.system.observability();
        let handler_metrics = self
            .metrics
            .get_or_init(|| {
                ctx.system
                    .observability()
                    .as_ref()
                    .and_then(|metrics| metrics.bind_handler(&self.name))
            })
            .clone();

        let future = async move {
            let _in_flight = handler_metrics.as_ref().map(|m| m.start());
            let started = Instant::now();
            let input: I = match if payload.is_empty() {
                serde_json::from_slice(b"null")
            } else {
                serde_json::from_slice(&payload)
            } {
                Ok(input) => input,
                Err(e) => {
                    let error_msg = format!("Failed to deserialize input: {}", e);
                    let error_msg_len = error_msg.len();
                    if let Some(metrics) = observability.as_ref() {
                        metrics.record_dispatch_failure(DispatchFailure::DeserializeTypedInput);
                    }
                    let send_result = match response_type {
                        ResponseType::AckNack => send_nack(backend, response_id, error_msg).await,
                        ResponseType::Unary => {
                            send_response_error(backend, response_id, headers.clone(), error_msg)
                                .await
                        }
                        ResponseType::FireAndForget => Ok(()),
                    };
                    if let Err(send_err) = send_result {
                        if let Some(metrics) = observability.as_ref() {
                            metrics.record_dispatch_failure(
                                DispatchFailure::ResponseSendTypedDeserialize,
                            );
                        }
                        debug!("Failed to send deserialization error: {}", send_err);
                    }
                    if let Some(metrics) = handler_metrics.as_ref() {
                        metrics.finish(
                            handler_response_type(response_type),
                            HandlerOutcome::Error,
                            started.elapsed(),
                            request_bytes,
                            error_msg_len,
                        );
                    }
                    return;
                }
            };

            let typed_ctx = TypedContext {
                message_id: msg_id,
                input,
                headers: headers.clone(),
                msg: system,
            };

            let result = executor.execute(typed_ctx).await;
            let mut outcome = HandlerOutcome::Success;
            let mut response_bytes = 0usize;

            let send_result = match (response_type, result) {
                (ResponseType::AckNack, Ok(_output)) => send_ack(backend, response_id).await,
                (ResponseType::AckNack, Err(err)) => {
                    let error_msg = err.to_string();
                    outcome = HandlerOutcome::Error;
                    response_bytes = error_msg.len();
                    send_nack(backend, response_id, error_msg).await
                }
                (ResponseType::Unary, Ok(output)) => match serde_json::to_vec(&output) {
                    Ok(serialized) => {
                        let bytes = Bytes::from(serialized);
                        response_bytes = bytes.len();
                        send_response(backend, response_id, headers.clone(), bytes).await
                    }
                    Err(e) => {
                        let error_msg = format!("Failed to serialize output: {}", e);
                        if let Some(metrics) = observability.as_ref() {
                            metrics.record_dispatch_failure(DispatchFailure::SerializeTypedOutput);
                        }
                        outcome = HandlerOutcome::Error;
                        response_bytes = error_msg.len();
                        send_response_error(backend, response_id, headers.clone(), error_msg).await
                    }
                },
                (ResponseType::Unary, Err(err)) => {
                    let error_msg = err.to_string();
                    outcome = HandlerOutcome::Error;
                    response_bytes = error_msg.len();
                    send_response_error(backend, response_id, headers.clone(), error_msg).await
                }
                (ResponseType::FireAndForget, _) => {
                    outcome = HandlerOutcome::Error;
                    error!("FireAndForget message incorrectly routed to typed unary handler");
                    Ok(())
                }
            };

            if let Err(e) = send_result {
                if let Some(metrics) = observability.as_ref() {
                    metrics.record_dispatch_failure(DispatchFailure::ResponseSendTypedUnary);
                }
                debug!("Failed to send response: {}", e);
            }

            if let Some(metrics) = handler_metrics.as_ref() {
                metrics.finish(
                    handler_response_type(response_type),
                    outcome,
                    started.elapsed(),
                    request_bytes,
                    response_bytes,
                );
            }
        };

        #[cfg(feature = "distributed-tracing")]
        {
            let span = tracing::info_span!(
                "velo.messenger.handler",
                handler = %span_handler_name,
                response_type = response_type_label(response_type),
                request_bytes
            );
            crate::observability::apply_remote_parent(&span, trace_headers.as_ref());
            Box::pin(future.instrument(span))
        }

        #[cfg(not(feature = "distributed-tracing"))]
        Box::pin(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// Helper Functions for Sending Responses
// ============================================================================

struct AckErrorHandler;
impl crate::transports::TransportErrorHandler for AckErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        error!("Failed to send ACK: {}", error);
    }
}

struct NackErrorHandler;
impl crate::transports::TransportErrorHandler for NackErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        error!("Failed to send NACK: {}", error);
    }
}

struct ResponseErrorHandler;
impl crate::transports::TransportErrorHandler for ResponseErrorHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        error!("Failed to send response: {}", error);
    }
}

static ACK_ERROR_HANDLER: std::sync::OnceLock<Arc<dyn crate::transports::TransportErrorHandler>> =
    std::sync::OnceLock::new();
static NACK_ERROR_HANDLER: std::sync::OnceLock<Arc<dyn crate::transports::TransportErrorHandler>> =
    std::sync::OnceLock::new();
static RESPONSE_ERROR_HANDLER: std::sync::OnceLock<
    Arc<dyn crate::transports::TransportErrorHandler>,
> = std::sync::OnceLock::new();

#[inline(always)]
fn get_ack_error_handler() -> Arc<dyn crate::transports::TransportErrorHandler> {
    ACK_ERROR_HANDLER
        .get_or_init(|| Arc::new(AckErrorHandler))
        .clone()
}

#[inline(always)]
fn get_nack_error_handler() -> Arc<dyn crate::transports::TransportErrorHandler> {
    NACK_ERROR_HANDLER
        .get_or_init(|| Arc::new(NackErrorHandler))
        .clone()
}

#[inline(always)]
fn get_response_error_handler() -> Arc<dyn crate::transports::TransportErrorHandler> {
    RESPONSE_ERROR_HANDLER
        .get_or_init(|| Arc::new(ResponseErrorHandler))
        .clone()
}

async fn send_ack(backend: Arc<VeloBackend>, response_id: ResponseId) -> Result<()> {
    let header = encode_event_header(EventType::Ack(response_id, Outcome::Ok));

    let outcome = backend.send_message_to_worker(
        WorkerId::from_u64(response_id.worker_id()),
        header,
        Bytes::new(),
        MessageType::Ack,
        get_ack_error_handler(),
    )?;
    drive_bp(outcome).await;

    Ok(())
}

#[cfg(feature = "distributed-tracing")]
fn response_type_label(response_type: ResponseType) -> &'static str {
    match response_type {
        ResponseType::FireAndForget => "fire_and_forget",
        ResponseType::AckNack => "ack_nack",
        ResponseType::Unary => "unary",
    }
}

fn handler_response_type(response_type: ResponseType) -> HandlerResponseType {
    match response_type {
        ResponseType::FireAndForget => HandlerResponseType::FireAndForget,
        ResponseType::AckNack => HandlerResponseType::AckNack,
        ResponseType::Unary => HandlerResponseType::Unary,
    }
}

async fn send_nack(
    backend: Arc<VeloBackend>,
    response_id: ResponseId,
    error_message: String,
) -> Result<()> {
    let header = encode_event_header(EventType::Ack(response_id, Outcome::Error));
    let payload = Bytes::from(error_message.into_bytes());

    let outcome = backend.send_message_to_worker(
        WorkerId::from_u64(response_id.worker_id()),
        header,
        payload,
        MessageType::Ack,
        get_nack_error_handler(),
    )?;
    drive_bp(outcome).await;

    Ok(())
}

async fn send_response_ok(
    backend: Arc<VeloBackend>,
    response_id: ResponseId,
    headers: Option<std::collections::HashMap<String, String>>,
) -> Result<()> {
    let header = encode_response_header(response_id, Outcome::Ok, headers)
        .map_err(|e| anyhow::anyhow!("Failed to encode response header: {}", e))?;

    let outcome = backend.send_message_to_worker(
        WorkerId::from_u64(response_id.worker_id()),
        header,
        Bytes::new(),
        MessageType::Response,
        get_response_error_handler(),
    )?;
    drive_bp(outcome).await;

    Ok(())
}

async fn send_response(
    backend: Arc<VeloBackend>,
    response_id: ResponseId,
    headers: Option<std::collections::HashMap<String, String>>,
    payload: Bytes,
) -> Result<()> {
    let header = encode_response_header(response_id, Outcome::Ok, headers)
        .map_err(|e| anyhow::anyhow!("Failed to encode response header: {}", e))?;

    let outcome = backend.send_message_to_worker(
        WorkerId::from_u64(response_id.worker_id()),
        header,
        payload,
        MessageType::Response,
        get_response_error_handler(),
    )?;
    drive_bp(outcome).await;

    Ok(())
}

async fn send_response_error(
    backend: Arc<VeloBackend>,
    response_id: ResponseId,
    headers: Option<std::collections::HashMap<String, String>>,
    error_message: String,
) -> Result<()> {
    let header = encode_response_header(response_id, Outcome::Error, headers)
        .map_err(|e| anyhow::anyhow!("Failed to encode response header: {}", e))?;
    let payload = Bytes::from(error_message.into_bytes());

    let outcome = backend.send_message_to_worker(
        WorkerId::from_u64(response_id.worker_id()),
        header,
        payload,
        MessageType::Response,
        get_response_error_handler(),
    )?;
    drive_bp(outcome).await;

    Ok(())
}

// ============================================================================
// Builder Structs
// ============================================================================

/// Emits the dispatch-mode selectors shared by all three handler builders.
///
/// The setters need no trait bounds, so they go in their own bare `impl` block
/// rather than being threaded through each builder's `where` clause.
macro_rules! impl_dispatch_mode_setters {
    ($ty:ident $(, $generic:ident)*) => {
        impl<E $(, $generic)*> $ty<E $(, $generic)*> {
            /// Run the handler on a task spawned per message. Default.
            pub fn spawn(mut self) -> Self {
                self.dispatch_mode = DispatchMode::Spawn;
                self.ordered = None;
                self
            }

            /// Run the handler on a task not registered with the messenger's
            /// tracker.
            pub fn inline(mut self) -> Self {
                self.dispatch_mode = DispatchMode::Inline;
                self.ordered = None;
                self
            }

            /// Handle messages from each sending instance in arrival order,
            /// with different senders running in parallel.
            ///
            /// Each sender gets an unbounded queue drained by one task. See
            /// [`OrderingKey::Sender`].
            ///
            /// Ordering is preserved, not created: if a peer is reachable over
            /// several transports, or a connection drops and reconnects
            /// mid-stream, arrival order was already lost upstream.
            ///
            /// Note that on a *unary* handler this serialises request/response
            /// per sender — a client issuing 100 concurrent calls will have
            /// them served one at a time.
            pub fn ordered(self) -> Self {
                self.ordered_with(OrderedConfig::by_sender())
            }

            /// Handle every message on a single lane, in total arrival order
            /// across all senders. See [`OrderingKey::Global`].
            pub fn ordered_global(self) -> Self {
                self.ordered_with(OrderedConfig::global())
            }

            /// Ordered dispatch with explicit configuration.
            pub fn ordered_with(mut self, config: OrderedConfig) -> Self {
                self.dispatch_mode = DispatchMode::Ordered;
                self.ordered = Some(config);
                self
            }

            /// Cap how many lanes may be running the handler at once.
            ///
            /// Only meaningful in ordered mode; call it after `.ordered()`.
            /// Ignored (with a warning) in any other mode. `0` is rejected
            /// immediately because it would park every lane forever.
            pub fn max_concurrent(mut self, limit: usize) -> Self {
                match self.ordered.take() {
                    Some(config) => {
                        self.ordered = Some(config.with_max_concurrent(Some(limit)));
                    }
                    None => {
                        tracing::warn!(
                            target: "crate::messenger::handlers",
                            handler = %self.name,
                            "max_concurrent() ignored: handler is not in ordered mode. \
                             Call .ordered() first."
                        );
                    }
                }
                self
            }
        }
    };
}

/// Picks the dispatcher for a built handler.
///
/// `ordered` is `Some` exactly when `mode` is [`DispatchMode::Ordered`], but the
/// fallback keeps this total rather than panicking on a future mode.
fn make_dispatcher<H: ActiveMessageHandler + 'static>(
    adapter: H,
    mode: DispatchMode,
    ordered: Option<OrderedConfig>,
) -> Arc<dyn ActiveMessageDispatcher> {
    match mode {
        DispatchMode::Inline => Arc::new(InlineDispatcher::new(adapter)),
        DispatchMode::Ordered => {
            Arc::new(OrderedDispatcher::new(adapter, ordered.unwrap_or_default()))
        }
        DispatchMode::Spawn => Arc::new(SpawnedDispatcher::new(adapter, TaskTracker::new())),
    }
}

pub struct AmHandlerBuilder<E> {
    executor: E,
    name: String,
    dispatch_mode: DispatchMode,
    ordered: Option<OrderedConfig>,
}

impl<E> AmHandlerBuilder<E>
where
    E: HandlerExecutor<Context, ()> + 'static,
{
    fn new(executor: E, name: String) -> Self {
        Self {
            executor,
            name,
            dispatch_mode: DispatchMode::Spawn,
            ordered: None,
        }
    }

    pub fn build(self) -> Handler {
        let adapter = AmExecutorAdapter::new(self.executor, self.name);
        let dispatcher = make_dispatcher(adapter, self.dispatch_mode, self.ordered);
        Handler { dispatcher }
    }
}

impl_dispatch_mode_setters!(AmHandlerBuilder);

pub struct UnaryHandlerBuilder<E> {
    executor: E,
    name: String,
    dispatch_mode: DispatchMode,
    ordered: Option<OrderedConfig>,
}

impl<E> UnaryHandlerBuilder<E>
where
    E: HandlerExecutor<Context, Option<Bytes>> + 'static,
{
    fn new(executor: E, name: String) -> Self {
        Self {
            executor,
            name,
            dispatch_mode: DispatchMode::Spawn,
            ordered: None,
        }
    }

    pub fn build(self) -> Handler {
        let adapter = UnaryExecutorAdapter::new(self.executor, self.name);
        let dispatcher = make_dispatcher(adapter, self.dispatch_mode, self.ordered);
        Handler { dispatcher }
    }
}

impl_dispatch_mode_setters!(UnaryHandlerBuilder);

pub struct TypedUnaryHandlerBuilder<E, I, O> {
    executor: E,
    name: String,
    dispatch_mode: DispatchMode,
    ordered: Option<OrderedConfig>,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<E, I, O> TypedUnaryHandlerBuilder<E, I, O>
where
    E: HandlerExecutor<TypedContext<I>, O> + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
{
    fn new(executor: E, name: String) -> Self {
        Self {
            executor,
            name,
            dispatch_mode: DispatchMode::Spawn,
            ordered: None,
            _phantom: PhantomData,
        }
    }

    pub fn build(self) -> Handler {
        let adapter = TypedUnaryExecutorAdapter::new(self.executor, self.name);
        let dispatcher = make_dispatcher(adapter, self.dispatch_mode, self.ordered);
        Handler { dispatcher }
    }
}

impl_dispatch_mode_setters!(TypedUnaryHandlerBuilder, I, O);

// ============================================================================
// Entry Point Functions
// ============================================================================

fn am_handler<F>(name: impl Into<String>, f: F) -> AmHandlerBuilder<SyncExecutor<F, Context, ()>>
where
    F: Fn(Context) -> Result<()> + Send + Sync + 'static,
{
    let name = name.into();
    let executor = SyncExecutor::new(f);
    AmHandlerBuilder::new(executor, name)
}

fn am_handler_async<F, Fut>(
    name: impl Into<String>,
    f: F,
) -> AmHandlerBuilder<AsyncExecutor<F, Context, ()>>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let name = name.into();
    let executor = AsyncExecutor::new(f);
    AmHandlerBuilder::new(executor, name)
}

fn unary_handler<F>(
    name: impl Into<String>,
    f: F,
) -> UnaryHandlerBuilder<SyncExecutor<F, Context, Option<Bytes>>>
where
    F: Fn(Context) -> UnifiedResponse + Send + Sync + 'static,
{
    let name = name.into();
    let executor = SyncExecutor::new(f);
    UnaryHandlerBuilder::new(executor, name)
}

fn unary_handler_async<F, Fut>(
    name: impl Into<String>,
    f: F,
) -> UnaryHandlerBuilder<AsyncExecutor<F, Context, Option<Bytes>>>
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = UnifiedResponse> + Send + 'static,
{
    let name = name.into();
    let executor = AsyncExecutor::new(f);
    UnaryHandlerBuilder::new(executor, name)
}

fn typed_unary<I, O, F>(
    name: impl Into<String>,
    f: F,
) -> TypedUnaryHandlerBuilder<SyncExecutor<F, TypedContext<I>, O>, I, O>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Result<O> + Send + Sync + 'static,
{
    let name = name.into();
    let executor = SyncExecutor::new(f);
    TypedUnaryHandlerBuilder::new(executor, name)
}

fn typed_unary_async<I, O, F, Fut>(
    name: impl Into<String>,
    f: F,
) -> TypedUnaryHandlerBuilder<AsyncExecutor<F, TypedContext<I>, O>, I, O>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    let name = name.into();
    let executor = AsyncExecutor::new(f);
    TypedUnaryHandlerBuilder::new(executor, name)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct CalcRequest {
        a: f64,
        b: f64,
        operation: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct CalcResponse {
        result: f64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct PingRequest {
        message: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct PingResponse {
        echo: String,
    }

    #[test]
    fn test_am_handler_builder() {
        let handler = am_handler("test_am", |_ctx| Ok(())).build();
        assert_eq!(handler.name(), "test_am");

        let handler = am_handler("test_am_inline", |_ctx| Ok(())).inline().build();
        assert_eq!(handler.name(), "test_am_inline");

        let handler = am_handler("test_am_spawn", |_ctx| Ok(())).spawn().build();
        assert_eq!(handler.name(), "test_am_spawn");
    }

    #[test]
    fn test_am_handler_async_builder() {
        let handler = am_handler_async("test_am_async", |_ctx| async move { Ok(()) }).build();
        assert_eq!(handler.name(), "test_am_async");

        let handler = am_handler_async("test_am_async_inline", |_ctx| async move { Ok(()) })
            .inline()
            .build();
        assert_eq!(handler.name(), "test_am_async_inline");
    }

    #[test]
    fn test_unary_handler_builder() {
        let handler = unary_handler("test_unary", |_ctx| Ok(None)).build();
        assert_eq!(handler.name(), "test_unary");

        let handler = unary_handler("test_unary_inline", |_ctx| Ok(None))
            .inline()
            .build();
        assert_eq!(handler.name(), "test_unary_inline");
    }

    #[test]
    fn test_unary_handler_async_builder() {
        let handler =
            unary_handler_async("test_unary_async", |_ctx| async move { Ok(None) }).build();
        assert_eq!(handler.name(), "test_unary_async");
    }

    #[test]
    fn test_typed_unary_builder() {
        let handler = typed_unary("test_typed", |ctx: TypedContext<PingRequest>| {
            Ok(PingResponse {
                echo: ctx.input.message,
            })
        })
        .build();
        assert_eq!(handler.name(), "test_typed");

        let handler = typed_unary("test_typed_inline", |ctx: TypedContext<PingRequest>| {
            Ok(PingResponse {
                echo: ctx.input.message,
            })
        })
        .inline()
        .build();
        assert_eq!(handler.name(), "test_typed_inline");
    }

    #[test]
    fn test_typed_unary_async_builder() {
        let handler = typed_unary_async(
            "test_typed_async",
            |ctx: TypedContext<PingRequest>| async move {
                Ok(PingResponse {
                    echo: ctx.input.message,
                })
            },
        )
        .build();
        assert_eq!(handler.name(), "test_typed_async");
    }

    #[test]
    fn test_typed_unary_calculator() {
        let handler = typed_unary("calculator", |ctx: TypedContext<CalcRequest>| {
            let req = ctx.input;
            let result = match req.operation.as_str() {
                "add" => req.a + req.b,
                "subtract" => req.a - req.b,
                "multiply" => req.a * req.b,
                "divide" => {
                    if req.b == 0.0 {
                        return Err(anyhow::anyhow!("Division by zero"));
                    }
                    req.a / req.b
                }
                _ => return Err(anyhow::anyhow!("Unknown operation: {}", req.operation)),
            };
            Ok(CalcResponse { result })
        })
        .build();

        assert_eq!(handler.name(), "calculator");
    }

    #[test]
    fn test_dispatch_modes() {
        let handler = am_handler("default", |_ctx| Ok(())).build();
        assert_eq!(handler.name(), "default");

        let handler = am_handler("inline", |_ctx| Ok(())).inline().build();
        assert_eq!(handler.name(), "inline");

        let handler = am_handler("spawn", |_ctx| Ok(())).spawn().build();
        assert_eq!(handler.name(), "spawn");
    }

    #[test]
    fn test_ordered_dispatch_modes_build_on_every_builder() {
        // The dispatch-mode setters come from a macro, so this pins that all
        // three builders actually got them and that `build()` accepts the mode.
        let handler = am_handler("am_ordered", |_ctx| Ok(())).ordered().build();
        assert_eq!(handler.name(), "am_ordered");

        let handler = unary_handler("unary_ordered", |_ctx| Ok(None))
            .ordered_global()
            .build();
        assert_eq!(handler.name(), "unary_ordered");

        let handler = typed_unary("typed_ordered", |ctx: TypedContext<PingRequest>| {
            Ok(PingResponse {
                echo: ctx.input.message,
            })
        })
        .ordered()
        .max_concurrent(4)
        .build();
        assert_eq!(handler.name(), "typed_ordered");

        let handler = am_handler_async("am_ordered_with", |_ctx| async move { Ok(()) })
            .ordered_with(
                OrderedConfig::by_sender()
                    .with_idle_lane_ttl(None)
                    .with_max_queue_depth(Some(16))
                    .with_overflow(OverflowPolicy::Reject),
            )
            .build();
        assert_eq!(handler.name(), "am_ordered_with");
    }

    #[test]
    fn test_ordered_defaults_to_per_sender_lanes() {
        let builder = am_handler("defaults", |_ctx| Ok(())).ordered();
        assert_eq!(builder.dispatch_mode, DispatchMode::Ordered);
        let config = builder.ordered.expect("ordered config");
        assert_eq!(config.key, OrderingKey::Sender);
        assert_eq!(config.idle_lane_ttl, Some(Duration::from_secs(30)));
        assert_eq!(config.max_concurrent, None, "unbounded unless asked");
        assert_eq!(config.max_queue_depth, None, "unbounded unless asked");
        assert_eq!(config.overflow, OverflowPolicy::Warn);

        let builder = am_handler("global", |_ctx| Ok(())).ordered_global();
        assert_eq!(
            builder.ordered.expect("ordered config").key,
            OrderingKey::Global
        );
    }

    #[test]
    fn test_dispatch_mode_last_call_wins() {
        // `.ordered()` after `.spawn()` wins...
        let builder = am_handler("a", |_ctx| Ok(())).spawn().ordered();
        assert_eq!(builder.dispatch_mode, DispatchMode::Ordered);
        assert!(builder.ordered.is_some());

        // ...and `.spawn()` after `.ordered()` wins, clearing the config so a
        // stale `max_concurrent` cannot leak into a non-ordered handler.
        let builder = am_handler("b", |_ctx| Ok(()))
            .ordered()
            .max_concurrent(8)
            .spawn();
        assert_eq!(builder.dispatch_mode, DispatchMode::Spawn);
        assert!(builder.ordered.is_none());

        let builder = am_handler("c", |_ctx| Ok(())).ordered().inline();
        assert_eq!(builder.dispatch_mode, DispatchMode::Inline);
        assert!(builder.ordered.is_none());
    }

    #[test]
    fn test_max_concurrent_applies_only_in_ordered_mode() {
        let builder = am_handler("limited", |_ctx| Ok(()))
            .ordered()
            .max_concurrent(32);
        assert_eq!(
            builder.ordered.expect("ordered config").max_concurrent,
            Some(32)
        );

        // Outside ordered mode there is no lane to limit, so this warns and is
        // otherwise a no-op rather than silently switching modes.
        let builder = am_handler("unlimited", |_ctx| Ok(())).max_concurrent(32);
        assert_eq!(builder.dispatch_mode, DispatchMode::Spawn);
        assert!(builder.ordered.is_none());
    }

    #[test]
    #[should_panic(expected = "max_concurrent must be greater than zero")]
    fn test_ordered_config_rejects_zero_concurrency() {
        let _ = OrderedConfig::by_sender().with_max_concurrent(Some(0));
    }
}
