// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Message dispatcher for active message routing.

use crate::observability::{DispatchFailure, OrderedMetricsHandle};
use crate::transports::VeloBackend;
use bytes::Bytes;
use dashmap::DashMap;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio_util::task::TaskTracker;
use tracing::{error, trace, warn};
use velo_ext::WorkerId;

use crate::Messenger;
use crate::messenger::common::events::{EventType, Outcome, encode_event_header};
use crate::messenger::common::messages::ResponseType;
use crate::messenger::common::responses::ResponseId;
use crate::messenger::handlers::{OrderedConfig, OrderingKey, OverflowPolicy};
use crate::messenger::server::lanes::{LaneObserver, LaneRouter, LaneRouterConfig};

/// Context passed to handlers during dispatch.
#[derive(Clone)]
pub(crate) struct HandlerContext {
    /// The response ID for correlation
    pub message_id: ResponseId,

    /// Message payload
    pub payload: Bytes,

    /// Response type (FireAndForget, AckNack, Unary)
    pub response_type: ResponseType,

    /// Optional user headers (for tracing, metadata, etc.)
    pub headers: Option<std::collections::HashMap<String, String>>,

    /// The messenger system for handler access
    pub system: Arc<Messenger>,
}

/// Base trait for active message handlers.
pub(crate) trait ActiveMessageHandler: Send + Sync {
    /// Handle a message asynchronously
    fn handle(
        &self,
        ctx: HandlerContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

    /// Get the handler name
    fn name(&self) -> &str;
}

/// Trait for dispatching messages to handlers.
pub(crate) trait ActiveMessageDispatcher: Send + Sync {
    /// Get the handler name
    fn name(&self) -> &str;

    /// Dispatch a message to the handler (non-async, kicks off handler execution)
    fn dispatch(&self, ctx: HandlerContext);
}

/// Dispatcher implementation that spawns handlers on a task tracker.
pub(crate) struct SpawnedDispatcher<H: ActiveMessageHandler> {
    handler: Arc<H>,
    task_tracker: TaskTracker,
}

impl<H: ActiveMessageHandler> SpawnedDispatcher<H> {
    pub fn new(handler: H, task_tracker: TaskTracker) -> Self {
        Self {
            handler: Arc::new(handler),
            task_tracker,
        }
    }
}

impl<H: ActiveMessageHandler + 'static> ActiveMessageDispatcher for SpawnedDispatcher<H> {
    fn name(&self) -> &str {
        self.handler.name()
    }

    fn dispatch(&self, ctx: HandlerContext) {
        let handler = self.handler.clone();
        let handler_name = handler.name().to_string();

        self.task_tracker.spawn(async move {
            trace!(target: "crate::messenger::dispatcher", handler = %handler_name, "Handler task started");
            handler.handle(ctx).await;
            trace!(target: "crate::messenger::dispatcher", handler = %handler_name, "Handler task completed");
        });
    }
}

/// Dispatcher implementation that spawns handlers on a detached task.
///
/// Despite the name this does not execute on the dispatcher task; it is
/// [`SpawnedDispatcher`] without the task-tracker registration, so a graceful
/// shutdown cannot wait for handlers dispatched this way.
pub(crate) struct InlineDispatcher<H: ActiveMessageHandler> {
    handler: Arc<H>,
}

impl<H: ActiveMessageHandler> InlineDispatcher<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

impl<H: ActiveMessageHandler + 'static> ActiveMessageDispatcher for InlineDispatcher<H> {
    fn name(&self) -> &str {
        self.handler.name()
    }

    fn dispatch(&self, ctx: HandlerContext) {
        let handler = self.handler.clone();

        tokio::spawn(async move {
            handler.handle(ctx).await;
        });
    }
}

/// Dispatcher implementation that serialises handler execution on per-key
/// ordering lanes.
///
/// Each lane is an unbounded channel drained by a single task, so messages
/// sharing a lane key are handled strictly in arrival order. Lanes for distinct
/// keys run concurrently, bounded by an optional semaphore.
///
/// Ordering is *preserved*, not created: this dispatcher hands messages to the
/// handler in the order they reached the messenger's receive loop. If a peer is
/// reachable over several transports, or a connection drops and reconnects
/// mid-stream, arrival order was already scrambled upstream and no dispatcher
/// can restore it.
pub(crate) struct OrderedDispatcher<H: ActiveMessageHandler> {
    handler: Arc<H>,
    config: OrderedConfig,
    /// Built on first dispatch — the runtime handle, task tracker, and metrics
    /// registry it needs are only reachable via `HandlerContext::system`, which
    /// does not exist when the handler is built.
    bound: OnceLock<BoundRouter>,
    /// Caps how many lanes may be mid-handler at once. Acquired per message
    /// *inside* the lane loop, so per-lane ordering is untouched: a lane that
    /// cannot get a permit parks with its queue intact.
    limiter: Option<Arc<Semaphore>>,
    /// The rendezvous-ordering caveat is worth saying once per handler, not
    /// once per message.
    rendezvous_warning: std::sync::Once,
    /// Likewise for load shedding: the counter carries the rate, the log line
    /// only needs to make it discoverable.
    shed_warning: std::sync::Once,
}

/// The parts of an [`OrderedDispatcher`] that need a live `Messenger`.
struct BoundRouter {
    router: Arc<LaneRouter<LaneKey, LaneItem>>,
    metrics: Option<OrderedMetricsHandle>,
}

/// The partition a message is routed to.
///
/// `None` is the single lane used by [`OrderingKey::Global`]; `Some(worker)`
/// is the per-sender lane used by [`OrderingKey::Sender`].
type LaneKey = Option<WorkerId>;

/// A message queued on an ordering lane.
struct LaneItem {
    ctx: HandlerContext,
    enqueued_at: Instant,
}

/// Bridges lane lifecycle events onto the Prometheus handle.
struct OrderedLaneObserver {
    metrics: OrderedMetricsHandle,
}

impl LaneObserver for OrderedLaneObserver {
    fn lane_created(&self) {
        self.metrics.lane_created();
    }

    fn lane_closed(&self) {
        self.metrics.lane_closed();
    }
}

impl<H: ActiveMessageHandler + 'static> OrderedDispatcher<H> {
    pub fn new(handler: H, config: OrderedConfig) -> Self {
        let limiter = config
            .max_concurrent
            .map(|limit| Arc::new(Semaphore::new(limit)));
        Self {
            handler: Arc::new(handler),
            config,
            bound: OnceLock::new(),
            limiter,
            rendezvous_warning: std::sync::Once::new(),
            shed_warning: std::sync::Once::new(),
        }
    }

    fn lane_key(&self, ctx: &HandlerContext) -> LaneKey {
        match self.config.key {
            OrderingKey::Global => None,
            // The sender's worker id is bit-packed into the response id it
            // minted, so this needs no wire support. `WorkerId` is a
            // collision-resistant hash of the sender's `InstanceId`, so
            // keying on it partitions by sending instance. It is 128 bits down
            // to 64, so a collision would merge two peers onto one lane --
            // harmless for ordering (it only over-serialises), which is why
            // this is the lane key rather than the identity handlers see.
            OrderingKey::Sender => Some(WorkerId::from_u64(ctx.message_id.worker_id())),
        }
    }

    fn bind(&self, system: &Arc<Messenger>) -> BoundRouter {
        let handler = self.handler.clone();
        // `Arc<str>`, not `String`: the consumer clones this per message but
        // only reads it on the panic branch.
        let handler_name: Arc<str> = Arc::from(self.handler.name());
        let limiter = self.limiter.clone();
        let metrics = system
            .observability()
            .as_ref()
            .and_then(|m| m.bind_ordered_dispatcher(&handler_name));

        let consumer_metrics = metrics.clone();
        let consumer = Arc::new(move |item: LaneItem| {
            let handler = handler.clone();
            let handler_name = Arc::clone(&handler_name);
            let limiter = limiter.clone();
            let metrics = consumer_metrics.clone();

            Box::pin(async move {
                if let Some(metrics) = metrics.as_ref() {
                    metrics.observe_wait(item.enqueued_at.elapsed());
                }

                // Held for the duration of the handler. Tokio's semaphore is
                // FIFO-fair, so a busy handler cannot starve any lane.
                let _permit = match limiter.as_ref() {
                    Some(sem) => sem.clone().acquire_owned().await.ok(),
                    None => None,
                };

                let ctx = item.ctx;
                let message_id = ctx.message_id;
                let response_type = ctx.response_type;
                let system = ctx.system.clone();

                // Unlike `SpawnedDispatcher`, a panic here would take down the
                // whole lane task rather than a single message — and every
                // later message for that key with it. Catch it so the lane
                // survives, and unblock the caller rather than letting it hang
                // to timeout. (Inert under `panic = "abort"`.)
                //
                // `handle()` is called *inside* the async block on purpose.
                // `AssertUnwindSafe(handler.handle(ctx))` would evaluate
                // `handle()` first and only wrap the future it returns, leaving
                // the adapter's synchronous prologue — context construction and
                // the lazy `bind_handler` metrics binding — outside the guard.
                let outcome = AssertUnwindSafe(async move { handler.handle(ctx).await })
                    .catch_unwind()
                    .await;

                if outcome.is_err() {
                    if let Some(metrics) = system.observability().as_ref() {
                        metrics.record_dispatch_failure(DispatchFailure::OrderedHandlerPanic);
                    }
                    error!(
                        target: "crate::messenger::dispatcher",
                        handler = %handler_name,
                        message_id = %message_id,
                        "Ordered handler panicked; lane preserved"
                    );
                    Self::fail_fast(&system, message_id, response_type, "handler panicked");
                }

                if let Some(metrics) = metrics.as_ref() {
                    metrics.dequeued();
                }
            }) as BoxFuture<'static, ()>
        });

        let observer = metrics
            .clone()
            .map(|metrics| Arc::new(OrderedLaneObserver { metrics }) as Arc<dyn LaneObserver>);

        let router = Arc::new(LaneRouter::new(
            consumer,
            LaneRouterConfig {
                idle_ttl: self.config.idle_lane_ttl,
                runtime: system.runtime().clone(),
                tracker: system.tracker().clone(),
                observer,
            },
        ));

        BoundRouter { router, metrics }
    }

    /// Send an error response so a waiting caller fails immediately instead of
    /// hanging until its own timeout. No-op for fire-and-forget.
    fn fail_fast(
        system: &Arc<Messenger>,
        message_id: ResponseId,
        response_type: ResponseType,
        reason: &'static str,
    ) {
        if matches!(response_type, ResponseType::FireAndForget) {
            return;
        }
        let backend = system.backend().clone();
        tokio::spawn(async move {
            if let Err(e) = DispatcherHub::send_error_response_static(
                &backend,
                message_id,
                format!("Handler failed: {reason}"),
            )
            .await
            {
                error!(
                    target: "crate::messenger::dispatcher",
                    "Failed to send error response for ordered handler: {}", e
                );
            }
        });
    }
}

impl<H: ActiveMessageHandler + 'static> ActiveMessageDispatcher for OrderedDispatcher<H> {
    fn name(&self) -> &str {
        self.handler.name()
    }

    fn dispatch(&self, ctx: HandlerContext) {
        // One lazy init for both the router and the pre-labelled metrics, so
        // the hot path does no `with_label_values` lookups.
        let bound = self.bound.get_or_init(|| self.bind(&ctx.system));
        let metrics = bound.metrics.as_ref();

        // The rendezvous path resolves large payloads in a detached task before
        // dispatching (see `create_message_handler`), so two rendezvous
        // messages from one sender can reach us out of order. Ordered mode
        // cannot restore that; warn once so it is not silently surprising.
        if ctx
            .headers
            .as_ref()
            .is_some_and(|h| h.contains_key(crate::messenger::large_payload::RV_HEADER_KEY))
        {
            self.rendezvous_warning.call_once(|| {
                warn!(
                    target: "crate::messenger::dispatcher",
                    handler = %self.handler.name(),
                    "Rendezvous (large-payload) messages resolve out-of-band and are not \
                     ordered relative to each other, even on an ordered handler"
                );
            });
        }

        // `max_queue_depth` bounds a single lane, not the handler: with
        // per-sender lanes a handler-wide cap would let one backed-up peer shed
        // traffic from peers whose lanes are empty, defeating the isolation
        // `OrderingKey::Sender` exists to provide. Only `Reject` passes the cap
        // down as an admission limit; `Warn` always enqueues and inspects the
        // resulting depth.
        let capacity = match self.config.overflow {
            OverflowPolicy::Reject => self.config.max_queue_depth,
            OverflowPolicy::Warn => None,
        };

        let key = self.lane_key(&ctx);
        let message_id = ctx.message_id;
        let response_type = ctx.response_type;
        let system = ctx.system.clone();

        match bound.router.route(
            key,
            LaneItem {
                ctx,
                enqueued_at: Instant::now(),
            },
            capacity,
        ) {
            Ok(depth_before) => {
                if let Some(metrics) = metrics {
                    metrics.enqueued();
                }
                // Edge-triggered: `depth_before == limit` is the message that
                // takes this lane over the cap. Deeper backlogs stay quiet
                // until the lane drains below the limit and crosses again, so
                // this cannot flood the receive loop the way a per-message
                // warn would.
                if let Some(limit) = self.config.max_queue_depth
                    && depth_before == limit
                {
                    warn!(
                        target: "crate::messenger::dispatcher",
                        handler = %self.handler.name(),
                        limit,
                        lane = ?key,
                        "Ordered lane exceeded max_queue_depth"
                    );
                }
            }
            Err(_shed) => {
                // Every shed message is counted; the log line fires once per
                // handler so a shed storm cannot flood. `OrderedLaneShed` is
                // the signal to alert on.
                if let Some(m) = system.observability().as_ref() {
                    m.record_dispatch_failure(DispatchFailure::OrderedLaneShed);
                }
                self.shed_warning.call_once(|| {
                    warn!(
                        target: "crate::messenger::dispatcher",
                        handler = %self.handler.name(),
                        limit = self.config.max_queue_depth.unwrap_or_default(),
                        lane = ?key,
                        "Shedding messages: ordered lane is at max_queue_depth. \
                         Logged once per handler; see velo_messenger_dispatch_failures_total \
                         {{reason=\"ordered_lane_shed\"}} for the rate"
                    );
                });
                Self::fail_fast(
                    &system,
                    message_id,
                    response_type,
                    "ordered lane queue full",
                );
            }
        }
    }
}

/// Main message dispatcher hub that routes messages to handlers.
pub(crate) struct DispatcherHub {
    /// Handler registry (lock-free for fast dispatch).
    /// Shared with HandlerManager so registration is immediately visible.
    handlers: Arc<DashMap<String, Arc<dyn ActiveMessageDispatcher>>>,

    /// Backend for sending messages
    backend: Arc<VeloBackend>,

    /// Messenger system reference (late-bound via OnceLock)
    system: OnceLock<Arc<Messenger>>,

    /// Notifies waiters when `system` has been set
    system_ready: tokio::sync::Notify,
}

impl DispatcherHub {
    /// Create a new dispatcher hub
    pub fn new(backend: Arc<VeloBackend>) -> Self {
        Self {
            handlers: Arc::new(DashMap::new()),
            backend,
            system: OnceLock::new(),
            system_ready: tokio::sync::Notify::new(),
        }
    }

    /// Initialize the system reference (must be called exactly once before dispatching)
    pub fn set_system(&self, system: Arc<Messenger>) -> anyhow::Result<()> {
        self.system
            .set(system)
            .map_err(|_| anyhow::anyhow!("System already initialized"))?;
        self.system_ready.notify_waiters();
        Ok(())
    }

    /// Get the system reference (panics if not initialized)
    pub(crate) fn system(&self) -> &Arc<Messenger> {
        self.system
            .get()
            .expect("System must be initialized before dispatching messages")
    }

    /// Wait until the system reference is available, then return it.
    pub(crate) async fn wait_for_system(&self) -> &Arc<Messenger> {
        // Fast path: already initialized
        if let Some(system) = self.system.get() {
            return system;
        }
        // Register interest before re-checking to avoid missed notification
        let notified = self.system_ready.notified();
        if let Some(system) = self.system.get() {
            return system;
        }
        notified.await;
        self.system
            .get()
            .expect("system must be set after notification")
    }

    /// Get a clone of the handlers Arc for sharing with HandlerManager.
    pub(crate) fn handlers_arc(&self) -> Arc<DashMap<String, Arc<dyn ActiveMessageDispatcher>>> {
        self.handlers.clone()
    }

    /// Get a list of all registered handler names
    pub(crate) fn list_handlers(&self) -> Vec<String> {
        self.handlers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Dispatch a message to the appropriate handler
    pub fn dispatch_message(&self, handler_name: &str, ctx: HandlerContext) {
        match self.handlers.get(handler_name) {
            Some(dispatcher) => {
                dispatcher.dispatch(ctx);
            }
            None => {
                self.handle_unknown_handler(handler_name, ctx);
            }
        }
    }

    /// Handle messages for unknown handlers
    fn handle_unknown_handler(&self, handler_name: &str, ctx: HandlerContext) {
        if let Some(metrics) = ctx.system.observability().as_ref() {
            metrics.record_dispatch_failure(DispatchFailure::DispatchUnknownHandler);
        }
        error!(
            target: "crate::messenger::dispatcher",
            handler = %handler_name,
            message_id = %ctx.message_id,
            "No handler registered for message"
        );

        let backend = self.backend.clone();
        let message_id = ctx.message_id;
        let handler_name = handler_name.to_string();

        match ctx.response_type {
            ResponseType::AckNack | ResponseType::Unary => {
                let error_message = format!("Handler '{}' not found", handler_name);
                tokio::spawn(async move {
                    if let Err(e) =
                        Self::send_error_response_static(&backend, message_id, error_message).await
                    {
                        error!(
                            target: "crate::messenger::dispatcher",
                            "Failed to send error response for unknown handler: {}", e
                        );
                    }
                });
            }
            ResponseType::FireAndForget => {
                warn!(
                    target: "crate::messenger::dispatcher",
                    handler = %handler_name,
                    "Fire-and-forget message to unknown handler, no response sent"
                );
            }
        }
    }

    /// Send an error response back to the sender.
    pub(crate) async fn send_error_response(
        &self,
        response_id: ResponseId,
        error_message: String,
    ) -> anyhow::Result<()> {
        Self::send_error_response_static(&self.backend, response_id, error_message).await
    }

    /// Send an error response back to the sender (static method)
    async fn send_error_response_static(
        backend: &VeloBackend,
        response_id: ResponseId,
        error_message: String,
    ) -> anyhow::Result<()> {
        use crate::transports::MessageType;

        let header = encode_event_header(EventType::Ack(response_id, Outcome::Error));
        let payload = Bytes::from(error_message.into_bytes());

        struct DispatcherErrorHandler;
        impl crate::transports::TransportErrorHandler for DispatcherErrorHandler {
            fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
                error!(target: "crate::messenger::dispatcher", "Failed to send error response: {}", error);
            }
        }

        static ERROR_HANDLER: std::sync::OnceLock<
            Arc<dyn crate::transports::TransportErrorHandler>,
        > = std::sync::OnceLock::new();
        let error_handler = ERROR_HANDLER
            .get_or_init(|| Arc::new(DispatcherErrorHandler))
            .clone();

        let outcome = backend.send_message_to_worker(
            WorkerId::from_u64(response_id.worker_id()),
            header,
            payload,
            MessageType::Ack,
            error_handler,
        )?;
        if let crate::transports::SendOutcome::Backpressured(bp) = outcome {
            bp.await;
        }

        Ok(())
    }
}
