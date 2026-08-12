// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Convenience builders for active message clients.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::ActiveMessageClient;
use crate::messenger::common::{ActiveMessage, MessageMetadata};
use crate::observability::ClientResolution;
use crate::transports::SendOutcome;
use velo_ext::{InstanceId, WorkerId};

mod results;
#[cfg(test)]
mod tests;

use results::{ResponseStage, StageState, drive_fire_send, drive_send_outcome};
pub use results::{SyncResult, TypedUnaryResult, UnaryResult};

/// Fire-and-forget builder.
pub struct AmSendBuilder {
    inner: MessageBuilder,
}

impl AmSendBuilder {
    pub(crate) fn new(client: Arc<ActiveMessageClient>, handler: &str) -> Result<Self> {
        Ok(Self {
            inner: MessageBuilder::new(client, handler)?,
        })
    }

    /// Create an `AmSendBuilder` without validating the handler name.
    ///
    /// Used by [`Messenger::am_send_streaming`] to bypass the underscore-prefix
    /// restriction so that `velo-streaming` can send frames to internal handlers
    /// like `_stream_data`.
    pub(crate) fn new_unchecked(client: Arc<ActiveMessageClient>, handler: &str) -> Self {
        Self {
            inner: MessageBuilder::new_unchecked(client, handler),
        }
    }

    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        self.inner = self.inner.payload(data)?;
        Ok(self)
    }

    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.inner = self.inner.raw_payload(data);
        self
    }

    pub fn instance(mut self, instance_id: InstanceId) -> Self {
        self.inner = self.inner.instance(instance_id);
        self
    }

    pub fn worker(mut self, worker_id: WorkerId) -> Self {
        self.inner = self.inner.worker(worker_id);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.inner = self.inner.headers(headers);
        self
    }

    /// Await a free response slot if the arena is at capacity (default:
    /// fail fast with `ResponseRegistrationError::Exhausted`). See
    /// [`MessageBuilder::await_capacity`] for rationale.
    pub fn await_capacity(mut self) -> Self {
        self.inner = self.inner.await_capacity();
        self
    }

    pub fn send(self) -> impl Future<Output = Result<()>> {
        self.inner.fire()
    }

    pub fn send_to(self, target: InstanceId) -> impl Future<Output = Result<()>> {
        self.inner.instance(target).fire()
    }
}

/// Builder for request/response flows that expect an acknowledgement only.
pub struct AmSyncBuilder {
    inner: MessageBuilder,
}

impl AmSyncBuilder {
    pub(crate) fn new(client: Arc<ActiveMessageClient>, handler: &str) -> Result<Self> {
        Ok(Self {
            inner: MessageBuilder::new(client, handler)?,
        })
    }

    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        self.inner = self.inner.payload(data)?;
        Ok(self)
    }

    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.inner = self.inner.raw_payload(data);
        self
    }

    pub fn instance(mut self, instance_id: InstanceId) -> Self {
        self.inner = self.inner.instance(instance_id);
        self
    }

    pub fn worker(mut self, worker_id: WorkerId) -> Self {
        self.inner = self.inner.worker(worker_id);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.inner = self.inner.headers(headers);
        self
    }

    /// Await a free response slot if the arena is at capacity (default:
    /// fail fast with `ResponseRegistrationError::Exhausted`).
    pub fn await_capacity(mut self) -> Self {
        self.inner = self.inner.await_capacity();
        self
    }

    pub fn send(self) -> SyncResult {
        self.inner.sync()
    }

    pub fn send_to(self, target: InstanceId) -> SyncResult {
        self.inner.instance(target).sync()
    }
}

/// Builder for unary handlers returning raw bytes.
pub struct UnaryBuilder {
    inner: MessageBuilder,
}

impl UnaryBuilder {
    pub(crate) fn new(client: Arc<ActiveMessageClient>, handler: &str) -> Result<Self> {
        Ok(Self {
            inner: MessageBuilder::new(client, handler)?,
        })
    }

    pub(crate) fn new_unchecked(client: Arc<ActiveMessageClient>, handler: &str) -> Self {
        Self {
            inner: MessageBuilder::new_unchecked(client, handler),
        }
    }

    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        self.inner = self.inner.payload(data)?;
        Ok(self)
    }

    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.inner = self.inner.raw_payload(data);
        self
    }

    pub fn instance(mut self, instance_id: InstanceId) -> Self {
        self.inner = self.inner.instance(instance_id);
        self
    }

    pub fn worker(mut self, worker_id: WorkerId) -> Self {
        self.inner = self.inner.worker(worker_id);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.inner = self.inner.headers(headers);
        self
    }

    /// Await a free response slot if the arena is at capacity (default:
    /// fail fast with `ResponseRegistrationError::Exhausted`).
    pub fn await_capacity(mut self) -> Self {
        self.inner = self.inner.await_capacity();
        self
    }

    pub fn send(self) -> UnaryResult {
        self.inner.unary()
    }

    pub fn send_to(self, target: InstanceId) -> UnaryResult {
        self.inner.instance(target).unary()
    }
}

/// Builder for typed unary handlers.
pub struct TypedUnaryBuilder<R> {
    inner: MessageBuilder,
    _marker: std::marker::PhantomData<R>,
}

impl<R> TypedUnaryBuilder<R>
where
    R: DeserializeOwned + Send + 'static,
{
    pub(crate) fn new(client: Arc<ActiveMessageClient>, handler: &str) -> Result<Self> {
        Ok(Self {
            inner: MessageBuilder::new(client, handler)?,
            _marker: std::marker::PhantomData,
        })
    }

    /// Create a `TypedUnaryBuilder` without validating the handler name.
    ///
    /// Intended for `velo-streaming` to call `_anchor_*` typed-unary handlers
    /// whose names start with underscore (normally rejected by `new`).
    pub(crate) fn new_unchecked(client: Arc<ActiveMessageClient>, handler: &str) -> Self {
        Self {
            inner: MessageBuilder::new_unchecked(client, handler),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        self.inner = self.inner.payload(data)?;
        Ok(self)
    }

    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.inner = self.inner.raw_payload(data);
        self
    }

    pub fn instance(mut self, instance_id: InstanceId) -> Self {
        self.inner = self.inner.instance(instance_id);
        self
    }

    pub fn worker(mut self, worker_id: WorkerId) -> Self {
        self.inner = self.inner.worker(worker_id);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.inner = self.inner.headers(headers);
        self
    }

    /// Await a free response slot if the arena is at capacity (default:
    /// fail fast with `ResponseRegistrationError::Exhausted`).
    pub fn await_capacity(mut self) -> Self {
        self.inner = self.inner.await_capacity();
        self
    }

    pub fn send(self) -> TypedUnaryResult<R> {
        self.inner.typed()
    }

    pub fn send_to(self, target: InstanceId) -> TypedUnaryResult<R> {
        self.inner.instance(target).typed()
    }
}

/// Error type for target resolution in message builders.
#[derive(Debug)]
enum ResolveError {
    /// Peer not found in cache - discovery needed
    UnresolvedPeer,
    /// Other validation or configuration errors
    Other(anyhow::Error),
}

/// Message type for metadata creation
#[derive(Debug, Clone, Copy)]
enum MsgType {
    Sync,
    Unary,
}

/// Which slow-path preamble a spawned task runs before sending.
#[derive(Debug, Clone, Copy)]
enum SlowPathKind {
    /// Peer is resolved but we still need to handshake.
    Handshake(InstanceId),
    /// Worker ID needs to be translated to an instance via discovery,
    /// followed by a handshake.
    Discovery(WorkerId),
}

impl From<ResolveError> for anyhow::Error {
    fn from(err: ResolveError) -> Self {
        match err {
            ResolveError::UnresolvedPeer => anyhow!("Peer not found"),
            ResolveError::Other(e) => e,
        }
    }
}

/// Minimal message builder supporting fire-and-forget and unary-style sends.
pub struct MessageBuilder {
    client: Arc<ActiveMessageClient>,
    handler: String,
    payload: Option<Bytes>,
    target_instance: Option<InstanceId>,
    target_worker: Option<WorkerId>,
    headers: Option<HashMap<String, String>>,
    // When set, slot acquisition awaits capacity instead of failing fast
    // with `ResponseRegistrationError::Exhausted`. Mirrors the transport's
    // `SendBackpressure` semantics: callers doing fan-out get bounded
    // in-flight backpressure for free.
    await_capacity: bool,
}

/// Slow-path fire completion: wait for the spawned task to complete the
/// outcome with Ok (successful enqueue) or Err (pre-wire failure), and map
/// the result into a `Result<()>` for the caller.
async fn finish_fire_via_awaiter(
    mut awaiter: crate::messenger::common::responses::ResponseAwaiter,
) -> Result<()> {
    awaiter
        .recv()
        .await
        .map(|_| ())
        .map_err(|e| anyhow!("{}", e))
}

/// Translate a synchronous fast-path `send_message` result into a
/// [`ResponseStage`]. On `Err`, logs and completes the outcome with the
/// error so the returned stage resolves via its awaiter.
fn stage_from_send(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: crate::messenger::common::responses::ResponseId,
    awaiter: crate::messenger::common::responses::ResponseAwaiter,
) -> ResponseStage {
    match send_result {
        Ok(SendOutcome::Enqueued) => ResponseStage::ready(awaiter),
        Ok(SendOutcome::Backpressured(bp)) => ResponseStage::with_bp(awaiter, Some(bp)),
        Err(e) => {
            tracing::error!(
                target: "crate::messenger::client",
                error = %e,
                "Failed to send message in fast path"
            );
            let _ = client
                .response_manager
                .complete_outcome(response_id, Err(format!("Fast-path send failed: {}", e)));
            ResponseStage::ready(awaiter)
        }
    }
}

impl MessageBuilder {
    pub fn new(client: Arc<ActiveMessageClient>, handler: &str) -> Result<Self> {
        validate_handler_name(handler)?;
        Ok(Self::new_unchecked(client, handler))
    }

    pub fn new_unchecked(client: Arc<ActiveMessageClient>, handler: &str) -> Self {
        Self {
            client,
            handler: handler.to_string(),
            payload: None,
            target_instance: None,
            target_worker: None,
            headers: None,
            await_capacity: false,
        }
    }

    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        let bytes =
            serde_json::to_vec(&data).map_err(|e| anyhow!("failed to serialize payload: {}", e))?;
        self.payload = Some(Bytes::from(bytes));
        Ok(self)
    }

    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.payload = Some(data);
        self
    }

    pub fn instance(mut self, instance_id: InstanceId) -> Self {
        self.target_instance = Some(instance_id);
        self
    }

    pub fn worker(mut self, worker_id: WorkerId) -> Self {
        self.target_worker = Some(worker_id);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Opt into backpressure on response-slot exhaustion.
    ///
    /// Default: `register_outcome` fails immediately with
    /// [`ResponseRegistrationError::Exhausted`] when the per-worker slot
    /// arena is full. With this flag, the builder instead awaits a free slot
    /// (matching `SendOutcome::Backpressured` ergonomics for transport-level
    /// channel saturation).
    ///
    /// Use this for fan-out workloads that may legitimately exceed 64k
    /// in-flight requests on a single worker — backpressure is preferable to
    /// per-request error handling.
    pub fn await_capacity(mut self) -> Self {
        self.await_capacity = true;
        self
    }

    fn resolve_target(&self) -> Result<InstanceId, ResolveError> {
        match (self.target_instance, self.target_worker) {
            (Some(instance), None) => Ok(instance),
            (None, Some(worker)) => self
                .client
                .backend
                .try_translate_worker_id(worker)
                .map_err(|_| ResolveError::UnresolvedPeer),
            (Some(_), Some(_)) => Err(ResolveError::Other(anyhow!(
                "Cannot set both .instance() and .worker() - they are mutually exclusive"
            ))),
            (None, None) => Err(ResolveError::Other(anyhow!(
                "Target not set. Call .instance() or .worker() before sending"
            ))),
        }
    }

    fn create_metadata(
        &self,
        response_id: crate::messenger::common::responses::ResponseId,
        message_type: MsgType,
    ) -> MessageMetadata {
        match message_type {
            MsgType::Sync => {
                MessageMetadata::new_sync(response_id, self.handler.clone(), self.headers.clone())
            }
            MsgType::Unary => {
                MessageMetadata::new_unary(response_id, self.handler.clone(), self.headers.clone())
            }
        }
    }

    fn spawn_slow_path(
        &self,
        kind: SlowPathKind,
        response_id: crate::messenger::common::responses::ResponseId,
        message_type: MsgType,
    ) {
        let client = self.client.clone();
        let handler = self.handler.clone();
        let payload = self.payload.clone();
        let headers = self.headers.clone();

        tokio::spawn(async move {
            // Stage 1 — resolve target (discovery if needed).
            let target = match kind {
                SlowPathKind::Handshake(target) => target,
                SlowPathKind::Discovery(worker_id) => {
                    match client.resolve_peer_via_discovery(worker_id).await {
                        Ok(instance_id) => instance_id,
                        Err(e) => {
                            if let Some(metrics) = client.observability.as_ref() {
                                metrics.record_client_resolution(ClientResolution::DiscoveryError);
                            }
                            tracing::error!(
                                target: "crate::messenger::client",
                                error = %e,
                                worker_id = %worker_id,
                                "Discovery failed"
                            );
                            let _ = client.response_manager.complete_outcome(
                                response_id,
                                Err(format!("Discovery failed: {}", e)),
                            );
                            return;
                        }
                    }
                }
            };

            // Stage 2 — handshake.
            if let Err(e) = client.ensure_peer_ready(target, &handler).await {
                if let Some(metrics) = client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::HandshakeError);
                }
                tracing::error!(
                    target: "crate::messenger::client",
                    error = %e,
                    "Failed to prepare peer in slow path"
                );
                let _ = client
                    .response_manager
                    .complete_outcome(response_id, Err(format!("Handshake failed: {}", e)));
                return;
            }

            // Stage 3 — send, drive bp, surface errors.
            let metadata = match message_type {
                MsgType::Sync => MessageMetadata::new_sync(response_id, handler, headers),
                MsgType::Unary => MessageMetadata::new_unary(response_id, handler, headers),
            };
            let message = ActiveMessage {
                metadata,
                payload: payload.unwrap_or_default(),
            };
            drive_send_outcome(
                &client,
                client.send_message(target, message),
                response_id,
                "slow-path",
            )
            .await;
        });
    }

    pub async fn fire(self) -> Result<()> {
        let target_result = self.resolve_target();
        let worker_id = self.target_worker;

        // `Other` resolution errors don't need a slot — short-circuit before
        // any allocation.
        let target_result = match target_result {
            Err(ResolveError::Other(e)) => return Err(e),
            other => other,
        };

        // Acquire per the builder's capacity policy. With `await_capacity`,
        // wait for a free slot instead of failing fast — mirrors the
        // SendBackpressure idiom transports already use.
        let outcome = acquire_awaiter(&self.client, self.await_capacity).await?;

        match target_result {
            Ok(target) if self.client.can_send_directly(target, &self.handler) => {
                if let Some(metrics) = self.client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::DirectSuccess);
                }
                // Fast path: send inline. Pre-wire errors (sync send failure
                // or channel close during bp.await) are surfaced via the
                // drive_fire_send Result; the awaiter is internal.
                let response_id = outcome.response_id();
                let message = ActiveMessage {
                    metadata: MessageMetadata::new_fire(response_id, self.handler, self.headers),
                    payload: self.payload.unwrap_or_default(),
                };
                drive_fire_send(self.client.send_message(target, message), outcome).await
            }
            Ok(target) => {
                // Slow path: awaiter already owned, spawn
                // discovery/handshake/send in a detached task, and wait on
                // the awaiter. The task completes the awaiter with Ok(None)
                // on successful enqueue or Err on any pre-wire failure.
                // Spawning preserves cancel-safety: if the caller drops
                // mid-wait, the frame still goes through (matching
                // sync/unary slow-path semantics).
                let response_id = outcome.response_id();
                self.spawn_fire_slow_path(SlowPathKind::Handshake(target), response_id);
                finish_fire_via_awaiter(outcome).await
            }
            Err(ResolveError::UnresolvedPeer) => {
                let Some(worker_id) = worker_id else {
                    return Err(anyhow!("UnresolvedPeer but no worker_id set"));
                };
                let response_id = outcome.response_id();
                self.spawn_fire_slow_path(SlowPathKind::Discovery(worker_id), response_id);
                finish_fire_via_awaiter(outcome).await
            }
            Err(ResolveError::Other(_)) => unreachable!("Other handled above"),
        }
    }

    fn spawn_fire_slow_path(
        &self,
        kind: SlowPathKind,
        response_id: crate::messenger::common::responses::ResponseId,
    ) {
        let client = self.client.clone();
        let handler = self.handler.clone();
        let payload = self.payload.clone();
        let headers = self.headers.clone();

        tokio::spawn(async move {
            // Stage 1 — resolve target.
            let target = match kind {
                SlowPathKind::Handshake(target) => target,
                SlowPathKind::Discovery(worker_id) => {
                    match client.resolve_peer_via_discovery(worker_id).await {
                        Ok(t) => t,
                        Err(e) => {
                            if let Some(metrics) = client.observability.as_ref() {
                                metrics.record_client_resolution(ClientResolution::DiscoveryError);
                            }
                            tracing::error!(
                                target: "crate::messenger::client",
                                error = %e,
                                worker_id = %worker_id,
                                "Discovery failed for fire-and-forget"
                            );
                            let _ = client.response_manager.complete_outcome(
                                response_id,
                                Err(format!("Discovery failed: {}", e)),
                            );
                            return;
                        }
                    }
                }
            };

            // Stage 2 — handshake.
            if let Err(e) = client.ensure_peer_ready(target, &handler).await {
                if let Some(metrics) = client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::HandshakeError);
                }
                tracing::error!(
                    target: "crate::messenger::client",
                    error = %e,
                    "Handshake failed for fire-and-forget"
                );
                let _ = client
                    .response_manager
                    .complete_outcome(response_id, Err(format!("Handshake failed: {}", e)));
                return;
            }

            // Stage 3 — send + complete outcome.
            let message = ActiveMessage {
                metadata: MessageMetadata::new_fire(response_id, handler, headers),
                payload: payload.unwrap_or_default(),
            };
            match client.send_message(target, message) {
                Ok(SendOutcome::Enqueued) => {
                    let _ = client
                        .response_manager
                        .complete_outcome(response_id, Ok(None));
                }
                Ok(SendOutcome::Backpressured(bp)) => {
                    bp.await;
                    // If DefaultErrorHandler already wrote Err during bp.await,
                    // this Ok is a no-op (slot already finished).
                    let _ = client
                        .response_manager
                        .complete_outcome(response_id, Ok(None));
                }
                Err(e) => {
                    tracing::error!(
                        target: "crate::messenger::client",
                        error = %e,
                        "Fire-and-forget send failed (slow path)"
                    );
                    let _ = client
                        .response_manager
                        .complete_outcome(response_id, Err(format!("Send failed: {}", e)));
                }
            }
        });
    }

    /// Post-acquisition dispatch. Given a pre-resolved target and a
    /// pre-acquired awaiter, either sends on the fast path (returning a
    /// populated `ResponseStage`) or spawns the slow path (returning a ready
    /// stage whose awaiter will be completed by the spawned task).
    ///
    /// Target resolution happens in [`MessageBuilder::make_stage_state`]
    /// *before* slot acquisition so `ResolveError::Other` (programmer
    /// misuse — target not set, or both `.instance()` and `.worker()` set)
    /// fails the caller without consuming a slot or blocking on
    /// `register_outcome_async`. This mirrors the ordering in [`fire`].
    fn dispatch_with_awaiter(
        self,
        target_result: Result<InstanceId, ResolveError>,
        awaiter: crate::messenger::common::responses::ResponseAwaiter,
        message_type: MsgType,
    ) -> ResponseStage {
        let worker_id = self.target_worker;
        let response_id = awaiter.response_id();

        match target_result {
            Ok(target) if self.client.can_send_directly(target, &self.handler) => {
                if let Some(metrics) = self.client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::DirectSuccess);
                }
                let message = ActiveMessage {
                    metadata: self.create_metadata(response_id, message_type),
                    payload: self.payload.unwrap_or_default(),
                };
                let send_result = self.client.send_message(target, message);
                stage_from_send(&self.client, send_result, response_id, awaiter)
            }
            Ok(target) => {
                self.spawn_slow_path(SlowPathKind::Handshake(target), response_id, message_type);
                ResponseStage::ready(awaiter)
            }
            Err(ResolveError::UnresolvedPeer) => {
                // `resolve_target` only returns UnresolvedPeer when
                // target_worker is Some, so this else-branch is a defensive
                // guard — unreachable in practice.
                let Some(worker_id) = worker_id else {
                    tracing::error!(target: "crate::messenger::client", "UnresolvedPeer but no worker_id set");
                    return ResponseStage::ready(awaiter);
                };
                self.spawn_slow_path(
                    SlowPathKind::Discovery(worker_id),
                    response_id,
                    message_type,
                );
                ResponseStage::ready(awaiter)
            }
            Err(ResolveError::Other(_)) => {
                unreachable!("ResolveError::Other is short-circuited in make_stage_state")
            }
        }
    }

    /// Build a `StageState` according to the builder's acquisition policy.
    ///
    /// Target resolution runs first so `ResolveError::Other` (programmer
    /// misuse) produces an immediate error stage and never consumes a slot.
    /// Under `await_capacity` this is load-bearing: resolving inside the
    /// deferred future would make the caller block on
    /// `register_outcome_async()` under arena saturation before learning
    /// the call can never succeed. Mirrors [`fire`]'s early return.
    ///
    /// - Default (fail-fast): acquire synchronously; propagate
    ///   `ResponseRegistrationError::Exhausted` as an immediate error stage.
    /// - `await_capacity`: defer acquisition into a boxed future that awaits
    ///   capacity before dispatching. Mirrors the `SendOutcome::Backpressured`
    ///   idiom transports already use.
    fn make_stage_state(self, message_type: MsgType) -> StageState {
        // Short-circuit programmer-misuse resolution errors before touching
        // the slot arena. Mirrors fire()'s early return so invalid target
        // configs never block on register_outcome_async.
        let target_result = match self.resolve_target() {
            Err(ResolveError::Other(e)) => return StageState::error(e),
            other => other,
        };

        if self.await_capacity {
            let fut = Box::pin(async move {
                let awaiter = self.client.response_manager.register_outcome_async().await;
                self.dispatch_with_awaiter(target_result, awaiter, message_type)
            });
            StageState::Pending(fut)
        } else {
            match self.client.register_outcome() {
                Ok(awaiter) => StageState::ready(self.dispatch_with_awaiter(
                    target_result,
                    awaiter,
                    message_type,
                )),
                Err(e) => StageState::error(anyhow!("Failed to register outcome: {}", e)),
            }
        }
    }

    pub fn sync(self) -> SyncResult {
        SyncResult {
            stage: self.make_stage_state(MsgType::Sync),
        }
    }

    pub fn unary(self) -> UnaryResult {
        UnaryResult {
            stage: self.make_stage_state(MsgType::Unary),
        }
    }

    pub fn typed<R>(self) -> TypedUnaryResult<R>
    where
        R: DeserializeOwned + Send + 'static,
    {
        TypedUnaryResult {
            stage: self.make_stage_state(MsgType::Unary),
            _marker: std::marker::PhantomData,
        }
    }
}

/// Acquire a response awaiter, honoring `await_capacity`:
///
/// - `false` — fail fast; stringify `ResponseRegistrationError::Exhausted`
///   into an `anyhow::Error` (existing behaviour).
/// - `true` — await a free slot before returning.
async fn acquire_awaiter(
    client: &ActiveMessageClient,
    await_capacity: bool,
) -> Result<crate::messenger::common::responses::ResponseAwaiter> {
    if await_capacity {
        Ok(client.response_manager.register_outcome_async().await)
    } else {
        client
            .register_outcome()
            .map_err(|e| anyhow!("Failed to register outcome: {}", e))
    }
}

pub(crate) fn validate_handler_name(handler: &str) -> Result<()> {
    if handler.starts_with('_') {
        anyhow::bail!(
            "Cannot directly call system handler '{}'. Use client convenience methods instead: health_check(), ensure_bidirectional_connection(), list_handlers(), await_handler()",
            handler
        );
    }
    Ok(())
}
