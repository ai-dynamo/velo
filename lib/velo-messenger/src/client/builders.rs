// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Convenience builders for active message clients.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::ActiveMessageClient;
use crate::common::{ActiveMessage, MessageMetadata};
use velo_common::{InstanceId, WorkerId};
use velo_observability::ClientResolution;
use velo_transports::{SendBackpressure, SendOutcome};

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

/// Shared state for all builder result futures.
///
/// Drives any `SendBackpressure` to completion before polling the
/// `ResponseAwaiter`. `immediate_error` short-circuits both. This is the one
/// place the bp-then-response sequence lives; each public result type
/// (`SyncResult`/`UnaryResult`/`TypedUnaryResult`) is a thin wrapper that
/// maps the raw response bytes to its declared `Output`.
struct ResponseStage {
    bp: Option<SendBackpressure>,
    awaiter: Option<crate::common::responses::ResponseAwaiter>,
    immediate_error: Option<anyhow::Error>,
}

impl ResponseStage {
    fn ready(awaiter: crate::common::responses::ResponseAwaiter) -> Self {
        Self {
            bp: None,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    fn with_bp(
        awaiter: crate::common::responses::ResponseAwaiter,
        bp: Option<SendBackpressure>,
    ) -> Self {
        Self {
            bp,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    fn error(err: anyhow::Error) -> Self {
        Self {
            bp: None,
            awaiter: None,
            immediate_error: Some(err),
        }
    }

    /// Drive bp (if any) then the awaiter. Returns the raw response bytes;
    /// wrappers map this into their own `Output` type.
    fn poll_raw(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        if let Some(err) = self.immediate_error.take() {
            return Poll::Ready(Err(err));
        }

        if let Some(bp) = self.bp.as_mut() {
            match Pin::new(bp).poll(cx) {
                Poll::Ready(()) => self.bp = None,
                Poll::Pending => return Poll::Pending,
            }
        }

        let awaiter = self
            .awaiter
            .as_mut()
            .expect("ResponseStage polled after completion");

        match awaiter.poll_recv(cx) {
            Poll::Ready(result) => {
                self.awaiter = None;
                Poll::Ready(result.map_err(|e| anyhow!(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Result wrapper for sync operations (acknowledgment only).
///
/// Send-side backpressure is transparent — callers just `.await` the result.
pub struct SyncResult {
    stage: ResponseStage,
}

impl SyncResult {
    fn error(err: anyhow::Error) -> Self {
        Self {
            stage: ResponseStage::error(err),
        }
    }
}

impl Future for SyncResult {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_raw(cx).map(|r| r.map(|_| ()))
    }
}

/// Result wrapper for unary operations returning raw bytes.
pub struct UnaryResult {
    stage: ResponseStage,
}

impl UnaryResult {
    fn error(err: anyhow::Error) -> Self {
        Self {
            stage: ResponseStage::error(err),
        }
    }
}

impl Future for UnaryResult {
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage
            .poll_raw(cx)
            .map(|r| r.map(|b| b.unwrap_or_default()))
    }
}

/// Result wrapper for typed unary operations with deserialization.
pub struct TypedUnaryResult<R> {
    stage: ResponseStage,
    _marker: std::marker::PhantomData<R>,
}

impl<R> TypedUnaryResult<R> {
    fn error(err: anyhow::Error) -> Self {
        Self {
            stage: ResponseStage::error(err),
            _marker: std::marker::PhantomData,
        }
    }
}

// Safe: `TypedUnaryResult` only holds `ResponseStage` (Unpin) and
// `PhantomData<R>`. The `R` type parameter never appears in a field that
// stores an `R`, so `Unpin` is correct regardless of `R`'s own `Unpin`.
impl<R> Unpin for TypedUnaryResult<R> {}

impl<R: DeserializeOwned> Future for TypedUnaryResult<R> {
    type Output = Result<R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_raw(cx).map(|r| match r {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("Failed to deserialize response: {}", e)),
            Ok(None) => Err(anyhow!("Expected response data, got empty")),
            Err(e) => Err(e),
        })
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
}

/// Drive a `send_message` result from inside a spawned (slow-path) task that
/// owns the response outcome.
///
/// - `Enqueued` → no-op; let the awaiter wait for the response.
/// - `Backpressured(bp)` → `.await` the bp future so the frame is actually
///   enqueued before this task returns.
/// - `Err` → log, emit `ClientResolution::SendError`, and complete the
///   response outcome with the error so callers don't wait forever.
async fn drive_send_outcome(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: crate::common::responses::ResponseId,
    path_description: &'static str,
) {
    match send_result {
        Ok(SendOutcome::Enqueued) => {}
        Ok(SendOutcome::Backpressured(bp)) => bp.await,
        Err(e) => {
            tracing::error!(
                target: "velo_messenger::client",
                error = %e,
                path = path_description,
                "Failed to send message"
            );
            if let Some(metrics) = client.observability.as_ref() {
                metrics.record_client_resolution(ClientResolution::SendError);
            }
            let _ = client
                .response_manager
                .complete_outcome(response_id, Err(format!("Send failed: {}", e)));
        }
    }
}

/// Fire-path driver. Returns `Ok(())` once the frame has been handed to the
/// transport (either fast-pathed or bp-enqueued). Returns `Err` for pre-wire
/// failures:
///
/// - Synchronous `send_message` error (peer unregistered, transport-level
///   refusal).
/// - `on_error` fires during `bp.await` (channel closed between hand-off and
///   drain — frame never made it to the wire). The `DefaultErrorHandler`
///   completes the awaiter with `Err`; after `bp.await` resolves we poll
///   the awaiter once non-blockingly and surface any completion we find.
///
/// After the frame is accepted by the wire the awaiter is simply dropped —
/// fire-and-forget semantics mean we don't observe remote processing.
async fn drive_fire_send(
    send_result: Result<SendOutcome>,
    mut awaiter: crate::common::responses::ResponseAwaiter,
) -> Result<()> {
    use futures::FutureExt;
    match send_result {
        Ok(SendOutcome::Enqueued) => Ok(()),
        Ok(SendOutcome::Backpressured(bp)) => {
            bp.await;
            // Transport invokes `on_error` synchronously inside the bp future
            // before it resolves, so DefaultErrorHandler has already written
            // Err into the awaiter's slot (if applicable) by the time we get
            // here. `now_or_never` polls once: `Some(Err)` means we failed
            // pre-wire; `None` or `Some(Ok(_))` means the frame was enqueued.
            match awaiter.recv().now_or_never() {
                Some(Err(e)) => Err(anyhow!("Send failed: {}", e)),
                _ => Ok(()),
            }
        }
        Err(e) => Err(e),
    }
}

/// Translate a synchronous fast-path `send_message` result into a
/// [`ResponseStage`]. On `Err`, logs and completes the outcome with the
/// error so the returned stage resolves via its awaiter.
fn stage_from_send(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: crate::common::responses::ResponseId,
    awaiter: crate::common::responses::ResponseAwaiter,
) -> ResponseStage {
    match send_result {
        Ok(SendOutcome::Enqueued) => ResponseStage::ready(awaiter),
        Ok(SendOutcome::Backpressured(bp)) => ResponseStage::with_bp(awaiter, Some(bp)),
        Err(e) => {
            tracing::error!(
                target: "velo_messenger::client",
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

/// Shared async helper for fire-and-forget: ensure peer ready, then send.
/// Errors are logged (fire-and-forget semantics — no outcome to complete).
async fn fire_send_after_ready(
    client: Arc<ActiveMessageClient>,
    target: InstanceId,
    handler: String,
    payload: Option<Bytes>,
    headers: Option<HashMap<String, String>>,
) {
    match client.ensure_peer_ready(target, &handler).await {
        Ok(_) => match client.register_outcome() {
            Ok(outcome) => {
                let response_id = outcome.response_id();
                let message = ActiveMessage {
                    metadata: MessageMetadata::new_fire(response_id, handler, headers),
                    payload: payload.unwrap_or_default(),
                };
                // Slow path runs in a spawned task — no caller to return Err
                // to, so log and drop.
                if let Err(e) = drive_fire_send(client.send_message(target, message), outcome).await
                {
                    tracing::error!(
                        target: "velo_messenger::client",
                        error = %e,
                        "Fire-and-forget send failed (slow path)"
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "velo_messenger::client",
                    error = %e,
                    "Failed to register outcome for fire-and-forget"
                );
            }
        },
        Err(e) => {
            tracing::error!(
                target: "velo_messenger::client",
                error = %e,
                handler = %handler,
                target = %target,
                "Failed to prepare peer for message"
            );
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
        response_id: crate::common::responses::ResponseId,
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
        response_id: crate::common::responses::ResponseId,
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
                                target: "velo_messenger::client",
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
                    target: "velo_messenger::client",
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

        match target_result {
            Ok(target) if self.client.can_send_directly(target, &self.handler) => {
                if let Some(metrics) = self.client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::DirectSuccess);
                }
                // Fast path: send immediately. Pre-wire errors (sync send
                // failure, or channel close during bp.await) are surfaced to
                // the caller via the drive_fire_send Result.
                let outcome = self.client.register_outcome()?;
                let response_id = outcome.response_id();
                let message = ActiveMessage {
                    metadata: MessageMetadata::new_fire(response_id, self.handler, self.headers),
                    payload: self.payload.unwrap_or_default(),
                };
                drive_fire_send(self.client.send_message(target, message), outcome).await
            }
            Ok(target) => {
                // Slow path: spawn handshake + send
                let client = self.client.clone();
                let handler = self.handler.clone();
                let payload = self.payload.clone();
                let headers = self.headers.clone();

                tokio::spawn(fire_send_after_ready(
                    client, target, handler, payload, headers,
                ));
                Ok(())
            }
            Err(ResolveError::UnresolvedPeer) => {
                let Some(worker_id) = worker_id else {
                    return Err(anyhow!("UnresolvedPeer but no worker_id set"));
                };

                // Discovery path: spawn discovery → handshake + send
                let client = self.client.clone();
                let handler = self.handler.clone();
                let payload = self.payload.clone();
                let headers = self.headers.clone();

                tokio::spawn(async move {
                    match client.resolve_peer_via_discovery(worker_id).await {
                        Ok(target) => {
                            fire_send_after_ready(client, target, handler, payload, headers).await;
                        }
                        Err(e) => {
                            if let Some(metrics) = client.observability.as_ref() {
                                metrics.record_client_resolution(ClientResolution::DiscoveryError);
                            }
                            tracing::error!(
                                target: "velo_messenger::client",
                                error = %e,
                                worker_id = %worker_id,
                                "Discovery failed for fire-and-forget"
                            );
                        }
                    }
                });
                Ok(())
            }
            Err(ResolveError::Other(e)) => Err(e),
        }
    }

    /// Shared dispatch for sync/unary/typed: resolves the target, registers
    /// the response outcome, and either sends on the fast path (returning a
    /// `ResponseStage`) or spawns the slow path (returning a ready stage
    /// whose awaiter will be completed by the spawned task).
    fn dispatch(self, message_type: MsgType) -> Result<ResponseStage, anyhow::Error> {
        let target_result = self.resolve_target();
        let worker_id = self.target_worker;

        let awaiter = self
            .client
            .register_outcome()
            .map_err(|e| anyhow!("Failed to register outcome: {}", e))?;
        let response_id = awaiter.response_id();

        Ok(match target_result {
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
                let Some(worker_id) = worker_id else {
                    tracing::error!(target: "velo_messenger::client", "UnresolvedPeer but no worker_id set");
                    return Ok(ResponseStage::ready(awaiter));
                };
                self.spawn_slow_path(
                    SlowPathKind::Discovery(worker_id),
                    response_id,
                    message_type,
                );
                ResponseStage::ready(awaiter)
            }
            Err(ResolveError::Other(e)) => {
                tracing::error!(target: "velo_messenger::client", error = %e, "Target resolution failed");
                let _ = self
                    .client
                    .response_manager
                    .complete_outcome(response_id, Err(format!("Resolution failed: {}", e)));
                ResponseStage::ready(awaiter)
            }
        })
    }

    pub fn sync(self) -> SyncResult {
        match self.dispatch(MsgType::Sync) {
            Ok(stage) => SyncResult { stage },
            Err(e) => SyncResult::error(e),
        }
    }

    pub fn unary(self) -> UnaryResult {
        match self.dispatch(MsgType::Unary) {
            Ok(stage) => UnaryResult { stage },
            Err(e) => UnaryResult::error(e),
        }
    }

    pub fn typed<R>(self) -> TypedUnaryResult<R>
    where
        R: DeserializeOwned + Send + 'static,
    {
        match self.dispatch(MsgType::Unary) {
            Ok(stage) => TypedUnaryResult {
                stage,
                _marker: std::marker::PhantomData,
            },
            Err(e) => TypedUnaryResult::error(e),
        }
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

#[cfg(test)]
mod tests {
    //! Unit tests for the shared builder machinery: `ResponseStage`,
    //! `stage_from_send`, `drive_send_outcome`, `drive_fire_send`. Each
    //! branch is exercised here so coverage doesn't depend on end-to-end
    //! transport integration.
    use super::*;
    use crate::common::responses::ResponseManager;
    use velo_transports::SendBackpressure;

    fn make_awaiter() -> (
        crate::common::responses::ResponseAwaiter,
        crate::common::responses::ResponseId,
        Arc<ResponseManager>,
    ) {
        // `ResponseManager::new` accepts a u64 worker-id alias in this crate.
        let rm = Arc::new(ResponseManager::new(1));
        let awaiter = rm.register_outcome().expect("register");
        let id = awaiter.response_id();
        (awaiter, id, rm)
    }

    fn ready_bp() -> SendBackpressure {
        SendBackpressure::new(Box::pin(async {}))
    }

    fn pending_bp() -> SendBackpressure {
        SendBackpressure::new(Box::pin(futures::future::pending::<()>()))
    }

    // ── ResponseStage ────────────────────────────────────────────────────

    #[tokio::test]
    async fn stage_ready_resolves_after_outcome_completes() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::ready(awaiter);
        let mut result = SyncResult { stage };

        // Completing the outcome lets the awaiter produce its value.
        assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"ok")))));
        let r = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("sync result completes");
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn stage_with_ready_bp_proceeds_to_awaiter() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::with_bp(awaiter, Some(ready_bp()));
        let mut result = UnaryResult { stage };

        assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"hello")))));
        let r = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("unary result completes");
        assert_eq!(r.unwrap(), Bytes::from_static(b"hello"));
    }

    #[tokio::test]
    async fn stage_pending_bp_blocks_until_resolved() {
        let (awaiter, _id, _rm) = make_awaiter();
        let stage = ResponseStage::with_bp(awaiter, Some(pending_bp()));
        let result = SyncResult { stage };
        // Pending bp means the future itself stays pending even though the
        // response manager isn't exercised. Verify the timeout fires.
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(100), result).await;
        assert!(outcome.is_err(), "pending bp should keep result pending");
    }

    #[tokio::test]
    async fn stage_immediate_error_short_circuits() {
        let stage = ResponseStage::error(anyhow!("boom"));
        let result = SyncResult { stage };
        let err = result.await.expect_err("immediate_error returns Err");
        assert!(err.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn unary_result_empty_response_becomes_empty_bytes() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::ready(awaiter);
        let mut result = UnaryResult { stage };

        assert!(rm.complete_outcome(id, Ok(None)));
        let r = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("unary resolves")
            .unwrap();
        assert_eq!(r, Bytes::new());
    }

    #[tokio::test]
    async fn typed_result_deserializes_payload() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::ready(awaiter);
        let mut result: TypedUnaryResult<i64> = TypedUnaryResult {
            stage,
            _marker: std::marker::PhantomData,
        };

        assert!(rm.complete_outcome(id, Ok(Some(Bytes::from(b"42".to_vec())))));
        let v = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("typed resolves")
            .unwrap();
        assert_eq!(v, 42);
    }

    #[tokio::test]
    async fn typed_result_empty_response_is_error() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::ready(awaiter);
        let mut result: TypedUnaryResult<i64> = TypedUnaryResult {
            stage,
            _marker: std::marker::PhantomData,
        };

        assert!(rm.complete_outcome(id, Ok(None)));
        let err = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("typed resolves")
            .expect_err("empty response → Err");
        assert!(err.to_string().contains("Expected response data"));
    }

    #[tokio::test]
    async fn typed_result_bad_json_is_error() {
        let (awaiter, id, rm) = make_awaiter();
        let stage = ResponseStage::ready(awaiter);
        let mut result: TypedUnaryResult<i64> = TypedUnaryResult {
            stage,
            _marker: std::marker::PhantomData,
        };

        assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"not-json")))));
        let err = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
            .await
            .expect("typed resolves")
            .expect_err("bad json → Err");
        assert!(err.to_string().contains("Failed to deserialize"));
    }

    // ── drive_fire_send ──────────────────────────────────────────────────

    #[tokio::test]
    async fn drive_fire_send_enqueued_is_ok() {
        let (awaiter, _id, _rm) = make_awaiter();
        assert!(
            drive_fire_send(Ok(SendOutcome::Enqueued), awaiter)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn drive_fire_send_bp_without_error_is_ok() {
        let (awaiter, _id, _rm) = make_awaiter();
        assert!(
            drive_fire_send(Ok(SendOutcome::Backpressured(ready_bp())), awaiter)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn drive_fire_send_bp_with_on_error_surfaces_err() {
        // Simulate the handler completing the awaiter with Err during bp.await
        // (i.e. on_error fired because the channel closed mid-drain). After
        // bp resolves, drive_fire_send should return Err.
        let (awaiter, id, rm) = make_awaiter();
        assert!(rm.complete_outcome(id, Err("peer disconnected".to_string())));
        let err = drive_fire_send(Ok(SendOutcome::Backpressured(ready_bp())), awaiter)
            .await
            .expect_err("should surface pre-wire failure");
        assert!(err.to_string().contains("peer disconnected"));
    }

    #[tokio::test]
    async fn drive_fire_send_sync_err_is_propagated() {
        let (awaiter, _id, _rm) = make_awaiter();
        let err = drive_fire_send(Err(anyhow!("peer not registered")), awaiter)
            .await
            .expect_err("sync err propagates");
        assert!(err.to_string().contains("peer not registered"));
    }

    // ── validate_handler_name ────────────────────────────────────────────

    #[test]
    fn validate_handler_name_accepts_public() {
        assert!(validate_handler_name("my_handler").is_ok());
    }

    #[test]
    fn validate_handler_name_rejects_system() {
        let err = validate_handler_name("_hello").unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot directly call system handler")
        );
    }
}
