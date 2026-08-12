// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Convenience builders for active message clients.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use super::ActiveMessageClient;
use crate::messenger::common::responses::{ResponseAwaiter, ResponseId, SlotBackpressure};
use crate::messenger::common::{ActiveMessage, MessageMetadata};
use crate::observability::ClientResolution;
use crate::transports::SendOutcome;
use velo_ext::{InstanceId, WorkerId};

mod results;
#[cfg(test)]
mod tests;

use results::{AdmissionReport, Dispatched, SendStage, drive_send_outcome};
pub use results::{Admitted, FireResult, SyncResult, TypedUnaryResult, UnaryResult};

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
    ///
    /// This is the one flag that makes a fire send depend on its result being
    /// polled, and only while the arena really is full — see [`FireResult`].
    pub fn await_capacity(mut self) -> Self {
        self.inner = self.inner.await_capacity();
        self
    }

    /// Issue the send.
    ///
    /// The frame is handed to the transport before this returns; awaiting the
    /// [`FireResult`] waits for it to be *admitted* to the transport's send
    /// channel. Dropping the result detaches from the send, it does not cancel
    /// it — see [`FireResult`].
    pub fn send(self) -> FireResult {
        self.inner.fire()
    }

    /// Issue the send to `target`. See [`send`](Self::send).
    pub fn send_to(self, target: InstanceId) -> FireResult {
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
    /// `am_send`: the remote never answers, so the send is done at admission.
    Fire,
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
    // with `ResponseRegistrationError::Exhausted`. The analogue one layer
    // down is `SendOutcome::Pending`: callers doing fan-out get bounded
    // in-flight backpressure for free.
    await_capacity: bool,
}

/// Translate a synchronous fast-path `send_message` result into a
/// [`Dispatched`] send.
///
/// An `Err` here was diagnosed on the caller's own task, so it becomes the
/// admission's terminal state directly — unlike the slow path there is no
/// second task that could be parked on the response slot needing to be told.
/// Dropping the awaiter returns the slot to the arena on the spot.
fn stage_from_send(send_result: Result<SendOutcome>, awaiter: ResponseAwaiter) -> Dispatched {
    match send_result {
        Ok(outcome) => Dispatched::issued(outcome, awaiter),
        Err(e) => {
            tracing::error!(
                target: "crate::messenger::client",
                error = %e,
                "Failed to send message in fast path"
            );
            Dispatched::failed(format!("Fast-path send failed: {}", e))
        }
    }
}

/// Build the metadata for one send.
fn build_metadata(
    response_id: ResponseId,
    handler: String,
    headers: Option<HashMap<String, String>>,
    message_type: MsgType,
) -> MessageMetadata {
    match message_type {
        MsgType::Fire => MessageMetadata::new_fire(response_id, handler, headers),
        MsgType::Sync => MessageMetadata::new_sync(response_id, handler, headers),
        MsgType::Unary => MessageMetadata::new_unary(response_id, handler, headers),
    }
}

/// Outcome of the builder's response-slot acquisition.
enum Acquisition {
    /// A slot was free, so the send can be issued right now.
    Allocated(ResponseAwaiter),
    /// The arena is full and the caller opted into waiting. Awaiting this
    /// yields a slot; the send is issued behind it.
    Deferred(SlotBackpressure),
    /// The arena is full and the caller wants to hear about it now.
    Exhausted(anyhow::Error),
}

/// A send whose target still needs resolving, running on its own task.
///
/// Everything the send needs after the builder is gone, so that discovery,
/// handshake, and the send itself survive the caller dropping its result.
struct SlowPath {
    client: Arc<ActiveMessageClient>,
    kind: SlowPathKind,
    handler: String,
    payload: Option<Bytes>,
    headers: Option<HashMap<String, String>>,
    response_id: ResponseId,
    message_type: MsgType,
}

impl SlowPath {
    /// Resolve, handshake, send — reporting where it stopped.
    async fn run(self) -> AdmissionReport {
        let target = self.resolve().await?;
        self.handshake(target).await?;
        self.send(target).await
    }

    /// Stage 1 — translate a worker id via discovery if that is all we have.
    async fn resolve(&self) -> std::result::Result<InstanceId, String> {
        let worker_id = match self.kind {
            SlowPathKind::Handshake(target) => return Ok(target),
            SlowPathKind::Discovery(worker_id) => worker_id,
        };
        match self.client.resolve_peer_via_discovery(worker_id).await {
            Ok(instance_id) => Ok(instance_id),
            Err(e) => {
                self.record(ClientResolution::DiscoveryError);
                tracing::error!(
                    target: "crate::messenger::client",
                    error = %e,
                    worker_id = %worker_id,
                    "Discovery failed"
                );
                Err(self.fail(format!("Discovery failed: {}", e)))
            }
        }
    }

    /// Stage 2 — exchange handler lists if we have not already.
    async fn handshake(&self, target: InstanceId) -> AdmissionReport {
        if let Err(e) = self.client.ensure_peer_ready(target, &self.handler).await {
            self.record(ClientResolution::HandshakeError);
            tracing::error!(
                target: "crate::messenger::client",
                error = %e,
                "Failed to prepare peer in slow path"
            );
            return Err(self.fail(format!("Handshake failed: {}", e)));
        }
        Ok(())
    }

    /// Stage 3 — send, and wait for the frame to reach the send channel.
    async fn send(self, target: InstanceId) -> AdmissionReport {
        let metadata = build_metadata(
            self.response_id,
            self.handler,
            self.headers,
            self.message_type,
        );
        let message = ActiveMessage {
            metadata,
            payload: self.payload.unwrap_or_default(),
        };
        drive_send_outcome(
            &self.client,
            self.client.send_message(target, message),
            self.response_id,
            "slow-path",
        )
        .await
    }

    /// Report a pre-send failure to a caller already parked on the response
    /// slot, and hand the same message back for the admission channel.
    ///
    /// Both, because the two are read by different waiters: a sync/unary result
    /// that is already awaiting its response would otherwise sit there until
    /// its own timeout.
    fn fail(&self, reason: String) -> String {
        let _ = self
            .client
            .response_manager
            .complete_outcome(self.response_id, Err(reason.clone()));
        reason
    }

    fn record(&self, resolution: ClientResolution) {
        if let Some(metrics) = self.client.observability.as_ref() {
            metrics.record_client_resolution(resolution);
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
    /// arena is full. With this flag, the builder instead awaits a free slot.
    ///
    /// This is about *response slots*, not about the transport's send channel:
    /// a saturated send channel is handled a layer down by the target's
    /// admission gate, which queues the frame and reports
    /// `SendOutcome::Pending` whether or not this flag is set. The two are
    /// independent backpressure sources that happen to feel the same to a
    /// caller who just awaits the result.
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

    fn create_metadata(&self, response_id: ResponseId, message_type: MsgType) -> MessageMetadata {
        build_metadata(
            response_id,
            self.handler.clone(),
            self.headers.clone(),
            message_type,
        )
    }

    /// Acquire a response slot per the builder's capacity policy.
    ///
    /// Every send registers one, fire-and-forget included: the slot supplies the
    /// `response_id` the frame carries, which is what lets the transport's error
    /// handler correlate a failed write back to this send.
    fn acquire(&self) -> Acquisition {
        if self.await_capacity {
            match self.client.response_manager.try_register_outcome() {
                crate::messenger::common::responses::RegisterOutcome::Allocated(awaiter) => {
                    Acquisition::Allocated(awaiter)
                }
                crate::messenger::common::responses::RegisterOutcome::Backpressured(
                    backpressure,
                ) => Acquisition::Deferred(backpressure),
            }
        } else {
            match self.client.register_outcome() {
                Ok(awaiter) => Acquisition::Allocated(awaiter),
                Err(e) => Acquisition::Exhausted(anyhow!("Failed to register outcome: {}", e)),
            }
        }
    }

    /// Hand the send to a detached task, and return the channel it reports on.
    ///
    /// Detaching is what keeps delivery independent of the caller: a fire result
    /// dropped on the spot, or a unary caller that goes away mid-handshake, must
    /// not withdraw a frame that is already on its way.
    fn spawn_slow_path(
        &self,
        kind: SlowPathKind,
        response_id: ResponseId,
        message_type: MsgType,
    ) -> oneshot::Receiver<AdmissionReport> {
        let (report_tx, report_rx) = oneshot::channel();
        let slow_path = SlowPath {
            client: self.client.clone(),
            kind,
            handler: self.handler.clone(),
            payload: self.payload.clone(),
            headers: self.headers.clone(),
            response_id,
            message_type,
        };

        tokio::spawn(async move {
            // The receiver is long gone whenever a fire caller dropped its
            // result. The send happened either way, which is the point.
            let _ = report_tx.send(slow_path.run().await);
        });

        report_rx
    }

    /// Post-acquisition dispatch: send inline when the peer is already known to
    /// carry the handler, otherwise hand the send to a detached task.
    ///
    /// Target resolution happens in [`MessageBuilder::make_stage`] *before* slot
    /// acquisition so `ResolveError::Other` (programmer misuse — target not set,
    /// or both `.instance()` and `.worker()` set) fails the caller without
    /// consuming a slot or waiting for one.
    fn dispatch(
        self,
        target_result: Result<InstanceId, ResolveError>,
        awaiter: ResponseAwaiter,
        message_type: MsgType,
    ) -> Dispatched {
        let worker_id = self.target_worker;
        let response_id = awaiter.response_id();

        match target_result {
            Ok(target) if self.client.can_send_directly(target, &self.handler) => {
                if let Some(metrics) = self.client.observability.as_ref() {
                    metrics.record_client_resolution(ClientResolution::DirectSuccess);
                }
                let metadata = self.create_metadata(response_id, message_type);
                let message = ActiveMessage {
                    metadata,
                    payload: self.payload.unwrap_or_default(),
                };
                stage_from_send(self.client.send_message(target, message), awaiter)
            }
            Ok(target) => Dispatched::detached(
                self.spawn_slow_path(SlowPathKind::Handshake(target), response_id, message_type),
                awaiter,
            ),
            Err(ResolveError::UnresolvedPeer) => {
                // `resolve_target` only returns UnresolvedPeer when
                // target_worker is Some, so this else-branch is a defensive
                // guard — unreachable in practice.
                let Some(worker_id) = worker_id else {
                    tracing::error!(target: "crate::messenger::client", "UnresolvedPeer but no worker_id set");
                    return Dispatched::failed("UnresolvedPeer but no worker_id set");
                };
                Dispatched::detached(
                    self.spawn_slow_path(
                        SlowPathKind::Discovery(worker_id),
                        response_id,
                        message_type,
                    ),
                    awaiter,
                )
            }
            Err(ResolveError::Other(_)) => {
                unreachable!("ResolveError::Other is short-circuited in make_stage")
            }
        }
    }

    /// Issue the send and wrap it in its [`SendStage`].
    ///
    /// Target resolution runs first so `ResolveError::Other` (programmer
    /// misuse) fails immediately and never consumes a slot. Under
    /// `await_capacity` that ordering is load-bearing: resolving inside the
    /// deferred future would make the caller wait for a slot before learning
    /// the call can never succeed.
    ///
    /// The send is then issued *eagerly* whenever a slot is free — including
    /// under `await_capacity` — so that a result nobody ever polls still
    /// delivers, and so that two sends issued in order reach the target's
    /// admission gate in that order. Only genuine arena exhaustion defers the
    /// send into the result, because there waiting is exactly what the caller
    /// asked for and spawning a task per waiting send would remove the
    /// backpressure it wanted.
    fn make_stage(self, message_type: MsgType) -> SendStage {
        let target_result = match self.resolve_target() {
            Err(ResolveError::Other(e)) => return SendStage::failed(e),
            other => other,
        };

        match self.acquire() {
            Acquisition::Allocated(awaiter) => {
                SendStage::Dispatched(self.dispatch(target_result, awaiter, message_type))
            }
            Acquisition::Deferred(backpressure) => {
                let deferred = Box::pin(async move {
                    let awaiter = backpressure.await;
                    self.dispatch(target_result, awaiter, message_type)
                });
                SendStage::Acquiring(deferred)
            }
            Acquisition::Exhausted(e) => SendStage::failed(e),
        }
    }

    /// Issue a fire-and-forget send.
    ///
    /// Synchronous: by the time this returns the frame belongs to the transport
    /// (or to a detached task). The [`FireResult`] observes admission, it does
    /// not drive it.
    pub fn fire(self) -> FireResult {
        FireResult {
            stage: self.make_stage(MsgType::Fire),
        }
    }

    pub fn sync(self) -> SyncResult {
        SyncResult {
            stage: self.make_stage(MsgType::Sync),
        }
    }

    pub fn unary(self) -> UnaryResult {
        UnaryResult {
            stage: self.make_stage(MsgType::Unary),
        }
    }

    pub fn typed<R>(self) -> TypedUnaryResult<R>
    where
        R: DeserializeOwned + Send + 'static,
    {
        TypedUnaryResult {
            stage: self.make_stage(MsgType::Unary),
            _marker: std::marker::PhantomData,
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
