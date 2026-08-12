// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Result types and send/backpressure state machinery backing the message builders.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::de::DeserializeOwned;

use super::super::ActiveMessageClient;
use crate::observability::ClientResolution;
use crate::transports::{SendBackpressure, SendOutcome};

/// Shared state for all builder result futures.
///
/// Drives any `SendBackpressure` to completion before polling the
/// `ResponseAwaiter`. `immediate_error` short-circuits both. This is the one
/// place the bp-then-response sequence lives; each public result type
/// (`SyncResult`/`UnaryResult`/`TypedUnaryResult`) is a thin wrapper that
/// maps the raw response bytes to its declared `Output`.
pub(super) struct ResponseStage {
    bp: Option<SendBackpressure>,
    awaiter: Option<crate::messenger::common::responses::ResponseAwaiter>,
    immediate_error: Option<anyhow::Error>,
}

/// Two-phase state for builder result futures.
///
/// In the fail-fast path the stage is constructed eagerly and stored as
/// `Ready`. In the `await_capacity` path slot acquisition is deferred into
/// a boxed future (`Pending`); the first poll drives it to completion,
/// produces a `ResponseStage`, and transitions to `Ready` for the rest of
/// the bp-then-response sequence. Collapsing both modes into one state
/// machine lets the public result types stay the same shape regardless of
/// acquisition policy.
pub(super) enum StageState {
    Ready(ResponseStage),
    Pending(futures::future::BoxFuture<'static, ResponseStage>),
}

impl StageState {
    pub(super) fn ready(stage: ResponseStage) -> Self {
        StageState::Ready(stage)
    }

    pub(super) fn error(err: anyhow::Error) -> Self {
        StageState::Ready(ResponseStage::error(err))
    }

    fn poll_raw(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        loop {
            match self {
                StageState::Pending(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(stage) => *self = StageState::Ready(stage),
                    Poll::Pending => return Poll::Pending,
                },
                StageState::Ready(stage) => return stage.poll_raw(cx),
            }
        }
    }
}

impl ResponseStage {
    pub(super) fn ready(awaiter: crate::messenger::common::responses::ResponseAwaiter) -> Self {
        Self {
            bp: None,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    pub(super) fn with_bp(
        awaiter: crate::messenger::common::responses::ResponseAwaiter,
        bp: Option<SendBackpressure>,
    ) -> Self {
        Self {
            bp,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    pub(super) fn error(err: anyhow::Error) -> Self {
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
    pub(super) stage: StageState,
}

impl Future for SyncResult {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_raw(cx).map(|r| r.map(|_| ()))
    }
}

/// Result wrapper for unary operations returning raw bytes.
pub struct UnaryResult {
    pub(super) stage: StageState,
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
    pub(super) stage: StageState,
    pub(super) _marker: std::marker::PhantomData<R>,
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

/// Drive a `send_message` result from inside a spawned (slow-path) task that
/// owns the response outcome.
///
/// - `Enqueued` → no-op; let the awaiter wait for the response.
/// - `Backpressured(bp)` → `.await` the bp future so the frame is actually
///   enqueued before this task returns.
/// - `Err` → log, emit `ClientResolution::SendError`, and complete the
///   response outcome with the error so callers don't wait forever.
pub(super) async fn drive_send_outcome(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: crate::messenger::common::responses::ResponseId,
    path_description: &'static str,
) {
    match send_result {
        Ok(SendOutcome::Enqueued) => {}
        Ok(SendOutcome::Backpressured(bp)) => bp.await,
        Err(e) => {
            tracing::error!(
                target: "crate::messenger::client",
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

/// Fast-path fire driver. Returns `Ok(())` once the frame has been handed to
/// the transport (either fast-pathed or bp-enqueued). Returns `Err` for
/// pre-wire failures:
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
pub(super) async fn drive_fire_send(
    send_result: Result<SendOutcome>,
    mut awaiter: crate::messenger::common::responses::ResponseAwaiter,
) -> Result<()> {
    use futures::FutureExt;
    // Some transports (e.g. TCP's `slow_path_send` via
    // `try_send_or_backpressure` on a disconnected channel, or early-returns
    // like "Transport not started" and "Failed to create connection") invoke
    // `on_error` synchronously and then return `Ok(())` — which `VeloBackend`
    // maps to `SendOutcome::Enqueued`. In those cases DefaultErrorHandler
    // has already completed the awaiter with Err before we get here. The
    // Backpressured arm has the same property once bp resolves (transport
    // calls on_error inside the bp future). Poll once in both arms and
    // surface any Err we find.
    match send_result {
        Ok(SendOutcome::Enqueued) => {}
        Ok(SendOutcome::Backpressured(bp)) => bp.await,
        Err(e) => return Err(e),
    }
    match awaiter.recv().now_or_never() {
        Some(Err(e)) => Err(anyhow!("Send failed: {}", e)),
        _ => Ok(()),
    }
}
