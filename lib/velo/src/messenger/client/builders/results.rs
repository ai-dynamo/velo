// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Result types and send-admission state machinery backing the message builders.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::de::DeserializeOwned;

use super::super::ActiveMessageClient;
use crate::observability::ClientResolution;
use crate::transports::{SendAdmission, SendOutcome};

/// Shared state for all builder result futures.
///
/// Waits for a queued frame's admission before polling the `ResponseAwaiter` —
/// a response cannot arrive for a frame that has not reached the send channel.
/// `immediate_error` short-circuits both. This is the one place the
/// admission-then-response sequence lives; each public result type
/// (`SyncResult`/`UnaryResult`/`TypedUnaryResult`) is a thin wrapper that
/// maps the raw response bytes to its declared `Output`.
pub(super) struct ResponseStage {
    admission: Option<SendAdmission>,
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
            admission: None,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    pub(super) fn with_admission(
        awaiter: crate::messenger::common::responses::ResponseAwaiter,
        admission: Option<SendAdmission>,
    ) -> Self {
        Self {
            admission,
            awaiter: Some(awaiter),
            immediate_error: None,
        }
    }

    pub(super) fn error(err: anyhow::Error) -> Self {
        Self {
            admission: None,
            awaiter: None,
            immediate_error: Some(err),
        }
    }

    /// Await admission (if the frame was queued) then the awaiter. Returns the
    /// raw response bytes; wrappers map this into their own `Output` type.
    ///
    /// A failed admission ends the call here rather than falling through to the
    /// awaiter: the frame never reached the wire, so no response is coming. The
    /// backend's completion hook has already released the response slot via
    /// `on_error`.
    fn poll_raw(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        if let Some(err) = self.immediate_error.take() {
            return Poll::Ready(Err(err));
        }

        if let Some(admission) = self.admission.as_mut() {
            match Pin::new(admission).poll(cx) {
                Poll::Ready(Ok(())) => self.admission = None,
                Poll::Ready(Err(error)) => {
                    self.admission = None;
                    self.awaiter = None;
                    return Poll::Ready(Err(anyhow!("Send failed: {error}")));
                }
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
/// - `Admitted` → no-op; let the awaiter wait for the response.
/// - `Pending(admission)` → `.await` it, so this task does not return before
///   the frame reaches the send channel. A failed admission is reported the
///   same way a routing error is: the frame never made it, so the caller must
///   not be left waiting for a response.
/// - `Err` → log, emit `ClientResolution::SendError`, and complete the
///   response outcome with the error so callers don't wait forever.
pub(super) async fn drive_send_outcome(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: crate::messenger::common::responses::ResponseId,
    path_description: &'static str,
) {
    let error = match send_result {
        Ok(SendOutcome::Admitted) => return,
        Ok(SendOutcome::Pending(admission)) => match admission.await {
            Ok(()) => return,
            Err(error) => anyhow!(error),
        },
        Err(error) => error,
    };

    tracing::error!(
        target: "crate::messenger::client",
        error = %error,
        path = path_description,
        "Failed to send message"
    );
    if let Some(metrics) = client.observability.as_ref() {
        metrics.record_client_resolution(ClientResolution::SendError);
    }
    let _ = client
        .response_manager
        .complete_outcome(response_id, Err(format!("Send failed: {}", error)));
}

/// Fast-path fire driver. Returns `Ok(())` once the frame has reached the
/// transport's send channel (immediately, or after waiting out a queue).
/// Returns `Err` for pre-wire failures:
///
/// - Synchronous `send_message` error (peer unregistered, transport-level
///   refusal).
/// - The admission resolves `Err` — the connection epoch died or the channel
///   closed before the frame was enqueued.
///
/// After the frame is accepted by the wire the awaiter is simply dropped —
/// fire-and-forget semantics mean we don't observe remote processing.
pub(super) async fn drive_fire_send(
    send_result: Result<SendOutcome>,
    mut awaiter: crate::messenger::common::responses::ResponseAwaiter,
) -> Result<()> {
    use futures::FutureExt;
    // A hard pre-wire failure inside a transport (peer unregistered, transport
    // not started, connection could not be created) is reported through
    // `on_error` and then returns `Admitted` — there is nothing left to wait
    // on. `DefaultErrorHandler` has already completed the awaiter with `Err` by
    // the time we get here, so poll it once, non-blockingly, and surface any
    // completion we find.
    match send_result {
        Ok(SendOutcome::Admitted) => {}
        Ok(SendOutcome::Pending(admission)) => {
            admission.await.map_err(|e| anyhow!("Send failed: {}", e))?;
        }
        Err(e) => return Err(e),
    }
    match awaiter.recv().now_or_never() {
        Some(Err(e)) => Err(anyhow!("Send failed: {}", e)),
        _ => Ok(()),
    }
}
