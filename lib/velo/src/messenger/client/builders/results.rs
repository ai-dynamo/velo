// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Result types and send-admission state machinery backing the message builders.
//!
//! A send passes through up to three stages, in this order:
//!
//! 1. **Acquisition** — take a response slot. Skipped unless the arena is full
//!    *and* the caller opted into waiting with `await_capacity`.
//! 2. **Admission** — the frame reaches the transport's send channel.
//! 3. **Response** — the remote answers. Fire-and-forget sends stop at 2.
//!
//! Each stage is its own typed state ([`SendStage`], [`AdmissionStage`]) rather
//! than a flag another stage has to interpret. That is what makes admission
//! separately observable: every result type exposes `admission_state()` and
//! `admitted()`, which read and drive stage 2 without consuming the result, so a
//! unary caller can watch its frame reach the wire and still await the same
//! result for the response afterwards.
//!
//! Both terminal admission answers are memoized. Once a send has been admitted
//! or has failed, every later reader gets the same answer — which is why
//! `admitted()` can resolve and the result still be awaited.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use tokio::sync::oneshot;

use super::super::ActiveMessageClient;
use crate::messenger::common::responses::{ResponseAwaiter, ResponseId};
use crate::observability::ClientResolution;
use crate::transports::{AdmissionState, SendAdmission, SendOutcome};

/// What a detached send task reports when it finishes.
///
/// `Ok(())` once the frame has reached the transport's send channel, otherwise
/// the message to hand the caller. A `String` rather than an `anyhow::Error`
/// because it crosses a task boundary and because the same failure has to be
/// reportable more than once — `admitted()` and a later `.await` of the result
/// must both surface it.
pub(super) type AdmissionReport = std::result::Result<(), String>;

/// How far a send has got towards the transport's send channel.
enum AdmissionStage {
    /// Terminal: the frame is on the send channel.
    Admitted,
    /// Terminal: the frame will never get there.
    Failed(String),
    /// The target's admission gate holds the frame behind its predecessors.
    /// The gate delivers it whether or not this is ever polled.
    Gated(SendAdmission),
    /// A detached task owns the whole send — discovery, handshake, or a
    /// deferred slot acquisition — and reports through this channel. The task
    /// never waits on the receiver, so dropping the result cannot stall it.
    Detached(oneshot::Receiver<AdmissionReport>),
}

impl AdmissionStage {
    /// Synchronous read. Never blocks and never advances the state machine.
    fn state(&self) -> AdmissionState {
        match self {
            Self::Admitted => AdmissionState::Admitted,
            Self::Failed(_) => AdmissionState::Failed,
            // The gate resolves tickets whether or not anyone polls, so its own
            // view of the ticket is fresher than any we could cache.
            Self::Gated(admission) => admission.state(),
            // A oneshot cannot be read without `&mut self`, so a detached send
            // reads `Pending` until something drives it.
            Self::Detached(_) => AdmissionState::Pending,
        }
    }

    /// Drive to a terminal state, memoizing the answer on the way.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match self {
                Self::Admitted => return Poll::Ready(Ok(())),
                Self::Failed(reason) => return Poll::Ready(Err(anyhow!("{reason}"))),
                Self::Gated(admission) => match Pin::new(admission).poll(cx) {
                    Poll::Ready(Ok(())) => *self = Self::Admitted,
                    Poll::Ready(Err(error)) => {
                        *self = Self::Failed(format!("Send failed: {error}"))
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Self::Detached(receiver) => match Pin::new(receiver).poll(cx) {
                    Poll::Ready(Ok(report)) => {
                        *self = match report {
                            Ok(()) => Self::Admitted,
                            Err(reason) => Self::Failed(reason),
                        }
                    }
                    // The task dropped its sender without reporting: it
                    // panicked, or the runtime shut down under it. Either way
                    // nothing is going to admit this frame now.
                    Poll::Ready(Err(_)) => {
                        *self = Self::Failed(
                            "Send failed: the send task ended without reporting".to_string(),
                        )
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

/// A send that has been issued: its admission stage plus its response slot.
pub(super) struct Dispatched {
    admission: AdmissionStage,
    /// The response slot registered for this send.
    ///
    /// Sync/unary/typed sends await it for the remote's answer. Fire sends
    /// never do — there is no response — and hold it only so the frame carries
    /// a `response_id` the transport's error handler can correlate against, and
    /// so `await_capacity` still gates on the arena.
    awaiter: Option<ResponseAwaiter>,
}

impl Dispatched {
    /// A send issued synchronously on the caller's task.
    pub(super) fn issued(outcome: SendOutcome, awaiter: ResponseAwaiter) -> Self {
        let admission = match outcome {
            SendOutcome::Admitted => AdmissionStage::Admitted,
            SendOutcome::Pending(admission) => AdmissionStage::Gated(admission),
        };
        Self {
            admission,
            awaiter: Some(awaiter),
        }
    }

    /// A send a spawned task owns; `receiver` carries its verdict back.
    pub(super) fn detached(
        receiver: oneshot::Receiver<AdmissionReport>,
        awaiter: ResponseAwaiter,
    ) -> Self {
        Self {
            admission: AdmissionStage::Detached(receiver),
            awaiter: Some(awaiter),
        }
    }

    /// A send that failed before any transport saw it — bad target
    /// configuration, an exhausted slot arena, an encode error.
    pub(super) fn failed(error: impl std::fmt::Display) -> Self {
        Self {
            admission: AdmissionStage::Failed(error.to_string()),
            awaiter: None,
        }
    }

    /// Admission, then the remote's response.
    ///
    /// A failed admission ends the call here rather than falling through to the
    /// awaiter: the frame never reached the wire, so no response is coming and
    /// waiting for one would burn the caller's timeout. Dropping the slot on
    /// that path returns it to the arena immediately.
    ///
    /// A result awaited twice reports an error rather than panicking. Polling a
    /// finished future is a caller bug either way, but `admitted()` hands out
    /// `&mut` results, which makes a second `.await` easy enough to write that
    /// taking down the caller's task over it would be the wrong trade.
    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        if let Err(error) = ready!(self.admission.poll(cx)) {
            self.awaiter = None;
            return Poll::Ready(Err(error));
        }

        let Some(awaiter) = self.awaiter.as_mut() else {
            return Poll::Ready(Err(anyhow!("send result polled after completion")));
        };
        match awaiter.poll_recv(cx) {
            Poll::Ready(result) => {
                self.awaiter = None;
                Poll::Ready(result.map_err(|e| anyhow!(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Admission, plus one non-blocking look at the response slot.
    ///
    /// The probe is there for transports that report a hard pre-wire failure
    /// (peer unregistered, connection could not be created) through the error
    /// handler and *still* return `Admitted`: by the time we get here the
    /// handler has already written that error into the slot, and ignoring it
    /// would turn a failed send into a silent `Ok`. `now_or_never` rather than a
    /// real poll so no waker is parked on a slot that is about to be recycled.
    fn poll_fire(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = ready!(self.admission.poll(cx)) {
            self.awaiter = None;
            return Poll::Ready(Err(error));
        }

        let probe = self
            .awaiter
            .as_mut()
            .and_then(|awaiter| awaiter.recv().now_or_never());
        // A fire send has no response to wait for, so the slot goes back now.
        self.awaiter = None;
        match probe {
            Some(Err(error)) => Poll::Ready(Err(anyhow!("Send failed: {error}"))),
            _ => Poll::Ready(Ok(())),
        }
    }
}

/// Acquisition and everything after it, as one state machine.
///
/// Sends are issued eagerly whenever a response slot is free, so a result that
/// is never polled still delivers. `Acquiring` exists only for the corner where
/// the arena is full and the caller opted into waiting: there the send really
/// must not happen until somebody drives it, because waiting is the point.
pub(super) enum SendStage {
    /// Slot acquisition — and the send behind it — run on the first poll.
    Acquiring(futures::future::BoxFuture<'static, Dispatched>),
    /// The send has been issued.
    Dispatched(Dispatched),
}

impl SendStage {
    /// A send that failed before any transport saw it.
    pub(super) fn failed(error: impl std::fmt::Display) -> Self {
        SendStage::Dispatched(Dispatched::failed(error))
    }

    fn admission_state(&self) -> AdmissionState {
        match self {
            // Nothing has been offered to a transport yet.
            SendStage::Acquiring(_) => AdmissionState::Pending,
            SendStage::Dispatched(dispatched) => dispatched.admission.state(),
        }
    }

    /// Drive slot acquisition to completion and hand back the issued send.
    ///
    /// Two matches rather than one loop: the borrow checker will not let a
    /// reference into `self` escape a match arm that also reassigns `self`.
    fn poll_dispatched(&mut self, cx: &mut Context<'_>) -> Poll<&mut Dispatched> {
        if let SendStage::Acquiring(fut) = self {
            match fut.as_mut().poll(cx) {
                Poll::Ready(dispatched) => *self = SendStage::Dispatched(dispatched),
                Poll::Pending => return Poll::Pending,
            }
        }
        match self {
            SendStage::Dispatched(dispatched) => Poll::Ready(dispatched),
            SendStage::Acquiring(_) => unreachable!("the branch above replaced Acquiring"),
        }
    }

    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>>> {
        ready!(self.poll_dispatched(cx)).poll_response(cx)
    }

    fn poll_fire(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        ready!(self.poll_dispatched(cx)).poll_fire(cx)
    }
}

/// Future returned by `admitted()` on any send result.
///
/// Borrows its result instead of consuming it, so a unary caller can wait for
/// the frame to reach the wire and then await the same result for the response.
/// Resolves `Ok(())` when the frame is on the transport's send channel and `Err`
/// when it never will be. Dropping it abandons the wait, never the send.
pub struct Admitted<'a> {
    stage: &'a mut SendStage,
}

impl Future for Admitted<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let stage = &mut *self.get_mut().stage;
        ready!(stage.poll_dispatched(cx)).admission.poll(cx)
    }
}

/// Give a result type its admission surface.
///
/// A macro rather than a trait so callers need no import, and rather than four
/// hand-written copies so the contract is stated once.
macro_rules! admission_api {
    ($result:ty $(, $param:ident)?) => {
        impl$(<$param>)? $result {
            /// Where this send has got to, right now.
            ///
            /// Cheap, synchronous, and safe outside an async context.
            /// [`AdmissionState::Pending`] means the frame is queued behind its
            /// predecessors on this target (or, under `await_capacity` with a
            /// full slot arena, that the send has not been issued yet);
            /// [`AdmissionState::Admitted`] that it reached the transport's send
            /// channel; [`AdmissionState::Failed`] that it never will.
            ///
            /// A send that took the synchronous fast path reads `Admitted` from
            /// the moment the result is handed back.
            pub fn admission_state(&self) -> AdmissionState {
                self.stage.admission_state()
            }

            /// Wait for the frame to reach the transport's send channel.
            ///
            /// Completes strictly before any response can: a remote cannot
            /// answer a frame that has not been sent. Borrows rather than
            /// consumes, so the result is still awaitable afterwards for the
            /// response. Resolves immediately for a send that was admitted
            /// synchronously.
            ///
            /// An `Err` here means the frame never reached the wire. Failures
            /// *after* admission are a different thing and surface through the
            /// transport's error handler (and, for sync/unary, through the
            /// result itself).
            pub fn admitted(&mut self) -> Admitted<'_> {
                Admitted {
                    stage: &mut self.stage,
                }
            }
        }
    };
}

/// Result of a fire-and-forget send.
///
/// Awaiting it completes at **admission** — when the frame reaches the
/// transport's send channel, not when the remote handles it. A fire send has no
/// response, so admission is the whole story: `Ok(())` means the frame is the
/// transport's problem now, `Err` that it never got that far. Failures after
/// admission (a write that fails, a peer that disconnects mid-frame) surface
/// through the transport's error handler, as they always have.
///
/// Awaiting the result is therefore stricter than
/// [`admitted`](FireResult::admitted): some transports report a hard pre-wire
/// failure through the error handler and *still* return admitted, and it is
/// awaiting the result that looks for one of those. `admitted()` answers only
/// the question it is named for — did the transport take the frame — so prefer
/// awaiting the result when you intend to act on a failed send.
///
/// **Dropping this does not cancel the send**, which is why it is deliberately
/// not `#[must_use]`: the send was issued when `send()` returned and the
/// target's admission gate delivers the frame whether or not anyone polls. The
/// single exception is `await_capacity` when the response-slot arena is
/// genuinely full — there the send waits for a slot, and waiting is what the
/// caller asked for, so the result has to be polled to make progress.
pub struct FireResult {
    pub(super) stage: SendStage,
}

impl Future for FireResult {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_fire(cx)
    }
}

/// Result wrapper for sync operations (acknowledgment only).
///
/// Awaiting it waits for the remote's acknowledgement. Use
/// [`admitted`](SyncResult::admitted) to wait only for the frame to be sent.
pub struct SyncResult {
    pub(super) stage: SendStage,
}

impl Future for SyncResult {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_response(cx).map(|r| r.map(|_| ()))
    }
}

/// Result wrapper for unary operations returning raw bytes.
pub struct UnaryResult {
    pub(super) stage: SendStage,
}

impl Future for UnaryResult {
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage
            .poll_response(cx)
            .map(|r| r.map(|b| b.unwrap_or_default()))
    }
}

/// Result wrapper for typed unary operations with deserialization.
pub struct TypedUnaryResult<R> {
    pub(super) stage: SendStage,
    pub(super) _marker: std::marker::PhantomData<R>,
}

// Safe: `TypedUnaryResult` only holds `SendStage` (Unpin) and `PhantomData<R>`.
// The `R` type parameter never appears in a field that stores an `R`, so `Unpin`
// is correct regardless of `R`'s own `Unpin`.
impl<R> Unpin for TypedUnaryResult<R> {}

impl<R: DeserializeOwned> Future for TypedUnaryResult<R> {
    type Output = Result<R>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stage.poll_response(cx).map(|r| match r {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("Failed to deserialize response: {}", e)),
            Ok(None) => Err(anyhow!("Expected response data, got empty")),
            Err(e) => Err(e),
        })
    }
}

admission_api!(FireResult);
admission_api!(SyncResult);
admission_api!(UnaryResult);
admission_api!(TypedUnaryResult<R>, R);

/// Drive a `send_message` result from inside a spawned (slow-path) task and
/// report what happened.
///
/// - `Admitted` → `Ok(())`; the response, if any, is the result's problem.
/// - `Pending(admission)` → `.await` it, so the task does not report before the
///   frame reaches the send channel. A failed admission is reported the same way
///   a routing error is: the frame never made it.
/// - `Err` → log and emit `ClientResolution::SendError`.
///
/// The response outcome is completed with the error as well as returned. The
/// return value is what the caller's result reads; completing the outcome is
/// what stops a sync/unary caller already parked on its awaiter from waiting out
/// its own timeout.
pub(super) async fn drive_send_outcome(
    client: &ActiveMessageClient,
    send_result: Result<SendOutcome>,
    response_id: ResponseId,
    path_description: &'static str,
) -> AdmissionReport {
    let error = match send_result {
        Ok(SendOutcome::Admitted) => return Ok(()),
        Ok(SendOutcome::Pending(admission)) => match admission.await {
            Ok(()) => return Ok(()),
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
    let reason = format!("Send failed: {}", error);
    let _ = client
        .response_manager
        .complete_outcome(response_id, Err(reason.clone()));
    Err(reason)
}
