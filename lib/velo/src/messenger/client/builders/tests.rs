// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the shared builder machinery: `ResponseStage`,
//! `stage_from_send`, `drive_send_outcome`, `drive_fire_send`. Each
//! branch is exercised here so coverage doesn't depend on end-to-end
//! transport integration.
use super::*;
use crate::messenger::common::responses::ResponseManager;
use crate::transports::{AdmissionError, AdmissionGate, SendAdmission};

fn make_awaiter() -> (
    crate::messenger::common::responses::ResponseAwaiter,
    crate::messenger::common::responses::ResponseId,
    Arc<ResponseManager>,
) {
    // `ResponseManager::new` accepts a u64 worker-id alias in this crate.
    let rm = Arc::new(ResponseManager::new(1));
    let awaiter = rm.register_outcome().expect("register");
    let id = awaiter.response_id();
    (awaiter, id, rm)
}

/// A gate over a one-slot channel that is already full, so the next send
/// queues instead of admitting.
///
/// The receiver and the gate are both retained so a test can choose how a
/// queued admission ends — released, or failed with the epoch.
struct Saturated {
    gate: AdmissionGate<()>,
    rx: flume::Receiver<()>,
}

impl Saturated {
    fn new() -> Self {
        let (tx, rx) = flume::bounded(1);
        let gate = AdmissionGate::new(tx, tokio::runtime::Handle::current());
        assert!(gate.send(()).is_admitted(), "the first send fills the slot");
        Self { gate, rx }
    }

    /// Queue a frame behind the full channel.
    fn queue(&self) -> SendAdmission {
        match self.gate.send(()) {
            SendOutcome::Pending(admission) => admission,
            SendOutcome::Admitted => panic!("a full channel must not admit"),
        }
    }

    /// Make room, so the queued frame can be enqueued.
    fn release(&self) {
        self.rx.try_recv().expect("a frame to release");
    }

    /// Kill the epoch, failing everything still queued.
    fn fail(&self) {
        self.gate.fail_all(AdmissionError::ConnectionReplaced);
    }
}

// ── ResponseStage ────────────────────────────────────────────────────

#[tokio::test]
async fn stage_ready_resolves_after_outcome_completes() {
    let (awaiter, id, rm) = make_awaiter();
    let stage = ResponseStage::ready(awaiter);
    let mut result = SyncResult {
        stage: StageState::Ready(stage),
    };

    // Completing the outcome lets the awaiter produce its value.
    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"ok")))));
    let r = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
        .await
        .expect("sync result completes");
    assert!(r.is_ok());
}

#[tokio::test]
async fn stage_with_admitted_frame_proceeds_to_awaiter() {
    let (awaiter, id, rm) = make_awaiter();
    let saturated = Saturated::new();
    let admission = saturated.queue();
    saturated.release();
    let stage = ResponseStage::with_admission(awaiter, Some(admission));
    let mut result = UnaryResult {
        stage: StageState::Ready(stage),
    };

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"hello")))));
    let r = tokio::time::timeout(std::time::Duration::from_secs(5), &mut result)
        .await
        .expect("unary result completes");
    assert_eq!(r.unwrap(), Bytes::from_static(b"hello"));
}

#[tokio::test]
async fn stage_waits_for_a_queued_frame() {
    let (awaiter, id, rm) = make_awaiter();
    let saturated = Saturated::new();
    let stage = ResponseStage::with_admission(awaiter, Some(saturated.queue()));
    let result = SyncResult {
        stage: StageState::Ready(stage),
    };
    // The response is already available; the result must still wait, because a
    // frame that has not reached the send channel cannot have been answered.
    assert!(rm.complete_outcome(id, Ok(None)));
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(100), result).await;
    assert!(
        outcome.is_err(),
        "an unadmitted frame should keep the result pending"
    );
}

#[tokio::test]
async fn stage_failed_admission_short_circuits() {
    // The frame never reached the wire, so no response is coming — the result
    // must fail rather than wait out its caller's timeout on the awaiter.
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let stage = ResponseStage::with_admission(awaiter, Some(saturated.queue()));
    let result = SyncResult {
        stage: StageState::Ready(stage),
    };
    saturated.fail();

    let err = tokio::time::timeout(std::time::Duration::from_secs(5), result)
        .await
        .expect("failed admission resolves the result")
        .expect_err("failed admission is an error");
    assert!(err.to_string().contains("Send failed"), "{err}");
}

#[tokio::test]
async fn stage_immediate_error_short_circuits() {
    let stage = ResponseStage::error(anyhow!("boom"));
    let result = SyncResult {
        stage: StageState::Ready(stage),
    };
    let err = result.await.expect_err("immediate_error returns Err");
    assert!(err.to_string().contains("boom"));
}

#[tokio::test]
async fn unary_result_empty_response_becomes_empty_bytes() {
    let (awaiter, id, rm) = make_awaiter();
    let stage = ResponseStage::ready(awaiter);
    let mut result = UnaryResult {
        stage: StageState::Ready(stage),
    };

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
        stage: StageState::Ready(stage),
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
        stage: StageState::Ready(stage),
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
        stage: StageState::Ready(stage),
        _marker: std::marker::PhantomData,
    };

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"not-json")))));
    let err = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
        .await
        .expect("typed resolves")
        .expect_err("bad json → Err");
    assert!(err.to_string().contains("Failed to deserialize"));
}

// ── StageState ───────────────────────────────────────────────────────
//
// Exercises the deferred-acquisition path (`StageState::Pending`) used by
// `MessageBuilder::await_capacity` without requiring a full
// `ActiveMessageClient`. We hand-build a boxed future that yields a ready
// `ResponseStage` and assert the poll loop transitions Pending → Ready
// and surfaces the awaiter's outcome.

#[tokio::test]
async fn stage_state_pending_transitions_and_resolves() {
    let (awaiter, id, rm) = make_awaiter();
    let fut: futures::future::BoxFuture<'static, ResponseStage> =
        Box::pin(async move { ResponseStage::ready(awaiter) });
    let mut result = SyncResult {
        stage: StageState::Pending(fut),
    };

    // Complete the outcome before polling so the transition proceeds.
    assert!(rm.complete_outcome(id, Ok(None)));
    let r = tokio::time::timeout(std::time::Duration::from_secs(1), &mut result)
        .await
        .expect("sync result resolves");
    assert!(r.is_ok());
}

#[tokio::test]
async fn stage_state_pending_awaits_inner_future() {
    // Pending future that never resolves — poll must stay Pending and
    // never touch the awaiter slot.
    let (awaiter, _id, _rm) = make_awaiter();
    let fut: futures::future::BoxFuture<'static, ResponseStage> = Box::pin(async {
        futures::future::pending::<()>().await;
        ResponseStage::ready(awaiter)
    });
    let result = SyncResult {
        stage: StageState::Pending(fut),
    };

    let outcome = tokio::time::timeout(std::time::Duration::from_millis(50), result).await;
    assert!(
        outcome.is_err(),
        "pending inner future should keep result pending"
    );
}

// ── drive_fire_send ──────────────────────────────────────────────────

#[tokio::test]
async fn drive_fire_send_admitted_is_ok() {
    let (awaiter, _id, _rm) = make_awaiter();
    assert!(
        drive_fire_send(Ok(SendOutcome::Admitted), awaiter)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn drive_fire_send_waits_out_a_queued_frame() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let admission = saturated.queue();
    saturated.release();
    assert!(
        drive_fire_send(Ok(SendOutcome::Pending(admission)), awaiter)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn drive_fire_send_failed_admission_is_err() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let admission = saturated.queue();
    saturated.fail();
    let err = drive_fire_send(Ok(SendOutcome::Pending(admission)), awaiter)
        .await
        .expect_err("a frame that never reached the channel is a failed send");
    assert!(err.to_string().contains("Send failed"), "{err}");
}

#[tokio::test]
async fn drive_fire_send_admitted_with_sync_on_error_surfaces_err() {
    // Transports like TCP's slow_path_send can invoke on_error
    // synchronously (e.g. transport not started, connection could not be
    // created) and still return Admitted. DefaultErrorHandler completes the
    // awaiter with Err before drive_fire_send runs — the Admitted arm must
    // surface that Err, not return Ok.
    let (awaiter, id, rm) = make_awaiter();
    assert!(rm.complete_outcome(id, Err("Connection closed immediately".to_string())));
    let err = drive_fire_send(Ok(SendOutcome::Admitted), awaiter)
        .await
        .expect_err("should surface sync on_error failure");
    assert!(err.to_string().contains("Connection closed immediately"));
}

#[tokio::test]
async fn drive_fire_send_admitted_frame_with_on_error_surfaces_err() {
    // The frame was admitted, but the handler completed the awaiter with Err
    // while it waited — the write itself failed. That is still a pre-wire
    // failure from the caller's point of view.
    let (awaiter, id, rm) = make_awaiter();
    assert!(rm.complete_outcome(id, Err("peer disconnected".to_string())));
    let saturated = Saturated::new();
    let admission = saturated.queue();
    saturated.release();
    let err = drive_fire_send(Ok(SendOutcome::Pending(admission)), awaiter)
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

// ── finish_fire_via_awaiter (slow-path completion) ───────────────────

#[tokio::test]
async fn finish_fire_via_awaiter_ok_on_success_completion() {
    let (awaiter, id, rm) = make_awaiter();
    // Simulate the spawned slow-path task completing the outcome with
    // Ok(None) after a successful enqueue.
    assert!(rm.complete_outcome(id, Ok(None)));
    assert!(finish_fire_via_awaiter(awaiter).await.is_ok());
}

#[tokio::test]
async fn finish_fire_via_awaiter_err_on_failure_completion() {
    let (awaiter, id, rm) = make_awaiter();
    // Simulate the spawned slow-path task completing with a discovery,
    // handshake, or send failure.
    assert!(rm.complete_outcome(id, Err("Handshake failed: nope".to_string())));
    let err = finish_fire_via_awaiter(awaiter)
        .await
        .expect_err("err completion surfaces");
    assert!(err.to_string().contains("Handshake failed"));
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
