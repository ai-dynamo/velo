// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the shared builder machinery: the admission stage, the
//! acquisition stage, the four result types, and `stage_from_send`. Each branch
//! is exercised here so coverage does not depend on end-to-end transport
//! integration; the parts that need a real wire (an unpolled fire send still
//! arriving, admission ordering ahead of a real response) live in
//! `tests/send_results.rs`.
use super::*;
use crate::messenger::common::responses::ResponseManager;
use crate::transports::{AdmissionError, AdmissionGate, AdmissionState, SendAdmission};
use futures::FutureExt;
use std::time::Duration;

/// Long enough that a resolution which is going to happen has happened.
const RESOLVES: Duration = Duration::from_secs(5);
/// Short enough to keep the suite quick when asserting something does *not*
/// resolve.
const STAYS_PENDING: Duration = Duration::from_millis(100);

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

fn fire_result(dispatched: Dispatched) -> FireResult {
    FireResult {
        stage: SendStage::Dispatched(dispatched),
    }
}

fn sync_result(dispatched: Dispatched) -> SyncResult {
    SyncResult {
        stage: SendStage::Dispatched(dispatched),
    }
}

fn unary_result(dispatched: Dispatched) -> UnaryResult {
    UnaryResult {
        stage: SendStage::Dispatched(dispatched),
    }
}

fn typed_result<R>(dispatched: Dispatched) -> TypedUnaryResult<R> {
    TypedUnaryResult {
        stage: SendStage::Dispatched(dispatched),
        _marker: std::marker::PhantomData,
    }
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

    /// Queue a frame and wrap it in a dispatched send.
    fn dispatch(
        &self,
        awaiter: crate::messenger::common::responses::ResponseAwaiter,
    ) -> Dispatched {
        Dispatched::issued(SendOutcome::Pending(self.queue()), awaiter)
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

// ── Admission state: the fast path ───────────────────────────────────

#[tokio::test]
async fn a_synchronously_admitted_send_reports_admitted_from_the_start() {
    let (awaiter, _id, _rm) = make_awaiter();
    let mut result = fire_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert_eq!(result.admission_state(), AdmissionState::Admitted);
    // There is nothing to wait for, so this must not need the executor to make
    // progress — hence `now_or_never` rather than `.await`.
    assert!(
        result
            .admitted()
            .now_or_never()
            .expect("an admitted send resolves without yielding")
            .is_ok()
    );
    // And the result itself is still awaitable afterwards.
    assert!(result.await.is_ok());
}

#[tokio::test]
async fn admitted_does_not_consume_a_unary_result() {
    let (awaiter, id, rm) = make_awaiter();
    let mut result = unary_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(result.admitted().await.is_ok());
    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"pong")))));
    let bytes = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary resolves")
        .expect("unary succeeds");
    assert_eq!(bytes, Bytes::from_static(b"pong"));
}

#[tokio::test]
async fn a_result_awaited_twice_errors_rather_than_panicking() {
    // Polling a finished future is a caller bug, but `admitted()` hands out
    // `&mut` results, so a stray second `.await` is easy to write and must not
    // take the caller's task down.
    let (awaiter, id, rm) = make_awaiter();
    let mut result = unary_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(rm.complete_outcome(id, Ok(None)));
    tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary resolves")
        .expect("unary succeeds");

    let err = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("the second await resolves too")
        .expect_err("there is no second response");
    assert!(err.to_string().contains("polled after completion"), "{err}");
}

// ── Admission state: a queued frame ──────────────────────────────────

#[tokio::test]
async fn a_queued_send_reads_pending_then_flips_to_admitted() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let mut result = fire_result(saturated.dispatch(awaiter));

    // Nothing has been polled: the gate's driver owns the frame, and the state
    // read goes straight to the ticket.
    assert_eq!(result.admission_state(), AdmissionState::Pending);

    saturated.release();
    tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("the admission resolves once the channel drains")
        .expect("the frame is admitted, not failed");
    assert_eq!(result.admission_state(), AdmissionState::Admitted);
}

#[tokio::test]
async fn admitted_resolves_before_the_response() {
    let (awaiter, id, rm) = make_awaiter();
    let saturated = Saturated::new();
    let mut result = unary_result(saturated.dispatch(awaiter));

    assert_eq!(result.admission_state(), AdmissionState::Pending);
    saturated.release();
    tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("the admission resolves")
        .expect("the frame is admitted");

    // The interesting half: admission is done while the response is still
    // outstanding, and waiting for it did not consume the result.
    assert!(
        tokio::time::timeout(STAYS_PENDING, &mut result)
            .await
            .is_err(),
        "the response has not arrived yet"
    );

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"pong")))));
    let bytes = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary resolves")
        .expect("unary succeeds");
    assert_eq!(bytes, Bytes::from_static(b"pong"));
}

#[tokio::test]
async fn a_result_waits_for_a_queued_frame_before_its_response() {
    let (awaiter, id, rm) = make_awaiter();
    let saturated = Saturated::new();
    let result = sync_result(saturated.dispatch(awaiter));

    // The response is already available; the result must still wait, because a
    // frame that has not reached the send channel cannot have been answered.
    assert!(rm.complete_outcome(id, Ok(None)));
    assert!(
        tokio::time::timeout(STAYS_PENDING, result).await.is_err(),
        "an unadmitted frame should keep the result pending"
    );
}

#[tokio::test]
async fn an_admitted_frame_proceeds_to_the_response() {
    let (awaiter, id, rm) = make_awaiter();
    let saturated = Saturated::new();
    let mut result = unary_result(saturated.dispatch(awaiter));
    saturated.release();

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"hello")))));
    let bytes = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary result completes")
        .expect("unary succeeds");
    assert_eq!(bytes, Bytes::from_static(b"hello"));
}

// ── Admission state: failure ─────────────────────────────────────────

#[tokio::test]
async fn a_failed_admission_reads_failed_and_repeats_its_error() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let mut result = fire_result(saturated.dispatch(awaiter));
    saturated.fail();

    let err = tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("a failed admission resolves the wait")
        .expect_err("a failed admission is an error");
    assert!(err.to_string().contains("Send failed"), "{err}");
    assert_eq!(result.admission_state(), AdmissionState::Failed);

    // Memoized: awaiting the result reports the same failure rather than
    // panicking on a consumed admission or hanging on a response that is never
    // coming.
    let err = result.await.expect_err("the send still failed");
    assert!(err.to_string().contains("Send failed"), "{err}");
}

#[tokio::test]
async fn a_failed_admission_short_circuits_the_response() {
    // The frame never reached the wire, so no response is coming — the result
    // must fail rather than wait out its caller's timeout on the awaiter.
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let result = sync_result(saturated.dispatch(awaiter));
    saturated.fail();

    let err = tokio::time::timeout(RESOLVES, result)
        .await
        .expect("failed admission resolves the result")
        .expect_err("failed admission is an error");
    assert!(err.to_string().contains("Send failed"), "{err}");
}

#[tokio::test]
async fn a_send_that_never_reached_a_transport_is_failed() {
    let mut result = sync_result(Dispatched::failed("boom"));

    assert_eq!(result.admission_state(), AdmissionState::Failed);
    let err = result
        .admitted()
        .now_or_never()
        .expect("a terminal state resolves without yielding")
        .expect_err("a failed send is an error");
    assert!(err.to_string().contains("boom"), "{err}");
    assert_eq!(result.await.expect_err("still failed").to_string(), "boom");
}

// ── Admission state: a detached send ─────────────────────────────────

#[tokio::test]
async fn a_detached_send_reports_through_its_channel() {
    let (awaiter, id, rm) = make_awaiter();
    let (report_tx, report_rx) = oneshot::channel();
    let mut result = unary_result(Dispatched::detached(report_rx, awaiter));

    // A oneshot cannot be read without polling it, so an undriven detached send
    // reads Pending.
    assert_eq!(result.admission_state(), AdmissionState::Pending);
    report_tx.send(Ok(())).expect("the receiver is alive");

    tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("the report resolves the admission")
        .expect("the task reported success");
    assert_eq!(result.admission_state(), AdmissionState::Admitted);

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"ok")))));
    let bytes = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary resolves")
        .expect("unary succeeds");
    assert_eq!(bytes, Bytes::from_static(b"ok"));
}

#[tokio::test]
async fn a_detached_failure_keeps_its_wording() {
    let (awaiter, _id, _rm) = make_awaiter();
    let (report_tx, report_rx) = oneshot::channel();
    let mut result = fire_result(Dispatched::detached(report_rx, awaiter));

    report_tx
        .send(Err("Handshake failed: nope".to_string()))
        .expect("the receiver is alive");

    let err = tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("the report resolves the admission")
        .expect_err("the task reported a failure");
    assert!(err.to_string().contains("Handshake failed"), "{err}");
    assert_eq!(result.admission_state(), AdmissionState::Failed);
}

#[tokio::test]
async fn a_detached_task_that_vanishes_fails_the_send() {
    // A task that panicked (or a runtime shutting down under it) drops the
    // sender without reporting. Nothing is going to admit the frame, so the
    // result must resolve rather than hang.
    let (awaiter, _id, _rm) = make_awaiter();
    let (report_tx, report_rx) = oneshot::channel::<AdmissionReport>();
    let result = fire_result(Dispatched::detached(report_rx, awaiter));
    drop(report_tx);

    let err = tokio::time::timeout(RESOLVES, result)
        .await
        .expect("a dropped sender resolves the result")
        .expect_err("a send nobody reported is a failed send");
    assert!(err.to_string().contains("without reporting"), "{err}");
}

// ── Acquisition stage ────────────────────────────────────────────────
//
// The deferred-acquisition path (`SendStage::Acquiring`) is used by
// `await_capacity` when the slot arena is genuinely full. We hand-build a boxed
// future that yields a dispatched send and assert the poll loop transitions
// Acquiring → Dispatched, from both entry points.

#[tokio::test]
async fn an_acquiring_send_reads_pending_and_then_resolves() {
    let (awaiter, id, rm) = make_awaiter();
    let deferred = Box::pin(async move { Dispatched::issued(SendOutcome::Admitted, awaiter) });
    let mut result = SyncResult {
        stage: SendStage::Acquiring(deferred),
    };

    // Nothing has been offered to a transport yet.
    assert_eq!(result.admission_state(), AdmissionState::Pending);

    assert!(rm.complete_outcome(id, Ok(None)));
    tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("sync result resolves")
        .expect("sync succeeds");
}

#[tokio::test]
async fn admitted_drives_the_acquisition_stage() {
    let (awaiter, _id, _rm) = make_awaiter();
    let deferred = Box::pin(async move { Dispatched::issued(SendOutcome::Admitted, awaiter) });
    let mut result = UnaryResult {
        stage: SendStage::Acquiring(deferred),
    };

    tokio::time::timeout(RESOLVES, result.admitted())
        .await
        .expect("acquisition then admission resolves")
        .expect("the send is admitted");
    assert_eq!(result.admission_state(), AdmissionState::Admitted);
}

#[tokio::test]
async fn an_acquiring_send_waits_for_its_slot() {
    // Acquisition that never completes — the result must stay pending and never
    // touch a response slot.
    let (awaiter, _id, _rm) = make_awaiter();
    let deferred = Box::pin(async {
        futures::future::pending::<()>().await;
        Dispatched::issued(SendOutcome::Admitted, awaiter)
    });
    let result = SyncResult {
        stage: SendStage::Acquiring(deferred),
    };

    assert!(
        tokio::time::timeout(STAYS_PENDING, result).await.is_err(),
        "a pending acquisition should keep the result pending"
    );
}

// ── Response mapping ─────────────────────────────────────────────────

#[tokio::test]
async fn unary_result_empty_response_becomes_empty_bytes() {
    let (awaiter, id, rm) = make_awaiter();
    let mut result = unary_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(rm.complete_outcome(id, Ok(None)));
    let bytes = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("unary resolves")
        .unwrap();
    assert_eq!(bytes, Bytes::new());
}

#[tokio::test]
async fn typed_result_deserializes_payload() {
    let (awaiter, id, rm) = make_awaiter();
    let mut result: TypedUnaryResult<i64> =
        typed_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from(b"42".to_vec())))));
    let value = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("typed resolves")
        .unwrap();
    assert_eq!(value, 42);
}

#[tokio::test]
async fn typed_result_empty_response_is_error() {
    let (awaiter, id, rm) = make_awaiter();
    let mut result: TypedUnaryResult<i64> =
        typed_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(rm.complete_outcome(id, Ok(None)));
    let err = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("typed resolves")
        .expect_err("empty response → Err");
    assert!(err.to_string().contains("Expected response data"));
}

#[tokio::test]
async fn typed_result_bad_json_is_error() {
    let (awaiter, id, rm) = make_awaiter();
    let mut result: TypedUnaryResult<i64> =
        typed_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    assert!(rm.complete_outcome(id, Ok(Some(Bytes::from_static(b"not-json")))));
    let err = tokio::time::timeout(RESOLVES, &mut result)
        .await
        .expect("typed resolves")
        .expect_err("bad json → Err");
    assert!(err.to_string().contains("Failed to deserialize"));
}

// ── Fire results ─────────────────────────────────────────────────────

#[tokio::test]
async fn fire_result_waits_out_a_queued_frame() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let result = fire_result(saturated.dispatch(awaiter));
    saturated.release();

    tokio::time::timeout(RESOLVES, result)
        .await
        .expect("the fire result resolves once the channel drains")
        .expect("the frame was admitted");
}

#[tokio::test]
async fn fire_result_surfaces_a_synchronous_transport_error() {
    // Transports like TCP's slow_path_send can invoke on_error synchronously
    // (transport not started, connection could not be created) and still return
    // Admitted. The error handler completes the response slot with Err before we
    // ever poll — the probe in `poll_fire` is what stops that turning into a
    // silent Ok.
    let (awaiter, id, rm) = make_awaiter();
    assert!(rm.complete_outcome(id, Err("Connection closed immediately".to_string())));
    let mut result = fire_result(Dispatched::issued(SendOutcome::Admitted, awaiter));

    // `admitted()` reports admission and nothing else — the transport did say
    // the frame was admitted. The probe belongs to awaiting the result, so that
    // is where the failure appears.
    assert!(result.admitted().await.is_ok());
    let err = result.await.expect_err("the send failed before the wire");
    assert!(err.to_string().contains("Connection closed immediately"));
}

#[tokio::test]
async fn fire_result_surfaces_a_transport_error_after_admission() {
    // The frame was admitted, but the handler recorded a failure while it
    // waited. Still a pre-wire failure from the caller's point of view.
    let (awaiter, id, rm) = make_awaiter();
    assert!(rm.complete_outcome(id, Err("peer disconnected".to_string())));
    let saturated = Saturated::new();
    let result = fire_result(saturated.dispatch(awaiter));
    saturated.release();

    let err = tokio::time::timeout(RESOLVES, result)
        .await
        .expect("the fire result resolves")
        .expect_err("the write failed");
    assert!(err.to_string().contains("peer disconnected"), "{err}");
}

#[tokio::test]
async fn fire_result_failed_admission_is_err() {
    let (awaiter, _id, _rm) = make_awaiter();
    let saturated = Saturated::new();
    let result = fire_result(saturated.dispatch(awaiter));
    saturated.fail();

    let err = tokio::time::timeout(RESOLVES, result)
        .await
        .expect("a failed admission resolves the result")
        .expect_err("a frame that never reached the channel is a failed send");
    assert!(err.to_string().contains("Send failed"), "{err}");
}

// ── stage_from_send ──────────────────────────────────────────────────

#[tokio::test]
async fn stage_from_send_admitted_is_ok() {
    let (awaiter, _id, _rm) = make_awaiter();
    let dispatched = stage_from_send(Ok(SendOutcome::Admitted), awaiter);
    assert!(fire_result(dispatched).await.is_ok());
}

#[tokio::test]
async fn stage_from_send_err_becomes_a_failed_admission() {
    let (awaiter, _id, _rm) = make_awaiter();
    let result = fire_result(stage_from_send(
        Err(anyhow!("peer not registered")),
        awaiter,
    ));

    assert_eq!(result.admission_state(), AdmissionState::Failed);
    let err = result
        .await
        .expect_err("a synchronous send error propagates");
    assert!(err.to_string().contains("peer not registered"), "{err}");
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
