// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordering, cancellation and epoch-death tests for the admission gate.

use super::*;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(10);

/// Unwrap an outcome that must have queued a ticket.
fn pending(outcome: SendOutcome) -> SendAdmission {
    match outcome {
        SendOutcome::Pending(admission) => admission,
        SendOutcome::Admitted => panic!("expected a queued ticket, got Admitted"),
    }
}

/// Receive one frame, failing the test rather than hanging.
async fn recv<T>(rx: &flume::Receiver<T>) -> T {
    timeout(LIMIT, rx.recv_async())
        .await
        .expect("receive timed out")
        .expect("channel closed")
}

/// Yield until `condition` holds, failing the test rather than spinning.
///
/// Preferred over a fixed number of yields, which flakes on a loaded runner.
async fn wait_until(label: &str, mut condition: impl FnMut() -> bool) {
    let waited = timeout(LIMIT, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(waited.is_ok(), "timed out waiting for {label}");
}

/// Assert an outcome took the synchronous fast path.
fn admitted(outcome: SendOutcome) {
    match outcome {
        SendOutcome::Admitted => {}
        SendOutcome::Pending(_) => panic!("expected Admitted, got a queued ticket"),
    }
}

// The auto-trait shape below is load-bearing for the transport swap: a gate
// lives behind `Arc<dyn Transport>` and its admissions are held across
// `.await` points in transport writer tasks.
const fn assert_send_sync<T: Send + Sync>() {}
const fn assert_unpin<T: Unpin>() {}
const _: () = {
    assert_send_sync::<SendAdmission>();
    assert_send_sync::<SendOutcome>();
    assert_send_sync::<AdmissionError>();
    assert_send_sync::<AdmissionGate<Vec<u8>>>();
    // `(&mut admission).await` in the tests below silently requires this.
    assert_unpin::<SendAdmission>();
};

#[test]
fn gate_is_clone() {
    fn assert_clone<T: Clone>() {}
    assert_clone::<AdmissionGate<u32>>();
}

#[tokio::test]
async fn capacity_one_preserves_a_b_c_order_with_b_unpolled() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    let b = pending(gate.send("B"));
    let c = pending(gate.send("C"));

    assert_eq!(b.state(), AdmissionState::Pending);
    assert_eq!(c.state(), AdmissionState::Pending);

    // `b` is never polled. Delivery must not depend on it.
    assert_eq!(recv(&rx).await, "A");
    assert_eq!(recv(&rx).await, "B");
    assert_eq!(recv(&rx).await, "C");

    // C's admission resolving implies B's frame was enqueued first.
    timeout(LIMIT, c).await.unwrap().unwrap();
    assert_eq!(b.state(), AdmissionState::Admitted);
}

#[tokio::test]
async fn cancel_frees_the_slot_and_keeps_successor_order() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    let b = pending(gate.send("B"));
    let c = pending(gate.send("C"));

    // NOTE: there is deliberately no `.await` between `send("B")` and this
    // cancel, so the driver has never been polled and B is still queued
    // with its frame. That is the exact-removal regime; cancelling a frame
    // the driver already handed to the channel is best-effort. Adding a
    // yield here would make this test racy.
    b.cancel();
    assert_eq!(gate.queued_len(), 1, "only C should remain queued");

    assert_eq!(recv(&rx).await, "A");
    assert_eq!(recv(&rx).await, "C");
    timeout(LIMIT, c).await.unwrap().unwrap();
    assert!(rx.try_recv().is_err(), "B must never be delivered");
}

/// The sibling of `cancel_frees_the_slot_and_keeps_successor_order`, for the
/// other regime: the frame is already parked in `send_async`, so the driver
/// — not `cancel` — has to abort it and drop it.
#[tokio::test]
async fn cancel_aborts_a_frame_the_driver_already_checked_out() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    let b = pending(gate.send("B"));
    let c = pending(gate.send("C"));

    // The channel is full, so B's `send_async` cannot complete until the
    // receiver takes A. That is what makes this deterministic: the abort
    // below is never racing a handoff.
    wait_until("B to be checked out", || gate.head_checked_out()).await;
    b.cancel();
    wait_until("the driver to drop B", || gate.queued_len() == 1).await;

    assert_eq!(recv(&rx).await, "A");
    assert_eq!(recv(&rx).await, "C");
    timeout(LIMIT, c).await.unwrap().unwrap();
    assert!(rx.try_recv().is_err(), "B must never be delivered");
}

/// Epoch death while the driver holds a frame: the checked-out frame stays
/// at the head so successors cannot overtake it, and the driver resolves it.
#[tokio::test]
async fn fail_all_aborts_a_frame_the_driver_already_checked_out() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    let mut b = pending(gate.send("B"));
    let mut c = pending(gate.send("C"));
    wait_until("B to be checked out", || gate.head_checked_out()).await;

    // B is parked in `send_async` and is failed by the driver; C is still
    // queued and is failed synchronously.
    gate.fail_all(AdmissionError::Failed("writer died".into()));

    let expected = Err(AdmissionError::Failed("writer died".into()));
    assert_eq!(timeout(LIMIT, &mut b).await.unwrap(), expected);
    assert_eq!(timeout(LIMIT, &mut c).await.unwrap(), expected);
    assert_eq!(b.state(), AdmissionState::Failed);
    assert_eq!(c.state(), AdmissionState::Failed);

    assert_eq!(recv(&rx).await, "A");
    assert!(rx.try_recv().is_err(), "dead-epoch frames must be dropped");

    // The successor epoch is unaffected by the old one's failure.
    admitted(gate.send("D"));
    assert_eq!(recv(&rx).await, "D");
}

#[tokio::test]
async fn unpolled_admissions_still_deliver() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    // Pure fire-and-forget: every outcome is dropped on the spot.
    for frame in ["A", "B", "C", "D", "E"] {
        drop(gate.send(frame));
    }

    let mut got = Vec::new();
    for _ in 0..5 {
        got.push(recv(&rx).await);
    }
    assert_eq!(got, ["A", "B", "C", "D", "E"]);
    // The last frame's queue entry is popped after its send resolves, so the
    // receiver can observe it a beat before the counter drops.
    wait_until("the gate to drain", || gate.queued_len() == 0).await;
}

#[tokio::test]
async fn different_gates_are_independent() {
    let (tx_a, rx_a) = flume::bounded(1);
    let (tx_b, rx_b) = flume::bounded(1);
    let gate_a = AdmissionGate::new(tx_a, Handle::current());
    let gate_b = AdmissionGate::new(tx_b, Handle::current());

    admitted(gate_a.send("a1"));
    let _blocked = pending(gate_a.send("a2"));
    assert_eq!(gate_a.queued_len(), 1);

    // Gate B is untouched by gate A's backlog.
    admitted(gate_b.send("b1"));
    assert_eq!(recv(&rx_b).await, "b1");
    admitted(gate_b.send("b2"));
    assert_eq!(gate_b.queued_len(), 0);

    // ...and gate A is still exactly where we left it.
    assert_eq!(gate_a.queued_len(), 1);
    assert_eq!(recv(&rx_a).await, "a1");
}

#[tokio::test]
async fn fail_all_resolves_outstanding_admissions_err() {
    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    let mut b = pending(gate.send("B"));
    let mut c = pending(gate.send("C"));

    gate.fail_all(AdmissionError::ConnectionReplaced);

    let b_result = timeout(LIMIT, &mut b).await.unwrap();
    let c_result = timeout(LIMIT, &mut c).await.unwrap();
    assert_eq!(b_result, Err(AdmissionError::ConnectionReplaced));
    assert_eq!(c_result, Err(AdmissionError::ConnectionReplaced));
    assert_eq!(b.state(), AdmissionState::Failed);
    assert_eq!(c.state(), AdmissionState::Failed);

    // Only the frame that was admitted before the failure is in the channel.
    assert_eq!(recv(&rx).await, "A");
    assert!(rx.try_recv().is_err(), "failed frames must be dropped");

    // The gate survives its epoch: a successor send admits again.
    admitted(gate.send("D"));
    assert_eq!(recv(&rx).await, "D");
}

#[tokio::test]
async fn admitted_fast_path_allocates_no_ticket() {
    let (tx, rx) = flume::bounded(4);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send("A"));
    assert_eq!(gate.queued_len(), 0, "the fast path must not take a ticket");
    assert!(!gate.driver_live(), "the fast path must not spawn a driver");

    assert_eq!(recv(&rx).await, "A");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_keep_their_own_order() {
    const TASKS: usize = 8;
    const SENDS: usize = 50;

    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    let collector = tokio::spawn(async move {
        let mut got = Vec::with_capacity(TASKS * SENDS);
        while got.len() < TASKS * SENDS {
            match rx.recv_async().await {
                Ok(frame) => got.push(frame),
                Err(_) => break,
            }
        }
        got
    });

    let mut senders = Vec::with_capacity(TASKS);
    for task in 0..TASKS {
        let gate = gate.clone();
        senders.push(tokio::spawn(async move {
            for seq in 0..SENDS {
                // Fire-and-forget; nothing is ever polled.
                drop(gate.send((task, seq)));
                tokio::task::yield_now().await;
            }
        }));
    }
    for sender in senders {
        timeout(LIMIT, sender).await.unwrap().unwrap();
    }

    let got: Vec<(usize, usize)> = timeout(LIMIT, collector).await.unwrap().unwrap();
    assert_eq!(got.len(), TASKS * SENDS, "every frame must be delivered");

    // The gate's guarantee is global FIFO by `send()` call order, which
    // implies each task observes its own frames in its own issue order.
    let mut next = [0usize; TASKS];
    for (task, seq) in got {
        assert_eq!(seq, next[task], "task {task} frames arrived out of order");
        next[task] += 1;
    }
}

/// Hooks are additive: the runtime's bookkeeping hook and a caller's observer
/// must both run, in registration order. A destructive last-wins slot here
/// silently disabled the backend's outbound metric and error reporting the
/// moment a caller attached its own observer.
#[tokio::test]
async fn hooks_are_additive_and_run_in_registration_order() {
    use std::sync::Mutex;

    let (tx, rx) = flume::bounded(1);
    let gate = AdmissionGate::new(tx, Handle::current());

    admitted(gate.send(0u8));
    let fired: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    let first = Arc::clone(&fired);
    let second = Arc::clone(&fired);
    let admission = pending(gate.send(1))
        .on_resolved(move |result| {
            assert!(result.is_ok());
            first.lock().unwrap().push("backend");
        })
        .on_resolved(move |result| {
            assert!(result.is_ok());
            second.lock().unwrap().push("caller");
        });

    assert_eq!(recv(&rx).await, 0);
    timeout(LIMIT, admission)
        .await
        .expect("admission timed out")
        .expect("admission should succeed");
    assert_eq!(recv(&rx).await, 1);

    wait_until("both hooks to fire", || fired.lock().unwrap().len() == 2).await;
    assert_eq!(*fired.lock().unwrap(), vec!["backend", "caller"]);

    // A hook added after resolution still runs, immediately, without
    // disturbing the ones that already fired.
    let late = Arc::clone(&fired);
    admitted(gate.send(2));
    let resolved = pending(gate.send(3));
    recv(&rx).await;
    recv(&rx).await;
    wait_until("ticket to resolve", || {
        resolved.state() == AdmissionState::Admitted
    })
    .await;
    drop(resolved.on_resolved(move |result| {
        assert!(result.is_ok());
        late.lock().unwrap().push("late");
    }));
    assert_eq!(*fired.lock().unwrap(), vec!["backend", "caller", "late"]);
}
