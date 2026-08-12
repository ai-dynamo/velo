// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Admission semantics for `Transport::send_message`.
//!
//! These tests use a purpose-built [`SlowSendTransport`] whose writer task can
//! be paused, which is what makes saturation deterministic — real transports
//! (tcp/zmq/etc.) drain so fast that a full channel is almost never observable
//! on demand. What is being pinned down here is the behaviour a saturated
//! channel produces: frames queue in issue order, a queued frame is delivered
//! whether or not anyone polls its admission, and only an explicit `cancel`
//! takes one back.

use bytes::Bytes;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::OnceCell;

use velo::observability::VeloMetrics;
use velo::transports::{
    AdmissionGate, HealthCheckError, MessageType, SendAdmission, SendOutcome, Transport,
    TransportAdapter, TransportError, TransportErrorHandler,
};
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress};

// ── SlowSendTransport ─────────────────────────────────────────────────────

/// Test transport whose writer task sleeps `frame_delay` between consumed
/// frames and can be paused outright, letting callers saturate the bounded
/// send channel on demand.
///
/// The transport does not actually move bytes anywhere — it simulates a slow
/// wire by holding each frame for `frame_delay` before recording its header
/// and discarding it. Use `consumed()` to observe drain progress and order.
struct SlowSendTransport {
    key: TransportKey,
    tx: flume::Sender<SendTask>,
    rx: Mutex<Option<flume::Receiver<SendTask>>>,
    /// Built in `start`, where the runtime handle the gate's driver needs
    /// first becomes available.
    gate: std::sync::OnceLock<AdmissionGate<SendTask>>,
    frame_delay: Duration,
    consumed: Arc<Mutex<Vec<Bytes>>>,
    started: Arc<AtomicBool>,
    /// When true, the writer task refuses to pull new frames. Lets tests hold
    /// the channel at its capacity so admissions stay queued deterministically.
    paused: Arc<AtomicBool>,
    /// Lazily populated via `Transport::set_observability`. Lets the counter
    /// test observe the transport send-backpressure counter.
    metrics: OnceCell<std::sync::Arc<dyn velo_ext::TransportObservability>>,
}

struct SendTask {
    header: Bytes,
    _payload: Bytes,
    _on_error: Arc<dyn TransportErrorHandler>,
}

impl SlowSendTransport {
    fn new(capacity: usize, frame_delay: Duration) -> Arc<Self> {
        let (tx, rx) = flume::bounded(capacity);
        Arc::new(Self {
            key: TransportKey::from("slow"),
            tx,
            rx: Mutex::new(Some(rx)),
            gate: std::sync::OnceLock::new(),
            frame_delay,
            consumed: Arc::new(Mutex::new(Vec::new())),
            started: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            metrics: OnceCell::new(),
        })
    }

    fn consumed(&self) -> Vec<Bytes> {
        self.consumed.lock().clone()
    }

    fn consumed_count(&self) -> usize {
        self.consumed.lock().len()
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    /// Send one frame with `header` as its identity.
    fn send(&self, header: &'static [u8], on_error: Arc<dyn TransportErrorHandler>) -> SendOutcome {
        self.send_message(
            InstanceId::new_v4(),
            Bytes::from_static(header),
            Bytes::from_static(b"p"),
            MessageType::Message,
            on_error,
        )
    }
}

impl Transport for SlowSendTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        let mut entries = std::collections::HashMap::<String, Vec<u8>>::new();
        entries.insert(self.key.to_string(), b"slow://local".to_vec());
        WorkerAddress::from_encoded(rmp_serde::to_vec(&entries).unwrap())
    }

    fn register(&self, _peer_info: PeerInfo) -> Result<(), TransportError> {
        Ok(())
    }

    fn send_message(
        &self,
        _instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        _message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        let task = SendTask {
            header,
            _payload: payload,
            _on_error: on_error,
        };
        let Some(gate) = self.gate.get() else {
            return SendOutcome::Admitted;
        };
        let outcome = gate.send(task);
        if let Some(m) = self.metrics.get()
            && !outcome.is_admitted()
        {
            m.record_send_backpressure();
        }
        outcome
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        _channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.started.store(true, Ordering::Release);
        let _ = self
            .gate
            .set(AdmissionGate::new(self.tx.clone(), rt.clone()));
        let rx = self.rx.lock().take().expect("start called twice");
        let consumed = self.consumed.clone();
        let paused = self.paused.clone();
        let delay = self.frame_delay;
        rt.spawn(async move {
            loop {
                // Check pause *before* attempting to pull from the channel —
                // this is how tests keep the channel saturated deterministically.
                while paused.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                match rx.try_recv() {
                    Ok(task) => {
                        tokio::time::sleep(delay).await;
                        consumed.lock().push(task.header);
                    }
                    Err(flume::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(flume::TryRecvError::Disconnected) => break,
                }
            }
        });
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&self) {}

    fn set_observability(
        &self,
        observability: std::sync::Arc<dyn velo_ext::TransportObservability>,
    ) {
        let _ = self.metrics.set(observability);
    }

    fn check_health(
        &self,
        _instance_id: InstanceId,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    > {
        Box::pin(async { Ok(()) })
    }
}

// ── Test error handler ────────────────────────────────────────────────────

struct CountingHandler {
    count: AtomicUsize,
}

impl CountingHandler {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
        })
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

impl TransportErrorHandler for CountingHandler {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
        self.count.fetch_add(1, Ordering::Release);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────

/// Upper bound on any test-setup await — individual tests supply their own
/// timeouts on the interesting waits.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Helper: build a started SlowSendTransport on the current runtime.
async fn make_started(capacity: usize, frame_delay: Duration) -> Arc<SlowSendTransport> {
    let t = SlowSendTransport::new(capacity, frame_delay);
    let (adapter, _streams) = velo::transports::make_channels();
    tokio::time::timeout(
        TEST_TIMEOUT,
        t.start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        ),
    )
    .await
    .expect("SlowSendTransport::start timed out")
    .expect("SlowSendTransport::start returned Err");
    t
}

/// Unwrap an outcome that must have queued.
fn queued(outcome: SendOutcome) -> SendAdmission {
    match outcome {
        SendOutcome::Pending(admission) => admission,
        SendOutcome::Admitted => panic!("expected a queued frame, got Admitted"),
    }
}

/// Wait until `condition` holds, failing the test rather than hanging.
async fn wait_until(label: &str, mut condition: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "timed out waiting for {label}");
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturated_channel_queues_the_frame() {
    // Capacity 2, writer paused so the channel stays full deterministically.
    let t = make_started(2, Duration::from_millis(1)).await;
    t.pause();
    let err = CountingHandler::new();

    for _ in 0..2 {
        assert!(
            t.send(b"h", err.clone()).is_admitted(),
            "the first two sends fit in the channel"
        );
    }

    // Third send: the channel is full, so the frame queues in the gate.
    let admission = queued(t.send(b"h", err.clone()));

    // Resume the writer so the queued frame can be enqueued.
    t.resume();
    tokio::time::timeout(Duration::from_secs(5), admission)
        .await
        .expect("the admission should resolve once the writer drains")
        .expect("the frame should be admitted, not failed");

    assert_eq!(err.count(), 0, "admission should not trigger on_error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_concurrent_sends_all_resolve() {
    // Small capacity + fast drain. Many concurrent callers that all await
    // their admission must all complete.
    let t = make_started(4, Duration::from_millis(5)).await;
    let err = CountingHandler::new();
    let n = 64usize;

    let mut tasks = Vec::with_capacity(n);
    for _ in 0..n {
        let t = t.clone();
        let err = err.clone();
        tasks.push(tokio::spawn(async move {
            if let SendOutcome::Pending(admission) = t.send(b"h", err) {
                admission.await.expect("admission should succeed");
            }
        }));
    }

    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("task should not deadlock")
            .expect("task should not panic");
    }

    wait_until("the writer to drain", || t.consumed_count() >= n).await;
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_an_admission_still_delivers() {
    // The headline inversion versus the old backpressure future: the frame
    // belongs to the gate, not to the handle, so dropping the handle without
    // polling it does not withdraw the send.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    assert!(t.send(b"first", err.clone()).is_admitted());
    drop(queued(t.send(b"second", err.clone())));

    t.resume();
    wait_until("both frames to drain", || t.consumed_count() >= 2).await;
    assert_eq!(
        t.consumed(),
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_timeout_around_an_admission_does_not_cancel_it() {
    // Wrapping an admission in `timeout` abandons the *wait*, not the frame.
    // Callers who really want to withdraw a frame must say so with `cancel`.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    assert!(t.send(b"first", err.clone()).is_admitted());
    let admission = queued(t.send(b"second", err.clone()));

    let res = tokio::time::timeout(Duration::from_millis(100), admission).await;
    assert!(res.is_err(), "timeout should fire (the writer is paused)");

    t.resume();
    wait_until("both frames to drain", || t.consumed_count() >= 2).await;
    assert_eq!(
        t.consumed(),
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_an_admission_withdraws_the_frame() {
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    assert!(t.send(b"first", err.clone()).is_admitted());
    // No await between the send and the cancel, so the gate's driver has never
    // run and the frame is still queued — the regime where cancellation is
    // exact rather than best-effort.
    queued(t.send(b"second", err.clone())).cancel();

    t.resume();
    wait_until("the first frame to drain", || t.consumed_count() >= 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        t.consumed(),
        vec![Bytes::from_static(b"first")],
        "a cancelled frame must never be delivered"
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_frames_keep_their_issue_order_unpolled() {
    // The guarantee the gate exists for, at the `Transport` seam: nothing here
    // is ever polled, and the frames still arrive in the order they were sent.
    let t = make_started(2, Duration::from_millis(1)).await;
    t.pause();
    let err = CountingHandler::new();

    let order: [&'static [u8]; 5] = [b"a", b"b", b"c", b"d", b"e"];
    for header in order {
        drop(t.send(header, err.clone()));
    }

    t.resume();
    wait_until("every frame to drain", || t.consumed_count() >= order.len()).await;
    assert_eq!(
        t.consumed(),
        order.map(Bytes::from_static).to_vec(),
        "frames must arrive in the order their sends returned"
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_sends_increment_the_backpressure_counter() {
    use velo::observability::test_helpers::MetricSnapshot;

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));

    // Capacity 1, writer paused so every send past the first has to queue.
    let t = make_started(1, Duration::from_millis(1)).await;
    t.set_observability(std::sync::Arc::new(metrics.bind_transport("slow"))
        as std::sync::Arc<dyn velo_ext::TransportObservability>);
    t.pause();
    let err = CountingHandler::new();

    assert!(t.send(b"h", err.clone()).is_admitted());

    let queued_sends = 5;
    let mut admissions = Vec::with_capacity(queued_sends);
    for _ in 0..queued_sends {
        admissions.push(queued(t.send(b"h", err.clone())));
    }

    let snapshot = MetricSnapshot::from_registry(&registry);
    let value = snapshot.counter(
        "velo_transport_send_backpressure_total",
        &[("transport", "slow")],
    );
    assert_eq!(
        value, queued_sends as f64,
        "the counter should fire once per queued send"
    );

    // Withdraw the queued frames so the paused writer is not left holding them.
    for admission in admissions {
        admission.cancel();
    }
    assert_eq!(err.count(), 0);
}
