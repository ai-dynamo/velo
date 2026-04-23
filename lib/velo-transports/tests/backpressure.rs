// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Backpressure semantics for `Transport::send_message`.
//!
//! These tests use a purpose-built [`SlowSendTransport`] whose writer task
//! sleeps between frames. That makes it trivial to saturate the per-peer
//! bounded flume channel and exercise the `SendBackpressure` return path —
//! real transports (tcp/zmq/etc.) drain so quickly that the Full branch is
//! almost never observable in practice.

use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use velo_common::{InstanceId, PeerInfo, TransportKey, WorkerAddress};
use velo_transports::{
    HealthCheckError, MessageType, SendBackpressure, Transport, TransportAdapter, TransportError,
    TransportErrorHandler,
};

// ── SlowSendTransport ─────────────────────────────────────────────────────

/// Test transport whose writer task sleeps `frame_delay` between consumed
/// frames, letting callers saturate the bounded send channel on demand.
///
/// The transport does not actually move bytes anywhere — it simulates a
/// slow wire by holding each frame for `frame_delay` before discarding it.
/// Use `consumed_count()` to observe drain progress.
struct SlowSendTransport {
    key: TransportKey,
    tx: flume::Sender<SendTask>,
    rx: parking_lot::Mutex<Option<flume::Receiver<SendTask>>>,
    frame_delay: Duration,
    consumed: Arc<AtomicU64>,
    started: Arc<AtomicBool>,
    /// When true, the writer task refuses to pull new frames. Lets tests
    /// hold the channel at its capacity so `SendBackpressure` futures stay
    /// pending deterministically.
    paused: Arc<AtomicBool>,
}

struct SendTask {
    _header: Bytes,
    _payload: Bytes,
    _on_error: Arc<dyn TransportErrorHandler>,
}

impl SlowSendTransport {
    fn new(capacity: usize, frame_delay: Duration) -> Arc<Self> {
        let (tx, rx) = flume::bounded(capacity);
        Arc::new(Self {
            key: TransportKey::from("slow"),
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            frame_delay,
            consumed: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
        })
    }

    fn consumed_count(&self) -> u64 {
        self.consumed.load(Ordering::Acquire)
    }

    fn pause(&self) {
        self.paused.store(true, Ordering::Release);
    }

    fn resume(&self) {
        self.paused.store(false, Ordering::Release);
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
    ) -> Result<(), SendBackpressure> {
        let task = SendTask {
            _header: header,
            _payload: payload,
            _on_error: on_error,
        };
        match self.tx.try_send(task) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(task)) => {
                let tx = self.tx.clone();
                Err(SendBackpressure::new(Box::pin(async move {
                    let _ = tx.send_async(task).await;
                })))
            }
            Err(flume::TrySendError::Disconnected(_)) => Ok(()),
        }
    }

    fn start(
        &self,
        _instance_id: InstanceId,
        _channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.started.store(true, Ordering::Release);
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
                    Ok(_task) => {
                        tokio::time::sleep(delay).await;
                        consumed.fetch_add(1, Ordering::Release);
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

// ── Tests ─────────────────────────────────────────────────────────────────

/// Helper: build a started SlowSendTransport on the current runtime.
async fn make_started(capacity: usize, frame_delay: Duration) -> Arc<SlowSendTransport> {
    let t = SlowSendTransport::new(capacity, frame_delay);
    let (adapter, _streams) = velo_transports::make_channels();
    t.start(
        InstanceId::new_v4(),
        adapter,
        tokio::runtime::Handle::current(),
    )
    .await
    .unwrap();
    t
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn saturating_channel_returns_backpressure() {
    // Capacity 2, writer paused so the channel stays full deterministically.
    let t = make_started(2, Duration::from_millis(1)).await;
    t.pause();
    let err = CountingHandler::new();

    // Two fast-path sends fill the channel.
    for _ in 0..2 {
        let r = t.send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            MessageType::Message,
            err.clone(),
        );
        assert!(r.is_ok(), "first two sends should enqueue synchronously");
    }

    // Third send: channel is full.
    let r = t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        err.clone(),
    );
    let bp = r.expect_err("third send should return Backpressure");

    // Resume the writer so the bp future can make progress.
    t.resume();
    tokio::time::timeout(Duration::from_secs(2), bp)
        .await
        .expect("bp future should resolve after writer drains");

    assert_eq!(err.count(), 0, "bp resolution should not trigger on_error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_concurrent_sends_all_resolve() {
    // Small capacity + fast drain. Many concurrent callers that all await
    // their bp must all complete.
    let t = make_started(4, Duration::from_millis(5)).await;
    let err = CountingHandler::new();
    let n = 64usize;

    let mut tasks = Vec::with_capacity(n);
    for _ in 0..n {
        let t = t.clone();
        let err = err.clone();
        tasks.push(tokio::spawn(async move {
            match t.send_message(
                InstanceId::new_v4(),
                Bytes::from_static(b"h"),
                Bytes::from_static(b"p"),
                MessageType::Message,
                err,
            ) {
                Ok(()) => {}
                Err(bp) => bp.await,
            }
        }));
    }

    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("task should not deadlock")
            .expect("task should not panic");
    }

    // Wait for the writer to drain everything.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while t.consumed_count() < n as u64 {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "writer did not drain: {} / {} consumed",
                t.consumed_count(),
                n
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_backpressure_does_not_enqueue() {
    // If a caller drops the SendBackpressure future before awaiting it, the
    // queued flume::send_async future is dropped too — the message should
    // *not* land in the channel.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    // Fill the channel.
    t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"a"),
        MessageType::Message,
        err.clone(),
    )
    .expect("first send enqueues");

    // Saturating send returns a bp — drop it immediately.
    let bp = t
        .send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"b"),
            MessageType::Message,
            err.clone(),
        )
        .expect_err("second send is backpressured");
    drop(bp);

    // Resume and wait long enough for the first frame to drain. Only the
    // first message should have been consumed; the dropped second bp must
    // not enqueue.
    t.resume();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while t.consumed_count() < 1 {
        if tokio::time::Instant::now() > deadline {
            panic!("writer did not drain the first frame");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give any spuriously-enqueued second frame time to drain too.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        t.consumed_count(),
        1,
        "dropped backpressure future must not enqueue"
    );
    assert_eq!(err.count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_wrapping_backpressured_send_cancels_cleanly() {
    // Wrapping the bp future in tokio::time::timeout cancels it cleanly when
    // the timeout fires — no enqueue, no error callback.
    let t = make_started(1, Duration::from_millis(5)).await;
    t.pause();
    let err = CountingHandler::new();

    t.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"a"),
        Bytes::from_static(b"a"),
        MessageType::Message,
        err.clone(),
    )
    .expect("first send enqueues");

    let bp = t
        .send_message(
            InstanceId::new_v4(),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"b"),
            MessageType::Message,
            err.clone(),
        )
        .expect_err("second send is backpressured");

    let res = tokio::time::timeout(Duration::from_millis(100), bp).await;
    assert!(res.is_err(), "timeout should fire (writer is paused)");
    assert_eq!(err.count(), 0);
}
