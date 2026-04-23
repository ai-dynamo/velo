// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::{InstanceId, PeerInfo, TransportKey, WorkerAddress};

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use velo_observability::VeloMetrics;

/// Errors returned by individual [`Transport`] implementations.
#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    /// The peer's [`WorkerAddress`] does not contain an entry for this transport.
    #[error("No endpoint found for transport")]
    NoEndpoint,

    /// The endpoint string could not be parsed (malformed URL, invalid address).
    #[error("Invalid endpoint format")]
    InvalidEndpoint,

    /// The target peer was never registered with this transport.
    #[error("Peer not registered: {0}")]
    PeerNotRegistered(InstanceId),

    /// The transport has not been started yet (no runtime handle).
    #[error("Transport not started")]
    NotStarted,

    /// No responders available for the peer (e.g. NATS request with no subscriber).
    #[error("No responders for peer")]
    NoResponders,
}

/// Error type specific to health check operations
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum HealthCheckError {
    /// The peer was never registered with this transport.
    #[error("Peer not registered with transport")]
    PeerNotRegistered,

    /// The transport has not been started yet.
    #[error("Transport not started")]
    TransportNotStarted,

    /// The peer is registered but no connection has ever been established.
    #[error("Connection never established to peer")]
    NeverConnected,

    /// An existing connection is unhealthy or the peer is unreachable.
    #[error("Connection failed or peer unreachable")]
    ConnectionFailed,

    /// The health check exceeded the specified timeout.
    #[error("Health check timed out")]
    Timeout,
}

/// Shared shutdown coordinator for graceful multi-phase shutdown.
///
/// **Phases**:
/// 1. **Gate** — `begin_drain()` flips the draining flag; transports reject new inbound requests.
/// 2. **Drain** — `wait_for_drain()` blocks until all in-flight guards are dropped.
/// 3. **Teardown** — `teardown_token().cancel()` kills listeners and writer tasks.
///
/// Hot-path cost: a single `AtomicBool::load(Relaxed)` per frame to check `is_draining()`.
#[derive(Clone)]
pub struct ShutdownState {
    inner: Arc<ShutdownStateInner>,
}

struct ShutdownStateInner {
    draining: AtomicBool,
    in_flight: AtomicUsize,
    drain_complete: Notify,
    teardown_token: CancellationToken,
}

impl ShutdownState {
    /// Create a new shutdown state. Not draining, zero in-flight.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownStateInner {
                draining: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                drain_complete: Notify::new(),
                teardown_token: CancellationToken::new(),
            }),
        }
    }

    /// Returns `true` if drain has been initiated (Phase 1).
    ///
    /// Uses `Relaxed` ordering — safe for the hot-path gate check because
    /// the flag is monotonic (false → true, never reset).
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::Relaxed)
    }

    /// Begin Phase 1: flip the draining flag. Idempotent.
    pub fn begin_drain(&self) {
        self.inner.draining.store(true, Ordering::Release);
    }

    /// Acquire an in-flight guard. The guard increments the counter on creation
    /// and decrements it on drop. Use this to track requests that are being processed.
    ///
    /// Guards are still acquirable after `begin_drain()` — this is intentional
    /// so that already-accepted work can be tracked.
    pub fn acquire(&self) -> InFlightGuard {
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlightGuard {
            inner: self.inner.clone(),
        }
    }

    /// Current number of in-flight requests. Primarily for testing/debugging.
    pub fn in_flight_count(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    /// Wait until in-flight count reaches zero. Returns immediately if already zero.
    pub async fn wait_for_drain(&self) {
        loop {
            if self.inner.in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            self.inner.drain_complete.notified().await;
        }
    }

    /// Get the Phase 3 teardown token. Cancel this to kill listeners/writers.
    pub fn teardown_token(&self) -> &CancellationToken {
        &self.inner.teardown_token
    }
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that decrements the in-flight counter on drop.
pub struct InFlightGuard {
    inner: Arc<ShutdownStateInner>,
}

impl InFlightGuard {
    /// Explicitly complete this guard (equivalent to dropping it).
    pub fn complete(self) {
        // Drop impl handles the decrement
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let prev = self.inner.in_flight.fetch_sub(1, Ordering::AcqRel);
        // If we just decremented to 0, notify waiters
        if prev == 1 {
            self.inner.drain_complete.notify_waiters();
        }
    }
}

/// Policy for how long to wait during the drain phase.
#[derive(Debug, Clone)]
pub enum ShutdownPolicy {
    /// Wait indefinitely for all in-flight requests to complete.
    WaitForever,
    /// Wait up to the given duration, then force teardown.
    Timeout(Duration),
}

/// Signal returned by [`Transport::send_message`] when the per-peer send channel
/// was saturated at call time.
///
/// Semantics:
/// - Caller must `.await` this to drive the deferred enqueue to completion.
/// - Output is `()` — failures during the deferred send are reported via the
///   [`TransportErrorHandler::on_error`] callback supplied to `send_message`,
///   not via the future's return value (preserves fire-and-forget-with-callback
///   semantics).
/// - Dropping the future before it resolves cancels the pending send cleanly
///   (the underlying `flume::send_async` future is drop-safe; the message is
///   not enqueued). **`on_error` is NOT invoked on drop** — callers that need
///   to observe dropped frames must track cancellation themselves. Fire-path
///   helpers in `velo-messenger` (e.g. `send_ack`, `send_nack`,
///   `send_response_*`) always `.await` the future so this case does not arise
///   in normal request/response flows.
///
/// ### Awaiter completion on deferred-send failure
///
/// If a deferred send fails *after* the frame was accepted by the transport
/// (channel closes between hand-off and wire, peer disconnects mid-write),
/// the transport invokes its `TransportErrorHandler::on_error(header, payload,
/// reason)` callback. The default messenger handler decodes the request
/// header's response id and completes the corresponding awaiter on
/// `ResponseManager` with the error, so sync/unary callers unblock
/// immediately rather than waiting for their own timeout to fire.
///
/// Custom `TransportErrorHandler` implementations are responsible for
/// completing any awaiter they care about; the trait itself does not enforce
/// this contract.
///
/// Reordering: concurrent callers where one hits `Backpressured` and another
/// fast-paths through `try_send` may land out of order at the remote. Callers
/// that require strict FIFO must serialize their sends.
pub struct SendBackpressure {
    fut: BoxFuture<'static, ()>,
}

impl SendBackpressure {
    /// Wrap a boxed future that drives the deferred send to completion.
    pub fn new(fut: BoxFuture<'static, ()>) -> Self {
        Self { fut }
    }
}

impl Future for SendBackpressure {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.fut.as_mut().poll(cx)
    }
}

impl std::fmt::Debug for SendBackpressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendBackpressure").finish_non_exhaustive()
    }
}

/// Attempt a non-blocking enqueue on a bounded flume channel, converting the
/// `Full` variant into a `SendBackpressure` future and the `Disconnected`
/// variant into a reported error.
///
/// This collapses the identical pattern every `Transport` impl used to write
/// inline: `try_send` → on `Full` wrap `send_async` in a bp future, on
/// `Disconnected` call `on_disconnected(task)` and return `Ok(())`.
///
/// The two callbacks differ only by the message string each transport wants
/// to surface:
/// - `on_disconnected(task)` is called synchronously when `try_send` returns
///   `Disconnected`.
/// - `on_closed_during_bp(task)` is called from inside the bp future if
///   `send_async` fails (channel closed while we were waiting for space).
#[inline]
pub fn try_send_or_backpressure<T, FDisc, FClosed>(
    tx: &flume::Sender<T>,
    task: T,
    on_disconnected: FDisc,
    on_closed_during_bp: FClosed,
) -> Result<(), SendBackpressure>
where
    T: Send + 'static,
    FDisc: FnOnce(T),
    FClosed: FnOnce(T) + Send + 'static,
{
    match tx.try_send(task) {
        Ok(()) => Ok(()),
        Err(flume::TrySendError::Full(task)) => {
            let tx = tx.clone();
            Err(SendBackpressure::new(Box::pin(async move {
                if let Err(flume::SendError(task)) = tx.send_async(task).await {
                    on_closed_during_bp(task);
                }
            })))
        }
        Err(flume::TrySendError::Disconnected(task)) => {
            on_disconnected(task);
            Ok(())
        }
    }
}

/// Outcome of a send through [`VeloBackend::send_message`](crate::VeloBackend::send_message).
///
/// The outer `Result` on `send_message` captures routing errors (peer not
/// registered, no compatible transport). This enum captures the success case's
/// enqueue status:
///
/// - [`SendOutcome::Enqueued`] — the frame was enqueued synchronously.
/// - [`SendOutcome::Backpressured`] — the per-peer send channel was full;
///   caller must `.await` the contained future to complete the send.
#[derive(Debug)]
pub enum SendOutcome {
    /// The frame was enqueued synchronously on the per-peer send channel.
    Enqueued,
    /// The per-peer send channel was saturated. Callers must `.await` the
    /// contained future to drive the deferred enqueue to completion.
    Backpressured(SendBackpressure),
}

/// Abstraction over a single message transport (TCP, HTTP, NATS, gRPC, UCX).
///
/// Implementations handle peer registration, message sending, listener lifecycle,
/// health checking, and graceful shutdown. The trait is object-safe so transports
/// can be stored as `Arc<dyn Transport>`.
pub trait Transport: Send + Sync {
    /// Unique key identifying this transport (e.g. `"tcp"`, `"grpc"`).
    fn key(&self) -> TransportKey;
    /// The [`WorkerAddress`] fragment advertised by this transport.
    fn address(&self) -> WorkerAddress;
    /// Register a remote peer, extracting its endpoint from [`PeerInfo`].
    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError>;

    /// Send an active message to the remote instance.
    ///
    /// - `Ok(())` — the frame was enqueued synchronously on the per-peer send
    ///   channel (fast path) *or* a hard error occurred and was reported via
    ///   `on_error` (cold-start failure, transport not started, etc).
    /// - `Err(SendBackpressure)` — the per-peer channel was full at call time;
    ///   the caller must `.await` the returned future to complete enqueue.
    ///
    /// The return type signals *backpressure*, not failure. All delivery
    /// failures continue to flow through `on_error`.
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure>;

    /// Start the transport (bind listener, spawn tasks) for the given instance.
    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Tear down the transport, cancelling all tasks and closing connections.
    fn shutdown(&self);

    /// Install shared observability state for the transport.
    ///
    /// Default implementation is a no-op for transports that do not emit
    /// transport-local metrics or tracing metadata.
    fn set_observability(&self, _observability: Arc<VeloMetrics>) {}

    /// Begin draining: reject new inbound requests while allowing responses.
    ///
    /// Default implementation is a no-op. Transports that need per-frame
    /// gating (e.g., unsubscribing from NATS subjects) should override this.
    fn begin_drain(&self) {}

    /// Check if a registered peer is reachable and healthy
    ///
    /// Returns Ok(()) if peer responds to health check within timeout.
    /// Different transports implement this differently:
    /// - NATS: request/reply to health subject
    /// - TCP: check existing connection or attempt new connection
    /// - HTTP: HEAD request to health endpoint
    /// - UCX: endpoint status check
    ///
    /// # Errors
    /// - `PeerNotRegistered`: Peer was never registered with this transport
    /// - `TransportNotStarted`: Transport hasn't been started yet
    /// - `NeverConnected`: Peer is registered but no connection has been established
    /// - `ConnectionFailed`: Connection exists/existed but is currently unhealthy or unreachable
    /// - `Timeout`: Health check took longer than the specified timeout
    fn check_health(
        &self,
        instance_id: InstanceId,
        timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), HealthCheckError>> + Send + '_>,
    >;
}

/// Callback trait invoked when a transport fails to deliver a message.
///
/// The original `header` and `payload` are returned so higher layers can
/// retry or log the failure.
pub trait TransportErrorHandler: Send + Sync {
    /// Called when message delivery fails. Receives the original data and error description.
    fn on_error(&self, header: Bytes, payload: Bytes, error: String);
}

/// Message type discriminator for routing frames to appropriate streams
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    #[allow(missing_docs)]
    Message = 0,
    #[allow(missing_docs)]
    Response = 1,
    #[allow(missing_docs)]
    Ack = 2,
    #[allow(missing_docs)]
    Event = 3,
    /// Sent back to a peer when we are draining and cannot accept new messages.
    /// The original request header is echoed back for correlation.
    ShuttingDown = 4,
}

impl MessageType {
    /// Try to convert a u8 to a MessageType
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(MessageType::Message),
            1 => Some(MessageType::Response),
            2 => Some(MessageType::Ack),
            3 => Some(MessageType::Event),
            4 => Some(MessageType::ShuttingDown),
            _ => None,
        }
    }

    /// Convert MessageType to u8
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Sender-side handle given to transports for routing inbound frames.
///
/// Each transport receives a clone of this adapter during [`Transport::start`]
/// and uses it to forward decoded `(header, payload)` pairs to the appropriate
/// stream based on [`MessageType`].
#[derive(Clone)]
pub struct TransportAdapter {
    /// Channel for inbound [`MessageType::Message`] frames.
    pub message_stream: flume::Sender<(Bytes, Bytes)>,
    /// Channel for inbound [`MessageType::Response`] and [`MessageType::ShuttingDown`] frames.
    pub response_stream: flume::Sender<(Bytes, Bytes)>,
    /// Channel for inbound [`MessageType::Ack`] and [`MessageType::Event`] frames.
    pub event_stream: flume::Sender<(Bytes, Bytes)>,
    /// Shared shutdown coordinator for drain-aware routing.
    pub shutdown_state: ShutdownState,
}

/// Receiver-side handle for consuming inbound frames from all transports.
///
/// Returned by [`make_channels`] alongside the corresponding [`TransportAdapter`].
/// Higher layers pull `(header, payload)` pairs from these channels.
pub struct DataStreams {
    /// Receiver for inbound message frames.
    pub message_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Receiver for inbound response and shutting-down frames.
    pub response_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Receiver for inbound ack and event frames.
    pub event_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Shared shutdown coordinator.
    pub shutdown_state: ShutdownState,
}

type DataStreamTuple = (
    flume::Receiver<(Bytes, Bytes)>,
    flume::Receiver<(Bytes, Bytes)>,
    flume::Receiver<(Bytes, Bytes)>,
);

impl DataStreams {
    /// Destructure into the three raw receivers `(message, response, event)`.
    pub fn into_parts(self) -> DataStreamTuple {
        (self.message_stream, self.response_stream, self.event_stream)
    }

    /// Receive a message with an in-flight guard for drain tracking.
    ///
    /// Returns `(header, payload, guard)`. The guard keeps the in-flight counter
    /// incremented until it is dropped or `complete()` is called.
    pub async fn recv_message_tracked(
        &self,
    ) -> Result<(Bytes, Bytes, InFlightGuard), flume::RecvError> {
        let (header, payload) = self.message_stream.recv_async().await?;
        let guard = self.shutdown_state.acquire();
        Ok((header, payload, guard))
    }
}

/// Create a matched pair of [`TransportAdapter`] (sender) and [`DataStreams`] (receiver).
///
/// Both sides share the same [`ShutdownState`] so drain coordination is automatic.
pub fn make_channels() -> (TransportAdapter, DataStreams) {
    let shutdown_state = ShutdownState::new();
    let (message_tx, message_rx) = flume::unbounded();
    let (response_tx, response_rx) = flume::unbounded();
    let (event_tx, event_rx) = flume::unbounded();
    (
        TransportAdapter {
            message_stream: message_tx,
            response_stream: response_tx,
            event_stream: event_tx,
            shutdown_state: shutdown_state.clone(),
        },
        DataStreams {
            message_stream: message_rx,
            response_stream: response_rx,
            event_stream: event_rx,
            shutdown_state,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout};

    #[test]
    fn test_shutdown_state_initial() {
        let state = ShutdownState::new();
        assert!(!state.is_draining());
        assert_eq!(state.in_flight_count(), 0);
    }

    #[test]
    fn test_begin_drain_flips_flag() {
        let state = ShutdownState::new();
        state.begin_drain();
        assert!(state.is_draining());
    }

    #[test]
    fn test_begin_drain_idempotent() {
        let state = ShutdownState::new();
        state.begin_drain();
        state.begin_drain();
        assert!(state.is_draining());
    }

    #[test]
    fn test_acquire_increments_inflight() {
        let state = ShutdownState::new();
        let _g1 = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
        let _g2 = state.acquire();
        assert_eq!(state.in_flight_count(), 2);
    }

    #[test]
    fn test_guard_drop_decrements_inflight() {
        let state = ShutdownState::new();
        let g = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
        drop(g);
        assert_eq!(state.in_flight_count(), 0);
    }

    #[test]
    fn test_guard_complete_decrements() {
        let state = ShutdownState::new();
        let g = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
        g.complete();
        assert_eq!(state.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_drain_immediate() {
        let state = ShutdownState::new();
        // Should complete immediately since in_flight is 0
        timeout(Duration::from_millis(100), state.wait_for_drain())
            .await
            .expect("wait_for_drain should complete immediately when in_flight is 0");
    }

    #[tokio::test]
    async fn test_wait_for_drain_blocks_then_completes() {
        let state = ShutdownState::new();
        let guard = state.acquire();

        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
            state_clone.wait_for_drain().await;
        });

        // Give the waiter time to park
        sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        // Drop guard → should unblock
        drop(guard);
        timeout(Duration::from_millis(100), handle)
            .await
            .expect("should complete after guard drop")
            .unwrap();
    }

    #[tokio::test]
    async fn test_multiple_guards_concurrent() {
        let state = ShutdownState::new();
        let guards: Vec<_> = (0..10).map(|_| state.acquire()).collect();
        assert_eq!(state.in_flight_count(), 10);

        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
            state_clone.wait_for_drain().await;
        });

        // Drop all guards
        drop(guards);

        timeout(Duration::from_millis(100), handle)
            .await
            .expect("should complete after all guards drop")
            .unwrap();
        assert_eq!(state.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_drain_with_zero_inflight() {
        let state = ShutdownState::new();
        state.begin_drain();
        // Should complete immediately
        timeout(Duration::from_millis(100), state.wait_for_drain())
            .await
            .expect("should complete immediately with zero in-flight");
    }

    #[test]
    fn test_acquire_works_after_drain() {
        let state = ShutdownState::new();
        state.begin_drain();
        let _g = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
    }

    #[test]
    fn test_guard_drop_during_panic() {
        let state = ShutdownState::new();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = state.acquire();
            assert_eq!(state.in_flight_count(), 1);
            panic!("intentional panic");
        }));

        assert!(result.is_err());
        // Guard's Drop should have fired even during unwind
        assert_eq!(state.in_flight_count(), 0);
    }

    #[test]
    fn test_shutting_down_from_u8() {
        assert_eq!(MessageType::from_u8(4), Some(MessageType::ShuttingDown));
    }

    #[test]
    fn test_shutting_down_as_u8() {
        assert_eq!(MessageType::ShuttingDown.as_u8(), 4);
    }

    #[test]
    fn test_unknown_message_type_still_none() {
        assert_eq!(MessageType::from_u8(5), None);
        assert_eq!(MessageType::from_u8(255), None);
    }

    #[test]
    fn test_make_channels_includes_shutdown_state() {
        let (adapter, streams) = make_channels();
        // Both sides should share the same ShutdownState (via Arc)
        assert!(!adapter.shutdown_state.is_draining());
        assert!(!streams.shutdown_state.is_draining());

        // Mutating one should be visible through the other
        adapter.shutdown_state.begin_drain();
        assert!(streams.shutdown_state.is_draining());
    }

    #[tokio::test]
    async fn test_recv_message_tracked_returns_guard() {
        let (adapter, streams) = make_channels();

        // Send a message through the adapter
        adapter
            .message_stream
            .send_async((
                bytes::Bytes::from_static(b"hdr"),
                bytes::Bytes::from_static(b"pay"),
            ))
            .await
            .unwrap();

        // Receive with tracking
        let (header, payload, guard) = streams.recv_message_tracked().await.unwrap();
        assert_eq!(&header[..], b"hdr");
        assert_eq!(&payload[..], b"pay");
        assert_eq!(streams.shutdown_state.in_flight_count(), 1);

        // Drop guard
        drop(guard);
        assert_eq!(streams.shutdown_state.in_flight_count(), 0);
    }

    #[test]
    fn test_shutdown_state_clone_shares_inner() {
        let s1 = ShutdownState::new();
        let s2 = s1.clone();
        s1.begin_drain();
        assert!(s2.is_draining());
        let _g = s1.acquire();
        assert_eq!(s2.in_flight_count(), 1);
    }

    #[test]
    fn test_teardown_token() {
        let state = ShutdownState::new();
        assert!(!state.teardown_token().is_cancelled());
        state.teardown_token().cancel();
        assert!(state.teardown_token().is_cancelled());
    }

    // ── try_send_or_backpressure ────────────────────────────────────────

    use std::sync::atomic::AtomicBool;

    #[test]
    fn try_send_or_bp_ok_on_empty_channel() {
        let (tx, rx) = flume::bounded::<u32>(4);
        let disc_fired = Arc::new(AtomicBool::new(false));
        let closed_fired = Arc::new(AtomicBool::new(false));
        let disc_c = disc_fired.clone();
        let closed_c = closed_fired.clone();

        let r = try_send_or_backpressure(
            &tx,
            42,
            move |_| disc_c.store(true, Ordering::Release),
            move |_| closed_c.store(true, Ordering::Release),
        );

        assert!(r.is_ok(), "empty channel should return Ok");
        assert_eq!(rx.try_recv().unwrap(), 42);
        assert!(!disc_fired.load(Ordering::Acquire));
        assert!(!closed_fired.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_send_or_bp_backpressure_on_full() {
        let (tx, rx) = flume::bounded::<u32>(1);
        tx.try_send(1).unwrap(); // fill it

        let closed_fired = Arc::new(AtomicBool::new(false));
        let closed_c = closed_fired.clone();

        let bp = try_send_or_backpressure(
            &tx,
            2,
            |_| panic!("disconnected"),
            move |_| closed_c.store(true, Ordering::Release),
        )
        .expect_err("should return bp");

        // Drain a slot so the bp future can complete.
        assert_eq!(rx.recv_async().await.unwrap(), 1);
        tokio::time::timeout(Duration::from_secs(1), bp)
            .await
            .expect("bp should resolve once space is available");
        assert_eq!(rx.recv_async().await.unwrap(), 2);
        assert!(!closed_fired.load(Ordering::Acquire));
    }

    #[test]
    fn try_send_or_bp_disconnected_calls_handler() {
        let (tx, rx) = flume::bounded::<u32>(1);
        drop(rx);

        let disc_fired = Arc::new(AtomicBool::new(false));
        let disc_c = disc_fired.clone();

        let r = try_send_or_backpressure(
            &tx,
            7,
            move |task| {
                assert_eq!(task, 7);
                disc_c.store(true, Ordering::Release);
            },
            |_| panic!("bp path should not run"),
        );
        assert!(r.is_ok(), "Disconnected → Ok(()) with handler fired");
        assert!(disc_fired.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_send_or_bp_drop_before_resolve_does_not_fire_closed() {
        let (tx, _rx) = flume::bounded::<u32>(1);
        tx.try_send(1).unwrap();

        let closed_fired = Arc::new(AtomicBool::new(false));
        let closed_c = closed_fired.clone();

        let bp = try_send_or_backpressure(
            &tx,
            2,
            |_| panic!("disconnected"),
            move |_| closed_c.store(true, Ordering::Release),
        )
        .expect_err("should return bp");
        drop(bp);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !closed_fired.load(Ordering::Acquire),
            "drop-before-resolve must not invoke on_closed_during_bp"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_send_or_bp_channel_closed_during_wait_fires_closed() {
        let (tx, rx) = flume::bounded::<u32>(1);
        tx.try_send(1).unwrap();

        let closed_fired = Arc::new(AtomicBool::new(false));
        let closed_c = closed_fired.clone();

        let bp = try_send_or_backpressure(
            &tx,
            2,
            |_| panic!("disconnected"),
            move |task| {
                assert_eq!(task, 2);
                closed_c.store(true, Ordering::Release);
            },
        )
        .expect_err("should return bp");

        // Close the receive side while bp is waiting — causes flume::send_async
        // to fail with SendError, which should invoke on_closed_during_bp.
        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), bp)
            .await
            .expect("bp resolves even on channel close");
        assert!(
            closed_fired.load(Ordering::Acquire),
            "closed-during-wait must invoke on_closed_during_bp"
        );
    }

    // ── SendBackpressure smoke test ─────────────────────────────────────

    #[tokio::test]
    async fn send_backpressure_passthrough_future() {
        let bp = SendBackpressure::new(Box::pin(async {}));
        bp.await;
    }
}
