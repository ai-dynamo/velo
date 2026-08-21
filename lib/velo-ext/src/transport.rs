// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active-message transport extension trait.
//!
//! This is the primary contract that out-of-tree transport authors implement.
//! Concrete impls (TCP, HTTP, NATS, gRPC, ZMQ) ship in the `velo` runtime
//! crate; external implementors `impl Transport for MyTransport` against this
//! crate without depending on the runtime.

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::admission::SendOutcome;
use crate::id::{InstanceId, PeerInfo, TransportKey, WorkerAddress};
use crate::observability::TransportObservability;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

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
/// Hot-path cost: one `SeqCst` `fetch_add` plus one `SeqCst` load per inbound
/// [`MessageType::Message`], both inside [`TransportAdapter::admit_message`].
/// Every other frame type is ungated and touches none of these atomics.
/// `is_draining()` is a `Relaxed` load, but it is a reporting hook, not
/// something the frame path calls — see below.
///
/// # Admission is not `is_draining()`
///
/// Deciding whether to accept an inbound [`MessageType::Message`] must go
/// through [`TransportAdapter::admit_message`], never through a bare
/// `is_draining()` check. `is_draining()` is a best-effort observer: a
/// check-then-enqueue sequence built on it is a plain interleaving race —
/// [`begin_drain`](Self::begin_drain), [`wait_for_drain`](Self::wait_for_drain)
/// and teardown can all complete inside the producer's check-to-enqueue gap,
/// and no memory ordering can close that. `admit_message` acquires the
/// in-flight guard *first* and then re-reads the flag with `SeqCst`, which
/// turns the pair into a store-buffer litmus that at least one side must win.
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
    /// **Best-effort observer, not an admission decision.** Uses `Relaxed`
    /// ordering: cheap enough for a per-frame hot-path peek, and sound for
    /// *reporting* because the flag is monotonic (false → true, never reset),
    /// so a `true` is always authoritative. A `false` is not — it may be
    /// stale, and acting on it to enqueue inbound work reopens the
    /// check-then-act race described on [`ShutdownState`]. Admit inbound
    /// [`MessageType::Message`] frames with
    /// [`TransportAdapter::admit_message`] instead.
    #[inline]
    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::Relaxed)
    }

    /// Admission-strength read of the draining flag.
    ///
    /// `SeqCst` so that it participates in the single total order that also
    /// contains [`begin_drain`](Self::begin_drain)'s store, the `fetch_add` in
    /// [`acquire`](Self::acquire), and [`wait_for_drain`](Self::wait_for_drain)'s
    /// load. Private on purpose: the only correct use is *after* acquiring a
    /// guard, which is what [`TransportAdapter::admit_message`] does.
    #[inline]
    fn is_draining_for_admission(&self) -> bool {
        self.inner.draining.load(Ordering::SeqCst)
    }

    /// Begin Phase 1: flip the draining flag. Idempotent.
    ///
    /// `SeqCst`: this store and [`wait_for_drain`](Self::wait_for_drain)'s
    /// load are one half of the admission litmus (the other half being
    /// [`acquire`](Self::acquire)'s `fetch_add` and the flag re-read in
    /// [`TransportAdapter::admit_message`]). Under acquire/release neither
    /// side's load would be ordered against the other side's store and both
    /// could read stale — shutdown seeing zero in-flight while a producer
    /// sees "not draining" — which is exactly the message that would slip
    /// past the gate *and* past the drain wait.
    pub fn begin_drain(&self) {
        self.inner.draining.store(true, Ordering::SeqCst);
    }

    /// Acquire an in-flight guard. The guard increments the counter on creation
    /// and decrements it on drop. Use this to track requests that are being processed.
    ///
    /// Guards are still acquirable after `begin_drain()` — this is intentional
    /// so that already-accepted work can be tracked.
    ///
    /// `SeqCst` on the increment: see [`begin_drain`](Self::begin_drain).
    pub fn acquire(&self) -> InFlightGuard {
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        InFlightGuard {
            inner: self.inner.clone(),
        }
    }

    /// Current number of in-flight requests. Primarily for testing/debugging.
    pub fn in_flight_count(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    /// Wait until in-flight count reaches zero. Returns immediately if already zero.
    ///
    /// Registers interest *before* re-checking the counter: `notify_waiters()`
    /// stores no permit, so a guard dropping between the check and the
    /// registration would strand this waiter forever under
    /// [`ShutdownPolicy::WaitForever`]. Creating the `Notified` future is
    /// enough — tokio wakes futures that exist at `notify_waiters()` time even
    /// if they have not been polled yet.
    ///
    /// The load is `SeqCst` so it is ordered against every producer's
    /// `fetch_add` in [`acquire`](Self::acquire) — see
    /// [`begin_drain`](Self::begin_drain). Note that a guard acquired and
    /// released purely to *reject* a message (the `Draining` arm of
    /// [`TransportAdapter::admit_message`]) makes the count transiently
    /// non-zero; the register-before-check loop above turns that into one
    /// extra iteration, not a missed wakeup.
    pub async fn wait_for_drain(&self) {
        loop {
            let notified = self.inner.drain_complete.notified();
            if self.inner.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
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

/// Abstraction over a single message transport (TCP, HTTP, NATS, gRPC, …).
///
/// Implementations handle peer registration, message sending, listener
/// lifecycle, health checking, and graceful shutdown. The trait is object-safe
/// so transports can be stored as `Arc<dyn Transport>`.
///
/// Out-of-tree implementors should `impl Transport for MyTransport`. The
/// default [`set_observability`](Transport::set_observability) hook is a no-op
/// — implementors only need to override it if they want to integrate with
/// the runtime's metrics handle (which they recover via
/// [`ObservabilityHook::downcast`]).
pub trait Transport: Send + Sync {
    /// Unique key identifying this transport (e.g. `"tcp"`, `"grpc"`).
    fn key(&self) -> TransportKey;
    /// The [`WorkerAddress`] fragment advertised by this transport.
    fn address(&self) -> WorkerAddress;
    /// Register a remote peer, extracting its endpoint from [`PeerInfo`].
    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError>;

    /// Send an active message to the remote instance.
    ///
    /// The frame is taken unconditionally: implementations must not hand it
    /// back, and the caller has no way to retract it. What the return value
    /// reports is *when* the frame reached the per-target send channel.
    ///
    /// - [`SendOutcome::Admitted`] — it is on the channel already. This is also
    ///   what a hard pre-wire failure returns, once `on_error` has been called
    ///   for it (peer unregistered, transport not started, oversized frame):
    ///   there is nothing left for the caller to wait on either way.
    /// - [`SendOutcome::Pending`] — the channel was saturated, so the frame is
    ///   queued in the target's [`AdmissionGate`](crate::admission::AdmissionGate)
    ///   behind its predecessors. The returned
    ///   [`SendAdmission`](crate::admission::SendAdmission) resolves `Ok(())` when the frame is
    ///   enqueued and `Err` when it never will be (the connection epoch died,
    ///   the channel closed). Delivery does **not** depend on the caller
    ///   polling it — dropping it is a legitimate fire-and-forget pattern.
    ///
    /// Implementations must route every send through one gate per target and
    /// keep no `try_send` path around it: an admission that can be overtaken by
    /// a later fast-path send is the reordering hazard the gate exists to
    /// remove (see the [`admission`](crate::admission) module docs).
    ///
    /// Failures *after* admission — the write itself — continue to flow
    /// through `on_error`.
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome;

    /// Largest single message this transport will carry to `target`, in bytes.
    ///
    /// The number bounds `header.len() + payload.len()` for one
    /// [`send_message`](Transport::send_message) — the *combined* frame
    /// content, not the payload alone. A caller that prepends its own envelope
    /// to the payload subtracts that envelope from this number; there is no
    /// second allowance hiding behind it.
    ///
    /// `None` means this transport does not know its capacity: nothing was
    /// negotiated, no limit is configured, or it has not started yet. It is
    /// **not** a claim of unlimited capacity — a caller that reads `None` must
    /// fall back to a conservative budget of its own choosing.
    ///
    /// The answer is per-target because it can be genuinely per-connection: a
    /// NATS client learns `max_payload` from the server it happens to be
    /// connected to, so two peers reached through two clients can differ.
    /// Transports with a single static limit ignore the argument.
    ///
    /// Nothing about this method changes what `send_message` does with an
    /// oversized frame — that still fails pre-wire through the send's
    /// `on_error` handler. It exists so callers can size sends to avoid
    /// meeting that failure at all.
    fn max_message_size(&self, _target: InstanceId) -> Option<usize> {
        None
    }

    /// Start the transport (bind listener, spawn tasks) for the given instance.
    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Tear down the transport, cancelling all tasks and closing connections.
    ///
    /// This is phase 3 of the runtime's graceful shutdown and the runtime calls
    /// it only after [`ShutdownState::begin_drain`] and the drain wait. In-tree
    /// transports also cancel the *shared*
    /// [`ShutdownState::teardown_token`] here, which is instance-wide: it stops
    /// every transport's listeners **and** the runtime's inbound message
    /// consumer, which then abandons whatever is still queued. Calling
    /// `shutdown()` directly on one transport of a live instance therefore
    /// kills inbound dispatch for all of them, with no drain and no
    /// [`MessageType::ShuttingDown`] correlation for the senders. Reach for
    /// the runtime's graceful shutdown instead unless that is precisely what
    /// you want.
    fn shutdown(&self);

    /// Install a transport-scoped observability handle.
    ///
    /// The runtime calls this once per transport during startup with a handle
    /// pre-bound to this transport's `key`. Implementations typically store it
    /// in an [`OnceLock`](std::sync::OnceLock) and call its methods on the
    /// hot path; transports that do not emit metrics can leave the default
    /// no-op.
    fn set_observability(&self, _observability: std::sync::Arc<dyn TransportObservability>) {}

    /// Notification hook for Phase 1 (Gate) of graceful shutdown.
    ///
    /// The runtime calls this *after* flipping the shared [`ShutdownState`]'s
    /// drain flag — the flag the runtime handed this transport inside the
    /// [`TransportAdapter`] at [`start`](Transport::start). Flipping that flag
    /// is the runtime's job, not this method's: per-frame gating is
    /// implemented by routing every inbound [`MessageType::Message`] through
    /// [`TransportAdapter::admit_message`], which returns
    /// [`AdmitOutcome::Draining`] with the frame handed back so this transport
    /// can answer it with a [`MessageType::ShuttingDown`] correlation reply.
    /// Do not gate on [`ShutdownState::is_draining`] directly — that is a
    /// best-effort observer and a check-then-enqueue built on it can let a
    /// message slip past both the gate and the drain wait.
    ///
    /// Override only for drain work the shared flag cannot express — e.g.,
    /// unsubscribing from broker subjects so new requests stop arriving at
    /// all, or pausing an accept loop. The default no-op is correct for
    /// transports whose listeners gate per-frame off the shared flag.
    ///
    /// Must be idempotent. Do not flip the shared `ShutdownState` here: it is
    /// instance-wide, shared by every transport of the instance, so flipping
    /// it from one transport would silently drain them all.
    fn begin_drain(&self) {}

    /// Check if a registered peer is reachable and healthy.
    ///
    /// Returns `Ok(())` if the peer responds within the timeout. Different
    /// transports implement this differently:
    /// - NATS: request/reply to health subject
    /// - TCP: check existing connection or attempt new connection
    /// - HTTP: HEAD request to health endpoint
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

/// An inbound [`MessageType::Message`] frame together with the in-flight
/// guard that makes it visible to [`ShutdownState::wait_for_drain`].
///
/// The guard is **mandatory**, which is the whole point of the type: a message
/// cannot sit on the inbound queue without being counted work. Producers never
/// build one directly — [`TransportAdapter::admit_message`] acquires the guard
/// and constructs it, and the message channel's sender is private so there is
/// no other way in. Whatever happens to the message afterwards — dispatched,
/// dropped on a decode error, handed back undelivered when the receiver has
/// gone, discarded by a consumer that is abandoning its backlog at teardown —
/// the guard rides along and its `Drop` releases the count. "Queued implies
/// counted" is therefore an invariant of the type system, not a convention
/// producers have to honour.
///
/// The one thing that does *not* release a guard is walking away from the
/// channel: flume frees buffered items only once the last endpoint is gone, so
/// a consumer that stops receiving while transports still hold sender clones
/// must drain and drop what it is abandoning, not merely drop its receiver.
///
/// `#[non_exhaustive]`: construct with [`InboundMessage::new`], match with a
/// trailing `..`.
#[non_exhaustive]
pub struct InboundMessage {
    /// The frame's header bytes.
    pub header: Bytes,
    /// The frame's payload bytes.
    pub payload: Bytes,
    /// Keeps the instance's in-flight count non-zero for as long as this
    /// message exists, queued or in a handler.
    pub guard: InFlightGuard,
}

impl InboundMessage {
    /// Bind a frame to an already-acquired in-flight guard.
    ///
    /// Public for consumer-side fabrication — a test that wants a realistic
    /// item to feed a receiver, or a harness that stands in for the runtime.
    /// It is not a way onto the inbound queue: the channel's sender is
    /// private, so producers still go through
    /// [`TransportAdapter::admit_message`].
    pub fn new(header: Bytes, payload: Bytes, guard: InFlightGuard) -> Self {
        Self {
            header,
            payload,
            guard,
        }
    }
}

/// What [`TransportAdapter::admit_message`] did with an inbound frame.
///
/// The rejecting variants hand the frame back, because the reply a transport
/// owes its peer is transport-specific: a TCP listener writes a
/// [`MessageType::ShuttingDown`] frame onto the same socket, NATS publishes to
/// the reply inbox, gRPC pushes onto the RPC's server-to-client stream, and a
/// reader with no reply path at all just drops it and records a rejection.
///
/// Deliberately *not* `#[non_exhaustive]`: each variant hands back a frame that
/// demands a different mandatory action, so a transport that grew a `_ => {}`
/// arm would silently swallow frames. A future outcome should be a compile
/// error at every producer — worth the coordinated bump it would cost.
#[derive(Debug)]
#[must_use = "the rejecting variants hand back a frame the caller still owes its peer a reply for"]
pub enum AdmitOutcome {
    /// The frame is queued and counted; nothing left for the caller to do.
    Admitted,
    /// This instance is draining and will not accept new requests. Reply
    /// [`MessageType::ShuttingDown`] with `header` echoed verbatim so the
    /// sender fails fast instead of waiting out its own timeout.
    Draining {
        /// The rejected request's header.
        header: Bytes,
        /// The rejected request's payload.
        payload: Bytes,
    },
    /// The inbound queue's receiver is gone — the runtime has torn down.
    /// Route the frame to the transport's error handler.
    Disconnected {
        /// The undelivered frame's header.
        header: Bytes,
        /// The undelivered frame's payload.
        payload: Bytes,
    },
}

/// Sender-side handle given to transports for routing inbound frames.
///
/// Each transport receives a clone of this adapter during [`Transport::start`]
/// and uses it to forward decoded `(header, payload)` pairs to the appropriate
/// stream based on [`MessageType`]. Inbound [`MessageType::Message`] frames are
/// the exception: they go through [`admit_message`](Self::admit_message), which
/// owns both the drain gate and the enqueue.
#[derive(Clone)]
pub struct TransportAdapter {
    /// Channel for inbound [`MessageType::Message`] frames.
    ///
    /// Private: every item must carry an [`InFlightGuard`], so the only way to
    /// enqueue is [`admit_message`](Self::admit_message).
    message_stream: flume::Sender<InboundMessage>,
    /// Channel for inbound [`MessageType::Response`] frames.
    pub response_stream: flume::Sender<(Bytes, Bytes)>,
    /// Channel for inbound [`MessageType::Ack`] and [`MessageType::Event`] frames.
    pub event_stream: flume::Sender<(Bytes, Bytes)>,
    /// Channel for inbound [`MessageType::ShuttingDown`] frames — drain
    /// rejections from a peer.
    ///
    /// Each carries the rejected *request's* header, echoed back verbatim so
    /// the sender can correlate it, and an empty payload. The header is in
    /// the request format, not the response format — which is why these
    /// frames have their own lane instead of sharing `response_stream`.
    pub shutdown_stream: flume::Sender<(Bytes, Bytes)>,
    /// Shared shutdown coordinator for drain-aware routing.
    pub shutdown_state: ShutdownState,
}

impl TransportAdapter {
    /// Offer an inbound [`MessageType::Message`] frame to the runtime.
    ///
    /// This is the *only* way to enqueue inbound request work, and it is the
    /// drain gate: transports must not pre-filter on
    /// [`ShutdownState::is_draining`].
    ///
    /// # Why acquire before checking
    ///
    /// The order below is load-bearing: acquire the guard, *then* read the
    /// draining flag, and only then send.
    ///
    /// Checking first and enqueueing second is a check-then-act race that no
    /// memory ordering can fix — the whole of
    /// [`begin_drain`](ShutdownState::begin_drain) →
    /// [`wait_for_drain`](ShutdownState::wait_for_drain) → teardown can run
    /// inside the gap between a producer's check and its send, and the message
    /// then lands on a queue nobody will ever drain.
    ///
    /// Acquiring first makes the two sides a store-buffer litmus instead. This
    /// side does `fetch_add(in_flight)` then `load(draining)`; the shutdown
    /// side does `store(draining)` then `load(in_flight)`. All four accesses
    /// are `SeqCst`, so they share one total order and at least one side must
    /// observe the other: either the shutdown wait sees the increment and
    /// parks until this message is done, or this call sees the flag and
    /// rejects. Both-stale — admitted *and* invisible to the drain — cannot
    /// happen.
    ///
    /// A rejected message therefore blips the in-flight count up and back
    /// down. That is harmless: `wait_for_drain` registers its wakeup before
    /// re-checking, so the blip costs it one extra loop iteration.
    ///
    /// Synchronous by design — the inbound channel is unbounded, so the send
    /// never blocks, and callers on non-async threads (a ZMQ listener thread,
    /// a UCX active-message callback) can use it unchanged.
    pub fn admit_message(&self, header: Bytes, payload: Bytes) -> AdmitOutcome {
        let guard = self.shutdown_state.acquire();

        if self.shutdown_state.is_draining_for_admission() {
            drop(guard);
            return AdmitOutcome::Draining { header, payload };
        }

        match self
            .message_stream
            .send(InboundMessage::new(header, payload, guard))
        {
            Ok(()) => AdmitOutcome::Admitted,
            Err(flume::SendError(InboundMessage {
                header,
                payload,
                guard,
                ..
            })) => {
                // Explicit: the guard must be released here, or an
                // undeliverable frame would keep `wait_for_drain` parked
                // forever under `ShutdownPolicy::WaitForever`.
                drop(guard);
                AdmitOutcome::Disconnected { header, payload }
            }
        }
    }
}

/// Receiver-side handle for consuming inbound frames from all transports.
///
/// Returned by [`make_channels`] alongside the corresponding [`TransportAdapter`].
/// Higher layers pull [`InboundMessage`]s off the message lane and
/// `(header, payload)` pairs off the other three.
pub struct DataStreams {
    /// Receiver for inbound message frames.
    ///
    /// Every item is an [`InboundMessage`] carrying its own [`InFlightGuard`],
    /// so a plain receive is already drain-tracked and the consumer must *not*
    /// acquire a guard of its own. (This is why there is no
    /// `recv_message_tracked`: it acquired after the dequeue, which left the
    /// queued-but-unconsumed window invisible to the drain.)
    ///
    /// A consumer that stops receiving while messages are still queued must
    /// drain and drop them, not just drop this receiver: flume keeps the
    /// buffer alive until the *last* endpoint goes, and transports hold sender
    /// clones for the instance's lifetime, so guards abandoned in the buffer
    /// pin [`ShutdownState::wait_for_drain`] above zero for good.
    pub message_stream: flume::Receiver<InboundMessage>,
    /// Receiver for inbound response frames.
    pub response_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Receiver for inbound ack and event frames.
    pub event_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Receiver for inbound shutting-down frames (drain rejections): the
    /// rejected request's header, echoed verbatim, with an empty payload.
    pub shutdown_stream: flume::Receiver<(Bytes, Bytes)>,
    /// Shared shutdown coordinator.
    pub shutdown_state: ShutdownState,
}

type DataStreamTuple = (
    flume::Receiver<InboundMessage>,
    flume::Receiver<(Bytes, Bytes)>,
    flume::Receiver<(Bytes, Bytes)>,
    flume::Receiver<(Bytes, Bytes)>,
);

impl DataStreams {
    /// Destructure into the four raw receivers
    /// `(message, response, event, shutdown)`.
    pub fn into_parts(self) -> DataStreamTuple {
        (
            self.message_stream,
            self.response_stream,
            self.event_stream,
            self.shutdown_stream,
        )
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
    let (shutdown_tx, shutdown_rx) = flume::unbounded();
    (
        TransportAdapter {
            message_stream: message_tx,
            response_stream: response_tx,
            event_stream: event_tx,
            shutdown_stream: shutdown_tx,
            shutdown_state: shutdown_state.clone(),
        },
        DataStreams {
            message_stream: message_rx,
            response_stream: response_rx,
            event_stream: event_rx,
            shutdown_stream: shutdown_rx,
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
    fn test_acquire_increments_inflight() {
        let state = ShutdownState::new();
        let _g1 = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
    }

    #[test]
    fn test_guard_drop_decrements_inflight() {
        let state = ShutdownState::new();
        let g = state.acquire();
        assert_eq!(state.in_flight_count(), 1);
        drop(g);
        assert_eq!(state.in_flight_count(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_drain_blocks_then_completes() {
        let state = ShutdownState::new();
        let guard = state.acquire();

        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
            state_clone.wait_for_drain().await;
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished());

        drop(guard);
        timeout(Duration::from_millis(100), handle)
            .await
            .expect("should complete after guard drop")
            .unwrap();
    }

    #[test]
    fn test_message_type_roundtrip() {
        for v in 0..=4 {
            let mt = MessageType::from_u8(v).unwrap();
            assert_eq!(mt.as_u8(), v);
        }
        assert_eq!(MessageType::from_u8(5), None);
    }

    #[test]
    fn test_make_channels_includes_shutdown_state() {
        let (adapter, streams) = make_channels();
        assert!(!adapter.shutdown_state.is_draining());
        adapter.shutdown_state.begin_drain();
        assert!(streams.shutdown_state.is_draining());
    }

    // ---------------------------------------------------------------------
    // Admission / drain-visibility invariants.
    //
    // The property under test is "queued implies counted": a frame that
    // `admit_message` put on the inbound queue keeps `wait_for_drain` parked
    // until somebody takes ownership of it and drops it. Before the guard rode
    // inside the queued item, the consumer acquired it only *after* the
    // dequeue, so a queued-but-unconsumed frame was invisible to the drain and
    // `wait_for_drain` returned while it was still sitting there.
    // ---------------------------------------------------------------------

    /// A frame sitting on the inbound queue is counted work: `wait_for_drain`
    /// must park until it is received *and dropped*, not until it is enqueued.
    #[tokio::test]
    async fn queued_message_holds_drain() {
        let (adapter, streams) = make_channels();

        let outcome = adapter.admit_message(
            Bytes::from_static(b"queued-header"),
            Bytes::from_static(b"queued-payload"),
        );
        assert!(matches!(outcome, AdmitOutcome::Admitted));
        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            1,
            "a queued message must be counted work the moment it is admitted"
        );

        adapter.shutdown_state.begin_drain();

        let waiter_state = adapter.shutdown_state.clone();
        let waiter = tokio::spawn(async move { waiter_state.wait_for_drain().await });

        sleep(Duration::from_millis(50)).await;
        assert!(
            !waiter.is_finished(),
            "wait_for_drain completed while a message was still queued and undispatched"
        );

        let queued = timeout(
            Duration::from_millis(500),
            streams.message_stream.recv_async(),
        )
        .await
        .expect("the admitted message must still be on the queue")
        .expect("recv");
        assert_eq!(&queued.header[..], b"queued-header");
        assert_eq!(&queued.payload[..], b"queued-payload");

        // Dropping the message drops its guard — this is what a consumer that
        // finished dispatching does.
        drop(queued);

        timeout(Duration::from_millis(500), waiter)
            .await
            .expect("wait_for_drain must complete once the queued message is released")
            .expect("waiter task panicked");
        assert_eq!(streams.shutdown_state.in_flight_count(), 0);
    }

    /// Admission during drain rejects and hands the frame back, and the guard
    /// it acquired to close the check-then-act race is released again.
    #[tokio::test]
    async fn admit_message_rejects_during_drain() {
        let (adapter, streams) = make_channels();
        adapter.shutdown_state.begin_drain();

        match adapter.admit_message(
            Bytes::from_static(b"reject-header"),
            Bytes::from_static(b"reject-payload"),
        ) {
            AdmitOutcome::Draining { header, payload } => {
                assert_eq!(&header[..], b"reject-header");
                assert_eq!(&payload[..], b"reject-payload");
            }
            AdmitOutcome::Admitted => panic!("a draining instance must not admit a Message"),
            AdmitOutcome::Disconnected { .. } => panic!("the receiver is still alive"),
        }

        assert!(
            streams.message_stream.is_empty(),
            "a rejected message must not reach the queue"
        );
        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            0,
            "the acquire-then-check probe guard must not outlive the rejection"
        );

        // The blip must not have left a waiter stranded either.
        timeout(
            Duration::from_millis(500),
            adapter.shutdown_state.wait_for_drain(),
        )
        .await
        .expect("wait_for_drain must complete after a rejected admission");
    }

    /// Discarding the inbound channel with work still on it releases every
    /// guard that work was holding — nobody has to compensate by hand.
    ///
    /// This is the RAII property the mandatory guard buys. A design where
    /// producers bump a fungible counter and the consumer "adopts" it on
    /// dequeue leaks the entire backlog here, and every later
    /// `wait_for_drain` hangs on a count that can no longer reach zero.
    ///
    /// Note *when* the release happens: flume keeps the queue alive until the
    /// last endpoint goes, so dropping only the receiver is not enough — the
    /// guards go with the buffer, once both ends are gone. Held deliberately
    /// as one assertion at the end rather than after each drop, so this test
    /// does not turn flume's current ordering into a requirement.
    #[test]
    fn dropped_channel_releases_queued_guards() {
        let (adapter, streams) = make_channels();
        let state = adapter.shutdown_state.clone();

        for i in 0..8u8 {
            let outcome = adapter.admit_message(
                Bytes::from(vec![b'h', i]),
                Bytes::from_static(b"queued-payload"),
            );
            assert!(matches!(outcome, AdmitOutcome::Admitted));
        }
        assert_eq!(state.in_flight_count(), 8);

        // Teardown: the consumer's receiver goes, then the transports holding
        // the adapter clones go.
        drop(streams);
        drop(adapter);

        assert_eq!(
            state.in_flight_count(),
            0,
            "discarding the inbound queue must release every guard it was holding"
        );
    }

    /// An undeliverable frame comes back to the caller so it can route it to
    /// the transport's error handler — and its guard is released, or the
    /// frame nobody can deliver would keep `wait_for_drain` parked forever.
    #[tokio::test]
    async fn admit_message_disconnected_returns_frames() {
        let (adapter, streams) = make_channels();
        drop(streams);

        match adapter.admit_message(
            Bytes::from_static(b"orphan-header"),
            Bytes::from_static(b"orphan-payload"),
        ) {
            AdmitOutcome::Disconnected { header, payload } => {
                assert_eq!(&header[..], b"orphan-header");
                assert_eq!(&payload[..], b"orphan-payload");
            }
            AdmitOutcome::Admitted => panic!("there is no receiver left to admit to"),
            AdmitOutcome::Draining { .. } => panic!("the instance is not draining"),
        }

        assert_eq!(
            adapter.shutdown_state.in_flight_count(),
            0,
            "an undeliverable frame must not strand the drain"
        );
        timeout(
            Duration::from_millis(500),
            adapter.shutdown_state.wait_for_drain(),
        )
        .await
        .expect("wait_for_drain must complete after an undeliverable admission");
    }

    /// `wait_for_drain` must not lose the wakeup when the last guard drops
    /// while it is between reading the counter and parking.
    ///
    /// `notify_waiters()` stores no permit, so a `Notified` future created
    /// *after* the drop never hears it. The fix creates the future before
    /// reading the counter; tokio records the `notify_waiters` generation at
    /// construction, so a future that merely exists when the drop happens
    /// completes on its first poll.
    ///
    /// The window is a handful of instructions inside a single task, so this
    /// scans it rather than hitting one interleaving. Both sides start from
    /// the same reference point — the waiter arming `armed` — and each then
    /// burns a busy-wait: the waiter a fixed `WAITER_LEAD`, the dropper a
    /// `spins` that sweeps `0..SPIN_SWEEP` across iterations. The fixed lead
    /// pays for the dropper's cache-coherence latency in seeing `armed`, which
    /// otherwise makes it land systematically *after* the window; the sweep
    /// then walks the drop across it. A lost wakeup is permanent, so the
    /// per-iteration bound is short and the first hit fails the test.
    ///
    /// Probabilistic by nature, but the constants are measured, not guessed:
    /// against the pre-fix `while load { notified().await }` the first hit
    /// landed between iteration 819 and 6703 across runs on two machines, so
    /// `ITERATIONS` leaves roughly 5x headroom over the worst observed.
    ///
    /// The per-iteration bound is a *detector*, not a deadline: a runner that
    /// fails to schedule the freshly spawned dropper thread inside it looks
    /// identical to a lost wakeup from here. So a timeout joins the dropper —
    /// making the guard drop a fact rather than an assumption — and re-awaits
    /// under a generous grace window. A genuinely lost wakeup is permanent
    /// (nothing else ever calls `notify_waiters`), so the grace window costs
    /// no detection power and turns a scheduler stall back into a pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_drain_survives_guard_dropped_at_the_check() {
        const ITERATIONS: usize = 32768;
        const SPIN_SWEEP: usize = 1024;
        const WAITER_LEAD: usize = 600;
        /// Second chance after the detector window, for a stalled runner.
        const GRACE: Duration = Duration::from_secs(2);

        // `black_box` inside each busy-wait keeps LLVM from folding the sum
        // into a closed form and deleting the delay the scan depends on.
        fn burn(rounds: usize) {
            let mut sink = 0usize;
            for k in 0..rounds {
                sink = std::hint::black_box(sink.wrapping_add(k));
            }
        }

        for iteration in 0..ITERATIONS {
            let state = ShutdownState::new();
            let guard = state.acquire();

            let armed = Arc::new(AtomicBool::new(false));
            let spins = iteration % SPIN_SWEEP;

            let dropper_armed = armed.clone();
            let dropper = std::thread::spawn(move || {
                while !dropper_armed.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                burn(spins);
                drop(guard);
            });

            let waiter_state = state.clone();
            let mut waiter = tokio::spawn(async move {
                armed.store(true, Ordering::Release);
                burn(WAITER_LEAD);
                waiter_state.wait_for_drain().await;
            });

            let finished = timeout(Duration::from_millis(200), &mut waiter).await;
            dropper.join().expect("dropper thread panicked");
            let joined = match finished {
                Ok(joined) => joined,
                // The drop has definitely happened now (the thread is joined),
                // so anything still parked is either a lost wakeup or a stall
                // that outlived the detector window.
                Err(_) => timeout(GRACE, &mut waiter).await.unwrap_or_else(|_| {
                    panic!(
                        "wait_for_drain lost the drain wakeup (iteration {iteration}, spins {spins})"
                    )
                }),
            };
            joined.expect("waiter task panicked");
        }
    }
}
