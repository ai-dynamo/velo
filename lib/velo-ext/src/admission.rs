// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordered per-target send admission.
//!
//! ## The hazard this exists to fix
//!
//! [`try_send_or_backpressure`](crate::transport::try_send_or_backpressure)
//! hands a caller that hit a full channel a [`SendBackpressure`] future holding
//! the frame. Nothing enqueues that frame until somebody polls the future, so a
//! *later* send to the same target can win the race: its `try_send` succeeds
//! while the earlier frame is still parked in an unpolled future. Two sends
//! issued in order A, B arrive at the remote as B, A. Fire-and-forget senders
//! make this worse — they may never poll at all, so A's frame can sit
//! indefinitely behind an unbounded number of successors.
//!
//! ## The guarantee
//!
//! An [`AdmissionGate`] wraps one bounded [`flume::Sender`] and serialises
//! everything that goes into it:
//!
//! > Frames enter the channel in the order their [`AdmissionGate::send`] calls
//! > returned, regardless of which admissions (if any) are ever polled.
//!
//! Two structural choices carry that guarantee:
//!
//! 1. **Frames live in the gate, not in the future.** [`SendAdmission`] is a
//!    completion observer and a cancellation handle — never the owner of the
//!    frame. Delivery therefore cannot depend on who polls.
//! 2. **A lazy driver task drains the queue.** The first queued ticket spawns a
//!    per-gate driver that pushes frames with `send_async` in FIFO order and
//!    resolves each ticket as its frame is enqueued. The driver parks (exits)
//!    when the queue empties and is respawned by the next queued ticket.
//!
//! The fast path is preserved: when the queue is empty *and* `try_send`
//! succeeds, [`AdmissionGate::send`] returns [`AdmissionOutcome::Admitted`]
//! without allocating a ticket, waking a driver, or touching a waker. Only
//! contended sends pay. Crucially, a frame the driver has checked out stays in
//! the queue (with its payload taken) until it has been enqueued or dropped, so
//! the "queue is empty" test cannot let a newcomer overtake a frame that is
//! mid-flight.
//!
//! ## Divergence from `SendBackpressure`: dropping does not cancel
//!
//! Dropping a [`SendBackpressure`] cancels the send. Dropping a
//! [`SendAdmission`] does **not** — the frame is already in the gate and will
//! be delivered. Cancellation is explicit, via [`SendAdmission::cancel`]. This
//! is deliberate: fire-and-forget senders drop their handle immediately and
//! must still see their frame delivered, which is irreconcilable with
//! drop-cancel. Callers that want the old behaviour call `cancel()`.
//!
//! [`SendBackpressure`]: crate::transport::SendBackpressure

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::task::{Context, Poll};

use futures::task::AtomicWaker;
use tokio_util::sync::CancellationToken;

/// Take a lock, ignoring poisoning.
///
/// Every critical section here is a handful of `VecDeque` operations with no
/// user code in between, so a poisoned lock means a panic elsewhere rather than
/// torn state. Propagating the panic would strand every outstanding ticket.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Synchronously observable state of a [`SendAdmission`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmissionState {
    /// The frame is queued in the gate and has not been enqueued yet.
    Pending,
    /// The frame has been enqueued on the transport's send channel.
    Admitted,
    /// The frame will never be enqueued; see the admission's error.
    Failed,
}

/// Why a frame was never admitted to the transport's send channel.
///
/// An admission failure is *not* a delivery failure. A frame that is admitted
/// can still fail on the wire, and those failures continue to flow through the
/// transport's error handler.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    /// [`SendAdmission::cancel`] withdrew the ticket before it was enqueued.
    #[error("send admission was cancelled")]
    Cancelled,

    /// The connection epoch that owned the ticket was replaced. The frame
    /// belonged to a connection that no longer exists; resending it on the
    /// successor connection is the caller's decision.
    #[error("connection was replaced before the frame was admitted")]
    ConnectionReplaced,

    /// The transport's send channel was closed (receiver dropped) before the
    /// frame could be enqueued.
    #[error("transport send channel closed before the frame was admitted")]
    ChannelClosed,

    /// The epoch died for a transport-specific reason.
    #[error("send admission failed: {0}")]
    Failed(String),
}

/// Outcome of [`AdmissionGate::send`].
///
/// Dropping this is a legitimate fire-and-forget pattern — the frame is already
/// owned by the gate and will be delivered either way — so it is deliberately
/// not `#[must_use]`.
#[derive(Debug)]
pub enum AdmissionOutcome {
    /// The frame was enqueued synchronously. No ticket was taken.
    Admitted,
    /// The frame was queued behind the gate's FIFO. The contained handle
    /// observes (and can withdraw) the ticket; it does not drive delivery.
    Pending(SendAdmission),
}

impl AdmissionOutcome {
    /// `true` if the frame took the synchronous fast path.
    pub fn is_admitted(&self) -> bool {
        matches!(self, Self::Admitted)
    }

    /// Take the admission handle, if this send queued a ticket.
    pub fn into_pending(self) -> Option<SendAdmission> {
        match self {
            Self::Admitted => None,
            Self::Pending(admission) => Some(admission),
        }
    }
}

/// Completion observer for one queued frame.
///
/// Resolves `Ok(())` when the frame is enqueued on the transport's send channel
/// and `Err` when it will never be. **Polling is optional**: the gate's driver
/// delivers queued frames whether or not anyone awaits, and dropping this
/// handle does not cancel the send (see the [module docs](self)). Use
/// [`cancel`](Self::cancel) to withdraw a frame.
pub struct SendAdmission {
    ticket: Arc<Ticket>,
    /// `None` for admissions that were already resolved at construction.
    gate: Option<Weak<dyn TicketRegistry>>,
}

impl SendAdmission {
    fn new(ticket: Arc<Ticket>, gate: Weak<dyn TicketRegistry>) -> Self {
        Self {
            ticket,
            gate: Some(gate),
        }
    }

    /// An admission that is already resolved — nothing is queued anywhere.
    fn resolved(outcome: Result<(), AdmissionError>) -> Self {
        let ticket = Ticket::new();
        ticket.resolve(outcome);
        Self {
            ticket: Arc::new(ticket),
            gate: None,
        }
    }

    /// Current state of the ticket. Cheap and synchronous; safe to call from a
    /// non-async context.
    pub fn state(&self) -> AdmissionState {
        self.ticket.state()
    }

    /// Withdraw the frame from the gate.
    ///
    /// Successors keep their relative order — the ticket is removed from the
    /// FIFO, not swapped out.
    ///
    /// Exactness has two regimes:
    ///
    /// - **Still queued** (the common case, including every ticket taken since
    ///   the driver last parked): the frame is removed and dropped under the
    ///   gate lock and the admission resolves [`AdmissionError::Cancelled`].
    ///   The frame is guaranteed never to reach the channel.
    /// - **Already checked out** by the driver, i.e. parked in `send_async`
    ///   waiting for capacity: cancellation is best-effort. If the channel
    ///   accepts the frame before the driver observes the cancellation, the
    ///   frame is delivered and the admission resolves `Admitted` instead. In
    ///   the reverse race a frame that landed in the channel may still report
    ///   `Cancelled`. Only one frame per gate is ever in this window.
    pub fn cancel(self) {
        if let Some(gate) = self.gate.as_ref().and_then(Weak::upgrade) {
            gate.cancel_ticket(&self.ticket);
        } else {
            // Gate is gone: nothing can deliver the frame any more.
            self.ticket.resolve(Err(AdmissionError::Cancelled));
        }
    }
}

impl Future for SendAdmission {
    type Output = Result<(), AdmissionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ticket = &self.get_mut().ticket;
        if let Some(outcome) = ticket.outcome() {
            return Poll::Ready(outcome);
        }
        ticket.waker.register(cx.waker());
        // Re-check: the ticket may have resolved between the first read and the
        // waker registration.
        match ticket.outcome() {
            Some(outcome) => Poll::Ready(outcome),
            None => Poll::Pending,
        }
    }
}

impl std::fmt::Debug for SendAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendAdmission")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Ticket state, resolved exactly once.
enum TicketOutcome {
    Pending,
    Admitted,
    Failed(AdmissionError),
}

struct Ticket {
    outcome: Mutex<TicketOutcome>,
    waker: AtomicWaker,
    /// Set by [`SendAdmission::cancel`] when the driver already owns the frame.
    cancel: CancellationToken,
}

impl Ticket {
    fn new() -> Self {
        Self {
            outcome: Mutex::new(TicketOutcome::Pending),
            waker: AtomicWaker::new(),
            cancel: CancellationToken::new(),
        }
    }

    fn state(&self) -> AdmissionState {
        match *lock(&self.outcome) {
            TicketOutcome::Pending => AdmissionState::Pending,
            TicketOutcome::Admitted => AdmissionState::Admitted,
            TicketOutcome::Failed(_) => AdmissionState::Failed,
        }
    }

    fn outcome(&self) -> Option<Result<(), AdmissionError>> {
        match &*lock(&self.outcome) {
            TicketOutcome::Pending => None,
            TicketOutcome::Admitted => Some(Ok(())),
            TicketOutcome::Failed(error) => Some(Err(error.clone())),
        }
    }

    /// Resolve the ticket. First writer wins; later attempts are no-ops.
    ///
    /// Never called with the gate lock held for a ticket whose waker could
    /// re-enter the gate, so the woken task cannot deadlock against us.
    fn resolve(&self, outcome: Result<(), AdmissionError>) {
        {
            let mut slot = lock(&self.outcome);
            if !matches!(*slot, TicketOutcome::Pending) {
                return;
            }
            *slot = match outcome {
                Ok(()) => TicketOutcome::Admitted,
                Err(error) => TicketOutcome::Failed(error),
            };
        }
        self.waker.wake();
    }

    fn is_live(&self) -> bool {
        !self.cancel.is_cancelled() && matches!(*lock(&self.outcome), TicketOutcome::Pending)
    }
}

/// One connection lifetime's worth of tickets.
///
/// [`AdmissionGate::fail_all`] cancels the current epoch and installs a fresh
/// one, so the gate itself is never poisoned: a successor connection's sends
/// use the new epoch and are unaffected by the old one's failure.
struct Epoch {
    token: CancellationToken,
    reason: OnceLock<AdmissionError>,
}

impl Epoch {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            reason: OnceLock::new(),
        }
    }

    /// Kill the epoch. Only ever called once per epoch (under the gate lock).
    fn fail(&self, error: AdmissionError) {
        let _ = self.reason.set(error);
        self.token.cancel();
    }

    fn reason(&self) -> AdmissionError {
        self.reason
            .get()
            .cloned()
            .unwrap_or(AdmissionError::ConnectionReplaced)
    }
}

/// A frame waiting its turn.
///
/// `item` is taken when the driver checks the frame out for delivery, but the
/// entry stays at the head of the queue until the send resolves. That keeps
/// `queue.is_empty()` false for the whole in-flight window, which is what stops
/// a fast-path send from overtaking a frame the driver is mid-way through.
struct QueuedFrame<T> {
    item: Option<T>,
    ticket: Arc<Ticket>,
}

struct GateState<T> {
    queue: VecDeque<QueuedFrame<T>>,
    /// A driver task exists and owns the queue. Only the driver clears this,
    /// and only under the lock with an empty queue.
    driver_live: bool,
    epoch: Arc<Epoch>,
}

struct GateInner<T> {
    tx: flume::Sender<T>,
    rt: tokio::runtime::Handle,
    state: Mutex<GateState<T>>,
}

/// Type-erased handle so [`SendAdmission`] does not have to carry `T`.
trait TicketRegistry: Send + Sync {
    fn cancel_ticket(&self, ticket: &Arc<Ticket>);
}

impl<T: Send + 'static> TicketRegistry for GateInner<T> {
    fn cancel_ticket(&self, ticket: &Arc<Ticket>) {
        let removed = {
            let mut state = lock(&self.state);
            let position = state
                .queue
                .iter()
                .position(|frame| Arc::ptr_eq(&frame.ticket, ticket));
            match position {
                // The frame is still queued: remove it (dropping the payload)
                // without disturbing its successors.
                Some(position) if state.queue[position].item.is_some() => {
                    state.queue.remove(position);
                    true
                }
                _ => false,
            }
        };
        if removed {
            ticket.resolve(Err(AdmissionError::Cancelled));
        } else {
            // Either the driver already owns the frame — it will observe this
            // and abort — or the ticket has already resolved, in which case
            // this is a no-op.
            ticket.cancel.cancel();
        }
    }
}

/// Ordered admission to one bounded send channel.
///
/// A gate is scoped to whatever the transport treats as a target: one per
/// connection for stream transports, one per peer over a shared writer for
/// broker transports. Cloning is cheap (the state is shared) so a gate can be
/// handed to every task that sends to that target.
///
/// See the [module docs](self) for the ordering guarantee and the deliberate
/// drop-does-not-cancel divergence from
/// [`SendBackpressure`](crate::transport::SendBackpressure).
pub struct AdmissionGate<T: Send + 'static> {
    inner: Arc<GateInner<T>>,
}

impl<T: Send + 'static> Clone for AdmissionGate<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send + 'static> std::fmt::Debug for AdmissionGate<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionGate")
            .field("queued", &self.queued_len())
            .finish_non_exhaustive()
    }
}

impl<T: Send + 'static> AdmissionGate<T> {
    /// Build a gate over a bounded channel.
    ///
    /// `rt` is used to spawn the driver task that drains queued frames;
    /// transports already hold a [`Handle`](tokio::runtime::Handle) from
    /// [`Transport::start`](crate::transport::Transport::start).
    ///
    /// The channel *must* be bounded — an unbounded channel never returns
    /// `Full`, so every send takes the fast path and the gate is inert.
    pub fn new(tx: flume::Sender<T>, rt: tokio::runtime::Handle) -> Self {
        Self {
            inner: Arc::new(GateInner {
                tx,
                rt,
                state: Mutex::new(GateState {
                    queue: VecDeque::new(),
                    driver_live: false,
                    epoch: Arc::new(Epoch::new()),
                }),
            }),
        }
    }

    /// Offer a frame to the channel, taking a ticket if it cannot go now.
    ///
    /// Synchronous and non-blocking. Returns [`AdmissionOutcome::Admitted`] if
    /// the queue was empty and the channel had room; otherwise the frame joins
    /// the gate's FIFO and the returned [`SendAdmission`] observes its ticket.
    ///
    /// If the channel's receiver has already been dropped the frame is dropped
    /// and an already-failed [`AdmissionOutcome::Pending`] carrying
    /// [`AdmissionError::ChannelClosed`] is returned — the two-variant outcome
    /// has no honest "admitted" answer for a closed channel. A caller that
    /// spawns a task per `Pending` will spawn one that finishes immediately.
    pub fn send(&self, item: T) -> AdmissionOutcome {
        let mut state = lock(&self.inner.state);

        // Fast path. The emptiness check and the try_send are one critical
        // section, so no concurrent sender can slip between them.
        let item = if state.queue.is_empty() {
            match self.inner.tx.try_send(item) {
                Ok(()) => return AdmissionOutcome::Admitted,
                Err(flume::TrySendError::Full(item)) => item,
                Err(flume::TrySendError::Disconnected(_)) => {
                    return AdmissionOutcome::Pending(SendAdmission::resolved(Err(
                        AdmissionError::ChannelClosed,
                    )));
                }
            }
        } else {
            item
        };

        let ticket = Arc::new(Ticket::new());
        state.queue.push_back(QueuedFrame {
            item: Some(item),
            ticket: Arc::clone(&ticket),
        });
        let spawn_driver = !state.driver_live;
        state.driver_live = true;
        drop(state);

        if spawn_driver {
            let inner = Arc::clone(&self.inner);
            self.inner.rt.spawn(drive(inner));
        }

        let weak = Arc::downgrade(&self.inner);
        let gate: Weak<dyn TicketRegistry> = weak;
        AdmissionOutcome::Pending(SendAdmission::new(ticket, gate))
    }

    /// Fail every outstanding ticket and drop the frames behind them.
    ///
    /// Called when the connection this gate feeds dies: each queued frame
    /// belongs to an epoch that no longer exists, so delivering it on the
    /// successor connection would be wrong. Every pending [`SendAdmission`]
    /// resolves `Err(error)` and flips to [`AdmissionState::Failed`].
    ///
    /// The gate is **not** poisoned — a fresh epoch is installed and later
    /// sends admit normally, so a transport may either rebuild a gate per
    /// connection or keep one and call this on each reconnect.
    ///
    /// The one frame the driver may already have handed to the channel is
    /// resolved by the driver rather than here: if the channel accepted it
    /// before the epoch died it resolves `Admitted`, because it really was
    /// delivered. Everything not yet enqueued fails.
    pub fn fail_all(&self, error: AdmissionError) {
        let failed = {
            let mut state = lock(&self.inner.state);
            let dead = std::mem::replace(&mut state.epoch, Arc::new(Epoch::new()));
            dead.fail(error.clone());

            let mut failed = Vec::new();
            let mut retained = VecDeque::new();
            for frame in std::mem::take(&mut state.queue) {
                if frame.item.is_some() {
                    // Dropping `frame` here drops the payload.
                    failed.push(frame.ticket);
                } else {
                    // Checked out by the driver; it owns the resolution. Keep
                    // it at the head so successors stay ordered behind it.
                    retained.push_back(frame);
                }
            }
            state.queue = retained;
            failed
        };

        for ticket in failed {
            ticket.resolve(Err(error.clone()));
        }
    }

    /// Number of tickets the gate is still holding.
    ///
    /// Zero on a gate whose sends are all taking the fast path. Primarily for
    /// tests, metrics, and saturation debugging.
    pub fn queued_len(&self) -> usize {
        lock(&self.inner.state).queue.len()
    }

    /// Whether a driver task currently owns the queue.
    #[cfg(test)]
    fn driver_live(&self) -> bool {
        lock(&self.inner.state).driver_live
    }

    /// Whether the driver has checked the head frame out and is parked in
    /// `send_async` waiting for capacity.
    #[cfg(test)]
    fn head_checked_out(&self) -> bool {
        lock(&self.inner.state)
            .queue
            .front()
            .is_some_and(|frame| frame.item.is_none())
    }
}

/// Drain the gate's queue in FIFO order until it empties.
///
/// This is the only thing that ever enqueues a queued frame, which is why the
/// gate's ordering guarantee holds without any caller polling.
async fn drive<T: Send + 'static>(inner: Arc<GateInner<T>>) {
    while let Some(checkout) = check_out_head(&inner) {
        let Checkout {
            item,
            ticket,
            epoch,
        } = checkout;

        // The send future owns the frame for the duration of this block and is
        // dropped before the ticket resolves — dropping it before flume accepts
        // the frame means the frame is never enqueued, so an aborted frame can
        // never surface behind its successors.
        let outcome = {
            let send = inner.tx.send_async(item);
            tokio::pin!(send);
            tokio::select! {
                // Biased so that a frame flume has already accepted reports
                // `Admitted` rather than being mislabelled by a cancellation
                // that lost the race. The frame is in the channel either way.
                biased;
                result = &mut send => match result {
                    Ok(()) => Ok(()),
                    Err(flume::SendError(_)) => Err(AdmissionError::ChannelClosed),
                },
                () = ticket.cancel.cancelled() => Err(AdmissionError::Cancelled),
                () = epoch.token.cancelled() => Err(epoch.reason()),
            }
        };

        {
            let mut state = lock(&inner.state);
            if let Some(head) = state.queue.front()
                && Arc::ptr_eq(&head.ticket, &ticket)
            {
                state.queue.pop_front();
            }
        }
        // Resolved outside the gate lock: the frame has already left (or been
        // dropped), so nothing a woken task does can reorder anything.
        ticket.resolve(outcome);
    }
}

struct Checkout<T> {
    item: T,
    ticket: Arc<Ticket>,
    epoch: Arc<Epoch>,
}

/// Take the next deliverable frame, skipping tickets that died while queued.
///
/// Returns `None` once the queue is empty, clearing `driver_live` under the
/// same lock so the next queued ticket spawns a fresh driver.
fn check_out_head<T: Send + 'static>(inner: &Arc<GateInner<T>>) -> Option<Checkout<T>> {
    let mut state = lock(&inner.state);
    loop {
        let Some(head) = state.queue.front() else {
            state.driver_live = false;
            return None;
        };
        if !head.ticket.is_live() {
            let frame = state.queue.pop_front().expect("front was just observed");
            drop(state);
            frame.ticket.resolve(Err(AdmissionError::Cancelled));
            state = lock(&inner.state);
            continue;
        }

        let epoch = Arc::clone(&state.epoch);
        let mut checked_out = None;
        if let Some(head) = state.queue.front_mut()
            && let Some(item) = head.item.take()
        {
            checked_out = Some((item, Arc::clone(&head.ticket)));
        }
        match checked_out {
            Some((item, ticket)) => {
                return Some(Checkout {
                    item,
                    ticket,
                    epoch,
                });
            }
            // Defensive: a checked-out frame is always removed before the
            // driver looks again, so this is unreachable in practice.
            None => {
                let frame = state.queue.pop_front().expect("front was just observed");
                drop(state);
                frame.ticket.resolve(Err(AdmissionError::Cancelled));
                state = lock(&inner.state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::time::timeout;

    const LIMIT: Duration = Duration::from_secs(10);

    /// Unwrap an outcome that must have queued a ticket.
    fn pending(outcome: AdmissionOutcome) -> SendAdmission {
        match outcome {
            AdmissionOutcome::Pending(admission) => admission,
            AdmissionOutcome::Admitted => panic!("expected a queued ticket, got Admitted"),
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
    fn admitted(outcome: AdmissionOutcome) {
        match outcome {
            AdmissionOutcome::Admitted => {}
            AdmissionOutcome::Pending(_) => panic!("expected Admitted, got a queued ticket"),
        }
    }

    // The auto-trait shape below is load-bearing for the transport swap: a gate
    // lives behind `Arc<dyn Transport>` and its admissions are held across
    // `.await` points in transport writer tasks.
    const fn assert_send_sync<T: Send + Sync>() {}
    const fn assert_unpin<T: Unpin>() {}
    const _: () = {
        assert_send_sync::<SendAdmission>();
        assert_send_sync::<AdmissionOutcome>();
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
}
