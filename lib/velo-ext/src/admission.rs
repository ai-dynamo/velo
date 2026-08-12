// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordered per-target send admission.
//!
//! ## The hazard this exists to fix
//!
//! The obvious way to handle a full per-target channel is to hand the caller a
//! future that owns the frame and completes the enqueue when polled. Velo
//! shipped exactly that until 0.7 and it reorders frames: nothing enqueues the
//! parked frame until somebody polls the future, so a *later* send to the same
//! target can win the race — its `try_send` succeeds while the earlier frame is
//! still sitting in an unpolled future. Two sends issued in order A, B arrive
//! at the remote as B, A. Fire-and-forget senders make it worse; they may never
//! poll at all, so A can sit behind an unbounded number of successors.
//!
//! The fix is structural rather than advisory: take the frame at `send` time
//! and never let its delivery depend on the caller.
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
//! succeeds, [`AdmissionGate::send`] returns [`SendOutcome::Admitted`] without
//! allocating a ticket, waking a driver, or touching a waker. Only contended
//! sends pay. Crucially, a frame the driver has checked out stays in the queue
//! (with its payload taken) until it has been enqueued or dropped, so the
//! "queue is empty" test cannot let a newcomer overtake a frame that is
//! mid-flight.
//!
//! ## Dropping an admission does not cancel it
//!
//! Dropping a [`SendAdmission`] leaves the frame in the gate, and the gate
//! still delivers it. Cancellation is explicit, via [`SendAdmission::cancel`].
//! This is the point of the design, not an oversight: fire-and-forget senders
//! drop their handle on the spot and must still see their frame delivered,
//! which is irreconcilable with drop-cancels-the-send.
//!
//! Callers that want to *observe* an outcome without holding the future — a
//! metric to record, a result channel to feed — register a
//! [`SendAdmission::on_resolved`] hook instead of polling.

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

/// Outcome of [`AdmissionGate::send`], and of
/// [`Transport::send_message`](crate::transport::Transport::send_message).
///
/// Dropping this is a legitimate fire-and-forget pattern — the frame is already
/// owned by the gate and will be delivered either way — so it is deliberately
/// not `#[must_use]`.
#[derive(Debug)]
pub enum SendOutcome {
    /// The frame was enqueued synchronously. No ticket was taken.
    Admitted,
    /// The frame was queued behind the gate's FIFO. The contained handle
    /// observes (and can withdraw) the ticket; it does not drive delivery.
    Pending(SendAdmission),
}

impl SendOutcome {
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

    /// Observe the outcome without polling.
    ///
    /// `on_resolved` receives exactly what awaiting this admission would have
    /// produced, and runs exactly once: at resolution if the ticket is still
    /// pending, or immediately if it has already resolved. That makes it the
    /// mechanism for callers who cannot await — a fire-and-forget send whose
    /// handle is about to be dropped, or a metric that must be recorded when
    /// the frame really lands rather than when it was offered.
    ///
    /// The hook runs on whichever task resolves the ticket, normally the gate's
    /// driver, so keep it short: a slow hook delays the next frame on this
    /// target. It must not call back into the same gate.
    ///
    /// There is no hook for [`SendOutcome::Admitted`] because there is nothing
    /// to wait for — that variant *is* the synchronous notification, and a
    /// caller wanting "exactly once per send" handles it on the spot.
    ///
    /// Hooks are additive and run in registration order. The runtime installs
    /// its own bookkeeping hook (outbound-frame metric, error reporting)
    /// before the admission reaches the caller, so a caller registering its
    /// own observer must not — and cannot — displace it.
    pub fn on_resolved(
        self,
        on_resolved: impl FnOnce(&Result<(), AdmissionError>) + Send + 'static,
    ) -> Self {
        self.ticket.add_hook(Box::new(on_resolved));
        self
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

fn read_outcome(outcome: &TicketOutcome) -> Option<Result<(), AdmissionError>> {
    match outcome {
        TicketOutcome::Pending => None,
        TicketOutcome::Admitted => Some(Ok(())),
        TicketOutcome::Failed(error) => Some(Err(error.clone())),
    }
}

/// Callback installed by [`SendAdmission::on_resolved`].
type ResolveHook = Box<dyn FnOnce(&Result<(), AdmissionError>) + Send>;

/// The outcome and the not-yet-fired hooks share one lock.
///
/// That is what makes hook registration race-free: the "has it resolved yet?"
/// test and the install are one critical section, so a hook can neither be
/// stored on an already-resolved ticket (never to run) nor be missed by a
/// `resolve` that ran a moment earlier.
///
/// Hooks are a `Vec`, not a slot: the runtime installs its own bookkeeping
/// hook (outbound metric, error handler) before the admission ever reaches the
/// caller, and the caller's hook must add to that, never replace it.
struct TicketState {
    outcome: TicketOutcome,
    hooks: Vec<ResolveHook>,
}

struct Ticket {
    state: Mutex<TicketState>,
    waker: AtomicWaker,
    /// Set by [`SendAdmission::cancel`] when the driver already owns the frame.
    cancel: CancellationToken,
}

impl Ticket {
    fn new() -> Self {
        Self {
            state: Mutex::new(TicketState {
                outcome: TicketOutcome::Pending,
                hooks: Vec::new(),
            }),
            waker: AtomicWaker::new(),
            cancel: CancellationToken::new(),
        }
    }

    fn state(&self) -> AdmissionState {
        match lock(&self.state).outcome {
            TicketOutcome::Pending => AdmissionState::Pending,
            TicketOutcome::Admitted => AdmissionState::Admitted,
            TicketOutcome::Failed(_) => AdmissionState::Failed,
        }
    }

    fn outcome(&self) -> Option<Result<(), AdmissionError>> {
        read_outcome(&lock(&self.state).outcome)
    }

    /// Resolve the ticket. First writer wins; later attempts are no-ops.
    ///
    /// Never called with the gate lock held for a ticket whose waker could
    /// re-enter the gate, so the woken task cannot deadlock against us. The
    /// hooks run last, outside the ticket lock, for the same reason — in
    /// registration order, so the runtime's bookkeeping hook fires before any
    /// caller-installed observer.
    fn resolve(&self, outcome: Result<(), AdmissionError>) {
        let hooks = {
            let mut state = lock(&self.state);
            if !matches!(state.outcome, TicketOutcome::Pending) {
                return;
            }
            state.outcome = match &outcome {
                Ok(()) => TicketOutcome::Admitted,
                Err(error) => TicketOutcome::Failed(error.clone()),
            };
            std::mem::take(&mut state.hooks)
        };
        self.waker.wake();
        for hook in hooks {
            hook(&outcome);
        }
    }

    /// Add a completion hook, running it on the spot if the ticket has
    /// already resolved.
    fn add_hook(&self, hook: ResolveHook) {
        let resolved = {
            let mut state = lock(&self.state);
            match read_outcome(&state.outcome) {
                None => {
                    state.hooks.push(hook);
                    return;
                }
                Some(outcome) => outcome,
            }
        };
        hook(&resolved);
    }

    fn is_live(&self) -> bool {
        !self.cancel.is_cancelled() && matches!(lock(&self.state).outcome, TicketOutcome::Pending)
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
/// See the [module docs](self) for the ordering guarantee and for why
/// dropping an admission does not cancel its frame.
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
    /// Synchronous and non-blocking. Returns [`SendOutcome::Admitted`] if
    /// the queue was empty and the channel had room; otherwise the frame joins
    /// the gate's FIFO and the returned [`SendAdmission`] observes its ticket.
    ///
    /// If the channel's receiver has already been dropped the frame is dropped
    /// and an already-failed [`SendOutcome::Pending`] carrying
    /// [`AdmissionError::ChannelClosed`] is returned — the two-variant outcome
    /// has no honest "admitted" answer for a closed channel. A caller that
    /// spawns a task per `Pending` will spawn one that finishes immediately.
    pub fn send(&self, item: T) -> SendOutcome {
        let mut state = lock(&self.inner.state);

        // Fast path. The emptiness check and the try_send are one critical
        // section, so no concurrent sender can slip between them.
        let item = if state.queue.is_empty() {
            match self.inner.tx.try_send(item) {
                Ok(()) => return SendOutcome::Admitted,
                Err(flume::TrySendError::Full(item)) => item,
                Err(flume::TrySendError::Disconnected(_)) => {
                    return SendOutcome::Pending(SendAdmission::resolved(Err(
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
        SendOutcome::Pending(SendAdmission::new(ticket, gate))
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
mod tests;
