// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Single-use, reason-carrying close signal with synchronous subscriber
//! fan-out.
//!
//! # Ordering contract
//!
//! The three state transitions in [`CloseSignal::close`] happen in strict
//! order and are observable in that order by other threads:
//!
//! 1. **`reason` is set** via [`OnceLock::set`].  From this point
//!    [`CloseSignal::is_closed`] returns `true` and
//!    [`CloseSignal::reason`] returns `Some`.
//! 2. **Subscribers are drained** — the callback `Vec` is taken out of
//!    the mutex (slot → `None`) and every registered `Fn` is called with
//!    `&reason`, synchronously, without the lock held.
//! 3. **[`CancellationToken`] is cancelled** — tasks parked on
//!    [`CloseSignal::cancelled`] or the handle returned by
//!    [`CloseSignal::closed`] are woken.
//!
//! Consequence: a subscriber that sets an [`std::sync::atomic::AtomicBool`]
//! or drains a `PendingMap` completes *before* any `tokio::select!` arm
//! parked on the token can run.  This is the **closed-before-drain
//! invariant**.
//!
//! # Not a RAII guard
//!
//! Dropping a [`CloseSignal`] clone **never** closes the signal.  You must
//! call [`CloseSignal::close`] explicitly.  Multiple owners may hold handles
//! without accidentally triggering shutdown.

use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

// ── Internal shared state ───────────────────────────────────────────────────

/// Heap-allocated subscriber callback.
type Subscriber = Box<dyn Fn(&Arc<str>) + Send + Sync>;

/// `Some(vec)` = open; `None` = close has taken the vec (closed).
///
/// The `Option` encodes the open/closed state so that `on_close` can
/// distinguish "not yet closed" from "already closed" with a single lock
/// acquisition and no separate atomic flag.
type SubscriberSlot = Mutex<Option<Vec<Subscriber>>>;

struct Inner {
    token: CancellationToken,
    reason: OnceLock<Arc<str>>,
    subscribers: SubscriberSlot,
}

// ── Public handle ───────────────────────────────────────────────────────────

/// A cheap, cloneable handle to a single-use close signal.
///
/// See the [module-level docs](self) for the full ordering contract and hook
/// usage.  The canonical pattern for wiring a `PendingMap` to drain on close
/// is shown there.
///
/// # Not a RAII guard
///
/// Dropping **any** clone of [`CloseSignal`] does **not** close the signal.
/// Call [`close`](Self::close) explicitly.
#[derive(Clone)]
pub struct CloseSignal {
    inner: Arc<Inner>,
}

impl Default for CloseSignal {
    /// Creates a new, open [`CloseSignal`].
    ///
    /// Identical to [`CloseSignal::new`].  Provided so callers can derive or
    /// embed [`CloseSignal`] in structs that use `#[derive(Default)]`.
    fn default() -> Self {
        Self::new()
    }
}

impl CloseSignal {
    /// Creates a new, open [`CloseSignal`].
    ///
    /// The subscribers slot is initialised to `Some(vec![])`, encoding the
    /// *open* state.  When [`close`](Self::close) is called the slot is taken
    /// (set to `None`), encoding the *closed* state for racing
    /// [`on_close`](Self::on_close) calls.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                token: CancellationToken::new(),
                reason: OnceLock::new(),
                subscribers: Mutex::new(Some(Vec::new())),
            }),
        }
    }

    /// Closes the signal with the given reason.
    ///
    /// The gate is the [`OnceLock`]: only the first caller wins; subsequent
    /// calls return `false` immediately without touching subscribers or the
    /// token.
    ///
    /// The winner executes, **in strict order**:
    ///
    /// 1. Sets `reason` (makes [`is_closed`](Self::is_closed) return `true`).
    /// 2. Locks `subscribers`, takes the `Vec` out (slot → `None`), drops the
    ///    lock, then calls each callback synchronously with `&reason`.
    /// 3. Cancels the inner [`CancellationToken`] (wakes async awaiters).
    ///
    /// This method is **synchronous and runtime-free** — it may be called from
    /// any thread, including threads with no Tokio executor.
    ///
    /// Returns `true` if this call won the gate, `false` if already closed.
    pub fn close(&self, reason: impl Into<Arc<str>>) -> bool {
        let reason: Arc<str> = reason.into();

        // ── Step 1: gate ─────────────────────────────────────────────────
        // Only the first call proceeds; losers return immediately.
        if self.inner.reason.set(reason.clone()).is_err() {
            return false;
        }
        // `reason` is now the canonical value in the OnceLock.  All reads of
        // `self.inner.reason.get()` from this point on return `Some(&reason)`.

        // ── Step 2: drain subscribers ─────────────────────────────────────
        // Acquire the lock only long enough to take the Vec out.  The slot
        // becomes `None`, signalling the "already closed" path in `on_close`.
        let subscribers = {
            let mut guard = self.inner.subscribers.lock();
            // `take()` returns the vec and leaves `None` in the slot.
            // `unwrap_or_default()` is unreachable (new() always puts Some),
            // but avoids an expect() that could panic in adversarial impls.
            guard.take().unwrap_or_default()
        };
        // Lock is released here.  Fire callbacks outside the lock so that a
        // callback calling `on_close` again doesn't deadlock.
        for f in &subscribers {
            f(&reason);
        }

        // ── Step 3: wake async awaiters ───────────────────────────────────
        // Token is cancelled *last* so that any task woken by the cancel
        // observes subscriber side-effects that were set in step 2.
        self.inner.token.cancel();

        true
    }

    /// Returns a clone of the inner [`CancellationToken`].
    ///
    /// Useful for integrating with existing `tokio_util` cancellation trees or
    /// passing into `tokio::select!` alongside other tokens.
    ///
    /// The token is cancelled during step 3 of [`close`](Self::close), **after**
    /// all registered subscribers have fired.  Use [`is_closed`](Self::is_closed)
    /// rather than `token.is_cancelled()` when you need to know whether close
    /// has been called — the token is the last thing to flip.
    pub fn closed(&self) -> CancellationToken {
        self.inner.token.clone()
    }

    /// Waits asynchronously until the signal is closed.
    ///
    /// Resolves after step 3 of [`close`](Self::close): the token is
    /// cancelled, all subscribers have already run, and
    /// [`reason`](Self::reason) is guaranteed to return `Some`.
    pub async fn cancelled(&self) {
        // Clone the token before awaiting to avoid holding a borrow of `self`
        // across the await point.
        let token = self.inner.token.clone();
        token.cancelled().await;
    }

    /// Returns `true` if the signal has been closed.
    ///
    /// This becomes `true` as soon as step 1 of [`close`](Self::close)
    /// completes — **before** subscribers fire and **before** the token is
    /// cancelled.
    ///
    /// Do not use `self.closed().is_cancelled()` as a proxy: the
    /// [`CancellationToken`] is the last thing to flip and briefly lags behind
    /// the `is_closed` state.
    pub fn is_closed(&self) -> bool {
        self.inner.reason.get().is_some()
    }

    /// Returns the close reason, or `None` if not yet closed.
    ///
    /// Guaranteed to return `Some` once [`is_closed`](Self::is_closed) returns
    /// `true`, once [`cancelled`](Self::cancelled) resolves, and inside every
    /// callback registered with [`on_close`](Self::on_close).
    pub fn reason(&self) -> Option<Arc<str>> {
        self.inner.reason.get().cloned()
    }

    /// Registers a callback to be invoked when the signal is closed.
    ///
    /// **If the signal is not yet closed**, the callback is appended to the
    /// subscriber list and will be called (synchronously, without the lock)
    /// when [`close`](Self::close) fires.
    ///
    /// **If the signal is already closed** (the subscriber slot is `None`),
    /// the callback is invoked immediately in the calling thread.  The reason
    /// is guaranteed to be `Some` at this point because [`close`](Self::close)
    /// sets the [`OnceLock`] *before* taking the subscriber vec.
    ///
    /// # No callback is ever lost or doubled
    ///
    /// `close` sets `reason` before it locks and takes the subscriber vec.
    /// A concurrent `on_close` call either:
    ///
    /// * Locks *before* the take: finds `Some`, pushes, lock released →
    ///   `close` will take the vec including this callback.
    /// * Locks *after* the take: finds `None` → reason is already set →
    ///   fires immediately.
    ///
    /// Neither path loses the callback; neither path fires it twice.
    ///
    /// # Example
    ///
    /// Wire a `PendingMap` to drain when the session closes.  Any
    /// `Fn(&Arc<str>)` is accepted; [`CloseSignal`] does not depend on
    /// [`crate::sync::PendingMap`] specifically.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use velo::sync::CloseSignal;
    /// let signal = CloseSignal::new();
    /// // Assume `pending` is a PendingMap<u64, SomeValue> already in scope.
    /// // signal.on_close({
    /// //     let p = pending.clone();
    /// //     move |r: &Arc<str>| { p.close(Arc::clone(r)); }
    /// // });
    /// // CloseSignal does not depend on PendingMap; any Fn(&Arc<str>) works:
    /// signal.on_close(|reason| println!("closed: {reason}"));
    /// signal.close("example");
    /// ```
    pub fn on_close(&self, f: impl Fn(&Arc<str>) + Send + Sync + 'static) {
        let mut guard = self.inner.subscribers.lock();
        match guard.as_mut() {
            Some(vec) => {
                // Signal is open; enqueue for later.
                vec.push(Box::new(f));
                // Guard drops here, releasing the lock.
            }
            None => {
                // Signal is already closed.  Release the lock before calling `f`
                // to avoid re-entrancy deadlock if `f` itself calls `on_close`.
                drop(guard);
                // INVARIANT: reason is set before the slot becomes None, so
                // `get()` is guaranteed to return Some here.
                let reason = self
                    .inner
                    .reason
                    .get()
                    .expect("reason must be set before subscribers slot becomes None")
                    .clone();
                f(&reason);
            }
        }
    }
}

// ── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::CloseSignal;

    /// TEST-CLOSESIG-01
    ///
    /// First close wins and returns `true`; second returns `false`; the reason
    /// set by the winner is preserved.
    #[test]
    fn test_close_signal_fire_once() {
        let sig = CloseSignal::new();
        assert!(!sig.is_closed(), "new signal must be open");
        assert!(sig.reason().is_none(), "new signal must have no reason");

        let first = sig.close("oops");
        assert!(first, "first close must return true");
        assert!(sig.is_closed());
        assert_eq!(sig.reason().unwrap().as_ref(), "oops");

        let second = sig.close("other");
        assert!(!second, "second close must return false");
        // Winner's reason must not be overwritten.
        assert_eq!(
            sig.reason().unwrap().as_ref(),
            "oops",
            "reason must not change after first close"
        );
    }

    /// TEST-CLOSESIG-02
    ///
    /// A Tokio task awaiting `cancelled()` wakes after `close()` and can read
    /// the reason via `reason()`.
    #[tokio::test]
    async fn test_close_signal_tokio_awaiter() {
        let sig = CloseSignal::new();
        let sig2 = sig.clone();

        let handle = tokio::spawn(async move {
            sig2.cancelled().await;
            sig2.reason()
                .expect("reason must be Some after cancelled() resolves")
        });

        sig.close("tokio-wake");
        let reason = handle.await.unwrap();
        assert_eq!(reason.as_ref(), "tokio-wake");
    }

    /// TEST-CLOSESIG-03
    ///
    /// - Multiple subscribers all fire exactly once.
    /// - Registration *after* close fires immediately in the calling thread.
    /// - A `tokio::select!` arm parked on the token observes the cancel only
    ///   after a subscriber side-effect is already visible (AtomicBool set
    ///   inside the subscriber).
    #[tokio::test]
    async fn test_close_signal_on_close() {
        let sig = CloseSignal::new();

        let fired_a = Arc::new(AtomicUsize::new(0));
        let fired_b = Arc::new(AtomicUsize::new(0));
        // Set to true inside subscriber B; the tokio task must see this
        // after the token is cancelled.
        let side_effect = Arc::new(AtomicBool::new(false));

        sig.on_close({
            let c = fired_a.clone();
            move |_| {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        sig.on_close({
            let c = fired_b.clone();
            let se = side_effect.clone();
            move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                se.store(true, Ordering::SeqCst);
            }
        });

        // Spawn a task that parks on the CancellationToken and then reads the
        // side-effect.  The ordering guarantee means the effect is visible.
        let token = sig.closed();
        let se2 = side_effect.clone();
        let waker = tokio::spawn(async move {
            token.cancelled().await;
            se2.load(Ordering::SeqCst)
        });

        sig.close("multi");

        // Both pre-registered subscribers must have fired synchronously.
        assert_eq!(
            fired_a.load(Ordering::SeqCst),
            1,
            "subscriber A must fire once"
        );
        assert_eq!(
            fired_b.load(Ordering::SeqCst),
            1,
            "subscriber B must fire once"
        );

        // Tokio task must observe the side-effect that was set in step 2
        // (subscriber fan-out), before the token woke it in step 3.
        let observed = waker.await.unwrap();
        assert!(
            observed,
            "tokio task must observe subscriber side-effect after cancel"
        );

        // Registration after close fires immediately in the calling thread.
        let fired_late = Arc::new(AtomicUsize::new(0));
        sig.on_close({
            let c = fired_late.clone();
            move |r| {
                assert_eq!(
                    r.as_ref(),
                    "multi",
                    "late callback must receive original reason"
                );
                c.fetch_add(1, Ordering::SeqCst);
            }
        });
        assert_eq!(
            fired_late.load(Ordering::SeqCst),
            1,
            "late on_close registration must fire immediately"
        );
    }

    /// TEST-CLOSESIG-04
    ///
    /// `close()` is callable from a plain `std::thread` with no Tokio runtime.
    /// A Tokio awaiter wakes and reads the correct reason.
    #[tokio::test]
    async fn test_close_signal_from_std_thread() {
        let sig = CloseSignal::new();
        let sig_thread = sig.clone();

        // Spawn a plain OS thread — no Tokio executor exists in that thread.
        let join = std::thread::spawn(move || sig_thread.close("from-std-thread"));

        // Park the tokio context on the signal.
        sig.cancelled().await;

        let reason = sig
            .reason()
            .expect("reason must be Some after cancelled() resolves");
        assert_eq!(reason.as_ref(), "from-std-thread");

        assert!(join.join().unwrap(), "std thread must have won the gate");
    }

    /// TEST-CLOSESIG-05
    ///
    /// N `std::thread`s racing `close()` — exactly one wins, the winning
    /// reason is one of the supplied inputs, and the registered subscriber
    /// fires exactly once.
    #[test]
    fn test_close_signal_race() {
        const N: usize = 32;

        let sig = CloseSignal::new();
        let fired = Arc::new(AtomicUsize::new(0));

        sig.on_close({
            let c = fired.clone();
            move |_| {
                c.fetch_add(1, Ordering::SeqCst);
            }
        });

        let winners = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(N);

        for i in 0..N {
            let sig2 = sig.clone();
            let w = winners.clone();
            handles.push(std::thread::spawn(move || {
                let reason = format!("thread-{i}");
                if sig2.close(reason.as_str()) {
                    w.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            winners.load(Ordering::SeqCst),
            1,
            "exactly one thread must win the gate"
        );
        let reason = sig.reason().expect("reason must be set");
        assert!(
            reason.starts_with("thread-"),
            "reason must be one of the thread inputs, got: {reason}"
        );
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "subscriber must fire exactly once across all racing threads"
        );
    }
}
