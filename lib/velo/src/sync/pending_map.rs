// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Caller-keyed pending-operation map with atomic drain-on-close semantics.
//!
//! [`PendingMap`] tracks in-flight operations keyed by caller-supplied `K`
//! values, resolves them individually via [`PendingMap::resolve`], and
//! drains all outstanding waiters atomically when [`PendingMap::close`] is
//! called.
//!
//! # Correctness invariant — single-lock insert/close serialization
//!
//! Insertion and the closed-check share **one** [`parking_lot::Mutex`]. This
//! eliminates the insert-after-drain race that arises when a sharded map and
//! a separate closed flag are used: a caller could pass the closed-check,
//! observe `Open`, be preempted while `close()` drains the map and sets the
//! flag, and then insert into an already-closed map where nobody will drain
//! the new entry. With a single mutex, `register` and `close` are fully
//! serialized: either `register` wins (the map is still open and the new
//! entry will be drained by a future `close`), or `close` wins (the map is
//! already `Closed` and `register` immediately returns `Err`). An entry
//! sitting in a closed map with nobody left to drain it is unrepresentable.
//! No post-insert re-check is needed or present.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::oneshot;

// ── Reason strings ────────────────────────────────────────────────────────────

/// Reason yielded by a [`Waiter`] when the [`PendingMap`] is dropped without
/// an explicit [`PendingMap::close`] call (i.e. the sender side is deallocated).
const DEFAULT_REASON: &str = "pending map dropped";

/// Reason delivered to a [`Waiter`] by an explicit [`PendingMap::cancel`] call.
/// Distinct from [`DEFAULT_REASON`] so that callers can distinguish a deliberate
/// per-key cancellation from the map being dropped.
const CANCEL_REASON: &str = "operation cancelled";

// ── Closed ────────────────────────────────────────────────────────────────────

/// Marker carried by every [`Waiter`] that resolves to an error, indicating
/// that the [`PendingMap`] was closed before the operation completed.
#[derive(Debug, Clone, Error)]
#[error("closed: {reason}")]
pub struct Closed {
    reason: Arc<str>,
}

impl Closed {
    /// The human-readable reason supplied to [`PendingMap::close`].
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

// ── RegisterError ─────────────────────────────────────────────────────────────

/// Error returned by [`PendingMap::register`] when the operation cannot be
/// registered.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RegisterError {
    /// The map has been closed; the enclosed [`Closed`] value carries the
    /// reason supplied to [`PendingMap::close`].
    #[error(transparent)]
    Closed(#[from] Closed),

    /// A waiter for this key is already registered.
    ///
    /// The original waiter is untouched and remains resolvable.
    #[error("key already registered")]
    Occupied,
}

// ── Internal state ────────────────────────────────────────────────────────────

enum State<K, V> {
    Open(HashMap<K, oneshot::Sender<Result<V, Closed>>>),
    Closed(Closed),
}

// ── PendingMap ────────────────────────────────────────────────────────────────

/// Caller-keyed pending-operation map with atomic drain-on-close semantics.
///
/// Each call to [`register`](Self::register) installs a `(key, waiter)` pair
/// and returns a [`Waiter`] future that resolves when the key is either
/// resolved via [`resolve`](Self::resolve), cancelled via
/// [`cancel`](Self::cancel), or the map is closed via
/// [`close`](Self::close).
///
/// Cloning a `PendingMap` produces a second handle to the **same** map.
/// All clones share state; closing through any handle closes all handles.
///
/// # Correctness
///
/// See the module-level documentation for the single-lock insert/close
/// serialization invariant.
pub struct PendingMap<K, V = ()> {
    inner: Arc<Mutex<State<K, V>>>,
}

// Manual Clone — only the Arc is cloned; K and V do not need Clone.
impl<K, V> Clone for PendingMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K: Eq + Hash, V> Default for PendingMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> PendingMap<K, V> {
    /// Create a new, empty, open `PendingMap`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::Open(HashMap::new()))),
        }
    }

    /// Register a pending operation for `key` and return a [`Waiter`] that
    /// resolves when the operation completes.
    ///
    /// This method is synchronous and does not require a Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::Closed`] if the map has already been closed.
    /// Returns [`RegisterError::Occupied`] if a waiter for `key` is already
    /// registered; the existing waiter is untouched.
    pub fn register(&self, key: K) -> Result<Waiter<V>, RegisterError> {
        // Single critical section: closed-check and insert are atomic.
        // See module-level doc for why this matters.
        let mut guard = self.inner.lock();
        match &mut *guard {
            State::Closed(c) => Err(RegisterError::Closed(c.clone())),
            State::Open(map) => {
                if map.contains_key(&key) {
                    return Err(RegisterError::Occupied);
                }
                let (tx, rx) = oneshot::channel();
                map.insert(key, tx);
                Ok(Waiter { rx })
            }
        }
    }

    /// Resolve the pending operation for `key` with `value`.
    ///
    /// Returns `true` if an entry was found and the value was delivered.
    /// Returns `false` if the map is closed, the key is absent, or the
    /// [`Waiter`] has been dropped.
    ///
    /// # Panics
    ///
    /// Never panics.
    pub fn resolve(&self, key: &K, value: V) -> bool {
        let tx = {
            let mut guard = self.inner.lock();
            match &mut *guard {
                State::Closed(_) => return false,
                State::Open(map) => match map.remove(key) {
                    Some(tx) => tx,
                    None => return false,
                },
            }
        };
        // Send outside the lock so the receiver's waker cannot re-enter
        // self.inner — mirroring the drop(guard)-before-send discipline in
        // close().  Ignore send error: the Waiter may have been dropped.
        let _ = tx.send(Ok(value));
        true
    }

    /// Cancel the pending operation for `key`.
    ///
    /// The corresponding [`Waiter`] will resolve to
    /// `Err(Closed { reason: "operation cancelled" })`.  This reason is
    /// distinct from the `"pending map dropped"` string produced when the
    /// [`PendingMap`] itself is dropped, so callers can distinguish a
    /// deliberate per-key cancellation from an uncontrolled shutdown.
    ///
    /// Returns `true` if an entry was removed, `false` if the map is closed
    /// or the key is absent. A second call for the same key returns `false`.
    pub fn cancel(&self, key: &K) -> bool {
        let tx = {
            let mut guard = self.inner.lock();
            match &mut *guard {
                State::Closed(_) => return false,
                State::Open(map) => match map.remove(key) {
                    Some(tx) => tx,
                    None => return false,
                },
            }
        };
        // Send outside the lock (consistent with resolve() and close()) and
        // deliver an explicit reason rather than relying on the dropped-sender
        // fold, so the waiter can distinguish this from a map-drop shutdown.
        let _ = tx.send(Err(Closed {
            reason: Arc::from(CANCEL_REASON),
        }));
        true
    }

    /// Close the map with a human-readable `reason` and drain all outstanding
    /// waiters.
    ///
    /// Every outstanding [`Waiter`] is resolved with `Err(Closed { reason })`.
    /// Subsequent calls to [`register`](Self::register) return
    /// [`RegisterError::Closed`]. Subsequent calls to [`resolve`](Self::resolve)
    /// or [`cancel`](Self::cancel) return `false`.
    ///
    /// This method is synchronous and safe to call from non-Tokio threads
    /// (e.g., PyO3/vLLM shutdown paths).
    ///
    /// Returns the number of waiters that were drained. Returns `0` if the
    /// map was already closed (first-caller wins; the original reason is
    /// preserved).
    ///
    /// Senders are notified **outside** the mutex to ensure waker callbacks
    /// cannot re-enter the lock.
    pub fn close(&self, reason: impl Into<Arc<str>>) -> usize {
        let closed = Closed {
            reason: reason.into(),
        };
        // Extract the open map under the lock, then release before sending.
        let moved_map = {
            let mut guard = self.inner.lock();
            if matches!(&*guard, State::Closed(_)) {
                return 0;
            }
            let old = mem::replace(&mut *guard, State::Closed(closed.clone()));
            // Explicit drop: release the lock before we touch the senders.
            // oneshot::Sender::send() is non-blocking but it does wake the
            // receiver's task; waker callbacks must not run under our mutex.
            drop(guard);
            match old {
                State::Open(map) => map,
                State::Closed(_) => unreachable!("state was Open before replace"),
            }
        };
        let count = moved_map.len();
        for (_, tx) in moved_map {
            let _ = tx.send(Err(closed.clone()));
        }
        count
    }

    /// Returns `true` if the map has been closed.
    pub fn is_closed(&self) -> bool {
        matches!(&*self.inner.lock(), State::Closed(_))
    }

    /// Returns the [`Closed`] value if the map has been closed, or `None` if
    /// it is still open.
    pub fn close_reason(&self) -> Option<Closed> {
        match &*self.inner.lock() {
            State::Closed(c) => Some(c.clone()),
            State::Open(_) => None,
        }
    }

    /// Number of pending (registered but not yet resolved) operations.
    ///
    /// Returns `0` if the map is closed, regardless of how many entries were
    /// present before closing.
    pub fn len(&self) -> usize {
        match &*self.inner.lock() {
            State::Open(map) => map.len(),
            State::Closed(_) => 0,
        }
    }

    /// Returns `true` if there are no pending operations.
    ///
    /// Equivalent to `self.len() == 0`. Returns `true` when the map is closed.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ── Waiter ────────────────────────────────────────────────────────────────────

/// A `Future` that resolves when the associated key is resolved, cancelled, or
/// the [`PendingMap`] is closed or dropped.
///
/// Dropping a `Waiter` without awaiting it does not cancel the corresponding
/// registration; the key remains in the map until [`PendingMap::resolve`],
/// [`PendingMap::cancel`], or [`PendingMap::close`] is called.
#[must_use = "a Waiter does nothing unless awaited"]
pub struct Waiter<V> {
    rx: oneshot::Receiver<Result<V, Closed>>,
}

impl<V> Future for Waiter<V> {
    type Output = Result<V, Closed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            // Sender was dropped without sending — only happens when the
            // PendingMap itself is deallocated without an explicit close().
            // cancel() and close() always send explicitly, so this arm is
            // the "map dropped without close" path only.
            Poll::Ready(Err(_recv_err)) => Poll::Ready(Err(Closed {
                reason: Arc::from(DEFAULT_REASON),
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// register then resolve yields Ok(value).
    #[tokio::test]
    async fn register_resolve_ok() {
        let map: PendingMap<u32, &str> = PendingMap::new();
        let waiter = map.register(1).expect("register");
        assert!(map.resolve(&1, "hello"));
        let result = waiter.await;
        assert_eq!(result.unwrap(), "hello");
    }

    /// resolve for an absent key returns false.
    #[tokio::test]
    async fn resolve_absent_returns_false() {
        let map: PendingMap<u32, &str> = PendingMap::new();
        assert!(!map.resolve(&99, "nope"));
    }

    /// close drains all waiters with the given reason; len() == 0 after.
    #[tokio::test]
    async fn close_drains_all() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        let w1 = map.register(1).expect("register 1");
        let w2 = map.register(2).expect("register 2");
        let w3 = map.register(3).expect("register 3");

        let count = map.close("shutting down");
        assert_eq!(count, 3);
        assert_eq!(map.len(), 0);

        let r1 = w1.await.unwrap_err();
        let r2 = w2.await.unwrap_err();
        let r3 = w3.await.unwrap_err();
        assert_eq!(r1.reason(), "shutting down");
        assert_eq!(r2.reason(), "shutting down");
        assert_eq!(r3.reason(), "shutting down");
    }

    /// close is idempotent: second call returns 0; first reason is preserved.
    #[tokio::test]
    async fn close_idempotent() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        let count1 = map.close("first");
        assert_eq!(count1, 0);

        let count2 = map.close("second");
        assert_eq!(count2, 0);

        // First reason wins.
        assert_eq!(map.close_reason().unwrap().reason(), "first");
    }

    /// register after close returns Err(RegisterError::Closed); len stays 0.
    #[tokio::test]
    async fn register_after_close_returns_closed() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        map.close("done");

        match map.register(1) {
            Err(RegisterError::Closed(c)) => assert_eq!(c.reason(), "done"),
            Ok(_) => panic!("expected RegisterError::Closed, got Ok(Waiter)"),
            Err(RegisterError::Occupied) => panic!("expected RegisterError::Closed, got Occupied"),
        }
        assert_eq!(map.len(), 0);
    }

    /// resolve and cancel after close both return false.
    #[tokio::test]
    async fn resolve_cancel_after_close_returns_false() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        map.close("done");

        assert!(!map.resolve(&1, ()));
        assert!(!map.cancel(&1));
    }

    /// double-register returns Err(Occupied); original waiter still resolvable.
    #[tokio::test]
    async fn double_register_occupied() {
        let map: PendingMap<u32, u32> = PendingMap::new();
        let original = map.register(42).expect("first register");

        match map.register(42) {
            Err(RegisterError::Occupied) => {}
            Ok(_) => panic!("expected RegisterError::Occupied, got Ok(Waiter)"),
            Err(e) => panic!("expected RegisterError::Occupied, got: {e}"),
        }

        // Original waiter is unaffected.
        assert!(map.resolve(&42, 7));
        assert_eq!(original.await.unwrap(), 7);
    }

    /// cancel returns true; waiter yields Err(Closed) with CANCEL_REASON
    /// ("operation cancelled"), distinct from the "pending map dropped" string
    /// produced when the map itself is dropped without close().
    /// Second cancel returns false.
    #[tokio::test]
    async fn cancel_yields_cancel_reason() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        let waiter = map.register(10).expect("register");

        assert!(map.cancel(&10));
        assert!(!map.cancel(&10), "second cancel returns false");

        let err = waiter.await.unwrap_err();
        assert_eq!(err.reason(), CANCEL_REASON);
        // Verify it is distinct from the map-dropped reason.
        assert_ne!(err.reason(), DEFAULT_REASON);
    }

    /// Dropping the PendingMap without close: outstanding waiter yields Err
    /// with DEFAULT_REASON and does not hang.
    #[tokio::test]
    async fn drop_without_close_resolves_waiter() {
        let waiter: Waiter<()> = {
            let map: PendingMap<u32, ()> = PendingMap::new();
            // map drops when this block exits, taking the inner Arc; the sender
            // side is dropped along with it, causing rx to yield RecvError.
            map.register(1).expect("register")
        };
        let err = waiter.await.unwrap_err();
        assert_eq!(err.reason(), DEFAULT_REASON);
    }

    /// is_closed reflects closed state.
    #[test]
    fn is_closed_tracks_state() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        assert!(!map.is_closed());
        map.close("x");
        assert!(map.is_closed());
    }

    /// len() returns pending count; 0 for closed maps.
    #[tokio::test]
    async fn len_and_is_empty() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        assert!(map.is_empty());

        let _w1 = map.register(1).expect("r1");
        let _w2 = map.register(2).expect("r2");
        assert_eq!(map.len(), 2);
        assert!(!map.is_empty());

        map.close("done");
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
    }

    /// Clone shares state.
    #[tokio::test]
    async fn clone_shares_state() {
        let map: PendingMap<u32, u32> = PendingMap::new();
        let clone = map.clone();

        let waiter = map.register(5).expect("register");
        // Resolve through the clone.
        assert!(clone.resolve(&5, 99));
        assert_eq!(waiter.await.unwrap(), 99);
    }

    /// close() through a clone affects the original.
    #[tokio::test]
    async fn close_through_clone() {
        let map: PendingMap<u32, ()> = PendingMap::new();
        let clone = map.clone();
        let waiter = map.register(1).expect("register");

        clone.close("via clone");
        assert!(map.is_closed());

        let err = waiter.await.unwrap_err();
        assert_eq!(err.reason(), "via clone");
    }
}
