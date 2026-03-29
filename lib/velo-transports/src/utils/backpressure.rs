// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Send-path backpressure utilities shared across transports.

/// Outcome of [`try_send_or_block`].
pub enum SendOutcome<T> {
    /// Message was delivered to the channel (or, on a single-threaded
    /// runtime, enqueued via a spawned async task — delivery is best-effort
    /// in that case).
    Sent,
    /// The receiver has been dropped; the message is returned.
    Disconnected(T),
}

/// Runtime context for choosing the correct blocking strategy.
pub enum BlockingStrategy {
    /// Multi-threaded Tokio runtime — use `block_in_place` to yield the
    /// worker thread while blocking.
    BlockInPlace,
    /// Single-threaded (current-thread) Tokio runtime — must not block the
    /// executor thread; spawn an async task instead.
    Spawn(tokio::runtime::Handle),
    /// Not inside a Tokio runtime — safe to block the OS thread directly.
    Direct,
}

/// Determine how a blocking send should proceed based on the current runtime.
#[inline]
pub fn blocking_strategy() -> BlockingStrategy {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                BlockingStrategy::Spawn(handle)
            } else {
                BlockingStrategy::BlockInPlace
            }
        }
        Err(_) => BlockingStrategy::Direct,
    }
}

/// Non-blocking `try_send` with blocking fallback on backpressure.
///
/// Fast path: `try_send` succeeds immediately (no allocation, no blocking).
/// Slow path (channel full):
/// - On a multi-threaded Tokio runtime: uses `block_in_place` so other tasks
///   can still make progress on the current worker thread.
/// - On a single-threaded (current-thread) runtime: falls back to spawning an
///   async `send_async` task to avoid deadlocking the executor. Error handling
///   in this path is fire-and-forget (errors are silently dropped).
/// - Outside any runtime: blocks the OS thread directly.
///
/// Returns `Disconnected` if the receiver has been dropped, giving the
/// caller the message back for error handling or reconnection.
#[inline]
pub fn try_send_or_block<T: Send + 'static>(tx: &flume::Sender<T>, msg: T) -> SendOutcome<T> {
    match tx.try_send(msg) {
        Ok(()) => SendOutcome::Sent,
        Err(flume::TrySendError::Full(msg)) => match blocking_strategy() {
            BlockingStrategy::BlockInPlace => match tokio::task::block_in_place(|| tx.send(msg)) {
                Ok(()) => SendOutcome::Sent,
                Err(flume::SendError(msg)) => SendOutcome::Disconnected(msg),
            },
            BlockingStrategy::Spawn(handle) => {
                let tx = tx.clone();
                handle.spawn(async move {
                    let _ = tx.send_async(msg).await;
                });
                SendOutcome::Sent
            }
            BlockingStrategy::Direct => match tx.send(msg) {
                Ok(()) => SendOutcome::Sent,
                Err(flume::SendError(msg)) => SendOutcome::Disconnected(msg),
            },
        },
        Err(flume::TrySendError::Disconnected(msg)) => SendOutcome::Disconnected(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Fast path ──────────────────────────────────────────────────

    #[test]
    fn fast_path_sends_immediately() {
        let (tx, rx) = flume::bounded(4);
        assert!(matches!(try_send_or_block(&tx, 42), SendOutcome::Sent));
        assert_eq!(rx.try_recv().unwrap(), 42);
    }

    // ── Disconnected path ──────────────────────────────────────────

    #[test]
    fn disconnected_returns_message() {
        let (tx, rx) = flume::bounded::<String>(4);
        drop(rx);
        match try_send_or_block(&tx, "hello".into()) {
            SendOutcome::Disconnected(msg) => assert_eq!(msg, "hello"),
            SendOutcome::Sent => panic!("expected Disconnected"),
        }
    }

    // ── Slow path (Direct — no runtime) ────────────────────────────

    #[test]
    fn slow_path_blocks_until_space_no_runtime() {
        // Channel of capacity 1; fill it, then drain from another thread.
        let (tx, rx) = flume::bounded(1);
        tx.send(1).unwrap(); // fill

        // Signal that the sender thread has started (and is about to block).
        let started = std::sync::Arc::new(std::sync::Barrier::new(2));
        let started2 = started.clone();

        let tx2 = tx.clone();
        let handle = std::thread::spawn(move || {
            started2.wait();
            // Should block until the receiver drains.
            try_send_or_block(&tx2, 2)
        });

        // Wait for the sender thread to start, then drain.
        started.wait();
        assert_eq!(rx.recv().unwrap(), 1);

        assert!(matches!(handle.join().unwrap(), SendOutcome::Sent));
        assert_eq!(rx.recv().unwrap(), 2);
    }

    #[test]
    fn slow_path_disconnected_no_runtime() {
        let (tx, rx) = flume::bounded(1);
        tx.send(1).unwrap(); // fill

        let started = std::sync::Arc::new(std::sync::Barrier::new(2));
        let started2 = started.clone();

        let handle = std::thread::spawn(move || {
            started2.wait();
            try_send_or_block(&tx, 2)
        });

        // Wait for sender to start, then drop receiver so it sees Disconnected.
        started.wait();
        drop(rx);

        assert!(matches!(
            handle.join().unwrap(),
            SendOutcome::Disconnected(2)
        ));
    }

    // ── Slow path (BlockInPlace — multi-threaded runtime) ──────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_path_block_in_place_multi_thread() {
        let (tx, rx) = flume::bounded(1);
        tx.send(1).unwrap(); // fill

        let tx2 = tx.clone();
        let send_task = tokio::task::spawn_blocking(move || try_send_or_block(&tx2, 2));

        // Yield to let spawn_blocking start, then drain.
        tokio::task::yield_now().await;
        assert_eq!(rx.recv_async().await.unwrap(), 1);

        assert!(matches!(send_task.await.unwrap(), SendOutcome::Sent));
        assert_eq!(rx.recv_async().await.unwrap(), 2);
    }

    // ── Slow path (Spawn — current-thread runtime) ─────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn slow_path_spawn_fallback_current_thread() {
        let (tx, rx) = flume::bounded(1);
        tx.send(1).unwrap(); // fill

        // On current_thread, try_send_or_block spawns an async task
        // and returns Sent immediately.
        assert!(matches!(try_send_or_block(&tx, 2), SendOutcome::Sent));

        // Drain the first message so the spawned task can complete.
        assert_eq!(rx.recv_async().await.unwrap(), 1);
        // Yield to let the spawned send_async task run.
        tokio::task::yield_now().await;
        assert_eq!(rx.recv_async().await.unwrap(), 2);
    }

    // ── blocking_strategy detection ────────────────────────────────

    #[test]
    fn strategy_direct_outside_runtime() {
        assert!(matches!(blocking_strategy(), BlockingStrategy::Direct));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn strategy_block_in_place_on_multi_thread() {
        assert!(matches!(
            blocking_strategy(),
            BlockingStrategy::BlockInPlace
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn strategy_spawn_on_current_thread() {
        assert!(matches!(blocking_strategy(), BlockingStrategy::Spawn(_)));
    }
}
