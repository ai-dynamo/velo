// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Send-path backpressure utilities shared across transports.

/// Outcome of [`try_send_or_block`].
pub enum SendOutcome<T> {
    /// Message was delivered to the channel.
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
