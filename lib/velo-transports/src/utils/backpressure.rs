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

/// Non-blocking `try_send` with blocking fallback on backpressure.
///
/// Fast path: `try_send` succeeds immediately (no allocation, no blocking).
/// Slow path (channel full): blocks the caller until the receiver drains
/// enough space — applying natural backpressure instead of spawning tasks.
///
/// Returns `Disconnected` if the receiver has been dropped, giving the
/// caller the message back for error handling or reconnection.
#[inline]
pub fn try_send_or_block<T>(tx: &flume::Sender<T>, msg: T) -> SendOutcome<T> {
    match tx.try_send(msg) {
        Ok(()) => SendOutcome::Sent,
        Err(flume::TrySendError::Full(msg)) => match tx.send(msg) {
            Ok(()) => SendOutcome::Sent,
            Err(flume::SendError(msg)) => SendOutcome::Disconnected(msg),
        },
        Err(flume::TrySendError::Disconnected(msg)) => SendOutcome::Disconnected(msg),
    }
}
