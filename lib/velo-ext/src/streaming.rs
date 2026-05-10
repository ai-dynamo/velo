// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Frame-level transport abstraction for ordered delivery streaming.
//!
//! This module defines the [`FrameTransport`] trait boundary consumed by the
//! Velo streaming runtime and implemented by the in-tree
//! `TcpFrameTransport`, `GrpcFrameTransport`, and (deprecated) `VeloFrameTransport`.
//! Out-of-tree implementors (custom RDMA, hardware transports, alternative
//! streaming substrates) should `impl FrameTransport for MyTransport` against
//! this contract.
//!
//! # Endpoint resolution
//!
//! Streaming transports advertise their listener endpoint(s) into the local
//! [`WorkerAddress`] via [`FrameTransport::address`], keyed by the same
//! [`TransportKey`] returned by [`FrameTransport::key`]. The Velo builder
//! merges streaming entries into the local PeerInfo's WorkerAddress alongside
//! messenger transport entries.
//!
//! When a peer is registered (via `Velo::register_peer` or discovery), the
//! runtime calls [`FrameTransport::register`] on every installed streaming
//! transport. The transport extracts its own entry from the peer's
//! WorkerAddress, decodes the endpoint(s), and caches the resolved socket
//! address keyed by the peer's [`WorkerId`].
//!
//! [`FrameTransport::connect`] then looks up the cached address by
//! [`WorkerId`] — no endpoint string is exchanged on the streaming attach
//! handshake.

use anyhow::Result;
use futures::future::BoxFuture;

use crate::id::{PeerInfo, TransportKey, WorkerAddress, WorkerId};

/// Transport abstraction for frame-level ordered delivery.
///
/// # Ordered-Delivery Contract
///
/// All frames -- including data frames (`Item`, `Heartbeat`) **and** sentinel
/// frames (`Dropped`, `Detached`, `Finalized`, `TransportError`) -- MUST travel
/// the **same physical channel** established by [`FrameTransport::bind`] /
/// [`FrameTransport::connect`]. Sentinels MUST NOT be injected via a side
/// channel; the FIFO ordering guarantee of the underlying channel is
/// load-bearing for the correctness of the streaming protocol.
///
/// Implementations MUST preserve send order: a frame sent before another MUST
/// be received before that other frame on the corresponding
/// [`flume::Receiver`].
///
/// # Usage
///
/// The transport operates at the raw byte level. Callers are responsible for
/// serializing frame values to `Vec<u8>` before sending via
/// [`flume::Sender::send_async`], and for deserializing bytes received from
/// [`flume::Receiver::recv_async`].
///
/// # Async Design
///
/// Both `bind` and `connect` return [`BoxFuture`] to support async
/// implementations (e.g., network setup). The heap allocation is acceptable
/// because these are setup-path calls, not per-frame hot-path operations.
pub trait FrameTransport: Send + Sync {
    /// Identifies this transport's entry in [`WorkerAddress`].
    ///
    /// Mirrors [`crate::Transport::key`]. Used by the streaming attach
    /// handshake to tell the client which `FrameTransport` to call
    /// [`connect`](Self::connect) on, and by [`register`](Self::register) to
    /// look up the peer's matching endpoint entry.
    fn key(&self) -> TransportKey;

    /// This transport's local listener endpoints, encoded for inclusion in the
    /// local [`WorkerAddress`].
    ///
    /// Mirrors [`crate::Transport::address`]. Returned at builder time so the
    /// Velo builder can merge it into the local PeerInfo's WorkerAddress.
    /// Implementations that do not open their own listener (e.g., a transport
    /// that piggybacks on the messenger) should return
    /// [`WorkerAddress::default`].
    fn address(&self) -> WorkerAddress;

    /// Notify this transport that a peer's [`PeerInfo`] is now known.
    ///
    /// Mirrors [`crate::Transport::register`]. The transport extracts its own
    /// entry from `peer_info.worker_address()` (using [`Self::key`]), decodes
    /// the endpoint(s), and caches a resolved socket address keyed by the
    /// peer's [`WorkerId`] for later use by [`Self::connect`].
    ///
    /// Default implementation is a no-op for transports that do not require
    /// per-peer state (e.g., a transport that piggybacks on the messenger
    /// which already tracks peers).
    fn register(&self, _peer_info: &PeerInfo) -> Result<()> {
        Ok(())
    }

    /// Bind a receive endpoint for the given anchor.
    ///
    /// - `anchor_id`: identifies which anchor this binding is for.
    /// - `session_id`: unique session identifier for this attachment; used by
    ///   the transport to discriminate between successive sessions on the same
    ///   anchor so that stale frames from a prior session are not delivered.
    ///
    /// Returns the receiver half of the per-session frame channel. Endpoint
    /// resolution is no longer string-based — the connecting peer resolves the
    /// listener address from the bound worker's [`WorkerAddress`] entry for
    /// this transport's [`Self::key`].
    ///
    /// The channel established by `bind` / `connect` MUST provide ordered,
    /// loss-free delivery of all frames including sentinels.
    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>>;

    /// Connect a write endpoint to the given peer's bound anchor.
    ///
    /// - `peer`: the [`WorkerId`] of the worker that called [`Self::bind`].
    ///   The transport looks up the peer's cached endpoint (populated via
    ///   [`Self::register`]) to determine the actual socket address.
    /// - `anchor_id`: identifies which anchor this writer is attached to.
    /// - `session_id`: unique session identifier for this attachment; used by
    ///   the transport to route frames to the correct reader.
    ///
    /// Returns a [`flume::Sender<Vec<u8>>`] for sending frames to the bound
    /// receiver.
    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time proof that [`FrameTransport`] is object-safe.
    fn _assert_object_safe(_transport: &dyn FrameTransport) {}
}
