// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The frame transport interface of the streaming runtime.
//!
//! [`FrameTransport`] is crate-private. Its implementors are the per-stream
//! transports of this crate (`TcpFrameTransport`, `GrpcFrameTransport`) and
//! the messenger mux. An outside crate cannot implement it: a stream rides
//! the mux over a messenger [`Transport`](velo_ext::Transport), so a new
//! transport implements that trait instead.
//!
//! # Endpoint resolution
//!
//! A frame transport advertises its listener endpoints in the local
//! [`WorkerAddress`] through [`FrameTransport::address`], under the key that
//! [`FrameTransport::key`] returns. The `Velo` builder merges these entries
//! into the local `PeerInfo`, beside the messenger transports' entries.
//!
//! When a peer is registered (by `Velo::register_peer` or by discovery), the
//! runtime calls [`FrameTransport::register`] on every installed frame
//! transport. The transport reads its own entry from the peer's
//! `WorkerAddress`, decodes it, and caches the socket address under the
//! peer's [`WorkerId`]. [`FrameTransport::connect`] then looks up that
//! address. The attach handshake carries no endpoint string.

use anyhow::Result;
use futures::future::BoxFuture;
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress, WorkerId};

/// A transport that carries one stream's frames in order.
///
/// # Ordered delivery
///
/// All frames, data frames (`Item`, `Heartbeat`) and sentinels (`Dropped`,
/// `Detached`, `Finalized`, `TransportError`) alike, travel the one channel
/// that [`bind`](Self::bind) and [`connect`](Self::connect) set up. A
/// sentinel never takes a side channel: the streaming protocol relies on the
/// channel's FIFO order. A frame sent before another is received before it.
///
/// The transport moves raw bytes. The caller encodes a frame before it sends
/// it and decodes what it receives.
///
/// `bind` and `connect` return a [`BoxFuture`]: they run once per stream
/// setup, not per frame, so the allocation is off the hot path.
pub(crate) trait FrameTransport: Send + Sync {
    /// This transport's entry key in a [`WorkerAddress`]. The attach
    /// handshake names it to tell the sender which transport to connect on.
    fn key(&self) -> TransportKey;

    /// This transport's local listener endpoints, encoded for the local
    /// [`WorkerAddress`]. A transport with no listener of its own (the mux)
    /// returns [`WorkerAddress::empty`].
    fn address(&self) -> WorkerAddress;

    /// Learn a peer: read this transport's entry from the peer's
    /// `WorkerAddress`, and cache its resolved address for
    /// [`connect`](Self::connect). The default does nothing, for a transport
    /// with no per-peer state.
    fn register(&self, _peer_info: &PeerInfo) -> Result<()> {
        Ok(())
    }

    /// Forget what [`register`](Self::register) cached for a peer, so that a
    /// later `register` starts fresh. Streams already connected to it may
    /// fail. The runtime calls this also for a peer that was never
    /// registered here, so it must be cheap and idempotent. The default does
    /// nothing.
    fn unregister(&self, _instance_id: InstanceId) {}

    /// Bind the receive end of one attachment of `anchor_id`. `session_id`
    /// tells successive attachments of one anchor apart, so a stale frame
    /// from an earlier session is never delivered. Returns the receiver of
    /// the attachment's frames.
    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>>;

    /// Connect the send end to `peer`'s bound `anchor_id` and `session_id`,
    /// at the address [`register`](Self::register) cached for `peer`.
    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>>;
}

/// The largest encoded item the per-stream transports of this crate carry in
/// one frame: the TCP streaming codec's frame limit, 16 MiB, which the gRPC
/// transport adopts so that one limit holds on every per-stream path. A
/// `StreamSender` on such a transport refuses a larger item at `send`
/// (`SendError::ItemTooLarge`). The gRPC server decodes, and its client
/// encodes, with this value plus the message envelope.
pub const MAX_ITEM_BYTES: usize = crate::transports::tcp::framing::DEFAULT_MAX_FRAME_SIZE as usize;

/// A frame transport of this crate that reports a stream's retirement itself:
/// `Sent` once its pump has written the terminal, `Discarded` when the write
/// fails or the connection ends first. A transport that is only a
/// [`FrameTransport`] cannot say when a frame left, so its sender reports
/// `Sent` when the terminal enters the egress channel.
pub(crate) trait RetiringFrameTransport: FrameTransport {
    /// As [`FrameTransport::connect`], with the stream's retirement for the
    /// pump to report through.
    fn connect_retiring(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
        closed: crate::streaming::control::SenderClosed,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>>;

    /// The largest encoded item this transport carries in one frame, when
    /// the producer should refuse a larger one at `send` rather than let the
    /// transport fail the stream.
    fn item_limit(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    /// The runtime holds frame transports as `Arc<dyn FrameTransport>`, so
    /// the trait must stay object-safe.
    fn _assert_object_safe(_transport: &dyn FrameTransport) {}

    /// Implements only the required methods, to reach the defaults.
    struct DefaultsOnly;

    impl FrameTransport for DefaultsOnly {
        fn key(&self) -> TransportKey {
            TransportKey::new("defaults-only")
        }
        fn address(&self) -> WorkerAddress {
            WorkerAddress::empty()
        }
        fn bind(&self, _: u64, _: u64) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
            async { Err(anyhow::anyhow!("bind stub")) }.boxed()
        }
        fn connect(
            &self,
            _: WorkerId,
            _: u64,
            _: u64,
        ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
            async { Err(anyhow::anyhow!("connect stub")) }.boxed()
        }
    }

    /// The defaults serve the mux, which keeps no per-peer state: `register`
    /// accepts any peer, and `unregister` accepts a known and an unknown one.
    #[test]
    fn default_register_and_unregister_accept_any_peer() {
        let transport = DefaultsOnly;
        let peer = PeerInfo::new(InstanceId::new_v4(), WorkerAddress::empty());
        assert!(transport.register(&peer).is_ok());
        transport.unregister(peer.instance_id());
        transport.unregister(InstanceId::new_v4());
    }
}
