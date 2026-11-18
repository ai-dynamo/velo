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

use std::sync::Arc;

use crate::streaming::control::SenderClosed;
use crate::streaming::messenger_mux::ingress::DrainSignal;
use crate::streaming::sender::RetiresOn;

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
    /// the attachment's frames, and the drain signal of a transport that
    /// issues credit.
    fn bind(&self, anchor_id: u64, session_id: u64) -> BoxFuture<'_, Result<Bound>>;

    /// Connect the send end to `peer`'s bound `anchor_id` and `session_id`,
    /// at the address [`register`](Self::register) cached for `peer`.
    ///
    /// `retire` is the stream's retirement, for a transport that can say when
    /// the terminal left. The returned [`Connected`] says whether this one
    /// took it. `None` where no sender waits on the stream's retirement.
    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
        retire: Option<SenderClosed>,
    ) -> BoxFuture<'_, Result<Connected>>;
}

/// What [`FrameTransport::bind`] hands back.
pub(crate) struct Bound {
    /// The attachment's frames.
    pub(crate) rx: flume::Receiver<Vec<u8>>,
    /// What the consumer tells for each record it takes out of `rx`, so the
    /// mux returns the sender's credit. `None` for a per-stream transport,
    /// which issues no credit.
    pub(crate) drain: Option<Arc<DrainSignal>>,
}

impl Bound {
    /// A receiver whose transport issues no credit.
    pub(crate) fn uncredited(rx: flume::Receiver<Vec<u8>>) -> Self {
        Self { rx, drain: None }
    }
}

/// What [`FrameTransport::connect`] hands back: the stream's egress channel,
/// who reports the stream's retirement, and the transport's item limit.
#[derive(Debug)]
pub(crate) struct Connected {
    /// The stream's egress channel.
    pub(crate) tx: flume::Sender<Vec<u8>>,
    /// Who reports the stream's retirement once the terminal is in `tx`.
    pub(crate) retires_on: RetiresOn,
    /// The largest encoded item the transport carries in one frame, when
    /// the producer should refuse a larger one at `send` rather than let the
    /// transport fail the stream.
    pub(crate) item_limit: Option<usize>,
}

impl Connected {
    /// A channel whose transport cannot say when a frame left, and has no
    /// item limit: the sender reports `Sent` as the terminal enters `tx`.
    pub(crate) fn queued(tx: flume::Sender<Vec<u8>>) -> Self {
        Self {
            tx,
            retires_on: RetiresOn::Queue,
            item_limit: None,
        }
    }
}

/// The largest encoded item the per-stream transports of this crate carry in
/// one frame: the TCP streaming codec's frame limit, 16 MiB, which the gRPC
/// transport adopts so that one limit holds on every per-stream path. A
/// `StreamSender` on such a transport refuses a larger item at `send`
/// (`SendError::ItemTooLarge`). The gRPC server decodes, and its client
/// encodes, with this value plus the message envelope.
pub const MAX_ITEM_BYTES: usize = crate::transports::tcp::framing::DEFAULT_MAX_FRAME_SIZE as usize;
