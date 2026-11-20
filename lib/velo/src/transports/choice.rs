// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Choosing the transport for one send.
//!
//! A node keeps one *primary* transport per peer, and every send that names
//! no transport uses it. A send can name a transport instead; it then uses
//! that transport or fails with [`TransportChoiceError`]. A reply goes back
//! over the transport its request came in on.

use std::sync::Arc;

use velo_ext::{InstanceId, Transport, TransportKey, TransportOrigin};

/// The most transports one node can have: a peer's route keeps one bit per
/// transport in a `u32`.
pub(crate) const MAX_TRANSPORTS: usize = 32;

/// A transport of this node, resolved once with
/// [`Velo::transport_route`](crate::Velo::transport_route), to name on many
/// sends with `.route(...)`.
///
/// A send with a route hashes nothing: it only checks that the peer is
/// registered on the transport. A route belongs to the node that made it; a
/// send on another node fails with [`TransportChoiceError::ForeignRoute`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransportRoute {
    pub(crate) owner: InstanceId,
    pub(crate) origin: TransportOrigin,
}

/// Why a send that named a transport was not sent.
///
/// A send that names a transport uses it or fails; it never falls back to
/// another transport. Requests return [`anyhow::Error`], so find this error
/// with `error.downcast_ref::<TransportChoiceError>()`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TransportChoiceError {
    /// This node has no transport with this key.
    #[error("no transport {transport} on this node")]
    NotInstalled {
        /// The key the send named.
        transport: TransportKey,
    },
    /// The peer is registered, but not on this transport: its address has
    /// no entry for it, or the transport refused the entry.
    #[error("peer {peer} is not registered on transport {transport}")]
    NotServedByPeer {
        /// The peer the send was for.
        peer: InstanceId,
        /// The transport the send named.
        transport: TransportKey,
    },
    /// The payload is over the staging threshold. A send that names its
    /// transport is never staged: the staged payload would be fetched
    /// apart from the call's route, maybe by RDMA.
    #[error(
        "a payload of {len} bytes is over the staging threshold of {threshold} bytes; \
         a send named on transport {transport} is not staged"
    )]
    WouldStage {
        /// The transport the send named.
        transport: TransportKey,
        /// The payload's length.
        len: usize,
        /// The staging threshold.
        threshold: usize,
    },
    /// The peer is not registered on this node.
    #[error("peer {peer} is not registered")]
    PeerNotRegistered {
        /// The peer the call named.
        peer: InstanceId,
    },
    /// The route was made by another node.
    #[error("the transport route was made by node {owner}, not by this node")]
    ForeignRoute {
        /// The node that made the route.
        owner: InstanceId,
    },
}

/// Why [`Velo::probe_peer`](crate::Velo::probe_peer) failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PeerProbeError {
    /// The route cannot be used for this peer.
    #[error(transparent)]
    Choice(#[from] TransportChoiceError),
    /// The transport's probe failed.
    #[error(transparent)]
    Health(#[from] velo_ext::HealthCheckError),
}

/// Which transport one send goes out on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// The peer's primary transport.
    Primary,
    /// This transport, or fail.
    Named(TransportOrigin),
    /// A reply: the transport the request came in on when the peer is
    /// registered on it, and the primary otherwise. The fallback is counted
    /// in `velo_transport_reply_reroutes_total`.
    Reply(Option<TransportOrigin>),
}

tokio::task_local! {
    /// The transport the request a handler is serving came in on. Set only
    /// around a handler built to read it.
    static REQUEST_ORIGIN: Option<TransportOrigin>;
}

/// Run `future` with `origin` readable through [`request_origin`].
pub(crate) async fn with_request_origin<F: std::future::Future>(
    origin: Option<TransportOrigin>,
    future: F,
) -> F::Output {
    REQUEST_ORIGIN.scope(origin, future).await
}

/// The transport the request being served came in on, inside a handler
/// built to read it. `None` elsewhere, and for an unnumbered adapter.
pub(crate) fn request_origin() -> Option<TransportOrigin> {
    REQUEST_ORIGIN.try_with(|origin| *origin).ok().flatten()
}

/// How a node reaches one registered peer.
pub(crate) struct PeerRoute {
    /// The first transport, in this node's priority order, that accepted the
    /// peer. Every send that names no transport uses it.
    pub(crate) primary: Arc<dyn Transport>,
    /// `primary`'s index.
    pub(crate) primary_origin: TransportOrigin,
    /// One bit per transport index that accepted the peer.
    pub(crate) served: u32,
}

impl PeerRoute {
    /// Whether the peer is registered on the transport with this origin.
    #[inline]
    pub(crate) fn serves(&self, origin: TransportOrigin) -> bool {
        serves(self.served, origin)
    }
}

/// Whether `origin`'s bit is set in `served`. An index past the mask is not
/// served: `checked_shl` keeps a forged origin from panicking.
#[inline]
pub(crate) fn serves(served: u32, origin: TransportOrigin) -> bool {
    1u32.checked_shl(u32::from(origin.index()))
        .is_some_and(|bit| served & bit != 0)
}

#[cfg(test)]
mod messenger_tests;

#[cfg(test)]
mod tests {
    use super::*;

    /// Every index in range reads its own bit, and an index past the mask is
    /// not served rather than a panic on the shift.
    #[test]
    fn serves_reads_one_bit_per_index() {
        let mask = 0b1000_0000_0000_0000_0000_0000_0000_0101;
        let served: Vec<u16> = (0..40)
            .filter(|i| serves(mask, TransportOrigin::new(*i)))
            .collect();
        assert_eq!(served, vec![0, 2, 31]);
    }
}
