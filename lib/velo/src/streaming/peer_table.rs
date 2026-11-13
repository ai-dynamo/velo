// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Keep the per-stream transport's peer table in step with the messenger's.
//!
//! A per-stream transport (TCP or gRPC) dials a peer at the endpoint it
//! cached for it in [`FrameTransport::register`]. The messenger backend owns
//! the node's peer table, and every way of learning a peer goes through it,
//! so [`StreamPeerTable`] watches that table and forwards each change.

use std::sync::Weak;

use velo_ext::{InstanceId, PeerInfo};

use crate::streaming::FrameTransport;
use crate::transports::PeerTableObserver;
use crate::transports::address::WorkerAddressBuilder;

/// Forwards the messenger's peer table changes to one stream transport.
///
/// Weak, because the mux, which is the stream transport of a mux-only node,
/// holds the messenger, and the messenger holds this.
pub(crate) struct StreamPeerTable {
    transport: Weak<dyn FrameTransport>,
}

impl StreamPeerTable {
    pub(crate) fn new(transport: Weak<dyn FrameTransport>) -> Self {
        Self { transport }
    }
}

impl PeerTableObserver for StreamPeerTable {
    fn registered(&self, peer: &PeerInfo) {
        let Some(transport) = self.transport.upgrade() else {
            return;
        };
        let key = transport.key();
        // A peer with no entry for this key is skipped, not refused: a
        // 0.22.0 node's `_hello` has no stream endpoint, a mux-only peer has
        // none, and a peer may use another stream transport. The transport
        // would refuse it, and the `_hello` handler would then not answer.
        match peer.worker_address().get_entry(key.as_str()) {
            Ok(Some(_)) => {}
            Ok(None) => {
                tracing::debug!(
                    peer = %peer.worker_id(),
                    streaming_key = %key,
                    "stream transport: peer has no endpoint for this key"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    peer = %peer.worker_id(),
                    streaming_key = %key,
                    %error,
                    "stream transport: cannot decode the peer's address; attaches to it will fail"
                );
                return;
            }
        }
        // The messenger registration stands: the peer can still be messaged.
        // An attach to it fails as one to a peer never registered.
        if let Err(error) = transport.register(peer) {
            tracing::warn!(
                peer = %peer.worker_id(),
                streaming_key = %key,
                error = %format!("{error:#}"),
                "stream transport refused the peer's endpoint; attaches to it will fail"
            );
        }
    }

    fn unregistered(&self, instance_id: InstanceId) {
        if let Some(transport) = self.transport.upgrade() {
            transport.unregister(instance_id);
        }
    }
}

/// The node's complete address: the messenger's entries and the stream
/// transport's endpoint. A transport that opens no listener of its own,
/// such as the mux, adds nothing.
///
/// On a key collision or an encoding error this logs and returns the
/// messenger's address: the node can still be messaged, and peers will not
/// see its stream endpoint.
pub(crate) fn complete_peer_info(
    messenger_peer: PeerInfo,
    stream: &dyn FrameTransport,
) -> PeerInfo {
    let stream_addr = stream.address();
    if stream_addr.as_bytes().is_empty()
        || stream_addr
            .available_transports()
            .map(|v| v.is_empty())
            .unwrap_or(true)
    {
        return messenger_peer;
    }
    let mut builder = WorkerAddressBuilder::new();
    if let Err(e) = builder.merge(messenger_peer.worker_address()) {
        tracing::warn!(
            instance_id = %messenger_peer.instance_id(),
            error = %e,
            "peer_info: failed to merge messenger WorkerAddress into builder; \
             falling back to messenger-only PeerInfo (streaming peers will not \
             see this worker's streaming endpoint)"
        );
        return messenger_peer;
    }
    if let Err(e) = builder.merge(&stream_addr) {
        tracing::warn!(
            instance_id = %messenger_peer.instance_id(),
            streaming_key = %stream.key(),
            error = %e,
            "peer_info: failed to merge streaming WorkerAddress into builder; \
             falling back to messenger-only PeerInfo (likely a key collision \
             with a messenger transport key)"
        );
        return messenger_peer;
    }
    match builder.build() {
        Ok(merged) => PeerInfo::new(messenger_peer.instance_id(), merged),
        Err(e) => {
            tracing::warn!(
                instance_id = %messenger_peer.instance_id(),
                error = %e,
                "peer_info: WorkerAddressBuilder::build() failed; falling back \
                 to messenger-only PeerInfo"
            );
            messenger_peer
        }
    }
}
