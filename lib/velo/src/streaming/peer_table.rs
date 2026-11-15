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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::future::BoxFuture;
    use velo_ext::{TransportKey, WorkerAddress, WorkerId};

    use super::*;

    /// A stream transport that records the peers it is asked to register.
    struct Recording {
        key: &'static str,
        address: WorkerAddress,
        refuse: bool,
        registered: parking_lot::Mutex<Vec<InstanceId>>,
        unregistered: parking_lot::Mutex<Vec<InstanceId>>,
    }

    impl Recording {
        fn new(key: &'static str, address: WorkerAddress, refuse: bool) -> Arc<Self> {
            Arc::new(Self {
                key,
                address,
                refuse,
                registered: Default::default(),
                unregistered: Default::default(),
            })
        }
    }

    impl FrameTransport for Recording {
        fn key(&self) -> TransportKey {
            TransportKey::new(self.key)
        }

        fn address(&self) -> WorkerAddress {
            self.address.clone()
        }

        fn register(&self, peer: &PeerInfo) -> anyhow::Result<()> {
            anyhow::ensure!(!self.refuse, "refused");
            self.registered.lock().push(peer.instance_id());
            Ok(())
        }

        fn unregister(&self, instance_id: InstanceId) {
            self.unregistered.lock().push(instance_id);
        }

        fn bind(
            &self,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, anyhow::Result<flume::Receiver<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(1).1) })
        }

        fn connect(
            &self,
            _peer: WorkerId,
            _anchor_id: u64,
            _session_id: u64,
        ) -> BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>> {
            Box::pin(async { Ok(flume::bounded::<Vec<u8>>(1).0) })
        }
    }

    fn address(keys: &[&str]) -> WorkerAddress {
        let mut builder = WorkerAddressBuilder::new();
        for key in keys {
            builder
                .add_entry(*key, Bytes::from(format!("{key}-endpoint")))
                .unwrap();
        }
        builder.build().unwrap()
    }

    fn peer(keys: &[&str]) -> PeerInfo {
        PeerInfo::new(InstanceId::new_v4(), address(keys))
    }

    fn observer(transport: &Arc<Recording>) -> StreamPeerTable {
        let transport: Arc<dyn FrameTransport> = Arc::clone(transport) as _;
        StreamPeerTable::new(Arc::downgrade(&transport))
    }

    /// Only a peer with an entry for the transport's key reaches it. A
    /// missing entry, an address that does not decode, and a transport
    /// that refuses the peer all leave the messenger's registration alone:
    /// the observer has no error to return.
    #[test]
    fn the_observer_forwards_only_a_peer_with_an_entry_for_its_key() {
        let transport = Recording::new("tcp-stream", WorkerAddress::empty(), false);
        let table = observer(&transport);
        let with_entry = peer(&["tcp", "tcp-stream"]);
        table.registered(&with_entry);
        table.registered(&peer(&["tcp"]));
        table.registered(&PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(Bytes::from_static(b"not msgpack")),
        ));
        table.unregistered(with_entry.instance_id());
        assert_eq!(*transport.registered.lock(), [with_entry.instance_id()]);
        assert_eq!(*transport.unregistered.lock(), [with_entry.instance_id()]);

        let refusing = Recording::new("tcp-stream", WorkerAddress::empty(), true);
        observer(&refusing).registered(&peer(&["tcp-stream"]));
        assert!(refusing.registered.lock().is_empty());
    }

    /// The observer holds the transport weakly, so it outlives it quietly.
    #[test]
    fn the_observer_does_nothing_once_the_transport_is_gone() {
        let transport = Recording::new("tcp-stream", WorkerAddress::empty(), false);
        let table = observer(&transport);
        drop(transport);
        table.registered(&peer(&["tcp-stream"]));
        table.unregistered(InstanceId::new_v4());
    }

    /// The stream endpoint is added to the messenger's address. A transport
    /// with no endpoint of its own adds nothing, and a key that collides
    /// with a messenger entry leaves the messenger's address as it is.
    #[test]
    fn complete_peer_info_adds_the_stream_endpoint_or_keeps_the_messenger_address() {
        let messenger = peer(&["tcp"]);

        let stream = Recording::new("tcp-stream", address(&["tcp-stream"]), false);
        let complete = complete_peer_info(messenger.clone(), stream.as_ref());
        assert_eq!(complete.instance_id(), messenger.instance_id());
        for key in ["tcp", "tcp-stream"] {
            assert_eq!(
                complete.worker_address().get_entry(key).unwrap(),
                Some(Bytes::from(format!("{key}-endpoint")))
            );
        }

        let mux = Recording::new("messenger-mux", WorkerAddress::empty(), false);
        assert_eq!(
            complete_peer_info(messenger.clone(), mux.as_ref()).worker_address(),
            messenger.worker_address()
        );

        let colliding = Recording::new("tcp", address(&["tcp"]), false);
        assert_eq!(
            complete_peer_info(messenger.clone(), colliding.as_ref()).worker_address(),
            messenger.worker_address()
        );
    }
}
