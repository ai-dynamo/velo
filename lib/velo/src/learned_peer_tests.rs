// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Every way a node learns a peer must reach the per-stream transport.
//!
//! The messenger backend owns the peer table, and the per-stream transport
//! keeps a second table of its own: the endpoint it dials for each peer. A
//! peer learned by `_hello`, by send-time discovery, or by
//! `Messenger::register_peer` used to reach only the first table. With the mux
//! off, an attach to such a peer then failed with "TCP streaming: peer ... not
//! registered", though messages to it worked. These tests learn a peer each
//! way, with the mux off, and attach to it.
//!
//! The rolling-upgrade tests cover both mixed pairs. A 0.22.0 node sends a
//! `_hello` with no stream endpoint, and its receiver must still answer it.
//! A new node sends one more address entry, and a 0.22.0 receiver must accept
//! it.

use super::*;
use crate::messenger::server::system_handlers::{HandlersResponse, HelloRequest};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::time::Duration;

const STREAM_KEY: &str = "tcp-stream";

/// A discovery backend that knows one peer.
struct OnePeer(PeerInfo);

impl PeerDiscovery for OnePeer {
    fn discover_by_worker_id(&self, worker: WorkerId) -> BoxFuture<'_, Result<PeerInfo>> {
        Box::pin(async move {
            anyhow::ensure!(worker == self.0.worker_id(), "unknown worker {worker}");
            Ok(self.0.clone())
        })
    }

    fn discover_by_instance_id(&self, instance: InstanceId) -> BoxFuture<'_, Result<PeerInfo>> {
        Box::pin(async move {
            anyhow::ensure!(
                instance == self.0.instance_id(),
                "unknown instance {instance}"
            );
            Ok(self.0.clone())
        })
    }
}

fn tcp_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

/// A node with a loopback per-stream TCP listener, the mux on or off.
async fn node(discovery: Option<PeerInfo>, mux: bool) -> Velo {
    let mut builder = Velo::builder()
        .add_transport(tcp_transport())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .messenger_mux(crate::streaming::MuxConfig {
            enabled: mux,
            ..Default::default()
        })
        .unwrap();
    if let Some(peer) = discovery {
        builder = builder.discovery(Arc::new(OnePeer(peer)));
    }
    builder.build().await.unwrap()
}

/// A messenger with no `Velo` around it. It installs no stream transport
/// and advertises messenger endpoints only, which is what every 0.22.0 node
/// sends in `_hello`.
#[allow(deprecated)] // Messenger::builder is deprecated in 0.22.1; a 0.22.0-style node needs it.
async fn messenger_only() -> Arc<Messenger> {
    Messenger::builder()
        .add_transport(tcp_transport())
        .build()
        .await
        .unwrap()
}

/// Whether `node`'s per-stream transport has an endpoint for `peer`.
fn stream_knows(node: &Velo, peer: InstanceId) -> bool {
    match &node.inner.stream_owner.transport {
        Some(OwnedStreamTransport::Tcp(transport)) => transport.knows_peer(peer.worker_id()),
        _ => unreachable!("these tests build per-stream TCP nodes"),
    }
}

async fn ping(from: &Velo, to: &Velo, by_worker: bool) {
    to.register_handler(Handler::unary_handler("ping", |_| Ok(Some(Bytes::new()))).build())
        .unwrap();
    let builder = from.unary("ping").unwrap().raw_payload(Bytes::new());
    let builder = if by_worker {
        builder.worker(to.instance_id().worker_id())
    } else {
        builder.instance(to.instance_id())
    };
    tokio::time::timeout(Duration::from_secs(5), builder.send())
        .await
        .expect("ping timed out")
        .expect("ping failed");
}

async fn close(nodes: &[&Velo]) {
    for node in nodes {
        node.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}

#[derive(Clone, Copy, Debug)]
enum Learned {
    /// `Velo::register_peer`, which reached the stream transport before.
    Velo,
    /// `Messenger::register_peer`.
    Messenger,
    /// The `_hello` the producer sends on its first message to the consumer.
    Hello,
    /// Discovery of the consumer when the producer first sends to its worker id.
    Discovery,
}

/// The producer learns the consumer by `path`, then attaches to an anchor
/// the consumer created. The attach dials the consumer through the
/// producer's stream transport, so it works only if that transport learned
/// the consumer too.
#[allow(deprecated)] // Messenger::register_peer is deprecated in 0.22.1; this test calls it on purpose.
async fn attach_after_learning(path: Learned, mux: bool) {
    let consumer = node(None, mux).await;
    let discovery = matches!(path, Learned::Discovery).then(|| consumer.peer_info());
    let producer = node(discovery, mux).await;
    match path {
        Learned::Velo => producer.register_peer(consumer.peer_info()).unwrap(),
        Learned::Messenger => producer
            .messenger()
            .register_peer(consumer.peer_info())
            .unwrap(),
        Learned::Hello | Learned::Discovery => {}
    }
    match path {
        // The consumer answers an attach through its own peer table, so it
        // learns the producer from the `_hello` of this first message.
        Learned::Velo | Learned::Messenger => ping(&producer, &consumer, false).await,
        Learned::Hello => {
            consumer.register_peer(producer.peer_info()).unwrap();
            // The consumer's first message carries its `_hello`.
            ping(&consumer, &producer, false).await;
        }
        Learned::Discovery => ping(&producer, &consumer, true).await,
    }
    assert!(
        producer
            .messenger()
            .backend()
            .is_registered(consumer.instance_id()),
        "{path:?}: the messenger learned the consumer"
    );
    if !mux {
        assert!(
            stream_knows(&producer, consumer.instance_id()),
            "{path:?}: the stream transport learned the consumer"
        );
    }
    let anchor = consumer.create_anchor::<u32>();
    let attached = tokio::time::timeout(
        Duration::from_secs(5),
        producer.attach_anchor::<u32>(anchor.handle()),
    )
    .await
    .expect("attach timed out");
    let outcome = attached
        .as_ref()
        .map(|sender| {
            sender
                .negotiated_transport()
                .map(|key| key.as_str().to_string())
        })
        .map_err(|error| format!("{error:#}"));
    drop(attached);
    drop(anchor);
    close(&[&producer, &consumer]).await;
    let expected = if mux {
        crate::streaming::MESSENGER_MUX_KEY
    } else {
        STREAM_KEY
    };
    assert_eq!(
        outcome,
        Ok(Some(expected.to_string())),
        "{path:?}, mux {mux}"
    );
}

#[tokio::test]
async fn hello_registers_the_sender_with_the_stream_transport() {
    attach_after_learning(Learned::Hello, false).await;
}

#[tokio::test]
async fn send_time_discovery_registers_with_the_stream_transport() {
    attach_after_learning(Learned::Discovery, false).await;
}

#[tokio::test]
async fn messenger_register_peer_registers_with_the_stream_transport() {
    attach_after_learning(Learned::Messenger, false).await;
}

/// Control: the path that always reached the stream transport.
#[tokio::test]
async fn velo_register_peer_registers_with_the_stream_transport() {
    attach_after_learning(Learned::Velo, false).await;
}

/// Control: with the mux on, an attach takes the mux, which needs no
/// stream endpoint, so `_hello` learning was always enough.
#[tokio::test]
async fn hello_with_the_mux_attaches_over_the_mux() {
    attach_after_learning(Learned::Hello, true).await;
}

/// `_hello` must carry the sender's stream endpoint. It used to carry the
/// messenger's address only, so the receiver had nothing to register with
/// its stream transport, however it forwarded the peer.
#[tokio::test]
#[allow(deprecated)] // Messenger::register_peer is deprecated in 0.22.1; this handler copies the real `_hello`.
async fn hello_carries_the_stream_endpoint() {
    let sender = node(None, false).await;
    let receiver = node(None, false).await;
    let captured = Arc::new(parking_lot::Mutex::new(None::<PeerInfo>));
    let output = Arc::clone(&captured);
    receiver
        .messenger()
        .register_streaming_handler(
            Handler::typed_unary_async("_hello", move |ctx: TypedContext<HelloRequest>| {
                let output = Arc::clone(&output);
                async move {
                    *output.lock() = Some(ctx.input.peer_info.clone());
                    ctx.msg.register_peer(ctx.input.peer_info)?;
                    Ok(HandlersResponse {
                        handlers: ctx.msg.list_local_handlers(),
                    })
                }
            })
            .spawn()
            .build(),
        )
        .unwrap();
    sender.register_peer(receiver.peer_info()).unwrap();
    ping(&sender, &receiver, false).await;
    let hello = captured.lock().take().expect("the receiver got a _hello");
    close(&[&sender, &receiver]).await;
    let entry = |peer: &PeerInfo| peer.worker_address().get_entry(STREAM_KEY).unwrap();
    assert!(entry(&sender.peer_info()).is_some());
    assert_eq!(entry(&hello), entry(&sender.peer_info()));
    assert_eq!(hello.instance_id(), sender.instance_id());
}

/// Run a `_hello` from `sender` to `receiver` with the real handshake client.
async fn handshake(sender: &Messenger, receiver: InstanceId) -> Result<Vec<String>> {
    tokio::time::timeout(
        Duration::from_secs(5),
        sender.client().get_peer_handlers(receiver),
    )
    .await
    .map_err(|_| anyhow::anyhow!("_hello timed out"))?
}

/// Rolling upgrade, old to new: a 0.22.0 node's `_hello` has no stream
/// endpoint. A new receiver must answer it and register the sender with its
/// messenger transports, and skip its stream transport, which would reject
/// an address with no entry for its key. Forwarding that rejection would
/// leave the handshake unanswered until the sender timed out.
#[tokio::test]
#[allow(deprecated)] // Messenger::{register_peer, graceful_shutdown} are deprecated in 0.22.1; the 0.22.0 sender calls them.
async fn a_hello_without_a_stream_endpoint_still_completes() {
    for mux in [false, true] {
        let old = messenger_only().await;
        let new = node(None, mux).await;
        assert!(
            old.peer_info()
                .worker_address()
                .get_entry(STREAM_KEY)
                .unwrap()
                .is_none(),
            "the 0.22.0 sender advertises no stream endpoint"
        );
        old.register_peer(new.peer_info()).unwrap();
        let handshake = handshake(&old, new.instance_id()).await;
        let messenger_knows = new.messenger().backend().is_registered(old.instance_id());
        let stream_knows = stream_knows(&new, old.instance_id());
        close(&[&new]).await;
        old.graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
        assert!(handshake.is_ok(), "mux {mux}: {handshake:?}");
        assert!(messenger_knows, "mux {mux}");
        assert!(!stream_knows, "mux {mux}: nothing to register");
    }
}

/// Rolling upgrade, new to old: a new node's `_hello` carries one more
/// address entry, its stream endpoint. A receiver whose messenger knows no
/// `tcp-stream` transport, as on every 0.22.0 node, ignores the entry and
/// registers the sender. A plain messenger is that receiver, and so is a
/// mux-only node, whose stream transport has a different key.
#[tokio::test]
#[allow(deprecated)] // Messenger::graceful_shutdown is deprecated in 0.22.1; the 0.22.0 receiver calls it.
async fn a_hello_with_a_stream_endpoint_reaches_a_receiver_without_one() {
    for mux_only in [false, true] {
        let new = node(None, false).await;
        let (receiver, receiver_velo) = if mux_only {
            let velo = Velo::builder()
                .add_transport(tcp_transport())
                .mux_only()
                .build()
                .await
                .unwrap();
            (Arc::clone(velo.messenger()), Some(velo))
        } else {
            (messenger_only().await, None)
        };
        assert!(
            new.peer_info()
                .worker_address()
                .get_entry(STREAM_KEY)
                .unwrap()
                .is_some()
        );
        new.register_peer(receiver.peer_info()).unwrap();
        let handshake = handshake(new.messenger(), receiver.instance_id()).await;
        let registered = receiver.backend().is_registered(new.instance_id());
        close(&[&new]).await;
        match receiver_velo {
            Some(velo) => close(&[&velo]).await,
            None => {
                receiver
                    .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
                    .await
            }
        }
        assert!(handshake.is_ok(), "mux_only {mux_only}: {handshake:?}");
        assert!(registered, "mux_only {mux_only}");
    }
}

/// Unregistering through `Velo` or `Messenger` drops the peer from the
/// stream transport too, and so does it for a peer the node learned by
/// `_hello`, which no caller registered by hand.
#[tokio::test]
#[allow(deprecated)] // Messenger::unregister_peer is deprecated in 0.22.1; this test calls it on purpose.
async fn unregister_reaches_the_stream_transport_on_every_path() {
    for through_messenger in [false, true] {
        let a = node(None, false).await;
        let b = node(None, false).await;
        a.register_peer(b.peer_info()).unwrap();
        // b learns a by a's `_hello`.
        ping(&a, &b, false).await;
        let forget = |node: &Velo, peer: InstanceId| {
            if through_messenger {
                node.messenger().unregister_peer(peer);
            } else {
                node.unregister_peer(peer);
            }
        };
        assert!(stream_knows(&a, b.instance_id()));
        assert!(stream_knows(&b, a.instance_id()));
        forget(&a, b.instance_id());
        forget(&b, a.instance_id());
        let a_knows = stream_knows(&a, b.instance_id());
        let b_knows = stream_knows(&b, a.instance_id());
        close(&[&a, &b]).await;
        assert!(
            !a_knows,
            "messenger path {through_messenger}: registered by hand"
        );
        assert!(
            !b_knows,
            "messenger path {through_messenger}: learned by _hello"
        );
    }
}

/// The stream transport follows the messenger's peer table; it does not
/// veto it. A stream endpoint this node cannot use leaves the peer reachable
/// by messages, out of the stream table, and an attach to it fails as one to
/// a peer never registered. The messenger transports that accepted the peer
/// are what make it registered.
#[tokio::test]
async fn an_unusable_stream_endpoint_does_not_block_messaging() {
    let a = node(None, false).await;
    let b = node(None, false).await;
    let mut address = crate::transports::address::WorkerAddressBuilder::new();
    address
        .merge(b.messenger().peer_info().worker_address())
        .unwrap();
    address
        .add_entry(STREAM_KEY, Bytes::from_static(b"not an endpoint list"))
        .unwrap();
    let broken = PeerInfo::new(b.instance_id(), address.build().unwrap());
    let registered = a.register_peer(broken);
    let stream_knows_b = stream_knows(&a, b.instance_id());
    if registered.is_ok() {
        ping(&a, &b, false).await;
    }
    close(&[&a, &b]).await;
    assert!(registered.is_ok(), "{registered:?}");
    assert!(!stream_knows_b);
}
