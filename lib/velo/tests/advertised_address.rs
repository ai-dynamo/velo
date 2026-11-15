// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A transport that advertises an address and port it does not bind.
//!
//! Each node sits behind its own proxy, which listens on a port of its own
//! and forwards to the node's bound socket. Each node advertises its proxy's
//! address, so every connection a peer dials goes through the proxy. A
//! request travels through the server's proxy and its response through the
//! client's, because each direction dials its own connection. The proxies
//! count the bytes they forward, so a request that went around them shows up
//! as a zero.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velo::transports::tcp::TcpTransportBuilder;
use velo::*;

/// Forwards every connection on its listener to `target`, and counts the
/// bytes it forwards in each direction.
struct TcpProxy {
    addr: SocketAddr,
    /// Dialer to target.
    forward: Arc<AtomicU64>,
    /// Target to dialer.
    backward: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl TcpProxy {
    /// Bind the proxy now, so its address can be advertised before the
    /// target exists; forward to the address `target` yields.
    async fn bind(target: tokio::sync::oneshot::Receiver<SocketAddr>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let forward = Arc::new(AtomicU64::new(0));
        let backward = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn({
            let forward = Arc::clone(&forward);
            let backward = Arc::clone(&backward);
            async move {
                let target = target.await.unwrap();
                loop {
                    let Ok((inbound, _)) = listener.accept().await else {
                        return;
                    };
                    let Ok(outbound) = tokio::net::TcpStream::connect(target).await else {
                        continue;
                    };
                    let (in_read, in_write) = inbound.into_split();
                    let (out_read, out_write) = outbound.into_split();
                    tokio::spawn(pump(in_read, out_write, Arc::clone(&forward)));
                    tokio::spawn(pump(out_read, in_write, Arc::clone(&backward)));
                }
            }
        });
        Self {
            addr,
            forward,
            backward,
            task,
        }
    }

    fn forwarded(&self) -> (u64, u64) {
        (
            self.forward.load(Ordering::Relaxed),
            self.backward.load(Ordering::Relaxed),
        )
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    counter: Arc<AtomicU64>,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                counter.fetch_add(n as u64, Ordering::Relaxed);
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = to.shutdown().await;
}

/// A node on a TCP transport bound to its own port, advertising `proxy`.
async fn tcp_node_behind(
    proxy: &TcpProxy,
    target: tokio::sync::oneshot::Sender<SocketAddr>,
) -> Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bound = listener.local_addr().unwrap();
    assert_ne!(bound, proxy.addr);
    target.send(bound).unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .advertise_addrs([proxy.addr])
        .build()
        .unwrap();
    Velo::builder()
        .add_transport(Arc::new(transport))
        .build()
        .await
        .unwrap()
}

async fn ping_both_ways(server: &Velo, client: &Velo) {
    let ping = Handler::unary_handler("ping", |ctx| Ok(Some(ctx.payload))).build();
    server.register_handler(ping).unwrap();
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();

    let payload = Bytes::from_static(b"through the proxy");
    let echoed: Bytes = tokio::time::timeout(
        Duration::from_secs(10),
        client
            .unary("ping")
            .unwrap()
            .raw_payload(payload.clone())
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("the request completes")
    .unwrap();
    assert_eq!(echoed, payload);
}

/// Two nodes, each behind its own proxy and advertising it: the request
/// crosses the server's proxy and the response the client's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_nodes_reach_each_other_through_their_advertised_proxies() {
    let (server_target, server_bound) = tokio::sync::oneshot::channel();
    let (client_target, client_bound) = tokio::sync::oneshot::channel();
    let server_proxy = TcpProxy::bind(server_bound).await;
    let client_proxy = TcpProxy::bind(client_bound).await;
    let server = tcp_node_behind(&server_proxy, server_target).await;
    let client = tcp_node_behind(&client_proxy, client_target).await;

    ping_both_ways(&server, &client).await;

    let (to_server, _) = server_proxy.forwarded();
    let (to_client, _) = client_proxy.forwarded();
    assert!(
        to_server > 0,
        "the request did not cross the server's proxy"
    );
    assert!(
        to_client > 0,
        "the response did not cross the client's proxy"
    );

    client
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
        .await;
    server
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
        .await;
}

/// The advertised address is the one in the `WorkerAddress`, port included,
/// and the bound port is not in it.
#[test]
fn tcp_advertises_the_given_address_and_port() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bound = listener.local_addr().unwrap();
    let advertised: SocketAddr = "10.1.2.3:45678".parse().unwrap();
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .advertise_addrs([advertised])
        .build()
        .unwrap();
    let raw = transport
        .address()
        .get_entry(transport.key())
        .unwrap()
        .unwrap();
    let endpoints: Vec<velo::transports::InterfaceEndpoint> = rmp_serde::from_slice(&raw).unwrap();
    let addrs: Vec<SocketAddr> = endpoints.iter().filter_map(|ep| ep.socket_addr()).collect();
    assert_eq!(addrs, vec![advertised]);
    assert!(!addrs.contains(&bound));
}

#[cfg(feature = "quic")]
mod quic {
    use std::collections::HashMap;

    use velo::transports::quic::{QuicEndpointInfo, QuicTransportBuilder};
    use velo_ext::MessageType;

    use super::*;

    /// Relays datagrams to `target`. Each client source gets its own
    /// upstream socket, so replies find their way back. Counts the bytes it
    /// relays toward the target, and the distinct sources it saw.
    struct UdpRelay {
        addr: SocketAddr,
        forward: Arc<AtomicU64>,
        sources: Arc<parking_lot::Mutex<Vec<SocketAddr>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl UdpRelay {
        async fn bind(target: tokio::sync::oneshot::Receiver<SocketAddr>) -> Self {
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let addr = socket.local_addr().unwrap();
            let forward = Arc::new(AtomicU64::new(0));
            let sources = Arc::new(parking_lot::Mutex::new(Vec::new()));
            let task = tokio::spawn({
                let forward = Arc::clone(&forward);
                let sources = Arc::clone(&sources);
                async move {
                    let target = target.await.unwrap();
                    let mut upstreams: HashMap<SocketAddr, Arc<tokio::net::UdpSocket>> =
                        HashMap::new();
                    let mut buf = vec![0u8; 65536];
                    loop {
                        let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                            return;
                        };
                        let upstream = match upstreams.get(&from) {
                            Some(upstream) => Arc::clone(upstream),
                            None => {
                                let upstream = Arc::new(
                                    tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                                );
                                upstream.connect(target).await.unwrap();
                                sources.lock().push(from);
                                upstreams.insert(from, Arc::clone(&upstream));
                                let (reply_to, upstream_reader, front) =
                                    (from, Arc::clone(&upstream), Arc::clone(&socket));
                                tokio::spawn(async move {
                                    let mut buf = vec![0u8; 65536];
                                    while let Ok(n) = upstream_reader.recv(&mut buf).await {
                                        let _ = front.send_to(&buf[..n], reply_to).await;
                                    }
                                });
                                upstream
                            }
                        };
                        forward.fetch_add(n as u64, Ordering::Relaxed);
                        let _ = upstream.send(&buf[..n]).await;
                    }
                }
            });
            Self {
                addr,
                forward,
                sources,
                task,
            }
        }
    }

    impl Drop for UdpRelay {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// A started QUIC transport with four server sockets and `lanes` lanes,
    /// advertising `relay`. The relay forwards to the first server socket
    /// only, so a lane that dialed any other socket would not get through.
    async fn quic_behind(
        relay: &UdpRelay,
        target: tokio::sync::oneshot::Sender<SocketAddr>,
        lanes: u16,
    ) -> (
        velo::transports::quic::QuicTransport,
        velo_ext::DataStreams,
        InstanceId,
    ) {
        let transport = QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .server_endpoints(4)
            .lanes(lanes)
            .advertise_addrs([relay.addr])
            .build()
            .unwrap();
        let raw = transport
            .address()
            .get_entry(transport.key())
            .unwrap()
            .unwrap();
        let info = QuicEndpointInfo::decode(&raw).unwrap();
        assert_eq!(
            info.endpoints
                .iter()
                .filter_map(|ep| ep.socket_addr())
                .collect::<Vec<_>>(),
            vec![relay.addr]
        );
        assert!(
            info.ports.is_empty(),
            "per-socket ports would send lanes around the relay"
        );
        let (adapter, streams) = velo_ext::make_channels();
        let id = InstanceId::new_v4();
        transport
            .start(id, adapter, tokio::runtime::Handle::current())
            .await
            .unwrap();
        target.send(transport.bound_addr()).unwrap();
        (transport, streams, id)
    }

    struct Ignore;
    impl velo_ext::TransportErrorHandler for Ignore {
        fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
            panic!("send failed: {error}");
        }
    }

    /// Two QUIC transports, each behind its own relay. A frame on each of
    /// the client's two lanes reaches the server, and a reply reaches the
    /// client, all through the relays; the server's relay sees one source per
    /// lane, so every lane dialed the advertised port.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quic_transports_reach_each_other_through_their_advertised_relays() {
        let (server_target, server_bound) = tokio::sync::oneshot::channel();
        let (client_target, client_bound) = tokio::sync::oneshot::channel();
        let server_relay = UdpRelay::bind(server_bound).await;
        let client_relay = UdpRelay::bind(client_bound).await;
        let (server, server_streams, server_id) =
            quic_behind(&server_relay, server_target, 1).await;
        let (client, client_streams, client_id) =
            quic_behind(&client_relay, client_target, 2).await;
        client
            .register(PeerInfo::new(server_id, server.address()))
            .unwrap();
        server
            .register(PeerInfo::new(client_id, client.address()))
            .unwrap();

        for lane in 0..2u16 {
            let _ = client.send_message_on_lane(
                server_id,
                lane,
                Bytes::from_static(b"h"),
                Bytes::from(vec![lane as u8; 4]),
                MessageType::Event,
                Arc::new(Ignore),
            );
        }
        let mut lanes_seen = Vec::new();
        for _ in 0..2 {
            let (_, payload) = tokio::time::timeout(
                Duration::from_secs(10),
                server_streams.event_stream.recv_async(),
            )
            .await
            .expect("each lane's frame arrives through the relay")
            .unwrap();
            lanes_seen.push(payload[0]);
        }
        lanes_seen.sort_unstable();
        assert_eq!(lanes_seen, vec![0, 1]);

        let _ = server.send_message(
            client_id,
            Bytes::from_static(b"h"),
            Bytes::from_static(b"reply"),
            MessageType::Event,
            Arc::new(Ignore),
        );
        let (_, payload) = tokio::time::timeout(
            Duration::from_secs(10),
            client_streams.event_stream.recv_async(),
        )
        .await
        .expect("the reply arrives through the client's relay")
        .unwrap();
        assert_eq!(&payload[..], b"reply");

        assert!(server_relay.forward.load(Ordering::Relaxed) > 0);
        assert!(client_relay.forward.load(Ordering::Relaxed) > 0);
        let sources = server_relay.sources.lock().clone();
        assert_eq!(
            sources.len(),
            2,
            "each of the client's two lanes dials the relay: {sources:?}"
        );

        client.shutdown();
        server.shutdown();
    }
}
