// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Which transport carries each frame between two nodes that both have TCP
//! and UDS, read from `velo_transport_frames_total` on both nodes.
//!
//! A send that names a transport uses it for the request, the reply, and
//! the `_hello` before it. A send that names none uses the sender's primary,
//! the first transport it added, and its reply comes back over the transport
//! the request came in on.
//!
//! The wait after a call is on the counts: every frame one node counted out
//! must be counted in by the other, per transport and message type.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use velo::observability::VeloMetrics;
use velo::transports::tcp::TcpTransportBuilder;
use velo::transports::uds::UdsTransportBuilder;
use velo::{
    Handler, PeerProbeError, ShutdownPolicy, Transport, TransportChoiceError, TransportKey,
    TypedContext, Velo,
};

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum Kind {
    Tcp,
    Uds,
}

struct Node {
    velo: Velo,
    reg: Registry,
    notes: Arc<AtomicU64>,
}

#[derive(Serialize, Deserialize)]
struct Add {
    a: i32,
    b: i32,
}

fn transport(kind: Kind, dir: &tempfile::TempDir) -> Arc<dyn Transport> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    match kind {
        Kind::Tcp => Arc::new(
            TcpTransportBuilder::new()
                .from_listener(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
                .unwrap()
                .build()
                .unwrap(),
        ),
        Kind::Uds => Arc::new(
            UdsTransportBuilder::new()
                .socket_path(
                    dir.path()
                        .join(format!("{}.sock", NEXT.fetch_add(1, Ordering::Relaxed))),
                )
                .build()
                .unwrap(),
        ),
    }
}

async fn node(dir: &tempfile::TempDir, kinds: &[Kind]) -> Node {
    let reg = Registry::new();
    let mut builder = Velo::builder()
        .mux_only()
        .metrics(Arc::new(VeloMetrics::register(&reg).unwrap()));
    for kind in kinds {
        builder = builder.add_transport(transport(*kind, dir));
    }
    let velo = builder.build().await.unwrap();
    let notes = Arc::new(AtomicU64::new(0));
    velo.register_handler(
        Handler::unary_handler("echo_len", |c| {
            Ok(Some(Bytes::from(c.payload.len().to_le_bytes().to_vec())))
        })
        .build(),
    )
    .unwrap();
    velo.register_handler(
        Handler::typed_unary("add", |c: TypedContext<Add>| Ok(c.input.a + c.input.b)).build(),
    )
    .unwrap();
    let seen = notes.clone();
    velo.register_handler(
        Handler::am_handler("note", move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .build(),
    )
    .unwrap();
    Node { velo, reg, notes }
}

/// Two nodes, each registered with the other.
async fn pair(dir: &tempfile::TempDir, a: &[Kind], b: &[Kind]) -> (Node, Node) {
    let a = node(dir, a).await;
    let b = node(dir, b).await;
    a.velo.register_peer(b.velo.peer_info()).unwrap();
    b.velo.register_peer(a.velo.peer_info()).unwrap();
    (a, b)
}

async fn shutdown(nodes: [&Node; 2]) {
    for n in nodes {
        n.velo
            .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}

/// (transport, direction, message_type) -> frames.
type Cells = BTreeMap<(String, String, String), u64>;

fn frames(reg: &Registry) -> Cells {
    let mut cells = Cells::new();
    for family in reg.gather() {
        if family.name() != "velo_transport_frames_total" {
            continue;
        }
        for metric in family.get_metric() {
            let label = |k: &str| {
                metric
                    .get_label()
                    .iter()
                    .find(|l| l.name() == k)
                    .map(|l| l.value().to_string())
                    .unwrap_or_default()
            };
            *cells
                .entry((
                    label("transport"),
                    label("direction"),
                    label("message_type"),
                ))
                .or_default() += metric.get_counter().value() as u64;
        }
    }
    cells
}

/// Frames per (transport, message type) that `from` sent to `to` since the
/// snapshots, once every one of them has been counted in on the other side.
struct Traffic {
    a_to_b: BTreeMap<(String, String), u64>,
    b_to_a: BTreeMap<(String, String), u64>,
}

impl Traffic {
    /// The transports that carried anything, either way.
    fn transports(&self) -> Vec<String> {
        let mut used: Vec<String> = self
            .a_to_b
            .keys()
            .chain(self.b_to_a.keys())
            .map(|(t, _)| t.clone())
            .collect();
        used.sort();
        used.dedup();
        used
    }
}

/// Run `op`, then wait until both nodes agree on every frame it caused.
async fn traffic<F: std::future::Future<Output = ()>>(
    a: &Registry,
    b: &Registry,
    op: F,
) -> Traffic {
    let (a0, b0) = (frames(a), frames(b));
    let deadline = tokio::time::Instant::now() + DEADLINE;
    tokio::time::timeout_at(deadline, op)
        .await
        .expect("the traffic operation timed out");
    let delta = |now: &Cells, then: &Cells, dir: &str| -> BTreeMap<(String, String), u64> {
        now.iter()
            .filter(|((_, d, _), _)| d == dir)
            .filter_map(|((t, _, m), v)| {
                let d = v - then
                    .get(&(t.clone(), dir.to_string(), m.clone()))
                    .unwrap_or(&0);
                (d > 0).then(|| ((t.clone(), m.clone()), d))
            })
            .collect()
    };
    tokio::time::timeout_at(deadline, async {
        loop {
            let (a1, b1) = (frames(a), frames(b));
            let traffic = Traffic {
                a_to_b: delta(&a1, &a0, "outbound"),
                b_to_a: delta(&b1, &b0, "outbound"),
            };
            if traffic.a_to_b == delta(&b1, &b0, "inbound")
                && traffic.b_to_a == delta(&a1, &a0, "inbound")
            {
                return traffic;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the two nodes never agreed on the frames")
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "the traffic operation timed out")]
async fn a_missing_reply_times_out_before_the_frame_check() {
    let (a, b) = (Registry::new(), Registry::new());
    traffic(&a, &b, tokio::time::sleep(DEADLINE * 2)).await;
}

/// The four operations, A to B, each naming `key` or none.
async fn four_ops(a: &Node, b: &Node, key: Option<&str>) {
    let id = b.velo.instance_id();
    let route = key.map(|k| a.velo.transport_route(k).unwrap());
    macro_rules! pin {
        ($builder:expr) => {
            match route {
                Some(route) => $builder.route(route),
                None => $builder,
            }
        };
    }
    let reply = pin!(a.velo.unary("echo_len").unwrap())
        .raw_payload(Bytes::from_static(b"ping"))
        .instance(id)
        .send()
        .await
        .unwrap();
    assert_eq!(&reply[..], &4usize.to_le_bytes());
    let sum: i32 = pin!(a.velo.typed_unary::<i32>("add").unwrap())
        .payload(&Add { a: 2, b: 3 })
        .unwrap()
        .instance(id)
        .send()
        .await
        .unwrap();
    assert_eq!(sum, 5);
    pin!(a.velo.am_sync("note").unwrap())
        .raw_payload(Bytes::new())
        .instance(id)
        .send()
        .await
        .unwrap();
    let before = b.notes.load(Ordering::SeqCst);
    pin!(a.velo.am_send("note").unwrap())
        .raw_payload(Bytes::new())
        .instance(id)
        .send()
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        while b.notes.load(Ordering::SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

/// A send that names a transport keeps all of its traffic there: the
/// `_hello` of the first call, every request, and every reply. The other
/// transport carries nothing, in either direction. Both orders of naming,
/// so neither transport passes by being the primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn named_sends_and_their_replies_stay_on_the_named_transport() {
    let dir = tempfile::tempdir().unwrap();
    for (named, other) in [("uds", "tcp"), ("tcp", "uds")] {
        let (a, b) = pair(&dir, &[Kind::Tcp, Kind::Uds], &[Kind::Tcp, Kind::Uds]).await;
        let seen = traffic(&a.reg, &b.reg, four_ops(&a, &b, Some(named))).await;
        assert_eq!(seen.transports(), vec![named.to_string()], "naming {named}");
        // `_hello`, unary, typed unary, am_sync, am_send.
        assert_eq!(seen.a_to_b.get(&(named.into(), "message".into())), Some(&5));
        // Three responses and one ack; `am_send` has no reply.
        assert_eq!(
            seen.b_to_a.get(&(named.into(), "response".into())),
            Some(&3)
        );
        assert_eq!(seen.b_to_a.get(&(named.into(), "ack".into())), Some(&1));
        assert!(!seen.transports().contains(&other.to_string()));
        shutdown([&a, &b]).await;
    }
}

/// A send that names nothing uses the sender's first transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unnamed_send_uses_the_first_transport_added() {
    let dir = tempfile::tempdir().unwrap();
    for (order, primary) in [
        ([Kind::Tcp, Kind::Uds], "tcp"),
        ([Kind::Uds, Kind::Tcp], "uds"),
    ] {
        let (a, b) = pair(&dir, &order, &order).await;
        let seen = traffic(&a.reg, &b.reg, four_ops(&a, &b, None)).await;
        assert_eq!(seen.transports(), vec![primary.to_string()]);
        shutdown([&a, &b]).await;
    }
}

/// When the two ends added their transports in different orders, a reply
/// comes back over the transport its request came in on, not over the
/// responder's own first transport. Before replies followed requests, A's
/// TCP requests got UDS replies here, so an edge could not be kept on one
/// transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replies_follow_the_request_when_the_ends_disagree_on_the_primary() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = pair(&dir, &[Kind::Tcp, Kind::Uds], &[Kind::Uds, Kind::Tcp]).await;
    let a_calls = traffic(&a.reg, &b.reg, four_ops(&a, &b, None)).await;
    assert_eq!(a_calls.transports(), vec!["tcp".to_string()]);
    let b_calls = traffic(&b.reg, &a.reg, four_ops(&b, &a, None)).await;
    assert_eq!(b_calls.transports(), vec!["uds".to_string()]);
    shutdown([&a, &b]).await;
}

/// What a caller can learn before it sends, and the typed errors of a choice
/// that cannot be met. None of them falls back to another transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_can_refuse_an_edge_before_it_sends() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = pair(&dir, &[Kind::Tcp, Kind::Uds], &[Kind::Tcp]).await;
    let id = b.velo.instance_id();
    let key = |k: &str| TransportKey::from(k);
    assert_eq!(a.velo.transport_keys(), vec![key("tcp"), key("uds")]);
    assert_eq!(a.velo.peer_transports(id), Some(vec![key("tcp")]));
    assert_eq!(
        a.velo.transport_route("quic"),
        Err(TransportChoiceError::NotInstalled {
            transport: key("quic")
        })
    );

    let tcp = a.velo.transport_route("tcp").unwrap();
    let uds = a.velo.transport_route("uds").unwrap();
    assert!(a.velo.route_capacity(id, tcp).unwrap().is_some());
    let not_served = TransportChoiceError::NotServedByPeer {
        peer: id,
        transport: key("uds"),
    };
    assert_eq!(a.velo.route_capacity(id, uds), Err(not_served.clone()));

    let seen = traffic(&a.reg, &b.reg, async {
        let error = a
            .velo
            .unary("echo_len")
            .unwrap()
            .route(uds)
            .raw_payload(Bytes::new())
            .instance(id)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<TransportChoiceError>(),
            Some(&not_served)
        );
        assert!(matches!(
            a.velo.probe_peer(id, uds, Duration::from_secs(1)).await,
            Err(PeerProbeError::Choice(e)) if e == not_served
        ));
    })
    .await;
    assert!(seen.transports().is_empty(), "a refused send sends nothing");

    // A route belongs to the node that made it.
    let foreign = b.velo.transport_route("tcp").unwrap();
    let error = a
        .velo
        .am_send("note")
        .unwrap()
        .route(foreign)
        .raw_payload(Bytes::new())
        .instance(id)
        .send()
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(TransportChoiceError::ForeignRoute { .. })
    ));
    shutdown([&a, &b]).await;
}

#[cfg(feature = "test-helpers")]
#[tokio::test]
async fn a_discovered_peer_missing_the_named_transport_takes_no_slot() {
    use futures::future::BoxFuture;
    use tokio::sync::Notify;
    use velo::ext::discovery::PeerDiscovery;
    use velo::{InstanceId, PeerInfo, WorkerId};

    struct Discovery {
        peer: PeerInfo,
        entered: Notify,
        released: Notify,
    }

    impl PeerDiscovery for Discovery {
        fn discover_by_worker_id(
            &self,
            worker: WorkerId,
        ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
            Box::pin(async move {
                assert_eq!(worker, self.peer.instance_id().worker_id());
                self.entered.notify_one();
                self.released.notified().await;
                Ok(self.peer.clone())
            })
        }

        fn discover_by_instance_id(
            &self,
            _: InstanceId,
        ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
            unreachable!("the call resolves a worker ID");
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let peer = node(&dir, &[Kind::Tcp]).await;
    let id = peer.velo.instance_id();
    let discovery = Arc::new(Discovery {
        peer: peer.velo.peer_info(),
        entered: Notify::new(),
        released: Notify::new(),
    });
    let sender = Velo::builder()
        .mux_only()
        .add_transport(transport(Kind::Tcp, &dir))
        .add_transport(transport(Kind::Uds, &dir))
        .discovery(discovery.clone())
        .build()
        .await
        .unwrap();
    let call = sender
        .unary("echo_len")
        .unwrap()
        .transport("uds")
        .worker(id.worker_id())
        .send();
    tokio::pin!(call);
    tokio::select! {
        _ = discovery.entered.notified() => {}
        result = &mut call => panic!("call ended before discovery: {result:?}"),
        _ = tokio::time::sleep(DEADLINE) => panic!("discovery did not start"),
    }
    let slots = sender.response_slots_in_use();
    discovery.released.notify_one();
    let error = tokio::time::timeout(DEADLINE, call)
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::NotServedByPeer {
            peer: id,
            transport: TransportKey::from("uds"),
        }),
    );
    assert_eq!(slots, 0, "the route refusal held a slot during discovery");
}

#[path = "transport_choice/capped_fetch.rs"]
mod capped_fetch;

#[tokio::test]
async fn a_named_send_cannot_request_staging_through_its_headers() {
    let dir = tempfile::tempdir().unwrap();
    let (sender, receiver) = pair(&dir, &[Kind::Tcp, Kind::Uds], &[Kind::Tcp, Kind::Uds]).await;
    let len = 300 * 1024;
    let handle = sender.velo.register_data(Bytes::from(vec![1; len]));
    let mut response = None;
    let seen = traffic(&sender.reg, &receiver.reg, async {
        response = Some(
            sender
                .velo
                .unary("echo_len")
                .unwrap()
                .headers(std::collections::HashMap::from([(
                    "_rv".to_string(),
                    handle.as_u128().to_string(),
                )]))
                .raw_payload(Bytes::from_static(b"inline"))
                .transport("uds")
                .instance(receiver.velo.instance_id())
                .timeout(DEADLINE)
                .send()
                .await,
        );
    })
    .await;
    let error = response.unwrap().unwrap_err();
    assert_eq!(
        error.downcast_ref::<TransportChoiceError>(),
        Some(&TransportChoiceError::StagingHeader {
            transport: TransportKey::from("uds"),
        }),
    );
    assert_eq!(
        seen.transports(),
        Vec::<String>::new(),
        "a refused send put frames on a transport",
    );
    shutdown([&sender, &receiver]).await;
}
