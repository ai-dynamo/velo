// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Rendezvous between three nodes, one of which has QUIC alone. No send names
//! a transport.
//!
//! A is a remote node with QUIC alone. B and C sit next to each other: each
//! adds TCP, then UCX, then QUIC. A node's primary for a peer is the first
//! transport it added that the peer also has. So B and C use TCP between
//! them, and QUIC to A, the one transport A shares with them.
//!
//! A makes a stream anchor and sends B a large request that carries the
//! anchor's handle. B sends the handle and the payload on to C. C attaches to
//! A's anchor and streams a large item and a small one straight to A. Each
//! large payload is staged, and its receiver fetches it:
//!
//! * A's request: B fetches it from A in chunks over QUIC. B offers no RDMA,
//!   because A has no UCX.
//! * B's call to C: C fetches it from B by RDMA. UCX runs over its tcp lane
//!   here, with software GET on, as in `rendezvous_rdma`.
//! * C's stream item: A fetches it from C in chunks over QUIC.
//!
//! The test reads `velo_transport_frames_total` and
//! `velo_rendezvous_rdma_path_total` on all three nodes. The data arriving
//! proves nothing about the route it took.

#![cfg(all(target_os = "linux", feature = "ucx", feature = "quic"))]

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::transports::quic::QuicTransportBuilder;
use velo::transports::tcp::TcpTransportBuilder;
use velo::transports::ucx::UcxTransportBuilder;
use velo::{
    Context, Handler, InstanceId, ShutdownPolicy, StreamAnchorHandle, StreamFrame, Transport,
    TransportKey, Velo,
};

const T: Duration = Duration::from_secs(20);
/// Over the 256 KiB staging threshold, and over one 512 KiB pull chunk.
const LARGE: usize = 1024 * 1024;
/// The anchor handle at the front of each request, as a little-endian u128.
const HANDLE_LEN: usize = 16;

struct Node {
    velo: Velo,
    reg: Registry,
}

/// A payload whose every byte depends on its offset, so a transfer that lands
/// shifted or short fails. See `rendezvous_rdma::pattern`.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i % 251) ^ ((i / 251) % 257)) as u8)
        .collect()
}

fn quic() -> Arc<dyn Transport> {
    Arc::new(
        QuicTransportBuilder::new()
            .bind_addr("127.0.0.1:0".parse().unwrap())
            .build()
            .unwrap(),
    )
}

async fn build(builder: velo::VeloBuilder, reg: Registry) -> Node {
    let velo = builder
        .mux_only()
        .metrics(Arc::new(VeloMetrics::register(&reg).unwrap()))
        .build()
        .await
        .unwrap();
    Node { velo, reg }
}

/// A node with QUIC alone.
async fn remote() -> Node {
    build(Velo::builder().add_transport(quic()), Registry::new()).await
}

/// A node with TCP, then UCX (and with it the RDMA path), then QUIC.
async fn local() -> Node {
    let tcp = TcpTransportBuilder::new()
        .from_listener(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .unwrap()
        .build()
        .unwrap();
    let ucx = UcxTransportBuilder::new()
        .tls("tcp")
        .allow_software_rma(true)
        .build()
        .unwrap();
    let builder = Velo::builder()
        .add_transport(Arc::new(tcp))
        .add_ucx_transport(Arc::new(ucx))
        .add_transport(quic());
    build(builder, Registry::new()).await
}

async fn introduce(x: &Node, y: &Node) {
    x.velo.register_peer(y.velo.peer_info()).unwrap();
    y.velo.register_peer(x.velo.peer_info()).unwrap();
}

async fn wait_for(from: &Node, to: &Node, handler: &str) {
    tokio::time::timeout(
        T,
        from.velo.wait_for_handler(to.velo.instance_id(), handler),
    )
    .await
    .unwrap_or_else(|_| panic!("{handler} never showed up"))
    .unwrap();
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

/// (transport, message_type) -> frames in one direction since `then`.
type Flow = BTreeMap<(String, String), u64>;

fn flow(now: &Cells, then: &Cells, dir: &str) -> Flow {
    now.iter()
        .filter(|((_, d, _), _)| d == dir)
        .filter_map(|((t, d, m), v)| {
            let moved = v - then.get(&(t.clone(), d.clone(), m.clone())).unwrap_or(&0);
            (moved > 0).then(|| ((t.clone(), m.clone()), moved))
        })
        .collect()
}

/// Only the cells of one transport.
fn on(flow: &Flow, transport: &str) -> Flow {
    flow.iter()
        .filter(|((t, _), _)| t == transport)
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

fn sum(x: &Flow, y: &Flow) -> Flow {
    let mut total = x.clone();
    for (k, v) in y {
        *total.entry(k.clone()).or_default() += v;
    }
    total
}

/// What each node sent and took in since the snapshots.
struct Seen {
    a_out: Flow,
    a_in: Flow,
    b_out: Flow,
    b_in: Flow,
    c_out: Flow,
    c_in: Flow,
}

impl Seen {
    /// Every frame sent has been taken in: per transport and message type,
    /// the three nodes counted as many in as out.
    fn settled(&self) -> bool {
        sum(&sum(&self.a_out, &self.b_out), &self.c_out)
            == sum(&sum(&self.a_in, &self.b_in), &self.c_in)
    }
}

/// Run `op`, then wait until the three nodes agree on every frame it caused.
async fn traffic<F: std::future::Future<Output = ()>>(nodes: [&Node; 3], op: F) -> Seen {
    let before = nodes.map(|n| frames(&n.reg));
    op.await;
    tokio::time::timeout(T, async {
        loop {
            let now = nodes.map(|n| frames(&n.reg));
            let seen = Seen {
                a_out: flow(&now[0], &before[0], "outbound"),
                a_in: flow(&now[0], &before[0], "inbound"),
                b_out: flow(&now[1], &before[1], "outbound"),
                b_in: flow(&now[1], &before[1], "inbound"),
                c_out: flow(&now[2], &before[2], "outbound"),
                c_in: flow(&now[2], &before[2], "inbound"),
            };
            if seen.settled() {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the three nodes never agreed on the frames")
}

/// `velo_rendezvous_rdma_path_total` for one reason label.
fn path(node: &Node, reason: &str) -> u64 {
    node.reg
        .gather()
        .iter()
        .filter(|family| family.name() == "velo_rendezvous_rdma_path_total")
        .flat_map(|family| family.get_metric())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == "reason" && pair.value() == reason)
        })
        .map(|metric| metric.get_counter().value() as u64)
        .sum()
}

/// The transports that carried anything.
fn transports(flows: &[&Flow]) -> Vec<String> {
    let mut used: Vec<String> = flows
        .iter()
        .flat_map(|f| f.keys().map(|(t, _)| t.clone()))
        .collect();
    used.sort();
    used.dedup();
    used
}

/// B passes A's request on to C, with the anchor handle in front.
fn register_forward(b: &Node, c_id: Arc<OnceLock<InstanceId>>) {
    let velo = b.velo.clone();
    b.velo
        .register_handler(
            Handler::unary_handler_async("work", move |ctx: Context| {
                let velo = velo.clone();
                let c_id = c_id.clone();
                async move {
                    let c = *c_id.get().expect("C is known");
                    let reply = velo
                        .unary("produce")?
                        .raw_payload(ctx.payload)
                        .instance(c)
                        .send()
                        .await?;
                    Ok(Some(reply))
                }
            })
            .build(),
        )
        .unwrap();
}

/// C attaches to the anchor in front of the payload and streams the rest to
/// it, then a short item, then the end.
fn register_produce(c: &Node) {
    let velo = c.velo.clone();
    c.velo
        .register_handler(
            Handler::unary_handler_async("produce", move |ctx: Context| {
                let velo = velo.clone();
                async move {
                    let (head, data) = ctx.payload.split_at(HANDLE_LEN);
                    let handle =
                        StreamAnchorHandle::from_u128(u128::from_le_bytes(head.try_into()?));
                    let tx = velo
                        .attach_anchor::<Vec<u8>>(handle)
                        .await
                        .map_err(|e| anyhow::anyhow!("attach: {e:?}"))?;
                    tx.send(data.to_vec())
                        .await
                        .map_err(|e| anyhow::anyhow!("send: {e:?}"))?;
                    tx.send(b"done".to_vec())
                        .await
                        .map_err(|e| anyhow::anyhow!("send: {e:?}"))?;
                    tx.finalize()
                        .map_err(|e| anyhow::anyhow!("finalize: {e:?}"))?;
                    Ok(Some(Bytes::from_static(b"streamed")))
                }
            })
            .build(),
        )
        .unwrap();
}

/// The layout in the module docs, end to end: every frame that touches A is
/// on QUIC, B and C keep their traffic to each other off QUIC, and only the
/// fetch between B and C takes the RDMA path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_only_node_stages_over_quic_while_its_peers_use_rdma_between_them() {
    let a = remote().await;
    let b = local().await;
    let c = local().await;
    let c_id = Arc::new(OnceLock::new());
    c_id.set(c.velo.instance_id()).unwrap();
    register_forward(&b, c_id);
    register_produce(&c);
    introduce(&a, &b).await;
    introduce(&a, &c).await;
    introduce(&b, &c).await;
    for (from, to, handler) in [
        (&a, &b, "work"),
        (&b, &c, "produce"),
        (&b, &a, "_rv_acquire"),
        (&c, &b, "_rv_acquire"),
        (&a, &c, "_rv_acquire"),
        (&c, &a, "_anchor_attach"),
    ] {
        wait_for(from, to, handler).await;
    }

    // The primary of each edge, asserted, so a change in how the primary is
    // picked fails here and not as a puzzle in the counts below.
    let id = |n: &Node| n.velo.instance_id();
    let key = |k: &str| TransportKey::from(k);
    assert_eq!(a.velo.peer_transports(id(&b)), Some(vec![key("quic")]));
    assert_eq!(
        b.velo.primary_transport_key(id(&a)).as_deref(),
        Some("quic")
    );
    assert_eq!(
        c.velo.primary_transport_key(id(&a)).as_deref(),
        Some("quic")
    );
    assert_eq!(
        b.velo.peer_transports(id(&c)),
        Some(vec![key("tcp"), key("ucx"), key("quic")])
    );
    assert_eq!(c.velo.primary_transport_key(id(&b)).as_deref(), Some("tcp"));

    // The transparent stager cuts from a mapped arena only; one pinned
    // registration maps it (see `rendezvous_rdma`). Without it B stages in
    // plain memory and C's fetch is chunked.
    for n in [&b, &c] {
        let warm = n.velo.register_data_pinned(&pattern(4096)).await;
        assert!(n.velo.metadata(warm).await.unwrap().pinned);
    }

    let paths_before = [&a, &b, &c].map(|n| (path(n, "ok"), path(n, "no_offer")));
    let payload = pattern(LARGE);
    let mut anchor = a.velo.create_anchor::<Vec<u8>>();
    let mut request = anchor.handle().as_u128().to_le_bytes().to_vec();
    request.extend_from_slice(&payload);

    let seen = traffic([&a, &b, &c], async {
        let call = a
            .velo
            .unary("work")
            .unwrap()
            .raw_payload(Bytes::from(request))
            .instance(id(&b))
            .send();
        let read = async {
            let mut items = Vec::new();
            loop {
                match tokio::time::timeout(T, anchor.next()).await {
                    Ok(Some(Ok(StreamFrame::Item(item)))) => items.push(item),
                    Ok(None) | Ok(Some(Ok(StreamFrame::Finalized))) => return items,
                    other => panic!("the stream did not end cleanly: {other:?}"),
                }
            }
        };
        let (reply, items) = tokio::join!(call, read);
        assert_eq!(&reply.expect("work")[..], b"streamed");
        assert_eq!(items.len(), 2);
        assert!(
            items[0] == payload,
            "the staged stream item arrived changed"
        );
        assert_eq!(items[1], b"done");
    })
    .await;

    // A has QUIC alone, so its own counts say little. That B and C sent and
    // took in over QUIC exactly what A took in and sent says the rest:
    // nothing between B and C went over QUIC.
    assert_eq!(transports(&[&seen.a_out, &seen.a_in]), vec!["quic"]);
    assert_eq!(
        sum(&on(&seen.b_in, "quic"), &on(&seen.c_in, "quic")),
        on(&seen.a_out, "quic"),
        "B and C took in QUIC frames that A did not send: they used QUIC between them"
    );
    assert_eq!(
        sum(&on(&seen.b_out, "quic"), &on(&seen.c_out, "quic")),
        on(&seen.a_in, "quic"),
        "B and C sent QUIC frames that A did not take in: they used QUIC between them"
    );
    // B's fetch from A: the acquire, at least two chunk pulls, the release.
    let b_to_a = on(&seen.b_out, "quic");
    assert!(
        b_to_a.get(&("quic".into(), "message".into())) >= Some(&4),
        "B fetched A's request off QUIC: {b_to_a:?}"
    );
    // A's request to B, then its fetch from C: the acquire, at least two
    // chunk pulls, the release.
    let a_out = on(&seen.a_out, "quic");
    assert!(
        a_out.get(&("quic".into(), "message".into())) >= Some(&5),
        "A fetched C's stream item off QUIC: {a_out:?}"
    );
    // B's call to C went over TCP, the primary between them; UCX carried no
    // frame, only the RDMA GET.
    assert_eq!(
        transports(&[&seen.b_out, &seen.c_in, &seen.c_out, &seen.b_in]),
        vec!["quic", "tcp"]
    );
    assert!(on(&seen.b_out, "tcp").get(&("tcp".into(), "message".into())) >= Some(&1));

    let paths_after = [&a, &b, &c].map(|n| (path(n, "ok"), path(n, "no_offer")));
    let moved = |i: usize| {
        (
            paths_after[i].0 - paths_before[i].0,
            paths_after[i].1 - paths_before[i].1,
        )
    };
    // B fetched A's request with no offer, and served C's fetch by RDMA.
    assert_eq!(moved(1), (1, 1), "B's (rdma, no offer) decisions");
    // C fetched B's call by RDMA, and served A's fetch with no offer.
    assert_eq!(moved(2), (1, 1), "C's (rdma, no offer) decisions");
    // A has no UCX, so it took the RDMA path for nothing.
    assert_eq!(moved(0).0, 0, "A took the RDMA path");

    for n in [&a, &b, &c] {
        n.velo
            .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
            .await;
    }
}
