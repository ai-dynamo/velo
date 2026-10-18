// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Endpoint, Handler, ShutdownPolicy, StreamFrame, Velo};

async fn tcp_node(mux_only: bool) -> (Arc<Velo>, std::net::SocketAddr) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let builder = Velo::builder().add_transport(Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    ));
    let builder = if mux_only {
        builder.mux_only()
    } else {
        builder
    };
    (builder.build().await.unwrap(), address)
}

async fn prove_round_trip(client: &Velo, server: &Velo, endpoint: Endpoint) {
    for node in [client, server] {
        node.register_handler(Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build())
            .unwrap();
    }
    // Repeated connects must retain the real route, not a temporary identity.
    for _ in 0..3 {
        let peer = tokio::time::timeout(Duration::from_secs(5), client.connect(endpoint.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer.instance_id(), server.instance_id());
        let actual = peer.worker_address().available_transports().unwrap();
        let expected = server
            .peer_info()
            .worker_address()
            .available_transports()
            .unwrap();
        assert_eq!(
            actual
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }
    for (from, to) in [(client, server), (server, client)] {
        let echoed = tokio::time::timeout(Duration::from_secs(5), async {
            from.unary("echo")
                .unwrap()
                .raw_payload(Bytes::from_static(b"endpoint"))
                .instance(to.instance_id())
                .send()
                .await
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(echoed, Bytes::from_static(b"endpoint"));

        let mut anchor = to.create_anchor::<u32>();
        let sender = tokio::time::timeout(
            Duration::from_secs(5),
            from.attach_anchor::<u32>(anchor.handle()),
        )
        .await
        .unwrap()
        .unwrap();
        sender.send(42).await.unwrap();
        sender.finalize().unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), anchor.next())
            .await
            .unwrap();
        assert!(matches!(frame, Some(Ok(StreamFrame::Item(42)))));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_endpoint_bootstraps_messaging_and_streams() {
    for mux_only in [false, true] {
        let (server, address) = tcp_node(mux_only).await;
        let (client, _) = tcp_node(mux_only).await;
        prove_round_trip(&client, &server, Endpoint::Tcp(address)).await;
        client
            .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
            .await;
        server
            .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
            .await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uds_endpoint_bootstraps_messaging_and_streams() {
    use velo::transports::uds::UdsTransportBuilder;
    let directory = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    for name in ["client", "server"] {
        let path = directory.path().join(name);
        let node = Velo::builder()
            .add_transport(Arc::new(
                UdsTransportBuilder::new()
                    .socket_path(&path)
                    .build()
                    .unwrap(),
            ))
            .mux_only()
            .build()
            .await
            .unwrap();
        nodes.push((node, path));
    }
    prove_round_trip(&nodes[0].0, &nodes[1].0, Endpoint::Uds(nodes[1].1.clone())).await;
    for (node, _) in nodes {
        node.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
            .await;
    }
}

#[cfg(unix)]
#[test]
fn uds_endpoint_rejects_lossy_paths() {
    use std::os::unix::ffi::OsStringExt;
    let path = std::path::PathBuf::from(std::ffi::OsString::from_vec(vec![b'/', 0xff]));
    assert!(velo::transports::uds::UdsTransport::worker_address(&path).is_err());
}
