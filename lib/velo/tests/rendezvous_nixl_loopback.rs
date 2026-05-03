// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Same-process two-`RendezvousManager` NIXL/RDMA tests.
//!
//! Spins up two messenger pairs over TCP loopback (each with its own
//! [`RendezvousManager`]) and exercises the pinned-register + RDMA-read path.
//!
//! Requires libnixl + UCX at runtime — see `velo-nixl` for env setup.
//! Inside the Claude bash sandbox these tests will fail at agent init with
//! `Failed to create unix domain socket for signal: Operation not permitted`;
//! run them outside the sandbox.

#![cfg(feature = "nixl")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo::messenger::Messenger;
use velo::rendezvous::RendezvousManager;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo_ext::WorkerId;

fn fresh_tcp_transport() -> Arc<TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

struct RvNode {
    messenger: Arc<Messenger>,
    manager: Arc<RendezvousManager>,
}

impl RvNode {
    async fn new() -> Self {
        let transport = fresh_tcp_transport();
        let messenger = Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .unwrap();
        let worker_id = WorkerId::from(messenger.instance_id());
        let manager = Arc::new(RendezvousManager::new(worker_id));
        manager
            .enable_nixl()
            .expect("enable_nixl failed (libnixl + UCX present?)");
        manager
            .register_handlers(Arc::clone(&messenger))
            .expect("register_handlers");
        Self { messenger, manager }
    }
}

async fn connect(a: &RvNode, b: &RvNode) {
    a.messenger.register_peer(b.messenger.peer_info()).unwrap();
    b.messenger.register_peer(a.messenger.peer_info()).unwrap();
    // Let peer connections settle.
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_register_and_remote_get() {
    let owner = RvNode::new().await;
    let consumer = RvNode::new().await;
    connect(&owner, &consumer).await;

    let payload = Bytes::from((0..64u32 * 1024).map(|i| i as u8).collect::<Vec<u8>>());
    let handle = owner
        .manager
        .register_data_pinned(payload.clone())
        .expect("register_data_pinned");

    let (data, lease_id) = consumer
        .manager
        .get(handle)
        .await
        .expect("consumer get over NIXL");
    assert_eq!(data.len(), payload.len());
    assert_eq!(&data[..], &payload[..]);

    consumer
        .manager
        .release(handle, lease_id)
        .await
        .expect("release");
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_local_fast_path_skips_nixl() {
    // Same manager registers and gets — no NIXL transfer should occur, since
    // `RendezvousManager::get` short-circuits when `target_worker == self`.
    let node = RvNode::new().await;

    let payload = Bytes::from(vec![0xCC; 1024]);
    let handle = node.manager.register_data_pinned(payload.clone()).unwrap();

    let (data, lease_id) = node.manager.get(handle).await.unwrap();
    assert_eq!(data.len(), payload.len());
    assert!(data.iter().all(|&b| b == 0xCC));

    node.manager.release(handle, lease_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn pinned_get_without_consumer_nixl_errors_cleanly() {
    // Owner has NIXL enabled; consumer does NOT. The acquire returns
    // `Rdma { .. }`, but `consumer_get` skips the NIXL branch (since the
    // consumer's NIXL endpoint isn't initialized) and falls through to the
    // legacy `Consumer::get` which reports a clean error.
    let owner = RvNode::new().await;

    let consumer = {
        let transport = fresh_tcp_transport();
        let messenger = Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .unwrap();
        let worker_id = WorkerId::from(messenger.instance_id());
        let manager = Arc::new(RendezvousManager::new(worker_id));
        // Note: enable_nixl deliberately NOT called.
        manager.register_handlers(Arc::clone(&messenger)).unwrap();
        RvNode { messenger, manager }
    };

    connect(&owner, &consumer).await;

    let handle = owner
        .manager
        .register_data_pinned(Bytes::from(vec![1u8; 256]))
        .unwrap();

    let err = consumer
        .manager
        .get(handle)
        .await
        .expect_err("expected error: consumer has no NIXL endpoint");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("Rdma") || msg.contains("nixl") || msg.contains("NIXL"),
        "unexpected error message: {msg}"
    );
}
