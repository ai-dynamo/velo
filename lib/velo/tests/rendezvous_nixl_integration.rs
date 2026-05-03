// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Phase 2 integration tests for the velo facade.
//!
//! Exercises `Velo::register_data_pinned` + `Velo::get` across two workers
//! connected via TCP loopback. Mirrors the chunked-pull large payload test
//! in [`rendezvous_integration`], but on the NIXL/RDMA path.
//!
//! These tests require libnixl + UCX at runtime — see `velo-nixl` for env
//! setup. Inside the Claude bash sandbox the agent init fails with
//! `Failed to create unix domain socket for signal: Operation not permitted`;
//! run them outside the sandbox.

#![cfg(feature = "nixl")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::*;

fn new_tcp_transport() -> Arc<TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

struct VeloPair {
    owner: Arc<Velo>,
    consumer: Arc<Velo>,
}

impl VeloPair {
    async fn new() -> Self {
        let owner = Velo::builder()
            .add_transport(new_tcp_transport())
            .build()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let consumer = Velo::builder()
            .add_transport(new_tcp_transport())
            .build()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        consumer.register_peer(owner.peer_info()).unwrap();
        owner.register_peer(consumer.peer_info()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        owner.enable_nixl().expect("owner enable_nixl");
        consumer.enable_nixl().expect("consumer enable_nixl");

        Self { owner, consumer }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rendezvous_nixl_pinned_1mib() {
    let pair = VeloPair::new().await;

    // 1 MiB payload with a verifiable byte pattern.
    let size = 1024 * 1024;
    let mut payload_vec = vec![0u8; size];
    for (i, byte) in payload_vec.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let payload = Bytes::from(payload_vec.clone());

    let handle = pair
        .owner
        .register_data_pinned(payload.clone())
        .expect("register_data_pinned");

    // Metadata query should report the slot as pinned.
    let meta = pair.consumer.metadata(handle).await.unwrap();
    assert_eq!(meta.total_len, size as u64);
    assert!(
        meta.pinned,
        "slot should be pinned (mode = StageMode::Pinned)"
    );

    // Consumer pulls via NIXL_READ.
    let (data, lease_id) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data.len(), size);
    for (i, &byte) in data.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "mismatch at offset {i}");
    }

    pair.consumer.release(handle, lease_id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn rendezvous_nixl_coexists_with_chunked() {
    // Mixed workload: a pinned slot AND a regular (chunked) slot, fetched
    // back-to-back from the same consumer. Confirms the acquire handler's
    // mode-branch and the consumer's `_with_nixl` variants don't break the
    // chunked path.
    let pair = VeloPair::new().await;

    let pinned_payload = Bytes::from(vec![0xAA; 8 * 1024]);
    let chunked_payload = Bytes::from(vec![0xBB; 8 * 1024]);

    let pinned_handle = pair
        .owner
        .register_data_pinned(pinned_payload.clone())
        .unwrap();
    let chunked_handle = pair.owner.register_data(chunked_payload.clone());

    let (pinned_bytes, pinned_lease) = pair.consumer.get(pinned_handle).await.unwrap();
    assert_eq!(&pinned_bytes[..], &pinned_payload[..]);

    let (chunked_bytes, chunked_lease) = pair.consumer.get(chunked_handle).await.unwrap();
    assert_eq!(&chunked_bytes[..], &chunked_payload[..]);

    pair.consumer
        .release(pinned_handle, pinned_lease)
        .await
        .unwrap();
    pair.consumer
        .release(chunked_handle, chunked_lease)
        .await
        .unwrap();
}
