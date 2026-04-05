// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the velo-rendezvous crate.
//!
//! Tests two Velo instances over TCP loopback exercising the full rendezvous
//! protocol: register_data on owner, get/metadata/detach/release on consumer.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use velo::*;
use velo_transports::tcp::TcpTransportBuilder;

// ---------------------------------------------------------------------------
// Helper: create two connected Velo instances over TCP
// ---------------------------------------------------------------------------

fn new_tcp_transport() -> Arc<velo_transports::tcp::TcpTransport> {
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
        let t1 = new_tcp_transport();
        let t2 = new_tcp_transport();

        let owner = Velo::builder().add_transport(t1).build().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let consumer = Velo::builder().add_transport(t2).build().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        consumer.register_peer(owner.peer_info()).unwrap();
        owner.register_peer(consumer.peer_info()).unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        Self { owner, consumer }
    }
}

// ---------------------------------------------------------------------------
// Test: small payload (inline path)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_small_payload_inline() {
    let pair = VeloPair::new().await;

    // Owner registers small data (well under 256 KiB inline threshold)
    let payload = Bytes::from(vec![0xAB; 1024]); // 1 KiB
    let handle = pair.owner.register_data(payload.clone());

    // Consumer pulls via get()
    let (data, lease_id) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data.len(), 1024);
    assert!(data.iter().all(|&b| b == 0xAB));

    // Release
    pair.consumer.release(handle, lease_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: large payload (chunked pull path)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_large_payload_chunked() {
    let pair = VeloPair::new().await;

    // Owner registers large data (exceeds 256 KiB inline threshold)
    let size = 1024 * 1024; // 1 MiB
    let mut payload_vec = vec![0u8; size];
    // Write a pattern so we can verify integrity
    for (i, byte) in payload_vec.iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }
    let payload = Bytes::from(payload_vec.clone());
    let handle = pair.owner.register_data(payload);

    // Consumer pulls via get()
    let (data, lease_id) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data.len(), size);
    // Verify integrity
    for (i, &byte) in data.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "mismatch at offset {i}");
    }

    // Release
    pair.consumer.release(handle, lease_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: metadata query (no lock)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_metadata() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0u8; 4096]);
    let handle = pair.owner.register_data(payload);

    // Consumer queries metadata
    let meta = pair.consumer.metadata(handle).await.unwrap();
    assert_eq!(meta.total_len, 4096);
    assert_eq!(meta.refcount, 1);
    assert!(!meta.pinned);
}

// ---------------------------------------------------------------------------
// Test: get_into with Vec<u8>
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_get_into_vec() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0xCD; 2048]);
    let handle = pair.owner.register_data(payload);

    // Consumer queries metadata first to know size
    let meta = pair.consumer.metadata(handle).await.unwrap();
    assert_eq!(meta.total_len, 2048);

    // Pull into pre-allocated Vec
    let mut buf = vec![0u8; meta.total_len as usize];
    let lease_id = pair.consumer.get_into(handle, &mut buf).await.unwrap();
    assert!(buf.iter().all(|&b| b == 0xCD));

    pair.consumer.release(handle, lease_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: get_into with BytesMut
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_get_into_bytesmut() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0xEF; 512]);
    let handle = pair.owner.register_data(payload);

    let mut buf = BytesMut::with_capacity(512);
    buf.resize(512, 0);
    let lease_id = pair.consumer.get_into(handle, &mut buf).await.unwrap();
    assert!(buf.iter().all(|&b| b == 0xEF));

    pair.consumer.release(handle, lease_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: detach then re-get (same handle, multiple reads)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_detach_and_reget() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0x42; 128]);
    let handle = pair.owner.register_data(payload);

    // First get
    let (data1, lease_id1) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data1.len(), 128);
    assert!(data1.iter().all(|&b| b == 0x42));

    // Detach (release lock, keep handle alive)
    pair.consumer.detach(handle, lease_id1).await.unwrap();

    // Small delay to let the detach AM propagate
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second get (same handle, should work because we detached, not released)
    let (data2, lease_id2) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data2.len(), 128);
    assert!(data2.iter().all(|&b| b == 0x42));

    // Now release
    pair.consumer.release(handle, lease_id2).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: ref counting with multiple consumers
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_ref_counting() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0x77; 256]);
    let handle = pair.owner.register_data(payload);

    // Increment refcount for a second consumer
    pair.consumer.ref_handle(handle).await.unwrap();

    // Verify refcount is now 2
    let meta = pair.consumer.metadata(handle).await.unwrap();
    assert_eq!(meta.refcount, 2);

    // First consumer gets and releases
    let (data1, lease_id1) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data1.len(), 256);
    pair.consumer.release(handle, lease_id1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Handle should still be alive (refcount was 2, now 1)
    let meta = pair.consumer.metadata(handle).await.unwrap();
    assert_eq!(meta.refcount, 1);

    // Second consumer gets and releases
    let (data2, lease_id2) = pair.consumer.get(handle).await.unwrap();
    assert_eq!(data2.len(), 256);
    pair.consumer.release(handle, lease_id2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Handle should be freed now (refcount 0, read_lock_count 0)
    let result = pair.consumer.metadata(handle).await;
    assert!(result.is_err(), "handle should be freed after all releases");
}

// ---------------------------------------------------------------------------
// Test: local fast-path (same worker)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_local_fast_path() {
    let pair = VeloPair::new().await;

    let payload = Bytes::from(vec![0x99; 512]);
    let handle = pair.owner.register_data(payload.clone());

    // Owner itself does the get (local fast-path, no network)
    let (data, lease_id) = pair.owner.get(handle).await.unwrap();
    assert_eq!(data, payload);

    // Local metadata
    let meta = pair.owner.metadata(handle).await.unwrap();
    assert_eq!(meta.total_len, 512);

    // Local release
    pair.owner.release(handle, lease_id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Test: large payload get_into (chunked path with explicit buffer)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_large_get_into() {
    let pair = VeloPair::new().await;

    // 512 KiB — exceeds inline threshold, will use chunked path
    let size = 512 * 1024;
    let mut payload_vec = vec![0u8; size];
    for (i, byte) in payload_vec.iter_mut().enumerate() {
        *byte = (i % 251) as u8; // prime modulus for pattern
    }
    let payload = Bytes::from(payload_vec.clone());
    let handle = pair.owner.register_data(payload);

    // Pull into Vec via get_into
    let mut buf = vec![0u8; size];
    let lease_id = pair.consumer.get_into(handle, &mut buf).await.unwrap();

    // Verify integrity
    for (i, &byte) in buf.iter().enumerate() {
        assert_eq!(byte, (i % 251) as u8, "mismatch at offset {i}");
    }

    pair.consumer.release(handle, lease_id).await.unwrap();
}

// ===========================================================================
// Transparent large payload tests
// ===========================================================================

// ---------------------------------------------------------------------------
// Test: transparent mode with unary handler (large payload auto-staged)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_transparent_large_payload_unary() {
    let pair = VeloPair::new().await;

    // Register an echo handler on the owner
    let handler = Handler::unary_handler("echo", |ctx: Context| Ok(Some(ctx.payload))).build();
    pair.owner.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a payload larger than the 256 KiB threshold — should be transparently
    // staged via rendezvous, then resolved on the receiver before the handler sees it.
    let size = 512 * 1024; // 512 KiB
    let mut payload_vec = vec![0u8; size];
    for (i, byte) in payload_vec.iter_mut().enumerate() {
        *byte = (i % 199) as u8;
    }
    let payload = Bytes::from(payload_vec);

    let response: Bytes = pair
        .consumer
        .unary("echo")
        .unwrap()
        .raw_payload(payload.clone())
        .instance(pair.owner.instance_id())
        .send()
        .await
        .unwrap();

    // The handler should have received the full payload transparently
    assert_eq!(response.len(), size);
    for (i, &byte) in response.iter().enumerate() {
        assert_eq!(byte, (i % 199) as u8, "mismatch at offset {i}");
    }
}

// ---------------------------------------------------------------------------
// Test: transparent mode with typed unary (large serialized payload)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_transparent_large_payload_typed_unary() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct BigPayload {
        data: Vec<u8>,
    }

    let pair = VeloPair::new().await;

    // Register a typed echo handler
    let handler = Handler::typed_unary(
        "typed_echo",
        |ctx: TypedContext<BigPayload>| -> anyhow::Result<BigPayload> { Ok(ctx.input) },
    )
    .build();
    pair.owner.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a payload that will exceed 256 KiB when serialized
    let big = BigPayload {
        data: vec![0xBB; 300 * 1024],
    };

    let response: BigPayload = pair
        .consumer
        .typed_unary::<BigPayload>("typed_echo")
        .unwrap()
        .payload(&big)
        .unwrap()
        .instance(pair.owner.instance_id())
        .send()
        .await
        .unwrap();

    assert_eq!(response.data.len(), big.data.len());
    assert!(response.data.iter().all(|&b| b == 0xBB));
}

// ---------------------------------------------------------------------------
// Test: small payloads still go inline (no rendezvous overhead)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_transparent_small_payload_stays_inline() {
    let pair = VeloPair::new().await;

    let handler = Handler::unary_handler("echo", |ctx: Context| Ok(Some(ctx.payload))).build();
    pair.owner.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 1 KiB — well under threshold, should NOT trigger rendezvous
    let payload = Bytes::from(vec![0xAA; 1024]);

    let response: Bytes = pair
        .consumer
        .unary("echo")
        .unwrap()
        .raw_payload(payload.clone())
        .instance(pair.owner.instance_id())
        .send()
        .await
        .unwrap();

    assert_eq!(response, payload);
}

// ---------------------------------------------------------------------------
// Test: transparent mode with fire-and-forget (am_send)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_transparent_large_payload_am_send() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pair = VeloPair::new().await;

    let received_len = Arc::new(AtomicUsize::new(0));
    let received_len_clone = received_len.clone();

    // Register a handler that records the received payload length
    let handler = Handler::am_handler("big_recv", move |ctx: Context| {
        received_len_clone.store(ctx.payload.len(), Ordering::SeqCst);
        Ok(())
    })
    .build();
    pair.owner.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a large fire-and-forget payload
    let size = 512 * 1024;
    let payload = Bytes::from(vec![0xCC; size]);

    pair.consumer
        .am_send("big_recv")
        .unwrap()
        .raw_payload(payload)
        .instance(pair.owner.instance_id())
        .send()
        .await
        .unwrap();

    // Wait for the AM to be processed (includes async resolve + handler dispatch)
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(received_len.load(Ordering::SeqCst), size);
}
