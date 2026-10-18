// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `velo::rendezvous` over TCP loopback.
//!
//! Two Velo instances exercising the chunked protocol end to end:
//! `register_data` on the owner, `get`/`metadata`/`detach`/`release` on the
//! consumer, and the transparent large-payload path over the top.
//!
//! The RDMA fast path has its own suite in `rendezvous_rdma.rs`, which needs
//! the `ucx` feature. Everything here is the path that is always available, and
//! it stays correct whether or not that feature is on.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use velo::transports::tcp::TcpTransportBuilder;
use velo::*;

// ---------------------------------------------------------------------------
// Helper: create two connected Velo instances over TCP
// ---------------------------------------------------------------------------

fn new_tcp_transport() -> Arc<velo::transports::tcp::TcpTransport> {
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
// Test: small payload (single chunk)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_small_payload_inline() {
    let pair = VeloPair::new().await;

    // There is no inline path: every payload is pulled chunk by chunk, and a
    // 1 KiB one is a single chunk. The size is here to exercise that boundary,
    // not to select a different code path.
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
// Test: large payload (several chunks)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_large_payload_chunked() {
    let pair = VeloPair::new().await;

    // Several chunks at the 512 KiB default, so the reassembly offsets matter.
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

    // Exactly one chunk at the 512 KiB default: the boundary where chunk_count
    // is 1 rather than 2.
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

/// A draining owner still serves `get` on a staged handle: the pull serves a
/// payload that was staged before the drain. It refuses `ref_handle` and
/// `metadata`, because those start a new consumer, and that is new work.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_owner_serves_get_but_refuses_a_new_consumer() {
    let pair = VeloPair::new().await;
    let payload = Bytes::from(vec![0x5a; 1024 * 1024]);
    let handle = pair.owner.register_data(payload);
    pair.owner.begin_drain();

    assert!(
        pair.consumer.ref_handle(handle).await.is_err(),
        "a draining owner took a new reference"
    );
    assert!(
        pair.consumer.metadata(handle).await.is_err(),
        "a draining owner answered a metadata query"
    );
    let (data, lease) = pair
        .consumer
        .get(handle)
        .await
        .expect("a draining owner refused the pull of a staged payload");
    assert_eq!(data.len(), 1024 * 1024);
    pair.consumer.release(handle, lease).await.unwrap();
}
// ---------------------------------------------------------------------------
// Opt-out: a node can withhold the built-in `_rv_*` pull routes entirely
// ---------------------------------------------------------------------------

/// Build one node, with the built-in pull routes installed or withheld.
async fn node(install_rendezvous_handlers: bool) -> Arc<Velo> {
    Velo::builder()
        .add_transport(new_tcp_transport())
        .install_rendezvous_handlers(install_rendezvous_handlers)
        .build()
        .await
        .unwrap()
}

/// The default path is covered by every test above. This one proves the
/// opt-out: with the routes withheld, a remote pull fails as an unknown
/// handler, while local register/get keep working.
#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_opt_out_rejects_bare_pulls_but_serves_locally() {
    let owner = node(false).await;
    let consumer = node(true).await;
    consumer.register_peer(owner.peer_info()).unwrap();
    owner.register_peer(consumer.peer_info()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from_static(b"staged");
    let handle = owner.register_data(payload.clone());

    // Local get on the owner: no handlers needed.
    let (local, lease) = owner.get(handle).await.unwrap();
    assert_eq!(local, payload);
    owner.release(handle, lease).await.unwrap();

    // A remote pull has no `_rv_acquire` to answer it.
    let err = consumer
        .get(handle)
        .await
        .expect_err("no pull routes installed");
    assert!(
        err.to_string().contains("_rv_acquire"),
        "unexpected error: {err}"
    );

    owner
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
    consumer
        .shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
        .await;
}

/// A host that withholds the built-in routes can still serve staged bytes
/// through its own guarded route: the handler checks the caller's
/// connection-announced identity and reads the local store. The policy in
/// this handler belongs to the host; velo only withholds the unguarded
/// routes.
#[tokio::test(flavor = "multi_thread")]
async fn test_rendezvous_opt_out_with_a_guarded_route() {
    let owner = node(false).await;
    let admitted = node(true).await;
    let intruder = node(true).await;
    for peer in [&admitted, &intruder] {
        peer.register_peer(owner.peer_info()).unwrap();
        owner.register_peer(peer.peer_info()).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let payload = Bytes::from_static(b"admitted bytes");
    let handle = owner.register_data(payload.clone());
    let local_id = handle.unpack().1;

    let allowed = admitted.instance_id();
    let store = Arc::clone(owner.rendezvous_manager().data_store());
    owner
        .messenger()
        .register_streaming_handler(
            Handler::unary_handler("_guarded_pull", move |ctx| {
                if ctx.sender_identity().instance_id() != Some(allowed) {
                    return Err(anyhow::anyhow!("not admitted"));
                }
                let bytes = store
                    .get_data(local_id)
                    .ok_or_else(|| anyhow::anyhow!("staged data is gone"))?;
                Ok(Some(bytes))
            })
            .build(),
        )
        .unwrap();

    // The admitted caller reads the staged bytes through the guarded route.
    let got = tokio::time::timeout(
        Duration::from_secs(5),
        admitted
            .messenger()
            .unary_streaming("_guarded_pull")
            .raw_payload(Bytes::new())
            .instance(owner.instance_id())
            .send(),
    )
    .await
    .expect("the call resolves")
    .expect("the guard admits it");
    assert_eq!(got, payload);

    // A different caller is rejected by the guard.
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        intruder
            .messenger()
            .unary_streaming("_guarded_pull")
            .raw_payload(Bytes::new())
            .instance(owner.instance_id())
            .send(),
    )
    .await
    .expect("the call resolves")
    .expect_err("the guard must reject a non-admitted caller");
    assert!(
        err.to_string().contains("not admitted"),
        "unexpected: {err}"
    );

    for node in [owner, admitted, intruder] {
        node.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(1)))
            .await;
    }
}
