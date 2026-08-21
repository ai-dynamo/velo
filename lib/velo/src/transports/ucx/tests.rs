// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-process loopback tests for the UCX transport.
//!
//! Two independent `ucp_context`s in one process, wired over the `tcp` lane:
//! with `UCP_ERR_HANDLING_MODE_PEER` the shm lanes are ineligible (no peer
//! failure handler), so tcp is the deterministic choice — and the exact code
//! path CI runs without RDMA hardware.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;

use super::{UcxTransport, UcxTransportBuilder};
use crate::transports::transport::{
    DataStreams, HealthCheckError, SendOutcome, Transport, TransportErrorHandler, make_channels,
};
use velo_ext::{InstanceId, MessageType, PeerInfo};

struct CountingErrors {
    count: AtomicUsize,
    notify: tokio::sync::Notify,
}

impl CountingErrors {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            notify: tokio::sync::Notify::new(),
        })
    }
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
    async fn wait_for_error(&self, timeout: Duration) -> bool {
        // Register the waiter BEFORE re-checking the count: `notify_waiters`
        // only wakes futures that already exist, so checking first and then
        // creating the future would lose a notification in between.
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.count() > 0 {
            return true;
        }
        tokio::time::timeout(timeout, notified).await.is_ok()
    }
}

impl TransportErrorHandler for CountingErrors {
    fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

struct Node {
    transport: Arc<UcxTransport>,
    streams: DataStreams,
    instance_id: InstanceId,
}

async fn start_node() -> Node {
    let transport = Arc::new(
        UcxTransportBuilder::new()
            .tls("tcp")
            .build()
            .expect("build ucx transport"),
    );
    let instance_id = InstanceId::new_v4();
    let (adapter, streams) = make_channels();
    transport
        .start(instance_id, adapter, tokio::runtime::Handle::current())
        .await
        .expect("start ucx transport");
    Node {
        transport,
        streams,
        instance_id,
    }
}

fn cross_register(a: &Node, b: &Node) {
    a.transport
        .register(PeerInfo::new(b.instance_id, b.transport.address()))
        .expect("register b in a");
    b.transport
        .register(PeerInfo::new(a.instance_id, a.transport.address()))
        .expect("register a in b");
}

async fn recv(rx: &flume::Receiver<(Bytes, Bytes)>, timeout: Duration) -> Option<(Bytes, Bytes)> {
    tokio::time::timeout(timeout, rx.recv_async())
        .await
        .ok()?
        .ok()
}

const T: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread")]
async fn message_round_trip_and_stream_routing() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    // Message → message_stream
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"hdr"),
        Bytes::from_static(b"payload"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(
        out,
        SendOutcome::Admitted | SendOutcome::Pending(_)
    ));
    let (h, p) = recv(&b.streams.message_stream, T)
        .await
        .expect("message arrives");
    assert_eq!(&h[..], b"hdr");
    assert_eq!(&p[..], b"payload");

    // Response → response_stream
    b.transport.send_message(
        a.instance_id,
        Bytes::from_static(b"resp-h"),
        Bytes::from_static(b"resp-p"),
        MessageType::Response,
        errs.clone(),
    );
    let (h, _) = recv(&a.streams.response_stream, T)
        .await
        .expect("response arrives");
    assert_eq!(&h[..], b"resp-h");

    // Event → event_stream
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"ev-h"),
        Bytes::new(),
        MessageType::Event,
        errs.clone(),
    );
    let (h, p) = recv(&b.streams.event_stream, T)
        .await
        .expect("event arrives");
    assert_eq!(&h[..], b"ev-h");
    assert!(p.is_empty());

    assert_eq!(errs.count(), 0, "no send errors expected");
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn many_messages_preserve_order() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    const N: u32 = 200;
    for i in 0..N {
        a.transport.send_message(
            b.instance_id,
            Bytes::from(i.to_le_bytes().to_vec()),
            Bytes::from(vec![0u8; 1024]),
            MessageType::Message,
            errs.clone(),
        );
    }
    for i in 0..N {
        let (h, p) = recv(&b.streams.message_stream, T)
            .await
            .expect("ordered message");
        assert_eq!(
            u32::from_le_bytes(h[..4].try_into().unwrap()),
            i,
            "order preserved"
        );
        assert_eq!(p.len(), 1024);
    }
    assert_eq!(errs.count(), 0);
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn unregistered_peer_reports_through_on_error() {
    let a = start_node().await;
    let errs = CountingErrors::new();
    let out = a.transport.send_message(
        InstanceId::new_v4(),
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        errs.wait_for_error(T).await,
        "pre-wire failure must reach on_error"
    );
    a.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_frame_fails_pre_wire() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    let limit = a
        .transport
        .max_message_size(b.instance_id)
        .expect("limit known");
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from(vec![0u8; limit + 1]),
        MessageType::Message,
        errs.clone(),
    );
    assert!(matches!(out, SendOutcome::Admitted));
    assert!(
        errs.wait_for_error(T).await,
        "oversized frame must reach on_error"
    );
    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_receiver_echoes_shutting_down() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    // Warm the path so the ShuttingDown reply exercises an established pair.
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"warm"),
        Bytes::new(),
        MessageType::Message,
        errs.clone(),
    );
    recv(&b.streams.message_stream, T)
        .await
        .expect("warmup arrives");

    b.streams.shutdown_state.begin_drain();
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"corr-id"),
        Bytes::from_static(b"ignored"),
        MessageType::Message,
        errs.clone(),
    );

    // The draining receiver must not deliver the message...
    assert!(
        recv(&b.streams.message_stream, Duration::from_millis(500))
            .await
            .is_none(),
        "draining receiver must not deliver new messages"
    );
    // ...and the sender sees ShuttingDown with the echoed header.
    let (h, _) = recv(&a.streams.response_stream, T)
        .await
        .expect("ShuttingDown echo");
    assert_eq!(&h[..], b"corr-id");

    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn health_check_semantics() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    // Unregistered peer.
    assert!(matches!(
        a.transport.check_health(InstanceId::new_v4(), T).await,
        Err(HealthCheckError::PeerNotRegistered)
    ));

    // Registered, reachable, but never connected: NeverConnected (TCP parity).
    assert!(matches!(
        a.transport.check_health(b.instance_id, T).await,
        Err(HealthCheckError::NeverConnected)
    ));

    // After traffic, healthy.
    a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::new(),
        MessageType::Message,
        errs.clone(),
    );
    recv(&b.streams.message_stream, T)
        .await
        .expect("message arrives");
    assert!(a.transport.check_health(b.instance_id, T).await.is_ok());

    a.transport.shutdown();
    b.transport.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_fails_queued_sends() {
    let a = start_node().await;
    let b = start_node().await;
    cross_register(&a, &b);
    let errs = CountingErrors::new();

    a.transport.shutdown();
    let out = a.transport.send_message(
        b.instance_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"p"),
        MessageType::Message,
        errs.clone(),
    );
    // Post-shutdown sends must not hang: either pre-wire on_error or a failed
    // admission (the channel behind the gate is closed).
    match out {
        SendOutcome::Admitted => {
            assert!(
                errs.wait_for_error(T).await,
                "post-shutdown send must surface an error"
            );
        }
        SendOutcome::Pending(admission) => {
            let resolved = tokio::time::timeout(T, admission)
                .await
                .expect("admission must resolve, not hang");
            assert!(resolved.is_err());
        }
    }
    b.transport.shutdown();
}
