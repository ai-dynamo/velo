// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{DEADLINE, Kind, node, node_with, shutdown, traffic, transport};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use velo::Velo;
use velo::ext::{
    HealthCheckError, InstanceId, MessageType, PeerInfo, SendOutcome, Transport, TransportAdapter,
    TransportError, TransportErrorHandler, TransportKey, WorkerAddress,
};

struct CappedUds {
    inner: Arc<dyn Transport>,
    capacity: AtomicUsize,
    rejected_bytes: AtomicUsize,
    rejected: Notify,
}

impl Transport for CappedUds {
    fn key(&self) -> TransportKey {
        self.inner.key()
    }

    fn address(&self) -> WorkerAddress {
        self.inner.address()
    }

    fn register(&self, peer: PeerInfo) -> Result<(), TransportError> {
        self.inner.register(peer)
    }

    fn max_message_size(&self, _: InstanceId) -> Option<usize> {
        Some(self.capacity.load(Ordering::Relaxed))
    }

    fn send_message(
        &self,
        instance: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> SendOutcome {
        let size = header.len() + payload.len();
        let capacity = self.capacity.load(Ordering::Relaxed);
        if size > capacity {
            self.rejected_bytes.store(size, Ordering::Relaxed);
            self.rejected.notify_one();
            on_error.on_error(
                header,
                payload,
                format!("frame of {size} exceeds {capacity}"),
            );
            return SendOutcome::Admitted;
        }
        self.inner
            .send_message(instance, header, payload, message_type, on_error)
    }

    fn start(
        &self,
        instance: InstanceId,
        adapter: TransportAdapter,
        runtime: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        self.inner.start(instance, adapter, runtime)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }

    fn set_observability(&self, observability: Arc<dyn velo::ext::TransportObservability>) {
        self.inner.set_observability(observability);
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        self.inner.closed()
    }

    fn check_health(
        &self,
        instance: InstanceId,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<(), HealthCheckError>> {
        self.inner.check_health(instance, timeout)
    }
}

#[tokio::test]
async fn staged_chunks_fit_the_named_transport() {
    let dir = tempfile::tempdir().unwrap();
    let peer = node(&dir, &[Kind::Tcp, Kind::Uds]).await;
    let cap = Arc::new(CappedUds {
        inner: transport(Kind::Uds, &dir),
        capacity: AtomicUsize::new(1024 * 1024),
        rejected_bytes: AtomicUsize::new(0),
        rejected: Notify::new(),
    });
    let sender = Velo::builder()
        .mux_only()
        .add_transport(transport(Kind::Tcp, &dir))
        .add_transport(cap.clone())
        .build()
        .await
        .unwrap();
    sender.register_peer(peer.velo.peer_info()).unwrap();
    peer.velo.register_peer(sender.peer_info()).unwrap();
    let instance = peer.velo.instance_id();
    let route = sender.transport_route("uds").unwrap();
    let len = 300usize * 1024;
    let send = || {
        sender
            .unary("echo_len")
            .unwrap()
            .route(route)
            .raw_payload(Bytes::from(vec![1; len]))
            .instance(instance)
            .timeout(DEADLINE)
            .send()
    };

    assert_eq!(&send().await.unwrap()[..], &len.to_le_bytes());
    let narrow = 128 * 1024;
    cap.capacity.store(narrow, Ordering::Relaxed);
    assert_eq!(
        sender.route_capacity(instance, route).unwrap(),
        Some(narrow)
    );
    let reply = tokio::select! {
        reply = send() => reply.unwrap(),
        _ = cap.rejected.notified() => {
            let size = cap.rejected_bytes.load(Ordering::Relaxed);
            panic!("staged fetch tried to send a {size}-byte frame over a {narrow}-byte route");
        }
    };
    assert_eq!(&reply[..], &len.to_le_bytes());
}

#[tokio::test]
async fn an_unnamed_get_sizes_chunks_for_the_consumers_primary() {
    let dir = tempfile::tempdir().unwrap();
    let consumer = node(&dir, &[Kind::Uds, Kind::Tcp]).await;
    let narrow = 128 * 1024;
    let cap = Arc::new(CappedUds {
        inner: transport(Kind::Uds, &dir),
        capacity: AtomicUsize::new(narrow),
        rejected_bytes: AtomicUsize::new(0),
        rejected: Notify::new(),
    });
    let owner = Velo::builder()
        .mux_only()
        .add_transport(transport(Kind::Tcp, &dir))
        .add_transport(cap.clone())
        .build()
        .await
        .unwrap();
    owner.register_peer(consumer.velo.peer_info()).unwrap();
    consumer.velo.register_peer(owner.peer_info()).unwrap();

    let data = Bytes::from(vec![1; 300 * 1024]);
    let handle = owner.register_data(data.clone());
    let (fetched, _) = tokio::select! {
        fetched = tokio::time::timeout(DEADLINE, consumer.velo.get(handle)) => {
            fetched.unwrap().unwrap()
        }
        _ = cap.rejected.notified() => {
            let size = cap.rejected_bytes.load(Ordering::Relaxed);
            panic!("get tried to send a {size}-byte frame over a {narrow}-byte reply route");
        }
    };
    assert!(fetched == data);
}

/// Every frame of a staged call travels on the call's route, in every
/// combination of: a named or an unnamed call, the same or a different add
/// order on the two nodes, and a wide or a capped route back. The caller,
/// who owns the staged payload, adds TCP then UDS, so an unnamed call rides
/// TCP and a named one UDS. In a capped cell the route's transport is capped
/// outbound at the caller, where the `_rv_pull` replies leave, and the other
/// is not: the chunks must be sized for the route's transport.
///
/// Each cell asserts that the call succeeds within its deadline, that every
/// frame both ways (request, reply, acquire, pulls and their replies,
/// release) used the one expected transport, and that no frame was too
/// large for the transport that carried it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_staged_frame_follows_the_call_route() {
    let len = 300 * 1024;
    let narrow = 128 * 1024;
    for named in [true, false] {
        for asymmetric in [false, true] {
            for capped in [false, true] {
                let cell = format!("named={named} asymmetric={asymmetric} capped={capped}");
                let dir = tempfile::tempdir().unwrap();
                let expected = if named { "uds" } else { "tcp" };
                // Only the route's transport is capped: chunks sized for any
                // other transport would not fit it.
                let wrap = |kind, key: &str| {
                    let capacity = if capped && key == expected {
                        narrow
                    } else {
                        usize::MAX
                    };
                    Arc::new(CappedUds {
                        inner: transport(kind, &dir),
                        capacity: AtomicUsize::new(capacity),
                        rejected_bytes: AtomicUsize::new(0),
                        rejected: Notify::new(),
                    })
                };
                let (tcp, uds) = (wrap(Kind::Tcp, "tcp"), wrap(Kind::Uds, "uds"));
                let caller = node_with(vec![tcp.clone(), uds.clone()]).await;
                let order = if asymmetric {
                    [Kind::Uds, Kind::Tcp]
                } else {
                    [Kind::Tcp, Kind::Uds]
                };
                let receiver = node(&dir, &order).await;
                caller
                    .velo
                    .register_peer(receiver.velo.peer_info())
                    .unwrap();
                receiver
                    .velo
                    .register_peer(caller.velo.peer_info())
                    .unwrap();
                let id = receiver.velo.instance_id();

                let seen = traffic(&caller, &receiver, async {
                    let mut call = caller
                        .velo
                        .unary("echo_len")
                        .unwrap()
                        .raw_payload(Bytes::from(vec![1; len]))
                        .instance(id)
                        .timeout(DEADLINE);
                    if named {
                        call = call.transport("uds");
                    }
                    let reply =
                        tokio::time::timeout(DEADLINE + Duration::from_secs(1), call.send())
                            .await
                            .unwrap_or_else(|_| panic!("{cell}: the call timed out"))
                            .unwrap_or_else(|e| panic!("{cell}: the call failed: {e:#}"));
                    assert_eq!(&reply[..], &len.to_le_bytes(), "{cell}");
                })
                .await;
                assert_eq!(seen.transports(), vec![expected.to_string()], "{cell}");
                for capped in [&tcp, &uds] {
                    assert_eq!(
                        capped.rejected_bytes.load(Ordering::Relaxed),
                        0,
                        "{cell}: a frame was too large for its transport"
                    );
                }
                // The acquire, the pulls and the release went to the caller.
                let fetches = seen
                    .b_to_a
                    .get(&(expected.into(), "message".into()))
                    .copied()
                    .unwrap_or(0);
                let chunks = if capped { len.div_ceil(narrow - 64) } else { 1 };
                assert!(
                    fetches as usize >= 2 + chunks,
                    "{cell}: {fetches} fetch messages for {chunks} chunks"
                );
                shutdown([&caller, &receiver]).await;
            }
        }
    }
}
