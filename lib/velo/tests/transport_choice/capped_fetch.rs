// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use super::{DEADLINE, Kind, node, transport};
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

/// An ordinary `get` whose consumer and owner added their transports in
/// different orders. The consumer pulls over its primary, UDS, and the
/// owner's replies follow each pull back over UDS, which is capped at
/// 128 KiB. The owner must size its chunks for the transport the replies
/// take, not for its own primary.
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
