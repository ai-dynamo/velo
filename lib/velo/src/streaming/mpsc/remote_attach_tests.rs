// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A remote MPSC attach whose data-plane connect the test controls. This
//! needs a frame transport the test implements, so it is a unit test: the
//! trait is crate-private.

use std::sync::Arc;
use std::time::Duration;

use velo_ext::{TransportKey, WorkerAddress, WorkerId};

use crate::streaming::transport::FrameTransport;
use crate::streaming::{AnchorManagerBuilder, MpscAnchorConfig};

/// A node that serves streams on the per-stream TCP transport (mux off, so
/// an attach to it negotiates `tcp-stream`), and a node that knows it.
async fn pair() -> (crate::Velo, crate::Velo) {
    let tcp = || {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        )
    };
    let a = crate::Velo::builder()
        .add_transport(tcp())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .messenger_mux(crate::streaming::MuxConfig {
            enabled: false,
            ..Default::default()
        })
        .unwrap()
        .build()
        .await
        .unwrap();
    let b = crate::Velo::builder()
        .add_transport(tcp())
        .mux_only()
        .build()
        .await
        .unwrap();
    a.register_peer(b.peer_info()).unwrap();
    b.register_peer(a.peer_info()).unwrap();
    (a, b)
}

/// Pause the data-plane connect after the receiver has accepted the attach.
struct PausedConnect {
    entered: flume::Sender<()>,
    result: flume::Receiver<anyhow::Result<flume::Sender<Vec<u8>>>>,
}

impl FrameTransport for PausedConnect {
    fn key(&self) -> TransportKey {
        TransportKey::new(crate::streaming::tcp_transport::TCP_STREAM_KEY)
    }

    fn address(&self) -> WorkerAddress {
        WorkerAddress::empty()
    }

    fn bind(
        &self,
        _: u64,
        _: u64,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<flume::Receiver<Vec<u8>>>> {
        Box::pin(async { anyhow::bail!("sender-only test transport") })
    }

    fn connect(
        &self,
        _: WorkerId,
        _: u64,
        _: u64,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async {
            self.entered.send_async(()).await?;
            self.result.recv_async().await?
        })
    }
}

/// Cancellation can arrive before connect returns. A failed or abandoned
/// connect must also remove the identity it registered before the attach RPC.
#[tokio::test(flavor = "multi_thread")]
async fn mpsc_identity_survives_early_cancel_and_cleans_up_failed_attach() {
    for outcome in ["cancel", "error", "abort"] {
        let (node_a, node_b) = pair().await;
        let messenger_b = Arc::clone(node_b.messenger());
        let (entered, connected) = flume::bounded(1);
        let (finish, result) = flume::bounded(1);
        // B's own manager, over a transport whose connect the test holds
        // open. Its handlers replace the ones B's `Velo` registered, so A's
        // `_stream_cancel` reaches this manager's sender registry.
        let am_b = Arc::new(
            AnchorManagerBuilder::default()
                .worker_id(messenger_b.instance_id().worker_id())
                .transport(Arc::new(PausedConnect { entered, result }) as Arc<dyn FrameTransport>)
                .messenger(Some(messenger_b.clone()))
                .build()
                .unwrap(),
        );
        am_b.register_handlers(messenger_b).unwrap();
        let anchor = node_a.create_mpsc_anchor_with_config::<u32>(MpscAnchorConfig {
            max_senders: Some(1),
            ..Default::default()
        });
        let handle = anchor.handle();
        let producer = am_b.clone();
        let attach =
            tokio::spawn(async move { producer.attach_mpsc_stream_anchor::<u32>(handle).await });
        tokio::time::timeout(Duration::from_secs(3), connected.recv_async())
            .await
            .unwrap()
            .unwrap();
        let token = am_b
            .sender_registry
            .senders
            .iter()
            .next()
            .expect("identity must be registered before connect")
            .cancel_token
            .clone();
        match outcome {
            "cancel" => {
                anchor.controller().cancel();
                tokio::time::timeout(Duration::from_secs(3), token.cancelled())
                    .await
                    .unwrap();
                let (tx, _rx) = flume::bounded(1);
                finish.send(Ok(tx)).unwrap();
                let sender = tokio::time::timeout(Duration::from_secs(3), attach)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(sender.cancellation_token().is_cancelled());
                assert!(sender.send(1).await.is_err());
            }
            "error" => {
                finish.send(Err(anyhow::anyhow!("connect failed"))).unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), attach)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_err()
                );
                let replacement = node_a
                    .attach_mpsc_anchor::<u32>(handle)
                    .await
                    .expect("failed remote connect must release max_senders capacity");
                drop(replacement);
            }
            "abort" => {
                attach.abort();
                assert!(attach.await.unwrap_err().is_cancelled());
            }
            _ => unreachable!(),
        }
        assert!(am_b.sender_registry.senders.is_empty(), "{outcome}");
        drop(anchor);
    }
}
