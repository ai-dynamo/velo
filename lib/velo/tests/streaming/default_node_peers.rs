// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A default node learns peers with no per-stream transport installed.
//!
//! A test binary of its own, with one test, because it installs a global
//! `tracing` subscriber to read what registration logs. A subscriber set
//! with `tracing::subscriber::set_default` in a binary whose other tests log
//! at the same time captured nothing at all in most runs, so its
//! "no warning" check would pass for nothing. See "Coding gotchas" in the
//! book.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use velo::{Handler, InstanceId, PeerDiscovery, PeerInfo, ShutdownPolicy, Velo, WorkerId};

/// A discovery backend that knows one peer.
struct OnePeer(PeerInfo);

impl PeerDiscovery for OnePeer {
    fn discover_by_worker_id(&self, worker: WorkerId) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        Box::pin(async move {
            anyhow::ensure!(worker == self.0.worker_id(), "unknown worker {worker}");
            Ok(self.0.clone())
        })
    }

    fn discover_by_instance_id(
        &self,
        instance: InstanceId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        Box::pin(async move {
            anyhow::ensure!(
                instance == self.0.instance_id(),
                "unknown instance {instance}"
            );
            Ok(self.0.clone())
        })
    }
}

/// Log lines captured by the global subscriber.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Everything captured so far, and an empty buffer after it.
    fn take(&self) -> String {
        String::from_utf8_lossy(&std::mem::take(&mut *self.0.lock().unwrap())).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogs;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn builder(discovery: Option<PeerInfo>) -> velo::VeloBuilder {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = Arc::new(
        velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    );
    let mut builder = Velo::builder().add_transport(transport);
    if let Some(peer) = discovery {
        builder = builder.discovery(Arc::new(OnePeer(peer)));
    }
    builder
}

async fn ping(from: &Velo, to: &Velo, by_worker: bool) {
    to.register_handler(Handler::unary_handler("ping", |_| Ok(Some(Bytes::new()))).build())
        .unwrap();
    let builder = from.unary("ping").unwrap().raw_payload(Bytes::new());
    let builder = if by_worker {
        builder.worker(to.instance_id().worker_id())
    } else {
        builder.instance(to.instance_id())
    };
    tokio::time::timeout(Duration::from_secs(5), builder.send())
        .await
        .expect("ping timed out")
        .expect("ping failed");
}

/// How the producer learns the consumer.
#[derive(Clone, Copy, Debug)]
enum Learned {
    /// `Velo::register_peer`.
    Velo,
    /// `Velo::discover_and_register_peer`, which asks discovery for the
    /// consumer's instance id. Until 0.23.0 this case called
    /// `Messenger::register_peer`, which is no longer public.
    DiscoverAndRegister,
    /// The `_hello` the consumer sends on its first message to the producer.
    Hello,
    /// Discovery of the consumer when the producer first sends to its worker id.
    Discovery,
}

/// A default node's peer observer forwards each peer it learns to the mux,
/// which has no endpoint key of its own in any address, so each registration
/// is skipped at debug level: no error, and no warning per peer. This holds
/// on every path the node learns a peer by.
///
/// The consumer is a default node, or one that opted in to the per-stream
/// path and so advertises a `tcp-stream` endpoint the producer cannot use.
/// Either way the attach rides the mux.
///
/// The debug line is the control that proves the capture sees registration.
#[tokio::test(flavor = "multi_thread")]
async fn a_default_node_learns_peers_without_a_per_stream_transport() {
    let logs = CapturedLogs::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish(),
    )
    .expect("this binary installs one subscriber");

    for consumer_opted_in in [false, true] {
        for path in [
            Learned::Velo,
            Learned::DiscoverAndRegister,
            Learned::Hello,
            Learned::Discovery,
        ] {
            let case = format!("{path:?}, consumer opted in {consumer_opted_in}");
            let consumer = if consumer_opted_in {
                builder(None).stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
            } else {
                builder(None)
            }
            .build()
            .await
            .unwrap();
            let discovery = matches!(path, Learned::Discovery | Learned::DiscoverAndRegister)
                .then(|| consumer.peer_info());
            let producer = builder(discovery).build().await.unwrap();
            // Only what this case logs from here on.
            logs.take();
            match path {
                Learned::Velo => producer.register_peer(consumer.peer_info()).unwrap(),
                Learned::DiscoverAndRegister => producer
                    .discover_and_register_peer(consumer.instance_id())
                    .await
                    .unwrap(),
                Learned::Hello | Learned::Discovery => {}
            }
            match path {
                Learned::Velo | Learned::DiscoverAndRegister => {
                    ping(&producer, &consumer, false).await
                }
                Learned::Hello => {
                    consumer.register_peer(producer.peer_info()).unwrap();
                    ping(&consumer, &producer, false).await;
                }
                Learned::Discovery => ping(&producer, &consumer, true).await,
            }
            let anchor = consumer.create_anchor::<u32>();
            let attached = tokio::time::timeout(
                Duration::from_secs(5),
                producer.attach_anchor::<u32>(anchor.handle()),
            )
            .await
            .expect("attach timed out");
            let outcome = attached
                .as_ref()
                .map(|sender| {
                    sender
                        .negotiated_transport()
                        .map(|key| key.as_str().to_string())
                })
                .map_err(|error| error.to_string());
            drop(attached);
            drop(anchor);
            // What a close logs is not about registration.
            let text = logs.take();
            for node in [&producer, &consumer] {
                node.shutdown(ShutdownPolicy::Timeout(Duration::from_secs(2)))
                    .await;
            }
            assert_eq!(
                outcome,
                Ok(Some(velo::streaming::MESSENGER_MUX_KEY.to_string())),
                "{case}"
            );
            assert!(
                text.contains("peer has no endpoint for this key"),
                "{case}: the capture must see the observer skip the peer:\n{text}"
            );
            let loud: Vec<&str> = text
                .lines()
                .filter(|line| line.contains(" WARN ") || line.contains(" ERROR "))
                .collect();
            assert!(loud.is_empty(), "{case}: {loud:#?}");
        }
    }
}
