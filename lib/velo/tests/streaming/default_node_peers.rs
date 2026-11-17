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
use velo::{Handler, ShutdownPolicy, Velo};

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

fn node() -> velo::VeloBuilder {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = Arc::new(
        velo::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    );
    Velo::builder().add_transport(transport)
}

/// A default node's peer observer forwards each peer it learns to the mux,
/// which has no endpoint key of its own in any address, so the registration
/// is skipped at debug level: no error, and no warning per peer. Every path
/// a node learns a peer by reaches the same skip in the peer table, so one
/// path covers them; `learned_peer_tests` in the lib covers the paths.
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

    let consumer = node().build().await.unwrap();
    let producer = node().build().await.unwrap();
    // Only what registration and the attach log from here on.
    logs.take();
    producer.register_peer(consumer.peer_info()).unwrap();
    consumer
        .register_handler(Handler::unary_handler("ping", |_| Ok(Some(Bytes::new()))).build())
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        producer
            .unary("ping")
            .unwrap()
            .raw_payload(Bytes::new())
            .instance(consumer.instance_id())
            .send(),
    )
    .await
    .expect("ping timed out")
    .expect("ping failed");
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
        Ok(Some(velo::streaming::MESSENGER_MUX_KEY.to_string()))
    );
    assert!(
        text.contains("peer has no endpoint for this key"),
        "the capture must see the observer skip the peer:\n{text}"
    );
    let loud: Vec<&str> = text
        .lines()
        .filter(|line| line.contains(" WARN ") || line.contains(" ERROR "))
        .collect();
    assert!(loud.is_empty(), "{loud:#?}");
}
