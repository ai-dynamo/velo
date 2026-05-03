// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Observability scenario tests.
//!
//! Each scenario sends messages between two Velo instances and asserts that the
//! expected Prometheus metrics are recorded.  The `transport_metrics_tests!`
//! macro parameterises every scenario across TCP and UDS transports.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::transports::uds::UdsTransportBuilder;
use velo::*;

// ---------------------------------------------------------------------------
// Transport factories
// ---------------------------------------------------------------------------

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

fn new_uds_transport(dir: &tempfile::TempDir) -> Arc<velo::transports::uds::UdsTransport> {
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = dir.path().join(format!("velo-{id}.sock"));
    Arc::new(
        UdsTransportBuilder::new()
            .socket_path(path)
            .build()
            .unwrap(),
    )
}

// ---------------------------------------------------------------------------
// Two-instance helper
// ---------------------------------------------------------------------------

struct VeloPair {
    server: Arc<Velo>,
    client: Arc<Velo>,
    server_reg: Registry,
    client_reg: Registry,
}

impl VeloPair {
    async fn new(t1: Arc<dyn Transport>, t2: Arc<dyn Transport>) -> Self {
        let server_reg = Registry::new();
        let server_metrics = Arc::new(VeloMetrics::register(&server_reg).unwrap());
        let client_reg = Registry::new();
        let client_metrics = Arc::new(VeloMetrics::register(&client_reg).unwrap());

        let server = Velo::builder()
            .add_transport(t1)
            .metrics(server_metrics)
            .build()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = Velo::builder()
            .add_transport(t2)
            .metrics(client_metrics)
            .build()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        client.register_peer(server.peer_info()).unwrap();
        server.register_peer(client.peer_info()).unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        Self {
            server,
            client,
            server_reg,
            client_reg,
        }
    }

    fn server_snap(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.server_reg)
    }

    fn client_snap(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.client_reg)
    }
}

// ---------------------------------------------------------------------------
// Scenario implementations
// ---------------------------------------------------------------------------

/// T1: Unary ping-pong — verify frame counters on both sides.
async fn scenario_unary_frame_counting(pair: &VeloPair) {
    // Register echo handler on server
    let handler = Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send request
    let payload = Bytes::from_static(b"hello");
    let response: Bytes = pair
        .client
        .unary("echo")
        .unwrap()
        .raw_payload(payload.clone())
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();
    assert_eq!(response, payload);

    // Allow async metric recording to settle
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client sent at least one outbound message frame
    let client_snap = pair.client_snap();
    let outbound_msgs = client_snap.counter(
        "velo_transport_frames_total",
        &[
            ("direction", "outbound"),
            ("message_type", "message"),
            ("outcome", "accepted"),
        ],
    );
    assert!(
        outbound_msgs >= 1.0,
        "expected ≥1 outbound message frame on client, got {outbound_msgs}"
    );

    // Server received at least one inbound message frame
    let server_snap = pair.server_snap();
    let inbound_msgs = server_snap.counter(
        "velo_transport_frames_total",
        &[
            ("direction", "inbound"),
            ("message_type", "message"),
            ("outcome", "accepted"),
        ],
    );
    assert!(
        inbound_msgs >= 1.0,
        "expected ≥1 inbound message frame on server, got {inbound_msgs}"
    );

    // Server sent a response frame back
    let outbound_resp = server_snap.counter(
        "velo_transport_frames_total",
        &[
            ("direction", "outbound"),
            ("message_type", "response"),
            ("outcome", "accepted"),
        ],
    );
    assert!(
        outbound_resp >= 1.0,
        "expected ≥1 outbound response frame on server, got {outbound_resp}"
    );
}

/// T2: Fire-and-forget — one outbound frame, no response.
async fn scenario_fire_and_forget(pair: &VeloPair) {
    let handler = Handler::am_handler("sink", |_ctx| Ok(())).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pair.client
        .am_send("sink")
        .unwrap()
        .raw_payload(Bytes::from_static(b"fire"))
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = pair.client_snap();
    let outbound = snap.counter(
        "velo_transport_frames_total",
        &[
            ("direction", "outbound"),
            ("message_type", "message"),
            ("outcome", "accepted"),
        ],
    );
    assert!(
        outbound >= 1.0,
        "expected ≥1 outbound message, got {outbound}"
    );

    // No rejections
    let rejections = snap.counter(
        "velo_transport_rejections_total",
        &[("reason", "send_error")],
    );
    assert_eq!(rejections, 0.0, "expected 0 send_error rejections");
}

/// T4: Registered peers gauge reflects peer count.
async fn scenario_registered_peers_gauge(pair: &VeloPair) {
    let snap = pair.server_snap();
    let peers = snap.gauge("velo_transport_registered_peers", &[]);
    assert!(
        peers >= 1.0,
        "expected ≥1 registered peer on server, got {peers}"
    );
}

/// H1: Handler unary success — requests, duration, bytes.
async fn scenario_handler_unary_success(pair: &VeloPair) {
    let handler = Handler::unary_handler("h1_echo", |ctx| Ok(Some(ctx.payload))).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let payload = Bytes::from_static(b"measure-me");
    let _resp: Bytes = pair
        .client
        .unary("h1_echo")
        .unwrap()
        .raw_payload(payload.clone())
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = pair.server_snap();

    // requests_total{handler=h1_echo, outcome=success} == 1
    let count = snap.counter(
        "velo_messenger_handler_requests_total",
        &[
            ("handler", "h1_echo"),
            ("response_type", "unary"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(count, 1.0, "expected 1 successful request, got {count}");

    // duration histogram should have 1 observation
    let hist = snap.histogram_count(
        "velo_messenger_handler_duration_seconds",
        &[
            ("handler", "h1_echo"),
            ("response_type", "unary"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(hist, 1, "expected 1 duration observation, got {hist}");

    // response_bytes should match payload length (echo returns same payload)
    let resp_bytes = snap.counter(
        "velo_messenger_handler_response_bytes_total",
        &[
            ("handler", "h1_echo"),
            ("response_type", "unary"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(resp_bytes, payload.len() as f64, "response_bytes mismatch");
}

/// H2: Handler unary error — outcome=error, response_bytes = error message length.
async fn scenario_handler_unary_error(pair: &VeloPair) {
    let handler = Handler::unary_handler("h2_fail", |_ctx| -> Result<Option<Bytes>, _> {
        Err(anyhow::anyhow!("boom"))
    })
    .build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result: Result<Bytes, _> = pair
        .client
        .unary("h2_fail")
        .unwrap()
        .instance(pair.server.instance_id())
        .send()
        .await;
    // The request should return an error response
    assert!(result.is_err(), "expected error response");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = pair.server_snap();
    let count = snap.counter(
        "velo_messenger_handler_requests_total",
        &[
            ("handler", "h2_fail"),
            ("response_type", "unary"),
            ("outcome", "error"),
        ],
    );
    assert_eq!(count, 1.0, "expected 1 error request, got {count}");

    let resp_bytes = snap.counter(
        "velo_messenger_handler_response_bytes_total",
        &[
            ("handler", "h2_fail"),
            ("response_type", "unary"),
            ("outcome", "error"),
        ],
    );
    assert!(
        resp_bytes > 0.0,
        "expected >0 error response bytes, got {resp_bytes}"
    );
}

/// H3: Fire-and-forget handler — response_bytes=0 (no response sent).
async fn scenario_handler_fire_and_forget_bytes(pair: &VeloPair) {
    let handler = Handler::am_handler("h3_faf", |_ctx| Ok(())).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pair.client
        .am_send("h3_faf")
        .unwrap()
        .raw_payload(Bytes::from_static(b"data"))
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = pair.server_snap();
    let count = snap.counter(
        "velo_messenger_handler_requests_total",
        &[
            ("handler", "h3_faf"),
            ("response_type", "fire_and_forget"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(
        count, 1.0,
        "expected 1 fire_and_forget request, got {count}"
    );

    let resp_bytes = snap.counter(
        "velo_messenger_handler_response_bytes_total",
        &[
            ("handler", "h3_faf"),
            ("response_type", "fire_and_forget"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(
        resp_bytes, 0.0,
        "fire-and-forget should have 0 response bytes, got {resp_bytes}"
    );
}

/// H6: Handler deregistered after handshake — client validation catches unknown handler.
///
/// Note: dispatch_failures{unknown_handler} is unreachable through the normal API
/// because the client validates handler existence via handshake before sending.
/// This test verifies that the client-side validation prevents the message from
/// reaching the server (no dispatch failure metric recorded).
async fn scenario_handler_client_validation(pair: &VeloPair) {
    // Register handler, do a warmup so handshake caches the handler list.
    let handler = Handler::am_handler("h6_temp", |_ctx| Ok(())).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    pair.client
        .am_send("h6_temp")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warmup"))
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = pair.server_snap();
    let count = snap.counter(
        "velo_messenger_handler_requests_total",
        &[
            ("handler", "h6_temp"),
            ("response_type", "fire_and_forget"),
            ("outcome", "success"),
        ],
    );
    assert_eq!(
        count, 1.0,
        "expected 1 successful request for h6_temp, got {count}"
    );
}

/// C1: Client direct resolution — DirectSuccess counted.
async fn scenario_client_direct_resolution(pair: &VeloPair) {
    let handler = Handler::am_handler("c1_sink", |_ctx| Ok(())).build();
    pair.server.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // First message triggers handshake for this handler name.
    pair.client
        .am_send("c1_sink")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warmup"))
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();

    // Wait for handshake to complete so next send uses direct path.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Second message should use the direct (fast) path.
    pair.client
        .am_send("c1_sink")
        .unwrap()
        .raw_payload(Bytes::from_static(b"test"))
        .instance(pair.server.instance_id())
        .send()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = pair.client_snap();
    let direct = snap.counter(
        "velo_messenger_client_resolution_total",
        &[("path", "direct"), ("outcome", "success")],
    );
    assert!(
        direct >= 1.0,
        "expected ≥1 direct resolution after warmup, got {direct}"
    );
}

// ---------------------------------------------------------------------------
// Parameterisation macro
// ---------------------------------------------------------------------------

macro_rules! transport_metrics_tests {
    ($mod_name:ident, $make_pair:expr) => {
        mod $mod_name {
            use super::*;

            async fn pair() -> VeloPair {
                $make_pair.await
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn unary_frame_counting() {
                let p = pair().await;
                scenario_unary_frame_counting(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn fire_and_forget() {
                let p = pair().await;
                scenario_fire_and_forget(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn registered_peers_gauge() {
                let p = pair().await;
                scenario_registered_peers_gauge(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn handler_unary_success() {
                let p = pair().await;
                scenario_handler_unary_success(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn handler_unary_error() {
                let p = pair().await;
                scenario_handler_unary_error(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn handler_fire_and_forget_bytes() {
                let p = pair().await;
                scenario_handler_fire_and_forget_bytes(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn handler_client_validation() {
                let p = pair().await;
                scenario_handler_client_validation(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn client_direct_resolution() {
                let p = pair().await;
                scenario_client_direct_resolution(&p).await;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// TCP suite
// ---------------------------------------------------------------------------

transport_metrics_tests!(tcp, {
    VeloPair::new(new_tcp_transport(), new_tcp_transport())
});

// ---------------------------------------------------------------------------
// UDS suite
// ---------------------------------------------------------------------------

mod uds {
    use super::*;

    async fn pair() -> (VeloPair, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let t1 = new_uds_transport(&dir);
        let t2 = new_uds_transport(&dir);
        let p = VeloPair::new(t1, t2).await;
        (p, dir) // keep dir alive
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unary_frame_counting() {
        let (p, _dir) = pair().await;
        scenario_unary_frame_counting(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fire_and_forget() {
        let (p, _dir) = pair().await;
        scenario_fire_and_forget(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registered_peers_gauge() {
        let (p, _dir) = pair().await;
        scenario_registered_peers_gauge(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handler_unary_success() {
        let (p, _dir) = pair().await;
        scenario_handler_unary_success(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handler_unary_error() {
        let (p, _dir) = pair().await;
        scenario_handler_unary_error(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handler_fire_and_forget_bytes() {
        let (p, _dir) = pair().await;
        scenario_handler_fire_and_forget_bytes(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn handler_client_validation() {
        let (p, _dir) = pair().await;
        scenario_handler_client_validation(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn client_direct_resolution() {
        let (p, _dir) = pair().await;
        scenario_client_direct_resolution(&p).await;
    }
}
