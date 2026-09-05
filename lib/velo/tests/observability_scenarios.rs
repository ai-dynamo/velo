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

#[cfg(feature = "zmq")]
fn new_zmq_transport() -> Arc<velo::transports::zmq::ZmqTransport> {
    Arc::new(
        velo::transports::zmq::ZmqTransportBuilder::new()
            .bind_endpoint("tcp://127.0.0.1:0")
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

/// T9: Inbound-queue conservation — every admitted `Message` is one the
/// dispatch loop eventually takes off `message_rx`.
///
/// There is no depth gauge for that queue, on purpose: sampling
/// `flume::Receiver::len()` reads the wrong number under exactly the load that
/// makes the depth interesting. The depth is instead *derived*, as
/// `velo_transport_frames_total{direction="inbound",message_type="message",
/// outcome="accepted"} - velo_messenger_inbound_dequeued_total`, and that
/// subtraction is only meaningful while the two counters agree at rest. This
/// scenario is what keeps them agreeing: a counter wired to the wrong side of
/// the loop, or a transport that admits without recording, shows up here as a
/// gap that never closes.
///
/// Traffic runs both ways because the identity has to hold on both: a request
/// is an inbound `Message` on the side that serves it, while the reply comes
/// back as a `Response` frame and never touches the inbound queue. One
/// direction would leave the other side's counters at a vacuous zero.
async fn scenario_inbound_queue_conservation(pair: &VeloPair) {
    for velo in [&pair.server, &pair.client] {
        let handler = Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build();
        velo.register_handler(handler).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    for (from, to) in [(&pair.client, &pair.server), (&pair.server, &pair.client)] {
        for _ in 0..4 {
            let payload = Bytes::from_static(b"conserve");
            let response: Bytes = from
                .unary("echo")
                .unwrap()
                .raw_payload(payload.clone())
                .instance(to.instance_id())
                .send()
                .await
                .unwrap();
            assert_eq!(response, payload);
        }
    }

    await_inbound_queue_quiesced("server", || pair.server_snap()).await;
    await_inbound_queue_quiesced("client", || pair.client_snap()).await;
}

/// Poll one side's snapshot until admitted and dequeued inbound messages agree
/// on a non-zero count, or fail with both numbers.
///
/// Equality is a property of quiescence, not of any fixed sleep: admission and
/// dequeue are the two ends of a real queue and the window between them is
/// real work, so the assertion has to wait the queue out rather than guess how
/// long it is.
///
/// The `transport` label is deliberately absent from the frame lookup: this
/// sums every series that matches the remaining labels, so it stays correct
/// whether `VeloPair` gives each side one transport or several.
async fn await_inbound_queue_quiesced(label: &str, snapshot: impl Fn() -> MetricSnapshot) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snap = snapshot();
        let accepted = snap.counter_sum(
            "velo_transport_frames_total",
            &[
                ("direction", "inbound"),
                ("message_type", "message"),
                ("outcome", "accepted"),
            ],
        );
        let dequeued = snap.counter("velo_messenger_inbound_dequeued_total", &[]);
        // Non-zero on both counts: every peer receives at least the other's
        // `_hello`, so a pair of zeroes is a dark counter, not a quiet queue.
        if accepted > 0.0 && (accepted - dequeued).abs() < f64::EPSILON {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label}: inbound queue never quiesced — accepted={accepted} dequeued={dequeued}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// T10: Egress conservation — every outbound frame the transport admitted is
/// one its per-connection writer eventually handed to the socket.
///
/// The mirror of T9, one layer further out. `finalize_send_outcome` counts a
/// frame as `velo_transport_frames_total{direction="outbound",
/// outcome="accepted"}` the moment it reaches the connection's bounded send
/// channel; `velo_transport_frames_written_total` counts it again once the
/// writer's `write_all` has returned for it. The difference is the egress
/// queue — the depth this instrumentation exists to measure — so the two
/// counters have to converge at rest or the derived depth is a fiction.
///
/// Traffic runs both ways for the same reason T9 does: each side's egress is
/// its own queue with its own writer task.
async fn scenario_egress_conservation(pair: &VeloPair) {
    for velo in [&pair.server, &pair.client] {
        let handler = Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build();
        velo.register_handler(handler).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    for (from, to) in [(&pair.client, &pair.server), (&pair.server, &pair.client)] {
        for _ in 0..4 {
            let payload = Bytes::from_static(b"egress");
            let response: Bytes = from
                .unary("echo")
                .unwrap()
                .raw_payload(payload.clone())
                .instance(to.instance_id())
                .send()
                .await
                .unwrap();
            assert_eq!(response, payload);
        }
    }

    await_egress_quiesced("server", || pair.server_snap()).await;
    await_egress_quiesced("client", || pair.client_snap()).await;
}

/// Poll one side's snapshot until admitted and written outbound frames agree on
/// a non-zero count, then assert the two egress histograms are consistent with
/// that count.
///
/// Equality is a property of quiescence, exactly as in
/// [`await_inbound_queue_quiesced`]: admission and the wire are the two ends of
/// a real queue.
///
/// The two histograms are counted differently on purpose. The queue wait is
/// observed once per frame, as the writer takes it off the channel, so its
/// count tracks the frame count. The write duration brackets one `write_all`
/// sequence, and the writer coalesces whatever is already queued into a single
/// write — so its count is at most the frame count, and equals it only when
/// every write happened to carry one frame.
async fn await_egress_quiesced(label: &str, snapshot: impl Fn() -> MetricSnapshot) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snap = snapshot();
        let accepted = snap.counter_sum(
            "velo_transport_frames_total",
            &[("direction", "outbound"), ("outcome", "accepted")],
        );
        let written = snap.counter_sum("velo_transport_frames_written_total", &[]);
        if accepted > 0.0 && (accepted - written).abs() < f64::EPSILON {
            let waits = snap.histogram_count("velo_transport_egress_queue_wait_seconds", &[]);
            let writes = snap.histogram_count("velo_transport_write_duration_seconds", &[]);
            assert_eq!(
                waits as f64, written,
                "{label}: one queue-wait observation per written frame — waits={waits} written={written}"
            );
            assert!(
                writes >= 1 && (writes as f64) <= written,
                "{label}: writes must be between one and one-per-frame — writes={writes} written={written}"
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{label}: egress never quiesced — accepted={accepted} written={written}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn inbound_queue_conservation() {
                let p = pair().await;
                scenario_inbound_queue_conservation(&p).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn egress_conservation() {
                let p = pair().await;
                scenario_egress_conservation(&p).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn inbound_queue_conservation() {
        let (p, _dir) = pair().await;
        scenario_inbound_queue_conservation(&p).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn egress_conservation() {
        let (p, _dir) = pair().await;
        scenario_egress_conservation(&p).await;
    }
}

// ---------------------------------------------------------------------------
// ZMQ: outbound accepted must not be double-counted
// ---------------------------------------------------------------------------
//
// Not run through `transport_metrics_tests!`: that macro's full suite assumes
// every scenario already holds for every transport, which is a broader claim
// than this fix makes. This one scenario is what the fix actually changes.

#[cfg(feature = "zmq")]
mod zmq {
    use super::*;

    /// NATS and ZMQ each used to record the outbound-accepted frame twice:
    /// once via `finalize_send_outcome` (transports.rs) when `send_message`
    /// returns `Admitted` — true the instant the frame reaches the sender's
    /// internal channel — and again from their own sender task/thread once
    /// the frame actually reached the wire. This test pins ZMQ; NATS shares
    /// the same `finalize_send_outcome` call site and the same shape of fix
    /// (a redundant `record_frame(Direction::Outbound, ...)` deleted from its
    /// sender task) but needs a running NATS server to exercise here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn outbound_accepted_is_counted_once_per_send() {
        let pair = VeloPair::new(new_zmq_transport(), new_zmq_transport()).await;

        let handler = Handler::am_handler("sink", |_ctx| Ok(())).build();
        pair.server.register_handler(handler).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Warm-up send: a client's first contact with a peer lazily triggers
        // one `_hello` handshake frame ahead of the message itself
        // (messenger/client/mod.rs), so counting from before ANY send would
        // conflate the handshake with the send this test is actually about.
        // Settle that here, outside the measurement window.
        pair.client
            .am_send("sink")
            .unwrap()
            .raw_payload(Bytes::from_static(b"warmup"))
            .instance(pair.server.instance_id())
            .send()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;

        let outbound_accepted = &[
            ("transport", "zmq"),
            ("direction", "outbound"),
            ("outcome", "accepted"),
        ];
        let before = pair
            .client_snap()
            .counter_sum("velo_transport_frames_total", outbound_accepted);

        pair.client
            .am_send("sink")
            .unwrap()
            .raw_payload(Bytes::from_static(b"fire"))
            .instance(pair.server.instance_id())
            .send()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;

        let after = pair
            .client_snap()
            .counter_sum("velo_transport_frames_total", outbound_accepted);
        assert_eq!(
            after - before,
            1.0,
            "one am_send must add exactly one outbound-accepted frame, not two; before={before} after={after}"
        );
    }
}

// ---------------------------------------------------------------------------
// Ordered dispatch
// ---------------------------------------------------------------------------
//
// Ordering lanes sit above the transport, so these are not parameterised across
// transports. This is also how lane reaping is asserted end to end, without
// adding a public introspection API just for tests.

mod ordered_dispatch {
    use super::*;

    /// Traffic-phase handler. Reaping is disabled here so the lane-count
    /// assertion cannot race a TTL that expires mid-send.
    const HANDLER: &str = "ordered_metrics";
    const LANE_LABEL: [(&str, &str); 1] = [("handler", HANDLER)];

    /// Reap-phase handler, exercised separately with a short TTL so the two
    /// concerns never contend.
    const REAPED_HANDLER: &str = "ordered_metrics_reaped";
    const REAPED_LABEL: [(&str, &str); 1] = [("handler", REAPED_HANDLER)];

    /// Polls `predicate` until it holds, failing rather than hanging.
    async fn wait_for(label: &str, mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for: {label}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lane_metrics_track_traffic_and_reaping() {
        // Two senders so the lane gauge is distinguishable from "one lane".
        let server_reg = Registry::new();
        let server_metrics = Arc::new(VeloMetrics::register(&server_reg).unwrap());
        let server = Velo::builder()
            .add_transport(new_tcp_transport())
            .metrics(server_metrics)
            .build()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut clients = Vec::new();
        for _ in 0..2 {
            let client = Velo::builder()
                .add_transport(new_tcp_transport())
                .build()
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.register_peer(server.peer_info()).unwrap();
            server.register_peer(client.peer_info()).unwrap();
            clients.push(client);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let handled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler_handled = Arc::clone(&handled);
        let handler = Handler::am_handler_async(HANDLER, move |_ctx| {
            let handled = Arc::clone(&handler_handled);
            async move {
                handled.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                Ok(())
            }
        })
        // Reaping off: the lane-count assertion below must not race a TTL
        // expiring while the second client is still handshaking and sending.
        .ordered_with(OrderedConfig::by_sender().with_idle_lane_ttl(None))
        .build();
        server.register_handler(handler).unwrap();

        let reaped = Handler::am_handler_async(REAPED_HANDLER, move |_ctx| async move { Ok(()) })
            .ordered_with(
                OrderedConfig::by_sender().with_idle_lane_ttl(Some(Duration::from_millis(100))),
            )
            .build();
        server.register_handler(reaped).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        for client in &clients {
            for _ in 0..5 {
                client
                    .am_send(HANDLER)
                    .unwrap()
                    .raw_payload(Bytes::from_static(b"x"))
                    .instance(server.instance_id())
                    .send()
                    .await
                    .unwrap();
            }
        }

        wait_for("all messages handled", || {
            handled.load(std::sync::atomic::Ordering::Acquire) == 10
        })
        .await;

        // `handled` is bumped inside the user closure, but `dequeued()` only
        // runs once the whole adapter future resolves — so poll the gauge
        // itself rather than asserting on it the instant the counter lands.
        wait_for("queue depth drained", || {
            MetricSnapshot::from_registry(&server_reg)
                .gauge("velo_messenger_ordered_lane_depth", &LANE_LABEL)
                == 0.0
        })
        .await;

        let snap = MetricSnapshot::from_registry(&server_reg);
        assert_eq!(
            snap.gauge("velo_messenger_ordered_lanes", &LANE_LABEL),
            2.0,
            "one lane per sending instance"
        );
        assert!(
            snap.histogram_count("velo_messenger_ordered_lane_wait_seconds", &LANE_LABEL) >= 10,
            "every message should record a lane-wait observation"
        );
        assert!(
            snap.counter("velo_messenger_ordered_lanes_created_total", &LANE_LABEL) >= 2.0,
            "lane creations are counted per sending instance"
        );

        // Reap phase, on its own handler so nothing above can race it: one
        // message, then idle past the TTL. The lane gauge returns to zero while
        // the creation counter, being monotonic, remembers the lane.
        clients[0]
            .am_send(REAPED_HANDLER)
            .unwrap()
            .raw_payload(Bytes::from_static(b"x"))
            .instance(server.instance_id())
            .send()
            .await
            .unwrap();

        // Wait for the lane to exist before waiting for it to go away — the
        // gauge reads 0.0 both before creation and after reaping, so polling
        // straight for 0.0 would pass without the lane ever having been built.
        wait_for("lane created", || {
            MetricSnapshot::from_registry(&server_reg)
                .counter("velo_messenger_ordered_lanes_created_total", &REAPED_LABEL)
                >= 1.0
        })
        .await;
        wait_for("lane reaped", || {
            MetricSnapshot::from_registry(&server_reg)
                .gauge("velo_messenger_ordered_lanes", &REAPED_LABEL)
                == 0.0
        })
        .await;

        let snap = MetricSnapshot::from_registry(&server_reg);
        assert!(
            snap.counter("velo_messenger_ordered_lanes_created_total", &REAPED_LABEL) >= 1.0,
            "lane creations are counted even after the lane is reaped"
        );
    }
}
