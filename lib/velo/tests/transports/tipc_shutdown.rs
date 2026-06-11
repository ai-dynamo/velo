// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(all(feature = "tipc", target_os = "linux"))]

//! Integration tests for TIPC graceful shutdown.
//!
//! * Standard shutdown scenarios via the `transport_shutdown_tests!` macro with
//!   `TipcShutdownClient` (`TipcStream` as the raw client-side stream).
//! * TIPC-specific invariant 2 pin: a plain `close()` (no `shutdown(Both)`) produces
//!   `ECONNRESET` at the listener, which must be treated as a graceful close — no
//!   `DecodeError` metric, no error log (proposal §2.3 / invariant 2).

#[macro_use]
mod common;

#[allow(unused_imports)]
use common::{TipcShutdownClient, shutdown_scenarios};

transport_shutdown_tests!(tipc, TipcShutdownClient);

// ── TIPC-specific: plain close() is treated as graceful (invariant 2) ─────────

/// Verify that `close(2)` without `shutdown(SHUT_RDWR)` — which the TIPC kernel
/// delivers as `ECONNRESET` to the listener — does **not** increment the
/// `DecodeError` rejection metric when the partial-frame decode buffer is empty.
///
/// Pins proposal invariant 2:
/// > "Plain `close()` surfaces as ECONNRESET at peer.  Listener treats
/// > `ConnectionReset` with empty partial-frame buffer as GRACEFUL close."
///
/// The decode counter must remain zero after normal peer teardown.
#[tokio::test]
async fn test_tipc_plain_close_no_decode_error_metric() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use velo::transports::tcp::TcpFrameCodec;
    use velo::transports::tipc::{TipcEndpoint, TipcStream, TipcTransportBuilder};
    use velo::transports::{MessageType, Transport, make_channels};
    use velo_ext::{
        Direction, InstanceId, TransportKey, TransportObservability, TransportRejection,
    };

    // ── TIPC availability gate ───────────────────────────────────────────────
    let transport = match TipcTransportBuilder::new().build() {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "test_tipc_plain_close_no_decode_error_metric: TIPC not available, skipping: {e}"
            );
            return;
        }
    };

    // ── Counting observability ───────────────────────────────────────────────
    let decode_error_count = Arc::new(AtomicU64::new(0));

    struct CountObs(Arc<AtomicU64>);

    impl TransportObservability for CountObs {
        fn record_frame(&self, _d: Direction, _t: &str, _b: usize) {}
        fn record_rejection(&self, reason: TransportRejection) {
            if reason == TransportRejection::DecodeError {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        fn set_registered_peers(&self, _: usize) {}
        fn set_active_connections(&self, _: usize) {}
        fn record_send_backpressure(&self) {}
    }

    transport.set_observability(Arc::new(CountObs(Arc::clone(&decode_error_count))));

    // ── Start the transport ──────────────────────────────────────────────────
    let (adapter, streams) = make_channels();
    transport
        .start(
            InstanceId::new_v4(),
            adapter,
            tokio::runtime::Handle::current(),
        )
        .await
        .expect("TIPC transport start should succeed");

    // ── Decode the local TIPC endpoint ───────────────────────────────────────
    let ep: TipcEndpoint = {
        let key = TransportKey::from("tipc");
        let bytes = transport.address().get_entry(&key).unwrap().unwrap();
        rmp_serde::from_slice(&bytes).unwrap()
    };

    // ── Connect and send one complete message frame ──────────────────────────
    let mut raw_stream = TipcStream::connect(ep.socket_ref, ep.node, Duration::from_secs(5))
        .await
        .expect("TipcStream::connect should succeed");
    TcpFrameCodec::encode_frame(&mut raw_stream, MessageType::Message, b"hdr", b"pay")
        .await
        .expect("encode_frame should succeed");

    // Wait for the listener to receive and route the frame so the per-connection
    // decode buffer is empty before we close the socket.
    tokio::time::timeout(Duration::from_secs(2), streams.message_stream.recv_async())
        .await
        .expect("message should arrive within 2 s")
        .expect("message channel should be open");

    // ── Plain close(): drop without shutdown(Both) ───────────────────────────
    // Spawning in blocking prevents a stalled tokio worker: TIPC close(2) can
    // block up to 8 s under link congestion (proposal §2.3 close-blocking hazard).
    // On a clean test connection this completes in < 1 ms.
    tokio::task::spawn_blocking(move || drop(raw_stream))
        .await
        .expect("spawn_blocking should complete without panic");

    // Give the TIPC listener 200 ms to process the ECONNRESET and update metrics.
    // TIPC topology delivers the abort notification in < 0.3 ms; 200 ms is ample.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── Assertion ────────────────────────────────────────────────────────────
    assert_eq!(
        decode_error_count.load(Ordering::Relaxed),
        0,
        "ECONNRESET with empty decode buffer must NOT increment DecodeError metric \
         (proposal invariant 2: plain close() = graceful close)"
    );

    // Cleanup: cancel the teardown token to stop the listener accept loop.
    streams.shutdown_state.teardown_token().cancel();
}
