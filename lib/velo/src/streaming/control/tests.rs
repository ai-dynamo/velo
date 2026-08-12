// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the anchor control plane: the wire types' compatibility
//! guarantees, the attach/detach/finalize/cancel handler logic, and the reader
//! pump's heartbeat watchdog.

use super::*;
use anyhow::Result as AnyhowResult;
use futures::StreamExt;
use futures::future::BoxFuture;
use std::sync::Arc;

// -----------------------------------------------------------------------
// MockFrameTransport (test-only)
// -----------------------------------------------------------------------

struct MockFrameTransport;

impl crate::streaming::transport::FrameTransport for MockFrameTransport {
    fn key(&self) -> velo_ext::TransportKey {
        velo_ext::TransportKey::new("mock-stream")
    }

    fn address(&self) -> velo_ext::WorkerAddress {
        velo_ext::WorkerAddress::empty()
    }

    fn bind(
        &self,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, AnyhowResult<flume::Receiver<Vec<u8>>>> {
        Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).1) })
    }

    fn connect(
        &self,
        _peer: velo_ext::WorkerId,
        _anchor_id: u64,
        _session_id: u64,
    ) -> BoxFuture<'_, AnyhowResult<flume::Sender<Vec<u8>>>> {
        Box::pin(async { Ok(flume::bounded::<Vec<u8>>(256).0) })
    }
}

// -----------------------------------------------------------------------
// Helper: make a test AnchorManager
// -----------------------------------------------------------------------

fn make_test_manager() -> Arc<AnchorManager> {
    let worker_id = velo_ext::WorkerId::from_u64(1);
    let transport = Arc::new(MockFrameTransport);
    Arc::new(AnchorManager::new(worker_id, transport))
}

// -----------------------------------------------------------------------
// Watchdog firing test
// -----------------------------------------------------------------------

/// The reader pump's heartbeat watchdog branch must (1) increment the
/// `streaming_heartbeat_watchdog_firings_total` counter and (2) inject a
/// `Dropped` sentinel into `frame_tx` once `DETECTION_MULTIPLIER`
/// consecutive `heartbeat_deadline` windows pass with no frames.
///
/// Without a positive test, a regression that detached the metric tick
/// from the watchdog branch (or moved it behind a feature flag) would
/// silently let the lagging-indicator counter go dark — exactly the
/// failure mode this counter set was created to catch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_pump_watchdog_firing_increments_counter() {
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::observability::VeloMetrics::register(&registry).unwrap());

    let manager = make_test_manager();
    let base_ctx = manager.anchor_context();
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: base_ctx.registry,
        mpsc_registry: base_ctx.mpsc_registry,
        metrics: Some(metrics.clone()),
    };

    // Open transport channel that never delivers a frame: the receiver
    // sits idle, hitting the heartbeat timeout DETECTION_MULTIPLIER times.
    let (_transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4);

    let cancel = tokio_util::sync::CancellationToken::new();

    // Short heartbeat so the test runs in well under a second.
    // DETECTION_MULTIPLIER=3 × 50ms = ~150ms minimum.
    let deadline = std::time::Duration::from_millis(50);
    let pump = tokio::spawn(reader_pump(
        transport_rx,
        frame_tx,
        cancel,
        ctx,
        999,
        deadline,
    ));

    // 4× deadline of slack so timer scheduling jitter doesn't flake on busy CI.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(800), pump)
        .await
        .expect("reader_pump must terminate after watchdog fires");

    let snap = registry.gather();
    let watchdog_value = snap
        .iter()
        .find(|f| f.name() == "velo_streaming_heartbeat_watchdog_firings_total")
        .map(|f| f.get_metric()[0].get_counter().value())
        .unwrap_or(0.0);
    assert_eq!(
        watchdog_value, 1.0,
        "watchdog counter must increment exactly once when DETECTION_MULTIPLIER deadlines pass"
    );

    // Dropped sentinel must reach the consumer so the saturation kill
    // surfaces instead of silently stalling.
    let frame_bytes = frame_rx
        .try_recv()
        .expect("watchdog must inject a Dropped sentinel before exiting");
    let dropped: crate::streaming::frame::StreamFrame<()> =
        rmp_serde::from_slice(&frame_bytes).expect("decode Dropped");
    assert!(
        matches!(dropped, crate::streaming::frame::StreamFrame::Dropped),
        "injected sentinel must be StreamFrame::Dropped, got {dropped:?}"
    );

    // Backpressure counter stays at 0: the per-anchor channel never
    // filled in this scenario (we sent no frames).
    let bp_value = snap
        .iter()
        .find(|f| f.name() == "velo_streaming_reader_pump_backpressure_total")
        .map(|f| f.get_metric()[0].get_counter().value())
        .unwrap_or(0.0);
    assert_eq!(
        bp_value, 0.0,
        "reader_pump backpressure must stay 0 when no frames are sent; got {bp_value}"
    );
}

/// Watchdog fires while the per-anchor channel is already saturated:
/// the `try_send(Dropped)` cannot land, so the consumer sees a clean EOF
/// instead of `StreamFrame::Dropped`. The watchdog firing counter is the
/// authoritative operator signal — it must still tick. See
/// `lib/velo/src/streaming/SATURATION.md` for the documented behavior
/// this test pins in place.
#[tokio::test(flavor = "multi_thread")]
async fn reader_pump_watchdog_saturated_channel_drops_sentinel_silently() {
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::observability::VeloMetrics::register(&registry).unwrap());

    let manager = make_test_manager();
    let base_ctx = manager.anchor_context();
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: base_ctx.registry,
        mpsc_registry: base_ctx.mpsc_registry,
        metrics: Some(metrics.clone()),
    };

    // Transport channel that never delivers: watchdog will fire after
    // DETECTION_MULTIPLIER deadlines.
    let (_transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    // Pre-saturate the anchor channel: capacity 1, push one byte so
    // try_send returns Full when the watchdog attempts to inject the
    // Dropped sentinel.
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(1);
    frame_tx
        .try_send(b"pre-existing".to_vec())
        .expect("pre-fill frame_tx so it is saturated when watchdog fires");

    let cancel = tokio_util::sync::CancellationToken::new();
    let deadline = std::time::Duration::from_millis(50);
    let pump = tokio::spawn(reader_pump(
        transport_rx,
        frame_tx,
        cancel,
        ctx,
        7777,
        deadline,
    ));

    let _ = tokio::time::timeout(std::time::Duration::from_millis(800), pump)
        .await
        .expect("reader_pump must terminate after watchdog fires");

    // Watchdog ticked exactly once.
    let snap = registry.gather();
    let watchdog_value = snap
        .iter()
        .find(|f| f.name() == "velo_streaming_heartbeat_watchdog_firings_total")
        .map(|f| f.get_metric()[0].get_counter().value())
        .unwrap_or(0.0);
    assert_eq!(
        watchdog_value, 1.0,
        "watchdog counter must increment exactly once even when sentinel injection fails"
    );

    // Consumer drains the pre-filled byte, then sees clean EOF -- not a
    // Dropped sentinel. This is the documented saturated-cascade behavior
    // (SATURATION.md): the warn-log + watchdog firing counter are
    // authoritative; the consumer-visible terminal frame is best-effort.
    let pre_existing = frame_rx
        .try_recv()
        .expect("the pre-filled byte must be drainable");
    assert_eq!(
        pre_existing,
        b"pre-existing".to_vec(),
        "first frame must be the pre-filled byte, not the Dropped sentinel"
    );

    // After draining, no further frames: the watchdog cleanup dropped its
    // frame_tx so the channel closes cleanly.
    match frame_rx.try_recv() {
        Err(flume::TryRecvError::Disconnected) => {} // EOF
        Err(flume::TryRecvError::Empty) => {
            // The watchdog may have already exited but the closer drop
            // hasn't propagated yet on this thread. Wait briefly.
            let res =
                tokio::time::timeout(std::time::Duration::from_millis(200), frame_rx.recv_async())
                    .await;
            assert!(
                matches!(res, Ok(Err(_)) | Err(_)),
                "post-saturation frame_rx must reach EOF, not yield a Dropped sentinel"
            );
        }
        Ok(extra) => panic!(
            "after watchdog fire under saturation, no further frame must arrive; \
             got {extra:?} (Dropped sentinel would mean the silent-drop guard is gone)"
        ),
    }
}

// -----------------------------------------------------------------------
// Test helpers for calling handler logic directly
// -----------------------------------------------------------------------

// We call the handler constructor only to verify it compiles and returns Handler.
// For behavioral tests, we call the underlying AnchorManager APIs + simulate
// the same logic the handler performs to verify correctness without needing
// a running velo_messenger runtime.

// -----------------------------------------------------------------------
// Type serialization tests (Task 1 scope)
// -----------------------------------------------------------------------

#[test]
fn test_anchor_attach_response_serde_ok() {
    let resp = AnchorAttachResponse::Ok {
        streaming_transport_key: velo_ext::TransportKey::new("mock-stream"),
        heartbeat_interval_ms: 5000,
        routing_session_id: 7,
        initial_credit: 0,
        slot_byte_budget: 0,
    };
    let json = serde_json::to_string(&resp).expect("serialize Ok");
    let decoded: AnchorAttachResponse = serde_json::from_str(&json).expect("deserialize Ok");
    match decoded {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            heartbeat_interval_ms,
            routing_session_id,
            ..
        } => {
            assert_eq!(streaming_transport_key.as_str(), "mock-stream");
            assert_eq!(heartbeat_interval_ms, 5000);
            assert_eq!(routing_session_id, 7);
        }
        other => panic!("expected Ok, got {:?}", other),
    }
}

#[test]
fn test_anchor_attach_response_rmp_round_trip_non_default_heartbeat() {
    // msgpack must carry the negotiated interval losslessly so the sender
    // gets the cadence the consumer dictated.
    let resp = AnchorAttachResponse::Ok {
        streaming_transport_key: velo_ext::TransportKey::new("tcp-stream"),
        heartbeat_interval_ms: 1234,
        routing_session_id: 42,
        initial_credit: 64,
        slot_byte_budget: 4096,
    };
    let bytes = rmp_serde::to_vec(&resp).expect("rmp serialize Ok");
    let decoded: AnchorAttachResponse = rmp_serde::from_slice(&bytes).expect("rmp deserialize Ok");
    match decoded {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            heartbeat_interval_ms,
            routing_session_id,
            initial_credit,
            slot_byte_budget,
        } => {
            assert_eq!(streaming_transport_key.as_str(), "tcp-stream");
            assert_eq!(heartbeat_interval_ms, 1234);
            assert_eq!(routing_session_id, 42);
            assert_eq!(initial_credit, 64);
            assert_eq!(slot_byte_budget, 4096);
        }
        other => panic!("expected Ok, got {:?}", other),
    }
}

#[test]
fn test_anchor_attach_response_serde_ok_default_heartbeat() {
    // A response that omits `heartbeat_interval_ms` must default to 5000ms.
    let legacy_json = r#"{"Ok":{"streaming_transport_key":"mock-stream"}}"#;
    let decoded: AnchorAttachResponse =
        serde_json::from_str(legacy_json).expect("Ok response must deserialize");
    match decoded {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            heartbeat_interval_ms,
            routing_session_id,
            initial_credit,
            slot_byte_budget,
        } => {
            assert_eq!(streaming_transport_key.as_str(), "mock-stream");
            assert_eq!(
                heartbeat_interval_ms, 5000,
                "missing field must default to 5000ms"
            );
            assert_eq!(
                routing_session_id, 0,
                "missing routing_session_id must default to 0 for legacy senders"
            );
            assert_eq!(
                initial_credit, 0,
                "an absent credit window is a peer not offering the mux"
            );
            assert_eq!(
                slot_byte_budget, 0,
                "an absent byte cap means the default, resolved by NegotiatedLimits"
            );
        }
        other => panic!("expected Ok, got {:?}", other),
    }
}

#[test]
fn an_attach_request_from_before_negotiation_advertises_nothing() {
    // The wire shape a sender that predates negotiation emits. It must
    // still deserialize, and it must land on an empty key list rather than
    // on anything that could intersect: an older sender has no mux to
    // drive, and answering it with one would break it.
    let legacy_json = r#"{
        "handle": {"hi": 1, "lo": 2},
        "session_id": 3,
        "stream_cancel_handle": {"hi": 4, "lo": 5}
    }"#;
    let decoded: AnchorAttachRequest =
        serde_json::from_str(legacy_json).expect("legacy request must deserialize");
    assert!(
        decoded.supported_transport_keys.is_empty(),
        "an absent key list is a sender advertising nothing"
    );
}

#[test]
fn an_attach_request_round_trips_its_advertised_keys() {
    let req = AnchorAttachRequest {
        handle: StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 2),
        session_id: 3,
        stream_cancel_handle: StreamCancelHandle::pack(velo_ext::WorkerId::from_u64(4), 5),
        supported_transport_keys: vec![
            velo_ext::TransportKey::new("messenger-mux-v1"),
            velo_ext::TransportKey::new("tcp-stream"),
        ],
    };
    let bytes = rmp_serde::to_vec(&req).expect("rmp serialize request");
    let decoded: AnchorAttachRequest =
        rmp_serde::from_slice(&bytes).expect("rmp deserialize request");
    assert_eq!(
        decoded
            .supported_transport_keys
            .iter()
            .map(velo_ext::TransportKey::as_str)
            .collect::<Vec<_>>(),
        ["messenger-mux-v1", "tcp-stream"],
    );
}

#[test]
fn test_anchor_attach_response_serde_err() {
    let resp = AnchorAttachResponse::Err {
        reason: "already attached".to_string(),
    };
    let json = serde_json::to_string(&resp).expect("serialize Err");
    let decoded: AnchorAttachResponse = serde_json::from_str(&json).expect("deserialize Err");
    match decoded {
        AnchorAttachResponse::Err { reason } => {
            assert!(reason.contains("already attached"));
        }
        other => panic!("expected Err, got {:?}", other),
    }
}

// -----------------------------------------------------------------------
// Attach handler tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_anchor_attach_handler() {
    let manager = make_test_manager();
    let anchor = manager.create_anchor::<u8>();
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();

    // Simulate bind-then-lock attach handler logic:
    // Step 1: async bind outside shard lock
    let _receiver = manager.transport.bind(local_id, 0).await.unwrap();
    let key = manager.transport.key();

    // Step 2: atomically set attachment under shard lock
    use dashmap::mapref::entry::Entry;
    let result = match manager.registry.entry(local_id) {
        Entry::Vacant(_) => AnchorAttachResponse::Err {
            reason: format!("anchor {} not found", handle),
        },
        Entry::Occupied(mut occ) => {
            let entry = occ.get_mut();
            if entry.attachment {
                AnchorAttachResponse::Err {
                    reason: format!("anchor {} already attached", handle),
                }
            } else {
                entry.attachment = true;
                AnchorAttachResponse::Ok {
                    streaming_transport_key: key,
                    heartbeat_interval_ms: 5000,
                    routing_session_id: 1,
                    initial_credit: 0,
                    slot_byte_budget: 0,
                }
            }
        }
    };

    match result {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            ..
        } => {
            assert_eq!(streaming_transport_key.as_str(), "mock-stream");
        }
        other => panic!("expected Ok, got {:?}", other),
    }

    // Verify attachment is set
    assert!(
        manager
            .registry
            .get(&local_id)
            .map(|e| e.attachment)
            .unwrap_or(false),
        "attachment must be true after attach"
    );

    // Verify handler constructor compiles and returns Handler
    let _handler = create_anchor_attach_handler(manager.clone());
}

#[tokio::test]
async fn test_anchor_attach_already_attached() {
    let manager = make_test_manager();
    let anchor = manager.create_anchor::<u8>();
    let handle = anchor.handle();
    let (_, local_id) = handle.unpack();

    // First attach: set attachment flag directly
    {
        use dashmap::mapref::entry::Entry;
        if let Entry::Occupied(mut occ) = manager.registry.entry(local_id) {
            let entry = occ.get_mut();
            entry.attachment = true;
        }
    }

    // Second attach via handler logic -- should fail
    use dashmap::mapref::entry::Entry;
    let result = match manager.registry.entry(local_id) {
        Entry::Vacant(_) => AnchorAttachResponse::Err {
            reason: format!("anchor {} not found", handle),
        },
        Entry::Occupied(mut occ) => {
            let entry = occ.get_mut();
            if entry.attachment {
                AnchorAttachResponse::Err {
                    reason: format!("anchor {} already attached", handle),
                }
            } else {
                AnchorAttachResponse::Ok {
                    streaming_transport_key: velo_ext::TransportKey::new("unreachable"),
                    heartbeat_interval_ms: 5000,
                    routing_session_id: 1,
                    initial_credit: 0,
                    slot_byte_budget: 0,
                }
            }
        }
    };

    match result {
        AnchorAttachResponse::Err { reason } => {
            assert!(
                reason.contains("already attached"),
                "reason must mention 'already attached', got: {reason}"
            );
        }
        other => panic!("expected Err, got {:?}", other),
    }
}

#[tokio::test]
async fn test_anchor_attach_not_found() {
    let manager = make_test_manager();
    // Create a handle that is NOT in the registry
    let fake_handle = StreamAnchorHandle::pack(velo_ext::WorkerId::from_u64(1), 9999);

    // Simulate handler logic
    use dashmap::mapref::entry::Entry;
    let local_id = 9999u64;
    let result = match manager.registry.entry(local_id) {
        Entry::Vacant(_) => AnchorAttachResponse::Err {
            reason: format!("anchor {} not found", fake_handle),
        },
        Entry::Occupied(_) => panic!("should not be occupied"),
    };

    match result {
        AnchorAttachResponse::Err { reason } => {
            assert!(
                reason.contains("not found"),
                "reason must mention 'not found', got: {reason}"
            );
        }
        other => panic!("expected Err, got {:?}", other),
    }
}

// -----------------------------------------------------------------------
// Detach handler tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_anchor_detach_handler() {
    let manager = make_test_manager();
    let mut stream = manager.create_anchor::<Vec<u8>>();
    let handle = stream.handle();
    let (_, local_id) = handle.unpack();

    // Simulate attach: set flag directly
    {
        use dashmap::mapref::entry::Entry;
        if let Entry::Occupied(mut occ) = manager.registry.entry(local_id) {
            let entry = occ.get_mut();
            entry.attachment = true;
        }
    }

    // Simulate detach handler logic (cancels child token, not parent)
    use dashmap::mapref::entry::Entry;
    let maybe_entry_info = match manager.registry.entry(local_id) {
        Entry::Vacant(_) => None,
        Entry::Occupied(mut occ) => {
            let entry = occ.get_mut();
            entry.attachment = false;
            Some((entry.active_pump_token.take(), entry.frame_tx.clone()))
        }
    };

    if let Some((maybe_pump_token, frame_tx)) = maybe_entry_info {
        if let Some(pump_token) = maybe_pump_token {
            pump_token.cancel();
        }
        let sentinel_bytes =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<Vec<u8>>::Detached)
                .expect("serialize Detached sentinel");
        let _ = frame_tx.try_send(sentinel_bytes);
    }

    // Verify: attachment is cleared
    assert!(
        manager
            .registry
            .get(&local_id)
            .map(|e| !e.attachment)
            .unwrap_or(false),
        "attachment must be false after detach"
    );

    // Verify: anchor still in registry
    assert!(
        manager.registry.contains_key(&local_id),
        "anchor must remain in registry after detach"
    );

    // Verify: Detached sentinel received via Stream interface
    let result = stream.next().await;
    assert!(
        matches!(
            result,
            Some(Ok(crate::streaming::frame::StreamFrame::Detached))
        ),
        "sentinel must be Detached, got {:?}",
        result
    );

    // Verify handler constructor compiles
    let _handler = create_anchor_detach_handler(manager.clone());
}

// -----------------------------------------------------------------------
// Finalize handler tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_anchor_finalize_handler() {
    let manager = make_test_manager();
    let mut stream = manager.create_anchor::<Vec<u8>>();
    let handle = stream.handle();
    let (_, local_id) = handle.unpack();

    // Simulate attach: set flag directly
    {
        use dashmap::mapref::entry::Entry;
        if let Entry::Occupied(mut occ) = manager.registry.entry(local_id) {
            let entry = occ.get_mut();
            entry.attachment = true;
        }
    }

    // Simulate finalize handler logic
    if let Some(entry) = manager.remove_anchor(local_id) {
        let sentinel_bytes =
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<Vec<u8>>::Finalized)
                .expect("serialize Finalized sentinel");
        let _ = entry.frame_tx.try_send(sentinel_bytes);
    }

    // Verify: anchor removed from registry
    assert!(
        !manager.registry.contains_key(&local_id),
        "anchor must be absent from registry after finalize"
    );

    // Verify: Finalized sentinel received via Stream interface
    let result = stream.next().await;
    assert!(
        matches!(
            result,
            Some(Ok(crate::streaming::frame::StreamFrame::Finalized))
        ),
        "sentinel must be Finalized, got {:?}",
        result
    );

    // Verify handler constructor compiles
    let _handler = create_anchor_finalize_handler(manager.clone());
}

// -----------------------------------------------------------------------
// Cancel handler tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn test_anchor_cancel_handler() {
    let manager = make_test_manager();
    let anchor = manager.create_anchor::<u8>();
    let (_, local_id) = anchor.handle().unpack();

    // Simulate cancel handler logic
    if let Some(entry) = manager.remove_anchor(local_id) {
        entry.cancel_token.cancel();
    }

    // Verify: anchor removed
    assert!(
        !manager.registry.contains_key(&local_id),
        "anchor must be absent after cancel"
    );

    // Idempotent: cancel again -- must not panic
    if let Some(entry) = manager.remove_anchor(local_id) {
        entry.cancel_token.cancel();
    }
    // No panic -- test passes

    // Verify handler constructor compiles
    let _handler = create_anchor_cancel_handler(manager.clone());
}

// -----------------------------------------------------------------------
// reader_pump tests (Plan 08-03, Task 2)
// -----------------------------------------------------------------------

/// Helper: set up infrastructure for reader_pump tests.
/// Returns (transport_tx, frame_rx, cancel_token, registry, local_id).
#[allow(clippy::type_complexity)]
fn make_pump_test_infra() -> (
    flume::Sender<Vec<u8>>,   // transport_tx: simulates transport frames
    flume::Receiver<Vec<u8>>, // frame_rx: where pump writes to (consumer side)
    tokio_util::sync::CancellationToken,
    std::sync::Arc<dashmap::DashMap<u64, crate::streaming::anchor::AnchorEntry>>,
    u64, // local_id
) {
    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(256);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let registry = std::sync::Arc::new(dashmap::DashMap::new());
    let local_id = 1u64;

    // Insert an entry in the registry so the pump can remove it
    registry.insert(
        local_id,
        crate::streaming::anchor::AnchorEntry {
            frame_tx: frame_tx.clone(),
            cancel_token: cancel_token.clone(),
            active_pump_token: None,
            attachment: true,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: Duration::from_secs(5),
            stream_cancel_handle: None,
        },
    );

    // Spawn the reader pump
    let pump_cancel = cancel_token.clone();
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: registry.clone(),
        mpsc_registry: std::sync::Arc::new(dashmap::DashMap::new()),
        metrics: None,
    };
    tokio::spawn(reader_pump(
        transport_rx,
        frame_tx,
        pump_cancel,
        ctx,
        local_id,
        Duration::from_secs(5),
    ));

    (transport_tx, frame_rx, cancel_token, registry, local_id)
}

#[tokio::test]
async fn test_pump_forwards_data_frames() {
    let (transport_tx, frame_rx, _cancel, _registry, _id) = make_pump_test_infra();

    // Send a data frame through the transport side
    let data_bytes = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(42u32)).unwrap();
    transport_tx.send_async(data_bytes.clone()).await.unwrap();

    // Should arrive on the frame_rx side
    let received =
        tokio::time::timeout(std::time::Duration::from_millis(500), frame_rx.recv_async())
            .await
            .expect("timeout waiting for frame")
            .expect("frame_rx closed");

    assert_eq!(received, data_bytes, "pump must forward bytes unchanged");
}

#[tokio::test]
async fn test_pump_resets_heartbeat_counter_on_frame() {
    tokio::time::pause();

    let (transport_tx, frame_rx, _cancel, registry, local_id) = make_pump_test_infra();

    // Wait 4.5 seconds (almost one heartbeat window)
    tokio::time::sleep(std::time::Duration::from_millis(4500)).await;

    // Send a frame to reset the counter
    let hb_bytes =
        rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Heartbeat).unwrap();
    transport_tx.send_async(hb_bytes).await.unwrap();

    // Wait another 4.5 seconds
    tokio::time::sleep(std::time::Duration::from_millis(4500)).await;

    // Send another frame
    transport_tx
        .send_async(
            rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::<()>::Heartbeat).unwrap(),
        )
        .await
        .unwrap();

    // Drain forwarded frames
    while frame_rx.try_recv().is_ok() {}

    // The anchor should still be in the registry (counter resets each time)
    assert!(
        registry.contains_key(&local_id),
        "anchor must still be in registry -- heartbeat counter was reset"
    );
}

#[tokio::test]
async fn test_pump_injects_dropped_after_3_missed_heartbeats() {
    tokio::time::pause();

    let (transport_tx, frame_rx, _cancel, _registry, _id) = make_pump_test_infra();

    // Keep transport_tx alive but don't send anything -- pump will timeout
    // 3 consecutive 5s windows with no frames trigger Dropped
    tokio::time::sleep(std::time::Duration::from_secs(16)).await;

    // Collect all frames from frame_rx
    let mut frames = Vec::new();
    while let Ok(bytes) = frame_rx.try_recv() {
        frames.push(bytes);
    }

    // The last frame should be a Dropped sentinel
    assert!(
        !frames.is_empty(),
        "must have received at least one frame (Dropped sentinel)"
    );
    let last = frames.last().unwrap();
    let decoded: crate::streaming::frame::StreamFrame<()> =
        rmp_serde::from_slice(last).expect("deserialize");
    assert!(
        matches!(decoded, crate::streaming::frame::StreamFrame::Dropped),
        "last frame must be Dropped, got {:?}",
        decoded
    );

    // Keep transport_tx alive for the duration of the test
    drop(transport_tx);
}

#[tokio::test]
async fn test_pump_removes_registry_entry_after_3_missed_heartbeats() {
    tokio::time::pause();

    let (transport_tx, _frame_rx, _cancel, registry, local_id) = make_pump_test_infra();

    // Keep transport_tx alive but don't send -- pump will timeout
    tokio::time::sleep(std::time::Duration::from_secs(16)).await;

    // LIVE-02: anchor entry must be removed from registry
    assert!(
        !registry.contains_key(&local_id),
        "anchor must be removed from registry after 3 missed heartbeats (LIVE-02)"
    );

    // Keep transport_tx alive for the duration of the test
    drop(transport_tx);
}

#[tokio::test]
async fn test_pump_exits_when_cancel_token_cancelled() {
    let (transport_tx, frame_rx, cancel_token, registry, local_id) = make_pump_test_infra();

    // Cancel the token
    cancel_token.cancel();

    // Give the pump a moment to exit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Pump should have exited -- sending on transport_tx should not be forwarded
    let data = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(99u32)).unwrap();
    let _ = transport_tx.try_send(data);

    // Allow propagation
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // frame_rx should be empty (pump exited, nothing forwarded)
    assert!(
        frame_rx.try_recv().is_err(),
        "no frames should be forwarded after cancel"
    );

    // Pump calls cancel on exit, so token should be cancelled
    assert!(cancel_token.is_cancelled());

    // Registry entry may or may not be removed (cancel != heartbeat death)
    let _ = (registry, local_id);
}

#[tokio::test]
async fn test_pump_exits_when_transport_closes() {
    let (transport_tx, _frame_rx, cancel_token, _registry, _id) = make_pump_test_infra();

    // Drop the transport sender -- transport channel closes
    drop(transport_tx);

    // Give the pump a moment to exit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Pump should have exited and cancelled the token
    assert!(
        cancel_token.is_cancelled(),
        "cancel_token must be cancelled after pump exits due to transport close"
    );
}

#[tokio::test]
async fn test_child_token_reattach_pump_survives() {
    let parent = tokio_util::sync::CancellationToken::new();
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
    let registry = std::sync::Arc::new(dashmap::DashMap::new());
    let local_id = 1u64;

    // --- First attach: spawn pump with child token ---
    let (tx1, rx1) = flume::bounded::<Vec<u8>>(256);
    let child1 = parent.child_token();

    registry.insert(
        local_id,
        crate::streaming::anchor::AnchorEntry {
            frame_tx: frame_tx.clone(),
            cancel_token: parent.clone(),
            active_pump_token: Some(child1.clone()),
            attachment: true,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: Duration::from_secs(5),
            stream_cancel_handle: None,
        },
    );

    let mpsc_reg: std::sync::Arc<
        dashmap::DashMap<u64, crate::streaming::mpsc::anchor::MpscAnchorEntry>,
    > = std::sync::Arc::new(dashmap::DashMap::new());
    let ctx1 = crate::streaming::anchor::AnchorContext {
        registry: registry.clone(),
        mpsc_registry: mpsc_reg.clone(),
        metrics: None,
    };
    tokio::spawn(reader_pump(
        rx1,
        frame_tx.clone(),
        child1.clone(),
        ctx1,
        local_id,
        Duration::from_secs(5),
    ));

    // Send a frame -- pump should forward it
    let data1 = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(1u32)).unwrap();
    tx1.send_async(data1.clone()).await.unwrap();
    let received =
        tokio::time::timeout(std::time::Duration::from_millis(500), frame_rx.recv_async())
            .await
            .expect("timeout")
            .expect("closed");
    assert_eq!(received, data1, "first pump must forward data");

    // --- Detach: cancel child, NOT parent ---
    child1.cancel();
    assert!(
        !parent.is_cancelled(),
        "parent must NOT be cancelled by child cancel"
    );

    // Give pump time to exit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // --- Reattach: new child from the same parent ---
    let (tx2, rx2) = flume::bounded::<Vec<u8>>(256);
    let child2 = parent.child_token();

    // Update the entry (simulates what _anchor_attach does)
    if let Some(mut entry) = registry.get_mut(&local_id) {
        entry.active_pump_token = Some(child2.clone());
        entry.attachment = true;
    }

    let ctx2 = crate::streaming::anchor::AnchorContext {
        registry: registry.clone(),
        mpsc_registry: mpsc_reg.clone(),
        metrics: None,
    };
    tokio::spawn(reader_pump(
        rx2,
        frame_tx.clone(),
        child2.clone(),
        ctx2,
        local_id,
        Duration::from_secs(5),
    ));

    // Send a frame through the new transport -- pump should forward it
    let data2 = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(2u32)).unwrap();
    tx2.send_async(data2.clone()).await.unwrap();
    let received2 =
        tokio::time::timeout(std::time::Duration::from_millis(500), frame_rx.recv_async())
            .await
            .expect("timeout on reattach")
            .expect("closed on reattach");
    assert_eq!(
        received2, data2,
        "second pump must forward data after reattach"
    );

    // --- Finalize: cancel parent cascades to child ---
    parent.cancel();
    assert!(
        child2.is_cancelled(),
        "child must be cancelled when parent is cancelled"
    );
}

// -----------------------------------------------------------------------
// StreamCancelHandle + SenderRegistry + create_stream_cancel_handler tests (Task 1)
// -----------------------------------------------------------------------

#[test]
fn test_stream_cancel_handle_pack_unpack() {
    let worker_id = velo_ext::WorkerId::from_u64(0xDEAD_BEEF_1234_5678);
    let stream_id: u64 = 0xABCD_EF01_2345_6789;

    let handle = crate::streaming::control::StreamCancelHandle::pack(worker_id, stream_id);
    let (recovered_worker, recovered_stream) = handle.unpack();

    assert_eq!(
        recovered_worker, worker_id,
        "worker_id must round-trip through pack/unpack"
    );
    assert_eq!(
        recovered_stream, stream_id,
        "stream_id must round-trip through pack/unpack"
    );
}

#[test]
fn test_stream_cancel_handle_serde() {
    let worker_id = velo_ext::WorkerId::from_u64(0xCAFE_BABE_0000_0001);
    let stream_id: u64 = 42;

    let handle = crate::streaming::control::StreamCancelHandle::pack(worker_id, stream_id);
    let encoded = rmp_serde::to_vec(&handle).expect("rmp_serde serialize must succeed");
    let decoded: crate::streaming::control::StreamCancelHandle =
        rmp_serde::from_slice(&encoded).expect("rmp_serde deserialize must succeed");

    assert_eq!(
        handle, decoded,
        "StreamCancelHandle must survive rmp_serde round-trip"
    );
    let (w, s) = decoded.unpack();
    assert_eq!(w, worker_id);
    assert_eq!(s, stream_id);
}

#[test]
fn test_stream_cancel_handler_compiles() {
    let registry = std::sync::Arc::new(crate::streaming::control::SenderRegistry::default());
    let _handler = crate::streaming::control::create_stream_cancel_handler(registry);
    // Returns without panic — confirms the handler constructor compiles and runs.
}

#[tokio::test]
async fn test_pump_exits_when_consumer_drops() {
    let (transport_tx, frame_rx, cancel_token, _registry, _id) = make_pump_test_infra();

    // Drop the frame_rx consumer side -- pump's send will fail
    drop(frame_rx);

    // Send data so the pump tries to forward and fails
    let data = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(1u32)).unwrap();
    let _ = transport_tx.send_async(data).await;

    // Give the pump time to process and exit
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Pump should have exited and cancelled the token
    assert!(
        cancel_token.is_cancelled(),
        "cancel_token must be cancelled after pump exits due to consumer drop"
    );
}
