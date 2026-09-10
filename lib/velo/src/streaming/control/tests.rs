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
use std::time::Duration;

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
        PumpContext {
            local_id: 999,
            heartbeat_deadline: deadline,
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
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

/// The sibling reap the transport-closed arm does must also (1) increment
/// `streaming_unclaimed_bind_reaped_total` and (2) inject a `Dropped`
/// sentinel, the same two obligations the watchdog branch above proves --
/// otherwise an operator watching the watchdog counter alone would see
/// nothing for every bind this arm reaps instead (a genuine pre-bind, an
/// ordinary attach, or an adopted attach, whichever never got its `OpenSlot`
/// before the accept window closed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_pump_unclaimed_bind_reap_increments_counter() {
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(crate::observability::VeloMetrics::register(&registry).unwrap());

    let manager = make_test_manager();
    let base_ctx = manager.anchor_context();
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: base_ctx.registry,
        mpsc_registry: base_ctx.mpsc_registry,
        metrics: Some(metrics.clone()),
    };

    let local_id = 999u64;
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4);
    ctx.registry.insert(
        local_id,
        crate::streaming::anchor::AnchorEntry {
            frame_tx: frame_tx.clone(),
            cancel_token: cancel_token.clone(),
            active_pump_token: None,
            attachment: true,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: std::time::Duration::from_secs(5),
            stream_cancel_handle: None,
            prebind: None,
        },
    );

    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    let (wake_tx, _wake_rx) = flume::bounded::<velo_ext::WorkerId>(16);
    let drain = Arc::new(crate::streaming::messenger_mux::ingress::DrainSignal::new(
        wake_tx,
    ));
    let pump_cancel = cancel_token.child_token();
    let pump = tokio::spawn(reader_pump(
        transport_rx,
        frame_tx,
        pump_cancel,
        ctx,
        PumpContext {
            local_id,
            heartbeat_deadline: std::time::Duration::from_secs(5),
            drain: Some(drain),
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    ));

    // Simulate the accept window's `release_bind`/`expire_bind`: it drops
    // the bind's `frame_tx`, the other end of this `transport_rx`. The
    // drain above was never claimed, so this is the reap arm under test,
    // not the watchdog (which would need 3 * 5s to fire).
    drop(transport_tx);
    tokio::time::timeout(std::time::Duration::from_millis(500), pump)
        .await
        .expect("reader_pump must exit once its transport channel closes")
        .expect("pump task must not panic");

    let snap = registry.gather();
    let reaped_value = snap
        .iter()
        .find(|f| f.name() == "velo_streaming_unclaimed_bind_reaped_total")
        .map(|f| f.get_metric()[0].get_counter().value())
        .unwrap_or(0.0);
    assert_eq!(
        reaped_value, 1.0,
        "unclaimed-bind-reaped counter must increment exactly once"
    );

    let frame_bytes = frame_rx
        .try_recv()
        .expect("the reap must inject a Dropped sentinel before exiting");
    let dropped: crate::streaming::frame::StreamFrame<()> =
        rmp_serde::from_slice(&frame_bytes).expect("decode Dropped");
    assert!(
        matches!(dropped, crate::streaming::frame::StreamFrame::Dropped),
        "injected sentinel must be StreamFrame::Dropped, got {dropped:?}"
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
        PumpContext {
            local_id: 7777,
            heartbeat_deadline: deadline,
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
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
            prebind: None,
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
        PumpContext {
            local_id,
            heartbeat_deadline: Duration::from_secs(5),
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    ));

    (transport_tx, frame_rx, cancel_token, registry, local_id)
}

/// Helper: reader_pump infra for a pump spawned over the mux, where a bare
/// [`crate::streaming::messenger_mux::ingress::DrainSignal`] stands in for the
/// mux's own -- `claimed()` is `None` until the test calls `claimed_by`,
/// exactly like a bind no `OpenSlot` has opened yet.
///
/// `prebound` selects which of the two real spawn sites this stands in for:
/// `true` is the zero-RTT pre-bind shape (`AnchorManager::prebind_anchor`);
/// `false` is an ordinary mux attach whose peer just hasn't sent its
/// `OpenSlot` yet. Both start with `drain: Some(unclaimed)` -- the mux parks
/// a `DrainSignal` for every bind, not only a pre-bound one -- which is
/// exactly the distinction `PumpContext::prebound` exists to carry explicitly
/// rather than infer from `drain.claimed()`.
///
/// `attachment` is independent of `prebound`, not `!prebound`: adoption is
/// not where the pair arises -- `AnchorManager::adopt_prebind`'s `Verdict::Adopt`
/// arm sets `attachment` and clears `prebound` (via `PreBind::adopt`) in the
/// same shard-lock hold, so a just-adopted pre-bind is never observed with
/// both set. The pair comes from `attach_stream_anchor`'s co-located branch
/// instead: it sets `attachment` and releases the `PreBind` without ever
/// touching the shared `prebound` flag its now-cancelled pump still reads as
/// `true`, and a fixture that tied the two together could never construct
/// that pair to test it -- which is exactly why the reader pump's
/// `!cancel_token.is_cancelled()` guard is load-bearing there.
///
/// Returns `(transport_tx, drain, frame_rx, cancel_token, registry, local_id)`.
/// `frame_rx` must be kept alive (even if unused) for as long as the pump
/// should run: dropping it disconnects `frame_tx` and the pump exits, same as
/// [`make_pump_test_infra`].
#[allow(clippy::type_complexity)]
fn make_prebind_pump_test_infra(
    heartbeat_deadline: Duration,
    prebound: bool,
    attachment: bool,
) -> (
    flume::Sender<Vec<u8>>,
    Arc<crate::streaming::messenger_mux::ingress::DrainSignal>,
    flume::Receiver<Vec<u8>>,
    tokio_util::sync::CancellationToken,
    std::sync::Arc<dashmap::DashMap<u64, crate::streaming::anchor::AnchorEntry>>,
    u64,
) {
    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(256);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let registry = std::sync::Arc::new(dashmap::DashMap::new());
    let local_id = 1u64;
    let (wake_tx, _wake_rx) = flume::bounded::<velo_ext::WorkerId>(16);
    let drain = Arc::new(crate::streaming::messenger_mux::ingress::DrainSignal::new(
        wake_tx,
    ));

    registry.insert(
        local_id,
        crate::streaming::anchor::AnchorEntry {
            frame_tx: frame_tx.clone(),
            cancel_token: cancel_token.clone(),
            active_pump_token: None,
            attachment,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: heartbeat_deadline,
            stream_cancel_handle: None,
            prebind: None,
        },
    );

    // A child of the entry's token, as every real spawn site derives one
    // (`anchor.rs`'s `prebind_anchor`, `control.rs`'s attach handler): a
    // fixture that instead cloned the parent would make `cancel_token`'s own
    // unconditional cancel-on-exit (`reader_pump`'s last line) indistinguishable
    // from the entry's token being cancelled by a removal this test is trying
    // to observe.
    let pump_cancel = cancel_token.child_token();
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
        PumpContext {
            local_id,
            heartbeat_deadline,
            drain: Some(Arc::clone(&drain)),
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(prebound)),
        },
    ));

    (
        transport_tx,
        drain,
        frame_rx,
        cancel_token,
        registry,
        local_id,
    )
}

/// Finding: the reader pump's heartbeat watchdog was armed at pre-bind time,
/// so a zero-RTT request that waited longer than `DETECTION_MULTIPLIER *
/// heartbeat_interval` for its worker to be scheduled was torn down before
/// its sender ever opened -- an undocumented cap on how long such a request
/// may sit in a queue.
///
/// A timeout with no claim is silence from a producer that does not exist,
/// not proof one died, so it must not count toward the watchdog at all.
#[tokio::test]
async fn test_pump_does_not_reap_an_unclaimed_prebind_on_heartbeat_silence() {
    tokio::time::pause();
    let heartbeat = Duration::from_millis(50);
    let (transport_tx, _drain, _frame_rx, _cancel, registry, local_id) =
        make_prebind_pump_test_infra(heartbeat, true, false);

    // Twice the window that reaps an already-claimed slot (see the sibling
    // test below) with nothing having claimed this one.
    tokio::time::sleep(heartbeat * (2 * DETECTION_MULTIPLIER as u32)).await;

    assert!(
        registry.contains_key(&local_id),
        "an unclaimed pre-bind must survive heartbeat silence -- nothing has \
         opened it yet, so a timeout proves nothing about a sender"
    );

    drop(transport_tx);
}

/// The other half of the same fix: once an `OpenSlot` claims the bind, a
/// sender genuinely exists and the watchdog must reap it exactly as it always
/// has if that sender goes silent. Gating on the claim must delay detection,
/// never defeat it.
#[tokio::test]
async fn test_pump_reaps_a_claimed_prebind_after_missed_heartbeats() {
    tokio::time::pause();
    let heartbeat = Duration::from_millis(50);
    let (transport_tx, drain, _frame_rx, _cancel, registry, local_id) =
        make_prebind_pump_test_infra(heartbeat, true, false);

    // What `open_slot` does to a bind's drain signal when an `OpenSlot`
    // claims it, without a mux in the loop.
    let peer = velo_ext::WorkerId::from_u64(0xABCD);
    let slot = crate::streaming::messenger_mux::protocol::SlotId::new(0, 0).expect("slot id");
    drain.claimed_by(
        peer,
        slot,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        flume::unbounded::<u32>().0,
    );

    // Same generous margin as the sibling test above and as
    // `test_pump_removes_registry_entry_after_3_missed_heartbeats` (3.2x
    // there): a tight `+1` window depends on this pump getting polled between
    // each paused-clock advance in exactly the order the assertion assumes.
    tokio::time::sleep(heartbeat * (2 * DETECTION_MULTIPLIER as u32)).await;

    assert!(
        !registry.contains_key(&local_id),
        "a claimed pre-bind whose sender goes silent must still be reaped"
    );

    drop(transport_tx);
}

/// Finding: an unclaimed pre-bind reclaimed by the mux's accept window (the
/// bind's `frame_tx` -- the other end of this `transport_rx` -- being
/// dropped) left the registry entry behind forever, because the pump's
/// transport-closed arm did nothing but break the loop. Once heartbeat detection
/// is gated on a claim (the fix above), the accept window is the *only*
/// reaper an abandoned pre-bind has, so this exit must do the cleanup the
/// watchdog-fired branch already does.
#[tokio::test]
async fn test_pump_reaps_an_unclaimed_prebind_when_its_bind_is_reclaimed() {
    let (transport_tx, _drain, _frame_rx, cancel_token, registry, local_id) =
        make_prebind_pump_test_infra(Duration::from_secs(5), true, false);

    // Simulate the accept window's `release_bind`/`expire_bind`: it drops the
    // `BindEntry`, and with it the `frame_tx` that feeds this `transport_rx`.
    drop(transport_tx);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        !registry.contains_key(&local_id),
        "an unclaimed pre-bind must be reaped once its bind is reclaimed -- \
         nothing else will, once heartbeat silence no longer can"
    );
    assert!(
        cancel_token.is_cancelled(),
        "reaping the entry must cancel its token, same as the watchdog branch"
    );
}

/// Finding: `drain.claimed().is_none()` is true of an ordinary mux attach's
/// pump too, for as long as the peer's `OpenSlot` is still in flight -- which
/// is always at least until after the attach response this pump was spawned
/// from already returned (see `PumpContext::prebound`). Before gating on
/// `prebound` instead, this pump silently stopped counting heartbeat misses
/// for the same window, so a sender that attached and then died before its
/// first frame went undetected until the mux's 60 s accept-window timer
/// reclaimed the bind, instead of the configured
/// `DETECTION_MULTIPLIER * heartbeat_interval`.
#[tokio::test]
async fn test_pump_reaps_an_ordinary_attach_with_unclaimed_mux_drain_after_missed_heartbeats() {
    tokio::time::pause();
    let heartbeat = Duration::from_millis(50);
    let (transport_tx, _drain, _frame_rx, _cancel, registry, local_id) =
        make_prebind_pump_test_infra(heartbeat, false, true);

    // Same generous margin the claimed-prebind sibling test uses.
    tokio::time::sleep(heartbeat * (2 * DETECTION_MULTIPLIER as u32)).await;

    assert!(
        !registry.contains_key(&local_id),
        "an ordinary attach's pump must still be reaped on heartbeat silence \
         even while the mux's drain signal for its bind reads unclaimed -- \
         a sender already exists here, unlike a real pre-bind"
    );

    drop(transport_tx);
}

/// The transport-closed-arm half of the same finding, corrected: an ordinary or
/// adopted attach's bind closing (its own accept window expiring because the
/// peer never sent an `OpenSlot`) must remove the registry entry exactly as
/// an unclaimed pre-bind's does. "Something else already owns telling the
/// registry" is true only once a sender has actually claimed the bind --
/// gating this arm on `prebound` instead of the claim left a bind that was
/// never claimed by *either* door (a real pre-bind, or an ordinary/adopted
/// attach whose peer died before its first `OpenSlot`) relying solely on the
/// heartbeat watchdog to reap it. That race is not always won: the watchdog
/// restarts counting from whenever this arm's caller stopped exempting it,
/// while the accept window is a fixed 60 s from bind creation, so at
/// `heartbeat_interval >= 20 s` the window always closes first -- and, before
/// this fix, closing first left the registry entry behind forever, with the
/// consumer's `StreamAnchor` wedged on `Poll::Pending`.
#[tokio::test]
async fn test_pump_reaps_an_attached_entry_with_unclaimed_mux_drain_when_its_bind_is_reclaimed() {
    let (transport_tx, _drain, frame_rx, cancel_token, registry, local_id) =
        make_prebind_pump_test_infra(Duration::from_secs(5), false, true);

    // Simulate the peer never opening its slot: the bind's `frame_tx` --
    // the other end of this `transport_rx` -- goes away.
    drop(transport_tx);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        !registry.contains_key(&local_id),
        "an attached entry whose mux drain was never claimed must be reaped \
         when its bind is reclaimed -- nothing else will, once the sender \
         never showed up on the wire"
    );
    assert!(
        cancel_token.is_cancelled(),
        "reaping the entry must cancel its token, same as the watchdog branch"
    );

    let sentinel = frame_rx
        .try_recv()
        .expect("the reap must inject a Dropped sentinel, not a silent close");
    assert_eq!(
        sentinel,
        *crate::streaming::sender::cached_dropped(),
        "the consumer must see SenderDropped, not a bare EOF"
    );
}

/// One timer per stream, not one per record.
///
/// The reader pump used to wrap every `recv_async` in a fresh
/// `tokio::time::timeout`. That registers a timer entry with tokio's driver
/// on the future's first poll and deregisters it on drop, both under the
/// driver's lock, so a stream carrying N records took 2N turns of that lock.
/// On the tier-3 rig those turns were 7.3 percent of the frontend's 72 cores.
/// A behavioural test cannot see the difference -- the same bytes come out
/// either way -- so this counts the arms directly and pins the shape of the
/// count: bounded, rather than one per record. The clock never gets near the
/// hour-long deadline below, so this says nothing about a timer that could
/// fire; `a_stream_under_traffic_never_fires_its_heartbeat_timer` is what
/// pins that.
#[tokio::test]
async fn a_thousand_records_arm_the_heartbeat_timer_a_handful_of_times() {
    tokio::time::pause();

    const RECORDS: usize = 1_000;
    // The arm before the loop is the whole steady state here: the deadline is
    // an hour and the test never advances the clock, so the timer has no
    // reason to fire. The headroom covers the paused clock auto-advancing if
    // the runtime does go idle between records, which costs one re-arm each
    // time. What the bound asserts is that it does not scale with `RECORDS`.
    const MAX_ARMS: u64 = 4;

    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(256);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(256);
    let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: std::sync::Arc::new(dashmap::DashMap::new()),
        mpsc_registry: std::sync::Arc::new(dashmap::DashMap::new()),
        metrics: None,
    };

    tokio::spawn(TIMER_ARMS.scope(
        Arc::clone(&arms),
        reader_pump(
            transport_rx,
            frame_tx,
            tokio_util::sync::CancellationToken::new(),
            ctx,
            PumpContext {
                local_id: 1,
                heartbeat_deadline: Duration::from_secs(3600),
                drain: None,
                prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        ),
    ));

    let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
    for i in 0..RECORDS {
        transport_tx
            .send_async(record.clone())
            .await
            .expect("the pump must still be reading");
        // Drained in step with the send. Letting a thousand records pile up
        // would park this task and the pump at the same time, and an idle
        // runtime under `tokio::time::pause` auto-advances to the sleep's
        // deadline -- firing the very timer this test is counting.
        let forwarded = frame_rx
            .recv_async()
            .await
            .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
        assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
    }

    let armed = arms.load(std::sync::atomic::Ordering::Relaxed);
    // Without this, deleting every `note_timer_arm()` call site leaves
    // `armed` at 0 and the bound below still passes -- an upper bound alone
    // does not prove the seam is wired to anything. The pre-loop arm is
    // unconditional and this point is reached only after a record has been
    // forwarded, so `>= 1` is exact and cannot flake.
    assert!(
        armed >= 1,
        "the pre-loop arm must have counted at least once"
    );
    assert!(
        armed <= MAX_ARMS,
        "reader_pump must arm its heartbeat timer a bounded number of times, not \
         once per record: {RECORDS} records armed it {armed} times (bound {MAX_ARMS})"
    );

    drop(transport_tx);
}

/// The discriminator for the bounded re-arm: a stream that keeps carrying
/// records must not let its heartbeat timer fire at all.
///
/// The first cut of this pump armed one `Sleep` per stream and re-armed it
/// only from its own fired arm. Under traffic that timer fired once every
/// `heartbeat_deadline`, found `idle < deadline`, re-armed and continued --
/// harmless at the 5 s deadline the manager defaults to, but not what the
/// pump's doc comment promises, and with thousands of pumps spawned in the
/// same second those fires arrive as a herd. Pushing the deadline forward
/// from the receive arm, but only once it is within half a deadline of
/// firing, is what removes them.
///
/// `fired == 0` is the whole discriminator here. The arm bound cannot be: the
/// receive-arm re-arm *raises* the arm count, because it moves the deadline up
/// to twice per deadline where the fired arm moved it once. What the arm bound
/// still pins is the property the timer hoist bought -- that the count does not
/// scale with the number of records.
///
/// The bound's derivation: one arm before the loop, plus at most one
/// receive-arm re-arm per `heartbeat_deadline / 2` of traffic. A re-arm sets
/// the deadline a full `heartbeat_deadline` out, and the next one cannot
/// happen until that deadline is back inside half of itself, which is
/// `heartbeat_deadline / 2` later at the earliest. Over `TRAFFIC_MS` that is
/// `TRAFFIC_MS / (DEADLINE_MS / 2)` re-arms; the remaining `+ 1` covers a
/// record landing exactly on a half-window edge.
///
/// `tokio::time::advance` moves the paused clock by an exact amount and gives
/// the runtime one scheduling turn, so every count here is derived rather than
/// measured. Each record is drained in step with its send: letting a backlog
/// build would park this task and the pump at the same time, and an idle
/// runtime under `tokio::time::pause` auto-advances to the pump's own sleep --
/// firing the very timer this test says must not fire.
#[tokio::test]
async fn a_stream_under_traffic_never_fires_its_heartbeat_timer() {
    tokio::time::pause();

    const DEADLINE_MS: u64 = 50;
    const STEP_MS: u64 = 5;
    const TRAFFIC_MS: u64 = 500;
    const RECORDS: usize = (TRAFFIC_MS / STEP_MS) as usize + 1;
    const MAX_ARMS: u64 = 1 + TRAFFIC_MS / (DEADLINE_MS / 2) + 1;

    let deadline = Duration::from_millis(DEADLINE_MS);
    let step = Duration::from_millis(STEP_MS);

    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4);
    let arms = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let fires = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: std::sync::Arc::new(dashmap::DashMap::new()),
        mpsc_registry: std::sync::Arc::new(dashmap::DashMap::new()),
        metrics: None,
    };

    tokio::spawn(TIMER_FIRES.scope(
        Arc::clone(&fires),
        TIMER_ARMS.scope(
            Arc::clone(&arms),
            reader_pump(
                transport_rx,
                frame_tx,
                tokio_util::sync::CancellationToken::new(),
                ctx,
                PumpContext {
                    local_id: 1,
                    heartbeat_deadline: deadline,
                    drain: None,
                    prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
            ),
        ),
    ));

    let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
    for i in 0..RECORDS {
        // Advance between records, never after the last one, so the clock
        // still reads the last frame's instant when the loop ends.
        if i > 0 {
            tokio::time::advance(step).await;
        }
        transport_tx
            .send_async(record.clone())
            .await
            .unwrap_or_else(|_| panic!("the pump must still be reading at record {i}"));
        let forwarded = frame_rx
            .recv_async()
            .await
            .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
        assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
    }

    let fired = fires.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        fired, 0,
        "reader_pump must not fire its timer under traffic: {RECORDS} records \
         {STEP_MS} ms apart under a {DEADLINE_MS} ms deadline fired it {fired} times"
    );

    let armed = arms.load(std::sync::atomic::Ordering::Relaxed);
    // Without this, deleting every `note_timer_arm()` call site leaves `armed`
    // at 0 and the bound below still passes.
    assert!(
        armed >= 1,
        "the pre-loop arm must have counted at least once"
    );
    assert!(
        armed <= MAX_ARMS,
        "reader_pump must re-arm its heartbeat timer at a bounded rate, not once \
         per record: {RECORDS} records armed it {armed} times (bound {MAX_ARMS})"
    );

    drop(transport_tx);
}

/// The other half of the same rule: a stream that stops is still caught
/// `DETECTION_MULTIPLIER` deadlines after its last record, and the fires the
/// test above forbids under traffic do happen once the traffic stops.
///
/// This is also the anti-tautology control for the fire seam. With
/// `note_timer_fire()` deleted, every `fired == 0` assertion passes vacuously;
/// the fire count asserted here is what says the seam is wired to the arm it
/// names.
///
/// The fire count is read as a delta across the traffic/silence boundary
/// rather than as a total, deliberately: the total depends on what the timer
/// did during the traffic phase, and this test has to stay green under the
/// fail-before revert that puts the first cut's shape back -- that is what
/// makes it evidence that detection timing did not move.
///
/// Writing `L` for the last record's instant and `d` for `heartbeat_deadline`,
/// the timer's deadline sits somewhere in `[L + d/2, L + d]` when the traffic
/// stops: the receive arm only ever pushes it to a full `d` out and only when
/// it was inside `d/2`. If it sits short of `L + d` the first fire finds
/// `idle < d`, counts no miss and re-arms to `L + d`; from there the misses
/// land at `L + d`, `L + 2d` and `L + 3d`, which is where a timer rebuilt per
/// record would have put them. So the pump exits at `L + 3d` exactly, having
/// fired `DETECTION_MULTIPLIER` times, or `DETECTION_MULTIPLIER + 1` when the
/// first fire was that no-miss re-arm.
#[tokio::test]
async fn a_stream_that_stops_is_caught_a_detection_window_after_its_last_record() {
    tokio::time::pause();

    const DEADLINE_MS: u64 = 50;
    const STEP_MS: u64 = 5;
    const TRAFFIC_MS: u64 = 500;
    const RECORDS: usize = (TRAFFIC_MS / STEP_MS) as usize + 1;

    let deadline = Duration::from_millis(DEADLINE_MS);
    let step = Duration::from_millis(STEP_MS);

    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(4);
    let fires = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: std::sync::Arc::new(dashmap::DashMap::new()),
        mpsc_registry: std::sync::Arc::new(dashmap::DashMap::new()),
        metrics: None,
    };

    let pump = tokio::spawn(TIMER_FIRES.scope(
        Arc::clone(&fires),
        reader_pump(
            transport_rx,
            frame_tx,
            tokio_util::sync::CancellationToken::new(),
            ctx,
            PumpContext {
                local_id: 1,
                heartbeat_deadline: deadline,
                drain: None,
                prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        ),
    ));

    let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(7u32)).unwrap();
    for i in 0..RECORDS {
        if i > 0 {
            tokio::time::advance(step).await;
        }
        transport_tx
            .send_async(record.clone())
            .await
            .unwrap_or_else(|_| panic!("the pump must still be reading at record {i}"));
        let forwarded = frame_rx
            .recv_async()
            .await
            .unwrap_or_else(|_| panic!("the pump must forward record {i}"));
        assert_eq!(forwarded, record, "record {i} must be forwarded unchanged");
    }

    // The pump stamps `last_frame` right after the forward this loop just
    // drained, and no advance separates the two, so the paused clock reads
    // that same instant here.
    let last_frame = tokio::time::Instant::now();
    let fires_under_traffic = fires.load(std::sync::atomic::Ordering::Relaxed);

    // Bounded rather than a bare await: a watchdog that never fired would
    // otherwise park this test on a channel nothing will ever feed, and a
    // paused clock with no other pending timer has nothing to auto-advance to
    // -- the runner would hang instead of going red.
    tokio::time::timeout(deadline * 20, pump)
        .await
        .expect("the watchdog must give up on a stream that stopped")
        .expect("the pump task must not panic");

    let caught_after = last_frame.elapsed();
    let window = deadline * u32::from(DETECTION_MULTIPLIER);
    assert!(
        caught_after >= window && caught_after < window + deadline,
        "a stream that stops must be caught {DETECTION_MULTIPLIER} deadlines after \
         its last record: caught after {caught_after:?}, want {window:?} within one \
         {deadline:?} deadline"
    );

    let fired = fires.load(std::sync::atomic::Ordering::Relaxed) - fires_under_traffic;
    let misses = u64::from(DETECTION_MULTIPLIER);
    assert!(
        fired == misses || fired == misses + 1,
        "a stream that stops must fire its timer once per window of silence \
         (plus at most one no-miss re-arm): fired {fired} times, want {misses} or \
         {}",
        misses + 1
    );

    drop(transport_tx);
    drop(frame_rx);
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

/// A forward blocked on a saturated `frame_tx` is the pump doing real work,
/// not the sender going silent, so it must not count against the heartbeat
/// budget. `last_frame` has to be stamped once the forward lands, not when
/// the frame arrives -- `messenger::server::lanes` already takes this stance
/// for its own consumer call (see its comment on `last_item`); this pins
/// `reader_pump` to the same rule.
///
/// Scenario: a capacity-1 `frame_tx` is pre-filled so `try_send` always
/// finds it full and the pump must block on `send_async`. One frame is sent;
/// the pump receives it and parks on the full channel. The clock advances
/// past one heartbeat deadline while it is parked -- time spent moving that
/// frame, not silence -- then the slot is drained so the send lands. From
/// that instant a correct pump still needs a full
/// `DETECTION_MULTIPLIER * heartbeat_deadline` of real silence before it
/// gives up; a pump that stamped `last_frame` on arrival instead already
/// believes one whole window of that silence has passed and gives up one
/// window early.
#[tokio::test]
async fn reader_pump_does_not_count_a_blocked_forward_as_heartbeat_silence() {
    tokio::time::pause();

    let deadline = Duration::from_millis(50);
    let (transport_tx, transport_rx) = flume::bounded::<Vec<u8>>(4);
    let (frame_tx, frame_rx) = flume::bounded::<Vec<u8>>(1);
    // Fill the one slot so `try_send` always finds it full.
    frame_tx.try_send(b"placeholder".to_vec()).unwrap();

    let registry = std::sync::Arc::new(dashmap::DashMap::new());
    let local_id = 1u64;
    let cancel_token = tokio_util::sync::CancellationToken::new();
    registry.insert(
        local_id,
        crate::streaming::anchor::AnchorEntry {
            frame_tx: frame_tx.clone(),
            cancel_token: cancel_token.clone(),
            active_pump_token: None,
            attachment: true,
            timeout_cancel: None,
            unattached_timeout: None,
            heartbeat_interval: deadline,
            stream_cancel_handle: None,
            prebind: None,
        },
    );
    let ctx = crate::streaming::anchor::AnchorContext {
        registry: registry.clone(),
        mpsc_registry: std::sync::Arc::new(dashmap::DashMap::new()),
        metrics: None,
    };
    tokio::spawn(reader_pump(
        transport_rx,
        frame_tx,
        cancel_token,
        ctx,
        PumpContext {
            local_id,
            heartbeat_deadline: deadline,
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    ));

    let record = rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(1u32)).unwrap();
    transport_tx.send_async(record.clone()).await.unwrap();

    // Let the forward stay blocked for slightly over one deadline before
    // anything drains it: this is the "the pump was busy, not silent"
    // interval the bug misattributes to the sender.
    tokio::time::sleep(deadline + Duration::from_millis(10)).await;

    // Free the slot the forward was waiting on.
    let placeholder = frame_rx.recv_async().await.unwrap();
    assert_eq!(placeholder, b"placeholder".to_vec());
    let forwarded = frame_rx.recv_async().await.unwrap();
    assert_eq!(forwarded, record, "the blocked record must still land");

    // No further frames arrive. Two more full windows of genuine silence --
    // three in total from the moment the forward actually finished -- must
    // still be needed before the entry is reaped. A half-window margin
    // (rather than a fixed few milliseconds) keeps this proportional to
    // `deadline`: the buggy stamp kills the entry at 2 windows past `S` (the
    // moment the forward landed), the correct one at 3, and this sleep lands
    // squarely between them either way.
    tokio::time::sleep(deadline * 2 + deadline / 2).await;

    assert!(
        registry.contains_key(&local_id),
        "reader_pump killed the stream one window early: a forward blocked by \
         backpressure was counted as heartbeat silence because `last_frame` was \
         stamped when the frame arrived instead of when the forward finished"
    );
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
            prebind: None,
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
        PumpContext {
            local_id,
            heartbeat_deadline: Duration::from_secs(5),
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
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
        PumpContext {
            local_id,
            heartbeat_deadline: Duration::from_secs(5),
            drain: None,
            prebound: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
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

/// Zero-RTT setup added a type, not a field: the attach request and response
/// encode exactly what they encoded before, and a peer that sends neither a
/// ticket nor anything about one still round-trips.
///
/// The negative is the point. `StreamOpenTicket` carries the same five values
/// as `AnchorAttachResponse::Ok`, and the cheap way to build it would have been
/// to hang it off the attach types — which would have put a new field on the
/// wire for every peer, ticket or no ticket. This is what says that did not
/// happen.
#[test]
fn attach_response_golden_encoding_unchanged() {
    // The request as a sender that knows nothing of tickets writes it: the
    // three fields that predate negotiation, plus the key list negotiation
    // added. Nothing else may be required to decode it.
    let ticketless_request = r#"{
        "handle": {"hi": 1, "lo": 2},
        "session_id": 3,
        "stream_cancel_handle": {"hi": 4, "lo": 5},
        "supported_transport_keys": ["messenger-mux-v1"]
    }"#;
    let decoded: AnchorAttachRequest =
        serde_json::from_str(ticketless_request).expect("a ticketless request must deserialize");
    assert_eq!(decoded.session_id, 3);
    assert_eq!(
        decoded
            .supported_transport_keys
            .iter()
            .map(velo_ext::TransportKey::as_str)
            .collect::<Vec<_>>(),
        ["messenger-mux-v1"]
    );

    // The response keeps its five fields and gains none. Compared as a value
    // rather than as bytes because the field *set* is the invariant; rmp-serde
    // writes named fields, so an added one would show up here as an extra key.
    let response = AnchorAttachResponse::Ok {
        streaming_transport_key: velo_ext::TransportKey::new("messenger-mux-v1"),
        heartbeat_interval_ms: 1234,
        routing_session_id: 42,
        initial_credit: 64,
        slot_byte_budget: 4096,
    };
    let json: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&response).expect("serialize"))
            .expect("reparse");
    let fields = json
        .get("Ok")
        .and_then(serde_json::Value::as_object)
        .expect("externally tagged Ok");
    let mut names: Vec<&str> = fields.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "heartbeat_interval_ms",
            "initial_credit",
            "routing_session_id",
            "slot_byte_budget",
            "streaming_transport_key",
        ],
        "the attach response gained or lost a field; zero-RTT must add neither"
    );

    // And the ticket is its own type, decodable on its own terms. All four
    // minted fields are required here, unlike the response above: a ticket
    // has no legacy sender to default for (see `StreamOpenTicket`'s own doc),
    // so `heartbeat_interval_ms` is set explicitly rather than left absent.
    let ticket: StreamOpenTicket = serde_json::from_str(
        r#"{"streaming_transport_key":"messenger-mux-v1","heartbeat_interval_ms":1500,"routing_session_id":7,"initial_credit":8,"slot_byte_budget":0}"#,
    )
    .expect("a fully-populated ticket must deserialize");
    assert_eq!(ticket.routing_session_id, 7);
    assert_eq!(ticket.heartbeat_interval_ms, 1500);
}

/// A ticket missing a field `from_limits` always sets is a corrupt envelope,
/// not an old sender, and must fail to decode rather than silently mint a
/// wrong one.
///
/// `AnchorAttachResponse::Ok`'s `#[serde(default)]` on these same four
/// fields exists for a sender old enough to predate them; a `StreamOpenTicket`
/// has no such sender; it is "only ever read by a peer new enough to have
/// been sent one" ([`StreamOpenTicket`]'s own doc). Inheriting the response's
/// defaults anyway turned a truncated or corrupted ticket into a stream that
/// silently opens against session id 0, a credit window that silently reads
/// "not offering the mux", or a heartbeat cadence that silently reads 5 s and
/// can cross a short-heartbeat anchor's watchdog, tearing down a live stream
/// -- all wrong answers reached without error.
#[test]
fn a_ticket_missing_a_minted_field_fails_rather_than_silently_defaulting() {
    let missing_routing_session_id = r#"{"streaming_transport_key":"messenger-mux-v1","initial_credit":8,"slot_byte_budget":4096}"#;
    assert!(
        serde_json::from_str::<StreamOpenTicket>(missing_routing_session_id).is_err(),
        "a ticket missing routing_session_id must not silently decode as session 0"
    );

    let missing_initial_credit = r#"{"streaming_transport_key":"messenger-mux-v1","routing_session_id":7,"slot_byte_budget":4096}"#;
    assert!(
        serde_json::from_str::<StreamOpenTicket>(missing_initial_credit).is_err(),
        "a ticket missing initial_credit must not silently decode as 'not offering the mux'"
    );

    let missing_slot_byte_budget = r#"{"streaming_transport_key":"messenger-mux-v1","routing_session_id":7,"initial_credit":8}"#;
    assert!(
        serde_json::from_str::<StreamOpenTicket>(missing_slot_byte_budget).is_err(),
        "a ticket missing slot_byte_budget must not silently decode as 'use the default'"
    );

    let missing_heartbeat = r#"{"streaming_transport_key":"messenger-mux-v1","routing_session_id":7,"initial_credit":8,"slot_byte_budget":4096}"#;
    assert!(
        serde_json::from_str::<StreamOpenTicket>(missing_heartbeat).is_err(),
        "a ticket missing heartbeat_interval_ms must not silently decode at 5000ms, which can \
         cross a short-heartbeat anchor's watchdog and tear down a live stream"
    );
}
