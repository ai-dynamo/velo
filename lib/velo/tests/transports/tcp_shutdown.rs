// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TCP graceful shutdown

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, TcpShutdownClient, shutdown_scenarios};

transport_shutdown_tests!(tcp, TcpShutdownClient);

#[tokio::test]
async fn test_tcp_admitted_frames_are_written_across_teardown() {
    tokio::time::timeout(
        OUTER_TEST_TIMEOUT,
        shutdown_scenarios::admitted_frames_are_written_across_teardown::<TcpShutdownClient>(),
    )
    .await
    .expect("admitted_frames_are_written_across_teardown timed out");
}

// --- Codec-level test (not transport-specific, but lives here for TCP coverage) ---
#[test]
fn test_shutting_down_frame_roundtrip() {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;
    use velo::transports::MessageType;
    use velo::transports::tcp::TcpFrameCodec;

    let header = b"correlation-header";
    let payload = b"";

    let mut buf = Vec::new();
    TcpFrameCodec::encode_frame_sync(&mut buf, MessageType::ShuttingDown, header, payload).unwrap();

    let mut codec = TcpFrameCodec::new();
    let mut bytes = BytesMut::from(&buf[..]);
    let (msg_type, decoded_header, decoded_payload) = codec.decode(&mut bytes).unwrap().unwrap();

    assert_eq!(msg_type, MessageType::ShuttingDown);
    assert_eq!(&decoded_header[..], header);
    assert_eq!(decoded_payload.len(), 0);
}

#[tokio::test]
async fn test_tcp_a_frame_admitted_while_dialing_survives_teardown() {
    tokio::time::timeout(
        OUTER_TEST_TIMEOUT,
        shutdown_scenarios::a_frame_admitted_while_dialing_survives_teardown::<TcpShutdownClient>(),
    )
    .await
    .expect("a_frame_admitted_while_dialing_survives_teardown timed out");
}

#[tokio::test]
async fn test_tcp_teardown_admits_nothing_from_the_gate() {
    tokio::time::timeout(
        OUTER_TEST_TIMEOUT,
        shutdown_scenarios::teardown_admits_nothing_from_the_gate::<TcpShutdownClient>(),
    )
    .await
    .expect("teardown_admits_nothing_from_the_gate timed out");
}

/// `shutdown()` refuses what waits in each admission gate without holding the
/// connection map, so an error handler may call back into the transport.
///
/// The refusals run the senders' hooks on the thread that calls `shutdown()`.
/// It used to refuse them while it iterated the map, under a shard's read
/// lock, so a handler that unregistered the peer on a failed send waited on
/// the lock its own thread held, and `shutdown()` never returned.
#[test]
fn shutdown_lets_an_error_handler_call_back_into_the_transport() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
    use velo::transports::{MessageType, Transport, TransportErrorHandler, make_channels};
    use velo_ext::{InstanceId, PeerInfo};

    struct UnregisterOnError {
        transport: Mutex<Option<Arc<TcpTransport>>>,
        peer: InstanceId,
        fired: AtomicUsize,
    }
    impl TransportErrorHandler for UnregisterOnError {
        fn on_error(&self, _header: Bytes, _payload: Bytes, _error: String) {
            self.fired.fetch_add(1, Ordering::SeqCst);
            let transport = self.transport.lock().unwrap().clone();
            if let Some(transport) = transport {
                transport.unregister(self.peer);
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let a = Arc::new(
        TcpTransportBuilder::new()
            .from_listener(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .unwrap()
            .channel_capacity(16)
            .build()
            .unwrap(),
    );
    // The peer's socket is bound but nobody accepts or reads: the writer
    // blocks once the kernel buffers fill, and the rest waits in the gate.
    let b = TcpTransportBuilder::new()
        .from_listener(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .unwrap()
        .build()
        .unwrap();
    let b_id = InstanceId::new_v4();
    let handler = Arc::new(UnregisterOnError {
        transport: Mutex::new(Some(Arc::clone(&a))),
        peer: b_id,
        fired: AtomicUsize::new(0),
    });
    let pending = runtime.block_on(async {
        let (adapter, _streams) = make_channels();
        tokio::time::timeout(
            common::OUTER_TEST_TIMEOUT,
            a.start(
                InstanceId::new_v4(),
                adapter,
                tokio::runtime::Handle::current(),
            ),
        )
        .await
        .expect("start timed out")
        .unwrap();
        a.register(PeerInfo::new(b_id, b.address())).unwrap();
        let mut pending = 0;
        for i in 0..512u32 {
            let header = Bytes::from(i.to_be_bytes().to_vec());
            let payload = Bytes::from(vec![0u8; 64 * 1024]);
            let outcome = a.send_message(
                b_id,
                header.clone(),
                payload.clone(),
                MessageType::Response,
                handler.clone(),
            );
            if let Some(admission) = outcome.into_pending() {
                pending += 1;
                let handler = handler.clone();
                drop(admission.on_resolved(move |result| {
                    if let Err(error) = result {
                        handler.on_error(header, payload, error.to_string());
                    }
                }));
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        pending
    });
    assert!(pending > 0, "frames must wait in the admission gate");

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let shutting_down = Arc::clone(&a);
    std::thread::spawn(move || {
        shutting_down.shutdown();
        let _ = done_tx.send(());
    });
    let returned = done_rx.recv_timeout(Duration::from_secs(10)).is_ok();
    *handler.transport.lock().unwrap() = None;
    // Not dropped: on a deadlock its workers wait on the same lock, and the
    // drop would wait for them. The stuck threads end with the process.
    runtime.shutdown_background();
    assert!(
        returned,
        "shutdown() did not return; the handler ran {} times",
        handler.fired.load(Ordering::SeqCst)
    );
    assert!(
        handler.fired.load(Ordering::SeqCst) > 0,
        "the gate's frames were refused"
    );
}
