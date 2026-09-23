// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use velo_ext::{
    DataStreams, InstanceId, MessageType, PeerInfo, Transport, TransportErrorHandler, make_channels,
};

use super::{QuicEndpointInfo, QuicTransport, QuicTransportBuilder};
use crate::transports::address::WorkerAddressBuilder;

#[derive(Default)]
struct Errors(Mutex<Vec<String>>);

impl TransportErrorHandler for Errors {
    fn on_error(&self, _header: Bytes, _payload: Bytes, error: String) {
        self.0.lock().unwrap().push(error);
    }
}

async fn started() -> (QuicTransport, DataStreams, InstanceId) {
    let transport = QuicTransportBuilder::new()
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, streams) = make_channels();
    let id = InstanceId::new_v4();
    transport
        .start(id, adapter, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (transport, streams, id)
}

/// `target`'s address, but naming `fingerprint` as its certificate.
fn peer_with_fingerprint(
    target: &QuicTransport,
    id: InstanceId,
    fingerprint: super::tls::Fingerprint,
) -> PeerInfo {
    let key = target.key();
    let raw = target.address().get_entry(&key).unwrap().unwrap();
    let mut info = QuicEndpointInfo::decode(&raw).unwrap();
    info.fingerprint = fingerprint;
    let mut builder = WorkerAddressBuilder::new();
    builder.add_entry(key, info.encode().unwrap()).unwrap();
    PeerInfo::new(id, builder.build().unwrap())
}

/// A dialer accepts only the certificate that the peer's address names. A
/// listener with another certificate on that address fails the handshake,
/// and the frame goes to the error handler instead of the listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_another_certificate_is_refused() {
    let (client, _client_streams, _) = started().await;
    let (server, server_streams, server_id) = started().await;
    let (stranger, _stranger_streams, _) = started().await;

    // Control: with the right fingerprint, the frame arrives.
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();
    let errors = Arc::new(Errors::default());
    let _ = client.send_message(
        server_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"trusted"),
        MessageType::Event,
        errors.clone(),
    );
    let (_, payload) = tokio::time::timeout(
        Duration::from_secs(5),
        server_streams.event_stream.recv_async(),
    )
    .await
    .expect("a trusted peer receives the frame")
    .unwrap();
    assert_eq!(&payload[..], b"trusted");

    // The same address, pinned to another certificate.
    let impostor_id = InstanceId::new_v4();
    client
        .register(peer_with_fingerprint(
            &server,
            impostor_id,
            stranger.fingerprint(),
        ))
        .unwrap();
    let _ = client.send_message(
        impostor_id,
        Bytes::from_static(b"h"),
        Bytes::from_static(b"untrusted"),
        MessageType::Event,
        errors.clone(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while errors.0.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the refused frame reaches the error handler");
    assert!(
        server_streams.event_stream.try_recv().is_err(),
        "nothing reaches a listener whose certificate is not the pinned one"
    );

    for t in [&client, &server, &stranger] {
        t.shutdown();
    }
}

/// Many small frames on one stream all arrive, in order. This load shape
/// hit "too many gaps in stream buffer" in quinn-proto 0.11.17 in Dynamo's
/// QUIC response plane; 0.11.18 is the floor in `Cargo.toml`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_of_small_frames_arrives_in_order() {
    const FRAMES: u32 = 50_000;
    let (client, _client_streams, _) = started().await;
    let (server, server_streams, server_id) = started().await;
    client
        .register(peer_with_fingerprint(
            &server,
            server_id,
            server.fingerprint(),
        ))
        .unwrap();

    let errors = Arc::new(Errors::default());
    let sender = tokio::spawn({
        let errors = errors.clone();
        async move {
            for i in 0..FRAMES {
                let outcome = client.send_message(
                    server_id,
                    Bytes::from(i.to_le_bytes().to_vec()),
                    Bytes::from_static(b"token"),
                    MessageType::Event,
                    errors.clone(),
                );
                if let velo_ext::SendOutcome::Pending(admission) = outcome {
                    admission.await.unwrap();
                }
            }
            client
        }
    });

    let received = tokio::time::timeout(Duration::from_secs(60), async {
        for expected in 0..FRAMES {
            let (header, _) = server_streams.event_stream.recv_async().await.unwrap();
            let got = u32::from_le_bytes(header[..4].try_into().unwrap());
            assert_eq!(got, expected, "frames arrive in send order");
        }
    })
    .await;
    received.expect("every frame arrives");
    assert!(errors.0.lock().unwrap().is_empty());

    let client = sender.await.unwrap();
    client.shutdown();
    server.shutdown();
}

async fn started_with(builder: QuicTransportBuilder) -> (QuicTransport, DataStreams, InstanceId) {
    let transport = builder
        .bind_addr("127.0.0.1:0".parse().unwrap())
        .build()
        .unwrap();
    let (adapter, streams) = make_channels();
    let id = InstanceId::new_v4();
    transport
        .start(id, adapter, tokio::runtime::Handle::current())
        .await
        .unwrap();
    (transport, streams, id)
}

async fn large_frames_round_trip(max_mtu: Option<u16>) {
    let configure = |b: QuicTransportBuilder| match max_mtu {
        Some(m) => b.max_mtu(m),
        None => b,
    };
    let (a, a_streams, a_id) = started_with(configure(QuicTransportBuilder::new())).await;
    let (b, b_streams, b_id) = started_with(configure(QuicTransportBuilder::new())).await;
    a.register(peer_with_fingerprint(&b, b_id, b.fingerprint()))
        .unwrap();
    b.register(peer_with_fingerprint(&a, a_id, a.fingerprint()))
        .unwrap();
    let errors = Arc::new(Errors::default());
    let payload = Bytes::from(vec![7u8; 64 * 1024]);
    let started_at = std::time::Instant::now();
    for i in 0..200u32 {
        let _ = a.send_message(
            b_id,
            Bytes::from(i.to_le_bytes().to_vec()),
            payload.clone(),
            MessageType::Event,
            errors.clone(),
        );
        let (h, p) =
            tokio::time::timeout(Duration::from_secs(5), b_streams.event_stream.recv_async())
                .await
                .unwrap_or_else(|_| panic!("request {i} stalled (max_mtu {max_mtu:?})"))
                .unwrap();
        let _ = b.send_message(a_id, h, p, MessageType::Response, errors.clone());
        tokio::time::timeout(
            Duration::from_secs(5),
            a_streams.response_stream.recv_async(),
        )
        .await
        .unwrap_or_else(|_| panic!("response {i} stalled (max_mtu {max_mtu:?})"))
        .unwrap();
    }
    // Each round trip takes well under a millisecond on loopback. A lost
    // send batch costs a loss-probe timeout per flight, tens of ms each, and
    // 200 round trips then take seconds.
    let elapsed = started_at.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "200 round trips of 64 KiB took {elapsed:?} (max_mtu {max_mtu:?}); \
         a send batch above the UDP datagram limit is lost on every flight"
    );
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_frames_round_trip_at_the_default_mtu() {
    large_frames_round_trip(None).await;
}

/// A jumbo-frame MTU (8952) is lowered to `GSO_SAFE_MAX_MTU`. Unclamped, a
/// 10-packet send batch exceeds the UDP datagram limit, the kernel refuses
/// it, and quinn-udp reports success, so every flight is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_frames_round_trip_at_a_jumbo_mtu() {
    large_frames_round_trip(Some(8952)).await;
}

#[test]
fn a_max_mtu_above_the_gso_batch_limit_is_lowered() {
    use super::GSO_SAFE_MAX_MTU;
    assert_eq!(GSO_SAFE_MAX_MTU, 6550);
    assert_eq!(super::clamp_to_gso_batch(8952), 6550);
    assert_eq!(super::clamp_to_gso_batch(1452), 1452);
}
