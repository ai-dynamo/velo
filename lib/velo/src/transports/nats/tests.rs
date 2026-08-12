// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the NATS transport's frame encoding, routing, and drain gate.

use super::*;
use crate::transports::transport::make_channels;

#[test]
fn test_begin_drain_flips_shutdown_state() {
    // Test the begin_drain logic directly via ShutdownState.
    // Since constructing a full NatsTransport requires a NATS client,
    // we verify the ShutdownState component independently.
    let state = ShutdownState::new();
    assert!(!state.is_draining());
    state.begin_drain();
    assert!(state.is_draining());
}

#[test]
fn test_route_frame_response_routes_during_drain() {
    // Verify that route_frame routes Response frames even when draining.
    // The drain gate is in run_receive_loop, not route_frame — so route_frame
    // should always route regardless. This confirms D-04.
    let (adapter, streams) = make_channels();
    adapter.shutdown_state.begin_drain();
    assert!(adapter.shutdown_state.is_draining());

    // Build a mock NATS message with Velo-Type=1 (Response), Velo-HLen=3
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(HEADER_VELO_TYPE, "1"); // Response
    headers.insert(HEADER_VELO_HLEN, "3");
    let msg = async_nats::Message {
        subject: "test".into(),
        reply: None,
        payload: Bytes::from_static(b"hdrpay"),
        headers: Some(headers),
        status: None,
        description: None,
        length: 0,
    };

    route_frame(&msg, &adapter, "nats", None);

    // Response should be routed to response_stream
    let result = streams.response_stream.try_recv();
    assert!(
        result.is_ok(),
        "Response frame must be routed even during drain"
    );
    let (header, payload) = result.unwrap();
    assert_eq!(&header[..], b"hdr");
    assert_eq!(&payload[..], b"pay");
}

#[test]
fn test_route_frame_event_routes_during_drain() {
    let (adapter, streams) = make_channels();
    adapter.shutdown_state.begin_drain();

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(HEADER_VELO_TYPE, "3"); // Event
    headers.insert(HEADER_VELO_HLEN, "2");
    let msg = async_nats::Message {
        subject: "test".into(),
        reply: None,
        payload: Bytes::from_static(b"evbody"),
        headers: Some(headers),
        status: None,
        description: None,
        length: 0,
    };

    route_frame(&msg, &adapter, "nats", None);

    let result = streams.event_stream.try_recv();
    assert!(
        result.is_ok(),
        "Event frame must be routed even during drain"
    );
}

#[test]
fn test_route_frame_message_routes_when_not_draining() {
    // route_frame always routes — the drain gate is in the loop, not route_frame.
    // This verifies Message frames do reach message_stream when NOT draining.
    let (adapter, streams) = make_channels();
    assert!(!adapter.shutdown_state.is_draining());

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(HEADER_VELO_TYPE, "0"); // Message
    headers.insert(HEADER_VELO_HLEN, "4");
    let msg = async_nats::Message {
        subject: "test".into(),
        reply: None,
        payload: Bytes::from_static(b"hdrrpayload"),
        headers: Some(headers),
        status: None,
        description: None,
        length: 0,
    };

    route_frame(&msg, &adapter, "nats", None);

    let result = streams.message_stream.try_recv();
    assert!(
        result.is_ok(),
        "Message frame must be routed when not draining"
    );
    let (header, payload) = result.unwrap();
    assert_eq!(&header[..], b"hdrr");
    assert_eq!(&payload[..], b"payload");
}

#[test]
fn test_shutting_down_response_type_value() {
    // Verify ShuttingDown is type 4 (used in drain gate Velo-Type header)
    assert_eq!(MessageType::ShuttingDown as u8, 4);
}

#[test]
fn test_build_nats_frame_header_and_payload() {
    let header = Bytes::from_static(b"hdr");
    let payload = Bytes::from_static(b"payload");
    let (nats_headers, nats_payload) = build_nats_frame(MessageType::Message, &header, &payload);

    assert_eq!(nats_headers.get(HEADER_VELO_TYPE).unwrap().as_str(), "0");
    assert_eq!(nats_headers.get(HEADER_VELO_HLEN).unwrap().as_str(), "3");
    assert_eq!(&nats_payload[..], b"hdrpayload");
}

#[test]
fn test_build_nats_frame_empty_header() {
    let header = Bytes::new();
    let payload = Bytes::from_static(b"payload");
    let (nats_headers, nats_payload) = build_nats_frame(MessageType::Response, &header, &payload);

    assert_eq!(nats_headers.get(HEADER_VELO_TYPE).unwrap().as_str(), "1");
    assert_eq!(nats_headers.get(HEADER_VELO_HLEN).unwrap().as_str(), "0");
    assert_eq!(&nats_payload[..], b"payload");
}

#[test]
fn test_build_nats_frame_empty_payload() {
    let header = Bytes::from_static(b"hdr");
    let payload = Bytes::new();
    let (nats_headers, nats_payload) = build_nats_frame(MessageType::Event, &header, &payload);

    assert_eq!(nats_headers.get(HEADER_VELO_TYPE).unwrap().as_str(), "3");
    assert_eq!(nats_headers.get(HEADER_VELO_HLEN).unwrap().as_str(), "3");
    assert_eq!(&nats_payload[..], b"hdr");
}

#[test]
fn test_build_nats_frame_both_empty() {
    let header = Bytes::new();
    let payload = Bytes::new();
    let (nats_headers, nats_payload) = build_nats_frame(MessageType::Ack, &header, &payload);

    assert_eq!(nats_headers.get(HEADER_VELO_TYPE).unwrap().as_str(), "2");
    assert_eq!(nats_headers.get(HEADER_VELO_HLEN).unwrap().as_str(), "0");
    assert!(nats_payload.is_empty());
}

#[test]
fn test_build_nats_frame_all_message_types() {
    for (msg_type, expected) in [
        (MessageType::Message, "0"),
        (MessageType::Response, "1"),
        (MessageType::Ack, "2"),
        (MessageType::Event, "3"),
        (MessageType::ShuttingDown, "4"),
    ] {
        let (headers, _) = build_nats_frame(msg_type, &Bytes::new(), &Bytes::new());
        assert_eq!(
            headers.get(HEADER_VELO_TYPE).unwrap().as_str(),
            expected,
            "Velo-Type mismatch for {:?}",
            msg_type
        );
    }
}
