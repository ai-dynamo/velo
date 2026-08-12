// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the gRPC transport's endpoint parsing, registration and builder.

use super::*;
use crate::transports::address::WorkerAddressBuilder;
use std::sync::atomic::{AtomicUsize, Ordering};
use velo_ext::PeerInfo;

struct TrackingErrorHandler {
    count: AtomicUsize,
}

impl TrackingErrorHandler {
    fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
        }
    }

    fn error_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl TransportErrorHandler for TrackingErrorHandler {
    fn on_error(&self, _: Bytes, _: Bytes, _: String) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

fn make_grpc_peer(addr: SocketAddr) -> PeerInfo {
    let instance_id = crate::InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    builder
        .add_entry("grpc", format!("grpc://{}", addr).into_bytes())
        .unwrap();
    PeerInfo::new(instance_id, builder.build().unwrap())
}

#[test]
fn test_parse_grpc_endpoint() {
    let addr = parse_grpc_endpoint(b"grpc://127.0.0.1:5555").unwrap();
    assert_eq!(addr.port(), 5555);

    let addr = parse_grpc_endpoint(b"127.0.0.1:6666").unwrap();
    assert_eq!(addr.port(), 6666);

    assert!(parse_grpc_endpoint(b"invalid").is_err());
}

#[test]
fn test_builder_default_prebinds() {
    // Builder without explicit bind_addr should pre-bind to 0.0.0.0:0
    let result = GrpcTransportBuilder::new().build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_with_bind_addr() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let result = GrpcTransportBuilder::new().bind_addr(addr).build();
    assert!(result.is_ok());
}

#[test]
fn test_builder_custom_key() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let transport = GrpcTransportBuilder::new()
        .bind_addr(addr)
        .key(TransportKey::from("my-grpc"))
        .build()
        .unwrap();
    assert_eq!(transport.key(), TransportKey::from("my-grpc"));
}

#[test]
fn test_register_peer_legacy_format() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = GrpcTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();

    let peer_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let peer = make_grpc_peer(peer_addr);
    let iid = peer.instance_id();

    transport.register(peer).unwrap();
    assert!(transport.peers.contains_key(&iid));
}

#[test]
fn test_register_peer_no_endpoint() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = GrpcTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();

    // Create a peer with a "tcp" entry, not "grpc"
    let instance_id = crate::InstanceId::new_v4();
    let mut builder = WorkerAddressBuilder::new();
    builder
        .add_entry("tcp", b"tcp://127.0.0.1:1234".to_vec())
        .unwrap();
    let peer = PeerInfo::new(instance_id, builder.build().unwrap());

    let result = transport.register(peer);
    assert!(matches!(result, Err(TransportError::NoEndpoint)));
}

#[test]
fn test_send_message_not_started() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let transport = GrpcTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();

    let error_handler = Arc::new(TrackingErrorHandler::new());
    transport
        .send_message(
            crate::InstanceId::new_v4(),
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload"),
            MessageType::Message,
            error_handler.clone(),
        )
        .expect("send returns Ok and reports via on_error");

    assert_eq!(error_handler.error_count(), 1);
}

#[test]
fn test_builder_multi_endpoint_format() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let transport = GrpcTransportBuilder::new()
        .from_listener(listener)
        .unwrap()
        .build()
        .unwrap();

    // The address should contain msgpack-encoded endpoints
    let wa = transport.address();
    let raw = wa.get_entry("grpc").unwrap().unwrap();
    let endpoints: Vec<InterfaceEndpoint> = rmp_serde::from_slice(&raw).unwrap();
    assert!(!endpoints.is_empty());
    for ep in &endpoints {
        assert_eq!(ep.port, addr.port());
    }
}
