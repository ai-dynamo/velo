// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! UDS host-affinity: peers whose advertised UDS socket is not visible on our
//! filesystem must not have UDS selected as their primary transport. The
//! backend's priority sort promotes TCP (or whatever is next) instead.

#![cfg(unix)]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use velo::transports::{
    DataStreams, Transport, VeloBackend, tcp::TcpTransportBuilder, uds::UdsTransportBuilder,
};
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress};

fn tcp_bind() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn build_backend(uds_path: std::path::PathBuf) -> (VeloBackend, DataStreams) {
    let uds = Arc::new(
        UdsTransportBuilder::new()
            .socket_path(&uds_path)
            .build()
            .unwrap(),
    );
    let tcp = Arc::new(
        TcpTransportBuilder::new()
            .bind_addr(tcp_bind())
            .build()
            .unwrap(),
    );
    let transports: Vec<Arc<dyn Transport>> = vec![uds, tcp];
    VeloBackend::new(transports, None).await.unwrap()
}

/// Build a PeerInfo whose address advertises UDS at `uds_path` (may or may not
/// exist) and the TCP entry copied from `real_backend_addr`.
fn peer_with_addresses(uds_path: &std::path::Path, real_backend_addr: &WorkerAddress) -> PeerInfo {
    let tcp_entry = real_backend_addr
        .get_entry(TransportKey::from("tcp"))
        .unwrap()
        .expect("backend must advertise tcp");

    let mut map: HashMap<String, Vec<u8>> = HashMap::new();
    map.insert(
        "uds".to_string(),
        format!("uds://{}", uds_path.display()).into_bytes(),
    );
    map.insert("tcp".to_string(), tcp_entry.to_vec());

    let encoded = rmp_serde::to_vec(&map).unwrap();
    let address = WorkerAddress::from_encoded(Bytes::from(encoded));

    PeerInfo::new(InstanceId::new_v4(), address)
}

#[tokio::test]
async fn uds_rejected_when_path_missing_promotes_tcp() {
    let dir = std::env::temp_dir().join(format!("uds-aff-{}", InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let our_uds = dir.join("self.sock");
    let (backend, _streams) = build_backend(our_uds).await;

    backend
        .set_transport_priority(vec![TransportKey::from("uds"), TransportKey::from("tcp")])
        .unwrap();

    let bogus_uds = dir.join(format!("missing-{}.sock", InstanceId::new_v4()));
    assert!(!bogus_uds.exists());

    let peer = peer_with_addresses(&bogus_uds, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend.register_peer(peer).expect("register must succeed");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP should be promoted to primary when UDS path is not visible"
    );
    let alts = backend.alternative_transport_keys(peer_id).unwrap();
    assert!(
        !alts.contains(&TransportKey::from("uds")),
        "UDS should not be an alternative either: {:?}",
        alts
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn uds_selected_when_socket_visible() {
    let dir = std::env::temp_dir().join(format!("uds-aff-ok-{}", InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let our_uds = dir.join("self.sock");
    let (backend, _streams) = build_backend(our_uds).await;

    backend
        .set_transport_priority(vec![TransportKey::from("uds"), TransportKey::from("tcp")])
        .unwrap();

    // Bind a UDS listener so the peer's socket exists on our fs.
    let peer_uds = dir.join("peer.sock");
    let _listener = tokio::net::UnixListener::bind(&peer_uds).unwrap();

    let peer = peer_with_addresses(&peer_uds, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend.register_peer(peer).expect("register must succeed");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("uds")),
        "UDS should remain primary for a peer whose socket is locally visible"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn uds_rejected_when_path_is_not_a_socket() {
    let dir = std::env::temp_dir().join(format!("uds-aff-ns-{}", InstanceId::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let our_uds = dir.join("self.sock");
    let (backend, _streams) = build_backend(our_uds).await;

    backend
        .set_transport_priority(vec![TransportKey::from("uds"), TransportKey::from("tcp")])
        .unwrap();

    let regular_file = dir.join("not-a-socket");
    std::fs::write(&regular_file, b"regular file").unwrap();

    let peer = peer_with_addresses(&regular_file, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend.register_peer(peer).expect("register must succeed");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP should be primary when peer UDS path is a regular file"
    );

    std::fs::remove_dir_all(&dir).ok();
}
