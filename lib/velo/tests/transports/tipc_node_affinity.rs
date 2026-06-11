// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(all(feature = "tipc", target_os = "linux"))]

//! TIPC node-affinity and reachability-gate tests.
//!
//! Tests the `Gate::{Reachable, Never, NotYet}` verdicts from proposal §5.3
//! as seen through the [`VeloBackend`] primary-transport selection, mirroring
//! the UDS host-affinity test pattern.
//!
//! ## Environment note
//!
//! Many of these tests run in a **no-bearer** TIPC setup (node = 0).  In that
//! environment:
//! - Same-nonce peers always get `Gate::Reachable` regardless of publication.
//! - Different-nonce peers with either side at node = 0 always get `Gate::Never`
//!   (zero-config TIPC cannot cross netns boundaries).
//! - `Gate::NotYet` (cold-start park) is only reachable for cross-node endpoints
//!   when both sides have nonzero TIPC node identities (bearer-enabled cluster).
//!
//! The cold-start recovery test works around this limitation by manually inserting
//! into `topology.pending` (the public test seam, proposal §9) to simulate the
//! parked state, then verifying the hook fires and re-registration succeeds.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use velo::transports::{
    DataStreams, Transport, VeloBackend,
    tcp::TcpTransportBuilder,
    tipc::{TipcEndpoint, TipcTransport, TipcTransportBuilder, TopologyState},
};
use velo_ext::{InstanceId, PeerInfo, TransportKey, WorkerAddress};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn tcp_bind() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

/// Build a VeloBackend with [TIPC, TCP] in priority order.
///
/// Returns `None` when the TIPC kernel module is not loaded (CI without TIPC).
async fn build_backend() -> Option<(Arc<TipcTransport>, VeloBackend, DataStreams)> {
    let tipc = match TipcTransportBuilder::new().build() {
        Ok(t) => Arc::new(t),
        Err(_) => return None, // TIPC module not loaded — skip
    };
    let tcp = Arc::new(
        TcpTransportBuilder::new()
            .bind_addr(tcp_bind())
            .build()
            .unwrap(),
    );
    let transports: Vec<Arc<dyn Transport>> = vec![tipc.clone() as Arc<dyn Transport>, tcp];
    let (backend, streams) = VeloBackend::new(transports, None).await.unwrap();
    Some((tipc, backend, streams))
}

/// Decode the `TipcEndpoint` from the transport's `WorkerAddress`.
fn decode_tipc_ep(transport: &TipcTransport) -> TipcEndpoint {
    let key = TransportKey::from("tipc");
    let bytes = transport.address().get_entry(&key).unwrap().unwrap();
    rmp_serde::from_slice(&bytes).unwrap()
}

/// Build a `PeerInfo` whose address advertises the given TIPC endpoint plus a
/// TCP fallback entry copied from `backend_addr`.
///
/// Using the backend's own TCP address as the "peer's TCP address" keeps tests
/// focused on TIPC gate logic without requiring a real remote TCP listener.
fn peer_with_tipc_ep(ep: &TipcEndpoint, backend_addr: &WorkerAddress) -> PeerInfo {
    let tcp_entry = backend_addr
        .get_entry(TransportKey::from("tcp"))
        .unwrap()
        .expect("backend must advertise tcp");

    let tipc_bytes = rmp_serde::to_vec_named(ep).unwrap();

    let mut map: HashMap<String, Vec<u8>> = HashMap::new();
    map.insert("tipc".to_string(), tipc_bytes);
    map.insert("tcp".to_string(), tcp_entry.to_vec());

    let encoded = rmp_serde::to_vec(&map).unwrap();
    let address = WorkerAddress::from_encoded(Bytes::from(encoded));
    PeerInfo::new(InstanceId::new_v4(), address)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Happy path: a peer on the same TIPC stack (equal `netns_nonce`) is always
/// reachable regardless of publication state — `Gate::Reachable` (same-nonce arm).
///
/// Mirrors `uds_selected_when_socket_visible`.
#[tokio::test]
async fn tipc_selected_when_same_stack() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    let local_ep = decode_tipc_ep(&tipc);

    // Build a real peer TIPC transport (same process → same nonce, same netid).
    let peer_tipc = TipcTransportBuilder::new()
        .build()
        .expect("peer TIPC transport build must succeed when module is loaded");
    let peer_ep = decode_tipc_ep(&peer_tipc);

    // Invariant: same nonce confirmed.
    assert_eq!(
        peer_ep.netns_nonce, local_ep.netns_nonce,
        "test requires a same-nonce peer (same process, same netns)"
    );

    let peer = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend.register_peer(peer).expect("register must succeed");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tipc")),
        "TIPC must be primary for a same-nonce (same-stack) peer (Gate::Reachable)"
    );
}

/// A peer whose `netid` differs from the local transport's `netid` hits
/// `Gate::Never` (wrong cluster) and must be demoted to TCP.
///
/// Mirrors `uds_rejected_when_path_missing_promotes_tcp`.
#[tokio::test]
async fn tipc_rejected_netid_mismatch_promotes_tcp() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    let local_ep = decode_tipc_ep(&tipc);

    // Fabricate a peer endpoint from a different TIPC cluster (different netid).
    let peer_ep = TipcEndpoint {
        version: 1,
        service_type: local_ep.service_type,
        service_instance: local_ep.service_instance.wrapping_add(1),
        node: 0,
        socket_ref: 0xDEAD_0001,
        netid: local_ep.netid.wrapping_add(1), // wrong cluster → Gate::Never
        node_id: [0u8; 16],
        netns_nonce: local_ep.netns_nonce.wrapping_add(1),
        scope: 2,
    };

    let peer = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend
        .register_peer(peer)
        .expect("register must succeed (TCP fallback)");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP must be promoted when peer's netid differs (Gate::Never)"
    );

    let alts = backend.alternative_transport_keys(peer_id).unwrap();
    assert!(
        !alts.contains(&TransportKey::from("tipc")),
        "TIPC must not appear as an alternative when netid differs: {alts:?}"
    );
}

/// A peer with a different `netns_nonce` and `node = 0` (zero-config TIPC, no
/// bearer) hits `Gate::Never` via the zero-node arm.
///
/// This captures the "same host, different netns, no bearer" case: zero-config
/// TIPC cannot cross a netns boundary because both sides advertise `node = 0`,
/// making exact-ref routing ambiguous across netns.
#[tokio::test]
async fn tipc_rejected_mismatched_nonce_zero_node_promotes_tcp() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    let local_ep = decode_tipc_ep(&tipc);

    // Fabricate a peer from a different netns on the same host.
    // Different nonce + node = 0 (same as local.node in no-bearer) → Gate::Never.
    let peer_ep = TipcEndpoint {
        version: 1,
        service_type: local_ep.service_type,
        service_instance: local_ep.service_instance.wrapping_add(2),
        node: 0, // same as local.node in no-bearer setup
        socket_ref: 0xDEAD_0002,
        netid: local_ep.netid,
        node_id: [0u8; 16],
        netns_nonce: local_ep.netns_nonce.wrapping_add(1), // different stack
        scope: 2,
    };

    let peer = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend
        .register_peer(peer)
        .expect("register must succeed (TCP fallback)");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP must be promoted when nonce differs and node=0 \
         (zero-config TIPC cannot cross netns boundaries)"
    );
}

/// A peer claiming `ep.node == local.node` with a different `netns_nonce` hits
/// `Gate::Never`: connecting to `{ref, node=local.node}` would route into our
/// own TIPC stack, which is a misconfiguration.
///
/// NOTE: In a no-bearer setup (`local.node = ep.node = 0`), this case fires
/// `Gate::Never` via the zero-node arm rather than the own-node-duplicate arm.
/// The observable outcome (no TIPC primary, TCP promoted) is identical.
#[tokio::test]
async fn tipc_rejected_unequal_nonce_own_node_promotes_tcp() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    let local_ep = decode_tipc_ep(&tipc);

    // ep.node == local.node with different nonce → Gate::Never.
    // In no-bearer (local.node = 0): hits the zero-node arm.
    // In bearer (local.node != 0): hits the own-node-duplicate arm.
    let peer_ep = TipcEndpoint {
        version: 1,
        service_type: local_ep.service_type,
        service_instance: local_ep.service_instance.wrapping_add(3),
        node: local_ep.node, // == local.node
        socket_ref: 0xDEAD_0003,
        netid: local_ep.netid,
        node_id: [0u8; 16],
        netns_nonce: local_ep.netns_nonce.wrapping_add(1), // different nonce
        scope: 2,
    };

    let peer = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend
        .register_peer(peer)
        .expect("register must succeed (TCP fallback)");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP must be promoted when ep.node == local.node with different nonce (Gate::Never)"
    );
}

/// A stale or unreachable cross-stack endpoint (different nonce, no live
/// publication in the local name table) must not become the primary transport.
///
/// In a no-bearer setup (`local.node = 0`), even a fabricated nonzero `ep.node`
/// triggers `Gate::Never` (via the zero-node arm on the LOCAL side).  In a
/// bearer-enabled cluster this endpoint would get `Gate::NotYet` (parked), with
/// the same TCP-primary outcome.  Both cases are tested here: TCP must be primary.
#[tokio::test]
async fn tipc_stale_endpoint_no_publication_promotes_tcp() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    let local_ep = decode_tipc_ep(&tipc);

    // Fabricate a cross-stack endpoint whose listener has since closed.
    // node = 0x0102_0304 simulates a bearer-enabled remote with nonzero node.
    // No publication for this {service_instance, socket_ref, node} exists.
    // In no-bearer (local.node=0): Gate::Never (zero-node arm on local side).
    // In bearer (local.node!=0 & node!=0): Gate::NotYet (publication absent).
    let peer_ep = TipcEndpoint {
        version: 1,
        service_type: local_ep.service_type,
        service_instance: local_ep.service_instance.wrapping_add(4),
        node: 0x0102_0304, // fabricated non-zero node (as if bearer-enabled)
        socket_ref: 0xABCD_5678,
        netid: local_ep.netid,
        node_id: [0u8; 16],
        netns_nonce: local_ep.netns_nonce.wrapping_add(1), // different stack
        scope: 2,
    };

    let peer = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer.instance_id();
    backend
        .register_peer(peer)
        .expect("register must succeed (TCP fallback)");

    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tcp")),
        "TCP must be promoted for a stale/unreachable endpoint \
         (Gate::Never in no-bearer; Gate::NotYet in bearer — both yield TCP primary)"
    );
}

/// Cold-start recovery: verify that a peer inserted into `topology.pending`
/// (simulating `Gate::NotYet` park) is re-driven by the re-register hook when
/// a TIPC_PUBLISHED event fires, and that TIPC becomes the primary transport.
///
/// This test exercises the hook mechanism end-to-end using the public test seam
/// (`TipcTransport::set_reregister_hook` + `TipcTransport::topology_state`,
/// proposal §9) in lieu of a bearer-enabled cluster where `Gate::NotYet` is
/// natively reachable.
///
/// Sequence:
/// 1. Build VeloBackend([TIPC, TCP]).
/// 2. Install a re-register hook that calls `backend.register_peer`.
/// 3. Insert a same-nonce peer into `topology.pending` (bypassing `register()`
///    so the same-nonce → `Gate::Reachable` shortcut is not taken).
/// 4. Build a trigger transport with the same `service_type` — this binds a
///    TIPC socket, which causes the topology server to emit `TIPC_PUBLISHED` to
///    all subscribers including the backend's topology watcher.
/// 5. Yield to the topology task (sleep 200 ms); the watcher calls
///    `redrive_pending` → hook fires → `register_peer` succeeds → TIPC primary.
#[tokio::test]
async fn tipc_cold_start_recovery_hook_fires() {
    let (tipc, backend, _streams) = match build_backend().await {
        Some(r) => r,
        None => return,
    };

    let backend = Arc::new(backend);

    backend
        .set_transport_priority(vec![TransportKey::from("tipc"), TransportKey::from("tcp")])
        .unwrap();

    // Build a peer transport (same process → same nonce, same netid).
    let peer_tipc = match TipcTransportBuilder::new().build() {
        Ok(t) => t,
        Err(_) => return,
    };
    let peer_ep = decode_tipc_ep(&peer_tipc);
    let peer_info = peer_with_tipc_ep(&peer_ep, backend.peer_info().worker_address());
    let peer_id = peer_info.instance_id();

    // Pre-condition: peer is not yet registered in the backend.
    assert!(
        backend.primary_transport_key(peer_id).is_none(),
        "peer must not be registered before cold-start recovery"
    );

    // Install the re-register hook (simulating what VeloBuilder does automatically).
    // The hook calls backend.register_peer, which re-evaluates the gate.
    {
        let b = Arc::clone(&backend);
        tipc.set_reregister_hook(Arc::new(move |pi| {
            let _ = b.register_peer(pi);
        }));
    }

    // Simulate Gate::NotYet park: insert directly into topology.pending without
    // going through Transport::register(), which would short-circuit to Reachable
    // (same nonce) and register immediately.
    let topo: Arc<TopologyState> = tipc.topology_state();
    topo.pending.insert(peer_id, peer_info);

    // Trigger a TIPC_PUBLISHED event by building another TIPC transport with the
    // same service_type.  build() pre-binds the socket; the TIPC name table update
    // triggers TIPC_PUBLISHED on all active topology-server subscriptions, including
    // the backend's watcher.
    //
    // We do NOT call start() on the trigger transport to avoid a blocking await
    // that would race with the topology task; the socket bind is sufficient to
    // produce the TIPC_PUBLISHED event.
    let local_ep = decode_tipc_ep(&tipc);
    let _trigger = TipcTransportBuilder::new()
        .service_type(local_ep.service_type)
        .build()
        .expect("trigger TIPC transport build must succeed");

    // Yield for 200 ms — the topology task processes TIPC_PUBLISHED, calls
    // redrive_pending, fires the hook, and calls register_peer.
    // TIPC topology events are delivered in < 0.3 ms; 200 ms is very conservative.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After cold-start recovery:
    // - register_peer(peer_info) was called via the hook
    // - TIPC.register → same nonce → Gate::Reachable → TIPC sorted first
    // - primary = TIPC
    assert_eq!(
        backend.primary_transport_key(peer_id),
        Some(TransportKey::from("tipc")),
        "TIPC must become primary after cold-start recovery via re-register hook"
    );

    // Verify peer was removed from pending (successful registration cleans it up).
    assert!(
        topo.pending.get(&peer_id).is_none(),
        "topology.pending must be empty after successful re-registration"
    );
}
