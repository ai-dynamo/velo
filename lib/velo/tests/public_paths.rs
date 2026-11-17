// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The public paths that velo's known consumers name.
//!
//! Each item in velo has one public path. When an item is hidden, or its path
//! moves, `cargo semver-checks` reports a breaking change, and a minor bump
//! covers it. Nothing reports a path that a consumer needs and that a later
//! change hid by mistake. This file names each path a consumer uses, so such
//! a change fails here first. The consumers: dynamo (`velo::streaming` and the
//! root stream types), dynamo-gdl (`velo::StreamOpenTicket` and its fields,
//! `Velo::staging_threshold`), rhino and kvbm (`velo::discovery`), and
//! deepseek-ultrafast (the RDMA staging surface). The 0.23.0 section of the
//! migration guide lists the paths each consumer moves to.
//!
//! The test only has to compile; its body checks a few values so it is not
//! empty.

#![allow(unused_imports)]

// dynamo: the stream types its response plane names.
use velo::streaming::{
    AnchorConfig, AutoFlush, ByteItemBuilder, FlushPolicy, MESSENGER_MUX_KEY, MuxConfig,
    ReservationConfig, TCP_STREAM_KEY,
};
use velo::{
    AttachError, SendError, StreamAnchor, StreamAnchorHandle, StreamController, StreamError,
    StreamFrame, StreamOpenTicket, StreamSender,
};

// rhino and kvbm: discovery.
use velo::discovery::{PeerDiscovery, ServiceDiscovery, ServiceEvent};

// deepseek-ultrafast: rendezvous and the RDMA staging surface.
#[cfg(all(target_os = "linux", feature = "ucx"))]
use velo::PinnedWriter;
#[cfg(all(target_os = "linux", feature = "ucx"))]
use velo::rendezvous::rdma::{RdmaConfig, RdmaPoolConfig, RdmaRendezvousConfig, RegionGuard};
use velo::{DataHandle, RdmaDestination, RendezvousWrite};

// Metrics, at its one path.
use velo::observability::VeloMetrics;

/// `StreamOpenTicket` keeps its public fields: a consumer carries the ticket
/// in its own envelope and reads them.
fn ticket_fields(ticket: &StreamOpenTicket) -> (u64, u64, u32, u32, u16) {
    let _key = &ticket.streaming_transport_key;
    (
        ticket.heartbeat_interval_ms,
        ticket.routing_session_id,
        ticket.initial_credit,
        ticket.slot_byte_budget,
        ticket.lane,
    )
}

/// The `Velo` methods that replaced reaching through the removed managers.
fn velo_methods(velo: &velo::Velo, handle: DataHandle) -> (bool, usize, usize) {
    (
        velo.unregister_data(handle),
        velo.staging_threshold(),
        velo.active_anchor_count(),
    )
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
fn region_staging(
    velo: &velo::Velo,
    guard: &RegionGuard,
) -> Result<DataHandle, velo::rendezvous::rdma::RdmaError> {
    velo.register_data_in_region(guard, 0..1)
}

#[test]
fn consumer_paths_resolve() {
    let _ = ticket_fields as fn(&StreamOpenTicket) -> _;
    let _ = velo_methods as fn(&velo::Velo, DataHandle) -> _;
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    let _ = region_staging as fn(&velo::Velo, &RegionGuard) -> _;

    let handle = DataHandle::pack(velo::WorkerId::from_u64(7), 9);
    assert_eq!(handle.unpack(), (velo::WorkerId::from_u64(7), 9));
    assert_ne!(MESSENGER_MUX_KEY, TCP_STREAM_KEY);
    let _ = AnchorConfig::default();
    let _ = MuxConfig::default();
}
