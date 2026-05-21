// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two-instance Velo bring-up shared by every soak scenario.
//!
//! Pattern mirrors `examples/examples/throughput.rs`: each side runs on its
//! own multi-thread runtime so the client can saturate the server without
//! starving its own send path. Each side also gets its own metrics registry
//! so the oracle can diff them independently.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prometheus::Registry;
use tokio::time::sleep;
use velo::{InstanceId, PeerInfo, StreamConfig, Velo, VeloMetrics};
use velo_examples::{TransportType, new_transport};

use super::StreamTransportKind;

/// One side of a server/client pair.
pub struct VeloSide {
    pub velo: Arc<Velo>,
    pub registry: Registry,
    pub metrics: Arc<VeloMetrics>,
    pub instance_id: InstanceId,
    pub peer_info: PeerInfo,
}

/// A connected pair of Velo instances. Both sides know about each other.
pub struct Pair {
    pub server: VeloSide,
    pub client: VeloSide,
}

/// Build a side with its own metrics registry, messenger transport, and
/// streaming transport.
async fn build_side(
    transport: TransportType,
    stream_transport: StreamTransportKind,
    tag: &str,
) -> Result<VeloSide> {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry)?);

    let tx = new_transport(transport, tag).await?;
    let stream_cfg = match stream_transport {
        StreamTransportKind::Tcp => StreamConfig::Tcp(None),
        #[cfg(feature = "grpc")]
        StreamTransportKind::Grpc => StreamConfig::Grpc(None),
        #[cfg(not(feature = "grpc"))]
        StreamTransportKind::Grpc => {
            anyhow::bail!("--stream-transport grpc requires building with --features grpc");
        }
    };
    let velo = Velo::builder()
        .add_transport(tx)
        .stream_config(stream_cfg)?
        .metrics(Arc::clone(&metrics))
        .build()
        .await?;

    let instance_id = velo.instance_id();
    let peer_info = velo.peer_info();

    Ok(VeloSide {
        velo,
        registry,
        metrics,
        instance_id,
        peer_info,
    })
}

/// Build a connected pair on a single async runtime. Suitable for scenarios
/// that don't need to kill one side mid-run.
pub async fn build_pair(
    transport: TransportType,
    stream_transport: StreamTransportKind,
    tag: &str,
) -> Result<Pair> {
    let server = build_side(transport, stream_transport, &format!("{tag}-srv")).await?;
    let client = build_side(transport, stream_transport, &format!("{tag}-cli")).await?;

    // Cross-register so both directions resolve.
    server.velo.register_peer(client.peer_info.clone())?;
    client.velo.register_peer(server.peer_info.clone())?;

    // Brief settle so listeners are ready before the scenario starts dispatching.
    sleep(Duration::from_millis(100)).await;

    Ok(Pair { server, client })
}
