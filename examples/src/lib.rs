// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for Velo examples — CLI transport selection, transport
//! construction, and a default tracing subscriber.

use std::sync::Arc;

use anyhow::Result;
use clap::ValueEnum;
use velo::transports::Transport;

/// Transport backends selectable at the CLI.
///
/// NATS is available whenever `velo-messenger`'s default features are enabled
/// (which is the case for any consumer of the `velo` facade today). ZMQ and
/// gRPC require explicit features on this crate.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TransportType {
    /// TCP transport (default).
    Tcp,
    /// Unix domain socket transport.
    #[cfg(unix)]
    Uds,
    /// ZMQ DEALER/ROUTER transport.
    #[cfg(feature = "zmq")]
    Zmq,
    /// NATS transport. Requires a local `nats-server` on `127.0.0.1:4222`.
    Nats,
    /// gRPC transport.
    #[cfg(feature = "grpc")]
    Grpc,
}

/// Build a transport on loopback for an example.
///
/// `tag` is used for NATS cluster IDs and UDS socket filenames so different
/// examples (or server/client pairs within one example) don't collide.
pub async fn new_transport(ty: TransportType, tag: &str) -> Result<Arc<dyn Transport>> {
    match ty {
        TransportType::Tcp => {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            Ok(Arc::new(
                velo::transports::tcp::TcpTransportBuilder::new()
                    .from_listener(listener)?
                    .build()?,
            ))
        }
        #[cfg(unix)]
        TransportType::Uds => {
            let socket_path =
                std::env::temp_dir().join(format!("velo-{tag}-{}.sock", uuid::Uuid::new_v4()));
            Ok(Arc::new(
                velo::transports::uds::UdsTransportBuilder::new()
                    .socket_path(&socket_path)
                    .build()?,
            ))
        }
        #[cfg(feature = "zmq")]
        TransportType::Zmq => Ok(Arc::new(
            velo::transports::zmq::ZmqTransportBuilder::new()
                .bind_endpoint("tcp://127.0.0.1:0")
                .build()?,
        )),
        TransportType::Nats => {
            let client = velo::transports::nats::utils::connect("nats://127.0.0.1:4222")
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to connect to NATS at 127.0.0.1:4222: {e}")
                })?;
            Ok(Arc::new(
                velo::transports::nats::NatsTransportBuilder::new(client, tag).build(),
            ))
        }
        #[cfg(feature = "grpc")]
        TransportType::Grpc => {
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            Ok(Arc::new(
                velo::transports::grpc::GrpcTransportBuilder::new()
                    .from_listener(listener)?
                    .build()?,
            ))
        }
    }
}

/// Shared clap fragment: a single `--transport <backend>` flag.
///
/// Embed with `#[command(flatten)]` in an example's CLI struct.
#[derive(clap::Args, Debug, Clone)]
pub struct TransportArgs {
    /// Transport backend to use.
    #[arg(long, default_value = "tcp")]
    pub transport: TransportType,
}

/// Initialize a reasonable default `tracing_subscriber` for examples.
///
/// Honors `RUST_LOG`, defaulting to `info`.
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}
