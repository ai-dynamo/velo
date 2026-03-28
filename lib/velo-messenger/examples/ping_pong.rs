// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ping-pong benchmark for Messenger active message system.
//!
//! This benchmark measures round-trip time (RTT) for unary messages between two Messenger instances
//! running in the same process on separate single-threaded tokio runtimes.

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use velo_messenger::{Handler, Messenger};
use velo_transports::Transport;
use velo_transports::tcp::TcpTransportBuilder;

#[cfg(feature = "grpc")]
use velo_transports::grpc::GrpcTransportBuilder;
#[cfg(feature = "nats")]
use velo_transports::nats::{NatsTransportBuilder, utils::connect};

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Supported transport backends for the ping-pong benchmark.
#[derive(Debug, Clone, ValueEnum)]
enum TransportType {
    /// TCP transport (default)
    Tcp,
    /// ZMQ DEALER/ROUTER transport
    #[cfg(feature = "zmq")]
    Zmq,
    /// NATS transport (requires NATS server on localhost:4222)
    #[cfg(feature = "nats")]
    Nats,
    /// gRPC transport
    #[cfg(feature = "grpc")]
    Grpc,
}

/// Create a transport instance based on the selected type.
async fn new_transport(transport_type: &TransportType) -> Arc<dyn Transport> {
    match transport_type {
        TransportType::Tcp => {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            Arc::new(
                TcpTransportBuilder::new()
                    .from_listener(listener)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        }
        #[cfg(feature = "zmq")]
        TransportType::Zmq => Arc::new(
            velo_transports::zmq::ZmqTransportBuilder::new()
                .bind_endpoint("tcp://127.0.0.1:0")
                .build()
                .unwrap(),
        ),
        #[cfg(feature = "nats")]
        TransportType::Nats => {
            let client = connect("nats://127.0.0.1:4222")
                .await
                .expect("failed to connect to NATS server at 127.0.0.1:4222");
            Arc::new(NatsTransportBuilder::new(client, "ping-pong").build())
        }
        #[cfg(feature = "grpc")]
        TransportType::Grpc => Arc::new(GrpcTransportBuilder::new().build().unwrap()),
    }
}

/// CLI arguments for ping-pong benchmark
#[derive(Parser, Debug)]
#[command(name = "ping_pong")]
#[command(about = "Benchmark Messenger unary message RTT performance")]
struct Args {
    /// Number of ping-pong iterations
    #[arg(long, default_value = "1000")]
    rounds: u32,

    /// Transport backend to use
    #[arg(long, default_value = "tcp")]
    transport: TransportType,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Using {:?} transport", args.transport);

    // Create two separate single-threaded tokio runtimes
    let runtime_server = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let runtime_client = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Channel to pass PeerInfo from server to client
    let (peer_info_tx, peer_info_rx) = std::sync::mpsc::channel();

    let transport_type = args.transport.clone();

    // Spawn server thread
    let server_handle = std::thread::spawn(move || {
        runtime_server.block_on(async {
            // Create server Messenger instance with selected transport
            let transport = new_transport(&transport_type).await;
            let messenger = Messenger::builder()
                .add_transport(transport)
                .build()
                .await
                .unwrap();

            // Give transport a moment to bind and start accepting connections
            sleep(Duration::from_millis(100)).await;

            // Register ping handler that returns empty bytes
            let ping_handler =
                Handler::unary_handler("ping", |_ctx| Ok(Some(Bytes::new()))).build();

            messenger.register_handler(ping_handler).unwrap();

            // Get PeerInfo and send to client
            let peer_info = messenger.peer_info();
            peer_info_tx.send(peer_info).unwrap();

            println!("Server started");
            println!("Server instance ID: {:?}", messenger.instance_id());

            // Keep runtime alive to handle requests
            std::future::pending::<()>().await;
        });
    });

    // Wait for server to send PeerInfo
    let server_peer_info = peer_info_rx.recv().unwrap();

    let transport_type = args.transport.clone();

    // Spawn client thread
    let client_handle = std::thread::spawn(move || {
        runtime_client.block_on(async {
            // Create client Messenger instance with selected transport
            let transport = new_transport(&transport_type).await;
            let messenger = Messenger::builder()
                .add_transport(transport)
                .build()
                .await
                .unwrap();

            // Give transport a moment to bind
            sleep(Duration::from_millis(100)).await;

            println!("Client started");
            println!("Client instance ID: {:?}", messenger.instance_id());
            println!("Connecting to server: {:?}", server_peer_info.instance_id());

            // Register peer
            messenger.register_peer(server_peer_info.clone()).unwrap();

            // Wait for connection to establish
            sleep(Duration::from_millis(500)).await;

            println!("Performing warmup ping...");

            // Warmup ping
            let warmup_result = messenger
                .unary("ping")?
                .raw_payload(Bytes::new())
                .instance(server_peer_info.instance_id())
                .send()
                .await;

            match warmup_result {
                Ok(_) => println!("Warmup complete"),
                Err(e) => {
                    println!("Warmup failed: {}", e);
                    return Err(e);
                }
            }

            sleep(Duration::from_millis(50)).await;

            println!(
                "Starting ping-pong measurements for {} rounds...",
                args.rounds
            );

            let mut rtts = Vec::new();

            for i in 1..=args.rounds {
                let start = Instant::now();

                let result = messenger
                    .unary("ping")?
                    .raw_payload(Bytes::new())
                    .instance(server_peer_info.instance_id())
                    .send()
                    .await;

                match result {
                    Ok(_) => {
                        let rtt = start.elapsed();
                        rtts.push(rtt);

                        if i % 100 == 0 {
                            println!("Round {}: RTT = {:?}", i, rtt);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Round {} failed: {}", i, e);
                    }
                }
            }

            if !rtts.is_empty() {
                let total: Duration = rtts.iter().sum();
                let avg = total / rtts.len() as u32;
                let min = rtts.iter().min().unwrap();
                let max = rtts.iter().max().unwrap();

                println!("\n=== RTT Statistics ===");
                println!("Rounds completed: {}/{}", rtts.len(), args.rounds);
                println!("Average RTT: {:?}", avg);
                println!("Min RTT: {:?}", min);
                println!("Max RTT: {:?}", max);

                let variance: f64 = rtts
                    .iter()
                    .map(|rtt| {
                        let diff = rtt.as_nanos() as f64 - avg.as_nanos() as f64;
                        diff * diff
                    })
                    .sum::<f64>()
                    / rtts.len() as f64;

                let std_dev = Duration::from_nanos(variance.sqrt() as u64);
                println!("Std deviation: {:?}", std_dev);
            } else {
                println!("No successful ping-pong rounds completed");
            }

            Ok(())
        })
    });

    // Wait for client to complete
    let client_result: Result<()> = client_handle.join().unwrap();
    client_result?;

    // Server will run indefinitely, but we can drop it here
    drop(server_handle);

    Ok(())
}
