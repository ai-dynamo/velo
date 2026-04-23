// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ping-pong benchmark for the Velo messaging facade.
//!
//! Measures round-trip time (RTT) for unary messages between two `Velo`
//! instances running in the same process on separate single-threaded tokio
//! runtimes.

use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use tokio::time::sleep;
use velo::{Handler, Velo};
use velo_examples::{TransportArgs, new_transport};

#[derive(Parser, Debug)]
#[command(name = "ping_pong")]
#[command(about = "Benchmark Velo unary message RTT performance")]
struct Args {
    /// Number of ping-pong iterations.
    #[arg(long, default_value = "1000")]
    rounds: u32,

    #[command(flatten)]
    tx: TransportArgs,
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Using {:?} transport", args.tx.transport);

    let runtime_server = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let runtime_client = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let (peer_info_tx, peer_info_rx) = std::sync::mpsc::channel();
    let transport_type = args.tx.transport;

    let server_handle = std::thread::spawn(move || {
        runtime_server.block_on(async {
            let transport = new_transport(transport_type, "ping-pong")
                .await
                .expect("build server transport");
            let velo = Velo::builder()
                .add_transport(transport)
                .build()
                .await
                .expect("build server velo");

            sleep(Duration::from_millis(100)).await;

            let ping_handler =
                Handler::unary_handler("ping", |_ctx| Ok(Some(Bytes::new()))).build();
            velo.register_handler(ping_handler).unwrap();

            peer_info_tx.send(velo.peer_info()).unwrap();
            println!("Server started");
            println!("Server instance ID: {:?}", velo.instance_id());

            std::future::pending::<()>().await;
        });
    });

    let server_peer_info = peer_info_rx.recv().unwrap();
    let transport_type = args.tx.transport;

    let client_handle = std::thread::spawn(move || {
        runtime_client.block_on(async {
            let transport = new_transport(transport_type, "ping-pong").await?;
            let velo = Velo::builder().add_transport(transport).build().await?;

            sleep(Duration::from_millis(100)).await;

            println!("Client started");
            println!("Client instance ID: {:?}", velo.instance_id());
            println!("Connecting to server: {:?}", server_peer_info.instance_id());

            velo.register_peer(server_peer_info.clone())?;

            sleep(Duration::from_millis(500)).await;

            println!("Performing warmup ping...");
            let warmup = velo
                .unary("ping")?
                .raw_payload(Bytes::new())
                .instance(server_peer_info.instance_id())
                .send()
                .await;
            match warmup {
                Ok(_) => println!("Warmup complete"),
                Err(e) => {
                    println!("Warmup failed: {e}");
                    return Err(e);
                }
            }

            sleep(Duration::from_millis(50)).await;

            println!(
                "Starting ping-pong measurements for {} rounds...",
                args.rounds
            );

            let mut rtts = Vec::with_capacity(args.rounds as usize);
            for i in 1..=args.rounds {
                let start = Instant::now();
                let result = velo
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
                            println!("Round {i}: RTT = {rtt:?}");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Round {i} failed: {e}");
                    }
                }
            }

            if rtts.is_empty() {
                println!("No successful ping-pong rounds completed");
            } else {
                let total: Duration = rtts.iter().sum();
                let avg = total / rtts.len() as u32;
                let min = rtts.iter().min().unwrap();
                let max = rtts.iter().max().unwrap();
                let variance = rtts
                    .iter()
                    .map(|rtt| {
                        let diff = rtt.as_nanos() as f64 - avg.as_nanos() as f64;
                        diff * diff
                    })
                    .sum::<f64>()
                    / rtts.len() as f64;
                let std_dev = Duration::from_nanos(variance.sqrt() as u64);

                println!("\n=== RTT Statistics ===");
                println!("Rounds completed: {}/{}", rtts.len(), args.rounds);
                println!("Average RTT: {avg:?}");
                println!("Min RTT: {min:?}");
                println!("Max RTT: {max:?}");
                println!("Std deviation: {std_dev:?}");
            }

            Ok(())
        })
    });

    client_handle.join().unwrap()?;
    drop(server_handle);
    Ok(())
}
