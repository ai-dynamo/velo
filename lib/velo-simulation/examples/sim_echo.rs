// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two velo instances communicating through simulated fabric.
//!
//! Instance B registers an "echo" handler. Instance A sends a unary request
//! to B and receives the echoed response. All communication travels through
//! the simulated network fabric under virtual time.
//!
//! Run with: cargo run -p velo-simulation --example sim_echo

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use loom_rs::sim::SimulationRuntime;
use velo::Handler;
use velo_simulation::{BisectionBandwidth, SimDiscovery, SimFabric, SimTransport};

fn main() -> anyhow::Result<()> {
    // 1. Create simulation runtime
    let mut sim = SimulationRuntime::new()?;
    let handle = sim.handle();

    // 2. Create shared fabric with realistic network model
    let fabric = Arc::new(SimFabric::new(
        handle.clone(),
        BisectionBandwidth {
            link_gbps: 200.0,
            bisection_gbps: 12_800.0,
            base_latency: Duration::from_micros(10),
        },
    ));
    let discovery = Arc::new(SimDiscovery::new(fabric.clone()));

    // 3. Build two velo instances inside the sim's tokio context
    let fabric_setup = fabric.clone();
    let discovery_setup = discovery.clone();
    let (velo_a, velo_b) = sim.run_until_complete(async move {
        let transport_a = Arc::new(SimTransport::new(fabric_setup.clone()));
        let a = velo::Velo::builder()
            .add_transport(transport_a)
            .discovery(discovery_setup.clone())
            .build()
            .await
            .expect("build velo A");

        let transport_b = Arc::new(SimTransport::new(fabric_setup.clone()));
        let b = velo::Velo::builder()
            .add_transport(transport_b)
            .discovery(discovery_setup.clone())
            .build()
            .await
            .expect("build velo B");

        // Register with discovery
        discovery_setup.register(a.peer_info());
        discovery_setup.register(b.peer_info());

        // Cross-register peers
        a.register_peer(b.peer_info()).unwrap();
        b.register_peer(a.peer_info()).unwrap();

        // Register echo handler on B
        b.register_handler(Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload))).build())
            .unwrap();

        (a, b)
    })?;

    println!("Instance A: {}", velo_a.instance_id());
    println!("Instance B: {}", velo_b.instance_id());
    println!("Virtual time after setup: {:?}", sim.now());

    // 4. Perform handshake (A discovers B's handlers) — separate from the echo
    let a_handshake = velo_a.clone();
    let b_id = velo_b.instance_id();
    sim.run_until_complete(async move {
        a_handshake.wait_for_handler(b_id, "echo").await.unwrap();
    })?;
    let handshake_time = sim.now();
    println!("Handshake complete at: {handshake_time:?}");

    // 5. Now send the actual echo — should only be one round-trip
    let a_echo = velo_a.clone();
    handle.spawn_at(sim.now(), async move {
        let resp = a_echo
            .unary("echo")
            .expect("build unary")
            .instance(b_id)
            .raw_payload(Bytes::from("hello from simulation"))
            .send()
            .await
            .expect("unary send");
        println!("Response: {:?}", String::from_utf8_lossy(&resp));
        assert_eq!(resp, Bytes::from("hello from simulation"));
    });

    let final_time = sim.run()?;
    let echo_time = final_time - handshake_time;
    println!("Echo round-trip: {echo_time:?}");
    println!("Total virtual time: {final_time:?}");

    Ok(())
}
