// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Discrete-event simulation transport for Velo.
//!
//! Run velo instances under virtual time in a single process, communicating
//! through a simulated network fabric with configurable delays and congestion.
//!
//! # Architecture
//!
//! - [`SimFabric`] owns the network model and all in-flight transfer state
//! - [`SimTransport`] implements the [`Transport`](velo_transports::Transport) trait,
//!   routing messages through the fabric's DES queue
//! - [`SimDiscovery`] implements [`PeerDiscovery`](velo_discovery::PeerDiscovery)
//!   with an in-memory registry
//! - [`NetworkModel`] is a pluggable trait; [`BisectionBandwidth`] is the default
//!
//! # Example
//!
//! ```ignore
//! use loom_rs::sim::SimulationRuntime;
//! use velo_simulation::{SimFabric, SimTransport, SimDiscovery, BisectionBandwidth};
//!
//! let mut sim = SimulationRuntime::new()?;
//! let handle = sim.handle();
//! let fabric = Arc::new(SimFabric::new(handle, BisectionBandwidth::default()));
//! let discovery = Arc::new(SimDiscovery::new(fabric.clone()));
//!
//! // Build velo instances with SimTransport and SimDiscovery
//! // ...
//! ```

pub mod discovery;
pub mod fabric;
pub mod network;
pub mod transport;

pub use discovery::SimDiscovery;
pub use fabric::SimFabric;
pub use network::{BisectionBandwidth, NetworkModel, NextCompletion};
pub use transport::SimTransport;
