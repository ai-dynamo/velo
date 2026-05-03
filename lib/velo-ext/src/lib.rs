// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![deny(missing_docs)]

//! Extension trait surface for Velo.
//!
//! Out-of-tree implementors of [`Transport`](transport::Transport),
//! [`FrameTransport`](streaming::FrameTransport), and
//! [`PeerDiscovery`](discovery::PeerDiscovery) depend on this crate. It
//! contains the trait definitions and the value/error types that appear in
//! their signatures — nothing more. Anyone who only *uses* Velo should depend
//! on the `velo` crate; only plugin authors should reach for `velo-ext`.
//!
//! ## Stability
//!
//! This crate is the contract that downstream impls compile against. New trait
//! methods land with default impls; signature changes require a deliberate
//! version bump coordinated with `velo`. See `Cargo.toml` for the full policy.

pub mod discovery;
pub mod id;
pub mod observability;
pub mod streaming;
pub mod transport;

// Convenience re-exports at the crate root for the most-used items. Plugin
// authors typically write `use velo_ext::Transport;` or `use velo_ext::WorkerId;`.
pub use discovery::{
    PeerDiscovery, PeerRegistrationGuard, ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard,
};
pub use id::{InstanceId, PeerInfo, TransportKey, WorkerAddress, WorkerAddressError, WorkerId};
pub use observability::{Direction, TransportObservability, TransportRejection};
pub use streaming::FrameTransport;
pub use transport::{
    DataStreams, HealthCheckError, InFlightGuard, MessageType, SendBackpressure, SendOutcome,
    ShutdownPolicy, ShutdownState, Transport, TransportAdapter, TransportError,
    TransportErrorHandler, make_channels, try_send_or_backpressure,
};
