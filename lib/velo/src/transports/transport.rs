// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The [`Transport`] trait and supporting value/error types are defined in
//! [`velo_ext::transport`] and re-exported here for backwards compatibility
//! while the workspace migrates to the two-crate (`velo` + `velo-ext`)
//! layout.

pub use velo_ext::transport::{
    DataStreams, HealthCheckError, InFlightGuard, MessageType, SendBackpressure, SendOutcome,
    ShutdownPolicy, ShutdownState, Transport, TransportAdapter, TransportError,
    TransportErrorHandler, make_channels, try_send_or_backpressure,
};
