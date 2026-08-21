// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The [`Transport`] trait and supporting value/error types are defined in
//! [`velo_ext::transport`] and re-exported here for backwards compatibility
//! while the workspace migrates to the two-crate (`velo` + `velo-ext`)
//! layout.
//!
//! Send admission lives in [`velo_ext::admission`] rather than alongside the
//! trait, because the gate is machinery a transport *uses* rather than part of
//! the contract it implements. The types that do appear in
//! [`Transport::send_message`]'s signature are re-exported here too, so callers
//! reaching into `velo::transports::*` get one complete surface.

pub use velo_ext::admission::{
    AdmissionError, AdmissionGate, AdmissionState, SendAdmission, SendOutcome,
};
pub use velo_ext::transport::{
    AdmitOutcome, DataStreams, HealthCheckError, InFlightGuard, InboundMessage, MessageType,
    ShutdownPolicy, ShutdownState, Transport, TransportAdapter, TransportError,
    TransportErrorHandler, make_channels,
};
