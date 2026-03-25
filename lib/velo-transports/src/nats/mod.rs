// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS transport implementation.
//!
//! This module provides the [`NatsTransport`] struct and its builder, along with
//! subject naming utilities for NATS pub/sub messaging, and convenience helpers
//! for creating NATS client connections.

pub(crate) mod subjects;
mod transport;
/// NATS client connection utilities.
pub mod utils;

pub(crate) use subjects::{health_subject, inbound_subject};
pub use transport::{NatsTransport, NatsTransportBuilder};
