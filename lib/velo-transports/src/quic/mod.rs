// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! QUIC Transport Module
//!
//! This module provides a QUIC transport implementation built on `quinn` with:
//! - TLS 1.3 encryption (mandatory for QUIC)
//! - Zero-copy frame codec (reuses `TcpFrameCodec`)
//! - Frame type routing (Message, Response, Ack, Event)
//! - Graceful 3-phase shutdown (Gate, Drain, Teardown)
//! - Self-signed certificates by default for cluster-internal use

mod listener;
pub mod tls;
mod transport;

pub use transport::{QuicTransport, QuicTransportBuilder};
