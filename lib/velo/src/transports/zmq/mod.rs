// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ZMQ Transport Module
//!
//! This module provides a high-performance ZMQ transport implementation with:
//! - DEALER/ROUTER socket pattern for bidirectional messaging
//! - Efficient multipart framing (message type + header + payload, one copy per frame)
//! - Two dedicated I/O threads (one listener, one sender) regardless of peer count
//! - Graceful shutdown with drain support

mod listener;
mod transport;

pub use transport::{ZmqTransport, ZmqTransportBuilder};
