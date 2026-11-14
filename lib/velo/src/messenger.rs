// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Velo Messenger
//!
//! Active messaging layer for Velo distributed systems. Provides client, server,
//! and handler infrastructure for request-response and fire-and-forget messaging
//! patterns over pluggable transports. Includes a distributed event system.

pub(crate) mod client;
pub(crate) mod large_payload;
#[allow(clippy::module_inception)]
mod messenger;

pub(crate) mod common;
pub(crate) mod handlers;
pub(crate) mod server;

pub(crate) mod discovery;
#[cfg(feature = "services")]
pub(crate) mod events;

// The handler and builder types have their public path at the crate root
// (`velo::Handler`, ...), not here.
pub(crate) use client::ActiveMessageClient;
pub use common::MessageId;
pub(crate) use discovery::PeerDiscovery;
pub(crate) use messenger::MessengerParts;
#[allow(deprecated)] // re-exported until 0.23.0 removes `MessengerBuilder`
pub use messenger::{Messenger, MessengerBuilder};
