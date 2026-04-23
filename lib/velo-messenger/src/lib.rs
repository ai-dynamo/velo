// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Velo Messenger
//!
//! Active messaging layer for Velo distributed systems. Provides client, server,
//! and handler infrastructure for request-response and fire-and-forget messaging
//! patterns over pluggable transports. Includes a distributed event system.

mod client;
pub mod large_payload;
mod messenger;

pub(crate) mod common;
pub(crate) mod handlers;
pub(crate) mod server;

pub mod discovery;
pub mod events;

pub use client::builders::{
    AmSendBuilder, AmSyncBuilder, SyncResult, TypedUnaryBuilder, TypedUnaryResult, UnaryBuilder,
    UnaryResult,
};
pub use common::MessageId;
pub use common::responses::{RegisterOutcome, ResponseRegistrationError, SlotBackpressure};
pub use discovery::PeerDiscovery;
pub use events::VeloEvents;
pub use handlers::{
    AmHandlerBuilder, AsyncExecutor, Context, DispatchMode, Handler, HandlerExecutor, SyncExecutor,
    TypedContext, TypedUnaryHandlerBuilder, UnaryHandlerBuilder, UnifiedResponse,
};
pub use large_payload::{LargePayloadResolver, LargePayloadStager, RV_HEADER_KEY};
pub use messenger::{Messenger, MessengerBuilder};
pub use server::EventFrameHandler;
pub use velo_observability::VeloMetrics;

// Re-exports from velo-common for convenience
pub use velo_common::{InstanceId, PeerInfo, WorkerAddress, WorkerId};

// Re-exports from velo-events for convenience
pub use velo_events::{
    Event, EventAwaiter, EventBackend, EventHandle, EventManager, EventPoison, EventStatus,
};
