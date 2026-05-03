// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Trait-based extension point for transparent large payload handling.
//!
//! These traits allow external crates (e.g., `velo-rendezvous`) to plug into the
//! messenger send/receive path without creating a circular dependency.
//!
//! **Sender side** ([`LargePayloadStager`]): When a payload exceeds a threshold,
//! the stager stores it locally and returns a handle string to embed in the message
//! headers. The actual payload is replaced with an empty `Bytes`.
//!
//! **Receiver side** ([`LargePayloadResolver`]): When a message arrives with the
//! `_rv` header, the resolver fetches the full payload from the remote stager
//! before the message is dispatched to the handler.

use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;

/// Sender-side: stages a large payload locally and returns a handle string.
///
/// Called synchronously in the `send_message()` hot path when the payload
/// exceeds [`threshold()`](LargePayloadStager::threshold).
pub trait LargePayloadStager: Send + Sync {
    /// Stage the payload and return a handle string to embed in the `_rv` header.
    fn stage(&self, payload: Bytes) -> String;

    /// Minimum payload size (in bytes) to trigger staging.
    /// Payloads smaller than this are sent inline as usual.
    fn threshold(&self) -> usize;
}

/// Receiver-side: resolves a handle string back to the full payload.
///
/// Called asynchronously when a received message contains the `_rv` header.
/// The implementation should fetch the data and release the remote handle
/// (since transparent mode uses single-use staging).
pub trait LargePayloadResolver: Send + Sync {
    /// Fetch the full payload from the handle string and release the remote slot.
    fn resolve(&self, handle_str: &str) -> BoxFuture<'_, Result<Bytes>>;
}

/// Header key used to signal that the payload was staged via rendezvous.
pub const RV_HEADER_KEY: &str = "_rv";
