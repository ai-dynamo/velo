// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NATS client connection utilities.
//!
//! Convenience helpers for creating `async_nats::Client` instances with sensible
//! defaults. These are separate from `NatsTransportBuilder` — the builder accepts
//! a pre-instantiated `Arc<Client>` so callers can manage TLS/auth themselves.
//! This module provides a quick path for common configurations.

use std::sync::Arc;

/// Connect to a NATS server with default options.
///
/// Returns an `Arc<async_nats::Client>` ready to pass to `NatsTransportBuilder::new()`.
///
/// # Arguments
///
/// * `url` - NATS server URL (e.g., `"nats://localhost:4222"`)
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), async_nats::ConnectError> {
/// use crate::transports::nats::utils::connect;
/// let client = connect("nats://localhost:4222").await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect(url: &str) -> Result<Arc<async_nats::Client>, async_nats::ConnectError> {
    let client = async_nats::connect(url).await?;
    Ok(Arc::new(client))
}

/// Connect to a NATS server with custom options.
///
/// Applies the given options before connecting. Use this for TLS, authentication,
/// custom timeouts, or other advanced configuration.
///
/// # Arguments
///
/// * `url` - NATS server URL (e.g., `"nats://localhost:4222"`)
/// * `options` - Pre-configured [`async_nats::ConnectOptions`]
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), async_nats::ConnectError> {
/// use crate::transports::nats::utils::connect_with_options;
/// let options = async_nats::ConnectOptions::new()
///     .name("velo-instance-1");
/// let client = connect_with_options("nats://localhost:4222", options).await?;
/// # Ok(())
/// # }
/// ```
pub async fn connect_with_options(
    url: &str,
    options: async_nats::ConnectOptions,
) -> Result<Arc<async_nats::Client>, async_nats::ConnectError> {
    let client = options.connect(url).await?;
    Ok(Arc::new(client))
}
