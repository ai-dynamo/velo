// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A hub for velo applications.
//!
//! A hub is one service that is the peer discovery for a velo application
//! and also hosts pluggable extensions.

#![deny(missing_docs)]

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
mod hub;
#[cfg(feature = "server")]
pub mod store;

#[cfg(feature = "server")]
pub use hub::*;

#[cfg(feature = "server")]
pub use axum;
/// The DER certificate and key types that [`HubTls`] takes.
#[cfg(feature = "server")]
pub use rustls::pki_types;
pub use velo;
pub use velo_hub_ext as protocol;
