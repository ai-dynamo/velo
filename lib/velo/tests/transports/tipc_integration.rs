// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TIPC transport
//!
//! Gated on `#[cfg(velo_tipc)]` (a rustc `--cfg` flag, not a Cargo feature)
//! so that `cargo test --all-features` on module-less Linux hosts compiles this
//! binary but runs zero tests — keeping the main CI job green regardless of
//! whether `tipc.ko` is present.  The dedicated `tipc-tests` CI job passes
//! `RUSTFLAGS="--cfg velo_tipc"` to activate these tests on a verified host.

#![cfg(velo_tipc)]

#[macro_use]
mod common;

use common::{TipcFactory, scenarios};

transport_integration_tests!(TipcFactory);
