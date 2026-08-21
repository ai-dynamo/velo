// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the UCX transport.
//!
//! Runs over `UCX_TLS=tcp` — the identical code path as RDMA lanes, with no
//! hardware requirement — so this suite runs on stock CI runners.

#![cfg(all(target_os = "linux", feature = "ucx"))]

#[macro_use]
mod common;

use common::{UcxFactory, scenarios};

transport_integration_tests!(UcxFactory);
