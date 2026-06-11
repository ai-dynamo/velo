// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TIPC transport

#![cfg(all(feature = "tipc", target_os = "linux"))]

#[macro_use]
mod common;

use common::{TipcFactory, scenarios};

transport_integration_tests!(TipcFactory);
