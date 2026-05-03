// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for UDS graceful shutdown

#![cfg(unix)]

#[macro_use]
mod common;

use common::{UdsShutdownClient, shutdown_scenarios};

transport_shutdown_tests!(uds, UdsShutdownClient);
