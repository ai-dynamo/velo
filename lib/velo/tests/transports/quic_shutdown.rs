// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Graceful-shutdown tests for the QUIC transport.

#![cfg(feature = "quic")]

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, QuicShutdownClient, shutdown_scenarios};

transport_shutdown_tests!(quic, QuicShutdownClient);
