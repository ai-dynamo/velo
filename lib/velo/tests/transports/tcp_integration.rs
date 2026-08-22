// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TCP transport

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, TcpFactory, scenarios};

transport_integration_tests!(TcpFactory);
transport_epoch_tests!(TcpFactory);
