// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the QUIC transport.

#![cfg(feature = "quic")]

#[macro_use]
mod common;

use common::{OUTER_TEST_TIMEOUT, QuicFactory, scenarios};

transport_integration_tests!(QuicFactory);
transport_epoch_tests!(QuicFactory);
