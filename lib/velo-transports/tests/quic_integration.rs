// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for QUIC transport

#![cfg(feature = "quic")]

#[macro_use]
mod common;

use common::{QuicFactory, scenarios};

transport_integration_tests!(QuicFactory);
