// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for NATS transport
//!
//! These tests require a running NATS server at nats://127.0.0.1:4222.
//! In CI, the NATS server is started as a service container.
//! Locally: `docker run -d -p 4222:4222 nats:latest`

#![cfg(feature = "nats")]

#[macro_use]
mod common;

use common::{NatsFactory, scenarios};

transport_integration_tests!(NatsFactory);
