// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for ZMQ transport

#![cfg(feature = "zmq")]

#[macro_use]
mod common;

use common::{ZmqFactory, scenarios};

transport_integration_tests!(ZmqFactory);
