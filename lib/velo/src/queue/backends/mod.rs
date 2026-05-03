// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Work queue backend implementations.

pub mod memory;

#[cfg(feature = "queue-messenger")]
pub mod messenger;

#[cfg(feature = "nats-queue")]
pub mod nats;
