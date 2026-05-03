// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`FrameTransport`] is defined in [`velo_ext::streaming`] and re-exported
//! here so existing imports continue to compile during the workspace
//! migration to the two-crate layout.

pub use velo_ext::streaming::FrameTransport;
