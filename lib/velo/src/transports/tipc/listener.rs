// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Two-phase bind, accept/serve/route/drain loop, and graceful-close handling
//! (ECONNRESET with empty partial-frame buffer treated as graceful peer close).
