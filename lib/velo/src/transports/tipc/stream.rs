// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `TipcStream`: `AsyncFd`-backed `AsyncRead`/`AsyncWrite` wrapper with non-blocking connect
//! and `poll_shutdown` mapped to `Shutdown::Both` (TIPC has no half-close).
