<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# velo

The Velo runtime: active messages, typed streams, distributed events, large-payload transfer (with RDMA over UCX), work queues, peer discovery, and Prometheus metrics, over pluggable transports.

Velo is experimental. The APIs are not stable.

- Documentation: [the Velo book](https://ai-dynamo.github.io/velo/)
- Repository: <https://github.com/ai-dynamo/velo>

A `Velo` instance wraps a `Messenger`, an `AnchorManager`, and a `RendezvousManager`. Use `velo.messenger()`, `velo.anchor_manager()`, or `velo.rendezvous_manager()` for direct access.

To write a transport or discovery backend in another crate, depend on `velo-ext` instead. It is the small, stable trait surface.
