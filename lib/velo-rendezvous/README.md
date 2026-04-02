<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# velo-rendezvous

Receiver-driven large-payload transfer for Velo.

`velo-rendezvous` stages bytes on the owner worker, returns a compact `DataHandle`,
and lets a consumer pull the data later. The current implementation is in-memory
only and uses active messages for control plus chunked payload fetches.

## Flow

1. The owner calls `register_data` or `register_data_with` and gets a `DataHandle`.
2. The handle is passed through any normal Velo path.
3. The consumer calls `metadata`, `get`, or `get_into`.
4. After a successful `get` or `get_into`, the consumer must call `detach` or `release`.

`detach` releases the read lock but keeps the handle alive. `release` releases the
lock and decrements the refcount.

## Main Types

- `RendezvousManager`: owner- and consumer-side entry point.
- `DataHandle`: compact wire handle containing owner worker id + local slot id.
- `DataMetadata`: size, refcount, and pinned-state metadata.
- `RegisterOptions`: registration options for staged slots.
- `RendezvousWrite`: destination trait for `get_into`.
- `RendezvousStager` / `RendezvousResolver`: transparent large-payload bridge for `velo-messenger`.

## Transparent Mode

When wired through `Velo`, payloads above the staging threshold are automatically
replaced with an `_rv` header on send and resolved before handler dispatch on
receive. Small payloads continue to travel inline.

## Current Limits

- Phase 1 only: staged data lives in process memory as `Bytes`.
- Transfers are pulled chunk-by-chunk over active messages.
- `StageMode::Pinned` is a placeholder for future RDMA-backed transfer.
- `RegisterOptions::ttl` is stored with the slot, but this crate does not yet
  provide a background reaper or automatic expiry path.

## Tests

Focused unit tests live in the crate. End-to-end rendezvous and transparent
payload scenarios live in `lib/velo/tests/rendezvous_integration.rs`.
