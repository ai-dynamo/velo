<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Move off the Messenger front door in 0.22.1

`Velo` is the entry point of the crate. `Messenger` is the messaging half that `Velo` wraps, and it stays public: `Velo::messenger()` returns it, and handlers, sends, and events still go through it.

What 0.22.1 deprecates is building and managing an instance through `Messenger` directly. A bare `Messenger` has no streaming transport, no anchor or rendezvous manager, and no per-stream listener. A peer that you register on it is not known to the streaming side, so a later `attach_anchor` fails with "peer not registered". The `Velo` forms do the whole job.

The items below warn in 0.22.1 and are removed in 0.23.0.

| Deprecated | Use |
|---|---|
| `Messenger::builder`, `MessengerBuilder` | `Velo::builder` (`VeloBuilder`) |
| `Messenger::register_peer` | `Velo::register_peer` |
| `Messenger::unregister_peer` | `Velo::unregister_peer` |
| `Messenger::discover_and_register_peer` | `Velo::discover_and_register_peer` |
| `Messenger::begin_drain` | `Velo::begin_drain` |
| `Messenger::graceful_shutdown` | `Velo::shutdown` |
| `Velo::graceful_shutdown` | `Velo::shutdown` |
| `Messenger::set_large_payload_support` | none: `Velo::builder` sets it up |

`Velo::shutdown` runs the same four phases as `graceful_shutdown`, then stops the streaming services. In 0.23.0 `graceful_shutdown` folds into `shutdown`. If you call `begin_drain` and then `shutdown(policy)`, the sequence is the same as before.

```rust,ignore
// before
let messenger = Messenger::builder().add_transport(transport).build().await?;
messenger.register_peer(peer_info)?;
messenger.graceful_shutdown(policy).await;

// after
let velo = Velo::builder().add_transport(transport).build().await?;
velo.register_peer(peer_info)?;
velo.shutdown(policy).await;
```

`VeloBuilder` has the same `add_transport`, `discovery`, and `metrics` methods. Where code needs the `Arc<Messenger>`, call `velo.messenger()`. A `Velo` opens no stream listener unless the builder calls `stream_config` or `stream_bind_addr` (see [Services and stream transports](migrate-services.md)).

## What moves in the known consumers

- **dynamo**: `lib/kvbm-config/src/messenger.rs:60`, `lib/kvbm-engine/src/testing/messenger.rs:32,73-74`, and `lib/runtime/src/pipeline/network/velo_response.rs:1659`.
- **kvbm**:
  - the test helper `crates/kvbm-engine/src/testing/messenger.rs:32`;
  - `Messenger::register_peer` in `crates/kvbm-connector/src/connector/leader/mod.rs:674` and `src/runtime.rs:199`, which move to the `Velo` that `KvbmRuntime` holds;
  - four calls in `crates/kvbm-hub/tests/tier_push_integration.rs`, which move to the same `Velo`.
- **dynamo-gdl**: `crates/gdl-runtime/src/dist/fabric.rs:142` (`graceful_shutdown`) and `:935`, `:940` (a test `Messenger`).

Line numbers are those of the versions that were current when 0.22.1 was written.
