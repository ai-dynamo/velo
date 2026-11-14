<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Move off the Messenger front door

`Velo` is the entry point of the crate. `Messenger` is the messaging half that `Velo` wraps, and it stays public: `Velo::messenger()` returns it, and handlers, sends, and events still go through it.

0.22.1 deprecates building and managing an instance through `Messenger` directly, and 0.23.0 removes it. A bare `Messenger` has no streaming transport, no anchor or rendezvous manager, and no per-stream listener. A peer that you register on it is not known to the streaming side, so a later `attach_anchor` fails with "peer not registered". The `Velo` forms do the whole job.

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

`Velo::shutdown` runs the same four phases as `graceful_shutdown`, then stops the streaming services. If you call `begin_drain` and then `shutdown(policy)`, the sequence is the same as before.

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

`VeloBuilder` has the same `add_transport`, `discovery`, and `metrics` methods. Where code needs the `Arc<Messenger>`, call `velo.messenger()`. A `Velo` that opens no stream listener is built with `.mux_only()` (see [Services and mux-only mode](migrate-services.md)).

## 0.23.0: the deprecated items are gone

Every item in the table above is removed. There is no forwarding method and no alias, so a call to one fails to compile. Also:

- `Velo::shutdown` is the one shutdown. It keeps everything `graceful_shutdown` did: the RDMA sweep before transport teardown, the join of the mux senders, and one run across clones.
- `velo::transports::VeloBackend` is no longer public. Code that built one with `VeloBackend::new` has no replacement: build a `Velo`. `VeloBackendError` stays public, because a failed send can carry it.
- Code outside velo cannot build a `Messenger` without a `Velo`. A test helper that built a bare `Messenger` builds a `Velo` with `.mux_only()`, keeps the `Velo` alive, registers peers on it, and passes `velo.messenger()` where it passed the `Messenger`:

```rust,ignore
let velo = Velo::builder()
    .add_transport(transport)
    .mux_only()
    .build()
    .await?;
velo.register_peer(peer_info)?;
let messenger = Arc::clone(velo.messenger());
```

A `Velo` registers its own anchor, rendezvous, and mux handlers on its `Messenger`. A test that builds its own `AnchorManager` or `RendezvousManager` over that `Messenger` replaces the `Velo`'s handlers of the same name.

## What moves in the known consumers

- **dynamo**:
  - `Messenger::builder` in `lib/kvbm-config/src/messenger.rs:60` and in the test helper `lib/kvbm-engine/src/testing/messenger.rs:32` moves to `Velo::builder`. The test helper uses `.mux_only()`.
  - `Messenger::register_peer` in `lib/kvbm-engine/src/testing/messenger.rs:73-74` moves to `Velo::register_peer`.
  - `graceful_shutdown` in the test at `lib/runtime/src/pipeline/network/velo_response.rs:1659` moves to `shutdown`.
- **kvbm**:
  - the test helper `crates/kvbm-engine/src/testing/messenger.rs:32` moves to a mux-only `Velo`, as in dynamo;
  - `Messenger::register_peer` in `crates/kvbm-connector/src/connector/leader/mod.rs:674` and `src/runtime.rs:199` moves to the `Velo` that `KvbmRuntime` holds;
  - four calls in `crates/kvbm-hub/tests/tier_push_integration.rs` move to the same `Velo`.
- **dynamo-gdl**:
  - `graceful_shutdown` in `crates/gdl-runtime/src/dist/fabric.rs:142` moves to `shutdown`;
  - the test `Messenger` at `:935` and `:940` moves to a mux-only `Velo`.

Line numbers are those of the versions that were current when 0.22.1 was written.
