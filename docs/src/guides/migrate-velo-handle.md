<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Migrate to the Velo handle in 0.21

`Velo` is now a cheap handle. `VeloBuilder::build()` returns `Velo`. It used to return `Arc<Velo>`.

`Velo` is `Clone`. A clone adds one reference count and does not copy any state. Every clone is the same instance.

## How to migrate

- Replace `Arc<Velo>` with `Velo` in your types and signatures.
- Keep `.clone()` as it is. It now clones the handle.
- Change `Arc::clone(&velo)` to `velo.clone()`.
- Remove `Arc::new(velo)` if you wrapped a built `Velo`. Pass `velo` or `velo.clone()` instead.

```rust,ignore
// 0.20: `velo` is an `Arc<Velo>`
let velo = Velo::builder().add_transport(transport).build().await?;
let worker = Arc::clone(&velo);

// 0.21: `velo` is a Velo
let velo = Velo::builder().add_transport(transport).build().await?;
let worker = velo.clone();
```

`Arc<Messenger>` and the other `Arc` values that `Velo` hands out do not change.

## What does not change

- Clones share one instance and one set of handlers, streams, and rendezvous state.
- The final drop of the last clone cancels the streaming services. Dropping any other clone does not.
- `shutdown` and `graceful_shutdown` run once, even when you call them through different clones. A later call waits for the first and then returns.
- Wire formats and `velo-ext` traits do not change.
