<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Migrate to the Velo handle in 0.21

`Velo` is now a cheap handle. `VeloBuilder::build()` returns `Velo`. It used to return `Arc<Velo>`.

`Velo` is `Clone`. A clone adds one reference count and does not copy any state. Every clone is the same instance.

## How to migrate

- Replace `Arc<Velo>` with `Velo` in your types and signatures.
- Replace `Weak<Velo>` with `WeakVelo` in your types and signatures.
- Remove `velo.as_ref()` and `&*velo`. Pass `&velo` instead. Replace `velo.as_ref().clone()` with `velo.clone()`.
- Keep `.clone()` as it is. It now clones the handle.
- Change `Arc::clone(&velo)` to `velo.clone()`.
- Change `Arc::downgrade(&velo)` to `velo.downgrade()`. It returns a `WeakVelo`, and `WeakVelo::upgrade()` returns `Option<Velo>`. A handler that reaches its own instance must hold a `WeakVelo`. A `Velo` clone in the handler keeps the instance alive, so its final drop never runs.
- Change `Weak::new()` to `WeakVelo::default()`. It points at no instance, and `upgrade()` returns `None`.
- Change `Arc::ptr_eq(&a, &b)` to `a.instance_id() == b.instance_id()`. Clones of one instance share an `InstanceId`, and separate instances never do. `Velo` does not expose reference counts.
- The other `Arc` and `Weak` methods have no counterpart: `try_unwrap`, `into_inner`, `get_mut`, `make_mut`, `strong_count`, `weak_count`, and `Weak::ptr_eq`. For identity, compare `instance_id()`. To share, clone.
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
- `graceful_shutdown` runs its teardown once, even when you call it through different clones. A later call waits for the first and then returns, unless the first teardown failed. Then a later call panics at once.
- `shutdown` calls `graceful_shutdown`, then closes the streaming services. It does the streaming step again on each call.
- Wire formats and `velo-ext` traits do not change.
