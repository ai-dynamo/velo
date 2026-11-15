<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Move off the manager accessors in 0.22.1

`Velo::anchor_manager()` and `Velo::rendezvous_manager()` are deprecated in 0.22.1. Each gives out a manager whose methods are now on `Velo`. A later breaking release removes both. Move each call to the `Velo` method in the tables below.

## Anchors

| Before | After |
|---|---|
| `velo.anchor_manager().create_anchor_with_config::<T>(config)` | `velo.create_anchor_with_config::<T>(config)` |
| `velo.anchor_manager().active_anchor_count()` | `velo.active_anchor_count()` |
| `velo.anchor_manager().renew_reservations(&reservations).await` | `velo.renew_reservations(&reservations).await` |
| `velo.anchor_manager().create_anchor::<T>()` | `velo.create_anchor::<T>()` |
| `velo.anchor_manager().attach_stream_anchor::<T>(handle).await` | `velo.attach_anchor::<T>(handle).await` |
| `velo.anchor_manager().prebind_anchor(handle)` | `velo.prebind_anchor(handle)` |
| `velo.anchor_manager().open_anchor_stream::<T>(handle, ticket).await` | `velo.open_anchor_stream::<T>(handle, ticket).await` |

`active_anchor_count` counts SPSC anchors only, as before.

## Rendezvous

| Before | After |
|---|---|
| `velo.rendezvous_manager().data_store()`, then `ref_decrement(id)` and `try_free(id)` | `velo.unregister_data(handle)` |
| `velo.rendezvous_manager().register_data_in_region(&guard, range)` | `velo.register_data_in_region(&guard, range)` |
| `rendezvous::transparent::DEFAULT_THRESHOLD` as the size above which a payload is staged | `velo.staging_threshold()` |
| `velo.rendezvous_manager().register_data(bytes)`, `get`, `detach`, `release`, `ref_handle`, `metadata` | the `Velo` method of the same name |

### The owner gives back a staged payload

`unregister_data` is the owner's call. It drops the one reference that `register_data` and the other `register_data_*` calls give the owner. A reader ends its lease with `release` or `detach`, as before.

- It is synchronous and needs no tokio runtime, so you can call it from `Drop`.
- If no reader holds a lease, it frees the slot at once. Otherwise the slot is freed when the last reader detaches or releases, or when its lease expires and is reaped. See [When a slot is freed](../concepts/rendezvous.md#when-a-slot-is-freed).
- It returns `true` only for the call that dropped the owner's reference. A second call does nothing and returns `false`. A handle that another node staged, or that this node no longer holds, also returns `false`.

The old sequence was `data_store()`, then `ref_decrement`, then `try_free` when `ref_decrement` returned `true`. A second run of it takes another reference, which can be a reader's. `unregister_data` cannot.

```rust,ignore
// 0.22.0
impl Drop for Staged {
    fn drop(&mut self) {
        let (_, id) = self.handle.unpack();
        let store = self.velo.rendezvous_manager().data_store();
        if store.ref_decrement(id) {
            store.try_free(id);
        }
    }
}

// 0.22.1
impl Drop for Staged {
    fn drop(&mut self) {
        self.velo.unregister_data(self.handle);
    }
}
```

For a slot staged with `register_data_in_region`, freeing the slot gives back its hold on the region. `RegionGuard::unregister` then does not wait for that slot.

### The staging threshold

`staging_threshold()` returns the threshold this node stages above: 256 KiB today, with no builder setting for it. A payload longer than it is staged through rendezvous. A payload of that length or shorter goes inline. Read it from the node instead of copying `DEFAULT_THRESHOLD`.

## Direct manager access

A few uses have no `Velo` form, such as the anchor manager's `transport_registry`. Keep them under `#[allow(deprecated)]` for now.
