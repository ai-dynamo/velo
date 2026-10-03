<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Migrate ownership, dispatch, and cancellation to 0.19

Velo 0.19 makes runtime ownership explicit and removes duplicate dispatch and cancellation APIs. Stream wire formats and `velo-ext` traits do not change.

## Runtime ownership

Keep a `Velo` owner for as long as its streams are needed. `Velo` values and `Arc<Velo>` can still be cloned. Dropping one clone does not stop streams. Dropping the final Velo owner cancels its streams and stops the builder-owned streaming listener and tasks, even if an `Arc<Messenger>` remains alive.

`Messenger` values no longer implement `Clone`. Clone `Arc<Messenger>` to share the instance:

```rust,ignore
let messages = Arc::clone(velo.messenger());
let streams = Arc::clone(&velo); // Keep this owner while streams are active.
```

A retained Messenger supports active messages after the final Velo owner is dropped; it does not keep Velo streaming services alive. Stream handles and handler contexts can also hold Messenger references. Final Messenger drop starts immediate transport teardown and wakes remote event waits. Local event completion remains available through retained event handles.

Drop requests cancellation. It does not drain accepted work or wait for tasks to exit. Continue to call `Velo::shutdown(policy).await` when those guarantees are needed. RDMA users must use explicit shutdown before relying on deregistration; Drop does not report registered memory as released. Discovery registration guards and custom frame transports remain the caller's responsibility.

## Handler dispatch

Replace `.inline()` with `.spawn()`, or omit the selector to use the default. The `DispatchMode` enum is removed. Select behavior with the builder methods: `.spawn()`, `.ordered()`, `.ordered_global()`, or `.ordered_with(config)`.

Both old modes ran one task for each message. Neither guaranteed message order. Use `.ordered()` or `.ordered_with(...)` when handlers need to run in arrival order.

Accepted handler calls still count toward graceful shutdown until the handler and its response send finish. `Messenger::tracker()` remains available.

## Stream cancellation

`SenderEntry::rx_closer` is removed. Code that constructs `SenderEntry` must omit this field. Code that dropped its receiver to cancel a sender must call `entry.cancel_token.cancel()` instead. If you remove an entry from `SenderRegistry` to cancel its sender, cancel the token before you drop the removed entry.

The normal `StreamController::cancel()` and `MpscStreamController::cancel()` APIs need no changes. SPSC and MPSC senders check their cancellation token before they send an item. A cancelled token also wakes a send that waits for channel space. Item and error sends return `SendError::ChannelClosed` after cancellation.

Graceful stop remains separate. For SPSC streams, `request_stop()` signals `stop_token()` and lets the producer send buffered output and call `finalize()`. Hard cancellation also signals the stop token, but rejects further item and error sends. Do not replace a graceful stop with `cancel_token.cancel()`.
