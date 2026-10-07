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

The handlers that `AnchorManager::register_handlers` installs no longer keep their manager alive, so keep your own `Arc<AnchorManager>` for as long as you use it. The manager still keeps its Messenger alive: drop the manager too before you expect final Messenger drop to start teardown. `RendezvousManager::register_handlers` no longer keeps the Messenger alive. The public handler factories `streaming::control::create_anchor_{attach,detach,finalize,cancel}_handler` and `streaming::mpsc::create_mpsc_anchor_{attach,detach,cancel}_handler` are removed; call `AnchorManager::register_handlers` instead. A manager whose handlers owned it formed a cycle with its Messenger, so neither was ever dropped.

A retained Messenger supports active messages after the final Velo owner is dropped; it does not keep Velo streaming services alive. Stream handles and handler contexts can also hold Messenger references. Final Messenger drop starts immediate transport teardown and wakes remote event waits. It does not complete a response wait made through that Messenger: such a wait ends at the caller's own deadline. Velo's internal tasks that wait on a response end at teardown. Local event completion remains available through retained event handles.

Drop requests cancellation and starts transport cleanup on an owned thread. It does not drain accepted work or wait for tasks or native threads to exit. A failed `VeloBuilder::build` also cleans up, in one of two ways. If a transport fails to start, the build stops the transports that already started on the building task, and waits for them to close before it returns the error, as step 3 of [Shutdown and drain](../concepts/shutdown.md) says. If the build fails after its transports started, dropping what it built cleans up as Drop does, so a listener port can stay bound for a short time after the error returns. Explicit shutdown runs the same transport cleanup, and waits for it before it waits for transport close. Continue to call `Velo::shutdown(policy).await` when those guarantees are needed. RDMA users must use explicit shutdown before relying on deregistration; Drop does not report registered memory as released. Discovery registration guards and custom frame transports remain the caller's responsibility.

## Handler dispatch

Replace `.inline()` with `.spawn()`, or omit the selector to use the default. The `DispatchMode` enum is removed. Select behavior with the builder methods: `.spawn()`, `.ordered()`, `.ordered_global()`, or `.ordered_with(config)`.

Both old modes ran one task for each message. Neither guaranteed message order. Use `.ordered()` or `.ordered_with(...)` when handlers need to run in arrival order.

Accepted handler calls still count toward graceful shutdown until the handler and its response send finish. `Messenger::tracker()` remains available.

## Stream cancellation

`SenderEntry::rx_closer` is removed, and `SenderEntry` has a new `closed` field. Code that constructs `SenderEntry` must omit `rx_closer` and set `closed: Default::default()`. Code that dropped its receiver to cancel a sender must call `entry.cancel()` instead. If you remove an entry from `SenderRegistry` to cancel its sender, call `cancel()` on it before you drop it.

The normal `StreamController::cancel()` and `MpscStreamController::cancel()` APIs need no changes. SPSC and MPSC senders check a cancelled flag before they send an item. `SenderEntry::cancel` sets the flag first, so the next send fails. Checking the token itself takes a lock on every item: 8.7 ns, against 0.3 ns for the flag. A token cancelled in another way sets the flag when the sender's heartbeat task next runs, so a send made before that can still go out. That task runs on the Tokio runtime that built the sender: if that runtime is gone, a direct token cancel never reaches `send`. Cancel through the stream controller or `SenderEntry::cancel` instead. A cancelled token also wakes a send that waits for channel space. Item and error sends return `SendError::ChannelClosed` after cancellation.

Graceful stop remains separate. For SPSC streams, `request_stop()` signals `stop_token()` and lets the producer send buffered output and call `finalize()`. Hard cancellation also signals the stop token, but rejects further item and error sends. Do not replace a graceful stop with `cancel_token.cancel()`.
