<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Migrate dispatch and cancellation to 0.19

Velo 0.19 removes two APIs that duplicated existing behavior. Stream wire formats and `velo-ext` traits do not change.

## Handler dispatch

Replace `.inline()` with `.spawn()`, or omit the selector to use the default. Replace `DispatchMode::Inline` with `DispatchMode::Spawn` in code that names the enum variant.

Both old modes ran one task for each message. Neither guaranteed message order. Use `.ordered()` or `.ordered_with(...)` when handlers need to run in arrival order.

Accepted handler calls still count toward graceful shutdown until the handler and its response send finish. `Messenger::tracker()` remains available.

## Stream cancellation

`SenderEntry::rx_closer` is removed. Code that constructs `SenderEntry` must omit this field. Code that dropped its receiver to cancel a sender must call `entry.cancel_token.cancel()` instead. If you remove an entry from `SenderRegistry` to cancel its sender, cancel the token before you drop the removed entry.

The normal `StreamController::cancel()` and `MpscStreamController::cancel()` APIs need no changes. SPSC and MPSC senders check their cancellation token before they send an item. A cancelled token also wakes a send that waits for channel space. Item and error sends return `SendError::ChannelClosed` after cancellation.

Graceful stop remains separate. For SPSC streams, `request_stop()` signals `stop_token()` and lets the producer send buffered output and call `finalize()`. Hard cancellation also signals the stop token, but rejects further item and error sends. Do not replace a graceful stop with `cancel_token.cancel()`.
