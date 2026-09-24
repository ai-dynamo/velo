// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! System handler registration for distributed event messages.

use std::sync::Arc;

use crate::messenger::handlers::Handler;

use super::VeloEvents;

/// Register the three event system handlers.
///
/// These are fire-and-forget active messages — the handler return value has no
/// observable effect on the caller, so errors are intentionally logged and
/// discarded rather than propagated. For `_event_trigger_request`, ACK/NACK
/// semantics are managed internally via response channels in `handle_trigger_request`,
/// not through the handler's `Result`.
///
/// All three go through `register_drain_exempt`, because none of them starts
/// new work, and a refusal hangs work already accepted:
///
/// - `_event_trigger` completes an awaiter of work this node accepted.
/// - `_event_trigger_request` completes an event this node created, and acks
///   the requester. The request is sent fire-and-forget, so the refusal echo
///   finds no awaiter, and the requester's ack wait never ends.
/// - `_event_subscribe` answers at once for a completed event, or records one
///   subscriber for a pending one. The refusal echo finds no awaiter here
///   either, and the subscriber's pending mark stops it from sending again.
///
/// Each hang also keeps a `WaitForever` drain waiting, on whichever side holds
/// the stuck work.
pub(crate) fn register_event_handlers(
    register_drain_exempt: impl Fn(Handler) -> anyhow::Result<()>,
    events: Arc<VeloEvents>,
) -> anyhow::Result<()> {
    // _event_subscribe: Remote node subscribes to a local event
    let events_clone = events.clone();
    register_drain_exempt(
        Handler::am_handler_async("_event_subscribe", move |ctx| {
            let events = events_clone.clone();
            async move {
                if let Err(e) = events.handle_subscribe(ctx.payload).await {
                    tracing::warn!("_event_subscribe handler error: {}", e);
                }
                Ok(())
            }
        })
        .spawn()
        .build(),
    )?;

    // _event_trigger: Completion notification from remote owner to subscriber
    let events_clone = events.clone();
    register_drain_exempt(
        Handler::am_handler("_event_trigger", move |ctx| {
            if let Err(e) = events_clone.handle_trigger(ctx.payload) {
                tracing::warn!("_event_trigger handler error: {}", e);
            }
            Ok(())
        })
        .spawn()
        .build(),
    )?;

    // _event_trigger_request: Remote trigger/poison request + ACK/NACK
    let events_clone = events.clone();
    register_drain_exempt(
        Handler::am_handler_async("_event_trigger_request", move |ctx| {
            let events = events_clone.clone();
            async move {
                if let Err(e) = events.handle_trigger_request(ctx.payload).await {
                    tracing::warn!("_event_trigger_request handler error: {}", e);
                }
                Ok(())
            }
        })
        .spawn()
        .build(),
    )?;

    Ok(())
}
