// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active message server.

pub(crate) mod dispatcher;
pub(crate) mod lanes;
pub(crate) mod system_handlers;

pub(crate) use system_handlers::register_system_handlers;

use crate::messenger::common::{
    events::{EventType, Outcome, decode_event_header},
    messages::{decode_active_message, decode_response_id_from_request_header},
    responses::{ResponseManager, decode_response_header},
};

use std::sync::Arc;

use crate::observability::{DispatchFailure, VeloMetrics};
use crate::transports::{DataStreams, InboundMessage, ShutdownState, VeloBackend};
use bytes::Bytes;
use tokio_util::task::TaskTracker;

use dispatcher::send_error_reply;
pub(crate) use dispatcher::{DispatcherHub, HandlerContext};

pub(crate) struct ActiveMessageServer {
    tracker: TaskTracker,
    hub: Arc<DispatcherHub>,
}

impl ActiveMessageServer {
    pub async fn new(
        response_manager: ResponseManager,
        data_streams: DataStreams,
        backend: Arc<VeloBackend>,
        public_tracker: TaskTracker,
        observability: Option<Arc<VeloMetrics>>,
        large_payload_resolver: Arc<
            std::sync::OnceLock<Arc<dyn crate::messenger::large_payload::LargePayloadResolver>>,
        >,
    ) -> Self {
        let shutdown_state = data_streams.shutdown_state.clone();
        let teardown = shutdown_state.teardown_token().clone();
        let tracker = TaskTracker::new();
        let (message_rx, response_rx, event_rx, shutdown_rx) = data_streams.into_parts();

        // Create dispatcher hub (shareable)
        let hub = Arc::new(DispatcherHub::new(backend.clone()));

        // Spawn message handler with direct dispatch (hot path)
        // Public waits include receive loops. Internal shutdown joins only
        // these loops, so application tasks cannot extend its timeout.
        tracker.spawn(public_tracker.track_future(create_message_handler(
            message_rx,
            hub.clone(),
            observability.clone(),
            large_payload_resolver,
            shutdown_state,
        )));

        tracker.spawn(
            public_tracker.track_future(teardown.clone().run_until_cancelled_owned(
                create_response_handler(response_manager.clone(), response_rx),
            )),
        );
        tracker.spawn(
            public_tracker.track_future(teardown.clone().run_until_cancelled_owned(
                create_ack_and_event_handler(response_manager.clone(), event_rx),
            )),
        );
        tracker.spawn(
            public_tracker.track_future(
                teardown.run_until_cancelled_owned(create_shutdown_handler(
                    response_manager,
                    shutdown_rx,
                )),
            ),
        );
        Self { tracker, hub }
    }

    pub(crate) async fn closed(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }

    /// Get a reference to the dispatcher hub
    pub(crate) fn hub(&self) -> &Arc<DispatcherHub> {
        &self.hub
    }
}

/// Message handler task - receives messages from backend and dispatches to handlers
/// This is the HOT PATH - optimized for low latency with direct dispatch
async fn create_message_handler(
    message_rx: flume::Receiver<InboundMessage>,
    hub: Arc<DispatcherHub>,
    observability: Option<Arc<VeloMetrics>>,
    large_payload_resolver: Arc<
        std::sync::OnceLock<Arc<dyn crate::messenger::large_payload::LargePayloadResolver>>,
    >,
    shutdown_state: ShutdownState,
) -> anyhow::Result<()> {
    // Wait for system initialization before processing messages
    tokio::select! {
        _ = hub.wait_for_system() => {},
        _ = shutdown_state.teardown_token().cancelled() => {},
    }

    // Phase 3 of graceful shutdown. `VeloBackend::graceful_shutdown` is the
    // only path that cancels this token *behind a drain*, so under
    // `WaitForever` it can only fire once the queue is provably empty (every
    // queued message holds a guard, so `in_flight == 0` implies nothing is
    // queued). Under `Timeout` it is what stops leftover queued work from
    // dispatching into an instance that has already declared itself dead.
    //
    // It is not the only caller: the TCP, UDS, QUIC, gRPC, and UCX
    // `Transport::shutdown` impls cancel this same shared token (ZMQ, NATS,
    // and the simulation transport tear down only their own private
    // machinery), so a direct `shutdown()` on one of those five on a live
    // instance ends this task — backlog abandoned, guards released, every
    // later admission answered `Disconnected` — for *all* transports, with no
    // drain and no `ShuttingDown` correlations. That is documented on
    // `Transport::shutdown` as the reason not to call it by hand.
    //
    // The check that makes it structural is the `is_cancelled()` *after* the
    // dequeue, not the select's arm order. A task woken with both a message
    // and a cancelled token pending re-polls the select from the top, so
    // whichever arm is `biased` first decides — and a check placed before the
    // park is stale by the time the wake arrives. Testing the token on the
    // message's own path to dispatch is the only placement that cannot be
    // outraced by the wake ordering; the remaining window is the instructions
    // between the check and `dispatch_message`, which is as narrow as
    // cancellation-versus-dispatch can be made.
    // `graceful_shutdown_timeout_drops_queued_work` in
    // `lib/velo/tests/drain_rejection.rs` pins it: delete the branch below
    // and the test dispatches 3 messages where it asserts 1.
    //
    // It is the *cheapest correct* placement, not a cheap one.
    // `CancellationToken::is_cancelled()` is not an atomic load — it locks the
    // token's `TreeNode` (a `Mutex<Inner>`; tokio-util 0.7.18/0.7.19 have no
    // atomic fast path in that module), so this is one uncontended mutex
    // acquire per inbound message. Measured on aarch64 (Cortex-X925, ~4 GHz,
    // pinned): ~8 ns standalone, ~6 ns marginal once the loop body carries
    // realistic work — a bigger body gives the core more to overlap the
    // lock's latency with. That is ~6% of this loop's body, and at the
    // 0.56-0.77M msg/s the fastest in-tree transport (uds) delivers
    // (pipeline mode; run-to-run spread), the whole dispatch task is 6-9%
    // of one core — so the check is ~0.4-0.5% of one core.
    // Re-measure after a tokio-util bump; revisit the design only if this
    // task ever sustains ~2M msgs/s (~20% of a core). Below that it is noise.
    //
    // Neither alternative is takeable, for different reasons. Putting the
    // teardown arm first is simply dearer — ~20 ns/message, because polling
    // a `cancelled()` future takes *two* uncontended mutex round-trips: the
    // `is_cancelled` above, plus the `Notify` waiters lock that re-polling
    // the inner `Notified` takes unconditionally once it is in the waiting
    // state. So the pinned future is polled only when the queue is empty and
    // this task is about to park; pinning still earns its keep there, since
    // rebuilding it per park would register — and on drop deregister — a
    // waker every time.
    //
    // A mirrored `AtomicBool` on `ShutdownState` is the opposite case: it
    // measures genuinely free (0.0 ns marginal — it rides the cache line
    // `in_flight` already pulls in, whereas the token's `TreeNode` is a
    // separate allocation nothing else on this path touches), and it is
    // still wrong. `teardown_token()` is `pub` in velo-ext and out-of-tree
    // transports are documented to cancel it directly, so the mirror would
    // read false for exactly those callers and dispatch the backlog anyway —
    // under `Timeout` only, with a non-empty queue only, in code no test here
    // can see. Reintroducing this bug silently is not worth 8 ns.
    let teardown = shutdown_state.teardown_token().clone();
    let mut teardown_fut = std::pin::pin!(teardown.cancelled());

    // Counts messages taken off the queue but deliberately not dispatched,
    // for the teardown log below.
    let mut abandoned = 0usize;

    // Resolved once, outside the loop. This task is the node's single consumer
    // of `message_rx`, so every inbound message pays whatever is in here; a
    // per-message label lookup would be a real cost on the hot path, and there
    // is nothing to look up.
    let inbound_dequeued = observability
        .as_ref()
        .map(|metrics| metrics.bind_inbound_dequeued());

    let mut held = BurstRef::default();

    loop {
        let inbound = match message_rx.try_recv() {
            Ok(inbound) => inbound,
            Err(flume::TryRecvError::Disconnected) => break,
            Err(flume::TryRecvError::Empty) => {
                held.release();
                tokio::select! {
                    biased;
                    received = message_rx.recv_async() => match received {
                        Ok(inbound) => inbound,
                        Err(_) => break,
                    },
                    _ = teardown_fut.as_mut() => break,
                }
            }
        };

        if teardown.is_cancelled() {
            // Teardown won: this message was queued before the instance
            // declared itself dead, so it is abandoned rather than dispatched.
            // Dropping it releases the in-flight guard it carried.
            drop(inbound);
            abandoned += 1;
            break;
        }

        // Counted between the abandonment check above and the decode below,
        // which is the only placement that keeps
        // `frames_total{inbound,message,accepted} - this` equal to the queue's
        // depth for as long as this instance is live: a message dropped at
        // teardown never became work, and a message that fails to decode has
        // still left the queue.
        //
        // Teardown ends the identity rather than preserving it, and that is
        // deliberate. The message abandoned on the cancelled token above, and
        // everything the post-loop sweep drops, are admitted frames that never
        // count as departures — so once the loop exits the derived depth reads
        // high by the abandoned count and stays there. The alternative,
        // counting them, would make a live queue's depth read low by however
        // much a *previous* teardown discarded, which is the reading that
        // matters.
        //
        // One writer today — this task is spawned once per instance. Sharding
        // the decode loop would make it many; the counter itself is atomic so
        // that stays correct, but it is worth knowing the assumption is being
        // broken.
        if let Some(dequeued) = &inbound_dequeued {
            dequeued.inc();
        }
        held.tick();

        // The guard was acquired by the transport at admission
        // (`TransportAdapter::admit_message`) and travelled with the frame, so
        // this message has been counted work since before it was queued.
        // Taking ownership of it *before* decoding means a decode failure
        // releases it on the spot instead of leaking it.
        let InboundMessage {
            header,
            payload,
            guard,
            ..
        } = inbound;
        // Held until the handler future (response send included) completes, so
        // phase 2's wait_for_drain covers accepted-but-unfinished work.
        let in_flight = Some(Arc::new(guard));

        match decode_active_message(header, payload) {
            Ok(message) => {
                let Some(system) = held.get(|| hub.system()) else {
                    break;
                };
                #[cfg(feature = "distributed-tracing")]
                let span = {
                    let span = tracing::info_span!(
                        "velo.messenger.server_receive",
                        handler = %message.metadata.handler_name,
                        response_type = ?message.metadata.response_type,
                        request_bytes = message.payload.len()
                    );
                    crate::observability::apply_remote_parent(
                        &span,
                        message.metadata.headers.as_ref(),
                    );
                    span
                };

                #[cfg(feature = "distributed-tracing")]
                let _entered = span.enter();

                tracing::debug!(
                    target: "crate::messenger::server",
                    handler = %message.metadata.handler_name,
                    "Received active message"
                );

                // Check for transparent rendezvous header: if present, spawn
                // an async task to resolve the payload before dispatching.
                // This keeps the hot path fast for normal (non-rendezvous) messages.
                let rv_handle_str = message.metadata.headers.as_ref().and_then(|h| {
                    h.get(crate::messenger::large_payload::RV_HEADER_KEY)
                        .cloned()
                });

                if let Some(handle_str) = rv_handle_str {
                    if let Some(resolver) = large_payload_resolver.get() {
                        let resolver = Arc::clone(resolver);
                        let hub = hub.clone();
                        let handler_name = message.metadata.handler_name.clone();
                        let message_id = message.metadata.response_id;
                        let response_type = message.metadata.response_type;
                        let headers = message.metadata.headers.clone();
                        // The resolve waits on the payload's owner, so the
                        // task must not hold the Messenger across it: that
                        // would keep its final drop, and teardown, from ever
                        // happening. The hub upgrades it again afterwards.
                        let backend = Arc::clone(system.backend());
                        drop(system);
                        tokio::spawn(async move {
                            match resolver.resolve(&handle_str).await {
                                Ok(resolved_payload) => {
                                    // Gone: the instance is being torn down,
                                    // and the message dies with its guard.
                                    let Some(system) = hub.system() else {
                                        return;
                                    };
                                    let ctx = HandlerContext {
                                        message_id,
                                        payload: resolved_payload,
                                        response_type,
                                        headers,
                                        system,
                                        in_flight,
                                    };
                                    hub.dispatch_message(&handler_name, ctx);
                                }
                                Err(e) => {
                                    tracing::error!(
                                        target: "crate::messenger::server",
                                        handler = %handler_name,
                                        "Failed to resolve large payload: {e}"
                                    );
                                    if matches!(
                                        response_type,
                                        crate::messenger::common::messages::ResponseType::AckNack
                                            | crate::messenger::common::messages::ResponseType::Unary
                                    ) {
                                        send_error_reply(
                                            &backend,
                                            &handler_name,
                                            message_id,
                                            format!("Failed to resolve large payload: {e}"),
                                        )
                                        .await;
                                    }
                                }
                            }
                        });
                        continue;
                    } else {
                        // No resolver installed — cannot process rendezvous payload
                        tracing::error!(
                            target: "crate::messenger::server",
                            handler = %message.metadata.handler_name,
                            "Received rendezvous message but no resolver installed"
                        );
                        if matches!(
                            message.metadata.response_type,
                            crate::messenger::common::messages::ResponseType::AckNack
                                | crate::messenger::common::messages::ResponseType::Unary
                        ) {
                            let backend = system.backend().clone();
                            let handler_name = message.metadata.handler_name.clone();
                            let message_id = message.metadata.response_id;
                            tokio::spawn(async move {
                                let _in_flight = in_flight;
                                send_error_reply(
                                    &backend,
                                    &handler_name,
                                    message_id,
                                    "Rendezvous resolver not configured on receiver".to_string(),
                                )
                                .await;
                            });
                        }
                        continue;
                    }
                }

                let ctx = HandlerContext {
                    message_id: message.metadata.response_id,
                    payload: message.payload,
                    response_type: message.metadata.response_type,
                    headers: message.metadata.headers,
                    system,
                    in_flight,
                };

                // Direct dispatch - inline, no channel hop!
                hub.dispatch_message(&message.metadata.handler_name, ctx);
            }
            Err(e) => {
                if let Some(metrics) = observability.as_ref() {
                    metrics.record_dispatch_failure(DispatchFailure::DecodeActiveMessage);
                }
                tracing::error!(target: "crate::messenger::server", "Failed to decode active message: {}", e);
            }
        }
    }

    // Teardown reached with work still on the queue — only possible under
    // `ShutdownPolicy::Timeout`, since `WaitForever` cannot cancel the token
    // until the queue is empty. Abandoning those messages is what the timeout
    // buys; abandoning their in-flight guards is not. flume frees a buffered
    // item only once *both* ends of the channel are gone, and every transport
    // holds a sender clone for the instance's lifetime, so guards left parked
    // in the buffer would pin `in_flight` above zero forever and wedge any
    // later or concurrent `wait_for_drain`.
    //
    // The sweep only has to cover what is already buffered. Anything a
    // producer admits after `message_rx` drops at the end of this function
    // releases its own guard: `admit_message` drops the guard on `SendError`
    // and reports `Disconnected` to the transport.
    while let Ok(queued) = message_rx.try_recv() {
        drop(queued);
        abandoned += 1;
    }
    if abandoned > 0 {
        tracing::warn!(
            target: "crate::messenger::server",
            abandoned,
            "Dropped inbound messages still queued at teardown; their senders \
             were admitted, so they get no ShuttingDown reply and wait out \
             their own response timeout"
        );
    }

    Ok(())
}

/// Creates a task that handles responses from the response channel.
async fn create_response_handler(
    response_manager: ResponseManager,
    response_rx: flume::Receiver<(Bytes, Bytes)>,
) -> anyhow::Result<()> {
    while let Ok((header, payload)) = response_rx.recv_async().await {
        match decode_response_header(header) {
            Ok((response_id, outcome, _headers)) => match outcome {
                Outcome::Ok => {
                    response_manager.complete_outcome(response_id, Ok(Some(payload)));
                }
                Outcome::Error => {
                    let error_message =
                        String::from_utf8(payload.to_vec()).unwrap_or("unknown error".to_string());
                    response_manager.complete_outcome(response_id, Err(error_message));
                }
            },
            Err(e) => {
                tracing::error!(target: "crate::messenger::server", "Failed to decode response header: {}", e);
            }
        }
    }
    Ok(())
}

/// Creates a task that handles drain rejections from the shutdown channel.
///
/// A peer that rejects a request during its drain echoes the *request*
/// header back in a `ShuttingDown` frame; the transport delivers it on
/// `DataStreams::shutdown_stream`. The awaiter keyed by that header's
/// response id is failed immediately instead of hanging until its own
/// timeout. Fire-and-forget ids have no awaiter and miss the arena, which
/// `complete_outcome` tolerates.
async fn create_shutdown_handler(
    response_manager: ResponseManager,
    shutdown_rx: flume::Receiver<(Bytes, Bytes)>,
) -> anyhow::Result<()> {
    while let Ok((header, _payload)) = shutdown_rx.recv_async().await {
        match decode_response_id_from_request_header(&header) {
            Some(response_id) => {
                tracing::debug!(
                    target: "crate::messenger::server",
                    response_id = %response_id,
                    "Completing awaiter for drain-rejected request"
                );
                response_manager.complete_outcome(
                    response_id,
                    Err("request rejected: peer is shutting down".to_string()),
                );
            }
            None => {
                tracing::warn!(
                    target: "crate::messenger::server",
                    header_len = header.len(),
                    "ShuttingDown frame did not carry a request-format header"
                );
            }
        }
    }
    Ok(())
}

/// Creates a task that handles events and acks from the event channel.
async fn create_ack_and_event_handler(
    response_manager: ResponseManager,
    event_rx: flume::Receiver<(Bytes, Bytes)>,
) -> anyhow::Result<()> {
    while let Ok((header, payload)) = event_rx.recv_async().await {
        let event_type = decode_event_header(header);
        match event_type {
            Some(EventType::Ack(response_id, Outcome::Ok)) => {
                response_manager.complete_outcome(response_id, Ok(Some(payload)));
            }
            Some(EventType::Ack(response_id, Outcome::Error)) => {
                let error_message =
                    String::from_utf8(payload.to_vec()).unwrap_or("unknown error".to_string());
                response_manager.complete_outcome(response_id, Err(error_message));
            }
            Some(EventType::Event(raw_handle, _)) => {
                tracing::warn!(
                    target: "crate::messenger::server",
                    raw_handle,
                    "Received unsupported event frame; distributed events use active messages"
                );
            }
            None => {}
        }
    }
    Ok(())
}

/// A strong reference to a weakly held value, kept across a burst of work.
///
/// The hub holds the messenger weakly, so its final drop can start teardown.
/// Upgrading per message costs a CAS loop on a count that handler tasks on
/// other threads keep moving: measured 2-4x a clone under contention. So the
/// receive loop upgrades once per burst and clones per message, as a strong
/// reference would. When the queue drains to one message per park, each park
/// costs a drop, an upgrade, and a clone where a strong reference cost one
/// clone; the park and wake cost far more.
///
/// Two bounds keep the held reference from extending the value's life: the
/// loop calls [`release`](Self::release) before it parks, and releases again
/// every [`BURST_REFRESH`] dequeues ([`tick`](Self::tick)). Without the
/// second, a peer that keeps the queue from ever draining would keep the
/// messenger alive after its owner dropped it, and teardown would never
/// start. The bound counts dequeues, not uses, so frames that fail to decode
/// count too.
struct BurstRef<T> {
    held: Option<Arc<T>>,
    dequeues: u32,
}

/// Re-upgrade after this many messages. Adds well under 1 ns per message.
const BURST_REFRESH: u32 = 128;

impl<T> Default for BurstRef<T> {
    fn default() -> Self {
        Self {
            held: None,
            dequeues: 0,
        }
    }
}

impl<T> BurstRef<T> {
    /// A strong reference, or `None` once `upgrade` finds the value gone.
    fn get(&mut self, upgrade: impl FnOnce() -> Option<Arc<T>>) -> Option<Arc<T>> {
        if self.held.is_none() {
            self.held = Some(upgrade()?);
        }
        self.held.clone()
    }

    /// Count one dequeue; release the reference every [`BURST_REFRESH`].
    fn tick(&mut self) {
        self.dequeues += 1;
        if self.dequeues > BURST_REFRESH {
            self.release();
        }
    }

    fn release(&mut self) {
        self.held = None;
        self.dequeues = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messenger::common::events::{EventType, Outcome, encode_event_header};
    use tokio::time::{Duration, timeout};

    /// A queue that never drains must not keep the messenger alive: once its
    /// owner drops it, the loop must see it gone within one refresh, so that
    /// final drop starts teardown even under a flood.
    #[test]
    fn a_burst_that_never_ends_still_releases_the_value() {
        let owner = Arc::new(7u32);
        let weak = Arc::downgrade(&owner);
        let mut held = BurstRef::default();
        held.tick();
        assert!(held.get(|| weak.upgrade()).is_some());
        drop(owner);
        let seen = (0..=BURST_REFRESH)
            .map_while(|_| {
                held.tick();
                held.get(|| weak.upgrade())
            })
            .count();
        assert!(seen < BURST_REFRESH as usize, "held past one refresh");
        assert!(weak.upgrade().is_none());
    }

    /// The bound counts dequeues, not uses: a flood of frames that fail to
    /// decode never asks for the messenger, and must still let it go.
    #[test]
    fn dequeues_that_never_use_the_value_still_release_it() {
        let owner = Arc::new(7u32);
        let weak = Arc::downgrade(&owner);
        let mut held = BurstRef::default();
        held.tick();
        assert!(held.get(|| weak.upgrade()).is_some());
        drop(owner);
        for _ in 0..=BURST_REFRESH {
            held.tick();
        }
        assert!(weak.upgrade().is_none(), "held past one refresh");
    }

    #[tokio::test]
    async fn ack_ok_completes_response() -> anyhow::Result<()> {
        let worker_id = 7;
        let response_manager = ResponseManager::new(worker_id);
        let (tx, rx) = flume::bounded(1);

        let handler = tokio::spawn(create_ack_and_event_handler(response_manager.clone(), rx));

        let mut awaiter = response_manager.register_outcome()?;
        let response_id = awaiter.response_id();

        let header = encode_event_header(EventType::Ack(response_id, Outcome::Ok));
        tx.send((header, Bytes::from("ok"))).expect("send frame");
        drop(tx);

        let result = timeout(Duration::from_millis(200), awaiter.recv()).await;
        let outcome = result.expect("timed out waiting for ack");
        assert_eq!(outcome.unwrap(), Some(Bytes::from("ok")));

        handler.await??;
        Ok(())
    }

    /// Drain rejections echo the request header, not a response-format header.
    #[tokio::test]
    async fn drain_rejection_echo_completes_awaiter() -> anyhow::Result<()> {
        use crate::messenger::common::messages::{ActiveMessage, MessageMetadata};

        let response_manager = ResponseManager::new(7);
        let (tx, rx) = flume::bounded(1);
        let handler = tokio::spawn(create_shutdown_handler(response_manager.clone(), rx));

        let mut awaiter = response_manager.register_outcome()?;
        let response_id = awaiter.response_id();

        // What the peer's listener echoes back: the request header, verbatim.
        let (header, _payload, _mt) = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, "some_handler".to_string(), None),
            payload: Bytes::new(),
        }
        .encode()?;
        tx.send((header, Bytes::new())).expect("send frame");
        drop(tx);

        let err = timeout(Duration::from_secs(1), awaiter.recv())
            .await
            .expect("awaiter must complete promptly")
            .expect_err("drain rejection must surface as an error");
        assert!(err.contains("shutting down"), "unexpected error: {err}");

        handler.await??;
        Ok(())
    }
}
