// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streams that open without asking: the receiver mints the terms, the sender
//! opens on them, and no `_anchor_attach` crosses the wire.
//!
//! The suite next door asks what two nodes agree on when one of them asks.
//! These ask what happens when the answer arrives before the question, which is
//! the same negotiation with the round trip removed — so the interesting half
//! is again the mixture: an anchor holding a pre-bound slot still has to serve
//! a sender that attaches the ordinary way, a sender that opens twice, a sender
//! on the wrong transport, and a sender on its own worker.
//!
//! A submodule rather than a second test binary: every fixture here belongs to
//! `mod.rs` — the two-node pair, the metric readers, the ordering harness — and
//! `use super::*` shares them whole, the way `outcome` does.

use super::*;
use velo::streaming::AttachError;
use velo::streaming::control::{AnchorAttachRequest, StreamOpenTicket};

/// A ticket as the worker receives it: through the encoding, never by
/// reference. An application carries one in the request envelope it already
/// sends, so anything that failed to serialise would fail there and not here.
fn ship(ticket: StreamOpenTicket) -> StreamOpenTicket {
    rmp_serde::from_slice(&rmp_serde::to_vec(&ticket).expect("encode ticket"))
        .expect("decode ticket")
}

/// Runs of the `_anchor_attach` handler on this node, whatever they answered.
///
/// Every arm of that handler records one operation — success labelled with the
/// key it settled on, error labelled `unknown` — so summing them counts handler
/// runs rather than happy ones. Zero is the claim zero-RTT makes: not that the
/// attach succeeded cheaply, but that no attach happened.
fn attach_handler_runs(node: &Node) -> f64 {
    let snapshot = node.snapshot();
    let attaches = |outcome: &str, scheme: &str| {
        snapshot.counter(
            "velo_streaming_anchor_operations_total",
            &[
                ("operation", "attach"),
                ("outcome", outcome),
                ("transport_scheme", scheme),
            ],
        )
    };
    attaches("success", MUX_KEY) + attaches("success", LEGACY_KEY) + attaches("error", "unknown")
}

/// Records this node refused because no bind matched their `OpenSlot`.
fn unknown_slot_drops(node: &Node) -> f64 {
    node.snapshot().counter(
        "velo_streaming_mux_records_dropped_total",
        &[("reason", "unknown_slot")],
    )
}

/// Send an `_anchor_attach` by hand, advertising exactly `keys`.
///
/// The real attach path always advertises everything this node has installed,
/// which is the wrong instrument for asking what a *particular* advertisement
/// is answered with.
async fn attach_advertising(
    producer: &Node,
    consumer: &Node,
    handle: StreamAnchorHandle,
    keys: &[&str],
) -> AnchorAttachResponse {
    let request = AnchorAttachRequest {
        handle,
        session_id: 1,
        stream_cancel_handle: StreamCancelHandle::pack(producer.worker_id(), 1),
        supported_transport_keys: keys
            .iter()
            .map(|key| velo_ext::TransportKey::new(*key))
            .collect(),
    };
    producer
        .velo
        .messenger()
        .typed_unary_streaming::<AnchorAttachResponse>("_anchor_attach")
        .payload(&request)
        .expect("payload")
        .worker(consumer.worker_id())
        .send()
        .await
        .expect("attach round trip")
}

/// The five fields of an `Ok` response, as the terms a sender opens on.
fn terms(response: AnchorAttachResponse) -> StreamOpenTicket {
    match response {
        AnchorAttachResponse::Ok {
            streaming_transport_key,
            heartbeat_interval_ms,
            routing_session_id,
            initial_credit,
            slot_byte_budget,
        } => StreamOpenTicket {
            streaming_transport_key,
            heartbeat_interval_ms,
            routing_session_id,
            initial_credit,
            slot_byte_budget,
        },
        AnchorAttachResponse::Err { reason } => panic!("attach rejected: {reason}"),
    }
}

/// Drive `count` items and a `Finalized` through an already-open sender,
/// asserting each arrives exactly once and in order.
async fn drain_stream(
    mut anchor: velo::streaming::StreamAnchor<u32>,
    sender: velo::streaming::StreamSender<u32>,
    count: u32,
) {
    let send = tokio::spawn(async move {
        for n in 0..count {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    let collect = async {
        let mut items = Vec::with_capacity(count as usize);
        let mut terminals = 0;
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => {
                    terminals += 1;
                    break;
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        (items, terminals)
    };

    let (items, terminals) = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");
    assert_eq!(
        items,
        (0..count).collect::<Vec<_>>(),
        "frames lost, duplicated or reordered"
    );
    assert_eq!(
        terminals, 1,
        "exactly one terminal, from exactly one sender"
    );
}

// ---------------------------------------------------------------------------
// The zero-RTT path itself
// ---------------------------------------------------------------------------

/// A stream opens, runs and ends without an `_anchor_attach` ever being sent.
#[tokio::test(flavor = "multi_thread")]
async fn zero_rtt_opens_without_any_attach_am() {
    const FRAMES: u32 = 400;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = ship(
        consumer
            .velo
            .prebind_anchor(handle)
            .expect("both sides run the mux, so a ticket is minted"),
    );
    assert_eq!(
        ticket.streaming_transport_key.as_str(),
        MUX_KEY,
        "a ticket is only ever minted for the mux; nothing else is pre-bindable"
    );

    let sender = producer
        .velo
        .open_anchor_stream::<u32>(handle, ticket)
        .await
        .expect("zero-RTT open");
    drain_stream(anchor, sender, FRAMES).await;

    assert_eq!(
        attach_handler_runs(&consumer),
        0.0,
        "the whole stream ran without the attach handler being entered once"
    );
    // Far more frames than the 8-credit window, so the run is only possible if
    // credit came back through the pre-bind's own reader pump.
    assert!(consumer.mux_records_received() >= f64::from(FRAMES));
    consumer.assert_no_reader_stall();
    eventually(|| consumer.mux_live_slots() == 0.0).await;
    eventually(|| producer.mux_live_slots() == 0.0).await;
}

/// Several zero-RTT streams to one peer share the batch flow and lose no batch.
///
/// A gap in the per-peer batch sequence is the receiver noticing that something
/// it should have seen never arrived. Pre-binding moves *when* a slot is
/// registered, not what a batch carries, so the sequence must stay unbroken —
/// and per-slot order must hold across streams that share every batch.
#[tokio::test(flavor = "multi_thread")]
async fn zero_rtt_run_reports_no_batch_seq_gaps() {
    const STREAMS: u32 = 6;
    const FRAMES: u32 = 100;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchors = Vec::new();
    let mut senders = Vec::new();
    for _ in 0..STREAMS {
        let anchor = consumer.velo.create_anchor::<u32>();
        let handle = transfer(anchor.handle());
        let ticket = ship(consumer.velo.prebind_anchor(handle).expect("ticket"));
        senders.push(
            producer
                .velo
                .open_anchor_stream::<u32>(handle, ticket)
                .await
                .expect("zero-RTT open"),
        );
        anchors.push(anchor);
    }

    let send = tokio::spawn(async move {
        for n in 0..FRAMES {
            for sender in &senders {
                sender.send(n).await.expect("send item");
            }
        }
        for sender in senders {
            sender.finalize().expect("finalize");
        }
    });

    let collectors = anchors.into_iter().map(|mut anchor| async move {
        let mut items = Vec::with_capacity(FRAMES as usize);
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    });
    let collected = tokio::time::timeout(PATIENCE, futures::future::join_all(collectors))
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");

    for items in &collected {
        assert_eq!(items, &(0..FRAMES).collect::<Vec<_>>());
    }
    assert_eq!(attach_handler_runs(&consumer), 0.0);
    assert_eq!(
        consumer
            .snapshot()
            .counter("velo_streaming_mux_batch_seq_gaps_total", &[]),
        0.0,
        "the receiver saw a hole in the peer's batch sequence"
    );
    consumer.assert_no_reader_stall();
}

// ---------------------------------------------------------------------------
// A sender that attaches anyway
// ---------------------------------------------------------------------------

/// A worker that ignores its ticket and attaches is given the slot that is
/// already waiting for it, not a second one.
///
/// The response's `routing_session_id` is what says so: a fresh bind would have
/// allocated the next one, so answering with the ticket's own is only possible
/// if the handler adopted the pre-bind rather than binding again.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_worker_attach_adopts_the_prebind() {
    const FRAMES: u32 = 200;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = consumer.velo.prebind_anchor(handle).expect("ticket");

    let answered =
        terms(attach_advertising(&producer, &consumer, handle, &[MUX_KEY, LEGACY_KEY]).await);
    assert_eq!(
        answered.routing_session_id, ticket.routing_session_id,
        "an adopting attach must answer with the pre-bind's own session, not a fresh bind's"
    );
    assert_eq!(answered.streaming_transport_key.as_str(), MUX_KEY);
    assert_eq!(answered.initial_credit, ticket.initial_credit);
    assert_eq!(answered.slot_byte_budget, ticket.slot_byte_budget);
    assert_eq!(
        consumer.attaches_over(MUX_KEY),
        1.0,
        "adoption is a successful attach and is recorded as one"
    );

    // Open on what the attach answered — the same slot the pre-bind pumped, so
    // one pump feeds this stream and one terminal ends it.
    let sender = producer
        .velo
        .open_anchor_stream::<u32>(handle, answered)
        .await
        .expect("open on the adopted terms");
    drain_stream(anchor, sender, FRAMES).await;
    consumer.assert_no_reader_stall();
    eventually(|| consumer.mux_live_slots() == 0.0).await;
}

/// A second open on an adopted ticket gets no second slot.
///
/// A retried request envelope carries the same ticket, so the stray is the
/// shape a duplicate delivery takes. Its `OpenSlot` names a pair whose bind the
/// first one already consumed, which is the reverse race the receive path
/// already answers: refuse that slot, leave every other one alone.
#[tokio::test(flavor = "multi_thread")]
async fn adopt_then_stray_open_slot_opens_no_second_sender() {
    const FRAMES: u32 = 120;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = consumer.velo.prebind_anchor(handle).expect("ticket");
    let answered =
        terms(attach_advertising(&producer, &consumer, handle, &[MUX_KEY, LEGACY_KEY]).await);
    assert_eq!(
        answered.routing_session_id, ticket.routing_session_id,
        "the attach adopted the pre-bind, so both opens below name the same pair"
    );

    let sender = producer
        .velo
        .open_anchor_stream::<u32>(handle, answered.clone())
        .await
        .expect("open on the adopted terms");
    sender.send(0).await.expect("first item");
    let first = tokio::time::timeout(PATIENCE, anchor.next())
        .await
        .expect("timed out")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(matches!(first, StreamFrame::Item(0)));

    // The stray: same terms, second slot, and the bind is already gone.
    let stray = producer
        .velo
        .open_anchor_stream::<u32>(handle, answered)
        .await
        .expect("the stray opens an egress slot before the receiver refuses it");
    let strayed = tokio::spawn(async move {
        for n in 0..u32::MAX {
            if stray.send(n).await.is_err() {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        unreachable!("the stray must be refused");
    });

    let send = tokio::spawn(async move {
        for n in 1..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    let collect = async {
        let mut items = vec![0u32];
        let mut terminals = 0;
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => {
                    terminals += 1;
                    break;
                }
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        (items, terminals)
    };
    let (items, terminals) = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");
    tokio::time::timeout(PATIENCE, strayed)
        .await
        .expect("the stray was never refused")
        .expect("stray task");

    assert_eq!(
        items,
        (0..FRAMES).collect::<Vec<_>>(),
        "the stray must not have injected anything into the live stream"
    );
    assert_eq!(terminals, 1, "one sender, one terminal");
    assert!(
        unknown_slot_drops(&consumer) >= 1.0,
        "the stray's OpenSlot must have been refused for want of a bind"
    );
    eventually(|| consumer.mux_live_slots() == 0.0).await;
}

/// Once the pre-bound slot is open, a late attach is refused rather than bound.
///
/// The slot is running under terms the worker already holds. A fresh bind here
/// would give the anchor a second sender, and adopting would hand out terms
/// that are already in use; refusing is the only answer that leaves the live
/// stream alone, and this is what proves it stays alone.
#[tokio::test(flavor = "multi_thread")]
async fn open_slot_then_late_attach_does_not_rebind() {
    const FRAMES: u32 = 60;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = ship(consumer.velo.prebind_anchor(handle).expect("ticket"));
    let sender = producer
        .velo
        .open_anchor_stream::<u32>(handle, ticket)
        .await
        .expect("zero-RTT open");

    // One delivered record proves the OpenSlot has claimed the bind.
    sender.send(0).await.expect("first item");
    let first = tokio::time::timeout(PATIENCE, anchor.next())
        .await
        .expect("timed out")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(matches!(first, StreamFrame::Item(0)));

    match attach_advertising(&producer, &consumer, handle, &[MUX_KEY, LEGACY_KEY]).await {
        AnchorAttachResponse::Err { reason } => assert!(
            reason.contains("already streaming through a pre-bound slot"),
            "unhelpful refusal: {reason}"
        ),
        AnchorAttachResponse::Ok { .. } => {
            panic!("a claimed pre-bind must not be bound or adopted a second time")
        }
    }

    let send = tokio::spawn(async move {
        for n in 1..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });
    let collect = async {
        let mut items = vec![0u32];
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    send.await.expect("send task");
    assert_eq!(
        items,
        (0..FRAMES).collect::<Vec<_>>(),
        "the refused attach must have left the running stream untouched"
    );
}

/// A sender that cannot speak the pre-bound transport is refused, and the slot
/// it could not take is given straight back.
///
/// The release is the half worth pinning: without it the anchor would hold a
/// bind nobody can claim for the whole accept window, and the sender's own
/// retry — the reasonable thing for it to do — would find the anchor still
/// pre-bound. The second attach succeeding on a *different* session is what
/// says the pre-bind is gone rather than merely unused.
#[tokio::test(flavor = "multi_thread")]
async fn prebind_key_mismatch_refuses_and_releases() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let ticket = consumer.velo.prebind_anchor(handle).expect("ticket");

    match attach_advertising(&producer, &consumer, handle, &[LEGACY_KEY]).await {
        AnchorAttachResponse::Err { reason } => {
            assert!(
                reason.contains(MUX_KEY),
                "the refusal must name the key the sender would have had to speak: {reason}"
            );
        }
        AnchorAttachResponse::Ok { .. } => {
            panic!("a sender that cannot open {MUX_KEY} must not be handed a slot bound on it")
        }
    }

    let answered =
        terms(attach_advertising(&producer, &consumer, handle, &[MUX_KEY, LEGACY_KEY]).await);
    assert_ne!(
        answered.routing_session_id, ticket.routing_session_id,
        "the released pre-bind must be gone, so this attach binds afresh"
    );

    let sender = producer
        .velo
        .open_anchor_stream::<u32>(handle, answered)
        .await
        .expect("open on the freshly bound terms");
    drain_stream(anchor, sender, 50).await;
}

/// A same-worker attach releases the pre-bind rather than streaming beside it.
///
/// A co-located sender writes straight into the anchor's channel, so the slot
/// bound for a remote one has no sender and never will. Releasing it is
/// observable from the other node: the ticket it was minted for stops working,
/// which is exactly right — the anchor already has its producer.
#[tokio::test(flavor = "multi_thread")]
async fn co_located_local_attach_releases_the_prebind() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = anchor.handle();
    let ticket = ship(consumer.velo.prebind_anchor(handle).expect("ticket"));

    // Same worker: the local branch, which is where the release happens.
    let local = consumer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("co-located attach");
    local.send(7).await.expect("local send");
    let first = tokio::time::timeout(PATIENCE, anchor.next())
        .await
        .expect("timed out")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(matches!(first, StreamFrame::Item(7)));

    // The remote sender's ticket is now stale: its OpenSlot finds no bind.
    let stale = producer
        .velo
        .open_anchor_stream::<u32>(transfer(handle), ticket)
        .await
        .expect("the stale ticket opens an egress slot before the receiver refuses it");
    let refused = tokio::spawn(async move {
        for n in 0..u32::MAX {
            if stale.send(n).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        unreachable!("a released bind must refuse its ticket");
    });
    tokio::time::timeout(PATIENCE, refused)
        .await
        .expect("the stale ticket was never refused")
        .expect("stale task");

    // And the local stream is untouched by any of it.
    local.send(8).await.expect("local send");
    local.finalize().expect("finalize");
    let collect = async {
        let mut items = Vec::new();
        while let Some(frame) = anchor.next().await {
            match frame.expect("no stream error") {
                StreamFrame::Item(value) => items.push(value),
                StreamFrame::Finalized => break,
                other => panic!("unexpected frame: {other:?}"),
            }
        }
        items
    };
    let items = tokio::time::timeout(PATIENCE, collect)
        .await
        .expect("timed out collecting items");
    assert_eq!(items, vec![8], "the stale sender injected a frame");
}

/// A co-located attach onto a *claimed* pre-bind is refused, not handed a
/// second sender.
///
/// The release next door is the co-located case the design is for: nobody took
/// the slot, so giving it back costs the stream nothing. Once an `OpenSlot` has
/// claimed it the slot *has* a sender, and this attach is a second one — the
/// state the remote twin already answers with "already streaming through a
/// pre-bound slot". Admitting it here would be worse than a double attach: the
/// new sender writes into the same `frame_tx` the pre-bind's pump is feeding,
/// and dropping a claimed `PreBind` posts `CloseSlot{UnknownSlot}` to the
/// producer, so the live stream dies of the attach that should have been
/// refused.
///
/// The claim is taken at the consumer's ingress, when the `OpenSlot` arrives —
/// not when `open_anchor_stream` returns — so the attach must be attempted
/// behind a delivered record. Attempting it earlier tests the unclaimed case a
/// second time and says nothing about this one.
#[tokio::test(flavor = "multi_thread")]
async fn co_located_local_attach_is_refused_once_the_prebind_is_claimed() {
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = anchor.handle();
    let ticket = ship(consumer.velo.prebind_anchor(handle).expect("ticket"));

    let sender = producer
        .velo
        .open_anchor_stream::<u32>(transfer(handle), ticket)
        .await
        .expect("zero-RTT open");
    sender.send(0).await.expect("first item");
    let first = tokio::time::timeout(PATIENCE, anchor.next())
        .await
        .expect("timed out")
        .expect("stream ended early")
        .expect("no stream error");
    assert!(
        matches!(first, StreamFrame::Item(0)),
        "the delivered record is what says the bind is claimed"
    );

    match consumer.velo.attach_anchor::<u32>(handle).await {
        Err(AttachError::AlreadyAttached { .. }) => {}
        Err(other) => panic!("a claimed pre-bind must refuse as an attached anchor does: {other}"),
        Ok(_) => panic!("a second sender was handed out for a stream that is already running"),
    }

    // And the running stream is untouched by the refusal — neither torn down
    // by a released pre-bind nor interleaved with a second sender's frames.
    drain_stream(anchor, sender, 20).await;
}
