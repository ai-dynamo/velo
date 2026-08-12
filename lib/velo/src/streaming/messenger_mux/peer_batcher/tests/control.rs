// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Coalesced control, epoch death, and teardown.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::observability::VeloMetrics;
use crate::streaming::messenger_mux::protocol::RecordType;
use crate::streaming::sender::cached_finalized;

// ---------------------------------------------------------------------------
// Control is state, not a queue
// ---------------------------------------------------------------------------

/// A batcher stalled on admission must not grow with what arrives behind it.
///
/// The stall is not exotic: a flush parks whenever the peer is congested, and a
/// congested peer is exactly when its ingress lane is busiest returning credit.
/// An unbounded control queue in that window is unbounded memory. Coalesced
/// state is O(live slots) whatever the arrival rate, and the deltas it merged
/// still deliver once the peer un-parks.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_batcher_coalesces_control_instead_of_queueing_it() {
    let (transport, wire) = StallingTransport::new(tokio::runtime::Handle::current());
    let sender = Messenger::builder()
        .add_transport(transport)
        .build()
        .await
        .expect("sender messenger");
    // A peer id the transport accepts. Nothing ever reads the far end; the
    // gate is the only thing under test.
    let peer_instance = velo_ext::InstanceId::new_v4();
    sender
        .register_peer(velo_ext::PeerInfo::new(
            peer_instance,
            velo_ext::WorkerAddress::from_encoded(
                rmp_serde::to_vec(&std::collections::HashMap::from([(
                    "stalling".to_string(),
                    b"stalling".to_vec(),
                )]))
                .expect("encode"),
            ),
        ))
        .expect("register peer");

    let registry = prometheus::Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let cancel = CancellationToken::new();
    let handle = spawn(
        peer_instance.worker_id(),
        BatcherContext {
            messenger: Arc::clone(&sender),
            config: MuxConfig::default(),
            metrics: Some(metrics.bind_mux()),
            epochs: Arc::new(AtomicU64::new(1)),
            batchers: Arc::new(DashMap::new()),
            cancel: cancel.clone(),
        },
    );

    // Open a slot. Its eager `OpenSlot` flush takes the gate's one free place.
    //
    // The inlet is deep because a batcher parked on admission is not draining
    // it — admission parking suspends the whole task, including the inlet
    // drain that credit starvation cannot suspend. That park is bounded by the
    // transport's own progress, which is the situation a socket was always in;
    // credit starvation is the unbounded one, and that is the one the withheld
    // queue exists for.
    let (inlet, inlet_rx) = flume::bounded::<Vec<u8>>(512);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    handle
        .open_slot(OpenSlotRequest {
            anchor_id: 1,
            session_id: 1,
            inlet: inlet_rx,
            credit: SlotCredit::new(0),
            slot_byte_budget: MuxConfig::default().slot_byte_budget,
            ack: ack_tx,
        })
        .await
        .expect("queue OpenSlot");
    let opened = tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("ack")
        .expect("ack delivered");
    assert!(opened.is_ok());

    let open_batch = OwnedBatch::decode(&{
        let (_, payload) = tokio::time::timeout(RECV_TIMEOUT, wire.recv_async())
            .await
            .expect("the OpenSlot flush must reach the wire")
            .expect("wire open");
        payload
    });
    let id = open_batch.records[0].slot;

    // Fill the gate's one place and leave it filled. Nothing drains `wire`
    // until the release phase, so every flush after this one parks.
    handle.grant(id, 1);
    inlet.send(item(0)).expect("queue record");
    eventually(|| wire.is_full()).await;

    // A control write the batcher can act on, so the flush it triggers is the
    // one that parks. After this the batcher is inside `flush().await` and
    // cannot take anything else off the control state.
    handle.reply(&[ReplyRecord::CloseSlot {
        slot: SlotId::from_raw(u32::MAX),
        reason: CloseReason::UnknownSlot,
    }]);
    let batches = |registry: &prometheus::Registry| {
        crate::observability::test_helpers::MetricSnapshot::from_registry(registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    };
    eventually(|| batches(&registry) >= 3.0).await;
    let parked_at = batches(&registry);

    // More records than the one credit already granted, so the merged grant is
    // what decides whether they flow.
    const QUEUED: u32 = 100;
    for n in 1..QUEUED {
        inlet.send(item(n)).expect("queue record");
    }

    // Ten thousand grants and ten thousand replies while it is stuck there.
    const MERGED: u32 = 10_000;
    let mut peak_pending = 0;
    for _ in 0..MERGED {
        handle.grant(id, 1);
        handle.reply(&[ReplyRecord::CreditUpdate { slot: id, delta: 1 }]);
        peak_pending = peak_pending.max(handle.pending_control());
    }
    // The stall was real: a batcher keeping up would have packed some of those
    // twenty thousand writes into batches by now.
    assert_eq!(
        batches(&registry),
        parked_at,
        "nothing may reach the messenger while the peer's gate is full"
    );
    assert!(
        peak_pending <= 3,
        "control must coalesce per slot, not queue: peaked at {peak_pending} entries \
         against 20 000 writes"
    );

    // Un-park the peer, one place at a time — the gate holds exactly one, so
    // the batcher flushes, parks again, and the test keeps freeing it. The
    // merged credit is applied as one grant and the merged reply goes out as
    // one record; what is asserted is that both arrive and that nothing is
    // left holding state behind them.
    let mut records: Vec<OwnedRecord> = Vec::new();
    let deadline = tokio::time::Instant::now() + RECV_TIMEOUT;
    let settled = loop {
        if tokio::time::Instant::now() >= deadline {
            break false;
        }
        match wire.try_recv() {
            Ok((_, payload)) => records.extend(OwnedBatch::decode(&payload).records),
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
        if handle.pending_control() == 0
            && records
                .iter()
                .filter(|r| r.kind == RecordType::Data)
                .count()
                == QUEUED as usize
            && records.iter().any(|r| r.kind == RecordType::CreditUpdate)
        {
            break true;
        }
    };
    assert!(
        settled,
        "the coalesced control must deliver once the peer un-parks: \
         {} entries still pending, {} of {QUEUED} records out",
        handle.pending_control(),
        records
            .iter()
            .filter(|r| r.kind == RecordType::Data)
            .count()
    );

    // The merged *values*, not merely their arrival.
    //
    // Asserted as a sum rather than as a single record of 10 000, because the
    // batcher is free to drain more than once — each freed place in the gate
    // lets it flush and look again. What must hold either way is conservation:
    // every delta written is delivered exactly once, in far fewer records than
    // it was written in. A merge that dropped or double-counted would move the
    // sum; a merge that did not happen would move the count.
    let credit: Vec<u32> = records
        .iter()
        .filter(|r| r.kind == RecordType::CreditUpdate)
        .map(|r| r.credit)
        .collect();
    assert_eq!(
        credit.iter().sum::<u32>(),
        MERGED,
        "coalescing must neither drop nor duplicate a delta"
    );
    assert!(
        credit.len() < MERGED as usize / 4,
        "ten thousand replies arrived as {} records — that is not coalescing",
        credit.len()
    );
    assert!(
        records
            .iter()
            .any(|r| r.kind == RecordType::CloseSlot && r.slot.raw() == u32::MAX),
        "the control written while parked has to survive the park"
    );
    for (n, record) in records
        .iter()
        .filter(|r| r.kind == RecordType::Data)
        .enumerate()
    {
        assert_eq!(record.data, item(n as u32), "record {n} out of order");
    }
    cancel.cancel();
}

// ---------------------------------------------------------------------------
// Epoch death
// ---------------------------------------------------------------------------

/// A singleton's failure resolving after its slot closed must not fail the
/// epoch.
///
/// The resolution carries the `SlotId` it was sent under, and a close-then-open
/// recycles that dense index under a new generation while the answer is still in
/// flight. Acting on it would take down every live slot on the peer over a
/// stream that ended cleanly before the answer arrived — and there is no gap to
/// protect, because the slot is gone. A connection-level failure is not lost
/// either: the next batch to this peer meets the same failure and fails the
/// epoch then.
#[tokio::test(flavor = "multi_thread")]
async fn a_singleton_failing_after_its_slot_closed_does_not_fail_the_epoch() {
    let harness = harness(MuxConfig::default()).await;

    let (inlet, stale) = harness.open(1, 1).await;
    harness.grant(stale, 8);
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    eventually(|| inlet.is_disconnected()).await;
    // Drain the terminal's batch so the next assertions read a quiet wire.
    while harness.try_next_batch().is_some() {}

    // The index comes back under a new generation.
    let (reopened_inlet, reopened) = harness.open(1, 2).await;
    assert_eq!(reopened.index(), stale.index());
    assert_ne!(reopened.generation(), stale.generation());

    // The old slot's singleton finally answers, and answers badly.
    harness.handle.control.singleton_resolved(stale, false);
    eventually(|| {
        harness.snapshot().counter(
            "velo_streaming_mux_records_dropped_total",
            &[("reason", "stale_singleton")],
        ) > 0.0
    })
    .await;

    assert!(
        !reopened_inlet.is_disconnected(),
        "the slot that reused the index must survive its predecessor's answer"
    );
    assert_eq!(
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[]),
        0.0,
        "a stale answer is not evidence about the connection this epoch has"
    );

    // And the reopened slot still works.
    harness.grant(reopened, 8);
    reopened_inlet.send(item(7)).expect("send on reopened slot");
    let mut seen = None;
    while seen.is_none() {
        seen = harness
            .next_batch()
            .await
            .records
            .into_iter()
            .find(|r| r.kind == RecordType::Data);
    }
    assert_eq!(seen.expect("record").data, item(7));
}

#[tokio::test(flavor = "multi_thread")]
async fn epoch_death_fails_every_live_slot_exactly_once() {
    let harness = harness(MuxConfig::default()).await;

    let mut inlets = Vec::new();
    let mut ids = Vec::new();
    for session in 0..3u64 {
        let (inlet, id) = harness.open(1, session).await;
        harness.grant(id, 8);
        inlets.push(inlet);
        ids.push(id);
    }
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        3.0
    );

    // A singleton whose admission never resolved. The batch it carried is gone,
    // so every slot packed into that epoch has a frame_seq gap that can never
    // close — which is why any failed admission fails the whole epoch.
    harness.handle.control.singleton_resolved(ids[0], false);

    for inlet in &inlets {
        eventually(|| inlet.is_disconnected()).await;
    }
    let snapshot = harness.snapshot();
    assert_eq!(
        snapshot.counter("velo_streaming_mux_epoch_deaths_total", &[]),
        1.0,
        "one death, not one per slot"
    );
    assert_eq!(
        snapshot.gauge("velo_streaming_mux_live_slots", &[]),
        0.0,
        "slots do not survive an epoch"
    );

    // The batcher stays usable: a reconnect is a fresh epoch, not a fresh task.
    let (_inlet, id) = harness.open(1, 99).await;
    assert!(
        ids.iter().any(|prior| prior.index() == id.index()),
        "the freed dense indices are reused, which is what the generation tag exists for"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_epoch_restarts_batch_sequences_and_bumps_generations() {
    let harness = harness(MuxConfig::default()).await;
    let (_inlet, first) = harness.open_with_header(1, 1).await;
    let (first_slot, first_header) = first;

    harness.handle.control.singleton_resolved(first_slot, false);
    eventually(|| harness.handle.live_slots.load(Ordering::Relaxed) == 0).await;

    let (_inlet, (second_slot, second_header)) = harness.open_with_header(1, 2).await;

    assert_eq!(second_slot.index(), first_slot.index());
    assert_eq!(
        second_slot.generation(),
        first_slot.generation().wrapping_add(1),
        "reuse of a dense index has to be distinguishable from the original"
    );
    assert!(
        second_header.peer_epoch > first_header.peer_epoch,
        "a reconnect is a new epoch, so the peer can discard the old one's \
         batches by header inspection"
    );
    assert_eq!(
        second_header.batch_seq, 0,
        "batch sequences are scoped by the epoch above them"
    );
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_transport_closes_every_producer_channel() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet_a, _) = harness.open(1, 1).await;
    let (inlet_b, _) = harness.open(1, 2).await;

    harness.cancel.cancel();

    eventually(|| inlet_a.is_disconnected() && inlet_b.is_disconnected()).await;
    assert_eq!(
        harness
            .snapshot()
            .gauge("velo_streaming_mux_live_slots", &[]),
        0.0
    );
}
