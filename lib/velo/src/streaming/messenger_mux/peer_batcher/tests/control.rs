// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Coalesced control, epoch death, and teardown.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use super::super::test_hooks::TestHooks;
use super::super::*;
use super::support::*;
use crate::streaming::messenger_mux::protocol::RecordType;
use crate::streaming::sender::cached_finalized;

/// A reply that lands after the drain that carried `retire` is not lost.
///
/// Retirement is decided in two places: the sweep claims the handle under the
/// registry lock and posts `retire` as control, and the task reads it on its
/// next drain and decides to stop. A reply posted between that drain and the
/// task's exit used to be applied by nobody — the drain loop is skipped once
/// `stopping` is set and nothing read the inbox again — while the liveness
/// flag the mux checked *before* posting still said alive. Where it bit: the
/// zero-RTT prompt close, `close_claimed_slot`, retires the last ingress slot
/// for the peer (which is what makes the batcher evictable) and only then
/// posts the `CloseSlot` that is the idle producer's one notification. Lost,
/// the producer's `StreamSender` stayed open for good: every later record it
/// sends is dropped at the receiver as `ClosedSlot`, with no reply.
///
/// Now the task's last read closes the inbox under its lock, so a reply
/// either rides the final flush or is refused and its writer re-resolves.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_posted_after_the_retire_drain_rides_the_final_flush() {
    let hooks = Arc::new(TestHooks::default());
    let harness = harness_with_hooks(MuxConfig::default(), Some(Arc::clone(&hooks))).await;

    // The sweep's eviction, by hand: claim, then post. With no slot ever
    // opened the batcher is idle from birth, so `retire` is its first wake.
    hooks.pause();
    assert!(
        harness.handle.try_retire(0),
        "an idle batcher holding no slots is evictable"
    );
    harness.handle.retire();
    // Parked past the drain that carried `retire`, with `stopping` decided
    // and the exit one step away.
    hooks.wait_until_parked().await;

    let slot = SlotId::from_raw(7);
    let close = [ReplyRecord::CloseSlot {
        slot,
        reason: CloseReason::UnknownSlot,
    }];
    assert!(
        harness.handle.reply(&close),
        "the inbox is open until the task's last read, so the reply is taken"
    );
    hooks.release();

    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 1, "the reply rides the final flush");
    assert_eq!(batch.records[0].kind, RecordType::CloseSlot);
    assert_eq!(batch.records[0].slot, slot);

    // The batch reaching the wire means the final flush ran, and the last
    // read is before it; from here on a writer is told to re-resolve.
    assert!(
        !harness.handle.reply(&close),
        "past its last read the batcher refuses the reply rather than losing it"
    );
}

// ---------------------------------------------------------------------------
// Control is state, not a queue
// ---------------------------------------------------------------------------

/// A batcher stalled on admission must not grow with what arrives behind it.
///
/// The stall is not exotic: a flush parks whenever the peer is congested, and a
/// congested peer is exactly when its ingress lane is busiest returning credit.
/// An unbounded control queue in that window is unbounded memory. Coalesced
/// state does not grow with arrival rate — see `ControlState`'s struct doc for
/// the bound each map actually carries — and the deltas it merged still
/// deliver once the peer un-parks.
#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_batcher_coalesces_control_instead_of_queueing_it() {
    let harness = stalled_harness(MuxConfig::default()).await;

    // Open a slot. Its eager `OpenSlot` flush takes the gate's one free place,
    // and taking it back off the wire here is what frees that place again.
    let (inlet, ack_rx) = harness.open(1, 1, 0).await;
    let opened = tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("ack")
        .expect("ack delivered");
    assert!(opened.is_ok());

    let open_batch = harness.next_wire_batch().await;
    let id = open_batch.records[0].slot;

    // Fill the gate's one place and leave it filled. Nothing drains `wire`
    // until the release phase, so every flush after this one parks.
    harness.handle.grant(id, 1);
    inlet.send(item(0)).expect("queue record");
    eventually(|| harness.wire.is_full()).await;

    // A control write the batcher can act on, so the flush it triggers is the
    // one that parks. After this the batcher is inside `flush().await` and
    // cannot take anything else off the control state.
    harness.handle.reply(&[ReplyRecord::CloseSlot {
        slot: SlotId::from_raw(u32::MAX),
        reason: CloseReason::UnknownSlot,
    }]);
    let batches = |registry: &prometheus::Registry| {
        crate::observability::test_helpers::MetricSnapshot::from_registry(registry)
            .counter("velo_streaming_mux_batches_total", &[("direction", "sent")])
    };
    eventually(|| batches(&harness.registry) >= 3.0).await;
    let parked_at = batches(&harness.registry);

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
        harness.handle.grant(id, 1);
        harness
            .handle
            .reply(&[ReplyRecord::CreditUpdate { slot: id, delta: 1 }]);
        peak_pending = peak_pending.max(harness.handle.pending_control());
    }
    // The stall was real: a batcher keeping up would have packed some of those
    // twenty thousand writes into batches by now.
    assert_eq!(
        batches(&harness.registry),
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
        match harness.wire.try_recv() {
            Ok((_, payload)) => records.extend(OwnedBatch::decode(&payload).records),
            Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
        }
        if harness.handle.pending_control() == 0
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
        harness.handle.pending_control(),
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

/// A cancelled batcher leaves the registry before it refuses a reply.
///
/// `send_replies` re-resolves a refused reply through the registry and loops
/// until a batcher takes it, and that loop terminates only if a closed batcher
/// is never the registered one. The retire path holds it because the sweep
/// removes the entry before posting `retire`. Cancellation is the other exit,
/// and it closed the inbox first and unregistered second, so a writer refused
/// in between resolved the same batcher again. Nothing writes after cancel
/// today — it comes only from `MuxCore::drop` — which is exactly why the
/// order is pinned here rather than argued from the callers.
///
/// The window is the few instructions between the two, so one attempt would
/// prove little; the writer spins on a thread of its own, as `send_replies`
/// spins, and the scenario runs enough times that the old order cannot pass
/// by luck. Each attempt cancels only once the inbox has taken a reply — the
/// writer provably inside its spin — because an attempt that cancelled first
/// would find the inbox closed and the entry gone under either order, and a
/// blocking-pool thread on a loaded runner can start later than any sleep
/// budgets for.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_batcher_is_unregistered_before_it_refuses_a_reply() {
    let (sender, capture, _batches) = capture_pair().await;
    let peer = capture.instance_id().worker_id();

    for attempt in 0..64 {
        let cancel = CancellationToken::new();
        let batchers: Arc<BatcherMap> = Arc::new(DashMap::new());
        let handle = spawn(
            peer,
            BatcherContext {
                messenger: Arc::clone(&sender),
                config: MuxConfig::default(),
                metrics: None,
                epochs: Arc::new(AtomicU64::new(1)),
                batchers: Arc::clone(&batchers),
                cancel: cancel.clone(),
                hooks: None,
            },
        );
        batchers.insert(peer, Arc::clone(&handle));

        let spinning = Arc::new(AtomicBool::new(false));
        let writer = {
            let handle = Arc::clone(&handle);
            let batchers = Arc::clone(&batchers);
            let spinning = Arc::clone(&spinning);
            tokio::task::spawn_blocking(move || {
                let close = [ReplyRecord::CloseSlot {
                    slot: SlotId::from_raw(7),
                    reason: CloseReason::UnknownSlot,
                }];
                // The first reply taken is what the cancel below waits for; if
                // it is refused instead nothing raises the flag and the wait
                // times out, which is the loud failure a stray close deserves.
                if handle.reply(&close) {
                    spinning.store(true, Ordering::Release);
                    while handle.reply(&close) {
                        std::hint::spin_loop();
                    }
                }
                // Refused. A re-resolve now must not hand this batcher back.
                batchers
                    .get(&peer)
                    .is_some_and(|entry| Arc::ptr_eq(entry.value(), &handle))
            })
        };
        // The exit being raced starts only once the writer is in its spin.
        eventually(|| spinning.load(Ordering::Acquire)).await;
        cancel.cancel();

        let still_registered = writer.await.expect("writer thread");
        assert!(
            !still_registered,
            "attempt {attempt}: the batcher refused a reply while still the \
             registered one, so a writer re-resolving would get it back"
        );
    }
}
