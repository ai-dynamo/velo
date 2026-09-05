// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Opening a slot without waiting for the peer to admit the `OpenSlot`, and
//! the fence-aware close lane that comes with it.
//!
//! Under [`MuxConfig::async_open_ack`] the open's batch is handed to the
//! transport and the caller is answered immediately. Several things have to
//! survive that, and they are what is pinned here:
//!
//! - the slot's first record must not overtake the `OpenSlot` that claims its
//!   buffer at the receiver, which is what the per-slot fence is for;
//! - a failed admission must still be epoch death. The ack already said `Ok`
//!   and the producer already holds its inlet, so nothing else is left to tell
//!   it the stream can never make progress;
//! - a `CloseSlot` owed while the fence is up waits for it, and the slow-consumer
//!   kill that writes one still disconnects its producer on the spot. Deferring
//!   the *record* must not defer the *kill* — this one is not a property of the
//!   new gate: `send_singleton` fences any over-budget non-terminal record, so
//!   the shipped default reaches the same deferral through a different door,
//!   which is why the arms at the bottom of this file run
//!   [`MuxConfig::default`];
//! - the admission's answer reaches the batcher however full the control map
//!   is. It is the only thing that lifts the fence, so it cannot be one of the
//!   entries the map's cap refuses;
//! - and the other direction of the same property: an admission that needed no
//!   fence must not be given one anyway. `fire_singleton` only raises the fence
//!   when [`AdmissionState::Admitted`] is not already the answer, so on an
//!   uncongested peer the slot's first record is never withheld waiting for a
//!   round trip nothing ordered it against — the round trip itself (the
//!   spawned watcher, the control-inbox insert, the eventual `unfence`) still
//!   happens, off the critical path, exactly as it does when the fence is
//!   raised.

use std::sync::Arc;
use std::time::Duration;

use super::super::control::MAX_PENDING_CONTROL;
use super::super::test_hooks::TestHooks;
use super::super::*;
use super::support::*;
use crate::streaming::messenger_mux::protocol::{RecordType, SlotId};

/// Credit generous enough that nothing here parks for want of it: the fence is
/// the only thing that may hold a record back.
const CREDIT: u32 = 4096;

fn async_open_ack() -> MuxConfig {
    MuxConfig {
        async_open_ack: true,
        ..MuxConfig::default()
    }
}

/// An admitted `OpenSlot` leaves nothing for the fence to wait for.
///
/// `_stream_batch` is a system handler, so `can_send_directly` takes the direct
/// send the moment the peer is registered — the harness's real messenger pair
/// registers each other before this test ever opens a slot. With the gate's one
/// place free, `AdmissionGate::send` returns `SendOutcome::Admitted`
/// synchronously, and `FireResult::admission_state()` reads that answer without
/// awaiting anything. On that path a fence buys no ordering: every record this
/// batcher dispatches after `open_detached`'s call to `fire_singleton` enters
/// the same FIFO admission gate behind the frame that was just admitted, since
/// the batcher dispatches one record at a time on its own task.
///
/// The spawned watcher, its control-inbox insert, the `Notify` wake and the
/// eventual `unfence` are unconditional — they run either way, off the
/// critical path. What fencing anyway used to cost is the *wait*: the slot's
/// first record could not be staged until that whole round trip had been
/// drained, which on exactly the uncongested peer `MuxConfig::async_open_ack`
/// exists to speed up bought no order at all.
///
/// The fence decision itself is made synchronously, before `fire_singleton`
/// ever yields, so `TestHooks::fenced_count` pins it without racing the
/// spawned admission watcher the way asserting on the withheld gauge would:
/// that watcher can resolve before or after a test gets to look, but nothing
/// races the fence itself.
#[tokio::test(flavor = "multi_thread")]
async fn an_admitted_open_slot_is_never_fenced() {
    let hooks = Arc::new(TestHooks::default());
    let harness = harness_with_hooks(async_open_ack(), Some(Arc::clone(&hooks))).await;

    let (_inlet, _slot) = harness.open_credited(1, 1, CREDIT).await;

    assert_eq!(
        hooks.fenced_count(),
        0,
        "an OpenSlot admitted synchronously has already reached the transport's \
         send channel; fencing it would only make the record wait for a round \
         trip that orders nothing"
    );
}

/// A slot's first record waits for the `OpenSlot`'s admission, not for the wire.
///
/// The receiver binds a slot on the `OpenSlot` that names it, so a `Data` record
/// arriving first has nowhere to land. Per-target FIFO is not enough to prevent
/// that when the admission does not resolve synchronously: a send to a peer the
/// messenger has not resolved yet is issued from a detached task, which keeps no
/// order against the send that follows it. So the slot is fenced whenever
/// `fire_singleton` does not read `AdmissionState::Admitted` back, and the
/// record waits in the withheld queue rather than in a batch. This arm forces
/// that by stalling the wire (see [`stalled_harness`]) so the first open's
/// `OpenSlot` is left `Pending`, not `Admitted`, when the second one is fired.
#[tokio::test(flavor = "multi_thread")]
async fn fence_holds_first_data_until_the_open_slot_admission_resolves() {
    let harness = stalled_harness(async_open_ack()).await;

    // The first open takes the gate's one free place and is admitted there and
    // then, so it is not the one under test — it is how the gate gets full.
    let (_first_inlet, first_ack) = harness.open(1, 1, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, first_ack)
        .await
        .expect("ack")
        .expect("ack delivered")
        .expect("slot allocated");
    eventually(|| harness.wire.is_full()).await;

    // The second one's `OpenSlot` parks behind it, and the ack comes back
    // anyway.
    let (inlet, ack_rx) = harness.open(2, 2, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("the ack must not wait on the parked admission")
        .expect("ack delivered")
        .expect("slot allocated");

    // The producer sends at once, as it may the moment it holds the inlet.
    inlet.send(item(0)).expect("queue record");

    // Withheld, not staged. The distinction is the whole assertion: a staged
    // record is one the batcher has already decided to put on the wire, and
    // without the fence that is where this one would be.
    harness.await_withheld(1).await;
    assert_eq!(
        harness.staged(),
        0.0,
        "the fence must keep the record out of the batch, not merely out of the flush"
    );

    // Free the gate. The parked `OpenSlot` is admitted, its resolution lifts
    // the fence, and the record follows it out.
    let first = harness.next_wire_batch().await;
    assert_eq!(first.records[0].kind, RecordType::OpenSlot);

    let opened = harness.next_wire_batch().await;
    assert_eq!(opened.records[0].kind, RecordType::OpenSlot);
    assert_eq!(opened.records[0].frame_seq, 0);

    let data = harness.next_wire_batch().await;
    assert_eq!(data.records[0].kind, RecordType::Data);
    assert_eq!(data.records[0].slot, opened.records[0].slot);
    assert_eq!(
        data.records[0].frame_seq, 1,
        "the first record of a slot follows the `OpenSlot` that claims it"
    );
}

/// A failed `OpenSlot` admission is still epoch death.
///
/// The awaited path fails the epoch from the flush that was never admitted. The
/// detached one has already answered `Ok` by the time the answer arrives, so the
/// failure has to travel the way an over-budget singleton's does — as coalesced
/// control naming the slot it was sent under — and mean the same thing there. A
/// batch that never reached the wire leaves a `frame_seq` gap the mux cannot
/// close, whichever path dispatched it.
///
/// This drives `on_owned_control`'s reaction to that control directly — the
/// same reaction the rendezvous path already exercised. It does not reach the
/// `tokio::spawn` watcher `open_detached` adds; the test below,
/// `a_real_admission_failure_reaches_the_detached_watcher`, is what drives a
/// genuine failure through that.
#[tokio::test(flavor = "multi_thread")]
async fn failed_open_slot_admission_is_still_epoch_death() {
    let harness = harness(async_open_ack()).await;
    let (inlet, slot) = harness.open_credited(1, 1, CREDIT).await;

    // The admission the detached watcher is waiting on comes back a failure.
    harness.handle.control.singleton_resolved(slot, false);

    eventually(|| {
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[])
            > 0.0
    })
    .await;
    eventually(|| inlet.is_disconnected()).await;
}

/// A real failed admission, arriving through the detached watcher itself.
///
/// `open_detached` answers the caller before `fire.await` resolves, and
/// watches it from `tokio::spawn(async move { control.singleton_resolved(id,
/// fire.await.is_ok()) })`. Nothing before this test drove that spawn to a
/// genuine `Err`; the arm above injects the answer the watcher would have
/// produced instead of producing it. Here the gate's one place is taken by a
/// filler, the target's `OpenSlot` parks behind it, and dropping the receiver
/// behind the gate fails the parked ticket for real — `AdmissionGate`'s driver
/// task resolves it `Err(AdmissionError::ChannelClosed)`, which is exactly what
/// a peer going away mid-send looks like from here.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_admission_failure_reaches_the_detached_watcher() {
    let mut harness = stalled_harness(async_open_ack()).await;
    let (_filler_inlet, inlet) = open_behind_a_full_gate(&harness).await;

    harness.disconnect_wire();

    eventually(|| {
        harness
            .snapshot()
            .counter("velo_streaming_mux_epoch_deaths_total", &[])
            > 0.0
    })
    .await;
    eventually(|| inlet.is_disconnected()).await;
}

/// The fence lifts even when the admission answers into a full control map.
///
/// A peer holding more live slots than [`MAX_PENDING_CONTROL`] — a router in
/// front of a worker that is carrying thousands of streams — fills the
/// owned-control map with legitimate grants between two drains. The `OpenSlot`
/// resolution that lands then is not one more grant: it is the only thing that
/// lifts this slot's fence, and refusing it leaves every record the slot ever
/// queues withheld until the consumer's heartbeat watchdog gives up. Measured
/// as HTTP 500s on the tier-3 rig's `velo4a` and `velo34` arms.
#[tokio::test(flavor = "multi_thread")]
async fn the_fence_lifts_when_the_admission_answers_into_a_full_control_map() {
    let hooks = Arc::new(TestHooks::default());
    let harness = stalled_harness_with_hooks(async_open_ack(), Some(Arc::clone(&hooks))).await;
    let (_filler_inlet, inlet) = open_behind_a_full_gate(&harness).await;
    inlet.send(item(0)).expect("queue record");
    harness.await_withheld(1).await;

    // Park the batcher mid-wake, so what lands in its inbox from here on stays
    // there until it is released.
    hooks.pause();
    harness.handle.kick_flush();
    hooks.wait_until_parked().await;

    // What a peer with that many streams sends between two drains: one grant
    // per slot, none of them this one.
    for index in 0..MAX_PENDING_CONTROL {
        let bogus = SlotId::new(1_000 + index as u32, 0).expect("index fits u24");
        harness.handle.grant(bogus, 1);
    }
    let filled = harness.handle.control.pending_len();
    assert!(filled >= MAX_PENDING_CONTROL, "the map is at its cap");
    // A delta from here, not an assertion that this is zero: the filler's own
    // `OpenSlot` resolves too (its admission was synchronous, so `fire_singleton`
    // never fenced it, but the watcher it still spawns calls
    // `singleton_resolved` regardless), and that resolution can land before or
    // after this line runs. It costs nothing either way — resolutions live in
    // their own map now (see `MAX_PENDING_CONTROL`'s doc) and can never be
    // refused — but a delta is what makes the property under test (the fenced
    // slot's own resolution is never refused) independent of that timing.
    let refused_before = harness.handle.control.refused();

    // Free the gate. The parked `OpenSlot` is admitted, and its resolution
    // arrives while the map is full.
    let first = harness.next_wire_batch().await;
    assert_eq!(first.records[0].kind, RecordType::OpenSlot);
    let opened = harness.next_wire_batch().await;
    assert_eq!(opened.records[0].kind, RecordType::OpenSlot);
    eventually(|| {
        harness.handle.control.pending_len() > filled || harness.handle.control.refused() > 0
    })
    .await;

    hooks.release();

    let data = harness.next_wire_batch().await;
    assert_eq!(data.records[0].kind, RecordType::Data);
    assert_eq!(data.records[0].slot, opened.records[0].slot);
    assert_eq!(
        data.records[0].frame_seq, 1,
        "the record follows the `OpenSlot` whose admission it waited for"
    );
    assert_eq!(
        harness.handle.control.refused(),
        refused_before,
        "the resolution was owed to the fence, not refused at the cap"
    );
}

// ---------------------------------------------------------------------------
// A close behind the fence
// ---------------------------------------------------------------------------

/// Live slots on the peer, as the batcher publishes them.
///
/// The "exactly one close" meter: a slot leaves this gauge when — and only
/// when — its `CloseSlot` is written.
fn live_slots(harness: &StalledHarness) -> f64 {
    harness
        .snapshot()
        .gauge("velo_streaming_mux_live_slots", &[])
}

/// Fill the gate's one place, then open a second slot whose `OpenSlot` parks in
/// it. Yields the filler's inlet and the parked slot's.
///
/// Two opens rather than one because the gate is what holds the fence open: the
/// first send takes the free place, and everything after it waits for a test to
/// take that place back. The filler's inlet comes back so the caller can hold
/// it — dropping it would queue an `InletClosed` of its own and put a second
/// close on the wire.
async fn open_behind_a_full_gate(
    harness: &StalledHarness,
) -> (flume::Sender<Vec<u8>>, flume::Sender<Vec<u8>>) {
    let (filler_inlet, first_ack) = harness.open(1, 1, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, first_ack)
        .await
        .expect("ack")
        .expect("ack delivered")
        .expect("slot allocated");
    eventually(|| harness.wire.is_full()).await;

    let (inlet, ack_rx) = harness.open(2, 2, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("the ack must not wait on the parked admission")
        .expect("ack delivered")
        .expect("slot allocated");
    (filler_inlet, inlet)
}

/// A third open, which is the *positive* proof that the close was not written.
///
/// A `CloseSlot` written while the fence is up goes out through `flush`, which
/// parks the batcher on the admission its own `OpenSlot` is still holding — and
/// every command queued after it waits there too. So an open that is answered
/// is an open the batcher was free to serve.
async fn open_answered_while_the_gate_is_full(harness: &StalledHarness) -> flume::Sender<Vec<u8>> {
    let (third_inlet, third_ack) = harness.open(3, 3, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, third_ack)
        .await
        .expect("a close held behind the fence must not park the batcher on admission")
        .expect("ack delivered")
        .expect("slot allocated");
    third_inlet
}

/// Take the three parked `OpenSlot`s off the gate and assert the close follows.
///
/// Draining a place lets the next frame in, so the wire replays the order the
/// gate admitted them in: the three opens, and then — only then — the close the
/// second slot owed.
async fn assert_the_close_follows_its_open(harness: &StalledHarness) {
    let first = harness.next_wire_batch().await;
    assert_eq!(first.records[0].kind, RecordType::OpenSlot);
    let opened = harness.next_wire_batch().await;
    assert_eq!(opened.records[0].kind, RecordType::OpenSlot);
    let third = harness.next_wire_batch().await;
    assert_eq!(third.records[0].kind, RecordType::OpenSlot);

    let close = harness.next_wire_batch().await;
    assert_eq!(close.records.len(), 1, "the close travels alone");
    assert_eq!(close.records[0].kind, RecordType::CloseSlot);
    assert_eq!(
        close.records[0].slot, opened.records[0].slot,
        "the close belongs to the slot whose admission it waited for"
    );
    assert_eq!(
        close.records[0].frame_seq, 1,
        "the close takes the sequence after its own `OpenSlot`, not one reserved \
         while the fence was up"
    );
    // Exactly one close: the two slots still open are the filler and the prober.
    eventually(|| (live_slots(harness) - 2.0).abs() < f64::EPSILON).await;
}

/// A departed producer's `CloseSlot` waits for the `OpenSlot` admission.
///
/// The close is the slot's second record, and it may no more overtake the
/// `OpenSlot` that claims the buffer it names than the first data record may.
/// A `CloseSlot` the receiver meets first names a slot it has never bound, so it
/// is dropped as `closed_slot` — and the `OpenSlot` landing behind it then
/// binds a stream nothing will ever close, which the consumer pays for by
/// waiting out its heartbeat watchdog. The fence is what holds it, and an empty
/// withheld queue is exactly the case the queue itself cannot cover.
#[tokio::test(flavor = "multi_thread")]
async fn a_departed_producer_s_close_waits_for_the_open_slot_admission() {
    let harness = stalled_harness(async_open_ack()).await;
    let (_filler_inlet, inlet) = open_behind_a_full_gate(&harness).await;

    // Nothing was ever sent, so nothing is withheld and the fence is the only
    // thing that can hold the close back.
    drop(inlet);

    let _prober_inlet = open_answered_while_the_gate_is_full(&harness).await;
    assert_the_close_follows_its_open(&harness).await;
}

/// The slow-consumer kill's `CloseSlot` waits for the `OpenSlot` admission too.
///
/// `overflow_kill` reaches the wire by a second door — the producer ran past the
/// byte cap rather than went away — and it discards the withheld queue on its
/// way, so the queue that defers a departed producer's close is empty by the
/// time this one is written. The fence has to be consulted directly.
#[tokio::test(flavor = "multi_thread")]
async fn an_overflow_kill_waits_for_the_open_slot_admission() {
    let harness = stalled_harness(MuxConfig {
        slot_byte_budget: 256,
        ..async_open_ack()
    })
    .await;
    let (_filler_inlet, inlet) = open_behind_a_full_gate(&harness).await;

    // Fenced, so every record is withheld however much credit the slot holds:
    // what this producer meets is the byte cap, not the ledger.
    for n in 0..64u32 {
        let _ = inlet.send(item(n));
    }
    // The kill has run — the positive fact this arm turns on, since the close it
    // writes is emitted in the same call.
    eventually(|| harness.overflow_dropped() > 0.0).await;

    let _prober_inlet = open_answered_while_the_gate_is_full(&harness).await;
    assert_the_close_follows_its_open(&harness).await;
}

/// The kill disconnects its producer at once; only the record waits.
///
/// `overflow_kill` is the per-slot slow-consumer kill, and what it is *for* is
/// cutting a producer off from a stream nobody is draining. The `CloseSlot` it
/// writes tells the far side; ending the inlet tells the near side, and that is
/// the half a fence must not postpone — a producer left connected keeps running
/// ahead into a slot whose records are already being thrown away, and learns
/// nothing until the peer un-parks, which on the congested peer this fence is
/// about may be a very long time.
///
/// The disconnect costs one turn of the batcher's drain loop rather than being
/// synchronous: closing the gate ends the slot's stream, `SelectAll` drops it on
/// the next poll, and dropping it is what drops the `flume::Receiver` the
/// producer's `Sender` is paired with. `close_local` has exactly the same lag,
/// so an unfenced kill's timing is unchanged.
///
/// `live_slots` is what separates "the close was deferred" from "the close was
/// written and is merely queued", and it is the only thing that does: a close
/// written while the fence is up would leave on the wire behind the `OpenSlot`
/// anyway and would carry the same `frame_seq`, so neither order nor sequence
/// tells the two apart. The gauge does, because `close_local` runs synchronously
/// inside `finish_close`.
#[tokio::test(flavor = "multi_thread")]
async fn an_overflow_kill_disconnects_its_producer_before_the_fence_lifts() {
    let harness = stalled_harness(MuxConfig {
        slot_byte_budget: 256,
        ..async_open_ack()
    })
    .await;
    let (_filler_inlet, inlet) = open_behind_a_full_gate(&harness).await;

    for n in 0..64u32 {
        let _ = inlet.send(item(n));
    }
    eventually(|| harness.overflow_dropped() > 0.0).await;

    eventually(|| inlet.is_disconnected()).await;

    let _prober_inlet = open_answered_while_the_gate_is_full(&harness).await;
    assert_eq!(
        live_slots(&harness),
        3.0,
        "the killed slot still owes its consumer a `CloseSlot`, so it is still \
         live: the filler, the killed slot, and the prober"
    );
    assert_the_close_follows_its_open(&harness).await;
}

/// Control: with the awaited ack the slot is never fenced, so nothing waits.
///
/// The fence-aware branch must not cost the default path a thing. Here the
/// `OpenSlot` was admitted before `connect` returned, so the slot is unfenced
/// the moment the producer holds its inlet and its close goes out on the spot.
#[tokio::test(flavor = "multi_thread")]
async fn the_awaited_ack_closes_a_departed_producer_s_slot_at_once() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, slot) = harness.open(1, 1).await;

    drop(inlet);

    let batch = harness.next_batch().await;
    assert_eq!(batch.records.len(), 1, "the close travels alone");
    assert_eq!(batch.records[0].kind, RecordType::CloseSlot);
    assert_eq!(batch.records[0].slot, slot);
    assert_eq!(
        batch.records[0].frame_seq, 1,
        "the close is the slot's second record"
    );
}

// ---------------------------------------------------------------------------
// The same lane, with the gate off
// ---------------------------------------------------------------------------
//
// `async_open_ack` is not what opens the deferral branch. `send_singleton`
// fences any over-budget non-terminal record, so a mux running the shipped
// default reaches a fenced slot with a close owed on it, and these two arms are
// that configuration. They pin the behaviour the isolation matrix's `velo0` arm
// runs.

/// Long enough for the batcher to have acted, short enough to keep the suite
/// quick. Only ever used to give a *negative* fact time to become false.
const SETTLE: Duration = Duration::from_millis(200);

/// A batch cap and a byte cap small enough that a test can go over both.
fn tight_batches() -> MuxConfig {
    MuxConfig {
        max_batch_bytes: 256,
        slot_byte_budget: 256,
        ..MuxConfig::default()
    }
}

/// A record no eager batch to this peer can hold, so it rides rendezvous.
fn over_budget_record() -> Vec<u8> {
    rmp_serde::to_vec(&crate::streaming::frame::StreamFrame::Item(vec![7u8; 512]))
        .expect("encode an over-budget record")
}

/// Rendezvous singletons dispatched — the positive fact that a slot is fenced.
fn rendezvous_singletons(harness: &StalledHarness) -> f64 {
    harness
        .snapshot()
        .counter("velo_streaming_mux_rendezvous_singletons_total", &[])
}

/// Fence a slot the way the default config does, and leave it fenced.
///
/// The open is awaited here, because the gate is off: its flush takes the
/// stalled transport's one free place and the ack follows. What parks is the
/// *next* write — the over-budget record's rendezvous singleton — and nothing
/// answers its admission until a test takes that place back. Yields the
/// producer's inlet with the singleton outstanding.
async fn fence_through_an_over_budget_record(harness: &StalledHarness) -> flume::Sender<Vec<u8>> {
    let (inlet, ack_rx) = harness.open(1, 1, CREDIT).await;
    tokio::time::timeout(RECV_TIMEOUT, ack_rx)
        .await
        .expect("ack")
        .expect("ack delivered")
        .expect("slot allocated");
    eventually(|| harness.wire.is_full()).await;

    inlet
        .send(over_budget_record())
        .expect("queue the over-budget record");
    eventually(|| rendezvous_singletons(harness) > 0.0).await;
    inlet
}

/// Drain the gate and assert the deferred close comes out behind the singleton.
async fn assert_the_close_follows_the_singleton(harness: &StalledHarness) {
    let open = harness.next_wire_batch().await;
    assert_eq!(open.records[0].kind, RecordType::OpenSlot);

    let singleton = harness.next_wire_batch().await;
    assert_eq!(
        singleton.records.len(),
        1,
        "an over-budget record goes alone"
    );
    assert_eq!(singleton.records[0].kind, RecordType::Data);

    let close = harness.next_wire_batch().await;
    assert_eq!(close.records.len(), 1, "the close travels alone");
    assert_eq!(close.records[0].kind, RecordType::CloseSlot);
    assert_eq!(
        close.records[0].slot, singleton.records[0].slot,
        "the close belongs to the slot whose admission it waited for"
    );
    assert_eq!(
        close.records[0].frame_seq, 2,
        "the close is the slot's third record: `OpenSlot`, the singleton, then it"
    );
    eventually(|| live_slots(harness) == 0.0).await;
}

/// Gate off: a departed producer's close waits for the singleton's admission.
///
/// The fence-aware branch in `on_inlet_closed` is reached by the default config
/// through the over-budget record, so this is a regression guard on today's
/// shipped behaviour rather than on anything the new gate introduced.
///
/// The settle cannot make this pass for the wrong reason. Had the close been
/// written during it, `finish_close` would have run `close_local` synchronously
/// and the gauge would already have dropped — the assertion is about *when the
/// decision was taken*, which is the one thing the wire cannot show, since a
/// close written early would queue behind the same singleton and carry the same
/// `frame_seq`.
#[tokio::test(flavor = "multi_thread")]
async fn the_default_defers_a_departed_producer_s_close_behind_a_fence() {
    let harness = stalled_harness(tight_batches()).await;
    let inlet = fence_through_an_over_budget_record(&harness).await;

    drop(inlet);
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        live_slots(&harness),
        1.0,
        "the close may not be written while the singleton's admission is unanswered"
    );

    assert_the_close_follows_the_singleton(&harness).await;
}

/// Gate off: the kill disconnects its producer at once and defers only the
/// record.
///
/// The overrun arm of the same default configuration. `overflow_kill` discards
/// the withheld queue on its way, so the queue that defers a departed producer's
/// close is empty here and the fence is the only thing left holding it — while
/// the producer, which is the party the kill exists to stop, must be cut off
/// immediately.
#[tokio::test(flavor = "multi_thread")]
async fn the_default_disconnects_an_overflow_kill_s_producer_behind_a_fence() {
    let harness = stalled_harness(tight_batches()).await;
    let inlet = fence_through_an_over_budget_record(&harness).await;

    // Fenced, so every record after the singleton is withheld however much
    // credit the slot holds, until the byte cap refuses one.
    for n in 0..64u32 {
        let _ = inlet.send(item(n));
    }
    eventually(|| harness.overflow_dropped() > 0.0).await;

    eventually(|| inlet.is_disconnected()).await;
    assert_eq!(
        live_slots(&harness),
        1.0,
        "the wire close still waits for the singleton it is ordered behind"
    );

    assert_the_close_follows_the_singleton(&harness).await;
}
