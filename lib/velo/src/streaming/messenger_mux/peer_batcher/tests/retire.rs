// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Stream retirement: the batcher reports a stream sent once the batch that
//! carries its terminal is admitted, and discarded when the slot or the batch
//! dies holding its output. Every way out must report, or a producer waiting
//! on `StreamSender::retired` waits forever.

use std::time::Duration;

use futures::FutureExt;

use super::super::*;
use super::support::*;
use crate::streaming::Retirement;
use crate::streaming::sender::cached_finalized;

/// The terminal rides a batch, and the stream retires as sent once that batch
/// is admitted. A record before it is not the end of the stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_retires_its_stream_once_its_batch_is_admitted() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, _, closed) = harness.open_watched(1, 1, 8).await;

    inlet.send(item(0)).expect("queue item");
    assert_eq!(harness.next_batch().await.records[0].data, item(0));
    assert_eq!(retired(&closed).now_or_never(), None);

    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    let batch = harness.next_batch().await;
    assert_eq!(batch.records[0].data, *cached_finalized());
    assert_eq!(
        tokio::time::timeout(RECV_TIMEOUT, retired(&closed)).await,
        Ok(Retirement::Sent)
    );
}

/// A slot closed with its output still withheld retires its stream as
/// discarded: here the consumer closes it, as a consumer that drops its
/// anchor does.
#[tokio::test(flavor = "multi_thread")]
async fn a_slot_closed_with_withheld_output_retires_as_discarded() {
    let harness = harness(MuxConfig::default()).await;
    let (inlet, slot, closed) = harness.open_watched(1, 1, 0).await;
    inlet.send(item(0)).expect("queue item");
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    harness.await_withheld(2).await;
    assert_eq!(retired(&closed).now_or_never(), None);

    harness.handle.peer_closed(
        slot,
        crate::streaming::messenger_mux::protocol::CloseReason::UnknownSlot,
    );
    assert_eq!(
        tokio::time::timeout(RECV_TIMEOUT, retired(&closed)).await,
        Ok(Retirement::Discarded)
    );
}

/// A batch the transport refuses never reached the wire: the epoch dies with
/// it, and the stream whose terminal it carried retires as discarded.
///
/// The terminal goes alone, so the batch parked on admission is the one that
/// carries it, and the guard in that batch is what reports. An item queued
/// before it could ride a batch of its own, and leave the terminal in the
/// inlet, where the slot's close would report instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_batch_retires_its_terminals_as_discarded() {
    let mut harness = stalled_harness(MuxConfig::default()).await;
    let (inlet, ack, closed) = harness.open_watched(1, 1, 8).await;
    tokio::time::timeout(RECV_TIMEOUT, ack)
        .await
        .expect("ack")
        .expect("ack delivered")
        .expect("slot allocated");
    // The `OpenSlot` holds the gate's one place, so the batch with the
    // terminal parks on admission.
    inlet
        .send(cached_finalized().clone())
        .expect("queue terminal");
    eventually(|| harness.transport.stalled() >= 1).await;
    assert_eq!(retired(&closed).now_or_never(), None);

    harness.disconnect_wire();
    assert_eq!(
        tokio::time::timeout(RECV_TIMEOUT, retired(&closed)).await,
        Ok(Retirement::Discarded)
    );
}

/// A batcher task that dies while its batch waits on admission takes the
/// stream's terminal with it. The stream still retires, as discarded: the
/// guard in the dropped batch reports it, so `retired()` never hangs.
///
/// The runtime shutdown is what drops the task without running its exit path,
/// the same as an abort or a panic would. The terminal goes alone, so the
/// parked batch is the one that carries it, as in the refused-batch test.
#[test]
fn an_aborted_batcher_retires_its_parked_terminal_as_discarded() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let (harness, closed, _inlet) = runtime.block_on(async {
        let harness = stalled_harness(MuxConfig::default()).await;
        let (inlet, ack, closed) = harness.open_watched(1, 1, 8).await;
        tokio::time::timeout(RECV_TIMEOUT, ack)
            .await
            .expect("ack")
            .expect("ack delivered")
            .expect("slot allocated");
        inlet
            .send(cached_finalized().clone())
            .expect("queue terminal");
        eventually(|| harness.transport.stalled() >= 1).await;
        (harness, closed, inlet)
    });
    assert_eq!(retired(&closed).now_or_never(), None);

    runtime.shutdown_timeout(Duration::from_secs(5));
    assert_eq!(
        retired(&closed).now_or_never(),
        Some(Retirement::Discarded),
        "the dropped batcher must report the stream it held"
    );
    // The messenger it held was built on the runtime just shut down; drop it
    // off any runtime, as the mux's own runtime-shutdown test does.
    std::thread::spawn(move || drop(harness))
        .join()
        .expect("drop the harness");
}
