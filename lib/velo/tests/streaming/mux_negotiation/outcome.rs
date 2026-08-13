// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What each side of a settled attach can observe about the outcome.
//!
//! The suite next door asks which transport two nodes agree on. These ask what
//! either of them can then *see* of that agreement without reading the other's
//! metrics: the key the sender is handed, and the split that lets a node account
//! for the batches it packed apart from the ones packed for it.
//!
//! A submodule rather than more of `mod.rs` because it is a
//! different question about the same fixtures, which `use super::*` shares
//! whole.

use super::*;

/// The sender is told which transport its attach settled on, and it is told the
/// same thing the receiver recorded.
///
/// Both arms assert the pair, not the accessor alone: the receiver labels its
/// attach counter with the key it answered with, so a sender-side report that
/// disagreed with it would be a report of something else. Reading only the
/// sender would pass just as well against a hardcoded key.
#[tokio::test(flavor = "multi_thread")]
async fn a_sender_is_told_which_transport_the_attach_settled_on() {
    // Both sides have the mux, so the attach settles on it.
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(MUX_KEY)
    );
    assert_eq!(
        consumer.attaches_over(MUX_KEY),
        1.0,
        "the sender named {MUX_KEY}, so that is what the receiver must have answered"
    );
    drop((sender, anchor, consumer, producer));

    // The receiver has no mux, so the same sender settles on the legacy path.
    let (consumer, producer) = pair(None, Some(mux_config())).await;
    let anchor = consumer.velo.create_anchor::<u32>();
    let sender = producer
        .velo
        .attach_anchor::<u32>(transfer(anchor.handle()))
        .await
        .expect("remote attach");

    assert_eq!(
        sender.negotiated_transport().map(|key| key.as_str()),
        Some(LEGACY_KEY),
        "the sender advertised the mux, but the answer is the receiver's to give"
    );
    assert_eq!(consumer.attaches_over(LEGACY_KEY), 1.0);
}

/// A same-worker attach settles on nothing, because nothing was negotiated.
///
/// `None` here is the honest answer rather than a missing one: the frames go
/// straight into the anchor's channel and never reach a transport, so no key
/// would describe them — including this node's own mux, which is installed.
#[tokio::test(flavor = "multi_thread")]
async fn a_same_worker_attach_negotiates_no_transport() {
    let node = node(Some(mux_config())).await;
    let anchor = node.velo.create_anchor::<u32>();
    let sender = node
        .velo
        .attach_anchor::<u32>(anchor.handle())
        .await
        .expect("local attach");

    assert!(node.registers_mux());
    assert_eq!(sender.negotiated_transport(), None);
    assert_eq!(
        node.attaches_over(MUX_KEY) + node.attaches_over(LEGACY_KEY),
        0.0,
        "a local attach never reaches the attach handler that labels that counter"
    );
}

/// A node's own packing is counted apart from what its peers packed for it.
///
/// Every mux node is both ends at once — credit rides back on `_stream_batch`,
/// so even a pure consumer packs batches — and the per-batch record histogram
/// used to carry no direction, which made its sum the two mixed together and
/// attributable to neither. This asserts the split at the only place it is
/// visible: one registry, both series, each holding its own side of the same
/// stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_counts_the_records_it_packs_apart_from_the_ones_it_receives() {
    const FRAMES: u32 = 400;
    let (consumer, producer) = pair(Some(mux_config()), Some(mux_config())).await;

    stream_spsc(&consumer, &producer, FRAMES).await;

    // The consumer received the stream and packed credit for it.
    assert!(
        consumer.mux_records_received() >= f64::from(FRAMES),
        "the consumer received {} records for a {FRAMES}-frame stream",
        consumer.mux_records_received()
    );
    assert!(
        consumer.mux_records_sent() > 0.0,
        "the consumer returned credit on {} batches but packed no records \
         into them",
        consumer.mux_batches_sent()
    );

    // The producer is the same node the other way round: it packed the stream
    // and received the credit.
    assert!(producer.mux_records_sent() >= f64::from(FRAMES));
    assert!(producer.mux_records_received() > 0.0);

    // The four assertions above are what an unlabelled histogram fails: a read
    // for a label the family does not carry matches nothing and returns zero,
    // so all four lower bounds collapse at once. Asserting the two sums merely
    // *differ* would add nothing — control traffic can coincide with data
    // traffic, and a run where it did would fail for no reason.
}
