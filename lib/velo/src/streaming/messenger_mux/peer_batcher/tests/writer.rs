// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Batch-sequence accounting in [`BatchWriter`].
//!
//! `batch_seq` is the receiver's only evidence that nothing was lost between two
//! batches: `note_batch_seq` meters the distance between what it expected and
//! what arrived. A sequence the writer reserves and then does not send would
//! read there as a batch that went missing, so every reserved sequence must
//! either reach the wire or be given back.
//!
//! Scope, stated plainly: this is an invariant of [`BatchWriter`], not a defect
//! the batcher can reach. Every batcher caller that meets a `None` from
//! `dispatch_singleton` fails the peer epoch immediately afterwards, and
//! `reset_epoch` restarts `batch_seq` at zero — so the refund is unobservable in
//! production today. The test drives the writer directly, which is the seam the
//! invariant lives at, and is what keeps a future caller that does *not* fail
//! the epoch honest.

use std::sync::Arc;

use super::super::writer::BatchWriter;
use super::support::{OwnedBatch, RECV_TIMEOUT, capture_pair};
use crate::streaming::messenger_mux::MuxConfig;
use crate::streaming::messenger_mux::protocol::{EncodeError, RecordType, SlotId};

/// The epoch every batch here is stamped with. Any value; it only has to be
/// stable, so a batch from another test cannot be mistaken for one of these.
const EPOCH: u64 = 7;

/// Opens, at the order of magnitude the response plane runs at (~3k/s).
const OPENS: u32 = 1000;

/// A refused encode must give its `batch_seq` back.
///
/// [`BatchWriter::dispatch_singleton`] reserves the sequence before it knows
/// whether the record will encode, exactly as [`BatchWriter::flush`] does — and
/// `flush` gives the sequence back when the batch turns out to be empty. The
/// singleton path has the same duty and two ways to leave without sending: an
/// encode that fails and a send builder that cannot be made. Neither may leave
/// a hole in the writer's numbering — see the module doc for why that is an
/// invariant asserted here rather than a gap production can currently reach.
#[tokio::test(flavor = "multi_thread")]
async fn open_slot_batch_seq_stays_contiguous_across_failures() {
    let (sender, capture, batches) = capture_pair().await;
    let peer = capture.instance_id().worker_id();
    let mut writer = BatchWriter::new(Arc::clone(&sender), peer, MuxConfig::default(), None, EPOCH);

    for n in 0..OPENS {
        // Every third open meets an encoder that refuses the record. Nothing
        // goes out, so the sequence it took is not a gap.
        if n % 3 == 0 {
            assert!(
                writer
                    .dispatch_singleton(|_| Err(EncodeError::BatchFull))
                    .is_none(),
                "a refused encode dispatches nothing"
            );
        }
        let slot = SlotId::from_raw(n);
        let fire = writer
            .dispatch_singleton(|encoder| encoder.push_open_slot(slot, 0, u64::from(n), 1))
            .expect("the open dispatches");
        fire.await.expect("admitted");
    }

    let mut seen: Vec<u32> = Vec::with_capacity(OPENS as usize);
    for n in 0..OPENS {
        let payload = tokio::time::timeout(RECV_TIMEOUT, batches.recv_async())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for open {n}"))
            .expect("capture channel closed");
        let batch = OwnedBatch::decode(&payload);
        assert_eq!(batch.header.peer_epoch, EPOCH);
        assert_eq!(batch.records[0].kind, RecordType::OpenSlot);
        seen.push(batch.header.batch_seq);
    }

    let expected: Vec<u32> = (0..OPENS).collect();
    assert_eq!(
        seen, expected,
        "a sequence reserved by a dispatch that sent nothing must be given back, \
         or every failure reads at the peer as a batch that went missing"
    );
}
