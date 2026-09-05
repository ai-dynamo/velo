// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The ingress-to-control seam for a bogus `OpenSlot` flood.
//!
//! `control.rs`'s own unit tests pin the bound `ControlInbox::reject_slot`
//! enforces once a `ReplyRecord::RejectSlot` reaches it. What they cannot
//! see is whether the ingress actually produces that variant for an
//! `OpenSlot` naming a slot it will never hold — the defect this file's
//! tests were added to catch named `entry_peer` refusing nothing while
//! `open_slot` fed it exactly that traffic through `ReplyRecord::CloseSlot`.
//! So this drives a real wire-encoded batch through `ingress::handle_batch`
//! and dispatches its `outcome.replies` the same way `deliver_batch` does,
//! proving the seam rather than either half of it in isolation.

use velo_ext::WorkerId;

use crate::streaming::messenger_mux::MuxConfig;
use crate::streaming::messenger_mux::ingress::{
    IngressRegistry, MAX_INGRESS_SLOTS_PER_PEER, handle_batch,
};
use crate::streaming::messenger_mux::peer_batcher::ReplyRecord;
use crate::streaming::messenger_mux::peer_batcher::control::{ControlInbox, MAX_PENDING_REJECTS};
use crate::streaming::messenger_mux::protocol::{BatchEncoder, SlotId};

fn peer() -> WorkerId {
    WorkerId::from_u64(0xF00D)
}

/// Send every `ReplyRecord` from one batch's outcome to `inbox`, exactly the
/// dispatch `BatcherHandle::reply` does.
fn apply_replies(inbox: &ControlInbox, replies: &[ReplyRecord]) {
    for record in replies {
        match *record {
            ReplyRecord::CreditUpdate { slot, delta } => inbox.reply_credit(slot, delta),
            ReplyRecord::CloseSlot { slot, reason } => inbox.reply_close(slot, reason),
            ReplyRecord::RejectSlot { slot, reason } => inbox.reject_slot(slot, reason),
        }
    }
}

/// A flood of out-of-range `OpenSlot`s in one batch produces `RejectSlot`
/// replies, and routing them into a batcher's `ControlInbox` the way
/// `deliver_batch` does stays bounded rather than growing one entry per
/// bogus record.
#[test]
fn a_bogus_open_slot_flood_reaches_control_as_a_bounded_reject() {
    let registry = IngressRegistry::default();
    let config = MuxConfig::default();

    // Comfortably past the cap the control-side lane enforces, and every
    // index is out of the ingress's own table range, so none of them can
    // ever become a held slot regardless of what this peer does next.
    const FLOOD: u32 = MAX_PENDING_REJECTS as u32 + 10_000;
    let mut encoder = BatchEncoder::new(1, 0);
    for i in 0..FLOOD {
        let id = SlotId::new(MAX_INGRESS_SLOTS_PER_PEER as u32 + i, 0).expect("index fits u24");
        encoder
            .push_open_slot(id, 0, 0xA, 0xB)
            .expect("record fits the batch");
    }
    let payload = encoder.finish().freeze();

    let outcome = handle_batch(&registry, &config, None, peer(), &payload);

    assert_eq!(
        outcome.replies.len(),
        FLOOD as usize,
        "every out-of-range OpenSlot gets a reply"
    );
    assert!(
        outcome
            .replies
            .iter()
            .all(|reply| matches!(reply, ReplyRecord::RejectSlot { .. })),
        "an OpenSlot the ingress never held must not become a `CloseSlot` reply — \
         that lane is unbounded on the control side by design, because a real \
         held-slot close or credit reply must never be refused"
    );

    let inbox = ControlInbox::default();
    apply_replies(&inbox, &outcome.replies);

    assert_eq!(
        inbox.pending_len(),
        MAX_PENDING_REJECTS,
        "the reject lane stops growing at its cap, unlike the credit lane it \
         merges into"
    );
    assert_eq!(
        inbox.refused(),
        FLOOD as u64 - MAX_PENDING_REJECTS as u64,
        "entries past the cap are refused and counted, not silently dropped \
         uncounted"
    );

    // The seam this file exists to pin does not stop at the inbox: a
    // `RejectSlot` only means anything once `drain` merges it into `peers`,
    // the map the batcher actually encodes replies from — see
    // `ControlState::drain`'s doc for why `rejects` merges rather than the
    // batcher reading it directly.
    let drained = inbox.take().expect("something pending");
    assert_eq!(
        drained.peers.len(),
        MAX_PENDING_REJECTS,
        "every capped rejection reaches the map the batcher encodes wire \
         replies from, not just the capped inbox lane"
    );
}
