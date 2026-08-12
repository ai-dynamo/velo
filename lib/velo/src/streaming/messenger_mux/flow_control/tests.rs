// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Credit and byte-budget tests.
//!
//! The invariant tests at the bottom are the ones the mux's safety argument
//! rests on: a slot buffer sized `C + 1` can never overflow if spends are
//! gated, which is what lets the ingress applier `try_send` without ever
//! blocking its peer's ordering lane.

use super::*;

/// Limits with a small, easy-to-exhaust credit.
fn limits(credit: u32) -> NegotiatedLimits {
    NegotiatedLimits::from_wire(credit, 4096).expect("credit is non-zero")
}

// ---------------------------------------------------------------------------
// Negotiation
// ---------------------------------------------------------------------------

#[test]
fn zero_initial_credit_means_a_legacy_peer() {
    // `#[serde(default)]` makes an older peer deserialize as one advertising
    // nothing — which is exactly right, and must not be defaulted around.
    assert_eq!(
        NegotiatedLimits::from_wire(0, 0),
        Err(NegotiationError::LegacyPeer)
    );
    assert_eq!(
        NegotiatedLimits::from_wire(0, DEFAULT_SLOT_BYTE_BUDGET),
        Err(NegotiationError::LegacyPeer),
        "a byte budget cannot rescue a peer that granted no credit"
    );
}

#[test]
fn zero_slot_byte_budget_means_use_the_default() {
    // Deliberately *not* the same meaning as a zero initial credit.
    let negotiated = NegotiatedLimits::from_wire(32, 0).expect("usable");
    assert_eq!(negotiated.initial_credit(), 32);
    assert_eq!(negotiated.slot_byte_budget(), DEFAULT_SLOT_BYTE_BUDGET);
}

#[test]
fn advertised_limits_pass_through_unchanged() {
    let negotiated = NegotiatedLimits::from_wire(7, 2048).expect("usable");
    assert_eq!(negotiated.initial_credit(), 7);
    assert_eq!(negotiated.slot_byte_budget(), 2048);
    assert_eq!(negotiated.slot_buffer_depth(), 8);
    assert_eq!(negotiated.open_credit(), SlotCredit::new(7));
    assert_eq!(negotiated.open_account(), SlotCreditAccount::new(7));
}

#[test]
fn the_documented_defaults_are_what_batching_md_says() {
    assert_eq!(DEFAULT_PEER_BYTE_BUDGET, 8 * 1024 * 1024);
    assert_eq!(DEFAULT_SLOT_BYTE_BUDGET, 1024 * 1024);
    assert_eq!(ByteBudget::per_peer().limit(), DEFAULT_PEER_BYTE_BUDGET);
    assert_eq!(
        ByteBudget::per_slot(&limits(1)).limit(),
        u64::from(limits(1).slot_byte_budget())
    );
}

// ---------------------------------------------------------------------------
// Credit classes
// ---------------------------------------------------------------------------

#[test]
fn records_classify_onto_the_right_reservation() {
    use RecordType::{CloseSlot, CreditUpdate, Data, OpenSlot, SlotHeartbeat};

    assert_eq!(CreditClass::of(Data, false), CreditClass::Data);
    assert_eq!(CreditClass::of(Data, true), CreditClass::Terminal);

    for control in [OpenSlot, CloseSlot, CreditUpdate] {
        assert_eq!(CreditClass::of(control, false), CreditClass::Control);
        assert_eq!(
            CreditClass::of(control, true),
            CreditClass::Control,
            "a control record cannot be a terminal sentinel"
        );
        assert!(control.is_control());
    }

    // A heartbeat spends data credit and is therefore droppable under
    // saturation. That drop *is* the per-slot saturation signal `reader_pump`'s
    // watchdog fires on; a reserve here would delete it.
    assert_eq!(CreditClass::of(SlotHeartbeat, false), CreditClass::Data);
    assert_eq!(CreditClass::of(SlotHeartbeat, true), CreditClass::Data);
}

#[test]
fn only_control_records_stay_out_of_the_slot_buffer() {
    assert!(CreditClass::Data.occupies_buffer());
    assert!(CreditClass::Terminal.occupies_buffer());
    assert!(!CreditClass::Control.occupies_buffer());
}

// ---------------------------------------------------------------------------
// Egress ledger
// ---------------------------------------------------------------------------

#[test]
fn data_credit_spends_down_to_exhaustion() {
    let mut credit = SlotCredit::new(3);
    assert_eq!(credit.data_available(), 3);

    for remaining in (0..3).rev() {
        assert!(credit.can_spend(CreditClass::Data));
        assert_eq!(credit.try_spend(CreditClass::Data), Ok(()));
        assert_eq!(credit.data_available(), remaining);
    }

    assert!(!credit.can_spend(CreditClass::Data));
    assert_eq!(
        credit.try_spend(CreditClass::Data),
        Err(CreditError::DataExhausted)
    );
    assert_eq!(credit.data_available(), 0, "a refused spend costs nothing");
}

#[test]
fn a_zero_credit_ledger_can_still_send_control_and_a_terminal() {
    let mut credit = SlotCredit::new(0);
    assert_eq!(
        credit.try_spend(CreditClass::Data),
        Err(CreditError::DataExhausted)
    );

    // The two reservations are exactly what keep a starved slot able to finish
    // and to close.
    assert!(credit.can_spend(CreditClass::Control));
    assert_eq!(credit.try_spend(CreditClass::Control), Ok(()));
    assert!(credit.can_spend(CreditClass::Terminal));
    assert_eq!(credit.try_spend(CreditClass::Terminal), Ok(()));
}

#[test]
fn control_records_are_never_blocked_however_often_they_are_sent() {
    let mut credit = SlotCredit::new(0);
    for _ in 0..1_000 {
        assert_eq!(credit.try_spend(CreditClass::Control), Ok(()));
    }
    assert_eq!(credit.data_available(), 0);
    assert!(credit.terminal_available(), "control spends nothing");
}

#[test]
fn the_terminal_reserve_is_spendable_exactly_once() {
    let mut credit = SlotCredit::new(5);
    assert!(credit.terminal_available());
    assert_eq!(credit.try_spend(CreditClass::Terminal), Ok(()));
    assert!(!credit.terminal_available());
    assert!(!credit.can_spend(CreditClass::Terminal));
    assert_eq!(
        credit.try_spend(CreditClass::Terminal),
        Err(CreditError::TerminalAlreadySpent)
    );
    assert_eq!(
        credit.data_available(),
        5,
        "the terminal never touches data credit"
    );
}

#[test]
fn a_credit_update_restores_sending_capacity() {
    let mut credit = SlotCredit::new(1);
    assert_eq!(credit.try_spend(CreditClass::Data), Ok(()));
    assert_eq!(
        credit.try_spend(CreditClass::Data),
        Err(CreditError::DataExhausted)
    );

    assert_eq!(credit.grant(4), 4);
    assert_eq!(credit.try_spend(CreditClass::Data), Ok(()));
    assert_eq!(credit.data_available(), 3);
}

#[test]
fn a_grant_cannot_be_made_to_wrap() {
    let mut credit = SlotCredit::new(u32::MAX - 1);
    assert_eq!(credit.grant(10), u32::MAX);
    assert_eq!(credit.grant(u32::MAX), u32::MAX);
}

// ---------------------------------------------------------------------------
// Ingress account
// ---------------------------------------------------------------------------

#[test]
fn admitting_data_is_refused_past_the_granted_limit() {
    let mut account = SlotCreditAccount::new(2);
    assert_eq!(account.limit(), 2);
    assert_eq!(account.data_free(), 2);

    assert_eq!(account.admit(CreditClass::Data), Ok(()));
    assert_eq!(account.admit(CreditClass::Data), Ok(()));
    assert_eq!(account.data_outstanding(), 2);
    assert_eq!(account.data_free(), 0);
    assert_eq!(account.buffered(), 2);

    // A third data record means the peer overspent its grant: a protocol
    // error scoped to this slot, not a stall.
    assert_eq!(
        account.admit(CreditClass::Data),
        Err(CreditError::DataExhausted)
    );
    assert_eq!(account.buffered(), 2, "a refused admit occupies nothing");
}

#[test]
fn control_records_admit_without_occupying_the_buffer() {
    let mut account = SlotCreditAccount::new(1);
    for _ in 0..100 {
        assert_eq!(account.admit(CreditClass::Control), Ok(()));
    }
    assert_eq!(account.buffered(), 0);
    assert_eq!(account.data_outstanding(), 0);
    assert_eq!(account.pending_grant(), 0);
}

#[test]
fn a_saturated_slot_still_has_room_for_its_terminal() {
    let mut account = SlotCreditAccount::new(2);
    assert_eq!(account.admit(CreditClass::Data), Ok(()));
    assert_eq!(account.admit(CreditClass::Data), Ok(()));
    assert_eq!(
        account.admit(CreditClass::Data),
        Err(CreditError::DataExhausted)
    );

    assert_eq!(account.admit(CreditClass::Terminal), Ok(()));
    assert_eq!(account.buffered(), 3);
    assert_eq!(account.buffered() as usize, account.buffer_depth());

    assert_eq!(
        account.admit(CreditClass::Terminal),
        Err(CreditError::TerminalAlreadySpent)
    );
    assert_eq!(account.buffered(), 3);
}

#[test]
fn draining_data_records_accrues_credit_to_advertise() {
    let mut account = SlotCreditAccount::new(4);
    for _ in 0..4 {
        assert_eq!(account.admit(CreditClass::Data), Ok(()));
    }

    assert_eq!(account.release(1), 1);
    assert_eq!(account.release(2), 3);
    assert_eq!(account.buffered(), 1);
    assert_eq!(account.data_outstanding(), 1);
    assert_eq!(account.data_free(), 3);

    assert_eq!(account.take_pending_grant(), Some(3));
    assert_eq!(account.take_pending_grant(), None, "the delta is drained");
    assert_eq!(account.pending_grant(), 0);
}

#[test]
fn draining_the_terminal_returns_no_credit() {
    let mut account = SlotCreditAccount::new(1);
    assert_eq!(account.admit(CreditClass::Data), Ok(()));
    assert_eq!(account.admit(CreditClass::Terminal), Ok(()));
    assert_eq!(account.buffered(), 2);

    // Both drain; only the data record is worth granting back, because the
    // slot closes behind the terminal.
    assert_eq!(account.release(2), 1);
    assert_eq!(account.buffered(), 0);
    assert_eq!(account.take_pending_grant(), Some(1));
}

#[test]
fn an_over_release_cannot_manufacture_credit() {
    let mut account = SlotCreditAccount::new(3);
    assert_eq!(account.admit(CreditClass::Data), Ok(()));

    assert_eq!(account.release(u32::MAX), 1);
    assert_eq!(account.buffered(), 0);
    assert_eq!(account.data_outstanding(), 0);
    assert_eq!(account.data_free(), 3, "never more than was granted");
}

// ---------------------------------------------------------------------------
// The C + 1 invariant
// ---------------------------------------------------------------------------

#[test]
fn the_slot_buffer_depth_is_c_plus_one() {
    assert_eq!(slot_buffer_depth(0), 1);
    assert_eq!(slot_buffer_depth(1), 2);
    assert_eq!(slot_buffer_depth(255), 256);
    // The u32 -> usize widening is what makes the `+ 1` unable to overflow.
    assert_eq!(slot_buffer_depth(u32::MAX), u32::MAX as usize + 1);
}

#[test]
fn a_gated_slot_never_exceeds_its_buffer_depth() {
    // The applier's proof, walked exhaustively for small C: whatever order
    // admits and releases arrive in, occupancy never passes `C + 1`, so
    // `try_send` into a `C + 1`-deep buffer cannot fail and the lane never
    // blocks.
    for limit in 0..=4u32 {
        let mut account = SlotCreditAccount::new(limit);
        let depth = account.buffer_depth();

        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 33) as u32
        };

        for _ in 0..10_000 {
            match next() % 4 {
                0 => {
                    let _ = account.admit(CreditClass::Data);
                }
                1 => {
                    let _ = account.admit(CreditClass::Terminal);
                }
                2 => {
                    let _ = account.admit(CreditClass::Control);
                }
                _ => {
                    account.release(next() % 3);
                }
            }

            assert!(
                account.buffered() as usize <= depth,
                "C = {limit}: buffered {} exceeded depth {depth}",
                account.buffered()
            );
            assert!(account.data_outstanding() <= limit);
            assert!(account.data_free() <= limit);
        }
    }
}

#[test]
fn the_egress_ledger_cannot_outspend_the_ingress_buffer() {
    // The two ledgers are opened from the same negotiated limits, so what the
    // sender is allowed to put on the wire is exactly what the receiver sized
    // its buffer for. This is the join between the two halves of the proof.
    // `C = 0` is unreachable through negotiation — it is the legacy-peer
    // signal — so the smallest negotiable slot is the one-credit slot.
    for credit in [1u32, 2, 8, 64] {
        let negotiated = limits(credit);
        let mut ledger = negotiated.open_credit();
        let mut account = negotiated.open_account();

        let mut admitted = 0usize;
        while ledger.try_spend(CreditClass::Data).is_ok() {
            assert_eq!(account.admit(CreditClass::Data), Ok(()));
            admitted += 1;
        }
        assert_eq!(admitted, credit as usize);

        assert_eq!(ledger.try_spend(CreditClass::Terminal), Ok(()));
        assert_eq!(account.admit(CreditClass::Terminal), Ok(()));
        admitted += 1;

        assert_eq!(admitted, negotiated.slot_buffer_depth());
        assert_eq!(admitted, account.buffer_depth());
        assert_eq!(account.buffered() as usize, admitted);
    }
}

// ---------------------------------------------------------------------------
// Byte budgets
// ---------------------------------------------------------------------------

#[test]
fn reservations_add_up_and_release_back() {
    let mut budget = ByteBudget::new(100);
    assert_eq!((budget.used(), budget.available()), (0, 100));

    assert_eq!(budget.try_reserve(60), Ok(()));
    assert_eq!((budget.used(), budget.available()), (60, 40));

    assert_eq!(budget.try_reserve(40), Ok(()));
    assert_eq!((budget.used(), budget.available()), (100, 0));

    budget.release(100);
    assert_eq!((budget.used(), budget.available()), (0, 100));
}

#[test]
fn a_reservation_that_does_not_fit_right_now_is_transient() {
    let mut budget = ByteBudget::new(100);
    assert_eq!(budget.try_reserve(80), Ok(()));

    let err = budget.try_reserve(30).expect_err("no room");
    assert_eq!(
        err,
        ByteBudgetError::Exhausted {
            requested: 30,
            available: 20,
            limit: 100,
        }
    );
    assert!(err.is_transient(), "draining will admit it");
    assert_eq!(budget.used(), 80, "a refused reservation costs nothing");

    budget.release(80);
    assert_eq!(budget.try_reserve(30), Ok(()));
}

#[test]
fn a_reservation_larger_than_the_budget_is_permanent() {
    let mut budget = ByteBudget::new(100);
    let err = budget.try_reserve(101).expect_err("never fits");
    assert_eq!(
        err,
        ByteBudgetError::ExceedsBudget {
            requested: 101,
            limit: 100,
        }
    );
    assert!(
        !err.is_transient(),
        "parking on this would wait forever; it belongs on the rendezvous path"
    );
    assert_eq!(budget.used(), 0);

    // Exactly the limit still fits — the boundary is inclusive.
    assert_eq!(budget.try_reserve(100), Ok(()));
}

#[test]
fn an_over_release_cannot_drive_usage_negative() {
    let mut budget = ByteBudget::new(100);
    assert_eq!(budget.try_reserve(10), Ok(()));
    budget.release(1_000);
    assert_eq!(budget.used(), 0);
    assert_eq!(budget.available(), 100);
}

#[test]
fn a_paired_reservation_rolls_the_slot_back_when_the_peer_refuses() {
    let mut peer = ByteBudget::new(64);
    let mut slot = ByteBudget::new(128);
    assert_eq!(try_reserve_pair(&mut peer, &mut slot, 50), Ok(()));

    let err = try_reserve_pair(&mut peer, &mut slot, 30).expect_err("peer is full");
    assert!(err.is_transient());
    // The leak this guards against: a slot reservation outliving a failed peer
    // reservation, invisible to the live-slots gauge for the life of the epoch.
    assert_eq!(slot.used(), 50, "slot rolled back");
    assert_eq!(peer.used(), 50, "peer untouched");
}

#[test]
fn a_paired_reservation_refused_by_the_slot_leaves_the_peer_alone() {
    let mut peer = ByteBudget::new(1_024);
    let mut slot = ByteBudget::new(64);

    let err = try_reserve_pair(&mut peer, &mut slot, 65).expect_err("over the slot cap");
    assert!(!err.is_transient());
    assert_eq!(peer.used(), 0);
    assert_eq!(slot.used(), 0);
}

#[test]
fn releasing_a_pair_returns_both_scopes() {
    let mut peer = ByteBudget::per_peer();
    let mut slot = ByteBudget::per_slot(&limits(4));

    assert_eq!(try_reserve_pair(&mut peer, &mut slot, 2_048), Ok(()));
    assert_eq!((peer.used(), slot.used()), (2_048, 2_048));

    release_pair(&mut peer, &mut slot, 2_048);
    assert_eq!((peer.used(), slot.used()), (0, 0));
    assert_eq!(peer.available(), DEFAULT_PEER_BYTE_BUDGET);
}

#[test]
fn many_slots_cannot_together_exceed_the_peer_budget() {
    // Frame credit alone bounds memory at `slots x C x max frame size`; this
    // is the number that makes it finite.
    let mut peer = ByteBudget::new(1_000);
    let mut slots: Vec<ByteBudget> = (0..10).map(|_| ByteBudget::new(500)).collect();

    let mut admitted = 0u64;
    for slot in &mut slots {
        while try_reserve_pair(&mut peer, slot, 100).is_ok() {
            admitted += 100;
        }
    }

    assert_eq!(
        admitted, 1_000,
        "the peer budget binds before the slot caps"
    );
    assert_eq!(peer.used(), 1_000);
    assert_eq!(peer.available(), 0);
    assert_eq!(
        slots.iter().map(ByteBudget::used).sum::<u64>(),
        peer.used(),
        "the two scopes agree on what is outstanding"
    );
}
