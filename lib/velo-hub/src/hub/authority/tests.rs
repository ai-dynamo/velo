// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `src/registration/tests.rs`. There, removal cleanup
//! ran inside the store's removal callback and the tests counted manager
//! calls. Here the state machine only says whether cleanup is due
//! ([`RevocationAction`], [`AbortOutcome`]) and the lifecycle task delivers
//! it, so the tests assert those answers and the Busy window directly.

use velo::{PeerInfo, WorkerAddress};

use super::*;
use crate::store::RegistrationRecord;

fn incarnation(value: u64) -> Incarnation {
    Incarnation::from_u64(value)
}

fn make_peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"registration-lifecycle-test".to_vec()),
    )
}

fn stored(peer: &PeerInfo, value: u64) -> StoredRegistration {
    StoredRegistration {
        record: RegistrationRecord::bare(peer.clone()),
        incarnation: incarnation(value),
    }
}

fn removal(peer: &PeerInfo, value: u64) -> Removal {
    Removal {
        id: peer.instance_id(),
        incarnation: incarnation(value),
    }
}

/// Register `peer` at `value` and return its credential.
fn register(
    authority: &Authority,
    peer: &PeerInfo,
    presented: Option<&Credential>,
    value: u64,
) -> Credential {
    let permit = authority
        .begin_registration(peer.instance_id(), presented, None)
        .unwrap();
    authority.record_write(&permit, incarnation(value)).unwrap();
    authority
        .commit_registration(&permit, incarnation(value))
        .unwrap();
    permit.credential().clone()
}

#[test]
fn reserved_registry_occupant_cannot_begin_client_registration() {
    let authority = Authority::default();
    let peer = make_peer();
    authority.synchronize_reservations(&[stored(&peer, 1)]);

    assert!(matches!(
        authority.begin_registration(peer.instance_id(), None, None),
        Err(AuthorityError::Unauthorized { .. })
    ));
}

#[test]
fn removal_before_startup_snapshot_sync_does_not_leave_phantom_reservation() {
    let authority = Authority::default();
    let peer = make_peer();

    assert_eq!(
        authority.begin_revocation(removal(&peer, 1)),
        RevocationAction::Ignore
    );
    authority.synchronize_reservations(&[stored(&peer, 1)]);

    authority
        .begin_registration(peer.instance_id(), None, None)
        .expect("a removal delivered before snapshot sync must suppress that stale reservation");
}

#[test]
fn stale_removal_does_not_revoke_replacement_registration() {
    let authority = Authority::default();
    let peer = make_peer();
    let first = register(&authority, &peer, None, 1);
    let replacement = register(&authority, &peer, Some(&first), 2);

    assert_eq!(
        authority.begin_revocation(removal(&peer, 1)),
        RevocationAction::Ignore,
        "a removal of the replaced incarnation must not trigger cleanup"
    );
    assert_eq!(
        authority
            .authorize(peer.instance_id(), Some(&replacement))
            .unwrap()
            .incarnation,
        incarnation(2)
    );
}

/// kvbm then aborted the permit and replayed the prior registration. Under
/// the B transaction a failure after the write fails closed instead; the
/// owner may register afresh.
#[test]
fn removal_of_attempted_incarnation_prevents_registration_commit() {
    let authority = Authority::default();
    let peer = make_peer();
    let permit = authority
        .begin_registration(peer.instance_id(), None, None)
        .unwrap();
    authority.record_write(&permit, incarnation(1)).unwrap();

    assert_eq!(
        authority.begin_revocation(removal(&peer, 1)),
        RevocationAction::Ignore,
        "the transaction in flight recorded the removal and delivers it"
    );

    assert!(matches!(
        authority.commit_registration(&permit, incarnation(1)),
        Err(AuthorityError::StateChanged { .. })
    ));
    authority.fail_closed_registration(permit, None).unwrap();
    authority
        .begin_registration(peer.instance_id(), None, None)
        .expect("a failed-closed owner registers afresh");
}

#[test]
fn fail_closed_rollback_reserves_a_registry_entry_that_could_not_be_removed() {
    let authority = Authority::default();
    let peer = make_peer();
    let permit = authority
        .begin_registration(peer.instance_id(), None, None)
        .unwrap();
    authority.record_write(&permit, incarnation(7)).unwrap();

    authority
        .fail_closed_registration(permit, Some(incarnation(7)))
        .unwrap();

    assert!(matches!(
        authority.begin_registration(peer.instance_id(), None, None),
        Err(AuthorityError::Unauthorized { .. })
    ));
}

#[test]
fn replacement_is_busy_until_matching_removal_cleanup_finishes() {
    let authority = Authority::default();
    let peer = make_peer();
    register(&authority, &peer, None, 1);

    assert_eq!(
        authority.begin_revocation(removal(&peer, 1)),
        RevocationAction::Cleanup
    );
    assert!(matches!(
        authority.begin_registration(peer.instance_id(), None, None),
        Err(AuthorityError::Busy { .. })
    ));

    authority.finish_revocation(removal(&peer, 1));
    authority
        .begin_registration(peer.instance_id(), None, None)
        .expect("a fresh lifecycle may begin only after cleanup completes");
}

// ---- new in velo-hub ---------------------------------------------------

/// Under B a failure before the write leaves the prior registration as it
/// was, unless the store removed it meanwhile; then the abort says the
/// removal is the caller's to deliver.
#[test]
fn abort_restores_the_prior_registration_unless_it_was_removed() {
    let authority = Authority::default();
    let peer = make_peer();
    let owner = peer.instance_id();
    let credential = register(&authority, &peer, None, 1);

    let permit = authority
        .begin_registration(owner, Some(&credential), None)
        .unwrap();
    assert_eq!(permit.previous().unwrap().incarnation, incarnation(1));
    assert_eq!(
        authority.abort_registration(permit).unwrap(),
        AbortOutcome::Restored
    );
    assert_eq!(
        authority
            .authorize(owner, Some(&credential))
            .unwrap()
            .incarnation,
        incarnation(1)
    );

    let permit = authority
        .begin_registration(owner, Some(&credential), None)
        .unwrap();
    assert_eq!(
        authority.begin_revocation(removal(&peer, 1)),
        RevocationAction::Ignore
    );
    assert_eq!(
        authority.abort_registration(permit).unwrap(),
        AbortOutcome::Revoked(incarnation(1))
    );
    assert!(matches!(
        authority.authorize(owner, Some(&credential)),
        Err(AuthorityError::NotFound { .. })
    ));
}

/// Mid-registration the old credential answers Busy, not Ok: this is what
/// keeps an extension mutation from landing between prepare and commit.
#[test]
fn authorize_is_busy_mid_registration() {
    let authority = Authority::default();
    let peer = make_peer();
    let credential = register(&authority, &peer, None, 1);
    let _permit = authority
        .begin_registration(peer.instance_id(), Some(&credential), None)
        .unwrap();
    assert!(matches!(
        authority.authorize(peer.instance_id(), Some(&credential)),
        Err(AuthorityError::Busy { .. })
    ));
}

/// While the store is not live, every credential check fails closed, and
/// recovers when it is live again.
#[test]
fn authority_fails_closed_while_the_store_is_not_live() {
    let authority = Authority::default();
    let peer = make_peer();
    let credential = register(&authority, &peer, None, 1);

    for continuity in [Continuity::Degraded, Continuity::Lost] {
        authority.set_continuity(continuity);
        assert_eq!(
            authority.authorize(peer.instance_id(), Some(&credential)),
            Err(AuthorityError::Unavailable)
        );
        assert!(matches!(
            authority.begin_registration(peer.instance_id(), Some(&credential), None),
            Err(AuthorityError::Unavailable)
        ));
        assert!(matches!(
            authority.begin_unregister(peer.instance_id(), Some(&credential)),
            Err(AuthorityError::Unavailable)
        ));
    }
    authority.set_continuity(Continuity::Live);
    authority
        .authorize(peer.instance_id(), Some(&credential))
        .unwrap();
}

#[test]
fn authorize_distinguishes_unknown_from_unauthorized() {
    let authority = Authority::default();
    let peer = make_peer();
    assert!(matches!(
        authority.authorize(peer.instance_id(), None),
        Err(AuthorityError::NotFound { .. })
    ));
    register(&authority, &peer, None, 1);
    assert!(matches!(
        authority.authorize(peer.instance_id(), None),
        Err(AuthorityError::Unauthorized { .. })
    ));
}

/// Cost of `authorize` on the request path. Not a gate: run it with
/// `cargo test --release -p velo-hub --features server authorize_cost -- --ignored --nocapture`.
#[test]
#[ignore = "measurement, not a check"]
fn authorize_cost() {
    const OWNERS: u64 = 10_000;
    const CALLS: u64 = 2_000_000;
    let authority = Authority::default();
    let mut owners = Vec::new();
    for value in 1..=OWNERS {
        let peer = make_peer();
        let credential = register(&authority, &peer, None, value);
        owners.push((peer.instance_id(), credential));
    }
    for threads in [1usize, 8] {
        let started = std::time::Instant::now();
        std::thread::scope(|scope| {
            for thread in 0..threads {
                let (authority, owners) = (&authority, &owners);
                scope.spawn(move || {
                    for call in 0..CALLS / threads as u64 {
                        let (owner, credential) =
                            &owners[((call as usize) * 7919 + thread) % owners.len()];
                        std::hint::black_box(
                            authority.authorize(*owner, Some(credential)).unwrap(),
                        );
                    }
                });
            }
        });
        let elapsed = started.elapsed();
        println!(
            "authorize: {threads} thread(s), {OWNERS} owners, {CALLS} calls: {:.1} ns/call per thread, {:.1} M calls/s total",
            elapsed.as_nanos() as f64 * threads as f64 / CALLS as f64,
            CALLS as f64 / elapsed.as_secs_f64() / 1e6
        );
    }
}

/// A reservation is for an entry the hub did not register. Any removal of
/// that owner means the entry left the store (a store holds one entry per
/// instance), even when it names another incarnation: a write the hub lost
/// track of may have replaced the reserved one before it expired. Keeping
/// the reservation would lock the owner out with no entry to clear it.
#[test]
fn any_removal_of_a_reserved_owner_clears_the_reservation() {
    let authority = Authority::default();
    let peer = make_peer();
    authority.synchronize_reservations(&[stored(&peer, 1)]);
    assert_eq!(
        authority.begin_revocation(removal(&peer, 2)),
        RevocationAction::Ignore
    );
    authority
        .begin_registration(peer.instance_id(), None, None)
        .expect("the reservation must go with the entry");
}

/// A recovery registration that aborts before its write (an extension
/// rejected it, the write did not apply) leaves the owner reserved: the
/// stored registration is not one this hub issued, so a failed attempt must
/// not make its credential authorize anything.
#[test]
fn an_aborted_recovery_registration_returns_the_owner_to_reserved() {
    let authority = Authority::default();
    let peer = make_peer();
    let owner = peer.instance_id();
    let credential = Credential::generate();
    let mut record = RegistrationRecord::bare(peer.clone());
    record.epoch = Some(RegistrationEpoch::new());
    record.accepted = Some(crate::store::CredentialDigest::of(&credential));
    let stored = StoredRegistration {
        record,
        incarnation: incarnation(1),
    };
    authority.synchronize_reservations(std::slice::from_ref(&stored));
    let permit = authority
        .begin_registration(owner, Some(&credential), Some(&stored))
        .unwrap();
    assert_eq!(
        authority.abort_registration(permit).unwrap(),
        AbortOutcome::Restored
    );
    assert!(matches!(
        authority.authorize(owner, Some(&credential)),
        Err(AuthorityError::Recovered { .. })
    ));
    assert!(matches!(
        authority.begin_unregister(owner, Some(&credential)),
        Err(AuthorityError::Unauthorized { .. })
    ));
    // Still recoverable afterwards.
    assert!(
        authority
            .begin_registration(owner, Some(&credential), Some(&stored))
            .is_ok()
    );
}
