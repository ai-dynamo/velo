// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Who may mutate what a registration owns.
//!
//! The hub alone holds credentials. [`Authority::authorize`] checks one and
//! returns the [`Lease`] it grants: the incarnation and epoch of the
//! registration the credential belongs to. Extensions fence their own
//! per-owner state with the lease's epoch.
//!
//! The state machine below is ported from kvbm-hub's
//! `RegistrationCredentials`, transition for transition. One owner moves
//! through:
//!
//! ```text
//!   (none) --begin_registration--> Registering --commit--> Active
//!   Active --begin_registration--> Registering --abort--> Active (prior)
//!   Active --begin_unregister----> Unregistering
//!   Active | Unregistering --store removal--> Revoking --finish--> (none)
//!   (none) --store removal before snapshot sync--> Removed
//!   snapshot sync: store entries the hub did not register --> Reserved
//! ```
//!
//! The transitions run on one map behind one mutex, exactly as in kvbm, so
//! a transition that spans owners (`synchronize_reservations`) stays atomic
//! against the removal hook. After each transition the touched owner's
//! read view is copied into a sharded map, and `authorize` reads only that:
//! one shard read lock, one lookup, a 16-byte constant-time compare, and an
//! atomic load for continuity. Measured by `authority::tests::authorize_cost`
//! (release, 10,000 owners, 20-core host): 37 ns per call on one thread;
//! 8 threads, 153 ns per call and 52 M calls/s in total. With the map behind
//! one `RwLock` instead, 8 threads took 847 ns per call and 9.4 M calls/s in
//! total, fewer than one thread did, because every reader writes the lock's
//! one cache line.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;
use velo::InstanceId;

use crate::protocol::{Credential, RegistrationEpoch};
use crate::store::{Continuity, CredentialDigest, Incarnation, Removal, StoredRegistration};

/// What a valid credential grants: the registration it belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lease {
    /// The registration's store incarnation.
    pub incarnation: Incarnation,
    /// The registration's epoch. An extension applies an owner-scoped
    /// mutation only if this equals the epoch it committed for the owner.
    pub epoch: RegistrationEpoch,
}

/// Why a credential check failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthorityError {
    /// The credential is missing, stale, or another owner's, or the entry
    /// belongs to no registration this hub made.
    #[error("credential does not authorize instance {owner}")]
    Unauthorized {
        /// The owner named in the request.
        owner: InstanceId,
    },
    /// Another mutation of this owner is in progress, or its removal is
    /// still being delivered. Retry.
    #[error("a mutation of instance {owner} is in progress")]
    Busy {
        /// The owner named in the request.
        owner: InstanceId,
    },
    /// The owner has no registration.
    #[error("instance {owner} has no registration")]
    NotFound {
        /// The owner named in the request.
        owner: InstanceId,
    },
    /// The owner's state changed under the transaction, for example the
    /// incarnation it wrote was removed.
    #[error("registration state of instance {owner} changed")]
    StateChanged {
        /// The owner.
        owner: InstanceId,
    },
    /// The hub restarted and holds the owner's registration from before.
    /// The presented credential may own it: register again presenting it.
    #[error("the hub restarted; register instance {owner} again with its credential")]
    Recovered {
        /// The owner named in the request.
        owner: InstanceId,
    },
    /// The store is not [`Continuity::Live`], so cached authority may be
    /// stale. Fails closed until it is.
    #[error("the hub's store is not live; authority is unavailable")]
    Unavailable,
}

/// The hub's credential authority. Cheap to clone; every clone is the same
/// authority.
#[derive(Clone, Default)]
pub struct Authority {
    inner: Arc<Inner>,
}

struct Inner {
    /// The source of truth. Every transition holds this lock.
    states: Mutex<HashMap<InstanceId, State>>,
    /// What `authorize` reads. Written only while `states` is held, right
    /// after a transition, so each owner's view is a state it was in.
    views: DashMap<InstanceId, View>,
    live: AtomicBool,
    /// Set by the first `synchronize_reservations`. Before it, a removal of
    /// an owner the hub has no state for is recorded, so the sync does not
    /// reserve an entry already gone; after it, such a removal (an entry
    /// the hub never held) changes nothing and leaves no record.
    synced: AtomicBool,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            views: DashMap::new(),
            live: AtomicBool::new(true),
            synced: AtomicBool::new(false),
        }
    }
}

/// An owner's state as `authorize` sees it. No entry: not found.
enum View {
    Active {
        credential: Credential,
        lease: Lease,
    },
    Reserved,
    Busy,
}

impl View {
    fn of(state: &State) -> Option<Self> {
        match state {
            State::Removed(_) => None,
            State::Reserved(_) => Some(Self::Reserved),
            State::Active(registration) => Some(Self::Active {
                credential: registration.credential.clone(),
                lease: Lease {
                    incarnation: registration.incarnation,
                    epoch: registration.epoch,
                },
            }),
            State::Registering(_) | State::Unregistering(_) | State::Revoking(_) => {
                Some(Self::Busy)
            }
        }
    }
}

/// The state map, held for one owner's transition. Dropping it publishes
/// that owner's new view, on every return path.
struct Transition<'a> {
    inner: &'a Inner,
    states: parking_lot::MutexGuard<'a, HashMap<InstanceId, State>>,
    owner: InstanceId,
}

impl std::ops::Deref for Transition<'_> {
    type Target = HashMap<InstanceId, State>;

    fn deref(&self) -> &Self::Target {
        &self.states
    }
}

impl std::ops::DerefMut for Transition<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.states
    }
}

impl Drop for Transition<'_> {
    fn drop(&mut self) {
        self.inner.publish(&self.states, self.owner);
    }
}

impl Inner {
    fn transition(&self, owner: InstanceId) -> Transition<'_> {
        Transition {
            inner: self,
            states: self.states.lock(),
            owner,
        }
    }

    /// Copy `owner`'s state into the read view. Call with `states` held.
    fn publish(&self, states: &HashMap<InstanceId, State>, owner: InstanceId) {
        match states.get(&owner).and_then(View::of) {
            Some(view) => {
                self.views.insert(owner, view);
            }
            None => {
                self.views.remove(&owner);
            }
        }
    }
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("owners", &self.inner.states.lock().len())
            .field("live", &self.inner.live.load(Ordering::Relaxed))
            .finish()
    }
}

enum State {
    /// A removal arrived after the hook was installed but before the
    /// snapshot was synchronized into reservations.
    Removed(Incarnation),
    /// A store entry this hub did not register (preloaded, or its own).
    Reserved(Incarnation),
    Active(Committed),
    Registering(InProgress),
    Unregistering(Committed),
    Revoking(Incarnation),
}

struct InProgress {
    credential: Credential,
    epoch: RegistrationEpoch,
    attempted: Option<Incarnation>,
    removed: HashSet<Incarnation>,
}

#[derive(Clone)]
struct Committed {
    credential: Credential,
    epoch: RegistrationEpoch,
    incarnation: Incarnation,
}

/// The right to finish one registration. Held by the transaction.
pub(crate) struct RegistrationPermit {
    owner: InstanceId,
    credential: Credential,
    epoch: RegistrationEpoch,
    previous: Option<Committed>,
    /// `previous` is a stored registration this hub recovered, not one it
    /// issued: an abort returns the owner to `Reserved`, not `Active`.
    recovered: bool,
}

impl RegistrationPermit {
    pub(crate) fn credential(&self) -> &Credential {
        &self.credential
    }

    pub(crate) fn epoch(&self) -> RegistrationEpoch {
        self.epoch
    }

    /// The registration this one replaces, if any.
    pub(crate) fn previous(&self) -> Option<Lease> {
        self.previous.as_ref().map(|prior| Lease {
            incarnation: prior.incarnation,
            epoch: prior.epoch,
        })
    }
}

/// The right to finish one unregistration.
pub(crate) struct UnregisterPermit {
    owner: InstanceId,
    registration: Committed,
}

impl UnregisterPermit {
    pub(crate) fn incarnation(&self) -> Incarnation {
        self.registration.incarnation
    }
}

/// How an aborted registration left the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortOutcome {
    /// The prior registration is active again (or there was none).
    Restored,
    /// The prior incarnation was removed meanwhile; its removal must now be
    /// delivered by the caller.
    Revoked(Incarnation),
}

/// What the removal hook must do about one removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RevocationAction {
    /// Nothing to deliver: no registration owned it, or a transaction in
    /// flight recorded it and will deliver it.
    Ignore,
    /// Authority is revoked; deliver the removal, then
    /// [`Authority::finish_revocation`].
    Cleanup,
}

impl Authority {
    /// Check `presented` for `owner`.
    pub fn authorize(
        &self,
        owner: InstanceId,
        presented: Option<&Credential>,
    ) -> Result<Lease, AuthorityError> {
        self.ensure_live()?;
        match self.inner.views.get(&owner).as_deref() {
            Some(View::Active { credential, lease }) if Some(credential) == presented => Ok(*lease),
            Some(View::Active { .. }) => Err(AuthorityError::Unauthorized { owner }),
            // No hashing here, on the hot path: the registration that
            // follows checks the credential against the stored record.
            Some(View::Reserved) if presented.is_some() => Err(AuthorityError::Recovered { owner }),
            Some(View::Reserved) => Err(AuthorityError::Unauthorized { owner }),
            Some(View::Busy) => Err(AuthorityError::Busy { owner }),
            None => Err(AuthorityError::NotFound { owner }),
        }
    }

    /// Whether the store is live, as last reported.
    pub fn is_live(&self) -> bool {
        self.inner.live.load(Ordering::Acquire)
    }

    pub(crate) fn set_continuity(&self, continuity: Continuity) {
        self.inner
            .live
            .store(continuity == Continuity::Live, Ordering::Release);
    }

    fn ensure_live(&self) -> Result<(), AuthorityError> {
        if self.is_live() {
            Ok(())
        } else {
            Err(AuthorityError::Unavailable)
        }
    }

    /// Reserve every store entry the hub did not register, so no client can
    /// claim it without a credential. A removal delivered between hook
    /// installation and this call suppresses its reservation. Call it once,
    /// with the hook's snapshot: only then does every removal since the
    /// snapshot reach it as a recorded `Removed`.
    pub(crate) fn synchronize_reservations(&self, snapshot: &[StoredRegistration]) {
        let current: HashMap<InstanceId, Incarnation> = snapshot
            .iter()
            .map(|stored| (stored.instance_id(), stored.incarnation))
            .collect();
        let mut states = self.inner.states.lock();
        // A removal of the snapshot's incarnation or a later one: the entry
        // the snapshot names is gone.
        let removed_before_sync: HashSet<InstanceId> = states
            .iter()
            .filter_map(|(owner, state)| match state {
                State::Removed(incarnation)
                    if current
                        .get(owner)
                        .is_some_and(|current| current <= incarnation) =>
                {
                    Some(*owner)
                }
                _ => None,
            })
            .collect();
        states.retain(|owner, state| match state {
            State::Removed(_) => false,
            State::Reserved(incarnation) => current.get(owner) == Some(incarnation),
            _ => true,
        });
        for (owner, incarnation) in current {
            if removed_before_sync.contains(&owner) {
                continue;
            }
            states.entry(owner).or_insert(State::Reserved(incarnation));
        }
        self.inner
            .views
            .retain(|owner, _| states.contains_key(owner));
        for owner in states.keys() {
            self.inner.publish(&states, *owner);
        }
        self.inner.synced.store(true, Ordering::Release);
    }

    /// `stored` is the store's record of `owner`, read under the
    /// registration lock; it lets a reserved owner back in (see
    /// [`recovers`](Self::recovers)).
    pub(crate) fn begin_registration(
        &self,
        owner: InstanceId,
        presented: Option<&Credential>,
        stored: Option<&StoredRegistration>,
    ) -> Result<RegistrationPermit, AuthorityError> {
        self.ensure_live()?;
        let mut states = self.inner.transition(owner);
        let recovered = matches!(states.get(&owner), Some(State::Reserved(_)));
        let previous = match states.get(&owner) {
            Some(State::Reserved(reserved)) => match Self::recovers(*reserved, presented, stored) {
                Some(previous) => Some(previous),
                None => return Err(AuthorityError::Unauthorized { owner }),
            },
            Some(State::Active(registration)) if Some(&registration.credential) != presented => {
                return Err(AuthorityError::Unauthorized { owner });
            }
            Some(State::Active(registration)) => Some(registration.clone()),
            Some(State::Removed(_)) if presented.is_some() => {
                return Err(AuthorityError::Unauthorized { owner });
            }
            Some(State::Removed(_)) => None,
            Some(State::Registering(_) | State::Unregistering(_) | State::Revoking(_)) => {
                return Err(AuthorityError::Busy { owner });
            }
            None if presented.is_some() => return Err(AuthorityError::Unauthorized { owner }),
            None => None,
        };
        let credential = Credential::generate();
        let epoch = RegistrationEpoch::new();
        states.insert(
            owner,
            State::Registering(InProgress {
                credential: credential.clone(),
                epoch,
                attempted: None,
                removed: HashSet::new(),
            }),
        );
        Ok(RegistrationPermit {
            owner,
            credential,
            epoch,
            previous,
            recovered,
        })
    }

    /// A reserved owner's way back in after a hub restart: the store holds
    /// its registration from before (`reserved`), and the presented
    /// credential's digest is one the record kept, either the one the
    /// registrant presented or the one that registration minted (its
    /// response may have been lost). The registration then proceeds as a
    /// re-registration of that one, and mints a new credential and epoch,
    /// so recovery works once. A bare or pinned record never recovers.
    fn recovers(
        reserved: Incarnation,
        presented: Option<&Credential>,
        stored: Option<&StoredRegistration>,
    ) -> Option<Committed> {
        let (presented, stored) = (presented?, stored?);
        let epoch = stored.record.epoch?;
        if stored.incarnation != reserved || stored.record.pinned {
            return None;
        }
        let digest = CredentialDigest::of(presented);
        let known = [stored.record.accepted, stored.record.pending];
        known.contains(&Some(digest)).then(|| Committed {
            credential: presented.clone(),
            epoch,
            incarnation: reserved,
        })
    }

    /// Record the incarnation the transaction's put produced. Fails if that
    /// incarnation was already removed.
    pub(crate) fn record_write(
        &self,
        permit: &RegistrationPermit,
        incarnation: Incarnation,
    ) -> Result<(), AuthorityError> {
        let mut states = self.inner.transition(permit.owner);
        let changed = AuthorityError::StateChanged {
            owner: permit.owner,
        };
        let Some(State::Registering(progress)) = states.get_mut(&permit.owner) else {
            return Err(changed);
        };
        if progress.credential != permit.credential {
            return Err(changed);
        }
        progress.attempted = Some(incarnation);
        if progress.removed.contains(&incarnation) {
            return Err(changed);
        }
        Ok(())
    }

    pub(crate) fn commit_registration(
        &self,
        permit: &RegistrationPermit,
        incarnation: Incarnation,
    ) -> Result<Lease, AuthorityError> {
        let mut states = self.inner.transition(permit.owner);
        let Some(State::Registering(progress)) = states.get(&permit.owner) else {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        };
        if progress.credential != permit.credential
            || progress.epoch != permit.epoch
            || progress.attempted != Some(incarnation)
            || progress.removed.contains(&incarnation)
        {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        }
        states.insert(
            permit.owner,
            State::Active(Committed {
                credential: permit.credential.clone(),
                epoch: permit.epoch,
                incarnation,
            }),
        );
        Ok(Lease {
            incarnation,
            epoch: permit.epoch,
        })
    }

    /// The removals recorded against `permit`'s registration, if the owner
    /// is still in it.
    fn removed_during<'s>(
        states: &'s HashMap<InstanceId, State>,
        permit: &RegistrationPermit,
    ) -> Result<&'s HashSet<Incarnation>, AuthorityError> {
        match states.get(&permit.owner) {
            Some(State::Registering(progress)) if progress.credential == permit.credential => {
                Ok(&progress.removed)
            }
            _ => Err(AuthorityError::StateChanged {
                owner: permit.owner,
            }),
        }
    }

    /// Abort a registration whose write did not apply, as far as the hub
    /// knows. The prior registration becomes active again unless it was
    /// removed meanwhile, or replaced: a recorded removal of a later
    /// incarnation is the transaction's own write, which applied after all
    /// and then left the store, taking the prior's place with it.
    pub(crate) fn abort_registration(
        &self,
        permit: RegistrationPermit,
    ) -> Result<AbortOutcome, AuthorityError> {
        let mut states = self.inner.transition(permit.owner);
        let removed = Self::removed_during(&states, &permit)?;
        match permit.previous {
            // A recovered registration was never this hub's to restore: the
            // owner goes back to reserved, where its credential still lets
            // it in by a registration that succeeds.
            Some(previous)
                if permit.recovered
                    && !removed.iter().any(|gone| *gone >= previous.incarnation) =>
            {
                states.insert(permit.owner, State::Reserved(previous.incarnation));
                Ok(AbortOutcome::Restored)
            }
            Some(previous) if !removed.iter().any(|gone| *gone >= previous.incarnation) => {
                states.insert(permit.owner, State::Active(previous));
                Ok(AbortOutcome::Restored)
            }
            Some(previous) => {
                states.remove(&permit.owner);
                Ok(AbortOutcome::Revoked(previous.incarnation))
            }
            None => {
                states.remove(&permit.owner);
                Ok(AbortOutcome::Restored)
            }
        }
    }

    /// Fail closed after the store write. If the attempted entry could not
    /// be removed, `reservation` keeps it reserved so nobody can claim it
    /// without a credential until it expires.
    pub(crate) fn fail_closed_registration(
        &self,
        permit: RegistrationPermit,
        reservation: Option<Incarnation>,
    ) -> Result<(), AuthorityError> {
        let mut states = self.inner.transition(permit.owner);
        let removed = Self::removed_during(&states, &permit)?;
        // The caller read `reservation` from the store before it settled; an
        // entry that left since then was recorded here, and nothing would
        // ever clear a reservation of it.
        let reservation = reservation.filter(|incarnation| !removed.contains(incarnation));
        match reservation {
            Some(incarnation) => {
                states.insert(permit.owner, State::Reserved(incarnation));
            }
            None => {
                states.remove(&permit.owner);
            }
        }
        Ok(())
    }

    pub(crate) fn begin_unregister(
        &self,
        owner: InstanceId,
        presented: Option<&Credential>,
    ) -> Result<UnregisterPermit, AuthorityError> {
        self.ensure_live()?;
        let mut states = self.inner.transition(owner);
        let registration = match states.get(&owner) {
            Some(State::Active(registration)) if Some(&registration.credential) == presented => {
                registration.clone()
            }
            Some(State::Active(_) | State::Reserved(_)) => {
                return Err(AuthorityError::Unauthorized { owner });
            }
            Some(State::Removed(_)) | None => return Err(AuthorityError::NotFound { owner }),
            Some(_) => return Err(AuthorityError::Busy { owner }),
        };
        states.insert(owner, State::Unregistering(registration.clone()));
        Ok(UnregisterPermit {
            owner,
            registration,
        })
    }

    /// Undo [`begin_unregister`](Self::begin_unregister) after a store
    /// failure, unless a removal arrived meanwhile.
    pub(crate) fn restore_unregister(&self, permit: UnregisterPermit) {
        let mut states = self.inner.transition(permit.owner);
        if matches!(
            states.get(&permit.owner),
            Some(State::Unregistering(registration))
                if registration.incarnation == permit.registration.incarnation
        ) {
            states.insert(permit.owner, State::Active(permit.registration));
        }
    }

    /// Revoke authority for a removed entry. Called by the store's removal
    /// hook, so it never blocks on anything but this map's lock.
    pub(crate) fn begin_revocation(&self, removal: Removal) -> RevocationAction {
        let mut states = self.inner.transition(removal.id);
        let owner = removal.id;
        let incarnation = removal.incarnation;
        match states.get_mut(&owner) {
            None => {
                if !self.inner.synced.load(Ordering::Acquire) {
                    states.insert(owner, State::Removed(incarnation));
                }
                RevocationAction::Ignore
            }
            // Any removal, not only of the reserved incarnation: a store holds
            // one entry per instance, so the reserved entry is gone even if a
            // write the hub lost track of replaced it first. Clearing cannot
            // hand an entry over: a claim without a credential writes with
            // `expected: None`, which fails while the store holds any entry.
            Some(State::Reserved(_)) => {
                states.remove(&owner);
                RevocationAction::Ignore
            }
            Some(State::Active(registration) | State::Unregistering(registration))
                if registration.incarnation == incarnation =>
            {
                states.insert(owner, State::Revoking(incarnation));
                RevocationAction::Cleanup
            }
            Some(State::Registering(progress)) => {
                progress.removed.insert(incarnation);
                RevocationAction::Ignore
            }
            _ => RevocationAction::Ignore,
        }
    }

    /// The removal was delivered; the owner may register again.
    pub(crate) fn finish_revocation(&self, removal: Removal) {
        let mut states = self.inner.transition(removal.id);
        if matches!(
            states.get(&removal.id),
            Some(State::Revoking(incarnation)) if *incarnation == removal.incarnation
        ) {
            states.remove(&removal.id);
        }
    }
}

#[cfg(test)]
mod tests;
