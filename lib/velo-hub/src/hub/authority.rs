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
use crate::store::{Continuity, Incarnation, Removal, StoredRegistration};

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
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            views: DashMap::new(),
            live: AtomicBool::new(true),
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
            Some(View::Active { .. } | View::Reserved) => {
                Err(AuthorityError::Unauthorized { owner })
            }
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
    /// installation and this call suppresses its reservation.
    pub(crate) fn synchronize_reservations(&self, snapshot: &[StoredRegistration]) {
        let current: HashMap<InstanceId, Incarnation> = snapshot
            .iter()
            .map(|stored| (stored.instance_id(), stored.incarnation))
            .collect();
        let mut states = self.inner.states.lock();
        let removed_before_sync: HashSet<(InstanceId, Incarnation)> = states
            .iter()
            .filter_map(|(owner, state)| match state {
                State::Removed(incarnation) if current.get(owner) == Some(incarnation) => {
                    Some((*owner, *incarnation))
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
            if removed_before_sync.contains(&(owner, incarnation)) {
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
    }

    pub(crate) fn begin_registration(
        &self,
        owner: InstanceId,
        presented: Option<&Credential>,
    ) -> Result<RegistrationPermit, AuthorityError> {
        self.ensure_live()?;
        let mut states = self.inner.transition(owner);
        let previous = match states.get(&owner) {
            Some(State::Active(registration)) if Some(&registration.credential) != presented => {
                return Err(AuthorityError::Unauthorized { owner });
            }
            Some(State::Active(registration)) => Some(registration.clone()),
            Some(State::Reserved(_)) => return Err(AuthorityError::Unauthorized { owner }),
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

    /// Abort before the store write. The prior registration becomes active
    /// again unless its incarnation was removed meanwhile.
    pub(crate) fn abort_registration(
        &self,
        permit: RegistrationPermit,
    ) -> Result<AbortOutcome, AuthorityError> {
        let mut states = self.inner.transition(permit.owner);
        let Some(State::Registering(progress)) = states.get(&permit.owner) else {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        };
        if progress.credential != permit.credential {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        }
        match permit.previous {
            Some(previous) if !progress.removed.contains(&previous.incarnation) => {
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
        let Some(State::Registering(progress)) = states.get(&permit.owner) else {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        };
        if progress.credential != permit.credential {
            return Err(AuthorityError::StateChanged {
                owner: permit.owner,
            });
        }
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
                states.insert(owner, State::Removed(incarnation));
                RevocationAction::Ignore
            }
            Some(State::Reserved(current)) if *current == incarnation => {
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
