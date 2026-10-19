// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Service discovery: named services, each a set of registered instances.
//!
//! [`ServiceDirectory`] is a built-in extension (reserved name `services`):
//! every registration is prepared and committed through it, so an owner's
//! services follow the transaction's ordering and die with the owner on the
//! same removal path as every extension's state. Its routes are core routes
//! under `/v1/services`.
//!
//! - A credentialed re-registration keeps the owner's services (the peer
//!   still serves them); a fresh registration starts with none.
//! - `PUT`/`DELETE` of an instance authorize with the owner's credential and
//!   apply only if the lease's epoch is the one committed here, checked
//!   under this directory's lock with the write (the extension fence).
//! - A watch takes its snapshot and subscribes under that lock, so no
//!   change between the two is lost or seen twice.
//! - A watch ends with `disconnected` when the store leaves `Live`: the
//!   store may then miss removals, so the watcher's set may be stale.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use velo::InstanceId;

use super::extension::{Committed, Extension, RegistrationView, Rejection, Txn};
use super::{Authorized, HubError, Lease};
use crate::protocol::{
    self, RegistrationEpoch, WATCH_ADDED, WATCH_DISCONNECTED, WATCH_INITIAL, WATCH_REMOVED,
};
use crate::store::{Continuity, Incarnation, Removal};

/// The directory's reserved extension name.
pub(crate) const NAME: &str = protocol::SERVICES_EXTENSION;

/// Changes a watcher can fall behind by before its stream ends with
/// `disconnected`. tokio allocates the whole ring for each watched name, so
/// it stays small; a watcher that falls behind watches again either way.
const WATCH_BUFFER: usize = 128;

/// Interval of the SSE keep-alive comment, so a client's idle read timeout
/// tells a stalled stream from a quiet one.
pub(crate) const WATCH_KEEP_ALIVE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
enum Change {
    Added(InstanceId),
    Removed(InstanceId),
}

struct Owner {
    epoch: RegistrationEpoch,
    incarnation: Incarnation,
    names: BTreeSet<String>,
}

struct Service {
    instances: HashSet<InstanceId>,
    changes: broadcast::Sender<Change>,
}

impl Service {
    fn new() -> Self {
        Self {
            instances: HashSet::new(),
            changes: broadcast::channel(WATCH_BUFFER).0,
        }
    }
}

#[derive(Default)]
struct Directory {
    owners: HashMap<InstanceId, Owner>,
    services: HashMap<String, Service>,
}

impl Directory {
    fn add(&mut self, name: &str, id: InstanceId) {
        let service = self
            .services
            .entry(name.to_string())
            .or_insert_with(Service::new);
        if service.instances.insert(id) {
            let _ = service.changes.send(Change::Added(id));
        }
    }

    fn remove(&mut self, name: &str, id: InstanceId) {
        if let Some(service) = self.services.get_mut(name)
            && service.instances.remove(&id)
        {
            let _ = service.changes.send(Change::Removed(id));
        }
        self.prune(name);
    }

    /// Drop a service nobody serves or watches.
    fn prune(&mut self, name: &str) {
        if self.services.get(name).is_some_and(|service| {
            service.instances.is_empty() && service.changes.receiver_count() == 0
        }) {
            self.services.remove(name);
        }
    }

    fn drop_owner(&mut self, id: InstanceId) {
        if let Some(owner) = self.owners.remove(&id) {
            for name in &owner.names {
                self.remove(name, id);
            }
        }
    }

    /// The owner, if `lease` is its committed registration.
    fn fenced(&mut self, id: InstanceId, lease: &Lease) -> Result<&mut Owner, HubError> {
        self.owners
            .get_mut(&id)
            .filter(|owner| owner.epoch == lease.epoch)
            .ok_or_else(|| {
                HubError::new(
                    protocol::ErrorCode::Conflict,
                    format!("the registration of {id} changed while the request ran; retry"),
                )
            })
    }
}

/// Named services and their instances. See the module docs.
pub(crate) struct ServiceDirectory {
    state: parking_lot::Mutex<Directory>,
    /// Cancelled at shutdown: every watch stream ends, so the listener
    /// drain never waits out one.
    closed: CancellationToken,
}

impl Default for ServiceDirectory {
    fn default() -> Self {
        Self {
            state: parking_lot::Mutex::new(Directory::default()),
            closed: CancellationToken::new(),
        }
    }
}

impl ServiceDirectory {
    // Reads collect under the lock and sort after it: every registration's
    // `commit` takes this lock under the hub's registration lock.
    pub(crate) fn list(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .state
            .lock()
            .services
            .iter()
            .filter(|(_, service)| !service.instances.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        names.sort();
        names
    }

    pub(crate) fn instances(&self, name: &str) -> Vec<InstanceId> {
        let instances: Vec<InstanceId> = self
            .state
            .lock()
            .services
            .get(name)
            .map(|service| service.instances.iter().copied().collect())
            .unwrap_or_default();
        sorted(instances)
    }

    pub(crate) fn put(&self, name: &str, auth: &Authorized) -> Result<(), HubError> {
        let mut state = self.state.lock();
        let owner = state.fenced(auth.owner, &auth.lease)?;
        owner.names.insert(name.to_string());
        state.add(name, auth.owner);
        Ok(())
    }

    pub(crate) fn delete(&self, name: &str, auth: &Authorized) -> Result<(), HubError> {
        let mut state = self.state.lock();
        let owner = state.fenced(auth.owner, &auth.lease)?;
        owner.names.remove(name);
        state.remove(name, auth.owner);
        Ok(())
    }

    /// An SSE stream of `name`: `initial`, then `added` / `removed`, until
    /// `continuity` leaves `Live`.
    pub(crate) fn watch(
        self: &Arc<Self>,
        name: &str,
        continuity: tokio::sync::watch::Receiver<Continuity>,
    ) -> Sse<impl Stream<Item = Result<Event, Infallible>> + Send + use<>> {
        let (initial, changes) = {
            let mut state = self.state.lock();
            let service = state
                .services
                .entry(name.to_string())
                .or_insert_with(Service::new);
            let initial: Vec<InstanceId> = service.instances.iter().copied().collect();
            (initial, service.changes.subscribe())
        };
        let initial = sorted(initial);
        let watch = Watch {
            directory: Arc::clone(self),
            name: name.to_string(),
            changes: Some(changes),
            initial: Some(initial),
            closed: self.closed.clone(),
            continuity,
        };
        let stream = futures::stream::unfold(watch, |mut watch| async move {
            let event = watch.next().await?;
            Some((Ok(event), watch))
        });
        Sse::new(stream).keep_alive(KeepAlive::new().interval(WATCH_KEEP_ALIVE))
    }
}

/// One watch stream. Dropping it unsubscribes and prunes the service if
/// nobody serves or watches it any more.
struct Watch {
    directory: Arc<ServiceDirectory>,
    name: String,
    changes: Option<broadcast::Receiver<Change>>,
    initial: Option<Vec<InstanceId>>,
    closed: CancellationToken,
    continuity: tokio::sync::watch::Receiver<Continuity>,
}

fn event(name: &str, data: &impl serde::Serialize) -> Event {
    Event::default()
        .event(name)
        .json_data(data)
        .expect("ids serialize")
}

impl Watch {
    async fn next(&mut self) -> Option<Event> {
        if let Some(initial) = self.initial.take() {
            return Some(event(WATCH_INITIAL, &initial));
        }
        let changes = self.changes.as_mut()?;
        let continuity = &mut self.continuity;
        let received = tokio::select! {
            _ = self.closed.cancelled() => None,
            // A dropped sender means the store is gone: stale too.
            _ = continuity.wait_for(|now| *now != Continuity::Live) => {
                self.changes = None;
                return Some(event(WATCH_DISCONNECTED, &()));
            }
            received = changes.recv() => Some(received),
        };
        match received {
            Some(Ok(Change::Added(id))) => Some(event(WATCH_ADDED, &id)),
            Some(Ok(Change::Removed(id))) => Some(event(WATCH_REMOVED, &id)),
            // Fell behind: the watcher's set is stale. Say so, then end.
            Some(Err(broadcast::error::RecvError::Lagged(_))) => {
                self.changes = None;
                Some(event(WATCH_DISCONNECTED, &()))
            }
            // The hub stopped, or the service is gone (it cannot be while
            // this receiver lives): end without an event.
            Some(Err(broadcast::error::RecvError::Closed)) | None => None,
        }
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        drop(self.changes.take());
        self.directory.state.lock().prune(&self.name);
    }
}

impl Extension for ServiceDirectory {
    const NAME: &'static str = NAME;
    /// The names the registrant serves. With a payload, the registration
    /// sets the owner's services to exactly these, so a client that sends
    /// its names on every registration never diverges from the hub.
    type Registration = BTreeSet<String>;
    type Staged = Option<BTreeSet<String>>;

    async fn shutdown(&self) {
        self.closed.cancel();
    }

    fn admit(
        &self,
        _view: &RegistrationView<'_>,
        payload: Option<&BTreeSet<String>>,
    ) -> Result<(), Rejection> {
        match payload
            .into_iter()
            .flatten()
            .find(|name| !protocol::is_valid_service_name(name))
        {
            Some(name) => Err(Rejection::bad_request(format!(
                "{name:?} is not a service name: {}",
                protocol::SERVICE_NAME_RULE
            ))),
            None => Ok(()),
        }
    }

    async fn prepare(
        &self,
        _txn: &Txn<'_>,
        payload: Option<BTreeSet<String>>,
    ) -> Result<Option<BTreeSet<String>>, Rejection> {
        Ok(payload)
    }

    fn commit(&self, names: Option<BTreeSet<String>>, registration: &Committed<'_>) {
        let mut state = self.state.lock();
        let id = registration.owner;
        // Without a payload, a fresh registration (no credential presented)
        // starts with no services and a re-registration with the credential
        // keeps them. With one, the owner serves exactly the names sent.
        let keep = match &names {
            Some(_) => BTreeSet::new(),
            None if registration.previous.is_some() => state
                .owners
                .get(&id)
                .map(|owner| owner.names.clone())
                .unwrap_or_default(),
            None => BTreeSet::new(),
        };
        let wanted = names.unwrap_or(keep);
        let held = state
            .owners
            .get(&id)
            .map(|owner| owner.names.clone())
            .unwrap_or_default();
        for name in held.difference(&wanted) {
            state.remove(name, id);
        }
        for name in wanted.difference(&held) {
            state.add(name, id);
        }
        state.owners.insert(
            id,
            Owner {
                epoch: registration.epoch,
                incarnation: registration.incarnation,
                names: wanted,
            },
        );
    }

    fn on_removed(&self, removal: Removal) {
        let mut state = self.state.lock();
        // A removal of an older incarnation is stale: a later registration
        // of the owner committed since.
        if state
            .owners
            .get(&removal.id)
            .is_some_and(|owner| owner.incarnation <= removal.incarnation)
        {
            state.drop_owner(removal.id);
        }
    }
}

fn sorted(mut instances: Vec<InstanceId>) -> Vec<InstanceId> {
    instances.sort_by_key(|id| id.as_u128());
    instances
}

/// Answer 400 for a name that is not one valid path segment.
pub(crate) fn check_name(name: &str) -> Result<(), HubError> {
    if protocol::is_valid_service_name(name) {
        Ok(())
    } else {
        Err(HubError::bad_request(format!(
            "{name:?} is not a service name: {}",
            protocol::SERVICE_NAME_RULE
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use velo::{PeerInfo, WorkerAddress};

    fn commit(
        directory: &ServiceDirectory,
        owner: InstanceId,
        incarnation: u64,
        fresh: bool,
    ) -> Lease {
        commit_with(directory, owner, incarnation, fresh, None)
    }

    fn commit_with(
        directory: &ServiceDirectory,
        owner: InstanceId,
        incarnation: u64,
        fresh: bool,
        names: Option<&[&str]>,
    ) -> Lease {
        let peer = PeerInfo::new(owner, WorkerAddress::from_encoded(b"x".to_vec()));
        let epoch = RegistrationEpoch::new();
        let incarnation = Incarnation::from_u64(incarnation);
        let previous = (!fresh).then(|| Lease {
            incarnation: Incarnation::from_u64(0),
            epoch: RegistrationEpoch::new(),
        });
        let extensions = BTreeMap::new();
        let names = names.map(|names| names.iter().map(|name| name.to_string()).collect());
        directory.commit(
            names,
            &Committed {
                owner,
                peer: &peer,
                incarnation,
                epoch,
                previous,
                extensions: &extensions,
            },
        );
        Lease { incarnation, epoch }
    }

    fn auth(owner: InstanceId, lease: Lease) -> Authorized {
        Authorized { owner, lease }
    }

    /// A mutation applies only under the epoch the directory committed: a
    /// lease from before a re-registration is refused with 409.
    #[test]
    fn a_stale_lease_is_fenced() {
        let directory = ServiceDirectory::default();
        let owner = InstanceId::new_v4();
        let old = commit(&directory, owner, 1, true);
        let new = commit(&directory, owner, 2, false);
        let error = directory.put("svc", &auth(owner, old)).unwrap_err();
        assert_eq!(error.code(), protocol::ErrorCode::Conflict);
        directory.put("svc", &auth(owner, new)).unwrap();
        assert_eq!(directory.instances("svc"), [owner]);
    }

    /// A registration's payload is the owner's whole set: names it lacks are
    /// removed, names it adds are added. Without one, a re-registration
    /// keeps the set and a fresh registration starts empty.
    #[test]
    fn a_payload_replaces_the_set_and_none_keeps_or_clears_it() {
        let directory = ServiceDirectory::default();
        let owner = InstanceId::new_v4();
        let lease = commit_with(&directory, owner, 1, true, Some(&["a", "b"]));
        assert_eq!(directory.list(), ["a", "b"]);
        commit_with(&directory, owner, 2, false, Some(&["b", "c"]));
        assert_eq!(directory.list(), ["b", "c"]);
        commit(&directory, owner, 3, false);
        assert_eq!(directory.list(), ["b", "c"]);
        commit(&directory, owner, 4, true);
        assert!(directory.list().is_empty());
        let _ = lease;
    }

    /// A removal of an older incarnation is stale and leaves a later
    /// registration's services; a removal of the current one drops them.
    #[test]
    fn a_stale_removal_keeps_a_later_registrations_services() {
        let directory = ServiceDirectory::default();
        let owner = InstanceId::new_v4();
        commit(&directory, owner, 1, true);
        let lease = commit(&directory, owner, 2, false);
        directory.put("svc", &auth(owner, lease)).unwrap();
        directory.on_removed(Removal {
            id: owner,
            incarnation: Incarnation::from_u64(1),
        });
        assert_eq!(directory.instances("svc"), [owner]);
        directory.on_removed(Removal {
            id: owner,
            incarnation: Incarnation::from_u64(2),
        });
        assert!(directory.instances("svc").is_empty());
        assert!(directory.list().is_empty());
    }

    /// A watcher that falls more than the buffer behind gets `disconnected`
    /// and its stream ends: its set is stale. (Over a real socket the
    /// kernel's buffers absorb the backlog, so this is tested here.)
    #[tokio::test]
    async fn a_lagging_watcher_is_disconnected() {
        let directory = Arc::new(ServiceDirectory::default());
        let owner = InstanceId::new_v4();
        let lease = commit(&directory, owner, 1, true);
        let (_live, continuity) = tokio::sync::watch::channel(Continuity::Live);
        let watch = Watch {
            directory: Arc::clone(&directory),
            name: "svc".into(),
            changes: Some(
                directory
                    .state
                    .lock()
                    .services
                    .entry("svc".into())
                    .or_insert_with(Service::new)
                    .changes
                    .subscribe(),
            ),
            initial: None,
            closed: directory.closed.clone(),
            continuity,
        };
        for _ in 0..=WATCH_BUFFER {
            directory.put("svc", &auth(owner, lease)).unwrap();
            directory.delete("svc", &auth(owner, lease)).unwrap();
        }
        let mut watch = watch;
        let event = watch.next().await.expect("an event");
        assert!(
            format!("{event:?}").contains(WATCH_DISCONNECTED),
            "{event:?}"
        );
        assert!(watch.next().await.is_none());
        drop(watch);
        assert!(directory.state.lock().services.is_empty(), "not pruned");
    }

    #[test]
    fn names_are_one_path_segment() {
        for good in ["a", "rhino-router", "kv.index_v2", &"x".repeat(128)] {
            assert!(check_name(good).is_ok(), "{good}");
        }
        for bad in ["", "a b", "a/b", "é", ".", "..", &"x".repeat(129)] {
            assert!(check_name(bad).is_err(), "{bad}");
        }
    }
}
