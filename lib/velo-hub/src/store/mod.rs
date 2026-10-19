// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Where the hub keeps registrations.
//!
//! [`HubStore`] is the backend contract. The hub's HTTP routes, its
//! registration transaction, and its own velo instance (through
//! [`PeerDiscovery`]) all read and write the same store. [`InMemoryStore`]
//! is the backend that ships; remote backends implement the same trait and
//! must pass the same conformance suite (`store::conformance`, feature
//! `test-support`).
//!
//! The stored record is complete enough for another hub to take over: the
//! peer, a digest of the credentials that may claim it, its epoch, and the
//! extension payloads as committed. It never holds a credential itself.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, PeerDiscovery, PeerInfo, WorkerId};

use crate::protocol::{Credential, RegistrationEpoch};

mod memory;

#[cfg(any(test, feature = "test-support"))]
pub mod conformance;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

pub use memory::{DEFAULT_PRUNE_INTERVAL, DEFAULT_TTL, InMemoryStore, InMemoryStoreBuilder};

/// Identity of one successful [`HubStore::put`].
///
/// Every put of an instance yields a greater one than that instance had
/// before. Work that outlives a request carries it, so a delayed touch or
/// remove cannot act on a later registration of the same instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Incarnation(u64);

impl Incarnation {
    /// An incarnation chosen by a backend.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// The backend's number.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Incarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identity of one write attempt. A put that answers
/// [`StoreError::Indeterminate`] is resolved by asking whether the record
/// now in the store was written by this op ([`HubStore::resolve`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OpId(uuid::Uuid);

impl OpId {
    /// A fresh random op id.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for OpId {
    fn default() -> Self {
        Self::new()
    }
}

/// SHA-256 of a [`Credential`]. The store keeps this, never the credential,
/// so a store dump does not hand out authority.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialDigest([u8; 32]);

impl CredentialDigest {
    /// The digest of `credential`.
    pub fn of(credential: &Credential) -> Self {
        use sha2::Digest;
        Self(sha2::Sha256::digest(credential.as_bytes()).into())
    }

    /// The 32 digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for CredentialDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CredentialDigest({:02x}{:02x}{:02x}{:02x}..)",
            self.0[0], self.0[1], self.0[2], self.0[3]
        )
    }
}

impl Serialize for CredentialDigest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut hex = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write;
            let _ = write!(hex, "{byte:02x}");
        }
        serializer.serialize_str(&hex)
    }
}

impl<'de> Deserialize<'de> for CredentialDigest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(deserializer)?;
        if hex.len() != 64 || !hex.is_ascii() {
            return Err(serde::de::Error::custom(
                "credential digest must be 64 hex digits",
            ));
        }
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(Self(bytes))
    }
}

/// What the hub writes for one registration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegistrationRecord {
    /// The registrant.
    pub peer: PeerInfo,
    /// The write attempt that produced this record.
    pub op: OpId,
    /// The registration's epoch. `None` for the hub's own entry.
    pub epoch: Option<RegistrationEpoch>,
    /// Digest of the credential the registrant presented, i.e. the one it
    /// holds until it reads the response to this registration.
    pub accepted: Option<CredentialDigest>,
    /// Digest of the credential this registration minted. With
    /// `accepted`, it is what a hub that takes the record over needs to
    /// let the registrant back in after a crash between the write and the
    /// response. The hub does not read either digest yet: an owner whose
    /// entry it did not write is reserved, and can register again only
    /// after that entry expires.
    pub pending: Option<CredentialDigest>,
    /// The extension payloads as committed, keyed by extension name.
    pub extensions: BTreeMap<String, serde_json::Value>,
    /// The hub's own entry: it never expires.
    pub pinned: bool,
}

impl RegistrationRecord {
    /// A record with no epoch, credentials, or payloads. The hub's own
    /// entry is one of these with `pinned` set.
    pub fn bare(peer: PeerInfo) -> Self {
        Self {
            peer,
            op: OpId::new(),
            epoch: None,
            accepted: None,
            pending: None,
            extensions: BTreeMap::new(),
            pinned: false,
        }
    }
}

/// A record as stored, with the incarnation its put produced.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredRegistration {
    /// What was written.
    pub record: RegistrationRecord,
    /// Which put wrote it.
    pub incarnation: Incarnation,
}

impl StoredRegistration {
    /// The registrant.
    pub fn peer(&self) -> &PeerInfo {
        &self.record.peer
    }

    /// The registrant's instance id.
    pub fn instance_id(&self) -> InstanceId {
        self.record.peer.instance_id()
    }
}

/// One entry without its payloads: what the heartbeat needs on each tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPeer {
    /// The registrant.
    pub peer: PeerInfo,
    /// The incarnation of its entry.
    pub incarnation: Incarnation,
    /// The hub's own entry.
    pub pinned: bool,
}

/// The exact store entry that left: removed by the hub or expired by the
/// backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Removal {
    /// The instance that left.
    pub id: InstanceId,
    /// The incarnation that left.
    pub incarnation: Incarnation,
}

/// Called once for every entry that leaves the store after the hook is
/// installed, outside every store lock. A put that replaces an entry is not
/// a removal: the hub writes replacements itself and knows about them.
pub type RemovalHook = Arc<dyn Fn(Removal) + Send + Sync + 'static>;

/// Whether the store's view is current.
///
/// A remote backend reads from a local cache fed by a watch. When the watch
/// lags or breaks, cached authority may be stale, so the hub refuses
/// authorization and new registrations while the store is not [`Live`].
/// Discovery reads keep answering from the cache.
///
/// [`Live`]: Continuity::Live
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Continuity {
    /// The cache is current.
    Live,
    /// The backend is behind or reconnecting; the cache may be stale.
    Degraded,
    /// The backend lost history (for example a compacted watch or a lost
    /// leader). The cache is stale until the backend resynchronizes.
    Lost,
}

/// Errors from [`HubStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Another instance holds this worker id.
    #[error("worker {worker_id} is held by instance {existing}")]
    Conflict {
        /// The contested worker id.
        worker_id: WorkerId,
        /// The instance that holds it.
        existing: InstanceId,
    },
    /// The instance has no entry.
    #[error("instance {0} not found")]
    NotFound(InstanceId),
    /// The instance's current incarnation is not the one the caller expected.
    #[error("instance {id} incarnation changed (expected {expected:?}, current {current:?})")]
    Stale {
        /// The instance.
        id: InstanceId,
        /// The incarnation the caller expected; `None` means "no entry".
        expected: Option<Incarnation>,
        /// The incarnation the store holds; `None` means "no entry".
        current: Option<Incarnation>,
    },
    /// The write may or may not have been applied (for example a lost
    /// response from a remote backend). Resolve it with
    /// [`HubStore::resolve`]. [`InMemoryStore`] never returns this.
    #[error("store write outcome unknown: {0}")]
    Indeterminate(String),
    /// The removal hook is already installed; it cannot be replaced.
    #[error("the removal hook is already installed")]
    HookInstalled,
    /// Any other backend failure. Nothing was written.
    #[error(transparent)]
    Backend(#[from] anyhow::Error),
}

/// The hub's registration backend.
///
/// Contract. The conformance suite checks each rule except liveness
/// (expiry, and that a pinned record never expires), which is specific to
/// each backend, and checks the linearization of the hook only
/// sequentially; a backend must show its own concurrent evidence for that.
///
/// - Every `put` of an instance yields a new [`Incarnation`], greater than
///   any that instance had before. The hub and its extensions order
///   removals by it, so a stale removal cannot drop a later registration.
/// - Conditional operations (`put` with `expected`, `remove`, `touch`) are
///   compare-and-swap in the backend.
/// - [`install_removal_hook`](Self::install_removal_hook) and its snapshot
///   share one linearization point: every entry present at that point is in
///   the snapshot, and every later removal is reported exactly once, outside
///   locks.
/// - Sync reads are served from a local view; a remote backend keeps it fed
///   by a watch and reports gaps through [`continuity`](Self::continuity).
/// - A pinned record never expires.
pub trait HubStore: PeerDiscovery + Send + Sync + 'static {
    /// Write `record`, conditional on the instance's current incarnation.
    /// `expected: None` requires that the instance has no entry;
    /// `Some(i)` requires that `i` is current. A mismatch is
    /// [`StoreError::Stale`]. A worker id held by another instance is
    /// [`StoreError::Conflict`]. Resets liveness.
    fn put(
        &self,
        record: RegistrationRecord,
        expected: Option<Incarnation>,
    ) -> BoxFuture<'_, Result<Incarnation, StoreError>>;

    /// Remove the entry if `incarnation` is current, and report it to the
    /// removal hook.
    fn remove(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Refresh liveness if `incarnation` is current.
    ///
    /// The heartbeat calls this once per registration per tick, from that
    /// registration's probe task, so one tick's calls run concurrently.
    fn touch(
        &self,
        id: InstanceId,
        incarnation: Incarnation,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// After a put answered [`StoreError::Indeterminate`]: the incarnation
    /// the put produced if the instance's current record was written by
    /// `op`, otherwise `None`. The answer must be settled: a `None` means
    /// the write did not and will not apply.
    fn resolve(
        &self,
        id: InstanceId,
        op: OpId,
    ) -> BoxFuture<'_, Result<Option<Incarnation>, StoreError>>;

    /// The instance's entry.
    fn get(&self, id: InstanceId) -> Option<StoredRegistration>;

    /// The instance's current incarnation.
    fn current_incarnation(&self, id: InstanceId) -> Option<Incarnation> {
        self.get(id).map(|stored| stored.incarnation)
    }

    /// Whether `incarnation` is the instance's current one.
    fn is_current(&self, id: InstanceId, incarnation: Incarnation) -> bool {
        self.current_incarnation(id) == Some(incarnation)
    }

    /// Whether the instance has an entry.
    fn contains(&self, id: InstanceId) -> bool {
        self.current_incarnation(id).is_some()
    }

    /// Every entry, each paired with its incarnation from the same snapshot.
    fn list(&self) -> Vec<StoredRegistration>;

    /// Every entry's peer, incarnation, and pinned flag, without the
    /// payloads. The hub calls this on every heartbeat tick; a backend
    /// should avoid copying payloads for it.
    fn peers(&self) -> Vec<StoredPeer> {
        self.list()
            .into_iter()
            .map(|stored| StoredPeer {
                incarnation: stored.incarnation,
                pinned: stored.record.pinned,
                peer: stored.record.peer,
            })
            .collect()
    }

    /// Install the hook that hears every later removal, and return the
    /// entries present at the same linearization point. Fails with
    /// [`StoreError::HookInstalled`] the second time.
    fn install_removal_hook(
        &self,
        hook: RemovalHook,
    ) -> Result<Vec<StoredRegistration>, StoreError>;

    /// Whether the store's view is current. See [`Continuity`].
    fn continuity(&self) -> watch::Receiver<Continuity>;

    /// Spawn the backend's liveness task (a TTL reaper), if it needs one.
    /// Backends with native leases return `None`. At shutdown the hub
    /// cancels `cancel`, waits up to its shutdown timeout for the task, then
    /// aborts it.
    fn spawn_liveness_task(self: Arc<Self>, _cancel: CancellationToken) -> Option<JoinHandle<()>> {
        None
    }
}
