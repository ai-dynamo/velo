// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Fixtures shared by the velo-hub integration suites.
//!
//! Two toy extensions stand in for kvbm's managers:
//!
//! - [`Role`] has kvbm's ConditionalDisagg semantics: a registrant is a
//!   prefill or a decode, and may not change role while registered.
//! - [`Ledger`] has the kvbm indexer's lifecycle semantics: an owner
//!   publishes generations of keyed entries, invalidates (retires) them,
//!   and anyone may query them. `prepare` hides the owner's entries and
//!   closes its publish path (owned by `Staged`, undone on drop, fenced by
//!   epoch), and every publish is fenced by the lease's epoch under the
//!   ledger's own lock. Those are the hiding and fencing rules of
//!   `Extension`, in the shape kvbm's indexer takes as an extension.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use velo_hub::axum::extract::State;
use velo_hub::axum::http::StatusCode;
use velo_hub::axum::routing::{get, post};
use velo_hub::axum::{Json, Router};
use velo_hub::protocol::{
    CREDENTIAL_HEADER, Credential, ErrorBody, HEARTBEAT_HANDLER, HeartbeatAck, HeartbeatRequest,
    RegisterRequest, RegisterResponse, RegistrationEpoch, instance_by_id, instance_heartbeat,
    paths,
};
use velo_hub::store::testing::Gate;
use velo_hub::store::{Incarnation, Removal};
use velo_hub::velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo_hub::velo::{Handler, InstanceId, PeerInfo, Velo, WorkerAddress};
use velo_hub::{
    Authorized, Committed, Extension, Hub, HubBuilder, HubConfig, HubError, HubJson, HubPath,
    Rejection, ServerConfig, Txn,
};

pub fn init_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn,velo_hub=debug")),
            )
            .with_test_writer()
            .try_init();
    });
}

// ---- hubs ---------------------------------------------------------------

pub fn config() -> HubConfig {
    HubConfig {
        server: ServerConfig::localhost(),
        ..HubConfig::default()
    }
}

pub fn builder() -> HubBuilder {
    Hub::builder(config())
}

pub async fn start() -> Hub {
    builder().serve().await.expect("start test hub")
}

pub fn tcp_transport() -> Arc<TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

pub fn velo_builder() -> velo_hub::velo::VeloBuilder {
    Velo::builder().add_transport(tcp_transport())
}

pub async fn new_velo() -> Velo {
    velo_builder().build().await.unwrap()
}

/// A peer with an opaque address. Only a hub without velo accepts it.
pub fn make_peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"test-addr".to_vec()),
    )
}

/// A peer whose address decodes but names no transport the hub has, so
/// velo's `register_peer` fails after the store write.
pub fn unreachable_peer() -> PeerInfo {
    let mut address: HashMap<String, Vec<u8>> = HashMap::new();
    address.insert("no-such-transport".to_string(), b"nowhere".to_vec());
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(rmp_serde::to_vec(&address).unwrap()),
    )
}

/// Answer the hub's heartbeat probes.
pub fn install_heartbeat(velo: &Velo) {
    velo.register_handler(
        Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
            HEARTBEAT_HANDLER,
            |ctx| async move {
                Ok(HeartbeatAck {
                    seq: ctx.input.seq,
                    ok: true,
                })
            },
        )
        .build(),
    )
    .unwrap();
}

/// Answer the hub's heartbeat probes with `ok: false`: what a registrant
/// answers while it holds no registration with this hub.
pub fn install_unregistered_heartbeat(velo: &Velo) {
    velo.register_handler(
        Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
            HEARTBEAT_HANDLER,
            |ctx| async move {
                Ok(HeartbeatAck {
                    seq: ctx.input.seq,
                    ok: false,
                })
            },
        )
        .build(),
    )
    .unwrap();
}

/// Answer the hub's heartbeat probes and count them.
pub fn install_counting_heartbeat(velo: &Velo) -> Arc<std::sync::atomic::AtomicUsize> {
    let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&probes);
    velo.register_handler(
        Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
            HEARTBEAT_HANDLER,
            move |ctx| {
                counter.fetch_add(1, Ordering::AcqRel);
                async move {
                    Ok(HeartbeatAck {
                        seq: ctx.input.seq,
                        ok: true,
                    })
                }
            },
        )
        .build(),
    )
    .unwrap();
    probes
}

/// Answer the hub's heartbeat probes after `delay`.
pub fn install_wedged_heartbeat(velo: &Velo, delay: Duration) {
    velo.register_handler(
        Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
            HEARTBEAT_HANDLER,
            move |ctx| async move {
                tokio::time::sleep(delay).await;
                Ok(HeartbeatAck {
                    seq: ctx.input.seq,
                    ok: true,
                })
            },
        )
        .build(),
    )
    .unwrap();
}

/// The handler [`echo_to`] calls: answers its input plus one.
pub const ECHO_HANDLER: &str = "echo-plus-one";

/// Install [`ECHO_HANDLER`] on `velo`.
pub fn install_echo(velo: &Velo) {
    velo.register_handler(
        Handler::typed_unary_async::<u64, u64, _, _>(ECHO_HANDLER, |ctx| async move {
            Ok(ctx.input + 1)
        })
        .build(),
    )
    .unwrap();
}

/// One unary from `from` to the instance `to`, by instance id. velo does
/// not ask discovery for such a call, so it reaches only a peer that
/// `from` registered before.
pub async fn echo_to(from: &Velo, to: InstanceId, value: u64) -> anyhow::Result<u64> {
    tokio::time::timeout(
        Duration::from_secs(5),
        from.typed_unary::<u64>(ECHO_HANDLER)?
            .payload(value)?
            .instance(to)
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the unary to {to} did not answer within 5 s"))?
}

// ---- HTTP ---------------------------------------------------------------

pub fn http() -> reqwest::Client {
    reqwest::Client::new()
}

pub fn control(hub: &Hub, path: &str) -> String {
    format!("http://{}{}", hub.control_addr(), path)
}

pub fn public(hub: &Hub, path: &str) -> String {
    format!("http://{}{}", hub.public_addr(), path)
}

pub fn request(peer: &PeerInfo) -> RegisterRequest {
    RegisterRequest {
        peer: peer.clone(),
        extensions: BTreeMap::new(),
    }
}

pub fn request_with(peer: &PeerInfo, extensions: &[(&str, serde_json::Value)]) -> RegisterRequest {
    RegisterRequest {
        peer: peer.clone(),
        extensions: extensions
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect(),
    }
}

pub async fn post_register(
    hub: &Hub,
    body: &RegisterRequest,
    credential: Option<&Credential>,
) -> reqwest::Response {
    let mut builder = http().post(control(hub, paths::INSTANCES)).json(body);
    if let Some(credential) = credential {
        builder = builder.header(CREDENTIAL_HEADER, credential.to_header_value());
    }
    builder.send().await.unwrap()
}

pub async fn register(
    hub: &Hub,
    body: &RegisterRequest,
    credential: Option<&Credential>,
) -> RegisterResponse {
    let response = post_register(hub, body, credential).await;
    let status = response.status();
    assert_eq!(
        status,
        200,
        "register failed: {}",
        response.text().await.unwrap()
    );
    response.json().await.unwrap()
}

pub async fn register_plain(
    hub: &Hub,
    peer: &PeerInfo,
    credential: Option<&Credential>,
) -> RegisterResponse {
    register(hub, &request(peer), credential).await
}

pub async fn error_code(response: reqwest::Response) -> velo_hub::protocol::ErrorCode {
    response.json::<ErrorBody>().await.unwrap().code
}

pub async fn heartbeat_status(
    hub: &Hub,
    owner: InstanceId,
    credential: Option<&Credential>,
) -> reqwest::StatusCode {
    let mut builder = http().post(control(hub, &instance_heartbeat(owner)));
    if let Some(credential) = credential {
        builder = builder.header(CREDENTIAL_HEADER, credential.to_header_value());
    }
    builder.send().await.unwrap().status()
}

pub async fn unregister_status(
    hub: &Hub,
    owner: InstanceId,
    credential: Option<&Credential>,
) -> reqwest::StatusCode {
    let mut builder = http().delete(control(hub, &instance_by_id(owner)));
    if let Some(credential) = credential {
        builder = builder.header(CREDENTIAL_HEADER, credential.to_header_value());
    }
    builder.send().await.unwrap().status()
}

/// Register `velo` with the hub and teach it the hub's own peer, so both
/// sides can message each other.
pub async fn wire_mutual(hub: &Hub, velo: &Velo) -> RegisterResponse {
    let response = register_plain(hub, &velo.peer_info(), None).await;
    let hub_id = response.hub_instance_id.expect("the hub runs velo");
    let lookup: velo_hub::protocol::PeerLookupResponse = http()
        .get(public(hub, &velo_hub::protocol::peers_by_instance(hub_id)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    velo.register_peer(lookup.peer).unwrap();
    response
}

/// Poll `condition` until it holds or `within` passes.
pub async fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if condition() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---- Role: kvbm's ConditionalDisagg semantics -----------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleKind {
    Prefill,
    Decode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleConfig {
    pub role: RoleKind,
}

pub fn role_payload(kind: RoleKind) -> (&'static str, serde_json::Value) {
    (
        Role::NAME,
        serde_json::to_value(RoleConfig { role: kind }).unwrap(),
    )
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleSnapshot {
    pub prefill: Vec<InstanceId>,
    pub decode: Vec<InstanceId>,
}

#[derive(Default)]
pub struct Role {
    members: Arc<Mutex<HashMap<InstanceId, (RoleKind, Incarnation)>>>,
}

impl Role {
    pub fn snapshot(&self) -> RoleSnapshot {
        let members = self.members.lock();
        let mut snapshot = RoleSnapshot::default();
        for (id, (kind, _)) in members.iter() {
            match kind {
                RoleKind::Prefill => snapshot.prefill.push(*id),
                RoleKind::Decode => snapshot.decode.push(*id),
            }
        }
        snapshot.prefill.sort_by_key(|id| id.as_u128());
        snapshot.decode.sort_by_key(|id| id.as_u128());
        snapshot
    }

    fn routes(self: Arc<Self>) -> Router {
        Router::new()
            .route(
                "/instances",
                get(|State(role): State<Arc<Role>>| async move { Json(role.snapshot()) }),
            )
            .with_state(self)
    }
}

impl Extension for Role {
    const NAME: &'static str = "role";
    type Registration = RoleConfig;
    type Staged = Option<RoleKind>;

    async fn prepare(
        &self,
        txn: &Txn<'_>,
        payload: Option<RoleConfig>,
    ) -> Result<Option<RoleKind>, Rejection> {
        if let Some(config) = &payload
            && let Some((existing, _)) = self.members.lock().get(&txn.owner)
            && *existing != config.role
        {
            return Err(Rejection::bad_request(format!(
                "instance {} is already a {existing:?}",
                txn.owner
            )));
        }
        Ok(payload.map(|config| config.role))
    }

    fn commit(&self, staged: Option<RoleKind>, registration: &Committed<'_>) {
        let mut members = self.members.lock();
        match staged {
            Some(kind) => {
                members.insert(registration.owner, (kind, registration.incarnation));
            }
            None => {
                members.remove(&registration.owner);
            }
        }
    }

    fn on_removed(&self, removal: Removal) {
        let mut members = self.members.lock();
        if members
            .get(&removal.id)
            .is_some_and(|(_, incarnation)| *incarnation <= removal.incarnation)
        {
            members.remove(&removal.id);
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        self.routes()
    }

    fn control_routes(self: Arc<Self>) -> Router {
        self.routes()
    }
}

// ---- Ledger: the kvbm indexer's lifecycle semantics -----------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publish {
    pub key: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hit {
    pub generation: u64,
    pub epoch: RegistrationEpoch,
}

struct Owner {
    epoch: RegistrationEpoch,
    incarnation: Incarnation,
    /// Cleared by `prepare`: queries miss while a registration is in
    /// flight.
    visible: bool,
    /// Cleared by `prepare`: publishes fail while a registration is in
    /// flight, whatever the authority says.
    publish_open: bool,
    entries: BTreeMap<String, (u64, bool)>,
}

#[derive(Default)]
struct LedgerState {
    owners: HashMap<InstanceId, Owner>,
}

#[derive(Default)]
struct LedgerFaults {
    reject_next_prepare: Option<Rejection>,
    gate_next_prepare: Option<Arc<Gate>>,
    hang_next_prepare: bool,
    panic_next_admit: bool,
    /// (entered, release): the next on_removed blocks its thread.
    block_next_removed: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    /// (entered, release): the next commit blocks its thread.
    block_next_commit: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    panic_next_prepare: bool,
    panic_next_commit: bool,
    panic_next_registered: bool,
    gate_next_registered: Option<Arc<Gate>>,
    gate_next_publish: Option<Arc<Gate>>,
}

/// Undo of `prepare`'s hide. Fenced by epoch: once the hidden state is gone
/// (removed, or replaced by a commit) it does nothing.
pub struct Unhide {
    state: Arc<Mutex<LedgerState>>,
    revealed: Arc<std::sync::atomic::AtomicUsize>,
    owner: InstanceId,
    epoch: RegistrationEpoch,
    armed: bool,
}

impl Drop for Unhide {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(owner) = self.state.lock().owners.get_mut(&self.owner)
            && owner.epoch == self.epoch
        {
            owner.visible = true;
            owner.publish_open = true;
            self.revealed.fetch_add(1, Ordering::AcqRel);
        }
    }
}

pub struct LedgerStaged {
    unhide: Option<Unhide>,
    participate: bool,
}

#[derive(Default)]
pub struct Ledger {
    state: Arc<Mutex<LedgerState>>,
    /// Every `on_removed`, with the incarnation the ledger held for that
    /// owner when it arrived.
    removals: Mutex<Vec<(Removal, Option<Incarnation>)>>,
    /// How many undos of a hide took effect, that is, made an owner's
    /// entries visible again.
    revealed: Arc<std::sync::atomic::AtomicUsize>,
    faults: Mutex<LedgerFaults>,
    pub attached: AtomicBool,
}

impl Ledger {
    pub fn reject_next_prepare(&self, rejection: Rejection) {
        self.faults.lock().reject_next_prepare = Some(rejection);
    }

    /// The next prepare hides, then stops at the gate.
    pub fn gate_next_prepare(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.faults.lock().gate_next_prepare = Some(Arc::clone(&gate));
        gate
    }

    /// The next prepare hides, then never finishes.
    pub fn hang_next_prepare(&self) {
        self.faults.lock().hang_next_prepare = true;
    }

    /// The next on_removed blocks its thread, which breaks the rule that
    /// sync hooks do not block. The first receiver yields once it is
    /// blocked; dropping the sender releases it.
    pub fn block_next_removed(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        self.faults.lock().block_next_removed = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    /// The next commit blocks its thread, as `block_next_removed` does.
    pub fn block_next_commit(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        self.faults.lock().block_next_commit = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    /// The next admit panics.
    pub fn panic_next_admit(&self) {
        self.faults.lock().panic_next_admit = true;
    }

    /// The next prepare hides, then panics.
    pub fn panic_next_prepare(&self) {
        self.faults.lock().panic_next_prepare = true;
    }

    pub fn panic_next_registered(&self) {
        self.faults.lock().panic_next_registered = true;
    }

    /// How many undos of a hide made entries visible again.
    pub fn revealed(&self) -> usize {
        self.revealed.load(Ordering::Acquire)
    }

    pub fn panic_next_commit(&self) {
        self.faults.lock().panic_next_commit = true;
    }

    /// The next on_registered stops at the gate (kvbm's "finalize" window).
    pub fn gate_next_registered(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.faults.lock().gate_next_registered = Some(Arc::clone(&gate));
        gate
    }

    /// The next publish stops at the gate after it is authorized and
    /// before it takes the ledger lock.
    pub fn gate_next_publish(&self) -> Arc<Gate> {
        let gate = Arc::new(Gate::default());
        self.faults.lock().gate_next_publish = Some(Arc::clone(&gate));
        gate
    }

    /// What a query for `owner`'s `key` returns.
    pub fn query(&self, owner: InstanceId, key: &str) -> Option<Hit> {
        let state = self.state.lock();
        let entry = state.owners.get(&owner)?;
        if !entry.visible {
            return None;
        }
        match entry.entries.get(key) {
            Some((generation, false)) => Some(Hit {
                generation: *generation,
                epoch: entry.epoch,
            }),
            _ => None,
        }
    }

    /// Removals that arrived for `owner` while the ledger already held a
    /// later incarnation of it: late copies, which must change nothing.
    pub fn late_removals(&self, owner: InstanceId) -> usize {
        self.removals
            .lock()
            .iter()
            .filter(|(removal, held)| {
                removal.id == owner && held.is_some_and(|held| held > removal.incarnation)
            })
            .count()
    }

    pub fn has_owner(&self, owner: InstanceId) -> bool {
        self.state.lock().owners.contains_key(&owner)
    }

    fn publish(&self, auth: Authorized, body: &Publish) -> Result<(), HubError> {
        let mut state = self.state.lock();
        let owner = state
            .owners
            .get_mut(&auth.owner)
            .ok_or_else(|| HubError::not_found("owner has no ledger"))?;
        // The fence: the lease must name the epoch this ledger
        // committed, checked under the ledger lock together with the write.
        if !owner.publish_open || owner.epoch != auth.lease.epoch {
            return Err(HubError::new(
                velo_hub::protocol::ErrorCode::Conflict,
                "fenced",
            ));
        }
        if let Some((generation, retired)) = owner.entries.get(&body.key)
            && (body.generation < *generation || (body.generation == *generation && *retired))
        {
            return Err(HubError::new(
                velo_hub::protocol::ErrorCode::Conflict,
                "generation is retired or older",
            ));
        }
        owner
            .entries
            .insert(body.key.clone(), (body.generation, false));
        Ok(())
    }

    fn invalidate(&self, auth: Authorized, body: &Publish) -> Result<bool, HubError> {
        let mut state = self.state.lock();
        let owner = state
            .owners
            .get_mut(&auth.owner)
            .ok_or_else(|| HubError::not_found("owner has no ledger"))?;
        if !owner.publish_open || owner.epoch != auth.lease.epoch {
            return Err(HubError::new(
                velo_hub::protocol::ErrorCode::Conflict,
                "fenced",
            ));
        }
        match owner.entries.get_mut(&body.key) {
            Some((generation, retired)) if *generation == body.generation => {
                *retired = true;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

async fn publish_route(
    State(ledger): State<Arc<Ledger>>,
    auth: Authorized,
    HubJson(body): HubJson<Publish>,
) -> Result<StatusCode, HubError> {
    let gate = ledger.faults.lock().gate_next_publish.take();
    if let Some(gate) = gate {
        gate.pass().await;
    }
    ledger.publish(auth, &body)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn invalidate_route(
    State(ledger): State<Arc<Ledger>>,
    auth: Authorized,
    HubJson(body): HubJson<Publish>,
) -> Result<Json<bool>, HubError> {
    ledger.invalidate(auth, &body).map(Json)
}

async fn query_route(
    State(ledger): State<Arc<Ledger>>,
    HubPath((owner, key)): HubPath<(InstanceId, String)>,
) -> Result<Json<Hit>, HubError> {
    ledger
        .query(owner, &key)
        .map(Json)
        .ok_or_else(|| HubError::not_found("miss"))
}

impl Extension for Ledger {
    const NAME: &'static str = "ledger";
    type Registration = ();
    type Staged = LedgerStaged;

    async fn attach(&self, _ctx: &velo_hub::AttachContext<'_>) -> anyhow::Result<()> {
        self.attached.store(true, Ordering::Release);
        Ok(())
    }

    fn admit(
        &self,
        _view: &velo_hub::RegistrationView<'_>,
        _payload: Option<&()>,
    ) -> Result<(), Rejection> {
        if std::mem::take(&mut self.faults.lock().panic_next_admit) {
            panic!("ledger admit panicked on request");
        }
        Ok(())
    }

    async fn prepare(&self, txn: &Txn<'_>, payload: Option<()>) -> Result<LedgerStaged, Rejection> {
        let (rejection, gate, hang, panic) = {
            let mut faults = self.faults.lock();
            (
                faults.reject_next_prepare.take(),
                faults.gate_next_prepare.take(),
                std::mem::take(&mut faults.hang_next_prepare),
                std::mem::take(&mut faults.panic_next_prepare),
            )
        };
        // Hide the owner's entries and close its publish path. The
        // guard exists before the first await, so a deadline that drops
        // this future still undoes the hide.
        let unhide = {
            let mut state = self.state.lock();
            state.owners.get_mut(&txn.owner).map(|owner| {
                owner.visible = false;
                owner.publish_open = false;
                Unhide {
                    state: Arc::clone(&self.state),
                    revealed: Arc::clone(&self.revealed),
                    owner: txn.owner,
                    epoch: owner.epoch,
                    armed: true,
                }
            })
        };
        if let Some(gate) = gate {
            gate.pass().await;
        }
        if hang {
            std::future::pending::<()>().await;
        }
        if panic {
            panic!("injected prepare panic");
        }
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        Ok(LedgerStaged {
            unhide,
            participate: payload.is_some(),
        })
    }

    fn commit(&self, mut staged: LedgerStaged, registration: &Committed<'_>) {
        if std::mem::take(&mut self.faults.lock().panic_next_commit) {
            panic!("injected commit panic");
        }
        let block = self.faults.lock().block_next_commit.take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
        if let Some(unhide) = staged.unhide.as_mut() {
            unhide.armed = false;
        }
        let mut state = self.state.lock();
        if staged.participate {
            // A new registration starts a fresh generation domain.
            state.owners.insert(
                registration.owner,
                Owner {
                    epoch: registration.epoch,
                    incarnation: registration.incarnation,
                    visible: true,
                    publish_open: true,
                    entries: BTreeMap::new(),
                },
            );
        } else {
            state.owners.remove(&registration.owner);
        }
    }

    async fn on_registered(&self, _registration: &Committed<'_>) {
        let (gate, panic) = {
            let mut faults = self.faults.lock();
            (
                faults.gate_next_registered.take(),
                std::mem::take(&mut faults.panic_next_registered),
            )
        };
        if let Some(gate) = gate {
            gate.pass().await;
        }
        if panic {
            panic!("injected on_registered panic");
        }
    }

    fn on_removed(&self, removal: Removal) {
        let block = self.faults.lock().block_next_removed.take();
        if let Some((entered, release)) = block {
            let _ = entered.send(());
            let _ = release.recv();
        }
        let mut state = self.state.lock();
        let held = state.owners.get(&removal.id).map(|owner| owner.incarnation);
        self.removals.lock().push((removal, held));
        if state
            .owners
            .get(&removal.id)
            .is_some_and(|owner| owner.incarnation <= removal.incarnation)
        {
            state.owners.remove(&removal.id);
        }
    }

    fn public_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route("/instances/{instance_id}/entries/{key}", get(query_route))
            .with_state(self)
    }

    fn control_routes(self: Arc<Self>) -> Router {
        Router::new()
            .route("/instances/{instance_id}/publish", post(publish_route))
            .route(
                "/instances/{instance_id}/invalidate",
                post(invalidate_route),
            )
            .with_state(self)
    }
}

pub fn ledger_opt_in() -> (&'static str, serde_json::Value) {
    (Ledger::NAME, serde_json::Value::Null)
}

pub async fn publish(
    hub: &Hub,
    owner: InstanceId,
    credential: &Credential,
    key: &str,
    generation: u64,
) -> reqwest::StatusCode {
    http()
        .post(control(
            hub,
            &format!("/v1/ext/ledger/instances/{owner}/publish"),
        ))
        .header(CREDENTIAL_HEADER, credential.to_header_value())
        .json(&Publish {
            key: key.to_string(),
            generation,
        })
        .send()
        .await
        .unwrap()
        .status()
}

pub async fn invalidate(
    hub: &Hub,
    owner: InstanceId,
    credential: &Credential,
    key: &str,
    generation: u64,
) -> Result<bool, reqwest::StatusCode> {
    let response = http()
        .post(control(
            hub,
            &format!("/v1/ext/ledger/instances/{owner}/invalidate"),
        ))
        .header(CREDENTIAL_HEADER, credential.to_header_value())
        .json(&Publish {
            key: key.to_string(),
            generation,
        })
        .send()
        .await
        .unwrap();
    if response.status().is_success() {
        Ok(response.json().await.unwrap())
    } else {
        Err(response.status())
    }
}

pub async fn query(hub: &Hub, owner: InstanceId, key: &str) -> Option<Hit> {
    let response = http()
        .get(public(
            hub,
            &format!("/v1/ext/ledger/instances/{owner}/entries/{key}"),
        ))
        .send()
        .await
        .unwrap();
    if response.status().is_success() {
        Some(response.json().await.unwrap())
    } else {
        None
    }
}

/// Send a request on a raw socket and return the socket without reading
/// the answer. Dropping it closes the connection outright, which makes
/// hyper drop the handler. (A client-side timeout in reqwest does not: the
/// handler keeps running.)
pub async fn send_and_abandon(
    hub: &Hub,
    method: &str,
    path: &str,
    credential: Option<&Credential>,
    body: Option<&serde_json::Value>,
) -> tokio::net::TcpStream {
    use tokio::io::AsyncWriteExt;
    let body = body.map(|value| value.to_string()).unwrap_or_default();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nhost: hub\r\ncontent-type: application/json\r\ncontent-length: {}\r\n",
        body.len()
    );
    if let Some(credential) = credential {
        request.push_str(&format!(
            "{CREDENTIAL_HEADER}: {}\r\n",
            credential.to_header_value().to_str().unwrap()
        ));
    }
    request.push_str("\r\n");
    request.push_str(&body);
    let mut socket = tokio::net::TcpStream::connect(hub.control_addr())
        .await
        .unwrap();
    socket.write_all(request.as_bytes()).await.unwrap();
    socket
}
