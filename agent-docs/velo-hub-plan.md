# velo-hub: definitive plan

Status: accepted direction, 2026-10-18, **amended by the review addendum in §13** (read it with §6, §7, §9, §11). Not implemented. Supersedes `agent-docs/hub-plan.md` (kept for its reasoning). Work branch only; `agent-docs/` never merges to `main`.

## 1. Goal

A crate, `velo-hub`, for building a **hub**: one service that is the peer and service discovery for a velo application and also hosts pluggable **extensions**. A program defines a hub in `main`, adds bundles of extensions, picks a store backend, and serves it.

Today's hub lives in kvbm (`kvbm/crates/kvbm-hub`) and rhino embeds it. dynamo and wcep may want one. The target composition:

```
velo-hub            generic hub: store, registration, discovery, lifecycle, client
  kvbm-hub          KvbmBundle: p2p, disagg, prefill-router, connector-control, indexer, kvbm-primary
    rhino-hub       RhinoBundle   (requires KvbmBundle)
    dynamo-hub      DynamoBundle  (requires KvbmBundle)
  wcep hub main     .bundle(kvbm).bundle(rhino).bundle(dynamo).bundle(wcep)
```

Velo does not depend on velo-hub. velo-hub is an opinionated layer for building velo applications: **when an app uses a hub, the hub is its peer and service discovery.** Apps that do not use a hub pick any velo discovery backend as today.

## 2. Decisions

| # | Decision | Why |
|---|---|---|
| 1 | Separate crate `velo-hub` (lib `velo_hub`) at `lib/velo-hub`. | User decision. Name matches `velo-ext`. `scripts/check-semver.sh` finds crates by `lib/*/` directory name, so it is covered with no script change. |
| 2 | `velo-hub` takes the **workspace version** (lockstep with `velo`) and depends on `velo` with a **caret** requirement on the current version, not `=`. | See §3. |
| 3 | **No separate `velo-hub-ext` crate.** Extension and bundle traits live in `velo_hub`. | See §3.3. |
| 4 | `Velo` becomes a single-`Arc` handle and `VeloBuilder::build` returns `Velo`, not `Arc<Velo>`. Done in velo **before** velo-hub's first release. | `Velo` is already `#[derive(Clone)]` over seven `Arc` fields (`lib/velo/src/lib.rs:185`), and `build()` wraps it in another `Arc` (`lib.rs:415`). A new public crate should not bake `Arc<Velo>` into its API. `Hub` and `HubClient` follow the same handle pattern. |
| 5 | Clean wire protocol. kvbm hub and connector move in lockstep. | User decision. No deployed mixed-version window to protect. |
| 6 | Registration hooks: **Option B** (admit, prepare, commit, on_registered, on_removed) with hub-owned authority. | User decision. See §6. |
| 7 | Store: pluggable `HubStore`. In-memory first, etcd next, Raft between hubs for HA later. The stored record is complete enough for failover from M1. | §7. |
| 8 | Service discovery watch over SSE. | User decision. A velo-stream watch stays a later option. |
| 9 | Bundles declare required bundles; they never construct them. `main` builds each bundle once. | Shared instances for the rhino/dynamo diamond; no hidden default config. |
| 10 | "Feature" is renamed "extension". | "Feature" collides with cargo features. |
| 11 | kvbm upgrades to velo 0.20.0 / velo-ext 0.5.6 (on crates.io) as its own PR first; it ports to velo-hub on the release that carries decision 4. | kvbm is on velo 0.4.1. |

## 3. Versioning

### 3.1 Why caret, not `=`

`velo-ext` is `=`-pinned because velo re-exports its types while it versions independently. A "compatible" velo-ext release could re-resolve downstream lockfiles under a velo that was never tested with it.

velo-hub is different:

- Pre-1.0, a caret requirement `velo = "0.21.0"` already means `>=0.21.0, <0.22.0`. It can never pull a second minor, so it cannot produce two velo copies on its own.
- `=` would force every app onto one exact velo patch. A velo patch fix could not reach a velo-hub user until velo-hub republished.

The real risk is a change cargo-semver-checks cannot see. velo-hub's public API names velo types (`Velo`, `PeerInfo`, axum `Router`), so moving velo-hub from velo 0.21 to 0.22 changes the identity of those types. That is a breaking change for velo-hub with no diff in velo-hub's own source.

**Lockstep versioning closes it structurally.** velo-hub uses `version.workspace = true`, so a velo minor bump is always a velo-hub minor bump. velo-hub X.Y.* always works with velo X.Y.*.

### 3.2 Rules (added to the repo `CLAUDE.md` in M1)

1. `lib/velo-hub/Cargo.toml`: `version.workspace = true`; `velo = { workspace = true }`.
2. `[workspace.dependencies]`: `velo = { path = "lib/velo", version = "<workspace version>" }` (caret). A check (in `check-semver.sh`) fails if this requirement's `major.minor` differs from `[workspace.package] version`.
3. velo and velo-hub publish together, from one commit.
4. velo-hub re-exports what its API exposes: `velo_hub::velo` and `velo_hub::axum`. Downstream hub crates use these re-exports rather than naming their own `axum`/`velo` versions.
5. Downstream hub crates (kvbm-hub, dynamo-hub, rhino-hub) depend on `velo-hub` with a caret.
6. `CLAUDE.md` "Two crates only" gains a documented `velo-hub` exception next to `ucx-rs`. Unlike `ucx-rs`, velo-hub does share types with velo; rules 1-4 are why that is safe.

### 3.3 Why no `velo-hub-ext`

`velo-ext` exists so transport and discovery authors can avoid depending on the heavy runtime. Hub extensions cannot avoid it: their context hands them a `Velo`, and their routes are axum `Router`s. A separate trait crate would still depend on velo and axum. It would add a fourth publishable crate that can reproduce the dual-copy bug against velo-hub, and it would not let bundles from different velo-hub versions mix in one hub.

Revisit only when both are true: there are out-of-tree extension authors who should not pull in server dependencies, and the extension trait has been stable across two velo-hub minors.

## 4. Crate layout

```
lib/velo-hub/
  Cargo.toml            features: server (axum), client (reqwest), etcd (etcd-client)
  src/
    lib.rs              re-exports velo, axum; prelude
    protocol.rs         paths, request/response types, errors, ids (serde only; both features)
    hub/                (server)
      mod.rs            Hub handle, HubBuilder, startup and shutdown
      graph.rs          bundle and extension graph validation, topological order
      extension.rs      Extension (typed), DynExtension (erased, blanket impl), Bundle
      context.rs        AttachContext, StartContext, Txn, Committed, Removal
      authority.rs      credential state machine (ported) + Authority API + axum extractor
      registration.rs   serialized transaction (§6)
      lifecycle.rs      removal delivery, serialized extension events
      heartbeat.rs      ported
      routes.rs         core routes, error mapping
      services.rs       service registry + SSE (M3)
    store/
      mod.rs            HubStore trait, StoredRegistration, conformance suite (test-support)
      memory.rs
      etcd.rs           (feature etcd, M4)
    client/             (client)
      mod.rs            HubClient handle: PeerDiscovery + ServiceDiscovery + registration guard
```

Feature-gated deps stay out of `velo` itself.

## 5. Extensions and bundles

### 5.1 Extension

```rust
pub trait Extension: Send + Sync + Sized + 'static {
    /// Wire name. Routes mount at /v1/ext/{NAME}; registration payloads key on it.
    const NAME: &'static str;
    /// What a registrant sends when it opts in. `()` if nothing.
    type Registration: DeserializeOwned + Serialize + Send + Sync;
    /// Validated, prepared, not yet visible. Dropping it aborts.
    type Staged: Send;

    /// Hub-side extensions this one needs. Drives attach order and typed lookup.
    fn requires() -> &'static [&'static str] { &[] }
    /// Extensions a registrant must also opt into when it opts into this one.
    fn registration_requires() -> &'static [&'static str] { &[] }

    // lifecycle
    fn attach(&self, ctx: &AttachContext) -> impl Future<Output = Result<()>> + Send;
    fn start(&self, ctx: &StartContext) -> impl Future<Output = Result<()>> + Send { async { Ok(()) } }
    fn shutdown(&self) -> impl Future<Output = ()> + Send { async {} }

    // surface
    fn public_routes(self: Arc<Self>) -> Router { Router::new() }
    fn control_routes(self: Arc<Self>) -> Router { Router::new() }
    fn descriptor(&self) -> serde_json::Value { serde_json::Value::Null }

    // registration (§6)
    fn admit(&self, req: &RegistrationView<'_>) -> Result<(), Rejection> { Ok(()) }
    fn prepare(&self, txn: &Txn<'_>, payload: Option<Self::Registration>)
        -> impl Future<Output = Result<Self::Staged, Rejection>> + Send;
    fn commit(&self, staged: Self::Staged, reg: &Committed<'_>);
    fn on_registered(&self, reg: &Committed<'_>) -> impl Future<Output = ()> + Send { async {} }
    fn on_removed(&self, removal: Removal);
}
```

- The hub stores `Arc<dyn DynExtension>`; a blanket impl over every `Extension` decodes `serde_json::Value` into `Registration` and boxes `Staged` as `Box<dyn Any + Send>`.
- Routes always mount under `/v1/ext/{NAME}/`. Core routes live elsewhere under `/v1/` and cannot be shadowed.
- `public_routes` are served on the public listener; `control_routes` on the control listener. Two listeners keep a security boundary (operator surface vs client surface).
- `AttachContext::extension::<T>() -> Result<Arc<T>>` returns an already-attached extension. It fails unless `T::NAME` is in `requires()`, so undeclared wiring cannot work by accident.
- `StartContext` additionally has the bound listener addresses and the hub's `Velo`. `start` replaces kvbm's late binding after `serve()` (`cd.start_dispatcher(..)`, tier broadcaster).

### 5.2 Bundle

```rust
pub trait Bundle: Send + 'static {
    const NAME: &'static str;
    fn requires() -> &'static [&'static str] { &[] }       // other bundles
    fn install(self, b: &mut BundleInstaller<'_>) -> Result<()>;   // adds extensions
}
```

- `BundleInstaller::extension(ext)` adds an extension; `BundleInstaller::config::<C>()` reads `[bundles.NAME]` from the hub config.
- The builder validates at `serve()`, **before binding any socket**: duplicate bundle or extension names, a missing required bundle or extension, cycles, and config sections that no bundle claims.
- No bundle constructs another. If rhino and dynamo both require kvbm, `main` adds `KvbmBundle` once and both resolve to it.
- Bundles can also be skipped entirely: `HubBuilder::extension(ext)` adds a single extension.

### 5.3 A hub main

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg: HubConfig = load_config()?;   // the binary chooses clap/figment/env
    let tcp = TcpTransportBuilder::new().bind(cfg.server.velo_addr()).build()?;
    let hub = velo_hub::Hub::builder(cfg)
        .store(velo_hub::store::InMemoryStore::default())
        .velo(velo::Velo::builder().add_transport(Arc::new(tcp)))
        .bundle(kvbm_hub::KvbmBundle::new())
        .bundle(rhino_hub::RhinoBundle::new())
        .serve()
        .await?;
    hub.run_until_signal().await
}
```

```rust
#[derive(Deserialize)]
pub struct HubConfig {
    pub server: ServerConfig,            // bind addr, ports, ttl, heartbeat, registration lock timeout
    #[serde(default)]
    pub bundles: BTreeMap<String, serde_json::Value>,
}
```

No clap or figment in the library.

## 6. Registration (Option B)

### 6.1 Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Hub
    participant A as Authority
    participant E as Extensions (topo order)
    participant S as HubStore
    participant V as Hub Velo
    C->>H: POST /v1/instances {peer, extensions:{name: payload}}
    H->>H: decode payloads (unknown name or bad payload: 400)
    H->>E: admit(view) for all (400 on reject)
    H->>H: acquire hub-wide registration lock (timeout: 503)
    H->>A: begin(owner, presented credential)  [owner authority now Busy]
    H->>H: decode peer WorkerAddress for velo (400 on error)
    H->>E: prepare(txn, Some|None) for all -> Staged
    alt any prepare fails
        H->>H: drop all Staged
        H->>A: abort (prior registration stays Active, untouched)
        H-->>C: 4xx/5xx
    end
    H->>S: put(record) -> incarnation
    alt store write fails
        H->>H: drop all Staged; abort (prior untouched)
    end
    H->>A: record_write(incarnation)
    H->>E: commit(staged, committed) for all  [infallible]
    H->>V: register_peer(peer)
    H->>A: commit -> credential, epoch
    alt credential commit fails (removal raced)
        H->>S: conditional remove(incarnation)
        H->>E: on_removed(id, incarnation) for all
        H->>A: fail closed
        H-->>C: 409, register again
    end
    H->>E: on_registered(committed) for all
    H->>H: release lock
    H-->>C: 200 {credential, epoch, hub_instance_id}
```

### 6.2 Invariants

1. **Every extension sees every registration.** `payload` is `None` when the registrant did not opt in. `commit` with `None` drops any state the owner had from a prior registration. This replaces kvbm's `participates` flag, the prior-only `on_unregister`, and its three "called on" sets.
2. **`prepare` makes no change any other code can observe.** Validation and reads only. Dropping `Staged` is the abort.
3. **All extension mutations are serialized.** `commit`, `on_registered` and `on_removed` all run while holding the hub-wide registration lock. Removals from the store (TTL expiry, DELETE, heartbeat eviction) revoke authority synchronously in the removal callback, then queue the `on_removed` delivery to a lifecycle task that takes the same lock. So nothing changes between an extension's `prepare` and its `commit`, and `commit` can be infallible without reservations.
4. **`on_registered` must not block.** It runs under the lock; long work (kvbm's module refresh) is spawned.
5. **Authority is the hub's alone.** Extensions authorize owner-scoped mutations through `Authority::authorize(owner, &credential) -> Result<Lease { incarnation, epoch }>` or the `Authorized` axum extractor. While an owner is mid-registration, `authorize` answers `Busy`; that is what kvbm's `stage_registration` emulated.
6. **Removal is incarnation-scoped.** `Removal { id, incarnation }`. An extension ignores a removal for an incarnation older than the one it committed.
7. **A failure before the store write leaves the prior registration untouched.** A failure after it fails closed: the new registration is removed and the client registers again. (kvbm today replays the prior registration instead; B drops replay.)
8. If the prior incarnation is removed while a re-registration is in flight, the ported credential state machine records it (`removed_incarnations`) and an abort reports `Revoked`, which delivers `on_removed` for the prior incarnation.

### 6.3 What ports and what is new

Ported from kvbm, with its tests: the credential state machine (`registration/mod.rs`), the removal hook installed atomically with a snapshot, incarnation-conditional `touch`/`unregister`, and incarnation-safe heartbeat. New: the serialized transaction, `prepare`/`commit`, the lifecycle task, and hub-owned authority. Deleted: replay rollback (`rollback_registration`, `restore_registration*`).

## 7. Store

### 7.1 Trait

`HubStore: PeerDiscovery` replaces kvbm's `PeerRegistry`. The record:

```rust
pub struct StoredRegistration {
    pub peer: PeerInfo,
    pub incarnation: Incarnation,
    pub credential_digest: CredentialDigest,      // SHA-256, never the credential itself
    pub epoch: RegistrationEpoch,
    pub extensions: BTreeMap<String, serde_json::Value>,   // the payloads as committed
    pub pinned: bool,                             // the hub's own entry; no TTL
}
```

Operations: `put(record) -> Incarnation`, conditional `remove(id, incarnation)`, conditional `touch(id, incarnation)`, sync reads (`get`, `contains`, `list`, `is_current`), `install_removal_hook(cb) -> snapshot`, and optional `spawn_liveness_task`.

Contract (enforced by a shared conformance suite that every backend runs):

- Every `put` yields a new incarnation, never reused for that instance.
- Conditional operations are compare-and-swap in the backend (in-memory: under the lock; etcd: a txn on `mod_revision`; Raft: a log entry).
- The removal hook is installed at one linearization point with its snapshot: every entry in the snapshot is present at that point, and every later removal is reported exactly once, outside locks.
- Sync reads are served from a local cache. Remote backends keep it fed by a watch.
- Pinning is part of the trait (fixes kvbm needing a concrete `InMemoryRegistry` to call `protect`).

### 7.2 Backends

| Backend | Milestone | Liveness | Incarnation | Removal hook |
|---|---|---|---|---|
| `InMemoryStore` | M1 | TTL reaper | counter | direct |
| `EtcdStore` | M4 | lease per peer | `mod_revision` | watch from the snapshot revision |
| Raft between hubs | M7 | leader-driven TTL | log index | applied removals |

### 7.3 Recovery

With a persistent store, a starting hub (or a new Raft leader) loads stored registrations and replays each one through `prepare(Some/None)` + `commit` using the stored payloads, then marks it Active with the stored credential digest. Clients keep their credentials across a hub restart or failover. If a stored registration no longer prepares (the operator changed incompatible config), the hub removes it and the client registers again. The in-memory store starts empty, so recovery is a no-op until M4, but the record shape above is fixed in M1.

## 8. Discovery surfaces

### 8.1 Peer discovery (M1)

`GET /v1/peers/instance/{id}`, `GET /v1/peers/worker/{id}` on both listeners.

### 8.2 Service discovery (M3)

- A service entry is owned by a registered peer and dies with it (same removal path, so no orphans).
- `GET /v1/services`, `GET /v1/services/{name}`.
- `GET /v1/services/{name}/watch`: SSE. First event `initial` (all instances), then `added`/`removed`, mirroring `velo_ext::ServiceEvent`. A dropped stream is reported to the client's watch as a disconnect.
- `PUT` / `DELETE /v1/services/{name}/instances/{id}` on the control listener, authorized by the owner's credential.

### 8.3 Client

`HubClient` (feature `client`) is a cheap-clone handle that implements `PeerDiscovery` and `ServiceDiscovery`. It owns the registration (credential, epoch, `hub_instance_id`) with an unregister-on-drop guard, registers the heartbeat handler on the app's `Velo`, and offers `ext::<T>()` JSON helpers scoped to `/v1/ext/{name}`. Bundle crates ship typed clients on top (`kvbm_hub::IndexerClient::new(&hub)`).

`HubClient::velo_builder()` returns a `VeloBuilder` with the hub already set as discovery, so the default path for a hub app is one call.

## 9. Lifecycle

Startup (`HubBuilder::serve`):

1. Validate the bundle/extension graph and config sections (no I/O yet).
2. Bind the public and control sockets.
3. Build the hub's `Velo` with the store as its discovery (optional; without it the hub is discovery-only and liveness is TTL-only).
4. Put the hub's own pinned entry.
5. `attach` every extension in topological order.
6. Install the store removal hook and take its snapshot (irreversible). Recover stored registrations (§7.3).
7. Spawn the reaper, heartbeat, lifecycle task and HTTP servers.
8. `start` every extension in topological order.

A failure in steps 1-6 runs `shutdown` on the extensions already attached (reverse order), removes the pinned entry, and leaves an injected store reusable (kvbm's `abort_startup` behavior, extended).

Shutdown (`Hub::shutdown`, and `run_until_signal` on Ctrl-C):

1. Gate new registrations (503).
2. `shutdown` every extension in reverse topological order.
3. Cancel the heartbeat, reaper and lifecycle tasks; drain both HTTP listeners.
4. Remove the hub's pinned entry.
5. `Velo::shutdown(policy)`. kvbm never shuts its hub `Velo` down today.

## 10. Wire protocol

| Path | Listener | Purpose |
|---|---|---|
| `GET /v1/health` | both | liveness |
| `GET /v1/hub` | both | hub id, velo id, extensions with name, requires, descriptor |
| `GET /v1/peers/instance/{id}`, `/v1/peers/worker/{id}` | both | peer discovery |
| `POST /v1/instances` | control | register (§6) |
| `GET /v1/instances` | control | list |
| `DELETE /v1/instances/{id}` | control | unregister (credential) |
| `POST /v1/instances/{id}/heartbeat` | control | touch (credential) |
| `POST /v1/instances/{id}/probe` | control | hub-initiated velo probe |
| `/v1/services/...` | see §8.2 | service discovery |
| `/v1/ext/{name}/...` | per extension | extension routes |

Credential header `x-velo-hub-credential`. Velo heartbeat handler `velo_hub_heartbeat`. `RegisterRequest { peer, extensions: map<name, json> }`.

## 11. Milestones

Each is one PR from current `main`, with its tests in the same PR. Tests that drive HTTP are ported first and go green before the new hook tests are written, so kvbm's observed behavior is the spec.

### velo

| M | Scope | Tests |
|---|---|---|
| V0 | `Velo` single-`Arc` handle; `build()` returns `Velo`. velo 0.21.0. | existing suite; clone shares shutdown (`ShutdownOnce`) |
| M1 | `lib/velo-hub` (`server`): protocol, `HubStore` + in-memory + conformance suite, authority (ported), serialized registration (§6), lifecycle task, heartbeat (ported), extensions/bundles/graph, startup/shutdown, core routes. `CLAUDE.md` exception and the version-sync check. | ported: kvbm `tests/integration.rs` (45, path changes only), `registry/tests.rs` (12), `registration/tests.rs` (6), `tests/registration_lifecycle.rs` (8; 2 probe extensions rewritten). New: topo attach/start, reverse shutdown; cycle/missing/duplicate/unclaimed config rejected before bind; unknown payload 400; extension cannot shadow core routes; prepare failure leaves prior untouched; post-write failure fails closed and delivers `on_removed`; removal during prepare cannot interleave with commit; authorize is `Busy` mid-registration; shutdown frees ports and shuts down `Velo`. |
| M2 | `client`: `HubClient`, registration guard, heartbeat handler, `velo_builder()`, ext helpers. | ported `client_unregister.rs`; two Velos discovering each other through a hub. |
| M3 | Services + SSE; `HubClient` as `ServiceDiscovery`. | watch order (initial then deltas); entries die with their owner; credential required; SSE reconnect reports a disconnect. |
| M4 | `etcd` store + recovery (§7.3). | conformance suite against etcd on :2379; lease expiry fires the removal hook once; a hub restart keeps registrations and credentials valid. |
| M5 | `examples/` hub binary: two toy bundles, one requiring the other, config file. | builds and serves in CI. |
| M6 | Book chapter, README. | `Book` CI job. |
| M7 | HA design note, then Raft between hubs (leader redirects writes, multi-URL client). | separate plan. |

M1 and M2 publish together with V0 as velo/velo-hub 0.21.0.

### Downstream

| Step | Repo | Scope |
|---|---|---|
| K0 | kvbm | velo 0.4.1 -> 0.20.0, velo-ext 0.2.0 -> 0.5.6. Own PR, no hub changes. |
| K1 | kvbm | kvbm-hub becomes `KvbmBundle` on velo-hub 0.21. Managers become extensions with prepare/commit split (p2p: check layout in prepare, add member and baseline in commit; indexer: kind/carrier/`KindConflict` checks in prepare, `ManifestIndexes::bind` in commit). `PrimaryConfig` checks become the `kvbm-primary` extension's `admit`. The indexer's bundle-directory credential copy is deleted in favor of `Authority`. kvbm's `RegistrationEpoch` becomes velo-hub's. Connector moves to `HubClient` + typed kvbm clients. `kvbmctl` stays in kvbm. |
| R1 | rhino | bump the kvbm rev; `RhinoBundle` requires `KvbmBundle`; reach the indexer through `ctx.extension::<Indexer>()` instead of holding the manager. |
| D1 / W1 | dynamo, wcep | bundles in the same pattern, when their needs are known. |

## 12. Risks

- **Serialized registration throughput.** Registrations are rare (worker start), but a slow `prepare` or `on_registered` blocks every registration and removal delivery. Mitigation: the lock wait has a timeout (503, client retries), `on_registered` spawns long work, and a metric records lock hold time.
- **Delayed `on_removed`.** Authority is revoked synchronously; extension cleanup is queued behind the lock. An extension's read path can briefly see state for a removed owner. Reads that matter must check `Authority` or the incarnation.
- **K0 is large.** 16 minor versions, at least 7 commits marked breaking (#54, #56, #64, #65, #92, #116, #117). It blocks K1 and R1.
- **Behavior change in B.** A failure after the store write no longer restores the prior registration. Clients must treat 409 on register as "register again".
- **etcd and Raft prove the store contract.** Write the etcd sketch against the trait in M1's review, before the trait ships, so M4 does not force a breaking change.

---

## 13. Review addendum, 2026-10-18 (Codex gpt-6-astra, adversarial, read-only)

Codex verdict: REJECT, 10 findings. Each was checked against the source before ruling. The direction holds (separate crate, lockstep + caret versioning, no ext crate, hook shape B). The registration, recovery and lifecycle details below **supersede** §6.1, §6.2 invariants 2/3/7, §7.1 operations, §7.3, §9 and the M1 test row in §11.

### 13.1 Ledger

| # | Finding | Ruling | Evidence checked |
|---|---|---|---|
| 1 | A hub `Authority` lease does not fence the mutation that follows it, and B loses "hide the owner while it re-registers". | **Valid.** | `tests/registration_lifecycle.rs:703` requires stale publish/invalidate to fail **and** credential-free queries to miss while a re-registration that omits the indexer is pending. B's side-effect-free `prepare` keeps the old advertisements visible. |
| 2 | A failed remote `put` may have written (lost response). | **Valid** for etcd/Raft; not reachable in-memory. | Store design; in-memory write is synchronous. |
| 3 | Recovery that replays payloads under the same epoch can resurrect retired publications. | **Valid.** | Retirement state lives in the bundle directory, not in the stored payload; `tests/registration_lifecycle.rs:962` asserts retirement survives. |
| 4 | The record cannot tell an accepted registration from an unfinished one; a crash after `put` locks the client out. | **Valid** for persistent stores. | The candidate credential is minted before the write (`registration/mod.rs:185`). |
| 5 | Recovery can reactivate an incarnation that expired during replay. | **Valid against §7.3**; resolved by 13.2 rule R5, which reuses kvbm's `synchronize_reservations`. | `registration/mod.rs:111-150`. |
| 6 | Remote backends lack freshness, watch-loss and leader-fencing semantics. | **Valid.** M4/M7 design gate; M1 trait must carry a continuity signal. | Plan §7. |
| 7 | `Velo::register_peer` can still fail after decoding (endpoint parse, NUMA mismatch); no failure path for it or `record_write`. | **Valid.** | `lib/velo/src/lib.rs:1013-1038`; kvbm handles both at `transaction/mod.rs:49,89`. |
| 8 | Dropped handler futures leave the owner `Registering` forever; a hung `prepare` holds the lock forever. | **Valid.** hyper drops in-flight service futures when the connection closes, and `RegistrationPermit` has no `Drop`. This is latent in kvbm today too (see 13.3). | `registration/mod.rs:28`. |
| 9 | Startup installs the irreversible hook before fallible recovery; HTTP serves before `start`; shutdown stops extensions before draining transactions. | **Valid.** | kvbm installs the hook last on purpose: `server.rs:414`. |
| 10 | "Path changes only" is wrong: lifecycle tests assert replay-restore; publishing before a real port risks freezing a wrong API; docs cannot wait for M6. | **Valid.** Also my counts were wrong: 14 lifecycle tests, 54 integration, 16 registry, 6 credential unit tests. Repo `CLAUDE.md` requires book + README updates with every merge to `main`. | `tests/registration_lifecycle.rs:962` asserts `restored_incarnation != initial_incarnation`, an artifact of replay. |

Codex agreed on versioning: caret is fine within a pre-1.0 minor, `=` would not stop an unrelated dependency picking another minor, and a `velo-hub-ext` exposing `Velo` and `Router` removes no constraint.

### 13.2 Revised rules

**R1. `prepare` may make reversible, fail-safe changes.** Supersedes invariant 2. (Approved by the user 2026-10-18.) `prepare` may *hide* or *revoke* (never add, never destroy). Every such change is owned by `Staged` and undone by its `Drop`. This is what kvbm's `stage_registration` does, made structural. The indexer's prepare hides the owner's advertisements and closes its own publish path; dropping `Staged` reopens both.

**R2. Extensions keep their own per-owner fence; the hub keeps the secret.** Corrects the "delete the fork" claim. The hub verifies the credential (`Authority::authorize -> Lease { incarnation, epoch }`). The extension keeps `{epoch, participating, visible}` per owner in its own state, written at `commit`, cleared at `on_removed`, hidden at `prepare` (R1). A mutation applies only if `lease.epoch` equals the extension's committed epoch, checked under the extension's own lock in the same critical section as the write. Only the duplicated credential secret and its staging state machine are deleted from the indexer.

**R3. The transaction is a hub-owned task.** The HTTP handler spawns it and awaits the `JoinHandle`; a client disconnect cannot abandon it. `prepare` and `put` each run under a deadline; expiry aborts like any failure. The removal callback only enqueues; it never waits on the registration lock or on lifecycle delivery. Hooks running under the lock never await lifecycle delivery. Cleanup is idempotent and incarnation-checked because the direct failure path and a queued removal can both deliver it.

**R4. One post-write failure path.** Supersedes invariant 7's after-write half. Any failure after `put` (`record_write`, `Velo::register_peer`, credential commit) runs the same steps: drop or reverse `Staged`, conditional store remove, `on_removed(id, new incarnation)` on every extension, `Velo::unregister_peer(id)` (safe under the lock because the owner is `Busy`, so no replacement exists), fail the credential closed, answer 409. The queued-removal path calls `Velo::unregister_peer` only if the store still says the removed incarnation is current or absent, so a delayed removal cannot disconnect a replacement.

**R5. Recovery restores discovery, not authority.** Supersedes §7.3. On startup with a persistent store, stored registrations become `Reserved` through the ported `synchronize_reservations` (which already handles expiry during startup). Peer discovery keeps working. The owner's next heartbeat answers "re-register"; the owner re-registers presenting its stored credential, which mints a **new epoch** through the normal path, so publishers resync into a fresh generation domain (`tests/registration_lifecycle.rs:1075`). Extension state is never replayed from payloads. The record keeps `accepted_digest` (last answered) and `pending: Option<{digest, op_id}>`; recovery accepts either digest, so a crash between `put` and the response cannot lock a client out.

**R6. Store writes are identified and may be indeterminate.** `put(record, op_id, expected: Option<Incarnation>) -> Result<Incarnation, StoreError>` where `StoreError::Indeterminate` means "may have been written". On `Indeterminate` the hub reads back by `op_id` before deciding between abort (R4 not needed) and fail-closed (R4). `remove` and `touch` get the same treatment. In-memory never returns `Indeterminate`.

**R7. The store reports continuity.** `HubStore::continuity() -> watch::Receiver<Continuity>` (`Live`, `Degraded`, `Lost`). While not `Live`, `Authority::authorize` fails closed and new registrations answer 503; discovery reads continue from the cache and are advisory. Watch compaction or leader loss sets `Lost` and forces a resync (R5 path). etcd: one fresh lease per incarnation; a heartbeat refreshes the lease captured with its incarnation, never a replacement's. Raft fencing is M7's design note.

**R8. Startup and shutdown.** Supersedes §9.

Startup: validate graph, bind sockets (admission closed: 503), build Velo, pin the hub entry, `attach` in order, `start` in order, install the removal hook and synchronize reservations (last fallible step, as kvbm does), spawn reaper/heartbeat/lifecycle, open admission. A failure at any step unwinds every earlier step in reverse, and the hook is only installed if everything before it succeeded, so an injected store stays reusable.

Shutdown: close admission, wait for in-flight transactions (take the registration lock), stop extension background producers, drain lifecycle delivery, `shutdown` extensions in reverse order, cancel heartbeat/reaper, drain HTTP listeners, remove the pinned entry, `Velo::shutdown(policy)`.

**R9. Prove the API with a real port before publishing.** New gate: velo-hub 0.21.0 is not published until a K1 spike (kvbm indexer + p2p as extensions, built against velo-hub by path or git) passes kvbm's lifecycle suite with every assertion classified as **kept** or **intentionally changed**, and until the store trait has an etcd sketch that compiles against it. K0 (velo 0.20 upgrade) therefore lands before the spike.

**R10. Docs ship with each merge.** M6 is removed. Each velo PR updates the book and `README.md`, per repo `CLAUDE.md`.

### 13.3 Side finding in kvbm (not part of this plan)

Finding 8 applies to kvbm today: `register_instance` awaits inline in the axum handler and `RegistrationPermit` has no `Drop`, so a client that disconnects while the handler is suspended leaves the owner `Registering`; every later register or heartbeat for that owner answers `Busy`. It is only reachable when a manager's `on_register` actually suspends. Per the validation rule, it needs a failing test in kvbm before any fix.

### 13.4 Test plan corrections (supersede the M1 row)

- Port with path changes only: `tests/integration.rs` (54), `src/registry/tests.rs` (16), `src/registration/tests.rs` (6).
- `tests/registration_lifecycle.rs` (14): classify every assertion before porting. Known intentional changes: `:962` restored incarnation differs (replay artifact; under B the invalid address is rejected before the write, so the incarnation is unchanged and everything else the test asserts still holds). Known keeps: `:703` (stale credential rejected and query misses during a pending omission; needs R1 and R2).
- New: client disconnect mid-transaction does not leave the owner `Busy`; `prepare` deadline aborts and releases the lock; `Indeterminate` put resolved by read-back both ways; post-write `register_peer` failure runs R4 and leaves no velo peer; delayed removal of an old incarnation does not unregister a replacement's velo peer; continuity `Lost` closes authorization; startup failure after `start` unwinds and leaves the store reusable; shutdown waits for an in-flight registration.

### 13.5 V0 follow-up, 2026-10-18

The V0 review (Codex gpt-6-astra, bb thread) found three minor issues: a stale `examples/Cargo.lock`, guide wording that claimed `shutdown` runs once (only `graceful_shutdown` does; `shutdown` repeats its streaming step), and a missing README note. Fixing the lockfile exposed a design gap the review could not see, because the examples did not build: `examples/throughput.rs` took `Arc::downgrade(&velo)` so its handler would not keep `velo` alive. The handle removed that option. V0 now adds `Velo::downgrade() -> WeakVelo` and `WeakVelo::upgrade() -> Option<Velo>`, with a test that a handler holding a `WeakVelo` does not keep the instance alive.

Rule for velo-hub: extension handlers and spawned tasks that reach the hub's `Velo` hold a `WeakVelo` and upgrade per call. Contexts hand out `Velo`; registering a handler that captures a strong clone is the cycle to avoid.

---

## 14. M1 implementation addendum, 2026-10-18

M1 is implemented on `bb/m1-velo-hub-server-core-thr_juy4b2a2x8` (stacked on V0). The plan held; these are the places the code departs from it or settles what it left open. Evidence is the test named, all in `lib/velo-hub`. The assertion ledger is `agent-docs/velo-hub-m1-ledger.md`.

### 14.1 Departures

| Plan | Code | Why |
|---|---|---|
| §6.1: decode the peer address after `begin`, always | Decoded before the lock, and only when the hub runs velo (`registration::decode`) | Pure validation needs no lock. Without velo the address is opaque; kvbm's own tests register `b"test-addr"` on velo-less hubs. |
| §6.1: `commit` then `register_peer` | `register_peer` before extension `commit` | A failure there then only drops `Staged`; the R4 path covers both orders anyway. |
| §5.1: `admit(&self, view)` | `admit(&self, view, payload: Option<&Self::Registration>)` | The payload is decoded once, before admit; admit needs it for config checks (kvbm's PrimaryConfig). |
| §5.1: `attach` required | `attach` has a default | Most toy and real extensions need none. |
| R6: `put(record, op_id, expected)` | `put(record, expected)`, op id is `record.op` | The record stores it for read-back anyway. `resolve(id, op)` is async. |
| R3: removals only enqueue; lifecycle delivers | Also: `DELETE` takes the lock and delivers its own removal | kvbm's observable behavior is that extension lists are empty when DELETE returns (`unregister_removes_from_extension_lists`). The queued copy is a no-op. |
| R4: "drop or reverse Staged, conditional remove, on_removed" | Order is remove, `on_removed`, then drop `Staged`; `Staged::drop` must be epoch-fenced | Dropping first un-hides prior state that is about to be removed. Documented on `Extension`. |
| (not in plan) panicking extension | The transaction owns permit and staged values; its `Drop` aborts before the write, and after it fails closed, delivers `on_removed` synchronously (lock still held), then queues `Job::Retire` for the store remove | Otherwise a panic leaves the owner Busy forever (`panicking_prepare_*`, `panicking_commit_fails_closed`, `a_panic_after_the_write_never_shows_the_prior_state_again`). |
| (not in plan) an indeterminate write whose read-back also fails | Drop the prior registration's extension state, forget the velo peer, reserve whatever the store holds, answer 503 (`indeterminate_put_that_cannot_be_resolved_drops_and_reserves`) | Neither outcome is knowable; fail closed. |
| §4 crate layout `routes.rs` errors | Every core-route failure is an `ErrorBody`, including axum's body/path rejections and unknown routes/methods (`HubJson`, `HubPath`, JSON fallbacks, new code `method_not_allowed`) | Clients act on the code. Found in review. |
| Authority storage: kvbm's `Mutex<HashMap>` | Same map under one mutex for transitions, plus a sharded read view (`DashMap`) written under that mutex | Measured: with readers on one `RwLock`, 8 threads made 9.4 M authorize calls/s in total, fewer than one thread (20.7 M/s). With the view: 52 M/s. Cross-owner atomicity of `synchronize_reservations` against the removal hook is kept because every transition still holds the one mutex. Same pattern for the in-memory store's discovery lookups. |
| `HubStore` | Added `peers() -> Vec<StoredPeer>` (no payloads) | The heartbeat lists every tick; `list()` deep-copies payloads under the read lock (19 ms vs 0.3 ms for 10,000 entries with 2 KiB payloads). |
| §3.2 rule 1 | `velo-hub` has `publish = false` | R9 gates publishing on the K1 spike; CLAUDE.md rule 1 says new crates start unpublishable. |
| §10 | `RegisterResponse { instance_id, hub_instance_id, credential, epoch }`; `GET /v1/instances` returns `{ peer, extensions, pinned }`; error codes add `busy`, `register_again`, `unavailable`, `bad_gateway`, `method_not_allowed` | Clean protocol; a client must tell "retry" from "register again". |

### 14.2 Open

- **R5 is half done.** The record carries `accepted` and `pending` credential digests, but the ported state machine still answers 401 for a `Reserved` owner whatever it presents. M4 must let `begin_registration` accept a `Reserved` owner presenting a credential whose digest matches either stored digest, and must add the test.
- **§12 lock-hold metric** is not implemented; the transaction logs the hold time at `debug`.
- **`client` feature** is an empty placeholder so `protocol` is proven to build without `server`.
- **Shutdown order (R8)** drains HTTP after extensions shut down, so an extension route can run after its extension's `shutdown`. Kept as specified; revisit if an extension needs routes closed first.
- **`Velo` peer-registration check** is decode-only before the write. A peer whose address decodes but no hub transport accepts fails after the write and fails closed (`post_write_register_peer_failure_*`). A pre-write dry run would need a velo API that does not exist.

### 14.3 Plan errors found

- §11 and §13.4 counts were both wrong. Actual: `tests/integration.rs` 59, `src/registry/tests.rs` 17, `src/registration/tests.rs` 6, `tests/registration_lifecycle.rs` 14, `tests/heartbeat_integration.rs` 5.
- "Port `tests/integration.rs` with path changes only" was wrong: 13 of its tests exercise kvbm's `HubClient` (M2) and 8 exercise kvbm-owned features (K1). 33 port to M1, one of them rewritten (its DELETE came from a client drop guard); 5 handler tests merge into `protocol` unit tests. Seven CD tests are about the hub's behavior and port with a toy `Role` extension.
- R4's step order (drop Staged before on_removed) would re-expose state; see 14.1.

### 14.4 After review, 2026-10-18

Three review rounds (record: `agent-docs/velo-hub-m1-review.md`) changed these behaviors from 14.1:

- **Unregistration is a hub-owned task**, like registration. hyper drops a handler when its client closes the socket.
- **Every wait under the registration lock or in shutdown is bounded**: store puts, read-backs and removes, `on_registered`, and the heartbeat's store calls run under `step_timeout`; each extension `shutdown` and hub task join, under `shutdown_timeout`. Panics in `attach`, `start`, `shutdown` and route building are caught and take the unwind path.
- **Shutdown and startup run as their own tasks**, so a caller that stops waiting cannot leave the hub half stopped; a dropped `Hub` stops in order on the runtime that served it. Shutdown releases the lock before the HTTP drain, then ends requests still in flight with 503 (a per-request layer; axum's connection tasks outlive an aborted server).
- **Routers are built before the removal hook.** They run extension code that can panic, and nothing after the hook may fail.
- **A reservation is cleared by any removal of its owner** (one ported transition changed; see the review record, round 3 row 1).
- **`ServerConfig::validate`** refuses zero intervals and timeouts.
