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
## 15. M2 plan: the `client` feature, 2026-10-19

M2 adds the client half, on top of M1. It ports kvbm-hub's `src/client.rs`, `src/handlers.rs` (heartbeat only) and `tests/client_unregister.rs`, plus the 13 `integration.rs` tests the M1 ledger deferred to M2 (rows 6, 26-37) and the drop half of row 38.

### 15.1 Shape

| Type | Role |
|---|---|
| `HubClient` | Cheap-clone handle (`Arc<Inner>`). HTTP to both listeners. Owns at most one registration: instance id, credential, epoch, `hub_instance_id`. `register`, `unregister`, `send_heartbeat`, `registration()`, `ext(name)`, `discovery()`, `velo_builder()`. Dropping the last clone sends `DELETE` best-effort on the current runtime. |
| `HubDiscovery` | Discovery-only cheap-clone handle (no registration). Implements `PeerDiscovery`. `velo_builder()` installs this one. |
| `ExtClient` | JSON helpers scoped to `/v1/ext/{name}` on either listener; `*_authorized` variants add the registration's credential. Bundle crates build typed clients on it. |
| `ClientError` | Typed: `Hub { status, code: ErrorCode, message }`, `Transport`, `Decode`, `NotRegistered`, `InvalidUrl`. `code()` lets a caller act on `busy` / `register_again`. |
| `HubClientBuilder` | `host`, `scheme`, `public_port`, `control_port`, `from_url`, `http_client`. |

### 15.2 Rulings (settled; do not re-litigate during implementation)

- **R2-1. Discovery is a separate handle.** kvbm passed `Arc<HubClient>` to velo as discovery, so velo kept the registration alive and dropping the user's client never unregistered. `HubDiscovery` shares only the HTTP client and URLs. Test: dropping the user's `HubClient` while a velo built from `velo_builder()` lives still sends `DELETE`.
- **R2-2. `register` re-registers.** With a registration held, `register` presents its credential and the hub mints a new credential and epoch (the M1 protocol). kvbm's `client_register_twice_errors` becomes **intentionally changed**: the second call re-registers. If the hub answers `register_again`, the client drops its registration and returns the error; the caller registers again.
- **R2-3. Two entry points.** `register_peer(peer, extensions)` is the primitive (kvbm's tests register bare `PeerInfo`s on velo-less hubs). `register(&velo, extensions)` installs the heartbeat handler on that velo first (velo replaces a handler of the same name, so this is safe to repeat), so the hub can probe the moment it answers. The handler holds a `Weak` of what it records (last `seq`, arrival time) and of the client's "holding" flag.
- **R2-4. Unregister keeps state on failure.** Success, 404 or 401 (the credential is no longer this registration's) clears the registration; any other answer keeps it, so an explicit retry or the drop retries (`client_unregister.rs`, both tests ported).
- **R2-5. No background loops in M2.** No client-side HTTP heartbeat loop and no automatic re-registration; the hub's velo heartbeat is the liveness path. Revisit if K1 needs it.
- **R2-6. Dependencies.** `client = ["dep:reqwest", "dep:url", ...]` with reqwest `default-features = false, features = ["json"]`. TLS is the caller's choice through `http_client`.

### 15.3 Tests (written first)

Ported: rows 6, 26-37 of the M1 ledger; the drop half of 38; `client_unregister.rs` (2). New: two Velos built from `velo_builder()` discover each other through a hub and exchange a message; the hub's velo heartbeat reaches the client's handler and keeps it registered; R2-1 drop test; R2-2 re-registration rotates credential and epoch and the old credential stops working; a typed `ClientError::Hub` carries `ErrorCode` (bad payload -> `bad_request`, stale credential -> `unauthorized`); `ExtClient` reaches an extension's public and control routes, with and without the credential.

### 15.4 Added during implementation, 2026-10-19

- **R2-7. The handler answers `ok` only while the client holds or is getting a registration.** Mega review round 3 of M1 found that a lost registration response locked the owner out while it answered the hub's probes. The hub now counts `ok: false` as a failed probe (M1 fix ab987f1); the client sets a `holding` flag when `register_peer` starts and stores `registration.is_some()` when it ends, so a probe that arrives before the response is still answered ok. A dropped client (the handler's `Weak` fails) answers not ok. Test: `a_client_without_the_registration_answers_not_ok_and_is_freed`.
- **Velo sends to `.instance(id)` do not consult discovery**; `.worker(id)` targets and `Velo::discover_and_register_peer` do. The two-velo test addresses the peer by worker id.
- Mutation checks: velo's discovery holding the client (R2-1) fails `dropping_the_client_unregisters_while_its_velo_lives`; `register` not installing the handler fails `the_hub_heartbeat_reaches_the_client_and_keeps_it_registered`.

### 15.5 After review pass 1, 2026-10-19

- **R2-8. One state cell.** `Shared { state: Mutex<State { response, acquiring }>, seq, at }`; an `Acquiring` guard clears `acquiring` on every exit, so a dropped `register` future cannot leave the handler answering ok (pass 1 A1). `op: tokio::Mutex<()>` only serializes register and unregister.
- **R2-9. `ExtClient` holds the registration weakly** (pass 1 A2), like the handler; its control calls answer `NotRegistered` once every `HubClient` is gone.
- **R2-10. Default timeouts**: connect 5 s, request 60 s (pass 1 A3). M3's watch stream needs a per-request override.
- **R2-11. "Owns nothing" drops the registration** on every call that presents the credential: `unauthorized`, `register_again`, `not_found`, or a raw HTTP 401 (pass 1 A4).

### 15.6 After review pass 2, 2026-10-19

- **R2-12. A dropped re-registration fails closed** (pass 2 A1): the `Acquiring` guard carries the presented credential and forgets it when the call ends without an answer. Cost: a re-registration cancelled before it reached the hub also drops the registration; the caller registers again.
- **R2-13. `ExtClient` forgets a rejected credential** (pass 2 A2): `credential_rejected` (unauthorized, register_again, raw 401) on any call; `owns_nothing` adds `not_found` for core routes only.
- **R2-14. Orphan flag** (pass 2 A3): a credential-free register refused `unauthorized` sets `orphaned`; the handler then ignores `acquiring` until a register succeeds, so a tight retry loop cannot reset the hub's failure counter. The tight-loop test failed 3 of 4 runs before, passes 5 of 5 after.

## 16. M3 plan: services and watch, 2026-10-19

Plan §8.2 scope: service entries owned by a registered peer that die with it; `GET /v1/services`, `GET /v1/services/{name}`, `GET /v1/services/{name}/watch` (SSE), `PUT`/`DELETE /v1/services/{name}/instances/{id}` on the control listener with the owner's credential; `HubClient` as `velo::discovery::ServiceDiscovery`.

### 16.1 Rulings

- **R3-0. Implementation: a built-in extension.** Every extension is prepared and committed on every registration (payloads are `Option`), so `ServiceDirectory` implements `Extension` under the reserved name `services`, is added first in start order, and gets the transaction's ordering, panic boundaries and removal delivery for free. Its routes are core routes under `/v1/services` (it mounts nothing under `/v1/ext`). A user extension named `services` is a duplicate-name error.
- **R3-1. Services are a hub-core component with the extension rules.** It keeps `owner -> (incarnation, epoch, set of names)` and `name -> set of owners`. `commit` records the owner's incarnation and epoch and keeps its names (a re-registration rotates the credential; the peer still serves the same services). `on_removed(id, incarnation)` drops the owner's names only if the removal is of its current incarnation or a later one (stale removals are ignored). A `PUT`/`DELETE` authorizes (`Authorized` -> `Lease`) and applies only if the lease's incarnation equals the recorded one, under the component's own lock (R2 fence). It is not an `Extension` in the graph: its routes are core routes, and it is always present.
- **R3-2. Watch = snapshot + subscribe under one lock.** Per name, a `tokio::sync::broadcast` channel. The SSE handler takes the lock, reads the set, subscribes, releases: no event between snapshot and subscription is lost or duplicated. Events: `initial` (instance ids), `added`, `removed`. A subscriber that lags (broadcast `Lagged`) gets a `disconnected` event and the stream ends; the client re-watches. The stream also ends when the hub stops: the directory's `shutdown` hook cancels a token every stream watches, and extensions shut down before the listener drain, so the drain never waits out an SSE stream. The server sends an SSE keep-alive comment every few seconds.
- **R3-3. Names** are one URL segment, `[A-Za-z0-9._-]{1,128}`; anything else is 400.
- **R3-4a. Watch transport.** reqwest's total request timeout would cut a long-lived stream. The default client builds a second HTTP client for watches: the same connect timeout, an idle read timeout (longer than the keep-alive), no total timeout. A caller-supplied `http_client` is used for watches as is, and its timeouts apply.
- **R3-4. Client.** `HubClient::services()` returns `HubServices`, implementing `ServiceDiscovery` (list, get, watch over SSE; a dropped or failed stream yields `Disconnected` then ends). `HubClient::register_service(name)` returns a `HubServiceRegistration` (implements `ServiceRegistrationGuard`; drop sends `DELETE` best effort). After `register` re-registers, existing service entries carry over on the hub, so the client re-puts nothing.
- **R3-5. Not persisted.** Services live in hub memory, like extension state; M4 recovery restores peer discovery only, and owners re-put their services when they re-register (recorded as M4 input).

### 16.2 Tests (first)

watch order (initial then deltas, no gap or duplicate under concurrent puts); entries die with their owner (DELETE, expiry, heartbeat eviction); credential required (401 without, 409/401 with a stale one); a stale removal does not drop a re-registered owner's services; a re-registration keeps the services; a lagging watcher gets `disconnected`; a watch ends when the hub stops and shutdown is not delayed; client `ServiceDiscovery` round trip between two velos; the client's watch reports `Disconnected` when the hub goes away; invalid names 400.

### 16.3 After review pass 1, 2026-10-19 (record: /tmp/m3r1/report.md)

- **R3-6. One guard per name; the names follow the registration** (pass 1 A1). The client keeps the names it serves; `register_service` refuses a second guard for a name (`AlreadyServing`), and every successful `register` puts every name again. A stale guard can no longer exist, so it cannot delete another guard's entry; and a fresh registration or a restarted hub gets the services back without caller action (the restart half of M4 D-8, done here).
- **R3-7. The hub does not probe an owner whose registration transaction runs** (pass 1 A2: with `orphaned` set, a slow conforming `on_registered` let the probes evict the entry before the answer). An `InFlight` set, entered by the transaction and left when it is settled, filters the heartbeat's registrations.
- **R3-8. One credentialed-call path** (pass 1 A3): `Shared::credentialed` applies the forget rule (core routes: rejected or not_found; extension routes: rejected) for every call that presents the credential; the client module is split (`client/{mod,services,sse}.rs`).
- **R3-9. No answer includes a timed-out or broken request** (pass 1 A4): only a connect or builder error proves the request never left.
- Also: the directory is moved to the front of the start order after `plan`; reads sort outside the directory lock; the watch ring is 128; `Acquiring::drop` takes one lock; the 50 s bound counts the built-in.

### 16.4 After review pass 2, 2026-10-19 (record: /tmp/m3r2/report.md)

- **R3-10. The registration carries the services; one writer between registrations** (pass 2 A1, proven four ways: a dropped guard's detached DELETE removed the next guard's entry in 34 of 40 rounds; a guard dropped or unregistered during a re-registration left its entry; a busy refusal could not be retried). The directory's payload is the owner's whole set (`Registration = BTreeSet<String>`; `commit` replaces); the client sends its `serving` set on every registration and records it as `confirmed`; between registrations `sync_services` (under the client's `op` lock) is the only code that sends service requests, one at a time, until `serving == confirmed`. This is pass 1's S6 plus the reconciler.
- **R3-11. "Answered" is positive** (pass 2 A2): only the hub's own response or a connect/builder error; a proxy's 502/504, a timeout, a broken body are no answer and fail closed.
- **R3-12. `register` answers Ok once the registration is held** (pass 2 A3); a later service-sync failure is left for `sync_services`.

### 16.5 After review pass 3 (final), 2026-10-19 (record: /tmp/m3r3/report.md)

- **R3-13. `.` and `..` are not service names** (pass 3 A0; pass 2 F1/F2): `Url::join` removes dot segments, so a guard for `..` sent its DELETE to `/v1/instances/{id}` and unregistered the instance, and `.` hit no route and the 404 made the client forget a live registration. Both refused on both sides.
- **R3-14. An `unsettled` set** (pass 3 A1): a name enters it before its request and leaves on a hub answer; `sync_services` sends unsettled names first, as `serving` says. A PUT the hub applied but whose answer was lost, then a dropped guard, no longer leaks the entry.
- **R3-15. The hub answers `register_again` when the prior registration is gone** (pass 3 A2): from `fail_unresolved` (was 503), and from a registration that panicked after the write when the presented credential no longer authorizes (was 500). Five hooks tests updated from 500/503 to 409.
- The review cap (3 passes) is reached; M3 lands after these.
## 17. M4 plan: the etcd store and recovery, 2026-10-19

Plan scope (§11 M4, R5, R6, R7): `etcd` store + recovery. Tests: conformance against etcd on :2379; lease expiry fires the removal hook once; a hub restart keeps registrations and credentials valid.

### 17.1 `EtcdStore` (feature `etcd`, `velo_hub::store::etcd`)

- **Keys.** `{prefix}/i/{instance_id}` holds the record (JSON: `RegistrationRecord` + lease id). `{prefix}/w/{worker_id}` holds the owning instance id. `{prefix}/op/{op_id}` is a tombstone (below). Default prefix `/velo-hub/default`. One hub per prefix (HA is M7).
- **Incarnation = the instance key's `mod_revision`.** etcd revisions only grow, so incarnations increase per instance (the contract since M1 round 2).
- **put.** Grant a lease (TTL, default 30 s), then one txn: compare the instance key (`mod_revision == expected`, or `version == 0` for `None`), the worker key (`mod_revision` equal to the one the cache holds, 0 if absent; a worker key owned by another instance is `Conflict` before the txn), and the tombstone `{prefix}/op/{op}` absent; then put both keys with the lease. The txn's header revision is the incarnation. Revoke the replaced incarnation's lease afterwards (best effort). A transport error or timeout after the request was sent is `Indeterminate`; a failed compare is `Stale`/`Conflict`.
- **resolve(id, op) is settled by a tombstone.** A timed-out etcd write can still apply later, so a plain read cannot promise "will not apply". `resolve` runs one txn: put `{prefix}/op/{op}` (short lease) if absent, then read the instance key. A put that applied before the tombstone shows `record.op == op` (answer its revision); one that arrives after fails its tombstone compare and never applies (answer `None`).
- **remove(id, inc).** Txn: compare `mod_revision == inc`, delete both keys; revoke the lease after.
- **touch(id, inc).** Look the incarnation up in the cache; keep alive the lease captured with it (one keep-alive round trip). A replacement has a different lease, so a late touch cannot refresh it (R7). TTL 0 in the answer is `NotFound`.
- **Pinned entries get a lease too**, kept alive by the store's liveness task: a hub that crashes loses its pinned entry after one TTL instead of leaving it for ever (M1 observation).
- **Cache, hook, continuity.** A task loads the prefix at revision R, then watches from R+1 with `prev_kv`. PUT updates the cache; DELETE (unregister, lease expiry) removes it and calls the hook with `prev_kv.mod_revision`, outside the lock. `install_removal_hook` sets the hook and snapshots the cache under the cache lock (same linearization as `InMemoryStore`). A watch error sets `Degraded`; reconnect resumes from the last revision; a compacted revision sets `Lost`, reloads the prefix, reports every entry that vanished in the gap as a removal, then `Live`.

- **Watch barrier: a call's removals are reported before it returns.** The M1 hub relies on the in-memory linearization: when `put`, `remove` or `resolve` returns, the removal hook has already run for every removal that call caused or raced (the D1/D2 fixes read `progress.removed` right after `resolve`; `unregister_now` delivers its own removal and relies on the hook having run, so the late hook cannot insert an unreclaimed `State::Removed` or leave the owner Busy after a 204). etcd reports removals through the watch, after the call. So every mutating call waits until the watch task has applied its txn's header revision (a `watch::Sender<i64>` of the last applied revision), under the call's deadline; a timeout is `Indeterminate`. The watch covers the whole prefix, tombstones included, so every txn's revision is observed. This becomes a `HubStore` contract rule with a sequential conformance check (a remove, then the hook's record checked before anything else runs).
- **One hub per prefix, enforced.** At connect the store takes `{prefix}/hub` (txn: create if absent, on the store's own kept-alive lease). A second hub on the prefix fails to start with a clear error. A clean stop revokes the lease; after a crash the next hub waits up to one TTL.
- **touch cost.** One keep-alive round trip per touch, from concurrent probe tasks, is about N/interval keep-alives per second (1,000/s at 10,000 registrants and 10 s). The store keeps one shared keep-alive stream (a task multiplexing requests by lease id) instead of a stream per call; measured at 10,000 leases before the book states a number.

### 17.2 Recovery (R5)

- **Reserved remembers the digests.** `synchronize_reservations` takes the snapshot's `accepted`/`pending` digests. `begin_registration` for a `Reserved` owner presenting a credential whose digest matches either one proceeds like a re-registration: `previous = Lease { incarnation, epoch: record.epoch }`, put with `expected = Some(incarnation)`, a new credential and a new epoch. Extension state is not replayed; extensions see `previous` with no state of their own, as for an owner they never heard of.
- **New error code `recovered`** (409): `authorize` for a `Reserved` owner whose credential matches answers it, meaning "the hub restarted; register again presenting this credential". The client keeps its credential on `recovered` (it is not a rejection), so the caller's next `register` re-registers.
- **The hub tells its velo about recovered peers** at startup (`register_peer` for each snapshot entry, best effort), so its probes reach them; without it every recovered entry failed its probes and was evicted (M1 round 2 observation).

### 17.2a Recorded M4 inputs, answered

| Input (where recorded) | Answer |
|---|---|
| Hook lags `remove()` (M1 round 3 N1) | the watch barrier |
| `State::Removed` for unknown owners never reclaimed (round 2) | with the barrier, a late hook for an owner already delivered no longer happens on the hub's own calls; a removal for an owner with no state (expiry of an entry the hub never saw) inserts nothing after sync: `begin_revocation` only records `Removed` before the first sync |
| Removal of an incarnation newer than the snapshot between hook install and sync (round 2) | `synchronize_reservations` drops a `Removed` record whose incarnation is newer than or equal to the snapshot's, not only equal |
| Continuity `Lost` -> `Live` does not reconcile (round 2) | on `Live` after `Lost`, the store has already reported the gap's removals as removals; the hub re-runs `synchronize_reservations` with the store's fresh snapshot so entries written by others (none, with the prefix lock) or vanished are reconciled; authorize stays closed until then |
| Pinned entry left after a crash (round 1) | pinned entries have a kept-alive lease |
| Preloaded entries evicted because velo has no peer (round 1) | startup registers snapshot peers with velo |
| `accepted`/`pending` digests never read (round 1) | Reserved keeps them; matching credential re-registers |

### 17.3 Tests

Conformance suite against etcd (`ETCD_URL`, default `http://127.0.0.1:2379`; the etcd test binary needs it, as velo's `discovery_etcd` does). New: lease expiry fires the hook exactly once; resolve's tombstone stops a late put (a put whose txn is held behind a gate until after resolve); a compacted watch reports `Lost` and the removals of the gap; restart: hub A registers a client with etcd, hub A stops without unregistering, hub B starts on the same prefix, the client's heartbeat answers `recovered`, its `register` with the credential succeeds with a new epoch, a stranger's credential-free register is refused; a pinned entry expires after its hub dies.

### 17.4 After the plan review (Fable, adversarial, read-only), 2026-10-19

The review: `/tmp/m4-plan-review.md`. Rulings, each on its evidence; these supersede 17.1-17.2 where they differ.

- **D-1. One custody lease per hub, not a lease per incarnation (departure from R7, for the user to ratify).** Evidence: per-incarnation leases need one keep-alive gRPC stream per touch with etcd-client 0.17 (`LeaseClient::keep_alive` opens a bidi stream per call; `LeaseKeeper` is bound to one lease, H4), about 1,000/s at 10,000 registrants; three round trips per put and a leaked lease per failed put (M7); a lease TTL coupled to the heartbeat cadence that can expire a healthy registrant after one skipped tick and one slow probe (H3); and a recovery window equal to that TTL (H2). R7's stated reason ("a heartbeat refreshes the lease captured with its incarnation, never a replacement's") is met by the incarnation check that `touch` needs in both designs. So: every key of the hub is written with one custody lease (long TTL, its own setting, default 5 min), kept alive by the store; `touch` is a cache timestamp with the incarnation check (as in memory); a store reaper deletes entries untouched for the registrant TTL with a conditional delete, reported by the watch like any removal. A new hub keeps alive the custody leases it finds in loaded records until no record references them.
- **D-2. Fencing (C3).** The lock key `{prefix}/hub` has its own short lease, revoked on a clean stop. Every txn compares `Compare::lease(lock_key, Equal, my_lock_lease)`. A failed lock compare, or a keep-alive answering TTL 0 for the lock lease, puts the store in a terminal `Fenced` continuity, and the hub stops itself.
- **D-3. The barrier holds for every txn (C1).** Every txn's failure branch writes a barrier key `{prefix}/b`, so every txn has a revision in the prefix that the watch reports. One round trip still; no progress notifications (CI runs etcd 3.5.0, before the 3.5.8 fix of etcd issue 15220). The failure branch also reads the instance key, so `NotFound` and `Stale` are told apart without a second request. "Applied" means the hook has returned for that revision (M2).
- **D-4. The store owns its tasks (C2).** `EtcdStore::connect` starts the watch, the reaper and the keep-alive; `close` (and `Drop`) stops them. `spawn_liveness_task` returns `None` for etcd. A call made while the store is closing whose txn succeeded returns `Ok` and logs that its hook will not run.
- **D-5. A barrier timeout sets `Degraded`** until the watch catches up (M1), so registrations answer 503 while the store lags rather than dropping owners.
- **D-6. Watch details (M2).** Resume from last applied + 1; take a removal's incarnation from the cache entry, not `prev_kv`.
- **D-7. Recovery reads the store, not cached digests (M3).** `Reserved(incarnation)` stays as it is. `begin_registration` on a Reserved owner with a credential reads `store.get(owner)` (under the registration lock), requires `record.epoch.is_some()`, the incarnation equal to the reserved one, and the credential's digest equal to `accepted` or `pending`; then `expected = Some(incarnation)`. `authorize` on a Reserved owner with any credential answers `recovered` (no hashing on the hot path; the register that follows is the check). The recovery put writes both digests anew, so recovery is single-use (M4).
- **D-8. The registrant learns of a restart (H1).** `HeartbeatRequest` and `RegisterResponse` carry `hub_id`. The client's handler compares it with the hub id it registered under; on a mismatch it marks the registration recovered (`HubClient::recovered()`, a watch receiver) and keeps answering ok so the entry survives until the app re-registers. On a successful `register`, the client re-puts every live service registration (R3-5).
- **D-9. Lost -> Live (M5).** The store reports the gap's removals, then flips `Live`; the hub re-runs `synchronize_reservations` from `list()`. `begin_revocation` records `Removed` only before the first sync (a `synced` flag).
- **D-10. Deadlines (M6).** etcd-client requests carry a deadline equal to `step_timeout`; tombstones live at least twice that, rounded up to etcd's whole-second minimum.
- **D-11. No worker keys (L1).** `WorkerId` is a function of `InstanceId`; the cache's worker index is exact because the hub is the only writer (fencing) and every call returns after the barrier.
- **D-12. Contract text (L2).** "A pinned record never expires" becomes "while its hub's custody lease lives"; `spawn_liveness_task` doc covers backends that run their own tasks.
- **D-13. A dead hub's pinned entry.** A new hub that keeps a dead hub's custody lease alive would keep that hub's pinned entry for ever. At startup, after loading, the store deletes (conditionally) every pinned record that is not its own hub's.

### 17.5 Implementation, 2026-10-19

Store (subagent, commits d17a1b3..c842fce, rebased): `EtcdStore` per D-1..D-6, D-10..D-13; departures it reported: D-10 holds on the client side only (etcd-client 0.17 sends no grpc-timeout, so a txn that reached etcd can still apply; the tombstone TTL narrows it); losing the custody lease fences too; `PrefixHeld` fails at once unless `wait_for_prefix` is set (added here); fencing is seen through `Continuity::Fenced`, not a new error variant; the reconnect checks for compaction before reopening. Measured (release, etcd 3.5.13 in Docker): put 2.8 ms, remove 2.5 ms (plain etcd put 2.2 ms), touch 0.17 us, a reaper pass expiring 10,000 entries 165-187 ms.

Hub (here): D-7 (`Authority::recovers`: a Reserved owner presenting a credential whose digest is the record's `accepted` or `pending`, with the reserved incarnation and an epoch, re-registers as a re-registration of the stored one; `authorize` answers `Recovered` (409 `recovered`) to any credential on a Reserved owner); D-8 (`hub_id` in `HeartbeatRequest` and `RegisterResponse`; the client's `recovered()` watch; the payload-carried services come back with the registration); D-9 (`synced` flag; the continuity task re-runs `synchronize_reservations` from `list()` on Live after Lost); `Fenced` stops the hub through a task holding a `Weak<HubInner>`; `stop` closes the store last; startup registers the snapshot's peers with velo; `synchronize_reservations` drops a `Removed` record at or above the snapshot's incarnation. Tests: `tests/recovery_etcd.rs` (restart lets the registrant back in, a foreign credential is refused, a fenced hub stops itself), each mutation-checked.

### 17.6 After review pass 1, 2026-10-19 (record: /tmp/m4r1/report.md)

- **A1 (proven):** an aborted recovery registration promoted the owner to `Active` on a credential this hub never issued. The permit now records `recovered`, and `abort_registration` restores `Reserved`.
- **A2 (proven at the authority level):** the resync after `Lost -> Live` took its snapshot outside the hook's linearization, so a removal in between left a phantom `Reserved`. **D-9 is reversed**: no resync. The store reports the gap's removals before it is live, and fencing means no other writer; `synchronize_reservations` is called once, at startup.
- **A3:** the fenced-stop watcher now also ends on the core cancel token. **A4:** doc drift fixed. **S3:** a reopened keep-alive stream sends at once.
- Final pass-1 report added (friend findings): **A3** a store fenced before the continuity task first ran never stopped the hub (the task now acts on the first value); **A5** the etcd reaper ran through the hub's stop after the probes stopped (the store's `spawn_liveness_task` now stops the reaper at the core cancel; `reap_batch` checks before it sends); **A6** recovered peers stayed in velo after their reserved entry went (`RevocationAction::ForgetPeer` -> `Job::ForgetPeer`; test `a_recovered_peer_is_forgotten_in_velo_when_its_entry_goes`, mutation-checked); **A7** `close` marked the lock released before revoking (now revokes first, marks after); **A8** a touch could be acknowledged and then reaped (SeqCst claim/re-check against touch's store/check); `recovered()` doc says `wait_for`; the recovered flag is set under the state lock. Not done: S1 (a lagging watch makes a failed compare `Indeterminate`; kept, because the barrier is what D1/D2 rely on, and `Degraded` already closes registrations), S2 (one load at connect), A10a (an unreadable record at reload), A10c (no recovery arm for unregister; self-heals through eviction). A3, A5, A7, A8 are verified by reading, not by a test.

### 17.7 After review pass 2, 2026-10-19 (record: /tmp/m4r2/report.md)

- **A1 (proven):** a recovery registration that failed before its write answered `register_again` (the M3 pass-3 rule inferred "written" from `authorize` failing, and a reserved owner fails it with `Recovered`), so the client dropped the one credential that could recover. The transaction now shares a `written` flag with the handler; `register_again` on `Internal` when the write happened, or when the credential fails for any reason but `Recovered`. Test: `a_recovery_that_fails_before_its_write_is_not_register_again`.
- **A2 (proven):** at about 7,000 registrants the prefix load exceeded tonic's 4 MiB decode limit, so `connect` failed and a `Lost` store could not reload. KV and watch clients are built through helpers that lift the limit. Test: `a_prefix_at_target_scale_loads` (10,000 records, 6.0 MB load; fails with the default limit).
- The continuity task's first-value `Fenced` arm also marks the authority fenced.
- Final pass-2 report: **A3** a reconnected watch reported `Live` before replaying the outage; reconnect now writes the barrier key and holds `Degraded` (through `lag_until`) until the watch applies it (the probe's cluster-wide revision would never be reached by a prefix watch on a shared cluster). **A4** a touch refused under a reaper claim was not recorded; the pre-filter is gone, so touch stores then checks. The A1 fallback is narrowed to `Unauthorized | NotFound` (not `Unavailable`/`Busy`/`Recovered`). `fail_unresolved` answers `recovered` when a credential was presented and an entry is reserved (that credential recovers it), and the client raises its flag on that answer. A fenced hub skips its pinned remove at stop. The builder refuses a request deadline of two thirds of the lock TTL or more. The stop order in the module doc and the book names the store close. The flaky `a_hung_store_remove_does_not_stop_the_heartbeat` (failed on CI for #130: 100 ms probe deadline with `max_failures = 1`) now uses 300 ms. Not done: the lock keep-alive starting only after `connect` returns (bounded by the deadline check), the velo registration loop over the raw snapshot (narrow), `begin_unregister` recovery arm (A10c), S1/S2.

### 17.8 Review pass 3 (final)

One finding, proven and fixed: **A1** `fail_after_write` answered `register_again` for an entry it could not remove, whose record names the presented credential, so the client dropped the one credential that recovers it and was locked out until the heartbeat evicted the entry. It now answers `recovered` in that case, as `fail_unresolved` does. The pass's probes for the pass-2 fixes (reconnect replay, `prior_gone` narrowing, unresolved write not applied, fenced stop) were mutation-checked and are kept as tests. Below the bar, not done: `reconnect` fencing without checking `closing` (a spurious log line on a closed store), and `fail_closed_registration` dropping the reservation after the code was chosen (one extra round trip).
