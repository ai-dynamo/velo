# The hub

The `velo-hub` crate builds a hub: one service that is the peer discovery for a velo application and also hosts extensions. A program builds the hub in `main`, adds extensions or bundles of extensions, picks a store, and serves it. `velo` does not depend on `velo-hub`. An application that does not use a hub picks any velo discovery backend, as before.

The crate has three cargo features. `server` builds the hub. `client` is reserved for the hub client, which is not in the crate yet. `test-support` adds the store conformance suite and `FaultStore`, a store that fails on request. The `protocol` module (paths, request and response types, ids) builds under either feature, and with no feature.

```rust,ignore
use velo_hub::{Hub, HubConfig};

let config: HubConfig = serde_json::from_str(&std::fs::read_to_string("hub.json")?)?;
let hub = Hub::builder(config)
    .velo(velo::Velo::builder().add_transport(tcp))
    .bundle(MyBundle::new())
    .serve()
    .await?;
hub.run_until_signal().await?;
```

The library reads no files and parses no flags. The program builds the `HubConfig`. Without `.store(...)`, the hub uses an `InMemoryStore`. Without `.velo(...)`, the hub only does discovery: it does not probe registrants, and liveness is the store's TTL.

## Listeners

A hub serves two HTTP listeners:

| Listener | Default port | Routes |
|---|---|---|
| public | 1337 | `GET /v1/health`, `GET /v1/hub`, peer lookups |
| control | 8337 | the core routes of the public listener, plus registration, unregistration, heartbeat, and probe |

Extension routes are under `/v1/ext/{name}/`. An extension's `public_routes` go on the public listener and its `control_routes` on the control listener. The hub checks that each extension name is one URL segment of `[a-z0-9_-]`, so an extension cannot add a route to a core path.

| Path | Purpose |
|---|---|
| `POST /v1/instances` | register: `{ peer, extensions: { name: payload } }` |
| `GET /v1/instances` | list the registrations |
| `DELETE /v1/instances/{id}` | unregister, with the credential |
| `POST /v1/instances/{id}/heartbeat` | refresh liveness, with the credential |
| `POST /v1/instances/{id}/probe` | the hub sends a heartbeat to the instance over velo |
| `GET /v1/peers/instance/{id}`, `GET /v1/peers/worker/{id}` | peer discovery |

A successful registration returns a credential and an epoch. The credential goes in the `x-velo-hub-credential` header of every mutation that the registration owns. Each registration gets a new credential and a new epoch, and the previous credential stops working. Every error that a core route answers is an `ErrorBody` with a stable `code`, also for a body or a path that the hub cannot parse, and for a route that does not exist. A client acts on the code: `busy` means retry, and `register_again` means register again without a credential. Extension routes get the same behavior when they use the `HubJson`, `HubPath`, and `Authorized` extractors and return `HubError`.

## Store

The hub keeps registrations in a `HubStore`. The hub's routes, its registration transaction, and its own velo instance all use the same store. The hub's velo instance uses the store as its `PeerDiscovery`. `InMemoryStore` is the store that ships. It expires an entry that nobody touched for the TTL (30 s by default).

The store contract:

- Each write of an instance gets a new incarnation. An incarnation is never used again for that instance.
- Writes, removals, and touches are conditional on the current incarnation. Work that runs later carries the incarnation that it saw, so it cannot change a later registration of the same instance.
- The removal hook and its snapshot share one point in time. Each entry present at that point is in the snapshot, and the hook hears each later removal once.
- The store reports its continuity. When the store is not `Live`, the hub refuses every credential with 503 and refuses new registrations. Peer lookups continue from the store's local view.
- A write can answer `Indeterminate`: a remote write can apply even when its answer is lost. The hub then asks the store which write produced the current record, by the write's op id.

The record holds the peer, the epoch, the extension payloads, a pinned flag for the hub's own entry, and SHA-256 digests of the credentials. The store never holds a credential, so a copy of the store does not give authority.

Each backend runs the conformance suite in `velo_hub::store::conformance` (feature `test-support`). `FaultStore` in `velo_hub::store::testing` is an in-memory store that fails, stops at a gate, or answers `Indeterminate` on request. It passes the same suite when no fault is set.

## Authority

Only the hub holds credentials. `Authority::authorize(owner, credential)` returns a `Lease` with the incarnation and the epoch of the registration that the credential belongs to. While a registration of that owner is in progress, `authorize` answers `Busy`.

A lease alone does not fence a mutation. A registration can start after `authorize` returns and before the mutation applies. So an extension keeps the epoch that it committed for each owner. It applies an owner's mutation only if the lease's epoch is equal to that epoch, and it does this check under its own lock, in the same critical section as the write.

`authorize` takes a read lock for one hash lookup and a 16-byte compare in constant time, plus one atomic load for continuity. The `Authorized` axum extractor does this for a route with an `{instance_id}` path parameter.

## Extensions

An extension implements `Extension`. It has a wire name, a payload type, a staged type, the extensions it requires, and these hooks:

| Hook | When | Rule |
|---|---|---|
| `admit` | before the registration lock | reject a payload that does not fit the hub's config |
| `prepare` | under the lock, before the store write | can fail; can only hide or revoke, and the staged value undoes that when it drops |
| `commit` | after the write | cannot fail |
| `on_registered` | after the credential is live | runs under `step_timeout_ms` and is dropped at the deadline; spawn work that awaits |
| `on_removed` | when an incarnation leaves the store | drop the owner's state if it belongs to that incarnation or an older one |

Each extension sees each registration. The payload is `None` when the registrant did not opt in. Then `commit` drops what the owner had before. `commit`, `on_registered`, and `on_removed` all run under one hub-wide registration lock, so nothing changes extension state between `prepare` and `commit`.

`requires` sets the attach order and the start order. At attach, `AttachContext::extension::<T>()` returns an extension that is already attached. It fails if `T` is not in `requires`, so a missing declaration fails at once.

A bundle (`Bundle`) adds a set of extensions and can require other bundles. A bundle never builds another bundle: the program adds each bundle once, and two bundles that require a third share one instance of it. A bundle reads its own section, `bundles.{name}`, of `HubConfig`.

The hub checks the graph before it binds a socket. These are errors: two bundles or two extensions with one name, a required bundle or extension that is not added, a cycle, a name that is not one URL segment, and a config section that no bundle owns.

## Registration

```text
handler   spawn the transaction task, and wait for it
task      decode payloads; check registration_requires; admit           400
          take the lock (503 after lock_timeout_ms); begin               401, 409 busy
          prepare each extension, each under step_timeout_ms
            failure: abort; the prior registration is not changed
          write to the store, under step_timeout_ms
            Indeterminate or timeout: resolve the write by its op id
          record the write; velo register_peer; commit each extension;
          commit the credential; on_registered each extension; unlock     200
            any failure after the write: remove the new entry,
            on_removed on each extension, forget the velo peer,
            refuse the credential                                        409 register_again
```

The transaction is a task of the hub. hyper drops a request handler when its client closes the connection. If the handler ran the transaction, a disconnect could leave the owner `busy` for ever. Decoding and `admit` run in the task as well, so a panic in any extension hook answers 500.

Each store write and remove under the lock, and each `on_registered`, runs under `step_timeout_ms`. A store or an extension that never answers cannot hold the lock. A remove that times out counts as indeterminate: the hub reads the store to learn whether it applied.

A failure before the store write leaves the prior registration exactly as it was. A failure after the write removes the registration, including the prior one, and the client registers again. The alternative is to write the prior registration back. kvbm-hub did that, but the restore is a second write that can fail too, and it gives the registration a new incarnation.

If an extension panics, the hub settles the owner. Before the write, it aborts. After the write, it refuses the credential and queues the removal of the entry.

When the hub runs velo, it decodes the peer's address before it takes the lock and answers 400 if the address is not valid. Without velo, the hub stores the address and does not read it.

## Removal

The store calls the hub's removal hook when an entry leaves: an unregistration, a TTL expiry, or a failed heartbeat. The hook revokes the owner's authority at once and puts the removal on a queue. It never waits for the registration lock. A lifecycle task takes the lock and delivers the removal: `on_removed` on each extension in reverse start order, then it forgets the velo peer, then the owner can register again. Until then, the owner's requests answer `busy`.

The lifecycle task forgets the velo peer only if no later incarnation replaced it. So a late delivery does not disconnect a replacement.

`DELETE /v1/instances/{id}` takes the lock and delivers the removal itself. When it answers, each extension has dropped the owner. The hook also queued a delivery, and that second delivery changes nothing. Like a registration, the unregistration runs as a task of the hub, so a client that disconnects cannot leave the owner `busy`.

## Heartbeat

When the hub runs velo, it sends a heartbeat to each registration every `heartbeat_interval_ms` (10 s by default). The registrant answers on the `velo_hub_heartbeat` handler. Each probe times out after one interval. After `heartbeat_max_failures` (3 by default) failures in a row, the hub removes that incarnation. A success touches that incarnation. Probes run in parallel, so a stopped peer does not delay the others. The hub does not probe its own pinned entry. Without velo, liveness is the store's TTL.

The heartbeat task holds a `WeakVelo`. Extension handlers and tasks that use the hub's `Velo` must also hold a `WeakVelo`, and upgrade it on each call. A handler that holds a `Velo` is in that instance's own handler registry, so the instance holds itself and is never dropped.

## Startup and shutdown

`HubBuilder::serve` does these steps in this order:

1. Check the bundle and extension graph and the config. This step does no I/O.
2. Bind the two listeners.
3. Build the hub's velo instance, with the store as its discovery.
4. Write the hub's own pinned entry.
5. Attach each extension, in dependency order.
6. Start each extension, in dependency order. Then build both routers, which runs each extension's route code.
7. Install the store's removal hook, and reserve each entry that the store already holds.
8. Start the reaper, the heartbeat, the lifecycle task, and the HTTP servers, then accept registrations.

If a step fails or panics, the hub undoes each earlier step in reverse order. Step 1 also refuses a config with a zero interval or timeout. The removal hook cannot be removed, so the hub installs it last. A store given to a hub that failed to start can go to another hub.

`Hub::shutdown` stops new registrations, waits for the registration that holds the lock, stops extension background work, delivers the queued removals, shuts each extension down in reverse order, and stops the heartbeat and the reaper. Then it releases the lock, so a request that waits for it answers 503 at once. It closes both listeners and waits up to `shutdown_timeout_ms` for the requests in flight. Then each request still in flight answers 503 and its handler is dropped, and the hub waits up to `shutdown_timeout_ms` again for the connections to close. So the listener drain can take two times `shutdown_timeout_ms`. Each extension's `shutdown`, each hub task, and the velo drain also get `shutdown_timeout_ms` each. Last, it removes the pinned entry and shuts the hub's velo instance down. When it returns, the listener ports are free.

The stop runs as its own task. A caller that stops waiting does not stop it, and a second call waits for the same stop. Dropping the last `Hub` clone runs the same stop in the background. `serve` also runs as its own task: if its caller stops waiting, startup finishes and the new hub stops in order.

## Cost of the request paths

Two reads are on the request path: `Authority::authorize`, for each owner-scoped mutation, and the peer lookup. Both read a sharded copy of the state, and the writers update that copy while they hold the one lock that orders all writes. A design with the readers on the writers' `RwLock` was measured first and rejected: every read writes the lock's cache line, so more threads gave fewer calls in total.

Setup: release build, 10,000 owners or peers, 2,000,000 calls spread over the owners, a 20-core host. The tests are `authorize_cost` and `lookup_cost`, run with `--ignored --release --test-threads=1`. The "one `RwLock`" column is the same two tests run on the code before the sharded copy existed.

| Read | 1 thread | 8 threads | 8 threads, one `RwLock` |
|---|---|---|---|
| `authorize` | 37 ns per call | 153 ns per call, 52 M calls/s in total | 847 ns per call, 9.4 M calls/s in total |
| `discover_by_instance_id` | 58 ns per call | 207 ns per call, 39 M calls/s in total | 995 ns per call, 8.0 M calls/s in total |

`get` and `list` copy each record with its payloads. With a 2 KiB payload, `get` took 248 ns, and `list` took 14 to 19 ms for 10,000 entries. The heartbeat uses `peers()`, which does not copy payloads: 0.3 ms for 10,000 entries.

A write holds the store's write lock and a shard of the copy, so its cost must not grow with the number of entries. With 10,000 entries (`write_cost`), a put takes 1.0 µs and a remove 0.6 µs, and a reaper pass that expires all 10,000 takes 7.2 ms. An index update that scanned every entry was measured first and rejected: 21 µs for each put and each remove, and 109 ms for that reaper pass, all of it with the write lock held.

## Versioning

`velo-hub` takes the workspace version and depends on `velo` with a caret on that version. See [Versioning](../development/versioning.md#velo-hub).
