# The hub

The `velo-hub` crate builds a hub: one service that is the peer discovery for a velo application and also hosts extensions. A program builds the hub in `main`, adds extensions or bundles of extensions, picks a store, and serves it. `velo` does not depend on `velo-hub`. An application that does not use a hub picks any velo discovery backend, as before.

The crate has three cargo features. `server` builds the hub. `client` builds the hub client, `velo_hub::client`, without the server. `test-support` adds the store conformance suite and `FaultStore`, a store that fails on request. The `protocol` module (paths, request and response types, ids) builds under either feature, and with no feature.

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

- Each write of an instance gets a new incarnation, greater than every earlier one of that instance. The hub and the extensions compare incarnations to tell a stale removal from a current one.
- Writes, removals, and touches are conditional on the current incarnation. Work that runs later carries the incarnation that it saw, so it cannot change a later registration of the same instance.
- The removal hook and its snapshot share one point in time. Each entry present at that point is in the snapshot, and the hook hears each later removal once.
- The store reports its continuity. When the store is not `Live`, the hub refuses every credential with 503 and refuses new registrations. Peer lookups continue from the store's local view. A `Fenced` store (another hub holds it) stops the hub.
- Every removal that a call causes or races reaches the removal hook before the call returns. The hub reads the hook's record right after a write, a read-back, or a remove.
- A write can answer `Indeterminate`: a remote write can apply even when its answer is lost. The hub then asks the store which write produced the current record, by the write's op id.

The record holds the peer, the epoch, the extension payloads, a pinned flag for the hub's own entry, and SHA-256 digests of the credentials. The store never holds a credential, so a copy of the store does not give authority.

Each backend runs the conformance suite in `velo_hub::store::conformance` (feature `test-support`). `FaultStore` in `velo_hub::store::testing` is an in-memory store that fails, stops at a gate, or answers `Indeterminate` on request. It passes the same suite when no fault is set.

## The etcd store

`EtcdStore` (feature `etcd`) keeps the registrations in etcd, under a prefix (`/velo-hub/default` by default), so they survive a hub restart.

- An entry's incarnation is the etcd revision of its key. etcd revisions only grow, so incarnations do too.
- Every write is one etcd transaction, conditional on the incarnation. When a write's answer is lost, the hub's read-back first writes a tombstone for the write's op id, and every write requires that tombstone to be absent. So a read-back that finds nothing is final: the write will not apply later.
- The store watches its prefix and keeps a local copy, which serves the sync reads and the peer lookups. A call waits until the watch has applied its own transaction and run the removal hook for it, so the hook rule above holds.
- One hub per prefix. The store takes a lock key on a short lease (10 s) when it connects, and every transaction checks that the lock is still its own. If the lock is lost, for example after a partition, the store is `Fenced` and the hub stops. A clean stop frees the lock. After a crash, the next hub waits for the lock with `wait_for_prefix`.
- Liveness does not use a lease per entry. A `touch` updates the local copy, and the store's reaper removes an entry that nobody touched for the registrant TTL (30 s). Every key of a hub is on one custody lease (5 min), kept alive by the store. So the entries of a hub that crashed stay for the custody TTL, which is how long a restarted hub can recover them. A lease per entry was rejected: it needs one etcd keep-alive stream per touch (about 1,000 a second for 10,000 registrants at the default interval), and a healthy registrant could expire after one late probe.
- A watch that falls behind etcd's compaction loses history. The store is then `Lost`, reloads the prefix, reports every entry that went away in the gap as a removal, and is `Live` again. The hub needs nothing more: the store is fenced to it, so no other writer added an entry in the gap.

Measured against a local etcd 3.5 in Docker (release build, one call at a time): a put takes 2.8 ms and a remove 2.5 ms, against 2.2 ms for a plain etcd put; a touch takes 0.17 µs, because it does not reach etcd; a reaper pass that expires 10,000 entries takes 165 to 187 ms.

## Recovery after a restart

A hub that starts on a store with registrations in it reserves each of them: nobody can claim the instance without a credential, and peer lookups keep working. It also tells its velo instance about each peer, so its probes reach them.

The registrant gets back in with its credential. The hub's heartbeat carries the hub's id, and the client notices that the id changed (`HubClient::recovered()`); a route the registrant calls with its credential answers `recovered`. The client then registers again presenting its credential. The hub checks the credential's digest against the stored record (the credential the registrant presented, or the one that registration minted, whose answer may have been lost), and the registration proceeds as a re-registration of the stored one, with a new credential and a new epoch. Extension state is not replayed: extensions see the owner as new. The registration carries the client's services, so they come back with it. A recovery that fails before its write (an extension refuses it) leaves the instance reserved, so the credential can try again. The credential the registrant presented stays able to recover until the registrant registers once more, because the record keeps it for the case where the answer to that registration was lost.

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

Each store call that the hub awaits (under the lock, in a request such as the HTTP heartbeat, or in the heartbeat task), and each `on_registered`, runs under `step_timeout_ms`. A store or an extension that never answers cannot hold the lock. A remove that times out counts as indeterminate: the hub reads the store to learn whether it applied.

A failure before the store write leaves the prior registration exactly as it was. A failure after the write removes the registration, including the prior one, and the client registers again. The alternative is to write the prior registration back. kvbm-hub did that, but the restore is a second write that can fail too, and it gives the registration a new incarnation.

If an extension panics, the hub settles the owner. Before the write, it aborts. After the write, it refuses the credential and queues the removal of the entry.

When the hub runs velo, it decodes the peer's address before it takes the lock and answers 400 if the address is not valid. Without velo, the hub stores the address and does not read it.

## Removal

The store calls the hub's removal hook when an entry leaves: an unregistration, a TTL expiry, or a failed heartbeat. The hook revokes the owner's authority at once and puts the removal on a queue. It never waits for the registration lock. A lifecycle task takes the lock and delivers the removal: `on_removed` on each extension in reverse start order, then it forgets the velo peer, then the owner can register again. Until then, the owner's requests answer `busy`.

The lifecycle task forgets the velo peer only if no later incarnation replaced it. So a late delivery does not disconnect a replacement.

`DELETE /v1/instances/{id}` takes the lock and delivers the removal itself. When it answers, each extension has dropped the owner. The hook also queued a delivery, and that second delivery changes nothing. Like a registration, the unregistration runs as a task of the hub, so a client that disconnects cannot leave the owner `busy`.

## Heartbeat

When the hub runs velo, it sends a heartbeat to each registration every `heartbeat_interval_ms` (10 s by default). The registrant answers on the `velo_hub_heartbeat` handler. Each probe times out after one interval. After `heartbeat_max_failures` (3 by default) failures in a row, the hub removes that incarnation. A success touches that incarnation. An answer with `ok: false` counts as a failure: a registrant answers that while it holds no registration with this hub, for example after the response to its registration was lost. The hub committed that registration, but nobody holds its credential, so the registrant could never register again while its probes kept the entry alive. The hub does not probe an instance while its registration transaction runs: the registrant has no answer yet, so it cannot tell its new registration from one it never got, and a slow but conforming `on_registered` would let the probes remove the entry before the answer arrives. Probes run in parallel, so a stopped peer does not delay the others. The hub does not probe its own pinned entry. Without velo, liveness is the store's TTL.

The heartbeat task holds a `WeakVelo`. Extension handlers and tasks that use the hub's `Velo` must also hold a `WeakVelo`, and upgrade it on each call. A handler that holds a `Velo` is in that instance's own handler registry, so the instance holds itself and is never dropped.

## Client

A program that uses a hub builds a `HubClient` (feature `client`), builds its `Velo` from the client, and registers:

```rust,ignore
use velo_hub::client::HubClient;

let hub = HubClient::builder().host("hub.example").build()?;
let velo = hub.velo_builder().add_transport(tcp).build().await?;
hub.register(&velo, Default::default()).await?;
// velo now finds every other registered peer through the hub.
hub.unregister().await?;
```

`velo_builder()` sets the hub as the velo discovery. velo resolves a worker target through it, and `Velo::discover_and_register_peer` resolves an instance id through it.

`register(&velo, extensions)` installs the heartbeat handler on `velo` first, so the hub's probes get an answer as soon as the hub answers the registration. `register_peer(peer, extensions)` registers a `PeerInfo` without a handler, for a hub without velo. A client holds one registration. A second `register` of the same peer presents the credential, and the hub replaces the registration: a new credential, a new epoch, and the old credential stops working. If the hub answers `unauthorized` or `register_again`, the client drops the registration it holds, and the next `register` starts again without a credential.

The client's heartbeat handler answers `ok: true` only while the client holds a registration or is getting one. So when the response to a registration is lost, the hub removes the registration that nobody can use after `heartbeat_max_failures` probes, and the client can register again. A response is lost when the network drops it, and also when the caller drops the `register` future (a timeout around it): the hub runs the registration as its own task and commits it anyway. A guard clears "getting one" on every exit from `register`, also when the future is dropped, so the handler never answers `ok` for a registration the client does not hold. A re-registration that gets no answer from the hub fails closed. No answer means anything but the hub's own response or a request that never left: its future is dropped, its request times out or breaks after it was sent, or a proxy in front of the hub answers 502 or 504. Then: the hub may have committed it and rotated the credential, so the client drops the registration it held, and the hub frees whatever it holds after `heartbeat_max_failures` probes. A `register` without a credential that the hub refuses with `unauthorized` means the hub holds a registration of this instance that the client never got. Until a `register` succeeds, the handler then answers not `ok` even during a call, so a caller that retries in a loop cannot keep that registration alive. The retry gets in after about `heartbeat_interval_ms` times `heartbeat_max_failures` (30 s by default).

An answer that says the hub does not accept the credential (`unauthorized`, `register_again`, on any call, also an extension call) drops the registration the client holds, and so does `not_found` for a heartbeat or an unregister. So `is_registered()` tells the truth again, and the next `register` starts without a credential.

The client is a cheap handle; its clones share the one registration. Dropping the last clone sends `DELETE` in the background on the current runtime, as a best effort: without a runtime, or when the runtime shuts down first, the request is not sent, and the hub's probes or the TTL remove the entry. A program that can await calls `unregister`. If `DELETE` fails, the client keeps the registration, so a retry or the drop sends it again.

velo keeps its discovery for as long as it lives, so the discovery that `velo_builder()` installs is a separate handle, `HubDiscovery`, that holds no registration. If velo held the client itself, the registration would live as long as velo does, and dropping the client would not unregister. The heartbeat handler and `ExtClient` hold the registration weakly for the same reason.

`client.ext(name)` makes JSON calls under `/v1/ext/{name}`. `get` and `post` use the public listener. `control_get`, `control_post`, and `control_delete` use the control listener and send the registration's credential; once every `HubClient` clone is dropped, they answer `NotRegistered`. A bundle crate builds its typed client on it. Every error that the hub answers is a `ClientError::Hub` with the hub's `ErrorCode`, so a caller acts on the code.

The default HTTP client has a 5 s connect timeout and a 60 s request timeout. A registration on a hub with default settings and up to two extensions of its own takes at most 50 s: 5 s to wait for the lock, and 5 s for each step under it (the built-in service directory counts as an extension). A caller that passes its own `reqwest::Client` sets its own timeouts.

## Services

A registered instance can serve named services. Another program asks the hub which instances serve a name, or watches the name. A service name is 1 to 128 of `A-Z a-z 0-9 . _ -`, and not `.` or `..`, so it is always one path segment and a URL resolver cannot turn it into another route.

| Route | Listener | Use |
|---|---|---|
| `GET /v1/services` | both | names that have at least one instance |
| `GET /v1/services/{name}` | both | the instances of one name |
| `GET /v1/services/{name}/watch` | both | server-sent events: `initial`, then `added` and `removed` |
| `PUT`, `DELETE /v1/services/{name}/instances/{id}` | control | add or remove an instance, with that instance's credential |

The services are a built-in extension, named `services`, first in the start order. So a service entry follows the same transaction and removal rules as an extension's state: it goes when its registration goes (unregister, expiry, or heartbeat eviction), and there is no second cleanup path that could leave an orphan. A registration may carry the names the instance serves, as the `services` payload: the hub then sets the instance's services to exactly those names. Without the payload, a re-registration with the credential keeps the instance's services, because the peer still serves them, and a registration without a credential starts with none. A `PUT` or `DELETE` applies only if its lease's epoch is the one the directory committed, under the directory's own lock. The hub does not list the built-in in `GET /v1/hub`, and no other extension can take its name.

A watch takes its snapshot and subscribes in one step, under the directory's lock, so a watcher never misses a change or sees one twice. A watcher that falls more than 128 changes behind gets `disconnected`, and its stream ends: its set is stale, so it watches again. At shutdown the directory's `shutdown` hook ends every watch stream, before the hub drains its listeners, so an open watch does not hold the drain. The hub sends a keep-alive comment every 10 s. A watch of a name that nobody serves costs the hub a small entry and a 128-slot event ring until the watch ends.

The client implements velo's `ServiceDiscovery` with `client.services()`. `client.register_service(name)` returns a guard; the client holds one guard per name and refuses a second. The client keeps the names it serves, and every registration carries them, so after any registration (fresh, re-registered, or on a hub that restarted) the hub's set is the client's, with no request per name. Between registrations, guards change only the client's set, and one writer, `sync_services`, sends the changes one request at a time while no registration runs. So two requests from one client never race. A request the hub refused is answered; one with no answer from the hub (a dropped future, a timeout, a proxy's 504) leaves its name unsettled, and the next pass sends it again as the client's set says. So a guard never removes another guard's entry, and the hub does not keep an entry nobody serves. `register` answers `Ok` once the registration is held. A failure to send a change made during the call is left for `sync_services`, which the caller can also run to retry. Dropping a guard stops serving the name, in the background.

Services are kept in the hub's memory, like extension state. A hub that restarts with a persistent store restores peer discovery only; the client's next registration carries its services back.

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

`Hub::shutdown` stops new registrations, waits for the registration that holds the lock, stops extension background work, delivers the queued removals, shuts each extension down in reverse order, and stops the heartbeat and the reaper. Then it releases the lock, so a request that waits for it answers 503 at once. It closes both listeners and waits up to `shutdown_timeout_ms` for the requests in flight. Then each request still in flight answers 503 and its handler is dropped, and the hub waits up to `shutdown_timeout_ms` again for the connections to close. So the listener drain can take two times `shutdown_timeout_ms`. The wait for the lifecycle task, each extension's `shutdown`, each hub task, and the velo drain also get `shutdown_timeout_ms` each. The wait for the lock gets at least as long as a registration can hold it: a `prepare` and an `on_registered` for each extension, and a write, a read-back, and a remove, each `step_timeout_ms`. So only a sync hook that blocks its thread, against the extension rules, can make the stop go on without the lock. Then it delivers no queued removal, because without the lock `on_removed` could run while that hook still runs. Last, it removes the pinned entry and shuts the hub's velo instance down. When the drain finishes within its budgets, the listener ports are free when it returns. If a connection outlives both budgets, the hub aborts its server task, and the port is freed when that abort takes effect.

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
