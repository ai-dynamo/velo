# Discovery

Discovery finds the addresses of peers. A backend implements the `PeerDiscovery` trait. It resolves an `InstanceId` or a `WorkerId` to a `PeerInfo`, which holds the transport addresses of the peer.

```rust,ignore
use velo::discovery::FilesystemPeerDiscovery;

let discovery = Arc::new(FilesystemPeerDiscovery::new("/tmp/peers.json")?);
let node = Velo::builder()
    .add_transport(tcp)
    .discovery(discovery)
    .build()
    .await?;

// Find a peer and register it.
node.discover_and_register_peer(peer_instance_id).await?;
```

The discovery traits and custom implementations are available without `services`. Built-in backends require it; `nats-discovery` and `etcd` enable it automatically.

`ServiceDiscovery` is a second trait. It maps a service name to the instances that provide it.

| Backend | Feature | Implements | Use |
|---|---|---|---|
| `FilesystemPeerDiscovery` | `services` | `PeerDiscovery` | Development, tests, one host |
| `FilesystemServiceDiscovery` | `services` | `ServiceDiscovery` | Development, tests, one host |
| `NatsPeerDiscovery` | `nats-discovery` | `PeerDiscovery` | More than one host, with NATS |
| `EtcdServiceDiscovery` | `etcd` | `ServiceDiscovery` | More than one host, with etcd |

A registration returns a guard. When the guard drops, the backend removes the registration.

A hub from the `velo-hub` crate is a discovery service for a whole application, with registration, liveness, and hosted extensions. A program uses it as its peer discovery through `HubClient::velo_builder()`, and as its service discovery through `HubClient::services()`. See [The hub](hub.md).

Without a discovery backend, register each peer by hand:

```rust,ignore
node_a.register_peer(node_b.peer_info())?;
```

A node can learn a peer by `register_peer`, by discovery when a send names the peer's worker, or by the `_hello` the peer sends with its first message, and each way registers the peer with the stream transport too, so the node can attach a stream to it.

## Forget a peer

`Velo::unregister_peer(instance_id)` undoes `register_peer`. It drops the peer from every transport, closes the connections to it, and forgets its handler list. A send to the peer then fails as a send to a peer never registered does. A later `register_peer`, with the same id or a new one, starts fresh. Other peers are not affected. The call is synchronous, cheap, and idempotent.

```rust,ignore
node_a.unregister_peer(node_b.instance_id());
```

A restarted peer has a new instance id. So a long-lived node whose peers restart, such as a worker that serves many frontends, keeps the state of every peer it ever saw unless it calls this when a peer leaves.

A forgotten peer that still talks to the node comes back in two ways. A `_hello` handshake from the peer registers it again. And with a discovery backend, a reply to the peer resolves it again: the node asks `PeerDiscovery::discover_by_worker_id`, registers the answer, and sends. A send that names the peer's instance does the same through `discover_by_instance_id`. Each miss runs its own lookup, a node runs at most 64 at once, and a reply's is bounded by `VeloBuilder::reply_lookup_timeout`. Without a discovery backend a forgotten peer stays forgotten until a `register_peer` or its `_hello`.

Unregister only an idle peer. A send, stream, or handshake in flight to it may fail, and one that finishes after the call can register the peer again. A `_hello` handshake from the peer registers it again too, and so can a reply, as above. The messenger mux keeps small per-peer receive tables, and this call does not drop them.

A node that forgets a peer it is still serving spends a discovery lookup to reach it again, and a burst of replies that all miss spends one each, up to the cap. A node that forgets a peer for good should publish no discovery record for it, so a reply that misses has nothing to resolve.

The etcd backend uses leases. If a test process stops and does not revoke its lease, its keys stay until the lease expires.
