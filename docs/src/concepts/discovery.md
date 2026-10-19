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

A hub from the `velo-hub` crate is a discovery service for a whole application, with registration, liveness, and hosted extensions. A program uses it as its discovery through `HubClient::velo_builder()`. See [The hub](hub.md).

Without a discovery backend, register each peer by hand:

```rust,ignore
node_a.register_peer(node_b.peer_info())?;
```

## Forget a peer

`Velo::unregister_peer(instance_id)` undoes `register_peer`. It drops the peer from every transport, closes the connections to it, and forgets its handler list. A send to the peer then fails as a send to a peer never registered does. A later `register_peer`, with the same id or a new one, starts fresh. Other peers are not affected. The call is synchronous, cheap, and idempotent.

```rust,ignore
node_a.unregister_peer(node_b.instance_id());
```

A restarted peer has a new instance id. So a long-lived node whose peers restart, such as a worker that serves many frontends, keeps the state of every peer it ever saw unless it calls this when a peer leaves.

Unregister only an idle peer. A send, stream, or handshake in flight to it may fail, and one that finishes after the call can register the peer again. A `_hello` handshake from the peer registers it again too. The messenger mux keeps small per-peer receive tables, and this call does not drop them.

The etcd backend uses leases. If a test process stops and does not revoke its lease, its keys stay until the lease expires.
