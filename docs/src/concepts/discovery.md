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

`ServiceDiscovery` is a second trait. It maps a service name to the instances that provide it.

| Backend | Feature | Implements | Use |
|---|---|---|---|
| `FilesystemPeerDiscovery` | always | `PeerDiscovery` | Development, tests, one host |
| `FilesystemServiceDiscovery` | always | `ServiceDiscovery` | Development, tests, one host |
| `NatsPeerDiscovery` | `nats-discovery` | `PeerDiscovery` | More than one host, with NATS |
| `EtcdServiceDiscovery` | `etcd` | `ServiceDiscovery` | More than one host, with etcd |

A registration returns a guard. When the guard drops, the backend removes the registration.

Without a discovery backend, register each peer by hand:

```rust,ignore
node_a.register_peer(node_b.peer_info())?;
```

The etcd backend uses leases. If a test process stops and does not revoke its lease, its keys stay until the lease expires.
