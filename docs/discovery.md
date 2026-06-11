# discovery

Peer and service discovery abstraction with filesystem, NATS, and etcd backends.

## Traits

### PeerDiscovery

Lookup interface over cluster membership. Resolve a `PeerInfo` (which includes the peer's `WorkerAddress`) from either a `WorkerId` or `InstanceId`.

```rust
pub trait PeerDiscovery: Send + Sync {
    fn discover_by_worker_id(&self, worker_id: WorkerId) -> BoxFuture<'_, Result<PeerInfo>>;
    fn discover_by_instance_id(&self, instance_id: InstanceId) -> BoxFuture<'_, Result<PeerInfo>>;
}
```

### ServiceDiscovery

Higher-level service registry abstraction for named service groups (e.g., "prefill", "decode"). Backends emit `ServiceEvent` notifications (`Initial`, `Added`, `Removed`, `Disconnected`).

```rust
pub trait ServiceDiscovery: Send + Sync {
    fn list_services(&self) -> BoxFuture<'_, Result<Vec<String>>>;
    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, Result<Vec<InstanceId>>>;
    fn watch_instances(
        &self,
        service_name: &str,
    ) -> BoxFuture<'_, Result<Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>>>;
}
```

`watch_instances` emits a `ServiceEvent::Initial` snapshot followed by `Added`/`Removed` deltas. When the backend disconnects, it emits `ServiceEvent::Disconnected` as the last event. Callers should re-establish the watch on disconnect.

Both traits are defined in `velo_ext::discovery` and re-exported from `velo::discovery`.

## Backends

| Backend | Type | Feature | Notes |
|---------|------|---------|-------|
| `FilesystemPeerDiscovery` | `PeerDiscovery` | always | JSON file on disk, dev/testing only |
| `FilesystemServiceDiscovery` | `ServiceDiscovery` | always | JSON file, single-host only |
| `nats::NatsPeerDiscovery` | `PeerDiscovery` | `nats-discovery` | NATS key-value store |
| `etcd::EtcdServiceDiscovery` | `ServiceDiscovery` | `etcd` | etcd v3 KV with watch |

There is no NATS `ServiceDiscovery` or etcd `PeerDiscovery` backend today — the
matrix is deliberately sparse; pair backends as needed.

## FilesystemPeerDiscovery

Stores peer records in a JSON file. Cross-process safe via `fs4` file locking (shared for reads, exclusive for writes) with atomic rename on write. Within-process uses `RwLock` for the in-memory cache. **Not suitable for multi-host deployments.**

```rust
use velo::discovery::FilesystemPeerDiscovery;

// Persistent file
let discovery = FilesystemPeerDiscovery::new("/tmp/peers.json")?;

// Throwaway (temp file, auto-deleted on drop)
let discovery = FilesystemPeerDiscovery::new_temp()?;

// Manual peer management
discovery.register_peer_info(&peer_info)?;
discovery.unregister_instance(instance_id)?;
```

File format:
```json
{
  "peers": [
    {
      "instance_id": "uuid-string",
      "worker_id": 123,
      "worker_address": "<msgpack bytes>",
      "address_checksum": 12345678
    }
  ]
}
```

## Registration Guards

`register_peer_info` returns a `FilesystemRegistrationGuard` (for filesystem backend) that auto-unregisters on drop. NATS and etcd backends return equivalent guards that stop heartbeating/renewing on drop.

## Wiring to Messenger

Pass a discovery backend to the `MessengerBuilder` or `VeloBuilder`:

```rust
use std::sync::Arc;
use velo::{Velo, discovery::FilesystemPeerDiscovery};

let discovery = Arc::new(FilesystemPeerDiscovery::new_temp()?);
let velo = Velo::builder()
    .add_transport(transport)
    .discovery(discovery)
    .build()
    .await?;
```

When no backend is configured, peers must be registered manually via `velo.register_peer(peer_info)`.

## Peer Lookup via Velo

Use `Velo::discover_and_register_peer` rather than `Messenger::discover_and_register_peer` — the `Velo` wrapper fans out to the streaming transport's `register()` as well, so attach operations don't fail with "peer not registered".

```rust
velo.discover_and_register_peer(instance_id).await?;
```
