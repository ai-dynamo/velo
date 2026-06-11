# rendezvous

Receiver-driven large-payload transfer for Velo. Stages bytes on the owner worker, returns a compact `DataHandle`, and lets a consumer pull the data later.

## Protocol

The consumer drives all data transfer:

1. Owner: `register_data(bytes)` → `DataHandle`
2. Pass handle to consumer via any channel (AM, event, message field)
3. Consumer: `metadata(handle)` — query size/info without locking
4. Consumer: `get(handle)` → `(Bytes, lease_id)` — acquire read lock and pull data
5. Consumer: `detach(handle, lease_id)` — release read lock, handle stays alive (can `get` again)
   **or** `release(handle, lease_id)` — release read lock and decrement refcount; freed when refcount hits 0

## Main Types

| Type | Role |
|------|------|
| `RendezvousManager` | Owner- and consumer-side entry point |
| `DataHandle` | Compact wire handle: upper 64 bits = `WorkerId`, lower 64 bits = local slot ID |
| `DataMetadata` | Size, refcount, pinned-state metadata |
| `RegisterOptions` | Options for staged slots: refcount, TTL |
| `StageMode` | `InMemory` (current) or `Pinned` (NIXL RDMA placeholder) |
| `RendezvousWrite` | Destination trait for `get_into` (write into a pre-allocated buffer) |
| `RendezvousStager` / `RendezvousResolver` | Transparent large-payload bridge for `Messenger` |

## Usage via Velo

```rust
// Stage data
let handle: DataHandle = velo.register_data(data);

// With options (TTL, refcount)
let handle = velo.register_data_with(data, RegisterOptions { refcount: 2, ..Default::default() });

// Consumer pulls
let meta = velo.metadata(handle).await?;
let (data, lease_id) = velo.get(handle).await?;
velo.release(handle, lease_id).await?;

// Multiple consumers: increment refcount first
velo.ref_handle(handle).await?;
```

## Using RendezvousManager Directly

```rust
use velo::rendezvous::RendezvousManager;

let mgr = RendezvousManager::new(worker_id);
mgr.register_handlers(Arc::clone(&messenger))?;
```

## Transfer Mechanism

Phase 1 (current): data is chunked and transferred over active messages when the consumer is on a different worker. Chunk size is fixed; the consumer sends `_rv_get_chunk` AM requests, the owner responds with payload bytes.

Phase 2 (placeholder): `StageMode::Pinned` reserves the enum variant for future NIXL/RDMA direct memory access via `dynamo-memory` arena allocators. `StageMode::Pinned` exists in the API but the RDMA path is not yet wired end-to-end.

## Transparent Mode

When built via `VeloBuilder`, `RendezvousStager` and `RendezvousResolver` are automatically installed on the `Messenger`. Payloads above the staging threshold are replaced with a `_rv` header on send and resolved before handler dispatch on receive. Small payloads continue inline. The threshold is internal to the stager.

## Current Limits

- `RegisterOptions::ttl` is stored per slot but there is no background reaper — TTL is advisory only until a reaper is added.
- Read lock count and refcount are tracked separately: both must reach zero for data to be freed.
