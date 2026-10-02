# Services and mux-only mode

Velo 0.19 groups distributed events, named work queues, and built-in discovery
backends under one default feature, `services`. The default build keeps these
APIs and the current stream listener behavior.

## Select services at build time

For an application that registers peers itself:

```toml
[dependencies]
velo = { version = "0.19", default-features = false }
```

This build retains TCP and Unix sockets, typed handlers, ACK/NACK messages,
SPSC and MPSC streams, per-stream TCP, the messenger mux, rendezvous, metrics,
and discovery traits. Custom discovery implementations still work. Add `ucx`
for UCX messaging and RDMA registration; those APIs do not need `services`.

The following APIs need `services`:

- `velo::events`, its root re-exports, `VeloEvents`, and event methods on `Velo`
  and `Messenger`.
- `velo::queue`, including the in-memory queue backend.
- Filesystem peer and service discovery.

`nats-discovery`, `etcd`, `nats-queue`, and `queue-messenger` enable `services`
automatically. They still select their own backends. `nats-transport` does not
enable `services`. This is a deliberate coarse boundary: selecting a discovery or queue backend also includes the other service APIs and their shared dependencies. It keeps the supported feature combinations small. Simulation remains independent of this feature.

If an existing dependency disables defaults and uses these APIs, add the feature:

```toml
velo = { version = "0.19", default-features = false, features = ["services"] }
```

Cargo combines features requested by all dependents. Another crate that enables
Velo's default features also enables services for the shared Velo package.
Without services, the messenger does not create an event manager or register the
`_event_*` handlers. The `fs4` and `lru` dependencies are omitted. Core request
acknowledgements and errors keep their current wire format.

## Select mux-only streams at runtime

The default builder creates a TCP stream listener beside the messenger mux.
That listener allows streams from peers that do not offer the mux. If every peer
supports the mux, use:

```rust,ignore
let node = velo::Velo::builder()
    .add_transport(transport)
    .mux_only()
    .build()
    .await?;
```

This starts no extra TCP or gRPC stream listener and advertises no stream
endpoint. Ordinary SPSC attach, prebound tickets, MPSC attach, and large-payload
rendezvous remain available. All remote streams use the messenger transport.

Do not combine `mux_only()` with `stream_config()` or `stream_bind_addr()`.
`build()` returns an error for that combination, a disabled `MuxConfig`, or an
active `VELO_MESSENGER_MUX_DISABLE` switch. It validates these choices before
starting transports. Peers that need a per-stream transport receive an attach
error. There is no silent fallback. To restore that fallback, remove `mux_only()`
and restart the instance. The fleet-wide mux kill switch is therefore not a fallback for mux-only nodes.

The compile-time services choice and the runtime stream choice are independent.
A node can keep all services and use mux-only streams, or omit services and keep
the default stream listener. Use `Velo::shutdown(policy)` to close either kind
of instance. See [Shutdown and drain](../concepts/shutdown.md).

If a peer accepts an attach but the producer cannot connect, the producer waits
up to one second for rollback before it returns the original connect error.
SPSC rollback checks the producer identity so a late cleanup cannot detach a
replacement. MPSC uses its sender ID. Rollback does not emit a `Detached` frame.

Older peers may not support the SPSC rollback handler. Cleanup failure is logged;
there is no unqualified detach fallback. Existing receiver watchdogs and accept
timeouts remain the backstop. A legacy transport's pending bind can remain until
its accept timeout even after anchor rollback succeeds. Dropping the attach
future, or losing the attach reply, is outside this cleanup guarantee.
