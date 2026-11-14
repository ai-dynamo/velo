# Services and stream transports

Velo 0.19 groups distributed events, named work queues, and built-in discovery
backends under one default feature, `services`. The default build keeps these
APIs. The stream transports are a separate, runtime choice.

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

## Select the stream transports at runtime

A builder with no stream config installs only the messenger mux. It starts no TCP or gRPC stream listener and advertises no stream endpoint. Ordinary SPSC attach, prebound tickets, MPSC attach, and large-payload rendezvous remain available. All remote streams use the messenger transport.

To serve peers that do not offer the mux, opt in to the per-stream path:

```rust,ignore
let node = velo::Velo::builder()
    .add_transport(transport)
    .stream_bind_addr(bind_ip) // or .stream_config(StreamConfig::Tcp(None))?
    .build()
    .await?;
```

This node installs a per-stream listener beside the mux. Each attach uses the mux when the peer offers it, and the per-stream transport when it does not.

`mux_only()` selects the mux alone explicitly. Do not combine it with `stream_config()` or `stream_bind_addr()`. `build()` returns an error for that combination, a disabled `MuxConfig`, or an active `VELO_MESSENGER_MUX_DISABLE` switch. It checks these choices before it starts transports.

A node without a per-stream listener refuses an attach from a peer that does not offer the mux. The error names `stream_config` and `stream_bind_addr`. There is no silent fallback.

The compile-time services choice and the runtime stream choice are independent.
A node can keep all services and use the mux alone, or omit services and opt in
to a per-stream listener. Use `Velo::shutdown(policy)` to close either kind
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

## 0.23.0: the per-stream listener is opt-in

Before 0.23.0, `Velo::builder().build()` started a TCP stream listener beside the mux. From 0.23.0, it starts no stream listener, and the mux carries every remote stream.

Two other 0.23.0 changes have their own guides: [Move off the Messenger front door](migrate-messenger-front-door.md) and [Move to the public paths of 0.23.0](migrate-public-paths.md). 0.23.0 also moves velo's own tasks to a runtime it owns; see [0.23.0: velo's tasks run on a runtime it owns](#0230-velos-tasks-run-on-a-runtime-it-owns).

These nodes need no change:

- A node that calls `stream_config` or `stream_bind_addr`. It keeps its per-stream listener. rhino and deepseek-ultrafast do this and keep working.
- A node that calls `mux_only()`. dynamo and dynamo-gdl do this.
- A default node whose peers all offer the mux. The mux is on by default since 0.16, so its streams already ride the mux.

Add `stream_config` or `stream_bind_addr` to a default node in these cases:

- A peer turns the mux off, or runs a velo without the mux, and attaches to this node. Without a listener, the attach fails with `peer does not support the messenger mux, and this instance has no per-stream transport to serve it on`. The message names `stream_config` and `stream_bind_addr`.
- This node attaches to a peer that has the mux off. The peer answers with `no common streaming transport`. That error comes from the peer, so it does not name the fix.
- This node must be able to turn the mux off. With the mux off by `MuxConfig { enabled: false, .. }` or `VELO_MESSENGER_MUX_DISABLE=1`, and no stream config, `build()` fails. The error names `stream_config` and `stream_bind_addr`.

So the kill switch is a rollback only for a node that opted in to the per-stream path. On a default node, a rollback is a code change: add `stream_bind_addr`, then set the switch and restart.

## 0.23.0: velo's tasks run on a runtime it owns

Before 0.23.0, velo ran its own tasks (the transports, the inbound loop, the messenger mux and its batch lanes) on the runtime that called `build`. From 0.23.0, `VeloBuilder::task_runtime` defaults to `TaskRuntime::Dedicated { threads: 1 }`: velo builds a one-thread runtime named `velo-rt` for these tasks and stops it in `Velo::shutdown` or on the last drop. Handlers still run on the runtime that called `build`. The code does not change; the behaviour does.

What it buys: an application task that holds its worker thread right after it calls velo (a blocking call, a long encode) no longer stalls velo's tasks. See [Known limitations](../appendix/limitations.md#a-task-that-holds-its-worker-thread-stalls-velos-tasks).

What it costs: each wake between the application and velo now crosses threads. In velo's own benchmark on one exclusive 72-core node, a single caller making one unary call at a time took about 3.5 to 4 times as long per call. Under concurrency the dedicated runtime did better, and in dynamo's frontend load test the frontend used 6% to 25% less CPU per output token. `TaskRuntime`'s documentation has the setup and the other numbers.

To keep the old behaviour, opt out:

```rust
let velo = Velo::builder()
    .task_runtime(TaskRuntime::Application)
    // ...
    .build()
    .await?;
```

An instance that opts out must keep blocking work off the tasks that call velo (`spawn_blocking` or `block_in_place`).

Measure your own workload if its latency rests on velo unary calls made one at a time. dynamo-gdl and kvbm make such calls: run their latency benchmarks on both settings, and opt out if `Application` is faster for them.

Also check:

- Tests that pause tokio's clock (`start_paused`, `tokio::time::pause`) control only their own runtime's clock, not velo's. A test that pauses time while velo runs a timer of its own may need `TaskRuntime::Application`.
- Code that counts the process's threads sees one more, `velo-rt`, per instance, and a short-lived `velo-rt-stop` thread when an instance stops.
