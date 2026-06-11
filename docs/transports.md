# transports

Multi-transport active message routing. Abstracts TCP, NATS, gRPC, ZMQ, and UDS behind a unified `Transport` trait.

The `Transport` trait is defined in `velo_ext` and re-exported as `velo::Transport`. The internal orchestrator (`VeloBackend`) is not part of the public API — consumers interact through `Messenger` or `Velo`.

## Transport Summary

| Transport | Module | Feature | Protocol |
|-----------|--------|---------|----------|
| `TcpTransport` | `transports::tcp` | always | Raw TCP, 11-byte preamble + header + payload |
| `UdsTransport` | `transports::uds` | always (unix) | Unix domain sockets, same framing as TCP |
| `NatsTransport` | `transports::nats` | `nats-transport` | NATS pub-sub, base64 header in NATS HeaderMap |
| `GrpcTransport` | `transports::grpc` | `grpc` | HTTP/2 bidirectional streaming, Protobuf `FramedData` wrapper |
| `ZmqTransport` | `transports::zmq` | `zmq` | ZeroMQ PUSH/PULL, bundled libzmq via `zeromq-src` |
| `TipcTransport` | `transports::tipc` | `tipc` (Linux only) | TIPC `SOCK_STREAM`, same `TcpFrameCodec` framing; zero-config intra-node or bearer-based cluster |

## Wire Format (TCP / UDS)

```text
┌──────────────┬───────────┬──────────────┬───────────────┬────────┬─────────┐
│ version (2B) │ type (1B) │ hdr_len (4B) │ pay_len (4B)  │ header │ payload │
│   u16 BE     │   u8      │   u32 BE     │   u32 BE      │ bytes  │ bytes   │
└──────────────┴───────────┴──────────────┴───────────────┴────────┴─────────┘
```

- `version`: currently 1
- `type`: `Message(0)`, `Response(1)`, `Ack(2)`, `Event(3)`, `ShuttingDown(4)`
- Max frame size: 16 MB

gRPC wraps the same preamble + header + payload in a Protobuf `FramedData` message.

## Transport Trait

```rust
pub trait Transport: Send + Sync {
    fn key(&self) -> TransportKey;
    fn address(&self) -> WorkerAddress;
    fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError>;
    fn send_message(
        &self,
        instance_id: InstanceId,
        header: Bytes,
        payload: Bytes,
        message_type: MessageType,
        on_error: Arc<dyn TransportErrorHandler>,
    ) -> Result<(), SendBackpressure>;
    fn start(
        &self,
        instance_id: InstanceId,
        channels: TransportAdapter,
        rt: tokio::runtime::Handle,
    ) -> BoxFuture<'_, anyhow::Result<()>>;
    fn check_health(
        &self,
        instance_id: InstanceId,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), HealthCheckError>> + Send + '_>>;
    fn shutdown(&self);
    fn set_observability(&self, _: Arc<dyn TransportObservability>) {}  // default no-op
}
```

Sends return `Result<(), SendBackpressure>` — callers check for backpressure rather than receiving an error callback for that condition. Hard delivery failures are reported via `on_error`. Three inbound streams (message, response, event) flow via `TransportAdapter` flume channels.

## Shutdown Model

3-phase graceful shutdown:

1. **Gate** — flip the draining flag; transports reject new inbound `Message` frames (`ShuttingDown` response sent)
2. **Drain** — wait for all in-flight `InFlightGuard`s to drop
3. **Teardown** — cancel the teardown token, stop all listener/writer tasks, call `shutdown()` on each transport

`Response`, `Ack`, and `Event` frames continue flowing during drain so in-flight work can complete.

## WorkerAddress and Peer Registration

`WorkerAddress` is a MessagePack-encoded map of `TransportKey → endpoint bytes`. Each transport contributes its endpoint:

```text
tcp  → Vec<InterfaceEndpoint>   (multi-interface advertise list)
grpc → SocketAddr
nats → subject prefix bytes
tipc → TipcEndpoint (rmp_serde::to_vec_named, key "tipc")
```

`TipcEndpoint` carries the exact socket address (`socket_ref`, `node`), service binding
(`service_type`, `service_instance`), cluster identity (`netid`), 128-bit node identity
(`node_id`), and a per-netns nonce (`netns_nonce`).  The transport's `register()` gate uses
these fields to determine reachability before caching the endpoint.  All post-v1 fields carry
`#[serde(default)]` so older decoders tolerate new fields; `to_vec_named` (not positional
`to_vec`) is mandatory for forward/backward compatibility.

When a peer registers via `WorkerAddress`, each transport extracts its own entry by `TransportKey` and caches the resolved endpoint. TCP uses NUMA + subnet scoring to select the best advertised interface endpoint at `register()` time.

## TCP Transport

```rust
use velo::transports::tcp::TcpTransportBuilder;

let listener = std::net::TcpListener::bind("0.0.0.0:0")?;
let transport = Arc::new(
    TcpTransportBuilder::new()
        .from_listener(listener)?
        .build()?
);
```

Properties:
- Zero-copy codec; DashMap connection pool; keepalive enabled
- Multi-interface advertise via `InterfaceEndpoint` list in `WorkerAddress`
- NUMA-aware endpoint selection: prefers interfaces in the same NUMA node, then subnet match, then first reachable

## NATS Transport

```rust
use velo::transports::nats::NatsTransport;

let client = async_nats::connect("nats://localhost:4222").await?;
let transport = Arc::new(NatsTransport::new(client, instance_id));
```

Subject scheme: `velo.{base58_instance_id}.{message_type}`. Drain unsubscribes from the message subject so new inbound messages stop while responses continue.

## gRPC Transport

Bidirectional streaming over HTTP/2. Tonic channels with exponential backoff reconnect. Subject scheme mirrors the message type lanes.

## Adding an Out-of-Tree Transport

External crates depend only on `velo-ext` and implement `Transport` against it:

```toml
[dependencies]
velo-ext = "0.2"  # caret: any 0.2.x; cargo unifies this with the exact =0.2.x pin in velo's workspace
```

Requirements:
1. Implement all required `Transport` methods
2. Store the `TransportObservability` handle from `set_observability` in a `OnceLock<Arc<dyn TransportObservability>>`
3. Emit `velo_transport_*` metrics via the handle on the hot path

See [CLAUDE.md](../CLAUDE.md) for the full checklist (in-tree transport steps also apply, minus the workspace wiring).

## Observability

Each transport receives a `Arc<dyn TransportObservability>` handle via `set_observability`. The runtime hands it pre-bound to the shared `VeloMetrics` registry. Metrics emitted through this handle appear in the `velo_transport_*` Prometheus series labeled with the transport's `TransportKey`.

## TIPC

**Feature gate**: `tipc` (Linux only — `#[cfg(all(target_os = "linux", feature = "tipc"))]`).

TIPC (`AF_TIPC = 30`, in-tree kernel module `tipc.ko` v2.0.0, mainline since 2.6.16) is a
`SOCK_STREAM` connection-oriented transport that reuses `TcpFrameCodec` verbatim.  Measured
intra-node RTT is ~4.4 µs vs ~7.7 µs for loopback TCP; link-layer failure detection defaults
to ~1.5–2.5 s (tunable to 50 ms) vs TCP keepalive's 60 s.

### Setup

**Single node / dev** — zero configuration required:

```sh
sudo modprobe tipc          # loads the kernel module; persist via /etc/modules-load.d/tipc.conf
```

**Cluster** — enable a bearer per node (requires `CAP_NET_ADMIN`; velo itself is unprivileged):

```sh
# L2 Ethernet bearer (identity auto-derived from MAC on kernel ≥ 4.17)
tipc bearer enable media eth device eth0

# L3 UDP bearer (cloud / routed networks; one side is sufficient for replicast)
tipc bearer enable media udp name udp1 localip 10.0.0.1 remoteip 10.0.0.255
```

All nodes with the same `netid` (default `4711`) on a shared bearer auto-form one cluster.
Set a unique `netid` per deployment: `tipc node set netid <id>`.  Pass the same value to
`TipcTransportBuilder::netid(<id>)` — velo cannot read the kernel's configured netid without
TIPC netlink; mismatched values cause all cross-node `register()` calls to return `Gate::Never`.

### Usage

```rust
use velo::transports::tipc::{TipcTransportBuilder, TipcScope};

let transport = Arc::new(
    TipcTransportBuilder::new()
        // .service_type(0x56454C4F)   // default: "VELO" in ASCII; must be ≥ 64
        // .scope(TipcScope::Cluster)  // default; Node scope hides from remote peers
        // .connect_timeout(Duration::from_secs(5))
        // .netid(4711)                // default 4711; MUST match `tipc node set netid <id>`
        //                            // on every node — velo cannot read the kernel value
        .build()?
);
```

`build()` returns `Err` with a clear message if `tipc.ko` is not loaded (`EAFNOSUPPORT`).
`start()` awaits topology subscription establishment and initial name-table replay before
returning, so `register()` never races cold caches.

### Reachability gate

`register()` applies a three-way verdict (`Reachable` / `Never` / `NotYet`) before caching a
remote endpoint:

- **Same `netns_nonce`** → same TIPC stack → `Reachable` immediately (intra-netns).
- **Different `netid`** → wrong cluster → `Never` (permanent rejection).
- **`node == 0` on either side with unequal nonce** → cross-netns without a bearer →
  `Never`.
- **`ep.node == local.node` with unequal nonce** → foreign stack claiming our node identity
  (misconfiguration) → `Never`.
- **Node alive AND live name-table publication matches** (`{service_instance, socket_ref, node}`)
  → `Reachable` (bearer path: zero-RTT confirmation via kernel name table).
- **Otherwise** → name-table still converging or stale endpoint → `NotYet`; the endpoint is
  parked and automatically re-driven through the re-register hook when a matching
  `TIPC_PUBLISHED` or node-up event arrives.  TCP is used in the interim; the flip to TIPC is
  automatic once the publication is visible (typically within milliseconds to a couple of
  seconds at cluster cold-start).

`TIPC_CLUSTER_SCOPE` (the builder default) is required for remote reachability; `NODE_SCOPE`
bindings are invisible in remote name tables and will never pass the `NotYet` check.

### Backpressure and congestion notes

TIPC's per-connection in-flight window is ~128 KiB until the receiver ACKs grow it (vs TCP's
multi-MiB initial buffer), so `SendBackpressure` engages earlier for bulk traffic.  Link
congestion is per node-pair: one slow node can stall writer tasks for all its peers on that
node simultaneously.  The only effective tuning knob is `SO_RCVBUF` on accepted sockets (or
the sysctl `tipc_rmem`; default 2 MiB).  `SO_SNDBUF` is a no-op on TIPC.

### Shutdown semantics

TIPC has **no half-close** — `shutdown(SHUT_WR)` and `shutdown(SHUT_RD)` return `EINVAL`;
only `SHUT_RDWR` is accepted.  `TipcStream::poll_shutdown` maps to `Shutdown::Both`.  A plain
`close()` surfaces as `ECONNRESET` at the peer rather than EOF; the listener treats
`ConnectionReset` with an empty partial-frame buffer as a graceful close (no `DecodeError`
metric, no error log).

### Security

TIPC cluster membership is **unauthenticated**: any node on the bearer domain sharing the
configured `netid` auto-joins the cluster.  Two unrelated deployments on the same L2 segment
silently merge under the default `netid 4711`.  Operators **must**:

1. Set a unique `netid` per deployment (`tipc node set netid <unique-id>`) and pass the
   same value to `TipcTransportBuilder::netid(<unique-id>)` — velo cannot read the kernel's
   configured netid without TIPC netlink; a mismatch silently prevents all cross-node peers
   from registering (`Gate::Never`).
2. Consider TIPC AEAD encryption (`tipc node set key`, kernel ≥ 5.5,
   `CONFIG_TIPC_CRYPTO=y`) for confidentiality and as an additional membership gate.
3. Be aware that CIS Level 2 baselines require `tipc.ko` to be unavailable — loading the
   module is a deliberate security decision in hardened fleets.

Known kernel CVEs affecting TIPC: CVE-2021-43267 (5.10–5.15), CVE-2022-0435 (bearer
required), CVE-2024-36886 (≤ 6.8, fixed 2024-05).  Recommended kernel floor: ≥ 6.9.

### Running TIPC integration tests

```sh
sudo modprobe tipc
RUSTFLAGS="--cfg velo_tipc" cargo test --features tipc --test transports_tipc
RUSTFLAGS="--cfg velo_tipc" cargo test --features tipc --test transports_tipc_shutdown
RUSTFLAGS="--cfg velo_tipc" cargo test --features tipc --test transports_tipc_node_affinity
```

The `--cfg velo_tipc` rustc flag is load-bearing (the `velo_endurance` pattern): the three
test files are gated `#![cfg(velo_tipc)]`, so `cargo test --all-features` *compiles* all
TIPC code but runs zero kernel-dependent tests — green on module-less machines with full
clippy coverage. Without the flag the test binaries build empty and report `0 tests`; CI's
`tipc-tests` job sets it after probing for the module.

### Public test seam

`TipcTransport::topology_state() -> Arc<TopologyState>` and
`TipcTransport::set_reregister_hook(hook)` are `pub` (not `pub(crate)`) to enable
integration tests to directly access the pending map and install hooks without the full
`VeloBuilder` stack (proposal §9).  The velo builder uses `topology_state()` internally
to wire `VeloBackend::register_peer` as the hook; out-of-tree callers may use
`set_reregister_hook` to install their own re-registration logic.
