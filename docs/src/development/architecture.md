# Workspace crates

The workspace has two library crates and one FFI crate.

| Path | Crate | Published | Contents |
|---|---|---|---|
| `lib/velo` | `velo` | yes | The runtime. All transports, discovery backends, streaming, the messenger, queues, rendezvous, and metrics are modules here. |
| `lib/velo-ext` | `velo-ext` | yes | The trait surface for out-of-tree plugins, and the value types these traits use |
| `crates/ucx-rs` | `ucx-rs` | yes, versioned alone | Rust bindings and a vendored static build of UCX, for the `ucx` feature |
| `examples` | `velo-examples` | no | Examples. Not a workspace member. |

## Why only two crates

There used to be nine crates: `velo-messenger`, `velo-transports`, `velo-streaming`, and more. Each had its own version. A 0.1.1 patch of one crate changed its dependency on another internal crate from 0.1 to 0.2. Cargo took 0.1.1 as compatible with 0.1.0, so downstream lockfiles got two copies of the messenger types. The result was a flood of E0277 and E0308 errors in downstream builds.

The fix was structural. All runtime code moved into `lib/velo/src/`. Do not add a new crate under `lib/`.

`ucx-rs` is the one exception. It is a leaf FFI crate. It exports no types that `velo` or `velo-ext` share, so it cannot cause two copies of a shared type. It must be on crates.io before a `velo` release with the `ucx` feature can ship.

## What goes in `velo-ext`

- The traits: `Transport`, `FrameTransport`, `PeerDiscovery`, `ServiceDiscovery`, `TransportObservability`.
- The types in their signatures: `WorkerId`, `InstanceId`, `PeerInfo`, `WorkerAddress`, `TransportKey`, `MessageType`, `TransportError`, `SendOutcome`, `ShutdownState`, `Direction`, and similar.
- The channel types in their signatures: `TransportAdapter`, `DataStreams`, `make_channels`, `AdmissionGate`.

These stay out of `velo-ext`:

- Concrete transports and discovery backends.
- `prometheus`, `tonic`, `axum`, and other heavy dependencies.
- The runtime types: `Messenger`, `Velo`, `AnchorManager`, `RendezvousManager`.

The test for the boundary: `cargo tree -p velo-ext | grep -c prometheus` must print `0`.

## Module map of `velo`

| Module | Contents |
|---|---|
| `velo::messenger` | Active messages, handlers, dispatch, distributed events |
| `velo::events` | The generational event system |
| `velo::streaming` | Anchors, senders, frame transports, and the mux |
| `velo::rendezvous` | Large-payload staging and the RDMA GET path |
| `velo::transports` | TCP, UDS, NATS, gRPC, ZMQ, UCX, and the shared ingress and writer code |
| `velo::discovery` | Filesystem, NATS, and etcd backends |
| `velo::queue` | Work queues |
| `velo::observability` | `VeloMetrics` and the Prometheus families |
| `velo::simulation` | The discrete-event simulation transport |
