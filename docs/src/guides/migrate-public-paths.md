<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Move to the public paths of 0.23.0

velo 0.23.0 and velo-ext 0.6.0 make three breaking changes to the public API:

- Each public item has one path. A program that names an item at a path that is gone must use the path in the map below.
- `FrameTransport` is no longer public. Stream transports are in-tree only.
- The wire messages and the internals of the anchor and rendezvous managers are no longer public. `Velo::anchor_manager()` and `Velo::rendezvous_manager()` are removed.

Two other 0.23.0 changes have their own guides: the `Messenger` front door is removed (see [Move off the Messenger front door](migrate-messenger-front-door.md)), and a `Velo` opens no per-stream listener by default (see [Services and stream transports](migrate-services.md#0230-the-per-stream-listener-is-opt-in)).

## The path map

Where an item had several paths, the path that consumers use stays. The others are removed.

### Streams

| Item | Path that stays | Paths removed |
|---|---|---|
| `StreamAnchor`, `StreamAnchorHandle`, `StreamController`, `StreamSender`, `StreamFrame`, `StreamError`, `AttachError`, `SendError` | `velo::X` | `velo::streaming::X`, and `velo::streaming::{anchor, frame, handle, sender}::X` |
| `StreamOpenTicket` | `velo::StreamOpenTicket` | `velo::streaming::control::StreamOpenTicket` |
| `AnchorConfig`, `AnchorReservation`, `ReservationConfig`, `ByteItemBuilder`, `ByteItemFrame`, `AnchorKind`, `Retired`, `Retirement` | `velo::streaming::X` | `velo::streaming::{anchor, frame, handle, sender}::X` |
| `MpscAnchorConfig`, `MpscFrame`, `MpscStreamAnchor`, `MpscStreamController`, `MpscStreamSender`, `SenderId` | `velo::streaming::X` | `velo::streaming::mpsc::X` and its submodules |
| `TcpFrameTransport`, `GrpcFrameTransport` | `velo::streaming::X` | `velo::streaming::{tcp_transport, grpc_transport}::X` |
| `TCP_STREAM_KEY`, `GRPC_STREAM_KEY` | `velo::streaming::X` | `velo::streaming::{tcp_transport, grpc_transport}::X` |

`AutoFlush`, `FlushPolicy`, `MuxConfig`, `MESSENGER_MUX_KEY` and `MAX_ITEM_BYTES` keep `velo::streaming::X`, their one path.

### Messaging, events and discovery

| Item | Path that stays | Paths removed |
|---|---|---|
| `Handler`, `Context`, `TypedContext`, the handler builders, the send builders and their results, `RequestTimeout`, `OrderedConfig`, `OrderingKey`, `OverflowPolicy`, `UnifiedResponse` | `velo::X` | `velo::messenger::X` |
| `VeloEvents` | `velo::VeloEvents` | `velo::messenger::VeloEvents`, `velo::messenger::events::VeloEvents` |
| `Event`, `EventAwaiter`, `EventBackend`, `EventHandle`, `EventManager`, `EventPoison`, `EventStatus` | `velo::X` | `velo::events::X` |
| `DistributedEventFactory` | `velo::events::DistributedEventFactory` | `velo::events::factory::DistributedEventFactory` |
| `PeerDiscovery` | `velo::discovery::PeerDiscovery` | `velo::PeerDiscovery`, `velo::messenger::PeerDiscovery`, `velo::messenger::discovery::PeerDiscovery` |
| `FilesystemPeerDiscovery` and the other filesystem discovery types | `velo::discovery::X` | `velo::discovery::{filesystem, filesystem_service}::X` |

`Messenger` is not in this map. `MessengerBuilder` is removed in 0.23.0; the [Messenger front door guide](migrate-messenger-front-door.md) covers it.

### Rendezvous, transports and metrics

| Item | Path that stays | Paths removed |
|---|---|---|
| `DataHandle`, `DataMetadata`, `RegisterOptions`, `StageMode`, `RendezvousWrite` | `velo::X` | `velo::rendezvous::X`, and `velo::rendezvous::{handle, protocol, store, write}::X` |
| `PinnedWriter`, `RdmaDestination` | `velo::X` | `velo::rendezvous::write::X` |
| `RegionGuard`, `RegionWatch`, `Deregistered`, `PinnedBuf`, `RdmaConfig`, `RdmaError`, `RdmaPoolConfig`, `RdmaRendezvousConfig`, `RegisterOwnedError` | `velo::rendezvous::rdma::X` | `velo::X` |
| `VeloMetrics` | `velo::observability::VeloMetrics` | `velo::VeloMetrics` |
| `Transport`, `ShutdownPolicy`, `AdmissionState` | `velo::X` | `velo::transports::X` |
| `InterfaceEndpoint`, `InterfaceFilter`, `parse_endpoints` and the other interface helpers | `velo::transports::utils::interfaces::X` | `velo::transports::X`, `velo::transports::utils::X` |
| The queue types (`NextOptions`, `WorkQueueBackend`, `WorkQueueError`, ...) | `velo::queue::X` | `velo::queue::{backend, error, options, receiver, sender}::X` |
| `ReceiverFuture`, `SenderFuture` | `velo::queue::X` | `velo::queue::backend::X` |
| The simulation types, and `Transfer` | `velo::simulation::X` | `velo::simulation::{discovery, fabric, network, transport}::X` |

`velo::ext` is the velo-ext crate itself. A velo-ext item keeps its path under it, beside the one velo path in the map.

### No longer public

| Item | Use instead |
|---|---|
| `AnchorManager`, `AnchorManagerBuilder`, `Velo::anchor_manager()` | The anchor methods on `Velo`. See [Move off the manager accessors](migrate-manager-accessors.md). |
| `RendezvousManager`, `Velo::rendezvous_manager()`, `rendezvous::store::DataStore` | The rendezvous methods on `Velo`, such as `unregister_data` and `register_data_in_region`. |
| `rendezvous::transparent::DEFAULT_THRESHOLD` | `Velo::staging_threshold()` |
| `RendezvousStager`, `RendezvousResolver` | Nothing. `Velo::builder` installs large payload support. |
| The control-plane wire messages: `AnchorAttachRequest`, `AnchorAttachResponse`, the detach, finalize and cancel requests, `StreamCancelHandle`, `StreamCancelRequest`, the MPSC attach, detach and cancel messages, and the `rendezvous::protocol` requests | Nothing. velo's own tests reach a few of them through `velo::test_helpers`, behind the `test-helpers` feature. That module is not a supported API. |
| `rendezvous::{handlers, consumer}`, `streaming::control::{SenderRegistry, SenderEntry, SenderClosed, create_stream_cancel_handler, DETECTION_MULTIPLIER}` | Nothing. |
| `velo::streaming::FrameTransport`, `velo::ext::FrameTransport`, `velo_ext::streaming` | See below. |

## FrameTransport

velo-ext 0.6.0 has no `streaming` module. A stream rides the messenger mux, so a new transport implements the messenger `Transport` and its streams get the mux. See [Write an out-of-tree transport](out-of-tree-transport.md).

The per-stream transports stay: `TcpFrameTransport`, `GrpcFrameTransport`, `StreamConfig`, `TcpConfig`, `GrpcConfig`, and `VeloBuilder::{stream_config, stream_bind_addr}` keep their signatures.

## What each consumer moves

The moves below come from the consumer trees as of 0.22.1. They cover the paths in the map. The [manager accessors guide](migrate-manager-accessors.md) covers the calls through `anchor_manager()` and `rendezvous_manager()`.

| Consumer | Moves |
|---|---|
| dynamo | `lib/runtime/src/pipeline/network/velo_response.rs`: `velo::streaming::{StreamAnchor, StreamAnchorHandle, StreamController, StreamError, StreamFrame, StreamSender}` to `velo::X`; `velo::streaming::control::StreamOpenTicket` to `velo::StreamOpenTicket`; `velo::VeloMetrics` to `velo::observability::VeloMetrics`. `AnchorConfig`, `AutoFlush`, `ByteItemBuilder`, `FlushPolicy`, `MuxConfig` and `MESSENGER_MUX_KEY` stay in `velo::streaming`. |
| dynamo-gdl | `crates/gdl-runtime/tests/{paceguard.rs, remote/setup.rs}`: `velo::streaming::control::StreamOpenTicket` to `velo::StreamOpenTicket`. `src/dist/fabric/renewal.rs`: `velo::streaming::StreamAnchorHandle` to `velo::StreamAnchorHandle`. `src/dist/fabric.rs`: `velo::PeerDiscovery` to `velo::discovery::PeerDiscovery`, `velo::VeloMetrics` to `velo::observability::VeloMetrics`, and `rendezvous::transparent::DEFAULT_THRESHOLD` to `Velo::staging_threshold()`. `tests/common/mod.rs`: `velo::VeloMetrics` to `velo::observability::VeloMetrics`. `crates/gdl-hub/src/pool/instances.rs`: `velo_hub::velo::transports::utils::parse_endpoints` to `velo_hub::velo::transports::utils::interfaces::parse_endpoints`. |
| dynamo-http-endpoints | Nothing in the map. Its `velo_hub::velo` paths stay. |
| kvbm | Nothing in the map. It keeps `velo::discovery::PeerDiscovery` and the root stream types. It implements only `PeerDiscovery` from velo-ext, which 0.6.0 does not change. |
| rhino | `crates/rhino-bootstrap/tests/velo_world.rs`: `velo::streaming::{StreamAnchorHandle, StreamFrame}` to `velo::X`. It keeps `velo::discovery::PeerDiscovery`. |
| deepseek-ultrafast | `crates/kv-transfer/src/rdma.rs`: `velo::rendezvous::store::DataStore` is gone. Use `Velo::unregister_data` for the owner's release, and `DataHandle::pack` where a test built a handle from a store. The RDMA types keep `velo::rendezvous::rdma::X`, and `PinnedWriter`, `RendezvousWrite` and `RdmaDestination` keep `velo::X`. |
