# Hot paths

The tier is how often a *cost* is paid, not where the code sits. Tier each
changed line, not the function around it: walk the callers from the changed
line up to an entry point below, and count the loops on the way. A function that is not in this map is not
cold; trace it. Find each symbol with `grep -rn 'fn <name>' lib/`. This file
has no line numbers on purpose: they go stale.

| Tier | The cost is paid | Reviewed |
|---|---|---|
| T1 | per message, frame, record, token or byte; or per batch or sweep when the work grows with streams, slots or peers | yes |
| T2 | per connection, stream open or close, slot, peer, constant-work batch, burst, or timer tick | yes, at lower severity |
| T3 | at build, start, register, discovery, handshake, shutdown; on error paths and first use | no, unless its effect is T1 |

Examples where the code site misleads:
- Setting `SO_RCVBUF` on an accepted socket runs once per connection, but it
  collapsed the TCP window for the life of that connection. That is a
  per-byte cost, so it is T1.
- A walk over every slot runs once per batch, but its work grows with the
  slot count. That is a per-record cost, so it is T1.
- An error branch inside a per-frame loop is T3, and so is a log line before
  the loop starts (T2 at most).

A file that is not compiled (docs, CI, scripts) has no tier. Check for
`include_str!` before you call a file uncompiled.

## Budgets, for scale

- One TCP message, 1 KB, depth 1, loopback: the full transport (rung L3) is
  about 11.4 µs one way at p50. UDS L3 is about 5.2 µs. The codec adds
  16 ns. `AdmissionGate::send` adds 16 ns over `flume::try_send`. Two
  cross-thread wakes are 84% of the transport's share.
  (docs/src/development/rdma-design.md, "Where the per-message time goes")
- One mux record: 5.3 to 8.6 µs of frontend CPU.
  (docs/src/operations/response-plane-performance.md, "The reader pump
  leaves the mux data path")
- So 100 ns per operation is about 1% of either budget.

## Unary send: T1 per message

- `lib/velo/src/messenger/client/builders/mod.rs`: `make_stage`, `acquire`,
  `dispatch`, `create_metadata`, `build_metadata`. `spawn_slow_path` (first
  contact, discovery) is T2.
- `lib/velo/src/messenger/client/mod.rs`: `can_send_directly`,
  `send_message`, `register_outcome`.
- `lib/velo/src/messenger/common/responses.rs`: `try_register_outcome`,
  `register_outcome`, `SlotArena`, `try_acquire`, `complete_outcome`.
- `lib/velo/src/messenger/common/messages.rs`: `encode_active_message`.
- `lib/velo/src/transports.rs`: `VeloBackend::send_message_on_lane`,
  `instrument_transport_error_handler`, `finalize_send_outcome`.
- `lib/velo/src/transports/{tcp,quic}/transport.rs`:
  `send_message_on_lane`, `admit`. `transports/uds/transport.rs`:
  `send_message`, `admit`.
- `lib/velo-ext/src/admission.rs`: `AdmissionGate::send`. The fast path is
  one lock plus `try_send`. A full channel takes a `Ticket` and may spawn
  `drive`.
- `lib/velo/src/transports/coalesce/mod.rs`: `run_coalescing_writer`,
  `FrameBatchBuffer::{classify, push, flush_to}`, `flush`,
  `write_frame_direct`, `write_all_bounded`, `wait_cancelled`, `EgressLog`.
- `lib/velo/src/transports/tcp/framing.rs`: `TcpFrameCodec::{encode_frame,
  append_frame, build_preamble}`, `stage_direct_prefix`.
- `lib/velo/src/transports/{tcp,uds}/writer.rs`: the `Coalescable` and
  `WriterObserver` impls of each connection writer.

## Receive: T1 per frame and message

- Connection loops: `TcpListener::handle_connection`
  (`transports/tcp/listener.rs`; `connection_teardown` runs once per
  connection), `handle_connection` (`transports/uds/listener.rs`),
  `serve_stream` (`transports/quic/listener.rs`). `run_dialed_reader`
  (`transports/ingress.rs`) is T3: a dialed socket normally carries only
  `ShuttingDown` replies.
- `TcpFrameCodec::decode`, `maybe_shrink_read_buffer`
  (`transports/tcp/framing.rs`).
- `route_frame`, `record_inbound_frame` (`transports/ingress.rs`).
- `TransportAdapter::admit_message`, `ShutdownState::acquire`
  (`lib/velo-ext/src/transport.rs`). Its SeqCst pair is a documented and
  accepted cost.
- The `create_message_handler` loop (`messenger/server/mod.rs`), with
  `BurstRef` and `BURST_REFRESH`.
- `decode_active_message` (`messenger/common/messages.rs`).
- `messenger/server/dispatcher.rs`: `DispatcherHub::dispatch_message`,
  `SpawnedDispatcher::dispatch` (one spawn per message, by design),
  `OrderedDispatcher::dispatch`. `messenger/server/lanes.rs`:
  `LaneRouter::route` and the lane task.
- `messenger/handlers/mod.rs`: the `handle` impls (`UnaryExecutorAdapter`
  and the others), `send_response`, `send_response_ok`,
  `send_response_error`. `encode_response_header` (`responses.rs`).

## Response arrival: T1 per response

`route_frame` → `create_response_handler` (`messenger/server/mod.rs`) →
`decode_response_header` → `complete_outcome` (`responses.rs`).

## Streams and the mux: T1 per record

- Producer: `StreamSender::send`, `StreamSender::send_bytes`,
  `is_terminal_sentinel` (`streaming/sender.rs`); `encode_bytes_item`,
  `decode_bytes_frame` (`streaming/frame.rs`); `send`
  (`streaming/mpsc/sender.rs`); `SenderClosed`
  (`streaming/control/registry.rs`).
- Batcher (`streaming/messenger_mux/peer_batcher/`): `run`, `drain_once`,
  `dispatch`, `flush` (`mod.rs`); `on_frame`, `emit_data`,
  `send_singleton` (`records.rs`); `ensure_batch`, `fits`, `flush`,
  `dispatch` (`writer.rs`); `slot_stream.rs`; `flush_gate.rs`. `admit`
  (`messenger_mux/flow_control/mod.rs`). Encode and decode
  (`messenger_mux/protocol/mod.rs`).
- Ingress: `MuxCore::deliver_batch` (`messenger_mux/mod.rs`) →
  `handle_batch`, `apply_record` (`ingress/mod.rs`) → `apply_data`,
  `deliver`, `admit`, `classify`, `reconcile`, `take_grant`
  (`ingress/slot.rs`). `DirtySlots::{mark, take}` (`ingress/dirty.rs`).
- Consumer: `StreamAnchor::poll_next` (`streaming/anchor.rs`); the direct
  feed (`streaming/control/feed.rs`).
- The per-stream path, used when the mux is not negotiated (`--legacy` in
  the harnesses). It is still T1 on that path: `reader_pump`
  (`streaming/control/pump.rs`), `streaming/tcp_transport.rs`,
  `streaming/grpc_transport.rs`.
- T2: the credit sweep (`messenger_mux/sweep.rs`), `lane_choice.rs`, the
  `OpenSlot` handling and `close_slot` (`ingress/mod.rs`), and
  `stream_watchdog` (`streaming/control/feed.rs`; it wakes once per
  window). The unattached deadlines: `AnchorDeadlineQueue::push` on
  anchor create and detach, and its sweep (`streaming/anchor.rs`); the
  bind expiry `MuxCore::expire_binds` (`streaming/messenger_mux/mod.rs`),
  on the shared `DeadlineQueue` (`streaming/deadline.rs`).

## Other transports: T1 per frame

- gRPC, `transports/grpc/transport.rs`: `send_message` → `admit` → the
  writer loop (`build_preamble`, `FramedData`, a tokio mpsc). Inbound:
  `transports/grpc/server.rs`.
- NATS, `transports/nats/transport.rs`: `send_message` → `build_nats_frame`
  → the sender task; its own `route_frame`.
- ZMQ, `transports/zmq/transport.rs`: `send_message` → the `run_sender`
  thread. Inbound: `transports/zmq/listener.rs`.
- UCX, `transports/ucx/transport.rs`: `send_message`, `admit` →
  `AdmissionGate` → a per-peer forwarder task (spawned in
  `get_or_create_connection`; a hop per send when it is parked) → the
  command ring → `transports/ucx/worker.rs`: `Doorbell::ring`, `run_loop`,
  `drain_ring`, `handle_cmd`, `post_am`. These run on the progress thread
  (`UcxConfig::spin_us`).
- Rendezvous and large payloads (`rendezvous/**`, `transports/ucx/rma.rs`,
  `messenger/large_payload`): T2 per transfer. A memory registration per
  transfer counts.

## Observability on these paths: T1

Metrics are off by default (`VeloBuilder::metrics`), but production and the
mux harnesses install them, so these are T1 when metrics are on.

- `observability.rs`: `TransportMetricsHandle::record_frame` (pre-bound
  arrays; its `with_label_values` arm is a debug-assert fallback),
  `record_frames_written`, `record_egress_*`, `egress_children` (a
  `OnceLock`).
- Any `VeloMetrics::record_*` called from T1 code: check whether it
  pre-binds. `bind_*` functions are setup; pre-binding is the pattern.
- `coalesce/mod.rs`: `EgressLog` and `EgressMetrics` take an `Instant::now`
  and one observe per frame. That cost is accepted in
  response-plane-performance.md, "Instrumentation cost".
- The `distributed-tracing` span in `VeloBackend::send_message_on_lane` is
  T1 when that feature is on.

## Measured defaults: a change to one is a T1 change

| Default | Where | Measured in |
|---|---|---|
| `COALESCE_THRESHOLD` 64 KiB, `DIRECT_PREFIX_CAP` 256 B | `transports/tcp/framing.rs` | concepts/transports.md, "Coalescing writer" |
| `DEFAULT_MAX_BATCH_BYTES`, `DEFAULT_MAX_BATCH_FRAMES` 1024 | `transports/coalesce/mod.rs` | concepts/batched-streaming.md, "Per-stream coalescing cannot reach the forward-pass shape" |
| `DEFAULT_SOCKET_BUFFERS` 2 MiB; `socket_buffers(None)` | `transports/tcp/listener.rs`, `transports/tcp/transport/builder.rs` | concepts/transports.md, "Socket buffers are set before data flows" |
| `DEFAULT_SHRINK_THRESHOLD`, `SHRINK_RESET_CAPACITY` | `transports/tcp/framing.rs` | code comments |
| `channel_capacity` 256 | `transports/tcp/transport/builder.rs`, `transports/uds/transport.rs` | `tx_budget --channel-capacity` |
| `BURST_REFRESH` 128 | `messenger/server/mod.rs` | code comment |
| `MuxConfig::default()`: `initial_credit` 32, `max_batch_bytes` 60 KiB, `credit_sweep_interval` 200 ms, `drain_visit_floor` 2 ms, `reply_linger` 1 ms, `async_open_ack` false, `flush_policy` Auto | `streaming/messenger_mux/config.rs` | guides/tune-batched-streaming.md; operations/response-plane-performance.md |
| `DEFAULT_PEER_BYTE_BUDGET`, `DEFAULT_SLOT_BYTE_BUDGET` | `streaming/messenger_mux/flow_control/mod.rs` | guides/tune-batched-streaming.md, "Change the byte budgets" |
| `GSO_SAFE_MAX_MTU` 6550, `DEFAULT_SERVER_ENDPOINTS`, UDP buffers | `transports/quic/transport/builder.rs` | operations/quic-performance.md |
| `UcxConfig::spin_us` 20 | `transports/ucx/transport.rs` | operations/rdma-performance.md |
| `DEFAULT_SENDER_CAPACITY` 1024 | `transports/nats/transport.rs` | none |

## Perf-sensitive dependencies

tokio, tokio-util (`CancellationToken` costs; the comment in
`create_message_handler` asks for a new measurement after a bump), flume,
dashmap, parking_lot, bytes, quinn and quinn-udp (GSO), tonic and prost,
async-nats, zmq, rmp-serde, prometheus.

A version change to any of these in a `Cargo.toml` or `Cargo.lock` is a
Stage 0 flag. `examples/` has its own `Cargo.lock`, so a bump in the root
lock does not reach the harnesses.
