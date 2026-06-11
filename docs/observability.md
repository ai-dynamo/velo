# observability

Shared Prometheus metrics for Velo. Does not run an exporter — callers register `VeloMetrics` into a `prometheus::Registry` and expose it however they want.

## Usage

```rust
use prometheus::Registry;
use velo::VeloMetrics;

let registry = Registry::new();
let metrics = Arc::new(VeloMetrics::register(&registry)?);

// Pass to builder
let velo = Velo::builder()
    .add_transport(transport)
    .metrics(metrics)
    .build()
    .await?;
```

`VeloMetrics` is internally cloned and distributed to each subsystem (transports, messenger, anchor manager, rendezvous manager).

## Metric Families

All metric names are in the `velo_` namespace.

### Transport

| Metric | Type | Labels |
|--------|------|--------|
| `velo_transport_frames_total` | Counter | `transport`, `direction`, `message_type` |
| `velo_transport_frame_bytes_total` | Counter | `transport`, `direction`, `message_type` |
| `velo_transport_rejections_total` | Counter | `transport`, `reason` |
| `velo_transport_send_backpressure_total` | Counter | `transport` |
| `velo_transport_registered_peers` | Gauge | `transport` |
| `velo_transport_active_connections` | Gauge | `transport` |

`direction` values: `inbound`, `outbound`
`message_type` values: `message`, `response`, `ack`, `event`, `shutting_down`
`reason` values (rejections): `draining`, `no_peer`, `channel_full`

### Messenger

| Metric | Type | Labels |
|--------|------|--------|
| `velo_messenger_handler_requests_total` | Counter | `handler`, `response_type` |
| `velo_messenger_handler_duration_seconds` | Histogram | `handler`, `outcome` |
| `velo_messenger_handler_request_bytes_total` | Counter | `handler` |
| `velo_messenger_handler_response_bytes_total` | Counter | `handler` |
| `velo_messenger_handler_in_flight` | Gauge | `handler` |
| `velo_messenger_dispatch_failures_total` | Counter | `reason` |
| `velo_messenger_client_resolution_total` | Counter | `outcome` |
| `velo_messenger_pending_responses` | Gauge | — |
| `velo_messenger_response_slot_exhausted_total` | Counter | — |

### Streaming

| Metric | Type | Labels |
|--------|------|--------|
| `velo_streaming_anchor_operations_total` | Counter | `operation` |
| `velo_streaming_anchor_operation_duration_seconds` | Histogram | `operation` |
| `velo_streaming_active_anchors` | Gauge | — |
| `velo_streaming_backpressure_total` | Counter | `transport` |
| `velo_streaming_reader_pump_backpressure_total` | Counter | — |
| `velo_streaming_server_pump_backpressure_total` | Counter | — |
| `velo_streaming_producer_send_backpressure_total` | Counter | — |
| `velo_streaming_heartbeat_watchdog_firings_total` | Counter | — |

### Rendezvous

| Metric | Type | Labels |
|--------|------|--------|
| `velo_rendezvous_operations_total` | Counter | `operation` |
| `velo_rendezvous_operation_duration_seconds` | Histogram | `operation` |
| `velo_rendezvous_bytes_total` | Counter | `direction` |
| `velo_rendezvous_active_slots` | Gauge | — |

## Type-Safe Label Enums

Call sites use typed enums rather than raw strings:

- `Direction` (`Inbound` / `Outbound`) — defined in `velo_ext::observability`, re-exported from `velo::observability`
- `TransportRejection` (`Draining` / `NoPeer` / `ChannelFull`) — same
- `HandlerOutcome`, `StreamingOp`, `RendezvousOp` — internal to `velo` crate

## Distributed Tracing

Enable the `distributed-tracing` feature for OpenTelemetry propagation helpers that inject the current context into message headers and extract a remote parent on receive. Uses `tracing_opentelemetry`.

## Test Helpers

`velo::observability::test_helpers::MetricSnapshot` and related types are available (when the `test-helpers` feature is enabled) for asserting counter, gauge, and histogram values in integration tests.

## External Transport Observability

Out-of-tree transports receive a `Arc<dyn TransportObservability>` handle via `Transport::set_observability`. The trait is defined in `velo_ext::observability` (no `prometheus` dep required in `velo-ext`). In-tree transports store it in `OnceLock<Arc<dyn TransportObservability>>` and call trait methods on the hot path. External transport authors get the same handle and emit into the same `velo_transport_*` Prometheus series.
