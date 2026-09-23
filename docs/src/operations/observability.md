# Observability

`velo::observability` gives Prometheus metrics for all subsystems. You create a `Registry` and register `VeloMetrics` into it. Velo does not run an exporter. Serve or scrape the registry with your own code.

```rust,ignore
use prometheus::Registry;
use velo::{Velo, VeloMetrics};

let registry = Registry::new();
let metrics = Arc::new(VeloMetrics::register(&registry)?);
let node = Velo::builder()
    .add_transport(tcp)
    .metrics(metrics)
    .build()
    .await?;
```

For the full list of families, see the [Metrics reference](../appendix/metrics.md).

## Families

| Area | What it covers |
|---|---|
| Transport | Frames, bytes, rejections, registered peers, active connections, send backpressure. TCP and UDS also give egress queue wait, frames written, and write duration. |
| Messenger | Handler requests, durations, payload bytes, in-flight handlers, dispatch failures, departures from the inbound queue, ordered lanes |
| Streaming | Anchor operations and durations, attach round-trip time, active anchors, backpressure, and the batched streaming (mux) families |
| Rendezvous and RDMA | Stage, get, and release operations, durations, bytes, active slots, registered bytes, RDMA path decisions |

With the `distributed-tracing` feature, Velo puts OpenTelemetry trace context in the message headers.

## Read some metrics as differences

Some quantities have no gauge. A sampled channel length reads the wrong number under the load that makes the depth worth knowing. Velo gives two counters instead, and you subtract them.

Before you subtract, aggregate both sides to the same labels. The two sides often have different labels, so a bare `a - b` matches nothing.

### Inbound queue depth

```promql
sum by (job, instance) (velo_transport_frames_total{direction="inbound",message_type="message",outcome="accepted"})
- sum by (job, instance) (velo_messenger_inbound_dequeued_total)
```

- The result can go negative for a short time. A transport counts the frame after `admit_message` has already put it on the queue, so the consumer can count the departure first. Clamp at zero.
- After a `Timeout` shutdown, the result stays high by the number of messages that were abandoned. Those messages never count as departures.
- The difference is correct only for transports that record what they admit. All in-tree messenger transports do. The `simulation` transport does not.

### Egress queue depth, for each transport

```promql
sum by (job, instance, transport) (velo_transport_frames_total{direction="outbound",outcome="accepted"})
- sum by (job, instance, transport) (velo_transport_frames_written_total)
```

This is the number of frames in front of the socket: in the bounded send channel, or staged in the writer. It has these limits:

- It does not include frames held in the admission gate. `velo_transport_send_backpressure_total` counts those, and `velo_transport_egress_queue_wait_seconds` covers both the gate and the channel.
- It stops at the kernel. The sockets have a 2 MiB send buffer. To see bytes below the counter, read `tx_queue` for the socket.
- It can go negative for a short time. Clamp at zero.
- It stays high after a frame fails: a replaced connection, a socket error, a failed connect, or a stop of the writer. Those frames were accepted and never written.
- Only the coalescing writer (TCP and UDS) publishes `frames_written`. gRPC, NATS, ZMQ, and UCX have no such series, so the query returns no rows for them. Do not add `or vector(0)`, because zero reads as "queue empty", not "not measured".
- The `transport` label is the `TransportKey` of the transport, not a fixed name. Select on the series, not on a name pattern.

### Queueing on the receiver during attach

```promql
  sum(rate(velo_streaming_anchor_attach_rtt_seconds_sum{outcome="success"}[5m]))
/ sum(rate(velo_streaming_anchor_attach_rtt_seconds_count{outcome="success"}[5m]))
-
  sum(rate(velo_streaming_anchor_operation_duration_seconds_sum{operation="attach",outcome="success"}[5m]))
/ sum(rate(velo_streaming_anchor_operation_duration_seconds_count{operation="attach",outcome="success"}[5m]))
```

Both families are histograms, so the query compares mean durations from `_sum` and `_count`. The `sum` removes the labels that differ between sender and receiver.

The sender records the round trip. The receiver records the time inside the handler. The difference is an upper bound on the queueing at the receiver: it also includes the send path, both wire legs, the handler spawn, and the wake of the sender task.

- The query uses `outcome="success"` on both sides, because the error populations do not compare.
- The query sums away `transport_scheme` and `instance`. The two series come from different nodes, and the sender can record `"unknown"` for a scheme that the receiver named.
- SPSC and MPSC attaches share one RTT series, so the result is an average over both.

## Batched streaming

For the mux families and how to find a stream that cannot keep up, see [Stream saturation](saturation.md).
