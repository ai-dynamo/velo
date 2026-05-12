# Streaming saturation runbook

This page is for operators who see a velo deployment with streaming traffic
behaving oddly — sender errors, missed deadlines, a `Dropped` frame appearing
without a producer crash. The cause is almost always **saturation**: the
producer side is generating frames faster than the consumer side can
sustainably drain. Velo tells you so via Prometheus counters; this page
tells you which to look at and how to interpret them.

## The cascade

A velo stream uses four bounded `flume` channels in series, plus a TCP /
gRPC byte stream:

```
producer (StreamSender::send)
    │
    ▼
connect-side flume   bounded(4096)   ← producer-side
    │
    ▼
TCP socket (kernel send/recv buffer) ─── network ─── TCP socket
                                                       │
                                                       ▼
                                       bind-side flume bounded(4096)
                                                       │
                                                       ▼
                                      reader_pump (consumer side)
                                                       │
                                                       ▼
                                       anchor frame_tx bounded(256)  ← smallest, fills first
                                                       │
                                                       ▼
                                                  consumer (StreamAnchor::next)
```

When the consumer falls behind, the **256-deep anchor channel fills first**.
That's the smallest channel in the cascade and it's where you see the
leading-indicator counter tick. Pressure then walks back up:

1. `anchor frame_tx` (256) fills →
2. `reader_pump`'s `try_send` returns `Full` → `velo_streaming_reader_pump_backpressure_total` ticks →
3. `bind-side flume` (4096) fills →
4. server pump's `try_send` returns `Full` → `velo_streaming_server_pump_backpressure_total` ticks →
5. TCP receive buffer fills → kernel sends a zero-window ACK →
6. producer-side TCP write stalls →
7. `connect-side flume` (4096) fills →
8. `StreamSender::send`'s `try_send` returns `Full` → `velo_streaming_producer_send_backpressure_total` ticks →
9. After `DETECTION_MULTIPLIER × heartbeat_interval_ms` (default 3 × 5s = 15s) of no frames,
   reader_pump's heartbeat watchdog fires → `velo_streaming_heartbeat_watchdog_firings_total`
   ticks, and the streaming session terminates. The consumer sees one of two
   things, depending on the cascade state at fire time:
   - **Anchor channel had room** (the common case — empty cascade because the
     producer truly died): consumer receives a `Dropped` terminal frame, which
     surfaces as `StreamError::SenderDropped` from `StreamAnchor::next()`.
   - **Anchor channel was already full** (the saturated-cascade case): the
     watchdog uses a non-blocking `try_send` so registry/cancel cleanup cannot
     deadlock, so the `Dropped` sentinel is silently lost. The consumer drains
     any queued frames and then receives `None` (clean EOF). A `tracing::warn!`
     fires with `local_id` so operators can correlate.

   In **both** cases, `velo_streaming_heartbeat_watchdog_firings_total` is the
   authoritative operator signal: if it ticks, a session was killed by the
   watchdog regardless of how the consumer saw the termination. Do not rely on
   `StreamError::SenderDropped` alone to detect watchdog kills.

## Counters and how to read them

All four are `Counter` (no labels) registered into the Prometheus registry
the application passes to `Velo::builder().metrics(...)`.

| Metric | What it means | Severity |
|---|---|---|
| `velo_streaming_reader_pump_backpressure_total` | The per-anchor 256-deep channel went `Full`. Reader pump fell through to the awaited send. | **Leading indicator.** Anything above 0 with a non-trivial rate means the consumer is at or near saturation. Use a `rate(...[1m])` panel. |
| `velo_streaming_server_pump_backpressure_total` | The 4096-deep transport-level channel went `Full`. The cascade has reached the server-side pump. | The cascade has propagated past the per-anchor channel. Combined with reader_pump backpressure, this is a confirmed saturation event. |
| `velo_streaming_producer_send_backpressure_total` | `StreamSender::send` saw the connect-side 4096-deep channel `Full`. | Producer-application visibility. The producer is now blocking on `send_async`. |
| `velo_streaming_heartbeat_watchdog_firings_total` | `DETECTION_MULTIPLIER × heartbeat_interval_ms` of total silence on a session. Reader pump injected `Dropped` and force-cleaned the session. | **Lagging indicator.** Anything above 0 means a session was killed. Combined with backpressure counters, this confirms it was a saturation kill (not a producer crash). |

## A reference Grafana dashboard

Three panels at minimum:

1. **`rate(velo_streaming_reader_pump_backpressure_total[1m])`** — climbing
   means the consumer is starting to fall behind. This is your earliest
   warning.

2. **`rate(velo_streaming_server_pump_backpressure_total[1m])`** and
   **`rate(velo_streaming_producer_send_backpressure_total[1m])`** plotted
   alongside the reader_pump rate — when these climb in sequence behind the
   reader_pump rate, you're watching the cascade walk upward.

3. **`increase(velo_streaming_heartbeat_watchdog_firings_total[5m])`** — any
   non-zero value here means a session died. Combine with the rate panels
   above to confirm whether it was saturation (counters climbing) or a
   real producer crash (counters flat).

## Watchdog log line

When the watchdog fires, `reader_pump` emits a single `tracing::warn` line
with the diagnostic context inline:

```
reader_pump: heartbeat watchdog fired, injecting Dropped (saturation indicator: see velo_streaming_*_backpressure_total)
  local_id=...
  anchor_frame_tx_len=256 anchor_frame_tx_cap=256
  transport_rx_len=4096 transport_rx_cap=4096
  heartbeat_deadline_ms=5000
  detection_multiplier=3
```

If `anchor_frame_tx_len` equals `anchor_frame_tx_cap` (or close to it), the
session was saturating — the cascade had filled the anchor channel and the
producer ran out of TCP credit. If both depths are near 0, the session went
silent for some other reason (real producer crash, network partition).

## Mitigation knobs

In rough order of "easiest to apply" to "biggest hammer":

1. **Slow the producer.** A `tokio::time::sleep` of even 100µs between
   sends, or `tokio::task::yield_now().await`, often takes a deployment
   from saturating to comfortable. The producer is unaware of consumer
   throughput; either back off voluntarily or use the producer-side
   backpressure counter as a feedback signal.

2. **Speed the consumer.** Move work *out* of the `anchor.next().await`
   loop into a downstream task connected by an unbounded channel; the
   anchor consumer should do the minimum to take the frame and hand it off.

3. **Resize channels.** The `bounded(256)` per-anchor channel is the
   smallest in the cascade; raising it absorbs bigger producer bursts at
   the cost of memory per active anchor. The 4096-deep transport channels
   already absorb several seconds of typical load on their own.

4. **Reduce the number of concurrent anchors.** Velo handles hundreds-to-low-thousands
   of concurrent anchors, but each consumer task multiplies channel
   memory and reader_pump scheduling pressure. If your application
   creates one anchor per work item, batch work items into one anchor.

5. **Architectural change**: split the heartbeat onto a dedicated
   side-channel so saturation can't kill an otherwise-healthy session.
   Not currently implemented; would surface as a follow-up if real
   workloads (not stress tests) hit the watchdog.

## What this is NOT

These counters do not measure latency, throughput, or bytes. They count
*backpressure events*. A high event rate doesn't mean things are broken;
it means the system is at its capacity ceiling and the application
should slow down or scale out. A zero rate means there is plenty of
headroom.
