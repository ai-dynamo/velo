# Tune batched streaming

Use this guide to change the settings of the messenger mux, choose a flush policy, or turn the mux off. Read [Batched streaming](../concepts/batched-streaming.md) first for the mechanisms that each setting controls.

The mux is on by default, and most deployments change nothing. Keep the defaults until a measurement shows a reason to change one.

## The mux is on by default

A `Velo` builder installs the mux with `MuxConfig::default()`. The per-stream transport stays configured beside it, because negotiation needs it to serve peers that do not offer the mux. An attach uses the mux only when both sides advertise `messenger-mux-v1`. Every other pair uses the per-stream path.

To turn the mux off without a code change, set `VELO_MESSENGER_MUX_DISABLE=1` and restart the process. Only `1`, `true`, `yes` and `on` (any case) count. Velo reads the variable once, when it builds the node. The variable wins over `enabled: true` set in code, so a benchmark that must measure the mux must not inherit it.

A node with the mux runs one sweep task, which wakes five times a second, and registers the `_stream_batch` handler, even if it never streams.

Call `messenger_mux` to change a setting, or to turn the mux off:

```rust
use velo::streaming::MuxConfig;
use velo::{StreamConfig, TcpConfig, Velo};

let velo = Velo::builder()
    .add_transport(transport)
    // The per-stream path stays configured. Negotiation picks per attach.
    .stream_config(StreamConfig::Tcp(Some(TcpConfig::new(bind_addr))))?
    .messenger_mux(MuxConfig {
        enabled: false, // Turn the mux off. Omit this line to keep it on.
        ..Default::default()
    })?
    .build()
    .await?;
```

Call `messenger_mux` once per `Velo` instance. A second call returns an error.

## Make sure that the mux carries your streams

1. On the sender, read `StreamSender::negotiated_transport()` after the attach.
2. Compare the result with `velo::streaming::MESSENGER_MUX_KEY`.

```rust
let sender = velo.attach_anchor::<Token>(handle).await?;
match sender.negotiated_transport() {
    Some(key) if key.as_str() == velo::streaming::MESSENGER_MUX_KEY => { /* multiplexed */ }
    Some(_) => { /* one connection per stream */ }
    None => { /* same worker, no transport */ }
}
```

3. On either node, read `velo_streaming_mux_batches_total{direction="sent"}`. The value increases only when the mux carries data.
4. Read `velo_streaming_mux_records_per_batch{direction="sent"}` to see how many records each batch carries.

`negotiated_transport()` exists on the SPSC `StreamSender` only. For an MPSC sender, use the metrics in steps 3 and 4.

If a benchmark must measure the mux, fail each request whose negotiated key is not `MESSENGER_MUX_KEY`. A silent fallback measures the per-stream path under a mux label.

## Select a flush policy

If you do not own the send loop, use the default `FlushPolicy::Auto`. The batcher writes when the peer admits the previous batch.

If you own a loop that produces one record per stream per pass, use `FlushPolicy::Manual`. A decode engine is this kind of loop. Call `flush_batch()` once at the end of each pass.

```rust
let velo = Velo::builder()
    .messenger_mux(MuxConfig {
        flush_policy: FlushPolicy::Manual,
        ..Default::default()
    })?
    // ...
    .build()
    .await?;

loop {
    for (sender, token) in forward_pass() {
        sender.send(token).await?;   // stage
    }
    velo.flush_batch();              // one write per peer
}
```

CAUTION: Under `Manual`, call `flush_batch()` after every pass. Without the call, the last records of each stream stay staged and do not arrive.

`flush_batch()` is synchronous and returns at once. It flushes every peer, because a producer cannot know which peer each sender feeds. It is valid under `Auto` too, where it forces an early write. It does nothing when no mux is installed.

If you want batching with a time limit, use `FlushPolicy::Auto(AutoFlush { on_admission: false, max_linger: Some(window) })`. The batcher then holds a batch for up to `window` after its oldest record.

## Settings reference

All settings are fields of `MuxConfig`. Always build it with `..Default::default()` so that a new field does not break your code.

| Field | Default | What it controls |
|---|---|---|
| `enabled` | `true` | Installs the mux and advertises `messenger-mux-v1`. Setting it to `false` is the rollback. |
| `max_batch_bytes` | 60 KiB | The configured cap on one batch. The eager budget and the 64 KiB coalescing threshold also clamp it. |
| `initial_credit` | 256 | Data credit C per slot. Each slot buffer holds C+1 records. Zero is refused at build time. |
| `slot_byte_budget` | 1 MiB | Bytes one slot can hold in flight, and the cap on its withheld queue. Zero means the default. |
| `peer_byte_budget` | 8 MiB | Bytes all slots of one peer can hold in flight on the receive side. |
| `credit_sweep_interval` | 200 ms | Period of the whole-table credit walk and the batcher eviction check. Zero is refused at build time. |
| `drain_visit_floor` | 2 ms | Shortest gap between two doorbell visits to the same peer. Zero turns the floor off. Values above 1 hour are clamped. |
| `batcher_idle_ttl` | 60 s | How long a batcher with no slots stays alive before eviction. |
| `flush_policy` | `Auto` with `on_admission: true` | When a batcher writes. See [Select a flush policy](#select-a-flush-policy). |
| `async_open_ack` | `false` | Whether a slot open returns before the transport admits its `OpenSlot`. |
| `reply_linger` | 1 ms | How long a credit reply waits for other records to share its batch. Zero writes each reply at once. |

## Change the credit window

`initial_credit` sets how many records a sender can send on one slot before it needs a grant.

- Keep the default of 256 for token streams.
- A stream longer than the window needs grants. The consumer node returns credit as its consumer drains, usually when the next batch from the producer arrives. A live consumer therefore rarely stalls its producer.
- Do not set a small window to save memory. A small window raises the credit-return latency per record. For a producer that ran out of credit, it is `(drain_visit_floor + reply_linger) / initial_credit`.
- Do not set zero. Zero on the wire means "not offering the mux", and the build refuses it.

Each slot buffer holds `initial_credit + 1` records. The byte budgets, not the record count, bound the memory.

## Change the byte budgets

If producers legitimately run far ahead of slow consumers and lose streams to the withheld-overflow kill, increase `slot_byte_budget`. A larger cap gives a producer more run-ahead before the kill.

To fail a wedged stream sooner, decrease `slot_byte_budget`.

If one peer carries many slots with large records, increase `peer_byte_budget`. It bounds only the receive side.

Read [Stream saturation](../operations/saturation.md) before you change either budget.

## Leave the credit timers at their defaults

`credit_sweep_interval`, `drain_visit_floor` and `reply_linger` interact. Keep the defaults unless a measurement shows a reason.

- The arrival path returns most credit on the next inbound batch. The periodic sweep is a backstop. A shorter sweep interval costs CPU in proportion to peers × slots and gains little.
- `drain_visit_floor` limits contention on the per-peer ingress lock. Setting it to zero lets a fast consumer drive back-to-back walks under that lock.
- `reply_linger` lets credit replies share batches. The measurement used a 512-worker rig at 8,192-way concurrency. The 1 ms default cut the frontend's outbound batches by 4.3 to 5.9 times against zero. It also cut the frontend's total write time from 13.5–14.3 s to 2.8–3.9 s per run.

If you set `reply_linger` to zero, each credit reply writes its own batch again. Do this only to compare against the earlier behavior.

## Do not enable async_open_ack without a measurement

`async_open_ack` returns from a slot open before the transport admits the `OpenSlot`. At 512 workers and 8,192-way concurrency, it made first-token p95 worse in every rep and did not improve p50.

It also adds a second way to lose a stream. On a congested peer, a producer can fill the slot byte cap before its own `OpenSlot` is admitted. The slot is then killed with a healthy consumer. Nothing caps how many slots can be open this way against one congested peer.

Do not enable it unless both of these conditions are true:

- The queue depth of a congested peer is bounded.
- You measured the open time that it removes, at your own concurrency.

## Use zero-RTT stream setup

Zero-RTT setup removes the attach round trip from each stream. If the consumer already sends each request to the producer in an envelope that you control, use it.

1. On the consumer, create the anchor.
2. Call `velo.prebind_anchor(handle)` from a runtime context.
3. If the call returns `Some(ticket)`, put the ticket in the request envelope. `StreamOpenTicket` implements `Serialize` and `Deserialize`.
4. If the producer is a different worker and the envelope has a ticket, call `velo.open_anchor_stream::<T>(handle, ticket)` on the producer.
5. If the producer is the worker that called `prebind_anchor`, call `velo.attach_anchor::<T>(handle)`. `open_anchor_stream` on the minting worker fails at once.
6. If the envelope has no ticket, call `velo.attach_anchor::<T>(handle)`.

```rust
// Consumer, at request registration.
let anchor = velo.create_anchor::<Token>();
let ticket = velo.prebind_anchor(anchor.handle());
send_request(RequestEnvelope { handle: anchor.handle(), ticket }).await?;

// Producer, on receipt.
let minted_here = envelope.handle.unpack().0 == velo.instance_id().worker_id();
let sender = match envelope.ticket {
    Some(ticket) if !minted_here => velo.open_anchor_stream::<Token>(envelope.handle, ticket).await?,
    _ => velo.attach_anchor::<Token>(envelope.handle).await?,
};
```

`prebind_anchor` returns a ticket whenever the consumer has the mux and the anchor can be pre-bound, and the mux is on by default. A producer without the mux cannot open that ticket: `open_anchor_stream` fails, and the first `attach_anchor` is refused. The refusal releases the pre-bind, so a retry attaches on the per-stream path. To avoid these failures when you roll the mux out, upgrade the producers before the consumers that mint tickets, or keep the mux off on those consumers until every producer has it.

A zero-RTT sender has no cancel handle, so its `cancellation_token` never fires. When the consumer drops the anchor, the producer's next `send` returns an error. An idle producer also receives a close from the consumer.

The ticket stays valid for the 60-second accept window. After the window, the consumer reaps the bind and sees `SenderDropped`.

## Roll back

1. Set `enabled: false`, or set `VELO_MESSENGER_MUX_DISABLE=1`, on the nodes that mint tickets.
2. Restart those nodes.
3. Do the same on the producers.
4. Restart the producers.

CAUTION: Do not roll back a producer alone while its consumer still mints tickets. The consumer refuses the producer's attach, because the producer no longer offers the pre-bound key.

Rolling the mux out needs the reverse order: producers first, then the consumers that mint tickets. See [Use zero-RTT stream setup](#use-zero-rtt-stream-setup).

Without zero-RTT setup, the order does not matter. Each new attach negotiates the per-stream path.

## Settings that are not levers

These changes were measured and did not help. Do not try them again without a new reason:

- A grant threshold of half a window. It cut credit traffic 19-fold, did not change CPU, and made inter-token p99 worse.
- A 500 µs data linger on the producers (`Auto { max_linger }`). It saved about 1 ms of frontend CPU per request and added about 10 ms to the request path.
- A shorter `credit_sweep_interval` for faster credit. The arrival path already returns credit on the next batch.
- `async_open_ack`, as described above.

[Response plane performance](../operations/response-plane-performance.md) has the measurements.
