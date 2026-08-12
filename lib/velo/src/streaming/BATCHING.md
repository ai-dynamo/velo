<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Batched / multiplexed streaming

This document proposes a transparent optimization to the streaming data plane:
collapse the N TCP connections a producer opens to a peer down to one, and
coalesce the frames destined for that peer into a single wire message.

It is written for two audiences. If you operate a velo deployment, the
[Motivation](#motivation) and [Configuration](#configuration) sections tell you
whether this helps you and how to switch it on. If you are implementing or
reviewing it, the rest is the protocol specification and the argument for why
it is correct.

Status: **P0–P2 implemented; P3–P6 specified.** See
[Implementation status](#implementation-status).

---

## Motivation

A velo stream is a 1:1 relationship. The consumer creates a `StreamAnchor`, the
producer attaches to its `StreamAnchorHandle`, and
[`FrameTransport::connect`](../../../velo-ext/src/streaming.rs) hands back a
channel that owns a dedicated TCP connection. When streams are few and
long-lived — a handful of bulk transfers between workers — this is the right
design. Each stream gets its own socket, its own kernel buffers, and its own
failure domain.

LLM inference violates every one of those assumptions.

A decode engine holds X requests in flight. Each forward pass emits **one token
per request**, and each token is one `StreamSender::send`. Those X anchors do
not live on X different machines: they belong to a much smaller set of Y routers
or frontends. In a typical disaggregated deployment X is 256–1024 and Y is 4–16.

So per forward pass, velo currently issues **X write syscalls producing X TCP
segments**, when Y of each would carry the same payload. And it holds X sockets
open to do it.

### What that costs

Measured from the code, per remote stream:

| Resource | Cost | Source |
|---|---|---|
| Sockets / file descriptors | 1 socket, 2 fds (one per side) | `tcp_transport.rs`, `connect()` |
| Socket buffers | 1 MiB send + 1 MiB receive *requested*; Linux doubles the request for bookkeeping | `configure_socket()` |
| Channel slots | 4096 connect-side + 4096 bind-side | `connect()`, `bind()` |
| Tokio tasks | 4 — heartbeat, egress pump, accept pump, `reader_pump` | `sender.rs`, `tcp_transport.rs`, `control.rs` |
| Bookkeeping | 1 registry entry + 1 spawned 60 s expiry timer per bind | `bind()` |
| Setup latency | 1 active-message round trip + 1 TCP dial round trip | `attach_remote()`, `connect()` |

Per token, per stream: one `rmp_serde` allocation, one channel hop, one
terminal-sentinel check, one `encode_frame` (an 11-byte preamble, coalesced into
a single `write_all`), and — because `TCP_NODELAY` is set — **one syscall and one
TCP segment**.

The important consequence is not that this is slow. It is that **it is a
ceiling, not a slope**:

> 1024 concurrent remote streams = 2048 file descriptors, roughly 2 GiB of
> requested socket buffer, and about 4096 tokio tasks. The default `ulimit -n`
> is 1024, so a process wedges at roughly **512 concurrent remote streams**
> regardless of how fast the hardware is.

Velo already knows about this. `SATURATION.md` mitigation #4 tells operators to
"reduce the number of concurrent anchors — if your application creates one
anchor per work item, batch work items into one anchor." The soak harness
documents (`examples/.../tier.rs`) that above ~256 anchors the many-anchors
scenario flakes: the producer outruns the 256-deep anchor channel, TCP
zero-windows, and the 15-second heartbeat watchdog starts killing healthy
sessions.

This proposal moves that batching job out of the application and into the
transport.

### Wire efficiency

A single decoded token serialized as `StreamFrame::Item(String)` is on the order
of 10–40 bytes. Today each one travels alone:

```
11 B velo preamble + ~40 B payload + ~66 B Ethernet/IP/TCP  →  ~13-35% efficiency
```

Batched, with 32 tokens sharing one frame to one destination:

```
11 B preamble + 8 B batch header + 32 × (9 B record + ~40 B)  =  1 segment
```

Roughly 4× better wire efficiency, and — the part that actually matters — **1
syscall and 1 segment instead of 32**.

---

## Measured results

The validation methodology below calls the batching ratio
(`frames_written / egress_flushes`) *"the cheapest and most decisive
experiment."* It has been run. Both numbers come from
`lib/velo/tests/streaming/tcp_batching.rs` on TCP loopback, and both are
reproducible with `cargo test --all-features --test streaming_tcp_batching --
--nocapture`.

| Workload | Frames | Egress flushes | Ratio |
|---|---|---|---|
| One stream, 20 000 frames back-to-back | 20 001 | 21 | **952 : 1** |
| 32 streams × 1 frame per pass (the LLM shape) | 3 232 | 3 232 | **1.00 : 1** |

Read those together, because the pair is the whole argument:

- **Write coalescing (P2) is enormously effective for bursty streams.** A
  producer that runs ahead pays one `write_all` per ~950 frames instead of one
  per frame. This required no wire change, no negotiation and no configuration.
  (The counter measures flushes, not syscalls — a large batch may cost more
  than one underlying write, so treat ~950:1 as the batching factor, not as a
  syscall-reduction factor.)
- **Write coalescing does nothing at all for the workload that motivated this
  document.** A forward pass places one frame on each of X *different* streams.
  Per-stream coalescing can only pack frames queued on the same stream, so each
  egress pump wakes with exactly one frame and the ratio is exactly 1.00 — one
  write per token, unchanged.

That second row is the case for multiplexing, measured rather than asserted.
The frames are there to be batched; they are simply on the wrong axis. Bucketing
by *destination worker* instead of by *stream* is the only thing that can reach
them, and at a typical X/Y of 32 it has the same 32× of syscalls and segments
available to it that the first row shows is achievable.

It is also a caution against declaring victory on P2. The ratio being excellent
in a benchmark that sends on one stream says nothing about production, which is
why `forward_pass_shape_does_not_coalesce_per_stream` asserts the *limitation*
and will fail loudly if anyone later concludes coalescing was sufficient.

## Design overview

Four mechanisms. Only the first is a prerequisite for the others.

1. **Multiplexing** — one persistent connection per destination worker, carrying
   many streams as *slots*.
2. **Batching** — many records packed into one wire frame and one `write_all`.
3. **Flow control** — per-slot credit, so one slow consumer cannot stall the
   shared connection.
4. **Flush policy** — when to write. Three policies, one mechanism.

Senders are oblivious to all of it. `StreamSender::send` enqueues exactly as it
does today; only the layer beneath it changes.

### Why bucketing by destination is free

`StreamAnchorHandle` packs a `WorkerId` into its upper 64 bits, and `WorkerId`
is a deterministic 1:1 xxh3 hash of `InstanceId`. So the destination of a send
is recoverable from the handle with a shift — no map lookup, no discovery hop,
no allocation. `handle.unpack().0` *is* the batching key.

---

## Protocol

### Connections

One persistent, **bidirectional** connection per `(local worker → peer WorkerId,
conn_index)`, dialed lazily and shared by every stream that hashes to it.

`connections_per_peer` defaults to 2. The reason is not primarily blast radius:
one TCP connection is one ordered byte stream, which means one receive-side
queue and one writer task, which caps encode throughput at a single core. A
small fan lets multiple cores participate. Slots map to connections by hashing
`(anchor_id, session_id)`, so a stream is pinned to one connection for its entire
life and the `FrameTransport` ordering contract holds trivially.

> **A stream's frames are never striped across connections.** Ordering is
> load-bearing for the streaming protocol's correctness, and TCP only orders
> within a connection.

Today's streaming connection is effectively unidirectional — the connector never
reads and the acceptor never writes. Credit returns require both, so a mux
connection runs 4 tasks instead of 2. That is O(Y), not O(X).

It also requires a **connection-level handshake**, because the acceptor
currently never learns the dialer's `WorkerId` and credit routing needs it. A
reserved magic value in the first 8 bytes lets one listener serve both the
legacy `(anchor_id, session_id)` handshake and the mux handshake:
`0xFFFF_FFFF_FFFF_FFFF` is unreachable as a real `anchor_id` short of 2^63 MPSC
anchors.

### Frame envelope

A batch nests **inside** the existing `TcpFrameCodec` frame. The codec, its
decoder — which already handles many frames per read buffer — and the 16 MiB
frame cap are all reused unchanged. The streaming transport passes an empty
header today, so the header slot is free:

```
TcpFrameCodec frame:
  [11 B preamble][8 B batch header][payload]

batch header:
  [u8 mux_version = 1][u8 flags][u16 record_count][u32 reserved]

payload = record_count × record:
  [u8 record_type][u32 BE slot][u32 BE len][len bytes body]

record_type: 0 = Data, 1 = OpenSlot, 2 = CloseSlot,
             3 = CreditUpdate, 4 = ConnHeartbeat
```

9 bytes per record. On a 40-byte token that is ~22% overhead; measure before
optimizing to a 7-byte layout (`u16` slot, 65 536 slots per connection) or
varint lengths.

Record bodies:

- **`Data`** carries the existing `rmp_serde`-encoded `StreamFrame` bytes,
  byte-for-byte identical to today. This matters more than it looks:
  `is_terminal_sentinel()` works unchanged on record bodies, so there is no new
  terminal-detection code and no new place for terminal handling to diverge.
- **`OpenSlot`** = `[u64 anchor_id][u64 session_id]` — literally the existing
  16-byte handshake relocated into a record, feeding the same
  `(anchor_id, session_id)` registry lookup.
- **`CloseSlot`** = `[u8 reason]` — `0` terminal-sent, `1` peer-gone, `2`
  unknown-slot, `3` protocol-error. **Bidirectional**: the receiver must be able
  to reject an unknown slot without killing the connection.
- **`CreditUpdate`** = `[u32 delta]`, receiver to sender.
- **`ConnHeartbeat`** carries no body.

#### The batch size cap

> **The default batch byte cap is `COALESCE_THRESHOLD` (64 KiB) minus the batch
> header and slack — about 60 KiB.**

This is the single most important tuning constant in the design, and it comes
straight out of code that already exists. `encode_frame` emits **one**
`write_all` if and only if `header + payload <= COALESCE_THRESHOLD`, and three
segmented writes otherwise. Since amortizing the syscall is the entire point of
batching, exceeding that threshold defeats the optimization.

Any single record larger than the cap is sent in a batch of its own. Oversized
batches must be prevented on encode: the decoder's length validation rejects
frames over 16 MiB with an `InvalidData` error, and under mux that failure mode
kills the connection — which means *every* slot on it.

### Slots

```
SlotId = (u24 index, u8 generation)   packed into a u32
```

A dense index means demux is a `Vec` lookup rather than a hash. At 60 KiB
batches this is roughly 1500 records per frame, so the per-record lookup is the
hot path.

> **Generations are a correctness requirement, not an optimization.**

Dense slot reuse without a generation tag means a stale record for a recycled
slot is delivered to whatever stream now occupies that index — request A's
tokens surfacing inside request B's response. Silent, cross-request, and
invisible to every existing test. Records whose generation does not match the
current occupant are dropped and metered. A `u8` is ample: TCP does not reorder,
so a stale record's in-flight window is a single connection traversal.

#### `OpenSlot` is eager

`OpenSlot` is sent when the stream attaches, in its own gate-overriding flush —
not lazily piggybacked onto the first data record.

The reason is a subtle interaction with `bind()`, which starts a 60-second
`ACCEPT_TIMEOUT` measuring *"time until a connection bearing this handshake
arrives."* If `OpenSlot` were lazy, that timer would silently come to mean *"time
until the producer produces its first token"* — and for a queued request with a
long prefill, it would expire and drop a perfectly healthy stream. Eager
`OpenSlot` preserves today's timing semantics exactly. It costs a record, not a
syscall, and concurrent attaches coalesce into the same batch anyway.

The reverse race — an `OpenSlot` for an `(anchor_id, session_id)` that was never
registered — must **not** kill the connection. Today the equivalent situation
just drops a socket. Under mux the receiver replies `CloseSlot{unknown}` and
discards that slot's records.

#### Connection loss

Every live slot that has not seen a terminal receives an injected
`StreamFrame::Dropped` — **not** `TransportError`. This reproduces the existing
behaviour of `pump_frames` and keeps the consumer-visible contract
(`StreamError::SenderDropped`) byte-identical. `TransportError` stays reserved
for protocol violations.

### Flow control

This is the part that makes multiplexing safe, and it is worth being explicit
about why it cannot be skipped.

One socket per peer means **one slow consumer can stall every stream sharing
that socket**. Today's receive path shows exactly how it would happen:
`pump_frames` falls through to an awaited `frame_tx.send_async(...)` when a
consumer's channel is full. Under mux that awaits inside the *shared* connection
reader, so a single saturated anchor blocks delivery for all of them — and then
every anchor's heartbeat watchdog fires at once. For inference that means one
stuck HTTP client throttling the GPU.

The fix is HTTP/2-style per-slot credit, with one critical difference from the
obvious design.

> **Credit is issued against a mux-owned per-slot buffer, never against the
> anchor's `frame_tx`.**

This is load-bearing. `frame_tx` has writers other than the mux: the local
same-worker attach path, the detach and finalize handlers, `reader_pump`'s own
watchdog injection, and — decisively — **M concurrent MPSC senders**. Any
"C credits against a C-deep channel" proof collapses the moment a second writer
exists. So `bind_muxed` returns a receiver of depth `C + 1` and **`reader_pump`
is otherwise unchanged**, draining it into `frame_tx` exactly as it does today.
The `SATURATION.md` cascade gains one link; the 256-deep anchor channel is still
the smallest and still fills first.

**Initial credit is negotiated, not constant.** MPSC anchor capacity is
caller-configurable, so the receiver advertises what it can absorb. Two fields
are added to `AnchorAttachResponse::Ok`, both `#[serde(default)]` so older
senders still deserialize:

```rust
#[serde(default)] initial_credit: u32,    // 0 = legacy peer, mux unusable
#[serde(default)] slot_byte_budget: u32,  // 0 = use default
```

**Credit is returned by `reader_pump`.** It gains an `Option<CreditReturn>` and
calls `credit.release(1)` after each successful handoff to `frame_tx` — exact,
O(1), and immediate. (flume has no consumed-callback; a per-slot drain task
would reintroduce the per-stream tasks we are removing, and polling the
receiver's length is only a sampled approximation.) A background sweep reclaims
credit for slots whose pump died.

**Reserved terminal credit.** Each slot holds back one credit spendable only by
a record matching `is_terminal_sentinel`; data may spend only `C`; the slot
buffer is sized `C + 1`. One reserved credit is provably sufficient because
`sent_terminal` guarantees at most one terminal per slot, after which the slot
closes. `SenderError` is deliberately *not* terminal and correctly spends data
credit — the stream continues after it.

**Byte credit is also required.** Frame-count credit alone bounds memory at
`slots × C × DEFAULT_MAX_FRAME_SIZE`, which is 256 × 256 × 16 MiB ≈ 1 TiB — a
meaningless bound. Today the kernel socket enforces a real ~1 MiB-per-stream
limit for free, and multiplexing deletes exactly that protection. So: a
per-connection byte budget (default 8 MiB) and a per-slot byte cap (default
1 MiB). Frame credit provides the no-head-of-line-blocking proof; byte credit
provides the memory bound. They are different jobs and both are needed.

**Control and data travel in separate lanes.** `finalize`, `detach` and `Drop`
use a *synchronous* channel send, which must stay non-blocking under mux. The
egress inlet is therefore split into an unbounded **control lane** (`OpenSlot`,
`CloseSlot`, `CreditUpdate`, terminals) and a bounded **data lane**, drained
control-first. Unbounded is safe because control volume is O(live slots), which
credit already bounds. This incidentally fixes a latent hazard that exists
*today*: `Drop`'s synchronous send on a full 4096-deep channel blocks a runtime
worker thread from inside a `Drop` in async context.

Credit returns get their own priority lane, so a peer whose egress is congested
never stops returning your credit.

> **Invariant.** A slot never has more than `C` frames outstanding against a
> `C + 1`-deep buffer, so the demux `try_send` always succeeds and **the
> connection reader never blocks**. `velo_streaming_mux_reader_stall_total > 0`
> is a bug, not a tuning signal.

### Terminal sentinels

Today the egress pump writes a terminal, breaks out of its loop, discards
anything queued behind it, and closes the socket. That discard is deliberate —
sending a frame queued behind a terminal races the consumer's cleanup and
produces spurious connection resets. Under mux this becomes per-slot:

1. Egress sees `is_terminal_sentinel(body)` for slot S, appends it to the
   current batch, marks S draining, and drops S's inlet receiver. Frames queued
   behind the terminal **for S** are discarded — today's semantics, correctly
   scoped. Other slots are untouched, which is strictly better than today.
2. `CloseSlot{TerminalSent}` is appended **in the same batch**, immediately
   after, so terminal-then-close is atomic.
3. The connection stays up. The slot is freed and its generation bumped; its
   credit and byte budget are released.
4. On the receive side the terminal is forwarded using the reserved credit, then
   `CloseSlot` drops the mux-side sender, so `reader_pump` exits through the same
   `Err` branch it uses today when a socket closes. **Identical code path.**
5. The existing "last frame was not terminal and the consumer is still attached,
   so inject `Dropped`" rule becomes per-slot, firing on
   `CloseSlot{reason != TerminalSent}` and on connection death.

A `velo_streaming_mux_live_slots` gauge must return to zero in every teardown
test. A leaked slot now leaks credit and byte budget for the life of the
connection, whereas a leaked socket today is at least visible in `lsof`.

### Heartbeats

A correction to a natural assumption first: **the per-stream heartbeat does not
detect a hung producer today.** It runs in a separate spawned task, so a wedged
producer loop keeps it ticking happily. What it actually detects is process or
host death, connection death, and sustained saturation — the last because
`try_send` silently drops heartbeats when the channel is full, converting
saturation into a watchdog kill. That is the saturation-kill path `SATURATION.md`
documents and half-apologizes for.

That makes connection-level liveness look attractive, and it is a trap: it
gains nothing over the design below and loses the per-slot saturation signal.

**Per-slot heartbeats, phase-aligned and suppressed**, get identical wire cost
with **zero** semantic change:

- A per-slot "last send tick" (`AtomicU64`, one relaxed store per frame on the
  send path). A slot that sent anything this interval skips its heartbeat —
  **suppression**.
- Each sender subscribes to a connection-level tick (`tokio::sync::watch` bumped
  by one timer task) instead of owning a `tokio::time::interval` — **phase
  alignment**. This is what makes heartbeats coalesce; today they are randomly
  phased across the interval and batching cannot merge them.
- For muxed senders, no per-sender task is spawned at all: the connection's
  heartbeat task iterates live slots and emits for the idle ones.

Result: N scattered frames per interval become at most one batch, N tasks become
one, and the receiver still sees a *per-slot* heartbeat inside its per-slot
deadline — so `reader_pump` and `DETECTION_MULTIPLIER` are untouched.

---

## Flush policy

Senders enqueue; the per-connection egress task decides when to write.

| Policy | Trigger | Added latency | Default |
|---|---|---|---|
| **Opportunistic** | blocking `recv`, then drain what is already queued | **none** | **on** |
| **Windowed** | wait up to `flush_window` after the first record | ≤ window | off |
| **Hinted** | an open `StreamBatch` gate defers flushing | ≤ hold time | off |

Opportunistic batching is the default precisely because it cannot make anything
worse: the egress task never waits for work that has not arrived. It simply
notices that more work is *already* queued and takes all of it. Under load,
batches form; under no load, behaviour is identical to today minus one syscall's
worth of bookkeeping.

These flush reasons override an open hint gate: byte cap, record cap, credit or
byte-budget starvation, `OpenSlot`, and the hold watchdog. This is what makes
the hint genuinely advisory — a deployment that ignores hints entirely is a
supported configuration, and a forgotten `close_batch()` degrades to the
windowed policy rather than stalling.

### The hint API

```rust
let batch = node.stream_batch();          // == start_batch()
for (sender, token) in outputs {
    sender.send(token).await?;
}
batch.flush().await?;                     // == close_batch()
// Drop schedules a flush if flush() was never called.
```

The guard is RAII because an early `?`-return must not strand a batch.
`start_batch()` / `close_batch()` exist as thin wrappers for callers who prefer
that shape. Nesting is refcounted.

#### Deadlock, and the two rules that prevent it

The hint gate introduces a genuine deadlock: a producer opens a gate and sends
`C + 1` frames to one slot. Frames 1..C sit unflushed; frame `C + 1` blocks
waiting for credit; credit requires a flush; the flush is gated. Two hard rules:

- **F1.** Before a slot inlet may *block* on credit or byte budget, it must call
  `egress.request_flush(FlushReason::Starved)`, and the egress task treats
  `Starved` as gate-overriding. Concretely: `try_acquire()`, then on failure
  `request_flush(...)`, and only *then* await.
- **F2.** A gate watchdog (default 5 ms, configurable) force-opens the gate
  regardless. Belt and braces for a bug in F1.

#### Is the hint API actually needed?

The measurement above narrows this considerably, and the answer depends on
something worth stating precisely.

A forward pass issues its X sends back-to-back with no `.await` between them, so
under multiplexing they land in one shared egress queue and the opportunistic
drain should see all of them — capturing most of the win with no hint at all.
That is the same effect the 952:1 burst row demonstrates, just with the queue
shared across streams rather than owned by one.

Where it breaks down is a producer that **does** await between sends — awaiting
a per-request tokenizer, a sampling callback, or anything that yields. Then each
send arrives at the egress task alone, the drain finds nothing queued behind it,
and the ratio collapses back toward 1.0 exactly as the forward-pass row shows.
An explicit barrier is the only thing that can group sends the runtime has
already scheduled apart.

So the hint is not the primary mechanism, and it should not be enabled by
default; but it is the difference between multiplexing working and multiplexing
doing nothing for a producer whose send loop yields. Measure the ratio under
mux first — if it is already high, the hint is insurance rather than a
requirement.

---

## Backward compatibility

No new negotiation mechanism is needed; the existing one is a good fit.
`AnchorAttachResponse::Ok` already carries `streaming_transport_key`, and the
sender already resolves it against its transport registry. Two changes:

1. `AnchorAttachRequest` gains
   `#[serde(default)] supported_transport_keys: Vec<TransportKey>`. Additive,
   internal to `velo`, and `serde(default)` means older senders still
   deserialize.
2. The attach handler currently hardcodes the local default transport's key. It
   instead intersects the sender's advertised keys with its own installed
   transports, preferring the mux key.

A node with mux installed registers **both** keys, so it still serves legacy
peers. This step is required rather than optional because `resolve_transport`
hard-errors on an unknown key in a non-empty registry — a receiver that
unilaterally answered `mux/tcp-stream` would break every older sender.

The result: a mux-capable pair negotiates mux, and every mixed pair silently
falls back to the per-stream path.

---

## Configuration

```rust
let node = Velo::builder()
    .add_transport(transport)
    .stream_config(StreamConfig::Tcp(Some(TcpConfig {
        bind_addr,
        batching: BatchConfig {
            mode: BatchMode::Multiplexed,        // default: PerStream
            flush: FlushPolicy::Opportunistic,   // default
            connections_per_peer: 2,
            max_batch_bytes: 60 * 1024,
            ..Default::default()
        },
    })))?
    .build()
    .await?;
```

Defaults are chosen so that switching `mode` on is the only decision an operator
has to make, and so that the transparent path never trades latency for
throughput without being asked.

---

## Observability

New series, alongside the existing `velo_streaming_*` collectors:

| Metric | Meaning |
|---|---|
| `velo_streaming_egress_flushes_total` | Batches flushed by the per-stream egress pump (one completed `write_all` each) |
| `velo_streaming_frames_written_total` | Logical frames written |
| `velo_streaming_connections_open` | Gauge of open streaming connections |
| `velo_streaming_batch_records_per_flush` | Histogram of records per batch |
| `velo_streaming_batch_bytes_per_flush` | Histogram of bytes per batch |
| `velo_streaming_batch_flush_total{reason}` | Mux-era flush reasons: `opportunistic\|window\|hint\|cap\|starved\|watchdog\|terminal`. Distinct from `egress_flushes_total`, which counts per-stream pump flushes and ships today |
| `velo_streaming_mux_live_slots` | Gauge; must return to zero at teardown |
| `velo_streaming_mux_reader_stall_total` | **Should always be zero.** Non-zero means the credit invariant is broken |
| `velo_streaming_mux_generation_mismatch_total` | Stale records dropped by the generation check |
| `velo_streaming_slot_credit_exhausted_total` | Per-slot credit starvation events |

> **`frames_written / egress_flushes` is the batching ratio** and the single
> number that says whether this is working. It is meaningful at any scale, which
> is why it ships before the protocol does. It counts flushes rather than
> syscalls -- `write_all` may loop internally and TCP segmentation is the
> kernel's call -- so read it as "how much the pump batched", not "how many
> syscalls were saved".

### A meaning change operators must know about

`velo_streaming_producer_send_backpressure_total` currently means *"the
4096-deep connect-side channel was full."* Under mux it means *"this slot ran
out of credit."* Arguably a more useful signal, but dashboards built on the old
meaning will shift under them. `SATURATION.md` is updated in the same change.

---

## Validation methodology

At two nodes and ten streams on loopback, the batched path will be the same or
marginally slower. The win is proportional to X/Y and only appears at scale, so
the methodology has to earn the claim rather than assert it. Five independent
lines of evidence, ordered by how few assumptions each requires.

### V1 — Resource-ceiling arithmetic

Pure accounting from the table in [Motivation](#what-that-costs), checked in as
a test that opens N streams and counts `/proc/self/fd`. This identifies a **hard
wall** — roughly 512 concurrent remote streams at default `ulimit -n` — not a
slope. It cannot be falsified by a small-scale null result, which is exactly why
it leads.

### V2 — Analytical cost model

With `s` = per-`write_all` syscall and TCP cost, `e` = encode, `f` = receive-side
decode and handoff, `w` = channel-handoff wake, `p` = per-record framing:

```
today:  X · (e + s + w_connect + w_recv + f)
mux:    X · (e + f + p) + Y · (s + w)
```

Three falsifiable predictions:

- **Syscalls and memory** improve unconditionally for X/Y > 1.
- **CPU** crosses over at roughly `(s + w) / p`. With `s ≈ 1–3 µs` and
  `p ≈ 20 ns`, mux is CPU-cheaper for any **X/Y > 2**, and saves >20% of
  streaming CPU once **X/Y > 8**.
- **Latency gets marginally worse** — one extra channel hop, on the order of
  1–5 µs. This is the term that can regress, and it must be reported next to
  every throughput number.

### V3 — Microbenchmarks

Criterion, `harness = false`. The targets measure the model's constants
directly: `encode_frame` for one payload versus one batch of N (measures `e`,
`p`, and the 64 KiB coalesce cliff); decoding N concatenated frames versus N
separate calls (`f`); a channel round trip (`w`, which calibrates whether
removing the forwarder hop is worth doing at all); and loopback N frames with
one write each versus batched (`s` — the money number, and it needs no
distributed setup).

### V4 — Deterministic simulation

`SimFabric` provides virtual time and a bisection-bandwidth model that charges
`base_latency` **per transfer**, so it captures the packing win directly and
deterministically. Using it for streaming requires a `SimFrameTransport` and a
`StreamConfig::Sim`, since today's `SimTransport` implements only the messenger
`Transport` trait.

**Be honest about what it measures.** It models the network term, and models it
*optimistically for mux*, since per-transfer latency is precisely what batching
eliminates. It models none of: syscalls, task scheduling, socket memory, or fd
exhaustion — which is where most of the real win lives. Net: it **understates
the total win while overstating the network component.**

Its higher value is as a **deterministic correctness harness** for credit
accounting, slot generation/ABA, and connection-loss injection under adversarial
interleavings — none of which a loopback test reproduces reliably.

### V5 — Loopback scale sweep

Sweep X ∈ {1, 8, 64, 256, 1024, 4096} against Y ∈ {1, 2, 8, 32} at ~64-byte
frames, A/B-ing the same binary between per-stream and mux. Record wall time,
CPU, peak RSS, peak fd count, the batching ratio, and **p50/p99 per-frame
latency**.

Loopback removes wire time, which exaggerates the syscall term — that cuts *for*
mux, so it should be noted and the sweep repeated across two hosts where
possible. Add a 2-core pinned variant: the task and syscall win is far more
visible under CPU scarcity, and **inference servers are CPU-scarce** — the
threads feeding the GPU want those cores.

### V6 — Falsification criteria

State up front what would sink this, then check each:

- **The batching ratio does not improve at X/Y = 64.** Sends are not temporally
  clustered, the premise is wrong, and the work should stop after P2. This is
  the cheapest and most decisive experiment, which is why P0 ships it first.
- **p99 per-frame latency regresses more than 2× at X/Y = 1** → negotiation must
  default off.
- **CPU is not reduced at X/Y = 64** → encode dominates, not syscalls, and the
  fix is a cheaper codec rather than multiplexing.
- **Measurable head-of-line blocking** under a mixed fast/slow consumer workload
  → the credit design failed.
- **`heartbeat_watchdog_firings_total` rises after P6** → heartbeat
  consolidation lost real failure detection.
- **The trap:** throughput improving *because* added latency let the consumer
  catch up. Always report latency beside throughput, and watch **time-to-first-token**
  specifically — any windowed or hinted policy can regress it. That is why
  opportunistic is the default.

---

## Implementation status

| Phase | Scope | Wire change | Status |
|---|---|---|---|
| **P0** | Batching-ratio counters, resource-ceiling test, benches | none | implemented |
| **P1** | Messenger TCP + UDS writer coalescing | none | implemented |
| **P2** | Streaming per-socket coalescing | none | implemented |
| **P3** | gRPC `Channel` caching per peer | none | specified |
| **P4** | Mux protocol, `FrameSink`, negotiation, MPSC | yes, negotiated | specified |
| **P5** | Hint API (`StreamBatch`), windowed policy | none | specified |
| **P6** | Heartbeat suppression and phase alignment | none | specified |
| **P7** | Lift the mux surface into `velo-ext` | none | specified |

P1 and P2 are wire-compatible in both directions and require no negotiation —
the frame decoder already accepts many frames per read buffer, so a coalescing
writer talks to an unmodified reader. They deliver the syscall amortization,
which the cost model predicts is the largest single term, without any protocol
risk.

### Why the mux surface is not in `velo-ext` yet

The obvious move is to add `open_mux` / `MuxChannel` / `set_mux_acceptor` to
`FrameTransport` so out-of-tree transports can participate. It is deferred for
three reasons:

1. **A bare channel pair cannot express connection death.** The mux layer must
   distinguish abnormal death from clean close in order to inject `Dropped`
   correctly — information the current code gets from whether the framed read
   returned `Err` or `None`, and which a `flume::Receiver` destroys.
2. **Coupled defaulted methods are a silent-failure shape.** velo-ext's rules
   require default implementations, which would permit a transport reporting
   `supports_mux() == true` while leaving the acceptor a no-op — senders opening
   muxes into a void. If it lands, it should be *one* method
   `as_mux() -> Option<&dyn MuxTransport>` returning a separate trait with no
   defaults.
3. **The blob-pipe abstraction may be permanently wrong.** An RDMA transport
   already multiplexes natively per queue pair; forcing it to serialize into a
   byte stream so velo can chop it back up is a pessimization. Removing a
   published velo-ext item is a major bump, so shipping the wrong shape is
   expensive.

Until then the implementation uses inherent methods on the concrete transport —
the pattern already established by `set_metrics`, which is deliberately kept off
the `FrameTransport` trait so out-of-tree implementors do not inherit a
`prometheus` dependency. Adding gRPC support is an enum variant, not a trait
change.

The cost of deferring is bounded: out-of-tree transports get no multiplexing for
a release or two, and nothing else breaks. Because the receiver never offers
`mux/...` for a transport that cannot do it, a mux-less transport degrades
correctly by construction.
