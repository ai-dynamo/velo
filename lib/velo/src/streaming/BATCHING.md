<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Batched / multiplexed streaming

This document proposes a transparent optimization to the streaming data plane:
stop opening a TCP connection per stream at all, carry every stream to a peer
over the connectivity the Messenger already maintains to it, and coalesce the
frames destined for that peer into a single active message.

It is written for two audiences. If you operate a velo deployment, the
[Motivation](#motivation) and [Configuration](#configuration) sections tell you
whether this helps you and what switching it on will look like once the mux
phases land. If you are implementing or reviewing it, the rest is the protocol
specification and the argument for why it is correct.

Status: **P0–P3 implemented; P4–P11 specified.** See
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

Batched, with 32 tokens sharing one `_stream_batch` active message to one
destination:

```
16 B batch header + 32 × (13 B record + ~40 B)  =  one AM payload
```

The outer framing — preamble, syscalls, TCP segmentation — belongs to whatever
messenger transport carries the AM, and the coalescing writer already packs
consecutive AMs into one `write_all`. Roughly 4× better wire efficiency, and —
the part that actually matters — **one write instead of 32**.

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
  producer that runs ahead hands the socket one batch per ~950 frames instead
  of one per frame. This required no wire change, no negotiation and no
  configuration. (The counter measures batches, not syscalls — a large batch
  may cost more than one underlying write — so treat ~950:1 as the batching
  factor, not as a syscall-reduction factor.)
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

1. **Multiplexing** — every stream to a peer becomes a *slot* on the Messenger
   connectivity that already exists to it. Streaming owns no sockets.
2. **Batching** — many records packed into one active message and one `write_all`.
3. **Flow control** — per-slot credit, so one slow consumer cannot stall the
   peer's shared ordering lane.
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

### Riding the Messenger

The mux is a `MessengerMuxTransport` implementing the streaming `FrameTransport`
contract, and it **replaces** the deprecated `VeloFrameTransport` rather than
sitting beside it. There is no dial, listener, acceptor or connection manager:
records for a peer are packed into `_stream_batch` active messages and handed to
the Messenger, which already holds connectivity to every peer this node talks to
and already knows who sent what.

That one decision deletes most of the protocol this section used to specify. No
`0xFFFF_FFFF_FFFF_FFFF` handshake magic — the sender's identity arrives in the AM
envelope, so credit has somewhere to be routed without a handshake invented to
learn it. No `connections_per_peer` fan; encode work spreads across one batcher
per peer, which is the throughput argument the fan used to make. No 4-task
connection lifecycle (heartbeat, egress pump, accept pump, reader), and no
acceptor-identity problem, because there is no acceptor.

Egress is **one lazy, cached, evictable `PeerBatcher` per remote instance**,
shared by every stream to that instance, created on first send and evicted when
idle. A node talking to Y peers holds Y batchers however many streams it holds —
O(Y) where the per-stream design is O(X). Its key is the batching key from [Why
bucketing by destination is free](#why-bucketing-by-destination-is-free)
unchanged: `WorkerId` and `InstanceId` are in 1:1 correspondence, so "destination
worker" and "remote instance" name the same bucket.

> **The cost is that streaming no longer owns its wire.** It shares queues,
> framing and backpressure with control traffic, and inherits whatever ordering
> its transport gives it. Ordering stops being a TCP guarantee and becomes an
> explicit protocol obligation — hence a per-slot sequence on every record.

### Frame envelope

A batch is the payload of one `_stream_batch` active message. There is no velo
preamble to reuse and no codec to nest inside — the Messenger frames the AM as it
frames anything else — so the batch carries its own header:

```
_stream_batch AM payload:
  [16 B batch header][payload]

batch header:
  [u8 mux_version = 1][u8 flags][u16 record_count]
  [u64 peer_epoch][u32 batch_seq]

payload = record_count × record:
  [u8 record_type][u32 BE slot][u32 BE frame_seq][u32 BE len][len bytes body]

record_type: 0 = Data, 1 = OpenSlot, 2 = CloseSlot,
             3 = CreditUpdate, 4 = SlotHeartbeat
```

The epoch is bumped whenever the sender's view of the peer is re-established;
`batch_seq` advances within it and is compared modulo, since an epoch outlives a
`u32` on a busy pair. Together they let ingress discard a stale epoch's batches
by header inspection rather than by draining them, and meter gaps. `frame_seq`
is **per slot** — where `u32` is unreachable — and is the authority on stream
order: transport and lane ordering are a fast path, not the proof.

13 bytes per record: ~33% overhead on a 40-byte token, up from the 9-byte layout
a dedicated connection could afford. The sequences are what buy ordering now that
a private TCP connection is not providing it free. Measure before shrinking them.

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
  to reject an unknown slot without failing the peer.
- **`CreditUpdate`** = `[u32 delta]`, receiver to sender.
- **`SlotHeartbeat`** carries no body.

#### The batch size cap

> **A batch is clamped by three numbers — the configured cap, the effective
> eager budget, and `COALESCE_THRESHOLD` (64 KiB) — whichever binds first.**

The packing *target* is still `COALESCE_THRESHOLD`, for exactly the reason it
always was: the shared coalescing writer stages a frame into one buffered
`write_all` if and only if `header + payload <= COALESCE_THRESHOLD`, and writes
it segmented otherwise. Amortizing the syscall is the entire point, so a batch
that exceeds the threshold gives back what batching bought.

What changed is the ceiling above it. There is no longer a 16 MiB decoder limit
whose breach kills a connection and every slot on it. In its place is the
**effective eager budget** — `min(Transport::max_message_size(target) where
known, rendezvous staging threshold)` less encoded-envelope overhead, the largest
payload the Messenger will carry inline to this peer. A batch over that budget
does not fail loudly; it quietly becomes a rendezvous transfer, paying a round
trip on behalf of every slot packed into it. The cap now exists to keep batches
*eager*, not to avoid a fatal decode.

A single record larger than the eager budget is **deliberately routed** through
rendezvous, alone in its batch. Deliberately, because the transport limit can bind
below the staging threshold, in which case an oversized send exceeds the transport
without ever tripping the stager. Unrelated slots keep flowing in eager batches
while it is in flight; the ordering consequence is dealt with in [Slots](#slots).

### Slots

```
SlotId = (u24 index, u8 generation)   packed into a u32,
         scoped by the sender's peer epoch
```

A dense index means demux is a `Vec` lookup rather than a hash. At 60 KiB
batches this is roughly 1100 records per batch, so the per-record lookup is the
hot path.

> **Generations are a correctness requirement, not an optimization.**

Dense slot reuse without a generation tag means a stale record for a recycled
slot is delivered to whatever stream now occupies that index — request A's
tokens surfacing inside request B's response. Silent, cross-request, and
invisible to every existing test. Records whose generation does not match the
current occupant are dropped and metered.

The epoch scopes the whole table above the generation: a generation survives slot
reuse within one sender's lifetime, the epoch survives the sender itself. Batches
in flight across a reconnect are therefore discarded wholesale rather than checked
record by record against state that has moved on — and a `u8` generation stays
ample once the epoch carries that job.

#### Ordering is per-slot, not per-connection

`_stream_batch` is registered with **ordered per-sender dispatch**: batches from
one peer are handled on that peer's lane, by one task, in arrival order, so the
general reordering problem does not arise and needs no window to solve it. That
is precisely what the deprecated `VeloFrameTransport` lacked — it layered a
4096-deep reorder buffer over a dispatcher that spawns a task per inbound
message, and under cross-stream contention the window overflowed and deadlocked
the consumer.

One narrow exception survives, and it is self-inflicted. Rendezvous payloads
resolve in a detached task *before* dispatch, so an oversized record routed that
way is not ordered against the eager batches around it — the ordered dispatcher
says so itself, and warns once per handler. Two mechanisms bound it. Egress
fences the slot: at most one rendezvous record per slot is outstanding, and the
batcher withholds that slot's later records until the staged send is admitted.
Ingress holds records arriving ahead of `frame_seq` in that slot's own buffer,
bounded by credit already granted, applying them when the gap closes. Overflow
closes **that slot** with `Dropped` and meters it; other slots are untouched and
the lane never blocks.

#### `OpenSlot` is eager

`OpenSlot` is sent when the stream attaches, in its own gate-overriding flush,
not piggybacked onto the first data record. `bind()` starts a 60-second
`ACCEPT_TIMEOUT`, rescoped to measure *"time until a batch bearing this
`OpenSlot` arrives."* Lazily, it would silently come to mean *"time until the
producer produces its first token"* and would expire a queued request with a long
prefill. It costs a record, not a send. The reverse race — an `OpenSlot` for an
`(anchor_id, session_id)` that was never registered — must **not** fail the peer:
the receiver replies `CloseSlot{unknown}` and discards that slot's records.

#### Peer loss

Every live slot in a dying epoch that has not seen a terminal receives an
injected `StreamFrame::Dropped` — **not** `TransportError`. Loss of Messenger
connectivity, peer eviction and batcher eviction all land here. This reproduces
`pump_frames`' existing behaviour and keeps the consumer-visible contract
(`StreamError::SenderDropped`) byte-identical; `TransportError` stays reserved
for protocol violations. Reconnect bumps the epoch and slots do not survive it,
which is what makes "exactly one `Dropped` per failed live slot" provable.

### Flow control

This is the part that makes multiplexing safe, and it is worth being explicit
about why it cannot be skipped.

The shared resource is no longer a socket; it is the peer's **ordering lane**. A
`_stream_batch` handler that awaits holds that lane, so every slot from that peer
stalls behind it — the head-of-line shape a shared socket would have had, with a
worse failure mode, because lane channels are unbounded and a blocking ingress
converts backpressure into unbounded memory growth rather than into a full
socket. Today's receive path shows exactly how it would happen: `pump_frames`
falls through to an awaited `frame_tx.send_async(...)` when a consumer's channel
is full. One saturated anchor would stall every stream from that peer, then every
one of their heartbeat watchdogs would fire at once — for inference, one stuck
HTTP client throttling the GPU.

So **ingress is bounded and nonblocking**, on HTTP/2-style per-slot credit, with
one critical difference from the obvious design.

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
are added to `AnchorAttachResponse::Ok` and its MPSC twin, both
`#[serde(default)]` so older senders still deserialize:

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

**Reserved terminal and control capacity.** Each slot holds back one credit
spendable only by a record matching `is_terminal_sentinel`; data may spend only
`C`; the slot buffer is sized `C + 1`. One reserved credit is provably sufficient
because `sent_terminal` guarantees at most one terminal per slot, after which the
slot closes. `SenderError` is deliberately *not* terminal and correctly spends
data credit. Control records reserve capacity the same way, so `OpenSlot`,
`CloseSlot` and `CreditUpdate` are never what a starved slot fails to deliver.

**Byte credit is the memory bound.** Frame-count credit alone bounds memory at
`slots × C × DEFAULT_MAX_FRAME_SIZE` — a meaningless number. Today the kernel
socket enforces a real ~1 MiB-per-stream limit for free; riding the Messenger
deletes exactly that protection, because the socket is now shared with control
traffic and is not per-stream at all. So: a per-peer byte budget (default 8 MiB)
and a per-slot byte cap (default 1 MiB). Frame credit gives the
no-head-of-line-blocking proof, byte credit the memory bound — different jobs,
both needed.

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

**Egress backpressure is admission, not hope.** Transports expose an ordered
per-target admission gate as `SendOutcome::{Admitted, Pending(SendAdmission)}`,
reserving the ticket synchronously so an unpolled slow-path send cannot be
overtaken. A batcher therefore learns at the send site, in order, that its peer is
congested, and parks itself rather than a runtime worker. It is also what the
rendezvous fence in [Slots](#slots) is made of.

> **Invariant.** A slot never has more than `C` frames outstanding against a
> `C + 1`-deep buffer, so the applier only `try_send`s into space credit already
> reserved and **never blocks its lane** — which matters because the lane runs
> each handler to completion before pulling the next batch.
> `velo_streaming_mux_reader_stall_total > 0` is a bug, not a tuning signal.

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
3. The peer batcher and the peer's ordering lane are untouched. The slot is
   freed and its generation bumped; its credit and byte budget are released.
4. On the receive side the terminal is forwarded using the reserved credit, then
   `CloseSlot` drops the mux-side sender, so `reader_pump` exits through the same
   `Err` branch it uses today when a socket closes. **Identical code path.**
5. The existing "last frame was not terminal and the consumer is still attached,
   so inject `Dropped`" rule becomes per-slot, firing on
   `CloseSlot{reason != TerminalSent}` and on epoch death.

A `velo_streaming_mux_live_slots` gauge must return to zero in every teardown
test. A leaked slot now leaks credit and byte budget for the life of the epoch,
whereas a leaked socket today is at least visible in `lsof`.

### Heartbeats

A correction to a natural assumption first: **the per-stream heartbeat does not
detect a hung producer today.** It runs in a separate spawned task, so a wedged
producer loop keeps it ticking happily. What it actually detects is process or
host death, connection death, and sustained saturation — the last because
`try_send` silently drops heartbeats when the channel is full, converting
saturation into a watchdog kill. That is the saturation-kill path `SATURATION.md`
documents and half-apologizes for.

Under the Messenger, two of those three are already someone else's job: it detects
process, host and connection death for its own peers, and the mux learns of it
through epoch death. The only thing a streaming beat still uniquely carries is
the **per-slot saturation signal** — exactly what a peer-level liveness beat
would throw away. So it stays per-slot and gets cheap rather than deleted.
**Phase-aligned and suppressed**, it costs the same wire with **zero** semantic
change:

- A per-slot "last send tick" (`AtomicU64`, one relaxed store per frame on the
  send path). A slot that sent anything this interval skips its heartbeat —
  **suppression**.
- Each sender subscribes to a peer-level tick (`tokio::sync::watch` bumped by one
  timer task) instead of owning a `tokio::time::interval` — **phase alignment**.
  This is what makes heartbeats coalesce; today they are randomly phased across
  the interval and batching cannot merge them.
- For muxed senders, no per-sender task is spawned at all: the batcher's
  heartbeat task iterates live slots and emits `SlotHeartbeat` for the idle ones.

Result: N scattered frames per interval become at most one batch, N tasks become
one, and the receiver still sees a *per-slot* heartbeat inside its per-slot
deadline — so `reader_pump` and `DETECTION_MULTIPLIER` are untouched.

---

## Flush policy

Senders enqueue; the peer batcher decides when to write.

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
supported configuration, and a forgotten `end_batch()` degrades to the
windowed policy rather than stalling.

### The hint API

```rust
let batch = node.stream_batch();          // == start_batch()
for (sender, token) in outputs {
    sender.send(token).await?;
}
batch.flush().await?;                     // == end_batch()
// Drop schedules a flush if flush() was never called.
```

The guard is RAII because an early `?`-return must not strand a batch.
`start_batch()` / `end_batch()` exist as thin wrappers for callers who prefer
that shape. Nesting is refcounted.

A burst is a hint, not a frame boundary. Size, latency, control and credit limits
may all cut a wire batch inside one open gate, so a caller may not assume that
what it bracketed arrives as one `_stream_batch`.

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

The mux is selected at **attach time**, not announced by a wire magic — there is
no first-bytes handshake left to hide one in. No new negotiation mechanism is
needed either. `AnchorAttachResponse::Ok` and its MPSC twin already carry
`streaming_transport_key`, and the sender already resolves it against its
transport registry. Two changes:

1. `AnchorAttachRequest` and `MpscAnchorAttachRequest` each gain
   `#[serde(default)] supported_transport_keys: Vec<TransportKey>`. Additive and
   internal to `velo`; `serde(default)` means an older sender still deserializes,
   as one advertising nothing, which is exactly right.
2. The attach handlers currently hardcode the local default transport's key.
   They instead intersect the sender's advertised keys with their own installed
   transports, preferring `messenger-mux-v1`.

A node with the mux enabled registers **both** `messenger-mux-v1` and its
configured legacy transport, so it still serves legacy peers. That is required
rather than optional because `resolve_transport` hard-errors on an unknown key in
a non-empty registry — a receiver that unilaterally answered `messenger-mux-v1`
would break every older sender.

The result: the mux is chosen only where both peers advertise it, and every other
pair silently uses the TCP or gRPC path unchanged. SPSC and MPSC are both
supported in the first negotiated version, so there is no half-migrated state
where one anchor kind rides the mux and the other does not.

---

## Configuration

> **Proposed API.** Nothing in this snippet exists yet — `.messenger_mux(...)`
> and `MuxConfig` land with P7/P8. It is written down now so the activation
> shape is reviewed with the protocol rather than improvised after it.

```rust
let node = Velo::builder()
    .add_transport(transport)
    // The legacy path stays configured; negotiation picks per attach.
    .stream_config(StreamConfig::Tcp(Some(TcpConfig { bind_addr })))?
    .messenger_mux(MuxConfig {
        enabled: true,                       // default: false
        flush: FlushPolicy::Opportunistic,   // default
        max_batch_bytes: 60 * 1024,          // further clamped by the eager budget
        ..Default::default()
    })?
    .build()
    .await?;
```

Activation is opt-in and stays that way for this work; the mux is not the default
transport. Defaults are otherwise chosen so `enabled` is the only decision an
operator makes, and so the transparent path never trades latency for throughput
unasked. The switch is also the rollback: set it back to `false` and the node
stops advertising `messenger-mux-v1`, so the next attach negotiates the legacy
path with no code or wire change. That is what makes a canary safe.

---

## Observability

New series, alongside the existing `velo_streaming_*` collectors:

| Metric | Meaning |
|---|---|
| `velo_streaming_egress_flushes_total` | Batches the per-stream egress pump handed to the socket. A unit of coalescing, not a syscall: a frame too large to pack is written segmented and still counts as one |
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
> syscalls -- `write_all` may loop internally, an oversized frame is written
> segmented as a single batch, and TCP segmentation is the kernel's call -- so
> read it as "how much the pump batched", not "how many syscalls were saved".

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

Pure accounting from the table in [Motivation](#what-that-costs), to be pinned
by a test that opens N streams and counts `/proc/self/fd`. This identifies a
**hard wall** — roughly 512 concurrent remote streams at default `ulimit -n` — not a
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
- **`heartbeat_watchdog_firings_total` rises after P10** → heartbeat
  consolidation lost real failure detection.
- **The trap:** throughput improving *because* added latency let the consumer
  catch up. Always report latency beside throughput, and watch **time-to-first-token**
  specifically — any windowed or hinted policy can regress it. That is why
  opportunistic is the default.

---

## Implementation status

| Phase | Scope | Wire change | Status |
|---|---|---|---|
| **P0** | Batching-ratio counters, loopback batching sweep | none | implemented |
| **P1** | Messenger TCP + UDS writer coalescing | none | implemented |
| **P2** | Streaming per-socket coalescing | none | implemented |
| **P3** | Ordered per-sender AM dispatch (`DispatchMode::Ordered`) | none | implemented |
| **P4** | gRPC `Channel` caching per peer | none | specified |
| **P5** | Ordered transport admission, `SendOutcome::Pending` | none | specified |
| **P6** | Eager-size guidance, `Transport::max_message_size` | none | specified |
| **P7** | `MessengerMuxTransport` — `_stream_batch`, `PeerBatcher`, slots, credit | yes | specified |
| **P8** | Attach-time negotiation, opt-in activation switch | additive | specified |
| **P9** | Hint API (`StreamBatch`), windowed policy | none | specified |
| **P10** | Heartbeat suppression and phase alignment | none | specified |
| **P11** | Lift the mux surface into `velo-ext` | none | specified |

P1 and P2 are wire-compatible in both directions and require no negotiation —
the frame decoder already accepts many frames per read buffer, so a coalescing
writer talks to an unmodified reader. They deliver the syscall amortization,
which the cost model predicts is the largest single term, without protocol risk.

P5 and P6 are prerequisites, not preliminaries: without admission the batcher has
no ordered per-target way to discover congestion, and without the eager budget it
cannot size a batch the Messenger will carry inline. P7 does not start until both
land, and until P8 lands it is dead code behind a false switch.

### Why the mux surface is not in `velo-ext` yet

Riding the Messenger removes most of the pressure to publish anything: the mux is
a *user* of the messenger send path, not something a transport implements, so an
out-of-tree `Transport` inherits multiplexing without knowing it exists. Still
tempting, and still deferred, is exposing a mux surface to out-of-tree
`FrameTransport` implementors as `open_mux` / `MuxChannel`, for two reasons:

1. **Coupled defaulted methods are a silent-failure shape.** velo-ext's rules
   require default implementations, which would permit a transport reporting
   `supports_mux() == true` while leaving the acceptor a no-op — senders opening
   muxes into a void. If it lands, it should be *one* method
   `as_mux() -> Option<&dyn MuxTransport>` returning a separate trait with no
   defaults.
2. **The blob-pipe abstraction may be permanently wrong.** An RDMA transport
   already multiplexes natively per queue pair; forcing it to serialize into a
   byte stream so velo can chop it back up is a pessimization. Removing a
   published velo-ext item is a major bump, so shipping the wrong shape is
   expensive.

The two velo-ext additions this work *does* make are not free.
`Transport::max_message_size(target) -> Option<usize>` is shaped to survive:
defaulted, `None` meaning "unknown" and costing the caller a conservative budget.
Admission is the larger commitment — `SendOutcome` gaining
`Pending(SendAdmission)` changes a published enum, so it is a coordinated
`velo-ext` and `velo` bump, and should land once rather than in pieces.

The cost of deferring the rest is bounded: out-of-tree `FrameTransport`s get no
multiplexing for a release or two, and nothing else breaks. Because a receiver
never offers `messenger-mux-v1` unless the mux is installed and enabled, a
mux-less deployment degrades correctly by construction.
