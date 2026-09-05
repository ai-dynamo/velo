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
whether this helps you and what switching it on looks like. If you are
implementing or reviewing it, the rest is the protocol specification and the
argument for why it is correct.

Status: **P0–P3 and P7–P9 implemented; the rest specified.** The mux is
therefore selectable — opt-in, off by default, negotiated per attach — and its
flush policy is configurable. See
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
4. **Flush policy** — when to write. Two policies, one mechanism.

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
contract, and it **replaced** the deprecated `VeloFrameTransport` rather than
sitting beside it — that transport is deleted. There is no dial, listener, acceptor or connection manager:
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
  [u8 record_type][u32 slot][u32 frame_seq][u32 len][len bytes body]

record_type: 0 = Data, 1 = OpenSlot, 2 = CloseSlot,
             3 = CreditUpdate, 4 = SlotHeartbeat
```

**Every multi-byte field is big-endian, header and record alike.** Stated once
here rather than per field, because a single exception would be the kind of
thing an implementor discovers from a corrupted length rather than from a spec.

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

  bits 31..8 : index        bits 7..0 : generation
```

Index in the high bits so the raw `u32` sorts by index, which is the order a
dense table is walked in.

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
fences the slot: at most one such singleton per slot is outstanding — a
rendezvous record, or (`MuxConfig::async_open_ack`) the slot's own `OpenSlot`
— and the batcher withholds that slot's later records until the staged send is
admitted — its `CloseSlot` included, since a close the receiver meets before
the record it is ordered behind is a close for a slot it cannot name. Only the
record waits.
The kill's other half, ending the producer's inlet, happens the moment the cap
is exceeded: it is the near side of the same event, and a producer left running
ahead into a slot whose records are already being discarded is the thing the
byte cap exists to stop.
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

Eager is about the *write*, not about the ack. `MuxConfig::async_open_ack`
separates the two: the `OpenSlot` is still cut into a batch of its own and still
handed to the transport before `connect` returns, so the accept window keeps
measuring the same thing, but the ack no longer waits for the transport to admit
it. On a congested peer that wait is a place in a send queue that is already
full, and a worker cannot start producing until it comes free. The open then
behaves exactly like an over-budget singleton: unless the admission is already
behind it — synchronously `Admitted`, as it is on an uncongested peer, where
per-target FIFO already orders it ahead of anything sent after it — the slot
is fenced until the admission resolves, so its first record cannot overtake
the `OpenSlot` that claims it, and a failed admission is epoch death either
way. The default is off — the awaited ack is what shipped.

The ack it skips is the open's own. The `OpenSlot` still goes in a batch of its
own, so whatever was already staged for the peer is written first and *that*
write still parks on admission: an open is wait-free only when nothing is
staged, and when something is it costs one frame more than the awaited path,
which packs the `OpenSlot` into the staged batch instead.

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

The two grants can disagree — `C` records of a megabyte each against a
one-megabyte slot cap — and where they do, the byte side wins by **throttling
the next grant rather than refusing a record whose frame credit was already
given**. Refusing would break a stream for a peer that respected everything it
was told; withholding credit stops the peer at the next window instead. The
ahead-of-sequence hold is the one place a byte reservation may still refuse,
because there the alternative is unbounded growth behind a gap that may never
close.

**Control and data travel in separate lanes.** `finalize`, `detach` and `Drop`
use a *synchronous* channel send, which must stay non-blocking under mux. The
egress inlet is therefore split into an unbounded **control lane** (`OpenSlot`,
`CloseSlot`, `CreditUpdate`, terminals) and a bounded **data lane**, drained
control-first. Unbounded is safe because control volume is O(live slots), which
credit already bounds. This incidentally fixes a latent hazard that exists
*today*: `Drop`'s synchronous send on a full 4096-deep channel blocks a runtime
worker thread from inside a `Drop` in async context.

> **The split lane is not what P7 built.** `FrameTransport::connect` hands the
> caller one `flume::Sender<Vec<u8>>` and nothing in that seam distinguishes a
> terminal from a token, so there is no second lane to drain first. Splitting it
> means changing a published `velo-ext` trait — a typed sink in place of a byte
> channel, which belongs with the P11 discussion below rather than ahead of
> negotiation.
>
> What P7 does instead is **drain the inlet unconditionally**. A slot with no
> credit still has its records pulled, into a per-slot withheld queue bounded by
> the slot byte cap, so the channel a synchronous send targets is never the thing
> that is full. The hazard is closed; the ordering guarantee is unchanged, since
> the queue is FIFO and a terminal in it waits for its predecessors exactly as it
> would have on the wire.
>
> The cost is that a producer running past the byte cap on a slot nobody is
> draining **kills that slot** — consumer sees `Dropped`, other slots untouched,
> metered as `withheld_overflow`. That is the per-slot slow-consumer kill this
> document prefers to the watchdog kill, made deterministic; `SATURATION.md`
> describes it from the operator's side.

The batcher's own control inlet is bounded the same way, and for the same
reason. Credit returns, closes and singleton resolutions arrive as **coalesced
per-slot state** rather than as messages: credit accumulates into a `u32`, a
close dominates the credit for its slot, and a failed singleton dominates a
successful one. A queue would have been unbounded exactly when it matters — a
flush parks on admission precisely when the peer is congested, which is when its
ingress lane is busiest returning credit — so the batcher is *woken*, never fed.
Attach requests keep a queue, bounded, because each carries its own channel and
its own waiting caller and there is nothing to merge.

Credit returns get their own priority lane, so a peer whose egress is congested
never stops returning your credit.

**Egress backpressure is admission, not hope.** Transports expose an ordered
per-target admission gate as `SendOutcome::{Admitted, Pending(SendAdmission)}`,
reserving the ticket synchronously so an unpolled slow-path send cannot be
overtaken. A batcher therefore learns at the send site, in order, that its peer is
congested, and parks itself rather than a runtime worker. It is also what the
singleton fence in [Slots](#slots) is made of.

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

Senders enqueue; the peer batcher decides when to write. Two policies, and what
separates them is only *who* decides.

| Policy | Trigger | Added latency | Default |
|---|---|---|---|
| **`Auto { on_admission }`** | end of every wake, having first drained what is already queued | **none** | **on** |
| **`Auto { max_linger }`** | up to `max_linger` after the batch's first record | ≤ window | off |
| **`Manual`** | `flush_batch()` | the caller's | off |

The two `Auto` conditions are a struct rather than two variants because they
compose — a batcher may hold both, and holding neither is a legitimate if
useless setting. `on_admission` is the opportunistic behaviour this document has
described since P0, named for its mechanism: a flush parks until the transport
admits it, so "at the end of every wake" is in practice "as soon as the peer took
the last batch". `max_linger` is the windowed policy, demoted from a policy to a
condition. `Manual` replaces the hinted one.

Opportunistic is the default precisely because it cannot make anything worse:
the egress task never waits for work that has not arrived. It simply notices
that more work is *already* queued and takes all of it. Under load, batches
form; under no load, behaviour is identical to today minus one syscall's worth
of bookkeeping.

**Two reasons to write override every policy**, and they are why a burst is a
hint rather than a frame boundary:

- **A batch at a clamp goes.** The byte cap, the record cap and the eager budget
  each cut a batch where they bind, because holding a full batch buys nothing —
  there is no room left to batch into.
- **Records that carry liveness go.** The awaited open path's `OpenSlot` keeps
  its own eager flush, and a `CloseSlot`, a `CreditUpdate` or a terminal moves
  the batch it was staged into. (`MuxConfig::async_open_ack`'s detached open
  bypasses this policy entirely, dispatching straight to the transport — see
  § "Configuration".) A `CreditUpdate` held back is a peer's sender starved
  with nothing left to rescue it, and no application on this side knows it
  owes that peer
  anything — so this is correctness, not courtesy.

Credit starvation also cuts a batch, from the other direction: a slot with no
credit contributes nothing to the batch at all, and its records wait in the
withheld queue instead.

### The flush API

```rust
for (sender, token) in outputs {
    sender.send(token).await?;   // stage
}
velo.flush_batch();              // one write per peer
```

One method. It is **sync and non-blocking** — it kicks each batcher and returns,
and it is deliberately not a backpressure point; whether a congested peer slows
the producer stays the job of per-slot credit and transport admission. It takes
**no argument**, because a producer holds `StreamSender`s and cannot know which
batcher each one feeds: the destination is packed into the anchor handle and
resolved several layers below, so a per-peer flush would be a call whose correct
use requires knowing something the API deliberately hides. And it is **valid
under either policy and never an error** — under `Manual` it is the write, under
`Auto` it forces one ahead of the conditions — so a call site does not have to
know how the node was configured.

A burst between two calls is a hint, not a frame boundary. The clamps and the
liveness records above may each cut a wire batch in between, so a caller may not
assume that what it bracketed arrives as one `_stream_batch`.

#### Why a call and not a guard

This document used to specify an RAII gate — `start_batch()` / `end_batch()`
around a `StreamBatch` guard, refcounted for nesting, flushing on `Drop` so an
early `?`-return could not strand a batch. The imperative call replaces it, and
deletes rather than solves most of what that design had to specify:

- **No open state.** A gate can be held across an `.await`, which is what made
  the deadlock below a *deadlock* and required two hard rules to prevent. A
  flush has nothing to hold, nothing to nest, and nothing that can be forgotten
  except the call itself.
- **No guard type.** One method against a guard, a pair of wrappers for callers
  who prefer that shape, and a refcount.
- **It fits the loop.** A forward pass is imperative — stage sends, flush, next
  iteration. The guard was ergonomics borrowed from synchronous mutexes for a
  problem that is neither synchronous nor a mutex.

The deadlock the two rules existed for cannot arise, because there is no gate to
close: a producer that sends `C + 1` frames to one slot has frames 1..C staged
and frame `C + 1` in the withheld queue, and the flush that returns credit is
one it makes itself, at the end of the pass it is already writing.

#### The one failure mode

Under `Manual`, records that end up staged after the last `flush_batch` wait for
the next one, and **nothing rescues them** — there is no timer behind the policy,
which is what makes "one write per pass, carrying that pass" a property of the
code rather than of the scheduler. Usually that means a producer that stopped
calling it. It also covers a subtler case: a slot starved of credit has its
records in the withheld queue rather than the batch, so a grant arriving after
the flush releases them into the *next* pass's batch. That one is bounded by the
stream's own end, since a terminal and an inlet close are both records that move
on their own.

The cost is latency rather than memory either way, since staged records are
bounded by the same clamps as any batch, and `velo_streaming_mux_staged_records`
is where an operator sees a plateau. A deployment that wants a net should ask
for one: `Auto { on_admission: false, max_linger: Some(w) }` is the same
batching with a window behind it.

#### Is an explicit flush actually needed?

The measurement above narrows this considerably, and the answer depends on
something worth stating precisely.

A forward pass issues its X sends back-to-back with no `.await` between them, so
under multiplexing they land in one shared egress queue and the opportunistic
drain should see all of them — capturing most of the win with no flush at all.
That is the same effect the 952:1 burst row demonstrates, just with the queue
shared across streams rather than owned by one.

Where it breaks down is a producer that **does** await between sends — awaiting
a per-request tokenizer, a sampling callback, or anything that yields. Then each
send arrives at the egress task alone, the drain finds nothing queued behind it,
and the ratio collapses back toward 1.0 exactly as the forward-pass row shows.
An explicit barrier is the only thing that can group sends the runtime has
already scheduled apart.

There is a second reason, which the measurements did not anticipate and which is
the stronger one for serving. **Opportunistic packing is not deterministic.**
How many records share a batch depends on how the runtime scheduled the batcher
against the producer, so the same workload gives a different answer run to run.
`examples/batched_streaming` measures it at a serving-shaped depth — 96 requests
against a batch of 32 — and five runs of each policy come out:

```text
--flush-policy auto     4.88  5.08  5.08  5.14  4.67
--flush-policy manual   5.38  5.38  5.38  5.38  5.38
```

`Manual` is the higher of the two here as well as the steady one, which the
design of this section did not predict: a batcher writing at every wake
sometimes wakes mid-pass and writes half of one, where a per-pass flush writes
the pass. The cross-pass surplus opportunistic packing was expected to win by is
real, but it needs the producer to outrun the batcher — with no gap between
passes the same example reads 6.61–7.44 for `auto` against 3.47–4.41 for
`Manual`. That surplus is throughput bought with per-token latency, and for a
decode engine that is the wrong trade.

Every figure above is reproducible, with its command and its unedited output, in
[`examples/examples/batched_streaming.evidence.md`](../../../examples/examples/batched_streaming.evidence.md).

So: `Auto` if you want the batcher to do the best it can with whatever it finds,
`Manual` if you want to know what it will do.

---

## Backward compatibility

The mux is selected at **attach time**, not announced by a wire magic — there is
no first-bytes handshake left to hide one in. No new negotiation mechanism is
needed either. `AnchorAttachResponse::Ok` and its MPSC twin already carry
`streaming_transport_key`, and the sender already resolves it against its
transport registry. Two changes, implemented in `streaming/negotiation.rs`:

1. `AnchorAttachRequest` and `MpscAnchorAttachRequest` each gain
   `#[serde(default)] supported_transport_keys: Vec<TransportKey>`. Additive and
   internal to `velo`; `serde(default)` means an older sender still deserializes,
   as one advertising nothing, which is exactly right — an empty list cannot
   intersect, so such a sender is always answered with the receiver's default
   key.
2. The attach handlers no longer hardcode the local default transport's key.
   They intersect the sender's advertised keys with their own installed
   transports, preferring `messenger-mux-v1`, and answer with the credit fields
   above when that is what they picked.

The sender then reads the answer. A key that is not `messenger-mux-v1` is the
legacy path and the credit fields are not its business, which is where every
older receiver lands. `messenger-mux-v1` **with** a window opens a slot already
holding it. `messenger-mux-v1` with **no** window is refused outright rather than
retried elsewhere: no shipped version answers that key, and a node that installs
a mux cannot be configured to advertise a zero window, so it can only mean a peer
that bound a mux receiver and then told us to ignore it — and connecting over any
other transport would reach nothing it is listening on, hanging until the
anchor's watchdog fires instead of failing where the mistake is.

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

```rust
let node = Velo::builder()
    .add_transport(transport)
    // The legacy path stays configured; negotiation picks per attach.
    .stream_config(StreamConfig::Tcp(Some(TcpConfig { bind_addr })))?
    .messenger_mux(MuxConfig {
        enabled: true,                // default: false
        max_batch_bytes: 60 * 1024,   // further clamped by the eager budget
        initial_credit: 256,          // advertised verbatim; zero is refused
        flush_policy: FlushPolicy::Manual,   // default: Auto, opportunistic
        async_open_ack: true,         // default: false; see the warning below
        ..Default::default()
    })?
    .build()
    .await?;
```

> **`initial_credit` may not be zero** — zero is the wire encoding of "not
> offering the mux", so a node configured that way would advertise a key and
> then tell every peer to ignore it, and the build refuses it.
>
> **`flush_policy` is the one knob with a call site attached.** `Manual` is only
> half a configuration: the other half is the producer calling `flush_batch()`,
> and a node configured this way whose producer does not is a node whose streams
> stop. Set it where you own the send loop.
>
> **`async_open_ack` trades the awaited `OpenSlot` for a second way to lose a
> stream, and the only measurement of it so far did not show the open it is
> meant to speed up.** It removes the wait a congested peer's send queue puts
> on opening one, but it is not free either: one `tokio::spawn`, a
> control-inbox map insert and, once the fence lifts, a `release_withheld`
> pass, per open — real costs paid whether or not that wait was on the
> critical path. The fence it installs instead withholds from the slot's first
> record regardless of credit — so a producer that starts generating into
> that same congestion can overrun the per-slot byte cap and be killed before its `OpenSlot` ever
> reaches the wire, unbounded across however many slots are opened this way
> at once (`SATURATION.md` § "Under the messenger mux"). Measured on
> `t3-iso1` (`agent-docs/w4a-async-open-ack-status.md`), the flag alone did
> not move TTFT p50 and made p95 worse in every rep; a control-inbox defect
> and an unconditional fence that withheld an uncongested peer's first record
> for no ordering reason, both from that same run, are fixed on this branch
> but not yet rerun. Set it where a stalled peer's queue depth is bounded and
> you have measured the open it is meant to speed up at your own concurrency
> — not on the expectation that it will.

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
| `velo_streaming_mux_batches_total{direction}` | `_stream_batch` active messages packed (`sent`) or decoded (`received`) |
| `velo_streaming_mux_records_per_batch{direction}` | Histogram of records carried by one of them. Labelled like its sibling and for the same reason: every mux node is both ends at once — credit rides back on `_stream_batch` — so an unlabelled sum would mix a node's own packing with its peers' and be attributable to neither |
| `velo_streaming_mux_staged_records` | Gauge of records packed into batches the batchers have open but have not written. Transient under `Auto`; under `Manual` a plateau is a producer that stopped calling `flush_batch()`, which is that policy's one failure mode |
| `velo_streaming_mux_live_slots` | Gauge; must return to zero at teardown |
| `velo_streaming_mux_reader_stall_total` | **Should always be zero.** Non-zero means the credit invariant is broken |
| `velo_streaming_mux_generation_mismatch_total` | Stale records dropped by the generation check |
| `velo_streaming_slot_credit_exhausted_total` | Per-slot credit starvation events |
| `velo_streaming_mux_drain_visits_total` | Per-peer credit reconciles the sweep task ran because a consumer drained, counted per walk. Divided by elapsed time it is the doorbell's visit rate, which `MuxConfig::drain_visit_floor` caps at `1 / floor` per peer; the periodic sweep's own walks are not counted |

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
| **P5** | Ordered transport admission, `SendOutcome::Pending` | none | implemented |
| **P6** | Eager-size guidance, `Transport::max_message_size` | none | implemented |
| **P7** | `MessengerMuxTransport` — `_stream_batch`, `PeerBatcher`, slots, credit | yes | implemented |
| **P8** | Attach-time negotiation, opt-in activation switch | additive | implemented |
| **P9** | Flush policy — `FlushPolicy`, `Velo::flush_batch()` | none | implemented |
| **P10** | Heartbeat suppression and phase alignment | none | specified |
| **P11** | Lift the mux surface into `velo-ext` | none | specified |

P1 and P2 are wire-compatible in both directions and require no negotiation —
the frame decoder already accepts many frames per read buffer, so a coalescing
writer talks to an unmodified reader. They deliver the syscall amortization,
which the cost model predicts is the largest single term, without protocol risk.

P5 and P6 are prerequisites, not preliminaries: without admission the batcher has
no ordered per-target way to discover congestion, and without the eager budget it
cannot size a batch the Messenger will carry inline. P7 did not start until both
landed, and P8 is what made it reachable: before it, the attach handlers
hardcoded the local default transport key and nothing ever answered
`messenger-mux-v1`.

Two names in the protocol specification above did not survive contact, both on
the receive side and neither changing what happens:

- **There is no `bind_muxed`.** The mux implements the plain
  `FrameTransport::bind`, which registers `(anchor_id, session_id)` in an
  ingress registry and hands back the `C + 1` receiver the spec describes; a
  peer's `OpenSlot` is what claims that registration. A second bind method
  would have had nothing to add, because the window it would have taken is not
  known at bind time — the *receiver* chooses it, and the sender learns it from
  the attach response. `connect_negotiated` is where that number enters, on the
  send side.
- **`reader_pump` is unchanged.** It drains the receiver `bind` returned into
  `frame_tx` exactly as it does for any other transport, which is what the spec
  asks for; the credit hook below is the part that landed differently.

P8 also closed the first of the two P7 deviations this document used to record.
**Initial credit is no longer advertised by a `CreditUpdate` on `OpenSlot`** — a
slot opens already holding the window the attach response carried, which removes
the round trip that cost before the first token. Keeping both would have granted
the sender `2C` against a `C + 1` buffer, so it was a swap rather than an
addition; ongoing credit returns still ride `CreditUpdate`.

The second stands:

- **Credit is returned by reconciling buffer occupancy**, not by `reader_pump`
  calling `credit.release(1)`. The mux compares what it admitted against what is
  still queued, on every inbound batch and on a periodic sweep. The effect is
  the same and the sweep bounds the latency; what it costs is a return arriving
  one sweep tick late when no further batch comes to drive reconciliation on the
  arrival path. The sweep is also what un-parks a sender whose peer has gone
  quiet.

One detail differs on the merits rather than on ordering. `CloseSlot` is
bidirectional with no direction bit, and both sides may hold a slot at the same
dense index, so the **reason carries the direction**: `TerminalSent` and
`PeerGone` travel owner → receiver, `UnknownSlot` and `ProtocolError` travel
receiver → owner.

P9 landed with its API replaced and its policy set reshaped. Both are recorded
above where they belong; what they cost is worth stating in one place.

- **`flush_batch()` replaced `start_batch()` / `end_batch()`.** The argument is
  in [Why a call and not a guard](#why-a-call-and-not-a-guard); the price is
  that an explicit per-pass flush caps a batch at that pass's own fan-out, where
  opportunistic packing can overshoot it by pulling in the next pass's records.
  Measured, that price is smaller and narrower than expected: it is only paid
  when the producer outruns the batcher, and at a serving-shaped depth `Manual`
  is the *faster* of the two as well as the repeatable one. See
  [Is an explicit flush actually needed?](#is-an-explicit-flush-actually-needed)
  for the numbers. It is still a real loss of throughput in the flat-out case,
  taken knowingly: what it buys is that no token lingers into a pass it was not
  produced in, and that the batch size is a number you can derive from the
  deployment instead of one you have to measure.
- **The windowed policy became a condition rather than a variant.** `Auto` holds
  a set of conditions that compose, so "opportunistic" and "windowed" stopped
  being alternatives and became `on_admission` and `max_linger`. Nothing about
  either behaviour changed; there is simply no longer a reason to pick one.
- **`Manual` has no watchdog.** The specification's F2 — a gate watchdog that
  force-opens after 5 ms — is gone with the gate. Under `Manual` a forgotten
  flush is not degraded into the windowed policy; it stalls the records it
  staged, and the deployment that wants the old behaviour configures the window
  explicitly. Making the manual policy quietly stop being manual is the worse
  failure: it would mean the determinism the policy exists for holds only until
  something is slow.

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

   Whatever shape it takes, **the seam should carry a typed sink rather than a
   `flume::Sender<Vec<u8>>`**. The byte channel is why P7 cannot split control
   from data: a terminal and a token are indistinguishable in it, so the only
   way to keep a synchronous terminal send from blocking is to drain everything
   and bound the overflow. A sink that accepts `Frame::{Data, Terminal, …}`
   would let the transport reserve capacity for the records that must never
   queue behind data, which is what "control and data travel in separate lanes"
   above actually asks for.
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

---

## Addendum, 2026-09-01: credit is returned by draining after all

Superseding, by addition rather than rewrite, the two claims recorded above: that
"credit is returned by reconciling buffer occupancy, not by `reader_pump`", and
that doing so has "the same effect" because "the sweep bounds the latency".

The effect was not the same, and the reason is structural. The sweep ran at
`credit_sweep_interval` — 2 ms — and each tick walked every slot of every ingress
peer, taking the same per-peer mutex the inbound batch path takes, to find the
few slots with anything to return. That is work proportional to *peers x slots x
time*, against credit returns proportional to *drains*. At the shape a
512-worker deployment presents to each of two frontends — 256 ingress peers —
the two diverge badly.

**What that costs in CPU is not currently a measured number.** The figures first
written here came from a rig run on a shared login node, and a paired
re-measurement showed the machine noise to be as large as the effect. They are
retracted rather than revised; see the banner in
`examples/examples/response_plane_bench.evidence.md`. A clean measurement under
an exclusive allocation is what should replace them.

What does not depend on that measurement: the sweep was the *only* path
returning credit to a slot whose peer had gone quiet, which is why 2 ms was
load-bearing rather than a tuning choice, and why the interval could not be
relaxed before this change. `lib/velo/tests/streaming/mux_credit.rs` pins that
directly — with the sweep unreachable, a draining consumer's stream died at
frame 4 of a 4-credit window before the hook and completes after it.

So P8's original instinct — return credit where the record actually leaves the
buffer — was right, and the deviation was a false economy at scale. What landed
now differs from the P8 text in two ways, both deliberate:

**The pump rings a doorbell; it does not release credit.** P8 asks
`reader_pump` for `credit.release(1)` per handoff. It instead posts the *peer* on
a bounded lane, and the sweep task runs the reconcile it already knew how to run.
The reason is the surviving sweep: two paths each releasing an amount for the same
drained record double-count, and `release` clamping per call does not save the
pair. Reconciliation recomputes residency from scratch and is therefore
idempotent, so a redundant visit is free and a lost one is only late. The
invariant this protects is that the account's residency belief must never
*under*-estimate: understate it and `admit` stops bounding the physical buffer,
`deliver` overflows a `C + 1` channel, and the slot dies as a protocol error.

**Wakes coalesce per peer, not per record.** A per-peer `AtomicBool` is set by
the first drain and taken down by the sweep task before it reconciles, so a drain
landing mid-visit posts a fresh wake rather than being swallowed. Per-record
posting would have replaced a periodic cost with a worse per-record one; a
per-slot record threshold — the other candidate — withholds credit for the first
`T` records of every slot and still posts once per slot per threshold, so it is
worse on both latency and volume.

**Coalescing bounds the visits above; a floor bounds them below.** Taking the
wake down before the walk is what stops a mid-walk drain being swallowed, and it
is also what lets a consumer that keeps up re-arm the flag immediately: the task
then runs wake → clear → walk every slot → re-armed, back to back, at a rate set
by the traffic rather than by need. Since each walk holds the same per-peer mutex
the inbound batch path takes, that is contention on the hot path of the shape
this mux is for. So `MuxConfig::drain_visit_floor` — **2 ms**, the interval the
sweep itself used to run at — is a ceiling on how often the doorbell may
reconcile one peer. A wake arriving inside the floor is neither cleared nor
walked: it is scheduled for when that peer next comes due, and because the flag
stays up, the drains until then coalesce into that scheduled visit. No wake is
lost; one is delayed by at most the floor. `velo_streaming_mux_drain_visits_total`
counts the walks, so the rate is observable in production, and
`lib/velo/tests/streaming/mux_credit.rs` pins it: before the floor, a stream
drained through an 8-record window rang the doorbell 1000 times in ~270 ms —
roughly seven times what the floor allows — and after it, the same 1000 walks are
spread across ~3 s.

The floor's own cost is credit latency, and only for a producer already parked
with nothing arriving to reconcile it on the arrival path: it waits up to one
floor per window, so `floor / initial_credit` per record. At the default
256-record window that is under 8 µs a record; the ~3 s above is what the same
floor costs at a window of 8, which is why the credit tests configure small
windows on purpose.

Consequently `credit_sweep_interval` now defaults to **200 ms**. `idle_ticks()`
derives from it, so batcher eviction is unaffected in wall-clock terms: the TTL is
`ticks × interval` either way.

Both anchor kinds are hooked. MPSC negotiates the mux in the same version as
SPSC, so `mpsc_reader_pump` carries the same signal; leaving it out would have
made the relaxed interval a silent hundred-fold regression for MPSC streams.

One claim in P8 above is **still not true of the implementation**, and is left
standing here rather than quietly corrected because the fix is not part of this
change: "a background sweep reclaims credit for slots whose pump died". It does
not, on any interval. `IngressSlot::reconcile` measures against `frame_tx.len()`,
which stays pinned once the receiver is dropped, so a dead pump's slot is closed
by the next arrival finding it unknown rather than reclaimed by the sweep.
