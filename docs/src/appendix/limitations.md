# Known limitations

What velo does not do yet, stated so that nobody has to find out from a
test. Each section says what happens, when, why, what to do about it, and
what would lift it. The pages of the book that touch a limitation say it in
one line and link here, so the two do not drift.

## `Sent` while a staged item is still held

**What happens.** `StreamSender::retired()` resolves `Retirement::Sent` while
the producer's node still holds an item of the stream. A graceful shutdown
right after `Sent` can cut that item, and the consumer reads a broken stream
instead of it.

**When.** Over the messenger mux, for an item larger than the eager budget:
the smaller of the transport's frame limit and the stager's threshold
(`RendezvousStager::with_threshold`, 256 KiB by default), less the message
envelope.

**Why.** Such an item leaves the producer as a handle. Its bytes stay staged
in the producer's rendezvous store until the consumer pulls them. `Sent` is
reported when the batch that carries the handle is admitted by the transport,
and the drain of `graceful_shutdown` counts requests in flight, not staged
payloads, so neither waits for the pull.

**Workaround.** Keep stream items under the eager budget, or wait for the
consumer's own acknowledgement before shutting down.

**Lifted by.** Not scheduled. The test
`sent_resolves_while_a_staged_item_is_still_held` in
`lib/velo/tests/streaming/retired.rs` pins the behaviour; a fix changes what
that test asserts.

## QUIC discards queued frames at teardown

**What happens.** A frame the QUIC transport admitted but had not yet written
when the transport tears down is reported as failed. A stream whose `Sent`
came just before the shutdown can lose its tail over QUIC.

**When.** At the teardown step of `graceful_shutdown`, over the `quic`
transport.

**Why.** TCP and UDS keep writing admitted frames for the teardown grace.
QUIC keeps written data in user space until the peer acknowledges it, and its
close waits for that acknowledgement, but it does not first write what is
still queued.

**Workaround.** Use TCP or UDS where the tail of a stream must survive a
shutdown that follows `Sent`.

**Lifted by.** Not scheduled.

## A hand-built `AnchorManager` reports a per-stream `Sent` at enqueue

**What happens.** Over the per-stream TCP and gRPC transports of this crate,
`Sent` means that the stream's pump wrote the terminal (TCP) or that the
server took the whole stream (gRPC), but only when the `Velo` builder
installed the transport. An `AnchorManager` built by hand, with
`AnchorManagerBuilder` or `AnchorManager::new`, reports `Sent` as the
terminal enters the egress channel, while the pump still has to write it.
The same holds for a frame transport from outside velo.

**When.** A producer that builds its `AnchorManager` itself, or a
`FrameTransport` implementation from another crate.

**Why.** `FrameTransport` is published in `velo-ext`, so the retirement does
not go through it. The `Velo` builder names the in-crate transports to the
anchor manager by key; nothing else can.

**Workaround.** Build the node with `Velo::builder()`.

**Lifted by.** [#142](https://github.com/ai-dynamo/velo/issues/142): a
defaulted method on `FrameTransport` at the next `velo-ext` release, which
also deletes the second map.

## Per-stream TCP cannot see a consumer that dropped its anchor

**What happens.** Over per-stream TCP, a consumer that dropped its anchor
before the producer's last write still leaves the producer with
`Retirement::Sent`. Over the mux and on the same worker the same case gives
`Discarded`.

**When.** A stream that rides per-stream TCP (the mux off, or a peer without
it), and a consumer that leaves while the producer still writes.

**Why.** The consumer's pump learns of the drop only from the next frame it
tries to forward, so the producer's write succeeds, and a successful write
of the terminal is what `Sent` means on this path. Telling the producer
earlier would take a message on the wire.

**Workaround.** Watch `StreamSender::cancellation_token()`, which the
consumer's drop fires, when the difference matters.

**Lifted by.** Not scheduled.

## A gRPC server from before velo 0.21 answers `Sent` for frames nobody took

**What happens.** Over per-stream gRPC, a producer on this velo reads
`Retirement::Sent` although the consumer never got the stream.

**When.** The consumer's node runs a velo from before 0.21, and its consumer
left before the stream ended, or the server could not receive a frame.

**Why.** `Sent` over gRPC means that the server ended the call cleanly, which
it does only once it has taken the whole request stream. A server from
before 0.21 ended the call cleanly in every case. A server on this velo ends
it with an error when its consumer left, when the stream ended without its
terminal, or when the server pump was dropped with the transport.

**Workaround.** Run the same velo on both nodes.

**Lifted by.** Upgrading the consumer's node.

## `MpscStreamSender` has no `retired()`

**What happens.** A producer on an MPSC stream cannot wait until its output
has left the node.

**When.** Any `MpscStreamSender`.

**Why.** The retirement is recorded for an MPSC stream too, over the mux and
the per-stream transports, but the sender exposes no future for it.

**Workaround.** None inside velo; an acknowledgement in the application's
protocol.

**Lifted by.** Not scheduled.

## The teardown grace covers only what is already queued

**What happens.** At teardown, a TCP or UDS writer keeps writing for the
transport's teardown grace (`teardown_flush_grace` on the builders, 1 s by
default), but only the frames already in its channel: at most
`channel_capacity` per connection. A frame that waits in the connection's admission gate when teardown starts is refused, and a send after teardown fails at once. A send that races the start of teardown can still be admitted and then fail; closing that needs a terminal state on the admission gate in `velo-ext`. Over the messenger mux, a refused batch retires its streams as `Discarded`.

**When.** The close step of `graceful_shutdown`.

**Why.** The grace starts after the drain. The time the work behind the
frames takes belongs to the drain, which `ShutdownPolicy::Timeout` bounds;
the policy cannot reach the writer without a new `velo-ext` trait method.

**Workaround.** Size the grace by the bytes that can be queued per connection
and the slowest peer's read rate, and give the drain its own time through
`ShutdownPolicy::Timeout`.

**Lifted by.** Not scheduled.

## A staged payload without a deadline can stay staged

**What happens.** The sender keeps a staged payload until the sender shuts
down when nobody fetches and releases it, and nothing gives it a deadline.

**When.** The message of a send without a `.timeout` never reaches a
receiver, or the receiver stops before it releases the payload. Also a fetch
that gives up against a sender from before the `_rv_lease_ms` header.

**Why.** Only the receiver's `release` frees a transparently staged payload.
A time-to-live comes only from a call's `.timeout`, and a chunked lease gets a
deadline only when a fetch with a bound asks for one.

**Workaround.** Give large calls a `.timeout`. It gives their payloads a
time-to-live.

**Lifted by.** Not scheduled. A default time-to-live for transparently staged
payloads would lift it, but it would also fail a fetch that is slower than
that time.

## A task that holds its worker thread stalls velo's tasks

**What happens.** An application task wakes a parked velo task, then holds its tokio worker thread without yielding. The velo task can then not run until the thread is free. For a stream over the mux, no record and no heartbeat leaves on that peer's lane, from this stream or any other (with the default single lane, that is every mux stream to the peer). While the sender holds data credit, the consumer's watchdog then ends the stream with `Dropped` after three to four heartbeat intervals of silence, though the producer is alive. For a unary or typed call, later messages to that peer on the same connection wait for the hold. A handler that returns its response and then holds its thread delays that connection's writer the same way. A consumer that reads items and then holds its thread delays the node-wide credit sweep, so credit returns that wait for the sweep are late for every peer; a batch that arrives can still return credit on its own.

**When.** velo runs on the application's multi-thread runtime, which is the default, and the task blocks or computes without yielding right after it calls velo: `std::thread::sleep`, a blocking call, a long encode or decode.

**Why.** The call wakes the velo task (the peer's batcher, a connection's writer, the credit sweep) from the application's worker. tokio puts a task woken there in that worker's LIFO slot, unless it has turned the slot off for the rest of a tick after three LIFO polls in a row, and no other worker steals from the slot. tokio 1.51.0 made the slot stealable, and 1.51.2 and 1.52.2 reverted it for its cost; `disable_lifo_slot` needs `tokio_unstable`.

**Workaround.** Keep blocking work off the task that calls velo. Run it in `tokio::task::spawn_blocking`, or wrap it in `tokio::task::block_in_place`, which hands the worker's queued tasks, the LIFO slot included, to another worker before it blocks.

**Lifted by.** Not scheduled. Running velo's own tasks on a runtime the application cannot hold would lift it; what that costs per message is not measured. `lib/velo/tests/streaming/held_thread.rs` tests the workaround, and shows the stall in an ignored test (`--ignored`): no test setup can force the stall, because another wake can reach the velo task first.
