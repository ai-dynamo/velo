# Batched streaming design

This chapter is the design record for the messenger mux. It lists the rulings that constrain future changes, each with the alternative that was rejected and what that alternative costs. It also records the validation method, the failures found and their mechanisms, and the hazards that remain open. [Batched streaming](../concepts/batched-streaming.md) describes how the mux works. Read that chapter first.

## Validation methodology

At two nodes and ten streams on loopback, the mux is the same speed as the per-stream path or slightly slower. The gain is proportional to X/Y (streams per peer) and appears only at scale. The method therefore uses independent lines of evidence, ordered by how few assumptions each needs.

### V1: resource-ceiling arithmetic

The per-stream path uses one socket per remote stream: one file descriptor and about 2 MiB of requested socket buffer in each process, and 4 tasks across the two ends. At the default `ulimit -n` of 1,024, a process stops below 1,024 concurrent remote streams, less its other descriptors. This is a hard wall, not a slope. A small-scale null result cannot refute it, which is why it comes first. The intended test opens N streams and counts `/proc/self/fd`. That test is not built.

### V2: analytical cost model

Let `s` be the syscall and TCP cost per `write_all`, `e` the encode cost, `f` the receive-side decode and handoff, `w` a channel-handoff wake and `p` the per-record framing cost:

```text
per-stream:  X · (e + s + w_connect + w_recv + f)
mux:         X · (e + f + p) + Y · (s + w)
```

The model makes three predictions that a measurement can refute:

- Syscalls and memory improve for any X/Y above 1.
- CPU crosses over where X/Y exceeds `(s + w) / (s + w_connect + w_recv - p)`. Because `p` (about 20 ns) is small next to `s` (1 to 3 µs), that ratio is near 1. The prediction is that the mux costs less CPU for X/Y above 2, and saves more than 20% of streaming CPU above X/Y of 8.
- Latency gets slightly worse, by one extra channel hop of 1 to 5 µs. Report this term beside every throughput number.

### V3: microbenchmarks

The planned Criterion targets measure the model constants directly:

- `encode_frame` for one payload against one batch of N, for `e`, `p` and the 64 KiB coalescing cliff.
- Decode of N frames at once against N calls, for `f`.
- A channel round trip, for `w`.
- Loopback N writes against one batched write, for `s`.

These benchmarks are not built.

### V4: deterministic simulation

`SimFabric` charges `base_latency` per transfer, so it captures the packing gain deterministically. Streaming over it needs a `SimFrameTransport` and a `StreamConfig::Sim`, which do not exist. The simulation models only the network term, and it favors the mux there. It models no syscalls, no scheduling, no socket memory and no descriptor exhaustion, which is where most of the real gain is. Its larger value is as a deterministic correctness harness for credit accounting, slot generations and connection loss under adversarial interleavings.

### V5: loopback scale sweep

The plan sweeps X over {1, 8, 64, 256, 1024, 4096} and Y over {1, 2, 8, 32} at about 64-byte frames, with the same binary on both paths. It records wall time, CPU, peak RSS, peak descriptor count, the batching ratio and p50 and p99 per-frame latency. Loopback removes wire time, which favors the mux, so the sweep needs a two-host repeat. A variant pinned to 2 cores shows the task and syscall gain under CPU scarcity, which is the normal state of an inference server. `response_plane_bench` covers part of this sweep. See [Benchmarking](../operations/benchmarking.md).

### V6: falsification criteria

If one of these criteria holds, the design stops or changes:

- **The batching ratio does not improve at X/Y of 64.** Then sends are not clustered in time and the premise is wrong. This is the cheapest and most decisive test. The per-stream path measures 1.00 for the forward-pass shape, and the mux measures 2.19 to 5.38 in `batched_streaming`.
- **p99 per-frame latency at X/Y of 1 is more than twice the per-stream figure.** Then negotiation must default to off. The mux was off by default until 2026-09-24 (see the note below).
- **CPU does not fall at X/Y of 64.** Then encode dominates, and the fix is a cheaper codec. On the external serving rig, the mux cost 8.09 ms of frontend CPU per request against 10.48 for one connection per request.
- **Measurable head-of-line blocking under mixed fast and slow consumers.** Then the credit design failed.
- **`velo_streaming_heartbeat_watchdog_firings_total` rises after heartbeat consolidation.** Then the consolidation lost real failure detection.
- **Throughput improves because added latency lets the consumer catch up.** This is the trap. Always report latency beside throughput, and watch time to first token. Any windowed or hinted flush policy can make it worse, which is why the default flush does not wait.

Note of 2026-09-24: the mux is now on by default. The reason is a failure of the per-stream path, not the latency criterion above. On a cluster, the per-stream path failed 80% to 92% of requests at about 2,500 new streams per second, because the workers could not get a local port. See [Batched streaming](../concepts/batched-streaming.md). The X/Y of 1 latency test was not measured again. A deployment with one stream per peer that sees a latency cost can set `enabled: false`.

## Rulings

### Ride the Messenger, not a connection per peer

The mux packs records into active messages on the Messenger's existing connections. A dedicated mux transport with its own connections needs four more parts:

- A handshake magic value to learn the sender's identity.
- A fan of connections per peer for encode parallelism.
- A four-task connection lifecycle.
- A solution for acceptor identity.

The Messenger envelope already carries the sender, and one batcher per peer spreads encode work.

The price is that streaming shares queues, framing and backpressure with control traffic. Order is an explicit protocol obligation, so every record carries a per-slot `frame_seq`. The 13-byte record header pays for this. Measure before you shrink it.

### Ordered dispatch, not a reorder window

`_stream_batch` uses ordered per-sender dispatch. The deprecated `VeloFrameTransport` put a 4,096-deep reorder buffer over a dispatcher that spawned a task per inbound message. Under cross-stream contention the window overflowed and deadlocked the consumer. That transport is deleted.

Do not shard the lane or reorder batches from one peer. `OpenSlot` travels in its own batch, and a `Data` batch that overtakes it truncates the head of the stream silently. Per-slot `frame_seq` is enough for `Data` only after `OpenSlot` lands.

### Credit against the mux buffer, never against the anchor channel

The anchor's `frame_tx` has writers other than the mux: the same-worker attach path, detach and finalize, the watchdog's injection and MPSC senders. A proof of "C credits against a C-deep channel" fails with a second writer. The mux therefore owns a C+1 buffer and a reader pump moves records into `frame_tx`.

This ruling keeps the reader pump. Merging the pump into the anchor channel removes one task hop per record (about 1 ms of frontend CPU per request on the serving rig), but it breaks the credit proof. Removing the pump for mux streams also removes the heartbeat watchdog and `velo_streaming_reader_pump_backpressure_total`.

### Generations and epochs

Dense slot reuse without a generation delivers a stale record to the stream that now holds its index. A `u8` generation is enough because the epoch scopes the whole table: batches from an old epoch are discarded as a whole.

### The byte side throttles grants and does not refuse records

When frame credit and the byte cap disagree, the receiver withholds the next grant. Refusing a record whose frame credit was already granted breaks a stream for a peer that obeyed every rule. The ingress hold is the one exception, because the alternative is unbounded growth behind a gap that can stay open.

### Drain every inlet, and do not split control from data

The first design split the egress inlet into an unbounded control lane and a bounded data lane. `FrameTransport::connect` returns one `flume::Sender<Vec<u8>>`, and in that byte channel a terminal and a token look the same. A split needs a typed sink in the `velo-ext` trait, which is a breaking change to a published crate.

The batcher instead drains every inlet, with or without credit, into a per-slot withheld queue that the slot byte budget bounds. A synchronous terminal send then never targets a channel that stays full. The cost is the per-slot kill: a producer that runs past the byte cap on a slot nobody drains loses that slot.

### Control is coalesced state, bounded by allocation

Credit returns, closes and singleton resolutions reach the batcher as per-slot state, not as messages. A queue is unbounded exactly when a flush parks on admission, which is when the peer returns the most credit.

A size cap on these maps was the wrong bound. A 4,096-entry cap was sized for about 1,024 slots per peer. On the serving rig one peer held 4,000 to 6,700 slots, and the cap refused grants, closes and the answer that lifts a fenced slot. A refused grant is lost for good, because the receiver zeroes its ungranted count when it mints the `CreditUpdate`. Each map is now bounded by what distinguishes a real key from a bogus one:

- Control that a peer sends about this side's slots is accepted only for an index this batcher allocated, at its live generation. A retired generation is dropped silently, because a close-then-reopen race is ordinary.
- Replies for slots that this side's ingress admitted are never refused.
- Rejections of an `OpenSlot` that ingress never admitted travel on a reject lane capped at 8,192. A dropped rejection loses no credit, so only this lane can have a cap.

### Admission is the backpressure, and a failed admission is epoch death

The batcher awaits each flush's admission, so a congested peer parks the batcher in FIFO order and not a runtime worker. `FireResult` erases the admission error to a string, so the batcher cannot match on a subset of errors. Treating every failure as epoch death is correct anyway. A batch that never reached the wire leaves a `frame_seq` gap in every slot it carried, and the mux does not retransmit.

### Eager OpenSlot, and credit from the attach response

`OpenSlot` goes out when the stream attaches, so the 60-second accept window measures "time until the `OpenSlot` arrives" and not "time until the first token". Otherwise a long prefill expires the window and kills a healthy request.

The slot opens already holding the window from the attach response. Before negotiation, the receiver sent a `CreditUpdate` on `OpenSlot`, which cost a round trip per stream. Both together grant 2C against a C+1 buffer, so the change was a swap.

### flush_batch is a call, and Manual has no timer

The rejected design was an RAII gate with a 5 ms watchdog that forced the gate open. A gate can be held across an `.await`, which made a deadlock possible. The watchdog turns a forgotten flush into the windowed policy. `Manual` is then deterministic only until something is slow, which is the worse failure. A deployment that wants a net configures `Auto { max_linger }`.

The one exception is a pending credit reply. Nothing on the replying side knows that a peer is owed credit, so `Manual` cannot leave the reply to the application. `reply_linger` carries the reply, and the batch around it, out after a bounded wait. A pass that contains a credit reply can therefore split across two writes under `Manual`.

The price of `Manual` is also known. A per-pass flush caps a batch at that pass's fan-out, where `Auto` can pack the next pass too. Measured, `Manual` is lower only when the producer outruns the batcher, and that surplus costs per-token latency.

### Negotiate per attach and register both transports

The receiver answers `messenger-mux-v1` only when the sender named it. `resolve_transport` fails on an unknown key, so a receiver that chooses the mux on its own breaks every older sender. A node with the mux registers both keys. The wire version lives in the key, so incompatible versions never pair. A zero window with the mux key is refused, because a fallback connects to a transport where nothing listens.

### Credit return: the pump posts, the reconcile decides

The first design had the reader pump call `credit.release(1)` after each handoff. The landed design has the pump count on the slot's `DrainSignal`, list the slot on a per-peer dirty lane and post the peer. A reconcile then swaps the count and releases it.

- **Why the pump does not release.** Releasing needs the peer mutex that the inbound batch path takes. Taking it per record trades a periodic cost for a worse per-record one. Two paths that each release an amount for one record also double-count, and the periodic sweep still exists. With the quantity on the slot, every visit is idempotent.
- **Why wakes coalesce per peer.** Posting per record replaces a periodic cost with a per-record one. A per-slot record threshold withholds credit for the first T records of every slot. It also still posts once per slot per threshold, so it is worse on latency and on volume.
- **Why the wake flag drops before the walk.** A drain that lands during a walk must post a fresh wake. The same property lets a fast consumer re-arm the flag at once, so the task walks back to back under the peer mutex. `drain_visit_floor` (2 ms, the old sweep interval) caps the visit rate per peer. A wake inside the floor is scheduled for when the peer comes due, and later drains coalesce into that visit.
- **Why the arrival path reads the dirty lane.** Narrowing the per-batch reconcile to the slots a batch delivered into left every other slot to the doorbell and the sweep. Every stream on the serving rig sends about 4 records more than its 256-record window, so each stream's tail needs one grant. On the rig, sender credit exhaustion rose from 13 to about 20,500 per process. Doorbell visits fell to about one per peer per 12 ms. The frontend's lane wait rose from 0.36 ms to 1.4 s per batch. Throughput halved to 1,516 req/s and TTFT p50 rose from 55 to 331 ms. The grant must ride the peer's next inbound batch.
- **Why the count replaces the occupancy estimate.** Reading `frame_tx.len()` takes the channel's lock. At about 1,000 slots per peer and 6 million batches per rep, that read was the largest velo-only cost on the frontend.
- **Why the order is clear, then swap.** A drain that lands between the two steps finds the listing down and lists the slot again. The next pass then finds either a zero count or the new drain, never a count with nothing to fetch it.

The doorbell's deferral queue holds at most one entry per peer. An earlier version queued a second entry when a periodic tick cleared a peer's wake and a drain re-armed it inside the floor. The queue then grew by one entry per tick, permanently. The floor is also clamped to one hour, because `last + floor` overflows for an absurd `Duration`.

The periodic sweep defaults to 200 ms and must be non-zero. `tokio::time::interval` panics on a zero period inside the spawned sweep task. The build returned `Ok` and the mux then ran with no credit backstop, no eviction and nobody reading the doorbell. The build now refuses a zero interval.

A grant threshold (grant only when half the window has drained) was built and measured. It cut credit updates 19-fold and did not change CPU. It made the inter-token p99 worse, because a slow reader exhausts the sender sooner when drained credit is held back. Do not build it again.

### Zero-RTT: pre-bind is the synchronous twin of bind

`MessengerMuxTransport::prebind` calls the same `open_bind` body as `FrameTransport::bind`. `bind` is async only because the trait is. One body keeps the two paths from drifting.

- **Rejected: bind on `OpenSlot`.** Minting a ticket and binding when the `OpenSlot` arrives has a simpler lifecycle. It inverts a layer (ingress resolves the anchor's channel from the registry) and it loses the meaning of the accept window.
- **No protocol version bump.** The ticket rides the application's envelope as an optional field. A version bump breaks an old worker outright. An absent field makes the worker attach the ordinary way.
- **The watchdog exemption is gated on `PumpContext::prebound`, not on the drain claim alone.** The mux parks a `DrainSignal` for every bind. The signal reads as unclaimed until the peer's `OpenSlot` arrives, which is after the attach response returned. `prebound` tells a real pre-bind (no sender yet) from an ordinary attach whose `OpenSlot` is still in flight. It is an `Arc<AtomicBool>` so that `PreBind::adopt` can clear it at once when a sender attaches the long way.
- **The exemption lets the accept window be the true backstop.** Counting silence before a sender exists caps the wait at `DETECTION_MULTIPLIER × heartbeat_interval` (15 s at the default), whatever the application configured.
- **Adoption does not restart the accept window.** An adopted attach inherits what is left of the 60 s. Whichever deadline comes first catches a sender that dies before its first record: the watchdog, measured from adoption, or the remaining accept window. With a heartbeat interval of 20 s or more, the accept window always wins.

### async_open_ack fences only when admission is pending

The first version fenced every detached open. On an uncongested peer the admission is already `Admitted` synchronously, and per-target FIFO already orders the slot's later records behind the `OpenSlot`. The fence then only made the first record wait for a spawn, a control insert, a wake and a release pass. The open now fences only when `admission_state()` is not `Admitted`.

This skip applies to `OpenSlot` only. A rendezvous record always fences its slot, even when admitted synchronously. The receiver resolves rendezvous bytes in a detached task before dispatch, so the sender's admission order does not order the receiver's apply.

### The batch gap meter keeps a high-water mark

A detached open and a later flush to the same peer are separate tasks, so their `batch_seq` values can reach the wire out of order. Per-slot order does not depend on `batch_seq`, so this inverts a counter, not a stream. The gap meter used `wrapping_sub`, and one inverted pair added 4,294,967,295 to `velo_streaming_mux_batch_seq_gaps_total`. The meter now keeps a high-water mark under RFC 1982 comparison. A batch behind the mark is not metered, so an inverted pair costs exactly one. A `batch_seq` per registration class removes that one. It is not built.

### One timer per stream in the reader pump

The pump arms one pinned `Sleep` for its stream. A received frame pushes the deadline only when the deadline is within half a window. When the timer fires, the pump re-arms from the last frame and counts a miss only if the last frame is older than the deadline.

The first version re-armed only from the fired arm. Under traffic every stream fired once per window, and 8,192 streams opened in the same second fired in phase. The receive-arm push is what keeps a live stream's timer from firing.

The pump stamps the frame time after the forward to the anchor channel completes. A forward blocked on a full anchor channel is the pump working, not the sender going silent. Stamping on arrival charges that block to the sender and fires the watchdog one window early.

The `select!` is `biased` toward the receive arm. `tokio::time::timeout`, which this replaced, always polled the receive first. An unbiased select can let a ready timer win over a ready frame, and report a real terminal as a watchdog `Dropped`. The messenger's ordered-lane router uses the same one-timer shape and the same bias.

### Heartbeats: the consolidation that is specified, not built

A per-stream heartbeat task does not detect a hung producer, because it runs on its own task. Under the mux, the Messenger detects process, host and connection death, and the mux learns of it through epoch death. The one signal a stream heartbeat still carries is per-slot saturation. The design keeps the heartbeat per slot and makes it cheap:

- **Suppression.** A per-slot "last send" tick (one relaxed `AtomicU64` store per frame). A slot that sent anything in the interval skips its heartbeat.
- **Phase alignment.** Each sender follows one peer-level tick (a `tokio::sync::watch` driven by one timer task) instead of its own `interval`. Heartbeats then coalesce into one batch instead of scattering across the interval.
- **No per-sender task.** The batcher walks its live slots on each tick and emits `SlotHeartbeat` for the idle ones.

N scattered heartbeats per interval become at most one batch, and N tasks become one. The receiver still sees a per-slot beat inside its per-slot deadline, so the reader pump and `DETECTION_MULTIPLIER` do not change. `SlotHeartbeat` must not join the reserved control class: a heartbeat dropped under saturation is the saturation signal. Today each sender still runs its own heartbeat task, and heartbeats travel as `Data` records.

## Why the mux surface is not in velo-ext

The mux is a user of the Messenger send path, not something a transport implements. An out-of-tree `Transport` gets multiplexing without knowing that the mux exists. Exposing a mux to out-of-tree `FrameTransport` implementors (for example `open_mux` and `MuxChannel`) is deferred for two reasons:

1. **Coupled defaulted methods fail silently.** The `velo-ext` rules require default implementations. A transport can then report `supports_mux() == true` and leave the acceptor a no-op, so senders open slots into nothing. If this surface lands, it must be one method, `as_mux() -> Option<&dyn MuxTransport>`, returning a separate trait with no defaults. The seam must carry a typed sink (`Frame::{Data, Terminal, ...}`), not a byte channel. A transport can then reserve capacity for records that must never queue behind data.
2. **A byte pipe can be the wrong abstraction.** An RDMA transport already multiplexes per queue pair. Forcing it to serialize into a byte stream so that velo can split it again is slower. Removing a published `velo-ext` item is a major version bump, so the wrong shape is expensive.

The additions this work made to `velo-ext` were shaped to last. `Transport::max_message_size(target) -> Option<usize>` has a default, and `None` means unknown, which costs the caller a conservative budget. `SendOutcome::Pending(SendAdmission)` changed a published enum, so it shipped once, as a coordinated `velo-ext` and `velo` release. The egress recorders on `TransportObservability` have no-op defaults.

The cost of deferral is bounded. Out-of-tree `FrameTransport`s get no multiplexing, and nothing else breaks. A receiver offers `messenger-mux-v1` only when the mux is installed and enabled, so a deployment without the mux degrades correctly.

Methods that the mux needs outside the trait (`advertised_limits`, `connect_negotiated`, `take_drain_signal`, `prebind`, `release_bind`, `close_claimed_slot`) are inherent on the concrete `MessengerMuxTransport`. `AnchorManager` already holds the concrete type, so none of them needs a trait change.

## Failures and their mechanisms

### A terminal send wedged the runtime

A test with six streams to one peer, 100 frames each and `initial_credit` 8 never finished. The same six streams passed with a window of 512, so credit exhaustion was the variable. The process had three threads, all in `futex_wait`, and used no CPU.

The first hypothesis was half right. It named the right call site (`finalize`, `detach` and `Drop` reach the inlet through a synchronous `flume::Sender::send`) and the right resource (the C+1 inlet). It was wrong in two ways:

- It assumed that a starved slot's channel never drains. The batcher had drained every inlet into its withheld queues and was idle, waiting for credit.
- It assumed that the terminal reserve was too small. The inlet was full of data records. The `+1` in the slot depth is a credit reservation on the receive side, not egress channel capacity. A reserved terminal seat fixes this test and leaves the defect.

The real mechanism: `std::thread::available_parallelism()` returned 1 on that host (while `nproc` reported 128), so the test runtime had one worker. The synchronous send blocked that worker. The batcher, the only task that can make room, did not run again, and runtime shutdown waited on the blocked worker. With W workers, W concurrent blocking terminal sends wedge the runtime. A decode engine that finalizes many starved streams at once is that case.

The fix is `send_terminal`. It calls `try_send`. On a full channel it hands the record to a task that awaits space. The task holds a sender clone, so the receiver cannot see end-of-stream before the sentinel. With no runtime on the thread, it blocks. The invariant is structural: no terminal send blocks a runtime-owned thread, at any credit, stream count or runtime size. `tokio::task::block_in_place` was rejected because it panics on a `current_thread` runtime.

`detach` clears the attachment flag only after the sentinel is in the channel. Otherwise a sender that re-attaches in the gap puts its records ahead of the `Detached` frame.

The test `a_terminal_lands_even_when_its_inlet_is_full` runs eight streams at `initial_credit` 1, the smallest window a peer can advertise. With the blocking send restored, it hangs. With the fix, it passes in 0.07 s.

The MPSC sender has the same class of defect on its same-worker path. `MpscStreamSender`'s `Drop` for a local sender still calls a blocking `tx.send`, and `test_mpsc_local_drop_preserved_under_backpressure` asserts that the drop blocks while the channel is full. Its remote path uses `try_send` and does not block.

### A credit reply was lost on epoch death

A `CreditUpdate` zeroes the slot's ungranted credit when it is minted, not when its batch is admitted. From then on, the batch holds the only copy of that credit.

- **Loss path 1.** The batcher applies replies before its own control. A reply staged from the first set can still be in the open batch when a failed singleton in the second set calls epoch death. Epoch death discards the batch. `reply_linger` widens this window from one control drain to up to the linger. The batcher now keeps its own copy of the credit in the open batch. On epoch death it hands the copy back, and the next batch advertises it again. This is correct and not a double grant: the credit belongs to ingress slots, which epoch death does not close. If the batcher is retiring and its inbox is closed, the credit goes to the batcher that took over the peer. `velo_streaming_mux_credit_reposted_total` counts the hand-backs. `velo_streaming_mux_credit_lost_total` counts credit that reached neither destination and must stay at zero.
- **Loss path 2.** A batch that fails to write loses its credit replies. This path is open on purpose. `Batcher::flush` clears the copy before the write. A transport that refused this batch refuses the rebuilt one too. The re-post also wakes the task. Closing this path therefore turns a failing transport into an unbounded retry paced by `reply_linger`. The idle reaper never reaches a task that wakes. Losing the credit of a peer whose writes fail costs nothing that the failing epoch did not already cost. No counter records this loss. If the decision is revisited, bound the retry.

Two alternatives were considered for path 1. The chosen one re-posts on discard. The other defers the zeroing until admission. That needs write confirmation per slot from the batcher back into ingress, across a boundary that the flush gate keeps.

### A prompt close was lost to batcher eviction

A consumer that dropped a claimed pre-bind posted a close to the producer. The close was lost when the sweep retired the peer's batcher between the removal of the ingress slot and the post. The idle producer's sender then stayed open. The batcher inbox now closes on its last drain. A writer that finds it closed gets its record back and posts through a fresh batcher. A duplicate close is idempotent at the peer. A cancelled batcher also unregisters before its inbox closes, so a refused writer cannot resolve the same batcher again.

### Tests that timed their preconditions were blind

Two batcher tests set up their state by timing and flaked on CI. `velo_streaming_mux_batches_total{direction="sent"}` counts batches offered, not landed. The increment happens before the send, so a test that sampled it raced the batcher's wake. The fixed tests observe the state that the code keeps: one waits for `SendOutcome::Pending` on the test transport, the other holds the batcher at a test barrier.

Mutation testing showed that both original tests were blind: under a mutation that broke the property under test, 0 of 30 runs failed. The fixed tests were red in 30 of 30. In one test, the `current_thread` runtime flavor is the detection mechanism: with the flavor reverted, the mutation went undetected.

## Open hazards

These properties of the current code are known and not yet changed:

- **The sweep does not reclaim credit for a dead pump.** A dead pump counts no drains. The next record for its slot finds the receiver gone and closes the slot with `UnknownSlot`.
- **Each bind spawns a 60-second accept-window task.** A claim does not cancel it. At 3,000 attaches per second, about 180,000 tasks are live. `expire_bind` is idempotent, so one reaper task with a monotonic deadline queue can replace them. It must keep the unconditional removal of the parked drain signal and the "no OpenSlot arrived" warning.
- **The per-peer drain flag map never shrinks.** A pump holds its peer's flag as an `Arc` for the life of its stream. Removing the map entry while such a pump lives leaves it setting a flag that nothing reads. That peer's credit then falls back to the periodic sweep. Removal must happen under the same visibility that retires slots and binds.
- **The drain listing uses two atomics.** On a weakly ordered target, a listing can be missed. The count stays, and the slot waits for the periodic walk. This was not observed on x86-64 or AArch64.
- **A fence lift can queue behind opens.** Under `async_open_ack`, the batcher polls the opens channel ahead of coalesced control. While opens for a peer are queued, the resolution that lifts a fence waits.
- **`velo_streaming_mux_live_slots` counts a slot killed while fenced.** Its registry entry survives until the admission resolves, which can be the rest of the epoch.
- **MPSC local `Drop` blocks.** See [A terminal send wedged the runtime](#a-terminal-send-wedged-the-runtime).
