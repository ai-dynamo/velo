# Lanes plan (issue #102)

Status: draft, 2026-09-29. Work branch only; `agent-docs/` never merges to `main`.

## Why

One quinn connection does its packet and crypto work on one task, so one peer caps at about 0.8 GB/s over QUIC. A prototype that stripes a peer over N QUIC connections, each on its own client UDP socket, scales almost linearly across two nodes (200G Ethernet, 64 KiB pipelined, 2 reps each):

| Lanes | MB/s |
|---|---|
| 1 | 788 to 789 |
| 2 | 1,552 to 1,558 |
| 4 | 2,994 to 3,020 |
| 8 | 5,773 to 5,850 |

TCP on one connection gave 2,409 to 2,474 MB/s in the same job. UDP receive-buffer errors stayed under 50 per run, so the gain is CPU spread over cores, not socket buffers. 64 B pipelined does not change with N. Data: `.research/perfwork/lanes/2node/`. The prototype (`exp/quic-lanes-proto`) sends round-robin and ignores order; it proves the ceiling moves, nothing more.

## Rulings carried from the issue and from Ryan

1. A lane is one ordered channel from a sender to a peer. Order holds per (peer, lane), not per peer.
2. A stream slot is pinned to one lane at bind time (attach or `prebind_anchor`) for its whole life.
3. The consumer chooses the lane: `hash(key) % N` when the caller gives a key, else the lane with the fewest live slots. The lane index travels in the attach response and in the `StreamOpenTicket`.
4. Non-mux messenger traffic stays on lane 0. Per-peer FIFO for ordinary active messages is not reopened here.
5. The default is one lane, which is today's behaviour, byte for byte on the wire.
6. No priority lane and no first-message path.
7. Producer backpressure lands first (branch `fix/mux-producer-backpressure`): the batcher pauses a slot's inlet at the byte cap. Without it, any bulk stream dies after 1 MiB of run-ahead and the stream cells cannot be measured.

## Rulings from the per-peer state inventory

The inventory (read-only pass over `messenger_mux/`) found that the receiver keys epoch, `batch_seq` and the slot table per peer, and that slot ids are only unique within one batcher. Two batchers to one peer would retire each other's slots and could cross records between streams. So:

8. **Ingress is per (peer, lane).** `IngressRegistry.peers` is keyed `(WorkerId, LaneIndex)`. Epoch, `batch_seq`, slots, `touched`, dirty set and `drain_pending` move with it. The drain wake channel and the doorbell floor are keyed the same way. `peer_byte_budget` is split evenly over the receiver's configured lanes, so the per-peer bound holds without shared state across tables.
9. **Batchers are per (peer, lane).** The batcher registry and every lookup in `MuxCore` take a lane. Epochs still come from the one shared counter.
10. **Replies go back on the arrival lane.** A batch that arrives on lane k is answered (credit, closes, rejects, lifecycle replies) through batcher (peer, k). `SlotClaim` records the lane its `OpenSlot` arrived on; consumer-side stop and cancel use it. The lane is never derived again from anything else.
11. **One handler per lane.** Lane 0 keeps the name `_stream_batch`; lane k > 0 is `_stream_batch.k`. Each is ordered by sender, so ordering becomes per (sender, lane) with no change to the dispatcher, `velo-ext` or the transports' receive side. Every node registers `MAX_LANES` (16) handlers at build time. The batch header carries the lane in its reserved flags byte, and ingress drops a batch whose header lane disagrees with its handler.
12. **Clamping.** The sender's lane for a slot is `lane % sender_lanes`, where `sender_lanes = min(MuxConfig::lanes, transport.lanes(peer), MAX_LANES)`. A peer that sends no lane field is lane 0. Replies use the arrival lane, which is always a lane the sender has a handler for.
13. **Ticket compatibility.** `StreamOpenTicket.lane` is `#[serde(default, skip_serializing_if = "is_zero")]`, so a lane-0 ticket is byte-identical to today and an old worker still decodes it. A non-zero lane needs workers upgraded before minters (rmp positional encoding rejects an extra element).
14. **Lane failure is confined.** A lane's connection failure is that (peer, lane) batcher's epoch death; only its slots fail. The per-lane ingress epoch keeps other lanes' slots alive.

## Transport API (velo-ext, additive)

```rust
/// Ordered channels this transport keeps to `target`. Frames sent on one
/// (target, lane) arrive in order; nothing is promised across lanes.
fn lanes(&self, _target: InstanceId) -> u16 { 1 }

/// As `send_message`, on `lane`. A lane at or past `lanes(target)` maps to
/// `lane % lanes(target)`. One admission gate per (target, lane); a lane never
/// fails over to another connection within an epoch.
fn send_message_on_lane(&self, target: InstanceId, lane: u16, header: Bytes, payload: Bytes,
    message_type: MessageType, on_error: Arc<dyn TransportErrorHandler>) -> SendOutcome {
    let _ = lane;
    self.send_message(target, header, payload, message_type, on_error)
}
```

Both have defaults, so out-of-tree transports keep compiling. `velo-ext` 0.5.3 to 0.6.0 is not needed for additive defaulted methods; `scripts/check-semver.sh` decides. The attach request, the attach response and `StreamOpenTicket` gain public fields, which is breaking for `velo` (struct literals), so `velo` goes to 0.18.0.

## Rulings from Ryan, 2026-09-29

15. **Key API.** `Velo::attach_anchor_keyed(handle, key: u64)` and `Velo::prebind_anchor_keyed(handle, key)`. The unkeyed `attach_anchor` and `prebind_anchor` choose the least-used lane.
16. **Version.** `velo` goes to 0.18.0, because the attach request, the attach response and `StreamOpenTicket` gain public fields.
17. **TCP keeps the default of one lane for now.** Two nodes, 64 KiB pipelined, TCP lanes prototype (`VELO_TCP_LANES`, round-robin), 2 reps: 1 lane 2,601 to 3,432 MB/s; 2 lanes 3,583 to 3,843; 4 lanes 3,357 to 3,781; 8 lanes 2,881 to 3,089. TCP flattens at about 3.5 GB/s, so its limit is not one connection. Loopback: 6.38 GB/s at 1 lane, 6.37 GB/s at 4. Data: `.research/perfwork/lanes/2node-tcp/`.

## PR sequence

1. `fix/mux-producer-backpressure` (#104): pause the inlet at the byte cap. Tests: the integration test that a fast producer waits and completes, and three unit tests that fail with the pause disabled.
2. `bench/throughput-stream`: the stream mode of `throughput`, two-host capable.
3. Transport lanes: the two `velo-ext` methods, QUIC (one client socket and endpoint per lane, connections keyed (peer, lane)), messenger `.lane(k)` on the send builder. TCP keeps the default (ruling 17). Tests: per-lane order under load, lanes independent (killing one lane's connection fails only that lane's frames), `closed()` waits for every lane, N = 1 is unchanged.
4. Mux lanes: rulings 8 to 14. Tests: per-lane order under load across many streams, lane selection (key hash and least-used), a lane failure fails only its slots, credit returns on the arrival lane, ticket round-trip with and without a lane, an old-format ticket decodes, N = 1 unchanged on the wire.
5. Measure and write up: two-node stream cells at N = 1, 2, 4, 8 for QUIC (and TCP), 64 B no-regression, then the Dynamo mocker rig at the default. Book chapter update.

## Stream baseline at one lane (2026-09-29)

Two nodes, `throughput --modes stream` (branch `bench/throughput-stream` on #104), 200,000 items, 2 reps, 32 server sockets on the consumer:

| Items | Streams | TCP MB/s | QUIC MB/s |
|---|---|---|---|
| 16 KiB | 16 | 2,158–2,191 | 533–738 |
| 16 KiB | 64 | 2,658–2,783 | 485–709 |
| 16 KiB | 256 | 2,256–2,885 | 487–684 |
| 64 B | 256 | 790k–888k items/s | 752k–908k items/s |

QUIC streams stop at the one-connection ceiling. TCP streams reach 2.2 to 2.9 GB/s through the same single batcher and ingress task, so the mux per peer is not the limit that lanes must lift. Ruling 12 stands. Data: `.research/perfwork/lanes/2node-stream/`.

## Measurement notes

- Per-stream throughput is credit-bound, not transport-bound: 32 records per credit round trip. Loopback TCP, 64 B items, one stream: 14.7k items/s. Stream cells need many concurrent streams to reach the transport.
- Stream items over 60 KiB ride rendezvous, so stream cells use 64 B and 16 KiB.

## Addendum, 2026-09-29 (after #104, #105, #106)

Base: `perf/lanes` merges #104 (`fix/mux-producer-backpressure`) and #106 (`perf/quic-lane-ports`, on #105). `velo` is 0.18.0 already; `velo-ext` 0.5.4. `Transport::lanes()` returns `NonZeroU16`. The messenger `.lane()` patch saved during #105 is at `/tmp/claude-2000518758/-lustre-fsw-core-dlfw-ci-ryan-velo/f0205b41-1ff7-4afc-aeff-edcfa2bad138/scratchpad/messenger-lane.patch`.

Rulings that replace or refine ruling 12:

18. **The mux follows the transport.** A peer's mux lane count is `transport.lanes(peer).get().min(MAX_LANES)`, with `MAX_LANES = 16`. No separate `MuxConfig` knob: a QUIC transport built with `lanes(8)` gives 8 mux lanes, and TCP stays at 1.
19. **Mux lane k rides transport lane k.** The batcher for (peer, k) sends with `.lane(k)`, so each handler name stays on one ordered connection.
20. **Every node registers `MAX_LANES` handlers at build.** Lane 0 is `_stream_batch`; lane k > 0 is `_stream_batch.k`. Each is ordered by sender and captures its lane. A node never receives a lane it did not register, because the sender's lane is at most the consumer's choice, and replies use the arrival lane.
21. **Lane selection by the consumer**: `lane_key` given: `hash(lane_key) % lanes`; else the lane with the fewest live ingress slots for that peer (attach) or the fewest local binds (pre-bind, peer unknown). `lanes` is the consumer transport's `lanes(peer)` (attach) or `lanes(any)` (pre-bind).
22. **The sender clamps**: its lane = `response.lane % own lanes(peer)`. A missing field is lane 0.
23. **Header cross-check**: the batch header's reserved flags byte carries the lane; ingress drops (and meters) a batch whose header lane differs from its handler's lane.

Stages:
- A. Pure re-key: batchers, ingress tables, drain wake, doorbell, sweep, `SlotClaim`, reply routing keyed by (peer, lane), with lane 0 everywhere. No behaviour change; the whole existing suite passes.
- B. Wire: handlers per lane, `.lane(k)` sends, header lane, lane in the attach response, ticket and requests, sender clamp; still choosing lane 0.
- C. Selection: `attach_anchor_keyed`, `prebind_anchor_keyed`, least-used choice, MPSC attach.
- D. Tests from the PR sequence list, examples knob `VELO_QUIC_LANES`, two-node stream measurement.
