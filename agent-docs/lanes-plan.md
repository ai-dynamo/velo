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

## Measurement notes

- Per-stream throughput is credit-bound, not transport-bound: 32 records per credit round trip. Loopback TCP, 64 B items, one stream: 14.7k items/s. Stream cells need many concurrent streams to reach the transport.
- Stream items over 60 KiB ride rendezvous, so stream cells use 64 B and 16 KiB.
