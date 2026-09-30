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

## Progress, 2026-09-29: Stage A done

Commits `6bc76f2` (batchers keyed by (peer, lane)) and `bc902a2` (ingress, drain wake, doorbell, sweep, `SlotClaim` keyed by (peer, lane)). Types: `LaneIndex(u16)` and `PeerLane { peer, lane }` in `messenger_mux/lane.rs`. Gate on compute: fmt 0, clippy 0, lib 1138 passed (2 new ingress tests: lanes keep separate tables; a claim and its wake name the arrival lane), all ten streaming integration suites pass.

Where lane 0 enters today (grep `LaneIndex::ZERO`): the `_stream_batch` handler in `MessengerMuxTransport::new`; `FrameTransport::connect` (stays lane 0: the bare trait carries no lane); `AnchorManager::connect_streaming` (Stage B takes the lane from the ticket or attach response); test helpers. Everything else takes its lane from a key or from `SlotClaim`.

For Stage B: `writer.rs` names `STREAM_BATCH_HANDLER` twice, in the send and in `compute_cap`'s `effective_eager_payload`, so the per-lane handler name must reach both. `table_byte_budget` in `ingress/mod.rs` is called with one lane; pass the receiver's lane count there, and note a budget below the lane count rounds to 0. `MAX_INGRESS_SLOTS_PER_PEER` is now per table, so the per-peer bound is that times the lane count. The health probe (`peer_is_alive`) runs per batcher, so once per lane.

## Progress, 2026-09-29: Stage B done

Commits `177ff75` (lanes on the wire: 16 batch handlers, `.lane(k)` sends, header lane, lane-mismatch drop, per-peer limits split over lanes), `02767b6` (lane in the attach responses and the ticket, `lane_key` in the attach requests, sender clamp, QUIC end-to-end tests), `5d95867` (book: Lanes section), `0393ac7` (MPSC lane test). `177ff75` was gated with `LaneIndex::clamped` present and removed only for the commit; every later commit was gated as committed.

Gate on compute (`lanes-gate-full.sh`, which now adds `transports_quic`, `transports_quic_shutdown`, `transports_tcp`): fmt 0, clippy 0, lib 1153 passed (1154 with the MPSC test), and all 13 integration suites pass (mux_credit 9, mux_flush 3, mux_negotiation 44, cancel 6, velo_integration 1, velo_specific 2, observability 4, tcp_batching 5, mpsc_integration 17, mpsc_remote_integration 12, transports_quic 22, transports_quic_shutdown 11, transports_tcp 22). Extra suites pass (timing 2, mock_transport 8, observability_scenarios 32; the two endurance suites run 0 tests), and `drain_rejection` 7. Mutation checks (`.research/perfwork/lanes-mutate-B.sh`, `lanes-gate-B-last.sh`) turn each new test red when its mechanism is removed: sender clamp, `.lane(k)` on sends, ticket skip-if-zero, header cross-check, per-table slot limit, MPSC lane copy.

What is in place:
- `lane.rs`: `MAX_LANES = 16`, `LaneIndex::clamped(lane, lanes)` (the only non-test constructor from a number), `handler_name()`, `mux_lanes(transport) = min(transport, 16)`, `is_batch_handler` (observability allowlist).
- Header: lane in the low nibble of `flags` (`protocol::LANE_FLAGS`); high nibble reserved. `MUX_VERSION` and `messenger-mux-v2` unchanged. Ingress drops a mismatched batch before any table is created, as `MuxDropReason::LaneMismatch` (`lane_mismatch`).
- Per-table limits (`ingress::TableLimits::split`): bytes `max(peer_byte_budget / lanes, slot_byte_budget)`, slot indices `MAX_INGRESS_SLOTS_PER_PEER / lanes` (name kept: it is per peer again). `lanes` is read only when a table is created, through `MuxCore::transport_lanes(peer)`, which falls back to 1 when the peer cannot be translated.
- Wire: `StreamOpenTicket.lane`, `AnchorAttachResponse::Ok.lane`, `MpscAnchorAttachResponse::Ok.lane` are `#[serde(default, skip_serializing_if = "is_zero_lane")]`; `lane_key: Option<u64>` on both attach requests is `skip_serializing_if = "Option::is_none"`. So lane 0 / no key is byte-identical in JSON, positional rmp and named rmp (tested). Deviation from the brief: the response `lane` and the request `lane_key` also skip when default, to keep ruling 5 literal.
- Consumer choice: `MessengerMuxTransport::choose_lane()` is the one decision point. It feeds `negotiation::Selection.lane` (SPSC and MPSC attach) and `prebind_anchor`'s ticket. It returns lane 0, or the lane a test forced with `force_lane` (per-mux `OnceLock`, `#[cfg(test)]`). `adopt_prebind` echoes the ticket's lane in its response.
- Sender: `connect_streaming` opens on `mux.sender_lane(peer, ticket.lane)` = `clamped(lane, transport_lanes(peer))`. Both the SPSC and the MPSC attach paths copy the response lane into the ticket they build.

For Stage C:
- `choose_lane()` needs arguments: the peer (attach: sender worker id, from `ctx`/`stream_cancel_handle`; pre-bind: none) and the `lane_key` from the request (attach) or the keyed API (`attach_anchor_keyed` / `prebind_anchor_keyed`). `select()` in `negotiation.rs` calls it, so it will need the request passed in. The receiver does not read `lane_key` yet; its doc says so.
- Least-used on attach: `IngressRegistry::live_slots(PeerLane)` per lane is the count; the lane count is `mux_lanes(transport_lanes(peer))`. Pre-bind has no peer: use `lanes(any)` and count local binds per lane (binds do not record a lane today; `register_bind` would need one).
- The receiver does not check that an `OpenSlot` arrives on the lane it chose. A sender that clamped (fewer lanes) legitimately arrives on another lane, so any such check must allow `chosen % sender_lanes`, which the receiver does not know. Leave it unchecked.
- Zero-RTT: if the worker cannot translate the minting node's worker id when it opens a ticket, `transport_lanes` falls back to 1 and the stream rides lane 0. Correct, but loses the spread.
- `MAX_INGRESS_SLOTS_PER_PEER / 16 = 4096` indices per lane. A keyed hash that piles many streams on one lane hits that before the peer-wide 65,536.
- Old-worker hazard (ruling 13) is now live code: a minting node that names a non-zero lane breaks old workers under positional rmp. Stage C turns on non-zero lanes only where the transport keeps more than one lane (QUIC with `lanes(n)`), so a default deployment stays lane 0.

## Correction, 2026-09-29: ruling 17 is withdrawn

The TCP lanes numbers behind ruling 17 came from a stale binary: the TCP-lanes prototype build failed (a `missing_docs` error) and the build script copied the old QUIC-only binary. With a working build, across two nodes, 64 KiB pipelined (GB/s = 10^9 B/s): TCP 1, 2, 4 and 8 lanes gave 2.5, 4.6, 8.1 and 13.1 GB/s with the process on NUMA node 1, and 17.6 GB/s at 8 lanes with both sides on the NIC's NUMA node 0. One TCP connection is limited by its receiver: one reader task spends about 0.85 core in `recvmsg`, runs on the NUMA node away from the NIC, and velo's 2 MiB `SO_RCVBUF` request is clamped to 416 KB and locked, which turns off autotuning. Data: `.research/perfwork/tcpprof/`.

New ruling 24: TCP implements lanes too, keyed (peer, lane) like QUIC. TCP needs no per-lane port: the kernel gives each accepted connection its own socket, and the listener reads each on its own task. Ryan (2026-09-29): rerun the Dynamo harness with lanes on TCP and on QUIC once the mux lanes land, pinned to velo main or a release.

## Progress, 2026-09-30: Stage C done

Commits `51d3f92` (lane choice by key or load, keyed API, tests; the `force_lane` hook is removed) and `43e62f1` (book: how the mux chooses a lane), and `4d17c46` (separate choice locks for attach and pre-bind; gated with fmt, clippy and the 129 lane/negotiation/ingress/prebind lib tests, after the full gate below).

What is in place:
- `messenger_mux/lane_choice.rs`: `stable_hash` (splitmix64 finalizer, golden values pinned in a test), `keyed_lane(key, lanes) = stable_hash(key) % mux_lanes(lanes)`, `LaneLoad`, `LaneReservation`.
- `MessengerMuxTransport::choose_lane(peer: Option<WorkerId>, key: Option<u64>) -> LaneReservation`. Lanes: `transport_lanes(peer)` on attach; `VeloBackend::max_lanes()` (max of `lanes(self.instance_id)` over installed transports) on pre-bind. With one lane everything is lane 0, so the old-worker ticket hazard stays off by default.
- Load. Attach: `live_slots(peer, k) + unclaimed attach binds (peer, k)`. Pre-bind: unclaimed pre-binds on k (plus any bare `FrameTransport::bind`, counted on lane 0). Ties to the lowest lane. The argmin and the increment happen under a mutex (unkeyed only; one for attach, one for pre-bind), so concurrent choices cannot pick the same lane from a stale read.
- A `LaneReservation` lives in `ingress::BindEntry`. Its `Drop` does one `fetch_sub` and nothing else, because the claim path drops the bind while it holds a table mutex and the choice reads tables. Every exit (claim, release, expiry, shutdown, attach arms that leave the bind to the accept window) gives the count back.
- `MessengerMuxTransport::bind_on_lane` replaces `prebind`: attach (through `negotiation::Selection::bind`) and pre-bind both use it. `select()` now takes the peer and the request's `lane_key`; the peer is `ctx.sender_worker_id()` from the envelope, not `stream_cancel_handle`.
- API (in 0.18.0): `Velo::{attach_anchor_keyed, prebind_anchor_keyed, attach_mpsc_anchor_keyed}` and `AnchorManager::{attach_stream_anchor_keyed, prebind_anchor_keyed, attach_mpsc_stream_anchor_keyed}`. A local anchor ignores the key.

Deviation from the brief: "fewest live ingress slots" on attach also counts unclaimed attach binds of that peer. An `OpenSlot` arrives only with the sender's first batch, so attaches answered together all see zero live slots; without the pending term, 8 concurrent attaches all go to lane 0 (the mutation check proves it).

Gate on compute (`lanes-gate-full.sh`): fmt 0, clippy 0, lib 1165 passed (11 new), all 13 integration suites pass with the same counts as Stage B. Mutation checks (`.research/perfwork/lanes-mutate-C.sh`): all 10 turn their test red: attach pending term dropped (8 concurrent attaches no longer 2 per lane), reservation never decremented (the e2e pre-bind test and the unit test), key hash forced to 0 (no key reaches lane 3), pre-bind count ignored (tickets not 0..3), SPSC and MPSC handlers dropping `req.lane_key` (slot not on lane 3), peer and local lane counts forced to 16 (a one-lane consumer names another lane), `max_lanes()` forced to 1 (pre-binds do not spread).

For Stage D:
- **Risk: unkeyed pre-bind can pile onto lane 0.** The pre-bind count falls on claim, as specified. When workers claim tickets within milliseconds (the Dynamo frontend), few pre-binds are pending at any moment, so most choices tie and go to lane 0, while the streams live for seconds. Measure the lane spread of unkeyed pre-binds on the rig (live slots per (peer, lane) on the frontend). If it is skewed, either count a pre-bind until its slot retires (move the reservation into `IngressSlot` at claim), or have Dynamo call `prebind_anchor_keyed` with a request id. The keyed path spreads regardless.
- The attach choice reads `live_slots` for each lane, and `PeerIngress::live()` scans the slot vector under the table mutex. It is per attach, not per record, but at 16 lanes and large tables it is 16 locks and scans; replace with a counter if it shows in a profile.
- A keyed hot spot fills one lane's slot share (`MAX_INGRESS_SLOTS_PER_PEER / lanes`, 4,096 at 16 lanes) before the peer-wide 65,536.
- **Deployment: pre-bind lanes follow the widest transport.** `max_lanes()` is the maximum over installed transports, so a minting node with QUIC `lanes(n > 1)` plus TCP names non-zero lanes in tickets for every worker, including workers it reaches over TCP. Turning on QUIC lanes on a minting node needs every worker upgraded first, not only the QUIC ones.
- An attach arm that fails after its bind (anchor removed, already attached, pre-bound meanwhile) leaves the bind to the accept window, so its reservation counts for up to 60 s.
- The unkeyed choice takes a mutex: one for attaches (held while it reads up to 16 slot tables) and a separate one for pre-binds (atomics only), so frontend pre-binds do not wait behind attach scans.
- `max_lanes()` assumes `lanes()` ignores its target. If TCP lanes (other worktree) make `lanes()` per peer, pre-bind needs a different count.
- `VELO_QUIC_LANES` in the examples only needs the transport builder's `lanes(n)`; the mux follows it. `throughput.rs` on `perf/lanes` has no stream mode yet (it is on `bench/throughput-stream`). Every example streams through unkeyed `attach_anchor`, so the measurement exercises the least-used choice on attach, not pre-bind.

## Progress, 2026-09-30: Stage D done

Branch state: `perf/lanes` merged #106's last head `9dec9e2` (one conflict, `streaming/control/feed.rs`, only `key` against `peer` in a variable name; kept the lane-keyed side), then `git merge -s ours origin/main` (538679c), after checking `origin/main^{tree}` equals `9dec9e2^{tree}` (both `7ae16d7`). The diff to `origin/main` is only the mux-lanes work. Gate after the merge: fmt 0, clippy 0, lib 1166, all 13 suites pass.

Commits:
- `3455039` fix: unkeyed pre-binds count the node's live slots per lane. Each `IngressSlot` holds a `LaneReservation` on `IngressRegistry.live` (a `LaneCounts`, one atomic per lane, summed over peers), taken on the arrival lane before the claimed bind drops. The pre-bind choice reads `live_on_lane(k) + pending pre-binds on k` under `choosing_local`, atomics only. `LaneLoad::reserve` now takes `live(Option<WorkerId>, LaneIndex)`.
- `9edcaca` book: the pre-bind rule.
- `dd94509` cherry-pick of the stream mode (`ad2d4ed` from `bench/throughput-stream`), no conflicts.
- `3a8320d` `VELO_QUIC_LANES` in `quic_from_env`, examples README and book examples page describe the stream mode. No TCP knob: #108's review fixes removed `tcp_from_env`/`VELO_TCP_LANES` from the examples, so that is left to #108.
- `84f0099` book: stream numbers in `quic-performance.md` (Streams over lanes) and one line in the batched-streaming Lanes section.

Tests: `claimed_pre_binds_keep_their_lane_counted_while_their_streams_live` (two producers, 8 pre-bound streams each claimed and live before the next ticket; red on the old code with `[8, 0, 0, 0]` against `[2, 2, 2, 2]`, log `.research/perfwork/gate-last-velo-lanes-impl/D-red.log`), `the_live_count_per_lane_matches_the_slot_tables` (ingress invariant through duplicate open, cancel before claim, consumer-side close, new epoch, shutdown), `pre_binds_count_the_nodes_live_slots_and_unclaimed_pre_binds` (unit). The Stage C test is renamed `pre_binds_spread_and_give_their_lane_back_on_release_and_expiry`: a claim no longer frees the lane, so its claim step now expects lane 0 (all four lanes at one).

Mutation checks (`.research/perfwork/lanes-D-fix.sh`, `lanes-D-mut2.sh`), all red: pre-bind live term dropped (2 tests), slot count taken on a throwaway counter (e2e and invariant), slot count on lane 0 instead of the arrival lane (invariant), live term dropped in `reserve` (unit), attach pending term dropped (Stage C's attach test still guards it). The Stage C script `lanes-mutate-C.sh` no longer applies its `pendingterm` sed (the line changed); `attach-no-pending` here replaces it.

Final gate: fmt 0, clippy 0, lib 1169 passed, all 13 suites pass with Stage B counts.

### Two-node stream measurement

Setup: `throughput --modes stream`, server (producer, attaches) on node A, client (consumer, places lanes) on node B, 200G Ethernet, `VELO_QUIC_LANES=n` both sides, default 4 server sockets, 200,000 items per cell, 2 reps. Script `.research/perfwork/lanesD-2node.sh`, binary built by `lanesD-build.sh` (build stops before copying on failure; loopback smoke: QUIC 64 streams 16 KiB 690 MiB/s at 1 lane, 3,200 at 4).

QUIC, 16 KiB, MiB/s (job 2926332, ptyche0217/0218, data `.research/perfwork/lanesD/quic/`):

| Lanes | 16 streams | 64 | 256 |
|---|---|---|---|
| 1 | 730–776 | 732–756 | 493–677 |
| 2 | 1,499–1,505 | 1,440–1,441 | 1,472–1,506 |
| 4 | 2,561–2,874 | 2,445–2,829 | 2,784–2,814 |
| 8 | 3,660–3,704 | 5,290–5,300 | 5,282–5,365 |
| TCP 1 conn | 1,722–1,776 | 2,098–2,125 | 1,916–2,056 |

64 B, items/s: QUIC 1 lane 174k–178k (16 streams), 736k–927k (256); QUIC 8 lanes 159k–161k, 1.75M–1.93M; TCP 1 conn 183k–186k, 851k–856k.

Lane-use evidence (job 2926584, `.research/perfwork/lanesD/quic-socks/socks.txt`): the consumer process had 5, 8 and 12 UDP sockets at 1, 4 and 8 lanes = 4 server sockets + one dial socket per lane it returned credit on, so streams sat on every lane. The same job gave 533–735 MiB/s at 1 lane and 3,721–5,150 at 8 (other node pair). The server-side sample read a stale server: `kill $S` on the srun does not stop the remote server, and `pgrep` without `-n` picked the oldest; the idle leftovers do not carry traffic.

TCP lanes, measured on a throwaway branch `tmp/lanesD-tcp` (worktree `velo-lanesD-tcp`: `perf/lanes` + `perf/tcp-lanes` at 020e310 + a local `VELO_TCP_LANES` knob; not gated), job 2926582, ptyche0157/0161, data `.research/perfwork/lanesD/tcp/`. 16 KiB MiB/s: 1 lane 2,361–2,390 / 2,779–2,990 / 2,880–3,158 (16/64/256 streams); 2 lanes 4,270–4,424 / 4,605–5,401 / 5,170–5,287; 4 lanes 3,960–4,015 / 8,876–9,701 / 7,739–8,716; 8 lanes 3,047–3,123 / 10,272–11,008 / 13,497–15,686. 64 B: 1 lane 179k–182k / 790k–807k; 8 lanes 162k–163k / 2.04M–2.20M. The consumer had 2 established TCP connections per lane (its dial for credit plus the producer's), 2/4/8/16 at 1/2/4/8 lanes.

What the PR reviewer must know:
- Few streams of small items lose about 9% at 8 lanes (QUIC and TCP alike: 16 streams of 64 B). Many streams gain 2x (64 B) to 7x (16 KiB, QUIC). Not profiled; the likely cause is fewer records per lane batch.
- 16 streams cannot fill 8 lanes: per-stream rate is credit-bound (32 records per round trip). TCP at 8 lanes with 16 streams (3,047–3,123) is below 2 lanes (4,270–4,424) for the same reason plus spread.
- The pre-bind choice now counts every peer's live slots, but not other peers' pending attach binds (they live for milliseconds and counting them needs a scan or a second counter).
- The live count is per arrival lane, not per chosen lane; a clamping sender's stream counts where it actually rides.
- `c196911` (an earlier merge on this branch) has no `Signed-off-by`; DCO will flag it when the PR opens. Not rewritten.
- The Dynamo rig run at the default is still open (ruling 24's rerun with lanes on TCP and QUIC).
