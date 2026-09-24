# QUIC response-plane sweep (2026-09-23)

Matrix `t3-quic1` (job 2885660, 44 min, overall_rc 0). Work branch `quic-sweep` at 1df7b05 (velo 0.13.1, feature `quic`). Raw results: `.research/results/t3-quic1/` (per-rep table `steady.tsv`, draw table from `analysis/draw/draw.py`). Analysis script: `.research/analysis/quic/steady.py`.

## Setup

- Nodes: ptyche0354 (frontend, etcd, nats, aiperf) and ptyche0355 (mockers), partition `tcpo`. Every ptyche partition reports 4x GB200 (Grace aarch64, 144 cores); there is no GB300 partition. Ryan chose tcpo.
- Image `rhino-dev-260903.sqsh` for gate, wheel, smoke, and matrix (the default 260831 has no rdma-core headers, and `--all-features` builds UCX with InfiniBand). Past matrices ran on `batch` with 260831, so compare arms within this matrix only.
- Rig NIC `enP6p3s0f1np1`, 200G Ethernet, MTU 1500. `net.core.rmem_max` = `wmem_max` = 212992; a job cannot raise them. The kernel grants 425,984 B against velo's 8 MiB / 4 MiB request.
- Pinned 72/72 (frontend cpus 0-71, aiperf/etcd/nats 72-143), 8 mocker processes x 64 workers = 512, concurrency 8192, ISL 1024, OSL 256, speedup 5, 250,000 requests, 3 reps per arm, arms interleaved per rep.
- Arms:
  - `velo-tcp`: the velo3 config (mux, zero-RTT attach, flush `Auto` on admission, reply linger 1 ms), TCP messenger transport.
  - `velo-quic`: the same over velo's QUIC transport. Frontend `DYN_VELO_QUIC_SERVER_ENDPOINTS=32`, mockers the default 4. MTU and stream window left at quinn's defaults.
  - `mux18p`: PR 11918's multiplexed TCP plane ported into dyn-pin.
  - `dynamo-quic`: Dynamo's QUIC plane brought up to ai-dynamo/dynamo#14876 (quinn 0.11.12, quinn-proto 0.11.18, 32 server endpoints, 16-frame / 4 KiB read budget, `RESPONSE_BUFFER_CAPACITY` 16,384 as upstream), `DYN_QUIC_RESPONSE_BATCH_INTERVAL_US=0` as in their campaign (the shipped default is 5 ms).
- Not run:
  - `velo-quic-mtu`: the rig NIC MTU is 1500, so `max_mtu` 6550 cannot take effect.
  - The pre-14876 Dynamo QUIC arm: both venvs load the one editable `_core.abi3.so`, so the old arm needs its own build and a sequential run. Not cheap.

## Checks

- Gate (tcpo, container): velo `check-tree-velo.sh quick` green (fmt, clippy `--all-features`, QUIC lib tests, `transports_quic`, `transports_quic_shutdown`, `transports_tcp`, `observability_scenarios`, `drain_rejection`). Adapter: fmt, clippy `-D warnings`, `velo_response` tests (3 new QUIC settings tests), the 22 `quic_response` tests from 14876, `environment_names` tests. One test-only adapter fix was forced by velo 0.13.1: `StreamOpenTicket` is `#[non_exhaustive]`, so the adapter's `test_ticket()` builds it through serde.
- Lockfiles: both dyn-pin lockfiles resolve exactly one quinn 0.11.12 and one quinn-proto 0.11.18.
- rustls: velo's QUIC TLS uses `builder_with_provider(ring)`, so dyn-pin's second provider (aws-lc-rs via kube) cannot make it panic.
- Smoke (`smoke-quic1`, one node): all four arms 256/256, zero errors.
- Transit: `assert-transit.sh` (replaces `assert-ucx-transit.sh`, takes the transport) passed on every velo rep. velo-quic: `frames_total{transport="quic"}` 9.15M in rep 1, tcp and ucx labels zero. velo-tcp: tcp only. The teardown dump needed no edit; it already dumps every `velo_` family.
- Zero-RTT: `assert-zero-rtt.sh` passed on every velo-tcp and velo-quic rep.
- Connection churn: velo-quic logged no `QUIC: connection to ... failed` or `QUIC transport error` lines. dynamo-quic logged 136 `response lane failed` lines per rep, all after the profiling window closed (teardown).
- Receive-buffer clamp warnings (each socket also logs a send-buffer one): 73 per velo-quic rep = 33 on the frontend (32 server sockets + 1 dial socket) + 5 per mocker (4 + 1). Dynamo's plane logs none, so its effective buffer size is not recorded.

## Results

"hold" is the backlog draw: the number of mocker processes whose mean first-response time is at least a quarter of the slowest one's, that is, how many processes held the 8,192-way backlog. Throughput and first-token latency move with it (`ttft-draw-confound`), so arms are compared at a matched draw. Steady state is requests that started 10 s or more into the profiling phase. ITL is the per-request mean. CPU is frontend CPU ms per request. UDP drops are the frontend node's `RcvbufErrors` delta from `/proc/net/snmp`.

| Rep | Arm | hold | req/s | TTFT p50 | TTFT p90 | TTFT p99 | ITL p50 | ITL p99 | CPU | UDP drops |
|---|---|---|---|---|---|---|---|---|---|---|
| 3 | velo-tcp | 1 | 2,282 | 41.9 | 138.5 | 227.3 | 1.72 | 105.0 | 12.02 | 0 |
| 1 | velo-quic | 1 | 2,246 | 42.7 | 140.8 | 232.2 | 1.74 | 106.6 | 12.58 | 3,898 |
| 2 | velo-quic | 1 | 2,212 | 40.7 | 144.1 | 235.6 | 1.71 | 110.2 | 12.18 | 5,818 |
| 1 | mux18p | 1 | 2,371 | 45.8 | 133.0 | 215.6 | 1.61 | 101.3 | 9.85 | 0 |
| 1 | dynamo-quic | 1 | 2,109 | 42.4 | 151.1 | 247.5 | 1.77 | 115.3 | 13.77 | 160,179 |
| 2 | velo-tcp | 2 | 2,760 | 41.1 | 84.3 | 114.5 | 1.50 | 49.0 | 13.11 | 0 |
| 3 | velo-quic | 2 | 2,742 | 38.9 | 85.6 | 111.7 | 1.47 | 45.1 | 13.09 | 5,132 |
| 2 | mux18p | 2 | 2,912 | 44.7 | 77.7 | 107.5 | 1.42 | 47.5 | 10.05 | 0 |
| 3 | mux18p | 2 | 2,825 | 45.5 | 79.7 | 110.5 | 1.43 | 48.8 | 10.03 | 0 |
| 2 | dynamo-quic | 2 | 2,328 | 39.1 | 97.3 | 148.4 | 1.59 | 67.9 | 14.35 | 82,184 |
| 3 | dynamo-quic | 2 | 2,496 | 38.9 | 92.2 | 123.3 | 1.53 | 50.7 | 13.95 | 94,359 |

Excluded: `rep1-velo-tcp` drew 7 holders (3,079 req/s, TTFT p50 56.9 ms, CPU 13.51), which no other rep matches. Zero client errors in all 12 reps.

Frontend datagrams received per rep: velo-quic 4.7-5.3M (drops 0.08-0.11%), dynamo-quic 45-48M (drops 0.17-0.36%).

The worker node drops about 1,790 datagrams per arm in every arm, TCP arms included. That is host background, not QUIC. The worker step is SIGKILLed at teardown and never writes an `after` snapshot, so its delta comes from the next arm's `before`; the last arm has none.

## Reading

- **velo-quic does not lose to velo-tcp.** At one holder (two velo-quic reps against one velo-tcp rep) and at two holders (one against one), req/s, first-token p50/p90/p99, ITL, and CPU per request agree within the spread between reps. The cells are n = 1-2 (velo-tcp at one holder and velo-quic at two holders are single runs), so the 1-2 ms first-token and 0-5% CPU differences are not findings. Step 6 (profile the frontend if velo-quic loses clearly) did not trigger.
- **Loopback said QUIC costs 15-30% more CPU per request than TCP; on the cluster at a matched draw it is within about 5%.** Most of the frontend's CPU is outside the transport, and the mux batches many records into each frame.
- **velo-quic against dynamo-quic.** At two holders velo-quic moved 2,742 req/s against 2,328-2,496, with first-token p99 112 ms against 123-148, ITL p99 45 against 51-68, and CPU 13.1 against 14.0-14.4 ms/request. At one holder the direction is the same. dynamo-quic's frontend dropped 16-18 times as many datagrams at two holders and 27-41 times at one. Measured: it received 0.69-0.73 datagrams per output token (frontend `InDatagrams` delta over aiperf `total_osl`), against 0.071-0.081 for velo-quic. With the batch interval at 0, Dynamo's plane sends close to one packet per token record, where velo's mux packs many records into each frame. Its shipped 5 ms interval would change that and was not measured.
- **Dynamo's QUIC works on this cluster now.** With quinn 0.11.12 / quinn-proto 0.11.18 and 32 endpoints it served 3 x 250,000 requests with zero client errors at `rmem_max` 212992. Earlier failures on this cluster are attributed to quinn-proto before 0.11.18 (the old dyn-pin build had 0.11.14; upstream's 0.11.17 fails with `too many gaps in stream buffer`) and the clamped buffer; this sweep did not reproduce the old build to separate the two.
- **mux18p** still has the lowest frontend CPU (about 10 ms/request) and 3-6% more req/s than the velo arms at two holders, with a first-token p50 5-6 ms behind them.
- **UDP drops at the clamped buffer.** With 32 frontend sockets, velo-quic lost about 0.1% of received datagrams, with no visible cost in the tail against velo-tcp at a matched draw.

## Open

- More matched pairs: a velo-tcp / velo-quic-only matrix (about 22 min for 3 reps) would put n >= 3 in each cell.
- dynamo-quic at its shipped 5 ms interval.
- `max_mtu` 6550 needs a path with MTU above 6550; none on this rig.
- A frontend endpoint sweep (4, 8, 32) against the drop count, if drops ever show in a tail.

## Rig and adapter changes (uncommitted, rig-local)

- dyn-pin: `DYN_VELO_RESPONSE_TRANSPORT=quic`; `DYN_VELO_QUIC_SERVER_ENDPOINTS`, `DYN_VELO_QUIC_MAX_MTU`, `DYN_VELO_QUIC_STREAM_WINDOW` mapped onto `QuicTransportBuilder`, parsed from raw values and logged on the "velo response plane listening" line; velo features `["ucx", "quic"]`; quinn 0.11.12; `quic_response.rs` at 14876 plus `RESPONSE_BUFFER_CAPACITY` 16,384. Patches and tarballs under `.research/logs/quic-sweep/`.
- Rig: arms `velo-tcp`, `velo-quic`, `velo-quic-mtu`, `dynamo-quic`; `assert-transit.sh TRANSPORT LOGDIR` asserts every velo arm (not only ucx); `host-limits.sh` snapshots the sysctls, NIC MTU, and UDP counters per arm; `RIG_PARTITION` in `run-ctr.sh` and `t3-submit.sh`; `quic-gate-wheel.sh`, `quic-smoke.sh`; `dbg/arm-parity-check.sh` accepts hyphenated arm names.
- The shared wheel now carries velo 0.13.1 and quinn 0.11.12, and `/work/velo` is on `quic-sweep`. Any other rig run uses that wheel until it is rebuilt.

## Addendum 2026-09-23: QUIC frame transport (`t3-qfs1`)

Question: velo-quic carries the messenger mux over one QUIC stream per peer. Dynamo's QUIC plane is closer to "QUIC does the multiplexing". Is a velo `FrameTransport` over QUIC, one QUIC stream per velo stream with no mux, a better comparison and a better transport?

Built `QuicFrameTransport` (branch `quic-frame-transport`, commit 06351db; `StreamConfig::Quic`): one connection per peer, dialed on first use under a per-peer dial lock; one unidirectional stream per velo stream with the TCP frame transport's 16-byte handshake and frame codec; the QUIC messenger transport's reuse-port sockets, pinned certificate, and MTU clamp. Ten unit tests plus a facade end-to-end test; three mutations (no dial lock, no `Dropped` injection, handshake ignores the session) each turn their tests red, with a green control. Adapter: `DYN_VELO_RESPONSE_STREAM_TRANSPORT=mux|quic|tcp` (mux off for the frame transports, zero-RTT refused off the mux, the attach assertion expects the arm's key). Measured on `quic-sweep` at 47c7b46 (the merge), tcpo ptyche0349/0350, image 260903, same load as `t3-quic1`.

Arms: `velo-quicfs` (QUIC frame transport, messenger on TCP, no zero-RTT, frontend 32 endpoints), `velo-tcpfs` (velo's per-stream TCP frame transport, the control for "one stream per response"), `velo-quic`, `velo-tcp`, `dynamo-quic` (batch interval 0). Post-run checks: `assert-stream-transport.sh` (every process resolved the arm's stream transport, no attach negotiated another) passed on every frame-transport rep; transit and zero-RTT checks passed on the rest.

| Rep | Arm | hold | req/s | Errors | TTFT p50 | TTFT p99 | ITL p99 | CPU | Frontend drops | Frontend datagrams |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | velo-quicfs | 8 | 2,932 | 0 | 57.2 | 131.1 | 32.4 | 16.68 | 27,694 | 2.74M |
| 2 | velo-quicfs | (1)* | 2,560 | 1,482 | 48.0 | 8,181 | 54.6 | 17.02 | 19,962 | 4.89M |
| 3 | velo-quicfs | (1)* | 2,535 | 1,271 | 48.1 | 9,731 | 56.5 | 17.56 | 18,558 | 4.93M |
| 1-3 | velo-tcpfs | 8 | 334-610 | 200,835-229,504 | 38-55 | 103-1,265 | 2-24 | 12.5-14.9 | 0 | - |
| 1, 3 | velo-quic | 1 | 2,254-2,276 | 0 | 41.5-42.4 | 230-232 | 106-107 | 12.2-12.7 | 4,226-4,510 | 4.80-4.87M |
| 2 | velo-quic | 2 | 2,742 | 0 | 41.8 | 113.0 | 45.2 | 13.15 | 2,633 | 4.47M |
| 1 | velo-tcp | 1 | 2,340 | 0 | 41.4 | 223.8 | 103.3 | 11.70 | 0 | - |
| 2, 3 | velo-tcp | 2 | 2,654-2,874 | 0 | 39.8-42.0 | 107-117 | 42-48 | 12.7-12.8 | 0 | - |
| 1, 3 | dynamo-quic | 1 | 1,991-2,016 | 0 | 42.9-43.4 | 262-264 | 122-123 | 13.5-13.8 | 128,551-132,765 | 44.8-45.0M |
| 2 | dynamo-quic | 2 | 2,397 | 0 | 39.4 | 139.6 | 63.4 | 13.90 | 93,599 | 45.4M |

\* The hold count for velo-quicfs reps 2 and 3 is contaminated: the stalled process's mean first-response time (about 1 s) makes it the only "holder" by construction.

### Findings

- **Per-stream TCP (velo-tcpfs) does not survive this load.** 80-92% of requests fail, so its latency and CPU columns mean nothing. The workers log `transport bind failed: Cannot assign requested address (os error 99)`, local port exhaustion. Inference, not checked with `ss`: one TCP connection per stream at about 2,500 new streams/s to one frontend address fills the ephemeral port range with TIME_WAIT. This is the architecture the mux replaced.
- **velo-quicfs has a burst failure on this rig.** In 2 of 3 reps, one worker process's connection (mocker_3, the process holding the backlog both times) went silent for about 20 s starting at the opening burst of 8,192 requests: the frontend accepted its new streams but their 16-byte handshakes did not arrive within 20 s, the worker's stream opens timed out at 20 s, and open streams got no frames, so the frontend's 15 s heartbeat watchdog dropped them (1,402-1,625 watchdog events). The connection then recovered, and the rest of the run had no errors. 1,271-1,482 requests failed per bad rep (0.5-0.6%), and steady first-token p99 went to 8-10 s. Rep 1 had no stall and no errors.
- **Mechanism (inference, not verified):** QUIC loss recovery backing off on one connection after a run of losses. The frontend dropped 18.5k-27.7k datagrams per velo-quicfs rep (0.4-1.0%), against 2.6k-4.5k for the mux over QUIC. Each connection hashes to one frontend socket whose buffer is clamped to 425,984 B, and a hot process opens about a thousand streams on its one connection at the burst. Confirming needs quinn's connection stats (lost packets, congestion events, PTO count) logged from the worker at the stall; not instrumented yet.
- **Packets are not the cost.** velo-quicfs's frontend received 2.7-4.9M datagrams per run, the same as velo-quic and a tenth of dynamo-quic's 45M: quinn packs frames from many streams into each packet. The "no batching across streams" concern does not show in packet count.
- **CPU is higher, with a confound.** velo-quicfs used 16.7-17.6 ms of frontend CPU per request, against 12.2-13.2 for velo-quic, 11.7-12.8 for velo-tcp, and 13.5-14.4 for dynamo-quic. The largest candidate is not the transport: velo-quicfs runs without zero-RTT (it needs the mux), so every request pays an `_anchor_attach` round trip on the messenger, with a frontend handler call, a bind, and a registry insert, which velo-quic and velo-tcp skip. The clean control is velo0 (mux, no zero-RTT), not in this matrix. Transport candidates, unprofiled: two tasks per stream on the accept side plus an expiry task per bind, a per-frame `Vec` copy, and per-stream quinn state. The first-token p50 of the clean rep (57 ms at 8 holders) carries the same confound.
- **No matched-draw comparison against dynamo-quic exists for velo-quicfs.** Its one clean rep drew 8 holders, which no other arm drew in this matrix.
- **velo-quic against velo-tcp, second matrix.** At one holder (n = 2 against 1) and two holders (n = 1 against 2), throughput, first-token latency, ITL, and CPU agree within the spread, as in `t3-quic1`. The earlier ruling stands with more pairs.

### Next, if the frame transport is to go on

1. Cheapest first: snapshot `/proc/net/udp` on the frontend before and after each arm. Its per-socket `drops` column shows whether the stall reps' drops pile up on one of the 32 sockets (one hashed socket overflowed).
2. Instrument: log `quinn::Connection::stats()` (lost packets, congestion events, cwnd, RTT) on both ends when a stream open times out and at teardown. Both ends, because the alternative is that the frontend's single driver task for the hot connection falls behind and the socket overflows for that reason; a frontend profile would show one hot quinn task. Rerun velo-quicfs with velo0 as the control.
3. If loss recovery is confirmed, the candidate remedy is several connections per peer (Dynamo's plane uses 8 bulk connections per worker), which spreads a hot process's streams over several sockets and caps what one stall takes down. A stream-open retry on a fresh connection is the other candidate.
4. Profile the frontend CPU of velo-quicfs against velo0 before any tuning.

## Addendum 2026-09-23 (later): diagnosis and several connections per peer (`t3-qfs2`)

Tree `quic-sweep` at 34aea09 (merge of `quic-frame-transport` 65063c0: `connections_per_peer`, `stats_interval`). tcpo ptyche0352/0353, image 260903, same load, 4 reps. Arms: `velo-quicfs` (1 connection per peer), `velo-quicfs8` (8 connections per peer, each from its own UDP socket), both logging quinn stats every 2 s on both ends; `velo0` (mux, no zero-RTT), the control that pays the same attach round trip. `/proc/net/udp` snapshotted per arm for per-socket drops. `analysis/quic/qstats.py` reads both.

| Arm | Reps | Stalled reps | Errors | hold | req/s | TTFT p50 | TTFT p99 | ITL p99 | CPU | Frontend datagrams | Frontend drops |
|---|---|---|---|---|---|---|---|---|---|---|---|
| velo-quicfs | 1, 2 | 2 | 3,143; 1,001 | (1)* | 2,742; 2,587 | 56.1; 47.7 | 107; 8,965 | 32; 47 | 17.8; 17.6 | 2.8M; 5.2M | 34k; 31k |
| velo-quicfs | 3, 4 | 0 | 0 | 6; 4 | 2,673; 2,782 | 47.7; 49.0 | 122; 122 | 44; 46 | 17.3; 15.7 | 5.0M; 5.0M | 15k; 32k |
| velo-quicfs8 | 1, 2, 4 | 0 | 0 | 2 | 2,408-2,433 | 38.3-39.3 | 124-135 | 51-61 | 16.7-17.1 | 38.8-40.2M | 42k-47k |
| velo-quicfs8 | 3 | 0 | 0 | 1 | 2,040 | 43.4 | 259 | 120 | 16.71 | 44.2M | 48k |
| velo0 | 1, 2, 4 | 0 | 0 | 2 | 2,797-2,838 | 39.7-41.5 | 114-116 | 44-49 | 12.96-13.04 | - | 0 |
| velo0 | 3 | 0 | 0 | 4 | 2,832 | 42.3 | 128 | 55 | 12.86 | - | 0 |

\* contaminated by the stall, as before. The stalled process was the one holding the backlog (mocker_3 in rep 1, mocker_4 in rep 2), so the stall follows the hot process, not an index.

### What the diagnosis showed (rep 1, mocker_3)

- **Not network loss.** The stalled connection lost no packets after the opening burst (7,317 lost, all in the first 4 s, the same as every other connection's 6,500-8,500). RTT stayed near 0.3 ms apart from one 494 ms sample in the burst. cwnd grew to 11-61 MB. Congestion control did not limit it.
- **Not one overflowed frontend socket.** Socket inodes map to reuse-port indexes (inode = 427 + index in rep 1). The stalled connection sat on socket 10, which dropped 4,344 datagrams; socket 8, carrying two healthy connections, dropped 10,747. Drops spread over every socket in use. The one-socket hypothesis is refuted.
- **The sender slowed down.** From 23:26:52 to 23:27:20 the stalled worker sent 1,500-3,000 packets per 2 s, against about 23,000 from a healthy worker. The frontend received exactly what was sent (its counters for that connection match the worker's). The frontend's "handshake timeout" is the receive side of this: QUIC opens every lower-numbered stream when a higher one arrives, so the frontend saw streams the worker had opened but not yet written for over 20 s. The worker's stream opens timed out at 20 s, and the frontend's 15 s heartbeat watchdog dropped the open streams. After the dropped streams cleared, the worker's send rate went to about 30,000 per 2 s.
- **Mechanism (inference, the worker is not profiled):** send-side throughput collapse on the hot worker's single connection. A few thousand per-stream writer tasks funnel into one quinn connection, whose state is behind one lock and whose I/O runs on one driver task; the 494 ms RTT sample during the burst fits a starved driver. Eight connections give that process eight locks and eight drivers, and the stall did not occur in 4 of 4 reps (against 4 of 7 with one connection across both matrices).

### Findings

- **Several connections per peer removes the stall** on this rig: 0 of 4 reps, 0 errors.
- **But it gives up packing.** With 8 connections the frontend received 39-44M datagrams per run, 8 times as many as with one (5M) and close to dynamo-quic's 45M: each connection has fewer frames ready at once, so quinn puts fewer in each packet.
- **Against the fair control (velo0, two holders), velo-quicfs8 loses.** 2,408-2,433 req/s against 2,797-2,838 (14% less), first-token p99 124-135 ms against 114-116, ITL p99 51-61 against 44-49, frontend CPU 16.7-17.1 ms/request against 13.0 (about 30% more). First-token p50 is 1-3 ms better (38.3-39.3 against 39.7-41.5).
- **The zero-RTT confound is settled.** velo0 pays the same attach round trip as velo-quicfs and runs at 13.0 ms/request, so the attach does not explain the frame transport's CPU. The frame transport itself costs about 3-5 ms/request more than the mux on the frontend.
- **One connection, clean reps (3, 4):** 2,673-2,782 req/s at 4-6 holders against velo0's 2,832 at 4, CPU 15.7-17.3 against 12.9. Close on throughput when it does not stall, still more CPU.

### Ruling on "could the frame transport beat the mux?"

Not on this rig. The frame transport's per-stream work costs 20-35% more frontend CPU than the mux at every connection count. With one connection it batches as well as the mux over QUIC but stalls under the burst; with eight it does not stall but sends eight times the packets and loses 14% of throughput against velo0. Its one advantage is a first-token p50 a few milliseconds lower. The mux over QUIC with zero-RTT (velo-quic) remains the better QUIC design here.

### Open

- A middle connection count (2 or 4) might keep most of the packing and avoid the stall. Not measured.
- Zero-RTT for frame transports (the frontend pre-binds the session; the worker opens the stream from the ticket) would remove the attach round trip from both velo0 and the frame transports. Not built.
- A worker-side profile of a stalled rep would confirm the quinn single-connection mechanism.
