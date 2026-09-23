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
