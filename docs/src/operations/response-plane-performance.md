# Response plane performance

This chapter reports how velo performs as the response plane of an LLM serving stack. It gives the result, the changes that produced it, the changes that did not help, and the mechanisms found on the way. [Benchmarking](benchmarking.md) describes the rig and the measurement rules. Read it before you compare any number here with a new one.

The response plane is the path that carries generated tokens from a worker back to the frontend. In these measurements velo runs the messenger mux over TCP inside Dynamo's serving stack. The comparison plane is Dynamo's multiplexed TCP response plane (Dynamo PR 11918, ported onto the same Dynamo base so that both planes share one stack). Both planes carry many streams over a few shared connections and batch their frames.

## Result

Measured on 2026-09-10 on the external rig that [Benchmarking](benchmarking.md#the-external-serving-rig) describes: 512 mocker workers, concurrency 8,192 and 250,000 requests per rep. The frontend ran on one tokio runtime with 72 workers. Each of two matrices ran three reps per plane.

The velo configuration: mux over TCP, zero-RTT stream setup, write on admission (`FlushPolicy::Auto` with `on_admission`), `reply_linger` 1 ms, `initial_credit` 256, `async_open_ack` off.

A holder is a mocker process that keeps part of the 8,192-request backlog after the opening burst. The holder count sets throughput and the tails for every plane, so reps compare only at a matched holder count. Steady state is the requests that started 10 s or later into the measured phase. Where two reps share a draw, both values are shown.

| Holders | Plane | req/s | TTFT p50 ms | Steady p90 ms | Steady p99 ms | Frontend CPU ms/req |
|---|---|---|---|---|---|---|
| 1 | velo | 2,440 | 39.5 | 132 | 216 | 11.9 |
| 1 | comparison | 2,274 / 2,323 | 46.4 / 46.2 | 137 / 134 | 226 / 217 | 10.1 / 10.0 |
| 2 | velo | 2,824 / 2,818 | 39.3 / 43.5 | 83 / 84 | 116 / 111 | 13.2 / 13.2 |
| 2 | comparison | 2,835 | 45.7 | 80 | 107 | 10.3 |
| 3 | velo | 3,447 / 3,133 | 45.4 / 44.5 | 87 / 96 | 181 / 171 | 13.0 / 13.0 |
| 3 | comparison | 3,211 | 48.3 | 103 | 229 | 11.4 |
| 5 to 6 | velo | 3,142 | 55.5 | 101 | 265 | 13.4 |
| 5 to 6 | comparison | 3,330 | 55.1 | 116 | 308 | 12.2 |

- At a matched draw, velo's first-token p50 is 2 to 7 ms ahead.
- Steady-state p90 and p99 are level or ahead.
- Throughput is equal within the draw.
- Both planes had zero errors in eighteen reps.
- Frontend CPU per request is 1.6 to 2.9 ms higher on velo. This is recorded, not a requirement.

### Against the shipping per-request plane

Dynamo's shipping plane opens one TCP connection per response. Measured on 2026-09-05 with core pinning, before zero-RTT setup. The velo arm here is the mux with an attach round trip per stream. CPU is the corrected figure.

| Plane | req/s (mean of 3) | TTFT p50 ms | TTFT p99 ms (raw) | Frontend CPU ms/req | Errors |
|---|---|---|---|---|---|
| Per-request TCP | 3,306 | 58 | 1,709 | 10.48 | 0 |
| velo mux, attach per stream | 3,210 | 71 | 833 | 8.09 | 0 |
| Comparison mux | 3,372 | 47 | 753 | 7.61 | 0 |

The per-request plane's accept loop is bistable. In the jammed state about 3,900 sockets sit connected but not accepted, and TTFT is the backlog divided by the accept rate. Its raw p99 of 1.7 s comes from that loop.

Dynamo's QUIC response plane (PR 11996) did not complete a clean run at this scale on this cluster. Six reps, measured before core pinning, had 4,947 to 84,525 errors each: HTTP 500s and streams with no content. Its low TTFT medians in those reps were survivorship, because dropped requests do not queue. A likely factor is `net.core.rmem_max` at 212,992 bytes, which clamps QUIC's UDP sockets about 30 times below what quinn requests. This is a result for this cluster, not a general verdict on QUIC.

## What produced the result

The changes are listed in order of effect. Each one landed with a test that fails without it.

1. **One tokio runtime in the frontend.** This is the largest lever, and it is outside velo. The Dynamo frontend ran two 72-worker runtimes on 72 cores. The Python bridge built its own runtime lazily, because its initialization code sat in a branch that never ran. The velo node ran on the first runtime. The HTTP handlers and the per-stream reader pumps ran on the second. Every response record crossed between them through the injection-queue mutex of the second runtime. That lock took 2.4% of the cores plus 6.8% in lock contention. A worker blocked on that mutex does not service the time driver. Linger timers then stopped for seconds, and credit stopped. The 8,192 streams fell into a 6 to 8 s limit cycle. The fix initializes the bridge with Dynamo's runtime, a 24-line change. Thread count fell from 242 to 154.
2. **Zero-RTT stream setup.** The worker sends without waiting for the attach round trip. The worker-ingress-to-first-token segment fell from 78 to 48 ms at p50.
3. **Credit on the next batch for every drained slot.** The reader pump names the slot it drained, and the next inbound batch reconciles it. Before this change, each inbound batch walked every slot of its peer. At about 1,000 slots per peer and 11 slots touched per batch, `flume::Shared::len` alone took 2.1% of the frontend's 72 cores, about 0.5 ms per request.
4. **One timer per stream in the reader pump.** The pump used to build a `tokio::time::timeout` per record. Each one took the time driver's lock twice. The `Sleep` subtree was 7.3% of the frontend's cores, plus 0.8% for the cancellation future. With one pinned timer, the timer subtree fell to 0.15%.
5. **Reply linger.** Credit replies wait up to 1 ms to share a batch. With zero-RTT setup, the frontend's egress had carried one batch per credit reply: 1,027,872 outbound batches against 97,042 without zero-RTT. The linger cut frontend batches four to six times and saved 0.4 ms of CPU per request.
6. **Control maps bounded by allocation.** A 4,096-entry control cap was sized for about 1,024 slots per peer. One mocker process held 4,000 to 6,700 live slots. The cap refused credit grants and closes, and with `async_open_ack` it refused the answer that lifts a slot's fence. A fenced slot then waited for the 15 s watchdog, and the client saw an HTTP 500.
7. **Drain-driven credit return with a visit floor.** Measured on 2026-09-01 on an exclusive 144-core node with the in-process harness at 256 ingress peers, 42 interleaved runs:

| Change | CPU per token |
|---|---|
| Credit sweep at 2 ms against 200 ms or 500 ms, no drain hook | 1.075 times |
| Drain hook alone | +0.30 µs per token (about 2%), worse in 34 of 42 paired runs |
| Before (2 ms sweep) against after (200 ms sweep with drain hook) | 13.34 against 12.73 µs per token (−4.5%) |

The hook costs about 2%. It is what makes the 200 ms sweep safe, because the sweep was the only path that returned credit to a quiet peer. The net gain is 4.5%, and the case for the change is correctness. Without a floor on doorbell visits, a stream drained through an 8-record window rang the doorbell 1,000 times in about 270 ms. The 2 ms floor spread the same 1,000 walks over about 3 s.

The Dynamo adapter also polls the anchor inline from the connection task instead of through a consumer task with a 64-deep mailbox. This removed about 0.2 ms of adapter CPU per request.

## What is not a lever

These changes were measured or traced and did not help. They are recorded so that nobody tries them again without a new reason.

| Change | What was measured | Verdict |
|---|---|---|
| Grant credit once per half window | Credit updates per rep fell from 54–59 million to 2.8–3.1 million. Frontend batches fell from 307,000–484,000 to 7,800–10,200. CPU per request did not change. Inter-token p99 rose to 68 and 76 ms against 40 and 42 ms on the same nodes, and sender credit exhaustion rose to 1,431 per rep. | Rejected. A threshold makes a slow reader exhaust the sender sooner. |
| An urgent grant for a starved slot | A sender runs out of credit about 300 times per rep and waits under 2 ms. Long gaps rise slightly in the last four token positions (76 to 106 gaps of 63.5 million). Only 21% to 30% of credit batches wait out the reply window. | Not built. It has under 1 ms to win on about 300 of 250,000 streams, and an urgent reply per wake returns one batch per wake. |
| A 32-worker frontend runtime | CPU per request fell 1.4 to 1.8 ms. TTFT p95 at one holder rose from about 206 ms to 317–366 ms, and the p50 lead disappeared. | Rejected. velo's larger task count needs the workers. |
| Coalesce SSE flushes | hyper drains the body until `Pending` and flushes once per poll, in both planes. velo flushes about 0.9 times per record, the comparison plane about 0.54, because its body task runs further behind. | No lever. More records per flush need a linger. |
| A 500 µs data linger on the workers | Frontend inbound batches fell from 7.5–8.6 million to 1.0 million per rep, and CPU fell to 9.10 ms per request. The request path grew: client-to-frontend 8.4–9.1 ms against 5.6, frontend-to-worker 12–13 ms against 5.5. TTFT p50 72 to 76 ms. | Not a ship setting. |
| `async_open_ack` alone | TTFT p95 was worse in every rep (209, 176, 252 ms against 142, 119, 136). p50 did not improve. With zero-RTT setup it cut the response segment from 48 to 39 ms at p50, but p95 stayed worse. | Off by default. |
| Reply linger as a first-token lever | The per-request segments did not move at equal load: 3.7 against 3.9–4.0 ms, 3.3 against 3.3–3.5 ms, 47.7 against 47.8–47.9 ms. | A batch and CPU fix only. The batch inflation was a symptom of contention, not its cause. |
| Shard the frontend ingest lane | The lane's cost was the per-batch slot walk, which item 3 removed. The profile shows no core-bound lane stage. | Not built. |
| Merge the reader pump into the anchor channel | Traced. The anchor channel has other writers, and credit is issued against the mux buffer's sole writer. | Forbidden by the credit invariant. |
| `SO_REUSEPORT` on the TCP path | Traced. The kernel demuxes TCP per connection, so the frontend already has one socket, queue and reader per peer. | Not applicable. A QUIC transport needs it, because one QUIC endpoint is one UDP socket. |
| A 60 KiB against 64 KiB batch cap | Traced. Both planes write one `writev` per batch. | Equivalent. |
| Flatten the MessagePack envelope | Profiled. Decode costs the same in both planes. | No lever. |
| A larger initial credit, a connection pool per peer, a different record header | Traced. None is on the measured path. | Not built. |
| Tune UCX | See [UCX transport instability](#ucx-transport-instability). | No knob reaches the mechanism. |

## Where velo's extra CPU goes

A per-subtree partition of a `perf` profile of both frontends, with one runtime (one rep each, measured on 2026-09-06). Each stack line goes to exactly one bucket, and each plane sums to 100%. The values are milliseconds per request.

| Bucket | velo | Comparison | Difference |
|---|---|---|---|
| HTTP and SSE connection task, socket writes | 7.51 | 5.03 | +2.48 |
| Reader pump (per-stream relay task) | 1.11 | 0 | +1.11 |
| Anchor (frame decode, gauge) | 0.79 | 0 | +0.79 |
| Adapter consumer task | 0.66 | 0 | +0.66 |
| Ingress lane, `handle_batch` | 0.65 | 0 | +0.65 |
| TCP transport, dispatch, batcher and credit | 0.50 | 0 | +0.50 |
| Tokio scheduler residual | 0.78 | 0.33 | +0.46 |
| Unattributed | 2.37 | 2.00 | +0.37 |
| Shared request path (SSE, router, preprocessor) | 3.77 | 3.62 | +0.15 |
| Comparison plane's reader and receiver | 0 | 1.24 | −1.24 |
| axum graceful-shutdown watch | 1.36 | 2.21 | −0.84 |
| Idle | 4.04 | 4.66 | −0.62 |

velo's anchor and adapter consumer (1.46) cost about what the comparison plane's reader and receiver cost (1.24). The excess is task hops. The reader pump is a channel-to-channel relay with no counterpart, and the lane, dispatch and transport are three stages where the comparison plane has one task. Each record also reaches the HTTP connection task as its own wake.

After the inline receiver, a second profile put the velo-only buckets at 3.20 ms per request. The buckets were: reader pump 1.01, anchor 0.85, ingress 0.51, adapter 0.49, TCP 0.18, dispatch 0.14, batcher 0.01. The reader pump and the anchor channel are the remaining addressable surplus. The pump cannot merge into the anchor channel (see the table above). Each other hop-chain change is optional and needs a same-matrix tail check.

Before the fixes above, these symbols appeared only in velo's profile, as a share of the 72 frontend cores:

| Symbol | Share | Source | State |
|---|---|---|---|
| `flume::Shared<T>::len` | 2.09% | Per-batch walk of every slot, reading each channel length under its lock | Removed |
| `parking_lot` lock slow paths | 1.58% | A `timeout` per received record in the reader pump | Removed |
| `flume::Sender<T>::try_send` | 0.93% | Delivery of each record into its anchor channel | Inherent |
| `set_active_anchor_gauge` | 0.49% | The gauge recounts the anchor registry on each create and retire | Open |
| `CancellationToken::is_cancelled` | 0.43% | Per-record checks on the delivery path | Open |

These costs are counted from source, not measured:

- **Accept-window tasks.** Each bind spawns one task that sleeps for the 60-second accept window, and a claim does not cancel it. At 3,000 attaches per second, about 180,000 such tasks and timers are live. This is a memory and task-count cost, not a per-record one.
- **The per-record copy.** Ingress copies each record body into a `Vec` (one allocation and one `memcpy`, estimated at 35 to 55 ns). Removing it needs `Bytes` from the slot buffer through the anchor channel.
- **Wakes.** `flume` fires a waker only for a parked receiver, so k records for one slot in one batch already cost one wake. The cost that remains is the two-hop structure: slot buffer to reader pump, then anchor channel to consumer.

## First-token mechanisms

A per-request join splits client TTFT into three segments. The join matches `aiperf`'s request id, the frontend's log and the mocker's log, with clock skew below 1 ms. The three segments sum to the client's TTFT.

- **A**: client send to the frontend's "request received" log line (HTTP ingress).
- **B**: the frontend to the worker handler's start (the request plane).
- **C**: worker ingress to the first token at the client (mostly the response plane).

TTFT equals the wait for the first HTTP byte to within 0.3 ms at every percentile, in every plane. No plane writes a byte before the first token.

These mechanisms were found:

- **A saturated load-generator node looked like a slow plane.** Before core pinning, velo's TTFT p50 read about 1.1 s. The queues that velo owns held a small share of it: 26 to 36 ms in the node-global inbound queue, about 110 ms in the ordered lanes and 79 ms in the worker's egress queue. The rest was the time between a wake and a run on a node at 95% utilization. Pinning moved velo's p50 to 98 ms at the same throughput.
- **The attach round trip was the largest term.** Before zero-RTT setup, the worker waited for `_anchor_attach` before it started generating. Under load before pinning, that round trip averaged 390 to 524 ms. With pinning it averaged 13 to 22 ms. Zero-RTT setup removes it.
- **The comparison plane has nothing on the first-record path that grows with load.** The frontend mints the stream id at request registration. The prologue goes on an urgent lane that the writer drains before ordered data. The only shared queue that a first frame crosses is bounded by a 256 KiB byte budget per connection.
- **Zero-RTT setup first moved the cost into the request path.** With two frontend runtimes, segments A and B each grew by about 8 to 9 ms under zero-RTT setup, while C fell. The frontend sent ten times more batches, one per credit reply. The reply linger removed the extra batches but not the A and B growth, so the batches were a symptom of frontend contention. After the one-runtime fix, velo's p50 moved ahead of the comparison plane, and segment B contributes nothing to the steady-state tail.
- **TTFT p50 followed the backlog draw.** Before the one-runtime fix, velo's p50 was 54 to 55 ms at one holder, 61 to 62 at two, 73 to 76 at three and 83 at five. The comparison plane stayed at 48 to 49 ms. After the fix, velo's p50 is 39 to 45 ms at one to three holders.
- **The steady-state tail is the hot mocker process and HTTP ingress, in both planes.** In steady state, segment B contributes nothing to p99. Segment C adds 125 to 209 ms, and 59% to 89% of the p90-to-p99 band sits on one mocker process with 3,000 to 6,400 requests in flight. Segment A adds 39 to 112 ms. The raw p99 is the opening burst, described in [Benchmarking](benchmarking.md#steady-state-and-raw-percentiles).
- **End-to-end and ITL tails measure one mocker process.** Arrivals are equal across the 8 mocker processes. In-flight counts are not: seven sit at 150 to 200 and one at 3,000 to 7,000 for the whole run, in every plane. By Little's law, a request on the hot process stays about 15 s against 0.43 s elsewhere. End-to-end p99 and ITL p99 therefore measure which process holds the backlog, not the plane.
- **velo's tail discipline is conditional.** Before pinning, velo appeared to keep a much shorter end-to-end tail. That was a starved frontend limiting how many streams ran at once. With the frontend on 48 cores and the load generator on 96, velo held 8.1 s end-to-end p99 at 3,008 req/s against the comparison plane's 25.2 s at 2,460. On a 72/72 split, both planes posted about 11.5 s.

## UCX transport instability

Measured on 2026-09-04, before core pinning: the velo mux over velo's UCX transport on InfiniBand (2 mlx5 adapters per node), against the same mux over TCP on 200G Ethernet. Three reps each, 512 workers, concurrency 8,192.

| Transport | req/s per rep | TTFT p99 ms per rep | Errors |
|---|---|---|---|
| TCP | 3,017 / 2,847 / 2,684 | 2,124 / 2,107 / 2,531 | 0 |
| UCX | 2,526 / 2,875 / 2,355 | 9,152 / 2,016 / 7,678 | 1,722 in rep 1 |

UCX did not beat TCP, and it did not hold its tail. The failure chain:

1. The KV router put about 5,760 concurrent streams on one of the eight mocker processes. This imbalance occurs with every transport.
2. All 64 workers in a process share one velo node and one mux peer link. All 1,722 failures were in that one process, spread evenly over its 64 workers.
3. Over UCX, that one link fell behind. On the hot process, the delay from worker completion to client completion grew to a median of 18.87 s. Over TCP, at the same or higher concurrency, it peaked at 1.0 to 2.75 s.
4. Heartbeats share the per-peer queue with data, so streams behind more than 15 s of backlog went silent at the frontend.
5. The reader-pump watchdog (3 × 5 s) killed 1,722 streams. Every log line showed `anchor_frame_tx_len=0` and `transport_rx_len=0`. `closed_slot` drops were 1,722 × 255 exactly, so no record had arrived before each kill.
6. The worker was healthy the whole time. It completed all 27,702 of its requests.

The defect is that the UCX send path has no backpressure edge beyond its shared ring:

- `ucp_am_send_nbx` never refuses. An exhausted endpoint returns a request pointer, not an error.
- `inflight_ops` counts posted operations but does not gate admission.
- The per-peer admission gate queues without a bound.
- All peers share one 1,024-entry ring into the progress thread. TCP gives each connection a 256-entry channel and a blocking write.

Because admission never blocks, the on-admission flush policy never parks, and batches collapse. UCX carried 6.97 to 8.65 records per batch against 18 to 29 over TCP with the same settings. That is 2.6 to 4.1 times the message rate for the same records.

These alternatives were checked and refuted:

- Send-backpressure counters do not follow the failure. Their totals ran opposite to the instability across reps 2 and 3.
- The peer byte budget (8 MiB over 5,760 slots) does not depend on the transport. The TCP control carried 5,326 streams on the same budget and held p99 at 2.5 s.
- Batch collapse amplifies the failure but does not start it. The clean rep had fully collapsed batches too.
- UCX scheduling is not unfair. The failed peer carried eight times the load of the others.
- Spinning progress threads do not starve the CPU. There were 9 progress threads on 288 cores.

No tuning setting reaches this mechanism. The fix has two parts. The UCX transport needs a backpressure edge (gate admission on in-flight operations or on a per-peer ring share). Liveness needs a heartbeat path that data cannot block, or a watchdog that can tell a starved stream from a dead one. The UCX inbound path now records frames like every other transport. The other two fixes are not built. [RDMA performance](rdma-performance.md) covers the UCX transport outside the response plane.

## Instrumentation cost

These instruments are on the hot path. Their costs are accepted without an A/B measurement:

- **Egress writer instruments** (`velo_transport_egress_queue_wait_seconds`, `velo_transport_frames_written_total`, `velo_transport_write_duration_seconds`, TCP and UDS only). Per frame: one `Instant::now()` at send, then one `elapsed()` and one histogram observe at dequeue. Per write: a second observe and up to five counter compare-and-swap loops. If this cost shows in a measurement, observe once per write on the oldest frame instead of once per frame.
- **Ordered-lane metrics for `_stream_batch`.** Per batch: four `Arc` clones, one `elapsed()` with one histogram observe, and one gauge increment and decrement. This cost is the measurement of the lane wait itself, so no A/B applies.
- **Batcher counters** (`velo_streaming_mux_records_sent_total`, `velo_streaming_mux_batcher_wakes_total`). The collectors are pre-bound into arrays. Per record: one `u16` increment. Per wake: one `AtomicF64` compare-and-swap. `velo_streaming_mux_staged_records` already paid one gauge compare-and-swap per staged record.

The rule for new instruments on these paths: no label lookup per record. Pre-bind the collectors, and observe per batch where possible.

The instruments did not perturb the rig. Three instrumented reps and one control rep stayed inside the historical band. One outlier rep was a hot mocker process with 1,887 live slots, not the instruments.

## Findings outside velo

- **axum's graceful-shutdown watch** costs 1.4 to 2.2 ms per request in both planes. It is one process-wide lock, polled again on every connection wake. It tracks the backlog draw, not the plane.
- **Two tokio runtimes in the frontend.** See item 1 of [What produced the result](#what-produced-the-result). The fix belongs in Dynamo's Python bindings and helps every response plane.
- **`aiperf` 0.10.0 can wait forever** when a record references a dataset entry that fails to decode. It then writes no export. The rig patches it to finish after 15 report intervals with no progress, once all credits are complete and at least 95% of records are processed. The patch fired on runs that missed 2 to 397 of 250,000 records.
- **Mixed model cards stop a mocker fleet silently.** Packed workers with `DYN_SYSTEM_PORT` set publish a mix of self-hosted and fallback model cards. Dynamo's discovery controller parks them in a `Conflict` state with no log line, and the fleet never serves. Set `DYN_SELF_HOST_METADATA=0` and leave `DYN_SYSTEM_PORT` unset.
- **The socket buffer clamp.** velo requests 2 MiB socket buffers. This cluster's `net.core.rmem_max` and `wmem_max` are 212,992 bytes and cannot be raised, which clamps each buffer to about 208 KiB and disables autotuning. At 0.5 ms round-trip time, one connection carries at most about 400 MB/s. The rig's worst-case mux traffic is about 200 MB/s in total, so the clamp does not bind with 4 or more worker processes per node.
