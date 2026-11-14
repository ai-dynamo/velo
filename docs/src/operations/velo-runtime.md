# A runtime for velo's own tasks

This chapter records whether velo should run its own tasks on a tokio runtime of its own, and what that costs. It gives the stall that motivates the question, four arms, and their measured cost at five loads.

## The question

velo's tasks run on the application's runtime by default. When an application task wakes a parked velo task from a worker thread, tokio puts the velo task in that worker's LIFO slot, and no other worker steals from the slot. If the application task then holds its thread (a blocking call, a long encode), the velo task waits for the hold to end.

Four wakes are exposed this way:

| Row | The application task | wakes | and the hold delays |
|---|---|---|---|
| 1 | calls stream `send` | the peer's batcher | every record and heartbeat on that peer's lane |
| 2 | makes a unary call | the connection's writer | the request, and later messages on that connection |
| 3 | an ordered handler returns its response, and the next call on its lane blocks | the connection's writer | the response |
| 4 | a consumer reads a record | the node-wide credit sweep | the credit return, so the next record of a stream out of credit |

A runtime that the application cannot hold removes the stall. It also makes each of those wakes a wake across threads, which costs time and CPU. The measurements below put a number on that cost.

## The arms

| Arm | Where velo's tasks run |
|---|---|
| A | Everything on the application's runtime. This is the default. |
| B1, B2 | The mux (its batchers, its credit sweep and its batch lanes) on a runtime of its own with 1 or 2 threads. Transports and the inbound loop stay on the application's runtime. |
| C1, C2, C4 | One velo runtime with 1, 2 or 4 threads for the transports, the inbound loop and the mux. Handlers stay on the application's runtime. |
| D | As A, but a stream `send` yields once (`tokio::task::yield_now`) when it woke a parked batcher. |

Each node builds its own runtime in `VeloBuilder::build`, so a process with N nodes has N velo runtimes. One measurement build selects the arm at run time, so every arm ran the same binary.

## Setup

- Host: one aarch64 workstation, 72 Neoverse-V2 cores, Linux 6.17, governor `performance`, exclusive for the run. Rust 1.99.1, tokio 1.54.0.
- Pinning: each process ran under `taskset -c 8-35`. The application's runtime had 4 worker threads, pinned with the main thread to cores 8 to 11. velo's runtime threads were pinned to cores 12 to 35. Arm A has no velo threads.
- Order: 10 reps of each cell. Within a rep, every arm ran once, and the arm that ran first rotated from cell to cell and rep to rep.
- Host record: before each run, the busy CPU over 3 s (median 0.02 cores, maximum 2.3). A run during which another process used 100 ms of CPU or more was run again. 24 of 840 runs were.
- Counters: process CPU comes from `CLOCK_PROCESS_CPUTIME_ID`. Thread parks are the voluntary context switches, summed over `/proc/self/task/*/status`. Batcher wakes and records per batch come from velo's mux metrics.
- Tables give the median of 10 reps, with the range in parentheses where it matters.

The loads, all over loopback TCP with the mux:

| Load | Harness and flags |
|---|---|
| One stream, closed loop | `response_plane_bench --load closed --streams 1 --records 20000 --warmup-records 2000 --flush-policy auto --credit-sweep-interval-ms 200` |
| 64 streams, closed loop | the same with `--streams 64 --records 2000 --warmup-records 200` |
| 64 streams from 4 peers | the same with `--peers 4` |
| Flood | `--load flood --streams 64 --records 5000 --warmup-records 500` |
| Unary, 1 and 64 callers | `tx_budget --rungs l4t --sizes 1024 --inflight 1` (20,000 calls) and `--inflight 64` (64,000 calls) |
| Serving shape | `response_plane_bench --anchor-hosts 2 --engines 8 --requests 2000 --credit-sweep-interval-ms 200 --warmup-requests 200` |
| Saturated | the one-stream, 64-stream and unary loads with `--saturate 8`: 8 tasks on the application's runtime that each spin for 50 µs and then yield |

In a closed loop, each stream sends one record and waits until the consumer has it before it sends the next. Latency is one way, from `send` to the consumer's read. In `l4t`, latency is the round trip of a unary call to an echo handler. Every arm shared the flags of its load and added `--arm <arm> --app-threads 4 --app-cpus 8-11 --velo-cpus 12-35`.

## Results

### Stalls

Each row ran 50 times per arm with a hold of 100 ms. A run stalled when the woken work took at least half the hold. A stalled run waited for the whole hold, about 100 to 103 ms.

| Row | A | B1, B2 | C1, C2, C4 | D |
|---|---|---|---|---|
| 1, stream `send` | 50/50 | 0/50 | 0/50 | 0/50 |
| 2, unary call | 50/50 | 50/50 | 0/50 | 50/50 |
| 3, handler response | 50/50 | 50/50 | 0/50 | 50/50 |
| 4, read of a record already waiting | 50/50 | 0/50 | 0/50 | 50/50 |
| 4, read as soon as the record arrives | 0/50 | 0/50 | 0/50 | 0/50 |

Only the C arms remove all four stalls. B removes the two wakes that reach the mux. D removes only row 1.

Row 4 depends on when the consumer reads. A consumer that reads a record as soon as it arrives did not stall the sweep in any arm. Two causes fit that result. With the default `drain_visit_floor` of 2 ms, a visit that comes too soon after the last one is put off to a timer, and the timer fires on whichever worker drives the clock. And such a consumer is usually the third task in a chain of LIFO wakes (connection reader, inbound loop, batch lane), and tokio turns the LIFO slot off after three LIFO polls in a row. With the floor and `reply_linger` at zero, the read still did not stall, so the floor is not the only cause. The LIFO limit fits, but this run did not test it directly. A consumer that comes back to records already waiting has no such chain, and it stalled in every run of arm A.

The tests are `lib/velo/tests/streaming/stall_rows.rs` on the measurement build. They report a rate and do not assert, because a test cannot force a tokio scheduling outcome by arranging wakes: another wake can reach the task first.

### Cost at each load

| Load | Measure | A | B1 | B2 | C1 | C2 | C4 | D |
|---|---|---|---|---|---|---|---|---|
| One stream | p50 latency (µs) | 37.2 | 40.8 | 42.2 | 32.9 | 33.5 | 33.8 | 40.3 |
| | p99 latency (µs) | 44.2 | 46.6 | 48.1 | 53.0 | 66.5 | 65.5 | 47.1 |
| | CPU per record (µs) | 46.2 | 62.3 | 74.5 | 41.9 | 47.6 | 47.9 | 56.9 |
| | thread parks per record | 2.3 | 6.0 | 8.1 | 4.0 | 5.1 | 5.1 | 3.4 |
| 64 streams | records per second (thousands) | 347 | 378 | 350 | 487 | 443 | 427 | 349 |
| | p99 latency (µs) | 270 | 278 | 277 | 151 | 252 | 320 | 271 |
| | CPU per record (µs) | 7.45 | 7.54 | 8.03 | 6.46 | 7.00 | 7.25 | 7.70 |
| 64 streams, 4 peers | records per second (thousands) | 335 | 541 | 559 | 592 | 855 | 831 | 273 |
| | p99 latency (µs) | 347 | 205 | 193 | 162 | 117 | 124 | 331 |
| | CPU per record (µs) | 11.8 | 10.4 | 11.3 | 8.38 | 9.26 | 10.3 | 14.6 |
| Flood, 64 streams | records per second (thousands) | 441 | 422 | 419 | 379 | 384 | 384 | 452 |
| | CPU per record (µs) | 4.78 | 5.25 | 5.28 | 5.29 | 5.29 | 5.22 | 4.75 |
| Unary, 1 caller | p50 round trip (µs) | 15.7 | 15.3 | 15.8 | 55.7 | 63.6 | 63.6 | 15.8 |
| | CPU per call (µs) | 16.0 | 15.7 | 16.1 | 68.2 | 85.3 | 85.4 | 16.1 |
| | thread parks per call | 0 | 0 | 0 | 8 | 10 | 10 | 0 |
| Unary, 64 callers | calls per second (thousands) | 223 | 222 | 223 | 275 | 367 | 428 | 223 |
| | p99 round trip (µs) | 449 | 459 | 443 | 341 | 243 | 215 | 450 |
| | CPU per call (µs) | 16.0 | 16.0 | 16.0 | 16.5 | 19.2 | 20.0 | 16.0 |
| Serving shape | CPU per token (µs) | 10.0 | 12.5 | 14.0 | 12.9 | 15.4 | 17.2 | 13.1 |
| | inter-token p50 (µs) | 2,137 | 2,359 | 2,356 | 2,409 | 2,419 | 2,414 | 2,687 |
| | inter-token p99 (µs) | 3,337 | 3,050 | 3,046 | 3,166 | 3,090 | 3,081 | 3,540 |
| | batcher wakes per token | 0.091 | 0.091 | 0.090 | 0.094 | 0.092 | 0.094 | 1.05 |

Unless the text says otherwise, each difference named below has ranges that do not overlap. In 7 to 9 of 10 reps of the C arms at one stream, p99.9 was about 1.1 ms, against about 50 µs in arms A, B and D. The cause was not measured. 1 ms is the default `reply_linger`, so a credit return that waits out the linger is one candidate.

### A saturated application runtime

| Load | Measure | A | B1 | C1 | C2 | D |
|---|---|---|---|---|---|---|
| One stream | p50 / p99 latency (µs) | 60.9 / 113 | 65.9 / 127 | 51.1 / 54.5 | 51.2 / 54.5 | 57.4 / 120 |
| 64 streams | records per second (thousands) | 286 | 289 | 432 | 394 | 286 |
| | p99 latency (µs) | 345 | 345 | 225 | 278 | 345 |
| Unary, 1 caller | p50 / p99 round trip (µs) | 86.6 / 160 | 86.3 / 158 | 104.7 / 107.5 | 104.8 / 107.6 | 86.7 / 158 |
| Unary, 64 callers | calls per second (thousands) | 175 | 174 | 273 | 341 | 175 |
| | p99 round trip (µs) | 532 | 534 | 354 | 288 | 536 |

CPU per record under saturation is mostly the spinning tasks', so it is left out.

A wake from velo's runtime into the busy application runtime did not wait for tokio's shared-queue check interval (about 200 µs of polling in tokio 1.54). The saturating tasks yield with `yield_now`, which defers them, so each worker's local queue empties after every 50 µs slice, and the worker then takes from the shared queue. So the delay these numbers show is probably at most one slice. A workload whose tasks keep the local queue full would check the shared queue only at the interval. This run did not measure that case.

## What the numbers show

- **One crossing at low load is expensive.** A unary call at one caller takes 15.7 µs on arm A, because the whole round trip runs on one worker with no thread park. Arm C adds four wakes across threads per round trip (8 to 10 parks), and the call takes 55.7 to 63.6 µs and 4.3 to 5.3 times the CPU.
- **At one stream, arm C costs little.** C1 has a lower p50 (32.9 µs against 37.2 µs) and less CPU per record than A. Its p99 is higher (53 to 67 µs against 44 µs), but the ranges overlap. B1 adds about 4 parks and 16 µs of CPU per record: the mux sits on one runtime and the connection writer on another.
- **Under concurrency, arm C wins.** At 64 streams C1 moves 40% more records than A at a lower p99 and 13% less CPU per record. With 4 busy peers C2 moves 2.5 times as many. At 64 unary callers C4 serves 1.9 times as many calls as A.
- **Flood and the serving shape favor A.** In a flood, the C arms move 13% to 14% fewer records at 10% more CPU. In the serving shape, every extra runtime adds 25% to 72% CPU per token and 10% to 13% to the inter-token p50, but lowers the inter-token p99 by 5% to 9%.
- **Threads past the number of busy peer lanes do not help.** With one busy peer lane, C1 beats C2 and C4 in throughput and p99. With four, C2 is best and C4 is no better. CPU per record rises from one thread to two in every load but the flood, where it does not move.
- **A saturated application runtime hurts A more than C.** At one stream, A's p99 rises from 44 µs to 113 µs, and C's stays at 54.5 µs. At one unary caller, C's p50 is 18 µs slower than A's, but its p99 is 52 µs lower.
- **D is not a fix.** It removes only the stream-send stall. It wakes the batcher about once per record instead of once per 11 records in the serving shape, which costs 31% more CPU per token and 15% more time to first token (p50 311 ms against 270 ms).

## Rejected alternatives

- **The mux alone on its own runtime (arm B).** It removes rows 1 and 4 only. At one stream it costs 35% more CPU per record than A. Where C helps (64 streams, 64 unary callers), B helps less or not at all. Where C loses (one unary caller, flood), B is close to A, because it leaves the transports and the inbound loop alone.
- **A yield after the wake (arm D).** It removes row 1 only, and it costs CPU and latency at every load that batches.
- **A `LocalSet` for velo's tasks.** It is `!Send` and must be driven by one thread. A wake from another thread goes to its remote queue and wakes that thread (tokio `task/local.rs`), as the wakes into arm C's runtime do. On a thread of its own it is a one-thread private runtime, which is arm C1, and on an application worker it can be held like arm A. So it is not a separate arm.
- **Stealing from the LIFO slot.** tokio 1.51.0 made the slot stealable, and 1.51.2 and 1.52.2 reverted that for its cost. `disable_lifo_slot` needs `tokio_unstable`.

## Reproduce

The commands above ran on a measurement build. The run script ran each cell for each arm in the order given in [Setup](#setup), and kept every run's output and host record. Follow the [measurement rules](benchmarking.md#measurement-rules): an exclusive node, both arms from one build, interleaved, at least 5 reps. Report latency beside throughput.
