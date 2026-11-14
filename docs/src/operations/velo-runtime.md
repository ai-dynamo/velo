# A runtime for velo's own tasks

velo runs its own tasks on a tokio runtime that it builds, `TaskRuntime::Dedicated { threads: 1 }`, unless the application opts out with `VeloBuilder::task_runtime(TaskRuntime::Application)`. This chapter gives the reason, the two sets of measurements behind the default, what they did not cover, and the questions they left open.

- **The default removes a stall.** On the application's runtime, an application task that wakes a velo task and then holds its thread delays that velo task for the whole hold. On the dedicated runtime no such hold delayed velo.
- **In dynamo's load test the default cost less.** dynamo's frontend used 6% to 25% less CPU per output token, with throughput and latency the same as on the application's runtime.
- **A single unary caller pays.** In velo's own benchmark, one caller making one unary call at a time took 3.5 times as long per call, at 4.3 times the CPU.
- **Opt out** with `TaskRuntime::Application` when latency depends on unary calls made one at a time, as in dynamo-gdl and kvbm. Measure both settings first.
- **Use one thread.** A second thread helped only with more busy peer lanes than threads. In dynamo's test it was worse than one everywhere.

## The stall

On the application's runtime, when an application task wakes a parked velo task from a worker thread, tokio puts the velo task in that worker's LIFO slot. No other worker steals from the slot. If the application task then holds its thread (a blocking call, a long encode), the velo task waits until the hold ends.

Four wakes are exposed this way:

| Row | The application task | wakes | and the hold delays |
|---|---|---|---|
| 1 | calls stream `send` | the peer's batcher | every record and heartbeat on that peer's lane |
| 2 | makes a unary call | the connection's writer | the request, and later messages on that connection |
| 3 | an ordered handler returns its response, and the next call on its lane blocks | the connection's writer | the response |
| 4 | a consumer reads a record | the node-wide credit sweep | the credit return, so the next record of a stream that is out of credit |

When every worker of the application's runtime is held, three more things stop: velo's reads from its sockets, the producer's heartbeats (so the consumer's watchdog ends the stream with `Dropped`), and velo's own handlers, which answer a peer's first contact.

On a runtime that the application cannot hold, none of these waits. The cost is that each wake between the application and velo becomes a wake across threads. [Known limitations](../appendix/limitations.md#a-task-that-holds-its-worker-thread-stalls-velos-tasks) describes the stall and the rule an application must follow when it opts out.

### Hand-offs per unary call

On the default, a unary call to a handler hands off between the two runtimes four times:

1. The caller hands the request to the client's connection writer, which runs on velo's runtime.
2. The server's inbound loop, on velo's runtime, passes the call to the handler on the application's runtime.
3. The handler hands its response to the server's connection writer, on velo's runtime.
4. The client's response path, on velo's runtime, wakes the caller.

On `TaskRuntime::Application` the whole call runs on the application's runtime. The tests `a_unary_call_crosses_between_runtimes_four_times` and `a_unary_call_on_the_application_runtime_stays_there` in `lib/velo/tests/task_runtime.rs` count these hand-offs by thread.

## The arms

Both data sets ran on a measurement build that selects, at run time, where velo's tasks run:

| Arm | Where velo's tasks run | Shipped setting |
|---|---|---|
| A | Everything on the application's runtime | `TaskRuntime::Application` |
| C1, C2 | One velo runtime with 1 or 2 threads for the transports, the inbound loop, the mux and its batch lanes | `TaskRuntime::Dedicated { threads: 1 }` (the default) and `{ threads: 2 }` |
| C4 | The same with 4 threads | None: `build` refuses more than 2 |
| B1, B2 | The mux (its batchers, its credit sweep and its batch lanes) on a runtime of its own with 1 or 2 threads | None, rejected below |
| D | As A, but a stream `send` yields once (`tokio::task::yield_now`) when it woke a parked batcher | None, rejected below |

The shipped dedicated runtime moves more than arm C did. It also runs the streams' heartbeats, watchdogs and deadline sweeps, and velo's own handlers (stream attach and control, rendezvous, events, first contact). In arm C these stayed on the application's runtime. Neither data set measured the shipped code.

Each node builds its own runtime in `VeloBuilder::build`, so a process with N nodes has N velo runtimes.

## velo's own benchmarks

### Setup

- Host: one aarch64 workstation with 72 Neoverse-V2 cores, Linux 6.17, governor `performance`, exclusive for the run. Rust 1.99.1, tokio 1.54.0.
- Pinning: each process ran under `taskset -c 8-35`. The application's runtime had 4 worker threads, pinned with the main thread to cores 8 to 11. velo's runtime threads were pinned to cores 12 to 35. The shipped runtime does not pin its threads.
- Order: 10 reps of each cell. Within a rep, every arm ran once, and the arm that ran first rotated from cell to cell and rep to rep.
- Host record: before each run, the busy CPU over 3 s (median 0.02 cores, maximum 2.3). A run during which another process used 100 ms of CPU or more was run again. 24 of 840 runs were.
- Counters: process CPU comes from `CLOCK_PROCESS_CPUTIME_ID`. Thread parks are the voluntary context switches, summed over `/proc/self/task/*/status`. Batcher wakes and records per batch come from velo's mux metrics.
- Tables give the median of 10 reps, with the range in parentheses where it matters. Unless the text says otherwise, each difference named below has ranges that do not overlap.

The loads, all over loopback TCP with the mux, with both nodes in one process (see [Limits of the in-process harness](benchmarking.md#limits-of-the-in-process-harness)):

| Load | Harness and flags |
|---|---|
| One stream, closed loop | `response_plane_bench --load closed --streams 1 --records 20000 --warmup-records 2000 --flush-policy auto --credit-sweep-interval-ms 200` |
| 64 streams, closed loop | the same with `--streams 64 --records 2000 --warmup-records 200` |
| 64 streams from 4 peers | the same with `--peers 4` |
| Flood | `--load flood --streams 64 --records 5000 --warmup-records 500` |
| Unary, 1 and 64 callers | `tx_budget --rungs l4t --sizes 1024 --inflight 1` (20,000 calls) and `--inflight 64` (64,000 calls) |
| Serving shape | `response_plane_bench --anchor-hosts 2 --engines 8 --requests 2000 --credit-sweep-interval-ms 200 --warmup-requests 200` |
| Saturated | the one-stream, 64-stream and unary loads with `--saturate 8`: 8 tasks on the application's runtime that each spin for 50 µs and then call `yield_now` |

In a closed loop, each stream sends one record and waits until the consumer has it before it sends the next. Latency is one way, from `send` to the consumer's read. In `l4t`, latency is the round trip of a unary call to an echo handler. Every arm shared the flags of its load and added `--arm <arm> --app-threads 4 --app-cpus 8-11 --velo-cpus 12-35`. The `--arm`, `--app-threads`, `--app-cpus`, `--velo-cpus` and `--saturate` flags and the `l4t` rung exist only in the measurement build.

### Stalls

Each row ran 50 times per arm with a hold of 100 ms. A run stalled when the woken work took at least half the hold. A stalled run waited for the whole hold, about 100 to 103 ms.

| Row | A | B1, B2 | C1, C2, C4 | D |
|---|---|---|---|---|
| 1, stream `send` | 50/50 | 0/50 | 0/50 | 0/50 |
| 2, unary call | 50/50 | 50/50 | 0/50 | 50/50 |
| 3, handler response | 50/50 | 50/50 | 0/50 | 50/50 |
| 4, read of a record already waiting | 50/50 | 0/50 | 0/50 | 50/50 |
| 4, read as soon as the record arrives | 0/50 | 0/50 | 0/50 | 0/50 |

Only the C arms removed all four stalls. B removed the two wakes that reach the mux. D removed only row 1. Row 4 stalled only when the consumer came back to a record that was already waiting. See the [open questions](#open-questions).

On the shipped code, `lib/velo/tests/streaming/stall_rows.rs` holds a live test for each row, and for the three rows of a fully held runtime, on the default. Each asserts that velo does not wait for the hold. The same rows on `TaskRuntime::Application` are ignored demonstrations (`--ignored`), because a test cannot force the stall: another wake can reach the velo task first ([Coding gotchas](../development/coding-gotchas.md#a-test-cannot-force-a-tokio-scheduling-outcome-by-arranging-wakes)).

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
| | thread parks per token | 0.105 | 0.295 | 0.468 | 0.439 | 0.557 | 0.690 | 0.165 |
| | inter-token p50 (µs) | 2,137 | 2,359 | 2,356 | 2,409 | 2,419 | 2,414 | 2,687 |
| | inter-token p99 (µs) | 3,337 | 3,050 | 3,046 | 3,166 | 3,090 | 3,081 | 3,540 |
| | batcher wakes per token | 0.091 | 0.091 | 0.090 | 0.094 | 0.092 | 0.094 | 1.05 |

In the serving shape every arm wrote the same 15.1 tokens per write, so the extra CPU per token is overhead, not extra work.

At one stream the C arms had a p99.9 of about 1.1 ms in 9 (C1), 7 (C2) and 6 (C4) of 10 reps. Arm A had it in 1 of 10 reps, and B and D in none. The other reps of every arm stayed under 75 µs. The cause is not known. See the [open questions](#open-questions).

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

A wake from velo's runtime into the busy application runtime goes to tokio's shared queue. It did not wait for tokio's shared-queue interval, which tokio 1.54 tunes to about 200 µs of polling. The tokio code explains why. A worker takes from the shared queue whenever its local queue is empty, not only at the interval. And `yield_now` does not put a task back in the local queue: tokio holds it until the worker next parks. So once a worker's spinning tasks have all yielded, its local queue is empty and it takes the waiting task. The measured cost fits: under saturation, C1's single-caller p50 rose by 49 µs (55.7 to 104.7) and its p99 by 47 µs, about one 50 µs slice. Tasks that never yield, or that keep the local queue full, would leave the shared queue to the interval. That case was not measured.

### What the numbers show

- **One crossing at low load is expensive.** A unary call at one caller takes 15.7 µs on arm A: the whole round trip runs on one worker with no thread park. C1 adds the four hand-offs above, and the call takes 55.7 µs at 4.3 times the CPU. Both nodes ran in one process, each with its own velo runtime, so in C1 each socket read also woke a parked velo thread. Between two hosts a socket read wakes a thread on the receiving host in every arm. How much of the gap remains between hosts was not measured.
- **At one stream, C1 costs little.** It has a lower p50 than A (32.9 µs against 37.2 µs) and 9% less CPU per record. Its p99 is higher (53 µs against 44 µs), but the ranges overlap.
- **Under concurrency, the C arms win.** At 64 streams, C1 moves 40% more records than A, at a lower p99 and 13% less CPU per record. With 4 busy peers, C2 moves 2.6 times as many. At 64 unary callers, C1 serves 1.2 times as many calls as A, and C2 serves 1.6 times as many.
- **Flood and the serving shape favor A.** In a flood, the C arms move 13% to 14% fewer records at 9% to 11% more CPU. In the serving shape, C1 uses 29% more CPU per token than A and C2 54% more. The inter-token p50 rises 13%, and the p99 falls 5% to 7%.
- **Threads past the number of busy peer lanes do not help.** With one busy peer lane, C1 beats C2 and C4 in throughput and p99. With four, C2 is best and C4 is no better. CPU per record rises from one thread to two in every load but the flood, where it does not move.
- **A saturated application runtime hurts A more than C.** At one stream, A's p99 rises from 44 µs to 113 µs, and C1's stays at 54.5 µs. At one unary caller, C1's p50 is 18 µs slower than A's, but its p99 is 52 µs lower.

## dynamo's load test

### Setup

- Host: the same kind of 72-core aarch64 node, Linux 6.17, governor `performance`, with no other load during the runs.
- Processes: dynamo's frontend (`python -m dynamo.frontend`) and 4 mock engine workers (`python -m dynamo.mocker`), which simulate an engine's timing for Qwen3-0.6B and use no GPU. The response plane was velo (`DYN_RESPONSE_PLANE=velo`), with the request plane on tcp, file discovery and the zmq event plane. dynamo used velo for its responses only, so velo carried no unary calls.
- Pinning: the frontend on cores 0 to 15, the 4 mock workers together on cores 16 to 31, and the load generator on cores 32 to 63. velo's threads were not pinned apart from their process.
- Load: aiperf, chat completions with streaming, 1024 input tokens and exactly 256 output tokens per request.
- Two token spacings. The main matrix used mocker speed-up 10, a token every 0.56 to 1.4 ms, at concurrency 1, 8, 64 and 256 (200, 1,500, 9,600 and 20,000 requests). A supplement used speed-up 1, a token every 5.8 to 8 ms, which is close to real serving, at concurrency 8 and 64 (160 and 1,000 requests).
- Order: 5 reps. Each rep started a fresh frontend and engines for each arm, warmed up, then ran every concurrency. The arm that ran first rotated from rep to rep.
- Counters: CPU per output token is each process group's CPU time from `/proc` over the run, divided by the output tokens. Thread parks are voluntary context switches. velo's use was checked by its metrics and its `velo-rt` threads.
- Arms A, C1 and C2, chosen by an environment variable on the measurement build.
- Tables give the median of 5 reps. The change against A is the median of the per-rep ratios.

### Results

| Token spacing | Concurrency | Output tokens/s, A | Frontend CPU per token, A (µs) | C1 against A | C2 against A |
|---|---|---|---|---|---|
| 0.56 to 1.4 ms | 1 | 1,705 | 40.1 | −12.5% | −4.7% |
| | 8 | 13,069 | 34.3 | −9.6% | −1.5% |
| | 64 | 81,704 | 27.9 | −11.8% | −6.9% |
| | 256 | 181,417 | 27.2 | −5.9% | −4.9% |
| 5.8 to 8 ms | 8 | 1,336 | 119.0 | −24.8% | −8.3% |
| | 64 | 7,605 | 70.5 | −17.5% | −7.7% |

- **CPU.** C1 used less frontend CPU per output token than A in every cell, 6% to 25% less. The mock engines' CPU per token fell by up to 34% in C1. C1's thread parks per token were 20% lower to 4% higher than A's.
- **Throughput.** Every arm was within 1.3% of A. The mock engines set the pace, and the frontend used at most 4.8 of its 16 cores, so this test could not show a throughput gain.
- **Latency.** For time to first token and inter-token latency, each median of C1 was within 1% of A's or had a range that overlapped A's, with one exception. At 5.8 to 8 ms spacing and concurrency 64, C1's median time to first token was 83.7 ms against A's 70.9 ms (ranges 75.4 to 96.9 against 68.3 to 79.3). No other cell showed it.
- **Two threads.** C2 used more frontend CPU per token than C1 in every cell. It had 4% to 23% more thread parks per token than A, and in some cells the engines used up to 17% more CPU per token than in A.

## Why the default

Only a dedicated runtime removes the stall in all its forms. In dynamo's load test, the default cost less CPU and changed neither throughput nor latency. Its cost falls on unary calls made one at a time, which dynamo did not make. For those, the opt-out stays.

## Who should opt out

Opt out with `TaskRuntime::Application` when latency depends on velo unary calls made one at a time. dynamo-gdl and kvbm make such calls. Run their latency benchmarks on both settings, and keep `Application` if it is faster for them. An instance that opts out must keep blocking work off the tasks that call velo. [0.23.0: velo's tasks run on a runtime it owns](../guides/migrate-services.md#0230-velos-tasks-run-on-a-runtime-it-owns) gives the code and the other things to check, such as tests that pause tokio's clock.

## What was not covered

- **The shipped code.** Both data sets ran arm C of the measurement build, which left the heartbeats, watchdogs, sweeps and velo's own handlers on the application's runtime.
- **A frontend-bound load.** In dynamo's test the mock engines set the pace.
- **Real engines across hosts.** Every run was on one host, over loopback.
- **velo unary calls in dynamo.** dynamo's frontend uses velo for responses only.
- **A single unary caller between hosts.** The 3.5 times figure comes from two nodes in one process.
- **Unpinned threads in velo's benchmark.** That benchmark pinned velo's threads to their own cores. The shipped runtime does not pin them, and dynamo's test pinned whole processes only.
- **An application runtime whose tasks do not yield.**

## Open questions

- **CPU per token.** In velo's serving shape, C1 used 29% more CPU per token than A (12.9 µs against 10.0 µs, with 0.44 thread parks per token against 0.11). In dynamo, C1 used 6% to 25% less. This is not explained. The setups differ in ways not tested as causes: velo's benchmark ran all nodes in one process with velo's threads pinned to their own cores, and dynamo ran the frontend and the engines as separate processes, pinned per process.
- **The 1.1 ms tail at one stream.** It appeared in most reps of the C arms and in 1 rep of A, and never under saturation. 1 ms is the default `reply_linger`, and a batch that holds only credit replies waits that long. But that wait applies in every arm, so it does not explain why the C arms show the tail far more often. No counter in the runs separated linger writes from others.
- **Row 4 on arrival.** A consumer that came back to a waiting record stalled the credit sweep in all 50 runs of arm A. A consumer that read the record as soon as it arrived stalled it in none, also with `drain_visit_floor` and `reply_linger` at zero. One explanation fits the tokio code. tokio turns the LIFO slot off for the third task it runs from the slot in a row (`MAX_LIFO_POLLS_PER_TICK` in its multi-thread worker), so a wake that task makes goes to the queue that other workers steal from. A consumer woken through the connection reader, the inbound loop and the batch lane would be that third task. This was not checked: the batch lane can wake another task after the consumer, which changes the order.

## Rejected alternatives

- **The mux alone on its own runtime (arm B).** It removes rows 1 and 4 only. At one stream it costs 35% more CPU per record than A. Where C helps (64 streams, 64 unary callers), B helps less or not at all. Where C loses (one unary caller, flood), B is close to A, because it leaves the transports and the inbound loop alone.
- **A yield after the wake (arm D).** It removes row 1 only. In the serving shape it wakes the batcher about once per token instead of once per 11 tokens, which costs 31% more CPU per token and 15% more time to first token (p50 311 ms against 270 ms).
- **A `LocalSet` for velo's tasks.** It is `!Send` and must be driven by one thread. A wake from another thread goes to its remote queue and wakes that thread (tokio `task/local.rs`), as the wakes into arm C's runtime do. On a thread of its own it is a one-thread private runtime, which is arm C1. On an application worker it can be held like arm A. So it is not a separate arm.
- **Stealing from the LIFO slot.** tokio 1.51.0 made the slot stealable, and 1.51.2 and 1.52.2 reverted that for its cost. `disable_lifo_slot` needs `tokio_unstable`.
- **More than two threads.** C4 was no better than C2 with four busy peers, and no better than C1 with one, at more CPU. `build` refuses a count other than 1 or 2.

## Reproduce

The commands above ran on the measurement build. To compare the settings on shipped code, set `VeloBuilder::task_runtime` in the harness. Follow the [measurement rules](benchmarking.md#measurement-rules): an exclusive node, both settings from one build, interleaved, at least 5 reps. Report latency beside throughput.
