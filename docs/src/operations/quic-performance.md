# QUIC performance

This chapter records what the QUIC transport costs against TCP, the settings that change it, and one quinn defect that the transport works around. For the design, see [Transports](../concepts/transports.md#quic).

## Settings

| Builder method | Default | Effect |
|---|---|---|
| `server_endpoints(n)` | 4 | Server sockets in the `SO_REUSEPORT` group (Linux). More sockets spread the receive load of many peers. |
| `udp_buffer_sizes(recv, send)` | 8 MiB, 4 MiB | Requested socket buffers. The kernel clamps them to `net.core.rmem_max` and `net.core.wmem_max`, and the transport logs the clamp. |
| `max_mtu(bytes)` | quinn's (1452) | Upper bound for path MTU discovery. Values above 6550 are lowered to 6550. |
| `stream_receive_window(bytes)` | quinn's | Flow-control window for the stream. |
| `keep_alive_interval(d)` | 5 s | QUIC keep-alive. |

The examples read `VELO_QUIC_MAX_MTU`, `VELO_QUIC_STREAM_WINDOW`, and `VELO_QUIC_SERVER_ENDPOINTS`, so a sweep can change them without new flags.

## Packets above 6550 bytes are lost

quinn gives up to 10 packets to one GSO send, and it does not limit the size of the batch. A UDP datagram holds at most 65507 bytes. Above 6550 bytes a packet, a full batch is too large, and the kernel returns `EMSGSIZE`. quinn-udp 0.5 treats `EMSGSIZE` as success, because it expects that error only from MTU probes. So the whole batch is lost with no error, and each flight waits for a loss-probe timeout.

Measured on loopback with 64 KiB messages, one at a time:

| `max_mtu` | Messages per second |
|---|---|
| 6550 | 2,666 |
| 6560 | 115 (p95 latency 28 ms) |

The transport lowers `max_mtu` to 6550 and logs a warning. The test `large_frames_round_trip_at_a_jumbo_mtu` requests 8952 and fails if 200 round trips of 64 KiB take 2 s or more. Without the limit they take about 9 s.

## Loopback measurements

Measured on 2026-09-23 on one aarch64 workstation (20 cores, shared, load average near 8), over loopback, with the `throughput` example and 5,000 messages for each cell. Two reps for each configuration. These numbers show direction only. The cluster sweep gives the numbers that count.

| Case | TCP | QUIC, default | QUIC, `max_mtu` 6550 |
|---|---|---|---|
| 64 B, one at a time (msg/s) | 21,100–23,500 | 12,000–12,200 | 10,200–12,500 |
| 64 B, one at a time, p50 (µs) | 37–41 | 74–77 | 77–92 |
| 64 B, 64 in flight (msg/s) | 294,000–297,000 | 191,000–195,000 | 199,000–200,000 |
| 64 B, pipelined (msg/s) | 482,000–486,000 | 465,000–574,000 | 486,000–562,000 |
| 64 KiB, one at a time (msg/s) | 9,600–11,900 | 2,500–2,600 | 3,200–3,300 |
| 64 KiB, 64 in flight (msg/s) | 16,700–19,900 | 8,400–8,800 | 10,900–11,200 |
| 64 KiB, pipelined (MB/s) | 2,800–4,400 | 580–620 | 770–790 |

- Small messages cost about twice the latency of TCP. Each direction passes through quinn's connection and endpoint driver tasks, so one message takes more task hops than on TCP.
- Large messages reach about 600 to 800 MB/s on one connection. One quinn task does the packet work and the encryption for a connection. TCP moves 64 KiB segments on loopback, and the kernel does that work.
- `max_mtu` 6550 gives 25% to 30% more for large messages when the path carries it. It does not help small messages.

### Settings that did not help

- **A larger initial congestion window.** 1 MiB instead of quinn's 14,720 bytes made no difference, at the default MTU or at 6550.
- **Larger flow-control windows.** A 16 MiB stream window with a 64 MiB send window made no difference to one-at-a-time traffic, and was slower with 64 in flight (9,300 to 9,600 against 10,900 to 11,200 msg/s).
- **Charging Tokio's task budget per batch of frames.** Dynamo's QUIC plane needed this because it polled quinn once per frame header and once per payload. The velo reader uses `FramedRead`, which decodes all buffered frames before it polls quinn again, and routes frames through `flume`, which does not charge the budget.

## Batched streaming over QUIC

Measured on the same workstation with `response_plane_bench` (2 anchor hosts, 64 engines, 2,000 requests, 200 warm-up requests, `--flush-policy auto`), three reps.

| Transport | Requests/s | TTFT p50 (ms) | ITL p50 (ms) | CPU (ms/request) |
|---|---|---|---|---|
| TCP | 8,200–11,600 | 31–72 | 2.2 | 0.39–0.48 |
| QUIC, default | 6,800–11,300 | 31–123 | 2.2 | 0.50–0.61 |
| QUIC, `max_mtu` 6550 | 7,500–9,600 | 54–102 | 2.1–2.3 | 0.58–0.69 |

In the two quieter reps, QUIC matched TCP on throughput and first-token latency (11,300 and 11,000 against 11,600 and 11,000 requests/s). It used 15% to 30% more CPU for each request. The mux batches many token records into each frame, so the per-message cost of QUIC matters less here than in the one-at-a-time tests. `max_mtu` 6550 was slower in all three reps with these small frames. Use it only for traffic with large messages.
