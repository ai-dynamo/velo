# RDMA performance

This chapter records measured numbers for the rendezvous RDMA path, with the setup behind each number. For how the path works, see [Rendezvous and RDMA](../concepts/rendezvous.md). For the setup procedure, see [Run rendezvous over RDMA](../guides/ucx-rdma.md).

> **Every two-node number here is an unaccelerated floor.** The runs used `rc_verbs` on a plain verbs memory domain. At the time, velo's vendored UCX opened no mlx5 memory domain (see [RDMA design](../development/rdma-design.md#the-mlx5-link-order-defect)). They are lower bounds on what the hardware does, not measurements of it. No run has used the accelerated `rc_mlx5` lane yet. Re-derive `rdma_min_bytes` on that lane before you trust the 64 KiB default.

## Two-node setup

| Item | Value |
|---|---|
| Measured on | 2026-08-29 |
| Nodes | Two nodes, `--exclusive`, 144 cores and 478 GB each |
| CPU and OS | aarch64 (Grace class), Ubuntu 22.04.5 |
| NIC | ConnectX-7 (`vendor_part_id` 4129, firmware 28.43.2566), three mlx5 devices per node |
| Port | `mlx5_2:1`, InfiniBand, `PORT_ACTIVE`, 400 Gb/s (4X NDR), MTU 4096. Both nodes report the same subnet manager LID. |
| Excluded ports | `mlx5_0:1` Ethernet 25 Gb/s (active, RoCE-capable) and `mlx5_1:1` Ethernet (down). Excluded so that no result is a silent RoCE measurement. |
| Memlock | `ulimit -l unlimited` |
| UCX | Vendored 1.22.0, static |
| Environment | `UCX_NET_DEVICES=mlx5_2:1 UCX_TLS=rc_verbs,ud_verbs,self` |

`UCX_PROTO_INFO=y` on the live run named the device for every protocol range:

```text
| 1476303..inf | (?) rendezvous zero-copy read from remote | rc_verbs/mlx5_2:1 50% on path0 and 50% on path1 |
|       0..107 | short                                     | rc_verbs/mlx5_2:1/path0                         |
```

## Cold pair: one transfer per process pair

Each rep started a fresh owner process and a fresh consumer process, one on each node, with the `rendezvous_rdma_two_proc` example. There were 5 reps per cell. The time is the example's own `velo.get()` measurement: the acquire round trip, the transfer, and the copy out of registered memory. `release` is not included. The chunked baseline ran with `VELO_RDMA_RENDEZVOUS_DISABLE=1`.

| Payload | RDMA `get()` median | Chunked `get()` median |
|---|---|---|
| 64 KiB | 14.21 ms | 0.346 ms |
| 1 MiB | 14.38 ms | 1.271 ms |
| 1 MiB + 4097 B (not granule-aligned) | 14.55 ms | 1.433 ms |
| 16 MiB | 16.11 ms | 14.597 ms |
| 256 MiB | 57.50 ms | 197.25 ms |

- All 50 transfers matched byte for byte, including the size that is not granule-aligned. No corruption, truncation, or hang occurred.
- All 25 RDMA reps reported `rdma/ok` on both sides.
- Every RDMA number includes the one-time endpoint wireup. The flat 14 ms floor from 64 KiB to 1 MiB is that wireup, not the transfer.
- The raw results show a wall time near 30 s per rep. That was NFS negative-dentry caching on the shared home directory, which delayed the file exchange between the two processes. It is not a velo latency.

## Warm pair: many transfers per process pair

The sweep harness (see [How the sweep works](#how-the-sweep-works)) ran 10 transfers per size inside one process pair on the same two nodes. The table gives medians of reps 1 to 9. Rep 0 is excluded because it carries the one-time wireup.

| Payload | RDMA warm median | Chunked warm median | RDMA advantage |
|---|---|---|---|
| 4 KiB | 108 µs | 151 µs | 1.40x |
| 8 KiB | 113 µs | 156 µs | 1.38x |
| 16 KiB | 117 µs | 161 µs | 1.38x |
| 32 KiB | 114 µs | 183 µs | 1.61x |
| 64 KiB | 123 µs | 194 µs | 1.58x |
| 128 KiB | 129 µs | 227 µs | 1.76x |
| 256 KiB | 131 µs | 280 µs | 2.14x |
| 512 KiB | 159 µs | 401 µs | 2.52x |
| 1 MiB | 229 µs | 717 µs | 3.13x |
| 4 MiB | 566 µs | 2816 µs | 4.98x |

- The first RDMA `get()` on a fresh peer pair took 14,145 µs. Later gets took about 108 µs.
- The first chunked transfer took 216 µs. It pays no comparable cost, because it uses the TCP control connection that already exists and never creates a UCX endpoint.
- All 100 sweep transfers took `rdma/ok`. All 100 transfers in the chunked run took `chunked/kill_switch`. Every payload matched.
- Warm RDMA is a fixed cost of about 105 µs plus a size term. The 1 MiB and 4 MiB points give about 9 GB/s marginal rate. Two points show the shape only and are not a bandwidth measurement.
- The rate is far below the 400 Gb/s link, because `get()` copies the payload out of registered memory into `Bytes`. `get_pinned()` and `get_into()` skip that copy. Neither was measured.

The fixed ~105 µs is the acquire round trip. Registration (next section) is not the bottleneck.

## Threshold verdict for `rdma_min_bytes`

The default stays at 64 KiB. The threshold turned out not to be the lever.

1. **Warm pair.** RDMA beats chunked at every measured size, down to 4 KiB (1.4x). There is no crossover above 4 KiB on this fabric. The 64 KiB default is therefore conservative. It costs about 40 to 70 µs per transfer in the 4 to 64 KiB band. That cost is smaller than the extra pinned memory and lease traffic that small pinned slots add.
2. **Cold pair.** The 14 ms wireup reverses the result. Chunked wins by 41x at 64 KiB, 11x at 1 MiB, and 1.1x at 16 MiB. RDMA wins only at 256 MiB (3.4x). The cold crossover lies between 16 MiB and 256 MiB, near the low end. The two cells are 16x apart, so the data supports no tighter number.
3. **The lever is the wireup.** A lower threshold makes the cold case worse. A threshold inside the cold band discards the warm win, which is the common case for any pair that transfers more than once. `UcxTransportBuilder::eager_endpoints` exists because of this result. It moves the wireup to `register()`. It is off by default.

Two caveats limit the comparison:

- The chunked arm used the kill switch, and the kill switch also makes the owner stage in plain memory (`pinned=false` in every chunked line, `pinned=true` in every RDMA line). Staging mode therefore changes together with the path. The table answers "enable RDMA or not". It is not a clean sweep of `rdma_min_bytes` with pinned staging on both arms. The 4 KiB gap is about 40 µs either way, so the verdict likely holds.
- All numbers are from the `rc_verbs` floor. The warm latency and the 14 ms wireup can change on `rc_mlx5`.

## Registration cost and raw GET latency

`bench_rma` ran two UCX workers in **one process** on one node. These are HCA-loopback numbers, not two-node numbers, and they do not compare with the tables above. The test header printed `UCX_TLS=tcp`, but the environment overrode it. The run used the same `rc_verbs` lane and port. Measured on 2026-08-29.

| Region size | `ucp_mem_map` | `ucp_mem_unmap` | Packed rkey |
|---|---|---|---|
| 4 KiB | 53.98 µs | 35.52 µs | 20 B |
| 1 MiB | 43.07 µs | 31.84 µs | 20 B |
| 64 MiB | 265.97 µs | 132.22 µs | 20 B |

| GET size | Latency, three timed passes | Throughput |
|---|---|---|
| 64 KiB | 24.93 / 22.43 / 19.71 µs | 2.5 to 3.2 GiB/s |
| 1 MiB | 50.27 / 49.82 / 49.92 µs | about 19.6 GiB/s |
| 16 MiB | 443.2 / 441.9 / 442.1 µs | about 35.3 GiB/s |

Registration costs 43 to 266 µs. It is not the bottleneck of a warm transfer, because the pool registers an arena once and many transfers use it. A registration per transfer adds this cost to every transfer, so pre-registered arenas stay the right design.

An earlier probe measured `ucp_mem_map` much higher on a different host: 161 to 805 µs for 4 KiB to 16 MiB, and 3.2 ms at 100 MiB. That probe ran on x86_64 (DGX B200, ConnectX-7 InfiniBand, MLNX OFED 25.07) with UCX 1.22.0 built on the node. The two results use different hosts, CPU architectures, driver stacks, and memory domains. They are not reconciled.

## Packed rkey size

| Lane | Memory domain | UCX | Host | Packed rkey |
|---|---|---|---|---|
| `UCX_TLS=tcp` | tcp (registers nothing) | 1.22.0 | any | 9 B (header only) |
| `rc_verbs`, CX-7 | verbs | 1.22.0 vendored | aarch64 | 20 B |
| CX-7 IB probe | DEVX (expected) | 1.22.0 | x86_64 B200 | 19 B |
| CX-7 IB probe | DEVX (expected) | 1.19.0 | x86_64 B200 | 18 B |

A packed rkey carries key material for each memory domain, and different domain types pack different amounts. The 20 B and 19 B results can therefore both be right. No run has measured a packed rkey under a DEVX domain in velo's own build. From the UCX source, each extra IB device adds 9 B.

The `rkey_pack_canary` test asserts at least 9 B, and the bound stays there. The 20 B value came from the verbs domain that the link-order defect forced. The value changes once the mlx5 domain opens.

## Correctness on InfiniBand

On the same two nodes and lane:

- All 23 `rendezvous_rdma` integration tests passed, including `a_malformed_descriptor_falls_back_to_chunked` and `a_failed_get_falls_back_chunked_exactly_once`.
- `get_cancelled_by_endpoint_replacement` passed in 0.16 s. The caller got an answer, the region was released, and teardown balanced.
- These RMA tests also passed: `rkey_pack_canary`, `preparse_accepts_real_packed_rkeys`, `map_get_roundtrip`, `get_zero_length`, `get_out_of_range`, `unmap_waits_for_inflight`, `shutdown_with_inflight_get`, `get_cancel_still_releases_the_region`, `peer_shutdown_during_get_answers_caller`, and `truncated_rkey_is_refused_before_ucx`.
- Four teardown tests (`map_get_roundtrip`, `unmap_waits_for_inflight`, `get_cancel_still_releases_the_region`, `peer_shutdown_during_get_answers_caller`) log `ucp_ep.c:2222 UCX ERROR ep ... has already been closed`. Every assertion holds. This double close does not occur over TCP.
- `unusable_rkey_is_refused_by_ucx_over_tcp` aborted the process. See [RDMA design](../development/rdma-design.md#a-stale-rkey-aborts-the-process-on-infiniband).

Not measured: the `abandon_rma_ops` teardown path under a stopped owner, `get_pinned()` and `get_into()` throughput, concurrent transfers, and any soak on hardware.

## Probe measurements that shaped the design

These numbers come from probes before the rendezvous code existed. They ran on x86_64 (DGX B200, ConnectX-7 InfiniBand, MLNX OFED 25.07) with UCX 1.22.0 and 1.19.0, pinned to `mlx5_0:1`. Measured on 2026-08-20.

- **One-sided GET.** A 1 MiB GET completed in 826 µs while the owner worker never ran its progress loop. The data matched. This held with `UCP_ERR_HANDLING_MODE_PEER` on `rc_mlx5` with DEVX on, with `UCX_IB_MLX5_DEVX=n`, and on `rc_verbs`.
- **Eager Active Messages.** With `UCP_AM_SEND_FLAG_EAGER` on every send, the receiver saw eager data at every size up to 8 MiB and never a rendezvous receive. Without the flag, sends switched to rendezvous at 1 MiB and above. Eager 8 MiB reached 1.28 GB/s against 1.21 GB/s for rendezvous. The single-port loopback path capped at 1.43 GB/s, and UCX reached 83 to 98% of the raw-verbs ceiling on that path.
- **Progress thread wakeup.** `ucp_worker_signal` against a spinning poller took 0.83 µs at p50 and 1.28 µs at p99. Against a blocking `poll()` it took 7.3 µs at p50, with a 115 to 165 µs tail from CPU idle states. A bare `eventfd` shows the same tail, so the platform causes it, not UCX. One earlier aarch64 sample of 3.09 µs was a lucky blocking draw.
- **UCX protocol boundaries** for Active Messages on `rc_mlx5`, host memory, CX-7: short up to 2038 B (2030 B with the reply flag), bcopy 2039 to 8118 B, zero-copy 8119 to 8246 B, multi-fragment zero-copy up to 311,293 B, rendezvous from 311,294 B.

The spin-then-park loop of the UCX progress thread (`UcxConfig::spin_us`, 20 µs by default) follows from the wakeup numbers. A loaded submitter pushes onto the ring in about 100 ns and skips the signal. Only an idle worker pays the wakeup.

## How the sweep works

The warm-pair table came from a measurement harness, `rv_sweep`. It is not a shipped example. It compiles against the current API as an example with `required-features = ["ucx"]`, using only dependencies that `examples/Cargo.toml` already has. This section describes it so that it can be rebuilt.

**Shape.** It has the same two-process shape as `rendezvous_rdma_two_proc`. Each role builds a `Velo` with a TCP transport, a UCX transport added through `add_ucx_transport`, a Prometheus registry, and `RdmaConfig { rendezvous: RdmaRendezvousConfig { rdma_min_bytes, .. }, .. }`.

**Flags.** `--role owner|consumer`, `--dir <shared directory>`, `--sizes <comma-separated bytes>` (default 4096 to 1048576 in powers of two), `--reps <n>` (default 10), and `--min-bytes <n>` (default 65536, applied to both roles). The 4 MiB row needs a `--sizes` list longer than the default.

**Owner.**

1. For each size, stage `--reps` separate slots with `register_data_pinned`. A slot is single-use, because `release` drops its only reference. One slot per rep lets the pair measure a warm endpoint instead of a new wireup each time.
2. Record `metadata(handle).pinned` for each slot, and print `rdma_registered_bytes()`.
3. Write `owner.json`: the owner's `PeerInfo` and a list of `(size, handle as u128 decimal, pinned)`.
4. Wait for `consumer.json`, register the consumer as a peer, and wait for a `done` file.
5. Call `graceful_shutdown(ShutdownPolicy::Timeout(30 s))`.

**Consumer.**

1. Read `owner.json`, build the instance, register the owner, and write `consumer.json`.
2. Wait up to 120 s for the owner's `_rv_acquire` handler with `wait_for_handler`.
3. For each slot: read `velo_rendezvous_rdma_path_total{reason="ok"}`, time `velo.get(handle)`, call `release`, and read the counter again. An increase means the RDMA path ran.
4. Verify the length and every byte against the pattern.
5. Print `SWEEP size=<n> rep=<n> pinned=<bool> path=<rdma|chunked> get_us=<n>`.
6. Print a summary of every non-zero `path_total` label, write `done`, and shut down.

**Payload.** Byte `i` is `(i * 31 + (i >> 8)) as u8`. The pattern does not repeat at the chunk size, so a chunk written at the wrong offset fails the comparison.

**File exchange.** Each card is written to a temporary name and renamed, which is atomic in one directory. The reader polls every 25 ms. Before each read, it calls `read_dir` on the parent directory. On NFS, a plain read of a name that the client cached as absent does not revalidate, and `read_dir` forces it. The `rendezvous_rdma_two_proc` example does not have this step.

**Arms.** The RDMA arm runs as is. The chunked arm in the table ran the same binary with `VELO_RDMA_RENDEZVOUS_DISABLE=1`.

**Two changes make the next run cleaner:**

- For a clean threshold sweep, run the chunked arm with `--min-bytes` above the largest size instead of the kill switch. Both arms then stage in pinned memory, and the owner answers `below_min`. Only the path differs.
- Add a `get_pinned` arm. `get()` includes a copy out of registered memory, and that copy dominates at large sizes.

**Run it.**

1. Build: `cargo build --manifest-path examples/Cargo.toml --release --features ucx --example rv_sweep`.
2. Allocate two `--exclusive` nodes on one subnet manager, each with an active InfiniBand port. Verify the port with `ibv_devinfo`.
3. Start the owner, then the consumer, with the same `--dir` and the same `UCX_NET_DEVICES` and `UCX_TLS` on both.
4. Verify the lane with `UCX_PROTO_INFO=y`, as in [Verify the lane](../guides/ucx-rdma.md#verify-the-lane).
5. Take medians over reps 1 and later.
