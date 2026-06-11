# TIPC Transport for Velo — Analysis & Proposal

Status: proposal (post-design-review). Winner of a three-design adversarial panel
(in-tree SOCK_STREAM vs. out-of-tree crate vs. TIPC-native RDM), with the panel's
grafts and all critique corrections folded in. Every kernel-behavior claim below is
tagged **[verified]** (proven on this machine, kernel 6.14.0-1015-nvidia, or in v6.14
kernel source) or **[assumed]** (documentation/source-read only, with the settling
experiment named). Refuted claims from earlier drafts have been fixed, not repeated.

---

## 1. Executive summary

Add TIPC as an **in-tree, feature-gated messenger transport** at
`lib/velo/src/transports/tipc/`, feature `tipc`,
`#[cfg(all(target_os = "linux", feature = "tipc"))]`. Socket type **SOCK_STREAM**,
reusing `TcpFrameCodec` verbatim (the UDS precedent — `uds/transport.rs:28`), wrapped
in `tokio::io::unix::AsyncFd` over a hand-written ~240-LOC `sys.rs` transcribed from
`/usr/include/linux/tipc.h`. **Zero new dependencies** (libc/socket2/tokio are already
in the tree), **zero velo-ext changes for phase 1**, no new workspace crate. Endpoints
ride as opaque msgpack bytes under WorkerAddress key `"tipc"`; the primary connect
target is the exact TIPC socket address `{ref, node}`, sidestepping anycast. A
`register()`-time reachability gate (netid + netns nonce + live name-table
publication match + node-state cache)
returns `TransportError::NoEndpoint` for unreachable peers so the priority sort
silently promotes TCP — TIPC can be enabled fleet-wide and only engages where it works.
Transient rejections (cold-start name-table propagation) are parked and re-driven
through `register_peer` by topology events, so a TCP demotion is never permanent.
Phasing: **P1** messenger `Transport` (~2,500 LOC, 4–6 days), **P2** streaming
`FrameTransport` (SOCK_STREAM clone of `TcpFrameTransport`, requires a breaking-for-velo
`StreamConfig` variant → velo 0.5.0), **P3** kernel topology service as
`PeerDiscovery`/`ServiceDiscovery` (zero-infrastructure discovery: bind *is*
registration, socket close *is* deregistration), **P4** group messaging as a
`WorkQueueBackend` plus an optional public group API. CI: compile-always via a
`velo_tipc` rustc cfg (the `velo_endurance` precedent, `lib/velo/Cargo.toml:105-109`),
run in a dedicated job on the org-controlled self-hosted runners after a
runner-host modprobe probe — which is **step zero** of the implementation.

Why bother: measured on this box, intra-node TIPC is 4.4 µs RTT vs 7.7 µs loopback
TCP and ~1.8× loopback TCP throughput at 65 KB; cross-node failure detection is
~2.1 s (tunable to 50 ms) vs TCP keepalive's 60 s; and phase 3 deletes the etcd/NATS
dependency for intra-cluster discovery.

---

## 2. TIPC primer (scoped to velo)

TIPC = Transparent Inter-Process Communication: `AF_TIPC = 30`, `SOL_TIPC = 271`,
in-tree kernel module (`tipc.ko`, module version 2.0.0), mainline since 2.6.16,
per-netns since 4.0. Everything below is the subset velo touches.

### 2.1 Addressing

Three address forms (UAPI `/usr/include/linux/tipc.h`):

```c
struct tipc_socket_addr   { __u32 ref;  __u32 node; };              /* exact socket  */
struct tipc_service_addr  { __u32 type; __u32 instance; };          /* logical name  */
struct tipc_service_range { __u32 type; __u32 lower; __u32 upper; };/* name range    */
```

carried in a 16-byte `sockaddr_tipc` (`family` u16, `addrtype` u8, `scope` i8, 12-byte
union) — **[verified]** sizeof 16 in both C and Rust `#[repr(C)]` on this host.
Address-type discriminators: `TIPC_SERVICE_RANGE = 1`, `TIPC_SERVICE_ADDR = 2`,
`TIPC_SOCKET_ADDR = 3`. Bind scopes: `TIPC_CLUSTER_SCOPE = 2`, `TIPC_NODE_SCOPE = 3`.
Service types 0–63 are reserved (`TIPC_RESERVED_TYPES 64`); type 1 is the topology
server (`TIPC_TOP_SRV`).

Key semantics:

- **Service-address connect/send is anycast** — load-balanced among all matching
  publishers, "closest-first" (local publications preferred, else round-robin;
  v6.14 `net/tipc/name_table.c:549-612`, `tipc_nametbl_lookup_anycast`). **[verified]**
  in source. Consequence: health probes and reconnects must use the exact
  `{ref, node}` socket address, never the service address.
- **Node identity**: 128-bit free-form since 4.17, auto-derived from the first
  bearer's MAC if unset, hashed to the 32-bit `node` value seen in socket addresses
  and topology events. With no bearer and no identity, `node == 0` and node-local
  operation still works fully — **[verified]** live (getsockname returned
  `addrtype=3, node=0x0`; connect to `{ref, 0}` succeeded).
- **Cluster identity (netid)**: u32, default **4711**. Nodes with equal netid on a
  shared bearer auto-merge into one cluster. This is an *unauthenticated* membership
  signal — see §10 security.

### 2.2 Socket types and exact reliability/flow-control semantics

| Type | Reliability | Flow control | Max msg | Velo verdict |
|---|---|---|---|---|
| SOCK_STREAM | connection, ordered, loss-free | end-to-end, block-granular (1024-B blocks) | byte stream (kernel chunks at 66,000 B internally — v6.14 `socket.c:1588`) | **chosen** for messenger + streaming |
| SOCK_SEQPACKET | connection, ordered, loss-free | same | 66,000 B/record (`EMSGSIZE` above) | rejected: record cap vs 16 MiB velo frames forces a segmentation layer |
| SOCK_RDM | link-layer retransmitted, per-sender FIFO; **silently dropped on receiver overflow by default** | link-level only — **no end-to-end** | 66,000 B | rejected for messenger/streaming; deferred fast-path candidate (§11) |
| SOCK_DGRAM | RDM minus link retransmission | same | 66,000 B | not considered |

Load-bearing facts, all **[verified]** empirically on this host unless noted:

- `TIPC_MAX_USER_MSG_SIZE = 66000` is exact and inclusive: 66,000-byte datagram OK,
  66,001 → `EMSGSIZE` (errno 90).
- STREAM has no message-size ceiling: a 1 MiB single send arrived byte-exact and
  in order over one connection (kernel chunks internally at 66,000 B, transparent to
  a byte-stream codec). Velo's 16 MiB `TcpFrameCodec` cap
  (`tcp/framing.rs:26`) rides over it unchanged.
- **End-to-end flow control on STREAM is real backpressure**: with the receiver not
  reading, a non-blocking sender accepted 131,072 bytes then hit `EAGAIN`; `EPOLLOUT`
  was withheld until the receiver drained, with zero loss. Kernel gate:
  `tipc_poll` reports `EPOLLOUT` only when `!cong_link_cnt && !tsk_conn_cong(tsk)`
  (v6.14 `socket.c:805-807`). Caveat: the advertised window derives **exclusively
  from the receiver's `sk_rcvbuf`** (`tsk_adv_blocks(sk_rcvbuf)`, `socket.c:1808-1812`)
  and the initial pre-ACK window is small (~132 KB observed) — `SO_SNDBUF` is a
  flow-control no-op on TIPC, and `TIPC_IMPORTANCE` (127) is ignored for connected
  sockets' receive limits (`socket.c:2304-2319`). The tuning knob is `SO_RCVBUF` on
  accepted sockets (and sysctl `tipc_rmem`, default 2 MiB here).
- **SOCK_RDM defaults to `TIPC_DEST_DROPPABLE=1`** — receiver overflow silently drops
  with zero signal to the sender. With it set to 0, `sendto` still never blocks or
  errors; rejects return **asynchronously** to the sender's own receive queue as
  0-byte datagrams carrying `TIPC_ERRINFO` (err = `TIPC_ERR_OVERLOAD` = 4) +
  `TIPC_RETDATA` truncated to 1,024 bytes (`MAX_FORWARD_SIZE`). Flood test: 14,563 of
  20,000 1-KB sends returned as rejects. This is why RDM cannot honor velo's
  `on_error(header, payload, …)` contract without sender-side payload retention —
  the decisive disqualifier.

### 2.3 Connection lifecycle quirks (all differ from TCP; all [verified])

These three were discovered by adversarial kernel probing and are **design inputs**,
not footnotes:

1. **No half-close.** `shutdown(SHUT_WR)` and `shutdown(SHUT_RD)` return `EINVAL`;
   only `SHUT_RDWR` is accepted (v6.14 `socket.c:2794-2800`; reproduced live).
   Queued data is fully delivered before the close signal (verified), so
   `shutdown(Both)` is safe.
2. **Plain `close()` surfaces as ECONNRESET at the peer, never EOF.** `tipc_release`
   sends a FIN carrying `TIPC_ERR_NO_PORT` (`socket.c:640`), and recv maps any
   errcode other than `TIPC_CONN_SHUTDOWN` to `ECONNRESET` (`socket.c:2078-2081`).
   Clean EOF (`recv() == 0`) happens **only** after explicit `shutdown(SHUT_RDWR)`.
   Pending data is flushed first in both cases — no loss, only signaling differs.
3. **`connect()` completes only when the remote application calls `accept()`.**
   The connect ACK is sent from inside `tipc_accept()` (`socket.c:2769-2778`); there
   is no kernel-backlog handshake and `tipc_listen` ignores the backlog argument
   (`socket.c:2660-2670`). Verified: socket not writable before remote accept,
   writable immediately after; queued SYNs are accepted later when the accept loop
   runs. Default kernel connect timeout `TIPC_CONN_TIMEOUT` (sockopt 130) = 8,000 ms.

Two more operational facts: a SYN rejected with `TIPC_ERR_OVERLOAD` is auto-retried
by the connecting kernel with randomized ~100 ms+ backoff until the connect timeout
(`socket.c:2228-2238`) **[verified in source]**; and `close()`/`shutdown()` can block
the calling thread up to a hardcoded 8 s while the link is congested or the conn
window full, ignoring `O_NONBLOCK` (`socket.c:548-560`) **[verified in source]** —
a tokio-worker stall hazard handled in §5.4.

### 2.4 Failure detection

- Stale socket ref → `ECONNREFUSED` in ~0.04 ms **[verified]**.
- Unknown/unreachable node with no bearer → synchronous `EHOSTUNREACH` **[verified]**.
- Peer process death mid-connection → `ECONNRESET` on read, `EPIPE` on write
  **[verified]** (SIGKILL test).
- **Node loss / partition**: the link layer aborts all connections toward the dead
  node after link tolerance — default 1,500 ms, range 50 ms–30,000 ms
  (`TIPC_MIN/DEF/MAX_LINK_TOL` = 50/1500/30000, `/usr/include/linux/tipc_config.h:168-170`).
  **[verified]** on a real two-node cluster built from two netns + veth + per-netns
  bearers on this machine: silent partition (netem 100% loss) → blocking reader got
  `ECONNRESET` **2,138 ms** after partition; local interface-down → **10 ms**
  (carrier event). Document as "~1.5–2.5 s", not "within 1.5 s" — tolerance
  parametrizes probing, it is not an upper bound.
- Established but silent connections are kernel-probed every 1 hour
  (`CONN_PROBING_INTV`, `socket.c:56`).

### 2.5 Topology service

Connect a SOCK_SEQPACKET socket to `{1,1}` (`TIPC_TOP_SRV`), write a 28-byte
`tipc_subscr { seq, timeout, filter, usr_handle[8] }`, read 48-byte `tipc_event`s:
`TIPC_PUBLISHED = 1`, `TIPC_WITHDRAWN = 2`, `TIPC_SUBSCR_TIMEOUT = 3`, with the
matched range and the publisher's `{ref, node}`. Filters: `TIPC_SUB_PORTS = 0x01`
(event per binding), `TIPC_SUB_SERVICE = 0x02` (edge-triggered), `TIPC_SUB_CANCEL = 0x04`.
**[verified]** live: PUBLISHED 0.331 ms after bind, WITHDRAWN 0.300 ms after socket
close — publications auto-withdraw on close/crash with **no lease, no keepalive, no
external daemon**. Special subscription types: 0 (`TIPC_NODE_STATE`) yields node
up/down events, 2 (`TIPC_LINK_STATE`) per-link events. This is the phase-3 substrate
and the phase-1 `NodeStateWatch` substrate.

### 2.6 Groups (kernel ≥ 4.14)

`setsockopt(SOL_TIPC, TIPC_GROUP_JOIN, &tipc_group_req{type, instance, scope, flags})`
on a SOCK_RDM socket. Send-mode dispatch by destination addrtype: none = broadcast,
service addr = **anycast (load-aware, skips congested members)**, range = multicast,
socket addr = unicast. Groups have full end-to-end flow control ("messages will never
be dropped because of destination buffer overflow") and per-source ordering across
modes; membership events arrive in-band as 0-byte MSG_OOB messages with
join-before-first / leave-after-last guarantees. **[assumed]** (source/doc-read;
untested locally) — phase-4 material.

### 2.7 Cluster ops and encryption

- Single node / dev: `sudo modprobe tipc` is the **entire** setup — zero-config
  node-local operation **[verified]**. The module never auto-loads: `tipc.ko`
  declares no `MODULE_ALIAS_NETPROTO`, so `socket(AF_TIPC, …)` returns
  `EAFNOSUPPORT` without loading it, even as root **[verified]**. Persist via
  `/etc/modules-load.d/tipc.conf`.
- Cluster: `tipc bearer enable media eth device <dev>` per node (L2; identity
  auto-derived from MAC on ≥ 4.17 — **[verified]** in the netns cluster: identity
  auto-derived, cluster formed in < 2 s, node numbers became nonzero hashes) or
  `media udp` for L3/cloud (replicast auto-learns peers; only one side needs config).
  Bearer config needs CAP_NET_ADMIN; **velo itself does nothing privileged** —
  unprivileged socket/bind/connect verified, including inside a default-seccomp
  Docker container.
- Containers/K8s: TIPC is per-netns; a pod with its own netns is its own TIPC node
  with no bearer, so cross-pod TIPC does not work under normal CNI **[verified]**
  (cross-netns send → `EHOSTUNREACH`). Supported: `hostNetwork: true`, bare metal, VMs.
- Encryption: AEAD AES-GCM via `tipc node set key`, kernel ≥ **5.5** (uapi
  `tipc_aead_key` + `net/tipc/crypto.c` present in v5.5 — corrected from earlier
  "5.6" claims) and `CONFIG_TIPC_CRYPTO=y` (build-time; enabled on this host).
  Without keys, all traffic is cleartext.
- Performance positioning (measured here + `Documentation/networking/tipc.rst`):
  TIPC wins latency everywhere and intra-node/inter-container throughput
  (64 B RTT 4.4 µs vs 7.7 µs TCP; 65,000 B throughput ~1.8× loopback TCP); raw
  inter-node bulk throughput trails tuned TCP (no segmentation offload).

---

## 3. Empirical validation on this machine

Kernel 6.14.0-1015-nvidia (aarch64), tipc.ko v2.0.0, glibc ≥ 2.36, Rust 1.95,
libc crate 0.2.186, tokio 1.52.3, socket2 0.6. Artifacts: `/tmp/tipc-probe/*.c`,
`/tmp/tipc-probe-rs/`, `/tmp/tipc_critic/critic.c`, kernel sources fetched to
`/tmp/tipc_*_614.c`. What was actually run and proven:

**Module & basics.** `sudo modprobe tipc` works passwordless; zero-config node-local
RDM/SEQPACKET/STREAM messaging with no bearer/identity/netid; `rmmod` then
`socket(AF_TIPC, SOCK_RDM)` → `EAFNOSUPPORT` (errno 97), deterministic, no autoload —
the builder fail-fast and CI preflight probe are sound and side-effect-free.

**Sockets & async.** Non-blocking semantics are standard: `EAGAIN` on empty recv,
`EPOLLIN` on arrival, `EINPROGRESS` connect completing with `SO_ERROR=0` + `EPOLLOUT`;
edge-triggered epoll works. A full Rust round trip — hand-defined 16-byte
`#[repr(C)] SockaddrTipc` (byte-verified against C sizeof: sockaddr_tipc=16,
tipc_subscr=28, tipc_event=48), raw bind/sendto/recvfrom, then
`tokio::io::unix::AsyncFd` async recv — completed end-to-end. **libc 0.2.186 ships
only `AF_TIPC`/`PF_TIPC`/`SOL_TIPC`** (`E0425` for `libc::sockaddr_tipc`); every
struct and `TIPC_*` constant must be hand-defined.

**STREAM contract probes.** 131,072-byte in-flight cap then EAGAIN with EPOLLOUT
withheld until drain, zero loss; 200-message ordering; 1 MiB single send intact;
data flushed before close in both close() and shutdown() paths; `shutdown(SHUT_WR/RD)`
→ `EINVAL`, `SHUT_RDWR` → 0; plain close → peer `ECONNRESET`, explicit
`shutdown(SHUT_RDWR)` → clean EOF; connect not writable until remote `accept()`;
5 queued SYNs all accepted later; stale-ref connect → `ECONNREFUSED` in 0.04 ms;
SIGKILL of a connected child → `ECONNRESET`.

**RDM disqualification.** Default silent drop under overflow; `DEST_DROPPABLE=0`
reject-returns truncated at 1,024 bytes, 14,563/20,000 returned under flood; rejects
raise EPOLLIN on the sender (error path is AsyncFd-drivable, but data and rejects
share one receive queue).

**Two-node cluster (netns + veth + eth bearers).** Bearer enable with auto-derived
identity; cluster formation < 2 s; cross-node socket-addr and service-addr connects;
silent-partition `ECONNRESET` at 2,138 ms; interface-down at 10 ms; cross-netns
isolation without bearers (`EHOSTUNREACH`).

**Topology.** PUBLISHED/WITHDRAWN at 0.33/0.30 ms with publisher `{ref,node}` in
the event.

**Encoding.** `rmp_serde::to_vec` (positional arrays) **breaks in both directions**
on any added struct field ("array had incorrect length"); `to_vec_named` +
`#[serde(default)]` round-trips both directions. This kills the naive
"msgpack field-tolerance" endpoint-evolution story and dictates §5.2.

**Perf.** tipcutils benchmark: TIPC 64 B RTT 4.4 µs vs loopback TCP 7.7 µs;
65,000 B 142.8 vs 91.7 Gb/s (run-to-run variance 30–50%, ordering stable).

**Netns-nonce inputs (§5.3).** `boot_id` identical inside an `unshare -Un` child;
`readlink /proc/self/ns/net` stable per netns (`net:[4026531840]` host, every
process) and distinct in a fresh netns (`net:[4026532986]`); `stat()` on the same
path returned fabricated, varying inodes under the development sandbox — the nonce
must parse the readlink target, not stat.

**Not validated here** (named residual risk, §10): multi-host bearer behavior
(only netns-pair), UDP-bearer MTU/fragmentation in real clouds, AEAD overhead,
SEQPACKET beyond the 66,000-B cap check, group messaging.

---

## 4. Binding strategy

**Chosen: hand-written minimal sys layer** — `sys.rs` (~7 `#[repr(C)]` structs,
~30 constants transcribed from the frozen UAPI header) + `socket2::Domain::from(AF_TIPC)`
+ `unsafe SockAddr::new` + `tokio::io::unix::AsyncFd`, entirely inside
`lib/velo/src/transports/tipc/`. The AsyncFd wrapper follows tokio-vsock
(Apache-2.0, actively maintained, 484 LOC for its whole async layer), the proven
pattern for exotic `AF_*` families; netlink-sys uses the same approach. Zero new
dependencies: socket2 0.6 and tokio are direct deps (`lib/velo/Cargo.toml`); `libc`
is already in the lockfile and becomes a direct dep under
`[target.'cfg(target_os = "linux")'.dependencies]` (one Cargo.toml line; add to the
`cargo-machete` ignored list if flagged, precedent at `lib/velo/Cargo.toml:103`).
Compile-time layout asserts (sizeof/offsets/constants vs libc) run on every CI build
with no kernel module required; layouts were byte-verified against C this session
and are frozen kernel UAPI. Optionally upstream the definitions to rust-lang/libc
later to retire the transcription risk.

**Rejected:**

- **crates.io `tipc` 0.1.2** (MIT) — dead since 2021-03, sync-only, no topology API,
  and **verified to fail compilation today**: its bindgen 0.55.1 build-dep panics on
  glibc ≥ 2.36 headers (`__atomic_wide_counter … is not a valid Ident`, reproduced
  locally). It is FFI over a vendored copy of tipcutils' libtipc.c, not raw syscalls.
- **`iris-ng` 0.1.2** — LGPL-2.1 (static-link complication), 0 stars, one-day project,
  same bindgen+cc bitrot class. Strictly dominated.
- **bindgen against system libtipc.so** — no distro ships libtipc.so (`ldconfig`
  empty); would devolve into vendoring the C anyway. libtipc is a 1,045-LOC
  convenience wrapper over syscalls; there is nothing in it worth FFI-ing to.
- **Separate `velo-tipc-sys` workspace crate** — violates the spirit of the two-crate
  rule (CLAUDE.md hard rule 1); buys nothing over a module.
- **genetlink crates (neli / rust-netlink)** for bearer config — no crate ships TIPC
  attribute definitions; velo never needs privileged config anyway. Operators use the
  iproute2 `tipc` CLI (ships in Ubuntu/Debian iproute2, dpkg-verified).

---

## 5. Phase 1 — messenger `Transport`

Implements all 9 methods of `velo_ext::Transport`
(`lib/velo-ext/src/transport.rs:297-364`). Structurally a copy of
`uds/transport.rs` (the canonical "TCP pattern on a non-TCP socket" precedent) with
`PathBuf` → `TipcEndpoint` and `UnixStream` → `TipcStream`. Zero velo-ext changes:
`TransportKey` is an arbitrary string, WorkerAddress values are fully opaque bytes
(`lib/velo-ext/src/id/address.rs`), `TransportError::{NoEndpoint, InvalidEndpoint}`,
all five `HealthCheckError` variants, and the five `TransportObservability` methods
cover the entire mapping — confirmed against source by the API-stability review.

### 5.1 Module layout & binding internals

```
lib/velo/src/transports/tipc/
  mod.rs        — docs + re-exports: TipcTransport, TipcTransportBuilder,
                  TipcStream, TipcEndpoint (pub: required by the external
                  tipc_shutdown.rs test crate, which must dial the advertised
                  endpoint and speak TcpFrameCodec — same as TcpShutdownClient,
                  tests/transports/common/mod.rs:552-606)
  sys.rs        — #[repr(C)] structs + constants + socket2 bridge + layout tests
  endpoint.rs   — TipcEndpoint encode/decode (to_vec_named) + cross-version tests
  socket.rs     — socket creation, sockopts, getsockname, tipc_available() probe
  stream.rs     — TipcStream: AsyncFd AsyncRead/AsyncWrite + nonblocking connect
  listener.rs   — two-phase bind + accept/serve/route/drain
  topology.rs   — NodeStateWatch (TIPC_NODE_STATE node-up cache) + VeloServiceWatch
                  (TIPC_SUB_PORTS subscription over the VELO service type:
                  instance → live publisher {ref, node})
  transport.rs  — Transport impl + builder + unit tests
```

`sys.rs` constants (verbatim from `/usr/include/linux/tipc.h`, all asserted against
the header values in layout tests): `AF_TIPC=30`, `SOL_TIPC=271`,
`TIPC_SERVICE_RANGE=1`, `TIPC_SERVICE_ADDR=2`, `TIPC_SOCKET_ADDR=3`,
`TIPC_CLUSTER_SCOPE=2`, `TIPC_NODE_SCOPE=3`, `TIPC_RESERVED_TYPES=64`,
`TIPC_CONN_TIMEOUT=130`, `TIPC_IMPORTANCE=127`, `TIPC_NODELAY=138`,
`TIPC_MAX_USER_MSG_SIZE=66000`, `TIPC_TOP_SRV=1`, `TIPC_NODE_STATE=0`,
`TIPC_SUB_PORTS=0x01`/`TIPC_SUB_SERVICE=0x02`/`TIPC_SUB_CANCEL=0x04`,
`TIPC_PUBLISHED=1`/`TIPC_WITHDRAWN=2`/`TIPC_SUBSCR_TIMEOUT=3`, plus `tipc_subscr`
(28 B) and `tipc_event` (48 B) for `topology.rs` and phase 3.

`stream.rs` — the corrected AsyncFd wrapper:

- `AsyncRead`/`AsyncWrite` via `poll_read_ready`/`poll_write_ready` + `try_io`,
  retry on WouldBlock/Interrupted (tokio-vsock pattern). Kernel backpressure
  (EPOLLOUT withheld) surfaces as WouldBlock → stalls the writer task → fills its
  flume queue → `SendBackpressure` to callers. No silent loss possible.
- **`poll_shutdown` → `shutdown(Shutdown::Both)`** — TIPC has no half-close
  (§2.3.1). Safe because queued data flushes before the close signal [verified].
- **Graceful-close discipline**: every owned teardown path (writer-task exit,
  health-probe drop, transport shutdown) calls `shutdown(Both)` explicitly before
  drop so the peer sees clean EOF, **and** the listener maps
  `io::ErrorKind::ConnectionReset` with an empty partial-frame buffer to the
  graceful-close branch (§2.3.2). Without both, every routine disconnect pollutes
  `velo_transport_rejections_total{reason="decode_error"}` and error logs. A test
  pins this.
- **Close-blocking hazard** (§2.3 last item): the final `close(2)` can block up to
  8 s under congestion regardless of O_NONBLOCK. Teardown paths therefore drop the
  fd via `tokio::task::spawn_blocking` when the writer exits with a congested queue;
  documented in the module.
- `connect()`: nonblocking connect tolerating EINPROGRESS, then
  `writable().await` + `SO_ERROR` check, wrapped in `tokio::time::timeout` by the
  caller. Documented deviation from TCP: completion is gated on the remote
  application's `accept()` (§2.3.3), so the 5 s connect timeout also bounds
  "remote accept loop wedged", and the kernel's own 8 s `TIPC_CONN_TIMEOUT` never
  engages first.

`Framed<TipcStream, TcpFrameCodec>` then works unchanged — the codec needs only
`AsyncRead + AsyncWrite + Unpin` and is `pub` via `velo::transports::tcp`
(UDS reuses it verbatim, `uds/transport.rs:28`).

### 5.2 Endpoint encoding — TIPC has no host:port

Opaque value under WorkerAddress key `"tipc"`, encoded with
**`rmp_serde::to_vec_named`** (mandatory: positional `to_vec` breaks both decode
directions on any future field addition — empirically pinned, §3) and every
post-v1 field carrying `#[serde(default)]`:

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TipcEndpoint {
    pub version: u8,            // = 1
    pub service_type: u32,      // bound {type, instance} — ops + phase-3 discovery
    pub service_instance: u32,
    pub node: u32,              // 32-bit node hash from getsockname; 0 = identity unset
    pub socket_ref: u32,        // listener port ref — PRIMARY connect target
    pub netid: u32,             // cluster identity; register()-time equality gate
    pub node_id: [u8; 16],      // 128-bit identity via ioctl(SIOCGETNODEID); zeros if unset (caller zero-inits — kernel skips the write when unset)
    pub netns_nonce: u64,       // xxh3_64(boot_id ++ netns ino) — a pure function of
                                // (boot, netns): identifies the TIPC stack. ino parsed
                                // from readlink("/proc/self/ns/net") = "net:[<ino>]",
                                // NOT stat() — see §5.3
    pub scope: u8,
}
```

Why socket-addr-primary: service-address lookup is anycast (§2.1) — exact
`{socket_ref, node}` connects are immune to instance collisions, precisely analogous
to TCP's `ip:port`. The ref is ephemeral per-socket, but so is the whole
WorkerAddress (rebuilt per process start under a fresh `InstanceId`), so staleness
semantics match TCP: stale ref → fast ECONNREFUSED → `on_error`. The service binding
is kept anyway — it is the cluster-visible announcement phase 3 subscribes to, and
it is free. A unit test pins endpoint cross-version tolerance (v1 bytes → vN struct
and vN bytes → v1 struct) and the `"tipc"`/`"tipc-stream"` key discipline
(`WorkerAddressBuilder::add_entry` errors on duplicate keys).

Doc note: kernel-assigned 32-bit refs can be recycled after close. At *register*
time this is fully closed: the §5.3 gate accepts a remote endpoint only if the name
table currently shows `{service_type, service_instance}` published by exactly
`{socket_ref, node}` (publications auto-withdraw on socket close/crash in ~0.3 ms,
§2.5), and a recycled ref cannot satisfy the triple because the new holder binds a
different random instance. What remains is the unavoidable point-in-time window —
the peer dying *after* register — which is the normal death path (connect
`ECONNREFUSED`-equivalent or `ECONNRESET` → `on_error`), identical to TCP semantics.

### 5.3 Builder, bind, and `register()`

```rust
TipcTransportBuilder::new()
    .key("tipc")                      // default
    .service_type(0x56454C4F)         // "VELO"; validated >= TIPC_RESERVED_TYPES (64)
    .service_instance(/* default: random u32 */)
    .scope(TipcScope::Cluster)
    .channel_capacity(256)
    .connect_timeout(Duration::from_secs(5))
    .build()?                         // pre-binds (TCP pattern): socket -> bind
                                      // service range -> listen -> getsockname
                                      // -> TipcEndpoint -> WorkerAddress entry.
                                      // EAFNOSUPPORT mapped to "TIPC kernel module
                                      // not loaded; run `sudo modprobe tipc`".
```

Pre-binding in `build()` (TCP pattern, `tcp/transport.rs:740-744`) means `address()`
is final before `start()` and `start()` cannot fail on bind. Two TIPC-specific
hardening steps at build time:

- **Collision probe + re-roll**: a one-shot topology lookup for our own
  `{type, instance}`; if another publisher already holds it, re-roll the random
  instance (the UDS stale-socket-file analog). Cheap insurance even though routing
  is by socket ref.
- **NodeStateWatch + VeloServiceWatch** (`topology.rs`, ~300 LOC incl. the
  pending-endpoint matcher feeding the re-register hook, one
  SOCK_SEQPACKET connection to `{1,1}` carrying two subscriptions): the
  `TIPC_NODE_STATE` subscription maintains `DashMap<u32 /*node*/, bool /*up*/>`;
  a `TIPC_SUB_PORTS` subscription over `{VELO_TYPE, 0, 0xffffffff}` maintains
  `DashMap<u32 /*instance*/, (u32 /*ref*/, u32 /*node*/)>` — the live name-table
  view of every velo TIPC listener in the cluster (and the phase-3 discovery
  primitive, grown early). The watches are spawned at build() but `start()` awaits
  subscription establishment + initial replay (sub-millisecond, §2.5) before
  returning, so register() never races cold caches; any rejection that does occur
  on the remote arm parks the peer for event-driven re-registration (§5.3) rather
  than demoting it permanently.

`register()` — the reachability gate (UDS host-affinity analog;
`Err(NoEndpoint)` is normal control flow that makes `VeloBackend::register_peer`
(`transports.rs:419-459`) silently promote the next transport, proven by
`tests/transports/uds_host_affinity.rs`):

```rust
enum Gate {
    Reachable,  // register now
    Never,      // permanent property of this endpoint — reject outright
    NotYet,     // stale OR still propagating — park for event-driven re-register
}

fn register(&self, peer_info: PeerInfo) -> Result<(), TransportError> {
    let ep: TipcEndpoint = decode(...)?;                  // NoEndpoint / InvalidEndpoint
    let local = &self.local_endpoint;
    let verdict = if ep.netid != local.netid {
        Gate::Never                                       // wrong cluster
    } else if ep.netns_nonce == local.netns_nonce {
        Gate::Reachable                                   // same netns ⇒ same TIPC stack
                                                          // ⇒ {ref, node} directly routable
    } else if ep.node == 0 || local.node == 0 {
        Gate::Never                                       // zero-config TIPC never crosses
                                                          // a netns boundary (§2.7)
    } else if ep.node == local.node {
        Gate::Never                                       // foreign stack claiming OUR node
                                                          // value: {ref, node} would route into
                                                          // our own stack — duplicate-identity
                                                          // misconfig, forced closed
    } else if self.node_state.is_up(ep.node)              // bearer path: node alive AND the
        && self.service_watch.publication_matches(        // peer's exact listener socket is
            ep.service_instance, ep.socket_ref, ep.node)  // live in the name table right now.
    {
        Gate::Reachable                                   // Both checks O(1), zero RTT.
    } else {
        Gate::NotYet                                      // node down or publication absent:
                                                          // indistinguishable-now (stale vs
                                                          // cold-start propagation)
    };
    match verdict {
        Gate::Reachable => {
            self.pending.remove(&peer_info.instance_id());
            self.peers.insert(peer_info.instance_id(), ep);
            self.update_peer_gauge();
            Ok(())
        }
        Gate::NotYet => {
            // Parked; the topology watch re-drives this PeerInfo through the
            // re-register hook when a matching PUBLISHED / node-up event arrives.
            self.pending.insert(peer_info.instance_id(), peer_info);
            Err(TransportError::NoEndpoint)
        }
        Gate::Never => Err(TransportError::NoEndpoint),
    }
}
```

This replaces the earlier "optimistic remote register" (the panel's biggest flagged
risk: an unreachable TIPC peer becoming primary) with an exact node-state check.
Synchronous and non-blocking, like the UDS `fs::metadata` gate.

**The nonce must be a pure function of (boot, netns)** — nothing more, nothing less
(two stop-review corrections landed here). The derivation is
`xxh3_64(boot_id ++ netns_ino)` where `netns_ino` is parsed from the kernel-generated
`readlink("/proc/self/ns/net")` target (`"net:[<ino>]"`) and `boot_id` from
`/proc/sys/kernel/random/boot_id`.
Both rejected alternatives failed one direction of the "equal nonce ⇔ same TIPC
stack" property the gate depends on:

- *Host-scoped* (machine-id ++ boot_id, the first draft): false **positives** — TIPC
  is per-netns, so same-host peers in different network namespaces have disjoint
  stacks, yet matched. A `{ref, node=0}` connect then either fails at send time or,
  on ref collision, reaches an unrelated local socket and dies at frame decode —
  exactly the late-failure mode the gate exists to prevent.
- *Mixing in machine-id* (second draft): false **negatives** — `/etc/machine-id` is
  read through the *mount* namespace, so two containers sharing one netns (k8s pod
  containers; a `hostNetwork` container vs. a host process) see different
  machine-ids and would compute different nonces for the same TIPC stack, silently
  gating co-located peers to TCP.

The chosen inputs avoid both: the nsfs inode uniquely identifies a netns within a
boot and is identical for every process in that netns regardless of mount ns;
`boot_id` is kernel-global (identical in every container, no file dependency) and
128-bit random per boot, supplying cross-host and cross-reboot uniqueness —
necessary because nsfs inode values carry near-zero cross-host entropy (the initial
netns has the same well-known inode on practically every Linux host —
**[verified]** `net:[4026531840]` here).

**Read the inode via `readlink`, not `stat`** **[verified]**: on this box, all
processes in the host netns agree on the readlink target `net:[4026531840]`
(stable across reads and processes; an `unshare -Un` child correctly shows a
distinct `net:[4026532986]`), but `stat()` on the same `/proc/*/ns/net` path
returned *fabricated, per-process-varying* inode numbers under the development
sandbox in use — syscall-interposing sandboxes (gVisor, seccomp-notify based)
can virtualize `stat` on procfs while passing `readlink` through to the
kernel-generated name. A stat-derived nonce would silently differ between
same-stack processes in such environments (the machine-id failure mode again);
the readlink string is generated from the real nsfs inode and survives. The
man-pages-blessed `st_dev`+`st_ino` comparison remains valid on bare metal, but
readlink parsing is strictly more robust and costs the same.

Residual risk: the kernel recycles nsfs inodes (`get_next_ino` counter) after a
netns is destroyed, so a *stale* discovery endpoint could nonce-match a later netns
on the same boot — the same failure class as recycled TIPC socket refs (§5.2 doc
note) and TCP port reuse: fast connect failure or frame-decode death, surfaced via
`on_error`.

Equal nonce ⇒ same TIPC stack ⇒ exact-ref connect is valid regardless of whether
`node` is 0 or bearer-derived. Unequal nonce with bearers on both sides falls
through to the topology node-state check — the §3 netns-pair veth cluster exercises
exactly this path. Unequal nonce with either side at `node == 0` is unreachable by
construction (§2.7: cross-netns send → `EHOSTUNREACH`). Unequal nonce with
`ep.node == local.node` is **explicitly forced closed** (third stop-review
correction — an earlier draft let it fall through to the node-state check, claiming
that would fail closed; it may not: TIPC publishes the own node's
`{TIPC_NODE_STATE, addr}` entry into the name table once the node is active, so
`is_up(local.node)` can be *true*, and connecting to `{ref, node == local.node}`
routes into our *own* stack, reaching whatever local socket holds that ref). A
genuinely remote peer can never legitimately carry our node value — TIPC rejects
duplicate identities at link establishment — so the explicit arm is strictly
correct: it closes the duplicate-identity misconfiguration without introducing any
false negative.

The remote arm is verified, not trusted (fourth stop-review correction — an earlier
draft accepted endpoints naming any live third node, leaving stale/garbage refs to
fail at send time, which is exactly the registered-but-broken-primary state the gate
exists to prevent): node liveness alone proves nothing about the *ref*, so the gate
additionally requires that the name table currently shows the endpoint's
`{VELO_TYPE, service_instance}` published by exactly `{socket_ref, node}`, served
O(1) from the VeloServiceWatch cache. This is sound because publications
auto-withdraw on socket close/crash with no lease or keepalive (**[verified]**
~0.3 ms, §2.5) — a dead listener's endpoint fails closed to TCP immediately — and a
recycled ref cannot match because its new holder binds a different random service
instance (and would have to match all three fields). Two consequences to document:
remote TIPC promotion requires the listener bound with `TIPC_CLUSTER_SCOPE` (the
builder default — a `NODE_SCOPE` binding is invisible in remote name tables and
correctly gates to TCP); and the check is point-in-time — a peer dying after
register() is the normal death path (§5.2 doc note), identical to TCP.

**Negative answers must not be permanent** (fifth stop-review correction).
`Transport::register` is synchronous and one-shot, and `VeloBackend::register_peer`
assigns the primary at that instant (`transports.rs:419-459`); nothing in the
runtime revisits the decision for an already-registered peer — the discovery slow
path (`transports.rs:212`) only fires for *unknown* peers. An earlier draft called
the watch warm-up race "benign — re-registration recovers," but nothing ever
triggered a re-registration: a peer registered at cluster cold-start (TIPC links
still forming, publication not yet replicated into our name table) would be demoted
to TCP *forever* — and cold-start is the common ordering, since discovery (etcd)
can deliver PeerInfo faster than a forming TIPC link converges (~1–2 s, §3). Three
mechanisms close this:

1. **Deterministic warm-up** — `start()` returns a `BoxFuture`; it awaits topology
   subscription establishment and initial replay (sub-ms locally, §2.5) before
   returning, so `register()` never races cold caches in the normal lifecycle.
2. **Transient vs. exact rejections** — netid mismatch, cross-netns `node == 0`,
   and duplicate-identity are permanent properties of *that endpoint* (a restarted
   peer arrives as a fresh endpoint through discovery): `Gate::Never`, rejected
   outright. Remote-arm failures are indistinguishable-now: a stale endpoint's
   publication stays absent forever, a propagating one appears within ms–s. These
   are `Gate::NotYet` — the full `PeerInfo` is parked in
   `pending: DashMap<InstanceId, PeerInfo>` (bounded by known-peer count; each
   fresh register() for the same InstanceId overwrites its entry).
3. **Event-driven re-registration** — the topology watch already receives
   `TIPC_PUBLISHED` and node-up events. When one matches a parked endpoint's
   `(instance, ref, node)` triple (or a node-up matches its node), the watch task
   re-submits the stored `PeerInfo` through a re-register hook: plain in-tree
   plumbing, zero velo-ext change — the velo builder, after constructing the
   `VeloBackend`, hands the TIPC transport an `Arc<dyn Fn(PeerInfo) + Send + Sync>`
   that calls `VeloBackend::register_peer`. That call is re-entrant by design —
   `primary_transport.insert()` overwrites (`transports.rs:452-456`) — so the
   second pass re-runs the full priority selection with warm caches and a present
   publication, promoting TIPC over TCP. Mid-stream primary switches are already
   within backend semantics (`set_transport_priority` exists; registration is
   idempotent).

Net behavior: cold-start pairs exchange their first messages over TCP for the
name-table convergence window (milliseconds to a couple of seconds), then flip to
TIPC automatically; genuinely stale endpoints stay parked and never fire an event.
Out-of-tree transport authors cannot reach the private hook — their equivalent is
re-calling the public `Velo::register_peer` from their own watcher task.

### 5.4 Send path — fire-and-forget + `SendBackpressure`

Byte-for-byte the UDS shape (`uds/transport.rs:293-334`): per-peer
`flume::bounded(256)` writer channels in `DashMap<InstanceId, ConnectionHandle>`;
fast-path `try_send`; `Full` → `record_send_backpressure()` +
`Err(SendBackpressure::new(send_async …))`; `Disconnected` → drop guard,
`remove_if(is_disconnected)`, gauge refresh, slow path via atomic `entry()`
get-or-create spawning `connection_writer_task`. All delivery failures →
`task.on_error(original header, original payload, msg)` and `Ok(())` — never a
delivery error in the Result (trait contract, `transport.rs:305-314`). The writer:

```text
connect (socket-addr {ref, node}, 5s timeout, cancellable)
  -> set TIPC_NODELAY=1 (best-effort: ignore ENOPROTOOPT/EINVAL — sockopt exists
     only on kernels >= 5.5; hard-failing would kill every connection on e.g.
     RHEL8's 4.18)
  -> set SO_RCVBUF 2 MiB on this side's sockets where we receive (accepted conns);
     NO SO_SNDBUF (flow-control no-op on TIPC, §2.2); NO TIPC_IMPORTANCE on the
     hot path (ignored for connected sockets)
  -> loop select!{ biased; cancel, rx.recv_async() } -> TcpFrameCodec::encode_frame
  -> on ANY exit: shutdown(Both) [graceful-close discipline], drain rx firing
     on_error("Connection closed") per queued task, drop(rx),
     remove_if(is_disconnected), gauge refresh; spawn_blocking the final close if
     the conn was congested (8s close-block hazard).
```

Dead-peer behavior matches TCP/UDS: write error breaks the loop, queued tasks error
out, stale handle self-removes, next send implicitly reconnects. No background retry.

Documented operational difference (not a contract difference): TIPC's per-connection
in-flight window is ~128 KiB until receiver acks grow it (§2.2), so
`SendBackpressure` engages earlier than TCP's 2 MiB-buffered writers for bulk
traffic; and link congestion is per node-pair, so one slow *node* can stall writer
tasks for all peers on that node simultaneously. Both noted in `docs/transports.md`.

### 5.5 Inbound listener → `TransportAdapter` routing

`start()` stores the runtime handle + shared `ShutdownState`, takes the pre-bound
listener, spawns serve. The serve loop mirrors `uds/listener.rs` exactly:

- accept loop `select!{ biased; teardown_token.cancelled(), accept() }`; per
  connection `Framed<TipcStream, TcpFrameCodec>`.
- **Drain gate** per decoded frame: `is_draining() && msg_type == Message` →
  `record_rejection(DrainRejected)` + write back a `ShuttingDown` frame echoing the
  request header with empty payload + continue. Response/Ack/Event always pass
  (pinned by the shared shutdown scenarios).
- `route_frame`: Message → message_stream, Response|ShuttingDown → response_stream,
  Ack|Event → event_stream; `record_frame(Inbound, label, bytes)` before send;
  route failure → `record_rejection(RouteFailed)` + error handler.
- **Close handling (TIPC-corrected)**: EOF → graceful; `ConnectionReset` with an
  empty decode buffer → graceful (peer used plain close); ConnectionReset
  mid-frame or any other decode error → `record_rejection(DecodeError)` + drop.
- `maybe_shrink_read_buffer` with `VELO_TIPC_SHRINK_THRESHOLD` (default 8 MiB).

`begin_drain` mirrors UDS — flip the captured `ShutdownState` — as belt-and-braces
so the transport is drain-correct standalone. (Corrected rationale: the drain
scenarios at `tests/transports/common/scenarios.rs:494-495` flip the shared adapter
state directly on line 495, which is why TCP's no-op passes today; nothing *requires*
the override, but it costs three lines and removes an ordering trap.)

`shutdown`: teardown-token cancel (accept loop + readers) + transport cancel token
(writers) + `connections.clear()` + gauges; safe pre-`start()`.

### 5.6 `check_health`

Two-tier:

1. **Zero-RTT fast path**: live writer channel → Ok (TCP algorithm,
   `tcp/transport.rs:427-473`); else a name-table/topology check — the
   `NodeStateWatch` answers "node down" instantly, and a one-shot topology lookup
   comparing the published binding's ref against the registered `socket_ref`
   answers "process gone" without a packet to the peer.
2. **Probe-connect fallback** to the exact `{socket_ref, node}` under the caller's
   timeout → `Ok` / `NeverConnected` / `ConnectionFailed` / `Timeout` /
   `PeerNotRegistered` — keeping TCP-equivalent semantics.

Documented semantic deviation: because TIPC connect completes only on remote
`accept()` (§2.3.3), a probe-connect measures "remote accept loop responsive", not
"listener bound" — a wedged runtime yields `Timeout` for a live process. Arguably a
feature; explicitly documented either way. Probe sockets use `shutdown(Both)` before
drop so probes don't pollute the peer's rejection metrics.

### 5.7 Observability

`set_observability` → `OnceLock<Arc<dyn TransportObservability>>` + immediate gauge
refresh. Emission split per the verified backend discipline
(`transports.rs:540-592`): the transport emits inbound `record_frame`,
`record_rejection(DrainRejected|DecodeError|RouteFailed)`,
`record_send_backpressure`, and the two gauges; outbound frames
(`finalize_send_outcome`) and `SendError` rejections
(`InstrumentedTransportErrorHandler`) are the backend's — no double-counting.

### 5.8 The 6-step in-tree checklist, instantiated

1. **Feature**: `tipc = []` in `lib/velo/Cargo.toml [features]`; `libc` as a
   linux-target direct dep; machete ignore if flagged.
2. **Module line**: `#[cfg(all(target_os = "linux", feature = "tipc"))] pub mod tipc;`
   in `lib/velo/src/transports.rs` (file is `#![deny(missing_docs)]` — all pub items
   documented).
3. **Module dir**: as §5.1.
4. **Tests**: `TipcFactory` + `TestTransportHandle::new_tipc()` /
   `TestCluster::new_tipc(size)` in `tests/transports/common/mod.rs` —
   **gated `#[cfg(all(feature = "tipc", target_os = "linux"))]`**, not feature-only:
   `common/` compiles into every transports test binary, and a feature-only gate
   breaks macOS under `--all-features` (the repo's own `#[cfg(unix)]` UDS gate at
   `common/mod.rs:30` is the precedent). `tests/transports/tipc_integration.rs`
   (`#![cfg(velo_tipc)]` + `transport_integration_tests!(TipcFactory)` — 20
   scenarios incl. drain gating, 900 KB frames, 3-node mesh),
   `tipc_shutdown.rs` (+ `ShutdownTestClient` over the pub `TipcStream`),
   `tipc_node_affinity.rs` (mirrors `uds_host_affinity.rs`: mismatched
   netid/netns_nonce → `NoEndpoint` → TCP promoted — including the same-host
   different-netns case, which a host-keyed nonce would wrongly pass, and the
   unequal-nonce `ep.node == local.node` duplicate-identity case, which must be
   forced closed rather than left to the node-state map, and a stale-endpoint case:
   bind, snapshot the endpoint, close the socket → register → `NoEndpoint` because
   the publication was withdrawn, and the cold-start recovery case: register while
   the publication is absent → TCP primary + endpoint parked → bind the service →
   watch event fires the re-register hook → primary flips to TIPC; the hook/matcher
   logic is also unit-tested with injected topology events), plus matching `[[test]]`
   entries with `required-features = ["tipc"]`.
5. **Examples**: `TransportType::Tipc` arm in `examples/src/lib.rs` +
   `tipc = ["velo/tipc"]` in `examples/Cargo.toml`, gated
   `cfg(all(feature = "tipc", target_os = "linux"))` (UDS precedent
   `examples/src/lib.rs:23,49`). Plus a `tipc-doctor` diagnostic example: module
   loaded? identity? bearers? peers visible? netid?
6. **Docs**: `docs/transports.md` — summary-table row, endpoint format, § TIPC
   (ops/modprobe/bearer/netid/key guidance, env vars, the §5.4 backpressure and
   congestion-coupling notes, security paragraph).

TIPC-specific tests beyond the checklist: ECONNRESET-on-plain-close handled as
graceful (no DecodeError metric) — pinned; a backpressure flood test cloning the TCP
`backpressure.rs` (drive the receiver into the conn-window stall); endpoint
cross-version decode; NodeStateWatch warm-up race; root-gated `#[ignore]` netns-pair
veth-bearer tests (node death → ECONNRESET → writer drain → on_error; register-gate
TCP fallback across netns) — the only true partition coverage any in-tree transport
would have.

### 5.9 File-by-file estimate

| File | LOC |
|---|---|
| `tipc/mod.rs` | 30 |
| `tipc/sys.rs` (+ layout tests) | 240 |
| `tipc/endpoint.rs` (+ cross-version tests) | 170 |
| `tipc/socket.rs` | 160 |
| `tipc/stream.rs` | 320 |
| `tipc/listener.rs` | 390 |
| `tipc/topology.rs` | 300 |
| `tipc/transport.rs` (+ unit tests) | 760 |
| tests: common factory + integration + shutdown + affinity + backpressure + netns | 380 |
| Cargo.toml / transports.rs / examples wiring | 40 |
| `docs/transports.md` § TIPC | 70 |
| CI job + probe script + canary | 50 |
| **Total** | **~2,800** |

Reference scale: uds = 1,191 + 443 + 23 LOC; the delta is the sys/AsyncFd layer and
the topology watches. Estimate 4–6 focused days including CI iteration.

---

## 6. Phase 2 — streaming `FrameTransport`

`lib/velo/src/streaming/tipc_transport.rs` (~550 LOC), key `"tipc-stream"`
(the `<scheme>-stream` convention), a SOCK_STREAM clone of `TcpFrameTransport`:

- One shared listener (own service `{type, instance}` + socket-addr advertisement,
  same `TipcEndpoint` encoding under `"tipc-stream"`); **one fresh TIPC connection
  per `(anchor_id, session_id)` attach**; 16-byte BE handshake first on the wire,
  20 s timeouts both sides; one-shot `DashMap<(u64,u64), flume::Sender>` slot
  registry with the TCP model's 60 s ACCEPT_TIMEOUT expiry (not gRPC's
  warn-and-overwrite); unknown/stale handshake → warn + drop.
- Frames as `TcpFrameCodec` Message frames with empty header; the ordered/loss-free
  same-channel MUST at `lib/velo-ext/src/streaming.rs:36-49` is satisfied by TIPC
  connection semantics (sequenced links + e2e block flow control — both [verified]).
- Connect-side pump: terminal-sentinel byte-compare, stop-and-close after terminal.
  Bind-side pump: **Dropped injection on abrupt close** when
  `!last_was_terminal && !consumer_dropped`. TIPC makes this *better* than TCP:
  queued data is flushed before the close signal [verified], so a written-then-dropped
  sentinel is never lost, and node loss aborts connections in ~1.5–2.5 s [verified,
  netns] — beating the 15 s heartbeat watchdog.
- **Correction inherited from §2.3.1**: the TCP skeleton's pump teardown calls
  `stream.shutdown().await` (= `SHUT_WR`) at `tcp_transport.rs:329` and `:522`; the
  TIPC clone must use `Shutdown::Both` (TipcStream::poll_shutdown already does).
- `register()` keyed by `WorkerId` per the trait docs; metrics installed via the
  concrete pre-type-erasure `set_metrics(Arc<VeloMetrics>)`, not the trait.
- Conformance: `run_transport_tests!` + clones of `unknown_session_is_rejected`,
  `dropped_sentinel_injected_on_abrupt_close`, `no_extra_dropped_after_finalized`;
  `docs/streaming.md` update.

**Semver reality (corrected)**: wiring requires a `StreamConfig::Tipc(Option<TipcStreamConfig>)`
variant + a `VeloBuilder::build` branch (`lib/velo/src/lib.rs:139-145, 252-279`).
`StreamConfig` is a **public exhaustive enum**, and cargo-semver-checks' default
heuristic enables the `tipc` feature, so the new variant fires `enum_variant_added`
and the PR **fails the semver gate unless velo bumps 0.4.x → 0.5.0** in the same PR
(pre-1.0 minor covers breaking per `scripts/check-semver.sh`). Either plan that bump
explicitly or do a one-time `#[non_exhaustive]` migration on `StreamConfig` (itself
breaking, once, then never again). velo-ext is untouched by the variant.

**Companion ecosystem PRs (independent of TIPC, shipped alongside phase 2 because it
is the right occasion):**

1. `VeloBuilder::stream_transport(Arc<dyn FrameTransport>)` escape hatch — closes
   the closed-enum gap for **all** out-of-tree FrameTransports (velo-only, additive).
2. A `streaming::sentinels` helper module in **velo-ext** exposing
   `dropped()/detached()/finalized()/is_terminal()` — today the terminal-sentinel
   encodings are `pub(crate)` in `lib/velo/src/streaming/sender.rs`, making the
   documented out-of-tree FrameTransport path unimplementable. This is the first
   velo-ext change in the whole plan: additive module, 0.2.0 → 0.2.1 (or 0.3.0 if we
   want to be conservative about the caret family external authors track), with the
   coordinated `=` re-pin and velo bump in the same PR per CLAUDE.md hard rule 3.
3. Export the 20-scenario transport conformance suite under the existing
   `test-helpers` feature — de-risks every future external transport author.

SOCK_RDM is explicitly **not** a FrameTransport candidate (receiver-overflow loss
violates the loss-free MUST). SEQPACKET loses to STREAM on the 66,000-B record cap.

---

## 7. Phase 3 (future) — topology service as PeerDiscovery/ServiceDiscovery

`lib/velo/src/discovery/tipc/` behind the same `tipc` feature. The pitch: the kernel
name table replaces etcd/NATS for intra-cluster discovery — **bind() is
registration, socket close is deregistration** (auto-withdraw [verified at 0.30 ms]),
no leases, no keepalives, no external daemon, no quorum. Registration stays
backend-concrete (`TipcPeerDiscovery::register(&PeerInfo) -> guard`), matching the
existing NATS/etcd/filesystem asymmetry — no velo-ext trait changes.

**The identity-encoding problem and chosen solution.** TIPC's per-publication
identity is `{u32 type, u32 instance}`; velo's `InstanceId` is a 128-bit UUIDv4 and
`WorkerId` a 64-bit xxh3 derived from it. Chosen design (Architect C's, adopted by
the panel verbatim):

- Publish at `{VELO_DISC_TYPE(cluster), fold32(worker_id)}` — the 32-bit instance is
  a **hint only**. The publisher is an RDM responder serving its pre-serialized
  `rmp(PeerInfo)` (NATS-responder pattern, `discovery/nats/mod.rs`; ≤ 66 KB datagram
  is ample).
- `discover_by_worker_id` = service-addressed request → fetch PeerInfo →
  **mandatory 128-bit post-fetch verification** (`discover_by_instance_id` reduces
  to worker_id + exact InstanceId compare).
- **Collision handling is load-bearing, not optional**: birthday math gives
  P(collision) ≈ 1.2e-4 at 1k instances, 1.2% at 10k, 39% at 64k — and anycast
  selection is *deterministic* ("closest-first"), so a hint collision without
  mitigation is a **persistent** miss, not a transient one. Mitigations: a
  `TIPC_SUB_PORTS` collision-walk (enumerate all publishers behind the instance,
  verify each by socket-addr-directed fetch) plus bind-time collision probe +
  instance re-roll (§5.3).
- `EHOSTUNREACH` on the bootstrap send is a microsecond "peer not found" fast-miss
  [verified] — better failure latency than NATS's 5 s timeout.

`TipcServiceDiscovery`: SEQPACKET watcher on `{1,1}` mapping
PUBLISHED/WITHDRAWN → Added/Removed. Specifics the sketch must honor:

- **Initial snapshot**: TIPC replays current state as PUBLISHED events with no
  end-marker. Synthesize `ServiceEvent::Initial` via the dual-subscription barrier —
  a parallel short-timeout subscription whose `TIPC_SUBSCR_TIMEOUT` event delimits
  the snapshot.
- **Removed correlation**: events carry `{type, instance, ref, node}`, not an
  InstanceId — maintain a `(type, inst, node, ref) → InstanceId` cache populated by
  per-PUBLISHED PeerInfo fetches; drop withdrawals never resolved; define behavior
  for peer-died-before-fetch.
- `list_services` → documented `Err(unsupported)` (hashed u32 type space is not
  enumerable; signature-legal, must be documented for generic consumers).
- Topology-server overflow behavior under slow subscribers is undocumented upstream —
  drain aggressively and treat watcher-connection loss as `Disconnected` [assumed;
  settling experiment: inspect `net/tipc/topsrv.c` / stress test].

~600–800 LOC.
Limits vs etcd/NATS: scope is the TIPC cluster domain (same L2/UDP-bearer reach,
same netid) — it cannot span WANs or arbitrary network topologies; it is the
zero-infra intra-cluster option, not a general replacement.

---

## 8. Phase 4 (future) — groups / multicast

Two layers, both sketches:

1. **`TipcQueueBackend: WorkQueueBackend`** over group anycast
   (`TIPC_GROUP_JOIN` on RDM): group anycast *is* work-queue semantics — load-aware
   member selection skipping congested members, end-to-end flow controlled, loss-free
   [assumed from kernel doc; untested locally]. `WorkQueueBackend` is a public velo
   trait consumed as `&dyn` (`lib/velo/src/queue/backend.rs`), so a new backend is
   purely additive — no API change. Positioning: non-durable, between InMemory and
   JetStream (item lost if no receiver is up).
2. **`Velo::join_group(name, cfg) -> GroupHandle`** — broadcast/anycast/multicast +
   in-band membership events as a velo-only concrete API (no velo-ext trait until an
   out-of-tree implementor exists; extracting a `GroupTransport` trait later is an
   additive velo-ext minor with the coordinated pin bump). Deliberately deferred
   until a concrete consumer exists. Do **not** attempt to back VeloEvents fan-out
   with groups — subscriber sets are per-handle dynamic and the events layer's
   fan-out point only knows unicast instances; that is a VeloEvents redesign, not a
   transport feature.

Capstone UX: a `VeloBuilder::with_tipc_cluster()` preset wiring transport +
discovery (+ queue) for the app author who wants the zero-infra cluster story in one
line. Note for the deferred RDM fast path (§11): surfacing TIPC overload rejects as
a distinct metric label would need a new `TransportRejection` variant — an exhaustive
velo-ext enum, i.e. a coordinated velo-ext bump. The "zero velo-ext changes" horizon
ends exactly there.

---

## 9. Testing & CI

**Local facts [verified]**: tests need only `modprobe tipc` — no bearer, no
identity, no external daemon (unlike NATS/etcd tests). Containers share the host
kernel; an unprivileged default-seccomp container can create/bind/use AF_TIPC
sockets once the host loads the module, and each container netns is an independent
TIPC node (so single-node tests inside CI job containers work unprivileged).

**Gating — compile always, run opt-in.**

- Kernel-dependent integration test bodies behind rustc cfg **`velo_tipc`**
  (exact in-repo precedent: `velo_endurance`, `lib/velo/Cargo.toml:105-109`, with a
  `check-cfg` lints entry). `cargo test --all-features --all-targets` (the test job,
  `ci.yml:99-129`, self-hosted `prod-velo-default-v1` runners) compiles all TIPC code
  — full clippy `-D warnings` coverage — and runs zero kernel-dependent tests:
  green on module-less machines, no skip-flakiness.
- Unit tests that run everywhere: sys layout asserts, endpoint round-trip +
  cross-version, register-gate truth table (pure function over two endpoints),
  builder validation. Guard discipline: nothing in-module may call
  `TipcTransportBuilder::build()` (binds a real socket) outside `velo_tipc` — comment
  + the `tipc_available()` probe as a skip-gate in any exception.
- Default builds (no `tipc` feature) contain zero TIPC code; macOS protected by the
  `target_os` half of every cfg, including tests/examples (§5.8.4–5).

**Dedicated `tipc-tests` CI job** on the self-hosted runners:

1. Preflight: 5-line probe (`socket(AF_TIPC, SOCK_RDM)`); on EAFNOSUPPORT fail with
   "load tipc.ko on runner hosts".
2. `RUSTFLAGS="--cfg velo_tipc ..." cargo test --features tipc --test transports_tipc
   --test transports_tipc_shutdown --test transports_tipc_node_affinity`.
3. **TIPC_CI=1 canary**: a test that hard-fails when CI claims TIPC support but the
   AF_TIPC probe fails — closes the silent-green-skip rot hole.
4. **Wire the job into the `ci-status` needs list** (`ci.yml:278` — hardcoded
   aggregation; without this the job gates nothing) and branch protection.
5. Coverage: the coverage job (`cargo llvm-cov --all-features`) never sets
   `velo_tipc`, so a ~2,800-LOC PR will blow the codecov 80% patch gate. Either run
   coverage with `velo_tipc` on TIPC-provisioned runners or add a codecov
   flag/carve-out for the rollout.
6. sccache note: a distinct RUSTFLAGS string keys a separate cache namespace (full
   dep-graph recompile for the job). Alternative worth evaluating: emit
   `cargo:rustc-cfg=velo_tipc` from build.rs off an env var with
   `rerun-if-env-changed`, confining recompiles to the velo crate.

**Step zero — verified vs assumed**: that the runners are org-controlled self-hosted
(`prod-velo-default-v1` across all CI jobs, AWS-hosted registry path) is
**[verified]** in `ci.yml`. That their *host* kernel can `modprobe tipc` is
**[assumed and possibly false]** — Amazon Linux 2023 ships no tipc.ko at all,
Bottlerocket likewise, and CIS-hardened images blocklist it. Before any other work:
a throwaway probe job (`uname -r` + `grep tipc /proc/modules` + AF_TIPC socket probe
from inside the job container). If the hosts can't load it, the fallback ladder is:
runner image change → dedicated TIPC runner pool → (last resort) GitHub-hosted
`ubuntu-*`, where `sudo modprobe tipc` is *plausible but flaky* (modules-extra not
preinstalled, version-skew failures documented in actions/runner-images #7587/#8080).

**Two-node coverage**: root-gated, `#[ignore]`-by-default netns-pair veth-bearer
tests (the §3 cluster recipe) exercising link-tolerance abort → ECONNRESET → writer
drain, and the register-gate TCP fallback. Real multi-host validation is a phase-1
exit criterion, not a CI gate.

**Developer runbook** (goes in `docs/transports.md`): on a Linux box,
`sudo modprobe tipc && RUSTFLAGS="--cfg velo_tipc" cargo test --features tipc --test
transports_tipc`. Without it, `cargo test --all-features` runs zero TIPC integration
tests with no indication — the doc and the canary are the discoverability story.

---

## 10. Risks & open questions

Honest list; everything here survived adversarial review (refuted items were fixed
above and are not repeated).

1. **CI runner-host kernel** — the single load-bearing CI assumption is unverified
   (§9 step zero). If hosts are AL2023/Bottlerocket, "one-time ops task" becomes a
   runner-image project.
2. **Security posture** (absent from the original design; owed to operators):
   (a) TIPC cluster membership is **unauthenticated** — any node on the bearer
   domain with netid 4711 auto-joins; two unrelated deployments on one L2 segment
   silently merge; a rogue host can bind types ≥ 64 and receive anycast traffic.
   Ops docs must mandate per-cluster netid + identities and frame AEAD keys
   (`tipc node set key`) as **admission control**, not just confidentiality.
   (b) Kernel attack surface: CVE-2021-43267 (remote heap overflow → RCE,
   5.10–5.15), CVE-2022-0435 (remote stack overflow, bearer required),
   CVE-2024-36886 (remote UAF in fragment reassembly, ≤ 6.8, fixed 2024-05) —
   recommend a kernel floor (≥ 6.9 for the last one) in the docs.
   (c) CIS L2 baselines explicitly require tipc to be unavailable — "enable
   fleet-wide" collides with compliance scanning in hardened fleets; loading the
   module is a security decision, not just connectivity.
3. **Cloud/UDP-bearer reality**: the UDP bearer advertises a 14,000-byte virtual
   MTU (IPv4) relying on IP fragmentation, which cloud fabrics/SGs routinely drop —
   silent blackholing for frames over ~1.4 KB unless the operator runs
   `tipc bearer set mtu` (supported by the installed CLI) down to path MTU; plus
   UDP/6118 SG rules and replicast (no VPC multicast). Settling experiment: two-VM
   VPC cluster, > 2 KB frames, default vs clamped MTU. Until then TIPC is positioned
   as a same-L2/VPC-domain transport with TCP fallback, never WAN.
4. **Kernel version floor** — not yet formally stated. Practical: ≥ 4.17 (auto
   identity), `TIPC_NODELAY` ≥ 5.5 (handled best-effort §5.4), groups ≥ 4.14,
   crypto ≥ 5.5 + `CONFIG_TIPC_CRYPTO=y` (distro-dependent), CVE floor per item 2.
   Decide and document a supported floor (proposal: 5.10+ tested, 6.x recommended).
5. **Backpressure window asymmetry** — ~128 KiB initial per-connection in-flight cap
   vs TCP's multi-MiB buffering: SendBackpressure engages much earlier for bulk
   traffic. Whether bumping receiver `SO_RCVBUF` (the only knob) to 16 MiB
   (sysctl max) is sufficient for velo's large-frame workloads needs a benchmark;
   the `2 MiB SO_SNDBUF` line from the original design was a no-op and is gone.
6. **Cross-peer congestion coupling** — link congestion is per node-pair; one slow
   node stalls writers for all its peers simultaneously. Contract-compliant
   (per-peer SendBackpressure still reported per channel) but operationally
   different from TCP; documented, not solved.
7. **Dynamic identity transition** — enabling the first bearer on a running system
   flips own node from 0 to a nonzero hash. Same-stack peers are unaffected (the
   netns_nonce match holds regardless of node value), but a remote peer that
   advertised `node=0` before the transition stays gated to TCP until it re-registers
   with a fresh endpoint — conservative, never wrong, but not analyzed end-to-end.
   Documented rule: enable bearers before starting velo; testing the transition is open.
8. **Multi-host validation gap** — partition behavior verified only on the
   netns-pair cluster; bearer failover, dual-link load sharing, and cross-node bulk
   throughput were not tested on real hardware. Phase-1 exit criterion.
9. **Topology-server overflow** (phase 3) — event-loss behavior under slow
   subscribers undocumented; needs `topsrv.c` inspection/stress before the watcher
   is treated as lossless. Same for the unbind-without-close ABI used by any future
   shared-socket multi-binding design.
10. **Phase-3 collision storms** — fold32 hint collisions are availability (not
    correctness) hazards thanks to verification, but the collision-walk path is
    hard to exercise (needs forced same-instance binds) and adds probe latency at
    scale.
11. **Cold-start ordering & re-register plumbing** — `start()` awaits watch replay
    and transient gate rejections are parked and re-driven by `TIPC_PUBLISHED` /
    node-up events through the in-tree re-register hook (§5.3), so a TCP demotion
    is never permanent. Residuals: cold-start pairs run TCP-first for the
    name-table convergence window (ms–s); the hook is extra plumbing with its own
    failure modes (unit-tested with injected events, integration-tested in the
    netns-pair cold-start scenario); a mass node-up event re-drives at most
    |pending| registrations — bounded, but worth a metric
    (`velo_transport_tipc_reregistrations_total`).
12. **hostNetwork co-tenancy** — all hostNetwork pods on a node are one TIPC node
    sharing one name table; a co-tenant binding overlapping service types is
    cross-visible. Docs note.
13. **Ref recycling** — stale `{ref, node}` can connect to an unrelated socket and
    fail only at frame decode (no connect-time identity handshake in the velo wire
    protocol). Same class as TCP port reuse; doc note, plus the option of a future
    hello frame if it ever bites.
14. **AEAD performance** — unmeasured; needs a keyed benchmark run before
    recommending encryption on hot paths.
15. **Housekeeping (resolved 2026-06-10, alongside this proposal)**: CLAUDE.md's
    hard-rule-2 snippet and external-author example now match the workspace's
    `=0.2.0` pin (`Cargo.toml:27`), and `docs/discovery.md`'s backend table no
    longer lists the nonexistent `EtcdPeerDiscovery` / `NatsServiceDiscovery`.

---

## 11. Alternatives considered

**B — out-of-tree `velo-transport-tipc` crate** (depends only on published
`velo-ext = "0.2"`). Genuinely valuable analysis: it proved phase 1 needs zero
velo/velo-ext changes even out-of-tree, correctly disqualified the
publish=false-member variant (cargo cannot publish `velo` with an unpublished
optional dep — reopening hard-rule-1's bug class), and identified the two real
out-of-tree streaming blockers (closed `StreamConfig`, `pub(crate)` sentinels) that
§6 adopts as ecosystem PRs. Rejected as the operating model because its central
motivation — keeping kernel-module requirements out of velo's CI — solves a problem
velo does not have (CI runs on org-controlled self-hosted runners), while paying a
permanent two-repo tax: release-cadence coupling on every velo-ext family bump
(lagging apps get two velo-ext copies and E0277 storms), a hand-ported conformance
suite policed by a cron job, a duplicated codec, an unfixable phase-4 dead end
(`WorkQueueBackend` lives in velo), and a register-gate hole (no node==0
disambiguation) in exactly the fleet-wide-enable scenario it advertised. If an
external-author showcase is ever wanted, extract this module after the conformance
suite is exportable — strictly cheaper in that order.

**C — TIPC-native: RDM messenger, SEQPACKET streaming, topology discovery, groups.**
The intellectually strongest use of TIPC, and its discovery/group phases are adopted
nearly verbatim here (§7, §8) — they are substrate-independent and graft cleanly
onto the STREAM chassis. Its phase-1 core was rejected on contract grounds: the RDM
reject path hands `on_error` a fabricated empty payload (the documented contract
carries the original header+payload; `TIPC_RETDATA` truncates at 1,024 bytes —
empirically pinned), reject-retention TTL expiry degrades to silent loss, the shared
TX socket couples backpressure across peers in violation of the per-peer semantics,
and its load-bearing behaviors (retention sizing, SEQPACKET cap, topsrv overflow,
unbind ABI) sat on its own speculative ledger. None of that is portable onto a fix;
the reverse grafts all are. The RDM design stays on file as a documented sub-66 KB
low-latency fast path — viable only below the messenger layer or with full-payload
retention, and gated on the `TransportRejection` velo-ext bump noted in §8.

---

## Appendix: decision table

| Decision | Choice | One-line why |
|---|---|---|
| Socket type (messenger + streaming) | SOCK_STREAM | codec reuse, no 66 KB cap, kernel e2e flow control, connection-oriented failure signals |
| Binding | hand-written sys.rs + socket2 + AsyncFd | both existing crates dead & verified broken; zero new deps |
| Endpoint | `to_vec_named` TipcEndpoint under key `"tipc"`, socket-addr primary | anycast-immune exact connects; bidirectional version tolerance |
| Reachability gate | netid == + netns_nonce (same-stack) + same-node-unequal-nonce forced closed + (node up AND live name-table publication match); transient rejections parked + event-driven re-register | exact O(1) zero-RTT; NoEndpoint → TCP promotion (UDS precedent); netns-scoped nonce because TIPC is per-netns; own-node claims never trusted; stale/recycled refs fail closed at register; cold-start demotions self-heal |
| Shutdown semantics | `Shutdown::Both` everywhere + ConnectionReset-as-graceful in listener | TIPC has no half-close; plain close is ECONNRESET |
| Test gating | `velo_tipc` rustc cfg + dedicated CI job + TIPC_CI canary | --all-features stays green module-less; no silent skip-rot |
| velo-ext | untouched until phase-2 sentinels (0.2.x bump, coordinated pin) | hard rules 2–4 |
| Phase order | messenger → streaming (+ecosystem PRs) → discovery → groups | risk-ascending; each phase independently shippable |
