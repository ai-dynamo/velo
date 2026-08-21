# RDMA Messaging Transport for Velo — Research Report & Proposal

Status: research complete (two rounds + P0 hardware probes), **decision made:
the vendored-UCX path (§11), selected after the P0 probes passed (§13.1)**;
the pure-ibverbs design (§5–§6) is retained as the documented fallback. Round 1: 18-agent program (7 research tracks, 7 adversarial
verifiers, completeness critique, 3 empirical gap-fills) scoping a pure-ibverbs
messaging transport. Round 2 (same day, 7 agents): the UCX path scoped in depth
after the requirements grew to include **RDMA GET/PUT for `velo::rendezvous`**
— build.rs vendoring (working prototype crate, empirically verified), cdylib
symbol isolation and two-UCX coexistence (measured), the safe-wrapper design,
UCP RMA semantics, and the like-for-like ibverbs RMA counterfactual. §1–§10 are
round 1 (with round-2 corrections marked); §11–§13 are round 2. Every
load-bearing claim was independently re-verified against primary sources; the
verdict is noted where a claim was refuted or corrected. Evidence tags:

- **[measured-spark]** — measured on the local DGX-Spark-class box (aarch64
  Cortex-X925, 4× ConnectX-7 RoCE ports all link-down, rdma-core 50, Linux 6.14).
  Verbs *object* behavior (QP/MR/SRQ creation, registration cost, CQE delivery
  via a self-addressed UD QP) is real hardware; no wire traffic was possible.
- **[measured-b200]** — measured on a computelab umbriel DGX-B200 node
  (x86_64, CX-7 MT4129 IB, MLNX OFED 25.07), real traffic over a two-HCA
  in-node path (`mlx5_0:1 → mlx5_2:1`, direct wire, MTU 512). Latency rows
  ≥ 8 KiB and all bandwidth rows hit an unexplained ~11.6 Gb/s path ceiling —
  treat those rows as lower bounds, not fabric constants.
- **[source]** — read directly from the project's source on GitHub (UCX master
  f1b7b01 2026-08-19 / 1.22.0 tarball, NIXL main @ v1.4.0, NCCL master,
  rdma-core master, eRPC, crate repos), not from docs or memory.
- **[verified]** — additionally confirmed by an independent adversarial
  fact-check against different primary sources.
- **[lit]** — from the research literature (eRPC NSDI'19, FaSST OSDI'16, HERD
  SIGCOMM'14, LITE SOSP'17, IRN SIGCOMM'18, Flock SOSP'21, ATC'16, FastWake
  APNet'23), with the specific numbers cited.
- **[refuted]/[corrected]** — a claim we went in with that the evidence killed
  or reshaped.

---

## 1. Executive summary

**Decision: the vendored-UCX path** — made on the "probe first, lean UCX"
protocol after the P0 hardware probes passed (§13.1; most importantly, PEER
error mode keeps zero-copy `rma_bw` lanes on RC regardless of DEVX/KSM).
How the call got here: round 1 recommended pure ibverbs; two things then
changed. First, the requirement grew:
`velo::rendezvous` must gain an RDMA GET/PUT data path (its protocol already
carries the placeholders — `AcquireResponse::Rdma` and `StageMode::Pinned` —
and the transparent `LargePayloadStager` routes every messenger payload over
256 KiB through rendezvous, so the RMA substrate serves *both* subsystems).
Second, round 2 resolved the UCX path's three biggest unknowns in its favor:
the vendored static build is a working, verified prototype crate (§11.1); the
two-UCX-in-one-process hazard is measured-safe with an understood mechanism
(§11.2); and the wrapper velo would own prices out ~25–40% *cheaper* than the
like-for-like ibverbs scope once RMA is included (~4.9k vs ~6–8.5k LOC,
§11.3/§12), because the expensive half of the work is common to both paths and
what UCX deletes is exactly the wire-protocol half ibverbs keeps. What UCX
*adds* is a different risk class: FFI-callback soundness, vendored-C build
ownership, protocol-selection opacity, a third flow-control layer, and one
open hardware question (PEER error mode × DEVX/KSM, §11.4) that P0 must
answer.

Round 1's findings stand on their own merits, and several reshaped both paths:

1. **The UCX dependency objection is actually solved** — and that is worth
   knowing even though it doesn't change the answer. Static-linking UCX is a
   first-class, CI-tested configuration: the dlopen module loader is compiled
   out of static objects by construction, and we *built and ran* a
   statically-linked UCX 1.22.0 on this workstation (13.8 s configure + 84
   CPU-s compile, 2.2 MB stripped, `ucp_init` green, four RoCE MDs enumerated,
   `ldd` shows zero UCX `.so`s) **[measured-spark]**. A vendored `ucx-src`
   crate in the zeromq-src mold is straightforwardly feasible (3.69 MB release
   tarball with pre-generated `configure`; the `lamellar-ucx-sys` crate,
   2026-07, already does a worse version of it) **[verified]**. BSD-3 license,
   no obstacle. *(Round 2 turned this into a working prototype crate and found
   four load-bearing corrections to the recipe recorded in §4.2 — most
   critically `--with-pic`, without which no cdylib can link. See §11.1.)*
2. **What actually kills the UCX path is the Rust wrapper, not the C build.**
   Your claim about the Rust UCX crates is confirmed and understated:
   async-ucx's published crate is 0.1.1 from Sep 2022; all 2025 modernization
   sits in unmerged PRs — including your own #11, open since Oct 2025 — the
   2021 cancellation-safety soundness hole (issue #1) was never fixed and the
   current `Drop` impl calls `ucp_request_free` without ever calling
   `ucp_request_cancel`; and Worker/Endpoint are `Rc`-based `!Send`
   **[verified]**. Meanwhile stock UCX is built with `--enable-mt` **off**, and
   even with it on, `UCS_THREAD_MODE_MULTI` degrades to one recursive lock
   around the whole worker **[verified]** — so any UCX integration must be
   architected as thread-pinned workers regardless. Owning a sound async
   wrapper over `ucp_am_send_nbx`'s callback lifetimes and tri-state returns is
   comparable engineering to owning the verbs data path directly, with less
   control and one extra hazard: **two UCX copies in one process** alongside
   NIXL's (NIXL ships `--with-soname-suffix` + `RTLD_DEEPBIND` machinery
   precisely because this bites; UCM installs process-global malloc/mmap hooks)
   **[verified]**. *(Round 2 revised both halves of this conclusion: the
   wrapper prices out ~25–40% cheaper than the like-for-like verbs path, not
   comparable — §11.3 — and the two-UCX hazard is measured-safe across an
   8-cell coexistence matrix with the mechanism understood — §11.2. The
   soundness hole is closed structurally by completion-owned op state.)*
3. **NIXL is disqualified for messaging on correctness, not taste.** NVIDIA's
   own backend contract doc: *"genNotif … does not provide any ordering
   guarantees"* **[verified]** — that alone violates Velo's per-target ordered
   admission contract. Beyond that: the AM header is stripped (`hdr = nullptr`
   with a receive-side assert `header_length == 0`), sends are pinned
   `UCP_AM_SEND_FLAG_EAGER` (no rendezvous ever), a notification payload is
   copied ~5× end-to-end, delivery is poll-only under the agent's *exclusive*
   lock with no fd/callback surfaced, and the default Rust `Agent::new()`
   spins a progress thread at `poll(fds, n, 0)` — one core at 100% **[source]**.
   Your specific claims, adjudicated in §4.3: bindings "not first-class" —
   half right (packaging is fine, concurrency design is worse than you
   thought); "C↔C++ vectors/strings" — right conclusion, wrong layer (the C
   shim is clean spans; the *C++ API signatures* force the copies, so it is
   unfixable without upstream C++ changes); "must validate all addresses" —
   **[refuted]**: `populate()` is rkey resolution you need anyway and is
   already amortized once-per-buffer-set via `prepXferDlist()`; there is no
   validation to bypass and an unsafe fast path would buy nothing.
4. **The honest performance picture** (this reshapes *why* we build it): we
   measured Velo's incumbent per-message budget with a 6-rung ablation
   (`examples/examples/tx_budget.rs`, now in-tree). TCP-loopback one-way at
   64 B + 1 KB is 11.4 µs, of which kernel/socket is 6.0 µs and **two
   cross-thread tokio task wakes are 4.8 µs**; codec and admission gate are
   ~16 ns each; there is no serialization **[measured-spark]**. So: intra-node,
   an ibverbs transport buys ~2.2–2.7× over TCP-loopback, roughly nothing over
   the UDS transport we already ship, and is *worse* than UDS if it parks on a
   completion channel. **Inter-node is the case**: ~2 µs wire + ~2.9 µs
   plumbing vs 15–50 µs TCP wire + 5.7 µs plumbing ≈ 4–11×. NVIDIA's own
   Dynamo guidance additionally argues RDMA is the *uniform* choice in K8s
   because pod isolation forecloses NVLink even same-node. Two incumbent bugs
   fell out of the baseline work (§10) — one of them (~100–350× TCP throughput
   collapse at ≥16 KB × 64 in-flight) matters today, RDMA or not.

Cost estimate for v1 (§9): **~4–6k LOC, 3–5 weeks** to a merged, feature-gated,
rxe-tested transport, on `jonhoo/rust-ibverbs` (active, MIT/Apache-2.0,
extended-verbs `ibv_wr_*` batching, rdmacm feature, comp-channel fd + AsyncFd
examples) **[verified]**. No `velo-ext` changes required for P1 — the existing
trait surface (including `max_message_size`) covers everything.

---

## 2. What the transport must beat — the measured incumbent budget

From the `tx_budget` ablation (each rung adds one layer; the delta *is* the
attribution). TCP loopback, 64 B header + 1 KB payload, depth 1, 4-worker
tokio, perf cores pinned **[measured-spark]**:

| rung | layers | one-way p50 |
|---|---|---|
| L0 | blocking socket syscalls, OS threads | 5,996 ns |
| L1 | + tokio | 5,652 ns (reactor ≈ free) |
| L2 | + `TcpFrameCodec` encode/decode | 5,668 ns (**codec +16 ns**) |
| L3 | full `Transport`/`DataStreams` | 11,368–11,599 ns (**plumbing +5,700 ns**) |

The +5,700 ns is 84% two cross-thread flume-send-wakes-parked-task hops
(2,393 ns each; 216 ns on a pinned `current_thread` runtime — an 11×
difference), ~900 ns residual (DashMap, `Bytes` clone, metrics).
`AdmissionGate::send` costs +16 ns over raw `flume::try_send`. UDS L3 one-way:
5,235 ns (2,289 ns pinned). Traffic reality check: Dynamo's request plane
carries control messages + streamed tokens — the 1 KB / 16 KB cells are what
matter; MB-scale KV transfer rides NIXL, not this plane **[source]**.

Consequences:

- An ibverbs transport removes the socket syscall + kernel stack and the
  *sender-side* hop (`ibv_post_send` is a userspace doorbell; with
  `IBV_SEND_INLINE` the buffer is reusable at return, so posting can happen
  inline in `send_message`). It cannot remove the receive-side hop into
  `DataStreams` or the gate. Floor ≈ 2.9 µs software + wire.
- ~3.7 µs of the incumbent's intra-node cost is removable **today with no
  RDMA** by eliminating cross-core wakes (the TCP module docs already suggest
  `LocalSet`). Worth doing independently; it also lowers the floor for the
  ibverbs transport itself.
- Wire references **[lit]**: IB RDMA ~1 µs one-way; RoCE ~2 µs; eRPC's 32 B
  RPC median 2.3 µs end-to-end proves what a userspace AM layer can reach.

---

## 3. UCX's active-message architecture — the reference design

UCX is the design reference: `ucp_am_send_nbx(ep, id, header, header_len,
buffer, count, param)` is almost exactly
`send_message(instance_id, header, payload, message_type, on_error)`. UCX
solves the same four problems Velo will hit; per-mechanism verdicts on what to
copy:

### 3.1 Wire format — copy this

8-byte fixed header **first**, user header as a **footer** so the payload
lands at a fixed aligned offset **[source: src/ucp/core/ucp_am.h]**:

```
single:      [ucp_am_hdr{am_id:u16, flags:u16, header_length:u32}] [payload] [user hdr]
first frag:  [ucp_am_hdr] [payload] [user hdr] [ftr{msg_id:u64, ep_id:u64, total:usize}]
mid/last:    [mid_hdr{offset:usize}] [payload] [ftr{msg_id:u64, ep_id:u64}]
```

Velo adaptation (§6.2) keeps the fixed-prefix/footer trick but does not need
the fragmentation layouts at all (§3.4).

### 3.2 Receive ownership — copy this

Every pre-posted RX buffer reserves headroom (`sizeof(desc) + 32`). The UCT AM
callback returns `UCS_OK` → transport reposts the buffer immediately, or
`UCS_INPROGRESS` → ownership transfers up; the transport won't repost until
`uct_iface_release_desc()` **[verified]**. mlx5 inline-scatter (≤32/64 B
payloads arrive *inside the CQE*) takes a copy path instead — the RX buffer was
never touched. This maps exactly onto a Rust handoff: either a `Bytes` aliasing
a registered RX slab with a drop-guard that reposts, or a copy for tiny
messages. (Whether Velo's downstream ever converts to `BytesMut` — which
deep-copies owner-backed `Bytes` — is an unresolved gate on the zero-copy
variant; §8.6.)

### 3.3 Flow control — copy this

RC uses one SRQ shared by all peers (defaults 4095 WQEs × 8256 B ≈ 32 MiB)
plus a **per-endpoint credit window** (default 512) whose soft-request /
hard-request / grant bits ride the top 3 bits of the 8-bit AM-id byte (UCT AM
ids are 5 bits); soft threshold 0.5, hard 0.25; stated purpose: *"avoiding RC
RnR backoff timer"* **[verified]**. Without credits, a shared SRQ + RNR is a
latency cliff (§6.4).

### 3.4 What Velo does NOT need from UCX

- **Wireup / UD auxiliary transport.** UCX bootstraps RC over a private UD
  channel only because it may lack an OOB channel; DC avoids the handshake by
  being connect-to-iface **[verified]**. Velo *has* a real OOB channel
  (discovery + `register()`), so the entire wireup subsystem — probably 30–40%
  of a UCX-shaped design — disappears (§6.1).
- **Multi-fragment eager + interval-tree reassembly.** A UCX artifact of its
  8 KiB segment size. RC's per-message ceiling is `port_attr.max_msg_sz` =
  **1 GiB** (measured `0x40000000` on CX-7) **[measured-spark, verified]** —
  one `ibv_post_send` with a 2-entry SGE (header + payload) carries any Velo
  frame with zero segmentation.
- **Rendezvous, in v1.** UCX's own cost model prices registration at
  16 µs + 0.06 ns/B; measured reality is ~2× cheaper but the conclusion holds:
  for host-memory KB-to-low-MB payloads, copy-into-preregistered-pool beats
  register-then-rendezvous below a crossover of **~0.5 MiB (cold source) to
  ~2 MiB (cache-resident source)** — governed by memcpy's LLC cliff
  (76 → 17 GB/s), not by registration cost **[measured-b200, corrected]**. For
  contrast, UCX proto-v2's own runtime choices on CX-7 (`UCX_PROTO_INFO=y`):
  AM short ≤ 2038 B, bcopy 2039–8118, zcopy from 8119, rendezvous only from
  **311,294 B** **[measured-b200]**.
- **The registration cache + UCM.** UCX intercepts malloc/mmap/munmap/madvise
  process-wide to invalidate its rcache — a bug class Velo dodges entirely by
  owning a static pre-registered pool and (optionally) registering caller
  `Bytes` only above the crossover, with explicit dereg tied to completion.
- **The proto-v2 cost model**, but steal its constants: IB send overhead
  `bcopy:5ns, cqe:20ns, db:40ns, wqe_fetch:350ns`; the 350 ns WQE-fetch
  latency is charged to bcopy/zcopy but **not** to inline sends — the
  quantified case for an inline fast path — and `rc_mlx5` (BlueFlame
  whole-WQE-into-UAR) vs `rc_verbs` is 40 vs 75 ns CPU/op — the quantified
  bound on what a future mlx5dv path buys (~35 ns; not worth vendor lock-in
  in v1) **[verified]**.
- **Thread modes.** `--enable-mt` defaults off; MULTI = one recursive lock per
  worker **[verified]**. The lesson transfers: one CQ/QP-set owner per thread,
  serialized posting via the existing per-target gate, `ibv_alloc_td` to drop
  the provider's internal locks (§6.6).

---

## 4. Options analysis

### 4.1 Option A — pure ibverbs (recommended)

The literature converges on the design (§5–6): every production RDMA messaging
system is either UD + software reliability (eRPC, FaSST — forced by their
one-QP-per-thread scale goals, pays MTU-sized packets and 6.2k SLOC of
protocol) or RC + mailboxes (HERD, FaRM, NCCL, libfabric-rxm). Velo's shape —
tens to low-thousands of peers, KB–MB payloads, an existing OOB discovery
channel, an existing per-target ordered gate — lands squarely on **RC + SRQ +
credits**:

- RC gives hardware reliability, ordering, and 1 GiB messages; UD would force
  software segmentation/reassembly/retransmission of everything above ~4 KB
  (MTU) **[verified]**.
- The RC scaling wall is real but far enough: throughput knee at 176–704
  active QPs on CX-5 (Flock), ~50% RDMA-throughput loss at 5,000 connections
  (eRPC; the specific figure could not be independently re-located and is
  flagged) **[lit, partially verified]**; responder-side state scales much
  better than requester-side (HERD). Measured host memory: 8.2 KB/QP at
  depth ≤4, 16.2 KB at depth 16 (12.2 with SRQ) **[measured-spark]**. At
  Velo's scale with small QP depths, fine; XRC (standard verbs, capability
  present on CX-7) and DC (mlx5dv-only, second send path — not v1) are the
  documented escape hatches **[verified]**.
- Risk transfer: we own credits, pools, error paths. Mitigated by the fact
  that the failure modes are now mapped (§6.4–6.8) and the two live Rust
  bindings cover the full verbs surface needed.

### 4.2 Option B — UCX via vendored static build + own wrapper

Viable, and the build story is genuinely *better* than our vendored zmq
(faster build, no cmake, BSD-3 vs MPL-2.0) **[measured-spark]**. Adds zero net
system dependencies — rdma-core is required either way **[verified]**. What it
buys over Option A: proto selection, rendezvous, multi-rail, a decade of NIC
quirk handling. What it costs: (i) the entire safe async wrapper, from scratch
(no maintained binding exists **[verified]**); (ii) thread-pinned worker
architecture forced by UCX's locking model; (iii) the arm/progress race
protocol on the event path (documented loop: progress-until-0 → arm →
handle-BUSY-by-progressing) with the same measured ~6 µs event-mode penalty as
raw verbs (UCX's RC CQ *is* an ibv comp channel; no async thread in the RC
completion path) **[measured-spark, source]**; (iv) the two-UCX-in-one-process
hazard in Dynamo (§1.2). **[superseded by §11]** Round 1's verdict ("the right fallback, not the
starting point") predates the rendezvous-RMA requirement and the round-2
findings; see §13 for the current framing. The `ucx-src` recipe sketched here
in round 1 (`--enable-static --disable-shared --with-verbs --with-rdmacm`,
per-subdir make, `-Wl,--undefined=` per the `.pc` files) was **incomplete in
four load-bearing ways** found and fixed in round 2 — missing `--with-pic`
(breaks every cdylib), an incomplete constructor list (omitting `ucs_init`
yields a silently half-dead UCX), `cargo:rustc-link-arg` not propagating across
crate boundaries (the `--undefined=` approach cannot live in a build script at
all), and two libc/libgcc link-ordering failures. The corrected, verified
recipe and working prototype crate are in §11.1.

### 4.3 Option C — NIXL (rejected for messaging)

Adjudicating the three claims from the prompt against source **[verified]**:

| claim | verdict | the actual finding |
|---|---|---|
| "Rust bindings not really first-class" | **split** | Packaging refutes it: `nixl-sys` on crates.io, monthly lockstep releases, docs.rs green. Design confirms it, worse: `Arc<std::sync::RwLock<AgentInner>>` with exclusive `write()` on the send/status/notify hot paths where the C++ takes shared locks (4 of 6 named methods confirmed; 2 are exclusive on the C++ side too **[corrected]**); README disables multithreaded tests "because NIXL might deadlock"; build.rs hardcodes `g++` and silently falls back to dlopen stubs that abort at first use. |
| "FFI is C↔C++ with vectors/strings" | **right conclusion, wrong layer** | The C shim itself is clean `(const void*, size_t)` spans. The copies are forced one level down by the **C++ API signatures** (`const std::string&`, `const std::vector<int>&`): `gen_notif` does `msg.assign(data, len)` — a full payload heap-copy per send; `make_xfer_req` materializes two `std::vector<int>`. Unfixable in the shim or in Rust; only upstream C++ changes help. |
| "NIXL must validate all addresses; unsafe bypass wanted" | **[refuted]** | What looks like validation is `nixlMemSection::populate()` — address→rkey *resolution* whose output you need. It is amortized once-per-buffer-set via `prepXferDlist()`; `makeXferReq`/`postXferReq` do index checks + a string-hash lookup + a generation check. There is no per-op validation to bypass, and a client-side-guaranteed-addresses unsafe API would save nothing meaningful. |

The disqualifiers are elsewhere: no ordering guarantee (contract violation),
no AM header, forced eager, ~5 copies, poll-only exclusive-locked delivery, no
fd. Notifications were measured byte-exact and unfragmented up to 8 MiB
**[measured-b200]** — so it *works*, it is just the wrong shape, and a
write-plus-doorbell design on NIXL ends with Velo writing the whole messaging
protocol anyway while NIXL contributes only rkey bookkeeping. **One idea is
worth stealing regardless**: `getLocalMD()`/`loadRemoteMD(blob)` where loading
the peer's opaque blob eagerly creates endpoints — a 1:1 match for
`address()`/`register(PeerInfo)`.

### 4.4 Unevaluated alternatives (flagged by the completeness critic)

libfabric (`verbs;ofi_rxm` — in every distro, already a NIXL backend), UCCL
(2026 software transport, 256 QPs/connection multipath), and Mooncake Transfer
Engine (raw ibverbs, PCIe-affinity topology) were **not** scored. rxm's own
man page documents the per-peer-RC failure mode and its SRQ/XRC remedies,
which independently corroborates the §4.1 design, but a fair scoring of
libfabric as "Option D" was not done. Noted as an open item (§8.9); it does
not obviously beat Option A for the same reason UCX doesn't — no maintained
Rust binding, and Velo needs a narrow subset.

---

## 5. Design constraints established by the research

The non-negotiables the mailbox/memory design must respect:

1. **Never poll payload memory for arrival.** Last-byte polling (HERD/FaRM
   style) violates IBTA o9-20, and is actively broken by
   `IBV_ACCESS_RELAXED_ORDERING` (man page: back-to-back writes "leave the
   region in an unknown state"), adaptive routing, and OOO placement
   **[verified]**. Supported arrival signals: SEND/RECV completions or
   `RDMA_WRITE_WITH_IMM` consuming a (zero-length) recv WQE.
2. **RNR is not backpressure.** Defaults are hostile: `min_rnr_timer = 0`
   encodes **655.36 ms**; `rnr_retry = 7` = infinite retry — a stalled
   receiver wedges the sender forever with no error **[verified]**. Design:
   credits make RNR unreachable in the common case; keep `rnr_retry=7` so
   refill races self-heal; set `min_rnr_timer` 1–6 (10–80 µs) so the rare race
   costs microseconds (production stacks use 12 = 0.64 ms).
3. **Verbs completions are not a liveness signal.** CX-5+ firmware enforces a
   minimum ack timeout of 16 (≈268 ms/try; independently confirmed by a DPDK
   mlx5 patch bumping 14→16); with `retry_cnt=7` a dead peer surfaces in
   ~3–4 s **[verified]**. `check_health` must be application-level (§6.7).
   APM is present-but-fragile; no traced production stack arms it — reconnect
   is the failover mechanism **[source]**.
4. **A static WorkerAddress blob cannot carry RC connection state for an open
   peer set.** QPN is minted at `ibv_create_qp()` per connection; NCCL and UCX
   both publish a static *listener* and exchange `{qpn, psn, gid/lid, mtu}`
   per connection (NCCL: over a plain TCP socket; minimal precedent:
   perftest's `pingpong_dest{lid,qpn,psn,gid}`) **[verified]**. A pre-created
   QP pool variant exists but requires bounding fan-out at startup and
   symmetric eager registration — wrong fit for dynamically growing worker
   sets (and the citations offered for it did not survive verification
   **[corrected]**).
5. **SRQ sizing is a one-shot decision on CX-7** — `SRQ_RESIZE` is absent from
   its device caps (max_srq_wr 32,767; limit-watermark event
   `IBV_EVENT_SRQ_LIMIT_REACHED` works: armed via `ibv_modify_srq`, verified
   accepted) **[measured-spark]**.
6. **Inline law on CX-7 (`ibv_post_send` path):** granted inline =
   64·k − 4 bytes; **RC max 828 B** (k=13), UD 956 B; requests ≥912 B fail
   `EINVAL`; requesting large inline shrinks how many send WRs fit
   **[measured-b200, measured-spark]**. (UCX's `am_short ≤ 2046` is its
   DevX-WQE + NIC device-memory path, not available through plain verbs
   **[corrected]**.) Measured benefit: −35% latency at 64 B (2.95→1.92 µs);
   zero at 512 B; crossover somewhere in 65–511 B — unsampled, and that is
   exactly Velo's header range (§8.8). Inline semantics: buffer reusable at
   post return — fire-and-forget with no completion tracking.
7. **Platform tuning is part of the deliverable, not an afterthought.** With
   default ARM cpuidle states, event-mode p50 at low rates degrades from
   ~10 µs to 88–97 µs (LPI exit latencies 42/231/433 µs on GB10-class — the
   Dynamo target silicon); leaving the mlx5 comp-vector IRQ on a busy core
   put p99.9 at 726–1082 µs vs 11–15 µs isolated **[measured-spark]**. The
   transport must document/set PM-QoS and IRQ-affinity guidance.

---

## 6. Proposed design: `velo::transports::ibverbs`

In-tree, feature `ibverbs`, `#[cfg(all(target_os = "linux", feature = "ibverbs"))]`,
module layout per CLAUDE.md (`transports/ibverbs/{mod,transport,listener,...}.rs`).
No `velo-ext` changes in P1: the existing trait surface covers everything
(`max_message_size(target)` returns the negotiated eager ceiling).

### 6.1 Endpoint model & bootstrap

- `address()`: msgpack blob under `TransportKey("ibverbs")` carrying
  `{bootstrap: ip:port, device hints: [(gid, gid_index, roce_version, mtu,
  numa)], incarnation: u64}`. The bootstrap endpoint is a tiny TCP listener
  owned by the transport (NCCL's pattern; reuses
  `transports::utils::interfaces` for multi-NIC endpoint selection like TCP
  does today).
- `register(PeerInfo)`: parse blob, select device/GID by NUMA + subnet
  affinity, store peer. Cheap and synchronous — **no connection yet** (mirrors
  `TcpTransport::register`).
- First send (or `ensure_connected`): dial the peer's bootstrap listener,
  exchange `{qpn, initial psn, gid+gid_index, mtu, credit window, eager
  ceiling, incarnation}` (one small struct each way, msgpack), drive
  INIT→RTR→RTS locally on both sides, done. Wire-critical fields per
  connection are exactly QPN/PSN/address-vector; MTU = min of both; timeouts
  and rd_atomic are local policy **[verified]**. The incarnation number
  rejects a restarted peer's stale state (§8.4).
- Why not rdma_cm: it needs IPoIB (or ibacm) for address resolution on native
  IB, caps private_data at 56 B, and brings its own event-channel state
  machine **[verified]**. A 50-line TCP exchange over infrastructure we
  already have is strictly simpler. RoCE-only deployments lose nothing.

### 6.2 Wire format (eager, v1)

One RC SEND per message, SGE list `[fixed_hdr | user header | payload]`:

```
fixed_hdr (8 B): magic/ver: u8, msg_type: u8 (velo MessageType), flags: u8,
                 credits_granted: u8, header_len: u32
```

- Credit grant piggybacks on every message (`credits_granted`), plus a
  dedicated `PURE_GRANT` control type for the idle-receiver case — UCX's FC
  design with a byte instead of stolen id bits (we have no 5-bit constraint).
- Small message (fixed+header+payload ≤ inline cap, ~828 B): posted
  `IBV_SEND_INLINE`, buffer free at return → `send_message` completes the
  fire-and-forget contract with zero retention.
- Medium (≤ eager slab size, default 8 KiB, negotiated at bootstrap): bcopy
  into a pre-registered TX slab, unsignaled with periodic signaled sends
  (eRPC's `kUnsigBatch`-style; a signaled send costs up to 25% message rate
  **[lit]**), slab returns to pool on completion.
- Large (> slab, ≤ 16 MiB frame cap): v1 copies through a size-classed
  registered pool (eRPC HugeAlloc shape: powers-of-two classes, hugepage
  chunks); above the measured ~0.5–2 MiB crossover, zcopy the caller's
  `Bytes` via per-message `ibv_reg_mr` held until CQE (dereg is ~constant
  12.4–14.5 µs with THP **[measured-b200]**). Single WR either way — no
  fragmentation protocol exists in v1 at all.
- Head-of-line note: a 16 MiB WR occupies the QP ~1.3 ms at 100 Gb/s; if this
  bites, chunking is a *policy* change (N WRs), not a wire-format change.

### 6.3 Mailbox: SRQ + SEND/RECV (v1); write-with-imm ring (v2)

One SRQ per device context, slab size = negotiated eager ceiling + headroom,
depth sized at startup (one-shot on CX-7, §5.5), refill batched (post-recv
batch 16, UCX default), low-watermark armed via `srq_limit`. Every RX slab
reserves headroom for a repost-on-drop guard (UCX's desc+32 pattern, §3.2);
v1 copies out to `Bytes` (measured memcpy at these sizes is sub-µs and the
copy path decouples SRQ refill from consumer speed **[measured-b200]**);
zero-copy `Bytes::from_owner` handoff is a v2 experiment gated on the
downstream `BytesMut` audit (§8.6).

Phase 2 (large-message / GPUDirect path): receiver-advertised
`RDMA_WRITE_WITH_IMM` into per-peer rings — NCCL's CTS-FIFO + LITE's
imm-as-tag pattern, chosen over polling designs for §5.1 reasons. The v1 wire
format reserves `flags` bits so this arrives without a version break; the
advertised buffer can be a dmabuf MR (`ibv_reg_dmabuf_mr`), which is the
GPUDirect door **[source]**.

### 6.4 Flow control

Per-peer credit window W (default 64, ⌊soft W/2 / hard W/4⌋ request
thresholds), a credit = one SRQ slab the peer may consume. Sender decrements
on post; admission gate (unchanged, +16 ns) queues when W = 0 — RNR becomes
unreachable in steady state (§5.2). Credits return via piggyback byte or
PURE_GRANT. This composes with (does not replace) the Messenger mux's
per-direction budgets; whether transport credits should eventually be
*surfaced* to the mux instead of layered under it is an open analysis (§8.5).

### 6.5 Completion → tokio

Per-worker: one CQ (own comp vector), completion-channel fd `O_NONBLOCK` in
`tokio::io::unix::AsyncFd`. Correct loop (measured and race-checked):
`ibv_req_notify_cq` **first**, then drain `ibv_poll_cq` (closes the arm race),
then await readable → drain `ibv_get_cq_event` to EAGAIN → `clear_ready` →
batch-drain poll → ack events amortized (every 16) **[measured-spark]**.

Measured matrix (CX-7, deep idle constrained, IRQ isolated) **[measured-spark]**:

| strategy | p50 @100k/s | p99 | cores |
|---|---|---|---|
| busy-poll | 2.06 µs | 2.64 µs | 1.000 |
| raw epoll | 8.02 µs | 8.96 µs | 0.409 |
| **tokio AsyncFd** | **8.16 µs** | 9.10 µs | 0.463 |
| hybrid spin 5 µs | 7.07 µs | 9.07 µs | 0.672 |

tokio over raw epoll costs +0.14 µs — the interrupt path, not tokio, is the
penalty. A shared busy-poll core does **not** deliver 2 µs to workers: the
dispatch hop adds 3.34 µs (≈5.4 µs delivered) while burning a full core.
**Default: per-worker event mode** (8 workers × 10k msg/s = 0.37 cores);
busy-poll opt-in above ~27k msg/s/worker (breakeven ~215k/node); hybrid spin
only as an opt-in low-rate tail control (at 1k/s, 50 µs spin cuts p99.9
919→128 µs for 5.6% of a core). Send-side posting happens inline in
`send_message` under the per-QP serialization the gate already provides;
`ibv_alloc_td` drops provider locks.

### 6.6 Trait mapping

| trait member | implementation |
|---|---|
| `send_message` | gate → connection handle (DashMap, epoch-reaped like TCP) → inline post / slab post. Failures **after** admission: CQE error → `on_error(header, payload, err)` with the retained `Bytes` (retention: none for inline; slab+original-`Bytes` refs for signaled sends — the `SendTask`-as-failure-token pattern from `coalesce` carries over). |
| `max_message_size` | negotiated per-peer eager ceiling; 16 MiB default cap (matches TCP codec). |
| `check_health` | application-level: bootstrap-TCP probe for never-connected peers (mirrors TCP transport's connect-probe semantics), lightweight ACK-type AM ping with deadline for connected peers. Never derived from QP state alone (§5.3). |
| `begin_drain` / shutdown | 3-phase mapping: gate = stop accepting inbound (drain flag checked in the CQ drain loop, `ShuttingDown` reply as today); drain = quiesce SQs (reap in-flight signaled sends); teardown = cancel poller tasks, QPs → ERR, reap `IBV_WC_WR_FLUSH_ERR` completions (recovering buffers for `on_error`), destroy. `IBV_EVENT_QP_FATAL`/async events feed the same path **[source]**. |
| `set_observability` | as TCP: `OnceLock` handle; new counters worth having: credits-stalled, SRQ low-watermark hits, RNR NAKs seen, inline/bcopy/zcopy split. |

Ordering: one QP per peer, one lane — RC preserves order, the gate preserves
admission order; the contract holds with no reorder logic. (Multi-rail
striping would break it and is explicitly out of scope until §8.3 is
designed.)

### 6.7 Rust binding

**`ibverbs` + `ibverbs-sys` (jonhoo)** **[verified]**: active (pushed
2026-07-09, 29 contributors, 1 open issue), MIT OR Apache-2.0 (clean vs our
Apache-2.0), covers device/PD/MR (incl. ODP + relaxed-ordering flags), typed
RC/UC/UD QPs with both manual `modify()` and handshake sugar, SRQ, comp
channels with `as_fd()` for AsyncFd, inline send, extended-verbs
`SendBatch`/`SendOp` doorbell batching with signaled/fenced/solicited, rdmacm
feature, GID-table enumeration with RoCEv2 filtering. The 2025-era critique
that it lacks WR batching is stale **[corrected]**. Runner-up sideway
(MPL-2.0; statically vendors a forked rdma-core; mlx5dv bindings at the sys
layer) is the fallback if we ever need mlx5dv/DEVX — jonhoo's tree has zero
mlx5dv. async-rdma is GPL-3.0-only: **design reference only** (its per-CQ
AsyncFd task + batch-drain architecture is the pattern §6.5 measured), no code
reuse. Caveats: neither candidate tests aarch64 in CI (we become the first;
budget for it), and `post_recv`/`submit` remain `unsafe` with caller-enforced
MR lifetimes — our pool design makes those invariants structural.

---

## 7. Measured-numbers appendix

Reference tables for implementation-time decisions. Provenance per tag.

**ib_send_lat RC one-way µs (mlx5_0→mlx5_2, MTU 512, depth 1)** **[measured-b200]**
(≥8 KiB rows are bounded by the unexplained 11.6 Gb/s path ceiling — floor
figures only):

| size | t_typ (no inline) | t_typ (inline 828) |
|---|---|---|
| 64 B | 2.95 | **1.92** |
| 512 B | 3.61 | 3.62 |
| 828 B | 3.95 (1 KiB row) | 3.95 |
| 8 KiB | 9.99 | n/a (>828) |
| 64 KiB | 52.5 | n/a |
| 1 MiB | 785 | n/a |

**UCX proto-v2 AM boundaries, rc_mlx5, host memory, CX-7** **[measured-b200]**:
short ≤2038 · bcopy 2039–8118 · zcopy 8119–8246 · multi-frag zcopy ≤311,293 ·
rndv ≥311,294. With reply flag: short ≤2030. (Independent corroboration of the
§3.4 "rendezvous is optional for our sizes" call.)

**reg/dereg vs memcpy, x86_64, median-of-101, pre-touched** **[measured-b200]**:

| size | reg 4K pages | reg 2M THP | dereg THP | memcpy |
|---|---|---|---|---|
| 4 KiB | 13.7 µs | 13.7 µs | 12.4 µs | 0.05 µs |
| 256 KiB | 18.7 µs | 14.5 µs | 12.4 µs | 4.6 µs |
| 1 MiB | 36.3 µs | 18.7 µs | 12.5 µs | 27.5–35.8 µs |
| 8 MiB | 250.9 µs | 36.5 µs | 12.9 µs | 486–494 µs |

(4K-page dereg grows linearly to 561 µs at 64 MiB — use THP-backed pools.
UCX's 16 µs + 0.06 ns/B model is ~2× pessimistic on this silicon.)

**CQ notification matrix**: §6.5. **Incumbent software budget**: §2.
**Inline grant law**: §5.6. **ODP**: implicit-ODP registration (one lkey for
the whole VA space) works on CX-7, ~11 ms one-time **[measured-spark]**;
ISPASS'21 documents pathological damming/flood modes **[lit]** — opt-in
capability probe only, never the default path.

**Operational traps recorded for implementers** **[measured-b200]**: partial
pkey membership (0x7fff) silently breaks UCX (`UCX_IB_PKEY=auto` requires full
membership) while raw verbs works — pin pkey explicitly on partitioned IB;
perftest `-I` prints the *requested* inline, not granted; default
`ulimit -l 8 MiB` (DGX Spark ships this) fails `ibv_reg_mr` at 32 MiB —
memlock limits are a deployment requirement; single-host UCX benchmarks
silently select shm and report fantasy numbers.

---

## 8. Open questions & risks (ranked)

1. **No end-to-end two-node RDMA measurement exists yet.** Both gap-fill
   testbeds were degraded (no cable locally; isolated IB subnets on
   umbriel). The §6 design's *absolute* win is projected from §2's software
   budget + literature wire times; the two-HCA path corroborates but its
   bandwidth ceiling is unexplained. **First implementation milestone is a
   two-node smoke** (§9 P0).
2. **K8s deployment reality is unresearched** (critic-flagged, unfilled):
   IPC_LOCK/memlock budgets, rdma-shared-device-plugin vs SR-IOV, GID index
   inside a netns, and — most materially — **RoCEv2 without PFC/ECN degrades
   under congestion via go-back-N**; the fabric prerequisite must be stated
   for any production rollout. Native-IB partition (pkey) handling likewise
   (§7 trap).
3. **Multi-NIC**: WorkerAddress schema above carries a device list, but
   selection policy, rail failover, and the striping-vs-ordering conflict are
   undesigned.
4. **Peer restart semantics**: incarnation number in the blob + bootstrap
   re-exchange handles the steady case; reconnect-storm bounding and the
   interaction with discovery-cache staleness need a state machine before P1
   review.
5. **Credits × Messenger mux**: two flow-control layers (transport credits
   under mux per-direction budgets) can double-buffer or fight; analysis of
   deadlock-freedom and whether to surface credits through
   `SendAdmission`/`AdmissionState` (a velo-ext-touching change — coordinated
   bump rules apply) is pending.
6. **Zero-copy RX** hinges on `Bytes::from_owner` semantics and an audit that
   no downstream path (Messenger, streaming mux, codecs) converts inbound
   `Bytes` to `BytesMut` (which deep-copies owner-backed bytes, silently
   deleting the win). v1 copies; v2 decides on evidence.
7. **Security**: QPN/PSN/GID (and any future rkeys) published through shared
   etcd/NATS are usable by anyone with fabric reach + discovery read access —
   RDMA has no authentication. At minimum: never publish rkeys in the v1 blob
   (v1 has none), document the trust boundary, scope any future rendezvous
   rkeys per-peer.
8. **Inline crossover unsampled in 65–511 B** — exactly the header range.
   Cheap to measure in P0; sets the inline-request size (which trades against
   SQ depth).
9. **libfabric/UCCL/Mooncake were not scored** (§4.4); the RC-scaling eRPC
   "5,000 connections" figure resisted independent verification; DC-vs-RC for
   SEND workloads at scale has no published measurement.
10. **rxe fidelity for CI** is assumed, not proven: which of SRQ, inline,
    write-with-imm, and extended verbs SoftRoCE faithfully implements needs a
    P0 probe (module present on this box **[measured-spark]**; self-hosted
    runners can modprobe it, GitHub-hosted cannot).

---

## 9. Phasing

- **P0 — evidence completion (≤1 week, parallel with review of this doc):**
  two-node RoCE/IB smoke via `ib_send_lat`/`ucx_perftest` on a lab
  allocation; inline crossover sweep 64–512 B; rxe capability probe
  (`transport_integration_tests!` dry-run over rxe on a self-hosted runner);
  `Bytes::from_owner` downstream audit. The `tx_budget` harness (already
  in-tree from this research: `examples/examples/tx_budget.rs`) is the
  regression baseline.
- **P1 — messenger `Transport` (~4–6k LOC, 3–5 weeks):** §6 as specified,
  eager-only, `ibverbs` feature, jonhoo binding, per-worker AsyncFd
  completions, rxe-gated CI + hardware-gated integration job (org runners,
  TIPC-precedent `velo_ibverbs` cfg pattern), `ping_pong` example wiring.
  Register-gate semantics mirror TIPC's design: unreachable/ineligible peers
  return `NoEndpoint` so the priority sort promotes TCP — enable fleet-wide,
  engage where it works.
- **P2 — zero-copy RX + large-message path:** `Bytes::from_owner` RX handoff
  (if the audit clears), receiver-advertised write-with-imm ring for
  > crossover payloads, dmabuf-ready MR plumbing (GPUDirect door, no wire
  change).
- **P3 — streaming `FrameTransport`** over the same connection substrate
  (needs the `StreamConfig` variant treatment TIPC P2 describes).
- **P4 — scale escapes as needed:** XRC (standard verbs) first, DC (mlx5dv +
  `ibv_wr_*`) only with measured cause; multi-rail with explicit reordering
  design.

---

## 10. Found in passing — incumbent bugs (filed separately)

1. **TCP transport throughput collapse, ~100–350×, ≥16 KiB × 64 in-flight**
   **[measured-spark]**: 256 KiB/64 → p50 RTT **1.45 s**, 22.9 MB/s vs
   7,960 MB/s for the raw codec rung and 2,648 MB/s for the identical Velo
   path over UDS. TCP-transport-specific; the 2 MiB socket-buffer/clamp
   hypothesis was tested and refuted (−18%, not −100×). Suspects: NODELAY ×
   three-segment writes, dual unidirectional connections. Repro:
   `tx_budget --rungs l3t --sizes 262144 --inflight 64`.
2. **`write_frame_direct` issues three `write_all` syscalls per frame above
   the 64 KiB coalesce threshold** (`coalesce/mod.rs:476-481`: 11 B preamble,
   64 B header, payload, on a NODELAY socket): +10.2 µs one-way at 256 KiB
   **[measured-spark]**. Fix: stage preamble+header into one buffer or
   bounded writev.

Both are pure-software wins available before any RDMA work, and (1) likely
affects production large-frame traffic today.

---

## 11. Round 2 — the UCX path, scoped

Same day, second 7-agent program (4 research + 3 adversarial verifiers), run
after the requirement grew to include RDMA GET/PUT for `velo::rendezvous`.
All empirical work reused and extended the round-1 artifacts: the static UCX
1.22.0 build on this workstation, plus new probe programs (AM/RMA loopback,
cdylib link tests, two-UCX coexistence drivers). A new evidence tag applies
here: **[measured-spark]** now includes real ConnectX object behavior *and*
full UCX protocol behavior over `self`/`cma`/`tcp` lanes (no IB wire, ports
down — IB-lane behavior is source-read plus the round-1 B200 numbers).

### 11.1 The build.rs answer: yes — vendored tarball, static, verified prototype

**Direct answer to "would we pull and build a static UCX as part of build.rs":
yes, but vendor the release tarball inside the crate rather than pulling at
build time** — hermetic, crates.io-clean, and the whole thing already exists
as a working prototype: a complete `ucx-src` crate (build.rs, Cargo.toml,
lib.rs, vendored 3.5 MiB tarball) plus a consumer cdylib were built and
verified end-to-end this session **[measured-spark]** (prototype preserved at
the session scratchpad, `w/ucx-src/` + `w/probe2/`).

Measured costs: **25.3 s cold build** (72.9 CPU-s — budget 2–4 min on a
2-vCPU runner), **0.17–0.45 s warm** (a stamp file keyed on the exact
configure-args makes incremental builds free and flag changes correctly
rebuild), 153 MB OUT_DIR, `cargo package` = **3.52 MiB** against the 10 MiB
crates.io limit. docs.rs: guarded by `DOCS_RS` early-return emitting a
`ucx_stub` cfg (verified 0.39 s). Cross-compilation: UCX's `config.sub`
accepts rustc triples verbatim; the one trap is configure silently falling
back to the native `cc` when no cross toolchain exists (exit 0!) — build.rs
must assert the cross compiler. License: BSD-3; ship `LICENSE-UCX` in the
crate, pin the tarball SHA-256, expose `pub const UCX_LICENSE` for downstream
attribution.

**Four corrections to the round-1 recipe, all measured, all load-bearing:**

1. **`--with-pic` is mandatory.** Static-only libtool emits non-PIC objects;
   linking them into any shared object fails
   (`relocation R_AARCH64_ADR_PREL_PG_HI21 … recompile with -fPIC`) — and
   Dynamo's Python extension is exactly a cdylib. Round 1 never caught this
   because it only linked executables. Worse, a Rust cdylib link against the
   non-PIC archives *succeeded* silently while producing a broken library.
2. **The constructor set must be complete, and derived, not hardcoded.** The
   full closure over the `.pc` files is `ucp_global_init, uct_init,
   ucs_init, uct_ib_init, uct_mlx5_init, uct_rdmacm_init[, uct_cma_init]`.
   Omitting `ucs_init` alone produces a library that links, loads, and fails
   `ucp_init` with a bare `UCS_ERR_INVALID_PARAM` and **zero log output at
   any level** — the logging subsystem is itself what failed to register.
   (Also: drop `--disable-logging` from the flag set; it doesn't compile
   logging out and total silence is the key diagnostic.)
3. **`cargo:rustc-link-arg` does not propagate across crate boundaries**, so
   the `-Wl,--undefined=` mechanism cannot live in a build script at all.
   The working, verified mechanism: `#[used]` static array of
   `unsafe extern "C" fn()` references to the constructor symbols in the
   crate's `lib.rs`, with archives emitted as `cargo:rustc-link-lib=static=`.
   Second landmine: a downstream crate that never *references* the crate
   drops it (and every native lib) from the link — `use ucx_src as _;` is a
   hard invariant.
4. **rustc's `-nodefaultlibs` + one-pass archive resolution** leaves
   `__aarch64_ldclr4_sync` (outline atomics — static `libgcc.a` only, aarch64)
   and `pthread_atfork` (`libc_nonshared.a`) unresolved; build.rs must
   re-append `static=gcc` (aarch64) and `dylib=c` after the UCX archives.

Other settled points: **hard-fail when the `ib` feature is on and rdma-core
headers are absent** — UCX's configure only *warns* and silently ships a
working TCP/shm-only UCX (the CI-goes-green-production-gets-TCP failure,
fully realized in a test build); system-UCX escape hatch via `UCX_DIR`
parsing `ucp_version.h` (verified against the system 1.20 install), API floor
1.10.0, supported floor 1.17–1.18; `links = "velo_ucx"` (lamellar-ucx-sys
owns `"ucx"`). Crate-shape tension with the workspace rules: a published
`ucx-src`-style crate is a third publishable crate (CLAUDE.md rule 1), but it
sits *outside the velo type graph* (build-dep only, exports no shared types),
so it cannot reproduce the dual-copy bug class the rule exists to prevent —
publish it as a separate leaf crate with a documented exception, or push the
vendoring into `lib/velo/build.rs` with checked-in bindings at
`transports/ucx/sys.rs` (at the cost of shipping the 3.5 MiB tarball inside
the `velo` package itself). Recommend the separate leaf crate.

### 11.2 Two-UCX-in-one-process: retired as a blocker

Round 1 ranked this the top operational hazard (NIXL loads its own libucp in
Dynamo). Round 2 measured it **[measured-spark]**:

- A Rust cdylib linking static UCX exports **zero** UCX symbols with no extra
  flags: rustc auto-emits a version script (`{global: <exports>; local: *;}`)
  that makes all 3,739 UCX symbols `STB_LOCAL`, while ELF constructors still
  fire. (Adding your own version script is a hard link error;
  `--exclude-libs,ALL` is redundant.) Symbol isolation is structural, free,
  and verified by `nm -D`/`objdump -T`/`readelf`.
- **8/8 coexistence cells pass**: static 1.22 cdylib + dlopen'd system
  libucp 1.20, both load orders × RTLD_LOCAL/GLOBAL × UCM hooks on/off —
  both `ucp_init`s succeed, no cross-binding, no crash, and mmap/malloc churn
  with both UCMs' trampolines chained works. With
  `UCX_MEM_EVENTS=n UCX_RCACHE_ENABLE=n`, **zero libc functions get patched
  at all** (verified by byte-diffing libc entry points, not log-grepping) —
  and velo never needs the rcache (it pins explicitly), so that is the
  default.
- Residuals: (a) rcache invalidation under live IB registration was
  untestable here (ports down) — `UCX_RCACHE_ENABLE=n` is the validated
  mitigation; (b) the "hooks-off ⇒ clean dlclose unload" sub-claim did not
  reproduce under adversarial re-testing (UCM self-pins via `RTLD_NODELETE`
  in `--with-pic` builds regardless) — irrelevant for velo, which never
  dlcloses, but recorded for accuracy.

### 11.3 The wrapper velo would own (~4.9k LOC incl. tests)

Full module-by-module design exists in the research record; the load-bearing
decisions:

- **One dedicated OS thread owns the `ucp_worker`** (`UCS_THREAD_MODE_SINGLE`),
  plain `poll(2)` loop on `ucp_worker_get_efd` with the documented
  arm/progress protocol; completions reach tokio via oneshots/flume (both
  wake from any thread). Build with `--enable-mt` + `mt_workers_shared=1` and
  pin workers SINGLE: source-verified, a SINGLE worker in an MT build takes
  **no lock** (the conditional CS macro reduces to a debug-only owner-thread
  assert), so the N-worker scaling escape (peer→worker by consistent hash)
  stays open at zero hot-path cost. The debug-only assert is a release-mode
  UB hazard closed by a `!Send` `WorkerThreadToken` required at every FFI
  call site.
- **Submission**: MPSC ring + adaptive-spin doorbell. Measured
  `ucp_worker_signal`→`poll` wake = **3.09 µs** — *worse* than the existing
  2.4 µs tokio hop, so a naive doorbell is a regression; with a ~20 µs spin
  window after the last completion, loaded submitters skip the signal
  entirely (~50–100 ns ring push) and only an idle worker pays 3 µs.
- **Cancellation soundness (closes async-ucx issue #1 structurally):**
  every op is completion-owned — one `Arc<OpState>` holding the
  `Bytes` + `on_error` handler goes through `user_data`; the UCX completion
  callback drops it; an awaiting future holds only a oneshot receiver.
  Dropped future ⇒ op continues, buffers live, `on_error` still fires.
  `ucp_am_send_nbx` has three mutually exclusive exits (NULL /
  request-ptr / error-ptr), exactly one drops the Arc, and the exit taken is
  **non-monotonic in size** (measured over tcp: 64 B→req, 64 KiB→NULL,
  1 MiB→NULL, 8 MiB→req) — so the test suite must sweep sizes. All
  `extern "C"` trampolines wrap in `catch_unwind`.
- **Pin `UCP_AM_SEND_FLAG_EAGER` on every AM send.** A real doc-vs-source
  refutation forced this: `UCP_AM_FLAG_PERSISTENT_DATA` does **not** prevent
  rendezvous-mode receives (ucp.h's guarantee is contradicted by
  `ucp_am_rndv_process_rts`, which sets `FLAG_RNDV` unconditionally;
  measured: an 8 MiB AM arrived RNDV despite PERSISTENT_DATA, and arrived
  eager with the flag pinned). Pinning EAGER deletes the entire two-stage
  `ucp_am_recv_data_nbx` module and caps AM size — large payloads route
  through rendezvous-GET, which is the shape both paths converge on (§12).
  v1 copies on receive (shutdown cannot wait on descriptors held by user
  code; descriptor pools are uncredited cross-peer flow control; and
  `ucp_am_data_release` is worker-thread-scoped so a zero-copy drop-guard
  costs a cross-thread hop anyway).
- **No listener, no bootstrap, no wireup**: `register(PeerInfo)` =
  `ucp_ep_create` from the peer's worker-address blob (measured 239 B; carry
  it in `WorkerAddress` with `am_id_base`, eager ceiling, incarnation).
  The entire §6.1 bootstrap subsystem of the ibverbs design does not exist
  on this path, and §8.4's restart races mostly dissolve (a restarted worker
  has a new address blob). `max_am_header` = 8101 measured; header size is a
  second cap the trait can't express — document it, fail pre-wire.
- LOC: ~3.9k non-test + ~950 test ≈ **4.9k**, bottom-up ±30% (calibration:
  async-ucx main is 2.5k for a broader-but-unsound surface). What to mine
  from async-ucx PR #11: the owned-`Bytes` WorkerAddress, the zeroed
  `RequestParam` builder, the bindgen allowlist (~200 LOC of value); the
  safety architecture is rebuilt, not rebased.

### 11.4 UCP RMA for rendezvous — semantics, and the two real hazards

The good news first **[measured-spark + source]**: `ucp_mem_map` registers
caller-provided host memory (one mapping can serve many `DataStore` slots);
the packed rkey for a single-IB-device host build is **20 bytes** (exact
serializer read; +9 B per extra device — the `md_map` bitmap in the blob *is*
UCX's multi-rail mechanism); `AcquireResponse::Rdma{descriptor}` therefore
needs ~40 bytes of msgpack. **GET completion is total** — data landed, no
flush needed. **PUT completion is local-only** — remote visibility requires
`ucp_ep_flush_nbx`, and the CMA measurement proving put-visibility-at-
completion is a per-transport accident, never a contract (a future
rendezvous-PUT path must flush before signalling). The consumer's destination
buffer needs no explicit map — `get/zcopy` registers on the fly through the
rcache — but destination buffers must be pooled or every fresh VA range pays
a fresh `ibv_reg_mr`. A single 100 MiB GET works (measured, 5.3 GB/s over
CMA; UCX fragments internally); chunking survives only as a velo-level policy
choice for progress/cancel granularity (8–32 MiB, not today's 512 KiB).
Concurrent GETs beyond `max_rd_atomic=16` queue transparently — no
hand-limiting needed, backpressure stays in the `AdmissionGate` sized by
bytes in flight. One `ucp_ep` from one OOB blob serves AM + RMA (lanes are
per-purpose within the ep), but `UCP_FEATURE_RMA` must be requested at
`ucp_init` — and requesting it perturbs AM lane selection, so the messenger's
measured behavior can shift when RMA is compiled in.

**API trap**: `ucp_memh_pack()` — the documented-in-`ucp.h` packer —
`ucs_fatal`s (process abort, not an error) unless the EXPORT flag is set; the
only working rkey packer is the "deprecated" `ucp_rkey_pack()` in
`ucp_compat.h`. Bind that one. `ucp_get/put_nbx` also hard-require
`proto_enable` (the 1.22 default — but `UCX_PROTO_ENABLE=n` is a common
workaround for proto-v2 bugs and silently kills all RMA), and multi-rail GET
requires `UCX_MAX_RMA_RAILS≥2` (default **1**).

**Hazard 1 — one-sidedness is a lane property, not an API property.** With a
native RMA lane, a GET completes with the owner never progressing (measured:
100 MiB in 19.9 ms, owner idle). Without one, UCX **silently** substitutes
`rma_am` software emulation over the AM lane, and the GET, the flush, and
PUT remote-visibility all **hang forever** on an un-progressed owner
(measured: 1 s, 5.9 M polls, nothing). This doesn't break CI (the owner that
answered `_rv_acquire` is progressing), but it forbids: a separate
un-progressed RMA worker; an owner progress loop that parks after the AM
queue drains while leases are outstanding; treating an issued descriptor as
license to idle. Mitigation: gate `StageMode::Pinned` on the ep having a real
`rma`/`rma_bw` lane (not `rma_am`), and keep the owner's progress loop live
for the lease lifetime.

**Hazard 2 — `UCP_ERR_HANDLING_MODE_PEER` eliminates lanes — RESOLVED for
RC (P0 probe, live CX-7 IB, umbriel-b200-006, UCX 1.22.0 and 1.19.0).**
The local measurement stood: PEER mode makes `self`/`posix`/`sysv` eps fail
creation and silently drops `cma`'s `rma_bw` lane. But on RC the feared
DEVX/KSM dependency **does not bind**: `select.c:2259` does add
`UCT_MD_FLAG_INVALIDATE_RMA` under PEER+rndv, and then `select.c:565-574`
strips it again for peer-to-peer (connect-to-ep) lanes — "both sides close
the connection in case of error". All RC transports are connect-to-ep, so
**`rma_bw#0` survives PEER mode on `rc_mlx5` with DEVX on, with
`UCX_IB_MLX5_DEVX=n`, and on `rc_verbs`** (lane lists byte-identical across
1.22/1.19; PROTO_INFO shows `zero-copy`, never `software emulation`; the
DEVX-off control genuinely removed the invalidation capability and the lane
survived anyway). A 1 MiB GET completed with the owner worker never
progressed (826 µs, data verified) — true one-sided on RC. **Velo takes both
peer-death detection and zero-copy RMA**; the `StageMode::Pinned` gate tests
for a real `rma_bw` lane (still correct), not for DEVX. The KSM requirement
only bites connect-to-iface transports — i.e. intra-node shm RMA under PEER
still falls back to emulation, unchanged.

Three more probe facts for the wrapper: (i) `UCX_TLS=rc_mlx5` alone cannot
wire up (`no auxiliary transport … Destination is unreachable`) — a
`ud`-class transport must be in the TLS list; under PEER the ud lane is
`keepalive wireup` only, and without PEER it disappears entirely (the config
must always include it). (ii) The `ucp_am_send_nbx` NULL-vs-request-ptr exit
is **version-sensitive** (1.19 returns NULL at sizes where 1.22 returns a
request) — the wrapper's three-exit discipline must never assume either.
(iii) Measured packed rkey on a real IB MD: 19 B (1.22) / 18 B (1.19), vs
the 20 B derived from source; and `ucp_mem_map` on a live IB context costs
161–805 µs for 4 KiB–16 MiB (3.2 ms at 100 MiB) — far above the bare
`ibv_reg_mr` numbers, making pin-once-at-stage and pooled destination
buffers mandatory, not advisory.

### 11.5 What round 2 resolves or changes in the round-1 ledger

- §1.2 / §4.2(iv) two-UCX hazard: **retired** (→ caveat: set
  `UCX_MEM_EVENTS=n`, assert symbol hygiene in CI). §4.2 recipe: corrected.
- §8.10 CI story: the UCX path runs the full `transport_integration_tests!`
  suite over `UCX_TLS=tcp,sm` on stock runners — the identical code path,
  demonstrated this session. That is strictly stronger than the ibverbs
  path's rxe assumption (still unproven).
- §8.2/§8.3 (K8s GID/pkey, multi-NIC selection, rail failover): absorbed by
  UCX on that path (with §7's pkey trap still applying); remain velo design
  work on the ibverbs path.
- §8.4 restart semantics: mostly dissolves on the UCX path (address blob
  changes on restart); remains on the ibverbs path.
- §8.5 flow-control composition: **worse on the UCX path** — three layers
  (mux budgets / AdmissionGate / UCX's internal FC + pending arbiter), and
  UCX's layer is not observable the way velo-owned credits are. Unanalyzed
  on both paths; carried forward.
- New open question (UCX): PEER × DEVX/KSM on real hardware (§11.4);
  eager-pinned multi-MiB AM behavior over rc_mlx5; the 3.09 µs doorbell and
  spin-window tuning on x86_64.

---

## 12. Rendezvous RMA — requirement update (applies to both paths)

`velo::rendezvous` is receiver-driven pull with the RMA seams already cut:
`AcquireResponse::Rdma { lease_id, descriptor }` and `StageMode::Pinned` are
in-tree placeholders, `Consumer::get/get_into` have explicit `bail!` arms to
fill, and the transparent `RendezvousStager`/`Resolver` implement the
messenger's `LargePayloadStager/Resolver` (threshold 256 KiB) — so **the GET
path accelerates large messenger payloads and explicit rendezvous through one
mechanism**, and both transport candidates converge on the same shape: AM
capped near ~1 MiB, bulk through rendezvous GET.

**Common protocol work regardless of path** (from the counterfactual track,
all verified against the tree):

1. `RvAcquireRequest` carries only the handle — no consumer capability/
   preference signal. The owner cannot know the peer has RDMA reachability
   before offering `Rdma`. A `#[serde(default)]` capability field is
   wire-compatible but is real work, plus an owner-side reachability check
   mirroring the §6.1 register-gate.
2. **Lease TTL + owner-side reaper: net-new subsystem.** RDMA READ/GET
   produces *no responder-side completion*; a consumer that dies mid-GET
   leaves the owner holding a read lock and a pinned MR indefinitely —
   `DataSlot.ttl` exists but nothing reads it, and no reaper task exists.
   Compounds into memlock exhaustion under §7's `ulimit -l` trap. Grace
   period should derive from dead-peer detection (~3–4 s), not be invented.
3. **MR scope = per-slot** (rkey is a remote-read capability; an arena MR
   leaks neighboring slots; revocation = dereg at release, which the
   existing refcount/read-lock lifecycle supports). Type-2 memory windows
   would be the finer-grained alternative but jonhoo's binding has no MW
   posting support (hardware has it; binding doesn't).
4. **`StageMode::Pinned` staging semantics must be decided**: register the
   caller's `Bytes` in place (fast; alignment/THP-dependent) vs copy into a
   pre-registered THP arena (predictable dereg cost; pays a memcpy). This is
   an API decision, currently implicit.
5. `get_into()` cannot be zero-copy as designed — `RendezvousWrite::
   write_chunk(offset, &[u8])` is copy-in and cannot expose a registered
   destination. Either extend the trait (`as_registered_slice()`-style) or
   scope v1 zero-copy to `get()` only.
6. Multi-HCA descriptor minting (per-device MRs vs per-consumer lazy
   minting) changes the MR-lifecycle cost by a device-count multiplier —
   decide explicitly.

**ibverbs-path additions** (like-for-like with §11.4): a **dedicated bulk RC
QP per peer** — two independent reasons: RC processes WQEs in order, so bulk
READs head-of-line-block AM sends on a shared QP; and any non-flush
completion error (e.g. `REM_ACCESS_ERR` from a stale-rkey race) drives the
whole QP to ERROR, killing AM traffic for that peer too. **Reserve the
second QPN in the P1 bootstrap struct now** even though RMA ships in P2 — a
later addition is a wire-format break. Windowed chunked READs (512 KiB
chunks, window = negotiated `max_rd_atomic` — **jonhoo's builder defaults
`max_rd_atomic` to 1 for RC; leaving it unset silently serializes the read
window**, measured cap 16 on CX-7); 12-byte `{addr,rkey}` descriptor
(`RemoteMemorySlice` precedent in the crate); dmabuf GPU door confirmed
wrapped (`ProtectionDomain::register_dmabuf`). Estimated **+1.8–2.7k LOC on
top of P1** — roughly the size of today's entire rendezvous module again,
properly a P2 phase.

**UCX-path additions**: §11.4's `rma.rs` + rendezvous integration are inside
the ~4.9k total; a velo-internal `RmaProvider` trait keeps
`velo::rendezvous` decoupled from the `ucx` feature (staying velo-internal
avoids any velo-ext bump; promoting it later so out-of-tree transports can
serve RMA is a deliberate rule-3/4 decision).

---

## 13. Decision matrix

Both paths are now scoped like-for-like: messaging transport + rendezvous
GET, AM capped with bulk routed through rendezvous, same worker/op-state/
admission/health skeleton, same common protocol work (§12.1–6).

| axis | pure ibverbs | UCX (vendored static) |
|---|---|---|
| total new code (incl. tests) | ~6–8.5k LOC (P1 4–6k + RMA 1.8–2.7k) | ~4.9k LOC (±30%) |
| what velo owns forever | wire protocol: credits, SRQ sizing (one-shot on CX-7), RNR/timeout tuning, QP state machine, bootstrap listener, fragmentation policy, multi-NIC/GID/pkey selection, restart races | safe FFI wrapper: callback soundness (`catch_unwind`, completion-owned ops), thread-affinity discipline, vendored C build + bindgen pinning, proto-behavior observability |
| deleted risk | no C dependency in-crate; no FFI callbacks (poll-only); full wire transparency; jonhoo's maintained sys layer for free | wire protocol, wireup/bootstrap, fabric heterogeneity, NIC quirks, rendezvous/zcopy thresholds, restart semantics — a decade of UCX hardening |
| added risk | rxe CI fidelity **assumed**; K8s GID/pkey/multi-rail all velo work; protocol bugs are velo bugs | proto-selection opacity (boundaries move across versions); version-skew vs checked-in bindings (mitigated: vendor + pin); 3-layer flow control; PEER × DEVX/KSM open hardware question |
| CI without hardware | rxe on self-hosted runners (unproven, §8.10) | `UCX_TLS=tcp,sm` on stock runners — **demonstrated**, identical code path |
| register()/bootstrap | TCP listener + per-conn QPN/PSN exchange + incarnation design (§6.1, §8.4) | `ucp_ep_create(blob)` — subsystem doesn't exist |
| intra-node | ~parity with UDS (§2); loopback RDMA needs lo-capable lanes | shm lanes free — but PEER mode disables shm RMA (§11.4) |
| GPU door | dmabuf MR plumbing project (P2, wrapped in binding) | `mem_type` byte + memory-type registration; GPU also unlocks UCX's RMA-rendezvous protocols |
| performance ceiling | full control: inline 828 B path, own thresholds; rc_mlx5-class DevX tricks unavailable (plain verbs) | rc_mlx5 accelerated transports (234 B–2 KiB short path, BlueFlame, inline-scatter CQE) for free; wqe-level control unavailable |
| flow control | velo-owned credits — observable, composable with mux by design | UCX-internal window + arbiter beneath velo's gate and the mux — opaque, 3 layers |
| failure semantics | seconds-scale RC timeouts (§5.3), app-level health (both paths) | same underlying timeouts; PEER-mode ep callbacks + FORCE close, at the §11.4 lane cost |
| supply chain | rdma-core (system) + ibverbs-sys crate | rdma-core (system) + vendored BSD-3 tarball velo re-ships and bumps per UCX minor |

**How to decide.** The paths differ less in destination than in what kind of
engineering velo signs up for: *protocol ownership in safe Rust* (ibverbs) vs
*soundness engineering at an FFI boundary in front of a mature but opaque
protocol stack* (UCX). Concretely:

- Pick **ibverbs** if wire-level transparency and all-Rust debuggability are
  worth ~1.5–3.5k extra LOC and owning fabric heterogeneity (multi-NIC,
  pkey, GID-in-netns, K8s) yourself — with rxe CI risk open.
- Pick **UCX** if the rendezvous-RMA + future-GPU amortization and the
  deleted protocol/fabric surface are worth the FFI-soundness discipline and
  the vendored-C ownership — with the PEER × KSM hardware question answered
  first.

### 13.1 P0 probe results and the decision (2026-08-20, umbriel-b200-006)

The three UCX-side probes ran on a live-IB CX-7 host (x86_64, MLNX OFED
25.07, `mlx5_0:1` pinned per the §7 pkey trap; UCX 1.22.0 built on-node,
cross-checked against system 1.19.0 — every P1 result identical on both):

- **P1 — PEER × `rma_bw`: PASS** (detail merged into §11.4). The one result
  that could have flipped the UCX lean did not; the hazard was misscoped and
  RC keeps zero-copy one-sided GETs under full peer-death handling.
- **P2 — eager-pinned AM: viable through 8 MiB.** `recv_attr` = DATA at every
  size (RNDV never appears with the flag pinned; without it, sends flip to
  RNDV at ≥1 MiB), and eager 8 MiB was marginally *faster* than rendezvous
  (1.28 vs 1.21 GB/s). Caveat: the single-port loopback path caps at
  1.43 GB/s (`ib_read_bw` envelope — UCX reached 83–98% of the raw-verbs
  ceiling on the identical path), so "not pathological" is established only
  up to that rate; the two-node run (§8.1) remains open and could still move
  the AM-cap threshold.
- **P3 — doorbell on x86_64:** `ucp_worker_signal` p50 0.83 µs / p99 1.28 µs
  against a spinning poller; 7.3 µs p50 with a blocking `poll()` and a
  ~115–165 µs cpuidle tail that a bare eventfd control reproduces exactly —
  platform idle-exit, not UCX. Confirms the §11.3 spin-then-park design and
  reclassifies the aarch64 3.09 µs single sample as a lucky blocking draw.

**Decision (per the "probe first, lean UCX" call): the UCX path is
selected.** Implementation proceeds per §11 — the vendored crate hardened
from the verified prototype, the wrapper with completion-owned op state,
EAGER-pinned AM, `register()` = `ucp_ep_create`, and the §12 rendezvous
integration behind a velo-internal `RmaProvider`. The pure-ibverbs design
(§5–§6) stands as the documented fallback and as the reference for what UCX
must beat if a wall appears.

*Implementation status (same day):* the crate shipped as **`ucx-rs`**
(`crates/ucx-rs`, workspace member with a documented publish exception;
`ucx-sys` is squatted on crates.io by the dead 2018 lemonrock binding) with
checked-in bindings and a link-and-run smoke test, and the messenger
transport landed as `velo::transports::ucx` behind the `ucx` feature —
clippy-clean under `--all-features -D warnings`, 10 unit tests + the full
21-scenario `transport_integration_tests!` suite green over `UCX_TLS=tcp`,
and `ping_pong --transport ucx` live end-to-end. Remaining for the phase:
apply adversarial-review findings, publish `ucx-rs`, CI wiring
(rdma-core headers on runners), then the §12 rendezvous RMA integration.

**Remaining open items, carried into implementation:** two-node fabric
numbers (§8.1) — now also to validate the eager AM cap and rendezvous
threshold at real bandwidth; the §8.5 three-layer flow-control composition
analysis (mux budgets / AdmissionGate / UCX-internal FC) — the most
important unresolved design question on the chosen path; the
`Bytes::from_owner` downstream audit (gates v2 zero-copy RX only);
K8s/PFC deployment prerequisites (§8.2, unchanged — UCX absorbs GID/pkey
selection but not fabric configuration); and rcache-invalidation coexistence
under live IB registration (§11.2 residual — moot if `UCX_RCACHE_ENABLE=n`
stays the default, as designed).
