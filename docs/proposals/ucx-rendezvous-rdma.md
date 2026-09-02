# UCX RDMA rendezvous integration — plan

Status: **approved** (2026-08-25, with amendments below). Branch: `ai/ucx-velo-rendezvous-integration-e91f5e`.

Sign-off amendments:
- External registration: `unsafe fn` + ptr/len constructor confirmed (D5).
- Lease deadlines confirmed; add a keepalive AM that extends the deadline (D8).
- EP idle close: off by default; connection-pool policy revisited later (D9).
- **New requirement (D12): the design must admit a later NIXL, libfabric, or
  other RDMA backend** — the registration layer and wire protocol are
  backend-discriminated; only the backend mechanics are UCX-specific.
- Hardware checkpoint: pause at the Phase-3 validation target with restart
  instructions if no cluster is reachable from this session (§2, checkpoint).
  **Discharged 2026-08-29** —
  `agent-docs/2026-08-29-rdma-phase3-hardware-checkpoint.md`.

Scope: native UCX RDMA GET path for `velo::rendezvous`, first-class memory
registration (internal pool + external RAII), registration/EP lifecycle
management. Supersedes the NIXL approach of PR #40 (open, unmerged,
CONFLICTING, no human review); adopts its good ideas, avoids its measured
mistakes (adopt/avoid analysis in the research appendix).

Research basis: 9-agent fan-out (3 codebase maps, UCX 1.22.0 source-level RMA
deep dive, prior-art survey of NIXL/Mooncake/libfabric/MPI-rndv/NCCL, Rust
allocator/RAII/cache research, PR #40 deep dive, adversarial completeness
critique). Reports archived in the session scratchpad; load-bearing facts are
inlined below where a decision depends on them.

---

## 1. Decisions

Each decision names its alternative and why the alternative lost. These are
the judgment calls; everything after §1 is consequence.

### D1 — Native `ucp_get_nbx` on the existing UCX transport context. No NIXL.

The `ucp_context` already requests `UCP_FEATURE_RMA`
(`transports/ucx/worker.rs:879`) with a comment reserving it for exactly this.
The two in-tree "Phase 2" comments disagree (`rendezvous.rs:18-21` says NIXL —
pre-Phase-3 lines, deleted by the cleanup Phase 3 records below;
`worker.rs:876-877` says native GET); this plan resolves them to native, and
Phase 3 updates the stale doc comment.

*Alternative considered — accept UCX's own rendezvous AMs* (drop the
`UCP_AM_RECV_ATTR_FLAG_RNDV` rejection and let UCX's internal RTS/RTR/GET
machinery move large AM payloads): rejected. It accelerates *push* messaging,
not the pull-by-handle model rendezvous is for, gives no external-memory
registration story, and reopens the descriptor-ownership/shutdown problem the
AM receive path deliberately closed (`worker.rs:553-561`).

### D2 — GET-first, receiver-driven, decided owner-side at `_rv_acquire`.

UCX 1.22 facts make GET strictly simpler than PUT:

- `ucp_get_nbx` completion is authoritative at **both** ends (`ucp.h:3893-97`)
  — no flush, no fence, no ordering hazard.
- `ucp_put_nbx` completion only frees the local buffer; notifying the peer
  requires a *completed* `ucp_ep_flush_nbx` first, and AM/RMA ride different
  lanes with no cross-lane ordering. Over `UCX_TLS=tcp`, PUT-flush additionally
  requires the peer to be progressing (deadlock class in shutdown paths).

Flow (mirrors PR #40's one good structural idea): consumer sends `_rv_acquire`
(existing handler) advertising RDMA capability; owner replies
`AcquireResponse::Rdma { lease_id, descriptor, lease_timeout_ms }` iff the slot
is pinned AND the request advertised capability, else `Ready{...}` (chunked).
Consumer issues `ucp_get_nbx`, then `_rv_detach`/`_rv_release` as today. The
acquire round-trip is the linchpin: the owner revalidates the registration on
every transfer, so **no cross-node invalidation protocol is needed for slot
memory**.

`lease_timeout_ms` is a correction to this plan's original two-field
description, not an addition made later: as built the response carries it as a
`#[serde(default)] u64` where `0` means *no deadline*, which is the encoding an
owner from before the reaper produces by omitting the field
(`lib/velo/src/rendezvous/protocol.rs:134-144`). It is skew-load-bearing, so it
belongs in any description of the response.

### D3 — Single-use unpacked rkeys in v1. No consumer-side rkey cache yet.

The critique surfaced a real conflict: "cache unpacked rkeys" vs "you then owe
a distributed INVALIDATE/ACK handshake with timeouts". Discriminating facts:

- Unpacked `ucp_rkey_h` is bound to `(region × EP)`, must die before its EP,
  and unpack is cheap (mpool-backed, `ucp_rkey.c:955-967`).
- A stale rkey on RC = `IBV_WC_REM_ACCESS_ERR` = **the whole QP dies**, killing
  every in-flight transfer on that connection. Never design for
  lazy-failure-and-retry.
- Four independent progress-thread paths destroy EPs (incarnation replace,
  reg-epoch revalidate, error reap, teardown); a cross-thread rkey cache would
  need hooks at all four.

So v1: the *packed* rkey (plain bytes, EP-independent) travels in the
descriptor per transfer; the consumer unpacks on the progress thread, GETs,
and destroys the rkey at op completion. The rkey's lifetime is the op's
lifetime — no cache, no invalidation protocol, no EP-destruction hooks beyond
"outstanding ops complete (or cancel) before EP close", which the
completion-owned `OpState` discipline already provides. Naming rule from the
critique, adopted verbatim: `PackedRkey(Bytes)` crosses the wire and lives
anywhere; `ucp_rkey_h` never leaves the progress thread.

*Deferred:* a consumer-side unpacked-rkey cache keyed
`(peer, ep-incarnation, region_generation)` living in `WorkerState`, if
per-transfer unpack ever shows up in a profile. Phase 5 decides on
measurements, not vibes.

### D4 — Arena-pool registration; per-slot registration is not a thing in v1.

Velo runs with `UCX_RCACHE_ENABLE=n` / `UCX_MEM_EVENTS=n` (deliberate — no
malloc hooks in the process; keep it). Under that config every `ucp_mem_map`
is a fresh `ibv_reg_mr` (linear in size) and UCX does zero on-the-fly caching,
so pre-registered arenas + `UCP_OP_ATTR_FIELD_MEMH` on the local side are
mandatory, not an optimization.

Pool design (from the Rust research, verified against `offset-allocator` 0.2
source):

- `ArenaSet`: append-only `Vec<Arc<Arena>>`, geometric growth (64 MiB → cap),
  each arena = one page-aligned allocation + one `ucp_mem_map` + one packed
  rkey (`Bytes`). *As built the allocation is `std::alloc::alloc_zeroed` under
  a granule-aligned `Layout`, not `mmap`*
  (`lib/velo/src/rendezvous/rdma/arena.rs:171-190`) — the plan named the
  syscall it expected the allocator to reach for, which is not a property
  anything depends on.
- Suballocation: `offset-allocator` (MIT, zero unsafe, O(1) alloc+free) behind
  `parking_lot::Mutex`, allocating in **4 KiB granules** (u32 range → 16 TiB
  per arena; float-bin waste ≤12.5%). `with_max_allocs` explicitly sized — but
  **sized *up*, to the granule count, not down**
  (`arena.rs:956`, rationale `arena.rs:80-84, 948-955`). *The plan sized it
  down because `Allocator::new`'s 128 Ki default eagerly allocates ~4 MB of
  node metadata per arena* — the arithmetic holds (128 Ki × ~28 B ≈ 3.6 MB) and
  the code names that same default as a trap (`arena.rs:954`), then pays the
  cost regardless. What was backwards is the direction, not the number:
  shrinking the node pool below the granule count trades a visible fixed cost
  (~28 B per granule, ~0.7% of the arena) for an invisible one, where a
  fragmented arena reports "full" while it still has room and the pool maps
  another arena it did not need.
- Oversize requests (≥ threshold, default 64 MiB) get a dedicated arena —
  avoids the 12.5% bin round-up costing 128 MiB on a 1 GiB object.
- The free token is the private `Allocation` value held in an owner-side
  table; the wire descriptor is `(addr, len, generation, packed_rkey)` — a
  remote address descriptor is *not* a free token, and the two never conflate.
- **Pool exhaustion falls back to chunked AM, never a hard error** (PR #40's
  64 MiB hard-error arena is its worst operational property).

Registering arbitrary caller `Bytes` in place is unsound as a default
(provenance uncontrolled; UCX rounds the pinned range outward past what the
caller owns; `from_static`/sub-slice cases). A register-in-place mode for
large owned buffers is Phase 5, behind an explicit API.

### D5 — External registration: `RegionGuard` RAII, tokio-uring drop policy.

`Velo::register_external_memory` returns a guard the caller must hold:

- `is_shutting_down()` / `shutdown_initiated().await` — velo shutdown observer.
- `deregistered().await` — resolves only when velo has fully quiesced and
  `ucp_mem_unmap` returned; then and only then may the caller free the memory.
- `unregister(timeout).await` — caller-initiated: gate new leases → drain
  in-flight ops/leases → flush → unmap. *As built it returns
  `Result<Deregistered, RdmaError>` where **both** `Ok` variants mean released:
  `Deregistered::DrainTimedOut` says "released without waiting for in-flight
  work" (`lib/velo/src/rendezvous/rdma/region.rs:245-254, 526`). The plan's
  plain success/failure shape is the bug that distinction was added to close —
  an `Err(Timeout)` conflates "still mapped" with "unmapped early", and only
  the first means the caller must not free.*
- `Drop` without awaiting = **background deregistration + `tracing::warn!`**.
  Never block in Drop (panics in a runtime), never abort. Memory stays mapped
  until the async dereg completes, so dropping early is a liveness bug for the
  caller, not UB for anyone.
- *Not planned, and shipped: `RegionGuard::watch() -> RegionWatch`
  (`region.rs:493, 628`), a clonable observational handle carrying the same
  observers. It exists because `unregister` consumes the guard, so without it a
  task that only wants to know when the memory was released would have to own
  the release.*

Ownership: the guard does **not** borrow. Constructors:
`register_external_memory(ptr: NonNull<u8>, len)` is `unsafe` with a
documented contract (caller keeps the allocation alive and un-freed until
`deregistered()` resolves — which is exactly the contract Ryan described), plus
a safe `register_owned` variant where velo takes ownership and hands the buffer
back on deregistration. *As built that variant is
`Velo::register_owned(Box<[u8]>) -> Result<RegionGuard, RegisterOwnedError>`
(`lib/velo/src/lib.rs:1213-1216`) — a distinct error type carrying the buffer
back on failure, not `RdmaError`, so a rejected registration does not eat the
allocation. Recovery on the success path is
`RegionGuard::unregister_owned(timeout) -> Result<(Box<[u8]>, Deregistered),
RdmaError>` (`region.rs:549-552`).* A `&mut [u8]`-shaped
API would be an aliasing lie: UCP's `prot` field is dead code, so every
RMA-registered region is remotely *writable* by any rkey holder regardless of
our GET-only protocol. Documented as a trust-domain assumption.

Per-region drain uses `velo_ext::ShutdownState`/`InFlightGuard` verbatim (the
SeqCst + register-notified-first discipline was paid for twice already; do not
hand-roll). Note: `velo::sync` (`PendingMap`/`CloseSignal`) is **not on this
branch or main** — it lives only on an unmerged branch; nothing here depends
on it.

Anchoring data in an external region: `register_data_in_region(&guard, range)
-> DataHandle` creates a pinned slot referencing the region, zero-copy. The
slot holds a region in-flight guard, so `unregister` naturally waits for
outstanding leases on anchors inside the region.

### D6 — Eligibility is consumer-advertised, owner-decided; fully skew-safe.

New `#[serde(default)]` field on `RvAcquireRequest`:
`rdma: Option<RdmaOffer>`. "Transport key + protocol version" is this
paragraph's own text failing to track D12, which replaced it: as built
`RdmaOffer { backends: Vec<String> }` is a preference-ordered list of backend
names and nothing else (`lib/velo/src/rendezvous/protocol.rs:90-95`). **There
is no protocol-version field on the offer**, so a future descriptor-format
change has no negotiation channel — see the Phase-5 addendum in §2. serde_json
ignores unknown fields and defaults missing ones, so: old consumer → no field
→ owner never sends `Rdma` (old consumers keep working); old owner → ignores
the field → replies `Ready` (new consumers fall back transparently). No hello
handshake change, no capability registry, no symmetric-priority assumption:
the GET rides the consumer's own UCX EP to the owner, so the only conditions
are "consumer has the ucx transport with the owner registered on it" — a
purely local check via `messenger.backend()` (pub(crate), same crate). UCX
does **not** need to be the primary transport; control plane can stay on TCP.

Failure fallback: if the GET errors, the consumer detaches the lease and
re-acquires with `rdma: None`. Pinned slots remain chunked-readable (the arena
slice is host memory; `get_chunk` serves it), so a pinned slot never strands a
non-RDMA consumer — PR #40's hard bifurcation is explicitly avoided.

A runtime kill switch (config + env var) forces `rdma: None` everywhere —
production rollback without a rebuild.

### D7 — Descriptor: velo-owned, versioned, length-framed binary.

`ucp_ep_rkey_unpack` parses with **no length bound** (public API passes
`length=0`; for `dst_version > 19` peers it reads past any bound) — a
truncated/corrupt blob is an OOB read inside UCX that no Rust wrapper can make
safe. Therefore `AcquireResponse::Rdma.descriptor` gets an explicit layout,
owned by velo:

```text
backend: u8 | version: u8 | flags: u8 | generation: u64 | addr: u64
| len: u64 | rkey_len: u16 | rkey: [u8; rkey_len]
```

(The leading `backend` discriminator comes from D12; `1 = ucx`.)

Encoding is packed little-endian with no padding or alignment: a 29-byte
header (`1+1+1+8+8+8+2`) followed by exactly `rkey_len` key bytes, so
`len(descriptor) == 29 + rkey_len` and a decoder that finds any trailing byte
rejects. Both the encoder and the parser apply the same bound — the descriptor
refuses `rkey_len > MAX_KEY_LEN` (4096) — and the UCX backend bounds its keys
more tightly still at `MAX_PACKED_RKEY` (1024), checked at map time and again
before `ucp_ep_rkey_unpack`, so an oversized key fails registration and never
reaches the wire.

The consumer rejects any mismatch between `rkey_len`, the actual remaining
bytes, and sanity bounds *before* the pointer reaches UCX. `generation` is the
owner's region generation (bumped on arena/region reuse), carried in the
descriptor for diagnostics there
(`lib/velo/src/rendezvous/descriptor.rs:139-141`), and future-proofs a Phase-5
rkey cache. **It is not echoed back**: `RvDetachRequest` and `RvReleaseRequest`
carry `handle` + `lease_id` only
(`lib/velo/src/rendezvous/protocol.rs:171-183`). The echo was never built and
nothing decided against it — this plan simply described a field that does not
exist. No foreign types (no `MemType` enums from FFI crates) on the wire.
The same struct serves the Phase-5 PUT reversal with a direction flag —
one descriptor primitive, two directions (the MPICH `prepare_rdma_info`
shape; every surveyed system converged on it). **This forward-compatibility
promise did not survive contact with the decoder Phase 3 built** — see the
addendum on Phase 5's first bullet in §2.

### D8 — Shutdown: rendezvous pinned-state drains *before* transport teardown.

The sharpest critique finding: an RDMA GET is invisible to the owner's
`ShutdownState.in_flight` (it's issued by the consumer's NIC), so today's
graceful shutdown could `ucp_mem_unmap` under a peer's in-flight GET. Fix, in
order, inside `Velo::graceful_shutdown`:

1. **Gate**: pinned staging refuses new RDMA acquires (acquires answer
   chunked or error); new registrations refused.
2. **Drain**: bounded wait for outstanding *pinned leases* (leases taken via
   an RDMA acquire hold a registration-layer in-flight guard released on
   detach/release). Backstop: every RDMA lease carries a deadline
   (`lease_timeout`, default 30 s, echoed in the acquire response); an
   owner-side reaper force-releases expired leases — this is also the answer
   to "consumer crashed mid-transfer" (PR #40's compounding leak). A
   fire-and-forget keepalive AM (`_rv_lease_renew { handle, lease_id }`)
   extends the deadline; consumers renew at `deadline/2` while a transfer or
   long hold is live, so the timeout can stay tight without penalizing slow
   links. Renewal loss is benign (the standing deadline applies).
3. **Deregister**: arenas + external regions unmapped on the progress thread
   (regions strictly before EP close — rkeys/regions before endpoints is
   UCX's contract).
4. Existing transport teardown proceeds unchanged.

Timeout behavior: warn + force-unmap. On IB a straggler GET then gets a
remote-access error (its problem, correctly); over tcp it's silent — accepted,
documented.

### D9 — Caches and eviction: byte budgets, not inactivity timers, for memory.

Challenging the prompt's framing deliberately: the survey found **no system
that evicts memory registrations on an inactivity timer** (UCX rcache,
libfabric, MPICH, NCCL all use LRU under byte/count ceilings; timers appear
only for staged *data* TTLs). Timer-evicting a registration a peer still holds
a descriptor for is precisely the QP-killing stale-rkey hazard. So:

- **Registration pressure** = configurable registered-bytes budget
  (`RLIMIT_MEMLOCK`-aware). Over budget → new pinned stagings fall back to
  chunked; empty arenas above the low-water mark are unmapped (idle *empty*
  arenas may use a timer — nothing references them, so it's safe).
- **EP inactivity** = where the timer legitimately lives. A progress-thread
  tick closes EPs with `now - last_used > idle_timeout && no in-flight ops`,
  reusing the existing parked-close machinery (`close_parked` /
  `pending_closes`). **Off by default** (decided at sign-off); connection-pool
  policy revisited later. No cache crate: UCX EPs are progress-thread-owned and
  need flush-close on that thread; a reaper over the existing
  `HashMap<InstanceId, EpEntry>` is the whole design. (Verified: UCX has no
  internal EP eviction; keepalive is failure detection only.)
- If Phase 5 adds register-in-place per-buffer registrations, that cache uses
  `quick_cache` (`Weighter` in bytes/u64, `Lifecycle::is_pinned` = has
  in-flight ops, mark→drain→destroy two-phase evict). moka is disqualified:
  since 0.12 it has no background threads, so TTI/eviction listeners only fire
  when someone touches the cache — an idle process never evicts.

### D10 — Everything stays runtime-internal. Zero `velo-ext` changes.

Rendezvous and the UCX transport are the same crate. Wiring: the builder path
that adds a `UcxTransport` keeps the concrete `Arc<UcxTransport>` (feature-
gated) and hands `RendezvousManager` an `RdmaEndpoint` handle exposing
`pub(crate)` operations (map/unmap/get/ensure-ep as ring commands with oneshot
completions). No `RdmaCapability` trait in `velo-ext`, no `as_any`, no version
bump, no external-implementor impact. Revisit only if an out-of-tree transport
ever needs RDMA (then the `set_observability`-shaped default-impl accessor is
the pattern, priced at a coordinated minor bump).

The `velo-ext` half held exactly as written — no trait change, no bump. What
this decision does **not** say, and a reader takes from it wrongly, is that
`velo`'s own public surface stayed as small. It did not. Publicly exported
today (`lib/velo/src/lib.rs:69-76`, all under the same
`cfg(all(target_os = "linux", feature = "ucx"))` gate as the transport):
`Deregistered`, `PinnedBuf`, `RdmaConfig`, `RdmaError`, `RdmaPoolConfig`,
`RdmaRendezvousConfig`, `RegionGuard`, `RegionWatch`, `RegisterOwnedError`,
`PinnedWriter` — of which this plan names only `RegionGuard` and `PinnedBuf`.
Also public and unplanned: `RendezvousManager::alloc_pinned_writer`
(`lib/velo/src/rendezvous.rs:853`); `RendezvousWrite::rdma_destination` with
the `RdmaDestination<'a>` type it returns
(`lib/velo/src/rendezvous/write.rs:49, 64`), which *is* Phase 3's "new
`RendezvousWrite` capability method, defaulted" but whose borrow-plus-
`TransferHold` shape — the thing that keeps a cancelled `get_into` from
returning granules the NIC is still writing into — this plan gives no hint of;
`StageMode` (`lib/velo/src/rendezvous/store.rs:55`), derived from `SlotBody`
rather than stored; and `RdmaTestHook` / `arm_rdma_hook`
(`rendezvous.rs:138, 640`), correctly gated behind `test-helpers`. `RdmaError`
is `#[non_exhaustive]`, which is what keeps Phase 5's lease, descriptor and
direction failures from being a breaking change.

RMA submission bypasses `send_message`/`AdmissionGate` (those are AM-frame
semantics — eager caps, drain rejection, `SendOutcome`): commands go to the
ring via `send_async` (bounded = natural backpressure), completions resolve
oneshots directly, and **no RMA completion callback ever enqueues onto the
ring** (self-deadlock class — the ring's own docs warn about it).

### D11 — Size thresholds, one table (all tunable via config)

| Knob | Set via | Default | Meaning |
|---|---|---|---|
| `transparent::DEFAULT_THRESHOLD` | compile-time constant | 256 KiB | messenger payload → staged via rendezvous |
| `DEFAULT_CHUNK_SIZE` | compile-time constant | 512 KiB | chunked-pull chunk size |
| `eager_max` | `UcxConfig` builder (velo's own cap, not an OpenUCX setting) | 1 MiB | AM frame cap (unrelated to the RDMA path) |
| `rdma_min_bytes` (new) | `RdmaRendezvousConfig` | 64 KiB | below this, pinned slots still answer chunked |
| `dedicated_arena_min` (new) | `RdmaPoolConfig` | 64 MiB | staging above this gets its own arena |
| `registered_bytes_budget` (new) | `RdmaPoolConfig` | 1 GiB | pool + external total; over → chunked fallback |
| `lease_timeout` (new) | `RdmaRendezvousConfig` | 30 s | RDMA lease deadline (reaper backstop) |

All seven defaults above still hold as built, verified one by one against
`transparent.rs:32`, `store.rs:48`, `transports/ucx/transport.rs:85`,
`rdma/mod.rs:157`, `arena.rs:118`, `arena.rs:119` and `rdma/mod.rs:158`. What
stopped holding is the claim that this is *one* table of the tunables — it is
now incomplete, which makes it misleading rather than merely dated. The knobs
it does not list:

| Knob | Set via | Default | Anchor |
|---|---|---|---|
| `initial_arena_bytes` | `RdmaPoolConfig` | 64 MiB | `arena.rs:85`, default `:116` |
| `max_arena_bytes` | `RdmaPoolConfig` | 1 GiB | `arena.rs:87`, default `:117` |
| `shutdown_timeout` | `RdmaConfig` | 30 s | `rdma/mod.rs:72`, default `:83` |
| `drop_dereg_timeout` | `RdmaConfig` | 30 s | `rdma/mod.rs:75`, default `:84`, used `region.rs:595` |
| `MIN_LEASE_TIMEOUT` | constant clamp, not configurable | 1 ms | `rendezvous.rs:1049, 1078-1089` |
| `arena_reclaim_after` | `RdmaPoolConfig` | `None` (off) | Phase 4, PR #69 — not on main |
| `retain_arena_bytes` | `RdmaPoolConfig` | 64 MiB low-water | Phase 4, PR #69 — not on main |
| `ep_idle_timeout` | `UcxConfig` / builder | `None`, floored | Phase 4, PR #69 — not on main |
| `eager_endpoints` | `UcxConfig` / builder | `false` | Phase 4, PR #69 — not on main |

`MIN_LEASE_TIMEOUT` is a clamp rather than a knob because sub-millisecond
encodes as `0` on the wire, which the protocol already spells *no deadline*: a
lease configured at 100 µs would otherwise become immortal.

`eager_endpoints` is worth calling out as unplanned. It exists because the
hardware checkpoint measured a ~14 ms one-time lazy-wireup cost against a
~108 µs warm GET — a lever this plan did not know about and did not price.

**Kill switch, which D6 promises and never names:** the environment variable is
`VELO_RDMA_RENDEZVOUS_DISABLE`, read once at `VeloBuilder::build` with
affirmative-only parsing (`lib/velo/src/lib.rs:478-489`), forcing
`RdmaRendezvousConfig::enabled` off whatever the config says
(`rdma/mod.rs:101-103`). Read once and centrally so one process cannot answer
half its acquires one way and half the other.

Ordering invariant: `rdma_min ≤ transparent_threshold < chunk_size` makes a
transparently-staged payload *size*-eligible for RDMA from its first byte over
the threshold. Size eligibility is necessary, not sufficient — the budget
(`BudgetExceeded`), the consumer/owner eligibility checks, and either kill
switch can still answer chunked.

### D12 — Backend-pluggable by construction (sign-off amendment)

A later NIXL, libfabric, or other RDMA implementation must slot in without
reshaping the registration layer or the wire protocol. Concretely:

- **Wire**: the descriptor (D7) leads with a backend discriminator
  (`backend: u8` registry: `1 = ucx`) and the key material is a
  backend-opaque blob. The consumer's `RdmaOffer` in `_rv_acquire` names the
  backends it can consume (v1: `["ucx"]`); the owner picks the first it can
  serve. A NIXL or libfabric path is a new discriminator, not a new protocol.
- **Runtime**: the registration layer (`ArenaSet`, `RegionGuard`, pinned
  staging) programs against a **velo-internal** trait — roughly
  `trait RdmaBackend { key(); map(ptr, len) -> BackendRegion; unmap(..);
  get(peer, descriptor, local) -> oneshot; }` — defined in
  `velo::rendezvous::rdma`, implemented by the UCX transport's
  `RdmaEndpoint` (Phase 1 mechanics). One backend per instance in v1;
  the trait exists so the second implementation is additive.
- **Boundary discipline unchanged**: the trait is `pub(crate)` and stays out
  of `velo-ext` (D10 holds). No backend-specific types (UCX handles, NIXL
  MemTypes) escape their module or reach the wire — the exact mistake PR #40
  made with `velo_nixl::MemType` in the wire format.
- What v1 explicitly does *not* abstract: progress-model differences (UCX's
  single progress thread vs NIXL's polling) live entirely behind the trait's
  async surface; if a future backend needs a different completion model, the
  oneshot-based contract already accommodates it.

---

## 2. What ships where — phases

Workflow per phase: implement (Sonnet workhorses, Opus for the gnarly unsafe/
FFI parts) → `/thermonuclear-review` (new skill, already drafted at
`.claude/skills/thermonuclear-review/SKILL.md`) → fix confirmed findings →
re-review touched dimensions → PR. Fable does synthesis/judgment only.
Each PR lands green on the standard gates (fmt, clippy `-D warnings`, machete,
semver, `--all-features` tests).

### Phase 1 — RMA plumbing in the UCX transport (PR 1)

The unsafe core, kept small and reviewed hardest.

- `Cmd` extensions on the progress thread: `MapRegion` (ucp_mem_map +
  `ucp_rkey_pack` + `ucp_mem_query` for the effective range), `UnmapRegion`,
  `RmaGet { peer, remote_addr, packed_rkey, local (arena memh + offset), reply:
  oneshot }`, internal ensure-EP reuse. `ucp_rkey_pack` (not `memh_pack`) —
  the deprecated-but-working one; `ucx-rs` measured `memh_pack` aborting via
  `ucs_fatal`. A canary test asserts pack succeeds so a future UCX bump fails
  loudly.
- GET posts with `UCP_OP_ATTR_FIELD_MEMH | FIELD_CALLBACK | FIELD_USER_DATA |
  FLAG_NO_IMM_CMPL` — NO_IMM_CMPL collapses the three-exit reclaim discipline
  to one path for RMA. Completion-owned `RmaOpState` (rides `inflight_ops`),
  rkey unpacked immediately before post and destroyed at completion on the
  progress thread, with an explicit ordering note: completion (including
  CANCELED from FORCE close) destroys the rkey before the EP entry is freed.
- `RdmaEndpoint` handle (`pub(crate)`): async map/unmap/get with oneshot
  results; teardown drains RMA ops with the existing bounded discipline.
- Tests: two in-process workers over `UCX_TLS=tcp` — map on A, GET from B,
  data integrity, zero-length no-op, GET-after-unmap (documented tcp
  limitation), teardown with in-flight GET, `md_map` empirical probe (assert
  the packed-rkey size CI actually produces, settling the degenerate-format
  question). Micro-bench harness printing `mem_map` cost vs size + GET latency
  (numbers feed Phase-3 threshold defaults; hardware numbers at the
  checkpoint).

### Phase 2 — Registration layer: pool + external RAII (PR 2)

New module `velo::rendezvous::rdma` (gated `#[cfg(all(target_os = "linux",
feature = "ucx"))]`, exactly matching the transport's gate).

- `ArenaSet`/`Arena`/`PinnedBuf` (pool suballoc per D4), `RegionGuard` +
  external registration APIs (D5), registered-bytes budget, per-region
  `ShutdownState` drain, generation counters.
- `Velo::graceful_shutdown` ordering change (D8 steps 1–3 wired in before
  transport teardown).
- Tests: alloc/free/fragmentation property tests, guard drop-without-await
  (warn + background dereg), `deregistered()` latching, shutdown-ordering
  (region unmap strictly before EP close — assert via instrumented ordering),
  budget exhaustion behavior, concurrent register/unregister/shutdown races
  under `loom`-style stress where practical. *"Where practical" resolved to
  "not": no permutation-checking harness covers these races, which are pinned
  by ordinary concurrency tests instead. (The `loom-rs` in the dependency tree
  is a different thing — a simulation runtime behind the `simulation` feature,
  used by `velo::simulation` and not by the registration layer.) A
  substitution, not a gap, but this bullet should not be read as a promise of
  `loom`.*

### Phase 3 — Protocol: the GET fast path end-to-end (PR 3)

- Wire: `RvAcquireRequest.rdma` offer field (backend list, D12), descriptor
  type + framing (D7, backend-discriminated), `AcquireResponse::Rdma`
  production, lease deadlines + owner-side reaper + `_rv_lease_renew`
  keepalive AM (D8).
- Owner APIs: `register_data_pinned(&[u8])` (stage-copy into pool),
  `register_data_in_region(&guard, range)` (zero-copy external),
  `StageMode::Pinned` becomes real; `DataSlot` body refactored to
  `enum SlotBody { InMemory(Bytes), Pinned(PinnedSlice) }` (kills PR #40's
  three-fields-one-fact bug class); chunked fallback serves pinned slots.
- Consumer APIs: `get()` (copy-out convenience), `get_pinned()` →
  zero-copy `PinnedBuf`, `get_into` fast path when the destination is
  registered (new `RendezvousWrite` capability method, defaulted); transparent
  large-payload mode picks the fast path automatically.
- Config + kill switch (D6), metrics (`rdma_registered_bytes`,
  `rdma_registrations_total`, path-selection counter with fallback reason —
  the one that proves RDMA is actually chosen — GET latency histogram,
  arena-utilization gauge), stale-comment cleanup (`rendezvous.rs:18-21`,
  `write.rs:17-18`, test comments).

  Metrics as shipped, six registered at `lib/velo/src/observability.rs:1391,
  1400, 1410, 1419, 1430, 1441`: `velo_rdma_registered_bytes`,
  `velo_rdma_registrations_total`, `velo_rdma_live_regions` (unplanned),
  `velo_rendezvous_rdma_path_total` (ten labelled fallback reasons, more than
  this bullet asked for), `velo_rendezvous_rdma_get_duration_seconds` and
  `velo_rendezvous_rdma_leases_reaped_total` (unplanned). **The
  arena-utilization gauge is the one item of this bullet that is not built** —
  there is no such series anywhere in `observability.rs`. It is a Phase-3
  deliverable and it is still owed.
- Tests: full matrix over tcp — pinned↔chunked consumer/owner cross-product,
  old-wire simulation (requests without the offer field), GET-failure
  fallback, lease-reaper, kill switch, two-process example
  (`rendezvous_rdma_two_proc.rs`, porting PR #40's launcher harness — its one
  unambiguously good artifact).
- Deliverables as shipped: the two-process example is
  `examples/examples/rendezvous_rdma_two_proc.rs`
  (`examples/Cargo.toml:56`); the stale-comment cleanup is done
  (`lib/velo/src/rendezvous.rs:17-28` now describes both paths,
  `lib/velo/src/rendezvous/write.rs:4-8` names `PinnedWriter`); the `md_map`
  probe is `rkey_pack_canary` (`lib/velo/src/transports/ucx/tests.rs:799`); the
  micro-bench harness is `bench_rma`, `#[ignore]`d and printing tcp numbers
  only (`tests.rs:1691-1693`).

**Checkpoint after Phase 3: hardware validation** on a real IB cluster
(compute-session MCP; `UCX_TLS=rc_mlx5,ud_mlx5`): correctness matrix +
latency/bandwidth vs chunked baseline + registration-cost numbers. Threshold
defaults revisited with data. Not a PR; a report.

**Addendum (2026-08-29): the checkpoint ran, and passed. Report:
`agent-docs/2026-08-29-rdma-phase3-hardware-checkpoint.md`.** computelab
lego-c2, ConnectX-7 NDR, two `--exclusive` nodes on one InfiniBand fabric. The
correctness matrix is byte-verified 50/50 and all 23 `rendezvous_rdma`
integration tests pass over real InfiniBand.

It could **not** run on the lane this paragraph prescribes. Velo's vendored UCX
exposed no mlx5-accelerated transports, and `UCX_TLS=rc_mlx5,ud_mlx5` degrades
to chunked *silently* — the setting this plan named is the setting that hides
the problem. The run therefore used `rc_verbs,ud_verbs,self`, and **every
latency, bandwidth and rkey-size number in the report is an unaccelerated
floor**. The root cause was proven off-hardware afterwards:
`crates/ucx-rs/build.rs:350-351` emits `static=uct_ib_mlx5` before
`static=uct_ib`, so ELF constructor order runs `uct_mlx5_init` first, both
constructors prepend to `uct_ib_ops`, and the verbs memory domain ends up at
the head — where it opens unconditionally and every mlx5 NIC comes up
unaccelerated. The fix is open as PR #70 (`ai/ucx-rs-mlx5-link-order`); until
it lands, `UCX_IB_MLX5_DEVX=y` in the environment is a working runtime
workaround.

The consequence for this plan is specific, and it is not "re-run for tidiness":
**`rdma_min_bytes = 64 KiB` must be re-derived on the accelerated lane before
it is trusted**, along with the ~14 ms wireup finding and the two findings in
§3's ledger below. An unaccelerated floor is a lower bound on what the hardware
does, not a measurement of it, and a threshold derived from a floor can only be
conservative by accident.

### Phase 4 — Lifecycle pressure: EP reaper + pool reclamation (PR 4)

- EP idle reaper on the progress thread (D9), config
  (`ep_idle_timeout`, default off or generous — decided with Ryan at
  checkpoint), metrics (`eps_closed_idle_total`).
- Empty-arena reclamation under the byte budget; low-water retention.
- Shutdown/teardown interaction tests; soak test (register/transfer/release
  loop asserting stable registered-bytes and EP counts).

Status: written, open as PR #69 (`ai/rdma-phase4-lifecycle`), **not on main**.
Nothing in this section can be checked against the tree yet, which is why the
D11 rows for its four knobs carry a PR reference rather than a file anchor.
Five things about it are worth recording here rather than in the PR:

- `eager_endpoints` is a Phase-4 addition this plan never anticipated, added
  because of the checkpoint's ~14 ms lazy-wireup finding (report §5).
- The metric this bullet names, `eps_closed_idle_total`, is **not** shipped as
  a Prometheus series. The count exists as an internal atomic reachable from
  tests. The soak deliverable is likewise not met as written: what PR #69
  carries is two partial soaks — registered-bytes stability against the mock
  backend, and endpoint-count stability over UCX — with no single loop
  asserting both axes together. Both are open items, not closed ones.
- `ep_idle_timeout`'s "decided with Ryan at checkpoint" (first deliverable
  above) was not discharged at the checkpoint itself, but **it has since been
  decided: 2026-09-01, ship as-is.** The reaper stays off by default (`None`),
  documented as experimental, with its measured cost stated where an operator
  will read it before enabling: reaping an endpoint whose peer still holds its
  side costs that peer one silently-lost frame plus up to a UCX keepalive
  interval (~20 s) before it self-heals. Two alternatives were offered and not
  taken — gating the reaper behind `test-helpers`, or dropping it from PR #69.
  The reasoning for shipping it: the cost is real but bounded, self-healing and
  reachable only by opting in, and Phase 5's pre-close "goodbye" AM would
  remove it entirely rather than being designed around.
- The knob keeps a 500 ms floor on any value a builder does set, sized to
  dominate the measured ~14 ms wireup. That wireup finding is also why the
  answer went toward eager wireup rather than toward a number for the idle
  timeout, and why `eager_endpoints` exists at all.
- D9's "connection-pool policy revisited later" is narrower than the reaper
  decision and remains open: the measurement it waited for now exists, but the
  broader pooling question it feeds has not been answered.

### Phase 5 — Role reversal (PUT) + measured optimizations (PR 5, scope-gated)

- Consumer-supplied descriptor in the acquire request ("PUT into this"):
  owner PUTs + `ucp_ep_flush_nbx` + completion AM (`_rv_put_done`), the flush
  carrying an explicit timeout (tcp SW-RMA flush needs the peer progressing).
  Same descriptor struct, direction flag (D7).

  **Addendum (2026-09-01): the direction flag is not additively deployable, as
  D7 assumed it would be.** D7 promised "one descriptor primitive, two
  directions". The decoder Phase 3 built refuses *any* non-zero `flags` byte
  outright (`lib/velo/src/rendezvous/descriptor.rs:225-227`,
  `DescriptorError::UnknownFlags`). That refusal is correct and should stay — a
  flag exists to change how the rest of the blob is read, so a decoder that
  ignores one it does not know reads the wrong thing. The consequence is that
  every Phase-3 and Phase-4 peer rejects a flagged descriptor and falls back to
  chunked: benign, and total, for the whole mixed-version window. Bumping
  `DESCRIPTOR_VERSION` (`descriptor.rs:68`) instead does not help, because
  there is nowhere to negotiate it — `RdmaOffer` carries backend names and no
  version (`lib/velo/src/rendezvous/protocol.rs:90-95`), so a version field
  would itself have to be added to the offer first, under the
  `#[serde(default)]` discipline, and shipped a release ahead. **Phase 5 must
  either budget for that negotiation step or accept universal chunked fallback
  in the mixed window — deliberately, and stated in its plan.**

  Additive surface Phase 5 needs and does not have today: a consumer-descriptor
  field on `RvAcquireRequest` (`protocol.rs:98-110`, `#[serde(default)]`); a
  `_rv_put_done` handler (the registered set is `_rv_metadata`, `_rv_acquire`,
  `_rv_pull`, `_rv_ref`, `_rv_detach`, `_rv_release`, `_rv_lease_renew`); and
  `put`/`flush` on the internal `RdmaBackend` trait, whose transfer surface is
  `get` alone (`lib/velo/src/rendezvous/rdma/backend.rs:177-223`).
- Owner-side TTL for *chunked* leases. Phase 3 gave RDMA leases a deadline and
  a reaper because an RDMA GET is invisible to the owner; chunked leases kept
  their existing no-deadline behaviour deliberately, as scope discipline. The
  gap that leaves: a consumer's `LeaseGuard` releases a chunked lease on every
  error path except one — a spawn that cannot land because the runtime is being
  torn down — and nothing on the owner reclaims it. Narrow, but the only lease
  class with no backstop at all.
- Only if the checkpoint numbers justify them: consumer-side unpacked-rkey
  cache (D3's deferral), register-in-place for large owned buffers
  (quick_cache design from D9).

Phases 1→2→3 are strictly sequential (each builds on the previous PR landing
on main).

**Phase 4 was predicted to be independent of 3 and parallelizable after 2. As
built it is not.** Its arena reclamation runs on the rendezvous lease reaper's
tick — Phase-3 machinery, started by `set_rdma_context`
(`lib/velo/src/rendezvous.rs:525`) with a period derived from `lease_timeout /
2` (`rendezvous.rs:550`) and stopped ahead of the registration sweep
(`rendezvous.rs:116-120, 574`). Reusing it was the right call: it is the one
periodic task the subsystem already has, and a second timer would have to be
ordered against the first anyway. But it means the stack is 1→2→3→4, which is
what the #66→#67→#68→#69 stack reflected. Nobody parallelized, so nothing is
lost by saying so plainly.

**Phase 5 waited for the checkpoint; the checkpoint has run.** What gates it
now is not the checkpoint but the mlx5 link-order fix (PR #70) — the two
optimizations at the end of Phase 5 are explicitly conditioned on measurements,
and the accelerated lane those measurements have to come from does not exist
yet. The gate moved; it did not disappear.

---

## 3. Test-fidelity ledger (what tcp CI cannot see)

Honest accounting, per the critique:

| Not testable over `UCX_TLS=tcp` | Mitigation |
|---|---|
| Stale-rkey → remote access error → **process abort** | Design excludes stale rkeys (single-use + acquire-time revalidation). Exercised on IB 2026-08-29 — resolved worse than priced; see note 1 |
| SW RMA validates nothing (bad addr = silent corruption) | Owner-authored addresses only (descriptor comes from the owner's own table); consumer never computes remote addresses; debug asserts on ranges |
| Multi-MD rkey serialization | **Settled.** `md_map` probe documents what CI covers (9 B over tcp: header only, the tcp MD registers nothing); measured 20 B on IB. The probe's `>= 9` floor stays — see note 2 |
| Registration cost realism | Bench harness both places; **thresholds re-derived 2026-08-29, verdict: no change** — see note 3 |

**Note 1 — row 1 resolved worse than D3 priced it.** D3 predicted that a stale
rkey on RC means `IBV_WC_REM_ACCESS_ERR` and "the whole QP dies". On
rc_verbs/CX-7 the observed outcome is `ucs_fatal` → `SIGABRT`: **the process
dies, not the QP**, with no Rust-level error to fall back from. Over tcp the
same blob is refused inside `ucp_ep_rkey_unpack` and comes back as an ordinary
error, so **a tcp-only CI can never see this class at all**. It stays out of
reach today because of D3's two legs — single-use rkeys and acquire-time
revalidation — and *not* because of the syntactic pre-parse
(`lib/velo/src/transports/ucx/rma.rs:551`), which contains framing, not
staleness: a semantically stale but syntactically perfect rkey passes it
identically. Two things on the table would hole that invariant. The Phase-5
rkey cache (D3's deferral) makes an rkey outlive its op by construction, and
Phase 4's arena reclamation rests on the argument that an empty arena is
referenced by nothing — an argument a cached rkey breaks. **Scoping the rkey
cache as "just an optimization, gated on a profile" is mispricing it.**

**Note 2 — row 3, and why the canary bound is not tightened.** The 20 B is a
*verbs-MD* measurement, taken on the build where the mlx5 memory domain never
opened. An independent probe on a DEVX MD measured 19 B on the same UCX 1.22
(`docs/proposals/ibverbs-transport.md:919-920`). Raising `rkey_pack_canary`'s
floor from `>= 9` to `== 20` would pin a number this build produces only
*because of* the link-order defect. Leave it, re-measure after PR #70, and
re-check `preparse_packed_rkey`'s assumptions against whatever comes back.

**Note 3 — row 4: keep `rdma_min_bytes` at 64 KiB, but not for the reason we
expected.** On a warm peer pair RDMA beat chunked at every size down to 4 KiB,
so there is no crossover above 4 KiB on this fabric and 64 KiB is conservative
— at a cost of roughly 40–70 µs per transfer in the 4–64 KiB band, which is not
worth the pinned-memory and lease pressure. Registration itself is cheap
(43–266 µs across 4 KiB–64 MiB) and is not the bottleneck; the ~105 µs acquire
round-trip dominates. **The threshold turned out not to be the lever.** The
lever is a ~14 ms one-time lazy-endpoint wireup on a fresh peer pair against a
108–229 µs warm GET, which is why Phase 4 added `eager_endpoints`. Two caveats
the numbers carry: the chunked arm ran under
`VELO_RDMA_RENDEZVOUS_DISABLE=1`, so staging mode co-varies with path — this
answers "enable RDMA at all", not a clean per-slot sweep; and the cold
crossover is bracketed only between 16 MiB and 256 MiB, cells 16× apart, so do
not interpolate a point estimate from it. D11's ordering invariant is
untouched. **All of it is an rc_verbs floor and owes a re-derivation on the
accelerated lane** — see the checkpoint addendum in §2.

## 4. Explicitly out of scope (this milestone)

- GPU/VRAM memory (design keeps `mem_type` out of the wire format on purpose;
  the descriptor gains a field when device memory arrives).
- Cross-version wire guarantees beyond the `#[serde(default)]` discipline —
  pre-1.0 velo assumes compatible-deploy windows; written down here so it
  isn't re-litigated.
- `velo-ext` RDMA surface for out-of-tree transports.
- Multi-blob (scatter) anchors — `DataHandle` stays one contiguous blob; a
  blob-list layer composes above it later.

## 5. Sign-off record (2026-08-25)

1. External-registration constructor: **`unsafe fn` + ptr/len** as primary,
   safe owned-buffer variant alongside.
2. Lease reaper: **force-release at deadline**, plus the `_rv_lease_renew`
   keepalive AM to extend it (D8).
3. EP idle close: **off by default**; connection-pool policy revisited later.
4. Hardware checkpoint: implementation **pauses at the Phase-3 validation
   target** and hands Ryan restart instructions (cluster session spin-up on
   a compute cluster) unless a session-reachable IB allocation exists at that
   point. **Done 2026-08-29** —
   `agent-docs/2026-08-29-rdma-phase3-hardware-checkpoint.md`. One item on the
   checkpoint's own list, `abandon_rma_ops` under a SIGSTOP'd owner, was not
   attempted and is still owed to a second cluster session.
5. Backend pluggability (NIXL / libfabric later) added as **D12**.
