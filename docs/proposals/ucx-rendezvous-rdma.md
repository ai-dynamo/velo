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
(`transports/ucx/worker.rs:569`) with a comment reserving it for exactly this.
The two in-tree "Phase 2" comments disagree (`rendezvous.rs:18-21` says NIXL,
`worker.rs:568-571` says native GET); this plan resolves them to native, and
Phase 3 updates the stale doc comment.

*Alternative considered — accept UCX's own rendezvous AMs* (drop the
`UCP_AM_RECV_ATTR_FLAG_RNDV` rejection and let UCX's internal RTS/RTR/GET
machinery move large AM payloads): rejected. It accelerates *push* messaging,
not the pull-by-handle model rendezvous is for, gives no external-memory
registration story, and reopens the descriptor-ownership/shutdown problem the
AM receive path deliberately closed (`worker.rs:295-324`).

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
`AcquireResponse::Rdma { lease_id, descriptor }` iff the slot is pinned AND the
request advertised capability, else `Ready{...}` (chunked). Consumer issues
`ucp_get_nbx`, then `_rv_detach`/`_rv_release` as today. The acquire round-trip
is the linchpin: the owner revalidates the registration on every transfer, so
**no cross-node invalidation protocol is needed for slot memory**.

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
  each arena = one `mmap` + one `ucp_mem_map` + one packed rkey (`Bytes`).
- Suballocation: `offset-allocator` (MIT, zero unsafe, O(1) alloc+free) behind
  `parking_lot::Mutex`, allocating in **4 KiB granules** (u32 range → 16 TiB
  per arena; float-bin waste ≤12.5%). `with_max_allocs` explicitly sized (the
  default eagerly allocates ~4 MB of node metadata per arena).
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
  in-flight ops/leases → flush → unmap.
- `Drop` without awaiting = **background deregistration + `tracing::warn!`**.
  Never block in Drop (panics in a runtime), never abort. Memory stays mapped
  until the async dereg completes, so dropping early is a liveness bug for the
  caller, not UB for anyone.

Ownership: the guard does **not** borrow. Constructors:
`register_external_memory(ptr: NonNull<u8>, len)` is `unsafe` with a
documented contract (caller keeps the allocation alive and un-freed until
`deregistered()` resolves — which is exactly the contract Ryan described), plus
a safe `register_owned(impl Into<BoxedBytes>)` variant where velo takes
ownership and hands the buffer back on deregistration. A `&mut [u8]`-shaped
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
`rdma: Option<RdmaOffer>` (transport key + protocol version). serde_json
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
owner's region generation (bumped on arena/region reuse) — echoed back in
`_rv_detach`/`_rv_release` for diagnostics, and future-proofs a Phase-5 rkey
cache. No foreign types (no `MemType` enums from FFI crates) on the wire.
The same struct serves the Phase-5 PUT reversal with a direction flag —
one descriptor primitive, two directions (the MPICH `prepare_rdma_info`
shape; every surveyed system converged on it).

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
  under `loom`-style stress where practical.

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
- Tests: full matrix over tcp — pinned↔chunked consumer/owner cross-product,
  old-wire simulation (requests without the offer field), GET-failure
  fallback, lease-reaper, kill switch, two-process example
  (`rendezvous_rdma_two_proc.rs`, porting PR #40's launcher harness — its one
  unambiguously good artifact).

**Checkpoint after Phase 3: hardware validation** on a real IB cluster
(compute-session MCP; `UCX_TLS=rc_mlx5,ud_mlx5`): correctness matrix +
latency/bandwidth vs chunked baseline + registration-cost numbers. Threshold
defaults revisited with data. Not a PR; a report.

### Phase 4 — Lifecycle pressure: EP reaper + pool reclamation (PR 4)

- EP idle reaper on the progress thread (D9), config
  (`ep_idle_timeout`, default off or generous — decided with Ryan at
  checkpoint), metrics (`eps_closed_idle_total`).
- Empty-arena reclamation under the byte budget; low-water retention.
- Shutdown/teardown interaction tests; soak test (register/transfer/release
  loop asserting stable registered-bytes and EP counts).

### Phase 5 — Role reversal (PUT) + measured optimizations (PR 5, scope-gated)

- Consumer-supplied descriptor in the acquire request ("PUT into this"):
  owner PUTs + `ucp_ep_flush_nbx` + completion AM (`_rv_put_done`), the flush
  carrying an explicit timeout (tcp SW-RMA flush needs the peer progressing).
  Same descriptor struct, direction flag (D7).
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
on main). Phase 4 is independent of 3 (can parallelize after 2). Phase 5 waits
for the checkpoint.

---

## 3. Test-fidelity ledger (what tcp CI cannot see)

Honest accounting, per the critique:

| Not testable over `UCX_TLS=tcp` | Mitigation |
|---|---|
| Stale-rkey → remote access error → QP death | Design excludes stale rkeys (single-use + acquire-time revalidation); hardware checkpoint exercises the error path deliberately once |
| SW RMA validates nothing (bad addr = silent corruption) | Owner-authored addresses only (descriptor comes from the owner's own table); consumer never computes remote addresses; debug asserts on ranges |
| Multi-MD rkey serialization | `md_map` probe test documents what CI covers; hardware checkpoint covers the real format |
| Registration cost realism | Bench harness both places; thresholds re-derived at checkpoint |

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
   point.
5. Backend pluggability (NIXL / libfabric later) added as **D12**.
