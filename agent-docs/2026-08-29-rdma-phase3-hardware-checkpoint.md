# velo RDMA rendezvous — Phase-3 hardware checkpoint

Dated evidence, not a living design. It records one run on one fabric on one
commit. `docs/proposals/ucx-rendezvous-rdma.md` is the design this checkpoint
was asked for; when the two disagree about what the code does today, the
proposal is right and this file is history.

**Commit under test:** `7276b99333dd3df10ff720436deb9eb0ca675187`, the head of
`ai/rdma-phase3-protocol` (PR #68). Cloned on the cluster from the pushed
public branch; no local repo changes, no commits. **Run date: 2026-08-29.**

**Lane: `rc_verbs`, not `rc_mlx5`.** The accelerated lane the plan prescribed
for this checkpoint did not exist in velo's vendored UCX build at run time — see
§2. **Every latency, bandwidth and packed-rkey number below is an unaccelerated
floor**, taken on a plain verbs memory domain. They are lower bounds on what
this hardware can do, not measurements of it. Nothing in §4, §5, §6 or §8 should
be quoted without that qualifier.

## What has changed since 7276b99

- **Nothing on the Phase-3 branch.** `7276b99` is still the head of
  `ai/rdma-phase3-protocol`. The out-of-range `mem_type` rejection at
  `lib/velo/src/transports/ucx/rma.rs:547-550` was already in the tree at run
  time — it does not change Finding 2's blob, which sets `mem_type = 0` and is
  accepted by design.
- **Phase 4 was written afterwards** and is stacked on top as PR #69
  (`ai/rdma-phase4-lifecycle`): the UCX endpoint idle reaper and eager wireup,
  arena reclamation (`arena_reclaim_after` / `retain_arena_bytes`), and the
  inbound-traffic keepalive. None of it existed when the matrix in §4 ran.
  `eager_endpoints` exists *because of* §5's 14 ms finding.
- **§2's root cause was proven off-hardware after the run** and fixed in PR #70
  (`ai/ucx-rs-mlx5-link-order`), which is open at the time of writing.
- **No number in this report has been re-measured since `7276b99`.** Treat
  every table as pinned to that commit and that lane.

---

## 1. Environment (VERIFIED)

| | |
|---|---|
| Cluster | computelab, partition `-/lego-c2@qs/0gpu-144cpu-480gb` |
| Nodes | `lego-c2-qs-36` (owner) and `lego-c2-qs-37` (consumer), `--exclusive`, 144 cores / 478 GB each |
| Arch / OS | **aarch64** (Grace-class), Ubuntu 22.04.5 |
| NIC | ConnectX-7 (`vendor_part_id 4129`, fw `28.43.2566`), 3 mlx5 devices per node |
| Port used | **`mlx5_2:1` — InfiniBand, PORT_ACTIVE, 400 Gb/s (4X NDR)**, MTU 4096, `sm_lid 0x7` **identical on both nodes** (same subnet manager, same fabric) |
| Excluded | `mlx5_0:1` Ethernet 25 Gb/s ACTIVE (RoCE-capable — deliberately excluded so no result can be a silent RoCE measurement), `mlx5_1:1` Ethernet DOWN |
| `ulimit -l` | unlimited |
| Toolchain | distro cmake/autotools; `libibverbs`/`librdmacm` headers present |

### Lane verification (VERIFIED)

`UCX_PROTO_INFO=y` on the live two-process run names the device for every protocol range,
including the rendezvous read:

```
| 1476303..inf | (?) rendezvous zero-copy read from remote | rc_verbs/mlx5_2:1 50% on path0 and 50% on path1 |
|       0..107 | short                                     | rc_verbs/mlx5_2:1/path0                         |
```

Every run below used `UCX_NET_DEVICES=mlx5_2:1 UCX_TLS=rc_verbs,ud_verbs,self`.
The device pin excludes both Ethernet ports, so tcp and RoCE are ruled out by
construction and confirmed by the protocol table. **`rc_mlx5` was NOT
exercised — it did not exist in this build. See Finding 1.**

---

## 2. Finding 1 — velo's vendored UCX exposed no mlx5-accelerated transports

**Status: root cause proven, fix written.** The mechanism below was reproduced
off-hardware after this run and corrected in PR #70. This section is kept
because its symptom and evidence are what killed two plausible wrong answers,
and because every number in this report was taken *before* the fix.

### The symptom (observed on the run)

`UCX_TLS=rc_mlx5,ud_mlx5` — the setting the plan prescribes for this
checkpoint, and the one an operator would put in production — makes velo
print:

```
UCX  WARN  transports 'rc_mlx5','ud_mlx5' are not available, please use one or more of:
ib, mm, posix, rc, rc_v, rc_verbs, self, shm, sm, sysv, tcp, ud, ud_v, ud_verbs
```

UCX then initialises with `self` only, the UCX transport cannot reach the peer,
and the transfer **silently falls back to the chunked path**. The example's
`rdma/ok` assertion caught it — the path-selection metric doing exactly the job
D6/Phase-3 built it for. Good for the design, bad for the build.

### Evidence that the build, not the hardware, is responsible

* `ucx-rs` configures with `--with-mlx5`; `config.log` records `HAVE_MLX5_DV 1`,
  `HAVE_DEVX 1`, `HAVE_MLX5_HW_UD 1`, `HAVE_INFINIBAND_MLX5DV_H 1`.
* `libuct_ib_mlx5.a` is built (6.5 MB) and **is** linked:
  `uct_rc_mlx5_iface_tl_ops`, `uct_ud_mlx5_iface_tl_ops`,
  `uct_dc_mlx5_iface_tl_ops` and `uct_mlx5_init` are all in the final binary's
  symbol table.
* The **system** UCX on the same node, same NIC (`/usr/bin/ucx_info`, 1.18.0)
  reports `rc_mlx5`, `ud_mlx5` **and** `dc_mlx5` available on `mlx5_2:1`. The
  hardware and driver support them.

Those three bullets say the accelerated code was compiled, was linked, and had
capable hardware under it. Whatever went wrong happened at runtime.

### Root cause (proven off-hardware, after this run)

`crates/ucx-rs/build.rs:350-351` (on `main` before PR #70) emitted
`static=uct_ib_mlx5` **before** `static=uct_ib`. Archive members land in the
rlib in emission order, so `.init_array` ran `uct_mlx5_init` before
`uct_ib_init`. Both constructors push their memory domains onto `uct_ib_ops`
with `ucs_list_add_head`, so the **later** one owns the head: mlx5 adds
`[devx, dv]`, verbs is then prepended, and the list is `[verbs, devx, dv]`.
`uct_ib_component_md_open` takes the first entry that returns `UCS_OK`, and
`uct_ib_verbs_md_open` succeeds unconditionally unless DEVX is forced. Every
mlx5 NIC therefore opened with a plain verbs memory domain, and `rc_mlx5` /
`dc_mlx5` / `ud_mlx5` queried zero devices — which is precisely the "registered
but no devices" state that produces the warning above.

The dlopen path cannot hit this: `uct_ib_init` ends with
`UCS_MODULE_FRAMEWORK_LOAD(uct_ib, 0)`, strictly *after* verbs registers. That
is why a system UCX 1.18 works on the same NIC and the vendored
`--disable-shared` build did not.

Reproduced on a workstation with four mlx5 devices and no cluster: the same
prebuilt archives linked in the two orders give `md open by
'uct_ib_verbs_md_ops'` and `md open by 'uct_ib_mlx5_devx_md_ops'`
respectively, at `UCX_LOG_LEVEL=debug`.

### The two hypotheses this run could not separate — BOTH DISPROVEN

Kept, not deleted, because the run genuinely could not tell them apart and the
next reader should see what the evidence above rules out.

1. **DISPROVEN — "`--disable-shared` means UCX's module loader never loads the
   static archive."** Wrong. The archive *is* pulled in and the mlx5 transports
   *are* registered; the defect is which memory domain wins the open, not
   whether the code is present.
2. **DISPROVEN — "1.18-vs-1.22 version skew between the system UCX and the
   vendored one."** Wrong. Reversing the archive order against the *same* 1.22
   archives fixes it. The version was never the variable.

The run recorded one blocked attempt to settle this — building `ucx-rs` against
the system UCX via `UCX_DIR=`, which the node could not support (system UCX
runtime present, headers absent). It is moot: the confound it would have
eliminated does not exist.

### The fix, and one correction for anyone tempted to harden it

PR #70 emits `static=uct_ib` before `static=uct_ib_mlx5` and adds
`crates/ucx-rs/tests/ctor_order.rs`, which reads the `uct_ib_ops` registration
list directly rather than decoding `.init_array` — no binutils, no ELF parsing.
Measured on this workstation across all four mlx5 devices, `md open by
'uct_ib_verbs_md_ops'` becomes `md open by 'uct_ib_mlx5_devx_md_ops'`, under
**both** GNU ld and the `mold` linker CI uses. The same PR passes `--with-devx`
explicitly, because upstream defaults it to `check` and silently drops the
accelerated domain.

**Repeating the archive defensively does NOT work.** `static=` bundles archive
members into the rlib, so a repeated archive collapses to its **last** position
and reproduces the bug exactly. `+whole-archive` and `+verbatim` change
inclusion and name resolution, not member position, so they do not help either.
The two emission lines are the whole mechanism, and the ordering test is the
only thing standing between a tidy-up and a silently unaccelerated build.

### Operator consequence, and the runtime workaround

Until PR #70 is in the build you are running: an operator who sets
`UCX_TLS=rc_mlx5,ud_mlx5` on real InfiniBand gets a **silent chunked
deployment**. That is also the example given in `UcxConfig::tls`'s doc comment
(`lib/velo/src/transports/ucx/transport.rs:61`).

**`UCX_IB_MLX5_DEVX=y` in the environment is a working runtime workaround.** It
forces `uct_ib_verbs_md_open` to bail, the DEVX memory domain opens instead,
and the mlx5 transports find their devices — on an unfixed binary, with no
rebuild.

---

## 3. Finding 2 — an unusable-but-well-formed rkey is fatal on IB, recoverable on tcp

`unusable_rkey_fails_cleanly_inside_ucx`, run over IB, **aborts the process**:

```
[lego-c2-qs-36:2150764:0:2150910] rc_verbs_impl.h:104  Fatal: receive completion[0] with error on
mlx5_2/0x5a18bc03ff00: general error, vendor_err 0x0 wr_id 0x5a18ce40d625
process didn't exit successfully: ... (signal: 6, SIGABRT: process abort signal)
```

Over `UCX_TLS=tcp` the same blob is rejected inside `ucp_ep_rkey_unpack` and
surfaces as `RmaError::Ucx`, which is what the test asserts. Over IB the unpack
**succeeds** (a local mlx5 memory domain does exist), the GET is posted with
unusable key material, and `uct_rc_verbs` turns the HCA completion error into
`ucs_fatal` → `SIGABRT`. This is D3's "stale rkey on RC =
`IBV_WC_REM_ACCESS_ERR` = the whole QP dies" prediction, and it is worse than
predicted: not a dead QP, a **dead process**, with no Rust-level error to fall
back from.

**Blast radius (VERIFIED — this is what keeps it out of "blocker"):** all **23
`rendezvous_rdma` integration tests pass over IB**, including
`a_malformed_descriptor_falls_back_to_chunked` and
`a_failed_get_falls_back_chunked_exactly_once`. The shipped path's
containment — the syntactic pre-parse `preparse_packed_rkey`
(`lib/velo/src/transports/ucx/rma.rs:535`, bounds wrapper `validate_packed_rkey`
at `:480`) — holds for every corruption the suite generates. The blob that kills the process
is one the pre-parse *accepts by design* (`md_map` names one MD, entry present,
`sys_dev = UNKNOWN`; the test's own comment calls it "self-terminating").

**Honest statement:** not reachable from the shipped path by anything the
current suite can produce, and the containment is **syntactic only**. A
semantically stale rkey — precisely the class D3's single-use rkeys and
acquire-time revalidation exist to make unreachable — would pass the pre-parse
identically. If that invariant is ever holed, the failure mode on IB is process
abort, not a fallback. Worth a follow-up: either a `UCX_ERROR_HANDLING`-style
guard, or an explicit note in the RMA safety docs that the pre-parse is the
only thing standing between a bad descriptor and `ucs_fatal`.

Two consequences the report did not draw at the time, both worth carrying
forward:

* **This finding and the shutdown force-unmap accepted risk are the same
  hazard.** `docs/proposals/ucx-rendezvous-rdma.md:239-240` prices the
  force-unmap straggler as *"On IB a straggler GET then gets a remote-access
  error (its problem, correctly)"*. Finding 2 says that may in fact be the
  straggler's *process aborting*. Neither document currently mentions the
  other.
* **A consumer-side rkey cache removes both legs of the safety argument at
  once.** Single-use rkeys mean an rkey cannot outlive its op; a cache makes it
  outlive the op by construction. Acquire-time revalidation means the owner
  re-checks every transfer; a cache is a decision not to. Any scoping that
  treats the cache as "just an optimization gated on a profile" is mispricing
  it.

---

## 4. Correctness + performance matrix (2 nodes, cold pair per rep)

Method: one owner process on node A and one consumer process on node B per rep,
5 reps per cell, via the `rendezvous_rdma_two_proc` example that ships on
`ai/rdma-phase3-protocol` (PR #68). Times are the example's own `velo.get()`
measurement (acquire round-trip + transfer + copy-out; `release` excluded).
**Each rep is a fresh process pair, so every RDMA number here includes one-time
endpoint establishment** — see §5.

`VELO_RDMA_RENDEZVOUS_DISABLE=1` supplies the chunked baseline.

| payload | RDMA `get()` median | chunked `get()` median | integrity | path label |
|---|---|---|---|---|
| 64 KiB | **14.21 ms** | 0.346 ms | ok / ok | `rdma/ok` / `chunked/kill_switch` |
| 1 MiB + 4097 B (non-granule) | **14.55 ms** | 1.433 ms | ok / ok | `rdma/ok` / `chunked/kill_switch` |
| 1 MiB | **14.38 ms** | 1.271 ms | ok / ok | `rdma/ok` / `chunked/kill_switch` |
| 16 MiB | **16.11 ms** | 14.597 ms | ok / ok | `rdma/ok` / `chunked/kill_switch` |
| 256 MiB | **57.50 ms** | 197.25 ms | ok / ok | `rdma/ok` / `chunked/kill_switch` |

* **50/50 transfers verified byte-for-byte** (offset-dependent pattern),
  including the non-granule size. Zero corruption, zero truncation, zero hangs.
* All 25 RDMA-mode reps report `rdma/ok=1` on **both** sides (the example exits
  non-zero otherwise, and all 25 owner and consumer processes exited 0).
* All 25 chunked-mode reps transferred correctly; they exit 1 only because of
  the example's deliberate "the fast path was not used" assertion.
* `wall_ms ≈ 30 000` in the raw results is **NFS negative-dentry caching** on
  the cluster `$HOME` (the only shared filesystem on these nodes, and it was
  full — 12 MB free) delaying the file rendezvous. It is not a velo latency. Do
  not read it as one.

---

## 5. The finding the threshold verdict turns on: a ~14 ms one-time cost

The matrix's flat ~14 ms floor across 64 KiB → 1 MiB is **not** per-transfer. A
purpose-built harness (`rv_sweep`, §10) that stages one slot per transfer and
runs 10 transfers per size inside a *single* process pair separates them:

| payload | RDMA warm median | chunked warm median | RDMA advantage |
|---|---|---|---|
| 4 KiB | 108 µs | 151 µs | 1.40× |
| 8 KiB | 113 µs | 156 µs | 1.38× |
| 16 KiB | 117 µs | 161 µs | 1.38× |
| 32 KiB | 114 µs | 183 µs | 1.61× |
| **64 KiB** | **123 µs** | **194 µs** | **1.58×** |
| 128 KiB | 129 µs | 227 µs | 1.76× |
| 256 KiB | 131 µs | 280 µs | 2.14× |
| 512 KiB | 159 µs | 401 µs | 2.52× |
| 1 MiB | 229 µs | 717 µs | 3.13× |
| 4 MiB | 566 µs | 2816 µs | 4.98× |

(medians of reps 1–9; rep 0 excluded because it carries the one-time cost)

* **First RDMA `get()` on a fresh peer pair: 14 145 µs.** Second and
  subsequent: ~108 µs. The chunked path's first transfer costs 216 µs — it pays
  nothing comparable, because it reuses the already-connected TCP control
  transport and never establishes a UCX endpoint.
* 100/100 sweep transfers took `rdma/ok`; 100/100 chunked-run transfers took
  `chunked/kill_switch`; every payload verified.
* Warm RDMA is a fixed ~105 µs plus a size-dependent term. A two-point fit over
  1 MiB → 4 MiB (229 µs → 566 µs) implies ~9 GB/s marginal, but that is two
  points and should be treated as shape, not a bandwidth measurement. Whatever
  the slope, it is well under the 400 Gb/s link because `Velo::get()` copies out
  of the pinned buffer into `Bytes`; `get_pinned()` is the interesting number
  and was **not** measured here.

**Acted on.** Phase 4's `eager_endpoints` establishes the endpoint at
registration rather than at first GET, so this cost lands off the transfer's
critical path. It exists because of this finding. The finding is closed; the
number has not been re-measured on an accelerated lane.

---

## 6. Registration cost and raw GET latency (`bench_rma`, over IB)

Two workers in **one process** on `lego-c2-qs-36` — these are HCA-loopback
numbers, not inter-node, and must not be mixed with §4/§5. The header the test
prints still says `UCX_TLS=tcp`; that label is stale — the environment
overrode it (see §10's trap).

| region size | `ucp_mem_map` | `ucp_mem_unmap` | packed rkey |
|---|---|---|---|
| 4 KiB | 53.98 µs | 35.52 µs | **20 B** |
| 1 MiB | 43.07 µs | 31.84 µs | **20 B** |
| 64 MiB | 265.97 µs | 132.22 µs | **20 B** |

| GET size | latency (3 timed passes) | throughput |
|---|---|---|
| 64 KiB | 24.93 / 22.43 / 19.71 µs | 2.5–3.2 GiB/s |
| 1 MiB | 50.27 / 49.82 / 49.92 µs | ~19.6 GiB/s |
| 16 MiB | 443.2 / 441.9 / 442.1 µs | ~35.3 GiB/s |

Registration is cheap enough (43–266 µs) that D4's pre-registered arenas remain
the right call but are not the bottleneck at these sizes; the ~105 µs acquire
round-trip dominates.

### Packed rkey: 20 B here, 19 B in the tree, and why both are right

**Packed rkey = 20 B on the `rc_verbs` / verbs-MD lane**, against exactly 9 B
over tcp. The 9-byte tcp figure is the header alone, because the tcp memory
domain registers nothing; the extra 11 bytes on IB are **verbs-MD** key
material, not mlx5 key material — §2 says no mlx5 memory domain was ever open
on this build.

`docs/proposals/ibverbs-transport.md:919-920` records a conflicting number from
an independent probe: *"Measured packed rkey on a real IB MD: 19 B (1.22) / 18
B (1.19), vs the 20 B derived from source."* Both are 1.22 on aarch64
ConnectX-7, and they disagree by a byte.

**The reconciliation, and how strong it is.** A packed rkey carries key
material per memory domain, and different memory-domain implementations pack
different amounts of it, so two probes that opened different MDs can both be
right. §2
proves what this run opened: a verbs MD, because the mlx5 one lost the
registration race. The other probe was on a build with no such defect, so the
DEVX MD is the one it would have opened. That is the best available
explanation — the mechanism fits, the versions match — but it is **not
proven**: the two probes ran on different hosts with different MLNX stacks, and
**no packed rkey has been measured under a DEVX MD on either**. Neither number
is necessarily the one velo will see after PR #70.

**The decision that follows — do not tighten `rkey_pack_canary`'s `>= 9`
bound.** The canary is at `lib/velo/src/transports/ucx/tests.rs:792`; the bound
is the `assert!` at `:806`. Raising it to 20 would pin a value this build
produces *only because of* the link-order defect, and the number is expected to
move once the DEVX MD opens. The 9-byte floor stays the tightest bound CI can
assert. The comment above the test (`tests.rs:785-790`) says real InfiniBand
"only the hardware checkpoint can observe" — the checkpoint has now observed
it, and the observation is *why the bound stays loose*, not a licence to
tighten it.

---

## 7. Failure-path items

| Item | Outcome |
|---|---|
| (a) FORCE-close cancellation on IB — `get_cancelled_by_endpoint_replacement` | **PASSED on IB** (0.16 s). No `get_am`/`get_offload` divergence observed: the caller was answered, the region released, teardown balanced. |
| (b) Stale/unusable rkey → remote access error | **Achieved by proxy.** The planned owner-unmap harness was not run. `unusable_rkey_fails_cleanly_inside_ucx` reached a comparable failure and produced a stronger result — process abort, not silent success. It is a *different* experiment: a hand-crafted unusable rkey, not a semantically stale rkey from an unmapped region. See Finding 2. |
| (c) `abandon_rma_ops` via a SIGSTOP'd owner | **NOT ATTEMPTED.** |

Other in-crate UCX RMA tests re-run over IB, all **passing**:
`rkey_pack_canary`, `preparse_accepts_real_packed_rkeys`, `map_get_roundtrip`,
`get_zero_length`, `get_out_of_range`, `unmap_waits_for_inflight`,
`shutdown_with_inflight_get`, `get_cancel_still_releases_the_region`,
`peer_shutdown_during_get_answers_caller`, `truncated_rkey_is_refused_before_ucx`.

**Observation (not a failure):** four teardown-path tests that pass on IB
(`map_get_roundtrip`, `unmap_waits_for_inflight`,
`get_cancel_still_releases_the_region`,
`peer_shutdown_during_get_answers_caller`) each emit
`ucp_ep.c:2222 UCX ERROR ep 0x… has already been closed`. Every assertion
holds, but it is a double-close that the tcp lane never surfaces, and it will
be log noise in production.

---

## 8. Threshold verdict (D11)

**Recommendation: keep `rdma_min_bytes = 64 KiB`. Do not lower it, do not raise
it.**

Reasoning from the measurements, not from vibes:

1. On a **warm** peer pair, RDMA beats chunked at *every* size measured, down to
   4 KiB (1.4×). There is no crossover above 4 KiB on this fabric — so 64 KiB is
   conservative, and the cost of that conservatism is at most ~40–70 µs per
   transfer in the 4–64 KiB band. That is not worth the extra pinned-memory
   pressure and lease traffic small slots would create.
   **Caveat on what this comparison isolates:** the chunked arm ran with the
   kill switch (`VELO_RDMA_RENDEZVOUS_DISABLE=1`), so the owner also staged in
   plain memory (`pinned=false` in every run-B line, against `pinned=true` in
   run A). Staging mode therefore co-varies with transfer path. That is the
   right comparison for "should this deployment enable RDMA at all", but it is
   not a clean per-slot isolation of `rdma_min_bytes` with the pool warm on both
   sides. The conclusion very likely survives — the gap at 4 KiB is ~40 µs
   either way — but do not read the table as a pure threshold sweep.
2. On a **cold** pair the picture inverts completely: the one-time ~14.1 ms
   endpoint cost means chunked wins by 41× at 64 KiB, 11× at 1 MiB, 1.1× at
   16 MiB, and only loses at 256 MiB (3.4×). The cold crossover therefore lies
   **between 16 MiB and 256 MiB, close to the low end** — the two cells are 16×
   apart, so no tighter number is supported by this data. Lowering
   `rdma_min_bytes` would make the cold case worse; raising it into that band
   would throw away the warm-path win, which is the common case for any peer
   pair that transfers more than once.
3. So the threshold is not the lever that matters. **The lever is the ~14 ms
   first-GET cost.** Concrete follow-ups, in priority order:
   - Establish the UCX endpoint eagerly at `register_peer` / first `_hello`, not
     lazily at first GET, so the cost lands off the transfer's critical path.
     This is the single highest-value change the numbers point to. (Done in
     Phase 4 as `eager_endpoints`; see §5.)
   - Failing that, document it, and let Phase 4's EP reaper policy default to
     *not* reaping (which it already does — D9 says off by default, and these
     numbers justify that default strongly).
4. `D11`'s ordering invariant (`rdma_min_bytes ≤ transparent threshold <
   chunk_size`, defaults 64 KiB / 256 KiB / 512 KiB) is untouched by any of the
   above and remains satisfied.
5. Everything here is measured on the **unaccelerated `rc_verbs` lane**
   (Finding 1, §2). Once PR #70 lands and the mlx5 transports are restored, warm
   RDMA latency should improve and the 14 ms wireup may change materially;
   **the threshold should be re-derived on the accelerated lane before
   `rdma_min_bytes` is touched.**

---

## 9. What was VERIFIED, attempted, and skipped

**Verified:** IB fabric and lane (`rc_verbs/mlx5_2:1`, 400 Gb/s NDR, shared SM);
the 5×2 correctness matrix with byte-level integrity; path-selection labels on
both sides; packed rkey = 20 B on the verbs-MD lane; `bench_rma` registration
and GET costs; 23/23 rendezvous integration tests on IB; 10/12 in-crate UCX RMA
tests on IB plus the two rkey tests; the warm-vs-cold latency separation; the
mlx5 transports' absence from velo's build and their presence in the system UCX
on the same NIC.

**Attempted, blocked, and since made moot:** building `ucx-rs` against the
system UCX via `UCX_DIR=` to eliminate the 1.18-vs-1.22 confound — the node
ships the system UCX runtime without headers. §2's root cause removes the
confound this would have settled.

**Skipped:** failure item (c) (`abandon_rma_ops` under a SIGSTOP'd owner).
`get_pinned()` / `get_into()` throughput (the copy-out dominates `get()` at
large sizes and was not isolated). Multi-transfer concurrency and any soak.

---

## 10. How to reproduce

The sweep harness that produced §5 is committed alongside this report as
`agent-docs/rv_sweep.rs`. (Original run: computelab job 4009689, since
released.)

**Why it is not a compiled example yet.** It targets Phase-3 API that is not on
`main`: `Velo::register_data_pinned`, `Velo::rdma_registered_bytes`,
`RdmaRendezvousConfig` / `RdmaConfig::rendezvous`, and the
`velo_rendezvous_rdma_path_total` metric family. It also needs `serde_json`,
which `examples/Cargo.toml` gains on the Phase-3 branch. Against `main` today
`cargo check --features ucx --example rv_sweep` fails with five errors. Once
PR #68 and PR #69 land, move it to `examples/examples/rv_sweep.rs` and add:

```toml
[[example]]
name = "rv_sweep"
required-features = ["ucx"]
```

`required-features` is mandatory, not decoration. Measured on the Phase-4 tree,
a bare `[[example]]` stanza plus `cargo check --example rv_sweep` under default
features fails with `E0599: no method named rdma_registered_bytes`, because
that method is `#[cfg]`-gated on the `ucx` feature and the harness calls it
outside any `cfg` block. Do not add a run step to the Examples CI job: this is a
two-node measurement tool and compiling it is the point.

**To run the sweep:**

1. Build it: `cargo build --manifest-path examples/Cargo.toml --release
   --features ucx --example rv_sweep`.
2. Allocate two `--exclusive` nodes that share a subnet manager and have an
   ACTIVE InfiniBand port. Confirm the port with `ibv_devinfo` before trusting
   any number — an ACTIVE Ethernet port on the same card will happily carry
   RoCE and look like a success.
3. Run owner and consumer with a shared rendezvous directory, under
   `UCX_NET_DEVICES=<device>:<port> UCX_TLS=rc_verbs,ud_verbs,self` (or
   `rc_mlx5,ud_mlx5` once PR #70 is in the build). Pin the device; do not rely
   on UCX picking it.
4. Verify the lane with `UCX_PROTO_INFO=y` and check the "rendezvous zero-copy
   read from remote" row names the device and transport you intended.

**The trap that will otherwise catch you.** velo applies the builder's `.tls()`
**only when `UCX_TLS` is unset** (`lib/velo/src/transports/ucx/worker.rs:848`).
The environment wins. That is what let this whole re-run happen on IB without
touching the code — and it is why `bench_rma`'s printed header still said
`UCX_TLS=tcp` in §6 while the run was on InfiniBand. Never trust a printed
config header over `UCX_PROTO_INFO`.
