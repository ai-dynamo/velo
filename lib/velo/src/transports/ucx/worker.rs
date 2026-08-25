// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The UCX progress thread.
//!
//! One dedicated OS thread owns the `ucp_context` and a single
//! `UCS_THREAD_MODE_SINGLE` worker (lock-free even in an `--enable-mt` build;
//! the price is that every `ucp_*` call on the worker must happen on this
//! thread — enforced here by construction: the raw handles never leave
//! [`worker_main`]). Work arrives over a bounded flume ring fed by the
//! per-peer [`AdmissionGate`](crate::transports::transport::AdmissionGate)s;
//! completions leave through UCX callbacks that resolve directly into velo's
//! channels, which wake tokio tasks from any thread.
//!
//! ## Ownership discipline (the async-ucx issue #1 fix)
//!
//! Every posted operation is **completion-owned**: exactly one
//! [`Arc<OpState>`] rides `ucp_request_param_t.user_data` into UCX, and the
//! send trampoline reclaims and drops it when the operation completes. The
//! buffers therefore live precisely as long as UCX may touch them, no future
//! owns them, and cancellation is not a concept the data path needs.
//! `ucp_am_send_nbx` has three mutually exclusive exits and exactly one of
//! them drops the Arc:
//!
//! 1. `NULL` — completed inline; the callback is *ignored even if set*, so the
//!    poster reclaims the Arc.
//! 2. request pointer — the trampoline fires exactly once, reclaims the Arc,
//!    and frees the request.
//! 3. error pointer — no callback; the poster reclaims and reports.
//!
//! Which exit a given send takes is non-monotonic in size and differs across
//! UCX versions — all three paths are always live.
//!
//! ## Wakeup protocol
//!
//! The loop drains the ring, progresses the worker to quiescence, then spins
//! for a short window before arming the worker and parking in `poll(2)` on
//! the wakeup fd. Submitters ring the [`Doorbell`] only when the loop is
//! parked (`armed == true`), because `ucp_worker_signal` costs ~1-3 µs while
//! a ring push costs ~100 ns. The park has a bounded timeout as a lost-wakeup
//! backstop.
//!
//! ## RMA ordering invariants
//!
//! The RMA path ([`super::rma`]) adds four rules that the rest of this module
//! is written to preserve. All UCX line references are to 1.22.0, the version
//! `ucx-rs` vendors.
//!
//! **No completion callback may enqueue onto the ring.** This thread is the
//! ring's only consumer and the ring is bounded, so a callback that blocks on a
//! full ring deadlocks the process. RMA completions therefore resolve a
//! `oneshot` (never blocks, safe from any thread) and hand the main-loop work
//! they generate — the region's in-flight decrement and the op's registry
//! removal — over through [`WorkerState::rma_completions`], mirroring the
//! `err_events` precedent that exists for exactly this handoff.
//!
//! **An unpacked `ucp_rkey_h` dies inside its operation's completion callback.**
//! UCX's documented rule is "destroy the rkey before the endpoint it was
//! unpacked on". The callback is the tightest point that satisfies it, and two
//! source facts make it safe rather than merely convenient:
//!
//! * `ucp_rkey_destroy` (`ucp_rkey.c:1134`) dereferences **no endpoint**. It
//!   releases each transport key through its `uct_component_h` — a context-level
//!   object — and returns the descriptor to `worker->rkey_mp`. The endpoint is
//!   not involved at all, so "before the endpoint" is satisfied by any call at
//!   all, and what actually has to outlive the rkey is the *worker*.
//! * On the close path the endpoint is alive anyway:
//!   `ucp_ep_close_nbx(FORCE)` → `ucp_ep_discard_lanes` →
//!   `ucp_worker_discard_tl_uct_ep` takes `ucp_ep_refcount_add(ucp_ep, discard)`
//!   (`ucp_worker.c:3775`), and `ucp_ep_delete` only deallocates at refcount
//!   zero, which is after the purge that drives these callbacks with
//!   `UCS_ERR_CANCELED`.
//!
//! The worker-lifetime requirement holds even on the one path where a callback
//! fires from *inside* `ucp_worker_destroy`: that function drives purges
//! (`ucp_worker_discard_uct_ep_cleanup`, `ucp_worker_destroy_eps`, lines
//! 3053-3055) well before `ucp_worker_destroy_mpools` tears down `rkey_mp`
//! (`ucp_worker.c:2129`). So the rkey outlives no endpoint and precedes no
//! mpool, whichever path completes it.
//!
//! **A parked unmap gates its region.** `Cmd::UnmapRegion` for a region with
//! operations in flight parks its reply in the region entry; from that moment
//! new `Cmd::RmaGet`s against the region are refused, and the reply resolves
//! from [`WorkerState::drain_rma_completions`] once the last operation lands.
//! Repeat unmaps attach as additional waiters rather than being refused, so a
//! caller that cancels and retries cannot be told a live, DMA-active region does
//! not exist. That drain runs on every pass, ungated by the ring being empty —
//! an awaited GET followed immediately by an unmap parks the unmap by
//! construction, and gating the drain would hold it there for as long as the
//! ring stayed busy.
//!
//! **Every RMA reply resolves, including at teardown.** Unlike a frame send,
//! whose failure path is a fire-and-forget `on_error` callback, an RMA operation
//! has a caller `await`ing a `oneshot`. Teardown's in-flight drain is bounded
//! and may expire with operations outstanding, and `ucp_worker_destroy` does not
//! run user completion callbacks for them — so the senders would go to the grave
//! inside UCX's request bookkeeping and the callers would await forever.
//! [`WorkerState::rma_ops`] therefore keeps an `Arc` of every posted operation,
//! and teardown takes the reply out of each survivor and answers
//! `ShuttingDown`. The reply lives behind a `take`-able slot, which is what
//! makes double-resolution impossible if the callback later fires anyway.
//!
//! ## What teardown cannot promise
//!
//! D8 asks for regions to be unmapped before endpoints are closed. That holds
//! for every region that is idle when teardown starts, and for every region
//! whose operations complete during the flush-close. It cannot hold for a
//! region whose GET is posted to a peer that has stopped progressing: Phase A's
//! flush-close never completes, and Phase B's FORCE close on that same endpoint
//! is a **no-op** — `ucp_ep_close_nbx` returns `UCS_ERR_NOT_CONNECTED` at its
//! `UCP_EP_FLAG_CLOSED` guard (`ucp_ep.c:2221`) because Phase A already set the
//! flag, so no second discard and no `CANCELED` purge happen. Such an operation
//! is completed only by the purges inside `ucp_worker_destroy`, i.e. *after*
//! `force_unmap_regions` has already deregistered its destination. Over tcp that
//! is silent; on IB the straggler completes with an access error. Accepted, and
//! a hardware-checkpoint item — the caller is still answered, which is the part
//! that had to be fixed.

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, warn};
use ucx_rs::{decode_status_ptr, status_string, sys};
use velo_ext::{AdmitOutcome, InstanceId, MessageType, TransportAdapter, TransportErrorHandler};

use super::address::{AM_ID_BASE, AM_KIND_COUNT, AM_KIND_PING, AM_KIND_PONG, UcxEndpoint};
use super::rma::{
    MAX_PACKED_RKEY, MappedRegion, RKEY_UNPACK_PAD, RmaError, RmaGetRequest, validate_packed_rkey,
};
use super::transport::UcxConfig;

/// One message staged for the progress thread.
pub(crate) struct SendTask {
    pub peer: InstanceId,
    pub msg_type: MessageType,
    pub header: Bytes,
    pub payload: Bytes,
    pub on_error: Arc<dyn TransportErrorHandler>,
}

impl SendTask {
    pub(crate) fn fail(self, why: impl Into<String>) {
        self.on_error
            .on_error(self.header, self.payload, why.into());
    }
}

/// Commands accepted by the progress thread.
pub(crate) enum Cmd {
    Send(SendTask),
    /// Health probe: send a ping AM carrying `token`; the pong resolves the
    /// matching entry in [`WorkerShared::pending_pings`].
    Ping {
        peer: InstanceId,
        token: u64,
    },
    /// Reply to a ping received from `reply_ep` (a raw `ucp_ep_h` owned by
    /// this worker, provided by UCX for the REPLY-flagged inbound AM).
    PongTo {
        reply_ep: usize,
        token: u64,
    },
    /// Echo a request header back as `ShuttingDown` while draining.
    ShuttingDownTo {
        reply_ep: usize,
        header: Bytes,
    },
    /// Register `[ptr, ptr + len)` for RMA under `region_id` and pack a remote
    /// key for it. `region_id` is minted by the submitter so the reply needs no
    /// correlation table.
    MapRegion {
        ptr: usize,
        len: usize,
        region_id: u64,
        reply: tokio::sync::oneshot::Sender<Result<MappedRegion, RmaError>>,
    },
    /// Deregister a region once it has no local RMA operation left.
    UnmapRegion {
        region_id: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), RmaError>>,
    },
    /// Pull remote memory into a previously mapped local region.
    RmaGet {
        req: RmaGetRequest,
        reply: tokio::sync::oneshot::Sender<Result<(), RmaError>>,
    },
    Shutdown,
}

impl Cmd {
    /// Answer a command that will never be executed because the progress thread
    /// is going away. Every reply channel must be resolved rather than dropped:
    /// a dropped `oneshot` reaches the caller as `ChannelClosed`, which is a
    /// worse diagnosis than the truth.
    pub(crate) fn refuse_for_shutdown(self) {
        match self {
            Cmd::Send(task) => task.fail("ucx transport shutting down"),
            Cmd::MapRegion { reply, .. } => {
                let _ = reply.send(Err(RmaError::ShuttingDown));
            }
            Cmd::UnmapRegion { reply, .. } => {
                let _ = reply.send(Err(RmaError::ShuttingDown));
            }
            Cmd::RmaGet { reply, .. } => {
                let _ = reply.send(Err(RmaError::ShuttingDown));
            }
            Cmd::Ping { .. } | Cmd::PongTo { .. } | Cmd::ShuttingDownTo { .. } | Cmd::Shutdown => {}
        }
    }
}

/// Wakes the parked progress thread. See the module docs for the protocol.
pub(crate) struct Doorbell {
    /// True while the progress thread is (about to be) parked on the wakeup
    /// fd. Submitters that observe `false` skip the signal entirely.
    armed: AtomicBool,
    /// The raw `ucp_worker_h` as usize, or 0 once the worker is being
    /// destroyed. `ucp_worker_signal` is documented safe from any thread; the
    /// mutex exists only to make "signal" and "destroy" mutually exclusive.
    worker: Mutex<usize>,
}

impl Doorbell {
    pub fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            worker: Mutex::new(0),
        }
    }

    /// Called by submitters after pushing to the ring.
    pub fn ring(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let guard = self.worker.lock().unwrap_or_else(|e| e.into_inner());
            let raw = *guard;
            if raw != 0 {
                // SAFETY: non-zero means worker_main has not reached destroy;
                // destroy zeroes this field under the same mutex first.
                unsafe { sys::ucp_worker_signal(raw as sys::ucp_worker_h) };
            }
        }
    }

    /// Signal regardless of the armed flag. Used for shutdown, where a lost
    /// wakeup means a hung `join()`; a spurious signal to a non-parked worker
    /// is harmless (the next `ucp_worker_arm` returns BUSY and the loop
    /// continues).
    pub fn ring_force(&self) {
        self.armed.store(false, Ordering::Release);
        let guard = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        let raw = *guard;
        if raw != 0 {
            // SAFETY: as in `ring` — the handle is zeroed under this mutex
            // before the worker is destroyed.
            unsafe { sys::ucp_worker_signal(raw as sys::ucp_worker_h) };
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    fn install(&self, worker: sys::ucp_worker_h) {
        *self.worker.lock().unwrap_or_else(|e| e.into_inner()) = worker as usize;
    }

    fn retire(&self) {
        *self.worker.lock().unwrap_or_else(|e| e.into_inner()) = 0;
    }
}

/// State shared between the transport (any thread) and the progress thread.
pub(crate) struct WorkerShared {
    pub ring_tx: flume::Sender<Cmd>,
    pub doorbell: Arc<Doorbell>,
    /// Peers registered via `Transport::register`, keyed by instance.
    pub peers: Arc<DashMap<InstanceId, UcxEndpoint>>,
    /// Outstanding health probes, resolved by the pong recv trampoline.
    pub pending_pings: Arc<DashMap<u64, tokio::sync::oneshot::Sender<()>>>,
    /// Peers whose endpoint hit a transport-level error; consulted by
    /// `check_health` and cleared when a fresh endpoint is established.
    pub failed_peers: Arc<DashMap<InstanceId, ()>>,
    /// Operations posted with a request outstanding (exit 2). Drained before
    /// worker destruction.
    pub inflight_ops: Arc<AtomicUsize>,
    /// Set by `shutdown()`. The ring alone cannot carry the exit signal: the
    /// progress thread itself holds ring senders (via this struct and the AM
    /// recv contexts), so `Disconnected` is unreachable, and a full ring can
    /// drop a `try_send(Cmd::Shutdown)`.
    pub shutdown_requested: Arc<AtomicBool>,
    /// Bumped by every `register()`. The progress thread revalidates its
    /// cached endpoints against the peers map when this moves, so a
    /// re-registered peer (new incarnation) gets a fresh endpoint instead of
    /// AMs on the stale one.
    pub reg_epoch: Arc<AtomicU64>,
    /// Regions currently held by `ucp_mem_map`. Maintained by the progress
    /// thread only; readable from anywhere as a gauge. Phase 3's
    /// `rdma_registered_bytes` metric reads from here, and the tests assert it
    /// returns to zero — a non-zero value after every region has been accounted
    /// for is a leaked registration, the failure this whole module guards.
    pub live_regions: Arc<AtomicUsize>,
    /// Unpacked `ucp_rkey_h`s not yet destroyed. Same discipline: incremented
    /// after `ucp_ep_rkey_unpack`, decremented at the single `ucp_rkey_destroy`
    /// call site, asserted back to zero by the tests. Signed because a negative
    /// value would mean a double destroy, which is worth seeing rather than
    /// wrapping.
    pub live_rkeys: Arc<AtomicI64>,
}

/// What the progress thread reports back once UCX is initialised.
pub(crate) struct StartupOut {
    /// The packed local worker address.
    pub worker_addr: Vec<u8>,
    /// `ucp_worker_attr.max_am_header` — the hard cap on velo header bytes.
    pub max_am_header: usize,
}

/// Everything [`worker_main`] needs, assembled by `UcxTransport::start`.
pub(crate) struct WorkerArgs {
    pub config: UcxConfig,
    pub ring_rx: flume::Receiver<Cmd>,
    pub shared: Arc<WorkerShared>,
    pub adapter: TransportAdapter,
    pub startup: tokio::sync::oneshot::Sender<anyhow::Result<StartupOut>>,
}

// ---------------------------------------------------------------------------
// Completion-owned operation state
// ---------------------------------------------------------------------------

enum OpKind {
    /// A velo frame: failure reports through `on_error` with the original
    /// buffers, per the `Transport` contract.
    Frame {
        header: Bytes,
        payload: Bytes,
        on_error: Arc<dyn TransportErrorHandler>,
    },
    /// Control traffic (ping/pong/shutting-down echo): buffers are retained
    /// for UCX's benefit only; failures are logged, not reported.
    Control { _hold: Bytes },
}

struct OpState {
    kind: OpKind,
    inflight: Arc<AtomicUsize>,
}

impl OpState {
    fn complete(self: Arc<Self>, status: sys::ucs_status_t) {
        if status != sys::ucs_status_t_UCS_OK
            && let Some(state) = Arc::into_inner(self)
        {
            match state.kind {
                OpKind::Frame {
                    header,
                    payload,
                    on_error,
                } => {
                    on_error.on_error(
                        header,
                        payload,
                        format!("ucx send failed: {}", status_string(status)),
                    );
                }
                OpKind::Control { .. } => {
                    debug!("ucx control send failed: {}", status_string(status));
                }
            }
        }
        // status == UCS_OK: dropping the Arc releases the buffers.
    }
}

/// The `ucp_send_nbx_callback_t` for every posted operation (exit 2).
///
/// SAFETY contract: `user_data` is `Arc::into_raw(Arc<OpState>)` placed there
/// by [`post_am`], and this trampoline is the only consumer. Runs on the
/// progress thread during `ucp_worker_progress`; a panic must not unwind into
/// C, hence `catch_unwind`.
unsafe extern "C" fn send_trampoline(
    request: *mut c_void,
    status: sys::ucs_status_t,
    user_data: *mut c_void,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: see contract above — exactly one reclaim per posted op.
        let state = unsafe { Arc::from_raw(user_data as *const OpState) };
        state.inflight.fetch_sub(1, Ordering::AcqRel);
        state.complete(status);
    }));
    if !request.is_null() {
        // SAFETY: `request` is the library-allocated request handle for this
        // completed operation; freeing it inside the completion callback is
        // the documented pattern.
        unsafe { sys::ucp_request_free(request) };
    }
}

/// What one completed RMA operation hands to the main loop: the region whose
/// in-flight count it was holding, and its entry in [`WorkerState::rma_ops`].
type RmaCompletion = (u64, u64);

/// The callback-to-main-loop queue of [`RmaCompletion`]s.
type RmaCompletions = Vec<RmaCompletion>;

/// Completion-owned state of one posted `ucp_get_nbx`.
///
/// Rides `user_data` as `Arc::into_raw`, exactly like [`OpState`]. Unlike a
/// frame send it owns two extra things: the single-use `ucp_rkey_h` unpacked
/// immediately before the post, and the caller's reply channel.
///
/// Two `Arc`s exist while the operation is live — one leaked into `user_data`,
/// one in [`WorkerState::rma_ops`] so teardown can find a survivor whose
/// callback will never run. The reply is therefore behind a `take`-able slot
/// rather than owned outright: whichever of the two resolves it first wins, and
/// the other finds `None`. That is what makes double-resolution unrepresentable.
struct RmaOpState {
    /// The unpacked `ucp_rkey_h` as `usize`. Raw handles are not `Send`, and
    /// this one never leaves the progress thread — the `usize` records that.
    ///
    /// Take-able for the same reason the reply is: `post_get` keeps defensive
    /// reclaim arms for a UCX that ignores `FLAG_NO_IMM_CMPL`, and if one of
    /// those ever fired alongside the callback, a plain field would be destroyed
    /// twice. Taking makes the second attempt find `None`.
    rkey: Mutex<Option<usize>>,
    /// Region whose in-flight count this operation holds.
    region_id: u64,
    /// Identifies this operation in [`WorkerState::rma_ops`].
    op_id: u64,
    reply: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), RmaError>>>>,
    /// The worker-wide count teardown drains.
    inflight: Arc<AtomicUsize>,
    /// Decremented at the single `ucp_rkey_destroy` call site below.
    live_rkeys: Arc<AtomicI64>,
    /// Handoff to the main loop; see the module docs.
    rma_completions: Arc<Mutex<RmaCompletions>>,
}

impl RmaOpState {
    /// Runs exactly once per posted operation, on the progress thread.
    fn complete(&self, status: sys::ucs_status_t) {
        if let Some(rkey) = self.rkey.lock().unwrap_or_else(|e| e.into_inner()).take() {
            // SAFETY: `rkey` was produced by `ucp_ep_rkey_unpack` for this
            // operation and has just been taken out of its only slot, so this is
            // the one and only destroy. `ucp_rkey_destroy` dereferences no
            // endpoint and returns the descriptor to the worker's mpool, both of
            // which outlive this call — see the module docs for the source facts.
            unsafe { sys::ucp_rkey_destroy(rkey as sys::ucp_rkey_h) };
            self.live_rkeys.fetch_sub(1, Ordering::Relaxed);
        }
        self.resolve(if status == sys::ucs_status_t_UCS_OK {
            Ok(())
        } else {
            Err(RmaError::Ucx {
                status_name: status_string(status),
            })
        });
        self.rma_completions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((self.region_id, self.op_id));
    }

    /// Answer the caller, at most once. Also used by teardown for operations
    /// whose completion callback will never run.
    fn resolve(&self, result: Result<(), RmaError>) {
        if let Some(reply) = self.reply.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let _ = reply.send(result);
        }
    }
}

/// The `ucp_send_nbx_callback_t` for RMA operations.
///
/// SAFETY contract: `user_data` is `Arc::into_raw(Arc<RmaOpState>)` placed
/// there by [`WorkerState::post_get`], and this trampoline is the only
/// consumer. Runs on the progress thread during `ucp_worker_progress`; a panic
/// must not unwind into C, hence `catch_unwind`.
unsafe extern "C" fn rma_trampoline(
    request: *mut c_void,
    status: sys::ucs_status_t,
    user_data: *mut c_void,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: see contract above — exactly one reclaim per posted op. The
        // registry in `WorkerState::rma_ops` holds the *other* Arc; dropping
        // this one here is what balances the `Arc::into_raw` at post time.
        let state = unsafe { Arc::from_raw(user_data as *const RmaOpState) };
        state.inflight.fetch_sub(1, Ordering::AcqRel);
        state.complete(status);
    }));
    if !request.is_null() {
        // SAFETY: `request` is the library-allocated request handle for this
        // completed operation; freeing it inside the completion callback is
        // the documented pattern.
        unsafe { sys::ucp_request_free(request) };
    }
}

// ---------------------------------------------------------------------------
// Inbound: AM recv trampoline
// ---------------------------------------------------------------------------

/// Shared context for all AM recv handlers.
struct RecvShared {
    adapter: TransportAdapter,
    ring_tx: flume::Sender<Cmd>,
    pending_pings: Arc<DashMap<u64, tokio::sync::oneshot::Sender<()>>>,
}

/// Per-handler argument: the shared context plus which AM kind this id is.
struct RecvArg {
    shared: Arc<RecvShared>,
    kind: u8,
}

/// The `ucp_am_recv_callback_t` registered for each of velo's AM ids.
///
/// v1 copies both header and payload out of UCX's buffers inside the callback
/// and always returns `UCS_OK` (never taking descriptor ownership): shutdown
/// then never has to wait on descriptors held by downstream consumers, and a
/// slow consumer cannot starve UCX's receive pool. A rendezvous-mode receive
/// can only mean a non-velo sender or version skew — every velo send pins
/// `UCP_AM_SEND_FLAG_EAGER` — and is refused with an error status, which
/// completes the *sender* with that status instead of silently dropping.
unsafe extern "C" fn recv_trampoline(
    arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const sys::ucp_am_recv_param_t,
) -> sys::ucs_status_t {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `arg` is the leaked `RecvArg` this handler was registered
        // with; it lives for the worker's lifetime.
        let ra = unsafe { &*(arg as *const RecvArg) };
        // SAFETY: `param` is valid for the duration of the callback.
        let p = unsafe { &*param };

        if p.recv_attr & sys::ucp_am_recv_attr_t_UCP_AM_RECV_ATTR_FLAG_RNDV as u64 != 0 {
            // Protocol violation (velo pins EAGER). Refusing with an error
            // completes the sender's request with this status.
            warn!("ucx: rejecting rendezvous-mode AM (kind {})", ra.kind);
            return sys::ucs_status_t_UCS_ERR_UNSUPPORTED;
        }

        // SAFETY: header/data are valid for header_length/length bytes for
        // the duration of the callback; we copy before returning.
        let header = if header_length == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(unsafe {
                std::slice::from_raw_parts(header as *const u8, header_length)
            })
        };
        let payload = if length == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data as *const u8, length) })
        };

        match ra.kind {
            AM_KIND_PING => {
                if header.len() >= 8 && !p.reply_ep.is_null() {
                    let token = u64::from_le_bytes(header[..8].try_into().unwrap());
                    let _ = ra.shared.ring_tx.try_send(Cmd::PongTo {
                        reply_ep: p.reply_ep as usize,
                        token,
                    });
                }
            }
            AM_KIND_PONG => {
                if header.len() >= 8 {
                    let token = u64::from_le_bytes(header[..8].try_into().unwrap());
                    if let Some((_, tx)) = ra.shared.pending_pings.remove(&token) {
                        let _ = tx.send(());
                    }
                }
            }
            kind => {
                let adapter = &ra.shared.adapter;
                match MessageType::from_u8(kind) {
                    Some(MessageType::Message) => {
                        // The drain gate: `admit_message` acquires the
                        // in-flight guard before it re-reads the draining flag
                        // and ships the guard with the frame, so a queued
                        // message is work `wait_for_drain` can see. Sync, so
                        // it is callable from this AM callback.
                        match adapter.admit_message(header, payload) {
                            AdmitOutcome::Admitted => {}
                            AdmitOutcome::Draining { header, .. } => {
                                // Echo the header back as ShuttingDown, like the
                                // TCP listener's per-frame drain gate.
                                if !p.reply_ep.is_null()
                                    && ra
                                        .shared
                                        .ring_tx
                                        .try_send(Cmd::ShuttingDownTo {
                                            reply_ep: p.reply_ep as usize,
                                            header,
                                        })
                                        .is_err()
                                {
                                    // Best-effort: a full ring drops the echo and
                                    // the requester waits out its own timeout.
                                    debug!("ucx: drain echo dropped (ring full)");
                                }
                            }
                            AdmitOutcome::Disconnected { .. } => {
                                debug!("ucx: inbound Message dropped (receiver gone)");
                            }
                        }
                    }
                    Some(MessageType::Response) => {
                        let _ = adapter.response_stream.send((header, payload));
                    }
                    Some(MessageType::ShuttingDown) => {
                        let _ = adapter.shutdown_stream.send((header, payload));
                    }
                    Some(MessageType::Ack) | Some(MessageType::Event) => {
                        let _ = adapter.event_stream.send((header, payload));
                    }
                    None => {
                        warn!("ucx: inbound AM with unknown kind {kind}");
                    }
                }
            }
        }
        sys::ucs_status_t_UCS_OK
    }));
    result.unwrap_or_else(|_| {
        warn!("ucx: panic in AM recv handler (message dropped)");
        sys::ucs_status_t_UCS_OK
    })
}

// ---------------------------------------------------------------------------
// Endpoint error handler
// ---------------------------------------------------------------------------

struct ErrArg {
    peer: InstanceId,
    failed: Arc<DashMap<InstanceId, ()>>,
    err_events: Arc<Mutex<Vec<InstanceId>>>,
}

/// `ucp_err_handler_cb_t`: fires on the progress thread when an endpoint hits
/// a transport-level error (requires `UCP_ERR_HANDLING_MODE_PEER`). The ep is
/// unusable after return; actual teardown happens in the main loop, not here.
unsafe extern "C" fn err_trampoline(
    arg: *mut c_void,
    _ep: sys::ucp_ep_h,
    status: sys::ucs_status_t,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `arg` is the leaked per-ep `ErrArg`, freed when the ep
        // entry is destroyed.
        let ea = unsafe { &*(arg as *const ErrArg) };
        warn!(
            "ucx: endpoint to {} failed: {}",
            ea.peer,
            status_string(status)
        );
        ea.failed.insert(ea.peer, ());
        ea.err_events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(ea.peer);
    }));
}

// ---------------------------------------------------------------------------
// The progress thread
// ---------------------------------------------------------------------------

struct EpEntry {
    ep: sys::ucp_ep_h,
    /// Leaked `ErrArg` reclaimed when the entry is destroyed.
    err_arg: *mut ErrArg,
    /// The peer incarnation this endpoint was created from; compared against
    /// the peers map after re-registrations.
    incarnation: u64,
}

/// One `ucp_mem_map`ed region, owned by the progress thread.
struct RegionEntry {
    memh: sys::ucp_mem_h,
    /// The range the submitter asked for. This — not the effective range — is
    /// what a caller-supplied offset may address, so a caller can never name a
    /// byte UCX pinned but the process does not own.
    requested_addr: u64,
    requested_len: u64,
    /// What `ucp_mem_query` reports UCX actually pinned; contains the requested
    /// range and may extend past it in both directions.
    effective_addr: u64,
    effective_len: u64,
    /// Local RMA operations posted against this region and not yet completed.
    /// Plain `usize`: only the progress thread ever reads or writes it.
    inflight: usize,
    /// Callers waiting for `inflight` to reach zero so the region can be
    /// unmapped. A non-empty list also refuses new GETs into the region.
    ///
    /// A list rather than a single slot because a cancelled `unmap_region`
    /// leaves the unmap in progress with nobody listening: a retry must attach
    /// to it, not be told the still-mapped, still-DMA-active region does not
    /// exist. All waiters resolve with the same outcome.
    pending_unmap: Vec<tokio::sync::oneshot::Sender<Result<(), RmaError>>>,
}

/// A validated GET, one `ucp_get_nbx` away from being posted.
struct PreparedGet {
    ep: sys::ucp_ep_h,
    /// Unpacked for this operation only; destroyed at its completion.
    rkey: sys::ucp_rkey_h,
    memh: sys::ucp_mem_h,
    local_addr: u64,
    remote_addr: u64,
    len: usize,
    region_id: u64,
}

struct WorkerState {
    context: sys::ucp_context_h,
    worker: sys::ucp_worker_h,
    efd: c_int,
    eps: HashMap<InstanceId, EpEntry>,
    err_events: Arc<Mutex<Vec<InstanceId>>>,
    /// Locally registered regions, keyed by the id the submitter minted.
    regions: HashMap<u64, RegionEntry>,
    /// Every posted RMA operation that has not completed, so teardown can
    /// answer the callers of operations UCX will never complete. Entries are
    /// removed by [`WorkerState::drain_rma_completions`].
    rma_ops: HashMap<u64, Arc<RmaOpState>>,
    /// Mints [`RmaOpState::op_id`]. Progress-thread-local.
    next_op_id: u64,
    /// Completions pushed by [`rma_trampoline`] and applied by
    /// [`WorkerState::drain_rma_completions`]. See the module docs: a
    /// completion callback must never touch the ring, and it cannot reach
    /// `WorkerState`, so this shared vector is the handoff — the same shape as
    /// `err_events`.
    rma_completions: Arc<Mutex<RmaCompletions>>,
    /// Last observed value of `WorkerShared::reg_epoch`.
    seen_reg_epoch: u64,
    shared: Arc<WorkerShared>,
    config: UcxConfig,
    /// Retained so the AM handler `arg` pointers stay valid for the worker's
    /// lifetime: UCX holds raw pointers into these allocations, and `Arc`
    /// gives each `RecvArg` the stable heap address that requires.
    _recv_args: Vec<Arc<RecvArg>>,
    /// Endpoints superseded mid-drain (see `ensure_ep`), closed at the next
    /// safe point by `close_parked`.
    parked_for_close: Vec<EpEntry>,
    /// Close requests from FORCE-mode endpoint closes that did not complete
    /// inline. They are polled (and freed) from the main loop instead of
    /// being waited on with a nested `ucp_worker_progress` — see
    /// `close_ep_raw` for why that matters.
    pending_closes: Vec<sys::ucs_status_ptr_t>,
}

/// Entry point of the dedicated progress thread.
pub(crate) fn worker_main(args: WorkerArgs) {
    let WorkerArgs {
        config,
        ring_rx,
        shared,
        adapter,
        startup,
    } = args;

    let state = match unsafe { init_ucx(&config, &shared, &adapter) } {
        Ok((state, out)) => {
            let _ = startup.send(Ok(out));
            state
        }
        Err(e) => {
            let _ = startup.send(Err(e));
            return;
        }
    };

    run_loop(state, ring_rx);
}

/// All UCX object creation, in one place. Runs once, on the progress thread.
unsafe fn init_ucx(
    config: &UcxConfig,
    shared: &Arc<WorkerShared>,
    adapter: &TransportAdapter,
) -> anyhow::Result<(WorkerState, StartupOut)> {
    unsafe {
        // -- context ---------------------------------------------------------
        let mut ucp_cfg: *mut sys::ucp_config_t = std::ptr::null_mut();
        let st = sys::ucp_config_read(std::ptr::null(), std::ptr::null(), &mut ucp_cfg);
        anyhow::ensure!(
            st == sys::ucs_status_t_UCS_OK,
            "ucp_config_read: {}",
            status_string(st)
        );
        // Operator-set UCX_* env always wins; these are velo's defaults.
        // MEM_EVENTS/RCACHE control UCM's process-global malloc/mmap hooks —
        // the messaging path never uses the registration cache, so keep the
        // hooks out of the process unless the operator asks for them.
        for (key, value) in [("RCACHE_ENABLE", "n"), ("MEM_EVENTS", "n")] {
            if std::env::var_os(format!("UCX_{key}")).is_none() {
                let k = std::ffi::CString::new(key).unwrap();
                let v = std::ffi::CString::new(value).unwrap();
                let _ = sys::ucp_config_modify(ucp_cfg, k.as_ptr(), v.as_ptr());
            }
        }
        if let Some(tls) = &config.tls
            && std::env::var_os("UCX_TLS").is_none()
        {
            let k = std::ffi::CString::new("TLS").unwrap();
            let v = std::ffi::CString::new(tls.as_str()).unwrap();
            let st = sys::ucp_config_modify(ucp_cfg, k.as_ptr(), v.as_ptr());
            anyhow::ensure!(
                st == sys::ucs_status_t_UCS_OK,
                "ucp_config_modify(TLS={tls}): {}",
                status_string(st)
            );
        }
        if let Some(devices) = &config.net_devices
            && std::env::var_os("UCX_NET_DEVICES").is_none()
        {
            let k = std::ffi::CString::new("NET_DEVICES").unwrap();
            let v = std::ffi::CString::new(devices.as_str()).unwrap();
            let st = sys::ucp_config_modify(ucp_cfg, k.as_ptr(), v.as_ptr());
            anyhow::ensure!(
                st == sys::ucs_status_t_UCS_OK,
                "ucp_config_modify(NET_DEVICES={devices}): {}",
                status_string(st)
            );
        }

        let mut params: sys::ucp_params_t = MaybeUninit::zeroed().assume_init();
        params.field_mask = (sys::ucp_params_field_UCP_PARAM_FIELD_FEATURES
            | sys::ucp_params_field_UCP_PARAM_FIELD_MT_WORKERS_SHARED)
            as u64;
        // RMA is requested now so the rendezvous GET path (P2) shares this
        // context; WAKEUP is mandatory for the efd/arm/signal protocol.
        params.features = (sys::ucp_feature_UCP_FEATURE_AM
            | sys::ucp_feature_UCP_FEATURE_RMA
            | sys::ucp_feature_UCP_FEATURE_WAKEUP) as u64;
        params.mt_workers_shared = 1;

        let mut context: sys::ucp_context_h = std::ptr::null_mut();
        let st = sys::ucp_init_version(
            sys::UCP_API_MAJOR,
            sys::UCP_API_MINOR,
            &params,
            ucp_cfg,
            &mut context,
        );
        sys::ucp_config_release(ucp_cfg);
        anyhow::ensure!(
            st == sys::ucs_status_t_UCS_OK,
            "ucp_init: {} (if this is InvalidParam with no UCX log output, a \
             constructor reference is missing — see ucx-rs)",
            status_string(st)
        );

        // -- worker ----------------------------------------------------------
        let mut wparams: sys::ucp_worker_params_t = MaybeUninit::zeroed().assume_init();
        wparams.field_mask = sys::ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
        wparams.thread_mode = sys::ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE;

        let mut worker: sys::ucp_worker_h = std::ptr::null_mut();
        let st = sys::ucp_worker_create(context, &wparams, &mut worker);
        if st != sys::ucs_status_t_UCS_OK {
            sys::ucp_cleanup(context);
            anyhow::bail!("ucp_worker_create: {}", status_string(st));
        }

        // -- AM handlers -----------------------------------------------------
        let recv_shared = Arc::new(RecvShared {
            adapter: adapter.clone(),
            ring_tx: shared.ring_tx.clone(),
            pending_pings: Arc::clone(&shared.pending_pings),
        });
        let mut recv_args = Vec::with_capacity(AM_KIND_COUNT as usize);
        for kind in 0..AM_KIND_COUNT {
            let arg = Arc::new(RecvArg {
                shared: Arc::clone(&recv_shared),
                kind,
            });
            let mut hp: sys::ucp_am_handler_param_t = MaybeUninit::zeroed().assume_init();
            hp.field_mask = (sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ID
                | sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_CB
                | sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ARG)
                as u64;
            hp.id = (AM_ID_BASE as u32) + kind as u32;
            hp.cb = Some(recv_trampoline);
            hp.arg = Arc::as_ptr(&arg) as *mut c_void;
            let st = sys::ucp_worker_set_am_recv_handler(worker, &hp);
            if st != sys::ucs_status_t_UCS_OK {
                sys::ucp_worker_destroy(worker);
                sys::ucp_cleanup(context);
                anyhow::bail!("set_am_recv_handler(kind {kind}): {}", status_string(st));
            }
            recv_args.push(arg);
        }

        // -- address + limits ------------------------------------------------
        let mut attr: sys::ucp_worker_attr_t = MaybeUninit::zeroed().assume_init();
        attr.field_mask = (sys::ucp_worker_attr_field_UCP_WORKER_ATTR_FIELD_ADDRESS
            | sys::ucp_worker_attr_field_UCP_WORKER_ATTR_FIELD_MAX_AM_HEADER)
            as u64;
        let st = sys::ucp_worker_query(worker, &mut attr);
        if st != sys::ucs_status_t_UCS_OK {
            sys::ucp_worker_destroy(worker);
            sys::ucp_cleanup(context);
            anyhow::bail!("ucp_worker_query: {}", status_string(st));
        }
        let worker_addr =
            std::slice::from_raw_parts(attr.address as *const u8, attr.address_length).to_vec();
        sys::ucp_worker_release_address(worker, attr.address);
        let max_am_header = attr.max_am_header;

        let mut efd: c_int = -1;
        let st = sys::ucp_worker_get_efd(worker, &mut efd);
        if st != sys::ucs_status_t_UCS_OK {
            sys::ucp_worker_destroy(worker);
            sys::ucp_cleanup(context);
            anyhow::bail!("ucp_worker_get_efd: {}", status_string(st));
        }

        shared.doorbell.install(worker);

        Ok((
            WorkerState {
                context,
                worker,
                efd,
                eps: HashMap::new(),
                err_events: Arc::new(Mutex::new(Vec::new())),
                regions: HashMap::new(),
                rma_ops: HashMap::new(),
                next_op_id: 1,
                rma_completions: Arc::new(Mutex::new(Vec::new())),
                seen_reg_epoch: shared.reg_epoch.load(Ordering::Acquire),
                shared: Arc::clone(shared),
                config: config.clone(),
                _recv_args: recv_args,
                parked_for_close: Vec::new(),
                pending_closes: Vec::new(),
            },
            StartupOut {
                worker_addr,
                max_am_header,
            },
        ))
    }
}

/// Drain up to `budget` commands. Returns `(observed_empty, keep_running)`.
fn drain_ring(
    state: &mut WorkerState,
    ring_rx: &flume::Receiver<Cmd>,
    budget: usize,
    last_activity: &mut Instant,
) -> (bool, bool) {
    let mut drained = 0;
    while drained < budget {
        match ring_rx.try_recv() {
            Ok(cmd) => {
                drained += 1;
                *last_activity = Instant::now();
                // A panicking user `on_error` handler must not unwind past the
                // loop and skip teardown.
                let cont = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.handle_cmd(cmd)
                }))
                .unwrap_or_else(|_| {
                    warn!("ucx: panic while handling a command (continuing)");
                    true
                });
                if !cont {
                    return (false, false);
                }
            }
            Err(flume::TryRecvError::Empty) => return (true, true),
            Err(flume::TryRecvError::Disconnected) => return (true, false),
        }
    }
    (false, true)
}

fn run_loop(mut state: WorkerState, ring_rx: flume::Receiver<Cmd>) {
    const DRAIN_BUDGET: usize = 64;
    /// Backstop park timeout: a lost doorbell costs at most this much latency.
    const PARK_MS: c_int = 100;

    let spin_window = Duration::from_micros(state.config.spin_us);
    // Post-progress drain bound: enough to empty a full ring plus a burst of
    // racing submitters, without risking an unbounded loop under saturation.
    let flush_budget = state.config.channel_capacity + DRAIN_BUDGET;
    let mut last_activity = Instant::now();

    'outer: loop {
        if state.shared.shutdown_requested.load(Ordering::Acquire) {
            break 'outer;
        }

        // -- drain the ring --------------------------------------------------
        let (_, keep_running) = drain_ring(&mut state, &ring_rx, DRAIN_BUDGET, &mut last_activity);
        if !keep_running {
            break 'outer;
        }

        // -- progress to quiescence -----------------------------------------
        // SAFETY: worker owned by this thread.
        while unsafe { sys::ucp_worker_progress(state.worker) } != 0 {
            last_activity = Instant::now();
        }

        // -- flush, then reap -------------------------------------------------
        // ORDERING INVARIANT (use-after-free guard): `Cmd::PongTo` and
        // `Cmd::ShuttingDownTo` carry raw `ucp_ep_h` pointers captured by the
        // recv trampoline, i.e. they are enqueued ONLY from AM callbacks,
        // which run ONLY inside `ucp_worker_progress` on this thread. The two
        // paths that free endpoints (`revalidate_eps`, `reap_failed_eps`)
        // therefore run only (a) after the ring has been drained to empty
        // following the progress call above, and (b) via FORCE closes that
        // never call `ucp_worker_progress` themselves (`close_ep_raw` defers
        // any close request to `poll_pending_closes`). Together these make
        // "a reply command exists for an endpoint that has been freed"
        // unreachable. If the ring cannot be emptied (sustained saturation),
        // closing is postponed; a stale-but-unclosed endpoint stays valid and
        // merely fails posts, which is safe.
        let (observed_empty, keep_running) =
            drain_ring(&mut state, &ring_rx, flush_budget, &mut last_activity);
        if !keep_running {
            break 'outer;
        }
        // Deliberately NOT gated on `observed_empty` — see the RMA ordering
        // invariants in the module docs.
        state.drain_rma_completions();
        if observed_empty {
            // All three close endpoints; all are only safe here (see above).
            state.close_parked();
            state.revalidate_eps();
            state.reap_failed_eps();
        }
        state.poll_pending_closes();

        if !ring_rx.is_empty() {
            continue;
        }

        // -- adaptive spin ----------------------------------------------------
        if last_activity.elapsed() < spin_window {
            std::hint::spin_loop();
            continue;
        }

        // -- arm + park -------------------------------------------------------
        state.shared.doorbell.arm();
        if !ring_rx.is_empty() {
            state.shared.doorbell.disarm();
            continue;
        }
        // SAFETY: worker owned by this thread.
        let st = unsafe { sys::ucp_worker_arm(state.worker) };
        if st == sys::ucs_status_t_UCS_ERR_BUSY {
            state.shared.doorbell.disarm();
            continue;
        }
        if st != sys::ucs_status_t_UCS_OK {
            warn!("ucx: ucp_worker_arm: {}", status_string(st));
            state.shared.doorbell.disarm();
            continue;
        }
        let mut pfd = nix::libc::pollfd {
            fd: state.efd,
            events: nix::libc::POLLIN,
            revents: 0,
        };
        // SAFETY: plain poll(2) on a valid fd; EINTR treated as a wakeup.
        unsafe { nix::libc::poll(&mut pfd, 1, PARK_MS) };
        state.shared.doorbell.disarm();
        last_activity = Instant::now();
    }

    state.teardown(ring_rx);
}

impl WorkerState {
    /// Returns false when the loop should exit.
    fn handle_cmd(&mut self, cmd: Cmd) -> bool {
        match cmd {
            Cmd::Send(task) => {
                match self.ensure_ep(task.peer) {
                    Ok(ep) => {
                        let kind = task.msg_type.as_u8();
                        // Message-type frames carry the REPLY flag so the
                        // receiver's drain gate can echo ShuttingDown without
                        // having registered us.
                        let reply = matches!(task.msg_type, MessageType::Message);
                        let op = Arc::new(OpState {
                            kind: OpKind::Frame {
                                header: task.header.clone(),
                                payload: task.payload.clone(),
                                on_error: task.on_error,
                            },
                            inflight: Arc::clone(&self.shared.inflight_ops),
                        });
                        self.post_am(ep, kind, task.header, task.payload, reply, op);
                    }
                    Err(e) => task.fail(format!("ucx endpoint unavailable: {e}")),
                }
            }
            Cmd::Ping { peer, token } => {
                match self.ensure_ep(peer) {
                    Ok(ep) => {
                        let header = Bytes::copy_from_slice(&token.to_le_bytes());
                        let op = Arc::new(OpState {
                            kind: OpKind::Control {
                                _hold: header.clone(),
                            },
                            inflight: Arc::clone(&self.shared.inflight_ops),
                        });
                        self.post_am(ep, AM_KIND_PING, header, Bytes::new(), true, op);
                    }
                    Err(_) => {
                        // No endpoint can be created for this peer: drop the
                        // pending entry, which closes the oneshot and makes
                        // `check_health` resolve immediately with
                        // `ConnectionFailed` instead of waiting out its
                        // deadline.
                        self.shared.pending_pings.remove(&token);
                    }
                }
            }
            Cmd::PongTo { reply_ep, token } => {
                let header = Bytes::copy_from_slice(&token.to_le_bytes());
                let op = Arc::new(OpState {
                    kind: OpKind::Control {
                        _hold: header.clone(),
                    },
                    inflight: Arc::clone(&self.shared.inflight_ops),
                });
                self.post_am(
                    reply_ep as sys::ucp_ep_h,
                    AM_KIND_PONG,
                    header,
                    Bytes::new(),
                    false,
                    op,
                );
            }
            Cmd::ShuttingDownTo { reply_ep, header } => {
                let op = Arc::new(OpState {
                    kind: OpKind::Control {
                        _hold: header.clone(),
                    },
                    inflight: Arc::clone(&self.shared.inflight_ops),
                });
                self.post_am(
                    reply_ep as sys::ucp_ep_h,
                    MessageType::ShuttingDown.as_u8(),
                    header,
                    Bytes::new(),
                    false,
                    op,
                );
            }
            Cmd::MapRegion {
                ptr,
                len,
                region_id,
                reply,
            } => {
                // `send` hands the value back when the receiver is already gone,
                // which is the only signal that the caller's future was dropped.
                // The region id died with it, so nobody can ever unmap this —
                // roll it back here rather than pin the caller's memory forever.
                if let Err(Ok(orphan)) = reply.send(self.map_region(ptr, len, region_id)) {
                    debug!(
                        "ucx: rolling back region {} (map_region caller went away)",
                        orphan.region_id
                    );
                    if let Some(entry) = self.regions.remove(&orphan.region_id) {
                        let _ = self.unmap_entry(entry);
                    }
                }
            }
            Cmd::UnmapRegion { region_id, reply } => self.unmap_region(region_id, reply),
            Cmd::RmaGet { req, reply } => self.rma_get(req, reply),
            Cmd::Shutdown => return false,
        }
        true
    }

    /// Post one AM. Consumes `op` per the three-exit discipline (module docs).
    fn post_am(
        &mut self,
        ep: sys::ucp_ep_h,
        kind: u8,
        header: Bytes,
        payload: Bytes,
        reply: bool,
        op: Arc<OpState>,
    ) {
        self.shared.inflight_ops.fetch_add(1, Ordering::AcqRel);
        let user_data = Arc::into_raw(op) as *mut c_void;

        // SAFETY: header/payload are owned by the OpState referenced from
        // user_data; they outlive the operation by construction.
        let mut param: sys::ucp_request_param_t = unsafe { MaybeUninit::zeroed().assume_init() };
        param.op_attr_mask = sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_CALLBACK
            | sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_USER_DATA
            | sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_FLAGS;
        param.flags = sys::ucp_send_am_flags_UCP_AM_SEND_FLAG_EAGER
            | sys::ucp_send_am_flags_UCP_AM_SEND_FLAG_COPY_HEADER
            | if reply {
                sys::ucp_send_am_flags_UCP_AM_SEND_FLAG_REPLY
            } else {
                0
            };
        param.cb.send = Some(send_trampoline);
        param.user_data = user_data;

        // SAFETY: ep is a live endpoint on this worker; buffers per above.
        let ptr = unsafe {
            sys::ucp_am_send_nbx(
                ep,
                (AM_ID_BASE as u32) + kind as u32,
                header.as_ptr() as *const c_void,
                header.len(),
                payload.as_ptr() as *const c_void,
                payload.len(),
                &param,
            )
        };

        match decode_status_ptr(ptr) {
            Ok(Some(_request)) => {
                // Exit 2: the trampoline owns the Arc and frees the request.
            }
            Ok(None) => {
                // Exit 1: completed inline; the callback is ignored even
                // though it was set — reclaim and drop.
                // SAFETY: reclaiming the Arc we leaked above; UCX will not
                // touch user_data for an inline-completed op.
                let state = unsafe { Arc::from_raw(user_data as *const OpState) };
                state.inflight.fetch_sub(1, Ordering::AcqRel);
                drop(state);
            }
            Err(status) => {
                // Exit 3: failed synchronously; no callback. `complete` runs
                // a user error handler — guard against unwinding.
                // SAFETY: as above.
                let state = unsafe { Arc::from_raw(user_data as *const OpState) };
                state.inflight.fetch_sub(1, Ordering::AcqRel);
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.complete(status)
                }));
            }
        }
    }

    fn ensure_ep(&mut self, peer: InstanceId) -> anyhow::Result<sys::ucp_ep_h> {
        let blob = self
            .shared
            .peers
            .get(&peer)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("peer {peer} not registered"))?;
        if let Some(entry) = self.eps.get(&peer) {
            if entry.incarnation == blob.incarnation {
                return Ok(entry.ep);
            }
            // The peer was re-registered with a new incarnation. Sends must
            // switch to a fresh endpoint NOW (the old incarnation may still be
            // reachable), but the old endpoint cannot be closed here: we are
            // inside a ring drain, and closing frees memory that a queued
            // reply command may still reference. Park it for the next safe
            // point (`close_parked`, run only after the ring is observed
            // empty).
            if let Some(old) = self.eps.remove(&peer) {
                debug!("ucx: peer {peer} re-registered; replacing endpoint");
                self.parked_for_close.push(old);
            }
        }

        let err_arg = Box::into_raw(Box::new(ErrArg {
            peer,
            failed: Arc::clone(&self.shared.failed_peers),
            err_events: Arc::clone(&self.err_events),
        }));

        // SAFETY: worker owned by this thread; the address bytes live across
        // the call (ucp_ep_create copies what it needs).
        let ep = unsafe {
            let mut params: sys::ucp_ep_params_t = MaybeUninit::zeroed().assume_init();
            params.field_mask = (sys::ucp_ep_params_field_UCP_EP_PARAM_FIELD_REMOTE_ADDRESS
                | sys::ucp_ep_params_field_UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE
                | sys::ucp_ep_params_field_UCP_EP_PARAM_FIELD_ERR_HANDLER)
                as u64;
            params.address = blob.worker_addr.as_ptr() as *const sys::ucp_address_t;
            params.err_mode = sys::ucp_err_handling_mode_t_UCP_ERR_HANDLING_MODE_PEER;
            params.err_handler = sys::ucp_err_handler_t {
                cb: Some(err_trampoline),
                arg: err_arg as *mut c_void,
            };

            let mut ep: sys::ucp_ep_h = std::ptr::null_mut();
            let st = sys::ucp_ep_create(self.worker, &params, &mut ep);
            if st != sys::ucs_status_t_UCS_OK {
                drop(Box::from_raw(err_arg));
                anyhow::bail!("ucp_ep_create: {}", status_string(st));
            }
            ep
        };

        self.shared.failed_peers.remove(&peer);
        self.eps.insert(
            peer,
            EpEntry {
                ep,
                err_arg,
                incarnation: blob.incarnation,
            },
        );
        debug!("ucx: created endpoint to {peer}");
        Ok(ep)
    }

    /// Close endpoints that `ensure_ep` replaced mid-drain.
    fn close_parked(&mut self) {
        for entry in std::mem::take(&mut self.parked_for_close) {
            self.close_ep(entry, true);
        }
    }

    /// Drop cached endpoints whose peer was re-registered with a different
    /// incarnation (or deregistered) since the endpoint was created. Runs on
    /// the cheap path only when `reg_epoch` moved.
    fn revalidate_eps(&mut self) {
        let epoch = self.shared.reg_epoch.load(Ordering::Acquire);
        if epoch == self.seen_reg_epoch {
            return;
        }
        self.seen_reg_epoch = epoch;
        let stale: Vec<InstanceId> = self
            .eps
            .iter()
            .filter(|(peer, entry)| match self.shared.peers.get(peer) {
                Some(blob) => blob.value().incarnation != entry.incarnation,
                None => true,
            })
            .map(|(peer, _)| *peer)
            .collect();
        for peer in stale {
            if let Some(entry) = self.eps.remove(&peer) {
                debug!("ucx: dropping stale endpoint to re-registered peer {peer}");
                // The old incarnation is gone (or replaced); FORCE completes
                // its in-flight ops with CANCELED, driving our callbacks.
                self.close_ep(entry, true);
            }
        }
    }

    /// Destroy endpoints whose error handler fired since the last pass.
    fn reap_failed_eps(&mut self) {
        let peers: Vec<InstanceId> = {
            let mut guard = self.err_events.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        for peer in peers {
            if let Some(entry) = self.eps.remove(&peer) {
                // The ep already failed: FORCE-close is the only close that
                // cannot block on the dead peer.
                self.close_ep(entry, true);
            }
        }
    }

    /// Close one endpoint. `force` completes outstanding ops with CANCELED
    /// (driving our completion callbacks) instead of flushing to the peer.
    fn close_ep(&mut self, entry: EpEntry, force: bool) {
        self.close_ep_raw(entry, force)
    }

    /// Close one endpoint WITHOUT progressing the worker.
    ///
    /// This must not call `ucp_worker_progress`: progress runs AM callbacks,
    /// which can enqueue reply commands holding raw endpoint pointers, and a
    /// subsequent close in the same pass could then free one of those
    /// endpoints before the command is consumed (the use-after-free class
    /// the loop ordering exists to prevent). A close request that does not
    /// complete inline is parked in `pending_closes` and reaped by the main
    /// loop; the request does not reference the endpoint after close.
    fn close_ep_raw(&mut self, entry: EpEntry, force: bool) {
        // SAFETY: worker/ep owned by this thread; err_arg was leaked at
        // creation and UCX will not call the handler after close is issued.
        unsafe {
            let mut param: sys::ucp_request_param_t = MaybeUninit::zeroed().assume_init();
            if force {
                param.op_attr_mask = sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_FLAGS;
                param.flags = sys::ucp_ep_close_flags_t_UCP_EP_CLOSE_FLAG_FORCE;
            }
            let ptr = sys::ucp_ep_close_nbx(entry.ep, &param);
            if let Ok(Some(request)) = decode_status_ptr(ptr) {
                self.pending_closes.push(request);
            }
            drop(Box::from_raw(entry.err_arg));
        }
    }

    /// Free deferred close requests that have completed. Called from the main
    /// loop after progress; never blocks.
    fn poll_pending_closes(&mut self) {
        if self.pending_closes.is_empty() {
            return;
        }
        self.pending_closes.retain(|req| {
            // SAFETY: each entry is a live request handle owned by this thread
            // until freed here.
            let st = unsafe { sys::ucp_request_check_status(*req) };
            if st == sys::ucs_status_t_UCS_INPROGRESS {
                true
            } else {
                unsafe { sys::ucp_request_free(*req) };
                false
            }
        });
    }

    // -- RMA ---------------------------------------------------------------

    /// `ucp_mem_map` caller memory in place, then query the range UCX actually
    /// pinned and pack a remote key for it.
    fn map_region(
        &mut self,
        ptr: usize,
        len: usize,
        region_id: u64,
    ) -> Result<MappedRegion, RmaError> {
        if self.shared.shutdown_requested.load(Ordering::Acquire) {
            return Err(RmaError::ShuttingDown);
        }
        if ptr == 0 || len == 0 || ptr.checked_add(len).is_none() {
            return Err(RmaError::OutOfRange);
        }

        // SAFETY: the context is owned by this thread. ADDRESS|LENGTH without
        // ALLOCATE registers the caller's pages in place; NONBLOCK is omitted
        // so the mapping is complete when the call returns.
        let memh = unsafe {
            let mut params: sys::ucp_mem_map_params_t = MaybeUninit::zeroed().assume_init();
            params.field_mask = (sys::ucp_mem_map_params_field_UCP_MEM_MAP_PARAM_FIELD_ADDRESS
                | sys::ucp_mem_map_params_field_UCP_MEM_MAP_PARAM_FIELD_LENGTH)
                as u64;
            params.address = ptr as *mut c_void;
            params.length = len;
            let mut memh: sys::ucp_mem_h = std::ptr::null_mut();
            let st = sys::ucp_mem_map(self.context, &params, &mut memh);
            if st != sys::ucs_status_t_UCS_OK {
                return Err(RmaError::Ucx {
                    status_name: status_string(st),
                });
            }
            memh
        };

        match self.describe_region(memh, ptr as u64, len as u64, region_id) {
            Ok(region) => Ok(region),
            Err(e) => {
                // SAFETY: `memh` was just produced by `ucp_mem_map` on this
                // context and is not referenced by any region entry, so nothing
                // can be using it.
                unsafe { sys::ucp_mem_unmap(self.context, memh) };
                Err(e)
            }
        }
    }

    /// Second half of [`Self::map_region`]: query, pack, record. Split out so
    /// every failure between the map and the record unmaps `memh` exactly once.
    fn describe_region(
        &mut self,
        memh: sys::ucp_mem_h,
        requested_addr: u64,
        requested_len: u64,
        region_id: u64,
    ) -> Result<MappedRegion, RmaError> {
        // SAFETY: `memh` is a live handle from `ucp_mem_map` on this thread's
        // context; `attr` is fully initialised before the call.
        let (effective_addr, effective_len) = unsafe {
            let mut attr: sys::ucp_mem_attr_t = MaybeUninit::zeroed().assume_init();
            attr.field_mask = (sys::ucp_mem_attr_field_UCP_MEM_ATTR_FIELD_ADDRESS
                | sys::ucp_mem_attr_field_UCP_MEM_ATTR_FIELD_LENGTH)
                as u64;
            let st = sys::ucp_mem_query(memh, &mut attr);
            if st != sys::ucs_status_t_UCS_OK {
                return Err(RmaError::Ucx {
                    status_name: status_string(st),
                });
            }
            (attr.address as u64, attr.length as u64)
        };

        // UCX rounds the pinned range outward, so it must contain the request.
        // If it ever does not, every offset computation below is built on a lie
        // — refuse rather than register the region.
        let requested_end = requested_addr + requested_len;
        let effective_end = effective_addr
            .checked_add(effective_len)
            .ok_or(RmaError::OutOfRange)?;
        if effective_addr > requested_addr || effective_end < requested_end {
            warn!(
                "ucx: ucp_mem_query reported [{effective_addr:#x}, {effective_end:#x}) which does \
                 not contain the mapped range [{requested_addr:#x}, {requested_end:#x})"
            );
            return Err(RmaError::OutOfRange);
        }

        // `ucp_rkey_pack` is the only working packer: `ucp_memh_pack` without
        // `UCP_MEMH_PACK_FLAG_EXPORT` aborts the process via `ucs_fatal` (see
        // the ucx-rs consumer invariants). The buffer is copied out and released
        // immediately, so nothing UCX-allocated escapes this thread.
        // SAFETY: context and memh are owned by this thread; `buf`/`size` are
        // written by UCX and only read for `size` bytes when the call succeeds.
        let packed_rkey = unsafe {
            let mut buf: *mut c_void = std::ptr::null_mut();
            let mut size: usize = 0;
            let st = sys::ucp_rkey_pack(self.context, memh, &mut buf, &mut size);
            if st != sys::ucs_status_t_UCS_OK {
                return Err(RmaError::Ucx {
                    status_name: status_string(st),
                });
            }
            let packed = if buf.is_null() || size == 0 {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(std::slice::from_raw_parts(buf as *const u8, size))
            };
            if !buf.is_null() {
                sys::ucp_rkey_buffer_release(buf);
            }
            packed
        };
        validate_packed_rkey(&packed_rkey)?;

        self.regions.insert(
            region_id,
            RegionEntry {
                memh,
                requested_addr,
                requested_len,
                effective_addr,
                effective_len,
                inflight: 0,
                pending_unmap: Vec::new(),
            },
        );
        self.shared.live_regions.fetch_add(1, Ordering::Relaxed);
        Ok(MappedRegion {
            region_id,
            effective_addr,
            effective_len,
            packed_rkey,
        })
    }

    /// Unmap now if the region is idle, otherwise attach the caller to the
    /// waiters already queued behind its last operation.
    ///
    /// Idempotent: an id naming no region answers `Ok(())`. "Nothing is mapped
    /// under this id" is the state the caller asked for, and reporting it as an
    /// error would make a retry after a cancelled unmap indistinguishable from a
    /// use-after-free bug.
    fn unmap_region(
        &mut self,
        region_id: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), RmaError>>,
    ) {
        let Some(entry) = self.regions.get_mut(&region_id) else {
            let _ = reply.send(Ok(()));
            return;
        };
        entry.pending_unmap.push(reply);
        if entry.inflight > 0 {
            return;
        }
        let entry = self
            .regions
            .remove(&region_id)
            .expect("looked up immediately above");
        self.finish_unmap(entry);
    }

    /// Unmap an entry already out of `self.regions` and tell every waiter.
    fn finish_unmap(&self, mut entry: RegionEntry) {
        let waiters = std::mem::take(&mut entry.pending_unmap);
        let result = self.unmap_entry(entry);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// `ucp_mem_unmap` one region entry that is already out of `self.regions`.
    fn unmap_entry(&self, entry: RegionEntry) -> Result<(), RmaError> {
        // SAFETY: `memh` came from `ucp_mem_map` on this thread's context and
        // the entry has been removed from `self.regions`, so no further
        // operation can be posted against it.
        let st = unsafe { sys::ucp_mem_unmap(self.context, entry.memh) };
        self.shared.live_regions.fetch_sub(1, Ordering::Relaxed);
        if st == sys::ucs_status_t_UCS_OK {
            Ok(())
        } else {
            Err(RmaError::Ucx {
                status_name: status_string(st),
            })
        }
    }

    /// Validate a GET, unpack its rkey and post it, or answer the caller.
    fn rma_get(
        &mut self,
        req: RmaGetRequest,
        reply: tokio::sync::oneshot::Sender<Result<(), RmaError>>,
    ) {
        match self.prepare_get(&req) {
            Err(e) => {
                let _ = reply.send(Err(e));
            }
            // Zero-length: UCX treats it as a no-op, so answer without posting.
            Ok(None) => {
                let _ = reply.send(Ok(()));
            }
            Ok(Some(prepared)) => self.post_get(prepared, reply),
        }
    }

    /// Re-check everything the submitter checked, then unpack the rkey.
    ///
    /// Returns `Ok(None)` for a zero-length GET. The submit-side checks in
    /// [`RdmaEndpoint`](super::rma::RdmaEndpoint) exist to save a ring slot;
    /// these are the ones that guard UCX.
    fn prepare_get(&mut self, req: &RmaGetRequest) -> Result<Option<PreparedGet>, RmaError> {
        if self.shared.shutdown_requested.load(Ordering::Acquire) {
            return Err(RmaError::ShuttingDown);
        }
        let (memh, requested_addr, requested_len, effective_addr, effective_len, unmapping) = {
            let entry = self
                .regions
                .get(&req.local_region)
                .ok_or(RmaError::RegionNotFound)?;
            (
                entry.memh,
                entry.requested_addr,
                entry.requested_len,
                entry.effective_addr,
                entry.effective_len,
                !entry.pending_unmap.is_empty(),
            )
        };
        if unmapping {
            return Err(RmaError::RegionNotFound);
        }
        let end = req
            .local_offset
            .checked_add(req.len)
            .ok_or(RmaError::OutOfRange)?;
        if end > requested_len {
            return Err(RmaError::OutOfRange);
        }
        // The requested-range check above is what keeps a caller inside memory
        // the process owns; this one keeps the pointer inside what UCX pinned.
        //
        // Unreachable while `describe_region`'s containment check holds — the
        // requested range is a subset of the effective one, so anything that
        // passed above passes here — and therefore not covered by a test. It
        // stays because the map-time check is the only thing making it
        // unreachable: if `ucp_mem_query` ever reported a range it does not
        // pin, this is what stops a pointer built on that going to UCX. The
        // checked arithmetic is there for the same reason.
        let local_addr = requested_addr
            .checked_add(req.local_offset)
            .ok_or(RmaError::OutOfRange)?;
        let local_end = local_addr
            .checked_add(req.len)
            .ok_or(RmaError::OutOfRange)?;
        let effective_end = effective_addr
            .checked_add(effective_len)
            .ok_or(RmaError::OutOfRange)?;
        if local_addr < effective_addr || local_end > effective_end {
            return Err(RmaError::OutOfRange);
        }
        if !self.shared.peers.contains_key(&req.peer) {
            return Err(RmaError::PeerNotRegistered(req.peer));
        }
        if req.len == 0 {
            return Ok(None);
        }
        validate_packed_rkey(&req.packed_rkey)?;

        let ep = self
            .ensure_ep(req.peer)
            .map_err(|e| RmaError::EndpointUnavailable(e.to_string()))?;

        // Containment for UCX's length-free parse comes from
        // `preparse_packed_rkey` above, which has proved that every read the
        // parse will perform lands inside `packed_rkey`. The copy below is
        // belt-and-braces for a parse that somehow escapes anyway: the filler is
        // `0xFF`, which is `UCS_SYS_DEVICE_ID_UNKNOWN` and the only byte that
        // terminates the stage-2 distance walk (`ucp_rkey.c:820`). Zero filler
        // would be worse than none — that walk sets `buffer_end = UINTPTR_MAX`
        // and would consume the zeroes as 3-byte records off the stack.
        let mut unpack_buf = [0xFFu8; MAX_PACKED_RKEY + RKEY_UNPACK_PAD];
        unpack_buf[..req.packed_rkey.len()].copy_from_slice(&req.packed_rkey);

        // SAFETY: `ep` is live on this worker and the blob has been pre-parsed,
        // so UCX's walk terminates within the first `packed_rkey.len()` bytes of
        // this buffer. The `0xFF` tail bounds it even if that reasoning is ever
        // wrong: the stage-1 walk finds max-length entries that immediately
        // exceed the buffer's own accounting, and the stage-2 walk stops on the
        // first filler byte it reads.
        let rkey = unsafe {
            let mut rkey: sys::ucp_rkey_h = std::ptr::null_mut();
            let st = sys::ucp_ep_rkey_unpack(ep, unpack_buf.as_ptr() as *const c_void, &mut rkey);
            if st != sys::ucs_status_t_UCS_OK {
                return Err(RmaError::Ucx {
                    status_name: status_string(st),
                });
            }
            rkey
        };
        self.shared.live_rkeys.fetch_add(1, Ordering::Relaxed);

        Ok(Some(PreparedGet {
            ep,
            rkey,
            memh,
            local_addr,
            remote_addr: req.remote_addr,
            len: req.len as usize,
            region_id: req.local_region,
        }))
    }

    /// Post one `ucp_get_nbx`. Consumes `prepared` and `reply`.
    ///
    /// The `RmaOpState` is fully built *before* the post because the completion
    /// callback may fire from inside `ucp_get_nbx` on this same thread.
    fn post_get(
        &mut self,
        prepared: PreparedGet,
        reply: tokio::sync::oneshot::Sender<Result<(), RmaError>>,
    ) {
        self.shared.inflight_ops.fetch_add(1, Ordering::AcqRel);
        if let Some(entry) = self.regions.get_mut(&prepared.region_id) {
            entry.inflight += 1;
        }
        let op_id = self.next_op_id;
        self.next_op_id += 1;
        let op = Arc::new(RmaOpState {
            rkey: Mutex::new(Some(prepared.rkey as usize)),
            region_id: prepared.region_id,
            op_id,
            reply: Mutex::new(Some(reply)),
            inflight: Arc::clone(&self.shared.inflight_ops),
            live_rkeys: Arc::clone(&self.shared.live_rkeys),
            rma_completions: Arc::clone(&self.rma_completions),
        });
        // Registered before the post, not after: the completion callback can
        // fire from inside `ucp_get_nbx`, and "an operation UCX knows about has
        // a registry entry" has to hold at every instant in between.
        self.rma_ops.insert(op_id, Arc::clone(&op));
        let user_data = Arc::into_raw(op) as *mut c_void;

        // SAFETY: zeroed `ucp_request_param_t` is a valid "nothing set" value;
        // every field consulted is named in `op_attr_mask` below.
        let mut param: sys::ucp_request_param_t = unsafe { MaybeUninit::zeroed().assume_init() };
        // FLAG_NO_IMM_CMPL collapses the usual three-exit reclaim discipline to
        // one: the callback always fires, so the state is reclaimed there.
        param.op_attr_mask = sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_CALLBACK
            | sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_USER_DATA
            | sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_MEMH
            | sys::ucp_op_attr_t_UCP_OP_ATTR_FLAG_NO_IMM_CMPL;
        param.cb.send = Some(rma_trampoline);
        param.user_data = user_data;
        param.memh = prepared.memh;

        // SAFETY: `ep` and `rkey` are live on this worker, the destination is
        // inside the region `memh` covers (bounds-checked in `prepare_get`), and
        // the region cannot be unmapped while `inflight > 0`.
        let ptr = unsafe {
            sys::ucp_get_nbx(
                prepared.ep,
                prepared.local_addr as *mut c_void,
                prepared.len,
                prepared.remote_addr,
                prepared.rkey,
                &param,
            )
        };

        match decode_status_ptr(ptr) {
            Ok(Some(_request)) => {
                // The trampoline owns the state and frees the request.
            }
            Ok(None) => {
                // Unreachable under FLAG_NO_IMM_CMPL, and kept because the flag
                // is the only thing making it so: if a future UCX ignores it,
                // an inline completion suppresses the callback and this is the
                // path that reclaims the state.
                // SAFETY: reclaiming the Arc leaked above; UCX will not touch
                // `user_data` for an inline-completed operation.
                let state = unsafe { Arc::from_raw(user_data as *const RmaOpState) };
                state.inflight.fetch_sub(1, Ordering::AcqRel);
                state.complete(sys::ucs_status_t_UCS_OK);
            }
            Err(status) => {
                // Failed synchronously: no callback, so reclaim here.
                // SAFETY: as above.
                let state = unsafe { Arc::from_raw(user_data as *const RmaOpState) };
                state.inflight.fetch_sub(1, Ordering::AcqRel);
                state.complete(status);
            }
        }
    }

    /// Apply the region in-flight decrements handed over by [`rma_trampoline`],
    /// retire the completed operations, and resolve any unmap they release.
    fn drain_rma_completions(&mut self) {
        let completed: RmaCompletions = {
            let mut guard = self
                .rma_completions
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if guard.is_empty() {
                return;
            }
            std::mem::take(&mut *guard)
        };
        for (region_id, op_id) in completed {
            // The operation has already answered its caller; teardown no longer
            // needs to know about it.
            self.rma_ops.remove(&op_id);
            let released = match self.regions.get_mut(&region_id) {
                Some(entry) => {
                    entry.inflight = entry.inflight.saturating_sub(1);
                    entry.inflight == 0 && !entry.pending_unmap.is_empty()
                }
                None => false,
            };
            if released && let Some(entry) = self.regions.remove(&region_id) {
                self.finish_unmap(entry);
            }
        }
    }

    /// Unmap every region with nothing in flight. Run before the endpoint-close
    /// phases of teardown, per D8's "regions before endpoints" ordering.
    fn unmap_idle_regions(&mut self) {
        let idle: Vec<u64> = self
            .regions
            .iter()
            .filter(|(_, entry)| entry.inflight == 0)
            .map(|(id, _)| *id)
            .collect();
        for region_id in idle {
            if let Some(entry) = self.regions.remove(&region_id) {
                self.finish_unmap(entry);
            }
        }
    }

    /// Last resort before `ucp_worker_destroy`: unmap what is left even if
    /// operations are still counted against it.
    ///
    /// The unmap happens regardless of `inflight` — an operation whose callback
    /// never fired (the same case the `inflight_ops` warning covers) would
    /// otherwise leave the mapping to `ucp_cleanup` and park its unmap waiters
    /// forever. Waiters on a region that was still busy are told
    /// [`RmaError::ShuttingDown`], because the contract they were waiting on
    /// ("no operation is touching this memory any more") is exactly what could
    /// not be honoured.
    fn force_unmap_regions(&mut self) {
        let remaining: Vec<u64> = self.regions.keys().copied().collect();
        for region_id in remaining {
            let Some(mut entry) = self.regions.remove(&region_id) else {
                continue;
            };
            let inflight = entry.inflight;
            if inflight == 0 {
                self.finish_unmap(entry);
                continue;
            }
            warn!("ucx: force-unmapping region {region_id} with {inflight} rma op(s) in flight");
            // Deliberately not `finish_unmap`: this inlines the same
            // take-waiters / unmap / notify sequence but discards
            // `unmap_entry`'s result, because the honest answer here is
            // `ShuttingDown` whether or not `ucp_mem_unmap` succeeded — the
            // contract the waiters were promised is "nothing is touching this
            // memory any more", and that is exactly what failed. Any change to
            // `finish_unmap` has to be considered against this site too.
            let waiters = std::mem::take(&mut entry.pending_unmap);
            let _ = self.unmap_entry(entry);
            for waiter in waiters {
                let _ = waiter.send(Err(RmaError::ShuttingDown));
            }
        }
    }

    /// Answer the caller of every operation whose completion callback will never
    /// run, then let the registry go.
    ///
    /// Called once, after teardown's bounded in-flight drain has expired. An
    /// operation still here has been abandoned inside UCX's request bookkeeping;
    /// `ucp_worker_destroy` may or may not purge it, and the caller cannot be
    /// left awaiting a `oneshot` that outcome decides. Taking the reply out is
    /// safe against a late callback by construction: it finds `None` and stays
    /// silent. The `Arc`s themselves are dropped here; the one UCX still holds
    /// keeps the state alive, and its rkey is leaked with it — the same leak the
    /// `inflight_ops` warning already reports.
    fn abandon_rma_ops(&mut self) {
        for (op_id, op) in std::mem::take(&mut self.rma_ops) {
            debug!("ucx: abandoning rma op {op_id} at teardown");
            op.resolve(Err(RmaError::ShuttingDown));
        }
    }

    fn teardown(mut self, ring_rx: flume::Receiver<Cmd>) {
        debug!("ucx: progress thread tearing down");

        // Fail everything still queued — mirrors the TCP writer's drain. A
        // second pass after a short pause shrinks (not closes — see the
        // module docs) the window where a racing sender's frame lands between
        // our last try_recv and the receiver drop and is discarded silently.
        for pass in 0..2 {
            while let Ok(cmd) = ring_rx.try_recv() {
                cmd.refuse_for_shutdown();
            }
            if pass == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        drop(ring_rx);

        // Regions before endpoints (D8): everything already idle goes away now,
        // including any unmap parked behind a completion that has already
        // landed. What is still busy is handled after the in-flight drain below.
        self.drain_rma_completions();
        self.unmap_idle_regions();

        // Close every endpoint CONCURRENTLY under one global deadline, so
        // teardown latency is bounded by the slowest peer, not the sum. This
        // runs on `shutdown()`'s caller-blocking path.
        let entries: Vec<EpEntry> = {
            let eps = std::mem::take(&mut self.eps);
            let mut all: Vec<EpEntry> = eps.into_values().collect();
            all.append(&mut self.parked_for_close);
            all
        };
        // Phase A: flush-mode close, all at once.
        let mut pending: Vec<(EpEntry, sys::ucs_status_ptr_t)> = Vec::new();
        for entry in entries {
            // SAFETY: worker/ep owned by this thread.
            let ptr = unsafe {
                let param: sys::ucp_request_param_t = MaybeUninit::zeroed().assume_init();
                sys::ucp_ep_close_nbx(entry.ep, &param)
            };
            match decode_status_ptr(ptr) {
                Ok(Some(req)) => pending.push((entry, req)),
                // Completed inline or failed: the ep is gone either way.
                // SAFETY: err_arg was leaked at creation.
                _ => unsafe { drop(Box::from_raw(entry.err_arg)) },
            }
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while !pending.is_empty() && Instant::now() < deadline {
            // SAFETY: worker owned by this thread.
            unsafe { sys::ucp_worker_progress(self.worker) };
            // This progress loop is where a flushing endpoint's outstanding RMA
            // operations land. Draining here is what lets a region that goes
            // idle *during* the close still be unmapped before the FORCE phase
            // rather than waiting for the backstop.
            self.drain_rma_completions();
            pending.retain(|(entry, req)| {
                // SAFETY: req is a live close request until freed here.
                let st = unsafe { sys::ucp_request_check_status(*req) };
                if st == sys::ucs_status_t_UCS_INPROGRESS {
                    true
                } else {
                    unsafe {
                        sys::ucp_request_free(*req);
                        drop(Box::from_raw(entry.err_arg));
                    }
                    false
                }
            });
        }
        // Anything that fell idle during the flush-close goes now, still ahead
        // of the endpoints Phase B is about to abandon.
        self.unmap_idle_regions();

        // Phase B: give up on whatever did not flush in time.
        //
        // Do not read this as "and now the outstanding operations get
        // cancelled". `ucp_ep_close_nbx` returns `UCS_ERR_NOT_CONNECTED` at its
        // `UCP_EP_FLAG_CLOSED` guard (`ucp_ep.c:2221`) for an endpoint Phase A
        // already close-initiated, so the FORCE flag buys nothing here: no
        // second discard, no `CANCELED` purge, no completion callbacks. What it
        // does is free the request and the leaked `ErrArg`. Operations posted to
        // a peer that stopped progressing are completed — if at all — by the
        // purges inside `ucp_worker_destroy`, long after this point; that is why
        // `abandon_rma_ops` below has to answer their callers directly.
        for (entry, req) in pending {
            // SAFETY: the flush-close request is abandoned; freeing it and
            // issuing the (guarded, hence no-op) force close is the documented
            // fallback.
            unsafe { sys::ucp_request_free(req) };
            self.close_ep_raw(entry, true);
        }

        // Drain in-flight operations so every OpState is dropped (and every
        // on_error fired) before the worker disappears.
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.shared.inflight_ops.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            // SAFETY: worker owned by this thread.
            unsafe { sys::ucp_worker_progress(self.worker) };
            // RMA completions release parked unmaps; a region that drains here
            // is unmapped with an honest `Ok`.
            self.drain_rma_completions();
        }
        let leaked = self.shared.inflight_ops.load(Ordering::Acquire);
        if leaked > 0 {
            warn!("ucx: {leaked} operation(s) still in flight at teardown");
        }
        self.drain_rma_completions();
        // Answer the callers of any RMA operation that outlived the drain. Their
        // completion callbacks are UCX's to run or not; the `oneshot` senders
        // live inside request bookkeeping this thread is about to walk away
        // from, so nothing else will ever resolve them.
        self.abandon_rma_ops();
        // Every remaining mapping must be gone before `ucp_worker_destroy`.
        self.force_unmap_regions();
        // Deferred FORCE-close requests: give them a bounded chance to
        // complete, then free whatever remains (the worker is going away).
        let deadline = Instant::now() + Duration::from_millis(500);
        while !self.pending_closes.is_empty() && Instant::now() < deadline {
            // SAFETY: worker owned by this thread.
            unsafe { sys::ucp_worker_progress(self.worker) };
            self.poll_pending_closes();
        }
        for req in self.pending_closes.drain(..) {
            // SAFETY: abandoning an incomplete close request is the documented
            // fallback immediately before worker destruction.
            unsafe { sys::ucp_request_free(req) };
        }

        // No signal may race the destroy: retire zeroes the handle under the
        // doorbell mutex before we free it.
        self.shared.doorbell.retire();
        // SAFETY: sole owner, this thread.
        unsafe {
            sys::ucp_worker_destroy(self.worker);
            sys::ucp_cleanup(self.context);
        }
        debug!("ucx: progress thread exited");
    }
}

/// Records the `OnceLock` slot type used by the transport for startup output.
pub(crate) type StartupSlot = OnceLock<StartupOut>;
