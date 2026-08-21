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

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::os::raw::{c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tracing::{debug, warn};
use ucx_rs::{decode_status_ptr, status_string, sys};
use velo_ext::{InstanceId, MessageType, TransportAdapter, TransportErrorHandler};

use super::address::{AM_ID_BASE, AM_KIND_COUNT, AM_KIND_PING, AM_KIND_PONG, UcxEndpoint};
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
    Shutdown,
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
                        if adapter.shutdown_state.is_draining() {
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
                        } else {
                            let _ = adapter.message_stream.send((header, payload));
                        }
                    }
                    Some(MessageType::Response) | Some(MessageType::ShuttingDown) => {
                        let _ = adapter.response_stream.send((header, payload));
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

struct WorkerState {
    context: sys::ucp_context_h,
    worker: sys::ucp_worker_h,
    efd: c_int,
    eps: HashMap<InstanceId, EpEntry>,
    err_events: Arc<Mutex<Vec<InstanceId>>>,
    /// Last observed value of `WorkerShared::reg_epoch`.
    seen_reg_epoch: u64,
    shared: Arc<WorkerShared>,
    config: UcxConfig,
    /// Retained so the AM handler `arg` pointers stay valid for the worker's
    /// lifetime: UCX holds raw pointers into these allocations, and `Arc`
    /// gives each `RecvArg` the stable heap address that requires.
    _recv_args: Vec<Arc<RecvArg>>,
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
                seen_reg_epoch: shared.reg_epoch.load(Ordering::Acquire),
                shared: Arc::clone(shared),
                config: config.clone(),
                _recv_args: recv_args,
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
        if observed_empty {
            // Both close endpoints; both are only safe here (see above).
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
                        // Leave the pending ping to time out; check_health
                        // maps the timeout to the right error.
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
        if let Some(entry) = self.eps.get(&peer) {
            return Ok(entry.ep);
        }
        let blob = self
            .shared
            .peers
            .get(&peer)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("peer {peer} not registered"))?;

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

    fn teardown(mut self, ring_rx: flume::Receiver<Cmd>) {
        debug!("ucx: progress thread tearing down");

        // Fail everything still queued — mirrors the TCP writer's drain. A
        // second pass after a short pause shrinks (not closes — see the
        // module docs) the window where a racing sender's frame lands between
        // our last try_recv and the receiver drop and is discarded silently.
        for pass in 0..2 {
            while let Ok(cmd) = ring_rx.try_recv() {
                if let Cmd::Send(task) = cmd {
                    task.fail("ucx transport shutting down");
                }
            }
            if pass == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        drop(ring_rx);

        // Close every endpoint CONCURRENTLY under one global deadline, so
        // teardown latency is bounded by the slowest peer, not the sum. This
        // runs on `shutdown()`'s caller-blocking path.
        let entries: Vec<EpEntry> = {
            let eps = std::mem::take(&mut self.eps);
            eps.into_values().collect()
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
        // Phase B: whatever did not flush in time gets cancelled. FORCE-mode
        // close completes outstanding ops with CANCELED, driving callbacks.
        for (entry, req) in pending {
            // SAFETY: the flush-close request is abandoned; freeing it and
            // force-closing the still-open ep is the documented fallback.
            unsafe { sys::ucp_request_free(req) };
            self.close_ep_raw(entry, true);
        }

        // Drain in-flight operations so every OpState is dropped (and every
        // on_error fired) before the worker disappears.
        let deadline = Instant::now() + Duration::from_secs(1);
        while self.shared.inflight_ops.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
            // SAFETY: worker owned by this thread.
            unsafe { sys::ucp_worker_progress(self.worker) };
        }
        let leaked = self.shared.inflight_ops.load(Ordering::Acquire);
        if leaked > 0 {
            warn!("ucx: {leaked} operation(s) still in flight at teardown");
        }
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
