// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Scratch, for measurement only, not for merge: choose at run time where
//! velo's own tasks run.
//!
//! | Arm | Where velo's tasks run |
//! |---|---|
//! | `A` | Everything on the application's runtime (the default). |
//! | `B<n>` | The mux on a runtime of its own with `n` threads. |
//! | `C<n>` | One velo runtime with `n` threads for the transports, the inbound loop and the mux. Handlers stay on the application's runtime. |
//! | `D` | As `A`, but a stream `send` yields once when it woke a parked batcher. |
//!
//! `VELO_ARM` selects the arm for every `VeloBuilder` built after it is set.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

/// Where velo's own tasks run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeArm {
    /// A: everything on the application's runtime.
    Default,
    /// B: the mux on a runtime of its own with this many threads.
    Mux(usize),
    /// C: transports, inbound loop and mux on one velo runtime.
    Velo(usize),
    /// D: the application's runtime; `send` yields after waking a parked batcher.
    Yield,
}

impl RuntimeArm {
    /// Parse `A`, `B1`, `B2`, `C1`, `C2`, `C4`, `D` (any thread count).
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let (head, tail) = s.split_at(s.len().min(1));
        let threads = || tail.parse::<usize>().ok().filter(|&n| n > 0);
        match head {
            "A" | "a" if tail.is_empty() => Some(Self::Default),
            "D" | "d" if tail.is_empty() => Some(Self::Yield),
            "B" | "b" => threads().map(Self::Mux),
            "C" | "c" => threads().map(Self::Velo),
            _ => None,
        }
    }

    /// The arm named by `VELO_ARM`, or `A`. Panics on a value it cannot read,
    /// so a typo cannot measure arm A under another label.
    pub fn from_env() -> Self {
        match std::env::var("VELO_ARM") {
            Ok(v) if !v.is_empty() => {
                Self::parse(&v).unwrap_or_else(|| panic!("VELO_ARM={v} is not an arm"))
            }
            _ => Self::Default,
        }
    }

    /// The short name, as `parse` reads it.
    pub fn label(&self) -> String {
        match self {
            Self::Default => "A".into(),
            Self::Mux(n) => format!("B{n}"),
            Self::Velo(n) => format!("C{n}"),
            Self::Yield => "D".into(),
        }
    }

    /// Threads velo adds for its own runtime.
    pub fn threads(&self) -> usize {
        match self {
            Self::Mux(n) | Self::Velo(n) => *n,
            _ => 0,
        }
    }
}

type ThreadStart = Arc<dyn Fn() + Send + Sync>;
static THREAD_START: OnceLock<ThreadStart> = OnceLock::new();

/// Run `f` on each thread velo's own runtimes start (to pin them, say).
/// Only the first call takes effect.
pub fn set_thread_start(f: impl Fn() + Send + Sync + 'static) {
    let _ = THREAD_START.set(Arc::new(f));
}

/// A runtime velo built for itself. Shut down without waiting, because the
/// last `Velo` may drop inside another runtime.
pub(crate) struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl OwnedRuntime {
    pub(crate) fn build(name: &str, threads: usize) -> std::io::Result<Self> {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(threads)
            .thread_name(name)
            .enable_all();
        if let Some(f) = THREAD_START.get() {
            let f = Arc::clone(f);
            builder.on_thread_start(move || f());
        }
        Ok(Self(Some(builder.build()?)))
    }

    pub(crate) fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().expect("runtime").handle().clone()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

// --- Probes ----------------------------------------------------------------

/// Doorbell wakes posted by a consumer's read, and where each doorbell visit
/// ran: (thread name, when). For the stall rows only.
pub static DRAIN_WAKES: AtomicU64 = AtomicU64::new(0);
pub static VISITS: parking_lot::Mutex<Vec<(String, std::time::Instant)>> =
    parking_lot::Mutex::new(Vec::new());

pub(crate) fn note_visit() {
    let name = std::thread::current().name().unwrap_or("?").to_string();
    VISITS.lock().push((name, std::time::Instant::now()));
}

// --- Arm D -----------------------------------------------------------------

static YIELD_ON: AtomicBool = AtomicBool::new(false);
static YIELDS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static WOKEN: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn enable_yield() {
    YIELD_ON.store(true, Ordering::Relaxed);
}

pub(crate) fn yield_enabled() -> bool {
    YIELD_ON.load(Ordering::Relaxed)
}

/// Sends that yielded because they woke a parked batcher (arm D).
pub fn yields() -> u64 {
    YIELDS.load(Ordering::Relaxed)
}

/// After a send: yield once if this thread just woke a parked batcher.
pub(crate) async fn maybe_yield() {
    if YIELD_ON.load(Ordering::Relaxed) && WOKEN.with(|w| w.replace(false)) {
        YIELDS.fetch_add(1, Ordering::Relaxed);
        tokio::task::yield_now().await;
    }
}

/// Clear a stale flag before a send, so `maybe_yield` sees only this send's wake.
pub(crate) fn clear_woken() {
    if YIELD_ON.load(Ordering::Relaxed) {
        WOKEN.with(|w| w.set(false));
    }
}

struct FlagWaker {
    inner: parking_lot::Mutex<Waker>,
}

impl Wake for FlagWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        WOKEN.with(|w| w.set(true));
        self.inner.lock().wake_by_ref();
    }
}

/// Polls `F` with a waker that flags the waking thread (arm D's batcher).
pub(crate) struct FlagWake<F> {
    inner: Pin<Box<F>>,
    flag: Option<(Arc<FlagWaker>, Waker)>,
}

impl<F> FlagWake<F> {
    pub(crate) fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
            flag: None,
        }
    }
}

impl<F: Future> Future for FlagWake<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        let (fw, waker) = this.flag.get_or_insert_with(|| {
            let fw = Arc::new(FlagWaker {
                inner: parking_lot::Mutex::new(cx.waker().clone()),
            });
            let waker = Waker::from(Arc::clone(&fw));
            (fw, waker)
        });
        {
            let mut inner = fw.inner.lock();
            if !inner.will_wake(cx.waker()) {
                *inner = cx.waker().clone();
            }
        }
        let waker = waker.clone();
        let mut cx = Context::from_waker(&waker);
        this.inner.as_mut().poll(&mut cx)
    }
}
