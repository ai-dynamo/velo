// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Where velo runs its own tasks: on the application's tokio runtime, or on a
//! runtime that velo builds and shuts down. See [`TaskRuntime`].

use std::future::Future;
use std::time::Duration;

use tokio::runtime::{Handle, Runtime};

use futures::future::BoxFuture;

/// Where velo runs its own tasks: the transports, the inbound loop, the
/// messenger mux, and the mux's batch lanes. Handlers always run on the
/// application's runtime, the one that calls [`VeloBuilder::build`].
///
/// Set it with [`VeloBuilder::task_runtime`].
///
/// # Why a dedicated runtime
///
/// On the application's runtime, an application task that wakes a velo task
/// and then holds its worker thread (a blocking call, a long encode) stalls
/// that velo task until the hold ends. tokio puts the woken task in the
/// worker's LIFO slot, and no other worker steals from it. A stream `send`, a
/// unary call, a handler's response and a consumer's read are all exposed.
/// On a dedicated runtime each of those wakes crosses to a thread the
/// application cannot hold, so none of them can stall. The book's "Known
/// limitations" appendix describes the stall and the other workaround
/// (`block_in_place` or `spawn_blocking`).
///
/// # What it costs
///
/// Each wake between the application and velo becomes a wake across threads.
/// Measured on loopback TCP with the mux, one dedicated thread against the
/// application's runtime:
///
/// - A single caller making one unary call at a time took about 3.5 times as
///   long per call and about 4 times the CPU, because the call no longer runs
///   on one worker without a thread park.
/// - A paced serving shape (2 hosts, 8 engines, records about every 2 ms) used
///   about 29% more CPU per token for the same work.
/// - Under concurrency it did better: 40% more records per second at 64
///   closed-loop streams, 1.2 to 1.6 times the calls at 64 unary callers, and
///   a flat p99 when the application's runtime was saturated.
///
/// # Threads
///
/// [`TaskRuntime::Dedicated`] takes 1 or 2 threads; [`VeloBuilder::build`]
/// refuses any other count. More threads helped only while there were more
/// busy peer lanes than threads: with one busy peer, 1 thread beat 2 and 4 in
/// throughput and p99; with four busy peers, 2 threads did best and 4 were no
/// better on streams. CPU per record rose with each thread added.
///
/// # Lifetime
///
/// Each [`Velo`](crate::Velo) built with `Dedicated` owns one runtime.
/// [`Velo::shutdown`](crate::Velo::shutdown) stops it after the transports
/// have closed, and returns once its threads have exited. If the instance is
/// dropped without `shutdown`, the last drop tears the transports down and
/// stops the runtime on a background thread, without blocking the caller, so
/// a drop inside an async task is safe. A [`Messenger`](crate::Messenger)
/// taken from the instance stops working when the runtime stops: the last
/// drop waits for the `Messenger` to drop for at most a second, then tears
/// the transports down anyway.
///
/// [`VeloBuilder::build`]: crate::VeloBuilder::build
/// [`VeloBuilder::task_runtime`]: crate::VeloBuilder::task_runtime
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskRuntime {
    /// Run velo's tasks on the runtime that calls
    /// [`VeloBuilder::build`](crate::VeloBuilder::build). The default.
    ///
    /// Application code must not hold a worker thread right after it calls
    /// velo; see the type's documentation.
    Application,
    /// Run velo's tasks on a multi-thread runtime that velo builds and shuts
    /// down, with `threads` worker threads named `velo-rt`. `threads` must be
    /// 1 or 2. [`TaskRuntime::dedicated`] gives 1.
    Dedicated {
        /// Worker threads: 1 or 2.
        threads: usize,
    },
}

impl TaskRuntime {
    /// The most threads [`TaskRuntime::Dedicated`] takes.
    pub const MAX_DEDICATED_THREADS: usize = 2;

    /// A dedicated runtime with one thread.
    pub const fn dedicated() -> Self {
        Self::Dedicated { threads: 1 }
    }

    /// Refuses a thread count `Dedicated` does not take.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Self::Dedicated { threads } = *self {
            anyhow::ensure!(
                (1..=Self::MAX_DEDICATED_THREADS).contains(&threads),
                "TaskRuntime::Dedicated takes 1 to {} threads, not {threads}: \
                 threads past the number of busy peer lanes did not help",
                Self::MAX_DEDICATED_THREADS
            );
        }
        Ok(())
    }
}

/// [`TaskRuntime::Application`].
// Making the dedicated runtime the default is a change to this one impl.
impl Default for TaskRuntime {
    fn default() -> Self {
        Self::Application
    }
}

/// The name of each thread of a dedicated runtime.
pub(crate) const THREAD_NAME: &str = "velo-rt";

/// How long a stop waits for the runtime's threads. A task that blocks a
/// runtime thread past this keeps that thread; the stop does not wait for it.
const STOP_GRACE: Duration = Duration::from_secs(2);

/// How long a stop from a drop waits for the transports' teardown and close.
/// Each transport bounds its own close (TCP and QUIC: 2.5 s by default).
const DROP_CLOSE_WAIT: Duration = Duration::from_secs(5);

/// A runtime velo built for its own tasks. Owned by `VeloInner`, so by every
/// clone of one `Velo`.
pub(crate) struct VeloRuntime {
    /// Taken by the first stop. A tokio mutex, so a second
    /// [`stop`](Self::stop) waits until the first has joined the threads.
    slot: tokio::sync::Mutex<Option<RuntimeSlot>>,
}

/// Owns the runtime until a stop takes it. Dropped with the runtime still in
/// it, it shuts the runtime down without waiting: `Runtime`'s own drop
/// panics inside an async context.
struct RuntimeSlot(Option<Runtime>);

impl Drop for RuntimeSlot {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

impl VeloRuntime {
    /// Builds the runtime and returns it with its handle.
    pub(crate) fn build(threads: usize) -> std::io::Result<(Self, Handle)> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name(THREAD_NAME)
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        Ok((
            Self {
                slot: tokio::sync::Mutex::new(Some(RuntimeSlot(Some(runtime)))),
            },
            handle,
        ))
    }

    /// Stops the runtime and waits until its threads have exited, or for
    /// [`STOP_GRACE`]. Call it only after every velo task that must finish has
    /// finished: the runtime drops the tasks it still has.
    pub(crate) async fn stop(&self) {
        let mut slot = self.slot.lock().await;
        let Some(runtime) = slot.take() else {
            return;
        };
        let (done, stopped) = tokio::sync::oneshot::channel();
        stop_on_thread(runtime, None, Some(done));
        // An error means the stop thread could not start; the slot then shut
        // the runtime down without waiting.
        let _ = stopped.await;
    }

    /// Stops the runtime from a drop: on a background thread, runs `teardown`
    /// on the runtime, then stops it. Never blocks the caller.
    ///
    /// The teardown runs the transports' shutdown hooks with the runtime's
    /// handle entered, and their close may need its tasks, so the runtime must
    /// outlive it. A runtime shut down first would leave a hook with dead
    /// timer and I/O drivers.
    pub(crate) fn stop_detached(&mut self, teardown: BoxFuture<'static, ()>) {
        if let Some(runtime) = self.slot.get_mut().take() {
            stop_on_thread(runtime, Some(teardown), None);
        }
    }
}

fn stop_on_thread(
    mut slot: RuntimeSlot,
    teardown: Option<BoxFuture<'static, ()>>,
    done: Option<tokio::sync::oneshot::Sender<()>>,
) {
    let stop = move || {
        if let Some(runtime) = slot.0.take() {
            if let Some(teardown) = teardown {
                // A thread outside any runtime may block on this one. The
                // timeout is built inside it: its timer needs the context.
                let _ = runtime
                    .block_on(async { tokio::time::timeout(DROP_CLOSE_WAIT, teardown).await });
            }
            runtime.shutdown_timeout(STOP_GRACE);
        }
        if let Some(done) = done {
            let _ = done.send(());
        }
    };
    // On a spawn error the closure drops here, and the slot it owns shuts the
    // runtime down without waiting.
    if let Err(error) = std::thread::Builder::new()
        .name(format!("{THREAD_NAME}-stop"))
        .spawn(stop)
    {
        tracing::error!(%error, "could not start a thread to stop velo's runtime");
    }
}

/// Polls `future` with `handle` entered, so what it creates belongs to that
/// runtime: tasks it spawns with `tokio::spawn`, sockets (tokio binds a socket
/// to the I/O driver of the context it is created in), and timers.
///
/// A transport's `start` runs inside this. Its listener and the clients it
/// connects (NATS, QUIC's endpoint) then live on velo's runtime, even when a
/// library spawns or registers through the current context instead of the
/// handle `start` is given.
pub(crate) async fn entered<F: Future>(handle: &Handle, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let _entered = handle.enter();
        future.as_mut().poll(cx)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_takes_one_or_two_threads() {
        assert!(TaskRuntime::Application.validate().is_ok());
        assert!(TaskRuntime::dedicated().validate().is_ok());
        assert!(TaskRuntime::Dedicated { threads: 2 }.validate().is_ok());
        for threads in [0, 3, 64] {
            let error = TaskRuntime::Dedicated { threads }
                .validate()
                .expect_err("refused");
            assert!(error.to_string().contains("1 to 2 threads"), "{error}");
        }
    }

    #[test]
    fn the_default_is_the_application_runtime() {
        assert_eq!(TaskRuntime::default(), TaskRuntime::Application);
    }

    /// A slot dropped inside an async task must not panic: `Runtime`'s own
    /// drop does there.
    #[tokio::test]
    async fn a_runtime_slot_dropped_in_an_async_task_does_not_panic() {
        let (runtime, _handle) = VeloRuntime::build(1).expect("runtime");
        drop(runtime);
    }

    #[tokio::test]
    async fn entered_spawns_on_the_entered_runtime() {
        let (runtime, handle) = VeloRuntime::build(1).expect("runtime");
        let name = entered(&handle, async {
            tokio::spawn(async { std::thread::current().name().map(str::to_owned) })
                .await
                .expect("join")
        })
        .await;
        assert_eq!(name.as_deref(), Some(THREAD_NAME));
        runtime.stop().await;
    }
}
