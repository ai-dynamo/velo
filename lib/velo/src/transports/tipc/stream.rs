// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `TipcStream`: `AsyncFd`-backed `AsyncRead`/`AsyncWrite` wrapper with non-blocking
//! connect and `poll_shutdown` mapped to `Shutdown::Both` (TIPC has no half-close).
//!
//! ## Shutdown discipline (invariant 1)
//!
//! TIPC has **no half-close**: `shutdown(SHUT_WR)` and `shutdown(SHUT_RD)` both
//! return `EINVAL` (kernel 6.14 `socket.c:2794-2800`, verified).  Every teardown
//! path must call [`TipcStream::shutdown_both`] (or `AsyncWriteExt::shutdown`)
//! before dropping the stream.
//!
//! ## Clean EOF vs. ECONNRESET (invariant 2)
//!
//! A plain `close()` without prior `shutdown` surfaces as `ECONNRESET` at the peer
//! (`tipc_release` sends `TIPC_ERR_NO_PORT`, which `recv` maps to `ECONNRESET`).
//! Only explicit `shutdown(Both)` before drop produces a clean EOF (`recv() == 0`).
//! The unit test `tipc_shutdown_produces_clean_eof` pins this invariant.
//!
//! ## Close-blocking hazard
//!
//! The final `close(2)` can block the calling thread up to 8 s under link
//! congestion regardless of `O_NONBLOCK` (kernel `socket.c:548-560`, verified).
//! Writer tasks that exit with a congested queue must drop `TipcStream` via
//! `tokio::task::spawn_blocking`, not inline on a tokio worker.
//!
//! ## Connect semantics (invariant 3)
//!
//! TIPC marks the connecting socket writable **only after the remote application
//! calls `accept()`**, not after a kernel-backlog handshake (there is none in TIPC,
//! `socket.c:2769-2778`, verified).  The `timeout` in [`TipcStream::connect`]
//! therefore also bounds "remote accept loop wedged", not just "connection refused".

use std::io;
use std::net::Shutdown;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::socket::{
    connect_to_socket_addr, create_tipc_stream, set_conn_timeout_ms, set_nodelay_best_effort,
};

// ── TipcStream ────────────────────────────────────────────────────────────────

/// Async TIPC `SOCK_STREAM` connection.
///
/// Wraps a non-blocking [`socket2::Socket`] in [`tokio::io::unix::AsyncFd`].
/// Implements [`AsyncRead`] + [`AsyncWrite`] + [`Unpin`] + [`Send`].
///
/// See module-level documentation for shutdown discipline, close-blocking hazard,
/// and connect semantics.
pub struct TipcStream {
    inner: AsyncFd<socket2::Socket>,
}

impl TipcStream {
    /// Wrap an existing `AF_TIPC SOCK_STREAM` socket in an async wrapper.
    ///
    /// Sets the socket to non-blocking mode before registering with the tokio
    /// reactor.  Called by the inbound listener with each accepted socket.
    pub fn from_socket(sock: socket2::Socket) -> io::Result<Self> {
        sock.set_nonblocking(true)?;
        Ok(Self {
            inner: AsyncFd::new(sock)?,
        })
    }

    /// Return a shared reference to the underlying [`socket2::Socket`].
    // Currently unused; retained as the low-level escape hatch (sockopt tweaks,
    // diagnostics). Drop the allow if it gains a caller.
    #[allow(dead_code)]
    pub fn socket(&self) -> &socket2::Socket {
        self.inner.get_ref()
    }

    /// Perform an explicit `shutdown(SHUT_RDWR)` so the peer sees a clean EOF.
    ///
    /// Must be called in every teardown path (writer-task exit, health-probe
    /// drop, transport shutdown) before the stream is dropped.  TIPC flushes
    /// queued data before delivering the close signal — verified (§2.3.1).
    ///
    /// This method is a convenience wrapper; `AsyncWriteExt::shutdown` calls
    /// `poll_shutdown`, which has identical semantics.
    pub fn shutdown_both(&self) -> io::Result<()> {
        self.inner.get_ref().shutdown(Shutdown::Both)
    }

    /// Connect to the exact TIPC socket address `{ref_, node}`.
    ///
    /// Steps:
    /// 1. Create a non-blocking `AF_TIPC SOCK_STREAM` socket.
    /// 2. Set `TIPC_CONN_TIMEOUT` to `timeout + 1 s` (so our timeout fires first).
    /// 3. Set `TIPC_NODELAY` best-effort (silently ignored on kernels < 5.5).
    /// 4. Non-blocking `connect()` — returns immediately with `EINPROGRESS`.
    /// 5. Await writability under `tokio::time::timeout(timeout, …)`.
    /// 6. Check `SO_ERROR` to verify the connect succeeded.
    ///
    /// ## Invariant 3
    ///
    /// `timeout` bounds BOTH the SYN delivery time AND the remote `accept()`
    /// latency: TIPC marks the socket writable only after the remote application
    /// calls `accept()` (§2.3.3).  A wedged remote accept loop produces
    /// `io::ErrorKind::TimedOut`, not a connection refused error.
    pub async fn connect(ref_: u32, node: u32, timeout: Duration) -> io::Result<Self> {
        let sock = create_tipc_stream()?;

        // Set the kernel TIPC_CONN_TIMEOUT slightly above the application timeout so
        // our tokio::time::timeout normally fires first.  The kernel silently caps
        // TIPC_CONN_TIMEOUT at 30 s (tipc_sock.c); for application timeouts > ~29 s
        // the kernel's ETIMEDOUT arrives via SO_ERROR before the tokio timeout — the
        // SO_ERROR check below remaps it to the same io::ErrorKind::TimedOut.
        let kernel_timeout_ms = u32::try_from(timeout.as_millis())
            .unwrap_or(u32::MAX)
            .saturating_add(1_000)
            .min(30_000);
        set_conn_timeout_ms(&sock, kernel_timeout_ms)?;
        set_nodelay_best_effort(&sock);

        // Non-blocking connect — tolerated EINPROGRESS / WouldBlock by socket.rs.
        connect_to_socket_addr(&sock, ref_, node)?;

        let stream = Self {
            inner: AsyncFd::new(sock)?,
        };

        // Await writability (= remote called accept()) under the application timeout.
        //
        // The guard is held in a nested block so its borrow of `stream.inner` ends
        // before `stream` is moved into the return value.
        {
            let _guard = tokio::time::timeout(timeout, stream.inner.writable())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TIPC connect timed out"))??;

            // SO_ERROR == 0 → connect succeeded; anything else is the connect error.
            //
            // Kernel cap: TIPC_CONN_TIMEOUT is silently clamped to ~30 s by the kernel.
            // For caller timeouts > ~29 s, the kernel's ETIMEDOUT arrives via SO_ERROR
            // before our tokio::time::timeout fires.  Remap it to the same error kind so
            // callers (e.g. check_health) see io::ErrorKind::TimedOut consistently.
            if let Some(err) = stream.inner.get_ref().take_error()? {
                if err.raw_os_error() == Some(libc::ETIMEDOUT) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TIPC connect timed out",
                    ));
                }
                return Err(err);
            }
            // `_guard` dropped here without clear_ready() — WRITE readiness retained;
            // borrow of `stream.inner` ends.
        }

        Ok(stream)
    }
}

// ── AsyncRead ────────────────────────────────────────────────────────────────

impl AsyncRead for TipcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut guard = ready!(self.inner.poll_read_ready(cx))?;

            // Compute the pointer and length outside the `try_io` closure.
            // After this point the mutable borrow of `buf` through `unfilled`
            // is released (NLL ends the borrow as soon as `unfilled` is last used),
            // allowing `buf.advance(n)` below.  The raw pointer is valid for the
            // entire `poll_read` call because `buf`'s storage outlives this frame.
            let unfilled = buf.initialize_unfilled();
            let ptr = unfilled.as_mut_ptr();
            let len = unfilled.len();
            // `unfilled` is last used above; its borrow of `buf` ends here.

            let result = guard.try_io(|inner| {
                let fd = inner.get_ref().as_raw_fd();
                // Retry EINTR inside the closure to avoid an extra epoll round-trip.
                loop {
                    // SAFETY: `ptr` points into `buf`'s storage, which is valid for
                    // `len` bytes and lives at least as long as `poll_read`.
                    // The fd is a valid non-blocking AF_TIPC SOCK_STREAM socket.
                    let n = unsafe { libc::recv(fd, ptr.cast::<libc::c_void>(), len, 0) };
                    if n >= 0 {
                        return Ok(n as usize);
                    }
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue, // retry EINTR inline
                        _ => return Err(e),
                    }
                }
            });

            match result {
                Ok(Ok(n)) => {
                    // `n` bytes are now initialised in `buf`'s unfilled region.
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                // `try_io` returned TryIoError (WouldBlock): readiness has been
                // cleared; loop back to re-register interest.
                Err(_would_block) => continue,
            }
        }
    }
}

// ── AsyncWrite ───────────────────────────────────────────────────────────────

impl AsyncWrite for TipcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let mut guard = ready!(self.inner.poll_write_ready(cx))?;

            let ptr = buf.as_ptr();
            let len = buf.len();

            match guard.try_io(|inner| {
                let fd = inner.get_ref().as_raw_fd();
                loop {
                    // SAFETY: `ptr` is valid for `len` bytes (slice invariant).
                    // MSG_NOSIGNAL suppresses SIGPIPE on broken connections;
                    // the error surfaces as io::ErrorKind::BrokenPipe instead.
                    let n = unsafe {
                        libc::send(fd, ptr.cast::<libc::c_void>(), len, libc::MSG_NOSIGNAL)
                    };
                    if n >= 0 {
                        return Ok(n as usize);
                    }
                    let e = io::Error::last_os_error();
                    match e.kind() {
                        io::ErrorKind::Interrupted => continue,
                        _ => return Err(e),
                    }
                }
            }) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // TIPC SOCK_STREAM has no user-visible flush; buffering is fully managed
        // by the kernel's flow-control layer.
        Poll::Ready(Ok(()))
    }

    /// Shut down the connection in both directions.
    ///
    /// TIPC has no half-close: `SHUT_WR` and `SHUT_RD` both return `EINVAL`.
    /// This implementation always calls `shutdown(SHUT_RDWR)` (invariant 1).
    ///
    /// The `shutdown(2)` syscall itself is fast (does not block); the
    /// close-blocking hazard described in the module docs applies to the
    /// subsequent `close(2)` when the stream is dropped, not to this call.
    ///
    /// After this returns, the peer will observe `recv() == 0` (clean EOF)
    /// rather than `ECONNRESET` (invariant 2).
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.inner.get_ref().shutdown(Shutdown::Both))
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::super::socket::{
        bind_single_instance_and_listen, create_tipc_stream, getsockname_ref_node, tipc_available,
    };

    /// Unique service type for stream unit tests.
    /// Must be ≥ 64 (`TIPC_RESERVED_TYPES`); chosen to avoid clashing with
    /// socket.rs tests (0x5654_0001) or integration test factories.
    const STREAM_TEST_TYPE: u32 = 0x5654_0002;

    // ── Test helpers ──────────────────────────────────────────────────────────

    /// Build a connected `(client, server)` TipcStream pair.
    ///
    /// `instance` is the TIPC service instance and must be unique across
    /// concurrently running tests.  Returns `None` if the TIPC module is not
    /// loaded so tests can self-skip without a hard failure.
    async fn make_pair(instance: u32) -> Option<(TipcStream, TipcStream)> {
        if !tipc_available() {
            eprintln!(
                "tipc_available() == false — TIPC module not loaded; skipping test. \
                 Run `sudo modprobe tipc` to enable TIPC tests."
            );
            return None;
        }

        // Create and bind a listener socket.
        let listener = create_tipc_stream().expect("create listener socket");
        bind_single_instance_and_listen(&listener, STREAM_TEST_TYPE, instance, 8)
            .expect("bind_single_instance_and_listen");
        let (socket_ref, node) = getsockname_ref_node(&listener).expect("getsockname");

        // Wrap the listener in AsyncFd so we can accept without blocking.
        let async_listener = AsyncFd::new(listener).expect("AsyncFd for listener socket");

        let accept_fut = async {
            loop {
                let mut guard = async_listener.readable().await?;
                match guard.try_io(|inner| inner.get_ref().accept()) {
                    Ok(Ok((sock, _addr))) => {
                        break TipcStream::from_socket(sock);
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_would_block) => continue,
                }
            }
        };

        let (connect_res, accept_res) = tokio::join!(
            TipcStream::connect(socket_ref, node, Duration::from_secs(5)),
            accept_fut
        );

        let client = connect_res.expect("TipcStream::connect");
        let server = accept_res.expect("accept");
        Some((client, server))
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// Basic write → read round-trip through the AsyncFd wrappers.
    #[tokio::test]
    async fn tipc_stream_write_read_roundtrip() {
        let Some((mut client, mut server)) = make_pair(0xDEAD_0010).await else {
            return;
        };

        let data = b"Hello, TIPC stream!";
        client.write_all(data).await.expect("write_all");

        let mut buf = vec![0u8; data.len()];
        server.read_exact(&mut buf).await.expect("read_exact");

        assert_eq!(&buf, data, "data round-trip must be byte-exact");
    }

    /// Bidirectional round-trip: client → server ping, server → client pong.
    #[tokio::test]
    async fn tipc_stream_bidirectional() {
        let Some((mut client, mut server)) = make_pair(0xDEAD_0011).await else {
            return;
        };

        client.write_all(b"ping").await.expect("ping write");
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.expect("ping read");
        assert_eq!(&buf, b"ping");

        server.write_all(b"pong").await.expect("pong write");
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.expect("pong read");
        assert_eq!(&buf, b"pong");
    }

    /// Pins invariant 1 and invariant 2:
    ///
    /// - Invariant 1: `poll_shutdown` must call `shutdown(Both)` (no half-close).
    /// - Invariant 2: explicit `shutdown(Both)` causes the peer to receive clean
    ///   EOF (`recv() == 0`), NOT `ECONNRESET`.
    ///
    /// If `poll_shutdown` were to call `shutdown(Write)` instead, TIPC returns
    /// `EINVAL` and the connection is silently not shut down.  If the stream were
    /// simply dropped without `shutdown`, the peer sees `ECONNRESET` — this test
    /// would then fail at the final `assert_eq!(n, 0)`.
    #[tokio::test]
    async fn tipc_shutdown_produces_clean_eof() {
        let Some((mut client, mut server)) = make_pair(0xDEAD_0012).await else {
            return;
        };

        let data = b"final frame before shutdown";
        client
            .write_all(data)
            .await
            .expect("write data before shutdown");

        // AsyncWriteExt::shutdown calls poll_shutdown → shutdown(Both).
        // TIPC flushes queued data before delivering the close signal (§2.3.1).
        client.shutdown().await.expect("shutdown(Both)");

        // The server must receive the data that was written before shutdown.
        let mut buf = vec![0u8; data.len()];
        server
            .read_exact(&mut buf)
            .await
            .expect("read data flushed before close signal");
        assert_eq!(
            &buf, data,
            "data written before shutdown must arrive intact"
        );

        // The server must then receive a clean EOF (n == 0), not ECONNRESET.
        // If this assertion fails, it means shutdown(Both) was not called — the
        // peer is seeing ECONNRESET from a plain close().
        let n = server
            .read(&mut buf[..1])
            .await
            .expect("read at EOF must succeed, not return ECONNRESET");
        assert_eq!(
            n, 0,
            "shutdown(Both) must produce clean EOF (recv == 0); \
             got n={n} — this indicates poll_shutdown did not call shutdown(SHUT_RDWR)"
        );
    }

    /// Large-payload write/read to exercise the TIPC flow-control window.
    ///
    /// 256 KB exceeds the initial TIPC conn window (~128 KB, §2.2) so the kernel
    /// will throttle the sender and this exercises the WouldBlock → AsyncFd
    /// backpressure path.
    #[tokio::test]
    async fn tipc_stream_large_payload() {
        let Some((mut client, mut server)) = make_pair(0xDEAD_0013).await else {
            return;
        };

        let data: Vec<u8> = (0u8..=255).cycle().take(256 * 1024).collect();
        let expected = data.clone();

        let write_fut = async { client.write_all(&data).await };
        let read_fut = async {
            let mut buf = vec![0u8; expected.len()];
            server.read_exact(&mut buf).await?;
            Ok::<Vec<u8>, io::Error>(buf)
        };

        let (write_res, read_res) = tokio::join!(write_fut, read_fut);
        write_res.expect("large write");
        let received = read_res.expect("large read");

        assert_eq!(received, expected, "256 KB round-trip must be byte-exact");
    }
}
