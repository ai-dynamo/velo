// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::task_tracker::TaskTrackerToken;
use tonic::transport::server::{Connected, TcpConnectInfo};

/// Tonic owns each connection task. Cancel its I/O to close even an unfinished
/// HTTP/2 handshake, which graceful shutdown alone cannot stop.
pub(super) struct Connection {
    stream: TcpStream,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    // Drop the socket before releasing its tracked lifetime.
    _tracked: TaskTrackerToken,
}

impl Connection {
    pub(super) fn new(
        stream: TcpStream,
        cancel: CancellationToken,
        tracked: TaskTrackerToken,
    ) -> Self {
        Self {
            stream,
            cancelled: Box::pin(cancel.cancelled_owned()),
            _tracked: tracked,
        }
    }

    fn check_cancelled(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::ErrorKind::ConnectionAborted.into())
        } else {
            Ok(())
        }
    }
}

impl Connected for Connection {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.stream.connect_info()
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check_cancelled(cx)?;
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check_cancelled(cx)?;
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.check_cancelled(cx)?;
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_cancelled(cx)?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check_cancelled(cx)?;
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
