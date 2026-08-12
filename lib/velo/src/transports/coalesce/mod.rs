// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Write coalescing for framed writer loops.
//!
//! Every outbound path in velo that speaks [`TcpFrameCodec`] — the messenger
//! TCP transport, the messenger UDS transport, and the streaming egress pump —
//! has the same shape: block for one item, take whatever else is already
//! queued, and push the lot at the socket. This module owns that loop exactly
//! once, in [`run_coalescing_writer`].
//!
//! It lives beside the transports rather than inside `tcp::framing` because it
//! is not TCP-specific: UDS and the streaming data plane run it over their own
//! writers, and routing their import through the TCP implementation namespace
//! would misstate the dependency.
//!
//! # Why coalescing is wire-compatible
//!
//! Frames are self-delimiting: each carries its own preamble with explicit
//! lengths, and [`TcpFrameCodec`]'s `Decoder` is driven in a loop by `Framed`,
//! which already handles several frames arriving in one read. The bytes a batch
//! produces are byte-identical to writing each frame separately, so a
//! coalescing writer talks to an *unmodified* reader and vice versa. No
//! negotiation, no version bump, no wire-format change — the only thing that
//! changes is how many writes produce the same bytes.
//!
//! # Why it adds no latency
//!
//! The loop blocks on `recv_async` for the first item exactly as before, then
//! takes additional items only via `try_recv` — it never waits for work that
//! has not arrived. Under light load each item still gets its own write; under
//! load, batches form on their own.
//!
//! # The delivery-reporting invariant
//!
//! Every item taken off the channel gets **exactly one**
//! [`Coalescable::on_write_error`] if it did not reach the wire, and **none**
//! if it did. The caller's own post-loop drain (e.g. `connection_writer_task`)
//! covers items still sitting in the channel, but it cannot see an item this
//! loop is holding — so the three paths that hold one (a mandatory flush
//! failing, a rejected encode, a failed direct write) report it themselves.

use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use velo_ext::MessageType;

use super::tcp::framing::{COALESCE_THRESHOLD, MIN_HEADER_SIZE, TcpFrameCodec};

/// Default cap on how many bytes one coalesced write may carry.
///
/// Set to [`COALESCE_THRESHOLD`] — the same point at which a single frame stops
/// being worth copying into a staging buffer at all. This is a *staging* cap,
/// not a kernel send-buffer estimate: the sockets these writers run on are
/// configured with 1–2 MiB send buffers. What it bounds is the memcpy done per
/// flush and the steady-state size of the per-connection staging buffer.
const DEFAULT_MAX_BATCH_BYTES: usize = COALESCE_THRESHOLD;

/// Default cap on how many frames one coalesced write may carry.
///
/// Bounds worst-case error attribution: a failed write must report the error
/// for every frame it was carrying, so an unbounded batch would mean an
/// unbounded error fan-out on a single connection fault.
const DEFAULT_MAX_BATCH_FRAMES: usize = 1024;

/// Reason handed to [`Coalescable::on_write_error`] when an item was dropped
/// because the flush that had to precede it failed.
const FLUSH_FAILED: &str = "batch flush failed";

/// An item a coalescing writer can put on the wire.
///
/// Implementors supply the three frame fields plus, optionally, terminal
/// semantics and a per-item failure notification.
pub(crate) trait Coalescable {
    /// Whether [`Self::on_write_error`] does anything.
    ///
    /// When `true` the writer holds each item until its batch reaches the
    /// wire, so a failed flush can report every item it was carrying. When
    /// `false` it drops each item as soon as the bytes are staged — the
    /// streaming egress pump has no per-frame error handler, and keeping a
    /// whole batch of payloads alive alongside the copy of them in the staging
    /// buffer would double live memory on the path coalescing exists to speed
    /// up.
    const REPORTS_ERRORS: bool = true;

    /// Frame type written in the preamble.
    fn msg_type(&self) -> MessageType;

    /// Frame header bytes.
    fn header(&self) -> &[u8];

    /// Frame payload bytes.
    fn payload(&self) -> &[u8];

    /// Whether the writer should stop after this item reaches the wire.
    ///
    /// Used by the streaming egress pump: after a terminal sentinel
    /// (Finalized / Dropped / Detached / TransportError) anything still queued
    /// is discarded, because sending it would race the consumer's
    /// post-terminal cleanup and trigger spurious resets on the wire.
    fn is_terminal(&self) -> bool {
        false
    }

    /// Report that this item did not reach the wire.
    ///
    /// `reason` is the bare cause — an `io::Error` rendering, or
    /// [`FLUSH_FAILED`] — so implementors can add their own
    /// transport-identifying prefix. Called at most once per item, and never
    /// for an item that was written.
    ///
    /// Takes `self` so an implementor can move an owned payload straight into
    /// its error handler rather than cloning it on the failure path.
    fn on_write_error(self, _reason: &str)
    where
        Self: Sized,
    {
    }
}

/// What ended a coalescing writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriterFailure {
    /// The socket rejected a write.
    Write,
    /// A frame could not be encoded — in practice, one above the codec's size
    /// limit.
    Encode,
}

/// Hooks for logging and metrics, so the loop itself stays transport-agnostic.
pub(crate) trait WriterObserver {
    /// One batch reached the wire carrying `frames` frames. Not called for a
    /// failed write.
    ///
    /// "Batch" is a unit of coalescing, not a syscall. A packed batch is one
    /// `write_all`; a frame routed to [`Staging::WriteDirect`] is written
    /// segmented across several and still reports once, with `frames = 1`.
    /// Anything counting these must describe them as batches — see
    /// `velo_streaming_egress_flushes_total`.
    fn on_flush(&self, _frames: usize) {}

    /// The writer is stopping. `frames` is how many items the failure
    /// implicates — the batch size for a failed flush, `1` for a single frame
    /// on the direct path.
    fn on_failure(&self, _kind: WriterFailure, _err: &io::Error, _frames: usize) {}
}

/// What to do with the next item, decided before it is touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Staging {
    /// Append to the current batch.
    Stage,
    /// Flush the current batch first, then append.
    FlushThenStage,
    /// Flush the current batch, then write this item straight to the socket
    /// without staging it.
    WriteDirect,
}

impl Staging {
    /// Whether the current batch has to go out before this item is handled.
    #[inline]
    fn needs_flush_first(self) -> bool {
        !matches!(self, Staging::Stage)
    }
}

/// Accumulates several encoded frames into one buffer so they can be handed to
/// the socket with a single `write_all`.
///
/// # Bounds
///
/// A frame larger than [`COALESCE_THRESHOLD`] is never staged — [`Self::classify`]
/// routes it to [`Staging::WriteDirect`], which writes it segmented and avoids
/// copying a large payload. So this buffer's high-water mark is one full batch
/// plus one sub-threshold frame, and a one-time 16 MiB message cannot
/// permanently inflate a per-connection buffer.
struct FrameBatchBuffer {
    buf: BytesMut,
    frames: usize,
    max_bytes: usize,
    max_frames: usize,
}

impl FrameBatchBuffer {
    fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_BATCH_FRAMES)
    }

    fn with_limits(max_bytes: usize, max_frames: usize) -> Self {
        Self {
            // Start small; `push` reserves what it needs and the allocation is
            // reused across flushes for the connection's life.
            buf: BytesMut::with_capacity(8 * 1024),
            frames: 0,
            max_bytes,
            max_frames,
        }
    }

    /// Number of frames staged but not yet written.
    #[inline]
    fn frame_count(&self) -> usize {
        self.frames
    }

    /// Decide how to handle a frame of this size.
    #[inline]
    fn classify(&self, header_len: usize, payload_len: usize) -> Staging {
        // Large frames keep the direct segmented write path: staging one would
        // memcpy the whole payload and leave the staging buffer holding that
        // capacity for the rest of the connection's life.
        if header_len.saturating_add(payload_len) > COALESCE_THRESHOLD {
            return Staging::WriteDirect;
        }
        if self.frames == 0 {
            return Staging::Stage;
        }
        if self.frames + 1 > self.max_frames
            || self.buf.len() + MIN_HEADER_SIZE + header_len + payload_len > self.max_bytes
        {
            return Staging::FlushThenStage;
        }
        Staging::Stage
    }

    /// Stage one frame. Does not touch the socket.
    #[inline]
    fn push(&mut self, msg_type: MessageType, header: &[u8], payload: &[u8]) -> io::Result<()> {
        TcpFrameCodec::append_frame(&mut self.buf, msg_type, header, payload)?;
        self.frames += 1;
        Ok(())
    }

    /// Write every staged frame with a single `write_all` and reset.
    ///
    /// The buffer is cleared whether or not the write succeeded: on failure the
    /// caller owns error reporting for the frames it staged, and retrying a
    /// partially-written frame stream would corrupt the peer's decoder.
    async fn flush_to<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> io::Result<()> {
        if self.frames == 0 {
            return Ok(());
        }
        let result = writer.write_all(&self.buf).await;
        self.buf.clear();
        self.frames = 0;
        result
    }

    /// Current allocation size. Used to pin the bound documented above.
    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.buf.capacity()
    }
}

/// Drain `rx` onto `writer`, coalescing whatever is already queued into single
/// writes, until the channel closes, `cancel` fires, a terminal item is
/// written, or the socket fails.
///
/// The caller keeps ownership of `writer` and is responsible for any teardown
/// (flush / shutdown) it wants afterwards.
///
/// `cancel` is `None` for writers whose only stop signal is the channel
/// closing. That case costs one `Poll::Pending` per wake and registers no
/// waker, so it does not add work to the streaming hot path.
pub(crate) async fn run_coalescing_writer<W, T, O>(
    writer: &mut W,
    rx: &flume::Receiver<T>,
    cancel: Option<&CancellationToken>,
    observer: &O,
) where
    W: AsyncWrite + Unpin,
    T: Coalescable,
    O: WriterObserver,
{
    let mut batch = FrameBatchBuffer::new();
    // Items staged into `batch` but not yet written. Held so a failed write can
    // report every item it was carrying, not just the last one.
    let mut staged: Vec<T> = Vec::new();

    'writer: loop {
        // Block for the first item. Cancellation is polled first so a hot send
        // queue cannot starve shutdown.
        let first = tokio::select! {
            biased;
            _ = wait_cancelled(cancel) => break 'writer,
            recv = rx.recv_async() => match recv {
                Ok(item) => item,
                // `recv_async` errors only once the channel is both
                // disconnected *and* drained, so queued items are never lost.
                Err(_) => break 'writer,
            },
        };

        let mut pending = Some(first);
        let mut terminal = false;

        while let Some(item) = pending.take() {
            let staging = batch.classify(item.header().len(), item.payload().len());

            if staging.needs_flush_first()
                && !flush(&mut batch, &mut staged, writer, observer).await
            {
                // `item` never entered the batch, so `flush` did not report it.
                item.on_write_error(FLUSH_FAILED);
                break 'writer;
            }

            if staging == Staging::WriteDirect {
                if let Err((kind, e)) = write_frame_direct(writer, &item).await {
                    observer.on_failure(kind, &e, 1);
                    item.on_write_error(&e.to_string());
                    break 'writer;
                }
                observer.on_flush(1);
                if item.is_terminal() {
                    terminal = true;
                    break;
                }
            } else {
                if let Err(e) = batch.push(item.msg_type(), item.header(), item.payload()) {
                    // Unreachable as written: `classify` sends anything above
                    // COALESCE_THRESHOLD down the direct path, and the codec's
                    // frame limit sits far above it. Handled rather than
                    // panicking so that the two limits drifting apart degrades
                    // to a reported error instead of a corrupt wire.
                    observer.on_failure(WriterFailure::Encode, &e, 1);
                    item.on_write_error(&e.to_string());
                    // Frames already staged are still valid — get them out.
                    flush(&mut batch, &mut staged, writer, observer).await;
                    break 'writer;
                }
                // Read before the move. `is_terminal` is consulted *after*
                // staging so the terminal frame itself still reaches the wire.
                let is_terminal = item.is_terminal();
                if T::REPORTS_ERRORS {
                    staged.push(item);
                } else {
                    // Nothing to report it to; the bytes are already staged.
                    drop(item);
                }
                if is_terminal {
                    terminal = true;
                    break;
                }
            }

            // Take whatever is already queued; never blocks. Cancellation is
            // re-checked every item so a continuously refilled queue cannot
            // keep this drain running across arbitrarily many flushes without
            // revisiting shutdown.
            pending = if is_cancelled(cancel) {
                None
            } else {
                rx.try_recv().ok()
            };
        }

        if !flush(&mut batch, &mut staged, writer, observer).await || terminal {
            break 'writer;
        }
    }

    debug_assert!(
        staged.is_empty(),
        "every exit path flushes or reports the staged batch"
    );
}

/// Write the staged batch. Returns `false` if the writer should stop.
///
/// On failure every item the batch was carrying is reported exactly once —
/// batching must not weaken the per-item error-reporting contract.
async fn flush<W, T, O>(
    batch: &mut FrameBatchBuffer,
    staged: &mut Vec<T>,
    writer: &mut W,
    observer: &O,
) -> bool
where
    W: AsyncWrite + Unpin,
    T: Coalescable,
    O: WriterObserver,
{
    debug_assert!(
        !T::REPORTS_ERRORS || batch.frame_count() == staged.len(),
        "a reporting writer must hold one item per staged frame"
    );
    // Taken from the batch, not from `staged`: a non-reporting writer drops its
    // items at staging time and leaves `staged` empty.
    let frames = batch.frame_count();
    if frames == 0 {
        return true;
    }
    match batch.flush_to(writer).await {
        Ok(()) => {
            staged.clear();
            observer.on_flush(frames);
            true
        }
        Err(e) => {
            observer.on_failure(WriterFailure::Write, &e, frames);
            let reason = e.to_string();
            for item in staged.drain(..) {
                item.on_write_error(&reason);
            }
            false
        }
    }
}

/// Write one frame straight to the socket, skipping the staging buffer.
///
/// This is [`TcpFrameCodec::encode_frame`]'s above-threshold branch — preamble,
/// header, and payload as three writes, so a large payload is never copied.
/// It is spelled out here rather than delegated so that a frame the codec
/// rejects is reported as [`WriterFailure::Encode`] instead of being
/// indistinguishable from a socket error.
async fn write_frame_direct<W, T>(
    writer: &mut W,
    item: &T,
) -> Result<(), (WriterFailure, io::Error)>
where
    W: AsyncWrite + Unpin,
    T: Coalescable,
{
    let header = item.header();
    let payload = item.payload();
    let lengths = u32::try_from(header.len())
        .and_then(|h| u32::try_from(payload.len()).map(|p| (h, p)))
        .map_err(|_| {
            (
                WriterFailure::Encode,
                io::Error::new(io::ErrorKind::InvalidData, "Frame length exceeds u32"),
            )
        });
    let (header_len, payload_len) = lengths?;
    let preamble = TcpFrameCodec::build_preamble(item.msg_type(), header_len, payload_len)
        .map_err(|e| (WriterFailure::Encode, e))?;

    for segment in [&preamble[..], header, payload] {
        writer
            .write_all(segment)
            .await
            .map_err(|e| (WriterFailure::Write, e))?;
    }
    Ok(())
}

/// Completes when `cancel` fires; never completes when there is no token.
async fn wait_cancelled(cancel: Option<&CancellationToken>) {
    match cancel {
        Some(token) => token.cancelled().await,
        None => std::future::pending::<()>().await,
    }
}

#[inline]
fn is_cancelled(cancel: Option<&CancellationToken>) -> bool {
    cancel.is_some_and(|token| token.is_cancelled())
}

#[cfg(test)]
mod tests;
