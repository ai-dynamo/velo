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
//! [`Coalescable::fail`](crate::transports::coalesce::Coalescable::fail)
//! if it did not reach the wire, and **none** if it did. The caller's own
//! post-loop drain (e.g. `connection_writer_task`) covers items still sitting
//! in the channel, but it cannot see an item this loop is holding — so the
//! three paths that hold one (a mandatory flush failing, a rejected encode, a
//! failed direct write) report it themselves.
//!
//! There is exactly **one** retention model. Staging an item copies its bytes
//! into the batch buffer and converts what is left into a
//! [`Coalescable::FailureToken`](crate::transports::coalesce::Coalescable::FailureToken),
//! and the writer holds the batch's tokens until the batch reaches the wire. A
//! path with no per-frame error handler sets `FailureToken = ()`, so its token
//! vector is zero-sized, retention costs nothing, and the frame's bytes are
//! released the instant they are staged.
//!
//! # Where items come from
//!
//! `rx` and `wrap` are a pair: the loop takes whatever the channel carries and
//! wraps it into the item type. The messenger writers own their channel's item
//! type and pass [`std::convert::identity`]; the streaming egress channel is
//! fixed to `Vec<u8>` by `FrameTransport::connect`, so its pump wraps each
//! frame into a streaming-local newtype right here. That keeps the terminal
//! semantics `Coalescable` carries attached to one path instead of landing on
//! a crate-wide `impl Coalescable for Vec<u8>` that any future caller would
//! silently inherit. Wrapping is a move into a transparent newtype — no
//! allocation, no copy.

use std::io;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use velo_ext::MessageType;

use super::tcp::framing::{
    COALESCE_THRESHOLD, DIRECT_PREFIX_CAP, MIN_HEADER_SIZE, TcpFrameCodec, stage_direct_prefix,
};

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

/// Reason handed to [`Coalescable::fail`] when an item was dropped because the
/// flush that had to precede it failed.
const FLUSH_FAILED: &str = "batch flush failed";

/// How many frames of each [`MessageType`] one write carried.
///
/// A fixed array rather than a `Vec<MessageType>` kept beside the failure
/// tokens: this is written once per staged frame on the writer's hot path and
/// read once per write, and the streaming egress pump — whose failure token is
/// `()` precisely so that retention costs it nothing — must not start paying
/// for a per-frame push it never reads.
pub(crate) struct FrameTally([u64; MESSAGE_TYPE_SLOTS]);

/// One tally slot per [`MessageType`] discriminant.
///
/// Derived from `ShuttingDown` because that variant holds the largest
/// discriminant today, which is an assumption and not a guarantee: a variant
/// given an explicit discriminant above the contiguous run would leave this
/// constant behind and make [`FrameTally::add`]'s index panic the connection
/// writer. `tally_has_one_slot_per_message_type` is what fails first — it
/// walks every byte [`MessageType::from_u8`] accepts, not just the ones below
/// this bound.
pub(crate) const MESSAGE_TYPE_SLOTS: usize = MessageType::ShuttingDown as usize + 1;

impl Default for FrameTally {
    fn default() -> Self {
        Self([0; MESSAGE_TYPE_SLOTS])
    }
}

impl FrameTally {
    /// Count one frame.
    #[inline]
    fn add(&mut self, msg_type: MessageType) {
        self.0[msg_type as usize] += 1;
    }

    /// Forget the frames counted so far. Called after every write, successful
    /// or not: a failed write's frames never reached the wire and are reported
    /// through [`Coalescable::fail`] instead.
    #[inline]
    fn clear(&mut self) {
        self.0 = [0; MESSAGE_TYPE_SLOTS];
    }

    /// The non-zero counts, as `(message type, frames)`.
    pub(crate) fn counts(&self) -> impl Iterator<Item = (MessageType, u64)> + '_ {
        self.0
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count > 0)
            .map(|(idx, &count)| {
                let msg_type =
                    MessageType::from_u8(idx as u8).expect("every tally slot names a MessageType");
                (msg_type, count)
            })
    }
}

/// The writer's egress bookkeeping: the tally for the write in flight, and the
/// one-time answer to [`WriterObserver::records_egress`].
///
/// Asking the observer once, at writer start, is what keeps the instruments
/// free for a writer that has no metrics handle — the streaming egress pump
/// runs this same loop on the data plane's hot path, and it takes no
/// timestamps at all.
struct EgressLog {
    tally: FrameTally,
    enabled: bool,
}

impl EgressLog {
    fn new<O: WriterObserver>(observer: &O) -> Self {
        Self {
            tally: FrameTally::default(),
            enabled: observer.records_egress(),
        }
    }

    /// Report one item leaving the send queue.
    #[inline]
    fn dequeued<T: Coalescable, O: WriterObserver>(&self, observer: &O, item: &T) {
        if self.enabled
            && let Some(queued_at) = item.queued_at()
        {
            observer.on_dequeue(queued_at.elapsed());
        }
    }

    /// Count one frame into the write being assembled.
    #[inline]
    fn staged(&mut self, msg_type: MessageType) {
        if self.enabled {
            self.tally.add(msg_type);
        }
    }

    /// Open a write bracket.
    #[inline]
    fn started(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    /// Close a bracket whose write reached the wire.
    #[inline]
    fn written<O: WriterObserver>(&mut self, observer: &O, started: Option<Instant>) {
        if let Some(started) = started {
            observer.on_write(&self.tally, started.elapsed());
        }
        self.tally.clear();
    }

    /// Close a bracket whose write failed. Nothing reached the wire, so
    /// nothing is counted written.
    #[inline]
    fn failed(&mut self) {
        self.tally.clear();
    }
}

/// An item a coalescing writer can put on the wire.
///
/// Implementors supply the three frame fields, what to retain for failure
/// reporting, and — optionally — terminal semantics.
pub(crate) trait Coalescable: Sized {
    /// What the writer retains per staged item so it can report a failure.
    ///
    /// Staging copies an item's bytes into the batch buffer, so from that
    /// point the item itself is dead weight: all it still owes is the
    /// notification a failed write must produce. Naming that residue gives the
    /// writer a single retention model — it always holds one token per staged
    /// frame — instead of one model for paths that report errors and another
    /// for paths that do not.
    ///
    /// `()` for paths with no per-frame error handler. `Vec<()>` is
    /// zero-sized and never allocates, so retention is free and the item drops
    /// as its bytes are staged.
    type FailureToken: Send;

    /// Frame type written in the preamble.
    fn msg_type(&self) -> MessageType;

    /// Frame header bytes.
    fn header(&self) -> &[u8];

    /// Frame payload bytes.
    fn payload(&self) -> &[u8];

    /// Convert the item into its failure token once its bytes are staged.
    ///
    /// Consumes the item so an implementor can move owned fields into the
    /// token rather than cloning them on the failure path.
    fn into_failure_token(self) -> Self::FailureToken;

    /// Report that a staged item did not reach the wire.
    ///
    /// `reason` is the bare cause — an `io::Error` rendering, or
    /// [`FLUSH_FAILED`] — so implementors can add their own
    /// transport-identifying prefix. Called at most once per token, and never
    /// for an item that was written.
    fn fail(token: Self::FailureToken, reason: &str);

    /// When the transport accepted this item, if it stamps one.
    ///
    /// `None` by default, and `None` for a transport running without an
    /// observability handle: the stamp costs a clock read per send, so it is
    /// taken only when something will read it. The streaming egress pump never
    /// stamps — its queueing is already covered by the streaming credit
    /// metrics.
    fn queued_at(&self) -> Option<Instant> {
        None
    }

    /// Whether the writer should stop after this item reaches the wire.
    ///
    /// Used by the streaming egress pump: after a terminal sentinel
    /// (Finalized / Dropped / Detached / TransportError) anything still queued
    /// is discarded, because sending it would race the consumer's
    /// post-terminal cleanup and trigger spurious resets on the wire.
    fn is_terminal(&self) -> bool {
        false
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

    /// Whether this writer's egress instruments are live.
    ///
    /// Read exactly once, when the writer starts. `false` buys the loop out of
    /// every timestamp and every tally increment, which is what lets the
    /// streaming egress pump share this code without paying for instruments it
    /// does not publish.
    fn records_egress(&self) -> bool {
        false
    }

    /// One item came off the send queue, `waited` after the transport accepted
    /// it. Called only for items that carry a [`Coalescable::queued_at`] stamp.
    fn on_dequeue(&self, _waited: Duration) {}

    /// One write reached the wire in `elapsed`, carrying the frames in `tally`.
    ///
    /// Not called for a failed write: those frames are reported through
    /// [`Coalescable::fail`] and must not be counted as written.
    fn on_write(&self, _tally: &FrameTally, _elapsed: Duration) {}
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
///
/// `wrap` turns a channel item into a writer item; see *Where items come from*
/// in the module docs for why it exists. Pass [`std::convert::identity`] when
/// the channel already carries the item type.
pub(crate) async fn run_coalescing_writer<W, I, T, O>(
    writer: &mut W,
    rx: &flume::Receiver<I>,
    wrap: impl Fn(I) -> T,
    cancel: Option<&CancellationToken>,
    observer: &O,
) where
    W: AsyncWrite + Unpin,
    T: Coalescable,
    O: WriterObserver,
{
    let mut batch = FrameBatchBuffer::new();
    // One token per frame staged into `batch` but not yet written, so a failed
    // write can report every frame it was carrying rather than just the last.
    // Zero-sized, and never allocating, when `FailureToken = ()`.
    let mut staged: Vec<T::FailureToken> = Vec::new();
    let mut egress = EgressLog::new(observer);

    'writer: loop {
        // Block for the first item. Cancellation is polled first so a hot send
        // queue cannot starve shutdown.
        let first = tokio::select! {
            biased;
            _ = wait_cancelled(cancel) => break 'writer,
            recv = rx.recv_async() => match recv {
                Ok(item) => wrap(item),
                // `recv_async` errors only once the channel is both
                // disconnected *and* drained, so queued items are never lost.
                Err(_) => break 'writer,
            },
        };
        egress.dequeued(observer, &first);

        let mut pending = Some(first);
        let mut terminal = false;

        while let Some(item) = pending.take() {
            let staging = batch.classify(item.header().len(), item.payload().len());

            if staging.needs_flush_first()
                && !flush::<_, T, _>(&mut batch, &mut staged, writer, observer, &mut egress).await
            {
                // `item` never entered the batch, so `flush` did not report it.
                T::fail(item.into_failure_token(), FLUSH_FAILED);
                break 'writer;
            }

            if staging == Staging::WriteDirect {
                // The preceding flush emptied the tally, so this write carries
                // exactly this one frame.
                egress.staged(item.msg_type());
                let started = egress.started();
                if let Err((kind, e)) = write_frame_direct(writer, &item).await {
                    egress.failed();
                    observer.on_failure(kind, &e, 1);
                    T::fail(item.into_failure_token(), &e.to_string());
                    break 'writer;
                }
                observer.on_flush(1);
                egress.written(observer, started);
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
                    T::fail(item.into_failure_token(), &e.to_string());
                    // Frames already staged are still valid — get them out.
                    flush::<_, T, _>(&mut batch, &mut staged, writer, observer, &mut egress).await;
                    break 'writer;
                }
                // Read before the move. `is_terminal` is consulted *after*
                // staging so the terminal frame itself still reaches the wire.
                let is_terminal = item.is_terminal();
                egress.staged(item.msg_type());
                // The bytes are in the batch now; all the item still owes is
                // its failure notification, so only that is kept.
                staged.push(item.into_failure_token());
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
                rx.try_recv().ok().map(&wrap)
            };
            if let Some(item) = pending.as_ref() {
                egress.dequeued(observer, item);
            }
        }

        if !flush::<_, T, _>(&mut batch, &mut staged, writer, observer, &mut egress).await
            || terminal
        {
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
/// On failure every frame the batch was carrying is reported exactly once —
/// batching must not weaken the per-item error-reporting contract.
///
/// The item type cannot be inferred from a `Vec<T::FailureToken>` argument, so
/// callers name it: `flush::<_, T, _>(..)`.
async fn flush<W, T, O>(
    batch: &mut FrameBatchBuffer,
    staged: &mut Vec<T::FailureToken>,
    writer: &mut W,
    observer: &O,
    egress: &mut EgressLog,
) -> bool
where
    W: AsyncWrite + Unpin,
    T: Coalescable,
    O: WriterObserver,
{
    // Unconditional: there is one retention model, so a token per staged frame
    // holds for every writer — including one whose token is `()`.
    debug_assert_eq!(
        staged.len(),
        batch.frame_count(),
        "the writer must hold one failure token per staged frame"
    );
    let frames = batch.frame_count();
    if frames == 0 {
        return true;
    }
    let started = egress.started();
    match batch.flush_to(writer).await {
        Ok(()) => {
            staged.clear();
            observer.on_flush(frames);
            egress.written(observer, started);
            true
        }
        Err(e) => {
            observer.on_failure(WriterFailure::Write, &e, frames);
            egress.failed();
            let reason = e.to_string();
            for token in staged.drain(..) {
                T::fail(token, &reason);
            }
            false
        }
    }
}

/// Write one frame straight to the socket, skipping the staging buffer.
///
/// This is [`TcpFrameCodec::encode_frame`]'s above-threshold branch — preamble
/// and header staged into one stack buffer and written ahead of the payload
/// (two writes; three when a header too large for [`DIRECT_PREFIX_CAP`] falls
/// back to its own segment), so a large payload is never copied. It is spelled
/// out here rather than delegated so that a frame the codec rejects is
/// reported as [`WriterFailure::Encode`] instead of being indistinguishable
/// from a socket error.
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

    let mut prefix = [0u8; DIRECT_PREFIX_CAP];
    let segments: &[&[u8]] = match stage_direct_prefix(&preamble, header, &mut prefix) {
        Some(len) => &[&prefix[..len], payload],
        None => &[&preamble[..], header, payload],
    };
    for segment in segments {
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
