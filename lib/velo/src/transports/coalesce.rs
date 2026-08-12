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
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = COALESCE_THRESHOLD;

/// Default cap on how many frames one coalesced write may carry.
///
/// Bounds worst-case error attribution: a failed write must report the error
/// for every frame it was carrying, so an unbounded batch would mean an
/// unbounded error fan-out on a single connection fault.
pub(crate) const DEFAULT_MAX_BATCH_FRAMES: usize = 1024;

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
pub(crate) enum Staging {
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
pub(crate) struct FrameBatchBuffer {
    buf: BytesMut,
    frames: usize,
    max_bytes: usize,
    max_frames: usize,
}

impl FrameBatchBuffer {
    pub(crate) fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_BATCH_FRAMES)
    }

    pub(crate) fn with_limits(max_bytes: usize, max_frames: usize) -> Self {
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
    pub(crate) fn frame_count(&self) -> usize {
        self.frames
    }

    /// Decide how to handle a frame of this size.
    #[inline]
    pub(crate) fn classify(&self, header_len: usize, payload_len: usize) -> Staging {
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
    pub(crate) fn push(
        &mut self,
        msg_type: MessageType,
        header: &[u8],
        payload: &[u8],
    ) -> io::Result<()> {
        TcpFrameCodec::append_frame(&mut self.buf, msg_type, header, payload)?;
        self.frames += 1;
        Ok(())
    }

    /// Write every staged frame with a single `write_all` and reset.
    ///
    /// The buffer is cleared whether or not the write succeeded: on failure the
    /// caller owns error reporting for the frames it staged, and retrying a
    /// partially-written frame stream would corrupt the peer's decoder.
    pub(crate) async fn flush_to<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
    ) -> io::Result<()> {
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
mod tests {
    use super::super::tcp::framing::DEFAULT_MAX_FRAME_SIZE;
    use super::*;
    use parking_lot::Mutex;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio_util::codec::Decoder;

    // -----------------------------------------------------------------------
    // Test doubles
    // -----------------------------------------------------------------------

    /// An `AsyncWrite` that records what it was handed, and can be made to fail
    /// or short-write on demand.
    #[derive(Default)]
    struct RecordingSink {
        /// Every byte accepted, in order.
        data: Vec<u8>,
        /// How many times `poll_write` was called.
        poll_writes: usize,
        /// Fail the `poll_write` at this index (0-based).
        fail_at: Option<usize>,
        /// Accept at most this many bytes per `poll_write`, forcing `write_all`
        /// to loop.
        max_per_write: Option<usize>,
        /// Cancelled on the first `poll_write`, to drive shutdown mid-drain.
        cancel_on_write: Option<CancellationToken>,
        /// Live-item counter sampled at each `poll_write`, to observe how many
        /// items the writer was still holding when it flushed.
        live_items: Option<Arc<AtomicUsize>>,
        /// One sample per `poll_write`, in order.
        live_at_write: Vec<usize>,
    }

    impl RecordingSink {
        fn failing_at(idx: usize) -> Self {
            Self {
                fail_at: Some(idx),
                ..Default::default()
            }
        }

        fn decode_frames(&self) -> Vec<(MessageType, Vec<u8>, Vec<u8>)> {
            let mut codec = TcpFrameCodec::new();
            let mut buf = BytesMut::from(&self.data[..]);
            let mut out = Vec::new();
            while let Some((t, h, p)) = codec.decode(&mut buf).expect("decode") {
                out.push((t, h.to_vec(), p.to_vec()));
            }
            assert!(buf.is_empty(), "decoder left {} bytes behind", buf.len());
            out
        }
    }

    impl AsyncWrite for RecordingSink {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let idx = self.poll_writes;
            self.poll_writes += 1;
            if let Some(live) = &self.live_items {
                let n = live.load(Ordering::SeqCst);
                self.live_at_write.push(n);
            }
            if let Some(token) = self.cancel_on_write.take() {
                token.cancel();
            }
            if self.fail_at == Some(idx) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "sink failure",
                )));
            }
            let n = self.max_per_write.map_or(buf.len(), |m| m.min(buf.len()));
            self.data.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Records how the loop reported its progress.
    #[derive(Default)]
    struct TestObserver {
        /// Frame count of each successful flush, in order.
        flushes: Mutex<Vec<usize>>,
        failures: Mutex<Vec<(WriterFailure, usize)>>,
    }

    impl TestObserver {
        fn flushes(&self) -> Vec<usize> {
            self.flushes.lock().clone()
        }
        fn frames_written(&self) -> usize {
            self.flushes.lock().iter().sum()
        }
        fn failures(&self) -> Vec<(WriterFailure, usize)> {
            self.failures.lock().clone()
        }
    }

    impl WriterObserver for TestObserver {
        fn on_flush(&self, frames: usize) {
            self.flushes.lock().push(frames);
        }
        fn on_failure(&self, kind: WriterFailure, _err: &io::Error, frames: usize) {
            self.failures.lock().push((kind, frames));
        }
    }

    /// A `Coalescable` that logs its own failure notifications into a shared
    /// list, so tests can assert each item is reported exactly once.
    struct TestItem {
        tag: String,
        header: Vec<u8>,
        payload: Vec<u8>,
        terminal: bool,
        errors: Arc<Mutex<Vec<String>>>,
    }

    impl Coalescable for TestItem {
        fn msg_type(&self) -> MessageType {
            MessageType::Message
        }
        fn header(&self) -> &[u8] {
            &self.header
        }
        fn payload(&self) -> &[u8] {
            &self.payload
        }
        fn is_terminal(&self) -> bool {
            self.terminal
        }
        fn on_write_error(self, reason: &str) {
            self.errors.lock().push(format!("{}: {reason}", self.tag));
        }
    }

    /// Builds items sharing one error sink.
    struct ItemFactory {
        errors: Arc<Mutex<Vec<String>>>,
    }

    impl ItemFactory {
        fn new() -> Self {
            Self {
                errors: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn item(&self, tag: &str, payload: Vec<u8>) -> TestItem {
            TestItem {
                tag: tag.to_string(),
                header: Vec::new(),
                payload,
                terminal: false,
                errors: Arc::clone(&self.errors),
            }
        }

        fn terminal(&self, tag: &str, payload: Vec<u8>) -> TestItem {
            TestItem {
                terminal: true,
                ..self.item(tag, payload)
            }
        }

        fn errors(&self) -> Vec<String> {
            self.errors.lock().clone()
        }

        /// How many times `tag` was reported as undelivered.
        fn reports_for(&self, tag: &str) -> usize {
            let prefix = format!("{tag}: ");
            self.errors
                .lock()
                .iter()
                .filter(|e| e.starts_with(&prefix))
                .count()
        }
    }

    /// Queue every item, close the channel, then run the writer to completion.
    async fn run_with(items: Vec<TestItem>, sink: &mut RecordingSink, observer: &TestObserver) {
        let (tx, rx) = flume::unbounded::<TestItem>();
        for item in items {
            tx.send(item).expect("queue item");
        }
        drop(tx);
        run_coalescing_writer(sink, &rx, None, observer).await;
    }

    // -----------------------------------------------------------------------
    // Wire compatibility
    // -----------------------------------------------------------------------

    /// The load-bearing property: a batch of N frames produces exactly the
    /// bytes N separate `encode_frame` calls would. That is what makes a
    /// coalescing writer wire-compatible with an unmodified peer.
    #[tokio::test]
    async fn batch_bytes_identical_to_sequential_writes() {
        let frames: Vec<(MessageType, Vec<u8>, Vec<u8>)> = vec![
            (
                MessageType::Message,
                b"h1".to_vec(),
                b"payload-one".to_vec(),
            ),
            (MessageType::Response, Vec::new(), b"two".to_vec()),
            (MessageType::Event, b"hdr3".to_vec(), Vec::new()),
            (MessageType::Ack, Vec::new(), Vec::new()),
        ];

        let mut sequential = Vec::new();
        for (t, h, p) in &frames {
            TcpFrameCodec::encode_frame_sync(&mut sequential, *t, h, p).unwrap();
        }

        let mut batch = FrameBatchBuffer::new();
        for (t, h, p) in &frames {
            batch.push(*t, h, p).unwrap();
        }
        assert_eq!(batch.frame_count(), frames.len());
        let mut batched = Vec::new();
        batch.flush_to(&mut batched).await.unwrap();

        assert_eq!(batched, sequential, "batched bytes must match sequential");
        assert_eq!(batch.frame_count(), 0, "flush resets the buffer");
    }

    /// An unmodified decoder must recover every frame from a coalesced write —
    /// this is what the receiving peer actually does.
    #[tokio::test]
    async fn coalesced_batch_decodes_frame_by_frame() {
        let mut batch = FrameBatchBuffer::new();
        for i in 0..32u8 {
            batch
                .push(MessageType::Message, &[], &[i; 24])
                .expect("push");
        }
        let mut wire = RecordingSink::default();
        batch.flush_to(&mut wire).await.unwrap();

        let decoded = wire.decode_frames();
        assert_eq!(decoded.len(), 32);
        for (i, (msg_type, header, payload)) in decoded.iter().enumerate() {
            assert_eq!(*msg_type, MessageType::Message);
            assert!(header.is_empty());
            assert_eq!(payload.as_slice(), &[i as u8; 24]);
        }
    }

    /// `write_all` may take several `poll_write` calls. Framing has to survive
    /// that, or a coalesced batch would corrupt the peer's decoder.
    #[tokio::test]
    async fn short_writes_preserve_framing() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink {
            // Deliberately not a frame boundary.
            max_per_write: Some(7),
            ..Default::default()
        };

        let items = (0..16u8)
            .map(|i| factory.item(&format!("i{i}"), vec![i; 40]))
            .collect();
        run_with(items, &mut sink, &observer).await;

        assert!(
            sink.poll_writes > 1,
            "the sink must have forced write_all to loop"
        );
        assert_eq!(observer.flushes(), vec![16], "still one logical flush");
        let decoded = sink.decode_frames();
        assert_eq!(decoded.len(), 16);
        for (i, (_, _, payload)) in decoded.iter().enumerate() {
            assert_eq!(payload.as_slice(), &[i as u8; 40]);
        }
        assert!(factory.errors().is_empty(), "nothing failed");
    }

    // -----------------------------------------------------------------------
    // Batching behaviour
    // -----------------------------------------------------------------------

    /// The point of the whole exercise: items already queued go out together.
    #[tokio::test]
    async fn queued_items_coalesce_into_one_flush() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = (0..8u8)
            .map(|i| factory.item(&format!("i{i}"), vec![i; 16]))
            .collect();
        run_with(items, &mut sink, &observer).await;

        assert_eq!(
            observer.flushes(),
            vec![8],
            "eight queued items must leave in one write"
        );
        assert_eq!(sink.decode_frames().len(), 8);
    }

    #[test]
    fn classify_respects_byte_cap() {
        let mut batch = FrameBatchBuffer::with_limits(128, 64);
        assert_eq!(batch.classify(0, 10), Staging::Stage, "empty batch stages");

        batch.push(MessageType::Message, &[], &[0u8; 64]).unwrap();
        assert_eq!(batch.classify(0, 8), Staging::Stage);
        assert_eq!(batch.classify(0, 128), Staging::FlushThenStage);
    }

    #[test]
    fn classify_respects_frame_cap() {
        let mut batch = FrameBatchBuffer::with_limits(1 << 20, 3);
        for _ in 0..3 {
            assert_eq!(batch.classify(0, 1), Staging::Stage);
            batch.push(MessageType::Message, &[], &[0u8; 1]).unwrap();
        }
        assert_eq!(batch.classify(0, 1), Staging::FlushThenStage);
    }

    /// A frame too large to be worth copying keeps the direct segmented write
    /// path, so it never enters the staging buffer. Without this, one 16 MiB
    /// message would leave a per-connection buffer holding 16 MiB for the life
    /// of the connection — `BytesMut::clear` keeps capacity.
    #[tokio::test]
    async fn large_frames_never_enter_the_staging_buffer() {
        let mut batch = FrameBatchBuffer::new();
        assert_eq!(
            batch.classify(0, COALESCE_THRESHOLD + 1),
            Staging::WriteDirect,
            "an oversized frame must not be staged even into an empty batch"
        );

        // Fill and flush repeatedly with the largest stageable frames; capacity
        // must settle at roughly one batch rather than growing without bound.
        for _ in 0..8 {
            while batch.classify(0, 4096) == Staging::Stage {
                batch.push(MessageType::Message, &[], &[7u8; 4096]).unwrap();
            }
            let mut sink = RecordingSink::default();
            batch.flush_to(&mut sink).await.unwrap();
        }
        assert!(
            batch.capacity() <= 4 * DEFAULT_MAX_BATCH_BYTES,
            "staging buffer grew to {} bytes; it should stay near the {}-byte batch cap",
            batch.capacity(),
            DEFAULT_MAX_BATCH_BYTES
        );
    }

    /// The consequence [`FrameBatchBuffer::classify`]'s [`Staging::WriteDirect`]
    /// routing exists to avoid: `BytesMut::clear` keeps capacity, so a staged
    /// large frame would leave every per-connection buffer holding that much
    /// memory for the life of the connection.
    #[tokio::test]
    async fn staging_a_large_frame_would_retain_its_capacity() {
        const BIG: usize = 4 * 1024 * 1024;
        let mut batch = FrameBatchBuffer::new();
        batch
            .push(MessageType::Message, &[], &vec![0u8; BIG])
            .unwrap();
        let mut sink = RecordingSink::default();
        batch.flush_to(&mut sink).await.unwrap();

        assert_eq!(batch.frame_count(), 0, "flush resets the frame count");
        assert!(
            batch.capacity() >= BIG,
            "flushing released {} bytes of capacity; if BytesMut started \
             shrinking on clear, the WriteDirect routing's rationale changed",
            batch.capacity()
        );
    }

    /// A large frame arriving behind staged frames must flush them first, then
    /// go out on its own — order preserved across the two paths.
    #[tokio::test]
    async fn large_frame_flushes_staged_frames_before_writing_direct() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let mut items: Vec<TestItem> = (0..3u8)
            .map(|i| factory.item(&format!("small{i}"), vec![i; 8]))
            .collect();
        items.push(factory.item("big", vec![0xAB; COALESCE_THRESHOLD + 1]));
        items.push(factory.item("after", vec![0xCD; 8]));
        run_with(items, &mut sink, &observer).await;

        assert_eq!(
            observer.flushes(),
            vec![3, 1, 1],
            "three staged, then the large frame alone, then the tail"
        );
        // The large frame went out segmented rather than through the staging
        // buffer: one write for the batch of three, then preamble + payload as
        // separate writes (the header is empty, so `write_all` skips it), then
        // one for the tail. Staging it instead would be three writes total.
        assert_eq!(
            sink.poll_writes, 4,
            "the large frame must take the segmented direct path"
        );

        let decoded = sink.decode_frames();
        assert_eq!(decoded.len(), 5);
        for (i, (_, _, payload)) in decoded.iter().take(3).enumerate() {
            assert_eq!(payload.as_slice(), &[i as u8; 8]);
        }
        assert_eq!(decoded[3].2.len(), COALESCE_THRESHOLD + 1);
        assert!(decoded[3].2.iter().all(|&b| b == 0xAB));
        assert_eq!(decoded[4].2.as_slice(), &[0xCD; 8]);
        assert!(factory.errors().is_empty());
    }

    /// A frame on the direct path is reported as **one** batch even though it
    /// takes several `write_all` calls to put on the wire.
    ///
    /// This is the asymmetry `velo_streaming_egress_flushes_total`'s help text
    /// has to describe. Calling that counter "one write_all each" would be
    /// wrong for exactly this path.
    #[tokio::test]
    async fn direct_write_reports_one_batch_despite_several_writes() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = vec![factory.item("big", vec![0xAB; COALESCE_THRESHOLD + 1])];
        run_with(items, &mut sink, &observer).await;

        assert_eq!(
            observer.flushes(),
            vec![1],
            "one batch carrying one frame, whatever it cost to write"
        );
        assert!(
            sink.poll_writes > 1,
            "the direct path splits the frame across writes ({} here), which is \
             why the counter must be described as batches, not syscalls",
            sink.poll_writes
        );
    }

    // -----------------------------------------------------------------------
    // Terminal and shutdown
    // -----------------------------------------------------------------------

    /// A terminal item batched alongside data must still reach the wire, and
    /// nothing queued behind it may follow.
    #[tokio::test]
    async fn terminal_flushes_its_batch_then_stops() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = vec![
            factory.item("a", vec![1; 8]),
            factory.item("b", vec![2; 8]),
            factory.terminal("fin", vec![3; 8]),
            factory.item("after-terminal", vec![4; 8]),
        ];
        run_with(items, &mut sink, &observer).await;

        let decoded = sink.decode_frames();
        assert_eq!(
            decoded.len(),
            3,
            "frames staged ahead of the terminal must be written, and nothing after it"
        );
        assert_eq!(decoded[2].2.as_slice(), &[3; 8]);
        assert_eq!(observer.flushes(), vec![3]);
    }

    /// Closing the channel must not lose what is still queued.
    #[tokio::test]
    async fn channel_close_drains_remaining_items() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = (0..5u8)
            .map(|i| factory.item(&format!("i{i}"), vec![i; 8]))
            .collect();
        run_with(items, &mut sink, &observer).await;

        assert_eq!(sink.decode_frames().len(), 5);
        assert!(factory.errors().is_empty());
    }

    /// The inner drain re-checks cancellation, so a queue that keeps refilling
    /// cannot hold shutdown across arbitrarily many flushes.
    ///
    /// Driven deterministically: the sink cancels the token on its first write,
    /// and the payloads are sized so the byte cap forces that first flush long
    /// before the queue is exhausted. Without the per-item check the loop would
    /// drain all 200 items before revisiting the token.
    #[tokio::test]
    async fn cancellation_interrupts_a_refilled_queue() {
        const QUEUED: usize = 200;
        // Eight of these exceed DEFAULT_MAX_BATCH_BYTES, so the first flush
        // lands after ~7 items.
        const PAYLOAD: usize = 8 * 1024;

        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let cancel = CancellationToken::new();
        let mut sink = RecordingSink {
            cancel_on_write: Some(cancel.clone()),
            ..Default::default()
        };

        let (tx, rx) = flume::unbounded::<TestItem>();
        for i in 0..QUEUED {
            tx.send(factory.item(&format!("i{i}"), vec![0u8; PAYLOAD]))
                .expect("queue");
        }
        // Keep the channel open so only cancellation can stop the loop.
        let _tx = tx;

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_coalescing_writer(&mut sink, &rx, Some(&cancel), &observer),
        )
        .await
        .expect("cancellation must stop the writer promptly");

        assert!(
            observer.frames_written() < QUEUED,
            "cancellation must interrupt the drain, but all {QUEUED} items were written"
        );
        assert!(
            !rx.is_empty(),
            "items should remain queued for the caller's own drain to report"
        );
    }

    // -----------------------------------------------------------------------
    // Item retention
    // -----------------------------------------------------------------------

    /// An item that counts itself live, so a test can see how many the writer
    /// was still holding at flush time.
    struct CountedItem<const REPORTS: bool> {
        payload: Vec<u8>,
        live: Arc<AtomicUsize>,
    }

    impl<const REPORTS: bool> CountedItem<REPORTS> {
        fn new(live: &Arc<AtomicUsize>, payload: Vec<u8>) -> Self {
            live.fetch_add(1, Ordering::SeqCst);
            Self {
                payload,
                live: Arc::clone(live),
            }
        }
    }

    impl<const REPORTS: bool> Drop for CountedItem<REPORTS> {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl<const REPORTS: bool> Coalescable for CountedItem<REPORTS> {
        const REPORTS_ERRORS: bool = REPORTS;
        fn msg_type(&self) -> MessageType {
            MessageType::Message
        }
        fn header(&self) -> &[u8] {
            &[]
        }
        fn payload(&self) -> &[u8] {
            &self.payload
        }
    }

    async fn live_items_at_flush<const REPORTS: bool>() -> Vec<usize> {
        let live = Arc::new(AtomicUsize::new(0));
        let observer = TestObserver::default();
        let mut sink = RecordingSink {
            live_items: Some(Arc::clone(&live)),
            ..Default::default()
        };

        let (tx, rx) = flume::unbounded::<CountedItem<REPORTS>>();
        for i in 0..8u8 {
            tx.send(CountedItem::<REPORTS>::new(&live, vec![i; 16]))
                .expect("queue");
        }
        drop(tx);
        run_coalescing_writer(&mut sink, &rx, None, &observer).await;

        assert_eq!(observer.flushes(), vec![8], "all eight in one flush");
        assert_eq!(live.load(Ordering::SeqCst), 0, "everything dropped by exit");
        sink.live_at_write
    }

    /// A writer that reports errors has to keep every item until its batch
    /// reaches the wire — that is what makes per-item error fan-out possible.
    #[tokio::test]
    async fn reporting_writer_holds_items_until_flush() {
        assert_eq!(
            live_items_at_flush::<true>().await,
            vec![8],
            "all eight items must still be alive when the batch is written"
        );
    }

    /// A writer with no error handler must not: the staging buffer already
    /// holds a copy of the bytes, so keeping the originals alive would double
    /// live memory on the streaming egress hot path.
    #[tokio::test]
    async fn non_reporting_writer_drops_items_as_it_stages_them() {
        assert_eq!(
            live_items_at_flush::<false>().await,
            vec![0],
            "items must be dropped at staging time, not held until flush"
        );
    }

    // -----------------------------------------------------------------------
    // Error reporting — one notification per unwritten item, none per written
    // -----------------------------------------------------------------------

    /// A failed write must report every item the batch was carrying.
    #[tokio::test]
    async fn write_failure_reports_every_staged_item() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::failing_at(0);

        let items = (0..5u8)
            .map(|i| factory.item(&format!("i{i}"), vec![i; 8]))
            .collect();
        run_with(items, &mut sink, &observer).await;

        let errors = factory.errors();
        assert_eq!(errors.len(), 5, "all five items reported: {errors:?}");
        for i in 0..5 {
            assert_eq!(
                factory.reports_for(&format!("i{i}")),
                1,
                "item i{i} must be reported exactly once: {errors:?}"
            );
        }
        assert_eq!(observer.failures(), vec![(WriterFailure::Write, 5)]);
    }

    /// The item held while a mandatory flush fails is not in the batch, so it
    /// owes its own notification — and must not get a second one.
    #[tokio::test]
    async fn flush_failure_before_staging_reports_the_held_item_once() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        // The very first flush fails. The large second item is what forces
        // that flush to happen before it can be handled.
        let mut sink = RecordingSink::failing_at(0);

        let items = vec![
            factory.item("staged", vec![1; 8]),
            factory.item("held", vec![2; COALESCE_THRESHOLD + 1]),
        ];
        run_with(items, &mut sink, &observer).await;

        let errors = factory.errors();
        assert_eq!(errors.len(), 2, "both items reported: {errors:?}");
        assert_eq!(factory.reports_for("staged"), 1);
        assert_eq!(factory.reports_for("held"), 1);
        assert!(
            errors.iter().any(|e| e == &format!("held: {FLUSH_FAILED}")),
            "the held item must carry the flush-failure reason: {errors:?}"
        );
    }

    /// A frame the codec rejects is reported as an encode failure, not a write
    /// failure, and the valid frames staged ahead of it still reach the wire.
    #[tokio::test]
    async fn encode_failure_flushes_staged_frames_and_reports_the_offender() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = vec![
            factory.item("good", vec![1; 8]),
            factory.item("bad", vec![0u8; (DEFAULT_MAX_FRAME_SIZE as usize) + 1]),
        ];
        run_with(items, &mut sink, &observer).await;

        assert_eq!(
            sink.decode_frames().len(),
            1,
            "the frame staged before the bad one must still be written"
        );
        let errors = factory.errors();
        assert_eq!(errors.len(), 1, "only the offender is reported: {errors:?}");
        assert_eq!(factory.reports_for("bad"), 1);
        assert_eq!(observer.failures(), vec![(WriterFailure::Encode, 1)]);
        assert_eq!(observer.flushes(), vec![1], "the good frame flushed");
    }

    /// A direct write that fails reports only the item it was carrying.
    #[tokio::test]
    async fn direct_write_failure_reports_only_that_item() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::failing_at(0);

        let items = vec![factory.item("big", vec![0xAB; COALESCE_THRESHOLD + 1])];
        run_with(items, &mut sink, &observer).await;

        assert_eq!(factory.errors().len(), 1, "{:?}", factory.errors());
        assert_eq!(factory.reports_for("big"), 1);
        assert_eq!(observer.failures(), vec![(WriterFailure::Write, 1)]);
    }

    /// Nothing is reported for items that reached the wire.
    #[tokio::test]
    async fn successful_writes_report_nothing() {
        let factory = ItemFactory::new();
        let observer = TestObserver::default();
        let mut sink = RecordingSink::default();

        let items = (0..12u8)
            .map(|i| factory.item(&format!("i{i}"), vec![i; 32]))
            .collect();
        run_with(items, &mut sink, &observer).await;

        assert_eq!(sink.decode_frames().len(), 12);
        assert!(factory.errors().is_empty());
        assert!(observer.failures().is_empty());
    }
}
