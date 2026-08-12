// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tests for the shared coalescing writer.

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
