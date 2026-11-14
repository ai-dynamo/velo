// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `VeloBuilder::staged_payload_max`: a payload a peer stages is checked
//! against the receiver's limit before anything of its size is allocated.
//!
//! A payload over the eager threshold (256 KiB) is staged by its sender, and
//! the receiver pulls it before the handler runs. The pull used to allocate
//! the size the owner stated, whatever it was, so one peer could make another
//! allocate up to 4 GiB per message. The check is on that stated size.
//!
//! The producer and the consumer run in one process, over TCP on ports the OS
//! picks, each on a tokio runtime of its own. The consumer's threads are
//! tagged, so the allocator records the largest single allocation the consumer
//! makes; the producer's own copy of the payload is not counted.
//!
//! A refusal of a stream item must end that one stream with an error, at once.
//! The staged record is never read, so without the slot named in its headers
//! the consumer would see a gap in its stream that never closes, and would
//! wait for the stream watchdog.

#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use velo::streaming::{StreamAnchor, StreamError, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Handler, Velo};

struct Counting;

thread_local! {
    /// Which test's consumer this thread runs for; 0 for none.
    static ROLE: Cell<usize> = const { Cell::new(0) };
}

/// The largest single allocation each consumer made, by role.
static LARGEST: [AtomicUsize; 4] = [const { AtomicUsize::new(0) }; 4];

fn record(size: usize) {
    // `try_with`: a thread's locals may be gone while it exits.
    let role = ROLE.try_with(Cell::get).unwrap_or(0);
    if role != 0 {
        LARGEST[role].fetch_max(size, Ordering::Relaxed);
    }
}

// SAFETY: every allocation is forwarded unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The consumer's limit: small, so the payloads stay cheap to build.
const LIMIT: usize = 1 << 20;

/// Far over [`LIMIT`], so a consumer that allocated it would show at once.
const BIG: usize = 64 << 20;

/// Every wait here is for an event that comes within milliseconds; the bound
/// only turns a defect into a failure instead of a hang.
const PATIENCE: Duration = Duration::from_secs(10);

/// A runtime whose threads count their allocations under `role`, or count
/// nothing when `role` is 0.
fn runtime(role: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || ROLE.with(|r| r.set(role)))
        .build()
        .expect("runtime")
}

async fn node(limit: Option<usize>) -> Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let transport = TcpTransportBuilder::new()
        .from_listener(listener)
        .expect("from_listener")
        .build()
        .expect("build transport");
    let builder = Velo::builder()
        .add_transport(Arc::new(transport))
        .mux_only();
    let builder = match limit {
        Some(limit) => builder.staged_payload_max(limit),
        None => builder,
    };
    builder.build().await.expect("build velo")
}

/// A producer node on an untagged runtime and a consumer node, limited to
/// [`LIMIT`], on a runtime tagged `role`. Both runtimes are returned so they
/// live as long as the test.
struct Pair {
    producer_rt: tokio::runtime::Runtime,
    consumer_rt: tokio::runtime::Runtime,
    producer: Velo,
    consumer: Velo,
    role: usize,
}

impl Pair {
    fn new(role: usize) -> Self {
        let producer_rt = runtime(0);
        let consumer_rt = runtime(role);
        let producer = producer_rt.block_on(node(None));
        let consumer = consumer_rt.block_on(node(Some(LIMIT)));
        producer
            .register_peer(consumer.peer_info())
            .expect("register consumer");
        consumer
            .register_peer(producer.peer_info())
            .expect("register producer");
        for (from, to, rt) in [
            (&producer, &consumer, &producer_rt),
            (&consumer, &producer, &consumer_rt),
        ] {
            rt.block_on(async {
                tokio::time::timeout(
                    PATIENCE,
                    from.wait_for_handler(to.instance_id(), "_anchor_attach"),
                )
                .await
                .expect("timed out waiting for the peer's control plane")
                .expect("peer never advertised the handler");
            });
        }
        Self {
            producer_rt,
            consumer_rt,
            producer,
            consumer,
            role,
        }
    }

    /// Run `f` on the consumer's runtime, from a thread tagged as the
    /// consumer, and return the largest single allocation the consumer made
    /// while it ran, with its result.
    fn on_consumer<T>(&self, f: impl std::future::Future<Output = T>) -> (T, usize) {
        ROLE.with(|r| r.set(self.role));
        LARGEST[self.role].store(0, Ordering::Relaxed);
        let out = self.consumer_rt.block_on(f);
        let largest = LARGEST[self.role].load(Ordering::Relaxed);
        ROLE.with(|r| r.set(0));
        (out, largest)
    }

    /// Whether the producer still holds a staged payload of at least `len`
    /// bytes. Slot ids are handed out in order, so a probe registered now has
    /// an id past every earlier one.
    fn producer_holds(&self, len: usize) -> bool {
        let (_, probe) = self
            .producer
            .register_data(Bytes::from_static(b"probe"))
            .unpack();
        (0..probe)
            .filter_map(|id| self.producer.staged_payload_len(id))
            .any(|total_len| total_len >= len as u64)
    }
}

/// What a consumer read from its stream, in order, as text.
async fn read_all(anchor: &mut StreamAnchor<Bytes>) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        match tokio::time::timeout(PATIENCE, anchor.next()).await {
            Err(_) => {
                seen.push("stalled".to_string());
                return seen;
            }
            Ok(None) => return seen,
            Ok(Some(Ok(StreamFrame::Item(bytes)))) => seen.push(format!("item {}", bytes.len())),
            Ok(Some(Ok(StreamFrame::Finalized))) => {
                seen.push("finalized".to_string());
                return seen;
            }
            Ok(Some(Ok(other))) => seen.push(format!("{other:?}")),
            Ok(Some(Err(StreamError::TransportError(why)))) => {
                seen.push(format!("transport error: {why}"));
                return seen;
            }
            Ok(Some(Err(error))) => {
                seen.push(format!("error: {error}"));
                return seen;
            }
        }
    }
}

/// Whether `text` names a number from `from` to `from + 4096`: the stated
/// size of a staged item, which is the item and the mux's framing.
fn names_size(text: &str, from: usize) -> bool {
    text.split(|c: char| !c.is_ascii_digit())
        .filter_map(|n| n.parse::<usize>().ok())
        .any(|n| (from..from + 4096).contains(&n))
}

/// Two streams from one producer to one consumer. One of them carries a
/// 64 MiB item to a consumer whose limit is 1 MiB.
///
/// - The consumer never allocates anything near the stated size.
/// - That stream ends with a `TransportError` that names the limit and the
///   stated size, after the item sent before it; it does not wait for its
///   watchdog.
/// - Its producer is told to stop: a later send fails.
/// - The other stream, on the same peer, carries its items before and after
///   the refusal and finishes.
/// - The producer's staged copy is released.
#[test]
fn a_refused_item_allocates_nothing_of_its_size_and_ends_only_its_stream() {
    let pair = Pair::new(1);
    let (mut refused, mut other) = pair.consumer_rt.block_on(async {
        (
            pair.consumer.create_anchor::<Bytes>(),
            pair.consumer.create_anchor::<Bytes>(),
        )
    });
    let (refused_handle, other_handle) = (refused.handle(), other.handle());
    let producer = pair.producer.clone();
    let (refused_tx, other_tx) = pair.producer_rt.block_on(async {
        (
            producer
                .attach_anchor::<Bytes>(refused_handle)
                .await
                .expect("attach"),
            producer
                .attach_anchor::<Bytes>(other_handle)
                .await
                .expect("attach"),
        )
    });

    let (go_on_tx, go_on) = tokio::sync::oneshot::channel::<()>();
    let other_writer = pair.producer_rt.spawn(async move {
        other_tx.send_bytes(b"before").await.expect("send before");
        go_on.await.expect("the test lets the other stream go on");
        other_tx.send_bytes(b"after").await.expect("send after");
        other_tx.finalize().expect("finalize");
    });
    let refused_writer = pair.producer_rt.spawn(async move {
        refused_tx.send_bytes(b"small").await.expect("send small");
        refused_tx
            .send_bytes(&vec![7u8; BIG])
            .await
            .expect("the big item is handed to the mux");
        // The refusal reaches the producer as a close of its slot.
        tokio::time::timeout(PATIENCE, async {
            loop {
                if refused_tx.send_bytes(b"more").await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the producer of the refused stream is told to stop");
    });

    let ((seen, first_of_other), largest) = pair.on_consumer(async {
        let first_of_other = read_all_until_one(&mut other).await;
        let seen = read_all(&mut refused).await;
        (seen, first_of_other)
    });
    assert_eq!(first_of_other, "item 6", "the other stream's first item");
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[0], "item 5", "the item before the refused one");
    let error = &seen[1];
    assert!(error.starts_with("transport error:"), "{seen:?}");
    assert!(
        error.contains(&LIMIT.to_string()),
        "names the limit: {error}"
    );
    assert!(names_size(error, BIG), "names the stated size: {error}");
    assert!(
        largest < BIG / 4,
        "the consumer allocated {largest} bytes at once for a {BIG}-byte item over a {LIMIT}-byte limit"
    );

    go_on_tx.send(()).expect("the other writer waits");
    let (rest_of_other, _) = pair.on_consumer(read_all(&mut other));
    assert_eq!(rest_of_other, ["item 5", "finalized"]);
    pair.producer_rt.block_on(async {
        tokio::time::timeout(PATIENCE, refused_writer)
            .await
            .expect("refused writer ends")
            .expect("refused writer");
        tokio::time::timeout(PATIENCE, other_writer)
            .await
            .expect("other writer ends")
            .expect("other writer");
    });

    let released = pair.producer_rt.block_on(async {
        tokio::time::timeout(PATIENCE, async {
            while pair.producer_holds(BIG) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    });
    assert!(released, "the producer still holds the refused payload");
}

/// The first item of a stream, as [`read_all`] names it.
async fn read_all_until_one(anchor: &mut StreamAnchor<Bytes>) -> String {
    match tokio::time::timeout(PATIENCE, anchor.next()).await {
        Ok(Some(Ok(StreamFrame::Item(bytes)))) => format!("item {}", bytes.len()),
        other => format!("{other:?}"),
    }
}

/// The control: an item just under the limit, past the eager threshold so it
/// is staged too, crosses whole.
#[test]
fn an_item_under_the_limit_crosses() {
    let pair = Pair::new(2);
    let mut anchor = pair
        .consumer_rt
        .block_on(async { pair.consumer.create_anchor::<Bytes>() });
    let handle = anchor.handle();
    let producer = pair.producer.clone();
    let len = LIMIT - (4 << 10);
    let writer = pair.producer_rt.spawn(async move {
        let tx = producer
            .attach_anchor::<Bytes>(handle)
            .await
            .expect("attach");
        tx.send_bytes(&vec![7u8; len]).await.expect("send");
        tx.finalize().expect("finalize");
    });
    let (seen, largest) = pair.on_consumer(read_all(&mut anchor));
    assert_eq!(seen, [format!("item {len}"), "finalized".to_string()]);
    assert!(
        largest >= len,
        "the consumer holds the item: {largest} bytes"
    );
    pair.producer_rt
        .block_on(async { tokio::time::timeout(PATIENCE, writer).await })
        .expect("writer ends")
        .expect("writer");
}

/// A unary request whose staged payload is over the limit fails with an
/// error that names both sizes, and its handler never runs.
#[test]
fn a_refused_request_gets_an_error_that_names_both_sizes() {
    let pair = Pair::new(3);
    let ran = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    pair.consumer
        .register_handler(
            Handler::unary_handler("echo", move |ctx| {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(Some(ctx.payload))
            })
            .build(),
        )
        .expect("register");
    let producer = pair.producer.clone();
    let consumer = pair.consumer.instance_id();
    let len = 2 * LIMIT;
    let reply = pair.producer_rt.block_on(async move {
        tokio::time::timeout(
            PATIENCE,
            producer
                .unary("echo")
                .expect("unary")
                .raw_payload(Bytes::from(vec![7u8; len]))
                .instance(consumer)
                .send(),
        )
        .await
        .expect("the reply comes within its bound")
    });
    let error = match reply {
        Ok(echo) => panic!(
            "a {len}-byte request over a {LIMIT}-byte limit was answered with {} bytes",
            echo.len()
        ),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains(&LIMIT.to_string()),
        "names the limit: {error}"
    );
    assert!(names_size(&error, len), "names the stated size: {error}");
    assert_eq!(ran.load(Ordering::Relaxed), 0, "the handler never ran");
}
