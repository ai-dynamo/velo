// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `DispatchMode::Ordered`.
//!
//! The guarantee under test is that messages from one sending instance reach
//! the handler in the order that instance sent them, while messages from
//! different instances may run in parallel.
//!
//! Note that `ordered_preserves_per_sender_order` is the load-bearing test: if
//! it is ever changed so that it passes under `.spawn()` too, it has stopped
//! testing anything.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::*;

fn new_transport() -> Arc<TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

async fn new_node() -> Arc<Velo> {
    let node = Velo::builder()
        .add_transport(new_transport())
        .build()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    node
}

/// Build a receiver plus `sender_count` senders, all mutually registered.
async fn cluster(sender_count: usize) -> (Arc<Velo>, Vec<Arc<Velo>>) {
    let receiver = new_node().await;
    let mut senders = Vec::with_capacity(sender_count);
    for _ in 0..sender_count {
        senders.push(new_node().await);
    }
    for sender in &senders {
        sender.register_peer(receiver.peer_info()).unwrap();
        receiver.register_peer(sender.peer_info()).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    (receiver, senders)
}

/// A `u32` sequence number as a payload.
fn seq_payload(seq: u32) -> Bytes {
    Bytes::from(seq.to_le_bytes().to_vec())
}

fn read_seq(payload: &Bytes) -> u32 {
    u32::from_le_bytes(
        payload
            .as_ref()
            .try_into()
            .expect("4-byte sequence payload"),
    )
}

/// Polls `predicate` until it holds, failing the test rather than hanging.
async fn wait_for(label: &str, mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {label}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_preserves_per_sender_order() {
    const MESSAGES: u32 = 500;

    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);

    let handler = Handler::am_handler_async("ordered_seq", move |ctx: Context| {
        let sink = Arc::clone(&sink);
        async move {
            // Yield so the runtime gets every opportunity to reorder us. Under
            // `.spawn()` this reliably scrambles the sequence.
            tokio::task::yield_now().await;
            sink.lock().push(read_seq(&ctx.payload));
            Ok(())
        }
    })
    .ordered()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for seq in 0..MESSAGES {
        sender
            .am_send("ordered_seq")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }

    wait_for("all messages handled", || {
        observed.lock().len() == MESSAGES as usize
    })
    .await;

    assert_eq!(
        *observed.lock(),
        (0..MESSAGES).collect::<Vec<_>>(),
        "ordered dispatch must hand messages to the handler in send order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_runs_senders_in_parallel() {
    const SENDERS: usize = 3;

    let (receiver, senders) = cluster(SENDERS).await;

    // Every lane's first message parks on the same barrier. If the lanes were
    // serialised this deadlocks, so completing at all is the assertion — no
    // timing heuristics, hence no flake.
    let barrier = Arc::new(tokio::sync::Barrier::new(SENDERS));
    let cleared = Arc::new(AtomicUsize::new(0));
    let per_sender = Arc::new(Mutex::new(HashMap::<u64, Vec<u32>>::new()));

    let handler_barrier = Arc::clone(&barrier);
    let handler_cleared = Arc::clone(&cleared);
    let handler_per_sender = Arc::clone(&per_sender);
    let handler = Handler::am_handler_async("parallel_lanes", move |ctx: Context| {
        let barrier = Arc::clone(&handler_barrier);
        let cleared = Arc::clone(&handler_cleared);
        let per_sender = Arc::clone(&handler_per_sender);
        async move {
            let seq = read_seq(&ctx.payload);
            let worker = ctx.sender_worker_id().as_u64();
            if seq == 0 {
                barrier.wait().await;
                cleared.fetch_add(1, Ordering::AcqRel);
            }
            per_sender.lock().entry(worker).or_default().push(seq);
            Ok(())
        }
    })
    .ordered()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for sender in &senders {
        for seq in 0..5u32 {
            sender
                .am_send("parallel_lanes")
                .unwrap()
                .raw_payload(seq_payload(seq))
                .instance(receiver.instance_id())
                .send()
                .await
                .unwrap();
        }
    }

    wait_for("every lane cleared the barrier", || {
        cleared.load(Ordering::Acquire) == SENDERS
    })
    .await;
    wait_for("all messages handled", || {
        per_sender.lock().values().map(Vec::len).sum::<usize>() == SENDERS * 5
    })
    .await;

    let per_sender = per_sender.lock();
    assert_eq!(per_sender.len(), SENDERS, "one lane per sending instance");
    for (worker, seqs) in per_sender.iter() {
        assert_eq!(
            seqs,
            &(0..5u32).collect::<Vec<_>>(),
            "sender {worker} was reordered"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_global_serializes_all_senders() {
    const SENDERS: usize = 3;
    const PER_SENDER: u32 = 20;

    let (receiver, senders) = cluster(SENDERS).await;

    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(Mutex::new(Vec::<(u64, u32)>::new()));

    let handler_in_flight = Arc::clone(&in_flight);
    let handler_peak = Arc::clone(&peak);
    let handler_observed = Arc::clone(&observed);
    let handler = Handler::am_handler_async("global_lane", move |ctx: Context| {
        let in_flight = Arc::clone(&handler_in_flight);
        let peak = Arc::clone(&handler_peak);
        let observed = Arc::clone(&handler_observed);
        async move {
            let concurrent = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
            peak.fetch_max(concurrent, Ordering::AcqRel);
            tokio::task::yield_now().await;
            observed
                .lock()
                .push((ctx.sender_worker_id().as_u64(), read_seq(&ctx.payload)));
            in_flight.fetch_sub(1, Ordering::AcqRel);
            Ok(())
        }
    })
    .ordered_global()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for sender in &senders {
        for seq in 0..PER_SENDER {
            sender
                .am_send("global_lane")
                .unwrap()
                .raw_payload(seq_payload(seq))
                .instance(receiver.instance_id())
                .send()
                .await
                .unwrap();
        }
    }

    wait_for("all messages handled", || {
        observed.lock().len() == SENDERS * PER_SENDER as usize
    })
    .await;

    assert_eq!(
        peak.load(Ordering::Acquire),
        1,
        "a global lane must never run two handlers at once"
    );

    // The interleaving across senders is arbitrary, but each sender's own
    // subsequence must still be in order.
    let observed = observed.lock();
    let mut grouped: HashMap<u64, Vec<u32>> = HashMap::new();
    for (worker, seq) in observed.iter() {
        grouped.entry(*worker).or_default().push(*seq);
    }
    assert_eq!(grouped.len(), SENDERS);
    for (worker, seqs) in grouped {
        assert_eq!(
            seqs,
            (0..PER_SENDER).collect::<Vec<_>>(),
            "sender {worker} was reordered on the global lane"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrent_caps_cross_lane_parallelism() {
    const SENDERS: usize = 6;
    const LIMIT: usize = 2;
    const PER_SENDER: u32 = 5;

    let (receiver, senders) = cluster(SENDERS).await;

    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let per_sender = Arc::new(Mutex::new(HashMap::<u64, Vec<u32>>::new()));

    let handler_in_flight = Arc::clone(&in_flight);
    let handler_peak = Arc::clone(&peak);
    let handler_per_sender = Arc::clone(&per_sender);
    let handler = Handler::am_handler_async("limited", move |ctx: Context| {
        let in_flight = Arc::clone(&handler_in_flight);
        let peak = Arc::clone(&handler_peak);
        let per_sender = Arc::clone(&handler_per_sender);
        async move {
            let concurrent = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
            peak.fetch_max(concurrent, Ordering::AcqRel);
            // Hold the permit long enough that, without the semaphore, all six
            // lanes would overlap.
            tokio::time::sleep(Duration::from_millis(10)).await;
            per_sender
                .lock()
                .entry(ctx.sender_worker_id().as_u64())
                .or_default()
                .push(read_seq(&ctx.payload));
            in_flight.fetch_sub(1, Ordering::AcqRel);
            Ok(())
        }
    })
    .ordered()
    .max_concurrent(LIMIT)
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for sender in &senders {
        for seq in 0..PER_SENDER {
            sender
                .am_send("limited")
                .unwrap()
                .raw_payload(seq_payload(seq))
                .instance(receiver.instance_id())
                .send()
                .await
                .unwrap();
        }
    }

    // Everything completes: the limiter parks lanes, it does not drop messages.
    wait_for("all messages handled", || {
        per_sender.lock().values().map(Vec::len).sum::<usize>() == SENDERS * PER_SENDER as usize
    })
    .await;

    assert!(
        peak.load(Ordering::Acquire) <= LIMIT,
        "peak concurrency {} exceeded max_concurrent({LIMIT})",
        peak.load(Ordering::Acquire)
    );

    for (worker, seqs) in per_sender.lock().iter() {
        assert_eq!(
            seqs,
            &(0..PER_SENDER).collect::<Vec<_>>(),
            "sender {worker} was reordered under a concurrency limit"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_queue_depth_rejects_without_metrics() {
    // `new_node` deliberately does not install Prometheus metrics. Queue
    // admission is a dispatcher concern, so Reject must still protect this
    // default configuration.
    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let handler_started = Arc::clone(&started);
    let handler_release = Arc::clone(&release);
    let handler = Handler::unary_handler_async("queue_cap", move |_ctx: Context| {
        let started = Arc::clone(&handler_started);
        let release = Arc::clone(&handler_release);
        async move {
            started.store(true, Ordering::Release);
            release.notified().await;
            Ok(Some(Bytes::from_static(b"first response")))
        }
    })
    .ordered_with(
        OrderedConfig::by_sender()
            .with_max_queue_depth(Some(1))
            .with_overflow(OverflowPolicy::Reject),
    )
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let first_sender = Arc::clone(sender);
    let first_target = receiver.instance_id();
    let first = tokio::spawn(async move {
        first_sender
            .unary("queue_cap")
            .unwrap()
            .raw_payload(seq_payload(0))
            .instance(first_target)
            .send()
            .await
    });

    wait_for("first handler started", || started.load(Ordering::Acquire)).await;

    let second = tokio::time::timeout(
        Duration::from_secs(1),
        sender
            .unary("queue_cap")
            .unwrap()
            .raw_payload(seq_payload(1))
            .instance(receiver.instance_id())
            .send(),
    )
    .await
    .expect("queue rejection must fail the caller promptly")
    .expect_err("second request must be rejected while the first is in flight");
    assert!(
        second.to_string().contains("ordered lane queue full"),
        "unexpected queue rejection: {second}"
    );

    release.notify_one();
    assert_eq!(
        first.await.unwrap().unwrap(),
        Bytes::from_static(b"first response")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_queue_depth_is_per_lane_not_per_handler() {
    // The point of per-sender lanes is isolation. A handler-wide depth cap
    // would let one backed-up peer shed traffic from peers whose lanes are
    // empty, which is exactly the coupling `OrderingKey::Sender` exists to
    // remove. Sender A wedges its lane far past the cap; sender B must still
    // get every message through.
    const CAP: usize = 2;
    const B_MESSAGES: u32 = 5;

    let (receiver, senders) = cluster(2).await;
    let (blocked_sender, free_sender) = (&senders[0], &senders[1]);
    let blocked_worker = blocked_sender.instance_id().worker_id().as_u64();

    let release = Arc::new(AtomicBool::new(false));
    let free_handled = Arc::new(Mutex::new(Vec::new()));

    let handler_release = Arc::clone(&release);
    let handler_free = Arc::clone(&free_handled);
    let handler = Handler::am_handler_async("per_lane_cap", move |ctx: Context| {
        let release = Arc::clone(&handler_release);
        let free = Arc::clone(&handler_free);
        async move {
            if ctx.sender_worker_id().as_u64() == blocked_worker {
                // Wedge sender A's lane so its queue builds past the cap.
                while !release.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } else {
                free.lock().push(read_seq(&ctx.payload));
            }
            Ok(())
        }
    })
    .ordered_with(
        OrderedConfig::by_sender()
            .with_max_queue_depth(Some(CAP))
            .with_overflow(OverflowPolicy::Reject),
    )
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire-and-forget, so shed messages do not surface as client errors —
    // sender A is meant to overflow here.
    for seq in 0..20u32 {
        blocked_sender
            .am_send("per_lane_cap")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }

    // One at a time, waiting for each to land: the cap applies to B's lane too,
    // so a burst could legitimately overflow B's *own* queue and prove nothing
    // about isolation. Send-and-wait keeps B's depth at 1 throughout, leaving
    // A's saturated lane as the only thing that could shed it.
    for seq in 0..B_MESSAGES {
        free_sender
            .am_send("per_lane_cap")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
        wait_for("free sender's message handled", || {
            free_handled.lock().len() == (seq + 1) as usize
        })
        .await;
    }

    assert_eq!(
        *free_handled.lock(),
        (0..B_MESSAGES).collect::<Vec<_>>(),
        "an unrelated sender must be neither shed nor reordered by another lane's backlog"
    );

    release.store(true, Ordering::Release);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_unary_responses_are_correct() {
    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let handler = Handler::unary_handler("ordered_echo", |ctx: Context| Ok(Some(ctx.payload)))
        .ordered()
        .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sequential calls.
    for seq in 0..100u32 {
        let response: Bytes = sender
            .unary("ordered_echo")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
        assert_eq!(
            read_seq(&response),
            seq,
            "sequential unary response mismatch"
        );
    }

    // Concurrent calls. Ordered mode serialises their *handling*, but each
    // caller must still get its own response back.
    let mut tasks = Vec::new();
    for seq in 0..100u32 {
        let sender = Arc::clone(sender);
        let target = receiver.instance_id();
        tasks.push(tokio::spawn(async move {
            let response: Bytes = sender
                .unary("ordered_echo")
                .unwrap()
                .raw_payload(seq_payload(seq))
                .instance(target)
                .send()
                .await
                .unwrap();
            (seq, read_seq(&response))
        }));
    }
    for task in tasks {
        let (sent, received) = task.await.unwrap();
        assert_eq!(sent, received, "concurrent unary response mismatch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_handler_error_does_not_stall_lane() {
    const MESSAGES: u32 = 10;
    const FAILING: u32 = 3;

    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let handler = Handler::am_handler_async("erroring", move |ctx: Context| {
        let sink = Arc::clone(&sink);
        async move {
            let seq = read_seq(&ctx.payload);
            sink.lock().push(seq);
            if seq == FAILING {
                anyhow::bail!("deliberate handler error");
            }
            Ok(())
        }
    })
    .ordered()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for seq in 0..MESSAGES {
        sender
            .am_send("erroring")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }

    wait_for("all messages reached the handler", || {
        observed.lock().len() == MESSAGES as usize
    })
    .await;
    assert_eq!(
        *observed.lock(),
        (0..MESSAGES).collect::<Vec<_>>(),
        "a handler error must not stall or reorder the lane"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_handler_panic_does_not_stall_lane() {
    // This is the failure mode ordered dispatch introduces and `.spawn()` does
    // not have: a panic kills the lane task, and without `catch_unwind` every
    // later message from that sender is enqueued onto a channel nobody reads.
    // Without the catch, this test hangs rather than failing.
    const MESSAGES: u32 = 10;
    const PANICKING: u32 = 3;

    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let handler = Handler::am_handler_async("panicking", move |ctx: Context| {
        let sink = Arc::clone(&sink);
        async move {
            let seq = read_seq(&ctx.payload);
            assert_ne!(seq, PANICKING, "deliberate handler panic");
            sink.lock().push(seq);
            Ok(())
        }
    })
    .ordered()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for seq in 0..MESSAGES {
        sender
            .am_send("panicking")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }

    wait_for("surviving messages handled", || {
        observed.lock().len() == (MESSAGES - 1) as usize
    })
    .await;

    let expected: Vec<u32> = (0..MESSAGES).filter(|s| *s != PANICKING).collect();
    assert_eq!(
        *observed.lock(),
        expected,
        "the lane must survive a panicking message and stay ordered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_lane_reused_after_idle_reap() {
    const BURST: u32 = 10;

    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let observed = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let handler = Handler::am_handler_async("reaped", move |ctx: Context| {
        let sink = Arc::clone(&sink);
        async move {
            sink.lock().push(read_seq(&ctx.payload));
            Ok(())
        }
    })
    .ordered_with(OrderedConfig::by_sender().with_idle_lane_ttl(Some(Duration::from_millis(100))))
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    for seq in 0..BURST {
        sender
            .am_send("reaped")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }
    wait_for("first burst handled", || {
        observed.lock().len() == BURST as usize
    })
    .await;

    // Idle well past the TTL so the lane reaps itself, then send again.
    tokio::time::sleep(Duration::from_millis(400)).await;

    for seq in BURST..BURST * 2 {
        sender
            .am_send("reaped")
            .unwrap()
            .raw_payload(seq_payload(seq))
            .instance(receiver.instance_id())
            .send()
            .await
            .unwrap();
    }
    wait_for("second burst handled", || {
        observed.lock().len() == (BURST * 2) as usize
    })
    .await;

    assert_eq!(
        *observed.lock(),
        (0..BURST * 2).collect::<Vec<_>>(),
        "a lane rebuilt after an idle reap must still be ordered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_context_exposes_sender_identity() {
    let (receiver, senders) = cluster(1).await;
    let sender = &senders[0];

    let seen = Arc::new(Mutex::new(Vec::<(u64, Option<String>)>::new()));
    let sink = Arc::clone(&seen);
    let handler = Handler::am_handler_async("identity", move |ctx: Context| {
        let sink = Arc::clone(&sink);
        async move {
            sink.lock().push((
                ctx.sender_worker_id().as_u64(),
                ctx.sender_instance_id().map(|id| id.to_string()),
            ));
            Ok(())
        }
    })
    .ordered()
    .build();
    receiver.register_handler(handler).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    sender
        .am_send("identity")
        .unwrap()
        .raw_payload(seq_payload(0))
        .instance(receiver.instance_id())
        .send()
        .await
        .unwrap();

    wait_for("message handled", || seen.lock().len() == 1).await;

    let (worker, instance) = seen.lock()[0].clone();
    assert_eq!(
        worker,
        sender.instance_id().worker_id().as_u64(),
        "sender_worker_id must identify the sending instance"
    );
    assert_eq!(
        instance,
        Some(sender.instance_id().to_string()),
        "sender_instance_id resolves once the peer is registered"
    );
}
