// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Where mux credit comes back from — the consumer draining, or the sweep.
//!
//! `BATCHING.md` § P8 specifies that credit is returned by `reader_pump`,
//! which "gains an `Option<CreditReturn>` and calls `credit.release(1)` after
//! each successful handoff to `frame_tx` — exact, O(1), and immediate", with a
//! background sweep only reclaiming credit for slots whose pump died.
//!
//! What shipped instead reconciles buffer occupancy on every inbound batch and
//! on a periodic sweep. The same document records that deviation and argues
//! "the effect is the same and the sweep bounds the latency".
//!
//! The effect is not the same, and this file is what shows it. The sweep runs
//! at `credit_sweep_interval` and per tick walks every slot of every ingress
//! peer, taking the same per-peer mutex the inbound batch path takes — work
//! that grows as `O(peers x slots)` while the credit it finds does not. What
//! that costs in CPU is not currently a measured number; the figures first
//! quoted here were taken on a shared machine and are retracted
//! (`examples/examples/response_plane_bench.evidence.md`).
//!
//! The correctness claim below does not depend on that measurement at all.
//!
//! So the sweep interval is not free to relax while it is the only thing
//! returning credit for a stream nobody is sending further batches to. These
//! tests pin the property that would make it free: **a consumer that drains
//! returns credit by draining**, not by waiting for a timer.
//!
//! Every test here goes through the real `Velo` attach path rather than binding
//! the transport directly, because that is the only path with a `reader_pump`
//! in it — and `reader_pump` is where the drain is observable. A test that
//! polls the receiver `bind` returns would never exercise the hook at all.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::observability::test_helpers::MetricSnapshot;
use velo::streaming::{MpscFrame, MuxConfig, StreamAnchor, StreamAnchorHandle, StreamFrame};
use velo::transports::tcp::TcpTransportBuilder;
use velo::{Velo, VeloBuilder};

const PATIENCE: Duration = Duration::from_secs(30);

/// Long enough that no test here can reach it. A sweep on this interval will
/// not tick even once inside a test's patience, so anything that completes has
/// completed without the sweep's help — which is the whole point.
const UNREACHABLE_SWEEP: Duration = Duration::from_secs(600);

/// A window far smaller than the traffic, so a producer that gets no credit
/// back parks and stays parked.
const SMALL_CREDIT: u32 = 4;

fn tcp_transport() -> Arc<velo::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    Arc::new(
        TcpTransportBuilder::new()
            .from_listener(listener)
            .expect("from_listener")
            .build()
            .expect("build transport"),
    )
}

struct Node {
    velo: Arc<Velo>,
    registry: Registry,
}

impl Node {
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot::from_registry(&self.registry)
    }

    /// The applier never met a full slot buffer. Non-zero is a broken credit
    /// invariant, and nothing else in a test would show it.
    ///
    /// Assert it on the **receiving** node: `reader_stall` is raised where a
    /// record is applied to a slot buffer, so on a producer it is vacuously
    /// zero.
    fn assert_no_reader_stall(&self) {
        assert_eq!(
            self.snapshot()
                .counter("velo_streaming_mux_reader_stall_total", &[]),
            0.0,
            "the applier hit a full slot buffer: credit and buffer depth disagree"
        );
    }

    /// Per-peer credit reconciles the sweep task ran because a consumer
    /// drained. Counts walks, not wakes.
    fn mux_drain_visits(&self) -> f64 {
        self.snapshot()
            .counter("velo_streaming_mux_drain_visits_total", &[])
    }
}

async fn node(config: MuxConfig) -> Node {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
    let velo: VeloBuilder = Velo::builder()
        .add_transport(tcp_transport())
        .stream_bind_addr(std::net::Ipv4Addr::LOCALHOST.into())
        .metrics(metrics);
    Node {
        velo: velo
            .messenger_mux(config)
            .expect("install mux")
            .build()
            .await
            .expect("build velo"),
        registry,
    }
}

/// A mux whose sweep will not tick inside this test's lifetime.
fn sweepless_mux(initial_credit: u32) -> MuxConfig {
    MuxConfig {
        enabled: true,
        initial_credit,
        credit_sweep_interval: UNREACHABLE_SWEEP,
        ..MuxConfig::default()
    }
}

/// Cross-register and wait until each side can see the other's attach handler.
async fn introduce(a: &Node, b: &Node) {
    a.velo
        .register_peer(b.velo.peer_info())
        .expect("register b on a");
    b.velo
        .register_peer(a.velo.peer_info())
        .expect("register a on b");
    for (from, to) in [(a, b), (b, a)] {
        tokio::time::timeout(
            PATIENCE,
            from.velo
                .wait_for_handler(to.velo.instance_id(), "_anchor_attach"),
        )
        .await
        .expect("timed out waiting for the peer's control plane")
        .expect("peer never advertised the handler");
    }
}

/// The handle as it would cross an RPC to the producer.
fn transfer(handle: StreamAnchorHandle) -> StreamAnchorHandle {
    StreamAnchorHandle::from_u128(handle.as_u128())
}

/// What the consumer saw when it asked for the next item.
///
/// Credit starvation has two symptoms, not one, and a test that only knows
/// about the timeout reports the other as a mystery. Records the sender cannot
/// transmit are pulled into its withheld queue anyway
/// (`peer_batcher/slot_stream.rs` module docs), so a producer that finishes its
/// run drops its sender with those records still stranded — and the consumer is
/// told `SenderDropped` rather than being left to wait.
#[derive(Debug)]
enum Next {
    Item(u32),
    /// Nothing arrived inside the window: the producer is parked.
    Stalled,
    /// The producer went away with records still owed to us.
    Stranded(String),
}

async fn next_item(anchor: &mut StreamAnchor<u32>, window: Duration) -> Next {
    match tokio::time::timeout(window, anchor.next()).await {
        Ok(Some(Ok(StreamFrame::Item(value)))) => Next::Item(value),
        Ok(Some(Ok(other))) => panic!("unexpected frame: {other:?}"),
        Ok(Some(Err(error))) => Next::Stranded(format!("{error:?}")),
        Ok(None) => Next::Stranded("the anchor closed".to_string()),
        Err(_) => Next::Stalled,
    }
}

/// Assert the next item is `expected`, naming credit starvation when it is not.
fn expect_item(got: Next, expected: u32, context: &str) {
    match got {
        Next::Item(value) => assert_eq!(value, expected, "frame out of order"),
        Next::Stalled => panic!(
            "frame {expected} never arrived: the producer is parked with no credit, and {context}"
        ),
        Next::Stranded(why) => panic!(
            "frame {expected} never arrived ({why}): the producer reached the end of its run and \
             dropped its sender while records it could not transmit were still withheld for want \
             of credit, and {context}"
        ),
    }
}

// ---------------------------------------------------------------------------

/// A draining consumer returns credit without the sweep.
///
/// The producer's window is `SMALL_CREDIT`; it sends many times that. The
/// consumer drains every frame as it arrives, so every credit the producer
/// spent has been freed at the consumer long before the producer needs it
/// back. The only question is whether anything tells the producer.
///
/// Today nothing does, until the sweep ticks — and here it never will, so this
/// fails. Which symptom it fails with depends on payload size: records the
/// sender may not transmit are still pulled into its withheld queue, so with
/// small records the producer runs to the end of its loop and drops its sender
/// with most of them stranded (`SenderDropped`), while with records big enough
/// to fill the slot's byte budget it parks instead. Both are the same defect
/// and `expect_item` names either. With credit returned at the drain point the
/// stream completes with the sweep never having run.
///
/// It fails for the right reason rather than by accident: `sweep_returns_credit_when_nobody_drains`
/// below is the control, and it shows the same configuration completing as soon
/// as the sweep is allowed to tick. If this test failed because the mux were
/// simply broken at `SMALL_CREDIT`, that control would fail too.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_consumer_returns_credit_without_the_sweep() {
    const FRAMES: u32 = 200;

    let consumer = node(sweepless_mux(SMALL_CREDIT)).await;
    let producer = node(sweepless_mux(SMALL_CREDIT)).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    let writer = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    // Drain as fast as frames arrive. Any stall here is the producer parked on
    // a credit return that never came.
    for n in 0..FRAMES {
        expect_item(
            next_item(&mut anchor, PATIENCE).await,
            n,
            "this consumer had already drained every earlier frame, so the credit was free — \
             nothing but the sweep returns it, and the sweep cannot tick inside this test",
        );
    }

    tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("producer did not finish")
        .expect("producer task panicked");

    // The drain path returned enough credit, and not more than enough.
    consumer.assert_no_reader_stall();
}

/// Control: the same configuration, with the sweep allowed to tick.
///
/// This is what makes the test above meaningful. It pins that
/// `SMALL_CREDIT` traffic over the mux does complete when *something* returns
/// credit, so a failure above is about the return path and not about the
/// window being unworkable.
#[tokio::test(flavor = "multi_thread")]
async fn sweep_returns_credit_when_nobody_drains() {
    const FRAMES: u32 = 200;

    let fast_sweep = |credit: u32| MuxConfig {
        enabled: true,
        initial_credit: credit,
        credit_sweep_interval: Duration::from_millis(1),
        ..MuxConfig::default()
    };

    let consumer = node(fast_sweep(SMALL_CREDIT)).await;
    let producer = node(fast_sweep(SMALL_CREDIT)).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    let writer = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    for n in 0..FRAMES {
        expect_item(
            next_item(&mut anchor, PATIENCE).await,
            n,
            "the sweep is running here, so this configuration is workable and the failure above \
             is about the return path rather than the window",
        );
    }

    tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("producer did not finish")
        .expect("producer task panicked");

    consumer.assert_no_reader_stall();
}

/// Credit returned by draining must not be returned twice.
///
/// A double release would let the producer hold more than `initial_credit`
/// records in flight against a `C + 1` buffer, which is the overspend the
/// credit ledger exists to prevent. The sweep still runs here, on its default
/// interval, so both return paths are live at once and race each other on
/// purpose — that race is the thing most likely to double-count.
///
/// The assertion is indirect but exact: `assert_no_reader_stall` fails if any
/// record ever found the slot buffer full, and a buffer sized `C + 1` can only
/// fill if the sender was granted more than `C`.
#[tokio::test(flavor = "multi_thread")]
async fn draining_and_sweeping_together_never_overspend_the_window() {
    const FRAMES: u32 = 500;

    let both = |credit: u32| MuxConfig {
        enabled: true,
        initial_credit: credit,
        // The default. Both paths return credit concurrently.
        ..MuxConfig::default()
    };

    let consumer = node(both(SMALL_CREDIT)).await;
    let producer = node(both(SMALL_CREDIT)).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    let writer = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    for n in 0..FRAMES {
        expect_item(
            next_item(&mut anchor, PATIENCE).await,
            n,
            "both credit-return paths are live here and must not disagree",
        );
    }

    tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("producer did not finish")
        .expect("producer task panicked");

    // The terminal must still arrive: the reserved terminal credit is a
    // separate ledger entry, and a drain-driven release that spent it would
    // strand the stream here rather than anywhere above.
    match tokio::time::timeout(PATIENCE, anchor.next()).await {
        Ok(Some(frame)) => match frame.expect("no stream error") {
            StreamFrame::Finalized => {}
            other => panic!("expected the terminal, got {other:?}"),
        },
        Ok(None) => panic!("the anchor closed before its terminal"),
        Err(_) => panic!("the terminal never arrived — reserved terminal credit was consumed"),
    }

    // The direct form of the claim in this test's name. Everything above shows
    // the stream *completed*; this shows it never overspent on the way, which
    // no ordering or arrival assertion can: a double-released credit lets the
    // sender put a `C + 1`-th record into a `C + 1`-deep buffer, and the only
    // trace of it is here.
    consumer.assert_no_reader_stall();
}

/// The doorbell's per-peer visit rate is floored, not just coalesced.
///
/// `sweep_peer` takes the peer's wake down *before* it walks, which is what
/// stops a drain landing mid-walk from being swallowed. What that ordering does
/// not do is bound how often the walk happens: the first record drained after a
/// visit clears the flag arms it again, so the sweep task runs
/// wake -> clear -> walk every slot -> re-armed, back to back. Coalescing bounds
/// visits by the *number of drain bursts*, which is a property of the traffic
/// and of nothing else.
///
/// The walk is not free. It iterates every slot of the peer, asks flume for each
/// slot buffer's `len()` — a channel-lock acquisition apiece — and holds the peer
/// mutex `handle_batch` needs on the ingress hot path. The workload this mux
/// exists for is one peer carrying hundreds to thousands of slots, so a visit
/// rate set by the traffic rather than by need is hot-path contention. Hence a
/// floor: at most one doorbell visit per peer per `drain_visit_floor`.
///
/// The shape here makes the unfloored rate a structural number rather than a
/// race between two tasks. With a window of `WINDOW` records the producer sends
/// a window, parks, and waits for the credit its consumer's drain returns — so
/// the peer is drained in exactly `FRAMES / WINDOW` bursts and, unfloored, is
/// visited exactly that many times. That count does not move with machine speed;
/// only the wall clock it happens in does. Measured before the floor: 1000
/// visits in ~270 ms, some seven times what a 2 ms floor allows.
///
/// Both halves of the assertion matter. `> 0` says the doorbell demonstrably
/// rang — a change that simply stopped ringing it would satisfy the ceiling and
/// break the credit path the rest of this file pins. The ceiling says it rang no
/// faster than the floor allows.
#[tokio::test(flavor = "multi_thread")]
async fn the_drain_doorbell_visits_a_peer_no_faster_than_the_floor() {
    const FRAMES: u32 = 8000;
    /// Small enough that the producer parks once per window and the run is a
    /// long series of drain-and-refill rounds, which is what puts a doorbell
    /// ring on each of them. `FRAMES / WINDOW` is the unfloored visit count.
    const WINDOW: u32 = 8;
    /// Headroom over the computed ceiling for the run's ragged edges — a visit
    /// in flight as the clock starts, one falling due as it stops. Deliberately
    /// small: slack large enough to swallow an unfloored rate would make the
    /// test decorative.
    const SLACK: f64 = 10.0;

    let quiet_ticker = MuxConfig {
        enabled: true,
        initial_credit: WINDOW,
        // Far longer than the run, so every visit counted below came from the
        // doorbell rather than from the periodic backstop.
        credit_sweep_interval: Duration::from_secs(5),
        // The default floor is what is under test.
        ..MuxConfig::default()
    };

    let consumer = node(quiet_ticker.clone()).await;
    let producer = node(quiet_ticker).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_anchor::<u32>(handle)
        .await
        .expect("remote attach");

    // The clock starts after the attach: TCP connect and handler discovery are
    // wall time no drain happens in, and counting them would inflate the ceiling
    // until an unfloored rate fit under it.
    let before = consumer.mux_drain_visits();
    let started = Instant::now();

    let writer = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        sender.finalize().expect("finalize");
    });

    for n in 0..FRAMES {
        expect_item(
            next_item(&mut anchor, PATIENCE).await,
            n,
            "the doorbell is the only thing returning credit here — the sweep cannot tick inside \
             this run",
        );
    }

    tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("producer did not finish")
        .expect("producer task panicked");

    // Visits first, elapsed second: the reverse order could time a window
    // shorter than the one the visits were counted over, and the ceiling is a
    // rate.
    let visits = consumer.mux_drain_visits() - before;
    let elapsed = started.elapsed();

    let floor = MuxConfig::default().drain_visit_floor;
    let ceiling = elapsed.as_secs_f64() / floor.as_secs_f64() + SLACK;
    assert!(
        visits > 0.0,
        "the doorbell never rang: {FRAMES} frames were drained through a {WINDOW}-record window \
         and the sweep task visited the peer {visits} times, so this run proves nothing about \
         the rate"
    );
    assert!(
        visits <= ceiling,
        "the doorbell visited one peer {visits} times in {elapsed:?}, past the {ceiling:.0} a \
         {floor:?} floor allows: every visit walks the peer's whole slot table under the mutex \
         the ingress hot path needs, so the rate is set by how often the consumer drains rather \
         than by anything the mux decided"
    );

    consumer.assert_no_reader_stall();
}

// ---------------------------------------------------------------------------
// MPSC
// ---------------------------------------------------------------------------

/// The same property, for MPSC anchors.
///
/// MPSC is not a side case here. `MpscAnchorAttachRequest::supported_transport_keys`
/// says MPSC "negotiates in the same version as SPSC so there is no
/// half-migrated state where one anchor kind rides the mux and the other does
/// not" (`streaming/mpsc/control.rs`), so an MPSC stream over the mux needs its
/// credit returned by draining for exactly the same reason — and it travels
/// through `mpsc_reader_pump`, a different function from the one the SPSC test
/// above exercises.
///
/// Without this test the SPSC test can go green over an MPSC path that still
/// depends entirely on the sweep, and relaxing the sweep interval would then be
/// a silent hundred-fold credit-latency regression for every MPSC stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_mpsc_consumer_returns_credit_without_the_sweep() {
    const FRAMES: u32 = 200;

    let consumer = node(sweepless_mux(SMALL_CREDIT)).await;
    let producer = node(sweepless_mux(SMALL_CREDIT)).await;
    introduce(&producer, &consumer).await;

    let mut anchor = consumer.velo.create_mpsc_anchor::<u32>();
    let handle = transfer(anchor.handle());
    let sender = producer
        .velo
        .attach_mpsc_anchor::<u32>(handle)
        .await
        .expect("remote mpsc attach");

    let writer = tokio::spawn(async move {
        for n in 0..FRAMES {
            sender.send(n).await.expect("send item");
        }
        // MPSC's terminal is `detach`: the sender leaves, the anchor stays open
        // for its other senders.
        sender.detach().await.expect("detach");
    });

    for n in 0..FRAMES {
        let got = match tokio::time::timeout(PATIENCE, anchor.next()).await {
            Ok(Some(Ok((_, MpscFrame::Item(value))))) => Next::Item(value),
            Ok(Some(Ok((_, other)))) => panic!("unexpected mpsc frame: {other:?}"),
            Ok(Some(Err(error))) => Next::Stranded(format!("{error:?}")),
            Ok(None) => Next::Stranded("the anchor closed".to_string()),
            Err(_) => Next::Stalled,
        };
        expect_item(
            got,
            n,
            "this is the MPSC pump, which is a different function from the SPSC one and needs \
             its own drain hook",
        );
    }

    tokio::time::timeout(PATIENCE, writer)
        .await
        .expect("producer did not finish")
        .expect("producer task panicked");

    consumer.assert_no_reader_stall();
}
