// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `.timeout` on sync and unary requests, over a real transport.
//!
//! velo sets no deadline on a request: the caller bounds its own wait. These
//! tests pin what `.timeout` promises when it does: the call fails with
//! [`RequestTimeout`], distinct from every other error; it gives its response
//! slot back at expiry; and a reply that comes later is discarded rather than
//! delivered to another call. The state machine is pinned branch by branch in
//! `messenger/client/builders/tests.rs`.
//!
//! The handlers here hold their reply on a semaphore instead of never
//! answering. A handler that never returns would keep the server's drain
//! waiting at teardown, and the test would hang instead of fail.

#[path = "support/staged_fetch.rs"]
mod staged_fetch;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use staged_fetch::{Stall, released_within, stall_fetches};
use tokio::sync::Semaphore;
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::*;

/// The timeout under test.
const TIMEOUT: Duration = Duration::from_millis(200);
/// How long a timed-out call may take, in all, before the test calls it hung.
/// Generous so a loaded CI runner does not flake; the point is that it ends.
const BOUND: Duration = Duration::from_secs(5);

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

/// A client and a server that know each other and have handshaked. The server
/// has `echo`, which counts its calls in the returned counter, and `held`,
/// `held_sync` and `held_typed`, which answer only once `gate` gives them a
/// permit.
async fn connected_pair(gate: Arc<Semaphore>) -> (Velo, Velo, Arc<AtomicUsize>) {
    connected_pair_with(gate, Velo::builder()).await
}

/// [`connected_pair`], with the server built from `server`.
async fn connected_pair_with(
    gate: Arc<Semaphore>,
    server: VeloBuilder,
) -> (Velo, Velo, Arc<AtomicUsize>) {
    let server = server.add_transport(new_transport()).build().await.unwrap();
    let echoes = Arc::new(AtomicUsize::new(0));
    let counter = echoes.clone();
    server
        .register_handler(
            Handler::unary_handler("echo", move |ctx| {
                counter.fetch_add(1, Ordering::AcqRel);
                Ok(Some(ctx.payload))
            })
            .build(),
        )
        .unwrap();
    let held = gate.clone();
    server
        .register_handler(
            Handler::unary_handler_async("held", move |_ctx| {
                let gate = held.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(Some(Bytes::from_static(b"late")))
                }
            })
            .build(),
        )
        .unwrap();
    let held = gate.clone();
    server
        .register_handler(
            Handler::am_handler_async("held_sync", move |_ctx| {
                let gate = held.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            })
            .build(),
        )
        .unwrap();
    let held = gate;
    server
        .register_handler(
            Handler::typed_unary_async("held_typed", move |_ctx: TypedContext<u32>| {
                let gate = held.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(7u32)
                }
            })
            .build(),
        )
        .unwrap();

    let client = Velo::builder()
        .add_transport(new_transport())
        .build()
        .await
        .unwrap();
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    client
        .messenger()
        .available_handlers(server.instance_id())
        .await
        .unwrap();
    (client, server, echoes)
}

/// Await `call`, sent at `started`, which must fail with [`RequestTimeout`]
/// no sooner than [`TIMEOUT`] after it.
async fn expect_timeout<T: std::fmt::Debug>(
    started: Instant,
    call: impl Future<Output = anyhow::Result<T>>,
) {
    let err = tokio::time::timeout(BOUND, call)
        .await
        .expect("a call with a timeout ends")
        .expect_err("nothing answered in time");
    let elapsed = started.elapsed();
    let timeout = err
        .downcast_ref::<RequestTimeout>()
        .unwrap_or_else(|| panic!("expected RequestTimeout, got: {err:#}"));
    assert_eq!(timeout.timeout(), TIMEOUT);
    assert!(elapsed >= TIMEOUT, "expired early, after {elapsed:?}");
}

/// Wait until the client holds `expected` response slots. A reply on its way
/// back can hold one for a moment after the call resolves on the other side.
async fn wait_for_slots(client: &Velo, expected: usize) {
    let settled = tokio::time::timeout(BOUND, async {
        while client.response_slots_in_use() != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "response slots stuck at {}, expected {expected}",
        client.response_slots_in_use()
    );
}

/// Whether `velo` gets back to `expected` response slots within [`BOUND`].
/// Unlike [`wait_for_slots`] it does not panic, so a test can open a gate
/// before it asserts, and a failure does not leave a handler waiting.
async fn settles_to(velo: &Velo, expected: usize) -> bool {
    tokio::time::timeout(BOUND, async {
        while velo.response_slots_in_use() != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// Over velo's staging threshold (256 KiB), so the sender stages the payload
/// and the receiver fetches it before the handler runs.
const STAGED: usize = 512 * 1024;

/// A timed send created outside Tokio must still free its staged payload at expiry.
#[test]
fn a_staged_send_created_outside_tokio_still_expires_on_its_owner() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let (client, server, _) = runtime.block_on(connected_pair(Arc::new(Semaphore::new(0))));
    let stall = Stall::new();
    stall_fetches(&client, &stall);
    assert!(tokio::runtime::Handle::try_current().is_err());

    let started = Instant::now();
    let call = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from(vec![0x5a; STAGED]))
        .timeout(TIMEOUT)
        .instance(server.instance_id())
        .send();

    runtime.block_on(async {
        tokio::time::timeout(BOUND, stall.entered.notified())
            .await
            .expect("the receiver began fetching");
        expect_timeout(started, call).await;
        let staged = stall.asked()[0];
        let released = released_within(&client, staged, Duration::from_secs(1)).await;
        stall.gate.add_permits(1);
        assert!(
            released,
            "a timed staged send created outside Tokio left its payload on the owner"
        );
    });
}

/// A `RendezvousManager` built by hand over a `Velo`'s messenger. Its handlers
/// replace the ones the `Velo` registered.
fn ttl_data_expires_after_handler_registration(stage_before_handlers: bool) -> bool {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let velo = runtime
        .block_on(
            Velo::builder()
                .add_transport(new_transport())
                .mux_only()
                .build(),
        )
        .unwrap();
    let messenger = Arc::clone(velo.messenger());
    let manager = Arc::new(RendezvousManager::new(messenger.instance_id().worker_id()));
    assert!(tokio::runtime::Handle::try_current().is_err());
    let stage = || {
        manager.register_data_with(
            Bytes::from_static(b"expires"),
            velo::rendezvous::RegisterOptions::new().ttl(Duration::from_millis(20)),
        )
    };
    let handle = if stage_before_handlers {
        let handle = stage();
        manager.register_handlers(Arc::clone(&messenger)).unwrap();
        handle
    } else {
        manager.register_handlers(Arc::clone(&messenger)).unwrap();
        stage()
    };
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.data_store().metadata(handle.unpack().1).is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    })
}

/// Binding the owner's runtime must sweep TTL data staged before its handlers.
#[test]
#[ignore = "F4: binding a runtime leaves pre-handler TTL data without a reaper"]
fn binding_the_owner_runtime_sweeps_data_staged_before_its_handlers() {
    assert!(
        ttl_data_expires_after_handler_registration(true),
        "TTL data stayed staged after the owner's runtime was bound"
    );
}

#[test]
fn a_standalone_owner_expires_data_staged_after_its_handlers() {
    assert!(ttl_data_expires_after_handler_registration(false));
}

/// A staged request's fetch gives the receiver's response slot back when the
/// caller's timeout runs out.
///
/// A payload over the staging threshold does not travel in the request. The
/// sender stages it, and the receiver fetches it with unary calls of its own
/// to the sender (`_rv_acquire`, then `_rv_pull`) before the handler runs.
/// Each of those calls holds one of the receiver's response slots until the
/// sender answers. When the sender stalls, the caller's `.timeout` ends the
/// call on the caller's side, but nothing ended the fetch: the receiver kept
/// the slot, and the request's drain guard, until the sender answered or the
/// receiver tore down. The slot arena is fixed size, so a sender that never
/// answers takes one slot from the receiver for each request it sends.
///
/// The sender now sends what is left of the timeout with the staged handle,
/// and the receiver drops the fetch when it runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_fetch_releases_the_receivers_slot_at_the_callers_timeout() {
    let (client, server, echoes) = connected_pair(Arc::new(Semaphore::new(0))).await;
    let target = server.instance_id();
    // A small call first, so the connection is up both ways and the fetch
    // below is the only thing that can hold a slot on the server.
    client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warm"))
        .timeout(BOUND)
        .instance(target)
        .send()
        .await
        .expect("echo answers");
    wait_for_slots(&server, 0).await;

    let stall = Stall::new();
    stall_fetches(&client, &stall);

    let started = Instant::now();
    let call = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from(vec![0x5a; STAGED]))
        .timeout(TIMEOUT)
        .instance(target)
        .send();
    tokio::time::timeout(BOUND, stall.entered.notified())
        .await
        .expect("the server began to fetch the staged payload");
    expect_timeout(started, call).await;

    let released = settles_to(&server, 0).await;
    let held = server.response_slots_in_use();
    stall.gate.add_permits(1);
    assert!(
        released,
        "the server still holds {held} response slot(s) for a fetch its caller gave up on"
    );
    assert_eq!(
        echoes.load(Ordering::Acquire),
        1,
        "the handler ran on a request whose payload never arrived"
    );
}

/// The receiver's own bound ends a stalled fetch when the caller set no
/// `.timeout`, and the caller hears why instead of waiting for ever. This is
/// the bound for sends that carry none: fire-and-forget sends, calls without a
/// timeout, and senders that predate the budget header.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_receivers_bound_ends_a_stalled_fetch_and_the_caller_hears_why() {
    let (client, server, echoes) = connected_pair_with(
        Arc::new(Semaphore::new(0)),
        Velo::builder().staged_fetch_timeout(TIMEOUT),
    )
    .await;
    let target = server.instance_id();
    client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from_static(b"warm"))
        .timeout(BOUND)
        .instance(target)
        .send()
        .await
        .expect("echo answers");
    wait_for_slots(&server, 0).await;

    let stall = Stall::new();
    stall_fetches(&client, &stall);

    let started = Instant::now();
    let call = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from(vec![0x5a; STAGED]))
        .instance(target)
        .send();
    tokio::time::timeout(BOUND, stall.entered.notified())
        .await
        .expect("the server began to fetch the staged payload");
    let outcome = tokio::time::timeout(BOUND, call).await;
    let elapsed = started.elapsed();
    let released = settles_to(&server, 0).await;
    stall.gate.add_permits(1);

    let err = outcome
        .expect("the server's bound ended a call that had none")
        .expect_err("the payload never arrived");
    assert!(err.downcast_ref::<RequestTimeout>().is_none(), "{err:#}");
    assert!(
        format!("{err:#}").contains("did not finish within 200 ms"),
        "{err:#}"
    );
    assert!(
        elapsed >= TIMEOUT,
        "the fetch ended early, after {elapsed:?}"
    );
    assert!(
        released,
        "the server kept the stalled fetch's response slot"
    );
    assert_eq!(echoes.load(Ordering::Acquire), 1);
}

/// A staged call that timed out leaves nothing staged on its sender, even
/// while the sender has not answered the fetch at all.
///
/// When the fetch had no bound, the receiver kept waiting on a slow sender,
/// then pulled and released, and the payload was freed. With the bound, the
/// receiver drops the fetch at the caller's deadline and tells the sender
/// nothing, so a payload whose fetch never got a lease stayed staged until
/// the sender shut down: one payload for each call that timed out. Its
/// time-to-live, the caller's own deadline, now frees it, and the late
/// acquire finds nothing to lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_staged_call_leaves_nothing_staged_on_a_slow_sender() {
    let (client, server, _echoes) = connected_pair(Arc::new(Semaphore::new(0))).await;
    let stall = Stall::new();
    stall_fetches(&client, &stall);

    let started = Instant::now();
    let call = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from(vec![0x5a; STAGED]))
        .timeout(TIMEOUT)
        .instance(server.instance_id())
        .send();
    tokio::time::timeout(BOUND, stall.entered.notified())
        .await
        .expect("the server began to fetch the staged payload");
    expect_timeout(started, call).await;

    let staged = stall.asked()[0];
    let released = released_within(&client, staged, BOUND).await;
    // The sender catches up, after the assertion's wait.
    stall.gate.add_permits(1);
    assert!(
        released,
        "the sender still holds the payload of a call that timed out"
    );
}

/// A fetch that the receiver's own bound ended leaves nothing staged on a
/// slow sender.
///
/// The call has no timeout, so its payload has no time-to-live. The fetch
/// asks the sender for a lease deadline of its bound, so the lease the slow
/// sender grants after the receiver gave up ends once no pull renews it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetch_ended_by_the_receivers_bound_leaves_nothing_staged_on_a_slow_sender() {
    let (client, server, _echoes) = connected_pair_with(
        Arc::new(Semaphore::new(0)),
        Velo::builder().staged_fetch_timeout(TIMEOUT),
    )
    .await;
    let stall = Stall::new();
    stall_fetches(&client, &stall);

    let call = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from(vec![0x5a; STAGED]))
        .instance(server.instance_id())
        .send();
    tokio::time::timeout(BOUND, stall.entered.notified())
        .await
        .expect("the server began to fetch the staged payload");
    tokio::time::timeout(BOUND, call)
        .await
        .expect("the server's bound ended the call")
        .expect_err("the payload never arrived");
    let staged = stall.asked()[0];
    assert!(
        staged_fetch::still_staged(&client, staged),
        "freed before its lease was granted: this test is not about the lease"
    );

    // The sender catches up, and grants a lease to a fetch that is gone.
    stall.gate.add_permits(1);
    assert!(
        released_within(&client, staged, BOUND).await,
        "the sender still holds a payload whose fetch its receiver gave up"
    );
}

/// Control for the stalled fetch: when the sender answers, a staged request
/// with a timeout gets the whole payload back, and neither side keeps a slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_request_with_a_timeout_answers_in_full() {
    let (client, server, echoes) = connected_pair(Arc::new(Semaphore::new(0))).await;
    let payload = Bytes::from((0..STAGED).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let reply = tokio::time::timeout(
        BOUND,
        client
            .unary("echo")
            .unwrap()
            .raw_payload(payload.clone())
            .timeout(BOUND)
            .instance(server.instance_id())
            .send(),
    )
    .await
    .expect("echo answers")
    .expect("echo succeeds");
    assert_eq!(reply, payload);
    assert_eq!(echoes.load(Ordering::Acquire), 1);
    wait_for_slots(&server, 0).await;
    wait_for_slots(&client, 0).await;
}

/// Sync, unary and typed unary to handlers that hold their reply all fail with
/// `RequestTimeout` once the timeout runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_with_no_answer_fails_with_request_timeout() {
    let gate = Arc::new(Semaphore::new(0));
    let (client, server, _) = connected_pair(gate.clone()).await;
    let target = server.instance_id();

    expect_timeout(
        Instant::now(),
        client
            .unary("held")
            .unwrap()
            .timeout(TIMEOUT)
            .instance(target)
            .send(),
    )
    .await;
    expect_timeout(
        Instant::now(),
        client
            .am_sync("held_sync")
            .unwrap()
            .timeout(TIMEOUT)
            .instance(target)
            .send(),
    )
    .await;
    expect_timeout(
        Instant::now(),
        client
            .typed_unary::<u32>("held_typed")
            .unwrap()
            .payload(1u32)
            .unwrap()
            .timeout(TIMEOUT)
            .instance(target)
            .send(),
    )
    .await;

    gate.add_permits(3);
}

/// A timed-out call gives its response slot back at expiry, even while the
/// caller still holds the result, and a dropped result gives its slot back
/// too. The late reply to the timed-out call is then discarded: the next call
/// gets its own answer. A late reply on a reused slot is pinned in
/// `messenger/client/builders/tests.rs`, because the free list here hands the
/// next call a fresh slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_or_dropped_call_releases_its_response_slot() {
    let gate = Arc::new(Semaphore::new(0));
    let (client, server, _) = connected_pair(gate.clone()).await;
    let target = server.instance_id();
    wait_for_slots(&client, 0).await;
    let baseline = client.response_slots_in_use();

    // Held past expiry on purpose: an awaited-by-value result is dropped when
    // it resolves, which would release the slot whatever expiry did.
    let started = Instant::now();
    let mut timed_out = client
        .unary("held")
        .unwrap()
        .timeout(TIMEOUT)
        .instance(target)
        .send();
    assert_eq!(client.response_slots_in_use(), baseline + 1);
    expect_timeout(started, &mut timed_out).await;
    assert_eq!(
        client.response_slots_in_use(),
        baseline,
        "the slot went back at expiry, with the result still held"
    );

    // Control: a dropped result, with no timeout, releases its slot as well.
    let dropped = client.unary("held").unwrap().instance(target).send();
    assert_eq!(client.response_slots_in_use(), baseline + 1);
    drop(dropped);
    assert_eq!(client.response_slots_in_use(), baseline);

    // Let both held handlers reply now. Their replies name released slots.
    gate.add_permits(2);
    let reply = tokio::time::timeout(
        BOUND,
        client
            .unary("echo")
            .unwrap()
            .raw_payload(Bytes::from_static(b"mine"))
            .timeout(BOUND)
            .instance(target)
            .send(),
    )
    .await
    .expect("echo answers")
    .expect("echo succeeds");
    assert_eq!(reply, Bytes::from_static(b"mine"));
    wait_for_slots(&client, baseline).await;
    drop(timed_out);
}

/// A peer the receiver has forgotten gets no reply, so only `.timeout` ends
/// its call.
///
/// A reply does not go back over the connection the request came in on. The
/// receiver routes it through its own peer table, to the sender's worker id.
/// When the receiver has unregistered the sender (velo-hub does this when a
/// registration is removed), the handler runs and its reply has nowhere to go:
/// the receiver logs a send error and the sender hears nothing. Without a
/// timeout the sender would wait for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_to_a_receiver_that_forgot_the_caller_times_out() {
    let gate = Arc::new(Semaphore::new(0));
    let (client, server, echoes) = connected_pair(gate).await;
    let target = server.instance_id();

    // Mutually registered: the reply comes back.
    let reply = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from_static(b"hi"))
        .timeout(BOUND)
        .instance(target)
        .send()
        .await
        .expect("echo answers while both know each other");
    assert_eq!(reply, Bytes::from_static(b"hi"));

    server.unregister_peer(client.instance_id());

    expect_timeout(
        Instant::now(),
        client
            .unary("echo")
            .unwrap()
            .raw_payload(Bytes::from_static(b"hi"))
            .timeout(TIMEOUT)
            .instance(target)
            .send(),
    )
    .await;
    // The request did arrive and the handler ran: what went missing is the
    // reply, not the request.
    assert_eq!(echoes.load(Ordering::Acquire), 2);
    wait_for_slots(&client, 0).await;
}

/// A call answered in time resolves `Ok`, and the timeout leaves nothing
/// behind: the slot is back, and a later expiry has nothing to fire on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fast_answer_with_a_timeout_resolves_ok() {
    let gate = Arc::new(Semaphore::new(0));
    let (client, server, _) = connected_pair(gate).await;
    let target = server.instance_id();

    let mut result = client
        .unary("echo")
        .unwrap()
        .raw_payload(Bytes::from_static(b"quick"))
        .timeout(TIMEOUT)
        .instance(target)
        .send();
    let reply = tokio::time::timeout(BOUND, &mut result)
        .await
        .expect("echo answers")
        .expect("an answer in time is Ok");
    assert_eq!(reply, Bytes::from_static(b"quick"));
    assert_eq!(client.response_slots_in_use(), 0);

    // Past the deadline the finished result is still finished: no timeout
    // fires late on it.
    tokio::time::sleep(TIMEOUT * 2).await;
    let again = futures::FutureExt::now_or_never(&mut result)
        .expect("a finished result resolves at once")
        .expect_err("a finished result stays finished");
    assert!(
        again.downcast_ref::<RequestTimeout>().is_none(),
        "{again:#}"
    );
}
