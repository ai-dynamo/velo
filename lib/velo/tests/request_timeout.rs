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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
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
    let server = Velo::builder()
        .add_transport(new_transport())
        .build()
        .await
        .unwrap();
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
/// too. The late reply to the timed-out call is then discarded: the next call,
/// which may reuse the same slot, gets its own answer.
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
