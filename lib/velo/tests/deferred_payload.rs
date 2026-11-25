// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A handler that decides on a staged payload before velo fetches it.
//!
//! A payload over the staging threshold (256 KiB) does not travel in its
//! message. The sender stages it, and the receiver fetches it with an
//! `_rv_acquire` call to the sender, then `_rv_pull` calls. A handler built
//! with `.defer_large_payload()` runs before that fetch, with the size in
//! `ctx.payload_len()`, and fetches with `ctx.take_payload()` only if it
//! wants the bytes. The sender frees the payload when the call's reply
//! arrives. These tests count the calls that reach the payload's owner and
//! read what it still holds, so "no fetch" and "freed" are facts, not
//! timing guesses.

#[path = "support/staged_fetch.rs"]
mod staged_fetch;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use staged_fetch::{Stall, released_within, stall_fetches, still_staged};
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::*;

/// Over the staging threshold (256 KiB), so the sender stages the payload.
const STAGED: usize = 300 * 1024;
/// How long a test waits for a state that must come. Long, so a loaded host
/// does not fail a correct run; a passing run does not wait for it.
const BOUND: Duration = Duration::from_secs(10);

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

/// `len` bytes that differ from one position to the next, starting at
/// `seed`, so a payload compared byte for byte cannot pass by accident.
fn probe(len: usize, seed: u8) -> Bytes {
    (0..len)
        .map(|i| seed.wrapping_add(i as u8))
        .collect::<Vec<u8>>()
        .into()
}

async fn velo_with(builder: VeloBuilder) -> Velo {
    builder
        .add_transport(new_transport())
        .build()
        .await
        .unwrap()
}

/// A client and a server that know each other. The client sends, so it owns
/// every payload it stages.
async fn pair_with(server: VeloBuilder) -> (Velo, Velo) {
    let server = velo_with(server).await;
    let client = velo_with(Velo::builder()).await;
    client.register_peer(server.peer_info()).unwrap();
    server.register_peer(client.peer_info()).unwrap();
    (client, server)
}

async fn connected_pair() -> (Velo, Velo) {
    pair_with(Velo::builder()).await
}

/// Learn the server's handlers, so a later `send()` puts its frame on the
/// wire before it returns, in the order of the calls.
async fn handshake(client: &Velo, server: &Velo) {
    client
        .available_handlers(server.instance_id())
        .await
        .unwrap();
}

/// One unary call to `handler` on `server`, with no timeout.
async fn call(
    client: &Velo,
    server: &Velo,
    handler: &str,
    payload: Bytes,
) -> anyhow::Result<Bytes> {
    client
        .unary(handler)
        .unwrap()
        .raw_payload(payload)
        .instance(server.instance_id())
        .send()
        .await
}

/// A unary call that carries `headers` and no payload of its own. With a
/// `_rv` header it is the message of a sender that stages without writing
/// `_rv_len` and never frees on reply.
async fn call_with_headers(
    client: &Velo,
    server: &Velo,
    handler: &str,
    headers: &[(&str, String)],
) -> anyhow::Result<Bytes> {
    let headers = headers
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect::<HashMap<_, _>>();
    client
        .unary(handler)
        .unwrap()
        .headers(headers)
        .instance(server.instance_id())
        .send()
        .await
}

/// Answer `name` on `owner` with `serve`, counting each call. Only a fetch
/// sends `_rv_acquire`, and only a receiver that needs a length it was not
/// sent sends `_rv_metadata`.
fn count_calls<Req, Resp>(
    owner: &Velo,
    name: &'static str,
    serve: fn(&Velo, &TypedContext<Req>) -> anyhow::Result<Resp>,
) -> Arc<AtomicUsize>
where
    Req: serde::de::DeserializeOwned + Send + Sync + 'static,
    Resp: serde::Serialize + Send + Sync + 'static,
{
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    let node = owner.downgrade();
    owner
        .messenger()
        .register_streaming_handler(
            Handler::typed_unary(name, move |ctx: TypedContext<Req>| {
                counter.fetch_add(1, Ordering::AcqRel);
                let node = node
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("owner dropped"))?;
                serve(&node, &ctx)
            })
            .build(),
        )
        .unwrap();
    seen
}

fn count_acquires(owner: &Velo) -> Arc<AtomicUsize> {
    count_calls(owner, "_rv_acquire", |node, ctx| {
        node.serve_rv_acquire(&ctx.input, ctx.headers.as_ref())
    })
}

fn count_metadata(owner: &Velo) -> Arc<AtomicUsize> {
    count_calls(owner, "_rv_metadata", |node, ctx| {
        node.serve_rv_metadata(&ctx.input)
    })
}

/// The local ids of the payloads `owner` holds staged. Ids count up from 1
/// on each node, and these tests stage a few each.
fn staged_ids(owner: &Velo) -> Vec<u64> {
    (1..=64).filter(|id| still_staged(owner, *id)).collect()
}

/// Stage `payload` on `owner` the way a sender without `_rv_len` does, and
/// return its `_rv` header. Nothing frees it on reply.
fn stage_by_hand(owner: &Velo, payload: Bytes, ttl: Option<Duration>) -> (u64, String) {
    let handle = match ttl {
        Some(ttl) => owner.register_data_with(payload, RegisterOptions::new().ttl(ttl)),
        None => owner.register_data(payload),
    };
    (handle.unpack().1, handle.as_u128().to_string())
}

/// What a handler saw.
#[derive(Debug, Default, Clone, PartialEq)]
struct Seen {
    len: Option<u64>,
    inline: usize,
}

fn seen(ctx: &Context) -> Seen {
    Seen {
        len: ctx.payload_len(),
        inline: ctx.payload.len(),
    }
}

/// A deferred handler that replies without taking its payload never makes
/// the receiver fetch it: the owner sees no `_rv_acquire`.
///
/// A saturated worker refuses a large request on its size, and the bytes
/// are the expensive part. Fetched before dispatch, the refusal costs a full
/// pull. The reply proves the count is final: an eager receiver fetches
/// before it dispatches, so a fetch would have reached the owner before the
/// handler ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_handler_refuses_without_fetching() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let saw = Arc::new(Mutex::new(None));
    let record = Arc::clone(&saw);
    server
        .register_handler(
            Handler::unary_handler("refuses", move |ctx| {
                *record.lock() = Some(seen(&ctx));
                Ok(Some(Bytes::from_static(b"busy")))
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    let reply = call(&client, &server, "refuses", probe(STAGED, 1))
        .await
        .expect("a refusal is a reply");

    assert_eq!(&reply[..], b"busy");
    assert_eq!(
        acquires.load(Ordering::Acquire),
        0,
        "the receiver fetched a payload its handler refused"
    );
    assert_eq!(
        saw.lock().clone(),
        Some(Seen {
            len: Some(STAGED as u64),
            inline: 0
        })
    );
}

/// An `am_sync` call defers too: its ack or nack frees the payload as a
/// reply does, so an AM handler that refuses never pulls it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_am_sync_handler_refuses_without_fetching() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let staged = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&staged);
    let owner = client.clone();
    server
        .register_handler(
            Handler::am_handler("refuses", move |ctx| {
                assert_eq!(ctx.payload_len(), Some(STAGED as u64));
                record.lock().extend(staged_ids(&owner));
                anyhow::bail!("busy")
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    let nack = client
        .am_sync("refuses")
        .unwrap()
        .raw_payload(probe(STAGED, 15))
        .instance(server.instance_id())
        .send()
        .await
        .expect_err("a refusal is a nack");

    assert!(nack.to_string().contains("busy"), "{nack:#}");
    assert_eq!(acquires.load(Ordering::Acquire), 0);
    let ids = staged.lock().clone();
    assert_eq!(ids.len(), 1);
    assert!(released_within(&client, ids[0], BOUND).await);
}

/// A deferred handler that takes its payload gets it byte for byte, with one
/// fetch, inside a timed call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_fetch_after_admission_returns_the_bytes() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let taken = Arc::new(Mutex::new(None));
    let record = Arc::clone(&taken);
    server
        .register_handler(
            Handler::unary_handler_async("takes", move |mut ctx: Context| {
                let record = Arc::clone(&record);
                async move {
                    *record.lock() = Some(ctx.take_payload().await?);
                    Ok(None)
                }
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    client
        .unary("takes")
        .unwrap()
        .raw_payload(probe(STAGED, 2))
        .instance(server.instance_id())
        .timeout(BOUND)
        .send()
        .await
        .expect("the handler takes its payload and replies");

    assert_eq!(taken.lock().clone(), Some(probe(STAGED, 2)));
    assert_eq!(acquires.load(Ordering::Acquire), 1);
}

/// The length comes from the sender's `_rv_len` header with no call to the
/// owner. A message from a sender that does not write it costs one
/// `_rv_metadata` call, and gives the same length.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_len_from_header_and_from_metadata_fallback() {
    let (client, server) = connected_pair().await;
    let metadata = count_metadata(&client);
    let acquires = count_acquires(&client);
    let lens = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&lens);
    server
        .register_handler(
            Handler::unary_handler("sizes", move |ctx| {
                record.lock().push(ctx.payload_len());
                Ok(None)
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    call(&client, &server, "sizes", probe(STAGED, 3))
        .await
        .unwrap();
    assert_eq!(
        metadata.load(Ordering::Acquire),
        0,
        "the header was ignored"
    );

    let (local_id, rv) = stage_by_hand(&client, probe(STAGED, 3), None);
    call_with_headers(&client, &server, "sizes", &[("_rv", rv)])
        .await
        .unwrap();
    assert_eq!(metadata.load(Ordering::Acquire), 1);
    assert_eq!(*lens.lock(), [Some(STAGED as u64), Some(STAGED as u64)]);
    assert_eq!(acquires.load(Ordering::Acquire), 0);
    client.unregister_data(DataHandle::pack(client.instance_id().worker_id(), local_id));
}

/// A handler that refuses without fetching cannot free the payload: freeing
/// needs the lease a fetch gives. The sender frees it when the reply comes,
/// so an untimed call, which gives its payload no time-to-live, leaves
/// nothing staged. An error reply frees it the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sender_frees_staged_payload_when_reply_arrives() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let staged = Arc::new(Mutex::new(Vec::new()));
    for (name, fails) in [("refuses", false), ("fails", true)] {
        let record = Arc::clone(&staged);
        let owner = client.clone();
        server
            .register_handler(
                Handler::unary_handler(name, move |_ctx| {
                    // Staged while the handler runs, so the reply frees it.
                    record.lock().extend(staged_ids(&owner));
                    if fails {
                        anyhow::bail!("no room");
                    }
                    Ok(None)
                })
                .defer_large_payload()
                .build(),
            )
            .unwrap();

        let reply = call(&client, &server, name, probe(STAGED, 4)).await;
        assert_eq!(reply.is_err(), fails);
        let ids = std::mem::take(&mut *staged.lock());
        assert_eq!(ids.len(), 1, "{name}: one payload staged while it ran");
        assert!(
            released_within(&client, ids[0], BOUND).await,
            "{name}: the sender kept a payload after its reply"
        );
    }
    assert_eq!(acquires.load(Ordering::Acquire), 0);
}

/// A handler capped at one fetch refuses a second while the first is in
/// flight, at once and without asking the owner. Once the first ends, the
/// next fetch runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetch_limit_refuses_without_fetching() {
    let (client, server) = connected_pair().await;
    let stall = Stall::new();
    stall_fetches(&client, &stall);
    server
        .register_handler(
            Handler::unary_handler_async("capped", |mut ctx: Context| async move {
                match ctx.take_payload().await {
                    Ok(bytes) => Ok(Some(bytes.slice(0..1))),
                    Err(err) if err.is::<FetchLimitReached>() => {
                        Ok(Some(Bytes::from(err.to_string())))
                    }
                    Err(err) => Err(err),
                }
            })
            .max_concurrent_fetches(1)
            .build(),
        )
        .unwrap();

    let first = {
        let (client, server) = (client.clone(), server.clone());
        tokio::spawn(async move { call(&client, &server, "capped", probe(STAGED, 10)).await })
    };
    tokio::time::timeout(BOUND, stall.entered.notified())
        .await
        .expect("the first fetch reached the owner");

    let refused = call(&client, &server, "capped", probe(STAGED, 20))
        .await
        .unwrap();
    assert_eq!(
        std::str::from_utf8(&refused).unwrap(),
        FetchLimitReached { limit: 1 }.to_string()
    );
    assert_eq!(
        stall.asked().len(),
        1,
        "a fetch over the cap reached the owner"
    );

    stall.gate.add_permits(2);
    assert_eq!(&first.await.unwrap().unwrap()[..], [10]);
    let third = call(&client, &server, "capped", probe(STAGED, 30))
        .await
        .unwrap();
    assert_eq!(&third[..], [30]);
    assert_eq!(stall.asked().len(), 2);
}

/// A handler without the setting runs on the fetched payload as before: the
/// payload is in the context, there is no deferred length, the staging
/// headers are gone, and `take_payload` hands back the same bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_deferred_handlers_unchanged() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let saw = Arc::new(Mutex::new(None));
    let record = Arc::clone(&saw);
    server
        .register_handler(
            Handler::unary_handler_async("eager", move |mut ctx: Context| {
                let record = Arc::clone(&record);
                async move {
                    let before = (seen(&ctx), ctx.headers.clone());
                    *record.lock() = Some((before, ctx.take_payload().await?));
                    Ok(None)
                }
            })
            .build(),
        )
        .unwrap();

    call(&client, &server, "eager", probe(STAGED, 5))
        .await
        .unwrap();

    let (before, taken) = saw.lock().clone().unwrap();
    assert_eq!(
        before,
        (
            Seen {
                len: None,
                inline: STAGED
            },
            None
        )
    );
    assert_eq!(taken, probe(STAGED, 5));
    assert_eq!(acquires.load(Ordering::Acquire), 1);
}

/// A receiver that fetches before dispatch also releases the payload, and
/// the sender frees it on the reply. Either can come first; together they
/// leave nothing staged, and the call is answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eager_receiver_and_freeing_sender_leave_nothing_staged() {
    let (client, server) = connected_pair().await;
    server
        .register_handler(
            Handler::unary_handler("eager", |ctx| Ok(Some(ctx.payload.slice(1..2)))).build(),
        )
        .unwrap();

    for seed in 0..8 {
        let reply = call(&client, &server, "eager", probe(STAGED, seed))
            .await
            .unwrap();
        assert_eq!(&reply[..], [seed.wrapping_add(1)]);
    }
    let gone = tokio::time::timeout(BOUND, async {
        while !staged_ids(&client).is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(gone.is_ok(), "left staged: {:?}", staged_ids(&client));
}

/// A receiver that does not know `_rv_len` passes it to its handler as an
/// ordinary header. A `_rv_len` without `_rv` is what such a receiver's
/// handler, or one that forwards headers, may send: it must not be read as
/// staging, and nothing on the receive path may refuse it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stray_rv_len_header_is_an_ordinary_header() {
    let (client, server) = connected_pair().await;
    let saw = Arc::new(Mutex::new(None));
    let record = Arc::clone(&saw);
    server
        .register_handler(
            Handler::unary_handler("plain", move |ctx| {
                *record.lock() = Some((seen(&ctx), ctx.headers.clone()));
                Ok(None)
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    call_with_headers(&client, &server, "plain", &[("_rv_len", "9".into())])
        .await
        .unwrap();

    let (seen, headers) = saw.lock().clone().unwrap();
    assert_eq!(seen, Seen::default());
    assert_eq!(
        headers,
        Some(HashMap::from([("_rv_len".to_string(), "9".to_string())]))
    );
}

/// A sender that does not free on reply leaves a refused payload staged
/// until its time-to-live, which a timed call gives it. An untimed call from
/// such a sender stays staged; that is a documented limitation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_senders_refused_payload_stays_until_its_time_to_live() {
    // Long, so that the check right after the reply cannot race it.
    const TTL: Duration = Duration::from_secs(5);
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    server
        .register_handler(
            Handler::unary_handler("refuses", |_ctx| Ok(None))
                .defer_large_payload()
                .build(),
        )
        .unwrap();

    let (local_id, rv) = stage_by_hand(&client, probe(STAGED, 6), Some(TTL));
    call_with_headers(&client, &server, "refuses", &[("_rv", rv)])
        .await
        .unwrap();

    assert!(
        still_staged(&client, local_id),
        "freed with no fetch and no free"
    );
    assert!(released_within(&client, local_id, TTL + BOUND).await);
    assert_eq!(acquires.load(Ordering::Acquire), 0);
}

/// A deferred handler that takes an old sender's payload fetches it as
/// before: the bytes arrive whole, and the fetch's own `_rv_release` frees
/// the payload, with nothing from the sender.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_old_senders_payload_is_fetched_and_released() {
    let (client, server) = connected_pair().await;
    let taken = Arc::new(Mutex::new(None));
    let record = Arc::clone(&taken);
    server
        .register_handler(
            Handler::unary_handler_async("takes", move |mut ctx: Context| {
                let record = Arc::clone(&record);
                async move {
                    *record.lock() = Some(ctx.take_payload().await?);
                    Ok(None)
                }
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    let (local_id, rv) = stage_by_hand(&client, probe(STAGED, 7), None);
    call_with_headers(&client, &server, "takes", &[("_rv", rv)])
        .await
        .unwrap();

    assert_eq!(taken.lock().clone(), Some(probe(STAGED, 7)));
    assert!(released_within(&client, local_id, BOUND).await);
}

/// A deferred payload is fetched once. A second take, here on a clone of the
/// context, fails without asking the owner again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn take_payload_is_single_use() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let results = Arc::new(Mutex::new(None));
    let record = Arc::clone(&results);
    server
        .register_handler(
            Handler::unary_handler_async("twice", move |mut ctx: Context| {
                let record = Arc::clone(&record);
                async move {
                    let mut clone = ctx.clone();
                    let first = ctx.take_payload().await;
                    let second = clone.take_payload().await;
                    *record.lock() = Some((first.is_ok(), second.is_err()));
                    Ok(None)
                }
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    call(&client, &server, "twice", probe(STAGED, 8))
        .await
        .unwrap();

    assert_eq!(*results.lock(), Some((true, true)));
    assert_eq!(acquires.load(Ordering::Acquire), 1);
}

/// The sender frees a payload when the reply arrives, so the payload can be
/// taken only before the handler returns. A context clone that tries later
/// fails without asking the owner, and no longer reports a length. It does
/// not get an empty payload as if the request had none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn take_payload_after_the_reply_fails() {
    let (client, server) = connected_pair().await;
    let acquires = count_acquires(&client);
    let (late_tx, late_rx) = tokio::sync::oneshot::channel();
    let late_tx = Mutex::new(Some(late_tx));
    let (replied_tx, replied_rx) = tokio::sync::oneshot::channel::<()>();
    let replied_rx = Mutex::new(Some(replied_rx));
    server
        .register_handler(
            Handler::unary_handler("keeps", move |ctx| {
                let mut clone = ctx.clone();
                let replied = replied_rx.lock().take().unwrap();
                let late = late_tx.lock().take().unwrap();
                tokio::spawn(async move {
                    let _ = replied.await;
                    let len = clone.payload_len();
                    let _ = late.send((len, clone.take_payload().await));
                });
                Ok(None)
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    call(&client, &server, "keeps", probe(STAGED, 9))
        .await
        .unwrap();
    replied_tx.send(()).unwrap();
    let (len, taken) = late_rx.await.unwrap();

    assert_eq!(len, None);
    assert!(taken.is_err(), "a take after the reply got {taken:?}");
    assert_eq!(acquires.load(Ordering::Acquire), 0);
}

/// A fire-and-forget message has no reply to free its payload, so a
/// deferring handler gets it fetched, as a handler that does not defer does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fire_and_forget_to_a_deferred_handler_is_fetched_first() {
    let (client, server) = connected_pair().await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Mutex::new(Some(tx));
    server
        .register_handler(
            Handler::am_handler("fire", move |ctx| {
                if let Some(tx) = tx.lock().take() {
                    let _ = tx.send((seen(&ctx), ctx.payload.clone()));
                }
                Ok(())
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    client
        .am_send("fire")
        .unwrap()
        .raw_payload(probe(STAGED, 11))
        .instance(server.instance_id())
        .send()
        .await
        .unwrap();
    let (seen, payload) = tokio::time::timeout(BOUND, rx).await.unwrap().unwrap();

    assert_eq!(
        seen,
        Seen {
            len: None,
            inline: STAGED
        }
    );
    assert_eq!(payload, probe(STAGED, 11));
}

/// A deferred payload with its length dispatches as it arrives, so an
/// ordered handler runs it before a small message sent after it. Fetched
/// before dispatch, it was dispatched from a task of its own, after the
/// small message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deferred_ordered_handler_keeps_arrival_order() {
    let (client, server) = connected_pair().await;
    let order = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&order);
    server
        .register_handler(
            Handler::unary_handler("ordered", move |ctx| {
                record.lock().push(ctx.payload_len().is_some());
                Ok(None)
            })
            .ordered()
            .defer_large_payload()
            .build(),
        )
        .unwrap();
    handshake(&client, &server).await;

    let staged = call(&client, &server, "ordered", probe(STAGED, 12));
    let small = call(&client, &server, "ordered", probe(16, 13));
    let (staged, small) = tokio::join!(staged, small);
    staged.unwrap();
    small.unwrap();

    assert_eq!(*order.lock(), [true, false], "true is the staged message");
}

/// A deferred payload over the receiver's limit fails at the take, with an
/// error the caller sees, and the sender still frees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn take_payload_over_the_receivers_limit_fails() {
    let (client, server) = pair_with(Velo::builder().staged_payload_max(STAGED - 1)).await;
    let staged = Arc::new(Mutex::new(Vec::new()));
    let record = Arc::clone(&staged);
    let owner = client.clone();
    server
        .register_handler(
            Handler::unary_handler_async("takes", move |mut ctx: Context| {
                record.lock().extend(staged_ids(&owner));
                async move {
                    ctx.take_payload().await?;
                    Ok(None)
                }
            })
            .defer_large_payload()
            .build(),
        )
        .unwrap();

    let err = call(&client, &server, "takes", probe(STAGED, 14))
        .await
        .expect_err("a payload over the limit is refused");

    assert!(err.to_string().contains("limit"), "{err:#}");
    let ids = staged.lock().clone();
    assert_eq!(ids.len(), 1);
    assert!(released_within(&client, ids[0], BOUND).await);
}
