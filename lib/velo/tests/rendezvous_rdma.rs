// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The rendezvous RDMA path, end to end over `UCX_TLS=tcp`.
//!
//! Same code path as an RDMA lane with no hardware requirement, so this runs on
//! stock CI runners. What tcp cannot see is recorded in the plan's
//! test-fidelity ledger; what it *can* see is every decision this phase makes:
//! which path an acquire takes, what happens when a descriptor is malformed,
//! what happens when the GET fails, and what the owner does about a consumer
//! that never comes back.
//!
//! # Assertions are on the path metric, not on timing
//!
//! "The data arrived" is true on both paths and therefore proves nothing about
//! which one ran. Every test here asserts on
//! `velo_rendezvous_rdma_path_total{path,reason}`, which is the series the
//! implementation emits at each decision point — so a test that says "this took
//! the RDMA path" fails if the fallback silently took over, which is exactly
//! the regression a correctness-only assertion would miss.
//!
//! Waits are on counters and handler availability, never on sleeps. Where a
//! condition can only become true in the background — the reaper — the wait is
//! a bounded poll that reports what it actually saw when it gives up, so a
//! regression reads as a failed assertion rather than as a timeout to debug.
//!
//! # What is covered elsewhere, and why
//!
//! * **The `VELO_RDMA_RENDEZVOUS_DISABLE` kill switch.** The environment is
//!   process-global and `cargo test` runs these in parallel, so a test that set
//!   the variable would switch the path off for every other test building a
//!   `Velo` at that moment — it would fail its neighbours, not itself. The rule
//!   it applies is unit-tested exhaustively in `lib.rs`
//!   (`the_rdma_kill_switch_reads_only_affirmatives`), the field it writes is
//!   exercised here through [`RdmaRendezvousConfig::enabled`], and the two
//!   together are the same statement without the harness hazard.
//! * **Descriptor framing.** `rendezvous::descriptor`'s own tests walk every
//!   truncation, trailing byte and lying length. Here the question is only what
//!   velo *does* when a descriptor will not decode.
//! * **Wire skew.** `rendezvous::protocol`'s tests round-trip against the
//!   pre-phase struct shapes in both directions. The end-to-end half — an
//!   acquire with no `rdma` key at all — is
//!   `an_acquire_without_the_offer_field_is_answered_chunked` below.

#![cfg(all(target_os = "linux", feature = "ucx", feature = "test-helpers"))]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::rendezvous::RdmaTestHook;
use velo::rendezvous::protocol::AcquireResponse;
use velo::transports::ucx::UcxTransportBuilder;
use velo::*;

/// Generous ceiling for anything that should resolve promptly.
const T: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One velo instance with a UCX transport and its own metrics registry.
struct Node {
    velo: Arc<Velo>,
    registry: Registry,
}

impl Node {
    async fn start(config: Option<RdmaConfig>) -> Self {
        let transport = Arc::new(
            UcxTransportBuilder::new()
                .tls("tcp")
                .build()
                .expect("build ucx transport"),
        );
        let registry = Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
        let mut builder = Velo::builder()
            .metrics(metrics)
            .add_ucx_transport(transport);
        if let Some(config) = config {
            builder = builder.rdma_config(config);
        }
        let velo = builder.build().await.expect("build velo");
        Self { velo, registry }
    }

    /// A node whose UCX transport was added with `add_transport` rather than
    /// `add_ucx_transport`.
    ///
    /// The documented messaging-only configuration, and the faithful stand-in
    /// for a consumer that predates the RDMA path: the transports match, the
    /// control plane works, and there is no registry, so no offer is ever made.
    async fn start_without_rdma() -> Self {
        let transport = Arc::new(
            UcxTransportBuilder::new()
                .tls("tcp")
                .build()
                .expect("build ucx transport"),
        );
        let registry = Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry).expect("register metrics"));
        let velo = Velo::builder()
            .metrics(metrics)
            .add_transport(transport)
            .build()
            .await
            .expect("build velo");
        Self { velo, registry }
    }

    /// Sum of `velo_rendezvous_rdma_path_total` for one reason label.
    fn path_count(&self, reason: &str) -> u64 {
        counter_with_label(
            &self.registry,
            "velo_rendezvous_rdma_path_total",
            "reason",
            reason,
        )
    }

    /// Value of a bare counter.
    fn counter(&self, name: &str) -> u64 {
        self.registry
            .gather()
            .iter()
            .filter(|family| family.name() == name)
            .flat_map(|family| family.get_metric())
            .map(|m| m.get_counter().value() as u64)
            .sum()
    }

    /// Value of a bare gauge.
    fn gauge(&self, name: &str) -> u64 {
        self.registry
            .gather()
            .iter()
            .filter(|family| family.name() == name)
            .flat_map(|family| family.get_metric())
            .map(|m| m.get_gauge().value() as u64)
            .sum()
    }

    /// Number of observations in a histogram.
    fn histogram_count(&self, name: &str) -> u64 {
        self.registry
            .gather()
            .iter()
            .filter(|family| family.name() == name)
            .flat_map(|family| family.get_metric())
            .map(|m| m.get_histogram().get_sample_count())
            .sum()
    }
}

fn counter_with_label(registry: &Registry, name: &str, label: &str, value: &str) -> u64 {
    registry
        .gather()
        .iter()
        .filter(|family| family.name() == name)
        .flat_map(|family| family.get_metric())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|pair| pair.name() == label && pair.value() == value)
        })
        .map(|metric| metric.get_counter().value() as u64)
        .sum()
}

/// Two connected instances, both with the RDMA path available.
struct Pair {
    owner: Node,
    consumer: Node,
}

impl Pair {
    async fn new() -> Self {
        Self::with_configs(None, None).await
    }

    async fn with_configs(owner: Option<RdmaConfig>, consumer: Option<RdmaConfig>) -> Self {
        let owner = Node::start(owner).await;
        let consumer = Node::start(consumer).await;
        connect(&owner, &consumer).await;
        Self { owner, consumer }
    }

    /// A capable owner and a consumer with no registry at all.
    async fn with_incapable_consumer() -> Self {
        let owner = Node::start(None).await;
        let consumer = Node::start_without_rdma().await;
        connect(&owner, &consumer).await;
        Self { owner, consumer }
    }
}

/// Poll `condition` until it holds, or panic with what was actually observed.
///
/// Not a sleep-sync: it cannot pass early by luck, and on failure it reports
/// the state rather than only that time ran out — the difference between a
/// failed assertion and a debugging session.
async fn wait_until<C, F, D>(what: &str, mut condition: C, mut describe: D)
where
    C: FnMut() -> F,
    F: std::future::Future<Output = bool>,
    D: FnMut() -> String,
{
    let deadline = std::time::Instant::now() + T;
    while std::time::Instant::now() < deadline {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} never became true; observed: {}", describe());
}

/// Whether the owner has freed a slot. Local, so it reflects the owner's own
/// state rather than a round trip that might race with it.
async fn slot_is_gone(node: &Node, handle: DataHandle) -> bool {
    node.velo.metadata(handle).await.is_err()
}

/// Register each side with the other and wait until the control plane is
/// usable. Deterministic: `wait_for_handler` resolves on the handler list
/// actually arriving, so there is nothing to sleep for.
async fn connect(owner: &Node, consumer: &Node) {
    consumer
        .velo
        .register_peer(owner.velo.peer_info())
        .expect("consumer registers owner");
    owner
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("owner registers consumer");

    tokio::time::timeout(
        T,
        consumer
            .velo
            .wait_for_handler(owner.velo.instance_id(), "_rv_acquire"),
    )
    .await
    .expect("owner handler list did not arrive")
    .expect("wait_for_handler");
    tokio::time::timeout(
        T,
        owner
            .velo
            .wait_for_handler(consumer.velo.instance_id(), "_rv_acquire"),
    )
    .await
    .expect("consumer handler list did not arrive")
    .expect("wait_for_handler");
    // The keepalive rides the same list; waiting for it explicitly keeps the
    // renewal test from racing the handler-list broadcast.
    tokio::time::timeout(
        T,
        consumer
            .velo
            .wait_for_handler(owner.velo.instance_id(), "_rv_lease_renew"),
    )
    .await
    .expect("owner keepalive handler did not arrive")
    .expect("wait_for_handler");
}

/// A payload whose every byte depends on its offset, so a transfer that lands
/// shifted, short, or duplicated fails rather than passing on a lucky pattern.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| (i.wrapping_mul(31).wrapping_add(i >> 8)) as u8)
        .collect()
}

fn assert_pattern(data: &[u8], expected_len: usize) {
    assert_eq!(data.len(), expected_len, "wrong length");
    let expected = pattern(expected_len);
    assert!(
        data == expected,
        "payload mismatch: first differing byte at {:?}",
        data.iter().zip(expected.iter()).position(|(a, b)| a != b)
    );
}

async fn shutdown(pair: Pair) {
    pair.consumer
        .velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
    pair.owner
        .velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
}

// ---------------------------------------------------------------------------
// 1. The fast path, at several sizes
// ---------------------------------------------------------------------------

/// A pinned owner and an offering consumer take the RDMA path, and the bytes
/// arrive intact at the threshold, well past it, and at a size that is not a
/// multiple of the 4 KiB suballocation granule or of the chunk size.
#[tokio::test(flavor = "multi_thread")]
async fn pinned_owner_and_offering_consumer_take_the_rdma_path() {
    let pair = Pair::new().await;

    for len in [64 * 1024, 1024 * 1024, 300 * 1024 + 7] {
        let before_owner = pair.owner.path_count("ok");
        let before_consumer = pair.consumer.path_count("ok");

        let payload = pattern(len);
        let handle = pair.owner.velo.register_data_pinned(&payload).await;
        assert!(
            pair.consumer
                .velo
                .metadata(handle)
                .await
                .expect("metadata")
                .pinned,
            "the owner staged {len} B in plain memory"
        );

        let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
        assert_pattern(&data, len);
        pair.consumer
            .velo
            .release(handle, lease)
            .await
            .expect("release");

        assert_eq!(
            pair.owner.path_count("ok"),
            before_owner + 1,
            "the owner did not answer {len} B with a descriptor"
        );
        assert_eq!(
            pair.consumer.path_count("ok"),
            before_consumer + 1,
            "the consumer did not complete an RDMA GET for {len} B"
        );
    }

    assert!(
        pair.consumer
            .histogram_count("velo_rendezvous_rdma_get_duration_seconds")
            >= 3,
        "the GET duration histogram was not observed"
    );
    shutdown(pair).await;
}

// ---------------------------------------------------------------------------
// 2. External regions: the raw-pointer chunked path, and the refusal
// ---------------------------------------------------------------------------

/// An anchor inside caller-registered memory serves the chunked path.
///
/// This is the read path with no `&[u8]` anywhere in it: the registration
/// contract forbids any Rust reference into a registered range, so `get_chunk`
/// copies out through a raw pointer. The range deliberately starts at an offset
/// that is neither granule- nor chunk-aligned and spans several chunks, because
/// the offset arithmetic in that copy is where an off-by-one would live.
#[tokio::test(flavor = "multi_thread")]
async fn an_external_region_anchor_serves_the_chunked_path() {
    let pair = Pair::with_incapable_consumer().await;

    const REGION: usize = 2 * 1024 * 1024;
    const START: u64 = 1_237; // not a multiple of 4096, of 512 KiB, or even of 2
    const LEN: u64 = 1_300 * 1024 + 11;

    let backing = pattern(REGION);
    let guard = pair
        .owner
        .velo
        .register_owned(backing.clone().into_boxed_slice())
        .await
        .map_err(|e| e.cause)
        .expect("register the caller's memory");

    let handle = pair
        .owner
        .velo
        .register_data_in_region(&guard, START..START + LEN)
        .expect("stage an anchor inside the region");

    let meta = pair.consumer.velo.metadata(handle).await.expect("metadata");
    assert!(meta.pinned, "an external anchor is a pinned slot");
    assert_eq!(meta.total_len, LEN);

    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_eq!(data.len(), LEN as usize);
    assert_eq!(
        &data[..],
        &backing[START as usize..(START + LEN) as usize],
        "the raw-pointer chunked read returned the wrong bytes"
    );
    assert_eq!(
        pair.owner.path_count("no_offer"),
        1,
        "a consumer with no registry must not have been offered the RDMA path"
    );
    assert_eq!(pair.owner.path_count("ok"), 0);

    pair.consumer
        .velo
        .release(handle, lease)
        .await
        .expect("release");
    wait_until(
        "the released anchor frees its slot",
        || slot_is_gone(&pair.owner, handle),
        || "the slot is still staged".to_string(),
    )
    .await;
    // With the anchor gone, nothing holds the region's in-flight count up, so
    // the drain finishes rather than timing out.
    assert_eq!(
        guard
            .unregister(T)
            .await
            .expect("unregister after the anchor was released"),
        Deregistered::Drained,
        "the anchor's in-flight guard outlived the slot that held it"
    );

    shutdown(pair).await;
}

/// Reading an anchor whose region has been unmapped refuses, instead of
/// touching memory the caller has been told it may free.
///
/// The in-flight guard makes a deregistration *wait* for staged anchors; this is
/// the other half. Shutdown's sweep is bounded, so a region with an anchor still
/// staged is unmapped anyway and its `deregistered()` latch is closed at the end
/// of `graceful_shutdown` regardless — D8's documented cost of bounding
/// shutdown. From that moment the slot's `RegionWatch` has to turn every read
/// into an error, or the chunked path would `copy_nonoverlapping` out of a
/// deregistered range.
///
/// Driven through `graceful_shutdown` rather than through
/// `RegionGuard::unregister`, which is not a deterministic way to reach this
/// state: `unregister` gives its *whole* sequence one budget, so a drain that
/// cannot finish consumes all of it and the unmap that follows gets none —
/// leaving `Err(Timeout)`, an unconfirmed unmap, and no latch, except when the
/// timer's granularity happens to let the unmap through. Shutdown's
/// `latch_all_deregistered` closes the latch unconditionally once the backend
/// reports nothing registered, which is the property this test needs.
///
/// The read is local, so it does not need the messenger that shutdown took
/// down: the question is what the *store* does, and that is exactly what a
/// remote `_rv_pull` would have reached.
#[tokio::test(flavor = "multi_thread")]
async fn reads_refuse_once_the_region_behind_them_is_gone() {
    let node = Node::start(None).await;

    let backing = pattern(512 * 1024);
    let guard = node
        .velo
        .register_owned(backing.clone().into_boxed_slice())
        .await
        .map_err(|e| e.cause)
        .expect("register");
    let watch = guard.watch();
    let handle = node
        .velo
        .register_data_in_region(&guard, 0..512 * 1024)
        .expect("stage an anchor");

    // While the region is live the anchor reads correctly, so the refusal below
    // is a change of state rather than a slot that never worked.
    let (data, lease) = node.velo.get(handle).await.expect("get while registered");
    assert_eq!(&data[..], &backing[..]);
    node.velo.detach(handle, lease).await.expect("detach");

    // The anchor holds an in-flight guard, so the sweep cannot drain the region;
    // it unmaps anyway and the latch is closed at the end of shutdown.
    // The budget is what the sweep spends waiting on a drain that cannot
    // finish, so it is also this test's runtime. Teardown itself is not bounded
    // by it — which is why the latch still closes with nothing left over.
    node.velo
        .graceful_shutdown(ShutdownPolicy::Timeout(Duration::from_millis(500)))
        .await;
    assert!(
        watch.is_deregistered(),
        "shutdown returned without declaring the region released, so this test \
         never reached the state it is about"
    );

    let err = node
        .velo
        .get(handle)
        .await
        .expect_err("reading an anchor whose region is gone must fail");
    assert!(
        err.to_string().contains("slot vanished"),
        "expected the read to refuse; got: {err}"
    );

    drop(guard);
}

/// `register_data_in_region` refuses a range it cannot honour, and refuses it
/// *before* taking an in-flight guard nothing would ever release.
#[tokio::test(flavor = "multi_thread")]
async fn register_data_in_region_checks_its_range() {
    let node = Node::start(None).await;
    let guard = node
        .velo
        .register_owned(vec![7u8; 64 * 1024].into_boxed_slice())
        .await
        .map_err(|e| e.cause)
        .expect("register");

    for range in [
        0..0,                 // empty at the start
        1024..1024,           // empty, elsewhere
        64 * 1024..65 * 1024, // wholly past the end
        60 * 1024..70 * 1024, // straddling the end
        0..u64::MAX,          // absurd
        // Inverted, built rather than written so clippy does not object to a
        // literal empty range. Refused by the same subtraction check that
        // catches the empty ones, not by a panic.
        std::ops::Range {
            start: 4096,
            end: 1024,
        },
    ] {
        assert_eq!(
            node.velo
                .register_data_in_region(&guard, range.clone())
                .err(),
            Some(RdmaError::OutOfRange),
            "range {range:?} should have been refused"
        );
    }

    // Nothing was staged, so nothing holds the region open.
    assert_eq!(
        guard.unregister(T).await.expect("unregister"),
        Deregistered::Drained,
        "a refused staging must not have taken an in-flight guard"
    );
    node.velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
}

// ---------------------------------------------------------------------------
// 3. Every reason the owner declines
// ---------------------------------------------------------------------------

/// A heap-staged slot is served chunked however capable the consumer is, and
/// the owner says why.
#[tokio::test(flavor = "multi_thread")]
async fn a_heap_staged_slot_is_answered_chunked() {
    let pair = Pair::new().await;

    let payload = pattern(1024 * 1024);
    let handle = pair.owner.velo.register_data(Bytes::from(payload.clone()));
    assert!(!pair.consumer.velo.metadata(handle).await.unwrap().pinned);

    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();

    assert_eq!(pair.owner.path_count("not_pinned"), 1);
    assert_eq!(pair.owner.path_count("ok"), 0);
    assert_eq!(pair.consumer.path_count("ok"), 0);
    shutdown(pair).await;
}

/// A pinned slot under `rdma_min_bytes` is served chunked: at that size the
/// GET replaces a single `_rv_pull` and does not pay for itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_slot_below_the_threshold_is_answered_chunked() {
    let pair = Pair::new().await;

    let payload = pattern(8 * 1024); // well under the 64 KiB default
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    assert!(
        pair.consumer.velo.metadata(handle).await.unwrap().pinned,
        "the owner should still have pinned it — the threshold is a transfer \
         decision, not a staging one"
    );

    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();

    assert_eq!(pair.owner.path_count("below_min"), 1);
    assert_eq!(pair.owner.path_count("ok"), 0);
    shutdown(pair).await;
}

/// Either side's kill switch alone takes the path out, which is what makes a
/// rollout reversible one node at a time.
#[tokio::test(flavor = "multi_thread")]
async fn either_kill_switch_alone_forces_the_chunked_path() {
    let off = || RdmaConfig {
        rendezvous: RdmaRendezvousConfig {
            enabled: false,
            ..RdmaRendezvousConfig::default()
        },
        ..RdmaConfig::default()
    };

    // Owner switched off: it has a pinned slot and a willing consumer, and
    // still answers chunked.
    let pair = Pair::with_configs(Some(off()), None).await;
    let payload = pattern(256 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    assert!(
        !pair.consumer.velo.metadata(handle).await.unwrap().pinned,
        "a switched-off owner should not spend pinned memory on a slot only the \
         chunked path will serve"
    );
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();
    // Two decisions, both recorded: the staging refused to spend pinned memory
    // on a slot only the chunked path would serve, and the acquire refused to
    // answer with a descriptor. Counting them separately is the point — an
    // operator reading the series wants to see the switch acting at both, not
    // that something somewhere declined once.
    assert_eq!(
        pair.owner.path_count("kill_switch"),
        2,
        "expected the staging decision and the acquire decision"
    );
    assert_eq!(pair.owner.path_count("ok"), 0);
    assert_eq!(pair.consumer.path_count("ok"), 0);
    shutdown(pair).await;

    // Consumer switched off: the owner has a pinned slot and would serve it,
    // but the acquire carries no offer.
    let pair = Pair::with_configs(None, Some(off())).await;
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    assert!(pair.consumer.velo.metadata(handle).await.unwrap().pinned);
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();
    assert_eq!(pair.consumer.path_count("kill_switch"), 1);
    assert_eq!(pair.owner.path_count("no_offer"), 1);
    assert_eq!(pair.owner.path_count("ok"), 0);
    shutdown(pair).await;
}

/// A pinned slot answers an acquire that has no `rdma` key at all — the shape
/// a consumer built before this phase puts on the wire.
///
/// Hand-crafted JSON rather than a struct with the field set to `None`: the
/// claim being tested is that a *missing* field defaults to "no offer", and a
/// struct that serialises `"rdma":null` would not test it.
#[tokio::test(flavor = "multi_thread")]
async fn an_acquire_without_the_offer_field_is_answered_chunked() {
    let pair = Pair::new().await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    assert!(pair.consumer.velo.metadata(handle).await.unwrap().pinned);

    let raw = handle.as_u128();
    let old_wire = format!(
        r#"{{"handle":{{"hi":{},"lo":{}}}}}"#,
        (raw >> 64) as u64,
        raw as u64
    );
    assert!(!old_wire.contains("rdma"), "the point of the test");

    // The streaming builder rather than `Velo::unary`: the public convenience
    // path refuses underscore-prefixed system handlers, and this test has to
    // put bytes on the wire that no current velo client would compose.
    let bytes: Bytes = pair
        .consumer
        .velo
        .messenger()
        .unary_streaming("_rv_acquire")
        .raw_payload(Bytes::from(old_wire))
        .instance(pair.owner.velo.instance_id())
        .send()
        .await
        .expect("an old-shaped acquire must still be answered");
    let response: AcquireResponse =
        serde_json::from_slice(&bytes).expect("the response is still an AcquireResponse");

    let lease = match response {
        AcquireResponse::Ready {
            lease_id,
            total_len,
            ..
        } => {
            assert_eq!(total_len, payload.len() as u64);
            lease_id
        }
        AcquireResponse::Rdma { .. } => {
            panic!("an acquire carrying no offer must never be answered with a descriptor")
        }
    };
    assert_eq!(pair.owner.path_count("no_offer"), 1);
    assert_eq!(pair.owner.path_count("ok"), 0);

    pair.consumer.velo.detach(handle, lease).await.unwrap();
    shutdown(pair).await;
}

/// A sub-millisecond lease timeout never puts `0` on the wire.
///
/// Zero is the "no deadline" encoding, so an owner that armed a deadline and
/// then reported zero would get a consumer that starts no renewal ticker — and
/// a reaper that force-releases the lease, and frees the pool slice, while the
/// peer's NIC is still reading from it. Silent wrong data out of a config field
/// nothing validated.
///
/// Read off the wire rather than inferred, because the wire value is the whole
/// claim: the owner's deadline and the milliseconds it reports must come from
/// one number.
#[tokio::test(flavor = "multi_thread")]
async fn a_sub_millisecond_lease_timeout_is_clamped_before_it_reaches_the_wire() {
    let pair = Pair::with_configs(
        Some(RdmaConfig {
            rendezvous: RdmaRendezvousConfig {
                lease_timeout: Duration::from_micros(300),
                ..RdmaRendezvousConfig::default()
            },
            ..RdmaConfig::default()
        }),
        None,
    )
    .await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;

    let raw = handle.as_u128();
    let acquire = format!(
        r#"{{"handle":{{"hi":{},"lo":{}}},"rdma":{{"backends":["ucx"]}}}}"#,
        (raw >> 64) as u64,
        raw as u64
    );
    let bytes: Bytes = pair
        .consumer
        .velo
        .messenger()
        .unary_streaming("_rv_acquire")
        .raw_payload(Bytes::from(acquire))
        .instance(pair.owner.velo.instance_id())
        .send()
        .await
        .expect("acquire");
    let response: AcquireResponse = serde_json::from_slice(&bytes).expect("AcquireResponse");

    match response {
        AcquireResponse::Rdma {
            lease_id,
            lease_timeout_ms,
            ..
        } => {
            assert!(
                lease_timeout_ms >= 1,
                "a deadline the consumer is told is absent means no renewal ticker, and a \
                 reaper that frees the source under a live GET"
            );
            pair.consumer.velo.detach(handle, lease_id).await.unwrap();
        }
        AcquireResponse::Ready { .. } => panic!("expected the RDMA path for a pinned slot"),
    }

    // And the transfer still works under the clamped config.
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    let _ = pair.consumer.velo.release(handle, lease).await;

    shutdown(pair).await;
}

// ---------------------------------------------------------------------------
// 4. The fallbacks
// ---------------------------------------------------------------------------

/// A descriptor the consumer cannot account for byte-for-byte is not used.
///
/// Each malformation is a separate acquire, so the counts say which one was
/// seen. The owner's side of the story is the same every time: it answered
/// `ok`, then answered the no-offer re-acquire.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_descriptor_falls_back_to_chunked() {
    let pair = Pair::new().await;
    let payload = pattern(512 * 1024);

    for (n, hook) in [
        RdmaTestHook::UnknownBackend,
        RdmaTestHook::TruncateDescriptor,
        RdmaTestHook::TrailingByte,
        RdmaTestHook::LyingKeyLength,
    ]
    .into_iter()
    .enumerate()
    {
        let handle = pair.owner.velo.register_data_pinned(&payload).await;
        pair.consumer
            .velo
            .rendezvous_manager()
            .arm_rdma_hook(hook.clone());

        let (data, lease) = pair
            .consumer
            .velo
            .get(handle)
            .await
            .unwrap_or_else(|e| panic!("{hook:?} must fall back, not fail: {e}"));
        assert_pattern(&data, payload.len());
        pair.consumer.velo.release(handle, lease).await.unwrap();

        let seen = n as u64 + 1;
        assert_eq!(
            pair.consumer.path_count("decode_error"),
            seen,
            "{hook:?} was not recorded as a decode failure"
        );
        assert_eq!(
            pair.consumer.path_count("ok"),
            0,
            "{hook:?} should not have completed an RDMA transfer"
        );
        assert_eq!(
            pair.owner.path_count("ok"),
            seen,
            "{hook:?}: the owner did answer with a descriptor"
        );
        assert_eq!(
            pair.owner.path_count("no_offer"),
            seen,
            "{hook:?}: the fallback re-acquire must carry no offer, exactly once"
        );
    }

    shutdown(pair).await;
}

/// A failed GET detaches, re-acquires chunked, and does that **once**.
///
/// The retry bound is the assertion that matters: the fallback acquire carries
/// no offer, so a second descriptor cannot arrive, and the owner's counts prove
/// exactly two acquires happened rather than a loop.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_get_falls_back_chunked_exactly_once() {
    let pair = Pair::new().await;

    let payload = pattern(768 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    pair.consumer
        .velo
        .rendezvous_manager()
        .arm_rdma_hook(RdmaTestHook::FailGet);

    let (data, lease) = pair
        .consumer
        .velo
        .get(handle)
        .await
        .expect("a failed GET must fall back, not surface");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();

    assert_eq!(pair.consumer.path_count("get_failed"), 1);
    assert_eq!(pair.consumer.path_count("ok"), 0);
    assert_eq!(
        pair.owner.path_count("ok"),
        1,
        "the owner answered exactly one descriptor"
    );
    assert_eq!(
        pair.owner.path_count("no_offer"),
        1,
        "exactly one no-offer re-acquire: more would be a retry loop"
    );

    // And the next transfer is unaffected — the hook was one-shot.
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer.velo.release(handle, lease).await.unwrap();
    assert_eq!(pair.consumer.path_count("ok"), 1);
    assert_eq!(pair.consumer.path_count("get_failed"), 1);

    shutdown(pair).await;
}

/// Pool pressure stages in plain memory rather than failing the staging call.
#[tokio::test(flavor = "multi_thread")]
async fn a_spent_budget_stages_in_plain_memory() {
    let node = Node::start(Some(RdmaConfig {
        pool: RdmaPoolConfig {
            // Smaller than a single arena, so the very first pooled staging is
            // refused by the budget rather than by fragmentation.
            registered_bytes_budget: 4096,
            ..RdmaPoolConfig::default()
        },
        ..RdmaConfig::default()
    }))
    .await;

    let payload = pattern(512 * 1024);
    let handle = node.velo.register_data_pinned(&payload).await;

    assert!(
        !node.velo.metadata(handle).await.unwrap().pinned,
        "a refused pool allocation must stage in plain memory"
    );
    assert_eq!(node.path_count("budget"), 1);
    assert_eq!(node.velo.rdma_registered_bytes(), 0);

    // And the data is still readable, which is the whole point of the fallback.
    let (data, lease) = node.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    node.velo.release(handle, lease).await.unwrap();

    node.velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
}

/// A `get` that fails after taking the lease still gives the lease back.
///
/// The failure here is the *documented* one: `get_pinned` needs a registry to
/// allocate a destination from, and an instance without a UCX transport answers
/// `NotConfigured`. Before the lease guard that meant every single call on such
/// an instance stranded a read lock — and a local lease carries no deadline, so
/// the reaper could never reclaim it and the slot was immortal.
///
/// The assertion is indirect on purpose: a leaked read lock is invisible in
/// metadata, and shows up only as a slot that a full release cannot free. That
/// is exactly the shape of the bug, so it is the shape of the test.
#[tokio::test(flavor = "multi_thread")]
async fn a_get_that_fails_after_taking_the_lease_returns_it() {
    let node = Node::start_without_rdma().await;

    let payload = pattern(8 * 1024);
    let handle = node.velo.register_data(Bytes::from(payload.clone()));

    for _ in 0..3 {
        let err = node
            .velo
            .get_pinned(handle)
            .await
            .expect_err("no registry, so there is nowhere to put the bytes");
        assert!(
            err.to_string().contains("no rdma backend configured"),
            "expected NotConfigured; got: {err}"
        );
    }

    // One full get + release takes the last reference and the last read lock.
    // A lease stranded above would still be holding one, and the slot would
    // survive.
    let (data, lease) = node.velo.get(handle).await.expect("get");
    assert_eq!(&data[..], &payload[..]);
    node.velo.release(handle, lease).await.expect("release");
    assert!(
        node.velo.metadata(handle).await.is_err(),
        "a failed get_pinned stranded its read lock, so the slot cannot be freed"
    );

    node.velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
}

/// The same, for the lease the *fallback* takes out.
///
/// A failed GET detaches its lease and re-acquires a fresh one; the copy into
/// the caller's destination happens after that, and a destination too small to
/// take the payload fails there. The fresh lease is the one at risk, and it is
/// as deadline-free as any other chunked lease.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_after_the_fallback_returns_its_fresh_lease() {
    let pair = Pair::new().await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    pair.consumer
        .velo
        .rendezvous_manager()
        .arm_rdma_hook(RdmaTestHook::FailGet);

    let mut too_small = [0u8; 64];
    let mut dest: &mut [u8] = &mut too_small;
    let err = pair
        .consumer
        .velo
        .get_into(handle, &mut dest)
        .await
        .expect_err("a 64-byte destination cannot take 512 KiB");
    assert!(
        err.to_string().contains("out of bounds"),
        "expected the destination to refuse; got: {err}"
    );
    assert_eq!(
        pair.consumer.path_count("get_failed"),
        1,
        "the fallback should have run before the write failed"
    );

    // The owner must be holding nothing: one full get + release frees the slot.
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    pair.consumer
        .velo
        .release(handle, lease)
        .await
        .expect("release");
    wait_until(
        "the slot is freed, so no lease was stranded by the failed write",
        || slot_is_gone(&pair.owner, handle),
        || "the slot is still staged, so a read lock outlived its get".to_string(),
    )
    .await;

    shutdown(pair).await;
}

// ---------------------------------------------------------------------------
// 5. Leases: the reaper and the keepalive
// ---------------------------------------------------------------------------

/// A consumer that takes an RDMA lease and never comes back has its lease
/// force-released, and the slot goes with it.
///
/// This is the leak PR #40 shipped: the owner cannot see an RDMA GET finish, so
/// without a deadline a crashed consumer's read lock *and* its reference are
/// held forever and the slot is immortal.
#[tokio::test(flavor = "multi_thread")]
async fn the_reaper_force_releases_an_abandoned_lease() {
    let pair = Pair::with_configs(
        Some(RdmaConfig {
            rendezvous: RdmaRendezvousConfig {
                lease_timeout: Duration::from_millis(300),
                ..RdmaRendezvousConfig::default()
            },
            ..RdmaConfig::default()
        }),
        None,
    )
    .await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;

    let (data, _abandoned_lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert_pattern(&data, payload.len());
    assert_eq!(
        pair.consumer.path_count("ok"),
        1,
        "this must be an RDMA lease"
    );

    wait_until(
        "the abandoned lease is reaped and its slot freed",
        || slot_is_gone(&pair.owner, handle),
        || {
            format!(
                "slot still staged; reaped={}",
                pair.owner
                    .counter("velo_rendezvous_rdma_leases_reaped_total")
            )
        },
    )
    .await;

    assert!(
        pair.owner
            .counter("velo_rendezvous_rdma_leases_reaped_total")
            >= 1,
        "the slot went away without the reaper being credited for it"
    );
    // The reaper's tick is also what samples the live-region gauge, so a run
    // that reaped something has necessarily published one.
    assert!(
        pair.owner.gauge("velo_rdma_live_regions") >= 1,
        "the reaper tick did not sample the backend's region count"
    );
    shutdown(pair).await;
}

/// A transfer that outlives its lease deadline several times over survives,
/// because the consumer renews while it runs.
///
/// The delay is injected: over `UCX_TLS=tcp` on loopback there is no honest way
/// to make a GET slow, and the condition the ticker exists for is precisely a
/// transfer that takes longer than half a deadline.
#[tokio::test(flavor = "multi_thread")]
async fn lease_renewal_carries_a_slow_transfer_past_several_deadlines() {
    let pair = Pair::with_configs(
        Some(RdmaConfig {
            rendezvous: RdmaRendezvousConfig {
                lease_timeout: Duration::from_millis(400),
                ..RdmaRendezvousConfig::default()
            },
            ..RdmaConfig::default()
        }),
        None,
    )
    .await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    pair.consumer
        .velo
        .rendezvous_manager()
        .arm_rdma_hook(RdmaTestHook::SlowGet(Duration::from_millis(1_400)));

    let started = std::time::Instant::now();
    let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
    assert!(
        started.elapsed() >= Duration::from_millis(1_400),
        "the delay was not applied, so nothing was tested"
    );
    assert_pattern(&data, payload.len());
    assert_eq!(
        pair.consumer.path_count("ok"),
        1,
        "the slow transfer should have completed on the RDMA path"
    );
    assert_eq!(
        pair.owner
            .counter("velo_rendezvous_rdma_leases_reaped_total"),
        0,
        "a renewed lease must not be reaped, however long the transfer takes"
    );
    assert!(
        pair.owner.velo.metadata(handle).await.is_ok(),
        "the slot was freed under a live transfer"
    );

    pair.consumer.velo.release(handle, lease).await.unwrap();
    shutdown(pair).await;
}

// ---------------------------------------------------------------------------
// 6. Zero-copy destinations
// ---------------------------------------------------------------------------

/// `get_pinned` hands back the registered buffer the NIC wrote into.
#[tokio::test(flavor = "multi_thread")]
async fn get_pinned_returns_the_bytes_without_a_copy_out() {
    let pair = Pair::new().await;

    let payload = pattern(512 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;

    let (buf, lease) = pair
        .consumer
        .velo
        .get_pinned(handle)
        .await
        .expect("get_pinned");
    assert_eq!(buf.len(), payload.len());
    assert_eq!(&buf[..], &payload[..]);
    assert_eq!(pair.consumer.path_count("ok"), 1);

    pair.consumer.velo.release(handle, lease).await.unwrap();
    drop(buf);
    shutdown(pair).await;
}

/// `get_into` a [`PinnedWriter`] lands the transfer in the caller's own
/// registered memory, with no copy at any point.
#[tokio::test(flavor = "multi_thread")]
async fn get_into_a_pinned_writer_takes_the_zero_copy_path() {
    let pair = Pair::new().await;

    let payload = pattern(768 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;

    let mut dest = pair
        .consumer
        .velo
        .alloc_pinned_writer(payload.len())
        .await
        .expect("a registered destination");
    let lease = pair
        .consumer
        .velo
        .get_into(handle, &mut dest)
        .await
        .expect("get_into");

    assert_eq!(dest.as_slice(), &payload[..]);
    assert_eq!(pair.consumer.path_count("ok"), 1);

    // An ordinary destination still works, taking the copy-once branch. Sized
    // at zero on purpose: `write_chunk` is what decides whether a destination
    // can take the bytes, and a growable one answers by resizing — a
    // `capacity()` check here would have sent this down the chunked fallback
    // over a number that says nothing about whether the write would succeed.
    let mut plain: Vec<u8> = Vec::new();
    let lease2 = pair
        .consumer
        .velo
        .get_into(handle, &mut plain)
        .await
        .expect("get_into a Vec");
    assert_eq!(plain, payload);
    assert_eq!(
        pair.consumer.path_count("ok"),
        2,
        "an ordinary destination should still ride the RDMA path, with one copy"
    );

    pair.consumer.velo.detach(handle, lease).await.unwrap();
    pair.consumer.velo.release(handle, lease2).await.unwrap();
    shutdown(pair).await;
}

/// Dropping a pooled buffer really does return its space.
///
/// Sized so the arena has room for exactly one of these at a time and the
/// budget has room for exactly one arena: a second live allocation has to grow
/// the pool, which the budget refuses. The same allocation after a drop must
/// therefore come out of the space the first one gave back — nothing else could
/// satisfy it.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_pinned_buffer_returns_its_pool_space() {
    const ARENA: u64 = 1 << 20;
    const CUT: usize = 768 * 1024;

    let node = Node::start(Some(RdmaConfig {
        pool: RdmaPoolConfig {
            initial_arena_bytes: ARENA,
            max_arena_bytes: ARENA,
            dedicated_arena_min: 64 << 20,
            // Room for one arena and not two, with slack for the page rounding
            // the backend may add to the first.
            registered_bytes_budget: ARENA + (ARENA / 2),
        },
        ..RdmaConfig::default()
    }))
    .await;

    let first = node
        .velo
        .alloc_pinned_writer(CUT)
        .await
        .expect("the first allocation maps an arena and fits inside it");
    assert!(node.velo.rdma_registered_bytes() >= ARENA);

    let refused = node.velo.alloc_pinned_writer(CUT).await;
    assert!(
        matches!(refused, Err(RdmaError::BudgetExceeded { .. })),
        "a second live allocation needs a second arena, which the budget must \
         refuse; got {refused:?}"
    );

    drop(first);
    node.velo
        .alloc_pinned_writer(CUT)
        .await
        .expect("the space the dropped buffer gave back must be reusable");

    node.velo
        .graceful_shutdown(ShutdownPolicy::Timeout(T))
        .await;
}

// ---------------------------------------------------------------------------
// 7. Transparent mode
// ---------------------------------------------------------------------------

/// A transparently staged payload rides the RDMA path once the pool is warm.
///
/// The warm-up is the honest part: the transparent stager runs inside the
/// messenger's synchronous send and never maps an arena, so a process whose
/// only staging is transparent stays on the chunked path. One explicit
/// `register_data_pinned` is what maps the arena the stager then cuts from.
#[tokio::test(flavor = "multi_thread")]
async fn transparent_staging_rides_the_rdma_path_once_the_pool_is_warm() {
    let pair = Pair::new().await;

    // `pair.owner` hosts the handler; `pair.consumer` sends, and is therefore
    // the *rendezvous owner* of the transparently staged payload.
    let handler = Handler::unary_handler("len", |ctx: Context| {
        Ok(Some(Bytes::from(ctx.payload.len().to_string())))
    })
    .build();
    pair.owner.velo.register_handler(handler).unwrap();
    tokio::time::timeout(
        T,
        pair.consumer
            .velo
            .wait_for_handler(pair.owner.velo.instance_id(), "len"),
    )
    .await
    .expect("handler list")
    .expect("wait_for_handler");

    // Warm the sender's pool. Without this the synchronous stager has no mapped
    // arena to cut from and stages in plain memory — which is correct, and is
    // why this line is here rather than hidden in the harness.
    let warm = pair
        .consumer
        .velo
        .register_data_pinned(&pattern(4096))
        .await;
    assert!(pair.consumer.velo.metadata(warm).await.unwrap().pinned);

    let size = 512 * 1024; // over the 256 KiB transparent threshold
    let payload = Bytes::from(pattern(size));
    let response: Bytes = pair
        .consumer
        .velo
        .unary("len")
        .unwrap()
        .raw_payload(payload)
        .instance(pair.owner.velo.instance_id())
        .send()
        .await
        .expect("unary");
    assert_eq!(response, Bytes::from(size.to_string()));

    assert_eq!(
        pair.consumer.path_count("ok"),
        1,
        "the sender staged the payload in plain memory instead of the pool"
    );
    assert_eq!(
        pair.owner.path_count("ok"),
        1,
        "the receiver resolved the payload over the chunked path"
    );
    shutdown(pair).await;
}
