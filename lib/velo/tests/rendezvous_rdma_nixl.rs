// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The rendezvous RDMA path over the NIXL backend, end to end.
//!
//! Two velo instances in one process, with TCP for control and NIXL for the
//! payload. Every scenario runs twice:
//!
//! - On the **loopback agent** (`VeloBuilder::add_nixl_rdma_loopback`), always.
//!   It needs no NIXL library, so this is what CI runs. It follows the NIXL
//!   behaviour that the backend's rules exist for.
//! - On a **real NIXL agent** (`VeloBuilder::add_nixl_rdma`), when
//!   `libnixl_capi.so` loads. Without the library that run is skipped, and the
//!   test prints that it was. Set `VELO_REQUIRE_NIXL=1` to make a skip a
//!   failure: that is how a machine with NIXL proves that it ran the real
//!   thing.
//!
//! ```bash
//! L=<nixl wheel>/.nixl_cu12.mesonpy.libs
//! VELO_REQUIRE_NIXL=1 LD_LIBRARY_PATH=$L NIXL_PLUGIN_DIR=$L/plugins UCX_TLS=tcp \
//!   cargo test --features nixl,test-helpers --test rendezvous_rdma_nixl
//! ```
//!
//! On a host with NVIDIA GPUs, NIXL refuses a `UCX_TLS` without CUDA support:
//! use `UCX_TLS=tcp,cuda_copy` there.
//!
//! As in `rendezvous_rdma.rs`, the assertions are on
//! `velo_rendezvous_rdma_path_total`, not on timing: the chunked path returns
//! the same bytes, so only the counter says which path ran.

#![cfg(all(target_os = "linux", feature = "nixl", feature = "test-helpers"))]

use std::sync::Arc;
use std::time::Duration;

use prometheus::Registry;
use velo::observability::VeloMetrics;
use velo::rendezvous::rdma::NixlLoopback;
use velo::*;

/// Generous ceiling for anything that should resolve promptly.
const T: Duration = Duration::from_secs(20);

struct Node {
    velo: Velo,
    registry: Registry,
}

impl Node {
    /// On the loopback network `net`, or on a real NIXL agent with `None`.
    async fn start(net: Option<&NixlLoopback>) -> anyhow::Result<Self> {
        let tcp = Arc::new(
            velo::transports::tcp::TcpTransportBuilder::new()
                .from_listener(std::net::TcpListener::bind("127.0.0.1:0")?)?
                .build()?,
        );
        let registry = Registry::new();
        let metrics = Arc::new(VeloMetrics::register(&registry)?);
        let builder = Velo::builder().metrics(metrics).add_transport(tcp);
        let nixl = NixlRdmaConfig {
            num_workers: 2,
            ..NixlRdmaConfig::default()
        };
        let builder = match net {
            Some(net) => builder.add_nixl_rdma_loopback(net.clone(), nixl),
            None => builder.add_nixl_rdma(nixl),
        };
        Ok(Self {
            velo: builder.build().await?,
            registry,
        })
    }

    fn path_count(&self, reason: &str) -> u64 {
        self.registry
            .gather()
            .iter()
            .filter(|family| family.name() == "velo_rendezvous_rdma_path_total")
            .flat_map(|family| family.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.name() == "reason" && l.value() == reason)
            })
            .map(|m| m.get_counter().value() as u64)
            .sum()
    }
}

struct Pair {
    /// The loopback network. `None` on a real NIXL agent.
    net: Option<NixlLoopback>,
    owner: Node,
    consumer: Node,
}

impl Pair {
    async fn start(net: Option<NixlLoopback>) -> anyhow::Result<Self> {
        let owner = Node::start(net.as_ref()).await?;
        let consumer = Node::start(net.as_ref()).await?;
        connect(&owner, &consumer).await;
        Ok(Self {
            net,
            owner,
            consumer,
        })
    }

    async fn shutdown(self) {
        self.consumer
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(T))
            .await;
        self.owner
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(T))
            .await;
    }
}

async fn connect(owner: &Node, consumer: &Node) {
    consumer
        .velo
        .register_peer(owner.velo.peer_info())
        .expect("consumer registers owner");
    owner
        .velo
        .register_peer(consumer.velo.peer_info())
        .expect("owner registers consumer");
    for (from, to) in [(consumer, owner), (owner, consumer)] {
        tokio::time::timeout(
            T,
            from.velo
                .wait_for_handler(to.velo.instance_id(), "_rv_lease_renew"),
        )
        .await
        .expect("the handler list did not arrive")
        .expect("wait_for_handler");
    }
}

/// One pair for each backend this machine can run.
///
/// The loopback pair always. The real pair when the library loads; when it
/// does not, the skip is printed, or is a failure under `VELO_REQUIRE_NIXL=1`.
/// Each pair is named as its turn starts, so the output of a failed assertion
/// says which agent it ran on.
async fn pairs() -> impl Iterator<Item = Pair> {
    let mut pairs = vec![
        Pair::start(Some(NixlLoopback::new()))
            .await
            .expect("a loopback pair always starts"),
    ];
    match Pair::start(None).await {
        Ok(pair) => pairs.push(pair),
        Err(e) => {
            let required = std::env::var("VELO_REQUIRE_NIXL").is_ok_and(|v| v == "1");
            assert!(
                !required,
                "VELO_REQUIRE_NIXL=1, and a real NIXL agent did not start: {e:#}"
            );
            eprintln!("skipping the real NIXL agent: {e:#}");
        }
    }
    pairs.into_iter().inspect(|pair| match pair.net {
        Some(_) => eprintln!("on the loopback agent"),
        None => eprintln!("on a real NIXL agent"),
    })
}

/// Every byte depends on its offset, with two coprime periods, so a transfer
/// that lands shifted or short does not pass by coincidence.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i % 251) ^ ((i / 251) % 257)) as u8)
        .collect()
}

/// A registered destination that refuses the chunked path, as a caller that
/// forbids a copy fallback writes it. If velo falls back, the test fails with
/// this error and not with a wrong counter.
struct RdmaOnly(PinnedWriter);

impl RendezvousWrite for RdmaOnly {
    fn capacity(&self) -> usize {
        self.0.len()
    }

    fn rdma_destination(&mut self) -> Option<RdmaDestination<'_>> {
        self.0.rdma_destination()
    }

    fn write_chunk(&mut self, _offset: usize, _data: &[u8]) -> anyhow::Result<()> {
        anyhow::bail!("velo used the chunked path for a transfer that must be RDMA")
    }
}

/// A pinned slot and an offering consumer take the RDMA path over NIXL, at the
/// threshold, well past it, and at a size that is no multiple of anything.
#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_payload_takes_the_rdma_path() {
    for pair in pairs().await {
        for (round, len) in [64 * 1024, 1 << 20, 300 * 1024 + 7].into_iter().enumerate() {
            let payload = pattern(len);
            let handle = pair.owner.velo.register_data_pinned(&payload).await;
            assert!(
                pair.consumer.velo.metadata(handle).await.unwrap().pinned,
                "the slot was not staged in registered memory"
            );
            let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
            assert!(data == payload, "{len} bytes arrived wrong");
            pair.consumer.velo.release(handle, lease).await.unwrap();

            let expected = round as u64 + 1;
            assert_eq!(pair.consumer.path_count("ok"), expected);
            assert_eq!(pair.owner.path_count("ok"), expected);
        }
        assert_eq!(pair.consumer.path_count("get_failed"), 0);
        pair.shutdown().await;
    }
}

/// One registered buffer for each payload, read straight into registered
/// memory: no copy on either side, and no chunked fallback allowed.
///
/// This is how a caller moves a large object it already holds (a KV cache,
/// say). It is also the hard case for NIXL's key cache: each payload is a new
/// registration, and the allocator hands the same address back. Each must be
/// read through its own registration, never through the entry of an earlier
/// one, which NIXL still holds and does not refuse.
#[tokio::test(flavor = "multi_thread")]
async fn a_registered_buffer_for_each_payload_is_read_with_no_copy() {
    for pair in pairs().await {
        const LEN: usize = 2 << 20;
        // A pool arena stays registered on the owner for the whole test. NIXL
        // answers a query about a region it has dropped in two ways, by
        // whether it holds anything else, and `unregister` below must read
        // both as "released". With nothing else registered only one of the
        // two is ever seen: the first adapter knew only that one, and failed
        // every unmap while an arena was registered.
        let staged = pair
            .owner
            .velo
            .register_data_pinned(&pattern(64 * 1024))
            .await;
        assert!(pair.consumer.velo.metadata(staged).await.unwrap().pinned);
        for round in 0u8..4 {
            let mut payload = pattern(LEN);
            payload[0] = round;
            let region = pair
                .owner
                .velo
                .register_owned(payload.clone().into_boxed_slice())
                .await
                .expect("register_owned");
            let handle = pair
                .owner
                .velo
                .register_data_in_region(&region, 0..LEN as u64)
                .expect("register_data_in_region");

            let writer = pair
                .consumer
                .velo
                .alloc_pinned_writer(LEN)
                .await
                .expect("a registered destination");
            let mut dest = RdmaOnly(writer);
            let lease = pair
                .consumer
                .velo
                .get_into(handle, &mut dest)
                .await
                .unwrap_or_else(|e| panic!("round {round}: {e:#}"));
            assert!(
                dest.0.as_slice() == &payload[..],
                "round {round}: wrong bytes"
            );
            pair.consumer.velo.release(handle, lease).await.unwrap();
            drop(dest);

            let released = tokio::time::timeout(T, region.unregister(T))
                .await
                .expect("unregister is bounded")
                .expect("unregister");
            assert_eq!(released, Deregistered::Drained, "round {round}");
        }
        assert_eq!(pair.consumer.path_count("ok"), 4);
        if let Some(net) = &pair.net {
            assert_eq!(
                net.stale_reads(),
                0,
                "a payload was read through an old entry"
            );
        }
        pair.shutdown().await;
    }
}

/// A heap slot is served by chunks, and says why.
#[tokio::test(flavor = "multi_thread")]
async fn a_heap_slot_takes_the_chunked_path() {
    for pair in pairs().await {
        let payload = pattern(700 * 1024);
        let handle = pair
            .owner
            .velo
            .register_data(bytes::Bytes::from(payload.clone()));
        let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
        assert!(data == payload);
        pair.consumer.velo.release(handle, lease).await.unwrap();

        assert_eq!(pair.owner.path_count("not_pinned"), 1);
        assert_eq!(pair.owner.path_count("ok"), 0);
        assert_eq!(pair.consumer.path_count("ok"), 0);
        pair.shutdown().await;
    }
}

/// Shutdown releases every registration, and says so.
///
/// The region here is never unregistered by its holder. `deregistered()` then
/// resolves only at the end of `graceful_shutdown`, and only if the backend
/// reports that it holds nothing: the registry asks before it frees a page.
/// The NIXL backend has a thread of its own, which no transport teardown
/// stops, so the answer depends on `RdmaBackend::teardown` running first.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_releases_every_registration() {
    for pair in pairs().await {
        let payload = pattern(1 << 20);
        let handle = pair.owner.velo.register_data_pinned(&payload).await;
        let (data, lease) = pair.consumer.velo.get(handle).await.expect("get");
        assert!(data == payload);
        pair.consumer.velo.release(handle, lease).await.unwrap();

        let region = pair
            .owner
            .velo
            .register_owned(vec![7u8; 64 * 1024].into_boxed_slice())
            .await
            .expect("register_owned");
        assert!(pair.owner.velo.rdma_registered_bytes() > 0);

        let Pair {
            net,
            owner,
            consumer,
        } = pair;
        consumer
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(T))
            .await;
        owner
            .velo
            .graceful_shutdown(ShutdownPolicy::Timeout(T))
            .await;

        tokio::time::timeout(T, region.deregistered())
            .await
            .unwrap_or_else(|_| panic!("the region was never declared released after shutdown"));
        assert_eq!(owner.velo.rdma_registered_bytes(), 0);
        assert_eq!(consumer.velo.rdma_registered_bytes(), 0);
        if let Some(net) = net {
            assert_eq!(net.registered_regions(), 0);
        }
    }
}

/// `unregister_peer` reaches the backend. A node that reads from many peers
/// over its life otherwise keeps the keys of all of them.
#[tokio::test(flavor = "multi_thread")]
async fn unregistering_a_peer_drops_its_state_in_the_backend() {
    let net = NixlLoopback::new();
    let pair = Pair::start(Some(net.clone()))
        .await
        .expect("a loopback pair");
    let payload = pattern(256 * 1024);
    let handle = pair.owner.velo.register_data_pinned(&payload).await;
    let (_, lease) = pair.consumer.velo.get(handle).await.expect("get");
    pair.consumer.velo.release(handle, lease).await.unwrap();
    assert_eq!(net.invalidates(), 0);

    pair.consumer
        .velo
        .unregister_peer(pair.owner.velo.instance_id());
    let deadline = tokio::time::Instant::now() + T;
    while net.invalidates() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the backend never dropped the peer"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    pair.shutdown().await;
}

/// Without the NIXL library, `build` returns an error that names the library,
/// and does not abort the process. This test runs where the library is
/// missing, which is CI: with the library present there is nothing to assert.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_library_fails_the_build_and_does_not_abort() {
    match Node::start(None).await {
        Err(e) => {
            let text = format!("{e:#}");
            assert!(
                text.contains("libnixl_capi.so"),
                "the error does not name the library: {text}"
            );
        }
        Ok(node) => {
            eprintln!("the NIXL library is present: nothing to assert");
            node.velo
                .graceful_shutdown(ShutdownPolicy::Timeout(T))
                .await;
        }
    }
}

#[cfg(feature = "ucx")]
mod with_ucx {
    use super::*;
    use velo::transports::ucx::UcxTransportBuilder;

    /// An instance has one RDMA backend. Two would need a rule for which one a
    /// slot is staged for, and the registry has one backend to ask.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_backends_on_one_instance_are_refused() {
        let ucx = Arc::new(
            UcxTransportBuilder::new()
                .tls("tcp")
                .build()
                .expect("build ucx transport"),
        );
        let built = Velo::builder()
            .add_ucx_transport(ucx)
            .add_nixl_rdma_loopback(NixlLoopback::new(), NixlRdmaConfig::default())
            .build()
            .await;
        let text = match built {
            Ok(_) => panic!("an instance was built with two RDMA backends"),
            Err(e) => format!("{e:#}"),
        };
        assert!(text.contains("one RDMA backend"), "{text}");
    }
}
