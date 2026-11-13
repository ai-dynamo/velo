// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The driver's rules, on the fake agent.
//!
//! Each test names a NIXL behaviour that was measured on libnixl 1.5.2 and the
//! rule the driver has because of it. The fake follows those behaviours
//! (`fake.rs`), so a rule that is removed fails its test here, where there is
//! no libnixl, and not only on a machine that has one.

use std::sync::Arc;
use std::time::Duration;

use velo_ext::InstanceId;

use super::backend::NixlBackend;
use super::driver::DriverConfig;
use super::fake::{FakeAgent, NixlLoopback};
use super::key::{NixlKey, agent_name};
use crate::rendezvous::rdma::backend::{BackendGet, BackendRegion, RdmaBackend, RdmaError};

/// Generous ceiling for anything that should resolve promptly.
const T: Duration = Duration::from_secs(10);
/// Long enough that a future which was going to resolve would have.
const SETTLE: Duration = Duration::from_millis(60);
/// Size of each buffer of a [`Rig`].
const LEN: usize = 16 * 1024;

struct Side {
    backend: Arc<NixlBackend>,
    instance: InstanceId,
}

fn config() -> DriverConfig {
    DriverConfig {
        spin: Duration::from_micros(50),
        teardown_timeout: Duration::from_secs(2),
        ..DriverConfig::default()
    }
}

async fn side_with(net: &NixlLoopback, cfg: DriverConfig) -> Side {
    let instance = InstanceId::new_v4();
    let name = agent_name(instance);
    let net = net.clone();
    let backend = NixlBackend::start(move || Ok(FakeAgent::new(&name, net)), cfg)
        .await
        .expect("start the fake backend");
    Side { backend, instance }
}

async fn side(net: &NixlLoopback) -> Side {
    side_with(net, config()).await
}

fn pattern(seed: u8) -> Box<[u8]> {
    (0..LEN)
        .map(|i| seed.wrapping_add((i % 251) as u8))
        .collect()
}

fn zeroed() -> Box<[u8]> {
    vec![0u8; LEN].into_boxed_slice()
}

/// Register `buf` with `side`. The pointer is taken mutably: a READ writes
/// through it.
async fn map(side: &Side, buf: &mut [u8]) -> BackendRegion {
    side.backend
        .map(buf.as_mut_ptr() as usize, buf.len())
        .await
        .expect("map")
}

/// A READ of `len` bytes at `src_off` of the owner's region `src` (which
/// covers `src_buf`), into `dst_off` of the local region `dst`.
fn read(
    owner: &Side,
    src: &BackendRegion,
    src_buf: &[u8],
    src_off: usize,
    dst: &BackendRegion,
    dst_off: usize,
    len: usize,
) -> BackendGet {
    BackendGet {
        peer: owner.instance,
        remote_addr: (src_buf.as_ptr() as usize + src_off) as u64,
        packed_key: src.packed_key.clone(),
        local_region_id: dst.backend_region_id,
        local_offset: dst_off as u64,
        len: len as u64,
    }
}

/// An owner with one registered buffer of patterned bytes, and a consumer with
/// one registered buffer of zeroes.
struct Rig {
    net: NixlLoopback,
    owner: Side,
    consumer: Side,
    src: Box<[u8]>,
    dst: Box<[u8]>,
    src_region: BackendRegion,
    dst_region: BackendRegion,
}

impl Rig {
    async fn new() -> Self {
        Self::with(config()).await
    }

    /// As [`new`](Self::new), with `cfg` for the consumer.
    async fn with(cfg: DriverConfig) -> Self {
        let net = NixlLoopback::new();
        let owner = side(&net).await;
        let consumer = side_with(&net, cfg).await;
        let (mut src, mut dst) = (pattern(4), zeroed());
        let src_region = map(&owner, &mut src).await;
        let dst_region = map(&consumer, &mut dst).await;
        Self {
            net,
            owner,
            consumer,
            src,
            dst,
            src_region,
            dst_region,
        }
    }

    fn read_at(&self, src_off: usize, dst_off: usize, len: usize) -> BackendGet {
        let (src, dst) = (&self.src_region, &self.dst_region);
        read(&self.owner, src, &self.src, src_off, dst, dst_off, len)
    }

    /// A READ of the whole source into the whole destination.
    fn read(&self) -> BackendGet {
        self.read_at(0, 0, LEN)
    }

    async fn get(&self) -> Result<(), RdmaError> {
        self.consumer.backend.get(self.read()).await
    }
}

async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + T;
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Whether `fut` is still pending after [`SETTLE`].
async fn still_pending<F: Future + Unpin>(fut: &mut F) -> bool {
    tokio::time::timeout(SETTLE, fut).await.is_err()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_moves_an_unaligned_sub_range() {
    let rig = Rig::new().await;
    let get = rig.read_at(4093, 17, 4097);
    rig.consumer.backend.get(get).await.expect("read");

    assert_eq!(&rig.dst[17..17 + 4097], &rig.src[4093..4093 + 4097]);
    assert!(rig.dst[..17].iter().all(|b| *b == 0), "wrote before");
    assert!(rig.dst[17 + 4097..].iter().all(|b| *b == 0), "wrote past");
}

/// A READ is checked against both regions before NIXL sees it.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_outside_either_region_is_refused() {
    let rig = Rig::new().await;
    let get = |req| rig.consumer.backend.get(req);

    // Past the end of the destination.
    assert_eq!(
        get(rig.read_at(0, 1, LEN)).await,
        Err(RdmaError::OutOfRange)
    );
    // Past the end of the owner's region: the key names the region, and the
    // range must lie inside it.
    assert_eq!(
        get(rig.read_at(LEN - 16, 0, 32)).await,
        Err(RdmaError::OutOfRange)
    );
    // A destination this side never registered.
    let mut unknown = rig.read_at(0, 0, 16);
    unknown.local_region_id = 9999;
    assert_eq!(get(unknown).await, Err(RdmaError::RegionNotFound));

    assert_eq!(rig.net.region_loads(), 0, "a refused read loaded something");
}

/// The owner deregisters a region and registers the same memory again. That is
/// ordinary: a pool arena that was reclaimed, or one buffer per payload.
///
/// NIXL keeps the consumer's entry for the old registration, and it does not
/// refuse a READ through it (measured: `Ok` over tcp; on InfiniBand a stale
/// key aborts the process). What keeps the old entry out of use is that each
/// registration has its own device id, and a READ names the id of the
/// registration the owner just described. So the new region loads beside the
/// old one, with no purge, and the READ goes through the new one.
///
/// The last step is the control: a READ that replays the **old** key does go
/// through the stale entry, and the fake counts it. Without that, a count of
/// zero above would prove nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_reused_address_is_not_read_through_its_old_registration() {
    let mut rig = Rig::new().await;
    let first = rig.src_region.clone();
    rig.get()
        .await
        .expect("read through the first registration");
    let unmapped = rig.owner.backend.unmap(first.backend_region_id).await;
    assert_eq!(unmapped, Ok(()));

    rig.src.fill(0x5a);
    rig.src_region = map(&rig.owner, &mut rig.src).await;
    assert_ne!(first.backend_region_id, rig.src_region.backend_region_id);
    assert_ne!(first.packed_key, rig.src_region.packed_key);
    rig.get()
        .await
        .expect("read through the second registration");

    assert!(rig.dst.iter().all(|b| *b == 0x5a));
    assert_eq!(rig.net.stale_reads(), 0, "read through the old entry");
    assert_eq!(rig.net.invalidates(), 0, "a reused address cost a purge");
    assert_eq!(rig.net.region_loads(), 2);

    // The control.
    rig.src_region = first;
    let _ = rig.get().await;
    assert_eq!(
        rig.net.stale_reads(),
        1,
        "the fake does not count a stale read"
    );
}

/// NIXL gives every READ posted from one thread to one of its workers, and the
/// driver is one thread. So each READ names a worker, in turn. Without that,
/// more workers buy nothing.
///
/// Loading is per region, not per READ: a load is a call into NIXL under its
/// lock, on the path of every transfer.
#[tokio::test(flavor = "multi_thread")]
async fn reads_go_round_the_workers_and_load_their_region_once() {
    let rig = Rig::with(DriverConfig {
        workers: 3,
        ..config()
    })
    .await;
    for _ in 0..6 {
        rig.get().await.expect("read");
    }
    assert_eq!(rig.net.posts_by_worker(), vec![2, 2, 2]);
    assert_eq!((rig.net.conn_loads(), rig.net.region_loads()), (1, 1));
}

/// With nothing in flight the driver blocks on its channel. NIXL has no fd to
/// wait on, so a driver that kept polling would cost a core for the life of
/// every instance, as NIXL's own progress thread does at a delay of zero
/// (measured: 100% of one core per agent).
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_driver_does_not_poll() {
    let rig = Rig::new().await;
    rig.get().await.expect("read");

    let polls = rig.net.polls();
    assert!(polls > 0, "the transfer was never polled");
    tokio::time::sleep(SETTLE).await;
    assert_eq!(rig.net.polls(), polls, "polled with nothing in flight");
}

/// A real agent finishes a short READ inside the post. Nothing is in flight
/// then: the driver does not poll, and the destination can go at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_that_finishes_in_the_post_is_not_in_flight() {
    let rig = Rig::new().await;
    rig.net.finish_at_post(true);
    assert_eq!(rig.get().await, Ok(()));
    assert_eq!(&rig.dst[..], &rig.src[..]);
    assert_eq!(rig.net.polls(), 0);

    let unmapped = rig.consumer.backend.unmap(rig.dst_region.backend_region_id);
    assert_eq!(tokio::time::timeout(T, unmapped).await.unwrap(), Ok(()));
    assert_eq!(rig.consumer.backend.live_registrations(), Some(0));
}

/// An unmap of a region that a READ is writing into waits for the READ. While
/// it waits no new READ may target the region, so the wait ends. A second
/// unmap joins the first.
///
/// The layer above frees the pages when `unmap` returns `Ok`. An unmap that
/// did not wait hands freed pages to a NIC that is still writing them.
#[tokio::test(flavor = "multi_thread")]
async fn an_unmap_waits_for_a_read_into_its_region() {
    let rig = Rig::new().await;
    let backend = &rig.consumer.backend;
    let id = rig.dst_region.backend_region_id;

    rig.net.hold_transfers(true);
    let get = rig.get();
    tokio::pin!(get);
    assert!(still_pending(&mut get).await);

    let (first, second) = (backend.unmap(id), backend.unmap(id));
    tokio::pin!(first, second);
    assert!(still_pending(&mut first).await, "unmapped under a read");
    assert!(still_pending(&mut second).await);
    assert_eq!(backend.live_registrations(), Some(1));

    // The region is on its way out: nothing new may write into it.
    assert_eq!(rig.get().await, Err(RdmaError::RegionNotFound));

    rig.net.hold_transfers(false);
    assert_eq!(tokio::time::timeout(T, &mut get).await.unwrap(), Ok(()));
    assert_eq!(tokio::time::timeout(T, &mut first).await.unwrap(), Ok(()));
    assert_eq!(tokio::time::timeout(T, &mut second).await.unwrap(), Ok(()));
    assert_eq!(&rig.dst[..], &rig.src[..]);
    assert_eq!(backend.live_registrations(), Some(0));
}

/// At the cap on loaded regions the peer is dropped and loaded again, and the
/// drop waits until nothing to that peer is in flight.
///
/// NIXL loses track of a transfer whose peer is dropped under it, and the
/// transfer writes on (measured: the last page of a 2 GiB destination changed
/// 1.6 s after the status said `NotFound`). So a READ that needs the drop
/// waits, and so does every READ behind it: one that went ahead would keep the
/// peer busy and the drop would never come.
#[tokio::test(flavor = "multi_thread")]
async fn a_purge_waits_for_the_reads_in_flight() {
    const PART: usize = 4096;
    let rig = Rig::with(DriverConfig {
        max_regions_per_peer: 1,
        ..config()
    })
    .await;
    let backend = &rig.consumer.backend;
    let mut other = pattern(9);
    let other_region = map(&rig.owner, &mut other).await;

    rig.net.hold_transfers(true);
    let first = backend.get(rig.read_at(0, 0, PART));
    // A second region at a cap of one: needs the purge.
    let (owner, dst) = (&rig.owner, &rig.dst_region);
    let second = backend.get(read(owner, &other_region, &other, 0, dst, PART, PART));
    // A loaded region, but behind a READ that waits for the purge.
    let third = backend.get(rig.read_at(0, 2 * PART, PART));
    tokio::pin!(first, second, third);
    assert!(still_pending(&mut first).await);
    assert!(still_pending(&mut second).await);
    assert!(still_pending(&mut third).await);
    assert_eq!(
        rig.net.invalidates(),
        0,
        "the peer was dropped under a read"
    );

    rig.net.hold_transfers(false);
    assert_eq!(tokio::time::timeout(T, &mut first).await.unwrap(), Ok(()));
    assert_eq!(tokio::time::timeout(T, &mut second).await.unwrap(), Ok(()));
    assert_eq!(tokio::time::timeout(T, &mut third).await.unwrap(), Ok(()));

    assert_eq!(&rig.dst[..PART], &rig.src[..PART]);
    assert_eq!(&rig.dst[PART..2 * PART], &other[..PART]);
    assert_eq!(&rig.dst[2 * PART..3 * PART], &rig.src[..PART]);
    // One purge to load the other region, and one more to load the first
    // again behind it.
    assert_eq!(rig.net.invalidates(), 2);
}

/// After a failed transfer NIXL refuses further loads on that peer's endpoint.
/// So the peer is dropped before the next load, and the next READ works.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_transfer_drops_the_peer_before_the_next_load() {
    let rig = Rig::new().await;
    rig.net.fail_next_transfer();
    let failed = rig.get().await;
    assert!(matches!(failed, Err(RdmaError::Backend(_))), "{failed:?}");
    assert_eq!(rig.net.invalidates(), 0);

    rig.get().await.expect("the read after a failed one");
    assert_eq!(rig.net.invalidates(), 1);
    assert_eq!(rig.net.conn_loads(), 2, "the peer was not loaded again");
    assert_eq!(&rig.dst[..], &rig.src[..]);
}

/// A blob that NIXL refuses can leave the peer unloadable for the life of the
/// agent (measured: 111 of 192 refused loads, and a drop of the peer brought
/// none of them back). The driver stops touching such a peer: READs from it
/// fail at once, with no further call into NIXL, and the consumer above takes
/// the chunked path.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_load_poisons_the_peer() {
    let rig = Rig::new().await;

    // A well-framed key whose region blob NIXL cannot parse.
    let good = NixlKey::decode(&rig.src_region.packed_key).expect("decode");
    let mut bad = rig.read();
    bad.packed_key = NixlKey {
        md: b"not a region blob",
        ..good
    }
    .encode(16 * 1024)
    .expect("encode");
    let refused = rig.consumer.backend.get(bad).await;
    assert!(matches!(refused, Err(RdmaError::Backend(_))), "{refused:?}");

    let loads = (rig.net.conn_loads(), rig.net.region_loads());
    let after = rig.get().await;
    assert!(
        matches!(&after, Err(RdmaError::Backend(detail)) if detail.contains("unusable")),
        "{after:?}"
    );
    assert_eq!(
        (rig.net.conn_loads(), rig.net.region_loads()),
        loads,
        "the driver called into the agent for a poisoned peer"
    );
    assert!(rig.dst.iter().all(|b| *b == 0));
}

/// A key's blobs must belong to the instance the READ is aimed at. NIXL files
/// what it loads under the name inside the blob, so a key that carries another
/// agent's blobs would put state under that agent's name.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_with_another_agents_blobs_is_refused() {
    let rig = Rig::new().await;
    let liar = side(&rig.net).await;
    // Aimed at `liar`, with a key made of the owner's blobs.
    let forged = || BackendGet {
        peer: liar.instance,
        ..rig.read()
    };

    let refused = rig.consumer.backend.get(forged()).await;
    assert_eq!(refused, Err(RdmaError::InvalidKey));
    assert!(rig.dst.iter().all(|b| *b == 0));

    // The instance that sent it is not asked again ...
    let again = rig.consumer.backend.get(forged()).await;
    assert!(matches!(again, Err(RdmaError::Backend(_))), "{again:?}");
    // ... and the agent whose blobs they were is unharmed.
    rig.get().await.expect("a read from the honest owner");
    assert_eq!(&rig.dst[..], &rig.src[..]);
}

/// A transfer that NIXL lost track of may still be writing. Its region is
/// never deregistered, the backend reports a registration that will not go
/// away, and the agent is not destroyed at teardown.
///
/// The count matters: the registry frees every page it still holds once the
/// backend reports zero registrations at the end of shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_transfer_keeps_its_region_registered_for_good() {
    let rig = Rig::new().await;
    let backend = &rig.consumer.backend;
    rig.net.lose_next_transfer();
    assert_eq!(rig.get().await, Err(RdmaError::Unconfirmed));

    let unmapped = backend.unmap(rig.dst_region.backend_region_id).await;
    assert!(
        matches!(unmapped, Err(RdmaError::Backend(_))),
        "{unmapped:?}"
    );
    // The region, and one for the transfer nobody can confirm.
    assert_eq!(backend.live_registrations(), Some(2));

    backend.teardown().await;
    assert_ne!(
        backend.live_registrations(),
        Some(0),
        "teardown declared a region released under a transfer that may still write it"
    );
    assert_eq!(rig.net.agents(), 2, "the agent was destroyed");
}

/// Teardown gives a READ a bounded time. One that is still running after it is
/// answered as unconfirmed, so the consumer leaks its destination, and it is
/// not released.
#[tokio::test(flavor = "multi_thread")]
async fn teardown_gives_up_a_read_that_does_not_finish() {
    let rig = Rig::with(DriverConfig {
        teardown_timeout: Duration::from_millis(50),
        ..config()
    })
    .await;
    let backend = &rig.consumer.backend;

    rig.net.hold_transfers(true);
    let get = rig.get();
    tokio::pin!(get);
    assert!(still_pending(&mut get).await);

    tokio::time::timeout(T, backend.teardown())
        .await
        .expect("teardown is bounded");
    assert_eq!(
        tokio::time::timeout(T, &mut get).await.unwrap(),
        Err(RdmaError::Unconfirmed)
    );
    assert_ne!(backend.live_registrations(), Some(0));
}

/// A teardown with nothing in flight releases what is left, destroys the
/// agent, and reports zero. After it every call is answered, and none hangs.
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_teardown_releases_everything_and_answers_what_follows() {
    // The consumer's region is left registered on purpose: teardown releases
    // it, as transport teardown does for a UCX region the sweep did not reach.
    let rig = Rig::new().await;
    let backend = &rig.consumer.backend;
    assert_eq!(rig.net.agents(), 2);

    tokio::time::timeout(T, backend.teardown())
        .await
        .expect("teardown");
    assert_eq!(backend.live_registrations(), Some(0));
    assert_eq!(rig.net.agents(), 1, "the agent was not destroyed");

    let mut later = zeroed();
    let mapped = backend.map(later.as_mut_ptr() as usize, later.len()).await;
    assert_eq!(mapped.map(|_| ()), Err(RdmaError::ShuttingDown));
    assert_eq!(rig.get().await, Err(RdmaError::ShuttingDown));
    assert_eq!(
        backend.unmap(rig.dst_region.backend_region_id).await,
        Ok(()),
        "a region that is gone is unmapped"
    );
    // A second teardown is answered too.
    tokio::time::timeout(T, backend.teardown())
        .await
        .expect("a second teardown");
}

/// A deregistration that NIXL does not confirm is not an unmap. The region
/// stays counted, and a later unmap can still succeed.
#[tokio::test(flavor = "multi_thread")]
async fn an_unconfirmed_deregistration_keeps_the_region_counted() {
    let rig = Rig::new().await;
    let (backend, id) = (&rig.owner.backend, rig.src_region.backend_region_id);

    rig.net.refuse_deregister(true);
    let refused = backend.unmap(id).await;
    assert!(matches!(refused, Err(RdmaError::Backend(_))), "{refused:?}");
    assert_eq!(backend.live_registrations(), Some(1));

    rig.net.refuse_deregister(false);
    assert_eq!(backend.unmap(id).await, Ok(()));
    assert_eq!(backend.live_registrations(), Some(0));
    // Idempotent.
    assert_eq!(backend.unmap(id).await, Ok(()));
}

/// The caller of a dropped `map` frees the pages. A registration that outlived
/// the drop would leave a NIC able to read freed memory.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_map_leaves_no_registration() {
    let net = NixlLoopback::new();
    let owner = side(&net).await;
    let mut src = pattern(4);

    {
        let map = owner.backend.map(src.as_mut_ptr() as usize, src.len());
        tokio::pin!(map);
        // Far enough for the command to be on its way, then dropped.
        let _ = futures::poll!(map.as_mut());
    }

    eventually("the registration is made and rolled back", || {
        net.registers() == 1 && net.registered_regions() == 0
    })
    .await;
    assert_eq!(owner.backend.live_registrations(), Some(0));
}

/// A key the descriptor cannot carry fails the `map`, where it is loud. If it
/// only failed to encode at acquire time, every transfer from the region would
/// fall back to the chunked path, and nothing would say why.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_over_the_bound_fails_the_map_and_registers_nothing() {
    let net = NixlLoopback::new();
    let cfg = DriverConfig {
        max_key_len: 40,
        ..config()
    };
    let owner = side_with(&net, cfg).await;
    let mut src = pattern(4);

    let mapped = owner
        .backend
        .map(src.as_mut_ptr() as usize, src.len())
        .await;
    assert!(
        matches!(&mapped, Err(RdmaError::Backend(detail)) if detail.contains("bound")),
        "{mapped:?}"
    );
    assert_eq!(net.registered_regions(), 0);
    assert_eq!(owner.backend.live_registrations(), Some(0));
}

/// The bindings panic on a blob whose agent name is not UTF-8, and the blob is
/// what a peer sent. A panic must not unwind into dropping the agent: that
/// would release transfers and registrations that are still in use. So the
/// driver catches it, leaks the agent, and the backend reports a registration
/// that will not go away.
///
/// Everybody who waits on the driver must still get an answer. The first
/// version leaked the driver's whole state to keep the agent alive, and the
/// reply channels went with it: a READ in flight to another region, and an
/// unmap parked behind it, waited for ever. Only the agent and the transfers
/// may be leaked.
#[tokio::test(flavor = "multi_thread")]
async fn a_panic_inside_the_agent_leaves_the_backend_stuck_and_answering() {
    let rig = Rig::new().await;
    let backend = &rig.consumer.backend;
    let (mut other, mut other_dst) = (pattern(9), zeroed());
    let other_region = map(&rig.owner, &mut other).await;
    let other_dst_region = map(&rig.consumer, &mut other_dst).await;

    // A READ in flight, and an unmap parked behind it.
    rig.net.hold_transfers(true);
    let in_flight = rig.get();
    let parked = backend.unmap(rig.dst_region.backend_region_id);
    tokio::pin!(in_flight, parked);
    assert!(still_pending(&mut in_flight).await);
    assert!(still_pending(&mut parked).await);

    // A READ of a region that is not loaded yet: its load panics.
    rig.net.panic_next_load();
    let (owner, dst) = (&rig.owner, &other_dst_region);
    let panicking = read(owner, &other_region, &other, 0, dst, 0, LEN);
    assert_eq!(backend.get(panicking).await, Err(RdmaError::Unconfirmed));

    assert_eq!(
        tokio::time::timeout(T, &mut in_flight)
            .await
            .expect("the read in flight was never answered"),
        Err(RdmaError::Unconfirmed)
    );
    let unmapped = tokio::time::timeout(T, &mut parked)
        .await
        .expect("the parked unmap was never answered");
    assert!(
        matches!(unmapped, Err(RdmaError::Backend(_))),
        "{unmapped:?}"
    );

    // Two regions, and one for the agent that cannot be released.
    eventually("the backend reports itself stuck", || {
        backend.live_registrations() == Some(3)
    })
    .await;
    assert_eq!(
        backend.unmap(other_dst_region.backend_region_id).await,
        Err(RdmaError::ShuttingDown),
        "a region the dead driver still holds is not unmapped"
    );
    let mut later = zeroed();
    let mapped = backend.map(later.as_mut_ptr() as usize, later.len()).await;
    assert_eq!(mapped.map(|_| ()), Err(RdmaError::ShuttingDown));
    tokio::time::timeout(T, backend.teardown())
        .await
        .expect("teardown of a stuck backend is answered");
    assert_eq!(rig.net.agents(), 2, "the agent was destroyed");
}

/// A forgotten peer is dropped, and the next READ loads it again. Without the
/// drop a long-lived node keeps the keys of every peer it ever read from.
#[tokio::test(flavor = "multi_thread")]
async fn a_forgotten_peer_is_dropped_and_can_come_back() {
    let rig = Rig::new().await;
    rig.get().await.expect("read");

    rig.consumer.backend.forget_peer(rig.owner.instance);
    eventually("the peer is dropped", || rig.net.invalidates() == 1).await;

    rig.get().await.expect("read after the peer was forgotten");
    assert_eq!((rig.net.conn_loads(), rig.net.region_loads()), (2, 2));
}
