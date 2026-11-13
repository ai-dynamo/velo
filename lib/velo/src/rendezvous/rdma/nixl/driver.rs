// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The NIXL driver: one thread that makes every call into the agent.
//!
//! NIXL has no completion callback and no file descriptor: a transfer is over
//! when a status poll says so. One thread polls, and owns the agent, the
//! region table and the peer table, because two operations must wait for the
//! transfers in flight: an unmap of a region that a READ is writing into, and
//! a purge of a peer (see `peers.rs`). With one owner both are plain
//! bookkeeping, as on the UCX progress thread.
//!
//! With nothing in flight the thread blocks on its command channel. It never
//! releases a transfer that may still be running, and never destroys an agent
//! that still holds a region or such a transfer. It leaks them, and
//! [`Shared::stuck`] keeps the registry from declaring the memory released.

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use velo_ext::InstanceId;

use super::agent::{NixlAgent, Posted, ReadReq, XferPoll};
use super::key::{HEADER_LEN, NixlKey, agent_name};
use super::peers::{Admit, Peer, PeerState};
use crate::rendezvous::descriptor::MAX_KEY_LEN;
use crate::rendezvous::rdma::arena::{GRANULE, page_enclosing_len};
use crate::rendezvous::rdma::backend::{BackendGet, BackendRegion, RdmaError};

/// Where the driver answers a command.
pub(crate) type Reply<T> = oneshot::Sender<Result<T, RdmaError>>;

/// What the backend asks of the driver.
pub(crate) enum Cmd {
    /// Register a region under `id`.
    Map {
        id: u64,
        ptr: usize,
        len: usize,
        reply: Reply<BackendRegion>,
    },
    /// Deregister a region. `reply` is `None` for a rollback nobody waits on.
    Unmap { id: u64, reply: Option<Reply<()>> },
    /// Read remote memory.
    Get { req: BackendGet, reply: Reply<()> },
    /// The instance unregistered a peer.
    ForgetPeer { peer: InstanceId },
    /// Stop: finish or give up what is in flight, release what can be
    /// released, and destroy the agent if that is safe.
    Teardown { reply: oneshot::Sender<()> },
}

/// The default of [`DriverConfig::spin`].
pub(crate) const DEFAULT_SPIN: Duration = Duration::from_micros(200);

/// Tuning the driver reads.
#[derive(Debug, Clone)]
pub(crate) struct DriverConfig {
    /// Workers the agent has. Each READ names one, in turn: NIXL gives every
    /// READ posted from one thread to one worker otherwise.
    pub workers: usize,
    /// How long to spin after a post or a completion before sleeping.
    pub spin: Duration,
    /// Region blobs kept for one peer before the peer is dropped and loaded
    /// again.
    pub max_regions_per_peer: usize,
    /// How long teardown waits for transfers in flight.
    pub teardown_timeout: Duration,
    /// Longest key `map` may build.
    pub max_key_len: usize,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            workers: 1,
            spin: DEFAULT_SPIN,
            max_regions_per_peer: 1024,
            teardown_timeout: Duration::from_secs(5),
            max_key_len: MAX_KEY_LEN,
        }
    }
}

/// What the backend reads without asking the driver.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    /// Regions the agent holds registered.
    pub live: AtomicUsize,
    /// The agent holds something that cannot be released: a transfer nobody
    /// can confirm, a region left at teardown, or a driver that panicked.
    /// Counts as one live registration, for good.
    pub stuck: AtomicBool,
}

/// Sleep between polls while a transfer is in flight: from the first, doubling
/// up to the second.
const MIN_PARK: Duration = Duration::from_micros(50);
const MAX_PARK: Duration = Duration::from_millis(1);

const TAINTED: &str = "nixl: a transfer nobody can confirm may still write this region";

struct Region {
    ptr: usize,
    len: usize,
    /// READs writing into this region.
    in_flight: usize,
    /// A READ into this region was lost track of. Never deregister it.
    tainted: bool,
    /// An unmap is waiting for `in_flight` to reach zero. No new READ may
    /// target the region.
    unmapping: bool,
    parked: Vec<Reply<()>>,
}

struct Waiting {
    req: BackendGet,
    reply: Reply<()>,
}

#[derive(Default)]
struct PeerEntry {
    peer: Peer,
    /// READs that wait for the peer to go idle, in arrival order. While this
    /// is not empty every new READ to the peer joins it: one that went ahead
    /// would keep the peer busy and starve the purge the others wait for.
    waiting: VecDeque<Waiting>,
    /// The instance unregistered this peer. Drop it once it is idle.
    forget: bool,
}

struct InFlight<X> {
    xfer: X,
    reply: Reply<()>,
    region: u64,
    peer: String,
}

struct Driver<A: NixlAgent> {
    agent: A,
    conn: Vec<u8>,
    cfg: DriverConfig,
    shared: Arc<Shared>,
    regions: HashMap<u64, Region>,
    peers: HashMap<String, PeerEntry>,
    in_flight: Vec<InFlight<A::Xfer>>,
    next_worker: usize,
    /// Teardown has begun. Nothing new is posted.
    stopping: bool,
}

/// Run the driver until the backend is dropped. Called on the driver thread,
/// with an agent that was created on it.
pub(crate) fn run<A: NixlAgent>(
    agent: A,
    conn: Vec<u8>,
    rx: flume::Receiver<Cmd>,
    cfg: DriverConfig,
    shared: Arc<Shared>,
) {
    let mut driver = Driver {
        agent,
        conn,
        cfg,
        shared: Arc::clone(&shared),
        regions: HashMap::new(),
        peers: HashMap::new(),
        in_flight: Vec::new(),
        next_worker: 0,
        stopping: false,
    };

    // The bindings can panic on what a peer sent (they unwrap the agent name
    // in a blob), and a panic that unwound this thread's state would drop the
    // agent and release transfers that may still be running.
    let served = catch_unwind(AssertUnwindSafe(|| {
        let stop = driver.serve(&rx);
        (stop, driver.teardown())
    }));
    let (stop, clean) = served.unwrap_or_else(|_| {
        tracing::error!(
            "nixl: the driver panicked. Its agent is leaked, its registrations stay \
             pinned, and every later rdma operation on this instance fails."
        );
        (None, false)
    });

    let held: HashSet<u64> = driver.regions.keys().copied().collect();
    if clean {
        // Nothing is registered and nothing is in flight: the agent can go.
        drop(driver);
    } else {
        shared.stuck.store(true, Ordering::Release);
        driver.leak();
    }
    if let Some(reply) = stop {
        let _ = reply.send(());
    }
    answer_stopped(&rx, &held);
}

/// After teardown: answer every command until the backend is dropped.
fn answer_stopped(rx: &flume::Receiver<Cmd>, held: &HashSet<u64>) {
    for cmd in rx.iter() {
        match cmd {
            Cmd::Map { reply, .. } => answer(reply, Err(RdmaError::ShuttingDown)),
            Cmd::Unmap { id, reply } => {
                if let Some(reply) = reply {
                    // A region this driver does not hold is unmapped, which is
                    // what was asked. One it still holds is not.
                    answer(
                        reply,
                        if held.contains(&id) {
                            Err(RdmaError::ShuttingDown)
                        } else {
                            Ok(())
                        },
                    );
                }
            }
            Cmd::Get { reply, .. } => answer(reply, Err(RdmaError::ShuttingDown)),
            Cmd::ForgetPeer { .. } => {}
            Cmd::Teardown { reply } => {
                let _ = reply.send(());
            }
        }
    }
}

fn answer<T>(reply: Reply<T>, value: Result<T, RdmaError>) {
    // The caller may have gone. Its transfer, if any, runs on regardless.
    let _ = reply.send(value);
}

impl<A: NixlAgent> Driver<A> {
    /// Keep the agent and every transfer still in flight alive for good, and
    /// drop the rest. A dropped reply tells its caller that the driver is
    /// gone, so no caller waits for an answer that cannot come.
    fn leak(self) {
        for flight in self.in_flight {
            std::mem::forget(flight.xfer);
        }
        std::mem::forget(self.agent);
    }

    /// Serve commands and poll transfers. Returns on `Teardown` (with its
    /// reply), or when every sender is gone (`None`).
    fn serve(&mut self, rx: &flume::Receiver<Cmd>) -> Option<oneshot::Sender<()>> {
        let mut last_activity = Instant::now();
        let mut park = MIN_PARK;
        loop {
            let cmd = if self.in_flight.is_empty() {
                // Idle. No poll, no timer, no spin.
                match rx.recv() {
                    Ok(cmd) => Some(cmd),
                    Err(_) => return None,
                }
            } else if last_activity.elapsed() < self.cfg.spin {
                match rx.try_recv() {
                    Ok(cmd) => Some(cmd),
                    Err(flume::TryRecvError::Empty) => {
                        std::hint::spin_loop();
                        None
                    }
                    Err(flume::TryRecvError::Disconnected) => return None,
                }
            } else {
                match rx.recv_timeout(park) {
                    Ok(cmd) => Some(cmd),
                    Err(flume::RecvTimeoutError::Timeout) => {
                        park = (park * 2).min(MAX_PARK);
                        None
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => return None,
                }
            };
            if let Some(cmd) = cmd {
                if let Some(stop) = self.handle(cmd) {
                    return Some(stop);
                }
                last_activity = Instant::now();
                park = MIN_PARK;
            }
            if self.poll_transfers() {
                last_activity = Instant::now();
                park = MIN_PARK;
            }
        }
    }

    fn handle(&mut self, cmd: Cmd) -> Option<oneshot::Sender<()>> {
        match cmd {
            Cmd::Map {
                id,
                ptr,
                len,
                reply,
            } => {
                let result = self.map(id, ptr, len);
                let mapped = result.is_ok();
                if reply.send(result).is_err() && mapped {
                    // The caller dropped its `map` and will free the pages.
                    self.unmap_now(id, Vec::new());
                }
            }
            Cmd::Unmap { id, reply } => self.unmap(id, reply),
            Cmd::Get { req, reply } => self.start_get(req, reply),
            Cmd::ForgetPeer { peer } => self.forget_peer(&agent_name(peer)),
            Cmd::Teardown { reply } => return Some(reply),
        }
        None
    }

    // -- regions ----------------------------------------------------------

    fn map(&mut self, id: u64, ptr: usize, len: usize) -> Result<BackendRegion, RdmaError> {
        if ptr == 0 || len == 0 {
            return Err(RdmaError::OutOfRange);
        }
        // NIXL does not report what it pinned. Registration pins whole pages,
        // so the range the kernel charges is the page-enclosing one.
        let effective_len = page_enclosing_len(ptr, len).ok_or(RdmaError::OutOfRange)?;
        let md = self
            .agent
            .register(ptr, len, id)
            .map_err(RdmaError::Backend)?;
        self.regions.insert(
            id,
            Region {
                ptr,
                len,
                in_flight: 0,
                tainted: false,
                unmapping: false,
                parked: Vec::new(),
            },
        );
        self.shared.live.fetch_add(1, Ordering::AcqRel);
        let key = NixlKey {
            region_dev: id,
            region_addr: ptr as u64,
            region_len: len as u64,
            conn: &self.conn,
            md: &md,
        };
        let Some(packed_key) = key.encode(self.cfg.max_key_len) else {
            // Fail here, where it is loud. A key that only fails at the
            // consumer is a silent chunked fallback on every transfer.
            let size = HEADER_LEN + self.conn.len() + md.len();
            self.unmap_now(id, Vec::new());
            return Err(RdmaError::Backend(format!(
                "nixl: the key for this region is {size} bytes, over the {} byte bound",
                self.cfg.max_key_len
            )));
        };
        Ok(BackendRegion {
            backend_region_id: id,
            effective_addr: (ptr & !(GRANULE - 1)) as u64,
            effective_len,
            packed_key,
        })
    }

    fn unmap(&mut self, id: u64, reply: Option<Reply<()>>) {
        let waiters: Vec<Reply<()>> = reply.into_iter().collect();
        match self.regions.get_mut(&id) {
            // Park it. From now on no new READ may target the region, so the
            // count can only fall. A repeat unmap joins the first.
            Some(region) if region.in_flight > 0 && !region.tainted => {
                region.unmapping = true;
                region.parked.extend(waiters);
            }
            _ => self.unmap_now(id, waiters),
        }
    }

    /// Deregister a region that nothing is writing into, and answer `waiters`.
    fn unmap_now(&mut self, id: u64, waiters: Vec<Reply<()>>) {
        let result = match self.regions.get(&id) {
            // Idempotent: a region this driver does not hold is unmapped.
            None => Ok(()),
            Some(region) if region.tainted => Err(RdmaError::Backend(TAINTED.to_string())),
            Some(region) => match self.agent.deregister(region.ptr, region.len, id) {
                Ok(()) => {
                    self.regions.remove(&id);
                    self.shared.live.fetch_sub(1, Ordering::AcqRel);
                    Ok(())
                }
                // Still registered, as far as anyone can tell. The region
                // stays in the table and stays counted.
                Err(e) => {
                    if let Some(region) = self.regions.get_mut(&id) {
                        region.unmapping = false;
                    }
                    Err(RdmaError::Backend(e))
                }
            },
        };
        for waiter in waiters {
            answer(waiter, result.clone());
        }
    }

    /// The local address a READ writes to, checked against the region.
    fn destination(&self, req: &BackendGet) -> Result<(usize, usize), RdmaError> {
        let region = self
            .regions
            .get(&req.local_region_id)
            .ok_or(RdmaError::RegionNotFound)?;
        if region.unmapping {
            return Err(RdmaError::RegionNotFound);
        }
        let len = usize::try_from(req.len).map_err(|_| RdmaError::OutOfRange)?;
        let offset = usize::try_from(req.local_offset).map_err(|_| RdmaError::OutOfRange)?;
        let end = offset.checked_add(len).ok_or(RdmaError::OutOfRange)?;
        if len == 0 || end > region.len {
            return Err(RdmaError::OutOfRange);
        }
        Ok((region.ptr + offset, len))
    }

    // -- transfers --------------------------------------------------------

    fn start_get(&mut self, req: BackendGet, reply: Reply<()>) {
        let name = agent_name(req.peer);
        if !self.entry(&name).waiting.is_empty() {
            self.entry(&name).waiting.push_back(Waiting { req, reply });
            return;
        }
        match self.post(&name, &req) {
            Ok(Some(Posted::Pending(xfer))) => {
                if let Some(region) = self.regions.get_mut(&req.local_region_id) {
                    region.in_flight += 1;
                }
                self.entry(&name).peer.began();
                self.in_flight.push(InFlight {
                    xfer,
                    reply,
                    region: req.local_region_id,
                    peer: name,
                });
            }
            Ok(Some(Posted::Done)) => answer(reply, Ok(())),
            Ok(None) => self.entry(&name).waiting.push_back(Waiting { req, reply }),
            Err(e) => answer(reply, Err(e)),
        }
    }

    /// Bring the peer to a state in which the READ may be posted, and post it.
    /// `None`: a purge is due and the peer is busy, so the READ must wait.
    fn post(&mut self, name: &str, req: &BackendGet) -> Result<Option<Posted<A::Xfer>>, RdmaError> {
        if self.stopping {
            return Err(RdmaError::ShuttingDown);
        }
        let (local_addr, len) = self.destination(req)?;
        let key = NixlKey::decode(&req.packed_key).map_err(|e| {
            tracing::warn!(peer = %name, error = e, "nixl: refusing a key that does not decode");
            RdmaError::InvalidKey
        })?;
        // The owner wrote both the range and the key. Check them against each
        // other anyway, before a NIC is asked to read the range.
        if !key.covers(req.remote_addr, req.len) {
            return Err(RdmaError::OutOfRange);
        }

        let cap = self.cfg.max_regions_per_peer;
        loop {
            match self.entry(name).peer.admit(&key, cap) {
                Admit::Ready => break,
                Admit::Wait => return Ok(None),
                Admit::Poisoned => {
                    return Err(RdmaError::Backend(format!(
                        "nixl: peer {name} is unusable after a refused load"
                    )));
                }
                Admit::Refuse => {
                    tracing::warn!(peer = %name, dev = key.region_dev, "nixl: refusing a key that reuses a region id for another range");
                    return Err(RdmaError::InvalidKey);
                }
                Admit::Purge => {
                    self.agent
                        .invalidate_remote(name)
                        .map_err(|e| self.poison(name, e))?;
                    self.entry(name).peer.purged();
                }
                Admit::Load { conn } => {
                    // Connection info first, then the region.
                    for blob in conn.then_some(key.conn).into_iter().chain([key.md]) {
                        let loaded = self
                            .agent
                            .load_remote(blob)
                            .map_err(|e| self.poison(name, e))?;
                        if loaded != name {
                            // `name` sent a key that is not its own. `loaded`
                            // now holds state nobody asked for: drop it before
                            // its next READ.
                            self.entry(&loaded).peer.suspect();
                            self.poison(name, format!("its key loaded as {loaded}"));
                            return Err(RdmaError::InvalidKey);
                        }
                    }
                    self.entry(name).peer.loaded(&key);
                }
            }
        }

        let worker = self.next_worker;
        self.next_worker = (worker + 1) % self.cfg.workers.max(1);
        let posted = self.agent.post_read(&ReadReq {
            peer: name,
            remote_addr: req.remote_addr,
            remote_dev: key.region_dev,
            local_addr,
            local_dev: req.local_region_id,
            len,
            worker,
        });
        posted.map(Some).map_err(|e| {
            // NIXL fails later loads on an endpoint that had an error, so the
            // peer is dropped before the next one.
            self.entry(name).peer.suspect();
            RdmaError::Backend(e)
        })
    }

    fn entry(&mut self, name: &str) -> &mut PeerEntry {
        // Not `entry()`: that takes an owned key, an allocation for each
        // lookup of a peer that is nearly always there.
        if !self.peers.contains_key(name) {
            self.peers.insert(name.to_string(), PeerEntry::default());
        }
        self.peers.get_mut(name).expect("inserted above")
    }

    /// A load for `name` was refused, or a drop of it failed. Returns the
    /// error for the READ that found out.
    fn poison(&mut self, name: &str, detail: String) -> RdmaError {
        self.entry(name).peer.poison();
        tracing::error!(
            peer = %name,
            detail,
            "nixl: a load for this peer was refused. Transfers from it take the chunked path \
             until it is registered again."
        );
        RdmaError::Backend(detail)
    }

    /// Poll every transfer in flight. Returns whether any finished.
    fn poll_transfers(&mut self) -> bool {
        let mut finished = false;
        let mut i = 0;
        while i < self.in_flight.len() {
            match self.agent.poll(&mut self.in_flight[i].xfer) {
                XferPoll::InProgress => i += 1,
                outcome => {
                    let done = self.in_flight.swap_remove(i);
                    self.complete(done, outcome);
                    finished = true;
                }
            }
        }
        finished
    }

    fn complete(&mut self, done: InFlight<A::Xfer>, outcome: XferPoll) {
        let InFlight {
            xfer,
            reply,
            region,
            peer,
        } = done;
        let result = match outcome {
            XferPoll::Done => {
                self.agent.finish(xfer);
                Ok(())
            }
            XferPoll::Failed(detail) => {
                self.agent.finish(xfer);
                self.entry(&peer).peer.suspect();
                Err(RdmaError::Backend(format!("nixl: {detail}")))
            }
            XferPoll::Unconfirmed(detail) => {
                // Not released: the transfer may still be running.
                std::mem::forget(xfer);
                self.shared.stuck.store(true, Ordering::Release);
                if let Some(region) = self.regions.get_mut(&region) {
                    region.tainted = true;
                }
                self.entry(&peer).peer.suspect();
                tracing::error!(
                    peer,
                    region,
                    detail,
                    "nixl: lost track of a transfer that may still be writing. Its region stays \
                     registered and its memory is never reused."
                );
                Err(RdmaError::Unconfirmed)
            }
            XferPoll::InProgress => unreachable!("a transfer in progress is not complete"),
        };
        answer(reply, result);

        let parked = self.regions.get_mut(&region).and_then(|r| {
            r.in_flight = r.in_flight.saturating_sub(1);
            (r.in_flight == 0 && r.unmapping).then(|| std::mem::take(&mut r.parked))
        });
        if let Some(waiters) = parked {
            self.unmap_now(region, waiters);
        }
        if self.entry(&peer).peer.ended() {
            self.peer_idle(&peer);
        }
    }

    /// Nothing is in flight to `name` any more: let the READs that waited go.
    fn peer_idle(&mut self, name: &str) {
        let waiting = std::mem::take(&mut self.entry(name).waiting);
        if waiting.is_empty() && self.entry(name).forget {
            self.drop_peer(name);
        }
        // Through the front door: one of them may have to wait again (a second
        // purge at the cap), and the rest then keep their order behind it.
        for Waiting { req, reply } in waiting {
            self.start_get(req, reply);
        }
    }

    // -- peers ------------------------------------------------------------

    fn forget_peer(&mut self, name: &str) {
        match self.peers.get_mut(name) {
            Some(entry) if entry.peer.in_flight() == 0 && entry.waiting.is_empty() => {
                self.drop_peer(name);
            }
            Some(entry) => entry.forget = true,
            None => {}
        }
    }

    fn drop_peer(&mut self, name: &str) {
        let Some(entry) = self.peers.remove(name) else {
            return;
        };
        if matches!(entry.peer.state(), PeerState::Loaded | PeerState::Suspect) {
            let _ = self.agent.invalidate_remote(name);
        }
    }

    // -- teardown ---------------------------------------------------------

    /// Stop. Returns whether the agent may be destroyed: nothing registered,
    /// nothing lost.
    fn teardown(&mut self) -> bool {
        self.stopping = true;

        // Give what is in flight a bounded time to finish, then give it up.
        // Completing a READ answers the unmaps parked on its region and the
        // READs that waited for its peer.
        let deadline = Instant::now() + self.cfg.teardown_timeout;
        while !self.in_flight.is_empty() && Instant::now() < deadline {
            if !self.poll_transfers() {
                std::thread::sleep(Duration::from_micros(200));
            }
        }
        for flight in std::mem::take(&mut self.in_flight) {
            let gave_up = "still running when teardown gave up".to_string();
            self.complete(flight, XferPoll::Unconfirmed(gave_up));
        }

        // The registry's sweep has unmapped what it could. Whatever is left is
        // released here, as transport teardown does for UCX.
        let ids: Vec<u64> = self.regions.keys().copied().collect();
        for id in ids {
            self.unmap_now(id, Vec::new());
        }

        let clean = self.regions.is_empty() && !self.shared.stuck.load(Ordering::Acquire);
        if !clean {
            tracing::error!(
                regions = self.regions.len(),
                "nixl: the agent still holds registrations at teardown. It is leaked, and \
                 their memory stays pinned."
            );
        }
        clean
    }
}
