// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A NIXL agent that lives in one process, for tests.
//!
//! CI has no libnixl, and a real agent cannot be asked to misbehave on cue.
//! This fake stands in for it. It follows the NIXL behaviour that the driver's
//! rules exist for, as measured on libnixl 1.5.2 (`docs/src/development/
//! rdma-design.md`):
//!
//! - Region blobs pile up for a peer. Only a drop of the whole peer removes
//!   them.
//! - A READ is served from an entry under the device id it names, and from no
//!   other.
//! - A stale entry is **not** refused. The fake reads through it, as NIXL does
//!   over tcp, and counts it in [`NixlLoopback::stale_reads`]. On InfiniBand
//!   that read aborts the process, so a test asserts the count is zero.
//! - A region blob before the connection info is refused, with no harm done.
//! - A blob that does not parse is refused.
//! - A drop of a peer with a transfer in flight makes the status of that
//!   transfer unknown. (In NIXL the transfer still lands later. The fake does
//!   not model the late write, only the answer.)
//!
//! Agents that share a [`NixlLoopback`] can read each other's memory. "Read"
//! is a copy between two addresses of this process, so both ends must be in
//! it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::agent::{NixlAgent, Posted, ReadReq, XferPoll};

/// The network a set of fake agents share, with the knobs and counters a test
/// uses.
#[derive(Clone, Default)]
pub struct NixlLoopback {
    inner: Arc<Net>,
}

/// One agent's registrations: device id to `(address, length)`.
type Registrations = HashMap<u64, (usize, usize)>;

#[derive(Default)]
struct Net {
    /// Agent name to its registrations.
    registered: Mutex<HashMap<String, Registrations>>,
    /// Transfers stay in progress while this is set.
    hold: AtomicBool,
    /// The next transfer that is polled fails.
    fail_next: AtomicBool,
    /// Every deregistration fails while this is set.
    refuse_deregister: AtomicBool,
    /// The next transfer that is polled is reported as lost track of.
    lose_next: AtomicBool,
    /// The next load panics, as the bindings do on a name that is not UTF-8.
    panic_next_load: AtomicBool,
    /// A transfer finishes inside the post, as a short one does in NIXL.
    done_at_post: AtomicBool,
    registers: AtomicUsize,
    stale_reads: AtomicUsize,
    conn_loads: AtomicUsize,
    region_loads: AtomicUsize,
    invalidates: AtomicUsize,
    polls: AtomicUsize,
    /// Posts by worker index.
    posts: Mutex<Vec<usize>>,
}

impl NixlLoopback {
    /// A new, empty network.
    pub fn new() -> Self {
        Self::default()
    }

    /// Keep transfers in progress (`true`), or let them finish (`false`).
    pub fn hold_transfers(&self, hold: bool) {
        self.inner.hold.store(hold, Ordering::SeqCst);
    }

    /// Make the next polled transfer fail.
    pub fn fail_next_transfer(&self) {
        self.inner.fail_next.store(true, Ordering::SeqCst);
    }

    /// Make every deregistration fail (`true`), or succeed again (`false`).
    pub fn refuse_deregister(&self, refuse: bool) {
        self.inner.refuse_deregister.store(refuse, Ordering::SeqCst);
    }

    /// Make the next polled transfer answer that nobody can say whether it
    /// stopped.
    pub fn lose_next_transfer(&self) {
        self.inner.lose_next.store(true, Ordering::SeqCst);
    }

    /// Make the next load of a blob panic inside the agent.
    pub fn panic_next_load(&self) {
        self.inner.panic_next_load.store(true, Ordering::SeqCst);
    }

    /// Finish each transfer inside its post (`true`), or at a poll (`false`).
    pub fn finish_at_post(&self, at_post: bool) {
        self.inner.done_at_post.store(at_post, Ordering::SeqCst);
    }

    /// Agents that exist. An agent that was leaked still counts.
    pub fn agents(&self) -> usize {
        self.inner.registered.lock().len()
    }

    /// Registrations made, over all agents and over time.
    pub fn registers(&self) -> usize {
        self.inner.registers.load(Ordering::SeqCst)
    }

    /// READs served through an entry whose registration the owner had already
    /// dropped. Each is a process abort on InfiniBand.
    pub fn stale_reads(&self) -> usize {
        self.inner.stale_reads.load(Ordering::SeqCst)
    }

    /// Connection blobs loaded, over all agents.
    pub fn conn_loads(&self) -> usize {
        self.inner.conn_loads.load(Ordering::SeqCst)
    }

    /// Region blobs loaded, over all agents.
    pub fn region_loads(&self) -> usize {
        self.inner.region_loads.load(Ordering::SeqCst)
    }

    /// Peers dropped, over all agents.
    pub fn invalidates(&self) -> usize {
        self.inner.invalidates.load(Ordering::SeqCst)
    }

    /// Status polls, over all agents.
    pub fn polls(&self) -> usize {
        self.inner.polls.load(Ordering::SeqCst)
    }

    /// How many READs each worker index was given.
    pub fn posts_by_worker(&self) -> Vec<usize> {
        self.inner.posts.lock().clone()
    }

    /// Regions registered, over all agents.
    pub fn registered_regions(&self) -> usize {
        self.inner
            .registered
            .lock()
            .values()
            .map(HashMap::len)
            .sum()
    }
}

/// What a fake agent keeps for one peer.
#[derive(Default)]
struct Remote {
    conn: bool,
    /// Device id to `(address, length)`.
    regions: HashMap<u64, (u64, u64)>,
}

/// A posted fake transfer.
pub(crate) struct FakeXfer {
    peer: String,
    src: usize,
    dst: usize,
    len: usize,
    /// The peer was dropped while this was in flight.
    lost: Arc<AtomicBool>,
}

pub(crate) struct FakeAgent {
    name: String,
    net: NixlLoopback,
    remotes: HashMap<String, Remote>,
    /// The `lost` flag of each transfer in flight, by peer.
    in_flight: HashMap<String, Vec<Arc<AtomicBool>>>,
}

impl FakeAgent {
    pub(crate) fn new(name: &str, net: NixlLoopback) -> Self {
        // An agent is on the network from its creation to its drop, which is
        // what `NixlLoopback::agents` counts.
        net.inner
            .registered
            .lock()
            .insert(name.to_string(), HashMap::new());
        Self {
            name: name.to_string(),
            net,
            remotes: HashMap::new(),
            in_flight: HashMap::new(),
        }
    }
}

/// The READ itself.
fn copy(src: usize, dst: usize, len: usize) {
    // SAFETY: test-only. Both ranges were checked against regions that agents
    // of this process registered, and the tests keep that memory alive while a
    // transfer can run.
    unsafe { std::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, len) }
}

const CONN: &str = "fake-conn";
const REGION: &str = "fake-region";

enum Blob {
    Conn {
        name: String,
    },
    Region {
        name: String,
        dev: u64,
        addr: u64,
        len: u64,
    },
}

fn parse(blob: &[u8]) -> Option<Blob> {
    let fields: Vec<&str> = std::str::from_utf8(blob).ok()?.split('\n').collect();
    match fields[..] {
        [CONN, name] => Some(Blob::Conn {
            name: name.to_string(),
        }),
        [REGION, name, dev, addr, len] => Some(Blob::Region {
            name: name.to_string(),
            dev: dev.parse().ok()?,
            addr: addr.parse().ok()?,
            len: len.parse().ok()?,
        }),
        _ => None,
    }
}

impl NixlAgent for FakeAgent {
    type Xfer = FakeXfer;

    fn conn_blob(&mut self) -> Result<Vec<u8>, String> {
        Ok(format!("{CONN}\n{}", self.name).into_bytes())
    }

    fn register(&mut self, addr: usize, len: usize, dev: u64) -> Result<Vec<u8>, String> {
        let mut registered = self.net.inner.registered.lock();
        let mine = registered.entry(self.name.clone()).or_default();
        if mine.insert(dev, (addr, len)).is_some() {
            return Err("register: device id already in use".to_string());
        }
        self.net.inner.registers.fetch_add(1, Ordering::SeqCst);
        Ok(format!("{REGION}\n{}\n{dev}\n{addr}\n{len}", self.name).into_bytes())
    }

    fn deregister(&mut self, _addr: usize, _len: usize, dev: u64) -> Result<(), String> {
        if self.net.inner.refuse_deregister.load(Ordering::SeqCst) {
            return Err("deregister: the region is still registered".to_string());
        }
        let mut registered = self.net.inner.registered.lock();
        registered
            .entry(self.name.clone())
            .or_default()
            .remove(&dev);
        Ok(())
    }

    fn load_remote(&mut self, blob: &[u8]) -> Result<String, String> {
        if self.net.inner.panic_next_load.swap(false, Ordering::SeqCst) {
            panic!("injected panic inside the fake nixl agent");
        }
        match parse(blob) {
            Some(Blob::Conn { name }) => {
                self.net.inner.conn_loads.fetch_add(1, Ordering::SeqCst);
                self.remotes.entry(name.clone()).or_default().conn = true;
                Ok(name)
            }
            Some(Blob::Region {
                name,
                dev,
                addr,
                len,
            }) => {
                let remote = self.remotes.entry(name.clone()).or_default();
                if !remote.conn {
                    // Measured: refused, and the peer is still loadable.
                    return Err("load_remote_md: NotFound".to_string());
                }
                self.net.inner.region_loads.fetch_add(1, Ordering::SeqCst);
                remote.regions.insert(dev, (addr, len));
                Ok(name)
            }
            // In NIXL such a load can leave the peer unloadable. The driver
            // must not ask again, so the fake has nothing to model after it.
            None => Err("load_remote_md: BackendError".to_string()),
        }
    }

    fn invalidate_remote(&mut self, agent: &str) -> Result<(), String> {
        self.net.inner.invalidates.fetch_add(1, Ordering::SeqCst);
        self.remotes.remove(agent);
        // Measured: the agent loses track of what was in flight to the peer.
        for lost in self.in_flight.remove(agent).unwrap_or_default() {
            lost.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn post_read(&mut self, req: &ReadReq<'_>) -> Result<Posted<FakeXfer>, String> {
        {
            let mut posts = self.net.inner.posts.lock();
            if posts.len() <= req.worker {
                posts.resize(req.worker + 1, 0);
            }
            posts[req.worker] += 1;
        }

        // The remote side: served from the entry under the named device id,
        // and from no other.
        let entry = self
            .remotes
            .get(req.peer)
            .and_then(|remote| remote.regions.get(&req.remote_dev))
            .copied();
        let Some((addr, len)) = entry else {
            return Err("create_xfer_req: no registration covers the remote range".to_string());
        };
        let end = req.remote_addr.checked_add(req.len as u64);
        if req.remote_addr < addr || end.is_none_or(|end| end > addr + len) {
            return Err("create_xfer_req: the remote range is outside its region".to_string());
        }

        // The local side must be registered with this agent.
        let registered = self.net.inner.registered.lock();
        let local = registered
            .get(&self.name)
            .and_then(|mine| mine.get(&req.local_dev))
            .copied();
        let local_ok = local.is_some_and(|(addr, len)| {
            req.local_addr >= addr
                && req
                    .local_addr
                    .checked_add(req.len)
                    .is_some_and(|end| end <= addr + len)
        });
        if !local_ok {
            return Err("create_xfer_req: the local range is not registered".to_string());
        }

        // A real agent does not check that the owner still holds the region.
        // Neither does this one: it reads, and counts the stale read.
        let owner_holds = registered
            .get(req.peer)
            .is_some_and(|theirs| theirs.contains_key(&req.remote_dev));
        if !owner_holds {
            self.net.inner.stale_reads.fetch_add(1, Ordering::SeqCst);
        }
        drop(registered);

        let (src, dst) = (req.remote_addr as usize, req.local_addr);
        if self.net.inner.done_at_post.load(Ordering::SeqCst) {
            copy(src, dst, req.len);
            return Ok(Posted::Done);
        }
        let lost = Arc::new(AtomicBool::new(false));
        self.in_flight
            .entry(req.peer.to_string())
            .or_default()
            .push(Arc::clone(&lost));
        Ok(Posted::Pending(FakeXfer {
            peer: req.peer.to_string(),
            src,
            dst,
            len: req.len,
            lost,
        }))
    }

    fn poll(&mut self, xfer: &mut FakeXfer) -> XferPoll {
        self.net.inner.polls.fetch_add(1, Ordering::SeqCst);
        if xfer.lost.load(Ordering::SeqCst) {
            return XferPoll::Unconfirmed("the peer was dropped during the transfer".into());
        }
        if self.net.inner.lose_next.swap(false, Ordering::SeqCst) {
            return XferPoll::Unconfirmed("injected".into());
        }
        if self.net.inner.hold.load(Ordering::SeqCst) {
            return XferPoll::InProgress;
        }
        if self.net.inner.fail_next.swap(false, Ordering::SeqCst) {
            return XferPoll::Failed("injected transfer failure".into());
        }
        copy(xfer.src, xfer.dst, xfer.len);
        XferPoll::Done
    }

    fn finish(&mut self, xfer: FakeXfer) {
        if let Some(flags) = self.in_flight.get_mut(&xfer.peer) {
            flags.retain(|flag| !Arc::ptr_eq(flag, &xfer.lost));
        }
    }
}

impl Drop for FakeAgent {
    fn drop(&mut self) {
        self.net.inner.registered.lock().remove(&self.name);
    }
}
