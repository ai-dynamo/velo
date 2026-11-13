// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What the driver needs from a NIXL agent.
//!
//! Two things implement this: the adapter over `nixl-sys` (`sys.rs`) and an
//! agent that lives in one process (`fake.rs`). The driver's rules are written
//! against this trait, so they run in CI, where there is no libnixl.
//!
//! An agent is created on the driver thread and never leaves it, so the trait
//! asks for neither `Send` nor `Sync`. An error is text: the driver only
//! reports it.

/// One READ: `len` bytes at `remote_addr` on `peer`, into `local_addr`.
#[derive(Debug)]
pub(crate) struct ReadReq<'a> {
    /// The owner's agent name.
    pub peer: &'a str,
    /// Address in the owner's address space.
    pub remote_addr: u64,
    /// The device id the owner registered that memory under.
    pub remote_dev: u64,
    /// Destination address, inside a region this agent registered.
    pub local_addr: usize,
    /// The device id this agent registered the destination under.
    pub local_dev: u64,
    /// Bytes to read.
    pub len: usize,
    /// Which of the agent's workers carries the READ.
    pub worker: usize,
}

/// What a post returned.
pub(crate) enum Posted<X> {
    /// The transfer finished inside the call.
    Done,
    /// The transfer is running. Poll it.
    Pending(X),
}

/// What a poll of a running transfer returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum XferPoll {
    /// Finished. The bytes have landed.
    Done,
    /// Still running.
    InProgress,
    /// Finished with an error. Nothing more will be written.
    Failed(String),
    /// The agent can no longer say. The transfer may still be writing.
    Unconfirmed(String),
}

/// A NIXL agent with one backend, as the driver uses it.
pub(crate) trait NixlAgent: 'static {
    /// A posted transfer. The driver never drops one that may still be
    /// running: it forgets it, because a release lets the agent reuse state
    /// the transfer still needs.
    type Xfer;

    /// This agent's connection info. The same bytes for the agent's life.
    fn conn_blob(&mut self) -> Result<Vec<u8>, String>;

    /// Register `[addr, addr + len)` under `dev`, and return that region's
    /// metadata blob.
    fn register(&mut self, addr: usize, len: usize, dev: u64) -> Result<Vec<u8>, String>;

    /// Deregister a region.
    ///
    /// `Ok` means the agent no longer knows the region, checked and not
    /// assumed: the caller frees the pages on it.
    fn deregister(&mut self, addr: usize, len: usize, dev: u64) -> Result<(), String>;

    /// Load a peer's connection info or one of its region blobs. Returns the
    /// agent name the blob states.
    fn load_remote(&mut self, blob: &[u8]) -> Result<String, String>;

    /// Drop everything this agent holds for a peer.
    ///
    /// Never call it while a transfer to that peer is running: the agent then
    /// loses track of the transfer, and the transfer runs on.
    fn invalidate_remote(&mut self, agent: &str) -> Result<(), String>;

    /// Post a READ.
    fn post_read(&mut self, req: &ReadReq<'_>) -> Result<Posted<Self::Xfer>, String>;

    /// Ask how a posted transfer is doing.
    fn poll(&mut self, xfer: &mut Self::Xfer) -> XferPoll;

    /// Release a transfer that is over.
    fn finish(&mut self, xfer: Self::Xfer);
}
