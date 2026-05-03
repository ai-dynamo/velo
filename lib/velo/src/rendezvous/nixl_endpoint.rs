// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NIXL endpoint state held by [`RendezvousManager`](crate::RendezvousManager).
//!
//! Only compiled when the `nixl` feature is enabled. Wraps a NIXL agent +
//! UCX backend + cached local metadata, plus a per-peer cache of loaded
//! remote-agent names so a (consumer, owner) pair only handshakes once.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use dashmap::DashMap;
use velo_ext::WorkerId;

/// A NIXL-registerable view of a `Bytes` payload.
///
/// `Bytes::as_ptr()` is stable for the lifetime of the `Bytes` value, and the
/// payload owned by the slot keeps that same `Bytes` alive (via clone) until
/// the slot frees. So registering against the address of `bytes.as_ptr()` is
/// sound as long as the slot owns one of the clones.
pub(crate) struct BytesNixlDescriptor {
    bytes: Bytes,
}

impl BytesNixlDescriptor {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

impl std::fmt::Debug for BytesNixlDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesNixlDescriptor")
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl velo_nixl::MemoryRegion for BytesNixlDescriptor {
    fn size(&self) -> usize {
        self.bytes.len()
    }

    unsafe fn as_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }
}

impl velo_nixl::NixlDescriptor for BytesNixlDescriptor {
    fn mem_type(&self) -> velo_nixl::MemType {
        velo_nixl::MemType::Dram
    }

    fn device_id(&self) -> u64 {
        0
    }
}

/// `OptArgs` newtype that promises `Send`/`Sync`.
///
/// `velo_nixl::OptArgs` wraps a `NonNull<...>` to a libnixl C struct and is
/// auto-not-`Send`/`Sync`. We construct it once during [`NixlEndpoint::create`],
/// add the UCX backend, and never mutate it again — we only borrow `&OptArgs`
/// at registration / xfer call sites. libnixl is documented as treating
/// `nixl_opt_args_t` as a read-only configuration once populated, so passing
/// the same pointer from multiple threads concurrently is safe.
struct SendSyncOptArgs(velo_nixl::OptArgs);

// SAFETY: see doc comment above. The wrapped `OptArgs` is treated as
// effectively immutable after construction; libnixl only reads it.
unsafe impl Send for SendSyncOptArgs {}
unsafe impl Sync for SendSyncOptArgs {}

/// Lazily-built NIXL state shared between owner and consumer roles within a
/// single [`RendezvousManager`](crate::RendezvousManager).
///
/// The agent name format is `velo-<worker_id>` so peers can derive each
/// other's names without an extra round-trip. (Initial discovery still uses a
/// handshake AM to fetch `local_md`.)
pub struct NixlEndpoint {
    pub(crate) agent: velo_nixl::Agent,
    pub(crate) agent_name: String,
    /// Pre-built `OptArgs` with the UCX backend added — required by libnixl
    /// for `register_memory` / `create_xfer_req` to disambiguate which
    /// backend to use when an agent has any backends configured.
    opt_args: SendSyncOptArgs,
    /// Peer worker_id → loaded remote agent name (cached after the first
    /// successful `load_remote_md`).
    pub(crate) remote_md_loaded: DashMap<WorkerId, String>,
}

impl NixlEndpoint {
    /// Create a UCX-backed endpoint named `velo-<worker_id>`.
    pub(crate) fn create(worker_id: WorkerId) -> Result<Arc<Self>> {
        let agent_name = format!("velo-{worker_id}");
        let (agent, _backend, opt_args) = velo_nixl::agent_with_ucx(&agent_name)
            .context("velo-nixl: failed to create UCX-backed agent")?;
        // Note: we deliberately do *not* cache `agent.get_local_md()` here.
        // libnixl's local-MD blob is a snapshot of the agent's current
        // registrations; if we cached it before any `register_data_pinned`
        // calls, peers that load it via `load_remote_md` would miss those
        // registrations. The handshake handler calls `get_local_md()` on
        // every request to pick up the latest registrations.
        // `_backend` drops here — `Backend` has no `Drop` impl, and the agent
        // retains the underlying C-side backend handle in `AgentInner.backends`.
        Ok(Arc::new(Self {
            agent,
            agent_name,
            opt_args: SendSyncOptArgs(opt_args),
            remote_md_loaded: DashMap::new(),
        }))
    }

    /// Snapshot the agent's current local metadata. Called from the handshake
    /// handler so the blob includes registrations that have happened since
    /// `enable_nixl()`.
    pub(crate) fn current_local_md(&self) -> Result<Vec<u8>> {
        self.agent
            .get_local_md()
            .context("velo-nixl: get_local_md failed")
    }

    /// Borrow the pre-built `OptArgs`.
    pub(crate) fn opt_args(&self) -> &velo_nixl::OptArgs {
        &self.opt_args.0
    }

    /// Register a `Bytes` payload with the NIXL agent and return both the
    /// registration handle and the same `Bytes` value (so the caller can
    /// store both in a slot).
    pub(crate) fn register_bytes(
        &self,
        data: Bytes,
    ) -> Result<(velo_nixl::RegistrationHandle, Bytes)> {
        let desc = BytesNixlDescriptor::new(data);
        let handle = self
            .agent
            .register_memory(&desc, Some(self.opt_args()))
            .context("velo-nixl: register_memory failed")?;
        Ok((handle, desc.into_bytes()))
    }
}
