// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! NIXL endpoint state held by [`RendezvousManager`](crate::RendezvousManager).
//!
//! Only compiled when the `nixl` feature is enabled. Wraps a NIXL agent +
//! UCX backend, plus arenas for staging owner-side payloads and consumer-side
//! destinations without paying NIXL `register_memory` cost on every operation.
//!
//! Arena lifecycle:
//! - The host arena is created at [`NixlEndpoint::create`] and lives for the
//!   process. A single big `SystemStorage` is registered with NIXL once;
//!   [`ArenaAllocator`] slices it into per-call buffers (`ArenaBuffer`).
//! - Device arenas are lazy-created on first use per CUDA device and cached
//!   in a `DashMap<u32, _>`. Same registration-once pattern.

use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashMap;
use dynamo_memory::{
    ArenaAllocator, ArenaBuffer, DeviceStorage, MemoryDescriptor, SystemStorage,
    nixl::{NixlRegistered, register_with_nixl},
};
use velo_ext::WorkerId;

/// Default size of the host arena (system memory pre-registered with NIXL).
///
/// Sized to absorb a few moderate model artifacts / tool outputs without
/// fragmentation; configurable via [`NixlEndpointConfig`]. Owners that pin
/// many small payloads will want this larger; consumers pulling once and
/// dropping can keep it small.
pub const DEFAULT_HOST_ARENA_BYTES: usize = 64 * 1024 * 1024;

/// Default size of each per-device VRAM arena. Lazy-created on first use.
pub const DEFAULT_DEVICE_ARENA_BYTES: usize = 64 * 1024 * 1024;

/// Default arena page size (4 KiB matches x86 VM page; aligns NIXL disk
/// transfers per dynamo-memory's `posix_memalign(_, 4096, _)` choice in
/// `SystemStorage::new`).
pub const DEFAULT_ARENA_PAGE_SIZE: usize = 4096;

/// Configuration knobs for [`NixlEndpoint::create_with_config`].
#[derive(Debug, Clone, Copy)]
pub struct NixlEndpointConfig {
    /// Size in bytes of the host arena. Allocated once at endpoint creation.
    pub host_arena_bytes: usize,
    /// Size in bytes of each device arena (one per device, lazy-created).
    pub device_arena_bytes: usize,
    /// Arena page size; must be a power of two.
    pub page_size: usize,
}

impl Default for NixlEndpointConfig {
    fn default() -> Self {
        Self {
            host_arena_bytes: DEFAULT_HOST_ARENA_BYTES,
            device_arena_bytes: DEFAULT_DEVICE_ARENA_BYTES,
            page_size: DEFAULT_ARENA_PAGE_SIZE,
        }
    }
}

/// Type alias for an arena buffer over registered host memory.
pub(crate) type HostArenaBuffer = ArenaBuffer<NixlRegistered<SystemStorage>>;

/// Arena slice in NIXL-registered VRAM. Returned by
/// [`RendezvousManager::get_into_device_arena`] and held by the caller until
/// the device data is no longer needed (drop releases the slice back to the
/// arena, which stays registered for the life of the endpoint).
pub type DeviceArenaBuffer = ArenaBuffer<NixlRegistered<DeviceStorage>>;

/// Host arena allocator over NIXL-registered system memory.
pub(crate) type HostArena = ArenaAllocator<NixlRegistered<SystemStorage>>;

/// Per-device VRAM arena allocator over NIXL-registered device memory.
pub(crate) type DeviceArena = ArenaAllocator<NixlRegistered<DeviceStorage>>;

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

    /// Pre-registered host memory shared by owner-side staging and
    /// consumer-side destinations. Slicing is lock-free amortized via the
    /// `offset_allocator`-backed [`ArenaAllocator`] from dynamo-memory.
    host_arena: HostArena,

    /// Per-device VRAM arenas. Lazy-created on first
    /// [`Self::device_arena`] call. Keyed by CUDA device ordinal.
    device_arenas: DashMap<u32, Arc<DeviceArena>>,

    /// Default size for lazily-created device arenas.
    device_arena_bytes: usize,
    /// Page size used for arena allocation (host + device).
    page_size: usize,
}

impl NixlEndpoint {
    /// Create a UCX-backed endpoint named `velo-<worker_id>` with default
    /// arena sizes.
    pub(crate) fn create(worker_id: WorkerId) -> Result<Arc<Self>> {
        Self::create_with_config(worker_id, NixlEndpointConfig::default())
    }

    /// Create a UCX-backed endpoint with caller-supplied arena sizing.
    pub(crate) fn create_with_config(
        worker_id: WorkerId,
        config: NixlEndpointConfig,
    ) -> Result<Arc<Self>> {
        let agent_name = format!("velo-{worker_id}");
        let (agent, _backend, opt_args) = velo_nixl::agent_with_ucx(&agent_name)
            .context("velo-nixl: failed to create UCX-backed agent")?;
        // Note: we deliberately do *not* cache `agent.get_local_md()` here.
        // libnixl's local-MD blob is a snapshot of the agent's current
        // registrations; the handshake handler calls `get_local_md()` on
        // every request to pick up the latest registrations.
        // `_backend` drops here — `Backend` has no `Drop` impl, and the agent
        // retains the underlying C-side backend handle in `AgentInner.backends`.

        let host_arena =
            build_host_arena(&agent, &opt_args, config.host_arena_bytes, config.page_size)
                .context("velo-nixl: failed to build host arena")?;

        Ok(Arc::new(Self {
            agent,
            agent_name,
            opt_args: SendSyncOptArgs(opt_args),
            remote_md_loaded: DashMap::new(),
            host_arena,
            device_arenas: DashMap::new(),
            device_arena_bytes: config.device_arena_bytes,
            page_size: config.page_size,
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

    /// Allocate a host arena slice and copy `data` into it. The returned
    /// buffer carries a NIXL descriptor with the registered base + this
    /// slice's offset/size; sending its `registered_descriptor()` over the
    /// wire is what a peer NIXL_READs from.
    ///
    /// Memcpy is unavoidable for the `register_data_pinned(&[u8])` shape
    /// since the caller's allocation isn't NIXL-registered. The win is
    /// amortizing the *registration* cost — `register_memory` is called
    /// once at endpoint creation, not once per call.
    pub(crate) fn stage_host_bytes(&self, data: &[u8]) -> Result<HostArenaBuffer> {
        let buf = self.host_arena.allocate(data.len()).map_err(|e| {
            anyhow::anyhow!(
                "velo-nixl: host arena allocation failed for {} bytes: {e}",
                data.len()
            )
        })?;
        // SAFETY: `buf` exposes a contiguous, exclusively-owned, page-aligned
        // region within the registered host arena; `data.len() == buf.size()`
        // by construction.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf.addr() as *mut u8, data.len());
        }
        Ok(buf)
    }

    /// Allocate a host arena slice for use as a NIXL_READ destination on the
    /// consumer. Unlike [`Self::stage_host_bytes`], no memcpy is performed:
    /// libnixl writes directly into the arena slice when the transfer
    /// completes.
    pub(crate) fn alloc_host_dst(&self, size: usize) -> Result<HostArenaBuffer> {
        self.host_arena.allocate(size).map_err(|e| {
            anyhow::anyhow!(
                "velo-nixl: host arena destination allocation failed for {size} bytes: {e}"
            )
        })
    }

    /// Get-or-create the VRAM arena for `device_id`. The arena is registered
    /// with NIXL once at creation; subsequent calls reuse it.
    pub(crate) fn device_arena(&self, device_id: u32) -> Result<Arc<DeviceArena>> {
        if let Some(arena) = self.device_arenas.get(&device_id) {
            return Ok(arena.clone());
        }
        let arena = Arc::new(build_device_arena(
            &self.agent,
            self.opt_args(),
            self.device_arena_bytes,
            self.page_size,
            device_id,
        )?);
        // Race-tolerant: if another caller raced and won, drop ours and use theirs.
        match self.device_arenas.entry(device_id) {
            dashmap::Entry::Occupied(slot) => Ok(slot.get().clone()),
            dashmap::Entry::Vacant(slot) => {
                slot.insert(arena.clone());
                Ok(arena)
            }
        }
    }

    /// Allocate a device arena slice and copy `data` from host memory into
    /// it via cudarc's `htod_sync_copy`. This is the owner-side primitive
    /// behind `register_data_pinned_device`.
    pub(crate) fn stage_device_bytes(
        &self,
        data: &[u8],
        device_id: u32,
    ) -> Result<DeviceArenaBuffer> {
        let arena = self.device_arena(device_id)?;
        let buf = arena.allocate(data.len()).map_err(|e| {
            anyhow::anyhow!(
                "velo-nixl: device arena allocation failed for {} bytes (dev={device_id}): {e}",
                data.len()
            )
        })?;
        // We perform a raw cudaMemcpy here rather than going through cudarc's
        // typed slice API because the arena slice is identified by its raw
        // device pointer; we don't need a typed `CudaSlice<u8>` view.
        // SAFETY: `buf.addr()` is a valid CUDA device pointer for `data.len()`
        // bytes (sub-slice of the registered DeviceStorage); `data.as_ptr()`
        // is valid host memory for `data.len()` bytes.
        let rc = unsafe {
            cuda_memcpy_h2d(
                buf.addr() as *mut std::ffi::c_void,
                data.as_ptr() as *const std::ffi::c_void,
                data.len(),
            )
        };
        if rc != 0 {
            anyhow::bail!(
                "velo-nixl: cudaMemcpy H2D failed (rc={rc}) for {} bytes on dev={device_id}",
                data.len()
            );
        }
        Ok(buf)
    }

    /// Allocate a device arena slice for use as a NIXL_READ destination.
    pub(crate) fn alloc_device_dst(
        &self,
        size: usize,
        device_id: u32,
    ) -> Result<DeviceArenaBuffer> {
        let arena = self.device_arena(device_id)?;
        arena.allocate(size).map_err(|e| {
            anyhow::anyhow!(
                "velo-nixl: device arena destination allocation failed ({size} bytes, dev={device_id}): {e}"
            )
        })
    }
}

fn build_host_arena(
    agent: &velo_nixl::Agent,
    opt_args: &velo_nixl::OptArgs,
    size: usize,
    page_size: usize,
) -> Result<HostArena> {
    let storage = SystemStorage::new(size)
        .with_context(|| format!("velo-nixl: SystemStorage::new({size}) for host arena"))?;
    let registered = register_with_nixl(storage, agent, Some(opt_args))
        .map_err(|_| anyhow::anyhow!("velo-nixl: register host arena failed"))?;
    ArenaAllocator::new(registered, page_size).context("velo-nixl: ArenaAllocator::new (host)")
}

fn build_device_arena(
    agent: &velo_nixl::Agent,
    opt_args: &velo_nixl::OptArgs,
    size: usize,
    page_size: usize,
    device_id: u32,
) -> Result<DeviceArena> {
    let storage = DeviceStorage::new(size, device_id)
        .with_context(|| format!("velo-nixl: cudaMalloc({size}, dev={device_id}) failed"))?;
    let registered = register_with_nixl(storage, agent, Some(opt_args)).map_err(|_| {
        anyhow::anyhow!(
            "velo-nixl: register VRAM arena on device {device_id} failed \
             (UCX likely lacks CUDA support — verify with `ucx_info -d`)"
        )
    })?;
    ArenaAllocator::new(registered, page_size)
        .with_context(|| format!("velo-nixl: ArenaAllocator::new (device {device_id})"))
}

// Raw cudaMemcpy H2D forward decl — we need it for arena staging from
// host slices without pulling typed cudarc into hot paths.
//
// `kind = 1` is `cudaMemcpyHostToDevice`. `#[link(name = "cudart")]` makes
// the cudart dependency explicit at this binding site rather than relying
// on transitive linkage via dynamo-memory → cudarc — keeps this resilient
// if the dep tree shifts and removes the silent-link-failure risk.
#[link(name = "cudart")]
unsafe extern "C" {
    #[link_name = "cudaMemcpy"]
    fn cudaMemcpy(
        dst: *mut std::ffi::c_void,
        src: *const std::ffi::c_void,
        count: usize,
        kind: i32,
    ) -> i32;
}

const CUDA_MEMCPY_HOST_TO_DEVICE: i32 = 1;

unsafe fn cuda_memcpy_h2d(
    dst: *mut std::ffi::c_void,
    src: *const std::ffi::c_void,
    count: usize,
) -> i32 {
    unsafe { cudaMemcpy(dst, src, count, CUDA_MEMCPY_HOST_TO_DEVICE) }
}
