// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Slot bodies a peer can read with an RDMA GET.
//!
//! A [`PinnedSlot`] is two things that must not drift apart: the *staging* that
//! keeps some registered bytes alive, and the *remote description* of where
//! those bytes are. PR #40 kept them as three independent fields on the slot —
//! a mode flag, an optional descriptor, an optional buffer — which made every
//! combination representable and only one of them meaningful. Here the staging
//! owns the description, so a slot that exists has both or neither.
//!
//! # A remote description is not a lifetime
//!
//! Holding a [`PinnedRemote`] does not keep anything alive; it says where bytes
//! *currently* live. What keeps them alive is the staging beside it — a pool
//! [`PinnedBuf`] whose drop returns the space, or an [`ExternalSlice`]'s
//! in-flight guard, which is what makes a caller's
//! [`RegionGuard::unregister`](crate::rendezvous::rdma::RegionGuard::unregister)
//! wait for the anchors staged inside their region rather than pulling the
//! registration out from under them.
//!
//! # External memory is read through pointers, never references
//!
//! The Phase-2 registration contract for
//! [`register_external_memory`](crate::Velo::register_external_memory) forbids
//! *any* Rust reference into a registered range: a peer holding the region's key
//! may write to it at any moment (UCP's `prot` field is dead code), which
//! contradicts what a shared reference promises and what a mutable one claims
//! exclusively. So the chunked fallback over an external slice never forms a
//! `&[u8]`. It copies with [`std::ptr::copy_nonoverlapping`] into a fresh
//! [`BytesMut`] per chunk — a transient raw read whose result is velo's own
//! memory, which is exactly what the caller of `get_chunk` is entitled to hold.
//!
//! A pool [`PinnedBuf`] is different only in who owns the pages: velo does, and
//! its `Deref` already carries the trust-domain caveat, so the pool path reads
//! through the slice it is designed to expose.
//!
//! # Reads re-check that the region is still there
//!
//! An external slice holds a [`RegionWatch`] and refuses every read once the
//! region is deregistered. That is not belt-and-braces. A region's
//! deregistration drains its in-flight count under a *bounded* budget and
//! unmaps regardless when the budget runs out
//! ([`Deregistered::DrainTimedOut`](crate::rendezvous::rdma::Deregistered)), at
//! which point the latch closes and the caller is entitled to free the memory.
//! The in-flight guard makes that outcome unlikely; the watch is what makes the
//! read *safe* when it happens anyway, because a refused chunk is an error the
//! consumer retries and a raw read of freed pages is not.

use bytes::{Bytes, BytesMut};
use velo_ext::InFlightGuard;

use super::descriptor::{DescriptorBackend, RdmaDescriptor};
use super::rdma::{PinnedBuf, RegionWatch};
use super::store::StageMode;

/// Where a peer would address some staged bytes, and with what key.
///
/// Everything an [`RdmaDescriptor`] needs, held in the shape the store keeps it
/// so the descriptor is built at acquire time and never cached on the wire.
#[derive(Clone, Debug)]
pub(crate) struct PinnedRemote {
    /// Which provider's key material [`packed_key`](Self::packed_key) holds.
    pub backend: DescriptorBackend,
    /// Absolute address of the first staged byte in this process.
    pub addr: u64,
    /// Length of the staged range.
    pub len: u64,
    /// Generation of the registration behind `addr`.
    pub generation: u64,
    /// The backend's packed key covering `addr`.
    pub packed_key: Bytes,
}

/// A slice of memory a caller registered and still owns.
///
/// The guard and the watch are the whole of its lifecycle story: the guard
/// makes `unregister` wait, and the watch makes a read after a forced unmap
/// refuse rather than touch freed pages.
pub(crate) struct ExternalSlice {
    /// Holds the region's drain open for as long as this slot is staged.
    /// Released when the slot is freed, which is what
    /// `RegionGuard::unregister` waits for.
    _in_flight: InFlightGuard,
    /// Observes the registration. Consulted before every read.
    watch: RegionWatch,
    /// First staged byte. A raw address deliberately: see the module docs.
    ptr: usize,
    /// Staged length.
    len: usize,
}

/// How some staged bytes are kept alive.
pub(crate) enum PinnedStaging {
    /// Cut from the arena pool. Velo owns the pages; dropping it returns the
    /// space and leaves the arena registered.
    Pool(PinnedBuf),
    /// A range inside memory a caller registered and still owns.
    External(ExternalSlice),
}

/// Staged data that lives in registered memory.
pub(crate) struct PinnedSlot {
    staging: PinnedStaging,
    remote: PinnedRemote,
}

impl PinnedSlot {
    /// Stage into pool memory the caller has already filled.
    pub(crate) fn from_pool(buf: PinnedBuf, backend: DescriptorBackend) -> Self {
        let r = buf.remote();
        Self {
            remote: PinnedRemote {
                backend,
                addr: r.addr,
                len: r.len,
                generation: r.generation,
                packed_key: r.packed_key,
            },
            staging: PinnedStaging::Pool(buf),
        }
    }

    /// Stage a range of a caller-registered region, zero-copy.
    ///
    /// `in_flight` must have been acquired from that region's own accounting
    /// *before* the region was checked for liveness — the guard is what makes a
    /// concurrent `unregister` wait, and acquiring it afterwards would be a
    /// check-then-act race with nothing on the other side to lose it.
    pub(crate) fn from_region(
        in_flight: InFlightGuard,
        watch: RegionWatch,
        backend: DescriptorBackend,
        addr: u64,
        len: u64,
        generation: u64,
        packed_key: Bytes,
    ) -> Self {
        Self {
            remote: PinnedRemote {
                backend,
                addr,
                len,
                generation,
                packed_key,
            },
            staging: PinnedStaging::External(ExternalSlice {
                _in_flight: in_flight,
                watch,
                ptr: addr as usize,
                len: len as usize,
            }),
        }
    }

    /// Staged length in bytes.
    pub(crate) fn len(&self) -> u64 {
        self.remote.len
    }

    /// Whether the memory behind this slot is still registered.
    ///
    /// Always true for pool staging: an arena outlives every suballocation cut
    /// from it, and the registry sweep is the only thing that unmaps one.
    pub(crate) fn is_live(&self) -> bool {
        match &self.staging {
            PinnedStaging::Pool(_) => true,
            PinnedStaging::External(slice) => !slice.watch.is_deregistered(),
        }
    }

    /// The wire descriptor for the whole staged range, or `None` if the memory
    /// behind it is gone.
    pub(crate) fn descriptor(&self) -> Option<RdmaDescriptor> {
        if !self.is_live() {
            return None;
        }
        Some(RdmaDescriptor {
            backend: self.remote.backend,
            generation: self.remote.generation,
            addr: self.remote.addr,
            len: self.remote.len,
            packed_key: self.remote.packed_key.clone(),
        })
    }

    /// Which backend serves this slot, for matching a consumer's offer.
    pub(crate) fn backend(&self) -> DescriptorBackend {
        self.remote.backend
    }

    /// Copy `len` staged bytes starting at `offset` into fresh memory.
    ///
    /// `None` if the range falls outside the slot or the region behind it has
    /// been deregistered. The copy is the point: what comes back is velo's own
    /// buffer, so the caller may hold it for as long as it likes without any
    /// relationship to the registration it came from.
    pub(crate) fn read_at(&self, offset: u64, len: usize) -> Option<Bytes> {
        if len == 0 {
            return Some(Bytes::new());
        }
        let end = offset.checked_add(len as u64)?;
        if end > self.remote.len {
            return None;
        }
        match &self.staging {
            PinnedStaging::Pool(buf) => {
                let start = usize::try_from(offset).ok()?;
                let stop = start.checked_add(len)?;
                buf.get(start..stop).map(Bytes::copy_from_slice)
            }
            PinnedStaging::External(slice) => {
                // Checked *before* the read, and the in-flight guard this slot
                // holds is what keeps a deregistration from completing between
                // the two. See the module docs for what remains and why it is
                // bounded by D8's documented drain-timeout risk.
                if slice.watch.is_deregistered() {
                    return None;
                }
                let start = usize::try_from(offset).ok()?;
                let stop = start.checked_add(len)?;
                if stop > slice.len {
                    return None;
                }
                let mut out = BytesMut::zeroed(len);
                // SAFETY: `ptr + start .. ptr + stop` lies inside the range the
                // caller registered — `stop <= slice.len`, checked above — and
                // that range is still registered, because this slot holds an
                // in-flight guard on the region and the watch above says no
                // deregistration has completed. `out` is a fresh allocation of
                // exactly `len` bytes and cannot overlap it. No reference into
                // the registered range is formed: the read is a raw copy out,
                // which is what the registration contract requires of every
                // access to caller-owned registered memory.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        (slice.ptr as *const u8).add(start),
                        out.as_mut_ptr(),
                        len,
                    );
                }
                Some(out.freeze())
            }
        }
    }

    /// Copy the whole staged range out.
    pub(crate) fn to_bytes(&self) -> Option<Bytes> {
        self.read_at(0, usize::try_from(self.remote.len).ok()?)
    }

    /// Reporting mode for [`DataMetadata`](super::protocol::DataMetadata).
    pub(crate) fn stage_mode(&self) -> StageMode {
        StageMode::Pinned
    }
}

impl std::fmt::Debug for PinnedSlot {
    /// Never prints the staged bytes: these are routinely megabytes, and a
    /// slot's identity is its range, not its contents.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let staging = match &self.staging {
            PinnedStaging::Pool(_) => "pool",
            PinnedStaging::External(_) => "external",
        };
        f.debug_struct("PinnedSlot")
            .field("staging", &staging)
            .field("addr", &self.remote.addr)
            .field("len", &self.remote.len)
            .field("generation", &self.remote.generation)
            .field("live", &self.is_live())
            .finish()
    }
}
