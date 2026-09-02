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
//! # Reads are ordered against the region going away
//!
//! An external slice holds a [`RegionWatch`] and reads through
//! [`RegionWatch::with_live`], which takes the region's copy gate, checks the
//! deregistration latch under it, and performs the copy before releasing.
//!
//! The in-flight guard is not enough on its own, and it is worth being exact
//! about why. A deregistration drains that count under a *bounded* budget and
//! unmaps regardless when the budget runs out
//! ([`Deregistered::DrainTimedOut`](crate::rendezvous::rdma::Deregistered)), so
//! the guard can still be outstanding when the latch closes — and the latch is
//! the moment the caller is told it may free the memory. A bare
//! `is_deregistered()` check followed by a copy would be a check-then-act
//! against exactly that event.
//!
//! Under the gate the two orderings that matter both hold: a copy already in
//! progress delays the latch, and a copy that starts after it sees the flag and
//! refuses. A refused chunk is an error the consumer retries; a raw read of
//! freed pages is not.
//!
//! The gate is taken per [`GATE_CHUNK`], not per request. A caller may anchor
//! gigabytes, and a single acquisition spanning the whole range would park a
//! deregistration — and the tokio worker running it — for the length of that
//! copy. Chunking keeps the bound the region's own documentation claims: one
//! chunk, not one anchor. A copy that finds the latch closed part-way through
//! discards what it has and refuses, which is what it would have done had the
//! latch closed one chunk earlier.

use bytes::{Bytes, BytesMut};
use velo_ext::InFlightGuard;

use super::descriptor::{DescriptorBackend, RdmaDescriptor};
use super::rdma::{PinnedBuf, RegionWatch};
use super::store::{DEFAULT_CHUNK_SIZE, StageMode};

/// The most bytes copied out of an external region under a single acquisition
/// of its copy gate.
///
/// The gate blocks a deregistration's latch, so the hold time is a bound on how
/// long `RegionGuard::deregistered` can be delayed by a reader — and on how long
/// a tokio worker sits in a blocking `write()` acquire. Matching the chunk size
/// the chunked path already serves means the worst case here is the worst case
/// that path already has.
const GATE_CHUNK: usize = DEFAULT_CHUNK_SIZE as usize;

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
    /// Always true for pool staging, and the reason is the same one arena
    /// reclamation rests on rather than an accident of who unmaps what. An arena
    /// is unmapped — by the shutdown sweep or by the periodic reclaim — only
    /// when `live() == 0`, meaning no suballocation exists. A `Pool` slot holds
    /// a [`PinnedBuf`], which *is* a suballocation, for its whole life. So while
    /// this slot exists its arena cannot be reclaimed, and the descriptor it
    /// hands out cannot outlive the registration behind it.
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
    ///
    /// External memory is copied in [`GATE_CHUNK`]-sized pieces, each under its
    /// own acquisition of the region's copy gate — see the module docs for what
    /// that bound is worth.
    pub(crate) fn read_at(&self, offset: u64, len: usize) -> Option<Bytes> {
        if len == 0 {
            return Some(Bytes::new());
        }
        let end = offset.checked_add(len as u64)?;
        if end > self.remote.len {
            return None;
        }
        let start = usize::try_from(offset).ok()?;
        let stop = start.checked_add(len)?;

        match &self.staging {
            PinnedStaging::Pool(buf) => buf.get(start..stop).map(Bytes::copy_from_slice),
            PinnedStaging::External(slice) => {
                if stop > slice.len {
                    return None;
                }
                let mut out = BytesMut::zeroed(len);
                let mut done = 0usize;
                while done < len {
                    let take = (len - done).min(GATE_CHUNK);
                    let from = start + done;
                    // One gate acquisition per chunk. The check and the copy
                    // are one step — splitting them would be a check-then-act
                    // against the one event that licenses the owner of this
                    // memory to free it — but the *whole* range is deliberately
                    // not one step: a caller may anchor gigabytes, and holding
                    // the gate across all of it would park the deregistration
                    // (and the tokio worker running it) for the length of the
                    // copy rather than for the length of a chunk.
                    //
                    // Refusing part-way is correct rather than merely
                    // tolerable: the partial buffer is dropped and the caller
                    // gets `None`, which is exactly what it would have got had
                    // the latch closed one chunk earlier.
                    let copied = slice.watch.with_live(|| {
                        // SAFETY: three facts, and the gate ties them together.
                        //
                        // * **In range.** `slice.ptr` and `slice.len` describe
                        //   the sub-range this anchor stages, which
                        //   `register_data_in_region` established lies inside
                        //   the registration: it checks `range.end` against
                        //   `RegionGuard::len` — the length the caller asked to
                        //   register — and derives the pointer from
                        //   `RegionGuard::addr`, the pointer that was
                        //   registered. `stop <= slice.len` is checked above
                        //   and `from + take <= stop` by construction, so this
                        //   read stays inside the anchor and therefore inside
                        //   the registration.
                        // * **Still allocated.** `with_live` runs this only
                        //   while the region's latch is closed *and* holds the
                        //   gate that `latch_deregistered` must take to close
                        //   it. So the caller cannot have been told it may free
                        //   this memory, and `RegionInner` — which the watch
                        //   keeps alive — has not dropped the buffer it owns.
                        //   Deliberately not a claim that the range is still
                        //   *registered*: a deregistration may be in flight,
                        //   and reading unpinned-but-allocated memory is fine.
                        // * **No aliasing reference.** The read is a raw copy
                        //   into `out`, a fresh allocation that cannot overlap
                        //   the source. No `&[u8]` into the registered range is
                        //   ever formed, which the registration contract
                        //   requires because a peer holding the region's key
                        //   may write to it at any moment.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                (slice.ptr as *const u8).add(from),
                                out[done..done + take].as_mut_ptr(),
                                take,
                            );
                        }
                    });
                    copied?;
                    done += take;
                }
                Some(out.freeze())
            }
        }
    }

    /// Copy the whole staged range out.
    ///
    /// Chunked exactly as [`read_at`](Self::read_at) is, which is the point:
    /// this is the path a local `get` and the shutdown demotion take, and it is
    /// the one that can be asked for the entire anchor at once.
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
