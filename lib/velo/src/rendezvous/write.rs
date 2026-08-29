// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`RendezvousWrite`] trait for pulling rendezvous data into explicit memory locations.
//!
//! Provides implementations for `&mut [u8]` (fixed-size), [`BytesMut`] and
//! `Vec<u8>` (auto-resizing), and — where the RDMA path is available —
//! [`PinnedWriter`], a destination a peer's NIC can write into directly.

use anyhow::Result;
use bytes::BytesMut;

/// Trait for destinations that can receive rendezvous data chunk-by-chunk.
///
/// Each chunk arrives with an offset indicating where it belongs in the
/// contiguous output. Implementations must handle writing the chunk at
/// the correct position.
pub trait RendezvousWrite {
    /// Write a chunk at the given byte offset.
    ///
    /// Called once per chunk as it arrives during a `get_into()` operation.
    /// `offset` is the byte position within the full payload.
    fn write_chunk(&mut self, offset: usize, data: &[u8]) -> Result<()>;

    /// Total capacity available in this destination (in bytes).
    ///
    /// Used to validate that the destination is large enough to hold
    /// the full payload before the transfer begins.
    fn capacity(&self) -> usize;

    /// Where a remote NIC could write into this destination, if anywhere.
    ///
    /// Defaulted to `None`, and every ordinary destination keeps the default: a
    /// `Vec<u8>` is not registered memory and cannot become registered memory
    /// by being asked nicely. Those destinations still benefit from the RDMA
    /// path — `get_into` reads into a pooled buffer and copies once, which is
    /// one memcpy against several chunk round trips — they just do not get the
    /// zero-copy version.
    ///
    /// [`PinnedWriter`] is the destination that returns `Some`. There is
    /// deliberately no public constructor for [`RdmaDestination`]: a valid one
    /// names a region this instance has registered with its RDMA backend, which
    /// only velo can arrange. An out-of-tree implementor keeps the default and
    /// loses nothing but the last copy.
    ///
    /// Takes `&mut self` because handing out a destination the NIC will write
    /// into must exclude every other writer for the duration, and `&mut` is the
    /// only thing that says so in a signature.
    fn rdma_destination(&mut self) -> Option<RdmaDestination<'_>> {
        None
    }
}

/// A registered range a remote NIC may write into.
///
/// Names a region by the id the local RDMA backend issued for it, plus an
/// offset inside that region. Both are opaque numbers here; the backend
/// bounds-checks them against the registration it actually holds, so a
/// destination that has gone stale is refused there rather than trusted here.
///
/// The lifetime is the point: it borrows the destination mutably for as long as
/// it exists, so the buffer cannot be read or written by anything else while a
/// transfer into it is outstanding.
pub struct RdmaDestination<'a> {
    region_id: u64,
    offset: u64,
    capacity: u64,
    /// Keeps the destination's suballocation out of the pool's free list until
    /// the *backend* reports the transfer finished.
    ///
    /// Without it, a caller that drops its `get_into` future would return the
    /// granules while the NIC was still writing into them, and the next
    /// allocation would be handed memory a cancelled transfer is about to
    /// overwrite. The transfer takes this out of the destination before it is
    /// submitted, so what the caller drops is no longer what keeps the space
    /// reserved.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    hold: Option<super::rdma::TransferHold>,
    /// Ties this destination to the `&mut` borrow it came from. The borrow is
    /// what excludes other *local* writers for as long as the destination
    /// exists; the hold above is what survives the borrow ending.
    marker: std::marker::PhantomData<&'a mut [u8]>,
}

impl RdmaDestination<'_> {
    /// Describe a registered destination.
    ///
    /// `pub(crate)`: `region_id` must name a region this instance registered
    /// with its backend, and only velo can arrange that. A public constructor
    /// would let a caller hand the backend a number it made up, which the
    /// backend would refuse — but only after the acquire had already committed
    /// to the RDMA path.
    /// Describe a registered destination, and hand the transfer the
    /// reservation that keeps it out of the pool's free list.
    ///
    /// There is deliberately no unheld constructor: every destination velo
    /// builds is one an outstanding transfer must be able to outlive, and a
    /// second constructor without that argument would be the easy way to
    /// reintroduce the recycle-under-the-NIC bug.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn held(
        region_id: u64,
        offset: u64,
        capacity: u64,
        hold: super::rdma::TransferHold,
    ) -> Self {
        Self {
            region_id,
            offset,
            capacity,
            hold: Some(hold),
            marker: std::marker::PhantomData,
        }
    }

    /// Take the reservation, to move it into the transfer.
    ///
    /// Called once, immediately before submitting: from then on the transfer
    /// owns the reservation and the destination is only a description.
    #[cfg(all(target_os = "linux", feature = "ucx"))]
    pub(crate) fn take_hold(&mut self) -> Option<super::rdma::TransferHold> {
        self.hold.take()
    }

    /// The local backend's id for the region this destination lives in.
    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    /// Offset of the destination inside that region, measured from the pointer
    /// that was registered.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// How many bytes may be written from [`offset`](Self::offset).
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
}

impl std::fmt::Debug for RdmaDestination<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaDestination")
            .field("region_id", &self.region_id)
            .field("offset", &self.offset)
            .field("capacity", &self.capacity)
            .finish()
    }
}

impl RendezvousWrite for &mut [u8] {
    fn write_chunk(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        let end = offset + data.len();
        if end > self.len() {
            anyhow::bail!(
                "write_chunk out of bounds: offset={offset}, len={}, capacity={}",
                data.len(),
                self.len()
            );
        }
        self[offset..end].copy_from_slice(data);
        Ok(())
    }

    fn capacity(&self) -> usize {
        self.len()
    }
}

impl RendezvousWrite for BytesMut {
    fn write_chunk(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        let end = offset + data.len();
        if end > self.len() {
            self.resize(end, 0);
        }
        self[offset..end].copy_from_slice(data);
        Ok(())
    }

    fn capacity(&self) -> usize {
        BytesMut::capacity(self)
    }
}

/// Wrapper to implement [`RendezvousWrite`] for a `Vec<u8>`.
impl RendezvousWrite for Vec<u8> {
    fn write_chunk(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        let end = offset + data.len();
        if end > self.len() {
            self.resize(end, 0);
        }
        self[offset..end].copy_from_slice(data);
        Ok(())
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }
}

/// A [`RendezvousWrite`] destination cut from the RDMA pool, so a
/// `get_into` lands the bytes without a copy.
///
/// Obtained from [`Velo::alloc_pinned_writer`](crate::Velo::alloc_pinned_writer).
/// It is the only destination that answers
/// [`rdma_destination`](RendezvousWrite::rdma_destination) with `Some`, and it
/// still implements the chunked path, so the same value works whichever way the
/// owner answers.
///
/// Holding one holds pool space; drop it or take the buffer with
/// [`into_inner`](Self::into_inner) once the bytes have been consumed.
///
/// # Dropping it does not cancel a transfer into it
///
/// If a `get_into` future is dropped while its transfer is outstanding, the
/// transfer still completes — that is UCX's contract, not a choice — and the
/// destination's pool space stays reserved until it does. What the writer holds
/// is one claim among two; releasing it early is safe and simply does not free
/// anything yet. The bytes may still change after the future is dropped, so a
/// writer recovered from a cancelled transfer must be treated as having
/// unspecified contents.
#[cfg(all(target_os = "linux", feature = "ucx"))]
pub struct PinnedWriter {
    buf: super::rdma::PinnedBuf,
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl PinnedWriter {
    pub(crate) fn new(buf: super::rdma::PinnedBuf) -> Self {
        Self { buf }
    }

    /// Length of the destination in bytes.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the destination is empty. Never true: the pool refuses
    /// zero-length allocations.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The bytes, as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// Take the underlying registered buffer.
    pub fn into_inner(self) -> super::rdma::PinnedBuf {
        self.buf
    }
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl std::ops::Deref for PinnedWriter {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf
    }
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl std::fmt::Debug for PinnedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedWriter")
            .field("buf", &self.buf)
            .finish()
    }
}

#[cfg(all(target_os = "linux", feature = "ucx"))]
impl RendezvousWrite for PinnedWriter {
    /// Fixed-size, like `&mut [u8]`: the destination is a pool allocation and
    /// cannot grow, so a chunk past the end is an error rather than a resize.
    fn write_chunk(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        let end = offset
            .checked_add(data.len())
            .ok_or_else(|| anyhow::anyhow!("write_chunk offset overflow"))?;
        if end > self.buf.len() {
            anyhow::bail!(
                "write_chunk out of bounds: offset={offset}, len={}, capacity={}",
                data.len(),
                self.buf.len()
            );
        }
        self.buf[offset..end].copy_from_slice(data);
        Ok(())
    }

    fn capacity(&self) -> usize {
        self.buf.len()
    }

    fn rdma_destination(&mut self) -> Option<RdmaDestination<'_>> {
        Some(RdmaDestination::held(
            self.buf.backend_region_id(),
            self.buf.arena_offset(),
            self.buf.len() as u64,
            // The transfer's own reservation. Dropping this writer while a
            // transfer into it is outstanding therefore returns *this* handle's
            // claim and nothing else; the space comes back when the backend
            // finishes.
            self.buf.hold(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defaulted capability really is defaulted: an ordinary destination
    /// answers `None`, which is what routes it to the copy path instead of
    /// handing the NIC an address it has no business writing to.
    #[test]
    fn ordinary_destinations_offer_no_rdma_destination() {
        let mut buf = vec![0u8; 16];
        assert!(buf.rdma_destination().is_none());

        let mut bytes = BytesMut::with_capacity(16);
        assert!(bytes.rdma_destination().is_none());

        let mut owned = vec![0u8; 16];
        let mut slice: &mut [u8] = &mut owned;
        assert!(slice.rdma_destination().is_none());
    }

    #[test]
    fn test_slice_write_chunk() {
        let mut buf = vec![0u8; 16];
        let mut slice: &mut [u8] = &mut buf;
        slice.write_chunk(0, &[1, 2, 3, 4]).unwrap();
        slice.write_chunk(4, &[5, 6, 7, 8]).unwrap();
        assert_eq!(&buf[..8], &[1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_slice_write_out_of_bounds() {
        let mut buf = vec![0u8; 4];
        let mut slice: &mut [u8] = &mut buf;
        assert!(slice.write_chunk(2, &[1, 2, 3]).is_err());
    }

    #[test]
    fn test_bytesmut_write_chunk_auto_resize() {
        let mut buf = BytesMut::with_capacity(4);
        buf.resize(4, 0);
        buf.write_chunk(0, &[1, 2]).unwrap();
        // Auto-resize beyond initial len
        buf.write_chunk(4, &[5, 6, 7, 8]).unwrap();
        assert_eq!(&buf[..], &[1, 2, 0, 0, 5, 6, 7, 8]);
    }

    #[test]
    fn test_vec_write_chunk_auto_resize() {
        let mut buf = vec![0u8; 4];
        buf.write_chunk(0, &[1, 2]).unwrap();
        buf.write_chunk(4, &[5, 6]).unwrap();
        assert_eq!(&buf, &[1, 2, 0, 0, 5, 6]);
    }
}
