// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`RendezvousWrite`] trait for pulling rendezvous data into explicit memory locations.
//!
//! Provides implementations for `&mut [u8]` (fixed-size) and [`BytesMut`] (auto-resizing).

use anyhow::Result;
use bytes::BytesMut;

/// Trait for destinations that can receive rendezvous data chunk-by-chunk.
///
/// Each chunk arrives with an offset indicating where it belongs in the
/// contiguous output. Implementations must handle writing the chunk at
/// the correct position.
///
/// For Phase 2 NIXL RDMA, this trait enables writing directly into
/// RDMA-registered memory regions.
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

#[cfg(test)]
mod tests {
    use super::*;

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
