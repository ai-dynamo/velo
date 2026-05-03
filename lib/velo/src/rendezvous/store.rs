// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Data store: the owner-side registry of staged rendezvous data.
//!
//! [`DataStore`] holds a [`DashMap`] of [`DataSlot`] entries keyed by local ID,
//! and a [`DashMap`] of [`TransferState`] entries for active chunked transfers.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

use crate::rendezvous::protocol::DataMetadata;

/// Default chunk size for chunked transfers (512 KiB).
pub const DEFAULT_CHUNK_SIZE: u32 = 512 * 1024;

/// How data is staged in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageMode {
    /// Phase 1: plain heap bytes, served via chunked pull.
    InMemory,
    /// Phase 2 (placeholder): RDMA-pinned arena memory via dynamo-memory + NIXL.
    Pinned,
}

/// A single slot in the data store registry.
#[allow(dead_code)]
pub(crate) struct DataSlot {
    /// The staged payload.
    pub data: Bytes,
    /// How the data is stored.
    pub mode: StageMode,
    /// Reference count. Defaults to 1. Decremented by release, freed at 0.
    pub refcount: AtomicU32,
    /// Active read lock count. Prevents cleanup while transfers are in flight.
    pub read_lock_count: AtomicU32,
    /// Cached total length for metadata queries.
    pub total_len: u64,
    /// When this slot was created.
    pub created_at: Instant,
    /// Optional time-to-live. Data is eligible for reaping after this duration.
    pub ttl: Option<std::time::Duration>,
}

/// State for an active chunked transfer.
#[allow(dead_code)]
pub(crate) struct TransferState {
    /// Which DataSlot this transfer reads from.
    pub slot_local_id: u64,
    /// The lease ID associated with the read lock.
    pub lease_id: u64,
    /// Size of each chunk in bytes.
    pub chunk_size: u32,
    /// Total number of chunks.
    pub chunk_count: u32,
    /// When this transfer was created.
    pub created_at: Instant,
}

/// Options for registering data.
#[derive(Debug, Clone)]
pub struct RegisterOptions {
    /// Optional time-to-live for the staged data.
    pub ttl: Option<std::time::Duration>,
}

impl RegisterOptions {
    pub fn new() -> Self {
        Self { ttl: None }
    }

    pub fn ttl(mut self, ttl: std::time::Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl Default for RegisterOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Owner-side registry of staged rendezvous data.
///
/// Holds data slots keyed by local ID and active chunked transfer state.
/// Thread-safe via [`DashMap`] and atomic counters.
pub struct DataStore {
    /// Monotonically increasing slot ID counter. Starts at 1 (0 is reserved).
    next_id: AtomicU64,
    /// Active data slots.
    pub(crate) slots: DashMap<u64, DataSlot>,
    /// Active chunked transfers keyed by transfer_id.
    pub(crate) transfers: DashMap<u64, TransferState>,
    /// Monotonically increasing transfer ID counter.
    next_transfer_id: AtomicU64,
    /// Monotonically increasing lease ID counter.
    next_lease_id: AtomicU64,
    /// Outstanding leases: lease_id → local_id. Consumed on detach/release.
    active_leases: DashMap<u64, u64>,
}

impl DataStore {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            slots: DashMap::new(),
            transfers: DashMap::new(),
            next_transfer_id: AtomicU64::new(1),
            next_lease_id: AtomicU64::new(1),
            active_leases: DashMap::new(),
        }
    }

    /// Register data and return the local slot ID.
    pub fn register(&self, data: Bytes, opts: Option<RegisterOptions>) -> u64 {
        let local_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let total_len = data.len() as u64;
        let ttl = opts.as_ref().and_then(|o| o.ttl);
        self.slots.insert(
            local_id,
            DataSlot {
                data,
                mode: StageMode::InMemory,
                refcount: AtomicU32::new(1),
                read_lock_count: AtomicU32::new(0),
                total_len,
                created_at: Instant::now(),
                ttl,
            },
        );
        local_id
    }

    /// Query metadata for a slot (no lock acquired).
    pub fn metadata(&self, local_id: u64) -> Option<DataMetadata> {
        self.slots.get(&local_id).map(|slot| DataMetadata {
            total_len: slot.total_len,
            refcount: slot.refcount.load(Ordering::Relaxed),
            pinned: slot.mode == StageMode::Pinned,
        })
    }

    /// Acquire a read lock on a slot. Returns a lease ID, or None if the slot doesn't exist.
    pub fn acquire_read_lock(&self, local_id: u64) -> Option<u64> {
        let slot = self.slots.get(&local_id)?;
        slot.read_lock_count.fetch_add(1, Ordering::Relaxed);
        let lease_id = self.next_lease_id.fetch_add(1, Ordering::Relaxed);
        self.active_leases.insert(lease_id, local_id);
        Some(lease_id)
    }

    /// Validate and consume a lease, returning the associated local slot ID.
    ///
    /// Returns `None` if the lease is invalid or has already been consumed.
    /// Each lease can only be consumed once, preventing double-detach/release.
    pub fn consume_lease(&self, lease_id: u64) -> Option<u64> {
        self.active_leases
            .remove(&lease_id)
            .map(|(_, local_id)| local_id)
    }

    /// Release a read lock on a slot. Returns true if the slot should be freed
    /// (refcount == 0 AND read_lock_count == 0).
    ///
    /// Uses checked arithmetic: returns `false` if the count is already zero
    /// instead of underflowing.
    pub fn release_read_lock(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            let result =
                slot.read_lock_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        if v > 0 { Some(v - 1) } else { None }
                    });
            match result {
                Ok(prev) => {
                    let read_locks = prev - 1;
                    let refcount = slot.refcount.load(Ordering::Relaxed);
                    read_locks == 0 && refcount == 0
                }
                Err(_) => {
                    tracing::warn!(
                        "release_read_lock: read_lock_count already 0 for slot {local_id}"
                    );
                    false
                }
            }
        } else {
            false
        }
    }

    /// Increment the reference count for a slot.
    pub fn ref_increment(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            slot.refcount.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Decrement the reference count. Returns true if the slot should be freed
    /// (refcount == 0 AND read_lock_count == 0).
    ///
    /// Uses checked arithmetic: returns `false` if the count is already zero
    /// instead of underflowing.
    pub fn ref_decrement(&self, local_id: u64) -> bool {
        if let Some(slot) = self.slots.get(&local_id) {
            let result = slot
                .refcount
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    if v > 0 { Some(v - 1) } else { None }
                });
            match result {
                Ok(prev) => {
                    let refcount = prev - 1;
                    let read_locks = slot.read_lock_count.load(Ordering::Relaxed);
                    refcount == 0 && read_locks == 0
                }
                Err(_) => {
                    tracing::warn!("ref_decrement: refcount already 0 for slot {local_id}");
                    false
                }
            }
        } else {
            false
        }
    }

    /// Remove a slot from the registry and return its data.
    pub fn remove(&self, local_id: u64) -> Option<Bytes> {
        self.slots.remove(&local_id).map(|(_, slot)| slot.data)
    }

    /// Try to free a slot if both refcount and read_lock_count are zero.
    pub fn try_free(&self, local_id: u64) {
        // Use entry API to avoid TOCTOU: only remove if both counters are zero.
        self.slots.remove_if(&local_id, |_, slot| {
            slot.refcount.load(Ordering::Relaxed) == 0
                && slot.read_lock_count.load(Ordering::Relaxed) == 0
        });
    }

    /// Get the data bytes for a slot (for inline responses or local fast-path).
    pub fn get_data(&self, local_id: u64) -> Option<Bytes> {
        self.slots.get(&local_id).map(|slot| slot.data.clone())
    }

    /// Get the total length of data in a slot.
    pub fn get_total_len(&self, local_id: u64) -> Option<u64> {
        self.slots.get(&local_id).map(|slot| slot.total_len)
    }

    /// Create a new chunked transfer for a slot.
    /// Returns (transfer_id, chunk_size, chunk_count).
    pub fn create_transfer(
        &self,
        local_id: u64,
        lease_id: u64,
        max_chunk_size: u32,
    ) -> Option<(u64, u32, u32)> {
        let slot = self.slots.get(&local_id)?;
        let total_len = slot.total_len;
        let chunk_size = max_chunk_size.min(DEFAULT_CHUNK_SIZE);
        let chunk_count = total_len.div_ceil(chunk_size as u64) as u32;

        let transfer_id = self.next_transfer_id.fetch_add(1, Ordering::Relaxed);
        self.transfers.insert(
            transfer_id,
            TransferState {
                slot_local_id: local_id,
                lease_id,
                chunk_size,
                chunk_count,
                created_at: Instant::now(),
            },
        );

        Some((transfer_id, chunk_size, chunk_count))
    }

    /// Get a specific chunk from an active transfer.
    pub fn get_chunk(&self, transfer_id: u64, chunk_index: u32) -> Option<Bytes> {
        let transfer = self.transfers.get(&transfer_id)?;
        let slot = self.slots.get(&transfer.slot_local_id)?;

        let offset = chunk_index as u64 * transfer.chunk_size as u64;
        let end = (offset + transfer.chunk_size as u64).min(slot.total_len);

        if offset >= slot.total_len {
            return None;
        }

        Some(slot.data.slice(offset as usize..end as usize))
    }

    /// Remove a completed transfer.
    pub fn remove_transfer(&self, transfer_id: u64) {
        self.transfers.remove(&transfer_id);
    }

    /// Remove all transfers associated with a given lease ID.
    pub fn remove_transfers_by_lease(&self, lease_id: u64) {
        self.transfers.retain(|_, state| state.lease_id != lease_id);
    }
}

impl Default for DataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let store = DataStore::new();
        let data = Bytes::from(vec![1u8, 2, 3, 4]);
        let id = store.register(data.clone(), None);
        assert_eq!(id, 1);
        assert_eq!(store.get_data(id).unwrap(), data);
    }

    #[test]
    fn test_metadata() {
        let store = DataStore::new();
        let data = Bytes::from(vec![0u8; 1024]);
        let id = store.register(data, None);
        let meta = store.metadata(id).unwrap();
        assert_eq!(meta.total_len, 1024);
        assert_eq!(meta.refcount, 1);
        assert!(!meta.pinned);
    }

    #[test]
    fn test_ref_counting() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("hello"), None);

        // Initial refcount is 1
        assert_eq!(store.metadata(id).unwrap().refcount, 1);

        // Increment
        assert!(store.ref_increment(id));
        assert_eq!(store.metadata(id).unwrap().refcount, 2);

        // Decrement (not yet free: refcount=1)
        assert!(!store.ref_decrement(id));

        // Decrement (free: refcount=0, no read locks)
        assert!(store.ref_decrement(id));

        // try_free should remove it
        store.try_free(id);
        assert!(store.metadata(id).is_none());
    }

    #[test]
    fn test_read_lock_prevents_free() {
        let store = DataStore::new();
        let id = store.register(Bytes::from("data"), None);

        // Acquire read lock
        let _lease = store.acquire_read_lock(id).unwrap();

        // Decrement refcount to 0
        let should_free = store.ref_decrement(id);
        // Should NOT free because read lock is held
        assert!(!should_free);

        // Release read lock — now should free
        let should_free = store.release_read_lock(id);
        assert!(should_free);
    }

    #[test]
    fn test_chunked_transfer() {
        let store = DataStore::new();
        let data = Bytes::from(vec![0xAA; 2000]);
        let id = store.register(data, None);
        let lease_id = store.acquire_read_lock(id).unwrap();

        let (transfer_id, chunk_size, chunk_count) =
            store.create_transfer(id, lease_id, 1024).unwrap();
        assert_eq!(chunk_size, 1024);
        assert_eq!(chunk_count, 2);

        // Get chunk 0
        let chunk0 = store.get_chunk(transfer_id, 0).unwrap();
        assert_eq!(chunk0.len(), 1024);
        assert!(chunk0.iter().all(|&b| b == 0xAA));

        // Get chunk 1 (partial)
        let chunk1 = store.get_chunk(transfer_id, 1).unwrap();
        assert_eq!(chunk1.len(), 976); // 2000 - 1024
        assert!(chunk1.iter().all(|&b| b == 0xAA));

        // Chunk 2 doesn't exist
        assert!(store.get_chunk(transfer_id, 2).is_none());

        // Cleanup
        store.remove_transfer(transfer_id);
        assert!(store.transfers.get(&transfer_id).is_none());
    }
}
