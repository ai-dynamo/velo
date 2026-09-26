// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The per-peer dirty-slot set: which slots drained since a pass last looked.
//!
//! Every consumer of a peer's streams lists its slot here on every drain, from
//! its own task, so this sits on the per-record path of every stream at once.
//! It was a bounded `flume` lane plus a per-slot `listed` flag; the lane's
//! mutex was taken per record by every consumer of the peer, and with about
//! one record per stream per batch nearly every drain found its slot unlisted
//! and paid it. A bitmap needs one `fetch_or` to list and dedups for free, and
//! it can never be full, so no drain ever has to give up its listing.
//!
//! Two levels: a bit per slot, and a summary bit per 64-slot word, so a pass
//! finds the listed slots without reading every word. Slot words are allocated
//! in chunks on first use, so a peer with a few hundred streams pays for one
//! chunk, not for [`MAX_INGRESS_SLOTS_PER_PEER`].
//!
//! **Ordering.** [`mark`](DirtySlots::mark) sets the slot bit, then the
//! summary bit; [`take`](DirtySlots::take) swaps the summary word, then each
//! slot word it names. A slot bit is never left set without a summary bit
//! behind it: whoever turns a slot bit on turns the summary bit on after it,
//! and a take that swapped the summary first leaves the slot bit for a later
//! take that will see the summary. The worst case is a summary bit whose word
//! is already empty, which costs a later take one wasted swap.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use super::MAX_INGRESS_SLOTS_PER_PEER;

const BITS: usize = 64;
const WORDS: usize = MAX_INGRESS_SLOTS_PER_PEER / BITS;
const SUMMARY_WORDS: usize = WORDS.div_ceil(BITS);
/// Slot words per lazily allocated chunk: 4,096 slots, 512 bytes.
const CHUNK_WORDS: usize = 64;
const CHUNKS: usize = WORDS.div_ceil(CHUNK_WORDS);

type Chunk = [AtomicU64; CHUNK_WORDS];

pub(crate) struct DirtySlots {
    summary: [AtomicU64; SUMMARY_WORDS],
    chunks: [OnceLock<Box<Chunk>>; CHUNKS],
}

impl DirtySlots {
    pub(crate) fn new() -> Self {
        Self {
            summary: [const { AtomicU64::new(0) }; SUMMARY_WORDS],
            chunks: [const { OnceLock::new() }; CHUNKS],
        }
    }

    fn word(&self, word: usize) -> &AtomicU64 {
        let chunk = self.chunks[word / CHUNK_WORDS]
            .get_or_init(|| Box::new([const { AtomicU64::new(0) }; CHUNK_WORDS]));
        &chunk[word % CHUNK_WORDS]
    }

    /// List `index`. `true` when this call listed it, `false` when it was
    /// already listed and not yet taken.
    ///
    /// `AcqRel`, so a caller's writes before the mark (the drain count) are
    /// visible to the take that clears it.
    pub(crate) fn mark(&self, index: u32) -> bool {
        let index = index as usize;
        debug_assert!(index < MAX_INGRESS_SLOTS_PER_PEER, "slot index {index}");
        let word = index / BITS;
        let bit = 1u64 << (index % BITS);
        if self.word(word).fetch_or(bit, Ordering::AcqRel) & bit != 0 {
            return false;
        }
        self.summary[word / BITS].fetch_or(1u64 << (word % BITS), Ordering::AcqRel);
        true
    }

    /// Clear every listed index and hand each one to `visit`, once.
    ///
    /// A mark racing this call is either taken here or left for the next
    /// take, never lost (see the module docs).
    pub(crate) fn take(&self, mut visit: impl FnMut(u32)) {
        for (s, summary) in self.summary.iter().enumerate() {
            // A plain load first: an empty word needs no RMW, and a swap of 0
            // for 0 would still take the line exclusive under consumers that
            // `fetch_or` it. A mark landing after this load is the next take's.
            if summary.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let mut words = summary.swap(0, Ordering::AcqRel);
            while words != 0 {
                let w = words.trailing_zeros() as usize;
                words &= words - 1;
                let word = s * BITS + w;
                // A summary bit implies the chunk exists: it is set only after
                // a mark allocated it.
                let Some(chunk) = self.chunks[word / CHUNK_WORDS].get() else {
                    continue;
                };
                let mut bits = chunk[word % CHUNK_WORDS].swap(0, Ordering::AcqRel);
                while bits != 0 {
                    let b = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    visit((word * BITS + b) as u32);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn taken(set: &DirtySlots) -> Vec<u32> {
        let mut out = Vec::new();
        set.take(|i| out.push(i));
        out
    }

    #[test]
    fn a_mark_lists_once_until_taken() {
        let set = DirtySlots::new();
        assert!(set.mark(7));
        assert!(!set.mark(7), "a listed slot is not listed twice");
        assert_eq!(taken(&set), vec![7]);
        assert!(taken(&set).is_empty(), "a take clears what it visits");
        assert!(set.mark(7), "a taken slot lists again");
    }

    #[test]
    fn indices_across_word_and_chunk_boundaries_come_back_in_order() {
        let set = DirtySlots::new();
        let last = (MAX_INGRESS_SLOTS_PER_PEER - 1) as u32;
        let marks = [last, 4096, 0, 63, 64, 4095, 1];
        for i in marks {
            assert!(set.mark(i));
        }
        let mut want = marks.to_vec();
        want.sort_unstable();
        assert_eq!(taken(&set), want);
    }

    #[test]
    fn an_idle_set_allocates_no_slot_words() {
        let set = DirtySlots::new();
        assert!(set.chunks.iter().all(|c| c.get().is_none()));
        set.mark(5000);
        let allocated = set.chunks.iter().filter(|c| c.get().is_some()).count();
        assert_eq!(allocated, 1, "only the chunk holding slot 5000");
    }

    /// Marks racing takes are never lost: every index marked is seen by some
    /// take, including a final one after the markers stop.
    #[test]
    fn concurrent_marks_are_never_lost() {
        let set = Arc::new(DirtySlots::new());
        let markers: Vec<_> = (0..4u32)
            .map(|t| {
                let set = Arc::clone(&set);
                std::thread::spawn(move || {
                    for round in 0..2_000u32 {
                        set.mark((t * 9_973 + round * 131) % 20_000);
                    }
                })
            })
            .collect();
        let mut seen = BTreeSet::new();
        while !markers.iter().all(|m| m.is_finished()) {
            set.take(|i| {
                seen.insert(i);
            });
        }
        for m in markers {
            m.join().unwrap();
        }
        set.take(|i| {
            seen.insert(i);
        });
        let want: BTreeSet<u32> = (0..4u32)
            .flat_map(|t| (0..2_000u32).map(move |round| (t * 9_973 + round * 131) % 20_000))
            .collect();
        assert_eq!(seen, want);
    }
}
