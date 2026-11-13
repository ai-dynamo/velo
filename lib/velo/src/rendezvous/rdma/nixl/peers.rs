// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What this agent has loaded for each peer, and what may be done next.
//!
//! NIXL keeps every region blob it loads, and drops only a whole peer. So a
//! consumer's agent holds keys for registrations the owner has since dropped.
//! A READ through one is a stale key: over tcp it reads whatever is there now,
//! and on InfiniBand it aborts the process. Two rules keep one out of use:
//!
//! 1. The owner registers each region under a device id it never uses again,
//!    and NIXL serves a READ only from an entry under the id the READ names.
//! 2. This mirror records which ids were loaded. A region is loaded once, and
//!    a READ is never posted for an id that is not in the mirror.
//!
//! A drop of a peer under a READ makes NIXL lose track of the READ, which
//! writes on. So a drop waits until nothing to the peer is in flight.

use std::collections::HashMap;

use super::key::NixlKey;

/// Where a peer stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum PeerState {
    /// Nothing loaded.
    #[default]
    Fresh,
    /// Connection info and at least one region loaded.
    Loaded,
    /// A transfer failed, after which NIXL refuses loads on that endpoint.
    /// Drop the peer before the next load.
    Suspect,
    /// A load was refused, which can leave the peer unloadable for the life of
    /// the agent. Do not touch this peer again.
    Poisoned,
}

/// What the driver must do for one READ.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admit {
    /// The region is loaded. Post.
    Ready,
    /// Load the region (and the connection info first, if `conn`), then post.
    Load {
        /// Whether the connection info is still to load.
        conn: bool,
    },
    /// Drop the peer, then ask again.
    Purge,
    /// A drop is due, and transfers to the peer are in flight. Wait for them.
    Wait,
    /// The key names a device id that is loaded with another range. An owner
    /// never reuses an id, so the key is wrong.
    Refuse,
    /// The peer is poisoned. Fail.
    Poisoned,
}

/// One peer, as this agent has loaded it.
#[derive(Debug, Default)]
pub(crate) struct Peer {
    state: PeerState,
    /// Device id to `(address, length)` of every region blob loaded.
    loaded: HashMap<u64, (u64, u64)>,
    /// READs posted to this peer and not yet over.
    in_flight: usize,
}

impl Peer {
    pub(crate) fn state(&self) -> PeerState {
        self.state
    }

    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// What a READ under `key` needs, with at most `cap` regions loaded. NIXL
    /// has no bound on what it keeps, so at the cap the peer is dropped.
    pub(crate) fn admit(&self, key: &NixlKey<'_>, cap: usize) -> Admit {
        let purge = if self.in_flight == 0 {
            Admit::Purge
        } else {
            Admit::Wait
        };
        match self.state {
            PeerState::Poisoned => Admit::Poisoned,
            PeerState::Suspect => purge,
            PeerState::Fresh => Admit::Load { conn: true },
            PeerState::Loaded => match self.loaded.get(&key.region_dev) {
                Some(&(addr, len)) if addr == key.region_addr && len == key.region_len => {
                    Admit::Ready
                }
                Some(_) => Admit::Refuse,
                None if self.loaded.len() >= cap => purge,
                None => Admit::Load { conn: false },
            },
        }
    }

    /// The region blob of `key` was loaded.
    pub(crate) fn loaded(&mut self, key: &NixlKey<'_>) {
        self.loaded
            .insert(key.region_dev, (key.region_addr, key.region_len));
        self.state = PeerState::Loaded;
    }

    /// The peer was dropped from the agent.
    pub(crate) fn purged(&mut self) {
        self.loaded.clear();
        self.state = PeerState::Fresh;
    }

    /// A transfer to the peer failed, or a post was refused.
    pub(crate) fn suspect(&mut self) {
        if self.state != PeerState::Poisoned {
            self.state = PeerState::Suspect;
        }
    }

    /// A load was refused, or a drop failed.
    pub(crate) fn poison(&mut self) {
        self.loaded.clear();
        self.state = PeerState::Poisoned;
    }

    /// A READ to the peer was posted and is running.
    pub(crate) fn began(&mut self) {
        self.in_flight += 1;
    }

    /// A READ to the peer is over. Returns whether the peer is now idle.
    pub(crate) fn ended(&mut self) -> bool {
        self.in_flight = self.in_flight.saturating_sub(1);
        self.in_flight == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(dev: u64, addr: u64, len: u64) -> NixlKey<'static> {
        NixlKey {
            region_dev: dev,
            region_addr: addr,
            region_len: len,
            conn: b"c",
            md: b"m",
        }
    }

    /// A peer with regions 1 and 2 loaded, and `busy` READs in flight.
    fn loaded(busy: usize) -> Peer {
        let mut peer = Peer::default();
        peer.loaded(&key(1, 0x1000, 64));
        peer.loaded(&key(2, 0x2000, 64));
        (0..busy).for_each(|_| peer.began());
        peer
    }

    fn suspect(busy: usize) -> Peer {
        let mut peer = loaded(busy);
        peer.suspect();
        peer
    }

    fn poisoned() -> Peer {
        let mut peer = loaded(0);
        peer.poison();
        // A later failure does not bring a poisoned peer back.
        peer.suspect();
        peer
    }

    const CONNECT: Admit = Admit::Load { conn: true };
    const LOAD: Admit = Admit::Load { conn: false };

    /// Every answer of `admit`, by the state of the peer, the key and the cap.
    #[test]
    fn admit_answers_by_state_key_and_cap() {
        use Admit::{Poisoned, Purge, Ready, Refuse, Wait};
        let (old, new) = (key(1, 0x1000, 64), key(3, 0x3000, 64));
        let (reused, moved, longer) = (key(3, 0x1000, 64), key(1, 0x2000, 64), key(1, 0x1000, 65));
        let cases = [
            (Peer::default(), old, 8, CONNECT),
            (loaded(0), old, 8, Ready),
            (loaded(0), new, 8, LOAD),
            // The owner reuses an address after it deregisters. That is the
            // common case, not a fault: no purge.
            (loaded(0), reused, 8, LOAD),
            // One id with two ranges.
            (loaded(0), moved, 8, Refuse),
            (loaded(0), longer, 8, Refuse),
            // At the cap a new region needs a purge, and a purge needs an idle
            // peer. The cap bounds what is kept, not what is read.
            (loaded(0), new, 2, Purge),
            (loaded(1), new, 2, Wait),
            (loaded(1), old, 2, Ready),
            // A loaded peer holds a region, so a cap of zero acts as one.
            (loaded(0), old, 0, Ready),
            // Purged before anything else, even for a loaded region.
            (suspect(0), old, 8, Purge),
            (suspect(1), old, 8, Wait),
            (poisoned(), old, 8, Poisoned),
        ];
        for (row, (peer, key, cap, expected)) in cases.into_iter().enumerate() {
            assert_eq!(peer.admit(&key, cap), expected, "row {row}");
        }
    }

    /// A purge empties the mirror: a region that was loaded must load again,
    /// connection info first.
    #[test]
    fn a_purged_peer_loads_from_nothing() {
        let mut peer = loaded(1);
        assert!(peer.ended());
        peer.purged();
        assert_eq!(
            peer.admit(&key(1, 0x1000, 64), 8),
            Admit::Load { conn: true }
        );
        assert_eq!(peer.state(), PeerState::Fresh);
    }
}
