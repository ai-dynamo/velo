// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The credit-return and eviction sweep task, and the per-peer floor on the
//! doorbell-driven visits it answers.
//!
//! Split out of the transport module because it reaches [`MuxCore`] through
//! two methods only, `sweep` and `visit_drained_peer`. Everything else here is
//! the floor's own bookkeeping.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use velo_ext::WorkerId;

use super::MuxCore;

/// Ceiling on an operator's [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor).
///
/// Not a tuning limit — a floor this long already means "the doorbell is off and
/// the periodic sweep owns credit return", and every value past it means the
/// same thing. It exists so `last + floor` is total arithmetic: a `Duration`
/// near the maximum would overflow the `Instant` and panic the sweep task, which
/// is a poor way to answer a misconfiguration.
const MAX_DRAIN_VISIT_FLOOR: Duration = Duration::from_secs(3600);

/// One peer's doorbell state.
struct PeerVisits {
    /// When the doorbell last walked this peer.
    last: tokio::time::Instant,
    /// Whether a deferred walk for it is already queued.
    ///
    /// Exactly one entry per peer is ever in the queue, and this is what says
    /// so. See [`DrainVisits`] for what a second one costs.
    queued: bool,
}

/// The per-peer floor on doorbell-driven visits, and the queue it defers into.
///
/// Coalescing alone leaves the visit rate a property of the traffic: a visit
/// takes the peer's wake down before it walks, so the next record drained arms
/// it again and the sweep task turns wake -> clear -> walk back to back. What
/// bounds it is this — a peer visited less than [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor)
/// ago is *not* walked and its wake is *not* cleared. It goes into the deferred
/// queue instead, and because the flag stays armed, every drain until then
/// coalesces into that one scheduled visit rather than posting another wake. So
/// deferral never loses a wake; it delays one by at most the floor.
///
/// **One queue entry per peer, and the reason is a ratchet.** A queued walk is
/// the authoritative next one, so a wake arriving while one is queued is
/// answered by it rather than queueing a second — and, past the floor or not, is
/// never walked out of band. Letting either happen leaves the queued entry
/// behind as residue, and the periodic tick supplies a steady source of them: it
/// calls `sweep_peer` on every peer, clearing the wake of one that is
/// *currently deferred*, whose consumer's next drain then re-arms and posts
/// again inside the same floor. One entry per tick, permanently, with the sweep
/// task's queue work growing to match. Measured before the bound: 64 rounds of
/// that pattern left 64 entries for a single peer, monotone, and a live probe
/// saw 59 floor-spaced walks continue after the traffic had provably stopped.
pub(super) struct DrainVisits {
    floor: Duration,
    peers: HashMap<WorkerId, PeerVisits>,
    /// Deferred walks, ordered by when they come due, at most one per peer.
    deferred: BinaryHeap<Reverse<(tokio::time::Instant, WorkerId)>>,
}

impl DrainVisits {
    pub(super) fn new(floor: Duration) -> Self {
        Self {
            floor: floor.min(MAX_DRAIN_VISIT_FLOOR),
            peers: HashMap::new(),
            deferred: BinaryHeap::new(),
        }
    }

    /// When the next deferred peer comes due, if any is waiting.
    pub(super) fn next_due(&self) -> Option<tokio::time::Instant> {
        self.deferred.peek().map(|Reverse((due, _))| *due)
    }

    /// Walks currently queued.
    ///
    /// The bound this type owes its caller is one entry per peer, so the size
    /// of the queue is the property worth asserting on and `next_due` alone
    /// cannot see it — a queue holding residue reports the same next deadline
    /// as one holding a single live entry.
    #[cfg(test)]
    pub(super) fn queued(&self) -> usize {
        self.deferred.len()
    }

    /// Answer a wake: `Some(peer)` to walk it now, `None` when the wake was
    /// deferred into a queued walk instead.
    ///
    /// A peer handed back is stamped here rather than by the caller after the
    /// walk. The invariant is what makes the floor hold: *everything this
    /// returns has already been counted as visited*, so two wakes for one peer
    /// cannot both be admitted, and the interval the floor measures is
    /// walk-start to walk-start — which is what the rate it bounds means.
    pub(super) fn admit(&mut self, peer: WorkerId, now: tokio::time::Instant) -> Option<WorkerId> {
        let Some(state) = self.peers.get_mut(&peer) else {
            self.peers.insert(
                peer,
                PeerVisits {
                    last: now,
                    queued: false,
                },
            );
            return Some(peer);
        };
        // Checked before the floor, not after. A queued walk answers this wake
        // whether or not the floor has since elapsed, and walking here instead
        // would strand that entry in the queue as residue — which is the whole
        // of the ratchet described on this type.
        if state.queued {
            return None;
        }
        if now.saturating_duration_since(state.last) < self.floor {
            state.queued = true;
            self.deferred.push(Reverse((state.last + self.floor, peer)));
            return None;
        }
        state.last = now;
        Some(peer)
    }

    /// Peers whose deferred walk has come due.
    pub(super) fn due(&mut self, now: tokio::time::Instant) -> Vec<WorkerId> {
        let mut ready = Vec::new();
        while self.next_due().is_some_and(|due| due <= now) {
            let Reverse((_, peer)) = self.deferred.pop().expect("peeked a moment ago");
            if let Some(state) = self.peers.get_mut(&peer) {
                state.queued = false;
            }
            // Back through `admit`, which is the one place the floor is decided.
            // An entry exists only for a peer `admit` has refused to walk since
            // it was queued, and its due instant is one floor past that peer's
            // last walk, so this always comes back `Some` and never re-queues.
            ready.extend(self.admit(peer, now));
        }
        ready
    }

    /// Drop per-peer state that can no longer defer anything.
    ///
    /// A last walk older than the floor admits the next wake immediately, so
    /// forgetting it changes no decision. It only keeps this map to the peers
    /// currently draining rather than to every peer the node has ever received
    /// from.
    ///
    /// A peer with a walk still queued is kept regardless of its age. Its state
    /// is what records that the walk is queued, and dropping it would let the
    /// next wake walk immediately and queue a second entry behind the one still
    /// sitting there — the residue this type exists to not accumulate.
    pub(super) fn forget_stale(&mut self, now: tokio::time::Instant) {
        let floor = self.floor;
        self.peers
            .retain(|_, state| state.queued || now.saturating_duration_since(state.last) < floor);
    }
}

/// Park until `due`, or forever when nothing is deferred.
///
/// `pending` rather than a zero-length sleep: an empty queue must leave the
/// timer arm silent, or the loop spins on a deadline that is always in the past
/// and becomes a worse version of the rate this floor exists to bound.
async fn deferred_visit_due(due: Option<tokio::time::Instant>) {
    match due {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Spawn the credit-return and eviction sweep.
///
/// Two sources, and which one does the work matters for cost. A draining
/// consumer posts its peer on the wake lane, and this reconciles **the slots
/// of that peer its pumps named** — bounded above by the drains, and bounded
/// below by [`MuxConfig::drain_visit_floor`](super::MuxConfig::drain_visit_floor), which is what keeps a consumer
/// that keeps up from turning the doorbell into a spin over the peer's slot
/// table. The ticker walks the whole table, as the backstop for what neither
/// the arrival path nor the doorbell reaches: a slot parked with nothing
/// further arriving and no consumer taking anything out, a slot whose drain
/// found the dirty lane full, and batcher eviction, which free-rides on the
/// same tick.
///
/// Before this, the ticker was the only source and ran at 500 Hz, walking every
/// slot of every peer to find the few with credit to return — work that scales
/// with peers and slots while the credit actually returned does not.
///
/// A tick does not stamp the peers it swept as visited. It could, and the cost
/// of not doing it is at most one extra doorbell walk per peer per tick period —
/// against a default interval a hundred times the floor, that is noise, and it
/// keeps the periodic path from having to report which peers it touched.
///
/// The tick does something subtler that is *not* noise, and [`DrainVisits`] is
/// where it is answered: `sweep_peer` clears the wake of every peer, including
/// one whose walk is already queued, so that peer's next drain re-arms and posts
/// a second wake inside the same floor. Deferring that wake into a second queue
/// entry is what used to leave one entry behind per tick, for good.
pub(super) fn spawn_sweep(core: &Arc<MuxCore>) {
    let weak = Arc::downgrade(core);
    let cancel = core.cancel.clone();
    let interval = core.config.credit_sweep_interval;
    let floor = core.config.drain_visit_floor;
    let drain_rx = core.drain_rx.clone();
    tokio::spawn(async move {
        enum Wake {
            Tick,
            Peer(WorkerId),
            Due,
        }
        // Non-zero by construction: `interval` panics on a zero period, and
        // `MessengerMuxTransport::new` refuses one before this task exists.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut visits = DrainVisits::new(floor);
        loop {
            // `biased` puts cancellation first, so teardown is never starved
            // by a busy lane. The remaining arms are deliberately *not* biased
            // against each other: an earlier version polled the ticker first,
            // which at a short interval left it almost always ready and starved
            // the drain arm — the event-driven path barely ran, and the periodic
            // walk did the work it was meant to replace. The deferred-visit
            // timer joins them on the same footing for the same reason.
            let deferred_until = visits.next_due();
            let wake = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                wake = async {
                    tokio::select! {
                        _ = ticker.tick() => Wake::Tick,
                        () = deferred_visit_due(deferred_until) => Wake::Due,
                        drained = drain_rx.recv_async() => match drained {
                            Ok(peer) => Wake::Peer(peer),
                            // Every sender is gone with the transport. Fall back
                            // to ticking so eviction still runs.
                            Err(_) => {
                                ticker.tick().await;
                                Wake::Tick
                            }
                        },
                    }
                } => wake,
            };
            let Some(core) = weak.upgrade() else {
                return;
            };
            let now = tokio::time::Instant::now();
            match wake {
                Wake::Tick => {
                    core.sweep();
                    visits.forget_stale(now);
                }
                Wake::Peer(peer) => {
                    if let Some(peer) = visits.admit(peer, now) {
                        core.visit_drained_peer(peer);
                    }
                }
                Wake::Due => {
                    for peer in visits.due(now) {
                        core.visit_drained_peer(peer);
                    }
                }
            }
        }
    });
}
