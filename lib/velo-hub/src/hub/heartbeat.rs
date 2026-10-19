// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Incarnation-safe heartbeat: the hub probes each registration over velo.
//! Ported from kvbm-hub `server/heartbeat.rs`.
//!
//! Each tick snapshots the store, so every probe and failure counter is
//! tied to one `(InstanceId, Incarnation)`. A probe's outcome touches or
//! removes only that incarnation, so an outcome from an earlier
//! registration cannot refresh or evict its replacement. Probes run
//! detached and each times out after one interval, so a wedged peer does
//! not delay the others. A probe that succeeds touches the store itself:
//! done in the loop, one at a time, the touches of a large fleet against a
//! remote store would take longer than the interval. The loop keeps only
//! the failure counters and the rare remove.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, WeakVelo};

use crate::protocol::{HEARTBEAT_HANDLER, HeartbeatAck, HeartbeatRequest, HubId};
use crate::store::{HubStore, Incarnation, StoredPeer};

type FailureKey = (InstanceId, Incarnation);

#[derive(Debug)]
enum ProbeOutcome {
    /// Answered, and the store was touched.
    Ok {
        id: InstanceId,
        incarnation: Incarnation,
    },
    Failed {
        id: InstanceId,
        incarnation: Incarnation,
        reason: String,
    },
}

/// Spawn the heartbeat manager. It holds a [`WeakVelo`]: a strong clone in
/// a task that only the hub cancels would keep a dropped hub's velo alive.
/// What the heartbeat needs from the hub's config, and the hub's id, which
/// every probe carries.
#[derive(Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) hub_id: HubId,
    pub(crate) interval: Duration,
    pub(crate) max_failures: u32,
    pub(crate) step_timeout: Duration,
}

pub(crate) fn spawn(
    velo: WeakVelo,
    store: Arc<dyn HubStore>,
    settings: Settings,
    cancel: CancellationToken,
    in_flight: Arc<InFlight>,
) -> JoinHandle<()> {
    let Settings {
        interval,
        max_failures,
        step_timeout,
        ..
    } = settings;
    let (tx, mut rx) = mpsc::unbounded_channel::<ProbeOutcome>();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // A failure outcome can await a store remove, so the loop can miss
        // ticks. Replaying them (tokio's default) would fan out a full round
        // of probes per missed tick at once.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        let mut failures: HashMap<FailureKey, u32> = HashMap::new();
        let mut seq: u64 = 0;
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    let Some(velo) = velo.upgrade() else { return };
                    seq = seq.wrapping_add(1);
                    let mut registrations = store.peers();
                    retain_current_failures(&mut failures, &registrations);
                    in_flight.skip(&mut registrations);
                    fan_out(&velo, &store, registrations, seq, &settings, &tx);
                }
                Some(outcome) = rx.recv() => {
                    handle_outcome(outcome, store.as_ref(), &mut failures, max_failures, step_timeout).await;
                }
            }
        }
    })
}

/// Owners whose registration transaction is running. The heartbeat does
/// not probe them: the registrant has no answer yet, so it cannot tell its
/// new registration from one it never got, and a slow but conforming
/// transaction (an `on_registered` takes up to `step_timeout`) would let
/// the probes evict the entry before the response arrives.
#[derive(Default)]
pub(crate) struct InFlight(parking_lot::Mutex<HashSet<InstanceId>>);

impl InFlight {
    /// Mark `owner` in flight until the guard drops.
    pub(crate) fn enter(&self, owner: InstanceId) -> InFlightGuard<'_> {
        self.0.lock().insert(owner);
        InFlightGuard { set: self, owner }
    }

    fn skip(&self, registrations: &mut Vec<StoredPeer>) {
        let set = self.0.lock();
        if !set.is_empty() {
            registrations.retain(|stored| !set.contains(&stored.peer.instance_id()));
        }
    }
}

/// See [`InFlight::enter`].
pub(crate) struct InFlightGuard<'a> {
    set: &'a InFlight,
    owner: InstanceId,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.set.0.lock().remove(&self.owner);
    }
}

fn retain_current_failures(failures: &mut HashMap<FailureKey, u32>, registrations: &[StoredPeer]) {
    if failures.is_empty() {
        return;
    }
    // A set, not a scan per counter: in a partition every registration
    // fails, and a scan would cost failures x registrations each tick.
    let current: HashSet<FailureKey> = registrations
        .iter()
        .map(|stored| (stored.peer.instance_id(), stored.incarnation))
        .collect();
    failures.retain(|key, _| current.contains(key));
}

fn fan_out(
    velo: &velo::Velo,
    store: &Arc<dyn HubStore>,
    registrations: Vec<StoredPeer>,
    seq: u64,
    settings: &Settings,
    tx: &mpsc::UnboundedSender<ProbeOutcome>,
) {
    let Settings {
        hub_id,
        interval,
        step_timeout,
        ..
    } = *settings;
    for stored in registrations {
        // Pinned entries are the hub's own; it does not probe itself.
        if stored.pinned {
            continue;
        }
        let id = stored.peer.instance_id();
        let incarnation = stored.incarnation;
        let velo = velo.downgrade();
        let store = Arc::clone(store);
        let tx = tx.clone();
        tokio::spawn(async move {
            let probe = async {
                let velo = velo
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("hub velo is gone"))?;
                probe(&velo, id, seq, Some(hub_id), interval).await
            };
            let outcome = match probe.await {
                // The registrant holds no registration with this hub (its
                // registration's response was lost, or it dropped its
                // client). Count it as a failure, so the entry goes and the
                // owner can register again.
                Ok(ack) if !ack.ok => ProbeOutcome::Failed {
                    id,
                    incarnation,
                    reason: "the registrant answered not ok: it holds no registration".to_string(),
                },
                Ok(ack) => {
                    // Conditional on the incarnation probed, so a late touch
                    // cannot refresh a replacement.
                    match super::bounded(step_timeout, store.touch(id, incarnation)).await {
                        Ok(()) => {
                            tracing::trace!(instance = %id, %incarnation, ack_seq = ack.seq, "heartbeat: refreshed")
                        }
                        Err(error) => {
                            tracing::trace!(instance = %id, %incarnation, %error, "heartbeat: stale touch ignored")
                        }
                    }
                    ProbeOutcome::Ok { id, incarnation }
                }
                Err(error) => ProbeOutcome::Failed {
                    id,
                    incarnation,
                    reason: format!("{error:#}"),
                },
            };
            let _ = tx.send(outcome);
        });
    }
}

/// Send one heartbeat to `id` over velo and wait up to `deadline` for the
/// ack. A peer that never answers must not hold a probe task, or the probe
/// route's request, open.
pub(crate) async fn probe(
    velo: &velo::Velo,
    id: InstanceId,
    seq: u64,
    hub_id: Option<HubId>,
    deadline: Duration,
) -> anyhow::Result<HeartbeatAck> {
    let send = velo
        .typed_unary(HEARTBEAT_HANDLER)?
        .payload(&HeartbeatRequest { seq, hub_id })?
        .instance(id)
        .send();
    tokio::time::timeout(deadline, send)
        .await
        .map_err(|_| anyhow::anyhow!("no answer within {deadline:?}"))?
}

/// The loop handles outcomes one at a time, so its store call is bounded:
/// a remove that never answered would stop every later probe and keep
/// shutdown, which joins this task, waiting.
async fn handle_outcome(
    outcome: ProbeOutcome,
    store: &dyn HubStore,
    failures: &mut HashMap<FailureKey, u32>,
    max_failures: u32,
    step_timeout: Duration,
) {
    match outcome {
        ProbeOutcome::Ok { id, incarnation } => {
            failures.remove(&(id, incarnation));
        }
        ProbeOutcome::Failed {
            id,
            incarnation,
            reason,
        } => {
            let key = (id, incarnation);
            let count = failures
                .entry(key)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            tracing::warn!(instance = %id, %incarnation, failures = *count, error = %reason, "heartbeat: probe failed");
            if *count >= max_failures {
                failures.remove(&key);
                if let Err(error) =
                    super::bounded(step_timeout, store.remove(id, incarnation)).await
                {
                    tracing::warn!(instance = %id, %incarnation, %error, "heartbeat: conditional remove ignored");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Ported from kvbm-hub `server/heartbeat.rs`.

    use super::*;
    use crate::store::{InMemoryStore, RegistrationRecord};
    use velo::{PeerInfo, WorkerAddress};

    fn peer(label: &[u8]) -> PeerInfo {
        PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(label.to_vec()),
        )
    }

    async fn put(store: &InMemoryStore, peer: &PeerInfo) -> Incarnation {
        store
            .put(
                RegistrationRecord::bare(peer.clone()),
                store.current_incarnation(peer.instance_id()),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stale_probe_failure_cannot_unregister_replacement_incarnation() {
        let store = InMemoryStore::default();
        let peer = peer(b"stale-probe");
        let stale = put(&store, &peer).await;
        let current = put(&store, &peer).await;
        let mut failures = HashMap::new();

        handle_outcome(
            ProbeOutcome::Failed {
                id: peer.instance_id(),
                incarnation: stale,
                reason: "old probe timed out".to_string(),
            },
            &store,
            &mut failures,
            1,
            Duration::from_secs(1),
        )
        .await;

        assert!(store.contains(peer.instance_id()));
        store
            .touch(peer.instance_id(), current)
            .await
            .expect("stale probe outcome removed the replacement");
    }

    #[tokio::test]
    async fn probe_failure_counters_are_scoped_to_incarnation() {
        let store = InMemoryStore::default();
        let peer = peer(b"probe-counter");
        let stale = put(&store, &peer).await;
        let current = put(&store, &peer).await;
        let mut failures = HashMap::new();

        for incarnation in [stale, current] {
            handle_outcome(
                ProbeOutcome::Failed {
                    id: peer.instance_id(),
                    incarnation,
                    reason: "independent failure".to_string(),
                },
                &store,
                &mut failures,
                2,
                Duration::from_secs(1),
            )
            .await;
        }

        assert_eq!(failures.get(&(peer.instance_id(), stale)), Some(&1));
        assert_eq!(failures.get(&(peer.instance_id(), current)), Some(&1));
        assert!(store.contains(peer.instance_id()));
    }
}
