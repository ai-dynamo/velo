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
//! not delay the others.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use velo::{InstanceId, WeakVelo};

use crate::protocol::{HEARTBEAT_HANDLER, HeartbeatAck, HeartbeatRequest};
use crate::store::{HubStore, Incarnation, StoredPeer};

type FailureKey = (InstanceId, Incarnation);

#[derive(Debug)]
enum ProbeOutcome {
    Ok {
        id: InstanceId,
        incarnation: Incarnation,
        ack_seq: u64,
    },
    Failed {
        id: InstanceId,
        incarnation: Incarnation,
        reason: String,
    },
}

/// Spawn the heartbeat manager. It holds a [`WeakVelo`]: a strong clone in
/// a task that only the hub cancels would keep a dropped hub's velo alive.
pub(crate) fn spawn(
    velo: WeakVelo,
    store: Arc<dyn HubStore>,
    interval: Duration,
    max_failures: u32,
    step_timeout: Duration,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ProbeOutcome>();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        // Outcomes are handled one at a time, each awaiting a store call, so
        // the loop can miss ticks. Replaying them (tokio's default) would fan
        // out a full round of probes per missed tick at once.
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
                    let registrations = store.peers();
                    retain_current_failures(&mut failures, &registrations);
                    fan_out(&velo, registrations, seq, interval, &tx);
                }
                Some(outcome) = rx.recv() => {
                    handle_outcome(outcome, store.as_ref(), &mut failures, max_failures, step_timeout).await;
                }
            }
        }
    })
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
    registrations: Vec<StoredPeer>,
    seq: u64,
    interval: Duration,
    tx: &mpsc::UnboundedSender<ProbeOutcome>,
) {
    for stored in registrations {
        // Pinned entries are the hub's own; it does not probe itself.
        if stored.pinned {
            continue;
        }
        let id = stored.peer.instance_id();
        let incarnation = stored.incarnation;
        let velo = velo.downgrade();
        let tx = tx.clone();
        tokio::spawn(async move {
            let probe = async {
                let velo = velo
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("hub velo is gone"))?;
                let ack: HeartbeatAck = velo
                    .typed_unary(HEARTBEAT_HANDLER)?
                    .payload(&HeartbeatRequest { seq })?
                    .instance(id)
                    .send()
                    .await?;
                Ok::<HeartbeatAck, anyhow::Error>(ack)
            };
            let outcome = match tokio::time::timeout(interval, probe).await {
                Ok(Ok(ack)) => ProbeOutcome::Ok {
                    id,
                    incarnation,
                    ack_seq: ack.seq,
                },
                Ok(Err(error)) => ProbeOutcome::Failed {
                    id,
                    incarnation,
                    reason: format!("{error:#}"),
                },
                Err(_) => ProbeOutcome::Failed {
                    id,
                    incarnation,
                    reason: format!("probe timed out after {interval:?}"),
                },
            };
            let _ = tx.send(outcome);
        });
    }
}

/// One store call under `step_timeout`. The loop handles outcomes one at a
/// time, so a call that never answered would stop every later probe and
/// keep shutdown, which joins this task, waiting.
async fn bounded(
    step_timeout: Duration,
    call: futures::future::BoxFuture<'_, Result<(), crate::store::StoreError>>,
) -> Result<(), crate::store::StoreError> {
    match tokio::time::timeout(step_timeout, call).await {
        Ok(result) => result,
        Err(_) => Err(crate::store::StoreError::Indeterminate(format!(
            "did not complete within {step_timeout:?}"
        ))),
    }
}

async fn handle_outcome(
    outcome: ProbeOutcome,
    store: &dyn HubStore,
    failures: &mut HashMap<FailureKey, u32>,
    max_failures: u32,
    step_timeout: Duration,
) {
    match outcome {
        ProbeOutcome::Ok {
            id,
            incarnation,
            ack_seq,
        } => {
            failures.remove(&(id, incarnation));
            match bounded(step_timeout, store.touch(id, incarnation)).await {
                Ok(()) => {
                    tracing::trace!(instance = %id, %incarnation, ack_seq, "heartbeat: refreshed")
                }
                Err(error) => {
                    tracing::trace!(instance = %id, %incarnation, %error, "heartbeat: stale touch ignored")
                }
            }
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
                if let Err(error) = bounded(step_timeout, store.remove(id, incarnation)).await {
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
