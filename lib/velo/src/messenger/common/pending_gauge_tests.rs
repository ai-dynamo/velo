// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! `velo_messenger_pending_responses` equals the number of response slots
//! held once concurrent sends and drops have finished.
//!
//! The count of held slots is one atomic. The gauge was a second value, set
//! from what that atomic returned: `fetch_add` or `fetch_sub`, then `set`.
//! Two threads can run those two steps interleaved, and the one that set the
//! older count can set it last. Thread A takes the count from 2 to 1, thread B
//! from 1 to 0; B sets 0, then A sets 1. The count is 0 and the gauge reads 1
//! until the next send. The other order reads 0 with a slot still held.
//!
//! A send holds a slot from `register_outcome` until its awaiter drops, so
//! these tests register and drop awaiters on plain threads, with no peer. A
//! barrier lines the threads up so their updates overlap; each round then
//! reads the gauge while every thread waits.

use std::sync::{Arc, Barrier};

use prometheus::Registry;

use super::responses::{ResponseAwaiter, ResponseManager};
use crate::observability::VeloMetrics;
use crate::observability::test_helpers::MetricSnapshot;

/// How many rounds each test runs. With the gauge set from a computed value,
/// both concurrent tests failed within this many rounds on every run.
const ROUNDS: usize = 20_000;

fn metered() -> (ResponseManager, Registry) {
    let registry = Registry::new();
    let metrics = Arc::new(VeloMetrics::register(&registry).unwrap());
    (
        ResponseManager::with_observability(7, Some(metrics)),
        registry,
    )
}

fn gauge(registry: &Registry) -> f64 {
    MetricSnapshot::from_registry(registry).gauge("velo_messenger_pending_responses", &[])
}

/// The control: one thread, so nothing overlaps.
#[test]
fn a_serial_send_and_drop_keep_the_gauge_in_sync() {
    let (manager, registry) = metered();
    let held = manager.register_outcome().unwrap();
    assert_eq!(gauge(&registry), 1.0, "a held awaiter holds a slot");
    drop(held);
    assert_eq!(gauge(&registry), 0.0, "its drop frees the slot");
}

/// Run up to [`ROUNDS`] rounds on `threads` threads, each with a slot of its
/// own (`Some` for the threads in `start_holding`). In a round, each thread
/// runs `prepare` on its slot, waits for the others, and runs `act`, so the
/// `act` calls overlap. Then, with every thread waiting, the gauge is read.
/// Returns the first round where it is not `expected`, and the value read.
fn first_stale_round(
    manager: &ResponseManager,
    registry: &Registry,
    threads: usize,
    start_holding: usize,
    expected: f64,
    prepare: impl Fn(&ResponseManager, &mut Option<ResponseAwaiter>) + Sync,
    act: impl Fn(&ResponseManager, &mut Option<ResponseAwaiter>) + Sync,
) -> Option<(usize, f64)> {
    let barrier = Barrier::new(threads);
    let stale = parking_lot::Mutex::new(None);
    std::thread::scope(|scope| {
        for worker in 0..threads {
            let (barrier, stale, prepare, act) = (&barrier, &stale, &prepare, &act);
            scope.spawn(move || {
                let mut slot =
                    (worker < start_holding).then(|| manager.register_outcome().unwrap());
                for round in 0..ROUNDS {
                    prepare(manager, &mut slot);
                    barrier.wait();
                    act(manager, &mut slot);
                    if barrier.wait().is_leader() {
                        let value = gauge(registry);
                        if value != expected {
                            *stale.lock() = Some((round, value));
                        }
                    }
                    // Every thread sees the leader's verdict for this round.
                    barrier.wait();
                    if stale.lock().is_some() {
                        return;
                    }
                }
            });
        }
    });
    stale.into_inner()
}

/// Eight threads each hold a slot and drop it at the same time. With every
/// awaiter gone the gauge must read 0.
#[test]
fn concurrent_drops_leave_the_gauge_at_zero() {
    let (manager, registry) = metered();
    let stale = first_stale_round(
        &manager,
        &registry,
        8,
        0,
        0.0,
        |manager, slot| *slot = Some(manager.register_outcome().unwrap()),
        |_, slot| *slot = None,
    );
    assert_eq!(
        stale, None,
        "(round, gauge) with every awaiter dropped and 0 slots held"
    );
}

/// One thread drops the slot it holds while the other takes one, and they
/// swap roles each round. One slot is held after each round, so the gauge
/// must read 1: a stale write leaves 0 with a slot held, or 2.
#[test]
fn one_held_slot_reads_one_while_sends_and_drops_overlap() {
    let (manager, registry) = metered();
    let stale = first_stale_round(
        &manager,
        &registry,
        2,
        1,
        1.0,
        |_, _| {},
        |manager, slot| {
            *slot = match slot.take() {
                Some(_dropped) => None,
                None => Some(manager.register_outcome().unwrap()),
            }
        },
    );
    assert_eq!(
        stale, None,
        "(round, gauge) with exactly 1 slot held after the round"
    );
}
