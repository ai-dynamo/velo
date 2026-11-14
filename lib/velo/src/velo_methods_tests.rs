// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The anchor and staging methods on [`Velo`](crate::Velo) that replace
//! reaching through `anchor_manager()` and `rendezvous_manager()`.
//!
//! Each test checks that the method reaches the state its manager form
//! changes, not only that it returns.

use std::sync::Arc;
use std::time::Duration;

use crate::streaming::{AnchorConfig, AnchorReservation};

async fn tcp_node() -> crate::Velo {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    crate::Velo::builder()
        .add_transport(Arc::new(
            crate::transports::tcp::TcpTransportBuilder::new()
                .from_listener(listener)
                .unwrap()
                .build()
                .unwrap(),
        ))
        .build()
        .await
        .unwrap()
}

async fn shut_down(velo: crate::Velo) {
    velo.shutdown(crate::ShutdownPolicy::Timeout(Duration::from_secs(2)))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_anchor_with_config_applies_the_config() {
    let velo = tcp_node().await;
    let timeout = Duration::from_secs(4_321);

    let anchor = velo.create_anchor_with_config::<u32>(AnchorConfig {
        unattached_timeout: Some(timeout),
        ..AnchorConfig::default()
    });

    let entry = velo
        .inner
        .anchor_manager
        .registry
        .get(&anchor.handle().unpack().1)
        .map(|entry| entry.unattached_timeout);
    drop(anchor);
    shut_down(velo).await;
    assert_eq!(entry, Some(Some(timeout)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_anchor_count_counts_spsc_anchors() {
    let velo = tcp_node().await;
    assert_eq!(velo.active_anchor_count(), 0);

    let first = velo.create_anchor::<u32>();
    let second = velo.create_anchor_with_config::<u32>(AnchorConfig::default());
    let mpsc = velo.create_mpsc_anchor::<u32>();
    let count = velo.active_anchor_count();

    drop((first, second, mpsc));
    shut_down(velo).await;
    assert_eq!(count, 2, "two SPSC anchors; the MPSC anchor is not counted");
}

/// A renewal moves an unattached anchor's deadline later. Read straight
/// from the entry, so the test does not wait on a timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_reservations_moves_the_unattached_deadline() {
    let velo = tcp_node().await;
    let anchor = velo.create_anchor_with_config::<u32>(AnchorConfig {
        unattached_timeout: Some(Duration::from_secs(60)),
        ..AnchorConfig::default()
    });
    let id = anchor.handle().unpack().1;
    let expires = |velo: &crate::Velo| {
        velo.inner
            .anchor_manager
            .registry
            .get(&id)
            .and_then(|entry| entry.expires)
            .expect("an unattached anchor has a deadline")
    };
    let before = expires(&velo);
    tokio::time::sleep(Duration::from_millis(20)).await;

    let renewed = velo
        .renew_reservations(&[AnchorReservation::new(anchor.handle(), None)])
        .await;
    let after = expires(&velo);

    drop(anchor);
    shut_down(velo).await;
    renewed.expect("a local renewal needs no transport");
    assert!(after > before, "the renewal did not move the deadline");
}

/// The threshold is the one the send path uses: a payload of exactly the
/// threshold goes inline, one byte more is staged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_threshold_is_the_one_the_send_path_uses() {
    let velo = tcp_node().await;
    let threshold = velo.staging_threshold();
    let client = velo.inner.messenger.client();
    let inline = client.stager_for(threshold).is_none();
    let staged = client.stager_for(threshold + 1).is_some();

    shut_down(velo).await;
    assert_eq!(
        threshold,
        crate::rendezvous::transparent::DEFAULT_THRESHOLD,
        "Velo installs the default stager"
    );
    assert!(inline, "a payload of exactly the threshold goes inline");
    assert!(staged, "a payload over the threshold is staged");
}
