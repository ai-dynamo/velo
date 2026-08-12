// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the client's eager-payload budget.

use super::*;

use crate::messenger::Messenger;
use crate::rendezvous::transparent::{DEFAULT_THRESHOLD, RendezvousStager};
use crate::transports::tcp::{TcpTransport, TcpTransportBuilder};

/// The `_stream_batch` envelope, spelled out here so a change to
/// [`envelope_overhead`] that these tests depend on shows up as a diff rather
/// than silently rebasing every expected number.
const STREAM_BATCH_ENVELOPE: usize = 22 + "_stream_batch".len();

/// The TCP codec's frame ceiling — what `TcpTransport::max_message_size`
/// reports. Written out rather than imported so the two are pinned against
/// each other instead of being the same expression twice.
const TCP_FRAME_CEILING: usize = 16 * 1024 * 1024;

#[test]
fn budget_falls_back_to_the_staging_threshold_when_capacity_is_unknown() {
    // A transport that reports `None` — gRPC and ZMQ today — must not be read
    // as unlimited. The stager's threshold is the only ceiling left.
    assert_eq!(
        eager_payload_budget(None, Some(64 * 1024), 100),
        64 * 1024 - 100
    );
}

#[test]
fn budget_takes_the_lower_of_the_two_ceilings() {
    // Transport binds below the threshold.
    assert_eq!(eager_payload_budget(Some(4096), Some(64 * 1024), 30), 4066);
    // Threshold binds below the transport.
    assert_eq!(
        eager_payload_budget(Some(TCP_FRAME_CEILING), Some(64 * 1024), 30),
        64 * 1024 - 30
    );
}

#[test]
fn budget_uses_the_transport_alone_when_no_stager_is_installed() {
    // No stager means no cheaper path to fall back to, so clamping to the
    // default threshold would forbid sends that would have worked.
    assert_eq!(
        eager_payload_budget(Some(TCP_FRAME_CEILING), None, STREAM_BATCH_ENVELOPE),
        TCP_FRAME_CEILING - STREAM_BATCH_ENVELOPE
    );
}

#[test]
fn budget_defaults_to_the_transparent_staging_threshold_when_nothing_is_known() {
    assert_eq!(
        eager_payload_budget(None, None, STREAM_BATCH_ENVELOPE),
        DEFAULT_THRESHOLD - STREAM_BATCH_ENVELOPE
    );
}

#[test]
fn budget_saturates_rather_than_wrapping_when_the_envelope_exceeds_the_ceiling() {
    // Reachable for real: headers may carry up to 16 KiB, so a deployment that
    // lowers `with_threshold` far enough can put the envelope above the
    // ceiling. Wrapping here would produce a near-`usize::MAX` budget — the
    // exact oversized send this whole boundary exists to stop.
    assert_eq!(eager_payload_budget(None, Some(1024), 4096), 0);
    assert_eq!(eager_payload_budget(Some(64), None, 4096), 0);
}

/// Build a messenger over a real TCP transport and register a peer on it, so
/// the budget is read through the full backend → transport path.
async fn tcp_messenger() -> (std::sync::Arc<Messenger>, std::sync::Arc<Messenger>) {
    async fn build() -> std::sync::Arc<Messenger> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let transport: std::sync::Arc<TcpTransport> = std::sync::Arc::new(
            TcpTransportBuilder::new()
                .from_listener(listener)
                .expect("from_listener")
                .build()
                .expect("build"),
        );
        Messenger::builder()
            .add_transport(transport)
            .build()
            .await
            .expect("messenger")
    }
    let (local, remote) = (build().await, build().await);
    local.register_peer(remote.peer_info()).expect("register");
    (local, remote)
}

#[tokio::test]
async fn tcp_budget_is_the_frame_ceiling_less_the_envelope() {
    let (local, remote) = tcp_messenger().await;

    // No stager installed: TCP's framed ceiling is the only bound.
    assert_eq!(
        local.effective_eager_payload(remote.instance_id(), "_stream_batch", None),
        TCP_FRAME_CEILING - STREAM_BATCH_ENVELOPE,
    );

    // An unregistered peer has no transport to ask, which is the same "cannot
    // say" as a transport that does not know — so the conservative default.
    assert_eq!(
        local.effective_eager_payload(crate::InstanceId::new_v4(), "_stream_batch", None),
        DEFAULT_THRESHOLD - STREAM_BATCH_ENVELOPE,
    );
}

#[tokio::test]
async fn lowering_the_rendezvous_threshold_lowers_the_budget() {
    let (local, remote) = tcp_messenger().await;
    let target = remote.instance_id();
    let before = local.effective_eager_payload(target, "_stream_batch", None);

    // 64 KiB is far below TCP's 16 MiB ceiling, so the threshold must bind.
    const LOWERED_THRESHOLD: usize = 64 * 1024;
    let manager = std::sync::Arc::new(crate::RendezvousManager::new(velo_ext::WorkerId::from_u64(
        7,
    )));
    local.set_large_payload_support(
        std::sync::Arc::new(
            RendezvousStager::new(manager.clone()).with_threshold(LOWERED_THRESHOLD),
        ),
        std::sync::Arc::new(crate::rendezvous::transparent::RendezvousResolver::new(
            manager,
        )),
    );

    let after = local.effective_eager_payload(target, "_stream_batch", None);
    assert_eq!(before, TCP_FRAME_CEILING - STREAM_BATCH_ENVELOPE);
    assert_eq!(after, LOWERED_THRESHOLD - STREAM_BATCH_ENVELOPE);

    // A header set costs exactly what the encoder would spend on it: the `_rv`
    // key and a 39-digit handle push the envelope out by the MessagePack map.
    let mut headers = HashMap::new();
    headers.insert(
        crate::messenger::large_payload::RV_HEADER_KEY.to_string(),
        "9".repeat(39),
    );
    assert_eq!(
        local.effective_eager_payload(target, "_stream_batch", Some(&headers)),
        LOWERED_THRESHOLD - (STREAM_BATCH_ENVELOPE + 1 + 4 + 41),
    );
}
