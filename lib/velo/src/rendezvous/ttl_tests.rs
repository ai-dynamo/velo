// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! TTL expiry of data staged on an owner whose handlers are registered after
//! the data. The manager is built by hand, so these are unit tests.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::rendezvous::RendezvousManager;

fn new_transport() -> Arc<crate::transports::tcp::TcpTransport> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    Arc::new(
        crate::transports::tcp::TcpTransportBuilder::new()
            .from_listener(listener)
            .unwrap()
            .build()
            .unwrap(),
    )
}

/// Stage TTL data on a `RendezvousManager` built by hand, before or after its
/// handlers are registered, and report whether the data expires. The
/// manager's handlers replace the ones the `Velo` registered.
fn ttl_data_expires_after_handler_registration(stage_before_handlers: bool) -> bool {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let velo = runtime
        .block_on(
            crate::Velo::builder()
                .add_transport(new_transport())
                .mux_only()
                .build(),
        )
        .unwrap();
    let messenger = Arc::clone(velo.messenger());
    let manager = Arc::new(RendezvousManager::new(messenger.instance_id().worker_id()));
    assert!(tokio::runtime::Handle::try_current().is_err());
    let stage = || {
        manager.register_data_with(
            Bytes::from_static(b"expires"),
            crate::rendezvous::RegisterOptions::new().ttl(Duration::from_millis(20)),
        )
    };
    let handle = if stage_before_handlers {
        let handle = stage();
        manager.register_handlers(Arc::clone(&messenger)).unwrap();
        handle
    } else {
        manager.register_handlers(Arc::clone(&messenger)).unwrap();
        stage()
    };
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.data_store().metadata(handle.unpack().1).is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok()
    })
}

/// Binding the owner's runtime must sweep TTL data staged before its handlers.
#[test]
#[ignore = "F4: binding a runtime leaves pre-handler TTL data without a reaper"]
fn binding_the_owner_runtime_sweeps_data_staged_before_its_handlers() {
    assert!(
        ttl_data_expires_after_handler_registration(true),
        "TTL data stayed staged after the owner's runtime was bound"
    );
}

#[test]
fn a_standalone_owner_expires_data_staged_after_its_handlers() {
    assert!(ttl_data_expires_after_handler_registration(false));
}
