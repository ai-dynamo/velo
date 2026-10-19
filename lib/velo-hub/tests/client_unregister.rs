// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ported from kvbm-hub `tests/client_unregister.rs`: a failed `DELETE`
//! keeps the registration, so an explicit retry or the drop sends it again.
//! A fake hub answers the first `DELETE` with 500.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use velo_hub::axum::extract::State;
use velo_hub::axum::http::StatusCode;
use velo_hub::axum::routing::{delete, post};
use velo_hub::axum::{Json, Router};
use velo_hub::client::HubClient;
use velo_hub::protocol::{
    Credential, ErrorBody, ErrorCode, HubId, RegisterRequest, RegisterResponse, RegistrationEpoch,
    paths,
};
use velo_hub::velo::{InstanceId, PeerInfo, WorkerAddress};

struct UnregisterServer {
    address: SocketAddr,
    delete_attempts: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for UnregisterServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn explicit_unregister_retries_after_server_failure() {
    let server = start_unregister_server().await;
    let client = register_client(&server).await;

    assert!(client.unregister().await.is_err());
    assert!(client.is_registered());
    assert!(client.unregister().await.is_ok());
    assert!(!client.is_registered());
    assert_eq!(server.delete_attempts.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn drop_retries_after_explicit_unregister_failure() {
    let server = start_unregister_server().await;
    let client = register_client(&server).await;

    assert!(client.unregister().await.is_err());
    drop(client);

    tokio::time::timeout(Duration::from_secs(1), async {
        while server.delete_attempts.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dropping the client should retry the DELETE");
}

async fn register_client(server: &UnregisterServer) -> HubClient {
    let client = HubClient::builder()
        .host("127.0.0.1")
        .public_port(server.address.port())
        .control_port(server.address.port())
        .build()
        .unwrap();
    client
        .register_peer(make_peer(), Default::default())
        .await
        .unwrap();
    client
}

async fn start_unregister_server() -> UnregisterServer {
    let delete_attempts = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(paths::INSTANCES, post(register))
        .route(paths::INSTANCE_BY_ID, delete(unregister))
        .with_state(Arc::clone(&delete_attempts));
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        velo_hub::axum::serve(listener, router).await.unwrap();
    });
    UnregisterServer {
        address,
        delete_attempts,
        task,
    }
}

async fn register(Json(request): Json<RegisterRequest>) -> Json<RegisterResponse> {
    Json(RegisterResponse {
        instance_id: request.peer.instance_id(),
        hub_instance_id: None,
        credential: Credential::from_header_str("00000000-0000-4000-8000-000000000001").unwrap(),
        epoch: RegistrationEpoch::new(),
        hub_id: HubId::new(),
    })
}

async fn unregister(
    State(delete_attempts): State<Arc<AtomicUsize>>,
) -> Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    if delete_attempts.fetch_add(1, Ordering::AcqRel) == 0 {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorBody {
                code: ErrorCode::Internal,
                message: "transient failure".to_string(),
                not_ready: false,
            }),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}

fn make_peer() -> PeerInfo {
    PeerInfo::new(
        InstanceId::new_v4(),
        WorkerAddress::from_encoded(b"client-unregister-test".to_vec()),
    )
}
