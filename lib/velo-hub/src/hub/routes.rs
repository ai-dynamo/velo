// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Core routes, extension mounting, and the [`Authorized`] extractor.

use std::sync::Arc;

use axum::extract::{FromRequest, FromRequestParts, Path, RawPathParams, Request, State};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use velo::{InstanceId, WorkerId};

use super::Shared;
use super::authority::{Authority, Lease};
use super::error::HubError;
use super::registration;
use crate::protocol::{
    self, CREDENTIAL_HEADER, Credential, ExtensionDescriptor, HEARTBEAT_HANDLER, HeartbeatAck,
    HeartbeatRequest, HeartbeatResponse, HubDescriptor, InstanceEntry, ListInstancesResponse,
    PeerLookupResponse, ProbeResponse, RegisterRequest, RegisterResponse, paths,
};
use crate::store::StoreError;

/// The credential in a request's [`CREDENTIAL_HEADER`], if any. A header
/// that is present but malformed is a 400, not a missing credential.
pub fn credential_from_headers(headers: &HeaderMap) -> Result<Option<Credential>, HubError> {
    let Some(value) = headers.get(CREDENTIAL_HEADER) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| HubError::bad_request("credential header is not ASCII"))?;
    Credential::from_header_str(value)
        .map(Some)
        .map_err(|error| HubError::bad_request(error.to_string()))
}

/// Extractor for an extension route that mutates what one registration
/// owns. It reads the owner from the route's `{instance_id}` path parameter
/// and the credential from [`CREDENTIAL_HEADER`], and rejects the request
/// unless the hub's [`Authority`] grants a [`Lease`].
///
/// The lease's epoch is the fence: apply the mutation only if it equals the
/// epoch the extension committed for `owner`, checked under the extension's
/// own lock with the write.
#[derive(Debug, Clone, Copy)]
pub struct Authorized {
    /// The owner named in the path.
    pub owner: InstanceId,
    /// What the credential grants.
    pub lease: Lease,
}

impl<S: Send + Sync> FromRequestParts<S> for Authorized {
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let params = RawPathParams::from_request_parts(parts, state)
            .await
            .map_err(|error| {
                HubError::internal(format!("Authorized needs path parameters: {error}"))
            })?;
        let owner = params
            .iter()
            .find(|(name, _)| *name == "instance_id")
            .map(|(_, value)| value)
            .ok_or_else(|| {
                HubError::internal("Authorized needs an {instance_id} path parameter")
            })?;
        let owner = uuid::Uuid::parse_str(owner)
            .map(InstanceId::from)
            .map_err(|_| HubError::bad_request(format!("{owner:?} is not an instance id")))?;
        let authority = parts
            .extensions
            .get::<Authority>()
            .ok_or_else(|| HubError::internal("Authorized is only available on hub routes"))?;
        let presented = credential_from_headers(&parts.headers)?;
        let lease = authority.authorize(owner, presented.as_ref())?;
        Ok(Self { owner, lease })
    }
}

/// [`axum::Json`] whose rejection is a [`HubError`], so a body the hub
/// cannot parse still answers with an [`ErrorBody`](crate::protocol::ErrorBody).
/// Extension routes can use it too.
#[derive(Debug, Clone, Copy, Default)]
pub struct HubJson<T>(pub T);

impl<T, S> FromRequest<S> for HubJson<T>
where
    axum::Json<T>: FromRequest<S, Rejection = axum::extract::rejection::JsonRejection>,
    S: Send + Sync,
{
    type Rejection = HubError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        axum::Json::<T>::from_request(request, state)
            .await
            .map(|axum::Json(value)| Self(value))
            .map_err(|rejection| HubError::bad_request(rejection.body_text()))
    }
}

/// [`axum::extract::Path`] whose rejection is a [`HubError`].
#[derive(Debug, Clone, Copy, Default)]
pub struct HubPath<T>(pub T);

impl<T, S> FromRequestParts<S> for HubPath<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|rejection| HubError::bad_request(rejection.body_text()))
    }
}

type HubState = State<Arc<Shared>>;

/// The public listener: discovery and the extensions' public routes.
pub(crate) fn public_router(shared: &Arc<Shared>) -> Router {
    let core = Router::new()
        .route(paths::HEALTH, get(health))
        .route(paths::HUB, get(describe))
        .route(paths::PEERS_BY_INSTANCE, get(peer_by_instance))
        .route(paths::PEERS_BY_WORKER, get(peer_by_worker))
        .with_state(Arc::clone(shared));
    mount(core, shared, |extension| extension.public_routes())
}

/// The control listener: everything public, plus registration.
pub(crate) fn control_router(shared: &Arc<Shared>) -> Router {
    let core = Router::new()
        .route(paths::HEALTH, get(health))
        .route(paths::HUB, get(describe))
        .route(paths::PEERS_BY_INSTANCE, get(peer_by_instance))
        .route(paths::PEERS_BY_WORKER, get(peer_by_worker))
        .route(paths::INSTANCES, get(list).post(register))
        .route(paths::INSTANCE_BY_ID, delete(unregister))
        .route(paths::INSTANCE_HEARTBEAT, post(heartbeat))
        .route(paths::INSTANCE_PROBE, post(probe))
        .with_state(Arc::clone(shared));
    mount(core, shared, |extension| extension.control_routes())
}

/// Nest each extension under `/v1/ext/{name}`. Names are validated as
/// single path segments before the hub binds, so an extension cannot reach
/// a core path.
fn mount(
    core: Router,
    shared: &Arc<Shared>,
    routes: impl Fn(&dyn super::extension::DynExtension) -> Router,
) -> Router {
    let mut router = core;
    for extension in &shared.extensions {
        router = router.nest(
            &protocol::extension_root(extension.name()),
            routes(extension.as_ref()),
        );
    }
    router
        .fallback(|| async { HubError::not_found("no such route") })
        .method_not_allowed_fallback(|| async {
            HubError::new(
                protocol::ErrorCode::MethodNotAllowed,
                "the route does not take this method",
            )
        })
        .layer(Extension(shared.authority.clone()))
}

async fn health() -> &'static str {
    "ok"
}

async fn describe(State(shared): HubState) -> Json<HubDescriptor> {
    Json(HubDescriptor {
        hub_id: shared.hub_id,
        velo_instance_id: shared.velo.as_ref().map(|velo| velo.instance_id()),
        extensions: shared
            .extensions
            .iter()
            .map(|extension| ExtensionDescriptor {
                name: extension.name().to_string(),
                requires: extension
                    .requires()
                    .iter()
                    .map(|name| name.to_string())
                    .collect(),
                registration_requires: extension
                    .registration_requires()
                    .iter()
                    .map(|name| name.to_string())
                    .collect(),
                descriptor: extension.descriptor(),
            })
            .collect(),
    })
}

async fn peer_by_instance(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
) -> Result<Json<PeerLookupResponse>, HubError> {
    shared
        .store
        .get(id)
        .map(|stored| {
            Json(PeerLookupResponse {
                peer: stored.record.peer,
            })
        })
        .ok_or_else(|| HubError::not_found(format!("instance {id} not found")))
}

async fn peer_by_worker(
    State(shared): HubState,
    HubPath(worker): HubPath<u64>,
) -> Result<Json<PeerLookupResponse>, HubError> {
    shared
        .store
        .discover_by_worker_id(WorkerId::from_u64(worker))
        .await
        .map(|peer| Json(PeerLookupResponse { peer }))
        .map_err(|_| HubError::not_found(format!("worker {worker} not found")))
}

async fn list(State(shared): HubState) -> Json<ListInstancesResponse> {
    Json(ListInstancesResponse {
        instances: shared
            .store
            .list()
            .into_iter()
            .map(|stored| InstanceEntry {
                extensions: stored.record.extensions.keys().cloned().collect(),
                pinned: stored.record.pinned,
                peer: stored.record.peer,
            })
            .collect(),
    })
}

async fn register(
    State(shared): HubState,
    headers: HeaderMap,
    HubJson(request): HubJson<RegisterRequest>,
) -> Result<Json<RegisterResponse>, HubError> {
    let presented = credential_from_headers(&headers)?;
    registration::register(shared, presented, request)
        .await
        .map(Json)
}

async fn unregister(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
    headers: HeaderMap,
) -> Result<StatusCode, HubError> {
    let presented = credential_from_headers(&headers)?;
    registration::unregister(&shared, id, presented).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn heartbeat(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
    headers: HeaderMap,
) -> Result<Json<HeartbeatResponse>, HubError> {
    let presented = credential_from_headers(&headers)?;
    let lease = shared.authority.authorize(id, presented.as_ref())?;
    shared
        .store
        .touch(id, lease.incarnation)
        .await
        .map_err(|error| match error {
            // Authorized, but a replacement won the race: not this caller's
            // registration any more.
            StoreError::Stale { .. } => {
                HubError::new(protocol::ErrorCode::Conflict, error.to_string())
            }
            other => other.into(),
        })?;
    Ok(Json(HeartbeatResponse { acknowledged: true }))
}

async fn probe(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
) -> Result<Json<ProbeResponse>, HubError> {
    if !shared.store.contains(id) {
        return Err(HubError::not_found(format!("instance {id} not registered")));
    }
    let velo = shared
        .velo
        .as_ref()
        .ok_or_else(|| HubError::internal("the hub runs no velo instance"))?;
    let gateway = |error: anyhow::Error| {
        HubError::new(
            protocol::ErrorCode::BadGateway,
            format!("probe failed: {error:#}"),
        )
    };
    let ack: HeartbeatAck = velo
        .typed_unary(HEARTBEAT_HANDLER)
        .map_err(gateway)?
        .payload(&HeartbeatRequest { seq: 0 })
        .map_err(gateway)?
        .instance(id)
        .send()
        .await
        .map_err(gateway)?;
    Ok(Json(ProbeResponse {
        seq: ack.seq,
        ok: ack.ok,
    }))
}
