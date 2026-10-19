// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Core routes, extension mounting, and the [`Authorized`] extractor.

use std::sync::Arc;

use axum::extract::{FromRequest, FromRequestParts, Path, RawPathParams, Request, State};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use velo::{InstanceId, WorkerId};

use tokio_util::sync::CancellationToken;

use super::Shared;
use super::authority::{Authority, Lease};
use super::error::HubError;
use super::registration;
use crate::protocol::{
    self, CREDENTIAL_HEADER, Credential, ExtensionDescriptor, HeartbeatResponse, HubDescriptor,
    InstanceEntry, ListInstancesResponse, PeerLookupResponse, ProbeResponse, RegisterRequest,
    RegisterResponse, paths,
};

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

/// The credential a request presents, if any, read by reference from the
/// headers (a `HeaderMap` extractor would clone them all).
pub(crate) struct Presented(pub(crate) Option<Credential>);

impl<S: Send + Sync> FromRequestParts<S> for Presented {
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        credential_from_headers(&parts.headers).map(Self)
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

/// The routes both listeners serve: health, the hub's description, and
/// peer discovery.
fn core_routes() -> Router<Arc<Shared>> {
    Router::new()
        .route(paths::HEALTH, get(health))
        .route(paths::HUB, get(describe))
        .route(paths::PEERS_BY_INSTANCE, get(peer_by_instance))
        .route(paths::PEERS_BY_WORKER, get(peer_by_worker))
}

/// The public listener: discovery and the extensions' public routes.
pub(crate) fn public_router(shared: &Arc<Shared>, stop: &CancellationToken) -> Router {
    let core = core_routes().with_state(Arc::clone(shared));
    mount(core, shared, stop, |extension| extension.public_routes())
}

/// The control listener: everything public, plus registration.
pub(crate) fn control_router(shared: &Arc<Shared>, stop: &CancellationToken) -> Router {
    let core = core_routes()
        .route(paths::INSTANCES, get(list).post(register))
        .route(paths::INSTANCE_BY_ID, delete(unregister))
        .route(paths::INSTANCE_HEARTBEAT, post(heartbeat))
        .route(paths::INSTANCE_PROBE, post(probe))
        .with_state(Arc::clone(shared));
    mount(core, shared, stop, |extension| extension.control_routes())
}

/// Nest each extension under `/v1/ext/{name}`. Names are validated as
/// single path segments before the hub binds, so an extension cannot reach
/// a core path.
fn mount(
    core: Router,
    shared: &Arc<Shared>,
    stop: &CancellationToken,
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
        .layer(axum::middleware::from_fn_with_state(
            stop.clone(),
            end_at_stop,
        ))
}

/// Ends a request still running when shutdown gives up waiting. axum
/// serves each connection in a task of its own that outlives the listener,
/// so without this a stuck handler would keep the hub's state alive after
/// `shutdown` returned.
async fn end_at_stop(
    State(stop): State<CancellationToken>,
    request: Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    tokio::select! {
        response = next.run(request) => response,
        _ = stop.cancelled() => HubError::unavailable("the hub stopped").into_response(),
    }
}

async fn health() -> &'static str {
    "ok"
}

async fn describe(State(shared): HubState) -> Result<Json<HubDescriptor>, HubError> {
    let mut extensions = Vec::with_capacity(shared.extensions.len());
    for extension in &shared.extensions {
        // Extension code on a public route: a panic must answer an
        // `ErrorBody`, not drop the connection.
        let descriptor =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| extension.descriptor()))
                .map_err(|_| {
                    tracing::error!(extension = extension.name(), "descriptor panicked");
                    HubError::internal(format!(
                        "extension {:?}: descriptor panicked",
                        extension.name()
                    ))
                })?;
        extensions.push(ExtensionDescriptor {
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
            descriptor,
        });
    }
    Ok(Json(HubDescriptor {
        hub_id: shared.hub_id,
        velo_instance_id: shared.velo.as_ref().map(|velo| velo.instance_id()),
        extensions,
    }))
}

async fn peer_by_instance(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
) -> Result<Json<PeerLookupResponse>, HubError> {
    // Discovery, not `get`: it copies the peer only, never the payloads.
    shared
        .store
        .discover_by_instance_id(id)
        .await
        .map(|peer| Json(PeerLookupResponse { peer }))
        .map_err(|_| HubError::not_found(format!("instance {id} not found")))
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
    Presented(presented): Presented,
    HubJson(request): HubJson<RegisterRequest>,
) -> Result<Json<RegisterResponse>, HubError> {
    registration::register(shared, presented, request)
        .await
        .map(Json)
}

async fn unregister(
    State(shared): HubState,
    HubPath(id): HubPath<InstanceId>,
    Presented(presented): Presented,
) -> Result<StatusCode, HubError> {
    registration::unregister(shared, id, presented).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn heartbeat(
    State(shared): HubState,
    Authorized { owner: id, lease }: Authorized,
) -> Result<Json<HeartbeatResponse>, HubError> {
    // Bounded like every store call the hub awaits: a stalled backend would
    // otherwise pile up one open request per registrant per heartbeat. A
    // `Stale` answer (a replacement won the race) is a 409.
    super::bounded(
        shared.config.step_timeout,
        shared.store.touch(id, lease.incarnation),
    )
    .await?;
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
    // Bounded like a heartbeat probe: a peer that never answers must not
    // hold the request, or shutdown's listener drain, open.
    let ack = super::heartbeat::probe(velo, id, 0, shared.config.heartbeat_interval)
        .await
        .map_err(|error| {
            HubError::new(
                protocol::ErrorCode::BadGateway,
                format!("probe failed: {error:#}"),
            )
        })?;
    Ok(Json(ProbeResponse {
        seq: ack.seq,
        ok: ack.ok,
    }))
}
