// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Extensions: what a hub hosts besides discovery.
//!
//! An extension has a wire name, routes under `/v1/ext/{NAME}`, and hooks
//! into every registration. The hub stores each one type-erased behind
//! [`DynExtension`]; the blanket impl decodes payloads into the
//! extension's own types once per registration.
//!
//! # Registration hooks
//!
//! For one registration the hub runs, with every extension in start order:
//!
//! ```text
//! admit(payload)            before the lock; reject malformed or misconfigured
//! -- registration lock --
//! prepare(txn, payload)     may hide or revoke; owned by Staged; may fail
//! -- store write --
//! commit(staged, reg)       infallible; makes the new registration visible
//! on_registered(reg)        must not block; spawn long work
//! -- unlock --
//! on_removed(removal)       later, under the lock, for each removed incarnation
//!                           (can arrive twice; see the trait rules)
//! ```
//!
//! Every extension sees every registration; `payload` is `None` when the
//! registrant did not opt in, and `commit` with no payload drops what the
//! owner had before.

use std::any::Any;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use axum::Router;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use velo::{InstanceId, PeerInfo};

use super::authority::Lease;
use super::context::{AttachContext, StartContext};
use crate::protocol::{ErrorCode, RegistrationEpoch};
use crate::store::{Incarnation, Removal};

/// A hub extension. See the module docs for the hook order.
///
/// # Rules for registration hooks
///
/// - `prepare` may only *hide* or *revoke* (never add, never destroy), and
///   every such change must be owned by `Staged` and undone by its `Drop`.
///   Dropping `Staged` is the abort.
/// - `prepare` runs under a deadline and its future can be dropped at any
///   await. Create the guard that undoes a change before the first await
///   after the change, and move it into `Staged`.
/// - An undo in `Staged::drop` must be fenced by epoch: after a failure
///   that removed the owner (`on_removed` ran first), it must do nothing.
/// - `commit` cannot fail; check everything in `prepare`. Nothing else
///   changes extension state between them, because `commit`,
///   `on_registered` and `on_removed` all run under the hub-wide
///   registration lock. A `commit` that panics anyway drops its own
///   `Staged` during the unwind, before the hub delivers `on_removed`, so
///   that one undo can briefly show the state it hid. (The hub delivers the
///   removal before it drops every other extension's `Staged`.)
/// - `on_removed` may be delivered twice for one removal, and may name an
///   incarnation older than the one committed; ignore those.
/// - An owner-scoped mutation outside the hooks (a route, a velo handler)
///   authorizes through [`Authority::authorize`](crate::Authority::authorize)
///   and applies only if the lease's epoch equals the epoch the extension
///   committed for that owner, checked under the extension's own lock in
///   the same critical section as the write.
/// - Handlers and tasks that reach the hub's [`Velo`](velo::Velo) hold a
///   [`WeakVelo`](velo::WeakVelo) and upgrade per call. A strong clone in a
///   handler registered on that same instance keeps it alive forever.
pub trait Extension: Send + Sync + 'static {
    /// Wire name: `[a-z0-9_-]{1,64}`. Routes mount at `/v1/ext/{NAME}`;
    /// registration payloads key on it.
    const NAME: &'static str;

    /// What a registrant sends when it opts in. Use `()` for "nothing"
    /// (sent as `null`).
    type Registration: DeserializeOwned + Serialize + Send + 'static;

    /// A prepared, not yet visible registration. Dropping it aborts.
    type Staged: Send + 'static;

    /// Hub-side extensions this one needs. Drives attach and start order
    /// and gates [`AttachContext::extension`].
    fn requires() -> &'static [&'static str] {
        &[]
    }

    /// Extensions a registrant must also opt into when it opts into this one.
    fn registration_requires() -> &'static [&'static str] {
        &[]
    }

    /// Wire the extension into the hub. Runs once, in dependency order,
    /// before any socket serves.
    fn attach(&self, _ctx: &AttachContext<'_>) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Start background work. Runs once, in dependency order, after every
    /// extension attached and before the hub serves.
    fn start(&self, _ctx: &StartContext<'_>) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }

    /// Stop. Runs once, in reverse dependency order, at hub shutdown or
    /// after a failed startup that attached this extension.
    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Routes on the public listener, mounted under `/v1/ext/{NAME}`.
    fn public_routes(self: Arc<Self>) -> Router {
        Router::new()
    }

    /// Routes on the control listener, mounted under `/v1/ext/{NAME}`.
    fn control_routes(self: Arc<Self>) -> Router {
        Router::new()
    }

    /// What `GET /v1/hub` shows for this extension.
    fn descriptor(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    /// Reject a registration before the hub takes its lock. Configuration
    /// checks belong here; this cannot see or change hub state.
    fn admit(
        &self,
        _view: &RegistrationView<'_>,
        _payload: Option<&Self::Registration>,
    ) -> Result<(), Rejection> {
        Ok(())
    }

    /// Validate and stage the registration. See the trait rules.
    fn prepare(
        &self,
        txn: &Txn<'_>,
        payload: Option<Self::Registration>,
    ) -> impl Future<Output = Result<Self::Staged, Rejection>> + Send;

    /// Make the staged registration visible. Cannot fail.
    fn commit(&self, staged: Self::Staged, registration: &Committed<'_>);

    /// After every extension committed and the credential is live. Runs
    /// under the lock and under `step_timeout_ms`: at the deadline the hub
    /// drops this future at whatever await it is on, logs it, and the
    /// registration still succeeds. Spawn anything that awaits.
    fn on_registered(&self, _registration: &Committed<'_>) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// The owner's incarnation left the store. Drop the owner's state if it
    /// belongs to `removal.incarnation` or an older one.
    fn on_removed(&self, removal: Removal);
}

/// Why an extension refused a registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub(crate) code: ErrorCode,
    pub(crate) message: String,
}

impl Rejection {
    /// 400: the payload is malformed or does not fit the hub's config.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::BadRequest,
            message: message.into(),
        }
    }

    /// 409: the registration conflicts with the extension's state.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Conflict,
            message: message.into(),
        }
    }

    /// 503: the extension cannot take registrations right now.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Unavailable,
            message: message.into(),
        }
    }

    /// 500: an unexpected failure.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: ErrorCode::Internal,
            message: message.into(),
        }
    }

    /// The error code the hub answers with.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// The message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Rejection {}

/// The request as [`Extension::admit`] sees it.
#[derive(Debug, Clone, Copy)]
pub struct RegistrationView<'a> {
    /// The registrant.
    pub peer: &'a PeerInfo,
    /// Every payload in the request, keyed by extension name.
    pub extensions: &'a BTreeMap<String, serde_json::Value>,
}

impl RegistrationView<'_> {
    /// Whether the registrant opted into extension `name`.
    pub fn opted_in(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }
}

/// One registration in flight, as [`Extension::prepare`] sees it.
#[derive(Debug, Clone, Copy)]
pub struct Txn<'a> {
    /// The registrant's instance.
    pub owner: InstanceId,
    /// The registrant.
    pub peer: &'a PeerInfo,
    /// The epoch this registration will have if it commits.
    pub epoch: RegistrationEpoch,
    /// The registration this one replaces, if the owner had one.
    pub previous: Option<Lease>,
    /// Every payload in the request, keyed by extension name.
    pub extensions: &'a BTreeMap<String, serde_json::Value>,
}

impl Txn<'_> {
    /// Whether the registrant opted into extension `name`.
    pub fn opted_in(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }
}

/// A registration that the store holds, as [`Extension::commit`] and
/// [`Extension::on_registered`] see it.
#[derive(Debug, Clone, Copy)]
pub struct Committed<'a> {
    /// The registrant's instance.
    pub owner: InstanceId,
    /// The registrant.
    pub peer: &'a PeerInfo,
    /// The store incarnation it was written as.
    pub incarnation: Incarnation,
    /// Its epoch.
    pub epoch: RegistrationEpoch,
    /// The registration it replaced, if any.
    pub previous: Option<Lease>,
    /// Every payload, keyed by extension name.
    pub extensions: &'a BTreeMap<String, serde_json::Value>,
}

impl Committed<'_> {
    /// Whether the registrant opted into extension `name`.
    pub fn opted_in(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }
}

pub(crate) type AnyPayload = Box<dyn Any + Send>;
pub(crate) type AnyStaged = Box<dyn Any + Send>;

/// An extension with its types erased, as the hub stores it.
pub(crate) trait DynExtension: Send + Sync {
    fn name(&self) -> &'static str;
    fn requires(&self) -> &'static [&'static str];
    fn registration_requires(&self) -> &'static [&'static str];
    /// The concrete `Arc<E>`, for typed lookup.
    fn as_any(&self) -> Arc<dyn Any + Send + Sync>;
    fn attach<'a>(&'a self, ctx: &'a AttachContext<'a>) -> BoxFuture<'a, anyhow::Result<()>>;
    fn start<'a>(&'a self, ctx: &'a StartContext<'a>) -> BoxFuture<'a, anyhow::Result<()>>;
    fn shutdown(&self) -> BoxFuture<'_, ()>;
    fn public_routes(&self) -> Router;
    fn control_routes(&self) -> Router;
    fn descriptor(&self) -> serde_json::Value;
    fn decode(&self, value: serde_json::Value) -> Result<AnyPayload, serde_json::Error>;
    fn admit(
        &self,
        view: &RegistrationView<'_>,
        payload: Option<&AnyPayload>,
    ) -> Result<(), Rejection>;
    fn prepare<'a>(
        &'a self,
        txn: &'a Txn<'a>,
        payload: Option<AnyPayload>,
    ) -> BoxFuture<'a, Result<AnyStaged, Rejection>>;
    fn commit(&self, staged: AnyStaged, registration: &Committed<'_>);
    fn on_registered<'a>(&'a self, registration: &'a Committed<'a>) -> BoxFuture<'a, ()>;
    fn on_removed(&self, removal: Removal);
}

pub(crate) struct Erased<E>(pub(crate) Arc<E>);

impl<E: Extension> DynExtension for Erased<E> {
    fn name(&self) -> &'static str {
        E::NAME
    }

    fn requires(&self) -> &'static [&'static str] {
        E::requires()
    }

    fn registration_requires(&self) -> &'static [&'static str] {
        E::registration_requires()
    }

    fn as_any(&self) -> Arc<dyn Any + Send + Sync> {
        Arc::clone(&self.0) as Arc<dyn Any + Send + Sync>
    }

    fn attach<'a>(&'a self, ctx: &'a AttachContext<'a>) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(self.0.attach(ctx))
    }

    fn start<'a>(&'a self, ctx: &'a StartContext<'a>) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(self.0.start(ctx))
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.0.shutdown())
    }

    fn public_routes(&self) -> Router {
        Arc::clone(&self.0).public_routes()
    }

    fn control_routes(&self) -> Router {
        Arc::clone(&self.0).control_routes()
    }

    fn descriptor(&self) -> serde_json::Value {
        self.0.descriptor()
    }

    fn decode(&self, value: serde_json::Value) -> Result<AnyPayload, serde_json::Error> {
        serde_json::from_value::<E::Registration>(value)
            .map(|payload| Box::new(payload) as AnyPayload)
    }

    fn admit(
        &self,
        view: &RegistrationView<'_>,
        payload: Option<&AnyPayload>,
    ) -> Result<(), Rejection> {
        let typed = payload.and_then(|payload| payload.downcast_ref::<E::Registration>());
        self.0.admit(view, typed)
    }

    fn prepare<'a>(
        &'a self,
        txn: &'a Txn<'a>,
        payload: Option<AnyPayload>,
    ) -> BoxFuture<'a, Result<AnyStaged, Rejection>> {
        let typed = payload.map(|payload| {
            *payload
                .downcast::<E::Registration>()
                .expect("the hub decodes each payload with its own extension")
        });
        Box::pin(async move {
            self.0
                .prepare(txn, typed)
                .await
                .map(|staged| Box::new(staged) as AnyStaged)
        })
    }

    fn commit(&self, staged: AnyStaged, registration: &Committed<'_>) {
        let staged = *staged
            .downcast::<E::Staged>()
            .expect("the hub commits each staged value with its own extension");
        self.0.commit(staged, registration);
    }

    fn on_registered<'a>(&'a self, registration: &'a Committed<'a>) -> BoxFuture<'a, ()> {
        Box::pin(self.0.on_registered(registration))
    }

    fn on_removed(&self, removal: Removal) {
        self.0.on_removed(removal);
    }
}
