// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The hub client: registration, discovery, and extension calls over HTTP.
//! Ported from kvbm-hub `src/client.rs`.
//!
//! ```text
//! HubClient    one registration (credential, epoch, hub velo id); HTTP to both
//!              listeners; dropping the last clone sends DELETE
//! HubDiscovery PeerDiscovery for velo; shares the HTTP client, holds no
//!              registration, so velo keeping it does not keep a registration
//! ExtClient    JSON calls under /v1/ext/{name}; control calls carry the credential
//! ```
//!
//! A typical program:
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use velo_hub::client::HubClient;
//! let hub = HubClient::builder().host("hub.example").build()?;
//! let velo = hub.velo_builder().build().await?;
//! hub.register(&velo, Default::default()).await?;
//! // ... velo finds every other registered peer through the hub ...
//! hub.unregister().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::pin::Pin;

use futures::{Stream, StreamExt};
use velo::discovery::{PeerDiscovery, ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};
use velo::{Handler, InstanceId, PeerInfo, Velo, VeloBuilder, WorkerId};

use crate::protocol::{
    self, CREDENTIAL_HEADER, Credential, DEFAULT_CONTROL_PORT, DEFAULT_PUBLIC_PORT, ErrorBody,
    ErrorCode, HEARTBEAT_HANDLER, HeartbeatAck, HeartbeatRequest, HeartbeatResponse, HubDescriptor,
    PeerLookupResponse, RegisterRequest, RegisterResponse,
};

/// Extension payloads of a registration, keyed by extension name.
pub type Extensions = BTreeMap<String, serde_json::Value>;

/// A failed hub call.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The hub answered with an [`ErrorBody`]. Act on [`code`](Self::code):
    /// `busy` means retry, `register_again` means register without a
    /// credential.
    #[error("hub answered {status}: {code:?}: {message}")]
    Hub {
        /// The HTTP status.
        status: StatusCode,
        /// The hub's error code.
        code: ErrorCode,
        /// The hub's message.
        message: String,
    },
    /// The hub answered an error without an [`ErrorBody`] (not a velo hub,
    /// or a proxy in between).
    #[error("hub answered {status}: {body}")]
    Unexpected {
        /// The HTTP status.
        status: StatusCode,
        /// The start of the body.
        body: String,
    },
    /// The request did not complete, or the answer did not decode.
    #[error("hub request failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// The call needs a registration and the client holds none.
    #[error("the client holds no registration")]
    NotRegistered,
    /// The client holds a registration of another instance.
    #[error("the client already holds the registration of instance {0}")]
    AlreadyRegistered(InstanceId),
    /// The hub URL is not valid.
    #[error("invalid hub url: {0}")]
    InvalidUrl(String),
    /// The local velo instance refused a step (installing the heartbeat
    /// handler).
    #[error("velo: {0:#}")]
    Velo(anyhow::Error),
}

impl ClientError {
    /// The hub's error code, if the hub answered one.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Hub { code, .. } => Some(*code),
            _ => None,
        }
    }
}

/// Builder for [`HubClient`]. Only the host is required.
#[derive(Debug, Clone)]
pub struct HubClientBuilder {
    host: Option<String>,
    scheme: String,
    public_port: u16,
    control_port: u16,
    http: Option<reqwest::Client>,
}

impl Default for HubClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            scheme: "http".to_string(),
            public_port: DEFAULT_PUBLIC_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            http: None,
        }
    }
}

impl HubClientBuilder {
    /// Scheme, host and public port from the hub's public URL (for example
    /// `http://hub:1337`). The control port keeps its default unless set.
    pub fn from_url(url: &str) -> Result<Self, ClientError> {
        let parsed =
            Url::parse(url).map_err(|error| ClientError::InvalidUrl(format!("{url}: {error}")))?;
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| ClientError::InvalidUrl(format!("{url}: no host")))?
            .to_string();
        Ok(Self::default()
            .scheme(parsed.scheme())
            .host(host)
            .public_port(
                parsed
                    .port_or_known_default()
                    .unwrap_or(DEFAULT_PUBLIC_PORT),
            ))
    }

    /// The hub's host name or address. Required.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// URL scheme, `http` by default.
    pub fn scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = scheme.into();
        self
    }

    /// Port of the public listener (discovery, public extension routes).
    pub fn public_port(mut self, port: u16) -> Self {
        self.public_port = port;
        self
    }

    /// Port of the control listener (registration, control extension routes).
    pub fn control_port(mut self, port: u16) -> Self {
        self.control_port = port;
        self
    }

    /// A configured `reqwest::Client` (TLS, proxies, timeouts). It keeps its
    /// own timeouts: without one, a call to a hub that is gone waits for
    /// ever. It also serves service watches, where a total request timeout
    /// ends every watch after that long; prefer a read timeout. Without
    /// this, the client uses [`DEFAULT_CONNECT_TIMEOUT`] and
    /// [`DEFAULT_REQUEST_TIMEOUT`], and [`DEFAULT_WATCH_IDLE_TIMEOUT`] for
    /// watches.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Build the client. It holds no registration yet.
    pub fn build(self) -> Result<HubClient, ClientError> {
        let host = self
            .host
            .ok_or_else(|| ClientError::InvalidUrl("no host".to_string()))?;
        // An IPv6 literal needs brackets in a URL.
        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let url = |port: u16| {
            Url::parse(&format!("{}://{host}:{port}", self.scheme))
                .map_err(|error| ClientError::InvalidUrl(format!("{host}:{port}: {error}")))
        };
        // A watch is one long response, so the default watch client has no
        // total timeout, only an idle one longer than the hub's keep-alive.
        let (http, watch) = match self.http {
            Some(http) => (http.clone(), http),
            None => (
                reqwest::Client::builder()
                    .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                    .timeout(DEFAULT_REQUEST_TIMEOUT)
                    .build()?,
                reqwest::Client::builder()
                    .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                    .read_timeout(DEFAULT_WATCH_IDLE_TIMEOUT)
                    .build()?,
            ),
        };
        let endpoints = Arc::new(Endpoints {
            http,
            watch,
            public: url(self.public_port)?,
            control: url(self.control_port)?,
        });
        Ok(HubClient {
            inner: Arc::new(Inner {
                endpoints,
                op: tokio::sync::Mutex::new(()),
                shared: Arc::new(Shared::default()),
            }),
        })
    }
}

/// Connect timeout of the client's default HTTP client.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Request timeout of the client's default HTTP client. Longer than a
/// registration can take on a hub with default settings and up to three
/// extensions: 5 s for the lock, then 5 s for each of `2n + 3` steps.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Idle timeout of a service watch on the default client: no bytes for
/// this long (the hub sends a keep-alive every 10 s) ends the watch with
/// `Disconnected`.
pub const DEFAULT_WATCH_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The HTTP clients and the two listener URLs, shared by every handle.
#[derive(Debug)]
struct Endpoints {
    http: reqwest::Client,
    /// For service watches: no total timeout.
    watch: reqwest::Client,
    public: Url,
    control: Url,
}

impl Endpoints {
    fn join(base: &Url, path: &str) -> Result<Url, ClientError> {
        base.join(path)
            .map_err(|error| ClientError::InvalidUrl(format!("{base}{path}: {error}")))
    }

    async fn call<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        method: Method,
        url: Url,
        credential: Option<&Credential>,
        body: Option<&B>,
    ) -> Result<T, ClientError> {
        let mut request = self.http.request(method, url);
        if let Some(credential) = credential {
            request = request.header(CREDENTIAL_HEADER, credential.to_header_value());
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        decode(request.send().await?).await
    }

    async fn lookup(&self, path: String) -> Result<PeerInfo, ClientError> {
        let url = Self::join(&self.public, &path)?;
        let found: PeerLookupResponse = self.call::<(), _>(Method::GET, url, None, None).await?;
        Ok(found.peer)
    }
}

/// Decode a hub answer. A success with an empty body (204) decodes as JSON
/// `null`, so `T = ()` reads it; use `serde::de::IgnoredAny` to ignore any
/// body.
async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, ClientError> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if status.is_success() {
        let body: &[u8] = if bytes.is_empty() { b"null" } else { &bytes };
        return serde_json::from_slice(body).map_err(|error| ClientError::Unexpected {
            status,
            body: format!("undecodable success body: {error}"),
        });
    }
    match serde_json::from_slice::<ErrorBody>(&bytes) {
        Ok(error) => Err(ClientError::Hub {
            status,
            code: error.code,
            message: error.message,
        }),
        Err(_) => Err(ClientError::Unexpected {
            status,
            body: String::from_utf8_lossy(&bytes[..bytes.len().min(256)]).into_owned(),
        }),
    }
}

/// The registration, as the client knows it. One cell, so no reader sees
/// two copies disagree.
#[derive(Debug, Default)]
struct State {
    /// The hub's answer to the registration the client holds.
    response: Option<RegisterResponse>,
    /// A register call is running. The hub may commit before the client
    /// sees the answer, and its first probe may arrive first.
    acquiring: bool,
    /// The hub refused a register without a credential: it holds a
    /// registration of this instance that the client never got. Until a
    /// register succeeds, the handler answers not ok even while a call is
    /// in flight, so retries cannot keep that registration alive.
    orphaned: bool,
}

impl State {
    /// The heartbeat handler's answer.
    fn answer_ok(&self) -> bool {
        self.response.is_some() || (self.acquiring && !self.orphaned)
    }
}

/// What the client's handles share: the registration and what the
/// heartbeat handler records. The heartbeat handler and [`ExtClient`] hold
/// it weakly, so neither keeps the registration alive.
#[derive(Debug, Default)]
struct Shared {
    state: parking_lot::Mutex<State>,
    seq: AtomicU64,
    at_unix_ms: AtomicU64,
}

impl Shared {
    fn credential(&self) -> Option<(InstanceId, Credential)> {
        self.state
            .lock()
            .response
            .as_ref()
            .map(|response| (response.instance_id, response.credential.clone()))
    }

    /// Drop the registration if it is still the one with `credential`: a
    /// concurrent re-registration may have replaced it.
    fn forget(&self, credential: &Credential) {
        let mut state = self.state.lock();
        if state
            .response
            .as_ref()
            .is_some_and(|response| &response.credential == credential)
        {
            state.response = None;
        }
    }
}

/// One register call. Its `Drop` runs on every exit, also when the future
/// is dropped: it clears [`State::acquiring`], and if the call presented a
/// credential and got no answer, it drops the registration. The hub may
/// have committed the re-registration and rotated the credential, so the
/// client cannot know whether it still holds anything; holding a dead
/// credential would answer the hub's probes ok for a registration nobody
/// can use. Failing closed lets the hub free it.
struct Acquiring<'a> {
    shared: &'a Shared,
    presented: Option<Credential>,
    answered: bool,
}

impl<'a> Acquiring<'a> {
    fn start(shared: &'a Shared, presented: Option<Credential>) -> Self {
        shared.state.lock().acquiring = true;
        Self {
            shared,
            presented,
            answered: false,
        }
    }
}

impl Drop for Acquiring<'_> {
    fn drop(&mut self) {
        if !self.answered
            && let Some(credential) = &self.presented
        {
            self.shared.forget(credential);
        }
        self.shared.state.lock().acquiring = false;
    }
}

/// An answer that says the hub does not accept this credential (any more).
fn credential_rejected(error: &ClientError) -> bool {
    match error {
        ClientError::Hub { code, .. } => {
            matches!(code, ErrorCode::Unauthorized | ErrorCode::RegisterAgain)
        }
        ClientError::Unexpected { status, .. } => *status == StatusCode::UNAUTHORIZED,
        _ => false,
    }
}

/// An answer on a core route that says this credential owns nothing on the
/// hub: rejected, or the instance is unknown (404). Not for extension
/// routes, where a 404 can be the extension's own miss.
fn owns_nothing(error: &ClientError) -> bool {
    credential_rejected(error) || error.code() == Some(ErrorCode::NotFound)
}

struct Inner {
    endpoints: Arc<Endpoints>,
    /// Serializes register and unregister calls; holds no data.
    op: tokio::sync::Mutex<()>,
    shared: Arc<Shared>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let Some(response) = self.shared.state.lock().response.take() else {
            return;
        };
        // Best effort: a program that can await calls `unregister`. Without
        // a runtime there is nothing to send the request on, and a runtime
        // that shuts down drops the task.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let endpoints = Arc::clone(&self.endpoints);
        runtime.spawn(async move {
            let _ = delete(&endpoints, response.instance_id, &response.credential).await;
        });
    }
}

/// `DELETE` the registration. Ok when this credential owns nothing there
/// any more: deleted now, unknown to the hub (404), or no longer this
/// credential's (401). In the 401 case the hub may still hold a later
/// registration of the peer whose answer this client lost; the hub's probes
/// remove it, because the client then answers them not ok.
async fn delete(
    endpoints: &Endpoints,
    instance_id: InstanceId,
    credential: &Credential,
) -> Result<(), ClientError> {
    let url = Endpoints::join(&endpoints.control, &protocol::instance_by_id(instance_id))?;
    match endpoints
        .call::<(), ()>(Method::DELETE, url, Some(credential), None)
        .await
    {
        Err(error) if owns_nothing(&error) => Ok(()),
        other => other,
    }
}

/// A client of one hub. Cheap to clone; every clone shares the one
/// registration, and dropping the last clone unregisters it in the
/// background, best effort. Prefer [`unregister`](Self::unregister) where
/// you can await.
#[derive(Clone)]
pub struct HubClient {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for HubClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubClient")
            .field("public", &self.inner.endpoints.public.as_str())
            .field("control", &self.inner.endpoints.control.as_str())
            .field("registered", &self.is_registered())
            .finish()
    }
}

impl HubClient {
    /// A builder; only the host is required.
    pub fn builder() -> HubClientBuilder {
        HubClientBuilder::default()
    }

    /// The public listener's base URL.
    pub fn public_url(&self) -> &Url {
        &self.inner.endpoints.public
    }

    /// The control listener's base URL.
    pub fn control_url(&self) -> &Url {
        &self.inner.endpoints.control
    }

    /// Peer discovery through this hub, for [`VeloBuilder::discovery`]. It
    /// holds no registration.
    pub fn discovery(&self) -> HubDiscovery {
        HubDiscovery {
            endpoints: Arc::clone(&self.inner.endpoints),
        }
    }

    /// A [`VeloBuilder`] whose discovery is this hub. Add transports, build,
    /// then [`register`](Self::register).
    pub fn velo_builder(&self) -> VeloBuilder {
        Velo::builder().discovery(Arc::new(self.discovery()))
    }

    /// Register `velo` with the hub. First installs the heartbeat handler
    /// on `velo` (replacing one installed before), so the hub's probes are
    /// answered as soon as the hub answers this call. Then as
    /// [`register_peer`](Self::register_peer).
    pub async fn register(
        &self,
        velo: &Velo,
        extensions: Extensions,
    ) -> Result<RegisterResponse, ClientError> {
        self.check_peer(velo.instance_id())?;
        velo.register_handler(heartbeat_handler(Arc::downgrade(&self.inner.shared)))
            .map_err(ClientError::Velo)?;
        self.register_peer(velo.peer_info(), extensions).await
    }

    /// Refuse a peer other than the one the client holds.
    fn check_peer(&self, peer: InstanceId) -> Result<Option<Credential>, ClientError> {
        match self.inner.shared.credential() {
            Some((held, _)) if held != peer => Err(ClientError::AlreadyRegistered(held)),
            Some((_, credential)) => Ok(Some(credential)),
            None => Ok(None),
        }
    }

    /// Register `peer`. If the client already holds a registration of this
    /// peer, it presents that credential and the hub replaces the
    /// registration: a new credential and a new epoch, and the old
    /// credential stops working. An answer that says the credential owns
    /// nothing (`unauthorized`, `register_again`) drops the registration
    /// the client held, so the next call registers afresh.
    ///
    /// Cancellation: if this future is dropped, the hub may still commit
    /// the registration. The client then holds nothing, answers the hub's
    /// probes not ok, and the hub removes that registration after
    /// `heartbeat_max_failures` probes; register again after that.
    pub async fn register_peer(
        &self,
        peer: PeerInfo,
        extensions: Extensions,
    ) -> Result<RegisterResponse, ClientError> {
        let _op = self.inner.op.lock().await;
        let presented = self.check_peer(peer.instance_id())?;
        let endpoints = &self.inner.endpoints;
        let url = Endpoints::join(&endpoints.control, protocol::paths::INSTANCES)?;
        let request = RegisterRequest { peer, extensions };
        // From here to the answer, no early return: a drop of `call` without
        // an answer means "the request may have reached the hub".
        let shared = &self.inner.shared;
        let mut call = Acquiring::start(shared, presented.clone());
        let answer: Result<RegisterResponse, ClientError> = endpoints
            .call(Method::POST, url, presented.as_ref(), Some(&request))
            .await;
        call.answered = true;
        match answer {
            Ok(response) => {
                // Set before `call` clears `acquiring`, so a probe never
                // finds neither.
                let mut state = shared.state.lock();
                state.response = Some(response.clone());
                state.orphaned = false;
                Ok(response)
            }
            Err(error) => {
                if credential_rejected(&error) {
                    match &presented {
                        Some(credential) => shared.forget(credential),
                        None => shared.state.lock().orphaned = true,
                    }
                }
                Err(error)
            }
        }
    }

    /// Unregister. Without a registration this does nothing. On failure the
    /// client keeps the registration, so a retry, or the drop, sends the
    /// `DELETE` again.
    pub async fn unregister(&self) -> Result<(), ClientError> {
        let _op = self.inner.op.lock().await;
        let Some((instance_id, credential)) = self.inner.shared.credential() else {
            return Ok(());
        };
        delete(&self.inner.endpoints, instance_id, &credential).await?;
        self.inner.shared.forget(&credential);
        Ok(())
    }

    /// Whether the client holds a registration.
    pub fn is_registered(&self) -> bool {
        self.inner.shared.state.lock().response.is_some()
    }

    /// The hub's answer to the registration the client holds: instance id,
    /// the hub's velo instance id, credential and epoch.
    pub fn registration(&self) -> Option<RegisterResponse> {
        self.inner.shared.state.lock().response.clone()
    }

    /// Refresh the registration's liveness over HTTP. The hub's own velo
    /// heartbeat does this for a client registered with
    /// [`register`](Self::register); this is for a hub without velo. An
    /// answer that says the hub no longer has the registration drops it.
    pub async fn send_heartbeat(&self) -> Result<(), ClientError> {
        let (instance_id, credential) = self
            .inner
            .shared
            .credential()
            .ok_or(ClientError::NotRegistered)?;
        let endpoints = &self.inner.endpoints;
        let url = Endpoints::join(
            &endpoints.control,
            &protocol::instance_heartbeat(instance_id),
        )?;
        let answer: Result<HeartbeatResponse, ClientError> = endpoints
            .call::<(), _>(Method::POST, url, Some(&credential), None)
            .await;
        match answer {
            Ok(answer) if answer.acknowledged => Ok(()),
            Ok(_) => Err(ClientError::Unexpected {
                status: StatusCode::OK,
                body: "heartbeat not acknowledged".to_string(),
            }),
            Err(error) => {
                if owns_nothing(&error) {
                    self.inner.shared.forget(&credential);
                }
                Err(error)
            }
        }
    }

    /// The last hub heartbeat the handler installed by
    /// [`register`](Self::register) answered: its sequence number and when
    /// it arrived. `None` before the first.
    pub fn last_hub_heartbeat(&self) -> Option<(u64, SystemTime)> {
        let shared = &self.inner.shared;
        let at = shared.at_unix_ms.load(Ordering::Acquire);
        (at != 0).then(|| {
            (
                shared.seq.load(Ordering::Acquire),
                UNIX_EPOCH + Duration::from_millis(at),
            )
        })
    }

    /// The hub's descriptor: its id, its velo instance, its extensions.
    pub async fn describe(&self) -> Result<HubDescriptor, ClientError> {
        let endpoints = &self.inner.endpoints;
        let url = Endpoints::join(&endpoints.public, protocol::paths::HUB)?;
        endpoints.call::<(), _>(Method::GET, url, None, None).await
    }

    /// Service discovery through this hub, for code that takes a
    /// [`ServiceDiscovery`]. It holds no registration.
    pub fn services(&self) -> HubServices {
        HubServices {
            endpoints: Arc::clone(&self.inner.endpoints),
        }
    }

    /// Add this client's instance to the service `name`. The entry lives
    /// until the guard unregisters it or the registration ends; a
    /// re-registration with the credential keeps it. Dropping the guard
    /// removes it in the background, best effort.
    pub async fn register_service(
        &self,
        name: &str,
    ) -> Result<HubServiceRegistration, ClientError> {
        if !protocol::is_valid_service_name(name) {
            return Err(ClientError::InvalidUrl(format!(
                "{name:?} is not a service name: 1 to 128 of A-Z a-z 0-9 . _ -"
            )));
        }
        let (instance_id, credential) = self
            .inner
            .shared
            .credential()
            .ok_or(ClientError::NotRegistered)?;
        service_call(
            &self.inner.endpoints,
            Method::PUT,
            name,
            instance_id,
            &credential,
        )
        .await?;
        Ok(HubServiceRegistration {
            endpoints: Arc::clone(&self.inner.endpoints),
            registration: Arc::downgrade(&self.inner.shared),
            name: name.to_string(),
            instance_id,
            done: false,
        })
    }

    /// Calls to the routes of the extension named `name`. The `ExtClient`
    /// does not keep the registration alive: once every `HubClient` clone
    /// is dropped, its control calls answer [`ClientError::NotRegistered`].
    pub fn ext(&self, name: &str) -> ExtClient {
        ExtClient {
            endpoints: Arc::clone(&self.inner.endpoints),
            registration: Arc::downgrade(&self.inner.shared),
            root: protocol::extension_root(name),
        }
    }
}

/// The client's velo heartbeat handler. It answers ok only while the client
/// holds or is getting a registration, so the hub removes one the client
/// never got. It holds the client's state weakly: velo keeping the handler
/// keeps no registration alive, and a dropped client answers not ok.
fn heartbeat_handler(shared: Weak<Shared>) -> Handler {
    Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
        HEARTBEAT_HANDLER,
        move |ctx| {
            let shared = shared.clone();
            async move {
                let seq = ctx.input.seq;
                let Some(shared) = shared.upgrade() else {
                    return Ok(HeartbeatAck { seq, ok: false });
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(1, |since| since.as_millis().max(1) as u64);
                shared.seq.store(seq, Ordering::Release);
                shared.at_unix_ms.store(now, Ordering::Release);
                let ok = shared.state.lock().answer_ok();
                Ok(HeartbeatAck { seq, ok })
            }
        },
    )
    .build()
}

/// Peer discovery through a hub's public listener. Cheap to clone. Get one
/// from [`HubClient::discovery`].
#[derive(Debug, Clone)]
pub struct HubDiscovery {
    endpoints: Arc<Endpoints>,
}

impl PeerDiscovery for HubDiscovery {
    fn discover_by_worker_id(
        &self,
        worker_id: WorkerId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        Box::pin(async move {
            Ok(self
                .endpoints
                .lookup(protocol::peers_by_worker(worker_id))
                .await?)
        })
    }

    fn discover_by_instance_id(
        &self,
        instance_id: InstanceId,
    ) -> BoxFuture<'_, anyhow::Result<PeerInfo>> {
        Box::pin(async move {
            Ok(self
                .endpoints
                .lookup(protocol::peers_by_instance(instance_id))
                .await?)
        })
    }
}

/// JSON calls to one extension's routes, under `/v1/ext/{name}`. Paths are
/// relative to that root (`"/instances/{id}/publish"`). Calls on the
/// control listener carry the registration's credential. A bundle crate
/// builds its typed client on this. For a route whose body you do not need,
/// decode `serde::de::IgnoredAny`.
#[derive(Debug, Clone)]
pub struct ExtClient {
    endpoints: Arc<Endpoints>,
    registration: Weak<Shared>,
    root: String,
}

/// Which listener an [`ExtClient`] call goes to. Control calls always carry
/// the credential.
#[derive(Clone, Copy)]
enum Listener {
    Public,
    Control,
}

impl ExtClient {
    async fn request<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        method: Method,
        listener: Listener,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, ClientError> {
        let (base, credential) = match listener {
            Listener::Public => (&self.endpoints.public, None),
            Listener::Control => {
                let shared = self
                    .registration
                    .upgrade()
                    .ok_or(ClientError::NotRegistered)?;
                let (_, credential) = shared.credential().ok_or(ClientError::NotRegistered)?;
                (&self.endpoints.control, Some((shared, credential)))
            }
        };
        let url = Endpoints::join(base, &format!("{}{path}", self.root))?;
        let answer = self
            .endpoints
            .call(
                method,
                url,
                credential.as_ref().map(|(_, credential)| credential),
                body,
            )
            .await;
        // A rejected credential is dead for every call: drop it, as the
        // core calls do. A 404 is not: it can be the extension's own miss.
        if let (Err(error), Some((shared, credential))) = (&answer, &credential)
            && credential_rejected(error)
        {
            shared.forget(credential);
        }
        answer
    }

    /// `GET` on the public listener.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request::<(), _>(Method::GET, Listener::Public, path, None)
            .await
    }

    /// `POST` with a JSON body on the public listener.
    pub async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        self.request(Method::POST, Listener::Public, path, Some(body))
            .await
    }

    /// `GET` on the control listener, with the credential.
    pub async fn control_get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request::<(), _>(Method::GET, Listener::Control, path, None)
            .await
    }

    /// `POST` with a JSON body on the control listener, with the credential.
    pub async fn control_post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        self.request(Method::POST, Listener::Control, path, Some(body))
            .await
    }

    /// `DELETE` on the control listener, with the credential.
    pub async fn control_delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request::<(), _>(Method::DELETE, Listener::Control, path, None)
            .await
    }
}

async fn service_call(
    endpoints: &Endpoints,
    method: Method,
    name: &str,
    instance_id: InstanceId,
    credential: &Credential,
) -> Result<(), ClientError> {
    let url = Endpoints::join(
        &endpoints.control,
        &protocol::service_instance(name, instance_id),
    )?;
    endpoints
        .call::<(), ()>(method, url, Some(credential), None)
        .await
}

/// One instance's entry in one service. Holds the registration weakly:
/// when the client is gone, its registration's end removes the entry.
#[derive(Debug)]
pub struct HubServiceRegistration {
    endpoints: Arc<Endpoints>,
    registration: Weak<Shared>,
    name: String,
    instance_id: InstanceId,
    done: bool,
}

impl HubServiceRegistration {
    /// The service's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The current credential of the registration this entry belongs to:
    /// a re-registration rotates it and keeps the entry.
    fn credential(&self) -> Option<Credential> {
        self.registration
            .upgrade()
            .and_then(|shared| shared.credential())
            .filter(|(id, _)| *id == self.instance_id)
            .map(|(_, credential)| credential)
    }

    async fn remove(&mut self) -> Result<(), ClientError> {
        if self.done {
            return Ok(());
        }
        // No registration: the hub dropped the entry with it.
        if let Some(credential) = self.credential() {
            match service_call(
                &self.endpoints,
                Method::DELETE,
                &self.name,
                self.instance_id,
                &credential,
            )
            .await
            {
                Err(error) if !owns_nothing(&error) => return Err(error),
                _ => {}
            }
        }
        self.done = true;
        Ok(())
    }
}

impl ServiceRegistrationGuard for HubServiceRegistration {
    fn unregister(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move { Ok(self.remove().await?) })
    }
}

impl Drop for HubServiceRegistration {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let Some(credential) = self.credential() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let endpoints = Arc::clone(&self.endpoints);
        let (name, instance_id) = (std::mem::take(&mut self.name), self.instance_id);
        runtime.spawn(async move {
            let _ = service_call(&endpoints, Method::DELETE, &name, instance_id, &credential).await;
        });
    }
}

/// Service discovery through a hub. Cheap to clone. Get one from
/// [`HubClient::services`].
#[derive(Debug, Clone)]
pub struct HubServices {
    endpoints: Arc<Endpoints>,
}

impl HubServices {
    fn checked(name: &str) -> Result<(), ClientError> {
        if protocol::is_valid_service_name(name) {
            Ok(())
        } else {
            Err(ClientError::InvalidUrl(format!(
                "{name:?} is not a service name"
            )))
        }
    }
}

impl ServiceDiscovery for HubServices {
    fn list_services(&self) -> BoxFuture<'_, anyhow::Result<Vec<String>>> {
        Box::pin(async move {
            let url = Endpoints::join(&self.endpoints.public, protocol::paths::SERVICES)?;
            let list: protocol::ServiceListResponse = self
                .endpoints
                .call::<(), _>(Method::GET, url, None, None)
                .await?;
            Ok(list.services)
        })
    }

    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, anyhow::Result<Vec<InstanceId>>> {
        let name = service_name.to_string();
        Box::pin(async move {
            Self::checked(&name)?;
            let url = Endpoints::join(&self.endpoints.public, &protocol::service(&name))?;
            let found: protocol::ServiceInstancesResponse = self
                .endpoints
                .call::<(), _>(Method::GET, url, None, None)
                .await?;
            Ok(found.instances)
        })
    }

    fn watch_instances(
        &self,
        service_name: &str,
    ) -> BoxFuture<'_, anyhow::Result<Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>>> {
        let name = service_name.to_string();
        Box::pin(async move {
            Self::checked(&name)?;
            let url = Endpoints::join(&self.endpoints.public, &protocol::service_watch(&name))?;
            let response = self
                .endpoints
                .watch
                .get(url)
                .send()
                .await
                .map_err(ClientError::from)?;
            if !response.status().is_success() {
                return Err(decode::<()>(response).await.unwrap_err().into());
            }
            let stream: Pin<Box<dyn Stream<Item = ServiceEvent> + Send>> =
                Box::pin(watch_events(response));
            Ok(stream)
        })
    }
}

/// Parse the hub's SSE stream into [`ServiceEvent`]s. Anything that ends
/// the stream other than the hub closing it cleanly after a
/// `disconnected` (a dropped connection, an idle timeout, a hub that
/// stopped, bytes that do not parse) yields `Disconnected`, then the stream
/// ends: the watcher's set is stale either way.
fn watch_events(response: reqwest::Response) -> impl Stream<Item = ServiceEvent> + Send {
    parse_events(response.bytes_stream())
}

fn parse_events<S, B>(bytes: S) -> impl Stream<Item = ServiceEvent> + Send
where
    S: Stream<Item = reqwest::Result<B>> + Send + 'static,
    B: AsRef<[u8]>,
{
    struct State<S> {
        bytes: Pin<Box<S>>,
        buffer: Vec<u8>,
        pending: std::collections::VecDeque<ServiceEvent>,
        ended: bool,
    }
    let state = State {
        bytes: Box::pin(bytes),
        buffer: Vec::new(),
        pending: std::collections::VecDeque::new(),
        ended: false,
    };
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                if event == ServiceEvent::Disconnected {
                    state.ended = true;
                }
                return Some((event, state));
            }
            if state.ended {
                return None;
            }
            match state.bytes.next().await {
                Some(Ok(chunk)) => {
                    state.buffer.extend_from_slice(chunk.as_ref());
                    while let Some(end) = find_event_end(&state.buffer) {
                        let raw: Vec<u8> = state.buffer.drain(..end).collect();
                        match parse_event(&raw) {
                            Ok(Some(event)) => state.pending.push_back(event),
                            Ok(None) => {}
                            Err(()) => state.pending.push_back(ServiceEvent::Disconnected),
                        }
                    }
                }
                Some(Err(_)) | None => state.pending.push_back(ServiceEvent::Disconnected),
            }
        }
    })
}

/// The end of the first complete SSE event in `buffer` (past its blank
/// line), if there is one.
fn find_event_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|pair| pair == b"\n\n")
        .map(|at| at + 2)
}

/// One SSE event: `None` for a comment (keep-alive), `Err` for one that
/// does not parse.
fn parse_event(raw: &[u8]) -> Result<Option<ServiceEvent>, ()> {
    let text = std::str::from_utf8(raw).map_err(|_| ())?;
    let (mut name, mut data) = (None, String::new());
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            name = Some(value.trim());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim());
        }
    }
    let Some(name) = name else {
        return Ok(None);
    };
    let event = match name {
        protocol::WATCH_INITIAL => {
            ServiceEvent::Initial(serde_json::from_str(&data).map_err(|_| ())?)
        }
        protocol::WATCH_ADDED => ServiceEvent::Added(serde_json::from_str(&data).map_err(|_| ())?),
        protocol::WATCH_REMOVED => {
            ServiceEvent::Removed(serde_json::from_str(&data).map_err(|_| ())?)
        }
        protocol::WATCH_DISCONNECTED => ServiceEvent::Disconnected,
        _ => return Ok(None),
    };
    Ok(Some(event))
}
