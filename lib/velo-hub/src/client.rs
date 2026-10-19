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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use reqwest::{Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use velo::discovery::PeerDiscovery;
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

    /// A configured `reqwest::Client` (timeouts, TLS). A default one is
    /// built otherwise.
    pub fn http_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    /// Build the client. It holds no registration yet.
    pub fn build(self) -> Result<HubClient, ClientError> {
        let host = self
            .host
            .ok_or_else(|| ClientError::InvalidUrl("no host".to_string()))?;
        let url = |port: u16| {
            Url::parse(&format!("{}://{host}:{port}", self.scheme))
                .map_err(|error| ClientError::InvalidUrl(format!("{host}:{port}: {error}")))
        };
        let http = match self.http {
            Some(http) => http,
            None => reqwest::Client::builder().build()?,
        };
        let endpoints = Arc::new(Endpoints {
            http,
            public: url(self.public_port)?,
            control: url(self.control_port)?,
        });
        Ok(HubClient {
            inner: Arc::new(Inner {
                endpoints,
                registration: tokio::sync::Mutex::new(None),
                held: parking_lot::Mutex::new(None),
                heartbeat: Arc::new(HubHeartbeat::default()),
            }),
        })
    }
}

/// The HTTP client and the two listener URLs, shared by every handle.
#[derive(Debug)]
struct Endpoints {
    http: reqwest::Client,
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
/// `null`, so `T = ()` reads it.
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

/// What the client keeps of its registration.
#[derive(Debug, Clone)]
struct Held {
    response: RegisterResponse,
}

/// What the client's heartbeat handler reads and records.
#[derive(Debug, Default)]
struct HubHeartbeat {
    /// The handler's answer: `true` while the client holds a registration
    /// or is acquiring one. `false` tells the hub that a registration it
    /// holds for this peer has no owner here (its response was lost), so
    /// the hub evicts it and the peer can register again.
    holding: AtomicBool,
    seq: AtomicU64,
    at_unix_ms: AtomicU64,
}

struct Inner {
    endpoints: Arc<Endpoints>,
    /// The registration. Held across register and unregister requests, so
    /// they never interleave.
    registration: tokio::sync::Mutex<Option<Held>>,
    /// A copy of the registration for sync reads, written under
    /// `registration`.
    held: parking_lot::Mutex<Option<Held>>,
    heartbeat: Arc<HubHeartbeat>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let Some(held) = self.registration.get_mut().take() else {
            return;
        };
        // Best effort: an awaiting caller should use `unregister`. Without a
        // runtime there is nothing to send the request on.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let endpoints = Arc::clone(&self.endpoints);
        runtime.spawn(async move {
            let _ = delete(&endpoints, &held).await;
        });
    }
}

/// `DELETE` the registration. Ok when it is gone: deleted now, or unknown
/// to the hub (404), or no longer this credential's (401).
async fn delete(endpoints: &Endpoints, held: &Held) -> Result<(), ClientError> {
    let url = Endpoints::join(
        &endpoints.control,
        &protocol::instance_by_id(held.response.instance_id),
    )?;
    match endpoints
        .call::<(), ()>(Method::DELETE, url, Some(&held.response.credential), None)
        .await
    {
        Err(ClientError::Hub {
            code: ErrorCode::NotFound | ErrorCode::Unauthorized,
            ..
        }) => Ok(()),
        other => other,
    }
}

/// A client of one hub. Cheap to clone; every clone shares the one
/// registration, and dropping the last clone unregisters it in the
/// background. Prefer [`unregister`](Self::unregister) where you can await.
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
        velo.register_handler(heartbeat_handler(Arc::downgrade(&self.inner.heartbeat)))
            .map_err(ClientError::Velo)?;
        self.register_peer(velo.peer_info(), extensions).await
    }

    /// Register `peer`. If the client already holds a registration of this
    /// peer, it presents that credential and the hub replaces the
    /// registration: a new credential and a new epoch, and the old
    /// credential stops working. An `unauthorized` or `register_again`
    /// answer to such a call drops the registration the client held, so the
    /// next call registers afresh.
    pub async fn register_peer(
        &self,
        peer: PeerInfo,
        extensions: Extensions,
    ) -> Result<RegisterResponse, ClientError> {
        let mut registration = self.inner.registration.lock().await;
        let presented = match registration.as_ref() {
            Some(held) if held.response.instance_id != peer.instance_id() => {
                return Err(ClientError::AlreadyRegistered(held.response.instance_id));
            }
            Some(held) => Some(held.response.credential.clone()),
            None => None,
        };
        // Answer probes ok from the moment the hub may commit: its first
        // probe can arrive before this call returns.
        self.inner.heartbeat.holding.store(true, Ordering::Release);
        let endpoints = &self.inner.endpoints;
        let url = Endpoints::join(&endpoints.control, protocol::paths::INSTANCES)?;
        let request = RegisterRequest { peer, extensions };
        let answer: Result<RegisterResponse, ClientError> = endpoints
            .call(Method::POST, url, presented.as_ref(), Some(&request))
            .await;
        let result = match answer {
            Ok(response) => {
                let held = Held {
                    response: response.clone(),
                };
                *self.inner.held.lock() = Some(held.clone());
                *registration = Some(held);
                Ok(response)
            }
            Err(error) => {
                if matches!(
                    error.code(),
                    Some(ErrorCode::Unauthorized | ErrorCode::RegisterAgain)
                ) {
                    *self.inner.held.lock() = None;
                    *registration = None;
                }
                Err(error)
            }
        };
        self.inner
            .heartbeat
            .holding
            .store(registration.is_some(), Ordering::Release);
        result
    }

    /// Unregister. Without a registration this does nothing. On failure the
    /// client keeps the registration, so a retry, or the drop, sends the
    /// `DELETE` again.
    pub async fn unregister(&self) -> Result<(), ClientError> {
        let mut registration = self.inner.registration.lock().await;
        let Some(held) = registration.as_ref() else {
            return Ok(());
        };
        delete(&self.inner.endpoints, held).await?;
        *self.inner.held.lock() = None;
        *registration = None;
        self.inner.heartbeat.holding.store(false, Ordering::Release);
        Ok(())
    }

    /// Whether the client holds a registration.
    pub fn is_registered(&self) -> bool {
        self.inner.held.lock().is_some()
    }

    /// The hub's answer to the registration the client holds: instance id,
    /// the hub's velo instance id, credential and epoch.
    pub fn registration(&self) -> Option<RegisterResponse> {
        self.inner
            .held
            .lock()
            .as_ref()
            .map(|held| held.response.clone())
    }

    /// Refresh the registration's liveness over HTTP. The hub's own velo
    /// heartbeat does this for a client registered with
    /// [`register`](Self::register); this is for a hub without velo.
    pub async fn send_heartbeat(&self) -> Result<(), ClientError> {
        let held = self
            .inner
            .held
            .lock()
            .clone()
            .ok_or(ClientError::NotRegistered)?;
        let endpoints = &self.inner.endpoints;
        let url = Endpoints::join(
            &endpoints.control,
            &protocol::instance_heartbeat(held.response.instance_id),
        )?;
        let answer: HeartbeatResponse = endpoints
            .call::<(), _>(Method::POST, url, Some(&held.response.credential), None)
            .await?;
        if !answer.acknowledged {
            return Err(ClientError::Unexpected {
                status: StatusCode::OK,
                body: "heartbeat not acknowledged".to_string(),
            });
        }
        Ok(())
    }

    /// The last hub heartbeat the handler installed by
    /// [`register`](Self::register) answered: its sequence number and when
    /// it arrived. `None` before the first.
    pub fn last_hub_heartbeat(&self) -> Option<(u64, SystemTime)> {
        let at = self.inner.heartbeat.at_unix_ms.load(Ordering::Acquire);
        (at != 0).then(|| {
            (
                self.inner.heartbeat.seq.load(Ordering::Acquire),
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

    /// Calls to the routes of the extension named `name`.
    pub fn ext(&self, name: &str) -> ExtClient {
        ExtClient {
            client: self.clone(),
            root: protocol::extension_root(name),
        }
    }
}

/// The client's velo heartbeat handler. It holds a `Weak` of what it
/// records, so velo keeping the handler keeps no client alive.
fn heartbeat_handler(heartbeat: Weak<HubHeartbeat>) -> Handler {
    Handler::typed_unary_async::<HeartbeatRequest, HeartbeatAck, _, _>(
        HEARTBEAT_HANDLER,
        move |ctx| {
            let heartbeat = heartbeat.clone();
            async move {
                let seq = ctx.input.seq;
                // A dropped client holds nothing: answer not ok.
                let Some(heartbeat) = heartbeat.upgrade() else {
                    return Ok(HeartbeatAck { seq, ok: false });
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(1, |since| since.as_millis().max(1) as u64);
                heartbeat.seq.store(seq, Ordering::Release);
                heartbeat.at_unix_ms.store(now, Ordering::Release);
                Ok(HeartbeatAck {
                    seq,
                    ok: heartbeat.holding.load(Ordering::Acquire),
                })
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
/// builds its typed client on this.
#[derive(Debug, Clone)]
pub struct ExtClient {
    client: HubClient,
    root: String,
}

impl ExtClient {
    fn url(&self, base: &Url, path: &str) -> Result<Url, ClientError> {
        Endpoints::join(base, &format!("{}{path}", self.root))
    }

    fn credential(&self) -> Result<Credential, ClientError> {
        self.client
            .inner
            .held
            .lock()
            .as_ref()
            .map(|held| held.response.credential.clone())
            .ok_or(ClientError::NotRegistered)
    }

    /// `GET` on the public listener.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let endpoints = &self.client.inner.endpoints;
        let url = self.url(&endpoints.public, path)?;
        endpoints.call::<(), _>(Method::GET, url, None, None).await
    }

    /// `POST` with a JSON body on the public listener.
    pub async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let endpoints = &self.client.inner.endpoints;
        let url = self.url(&endpoints.public, path)?;
        endpoints.call(Method::POST, url, None, Some(body)).await
    }

    /// `GET` on the control listener, with the credential.
    pub async fn control_get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let credential = self.credential()?;
        let endpoints = &self.client.inner.endpoints;
        let url = self.url(&endpoints.control, path)?;
        endpoints
            .call::<(), _>(Method::GET, url, Some(&credential), None)
            .await
    }

    /// `POST` with a JSON body on the control listener, with the credential.
    pub async fn control_post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let credential = self.credential()?;
        let endpoints = &self.client.inner.endpoints;
        let url = self.url(&endpoints.control, path)?;
        endpoints
            .call(Method::POST, url, Some(&credential), Some(body))
            .await
    }

    /// `DELETE` on the control listener, with the credential.
    pub async fn control_delete<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let credential = self.credential()?;
        let endpoints = &self.client.inner.endpoints;
        let url = self.url(&endpoints.control, path)?;
        endpoints
            .call::<(), _>(Method::DELETE, url, Some(&credential), None)
            .await
    }
}
