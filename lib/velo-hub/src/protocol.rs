// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The hub's HTTP wire protocol: paths, request and response bodies, errors,
//! and ids. Serde types only, so it builds under every cargo feature.
//!
//! A hub serves two listeners. The **public** listener carries peer
//! discovery. The **control** listener also carries registration, heartbeat
//! and probe. Extension routes live under [`paths::EXT`]`/{name}`, on the
//! listener the extension puts them on. Request and response bodies are
//! JSON, except the `ok` text of [`paths::HEALTH`] and the empty 204 of a
//! successful unregister. Every error body is an [`ErrorBody`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use velo::{InstanceId, PeerInfo, WorkerId};

/// Default port of the public listener (peer discovery).
pub const DEFAULT_PUBLIC_PORT: u16 = 1337;

/// Default port of the control listener (registration, heartbeat, probe).
pub const DEFAULT_CONTROL_PORT: u16 = 8337;

/// Header that carries a registration's [`Credential`]. Credentials never
/// appear in URLs, so they do not reach access logs.
pub const CREDENTIAL_HEADER: &str = "x-velo-hub-credential";

/// Name of the velo handler a registrant installs so the hub can probe it.
pub const HEARTBEAT_HANDLER: &str = "velo_hub_heartbeat";

/// URL paths. Path parameters use axum's `{name}` syntax.
pub mod paths {
    /// `GET`: liveness of the hub process. Both listeners.
    pub const HEALTH: &str = "/v1/health";
    /// `GET`: the hub's ids and its extensions. Both listeners.
    pub const HUB: &str = "/v1/hub";
    /// `GET`: look a peer up by instance id. Both listeners.
    pub const PEERS_BY_INSTANCE: &str = "/v1/peers/instance/{instance_id}";
    /// `GET`: look a peer up by worker id. Both listeners.
    pub const PEERS_BY_WORKER: &str = "/v1/peers/worker/{worker_id}";
    /// `POST`: register. `GET`: list registrations. Control listener.
    pub const INSTANCES: &str = "/v1/instances";
    /// `DELETE`: unregister, with the credential. Control listener.
    pub const INSTANCE_BY_ID: &str = "/v1/instances/{instance_id}";
    /// `POST`: refresh liveness, with the credential. Control listener.
    pub const INSTANCE_HEARTBEAT: &str = "/v1/instances/{instance_id}/heartbeat";
    /// `POST`: the hub probes the instance over velo. Control listener.
    pub const INSTANCE_PROBE: &str = "/v1/instances/{instance_id}/probe";
    /// Prefix of every extension's routes: `/v1/ext/{name}/...`.
    pub const EXT: &str = "/v1/ext";
    /// `GET`: names of the services with at least one instance. Both
    /// listeners.
    pub const SERVICES: &str = "/v1/services";
    /// `GET`: the instances of one service. Both listeners.
    pub const SERVICE: &str = "/v1/services/{service}";
    /// `GET`: watch one service as server-sent events. Both listeners.
    pub const SERVICE_WATCH: &str = "/v1/services/{service}/watch";
    /// `PUT` / `DELETE`: add or remove an instance of a service, with the
    /// instance's credential. Control listener.
    pub const SERVICE_INSTANCE: &str = "/v1/services/{service}/instances/{instance_id}";
}

/// `/v1/services/{name}`.
pub fn service(name: &str) -> String {
    format!("/v1/services/{name}")
}

/// `/v1/services/{name}/watch`.
pub fn service_watch(name: &str) -> String {
    format!("/v1/services/{name}/watch")
}

/// `/v1/services/{name}/instances/{id}`.
pub fn service_instance(name: &str, id: InstanceId) -> String {
    format!("/v1/services/{name}/instances/{id}")
}

/// The name of the hub's built-in service directory. A registration's
/// payload under this name is the set of service names the registrant
/// serves; the hub sets the owner's services to exactly that set.
pub const SERVICES_EXTENSION: &str = "services";

/// The rule for service names, as error messages state it.
pub const SERVICE_NAME_RULE: &str = "1 to 128 of A-Z a-z 0-9 . _ -, not . or ..";

/// Whether `name` is a valid service name: see [`SERVICE_NAME_RULE`]. A
/// valid name is always one URL path segment.
pub fn is_valid_service_name(name: &str) -> bool {
    // `.` and `..` are dot segments: a URL resolver removes them, so a
    // request for such a name would reach another route (`..` would turn a
    // service's DELETE into an unregister).
    name != "."
        && name != ".."
        && (1..=128).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Names of the events of a service watch. Each event's data is JSON:
/// [`WATCH_INITIAL`] carries `[instance_id, ...]`, [`WATCH_ADDED`] and
/// [`WATCH_REMOVED`] one `instance_id`, [`WATCH_DISCONNECTED`] `null`.
pub const WATCH_INITIAL: &str = "initial";
/// See [`WATCH_INITIAL`].
pub const WATCH_ADDED: &str = "added";
/// See [`WATCH_INITIAL`].
pub const WATCH_REMOVED: &str = "removed";
/// See [`WATCH_INITIAL`]. The last event of a stream the hub ends early (the
/// watcher fell behind); the watcher treats its set as stale and watches
/// again.
pub const WATCH_DISCONNECTED: &str = "disconnected";

/// `/v1/peers/instance/{id}`.
pub fn peers_by_instance(id: InstanceId) -> String {
    format!("/v1/peers/instance/{id}")
}

/// `/v1/peers/worker/{id}`.
pub fn peers_by_worker(id: WorkerId) -> String {
    format!("/v1/peers/worker/{}", id.as_u64())
}

/// `/v1/instances/{id}`.
pub fn instance_by_id(id: InstanceId) -> String {
    format!("/v1/instances/{id}")
}

/// `/v1/instances/{id}/heartbeat`.
pub fn instance_heartbeat(id: InstanceId) -> String {
    format!("/v1/instances/{id}/heartbeat")
}

/// `/v1/instances/{id}/probe`.
pub fn instance_probe(id: InstanceId) -> String {
    format!("/v1/instances/{id}/probe")
}

/// `/v1/ext/{name}`, the root of one extension's routes.
pub fn extension_root(name: &str) -> String {
    format!("{}/{name}", paths::EXT)
}

/// Bearer secret that authorizes the mutations one registration owns:
/// re-register, unregister, heartbeat, and extension mutations.
///
/// The hub mints a new one on every successful registration, and the
/// previous one stops working. The hub's store keeps only a digest of it.
/// `Debug` never prints it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Credential(uuid::Uuid);

impl Credential {
    /// A fresh random credential.
    #[cfg(feature = "server")]
    pub(crate) fn generate() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// The raw bytes, for hashing and constant-time comparison.
    #[cfg(feature = "server")]
    pub(crate) fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Parse the value of a [`CREDENTIAL_HEADER`].
    pub fn from_header_str(value: &str) -> Result<Self, InvalidCredential> {
        uuid::Uuid::parse_str(value)
            .map(Self)
            .map_err(|_| InvalidCredential)
    }

    /// The value for a [`CREDENTIAL_HEADER`], marked sensitive so HTTP
    /// stacks that honor the flag keep it out of logs.
    pub fn to_header_value(&self) -> http::HeaderValue {
        let mut value = http::HeaderValue::from_str(&self.0.to_string())
            .expect("a UUID is a valid header value");
        value.set_sensitive(true);
        value
    }
}

/// Equality runs in constant time over the 16 bytes, so a remote caller
/// cannot learn a prefix of a live credential from response timing.
impl PartialEq for Credential {
    fn eq(&self, other: &Self) -> bool {
        let difference = self
            .0
            .as_bytes()
            .iter()
            .zip(other.0.as_bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b));
        difference == 0
    }
}

impl Eq for Credential {}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([REDACTED])")
    }
}

/// A [`CREDENTIAL_HEADER`] value that is not a credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCredential;

impl std::fmt::Display for InvalidCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid credential header")
    }
}

impl std::error::Error for InvalidCredential {}

/// Identity of one successful registration. Unlike the store incarnation it
/// is safe to hand to clients, and every registration gets a new one, so
/// extensions use it to fence state from an earlier registration of the
/// same instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RegistrationEpoch(uuid::Uuid);

impl RegistrationEpoch {
    /// A fresh random epoch.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    /// Rebuild an epoch from its UUID, e.g. one read from a store.
    pub const fn from_uuid(value: uuid::Uuid) -> Self {
        Self(value)
    }

    /// The UUID inside.
    pub const fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }
}

impl Default for RegistrationEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RegistrationEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identity of one hub process, new at each start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HubId(uuid::Uuid);

impl HubId {
    /// A fresh random id.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for HubId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for HubId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Body of `POST /v1/instances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// The registrant's velo identity and address.
    pub peer: PeerInfo,
    /// One payload per extension the registrant opts into, keyed by
    /// extension name. An extension that is not named here still sees the
    /// registration, with no payload.
    #[serde(default)]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

/// Body of a successful `POST /v1/instances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// The registered instance, echoed.
    pub instance_id: InstanceId,
    /// The hub's own velo instance, when the hub runs velo. Look it up with
    /// [`peers_by_instance`] to reach the hub over velo.
    #[serde(default)]
    pub hub_instance_id: Option<InstanceId>,
    /// Authorizes this registration's mutations until the next registration.
    pub credential: Credential,
    /// This registration's epoch.
    pub epoch: RegistrationEpoch,
}

/// Body of a successful peer lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerLookupResponse {
    /// The peer.
    pub peer: PeerInfo,
}

/// Body of a successful `POST /v1/instances/{id}/heartbeat`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// `true`: the hub refreshed the registration's liveness.
    pub acknowledged: bool,
}

/// Body of a successful `POST /v1/instances/{id}/probe`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResponse {
    /// Sequence number the instance echoed.
    pub seq: u64,
    /// Whether the instance reported itself healthy.
    pub ok: bool,
}

/// One registration in `GET /v1/instances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceEntry {
    /// The registrant.
    pub peer: PeerInfo,
    /// The extensions it opted into, sorted.
    pub extensions: Vec<String>,
    /// `true` for the hub's own entry, which never expires.
    pub pinned: bool,
}

/// Body of `GET /v1/instances`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListInstancesResponse {
    /// Every registration the hub holds, including its own.
    pub instances: Vec<InstanceEntry>,
}

/// One extension in `GET /v1/hub`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionDescriptor {
    /// Wire name; its routes are under [`extension_root`].
    pub name: String,
    /// Hub-side extensions it needs.
    pub requires: Vec<String>,
    /// Extensions a registrant must also opt into when it opts into this one.
    pub registration_requires: Vec<String>,
    /// Extension-defined description, `null` if it has none.
    #[serde(default)]
    pub descriptor: serde_json::Value,
}

/// Body of `GET /v1/hub`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubDescriptor {
    /// This hub process.
    pub hub_id: HubId,
    /// The hub's velo instance, when it runs velo.
    #[serde(default)]
    pub velo_instance_id: Option<InstanceId>,
    /// Extensions in start order.
    pub extensions: Vec<ExtensionDescriptor>,
}

/// Velo message the hub sends a registrant on each heartbeat probe.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Sequence number, echoed in the ack.
    pub seq: u64,
}

/// A registrant's answer to a [`HeartbeatRequest`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatAck {
    /// The request's sequence number.
    pub seq: u64,
    /// `true` while the registrant holds a registration with this hub.
    /// `false` (it lost the registration's response, or dropped its client)
    /// counts as a failed probe: after `heartbeat_max_failures` the hub
    /// removes the entry, and the registrant can register again without a
    /// credential.
    pub ok: bool,
}

/// `GET /v1/services`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceListResponse {
    /// Names with at least one instance, sorted.
    pub services: Vec<String>,
}

/// `GET /v1/services/{name}`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceInstancesResponse {
    /// The instances, sorted. Empty for a name nobody serves.
    pub instances: Vec<InstanceId>,
}

/// Body of every non-2xx response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What a client should do about it.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl std::fmt::Display for ErrorBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ErrorBody {}

/// Stable error codes. Clients act on the code, not the HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// 400: the request is malformed or an extension rejected its payload.
    BadRequest,
    /// 401: the credential is missing, stale, or another instance's.
    Unauthorized,
    /// 404: the instance or worker is not registered, or the route does
    /// not exist.
    NotFound,
    /// 405: the route exists, but not for this method.
    MethodNotAllowed,
    /// 409: the request conflicts with the hub's state, for example a
    /// worker id another instance holds, or an extension's rule. Retrying
    /// the same request does not help.
    Conflict,
    /// 409: another mutation of the same instance is in progress, or its
    /// removal is still being delivered. Retry.
    Busy,
    /// 409: the registration failed after the hub had recorded it, so the
    /// hub removed it, including the previous registration. Register again
    /// without a credential.
    RegisterAgain,
    /// 503: the hub is starting, stopping, overloaded, or its store has
    /// lost continuity. Retry later.
    Unavailable,
    /// 502: the hub could not reach the instance over velo.
    BadGateway,
    /// 500: an unexpected failure in the hub.
    Internal,
}

impl ErrorCode {
    /// The HTTP status the hub answers with this code.
    pub fn status(self) -> http::StatusCode {
        use http::StatusCode;
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Conflict | Self::Busy | Self::RegisterAgain => StatusCode::CONFLICT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::BadGateway => StatusCode::BAD_GATEWAY,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo::WorkerAddress;

    fn peer() -> PeerInfo {
        PeerInfo::new(
            InstanceId::new_v4(),
            WorkerAddress::from_encoded(b"addr".to_vec()),
        )
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credential = Credential(uuid::Uuid::new_v4());
        let shown = format!("{credential:?}");
        assert_eq!(shown, "Credential([REDACTED])");
        assert!(!shown.contains(&credential.0.to_string()));
    }

    #[test]
    fn credential_header_round_trips_and_is_sensitive() {
        let credential = Credential(uuid::Uuid::new_v4());
        let header = credential.to_header_value();
        assert!(header.is_sensitive());
        let parsed = Credential::from_header_str(header.to_str().unwrap()).unwrap();
        assert_eq!(parsed, credential);
        assert_eq!(Credential::from_header_str("nope"), Err(InvalidCredential));
    }

    #[test]
    fn credential_equality_compares_every_byte() {
        let a = Credential(uuid::Uuid::from_u128(1));
        let b = Credential(uuid::Uuid::from_u128(1 << 120));
        assert_ne!(a, b);
        assert_eq!(a, a.clone());
    }

    /// A registrant that opts into no extension may omit the field.
    #[test]
    fn register_request_without_extensions_decodes() {
        let peer = peer();
        let json = format!(r#"{{"peer":{}}}"#, serde_json::to_string(&peer).unwrap());
        let request: RegisterRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(request.peer, peer);
        assert!(request.extensions.is_empty());
    }

    #[test]
    fn handler_name_and_header_are_stable() {
        assert_eq!(HEARTBEAT_HANDLER, "velo_hub_heartbeat");
        assert_eq!(CREDENTIAL_HEADER, "x-velo-hub-credential");
    }

    #[test]
    fn heartbeat_messages_round_trip() {
        let request: HeartbeatRequest =
            serde_json::from_str(&serde_json::to_string(&HeartbeatRequest { seq: 99 }).unwrap())
                .unwrap();
        assert_eq!(request.seq, 99);
        let ack: HeartbeatAck = serde_json::from_str(
            &serde_json::to_string(&HeartbeatAck { seq: 7, ok: true }).unwrap(),
        )
        .unwrap();
        assert_eq!((ack.seq, ack.ok), (7, true));
        assert_eq!(HeartbeatRequest::default().seq, 0);
        assert_eq!(HeartbeatAck::default().seq, 0);
        assert!(!HeartbeatAck::default().ok);
    }

    #[test]
    fn error_codes_use_snake_case_and_map_to_statuses() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::RegisterAgain).unwrap(),
            r#""register_again""#
        );
        assert_eq!(ErrorCode::Busy.status(), http::StatusCode::CONFLICT);
        assert_eq!(
            ErrorCode::Unavailable.status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn paths_match_their_helpers() {
        let id = InstanceId::new_v4();
        assert_eq!(
            instance_heartbeat(id),
            paths::INSTANCE_HEARTBEAT.replace("{instance_id}", &id.to_string())
        );
        assert_eq!(extension_root("probe"), "/v1/ext/probe");
    }
}
