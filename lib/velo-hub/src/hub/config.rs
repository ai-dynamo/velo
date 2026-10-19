// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Hub configuration. The library reads no files and parses no flags; the
//! binary builds a [`HubConfig`] however it likes (TOML, JSON, env) and
//! hands it to [`Hub::builder`](crate::Hub::builder).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::protocol::{DEFAULT_CONTROL_PORT, DEFAULT_PUBLIC_PORT};
use crate::store::{DEFAULT_PRUNE_INTERVAL, DEFAULT_TTL};

/// Everything a hub reads at startup.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HubConfig {
    /// Listeners, liveness, and deadlines.
    #[serde(default)]
    pub server: ServerConfig,
    /// One section per bundle, keyed by bundle name. A bundle reads its own
    /// with [`BundleInstaller::config`](crate::BundleInstaller::config). A
    /// section no bundle claims fails startup, so a typo cannot silently
    /// drop configuration.
    #[serde(default)]
    pub bundles: BTreeMap<String, serde_json::Value>,
}

/// Listener, liveness, and deadline settings. Durations are milliseconds
/// on the wire (`ttl_ms`, ...).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Address both listeners bind (default `0.0.0.0`).
    pub bind_addr: IpAddr,
    /// Public listener port (default 1337; 0 picks a free port).
    pub public_port: u16,
    /// Control listener port (default 8337; 0 picks a free port).
    pub control_port: u16,
    /// TTL of the default in-memory store (ignored with an injected store).
    #[serde(rename = "ttl_ms", with = "millis")]
    pub ttl: Duration,
    /// Reaper interval of the default in-memory store.
    #[serde(rename = "prune_interval_ms", with = "millis")]
    pub prune_interval: Duration,
    /// Interval between the hub's velo heartbeat probes. Each probe also
    /// times out after one interval.
    #[serde(rename = "heartbeat_interval_ms", with = "millis")]
    pub heartbeat_interval: Duration,
    /// Consecutive failed probes before the hub removes a registration.
    pub heartbeat_max_failures: u32,
    /// How long a registration or unregistration waits for the hub-wide
    /// registration lock before answering 503.
    #[serde(rename = "lock_timeout_ms", with = "millis")]
    pub lock_timeout: Duration,
    /// Deadline of each step that runs under the registration lock: each
    /// extension `prepare` and `on_registered`, and each store write,
    /// read-back, and remove; also each store call of the heartbeat. A
    /// `prepare` or write that expires aborts the registration like any
    /// failure; an `on_registered` that expires is dropped and logged. So a
    /// hung extension or backend cannot hold the lock forever.
    #[serde(rename = "step_timeout_ms", with = "millis")]
    pub step_timeout: Duration,
    /// Shutdown budget, applied to each wait in turn: the registration
    /// lock; each extension `shutdown`; each hub task to stop; the HTTP requests in flight to
    /// finish, then, after they are ended with 503, their connections to
    /// close; and the drain of the hub's velo instance.
    #[serde(rename = "shutdown_timeout_ms", with = "millis")]
    pub shutdown_timeout: Duration,
}

/// Default heartbeat probe interval.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// Default consecutive probe failures before removal.
pub const DEFAULT_HEARTBEAT_MAX_FAILURES: u32 = 3;
/// Default wait for the registration lock.
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
/// Default deadline of one registration step.
pub const DEFAULT_STEP_TIMEOUT: Duration = Duration::from_secs(5);
/// Default velo drain budget at shutdown.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            public_port: DEFAULT_PUBLIC_PORT,
            control_port: DEFAULT_CONTROL_PORT,
            ttl: DEFAULT_TTL,
            prune_interval: DEFAULT_PRUNE_INTERVAL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_max_failures: DEFAULT_HEARTBEAT_MAX_FAILURES,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
            step_timeout: DEFAULT_STEP_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

impl ServerConfig {
    /// Defaults with both listeners on `127.0.0.1`, port 0. For tests.
    pub fn localhost() -> Self {
        Self {
            bind_addr: IpAddr::V4(Ipv4Addr::LOCALHOST),
            public_port: 0,
            control_port: 0,
            ..Self::default()
        }
    }
}

impl ServerConfig {
    /// Refuse values that would make a hub task panic or every step expire.
    pub fn validate(&self) -> anyhow::Result<()> {
        for (field, value) in [
            ("heartbeat_interval_ms", self.heartbeat_interval),
            ("prune_interval_ms", self.prune_interval),
            ("lock_timeout_ms", self.lock_timeout),
            ("step_timeout_ms", self.step_timeout),
            ("shutdown_timeout_ms", self.shutdown_timeout),
        ] {
            if value.is_zero() {
                anyhow::bail!("server.{field} must be more than 0");
            }
        }
        Ok(())
    }
}

mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        value: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(u64::try_from(value.as_millis()).unwrap_or(u64::MAX))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_reads_millisecond_fields_and_defaults_the_rest() {
        let config: HubConfig = serde_json::from_value(serde_json::json!({
            "server": { "control_port": 9000, "ttl_ms": 1500 },
            "bundles": { "toy": { "depth": 2 } }
        }))
        .unwrap();
        assert_eq!(config.server.control_port, 9000);
        assert_eq!(config.server.ttl, Duration::from_millis(1500));
        assert_eq!(config.server.public_port, DEFAULT_PUBLIC_PORT);
        assert_eq!(config.bundles["toy"]["depth"], 2);
        let empty: HubConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(
            empty.server.heartbeat_max_failures,
            DEFAULT_HEARTBEAT_MAX_FAILURES
        );
    }
}
