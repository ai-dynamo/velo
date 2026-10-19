// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Service discovery through the hub, and the services a client serves.
//!
//! The client keeps two sets: `serving`, the names its guards hold, and
//! `confirmed`, what the hub holds for its registration. Every registration
//! carries `serving` and the hub takes it as the owner's whole set, so the
//! two agree after any registration. Between registrations, guards change
//! only `serving`, and `sync_services` is the one writer of the hub's set:
//! it holds the client's operation lock (so it never runs beside a
//! registration) and sends one request at a time until the sets agree. So
//! two requests from one client never race, a failed one stays in the diff
//! for the next pass, and a guard never acts on another guard's entry.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::Stream;
use futures::future::BoxFuture;
use reqwest::Method;
use velo::InstanceId;
use velo::discovery::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};

use super::sse::parse_events;
use super::{ClientError, Endpoints, HubClient, Inner, Listener, Route};
use crate::protocol;

fn check_name(name: &str) -> Result<(), ClientError> {
    if protocol::is_valid_service_name(name) {
        Ok(())
    } else {
        Err(ClientError::InvalidServiceName(name.to_string()))
    }
}

impl HubClient {
    /// Service discovery through this hub, for code that takes a
    /// [`ServiceDiscovery`]. It holds no registration.
    pub fn services(&self) -> HubServices {
        HubServices {
            endpoints: Arc::clone(&self.inner.endpoints),
        }
    }

    /// Serve the service `name` with this client's instance. The client
    /// holds one guard per name ([`ClientError::AlreadyServing`] for a
    /// second). The name goes to the hub now if the client is registered,
    /// and with every registration after. On `Err` there is no guard and
    /// the name is not served; call again. The error can belong to another
    /// name whose change was still waiting. Dropping the guard stops
    /// serving it, in the background.
    pub async fn register_service(
        &self,
        name: &str,
    ) -> Result<HubServiceRegistration, ClientError> {
        check_name(name)?;
        if !self.inner.shared.serving.lock().insert(name.to_string()) {
            return Err(ClientError::AlreadyServing(name.to_string()));
        }
        let guard = HubServiceRegistration {
            client: Arc::downgrade(&self.inner),
            name: name.to_string(),
            serving: true,
        };
        self.sync_services().await?;
        Ok(guard)
    }

    /// Bring the hub's services for this registration to the names the
    /// client serves, one request at a time. Without a registration there
    /// is nothing to do: the next registration carries the names. Stops at
    /// the first failure, which a later pass retries.
    pub async fn sync_services(&self) -> Result<(), ClientError> {
        let _op = self.inner.op.lock().await;
        let shared = &self.inner.shared;
        let endpoints = &self.inner.endpoints;
        loop {
            let Some((instance_id, _)) = shared.credential() else {
                return Ok(());
            };
            // Unsettled names first (their state on the hub is unknown),
            // sent as `serving` dictates; then the plain differences. Both
            // requests are idempotent on the hub.
            let next = {
                let serving = shared.serving.lock();
                let confirmed = shared.confirmed.lock();
                let unsettled = shared.unsettled.lock();
                unsettled
                    .iter()
                    .next()
                    .map(|name| {
                        let method = if serving.contains(name) {
                            Method::PUT
                        } else {
                            Method::DELETE
                        };
                        (method, name.clone())
                    })
                    .or_else(|| {
                        serving
                            .difference(&confirmed)
                            .next()
                            .map(|name| (Method::PUT, name.clone()))
                    })
                    .or_else(|| {
                        confirmed
                            .difference(&serving)
                            .next()
                            .map(|name| (Method::DELETE, name.clone()))
                    })
            };
            let Some((method, name)) = next else {
                return Ok(());
            };
            let path = protocol::service_instance(&name, instance_id);
            let put = method == Method::PUT;
            // Unsettled until the hub answers: a dropped future or an answer
            // that is not the hub's leaves it so.
            shared.unsettled.lock().insert(name.clone());
            // `credentialed` drops a rejected registration, which clears
            // these sets; the loop then ends for want of a credential.
            let answer = shared
                .credentialed::<(), ()>(
                    endpoints,
                    method,
                    Listener::Control,
                    &path,
                    None,
                    Route::Core,
                )
                .await;
            match answer {
                Ok(()) => {
                    shared.unsettled.lock().remove(&name);
                    let mut confirmed = shared.confirmed.lock();
                    if put {
                        confirmed.insert(name);
                    } else {
                        confirmed.remove(&name);
                    }
                }
                Err(error) => {
                    // The hub answered: it did not apply the request.
                    if matches!(error, ClientError::Hub { .. }) {
                        shared.unsettled.lock().remove(&name);
                    }
                    return Err(error);
                }
            }
        }
    }
}

/// The client serving one service name. Holds the client weakly: once every
/// `HubClient` is gone, the registration ends, and the hub drops the entry
/// with it.
#[derive(Debug)]
pub struct HubServiceRegistration {
    client: Weak<Inner>,
    name: String,
    /// Whether the name is still in the client's set.
    serving: bool,
}

impl HubServiceRegistration {
    /// The service's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stop serving the name, then bring the hub's set to the client's.
    /// Retrying after a failure sends the remove again.
    async fn remove(&mut self) -> Result<(), ClientError> {
        let Some(inner) = self.client.upgrade() else {
            self.serving = false;
            return Ok(());
        };
        if self.serving {
            inner.shared.serving.lock().remove(&self.name);
            self.serving = false;
        }
        HubClient { inner }.sync_services().await
    }
}

impl ServiceRegistrationGuard for HubServiceRegistration {
    fn unregister(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move { Ok(self.remove().await?) })
    }
}

impl Drop for HubServiceRegistration {
    fn drop(&mut self) {
        if !self.serving {
            return;
        }
        let Some(inner) = self.client.upgrade() else {
            return;
        };
        inner.shared.serving.lock().remove(&self.name);
        if inner.shared.credential().is_none() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let client = HubClient { inner };
        runtime.spawn(async move {
            let _ = client.sync_services().await;
        });
    }
}

/// Service discovery through a hub. Cheap to clone. Get one from
/// [`HubClient::services`].
#[derive(Debug, Clone)]
pub struct HubServices {
    endpoints: Arc<Endpoints>,
}

impl ServiceDiscovery for HubServices {
    fn list_services(&self) -> BoxFuture<'_, anyhow::Result<Vec<String>>> {
        Box::pin(async move {
            let list: protocol::ServiceListResponse = self
                .endpoints
                .call::<(), _>(
                    Method::GET,
                    Listener::Public,
                    protocol::paths::SERVICES,
                    None,
                    None,
                )
                .await?;
            Ok(list.services)
        })
    }

    fn get_instances(&self, service_name: &str) -> BoxFuture<'_, anyhow::Result<Vec<InstanceId>>> {
        let name = service_name.to_string();
        Box::pin(async move {
            check_name(&name)?;
            let found: protocol::ServiceInstancesResponse = self
                .endpoints
                .call::<(), _>(
                    Method::GET,
                    Listener::Public,
                    &protocol::service(&name),
                    None,
                    None,
                )
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
            check_name(&name)?;
            let endpoints = &self.endpoints;
            let response = endpoints
                .send::<()>(
                    &endpoints.watch,
                    Method::GET,
                    Listener::Public,
                    &protocol::service_watch(&name),
                    None,
                    None,
                )
                .await?;
            let stream: Pin<Box<dyn Stream<Item = ServiceEvent> + Send>> =
                Box::pin(parse_events(response.bytes_stream()));
            Ok(stream)
        })
    }
}
