// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Service discovery through the hub, and the services a client serves.
//!
//! A client serves each name through one [`HubServiceRegistration`]. The
//! names live in the client, not in the guard's registration: every
//! successful `register` puts them again, so a fresh registration (which
//! the hub starts with no services) and a hub that restarted get them
//! back, and a guard never acts on another guard's entry.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use futures::Stream;
use futures::future::BoxFuture;
use reqwest::Method;
use velo::InstanceId;
use velo::discovery::{ServiceDiscovery, ServiceEvent, ServiceRegistrationGuard};

use super::sse::parse_events;
use super::{ClientError, Endpoints, HubClient, Route, Shared, decode, owns_nothing};
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
    /// second). The entry follows the client's registration: every
    /// successful `register` puts it again. Dropping the guard removes it
    /// in the background, best effort.
    pub async fn register_service(
        &self,
        name: &str,
    ) -> Result<HubServiceRegistration, ClientError> {
        check_name(name)?;
        let shared = &self.inner.shared;
        if !shared.services.lock().insert(name.to_string()) {
            return Err(ClientError::AlreadyServing(name.to_string()));
        }
        // From here the name is the guard's; it gives it back on drop.
        let guard = HubServiceRegistration {
            endpoints: Arc::clone(&self.inner.endpoints),
            registration: Arc::downgrade(shared),
            name: name.to_string(),
            removed: false,
        };
        put(&self.inner.endpoints, shared, name).await?;
        Ok(guard)
    }

    /// Put every name this client serves, after a registration.
    pub(super) async fn put_services(&self) -> Result<(), ClientError> {
        let names: Vec<String> = self.inner.shared.services.lock().iter().cloned().collect();
        for name in names {
            put(&self.inner.endpoints, &self.inner.shared, &name).await?;
        }
        Ok(())
    }
}

async fn put(endpoints: &Endpoints, shared: &Shared, name: &str) -> Result<(), ClientError> {
    service_call(endpoints, shared, Method::PUT, name).await
}

async fn service_call(
    endpoints: &Endpoints,
    shared: &Shared,
    method: Method,
    name: &str,
) -> Result<(), ClientError> {
    let (instance_id, _) = shared.credential().ok_or(ClientError::NotRegistered)?;
    let url = Endpoints::join(
        &endpoints.control,
        &protocol::service_instance(name, instance_id),
    )?;
    shared
        .credentialed::<(), ()>(endpoints, method, url, None, Route::Core)
        .await
}

/// The client serving one service name. Holds the client weakly: once the
/// client is gone, its registration's end removes the entry.
#[derive(Debug)]
pub struct HubServiceRegistration {
    endpoints: Arc<Endpoints>,
    registration: Weak<Shared>,
    name: String,
    removed: bool,
}

impl HubServiceRegistration {
    /// The service's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stop serving: give the name back, then remove the entry. Without a
    /// registration there is no entry to remove.
    async fn remove(&mut self) -> Result<(), ClientError> {
        if self.removed {
            return Ok(());
        }
        let Some(shared) = self.registration.upgrade() else {
            self.removed = true;
            return Ok(());
        };
        shared.services.lock().remove(&self.name);
        self.removed = true;
        match service_call(&self.endpoints, &shared, Method::DELETE, &self.name).await {
            Err(ClientError::NotRegistered) => Ok(()),
            Err(error) if owns_nothing(&error) => Ok(()),
            other => other,
        }
    }
}

impl ServiceRegistrationGuard for HubServiceRegistration {
    fn unregister(&mut self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move { Ok(self.remove().await?) })
    }
}

impl Drop for HubServiceRegistration {
    fn drop(&mut self) {
        if self.removed {
            return;
        }
        let Some(shared) = self.registration.upgrade() else {
            return;
        };
        shared.services.lock().remove(&self.name);
        if shared.credential().is_none() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let endpoints = Arc::clone(&self.endpoints);
        let name = std::mem::take(&mut self.name);
        runtime.spawn(async move {
            let _ = service_call(&endpoints, &shared, Method::DELETE, &name).await;
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
            check_name(&name)?;
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
            check_name(&name)?;
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
                Box::pin(parse_events(response.bytes_stream()));
            Ok(stream)
        })
    }
}
