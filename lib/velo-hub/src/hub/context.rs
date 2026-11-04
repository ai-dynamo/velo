// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What an extension is handed at attach and start.

use std::any::Any;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::anyhow;
use tokio_util::sync::CancellationToken;
use velo::Velo;

use super::authority::Authority;
use super::extension::Extension;
use crate::protocol::HubId;
use crate::store::HubStore;

/// The hub services every extension context shares.
pub(crate) struct Services {
    pub(crate) hub_id: HubId,
    pub(crate) velo: Option<Velo>,
    pub(crate) store: Arc<dyn HubStore>,
    pub(crate) authority: Authority,
    /// Cancelled first at shutdown, and on a failed startup.
    pub(crate) cancel: CancellationToken,
}

pub(crate) type Attached = HashMap<&'static str, Arc<dyn Any + Send + Sync>>;

/// Handed to [`Extension::attach`].
pub struct AttachContext<'a> {
    pub(crate) name: &'static str,
    pub(crate) requires: &'static [&'static str],
    pub(crate) attached: &'a Attached,
    pub(crate) services: &'a Services,
}

impl AttachContext<'_> {
    /// This hub process.
    pub fn hub_id(&self) -> HubId {
        self.services.hub_id
    }

    /// The hub's velo instance, if it runs one. A handler registered on it
    /// must capture [`Velo::downgrade`], not a clone of this.
    pub fn velo(&self) -> Option<&Velo> {
        self.services.velo.as_ref()
    }

    /// The hub's store.
    pub fn store(&self) -> &Arc<dyn HubStore> {
        &self.services.store
    }

    /// The hub's credential authority.
    pub fn authority(&self) -> &Authority {
        &self.services.authority
    }

    /// Cancelled when the hub stops, before extensions shut down. Hang
    /// background work off it.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.services.cancel
    }

    /// An extension this one declared in [`Extension::requires`]. It is
    /// attached already, because `requires` drives the attach order. Fails
    /// for any undeclared extension, so wiring cannot work by accident.
    pub fn extension<T: Extension>(&self) -> anyhow::Result<Arc<T>> {
        if !self.requires.contains(&T::NAME) {
            return Err(anyhow!(
                "extension {:?} looked up {:?} without declaring it in requires()",
                self.name,
                T::NAME
            ));
        }
        let any = self
            .attached
            .get(T::NAME)
            .ok_or_else(|| anyhow!("extension {:?} is not attached", T::NAME))?;
        Arc::clone(any)
            .downcast::<T>()
            .map_err(|_| anyhow!("extension {:?} has a different type", T::NAME))
    }
}

/// Handed to [`Extension::start`]: the attach context plus the bound
/// listener addresses.
pub struct StartContext<'a> {
    pub(crate) attach: AttachContext<'a>,
    pub(crate) public_addr: SocketAddr,
    pub(crate) control_addr: SocketAddr,
}

impl StartContext<'_> {
    /// The public listener's bound address.
    pub fn public_addr(&self) -> SocketAddr {
        self.public_addr
    }

    /// The control listener's bound address.
    pub fn control_addr(&self) -> SocketAddr {
        self.control_addr
    }
}

impl<'a> Deref for StartContext<'a> {
    type Target = AttachContext<'a>;

    fn deref(&self) -> &Self::Target {
        &self.attach
    }
}
