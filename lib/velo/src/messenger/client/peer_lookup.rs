// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The id a peer-table miss asks discovery for.
//!
//! The lookup itself is `VeloBackend::resolve_peer`: each miss runs its own,
//! inside the caller's future and the caller's bound. There is no shared
//! lookup and no state here.

use std::fmt;

use velo_ext::{InstanceId, WorkerId};

use crate::transports::VeloBackend;

/// The peer a miss names: a reply knows the worker it answers, an `.instance`
/// send knows the instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerKey {
    Worker(WorkerId),
    Instance(InstanceId),
}

impl PeerKey {
    /// Whether `instance` is a peer this key asked for.
    pub(crate) fn names(self, instance: InstanceId) -> bool {
        match self {
            Self::Worker(worker) => instance.worker_id() == worker,
            Self::Instance(asked) => instance == asked,
        }
    }

    /// The instance the table holds a route to for `self`, if any.
    pub(crate) fn registered_in(self, backend: &VeloBackend) -> Option<InstanceId> {
        let instance = match self {
            Self::Worker(worker) => backend.try_translate_worker_id(worker).ok()?,
            Self::Instance(instance) => instance,
        };
        backend.is_registered(instance).then_some(instance)
    }
}

impl fmt::Display for PeerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Worker(worker) => write!(f, "worker {worker}"),
            Self::Instance(instance) => write!(f, "instance {instance}"),
        }
    }
}

/// A lookup that did not answer before its caller's deadline.
#[derive(Debug, thiserror::Error)]
#[error("Discovery for {0} ran out of time")]
pub(crate) struct LookupTimedOut(pub(crate) PeerKey);
