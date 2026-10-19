// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Bundles, and the validation of the bundle and extension graph.
//!
//! The hub checks the whole graph before it binds a socket or builds its
//! velo instance, so a misassembled hub fails with no side effects.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::extension::{DynExtension, Erased, Extension};

/// A set of extensions that ships together, such as everything kvbm adds
/// to a hub.
///
/// A bundle declares the bundles it needs and never constructs them: the
/// program adds each bundle once, so two bundles that need a third share
/// one instance of it.
pub trait Bundle: Send + 'static {
    /// Name; also the key of its section in [`HubConfig::bundles`](crate::HubConfig::bundles).
    const NAME: &'static str;

    /// Bundles this one needs.
    fn requires() -> &'static [&'static str] {
        &[]
    }

    /// Add this bundle's extensions. Runs at [`HubBuilder::serve`](crate::HubBuilder::serve),
    /// before any I/O, after every bundle it requires installed.
    fn install(self, installer: &mut BundleInstaller<'_>) -> anyhow::Result<()>;
}

/// Handed to [`Bundle::install`].
pub struct BundleInstaller<'a> {
    bundle: &'static str,
    config: Option<&'a serde_json::Value>,
    extensions: &'a mut Vec<PendingExtension>,
}

impl BundleInstaller<'_> {
    /// The installing bundle's name.
    pub fn name(&self) -> &'static str {
        self.bundle
    }

    /// Add an extension.
    pub fn extension<E: Extension>(&mut self, extension: impl Into<Arc<E>>) -> &mut Self {
        self.extensions
            .push(PendingExtension::new(extension.into(), self.bundle));
        self
    }

    /// This bundle's config section, if the hub config has one.
    pub fn config<C: DeserializeOwned>(&self) -> anyhow::Result<Option<C>> {
        self.config
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|error| {
                    anyhow::anyhow!("config section [bundles.{}]: {error}", self.bundle)
                })
            })
            .transpose()
    }
}

/// An extension added to the builder, with who added it.
pub(crate) struct PendingExtension {
    pub(crate) extension: Arc<dyn DynExtension>,
    pub(crate) owner: &'static str,
}

impl PendingExtension {
    pub(crate) fn new<E: Extension>(extension: Arc<E>, owner: &'static str) -> Self {
        Self {
            extension: Arc::new(Erased(extension)),
            owner,
        }
    }
}

type Install = Box<dyn FnOnce(&mut BundleInstaller<'_>) -> anyhow::Result<()> + Send>;

/// A bundle added to the builder, not yet installed.
pub(crate) struct PendingBundle {
    name: &'static str,
    requires: &'static [&'static str],
    install: Install,
}

impl PendingBundle {
    pub(crate) fn new<B: Bundle>(bundle: B) -> Self {
        Self {
            name: B::NAME,
            requires: B::requires(),
            install: Box::new(move |installer| bundle.install(installer)),
        }
    }
}

/// The owner name of extensions added straight to the builder.
pub(crate) const HUB_OWNER: &str = "(hub)";

/// Why the hub refused to start before doing any I/O.
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// An extension name is not `[a-z0-9_-]{1,64}`.
    #[error("extension name {0:?} must be 1-64 characters of [a-z0-9_-]")]
    InvalidName(&'static str),
    /// Two bundles share a name.
    #[error("bundle {0:?} is added twice")]
    DuplicateBundle(&'static str),
    /// Two extensions share a name.
    #[error("extension {name:?} is added by both {first} and {second}")]
    DuplicateExtension {
        /// The name.
        name: &'static str,
        /// Who added it first.
        first: &'static str,
        /// Who added it again.
        second: &'static str,
    },
    /// A bundle needs a bundle that was not added.
    #[error("bundle {bundle:?} requires bundle {requires:?}, which was not added")]
    MissingBundle {
        /// The bundle.
        bundle: &'static str,
        /// What it needs.
        requires: &'static str,
    },
    /// An extension needs an extension that was not added.
    #[error("extension {extension:?} requires extension {requires:?}, which was not added")]
    MissingExtension {
        /// The extension.
        extension: &'static str,
        /// What it needs.
        requires: &'static str,
    },
    /// An extension's `registration_requires` names an unknown extension.
    #[error(
        "extension {extension:?} has registrants also opt into {requires:?}, which was not added"
    )]
    MissingRegistrationRequirement {
        /// The extension.
        extension: &'static str,
        /// What it names.
        requires: &'static str,
    },
    /// Bundles require each other in a cycle.
    #[error("bundles require each other in a cycle: {0:?}")]
    BundleCycle(Vec<&'static str>),
    /// Extensions require each other in a cycle.
    #[error("extensions require each other in a cycle: {0:?}")]
    ExtensionCycle(Vec<&'static str>),
    /// A config section names no bundle that was added.
    #[error("config section [bundles.{0}] belongs to no bundle that was added")]
    UnclaimedConfig(String),
    /// A bundle's install failed.
    #[error("bundle {bundle:?} failed to install: {source}")]
    Install {
        /// The bundle.
        bundle: &'static str,
        /// Why.
        source: anyhow::Error,
    },
}

fn valid_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
}

/// Kahn's algorithm with insertion order as the tie-break, so the result
/// is deterministic. Returns the order, or the nodes left on a cycle (and
/// anything that depends on one).
fn topological(
    names: &[&'static str],
    edges: &[Vec<usize>],
) -> Result<Vec<usize>, Vec<&'static str>> {
    let mut indegree: Vec<usize> = edges.iter().map(Vec::len).collect();
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); names.len()];
    for (node, requires) in edges.iter().enumerate() {
        for &required in requires {
            dependents[required].push(node);
        }
    }
    let mut order = Vec::with_capacity(names.len());
    let mut done = vec![false; names.len()];
    while order.len() < names.len() {
        let Some(next) = (0..names.len()).find(|&node| !done[node] && indegree[node] == 0) else {
            let mut cycle: Vec<&'static str> = (0..names.len())
                .filter(|&node| !done[node])
                .map(|node| names[node])
                .collect();
            cycle.sort_unstable();
            return Err(cycle);
        };
        done[next] = true;
        order.push(next);
        for &dependent in &dependents[next] {
            indegree[dependent] -= 1;
        }
    }
    Ok(order)
}

/// Install every bundle and return all extensions in start order.
pub(crate) fn plan(
    bundles: Vec<PendingBundle>,
    direct: Vec<PendingExtension>,
    config: &BTreeMap<String, serde_json::Value>,
) -> Result<Vec<Arc<dyn DynExtension>>, GraphError> {
    // Bundles: unique, present, acyclic.
    let mut bundle_index: HashMap<&'static str, usize> = HashMap::new();
    for (index, bundle) in bundles.iter().enumerate() {
        if bundle_index.insert(bundle.name, index).is_some() {
            return Err(GraphError::DuplicateBundle(bundle.name));
        }
    }
    let mut bundle_edges = Vec::with_capacity(bundles.len());
    for bundle in &bundles {
        let mut edges = Vec::new();
        for &required in bundle.requires {
            let index = *bundle_index
                .get(required)
                .ok_or(GraphError::MissingBundle {
                    bundle: bundle.name,
                    requires: required,
                })?;
            edges.push(index);
        }
        bundle_edges.push(edges);
    }
    let bundle_names: Vec<&'static str> = bundles.iter().map(|bundle| bundle.name).collect();
    let bundle_order =
        topological(&bundle_names, &bundle_edges).map_err(GraphError::BundleCycle)?;

    // Every config section belongs to an added bundle.
    if let Some(section) = config
        .keys()
        .find(|key| !bundle_index.contains_key(key.as_str()))
    {
        return Err(GraphError::UnclaimedConfig(section.clone()));
    }

    // Install in dependency order, then the extensions added directly.
    let mut slots: Vec<Option<PendingBundle>> = bundles.into_iter().map(Some).collect();
    let mut pending = Vec::new();
    for index in bundle_order {
        let bundle = slots[index].take().expect("each bundle installs once");
        let mut installer = BundleInstaller {
            bundle: bundle.name,
            config: config.get(bundle.name),
            extensions: &mut pending,
        };
        (bundle.install)(&mut installer).map_err(|source| GraphError::Install {
            bundle: bundle.name,
            source,
        })?;
    }
    pending.extend(direct);

    // Extensions: valid, unique, present, acyclic.
    let mut extension_index: HashMap<&'static str, usize> = HashMap::new();
    for (index, entry) in pending.iter().enumerate() {
        let name = entry.extension.name();
        if !valid_name(name) {
            return Err(GraphError::InvalidName(name));
        }
        if let Some(&first) = extension_index.get(name) {
            return Err(GraphError::DuplicateExtension {
                name,
                first: pending[first].owner,
                second: entry.owner,
            });
        }
        extension_index.insert(name, index);
    }
    let mut extension_edges = Vec::with_capacity(pending.len());
    for entry in &pending {
        let extension = &entry.extension;
        let mut edges = Vec::new();
        let mut seen = HashSet::new();
        for &required in extension.requires() {
            let index = *extension_index
                .get(required)
                .ok_or(GraphError::MissingExtension {
                    extension: extension.name(),
                    requires: required,
                })?;
            if seen.insert(index) {
                edges.push(index);
            }
        }
        for &required in extension.registration_requires() {
            if !extension_index.contains_key(required) {
                return Err(GraphError::MissingRegistrationRequirement {
                    extension: extension.name(),
                    requires: required,
                });
            }
        }
        extension_edges.push(edges);
    }
    let extension_names: Vec<&'static str> =
        pending.iter().map(|entry| entry.extension.name()).collect();
    let order =
        topological(&extension_names, &extension_edges).map_err(GraphError::ExtensionCycle)?;
    let mut slots: Vec<Option<Arc<dyn DynExtension>>> = pending
        .into_iter()
        .map(|entry| Some(entry.extension))
        .collect();
    Ok(order
        .into_iter()
        .map(|index| slots[index].take().expect("each extension is ordered once"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_url_segments() {
        for name in ["probe", "kv-index", "p2p_2", "a"] {
            assert!(valid_name(name), "{name}");
        }
        for name in ["", "Probe", "a/b", "..", "a b", &"x".repeat(65)] {
            assert!(!valid_name(name), "{name}");
        }
    }

    #[test]
    fn topological_order_is_stable_and_reports_cycles() {
        // c requires a; b requires c. Insertion order breaks ties.
        let names = ["a", "b", "c", "d"];
        let edges = vec![vec![], vec![2], vec![0], vec![]];
        assert_eq!(topological(&names, &edges).unwrap(), vec![0, 2, 1, 3]);
        let cyclic = vec![vec![1], vec![0], vec![], vec![0]];
        assert_eq!(
            topological(&names, &cyclic).unwrap_err(),
            vec!["a", "b", "d"]
        );
    }
}
