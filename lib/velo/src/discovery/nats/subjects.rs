// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pure functions for constructing NATS discovery subject strings.
//!
//! Subjects follow the pattern:
//! - Per-instance: `{cluster_id}.velo.discover.instance.{base58(instance_id_bytes)}`
//! - Per-worker:   `{cluster_id}.velo.discover.worker.{worker_id_u64}`
//!
//! The `discover.` namespace separates discovery subjects from data and health
//! subjects defined in `velo-transports/src/nats/subjects.rs`.

use velo_ext::{InstanceId, WorkerId};

/// Construct the discovery subject for a given instance.
///
/// Format: `{cluster_id}.velo.discover.instance.{base58(instance_id.as_bytes())}`
///
/// The base58 segment is typically 21–22 characters for a 16-byte UUID.
pub fn discovery_instance_subject(cluster_id: &str, instance_id: InstanceId) -> String {
    let b58 = bs58::encode(instance_id.as_bytes()).into_string();
    format!("{cluster_id}.velo.discover.instance.{b58}")
}

/// Construct the discovery subject for a given worker.
///
/// Format: `{cluster_id}.velo.discover.worker.{worker_id_u64}`
///
/// Worker subjects use the raw u64 decimal string since WorkerId is already compact.
pub fn discovery_worker_subject(cluster_id: &str, worker_id: WorkerId) -> String {
    format!("{cluster_id}.velo.discover.worker.{}", worker_id.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_instance_subject_deterministic() {
        let instance_id = InstanceId::new_v4();
        let s1 = discovery_instance_subject("my-cluster", instance_id);
        let s2 = discovery_instance_subject("my-cluster", instance_id);
        assert_eq!(
            s1, s2,
            "Same InstanceId must produce same discovery subject"
        );
    }

    #[test]
    fn test_discovery_worker_subject_deterministic() {
        let instance_id = InstanceId::new_v4();
        let worker_id = instance_id.worker_id();
        let s1 = discovery_worker_subject("prod", worker_id);
        let s2 = discovery_worker_subject("prod", worker_id);
        assert_eq!(s1, s2, "Same WorkerId must produce same discovery subject");
    }

    #[test]
    fn test_different_instances_produce_different_subjects() {
        let id1 = InstanceId::new_v4();
        let id2 = InstanceId::new_v4();
        let s1 = discovery_instance_subject("cluster", id1);
        let s2 = discovery_instance_subject("cluster", id2);
        assert_ne!(
            s1, s2,
            "Different InstanceIds must produce different subjects"
        );
    }

    #[test]
    fn test_discovery_instance_subject_format() {
        let instance_id = InstanceId::new_v4();
        let subject = discovery_instance_subject("test-cluster", instance_id);

        assert!(
            subject.starts_with("test-cluster.velo.discover.instance."),
            "Subject must start with 'test-cluster.velo.discover.instance.', got: {subject}"
        );

        let prefix = "test-cluster.velo.discover.instance.";
        let b58_part = &subject[prefix.len()..];
        assert!(
            b58_part.chars().all(|c| c.is_ascii_alphanumeric()),
            "Base58 segment must be all alphanumeric, got: {b58_part}"
        );
        assert!(
            b58_part.len() <= 22 && !b58_part.is_empty(),
            "Base58 of 16 bytes must be 1-22 chars, got len={} for '{b58_part}'",
            b58_part.len()
        );
    }

    #[test]
    fn test_discovery_worker_subject_format() {
        let instance_id = InstanceId::new_v4();
        let worker_id = instance_id.worker_id();
        let subject = discovery_worker_subject("test-cluster", worker_id);

        assert!(
            subject.starts_with("test-cluster.velo.discover.worker."),
            "Subject must start with 'test-cluster.velo.discover.worker.', got: {subject}"
        );

        let prefix = "test-cluster.velo.discover.worker.";
        let id_part = &subject[prefix.len()..];
        let parsed: u64 = id_part
            .parse()
            .expect("Worker ID segment must be a valid u64");
        assert_eq!(parsed, worker_id.as_u64());
    }

    #[test]
    fn test_discovery_subjects_no_collision_with_data_subjects() {
        let instance_id = InstanceId::new_v4();
        let discovery = discovery_instance_subject("c", instance_id);
        // Data subjects use {cluster}.velo.{base58} (no "discover." segment)
        // Discovery uses {cluster}.velo.discover.instance.{base58}
        assert!(
            discovery.contains(".discover."),
            "Discovery subjects must contain '.discover.' namespace"
        );
    }

    #[test]
    fn test_cluster_id_preserved_in_discovery_subjects() {
        let id = InstanceId::new_v4();
        let subject = discovery_instance_subject("my.cluster.name", id);
        assert!(
            subject.starts_with("my.cluster.name.velo.discover.instance."),
            "Cluster ID must be preserved verbatim"
        );
    }
}
