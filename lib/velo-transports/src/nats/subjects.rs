// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Pure functions for constructing NATS subject strings.
//!
//! Subjects follow the pattern:
//! - Inbound data: `{cluster_id}.velo.{base58(instance_id_bytes)}`
//! - Health check:  `{cluster_id}.velo.{base58(instance_id_bytes)}.health`
//!
//! Base58 encoding (bitcoin alphabet) is used for the identity segment because
//! it produces alphanumeric-only output with no dots or hyphens, making it safe
//! as a NATS subject token without further escaping.

use velo_common::InstanceId;

/// Construct the inbound data subject for a given instance.
///
/// Format: `{cluster_id}.velo.{base58(instance_id.as_bytes())}`
///
/// The base58 segment is typically 21–22 characters for a 16-byte UUID.
pub fn inbound_subject(cluster_id: &str, instance_id: InstanceId) -> String {
    let b58 = bs58::encode(instance_id.as_bytes()).into_string();
    format!("{cluster_id}.velo.{b58}")
}

/// Construct the health request-reply subject for a given instance.
///
/// Format: `{cluster_id}.velo.{base58(instance_id.as_bytes())}.health`
pub fn health_subject(cluster_id: &str, instance_id: InstanceId) -> String {
    let b58 = bs58::encode(instance_id.as_bytes()).into_string();
    format!("{cluster_id}.velo.{b58}.health")
}

#[cfg(test)]
mod tests {
    use super::*;
    use velo_common::InstanceId;

    #[test]
    fn test_inbound_subject_deterministic() {
        let instance_id = InstanceId::new_v4();
        let s1 = inbound_subject("my-cluster", instance_id);
        let s2 = inbound_subject("my-cluster", instance_id);
        assert_eq!(s1, s2, "Same InstanceId must produce same subject");
    }

    #[test]
    fn test_health_subject_deterministic() {
        let instance_id = InstanceId::new_v4();
        let s1 = health_subject("prod", instance_id);
        let s2 = health_subject("prod", instance_id);
        assert_eq!(s1, s2, "Same InstanceId must produce same health subject");
    }

    #[test]
    fn test_different_instance_ids_produce_different_subjects() {
        let id1 = InstanceId::new_v4();
        let id2 = InstanceId::new_v4();
        let s1 = inbound_subject("cluster", id1);
        let s2 = inbound_subject("cluster", id2);
        assert_ne!(
            s1, s2,
            "Different InstanceIds must produce different subjects"
        );
    }

    #[test]
    fn test_inbound_subject_format() {
        let instance_id = InstanceId::new_v4();
        let subject = inbound_subject("test-cluster", instance_id);

        // Must start with cluster_id prefix
        assert!(
            subject.starts_with("test-cluster.velo."),
            "Subject must start with 'test-cluster.velo.', got: {subject}"
        );

        // Split and verify the base58 segment
        let parts: Vec<&str> = subject.splitn(3, '.').collect();
        assert_eq!(
            parts.len(),
            3,
            "Subject must have format cluster.velo.base58"
        );

        // The third segment after "test-cluster.velo." is the base58 portion
        let prefix = "test-cluster.velo.";
        let b58_part = &subject[prefix.len()..];

        // Base58 characters must all be alphanumeric (no dots, hyphens, or special chars)
        assert!(
            b58_part.chars().all(|c| c.is_ascii_alphanumeric()),
            "Base58 segment must be all alphanumeric, got: {b58_part}"
        );

        // A 16-byte input encodes to at most 22 characters in base58
        assert!(
            b58_part.len() <= 22 && !b58_part.is_empty(),
            "Base58 of 16 bytes must be 1-22 chars, got len={} for '{b58_part}'",
            b58_part.len()
        );
    }

    #[test]
    fn test_health_subject_appends_dot_health() {
        let instance_id = InstanceId::new_v4();
        let inbound = inbound_subject("cluster", instance_id);
        let health = health_subject("cluster", instance_id);

        assert_eq!(
            health,
            format!("{inbound}.health"),
            "health_subject must equal inbound_subject + '.health'"
        );
    }

    #[test]
    fn test_base58_segment_has_no_special_chars() {
        // Run across multiple IDs to increase confidence
        for _ in 0..20 {
            let id = InstanceId::new_v4();
            let subject = inbound_subject("c", id);
            // Extract base58 part after "c.velo."
            let b58 = &subject["c.velo.".len()..];
            for ch in b58.chars() {
                assert!(
                    ch.is_ascii_alphanumeric(),
                    "Unexpected char '{ch}' in base58 segment '{b58}'"
                );
            }
        }
    }

    #[test]
    fn test_cluster_id_is_preserved_in_subject() {
        let id = InstanceId::new_v4();
        let subject = inbound_subject("my.cluster.name", id);
        assert!(
            subject.starts_with("my.cluster.name.velo."),
            "Cluster ID must be preserved verbatim in subject prefix"
        );
    }
}
