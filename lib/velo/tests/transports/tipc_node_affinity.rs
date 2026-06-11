#![cfg(all(feature = "tipc", target_os = "linux"))]

//! TODO: node-affinity and reachability-gate tests (netid mismatch, netns_nonce, duplicate-identity,
//! stale endpoint, and cold-start recovery) mirroring `uds_host_affinity.rs`.
