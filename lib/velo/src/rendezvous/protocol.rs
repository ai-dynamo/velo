// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Wire types for the rendezvous control-plane protocol.
//!
//! All types are serialized via serde_json for use with velo-messenger's
//! typed-unary and fire-and-forget active message handlers.
//!
//! # Version skew, in both directions
//!
//! Pre-1.0 velo assumes compatible-deploy windows, but the RDMA path is
//! deliberately built so a mixed window degrades to the behaviour that existed
//! before it rather than to an error. Two `#[serde(default)]` fields carry the
//! whole story, and each is safe in the direction it is not present:
//!
//! * [`RvAcquireRequest::rdma`] — a **new consumer** advertises which RDMA
//!   backends it can consume. An old owner does not know the field; serde_json
//!   ignores unknown fields, so it replies [`AcquireResponse::Ready`] and the
//!   consumer pulls chunks. An **old consumer** omits the field; it defaults to
//!   `None`, and an owner that sees no offer never answers
//!   [`AcquireResponse::Rdma`] — which is exactly right, because a consumer
//!   that did not ask could not decode the descriptor anyway.
//!
//! * [`AcquireResponse::Rdma::lease_timeout_ms`] — a **new owner** tells the
//!   consumer how long the lease survives without a keepalive. An old consumer
//!   never sees an `Rdma` response at all (it sent no offer), so the field only
//!   ever reaches a consumer that understands it. It is nonetheless
//!   `#[serde(default)]` so that the *variant* stays readable if a future owner
//!   omits it, and `0` is the documented "no deadline" encoding rather than a
//!   sentinel a reader has to know about: a consumer that gets `0` starts no
//!   renewal ticker, which is precisely the pre-deadline behaviour.
//!
//! The rule that makes both work: every field added to these types from here on
//! is `#[serde(default)]`, and every default must mean *what this protocol did
//! before the field existed*. A default that means "the new behaviour" would
//! make an old peer's silence look like consent.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_metadata` typed-unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvMetadataRequest {
    /// The handle to query (as u128 wire format).
    pub handle: RvHandleWire,
}

/// Response from `_rv_metadata`: lightweight info about staged data (no lock acquired).
#[derive(Debug, Serialize, Deserialize)]
pub struct DataMetadata {
    /// Total bytes of the staged payload.
    pub total_len: u64,
    /// Current refcount.
    pub refcount: u32,
    /// Whether the data is RDMA-pinned (Phase 2).
    pub pinned: bool,
}

// ---------------------------------------------------------------------------
// Acquire (read lock + data transfer initiation)
// ---------------------------------------------------------------------------

/// What RDMA backends a consumer can pull with (D6, D12).
///
/// Purely a *capability advertisement*: the consumer states what it could
/// consume, and the owner decides. There is no negotiation and no handshake
/// change — the offer rides the acquire that was already happening.
///
/// The names are backend keys (`"ucx"` in v1), matched against the owner's own
/// [`RdmaBackend::key`](crate::rendezvous::rdma::RdmaBackend::key). Naming them
/// rather than numbering them keeps the offer readable in a log and keeps a
/// future provider from having to claim a number before it exists.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RdmaOffer {
    /// Backends this consumer can pull with, most preferred first. The owner
    /// serves the first one it can.
    pub backends: Vec<String>,
}

/// Request payload for the `_rv_acquire` typed-unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvAcquireRequest {
    /// The handle to acquire a read lock on.
    pub handle: RvHandleWire,
    /// RDMA backends this consumer can pull with, if any.
    ///
    /// Absent on the wire for a consumer built before the RDMA path, and
    /// `None` for one that has it switched off or has no UCX endpoint to the
    /// owner. Either way the owner answers [`AcquireResponse::Ready`]. See the
    /// module docs for the full skew story.
    #[serde(default)]
    pub rdma: Option<RdmaOffer>,
}

/// Response from `_rv_acquire`: transfer metadata with read lock.
///
/// Data is always pulled via `_rv_pull` — even for single-chunk payloads —
/// unless the owner could answer with an RDMA descriptor. This avoids
/// JSON-encoding binary data in the typed-unary response and ensures a single
/// code path for all payload sizes.
#[derive(Debug, Serialize, Deserialize)]
pub enum AcquireResponse {
    /// Data available via chunked pull (1 or more chunks).
    Ready {
        lease_id: u64,
        transfer_id: u64,
        total_len: u64,
        chunk_size: u32,
        chunk_count: u32,
    },
    /// The slot is staged in registered memory and the consumer offered a
    /// backend that can read it: here is where to read from.
    ///
    /// No transfer is created for this lease — there are no chunks to pull.
    /// The consumer issues one RDMA GET and then detaches or releases exactly
    /// as it would after a chunked pull.
    Rdma {
        lease_id: u64,
        /// A [`RdmaDescriptor`](crate::rendezvous::descriptor::RdmaDescriptor)
        /// in its fixed binary layout. Deliberately not a serde structure; see
        /// that module for why the framing has to be velo's own.
        descriptor: Vec<u8>,
        /// How long the lease survives without a `_rv_lease_renew`, in
        /// milliseconds. `0` means no deadline — the encoding an owner from
        /// before the reaper existed produces by omitting the field.
        #[serde(default)]
        lease_timeout_ms: u64,
    },
}

// ---------------------------------------------------------------------------
// Pull (individual chunk retrieval)
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_pull` unary handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvPullRequest {
    /// Transfer ID from the `AcquireResponse::Chunked`.
    pub transfer_id: u64,
    /// Zero-based chunk index to retrieve.
    pub chunk_index: u32,
}

// ---------------------------------------------------------------------------
// Ref / Detach / Release
// ---------------------------------------------------------------------------

/// Request payload for the `_rv_ref` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvRefRequest {
    pub handle: RvHandleWire,
}

/// Request payload for the `_rv_detach` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvDetachRequest {
    pub handle: RvHandleWire,
    pub lease_id: u64,
}

/// Request payload for the `_rv_release` fire-and-forget handler.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvReleaseRequest {
    pub handle: RvHandleWire,
    pub lease_id: u64,
}

/// Request payload for the `_rv_lease_renew` fire-and-forget handler (D8).
///
/// Pushes an RDMA lease's deadline out by the timeout it was granted under, so
/// a transfer that is merely *slow* is not mistaken for a consumer that
/// vanished. Fire-and-forget on purpose: a lost renewal is benign — the
/// standing deadline still applies and the next renewal arrives before it — and
/// making it a round trip would put a control-plane wait in the middle of a
/// transfer whose whole point is not to have one.
///
/// A renewal for a lease that has already ended is a no-op, not an error. The
/// keepalive races the release that ends the transfer by construction.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvLeaseRenewRequest {
    pub handle: RvHandleWire,
    pub lease_id: u64,
}

// ---------------------------------------------------------------------------
// Handle wire format
// ---------------------------------------------------------------------------

/// Wire-safe representation of a [`crate::DataHandle`] as two u64 fields.
///
/// Avoids u128 serialization issues across serde backends.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RvHandleWire {
    pub hi: u64,
    pub lo: u64,
}

impl RvHandleWire {
    pub fn from_handle(handle: crate::DataHandle) -> Self {
        let raw = handle.as_u128();
        Self {
            hi: (raw >> 64) as u64,
            lo: raw as u64,
        }
    }

    pub fn to_handle(self) -> crate::DataHandle {
        crate::DataHandle::from_u128(((self.hi as u128) << 64) | (self.lo as u128))
    }
}

// ---------------------------------------------------------------------------
// Error response
// ---------------------------------------------------------------------------

/// Error returned by rendezvous control-plane handlers.
#[derive(Debug, Serialize, Deserialize)]
pub struct RvError {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shapes as they were *before* this phase, reconstructed here so the
    /// skew tests exercise a real old peer rather than a new one with a field
    /// set to `None`.
    ///
    /// Keeping them in the test rather than under a `#[cfg]` in the module is
    /// deliberate: they are not a supported configuration of this build, they
    /// are the other side of a deploy window, and they must stay frozen even
    /// when the live types move.
    mod old_wire {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize)]
        pub struct RvHandleWire {
            pub hi: u64,
            pub lo: u64,
        }

        #[derive(Debug, Serialize, Deserialize)]
        pub struct RvAcquireRequest {
            pub handle: RvHandleWire,
        }

        #[derive(Debug, Serialize, Deserialize)]
        pub enum AcquireResponse {
            Ready {
                lease_id: u64,
                transfer_id: u64,
                total_len: u64,
                chunk_size: u32,
                chunk_count: u32,
            },
            Rdma {
                lease_id: u64,
                descriptor: Vec<u8>,
            },
        }
    }

    fn handle() -> RvHandleWire {
        RvHandleWire { hi: 7, lo: 42 }
    }

    /// An old consumer's acquire has no `rdma` key at all. It must deserialize,
    /// and it must default to "no offer" — an owner that read a *missing* field
    /// as an offer would send a descriptor to a peer that cannot decode it.
    #[test]
    fn an_old_consumers_acquire_carries_no_offer() {
        let old = serde_json::to_vec(&old_wire::RvAcquireRequest {
            handle: old_wire::RvHandleWire { hi: 7, lo: 42 },
        })
        .expect("serialize the old shape");
        assert!(
            !String::from_utf8_lossy(&old).contains("rdma"),
            "the old shape must not mention the field this test is about"
        );

        let new: RvAcquireRequest = serde_json::from_slice(&old).expect("deserialize");
        assert!(new.rdma.is_none());
        assert_eq!(new.handle.hi, 7);
        assert_eq!(new.handle.lo, 42);
    }

    /// A new consumer's acquire carries the offer. An old owner must still read
    /// it — serde_json ignores unknown fields — and gets exactly the request it
    /// used to get.
    #[test]
    fn an_old_owner_ignores_a_new_consumers_offer() {
        let new = serde_json::to_vec(&RvAcquireRequest {
            handle: handle(),
            rdma: Some(RdmaOffer {
                backends: vec!["ucx".to_string()],
            }),
        })
        .expect("serialize");

        let old: old_wire::RvAcquireRequest =
            serde_json::from_slice(&new).expect("an old owner must still parse a new acquire");
        assert_eq!(old.handle.hi, 7);
        assert_eq!(old.handle.lo, 42);
    }

    /// An explicit `null` is the same as absent: a consumer with the RDMA path
    /// compiled in but switched off sends `rdma: null`, and the owner must read
    /// that as "no offer" rather than as an empty offer it might mishandle.
    #[test]
    fn an_explicit_null_offer_is_no_offer() {
        let json = br#"{"handle":{"hi":7,"lo":42},"rdma":null}"#;
        let req: RvAcquireRequest = serde_json::from_slice(json).expect("deserialize");
        assert!(req.rdma.is_none());
    }

    /// An offer naming only backends this owner does not have is well-formed
    /// and simply unservable — it must parse, so the owner can decline it on
    /// the merits rather than failing the acquire.
    #[test]
    fn an_offer_of_unknown_backends_still_parses() {
        let json = br#"{"handle":{"hi":1,"lo":2},"rdma":{"backends":["nixl","libfabric"]}}"#;
        let req: RvAcquireRequest = serde_json::from_slice(json).expect("deserialize");
        assert_eq!(
            req.rdma.expect("offer").backends,
            vec!["nixl".to_string(), "libfabric".to_string()]
        );
    }

    /// An `Rdma` response from an owner that predates lease deadlines has no
    /// `lease_timeout_ms`. It must deserialize to `0`, which is the documented
    /// "no deadline" encoding and therefore the pre-deadline behaviour.
    #[test]
    fn an_rdma_response_without_a_lease_timeout_means_no_deadline() {
        let old = serde_json::to_vec(&old_wire::AcquireResponse::Rdma {
            lease_id: 9,
            descriptor: vec![1, 2, 3],
        })
        .expect("serialize the old shape");

        let new: AcquireResponse = serde_json::from_slice(&old).expect("deserialize");
        match new {
            AcquireResponse::Rdma {
                lease_id,
                descriptor,
                lease_timeout_ms,
            } => {
                assert_eq!(lease_id, 9);
                assert_eq!(descriptor, vec![1, 2, 3]);
                assert_eq!(lease_timeout_ms, 0, "a missing timeout must mean none");
            }
            other => panic!("expected Rdma, got {other:?}"),
        }
    }

    /// And the reverse: a new owner's `Rdma` response is still readable by a
    /// reader that does not know the field.
    #[test]
    fn a_new_rdma_response_is_readable_by_an_old_consumer() {
        let new = serde_json::to_vec(&AcquireResponse::Rdma {
            lease_id: 9,
            descriptor: vec![1, 2, 3],
            lease_timeout_ms: 30_000,
        })
        .expect("serialize");

        let old: old_wire::AcquireResponse = serde_json::from_slice(&new).expect("deserialize");
        match old {
            old_wire::AcquireResponse::Rdma {
                lease_id,
                descriptor,
            } => {
                assert_eq!(lease_id, 9);
                assert_eq!(descriptor, vec![1, 2, 3]);
            }
            other => panic!("expected Rdma, got {other:?}"),
        }
    }

    /// The chunked variant is untouched by this phase, and a round trip in both
    /// directions is what says so.
    #[test]
    fn the_ready_variant_is_unchanged_in_both_directions() {
        let new = serde_json::to_vec(&AcquireResponse::Ready {
            lease_id: 1,
            transfer_id: 2,
            total_len: 3,
            chunk_size: 4,
            chunk_count: 5,
        })
        .expect("serialize");
        let old: old_wire::AcquireResponse = serde_json::from_slice(&new).expect("old reads new");
        assert!(matches!(old, old_wire::AcquireResponse::Ready { .. }));

        let old_bytes = serde_json::to_vec(&old_wire::AcquireResponse::Ready {
            lease_id: 1,
            transfer_id: 2,
            total_len: 3,
            chunk_size: 4,
            chunk_count: 5,
        })
        .expect("serialize");
        let new: AcquireResponse = serde_json::from_slice(&old_bytes).expect("new reads old");
        assert!(matches!(
            new,
            AcquireResponse::Ready {
                lease_id: 1,
                transfer_id: 2,
                total_len: 3,
                chunk_size: 4,
                chunk_count: 5,
            }
        ));
    }
}
