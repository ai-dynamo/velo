// SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Message ID - A human-readable, compact identifier for active messages
//!
//! This wraps the internal `ResponseId` and provides multiple display formats
//! optimized for different use cases:
//!
//! - **Default/Display**: Compact Base58 encoding (e.g., `w:3yQ7K8pRv / m:9ZzK2xPq`)
//! - **Debug**: Full hex with component breakdown
//! - **Alternate**: UUID format for compatibility

use super::responses::ResponseId;
use std::fmt;
use std::num::NonZeroU64;
use velo_ext::WorkerId;

/// A message identifier that provides human-readable display formats
///
/// Internally wraps a `ResponseId` (which is a u128) composed of:
/// - Worker ID (64 bits)
/// - Slot index (16 bits)
/// - Generation (48 bits)
///
/// # Display Formats
///
/// ```text
/// Display:   w:3yQ7K8pRv / m:9ZzK2xPq
/// Debug:     MessageId { worker: 12345678, slot: 42, gen: 7890 }
/// Alternate: 01234567-89ab-cdef-0123-456789abcdef (UUID)
/// ```
///
/// Two ids are equal when they name the same message on the wire.
///
/// The second field is not part of the id. It is the receiver's key to a
/// staged payload that the message's handler deferred, set once when the
/// handler's context is made. It lets a context clone find that payload, and
/// tell a deferred payload from an empty one after the handler has returned.
#[derive(Clone, Copy)]
pub struct MessageId(ResponseId, Option<NonZeroU64>);

impl PartialEq for MessageId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for MessageId {}

impl std::hash::Hash for MessageId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl MessageId {
    /// Create a new MessageId from a ResponseId
    pub(crate) fn new(response_id: ResponseId) -> Self {
        Self(response_id, None)
    }

    /// The id of a message whose staged payload its handler fetches, with
    /// the key the receiver holds that payload under.
    pub(crate) fn deferred(response_id: ResponseId, key: NonZeroU64) -> Self {
        Self(response_id, Some(key))
    }

    pub(crate) fn deferred_key(&self) -> Option<NonZeroU64> {
        self.1
    }

    /// Get the underlying ResponseId
    #[expect(dead_code)]
    pub(crate) fn response_id(&self) -> ResponseId {
        self.0
    }

    /// Get the worker ID that created this message
    pub fn worker_id(&self) -> WorkerId {
        WorkerId::from_u64(self.0.worker_id())
    }

    pub fn worker(&self) -> String {
        let worker_id = self.worker_id().as_u64();
        Self::encode_u64_base58(worker_id)
    }

    pub fn message(&self) -> String {
        let message_id = self.generation() << 16 | self.slot_index() as u64;
        Self::encode_u64_base58(message_id)
    }

    /// Get the slot index (internal)
    pub(crate) fn slot_index(&self) -> usize {
        self.0.slot_index()
    }

    /// Get the generation counter (internal)
    pub(crate) fn generation(&self) -> u64 {
        self.0.generation()
    }

    /// Encode a u64 as Base58 (compact, human-readable)
    fn encode_u64_base58(value: u64) -> String {
        bs58::encode(value.to_be_bytes()).into_string()
    }

    /// Encode worker and message parts separately for display
    fn display_parts(&self) -> (String, String) {
        (self.worker(), self.message())
    }
}

impl From<ResponseId> for MessageId {
    fn from(response_id: ResponseId) -> Self {
        Self::new(response_id)
    }
}

impl From<MessageId> for ResponseId {
    fn from(message_id: MessageId) -> Self {
        message_id.0
    }
}

/// Default Display: Compact Base58 format
/// Example: `w:3yQ7K8pRv / m:9ZzK2xPq`
impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (worker, message) = self.display_parts();
        write!(f, "w:{} / m:{}", worker, message)
    }
}

/// Debug format: Shows component breakdown with hex values
/// Example: `MessageId { worker: 0x1234, slot: 42, generation: 0x1e240 }`
impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            // Alternate debug shows UUID
            write!(f, "MessageId({})", self.0)
        } else {
            // Standard debug shows components
            f.debug_struct("MessageId")
                .field("worker", &format_args!("{:#x}", self.worker_id().as_u64()))
                .field("slot", &self.slot_index())
                .field("generation", &format_args!("{:#x}", self.generation()))
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key to a deferred payload is not part of the id: a handler's
    /// `ctx.message_id` equals the id of the same message from anywhere
    /// else, and hashes the same, whether or not its payload was deferred.
    #[test]
    fn a_deferred_key_does_not_change_the_id() {
        use std::hash::{BuildHasher, RandomState};

        let wire = ResponseId::from_u128(7);
        let plain = MessageId::new(wire);
        let deferred = MessageId::deferred(wire, NonZeroU64::MIN);
        let hasher = RandomState::new();

        assert_eq!(plain, deferred);
        assert_eq!(hasher.hash_one(plain), hasher.hash_one(deferred));
        assert_eq!(plain.to_string(), deferred.to_string());
        assert_ne!(plain, MessageId::new(ResponseId::from_u128(8)));
    }

    #[test]
    fn test_message_id_display() {
        let response_id = ResponseId::from_u128(0x0001_0000_0000_002A_0000_0000_0000_1234);
        let msg_id = MessageId::new(response_id);

        let display = format!("{}", msg_id);
        assert!(display.contains("/"));

        assert!(display.len() < 40);
    }

    #[test]
    fn test_message_id_debug() {
        let response_id = ResponseId::from_u128(0x0001_0000_0000_002A_0000_0000_0000_1234);
        let msg_id = MessageId::new(response_id);

        let debug = format!("{:?}", msg_id);
        assert!(debug.contains("MessageId"));
        assert!(debug.contains("worker"));
        assert!(debug.contains("slot"));
        assert!(debug.contains("generation"));
    }

    #[test]
    fn test_message_id_alternate_debug() {
        let response_id = ResponseId::from_u128(0x0001_0000_0000_002A_0000_0000_0000_1234);
        let msg_id = MessageId::new(response_id);

        let debug = format!("{:#?}", msg_id);
        assert!(debug.contains("MessageId"));
    }

    #[test]
    fn test_message_id_conversion() {
        let response_id = ResponseId::from_u128(12345);
        let msg_id = MessageId::from(response_id);
        let back: ResponseId = msg_id.into();

        assert_eq!(response_id, back);
    }

    #[test]
    fn test_base58_encoding_is_compact() {
        let worker_id = 0x123456789ABCDEF0u64;
        let encoded = MessageId::encode_u64_base58(worker_id);

        assert!(encoded.len() < 16);
        assert!(encoded.len() >= 10);

        assert!(!encoded.contains('0'));
        assert!(!encoded.contains('O'));
        assert!(!encoded.contains('I'));
        assert!(!encoded.contains('l'));
    }
}
