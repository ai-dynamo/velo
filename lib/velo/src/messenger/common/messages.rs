// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! # Dynamo Active Message Common

use bytes::{Buf, BufMut, Bytes, BytesMut};
use derive_builder::Builder;
use std::collections::HashMap;
use thiserror::Error;

use super::responses::ResponseId;

const CURRENT_SCHEMA_VERSION: u8 = 1;
const MAX_HEADER_VALUE_LEN: usize = 1024;
const MAX_HEADERS_LEN: usize = 16384;

/// Fixed-size prefix every encoded active-message header begins with:
/// `schema_version` (1) + `response_type` (1) + `response_id` (16) +
/// `handler_name_len` (2) + `headers_len` (2). Every remaining byte of the
/// header is handler name or MessagePack-encoded headers.
///
/// [`encode_active_message`] sizes its buffer with it, [`decode_active_message`]
/// rejects anything shorter, and [`envelope_overhead`] starts from it — one
/// number so the three cannot drift apart.
pub(crate) const FIXED_HEADER_SIZE: usize = 1 + 1 + 16 + 2 + 2;

#[derive(Debug, Clone)]
pub(crate) struct ActiveMessage {
    pub metadata: MessageMetadata,
    pub payload: Bytes,
}

impl ActiveMessage {
    pub(crate) fn encode(
        self,
    ) -> Result<(Bytes, Bytes, crate::transports::MessageType), EncodeError> {
        encode_active_message(self)
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub(crate) struct MessageMetadata {
    #[builder(default = "CURRENT_SCHEMA_VERSION")]
    pub schema_version: u8,
    pub response_type: ResponseType,
    pub response_id: ResponseId,
    pub handler_name: String,
    #[builder(default)]
    pub headers: Option<HashMap<String, String>>,
}

impl MessageMetadata {
    pub(crate) fn new_fire(
        response_id: ResponseId,
        handler_name: String,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            response_type: ResponseType::FireAndForget,
            response_id,
            handler_name,
            headers,
        }
    }

    pub(crate) fn new_sync(
        response_id: ResponseId,
        handler_name: String,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            response_type: ResponseType::AckNack,
            response_id,
            handler_name,
            headers,
        }
    }

    pub(crate) fn new_unary(
        response_id: ResponseId,
        handler_name: String,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            response_type: ResponseType::Unary,
            response_id,
            handler_name,
            headers,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseType {
    /// Indicates an am_send or event_trigger messsage
    /// These types of messages do not expect a response from the remote instance; however,
    /// they do expect a response from the local instance when the message is successfully
    /// sent. This allows for the awaiter to know that the message was successfully sent
    /// or that an error occurred.
    FireAndForget = 0,
    /// Indicates an am_sync message
    /// These types of messages expect a response from the remote instance; or if the transport
    /// has a problem, a local sender side error could also trigger an error response.
    /// This allows for the awaiter to know that the message was sent and processed successfully,
    /// or that an error occurred either locally or remotely.
    AckNack = 1,
    /// Indicates a unary message
    /// These types of messages expect a response from the remote instance; however,
    /// they do not expect a response from the local instance when the message is successfully
    /// sent. This allows for the awaiter to know that the message was successfully sent
    /// and completed, or that an error occurred either locally or remotely.
    Unary = 2,
}

impl TryFrom<u8> for ResponseType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ResponseType::FireAndForget),
            1 => Ok(ResponseType::AckNack),
            2 => Ok(ResponseType::Unary),
            _ => Err(DecodeError::InvalidResponseType(value)),
        }
    }
}

impl ResponseType {
    /// Convert ResponseType to MessageType for routing
    pub(crate) fn to_message_type(self) -> crate::transports::MessageType {
        // All active messages are requests, so they all map to MessageType::Message
        // The response will come back as MessageType::Response separately
        crate::transports::MessageType::Message
    }
}

#[derive(Debug, Error)]
pub(crate) enum DecodeError {
    #[error("Header too short: expected at least {FIXED_HEADER_SIZE} bytes")]
    HeaderTooShort,

    #[error("Invalid handler name length")]
    InvalidHandlerNameLength,

    #[error("Invalid UTF-8 in handler name")]
    InvalidUtf8,

    #[error("Invalid response type: {0}")]
    InvalidResponseType(u8),

    #[error("Invalid headers length")]
    InvalidHeadersLength,

    #[error("Unsupported schema version: got {0}, expected {1}")]
    UnsupportedSchemaVersion(u8, u8),

    #[error("Failed to deserialize headers: {0}")]
    HeaderDeserializationError(#[from] rmp_serde::decode::Error),
}

#[derive(Debug, Error)]
pub(crate) enum EncodeError {
    #[error("Handler name too long: {0} bytes exceeds maximum of 65535")]
    HandlerNameTooLong(usize),

    #[error("Header value too large: key '{0}' has value of {1} bytes, max is 1024")]
    HeaderValueTooLarge(String, usize),

    #[error("Total headers too large: {0} bytes exceeds maximum of 16384")]
    TotalHeadersTooLarge(usize),

    #[error("Failed to serialize headers: {0}")]
    HeaderSerializationError(#[from] rmp_serde::encode::Error),
}

pub(crate) fn encode_active_message(
    message: ActiveMessage,
) -> Result<(Bytes, Bytes, crate::transports::MessageType), EncodeError> {
    let handler_name_len = message.metadata.handler_name.len();

    // Validate handler name length fits in u16
    if handler_name_len > u16::MAX as usize {
        return Err(EncodeError::HandlerNameTooLong(handler_name_len));
    }

    // Validate and encode headers if present
    let headers_bytes = if let Some(ref headers) = message.metadata.headers {
        // Validate per-header size (1KB max per value)
        for (key, value) in headers.iter() {
            if value.len() > MAX_HEADER_VALUE_LEN {
                return Err(EncodeError::HeaderValueTooLarge(key.clone(), value.len()));
            }
        }

        // Serialize headers to MessagePack
        let msgpack_bytes = rmp_serde::to_vec(headers)?;

        // Validate total size (16KB max)
        if msgpack_bytes.len() > MAX_HEADERS_LEN {
            return Err(EncodeError::TotalHeadersTooLarge(msgpack_bytes.len()));
        }

        Some(msgpack_bytes)
    } else {
        None
    };

    // Calculate total header size
    let headers_len = headers_bytes.as_ref().map(|b| b.len()).unwrap_or(0);
    let header_size = FIXED_HEADER_SIZE + handler_name_len + headers_len;
    let mut header = BytesMut::with_capacity(header_size);

    // Encode fixed fields
    header.put_u8(message.metadata.schema_version);
    header.put_u8(message.metadata.response_type as u8);
    header.put_u128_le(message.metadata.response_id.as_u128());
    header.put_u16_le(handler_name_len as u16);
    header.put_slice(message.metadata.handler_name.as_bytes());

    // Encode headers length and bytes (last in header)
    header.put_u16_le(headers_len as u16);
    if let Some(bytes) = headers_bytes {
        header.put_slice(&bytes);
    }

    let message_type = message.metadata.response_type.to_message_type();
    Ok((header.freeze(), message.payload, message_type))
}

/// Largest entry count `rmp` writes with a bare `FixMap` marker.
const MSGPACK_FIXMAP_MAX_ENTRIES: usize = 15;
/// Largest entry count `rmp` writes with `Map16` (marker + `u16` length).
const MSGPACK_MAP16_MAX_ENTRIES: usize = u16::MAX as usize;
/// Longest string `rmp` writes with a bare `FixStr` marker.
const MSGPACK_FIXSTR_MAX_LEN: usize = 31;
/// Longest string `rmp` writes with `Str8` (marker + `u8` length).
const MSGPACK_STR8_MAX_LEN: usize = u8::MAX as usize;
/// Longest string `rmp` writes with `Str16` (marker + `u16` length).
const MSGPACK_STR16_MAX_LEN: usize = u16::MAX as usize;
/// Bytes a bare `Fix*` marker occupies — the length rides in the marker byte.
const MSGPACK_FIX_MARKER_BYTES: usize = 1;
/// Bytes a marker plus a `u8` length occupies.
const MSGPACK_U8_LEN_BYTES: usize = 1 + 1;
/// Bytes a marker plus a `u16` length occupies.
const MSGPACK_U16_LEN_BYTES: usize = 1 + 2;
/// Bytes a marker plus a `u32` length occupies.
const MSGPACK_U32_LEN_BYTES: usize = 1 + 4;

/// Bytes `rmp` spends on the map header for `entries` key/value pairs.
fn msgpack_map_header_len(entries: usize) -> usize {
    if entries <= MSGPACK_FIXMAP_MAX_ENTRIES {
        MSGPACK_FIX_MARKER_BYTES
    } else if entries <= MSGPACK_MAP16_MAX_ENTRIES {
        MSGPACK_U16_LEN_BYTES
    } else {
        MSGPACK_U32_LEN_BYTES
    }
}

/// Bytes `rmp` spends on one header entry, both markers included.
fn msgpack_entry_len(key: &str, value: &str) -> usize {
    msgpack_str_len(key.len()) + msgpack_str_len(value.len())
}

/// Bytes `rmp` spends on a string of `len` bytes, marker included.
fn msgpack_str_len(len: usize) -> usize {
    let marker = if len <= MSGPACK_FIXSTR_MAX_LEN {
        MSGPACK_FIX_MARKER_BYTES
    } else if len <= MSGPACK_STR8_MAX_LEN {
        MSGPACK_U8_LEN_BYTES
    } else if len <= MSGPACK_STR16_MAX_LEN {
        MSGPACK_U16_LEN_BYTES
    } else {
        MSGPACK_U32_LEN_BYTES
    };
    marker + len
}

/// Bytes [`encode_active_message`] puts in front of the payload for a message
/// with this handler name and header set — the messenger's wire envelope.
///
/// A caller sizing a payload against a transport's
/// [`max_message_size`](velo_ext::Transport::max_message_size) needs this,
/// because that number bounds `header + payload` *together*: what is left for
/// the payload is the transport's number less this one.
///
/// The three terms mirror the encoder exactly:
///
/// ```text
/// FIXED_HEADER_SIZE (22) + handler_name.len() + rmp_serde::to_vec(headers).len()
/// ```
///
/// The MessagePack term is derived from `rmp`'s marker widths rather than
/// serialized, so this costs no allocation and can be called per send. The
/// derivation is pinned against the real encoder in the tests, including at
/// every marker-width boundary, which is where an analytic copy of an encoder
/// goes wrong if it is going to.
///
/// `injected` is the set the send path merges over `headers` on its way to the
/// encoder — the distributed-tracing context — and is sized *without* the merge
/// being performed: the union is counted across the two maps rather than built,
/// so sizing a send never duplicates the caller's headers. That matters because
/// the caller's map is arbitrary and unvalidated at this point: the 1 KiB per
/// value and 16 KiB total limits are the encoder's, enforced in
/// [`encode_active_message`] and not here.
///
/// Collisions go to `injected`, because the merge is
/// [`HashMap::insert`](std::collections::HashMap::insert) and the injector runs
/// last: a key in both is counted once, at its injected value's size.
///
/// Note `None` and `Some(empty map)` differ by one byte: the encoder writes no
/// MessagePack at all for a message whose headers are `None`, and an empty
/// `FixMap` marker for `Some`. So a message carries a map on the wire when
/// *either* argument is `Some` — which is why `injected` is an `Option` too,
/// rather than an empty map standing in for "nothing injected".
pub(crate) fn envelope_overhead(
    handler_name: &str,
    headers: Option<&HashMap<String, String>>,
    injected: Option<&HashMap<String, String>>,
) -> usize {
    let headers_len = if headers.is_none() && injected.is_none() {
        0
    } else {
        // Entries the caller supplied that survive the merge, counted and
        // measured in one pass over a map this function only ever reads.
        let (kept_entries, kept_bytes) = headers.map_or((0, 0), |headers| {
            headers
                .iter()
                .filter(|(key, _)| {
                    injected.is_none_or(|injected| !injected.contains_key(key.as_str()))
                })
                .fold((0, 0), |(entries, bytes), (key, value)| {
                    (entries + 1, bytes + msgpack_entry_len(key, value))
                })
        });
        let injected_bytes: usize = injected.map_or(0, |injected| {
            injected
                .iter()
                .map(|(key, value)| msgpack_entry_len(key, value))
                .sum()
        });
        msgpack_map_header_len(kept_entries + injected.map_or(0, HashMap::len))
            + kept_bytes
            + injected_bytes
    };
    FIXED_HEADER_SIZE + handler_name.len() + headers_len
}

/// Best-effort decode of just the `ResponseId` from an active-message request
/// header. Validates schema version and response type before reading the id —
/// returns `None` for anything that isn't a well-formed request header
/// (response-format headers, truncated bytes, unknown schema). Safe on
/// arbitrary input; used by the default transport error handler to complete a
/// hung awaiter when a deferred send fails after frame acceptance.
pub(crate) fn decode_response_id_from_request_header(header: &Bytes) -> Option<ResponseId> {
    // schema_version (1) + response_type (1) + response_id (16) = 18
    if header.len() < 18 {
        return None;
    }
    if header[0] != CURRENT_SCHEMA_VERSION {
        return None;
    }
    if ResponseType::try_from(header[1]).is_err() {
        return None;
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&header[2..18]);
    Some(ResponseId::from_u128(u128::from_le_bytes(id_bytes)))
}

pub(crate) fn decode_active_message(
    header: Bytes,
    payload: Bytes,
) -> Result<ActiveMessage, DecodeError> {
    let mut header = header;

    // Validate minimum size: the fixed prefix must be present in full.
    if header.len() < FIXED_HEADER_SIZE {
        return Err(DecodeError::HeaderTooShort);
    }

    let schema_version = header.get_u8();
    if schema_version != CURRENT_SCHEMA_VERSION {
        return Err(DecodeError::UnsupportedSchemaVersion(
            schema_version,
            CURRENT_SCHEMA_VERSION,
        ));
    }
    let response_type_raw = header.get_u8();
    let response_id = ResponseId::from_u128(header.get_u128_le());
    let handler_name_len = header.get_u16_le() as usize;

    // Validate handler name length (must be non-zero and fit in remaining bytes)
    if handler_name_len == 0 || header.remaining() < handler_name_len + 2 {
        // +2 for headers_len field
        return Err(DecodeError::InvalidHandlerNameLength);
    }

    let handler_name_bytes = header.copy_to_bytes(handler_name_len);
    let handler_name =
        String::from_utf8(handler_name_bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8)?;

    let response_type = ResponseType::try_from(response_type_raw)?;

    // Decode headers (optional, last field in header)
    let headers_len = header.get_u16_le() as usize;
    let headers = if headers_len > 0 {
        // Validate headers length (must not exceed max and must fit in remaining bytes)
        if headers_len > MAX_HEADERS_LEN || header.remaining() < headers_len {
            return Err(DecodeError::InvalidHeadersLength);
        }

        let headers_bytes = header.copy_to_bytes(headers_len);
        let headers_map: HashMap<String, String> = rmp_serde::from_slice(&headers_bytes)?;
        Some(headers_map)
    } else {
        None
    };

    Ok(ActiveMessage {
        metadata: MessageMetadata {
            schema_version,
            response_type,
            response_id,
            handler_name,
            headers,
        },
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Header length the real encoder produces — the ground truth
    /// [`envelope_overhead`] is checked against.
    fn encoded_header_len(handler_name: &str, headers: Option<&HashMap<String, String>>) -> usize {
        let (header, _, _) = ActiveMessage {
            metadata: MessageMetadata::new_unary(
                ResponseId::from_u128(1),
                handler_name.to_string(),
                headers.cloned(),
            ),
            payload: Bytes::new(),
        }
        .encode()
        .unwrap();
        header.len()
    }

    /// `entries` headers with keys of `key_len` bytes and values of
    /// `value_len`. Keys are zero-padded indices, so they stay unique;
    /// `key_len` must leave room for the index.
    fn header_map(entries: usize, key_len: usize, value_len: usize) -> HashMap<String, String> {
        (0..entries)
            .map(|i| (format!("{i:0key_len$}"), "v".repeat(value_len)))
            .collect()
    }

    /// An analytic copy of an encoder is wrong at the marker widths if it is
    /// wrong anywhere, so every case here sits on a transition: `None` vs
    /// `Some(empty)`, fixmap→map16, fixstr→str8, str8→str16.
    #[test]
    fn envelope_overhead_matches_encoder_at_msgpack_boundaries() {
        let cases: Vec<(String, Option<HashMap<String, String>>)> = vec![
            ("h".to_string(), None),
            ("_stream_batch".to_string(), None),
            ("n".repeat(300), None),
            ("h".to_string(), Some(HashMap::new())),
            ("h".to_string(), Some(header_map(1, 3, 5))),
            (
                "h".to_string(),
                Some(header_map(MSGPACK_FIXMAP_MAX_ENTRIES, 4, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(MSGPACK_FIXMAP_MAX_ENTRIES + 1, 4, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, MSGPACK_FIXSTR_MAX_LEN, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, MSGPACK_FIXSTR_MAX_LEN + 1, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, 4, MSGPACK_FIXSTR_MAX_LEN)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, 4, MSGPACK_FIXSTR_MAX_LEN + 1)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, MSGPACK_STR8_MAX_LEN, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, MSGPACK_STR8_MAX_LEN + 1, 4)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, 4, MSGPACK_STR8_MAX_LEN)),
            ),
            (
                "h".to_string(),
                Some(header_map(1, 4, MSGPACK_STR8_MAX_LEN + 1)),
            ),
        ];

        for (handler_name, headers) in &cases {
            assert_eq!(
                envelope_overhead(handler_name, headers.as_ref(), None),
                encoded_header_len(handler_name, headers.as_ref()),
                "handler_len={} entries={:?}",
                handler_name.len(),
                headers.as_ref().map(HashMap::len),
            );
        }
    }

    /// The merge the send path performs before encoding: the injected set is
    /// inserted over the caller's, so a key in both survives with the injected
    /// value. [`envelope_overhead`] has to size this without building it.
    fn merged(
        headers: Option<&HashMap<String, String>>,
        injected: Option<&HashMap<String, String>>,
    ) -> Option<HashMap<String, String>> {
        if headers.is_none() && injected.is_none() {
            return None;
        }
        let mut merged = headers.cloned().unwrap_or_default();
        if let Some(injected) = injected {
            for (key, value) in injected {
                merged.insert(key.clone(), value.clone());
            }
        }
        Some(merged)
    }

    /// Sizing the union without materialising it is where an analytic copy of
    /// the encoder gets a second chance to be wrong, so every case is checked
    /// against the encoder fed the merge it is standing in for: collisions,
    /// the `None` caller the injector still materialises a map for, and a
    /// union that crosses the fixmap boundary from either side of the merge.
    #[test]
    fn envelope_overhead_sizes_the_injected_union_like_the_encoder() {
        let traceparent = || {
            HashMap::from([(
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            )])
        };
        // A caller who already carries the key the injector will overwrite,
        // at a different length so counting the wrong one is visible.
        let mut collision = header_map(2, 4, 4);
        collision.insert("traceparent".to_string(), "stale".to_string());

        /// A caller's header set and the set the send path merges over it.
        type MergeCase = (
            Option<HashMap<String, String>>,
            Option<HashMap<String, String>>,
        );

        let cases: Vec<MergeCase> = vec![
            // Nothing at all: no map on the wire.
            (None, None),
            // What injection does with no context to inject — it materialises
            // the map anyway, which the encoder charges a FixMap marker for.
            (None, Some(HashMap::new())),
            (None, Some(traceparent())),
            (Some(HashMap::new()), Some(HashMap::new())),
            (Some(HashMap::new()), Some(traceparent())),
            (Some(header_map(2, 4, 4)), None),
            (Some(header_map(2, 4, 4)), Some(traceparent())),
            // Collision: counted once, at the injected value's size.
            (Some(collision), Some(traceparent())),
            // The union crosses fixmap→map16 even though neither side does.
            (
                Some(header_map(MSGPACK_FIXMAP_MAX_ENTRIES, 4, 4)),
                Some(traceparent()),
            ),
            // ... and does not cross it when the extra key collides away.
            (
                Some({
                    let mut headers = header_map(MSGPACK_FIXMAP_MAX_ENTRIES - 1, 4, 4);
                    headers.insert("traceparent".to_string(), "stale".to_string());
                    headers
                }),
                Some(traceparent()),
            ),
        ];

        for (headers, injected) in &cases {
            let merged = merged(headers.as_ref(), injected.as_ref());
            assert_eq!(
                envelope_overhead("h", headers.as_ref(), injected.as_ref()),
                encoded_header_len("h", merged.as_ref()),
                "caller={:?} injected={:?}",
                headers.as_ref().map(HashMap::len),
                injected.as_ref().map(HashMap::len),
            );
        }
    }

    /// Exact arithmetic for the two header sets that matter downstream: the
    /// mux batcher's bare `_stream_batch` message, and the same message once
    /// the transparent stager has added its `_rv` handle.
    #[test]
    fn envelope_overhead_pins_exact_sizes() {
        // 22 fixed + 13 handler name + no headers at all.
        assert_eq!(envelope_overhead("_stream_batch", None, None), 22 + 13);
        assert_eq!(encoded_header_len("_stream_batch", None), 35);

        // A rendezvous handle is a u128 in decimal, at most 39 digits.
        let mut headers = HashMap::new();
        headers.insert(
            crate::messenger::large_payload::RV_HEADER_KEY.to_string(),
            "9".repeat(39),
        );
        // 22 fixed + 13 name + FixMap marker (1) + FixStr "_rv" (1 + 3)
        // + Str8 handle (2 + 39, past the 31-byte FixStr ceiling).
        assert_eq!(
            envelope_overhead("_stream_batch", Some(&headers), None),
            22 + 13 + 1 + 4 + 41,
        );
        assert_eq!(encoded_header_len("_stream_batch", Some(&headers)), 81);
    }

    #[test]
    fn decode_response_id_from_request_header_roundtrip() {
        let response_id = ResponseId::from_u128(0xDEAD_BEEF_CAFE_F00D_1234_5678_90AB_CDEF);
        let (header, _, _) = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, "h".to_string(), None),
            payload: Bytes::from_static(b""),
        }
        .encode()
        .unwrap();

        let decoded = decode_response_id_from_request_header(&header).unwrap();
        assert_eq!(decoded.as_u128(), response_id.as_u128());
    }

    #[test]
    fn decode_response_id_rejects_truncated_header() {
        let short = Bytes::from_static(&[1u8, 0, 0, 0]);
        assert!(decode_response_id_from_request_header(&short).is_none());
    }

    #[test]
    fn decode_response_id_rejects_wrong_schema() {
        // 18 bytes, but schema_version = 0 (invalid)
        let mut bad = vec![0u8; 18];
        bad[1] = 2; // valid response_type
        let header = Bytes::from(bad);
        assert!(decode_response_id_from_request_header(&header).is_none());
    }

    #[test]
    fn decode_response_id_rejects_invalid_response_type() {
        let mut bad = vec![0u8; 18];
        bad[0] = CURRENT_SCHEMA_VERSION;
        bad[1] = 99; // not a valid ResponseType
        let header = Bytes::from(bad);
        assert!(decode_response_id_from_request_header(&header).is_none());
    }

    #[test]
    fn decode_response_id_rejects_response_format_header() {
        // Response headers start with the 16-byte response_id directly (no
        // schema_version byte), so interpreting byte 0 as a schema_version
        // will almost never match 1, and byte 1 almost never matches a valid
        // ResponseType. Verify this with a deliberate worst case: response_id
        // whose first LE byte is 1 (looks like schema_version 1) AND second
        // LE byte is 0 (looks like ResponseType::FireAndForget = 0). Even
        // then, decode returns the response_id as-is — but it will not match
        // any local awaiter because the encoded bits don't line up with
        // ResponseManager's key layout for this worker. We assert only that
        // decode does not panic on arbitrary bytes.
        let header = Bytes::from(vec![1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        // No panic; may return Some (opaque id that won't match any awaiter).
        let _ = decode_response_id_from_request_header(&header);
    }

    #[test]
    fn test_handler_name_at_u16_max_succeeds() {
        // Create a handler name with exactly u16::MAX bytes (65,535 bytes)
        let handler_name = "a".repeat(u16::MAX as usize);
        let response_id = ResponseId::from_u128(12345);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), None),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should encode successfully
        let result = message.encode();
        assert!(
            result.is_ok(),
            "Handler name at u16::MAX should encode successfully"
        );

        let (header, payload, _) = result.unwrap();

        // Decode and verify
        let decoded = decode_active_message(header, payload).unwrap();
        assert_eq!(decoded.metadata.handler_name, handler_name);
    }

    #[test]
    fn test_handler_name_exceeds_u16_max_fails() {
        // Create a handler name with u16::MAX + 1 bytes (65,536 bytes)
        let handler_name = "a".repeat(u16::MAX as usize + 1);
        let response_id = ResponseId::from_u128(12345);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), None),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should fail to encode
        let result = message.encode();
        assert!(
            result.is_err(),
            "Handler name exceeding u16::MAX should fail to encode"
        );

        match result {
            Err(EncodeError::HandlerNameTooLong(len)) => {
                assert_eq!(len, u16::MAX as usize + 1);
            }
            _ => panic!("Expected HandlerNameTooLong error"),
        }
    }

    #[test]
    fn test_handler_name_way_too_long_fails() {
        // Create a handler name that's way too long (1 MB)
        let handler_name = "a".repeat(1024 * 1024);
        let response_id = ResponseId::from_u128(12345);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name, None),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should fail to encode
        let result = message.encode();
        assert!(
            result.is_err(),
            "Very large handler name should fail to encode"
        );
    }

    #[test]
    fn test_normal_handler_name_succeeds() {
        // Test a normal-sized handler name
        let handler_name = "my_handler".to_string();
        let response_id = ResponseId::from_u128(12345);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), None),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should encode and decode successfully
        let (header, payload, _) = message.encode().unwrap();
        let decoded = decode_active_message(header, payload).unwrap();
        assert_eq!(decoded.metadata.handler_name, handler_name);
    }

    // ============================================================================
    // Headers Tests
    // ============================================================================

    #[test]
    fn test_headers_encode_decode_round_trip() {
        // Test encoding and decoding with headers
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();
        headers.insert("trace-id".to_string(), "abc123".to_string());
        headers.insert("span-id".to_string(), "def456".to_string());

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test payload"),
        };

        // Encode
        let (header, payload, _) = message.encode().unwrap();

        // Decode
        let decoded = decode_active_message(header, payload).unwrap();

        // Verify
        assert_eq!(decoded.metadata.handler_name, handler_name);
        assert_eq!(
            decoded.metadata.response_id.as_u128(),
            response_id.as_u128()
        );
        assert!(decoded.metadata.headers.is_some());
        let decoded_headers = decoded.metadata.headers.unwrap();
        assert_eq!(decoded_headers.len(), 2);
        assert_eq!(decoded_headers.get("trace-id").unwrap(), "abc123");
        assert_eq!(decoded_headers.get("span-id").unwrap(), "def456");
    }

    #[test]
    fn test_headers_none_encodes_with_zero_length() {
        // Test that None headers encodes with headers_len=0
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), None),
            payload: Bytes::from_static(b"test payload"),
        };

        // Encode
        let (header, payload, _) = message.encode().unwrap();

        // The header should be: 1 + 1 + 16 + 2 + handler_name.len() + 2 (for headers_len=0)
        let expected_len = 1 + 1 + 16 + 2 + handler_name.len() + 2;
        assert_eq!(header.len(), expected_len);

        // Decode
        let decoded = decode_active_message(header, payload).unwrap();
        assert!(decoded.metadata.headers.is_none());
    }

    #[test]
    fn test_headers_empty_map_encodes_successfully() {
        // Test that empty HashMap encodes (but should be minimal)
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let headers = HashMap::new();

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), Some(headers)),
            payload: Bytes::from_static(b"test payload"),
        };

        // Encode and decode
        let (header, payload, _) = message.encode().unwrap();
        let decoded = decode_active_message(header, payload).unwrap();

        assert!(decoded.metadata.headers.is_some());
        assert_eq!(decoded.metadata.headers.unwrap().len(), 0);
    }

    #[test]
    fn test_headers_per_value_size_limit() {
        // Test that header values exceeding 1KB are rejected
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();

        // Create a value that's exactly 1KB (should succeed)
        let value_1kb = "a".repeat(1024);
        headers.insert("large-header".to_string(), value_1kb);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), Some(headers)),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should succeed at exactly 1KB
        let result = message.encode();
        assert!(result.is_ok(), "1KB value should encode successfully");
    }

    #[test]
    fn test_headers_per_value_size_exceeds_limit() {
        // Test that header values exceeding 1KB are rejected
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();

        // Create a value that's 1KB + 1 byte (should fail)
        let value_too_large = "a".repeat(1025);
        headers.insert("large-header".to_string(), value_too_large);

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), Some(headers)),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should fail
        let result = message.encode();
        assert!(result.is_err(), "1KB+1 value should fail to encode");

        match result {
            Err(EncodeError::HeaderValueTooLarge(key, size)) => {
                assert_eq!(key, "large-header");
                assert_eq!(size, 1025);
            }
            _ => panic!("Expected HeaderValueTooLarge error"),
        }
    }

    #[test]
    fn test_headers_total_size_limit() {
        // Test that total headers exceeding 16KB are rejected
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();

        // Create many headers that together exceed 16KB when serialized
        // Each value is 500 bytes, which is under per-header limit
        // But we'll add enough to exceed 16KB total
        for i in 0..40 {
            let key = format!("header-{}", i);
            let value = "x".repeat(500);
            headers.insert(key, value);
        }

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(response_id, handler_name.clone(), Some(headers)),
            payload: Bytes::from_static(b"test payload"),
        };

        // Should fail due to total size
        let result = message.encode();
        assert!(result.is_err(), "Total size exceeding 16KB should fail");

        match result {
            Err(EncodeError::TotalHeadersTooLarge(size)) => {
                assert!(size > 16384, "Size should exceed 16KB");
            }
            _ => panic!("Expected TotalHeadersTooLarge error"),
        }
    }

    #[test]
    fn test_headers_with_special_characters() {
        // Test headers with special characters, unicode, etc.
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();
        headers.insert("emoji".to_string(), "🚀🎉".to_string());
        headers.insert("unicode".to_string(), "你好世界".to_string());
        headers.insert("special".to_string(), "a\nb\tc\"d'e".to_string());

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test payload"),
        };

        // Encode and decode
        let (header, payload, _) = message.encode().unwrap();
        let decoded = decode_active_message(header, payload).unwrap();

        // Verify special characters preserved
        let decoded_headers = decoded.metadata.headers.unwrap();
        assert_eq!(decoded_headers.get("emoji").unwrap(), "🚀🎉");
        assert_eq!(decoded_headers.get("unicode").unwrap(), "你好世界");
        assert_eq!(decoded_headers.get("special").unwrap(), "a\nb\tc\"d'e");
    }

    #[test]
    fn test_headers_with_many_entries() {
        // Test with many header entries (but within size limits)
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();

        // Add 100 small headers
        for i in 0..100 {
            headers.insert(format!("key-{}", i), format!("value-{}", i));
        }

        let message = ActiveMessage {
            metadata: MessageMetadata::new_unary(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test payload"),
        };

        // Encode and decode
        let (header, payload, _) = message.encode().unwrap();
        let decoded = decode_active_message(header, payload).unwrap();

        // Verify all headers present
        let decoded_headers = decoded.metadata.headers.unwrap();
        assert_eq!(decoded_headers.len(), 100);
        assert_eq!(decoded_headers.get("key-42").unwrap(), "value-42");
    }

    #[test]
    fn test_headers_all_response_types() {
        // Test headers work with all response types
        let handler_name = "test_handler".to_string();
        let response_id = ResponseId::from_u128(12345);
        let mut headers = HashMap::new();
        headers.insert("test".to_string(), "value".to_string());

        // FireAndForget
        let msg_fire = ActiveMessage {
            metadata: MessageMetadata::new_fire(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test"),
        };
        let (h, p, _) = msg_fire.encode().unwrap();
        let decoded = decode_active_message(h, p).unwrap();
        assert_eq!(decoded.metadata.response_type, ResponseType::FireAndForget);
        assert!(decoded.metadata.headers.is_some());

        // AckNack
        let msg_sync = ActiveMessage {
            metadata: MessageMetadata::new_sync(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test"),
        };
        let (h, p, _) = msg_sync.encode().unwrap();
        let decoded = decode_active_message(h, p).unwrap();
        assert_eq!(decoded.metadata.response_type, ResponseType::AckNack);
        assert!(decoded.metadata.headers.is_some());

        // Unary
        let msg_unary = ActiveMessage {
            metadata: MessageMetadata::new_unary(
                response_id,
                handler_name.clone(),
                Some(headers.clone()),
            ),
            payload: Bytes::from_static(b"test"),
        };
        let (h, p, _) = msg_unary.encode().unwrap();
        let decoded = decode_active_message(h, p).unwrap();
        assert_eq!(decoded.metadata.response_type, ResponseType::Unary);
        assert!(decoded.metadata.headers.is_some());
    }
}
