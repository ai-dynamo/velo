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
mod tests;
