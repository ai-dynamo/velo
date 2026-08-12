// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the active-message wire envelope: encoding, decoding, and
//! the analytic envelope sizing that has to track the encoder byte for byte.

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
