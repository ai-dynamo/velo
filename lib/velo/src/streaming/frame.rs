// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! [`StreamFrame<T>`]: the seven-variant wire frame enum for the streaming protocol.
//!
//! Also the hand-written codec for `Item` frames of a `Bytes` stream, which
//! skips serde on both ends. See `encode_bytes_item` and `decode_bytes_frame`.

use bytes::{Buf, Bytes};
use serde::{Deserialize, Serialize};

use crate::streaming::sender::ITEM_PREFIX;

/// A single frame traveling over the streaming wire.
///
/// `T` is the user-defined item payload type. It must implement `Serialize` and
/// `Deserialize` at call sites. `String` is used as the error carrier in
/// [`SenderError`](StreamFrame::SenderError) and
/// [`TransportError`](StreamFrame::TransportError) to avoid requiring
/// `T: std::error::Error` and to ensure msgpack compatibility.
///
/// Consumers MUST NOT be exposed to [`StreamFrame::Heartbeat`] — the API layer
/// filters heartbeat frames before surfacing items to user code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamFrame<T> {
    /// A data item carrying the user payload `T`.
    Item(T),
    /// A soft error reported by the sender. Does not terminate the stream;
    /// the sender may continue sending `Item` frames afterwards.
    SenderError(String),
    /// The sender was dropped without an explicit detach or finalize.
    /// This is the last frame the anchor receives from a sender that exited abruptly.
    Dropped,
    /// The sender explicitly detached; a new sender may attach to the same anchor.
    Detached,
    /// The stream is permanently closed; no further items will arrive.
    Finalized,
    /// Internal liveness ping. Never exposed to the user API layer.
    Heartbeat,
    /// The transport layer encountered an unrecoverable error.
    TransportError(String),
}

// ---------------------------------------------------------------------------
// Error types for the consumer (StreamAnchor) and producer (StreamSender) APIs
// ---------------------------------------------------------------------------

/// Errors surfaced to [`StreamAnchor`](crate::streaming::anchor::StreamAnchor) consumers.
///
/// These represent problems that occurred on the sender side or during
/// transport/deserialization. The stream may or may not continue producing
/// items after a `StreamError`, depending on the variant.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StreamError {
    /// The sender reported a soft error via [`StreamFrame::SenderError`].
    #[error("sender error: {0}")]
    SenderError(String),
    /// The sender was dropped without an explicit detach or finalize.
    #[error("sender dropped without explicit detach/finalize")]
    SenderDropped,
    /// The transport layer encountered an unrecoverable error.
    #[error("transport error: {0}")]
    TransportError(String),
    /// A received frame could not be deserialized into `StreamFrame<T>`.
    #[error("deserialization error: {0}")]
    DeserializationError(String),
}

/// Errors surfaced to [`crate::streaming::sender::StreamSender`] callers when a send operation fails.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// The underlying channel is closed (receiver dropped or anchor removed).
    #[error("channel closed")]
    ChannelClosed,
    /// The item could not be serialized into the wire format.
    #[error("serialization error: {0}")]
    SerializationError(String),
    /// The encoded item is larger than the stream's transport carries in one
    /// frame (per-stream gRPC: 16 MiB). Nothing was sent; the stream stays
    /// open.
    #[error("item of {len} bytes is over the transport's limit of {limit} bytes")]
    ItemTooLarge { len: usize, limit: usize },
}

// ---------------------------------------------------------------------------
// Byte items without serde
// ---------------------------------------------------------------------------

/// msgpack `bin` markers, named by the width of the length after them.
const BIN8: u8 = 0xc4;
const BIN16: u8 = 0xc5;
const BIN32: u8 = 0xc6;

/// The msgpack `bin` header for `len` bytes in its shortest form, which is the
/// form `rmp_serde` writes. Returns the buffer and how much of it is used.
fn bin_header(len: u32) -> ([u8; 5], usize) {
    let mut header = [0; 5];
    let used = if let Ok(len) = u8::try_from(len) {
        header[..2].copy_from_slice(&[BIN8, len]);
        2
    } else if let Ok(len) = u16::try_from(len) {
        header[0] = BIN16;
        header[1..3].copy_from_slice(&len.to_be_bytes());
        3
    } else {
        header[0] = BIN32;
        header[1..5].copy_from_slice(&len.to_be_bytes());
        5
    };
    (header, used)
}

/// Encode `payload` as a `StreamFrame::Item` of a `Bytes` stream, without serde.
///
/// The bytes are exactly what `rmp_serde` writes for
/// `StreamFrame::Item(Bytes::copy_from_slice(payload))`, so a typed consumer
/// reads them unchanged. The frame is one allocation of the exact size and one
/// copy of the payload. `rmp_serde` grows its buffer as it writes, and the
/// exact size also lets the consumer take the buffer as `Bytes` without
/// allocating (see [`decode_bytes_frame`]).
///
/// # Errors
///
/// [`SendError::SerializationError`] for a payload over `u32::MAX` bytes, the
/// limit of a msgpack `bin`. `rmp_serde` would cut the length to 32 bits and
/// write a header that does not match the payload.
pub(crate) fn encode_bytes_item(payload: &[u8]) -> Result<Vec<u8>, SendError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        SendError::SerializationError(format!(
            "a byte item of {} bytes is over the msgpack bin limit of {} bytes",
            payload.len(),
            u32::MAX
        ))
    })?;
    let (header, used) = bin_header(len);
    let mut frame = Vec::with_capacity(ITEM_PREFIX.len() + used + payload.len());
    frame.extend_from_slice(ITEM_PREFIX);
    frame.extend_from_slice(&header[..used]);
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Decode one frame of a `Bytes` stream, keeping an item's payload in place.
///
/// An `Item` that holds one msgpack `bin` running to the end of the frame
/// becomes a view into `frame`: the payload is not copied. Serde cannot do
/// this, because `Bytes`' `Deserialize` copies what it is given into a new
/// buffer. Taking the `Vec` as `Bytes` allocates nothing when its length is
/// its capacity, as the mux's records and [`encode_bytes_item`]'s frames are.
///
/// Every other frame goes through `rmp_serde` as `StreamFrame<Bytes>`: the
/// sentinels, an item in another msgpack form (an array of integers, a string),
/// and a malformed frame. So this returns what `rmp_serde` returns for any
/// input, and a consumer may switch between the two decodes freely.
pub(crate) fn decode_bytes_frame(
    frame: Vec<u8>,
) -> Result<StreamFrame<Bytes>, rmp_serde::decode::Error> {
    let Some(start) = bin_item_start(&frame) else {
        return rmp_serde::from_slice(&frame);
    };
    let mut item = Bytes::from(frame);
    // `advance`, not `slice`: `slice` clones, and the first clone of a `Bytes`
    // made from a `Vec` allocates its shared header.
    item.advance(start);
    Ok(StreamFrame::Item(item))
}

/// Where the payload starts, when `frame` is an `Item` holding one msgpack
/// `bin` that ends where the frame ends.
///
/// `None` for anything else, including a `bin` with bytes after it, which
/// `rmp_serde` decodes and ignores the rest of. Leaving that to `rmp_serde`
/// keeps the two decodes equal on every input.
fn bin_item_start(frame: &[u8]) -> Option<usize> {
    let rest = frame.strip_prefix(ITEM_PREFIX)?;
    let (len, header) = match *rest.first()? {
        BIN8 => (usize::from(*rest.get(1)?), 2),
        BIN16 => {
            let len = u16::from_be_bytes(rest.get(1..3)?.try_into().ok()?);
            (usize::from(len), 3)
        }
        BIN32 => {
            let len = u32::from_be_bytes(rest.get(1..5)?.try_into().ok()?);
            (usize::try_from(len).ok()?, 5)
        }
        _ => return None,
    };
    // The header was read, so `rest` is at least `header` long.
    (rest.len() - header == len).then_some(ITEM_PREFIX.len() + header)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lengths on each side of every `bin` width, plus the large and empty
    /// payloads a response stream sends.
    const BYTE_ITEM_LENGTHS: &[usize] = &[0, 1, 255, 256, 65_535, 65_536, 1 << 20];

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// The hand-written frame is the frame `rmp_serde` writes for the same
    /// item as `Bytes`. This is what lets a `send_bytes` producer feed a typed
    /// consumer, and a typed producer feed a `next_bytes` consumer: there is
    /// one wire form, not two.
    #[test]
    fn byte_items_encode_as_rmp_serde_does() {
        for &len in BYTE_ITEM_LENGTHS {
            let payload = payload(len);
            let raw = encode_bytes_item(&payload).unwrap();
            let typed =
                rmp_serde::to_vec(&StreamFrame::Item(Bytes::from(payload.clone()))).unwrap();
            assert_eq!(raw, typed, "len {len}");
            // The buffer is sized exactly, which `decode_bytes_frame` relies
            // on to take it as `Bytes` without allocating.
            assert_eq!(raw.len(), raw.capacity(), "len {len}");
        }
    }

    /// Every frame decodes to what `rmp_serde` decodes it to: items of every
    /// `bin` width, every sentinel, an item in another msgpack form, a `bin`
    /// with bytes after it, and frames that do not decode at all.
    #[test]
    fn byte_frames_decode_as_rmp_serde_does() {
        let enc = |frame: &StreamFrame<Bytes>| rmp_serde::to_vec(frame).unwrap();
        let mut frames: Vec<Vec<u8>> = BYTE_ITEM_LENGTHS
            .iter()
            .map(|&len| encode_bytes_item(&payload(len)).unwrap())
            .collect();
        frames.extend([
            enc(&StreamFrame::SenderError("soft".to_string())),
            enc(&StreamFrame::Dropped),
            enc(&StreamFrame::Detached),
            enc(&StreamFrame::Finalized),
            enc(&StreamFrame::Heartbeat),
            enc(&StreamFrame::TransportError("reset".to_string())),
            // An item a typed producer of `Vec<u8>` writes: an array of integers.
            rmp_serde::to_vec(&StreamFrame::Item(vec![1u8, 2, 3])).unwrap(),
            // A `bin` with a byte after it, and one cut short.
            [encode_bytes_item(b"abc").unwrap(), vec![0xc0]].concat(),
            encode_bytes_item(b"abc").unwrap()[..9].to_vec(),
            ITEM_PREFIX.to_vec(),
            Vec::new(),
        ]);
        for frame in frames {
            let raw = decode_bytes_frame(frame.clone());
            let typed = rmp_serde::from_slice::<StreamFrame<Bytes>>(&frame);
            assert_eq!(format!("{raw:?}"), format!("{typed:?}"), "{frame:02x?}");
        }
    }

    /// A decoded item is a view into the frame it arrived in, not a copy, and
    /// taking it costs no allocation at all.
    ///
    /// This is the whole point of the byte path: `Bytes`' `Deserialize` copies
    /// every payload into a fresh buffer. The pointer check proves no copy;
    /// the allocation count proves the `Bytes` header was not allocated
    /// either, which `slice` would have done.
    #[test]
    fn a_decoded_byte_item_borrows_the_frame_buffer() {
        for &len in BYTE_ITEM_LENGTHS {
            let frame = encode_bytes_item(&payload(len)).unwrap();
            let range = frame.as_ptr_range();
            let (decoded, allocations) =
                crate::test_alloc::allocations_in(|| decode_bytes_frame(frame));
            let Ok(StreamFrame::Item(item)) = decoded else {
                panic!("len {len}: expected an item, got {decoded:?}");
            };
            assert_eq!(allocations, 0, "len {len}");
            assert_eq!(item.len(), len);
            assert_eq!(item.as_ptr_range().end, range.end, "len {len}");
            assert!(range.contains(&item.as_ptr()) || len == 0, "len {len}");
            assert_eq!(item, payload(len));
        }
    }

    #[test]
    fn test_stream_frame_variants() {
        // Verify all seven variants compile and serialize/deserialize via rmp-serde
        let frames: Vec<StreamFrame<u32>> = vec![
            StreamFrame::Item(42u32),
            StreamFrame::SenderError("soft error".to_string()),
            StreamFrame::Dropped,
            StreamFrame::Detached,
            StreamFrame::Finalized,
            StreamFrame::Heartbeat,
            StreamFrame::TransportError("connection reset".to_string()),
        ];

        for frame in &frames {
            let encoded = rmp_serde::to_vec(frame).expect("serialize StreamFrame");
            let decoded: StreamFrame<u32> =
                rmp_serde::from_slice(&encoded).expect("deserialize StreamFrame");
            // Verify the variant survived the round-trip
            match (frame, &decoded) {
                (StreamFrame::Item(a), StreamFrame::Item(b)) => assert_eq!(a, b),
                (StreamFrame::SenderError(a), StreamFrame::SenderError(b)) => assert_eq!(a, b),
                (StreamFrame::Dropped, StreamFrame::Dropped) => {}
                (StreamFrame::Detached, StreamFrame::Detached) => {}
                (StreamFrame::Finalized, StreamFrame::Finalized) => {}
                (StreamFrame::Heartbeat, StreamFrame::Heartbeat) => {}
                (StreamFrame::TransportError(a), StreamFrame::TransportError(b)) => {
                    assert_eq!(a, b)
                }
                _ => panic!("variant mismatch after round-trip"),
            }
        }
    }

    #[test]
    fn test_stream_frame_item_direct_payload() {
        // Item(T) contains T directly, not Result<T, String>
        let frame = StreamFrame::Item(42u32);
        let encoded = rmp_serde::to_vec(&frame).expect("serialize");
        let decoded: StreamFrame<u32> = rmp_serde::from_slice(&encoded).expect("deserialize");
        match decoded {
            StreamFrame::Item(val) => assert_eq!(val, 42u32),
            other => panic!("expected Item(42), got {:?}", other),
        }
    }

    #[test]
    fn test_stream_frame_sender_error_round_trip() {
        let frame = StreamFrame::<u32>::SenderError("msg".to_string());
        let encoded = rmp_serde::to_vec(&frame).expect("serialize");
        let decoded: StreamFrame<u32> = rmp_serde::from_slice(&encoded).expect("deserialize");
        match decoded {
            StreamFrame::SenderError(msg) => assert_eq!(msg, "msg"),
            other => panic!("expected SenderError, got {:?}", other),
        }
    }

    #[test]
    fn test_stream_error_display() {
        assert_eq!(
            StreamError::SenderError("msg".to_string()).to_string(),
            "sender error: msg"
        );
        assert_eq!(
            StreamError::SenderDropped.to_string(),
            "sender dropped without explicit detach/finalize"
        );
        assert_eq!(
            StreamError::TransportError("msg".to_string()).to_string(),
            "transport error: msg"
        );
        assert_eq!(
            StreamError::DeserializationError("msg".to_string()).to_string(),
            "deserialization error: msg"
        );
    }

    #[test]
    fn test_send_error_display() {
        assert_eq!(SendError::ChannelClosed.to_string(), "channel closed");
        assert_eq!(
            SendError::SerializationError("msg".to_string()).to_string(),
            "serialization error: msg"
        );
    }
}
