// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Wire encode/decode for the messenger mux, per `BATCHING.md` § "Frame
//! envelope" and § "Slots".
//!
//! Pure and synchronous: no I/O, no async, no allocation sized by an untrusted
//! wire field. A batch is the payload of one `_stream_batch` active message:
//!
//! ```text
//! [16 B batch header][record_count × record]
//!
//! batch header:
//!   [u8 mux_version = 1][u8 flags][u16 record_count][u64 peer_epoch][u32 batch_seq]
//!
//! record:
//!   [u8 record_type][u32 slot][u32 frame_seq][u32 len][len bytes body]
//! ```
//!
//! Every multi-byte field is big-endian, header and record alike.
//!
//! Decoding never panics. [`BatchDecoder`] reads exclusively through
//! `slice::get`, bounds every offset with checked arithmetic, and refuses to
//! size anything from `record_count` or `len` before the bytes behind them have
//! been proven present — a peer that declares 65 535 records over a 20-byte
//! payload gets one [`DecodeError`], not a loop and not an allocation.

use bytes::{BufMut, BytesMut};
#[cfg(test)]
use std::cmp::Ordering;
use std::fmt;
use std::iter::FusedIterator;
use std::ops::Range;

/// Mux wire version carried in every batch header.
///
/// Bumped only for a change that an older peer cannot skip past. Negotiation
/// (`messenger-mux-v1`, Stage F) already keeps unequal versions from meeting;
/// the field is the belt to that pair of braces.
pub(crate) const MUX_VERSION: u8 = 1;

/// Encoded size of a batch header.
pub(crate) const BATCH_HEADER_LEN: usize = 16;

/// Encoded size of a record header, body excluded.
///
/// 13 bytes: ~33 % overhead on a 40-byte token, up from the 9-byte layout a
/// dedicated connection could afford. The two sequences are what buy ordering
/// now that a private TCP connection is not providing it free. `BATCHING.md`
/// says it plainly — measure before shrinking them.
pub(crate) const RECORD_HEADER_LEN: usize = 13;

/// Records one batch can carry, the ceiling of the `u16` `record_count` field.
pub(crate) const MAX_RECORDS_PER_BATCH: u16 = u16::MAX;

/// Largest slot index representable in the `u24` half of a [`SlotId`].
pub(crate) const MAX_SLOT_INDEX: u32 = 0x00FF_FFFF;

// ---------------------------------------------------------------------------
// Primitive readers
// ---------------------------------------------------------------------------

/// Reads a big-endian `u8` at `at`, or `None` when it does not fit.
fn read_u8(src: &[u8], at: usize) -> Option<u8> {
    src.get(at).copied()
}

/// Reads a big-endian `u16` at `at`, or `None` when it does not fit.
fn read_u16(src: &[u8], at: usize) -> Option<u16> {
    let end = at.checked_add(2)?;
    let raw: [u8; 2] = src.get(at..end)?.try_into().ok()?;
    Some(u16::from_be_bytes(raw))
}

/// Reads a big-endian `u32` at `at`, or `None` when it does not fit.
fn read_u32(src: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let raw: [u8; 4] = src.get(at..end)?.try_into().ok()?;
    Some(u32::from_be_bytes(raw))
}

/// Reads a big-endian `u64` at `at`, or `None` when it does not fit.
fn read_u64(src: &[u8], at: usize) -> Option<u64> {
    let end = at.checked_add(8)?;
    let raw: [u8; 8] = src.get(at..end)?.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

// ---------------------------------------------------------------------------
// Batch header
// ---------------------------------------------------------------------------

/// The 16-byte header prefixing every `_stream_batch` payload.
///
/// `peer_epoch` is bumped whenever the sender's view of the peer is
/// re-established; `batch_seq` advances within it and is compared modulo (see
/// [`batch_seq_cmp`]), since an epoch outlives a `u32` on a busy pair. Together
/// they let ingress discard a stale epoch's batches by header inspection rather
/// than by draining them, and meter gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchHeader {
    /// Wire version; [`MUX_VERSION`] for anything this build produces.
    pub(crate) mux_version: u8,
    /// Reserved for future per-batch bits. Senders write `0`; receivers ignore
    /// unknown bits rather than failing the peer.
    pub(crate) flags: u8,
    /// Records that follow. Written by [`BatchEncoder::finish`], not at open.
    pub(crate) record_count: u16,
    /// The sending side's epoch for this peer.
    pub(crate) peer_epoch: u64,
    /// Batch counter within `peer_epoch`, compared modulo.
    pub(crate) batch_seq: u32,
}

impl BatchHeader {
    /// A header for the current wire version with no flags and no records yet.
    pub(crate) const fn new(peer_epoch: u64, batch_seq: u32) -> Self {
        Self {
            mux_version: MUX_VERSION,
            flags: 0,
            record_count: 0,
            peer_epoch,
            batch_seq,
        }
    }

    /// Whether this build can interpret the records behind this header.
    pub(crate) const fn is_supported(&self) -> bool {
        self.mux_version == MUX_VERSION
    }

    /// Appends the 16 header bytes to `out`.
    pub(crate) fn encode_into(&self, out: &mut BytesMut) {
        out.put_u8(self.mux_version);
        out.put_u8(self.flags);
        out.put_u16(self.record_count);
        out.put_u64(self.peer_epoch);
        out.put_u32(self.batch_seq);
    }

    /// Reads a header off the front of `src` without validating its version.
    ///
    /// This is the *metering* entry point: ingress needs `peer_epoch` and
    /// `batch_seq` in hand to count a stale-epoch or unknown-version drop, so
    /// version rejection lives in [`BatchDecoder::new`] instead — the point
    /// past which the record bytes actually get interpreted.
    pub(crate) fn decode(src: &[u8]) -> Result<Self, DecodeError> {
        let (Some(mux_version), Some(flags), Some(record_count), Some(peer_epoch), Some(batch_seq)) = (
            read_u8(src, 0),
            read_u8(src, 1),
            read_u16(src, 2),
            read_u64(src, 4),
            read_u32(src, 12),
        ) else {
            return Err(DecodeError::TruncatedBatchHeader { len: src.len() });
        };
        Ok(Self {
            mux_version,
            flags,
            record_count,
            peer_epoch,
            batch_seq,
        })
    }
}

// ---------------------------------------------------------------------------
// Slot identity
// ---------------------------------------------------------------------------

/// `(u24 index, u8 generation)` packed into a `u32`, scoped by the sender's
/// peer epoch.
///
/// The index is dense so ingress demux is a `Vec` lookup rather than a hash: at
/// 60 KiB batches that is roughly 1100 lookups per batch, squarely on the hot
/// path. The generation is a *correctness* requirement, not an optimization —
/// dense slot reuse without it delivers a stale record for a recycled index to
/// whatever stream now occupies it, surfacing request A's tokens inside request
/// B's response. The epoch scopes the table above the generation, which is what
/// keeps a `u8` generation ample.
///
/// Packing is index in the high 24 bits, generation in the low 8, so the raw
/// `u32` sorts by index.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SlotId(u32);

impl SlotId {
    /// Packs `index` and `generation`, or `None` when `index` overflows `u24`.
    pub(crate) const fn new(index: u32, generation: u8) -> Option<Self> {
        if index > MAX_SLOT_INDEX {
            return None;
        }
        Some(Self((index << 8) | generation as u32))
    }

    /// Wraps a wire-order `u32`. Every bit pattern is a valid `SlotId`; whether
    /// the index names a live slot is the registry's question, not this one's.
    pub(crate) const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }

    /// The packed `u32` as it travels on the wire.
    pub(crate) const fn raw(self) -> u32 {
        self.0
    }

    /// The dense slot index.
    pub(crate) const fn index(self) -> u32 {
        self.0 >> 8
    }

    /// The generation occupying that index.
    pub(crate) const fn generation(self) -> u8 {
        (self.0 & 0xFF) as u8
    }

    /// Same index, different generation.
    #[cfg(test)]
    pub(crate) const fn with_generation(self, generation: u8) -> Self {
        Self((self.0 & !0xFF) | generation as u32)
    }

    /// Same index, next generation — wrapping, because the epoch above it is
    /// what makes a reused generation unreachable rather than merely unlikely.
    #[cfg(test)]
    pub(crate) const fn next_generation(self) -> Self {
        self.with_generation(self.generation().wrapping_add(1))
    }
}

impl fmt::Debug for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotId")
            .field("index", &self.index())
            .field("generation", &self.generation())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Record taxonomy
// ---------------------------------------------------------------------------

/// The `record_type` discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum RecordType {
    /// An `rmp_serde`-encoded `StreamFrame`, byte-for-byte as today.
    Data = 0,
    /// Slot open: `[u64 anchor_id][u64 session_id]`.
    OpenSlot = 1,
    /// Slot close: `[u8 reason]`. Bidirectional.
    CloseSlot = 2,
    /// Credit grant: `[u32 delta]`, receiver to sender.
    CreditUpdate = 3,
    /// Per-slot liveness beat. No body.
    SlotHeartbeat = 4,
}

impl RecordType {
    /// The label value `velo_streaming_mux_records_sent_total` files this
    /// type under.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::OpenSlot => "open_slot",
            Self::CloseSlot => "close_slot",
            Self::CreditUpdate => "credit_update",
            Self::SlotHeartbeat => "slot_heartbeat",
        }
    }

    /// Decodes a discriminant, or `None` for a type this build does not know.
    pub(crate) const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Data),
            1 => Some(Self::OpenSlot),
            2 => Some(Self::CloseSlot),
            3 => Some(Self::CreditUpdate),
            4 => Some(Self::SlotHeartbeat),
            _ => None,
        }
    }

    /// The wire discriminant.
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Whether this type rides the reserved control capacity.
    ///
    /// Exactly `OpenSlot`, `CloseSlot` and `CreditUpdate` — the list
    /// `BATCHING.md` § "Flow control" gives, and `SlotHeartbeat` is
    /// deliberately not on it. A heartbeat dropped under saturation *is* the
    /// per-slot saturation signal `reader_pump`'s `DETECTION_MULTIPLIER`
    /// watches for; granting it a reserve would delete the watchdog kill that
    /// `SATURATION.md` documents.
    #[cfg(test)]
    pub(crate) const fn is_control(self) -> bool {
        matches!(self, Self::OpenSlot | Self::CloseSlot | Self::CreditUpdate)
    }
}

/// Why a slot closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum CloseReason {
    /// A terminal sentinel was sent for this slot; the close is its atomic
    /// same-batch companion. The only reason that does *not* inject `Dropped`.
    TerminalSent = 0,
    /// The peer or its epoch died under a live slot.
    PeerGone = 1,
    /// Records arrived for an `(anchor_id, session_id)` that was never
    /// registered. The receiver rejects the slot without failing the peer.
    UnknownSlot = 2,
    /// The slot violated the protocol — a credit overspend, or a hold buffer
    /// overrun. Scoped to this slot; other slots are untouched.
    ProtocolError = 3,
}

impl CloseReason {
    /// Decodes a reason byte, or `None` for one this build does not know.
    pub(crate) const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::TerminalSent),
            1 => Some(Self::PeerGone),
            2 => Some(Self::UnknownSlot),
            3 => Some(Self::ProtocolError),
            _ => None,
        }
    }

    /// The wire discriminant.
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A decoded record body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordBody<'a> {
    /// Opaque `StreamFrame` bytes. `is_terminal_sentinel` works on these
    /// unchanged, which is why terminal handling gains no new code path.
    Data(&'a [u8]),
    /// The existing 16-byte attach handshake, relocated into a record.
    OpenSlot { anchor_id: u64, session_id: u64 },
    /// Slot teardown.
    CloseSlot { reason: CloseReason },
    /// Additional data credit for the slot.
    CreditUpdate { delta: u32 },
    /// Liveness only.
    SlotHeartbeat,
}

impl RecordBody<'_> {
    /// The discriminant this body encodes as.
    #[cfg(test)]
    pub(crate) const fn record_type(&self) -> RecordType {
        match self {
            Self::Data(_) => RecordType::Data,
            Self::OpenSlot { .. } => RecordType::OpenSlot,
            Self::CloseSlot { .. } => RecordType::CloseSlot,
            Self::CreditUpdate { .. } => RecordType::CreditUpdate,
            Self::SlotHeartbeat => RecordType::SlotHeartbeat,
        }
    }
}

/// One decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record<'a> {
    /// Which slot, and which generation of it.
    pub(crate) slot: SlotId,
    /// Per-slot sequence — the authority on stream order. Transport and lane
    /// ordering are a fast path, not the proof.
    pub(crate) frame_seq: u32,
    /// The typed body.
    pub(crate) body: RecordBody<'a>,
    /// Where the body sits inside the whole batch payload, header included.
    ///
    /// A caller holding the payload as `Bytes` can `slice(range)` for an
    /// owner-shared body instead of copying out of [`RecordBody::Data`].
    pub(crate) body_range: Range<usize>,
}

impl Record<'_> {
    /// The discriminant this record decoded from.
    #[cfg(test)]
    pub(crate) const fn record_type(&self) -> RecordType {
        self.body.record_type()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a record could not be appended to a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EncodeError {
    /// The batch already holds `u16::MAX` records. The caller cuts a batch.
    #[error("batch is full at {MAX_RECORDS_PER_BATCH} records")]
    BatchFull,
    /// The body does not fit the `u32` length field.
    #[error("record body of {len} bytes exceeds the u32 length field")]
    BodyTooLarge { len: usize },
}

/// Why a batch could not be decoded.
///
/// Every variant carries the offset it was detected at, so ingress can meter a
/// malformed peer precisely instead of logging "bad batch".
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DecodeError {
    /// Fewer than [`BATCH_HEADER_LEN`] bytes arrived.
    #[error("batch payload of {len} bytes is shorter than the {BATCH_HEADER_LEN}-byte header")]
    TruncatedBatchHeader { len: usize },
    /// The header names a wire version this build cannot interpret.
    #[error("unsupported mux wire version {version}, expected {MUX_VERSION}")]
    UnsupportedVersion { version: u8 },
    /// The payload ended cleanly, but short of `record_count` records.
    #[error("batch declared {declared} records but ended after {decoded}")]
    RecordCountMismatch { declared: u16, decoded: u16 },
    /// A record header was cut mid-field.
    #[error("record header at offset {offset} needs {RECORD_HEADER_LEN} bytes, {remaining} remain")]
    TruncatedRecordHeader { offset: usize, remaining: usize },
    /// A record header declared more body than the payload holds.
    #[error("record at offset {offset} declared a {declared_len}-byte body, {remaining} remain")]
    TruncatedRecordBody {
        offset: usize,
        declared_len: u32,
        remaining: usize,
    },
    /// A `record_type` byte this build does not know.
    #[error("unknown record type {value} at offset {offset}")]
    UnknownRecordType { offset: usize, value: u8 },
    /// A fixed-layout body arrived at the wrong length.
    #[error(
        "record type {record_type:?} at offset {offset} requires a {expected}-byte body, got {actual}"
    )]
    BodyLengthMismatch {
        offset: usize,
        record_type: RecordType,
        expected: usize,
        actual: u32,
    },
    /// A `CloseSlot` reason byte this build does not know.
    #[error("unknown close reason {value} at offset {offset}")]
    UnknownCloseReason { offset: usize, value: u8 },
    /// `record_count` records were decoded and bytes were still left over.
    #[error("{remaining} trailing bytes after the declared records, at offset {offset}")]
    TrailingBytes { offset: usize, remaining: usize },
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encoded size of a record carrying `body_len` bytes, or `None` when that
/// overflows `usize`.
pub(crate) const fn record_encoded_len(body_len: usize) -> Option<usize> {
    body_len.checked_add(RECORD_HEADER_LEN)
}

/// Appends records into one batch buffer, finalizing `record_count` at the end.
///
/// The header is written at open with `record_count = 0` and patched by
/// [`finish`](Self::finish), so the count never has to be known in advance —
/// which is what lets the batcher pack until a cap, a hint or a starvation
/// signal cuts the batch.
pub(crate) struct BatchEncoder {
    buf: BytesMut,
    record_count: u16,
}

impl BatchEncoder {
    /// Opens a batch in a fresh buffer.
    pub(crate) fn new(peer_epoch: u64, batch_seq: u32) -> Self {
        Self::with_buffer(BytesMut::new(), peer_epoch, batch_seq)
    }

    /// Opens a batch in a caller-supplied buffer, clearing it first.
    ///
    /// The batcher hands back its staging buffer this way so a steady-state
    /// flush allocates nothing.
    pub(crate) fn with_buffer(mut buf: BytesMut, peer_epoch: u64, batch_seq: u32) -> Self {
        buf.clear();
        BatchHeader::new(peer_epoch, batch_seq).encode_into(&mut buf);
        Self {
            buf,
            record_count: 0,
        }
    }

    /// Records appended so far.
    pub(crate) const fn record_count(&self) -> u16 {
        self.record_count
    }

    /// Whether the batch is worth sending.
    pub(crate) const fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Bytes written so far, header included.
    pub(crate) fn encoded_len(&self) -> usize {
        self.buf.len()
    }

    /// Appends a `Data` record carrying opaque `StreamFrame` bytes.
    pub(crate) fn push_data(
        &mut self,
        slot: SlotId,
        frame_seq: u32,
        body: &[u8],
    ) -> Result<(), EncodeError> {
        self.push(RecordType::Data, slot, frame_seq, body.len(), |buf| {
            buf.put_slice(body);
        })
    }

    /// Appends an `OpenSlot` record.
    pub(crate) fn push_open_slot(
        &mut self,
        slot: SlotId,
        frame_seq: u32,
        anchor_id: u64,
        session_id: u64,
    ) -> Result<(), EncodeError> {
        self.push(RecordType::OpenSlot, slot, frame_seq, 16, |buf| {
            buf.put_u64(anchor_id);
            buf.put_u64(session_id);
        })
    }

    /// Appends a `CloseSlot` record.
    pub(crate) fn push_close_slot(
        &mut self,
        slot: SlotId,
        frame_seq: u32,
        reason: CloseReason,
    ) -> Result<(), EncodeError> {
        self.push(RecordType::CloseSlot, slot, frame_seq, 1, |buf| {
            buf.put_u8(reason.as_u8());
        })
    }

    /// Appends a `CreditUpdate` record.
    pub(crate) fn push_credit_update(
        &mut self,
        slot: SlotId,
        frame_seq: u32,
        delta: u32,
    ) -> Result<(), EncodeError> {
        self.push(RecordType::CreditUpdate, slot, frame_seq, 4, |buf| {
            buf.put_u32(delta);
        })
    }

    /// Appends a `SlotHeartbeat` record.
    #[cfg(test)]
    pub(crate) fn push_heartbeat(
        &mut self,
        slot: SlotId,
        frame_seq: u32,
    ) -> Result<(), EncodeError> {
        self.push(RecordType::SlotHeartbeat, slot, frame_seq, 0, |_| {})
    }

    /// Writes `record_count` into the reserved header field and yields the
    /// finished batch.
    pub(crate) fn finish(mut self) -> BytesMut {
        if let Some(field) = self.buf.get_mut(2..4) {
            field.copy_from_slice(&self.record_count.to_be_bytes());
        }
        self.buf
    }

    /// Shared record-header write. `body_len` is validated before a single byte
    /// is appended, so a rejected push leaves the batch exactly as it was.
    fn push<F>(
        &mut self,
        record_type: RecordType,
        slot: SlotId,
        frame_seq: u32,
        body_len: usize,
        write_body: F,
    ) -> Result<(), EncodeError>
    where
        F: FnOnce(&mut BytesMut),
    {
        if self.record_count == MAX_RECORDS_PER_BATCH {
            return Err(EncodeError::BatchFull);
        }
        let len =
            u32::try_from(body_len).map_err(|_| EncodeError::BodyTooLarge { len: body_len })?;
        self.buf.put_u8(record_type.as_u8());
        self.buf.put_u32(slot.raw());
        self.buf.put_u32(frame_seq);
        self.buf.put_u32(len);
        write_body(&mut self.buf);
        self.record_count += 1;
        Ok(())
    }
}

impl fmt::Debug for BatchEncoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchEncoder")
            .field("record_count", &self.record_count)
            .field("encoded_len", &self.buf.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Iterator over the records of one batch payload.
///
/// Fused on the first error: a malformed batch yields exactly one `Err` and
/// then stops, so a caller draining the iterator cannot spin on a bad byte.
#[derive(Debug)]
pub(crate) struct BatchDecoder<'a> {
    payload: &'a [u8],
    header: BatchHeader,
    offset: usize,
    decoded: u16,
    done: bool,
}

impl<'a> BatchDecoder<'a> {
    /// Reads the header and rejects a version this build cannot interpret.
    ///
    /// Use [`BatchHeader::decode`] instead when the header is wanted for
    /// metering regardless of whether the records will be read.
    pub(crate) fn new(payload: &'a [u8]) -> Result<Self, DecodeError> {
        let header = BatchHeader::decode(payload)?;
        if !header.is_supported() {
            return Err(DecodeError::UnsupportedVersion {
                version: header.mux_version,
            });
        }
        Ok(Self {
            payload,
            header,
            offset: BATCH_HEADER_LEN,
            decoded: 0,
            done: false,
        })
    }

    /// The batch header.
    #[cfg(test)]
    pub(crate) const fn header(&self) -> BatchHeader {
        self.header
    }

    /// Records yielded so far.
    #[cfg(test)]
    pub(crate) const fn decoded(&self) -> u16 {
        self.decoded
    }

    /// Decodes one record at `self.offset`, leaving `self` unchanged on error.
    fn next_record(&mut self) -> Result<Record<'a>, DecodeError> {
        let offset = self.offset;
        let remaining = self.payload.len().saturating_sub(offset);

        let (Some(type_byte), Some(slot), Some(frame_seq), Some(len)) = (
            read_u8(self.payload, offset),
            read_u32(self.payload, offset + 1),
            read_u32(self.payload, offset + 5),
            read_u32(self.payload, offset + 9),
        ) else {
            return Err(if remaining == 0 {
                DecodeError::RecordCountMismatch {
                    declared: self.header.record_count,
                    decoded: self.decoded,
                }
            } else {
                DecodeError::TruncatedRecordHeader { offset, remaining }
            });
        };

        let record_type = RecordType::from_u8(type_byte).ok_or(DecodeError::UnknownRecordType {
            offset,
            value: type_byte,
        })?;

        let body_start = offset + RECORD_HEADER_LEN;
        let body_len = usize::try_from(len).unwrap_or(usize::MAX);
        let body = body_start
            .checked_add(body_len)
            .and_then(|end| self.payload.get(body_start..end))
            .ok_or(DecodeError::TruncatedRecordBody {
                offset,
                declared_len: len,
                remaining: self.payload.len().saturating_sub(body_start),
            })?;

        let decoded_body = decode_body(record_type, body, offset, len)?;
        self.offset = body_start + body.len();
        self.decoded += 1;
        Ok(Record {
            slot: SlotId::from_raw(slot),
            frame_seq,
            body: decoded_body,
            body_range: body_start..body_start + body.len(),
        })
    }
}

/// Interprets a body whose bytes are already proven present.
///
/// `declared_len` equals `body.len()` and is threaded through only so the error
/// reports the wire field rather than a re-derived cast.
///
/// Each arm reads its fields first — so a *short* body falls out of the `else` —
/// then rejects a *long* one explicitly. Neither path can index out of bounds
/// and neither leaves an unreachable branch behind.
fn decode_body(
    record_type: RecordType,
    body: &[u8],
    offset: usize,
    declared_len: u32,
) -> Result<RecordBody<'_>, DecodeError> {
    let mismatch = |expected: usize| DecodeError::BodyLengthMismatch {
        offset,
        record_type,
        expected,
        actual: declared_len,
    };
    match record_type {
        RecordType::Data => Ok(RecordBody::Data(body)),
        RecordType::OpenSlot => {
            let (Some(anchor_id), Some(session_id)) = (read_u64(body, 0), read_u64(body, 8)) else {
                return Err(mismatch(16));
            };
            if body.len() != 16 {
                return Err(mismatch(16));
            }
            Ok(RecordBody::OpenSlot {
                anchor_id,
                session_id,
            })
        }
        RecordType::CloseSlot => {
            let Some(byte) = read_u8(body, 0) else {
                return Err(mismatch(1));
            };
            if body.len() != 1 {
                return Err(mismatch(1));
            }
            let reason = CloseReason::from_u8(byte).ok_or(DecodeError::UnknownCloseReason {
                offset,
                value: byte,
            })?;
            Ok(RecordBody::CloseSlot { reason })
        }
        RecordType::CreditUpdate => {
            let Some(delta) = read_u32(body, 0) else {
                return Err(mismatch(4));
            };
            if body.len() != 4 {
                return Err(mismatch(4));
            }
            Ok(RecordBody::CreditUpdate { delta })
        }
        RecordType::SlotHeartbeat => {
            if !body.is_empty() {
                return Err(mismatch(0));
            }
            Ok(RecordBody::SlotHeartbeat)
        }
    }
}

impl<'a> Iterator for BatchDecoder<'a> {
    type Item = Result<Record<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if self.decoded == self.header.record_count {
            self.done = true;
            let remaining = self.payload.len().saturating_sub(self.offset);
            if remaining > 0 {
                return Some(Err(DecodeError::TrailingBytes {
                    offset: self.offset,
                    remaining,
                }));
            }
            return None;
        }
        match self.next_record() {
            Ok(record) => Some(Ok(record)),
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.done {
            return (0, Some(0));
        }
        // Upper bound only: `record_count` is an untrusted field, so it may
        // never size an allocation. `Iterator::collect` reads the lower bound.
        (
            0,
            Some(usize::from(self.header.record_count - self.decoded)),
        )
    }
}

impl FusedIterator for BatchDecoder<'_> {}

// ---------------------------------------------------------------------------
// Sequence arithmetic
// ---------------------------------------------------------------------------

/// Compares two `batch_seq` values modulo `2^32`, RFC 1982 style.
///
/// `a` is `Greater` than `b` when `a - b` lands in the first half of the ring,
/// which makes `0` correctly newer than `u32::MAX`. An epoch outlives a `u32`
/// on a busy pair, so plain `<` would declare every post-wrap batch stale.
///
/// The antipode is deliberately left inconsistent, exactly as RFC 1982 leaves
/// it: at a distance of exactly `2^31` both orderings report `Less`, so each
/// value reads as stale from the other's vantage. Nothing sane can be said
/// about two sequences two billion batches apart, and the alternative is a
/// silent, arbitrary tie-break.
#[cfg(test)]
pub(crate) fn batch_seq_cmp(a: u32, b: u32) -> Ordering {
    (a.wrapping_sub(b) as i32).cmp(&0)
}

/// Whether `candidate` is newer than `last_seen` under [`batch_seq_cmp`].
#[cfg(test)]
pub(crate) fn batch_seq_is_newer(candidate: u32, last_seen: u32) -> bool {
    batch_seq_cmp(candidate, last_seen) == Ordering::Greater
}

/// Batches skipped between the expected next sequence and the one that
/// arrived: `0` when `received` is exactly what was expected.
///
/// Feeds the gap meter, not a decision — the mux does not retransmit, so a gap
/// is reported and moved past.
pub(crate) fn batch_seq_gap(expected: u32, received: u32) -> u32 {
    received.wrapping_sub(expected)
}

#[cfg(test)]
mod tests;
