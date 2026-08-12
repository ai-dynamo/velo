// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Wire tests for the mux protocol.
//!
//! The golden-byte tests are the load-bearing ones. A roundtrip suite passes
//! happily with `record_count` and `batch_seq` swapped, or with every field
//! little-endian — it only proves the encoder and decoder agree with each
//! other, which they would even if both were wrong. E2's peer has to interop
//! with `BATCHING.md`, so the layout is pinned to literal byte arrays here.

use super::*;

/// A slot with a distinctive index and generation in every nibble.
fn slot() -> SlotId {
    SlotId::new(0x00AB_CDEF, 0x42).expect("index fits u24")
}

/// Encodes one batch from a closure, for the roundtrip tests.
fn encode_batch<F>(peer_epoch: u64, batch_seq: u32, fill: F) -> BytesMut
where
    F: FnOnce(&mut BatchEncoder),
{
    let mut encoder = BatchEncoder::new(peer_epoch, batch_seq);
    fill(&mut encoder);
    encoder.finish()
}

/// Decodes a whole batch, failing the test on the first error.
fn decode_all(payload: &[u8]) -> (BatchHeader, Vec<Record<'_>>) {
    let decoder = BatchDecoder::new(payload).expect("header decodes");
    let header = decoder.header();
    let records = decoder
        .collect::<Result<Vec<_>, _>>()
        .expect("records decode");
    (header, records)
}

/// Drains a decoder and returns the first error, if any.
fn first_error(payload: &[u8]) -> Option<DecodeError> {
    let decoder = match BatchDecoder::new(payload) {
        Ok(decoder) => decoder,
        Err(err) => return Some(err),
    };
    decoder.filter_map(Result::err).next()
}

// ---------------------------------------------------------------------------
// Golden bytes
// ---------------------------------------------------------------------------

#[test]
fn batch_header_encodes_to_exact_bytes() {
    let header = BatchHeader {
        mux_version: 1,
        flags: 0,
        record_count: 2,
        peer_epoch: 0x0102_0304_0506_0708,
        batch_seq: 0xDEAD_BEEF,
    };
    let mut buf = BytesMut::new();
    header.encode_into(&mut buf);

    assert_eq!(
        buf.as_ref(),
        &[
            0x01, // mux_version
            0x00, // flags
            0x00, 0x02, // record_count, big-endian
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // peer_epoch
            0xDE, 0xAD, 0xBE, 0xEF, // batch_seq
        ]
    );
    assert_eq!(buf.len(), BATCH_HEADER_LEN);
}

#[test]
fn data_record_encodes_to_exact_bytes() {
    let batch = encode_batch(0, 0, |encoder| {
        encoder
            .push_data(slot(), 0x0000_0007, &[0xAA, 0xBB, 0xCC])
            .expect("push");
    });

    // Header, then `[u8 type][u32 slot][u32 frame_seq][u32 len][body]`.
    assert_eq!(
        &batch[BATCH_HEADER_LEN..],
        &[
            0x00, // Data
            0xAB, 0xCD, 0xEF, 0x42, // slot: index 0xABCDEF, generation 0x42
            0x00, 0x00, 0x00, 0x07, // frame_seq
            0x00, 0x00, 0x00, 0x03, // len
            0xAA, 0xBB, 0xCC, // body
        ]
    );
    assert_eq!(batch.len(), BATCH_HEADER_LEN + RECORD_HEADER_LEN + 3);
}

#[test]
fn open_slot_body_encodes_to_exact_bytes() {
    let batch = encode_batch(0, 0, |encoder| {
        encoder
            .push_open_slot(slot(), 1, 0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00)
            .expect("push");
    });

    assert_eq!(
        &batch[BATCH_HEADER_LEN..],
        &[
            0x01, // OpenSlot
            0xAB, 0xCD, 0xEF, 0x42, //
            0x00, 0x00, 0x00, 0x01, //
            0x00, 0x00, 0x00, 0x10, // len = 16
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // anchor_id
            0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, // session_id
        ]
    );
}

#[test]
fn close_slot_body_encodes_to_exact_bytes() {
    for (reason, byte) in [
        (CloseReason::TerminalSent, 0x00),
        (CloseReason::PeerGone, 0x01),
        (CloseReason::UnknownSlot, 0x02),
        (CloseReason::ProtocolError, 0x03),
    ] {
        let batch = encode_batch(0, 0, |encoder| {
            encoder.push_close_slot(slot(), 0, reason).expect("push");
        });
        assert_eq!(
            &batch[BATCH_HEADER_LEN..],
            &[
                0x02, // CloseSlot
                0xAB, 0xCD, 0xEF, 0x42, //
                0x00, 0x00, 0x00, 0x00, //
                0x00, 0x00, 0x00, 0x01, // len = 1
                byte,
            ],
            "reason {reason:?}"
        );
        assert_eq!(reason.as_u8(), byte);
    }
}

#[test]
fn credit_update_body_encodes_to_exact_bytes() {
    let batch = encode_batch(0, 0, |encoder| {
        encoder
            .push_credit_update(slot(), 0, 0x0000_0100)
            .expect("push");
    });

    assert_eq!(
        &batch[BATCH_HEADER_LEN..],
        &[
            0x03, // CreditUpdate
            0xAB, 0xCD, 0xEF, 0x42, //
            0x00, 0x00, 0x00, 0x00, //
            0x00, 0x00, 0x00, 0x04, // len = 4
            0x00, 0x00, 0x01, 0x00, // delta
        ]
    );
}

#[test]
fn heartbeat_encodes_to_a_bare_header() {
    let batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 9).expect("push");
    });

    assert_eq!(
        &batch[BATCH_HEADER_LEN..],
        &[
            0x04, // SlotHeartbeat
            0xAB, 0xCD, 0xEF, 0x42, //
            0x00, 0x00, 0x00, 0x09, //
            0x00, 0x00, 0x00, 0x00, // len = 0
        ]
    );
    assert_eq!(batch.len(), BATCH_HEADER_LEN + RECORD_HEADER_LEN);
}

#[test]
fn record_count_is_patched_in_at_finish() {
    let batch = encode_batch(7, 7, |encoder| {
        for seq in 0..5 {
            encoder.push_heartbeat(slot(), seq).expect("push");
        }
        // Not written yet: the header still says zero mid-batch.
        assert_eq!(encoder.record_count(), 5);
    });

    assert_eq!(&batch[2..4], &[0x00, 0x05]);
    assert_eq!(BatchHeader::decode(&batch).expect("header").record_count, 5);
}

// ---------------------------------------------------------------------------
// Roundtrips
// ---------------------------------------------------------------------------

#[test]
fn batch_header_roundtrips_over_edge_values() {
    for (peer_epoch, batch_seq, record_count, flags) in [
        (0, 0, 0, 0),
        (u64::MAX, u32::MAX, u16::MAX, u8::MAX),
        (1, 0xFFFF_FFFF, 1, 0b1010_1010),
        (0x8000_0000_0000_0000, 0x8000_0000, 0x8000, 1),
    ] {
        let header = BatchHeader {
            mux_version: MUX_VERSION,
            flags,
            record_count,
            peer_epoch,
            batch_seq,
        };
        let mut buf = BytesMut::new();
        header.encode_into(&mut buf);
        assert_eq!(BatchHeader::decode(&buf).expect("decode"), header);
    }
}

#[test]
fn every_record_type_roundtrips() {
    let batch = encode_batch(0xFEED, 0xBEEF, |encoder| {
        encoder
            .push_open_slot(slot(), 0, 0x1122_3344_5566_7788, 0x99AA_BBCC_DDEE_FF00)
            .expect("push");
        encoder.push_data(slot(), 1, b"tokens").expect("push");
        encoder.push_data(slot(), 2, &[]).expect("push");
        encoder
            .push_credit_update(slot(), 3, u32::MAX)
            .expect("push");
        encoder.push_heartbeat(slot(), 4).expect("push");
        encoder
            .push_close_slot(slot(), 5, CloseReason::TerminalSent)
            .expect("push");
    });

    let (header, records) = decode_all(&batch);
    assert_eq!(header.mux_version, MUX_VERSION);
    assert_eq!(header.peer_epoch, 0xFEED);
    assert_eq!(header.batch_seq, 0xBEEF);
    assert_eq!(header.record_count, 6);
    assert!(header.is_supported());

    let bodies: Vec<RecordBody<'_>> = records.iter().map(|record| record.body).collect();
    assert_eq!(
        bodies,
        vec![
            RecordBody::OpenSlot {
                anchor_id: 0x1122_3344_5566_7788,
                session_id: 0x99AA_BBCC_DDEE_FF00,
            },
            RecordBody::Data(b"tokens"),
            RecordBody::Data(&[]),
            RecordBody::CreditUpdate { delta: u32::MAX },
            RecordBody::SlotHeartbeat,
            RecordBody::CloseSlot {
                reason: CloseReason::TerminalSent,
            },
        ]
    );

    for (index, record) in records.iter().enumerate() {
        assert_eq!(record.slot, slot());
        assert_eq!(record.frame_seq, index as u32);
        assert_eq!(record.record_type(), record.body.record_type());
    }
}

#[test]
fn every_close_reason_roundtrips() {
    for reason in [
        CloseReason::TerminalSent,
        CloseReason::PeerGone,
        CloseReason::UnknownSlot,
        CloseReason::ProtocolError,
    ] {
        let batch = encode_batch(0, 0, |encoder| {
            encoder.push_close_slot(slot(), 0, reason).expect("push");
        });
        let (_, records) = decode_all(&batch);
        assert_eq!(records[0].body, RecordBody::CloseSlot { reason });
        assert_eq!(CloseReason::from_u8(reason.as_u8()), Some(reason));
    }
}

#[test]
fn body_range_indexes_the_payload_for_zero_copy_slicing() {
    let batch = encode_batch(0, 0, |encoder| {
        encoder.push_data(slot(), 0, b"first").expect("push");
        encoder.push_data(slot(), 1, b"second").expect("push");
    });

    let (_, records) = decode_all(&batch);
    assert_eq!(
        records[0].body_range,
        BATCH_HEADER_LEN + RECORD_HEADER_LEN..BATCH_HEADER_LEN + RECORD_HEADER_LEN + 5
    );
    for record in &records {
        let RecordBody::Data(body) = record.body else {
            unreachable!("data records");
        };
        assert_eq!(&batch[record.body_range.clone()], body);
    }
}

#[test]
fn an_empty_batch_is_a_bare_header() {
    let batch = encode_batch(3, 4, |_| {});
    assert_eq!(batch.len(), BATCH_HEADER_LEN);

    let (header, records) = decode_all(&batch);
    assert_eq!(header.record_count, 0);
    assert!(records.is_empty());
}

#[test]
fn a_large_body_roundtrips_intact() {
    let body: Vec<u8> = (0..70_000u32).map(|byte| byte as u8).collect();
    let batch = encode_batch(0, 0, |encoder| {
        encoder.push_data(slot(), 0, &body).expect("push");
    });

    let (_, records) = decode_all(&batch);
    assert_eq!(records[0].body, RecordBody::Data(&body));
}

#[test]
fn a_reused_buffer_starts_a_clean_batch() {
    let first = encode_batch(1, 1, |encoder| {
        encoder.push_data(slot(), 0, b"stale").expect("push");
    });

    let mut encoder = BatchEncoder::with_buffer(first, 2, 2);
    assert!(encoder.is_empty());
    assert_eq!(encoder.encoded_len(), BATCH_HEADER_LEN);
    encoder.push_heartbeat(slot(), 0).expect("push");
    let batch = encoder.finish();

    let (header, records) = decode_all(&batch);
    assert_eq!((header.peer_epoch, header.batch_seq), (2, 2));
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].body, RecordBody::SlotHeartbeat);
}

#[test]
fn encoded_len_tracks_the_record_arithmetic() {
    let mut encoder = BatchEncoder::new(0, 0);
    assert_eq!(encoder.encoded_len(), BATCH_HEADER_LEN);

    encoder.push_data(slot(), 0, &[0; 40]).expect("push");
    assert_eq!(
        encoder.encoded_len(),
        BATCH_HEADER_LEN + record_encoded_len(40).expect("fits")
    );

    assert_eq!(record_encoded_len(0), Some(RECORD_HEADER_LEN));
    assert_eq!(record_encoded_len(usize::MAX), None);
}

// ---------------------------------------------------------------------------
// Encoder limits
// ---------------------------------------------------------------------------

#[test]
fn the_batch_fills_at_the_record_count_ceiling() {
    let mut encoder = BatchEncoder::new(0, 0);
    for seq in 0..u32::from(MAX_RECORDS_PER_BATCH) {
        encoder.push_heartbeat(slot(), seq).expect("push");
    }
    assert_eq!(encoder.record_count(), MAX_RECORDS_PER_BATCH);

    let len_before = encoder.encoded_len();
    assert_eq!(
        encoder.push_heartbeat(slot(), 0),
        Err(EncodeError::BatchFull)
    );
    // A rejected push leaves the batch byte-identical.
    assert_eq!(encoder.encoded_len(), len_before);
    assert_eq!(encoder.record_count(), MAX_RECORDS_PER_BATCH);

    let batch = encoder.finish();
    let header = BatchHeader::decode(&batch).expect("header");
    assert_eq!(header.record_count, MAX_RECORDS_PER_BATCH);
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

#[test]
fn a_short_header_never_decodes() {
    let mut buf = BytesMut::new();
    BatchHeader::new(1, 1).encode_into(&mut buf);

    for len in 0..BATCH_HEADER_LEN {
        assert_eq!(
            BatchHeader::decode(&buf[..len]),
            Err(DecodeError::TruncatedBatchHeader { len }),
            "truncated to {len}"
        );
        assert_eq!(
            BatchDecoder::new(&buf[..len]).err(),
            Some(DecodeError::TruncatedBatchHeader { len })
        );
    }
    assert!(BatchHeader::decode(&buf).is_ok());
}

#[test]
fn an_unknown_version_stops_the_decoder_but_not_the_meter() {
    let mut batch = encode_batch(0xABCD, 0x1234, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
    });
    batch[0] = 7;

    // Peeking still yields the epoch and sequence a stale-version drop is
    // metered against.
    let header = BatchHeader::decode(&batch).expect("peek");
    assert_eq!(header.mux_version, 7);
    assert_eq!(header.peer_epoch, 0xABCD);
    assert_eq!(header.batch_seq, 0x1234);
    assert!(!header.is_supported());

    assert_eq!(
        BatchDecoder::new(&batch).err(),
        Some(DecodeError::UnsupportedVersion { version: 7 })
    );
}

#[test]
fn flags_are_carried_verbatim_and_do_not_fail_the_peer() {
    let mut batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
    });
    batch[1] = 0b1111_1111;

    let (header, records) = decode_all(&batch);
    assert_eq!(header.flags, 0b1111_1111);
    assert_eq!(records.len(), 1);
}

#[test]
fn truncating_a_batch_anywhere_yields_an_error_and_never_a_panic() {
    let batch = encode_batch(1, 2, |encoder| {
        encoder
            .push_open_slot(slot(), 0, 0x1111, 0x2222)
            .expect("push");
        encoder.push_data(slot(), 1, b"payload").expect("push");
        encoder
            .push_close_slot(slot(), 2, CloseReason::TerminalSent)
            .expect("push");
    });

    for len in 0..batch.len() {
        let err = first_error(&batch[..len])
            .unwrap_or_else(|| panic!("truncation to {len} of {} decoded cleanly", batch.len()));
        // Every cut lands in exactly one of the four truncation shapes.
        assert!(
            matches!(
                err,
                DecodeError::TruncatedBatchHeader { .. }
                    | DecodeError::TruncatedRecordHeader { .. }
                    | DecodeError::TruncatedRecordBody { .. }
                    | DecodeError::RecordCountMismatch { .. }
            ),
            "truncation to {len} produced {err:?}"
        );
    }

    assert!(first_error(&batch).is_none());
}

#[test]
fn a_batch_ending_on_a_record_boundary_is_a_count_mismatch() {
    let mut batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
        encoder.push_heartbeat(slot(), 1).expect("push");
        encoder.push_heartbeat(slot(), 2).expect("push");
    });
    batch.truncate(BATCH_HEADER_LEN + RECORD_HEADER_LEN);

    assert_eq!(
        first_error(&batch),
        Some(DecodeError::RecordCountMismatch {
            declared: 3,
            decoded: 1,
        })
    );
}

#[test]
fn a_batch_undercounting_its_records_reports_trailing_bytes() {
    let mut batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
        encoder.push_heartbeat(slot(), 1).expect("push");
    });
    batch[2..4].copy_from_slice(&1u16.to_be_bytes());

    assert_eq!(
        first_error(&batch),
        Some(DecodeError::TrailingBytes {
            offset: BATCH_HEADER_LEN + RECORD_HEADER_LEN,
            remaining: RECORD_HEADER_LEN,
        })
    );
}

#[test]
fn a_wildly_overstated_record_count_costs_one_error() {
    // 65 535 records declared over four bytes of payload: no loop, no
    // allocation sized from the wire, one error.
    let mut payload = BytesMut::new();
    BatchHeader {
        mux_version: MUX_VERSION,
        flags: 0,
        record_count: u16::MAX,
        peer_epoch: 0,
        batch_seq: 0,
    }
    .encode_into(&mut payload);
    payload.extend_from_slice(&[0xFF; 4]);

    let mut decoder = BatchDecoder::new(&payload).expect("header");
    assert_eq!(
        decoder.next(),
        Some(Err(DecodeError::TruncatedRecordHeader {
            offset: BATCH_HEADER_LEN,
            remaining: 4,
        }))
    );
    assert_eq!(decoder.next(), None, "the decoder fuses on error");
}

#[test]
fn an_unknown_record_type_is_rejected_with_its_offset() {
    let mut batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
        encoder.push_heartbeat(slot(), 1).expect("push");
    });
    let second = BATCH_HEADER_LEN + RECORD_HEADER_LEN;
    batch[second] = 5;

    assert_eq!(
        first_error(&batch),
        Some(DecodeError::UnknownRecordType {
            offset: second,
            value: 5,
        })
    );

    for value in 0..=4u8 {
        assert!(RecordType::from_u8(value).is_some(), "type {value}");
    }
    for value in 5..=u8::MAX {
        assert_eq!(RecordType::from_u8(value), None, "type {value}");
    }
}

#[test]
fn a_fixed_layout_body_at_the_wrong_length_is_rejected() {
    // Hand-build each fixed-layout record with a deliberately wrong `len`.
    for (record_type, expected, wrong_len) in [
        (RecordType::OpenSlot, 16, 15),
        (RecordType::OpenSlot, 16, 17),
        (RecordType::CloseSlot, 1, 0),
        (RecordType::CloseSlot, 1, 2),
        (RecordType::CreditUpdate, 4, 3),
        (RecordType::CreditUpdate, 4, 5),
        (RecordType::SlotHeartbeat, 0, 1),
    ] {
        let mut payload = BytesMut::new();
        BatchHeader {
            mux_version: MUX_VERSION,
            flags: 0,
            record_count: 1,
            peer_epoch: 0,
            batch_seq: 0,
        }
        .encode_into(&mut payload);
        payload.put_u8(record_type.as_u8());
        payload.put_u32(slot().raw());
        payload.put_u32(0);
        payload.put_u32(wrong_len);
        payload.put_slice(&vec![0u8; wrong_len as usize]);

        assert_eq!(
            first_error(&payload),
            Some(DecodeError::BodyLengthMismatch {
                offset: BATCH_HEADER_LEN,
                record_type,
                expected,
                actual: wrong_len,
            }),
            "{record_type:?} at len {wrong_len}"
        );
    }
}

#[test]
fn an_unknown_close_reason_is_rejected() {
    for value in 4..=u8::MAX {
        let mut batch = encode_batch(0, 0, |encoder| {
            encoder
                .push_close_slot(slot(), 0, CloseReason::PeerGone)
                .expect("push");
        });
        let body = BATCH_HEADER_LEN + RECORD_HEADER_LEN;
        batch[body] = value;

        assert_eq!(
            first_error(&batch),
            Some(DecodeError::UnknownCloseReason {
                offset: BATCH_HEADER_LEN,
                value,
            }),
            "reason {value}"
        );
        assert_eq!(CloseReason::from_u8(value), None);
    }
}

#[test]
fn an_enormous_declared_body_length_does_not_allocate() {
    let mut payload = BytesMut::new();
    BatchHeader {
        mux_version: MUX_VERSION,
        flags: 0,
        record_count: 1,
        peer_epoch: 0,
        batch_seq: 0,
    }
    .encode_into(&mut payload);
    payload.put_u8(RecordType::Data.as_u8());
    payload.put_u32(slot().raw());
    payload.put_u32(0);
    payload.put_u32(u32::MAX);
    payload.put_slice(b"four");

    assert_eq!(
        first_error(&payload),
        Some(DecodeError::TruncatedRecordBody {
            offset: BATCH_HEADER_LEN,
            declared_len: u32::MAX,
            remaining: 4,
        })
    );
}

#[test]
fn the_decoder_fuses_after_the_first_error() {
    let mut batch = encode_batch(0, 0, |encoder| {
        encoder.push_heartbeat(slot(), 0).expect("push");
        encoder.push_heartbeat(slot(), 1).expect("push");
        encoder.push_heartbeat(slot(), 2).expect("push");
    });
    batch[BATCH_HEADER_LEN] = 9;

    let mut decoder = BatchDecoder::new(&batch).expect("header");
    assert!(matches!(
        decoder.next(),
        Some(Err(DecodeError::UnknownRecordType { .. }))
    ));
    assert_eq!(decoder.next(), None);
    assert_eq!(decoder.next(), None);
    assert_eq!(decoder.decoded(), 0);
}

#[test]
fn the_size_hint_never_promises_a_lower_bound_from_the_wire() {
    let mut payload = BytesMut::new();
    BatchHeader {
        mux_version: MUX_VERSION,
        flags: 0,
        record_count: u16::MAX,
        peer_epoch: 0,
        batch_seq: 0,
    }
    .encode_into(&mut payload);

    let decoder = BatchDecoder::new(&payload).expect("header");
    // Lower bound is zero, so `collect` cannot be talked into a 65 535-element
    // reservation by a peer that sent sixteen bytes.
    assert_eq!(
        decoder.size_hint(),
        (0, Some(usize::from(u16::MAX))),
        "record_count is an upper bound only"
    );
}

#[test]
fn arbitrary_bytes_never_panic_the_decoder() {
    // A cheap deterministic LCG beats a fuzz harness we cannot run in CI: it
    // covers the header/record/body boundaries with garbage on every run,
    // reproducibly.
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (state >> 33) as u32
    };

    for len in 0..96usize {
        for _ in 0..64 {
            let payload: Vec<u8> = (0..len).map(|_| next() as u8).collect();

            // Both entry points, drained to exhaustion. Completing is the
            // assertion; a panic fails the test.
            let _ = BatchHeader::decode(&payload);
            if let Ok(decoder) = BatchDecoder::new(&payload) {
                for record in decoder.flatten() {
                    assert!(record.body_range.end <= payload.len());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SlotId
// ---------------------------------------------------------------------------

#[test]
fn slot_ids_pack_and_unpack_at_the_edges() {
    for (index, generation) in [
        (0, 0),
        (0, u8::MAX),
        (1, 1),
        (MAX_SLOT_INDEX, 0),
        (MAX_SLOT_INDEX, u8::MAX),
        (0x00FF_FFFE, 0x7F),
    ] {
        let id = SlotId::new(index, generation).expect("index fits u24");
        assert_eq!(id.index(), index, "index {index:#x}");
        assert_eq!(id.generation(), generation, "generation {generation}");
        assert_eq!(SlotId::from_raw(id.raw()), id);
    }
}

#[test]
fn slot_ids_reject_an_index_past_u24() {
    assert!(SlotId::new(MAX_SLOT_INDEX, 0).is_some());
    assert_eq!(SlotId::new(MAX_SLOT_INDEX + 1, 0), None);
    assert_eq!(SlotId::new(u32::MAX, 0), None);
}

#[test]
fn every_raw_u32_is_a_slot_id() {
    for raw in [0, 1, u32::MAX, 0xFFFF_FF00, 0x0000_00FF] {
        let id = SlotId::from_raw(raw);
        assert_eq!(id.raw(), raw);
        assert_eq!(
            SlotId::new(id.index(), id.generation()),
            Some(id),
            "raw {raw:#x}"
        );
    }
}

#[test]
fn generations_wrap_without_disturbing_the_index() {
    let mut id = SlotId::new(0x00AB_CDEF, u8::MAX - 1).expect("fits");
    id = id.next_generation();
    assert_eq!(id.generation(), u8::MAX);
    assert_eq!(id.index(), 0x00AB_CDEF);

    id = id.next_generation();
    assert_eq!(
        id.generation(),
        0,
        "the epoch above it is what bounds reuse"
    );
    assert_eq!(id.index(), 0x00AB_CDEF);

    // A full lap returns to where it started.
    let start = SlotId::new(7, 3).expect("fits");
    let mut walked = start;
    for _ in 0..256 {
        walked = walked.next_generation();
    }
    assert_eq!(walked, start);
}

#[test]
fn with_generation_replaces_only_the_low_byte() {
    let id = SlotId::new(MAX_SLOT_INDEX, 0).expect("fits");
    let bumped = id.with_generation(0xAB);
    assert_eq!(bumped.index(), MAX_SLOT_INDEX);
    assert_eq!(bumped.generation(), 0xAB);
    assert_eq!(bumped.with_generation(0), id);
}

#[test]
fn slot_id_debug_shows_both_halves() {
    let rendered = format!("{:?}", SlotId::new(0x00AB_CDEF, 0x42).expect("fits"));
    assert!(rendered.contains("index"), "{rendered}");
    assert!(rendered.contains("generation"), "{rendered}");
    assert!(rendered.contains("11259375"), "{rendered}"); // 0xABCDEF
    assert!(rendered.contains("66"), "{rendered}"); // 0x42
}

// ---------------------------------------------------------------------------
// Record classification
// ---------------------------------------------------------------------------

#[test]
fn only_open_close_and_credit_are_control() {
    assert!(RecordType::OpenSlot.is_control());
    assert!(RecordType::CloseSlot.is_control());
    assert!(RecordType::CreditUpdate.is_control());
    assert!(!RecordType::Data.is_control());
    // Deliberate: a heartbeat dropped under saturation is the per-slot
    // saturation signal, so it must not ride the control reserve.
    assert!(!RecordType::SlotHeartbeat.is_control());
}

#[test]
fn record_type_discriminants_match_the_wire() {
    for (record_type, value) in [
        (RecordType::Data, 0),
        (RecordType::OpenSlot, 1),
        (RecordType::CloseSlot, 2),
        (RecordType::CreditUpdate, 3),
        (RecordType::SlotHeartbeat, 4),
    ] {
        assert_eq!(record_type.as_u8(), value);
        assert_eq!(RecordType::from_u8(value), Some(record_type));
    }
}

// ---------------------------------------------------------------------------
// Sequence arithmetic
// ---------------------------------------------------------------------------

#[test]
fn batch_sequences_compare_in_order_away_from_the_wrap() {
    assert_eq!(batch_seq_cmp(5, 3), Ordering::Greater);
    assert_eq!(batch_seq_cmp(3, 5), Ordering::Less);
    assert_eq!(batch_seq_cmp(7, 7), Ordering::Equal);
    assert!(batch_seq_is_newer(1, 0));
    assert!(!batch_seq_is_newer(0, 1));
    assert!(!batch_seq_is_newer(4, 4));
}

#[test]
fn batch_sequences_compare_across_the_wrap() {
    // The whole point: an epoch outlives a u32 on a busy pair, so 0 has to
    // read as newer than u32::MAX rather than two billion batches stale.
    assert_eq!(batch_seq_cmp(0, u32::MAX), Ordering::Greater);
    assert_eq!(batch_seq_cmp(u32::MAX, 0), Ordering::Less);
    assert!(batch_seq_is_newer(0, u32::MAX));
    assert!(batch_seq_is_newer(3, u32::MAX - 2));
    assert!(!batch_seq_is_newer(u32::MAX - 2, 3));
}

#[test]
fn the_sequence_antipode_reads_as_stale_from_both_sides() {
    let far = 1u32 << 31;
    // RFC 1982 leaves this undefined; we resolve it as "neither is newer", so
    // a batch two billion ahead is discarded rather than silently accepted.
    assert_eq!(batch_seq_cmp(0, far), Ordering::Less);
    assert_eq!(batch_seq_cmp(far, 0), Ordering::Less);
    assert!(!batch_seq_is_newer(0, far));
    assert!(!batch_seq_is_newer(far, 0));
}

#[test]
fn the_sequence_gap_counts_skipped_batches_through_the_wrap() {
    assert_eq!(batch_seq_gap(5, 5), 0);
    assert_eq!(batch_seq_gap(5, 6), 1);
    assert_eq!(batch_seq_gap(u32::MAX, 0), 1);
    assert_eq!(batch_seq_gap(u32::MAX - 1, 2), 4);
    // A duplicate or reordered batch reads as an enormous gap, which is why
    // the value meters and does not decide.
    assert_eq!(batch_seq_gap(5, 4), u32::MAX);
}
