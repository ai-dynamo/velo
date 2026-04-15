// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Consumer-facing frame type for MPSC streams.
//!
//! The wire format is still [`crate::frame::StreamFrame<T>`] — only the
//! consumer API is reshaped so sender lifecycle signals (`Detached`,
//! `Dropped`) are *events* rather than stream terminators.

use crate::frame::StreamFrame;

/// A frame surfaced to [`crate::mpsc::MpscStreamAnchor`] consumers, paired
/// with the originating [`crate::mpsc::SenderId`].
///
/// None of these variants are terminal for the anchor itself — the anchor's
/// stream only ends when [`crate::mpsc::MpscStreamController::cancel`] is
/// called, the consumer drops the anchor, or the anchor's channel closes
/// because every sender has gone away (plus unattached timeout expiry).
#[derive(Debug)]
pub enum MpscFrame<T> {
    /// A data item from the sender.
    Item(T),
    /// A soft error reported by the sender. Does not remove the sender from
    /// the anchor; subsequent items from the same sender are still possible.
    SenderError(String),
    /// The sender exited cleanly (`StreamSender::detach` or, if a misbehaving
    /// peer sends one, `Finalized`). The sender's slot is removed from the
    /// anchor; other senders keep running.
    Detached,
    /// The sender exited without an explicit `detach`. `None` indicates
    /// `StreamFrame::Dropped` (crash / heartbeat timeout); `Some(reason)` is
    /// a folded `StreamFrame::TransportError` whose reason is preserved for
    /// logging.
    Dropped(Option<String>),
}

impl<T> MpscFrame<T> {
    /// Convert a wire frame into the consumer-facing variant.
    ///
    /// Returns `None` for [`StreamFrame::Heartbeat`] (filtered before the
    /// consumer ever sees it).
    pub(crate) fn from_stream_frame(frame: StreamFrame<T>) -> Option<Self> {
        match frame {
            StreamFrame::Heartbeat => None,
            StreamFrame::Item(t) => Some(Self::Item(t)),
            StreamFrame::SenderError(s) => Some(Self::SenderError(s)),
            StreamFrame::Detached | StreamFrame::Finalized => Some(Self::Detached),
            StreamFrame::Dropped => Some(Self::Dropped(None)),
            StreamFrame::TransportError(s) => Some(Self::Dropped(Some(s))),
        }
    }

    /// True if this frame signals that the sender has left the anchor
    /// (clean detach or abnormal drop). The anchor implementation uses this
    /// to remove the sender slot from the registry entry.
    pub(crate) fn is_sender_exit(&self) -> bool {
        matches!(self, Self::Detached | Self::Dropped(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_filtered() {
        let frame: StreamFrame<u32> = StreamFrame::Heartbeat;
        assert!(MpscFrame::from_stream_frame(frame).is_none());
    }

    #[test]
    fn finalized_maps_to_detached() {
        let frame: StreamFrame<u32> = StreamFrame::Finalized;
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        assert!(matches!(mapped, MpscFrame::Detached));
    }

    #[test]
    fn transport_error_folds_into_dropped() {
        let frame: StreamFrame<u32> = StreamFrame::TransportError("boom".into());
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        match mapped {
            MpscFrame::Dropped(Some(reason)) => assert_eq!(reason, "boom"),
            other => panic!("expected Dropped(Some), got {:?}", other),
        }
    }

    #[test]
    fn dropped_no_reason() {
        let frame: StreamFrame<u32> = StreamFrame::Dropped;
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        assert!(matches!(mapped, MpscFrame::Dropped(None)));
    }

    #[test]
    fn item_round_trip() {
        let frame: StreamFrame<u32> = StreamFrame::Item(7);
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        match mapped {
            MpscFrame::Item(v) => assert_eq!(v, 7),
            other => panic!("expected Item, got {:?}", other),
        }
    }
}
