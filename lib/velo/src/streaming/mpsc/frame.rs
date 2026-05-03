// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Consumer-facing frame type for MPSC streams.
//!
//! The wire format is still [`crate::streaming::frame::StreamFrame<T>`] — only the
//! consumer API is reshaped so sender lifecycle signals (`Detached`,
//! `Dropped`) are *events* rather than stream terminators.

use crate::streaming::frame::StreamFrame;

/// A frame surfaced to [`crate::streaming::mpsc::MpscStreamAnchor`] consumers, paired
/// with the originating [`crate::streaming::mpsc::SenderId`].
///
/// None of these variants are terminal for the anchor itself — the anchor's
/// stream only ends when [`crate::streaming::mpsc::MpscStreamController::cancel`] is
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

    #[test]
    fn sender_error_maps_to_sender_error() {
        let frame: StreamFrame<u32> = StreamFrame::SenderError("soft".into());
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        match mapped {
            MpscFrame::SenderError(msg) => assert_eq!(msg, "soft"),
            other => panic!("expected SenderError, got {:?}", other),
        }
    }

    #[test]
    fn detach_maps_to_detached() {
        let frame: StreamFrame<u32> = StreamFrame::Detached;
        let mapped = MpscFrame::from_stream_frame(frame).expect("mapped");
        assert!(matches!(mapped, MpscFrame::Detached));
    }

    #[test]
    fn is_sender_exit_predicate() {
        // Item / SenderError are NOT exit events (anchor keeps the sender slot).
        let item: MpscFrame<u32> = MpscFrame::Item(1);
        assert!(!item.is_sender_exit());
        let err: MpscFrame<u32> = MpscFrame::SenderError("x".into());
        assert!(!err.is_sender_exit());

        // Detached / Dropped ARE exit events.
        let detached: MpscFrame<u32> = MpscFrame::Detached;
        assert!(detached.is_sender_exit());
        let dropped_none: MpscFrame<u32> = MpscFrame::Dropped(None);
        assert!(dropped_none.is_sender_exit());
        let dropped_some: MpscFrame<u32> = MpscFrame::Dropped(Some("why".into()));
        assert!(dropped_some.is_sender_exit());
    }
}
