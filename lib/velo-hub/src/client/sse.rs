// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The client side of a service watch: the hub's server-sent events, parsed
//! into [`ServiceEvent`]s. Anything that ends the stream other than the hub
//! closing it cleanly after a `disconnected` (a dropped connection, an idle
//! timeout, a hub that stopped, bytes that do not parse) yields
//! `Disconnected`, then the stream ends: the watcher's set is stale either
//! way.

use std::pin::Pin;

use futures::{Stream, StreamExt};
use velo::discovery::ServiceEvent;

use crate::protocol;

pub(super) fn parse_events<S, B>(bytes: S) -> impl Stream<Item = ServiceEvent> + Send
where
    S: Stream<Item = reqwest::Result<B>> + Send + 'static,
    B: AsRef<[u8]>,
{
    struct State<S> {
        bytes: Pin<Box<S>>,
        buffer: Vec<u8>,
        pending: std::collections::VecDeque<ServiceEvent>,
        ended: bool,
    }
    let state = State {
        bytes: Box::pin(bytes),
        buffer: Vec::new(),
        pending: std::collections::VecDeque::new(),
        ended: false,
    };
    futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                if event == ServiceEvent::Disconnected {
                    state.ended = true;
                }
                return Some((event, state));
            }
            if state.ended {
                return None;
            }
            match state.bytes.next().await {
                Some(Ok(chunk)) => {
                    state.buffer.extend_from_slice(chunk.as_ref());
                    while let Some(end) = find_event_end(&state.buffer) {
                        let raw: Vec<u8> = state.buffer.drain(..end).collect();
                        match parse_event(&raw) {
                            Ok(Some(event)) => state.pending.push_back(event),
                            Ok(None) => {}
                            Err(()) => state.pending.push_back(ServiceEvent::Disconnected),
                        }
                    }
                }
                Some(Err(_)) | None => state.pending.push_back(ServiceEvent::Disconnected),
            }
        }
    })
}

/// The end of the first complete SSE event in `buffer` (past its blank
/// line), if there is one.
fn find_event_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|pair| pair == b"\n\n")
        .map(|at| at + 2)
}

/// One SSE event: `None` for a comment (keep-alive), `Err` for one that
/// does not parse.
fn parse_event(raw: &[u8]) -> Result<Option<ServiceEvent>, ()> {
    let text = std::str::from_utf8(raw).map_err(|_| ())?;
    let (mut name, mut data) = (None, String::new());
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            name = Some(value.trim());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim());
        }
    }
    let Some(name) = name else {
        return Ok(None);
    };
    let event = match name {
        protocol::WATCH_INITIAL => {
            ServiceEvent::Initial(serde_json::from_str(&data).map_err(|_| ())?)
        }
        protocol::WATCH_ADDED => ServiceEvent::Added(serde_json::from_str(&data).map_err(|_| ())?),
        protocol::WATCH_REMOVED => {
            ServiceEvent::Removed(serde_json::from_str(&data).map_err(|_| ())?)
        }
        protocol::WATCH_DISCONNECTED => ServiceEvent::Disconnected,
        _ => return Ok(None),
    };
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(chunks: &[&str]) -> Vec<ServiceEvent> {
        let chunks: Vec<reqwest::Result<Vec<u8>>> = chunks
            .iter()
            .map(|chunk| Ok(chunk.as_bytes().to_vec()))
            .collect();
        futures::executor::block_on(parse_events(futures::stream::iter(chunks)).collect())
    }

    const ID: &str = "\"5f0c8f8e-2a4e-4b6a-9d0b-3c3f4e5a6b7c\"";

    fn id() -> velo::InstanceId {
        serde_json::from_str(ID).unwrap()
    }

    #[test]
    fn events_split_across_chunks_and_keep_alives_parse() {
        let added = format!("event: added\ndata: {ID}\n\n");
        let (a, b) = added.split_at(9);
        let got = events(&["event: initial\ndata: []\n", "\n", ":\n\n", a, b]);
        assert_eq!(
            got,
            [
                ServiceEvent::Initial(vec![]),
                ServiceEvent::Added(id()),
                ServiceEvent::Disconnected
            ]
        );
    }

    /// The hub's `disconnected` event, in the form axum's `Sse` writes it,
    /// ends the stream with one `Disconnected` and nothing after it.
    #[test]
    fn the_hubs_disconnected_event_ends_the_stream() {
        let got = events(&[
            "event: initial\ndata: []\n\n",
            "event: disconnected\ndata: null\n\n",
            &format!("event: added\ndata: {ID}\n\n"),
        ]);
        assert_eq!(
            got,
            [ServiceEvent::Initial(vec![]), ServiceEvent::Disconnected]
        );
    }

    #[test]
    fn bad_data_disconnects_and_unknown_events_are_ignored() {
        let got = events(&[
            "event: later-kind\ndata: 1\n\n",
            "event: added\ndata: nope\n\n",
        ]);
        assert_eq!(got, [ServiceEvent::Disconnected]);
    }

    #[test]
    fn a_dropped_stream_disconnects() {
        let chunks: Vec<reqwest::Result<Vec<u8>>> =
            vec![Ok(b"event: initial\ndata: []\n\n".to_vec())];
        let got: Vec<ServiceEvent> =
            futures::executor::block_on(parse_events(futures::stream::iter(chunks)).collect());
        assert_eq!(
            got,
            [ServiceEvent::Initial(vec![]), ServiceEvent::Disconnected]
        );
    }
}
