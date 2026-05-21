// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active-message-backed [`FrameTransport`] (deprecated).
//!
//! This implementation layers a per-(anchor, session) reorder buffer on top of
//! velo-messenger's fire-and-forget AM dispatcher. It is preserved as a
//! reference impl for the messenger-piggybacked use case but is **no longer
//! constructable via `Velo::builder()`** because of correctness issues under
//! multi-stream concurrency: the AM dispatcher spawns one tokio task per
//! inbound message and is fundamentally unordered, so the reorder buffer's
//! window can overflow under cross-stream contention and deadlock the
//! consumer.
//!
//! Prefer [`crate::streaming::TcpFrameTransport`] or
//! [`crate::streaming::GrpcFrameTransport`].
//!
//! # Wire layout (unchanged)
//!
//! ```text
//! AM headers: { "anchor_id": "<u64>", "session_id": "<u64>", "seq": "<u64>" }
//! AM payload: [ frame_bytes: ... ]
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::messenger::{Context, Handler, Messenger};
use crate::observability::{StreamingTransportMetricsHandle, VeloMetrics};
use anyhow::Result;
use dashmap::DashMap;
use futures::future::BoxFuture;
use velo_ext::{PeerInfo, TransportKey, WorkerAddress, WorkerId};

use crate::streaming::transport::FrameTransport;

const ANCHOR_ID_HEADER: &str = "anchor_id";
const SESSION_ID_HEADER: &str = "session_id";
const STREAM_SEQ_HEADER: &str = "seq";

/// Default streaming-transport key for the AM-backed transport.
pub const VELO_STREAM_KEY: &str = "velo-stream";

const REORDER_WINDOW: usize = 4096;

type Deposit = (Option<u64>, Vec<u8>);
type DispatchMap = DashMap<(u64, u64), DispatchEntry>;

struct DispatchEntry {
    token: u64,
    sender: flume::Sender<Deposit>,
}

static DISPATCH_TOKEN: AtomicU64 = AtomicU64::new(0);

fn next_dispatch_token() -> u64 {
    DISPATCH_TOKEN.fetch_add(1, Ordering::Relaxed)
}

pub struct VeloFrameTransport {
    messenger: Arc<Messenger>,
    dispatch: Arc<DispatchMap>,
    backpressure_count: Arc<AtomicU64>,
    streaming_metrics: Option<StreamingTransportMetricsHandle>,
    key: TransportKey,
}

impl VeloFrameTransport {
    /// Create a new `VeloFrameTransport` and register the `_stream_data` handler.
    pub fn new(messenger: Arc<Messenger>, metrics: Option<Arc<VeloMetrics>>) -> Result<Self> {
        let dispatch: Arc<DispatchMap> = Arc::new(DashMap::new());
        let backpressure_count: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        let handler_dispatch = dispatch.clone();
        let streaming_metrics = metrics
            .as_ref()
            .map(|metrics| metrics.bind_streaming_transport("velo"));
        let handler = Handler::am_handler_async("_stream_data", move |ctx: Context| {
            let handler_dispatch = handler_dispatch.clone();
            async move {
                let headers = match ctx.headers.as_ref() {
                    Some(h) => h,
                    None => {
                        tracing::warn!("_stream_data: missing headers, dropping frame");
                        return Ok(());
                    }
                };
                let anchor_id = match headers
                    .get(ANCHOR_ID_HEADER)
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    Some(id) => id,
                    None => {
                        tracing::warn!(
                            "_stream_data: missing or invalid {} header, dropping frame",
                            ANCHOR_ID_HEADER
                        );
                        return Ok(());
                    }
                };
                let session_id = match headers
                    .get(SESSION_ID_HEADER)
                    .and_then(|v| v.parse::<u64>().ok())
                {
                    Some(id) => id,
                    None => {
                        tracing::warn!(
                            anchor_id,
                            "_stream_data: missing or invalid {} header, dropping frame",
                            SESSION_ID_HEADER
                        );
                        return Ok(());
                    }
                };
                let seq = match headers.get(STREAM_SEQ_HEADER) {
                    None => None,
                    Some(raw) => match raw.parse::<u64>() {
                        Ok(v) => Some(v),
                        Err(e) => {
                            tracing::error!(
                                anchor_id,
                                session_id,
                                raw = %raw,
                                error = %e,
                                "_stream_data: malformed {} header, dropping frame",
                                STREAM_SEQ_HEADER
                            );
                            return Ok(());
                        }
                    },
                };

                let frame_bytes = ctx.payload.to_vec();

                let deposit_tx = handler_dispatch
                    .get(&(anchor_id, session_id))
                    .map(|entry| entry.value().sender.clone());

                if let Some(tx) = deposit_tx
                    && tx.send((seq, frame_bytes)).is_err()
                {
                    // Deliverer exited.
                }
                Ok(())
            }
        })
        .build();

        messenger.register_streaming_handler(handler)?;

        Ok(Self {
            messenger,
            dispatch,
            backpressure_count,
            streaming_metrics,
            key: TransportKey::new(VELO_STREAM_KEY),
        })
    }

    pub fn backpressure_count(&self) -> u64 {
        self.backpressure_count.load(Ordering::Relaxed)
    }

    /// Remove all dispatch entries for the given anchor (any session).
    pub fn unbind(&self, anchor_id: u64) {
        self.dispatch.retain(|&(aid, _), _| aid != anchor_id);
    }
}

struct DelivererCtx {
    deposit_rx: flume::Receiver<Deposit>,
    token: u64,
    consumer_tx: flume::Sender<Vec<u8>>,
    backpressure: Arc<AtomicU64>,
    metrics: Option<StreamingTransportMetricsHandle>,
    dispatch: Arc<DispatchMap>,
    anchor_id: u64,
    session_id: u64,
}

async fn run_deliverer(ctx: DelivererCtx) {
    let DelivererCtx {
        deposit_rx,
        token,
        consumer_tx,
        backpressure,
        metrics,
        dispatch,
        anchor_id,
        session_id,
    } = ctx;

    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut next_expected: u64 = 0;
    struct Cleanup {
        dispatch: Arc<DispatchMap>,
        token: u64,
        anchor_id: u64,
        session_id: u64,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.dispatch
                .remove_if(&(self.anchor_id, self.session_id), |_, entry| {
                    entry.token == self.token
                });
        }
    }
    let _cleanup = Cleanup {
        dispatch,
        token,
        anchor_id,
        session_id,
    };

    loop {
        let (seq_opt, bytes) = match deposit_rx.recv_async().await {
            Ok(p) => p,
            Err(_) => return,
        };

        let mut ctx = IngestCtx {
            pending: &mut pending,
            next_expected: &mut next_expected,
            consumer_tx: &consumer_tx,
            backpressure: &backpressure,
            metrics: metrics.as_ref(),
            anchor_id,
            session_id,
        };

        if !ingest(seq_opt, bytes, &mut ctx).await {
            return;
        }

        while let Ok((seq_opt, bytes)) = deposit_rx.try_recv() {
            if !ingest(seq_opt, bytes, &mut ctx).await {
                return;
            }
        }
    }
}

struct IngestCtx<'a> {
    pending: &'a mut BTreeMap<u64, Vec<u8>>,
    next_expected: &'a mut u64,
    consumer_tx: &'a flume::Sender<Vec<u8>>,
    backpressure: &'a AtomicU64,
    metrics: Option<&'a StreamingTransportMetricsHandle>,
    anchor_id: u64,
    session_id: u64,
}

async fn ingest(seq_opt: Option<u64>, bytes: Vec<u8>, ctx: &mut IngestCtx<'_>) -> bool {
    match seq_opt {
        None => forward(bytes, ctx).await,
        Some(seq) => {
            if seq < *ctx.next_expected {
                return true;
            }
            if seq == *ctx.next_expected {
                if !forward(bytes, ctx).await {
                    return false;
                }
                *ctx.next_expected += 1;
                while let Some(b) = ctx.pending.remove(ctx.next_expected) {
                    if !forward(b, ctx).await {
                        return false;
                    }
                    *ctx.next_expected += 1;
                }
                return true;
            }
            if ctx.pending.len() >= REORDER_WINDOW && !ctx.pending.contains_key(&seq) {
                tracing::error!(
                    anchor_id = ctx.anchor_id,
                    session_id = ctx.session_id,
                    seq,
                    next_expected = *ctx.next_expected,
                    window = REORDER_WINDOW,
                    "_stream_data: reorder window exceeded; closing stream"
                );
                return false;
            }
            ctx.pending.insert(seq, bytes);
            true
        }
    }
}

async fn forward(bytes: Vec<u8>, ctx: &IngestCtx<'_>) -> bool {
    match ctx.consumer_tx.try_send(bytes) {
        Ok(()) => true,
        Err(flume::TrySendError::Full(bytes)) => {
            ctx.backpressure.fetch_add(1, Ordering::Relaxed);
            if let Some(metrics) = ctx.metrics {
                metrics.record_backpressure();
            }
            ctx.consumer_tx.send_async(bytes).await.is_ok()
        }
        Err(flume::TrySendError::Disconnected(_)) => false,
    }
}

impl FrameTransport for VeloFrameTransport {
    fn key(&self) -> TransportKey {
        self.key.clone()
    }

    fn address(&self) -> WorkerAddress {
        // VeloFrameTransport piggybacks on the messenger; no separate listener.
        WorkerAddress::empty()
    }

    fn register(&self, _peer_info: &PeerInfo) -> Result<()> {
        // Messenger already tracks the peer; nothing to do here.
        Ok(())
    }

    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Receiver<Vec<u8>>>> {
        let dispatch = self.dispatch.clone();
        let backpressure = self.backpressure_count.clone();
        let metrics = self.streaming_metrics.clone();
        Box::pin(async move {
            let (consumer_tx, consumer_rx) = flume::bounded::<Vec<u8>>(256);
            dispatch.retain(|&(aid, _), entry| aid != anchor_id || !entry.sender.is_disconnected());
            let (deposit_tx, deposit_rx) = flume::unbounded::<Deposit>();
            let token = next_dispatch_token();
            dispatch.insert(
                (anchor_id, session_id),
                DispatchEntry {
                    token,
                    sender: deposit_tx,
                },
            );
            tokio::spawn(run_deliverer(DelivererCtx {
                deposit_rx,
                token,
                consumer_tx,
                backpressure,
                metrics,
                dispatch: dispatch.clone(),
                anchor_id,
                session_id,
            }));
            Ok(consumer_rx)
        })
    }

    fn connect(
        &self,
        peer: WorkerId,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let messenger = self.messenger.clone();
        Box::pin(async move {
            let (tx, rx) = flume::bounded::<Vec<u8>>(256);

            tokio::spawn(async move {
                let mut seq: u64 = 0;
                while let Ok(frame_bytes) = rx.recv_async().await {
                    let mut headers = HashMap::with_capacity(3);
                    headers.insert(ANCHOR_ID_HEADER.to_string(), anchor_id.to_string());
                    headers.insert(SESSION_ID_HEADER.to_string(), session_id.to_string());
                    headers.insert(STREAM_SEQ_HEADER.to_string(), seq.to_string());

                    let send = match messenger.am_send_streaming("_stream_data") {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!("am_send_streaming builder: {}", e);
                            break;
                        }
                    };
                    if let Err(e) = send
                        .headers(headers)
                        .raw_payload(bytes::Bytes::from(frame_bytes))
                        .worker(peer)
                        .send()
                        .await
                    {
                        tracing::error!("_stream_data am_send failed: {}", e);
                        break;
                    }
                    // Saturating: 2^64 frames is unreachable in practice, but
                    // wrapping to 0 would break the deliverer's monotonic-seq
                    // assumption inside a session and silently corrupt ordering.
                    seq = seq.saturating_add(1);
                }
            });
            Ok(tx)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spawn_deliverer() -> (
        flume::Sender<Deposit>,
        flume::Receiver<Vec<u8>>,
        Arc<AtomicU64>,
    ) {
        let (deposit_tx, deposit_rx) = flume::unbounded::<Deposit>();
        let (consumer_tx, consumer_rx) = flume::bounded::<Vec<u8>>(64);
        let backpressure = Arc::new(AtomicU64::new(0));
        let dispatch: Arc<DispatchMap> = Arc::new(DashMap::new());
        tokio::spawn(super::run_deliverer(super::DelivererCtx {
            deposit_rx,
            token: super::next_dispatch_token(),
            consumer_tx,
            backpressure: backpressure.clone(),
            metrics: None,
            dispatch,
            anchor_id: 42,
            session_id: 7,
        }));
        (deposit_tx, consumer_rx, backpressure)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deliverer_reorders_shuffled_seqs_in_order() {
        let (tx, rx, _) = spawn_deliverer();
        let order = [3u64, 0, 5, 4, 2, 1, 8, 7, 6, 11, 9, 10, 13, 12, 15, 14];
        for s in order {
            tx.send_async((Some(s), vec![s as u8])).await.unwrap();
        }
        for expected in 0u8..16 {
            let bytes = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv_async())
                .await
                .expect("recv timeout")
                .expect("deliverer closed");
            assert_eq!(bytes, vec![expected], "frames out of order");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deliverer_accepts_head_of_line_at_full_window() {
        let (tx, rx, _) = spawn_deliverer();
        for s in 1u64..=(REORDER_WINDOW as u64) {
            tx.send_async((Some(s), vec![(s & 0xff) as u8]))
                .await
                .unwrap();
        }
        tx.send_async((Some(0), vec![0])).await.unwrap();

        for expected in 0u64..=(REORDER_WINDOW as u64) {
            let bytes = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv_async())
                .await
                .expect("recv timeout — stream was wrongly closed")
                .expect("deliverer closed prematurely");
            assert_eq!(
                bytes,
                vec![(expected & 0xff) as u8],
                "frame at seq {expected}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deliverer_window_overflow_closes_stream() {
        let (tx, rx, _) = spawn_deliverer();
        for s in 1u64..=(REORDER_WINDOW as u64) {
            tx.send_async((Some(s), vec![s as u8])).await.unwrap();
        }
        tx.send_async((Some(REORDER_WINDOW as u64 + 1), vec![0]))
            .await
            .unwrap();

        let res = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv_async()).await;
        match res {
            Ok(Err(_)) => { /* channel closed, expected */ }
            Ok(Ok(b)) => panic!("expected closed channel, got frame: {b:?}"),
            Err(_) => panic!("timed out waiting for channel close"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cleanup_does_not_evict_newer_binding_for_same_key() {
        let key = (1u64, 2u64);
        let dispatch: Arc<DispatchMap> = Arc::new(DashMap::new());

        let (a_tx, a_rx) = flume::unbounded::<Deposit>();
        let (a_consumer_tx, _a_consumer_rx) = flume::bounded::<Vec<u8>>(64);
        let a_token = super::next_dispatch_token();
        dispatch.insert(
            key,
            super::DispatchEntry {
                token: a_token,
                sender: a_tx.clone(),
            },
        );
        let a_deliverer = tokio::spawn(super::run_deliverer(super::DelivererCtx {
            deposit_rx: a_rx,
            token: a_token,
            consumer_tx: a_consumer_tx,
            backpressure: Arc::new(AtomicU64::new(0)),
            metrics: None,
            dispatch: dispatch.clone(),
            anchor_id: key.0,
            session_id: key.1,
        }));

        let (b_tx, _b_rx) = flume::unbounded::<Deposit>();
        let b_token = super::next_dispatch_token();
        dispatch.insert(
            key,
            super::DispatchEntry {
                token: b_token,
                sender: b_tx.clone(),
            },
        );

        drop(a_tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), a_deliverer)
            .await
            .expect("A deliverer did not exit after channel close (5s timeout)")
            .expect("A deliverer panicked");

        let current = dispatch.get(&key).expect("B entry was clobbered");
        assert_eq!(current.value().token, b_token);
        assert!(current.value().sender.same_channel(&b_tx));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deliverer_forwards_legacy_no_seq_in_arrival_order() {
        let (tx, rx, _) = spawn_deliverer();
        for v in 0u8..8 {
            tx.send_async((None, vec![v])).await.unwrap();
        }
        for expected in 0u8..8 {
            let bytes = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv_async())
                .await
                .expect("recv timeout")
                .expect("deliverer closed");
            assert_eq!(bytes, vec![expected]);
        }
    }
}
