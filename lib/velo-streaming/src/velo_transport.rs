// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Production [`FrameTransport`] implementation backed by velo-messenger's
//! active message (AM) fire-and-forget system.
//!
//! [`VeloFrameTransport`] uses a single shared `_stream_data` AM handler
//! registered at construction time. Each incoming AM carries the target
//! `anchor_id` as a string value in the AM headers map (key
//! [`ANCHOR_ID_HEADER`]). The payload contains only the raw frame bytes --
//! no binary prefix.
//!
//! # Routing
//!
//! ```text
//! AM headers: { "anchor_id": "<u64>", "session_id": "<u64>", "seq": "<u64>" }
//! AM payload: [ frame_bytes: ... ]
//! ```
//!
//! # Ordering
//!
//! The sender pump stamps a monotonic per-pump `seq` on every outbound frame
//! (data and heartbeats alike). The receiver-side `_stream_data` handler keeps
//! a per-(anchor_id, session_id) reorder buffer and a single deliverer task
//! that forwards frames to the consumer in `seq` order. This restores per-sender
//! FIFO even when the messenger's AM dispatcher (which spawns one tokio task
//! per AM) delivers handler invocations out of order. Frames missing the `seq`
//! header are treated as in-order (back-compat with senders that predate this
//! header).
//!
//! # Construction
//!
//! ```ignore
//! let transport = VeloFrameTransport::new(messenger, worker_id)?;
//! let manager = AnchorManagerBuilder::default()
//!     .worker_id(worker_id)
//!     .transport(Arc::new(transport))
//!     .build()?;
//! ```

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use dashmap::DashMap;
use futures::future::BoxFuture;
use velo_common::WorkerId;
use velo_messenger::{Context, Handler, Messenger};
use velo_observability::{StreamingTransportMetricsHandle, VeloMetrics};

use crate::transport::FrameTransport;

/// AM header key used to route frames to the correct anchor's dispatch channel.
const ANCHOR_ID_HEADER: &str = "anchor_id";

/// AM header key for session-level routing within a single anchor.
const SESSION_ID_HEADER: &str = "session_id";

/// AM header carrying the per-pump monotonic frame sequence number.
///
/// Stamped on every frame the `connect()` pump emits (data and heartbeats).
/// Used by the receiver to restore send order despite concurrent AM dispatch.
const STREAM_SEQ_HEADER: &str = "seq";

/// Maximum number of out-of-order frames the receiver will buffer per
/// (anchor_id, session_id) before declaring the stream broken. Sized to
/// comfortably absorb tokio scheduler jitter for high-throughput streams
/// (10k frames in tight loops can exhibit reorder distances in the hundreds
/// when many handler tasks are queued at once).
const REORDER_WINDOW: usize = 4096;

/// One deposit posted by the `_stream_data` handler to its session's deliverer.
///
/// `seq = None` means the sender did not stamp a `seq` header. This only
/// matters for rolling upgrades: a new receiver paired with an old sender
/// binary degrades to legacy behavior (arrival-order forwarding, no reorder
/// protection — the same behavior that existed before this fix). Both halves
/// at the current version always stamp `seq`.
type Deposit = (Option<u64>, Vec<u8>);

/// Dispatch map: routes (anchor_id, session_id) → deposit channel into the
/// per-session deliverer task. The deliverer owns the BTreeMap reorder buffer
/// and the consumer's flume sender — handlers only push deposits, eliminating
/// any cross-task locking on the hot path.
type DispatchMap = DashMap<(u64, u64), flume::Sender<Deposit>>;

pub struct VeloFrameTransport {
    messenger: Arc<Messenger>,
    dispatch: Arc<DispatchMap>,
    worker_id: WorkerId,
    /// Number of times the per-(anchor, session) deliverer task hit the slow
    /// (blocking) send path because the consumer's flume channel was full.
    /// Indicates downstream consumer backpressure.
    backpressure_count: Arc<AtomicU64>,
    /// Optional bound metrics handle, cloned per-session into deliverer tasks.
    streaming_metrics: Option<StreamingTransportMetricsHandle>,
}

impl VeloFrameTransport {
    /// Create a new `VeloFrameTransport` and register the `_stream_data` handler.
    ///
    /// # Arguments
    ///
    /// * `messenger` - Injected `Arc<Messenger>`, must already be constructed.
    /// * `worker_id` - This worker's identity, used for endpoint URI construction.
    ///
    /// # Errors
    ///
    /// Returns an error if handler registration fails (e.g., duplicate handler name).
    pub fn new(
        messenger: Arc<Messenger>,
        worker_id: WorkerId,
        metrics: Option<Arc<VeloMetrics>>,
    ) -> Result<Self> {
        let dispatch: Arc<DispatchMap> = Arc::new(DashMap::new());
        let backpressure_count: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        // Register the shared _stream_data handler.
        // The handler captures dispatch and deposits frames into the matching
        // per-(anchor, session) reorder state, then notifies the deliverer.
        // Backpressure / metrics are tracked on the deliverer side, not here.
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
                // Pre-fix senders don't stamp seq; deliverer treats those as
                // "no reorder protection" (forwards in arrival order).
                let seq = headers
                    .get(STREAM_SEQ_HEADER)
                    .and_then(|v| v.parse::<u64>().ok());

                let frame_bytes = ctx.payload.to_vec();

                // Clone the deposit sender out of the DashMap; never hold a
                // shard guard across an await point.
                let deposit_tx = handler_dispatch
                    .get(&(anchor_id, session_id))
                    .map(|entry| entry.value().clone());

                if let Some(tx) = deposit_tx
                    && tx.send_async((seq, frame_bytes)).await.is_err()
                {
                    // Deliverer exited (consumer closed or unbound). The
                    // dispatch entry has already been or will be removed by the
                    // owner; nothing else for the handler to do.
                }
                Ok(())
            }
        })
        .build();

        messenger.register_streaming_handler(handler)?;

        Ok(Self {
            messenger,
            dispatch,
            worker_id,
            backpressure_count,
            streaming_metrics,
        })
    }

    /// Returns the number of times a per-session deliverer task had to await
    /// because the consumer's flume channel was full. Indicates downstream
    /// consumer backpressure.
    pub fn backpressure_count(&self) -> u64 {
        self.backpressure_count.load(Ordering::Relaxed)
    }

    /// Remove all dispatch entries for the given anchor (any session).
    ///
    /// Called when the reader_pump exits or the anchor is cleaned up. Dropping
    /// the deposit sender closes the corresponding deliverer's deposit
    /// receiver, so the deliverer task exits cleanly on its next iteration.
    /// Subsequent AM frames targeting this anchor_id are silently dropped.
    pub fn unbind(&self, anchor_id: u64) {
        self.dispatch.retain(|&(aid, _), _| aid != anchor_id);
    }
}

/// Per-session deliverer task.
///
/// Owns the consumer flume sender and the BTreeMap reorder buffer. Receives
/// `(Option<seq>, bytes)` deposits from the `_stream_data` handler via
/// `deposit_rx` (one task per (anchor, session)). Frames stamped with `seq`
/// are reordered into send order; frames without `seq` (legacy senders) are
/// forwarded in arrival order.
///
/// The deliverer is the *sole* writer to `consumer_tx`, so ordering is
/// enforced in one place regardless of how many handler tasks deposit
/// concurrently. Exits when the deposit channel is closed (unbind / dispatch
/// entry replaced) or when the consumer drops the receiver.
async fn run_deliverer(
    deposit_rx: flume::Receiver<Deposit>,
    consumer_tx: flume::Sender<Vec<u8>>,
    backpressure: Arc<AtomicU64>,
    metrics: Option<StreamingTransportMetricsHandle>,
    dispatch: Arc<DispatchMap>,
    anchor_id: u64,
    session_id: u64,
) {
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut next_expected: u64 = 0;
    // On exit (overflow, consumer-close, or deposit channel close), proactively
    // remove our dispatch entry so handlers stop trying to deposit and the map
    // does not accumulate dead entries on long-lived MPSC anchors.
    struct Cleanup(Arc<DispatchMap>, u64, u64);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            self.0.remove(&(self.1, self.2));
        }
    }
    let _cleanup = Cleanup(dispatch, anchor_id, session_id);

    loop {
        let (seq_opt, bytes) = match deposit_rx.recv_async().await {
            Ok(p) => p,
            Err(_) => return, // unbound / replaced
        };

        if !ingest(
            seq_opt,
            bytes,
            &mut pending,
            &mut next_expected,
            &consumer_tx,
            &backpressure,
            metrics.as_ref(),
            anchor_id,
            session_id,
        )
        .await
        {
            return;
        }

        // Drain anything else already buffered on the deposit channel before
        // re-suspending. Keeps the BTreeMap small and improves throughput.
        while let Ok((seq_opt, bytes)) = deposit_rx.try_recv() {
            if !ingest(
                seq_opt,
                bytes,
                &mut pending,
                &mut next_expected,
                &consumer_tx,
                &backpressure,
                metrics.as_ref(),
                anchor_id,
                session_id,
            )
            .await
            {
                return;
            }
        }
    }
}

/// Process one deposit; returns `false` if the deliverer must exit (consumer
/// closed or window overflowed).
#[allow(clippy::too_many_arguments)]
async fn ingest(
    seq_opt: Option<u64>,
    bytes: Vec<u8>,
    pending: &mut BTreeMap<u64, Vec<u8>>,
    next_expected: &mut u64,
    consumer_tx: &flume::Sender<Vec<u8>>,
    backpressure: &AtomicU64,
    metrics: Option<&StreamingTransportMetricsHandle>,
    anchor_id: u64,
    session_id: u64,
) -> bool {
    match seq_opt {
        // Legacy / pre-fix sender: forward immediately, no reorder protection.
        None => forward(bytes, consumer_tx, backpressure, metrics).await,

        Some(seq) => {
            if seq < *next_expected {
                // Already-delivered seq (shouldn't happen on FIFO transports,
                // but harmless to ignore).
                return true;
            }
            if pending.len() >= REORDER_WINDOW && !pending.contains_key(&seq) {
                tracing::error!(
                    anchor_id,
                    session_id,
                    seq,
                    next_expected = *next_expected,
                    window = REORDER_WINDOW,
                    "_stream_data: reorder window exceeded; closing stream"
                );
                return false;
            }
            pending.insert(seq, bytes);
            // Drain contiguous run.
            while let Some(b) = pending.remove(next_expected) {
                if !forward(b, consumer_tx, backpressure, metrics).await {
                    return false;
                }
                *next_expected += 1;
            }
            true
        }
    }
}

async fn forward(
    bytes: Vec<u8>,
    consumer_tx: &flume::Sender<Vec<u8>>,
    backpressure: &AtomicU64,
    metrics: Option<&StreamingTransportMetricsHandle>,
) -> bool {
    match consumer_tx.try_send(bytes) {
        Ok(()) => true,
        Err(flume::TrySendError::Full(bytes)) => {
            backpressure.fetch_add(1, Ordering::Relaxed);
            if let Some(metrics) = metrics {
                metrics.record_backpressure();
            }
            consumer_tx.send_async(bytes).await.is_ok()
        }
        Err(flume::TrySendError::Disconnected(_)) => false,
    }
}

impl FrameTransport for VeloFrameTransport {
    fn bind(
        &self,
        anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<(String, flume::Receiver<Vec<u8>>)>> {
        let worker_id = self.worker_id;
        let dispatch = self.dispatch.clone();
        let backpressure = self.backpressure_count.clone();
        let metrics = self.streaming_metrics.clone();
        Box::pin(async move {
            let (consumer_tx, consumer_rx) = flume::bounded::<Vec<u8>>(256);
            // Drop dispatch entries whose deposit receiver is gone (deliverer
            // exited because the consumer closed). Live siblings (MPSC case:
            // multiple concurrent session_ids on the same anchor) are preserved.
            dispatch.retain(|&(aid, _), tx| aid != anchor_id || !tx.is_disconnected());
            // Unbounded deposit channel: the deliverer drains contiguous runs
            // eagerly, so steady-state occupancy is small. The hard memory cap
            // is enforced by REORDER_WINDOW on the reorder buffer itself.
            let (deposit_tx, deposit_rx) = flume::unbounded::<Deposit>();
            dispatch.insert((anchor_id, session_id), deposit_tx);
            tokio::spawn(run_deliverer(
                deposit_rx,
                consumer_tx,
                backpressure,
                metrics,
                dispatch.clone(),
                anchor_id,
                session_id,
            ));
            let endpoint = format!("velo://{}/stream/{}", worker_id.as_u64(), anchor_id);
            Ok((endpoint, consumer_rx))
        })
    }

    fn connect(
        &self,
        endpoint: &str,
        _anchor_id: u64,
        session_id: u64,
    ) -> BoxFuture<'_, Result<flume::Sender<Vec<u8>>>> {
        let endpoint = endpoint.to_string();
        let messenger = self.messenger.clone();
        Box::pin(async move {
            let (target_worker_id, target_anchor_id) = parse_velo_uri(&endpoint)?;
            let (tx, rx) = flume::bounded::<Vec<u8>>(256);

            // Spawn pump task: reads from rx, sends AM per frame with
            // anchor_id + session_id + monotonic seq routed via AM headers.
            // The seq counter is the single sequential origin for outbound
            // frames on this (anchor, session) — covers data and heartbeats
            // alike, since both share this flume channel.
            tokio::spawn(async move {
                let mut seq: u64 = 0;
                while let Ok(frame_bytes) = rx.recv_async().await {
                    let mut headers = HashMap::with_capacity(3);
                    headers.insert(ANCHOR_ID_HEADER.to_string(), target_anchor_id.to_string());
                    headers.insert(SESSION_ID_HEADER.to_string(), session_id.to_string());
                    headers.insert(STREAM_SEQ_HEADER.to_string(), seq.to_string());

                    if let Err(e) = messenger
                        .am_send_streaming("_stream_data")
                        .expect("am_send_streaming builder")
                        .headers(headers)
                        .raw_payload(bytes::Bytes::from(frame_bytes))
                        .worker(WorkerId::from_u64(target_worker_id))
                        .send()
                        .await
                    {
                        tracing::error!("_stream_data am_send failed: {}", e);
                        break;
                    }
                    seq += 1;
                }
            });

            Ok(tx)
        })
    }
}

/// Parse a `velo://` URI into `(worker_id, anchor_id)`.
///
/// Expected format: `velo://{worker_id}/stream/{anchor_id}`
///
/// # Errors
///
/// Returns `Err` on malformed URIs (missing prefix, wrong segment count,
/// non-numeric IDs, wrong path segment).
pub fn parse_velo_uri(uri: &str) -> Result<(u64, u64)> {
    let stripped = uri
        .strip_prefix("velo://")
        .ok_or_else(|| anyhow::anyhow!("invalid velo URI: missing velo:// prefix: {}", uri))?;
    let parts: Vec<&str> = stripped.split('/').collect();
    if parts.len() != 3 || parts[1] != "stream" {
        anyhow::bail!(
            "invalid velo URI format: expected velo://{{worker_id}}/stream/{{anchor_id}}, got: {}",
            uri
        );
    }
    let worker_id: u64 = parts[0]
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid worker_id in URI: {}", parts[0]))?;
    let anchor_id: u64 = parts[2]
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid anchor_id in URI: {}", parts[2]))?;
    Ok((worker_id, anchor_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_velo_uri_valid() {
        let (wid, aid) = parse_velo_uri("velo://123/stream/456").unwrap();
        assert_eq!(wid, 123);
        assert_eq!(aid, 456);
    }

    #[test]
    fn test_parse_velo_uri_missing_prefix() {
        assert!(parse_velo_uri("http://123/stream/456").is_err());
    }

    #[test]
    fn test_parse_velo_uri_non_numeric_worker() {
        assert!(parse_velo_uri("velo://abc/stream/456").is_err());
    }

    #[test]
    fn test_parse_velo_uri_non_numeric_anchor() {
        assert!(parse_velo_uri("velo://123/stream/xyz").is_err());
    }

    #[test]
    fn test_parse_velo_uri_wrong_path_segment() {
        assert!(parse_velo_uri("velo://123/wrong/456").is_err());
    }

    #[test]
    fn test_parse_velo_uri_too_few_segments() {
        assert!(parse_velo_uri("velo://123/stream").is_err());
    }

    #[test]
    fn test_parse_velo_uri_too_many_segments() {
        assert!(parse_velo_uri("velo://123/stream/456/extra").is_err());
    }

    // -----------------------------------------------------------------
    // Reorder-buffer unit tests (drive `run_deliverer` directly).
    // -----------------------------------------------------------------

    fn spawn_deliverer() -> (
        flume::Sender<Deposit>,
        flume::Receiver<Vec<u8>>,
        Arc<AtomicU64>,
    ) {
        let (deposit_tx, deposit_rx) = flume::unbounded::<Deposit>();
        let (consumer_tx, consumer_rx) = flume::bounded::<Vec<u8>>(64);
        let backpressure = Arc::new(AtomicU64::new(0));
        let dispatch: Arc<DispatchMap> = Arc::new(DashMap::new());
        tokio::spawn(super::run_deliverer(
            deposit_rx,
            consumer_tx,
            backpressure.clone(),
            None,
            dispatch,
            42,
            7,
        ));
        (deposit_tx, consumer_rx, backpressure)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deliverer_reorders_shuffled_seqs_in_order() {
        let (tx, rx, _) = spawn_deliverer();
        // Deposit 0..16 in a deliberately shuffled order.
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
    async fn deliverer_window_overflow_closes_stream() {
        let (tx, rx, _) = spawn_deliverer();
        // Never send seq=0. Send REORDER_WINDOW gap-fillers, then one more
        // distinct seq to push the deliverer over the cap.
        for s in 1u64..=(REORDER_WINDOW as u64) {
            tx.send_async((Some(s), vec![s as u8])).await.unwrap();
        }
        // This deposit triggers overflow (pending.len() == REORDER_WINDOW and
        // the new seq isn't already in pending).
        tx.send_async((Some(REORDER_WINDOW as u64 + 1), vec![0]))
            .await
            .unwrap();

        // Deliverer must have exited; consumer channel closed with no items.
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv_async()).await;
        match res {
            Ok(Err(_)) => { /* channel closed, expected */ }
            Ok(Ok(b)) => panic!("expected closed channel, got frame: {b:?}"),
            Err(_) => panic!("timed out waiting for channel close"),
        }
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
