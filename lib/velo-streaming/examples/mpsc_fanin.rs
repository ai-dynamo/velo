// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Demonstrates `velo_streaming::mpsc`: one aggregator, many producers, one
//! consumer-owned finalize path.
//!
//! Default scenario (no flags): 4 producers fan into a single
//! `MpscStreamAnchor<WorkItem>`.
//! - Producer 0 detaches after `items/2` items (clean exit, non-terminal).
//! - Producer 1 is task-aborted mid-stream to trigger a heartbeat-timeout
//!   `Dropped` event (non-terminal).
//! - Producers 2..N run to completion and drop cleanly (`Dropped(None)`).
//! - The consumer counts items per `SenderId`, prints a summary, then calls
//!   `controller.cancel()` to finalize.
//!
//! Run: `cargo run --example mpsc_fanin -- --producers 4 --items 40`
//!
//! Flags (simple parser — no clap dependency):
//!   --producers N       number of producers (default 4)
//!   --items N           items per producer (default 40)
//!   --heartbeat-ms N    heartbeat interval (default 200 ms)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use velo_common::WorkerId;
use velo_streaming::{
    AnchorManager, FrameTransport, MpscAnchorConfig, MpscFrame, MpscStreamSender, SenderId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkItem {
    seq: u32,
    producer: u32,
}

struct Args {
    producers: u32,
    items: u32,
    heartbeat: Duration,
}

fn parse_args() -> Args {
    let mut args = Args {
        producers: 4,
        items: 40,
        heartbeat: Duration::from_millis(200),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--producers" => {
                args.producers = it.next().and_then(|s| s.parse().ok()).unwrap_or(4);
            }
            "--items" => {
                args.items = it.next().and_then(|s| s.parse().ok()).unwrap_or(40);
            }
            "--heartbeat-ms" => {
                let ms: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(200);
                args.heartbeat = Duration::from_millis(ms);
            }
            "--help" | "-h" => {
                println!("usage: mpsc_fanin [--producers N] [--items N] [--heartbeat-ms N]");
                std::process::exit(0);
            }
            other => {
                eprintln!("ignoring unknown arg: {other}");
            }
        }
    }
    args
}

/// In-memory transport — sufficient for a same-process fan-in demo.
struct LoopbackTransport;

impl FrameTransport for LoopbackTransport {
    fn bind(
        &self,
        anchor_id: u64,
        _session_id: u64,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<(String, flume::Receiver<Vec<u8>>)>> {
        Box::pin(async move {
            let (_tx, rx) = flume::bounded::<Vec<u8>>(256);
            Ok((format!("loopback://{anchor_id}"), rx))
        })
    }

    fn connect(
        &self,
        _endpoint: &str,
        _anchor_id: u64,
        _session_id: u64,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<flume::Sender<Vec<u8>>>> {
        Box::pin(async move {
            let (tx, _rx) = flume::bounded::<Vec<u8>>(1);
            Ok(tx)
        })
    }
}

async fn producer(id: u32, sender: MpscStreamSender<WorkItem>, items: u32, detach_at: Option<u32>) {
    for seq in 0..items {
        if let Some(threshold) = detach_at
            && seq == threshold
        {
            println!("[producer {id}] detaching at seq {seq}");
            let _ = sender.detach();
            return;
        }
        if sender.send(WorkItem { seq, producer: id }).await.is_err() {
            println!("[producer {id}] channel closed at seq {seq}, exiting");
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("[producer {id}] finished all {items} items, dropping");
    // Drop sender → Dropped(None) sentinel.
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = parse_args();
    println!(
        "mpsc_fanin: producers={} items_per_producer={} heartbeat={:?}",
        args.producers, args.items, args.heartbeat
    );

    let mgr = Arc::new(AnchorManager::new(
        WorkerId::from_u64(1),
        Arc::new(LoopbackTransport),
    ));

    let config = MpscAnchorConfig {
        heartbeat_interval: Some(args.heartbeat),
        unattached_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    };
    let mut anchor = mgr.create_mpsc_anchor_with_config::<WorkItem>(config);
    let handle = anchor.handle();
    let controller = anchor.controller();

    let mut tasks = Vec::new();
    for i in 0..args.producers {
        let sender = mgr
            .attach_mpsc_stream_anchor::<WorkItem>(handle)
            .await
            .expect("attach producer");
        println!("[consumer] attached producer {i} → {}", sender.sender_id());

        let detach_at = if i == 0 { Some(args.items / 2) } else { None };

        let handle_task = tokio::spawn(producer(i, sender, args.items, detach_at));

        // Producer 1 simulates a crash: abort the task after a short delay
        // so the heartbeat-timeout path fires a Dropped(None) event.
        if i == 1 {
            let abort_handle = handle_task.abort_handle();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                println!("[chaos] aborting producer 1 mid-stream");
                abort_handle.abort();
            });
        }

        tasks.push(handle_task);
    }

    // Consume frames until each producer has signalled its exit.
    let mut items_by_sender: HashMap<SenderId, u32> = HashMap::new();
    let mut exits_seen: HashMap<SenderId, String> = HashMap::new();
    let expected_exits = args.producers as usize;

    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), anchor.next()).await;
        match next {
            Ok(Some(Ok((sid, MpscFrame::Item(item))))) => {
                *items_by_sender.entry(sid).or_default() += 1;
                // Print sparsely to keep stdout readable on large runs.
                if item.seq % 10 == 0 {
                    println!("[{sid}] Item(seq={}, producer={})", item.seq, item.producer);
                }
            }
            Ok(Some(Ok((sid, MpscFrame::Detached)))) => {
                println!("[{sid}] Detached");
                exits_seen.insert(sid, "Detached".to_string());
            }
            Ok(Some(Ok((sid, MpscFrame::Dropped(reason))))) => {
                println!("[{sid}] Dropped({:?})", reason);
                exits_seen.insert(sid, format!("Dropped({reason:?})"));
            }
            Ok(Some(Ok((sid, MpscFrame::SenderError(msg))))) => {
                println!("[{sid}] SenderError({msg})");
            }
            Ok(Some(Err(e))) => {
                println!("[consumer] stream error: {e}");
            }
            Ok(None) => {
                println!("[consumer] stream ended (channel closed)");
                break;
            }
            Err(_) => {
                println!("[consumer] 5s silence — stopping");
                break;
            }
        }
        if exits_seen.len() >= expected_exits {
            break;
        }
    }

    println!("\n[consumer] summary:");
    let mut sids: Vec<_> = items_by_sender.keys().copied().collect();
    sids.sort();
    for sid in sids {
        let count = items_by_sender.get(&sid).copied().unwrap_or(0);
        let exit = exits_seen
            .get(&sid)
            .map(String::as_str)
            .unwrap_or("<still active>");
        println!("    {sid}: {count} items, exit={exit}");
    }

    println!("[consumer] calling controller.cancel()");
    controller.cancel();

    for t in tasks {
        let _ = t.await;
    }
    Ok(())
}
