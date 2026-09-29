// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Throughput benchmark for the Velo messaging facade.
//!
//! Measures messages/sec and bytes/sec across three send patterns:
//!
//! - **Sequential**: one message in-flight at a time (baseline per-message cost)
//! - **Concurrent**: N messages in-flight simultaneously (tests parallelism)
//! - **Pipeline**: fire-and-forget with no per-message await (ceiling throughput)
//! - **Stream**: the server streams items to anchors on the client, over the
//!   messenger mux. Not run by default; select it with `--modes stream`.
//!
//! Results are printed as a table with latency percentiles (p50/p95/p99) for
//! sequential and concurrent modes.
//!
//! By default both halves run in one process, over loopback. To measure a
//! network, run `--role server` on one host and `--role client` on another,
//! with the same `--peer-file` on a shared filesystem and `VELO_BIND_IP` set to
//! each host's address on the network under test.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tokio::task::JoinSet;
use tokio::time::sleep;
use velo::{Handler, InstanceId, Velo};
use velo_examples::{TransportArgs, TransportType, new_transport};

#[derive(Parser, Debug)]
#[command(name = "throughput")]
#[command(about = "Benchmark Velo throughput: msgs/sec, MB/sec, latency percentiles")]
struct Args {
    /// Messages per benchmark cell (warmup is 10% of this, minimum 100).
    #[arg(long, default_value = "10000")]
    count: u64,

    #[command(flatten)]
    tx: TransportArgs,

    /// Payload sizes in bytes (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [0usize, 1024, 65536])]
    payload_sizes: Vec<usize>,

    /// Concurrency levels for concurrent mode (comma-separated).
    #[arg(long, value_delimiter = ',', default_values_t = [1usize, 10, 100])]
    concurrency: Vec<usize>,

    /// Which send patterns to run (comma-separated).
    #[arg(
        long,
        value_enum,
        value_delimiter = ',',
        default_values_t = [Mode::Sequential, Mode::Concurrent, Mode::Pipeline]
    )]
    modes: Vec<Mode>,

    /// Streams open at once for stream mode (comma-separated). `--count`
    /// items are split evenly across them.
    #[arg(long, value_delimiter = ',', default_values_t = [1usize, 16])]
    streams: Vec<usize>,

    /// Which half of the benchmark this process runs. `both` runs the server
    /// and the client in one process.
    #[arg(long, value_enum, default_value_t = Role::Both)]
    role: Role,

    /// Where the server writes its peer info and the client reads it, for
    /// `--role server` and `--role client`.
    #[arg(long)]
    peer_file: Option<std::path::PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Sequential,
    Concurrent,
    Pipeline,
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Role {
    Both,
    Server,
    Client,
}

/// How the client learns that the server counted every pipelined message.
///
/// In one process the server's `count` handler signals a channel. Across
/// processes the client polls `count_done` once a millisecond, which is small
/// against a pipeline run of seconds.
/// Where the client finds the server.
enum PeerSource {
    /// Handed over in the same process.
    Known(velo::PeerInfo),
    /// Written by `--role server`. The file can be left over from an earlier
    /// run, so the client reads it again until the server named in it answers.
    File(std::path::PathBuf),
}

#[derive(Clone)]
enum Done {
    Local(flume::Receiver<()>),
    Remote,
}

struct CellResult {
    mode: &'static str,
    payload_bytes: usize,
    concurrency: Option<usize>,
    msgs_per_sec: f64,
    mb_per_sec: f64,
    p50_us: Option<u64>,
    p95_us: Option<u64>,
    p99_us: Option<u64>,
    p99_9_us: Option<u64>,
}

// ---------------------------------------------------------------------------
// Sequential
// ---------------------------------------------------------------------------

async fn run_sequential(
    velo: &Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    for _ in 0..warmup {
        velo.unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();
    for _ in 0..count {
        let t = Instant::now();
        velo.unary("echo")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
        let _ = hist.record(t.elapsed().as_micros() as u64);
    }
    let elapsed = start.elapsed();

    Ok(CellResult {
        mode: "sequential",
        payload_bytes: payload.len(),
        concurrency: Some(1),
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Concurrent
// ---------------------------------------------------------------------------

async fn run_concurrent(
    velo: Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
    concurrency: usize,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);
    {
        let mut set = JoinSet::new();
        for _ in 0..warmup {
            if set.len() >= concurrency
                && let Some(Ok(Err(e))) = set.join_next().await
            {
                return Err(e);
            }
            let v = Arc::clone(&velo);
            let p = payload.clone();
            set.spawn(async move {
                v.unary("echo")?
                    .raw_payload(p)
                    .instance(target)
                    .send()
                    .await
            });
        }
        while set.join_next().await.is_some() {}
    }

    let mut hist = Histogram::<u64>::new(3).unwrap();
    let mut set: JoinSet<Result<u64>> = JoinSet::new();
    let start = Instant::now();

    for _ in 0..count {
        if set.len() >= concurrency {
            match set.join_next().await {
                Some(Ok(Ok(us))) => {
                    let _ = hist.record(us);
                }
                Some(Ok(Err(e))) => return Err(e),
                _ => {}
            }
        }
        let v = Arc::clone(&velo);
        let p = payload.clone();
        set.spawn(async move {
            let t = Instant::now();
            v.unary("echo")?
                .raw_payload(p)
                .instance(target)
                .send()
                .await?;
            Ok(t.elapsed().as_micros() as u64)
        });
    }
    while let Some(result) = set.join_next().await {
        match result {
            Ok(Ok(us)) => {
                let _ = hist.record(us);
            }
            Ok(Err(e)) => return Err(e),
            _ => {}
        }
    }

    let elapsed = start.elapsed();
    Ok(CellResult {
        mode: "concurrent",
        payload_bytes: payload.len(),
        concurrency: Some(concurrency),
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: Some(hist.value_at_quantile(0.50)),
        p95_us: Some(hist.value_at_quantile(0.95)),
        p99_us: Some(hist.value_at_quantile(0.99)),
        p99_9_us: Some(hist.value_at_quantile(0.999)),
    })
}

// ---------------------------------------------------------------------------
// Pipeline (fire-and-forget)
// ---------------------------------------------------------------------------

async fn set_target(velo: &Arc<Velo>, target: InstanceId, count: u64) -> Result<()> {
    let payload = Bytes::from(rmp_serde::to_vec(&count)?);
    velo.unary("set_target")?
        .raw_payload(payload)
        .instance(target)
        .send()
        .await?;
    Ok(())
}

async fn run_pipeline(
    velo: &Arc<Velo>,
    target: InstanceId,
    payload: Bytes,
    count: u64,
    done: &Done,
) -> Result<CellResult> {
    let warmup = (count / 10).max(100);

    set_target(velo, target, warmup).await?;
    for _ in 0..warmup {
        velo.am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    wait_done(velo, target, done, Duration::from_secs(30))
        .await
        .map_err(|e| anyhow::anyhow!("pipeline warmup: {e}"))?;

    set_target(velo, target, count).await?;
    let start = Instant::now();
    for _ in 0..count {
        velo.am_send("count")?
            .raw_payload(payload.clone())
            .instance(target)
            .send()
            .await?;
    }
    wait_done(velo, target, done, Duration::from_secs(60))
        .await
        .map_err(|e| anyhow::anyhow!("pipeline run did not complete: {e}"))?;

    let elapsed = start.elapsed();
    Ok(CellResult {
        mode: "pipeline",
        payload_bytes: payload.len(),
        concurrency: None,
        msgs_per_sec: count as f64 / elapsed.as_secs_f64(),
        mb_per_sec: (count as f64 * payload.len() as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        p50_us: None,
        p95_us: None,
        p99_us: None,
        p99_9_us: None,
    })
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/// What the client asks the server to stream to one anchor.
#[derive(serde::Serialize, serde::Deserialize)]
struct StreamRequest {
    handle: u128,
    items: u64,
    item_bytes: usize,
}

/// Open `streams` anchors, have the server fill each with `items` items, and
/// wait for every terminal.
async fn stream_round(
    velo: &Arc<Velo>,
    target: InstanceId,
    item_bytes: usize,
    items: u64,
    streams: usize,
) -> Result<()> {
    let mut drains = JoinSet::new();
    for _ in 0..streams {
        let mut anchor = velo.create_anchor::<Bytes>();
        let request = StreamRequest {
            handle: anchor.handle().as_u128(),
            items,
            item_bytes,
        };
        drains.spawn(async move {
            let mut seen = 0u64;
            while let Some(frame) = anchor.next().await {
                match frame? {
                    velo::streaming::StreamFrame::Item(item) => {
                        anyhow::ensure!(item.len() == item_bytes, "item of {} bytes", item.len());
                        seen += 1;
                    }
                    velo::streaming::StreamFrame::Finalized => break,
                    other => anyhow::bail!("unexpected frame: {other:?}"),
                }
            }
            anyhow::ensure!(seen == items, "saw {seen} of {items} items");
            Ok(())
        });
        velo.unary("stream_to")?
            .raw_payload(Bytes::from(rmp_serde::to_vec(&request)?))
            .instance(target)
            .send()
            .await?;
    }
    while let Some(joined) = drains.join_next().await {
        joined??;
    }
    Ok(())
}

async fn run_stream(
    velo: &Arc<Velo>,
    target: InstanceId,
    item_bytes: usize,
    count: u64,
    streams: usize,
) -> Result<CellResult> {
    let streams = streams.max(1);
    let items = (count / streams as u64).max(1);
    let warmup = (items / 10).max(100);
    stream_round(velo, target, item_bytes, warmup, streams)
        .await
        .map_err(|e| anyhow::anyhow!("stream warmup: {e}"))?;

    let start = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(120),
        stream_round(velo, target, item_bytes, items, streams),
    )
    .await
    .map_err(|_| anyhow::anyhow!("stream run timed out"))??;
    let elapsed = start.elapsed().as_secs_f64();

    let total = items * streams as u64;
    Ok(CellResult {
        mode: "stream",
        payload_bytes: item_bytes,
        concurrency: Some(streams),
        msgs_per_sec: total as f64 / elapsed,
        mb_per_sec: (total as f64 * item_bytes as f64) / elapsed / 1_048_576.0,
        p50_us: None,
        p95_us: None,
        p99_us: None,
        p99_9_us: None,
    })
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

async fn wait_done(
    velo: &Arc<Velo>,
    target: InstanceId,
    done: &Done,
    limit: Duration,
) -> Result<()> {
    match done {
        Done::Local(rx) => {
            tokio::time::timeout(limit, rx.recv_async())
                .await
                .map_err(|_| anyhow::anyhow!("timed out"))?
                .map_err(|e| anyhow::anyhow!("done channel error: {e}"))?;
        }
        Done::Remote => {
            let deadline = Instant::now() + limit;
            loop {
                let reply = velo
                    .unary("count_done")?
                    .raw_payload(Bytes::new())
                    .instance(target)
                    .send()
                    .await?;
                if reply.first() == Some(&1) {
                    break;
                }
                if Instant::now() > deadline {
                    anyhow::bail!("timed out");
                }
                sleep(Duration::from_millis(1)).await;
            }
        }
    }
    Ok(())
}

fn format_payload(bytes: usize) -> String {
    if bytes == 0 {
        "0 B".to_string()
    } else if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{} KB", bytes / 1024)
    } else {
        format!("{} MB", bytes / (1024 * 1024))
    }
}

fn print_table(results: &[CellResult], transport: TransportType) {
    println!("\n=== Throughput Benchmark ({transport:?}) ===\n");
    println!(
        "{:<12} {:>8} {:>12} {:>14} {:>9} {:>8} {:>8} {:>8} {:>8}",
        "Mode",
        "Payload",
        "Concurrency",
        "Msgs/sec",
        "MB/sec",
        "p50 µs",
        "p95 µs",
        "p99 µs",
        "p99.9 µs"
    );
    println!("{}", "-".repeat(95));

    let mut last_payload = usize::MAX;
    for r in results {
        if r.payload_bytes != last_payload && last_payload != usize::MAX {
            println!();
        }
        last_payload = r.payload_bytes;

        let concurrency = r
            .concurrency
            .map(|c| c.to_string())
            .unwrap_or_else(|| "-".to_string());
        let p50 = r
            .p50_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p95 = r
            .p95_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p99 = r
            .p99_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());
        let p999 = r
            .p99_9_us
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".into());

        println!(
            "{:<12} {:>8} {:>12} {:>14} {:>9.2} {:>8} {:>8} {:>8} {:>8}",
            r.mode,
            format_payload(r.payload_bytes),
            concurrency,
            format!("{:.0}", r.msgs_per_sec),
            r.mb_per_sec,
            p50,
            p95,
            p99,
            p999,
        );
    }
    println!();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Using {:?} transport", args.tx.transport);
    println!(
        "count={}, payload_sizes={:?}, concurrency={:?}, streams={:?}, modes={:?}, role={:?}",
        args.count, args.payload_sizes, args.concurrency, args.streams, args.modes, args.role
    );
    match args.role {
        Role::Both => run_both(args),
        Role::Server => {
            let path = args
                .peer_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--role server needs --peer-file"))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            // An old file names a server that is gone; a client started
            // before this one writes its own would read it.
            let staged = path.with_extension("staged");
            for old in [&path, &staged] {
                match std::fs::remove_file(old) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            runtime.block_on(async move {
                let (done_tx, _done_rx) = flume::bounded::<()>(1);
                let velo = serve(args.tx.transport, done_tx).await?;
                // Written under another name and renamed, so a client never
                // reads a half-written file.
                std::fs::write(&staged, rmp_serde::to_vec(&velo.peer_info())?)?;
                std::fs::rename(&staged, &path)?;
                println!("Server ready; peer info in {}", path.display());
                std::future::pending::<()>().await;
                Ok(())
            })
        }
        Role::Client => {
            let path = args
                .peer_file
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--role client needs --peer-file"))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let transport = args.tx.transport;
            let results =
                runtime.block_on(run_client(&args, PeerSource::File(path), Done::Remote))?;
            print_table(&results, transport);
            Ok(())
        }
    }
}

/// Both halves in one process, on two runtimes.
fn run_both(args: Args) -> Result<()> {
    let runtime_server = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let runtime_client = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let (peer_info_tx, peer_info_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = flume::bounded::<()>(1);

    let transport_type = args.tx.transport;
    let server_handle = std::thread::spawn(move || {
        runtime_server.block_on(async move {
            let velo = serve(transport_type, done_tx).await.expect("build server");
            peer_info_tx.send(velo.peer_info()).unwrap();
            println!("Server ready");
            std::future::pending::<()>().await;
        });
    });

    let server_peer_info = peer_info_rx.recv().unwrap();
    let client_handle = std::thread::spawn(move || -> Result<Vec<CellResult>> {
        runtime_client.block_on(run_client(
            &args,
            PeerSource::Known(server_peer_info),
            Done::Local(done_rx),
        ))
    });

    let results = client_handle.join().unwrap()?;
    print_table(&results, transport_type);
    drop(server_handle);
    Ok(())
}

/// Build the server and register its handlers.
async fn serve(transport_type: TransportType, done_tx: flume::Sender<()>) -> Result<Arc<Velo>> {
    let transport = new_transport(transport_type, "throughput").await?;
    let velo = Velo::builder().add_transport(transport).build().await?;

    sleep(Duration::from_millis(100)).await;

    let echo_handler = Handler::unary_handler("echo", |ctx| Ok(Some(ctx.payload.clone()))).build();
    velo.register_handler(echo_handler)?;

    let counter = Arc::new(AtomicU64::new(0));
    let target = Arc::new(AtomicU64::new(u64::MAX));
    {
        let counter_for_set = Arc::clone(&counter);
        let target_for_set = Arc::clone(&target);
        let set_target_handler = Handler::unary_handler("set_target", move |ctx| {
            let n: u64 = rmp_serde::from_slice(&ctx.payload)
                .map_err(|e| anyhow::anyhow!("deserialize: {e}"))?;
            counter_for_set.store(0, Ordering::SeqCst);
            target_for_set.store(n, Ordering::SeqCst);
            Ok(Some(Bytes::new()))
        })
        .build();
        velo.register_handler(set_target_handler)?;
    }
    {
        let counter = Arc::clone(&counter);
        let target = Arc::clone(&target);
        let count_handler = Handler::am_handler("count", move |_ctx| {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            let t = target.load(Ordering::SeqCst);
            if n >= t {
                let _ = done_tx.try_send(());
            }
            Ok(())
        })
        .build();
        velo.register_handler(count_handler)?;
    }
    {
        let count_done_handler = Handler::unary_handler("count_done", move |_ctx| {
            let reached = counter.load(Ordering::SeqCst) >= target.load(Ordering::SeqCst);
            Ok(Some(Bytes::from(vec![u8::from(reached)])))
        })
        .build();
        velo.register_handler(count_done_handler)?;
    }
    {
        // A weak reference: the handler is owned by `velo`'s own registry.
        let weak = Arc::downgrade(&velo);
        let stream_to_handler = Handler::unary_handler("stream_to", move |ctx| {
            let request: StreamRequest = rmp_serde::from_slice(&ctx.payload)
                .map_err(|e| anyhow::anyhow!("deserialize: {e}"))?;
            let velo = weak
                .upgrade()
                .ok_or_else(|| anyhow::anyhow!("server is shutting down"))?;
            tokio::spawn(async move {
                if let Err(e) = stream_items(&velo, request).await {
                    eprintln!("stream_to failed: {e:#}");
                }
            });
            Ok(Some(Bytes::new()))
        })
        .build();
        velo.register_handler(stream_to_handler)?;
    }
    Ok(velo)
}

/// Attach to the client's anchor and send it `items` items.
async fn stream_items(velo: &Velo, request: StreamRequest) -> Result<()> {
    let handle = velo::streaming::StreamAnchorHandle::from_u128(request.handle);
    let sender = velo.attach_anchor::<Bytes>(handle).await?;
    let item = Bytes::from(vec![0u8; request.item_bytes]);
    for _ in 0..request.items {
        sender.send(item.clone()).await?;
    }
    sender.finalize()?;
    Ok(())
}

/// Read the peer file until the server it names answers an `echo`.
///
/// A file left over from an earlier run names a server that is gone, and the
/// new server removes it only when it starts. So a failed `echo` means "read
/// the file again", not "give up", until the deadline.
async fn reach_server_from_file(velo: &Arc<Velo>, path: &std::path::Path) -> Result<InstanceId> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut tried: Option<InstanceId> = None;
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && let Ok(info) = rmp_serde::from_slice::<velo::PeerInfo>(&bytes)
            && tried != Some(info.instance_id())
        {
            let target = info.instance_id();
            tried = Some(target);
            velo.register_peer(info)?;
            let echo = velo
                .unary("echo")?
                .raw_payload(Bytes::new())
                .instance(target)
                .send();
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(5), echo).await {
                return Ok(target);
            }
            println!(
                "server in {} did not answer; waiting for a new one",
                path.display()
            );
        }
        if Instant::now() > deadline {
            anyhow::bail!("no live server in {} after 120 s", path.display());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Run every benchmark cell against the server.
async fn run_client(args: &Args, peer: PeerSource, done: Done) -> Result<Vec<CellResult>> {
    let transport = new_transport(args.tx.transport, "throughput").await?;
    let velo = Velo::builder().add_transport(transport).build().await?;

    sleep(Duration::from_millis(100)).await;

    let target = match peer {
        PeerSource::Known(server_peer_info) => {
            velo.register_peer(server_peer_info.clone())?;
            let target = server_peer_info.instance_id();
            sleep(Duration::from_millis(500)).await;
            velo.unary("echo")?
                .raw_payload(Bytes::new())
                .instance(target)
                .send()
                .await?;
            target
        }
        PeerSource::File(path) => reach_server_from_file(&velo, &path).await?,
    };

    let mut all_results = Vec::new();
    for &size in &args.payload_sizes {
        let payload = Bytes::from(vec![0u8; size]);

        if args.modes.contains(&Mode::Sequential) {
            println!("\nSequential, payload={}", format_payload(size));
            all_results.push(run_sequential(&velo, target, payload.clone(), args.count).await?);
        }

        if args.modes.contains(&Mode::Concurrent) {
            for &c in &args.concurrency {
                println!(
                    "Concurrent concurrency={c}, payload={}",
                    format_payload(size)
                );
                all_results.push(
                    run_concurrent(Arc::clone(&velo), target, payload.clone(), args.count, c)
                        .await?,
                );
            }
        }

        if args.modes.contains(&Mode::Pipeline) {
            println!("Pipeline, payload={}", format_payload(size));
            all_results
                .push(run_pipeline(&velo, target, payload.clone(), args.count, &done).await?);
        }

        if args.modes.contains(&Mode::Stream) {
            for &streams in &args.streams {
                println!("Stream streams={streams}, payload={}", format_payload(size));
                all_results.push(run_stream(&velo, target, size, args.count, streams).await?);
            }
        }
    }

    Ok(all_results)
}
