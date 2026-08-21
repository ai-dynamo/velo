// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Per-message latency-budget ablation for the messenger send path.
//!
//! Five rungs, each adding exactly one layer. The *delta* between adjacent
//! rungs is the attribution, by construction:
//!
//! | rung | what it is | delta attributes |
//! |------|------------|------------------|
//! | `l0` | blocking `write`/`read` on a socket, OS threads | syscall + kernel loopback/UDS RTT |
//! | `l1` | same bytes over tokio `TcpStream`/`UnixStream` | epoll/reactor + task wake + scheduler hop |
//! | `l2` | + `TcpFrameCodec` encode + `FramedRead` decode | preamble build, staging memcpy, `split_to().freeze()` |
//! | `l3` | + full `Transport::send_message` / `DataStreams` | DashMap, `AdmissionGate`, per-conn flume, coalescing writer task, `route_frame`, unbounded flume, consumer recv |
//!
//! Everything is an echo ping-pong of the *same* frame size in both
//! directions, so `one_way = rtt / 2`.
//!
//! Also reported:
//! * `send_message` synchronous caller-side cost (the direct comparand to
//!   `ibv_post_send`).
//! * clock overhead (`Instant::now()` pair), so the ns budget can be corrected.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use futures::StreamExt;
use hdrhistogram::Histogram;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::FramedRead;

use velo::transports::tcp::{TcpFrameCodec, TcpTransportBuilder};
use velo::transports::uds::UdsTransportBuilder;
use velo::transports::{MessageType, Transport, TransportErrorHandler, make_channels};
use velo::{InstanceId, PeerInfo};

/// Frame preamble written by `TcpFrameCodec` (version + type + 2 lengths).
const PREAMBLE: usize = 11;
/// Header size used for every cell, per the task spec.
const HEADER: usize = 64;

#[derive(Parser, Debug)]
#[command(name = "tx_budget")]
struct Args {
    /// Payload sizes in bytes.
    #[arg(long, value_delimiter = ',', default_values_t = [1024usize, 16384, 262144, 1048576, 8388608])]
    sizes: Vec<usize>,

    /// In-flight depths.
    #[arg(long, value_delimiter = ',', default_values_t = [1usize, 64])]
    inflight: Vec<usize>,

    /// Rungs to run: l0t,l1t,l2t,l3t,l0u,l1u,l2u,l3u
    #[arg(
        long,
        value_delimiter = ',',
        default_values_t = ["l0t".to_string(), "l1t".to_string(), "l2t".to_string(), "l3t".to_string(), "l0u".to_string(), "l1u".to_string(), "l2u".to_string(), "l3u".to_string()]
    )]
    rungs: Vec<String>,

    /// Approximate bytes moved per cell (drives iteration count).
    #[arg(long, default_value = "400000000")]
    bytes_per_cell: u64,

    /// Hard cap on iterations per cell.
    #[arg(long, default_value = "20000")]
    max_iters: u64,

    /// Floor on iterations per cell.
    #[arg(long, default_value = "300")]
    min_iters: u64,

    /// Per-connection flume capacity for the l3 rungs (256 = production default).
    #[arg(long, default_value = "256")]
    channel_capacity: usize,

    /// Emit machine-readable `RESULT` lines.
    #[arg(long, default_value = "true")]
    csv: bool,

    /// Apply the TcpTransport's own socket options (SO_SNDBUF/SO_RCVBUF = 2 MiB)
    /// to the raw l1/l2 rungs, to test whether they cause the depth-64 collapse.
    #[arg(long, default_value = "false")]
    socket_opts: bool,

    /// Tokio worker threads. `1` builds a `current_thread` runtime, which
    /// removes every cross-thread wake from the l3 rungs.
    #[arg(long, default_value = "4")]
    worker_threads: usize,
}

// ---------------------------------------------------------------------------
// Timing plumbing
// ---------------------------------------------------------------------------

/// Lock-free per-sequence send timestamps, in ns since `base`.
struct Timing {
    base: Instant,
    slots: Vec<AtomicU64>,
}

impl Timing {
    fn new(n: usize) -> Self {
        Self {
            base: Instant::now(),
            slots: (0..n).map(|_| AtomicU64::new(0)).collect(),
        }
    }
    #[inline]
    fn mark(&self, seq: usize) {
        self.slots[seq].store(self.base.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
    #[inline]
    fn elapsed_ns(&self, seq: usize) -> u64 {
        let now = self.base.elapsed().as_nanos() as u64;
        now.saturating_sub(self.slots[seq].load(Ordering::Relaxed))
    }
}

fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).expect("histogram bounds")
}

#[derive(Clone, Copy)]
struct Cell {
    size: usize,
    inflight: usize,
    warmup: usize,
    iters: usize,
}

impl Cell {
    fn total(&self) -> usize {
        self.warmup + self.iters
    }
    fn frame_len(&self) -> usize {
        PREAMBLE + HEADER + self.size
    }
}

struct Outcome {
    rtt: Histogram<u64>,
    /// Only populated by the l3 rungs.
    send_call: Option<Histogram<u64>>,
    wall: Duration,
}

fn seq_of(buf: &[u8]) -> usize {
    u64::from_le_bytes(buf[..8].try_into().unwrap()) as usize
}

fn put_seq(buf: &mut [u8], seq: usize) {
    buf[..8].copy_from_slice(&(seq as u64).to_le_bytes());
}

/// Build a full wire frame (preamble + header + payload) for the raw rungs.
fn raw_frame(size: usize) -> Vec<u8> {
    let mut v = vec![0u8; PREAMBLE + HEADER + size];
    v[0..2].copy_from_slice(&1u16.to_be_bytes());
    v[2] = MessageType::Message.as_u8();
    v[3..7].copy_from_slice(&(HEADER as u32).to_be_bytes());
    v[7..11].copy_from_slice(&(size as u32).to_be_bytes());
    v
}

// ---------------------------------------------------------------------------
// L0 — blocking sockets on OS threads
// ---------------------------------------------------------------------------

fn l0_blocking<S>(cell: Cell, mut client: S, mut server: S) -> Result<Outcome>
where
    S: std::io::Read + std::io::Write + Send + 'static,
{
    let flen = cell.frame_len();
    let total = cell.total();
    let srv = std::thread::spawn(move || {
        let mut buf = vec![0u8; flen];
        for _ in 0..total {
            if server.read_exact(&mut buf).is_err() {
                return;
            }
            if server.write_all(&buf).is_err() {
                return;
            }
        }
    });

    let mut frame = raw_frame(cell.size);
    let mut rbuf = vec![0u8; flen];
    let mut hist = new_hist();
    let start = Instant::now();
    for i in 0..total {
        put_seq(&mut frame[PREAMBLE..], i);
        let t0 = Instant::now();
        client.write_all(&frame)?;
        client.read_exact(&mut rbuf)?;
        if i >= cell.warmup {
            hist.record(t0.elapsed().as_nanos() as u64).ok();
        }
    }
    let wall = start.elapsed();
    drop(client);
    let _ = srv.join();
    Ok(Outcome {
        rtt: hist,
        send_call: None,
        wall,
    })
}

// ---------------------------------------------------------------------------
// L1 — raw bytes over tokio
// ---------------------------------------------------------------------------

async fn l1_tokio<R, W, SR, SW>(
    cell: Cell,
    mut crd: R,
    mut cwr: W,
    mut srd: SR,
    mut swr: SW,
) -> Result<Outcome>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    SR: tokio::io::AsyncRead + Unpin + Send + 'static,
    SW: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let flen = cell.frame_len();
    let total = cell.total();

    tokio::spawn(async move {
        let mut buf = vec![0u8; flen];
        for _ in 0..total {
            if srd.read_exact(&mut buf).await.is_err() {
                return;
            }
            if swr.write_all(&buf).await.is_err() {
                return;
            }
        }
    });

    let timing = Arc::new(Timing::new(total));
    let (credit_tx, credit_rx) = flume::bounded::<()>(cell.inflight.max(1));
    for _ in 0..cell.inflight {
        credit_tx.send(()).ok();
    }

    let t = Arc::clone(&timing);
    let warmup = cell.warmup;
    let reader = tokio::spawn(async move {
        let mut hist = new_hist();
        let mut buf = vec![0u8; flen];
        for _ in 0..total {
            if crd.read_exact(&mut buf).await.is_err() {
                break;
            }
            let seq = seq_of(&buf[PREAMBLE..]);
            let ns = t.elapsed_ns(seq);
            if seq >= warmup {
                hist.record(ns).ok();
            }
            credit_tx.send_async(()).await.ok();
        }
        hist
    });

    let mut frame = raw_frame(cell.size);
    let start = Instant::now();
    for i in 0..total {
        credit_rx.recv_async().await.ok();
        put_seq(&mut frame[PREAMBLE..], i);
        timing.mark(i);
        cwr.write_all(&frame).await?;
    }
    let hist = reader.await?;
    Ok(Outcome {
        rtt: hist,
        send_call: None,
        wall: start.elapsed(),
    })
}

// ---------------------------------------------------------------------------
// L2 — TcpFrameCodec encode/decode over tokio
// ---------------------------------------------------------------------------

async fn l2_framed<R, W, SR, SW>(
    cell: Cell,
    crd: R,
    mut cwr: W,
    srd: SR,
    mut swr: SW,
) -> Result<Outcome>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    SR: tokio::io::AsyncRead + Unpin + Send + 'static,
    SW: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let total = cell.total();

    tokio::spawn(async move {
        let mut framed = FramedRead::new(srd, TcpFrameCodec::new());
        while let Some(Ok((mt, h, p))) = framed.next().await {
            if TcpFrameCodec::encode_frame(&mut swr, mt, &h, &p)
                .await
                .is_err()
            {
                return;
            }
        }
    });

    let timing = Arc::new(Timing::new(total));
    let (credit_tx, credit_rx) = flume::bounded::<()>(cell.inflight.max(1));
    for _ in 0..cell.inflight {
        credit_tx.send(()).ok();
    }

    let t = Arc::clone(&timing);
    let warmup = cell.warmup;
    let reader = tokio::spawn(async move {
        let mut hist = new_hist();
        let mut framed = FramedRead::new(crd, TcpFrameCodec::new());
        for _ in 0..total {
            let Some(Ok((_, h, _p))) = framed.next().await else {
                break;
            };
            let seq = seq_of(&h);
            let ns = t.elapsed_ns(seq);
            if seq >= warmup {
                hist.record(ns).ok();
            }
            credit_tx.send_async(()).await.ok();
        }
        hist
    });

    let mut header = vec![0u8; HEADER];
    let payload = vec![0u8; cell.size];
    let start = Instant::now();
    for i in 0..total {
        credit_rx.recv_async().await.ok();
        put_seq(&mut header, i);
        timing.mark(i);
        TcpFrameCodec::encode_frame(&mut cwr, MessageType::Message, &header, &payload).await?;
    }
    let hist = reader.await?;
    Ok(Outcome {
        rtt: hist,
        send_call: None,
        wall: start.elapsed(),
    })
}

// ---------------------------------------------------------------------------
// L3 — full Transport
// ---------------------------------------------------------------------------

struct NoopErr;
impl TransportErrorHandler for NoopErr {
    fn on_error(&self, _h: Bytes, _p: Bytes, e: String) {
        eprintln!("transport error: {e}");
    }
}

async fn l3_transport(
    cell: Cell,
    client: Arc<dyn Transport>,
    server: Arc<dyn Transport>,
) -> Result<Outcome> {
    let id_c = InstanceId::new_v4();
    let id_s = InstanceId::new_v4();
    let (adapter_c, streams_c) = make_channels();
    let (adapter_s, streams_s) = make_channels();
    let rt = tokio::runtime::Handle::current();

    client.start(id_c, adapter_c, rt.clone()).await?;
    server.start(id_s, adapter_s, rt.clone()).await?;
    client.register(PeerInfo::new(id_s, server.address()))?;
    server.register(PeerInfo::new(id_c, client.address()))?;

    let total = cell.total();
    let errh: Arc<dyn TransportErrorHandler> = Arc::new(NoopErr);

    // Echo side.
    {
        let server = Arc::clone(&server);
        let errh = Arc::clone(&errh);
        tokio::spawn(async move {
            while let Ok((h, p)) = streams_s.message_stream.recv_async().await {
                ECHOED.fetch_add(1, Ordering::Relaxed);
                server.send_message(id_c, h, p, MessageType::Response, Arc::clone(&errh));
            }
        });
    }

    let timing = Arc::new(Timing::new(total));
    let (credit_tx, credit_rx) = flume::bounded::<()>(cell.inflight.max(1));
    for _ in 0..cell.inflight {
        credit_tx.send(()).ok();
    }

    let t = Arc::clone(&timing);
    let warmup = cell.warmup;
    let reader = tokio::spawn(async move {
        let mut hist = new_hist();
        for _ in 0..total {
            let Ok((h, _p)) = streams_c.response_stream.recv_async().await else {
                break;
            };
            let seq = seq_of(&h);
            let ns = t.elapsed_ns(seq);
            if seq >= warmup {
                hist.record(ns).ok();
            }
            credit_tx.send_async(()).await.ok();
        }
        hist
    });

    // Watchdog: report where a stall happened instead of hanging forever.
    let wd = tokio::spawn(async move {
        let mut last = (0u64, 0u64, 0u64);
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let now = (
                SENT.load(Ordering::Relaxed),
                ECHOED.load(Ordering::Relaxed),
                GOT.load(Ordering::Relaxed),
            );
            if now == last && now.0 > 0 {
                eprintln!(
                    "STALL: sent={} echoed={} got={} (client->server lost {}, server->client lost {})",
                    now.0, now.1, now.2, now.0 - now.1, now.1 - now.2
                );
                std::process::exit(9);
            }
            last = now;
        }
    });

    let payload = Bytes::from(vec![0u8; cell.size]);
    let mut send_hist = new_hist();
    let mut header = vec![0u8; HEADER];
    let start = Instant::now();
    for i in 0..total {
        credit_rx.recv_async().await.ok();
        put_seq(&mut header, i);
        let hdr = Bytes::from(header.clone());
        timing.mark(i);
        let t0 = Instant::now();
        let outcome = client.send_message(
            id_s,
            hdr,
            payload.clone(),
            MessageType::Message,
            Arc::clone(&errh),
        );
        let call_ns = t0.elapsed().as_nanos() as u64;
        if i >= cell.warmup {
            send_hist.record(call_ns).ok();
        }
        // Fire-and-forget: do not await the admission (dropping it does not
        // cancel), but count how often we left the fast path.
        SENT.fetch_add(1, Ordering::Relaxed);
        if !outcome.is_admitted() {
            PENDING.fetch_add(1, Ordering::Relaxed);
        }
    }
    let hist = reader.await?;
    wd.abort();
    SENT.store(0, Ordering::Relaxed);
    ECHOED.store(0, Ordering::Relaxed);
    GOT.store(0, Ordering::Relaxed);
    let wall = start.elapsed();
    client.shutdown();
    server.shutdown();
    Ok(Outcome {
        rtt: hist,
        send_call: Some(send_hist),
        wall,
    })
}

static PENDING: AtomicU64 = AtomicU64::new(0);
static SENT: AtomicU64 = AtomicU64::new(0);
static ECHOED: AtomicU64 = AtomicU64::new(0);
static GOT: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// LH — pure task hop: two tokio tasks, two flume channels, no I/O
//
// Prices the primitive that dominates the l3 − l2 delta: one `flume` send that
// has to wake a parked tokio task on another worker thread.
// ---------------------------------------------------------------------------

async fn lh_task_hop(cell: Cell) -> Result<Outcome> {
    let total = cell.total();
    let (to_srv_tx, to_srv_rx) = flume::bounded::<(Bytes, Bytes)>(cell.inflight.max(1) * 4);
    let (to_cli_tx, to_cli_rx) = flume::unbounded::<(Bytes, Bytes)>();

    tokio::spawn(async move {
        while let Ok(item) = to_srv_rx.recv_async().await {
            if to_cli_tx.send_async(item).await.is_err() {
                return;
            }
        }
    });

    let timing = Arc::new(Timing::new(total));
    let (credit_tx, credit_rx) = flume::bounded::<()>(cell.inflight.max(1));
    for _ in 0..cell.inflight {
        credit_tx.send(()).ok();
    }
    let t = Arc::clone(&timing);
    let warmup = cell.warmup;
    let reader = tokio::spawn(async move {
        let mut hist = new_hist();
        for _ in 0..total {
            let Ok((h, _p)) = to_cli_rx.recv_async().await else {
                break;
            };
            let seq = seq_of(&h);
            let ns = t.elapsed_ns(seq);
            if seq >= warmup {
                hist.record(ns).ok();
            }
            credit_tx.send_async(()).await.ok();
        }
        hist
    });

    let payload = Bytes::from(vec![0u8; cell.size]);
    let mut header = vec![0u8; HEADER];
    let mut send_hist = new_hist();
    let start = Instant::now();
    for i in 0..total {
        credit_rx.recv_async().await.ok();
        put_seq(&mut header, i);
        let hdr = Bytes::from(header.clone());
        timing.mark(i);
        let t0 = Instant::now();
        to_srv_tx.send_async((hdr, payload.clone())).await.ok();
        if i >= cell.warmup {
            send_hist.record(t0.elapsed().as_nanos() as u64).ok();
        }
    }
    let hist = reader.await?;
    Ok(Outcome {
        rtt: hist,
        send_call: Some(send_hist),
        wall: start.elapsed(),
    })
}

// ---------------------------------------------------------------------------
// LG — synchronous cost of AdmissionGate::send vs raw flume try_send
// ---------------------------------------------------------------------------

async fn lg_gate(cell: Cell) -> Result<Outcome> {
    let total = cell.total();
    let (tx, rx) = flume::bounded::<(Bytes, Bytes)>(cell.inflight.max(1));
    let gate = velo::transports::AdmissionGate::new(tx.clone(), tokio::runtime::Handle::current());

    tokio::spawn(async move { while rx.recv_async().await.is_ok() {} });

    let payload = Bytes::from(vec![0u8; cell.size]);
    let header = Bytes::from(vec![0u8; HEADER]);

    let mut raw = new_hist();
    for i in 0..total {
        let t0 = Instant::now();
        let r = tx.try_send((header.clone(), payload.clone()));
        let ns = t0.elapsed().as_nanos() as u64;
        if i >= cell.warmup && r.is_ok() {
            raw.record(ns).ok();
        }
        if r.is_err() {
            tokio::task::yield_now().await;
        }
    }

    let mut gated = new_hist();
    let start = Instant::now();
    for i in 0..total {
        let t0 = Instant::now();
        let outcome = gate.send((header.clone(), payload.clone()));
        let ns = t0.elapsed().as_nanos() as u64;
        if i >= cell.warmup {
            gated.record(ns).ok();
        }
        if !outcome.is_admitted() {
            PENDING.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    }
    println!(
        "      raw flume try_send: p50={} p99={} ns | AdmissionGate::send: p50={} p99={} ns | queued={}",
        raw.value_at_quantile(0.50),
        raw.value_at_quantile(0.99),
        gated.value_at_quantile(0.50),
        gated.value_at_quantile(0.99),
        PENDING.load(Ordering::Relaxed)
    );
    Ok(Outcome {
        rtt: gated,
        send_call: None,
        wall: start.elapsed(),
    })
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

fn uds_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("velo-txb-{tag}-{}.sock", uuid::Uuid::new_v4()))
}

async fn tcp_pair_opts(opts: bool) -> Result<(tokio::net::TcpStream, tokio::net::TcpStream)> {
    let (a, b) = tcp_pair().await?;
    if opts {
        for s in [&a, &b] {
            let sr = socket2::SockRef::from(s);
            sr.set_send_buffer_size(2_097_152).ok();
            sr.set_recv_buffer_size(2_097_152).ok();
        }
    }
    Ok((a, b))
}

async fn tcp_pair() -> Result<(tokio::net::TcpStream, tokio::net::TcpStream)> {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = l.local_addr()?;
    let (a, b) = tokio::join!(tokio::net::TcpStream::connect(addr), l.accept());
    let a = a?;
    let (b, _) = b?;
    a.set_nodelay(true)?;
    b.set_nodelay(true)?;
    Ok((a, b))
}

async fn run_cell(rung: &str, cell: Cell, args: &Args) -> Result<Outcome> {
    match rung {
        "l0t" => {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            let addr = l.local_addr()?;
            let c = std::net::TcpStream::connect(addr)?;
            let (s, _) = l.accept()?;
            c.set_nodelay(true)?;
            s.set_nodelay(true)?;
            tokio::task::spawn_blocking(move || l0_blocking(cell, c, s)).await?
        }
        "l0u" => {
            let p = uds_path("l0");
            let l = std::os::unix::net::UnixListener::bind(&p)?;
            let c = std::os::unix::net::UnixStream::connect(&p)?;
            let (s, _) = l.accept()?;
            let r = tokio::task::spawn_blocking(move || l0_blocking(cell, c, s)).await?;
            let _ = std::fs::remove_file(&p);
            r
        }
        "l1t" => {
            let (a, b) = tcp_pair_opts(args.socket_opts).await?;
            let (ar, aw) = a.into_split();
            let (br, bw) = b.into_split();
            l1_tokio(cell, ar, aw, br, bw).await
        }
        "l2t" => {
            let (a, b) = tcp_pair_opts(args.socket_opts).await?;
            let (ar, aw) = a.into_split();
            let (br, bw) = b.into_split();
            l2_framed(cell, ar, aw, br, bw).await
        }
        "l1u" | "l2u" => {
            let p = uds_path("lx");
            let l = tokio::net::UnixListener::bind(&p)?;
            let (a, b) = tokio::join!(tokio::net::UnixStream::connect(&p), l.accept());
            let a = a?;
            let (b, _) = b?;
            let (ar, aw) = a.into_split();
            let (br, bw) = b.into_split();
            let r = if rung == "l1u" {
                l1_tokio(cell, ar, aw, br, bw).await
            } else {
                l2_framed(cell, ar, aw, br, bw).await
            };
            let _ = std::fs::remove_file(&p);
            r
        }
        "l3t" => {
            let lc = std::net::TcpListener::bind("127.0.0.1:0")?;
            let ls = std::net::TcpListener::bind("127.0.0.1:0")?;
            let c: Arc<dyn Transport> = Arc::new(
                TcpTransportBuilder::new()
                    .from_listener(lc)?
                    .channel_capacity(args.channel_capacity)
                    .build()?,
            );
            let s: Arc<dyn Transport> = Arc::new(
                TcpTransportBuilder::new()
                    .from_listener(ls)?
                    .channel_capacity(args.channel_capacity)
                    .build()?,
            );
            l3_transport(cell, c, s).await
        }
        "l3u" => {
            let pc = uds_path("l3c");
            let ps = uds_path("l3s");
            let c: Arc<dyn Transport> = Arc::new(
                UdsTransportBuilder::new()
                    .socket_path(&pc)
                    .channel_capacity(args.channel_capacity)
                    .build()?,
            );
            let s: Arc<dyn Transport> = Arc::new(
                UdsTransportBuilder::new()
                    .socket_path(&ps)
                    .channel_capacity(args.channel_capacity)
                    .build()?,
            );
            let r = l3_transport(cell, c, s).await;
            let _ = std::fs::remove_file(&pc);
            let _ = std::fs::remove_file(&ps);
            r
        }
        "lh" => lh_task_hop(cell).await,
        "lg" => lg_gate(cell).await,
        other => anyhow::bail!("unknown rung {other}"),
    }
}

fn clock_overhead_ns() -> f64 {
    let n = 2_000_000u32;
    let t0 = Instant::now();
    let mut acc = 0u64;
    for _ in 0..n {
        acc = acc.wrapping_add(Instant::now().elapsed().as_nanos() as u64);
    }
    std::hint::black_box(acc);
    t0.elapsed().as_nanos() as f64 / n as f64
}

fn main() -> Result<()> {
    let args = Args::parse();
    let rt = if args.worker_threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(args.worker_threads)
            .enable_all()
            .build()?
    };
    rt.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {

    let clk = clock_overhead_ns();
    println!(
        "# clock: one Instant::now()+elapsed pair = {:.1} ns (subtract ~{:.0} ns per timed span)",
        clk,
        clk / 2.0
    );
    println!(
        "# frame = {PREAMBLE}B preamble + {HEADER}B header + payload; rtt is echo of same size, one_way = rtt/2"
    );
    println!(
        "{:<5} {:>9} {:>4} {:>7} {:>9} {:>9} {:>10} {:>10} {:>9}",
        "rung", "payload", "ifl", "iters", "p50_rtt", "p99_rtt", "p999_rtt", "p50_1way", "MB/s"
    );

    for rung in &args.rungs {
        for &size in &args.sizes {
            for &ifl in &args.inflight {
                if rung.starts_with("l0") && ifl != 1 {
                    continue; // blocking rung is depth-1 only
                }
                let flen = (PREAMBLE + HEADER + size) as u64;
                let iters = (args.bytes_per_cell / flen)
                    .clamp(args.min_iters, args.max_iters)
                    as usize;
                let cell = Cell {
                    size,
                    inflight: ifl,
                    warmup: (iters / 10).max(20),
                    iters,
                };
                PENDING.store(0, Ordering::Relaxed);
                let out = match run_cell(rung, cell, &args).await {
                    Ok(o) => o,
                    Err(e) => {
                        println!("{rung:<5} {size:>9} {ifl:>4}  FAILED: {e}");
                        continue;
                    }
                };
                let n = out.rtt.len();
                let mbps = (n as f64 * flen as f64 * 2.0) / out.wall.as_secs_f64() / 1e6;
                println!(
                    "{:<5} {:>9} {:>4} {:>7} {:>9} {:>9} {:>10} {:>10} {:>9.1}",
                    rung,
                    size,
                    ifl,
                    n,
                    out.rtt.value_at_quantile(0.50),
                    out.rtt.value_at_quantile(0.99),
                    out.rtt.value_at_quantile(0.999),
                    out.rtt.value_at_quantile(0.50) / 2,
                    mbps
                );
                if let Some(sc) = out.send_call.as_ref() {
                    println!(
                        "      send_message() sync cost: p50={} p99={} p999={} ns (clock-uncorrected); non-fast-path admissions={}",
                        sc.value_at_quantile(0.50),
                        sc.value_at_quantile(0.99),
                        sc.value_at_quantile(0.999),
                        PENDING.load(Ordering::Relaxed)
                    );
                }
                if args.csv {
                    println!(
                        "RESULT,{},{},{},{},{},{},{},{:.1}",
                        rung,
                        size,
                        ifl,
                        n,
                        out.rtt.value_at_quantile(0.50),
                        out.rtt.value_at_quantile(0.99),
                        out.rtt.value_at_quantile(0.999),
                        mbps
                    );
                }
                use std::io::Write as _;
                std::io::stdout().flush().ok();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Ok(())
}
