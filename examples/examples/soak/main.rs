// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// The harness keeps a few helpers (e.g. RSS sampler, fault-injected oracle
// fields) ahead of the scenarios that will use them. Allow the dead_code
// across the binary so we don't bury reserved scaffolding behind per-item
// attributes.
#![allow(dead_code)]

//! Velo soak harness — multi-tier stress + correctness driver for the messenger,
//! streaming, and rendezvous surfaces.
//!
//! ```text
//! soak messenger  --transport tcp|grpc  --tier ci|nightly|long  [--faults]
//! soak stream     --transport tcp|grpc  --tier ci|nightly|long  [--faults]
//! soak rendezvous --transport tcp|grpc  --tier ci|nightly|long  [--faults]
//! soak all        --transport tcp|grpc  --tier ci|nightly|long  [--faults]
//! ```

mod faults;
mod harness;
mod messenger;
mod rendezvous;
mod stream;

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use velo_examples::TransportType;

use crate::harness::ScenarioCtx;
use crate::harness::oracle::ScenarioReport;
use crate::harness::tier::Tier;

/// Streaming-transport selection for the soak harness.
///
/// `Velo` (the AM-backed `VeloFrameTransport`) is intentionally **not** an
/// option — it has known correctness issues under multi-stream concurrency
/// and is undergoing phased deprecation. See `lib/velo/src/streaming.rs` for
/// the deprecation notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lower")]
enum StreamTransport {
    Tcp,
    Grpc,
}

#[derive(Parser, Debug)]
#[command(name = "soak", about = "Velo soak / stress / correctness harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Messenger soak (am_send / unary / drain / fault scenarios).
    Messenger(Args),
    /// Streaming soak (single + many anchors / large items / fault scenarios).
    Stream(Args),
    /// Rendezvous soak (inline + chunked + refcount + fault scenarios).
    Rendezvous(Args),
    /// Run every area in sequence.
    All(Args),
}

#[derive(clap::Args, Debug, Clone)]
struct Args {
    /// Messenger transport backend (tcp or grpc).
    #[arg(long, default_value = "tcp")]
    transport: TransportType,

    /// Streaming transport backend (tcp or grpc). Defaults to tcp; see
    /// `StreamTransport` for why `velo` is no longer an option.
    #[arg(long, default_value = "tcp")]
    stream_transport: StreamTransport,

    /// Sizing tier. Determines per-scenario duration and load defaults.
    #[arg(long, default_value = "ci")]
    tier: Tier,

    /// Override per-scenario duration (e.g. `5s`, `30m`, `2h`).
    #[arg(long, value_parser = humantime::parse_duration)]
    duration: Option<Duration>,

    /// Run fault-injection variants (panicking handler, peer kill, etc).
    #[arg(long)]
    faults: bool,

    /// Subset of scenario IDs (e.g. `M1,M5,F2`). Empty = all in scope.
    #[arg(long, value_delimiter = ',')]
    scenarios: Vec<String>,
}

impl Args {
    fn into_ctx(self) -> ScenarioCtx {
        ScenarioCtx {
            tier: self.tier,
            duration: self.duration,
            faults: self.faults,
            scenarios: self.scenarios,
            transport: self.transport,
            stream_transport: match self.stream_transport {
                StreamTransport::Tcp => crate::harness::StreamTransportKind::Tcp,
                StreamTransport::Grpc => crate::harness::StreamTransportKind::Grpc,
            },
        }
    }
}

fn main() -> Result<()> {
    velo_examples::init_tracing();
    let cli = Cli::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let result: Result<Vec<ScenarioReport>> = runtime.block_on(async move {
        match cli.cmd {
            Cmd::Messenger(args) => messenger::run_all(&args.into_ctx()).await,
            Cmd::Stream(args) => stream::run_all(&args.into_ctx()).await,
            Cmd::Rendezvous(args) => rendezvous::run_all(&args.into_ctx()).await,
            Cmd::All(args) => {
                let ctx = args.into_ctx();
                let mut all = Vec::new();
                all.extend(messenger::run_all(&ctx).await?);
                all.extend(stream::run_all(&ctx).await?);
                all.extend(rendezvous::run_all(&ctx).await?);
                Ok(all)
            }
        }
    });

    match result {
        Ok(reports) => {
            print_report_table(&reports);
            Ok(())
        }
        Err(e) => {
            eprintln!("\n[soak] FAILED: {e:#}\n");
            std::process::exit(1);
        }
    }
}

fn print_report_table(reports: &[ScenarioReport]) {
    println!();
    println!("=== Soak report ===");
    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>8} {:>8} {:>10}  note",
        "id", "elapsed", "sent", "received", "faults", "errors", "bp"
    );
    println!("{}", "-".repeat(95));
    let started = Instant::now();
    for r in reports {
        println!(
            "{:<6} {:>10} {:>10} {:>10} {:>8} {:>8} {:>10}  {}",
            r.id,
            humantime::format_duration(round_to_sec(r.elapsed)).to_string(),
            r.sent,
            r.received,
            r.fault_injected,
            r.on_error_seen,
            r.backpressure_seen,
            r.note,
        );
    }
    println!();
    let _ = started; // keep import simple
}

fn round_to_sec(d: Duration) -> Duration {
    Duration::from_secs(d.as_secs().max(1))
}
