// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Scratch, for the runtime measurements: the arm switch, the pinned
//! application runtime, a saturation load, and process counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args;
use velo::scratch_arm::RuntimeArm;

#[derive(Args, Debug, Clone)]
pub struct ArmArgs {
    /// Where velo's tasks run: A, B<n>, C<n> or D (see `velo::scratch_arm`).
    #[arg(long, default_value = "A")]
    pub arm: String,
    /// Worker threads of the application's runtime.
    #[arg(long, default_value_t = 4)]
    pub app_threads: usize,
    /// Cores for the application's runtime (and the main thread), e.g. `8-11`.
    #[arg(long)]
    pub app_cpus: Option<String>,
    /// Cores for velo's own runtime threads (arms B and C), e.g. `12-15`.
    #[arg(long)]
    pub velo_cpus: Option<String>,
    /// CPU-bound tasks that keep the application's workers busy. Each spins
    /// for `--saturate-slice-us` and yields. 0 is off.
    #[arg(long, default_value_t = 0)]
    pub saturate: usize,
    #[arg(long, default_value_t = 50)]
    pub saturate_slice_us: u64,
}

impl ArmArgs {
    pub fn arm(&self) -> Result<RuntimeArm> {
        RuntimeArm::parse(&self.arm)
            .ok_or_else(|| anyhow::anyhow!("--arm {} is not an arm", self.arm))
    }

    /// The application's runtime, its workers pinned to `--app-cpus`. Also
    /// sets `VELO_ARM` and the pin for velo's own threads, and pins the
    /// calling thread to `--app-cpus`.
    pub fn runtime(&self) -> Result<tokio::runtime::Runtime> {
        let arm = self.arm()?;
        // SAFETY: called from main before any other thread exists.
        unsafe { std::env::set_var("VELO_ARM", arm.label()) };
        if let Some(cpus) = &self.velo_cpus {
            let set = parse_cpus(cpus)?;
            velo::scratch_arm::set_thread_start(move || pin(&set));
        }
        let app = self.app_cpus.as_deref().map(parse_cpus).transpose()?;
        if let Some(set) = &app {
            pin(set);
        }
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(self.app_threads).enable_all();
        if let Some(set) = app {
            builder.on_thread_start(move || pin(&set));
        }
        Ok(builder.build()?)
    }

    pub fn label(&self) -> String {
        self.arm().map(|a| a.label()).unwrap_or_default()
    }

    /// Start the saturation load on the current runtime, if asked for.
    pub fn saturate(&self) -> Saturation {
        let stop = Arc::new(AtomicBool::new(false));
        let slice = Duration::from_micros(self.saturate_slice_us);
        for _ in 0..self.saturate {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    while t.elapsed() < slice {
                        std::hint::spin_loop();
                    }
                    tokio::task::yield_now().await;
                }
            });
        }
        Saturation(stop)
    }
}

/// Stops the saturation tasks when dropped.
pub struct Saturation(Arc<AtomicBool>);

impl Drop for Saturation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub fn parse_cpus(s: &str) -> Result<Vec<usize>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        if let Some((a, b)) = part.split_once('-') {
            out.extend(a.trim().parse::<usize>()?..=b.trim().parse::<usize>()?);
        } else {
            out.push(part.trim().parse()?);
        }
    }
    Ok(out)
}

fn pin(cpus: &[usize]) {
    // SAFETY: a zeroed cpu_set_t is valid, and CPU_SET stays in its bounds
    // for cpus below CPU_SETSIZE.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// Process counters: CPU, voluntary context switches over every thread, and
/// the thread count.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcStats {
    pub cpu_s: f64,
    pub vcsw: u64,
    pub threads: u64,
    pub at: Option<Instant>,
}

impl ProcStats {
    pub fn now() -> Self {
        let mut s = ProcStats {
            at: Some(Instant::now()),
            ..Default::default()
        };
        // Process CPU (user + system, every thread). The clock, not
        // `/proc/self/stat`, whose 10 ms ticks are too coarse for short cells.
        // SAFETY: a zeroed timespec is valid output storage.
        unsafe {
            let mut ts: libc::timespec = std::mem::zeroed();
            libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts);
            s.cpu_s = ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9;
        }
        if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
            for task in dir.flatten() {
                s.threads += 1;
                if let Ok(status) = std::fs::read_to_string(task.path().join("status")) {
                    for line in status.lines() {
                        if let Some(v) = line.strip_prefix("voluntary_ctxt_switches:") {
                            s.vcsw += v.trim().parse::<u64>().unwrap_or(0);
                        }
                    }
                }
            }
        }
        s
    }

    /// `(cpu_s, vcsw)` between `earlier` and `self`, and this sample's threads.
    pub fn since(&self, earlier: &ProcStats) -> (f64, u64, u64) {
        (
            self.cpu_s - earlier.cpu_s,
            self.vcsw.saturating_sub(earlier.vcsw),
            self.threads,
        )
    }
}

/// Mux counters summed over a set of registries: batcher wakes, batches sent
/// and records sent.
pub fn mux_counters(registries: &[&prometheus::Registry]) -> (f64, f64, f64) {
    let mut wakes = 0.0;
    let mut batches = 0.0;
    let mut records = 0.0;
    for r in registries {
        let snap = velo::observability::test_helpers::MetricSnapshot::from_registry(r);
        wakes += snap.counter_sum("velo_streaming_mux_batcher_wakes_total", &[]);
        batches += snap.histogram_count(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "sent")],
        ) as f64;
        records += snap.histogram_sum(
            "velo_streaming_mux_records_per_batch",
            &[("direction", "sent")],
        );
    }
    (wakes, batches, records)
}
