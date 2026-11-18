// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! What a crash of the unit-test binary looks like.
//!
//! When this binary dies in CI, the log is often all there is. These tests pin
//! one fact a reader of that log needs: a stack overflow on any thread Rust
//! starts (a test thread, a `std::thread`, a tokio worker) ends in SIGABRT and
//! prints `has overflowed its stack`. It is never a bare SIGSEGV.
//!
//! Two things keep it so. Rust installs a SIGSEGV handler that knows each
//! thread's guard page, and stack probes make every large frame touch that
//! page in order, so even a 256 KiB frame cannot jump past it. Both hold in
//! the debug build and in the `-C instrument-coverage` build that CI's
//! Coverage job runs.
//!
//! So a bare `signal: 11, SIGSEGV` from this binary points at native code, at
//! `unsafe`, or at the host. Do not chase it as a stack overflow. If something
//! linked in later takes over SIGSEGV before Rust installs its handler (a
//! native library's crash reporter, for example), Rust skips its handler, and
//! an overflow then shows as a bare SIGSEGV. The test below fails then, which
//! is the point.

#![cfg(target_os = "linux")]

use std::os::unix::process::ExitStatusExt;
use std::process::Command;

/// Set by the parent test to make [`stack_overflow_probe`] overflow a stack.
/// The value names the kind of thread to overflow.
const PROBE_ENV: &str = "VELO_STACK_OVERFLOW_PROBE";

const SIGABRT: i32 = 6;

#[inline(never)]
fn recurse(depth: u64) -> u64 {
    // A 4 KiB frame that the optimiser cannot drop or turn into a loop.
    let frame = std::hint::black_box([depth as u8; 4096]);
    if depth == 0 {
        return u64::from(frame[0]);
    }
    recurse(depth - 1) + u64::from(std::hint::black_box(frame[17]))
}

#[inline(never)]
fn recurse_big(depth: u64) -> u64 {
    // A frame far larger than a guard page: only stack probes catch it.
    let frame = std::hint::black_box([depth as u8; 256 * 1024]);
    if depth == 0 {
        return u64::from(frame[0]);
    }
    recurse_big(depth - 1) + u64::from(std::hint::black_box(frame[100_000]))
}

/// The child half of
/// [`a_stack_overflow_aborts_with_a_message_not_a_bare_sigsegv`]. It returns
/// at once unless that test started this binary with [`PROBE_ENV`] set.
#[test]
fn stack_overflow_probe() {
    let Some(kind) = std::env::var_os(PROBE_ENV) else {
        return;
    };
    match kind.to_str() {
        Some("thread") => {
            let thread = std::thread::Builder::new()
                .stack_size(64 * 1024)
                .spawn(|| recurse(u64::MAX))
                .unwrap();
            let _ = thread.join();
        }
        Some("big-frames") => {
            let thread = std::thread::Builder::new()
                .stack_size(1024 * 1024)
                .spawn(|| recurse_big(u64::MAX))
                .unwrap();
            let _ = thread.join();
        }
        Some("tokio") => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(64 * 1024)
                .build()
                .unwrap();
            // Spawned, so the overflow runs on the worker, not on this thread.
            let _ = runtime.block_on(async { tokio::spawn(async { recurse(u64::MAX) }).await });
        }
        other => panic!("unknown {PROBE_ENV} value {other:?}"),
    }
    unreachable!("the probe returned instead of overflowing its stack");
}

/// A stack overflow on a thread Rust starts aborts the process with Rust's
/// message. It never dies as a bare SIGSEGV.
///
/// One CI Coverage run of this binary died with `signal: 11, SIGSEGV` and no
/// other output, in the middle of the mux tests. A stack overflow in the
/// instrumented build was the first suspect. This test rules it out: it runs
/// the binary again, overflows a 64 KiB stack on a plain thread and on a tokio
/// worker, and a 1 MiB stack with 256 KiB frames, and checks each time that
/// the process ends in SIGABRT and says a thread overflowed. Lowering `RUST_MIN_STACK` on the whole binary gives the
/// same result: the first overflow is reported as SIGABRT with this message.
#[test]
fn a_stack_overflow_aborts_with_a_message_not_a_bare_sigsegv() {
    let exe = std::env::current_exe().unwrap();
    for kind in ["thread", "big-frames", "tokio"] {
        let out = Command::new(&exe)
            .args([
                "--exact",
                "crash_report_tests::stack_overflow_probe",
                "--test-threads=1",
            ])
            .env(PROBE_ENV, kind)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            out.status.signal(),
            Some(SIGABRT),
            "{kind}: a stack overflow must abort, got {:?}; stderr:\n{stderr}",
            out.status
        );
        assert!(
            stderr.contains("has overflowed its stack"),
            "{kind}: the abort must say which thread overflowed; stderr:\n{stderr}"
        );
    }
}
