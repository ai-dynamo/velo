// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Rust bindings for [UCX](https://openucx.org) (Unified Communication X),
//! with a vendored static build of the UCX libraries.
//!
//! By default this crate extracts and builds the UCX release tarball shipped
//! inside it (`--enable-static --with-pic`, InfiniBand/RoCE via rdma-core) and
//! statically absorbs the resulting archives into the consumer. No UCX `.so`
//! files, plugin directories, or `LD_LIBRARY_PATH` configuration exist at
//! runtime — the only dynamic dependencies are the system's `libibverbs.so.1`
//! / `libmlx5.so.1` / `librdmacm.so.1` (with the `ib`/`rdmacm` features).
//!
//! Set `UCX_DIR=/path/to/ucx` to link a preinstalled UCX (>= 1.17) instead.
//!
//! ## Scope
//!
//! The bound surface is currently the UCP layer that
//! [velo](https://github.com/ai-dynamo/velo) needs — context/worker/endpoint
//! lifecycle, Active Messages (`ucp_am_send_nbx` and the recv-handler path),
//! RMA (`ucp_mem_map`/`ucp_rkey_pack`/`ucp_get_nbx`/`ucp_put_nbx`), wakeup
//! (`ucp_worker_get_efd`/`arm`/`signal`), and flush/close. Coverage grows as
//! needed; the raw bindings live in [`sys`] and are regenerated with the
//! non-default `bindgen` feature.
//!
//! ## Consumer invariants (read before depending on this crate)
//!
//! * **Reference the crate.** A dependent that never names `ucx_rs` (e.g.
//!   `use ucx_rs as _;`) is dropped from the link along with every native
//!   library this crate emits; the failure only appears at dlopen time.
//! * **Do not add your own `-Wl,--version-script`** to a cdylib consumer:
//!   rustc supplies one, ld rejects a second, and rustc's already hides every
//!   UCX symbol (measured: a cdylib linking this crate exports zero
//!   `ucp_*`/`ucs_*`/`uct_*`/`ucm_*` symbols with no extra flags).
//! * **`ucp_init` is a static-inline** in `ucp.h` and does not exist in the
//!   archives; call [`sys::ucp_init_version`] with
//!   [`sys::UCP_API_MAJOR`]/[`sys::UCP_API_MINOR`].
//! * **`ucp_rkey_pack` is the only working rkey packer.** `ucp_memh_pack`
//!   without `UCP_MEMH_PACK_FLAG_EXPORT` aborts the process via `ucs_fatal`
//!   (verified against UCX 1.22.0).
//!
//! ## Licensing
//!
//! The crate's own code is Apache-2.0. The vendored UCX sources and the
//! statically-linked libraries are BSD-3-Clause; binary redistributions must
//! reproduce the UCX notice, available programmatically as [`UCX_LICENSE`].

#![cfg_attr(ucx_stub, allow(unused))]

/// Raw FFI bindings to the UCP/UCS API (bindgen output, checked in).
///
/// Regenerate with `UCX_RS_REGEN_BINDINGS=1 cargo build --features bindgen`
/// (requires libclang; the feature alone is deliberately a no-op so that
/// `--all-features` builds never rewrite tracked sources). The output is
/// written back to `src/bindings_linux_<arch>.rs` and must be committed. Both LP64 Linux targets currently produce identical bindings;
/// per-arch files are kept so a future divergence is a diff, not a mystery.
#[cfg(not(ucx_stub))]
#[allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    dead_code,
    unsafe_op_in_unsafe_fn
)]
pub mod sys {
    #[cfg(target_arch = "aarch64")]
    include!("bindings_linux_aarch64.rs");
    #[cfg(target_arch = "x86_64")]
    include!("bindings_linux_x86_64.rs");
}

/// The UCX version vendored inside this crate (informational; `UCX_DIR`
/// consumers may be running a different, `>= 1.17`, version).
pub const VENDORED_UCX_VERSION: &str = "1.22.0";

/// The UCX license text (BSD-3-Clause). Binary redistributions of anything
/// linking this crate must reproduce this notice in their documentation or
/// other materials (BSD-3 clause 2); embedding this constant in an
/// about/attribution surface satisfies that mechanically.
pub const UCX_LICENSE: &str = include_str!("../LICENSE-UCX");

/// Constructor forcing.
///
/// UCX registers its components (transports, memory domains, the UCS config
/// tables that `ucp_init` depends on) from ELF constructors. In a static link
/// the linker only pulls an archive member that some symbol references, so
/// without these references the constructors never run and `ucp_init` fails
/// with a bare `UCS_ERR_INVALID_PARAM` and **no log output at any level**
/// (the logging subsystem is itself registered from `ucs_init`).
///
/// `cargo:rustc-link-arg=-Wl,--undefined=...` cannot do this job — it does
/// not propagate past this crate's own (linkless) build. Real symbol
/// references from a `#[used]` static are the mechanism that works and that
/// survives `--gc-sections`. The symbol set mirrors the `Libs.private:`
/// markers in UCX's own shipped pkg-config files, one per component.
///
/// The order of the array below is *not* the constructor order and changing it
/// changes nothing: `.init_array` order comes from the order `build.rs` emits
/// its `cargo:rustc-link-lib=static=...` lines. That order is load-bearing for
/// `uct_ib` versus `uct_ib_mlx5` — see the comment at those two lines, and
/// `tests/ctor_order.rs`, which fails if they are ever swapped back.
/// Only meaningful for the vendored static build: with a shared system UCX
/// (`UCX_DIR`), the transport modules are runtime-dlopen'd plugins whose init
/// symbols are absent from the core libraries, and the core constructors run
/// automatically at load. Referencing module symbols there would break the
/// consumer's link.
#[cfg(all(not(ucx_stub), ucx_vendored))]
mod ctors {
    unsafe extern "C" {
        fn ucs_init();
        fn uct_init();
        fn ucp_global_init();
        #[cfg(feature = "ib")]
        fn uct_ib_init();
        #[cfg(feature = "ib")]
        fn uct_mlx5_init();
        #[cfg(feature = "rdmacm")]
        fn uct_rdmacm_init();
        #[cfg(feature = "cma")]
        fn uct_cma_init();
    }

    #[used]
    static UCX_CTORS: &[unsafe extern "C" fn()] = &[
        ucs_init,
        uct_init,
        ucp_global_init,
        #[cfg(feature = "ib")]
        uct_ib_init,
        #[cfg(feature = "ib")]
        uct_mlx5_init,
        #[cfg(feature = "rdmacm")]
        uct_rdmacm_init,
        #[cfg(feature = "cma")]
        uct_cma_init,
    ];
}

#[cfg(not(ucx_stub))]
mod helpers {
    use super::sys;
    use std::ffi::CStr;

    /// `ucs_status_string` as a safe `&'static str`.
    pub fn status_string(status: sys::ucs_status_t) -> &'static str {
        // SAFETY: ucs_status_string returns a pointer into a static table for
        // every input value (unknown values map to "Unknown error").
        unsafe {
            CStr::from_ptr(sys::ucs_status_string(status))
                .to_str()
                .unwrap_or("<non-utf8 ucs status>")
        }
    }

    /// Decode the tri-state `ucs_status_ptr_t` returned by every `*_nbx`
    /// operation.
    ///
    /// * `Ok(None)` — completed immediately (`NULL`); **the completion
    ///   callback will NOT be invoked**, even if one was supplied.
    /// * `Ok(Some(request))` — in progress; the callback fires exactly once,
    ///   after which the request must be released with `ucp_request_free`.
    /// * `Err(status)` — failed synchronously; no request, no callback.
    ///
    /// Which arm a given send takes is neither stable across sizes nor across
    /// UCX versions (measured: 1.19 completes inline at sizes where 1.22
    /// returns a request) — callers must handle all three, always.
    pub fn decode_status_ptr(
        ptr: sys::ucs_status_ptr_t,
    ) -> Result<Option<sys::ucs_status_ptr_t>, sys::ucs_status_t> {
        let v = ptr as isize;
        if v == 0 {
            Ok(None)
        } else if (-100..0).contains(&v) {
            // UCS_PTR_IS_ERR: error statuses are the range [-100, -1] packed
            // into the pointer value (UCS_ERR_LAST = -100).
            Err(v as sys::ucs_status_t)
        } else {
            Ok(Some(ptr))
        }
    }
}

#[cfg(not(ucx_stub))]
pub use helpers::{decode_status_ptr, status_string};
