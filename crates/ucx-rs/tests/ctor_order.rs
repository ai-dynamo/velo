// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The mlx5 memory domain must not sit behind the verbs one.
//!
//! `uct_ib_init` and `uct_mlx5_init` both register their memory domains onto
//! `uct_ib_ops` with `ucs_list_add_head`, so whichever constructor runs *later*
//! owns the head of the list. `uct_ib_component_md_open` takes the first entry
//! that opens, and the verbs domain opens for anything — so a verbs head means
//! every mlx5 NIC comes up on a plain verbs domain and `rc_mlx5`, `dc_mlx5` and
//! `ud_mlx5` report zero devices. UCX warns that the transports are
//! unavailable and velo silently falls back to its chunked path.
//!
//! Constructor order is archive order, and archive order is the order
//! `build.rs` emits `cargo:rustc-link-lib=static=...`. That makes two adjacent
//! lines in a build script load-bearing for whether RDMA is accelerated, which
//! is exactly the kind of thing a later tidy-up reorders. Hence this test.
//!
//! It reads the registration list itself rather than decoding `.init_array`:
//! the list is what `uct_ib_component_md_open` actually consumes, and reading
//! it needs no binutils, no ELF parsing and no relocation assumptions. Test
//! bodies run after `.init_array`, so the list is complete by the time we look.

// Vendored static build only. With a shared system UCX the transport modules
// are dlopen'd plugins and `uct_ib_ops` is populated by the loader instead.
#![cfg(all(not(ucx_stub), ucx_vendored, feature = "ib"))]

use std::ffi::{CStr, c_char, c_void};

/// `ucs_list_link_t` — `src/ucs/datastruct/list.h:32`.
#[repr(C)]
struct UcsListLink {
    prev: *mut UcsListLink,
    next: *mut UcsListLink,
}

/// `uct_ib_md_ops_entry_t` — `src/uct/ib/base/ib_md.h:250`. The list link
/// leads the struct, so a node pointer is also an entry pointer.
#[repr(C)]
struct IbMdOpsEntry {
    list: UcsListLink,
    name: *const c_char,
    ops: *mut c_void,
}

unsafe extern "C" {
    static uct_ib_ops: UcsListLink;
}

/// Every memory domain the vendored build can register. An unrecognised name
/// means the struct layout drifted and the rest of this test is meaningless.
const KNOWN: &[&str] = &[
    "uct_ib_mlx5_devx_md_ops",
    "uct_ib_mlx5_md_ops",
    "uct_ib_verbs_md_ops",
];

fn md_ops_order() -> Vec<String> {
    let head: *const UcsListLink = &raw const uct_ib_ops;
    let mut out = Vec::new();
    let mut node = unsafe { (*head).next };
    while !node.is_null() && node.cast_const() != head {
        assert!(
            out.len() < 16,
            "uct_ib_ops did not terminate within 16 nodes — layout drift"
        );
        let entry = node.cast_const().cast::<IbMdOpsEntry>();
        out.push(
            unsafe { CStr::from_ptr((*entry).name) }
                .to_string_lossy()
                .into_owned(),
        );
        node = unsafe { (*node).next };
    }
    out
}

#[test]
fn verbs_md_is_not_at_the_head_of_uct_ib_ops() {
    // Forces the rlib — and with it the bundled UCX archives and their
    // constructors — into this test binary's link. Without a real call the
    // externs above do not resolve.
    assert_eq!(ucx_rs::status_string(0), "Success");

    let names = md_ops_order();
    assert!(
        !names.is_empty(),
        "uct_ib_ops is empty: no IB memory domain registered at all"
    );
    for n in &names {
        assert!(
            KNOWN.contains(&n.as_str()),
            "unknown MD entry {n:?} — layout drift. list = {names:?}"
        );
    }
    assert!(
        names.iter().any(|n| n == "uct_ib_verbs_md_ops"),
        "verbs MD absent: {names:?}"
    );
    assert!(
        names.len() >= 2,
        "only one MD registered — the mlx5 module did not register: {names:?}"
    );
    assert_ne!(
        names[0].as_str(),
        "uct_ib_verbs_md_ops",
        "uct_mlx5_init ran before uct_ib_init, so the verbs domain owns the head of \
         uct_ib_ops and wins every open — mlx5 NICs will expose no rc_mlx5/dc_mlx5/ud_mlx5. \
         Emit uct_ib before uct_ib_mlx5 in build.rs. list = {names:?}"
    );
    // The DEVX domain specifically, not merely "something that is not verbs".
    // `uct_mlx5_init` registers `dv` and then `devx`, both onto the head, so on
    // a DEVX build the accelerated domain is what a correct order leaves in
    // front. Accepting the plain `uct_ib_mlx5_md_ops` here would let a build
    // that quietly lost DEVX pass while still answering "not verbs" — which is
    // the same silent degradation in a different place. Comparing the name as a
    // string keeps this free at link time: nothing here references a symbol
    // that only exists under `HAVE_DEVX`.
    assert_eq!(
        names[0].as_str(),
        "uct_ib_mlx5_devx_md_ops",
        "the head of uct_ib_ops is not the DEVX memory domain, so this build lost DEVX \
         even though build.rs asks for it. Check that nothing in UCX_EXTRA_CONFIGURE \
         disabled it. list = {names:?}"
    );
}
