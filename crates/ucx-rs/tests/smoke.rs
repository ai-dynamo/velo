// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Link-and-run smoke test.
//!
//! This is the test that catches the two silent static-link failure modes:
//! a missing constructor reference (ucp_init returns UCS_ERR_INVALID_PARAM
//! with zero log output) and a dropped archive (undefined symbols at load).
//! It needs no RDMA hardware — the tcp/shm/self transports are compiled in
//! unconditionally.

#![cfg(not(ucx_stub))]

use std::mem::MaybeUninit;
use ucx_rs::sys;

#[test]
fn ucp_init_worker_address_roundtrip() {
    unsafe {
        // Context: AM + RMA + WAKEUP, the velo feature set. ucp_init is a
        // static inline in ucp.h — ucp_init_version is the real entry point.
        let mut params: sys::ucp_params_t = MaybeUninit::zeroed().assume_init();
        params.field_mask = (sys::ucp_params_field_UCP_PARAM_FIELD_FEATURES
            | sys::ucp_params_field_UCP_PARAM_FIELD_MT_WORKERS_SHARED)
            as u64;
        params.features = (sys::ucp_feature_UCP_FEATURE_AM
            | sys::ucp_feature_UCP_FEATURE_RMA
            | sys::ucp_feature_UCP_FEATURE_WAKEUP) as u64;
        params.mt_workers_shared = 1;

        let mut ctx: sys::ucp_context_h = std::ptr::null_mut();
        let st = sys::ucp_init_version(
            sys::UCP_API_MAJOR,
            sys::UCP_API_MINOR,
            &params,
            std::ptr::null(),
            &mut ctx,
        );
        assert_eq!(
            st,
            sys::ucs_status_t_UCS_OK,
            "ucp_init failed: {} — if this is UCS_ERR_INVALID_PARAM with no \
             UCX log output, a constructor reference is missing (see ctors in \
             ucx-rs/src/lib.rs)",
            ucx_rs::status_string(st)
        );

        // Worker in SINGLE mode (lock-free even in an --enable-mt build).
        let mut wparams: sys::ucp_worker_params_t = MaybeUninit::zeroed().assume_init();
        wparams.field_mask = sys::ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
        wparams.thread_mode = sys::ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE;

        let mut worker: sys::ucp_worker_h = std::ptr::null_mut();
        let st = sys::ucp_worker_create(ctx, &wparams, &mut worker);
        assert_eq!(
            st,
            sys::ucs_status_t_UCS_OK,
            "{}",
            ucx_rs::status_string(st)
        );

        // Worker address — what velo publishes through discovery.
        let mut attr: sys::ucp_worker_attr_t = MaybeUninit::zeroed().assume_init();
        attr.field_mask = sys::ucp_worker_attr_field_UCP_WORKER_ATTR_FIELD_ADDRESS as u64;
        let st = sys::ucp_worker_query(worker, &mut attr);
        assert_eq!(
            st,
            sys::ucs_status_t_UCS_OK,
            "{}",
            ucx_rs::status_string(st)
        );
        assert!(!attr.address.is_null());
        assert!(attr.address_length > 0, "empty worker address");
        sys::ucp_worker_release_address(worker, attr.address);

        // Wakeup surface — the fd velo's progress thread polls.
        let mut efd: std::os::raw::c_int = -1;
        let st = sys::ucp_worker_get_efd(worker, &mut efd);
        assert_eq!(
            st,
            sys::ucs_status_t_UCS_OK,
            "{}",
            ucx_rs::status_string(st)
        );
        assert!(efd >= 0);

        sys::ucp_worker_destroy(worker);
        sys::ucp_cleanup(ctx);
    }
}

#[test]
fn status_helpers() {
    assert_eq!(ucx_rs::status_string(sys::ucs_status_t_UCS_OK), "Success");
    assert!(ucx_rs::decode_status_ptr(std::ptr::null_mut()).is_ok());
    let err = -2isize as ucx_rs::sys::ucs_status_ptr_t; // UCS_ERR_NO_RESOURCE
    assert_eq!(
        ucx_rs::decode_status_ptr(err),
        Err(sys::ucs_status_t_UCS_ERR_NO_RESOURCE)
    );
}
