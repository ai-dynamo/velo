// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Smoke tests for the velo-nixl wrapper.
//!
//! These exercise the full UCX/CMA pipeline: agent + backend creation,
//! memory registration, metadata exchange, and a same-process two-agent
//! NIXL_READ. Mirrors the structure of `nixl/examples/cpp/nixl_example.cpp`.

use velo_nixl::{
    MemType, NixlRegistration, SystemStorage, XferDescList, XferOp, agent_with_ucx, wait_xfer,
};

#[tokio::test(flavor = "multi_thread")]
async fn agent_create_destroy() {
    let (_agent, _backend, _opt_args) = agent_with_ucx("velo-nixl-smoke-1")
        .expect("agent_with_ucx failed (is libnixl + UCX plugin available?)");
    // dropped automatically — verify clean teardown
}

#[tokio::test(flavor = "multi_thread")]
async fn two_agent_dram_read() {
    // Two agents in the same process — same pattern as nixl_example.cpp.
    let (a_src, _b_src, opts_src) = agent_with_ucx("velo-nixl-smoke-src").unwrap();
    let (a_dst, _b_dst, opts_dst) = agent_with_ucx("velo-nixl-smoke-dst").unwrap();

    // Allocate and register source buffer (filled with 0xAB).
    const LEN: usize = 64 * 1024; // 64 KiB
    let mut src = SystemStorage::new(LEN).unwrap();
    src.memset(0xAB);
    src.register(&a_src, Some(&opts_src)).unwrap();

    // Allocate and register destination buffer (zeroed).
    let mut dst = SystemStorage::new(LEN).unwrap();
    dst.register(&a_dst, Some(&opts_dst)).unwrap();

    // Capture addresses (the storage objects own the buffers; the dst slice
    // gets verified after the transfer).
    let src_addr = src.as_slice().as_ptr() as usize;
    let dst_addr = dst.as_slice().as_ptr() as usize;

    // Exchange metadata: dst loads src's MD so it can READ from src's memory.
    let src_md = a_src.get_local_md().unwrap();
    let src_name = a_dst.load_remote_md(&src_md).unwrap();
    assert_eq!(src_name, "velo-nixl-smoke-src");

    // Build descriptor lists.
    let mut local_descs = XferDescList::new(MemType::Dram).unwrap();
    local_descs.add_desc(dst_addr, LEN, 0);

    let mut remote_descs = XferDescList::new(MemType::Dram).unwrap();
    remote_descs.add_desc(src_addr, LEN, 0);

    // dst agent reads from src agent's memory.
    let req = a_dst
        .create_xfer_req(
            XferOp::Read,
            &local_descs,
            &remote_descs,
            &src_name,
            Some(&opts_dst),
        )
        .unwrap();

    let _in_progress = a_dst.post_xfer_req(&req, Some(&opts_dst)).unwrap();
    wait_xfer(&a_dst, &req).await.unwrap();

    // Verify every byte was copied.
    let dst_slice = dst.as_slice();
    assert_eq!(dst_slice.len(), LEN);
    assert!(
        dst_slice.iter().all(|&b| b == 0xAB),
        "dst buffer not filled with 0xAB after NIXL_READ"
    );

    // Cleanup remote MD before drop (avoids spurious warnings).
    a_dst.invalidate_remote_md(&src_name).ok();
}
