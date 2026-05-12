// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! VRAM smoke test for velo-nixl.
//!
//! Mirrors `tests/smoke.rs::two_agent_dram_read` but allocates the source and
//! destination buffers on a CUDA device via `cudaMalloc`, registers them with
//! `MemType::Vram`, and verifies the round-trip after a NIXL_READ. Requires a
//! CUDA-aware UCX (`cuda_copy` / `cuda_ipc` transports) — fails at agent init
//! otherwise. Gated behind the `cuda-tests` feature.

use std::ffi::c_void;

use velo_nixl::{
    Agent, MemType, MemoryRegion, NixlDescriptor, NixlError, NixlRegistration, OptArgs,
    RegistrationHandle, XferDescList, XferOp, agent_with_ucx, wait_xfer,
};

#[link(name = "cudart")]
unsafe extern "C" {
    fn cudaSetDevice(device: i32) -> i32;
    fn cudaMalloc(dev_ptr: *mut *mut c_void, size: usize) -> i32;
    fn cudaFree(dev_ptr: *mut c_void) -> i32;
    fn cudaMemset(dev_ptr: *mut c_void, value: i32, count: usize) -> i32;
    fn cudaMemcpy(dst: *mut c_void, src: *const c_void, count: usize, kind: i32) -> i32;
    fn cudaDeviceSynchronize() -> i32;
}

const CUDA_MEMCPY_DEVICE_TO_HOST: i32 = 2;

#[derive(Debug)]
struct VramBuffer {
    ptr: *mut c_void,
    size: usize,
    device_id: u64,
    handle: Option<RegistrationHandle>,
}

// CUDA device pointers are usable from any host thread; the underlying device
// allocation is not stack-bound, and the test never aliases mutably across
// threads.
unsafe impl Send for VramBuffer {}
unsafe impl Sync for VramBuffer {}

impl VramBuffer {
    fn new(size: usize, device_id: u64) -> Self {
        unsafe { assert_eq!(cudaSetDevice(device_id as i32), 0, "cudaSetDevice failed") };
        let mut ptr: *mut c_void = std::ptr::null_mut();
        let rc = unsafe { cudaMalloc(&mut ptr as *mut *mut c_void, size) };
        assert_eq!(rc, 0, "cudaMalloc({size}) failed: {rc}");
        Self {
            ptr,
            size,
            device_id,
            handle: None,
        }
    }

    fn memset(&mut self, value: u8) {
        let rc = unsafe { cudaMemset(self.ptr, value as i32, self.size) };
        assert_eq!(rc, 0, "cudaMemset failed");
        let rc = unsafe { cudaDeviceSynchronize() };
        assert_eq!(rc, 0, "cudaDeviceSynchronize failed");
    }

    fn copy_to_host(&self, out: &mut [u8]) {
        assert_eq!(out.len(), self.size);
        let rc = unsafe {
            cudaMemcpy(
                out.as_mut_ptr() as *mut c_void,
                self.ptr,
                self.size,
                CUDA_MEMCPY_DEVICE_TO_HOST,
            )
        };
        assert_eq!(rc, 0, "cudaMemcpy D2H failed");
    }
}

impl Drop for VramBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { cudaFree(self.ptr) };
        }
    }
}

impl MemoryRegion for VramBuffer {
    fn size(&self) -> usize {
        self.size
    }

    unsafe fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }
}

impl NixlDescriptor for VramBuffer {
    fn mem_type(&self) -> MemType {
        MemType::Vram
    }

    fn device_id(&self) -> u64 {
        self.device_id
    }
}

impl NixlRegistration for VramBuffer {
    fn register(&mut self, agent: &Agent, opt_args: Option<&OptArgs>) -> Result<(), NixlError> {
        let h = agent.register_memory(self, opt_args)?;
        self.handle = Some(h);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_agent_vram_read() {
    const LEN: usize = 64 * 1024;

    let (a_src, _b_src, opts_src) = agent_with_ucx("velo-nixl-vram-src").unwrap();
    let (a_dst, _b_dst, opts_dst) = agent_with_ucx("velo-nixl-vram-dst").unwrap();

    let mut src = VramBuffer::new(LEN, 0);
    src.memset(0xCD);
    src.register(&a_src, Some(&opts_src))
        .expect("register VRAM src — UCX must be CUDA-aware");

    let mut dst = VramBuffer::new(LEN, 0);
    dst.memset(0x00);
    dst.register(&a_dst, Some(&opts_dst))
        .expect("register VRAM dst");

    let src_addr = unsafe { src.as_ptr() } as usize;
    let dst_addr = unsafe { dst.as_ptr() } as usize;

    let src_md = a_src.get_local_md().unwrap();
    let src_name = a_dst.load_remote_md(&src_md).unwrap();
    assert_eq!(src_name, "velo-nixl-vram-src");

    let mut local_descs = XferDescList::new(MemType::Vram).unwrap();
    local_descs.add_desc(dst_addr, LEN, 0);

    let mut remote_descs = XferDescList::new(MemType::Vram).unwrap();
    remote_descs.add_desc(src_addr, LEN, 0);

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

    let mut host = vec![0u8; LEN];
    dst.copy_to_host(&mut host);
    assert!(
        host.iter().all(|&b| b == 0xCD),
        "dst VRAM not filled with 0xCD after NIXL_READ — first byte: 0x{:x}",
        host.first().copied().unwrap_or(0)
    );

    a_dst.invalidate_remote_md(&src_name).ok();
}
