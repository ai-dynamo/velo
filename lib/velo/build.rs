// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `velo_rdma`: the backend-neutral RDMA layer (the registry, the arena
    // pool, regions, and the rendezvous GET path) is compiled when an RDMA
    // backend is. Today that is the `ucx` feature, on Linux.
    //
    // Read from the environment, not `cfg!`: a build script is compiled for
    // the host, and these describe the target.
    let linux = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");
    let backend = std::env::var_os("CARGO_FEATURE_UCX").is_some();
    if linux && backend {
        println!("cargo:rustc-cfg=velo_rdma");
    }

    // Only compile proto files if grpc feature is enabled
    #[cfg(feature = "grpc")]
    {
        tonic_build::configure()
            .bytes(["velo.streaming.v1.FramedData"])
            .compile_protos(&["proto/velo.proto"], &["proto"])?;
    }
    Ok(())
}
