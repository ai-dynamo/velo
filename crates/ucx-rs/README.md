# ucx-rs

Rust bindings for [UCX](https://openucx.org) (Unified Communication X), with a
vendored static build of the UCX libraries.

This crate embeds the **unmodified UCX 1.22.0 release tarball**
(`vendor/ucx-1.22.0.tar.gz`, SHA-256 pinned in `Cargo.toml` under
`package.metadata.velo`). By default `build.rs` extracts it, builds it
`--enable-static --with-pic` (InfiniBand/RoCE via the system rdma-core when the
`ib` feature is on), and statically absorbs the archives into the consumer:
no UCX shared objects, plugin directories, or environment configuration exist
at runtime. Cold build is ~25 s on a 20-core machine (~73 CPU-seconds); warm
rebuilds are ~0.3 s via a configure-args stamp.

Set `UCX_DIR=/path/to/ucx` to link a preinstalled UCX (>= 1.17) instead.

## Features

| feature  | default | effect |
|----------|---------|--------|
| `ib`     | yes     | InfiniBand/RoCE transports. **Hard-fails** if rdma-core headers (`libibverbs-dev`, `libmlx5`) are absent — UCX's own configure only warns and would silently produce a TCP-only build. |
| `rdmacm` | yes     | RDMA connection manager (needs `librdmacm-dev`). |
| `mt`     | yes     | `--enable-mt`. SINGLE-mode workers stay lock-free; this only makes MULTI usable. |
| `cma`    | no      | Cross-memory-attach shared memory transport. |
| `bindgen`| no      | Regenerate the checked-in bindings: `UCX_RS_REGEN_BINDINGS=1 cargo build --features bindgen` (needs libclang). The feature alone is a no-op, so `--all-features` CI never rewrites tracked sources. |

CI without RDMA packages should build with `--no-default-features` (TCP and
shared-memory transports are always compiled in), or install
`libibverbs-dev librdmacm-dev` — headers only, no hardware needed.

## Scope

The bound surface is the UCP layer needed by
[velo](https://github.com/ai-dynamo/velo): context/worker/endpoint lifecycle,
Active Messages, RMA, wakeup, flush/close. Raw bindings are in `ucx_rs::sys`;
coverage grows as needed. See the crate docs for the consumer invariants
(constructor forcing, `ucp_init_version`, `ucp_rkey_pack`).

## License

Crate code: Apache-2.0. The vendored UCX sources and statically-linked
libraries: BSD-3-Clause (`LICENSE-UCX`). Binary redistributions must reproduce
the UCX notice; `ucx_rs::UCX_LICENSE` exposes it programmatically.
