# Run rendezvous over NIXL

This guide builds velo with the `nixl` feature and uses a NIXL agent as the RDMA backend of rendezvous. For how the path works, see [Rendezvous and RDMA](../concepts/rendezvous.md#the-nixl-backend). For why it has this shape, see [RDMA design](../development/rdma-design.md#nixl-as-a-rendezvous-backend).

NIXL is not a messenger transport in velo. It moves rendezvous payloads only. Every message, including the rendezvous control messages, uses the transports the instance has (TCP, for example).

## Build

```toml
velo = { version = "...", features = ["nixl"] }
```

The build needs Linux, a C++ compiler and `libclang`: the `nixl-sys` crate runs `bindgen` and compiles a small C++ file in every build. It needs no NIXL library. `nixl-sys` is built with its `stub-api` feature, so the binary loads `libnixl_capi.so` when the first agent is created. A machine with NIXL installed and one without build the same binary.

## Give the process a NIXL library

velo is tested with NIXL 1.5.2. The NIXL wheels on PyPI ship the library, the plugins, and their own UCX:

```bash
pip install --no-deps nixl-cu12==1.5.2
L=<site-packages>/.nixl_cu12.mesonpy.libs
export LD_LIBRARY_PATH=$L
export NIXL_PLUGIN_DIR=$L/plugins
```

`--no-deps` leaves out `torch`, which the wheel lists and the library does not need. A NIXL built from source works the same way: put the directory with `libnixl_capi.so` on the loader path and point `NIXL_PLUGIN_DIR` at the plugins.

If the library is not found, `VeloBuilder::build` returns an error that names `libnixl_capi.so`. It does not abort.

## Wire it

```rust
use velo::{NixlRdmaConfig, Velo};

let velo = Velo::builder()
    .add_transport(tcp)                       // control plane
    .add_nixl_rdma(NixlRdmaConfig {
        num_workers: 4,
        ..Default::default()
    })
    .build()
    .await?;
```

After that the rendezvous and registration calls are the same as with UCX: `register_data_pinned`, `register_owned`, `register_data_in_region`, `get`, `get_pinned`, `get_into`, `alloc_pinned_writer`. `RdmaConfig` (`rdma_config`) tunes the pool and the leases for both backends.

An instance has one RDMA backend. `build` fails if `add_nixl_rdma` and `add_ucx_transport` are both used. Two instances use the RDMA path with each other only if both use the same backend. A NIXL instance and a UCX instance still exchange payloads, on the chunked path, and the owner counts `backend_mismatch`.

| `NixlRdmaConfig` field | Default | Effect |
|---|---|---|
| `num_workers` | 1 | UCX workers inside the agent. Each READ is given to one of them in turn. |
| `devices` | empty | Network devices for NIXL's UCX plugin (`mlx5_0:1,mlx5_1:1`). Empty leaves NIXL's default. |
| `spin` | 200 µs | How long velo's NIXL thread spins after a post or a completion before it sleeps between polls. |

A READ that runs longer than `spin` is noticed up to 1 ms after it ends. On two nodes with 400 Gb/s InfiniBand (2026-11-12), that made a 16 MiB READ take 26% longer and a 64 MiB READ 45% longer than with a spin that covers the READ. A `spin` of 5 ms removed both. The cost of a longer spin is one core that spins for that long after each READ.

### UCX settings

NIXL's UCX plugin reads the `UCX_*` environment, as any UCX does.

- **On a host with NVIDIA GPUs, `UCX_TLS` must include CUDA support.** NIXL refuses to create its backend for `UCX_TLS=tcp` there. Use `UCX_TLS=tcp,cuda_copy`.
- NIXL sets two RMA rails itself. `UCX_MAX_RMA_RAILS` in the environment wins over that. velo's own UCX transport leaves the UCX default of one.
- Set `UCX_RCACHE_ENABLE=n` and `UCX_MEM_EVENTS=n`, the defaults of velo's UCX transport, so that a deregistration unpins. velo also passes both to the plugin, but NIXL 1.5.2 accepts any key there, so nothing shows that they take effect.

## Check that the path runs

The chunked path returns the same bytes, so a correct transfer proves nothing. Read the counter on the consumer:

```text
velo_rendezvous_rdma_path_total{path="rdma",reason="ok"}
```

The two-process example prints it for both sides, and exits non-zero if the RDMA path was not taken:

```bash
cargo run --manifest-path examples/Cargo.toml --features nixl \
  --example rendezvous_rdma_two_proc -- --backend nixl --nixl-workers 2
```

For the tests, see [Testing](../development/testing.md).

## What velo cannot check

With UCX, velo parses a peer's key before UCX sees it. With NIXL it cannot: the two blobs in a descriptor are in NIXL's format. A peer that sends a bad descriptor can stop RDMA transfers from itself, and [can crash the consumer](../development/rdma-design.md#less-containment-than-ucx). Use the NIXL backend among peers that run the same NIXL version and that you trust to send correct descriptors. That is the same trust that registering memory already needs.

When NIXL refuses a load, velo stops using the RDMA path for that peer and logs it once. Transfers from the peer take the chunked path until the peer is unregistered and registered again.

## Common failures

| Symptom | Cause |
|---|---|
| `build` fails: `libnixl_capi.so was not found` | The library directory is not on the loader path. |
| `build` fails: `create_backend` | NIXL could not load its UCX plugin (`NIXL_PLUGIN_DIR`), or it refused `UCX_TLS` (GPU host, no `cuda_copy`). NIXL prints the reason on stderr. |
| Every transfer counts `get_failed` for one peer | NIXL refused a load for that peer. Look for the "a load for this peer was refused" log line. |
| The process crashes at exit, after a clean shutdown | The binary has both the `ucx` and the `nixl` feature and runs against the PyPI wheel. The process then holds two copies of `libnl-route`: the system's, through velo's UCX, and the wheel's. Build with `nixl` and without `ucx`. |
