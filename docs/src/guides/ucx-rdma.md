# Run rendezvous over RDMA

This guide builds velo with the `ucx` feature and wires the UCX transport for the rendezvous RDMA path. It then verifies that the path runs on the NIC you expect. For how the path works, see [Rendezvous and RDMA](../concepts/rendezvous.md).

## What you need

- Linux on x86_64 or aarch64. The `ucx` feature does not exist on other platforms.
- A C compiler and GNU `make`. The vendored UCX tarball ships a generated `configure`, so autotools and `cmake` are not necessary for this crate.
- The rdma-core development headers. The build looks for three files: `infiniband/verbs.h`, `infiniband/mlx5dv.h`, and `rdma/rdma_cma.h`. On Debian and Ubuntu, `libibverbs-dev` and `librdmacm-dev` supply them.
- For real RDMA: an InfiniBand or RoCE port in state `PORT_ACTIVE`, and a memlock limit large enough for the registered-bytes budget.

The headers are necessary even for a build that only runs over `UCX_TLS=tcp`. The UCX tests and CI run that way with no RDMA hardware.

## Build

1. Install the headers:

   ```bash
   sudo apt-get install -y libibverbs-dev librdmacm-dev
   ```

2. Turn on the feature in your `Cargo.toml`:

   ```toml
   velo = { version = "...", features = ["ucx"] }
   ```

3. Build:

   ```bash
   cargo build --features ucx
   ```

The first build compiles UCX 1.22.0 from the tarball inside `crates/ucx-rs`. This takes about 25 s on a 20-core machine (about 73 CPU-seconds). Later builds reuse it in about 0.3 s. A stamp file records the exact `configure` arguments and toolchain variables, so a change to either rebuilds UCX.

The result links UCX statically. At run time, no UCX shared objects, plugin directories, or `LD_LIBRARY_PATH` settings exist. The only dynamic dependencies are `libibverbs.so.1`, `libmlx5.so.1`, and `librdmacm.so.1`.

### Why the build refuses a TCP-only UCX

If the verbs headers are missing, UCX's own `configure` prints a warning and builds a UCX with only TCP and shared memory. That build passes CI and then runs production traffic over TCP. `ucx-rs` refuses instead, with a panic that names the missing header.

For the same reason, `ucx-rs` passes `--with-devx`. Upstream defaults DEVX to `check`, which silently drops the accelerated mlx5 memory domain.

### Build variables

| Variable | Effect |
|---|---|
| `CPATH`, `C_INCLUDE_PATH` | Extra header directories for the header check. |
| `UCX_RS_IGNORE_MISSING_HEADERS` | Skip the header check, for a non-standard prefix that `configure` still finds. |
| `UCX_DIR=/opt/ucx` | Link a preinstalled UCX 1.17 or later instead of the tarball. It needs a shared `libucp.so`. The transport modules are then UCX's own plugins. |
| `UCX_EXTRA_CONFIGURE` | Extra `configure` arguments, appended last. The build refuses `--without-devx`, `--with-devx=no`, and `--with-devx=check`, because the last option wins. |
| `CC_<target>` or `CC` | The cross compiler. For a cross build with no cross compiler, the build stops. Without this check, `configure` silently builds for the host. |

The `velo` feature `ucx` turns on the `ucx-rs` default features: `ib`, `rdmacm`, and `mt`. You cannot turn off `ib` through `velo`.

## Wire the transport

Add UCX with `add_ucx_transport`, not `add_transport`. Only `add_ucx_transport` creates the RDMA registry. Keep a TCP transport beside it for the control plane.

```rust
use std::sync::Arc;
use velo::transports::tcp::TcpTransportBuilder;
use velo::transports::ucx::UcxTransportBuilder;
use velo::{RdmaConfig, Velo};

let tcp = Arc::new(
    TcpTransportBuilder::new()
        .from_listener(std::net::TcpListener::bind("0.0.0.0:0")?)?
        .build()?,
);
// Eager wireup moves the ~14 ms first-GET endpoint cost to register().
let ucx = Arc::new(UcxTransportBuilder::new().eager_endpoints(true).build()?);

let velo = Velo::builder()
    .add_transport(tcp)
    .add_ucx_transport(ucx)
    .rdma_config(RdmaConfig::default())
    .build()
    .await?;

// Owner: stage in registered memory. This call also maps the first arena.
let handle = velo.register_data_pinned(&payload).await;
```

The consumer calls `get`, `get_pinned`, or `get_into` as usual. Register the peers before the first `get`. A `get` to an owner that is not registered yet takes the chunked path.

If the only staging in a process is transparent (large messenger payloads), call `register_data_pinned` once at startup. The transparent stager never maps an arena, so without that call it never uses the RDMA path.

## Set the runtime environment

Velo reads the UCX variables from the environment. **The environment wins over the builder.** `UcxTransportBuilder::tls` and `net_devices` apply only when `UCX_TLS` or `UCX_NET_DEVICES` is not set.

| Variable | Value | Why |
|---|---|---|
| `UCX_NET_DEVICES` | the port, for example `mlx5_2:1` | Pins the NIC and port. Do not let UCX choose. |
| `UCX_TLS` | `rc_mlx5,ud_mlx5,self` or `rc_verbs,ud_verbs,self` | Pins the transport. An RC-only list cannot wire up. See the note below this table. |
| `UCX_PROTO_INFO` | `y` | Prints the protocol table, with the device for each range. |
| `UCX_LOG_LEVEL` | `debug` | Shows which memory domain opened. |
| `UCX_IB_MLX5_DEVX` | `y` | Forces the DEVX memory domain open. See the failures table. |
| `VELO_RDMA_RENDEZVOUS_DISABLE` | `1` | Turns the RDMA path off with no rebuild. |

NOTE: No hardware run has used velo's build on `rc_mlx5` yet. All two-node results so far are on `rc_verbs` (see [RDMA performance](../operations/rdma-performance.md)). On `rc_mlx5`, do all steps in [Verify the lane](#verify-the-lane).

Velo sets `UCX_RCACHE_ENABLE=n` and `UCX_MEM_EVENTS=n` unless you set them. With these values, UCX patches no libc functions in the process. Keep them. Velo registers memory explicitly and never uses the UCX registration cache.

Do not set `UCX_PROTO_ENABLE=n`. It is a common workaround for protocol-v2 faults, and it silently disables all RMA. A multi-rail GET needs `UCX_MAX_RMA_RAILS` of 2 or more. The default is 1.

### Prepare each node

1. Find the port:

   ```bash
   ibv_devinfo
   ```

2. Pick a port with `state: PORT_ACTIVE` and `link_layer: InfiniBand`. An active Ethernet port on the same card carries RoCE and looks like a success.
3. Set `UCX_NET_DEVICES` to that port, for example `mlx5_2:1`.
4. Set `UCX_TLS`. Include a `ud` transport beside the `rc` transport.
5. Raise the memlock limit:

   ```bash
   ulimit -l unlimited
   ```

6. If the IB partition gives this node partial pkey membership (`0x7fff`), set `UCX_IB_PKEY` explicitly. `UCX_IB_PKEY=auto` needs full membership, and raw verbs works where UCX fails.

Some systems ship a memlock limit of 8 MiB. With that limit, `ibv_reg_mr` fails at 32 MiB, and the first 64 MiB arena cannot map.

## Verify the lane

A correct transfer does not prove that RDMA ran. The chunked path also returns correct bytes. Do all three steps.

1. Run with `UCX_LOG_LEVEL=debug` and find the memory domain line:

   ```bash
   UCX_LOG_LEVEL=debug ./your-binary 2>&1 | grep 'md open by'
   ```

   On a mlx5 NIC, the line must say `uct_ib_mlx5_devx_md_ops`. If it says `uct_ib_verbs_md_ops`, the accelerated transports have no devices.

2. Run with `UCX_PROTO_INFO=y`. Find the row `rendezvous zero-copy read from remote`. It must name the transport and device that you set, for example `rc_mlx5/mlx5_2:1`.

3. Read `velo_rendezvous_rdma_path_total` on the consumer. The series `{path="rdma", reason="ok"}` must increase with each transfer. Any other reason names why the path was not used. See [Rendezvous and RDMA](../concepts/rendezvous.md#when-the-owner-answers-rdma).

Do not use the system `ucx_info` to verify velo's build. The system UCX loads its transports as plugins and is a different library. On the same NIC, the system UCX can report `rc_mlx5` while velo's static build does not.

Do not trust a configuration header that a program prints. The environment overrides the builder, so a header that says `UCX_TLS=tcp` can describe a run on InfiniBand. The `UCX_PROTO_INFO` table is the truth.

## Run the two-process example

The example stages 8 MiB on an owner and pulls it on a consumer, with a TCP control plane and UCX beside it. The consumer exits non-zero if the transfer did not take the RDMA path.

On one host, over TCP:

```bash
UCX_TLS=tcp cargo run --manifest-path examples/Cargo.toml --release --features ucx --example rendezvous_rdma_two_proc
```

With no `--role`, the example starts both roles as child processes.

On two nodes:

1. Build on both nodes:

   ```bash
   cargo build --manifest-path examples/Cargo.toml --release --features ucx --example rendezvous_rdma_two_proc
   ```

2. Pick a directory that both nodes can read and write, for example `/shared/rv`.
3. Start the owner on node A:

   ```bash
   UCX_NET_DEVICES=mlx5_2:1 UCX_TLS=rc_mlx5,ud_mlx5,self \
     ./examples/target/release/examples/rendezvous_rdma_two_proc --role owner --dir /shared/rv
   ```

4. Start the consumer on node B, with the same variables and `--role consumer`.
5. Read the `velo_rendezvous_rdma_path_total` lines that both sides print at the end.

Use `--size <bytes>` to change the payload. The two processes find each other through JSON files in `--dir`. On NFS, negative-dentry caching can delay that exchange by about 30 s. That delay is not a velo latency.

CI runs the single-host form with `UCX_TLS=tcp` on every pull request.

## Run the tests

The UCX suites set `tls("tcp")` in code. The environment wins, so the same suites run on InfiniBand when `UCX_TLS` and `UCX_NET_DEVICES` are set.

```bash
# Rendezvous RDMA path, end to end
timeout 300 cargo test -p velo --features ucx,test-helpers --test rendezvous_rdma

# UCX transport and RMA plumbing
timeout 300 cargo test -p velo --features ucx,test-helpers --lib transports::ucx

# Registration layer
timeout 300 cargo test -p velo --features ucx,test-helpers --lib rendezvous::rdma

# The mlx5 memory domain sits ahead of the verbs one
timeout 300 cargo test -p ucx-rs --test ctor_order

# Registration and GET cost (prints numbers, asserts nothing)
timeout 300 cargo test -p velo --features ucx,test-helpers --lib bench_rma -- --ignored --nocapture
```

CAUTION: Do not run `unusable_rkey_is_refused_by_ucx_over_tcp` on InfiniBand. It aborts the process with `SIGABRT` there. Over TCP it passes.

Skip it on an InfiniBand run:

```bash
timeout 300 cargo test -p velo --features ucx,test-helpers --lib transports::ucx -- --skip unusable_rkey_is_refused_by_ucx_over_tcp
```

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| `UCX WARN transports 'rc_mlx5','ud_mlx5' are not available`, and transfers report `chunked` | The mlx5 memory domain did not open. The verbs domain opened in its place. | Rebuild with the current `ucx-rs`. On an old binary, set `UCX_IB_MLX5_DEVX=y`. |
| `md open by 'uct_ib_verbs_md_ops'` on a mlx5 NIC | Same as above. | Same as above. Run `cargo test -p ucx-rs --test ctor_order`. |
| `no auxiliary transport ... Destination is unreachable` | `UCX_TLS` names an RC transport with no `ud` transport. | Add `ud_mlx5` or `ud_verbs`. |
| `ucp_init: ... InvalidParam` with no UCX log output | A UCX constructor did not run. The logging subsystem registers from a constructor, so nothing logs. | Make sure that the binary references `ucx_rs` (`use ucx_rs as _;` in a crate that links it directly). |
| Build panic: `feature \`ib\` is enabled but <infiniband/verbs.h> was not found` | The rdma-core headers are missing. | Install `libibverbs-dev` and `librdmacm-dev`. |
| `configure` fails: `devx requested but not found` | The rdma-core on the build host has no DEVX support. | Install a newer rdma-core. |
| `undefined symbol: __aarch64_ldclr4_sync` (aarch64) | The linker did not find the static `libgcc.a`. | Install the `libgcc-<version>-dev` package. |
| `relocation R_AARCH64_ADR_PREL_PG_HI21 ... recompile with -fPIC` | A UCX built without `--with-pic` went into a shared object. | Use the vendored build, or rebuild the system UCX with `--with-pic`. |
| `anonymous version tag cannot be combined with other version tags` | A cdylib added its own `--version-script`. rustc already supplies one. | Remove the extra script. rustc's script already hides every UCX symbol. |
| Consumer reports `no_offer` | The owner was not registered on the consumer yet (first `get`), or UCX is not registered for the owner. | Register the peer before the first `get`. Make sure that the owner advertises a UCX address. |
| Owner reports `no_offer` | The consumer sent no offer: it has no UCX, its kill switch is on, or this was the fallback acquire after a failed GET. | Read the consumer's own `path_total` reasons. |
| Either side reports `kill_switch` | `VELO_RDMA_RENDEZVOUS_DISABLE` is set to an affirmative value, or `RdmaRendezvousConfig::enabled` is `false`. | Unset the variable and restart. |
| `not_configured` | UCX was added with `add_transport`. | Use `add_ucx_transport`. |
| `pool_exhausted` on transparent sends | No arena is mapped. | Call `register_data_pinned` once at startup. |
| `budget` | The registered-bytes budget is spent. | Raise `RdmaPoolConfig::registered_bytes_budget`, or set `arena_reclaim_after`. |
| `ibv_reg_mr` fails at 32 MiB | The memlock limit is 8 MiB. | Set `ulimit -l unlimited`, or grant `IPC_LOCK` in a container. |
| Process abort: `rc_verbs_impl.h:104 Fatal: receive completion ... with error` | A GET used an unusable or stale rkey on InfiniBand. | Report it. The single-use rkey rule was broken, or the TCP-only test above ran on InfiniBand. |
| `ucp_ep.c:2222 UCX ERROR ep ... has already been closed` at teardown on InfiniBand | A double close during teardown. Assertions still pass. | None. This is known log noise. |
| `tcp_ep ... recv(-1) failed: Input/output error` on a peer | The endpoint idle reaper closed an endpoint. | None, if `ep_idle_timeout` is on. That peer's Messages and pings to this side are lost until keepalive fails its endpoint (about 20 s). See [Endpoint idle reaper](../concepts/rendezvous.md#endpoint-idle-reaper). |
