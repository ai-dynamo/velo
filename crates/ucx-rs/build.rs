//! ucx-rs — Rust bindings + vendored static build of UCX 1.22.0 for velo.
//!
//! Contract:
//!   * `links = "ucx-rs"` in Cargo.toml, so exactly one crate in the graph may
//!     own the native UCX libraries; downstream crates read `DEP_UCX_RS_*`.
//!     NOT `links = "ucx"` — `lamellar-ucx-sys` 0.1.0 already claims that key on
//!     crates.io (verified), and cargo hard-errors on a collision.
//!   * Emits every link flag needed to statically absorb UCX into the consumer,
//!     including the `-Wl,--undefined=` constructor forcing that the shipped
//!     `.pc` files document. Miss one of those and UCX links but mis-initialises.
//!
//! Escape hatch: set `UCX_DIR=/opt/ucx` to link a preinstalled UCX (>= 1.17)
//! instead of building the vendored tarball.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const UCX_VERSION: &str = "1.22.0";
const TARBALL: &str = "vendor/ucx-1.22.0.tar.gz";
/// Floor for the API surface velo uses: the reworked AM API
/// (`ucp_am_send_nbx`, `ucp_am_recv_data_nbx`, `UCP_AM_RECV_ATTR_FLAG_RNDV`)
/// landed in UCX 1.10.0; we require 1.17.0 for the fixes on top of it.
const MIN_SYSTEM_UCX: (u32, u32) = (1, 17);

fn main() {
    // ---- fingerprint: rebuild UCX only when these change -------------------
    println!("cargo::rustc-check-cfg=cfg(ucx_stub)");
    println!("cargo::rustc-check-cfg=cfg(ucx_vendored)");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={TARBALL}");
    for v in [
        "UCX_DIR",
        "UCX_EXTRA_CONFIGURE",
        "UCX_RS_REGEN_BINDINGS",
        "CC",
        "CFLAGS",
        "CXXFLAGS",
        "AR",
        "RANLIB",
        "STRIP",
    ] {
        println!("cargo:rerun-if-env-changed={v}");
    }

    let docsrs = env::var_os("DOCS_RS").is_some();
    let want_ib = cfg!(feature = "ib");
    let want_rdmacm = cfg!(feature = "rdmacm");

    if docsrs {
        // docs.rs: no network (fine — the tarball ships in the crate) but also
        // no rdma-core headers and a hard build timeout. Emit nothing; the
        // downstream `-sys` crate must be `#[cfg(docsrs)]`-stubbed.
        println!("cargo:rustc-cfg=ucx_stub");
        println!("cargo:include=");
        return;
    }

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());

    let (include_dir, lib_dir, module_dir, vendored, version) = match env::var("UCX_DIR") {
        Ok(dir) => {
            let dir = PathBuf::from(dir);
            let v = check_system_version(&dir);
            // Multiarch installs use lib64 (or a triplet dir); take the first
            // libdir that holds a SHARED libucp. The UCX_DIR path links the
            // core libraries dynamically (transport modules are then UCX's
            // own dlopen'd plugins); a static-only system install would need
            // the vendored-style archive link list and is not supported.
            let libdir = [
                "lib",
                "lib64",
                "lib/x86_64-linux-gnu",
                "lib/aarch64-linux-gnu",
            ]
            .iter()
            .map(|l| dir.join(l))
            .find(|d| d.join("libucp.so").exists())
            .unwrap_or_else(|| {
                panic!(
                    "ucx-rs: UCX_DIR={} has no shared libucp.so under lib/ or lib64/ \
                     (static-only installs are not supported via UCX_DIR; unset it to \
                     use the vendored static build)",
                    dir.display()
                )
            });
            let moddir = libdir.join("ucx");
            (dir.join("include"), libdir, moddir, false, v)
        }
        Err(_) => {
            let prefix = build_vendored(&out, want_ib, want_rdmacm);
            (
                prefix.join("include"),
                prefix.join("lib"),
                prefix.join("lib/ucx"),
                true,
                UCX_VERSION.to_string(),
            )
        }
    };

    if vendored {
        // Gates the module-constructor references in src/lib.rs: with a
        // shared system UCX, the transport modules are dlopen'd plugins whose
        // init symbols are NOT in the core libraries — referencing them would
        // fail the consumer's link.
        println!("cargo:rustc-cfg=ucx_vendored");
    }
    emit_link_flags(&lib_dir, &module_dir, vendored, want_ib, want_rdmacm);

    // ---- DEP_UCX_RS_* for the downstream -sys crate ---------------------------
    println!("cargo:include={}", include_dir.display());
    println!("cargo:lib={}", lib_dir.display());
    println!("cargo:version={version}");
    println!("cargo:vendored={}", vendored as u8);
    println!("cargo:has_ib={}", want_ib as u8);

    // --all-features CI runs enable the `bindgen` feature; regeneration must
    // still be an explicit developer action, because it rewrites tracked
    // source files and requires libclang.
    #[cfg(feature = "bindgen")]
    if env::var_os("UCX_RS_REGEN_BINDINGS").is_some() {
        regenerate_bindings(&include_dir);
    } else {
        println!(
            "cargo:warning=ucx-rs: `bindgen` feature enabled but UCX_RS_REGEN_BINDINGS is not \
             set; skipping bindings regeneration"
        );
    }
}

/// Verify the vendored tarball against the pinned digest before executing
/// anything from it — the pin in Cargo.toml metadata is enforced here.
/// Returns the digest so it can be folded into the rebuild stamp.
fn verify_tarball_sha256(path: &Path) -> String {
    use sha2::Digest;
    const PINNED: &str = "258941cddd14ca60d38c0d31b9b09ec1052c901086841011a498da8b55a3cb24";
    let bytes = fs::read(path).expect("read vendored UCX tarball");
    let digest = format!("{:x}", sha2::Sha256::digest(&bytes));
    assert_eq!(
        digest, PINNED,
        "ucx-rs: vendored UCX tarball digest mismatch — refusing to build an \
         unexpected tarball"
    );
    digest
}

/// Extract + configure + per-subdir make. Returns the install prefix.
fn build_vendored(out: &Path, want_ib: bool, want_rdmacm: bool) -> PathBuf {
    let src = out.join(format!("ucx-{UCX_VERSION}"));
    let build = out.join("ucx-build");
    let prefix = out.join("ucx-install");

    // A completed marker makes an interrupted build restart cleanly and lets a
    // warm OUT_DIR skip the whole 4-minute step.
    // The stamp is what makes an incremental build cheap: touching build.rs
    // ALWAYS recompiles and re-runs the build script, so `rerun-if-changed`
    // alone would not spare the 25 s / 73 CPU-s C build (measured). The stamp
    // records the exact configure line, so a change to the flags does force a
    // rebuild while an unrelated edit does not.
    let stamp = prefix.join(".velo-ucx-stamp");

    // ---- the pinned flag set ------------------------------------------------
    // `--with-pic` is LOAD-BEARING. `--enable-static --disable-shared` alone
    // makes libtool emit non-PIC objects; linking those into any cdylib fails on
    // aarch64 with "relocation R_AARCH64_ADR_PREL_PG_HI21 ... recompile with
    // -fPIC" (measured), which breaks every PyO3 extension module (Dynamo).
    let mut args: Vec<String> = vec![
        format!("--prefix={}", prefix.display()),
        "--enable-static".into(),
        "--disable-shared".into(),
        "--with-pic".into(),
        "--enable-optimizations".into(),
        "--disable-debug".into(),
        "--disable-assertions".into(),
        "--disable-params-check".into(),
        "--disable-doxygen-doc".into(),
        // NB: --disable-numa was dropped upstream; 1.22 warns "unrecognized option"...
        // Everything we do not ship. Each --without- cuts build time and drops
        // a transitive .so dependency from the final binary.
        "--without-cuda".into(),
        "--without-rocm".into(),
        "--without-java".into(),
        "--without-go".into(),
        "--without-fuse3".into(),
        "--without-xpmem".into(),
        "--without-knem".into(),
        "--without-gdrcopy".into(),
    ];
    // NOTE: we deliberately do NOT pass --disable-logging. It only lowers
    // UCS_MAX_LOG_LEVEL to DEBUG (measured: config.h keeps
    // `UCS_MAX_LOG_LEVEL UCS_LOG_LEVEL_DEBUG`), and UCX_LOG_LEVEL=debug is the
    // only field-diagnosable thing about a link that half-initialised.

    if cfg!(feature = "mt") {
        args.push("--enable-mt".into()); // UCS_THREAD_MODE_MULTI; off by default upstream
    }
    if want_ib {
        // configure only WARNs when verbs headers are missing (see
        // src/uct/ib/configure.m4: `AC_MSG_WARN([ibverbs header files not
        // found]); with_ib=no`) and then silently produces a tcp/shm-only UCX.
        // We refuse to let that reach a green CI run.
        require_header("infiniband/verbs.h", "ib", "rdma-core / libibverbs-dev");
        require_header("infiniband/mlx5dv.h", "ib", "libmlx5 / rdma-core");
        args.push("--with-verbs".into());
        args.push("--with-mlx5".into());
        // Upstream defaults `--with-devx` to `check`, which degrades to a
        // DEVX-less mlx5 build with no error. Asking for it explicitly makes
        // configure hard-fail (`src/uct/ib/configure.m4:213-214`, "devx
        // requested but not found") rather than silently dropping the
        // accelerated memory domain this crate exists to provide.
        args.push("--with-devx".into());
    } else {
        args.push("--without-verbs".into());
    }
    if want_rdmacm {
        require_header("rdma/rdma_cma.h", "rdmacm", "librdmacm-dev / rdma-core");
        args.push("--with-rdmacm".into());
    } else {
        args.push("--without-rdmacm".into());
    }
    args.push("--without-efa".into());

    // Cross-compilation. UCX's bundled config.sub accepts rustc triples verbatim
    // (verified: aarch64/x86_64 x gnu/musl all round-trip unchanged), so no
    // triple translation is needed — pass TARGET/HOST straight through.
    //
    // The trap: with only --host set and no cross toolchain in PATH, configure
    // prints `WARNING: using cross tools not prefixed with host triplet`, falls
    // back to the NATIVE cc, and still exits 0 (verified on this aarch64 box
    // targeting x86_64). You then get archives for the wrong architecture and a
    // link error hundreds of lines later. Refuse instead.
    let mut cross_env: Vec<(&str, String)> = Vec::new();
    let (host, target) = (env::var("HOST").unwrap(), env::var("TARGET").unwrap());
    if host != target {
        let cc = target_cc();
        assert!(
            which(&cc),
            "ucx-rs: cross build {host} -> {target} but no cross C compiler was \n\
             found (`{cc}`). Set CC_{} (or CC) to the cross gcc; UCX's configure \n\
             would otherwise silently build for the host architecture.",
            target.replace('-', "_")
        );
        args.push(format!("--host={target}"));
        args.push(format!("--build={host}"));
        // AR/RANLIB must match, or libtool builds archives the target ld cannot
        // read. Passed on the Command (below) rather than via set_var, which is
        // `unsafe` in edition 2024.
        for (var, suffix) in [("AR", "ar"), ("RANLIB", "ranlib"), ("STRIP", "strip")] {
            let tool = format!("{target}-{suffix}");
            if env::var(var).is_err() && which(&tool) {
                cross_env.push((var, tool));
            }
        }
        cross_env.push(("CC", cc));
    }
    if let Ok(extra) = env::var("UCX_EXTRA_CONFIGURE") {
        args.extend(extra.split_whitespace().map(str::to_owned));
    }

    // ---- warm-OUT_DIR short circuit ----------------------------------------
    // The stamp must cover everything that changes the produced objects: the
    // configure argv AND the toolchain environment configure consumes.
    let env_fingerprint: String = ["CC", "CFLAGS", "CXXFLAGS", "AR", "RANLIB", "STRIP"]
        .iter()
        .map(|v| format!("{v}={};", env::var(v).unwrap_or_default()))
        .collect();
    // The tarball is verified (and its digest folded into the stamp) BEFORE
    // the warm short-circuit, so a changed tarball can neither reuse stale
    // artifacts nor slip past the pinned-digest check.
    let tarball_path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join(TARBALL);
    let tarball_digest = verify_tarball_sha256(&tarball_path);
    let want = format!(
        "{} :: {env_fingerprint} :: tarball={tarball_digest}",
        args.join(" ")
    );
    if fs::read_to_string(&stamp).is_ok_and(|s| s == want) {
        return prefix;
    }

    // A directory without the marker is a partially extracted tree from an
    // interrupted build — remove and re-extract rather than wedging forever.
    let extracted_marker = out.join(format!(".extracted-{UCX_VERSION}"));
    if !extracted_marker.exists() {
        if src.exists() {
            let _ = fs::remove_dir_all(&src);
        }
        let f = fs::File::open(&tarball_path).expect("vendored UCX tarball missing");
        tar::Archive::new(flate2::read::GzDecoder::new(f))
            .unpack(out)
            .expect("failed to unpack UCX tarball");
        fs::write(&extracted_marker, "ok").unwrap();
    }
    fs::create_dir_all(&build).unwrap();

    // -Wno-error: UCX's release tarball still trips newer GCC/Clang warnings.
    // -fPIC belongs to --with-pic, not here, so libtool tags objects correctly.
    let mut cmd = Command::new(src.join("configure"));
    cmd.current_dir(&build)
        .args(&args)
        .env(
            "CFLAGS",
            format!("{} -Wno-error", env::var("CFLAGS").unwrap_or_default()),
        )
        .env("CXXFLAGS", "-Wno-error")
        .envs(cross_env.iter().map(|(k, v)| (*k, v.as_str())));
    run(&mut cmd, "configure");

    // The top-level `make` is broken under --disable-shared (it tries to link
    // the tools against .so targets that were never built). Build the four
    // library subdirs individually — that is the whole of what we ship.
    let jobs = env::var("NUM_JOBS").unwrap_or_else(|_| "4".into());
    for sub in ["src/ucm", "src/ucs", "src/uct", "src/ucp"] {
        run(
            Command::new("make")
                .current_dir(&build)
                .args(["-C", sub, "-j", &jobs]),
            sub,
        );
        run(
            Command::new("make")
                .current_dir(&build)
                .args(["-C", sub, "install"]),
            sub,
        );
    }

    fs::write(&stamp, &want).unwrap();
    prefix
}

/// Emit `cargo:rustc-link-*`. Order matters more than usual here.
///
/// CRITICAL: `cargo:rustc-link-arg` does NOT propagate to dependent crates —
/// it only affects link steps of *this* package (measured: a downstream cdylib
/// linked with zero `-lucp` and failed with `undefined symbol: ucs_status_string`).
/// Only `rustc-link-lib` / `rustc-link-search` cross the crate boundary. So the
/// archives go out as `link-lib`, and the constructor forcing that used to be
/// `-Wl,--undefined=` is expressed instead as real symbol references from
/// `src/lib.rs` (see `UCX_CTORS`), which the linker resolves out of the archives
/// and which survive `--gc-sections` because the static is `#[used]`.
fn emit_link_flags(lib: &Path, modules: &Path, vendored: bool, ib: bool, rdmacm: bool) {
    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-search=native={}", modules.display());

    if !vendored {
        for l in ["ucp", "uct", "ucs", "ucm"] {
            println!("cargo:rustc-link-lib=dylib={l}");
        }
        return;
    }

    // ORDER IS LOAD-BEARING between these two, and not for symbol resolution.
    //
    // `static=` bundles each archive's members into this crate's rlib in
    // emission order, so this order becomes `.init_array` order. `uct_ib_init`
    // and `uct_mlx5_init` both register their memory domains with
    // `ucs_list_add_head`, so whichever runs *later* owns the head of
    // `uct_ib_ops`. `uct_ib_verbs_md_open` accepts any device it can open — it
    // refuses only when DEVX is forced (`ib_md.c:1549`) — so a verbs head wins
    // every open, and `rc_mlx5`/`dc_mlx5`/`ud_mlx5` then find zero devices.
    // `uct_ib_component_md_open` (`ib_md.c:1090-1101`) continues only on
    // `UCS_ERR_UNSUPPORTED`, so a verbs IO error aborts the device's MD open
    // outright rather than falling through to mlx5.
    //
    // Emitting `uct_ib` first therefore puts mlx5 at the head, which is what we
    // want. Repeating `uct_ib` defensively does NOT work: rustc collapses a
    // repeated archive to its last position, reproducing the bug.
    // `+whole-archive`/`+verbatim` change inclusion and name resolution, not
    // member position, so they do not help either.
    //
    // `tests/ctor_order.rs` is the guard. Do not reorder these two lines.
    if ib {
        println!("cargo:rustc-link-lib=static=uct_ib");
        println!("cargo:rustc-link-lib=static=uct_ib_mlx5");
    }
    if rdmacm {
        println!("cargo:rustc-link-lib=static=uct_rdmacm");
    }
    if cfg!(feature = "cma") {
        println!("cargo:rustc-link-lib=static=uct_cma");
    }
    for l in ["ucp", "uct", "ucs", "ucm", "ucs_signal"] {
        println!("cargo:rustc-link-lib=static={l}");
    }

    if ib {
        println!("cargo:rustc-link-lib=dylib=ibverbs");
        println!("cargo:rustc-link-lib=dylib=mlx5");
    }
    if rdmacm {
        println!("cargo:rustc-link-lib=dylib=rdmacm");
    }
    for l in ["m", "dl", "rt"] {
        println!("cargo:rustc-link-lib=dylib={l}");
    }

    // Two ordering repairs. rustc emits `-nodefaultlibs` and places its own
    // `-lgcc_s ... -lc` ahead of build-script libs, so undefined symbols in the
    // UCX archives cannot reach them. Both were measured as hard dlopen failures:
    //   aarch64: `undefined symbol: __aarch64_ldclr4_sync` — the outline-atomic
    //            helpers exist only in the static libgcc.a, not libgcc_s.so.1
    //   all:     `undefined symbol: pthread_atfork` — a libc_nonshared.a stub
    if env::var("CARGO_CFG_TARGET_ARCH").as_deref() == Ok("aarch64") {
        // Ask the TARGET compiler (CC_<target>, then CC, then cc) so a cross
        // build never adds the host's libgcc directory ahead of `-lgcc`.
        let cc = target_cc();
        let mut found = false;
        if let Ok(o) = Command::new(&cc).arg("-print-libgcc-file-name").output()
            && o.status.success()
        {
            let out = String::from_utf8_lossy(&o.stdout).trim().to_string();
            // clang may answer with its compiler-rt builtins archive instead;
            // only trust an actual libgcc.a.
            if out.ends_with("libgcc.a")
                && Path::new(&out).exists()
                && let Some((dir, _)) = out.rsplit_once('/')
            {
                println!("cargo:rustc-link-search=native={dir}");
                found = true;
            }
        }
        if !found {
            // Fall back to the GCC installation directories directly (clang
            // wrappers such as sccache-cc do not always answer usefully).
            let triple_dirs = [
                "/usr/lib/gcc/aarch64-linux-gnu",
                "/usr/lib/gcc/aarch64-unknown-linux-gnu",
            ];
            'search: for base in triple_dirs {
                if let Ok(entries) = fs::read_dir(base) {
                    let mut versions: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
                    versions.sort();
                    for v in versions.iter().rev() {
                        if v.join("libgcc.a").exists() {
                            println!("cargo:rustc-link-search=native={}", v.display());
                            found = true;
                            break 'search;
                        }
                    }
                }
            }
        }
        if !found {
            println!(
                "cargo:warning=ucx-rs: libgcc.a not located; aarch64 outline-atomic helpers \
                 may be unresolved at link time (install libgcc-*-dev)"
            );
        }
        println!("cargo:rustc-link-lib=static=gcc");
    }
    println!("cargo:rustc-link-lib=dylib=c");

    // Do NOT add `-Wl,--version-script=...` in any consumer: rustc already
    // supplies one for cdylib targets and ld rejects a second with "anonymous
    // version tag cannot be combined with other version tags" (measured).
    // Rustc's own script already hides every UCX symbol: a cdylib linking this
    // crate exported exactly 1 dynamic symbol and 0 ucp_/ucs_/uct_/ucm_ symbols
    // (measured), with 3756 of them present as LOCAL. `-Wl,--exclude-libs,ALL`
    // is redundant; it also links cleanly and keeps constructors firing.
}

fn require_header(header: &str, feature: &str, package: &str) {
    if env::var_os("UCX_RS_IGNORE_MISSING_HEADERS").is_some() {
        return; // operator override for non-standard prefixes
    }
    let mut dirs: Vec<PathBuf> = ["/usr/include", "/usr/local/include"]
        .iter()
        .map(PathBuf::from)
        .collect();
    for var in ["CPATH", "C_INCLUDE_PATH"] {
        if let Ok(v) = env::var(var) {
            dirs.extend(v.split(':').filter(|p| !p.is_empty()).map(PathBuf::from));
        }
    }
    if dirs.iter().any(|d| d.join(header).exists()) {
        return;
    }
    panic!(
        "ucx-rs: feature `{feature}` is enabled but <{header}> was not found.\n\
         Install {package}, or build with --no-default-features to get a \n\
         TCP/shared-memory-only UCX. Note that UCX's own configure only WARNS \n\
         about this and would otherwise silently produce a UCX with no \n\
         InfiniBand/RoCE transport at all."
    );
}

fn check_system_version(dir: &Path) -> String {
    let hdr = dir.join("include/ucp/api/ucp_version.h");
    let txt = fs::read_to_string(&hdr)
        .unwrap_or_else(|e| panic!("UCX_DIR={} has no {}: {e}", dir.display(), hdr.display()));
    let grab = |k: &str| -> u32 {
        txt.lines()
            .find(|l| l.contains(&format!("#define {k}")))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    let (maj, min) = (grab("UCP_API_MAJOR"), grab("UCP_API_MINOR"));
    if (maj, min) < MIN_SYSTEM_UCX {
        panic!(
            "ucx-rs: UCX_DIR points at UCX {maj}.{min}, but velo needs \
             >= {}.{}. The reworked Active Message API (ucp_am_send_nbx / \
             ucp_am_recv_data_nbx / UCP_AM_RECV_ATTR_FLAG_RNDV) first appeared \
             in 1.10.0; 1.17.0 is our supported floor.",
            MIN_SYSTEM_UCX.0, MIN_SYSTEM_UCX.1
        );
    }
    println!(
        "cargo:warning=ucx-rs: using system UCX {maj}.{min} from {}",
        dir.display()
    );
    format!("{maj}.{min}")
}

/// The C compiler for the build TARGET: `CC_<target>` (cargo convention),
/// then `CC`, then the default cross/native name.
fn target_cc() -> String {
    let (host, target) = (env::var("HOST").unwrap(), env::var("TARGET").unwrap());
    env::var(format!("CC_{}", target.replace('-', "_")))
        .or_else(|_| env::var("CC"))
        .unwrap_or_else(|_| {
            if host == target {
                "cc".into()
            } else {
                format!("{target}-gcc")
            }
        })
}

fn which(prog: &str) -> bool {
    Command::new(prog)
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}

fn run(cmd: &mut Command, what: &str) {
    let program = cmd.get_program().to_string_lossy().into_owned();
    let st = cmd.status().unwrap_or_else(|e| {
        panic!(
            "ucx-rs: could not spawn `{program}` for {what}: {e}. The vendored UCX \
             build needs a C compiler and GNU `make` on PATH (the release tarball \
             ships a pre-generated configure, so autoconf/automake/libtool are NOT \
             required)."
        )
    });
    assert!(st.success(), "ucx-rs: {what} failed ({st})");
}

#[cfg(feature = "bindgen")]
fn regenerate_bindings(include: &Path) {
    // Checked-in bindings are the default (`src/bindings_linux_{arch}.rs`);
    // this path exists only to refresh them and needs libclang.
    let out = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("src");
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    // `ucp_compat.h` is pulled in by ucp.h and matters: `ucp_rkey_pack` — the
    // ONLY working rkey packer (`ucp_memh_pack` ucs_fatal's without the EXPORT
    // flag) — is declared there, not in ucp.h.
    bindgen::Builder::default()
        .header_contents(
            "ucx.h",
            "#include <ucp/api/ucp.h>\n#include <ucs/type/status.h>\n",
        )
        .clang_arg(format!("-I{}", include.display()))
        .allowlist_function("ucp_.*|ucs_status_string")
        .allowlist_type("ucp_.*|ucs_.*")
        .allowlist_var("ucp_.*|ucs_.*|UCP_.*|UCS_.*")
        .derive_default(true)
        .layout_tests(true)
        .generate()
        .expect("bindgen")
        .write_to_file(out.join(format!("bindings_linux_{arch}.rs")))
        .expect("write bindings");
}
