# Versioning

`velo` and `velo-ext` are published. Out-of-tree code compiles against `velo-ext`, so every change to it reaches every downstream implementation. The rules below are the structural fix for the bug class in [Workspace crates](architecture.md). They are not optional.

## Rules

1. **Only `velo` and `velo-ext` publish.** A new workspace crate gets `publish = false`. `ucx-rs`, `velo-hub`, and `velo-hub-ext` are the documented exceptions. `velo-hub` has `publish = false` until a real port of kvbm's hub passes kvbm's lifecycle tests against it, and `velo-hub-ext` publishes together with it.
2. **`velo-ext` has an exact pin.** `[workspace.dependencies]` has `velo-ext = { path = "lib/velo-ext", version = "=X.Y.Z" }`. A caret requirement lets a later "compatible" patch change downstream lockfiles without notice. Do not change `=` to a caret.
3. **When `velo-ext` moves past its latest published version, `velo` must move past its own.** Update the pin to match.
4. **A new trait method in `velo-ext` needs a default implementation.** A method with no default breaks every external implementation. A defaulted addition is a patch release (`0.5.0` to `0.5.1`). Before 1.0, Cargo reads `0.y` as the breaking position.
5. **A change to a signature, bound, or parameter of a trait method is breaking.** Before 1.0, that is a minor bump (`0.5` to `0.6`), and `velo` must carry a bump over its latest published version too.
6. **Removing a public item from `velo-ext` is breaking.**
7. **A bump is measured against the latest published version.** Only versions on crates.io take part in Cargo's resolution. So `main` must carry a large enough bump over the latest published version for everything merged since that publish. Several breaking changes between two publishes share one bump. For example, if crates.io has `0.17.0` and `main` is at `0.18.0`, a breaking change stays at `0.18.0`. After the next publish, the first breaking change bumps again.

## velo-hub

`velo-hub-ext` is the wire protocol of the hub, on `velo-ext` alone. `velo-hub` re-exports it as `velo_hub::protocol`, so its types are part of the `velo-hub` API. The rules below cover both crates.

The API of `velo-hub` uses velo types (`Velo`, `PeerInfo`, axum `Router`). A move of `velo-hub` from velo 0.21 to velo 0.22 changes these types, so it is a breaking change of `velo-hub`, but its own source does not change. `cargo semver-checks` cannot see that. These rules make it safe:

1. `velo-hub` and `velo-hub-ext` use `version.workspace = true`. A velo minor bump is always a `velo-hub` and `velo-hub-ext` minor bump, and `velo-hub` X.Y works with velo X.Y.
2. `[workspace.dependencies]` requires velo with a caret on the workspace version: `velo = { path = "lib/velo", version = "0.21.0", default-features = false }`. Before 1.0, a caret on `0.21.0` means `>=0.21.0, <0.22.0`, so it never admits a second minor. An exact `=` pin would also stop a velo patch from reaching users of `velo-hub`. `velo-hub-ext` has the same kind of line: `velo-hub-ext = { path = "lib/velo-hub-ext", version = "0.21.0" }`.
3. `scripts/check-semver.sh` fails if the major.minor of either requirement is not the workspace version. This check runs before the `semver:skip` escape and before the script stops for a change with no crate changes.
4. velo, `velo-hub`, and `velo-hub-ext` publish together, from one commit.
5. `velo-hub` re-exports `velo`, `axum`, and `velo-hub-ext` (as `velo_hub::protocol`). Hub crates downstream use these re-exports, and require `velo-hub` with a caret. CI also checks that `velo-hub-ext` depends on no `velo` runtime and no `prometheus`.

## The CI gate

The `Semver Check` job runs `scripts/check-semver.sh`. For each crate that the change touches, the script reads the latest version of the crate from the crates.io index and runs `cargo semver-checks check-release` against it. It fails when a breaking change is not covered by the version bump over that published version. It skips a crate that was never published (the index answers 404). Any other answer from the index fails the gate: no connection, an error status, a page that is not an index entry, or a crate whose every version is yanked or a pre-release. The gate also fails a version below the latest published one, and a version with a pre-release suffix, even without a breaking change. Build metadata, such as the `+ucx.1.22.0` of `ucx-rs`, is ignored. A change to the root `Cargo.toml` also selects every crate that takes its version from it. The check is cumulative, so a breaking change that reached `main` without a bump fails the next change that touches that crate. The `semver:skip` PR label exists for emergencies. Use it only when a reviewer agrees.

To run the gate locally:

```bash
bash scripts/check-semver.sh
```

## Inside the workspace

While all callers of an item are in the workspace, the compiler finds them all. Change the interface, change the callers, and delete the old form. Do not keep a second entry point, a forwarding wrapper, or an alias for an old name.
