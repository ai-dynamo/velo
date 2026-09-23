# Versioning

`velo` and `velo-ext` are published. Out-of-tree code compiles against `velo-ext`, so every change to it reaches every downstream implementation. The rules below are the structural fix for the bug class in [Workspace crates](architecture.md). They are not optional.

## Rules

1. **Only `velo` and `velo-ext` publish.** A new workspace crate gets `publish = false`. `ucx-rs` is the one documented exception.
2. **`velo-ext` has an exact pin.** `[workspace.dependencies]` has `velo-ext = { path = "lib/velo-ext", version = "=X.Y.Z" }`. A caret requirement lets a later "compatible" patch change downstream lockfiles without notice. Do not change `=` to a caret.
3. **A `velo-ext` bump needs a `velo` bump in the same PR.** Update the pin to match.
4. **A new trait method in `velo-ext` needs a default implementation.** A method with no default breaks every external implementation. A defaulted addition is a patch release (`0.5.0` to `0.5.1`). Before 1.0, Cargo reads `0.y` as the breaking position.
5. **A change to a signature, bound, or parameter of a trait method is breaking.** Before 1.0, that is a minor bump (`0.5` to `0.6`), with a `velo` bump in the same PR.
6. **Removing a public item from `velo-ext` is breaking.**

## The CI gate

The `Semver Check` job runs `scripts/check-semver.sh`. The script runs `cargo semver-checks check-release` for each crate against `origin/main`. It fails when a breaking change does not have a large enough version bump. The `semver:skip` PR label exists for emergencies. Use it only when a reviewer agrees.

To run the gate locally:

```bash
bash scripts/check-semver.sh
```

## Inside the workspace

While all callers of an item are in the workspace, the compiler finds them all. Change the interface, change the callers, and delete the old form. Do not keep a second entry point, a forwarding wrapper, or an alias for an old name.
