<!--
SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Contributing to Velo

Thank you for your interest in contributing to Velo! Every contribution helps make Velo better for everyone.

**TL;DR:**

1. Fork and clone the repo
2. Create a branch: `git checkout -b yourname/fix-description`
3. Make changes, run `pre-commit`
4. Commit with DCO sign-off: `git commit -s -m "fix: description"`
5. Open a PR targeting `main`

## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (version pinned in `rust-toolchain.toml`)
- [pre-commit](https://pre-commit.com/#install)
- [protoc](https://grpc.io/docs/protoc-installation/) (for gRPC code generation)

### Setup

```bash
git clone https://github.com/ai-dynamo/velo.git
cd velo
pre-commit install
cargo build
cargo test
```

## Code Style & Quality

All code must pass the following checks before merging:

```bash
# Format code
cargo fmt

# Lint (must pass with zero warnings)
cargo clippy -- -D warnings

# License and dependency audit
cargo deny check licenses bans
```

### Pre-commit Hooks

Pre-commit hooks run `cargo fmt` automatically on every commit. Install them with:

```bash
pre-commit install
```

Run against all files manually:

```bash
pre-commit run --all-files
```

## Pull Request Guidelines

- Keep PRs focused — one concern per PR
- All CI checks must pass (formatting, clippy, tests, coverage)
- Code coverage must not decrease
- No compiler warnings
- All tests must pass

## velo-ext API stability

`velo-ext` is the published trait surface that out-of-tree implementors of `Transport`, `FrameTransport`, `PeerDiscovery`, and `ServiceDiscovery` compile against. Every change to it propagates to every downstream impl, so the bar is higher than for the runtime crate.

When changing `lib/velo-ext/`:

1. **New trait methods MUST land with default implementations.** Adding a bare method is a breaking change for every external impl — and a major-version bump while we are pre-1.0 means a coordinated `velo` release.
2. **Signature, bound, or parameter changes to existing trait methods are breaking** (minor bump pre-1.0). They require a `velo` bump in the same PR so the runtime's exact `=` pin tracks.
3. **Removing a trait, type, or public item is breaking** and requires a major bump.
4. **`velo`'s dep on `velo-ext` is an exact `=` pin in `[workspace.dependencies]`.** Bumping `velo-ext` without bumping the pin will break the workspace lockfile.
5. **The CI `semver:` job is the enforcement gate** — it runs `cargo semver-checks` per changed crate against `origin/main` and fails the build if a breaking change is not paired with a sufficient version bump. Use the `semver:skip` PR label only with explicit reviewer agreement.

Any other workspace crate (`velo-messenger`, `velo-transports`, `velo-streaming`, etc.) is marked `publish = false` and is internal to this repository. External consumers can only depend on `velo` and `velo-ext`, so internal API changes do not require version coordination.

## DCO & Licensing

### Developer Certificate of Origin

Velo requires all contributions to be signed off with the [Developer Certificate of Origin (DCO)](https://developercertificate.org/). This certifies that you have the right to submit your contribution under the project's [Apache 2.0 license](LICENSE).

Each commit must include a sign-off line:

```text
Signed-off-by: Your Name <your.email@example.com>
```

Add this automatically with the `-s` flag:

```bash
git commit -s -m "fix: your descriptive message"
```

**Requirements:**
- Use your real name (no pseudonyms or anonymous contributions)
- Your `user.name` and `user.email` must be configured in git

DCO check failed? See our [DCO Troubleshooting Guide](DCO.md).

### License

By contributing, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).
