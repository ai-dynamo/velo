#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# End-to-end test for scripts/check-semver.sh's version extraction.
#
# check-semver.sh runs top-to-bottom (it is not sourceable), and the defect
# under test is about how two git refs interact — a workspace-inherited
# version resolved from the wrong tree is indistinguishable, from a single
# string in isolation, from one resolved correctly. So this drives the real
# script against a throwaway git fixture repo rather than unit-testing its
# functions, and stubs `cargo` on PATH so it needs no network and no real
# cargo-semver-checks install.
#
# Usage: bash scripts/check-semver.test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK_SEMVER="${SCRIPT_DIR}/check-semver.sh"

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

pass_count=0
fail_count=0

report() {
    local name="$1" ok="$2" detail="$3"
    if [[ "$ok" == "0" ]]; then
        echo "ok   - $name"
        pass_count=$((pass_count + 1))
    else
        echo "FAIL - $name"
        echo "$detail" | sed 's/^/       /'
        fail_count=$((fail_count + 1))
    fi
}

# ── Stub cargo on PATH ────────────────────────────────────────────────────
# check-semver.sh calls the standalone `cargo-semver-checks` binary once (to
# check its installed version) and `cargo semver-checks check-release`
# separately. Stub both so the test needs neither a real cargo install nor a
# real cargo-semver-checks binary, and controls whether a "breaking change"
# is reported without needing one to exist in the fixture crate's source.
STUB_BIN="$TMP_ROOT/bin"
mkdir -p "$STUB_BIN"

cat > "$STUB_BIN/cargo-semver-checks" <<'EOF'
#!/usr/bin/env bash
echo "cargo-semver-checks 0.47.0"
EOF
chmod +x "$STUB_BIN/cargo-semver-checks"

cat > "$STUB_BIN/cargo" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == "semver-checks" ]]; then
    echo "--- failure some_lint: fixture-forced breaking change ---"
    exit 1
fi
echo "unexpected cargo invocation: $*" >&2
exit 1
EOF
chmod +x "$STUB_BIN/cargo"

export PATH="$STUB_BIN:$PATH"

GIT="git -c user.email=test@example.com -c user.name=test -c commit.gpgsign=false"

# ── Fixture repo builder ───────────────────────────────────────────────────
# Lays down a workspace shaped like velo's: a virtual root manifest with
# [workspace.package] version, one crate that inherits it
# (version.workspace = true, mirrors lib/velo), and one crate with a literal
# version (mirrors lib/velo-ext). $1: root version, $2: literal-crate
# version, $3: commit message, $4: a marker written into lib/velo/src/lib.rs
# so consecutive commits always produce a real diff there — this is what
# makes check-semver.sh select "velo" as a changed crate.
write_fixture_commit() {
    local root_version="$1" literal_version="$2" message="$3" marker="$4"

    cat > Cargo.toml <<EOF
[workspace]
members = ["lib/velo", "lib/velo-ext"]
resolver = "3"

[workspace.package]
version = "${root_version}"
edition = "2024"
EOF

    mkdir -p lib/velo/src lib/velo-ext/src
    cat > lib/velo/Cargo.toml <<'EOF'
[package]
name = "velo"
version.workspace = true
edition.workspace = true
EOF
    echo "pub fn touch() {} // ${marker}" > lib/velo/src/lib.rs

    cat > lib/velo-ext/Cargo.toml <<EOF
[package]
name = "velo-ext"
version = "${literal_version}"
edition = "2024"
EOF
    echo "pub fn touch() {} // ${marker}" > lib/velo-ext/src/lib.rs

    git add -A
    $GIT commit -q -m "$message"
}

new_fixture_repo() {
    local dir="$1"
    mkdir -p "$dir"
    (cd "$dir" && git init -q -b main)
}

# ── (a) workspace-inherited version resolves correctly ─────────────────────
# ── (b) literal version still resolves ──────────────────────────────────────
# One fixture covers both: velo inherits from [workspace.package], velo-ext
# is literal. A real breaking change is forced (via the cargo stub) with an
# insufficient bump (0.10.0 -> 0.10.0, no change), so the script must reach
# the "BREAKING CHANGES without sufficient version bump" report — which it
# can only do if both extractions succeeded.
{
    repo="$TMP_ROOT/repo-ab"
    new_fixture_repo "$repo"
    (
        cd "$repo"
        write_fixture_commit "0.10.0" "0.5.0" "base" "base"
        base_sha=$(git rev-parse HEAD)
        write_fixture_commit "0.10.0" "0.5.0" "pr, no bump" "pr"

        set +e
        out=$(BASE_REF="$base_sha" bash "$CHECK_SEMVER" 2>&1)
        exit_code=$?
        set -e
        echo "$out" > "$TMP_ROOT/ab.out"
        echo "$exit_code" > "$TMP_ROOT/ab.exit"
    )
}
out=$(cat "$TMP_ROOT/ab.out")
exit_code=$(cat "$TMP_ROOT/ab.exit")

ok=1
if [[ "$exit_code" == "1" ]] \
    && echo "$out" | grep -qF 'version on' \
    && echo "$out" | grep -qF '0.10.0' \
    && ! echo "$out" | grep -qi 'unbound variable' \
    && ! echo "$out" | grep -qi 'workspace = true' \
    && echo "$out" | grep -qF 'Checking velo-ext' \
    && echo "$out" | grep -qF '0.5.0'; then
    ok=0
fi
report "(a)+(b) workspace-inherited and literal versions both resolve (no unbound-variable crash, no raw 'workspace = true' string)" "$ok" "$out"

# ── (c) baseline arm reads BASE_REF's root manifest, not the worktree's ────
# Bump the root [workspace.package] version between the base commit and the
# PR commit. The correct implementation reports base=0.10.0, pr=0.11.0. The
# bug this guards against — reading the worktree's root Cargo.toml for BOTH
# sides — would report base=0.11.0, pr=0.11.0 instead: identical values, so
# no bump ever looks insufficient and the gate goes vacuous exactly when a
# real bump is missing on the base side.
{
    repo="$TMP_ROOT/repo-c"
    new_fixture_repo "$repo"
    (
        cd "$repo"
        write_fixture_commit "0.10.0" "0.5.0" "base" "base"
        base_sha=$(git rev-parse HEAD)
        write_fixture_commit "0.11.0" "0.6.0" "pr, root version bumped" "pr"

        set +e
        out=$(BASE_REF="$base_sha" bash "$CHECK_SEMVER" 2>&1)
        exit_code=$?
        set -e
        echo "$out" > "$TMP_ROOT/c.out"
        echo "$exit_code" > "$TMP_ROOT/c.exit"
    )
}
out=$(cat "$TMP_ROOT/c.out")

# Pre-1.0, 0.10.0 -> 0.11.0 is a sufficient (minor) bump, so this run must
# pass cleanly — but only a correct extraction proves that: the buggy
# same-tree-for-both-sides reading (base=0.11.0, pr=0.11.0) would report
# bump_type=none and WRONGLY fail this as insufficient. So this case asserts
# the correct exit code (0) rather than inspecting version strings in a
# failure report that a correct run never produces.
ok=1
if [[ "$(cat "$TMP_ROOT/c.exit")" == "0" ]] \
    && echo "$out" | grep -qF 'version bumped 0.10.0 -> 0.11.0' \
    && echo "$out" | grep -qF 'version bumped 0.5.0 -> 0.6.0'; then
    ok=0
fi
report "(c) baseline arm reads BASE_REF's root manifest (base=0.10.0), not the worktree's HEAD manifest (which would wrongly read base=0.11.0 too)" "$ok" "$out"

# ── (d) an unparseable version fails loudly ─────────────────────────────────
# A root manifest whose [workspace.package] version cargo itself would
# reject (build-metadata that parse_version's naive `.`-split cannot handle)
# must produce a clear ::error:: and a clean exit 1 — never an "unbound
# variable" or "invalid arithmetic operator" abort, and never a silent pass.
{
    repo="$TMP_ROOT/repo-d"
    new_fixture_repo "$repo"
    (
        cd "$repo"
        write_fixture_commit "0.10.0" "0.5.0" "base" "base"
        base_sha=$(git rev-parse HEAD)
        write_fixture_commit "0.10" "0.5.0" "pr, unparseable version" "pr"

        set +e
        out=$(BASE_REF="$base_sha" bash "$CHECK_SEMVER" 2>&1)
        exit_code=$?
        set -e
        echo "$out" > "$TMP_ROOT/d.out"
        echo "$exit_code" > "$TMP_ROOT/d.exit"
    )
}
out=$(cat "$TMP_ROOT/d.out")
exit_code=$(cat "$TMP_ROOT/d.exit")

ok=1
if [[ "$exit_code" == "1" ]] \
    && echo "$out" | grep -qi '::error::Could not parse version' \
    && ! echo "$out" | grep -qi 'unbound variable' \
    && ! echo "$out" | grep -qi 'invalid arithmetic operator'; then
    ok=0
fi
report "(d) unparseable version fails loudly with ::error:: and exit 1 (not an unbound-variable / arithmetic abort)" "$ok" "$out"

# ── (e) `+build` metadata compares on the semver core ──────────────────────
# crates/ucx-rs pins `0.1.0+ucx.1.22.0`: the metadata records the vendored UCX
# release and, per semver 2.0 §10, takes no part in precedence. A correctly
# bumped ucx-rs release must PASS the gate. Rejecting the whole string as
# unparseable would make every future ucx-rs breaking change unsatisfiable —
# and `semver:skip` is not even a label on this repo.
{
    repo="$TMP_ROOT/repo-e"
    mkdir -p "$repo"
    (cd "$repo" && git init -q -b main)
    (
        cd "$repo"
        write_ucx_commit() {
            mkdir -p crates/ucx-rs/src
            printf '[workspace]\nmembers = ["crates/ucx-rs"]\nresolver = "3"\n\n[workspace.package]\nversion = "0.10.0"\nedition = "2024"\n' > Cargo.toml
            printf '[package]\nname = "ucx-rs"\nversion = "%s"\nedition = "2024"\n' "$1" > crates/ucx-rs/Cargo.toml
            echo "pub fn touch() {} // $2" > crates/ucx-rs/src/lib.rs
            git add -A
            $GIT commit -q -m "$2"
        }
        write_ucx_commit "0.1.0+ucx.1.22.0" base
        base_sha=$(git rev-parse HEAD)
        write_ucx_commit "0.2.0+ucx.1.22.0" pr

        set +e
        out=$(BASE_REF="$base_sha" bash "$CHECK_SEMVER" 2>&1)
        exit_code=$?
        set -e
        echo "$out" > "$TMP_ROOT/e.out"
        echo "$exit_code" > "$TMP_ROOT/e.exit"
    )
}
out=$(cat "$TMP_ROOT/e.out")
ok=1
if [[ "$(cat "$TMP_ROOT/e.exit")" == "0" ]] \
    && echo "$out" | grep -qF 'breaking changes, but version bumped' \
    && ! echo "$out" | grep -qi 'Could not parse version' \
    && ! echo "$out" | grep -qi 'invalid arithmetic operator'; then
    ok=0
fi
report "(e) ucx-rs's +build metadata compares on the semver core; a correct bump passes the gate" "$ok" "$out"

echo ""

echo "passed: $pass_count, failed: $fail_count"
[[ "$fail_count" -eq 0 ]]
