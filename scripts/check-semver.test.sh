#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# End-to-end test for scripts/check-semver.sh.
#
# check-semver.sh runs top-to-bottom (it is not sourceable), so this drives
# the real script against a throwaway git fixture repo rather than
# unit-testing its functions. It stubs `cargo` and `curl` on PATH: the cargo
# stub forces a "breaking change" and records its arguments, and the curl
# stub serves a fixture crates.io index from a directory. The test needs no
# network and no real cargo-semver-checks install.
#
# Usage: bash scripts/check-semver.test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK_SEMVER="${CHECK_SEMVER:-${SCRIPT_DIR}/check-semver.sh}"

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

# ── Stubs on PATH ──────────────────────────────────────────────────────────
# check-semver.sh calls the standalone `cargo-semver-checks` binary once (to
# check its installed version) and `cargo semver-checks check-release`
# separately. Stub both, so the test controls whether a "breaking change" is
# reported without one existing in the fixture crate's source.
STUB_BIN="$TMP_ROOT/bin"
mkdir -p "$STUB_BIN"

cat > "$STUB_BIN/cargo-semver-checks" <<'STUB'
#!/usr/bin/env bash
echo "cargo-semver-checks 0.47.0"
STUB
chmod +x "$STUB_BIN/cargo-semver-checks"

cat > "$STUB_BIN/cargo" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$CARGO_ARGS_LOG"
if [[ "$1" == "semver-checks" ]]; then
    echo "--- failure some_lint: fixture-forced breaking change ---"
    exit 1
fi
echo "unexpected cargo invocation: $*" >&2
exit 1
STUB
chmod +x "$STUB_BIN/cargo"

# The curl stub serves "$INDEX_DIR/<index path>" for any URL under
# $SEMVER_INDEX_URL, and answers 404 for a crate the fixture index lacks, as
# the sparse index does. INDEX_FAIL=1 makes it fail like an unreachable host,
# INDEX_STATUS answers that HTTP status, and INDEX_BODY answers 200 with that
# body for every crate.
cat > "$STUB_BIN/curl" <<'STUB'
#!/usr/bin/env bash
out="" url=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -w) shift 2 ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
if [[ "${INDEX_FAIL:-0}" == "1" ]]; then
    echo "curl: (6) Could not resolve host" >&2
    exit 6
fi
if [[ -n "${INDEX_STATUS:-}" ]]; then
    echo "error" > "$out"
    printf '%s' "$INDEX_STATUS"
    exit 0
fi
if [[ -n "${INDEX_BODY:-}" ]]; then
    printf '%s\n' "$INDEX_BODY" > "$out"
    printf 200
    exit 0
fi
path="${url#"${SEMVER_INDEX_URL}"/}"
if [[ -f "$INDEX_DIR/$path" ]]; then
    cp "$INDEX_DIR/$path" "$out"
    printf 200
else
    : > "$out"
    printf 404
fi
STUB
chmod +x "$STUB_BIN/curl"

export PATH="$STUB_BIN:$PATH"
export SEMVER_INDEX_URL="https://index.test"
export INDEX_DIR="$TMP_ROOT/index"
export CARGO_ARGS_LOG="$TMP_ROOT/cargo-args.log"

# Records versions of a crate in the fixture index, one sparse-index line
# each. A version with a trailing "!" is yanked.
publish() {
    local name="$1"; shift
    local path v yanked
    case ${#name} in
        1) path="1/${name}" ;;
        2) path="2/${name}" ;;
        3) path="3/${name:0:1}/${name}" ;;
        *) path="${name:0:2}/${name:2:2}/${name}" ;;
    esac
    mkdir -p "$INDEX_DIR/$(dirname "$path")"
    : > "$INDEX_DIR/$path"
    for v in "$@"; do
        yanked=false
        if [[ "$v" == *! ]]; then
            yanked=true
            v="${v%!}"
        fi
        printf '{"name":"%s","vers":"%s","deps":[],"cksum":"0","features":{},"yanked":%s}\n' \
            "$name" "$v" "$yanked" >> "$INDEX_DIR/$path"
    done
}

reset_index() {
    rm -rf "$INDEX_DIR" "$CARGO_ARGS_LOG"
    mkdir -p "$INDEX_DIR"
}

GIT="git -c user.email=test@example.com -c user.name=test -c commit.gpgsign=false"

# ── Fixture repo builder ───────────────────────────────────────────────────
# Lays down a workspace shaped like velo's: a virtual root manifest with
# [workspace.package] version, one crate that inherits it
# (version.workspace = true, mirrors lib/velo), and one crate with a literal
# version (mirrors lib/velo-ext). $1: root version, $2: literal-crate
# version, $3: commit message, $4: a marker written into both crates' lib.rs
# so consecutive commits always produce a real diff there. That diff is what
# makes check-semver.sh select both crates as changed.
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

# Builds a fixture repo with a base commit and a change commit, and runs the
# gate on the change. $1: case name, $2/$3: base versions (root, literal),
# $4/$5: change versions, $6: optional source marker for the change commit.
# Leaves $TMP_ROOT/<case>.out and .exit.
run_case() {
    local name="$1" base_root="$2" base_lit="$3" pr_root="$4" pr_lit="$5"
    # A 6th argument keeps the crates' sources unchanged, so the change
    # touches only the manifests.
    local change_marker="${6:-change}"
    local repo="$TMP_ROOT/repo-$name"
    mkdir -p "$repo"
    (
        cd "$repo"
        git init -q -b main
        write_fixture_commit "$base_root" "$base_lit" "base" "base"
        local base_sha
        base_sha=$(git rev-parse HEAD)
        write_fixture_commit "$pr_root" "$pr_lit" "change" "$change_marker"
        set +e
        BASE_REF="$base_sha" bash "$CHECK_SEMVER" > "$TMP_ROOT/$name.out" 2>&1
        echo $? > "$TMP_ROOT/$name.exit"
        set -e
    )
}

# ── (a) a breaking change at the published version fails ───────────────────
# velo inherits its version from [workspace.package] and velo-ext has a
# literal one, so this also proves both extractions work. The gate must
# compare against the published versions and pass them to
# cargo-semver-checks as the baseline.
reset_index
publish velo 0.9.0 0.10.0
publish velo-ext 0.5.0
run_case a 0.10.0 0.5.0 0.10.0 0.5.0
out=$(cat "$TMP_ROOT/a.out")
ok=1
if [[ "$(cat "$TMP_ROOT/a.exit")" == "1" ]] \
    && echo "$out" | grep -qF 'latest published version: 0.10.0' \
    && echo "$out" | grep -qF 'latest published version: 0.5.0' \
    && grep -qF -- '--package velo --baseline-version 0.10.0' "$CARGO_ARGS_LOG" \
    && grep -qF -- '--package velo-ext --baseline-version 0.5.0' "$CARGO_ARGS_LOG" \
    && ! echo "$out" | grep -qi 'unbound variable' \
    && ! echo "$out" | grep -qi 'workspace = true'; then
    ok=0
fi
report "(a) a breaking change at the published version fails, checked against the published baseline" "$ok" "$out"

# ── (b) a bump that main already carries covers a later breaking change ─────
# The rule this gate enforces: a version is bumped against the latest
# published version, for everything merged since that publish. Main went to
# 0.11.0 (and velo-ext to 0.6.0) in an earlier change; this change is also
# breaking and keeps those versions. It must pass. A gate that compares
# against the base branch instead sees 0.11.0 -> 0.11.0 and fails it.
reset_index
publish velo 0.9.0 0.10.0
publish velo-ext 0.5.0
run_case b 0.11.0 0.6.0 0.11.0 0.6.0
out=$(cat "$TMP_ROOT/b.out")
ok=1
if [[ "$(cat "$TMP_ROOT/b.exit")" == "0" ]] \
    && echo "$out" | grep -qF 'version bumped 0.10.0 -> 0.11.0' \
    && echo "$out" | grep -qF 'version bumped 0.5.0 -> 0.6.0'; then
    ok=0
fi
report "(b) a bump main already carries over the published version covers a later breaking change" "$ok" "$out"

# ── (c) yanked versions are not the baseline ───────────────────────────────
# 0.11.0 was published and yanked. The baseline is 0.10.0, so 0.11.0 on the
# change is a sufficient bump.
reset_index
publish velo 0.10.0 0.11.0!
publish velo-ext 0.5.0
run_case c 0.10.0 0.6.0 0.11.0 0.6.0
out=$(cat "$TMP_ROOT/c.out")
ok=1
if [[ "$(cat "$TMP_ROOT/c.exit")" == "0" ]] \
    && echo "$out" | grep -qF 'version bumped 0.10.0 -> 0.11.0'; then
    ok=0
fi
report "(c) a yanked version is not the baseline" "$ok" "$out"

# ── (d) an unparseable version fails loudly ─────────────────────────────────
# A version that check_bump_sufficient's `.`-split cannot compare must give a
# clear ::error:: and a clean exit 1: never an "unbound variable" or "invalid
# arithmetic operator" abort, and never a silent pass.
reset_index
publish velo 0.10.0
publish velo-ext 0.5.0
run_case d 0.10.0 0.5.0 0.10 0.5.0
out=$(cat "$TMP_ROOT/d.out")
ok=1
if [[ "$(cat "$TMP_ROOT/d.exit")" == "1" ]] \
    && echo "$out" | grep -qi '::error::Could not parse version' \
    && ! echo "$out" | grep -qi 'unbound variable' \
    && ! echo "$out" | grep -qi 'invalid arithmetic operator'; then
    ok=0
fi
report "(d) an unparseable version fails loudly with ::error:: and exit 1" "$ok" "$out"

# ── (e) `+build` metadata compares on the semver core ──────────────────────
# crates/ucx-rs pins `0.1.0+ucx.1.22.0`: the metadata records the vendored UCX
# release and, per semver 2.0 section 10, takes no part in precedence. A
# correctly bumped ucx-rs must pass. Rejecting the whole string as
# unparseable would make every future ucx-rs breaking change unsatisfiable.
reset_index
publish ucx-rs "0.1.0+ucx.1.22.0"
{
    repo="$TMP_ROOT/repo-e"
    mkdir -p "$repo"
    (
        cd "$repo"
        git init -q -b main
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
        write_ucx_commit "0.2.0+ucx.1.22.0" change
        set +e
        BASE_REF="$base_sha" bash "$CHECK_SEMVER" > "$TMP_ROOT/e.out" 2>&1
        echo $? > "$TMP_ROOT/e.exit"
        set -e
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
report "(e) ucx-rs's +build metadata compares on the semver core; a correct bump passes" "$ok" "$out"

# ── (f) a crate that was never published is skipped ─────────────────────────
# The index answers 404 for it, so there is no baseline to break.
reset_index
publish velo-ext 0.5.0
run_case f 0.10.0 0.6.0 0.10.0 0.6.0
out=$(cat "$TMP_ROOT/f.out")
ok=1
if [[ "$(cat "$TMP_ROOT/f.exit")" == "0" ]] \
    && echo "$out" | grep -qF 'velo: never published, skipping semver check' \
    && ! grep -qF -- '--package velo ' "$CARGO_ARGS_LOG"; then
    ok=0
fi
report "(f) a crate the registry has never seen is skipped" "$ok" "$out"

# ── (g) an unreachable index fails, and is not read as "unpublished" ────────
# Reading a network error as a 404 would skip every check and pass.
reset_index
publish velo 0.10.0
publish velo-ext 0.5.0
INDEX_FAIL=1 run_case g 0.10.0 0.5.0 0.10.0 0.5.0
out=$(cat "$TMP_ROOT/g.out")
ok=1
if [[ "$(cat "$TMP_ROOT/g.exit")" == "1" ]] \
    && echo "$out" | grep -qF '::error::Could not reach' \
    && ! echo "$out" | grep -qF 'never published'; then
    ok=0
fi
report "(g) an unreachable index fails the gate instead of skipping the check" "$ok" "$out"


# ── (h) an HTTP 200 that is not an index fails ───────────────────────────────
# A proxy or a captive portal can answer 200 with a page. Reading that as
# "never published" would skip every check and pass.
reset_index
INDEX_BODY='<html>sign in</html>' run_case h 0.10.0 0.5.0 0.10.0 0.5.0
out=$(cat "$TMP_ROOT/h.out")
ok=1
if [[ "$(cat "$TMP_ROOT/h.exit")" == "1" ]] \
    && echo "$out" | grep -qF '::error::' \
    && ! echo "$out" | grep -qF 'never published'; then
    ok=0
fi
report "(h) an HTTP 200 whose body is not an index fails the gate" "$ok" "$out"

# ── (i) a crate whose every version is yanked fails ──────────────────────────
# There is no baseline to check against, and that needs a person's decision.
reset_index
publish velo 0.10.0! 0.11.0!
publish velo-ext 0.5.0
run_case i 0.11.0 0.5.0 0.11.0 0.5.0
out=$(cat "$TMP_ROOT/i.out")
ok=1
if [[ "$(cat "$TMP_ROOT/i.exit")" == "1" ]] \
    && echo "$out" | grep -qF 'every published version' \
    && ! echo "$out" | grep -qF 'never published'; then
    ok=0
fi
report "(i) a crate with every version yanked fails the gate" "$ok" "$out"

# ── (j) an index error status fails ──────────────────────────────────────────
reset_index
INDEX_STATUS=500 run_case j 0.10.0 0.5.0 0.10.0 0.5.0
out=$(cat "$TMP_ROOT/j.out")
ok=1
if [[ "$(cat "$TMP_ROOT/j.exit")" == "1" ]] \
    && echo "$out" | grep -qF 'answered HTTP 500' \
    && ! echo "$out" | grep -qF 'never published'; then
    ok=0
fi
report "(j) an index error status other than 404 fails the gate" "$ok" "$out"

# ── (k) a version below the published one fails ─────────────────────────────
# cargo-semver-checks treats any minor change before 1.0 as allowed to break,
# in either direction, so it passes 0.9.0 against a published 0.10.0. A bad
# merge that lowers the version must not pass.
reset_index
publish velo 0.10.0
publish velo-ext 0.5.0
run_case k 0.10.0 0.5.0 0.9.0 0.5.0
out=$(cat "$TMP_ROOT/k.out")
ok=1
if [[ "$(cat "$TMP_ROOT/k.exit")" == "1" ]] \
    && echo "$out" | grep -qF 'is below its latest published version 0.10.0'; then
    ok=0
fi
report "(k) a version below the latest published version fails the gate" "$ok" "$out"

# ── (l) a change to the root manifest selects the crates that inherit it ─────
# velo takes its version from [workspace.package] in the root Cargo.toml. A
# change that touches only that file can change velo's version, so velo must
# be checked.
reset_index
publish velo 0.10.0
publish velo-ext 0.5.0
run_case l 0.11.0 0.5.0 0.10.0 0.5.0 base
out=$(cat "$TMP_ROOT/l.out")
ok=1
if [[ "$(cat "$TMP_ROOT/l.exit")" == "1" ]] \
    && echo "$out" | grep -qF 'Checking velo against 0.10.0' \
    && ! echo "$out" | grep -qF 'Checking velo-ext'; then
    ok=0
fi
report "(l) a root-manifest change selects the crates that inherit its version" "$ok" "$out"

echo ""

echo "passed: $pass_count, failed: $fail_count"
[[ "$fail_count" -eq 0 ]]
