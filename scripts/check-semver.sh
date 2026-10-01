#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# BASE_REF only selects which crates this change touches. The baseline that
# a crate is checked against is its latest version on crates.io: only
# published versions take part in Cargo's resolution, so a version on `main`
# that was never published needs no bump of its own. Several breaking
# changes between two publishes all ride on the one bump `main` carries over
# the published version (pre-1.0: one minor bump).
BASE_REF="${BASE_REF:-origin/main}"
SEMVER_INDEX_URL="${SEMVER_INDEX_URL:-https://index.crates.io}"

# ── Escape hatch: semver:skip label ──────────────────────────────────────────
if [[ "${SEMVER_SKIP:-false}" == "true" ]]; then
    echo "::notice::Semver check skipped via semver:skip label"
    exit 0
fi

# ── Determine which crates have source changes ──────────────────────────────
changed_files=$(git diff --name-only "${BASE_REF}...HEAD" 2>/dev/null || git diff --name-only "${BASE_REF}" HEAD)

if [[ -z "$changed_files" ]]; then
    echo "No changes detected against ${BASE_REF}, skipping semver check."
    exit 0
fi

# Publishable crates live under lib/ (velo, velo-ext) and crates/ (ucx-rs).
# Track each crate's directory so the baseline lookups below do not assume lib/.
changed_crates=()
declare -A crate_dirs=()
for crate_dir in lib/*/ crates/*/; do
    [[ -d "$crate_dir" ]] || continue
    crate_dir="${crate_dir%/}"
    crate_name=$(basename "$crate_dir")
    # Any file in the crate can change its public surface (src/, build.rs,
    # Cargo.toml features/deps) — match the whole crate directory.
    # A crate whose version is `version.workspace = true` takes it from the
    # root manifest, so a change to the root manifest alone can change it.
    if echo "$changed_files" | grep -qE "^${crate_dir}/" \
        || { echo "$changed_files" | grep -qx 'Cargo.toml' \
            && grep -qE '^version\.workspace[[:space:]]*=[[:space:]]*true' "${crate_dir}/Cargo.toml" 2>/dev/null; }; then
        changed_crates+=("$crate_name")
        crate_dirs["$crate_name"]="$crate_dir"
    fi
done

if [[ ${#changed_crates[@]} -eq 0 ]]; then
    echo "No library source changes detected, skipping semver check."
    exit 0
fi

echo "Crates with source changes: ${changed_crates[*]}"
echo ""

# ── Ensure cargo-semver-checks is installed ──────────────────────────────────
SEMVER_CHECKS_VERSION="0.47.0"
if ! cargo-semver-checks --version 2>/dev/null | grep -qF "$SEMVER_CHECKS_VERSION"; then
    echo "Installing cargo-semver-checks@${SEMVER_CHECKS_VERSION}..."
    cargo install "cargo-semver-checks@${SEMVER_CHECKS_VERSION}" --locked
fi

# ── Check each changed crate individually ────────────────────────────────────

# A crate's own Cargo.toml pins its version either literally
# (`version = "X.Y.Z"`) or by inheriting the workspace root's
# (`version.workspace = true` — velo's form since the workspace collapse
# folded per-crate versioning into [workspace.package]). Resolve the
# inherited form here so every caller below only ever sees an X.Y.Z string.
extract_workspace_version() {
    local manifest_text="$1"
    # Scope to the [workspace.package] section by name rather than grepping
    # the first `^version` line in the whole file: a root manifest is free to
    # carry other `[section]` stanzas with their own `version = ...` line
    # above it, and nothing enforces section order.
    printf '%s\n' "$manifest_text" | awk '
        /^\[/ { in_section = ($0 == "[workspace.package]") }
        in_section && /^version[[:space:]]*=/ { print; exit }
    '
}

# The version of a crate in the working tree.
extract_crate_version() {
    local crate_name="$1"
    local crate_dir="${crate_dirs[$crate_name]}"
    local crate_manifest raw

    crate_manifest=$(cat "${crate_dir}/Cargo.toml" 2>/dev/null) || crate_manifest=""
    raw=$(printf '%s\n' "$crate_manifest" | grep -m1 '^version') || raw=""

    if printf '%s' "$raw" | grep -q 'workspace[[:space:]]*=[[:space:]]*true'; then
        # Inherited version: the real value lives in [workspace.package] in
        # the root manifest.
        local root_manifest
        root_manifest=$(cat Cargo.toml 2>/dev/null) || root_manifest=""
        raw=$(extract_workspace_version "$root_manifest") || raw=""
    fi

    # -n ... p: print ONLY on a successful substitution. A plain `sed 's/.../'`
    # prints the input unchanged when the pattern does not match, which is
    # what let `version.workspace = true` pass straight through as if it were
    # a version string in the first place — the actual root cause here.
    printf '%s\n' "$raw" | sed -n 's/.*"\(.*\)".*/\1/p'
}

parse_version() {
    IFS='.' read -r major minor patch <<< "$1"
    echo "$major $minor $patch"
}

check_bump_sufficient() {
    local base_version="$1"
    local pr_version="$2"

    read -r base_major base_minor base_patch <<< "$(parse_version "$base_version")"
    read -r pr_major pr_minor pr_patch <<< "$(parse_version "$pr_version")"

    local bump_type="none"
    if [[ "$pr_major" -gt "$base_major" ]]; then
        bump_type="major"
    elif [[ "$pr_major" -eq "$base_major" && "$pr_minor" -gt "$base_minor" ]]; then
        bump_type="minor"
    elif [[ "$pr_major" -eq "$base_major" && "$pr_minor" -eq "$base_minor" && "$pr_patch" -gt "$base_patch" ]]; then
        bump_type="patch"
    fi

    # Pre-1.0: minor bump covers breaking changes. Post-1.0: major bump required.
    if [[ "$base_major" -eq 0 ]]; then
        [[ "$bump_type" == "minor" || "$bump_type" == "major" ]] && return 0
    else
        [[ "$bump_type" == "major" ]] && return 0
    fi
    return 1
}

required_bump_label() {
    local base_version="$1"
    read -r base_major base_minor _ <<< "$(parse_version "$base_version")"
    if [[ "$base_major" -eq 0 ]]; then
        echo "minor (e.g. ${base_major}.$((base_minor + 1)).0)"
    else
        echo "major (e.g. $((base_major + 1)).0.0)"
    fi
}

# The sparse-index path of a crate (https://doc.rust-lang.org/cargo/reference/registry-index.html).
index_path() {
    local name="${1,,}"
    case ${#name} in
        1) echo "1/${name}" ;;
        2) echo "2/${name}" ;;
        3) echo "3/${name:0:1}/${name}" ;;
        *) echo "${name:0:2}/${name:2:2}/${name}" ;;
    esac
}

# Prints the crate's highest non-yanked version on the registry, or nothing
# if the registry has never seen the crate (HTTP 404). Any other failure
# exits: a network error must not read as "unpublished", which would skip
# the check.
latest_published_version() {
    local crate_name="$1" body status
    body=$(mktemp)
    status=$(curl -sS -o "$body" -w '%{http_code}' "${SEMVER_INDEX_URL}/$(index_path "$crate_name")") || {
        rm -f "$body"
        echo "::error::Could not reach ${SEMVER_INDEX_URL} for ${crate_name}" >&2
        exit 1
    }
    if [[ "$status" == "404" ]]; then
        rm -f "$body"
        return 0
    fi
    if [[ "$status" != "200" ]]; then
        rm -f "$body"
        echo "::error::${SEMVER_INDEX_URL} answered HTTP ${status} for ${crate_name}" >&2
        exit 1
    fi
    if ! grep -qF '"vers":"' "$body"; then
        rm -f "$body"
        echo "::error::${SEMVER_INDEX_URL} answered 200 for ${crate_name} with a body that is not an index entry" >&2
        exit 1
    fi
    local latest
    latest=$(grep -F '"yanked":false' "$body" | sed -n 's/.*"vers":"\([^"]*\)".*/\1/p' | sort -V | tail -1) || true
    rm -f "$body"
    if [[ -z "$latest" ]]; then
        echo "::error::every published version of ${crate_name} is yanked; there is no baseline to check against" >&2
        exit 1
    fi
    echo "$latest"
}

failures=()

for crate_name in "${changed_crates[@]}"; do
    published=$(latest_published_version "$crate_name")
    if [[ -z "$published" ]]; then
        echo "  ${crate_name}: never published, skipping semver check"
        continue
    fi

    echo "Checking ${crate_name} against ${published}, its latest published version..."

    base_version="$published"
    pr_version=$(extract_crate_version "$crate_name")

    # crates/ucx-rs pins "0.1.0+ucx.1.22.0". Build metadata records which UCX
    # release is vendored and takes no part in precedence (semver 2.0 s10), so
    # strip it before comparing. Rejecting it outright would make every future
    # ucx-rs breaking change unsatisfiable — there is no version that both
    # names the vendored UCX and passes the gate.
    base_version="${base_version%%+*}"
    pr_version="${pr_version%%+*}"

    # What survives must be a plain X.Y.Z: that is the only shape
    # check_bump_sufficient's arithmetic can compare. An empty string (the old
    # crash trigger) and a pre-release suffix both fail here, loudly and
    # closed, rather than reaching `-gt` and aborting the shell mid-run.
    if [[ ! "$base_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || [[ ! "$pr_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "::error::Could not parse version for ${crate_name} (base='${base_version}' pr='${pr_version}')"
        exit 1
    fi

    # Before 1.0, cargo-semver-checks allows any change across a minor
    # version in either direction, so it would pass a version below the
    # published one. A lower version is a bad merge, not a release.
    lowest=$(printf '%s\n%s\n' "$pr_version" "$base_version" | sort -V | head -1)
    if [[ "$pr_version" != "$base_version" && "$lowest" == "$pr_version" ]]; then
        echo "::error::${crate_name} ${pr_version} is below its latest published version ${base_version}"
        exit 1
    fi

    crate_output=""
    crate_exit=0
    crate_output=$(cargo semver-checks check-release \
        --package "$crate_name" \
        --baseline-version "$published" 2>&1) || crate_exit=$?

    if [[ $crate_exit -eq 0 ]]; then
        echo "  ${crate_name}: no breaking changes"
        continue
    fi

    # Distinguish tool errors from actual semver violations
    if ! echo "$crate_output" | grep -qiE '(BREAKING|--- failure|semver requires)'; then
        echo "::error::cargo-semver-checks failed for ${crate_name} (not a semver violation — likely a build error):"
        echo "$crate_output"
        exit 1
    fi

    if check_bump_sufficient "$base_version" "$pr_version"; then
        echo "  ${crate_name}: breaking changes, but version bumped ${base_version} -> ${pr_version}"
        continue
    fi

    echo "  ${crate_name}: BREAKING CHANGES without sufficient version bump"
    echo ""
    echo "$crate_output"
    echo ""
    failures+=("${crate_name}|${base_version}|${pr_version}")
done

echo ""

if [[ ${#failures[@]} -eq 0 ]]; then
    echo "All semver checks passed."
    exit 0
fi

# ── Report failures ──────────────────────────────────────────────────────────
echo "::error::Breaking API changes detected without sufficient version bumps."
echo ""
echo "  The following crates have breaking changes and need a version bump:"
echo ""
for entry in "${failures[@]}"; do
    IFS='|' read -r name base_ver pr_ver <<< "$entry"
    required=$(required_bump_label "$base_ver")
    echo "    ${name}"
    echo "      latest published version: ${base_ver}"
    echo "      version on this change:   ${pr_ver}"
    echo "      required: at least ${required}"
    echo ""
done
echo "  To skip this check, add the 'semver:skip' label to the PR."
exit 1
