#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

BASE_REF="${BASE_REF:-origin/main}"

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

changed_crates=()
for crate_dir in lib/*/; do
    crate_name=$(basename "$crate_dir")
    if echo "$changed_files" | grep -qE "^lib/${crate_name}/src/"; then
        changed_crates+=("$crate_name")
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
extract_crate_version() {
    local crate_name="$1"
    local source="$2"
    if [[ "$source" == "HEAD" ]]; then
        grep '^version' "lib/${crate_name}/Cargo.toml" | head -1 | sed 's/.*"\(.*\)".*/\1/'
    else
        git show "${source}:lib/${crate_name}/Cargo.toml" 2>/dev/null \
            | grep '^version' | head -1 | sed 's/.*"\(.*\)".*/\1/'
    fi
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

failures=()

for crate_name in "${changed_crates[@]}"; do
    # New crates that don't exist on the base branch — skip semver check
    if ! git show "${BASE_REF}:lib/${crate_name}/Cargo.toml" &>/dev/null; then
        echo "  ${crate_name}: new crate, skipping semver check"
        continue
    fi

    echo "Checking ${crate_name}..."

    crate_output=""
    crate_exit=0
    crate_output=$(cargo semver-checks check-release \
        --package "$crate_name" \
        --baseline-rev "${BASE_REF}" 2>&1) || crate_exit=$?

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

    # Breaking changes detected — check version bump
    base_version=$(extract_crate_version "$crate_name" "${BASE_REF}")
    pr_version=$(extract_crate_version "$crate_name" "HEAD")

    if [[ -z "$base_version" || -z "$pr_version" ]]; then
        echo "::error::Could not parse version for ${crate_name}"
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
    echo "      version on ${BASE_REF}: ${base_ver}"
    echo "      version on PR:          ${pr_ver}"
    echo "      required: at least ${required}"
    echo ""
done
echo "  To skip this check, add the 'semver:skip' label to the PR."
exit 1
