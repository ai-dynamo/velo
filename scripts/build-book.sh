#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Build the mdBook in docs/ and check its internal links.
#
# With INSTALL_TOOLS=1, download the pinned tool versions first (x86_64 Linux
# release binaries, as on GitHub-hosted runners). The versions match
# ../rhino so both books render the same way.
set -euo pipefail

MDBOOK_VERSION="0.4.52"
MDBOOK_MERMAID_VERSION="0.16.2"
MDBOOK_LINKCHECK_VERSION="0.7.7"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${INSTALL_TOOLS:-0}" == "1" ]]; then
  bin="${root}/.book-tools/bin"
  mkdir -p "${bin}"
  curl -sSfL "https://github.com/rust-lang/mdBook/releases/download/v${MDBOOK_VERSION}/mdbook-v${MDBOOK_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C "${bin}"
  curl -sSfL "https://github.com/badboy/mdbook-mermaid/releases/download/v${MDBOOK_MERMAID_VERSION}/mdbook-mermaid-v${MDBOOK_MERMAID_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
    | tar -xz -C "${bin}"
  tmp="$(mktemp -d)"
  curl -sSfL -o "${tmp}/linkcheck.zip" \
    "https://github.com/Michael-F-Bryan/mdbook-linkcheck/releases/download/v${MDBOOK_LINKCHECK_VERSION}/mdbook-linkcheck.x86_64-unknown-linux-gnu.zip"
  unzip -q -o "${tmp}/linkcheck.zip" -d "${bin}"
  chmod +x "${bin}"/mdbook*
  export PATH="${bin}:${PATH}"
fi

mdbook --version
mdbook-mermaid --version
mdbook-linkcheck --version

# The mermaid assets are generated, not committed (see docs/.gitignore).
mdbook-mermaid install "${root}/docs"
mdbook build "${root}/docs"

# With more than one renderer, mdBook writes the HTML to book/html/.
test -f "${root}/docs/book/html/index.html"

# The book links to sections of LIMITATIONS.md by anchor, and the link checker
# does not follow web links. Check each anchor against the file's headings, so
# a renamed heading fails here instead of leaving a dead link.
limitations="${root}/LIMITATIONS.md"
headings="$(grep -E '^## ' "${limitations}" \
  | sed -E 's/^## //; s/`//g' \
  | tr '[:upper:]' '[:lower:]' \
  | sed -E 's/[^a-z0-9_ -]//g; s/ /-/g')"
status=0
for anchor in $(grep -rhoE 'LIMITATIONS\.md#[A-Za-z0-9_-]+' "${root}/docs/src" "${root}/README.md" | sed 's/.*#//' | tr '[:upper:]' '[:lower:]' | sort -u); do
  if ! grep -qx "${anchor}" <<<"${headings}"; then
    echo "docs link to LIMITATIONS.md#${anchor}, which is not a heading there" >&2
    status=1
  fi
done
exit "${status}"
