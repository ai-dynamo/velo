// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Every public item that exists only under the `test-helpers` feature is
//! `#[doc(hidden)]`.
//!
//! `scripts/check-semver.sh` runs `cargo semver-checks` with no feature
//! flags, which checks every feature, so `test-helpers` is on and its items
//! are in the gated surface. Once a release ships, a change to one of them,
//! or to a wire message one of them names, fails the semver gate as a
//! breaking change, though these items are not a supported API.
//! `cargo semver-checks` skips hidden items, so each one carries
//! `#[doc(hidden)]`. A new helper added without it is missed in review: one
//! was, after the rest were hidden. This test reads the source, so it fails
//! the moment such an item lands.

use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// The public items gated on `test-helpers` that have no `#[doc(hidden)]`,
/// as `file:line: item`.
///
/// The scan reads each block of outer attributes above an item, joining an
/// attribute that spans several lines. An item counts when a `#[cfg(...)]`
/// in its block names `test-helpers` (and not under `not(...)`), and the item
/// is `pub`. It is hidden by `#[doc(hidden)]`, or by a `#[cfg_attr(...)]`
/// that names `test-helpers` and `doc(hidden)`, which holds whenever the
/// gated item exists. A `pub(crate)` item is not public, so it does not
/// count.
fn unhidden(source: &str, file: &str) -> Vec<String> {
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    // Each attribute as one string, with the line it starts on.
    let mut block: Vec<(usize, String)> = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_start();
        if line.starts_with("#[") {
            let start = index;
            let mut attribute = String::from(line);
            let mut depth = bracket_depth(line);
            while depth > 0 && index + 1 < lines.len() {
                index += 1;
                let next = lines[index].trim();
                attribute.push_str(next);
                depth += bracket_depth(next);
            }
            block.push((start, attribute));
        } else if line.starts_with("///") || line.starts_with("//") {
            // Doc and plain comments sit inside an attribute block.
        } else {
            let gate = block.iter().find(|(_, attribute)| {
                attribute.starts_with("#[cfg(")
                    && attribute.contains("\"test-helpers\"")
                    && !attribute.contains("not(")
            });
            let hidden = block.iter().any(|(_, attribute)| {
                attribute.starts_with("#[doc(hidden)]")
                    || (attribute.starts_with("#[cfg_attr(")
                        && attribute.contains("\"test-helpers\"")
                        && attribute.contains("doc(hidden)"))
            });
            if let Some((start, _)) = gate
                && line.starts_with("pub ")
                && !hidden
            {
                found.push(format!("{file}:{}: {line}", start + 1));
            }
            block.clear();
        }
        index += 1;
    }
    found
}

/// How far `text` opens square brackets: `[` adds one, `]` takes one.
fn bracket_depth(text: &str) -> i32 {
    text.chars().fold(0, |depth, c| match c {
        '[' => depth + 1,
        ']' => depth - 1,
        _ => depth,
    })
}

#[test]
fn every_test_helpers_item_is_hidden() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&src, &mut files);
    files.sort();
    let mut found = Vec::new();
    let mut gated = 0;
    for path in &files {
        let source = std::fs::read_to_string(path).unwrap();
        gated += source.matches("\"test-helpers\"").count();
        let name = path.strip_prefix(&src).unwrap().display().to_string();
        found.extend(unhidden(&source, &name));
    }
    // Control: the scan has to see the gated items to prove anything.
    assert!(
        gated > 5,
        "found only {gated} test-helpers gates under src/"
    );
    assert!(
        found.is_empty(),
        "public test-helpers items without #[doc(hidden)]:\n{}",
        found.join("\n")
    );
}

/// The scanner itself: it flags an unhidden public item, and passes a hidden
/// one, a `pub(crate)` one, and one under `not(feature = "test-helpers")`.
#[test]
fn the_scan_tells_hidden_from_unhidden() {
    let source = r#"
#[cfg(feature = "test-helpers")]
pub fn flagged() {}

#[cfg(feature = "test-helpers")]
#[doc(hidden)]
pub fn hidden_below() {}

#[doc(hidden)]
#[cfg(feature = "test-helpers")]
pub fn hidden_above() {}

#[cfg(any(test, feature = "test-helpers"))]
pub(crate) fn crate_only() {}

#[cfg(not(feature = "test-helpers"))]
pub fn not_gated() {}
"#;
    let found = unhidden(source, "x.rs");
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("flagged"), "{found:?}");
}

#[test]
fn the_scan_reads_a_multiline_cfg() {
    let source = r#"
#[cfg(all(
    target_os = "linux",
    feature = "ucx",
    feature = "test-helpers",
    feature = "distributed-tracing"
))]
pub struct Exposed;
"#;
    let found = unhidden(source, "x.rs");
    assert_eq!(
        found.len(),
        1,
        "a public test helper escaped the scan: {found:?}"
    );
}

#[test]
fn the_scan_accepts_a_conditionally_hidden_item() {
    let source = r#"
#[cfg(feature = "test-helpers")]
#[cfg_attr(feature = "test-helpers", doc(hidden))]
pub struct Hidden;
"#;
    let found = unhidden(source, "x.rs");
    assert!(
        found.is_empty(),
        "the helper is hidden whenever it exists: {found:?}"
    );
}

#[test]
fn the_scan_checks_public_types_and_reexports() {
    let source = r#"
#[cfg(feature = "test-helpers")]
pub struct Helper;

#[cfg(feature = "test-helpers")]
pub use std::time::Duration as HelperDuration;
"#;
    let found = unhidden(source, "x.rs");
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].contains("pub struct Helper"), "{found:?}");
    assert!(
        found[1].contains("pub use std::time::Duration"),
        "{found:?}"
    );
}
