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
/// An item counts when a `#[cfg(...)]` naming `test-helpers` (and not under
/// `not(...)`) sits in the attribute block right above a `pub` item. A
/// `pub(crate)` item is not public, so it does not count.
fn unhidden(source: &str, file: &str) -> Vec<String> {
    let lines: Vec<&str> = source.lines().collect();
    let mut found = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let line = line.trim_start();
        if !(line.starts_with("#[cfg(") && line.contains("\"test-helpers\""))
            || line.contains("not(")
        {
            continue;
        }
        let mut hidden = false;
        // Look back and forward through the attribute block.
        for above in lines[..index].iter().rev() {
            let above = above.trim_start();
            if !(above.starts_with("#[") || above.starts_with("///")) {
                break;
            }
            hidden |= above.starts_with("#[doc(hidden)]");
        }
        let mut item = None;
        for below in &lines[index + 1..] {
            let below = below.trim_start();
            if below.starts_with("#[") {
                hidden |= below.starts_with("#[doc(hidden)]");
                continue;
            }
            item = Some(below);
            break;
        }
        if let Some(item) = item
            && item.starts_with("pub ")
            && !hidden
        {
            found.push(format!("{file}:{}: {item}", index + 1));
        }
    }
    found
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
