//! The JSON behind each dashboard view.

pub mod books;
pub mod capital;
pub mod now;
pub mod opportunities;
pub mod pairs;
pub mod trades;

use std::time::UNIX_EPOCH;

/// Age of a file in seconds. Absence stays unknown rather than becoming zero.
pub fn age_secs(path: &str) -> Option<u64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    let mtime = modified.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(arb_core::clock::now_secs().saturating_sub(mtime))
}
