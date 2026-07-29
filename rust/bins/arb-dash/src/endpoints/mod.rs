//! The JSON behind each view, one module per view.
//!
//! Every builder here rebuilds its answer from the files on each request and
//! caches nothing, so no view can show a number the files no longer support.
//! What lives in this file is only what two or more of them need.

pub mod books;
pub mod current;
pub mod intents;
pub mod opportunities;
pub mod pairs;
pub mod trades;

use std::collections::HashMap;
use std::time::UNIX_EPOCH;

use arb_core::clock::now_secs;

use crate::VENUES;

/// Newest ToB sample per (venue, market) — the current book, as far as the
/// rollup knows it.
pub type Latest = HashMap<(String, String), arb_tob::TobSample>;

pub fn age_secs(path: &str) -> Option<u64> {
    let m = std::fs::metadata(path).ok()?;
    let mtime = m.modified().ok()?.duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(now_secs().saturating_sub(mtime))
}

/// The ToB rollup files for one day that actually exist. A venue that was not
/// rolled up is absent, not empty.
pub fn rollup_paths(rollup_dir: &str, day: &str) -> Vec<String> {
    VENUES
        .iter()
        .map(|v| format!("{rollup_dir}/tob-{v}-{day}.jsonl"))
        .filter(|p| std::path::Path::new(p).is_file())
        .collect()
}

/// Beyond this the rollup is history, not evidence about now.
pub const MAX_COVERAGE_AGE_S: i64 = 1800;

/// How far the ROLLUP covers, not when an individual market last moved.
///
/// This distinction cost me a wrong headline. The series emits on CHANGE, so a
/// sample from 13 hours ago on a market that has not moved IS the current book.
/// Treating per-market sample age as staleness conflates "quiet" with
/// "unknown" and reported 0 of 67 pairs as actionable when 47 had a perfectly
/// fillable book (median spread 2c). What actually goes stale is the rollup
/// itself: if it was built at noon, nothing in it knows about the afternoon.
///
/// `i64::MAX` when the rollup carried no sample at all. The clock is the
/// caller's because the two views that ask derive it differently.
pub fn coverage_age_s(latest: &Latest, now_ns: i64) -> i64 {
    let coverage_ns = latest.values().map(|s| s.ts_local_ns).max().unwrap_or(0);
    if coverage_ns > 0 {
        (now_ns - coverage_ns) / 1_000_000_000
    } else {
        i64::MAX
    }
}
