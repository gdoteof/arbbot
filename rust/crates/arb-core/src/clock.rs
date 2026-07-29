//! Wall-clock reads, in one place.
//!
//! There is nothing clever here. It exists because the same four lines
//! (`SystemTime::now().duration_since(UNIX_EPOCH)`) were written out six times
//! across five files in four different return types, and the copies had already
//! drifted on the one decision that matters: what to do when the read fails.
//!
//! That decision is deliberate and it differs by caller, so it is named rather
//! than unified. [`now_local_ns`] PANICS — it stamps `ts_local_ns` on every tape
//! event, and a tape line silently carrying epoch 0 is a corrupt record that
//! outlives the process. Everything else saturates to 0, because a bad clock
//! must not take the order path down.
//!
//! A `duration_since(UNIX_EPOCH)` error means the system clock is set before
//! 1970. It has never happened here; the point is that the two answers are on
//! purpose rather than an accident of which copy you landed in.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn since_epoch() -> Option<Duration> {
    SystemTime::now().duration_since(UNIX_EPOCH).ok()
}

/// Epoch nanoseconds, 0 if the clock is unreadable.
pub fn now_ns() -> i64 {
    since_epoch().map(|d| d.as_nanos() as i64).unwrap_or(0)
}

/// Epoch seconds with a fractional part, 0.0 if the clock is unreadable.
pub fn now_s() -> f64 {
    since_epoch().map(|d| d.as_secs_f64()).unwrap_or(0.0)
}

/// Whole epoch seconds, 0 if the clock is unreadable.
pub fn now_secs() -> u64 {
    since_epoch().map(|d| d.as_secs()).unwrap_or(0)
}

/// Epoch nanoseconds for a tape event's `ts_local_ns`. Panics rather than
/// stamping 0 — see the module note.
pub fn now_local_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_units_agree_with_each_other() {
        // Same instant to within the time this test takes to run.
        let (ns, s, secs) = (now_ns(), now_s(), now_secs());
        assert!(ns > 1_700_000_000_000_000_000, "epoch nanos look wrong: {ns}");
        assert!((s - secs as f64).abs() < 2.0);
        assert!((ns as f64 / 1e9 - s).abs() < 2.0);
    }
}
