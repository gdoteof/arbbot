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

use std::collections::{HashMap, HashSet};
use std::time::UNIX_EPOCH;

use arb_core::clock::now_secs;
use arb_registry::Registry;

use crate::VENUES;

/// Newest ToB sample per (venue, market) — the current book, as far as the
/// rollup knows it.
pub type Latest = HashMap<(String, String), arb_tob::TobSample>;

/// Seconds of coverage age per VENUE. A venue the rollup carries no
/// registry-backed sample for is ABSENT, not zero.
pub type Coverage = HashMap<String, i64>;

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

/// How far the ROLLUP covers PER VENUE, not when an individual market last
/// moved.
///
/// This distinction cost me a wrong headline. The series emits on CHANGE, so a
/// sample from 13 hours ago on a market that has not moved IS the current book.
/// Treating per-market sample age as staleness conflates "quiet" with
/// "unknown" and reported 0 of 67 pairs as actionable when 47 had a perfectly
/// fillable book (median spread 2c). What actually goes stale is the rollup
/// itself: if it was built at noon, nothing in it knows about the afternoon.
///
/// PER VENUE because the venue is the unit that dies. Each tape is recorded on
/// its own connection and replayed independently (`arb_tob::build_day`), so a
/// polymarket_us recorder that stops at 09:00 leaves that venue's samples at
/// 09:00 while kalshi keeps sampling all afternoon. `latest` mixes all three
/// venues into one map, and the newest sample ANYWHERE in it is not evidence
/// about any particular one: a single `.max()` read seconds off kalshi and
/// painted the board ROLLUP CURRENT over a six-hour-dead book — the same wrong
/// headline this gate was built to stop, one venue too coarse.
///
/// Over the REGISTRY-BACKED markets only, because `min` over the whole venue
/// reports the freshness of its NOISIEST market and most of a venue's markets
/// are not ones this board prices. The 2026-07-28 polymarket_us tape carries
/// 749 markets and the registry names 88: the other 661 are there because the
/// pm-us universe is built from `polymarket_us_tags` (`main.rs:206`), not from
/// the registry, so 88% of the population certifying the venue is a population
/// no pair is priced off.
///
/// WHAT THIS CATCHES, stated no wider than it is: a failure that takes out
/// EVERY registry leg on a venue while its catalog markets keep flowing. Not
/// one market and not one chunk — the 88 pm-us legs span ten families spread
/// across the whole slug list, so losing any single 150-slug subscribe window
/// (`pmus.rs:166`) leaves dozens still ticking and `min` over the survivors
/// does not move. The reachable shape is the catalog itself: the pm-us slug
/// list is whatever `markets_by_tags` returned at startup, filtered `!closed`
/// (`main.rs:206`), so a catalog that omits our markets subscribes 600+ others
/// and none of ours. That is the `UNSUBSCRIBED` state below, and it is the one
/// this basis exists for.
///
/// What it buys, measured on the 07-28 tape rather than argued: at every one of
/// the 18 hourly checkpoints across that day, `min` over all 749 markets reads
/// 0-4s. It is a socket-liveness bit and nothing finer — anything short of the
/// whole feed dying is invisible to it. `min` over the 88 ranges 1-151s on the
/// same checkpoints, so it is at least CAPABLE of registering less than total
/// death. That is the improvement; see the residual below for what it still is
/// not.
///
/// NOT the whole-socket death, which the old `min` over 749 already caught
/// (pm-us is one task over the whole list). NOT this repo's documented silent
/// subscriber eviction, which is DOWNSTREAM of the tape — `Core::on_event`
/// writes at `core.rs:116` and only then publishes to the broadcaster at
/// `:121`. And on polymarket_us specifically, NOT the per-market repair paths
/// either: `pmus::parse_ws_message` emits only snapshots and trades
/// (`pmus.rs:40`), gaps arise only in `apply_delta` (`book.rs:155`) and the
/// snapshot arm returns `Ok(())` unconditionally, so no pm-us event ever asks
/// for a resnapshot; and `evict_book`'s only callers are Kalshi
/// (`main.rs:364`) and Polymarket (`main.rs:374`). Where those paths DO apply,
/// a dropped book is restored by the 300s sweep (`RESNAP_EVERY`,
/// `RESNAP_FULL_S`), well inside `MAX_COVERAGE_AGE_S`, so they cannot move a
/// chip. The unbounded one is an EVICTED market, which leaves the sweep until
/// the venue reports it live again.
///
/// ALL registry legs, not the tradable ones, and this half is measured rather
/// than argued. Only 6 polymarket markets back a tradable pair, and the newest
/// of those 6 is 10,832s / 13,473s / 62,617s old on the 07-26, 07-27 and 07-28
/// tapes while the feed is demonstrably alive — a basis that narrow re-enters
/// the quiet-market trap this doc comment opens with. It is also the wrong
/// SUBJECT: coverage is a claim about what the recorder is delivering, and
/// vetting is a claim about a pair, so a `verdict: rejected` must not move what
/// a venue's freshness reads.
///
/// The residual, so nobody reads this as more than it is: `min` over 88 is
/// still "one live market certifies the other 87", so a single frozen market —
/// on any venue, by any of the mechanisms above — is as invisible now as it was
/// before. On the healthy 07-28 tape polymarket chips green at 36s while all
/// six tradable polymarket legs are 62,617s old. Narrowing the population to
/// the one the board prices is all this does; sourcing coverage from the
/// recorder's own per-feed liveness, rather than from an emit-on-change tape,
/// is the answer to the rest.
///
/// The clock is the caller's because the two views that ask derive it
/// differently.
pub fn coverage_by_venue(latest: &Latest, reg: &Registry, now_ns: i64) -> Coverage {
    let backed: HashSet<(&str, &str)> = reg
        .relationships
        .iter()
        .flat_map(|r| r.legs.iter().map(|l| (l.venue.as_str(), l.market_id.as_str())))
        .collect();
    let mut cov = Coverage::new();
    for ((venue, market), s) in latest {
        if !backed.contains(&(venue.as_str(), market.as_str())) {
            continue;
        }
        let age = (now_ns - s.ts_local_ns) / 1_000_000_000;
        let e = cov.entry(venue.clone()).or_insert(age);
        *e = (*e).min(age);
    }
    cov
}

/// `i64::MAX` for a venue the rollup carried no registry-backed sample for.
/// Zero would read as "sampled this instant" over no book whatsoever — and a
/// rollup holding only markets no pair prices is exactly that: evidence that
/// the venue's tape is being written, and none at all about the books this
/// board prices off.
pub fn venue_age_s(cov: &Coverage, venue: &str) -> i64 {
    cov.get(venue).copied().unwrap_or(i64::MAX)
}

/// One number for the whole rollup: its STALEST venue, because the board is
/// only as current as the worst leg it prices off.
///
/// Only venues the rollup actually carries. A venue with no samples is a HOLE
/// in the board — pairs on it are never evaluated, and the views count them as
/// such — not a claim that the samples the rollup does have are old.
pub fn stalest_age_s(cov: &Coverage) -> i64 {
    cov.values().copied().max().unwrap_or(i64::MAX)
}

/// Per-venue coverage as the board renders it: ALL THREE venues, always, so a
/// venue missing from the rollup is visible as missing rather than as merely
/// absent from a list.
///
/// `recorded` separates the two ways coverage can be ABSENT, because they take
/// different repairs and the board tells the operator which one to do. No
/// rollup for the venue at all is a rollup to rebuild. A rollup that is being
/// written but carries not one market the registry names is a SUBSCRIPTION
/// that no longer covers the pairs, and rebuilding the rollup would replay the
/// same markets and change nothing.
pub fn coverage_json(cov: &Coverage, latest: &Latest) -> serde_json::Value {
    VENUES
        .iter()
        .map(|v| {
            let age = venue_age_s(cov, v);
            serde_json::json!({
                "venue": v,
                "coverage_age_s": if age == i64::MAX { -1 } else { age },
                "current": age <= MAX_COVERAGE_AGE_S,
                "recorded": latest.keys().any(|(venue, _)| venue == v),
            })
        })
        .collect::<Vec<_>>()
        .into()
}

#[cfg(test)]
mod tests {
    use super::{coverage_by_venue, stalest_age_s, venue_age_s, Coverage, Latest, MAX_COVERAGE_AGE_S};

    const NOW_NS: i64 = 1_785_250_000_000_000_000;
    const S: i64 = 1_000_000_000;

    fn book(venue: &str, market: &str, age_s: i64) -> ((String, String), arb_tob::TobSample) {
        (
            (venue.to_string(), market.to_string()),
            arb_tob::TobSample {
                venue: venue.into(),
                market_id: market.into(),
                ts_local_ns: NOW_NS - age_s * S,
                bid: Some("0.40".into()),
                bid_size: Some("100".into()),
                ask: Some("0.42".into()),
                ask_size: Some("100".into()),
            },
        )
    }

    /// A registry naming exactly these legs. The coverage basis is all
    /// `coverage_by_venue` reads off a registry, so that is all this builds.
    fn registry(legs: &[(String, String)]) -> arb_registry::Registry {
        arb_registry::Registry {
            relationships: legs
                .iter()
                .map(|(v, m)| {
                    serde_json::from_value(serde_json::json!({
                        "id": format!("{v}-{m}"),
                        "legs": [{ "venue": v, "market_id": m }],
                    }))
                    .expect("a relationship")
                })
                .collect(),
        }
    }

    /// Coverage over a rollup where the registry names only `backed` — the
    /// venue's other markets are recorded (the tag catalog brings in 661 of the
    /// 749 on pm-us) and priced by nothing.
    fn cov_backed(
        backed: &[(&str, &str)],
        samples: impl IntoIterator<Item = ((String, String), arb_tob::TobSample)>,
    ) -> Coverage {
        let mut latest = Latest::new();
        latest.extend(samples);
        let legs: Vec<(String, String)> =
            backed.iter().map(|(v, m)| (v.to_string(), m.to_string())).collect();
        coverage_by_venue(&latest, &registry(&legs), NOW_NS)
    }

    /// Every sampled market backed by the registry: the tests below this one
    /// are about freshness, not about which markets we subscribed to.
    fn cov(samples: impl IntoIterator<Item = ((String, String), arb_tob::TobSample)>) -> Coverage {
        let mut latest = Latest::new();
        latest.extend(samples);
        let legs: Vec<(String, String)> = latest.keys().cloned().collect();
        coverage_by_venue(&latest, &registry(&legs), NOW_NS)
    }

    fn aged(samples: impl IntoIterator<Item = ((String, String), arb_tob::TobSample)>) -> i64 {
        venue_age_s(&cov(samples), "kalshi")
    }

    /// The distinction in this function's doc comment, and the one that cost a
    /// wrong headline. The ToB series emits on CHANGE, so a 13-hour-old sample
    /// on a market nobody has quoted since IS the current book. Coverage is
    /// therefore the NEWEST sample in the rollup, never the oldest: reading it
    /// per market conflates "quiet" with "unknown" and reported 0 of 67 pairs
    /// actionable when 47 had a perfectly fillable book.
    #[test]
    fn a_quiet_market_does_not_age_the_rollup() {
        let age = aged([book("kalshi", "KXQUIET", 13 * 3600), book("kalshi", "KXBUSY", 10)]);
        assert_eq!(age, 10, "the newest sample is the coverage, not the oldest");
        assert!(age <= MAX_COVERAGE_AGE_S, "a rollup sampled 10s ago is current");
    }

    /// What actually goes stale is the rollup itself: built at noon, it knows
    /// nothing about the afternoon. Thirty minutes is the line and it is
    /// inclusive — without this gate the board reported "10 actionable" off
    /// 22-hour-old quotes.
    #[test]
    fn a_rollup_that_stopped_covering_is_not_evidence_about_now() {
        assert_eq!(aged([book("kalshi", "KXONE", MAX_COVERAGE_AGE_S)]), MAX_COVERAGE_AGE_S);
        assert!(aged([book("kalshi", "KXONE", MAX_COVERAGE_AGE_S)]) <= MAX_COVERAGE_AGE_S);
        assert!(aged([book("kalshi", "KXONE", MAX_COVERAGE_AGE_S + 1)]) > MAX_COVERAGE_AGE_S);
    }

    /// An EMPTY rollup is UNKNOWN, not fresh. Zero would read as "sampled this
    /// instant" and mark every row on the board actionable off no book at all.
    #[test]
    fn a_rollup_with_no_samples_is_unknown_not_current() {
        assert_eq!(aged([]), i64::MAX);
        assert!(aged([]) > MAX_COVERAGE_AGE_S);
        assert_eq!(stalest_age_s(&cov([])), i64::MAX);
    }

    /// The gate above was built one venue too coarse, and this is the shape of
    /// the failure: the polymarket_us recorder dies at 09:00, its samples stop
    /// there, kalshi keeps sampling — and a single newest-sample-ANYWHERE age
    /// reads seconds. One venue may never borrow another's freshness.
    #[test]
    fn a_dead_venue_does_not_borrow_a_live_ones_coverage() {
        let c = cov([book("kalshi", "KXBUSY", 10), book("polymarket_us", "PMDEAD", 6 * 3600)]);
        assert_eq!(venue_age_s(&c, "kalshi"), 10, "kalshi is genuinely current");
        assert_eq!(venue_age_s(&c, "polymarket_us"), 6 * 3600, "and polymarket_us genuinely is not");
        assert!(venue_age_s(&c, "polymarket_us") > MAX_COVERAGE_AGE_S);
        assert_eq!(stalest_age_s(&c), 6 * 3600, "a rollup is as current as its worst venue");
        assert_eq!(venue_age_s(&c, "polymarket"), i64::MAX, "a venue with no samples is unknown");
    }

    /// The hole the per-venue gate left, and the one this basis closes. `min`
    /// over EVERY market reports the venue's NOISIEST one, and on pm-us 661 of
    /// the 749 markets in the tape are catalog markets no pair prices. The
    /// market ids here are real ones from the 07-28 tape; the tradable leg is
    /// frozen six hours back and two Billboard markets nothing references are
    /// ticking, which is what a partial freeze looks like from the rollup.
    #[test]
    fn a_market_we_do_not_price_does_not_certify_the_ones_we_do() {
        let c = cov_backed(
            &[("polymarket_us", "tac-nobel-peace-2026-10-09-elomus")],
            [
                book("polymarket_us", "tac-nobel-peace-2026-10-09-elomus", 6 * 3600),
                book("polymarket_us", "ccpc-bilbrd-1album-any2026-eminem", 1),
                book("polymarket_us", "ccpc-bilbrd-1album-any2026-ladgag", 2),
            ],
        );
        assert_eq!(venue_age_s(&c, "polymarket_us"), 6 * 3600, "only priced books certify");
        assert!(venue_age_s(&c, "polymarket_us") > MAX_COVERAGE_AGE_S);
    }

    /// The end of that same road: not one market the registry names is in the
    /// venue's rollup. UNKNOWN, not fresh — a tape still being written for
    /// markets nothing prices is evidence about the tape and none at all about
    /// the books this board prices. The headline is unchanged because it
    /// reports the venues the rollup COVERS; what carries the hole is the
    /// venue's own chip and the row gate, both of which read `venue_age_s`.
    #[test]
    fn a_venue_whose_rollup_holds_no_market_we_price_is_unknown_not_fresh() {
        let c = cov_backed(
            &[("kalshi", "KXONE")],
            [
                book("kalshi", "KXONE", 10),
                book("polymarket_us", "ccpc-bilbrd-1album-any2026-eminem", 1),
            ],
        );
        assert_eq!(venue_age_s(&c, "kalshi"), 10, "kalshi is genuinely current");
        assert_eq!(venue_age_s(&c, "polymarket_us"), i64::MAX, "an unpriced tape is not coverage");
        assert!(venue_age_s(&c, "polymarket_us") > MAX_COVERAGE_AGE_S, "so no row on it is live");
        assert_eq!(stalest_age_s(&c), 10, "a venue we have no evidence about is a hole, not an age");
    }
}
