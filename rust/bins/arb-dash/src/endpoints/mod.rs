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
/// reports the freshness of its NOISIEST market. The 2026-07-28 polymarket_us
/// tape carries 749 markets and the registry names 88 of them; the rest are the
/// sports catalog `arb-recorder` subscribes by tag. A recorder that loses the
/// arb subscription while sports keep flowing — this repo's documented silent
/// eviction — still read SECONDS on the venue, painted the chip green, and left
/// every arb row `actionable` off books that stopped hours ago.
///
/// ALL registry legs, not the tradable ones. Coverage is a claim about the
/// RECORDER — `config::load_universe` builds its subscription from exactly
/// these legs — while vetting is a claim about a PAIR, so a `verdict: rejected`
/// must not move a venue's freshness. And the narrow subset is measurably
/// wrong: only 6 polymarket markets back a tradable pair, and the newest of
/// those 6 is 3 to 17 hours old on all three real tapes (07-26..28) while the
/// feed is demonstrably alive. That is the quiet-market trap this doc comment
/// opens with, re-entered through a basis too narrow to notice a tick.
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
/// rollup holding only markets no pair prices is exactly that: it is evidence
/// that the venue's socket is up, and none at all about the books this board
/// prices off.
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
pub fn coverage_json(cov: &Coverage) -> serde_json::Value {
    VENUES
        .iter()
        .map(|v| {
            let age = venue_age_s(cov, v);
            serde_json::json!({
                "venue": v,
                "coverage_age_s": if age == i64::MAX { -1 } else { age },
                "current": age <= MAX_COVERAGE_AGE_S,
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
    /// venue's other markets are recorded (sports, by tag) and priced by
    /// nothing.
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
    /// over EVERY market reports the venue's NOISIEST one: polymarket_us
    /// carries 749 markets and the registry names 88, the rest being the sports
    /// catalog the recorder subscribes by tag. A recorder that silently loses
    /// the arb subscription while sports keep flowing then read SECONDS on a
    /// venue whose every priced book had stopped hours ago.
    #[test]
    fn a_sports_feed_does_not_certify_the_arb_book_it_stopped_carrying() {
        let c = cov_backed(
            &[("polymarket_us", "PMARB")],
            [
                book("polymarket_us", "PMARB", 6 * 3600),
                book("polymarket_us", "nfl-kc-buf", 1),
                book("polymarket_us", "nba-lal-bos", 2),
            ],
        );
        assert_eq!(venue_age_s(&c, "polymarket_us"), 6 * 3600, "only priced books certify");
        assert!(venue_age_s(&c, "polymarket_us") > MAX_COVERAGE_AGE_S);
    }

    /// The end of that same road: the arb subscription never arrived at all and
    /// the rollup is sports only. UNKNOWN, not fresh — a ticking sports catalog
    /// is evidence the venue's socket is up and none at all about the books
    /// this board prices. The headline is unchanged because it reports the
    /// venues the rollup COVERS; what carries the hole is the venue's own chip
    /// and the row gate, both of which read `venue_age_s`.
    #[test]
    fn a_venue_whose_rollup_holds_no_market_we_price_is_unknown_not_fresh() {
        let c = cov_backed(
            &[("kalshi", "KXONE")],
            [book("kalshi", "KXONE", 10), book("polymarket_us", "nfl-kc-buf", 1)],
        );
        assert_eq!(venue_age_s(&c, "kalshi"), 10, "kalshi is genuinely current");
        assert_eq!(venue_age_s(&c, "polymarket_us"), i64::MAX, "sports are not our coverage");
        assert!(venue_age_s(&c, "polymarket_us") > MAX_COVERAGE_AGE_S, "so no row on it is live");
        assert_eq!(stalest_age_s(&c), 10, "a venue we have no evidence about is a hole, not an age");
    }
}
