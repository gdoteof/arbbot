//! The register of tracked pairs, and one pair end to end.

use arb_core::resolve::{iso_from_day, parse_iso};
use arb_query::{opps, sources_for_range};
use arb_registry::{Allowlist, Registry};
use arb_tob::series;

use crate::http::query_param;
use crate::{integrity, Args};

/// The register of tracked pairs. Distinguishes DETECTED from PERMITTED: a
/// pair can carry the best edge on the board and still be untradable because
/// only a human verdict opens the gate (registry/model.py:132).
pub fn list_json(a: &Args) -> String {
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);
    let rows: Vec<serde_json::Value> = reg
        .relationships
        .iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "kind": r.kind,
                "verdict": r.verdict,
                "vetted_by": r.vetted_by,
                "tradable": r.tradable(&allow),
                "human_vetted": r.human_vetted(),
                "oracle_risk": r.oracle_risk,
                "tranche": r.tranche,
                "legs": r.legs.iter().map(|l| format!("{}:{}", l.venue, l.market_id))
                        .collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "count": rows.len(),
        "tradable": reg.relationships.iter().filter(|r| r.tradable(&allow)).count(),
        "human_vetted": reg.relationships.iter().filter(|r| r.human_vetted()).count(),
        "allowlisted": allow.len(),
        "rows": rows,
    })
    .to_string()
}

/// One pair, end to end: what we asserted about it, both legs' quotes over
/// time, and the opportunity the scanner logged. The chart's load-bearing
/// series is `basket_cost` — below 1.00 is an arb before fees, so the arb is
/// visible as an AREA rather than something you infer from two lines.
pub fn detail_json(a: &Args, query: &str) -> String {
    let Some(rel_id) = query_param(query, "rel") else {
        return "{\"error\":\"rel is required\"}".into();
    };
    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let Some(r) = reg.get(&rel_id) else {
        return format!("{{\"error\":\"unknown relationship {rel_id}\"}}");
    };
    if r.legs.len() < 2 {
        return format!("{{\"error\":\"{rel_id} has {} leg(s)\"}}", r.legs.len());
    }
    let to = query_param(query, "to").unwrap_or_else(|| integrity::build(&a.data_dir).today);
    let from = query_param(query, "from").unwrap_or_else(|| to.clone());

    // ToB files are per venue per day; collect the days in range that exist.
    let days = day_range(&from, &to);
    let paths = |venue: &str| -> Vec<String> {
        days.iter()
            .map(|d| format!("{}/tob-{venue}-{d}.jsonl", a.rollup_dir))
            .filter(|p| std::path::Path::new(p).is_file())
            .collect()
    };
    let (la, lb) = (&r.legs[0], &r.legs[1]);
    let sa = series::load_market(&paths(&la.venue), &la.venue, &la.market_id);
    let sb = series::load_market(&paths(&lb.venue), &lb.venue, &lb.market_id);
    let points = series::align(&sa, &sb);
    // A basket cost is only believable if BOTH legs quoted recently. Anything
    // older is forward-fill: a price that was never simultaneously available.
    const FRESH_NS: i64 = 300_000_000_000; // 5 min
    let priced = points.iter().filter(|p| p.basket_cost.is_some()).count();
    let fresh: Vec<&series::AlignedPoint> =
        points.iter().filter(|p| p.is_fresh(FRESH_NS) && p.basket_cost.is_some()).collect();
    let fresh_under_1 = fresh.iter().filter(|p| p.basket_cost.unwrap() < 1.0).count();
    let best_fresh = fresh
        .iter()
        .filter_map(|p| p.basket_cost)
        .fold(f64::INFINITY, f64::min);

    let sources = sources_for_range(&a.scan_dir, &a.parquet_dir, "opportunities", &from, &to);
    // Every Err out of `summarize` is a read that FAILED. `unwrap_or_default()`
    // turned each one into an empty Vec, which serialized as
    // `"opportunity": null` — identical to a pair the scanner watched and never
    // logged, and rendered as "no opportunity in range". Silence and evidence
    // of absence are opposite facts.
    //
    // Reported BESIDE the payload, not as the top-level `error` that
    // `/api/opportunities` returns, because the two are not the same shape of
    // answer. There, the error kills a panel that is entirely about
    // opportunities; here it would trip `if(d.error) return fail(...)` in the
    // router and replace the whole drill-down — including the basket-cost
    // chart, which comes off the ToB rollup and the registry and is the main
    // forensic instrument during exactly the broken-archive incident this
    // reports. One unreadable file must not blank seven fields it never fed.
    let (opp, opp_error) = match opps::summarize(&sources, Some(&rel_id)) {
        Ok(rows) => (rows, None),
        Err(e) => (Vec::new(), Some(e)),
    };

    serde_json::json!({
        "from": from, "to": to,
        "relationship": r,
        "tradable": r.tradable(&Allowlist::load(&a.tradable)),
        "human_vetted": r.human_vetted(),
        "leg_a": format!("{}:{}", la.venue, la.market_id),
        "leg_b": format!("{}:{}", lb.venue, lb.market_id),
        "samples_a": sa.len(),
        "samples_b": sb.len(),
        "points": points,
        "fresh_max_age_s": FRESH_NS / 1_000_000_000,
        "priced_points": priced,
        "fresh_points": fresh.len(),
        "fresh_points_under_1": fresh_under_1,
        "best_fresh_cost": if best_fresh.is_finite() { Some(best_fresh) } else { None },
        "opportunity": opp.first(),
        "opportunity_error": opp_error,
    })
    .to_string()
}

/// Inclusive YYYY-MM-DD range. Capped so a careless URL cannot ask the server
/// to stat thousands of files.
fn day_range(from: &str, to: &str) -> Vec<String> {
    // Epoch days in, ISO strings out. This used to carry its own ISO parser,
    // its own days-from-civil, and a month-length table with the leap-year rule
    // written out — a fourth calendar in a workspace that already had one, and
    // the only one whose off-by-one on negative years needed a paragraph of
    // comment to explain away. `arb_core::resolve` is that one calendar.
    let (Some(a0), Some(b0)) = (parse_iso(from), parse_iso(to)) else { return vec![] };
    if b0 < a0 || b0 - a0 > 400 {
        return vec![];
    }
    (a0..=b0).map(iso_from_day).collect()
}

#[cfg(test)]
mod tests {
    use super::{day_range, detail_json};
    use crate::Args;

    const REGISTRY: &str = "\
relationships:
  - id: xvus-fixture
    legs:
      - venue: kalshi
        market_id: K
      - venue: polymarket_us
        market_id: P
";

    /// A dash whose scan archive holds exactly one day, in whichever state the
    /// caller wants it. The registry is real (the endpoint loads one before it
    /// reads anything); the ToB rollup is not, so the chart is empty and the
    /// only thing under test is what the opportunity read answers.
    fn args(name: &str, parquet: Option<&[u8]>, jsonl: Option<&str>) -> (Args, String) {
        let base =
            std::env::temp_dir().join(format!("arb-dash-pairs-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("scan")).unwrap();
        std::fs::create_dir_all(base.join("parquet")).unwrap();
        std::fs::write(base.join("registry.yaml"), REGISTRY).unwrap();
        if let Some(b) = parquet {
            std::fs::write(base.join("parquet/opportunities-2026-07-27.parquet"), b).unwrap();
        }
        if let Some(t) = jsonl {
            std::fs::write(base.join("scan/opportunities-2026-07-27.jsonl"), t).unwrap();
        }
        let mut a = Args::for_test();
        a.scan_dir = base.join("scan").to_string_lossy().to_string();
        a.parquet_dir = base.join("parquet").to_string_lossy().to_string();
        a.registry = base.join("registry.yaml").to_string_lossy().to_string();
        (a, base.to_string_lossy().to_string())
    }

    fn detail(a: &Args) -> serde_json::Value {
        let raw = detail_json(a, "rel=xvus-fixture&from=2026-07-27&to=2026-07-27");
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("not json ({e}): {raw}"))
    }

    /// A read that FAILED must not answer the question. `unwrap_or_default()`
    /// collapsed every `Err` from `opps::summarize` into an empty Vec, so an
    /// unreadable archive serialized as `"opportunity": null` — the same bytes
    /// a pair the scanner watched all day and never logged produces, and the
    /// UI says "the scanner logged no opportunity for this pair in range" over
    /// both. Silence and evidence of absence are opposite facts.
    ///
    /// The rest of the payload SURVIVES: the basket-cost chart is read off the
    /// ToB rollup and the registry, it is the instrument an operator wants
    /// most while an archive is broken, and a top-level `error` would blank it
    /// (`index.html`: `if(d.error) return fail(f, d.error)`).
    #[test]
    fn an_unreadable_scan_archive_is_an_error_beside_the_payload_not_instead_of_it() {
        // Magic at both ends and garbage between: a file the resolver is right
        // to hand over and the reader cannot open.
        let (a, base) = args("broken", Some(b"PAR1not a parquetPAR1"), None);
        let out = detail(&a);
        assert!(
            out["opportunity_error"]
                .as_str()
                .is_some_and(|e| e.contains("opportunities-2026-07-27.parquet")),
            "the failing file is not named: {out}"
        );
        assert!(out["opportunity"].is_null(), "a failed read still answered: {out}");
        assert!(out["error"].is_null(), "one unreadable file must not blank the drill-down");
        assert_eq!(out["relationship"]["id"], "xvus-fixture", "the payload survived: {out}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The distinction that has to survive the fix. A tape that was read and
    /// holds nothing for this pair is still `"opportunity": null` and no
    /// error — otherwise the drill-down cries wolf on every quiet pair, which
    /// is the same defect pointed the other way.
    #[test]
    fn a_tape_that_was_read_and_held_nothing_is_still_null() {
        let (a, base) = args(
            "quiet",
            None,
            Some("{\"relationship_id\":\"someone-else\",\"ts_local_ns\":0}\n"),
        );
        let out = detail(&a);
        assert!(out["opportunity_error"].is_null(), "a readable tape reported an error: {out}");
        assert!(out["opportunity"].is_null(), "{out}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Every day this returns becomes a `tob-<venue>-<day>.jsonl` stat, three
    /// venues wide, so an unbounded or malformed range is a filesystem storm
    /// driven straight off a URL.
    #[test]
    fn a_range_of_one_day_is_that_day() {
        assert_eq!(day_range("2026-07-28", "2026-07-28"), ["2026-07-28"]);
    }

    /// `to` before `from` is a typo in a query string, and the answer is no
    /// days.
    ///
    /// Be honest about what this catches: the explicit `b0 < a0` guard is
    /// REDUNDANT, because `(a0..=b0)` with `a0 > b0` is already an empty
    /// iterator, and deleting that guard leaves this test green. Verified by
    /// mutation. It pins the outcome, not the line — which is what a caller
    /// depends on, and what would break if this were ever rewritten as a
    /// hand-rolled loop over days.
    #[test]
    fn a_reversed_range_is_empty_not_walked_backwards() {
        assert!(day_range("2026-07-28", "2026-07-01").is_empty());
        assert!(day_range("2026-07-28", "2026-07-27").is_empty(), "one day reversed");
    }

    /// The cap is on the SPAN, not the count: 400 days apart is 401 days of
    /// files and is served, and one day further is refused outright rather
    /// than truncated — a silently shortened range would answer a question
    /// nobody asked.
    #[test]
    fn the_cap_is_four_hundred_days_apart_and_refuses_rather_than_truncates() {
        assert_eq!(day_range("2025-01-01", "2026-02-05").len(), 401);
        assert!(day_range("2025-01-01", "2026-02-06").is_empty());
    }

    /// This function used to carry its own days-from-civil and its own
    /// month-length table with the leap-year rule written out. February 29th
    /// is the day that table existed to get right.
    #[test]
    fn a_leap_day_is_walked_not_skipped() {
        assert_eq!(
            day_range("2024-02-28", "2024-03-01"),
            ["2024-02-28", "2024-02-29", "2024-03-01"]
        );
        assert_eq!(
            day_range("2026-02-27", "2026-03-01"),
            ["2026-02-27", "2026-02-28", "2026-03-01"],
            "2026 is not a leap year"
        );
    }

    /// A year boundary is not a special case and must not become one.
    #[test]
    fn a_range_crosses_new_year_without_a_gap() {
        assert_eq!(
            day_range("2025-12-30", "2026-01-02"),
            ["2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"]
        );
    }

    /// A date this cannot read yields NO days rather than a range anchored at
    /// the epoch: `?from=yesterday` would otherwise ask for twenty thousand.
    #[test]
    fn an_unreadable_date_yields_no_days_at_all() {
        for bad in ["", "yesterday", "2026-7-28", "20260728", "2026-13-01", "2026-07-32"] {
            assert!(day_range(bad, "2026-07-28").is_empty(), "`{bad}` was read as a from-date");
            assert!(day_range("2026-07-28", bad).is_empty(), "`{bad}` was read as a to-date");
        }
    }
}
