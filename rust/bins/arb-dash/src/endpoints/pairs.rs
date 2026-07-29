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
    let opp = opps::summarize(&sources, Some(&rel_id)).unwrap_or_default();

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
