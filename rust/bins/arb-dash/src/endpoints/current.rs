//! Top N baskets by edge, priced off the current book.
//!
//! Same four steps as the intents view: read the URL, load the rollup, price
//! each pair, rank and summarise.

use arb_core::fees::FeeSchedule;
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_registry::{Allowlist, Registry, Relationship};
use arb_scenario::{price, Priced, Quote, Scenario};
use arb_tob::series;

use crate::endpoints::{coverage_age_s, rollup_paths, Latest, MAX_COVERAGE_AGE_S};
use crate::http::query_param;
use crate::{integrity, Args, FEE_CATEGORY};

/// The knobs this view takes off the URL, with their defaults.
struct Query {
    scenario: Scenario,
    clip_s: String,
    n: usize,
    day: String,
    only_tradable: bool,
    max_spread_s: String,
}

impl Query {
    fn parse(a: &Args, query: &str) -> Query {
        Query {
            scenario: query_param(query, "scenario")
                .and_then(|s| Scenario::parse(&s))
                .unwrap_or(Scenario::TakeTake),
            clip_s: query_param(query, "clip").unwrap_or_else(|| "25".into()),
            n: query_param(query, "n").and_then(|s| s.parse().ok()).unwrap_or(25),
            day: query_param(query, "day")
                .unwrap_or_else(|| integrity::build(&a.data_dir).today),
            // Default to the permitted universe. Detected-but-not-permitted edge is
            // genuinely worth seeing (it is the case for vetting a pair), but it should
            // be asked for, not ranked first by default.
            only_tradable: query_param(query, "all").as_deref() != Some("1"),
            max_spread_s: query_param(query, "max_spread").unwrap_or_else(|| "0.05".into()),
        }
    }
}

/// The arithmetic context and the sizes every row is priced at, fixed for the
/// whole request.
struct Pricing {
    cx: Cx,
    sched: FeeSchedule,
    clip: D,
    max_spread: D,
}

impl Pricing {
    fn new(q: &Query) -> Pricing {
        let mut cx = Cx::default();
        let sched = FeeSchedule::new(&mut cx);
        let clip = cx.parse_exact(&q.clip_s);
        let max_spread = cx.parse_exact(&q.max_spread_s);
        Pricing { cx, sched, clip, max_spread }
    }

    /// Every scenario is priced for every pair, so switching the view is a
    /// re-sort rather than a re-read, and a pair that is unprofitable to take
    /// but profitable to make is visible as such.
    ///
    /// `None` when the scenario the caller asked for does not price at all.
    fn all_scenarios(
        &mut self,
        want: Scenario,
        va: Venue,
        vb: Venue,
        qa: &Quote,
        qb: &Quote,
    ) -> Option<(Priced, serde_json::Map<String, serde_json::Value>)> {
        let mut by_scenario = serde_json::Map::new();
        let mut chosen: Option<Priced> = None;
        for sc in Scenario::all() {
            if let Some(p) = price(
                &mut self.cx, &self.sched, sc, va, vb, qa, qb, self.clip, FEE_CATEGORY,
                self.max_spread,
            ) {
                if sc == want {
                    chosen = Some(p.clone());
                }
                by_scenario.insert(sc.as_str().into(), serde_json::to_value(&p).unwrap());
            }
        }
        Some((chosen?, by_scenario))
    }
}

/// One row of the board, or `None` for a pair this view cannot rank: not two
/// legs, an unknown venue, no book on one side, unpriceable under the chosen
/// scenario, or not permitted when only permitted pairs were asked for.
fn row(
    p: &mut Pricing,
    q: &Query,
    r: &Relationship,
    allow: &Allowlist,
    latest: &Latest,
    now_ns: i64,
) -> Option<serde_json::Value> {
    if r.legs.len() != 2 {
        return None;
    }
    let (la, lb) = (&r.legs[0], &r.legs[1]);
    let (Some(va), Some(vb)) = (Venue::parse(&la.venue), Venue::parse(&lb.venue)) else {
        return None;
    };
    let ka = (la.venue.clone(), la.market_id.clone());
    let kb = (lb.venue.clone(), lb.market_id.clone());
    let (Some(sa), Some(sb)) = (latest.get(&ka), latest.get(&kb)) else { return None };
    let qa = Quote {
        bid: sa.bid.clone(), bid_size: sa.bid_size.clone(),
        ask: sa.ask.clone(), ask_size: sa.ask_size.clone(),
    };
    let qb = Quote {
        bid: sb.bid.clone(), bid_size: sb.bid_size.clone(),
        ask: sb.ask.clone(), ask_size: sb.ask_size.clone(),
    };

    let (priced, by_scenario) = p.all_scenarios(q.scenario, va, vb, &qa, &qb)?;
    let tradable = r.tradable(allow);
    if q.only_tradable && !tradable {
        return None;
    }
    Some(serde_json::json!({
        "relationship_id": r.id,
        "tradable": tradable,
        "verdict": r.verdict,
        "intl_leg": la.venue == "polymarket" || lb.venue == "polymarket",
        "leg_a": format!("{}:{}", la.venue, la.market_id),
        "leg_b": format!("{}:{}", lb.venue, lb.market_id),
        "quote_age_a_s": (now_ns - sa.ts_local_ns) / 1_000_000_000,
        "quote_age_b_s": (now_ns - sb.ts_local_ns) / 1_000_000_000,
        "edge_per_contract": priced.edge_per_contract,
        "priced": priced,
        "scenarios": by_scenario,
    }))
}

/// Plausible fills first, THEN by edge. Sorting on raw edge alone puts
/// un-fillable wide-spread rests at the top, which is exactly backwards for
/// a view whose job is to say what to trade.
fn rank(rows: &mut [serde_json::Value]) {
    rows.sort_by(|x, y| {
        let ok = |v: &serde_json::Value| v["priced"]["fill_plausible"].as_bool().unwrap_or(false);
        let f = |v: &serde_json::Value| {
            v["edge_per_contract"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::MIN)
        };
        ok(y).cmp(&ok(x)).then(f(y).partial_cmp(&f(x)).unwrap_or(std::cmp::Ordering::Equal))
    });
}

/// Top N baskets by edge under a chosen execution style.
///
/// Ranked on CURRENT quotes — the latest sample in the ToB rollup — so every
/// row carries its quote age. This is as fresh as the rollup, not as fresh as
/// the venue, and saying so is the difference between an instrument and a lie.
pub fn json(a: &Args, query: &str) -> String {
    let q = Query::parse(a, query);

    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);

    let paths = rollup_paths(&a.rollup_dir, &q.day);
    if paths.is_empty() {
        return format!(
            "{{\"error\":\"no ToB rollup for {} — run arb-tob --day {}\"}}", q.day, q.day
        );
    }
    let latest = series::latest_by_market(&paths);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    // How far the rollup COVERS — the same gate the intents view applies, and
    // for the same reason. Ranking today's board off a rollup built this
    // morning is ranking this morning's board. Per-market sample age cannot
    // stand in for it: the series emits on change, so a quiet market's old
    // sample is still the current book.
    let coverage_age_s = coverage_age_s(&latest, now_ns);
    let rollup_current = coverage_age_s <= MAX_COVERAGE_AGE_S;

    let mut pricing = Pricing::new(&q);
    let mut rows: Vec<serde_json::Value> = reg
        .relationships
        .iter()
        .filter_map(|r| row(&mut pricing, &q, r, &allow, &latest, now_ns))
        .collect();

    rank(&mut rows);
    let total = rows.len();
    let profitable = rows
        .iter()
        .filter(|v| v["priced"]["profitable"].as_bool().unwrap_or(false))
        .count();
    // Actionable is a claim about NOW, so a rollup that stopped covering hours
    // ago cannot produce one. Without this the board happily reported "10
    // actionable" off 22-hour-old quotes.
    let actionable = rows
        .iter()
        .filter(|v| {
            rollup_current
                && v["priced"]["profitable"].as_bool().unwrap_or(false)
                && v["priced"]["fill_plausible"].as_bool().unwrap_or(false)
        })
        .count();
    rows.truncate(q.n);

    serde_json::json!({
        "day": q.day,
        "scenario": q.scenario.as_str(),
        "clip": q.clip_s,
        "priced_pairs": total,
        "profitable": profitable,
        "actionable": actionable,
        "rollup_current": rollup_current,
        "rollup_coverage_age_s": if coverage_age_s == i64::MAX { -1 } else { coverage_age_s },
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "max_spread": q.max_spread_s,
        "fee_category": FEE_CATEGORY,
        "only_tradable": q.only_tradable,
        "shown": rows.len(),
        "rows": rows,
    })
    .to_string()
}
