//! What the engine is RESTING on right now.
//!
//! Four steps, and they are separable: fold the intent tape, index the
//! registry, join our quotes to the venue books, price every route. The
//! per-pair work is `Pair` below; `json` is the wiring and the summary.

use std::collections::HashMap;

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_query::intents;
use arb_registry::{Allowlist, Leg, Registry, Relationship};
use arb_scenario::price_at;
use arb_tob::{series, TobSample};

use crate::endpoints::{coverage_age_s, rollup_paths, Latest, MAX_COVERAGE_AGE_S};
use crate::{integrity, Args, FEE_CATEGORY};

/// market -> relationship. A market can back more than one pair; the first
/// is enough to name the row, and the leg lookup below is exact anyway.
fn market_index(reg: &Registry) -> HashMap<(String, String), String> {
    let mut by_market: HashMap<(String, String), String> = HashMap::new();
    for r in &reg.relationships {
        if r.legs.len() != 2 {
            continue;
        }
        for l in &r.legs {
            by_market
                .entry((l.venue.clone(), l.market_id.clone()))
                .or_insert_with(|| r.id.clone());
        }
    }
    by_market
}

/// Our resting quote per (venue, market, side).
type Resting<'a> = HashMap<(String, String, String), &'a intents::LiveOrder>;

/// Every quote the fold says we still have up, plus the pairs those quotes
/// belong to, in the order the fold saw them.
fn resting<'a>(
    st: &'a intents::IntentState,
    by_market: &HashMap<(String, String), String>,
) -> (Resting<'a>, Vec<String>) {
    let mut ours: Resting = HashMap::new();
    let mut rels: Vec<String> = Vec::new();
    for o in &st.live {
        if let Some(rel) = by_market.get(&(o.venue.clone(), o.market.clone())) {
            if !rels.contains(rel) {
                rels.push(rel.clone());
            }
        }
        ours.insert((o.venue.clone(), o.market.clone(), o.side.clone()), o);
    }
    (ours, rels)
}

/// One pair with both sides already looked up: our resting quotes from the
/// intent tape, the venue books from the ToB rollup.
struct Pair<'a> {
    rel: &'a Relationship,
    la: &'a Leg,
    lb: &'a Leg,
    va: Venue,
    vb: Venue,
    rest_a: Option<&'a intents::LiveOrder>,
    rest_b: Option<&'a intents::LiveOrder>,
    qa: Option<&'a TobSample>,
    qb: Option<&'a TobSample>,
}

impl<'a> Pair<'a> {
    /// `None` for a pair we are resting nothing on — it has no row here by
    /// construction — or one whose venues we cannot price.
    fn join(
        reg: &'a Registry,
        rel_id: &str,
        ours: &Resting<'a>,
        latest: &'a Latest,
    ) -> Option<Pair<'a>> {
        let r = reg.get(rel_id)?;
        let (la, lb) = (&r.legs[0], &r.legs[1]);
        let (Some(va), Some(vb)) = (Venue::parse(&la.venue), Venue::parse(&lb.venue)) else {
            return None;
        };

        let get = |l: &Leg, side: &str| {
            ours.get(&(l.venue.clone(), l.market_id.clone(), side.to_string())).copied()
        };
        // Buying YES on A means resting a BID; selling YES on B means resting an ASK.
        let rest_a = get(la, "bid");
        let rest_b = get(lb, "ask");
        if rest_a.is_none() && rest_b.is_none() {
            return None;
        }
        let qa = latest.get(&(la.venue.clone(), la.market_id.clone()));
        let qb = latest.get(&(lb.venue.clone(), lb.market_id.clone()));
        Some(Pair { rel: r, la, lb, va, vb, rest_a, rest_b, qa, qb })
    }
}

/// ROUTES, not a single style. Resting on both legs is not "make/make
/// hoping both fill" — it is TWO make-take entry points: whichever side
/// gets hit, we immediately take the other. So price every route the
/// pair currently supports and rank on the best one.
///
///   take-take      cross both books now (no resting needed)
///   make-a-take-b  our bid on A fills -> take B at its bid
///   take-a-make-b  our ask on B fills -> take A at its ask
fn routes_for(
    p: &Pair,
    cx: &mut Cx,
    sched: &FeeSchedule,
    clip: D,
) -> (serde_json::Map<String, serde_json::Value>, Option<(f64, &'static str)>) {
    let (va, vb) = (p.va, p.vb);
    let mut routes = serde_json::Map::new();
    let mut best: Option<(f64, &'static str)> = None;
    let mut consider = |label: &'static str,
                        ra: Role, rb: Role,
                        ea: Option<String>, eb: Option<String>,
                        cx: &mut Cx,
                        routes: &mut serde_json::Map<String, serde_json::Value>,
                        best: &mut Option<(f64, &'static str)>| {
        let (Some(ea), Some(eb)) = (ea, eb) else { return };
        let Some(p) = price_at(cx, sched, label, va, vb, ra, rb, &ea, &eb, clip, FEE_CATEGORY)
        else {
            return;
        };
        if let Ok(e) = p.edge_per_contract.parse::<f64>() {
            if best.map_or(true, |(b, _)| e > b) {
                *best = Some((e, label));
            }
        }
        routes.insert(label.into(), serde_json::to_value(&p).unwrap());
    };

    let va_ask = p.qa.and_then(|s| s.ask.clone());
    let vb_bid = p.qb.and_then(|s| s.bid.clone());
    // the immediate crossing — always available if both books are known
    consider("take-take", Role::Taker, Role::Taker,
             va_ask.clone(), vb_bid.clone(), cx, &mut routes, &mut best);
    // our resting bid on A fills, then we take B
    if let Some(o) = p.rest_a {
        consider("make-a-take-b", Role::Maker, Role::Taker,
                 Some(o.price.clone()), vb_bid.clone(), cx, &mut routes, &mut best);
    }
    // our resting ask on B fills, then we take A
    if let Some(o) = p.rest_b {
        consider("take-a-make-b", Role::Taker, Role::Maker,
                 va_ask.clone(), Some(o.price.clone()), cx, &mut routes, &mut best);
    }
    (routes, best)
}

/// Seconds since a market's last ToB sample.
fn q_age(s: Option<&TobSample>, now: f64) -> Option<i64> {
    s.map(|s| ((now * 1e9) as i64 - s.ts_local_ns) / 1_000_000_000)
}

fn spread_of(s: Option<&TobSample>) -> Option<f64> {
    let s = s?;
    Some(s.ask.as_ref()?.parse::<f64>().ok()? - s.bid.as_ref()?.parse::<f64>().ok()?)
}

fn size_at(s: Option<&TobSample>, ask: bool) -> Option<f64> {
    let s = s?;
    let v = if ask { s.ask_size.as_ref() } else { s.bid_size.as_ref() };
    v?.parse::<f64>().ok()
}

/// Whether the best route is a trade or only a number: can we rest where it
/// needs us to, and is there size behind what it makes us cross.
struct Feasible {
    rest_spread: Option<f64>,
    fillable: bool,
    take_depth: Option<f64>,
    deep_enough: bool,
    have_books: bool,
}

fn feasible(p: &Pair, best_route: &str) -> Feasible {
    // Fill plausibility applies to the leg we REST on, and only for the
    // make-take routes; a take-take needs no rest at all.
    let rest_spread: Option<f64> = match best_route {
        "make-a-take-b" => spread_of(p.qa),
        "take-a-make-b" => spread_of(p.qb),
        _ => None,
    };
    const MAX_REST_SPREAD: f64 = 0.05;
    let fillable = rest_spread.map_or(true, |s| s <= MAX_REST_SPREAD);
    let have_books = p.qa.is_some() && p.qb.is_some();

    // Depth at the touch on the legs this route CROSSES. A rested leg
    // needs none — we are the size there — but a taken leg is only good
    // for what is actually resting behind the price. `None` means the
    // tape carried no size for a leg we would have to cross, which is not
    // permission to assume it is deep.
    let takes: &[Option<f64>] = match best_route {
        "take-take" => &[size_at(p.qa, true), size_at(p.qb, false)],
        "make-a-take-b" => &[size_at(p.qb, false)],
        "take-a-make-b" => &[size_at(p.qa, true)],
        _ => &[],
    };
    let take_depth = takes
        .iter()
        .try_fold(f64::INFINITY, |m: f64, d| d.map(|d| m.min(d)))
        .filter(|d| d.is_finite());
    const CLIP: f64 = 25.0;
    let deep_enough = take_depth.is_some_and(|d| d >= CLIP);

    Feasible { rest_spread, fillable, take_depth, deep_enough, have_books }
}

/// Actionable first. An un-fillable or stale-priced row is information, not
/// a trade, and must not head the list.
fn rank(rows: &mut [serde_json::Value]) {
    rows.sort_by(|x, y| {
        let ok = |v: &serde_json::Value| v["actionable"].as_bool().unwrap_or(false);
        let f = |v: &serde_json::Value| {
            v["edge"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::MIN)
        };
        ok(y).cmp(&ok(x)).then(f(y).partial_cmp(&f(x)).unwrap_or(std::cmp::Ordering::Equal))
    });
}

/// What the engine would have RESTING right now, joined to the pair it belongs
/// to and priced with fees broken out.
///
/// The intent stream is MAKER-ONLY: place, reprice, cancel. A take-take
/// crossing is an immediate execution, never a resting quote, so it cannot
/// appear here by construction — that lives in the scenario view. What the
/// stream DOES tell us is which legs we are resting on, and that is the
/// execution style:
///   resting on leg A only  -> make A, take B when it fills
///   resting on leg B only  -> take A, make B
///   resting on both        -> make/make (no taking at all)
///
/// The take leg is priced off the venue quote from the ToB rollup, so its age
/// is reported per row: a stale quote makes the edge a guess, not a number.
pub fn json(a: &Args) -> String {
    let text = match std::fs::read_to_string(&a.intents_path) {
        Ok(t) => t,
        Err(e) => {
            return serde_json::json!({
                "error": format!("read {}: {e}", a.intents_path),
                "hint": "start the dry-run engine: systemctl --user start arbbot-trader-rs",
            })
            .to_string()
        }
    };
    let st = intents::fold(&text);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let age = if st.last_ts > 0.0 { now - st.last_ts } else { -1.0 };

    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);
    let (ours, rels) = resting(&st, &market_index(&reg));

    let day = integrity::build(&a.data_dir).today;
    let latest = series::latest_by_market(&rollup_paths(&a.rollup_dir, &day));
    let coverage_age_s = coverage_age_s(&latest, (now * 1e9) as i64);
    let rollup_current = coverage_age_s <= MAX_COVERAGE_AGE_S;

    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let clip = cx.parse_exact("25");

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for rel_id in rels {
        let Some(p) = Pair::join(&reg, &rel_id, &ours, &latest) else { continue };
        let (routes, best) = routes_for(&p, &mut cx, &sched, clip);
        if routes.is_empty() {
            continue;
        }
        let (best_edge, best_route) = best.unwrap_or((f64::MIN, "none"));
        let priced = routes.get(best_route).cloned();
        let f = feasible(&p, best_route);
        let actionable =
            f.fillable && f.deep_enough && f.have_books && rollup_current && best_edge > 0.0;

        rows.push(serde_json::json!({
            "relationship_id": rel_id,
            "tradable": p.rel.tradable(&allow),
            // Polymarket international's taker fee is category-dependent and we
            // price it at the default; see FEE_CATEGORY.
            "intl_leg": p.la.venue == "polymarket" || p.lb.venue == "polymarket",
            "best_route": best_route,
            "routes": routes,
            "leg_a": {
                "venue": p.la.venue, "market": p.la.market_id,
                "our_bid": p.rest_a.map(|o| o.price.clone()),
                "our_count": p.rest_a.map(|o| o.count),
                "reprices": p.rest_a.map(|o| o.reprices),
                "venue_bid": p.qa.and_then(|s| s.bid.clone()),
                "venue_ask": p.qa.and_then(|s| s.ask.clone()),
                "quote_age_s": q_age(p.qa, now),
            },
            "leg_b": {
                "venue": p.lb.venue, "market": p.lb.market_id,
                "our_ask": p.rest_b.map(|o| o.price.clone()),
                "our_count": p.rest_b.map(|o| o.count),
                "reprices": p.rest_b.map(|o| o.reprices),
                "venue_bid": p.qb.and_then(|s| s.bid.clone()),
                "venue_ask": p.qb.and_then(|s| s.ask.clone()),
                "quote_age_s": q_age(p.qb, now),
            },
            "priced": priced,
            "edge_f": best_edge,
            "rest_spread": f.rest_spread,
            "fillable": f.fillable,
            "take_depth": f.take_depth,
            "deep_enough": f.deep_enough,
            "have_books": f.have_books,
            // Time since this market's book last MOVED — information about how
            // quiet it is, deliberately not a staleness flag.
            "since_change_s": q_age(p.qa, now).unwrap_or(-1).max(q_age(p.qb, now).unwrap_or(-1)),
            "actionable": actionable,
            "edge": priced.as_ref().and_then(|p| p["edge_per_contract"].as_str().map(String::from)),
            "last_ts": p.rest_a.map(|o| o.ts).into_iter()
                        .chain(p.rest_b.map(|o| o.ts)).fold(0.0f64, f64::max),
        }));
    }

    rank(&mut rows);
    let actionable = rows.iter().filter(|v| v["actionable"].as_bool().unwrap_or(false)).count();

    serde_json::json!({
        "actionable": actionable,
        "max_rest_spread": 0.05,
        "min_take_depth": 25,
        "rollup_current": rollup_current,
        "rollup_coverage_age_s": if coverage_age_s == i64::MAX { -1 } else { coverage_age_s },
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "engine_live": age >= 0.0 && age < 120.0,
        "last_intent_age_s": age.round() as i64,
        "resting_orders": st.live.len(),
        "pairs": rows.len(),
        "opens": st.opens, "reprices": st.reprices, "cancels": st.cancels,
        "total": st.total,
        "clip": "25",
        "fee_category": FEE_CATEGORY,
        "maker_only": true,
        "rows": rows,
    })
    .to_string()
}
