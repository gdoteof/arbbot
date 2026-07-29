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

use crate::endpoints::{
    coverage_by_venue, coverage_json, rollup_paths, stalest_age_s, venue_age_s, Latest,
    MAX_COVERAGE_AGE_S,
};
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
    let consider = |label: &'static str,
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
            if best.is_none_or(|(b, _)| e > b) {
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
    let fillable = rest_spread.is_none_or(|s| s <= MAX_REST_SPREAD);
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
///   resting on both        -> BOTH of the above, priced and ranked
///
/// That last line used to read "make/make (no taking at all)". The code has
/// never done that, `routes_for` says the opposite four lines from here, and
/// `resting_on_both_legs_is_two_make_take_routes_not_one_make_make` now pins
/// which of the two was right.
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
    let cov = coverage_by_venue(&latest, (now * 1e9) as i64);
    let coverage_age_s = stalest_age_s(&cov);
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
        // Per VENUE, and the older of the two: this row is only as current as
        // its stalest leg, and a live feed on one venue is not evidence about
        // the other. A whole-rollup `rollup_current` here let a dead venue's
        // rows ride on a live venue's newest sample.
        let covered = venue_age_s(&cov, &p.la.venue).max(venue_age_s(&cov, &p.lb.venue))
            <= MAX_COVERAGE_AGE_S;
        let actionable =
            f.fillable && f.deep_enough && f.have_books && covered && best_edge > 0.0;

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
        "venues": coverage_json(&cov),
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "engine_live": (0.0..120.0).contains(&age),
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

#[cfg(test)]
mod tests {
    use super::*;

    const NS: i64 = 1_785_250_000_000_000_000;

    fn rel(id: &str, legs: &[(&str, &str)]) -> Relationship {
        let legs: Vec<serde_json::Value> = legs
            .iter()
            .map(|(v, m)| serde_json::json!({ "venue": v, "market_id": m }))
            .collect();
        serde_json::from_value(serde_json::json!({ "id": id, "legs": legs }))
            .expect("relationship fixture")
    }

    fn order(venue: &str, market: &str, side: &str, price: &str) -> intents::LiveOrder {
        intents::LiveOrder {
            order_id: format!("m-{market}-{side}"),
            venue: venue.into(),
            market: market.into(),
            side: side.into(),
            price: price.into(),
            count: 25.0,
            ts: 1_785_250_000.0,
            reprices: 0,
        }
    }

    /// A book with sizes. `None` for a size is a real shape — the tape does
    /// not always carry one — so it is spelled out rather than defaulted.
    fn tob(bid: &str, ask: &str, bid_size: Option<&str>, ask_size: Option<&str>) -> TobSample {
        TobSample {
            venue: "kalshi".into(),
            market_id: "M".into(),
            ts_local_ns: NS,
            bid: Some(bid.into()),
            bid_size: bid_size.map(String::from),
            ask: Some(ask.into()),
            ask_size: ask_size.map(String::from),
        }
    }

    fn deep(bid: &str, ask: &str) -> TobSample {
        tob(bid, ask, Some("100"), Some("100"))
    }

    fn pair<'a>(
        r: &'a Relationship,
        rest_a: Option<&'a intents::LiveOrder>,
        rest_b: Option<&'a intents::LiveOrder>,
        qa: Option<&'a TobSample>,
        qb: Option<&'a TobSample>,
    ) -> Pair<'a> {
        Pair {
            rel: r,
            la: &r.legs[0],
            lb: &r.legs[1],
            va: Venue::parse(&r.legs[0].venue).expect("leg a venue"),
            vb: Venue::parse(&r.legs[1].venue).expect("leg b venue"),
            rest_a,
            rest_b,
            qa,
            qb,
        }
    }

    fn priced(p: &Pair) -> (serde_json::Map<String, serde_json::Value>, Option<(f64, &'static str)>)
    {
        let mut cx = Cx::default();
        let sched = FeeSchedule::new(&mut cx);
        let clip = cx.parse_exact("25");
        routes_for(p, &mut cx, &sched, clip)
    }

    fn xv() -> Relationship {
        rel("xv-test", &[("kalshi", "KXA"), ("polymarket_us", "PMB")])
    }

    // ----- the index that keeps `Pair::join` from panicking ----------------

    /// `Pair::join` reads `legs[0]` and `legs[1]` with no length check of its
    /// own, and handed a one-leg relationship it panics on `legs[1]` —
    /// verified by calling it directly. The ONLY thing between that and a
    /// 500 on a live operator tool is this filter, three functions upstream:
    /// the ids that reach `join` come from `resting`, which reads them out of
    /// this index and nowhere else. The coupling is invisible at both sites
    /// and one edit away, so it is pinned here rather than assumed.
    #[test]
    fn only_two_leg_relationships_are_indexed_because_pair_join_indexes_both() {
        let reg = Registry {
            relationships: vec![
                rel("one-leg", &[("kalshi", "KXSOLO")]),
                xv(),
                rel("three-leg", &[("kalshi", "KX1"), ("kalshi", "KX2"), ("kalshi", "KX3")]),
            ],
        };
        let idx = market_index(&reg);
        let at = |v: &str, m: &str| idx.get(&(v.to_string(), m.to_string())).cloned();
        assert_eq!(at("kalshi", "KXA").as_deref(), Some("xv-test"));
        assert_eq!(at("polymarket_us", "PMB").as_deref(), Some("xv-test"));
        assert_eq!(at("kalshi", "KXSOLO"), None, "a one-leg id would panic Pair::join");
        assert_eq!(at("kalshi", "KX1"), None, "a three-leg id would silently lose leg 3");
        for id in idx.values() {
            let n = reg.get(id).expect("indexed id is in the registry").legs.len();
            assert_eq!(n, 2, "{id} reached the index with {n} legs");
        }
    }

    /// The same coupling from the other end. We can genuinely be resting a
    /// quote on a market that only a one-leg relationship names, and the fold
    /// hands that order to `resting` like any other — it must yield no pair to
    /// join, while the order itself stays tracked.
    #[test]
    fn a_quote_on_a_one_leg_relationship_produces_no_pair_to_join() {
        let reg = Registry { relationships: vec![rel("one-leg", &[("kalshi", "KXSOLO")])] };
        let st = intents::IntentState {
            live: vec![order("kalshi", "KXSOLO", "bid", "0.40")],
            ..Default::default()
        };
        let (ours, rels) = resting(&st, &market_index(&reg));
        assert!(rels.is_empty(), "nothing to join, so nothing indexes legs[1]");
        assert_eq!(ours.len(), 1, "the order is still ours and still tracked");
    }

    // ----- what counts as OUR quote on a pair ------------------------------

    /// We buy YES on leg A and sell YES on leg B, so our quote on A is a BID
    /// and our quote on B is an ASK. Quotes on the other sides belong to some
    /// other strategy; joining them would price a basket we are not working.
    #[test]
    fn ours_is_a_bid_on_leg_a_and_an_ask_on_leg_b_and_nothing_else() {
        let reg = Registry { relationships: vec![xv()] };
        let latest = Latest::new();
        let wrong = [order("kalshi", "KXA", "ask", "0.40"), order("polymarket_us", "PMB", "bid", "0.60")];
        let mut ours: Resting = HashMap::new();
        for o in &wrong {
            ours.insert((o.venue.clone(), o.market.clone(), o.side.clone()), o);
        }
        assert!(
            Pair::join(&reg, "xv-test", &ours, &latest).is_none(),
            "an ask on A and a bid on B are not this pair's quotes"
        );

        let right = order("kalshi", "KXA", "bid", "0.40");
        ours.insert(("kalshi".into(), "KXA".into(), "bid".into()), &right);
        let p = Pair::join(&reg, "xv-test", &ours, &latest).expect("our bid on A joins");
        assert!(p.rest_a.is_some());
        assert!(p.rest_b.is_none(), "the bid on B is still not ours");
    }

    /// A venue this cannot price is not a pair, and must fall out before
    /// anything downstream tries to fee it.
    #[test]
    fn a_pair_on_an_unpriceable_venue_is_not_joined() {
        let reg = Registry {
            relationships: vec![rel("odd", &[("ftx", "FTXA"), ("kalshi", "KXB")])],
        };
        let o = order("ftx", "FTXA", "bid", "0.40");
        let mut ours: Resting = HashMap::new();
        ours.insert(("ftx".into(), "FTXA".into(), "bid".into()), &o);
        assert!(Pair::join(&reg, "odd", &ours, &Latest::new()).is_none());
    }

    // ----- routes: the execution styles a pair currently supports ----------

    /// Resting on leg A only is ONE make-take entry point: our bid fills and
    /// we immediately take B. The mirror route needs a resting ask on B that
    /// we do not have, and must not be priced as if we did.
    #[test]
    fn resting_on_leg_a_only_offers_make_a_take_b() {
        let r = xv();
        let o = order("kalshi", "KXA", "bid", "0.30");
        let (qa, qb) = (deep("0.40", "0.42"), deep("0.62", "0.64"));
        let (routes, best) = priced(&pair(&r, Some(&o), None, Some(&qa), Some(&qb)));
        assert!(routes.contains_key("make-a-take-b"));
        assert!(routes.contains_key("take-take"), "the crossing is always available");
        assert!(!routes.contains_key("take-a-make-b"), "we are not resting on B");
        assert!(best.is_some());
    }

    /// The mirror.
    #[test]
    fn resting_on_leg_b_only_offers_take_a_make_b() {
        let r = xv();
        let o = order("polymarket_us", "PMB", "ask", "0.70");
        let (qa, qb) = (deep("0.40", "0.42"), deep("0.62", "0.64"));
        let (routes, _) = priced(&pair(&r, None, Some(&o), Some(&qa), Some(&qb)));
        assert!(routes.contains_key("take-a-make-b"));
        assert!(routes.contains_key("take-take"));
        assert!(!routes.contains_key("make-a-take-b"), "we are not resting on A");
    }

    /// Resting on BOTH legs is not "make/make hoping both fill" — it is two
    /// make-take entry points, whichever side gets hit first. No make/make
    /// route exists in the output and none may: a basket that needs both of
    /// our quotes to fill is not one that can be priced as available.
    ///
    #[test]
    fn resting_on_both_legs_is_two_make_take_routes_not_one_make_make() {
        let r = xv();
        let (oa, ob) =
            (order("kalshi", "KXA", "bid", "0.30"), order("polymarket_us", "PMB", "ask", "0.70"));
        let (qa, qb) = (deep("0.40", "0.42"), deep("0.62", "0.64"));
        let (routes, _) = priced(&pair(&r, Some(&oa), Some(&ob), Some(&qa), Some(&qb)));
        assert_eq!(routes.len(), 3, "take-take and both make-take entry points");
        for want in ["take-take", "make-a-take-b", "take-a-make-b"] {
            assert!(routes.contains_key(want), "{want} missing");
        }
    }

    /// The best route drives the row's rank, its displayed edge AND which
    /// feasibility test is applied to it, so it must be the highest-edge route
    /// and not simply the last one considered.
    ///
    /// The fixture is built so those two answers differ, because otherwise
    /// this test proves nothing: routes are considered take-take, then
    /// make-a-take-b, then take-a-make-b, and here the WINNER is the middle
    /// one. Making leg A at 0.30 against a 0.42 ask is worth 12c a contract;
    /// our ask on B is a stale 0.50 against a venue bid that has since moved
    /// up to 0.62, so the route considered last is the worst of the three.
    #[test]
    fn the_best_route_is_the_one_with_the_most_edge() {
        let r = xv();
        let o = order("kalshi", "KXA", "bid", "0.30");
        let stale = order("polymarket_us", "PMB", "ask", "0.50");
        let (qa, qb) = (deep("0.40", "0.42"), deep("0.62", "0.64"));
        let (routes, best) = priced(&pair(&r, Some(&o), Some(&stale), Some(&qa), Some(&qb)));
        let (edge, label) = best.expect("a priced route");
        assert_eq!(label, "make-a-take-b", "making A beats crossing it at 0.42");
        let from_routes = routes[label]["edge_per_contract"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .expect("the winning route's own edge");
        assert!((edge - from_routes).abs() < 1e-12, "the ranked edge is the row's edge");
        let of = |route: &str| {
            routes[route]["edge_per_contract"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .expect("a route's own edge")
        };
        assert!(edge > of("take-take"), "{edge} should beat the crossing");
        assert!(edge > of("take-a-make-b"), "and the stale quote considered after it");
    }

    /// A route may only be priced off books it actually has. A leg with no ToB
    /// sample cannot be crossed, so every route that crosses it is absent —
    /// not priced at zero, and not priced off our own quote standing in for
    /// the venue's.
    #[test]
    fn a_leg_with_no_book_prices_no_route_that_crosses_it() {
        let r = xv();
        let (oa, ob) =
            (order("kalshi", "KXA", "bid", "0.30"), order("polymarket_us", "PMB", "ask", "0.70"));
        let qa = deep("0.40", "0.42");
        let (routes, best) = priced(&pair(&r, Some(&oa), Some(&ob), Some(&qa), None));
        assert_eq!(routes.len(), 1, "only the route that crosses A survives");
        assert!(routes.contains_key("take-a-make-b"));
        assert_eq!(best.map(|(_, l)| l), Some("take-a-make-b"));
    }

    // ----- feasibility: whether the best route is a trade or a number ------

    /// A take-take crosses both books and rests nothing, so no rest spread
    /// gates it. Applying the spread test to it would drop crossings that were
    /// available to take at that instant.
    #[test]
    fn a_take_take_rests_nothing_so_no_spread_gates_it() {
        let r = xv();
        let (qa, qb) = (deep("0.20", "0.80"), deep("0.20", "0.80"));
        let f = feasible(&pair(&r, None, None, Some(&qa), Some(&qb)), "take-take");
        assert!(f.rest_spread.is_none());
        assert!(f.fillable, "a 60c book is fine to CROSS at the touch");
    }

    /// Fill plausibility applies to the leg we REST on, and to that leg only.
    /// Reading the other leg's spread rejects a quote sitting inside a 1c book
    /// because the leg we were going to cross happened to be wide.
    #[test]
    fn the_spread_that_gates_a_rest_is_the_one_we_rest_on() {
        let r = xv();
        let (oa, ob) =
            (order("kalshi", "KXA", "bid", "0.40"), order("polymarket_us", "PMB", "ask", "0.70"));
        let (tight, wide) = (deep("0.40", "0.41"), deep("0.20", "0.80"));
        let p = pair(&r, Some(&oa), Some(&ob), Some(&tight), Some(&wide));

        let make_a = feasible(&p, "make-a-take-b");
        assert!((make_a.rest_spread.expect("leg A spread") - 0.01).abs() < 1e-9);
        assert!(make_a.fillable, "resting inside a 1c book fills");

        let make_b = feasible(&p, "take-a-make-b");
        assert!((make_b.rest_spread.expect("leg B spread") - 0.60).abs() < 1e-9);
        assert!(!make_b.fillable, "resting inside a 60c book does not");
    }

    /// Five cents is the line, and it is inclusive.
    #[test]
    fn the_rest_spread_limit_is_five_cents() {
        let r = xv();
        let o = order("kalshi", "KXA", "bid", "0.40");
        let qb = deep("0.62", "0.64");
        let at = |bid: &str, ask: &str| {
            let qa = deep(bid, ask);
            feasible(&pair(&r, Some(&o), None, Some(&qa), Some(&qb)), "make-a-take-b").fillable
        };
        assert!(at("0.40", "0.45"), "exactly 5c is still fillable");
        assert!(!at("0.40", "0.46"));
    }

    /// A leg we would have to CROSS is only good for what is resting behind
    /// its price, and `None` — the tape carried no size — is not permission to
    /// assume it is deep. That default is the difference between a clip that
    /// fills and one that walks the book.
    #[test]
    fn missing_size_on_a_leg_we_cross_is_not_permission_to_assume_depth() {
        let r = xv();
        let o = order("kalshi", "KXA", "bid", "0.40");
        let (qa, qb) = (deep("0.40", "0.42"), tob("0.62", "0.64", None, Some("100")));
        let f = feasible(&pair(&r, Some(&o), None, Some(&qa), Some(&qb)), "make-a-take-b");
        assert!(f.take_depth.is_none(), "no size on the bid we would hit");
        assert!(!f.deep_enough);
        assert!(f.have_books, "the book is there; only its size is not");
    }

    /// Depth on a take-take is the THINNER of the two legs. A 25-lot needs 25
    /// on both sides; the max, or an average, would size off a book that
    /// cannot fill it.
    #[test]
    fn take_take_depth_is_the_thinner_of_the_two_legs() {
        let r = xv();
        let at = |a: &str, b: &str| {
            let qa = tob("0.40", "0.42", Some("999"), Some(a));
            let qb = tob("0.62", "0.64", Some(b), Some("999"));
            feasible(&pair(&r, None, None, Some(&qa), Some(&qb)), "take-take")
        };
        let f = at("100", "30");
        assert_eq!(f.take_depth, Some(30.0), "the ask on A is 100 but the bid on B is 30");
        assert!(f.deep_enough);
        assert_eq!(at("30", "100").take_depth, Some(30.0), "either side may be the thin one");
        assert!(at("25", "25").deep_enough, "a clip of 25 needs exactly 25");
        assert!(!at("24", "100").deep_enough);
    }

    /// A leg we REST on needs no depth — we are the size there. Counting the
    /// rested leg's size would reject a route on the strength of a book we
    /// were never going to cross.
    #[test]
    fn a_rested_leg_needs_no_depth_because_we_are_the_size() {
        let r = xv();
        let o = order("kalshi", "KXA", "bid", "0.40");
        let qa = tob("0.40", "0.42", Some("1"), Some("1"));
        let qb = tob("0.62", "0.64", Some("100"), Some("100"));
        let f = feasible(&pair(&r, Some(&o), None, Some(&qa), Some(&qb)), "make-a-take-b");
        assert_eq!(f.take_depth, Some(100.0), "only leg B is crossed");
        assert!(f.deep_enough);
    }

    /// `have_books` is a separate claim from fillability: a route can price
    /// off one venue book and our own quote and still not be a trade, because
    /// nothing knows what the other leg is worth.
    #[test]
    fn a_missing_book_on_either_leg_is_not_a_trade() {
        let r = xv();
        let o = order("polymarket_us", "PMB", "ask", "0.70");
        let qa = deep("0.40", "0.42");
        assert!(!feasible(&pair(&r, None, Some(&o), None, Some(&qa)), "take-a-make-b").have_books);
        assert!(!feasible(&pair(&r, None, Some(&o), Some(&qa), None), "take-a-make-b").have_books);
        assert!(feasible(&pair(&r, None, Some(&o), Some(&qa), Some(&qa)), "take-a-make-b")
            .have_books);
    }
}
