//! Edge over time.
//!
//! The intents view is a snapshot; these are its charts. Both endpoints here
//! are thin — the work is `edge`, which joins our own posted quotes to the
//! venue books on one timeline.

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::Cx;
use arb_query::intents;
use arb_registry::{Registry, Relationship};
use arb_scenario::price_at;
use arb_tob::series;

use crate::endpoints;
use crate::http::query_param;
use crate::{integrity, Args, FEE_CATEGORY};

/// Post-fee edge over time for one pair.
///
/// Joins two independent series: our own posted quote (from the intent stream)
/// and the venue books (from the ToB rollup). Both forward-fill — a quote
/// stands until replaced — and at every point where enough is known we price
/// each available route. This is what makes "top N by spread" a time series
/// rather than a snapshot.
pub fn edge(a: &Args, rel: &Relationship) -> Vec<serde_json::Value> {
    let (la, lb) = (&rel.legs[0], &rel.legs[1]);
    let (Some(va), Some(vb)) = (Venue::parse(&la.venue), Venue::parse(&lb.venue)) else { return vec![] };

    let day = integrity::build(&a.data_dir).today;
    let path = |v: &str| format!("{}/tob-{v}-{day}.jsonl", a.rollup_dir);
    let sa = series::load_market(&[path(&la.venue)], &la.venue, &la.market_id);
    let sb = series::load_market(&[path(&lb.venue)], &lb.venue, &lb.market_id);

    let text = std::fs::read_to_string(&a.intents_path).unwrap_or_default();
    let markets = vec![
        (la.venue.clone(), la.market_id.clone()),
        (lb.venue.clone(), lb.market_id.clone()),
    ];
    let ours = intents::history_for(&text, &markets);

    // One merged timeline in nanoseconds; intents carry seconds.
    let mut ts: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    ts.extend(sa.iter().map(|s| s.ts_local_ns));
    ts.extend(sb.iter().map(|s| s.ts_local_ns));
    ts.extend(ours.iter().map(|e| (e.ts * 1e9) as i64));

    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let clip = cx.parse_exact("25");

    let (mut ia, mut ib, mut io) = (0usize, 0usize, 0usize);
    let (mut ca, mut cb) = (None::<&arb_tob::TobSample>, None::<&arb_tob::TobSample>);
    let (mut our_a, mut our_b) = (None::<String>, None::<String>);
    let mut out = Vec::new();

    for t in ts {
        while ia < sa.len() && sa[ia].ts_local_ns <= t {
            ca = Some(&sa[ia]);
            ia += 1;
        }
        while ib < sb.len() && sb[ib].ts_local_ns <= t {
            cb = Some(&sb[ib]);
            ib += 1;
        }
        while io < ours.len() && (ours[io].ts * 1e9) as i64 <= t {
            let e = &ours[io];
            let on_a = e.venue == la.venue && e.market == la.market_id;
            // We rest a BID on leg A (buying YES) and an ASK on leg B.
            let v = if e.kind == "cancel" { None } else { Some(e.price.clone()) };
            if on_a && e.side == "bid" {
                our_a = v;
            } else if !on_a && e.side == "ask" {
                our_b = v;
            }
            io += 1;
        }

        let va_ask = ca.and_then(|s| s.ask.clone());
        let vb_bid = cb.and_then(|s| s.bid.clone());
        let mut row = serde_json::Map::new();
        let mut push = |label: &str, ra: Role, rb: Role, ea: &Option<String>, eb: &Option<String>,
                        cx: &mut Cx, row: &mut serde_json::Map<String, serde_json::Value>| {
            if let (Some(ea), Some(eb)) = (ea, eb) {
                if let Some(p) =
                    price_at(cx, &sched, "s", va, vb, ra, rb, ea, eb, clip, FEE_CATEGORY)
                {
                    if let Ok(v) = p.edge_per_contract.parse::<f64>() {
                        row.insert(label.into(), serde_json::json!(v));
                    }
                }
            }
        };
        push("take_take", Role::Taker, Role::Taker, &va_ask, &vb_bid, &mut cx, &mut row);
        push("make_a_take_b", Role::Maker, Role::Taker, &our_a, &vb_bid, &mut cx, &mut row);
        push("take_a_make_b", Role::Taker, Role::Maker, &va_ask, &our_b, &mut cx, &mut row);
        if row.is_empty() {
            continue;
        }
        row.insert("ts".into(), serde_json::json!(t as f64 / 1e9));
        out.push(serde_json::Value::Object(row));
    }
    out
}

/// Edge series for the top N pairs, so the intents page opens on a picture of
/// where the money has been rather than a single instant.
pub fn top_json(a: &Args, query: &str) -> String {
    let n: usize = query_param(query, "n").and_then(|s| s.parse().ok()).unwrap_or(5);
    let body = endpoints::intents::json(a);
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) else {
        return "{\"error\":\"intents unavailable\"}".into();
    };
    let Ok(reg) = Registry::load(&a.registry) else {
        return "{\"error\":\"registry unavailable\"}".into();
    };
    let empty = vec![];
    let rows = v["rows"].as_array().unwrap_or(&empty);
    let mut out = Vec::new();
    for r in rows.iter().filter(|r| r["actionable"].as_bool().unwrap_or(false)).take(n) {
        let Some(id) = r["relationship_id"].as_str() else { continue };
        let Some(rel) = reg.get(id) else { continue };
        if rel.legs.len() != 2 {
            continue;
        }
        out.push(serde_json::json!({
            "relationship_id": id,
            "best_route": r["best_route"],
            "edge": r["edge"],
            "series": edge(a, rel),
        }));
    }
    serde_json::json!({ "n": out.len(), "rows": out }).to_string()
}

/// One pair's intent history — the engine's own quote, moving over time.
pub fn intent_json(a: &Args, query: &str) -> String {
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
    let text = std::fs::read_to_string(&a.intents_path).unwrap_or_default();
    let markets: Vec<(String, String)> =
        r.legs.iter().map(|l| (l.venue.clone(), l.market_id.clone())).collect();
    let events = intents::history_for(&text, &markets);
    serde_json::json!({
        "relationship_id": rel_id,
        "legs": markets.iter().map(|(v, m)| format!("{v}:{m}")).collect::<Vec<_>>(),
        "events": events,
        "edge_series": edge(a, r),
    })
    .to_string()
}
