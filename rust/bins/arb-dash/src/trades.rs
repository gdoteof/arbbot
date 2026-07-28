//! Trades this system made, with their accounting.
//!
//! Source of truth is the append-only ledger (`data/exec/trades.jsonl`).
//! Everything here is DERIVED on each request — nothing is stored twice, so
//! the tab cannot drift from what the engine booked.
//!
//! THE LEDGER HAS TWO RECORD SHAPES and they account differently. Getting this
//! wrong is not cosmetic: the first version of this file priced only the newer
//! shape and reported the book as $3.76 in the RED when it was in profit.
//!
//!   * Python-era (no `source`): legs carry `action: buy_yes|buy_no` and often
//!     their own settled `fees`; the record carries `cost_usd` and
//!     `payoff_usd`. `cost_usd` is ALL-IN — fees are already inside it.
//!   * Engine (`source: arb-trader`): legs carry `side: bid|ask` and prices,
//!     and the record says `fees_pending` because the engine does not read
//!     fill reports. Cost must be derived and fees MODELLED.
//!
//! Both reduce to the same thing: a cross-venue basket costs some amount per
//! contract and pays exactly $1.00 per contract at resolution, whatever the
//! event does. Profit is payoff minus cost. That is what makes it locked
//! rather than a forecast.
//!
//! Status matters too. Only `open` positions still tie up capital, so only
//! they carry an APR-to-hold; `realized` and `unwound` are history, and
//! `correction` is a compensating adjustment, not a new position.

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::resolve::{resolve_date, today_iso, years_between};
use arb_core::scan::Cx;

fn venue_of(s: &str) -> Option<Venue> {
    match s {
        "kalshi" => Some(Venue::Kalshi),
        "polymarket_us" => Some(Venue::PolymarketUs),
        "polymarket" => Some(Venue::Polymarket),
        _ => None,
    }
}

fn num(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Build the `/api/trades` payload from the ledger text.
pub fn build(ledger_text: &str, fee_category: &str, now_s: f64) -> serde_json::Value {
    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let today = today_iso(now_s);

    let mut rows: Vec<serde_json::Value> = Vec::new();
    // Open and realized are tracked apart on purpose: adding them would report
    // money already banked as if it were still working.
    let (mut open_n, mut open_ct, mut open_cost, mut open_net) = (0u64, 0i64, 0.0f64, 0.0f64);
    let (mut real_n, mut real_net) = (0u64, 0.0f64);
    let mut fees_settled_total = 0.0f64;
    let mut fees_modelled_total = 0.0f64;
    let mut split: std::collections::BTreeMap<(String, String), (u64, i64, f64)> =
        Default::default();
    let mut by_strategy: std::collections::BTreeMap<String, (u64, i64, f64)> = Default::default();
    let mut unparsed = 0u64;
    let mut corrections = 0u64;

    for line in ledger_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(rec) = serde_json::from_str::<serde_json::Value>(line) else {
            // A torn tail line is normal: the engine appends while we read.
            unparsed += 1;
            continue;
        };
        let status = rec.get("status").and_then(|v| v.as_str()).unwrap_or("open").to_string();
        if status == "correction" {
            corrections += 1;
            continue;
        }
        let rel_id = rec.get("relationship_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let qty = rec.get("qty").and_then(|v| v.as_i64()).unwrap_or(0);
        let ts = rec.get("ts").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let source = rec.get("source").and_then(|v| v.as_str()).unwrap_or("python").to_string();
        let strategy = rec
            .get("strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("make-take")
            .to_string();
        let empty = vec![];
        let legs = rec.get("legs").and_then(|v| v.as_array()).unwrap_or(&empty);

        // --- fees -----------------------------------------------------------
        // Settled fees beat modelled ones wherever the ledger has them. A
        // record that reports its own fees is bank truth; a modelled number is
        // this dashboard's opinion, and the two must not be presented alike.
        let mut fees = 0.0f64;
        let mut settled = false;
        let mut leg_out = Vec::new();
        let mut derived_cost_ct = 0.0f64;
        let mut have_derived = true;
        for l in legs {
            let venue_s = l.get("venue").and_then(|v| v.as_str()).unwrap_or("").to_string();
            // Older records predate the role field. Say "unrecorded" rather
            // than leaving a blank cell that reads like a missing value in the
            // UI — and never silently default it to maker, which would price
            // the leg at the cheaper coefficient.
            let role_s = match l.get("role").and_then(|v| v.as_str()) {
                Some(r) if !r.is_empty() => r.to_string(),
                _ => "unrecorded".to_string(),
            };
            let side = l.get("side").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let action = l.get("action").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let lqty = l.get("qty").and_then(|v| v.as_i64()).unwrap_or(qty);
            let px = num(l.get("avg_price")).or_else(|| num(l.get("yes_price"))).unwrap_or(0.0);

            let leg_fee = match num(l.get("fees")) {
                Some(f) => {
                    settled = true;
                    f
                }
                None => {
                    // Only an explicit `maker` gets the maker coefficient.
                    // An unrecorded role prices as a TAKER: overstating our
                    // own costs is the safe direction for an accounting view,
                    // and defaulting the other way would quietly flatter it.
                    let role =
                        if role_s == "maker" { Role::Maker } else { Role::Taker };
                    match venue_of(&venue_s) {
                        Some(v) => {
                            let p = cx.parse_exact(&format!("{px}"));
                            let sz = cx.from_i64(lqty);
                            let f = sched.fee(&mut cx, v, role, p, sz, fee_category);
                            cx.emit_6dp(f).parse::<f64>().unwrap_or(0.0)
                        }
                        None => 0.0,
                    }
                }
            };
            fees += leg_fee;

            // What this leg cost per contract. `yes_price` is always quoted on
            // the YES side, so buying NO costs (1 - yes_price); selling YES is
            // economically the same as buying NO.
            let cost_ct = match (action.as_str(), side.as_str()) {
                ("buy_no", _) | (_, "no") => 1.0 - px,
                ("buy_yes", _) | (_, "yes") => px,
                (_, "bid") => px,        // engine: bought at this price
                (_, "ask") => 1.0 - px,  // engine: sold YES == bought NO
                _ => {
                    have_derived = false;
                    0.0
                }
            };
            derived_cost_ct += cost_ct;

            let e = split.entry((venue_s.clone(), role_s.clone())).or_insert((0, 0, 0.0));
            e.0 += 1;
            e.1 += lqty;
            e.2 += leg_fee;

            leg_out.push(serde_json::json!({
                "venue": venue_s,
                "market_id": l.get("market_id").and_then(|v| v.as_str()).unwrap_or(""),
                "side": if side.is_empty() { action.clone() } else { side },
                "role": role_s,
                "qty": lqty,
                "price": px,
                "fee_usd": leg_fee,
            }));
        }

        // --- cost, payoff, profit -------------------------------------------
        // A basket pays exactly $1.00 per contract at resolution regardless of
        // the outcome — that is the definition of the position being hedged.
        let payoff = num(rec.get("payoff_usd")).unwrap_or(qty as f64);
        // The record's own cost is authoritative and ALL-IN (fees inside).
        // Derived cost is ex-fees, so fees must then be subtracted separately.
        let (cost, fees_in_cost) = match num(rec.get("cost_usd")) {
            Some(c) => (Some(c), true),
            None if have_derived && qty > 0 => (Some(derived_cost_ct * qty as f64), false),
            None => (None, false),
        };
        let net = cost.map(|c| if fees_in_cost { payoff - c } else { payoff - c - fees });

        let (resolves, estimated) = match resolve_date(&rel_id) {
            Some((d, est)) => (Some(d.to_string()), est),
            None => (None, false),
        };
        // APR only means something while the capital is still committed.
        // A realized trade's money is back and earning elsewhere.
        let years = resolves.as_deref().and_then(|d| years_between(&today, d));
        let apr = match (net, cost, years) {
            (Some(n), Some(c), Some(y)) if status == "open" && c > 0.0 && y > 0.0 => {
                Some(n / c / y * 100.0)
            }
            _ => None,
        };

        if settled {
            fees_settled_total += fees;
        } else {
            fees_modelled_total += fees;
        }
        match status.as_str() {
            "open" => {
                open_n += 1;
                open_ct += qty;
                open_cost += cost.unwrap_or(0.0);
                open_net += net.unwrap_or(0.0);
            }
            _ => {
                real_n += 1;
                real_net += net.unwrap_or(0.0);
            }
        }
        let s = by_strategy.entry(strategy.clone()).or_insert((0, 0, 0.0));
        s.0 += 1;
        s.1 += qty;
        s.2 += net.unwrap_or(0.0);

        rows.push(serde_json::json!({
            "ts": ts,
            "relationship_id": rel_id,
            "strategy": strategy,
            "source": source,
            "status": status,
            "qty": qty,
            "cost_usd": cost,
            "payoff_usd": payoff,
            "fees_usd": fees,
            "fees_settled": settled,
            "net_usd": net,
            "resolves_by": resolves,
            "resolves_estimated": estimated,
            "apr_pct": apr,
            "legs": leg_out,
        }));
    }

    // Newest first: the question on opening this tab is almost always "what
    // did it just do", not "what did it do three weeks ago".
    rows.sort_by(|a, b| {
        let (x, y) = (a["ts"].as_f64().unwrap_or(0.0), b["ts"].as_f64().unwrap_or(0.0));
        y.total_cmp(&x)
    });

    // Capital-weighted, not a mean of per-trade APRs: a $2 trade at 40%/yr and
    // a $200 trade at 5%/yr do not average to 22%.
    let mut num_w = 0.0f64;
    let mut den_w = 0.0f64;
    for r in &rows {
        if let (Some(c), Some(a)) = (r["cost_usd"].as_f64(), r["apr_pct"].as_f64()) {
            num_w += c * a;
            den_w += c;
        }
    }
    let blended_apr = if den_w > 0.0 { Some(num_w / den_w) } else { None };

    serde_json::json!({
        "as_of": today,
        "totals": {
            "trades": rows.len(),
            "open_trades": open_n,
            "open_contracts": open_ct,
            "open_cost_usd": open_cost,
            "open_net_usd": open_net,
            "realized_trades": real_n,
            "realized_net_usd": real_net,
            "fees_settled_usd": fees_settled_total,
            "fees_modelled_usd": fees_modelled_total,
            "blended_apr_pct": blended_apr,
        },
        "by_venue_role": split.into_iter().map(|((v, r), (legs, ct, fee))| {
            serde_json::json!({"venue": v, "role": r, "legs": legs,
                               "contracts": ct, "fees_usd": fee})
        }).collect::<Vec<_>>(),
        "by_strategy": by_strategy.into_iter().map(|(k, (n, ct, net))| {
            serde_json::json!({"strategy": k, "trades": n, "contracts": ct, "net_usd": net})
        }).collect::<Vec<_>>(),
        "fee_note": format!(
            "settled fees come from the ledger; modelled fees are priced by arb_core::fees at \
             category '{fee_category}' for records the engine booked with fees_pending"
        ),
        "corrections": corrections,
        "unparsed_lines": unparsed,
        "rows": rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The take-take this engine made on 2026-07-28: sell PM YES @0.08, buy
    /// Kalshi YES @0.04 on 5 contracts. Engine shape — fees_pending, so cost
    /// must be derived and fees modelled.
    const ENGINE: &str = r#"{"ts":1785250000.0,"relationship_id":"xvus-nobel-peace-26-donaldtrump","qty":5,"strategy":"take-take","status":"open","source":"arb-trader","fees_pending":true,"legs":[{"venue":"polymarket_us","market_id":"tac-nobel-peace-2026-10-09-dontru","side":"ask","role":"taker","qty":5,"yes_price":"0.0800"},{"venue":"kalshi","market_id":"KXNOBELPEACE-26-DJT","side":"bid","role":"taker","qty":5,"yes_price":"0.0400"}]}"#;

    /// A real Python-era record: buy_yes + buy_no, settled per-leg fees, and
    /// an all-in `cost_usd`.
    const PYTHON: &str = r#"{"ts":1784646659.716,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","qty":50,"status":"open","legs":[{"venue":"kalshi","market_id":"KXFRENCHPRES-27-JMEL","action":"buy_yes","qty":50,"avg_price":"0.1100","fees":"0.3427","role":"taker","side":"yes"},{"venue":"polymarket_us","market_id":"ewc-pres-fra-2027-04-11-jeamel","action":"buy_no","qty":50,"yes_price":"0.2500","role":"taker","side":"no"}],"cost_usd":43.9027,"payoff_usd":50.0}"#;

    #[test]
    fn engine_shape_derives_cost_and_models_fees() {
        let out = build(ENGINE, "default", 1785250000.0);
        let r = &out["rows"][0];
        // buy Kalshi YES @0.04 + sell PM YES @0.08 (== buy NO @0.92) = 0.96/ct
        assert!((r["cost_usd"].as_f64().unwrap() - 4.80).abs() < 1e-9);
        assert_eq!(r["payoff_usd"], 5.0);
        assert_eq!(r["fees_settled"], false, "engine records are fees_pending");
        assert!(r["fees_usd"].as_f64().unwrap() > 0.0);
        // net = payoff - cost - modelled fees, and must be under the 0.20 gross
        let net = r["net_usd"].as_f64().unwrap();
        assert!(net > 0.0 && net < 0.20, "net {net} should be a shaved 0.20");
    }

    /// The regression that made the whole tab wrong: pricing a Python record
    /// with the engine's rules reported the book in the red.
    #[test]
    fn python_shape_uses_its_own_settled_cost_and_fees() {
        let out = build(PYTHON, "default", 1784646659.0);
        let r = &out["rows"][0];
        assert_eq!(r["fees_settled"], true, "the ledger reported real fees");
        assert!((r["cost_usd"].as_f64().unwrap() - 43.9027).abs() < 1e-9);
        // cost_usd is ALL-IN, so fees must NOT be subtracted a second time
        assert!((r["net_usd"].as_f64().unwrap() - 6.0973).abs() < 1e-6);
    }

    #[test]
    fn both_shapes_together_are_in_profit() {
        let out = build(&format!("{PYTHON}\n{ENGINE}"), "default", 1785250000.0);
        let t = &out["totals"];
        assert_eq!(t["open_trades"], 2);
        assert!(t["open_net_usd"].as_f64().unwrap() > 0.0, "this book is in profit");
    }

    /// Realized money is back in the account; counting it as open would report
    /// banked profit as if it were still working.
    #[test]
    fn realized_is_kept_apart_from_open() {
        let done = PYTHON.replace(r#""status":"open""#, r#""status":"realized""#);
        let out = build(&format!("{ENGINE}\n{done}"), "default", 1785250000.0);
        let t = &out["totals"];
        assert_eq!(t["open_trades"], 1);
        assert_eq!(t["realized_trades"], 1);
        assert!(t["realized_net_usd"].as_f64().unwrap() > 0.0);
    }

    /// A correction is a compensating append, not a position.
    #[test]
    fn corrections_are_not_positions() {
        let c = PYTHON.replace(r#""status":"open""#, r#""status":"correction""#);
        let out = build(&format!("{ENGINE}\n{c}"), "default", 1785250000.0);
        assert_eq!(out["totals"]["trades"], 1);
        assert_eq!(out["corrections"], 1);
    }

    /// APR is only meaningful while the capital is still committed.
    #[test]
    fn only_open_positions_carry_an_apr() {
        let done = ENGINE.replace(r#""status":"open""#, r#""status":"realized""#);
        let out = build(&done, "default", 1785250000.0);
        assert!(out["rows"][0]["apr_pct"].is_null(), "realized money earns nothing here");
    }

    #[test]
    fn role_changes_the_modelled_fee() {
        let taker = build(ENGINE, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
        let as_maker = ENGINE.replace(r#""role":"taker""#, r#""role":"maker""#);
        let maker =
            build(&as_maker, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
        assert!(maker < taker, "maker legs must not pay the taker fee ({maker} vs {taker})");
    }

    #[test]
    fn splits_make_and_take_by_venue() {
        let out = build(ENGINE, "default", 1785250000.0);
        let split = out["by_venue_role"].as_array().unwrap();
        assert_eq!(split.len(), 2);
        for s in split {
            assert_eq!(s["role"], "taker");
            assert_eq!(s["contracts"], 5);
        }
    }

    #[test]
    fn a_torn_tail_line_does_not_lose_the_rest() {
        let out = build(&format!("{ENGINE}\n{{\"ts\":1.0,\"relat"), "default", 1785250000.0);
        assert_eq!(out["totals"]["trades"], 1);
        assert_eq!(out["unparsed_lines"], 1);
    }
}
