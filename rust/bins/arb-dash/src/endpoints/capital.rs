//! The root view reads the trader's automatic venue snapshot, never report exports.
use crate::Args;
use arb_registry::Registry;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

type EntryBasis = BTreeMap<String, (f64, f64)>; // relationship -> (qty, all-in cost)
const EXECUTABLE_MARK_MAX_AGE_S: u64 = 180;
fn number(v: &Value) -> Option<f64> {
    let n = v.as_f64().or_else(|| v.as_str()?.parse().ok())?;
    n.is_finite().then_some(n)
}
fn holding_number(h: &Value, key: &str) -> Option<f64> {
    number(&h[key])
}
fn executable_mark_fresh(h: &Value, now: u64, snapshot_fresh: bool) -> bool {
    snapshot_fresh
        && h["executable_mark_at"]
            .as_u64()
            .is_some_and(|at| at <= now && now - at <= EXECUTABLE_MARK_MAX_AGE_S)
}

/// Put the two venue positions for an equivalent outcome on the same row. A
/// holding's value becomes an executable mark only when its source-book time
/// and the enclosing account snapshot are both fresh.
fn position_pairs(
    snapshot: &Value,
    registry: Option<&Registry>,
    entry_basis: &EntryBasis,
    now: u64,
    snapshot_fresh: bool,
) -> Value {
    let mut holdings: BTreeMap<(String, String), Value> = BTreeMap::new();
    for venue in ["kalshi", "polymarket_us"] {
        for h in snapshot["accounts"][venue]["holdings"]
            .as_array()
            .into_iter()
            .flatten()
        {
            if let Some(market) = h["market"].as_str() {
                holdings.insert((venue.into(), market.into()), h.clone());
            }
        }
    }

    let mut claimed = BTreeSet::new();
    let mut pairs = Vec::new();
    if let Some(registry) = registry {
        for rel in &registry.relationships {
            let kalshi = rel.legs.iter().find(|l| l.venue == "kalshi");
            let pmus = rel.legs.iter().find(|l| l.venue == "polymarket_us");
            let (Some(kleg), Some(pleg)) = (kalshi, pmus) else {
                continue;
            };
            let kh = holdings.get(&("kalshi".into(), kleg.market_id.clone()));
            let ph = holdings.get(&("polymarket_us".into(), pleg.market_id.clone()));
            if kh.is_none() && ph.is_none() {
                continue;
            }

            let leg = |venue: &str, market: &str, h: Option<&Value>| {
                let Some(h) = h else {
                    return json!({"venue":venue,"market":market,"present":false});
                };
                let qty = holding_number(h, "quantity");
                let value = holding_number(h, "value_usd");
                let mark_fresh = executable_mark_fresh(h, now, snapshot_fresh);
                let held_mark = mark_fresh
                    .then(|| qty.zip(value))
                    .flatten()
                    .and_then(|(q, v)| (q != 0.).then_some(v / q.abs()));
                let yes_mark = qty
                    .zip(held_mark)
                    .map(|(q, m)| if q > 0. { m } else { 1. - m });
                json!({
                    "venue":venue, "market":market, "present":true,
                    "quantity":qty, "value_usd":value,
                    "held_side":qty.map(|q| if q >= 0. { "YES" } else { "NO" }),
                    "held_mark":held_mark, "yes_mark":yes_mark,
                    "mark_updated_at":h["mark_updated_at"],
                    "executable_mark_at":h["executable_mark_at"],
                    "mark_fresh":mark_fresh,
                })
            };
            let kj = leg("kalshi", &kleg.market_id, kh);
            let pj = leg("polymarket_us", &pleg.market_id, ph);
            if kh.is_some() {
                claimed.insert(("kalshi".into(), kleg.market_id.clone()));
            }
            if ph.is_some() {
                claimed.insert(("polymarket_us".into(), pleg.market_id.clone()));
            }

            let kq = number(&kj["quantity"]);
            let pq = number(&pj["quantity"]);
            let opposed = kq.zip(pq).is_some_and(|(k, p)| k.signum() != p.signum());
            let paired_qty = kq.zip(pq).map(|(k, p)| k.abs().min(p.abs()));
            let qty_gap = kq.zip(pq).map(|(k, p)| (k.abs() - p.abs()).abs());
            let pair_mark = if opposed {
                number(&kj["held_mark"])
                    .zip(number(&pj["held_mark"]))
                    .map(|(k, p)| k + p)
            } else {
                None
            };
            let carry_per_contract = pair_mark.map(|m| 1. - m);
            let carry_usd = carry_per_contract.zip(paired_qty).map(|(c, q)| c * q);
            let balanced = opposed && qty_gap.is_some_and(|g| g < 0.005);
            let (basis_qty, entry_cost, entry_price) = entry_basis
                .get(&rel.id)
                .map(|(q, c)| (Some(*q), Some(*c), (*q > 0.).then_some(*c / *q)))
                .unwrap_or((None, None, None));
            let label = rel
                .direction
                .as_deref()
                .and_then(|d| d.rsplit_once(':').map(|(_, label)| label.trim()))
                .filter(|label| !label.is_empty() && label.len() <= 80)
                .unwrap_or_else(|| rel.id.strip_prefix("xvus-").unwrap_or(&rel.id));
            pairs.push(json!({
                "relationship_id":rel.id, "label":label,
                "kalshi":kj, "polymarket_us":pj,
                "opposed":opposed, "balanced":balanced,
                "paired_qty":paired_qty, "qty_gap":qty_gap,
                "liquidation_value_per_contract":pair_mark,
                "distance_to_par_cents":carry_per_contract.map(|v| v * 100.),
                "distance_to_par_usd":carry_usd,
                "entry_basis_qty":basis_qty,
                "entry_cost_usd":entry_cost,
                "entry_price_per_contract":entry_price,
            }));
        }
    }
    pairs.sort_by(|a, b| {
        number(&b["distance_to_par_usd"])
            .unwrap_or(f64::NEG_INFINITY)
            .total_cmp(&number(&a["distance_to_par_usd"]).unwrap_or(f64::NEG_INFINITY))
    });

    let orphans: Vec<Value> = holdings
        .into_iter()
        .filter_map(|((venue, market), h)| {
            (!claimed.contains(&(venue.clone(), market.clone()))).then(|| {
                json!({
                    "venue":venue, "market":market, "quantity":number(&h["quantity"]),
                    "value_usd":number(&h["value_usd"]), "mark_updated_at":h["mark_updated_at"]
                })
            })
        })
        .collect();
    json!({"pairs":pairs,"orphans":orphans})
}

/// Aggregate the Trades view's folded OPEN rows into one weighted entry price
/// per relationship.  `cost_usd` is already all-in and pro-rated after partial
/// unwinds, so this preserves the original lot basis instead of netting later
/// sale proceeds into the contracts that remain.
fn entry_basis(ledger_text: &str, now: f64) -> EntryBasis {
    let book = crate::trades::build(ledger_text, crate::FEE_CATEGORY, now);
    let mut out: EntryBasis = BTreeMap::new();
    for row in book["rows"].as_array().into_iter().flatten() {
        if row["status"] != "open" || row["hedged"] != true {
            continue;
        }
        let (Some(rel), Some(qty), Some(cost)) = (
            row["relationship_id"].as_str(),
            number(&row["qty"]),
            number(&row["cost_usd"]),
        ) else {
            continue;
        };
        if qty <= 0. || cost < 0. {
            continue;
        }
        let totals = out.entry(rel.to_string()).or_default();
        totals.0 += qty;
        totals.1 += cost;
    }
    out
}

fn build(
    mut snapshot: Value,
    funding: Option<Value>,
    registry: Option<&Registry>,
    entry_basis: &EntryBasis,
    now: u64,
    alive: bool,
) -> Value {
    let at = snapshot["at"].as_u64().unwrap_or(0);
    let max = snapshot["max_age_s"].as_u64().unwrap_or(0);
    let fresh = alive && at <= now && max > 0 && now - at <= max;
    let Some(accounts) = snapshot["accounts"].as_object() else {
        return json!({"error":"No usable venue capital snapshot yet"});
    };
    if accounts.len() != 2
        || !accounts.contains_key("kalshi")
        || !accounts.contains_key("polymarket_us")
    {
        return json!({"error":"Incomplete venue capital snapshot; both accounts are required"});
    }
    let mut cash = 0.;
    let mut reserved = 0.;
    let mut positions = 0.;
    let mut equity = 0.;
    let mut deposits = 0.;
    let mut funding_known = true;
    for (venue, a) in accounts {
        let fields = [
            "available_cash_usd",
            "reserved_cash_usd",
            "positions_value_usd",
            "equity_usd",
        ];
        let values: Option<Vec<f64>> = fields.iter().map(|k| number(&a[k])).collect();
        let Some(v) = values else {
            return json!({"error":"Unreadable venue capital amount"});
        };
        cash += v[0];
        reserved += v[1];
        positions += v[2];
        equity += v[3];
        match funding.as_ref().and_then(|f| number(&f[venue])) {
            Some(n) => deposits += n,
            None => funding_known = false,
        }
    }
    let k = &snapshot["accounts"]["kalshi"];
    let holdings = k["holdings"].as_array();
    let count = holdings.map_or(0, Vec::len);
    let marked: Vec<f64> = holdings
        .into_iter()
        .flatten()
        .filter(|h| executable_mark_fresh(h, now, fresh))
        .filter_map(|h| number(&h["value_usd"]))
        .collect();
    let subtotal: f64 = marked.iter().sum();
    let complete = holdings.is_some() && marked.len() == count;
    snapshot["kalshi_reconciliation"] = json!({
        "positions": count, "marked_positions": marked.len(), "complete": complete,
        "marked_subtotal_usd": subtotal,
        "difference_usd": complete.then(|| subtotal - number(&k["positions_value_usd"]).unwrap()),
    });
    snapshot["fresh"] = json!(fresh);
    snapshot["age_s"] = json!(now.saturating_sub(at));
    snapshot["producer_alive"] = json!(alive);
    snapshot["funding"] = funding.unwrap_or(Value::Null);
    snapshot["totals"] = json!({"available_cash_usd":cash,"reserved_cash_usd":reserved,"positions_value_usd":positions,
        "equity_usd":equity,"deposits_usd":funding_known.then_some(deposits),
        "profit_usd":funding_known.then_some(equity-deposits),"return_pct":(funding_known && deposits>0.).then_some((equity/deposits-1.)*100.)});
    snapshot["position_comparison"] = position_pairs(&snapshot, registry, entry_basis, now, fresh);
    snapshot
}
pub fn json(a: &Args) -> String {
    let path = format!("{}/exec/capital.json", a.data_dir);
    let snapshot: Value = match std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
    {
        Some(v) => v,
        None => {
            return json!({"error":"Waiting for the trader's automatic venue capital poll"})
                .to_string()
        }
    };
    let funding = std::fs::read_to_string("config/funding.yaml")
        .ok()
        .and_then(|s| serde_yaml::from_str::<Value>(&s).ok());
    let alive = snapshot["pid"].as_u64().is_some_and(|pid| {
        std::fs::read(format!("/proc/{pid}/cmdline"))
            .ok()
            .is_some_and(|b| String::from_utf8_lossy(&b).contains("arb-trader"))
    });
    let registry = Registry::load(&a.registry).ok();
    let ledger = std::fs::read_to_string(&a.ledger_path).unwrap_or_default();
    let basis = entry_basis(&ledger, arb_core::clock::now_secs() as f64);
    build(
        snapshot,
        funding,
        registry.as_ref(),
        &basis,
        arb_core::clock::now_secs(),
        alive,
    )
    .to_string()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot() -> Value {
        json!({"at":100,"max_age_s":180,"accounts":{"kalshi":{"available_cash_usd":"20","reserved_cash_usd":"5","positions_value_usd":"80","equity_usd":"105"},"polymarket_us":{"available_cash_usd":"0","reserved_cash_usd":"0","positions_value_usd":"0","equity_usd":"0"}}})
    }
    #[test]
    fn reconciliation_requires_every_position_and_preserves_venue_equity() {
        let mut s = snapshot();
        s["accounts"]["kalshi"]["holdings"] = json!([
            {"value_usd":"50", "executable_mark_at":100},
            {"value_usd":null, "executable_mark_at":100}
        ]);
        let a = build(s.clone(), None, None, &EntryBasis::new(), 110, true);
        assert_eq!(a["kalshi_reconciliation"]["difference_usd"], Value::Null);
        assert_eq!(a["kalshi_reconciliation"]["marked_subtotal_usd"], 50.);
        assert_eq!(a["totals"]["equity_usd"], 105.);
        s["accounts"]["kalshi"]["holdings"][1]["value_usd"] = json!("25");
        let a = build(s, None, None, &EntryBasis::new(), 110, true);
        assert_eq!(a["kalshi_reconciliation"]["difference_usd"], -5.);
        assert_eq!(a["kalshi_reconciliation"]["complete"], true);
        assert_eq!(a["totals"]["equity_usd"], 105.);
    }
    #[test]
    fn deposits_only_change_profit_never_capital() {
        let a = build(
            snapshot(),
            Some(json!({"kalshi":"100","polymarket_us":"0"})),
            None,
            &EntryBasis::new(),
            110,
            true,
        );
        let b = build(
            snapshot(),
            Some(json!({"kalshi":"99999","polymarket_us":"0"})),
            None,
            &EntryBasis::new(),
            110,
            true,
        );
        assert_eq!(a["totals"]["equity_usd"], b["totals"]["equity_usd"]);
        assert_eq!(a["totals"]["profit_usd"], 5.);
        assert_eq!(a["fresh"], true);
    }
    #[test]
    fn stale_dead_future_and_missing_funding_are_explicit() {
        assert_eq!(
            build(snapshot(), None, None, &EntryBasis::new(), 281, true)["fresh"],
            false
        );
        assert_eq!(
            build(snapshot(), None, None, &EntryBasis::new(), 110, false)["fresh"],
            false
        );
        assert_eq!(
            build(snapshot(), None, None, &EntryBasis::new(), 99, true)["fresh"],
            false
        );
        assert_eq!(
            build(snapshot(), None, None, &EntryBasis::new(), 110, true)["totals"]["profit_usd"],
            Value::Null
        );
    }

    #[test]
    fn pairs_positions_and_derives_conservative_distance_to_par() {
        let mut s = snapshot();
        s["accounts"]["kalshi"]["holdings"] = json!([{
            "market":"K", "quantity":"10", "value_usd":"3", "executable_mark_at":100
        }]);
        s["accounts"]["polymarket_us"]["holdings"] = json!([{
            "market":"P", "quantity":"-8", "value_usd":"5.2", "executable_mark_at":100
        }]);
        let path =
            std::env::temp_dir().join(format!("capital-registry-{}.yaml", std::process::id()));
        std::fs::write(&path, "relationships:\n- id: xvus-test-outcome\n  direction: 'same: Test outcome'\n  legs:\n  - { venue: kalshi, market_id: K }\n  - { venue: polymarket_us, market_id: P }\n").unwrap();
        let registry = Registry::load(path.to_str().unwrap()).unwrap();
        let basis = EntryBasis::from([("xvus-test-outcome".into(), (8., 6.4))]);
        let a = build(s, None, Some(&registry), &basis, 110, true);
        let p = &a["position_comparison"]["pairs"][0];
        assert_eq!(p["label"], "Test outcome");
        assert_eq!(p["paired_qty"], 8.);
        assert_eq!(p["qty_gap"], 2.);
        assert!((number(&p["distance_to_par_cents"]).unwrap() - 5.).abs() < 1e-9);
        assert!((number(&p["distance_to_par_usd"]).unwrap() - 0.4).abs() < 1e-9);
        assert!((number(&p["entry_price_per_contract"]).unwrap() - 0.8).abs() < 1e-9);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn liquidation_requires_fresh_marks_on_both_legs_but_preserves_basis_and_valuation() {
        let mut s = snapshot();
        s["at"] = json!(200);
        s["accounts"]["kalshi"]["holdings"] = json!([{
            "market":"K", "quantity":"-150", "value_usd":"58.5",
            "mark_updated_at":"live book at 280", "executable_mark_at":280
        }]);
        s["accounts"]["polymarket_us"]["holdings"] = json!([{
            "market":"P", "quantity":"150", "value_usd":"96",
            "mark_updated_at":"2026-08-29T00:00:00Z"
        }]);
        let path = std::env::temp_dir().join(format!(
            "capital-freshness-registry-{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, "relationships:\n- id: xvus-test-outcome\n  legs:\n  - { venue: kalshi, market_id: K }\n  - { venue: polymarket_us, market_id: P }\n").unwrap();
        let registry = Registry::load(path.to_str().unwrap()).unwrap();
        let basis = EntryBasis::from([("xvus-test-outcome".into(), (150., 120.))]);

        // A fresh account poll cannot turn a venue fallback value into an
        // executable quote, even when the other leg has a current book.
        let a = build(s.clone(), None, Some(&registry), &basis, 281, true);
        let p = &a["position_comparison"]["pairs"][0];
        assert_eq!(p["kalshi"]["mark_fresh"], true);
        assert_eq!(p["polymarket_us"]["mark_fresh"], false);
        assert_eq!(p["liquidation_value_per_contract"], Value::Null);
        assert_eq!(p["entry_price_per_contract"], 0.8);
        assert_eq!(p["polymarket_us"]["value_usd"], 96.);
        assert_eq!(a["totals"]["equity_usd"], 105.);

        s["accounts"]["polymarket_us"]["holdings"][0]["executable_mark_at"] = json!(280);
        let both_fresh = build(s.clone(), None, Some(&registry), &basis, 281, true);
        assert!(
            (number(
                &both_fresh["position_comparison"]["pairs"][0]["liquidation_value_per_contract"]
            )
            .unwrap()
                - 1.03)
                .abs()
                < 1e-9
        );

        // Even otherwise-current marks fail closed with a future timestamp or
        // once the producer snapshot itself is stale/dead.
        s["accounts"]["polymarket_us"]["holdings"][0]["executable_mark_at"] = json!(282);
        let future = build(s.clone(), None, Some(&registry), &basis, 281, true);
        assert_eq!(
            future["position_comparison"]["pairs"][0]["liquidation_value_per_contract"],
            Value::Null
        );
        s["accounts"]["polymarket_us"]["holdings"][0]["executable_mark_at"] = json!(280);
        let dead = build(s.clone(), None, Some(&registry), &basis, 281, false);
        assert_eq!(
            dead["position_comparison"]["pairs"][0]["liquidation_value_per_contract"],
            Value::Null
        );
        let stale = build(s, None, Some(&registry), &basis, 381, true);
        assert_eq!(
            stale["position_comparison"]["pairs"][0]["liquidation_value_per_contract"],
            Value::Null
        );
        let _ = std::fs::remove_file(path);
    }
}
