//! Our own lots, read off the trade ledger: the evidence the calibration
//! weighs the tape against.

use std::collections::HashMap;

use serde_json::Value;

use arb_core::fees::kalshi_charges_makers;

use crate::sim::{Dir, KALSHI_MAKER, KALSHI_TAKER, PMUS_TAKER};

/// The entry strategies that open a two-legged basket. Naked repairs, probes
/// and sports runs are not the trade this lens is about.
const ENTRIES: [&str; 3] = ["maker-hedge", "take-take", "make-take"];
/// A basket outside this range is a mis-booked leg (a NO cost written into
/// `yes_price`), not a trade: no pair has ever crossed for half its payout.
const SANE_COST: (f64, f64) = (0.5, 1.05);

#[derive(Clone, Debug, PartialEq)]
pub struct Lot {
    pub rel: String,
    pub ts: f64,
    pub dir: Dir,
    /// Per contract, including modelled entry fees.
    pub cost: f64,
    /// When the first maker exit closed (part of) it, if one did.
    pub exit_ts: Option<f64>,
    /// When anything first closed (part of) it: a maker exit, a settlement,
    /// an unwind.
    pub closed_ts: Option<f64>,
}

fn num(v: Option<&Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn text<'a>(r: &'a Value, field: &str) -> &'a str {
    r.get(field).and_then(Value::as_str).unwrap_or("")
}

/// Which side a leg holds, read the way `arb-settle` and the Trades tab read
/// it: the engine spells a YES bid `bid` and a NO bid `ask`.
fn held_yes(action: &str, side: &str) -> Option<bool> {
    if action == "sell" || action.starts_with("close_via_") {
        return None;
    }
    match (action, side) {
        ("buy_no", _) | (_, "no") => Some(false),
        ("buy_yes", _) | (_, "yes") => Some(true),
        (_, "bid") => Some(true),
        (_, "ask") => Some(false),
        _ => None,
    }
}

/// A two-legged Kalshi + PM-US entry as (direction, cost per contract).
fn basket(rec: &Value) -> Option<(Dir, f64)> {
    let legs = rec.get("legs")?.as_array()?;
    let (mut kalshi, mut pmus, mut cost) = (None, None, 0.0);
    if legs.len() != 2 {
        return None;
    }
    for l in legs {
        let yes = held_yes(text(l, "action"), text(l, "side"))?;
        let px = num(l.get("yes_price"))?;
        let paid = if yes { px } else { 1.0 - px };
        let maker = text(l, "role") == "maker";
        let fee = |c: f64| c * paid * (1.0 - paid);
        match text(l, "venue") {
            "kalshi" => {
                kalshi = Some(yes);
                // A rest pays only on the series Kalshi flags; a leg with no
                // market id is charged, as the engine charges it.
                let k_maker = if kalshi_charges_makers(text(l, "market_id")) { KALSHI_MAKER } else { 0.0 };
                cost += paid + fee(if maker { k_maker } else { KALSHI_TAKER });
            }
            "polymarket_us" => {
                pmus = Some(yes);
                cost += paid + if maker { 0.0 } else { fee(PMUS_TAKER) };
            }
            _ => return None,
        }
    }
    if !(SANE_COST.0..SANE_COST.1).contains(&cost) {
        return None;
    }
    match (kalshi?, pmus?) {
        (true, false) => Some((Dir::KYes, cost)),
        (false, true) => Some((Dir::KNo, cost)),
        _ => None,
    }
}

/// The earliest close time per basket, keyed as the ledger keys baskets:
/// `(relationship_id, entry ts bits)`.
type FirstClose = HashMap<(String, u64), f64>;

/// Every basket entry, with the times of its first maker exit and its first
/// close of any kind. `records` must already have had corrections folded in.
pub fn lots(records: &[Value]) -> Vec<Lot> {
    let (mut exits, mut closes) = (FirstClose::new(), FirstClose::new());
    for r in records.iter().filter(|r| text(r, "status") == "unwound") {
        if let (Some(of), Some(ts)) = (num(r.get("closes_ts")), num(r.get("ts"))) {
            let key = (text(r, "relationship_id").to_string(), of.to_bits());
            let first = |m: &mut FirstClose| {
                let e = m.entry(key.clone()).or_insert(ts);
                *e = e.min(ts);
            };
            first(&mut closes);
            if text(r, "strategy") == "maker-exit" {
                first(&mut exits);
            }
        }
    }
    records
        .iter()
        .filter(|r| text(r, "status") == "open" && ENTRIES.contains(&text(r, "strategy")))
        .filter_map(|r| {
            let rel = text(r, "relationship_id").to_string();
            let ts = num(r.get("ts"))?;
            let (dir, cost) = basket(r)?;
            let key = (rel.clone(), ts.to_bits());
            let (exit_ts, closed_ts) = (exits.get(&key).copied(), closes.get(&key).copied());
            Some(Lot { rel, ts, dir, cost, exit_ts, closed_ts })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Kalshi YES at 0.40 and PM-US YES at 0.50, or the mirror for a Kalshi
    /// NO basket (Kalshi YES 0.55, PM-US YES 0.45), so either side costs ~$0.90.
    fn entry(ts: f64, k_side: &str, p_side: &str) -> Value {
        let (k, p) = if k_side == "bid" { ("0.40", "0.50") } else { ("0.55", "0.45") };
        json!({"status":"open","strategy":"maker-hedge","relationship_id":"xvus-a-b","ts":ts,
            "legs":[{"venue":"kalshi","side":k_side,"yes_price":k,"role":"maker"},
                    {"venue":"polymarket_us","side":p_side,"yes_price":p,"role":"taker"}]})
    }

    #[test]
    fn a_kalshi_bid_and_a_pmus_ask_is_long_kalshi_yes() {
        let l = lots(&[entry(100.0, "bid", "ask")]);
        assert_eq!(l.len(), 1);
        assert_eq!(l[0].dir, Dir::KYes);
        let want = 0.40 + KALSHI_MAKER * 0.24 + 0.50 + PMUS_TAKER * 0.25;
        assert!((l[0].cost - want).abs() < 1e-12, "no market id: charged as a maker");
        assert_eq!(l[0].exit_ts, None);
    }

    #[test]
    fn a_rest_on_a_series_that_charges_no_makers_pays_none() {
        let mut e = entry(100.0, "bid", "ask");
        e["legs"][0]["market_id"] = "KXTIME-26-POP".into();
        let l = lots(&[e]);
        let want = 0.40 + 0.50 + PMUS_TAKER * 0.25;
        assert!((l[0].cost - want).abs() < 1e-12, "{}", l[0].cost);
    }

    #[test]
    fn the_first_maker_exit_dates_the_lot() {
        let recs = [
            entry(100.0, "ask", "bid"),
            json!({"status":"unwound","strategy":"maker-exit","relationship_id":"xvus-a-b","closes_ts":100.0,"ts":900.0}),
            json!({"status":"unwound","strategy":"maker-exit","relationship_id":"xvus-a-b","closes_ts":100.0,"ts":500.0}),
            json!({"status":"unwound","strategy":"settlement","relationship_id":"xvus-a-b","closes_ts":100.0,"ts":50.0}),
        ];
        let l = lots(&recs);
        assert_eq!(l[0].dir, Dir::KNo);
        assert_eq!(l[0].exit_ts, Some(500.0), "settlement is not a maker exit");
        assert_eq!(l[0].closed_ts, Some(50.0), "but it is a close");
    }

    #[test]
    fn a_mis_booked_leg_is_not_a_lot() {
        let mut r = entry(1.0, "bid", "ask");
        r["legs"][1]["yes_price"] = json!("0.12"); // a NO cost written as the YES price
        assert!(lots(&[r]).is_empty());
    }

    #[test]
    fn a_one_sided_or_foreign_record_is_not_a_lot() {
        let mut both_yes = entry(1.0, "bid", "bid");
        assert!(lots(std::slice::from_ref(&both_yes)).is_empty());
        both_yes["strategy"] = json!("naked-hedge");
        both_yes["legs"][1]["side"] = json!("ask");
        assert!(lots(&[both_yes]).is_empty());
    }
}
