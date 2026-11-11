//! Reward trading on the Trades view: the fills and positions of the Kalshi
//! liquidity-incentive quoter (`arb-trader --lip`).
//!
//! That quoter is in neither the ledger (`trades.jsonl`) nor the double-entry
//! books. It keeps its own append-only log, `data/exec/lip.jsonl`, of `fill`
//! and `adopt` records, and this view folds that log. Three sources, kept
//! apart:
//!
//!   * the log: every fill, and per market the position, its average price,
//!     and the P&L realized on contracts bought and sold back (average cost,
//!     before fees);
//!   * the quoter's block in the armed engine's 60 s summary line: resting
//!     orders, the P&L marked to mid, the modelled reward, and why a market
//!     rests nothing. These are shown verbatim and never recomputed here;
//!   * `kalshi_positions.json` from arb-venue-sync: the venue's own position
//!     and fees, beside the log's position as a check on it.
//!
//! The reward itself is not here. Kalshi pays it at the end of each program
//! period, and nothing on disk records it until then.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::Args;

/// Newest fills returned. The log holds every fill since the quoter first ran.
const FILLS_SHOWN: usize = 500;

/// One market's position from the log, on the YES axis: positive is long YES.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Position {
    pub pos: f64,
    /// Average price of the open position; 0 when flat.
    pub avg: f64,
    /// Dollars realized on contracts closed against the average, before fees.
    pub realized: f64,
    pub fills: usize,
    pub contracts: f64,
    pub last_ts: f64,
}

impl Position {
    /// `q` contracts traded at `p`: positive bought YES, negative sold it.
    fn trade(&mut self, q: f64, p: f64) {
        if self.pos == 0.0 || (self.pos > 0.0) == (q > 0.0) {
            let n = self.pos.abs() + q.abs();
            self.avg = (self.avg * self.pos.abs() + p * q.abs()) / n;
            self.pos += q;
            return;
        }
        let closed = q.abs().min(self.pos.abs());
        self.realized += closed * (p - self.avg) * self.pos.signum();
        self.pos += q;
        if self.pos.abs() < 1e-9 {
            self.pos = 0.0;
            self.avg = 0.0;
        } else if (self.pos > 0.0) == (q > 0.0) {
            // Traded through flat: what is left opened at this price.
            self.avg = p;
        }
    }
}

/// The log, folded: per-market positions, the fills newest first, and the
/// lines that could not be read.
#[derive(Debug, Default)]
pub struct Folded {
    pub positions: BTreeMap<String, Position>,
    pub fills: Vec<Value>,
    pub adopts: usize,
    pub bad_lines: usize,
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn fold(text: &str) -> Folded {
    let mut out = Folded::default();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(r) = serde_json::from_str::<Value>(line) else {
            out.bad_lines += 1;
            continue;
        };
        let event = r["event"].as_str().unwrap_or_default();
        if event != "fill" && event != "adopt" {
            continue;
        }
        let (Some(market), Some(qty), Some(price)) = (r["market"].as_str(), num(&r["qty"]), num(&r["price"])) else {
            out.bad_lines += 1;
            continue;
        };
        // A fill's qty is unsigned and its side says which way. An adoption's
        // qty is already signed: what the venue held beyond the log.
        let q = match (event, r["side"].as_str()) {
            ("adopt", _) => qty,
            (_, Some("bid")) => qty,
            (_, Some("ask")) => -qty,
            _ => {
                out.bad_lines += 1;
                continue;
            }
        };
        let p = out.positions.entry(market.to_string()).or_default();
        p.trade(q, price);
        p.last_ts = num(&r["ts"]).unwrap_or(p.last_ts);
        if event == "adopt" {
            out.adopts += 1;
            continue;
        }
        p.fills += 1;
        p.contracts += qty;
        out.fills.push(json!({
            "ts": num(&r["ts"]),
            "market": market,
            "side": if q > 0.0 { "buy YES" } else { "sell YES" },
            "qty": qty,
            "price": price,
            "pos_after": num(&r["pos"]),
            "order": r["order"],
        }));
    }
    out.fills.reverse();
    out
}

/// The venue's own position file: ticker -> its row.
fn venue_positions(a: &Args) -> (BTreeMap<String, Value>, Option<u64>) {
    let path = format!("{}/kalshi_positions.json", a.kalshi_dir);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    let by = rows
        .into_iter()
        .filter_map(|r| Some((r["ticker"].as_str()?.to_string(), r)))
        .collect();
    (by, super::age_secs(&path))
}

pub fn json(a: &Args) -> String {
    let log = format!("{}/exec/lip.jsonl", a.data_dir);
    let text = match std::fs::read_to_string(&log) {
        Ok(t) => t,
        // Never run: nothing to show, which is not an error.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return json!({ "error": format!("read {log}: {e}") }).to_string(),
    };
    let folded = fold(&text);
    let (venue, venue_age_s) = venue_positions(a);

    let (live, live_age_s, live_error) = match super::now::armed_engine() {
        Some((unit, p)) => {
            let (v, age, err) = super::now::engine_summary(&unit, p.up_s);
            (v.and_then(|v| v.get("lip").cloned()), age, err)
        }
        None => (None, None, Some("no arb-trader is running".to_string())),
    };
    let live_mkts = live.as_ref().and_then(|l| l.get("markets")).cloned().unwrap_or(Value::Null);

    // A market shows when the log traded it, or the quoter rests or holds there now.
    let mut names: Vec<String> = folded.positions.keys().cloned().collect();
    if let Some(m) = live_mkts.as_object() {
        for (k, v) in m {
            let active = v["pos"].as_i64().unwrap_or(0) != 0
                || v["resting"].as_array().is_some_and(|r| r.iter().any(|x| !x.is_null()));
            if active && !names.contains(k) {
                names.push(k.clone());
            }
        }
    }
    let mut rows: Vec<Value> = names
        .iter()
        .map(|m| {
            let p = folded.positions.get(m).cloned().unwrap_or_default();
            let l = &live_mkts[m];
            let v = venue.get(m);
            json!({
                "market": m,
                "pos": p.pos,
                "avg_price": (p.pos != 0.0).then_some(p.avg),
                "realized_usd": p.realized,
                "fills": p.fills,
                "contracts": p.contracts,
                "last_fill_ts": (p.fills > 0).then_some(p.last_ts),
                "venue_pos": v.and_then(|v| num(&v["position_fp"])),
                "venue_fees_usd": v.and_then(|v| num(&v["fees_paid_dollars"])),
                "pnl_marked_usd": l.get("pnl_usd").cloned().unwrap_or(Value::Null),
                "modelled_usd_day": l.get("modelled_usd_day").cloned().unwrap_or(Value::Null),
                "resting": l.get("resting").cloned().unwrap_or(Value::Null),
                "held_by": l.get("held_by").cloned().unwrap_or(Value::Null),
            })
        })
        .collect();
    rows.sort_by(|x, y| {
        let k = |r: &Value| -num(&r["pos"]).unwrap_or(0.0).abs();
        k(x).partial_cmp(&k(y)).unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x["market"].as_str().cmp(&y["market"].as_str()))
    });

    let realized: f64 = folded.positions.values().map(|p| p.realized).sum();
    let stop = std::fs::read_to_string(format!("{log}.STOP")).ok();
    json!({
        "log": log,
        "positions": rows,
        "fills": folded.fills.iter().take(FILLS_SHOWN).collect::<Vec<_>>(),
        "fills_total": folded.fills.len(),
        "adopts": folded.adopts,
        "bad_lines": folded.bad_lines,
        "totals": {
            "realized_usd": realized,
            "contracts": folded.positions.values().map(|p| p.contracts).sum::<f64>(),
            "open_markets": folded.positions.values().filter(|p| p.pos != 0.0).count(),
            "net_contracts": folded.positions.values().map(|p| p.pos.abs()).sum::<f64>(),
        },
        "live": live.map(|mut l| {
            if let Some(o) = l.as_object_mut() {
                o.remove("markets");
            }
            l
        }),
        "live_age_s": live_age_s,
        "live_error": live_error,
        "venue_age_s": venue_age_s,
        "stop": stop,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(side: &str, qty: i64, price: &str, ts: f64) -> String {
        format!(
            r#"{{"event":"fill","market":"KXA-1","order":"o","pos":0,"price":"{price}","qty":{qty},"side":"{side}","ts":{ts}}}"#
        )
    }

    /// A long sold through flat into a short, added to, then partly bought
    /// back: +2c on the 33 closed long, -3.34c on the 20 closed short.
    #[test]
    fn a_round_trip_and_a_flip_through_flat_fold_at_average_cost() {
        let text = [
            fill("bid", 33, "0.8400", 1.0),
            fill("ask", 100, "0.8600", 2.0),
            fill("ask", 33, "0.8800", 3.0),
            fill("bid", 20, "0.9000", 4.0),
        ]
        .join("\n");
        let f = fold(&text);
        let p = &f.positions["KXA-1"];
        assert_eq!(p.pos, -80.0);
        assert!((p.avg - 0.8666).abs() < 1e-9, "short 67 @ 0.86 and 33 @ 0.88, got {}", p.avg);
        assert!((p.realized - (33.0 * 0.02 - 20.0 * (0.90 - 0.8666))).abs() < 1e-9, "got {}", p.realized);
        assert_eq!(p.fills, 4);
        assert_eq!(p.contracts, 186.0);
        assert_eq!(f.fills.len(), 4);
        assert_eq!(f.fills[0]["ts"], 4.0, "newest first");
        assert_eq!(f.fills[0]["side"], "buy YES");
        assert_eq!(f.fills[3]["side"], "buy YES");
        assert_eq!(f.fills[2]["side"], "sell YES");
    }

    #[test]
    fn closing_to_flat_clears_the_average_and_an_adoption_moves_the_position() {
        let text = [
            fill("bid", 10, "0.4000", 1.0),
            fill("ask", 10, "0.4500", 2.0),
            r#"{"event":"adopt","market":"KXA-1","qty":-5,"price":"0.5000","ts":3.0}"#.to_string(),
        ]
        .join("\n");
        let f = fold(&text);
        let p = &f.positions["KXA-1"];
        assert!((p.realized - 0.5).abs() < 1e-9);
        assert_eq!((p.pos, p.avg), (-5.0, 0.5));
        assert_eq!(f.adopts, 1);
        assert_eq!(f.fills.len(), 2, "an adoption is not a fill");
    }

    #[test]
    fn stops_are_skipped_and_torn_lines_are_counted() {
        let text = [
            r#"{"event":"stop","why":"LOSS STOP"}"#.to_string(),
            "\0\0{\"event\":\"fi".to_string(),
            r#"{"event":"fill","market":"KXA-1","qty":5,"price":"0.5","side":"sideways"}"#.to_string(),
            fill("bid", 5, "0.5000", 1.0),
        ]
        .join("\n");
        let f = fold(&text);
        assert_eq!(f.bad_lines, 2);
        assert_eq!(f.positions["KXA-1"].pos, 5.0);
    }
}
