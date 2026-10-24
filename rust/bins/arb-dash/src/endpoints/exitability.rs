//! The Exitability view: `arb-exitability`'s nightly fit, priced at the bar in
//! force right now.
//!
//! The fit says, per pair, how likely a lot is to come back at par inside two
//! weeks (`p`, and its 80% lower bound `p_lcb`) and how soon (`mean_exit_days`).
//! What that is WORTH depends on the hurdle, which moves every day, so the
//! margins are computed here on each request rather than frozen into the file:
//!
//!   hold value   V_hold = 1 / (1 + r·T)        T = years to resolution
//!   exit value   V_exit = 1 / (1 + r·τ)        τ = mean years to a par exit
//!   lens value   V0     = p_lcb·V_exit + (1 − p_lcb)·V_hold,  never below V_hold
//!
//! The entry bar today admits a basket costing less than `V_hold`, i.e. a lock
//! of at least `1 − V_hold`. The lens would admit one costing less than `V0`.
//! Both margins are shown; the engine reads neither from here.

use std::collections::BTreeMap;

use arb_core::clock::now_secs;
use arb_core::resolve::{parse_iso, today_iso};
use serde_json::{json, Value};

use crate::Args;

/// The engine's floor, used when no armed engine has reported a bar.
const FLOOR_APR_PCT: f64 = 50.0;

/// What a pair's lock must be under each rule, at hurdle `r` (a fraction).
#[derive(Debug, PartialEq)]
pub(crate) struct Margins {
    pub hold: f64,
    pub lens: f64,
}

pub(crate) fn margins(r: f64, days_to_resolve: f64, p_lcb: f64, mean_exit_days: Option<f64>) -> Margins {
    let v_hold = 1.0 / (1.0 + r * days_to_resolve.max(1.0) / 365.25);
    let v0 = match mean_exit_days {
        Some(tau) if tau.is_finite() => {
            let v_exit = 1.0 / (1.0 + r * tau.max(0.0) / 365.25);
            (p_lcb * v_exit + (1.0 - p_lcb) * v_hold).max(v_hold)
        }
        _ => v_hold,
    };
    Margins { hold: 1.0 - v_hold, lens: 1.0 - v0 }
}

/// How many of `locks` (ascending) are at least `m`.
fn clearing(locks: &[f64], m: f64) -> usize {
    locks.len() - locks.partition_point(|l| *l < m - 1e-9)
}

fn read_json(path: &str) -> Result<Value, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("parse {path}: {e}"))
}

/// One row per `as_of`, the last run of each winning: a backfill re-run
/// replaces the line it re-measured rather than doubling it.
fn history(path: &str) -> Vec<Value> {
    let mut by_day: BTreeMap<String, Value> = BTreeMap::new();
    for line in std::fs::read_to_string(path).unwrap_or_default().lines() {
        if let Ok(v) = serde_json::from_str::<Value>(line) {
            if let Some(d) = v.get("as_of").and_then(Value::as_str) {
                by_day.insert(d.to_string(), v);
            }
        }
    }
    by_day.into_values().collect()
}

pub fn json(a: &Args) -> String {
    let dir = format!("{}/exitability", a.data_dir);
    let mut doc = match read_json(&format!("{dir}/latest.json")) {
        Ok(d) => d,
        Err(e) => {
            return json!({"error": format!("{e} — arbbot-exitability.timer writes it nightly")}).to_string()
        }
    };
    let (bar_pct, bar_source) = match super::now::maker_apr_bar() {
        Some(b) => (b, "engine"),
        None => (FLOOR_APR_PCT, "floor"),
    };
    let r = bar_pct / 100.0;
    let now = now_secs() as f64;
    let today = parse_iso(&today_iso(now));
    let window_days = doc.get("window_days").and_then(Value::as_f64).unwrap_or(28.0);

    let hist = history(&format!("{dir}/history.jsonl"));
    let mut series: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for h in &hist {
        let day = h.get("as_of").cloned().unwrap_or(Value::Null);
        for (rel, v) in h.get("pairs").and_then(Value::as_object).into_iter().flatten() {
            series.entry(rel.clone()).or_default().push(json!([day, v.get("p"), v.get("p_lcb")]));
        }
    }

    if let Some(pairs) = doc.get_mut("pairs").and_then(Value::as_array_mut) {
        for p in pairs {
            let days = p
                .get("resolves")
                .and_then(Value::as_str)
                .and_then(parse_iso)
                .zip(today)
                .map(|(rd, t)| (rd - t) as f64);
            let p_lcb = p.get("p_lcb").and_then(Value::as_f64).unwrap_or(0.0);
            let tau = p.get("mean_exit_days").and_then(Value::as_f64);
            let locks: Vec<f64> = p
                .get("locks")
                .and_then(Value::as_array)
                .map(|l| l.iter().filter_map(Value::as_f64).collect())
                .unwrap_or_default();
            let o = p.as_object_mut().expect("a pair is an object");
            o.insert("days_to_resolve".into(), json!(days));
            if let Some(d) = days.filter(|d| *d > 0.0) {
                let m = margins(r, d, p_lcb, tau);
                o.insert("margin_hold".into(), json!(m.hold));
                o.insert("margin_lens".into(), json!(m.lens));
                o.insert("clear_hold_per_day".into(), json!(clearing(&locks, m.hold) as f64 / window_days));
                o.insert("clear_lens_per_day".into(), json!(clearing(&locks, m.lens) as f64 / window_days));
            }
            o.insert("history".into(), json!(series.remove(o["rel"].as_str().unwrap_or("")).unwrap_or_default()));
        }
    }
    let o = doc.as_object_mut().expect("latest.json is an object");
    o.insert("bar_pct".into(), json!(bar_pct));
    o.insert("bar_source".into(), json!(bar_source));
    o.insert("age_s".into(), json!(super::age_secs(&format!("{dir}/latest.json"))));
    o.insert(
        "runs".into(),
        hist.iter()
            .map(|h| {
                json!({"as_of": h.get("as_of"), "k": h.get("k"), "calibration_lots": h.get("calibration_lots"),
                       "p_global": h.get("p_global")})
            })
            .collect(),
    );
    doc.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    /// 100 days at 50%: holding needs a 12c lock. A pair that exits at par in
    /// two days 87% of the time needs about 2c.
    #[test]
    fn a_fast_exit_lowers_the_margin_a_long_pair_needs() {
        let m = margins(0.5, 100.0, 0.87, Some(2.0));
        assert!((m.hold - 0.1204).abs() < 1e-3, "{}", m.hold);
        assert!(m.lens < 0.02 && m.lens > 0.01, "{}", m.lens);
    }

    #[test]
    fn no_exit_evidence_is_the_hold_rule() {
        let m = margins(0.5, 100.0, 0.0, Some(2.0));
        assert!(close(m.hold, m.lens));
        let m = margins(0.5, 100.0, 0.9, None);
        assert!(close(m.hold, m.lens));
    }

    /// An exit slower than resolution is worth nothing extra, not less.
    #[test]
    fn the_lens_never_asks_for_more_than_holding() {
        let m = margins(0.5, 5.0, 0.9, Some(30.0));
        assert!(close(m.hold, m.lens));
    }

    #[test]
    fn clearing_counts_locks_at_or_above_the_margin() {
        let l = [0.005, 0.01, 0.02, 0.03];
        assert_eq!(clearing(&l, 0.02), 2);
        assert_eq!(clearing(&l, 0.0), 4);
        assert_eq!(clearing(&l, 0.5), 0);
    }

    #[test]
    fn a_missing_fit_is_an_error_that_names_the_timer() {
        let v: Value = serde_json::from_str(&json(&Args::for_test())).unwrap();
        assert!(v["error"].as_str().unwrap().contains("arbbot-exitability.timer"));
    }
}
