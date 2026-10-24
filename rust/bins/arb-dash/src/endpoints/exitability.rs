//! The Exitability view: `arb-exitability`'s nightly fit, priced at the bar in
//! force right now.
//!
//! The fit says, per pair, how likely a lot the hold rule refuses is to be sold
//! back by the engine inside two weeks (`lens_p_lcb`, an 80% lower bound), how
//! soon (`lens_exit_days`), and for how much over what it cost (`lens_gain`).
//! What that is WORTH depends on the hurdle, which moves every day, so the
//! margins are computed here on each request rather than frozen into the file.
//! At hurdle r a basket costing C is worth holding to resolution iff
//! C ≤ V_hold = 1 / (1 + r·T), T = years to resolution. With the exit it is
//! worth buying iff, with τ = years to it,
//!
//!   p·(C + g) / (1 + r·τ) + (1 − p)·V_hold ≥ C
//!   ⇔  C ≤ C_max = (p·g / (1 + r·τ) + (1 − p)·V_hold) / (1 − p / (1 + r·τ))
//!
//! so the hold rule asks for a lock of `1 − V_hold` and the lens for
//! `1 − C_max`: never more than holding asks, and never under the engine's
//! half-cent floor. When resolution is inside the horizon, p is cut to the
//! share of the pair's exits that come before it. Both margins are shown; the
//! engine reads neither from here.

use std::collections::BTreeMap;

use arb_core::clock::now_secs;
use arb_core::resolve::{parse_iso, today_iso};
use serde_json::{json, Value};

use crate::Args;

/// The engine's floor, used when no armed engine has reported a bar.
const FLOOR_APR_PCT: f64 = 50.0;
/// The engine's half-cent floor: no lock under it is entered, so no lens
/// margin is under it either.
const MIN_LOCK: f64 = 0.005;

/// What a pair's lock must be under each rule, at hurdle `r` (a fraction).
#[derive(Debug, PartialEq)]
pub(crate) struct Margins {
    pub hold: f64,
    pub lens: f64,
}

/// What a lot the hold rule refuses does on a pair: the chance it is sold
/// back inside the horizon, the mean days until it is, and the mean proceeds
/// over cost when it is.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Exit {
    pub p: f64,
    pub days: f64,
    pub gain: f64,
}

pub(crate) fn margins(r: f64, days_to_resolve: f64, exit: Option<Exit>) -> Margins {
    let v_hold = 1.0 / (1.0 + r * days_to_resolve.max(1.0) / 365.25);
    let hold = 1.0 - v_hold;
    let floor = MIN_LOCK.min(hold);
    let lens = exit.filter(|x| x.p > 0.0 && x.days.is_finite() && x.gain.is_finite()).map_or(hold, |x| {
        let d = 1.0 + r * x.days.max(0.0) / 365.25;
        let room = 1.0 - x.p / d;
        if room <= 1e-9 {
            return floor;
        }
        let c_max = (x.p * x.gain / d + (1.0 - x.p) * v_hold) / room;
        (1.0 - c_max).clamp(floor, hold)
    });
    Margins { hold, lens }
}

/// The share of a pair's exits inside the horizon that come within `days`:
/// its `curve` at `curve_days`, joined by straight lines from the origin.
fn share_by(curve_days: &[f64], curve: &[f64], days: f64) -> f64 {
    let (Some(&last_day), Some(&last)) = (curve_days.last(), curve.last()) else { return 1.0 };
    if last <= 0.0 || days >= last_day {
        return 1.0;
    }
    let mut prev = (0.0, 0.0);
    for (&d, &v) in curve_days.iter().zip(curve) {
        if days <= d {
            let at = prev.1 + (v - prev.1) * (days - prev.0) / (d - prev.0);
            return (at / last).clamp(0.0, 1.0);
        }
        prev = (d, v);
    }
    1.0
}

/// Entries clearing a margin `m`, read off the nightly's `clearing` grid of
/// step `step` at the grid point at or above `m`, so never more than do.
fn clearing_at(curve: &[u64], step: f64, m: f64) -> u64 {
    let j = (m / step - 1e-9).ceil().max(0.0) as usize;
    curve.get(j).copied().unwrap_or(0)
}

fn floats(v: Option<&Value>) -> Vec<f64> {
    v.and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_f64).collect()).unwrap_or_default()
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
    let curve_days = floats(doc.get("curve_days"));
    let step = doc.get("clear_step").and_then(Value::as_f64).unwrap_or(0.001);

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
            let num = |k: &str| p.get(k).and_then(Value::as_f64);
            let exit = (|| Some(Exit { p: num("lens_p_lcb")?, days: num("lens_exit_days")?, gain: num("lens_gain")? }))();
            let curve = floats(p.get("curve"));
            let clearing: Vec<u64> =
                p.get("clearing").and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_u64).collect()).unwrap_or_default();
            // Per day the pair was on the tape at all, not per day of the window:
            // a pair listed last week has had a week to fill. At least one day,
            // so an afternoon's fills do not read as a rate.
            let observed_days = (num("valid_minutes").unwrap_or(0.0) / 1440.0).max(1.0);
            let Some(o) = p.as_object_mut() else { continue };
            o.insert("days_to_resolve".into(), json!(days));
            if let Some(d) = days.filter(|d| *d >= 0.0) {
                let exit = exit.map(|x| Exit { p: x.p * share_by(&curve_days, &curve, d), ..x });
                let m = margins(r, d, exit);
                o.insert("margin_hold".into(), json!(m.hold));
                o.insert("margin_lens".into(), json!(m.lens));
                o.insert("clear_hold_per_day".into(), json!(clearing_at(&clearing, step, m.hold) as f64 / observed_days));
                o.insert("clear_lens_per_day".into(), json!(clearing_at(&clearing, step, m.lens) as f64 / observed_days));
            }
            let rel = o.get("rel").and_then(Value::as_str).unwrap_or("").to_string();
            o.insert("history".into(), json!(series.remove(&rel).unwrap_or_default()));
        }
    }
    let Some(o) = doc.as_object_mut() else {
        return json!({"error": "latest.json is not an object"}).to_string();
    };
    o.insert("bar_pct".into(), json!(bar_pct));
    o.insert("bar_source".into(), json!(bar_source));
    o.insert("age_s".into(), json!(super::age_secs(&format!("{dir}/latest.json"))));
    o.insert(
        "runs".into(),
        hist.iter()
            .map(|h| {
                json!({"as_of": h.get("as_of"), "share": h.get("share"), "rate": h.get("rate"),
                       "calibration_lots": h.get("calibration_lots"), "p_global": h.get("p_global")})
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

    fn exit(p: f64, days: f64, gain: f64) -> Option<Exit> {
        Some(Exit { p, days, gain })
    }

    /// 100 days at 50%: holding needs a 12c lock. A lot sold back for 2c over
    /// its cost within three days, half the time, needs about 10c; and at that
    /// cost it is worth exactly what it costs.
    #[test]
    fn the_lens_margin_is_where_the_lot_breaks_even() {
        let (r, t) = (0.5, 100.0);
        let m = margins(r, t, exit(0.5, 3.0, 0.02));
        assert!((m.hold - 0.1204).abs() < 1e-3, "{}", m.hold);
        assert!((m.lens - 0.1041).abs() < 1e-3, "{}", m.lens);
        let (c, v_hold, d) = (1.0 - m.lens, 1.0 - m.hold, 1.0 + r * 3.0 / 365.25);
        assert!((0.5 * (c + 0.02) / d + 0.5 * v_hold - c).abs() < 1e-9, "zero NPV at the margin");
    }

    /// Sold back nearly always and at once, a lot is worth nearly what it
    /// sells for, so the lens drops to the engine's floor and no further.
    #[test]
    fn a_sure_quick_exit_asks_only_the_engines_floor() {
        assert!(close(margins(0.5, 100.0, exit(0.99, 0.1, 0.05)).lens, MIN_LOCK));
        assert!(close(margins(0.5, 100.0, exit(1.0, 0.0, 0.05)).lens, MIN_LOCK));
        let tomorrow = margins(0.5, 1.0, exit(0.99, 0.1, 0.05));
        assert!(tomorrow.hold < MIN_LOCK && close(tomorrow.lens, tomorrow.hold), "never over holding");
    }

    #[test]
    fn no_exit_evidence_is_the_hold_rule() {
        let m = margins(0.5, 100.0, exit(0.0, 2.0, 0.02));
        assert!(close(m.hold, m.lens));
        let m = margins(0.5, 100.0, None);
        assert!(close(m.hold, m.lens));
    }

    /// An exit slower than resolution is worth nothing extra, not less.
    #[test]
    fn the_lens_never_asks_for_more_than_holding() {
        let m = margins(0.5, 5.0, exit(0.9, 30.0, 0.02));
        assert!(close(m.hold, m.lens));
    }

    #[test]
    fn resolution_inside_the_horizon_keeps_only_the_exits_before_it() {
        let (days, curve) = ([1.0, 7.0, 14.0], [0.2, 0.5, 0.6]);
        assert!(close(share_by(&days, &curve, 14.0), 1.0));
        assert!(close(share_by(&days, &curve, 30.0), 1.0));
        assert!(close(share_by(&days, &curve, 7.0), 0.5 / 0.6));
        assert!(close(share_by(&days, &curve, 0.5), 0.1 / 0.6));
        assert!(close(share_by(&days, &curve, 0.0), 0.0));
        assert!(close(share_by(&days, &[0.0, 0.0, 0.0], 3.0), 1.0), "no exits to cut");
    }

    #[test]
    fn clearing_reads_the_grid_at_or_above_the_margin() {
        let curve = [5, 5, 4, 2];
        assert_eq!(clearing_at(&curve, 0.001, 0.0), 5);
        assert_eq!(clearing_at(&curve, 0.001, 0.0015), 4);
        assert_eq!(clearing_at(&curve, 0.001, 0.002), 4);
        assert_eq!(clearing_at(&curve, 0.001, 0.003), 2);
        assert_eq!(clearing_at(&curve, 0.001, 0.01), 0);
    }

    #[test]
    fn a_missing_fit_is_an_error_that_names_the_timer() {
        let v: Value = serde_json::from_str(&json(&Args::for_test())).unwrap();
        assert!(v["error"].as_str().unwrap().contains("arbbot-exitability.timer"));
    }
}
