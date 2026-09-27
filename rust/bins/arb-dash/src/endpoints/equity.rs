//! Day-by-day portfolio equity, from the samples `arbbot-equity.timer`
//! (scripts/snapshot_equity.sh) appends to `data/exec/equity_history.jsonl`.
//!
//! A day's close is its LAST sample, and its change is measured from the
//! previous recorded day's close — so a gap straddling midnight lands in the
//! day the next sample falls in instead of vanishing. The first recorded day
//! has no previous close and is measured from its own first sample.
//!
//! Deposits and withdrawals are NOT netted out: config/funding.yaml carries no
//! dates, so a transfer reads as a gain or loss on the day it lands.
use crate::endpoints::capital::number;
use crate::Args;
use serde_json::{json, Value};
use std::collections::BTreeMap;

struct Sample {
    at: u64,
    kalshi: f64,
    pmus: f64,
}

impl Sample {
    fn equity(&self) -> f64 {
        self.kalshi + self.pmus
    }
}

/// A sample missing either venue is unreadable, never half an equity figure.
fn sample(v: &Value) -> Option<(String, Sample)> {
    let a = &v["accounts"];
    let s = Sample {
        at: v["at"].as_u64()?,
        kalshi: number(&a["kalshi"]["equity_usd"])?,
        pmus: number(&a["polymarket_us"]["equity_usd"])?,
    };
    Some((v["day"].as_str()?.to_string(), s))
}

fn build(text: &str) -> Value {
    let mut days: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    let mut unreadable = 0;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        match serde_json::from_str::<Value>(line).ok().as_ref().and_then(sample) {
            Some((day, s)) => days.entry(day).or_default().push(s),
            None => unreadable += 1,
        }
    }
    let mut prev_close: Option<f64> = None;
    let mut rows = Vec::new();
    for (day, mut ss) in days {
        ss.sort_by_key(|s| s.at);
        let (Some(first), Some(last)) = (ss.first(), ss.last()) else { continue };
        let (open, close) = (first.equity(), last.equity());
        let low = ss.iter().map(Sample::equity).fold(f64::INFINITY, f64::min);
        let high = ss.iter().map(Sample::equity).fold(f64::NEG_INFINITY, f64::max);
        let base = prev_close.unwrap_or(open);
        rows.push(json!({
            "day": day, "samples": ss.len(), "close_at": last.at,
            "open_usd": open, "close_usd": close, "low_usd": low, "high_usd": high,
            "kalshi_usd": last.kalshi, "polymarket_us_usd": last.pmus,
            "change_usd": close - base,
            "change_pct": (base > 0.).then(|| (close / base - 1.) * 100.),
            "change_from": if prev_close.is_some() { "previous close" } else { "first sample" },
        }));
        prev_close = Some(close);
    }
    json!({"days": rows, "unreadable": unreadable})
}

pub fn json(a: &Args) -> String {
    let path = format!("{}/exec/equity_history.jsonl", a.data_dir);
    match std::fs::read_to_string(&path) {
        Ok(text) => build(&text).to_string(),
        Err(e) => json!({"error": format!(
            "No equity history yet ({path}: {e}). arbbot-equity.timer appends a sample every 15 minutes."
        )})
        .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(at: u64, day: &str, kalshi: &str, pmus: &str) -> String {
        json!({"at": at, "day": day, "accounts": {
            "kalshi": {"equity_usd": kalshi}, "polymarket_us": {"equity_usd": pmus}}})
        .to_string()
    }

    #[test]
    fn a_day_changes_from_the_previous_close_and_the_first_from_its_own_open() {
        let text = [
            line(100, "2026-09-27", "300", "700"),
            line(200, "2026-09-27", "310", "700"),
            line(300, "2026-09-28", "300", "690"),
            line(400, "2026-09-28", "305", "700"),
        ]
        .join("\n");
        let d = build(&text);
        let days = d["days"].as_array().unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0]["change_usd"], 10.);
        assert_eq!(days[0]["change_from"], "first sample");
        assert_eq!(days[1]["open_usd"], 990.);
        assert_eq!(days[1]["close_usd"], 1005.);
        assert_eq!(days[1]["change_usd"], -5.);
        assert_eq!(days[1]["change_from"], "previous close");
        assert_eq!(days[1]["low_usd"], 990.);
        assert_eq!(days[1]["high_usd"], 1005.);
        assert_eq!(days[1]["kalshi_usd"], 305.);
    }

    #[test]
    fn the_close_is_the_latest_sample_not_the_last_line() {
        let text = [line(200, "2026-09-27", "1", "2"), line(100, "2026-09-27", "5", "5")].join("\n");
        let d = build(&text);
        assert_eq!(d["days"][0]["close_usd"], 3.);
        assert_eq!(d["days"][0]["open_usd"], 10.);
    }

    #[test]
    fn a_torn_line_or_a_one_venue_sample_is_counted_not_summed() {
        let one_venue = json!({"at": 150, "day": "2026-09-27",
            "accounts": {"kalshi": {"equity_usd": "999"}}})
        .to_string();
        let text = [line(100, "2026-09-27", "1", "2"), one_venue, "{\"at\":2".into()].join("\n");
        let d = build(&text);
        assert_eq!(d["unreadable"], 2);
        assert_eq!(d["days"][0]["samples"], 1);
        assert_eq!(d["days"][0]["close_usd"], 3.);
    }
}
