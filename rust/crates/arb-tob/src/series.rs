//! Read back the ToB series and align two legs onto one timeline.
//!
//! Samples are emitted on CHANGE, so the two legs of a pair almost never share
//! timestamps. Aligning means merging their timestamps and FORWARD-FILLING each
//! leg — a quote stands until it is replaced, which is what a book actually
//! does. The alternative (only plotting instants where both legs ticked) would
//! throw away nearly every point.

use std::collections::BTreeSet;

use serde::Serialize;

use crate::TobSample;

/// One leg's quote at a point on the merged timeline. `None` before that leg's
/// first sample — the series legitimately starts when the book first synced,
/// which for a day-boundary replay is not midnight.
#[derive(Debug, Clone, Serialize)]
pub struct LegPoint {
    pub bid: Option<String>,
    pub ask: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlignedPoint {
    pub ts_local_ns: i64,
    pub a: LegPoint,
    pub b: LegPoint,
    /// Cost to open the basket at these quotes, gross of fees: lift leg A's ask
    /// and take the opposite side of leg B. Below 1.00 is an arb before fees.
    /// `None` until both legs have quoted.
    pub basket_cost: Option<f64>,
    /// Nanoseconds since each leg ACTUALLY ticked. Forward-fill makes every
    /// point look equally fresh, which would overstate arbitrage: a stale quote
    /// on one leg paired with a live one on the other produces a basket cost
    /// that was never simultaneously available. Carried so a consumer can
    /// discount or hide those points rather than believe them.
    pub age_a_ns: Option<i64>,
    pub age_b_ns: Option<i64>,
}

impl AlignedPoint {
    /// Both legs quoted within `max_age_ns`, so the cost was actually
    /// obtainable at this instant.
    pub fn is_fresh(&self, max_age_ns: i64) -> bool {
        matches!((self.age_a_ns, self.age_b_ns), (Some(a), Some(b))
            if a <= max_age_ns && b <= max_age_ns)
    }
}

/// Load samples for one market from a set of daily ToB files.
pub fn load_market(paths: &[String], venue: &str, market_id: &str) -> Vec<TobSample> {
    let mut out = Vec::new();
    for p in paths {
        let Ok(text) = std::fs::read_to_string(p) else { continue };
        for line in text.lines() {
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            // Cheap pre-filter: skip the serde cost for other markets entirely.
            if !t.contains(market_id) {
                continue;
            }
            let Ok(s) = serde_json::from_str::<TobSample>(t) else { continue };
            if s.venue == venue && s.market_id == market_id {
                out.push(s);
            }
        }
    }
    out.sort_by_key(|s| s.ts_local_ns);
    out
}

fn f(s: &Option<String>) -> Option<f64> {
    s.as_ref().and_then(|x| x.parse().ok())
}

/// Merge two legs onto one forward-filled timeline.
///
/// `b_side_is_yes` says whether leg B is held on the YES axis like leg A. For
/// the cross-venue baskets we run, both legs are quoted on the YES axis but we
/// take OPPOSITE sides, so the basket costs `ask_a + (1 - bid_b)`.
pub fn align(a: &[TobSample], b: &[TobSample]) -> Vec<AlignedPoint> {
    let mut ts: BTreeSet<i64> = BTreeSet::new();
    ts.extend(a.iter().map(|s| s.ts_local_ns));
    ts.extend(b.iter().map(|s| s.ts_local_ns));

    let (mut ia, mut ib) = (0usize, 0usize);
    let (mut ca, mut cb): (Option<&TobSample>, Option<&TobSample>) = (None, None);
    let mut out = Vec::with_capacity(ts.len());

    for t in ts {
        while ia < a.len() && a[ia].ts_local_ns <= t {
            ca = Some(&a[ia]);
            ia += 1;
        }
        while ib < b.len() && b[ib].ts_local_ns <= t {
            cb = Some(&b[ib]);
            ib += 1;
        }
        let la = LegPoint {
            bid: ca.and_then(|s| s.bid.clone()),
            ask: ca.and_then(|s| s.ask.clone()),
        };
        let lb = LegPoint {
            bid: cb.and_then(|s| s.bid.clone()),
            ask: cb.and_then(|s| s.ask.clone()),
        };
        let basket_cost = match (f(&la.ask), f(&lb.bid)) {
            (Some(ask_a), Some(bid_b)) => Some(ask_a + (1.0 - bid_b)),
            _ => None,
        };
        out.push(AlignedPoint {
            ts_local_ns: t,
            a: la,
            b: lb,
            basket_cost,
            age_a_ns: ca.map(|s| t - s.ts_local_ns),
            age_b_ns: cb.map(|s| t - s.ts_local_ns),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(venue: &str, m: &str, ts: i64, bid: &str, ask: &str) -> TobSample {
        TobSample {
            venue: venue.into(),
            market_id: m.into(),
            ts_local_ns: ts,
            bid: Some(bid.into()),
            bid_size: None,
            ask: Some(ask.into()),
            ask_size: None,
        }
    }

    #[test]
    fn forward_fills_each_leg_and_computes_basket_cost() {
        let a = vec![s("kalshi", "K", 10, "0.20", "0.22"), s("kalshi", "K", 30, "0.25", "0.26")];
        let b = vec![s("polymarket_us", "P", 20, "0.80", "0.83")];
        let got = align(&a, &b);

        // merged timeline 10, 20, 30
        assert_eq!(got.len(), 3);

        // t=10: only leg A has quoted -> no basket cost yet
        assert_eq!(got[0].a.ask.as_deref(), Some("0.22"));
        assert!(got[0].b.bid.is_none());
        assert!(got[0].basket_cost.is_none());

        // t=20: leg A forward-filled from t=10, leg B fresh.
        // cost = 0.22 + (1 - 0.80) = 0.42
        assert_eq!(got[1].a.ask.as_deref(), Some("0.22"), "leg A must forward-fill");
        assert!((got[1].basket_cost.unwrap() - 0.42).abs() < 1e-9);

        // t=30: leg A moves, leg B forward-fills. cost = 0.26 + 0.20 = 0.46
        assert_eq!(got[2].b.bid.as_deref(), Some("0.80"), "leg B must forward-fill");
        assert!((got[2].basket_cost.unwrap() - 0.46).abs() < 1e-9);
    }

    #[test]
    fn staleness_is_carried_so_a_forward_filled_cost_is_not_believed() {
        // Leg A quotes once at t=10 and never again; leg B ticks at t=1000.
        // The t=1000 point has a 990ns-old leg A — a cost that was never
        // simultaneously available.
        let a = vec![s("kalshi", "K", 10, "0.20", "0.22")];
        let b = vec![s("polymarket_us", "P", 10, "0.80", "0.83"),
                     s("polymarket_us", "P", 1000, "0.90", "0.93")];
        let got = align(&a, &b);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].age_a_ns, Some(0));
        assert_eq!(got[1].age_a_ns, Some(990), "leg A is 990ns stale here");
        assert_eq!(got[1].age_b_ns, Some(0));
        assert!(got[0].is_fresh(100));
        assert!(!got[1].is_fresh(100), "stale leg must not read as fresh");
        // and it IS the cheap-looking point, which is exactly the trap
        assert!(got[1].basket_cost.unwrap() < got[0].basket_cost.unwrap());
    }

    #[test]
    fn a_cost_below_one_is_the_arb() {
        // Kalshi ask 0.30, PM-US bid 0.75 -> 0.30 + 0.25 = 0.55, deep arb.
        let a = vec![s("kalshi", "K", 1, "0.29", "0.30")];
        let b = vec![s("polymarket_us", "P", 1, "0.75", "0.78")];
        let got = align(&a, &b);
        assert_eq!(got.len(), 1);
        assert!(got[0].basket_cost.unwrap() < 1.0);
    }

    #[test]
    fn load_market_filters_by_venue_and_market() {
        let p = std::env::temp_dir().join(format!("arb-tob-series-{}.jsonl", std::process::id()));
        let lines = [
            serde_json::to_string(&s("kalshi", "K", 2, "0.1", "0.2")).unwrap(),
            serde_json::to_string(&s("kalshi", "OTHER", 3, "0.1", "0.2")).unwrap(),
            serde_json::to_string(&s("polymarket_us", "K", 4, "0.1", "0.2")).unwrap(),
            serde_json::to_string(&s("kalshi", "K", 1, "0.3", "0.4")).unwrap(),
        ];
        std::fs::write(&p, lines.join("\n")).unwrap();
        let got = load_market(&[p.to_string_lossy().to_string()], "kalshi", "K");
        assert_eq!(got.len(), 2, "same venue AND market only");
        assert_eq!(got[0].ts_local_ns, 1, "sorted by time");
        let _ = std::fs::remove_file(p);
    }
}
