//! Take-take: cross both books at once, detected on the tape event that
//! creates the opportunity.
//!
//! Port of `scripts/auto_take_take.py`. The Python version was a 5-minute
//! poll; crossings are transient, so this lives on the engine's book-event
//! path instead (Geoff 2026-07-28: "we need to be able to act in milliseconds
//! not minutes").
//!
//! THE BAR (Geoff 2026-07-21) is unchanged by the port: a crossing's
//! net-of-fee edge, expressed as APR, must beat our current BLENDED portfolio
//! APR — idle cash is better kept for a better crossing than spent on a
//! long-dated one that barely clears fees.
//!
//! Only the K->PM direction auto-executes (buy Kalshi YES at its ask, open PM
//! NO by selling PM YES at its bid). The persistent basis makes that the
//! profitable side; the reverse is detected and reported, never traded.
//!
//! Fees use the same conservative flat `FEE_CT` the Python bar used rather
//! than the engine's exact per-venue fee model. That is deliberate: this is a
//! port of a POLICY, and changing the fee basis would silently move the bar.

use arb_core::book::BookBuilder;
use arb_core::model::Venue;
use arb_core::scan::{Cx, Rel};

/// Conservative both-leg taker fee per contract. Port of `FEE_CT`.
const FEE_CT: &str = "0.02";

// The resolve-date table lives in arb_core::resolve, not here: the dashboard
// reports the APR of a trade this engine decided to make, and two copies of
// the table would let them disagree about what the same trade was worth.
pub use arb_core::resolve::{today_iso, years_to};
use arb_core::resolve::parse_iso;

/// Bar used when marks give us no blended APR. Port of Python's
/// `bar = args.min_apr if ... else (bapr or 12.0)` — a MISSING bar must not
/// collapse to zero, or every crossing that merely clears fees would fire.
pub const DEFAULT_BAR_APR: f64 = 12.0;

/// Current blended portfolio APR from `data/exec/marks.json` — the bar itself.
/// Port of `blended_apr()`: locked profit over cost, annualised by the
/// COST-WEIGHTED average time to resolution. `None` when marks are missing or
/// carry no priceable position, and a `None` bar must not be read as "no bar".
pub fn blended_apr(marks_json: &str, today_iso: &str) -> Option<f64> {
    let doc: serde_json::Value = serde_json::from_str(marks_json).ok()?;
    let today = parse_iso(today_iso)?;
    let (mut num, mut den, mut cost, mut prof) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    for p in doc.get("positions")?.as_array()? {
        let c = p.get("cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let pr = p.get("locked_profit_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
        cost += c;
        prof += pr;
        let Some(rb) = p.get("resolves_by").and_then(|v| v.as_str()) else { continue };
        if c == 0.0 {
            continue;
        }
        let Some(days) = parse_iso(rb).map(|d| d - today) else { continue };
        let yrs = days.max(1) as f64 / 365.25;
        num += c * yrs;
        den += c;
    }
    if den == 0.0 || cost == 0.0 {
        return None;
    }
    let wavg = num / den;
    if wavg == 0.0 {
        return None;
    }
    Some(prof / cost / wavg * 100.0)
}

/// Per-relationship re-fire gate.
///
/// A crossing is present on EVERY book event until someone takes it, and the
/// concentration cap reads exposure that does not move until a fill books. So
/// between placing leg 1 and booking it, an ungated detector would re-place
/// the same crossing on every tick. This is the thing standing between "acts
/// in milliseconds" and "sends a hundred orders in a second".
#[derive(Default)]
pub struct Gate {
    until: std::collections::HashMap<String, f64>,
}

impl Gate {
    /// `true` if this relationship may act now, which ALSO starts its
    /// cooldown — callers must not ask unless they intend to act.
    pub fn take(&mut self, rel_id: &str, now: f64, cooldown_s: f64) -> bool {
        if now < self.until.get(rel_id).copied().unwrap_or(f64::MIN) {
            return false;
        }
        self.until.insert(rel_id.to_string(), now + cooldown_s);
        true
    }
}

/// A crossing that clears the bar, sized and ready to fire.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub rel_id: String,
    /// Leg 1 — the CONSTRAINED leg, sold first (PM-US YES at its bid).
    pub pmus_market: String,
    pub pmus_bid: String,
    /// Leg 2 — bought for exactly what leg 1 fills (Kalshi YES at its ask).
    pub kalshi_market: String,
    pub kalshi_ask: String,
    pub size: i64,
    pub edge: String,
    pub net: String,
    pub apr: f64,
}

/// Why a relationship did not fire. Reported, not silently dropped: a
/// crossing that ALMOST cleared is the interesting telemetry.
#[derive(Debug, Clone, PartialEq)]
pub enum Skip {
    NoBook,
    /// A venue's own book is crossed (best ask <= best bid). That state is
    /// impossible on a live venue and always means OUR book is corrupt — a
    /// level added and never removed, or a missed resync. Observed
    /// 2026-07-28: KXRATECUT-26DEC31 carried a phantom ask at 0.073 under a
    /// 0.176 bid, which the detector read as a 9.7c crossing worth 20%/yr
    /// against a PM-US book that in truth agreed with Kalshi at ~17c.
    /// Trading it would have sold PM at 0.17 and then been unable to buy
    /// Kalshi anywhere near 0.073, leaving a naked short.
    CrossedBook { venue: &'static str },
    NoResolveDate,
    EdgeUnderFees,
    ReverseDirection,
    BelowBar { apr: f64, bar: f64 },
    AtCap { open: i64 },
    NoDepth,
}

fn top(levels: &[arb_core::model::Level]) -> Option<(&str, &str)> {
    levels.first().map(|l| (l.price.as_str(), l.size.as_str()))
}

fn leg_market(rel: &Rel, venue: Venue) -> Option<&str> {
    rel.legs.iter().find(|l| l.venue == venue).map(|l| l.market_id.as_str())
}

/// Test one relationship against the live books. Pure: no I/O, no clock — the
/// caller supplies `today_iso` and the bar, which is what makes this
/// replayable through the WAL harness.
#[allow(clippy::too_many_arguments)]
pub fn detect(
    cx: &mut Cx,
    rel: &Rel,
    books: &BookBuilder,
    today_iso: &str,
    bar_apr: f64,
    max_ct_per_rel: i64,
    open_ct: i64,
    max_clip: i64,
) -> Result<Candidate, Skip> {
    let (Some(kt), Some(ps)) = (leg_market(rel, Venue::Kalshi), leg_market(rel, Venue::PolymarketUs))
    else {
        return Err(Skip::NoBook);
    };
    let (Some(kb), Some(pb_book)) = (books.get(Venue::Kalshi, kt), books.get(Venue::PolymarketUs, ps))
    else {
        return Err(Skip::NoBook);
    };
    let (Some((k_ask, k_ask_sz)), Some((k_bid, _))) = (top(&kb.asks), top(&kb.bids)) else {
        return Err(Skip::NoBook);
    };
    let (Some((p_bid, p_bid_sz)), Some((p_ask, _))) =
        (top(&pb_book.bids), top(&pb_book.asks))
    else {
        return Err(Skip::NoBook);
    };

    let (ka, pb) = (cx.parse_exact(k_ask), cx.parse_exact(p_bid));
    let (kb_px, pa) = (cx.parse_exact(k_bid), cx.parse_exact(p_ask));
    // Sanity BEFORE arithmetic: a venue cannot be offering below its own bid.
    // If it appears to be, our book is wrong, and every edge derived from it
    // is fiction. Refuse rather than trade against our own corruption.
    if cx.cmp(ka, kb_px) != std::cmp::Ordering::Greater {
        return Err(Skip::CrossedBook { venue: "kalshi" });
    }
    if cx.cmp(pa, pb) != std::cmp::Ordering::Greater {
        return Err(Skip::CrossedBook { venue: "polymarket_us" });
    }
    // K->PM: buy Kalshi YES at its ask, sell PM YES at its bid.
    let edge_kpm = cx.sub(pb, ka);
    // The reverse leg-pairing, detected only so we can refuse it.
    let edge_pmk = cx.sub(kb_px, pa);
    if cx.cmp(edge_kpm, edge_pmk) == std::cmp::Ordering::Less {
        return Err(Skip::ReverseDirection);
    }

    let fee = cx.parse_exact(FEE_CT);
    let net = cx.sub(edge_kpm, fee);
    if !cx.is_pos(net) {
        return Err(Skip::EdgeUnderFees);
    }
    let cost_ct = cx.one_minus(edge_kpm);
    if !cx.is_pos(cost_ct) {
        return Err(Skip::EdgeUnderFees);
    }

    let Some(yrs) = years_to(&rel.id, today_iso) else {
        return Err(Skip::NoResolveDate);
    };
    // APR leaves exact arithmetic here, matching Python, which also floats at
    // this line. The comparison is a policy threshold, not money.
    let net_f: f64 = cx.emit_6dp(net).parse().unwrap_or(0.0);
    let cost_f: f64 = cx.emit_6dp(cost_ct).parse().unwrap_or(0.0);
    if cost_f <= 0.0 {
        return Err(Skip::EdgeUnderFees);
    }
    let apr = net_f / cost_f / yrs * 100.0;
    if apr < bar_apr {
        return Err(Skip::BelowBar { apr, bar: bar_apr });
    }

    let headroom = (max_ct_per_rel - open_ct).max(0);
    if headroom < 1 {
        return Err(Skip::AtCap { open: open_ct });
    }
    // Size is bounded by the depth on the levels we would actually cross.
    let depth_k: i64 = k_ask_sz.parse::<f64>().map(|f| f as i64).unwrap_or(0);
    let depth_p: i64 = p_bid_sz.parse::<f64>().map(|f| f as i64).unwrap_or(0);
    let size = headroom.min(depth_k.min(depth_p)).min(max_clip);
    if size < 1 {
        return Err(Skip::NoDepth);
    }

    Ok(Candidate {
        rel_id: rel.id.clone(),
        pmus_market: ps.to_string(),
        pmus_bid: p_bid.to_string(),
        kalshi_market: kt.to_string(),
        kalshi_ask: k_ask.to_string(),
        size,
        edge: cx.emit_6dp(edge_kpm),
        net: cx.emit_6dp(net),
        apr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::model::Level;
    use arb_core::scan::{RelLeg, RelType};

    fn lvl(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    fn rel(id: &str) -> Rel {
        Rel {
            id: id.into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        }
    }

    fn books(k_bid: &str, k_ask: &str, k_ask_sz: &str, p_bid: &str, p_bid_sz: &str, p_ask: &str) -> BookBuilder {
        let mut b = BookBuilder::new();
        b.apply_snapshot(Venue::Kalshi, "K", vec![lvl(k_bid, "50")], vec![lvl(k_ask, k_ask_sz)], 1, 0, None);
        b.apply_snapshot(
            Venue::PolymarketUs,
            "P",
            vec![lvl(p_bid, p_bid_sz)],
            vec![lvl(p_ask, "50")],
            1,
            0,
            None,
        );
        b
    }

    // resolve-date, civil-day and years_to coverage lives with the table in
    // arb_core::resolve — duplicating it here is the drift the move prevents.

    /// The bar itself: cost-weighted average horizon, not a plain mean.
    #[test]
    fn blended_apr_weights_by_cost() {
        let marks = r#"{"positions":[
            {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"},
            {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"}]}"#;
        // 10 profit / 200 cost / 1yr = 5%/yr
        let apr = blended_apr(marks, "2026-07-28").unwrap();
        assert!((apr - 5.0).abs() < 0.05, "expected ~5%/yr, got {apr}");
        assert_eq!(blended_apr(r#"{"positions":[]}"#, "2026-07-28"), None);
    }

    /// Reproduces the Python dry-run line for melenchon captured 2026-07-28:
    ///   edge=+7c net=+5c apr=7%
    /// france-pres-27 resolves 2027-04-25, so ~0.74yr from 2026-07-28.
    #[test]
    fn matches_python_melenchon_line() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        // PM bid 0.50, Kalshi ask 0.43 -> edge 7c, net 5c, cost 0.93
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        let got = detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20).expect("clears a zero bar");
        assert_eq!(got.edge, "0.070000");
        assert_eq!(got.net, "0.050000");
        assert!((got.apr - 7.0).abs() < 0.5, "expected ~7%/yr like Python, got {}", got.apr);
    }

    #[test]
    fn refuses_when_below_the_bar() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        // same crossing, 10%/yr bar -> the 7%/yr APR must not fire
        match detect(&mut cx, &r, &b, "2026-07-28", 10.0, 50, 0, 20) {
            Err(Skip::BelowBar { apr, bar }) => {
                assert!(apr < bar, "{apr} should be under {bar}");
            }
            other => panic!("expected BelowBar, got {other:?}"),
        }
    }

    #[test]
    fn refuses_when_edge_does_not_cover_fees() {
        let mut cx = Cx::default();
        let r = rel("xvus-nobel-peace-26-elonmusk");
        // 2c edge == the 2c fee -> net zero, not tradable. Kalshi bid/ask must
        // be a SANE book (ask strictly above bid) or the crossed-book guard
        // fires first and we would not be testing the fee floor at all.
        let b = books("0.09", "0.10", "20", "0.12", "20", "0.20");
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20),
            Err(Skip::EdgeUnderFees)
        );
    }

    /// The 2026-07-28 fedcut false positive, reconstructed from the tape.
    /// Kalshi's real book was bid 0.1760 / ask 0.1820 and PM-US agreed at
    /// 0.1700 — no crossing existed. A phantom ask at 0.0730 that was added
    /// and never removed made it read as +9.7c / 20%/yr.
    #[test]
    fn refuses_a_crossed_book_rather_than_trading_the_phantom() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");
        // ask 0.0730 sits UNDER the 0.1760 bid — impossible on a live venue
        let b = books("0.1760", "0.0730", "26", "0.1700", "330", "0.1800");
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-07-28", 10.0, 50, 0, 5),
            Err(Skip::CrossedBook { venue: "kalshi" })
        );
        // and with the REAL ask there is simply no trade, as it should be
        let good = books("0.1760", "0.1820", "305", "0.1700", "330", "0.1800");
        assert!(
            detect(&mut cx, &r, &good, "2026-07-28", 10.0, 50, 0, 5).is_err(),
            "true book offers no crossing"
        );
    }

    #[test]
    fn refuses_the_reverse_direction() {
        let mut cx = Cx::default();
        let r = rel("xvus-nobel-peace-26-elonmusk");
        // PM cheap / Kalshi dear: the profitable pairing is PM->K, not auto-traded
        let b = books("0.50", "0.55", "20", "0.10", "20", "0.12");
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20),
            Err(Skip::ReverseDirection)
        );
    }

    #[test]
    fn refuses_an_unknown_family_with_no_resolve_date() {
        let mut cx = Cx::default();
        let r = rel("xvus-mystery-99-somebody");
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20),
            Err(Skip::NoResolveDate)
        );
    }

    /// Size is bounded by the THINNEST of the two levels we cross, the clip,
    /// and remaining per-rel headroom — whichever binds first.
    #[test]
    fn size_takes_the_binding_constraint() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        // depth binds: PM bid only 3 deep
        let b = books("0.40", "0.43", "20", "0.50", "3", "0.55");
        assert_eq!(detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20).unwrap().size, 3);
        // clip binds
        let b2 = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        assert_eq!(detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 0, 5).unwrap().size, 5);
        // headroom binds
        assert_eq!(detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 48, 20).unwrap().size, 2);
        // at cap
        assert_eq!(
            detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 50, 20),
            Err(Skip::AtCap { open: 50 })
        );
    }
}
