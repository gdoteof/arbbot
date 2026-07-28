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
//! Fees were originally the flat `FEE_CT` the Python bar used, on the grounds
//! that this is a port of a POLICY and changing the fee basis would silently
//! move the bar. That was wrong, and the 2026-07-28 audit caught it: both
//! venues charge `k*P*(1-P)`, which summed over the two TAKER legs is
//! ~0.13*P*(1-P) and so EXCEEDS 0.02 for P in (0.19, 0.81) — the band that
//! near-dated at-the-money markets actually trade in. Six of the sixteen
//! take-take baskets ever booked (the xvus-time-poty-26 names, crossed at
//! 0.19/0.36) really paid 2.5-3.2c per contract against a 2c charge. Worse,
//! a 3c crossing on a 16-day fedcut computed to 23.5%/yr, cleared the live
//! 10.0%/yr bar, and would have LOST 0.25c/contract.
//!
//! So the fee now comes from `arb_core::fees` — the same per-venue curves the
//! quoter, the scanner and the dashboard use — and `FEE_CT` survives only as
//! the policy floor beneath it.

use arb_core::book::BookBuilder;
use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::{Cx, Rel, D};

/// Policy FLOOR on the both-leg taker fee per contract — no longer the fee
/// itself. Python's `FEE_CT` was the whole model; `arb_core::fees` is now, and
/// it comes out BELOW this at the tails (0.0064/ct on the 0.028/0.080
/// france-pres crossings this engine actually took).
///
/// Kept as a floor deliberately. Correcting an UNDERSTATED fee is this fix's
/// mandate; loosening what fires at the tails is not, and `max(real, floor)`
/// is the only shape that can never charge less than reality. It also stands
/// in for two costs `detect` genuinely cannot see: PM-US's per-market
/// `feeCoefficient` (the schedule's 0.06 is documented as the observed
/// FALLBACK, and `taker_coef_override` is never populated in this workspace),
/// and a clip that walks past the touch it was sized against.
const FEE_CT: &str = "0.02";

/// The fee curves, built once. A `FeeSchedule` is a constant coefficient
/// table, so one process-wide instance and the engine's own
/// `FeeSchedule::new` are the same object by construction — and `detect` runs
/// on the book-event path, where re-parsing thirteen decimals per
/// relationship per event would be pure waste.
fn schedule() -> &'static FeeSchedule {
    static S: std::sync::OnceLock<FeeSchedule> = std::sync::OnceLock::new();
    S.get_or_init(|| FeeSchedule::new(&mut Cx::default()))
}

/// Both-leg taker fee per contract for a K->PM crossing of `size` contracts:
/// buy Kalshi YES at `k_ask`, sell PM-US YES at `p_bid`. Floored at `FEE_CT`.
///
/// SIZE IS AN INPUT, not a scale factor. Kalshi ceils its fee to the cent per
/// ORDER (`fees.rs`, `quantize_ceil`), so at 0.47 a 5-lot pays 0.0180/ct of
/// Kalshi fee and a 50-lot pays 0.0176 — and it is not even monotone, because
/// what matters is where the ceiling lands. There is no per-contract figure to
/// multiply, which is why this is computed after the clip is known.
fn both_leg_taker_fee(cx: &mut Cx, k_ask: D, p_bid: D, size: i64) -> D {
    let sched = schedule();
    let n = cx.from_i64(size);
    // `category` is read only by Polymarket INTL's curve; Kalshi's and PM-US's
    // ignore it entirely (fees.rs), and take-take only ever touches those two.
    let k = sched.fee(cx, Venue::Kalshi, Role::Taker, k_ask, n, "default");
    let p = sched.fee(cx, Venue::PolymarketUs, Role::Taker, p_bid, n, "default");
    let both = cx.add(k, p);
    let per_ct = cx.div(both, n);
    let floor = cx.parse_exact(FEE_CT);
    if cx.cmp(per_ct, floor) == std::cmp::Ordering::Less { floor } else { per_ct }
}

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
///
/// Every accumulator runs over the SAME set of positions. Python's version (and
/// this port until 2026-07-28) summed profit and cost over ALL positions but
/// weighted the horizon over only the DATED ones, so an undated position — or
/// one with zero cost — put money in the yield without putting time in the
/// denominator. A position carrying cost and no locked profit dragged the bar
/// DOWN, which is the fail-open direction: a low bar fires unprofitable
/// crossings. Inert on the 2026-07-28 marks file (all 27 positions dated,
/// none zero-cost, so both denominators give 10.0088%/yr), latent otherwise.
pub fn blended_apr(marks_json: &str, today_iso: &str) -> Option<f64> {
    let doc: serde_json::Value = serde_json::from_str(marks_json).ok()?;
    let today = parse_iso(today_iso)?;
    let (mut num, mut den, mut prof) = (0.0f64, 0.0f64, 0.0f64);
    for p in doc.get("positions")?.as_array()? {
        let c = p.get("cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let pr = p.get("locked_profit_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let Some(rb) = p.get("resolves_by").and_then(|v| v.as_str()) else { continue };
        if c == 0.0 {
            continue;
        }
        let Some(days) = parse_iso(rb).map(|d| d - today) else { continue };
        let yrs = days.max(1) as f64 / 365.25;
        prof += pr;
        num += c * yrs;
        den += c;
    }
    if den == 0.0 {
        return None;
    }
    let wavg = num / den;
    if wavg == 0.0 {
        return None;
    }
    Some(prof / den / wavg * 100.0)
}

/// How far behind `marks.json` may be before its bar stops being usable.
///
/// `arbbot-marks.timer` rewrites the file every 2 minutes, so 15 minutes is
/// seven missed runs: a slow run or one transient failure survives, a wedged
/// service does not.
pub const MAX_MARKS_AGE_S: f64 = 900.0;

/// The take-take bar, and whether it may be traded against at all.
///
/// Deliberately NOT an `Option<f64>`. On 2026-07-28 the engine read the bar as
/// `read_to_string(..).ok().and_then(blended_apr).unwrap_or(DEFAULT_BAR_APR)`,
/// `marks.json` stopped being written at 12:46:12 — the engine's own first
/// ledger record landed 8 s later carrying no `cost_usd`, and
/// `scripts/mark_positions.py:111` has raised `KeyError` every 2 minutes since
/// — and the armed session then spent four hours firing against a frozen
/// 10.0088%/yr bar without one line of output saying so. The engine broke the
/// input to its own risk decision and could not tell.
///
/// A stale bar is not a missing bar. Collapsing the two is the bug, so the
/// type refuses to let a caller do it.
#[derive(Debug, Clone, PartialEq)]
pub enum Bar {
    /// Marks are recent and priceable. `age_s` is how far behind they were.
    Fresh { apr: f64, age_s: f64 },
    /// Marks exist but their bar cannot be trusted: older than
    /// `MAX_MARKS_AGE_S`, clock-skewed into the future, undated, or corrupt.
    /// Take-take must not fire while this holds — the marks service being
    /// broken is itself a reason not to trade, and the frozen value is wrong
    /// in BOTH directions, so no substitute is defensible.
    Untrusted { why: String },
    /// There is no portfolio to average yet: no marks file, or none of its
    /// positions is priceable. A cold start, not a fault — `DEFAULT_BAR_APR`
    /// is the right answer, and it sits ABOVE every blended value observed so
    /// far, so it declines rather than over-fires.
    NoPortfolio { why: String },
}

impl Bar {
    /// The bar to test crossings against. `None` is a REFUSAL to run take-take
    /// at all, not a missing value: there is no default to substitute, because
    /// substituting one is exactly what went wrong.
    #[must_use]
    pub fn tradable(&self) -> Option<f64> {
        match self {
            Bar::Fresh { apr, .. } => Some(*apr),
            Bar::NoPortfolio { .. } => Some(DEFAULT_BAR_APR),
            Bar::Untrusted { .. } => None,
        }
    }

    /// One line for the operator. A refused bar must be loud: the failure it
    /// reports is silent everywhere else.
    pub fn describe(&self) -> String {
        match self {
            Bar::Fresh { apr, age_s } => format!("bar {apr:.4}%/yr (marks {age_s:.0}s old)"),
            Bar::NoPortfolio { why } => {
                format!("bar {DEFAULT_BAR_APR:.1}%/yr (default — {why})")
            }
            Bar::Untrusted { why } => format!("NO BAR — take-take REFUSED: {why}"),
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` -> epoch seconds. The one shape
/// `scripts/mark_positions.py` writes (`time.strftime(..., time.gmtime())`),
/// pinned strictly so a format change reads as untrusted rather than as a
/// guess. Local to the staleness guard: `arb_core::resolve` is deliberately
/// date-only, and only this needs sub-day resolution.
fn parse_iso8601_z(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    if b.len() != 20 || b[10] != b'T' || b[13] != b':' || b[16] != b':' || b[19] != b'Z' {
        return None;
    }
    let day = parse_iso(s)?;
    let h: i64 = s.get(11..13)?.parse().ok()?;
    let m: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    if h > 23 || m > 59 || sec > 60 {
        return None;
    }
    Some((day * 86400 + h * 3600 + m * 60 + sec) as f64)
}

/// The bar, aged against the marks file's OWN `generated_at` rather than its
/// mtime — mtime can be freshened by a touch or a copy, `generated_at` is what
/// the writer asserts about the data. Pure, so the staleness rule is
/// replayable: the caller supplies the file's text and the clock.
///
/// An empty `marks_json` means "no file" (the caller's read failed), which is
/// a cold start. Text that is present but unparseable means something WROTE
/// garbage, which is a fault — those two must not share an answer.
pub fn bar_from_marks(marks_json: &str, now: f64) -> Bar {
    if marks_json.trim().is_empty() {
        return Bar::NoPortfolio { why: "no marks file".into() };
    }
    let Ok(doc) = serde_json::from_str::<serde_json::Value>(marks_json) else {
        return Bar::Untrusted { why: "marks.json present but not parseable".into() };
    };
    let Some(gen) = doc.get("generated_at").and_then(|v| v.as_str()).and_then(parse_iso8601_z)
    else {
        return Bar::Untrusted { why: "marks.json carries no usable generated_at".into() };
    };
    let age_s = now - gen;
    if age_s.abs() > MAX_MARKS_AGE_S {
        return Bar::Untrusted {
            why: format!("marks.json is {age_s:.0}s off (limit {MAX_MARKS_AGE_S:.0}s)"),
        };
    }
    match blended_apr(marks_json, &today_iso(now)) {
        Some(apr) => Bar::Fresh { apr, age_s },
        None => Bar::NoPortfolio { why: "no priceable position in marks.json".into() },
    }
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
    /// Both-leg taker fee per contract charged against `edge` — the real
    /// per-venue curves at THIS `size`, floored at `FEE_CT`. Reported because
    /// it is no longer a constant a reader can assume.
    pub fee: String,
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

    // No fee can rescue a non-positive gross edge, so bail before doing the
    // work of sizing it.
    if !cx.is_pos(edge_kpm) {
        return Err(Skip::EdgeUnderFees);
    }
    let cost_ct = cx.one_minus(edge_kpm);
    if !cx.is_pos(cost_ct) {
        return Err(Skip::EdgeUnderFees);
    }

    let Some(yrs) = years_to(&rel.id, today_iso) else {
        return Err(Skip::NoResolveDate);
    };

    // SIZE BEFORE FEE. Kalshi ceils its fee to the cent per ORDER, so the
    // per-contract cost is a function of the clip and simply does not exist
    // until the clip does. Sizing is bounded by remaining per-rel headroom,
    // the depth on the two levels we would actually cross, and the clip.
    let headroom = (max_ct_per_rel - open_ct).max(0);
    if headroom < 1 {
        return Err(Skip::AtCap { open: open_ct });
    }
    let depth_k: i64 = k_ask_sz.parse::<f64>().map(|f| f as i64).unwrap_or(0);
    let depth_p: i64 = p_bid_sz.parse::<f64>().map(|f| f as i64).unwrap_or(0);
    let size = headroom.min(depth_k.min(depth_p)).min(max_clip);
    if size < 1 {
        return Err(Skip::NoDepth);
    }

    let fee = both_leg_taker_fee(cx, ka, pb, size);
    let net = cx.sub(edge_kpm, fee);
    if !cx.is_pos(net) {
        return Err(Skip::EdgeUnderFees);
    }

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

    Ok(Candidate {
        rel_id: rel.id.clone(),
        pmus_market: ps.to_string(),
        pmus_bid: p_bid.to_string(),
        kalshi_market: kt.to_string(),
        kalshi_ask: k_ask.to_string(),
        size,
        edge: cx.emit_6dp(edge_kpm),
        fee: cx.emit_6dp(fee),
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

    /// F6: profit, cost and horizon must come from one set of positions.
    ///
    /// The undated position below carries cost and NO locked profit. Weighting
    /// the horizon over the dated position only, while dividing profit by both
    /// costs, halved the yield and reported 2.5%/yr for a book returning 5% —
    /// and a bar that is too LOW fires crossings that lose money.
    #[test]
    fn an_undated_position_cannot_dilute_the_bar() {
        let marks = r#"{"positions":[
            {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"},
            {"cost_usd":100.0,"locked_profit_usd":0.0}]}"#;
        let apr = blended_apr(marks, "2026-07-28").unwrap();
        assert!((apr - 5.0).abs() < 0.05, "expected the dated book's 5%/yr, got {apr}");

        // and a zero-cost position is excluded from both sides the same way:
        // it has no capital to weight and no yield to average.
        let with_zero = r#"{"positions":[
            {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"},
            {"cost_usd":0.0,"locked_profit_usd":9.0,"resolves_by":"2027-07-28"}]}"#;
        let apr = blended_apr(with_zero, "2026-07-28").unwrap();
        assert!((apr - 5.0).abs() < 0.05, "expected 5%/yr, got {apr}");
    }

    /// The exact numbers from the 2026-07-28 armed session: `marks.json` was
    /// last written 12:46:12 local (16:46:12Z) and the engine was still trading
    /// against its 10.0088%/yr bar over four hours later.
    #[test]
    fn a_stale_marks_file_yields_no_tradable_bar() {
        // 2026-07-28T16:46:12Z
        let generated = 1785257172.0;
        let marks = r#"{"generated_at":"2026-07-28T16:46:12Z","positions":[
                {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"}]}"#;

        // fresh: two minutes old, one marks-timer period
        match bar_from_marks(marks, generated + 120.0) {
            Bar::Fresh { apr, age_s } => {
                assert!((apr - 5.0).abs() < 0.05, "got {apr}");
                assert!((age_s - 120.0).abs() < 0.001);
            }
            other => panic!("two minutes old is fresh, got {other:?}"),
        }

        // the real failure: 15210s behind, which is what the armed engine read
        let stale = bar_from_marks(marks, generated + 15210.0);
        assert!(matches!(stale, Bar::Untrusted { .. }), "got {stale:?}");
        assert_eq!(stale.tradable(), None, "a stale bar must refuse, not default");
        assert!(stale.describe().contains("REFUSED"), "{}", stale.describe());

        // the boundary, and one second past it
        assert!(bar_from_marks(marks, generated + MAX_MARKS_AGE_S).tradable().is_some());
        assert!(bar_from_marks(marks, generated + MAX_MARKS_AGE_S + 1.0).tradable().is_none());

        // a clock skewed far into the FUTURE is just as unusable
        assert!(bar_from_marks(marks, generated - MAX_MARKS_AGE_S - 1.0).tradable().is_none());
    }

    /// The three ways there is no bar are not one thing. A missing file is a
    /// cold start and gets the default; a file that exists but cannot be aged
    /// or parsed is a FAULT and gets a refusal.
    #[test]
    fn a_cold_start_defaults_but_a_broken_marks_file_refuses() {
        let now = 1785257172.0;
        assert_eq!(bar_from_marks("", now).tradable(), Some(DEFAULT_BAR_APR));
        assert_eq!(bar_from_marks("   \n", now).tradable(), Some(DEFAULT_BAR_APR));

        // present, timestamped, but nothing priceable yet
        let empty = r#"{"generated_at":"2026-07-28T16:46:12Z","positions":[]}"#;
        assert_eq!(bar_from_marks(empty, now).tradable(), Some(DEFAULT_BAR_APR));

        // torn or half-written
        assert_eq!(bar_from_marks(r#"{"generated_at":"2026-07-2"#, now).tradable(), None);
        // no timestamp at all: unageable, so untrustable
        let undated = r#"{"positions":[
            {"cost_usd":100.0,"locked_profit_usd":5.0,"resolves_by":"2027-07-28"}]}"#;
        assert_eq!(bar_from_marks(undated, now).tradable(), None);
        // a timestamp we do not recognise must not be guessed at
        for bad in ["2026-07-28 16:46:12", "2026-07-28T16:46:12", "2026-07-28T99:46:12Z", ""] {
            let doc = format!(r#"{{"generated_at":"{bad}","positions":[]}}"#);
            assert_eq!(bar_from_marks(&doc, now).tradable(), None, "{bad} must not parse");
        }
    }

    /// `generated_at` is the writer's own statement about the data, and this is
    /// the epoch it maps to — pinned because the whole staleness guard hangs
    /// off it. Value taken from the live file, whose mtime is the same second.
    #[test]
    fn generated_at_parses_to_the_epoch_the_file_was_written() {
        assert_eq!(parse_iso8601_z("2026-07-28T16:46:12Z"), Some(1785257172.0));
        assert_eq!(parse_iso8601_z("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(parse_iso8601_z("2026-07-28T16:46:12"), None, "no Z");
        assert_eq!(parse_iso8601_z("2026-13-28T16:46:12Z"), None, "month 13");
        assert_eq!(parse_iso8601_z("2026-07-28T16:60:12Z"), None, "minute 60");
    }

    /// The melenchon crossing the Python dry-run printed on 2026-07-28 as
    ///   edge=+7c net=+5c apr=7%
    /// — and what it is really worth. Python's flat 2c fee was under half the
    /// truth: at Kalshi ask 0.43 the taker fee is
    /// `ceil_cents(0.07 * 20 * 0.43 * 0.57) = 0.35` (0.0175/ct) and at PM-US
    /// bid 0.50 it is `0.06 * 0.50 * 0.50 * 20 = 0.30` (0.015/ct), so
    /// **0.0325/ct**, not 0.02. Net is 3.75c not 5c, and the APR 5.43%/yr not
    /// 7% — 38% lower, exactly as the audit computed.
    /// france-pres-27 resolves 2027-04-25, so ~0.742yr from 2026-07-28.
    #[test]
    fn melenchon_pays_the_real_both_leg_fee_not_the_flat_two_cents() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        // PM bid 0.50, Kalshi ask 0.43 -> edge 7c, cost 0.93
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        let got = detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20).expect("clears a zero bar");
        assert_eq!(got.edge, "0.070000");
        assert_eq!(got.fee, "0.032500", "0.0175 kalshi + 0.015 pmus, per contract");
        assert_eq!(got.net, "0.037500");
        assert!((got.apr - 5.4346).abs() < 0.01, "expected ~5.43%/yr, got {}", got.apr);
    }

    #[test]
    fn refuses_when_below_the_bar() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        // same crossing, 10%/yr bar -> the 5.43%/yr APR must not fire
        match detect(&mut cx, &r, &b, "2026-07-28", 10.0, 50, 0, 20) {
            Err(Skip::BelowBar { apr, bar }) => {
                assert!(apr < bar, "{apr} should be under {bar}");
            }
            other => panic!("expected BelowBar, got {other:?}"),
        }
    }

    /// C9, the concrete loss the audit priced. A 3c crossing on an at-the-money
    /// fedcut market 16 days from resolution: under the flat 2c fee net reads
    /// +1c and the APR 23.5%/yr, which clears the live 10.0088%/yr bar and
    /// FIRES. The real both-leg fee at 0.49/0.52 is 0.032476/ct, so the trade
    /// is worth -0.25c/contract and must never be sent.
    ///
    /// This family is why it matters: the armed drop-in passed
    /// `--rel-prefix xvus-fedcut-26` AND `--take-take`, and fedcut is
    /// near-dated and at-the-money — the worst corner of the curve.
    #[test]
    fn refuses_the_three_cent_fedcut_crossing_that_loses_money() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");
        // resolves 2026-12-31; from 2026-12-15 that is 16 days
        let b = books("0.48", "0.49", "20", "0.52", "20", "0.53");
        // the live bar the armed session was using
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-12-15", 10.0088, 50, 0, 20),
            Err(Skip::EdgeUnderFees)
        );
        // and it is the FEE that refuses it, not the bar: a zero bar cannot
        // rescue a crossing whose net is negative
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-12-15", 0.0, 50, 0, 20),
            Err(Skip::EdgeUnderFees)
        );
    }

    /// The other half of C9: this must not become a switch that turns take-take
    /// off. The band the engine actually traded is the tails, where the real
    /// curves come out BELOW the 2c policy floor — 0.0064/ct on the
    /// 0.028/0.080 france-pres crossings, 0.0069/ct here — so the floor binds
    /// and behaviour there is unchanged.
    ///
    /// Ten of the sixteen take-take baskets ever booked sit in this
    /// floor-bound region.
    #[test]
    fn a_tail_crossing_still_fires() {
        let mut cx = Cx::default();
        let r = rel("xvus-nobel-peace-26-elonmusk");
        // Kalshi ask 0.03 / PM bid 0.08 -> 5c edge, 73 days to 2026-10-09
        let b = books("0.02", "0.03", "20", "0.08", "20", "0.09");
        let got = detect(&mut cx, &r, &b, "2026-07-28", 10.0088, 50, 0, 20)
            .expect("a 5c tail crossing must still clear the live bar");
        assert_eq!(got.fee, "0.020000", "real fee is 0.006916/ct — the floor binds");
        assert_eq!(got.net, "0.030000");
        assert!((got.apr - 15.799).abs() < 0.01, "got {}", got.apr);
        assert_eq!(got.size, 20);

        // Where the band ENDS at this horizon: 4c clears the 10%/yr bar, 3c
        // does not. Nothing here is switched off, it is priced.
        let four = books("0.02", "0.03", "20", "0.07", "20", "0.09");
        assert!(detect(&mut cx, &r, &four, "2026-07-28", 10.0088, 50, 0, 20).is_ok());
        let three = books("0.02", "0.03", "20", "0.06", "20", "0.09");
        assert!(matches!(
            detect(&mut cx, &r, &three, "2026-07-28", 10.0088, 50, 0, 20),
            Err(Skip::BelowBar { .. })
        ));
    }

    /// Kalshi ceils its fee to the cent PER ORDER, so the per-contract fee is
    /// not linear in the clip and is not even monotone in it — what matters is
    /// where the ceiling lands. At Kalshi ask 0.47:
    ///   clip  5: ceil(0.07*5*0.47*0.53)  = 0.09 -> 0.0180/ct
    ///   clip 20: ceil(0.07*20*0.47*0.53) = 0.35 -> 0.0175/ct
    ///   clip 50: ceil(0.07*50*0.47*0.53) = 0.88 -> 0.0176/ct  <- worse than 20
    /// PM-US does not ceil, so its 0.014946/ct is the same at every clip.
    ///
    /// A per-contract figure assumed linear would have charged all three the
    /// same, which is why the fee is computed after the clip is known.
    #[test]
    fn the_kalshi_ceiling_prices_each_clip_separately() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");
        // 6c edge so every clip clears and a Candidate exists to inspect
        let b = books("0.46", "0.47", "500", "0.53", "500", "0.54");
        let f = |cx: &mut Cx, clip: i64| {
            let got = detect(cx, &r, &b, "2026-12-15", 10.0, 50, 0, clip)
                .unwrap_or_else(|e| panic!("clip {clip} should fire, got {e:?}"));
            assert_eq!(got.size, clip);
            got.fee
        };
        assert_eq!(f(&mut cx, 5), "0.032946", "0.0180 + 0.014946");
        assert_eq!(f(&mut cx, 20), "0.032446", "0.0175 + 0.014946");
        assert_eq!(f(&mut cx, 50), "0.032546", "0.0176 + 0.014946 — dearer than clip 20");
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
