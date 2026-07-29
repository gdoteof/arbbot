//! arb-scenario — what a basket actually costs under each execution style.
//!
//! A cross-venue basket buys YES on leg A and NO on leg B, so it pays $1.00 at
//! resolution whichever way the event goes. The edge is `1.00 - total cost`,
//! and the cost depends on HOW you execute:
//!
//!   take-take    lift A's ask, hit B's bid          — certain fill, worst price
//!   make A       rest a bid on A, take B            — better price, fill risk
//!   make B       take A, rest an ask on B
//!
//! Fees are not a rounding detail here. Kalshi's maker coefficient is 0.0175
//! against 0.07 taker — a 4x difference — and PM-US makers pay nothing at all.
//! So the execution style can flip a basket from unprofitable to profitable
//! without any price moving.
//!
//! SIZE MATTERS, and this is the subtle one: Kalshi ceils its fee to the cent
//! PER ORDER (`fees.rs`, `quantize_ceil(x, 2)`). At clip 1 a 1.3-cent fee
//! becomes 2 cents; at clip 25 the same curve costs 1.32 cents per contract.
//! Quoting per-contract economics without fixing a clip size is meaningless,
//! so callers must pass one.

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Scenario {
    TakeTake,
    MakeATakeB,
    TakeAMakeB,
}

impl Scenario {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scenario::TakeTake => "take-take",
            Scenario::MakeATakeB => "make-a-take-b",
            Scenario::TakeAMakeB => "take-a-make-b",
        }
    }

    pub fn parse(s: &str) -> Option<Scenario> {
        match s {
            "take-take" => Some(Scenario::TakeTake),
            "make-a-take-b" => Some(Scenario::MakeATakeB),
            "take-a-make-b" => Some(Scenario::TakeAMakeB),
            _ => None,
        }
    }

    pub fn all() -> [Scenario; 3] {
        [Scenario::TakeTake, Scenario::MakeATakeB, Scenario::TakeAMakeB]
    }

    fn roles(&self) -> (Role, Role) {
        match self {
            Scenario::TakeTake => (Role::Taker, Role::Taker),
            Scenario::MakeATakeB => (Role::Maker, Role::Taker),
            Scenario::TakeAMakeB => (Role::Taker, Role::Maker),
        }
    }
}

/// Top of book on the YES axis for one leg.
///
/// Sizes are part of the quote, not decoration. A price with no size behind it
/// is not a price you can trade: lifting a 25-lot against an ask good for 3
/// contracts fills 3 at that price and the rest wherever the book goes.
#[derive(Debug, Clone, Default)]
pub struct Quote {
    pub bid: Option<String>,
    pub bid_size: Option<String>,
    pub ask: Option<String>,
    pub ask_size: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Priced {
    pub scenario: &'static str,
    /// YES price paid on leg A.
    pub entry_a: String,
    /// YES price at which leg B is sold (so NO is bought at 1 - this).
    pub exit_b: String,
    /// entry_a + (1 - exit_b), before fees.
    pub gross_cost: String,
    pub fee_a: String,
    pub fee_b: String,
    /// Per contract, fees included.
    pub total_cost: String,
    /// 1.00 - total_cost. Positive is a profitable basket.
    pub edge_per_contract: String,
    pub profitable: bool,
    /// Widest spread across the legs we intend to REST on. `None` for
    /// take-take, which rests nothing.
    pub maker_spread: Option<String>,
    /// Contracts resting at the touch on the legs we would TAKE — the binding
    /// one, i.e. the smaller of the two when both legs are taken. `None` when
    /// the tape carried no size.
    pub take_depth: Option<String>,
    /// Whether this basket is a realistic fill at `clip`. Two independent ways
    /// it is not, and both have burned us:
    ///
    /// 1. MAKING is priced at the far touch, the OPTIMISTIC bound: you capture
    ///    the whole spread. On a tight book that is roughly right. On a wide
    ///    one it is fiction — resting an ask at 0.99 against a 0.40 bid does
    ///    not fill. Observed 2026-07-26: the top five take-a-make-b baskets had
    ///    leg-B spreads of 0.59, 0.48, 0.39, 0.32 and 0.48, showing edges up
    ///    to +0.54.
    /// 2. TAKING was treated as a certain fill at any size. It is not: the
    ///    price is only good for the quantity resting behind it, so a clip
    ///    larger than the touch walks the book and the edge shown was never
    ///    available for the size shown.
    ///
    /// Ranking must not put fiction first, so callers should sort implausible
    /// rows below plausible ones rather than by raw edge.
    pub fill_plausible: bool,
}

/// Price a basket from EXPLICIT entry prices — the prices we are actually
/// resting at, rather than the venue touch.
///
/// The scenario view asks "what if we quoted at the touch"; this asks "what is
/// the basket worth at the prices our engine has actually posted". They differ
/// whenever the quoter improves or backs off the touch, which is most of the
/// time.
#[allow(clippy::too_many_arguments)]
pub fn price_at(
    cx: &mut Cx,
    sched: &FeeSchedule,
    label: &'static str,
    venue_a: Venue,
    venue_b: Venue,
    role_a: Role,
    role_b: Role,
    entry_a: &str,
    exit_b: &str,
    clip: D,
    category: &str,
) -> Option<Priced> {
    let pa: D = cx.parse(entry_a)?;
    let pb: D = cx.parse(exit_b)?;
    let no_cost = cx.one_minus(pb);
    let gross = cx.add(pa, no_cost);

    let fee_a_total = sched.fee(cx, venue_a, role_a, pa, clip, category);
    let fee_b_total = sched.fee(cx, venue_b, role_b, pb, clip, category);
    let fee_a = cx.div(fee_a_total, clip);
    let fee_b = cx.div(fee_b_total, clip);
    let total = {
        let t = cx.add(gross, fee_a);
        cx.add(t, fee_b)
    };
    let one = cx.parse_exact("1");
    let edge = cx.sub(one, total);
    let zero = cx.zero();
    Some(Priced {
        scenario: label,
        entry_a: entry_a.to_string(),
        exit_b: exit_b.to_string(),
        gross_cost: gross.to_string(),
        fee_a: fee_a.to_string(),
        fee_b: fee_b.to_string(),
        total_cost: total.to_string(),
        edge_per_contract: edge.to_string(),
        profitable: cx.cmp(edge, zero) == std::cmp::Ordering::Greater,
        maker_spread: None,
        take_depth: None,
        fill_plausible: true,
    })
}

/// Price one basket under one scenario. `None` when a needed side is missing —
/// making on a leg needs the side you would REST on, taking needs the side you
/// would HIT, and they are different sides.
pub fn price(
    cx: &mut Cx,
    sched: &FeeSchedule,
    scenario: Scenario,
    venue_a: Venue,
    venue_b: Venue,
    qa: &Quote,
    qb: &Quote,
    clip: D,
    category: &str,
    max_maker_spread: D,
) -> Option<Priced> {
    let (role_a, role_b) = scenario.roles();

    // Leg A buys YES: take = lift the ask, make = rest at the bid.
    let entry_a = match role_a {
        Role::Taker => qa.ask.as_ref()?,
        Role::Maker => qa.bid.as_ref()?,
    };
    // Leg B sells YES (i.e. buys NO): take = hit the bid, make = rest at the ask.
    let exit_b = match role_b {
        Role::Taker => qb.bid.as_ref()?,
        Role::Maker => qb.ask.as_ref()?,
    };
    let pa: D = cx.parse(entry_a)?;
    let pb: D = cx.parse(exit_b)?;

    let no_cost = cx.one_minus(pb);
    let gross = cx.add(pa, no_cost);

    // The fee curve is p*(1-p), symmetric, so leg B's fee is the same whether
    // it is expressed on the YES or NO axis. No ambiguity to resolve.
    let fee_a_total = sched.fee(cx, venue_a, role_a, pa, clip, category);
    let fee_b_total = sched.fee(cx, venue_b, role_b, pb, clip, category);
    let fee_a = cx.div(fee_a_total, clip);
    let fee_b = cx.div(fee_b_total, clip);

    let total = {
        let t = cx.add(gross, fee_a);
        cx.add(t, fee_b)
    };
    let one = cx.parse_exact("1");
    let edge = cx.sub(one, total);
    let zero = cx.zero();
    let profitable = cx.cmp(edge, zero) == std::cmp::Ordering::Greater;

    // Spread on each leg we would rest on.
    let mut maker_spread: Option<D> = None;
    let widest = |q: &Quote, cx: &mut Cx, cur: &mut Option<D>| {
        if let (Some(b), Some(ak)) = (q.bid.as_ref(), q.ask.as_ref()) {
            if let (Some(b), Some(ak)) = (cx.parse(b), cx.parse(ak)) {
                let sp = cx.sub(ak, b);
                *cur = Some(match *cur {
                    Some(prev) if cx.cmp(prev, sp) != std::cmp::Ordering::Less => prev,
                    _ => sp,
                });
            }
        }
    };
    if role_a == Role::Maker {
        widest(qa, cx, &mut maker_spread);
    }
    if role_b == Role::Maker {
        widest(qb, cx, &mut maker_spread);
    }
    let spread_ok = match maker_spread {
        None => true, // nothing rested
        Some(sp) => cx.cmp(sp, max_maker_spread) != std::cmp::Ordering::Greater,
    };

    // Size at the touch we would cross, on each leg we TAKE. The binding
    // constraint is the smaller one: a basket needs both fills. A leg whose
    // size the tape did not carry is UNKNOWN, not deep — silence about size is
    // not evidence of size, and letting the other leg's depth stand in for it
    // is how a thin book passes for a fillable one.
    let mut needed: Vec<Option<&String>> = Vec::new();
    if role_a == Role::Taker {
        needed.push(qa.ask_size.as_ref());
    }
    if role_b == Role::Taker {
        needed.push(qb.bid_size.as_ref());
    }
    let mut take_depth: Option<D> = None;
    let mut all_known = true;
    for sz in needed {
        match sz.and_then(|s| cx.parse(s)) {
            Some(d) => {
                take_depth = Some(match take_depth {
                    Some(prev) if cx.cmp(prev, d) != std::cmp::Ordering::Greater => prev,
                    _ => d,
                })
            }
            None => all_known = false,
        }
    }
    let depth_ok = all_known
        && match take_depth {
            Some(d) => cx.cmp(d, clip) != std::cmp::Ordering::Less,
            None => false,
        };
    let fill_plausible = spread_ok && depth_ok;

    Some(Priced {
        maker_spread: maker_spread.map(|d| d.to_string()),
        take_depth: take_depth.map(|d| d.to_string()),
        fill_plausible,
        scenario: scenario.as_str(),
        entry_a: entry_a.clone(),
        exit_b: exit_b.clone(),
        gross_cost: gross.to_string(),
        fee_a: fee_a.to_string(),
        fee_b: fee_b.to_string(),
        total_cost: total.to_string(),
        edge_per_contract: edge.to_string(),
        profitable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A book deep enough that size is not what the test is about.
    fn q(bid: &str, ask: &str) -> Quote {
        qs(bid, "1000", ask, "1000")
    }

    fn qs(bid: &str, bid_size: &str, ask: &str, ask_size: &str) -> Quote {
        Quote {
            bid: Some(bid.into()),
            bid_size: Some(bid_size.into()),
            ask: Some(ask.into()),
            ask_size: Some(ask_size.into()),
        }
    }

    fn setup() -> (Cx, FeeSchedule, D) {
        let mut cx = Cx::default();
        let s = FeeSchedule::new(&mut cx);
        let wide = cx.parse_exact("1");
        (cx, s, wide)
    }

    #[test]
    fn making_beats_taking_on_both_price_and_fee() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        let (qa, qb) = (q("0.20", "0.22"), q("0.80", "0.83"));

        let tt = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                       Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).unwrap();
        let ma = price(&mut cx, &sched, Scenario::MakeATakeB, Venue::Kalshi,
                       Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).unwrap();

        // take-take lifts A's ask (0.22); making A rests at the bid (0.20)
        assert_eq!(tt.entry_a, "0.22");
        assert_eq!(ma.entry_a, "0.20");
        let (t, m) = (cx.parse_exact(&tt.total_cost), cx.parse_exact(&ma.total_cost));
        assert_eq!(cx.cmp(m, t), std::cmp::Ordering::Less, "maker must cost less");
        // and the fee itself must drop: kalshi maker coef is 0.0175 vs 0.07
        let (fa_t, fa_m) = (cx.parse_exact(&tt.fee_a), cx.parse_exact(&ma.fee_a));
        assert_eq!(cx.cmp(fa_m, fa_t), std::cmp::Ordering::Less, "maker fee must be lower");
    }

    #[test]
    fn pmus_maker_is_free_so_making_leg_b_removes_its_fee_entirely() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        let (qa, qb) = (q("0.20", "0.22"), q("0.80", "0.83"));
        let r = price(&mut cx, &sched, Scenario::TakeAMakeB, Venue::Kalshi,
                      Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).unwrap();
        assert_eq!(cx.parse_exact(&r.fee_b).to_string(), "0", "pmus maker pays nothing");
        // making B rests at B's ask (0.83), so NO costs 1 - 0.83 = 0.17
        assert_eq!(r.exit_b, "0.83");
    }

    /// The trap: Kalshi ceils its fee to the cent PER ORDER, so per-contract
    /// economics change with clip size even though no price moved.
    #[test]
    fn kalshi_fee_ceiling_makes_small_clips_much_worse_per_contract() {
        let (mut cx, sched, wide) = setup();
        let (qa, qb) = (q("0.20", "0.22"), q("0.80", "0.83"));
        let one = cx.parse_exact("1");
        let twenty_five = cx.parse_exact("25");
        let small = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                          Venue::PolymarketUs, &qa, &qb, one, "politics", wide).unwrap();
        let big = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                        Venue::PolymarketUs, &qa, &qb, twenty_five, "politics", wide).unwrap();
        let (fs, fb) = (cx.parse_exact(&small.fee_a), cx.parse_exact(&big.fee_a));
        assert_eq!(cx.cmp(fs, fb), std::cmp::Ordering::Greater,
                   "clip 1 must cost MORE per contract than clip 25");
    }

    /// A wide book makes the far-touch maker price fiction; the row must say so.
    #[test]
    fn a_wide_maker_spread_is_flagged_implausible() {
        let (mut cx, sched, _wide) = setup();
        let clip = cx.parse_exact("25");
        let tight = cx.parse_exact("0.05");
        let qa = q("0.42", "0.43");
        // leg B bid 0.40 / ask 0.99 -> resting at 0.99 will never fill
        let qb = q("0.40", "0.99");
        let r = price(&mut cx, &sched, Scenario::TakeAMakeB, Venue::Kalshi,
                      Venue::PolymarketUs, &qa, &qb, clip, "politics", tight).unwrap();
        assert_eq!(r.maker_spread.as_deref(), Some("0.59"));
        assert!(!r.fill_plausible, "0.59 spread must not read as a fillable rest");
        assert!(r.profitable, "and it still LOOKS profitable, which is the trap");

        // take-take rests nothing, so the spread cannot disqualify it
        let tt = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                       Venue::PolymarketUs, &qa, &qb, clip, "politics", tight).unwrap();
        assert!(tt.maker_spread.is_none());
        assert!(tt.fill_plausible);

        // a tight book makes the same maker scenario plausible
        let qb2 = q("0.80", "0.83");
        let ok = price(&mut cx, &sched, Scenario::TakeAMakeB, Venue::Kalshi,
                       Venue::PolymarketUs, &qa, &qb2, clip, "politics", tight).unwrap();
        assert!(ok.fill_plausible);
    }

    /// Our posted price is not the touch, so the two must be able to differ.
    #[test]
    fn price_at_uses_our_posted_price_not_the_touch() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        let (qa, qb) = (q("0.20", "0.22"), q("0.80", "0.83"));
        // at-touch maker on A rests at 0.20
        let touch = price(&mut cx, &sched, Scenario::MakeATakeB, Venue::Kalshi,
                          Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).unwrap();
        assert_eq!(touch.entry_a, "0.20");
        // our engine actually posted 0.18 — better basket, same fee role
        let ours = price_at(&mut cx, &sched, "make-a-take-b", Venue::Kalshi,
                            Venue::PolymarketUs, Role::Maker, Role::Taker,
                            "0.18", "0.80", clip, "politics").unwrap();
        assert_eq!(ours.entry_a, "0.18");
        let (t, o) = (cx.parse_exact(&touch.edge_per_contract),
                      cx.parse_exact(&ours.edge_per_contract));
        assert_eq!(cx.cmp(o, t), std::cmp::Ordering::Greater,
                   "posting inside the touch must show a better edge");
    }

    /// Regression: ISSUE-002 — take-take claimed a certain fill at any size.
    /// Found by /qa on 2026-07-27.
    /// Report: .gstack/qa-reports/qa-report-arb-dash-2026-07-27.md
    ///
    /// The tape carries bid_size/ask_size and the pricer ignored them, so a
    /// 25-lot "crossing" against an ask good for 3 contracts ranked top of the
    /// board as actionable.
    #[test]
    fn a_thin_touch_cannot_fill_the_clip() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        // prices identical to the deep case; only the size behind them differs
        let thin_a = qs("0.20", "1000", "0.22", "3");
        let deep_b = q("0.80", "0.83");

        let tt = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                       Venue::PolymarketUs, &thin_a, &deep_b, clip, "politics", wide).unwrap();
        assert_eq!(tt.take_depth.as_deref(), Some("3"), "the binding leg is A's 3");
        assert!(tt.profitable, "and it still LOOKS profitable, which is the trap");
        assert!(!tt.fill_plausible, "3 contracts cannot fill a 25 clip");

        // resting on A instead never crosses it, so A's thin ask is irrelevant
        let ma = price(&mut cx, &sched, Scenario::MakeATakeB, Venue::Kalshi,
                       Venue::PolymarketUs, &thin_a, &deep_b, clip, "politics", wide).unwrap();
        assert_eq!(ma.take_depth.as_deref(), Some("1000"), "only leg B is taken");
        assert!(ma.fill_plausible);

        // exactly the clip is enough; one short is not
        let exact = qs("0.20", "1000", "0.22", "25");
        let short = qs("0.20", "1000", "0.22", "24");
        assert!(price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi, Venue::PolymarketUs,
                      &exact, &deep_b, clip, "politics", wide).unwrap().fill_plausible);
        assert!(!price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi, Venue::PolymarketUs,
                       &short, &deep_b, clip, "politics", wide).unwrap().fill_plausible);
    }

    /// A tape with no size cannot be used to assert a fill.
    #[test]
    fn unknown_depth_is_not_a_pass() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        let sizeless = Quote { bid: Some("0.20".into()), bid_size: None,
                               ask: Some("0.22".into()), ask_size: None };
        // leg B IS deep — the missing size on A must not be papered over by it
        let r = price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                      Venue::PolymarketUs, &sizeless, &q("0.80", "0.83"), clip,
                      "politics", wide).unwrap();
        assert!(!r.fill_plausible, "silence about size is not evidence of size");
    }

    #[test]
    fn a_missing_side_yields_no_price_rather_than_a_guess() {
        let (mut cx, sched, wide) = setup();
        let clip = cx.parse_exact("25");
        // no ask on A: cannot TAKE leg A
        let qa = Quote { bid: Some("0.20".into()), bid_size: Some("1000".into()),
                         ask: None, ask_size: None };
        let qb = q("0.80", "0.83");
        assert!(price(&mut cx, &sched, Scenario::TakeTake, Venue::Kalshi,
                      Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).is_none());
        // but making A only needs the bid
        assert!(price(&mut cx, &sched, Scenario::MakeATakeB, Venue::Kalshi,
                      Venue::PolymarketUs, &qa, &qb, clip, "politics", wide).is_some());
    }
}
