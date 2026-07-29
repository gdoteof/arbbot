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
use arb_core::model::{BookSide, Venue};
use arb_core::scan::{feasible_min_payoff, Cx, Rel, Side, D};

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
    /// The PM-US leg: sell YES at this bid, which is how a NO is opened.
    pub pmus_market: String,
    pub pmus_bid: String,
    /// The Kalshi leg: buy YES at this ask.
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
    /// Which venue LEG 1 goes to: the touch likeliest to VANISH before we
    /// reach it. Only leg 1 is placed — its fill mints the other leg through
    /// `hedge_anchor` — so a leg-1 miss costs nothing, while a leg-2 miss is a
    /// naked leg.
    ///
    /// Leg 1 was PM-US for as long as take-take has existed, described as "the
    /// CONSTRAINED leg". That phrase comes from `scripts/execute_xv.py`, and
    /// there it meant DEPTH and nothing else: "fill the CONSTRAINED leg (PM US
    /// bid, thinner) FIRST via IOC; then buy EXACTLY the filled quantity on
    /// Kalshi (deep, stable) -> bounds leg risk". It was an observation about
    /// which book was thin in July 2026, not a venue rule — there is no order
    /// semantics, fee, settlement or geoblock reason PM-US must lead, and the
    /// hedge pipeline derives either direction from `rel.legs` alone.
    ///
    /// 2026-07-29 00:53:50 is that observation failing. Kalshi's ask on
    /// KXRATECUT-26DEC31 had been 0.1300 x 4 for seventeen seconds when a ONE
    /// lot appeared at 0.1080. `detect` fired on it (edge 0.062, net 0.042,
    /// 11%/yr against a 10.06% bar), leg 1 sold instantly into a PM-US bid
    /// **476** deep, and 159 ms later the 0.1080 was gone — PULLED, not taken:
    /// no trade printed there and the ask was back at 0.1300 two hundred
    /// seconds later. We had taken the ROBUST leg first and were left chasing
    /// the fragile one, 1 contract short on PM-US and unhedged, the hedge
    /// correctly refusing to chase 1.2c past its 1c slip budget.
    ///
    /// Speed cannot fix that — `exec_hop_latency` is p50 100.7ms / p90
    /// 805.3ms before a packet reaches a venue — so ORDER is the fix.
    pub lead: Venue,
}

impl Candidate {
    /// Leg 1 as an order: venue, market, price, and the ORDER side to send.
    ///
    /// A Kalshi lead BUYS YES at the ask, which is a BID-side order; a PM-US
    /// lead SELLS YES at the bid, which is an ASK-side order (`arb_venue::wire`
    /// turns that into `ORDER_INTENT_BUY_SHORT`, i.e. opens NO). Pairing a
    /// price with the wrong side buys the leg it meant to sell at a price
    /// chosen for the other side of the book, which is the exact failure
    /// `intent_actions` records — so the pairing is written once, here, and
    /// tested, rather than inlined at a fire site no unarmed run can reach.
    ///
    /// The `else` is PM-US because `detect` mints no other lead; it is also
    /// today's behaviour, so an unreachable third venue degrades to what leg 1
    /// has always been.
    pub fn leg1(&self) -> (Venue, &str, &str, BookSide) {
        if self.lead == Venue::Kalshi {
            (Venue::Kalshi, &self.kalshi_market, &self.kalshi_ask, BookSide::Bid)
        } else {
            (Venue::PolymarketUs, &self.pmus_market, &self.pmus_bid, BookSide::Ask)
        }
    }
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
    /// The net edge does not survive the slip the HEDGE is allowed to give
    /// away. Distinct from `EdgeUnderFees`: this crossing is profitable at the
    /// prices on screen and stops being profitable at prices leg 2 may legally
    /// fill at.
    NetUnderSlip { net: String, slip: String },
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

/// Does the K->PM pairing pay AT LEAST its $1 back in every state this
/// relationship allows? A `false` here is not a price problem: no price makes
/// this pairing an arb, so `detect` must never be called on it.
///
/// `detect` picks its two legs by VENUE and every line of it assumes
/// YES(kalshi) is the same claim as YES(pmus) — true of an equivalence and of
/// nothing else. The take path never read `rel.rtype`; the MAKER path on the
/// same `Rel` has refused on exactly this since scan.rs was written, and this
/// is that check with the sides fixed by DIRECTION (we buy the Kalshi leg's
/// YES and sell the PM-US leg's) instead of by which leg rests.
///
/// On a 2-leg `implies` whose ANTECEDENT is the Kalshi leg the minimum is 0:
/// the state "consequent happens, antecedent does not" pays our long leg
/// nothing while the short leg owes $1, so a basket logged as riskless is
/// -$1.00/contract there, ledgered open+hedged and re-seeded as such on the
/// next restart. Direction is the whole question and only the payoff model
/// knows it — the same `implies` with the antecedent on PM-US buys the
/// consequent and sells the antecedent, pays at least $1 in every state, and
/// must still fire. A type blacklist would have refused a real arb.
///
/// ASKED ONCE PER RELATIONSHIP, at engine construction, because the answer is
/// a registry fact that cannot change while the process runs — and
/// `feasible_states` allocates a fresh `Vec<Vec<i64>>` per call, which is the
/// same reason `schedule()` above is a `OnceLock` rather than a re-parse per
/// book event.
pub fn pairing_pays(rel: &Rel) -> bool {
    let sides: Vec<Side> = rel
        .legs
        .iter()
        .map(|l| if l.venue == Venue::Kalshi { Side::Yes } else { Side::No })
        .collect();
    feasible_min_payoff(rel.rtype, &sides) >= 1
}

/// Test one relationship against the live books. Pure: no I/O, no clock — the
/// caller supplies `today_iso`, the bar and the hedge's slip budget, which is
/// what makes this replayable through the WAL harness.
///
/// PRECONDITION: `pairing_pays(rel)`. Everything below reads the two legs as
/// two spellings of one claim, which is what that answers, and it is answered
/// once at load rather than per book event.
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
    max_slip: &str,
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
    // WHICH LEG LEADS (see `Candidate::lead`). The thinner cushion behind
    // `size` is the leg likeliest to be gone when we get there, and it is the
    // one to risk on. Both coverage ratios are over the SAME `size`, so
    // comparing `depth_k / size` against `depth_p / size` is comparing the two
    // depths: one comparison of two integers already in hand, no clock read
    // and no lookup on the decision path. Ties keep PM-US, which is what leg 1
    // has always been.
    let lead = if depth_k < depth_p { Venue::Kalshi } else { Venue::PolymarketUs };

    let fee = both_leg_taker_fee(cx, ka, pb, size);
    let net = cx.sub(edge_kpm, fee);
    if !cx.is_pos(net) {
        return Err(Skip::EdgeUnderFees);
    }
    // AND THE EDGE MUST OUTLAST THE HEDGE'S OWN ALLOWANCE. `net > 0` was the
    // only magnitude test here, but only leg 1 is placed at the price it was
    // detected at; leg 2 is a hedge, and `hedge_price_acceptable` will fill it
    // up to `max_slip` worse than the anchor — boundary INCLUDED. So the worst
    // in-policy outcome of firing is `net - max_slip` per contract, and
    // everything in `0 < net <= max_slip` is a trade the engine books as
    // riskless and its own hedge policy is licensed to turn negative.
    //
    // Live shape at the default 0.01: a 10-day fedcut crossing at 0.49/0.526
    // has edge 0.036 and both-leg fee 0.032459, so net 0.003541 -> 13.42%/yr,
    // clears a ~10%/yr blended bar, FIRES. Depths tie at 20, so leg 1 is PM-US
    // and lands on the PM-US bid — it cannot touch the Kalshi ask. What moves
    // that ask is a THIRD PARTY lifting the 20-lot at 0.49 inside the window
    // between our leg-1 fill and leg 2 arriving (`exec_hop_latency` is p50
    // 100.7ms / p90 805.3ms), reprinting it at 0.50: exactly 1c, exactly
    // acceptable, and realised net is 0.026 - 0.032459 = -0.006459/ct. Leg 2's
    // own impact cannot be the source of leg 2's slip — by then it is already
    // committed. At that horizon the bar only demands net >= ~0.0027, so the
    // allowance was up to 2.8x the entire edge.
    //
    // The rule is `net > max_slip`, strictly: it is the weakest rule that makes
    // the worst in-policy fill still profitable, and it is what makes hedge.rs's
    // "max_slip is exactly how much of that edge we will surrender" true rather
    // than aspirational. Two honest caveats about "weakest" and "sufficient":
    //
    //   * IT COMPOUNDS WITH `FEE_CT`, which is a floor on this very `net`, so
    //     the test really is `edge - max(real_fee, 0.02) > max_slip` while the
    //     rule argued for is `edge - real_fee > max_slip`. On the tail
    //     crossings this engine actually fires that gap is larger than the
    //     budget: nobel-peace-26-donaldtrump x5 at 0.04/0.08 pays a real
    //     0.008416/ct (Kalshi ceil(0.07*5*0.04*0.96)=0.02 -> 0.004, PM-US
    //     0.06*0.08*0.92 = 0.004416), so reported net 0.020000 understates true
    //     net 0.031584 by 0.011584 — and the effective requirement in true-net
    //     terms becomes 0.021584, i.e. 2.16x the nominal budget. Deliberate but
    //     not free: the floor stands in for PM-US's per-market `feeCoefficient`
    //     and for a clip that walks past its touch (see FEE_CT), and ten of the
    //     sixteen baskets ever booked sit in that floor-bound region. This is
    //     the margin, and it is why no second one is added on top.
    //   * IT IS SUFFICIENT ONLY TO FIRST ORDER. `fee` is priced at the anchor,
    //     and k*P*(1-P) rises toward P=0.5, so a slipped fill can cost up to
    //     ~0.00025/ct more than charged here. Two orders of magnitude under the
    //     floor's own cushion, but it is not zero.
    //
    // Not re-sized per basket from the edge either: that would push `max_slip`
    // down into per-obligation state on the money path, and its failure mode is
    // worse than this one's — a tighter budget makes the hedge WAIT, and
    // waiting is naked, whereas refusing to fire costs only a trade we never
    // had.
    let slip = cx.parse_exact(max_slip);
    if cx.cmp(net, slip) != std::cmp::Ordering::Greater {
        return Err(Skip::NetUnderSlip { net: cx.emit_6dp(net), slip: max_slip.to_string() });
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
        lead,
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
        rel_typed(id, RelType::CrossVenueEquivalent, Venue::Kalshi)
    }

    /// `first` is the venue of LEG 0, which is what fixes the direction of an
    /// ordered type: `implies` and `date-ladder` read leg 0 as the antecedent.
    fn rel_typed(id: &str, rtype: RelType, first: Venue) -> Rel {
        let k = RelLeg { venue: Venue::Kalshi, market_id: "K".into() };
        let p = RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() };
        Rel {
            id: id.into(),
            rtype,
            tranche: "head".into(),
            legs: if first == Venue::Kalshi { vec![k, p] } else { vec![p, k] },
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
        let got =
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20, "0.01").expect("clears a zero bar");
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
        match detect(&mut cx, &r, &b, "2026-07-28", 10.0, 50, 0, 20, "0.01") {
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
            detect(&mut cx, &r, &b, "2026-12-15", 10.0088, 50, 0, 20, "0.01"),
            Err(Skip::EdgeUnderFees)
        );
        // and it is the FEE that refuses it, not the bar: a zero bar cannot
        // rescue a crossing whose net is negative
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-12-15", 0.0, 50, 0, 20, "0.01"),
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
        let got = detect(&mut cx, &r, &b, "2026-07-28", 10.0088, 50, 0, 20, "0.01")
            .expect("a 5c tail crossing must still clear the live bar");
        assert_eq!(got.fee, "0.020000", "real fee is 0.006916/ct — the floor binds");
        assert_eq!(got.net, "0.030000");
        assert!((got.apr - 15.799).abs() < 0.01, "got {}", got.apr);
        assert_eq!(got.size, 20);

        // Where the band ENDS at this horizon: 4c clears the 10%/yr bar, 3c
        // does not. Nothing here is switched off, it is priced.
        let four = books("0.02", "0.03", "20", "0.07", "20", "0.09");
        assert!(detect(&mut cx, &r, &four, "2026-07-28", 10.0088, 50, 0, 20, "0.01").is_ok());
        let three = books("0.02", "0.03", "20", "0.06", "20", "0.09");
        // A 3c edge against the 2c floor is net EXACTLY the 1c slip budget, so
        // the hedge could give the whole thing away and this is now refused one
        // step before the bar gets to see it (it was `BelowBar` when `net > 0`
        // was the only magnitude test). Same verdict, truer reason.
        assert!(matches!(
            detect(&mut cx, &r, &three, "2026-07-28", 10.0088, 50, 0, 20, "0.01"),
            Err(Skip::NetUnderSlip { .. })
        ));
        // ...and with no budget to lose it is the BAR that refuses it, exactly
        // as before: the new gate adds a floor, it does not move the bar.
        assert!(matches!(
            detect(&mut cx, &r, &three, "2026-07-28", 10.0088, 50, 0, 20, "0"),
            Err(Skip::BelowBar { .. })
        ));
    }

    /// A crossing whose ENTIRE net edge fits inside the hedge's slip budget.
    ///
    /// 10 days out from 2026-12-31, Kalshi ask 0.49 / PM-US bid 0.526, clip 20:
    ///   edge 0.036
    ///   fee  ceil_cents(0.07*20*0.49*0.51) = 0.35    -> 0.017500/ct
    ///        + 0.06*0.526*0.474                      -> 0.014959/ct
    ///                                                 = 0.032459/ct
    ///   net  0.003541 -> 0.003541/0.964/(10/365.25) = 13.42%/yr
    /// which clears the ~10%/yr blended bar and fires. Leg 1 takes PM-US; our
    /// own take clears the 0.49 ask, it reprints at 0.50 — exactly the 1c
    /// budget, which `hedge_price_acceptable` ACCEPTS — and the basket realises
    /// 0.026 - 0.032459 = -0.006459/ct. A +$0.07 basket logged as riskless
    /// settles as a -$0.13 loss, on a leg-2 move the engine itself licensed.
    ///
    /// At this horizon the bar only demands net >= ~0.0027, so the whole band
    /// 0.0027..0.01 was fireable and negative-EV: the allowance could be 2.8x
    /// the edge. This is the band the gate closes.
    #[test]
    fn refuses_a_crossing_the_hedge_is_licensed_to_erase() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");
        let b = books("0.48", "0.49", "20", "0.526", "20", "0.53");

        // what it is worth on screen — and that it does clear the bar, so the
        // refusal below is the slip gate and nothing else
        let priced = detect(&mut cx, &r, &b, "2026-12-21", 10.0, 50, 0, 20, "0")
            .expect("with no budget to lose, this is a 13.4%/yr crossing");
        assert_eq!(priced.edge, "0.036000");
        assert_eq!(priced.fee, "0.032459", "0.0175 kalshi + 0.014959 pmus");
        assert_eq!(priced.net, "0.003541");
        assert!((priced.apr - 13.4165).abs() < 0.01, "got {}", priced.apr);

        // at the live budget the hedge may give away 0.01 of a 0.003541 edge
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-12-21", 10.0, 50, 0, 20, "0.01"),
            Err(Skip::NetUnderSlip { net: "0.003541".into(), slip: "0.01".into() })
        );

        // The boundary is refused too, because `hedge_price_acceptable` accepts
        // ITS boundary: at net == slip exactly, the worst in-policy fill nets
        // zero. Net here is 0.036 - 0.03245944 exactly, so this is the real
        // knife edge and not a rounded one.
        assert!(matches!(
            detect(&mut cx, &r, &b, "2026-12-21", 10.0, 50, 0, 20, "0.00354056"),
            Err(Skip::NetUnderSlip { .. })
        ));
        assert!(detect(&mut cx, &r, &b, "2026-12-21", 10.0, 50, 0, 20, "0.00354055").is_ok());

        // ...and this is not a switch that turns take-take off. The crossings
        // the engine actually traded clear the budget several times over: the
        // 4.2c fedcut of 2026-07-29 is 4.2x it, and melenchon 3.75x.
        let fired = books("0.0830", "0.1080", "1", "0.1700", "476", "0.1800");
        let got = detect(&mut cx, &r, &fired, "2026-07-29", 10.0, 50, 0, 20, "0.01")
            .expect("a 4.2c net crossing is not near the 1c budget");
        assert_eq!(got.net, "0.042000");
    }

    /// DEFECT B: `detect` picked its legs by VENUE and assumed YES(kalshi) was
    /// the same claim as YES(pmus). That is true of an equivalence and of
    /// nothing else, and the human-vetting gate says nothing about type.
    ///
    /// On a 2-leg `implies` whose ANTECEDENT is the Kalshi leg, take-take buys
    /// the antecedent and sells the consequent: the state "B happens, A does
    /// not" pays our long leg nothing while the short leg owes $1, so the
    /// "riskless" basket is -$1.00/contract there — booked open+hedged and
    /// re-seeded as such on the next restart.
    ///
    /// Latent, deliberately fixed anyway. The registry's one cross-venue
    /// `implies` (`xv-hantavirus-implies-any-pandemic`) pairs Kalshi with
    /// polymarket INTL, so it has no PM-US leg and stops at `NoBook`; its
    /// antecedent is the PM leg in any case, which is the SAFE direction. But
    /// it holds 15 two-leg `implies` and 17 two-leg `exclusive` rels already,
    /// and one of those vetted cross-venue is all it takes.
    ///
    /// The guard is `feasible_min_payoff`, the maker path's own model
    /// (scan.rs:648), not a type blacklist — because direction decides, and a
    /// blacklist would refuse the half of `implies` that is a real arb.
    #[test]
    fn refuses_a_pairing_with_a_state_that_pays_nothing() {
        let mut cx = Cx::default();
        let id = "xvus-nobel-peace-26-elonmusk";
        let b = books("0.02", "0.03", "20", "0.08", "20", "0.09");

        // control: the equivalence this crossing has always been, unchanged —
        // and it still fires, which is what makes the refusals below mean
        // something.
        let eq = rel(id);
        assert!(pairing_pays(&eq));
        assert!(detect(&mut cx, &eq, &b, "2026-07-28", 10.0088, 50, 0, 20, "0.01").is_ok());

        // same legs, same venues — only `rtype` differs
        assert!(!pairing_pays(&rel_typed(id, RelType::Implies, Venue::Kalshi)));

        // ...and it is the PAYOFF that refuses, not the word `implies`: put the
        // antecedent on PM-US and the same trade buys the consequent and sells
        // the antecedent, which pays at least $1 in every feasible state.
        let safe = rel_typed(id, RelType::Implies, Venue::PolymarketUs);
        assert!(pairing_pays(&safe), "the safe direction of an implication is a genuine arb");
        assert!(detect(&mut cx, &safe, &b, "2026-07-28", 10.0088, 50, 0, 20, "0.01").is_ok());

        // `exclusive` has a state where BOTH legs lose, which pays nothing
        // whichever leg we are long — so both orders are refused.
        for first in [Venue::Kalshi, Venue::PolymarketUs] {
            assert!(!pairing_pays(&rel_typed(id, RelType::Exclusive, first)), "{first:?} first");
        }
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
            let got = detect(cx, &r, &b, "2026-12-15", 10.0, 50, 0, clip, "0.01")
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
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20, "0.01"),
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
            detect(&mut cx, &r, &b, "2026-07-28", 10.0, 50, 0, 5, "0.01"),
            Err(Skip::CrossedBook { venue: "kalshi" })
        );
        // and with the REAL ask there is simply no trade, as it should be
        let good = books("0.1760", "0.1820", "305", "0.1700", "330", "0.1800");
        assert!(
            detect(&mut cx, &r, &good, "2026-07-28", 10.0, 50, 0, 5, "0.01").is_err(),
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
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20, "0.01"),
            Err(Skip::ReverseDirection)
        );
    }

    #[test]
    fn refuses_an_unknown_family_with_no_resolve_date() {
        let mut cx = Cx::default();
        let r = rel("xvus-mystery-99-somebody");
        let b = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        assert_eq!(
            detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20, "0.01"),
            Err(Skip::NoResolveDate)
        );
    }

    /// THE INCIDENT, 2026-07-29 00:53:50, reconstructed from
    /// `data/raw/kalshi-2026-07-29.jsonl` and
    /// `data/raw/polymarket_us-2026-07-29.jsonl`.
    ///
    /// KXRATECUT-26DEC31 sat bid 0.0830 x 143.75 / ask 0.1300 x 4 for
    /// seventeen seconds. At 00:53:50.535 a ONE lot appeared at 0.1080 and
    /// this detector fired on it — edge 0.062, net 0.042, 10.55%/yr against
    /// the session's 10.0585%/yr bar — while PM-US `cbpac-usfed-2026-cut` was
    /// bid 0.1700 x **476**.
    ///
    /// Leg 1 went to PM-US and sold into that 476-deep bid instantly. At
    /// 00:53:50.694, 159 ms later, the 0.1080 was gone: PULLED, not taken —
    /// no trade printed at 0.1080 and the ask was back at 0.1300 x 4. One
    /// contract short on PM-US, unhedged.
    ///
    /// The 1-lot covers `size` exactly and nothing else, so it is the leg to
    /// risk: leg 1 goes to KALSHI, and a miss there leaves no position at all.
    #[test]
    fn the_pulled_one_lot_makes_kalshi_the_leading_leg() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");
        let b = books("0.0830", "0.1080", "1", "0.1700", "476", "0.1800");
        // the bar the armed session was really running against
        let got = detect(&mut cx, &r, &b, "2026-07-29", 10.058485370878834, 50, 0, 20, "0.01")
            .expect("the crossing that fired must still fire");
        assert_eq!(got.size, 1, "the 1-lot ask is what bounds it");
        assert_eq!(got.edge, "0.062000");
        assert_eq!(got.fee, "0.020000", "real fee 0.018466/ct — the floor binds");
        assert_eq!(got.net, "0.042000");
        assert!((got.apr - 10.5497).abs() < 0.01, "got {}", got.apr);

        assert_eq!(got.lead, Venue::Kalshi, "the 1-lot is the leg that vanishes");
        assert_eq!(
            got.leg1(),
            (Venue::Kalshi, "K", "0.1080", BookSide::Bid),
            "leg 1 BUYS Kalshi YES at the ask, which is a bid-side order"
        );
    }

    /// ...and the rule is a comparison, not a new hardcoded venue. When PM-US
    /// is the thin side — which is what `scripts/execute_xv.py` observed when
    /// it called PM-US "the CONSTRAINED leg (PM US bid, thinner)" — leg 1 is
    /// PM-US exactly as before, and so is a tie.
    #[test]
    fn the_thinner_touch_leads_and_a_tie_keeps_pm_us() {
        let mut cx = Cx::default();
        let r = rel("xvus-fedcut-26-usfed-2026-cut");

        // PM-US thin, Kalshi deep: the July-2026 shape.
        let pm_thin = books("0.0830", "0.1080", "476", "0.1700", "1", "0.1800");
        let got =
            detect(&mut cx, &r, &pm_thin, "2026-07-29", 10.0, 50, 0, 20, "0.01").expect("fires");
        assert_eq!(got.lead, Venue::PolymarketUs);
        assert_eq!(
            got.leg1(),
            (Venue::PolymarketUs, "P", "0.1700", BookSide::Ask),
            "leg 1 SELLS PM-US YES at the bid, which is an ask-side order"
        );

        // Equally covered: no reason to move, so nothing moves.
        let tie = books("0.0830", "0.1080", "20", "0.1700", "20", "0.1800");
        let got = detect(&mut cx, &r, &tie, "2026-07-29", 10.0, 50, 0, 20, "0.01").expect("fires");
        assert_eq!(got.lead, Venue::PolymarketUs, "a tie keeps today's behaviour");

        // Coverage, not raw depth, is the question — but both ratios divide by
        // the same `size`, so the clip that binds both cannot reorder them.
        let clipped =
            detect(&mut cx, &r, &pm_thin, "2026-07-29", 10.0, 50, 0, 1, "0.01").expect("fires");
        assert_eq!(clipped.size, 1);
        assert_eq!(clipped.lead, Venue::PolymarketUs, "still the thinner touch");
    }

    /// Size is bounded by the THINNEST of the two levels we cross, the clip,
    /// and remaining per-rel headroom — whichever binds first.
    #[test]
    fn size_takes_the_binding_constraint() {
        let mut cx = Cx::default();
        let r = rel("xvus-france-pres-27-jeanlucmelenchon");
        // depth binds: PM bid only 3 deep
        let b = books("0.40", "0.43", "20", "0.50", "3", "0.55");
        assert_eq!(detect(&mut cx, &r, &b, "2026-07-28", 0.0, 50, 0, 20, "0.01").unwrap().size, 3);
        // clip binds
        let b2 = books("0.40", "0.43", "20", "0.50", "20", "0.55");
        assert_eq!(detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 0, 5, "0.01").unwrap().size, 5);
        // headroom binds
        assert_eq!(detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 48, 20, "0.01").unwrap().size, 2);
        // at cap
        assert_eq!(
            detect(&mut cx, &r, &b2, "2026-07-28", 0.0, 50, 50, 20, "0.01"),
            Err(Skip::AtCap { open: 50 })
        );
    }
}
