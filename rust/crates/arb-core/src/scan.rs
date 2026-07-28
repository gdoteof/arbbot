//! Edge scanner — statement-for-statement port of src/arbbot/scan/scanner.py
//! under the golden digest gate. All arithmetic runs in a 28-significant-
//! digit ROUND_HALF_EVEN context (CPython decimal's default), via libdecnumber
//! (the `dec` crate) — the same General Decimal Arithmetic spec Python
//! implements — so context-rounded divisions (vwap = cost/size) that feed
//! fee curves match bit-for-bit at emit precision.

use crate::book::{Book, BookBuilder};
use crate::fees::{leg_fee, FeeSchedule, Role};
use crate::model::Venue;
use dec::{Context, Rounding};
use std::cmp::Ordering;

pub type D = dec::Decimal<12>; // 36-digit capacity, ops rounded to prec 28

/// Shared contexts: HALF_EVEN for general math (Python's default), plus the
/// two explicit-rounding quantize modes the scanner uses (ROUND_DOWN sizing,
/// ROUND_CEILING fee cents).
pub struct Cx {
    even: Context<D>,
    down: Context<D>,
    ceil: Context<D>,
    q_cent: D,
    q_emit: D,
    q_one: D,
    q_4dp: D,
    two: D,
    pub one: D,
}

impl Default for Cx {
    fn default() -> Self {
        let mut even = Context::<D>::default();
        even.set_precision(28).expect("prec 28");
        even.set_rounding(Rounding::HalfEven);
        let mut down = even.clone();
        down.set_rounding(Rounding::Down);
        let mut ceil = even.clone();
        ceil.set_rounding(Rounding::Ceiling);
        let mut tmp = even.clone();
        let q_cent = tmp.parse("0.01").expect("q");
        let q_emit = tmp.parse("0.000001").expect("q");
        let q_one = tmp.parse("1").expect("q");
        let q_4dp = tmp.parse("0.0001").expect("q");
        let two = tmp.parse("2").expect("q");
        let one = tmp.parse("1").expect("q");
        Cx { even, down, ceil, q_cent, q_emit, q_one, q_4dp, two, one }
    }
}

impl Cx {
    pub fn parse_exact(&mut self, s: &str) -> D {
        self.even.parse(s).unwrap_or_else(|_| self.zero())
    }

    pub fn parse(&mut self, s: &str) -> Option<D> {
        self.even.parse(s).ok()
    }

    pub fn zero(&mut self) -> D {
        self.even.parse("0").expect("zero")
    }

    pub fn from_i64(&mut self, v: i64) -> D {
        self.even.parse(v.to_string().as_str()).expect("int")
    }

    pub fn add(&mut self, mut a: D, b: D) -> D {
        self.even.add(&mut a, &b);
        a
    }

    pub fn sub(&mut self, mut a: D, b: D) -> D {
        self.even.sub(&mut a, &b);
        a
    }

    pub fn mul(&mut self, mut a: D, b: D) -> D {
        self.even.mul(&mut a, &b);
        a
    }

    pub fn div(&mut self, mut a: D, b: D) -> D {
        self.even.div(&mut a, &b);
        a
    }

    pub fn one_minus(&mut self, v: D) -> D {
        let one = self.one;
        self.sub(one, v)
    }

    pub fn cmp(&mut self, a: D, b: D) -> Ordering {
        let d = self.sub(a, b);
        if d.is_zero() {
            Ordering::Equal
        } else if d.is_negative() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }

    pub fn min(&mut self, a: D, b: D) -> D {
        if self.cmp(a, b) == Ordering::Greater { b } else { a }
    }

    pub fn is_pos(&mut self, v: D) -> bool {
        !v.is_zero() && !v.is_negative()
    }

    /// x.quantize(Decimal("0.01"), ROUND_CEILING) — ceil_cents; `dp` fixed 2.
    pub fn quantize_ceil(&mut self, mut x: D, _dp: u8) -> D {
        let q = self.q_cent;
        self.ceil.quantize(&mut x, &q);
        x
    }

    /// x.quantize(Decimal(1), ROUND_DOWN) — whole contracts.
    pub fn quantize_int_down(&mut self, mut x: D) -> D {
        let q = self.q_one;
        self.down.quantize(&mut x, &q);
        x
    }

    /// x.quantize(Decimal(1), ROUND_CEILING) — maker-ask tick rounding.
    pub fn quantize_int_ceil(&mut self, mut x: D) -> D {
        let q = self.q_one;
        self.ceil.quantize(&mut x, &q);
        x
    }

    /// x.quantize(Decimal("0.0001")) — default HALF_EVEN (quoter APR margin).
    pub fn quantize_4dp(&mut self, mut x: D) -> D {
        let q = self.q_4dp;
        self.even.quantize(&mut x, &q);
        x
    }

    /// x.quantize(Decimal("0.01"), rounding) — quoter tick alignment.
    pub fn quantize_cent(&mut self, mut x: D, up: bool) -> D {
        let q = self.q_cent;
        if up {
            let mut c = self.ceil.clone();
            c.set_rounding(Rounding::Up);
            c.quantize(&mut x, &q);
        } else {
            self.down.quantize(&mut x, &q);
        }
        x
    }

    pub fn halve(&mut self, x: D) -> D {
        let two = self.two;
        let h = self.div(x, two);
        self.quantize_int_down(h)
    }

    /// Emit-scale strings (Opportunity.to_json_dict): 6dp HALF_EVEN, or
    /// whole contracts for size.
    pub fn emit_6dp(&mut self, mut x: D) -> String {
        let q = self.q_emit;
        self.even.quantize(&mut x, &q);
        x.to_standard_notation_string()
    }

    pub fn emit_int(&mut self, mut x: D) -> String {
        let q = self.q_one;
        self.even.quantize(&mut x, &q);
        x.to_standard_notation_string()
    }
}

// ---------- relationship model (scan-relevant subset) ----------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelType {
    CrossVenueEquivalent,
    EquivalentPair,
    Bundle,
    Partition,
    Exclusive,
    DateLadder,
    Implies,
    Rollup,
}

impl RelType {
    /// The registry spelling. This is the `by_class` key the risk manager
    /// aggregates exposure under (Python `rel.type.value`), so it must stay
    /// the exact inverse of `from_str`.
    pub fn as_str(self) -> &'static str {
        match self {
            RelType::CrossVenueEquivalent => "cross-venue-equivalent",
            RelType::EquivalentPair => "equivalent-pair",
            RelType::Bundle => "bundle",
            RelType::Partition => "partition",
            RelType::Exclusive => "exclusive",
            RelType::DateLadder => "date-ladder",
            RelType::Implies => "implies",
            RelType::Rollup => "rollup",
        }
    }

    pub fn from_str(s: &str) -> Option<RelType> {
        Some(match s {
            "cross-venue-equivalent" => RelType::CrossVenueEquivalent,
            "equivalent-pair" => RelType::EquivalentPair,
            "bundle" => RelType::Bundle,
            "partition" => RelType::Partition,
            "exclusive" => RelType::Exclusive,
            "date-ladder" => RelType::DateLadder,
            "implies" => RelType::Implies,
            "rollup" => RelType::Rollup,
            _ => return None,
        })
    }
}

pub struct RelLeg {
    pub venue: Venue,
    pub market_id: String,
}

pub struct Rel {
    pub id: String,
    pub rtype: RelType,
    pub tranche: String, // "head" | "long-tail"
    pub legs: Vec<RelLeg>,
}

/// feasible_states() — port of registry/model.py.
pub fn feasible_states(rtype: RelType, n: usize) -> Vec<Vec<i64>> {
    match rtype {
        RelType::CrossVenueEquivalent | RelType::EquivalentPair => {
            vec![vec![0; n], vec![1; n]]
        }
        RelType::Bundle => vec![vec![0, 1], vec![1, 0]],
        RelType::Implies => vec![vec![0, 0], vec![0, 1], vec![1, 1]],
        RelType::DateLadder => (0..=n)
            .map(|k| (0..n).map(|i| if i < n - k { 0 } else { 1 }).collect())
            .collect(),
        RelType::Partition => (0..n)
            .map(|j| (0..n).map(|i| i64::from(i == j)).collect())
            .collect(),
        RelType::Exclusive => {
            let mut states = vec![vec![0; n]];
            states.extend((0..n).map(|j| {
                (0..n).map(|i| i64::from(i == j)).collect::<Vec<_>>()
            }));
            states
        }
        RelType::Rollup => (0..(1u32 << (n - 1)))
            .map(|mask| {
                let parts: Vec<i64> =
                    (0..n - 1).map(|i| i64::from((mask >> i) & 1 == 1)).collect();
                let mut v = vec![parts.iter().copied().max().unwrap_or(0)];
                v.extend(parts);
                v
            })
            .collect(),
    }
}

fn feasible_min_payoff(rtype: RelType, sides: &[Side]) -> i64 {
    feasible_states(rtype, sides.len())
        .iter()
        .map(|state| {
            sides
                .iter()
                .zip(state)
                .map(|(s, &x)| if *s == Side::Yes { x } else { 1 - x })
                .sum::<i64>()
        })
        .min()
        .expect("nonempty states")
}

// ---------- market metadata (golden path: pydantic Market defaults) ----------

pub struct MarketMeta {
    pub category: String,
    pub min_order_size: D,
    pub close_time: Option<String>,
    pub taker_coef_override: Option<D>,
}

impl MarketMeta {
    pub fn default_for_golden(cx: &mut Cx) -> MarketMeta {
        MarketMeta {
            category: "default".into(),
            min_order_size: cx.one,
            close_time: None,
            taker_coef_override: None,
        }
    }
}

// ---------- opportunity + canonical emit ----------

pub struct BasketLegOut {
    pub venue: Venue,
    pub market_id: String,
    pub buy_side: Side,
    pub vwap: D,
    pub fee: D,
}

pub struct Opportunity {
    pub relationship_id: String,
    pub tranche: String,
    pub basket: Vec<BasketLegOut>,
    pub signature: String,
    pub size: D,
    pub gross_cost: D,
    pub fees: D,
    pub min_payoff: D,
    pub net_edge_total: D,
    pub net_edge_per_contract: D,
    pub bucket: &'static str,
    pub ts_local_ns: i64,
    pub max_hold_close_time: Option<String>,
}

/// Python json.dumps string escaping (ensure_ascii=True default).
fn json_escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let cp = c as u32;
                if cp > 0xFFFF {
                    let v = cp - 0x10000;
                    out.push_str(&format!(
                        "\\u{:04x}\\u{:04x}",
                        0xD800 + (v >> 10),
                        0xDC00 + (v & 0x3FF)
                    ));
                } else {
                    out.push_str(&format!("\\u{cp:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

impl Opportunity {
    /// json.dumps(to_json_dict(), sort_keys=True, separators=(",",":")) —
    /// keys emitted literally in sorted order.
    pub fn canonical_line(&self, cx: &mut Cx) -> String {
        let mut o = String::with_capacity(512);
        o.push_str("{\"basket\":[");
        for (i, leg) in self.basket.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            o.push_str("{\"buy_side\":");
            json_escape(if leg.buy_side == Side::Yes { "yes" } else { "no" }, &mut o);
            o.push_str(",\"fee\":");
            json_escape(&cx.emit_6dp(leg.fee), &mut o);
            o.push_str(",\"market_id\":");
            json_escape(&leg.market_id, &mut o);
            o.push_str(",\"role\":\"taker\",\"venue\":");
            json_escape(leg.venue.as_str(), &mut o);
            o.push_str(",\"vwap\":");
            json_escape(&cx.emit_6dp(leg.vwap), &mut o);
            o.push('}');
        }
        o.push_str("],\"bucket\":");
        json_escape(self.bucket, &mut o);
        o.push_str(",\"fees\":");
        json_escape(&cx.emit_6dp(self.fees), &mut o);
        o.push_str(",\"gross_cost\":");
        json_escape(&cx.emit_6dp(self.gross_cost), &mut o);
        o.push_str(",\"max_hold_close_time\":");
        match &self.max_hold_close_time {
            Some(t) => json_escape(t, &mut o),
            None => o.push_str("null"),
        }
        o.push_str(",\"min_payoff\":");
        json_escape(&cx.emit_6dp(self.min_payoff), &mut o);
        o.push_str(",\"net_edge_per_contract\":");
        json_escape(&cx.emit_6dp(self.net_edge_per_contract), &mut o);
        o.push_str(",\"net_edge_total\":");
        json_escape(&cx.emit_6dp(self.net_edge_total), &mut o);
        o.push_str(",\"relationship_id\":");
        json_escape(&self.relationship_id, &mut o);
        o.push_str(",\"signature\":");
        json_escape(&self.signature, &mut o);
        o.push_str(",\"size\":");
        json_escape(&cx.emit_int(self.size), &mut o);
        o.push_str(",\"tranche\":");
        json_escape(&self.tranche, &mut o);
        o.push_str(",\"ts_local_ns\":");
        o.push_str(&self.ts_local_ns.to_string());
        o.push('}');
        o
    }
}

// ---------- the scanner ----------

type Ladder = Vec<(D, D)>; // (price, size), best first

fn parse_ladder(cx: &mut Cx, levels: &[crate::model::Level]) -> Ladder {
    levels
        .iter()
        .filter_map(|l| Some((cx.parse(&l.price)?, cx.parse(&l.size)?)))
        .collect()
}

fn no_ask_ladder(cx: &mut Cx, bids: &Ladder) -> Ladder {
    bids.iter().map(|(p, s)| (cx.one_minus(*p), *s)).collect()
}

fn walk_cost(cx: &mut Cx, ladder: &Ladder, size: D) -> Option<D> {
    let mut remaining = size;
    let mut cost = cx.zero();
    for (price, lsize) in ladder {
        let take = cx.min(remaining, *lsize);
        let part = cx.mul(take, *price);
        cost = cx.add(cost, part);
        remaining = cx.sub(remaining, take);
        if remaining.is_zero() {
            return Some(cost);
        }
    }
    None
}

fn max_depth(cx: &mut Cx, ladder: &Ladder) -> D {
    let mut total = cx.zero();
    for (_, s) in ladder {
        total = cx.add(total, *s);
    }
    total
}

#[allow(clippy::too_many_arguments)]
fn price_basket(
    cx: &mut Cx,
    fees: &FeeSchedule,
    rel: &Rel,
    sides: &[Side],
    ladders: &[Ladder],
    metas: &dyn Fn(&RelLeg) -> MarketMeta,
    size: D,
    ts_local_ns: i64,
) -> Option<Opportunity> {
    let mut gross = cx.zero();
    let mut total_fees = cx.zero();
    let mut legs_out = Vec::with_capacity(rel.legs.len());
    let mut min_orders: Vec<D> = Vec::new();
    let mut close_times: Vec<String> = Vec::new();
    for ((leg, side), ladder) in rel.legs.iter().zip(sides).zip(ladders) {
        let cost = walk_cost(cx, ladder, size)?;
        let vwap = cx.div(cost, size);
        let meta = metas(leg);
        let fee = leg_fee(
            cx, fees, leg.venue, Role::Taker, vwap, size,
            &meta.category, meta.taker_coef_override,
        );
        gross = cx.add(gross, cost);
        total_fees = cx.add(total_fees, fee);
        legs_out.push(BasketLegOut {
            venue: leg.venue,
            market_id: leg.market_id.clone(),
            buy_side: *side,
            vwap,
            fee,
        });
        min_orders.push(meta.min_order_size);
        if let Some(ct) = meta.close_time {
            close_times.push(ct);
        }
    }
    let min_payoff_i = feasible_min_payoff(rel.rtype, sides);
    let min_payoff = cx.from_i64(min_payoff_i);
    let payoff_total = cx.mul(min_payoff, size);
    let after_cost = cx.sub(payoff_total, gross);
    let net_total = cx.sub(after_cost, total_fees);
    let bucket = if min_orders
        .iter()
        .all(|mo| cx.cmp(size, *mo) != Ordering::Less)
    {
        "primary"
    } else {
        "sub_minimum"
    };
    let sig: String = format!(
        "{}:{}",
        rel.id,
        sides
            .iter()
            .map(|s| if *s == Side::Yes { 'Y' } else { 'N' })
            .collect::<String>()
    );
    let per_contract = cx.div(net_total, size);
    Some(Opportunity {
        relationship_id: rel.id.clone(),
        tranche: rel.tranche.clone(),
        basket: legs_out,
        signature: sig,
        size,
        gross_cost: gross,
        fees: total_fees,
        min_payoff,
        net_edge_total: net_total,
        net_edge_per_contract: per_contract,
        bucket,
        ts_local_ns,
        max_hold_close_time: close_times.into_iter().max(),
    })
}

pub fn scan_relationship(
    cx: &mut Cx,
    fees: &FeeSchedule,
    rel: &Rel,
    books: &BookBuilder,
    metas: &dyn Fn(&RelLeg) -> MarketMeta,
    ts_local_ns: i64,
) -> Vec<Opportunity> {
    let n = rel.legs.len();
    let mut leg_books: Vec<&Book> = Vec::with_capacity(n);
    for leg in &rel.legs {
        match books.get(leg.venue, &leg.market_id) {
            Some(b) if !b.is_crossed() => leg_books.push(b),
            _ => return vec![], // unsynced or self-crossed: data quality
        }
    }
    let parsed_bids: Vec<Ladder> =
        leg_books.iter().map(|b| parse_ladder(cx, &b.bids)).collect();
    let parsed_asks: Vec<Ladder> =
        leg_books.iter().map(|b| parse_ladder(cx, &b.asks)).collect();

    let max_basket = cx.parse_exact("10000");
    let has_kalshi = rel.legs.iter().any(|l| l.venue == Venue::Kalshi);

    let side_masks: Vec<u32> = if n <= 10 {
        (0..(1u32 << n)).collect()
    } else {
        vec![0, (1u32 << n) - 1] // all-YES, all-NO
    };
    let mut out = Vec::new();
    for mask in side_masks {
        // bit i (from the left / leg 0 = high bit) 0 => YES, 1 => NO,
        // matching itertools.product((YES, NO), repeat=n) order
        let sides: Vec<Side> = (0..n)
            .map(|i| {
                if (mask >> (n - 1 - i)) & 1 == 0 { Side::Yes } else { Side::No }
            })
            .collect();
        if feasible_min_payoff(rel.rtype, &sides) < 1 {
            continue;
        }
        let ladders: Vec<Ladder> = sides
            .iter()
            .enumerate()
            .map(|(i, s)| {
                if *s == Side::Yes {
                    parsed_asks[i].clone()
                } else {
                    no_ask_ladder(cx, &parsed_bids[i])
                }
            })
            .collect();
        if ladders.iter().any(|l| l.is_empty()) {
            continue;
        }
        let mut size = max_basket;
        for lad in &ladders {
            let d = max_depth(cx, lad);
            size = cx.min(size, d);
        }
        if has_kalshi {
            size = cx.quantize_int_down(size);
        }
        if !cx.is_pos(size) {
            continue;
        }
        // shrink until profitable at depth-walked prices (start full, halve)
        let mut best: Option<Opportunity> = None;
        let mut candidate = size;
        for _ in 0..12 {
            if let Some(opp) = price_basket(
                cx, fees, rel, &sides, &ladders, metas, candidate, ts_local_ns,
            ) {
                if cx.is_pos(opp.net_edge_per_contract) {
                    best = Some(opp);
                    break;
                }
            }
            candidate = cx.halve(candidate);
            if !cx.is_pos(candidate) {
                break;
            }
        }
        if let Some(b) = best {
            out.push(b);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Level;

    fn lvl(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    /// Pinned against tests/test_golden.py _decision_stream() output
    /// (Python-generated 2026-07-23): the byte-for-byte parity contract.
    const PY_LINE: &str = r#"{"basket":[{"buy_side":"yes","fee":"0.840000","market_id":"K","role":"taker","venue":"kalshi","vwap":"0.400000"},{"buy_side":"no","fee":"0.495000","market_id":"P","role":"taker","venue":"polymarket","vwap":"0.450000"}],"bucket":"primary","fees":"1.335000","gross_cost":"42.500000","max_hold_close_time":"2026-08-01T00:00:00Z","min_payoff":"1.000000","net_edge_per_contract":"0.123300","net_edge_total":"6.165000","relationship_id":"eq1","signature":"eq1:YN","size":"50","tranche":"head","ts_local_ns":7}"#;

    #[test]
    fn matches_python_reference_line() {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.38", "50")],
                          vec![lvl("0.40", "50")], 1, 1, None);
        bb.apply_snapshot(Venue::Polymarket, "P", vec![lvl("0.55", "50")],
                          vec![lvl("0.60", "50")], 1, 1, None);
        let rel = Rel {
            id: "eq1".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::Polymarket, market_id: "P".into() },
            ],
        };
        // test fixture markets: min_order 1, close_time set (test_scanner.market)
        let metas = |_leg: &RelLeg| MarketMeta {
            category: "default".into(),
            min_order_size: Cx::default().one,
            close_time: Some("2026-08-01T00:00:00Z".into()),
            taker_coef_override: None,
        };
        let mut cx2 = Cx::default();
        let opps = scan_relationship(&mut cx2, &fees, &rel, &bb, &metas, 7);
        assert_eq!(opps.len(), 1);
        assert_eq!(opps[0].canonical_line(&mut cx2), PY_LINE);
    }

    fn fedcut_rel() -> Rel {
        Rel {
            id: "xvus-fedcut-26-usfed-2026-cut".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "KXRATECUT-26DEC31".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "cbpac-usfed-2026-cut".into() },
            ],
        }
    }

    /// audit C5: both maker pricing functions price entirely off the HEDGE
    /// book, so a crossed hedge book makes the quote fiction. Same rule and
    /// same position (before any arithmetic) as the take-take detector's guard
    /// from 4542e5f.
    ///
    /// Numbers are the real KXRATECUT-26DEC31 book of 2026-07-28: a phantom ask
    /// at 0.0730 under a 0.1770 bid. The control moves ONLY the bid, below the
    /// same ask, so the single variable under test is the crossing itself.
    #[test]
    fn no_maker_quote_off_a_crossed_hedge_book() {
        let rel = fedcut_rel();
        let metas = |_l: &RelLeg| MarketMeta::default_for_golden(&mut Cx::default());
        let quote = |bids: Vec<Level>, asks: Vec<Level>| {
            let mut cx = Cx::default();
            let fees = FeeSchedule::new(&mut cx);
            let mut bb = BookBuilder::new();
            bb.apply_snapshot(Venue::Kalshi, "KXRATECUT-26DEC31", bids, asks, 1, 1, None);
            let clip = cx.from_i64(5);
            // maker leg 1 (PM-US), hedge leg 0 (Kalshi)
            let bid_q = maker_quote(&mut cx, &fees, &rel, 1, &bb, &metas, clip);
            let ask_q = maker_ask_quote(&mut cx, &fees, &rel, 1, &bb, &metas, clip);
            (bid_q.is_some(), ask_q.is_some())
        };

        let crossed = vec![lvl("0.1770", "305")];
        let locked = vec![lvl("0.0730", "305")];
        let sane = vec![lvl("0.0720", "305")];
        let asks = vec![lvl("0.0730", "26")];

        assert_eq!(quote(crossed, asks.clone()), (false, false), "crossed book priced a quote");
        assert_eq!(quote(locked, asks.clone()), (false, false), "locked book priced a quote");
        // the control: same ask, bid one tick BELOW it. Both sides must still
        // price, or the guard has switched the maker off rather than gated it.
        assert_eq!(quote(sane, asks), (true, true), "the guard silenced a sane book");
    }

    #[test]
    fn feasible_states_match_python() {
        assert_eq!(feasible_states(RelType::CrossVenueEquivalent, 2),
                   vec![vec![0, 0], vec![1, 1]]);
        assert_eq!(feasible_states(RelType::Bundle, 2),
                   vec![vec![0, 1], vec![1, 0]]);
        assert_eq!(feasible_states(RelType::DateLadder, 2),
                   vec![vec![0, 0], vec![0, 1], vec![1, 1]]);
        assert_eq!(feasible_states(RelType::Exclusive, 2),
                   vec![vec![0, 0], vec![1, 0], vec![0, 1]]);
        assert_eq!(feasible_states(RelType::Rollup, 3),
                   vec![vec![0, 0, 0], vec![1, 1, 0], vec![1, 0, 1], vec![1, 1, 1]]);
    }
}

// ---------- maker quotes (quoter targets; intent-parity surface) ----------

/// maker_quote — max YES-bid to rest on leg `i` such that hedging on fill
/// still clears fees. Port of scanner.py maker_quote (min_edge = 0).
pub fn maker_quote(
    cx: &mut Cx,
    fees: &FeeSchedule,
    rel: &Rel,
    maker_leg_index: usize,
    books: &BookBuilder,
    metas: &dyn Fn(&RelLeg) -> MarketMeta,
    hedge_size: D,
) -> Option<D> {
    if rel.legs.len() != 2 {
        return None;
    }
    let sides: Vec<Side> = (0..2)
        .map(|k| if k == maker_leg_index { Side::Yes } else { Side::No })
        .collect();
    if feasible_min_payoff(rel.rtype, &sides) < 1 {
        return None;
    }
    let hedge_index = 1 - maker_leg_index;
    let (maker_leg, hedge_leg) = (&rel.legs[maker_leg_index], &rel.legs[hedge_index]);
    let hedge_book = books.get(hedge_leg.venue, &hedge_leg.market_id)?;
    // Sanity BEFORE arithmetic, same rule and same place as the take-take
    // detector (4542e5f): this whole quote is priced off the hedge book, so a
    // crossed hedge book makes the quote fiction. `Book::crossing` says why.
    if hedge_book.is_crossed() {
        return None;
    }
    let bids = parse_ladder(cx, &hedge_book.bids);
    let ladder = no_ask_ladder(cx, &bids);
    let hedge_cost = walk_cost(cx, &ladder, hedge_size)?;
    let hedge_vwap = cx.div(hedge_cost, hedge_size);
    let hedge_meta = metas(hedge_leg);
    let maker_meta = metas(maker_leg);
    let hedge_fee = crate::fees::leg_fee(
        cx, fees, hedge_leg.venue, crate::fees::Role::Taker, hedge_vwap, hedge_size,
        &hedge_meta.category, hedge_meta.taker_coef_override,
    );
    // rough = 1 - hedge_vwap - hedge_fee/hedge_size (min_edge 0)
    let one = cx.one;
    let a = cx.sub(one, hedge_vwap);
    let per = cx.div(hedge_fee, hedge_size);
    let rough = cx.sub(a, per);
    if !cx.is_pos(rough) {
        return None;
    }
    let maker_fee = fees.fee(
        cx, maker_leg.venue, crate::fees::Role::Maker, rough, hedge_size,
        &maker_meta.category,
    );
    let mper = cx.div(maker_fee, hedge_size);
    let p = cx.sub(rough, mper);
    if !cx.is_pos(p) {
        return None;
    }
    // p = (p / tick).quantize(ONE, ROUND_DOWN) * tick   (tick = 0.01 default)
    let tick = cx.parse_exact("0.01");
    let ticks = cx.div(p, tick);
    let ticks = cx.quantize_int_down(ticks);
    let p = cx.mul(ticks, tick);
    if cx.is_pos(p) { Some(p) } else { None }
}

/// maker_ask_quote — min YES-ask; mirror with ROUND_CEILING.
pub fn maker_ask_quote(
    cx: &mut Cx,
    fees: &FeeSchedule,
    rel: &Rel,
    maker_leg_index: usize,
    books: &BookBuilder,
    metas: &dyn Fn(&RelLeg) -> MarketMeta,
    hedge_size: D,
) -> Option<D> {
    if rel.legs.len() != 2 {
        return None;
    }
    let sides: Vec<Side> = (0..2)
        .map(|k| if k == maker_leg_index { Side::No } else { Side::Yes })
        .collect();
    if feasible_min_payoff(rel.rtype, &sides) < 1 {
        return None;
    }
    let hedge_index = 1 - maker_leg_index;
    let (maker_leg, hedge_leg) = (&rel.legs[maker_leg_index], &rel.legs[hedge_index]);
    let hedge_book = books.get(hedge_leg.venue, &hedge_leg.market_id)?;
    // The C5 exposure verbatim: this walks the hedge ASK ladder, whose first
    // level is the phantom on a crossed book. Refuse before any arithmetic.
    if hedge_book.is_crossed() {
        return None;
    }
    let ladder = parse_ladder(cx, &hedge_book.asks);
    let hedge_cost = walk_cost(cx, &ladder, hedge_size)?;
    let hedge_vwap = cx.div(hedge_cost, hedge_size);
    let hedge_meta = metas(hedge_leg);
    let maker_meta = metas(maker_leg);
    let hedge_fee = crate::fees::leg_fee(
        cx, fees, hedge_leg.venue, crate::fees::Role::Taker, hedge_vwap, hedge_size,
        &hedge_meta.category, hedge_meta.taker_coef_override,
    );
    let per = cx.div(hedge_fee, hedge_size);
    let rough = cx.add(hedge_vwap, per);
    let one = cx.one;
    let capped = cx.min(rough, one);
    let maker_fee = fees.fee(
        cx, maker_leg.venue, crate::fees::Role::Maker, capped, hedge_size,
        &maker_meta.category,
    );
    let mper = cx.div(maker_fee, hedge_size);
    let p = cx.add(rough, mper);
    if cx.cmp(p, one) != std::cmp::Ordering::Less {
        return None;
    }
    let tick = cx.parse_exact("0.01");
    let ticks = cx.div(p, tick);
    let ticks = cx.quantize_int_ceil(ticks);
    let p = cx.mul(ticks, tick);
    if cx.cmp(p, one) == std::cmp::Ordering::Less { Some(p) } else { None }
}
