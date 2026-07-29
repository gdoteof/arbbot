//! Maker-quoter policy — port of src/arbbot/exec/quoter.py under the
//! intent-parity gate (scripts/intent_replay.py is the Python reference).
//!
//! Harness conventions (both sides identical): jitter knobs 0 (exact-match
//! deadband, no random), clip 5, min_requote_s 15.0 of TAPE time, default
//! Market metadata (tick 0.01), risk always allows, kill switch off, mock
//! gateway with globally sequential ids "m1","m2",... Canonical intent lines
//! are `crate::intent::Intent` values (sorted keys, compact separators,
//! integral floats printed with ".0" — matching Python json.dumps). This
//! emits the DECISION; `Intent::to_line` is the only thing that emits bytes.

use crate::book::BookBuilder;
use crate::fees::FeeSchedule;
use crate::intent::{self, Intent};
use crate::model::{BookSide, Venue};
use crate::scan::{maker_ask_quote, maker_quote, Cx, MarketMeta, Rel, RelType, D};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Mirror of quoter.py TOXGATE_MAX / TOXGATE_MAX_AGE.
const TOXGATE_MAX: f64 = 0.03;
const TOXGATE_MAX_AGE: f64 = 120.0;

/// The research toxicity shadow feed (data/exec/toxgate.json), loaded by the
/// caller (file I/O stays outside arb-core). Mirrors quoter.py `_toxgate`:
/// stale feed (doc ts older than TOXGATE_MAX_AGE vs `now`) fails open.
/// One risk consultation per would-be place.
///
/// The engine implements this: it owns the exposure fold, the balances and the
/// caps, none of which belong in a pure quoter. `None` on a Quoter means ALWAYS
/// ALLOW, which is what the golden/intent replays want — risk is not part of
/// the decision contract they pin.
pub trait RiskGate: Send + Sync {
    /// `notional` is the clip in USD (a prediction-market contract is ~$1, so
    /// contracts and dollars are the same number — Python passes
    /// `Decimal(q)`). `venue` is the leg being quoted, which is the venue whose
    /// cash the order would spend.
    fn check(&self, rel: &Rel, venue: Venue, notional: i64) -> RiskVerdict;
}

pub struct RiskVerdict {
    pub allowed: bool,
    /// Human-readable cap breaches, surfaced verbatim in the `skip` intent.
    pub reasons: Vec<String>,
}

pub struct Toxgate {
    pub ts: f64,
    /// market_id -> side ("bid"/"ask") -> expected adverse cost per contract
    ///
    /// Keyed by the WIRE spelling because that is what the research file on
    /// disk contains; the lookup below is the only reader, and it asks with
    /// `BookSide::as_str`. A key that is neither is simply never asked for,
    /// which is the same fail-open a missing entry gets.
    pub markets: HashMap<String, HashMap<String, f64>>,
}

impl Toxgate {
    fn score(&self, market_id: &str, side: BookSide, now: f64) -> Option<f64> {
        if now - self.ts > TOXGATE_MAX_AGE {
            return None; // stale — fail open
        }
        self.markets.get(market_id)?.get(side.as_str()).copied()
    }
}

pub fn maker_leg_indices(rtype: RelType) -> Vec<usize> {
    match rtype {
        RelType::CrossVenueEquivalent | RelType::EquivalentPair => vec![0, 1],
        RelType::Implies => vec![1], // consequent only
        _ => vec![],
    }
}

struct RestingQuote {
    order_id: String,
    price: D,
    count: i64,
}

pub struct Quoter {
    pub rel: Rel,
    clip: i64,
    min_requote_s: f64,
    /// Pre-computed `_apr_margin()` (min_apr/resolve_years are static per
    /// harness run; quoter.py recomputes the same value per call). None when
    /// the APR hurdle is disabled — exact original behavior (subtracting the
    /// Python Decimal(0) is a numeric no-op through quantize_cent).
    apr_margin: Option<D>,
    /// (market_id, side) pairs another subsystem owns (maker-unwind exit
    /// asks) — `target` returns None so any entry quote cancels, none rests.
    suppress: HashSet<(String, BookSide)>,
    /// Toxicity gate feed; None => gate off (exact original behavior).
    toxgate: Option<Arc<Toxgate>>,
    risk: Option<Arc<dyn RiskGate>>,
    resting: HashMap<(usize, BookSide), RestingQuote>,
    last_quote_ts: HashMap<(usize, BookSide), f64>,
}

fn default_meta(cx: &mut Cx) -> MarketMeta {
    MarketMeta::default_for_golden(cx)
}

impl Quoter {
    pub fn new(rel: Rel) -> Self {
        Quoter {
            rel,
            clip: 5,
            min_requote_s: 15.0,
            apr_margin: None, // hurdle off
            suppress: HashSet::new(),
            toxgate: None,
            risk: None,
            resting: HashMap::new(),
            last_quote_ts: HashMap::new(),
        }
    }

    /// quoter.py `_apr_margin`: lock/(1-lock)/yrs >= apr => lock >= a/(1+a),
    /// a = Decimal(str(min_apr))/100 * Decimal(str(resolve_years)), quantized
    /// to 0.0001 HALF_EVEN. `str(float)` == serde_json f64 formatting (both
    /// shortest-repr with ".0" on integral values), so parse THAT string.
    pub fn set_apr(&mut self, cx: &mut Cx, min_apr: f64, resolve_years: Option<f64>) {
        self.apr_margin = None;
        if min_apr <= 0.0 {
            return;
        }
        let Some(yrs) = resolve_years else { return };
        if yrs == 0.0 {
            return; // Python: `not self.resolve_years`
        }
        let apr_s = serde_json::to_string(&min_apr).expect("f64 json");
        let yrs_s = serde_json::to_string(&yrs).expect("f64 json");
        let apr = cx.parse_exact(&apr_s);
        let yrs_d = cx.parse_exact(&yrs_s);
        let hundred = cx.from_i64(100);
        let a0 = cx.div(apr, hundred);
        let a = cx.mul(a0, yrs_d);
        let one = cx.one;
        let denom = cx.add(one, a);
        let m = cx.div(a, denom);
        self.apr_margin = Some(cx.quantize_4dp(m));
    }

    pub fn set_suppress(&mut self, pairs: HashSet<(String, BookSide)>) {
        self.suppress = pairs;
    }

    pub fn set_risk(&mut self, risk: Option<Arc<dyn RiskGate>>) {
        self.risk = risk;
    }

    pub fn set_toxgate(&mut self, tox: Option<Arc<Toxgate>>) {
        self.toxgate = tox;
    }

    /// Best price on `side` from OTHER participants (our resting subtracted).
    fn touch_excl_self(
        &self,
        cx: &mut Cx,
        books: &BookBuilder,
        i: usize,
        side: BookSide,
    ) -> Option<D> {
        let leg = &self.rel.legs[i];
        let book = books.get(leg.venue, &leg.market_id)?;
        let rq = self.resting.get(&(i, side));
        let levels = match side {
            BookSide::Bid => &book.bids,
            BookSide::Ask => &book.asks,
        };
        for lvl in levels {
            let p = cx.parse(&lvl.price)?;
            let mut size = cx.parse(&lvl.size)?;
            if let Some(rq) = rq {
                if cx.cmp(p, rq.price) == Ordering::Equal {
                    let c = cx.from_i64(rq.count);
                    size = cx.sub(size, c);
                }
            }
            if cx.is_pos(size) {
                return Some(p);
            }
        }
        None
    }

    fn target(
        &self,
        cx: &mut Cx,
        fees: &FeeSchedule,
        books: &BookBuilder,
        i: usize,
        side: BookSide,
    ) -> Option<D> {
        let leg = &self.rel.legs[i];
        if self.suppress.contains(&(leg.market_id.clone(), side)) {
            return None; // side owned by maker-unwind — cancel/stay out
        }
        let book = books.get(leg.venue, &leg.market_id)?;
        // tick 0.01 (default Market), safety_ticks 0 => buf 0
        let cur = self.resting.get(&(i, side));
        let comp = self.touch_excl_self(cx, books, i, side);
        let tick = cx.parse_exact("0.01");
        let clip = cx.from_i64(self.clip);
        let metas = |l: &crate::scan::RelLeg| {
            let _ = l;
            default_meta(&mut Cx::default())
        };
        if side == BookSide::Bid {
            let mut p_max = maker_quote(cx, fees, &self.rel, i, books, &metas, clip)?;
            if let Some(m) = self.apr_margin {
                p_max = cx.sub(p_max, m); // fill must annualize >= min_apr
            }
            let comp = match comp {
                None => {
                    // hold a still-profitable quote; never invent aggression
                    return match cur {
                        Some(c) if cx.cmp(c.price, p_max) != Ordering::Greater => {
                            Some(c.price)
                        }
                        _ => None,
                    };
                }
                Some(c) => c,
            };
            if cx.cmp(p_max, comp) == Ordering::Less {
                return None; // can't get inside the competition profitably
            }
            let step = cx.add(comp, tick);
            let mut raw = cx.min(p_max, step);
            if let Some(ba) = book.asks.first() {
                let bap = cx.parse(&ba.price)?;
                let cap = cx.sub(bap, tick);
                raw = cx.min(raw, cap);
            }
            if cx.cmp(raw, comp) == Ordering::Less {
                return None; // too tight to post passively
            }
            Some(cx.quantize_cent(raw, false))
        } else {
            let mut p_min = maker_ask_quote(cx, fees, &self.rel, i, books, &metas, clip)?;
            if let Some(m) = self.apr_margin {
                p_min = cx.add(p_min, m); // fill must annualize >= min_apr
            }
            let comp = match comp {
                None => {
                    return match cur {
                        Some(c) if cx.cmp(c.price, p_min) != Ordering::Less => Some(c.price),
                        _ => None,
                    };
                }
                Some(c) => c,
            };
            if cx.cmp(p_min, comp) == Ordering::Greater {
                return None;
            }
            let step = cx.sub(comp, tick);
            let mut raw = if cx.cmp(p_min, step) == Ordering::Greater { p_min } else { step };
            if let Some(bb) = book.bids.first() {
                let bbp = cx.parse(&bb.price)?;
                let floor = cx.add(bbp, tick);
                if cx.cmp(raw, floor) == Ordering::Less {
                    raw = floor;
                }
            }
            if cx.cmp(raw, comp) == Ordering::Greater {
                return None;
            }
            Some(cx.quantize_cent(raw, true))
        }
    }

    /// CROSSED-BOOK GUARD for the maker path (audit C5, 2026-07-28).
    ///
    /// `4542e5f` put this rule into the take-take detector only. The maker path
    /// had none, and crossed Kalshi books are not microsecond artifacts.
    /// Measured by replaying data/raw/kalshi-2026-07-28.jsonl (381,632 book
    /// events, 191 markets, 906 min): 6 markets crossed at some point, 4 still
    /// crossed at end-of-tape, longest unbroken run 441 min. THREE of them are
    /// markets this engine is armed to quote — KXRATECUT-26DEC31 (441 min
    /// inverted, `xvus-fedcut-26-usfed-2026-cut`, in `config/tradable.yaml` and
    /// matched by the armed drop-in's `--rel-prefix xvus-fedcut-26`) and both
    /// legs of the `vetted_by: human` `kalshi-fed-dec26-t375-implies-t350`
    /// (T3.50 316 min inverted, T3.75 10 min locked).
    ///
    /// `maker_ask_quote` walked the phantom ask, the quote looked profitable, it
    /// rested, it got lifted — and the hedge then refused at `max_slip 0.01`
    /// because the real hedge price was never there. Naked short to resolution.
    ///
    /// BOTH leg books are checked, not just the hedge:
    ///   * the HEDGE book prices the quote (`maker_quote` walks its bids,
    ///     `maker_ask_quote` its asks) — a phantom there is a fictional price;
    ///   * the MAKER book decides where we sit (`touch_excl_self` for the
    ///     competition, plus the best-ask cap / best-bid floor below). A
    ///     phantom there can put our "passive" quote through the real touch,
    ///     which is a taker order wearing a maker's clothes.
    ///
    /// Returns the reason string, so the skip names the venue, the market and
    /// the inversion rather than joining the undifferentiated no-edge silence.
    fn crossed_reason(&self, books: &BookBuilder, i: usize) -> Option<String> {
        // maker leg first: it is the one whose market_id the operator sees in
        // this leg's place/cancel intents, so naming it first reads naturally.
        for j in [i, 1 - i] {
            let Some(leg) = self.rel.legs.get(j) else { continue };
            let Some(book) = books.get(leg.venue, &leg.market_id) else { continue };
            if let Some((bid, ask)) = book.crossing() {
                return Some(format!(
                    "crossed book {} {} bid {bid} >= ask {ask}",
                    leg.venue.as_str(),
                    leg.market_id
                ));
            }
        }
        None
    }

    fn hedge_has_depth(
        &self,
        cx: &mut Cx,
        books: &BookBuilder,
        i: usize,
        side: BookSide,
    ) -> bool {
        let hedge_leg = &self.rel.legs[1 - i];
        let Some(b) = books.get(hedge_leg.venue, &hedge_leg.market_id) else {
            return false;
        };
        let lvl = match side {
            BookSide::Bid => b.bids.first(),
            BookSide::Ask => b.asks.first(),
        };
        match lvl {
            Some(l) => {
                let Some(sz) = cx.parse(&l.size) else { return false };
                let clip = cx.from_i64(self.clip);
                cx.cmp(sz, clip) != Ordering::Less
            }
            None => false,
        }
    }

    /// Emit price exactly like Python str(Decimal) of the quantized value.
    fn px(cx: &mut Cx, p: D) -> String {
        let _ = cx;
        p.to_standard_notation_string()
    }

    /// Cancel every resting quote (kill switch / shutdown), emitting cancel
    /// intents in deterministic (leg, side) order. Mirrors Python cancel_all.
    pub fn cancel_all(&mut self, cx: &mut Cx, now: f64, intents: &mut Vec<Intent>) {
        let mut keys: Vec<(usize, BookSide)> = self.resting.keys().copied().collect();
        // By the WIRE spelling, so `ask` comes before `bid` — the order these
        // cancels have always gone out in, and one the golden digest hashes.
        // `BookSide` is deliberately not `Ord` so that this cannot quietly
        // become declaration order (bid before ask) when the key stopped being
        // a string.
        keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.as_str().cmp(b.1.as_str())));
        for key in keys {
            let curq = self.resting.remove(&key).expect("key from keys()");
            let leg = &self.rel.legs[key.0];
            intents.push(Intent::Cancel(intent::Cancel {
                cancel: leg.market_id.clone(),
                order_id: curq.order_id,
                price: Self::px(cx, curq.price),
                side: key.1,
                ts: now,
                venue: leg.venue,
            }));
            self.last_quote_ts.remove(&key);
        }
    }

    pub fn on_book(
        &mut self,
        cx: &mut Cx,
        fees: &FeeSchedule,
        books: &BookBuilder,
        now: f64,
        next_oid: &mut u64,
        intents: &mut Vec<Intent>,
    ) {
        let leg_indices = maker_leg_indices(self.rel.rtype);
        for i in leg_indices {
            // Crossed book FIRST, before any pricing — the take-take
            // precedent's ordering ("sanity BEFORE arithmetic"). It is a
            // property of this leg's two BOOKS, not of a side, so it is asked
            // once per leg and reported once: emitting the identical reason
            // twice would double the log for no added information.
            //
            // Both sides then fall through to `target` None, which is the
            // existing cancel path, and that is deliberate: a book we can no
            // longer trust is a book we cannot leave a quote resting against.
            // (Contrast the risk gate below, which must NOT cancel — a cap
            // refusal is a reason not to add exposure, not evidence that the
            // resting price is wrong.)
            let crossed = self.crossed_reason(books, i);
            if let Some(reason) = &crossed {
                intents.push(Intent::Skip(intent::Skip { skip: vec![reason.clone()], ts: now }));
            }
            for side in [BookSide::Bid, BookSide::Ask] {
                let key = (i, side);
                let leg_venue = self.rel.legs[i].venue;
                let leg_market = self.rel.legs[i].market_id.clone();
                let mut target = match &crossed {
                    Some(_) => None,
                    None => self.target(cx, fees, books, i, side),
                };
                if target.is_some() && !self.hedge_has_depth(cx, books, i, side) {
                    target = None;
                }
                // toxgate (card 059ce700): a toxic (market, side) is unviable —
                // emit the skip record, cancel any resting quote, rest nothing.
                // Advisory + fail-open: missing/stale feed never blocks.
                if target.is_some() {
                    if let Some(tox) =
                        self.toxgate.as_ref().and_then(|t| t.score(&leg_market, side, now))
                    {
                        if tox > TOXGATE_MAX {
                            intents.push(Intent::Skip(intent::Skip {
                                // `as_str`, not `{side:?}`: this reason string
                                // reaches the intents file and the digest, and
                                // Debug would spell it `Bid`.
                                skip: vec![format!(
                                    "toxgate {} {tox:.3} > {TOXGATE_MAX}",
                                    side.as_str()
                                )],
                                ts: now,
                            }));
                            target = None;
                        }
                    }
                }
                let Some(target) = target else {
                    if let Some(curq) = self.resting.remove(&key) {
                        intents.push(Intent::Cancel(intent::Cancel {
                            cancel: leg_market,
                            order_id: curq.order_id,
                            price: Self::px(cx, curq.price),
                            side,
                            ts: now,
                            venue: leg_venue,
                        }));
                        // KEEP last_quote_ts: re-entry on this side is throttled
                        // like a reprice (card 6fb469da). Dropping it made a
                        // cancelled side re-postable on the very next book event.
                    }
                    continue;
                };
                // hysteresis: exact-match hold (deadband 0)
                if let Some(curq) = self.resting.get(&key) {
                    if cx.cmp(curq.price, target) == Ordering::Equal {
                        continue;
                    }
                }
                // requote throttle, on BOTH paths: repricing a still-profitable
                // quote AND re-entering a side we recently cancelled. Cancels stay
                // prompt (target None returns above); what this stops is the
                // re-post half of a cancel/re-post loop — on fraalb a 500-1000 lot
                // maker walks the ask down 1c at a time, we chase to our floor,
                // cancel, they pull, and we re-posted on the next event: 961 places
                // + 420 cancels in 24h on one relationship, 74% of all order
                // traffic, for ZERO fills (card 6fb469da).
                //
                // A side never quoted has NO timestamp and is never throttled. That
                // is why this is an Option check and not `unwrap_or(0.0)`: `now` is
                // TAPE time, which starts near zero, so a 0.0 default would throttle
                // the very first quote of every side.
                //
                // INVARIANT for the P4 fill path: a FILL must clear last_quote_ts
                // for its key, so a filled side re-quotes at once instead of
                // serving out a re-entry throttle. The quoter has no fill path
                // today; whoever adds one owns this.
                if let Some(&last) = self.last_quote_ts.get(&key) {
                    if now - last < self.min_requote_s {
                        continue;
                    }
                }
                // RISK (quoter.py:302). Consulted BEFORE the cancel below, and
                // that ordering is the point: a rejected REPLACEMENT leaves the
                // existing quote resting instead of pulling a good one to make
                // room for an order risk will not allow. Emits a `skip` intent
                // naming the breached caps.
                if let Some(gate) = &self.risk {
                    let v = gate.check(&self.rel, self.rel.legs[i].venue, self.clip);
                    if !v.allowed {
                        intents.push(Intent::Skip(intent::Skip { skip: v.reasons, ts: now }));
                        continue;
                    }
                }
                let mut replaced: Option<(String, String)> = None;
                if let Some(curq) = self.resting.remove(&key) {
                    replaced = Some((curq.order_id, Self::px(cx, curq.price)));
                }
                *next_oid += 1;
                let oid = format!("m{}", *next_oid);
                let price_s = Self::px(cx, target);
                let (replaces, old_price) = match replaced {
                    Some((roid, oldp)) => (Some(roid), Some(oldp)),
                    None => (None, None),
                };
                intents.push(Intent::Place(intent::Place {
                    count: self.clip,
                    old_price,
                    order_id: oid.clone(),
                    place: leg_market,
                    price: price_s,
                    replaces,
                    // The quoter rests post-only GTC makers and nothing else:
                    // no retry chain, no strategy tag, never a taker.
                    retry: None,
                    side,
                    tag: None,
                    taker: false,
                    ts: now,
                    venue: leg_venue,
                }));
                self.resting.insert(
                    key,
                    RestingQuote { order_id: oid, price: target, count: self.clip },
                );
                self.last_quote_ts.insert(key, now);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;
    use crate::model::Level;
    use crate::scan::RelLeg;

    pub(crate) fn lvl(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    /// The fixture from tests/test_intent_replay.py: a cross-venue pair whose
    /// Kalshi bid funds a PM-US maker YES-bid one tick inside.
    pub(crate) fn fixture() -> (Cx, FeeSchedule, BookBuilder, Quoter) {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let mut bb = BookBuilder::new();
        // K deep bid 0.60 => hedging NO costs 0.40
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "500")],
                          vec![lvl("0.99", "1")], 1, 1_000_000_000, None);
        let rel = Rel {
            id: "xv-test".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        };
        (cx, fees, bb, Quoter::new(rel))
    }

    pub(crate) fn pm_bid(bb: &mut BookBuilder, px: &str, seq: u64, ts_ns: i64) {
        bb.apply_snapshot(Venue::PolymarketUs, "P", vec![lvl(px, "500")],
                          vec![lvl("0.99", "1")], seq, ts_ns, None);
    }

    /// The place on `market`, or None. These assertions used to be substring
    /// matches on the emitted line (`i.contains(r#""place":"P""#)`), which was
    /// the only handle the tests had when an intent WAS a string.
    pub(crate) fn place_on<'a>(intents: &'a [Intent], market: &str) -> Option<&'a intent::Place> {
        intents.iter().find_map(|i| match i {
            Intent::Place(p) if p.place == market => Some(p),
            _ => None,
        })
    }

    pub(crate) fn cancel_on<'a>(intents: &'a [Intent], market: &str) -> Option<&'a intent::Cancel> {
        intents.iter().find_map(|i| match i {
            Intent::Cancel(c) if c.cancel == market => Some(c),
            _ => None,
        })
    }

    pub(crate) fn any_place(intents: &[Intent]) -> bool {
        intents.iter().any(|i| matches!(i, Intent::Place(_)))
    }

    pub(crate) fn any_cancel(intents: &[Intent]) -> bool {
        intents.iter().any(|i| matches!(i, Intent::Cancel(_)))
    }

    pub(crate) fn skips(intents: &[Intent]) -> Vec<String> {
        intents
            .iter()
            .filter_map(|i| match i {
                Intent::Skip(s) => Some(s.skip.join("; ")),
                _ => None,
            })
            .collect()
    }

    /// `now` is TAPE time, which starts near zero. A side that has never been
    /// quoted has no timestamp and must not be throttled — an `unwrap_or(0.0)`
    /// default would suppress the very first quote of every side.
    #[test]
    fn the_first_quote_of_a_side_is_never_throttled() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0u64;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        // tape time 2.0 is well inside min_requote_s (15.0)
        q.on_book(&mut cx, &fees, &bb, 2.0, &mut oid, &mut intents);
        assert_eq!(intents.len(), 1, "first quote suppressed: {intents:?}");
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");
    }

    /// An amend is ONE intent carrying `replaces` — Python performed the cancel
    /// inline through the gateway and used `replaces` only to label the activity
    /// feed, so the intent stream never contained it and the parity harness
    /// pins that. The executor therefore has to turn `replaces` into a real
    /// cancel: `arb-trader`'s `intent_actions` does, and this pins the contract
    /// it depends on. Until 2026-07-28 nothing did, and every superseded quote
    /// stayed resting at a price the quoter had already moved away from.
    #[test]
    fn a_reprice_names_the_order_it_replaces() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0u64;
        let mut intents = Vec::new();

        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(intents.len(), 1, "{intents:?}");
        let first = place_on(&intents, "P").expect("the entry place");
        assert!(first.replaces.is_none(), "an entry replaces nothing");
        let (old_oid, old_px) = (first.order_id.clone(), first.price.clone());

        // past min_requote_s, with a book that wants a different price
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);

        assert_eq!(intents.len(), 1, "an amend is ONE intent, not a cancel + a place");
        let reprice = place_on(&intents, "P").expect("the reprice");
        assert_eq!(reprice.replaces.as_ref(), Some(&old_oid));
        assert_eq!(reprice.old_price.as_ref(), Some(&old_px));
        assert_ne!(reprice.order_id, old_oid, "the replacement is a new order id");
    }

    /// The toxgate's skip reason is an emitted byte string: it lands in
    /// `intents.jsonl` and in the golden digest. `side` is a `BookSide` now, so
    /// the format has to ask for `as_str()` — `{side}` would not compile and
    /// `{side:?}` would compile and silently write `Bid`.
    #[test]
    fn a_toxic_side_skips_by_its_wire_spelling() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        q.set_toxgate(Some(Arc::new(Toxgate {
            ts: 100.0,
            markets: HashMap::from([(
                "P".to_string(),
                HashMap::from([("bid".to_string(), 0.05)]),
            )]),
        })));
        let mut oid = 0u64;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);

        assert_eq!(skips(&intents), vec!["toxgate bid 0.050 > 0.03"]);
        assert!(!any_place(&intents), "a toxic side rests nothing: {intents:?}");
    }

    /// card 6fb469da (the fraalb sawtooth): the throttle applies to RE-ENTRY
    /// after a cancel, not just to repricing a resting quote.
    #[test]
    fn re_entry_after_a_cancel_is_throttled_then_allowed() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0u64;

        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(intents.len(), 1, "expected the entry place: {intents:?}");

        // K bid collapses => the PM maker quote is unviable => prompt cancel
        intents.clear();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.30", "500")],
                          vec![lvl("0.99", "1")], 2, 101_000_000_000, None);
        q.on_book(&mut cx, &fees, &bb, 101.0, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_some(),
                "cancel must stay prompt: {intents:?}");

        // K recovers 1s later: viable again, but re-entry is inside the throttle
        intents.clear();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "500")],
                          vec![lvl("0.99", "1")], 3, 102_000_000_000, None);
        q.on_book(&mut cx, &fees, &bb, 102.0, &mut oid, &mut intents);
        assert!(intents.is_empty(), "re-entry must be throttled: {intents:?}");

        // past min_requote_s from the last placement, re-entry is allowed
        intents.clear();
        pm_bid(&mut bb, "0.30", 2, 120_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 120.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(),
                "re-entry must resume after the throttle: {intents:?}");
    }
}

#[cfg(test)]
mod risk_gate_tests {
    use super::tests_support::*;
    use super::*;

    struct Always(bool);
    impl RiskGate for Always {
        fn check(&self, _rel: &Rel, _venue: Venue, _notional: i64) -> RiskVerdict {
            RiskVerdict {
                allowed: self.0,
                reasons: if self.0 { vec![] } else { vec!["per-relationship tail cap".into()] },
            }
        }
    }

    /// No gate = always allow. The golden and intent replays run this way; risk
    /// is not part of the decision contract they pin.
    #[test]
    fn no_gate_means_always_allow() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(intents.len(), 1);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");
    }

    /// A refused ENTRY places nothing and says why.
    #[test]
    fn a_refused_entry_emits_a_skip_naming_the_reasons() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        q.set_risk(Some(std::sync::Arc::new(Always(false))));
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);

        assert_eq!(intents.len(), 1, "{intents:?}");
        let why = skips(&intents);
        assert_eq!(why.len(), 1, "{intents:?}");
        assert!(why[0].contains("tail cap"), "{why:?}");
        assert!(place_on(&intents, "P").is_none(), "nothing may be placed: {intents:?}");
        assert_eq!(oid, 0, "a refused order must not consume an order id");
    }

    /// THE ordering property (quoter.py:302): risk is consulted BEFORE the
    /// cancel, so refusing a REPLACEMENT leaves the existing quote resting.
    /// Checking after the cancel would pull a good quote to make room for an
    /// order risk was never going to allow.
    #[test]
    fn a_refused_replacement_leaves_the_existing_quote_resting() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();

        // entry lands while risk still allows
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");

        // now risk refuses, and the book moves enough to want a reprice
        q.set_risk(Some(std::sync::Arc::new(Always(false))));
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);

        assert!(!skips(&intents).is_empty(), "expected a skip: {intents:?}");
        assert!(
            cancel_on(&intents, "P").is_none(),
            "the resting quote must NOT be pulled: {intents:?}"
        );
    }

    /// An UNVIABLE quote is still cancelled promptly when risk refuses — the
    /// gate must never strand an order the book says is bad.
    #[test]
    fn risk_never_blocks_a_cancel() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");

        q.set_risk(Some(std::sync::Arc::new(Always(false))));
        intents.clear();
        // K collapses => the PM quote is unviable
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.30", "500")],
                          vec![lvl("0.99", "1")], 2, 131_000_000_000, None);
        q.on_book(&mut cx, &fees, &bb, 131.0, &mut oid, &mut intents);

        assert!(
            cancel_on(&intents, "P").is_some(),
            "an unviable quote must still cancel: {intents:?}"
        );
    }
}

/// audit C5 — the maker path had no crossed-book guard, and crossed Kalshi
/// books are chronic: 6 of 191 markets on the 2026-07-28 tape, 4 of them still
/// crossed at end-of-tape, worst run 441 min. On the other venue there were
/// NONE — 0 of 749 PM-US markets over 7.76M events, because that feed is
/// snapshot-only and so has no add-and-never-remove failure mode.
///
/// Every fixture here is the real KXRATECUT-26DEC31 / usfed-2026-cut pair,
/// which was in `config/tradable.yaml` and matched by the armed drop-in's
/// `--rel-prefix xvus-fedcut-26`, so this was live exposure and not a
/// hypothetical. The control fixture differs from the crossed one in ONE value
/// — the Kalshi bid — so any test that passes for the wrong reason shows up as
/// the control failing.
#[cfg(test)]
mod crossed_book_tests {
    use super::tests_support::{any_cancel, any_place, lvl, skips};
    use super::*;
    use crate::model::Level;
    use crate::scan::RelLeg;

    const KMKT: &str = "KXRATECUT-26DEC31";
    const PMKT: &str = "cbpac-usfed-2026-cut";

    /// Kalshi bid/ask as given; PM-US bid 0.1600 / ask 0.1700 (the tape's true
    /// PM-US book, which agreed with Kalshi's real 0.1820 ask).
    fn books(k_bids: Vec<Level>, k_asks: Vec<Level>) -> (Cx, FeeSchedule, BookBuilder, Quoter) {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, KMKT, k_bids, k_asks, 1, 1_000_000_000, None);
        bb.apply_snapshot(Venue::PolymarketUs, PMKT, vec![lvl("0.1600", "330")],
                          vec![lvl("0.1700", "330")], 1, 1_000_000_000, None);
        let rel = Rel {
            id: "xvus-fedcut-26-usfed-2026-cut".into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: KMKT.into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: PMKT.into() },
            ],
        };
        (cx, fees, bb, Quoter::new(rel))
    }

    /// The phantom ask, exactly as recorded: added twice, never removed, and
    /// pinned as best-ask under the bid because asks sort ascending.
    fn phantom_asks() -> Vec<Level> {
        vec![lvl("0.0730", "26")]
    }

    fn run(q: &mut Quoter, cx: &mut Cx, fees: &FeeSchedule, bb: &BookBuilder) -> Vec<Intent> {
        let mut oid = 0u64;
        let mut intents = Vec::new();
        q.on_book(cx, fees, bb, 1.0, &mut oid, &mut intents);
        intents
    }

    /// THE failure this fix exists to stop: `maker_ask_quote` walks the hedge
    /// ask ladder, whose first level on a crossed book is the phantom. The
    /// quote looks profitable, rests, gets lifted — and the hedge then refuses
    /// at `max_slip 0.01` because 0.073 was never there. Naked short to
    /// resolution.
    #[test]
    fn a_crossed_hedge_book_produces_no_maker_quote() {
        let (mut cx, fees, bb, mut q) = books(vec![lvl("0.1770", "305")], phantom_asks());
        let intents = run(&mut q, &mut cx, &fees, &bb);
        assert!(
            !any_place(&intents),
            "quoted off a crossed book: {intents:?}"
        );
    }

    /// The over-correction guard. Same ask, same sizes, same PM-US book — the
    /// Kalshi BID moves one tick below the ask instead of ten cents above it.
    /// A quote must still come out, or the fix has switched the maker off.
    #[test]
    fn an_uncrossed_book_of_the_same_shape_still_quotes() {
        let (mut cx, fees, bb, mut q) = books(vec![lvl("0.0720", "305")], phantom_asks());
        let intents = run(&mut q, &mut cx, &fees, &bb);
        assert!(
            any_place(&intents),
            "the guard silenced a sane book: {intents:?}"
        );
        assert!(
            !skips(&intents).iter().any(|s| s.contains("crossed book")),
            "a sane book must raise no crossed-book skip: {intents:?}"
        );
    }

    /// A locked book (bid == ask) is refused, per `Book::crossing`: both venues
    /// match on equality, so locked means our copy is stale — and there is no
    /// room to post inside it anyway. One tick apart is NOT locked and trades.
    #[test]
    fn a_locked_book_is_refused_but_a_one_tick_book_is_not() {
        let (mut cx, fees, bb, mut q) = books(vec![lvl("0.0730", "305")], phantom_asks());
        let locked = run(&mut q, &mut cx, &fees, &bb);
        assert!(
            !any_place(&locked),
            "quoted off a locked book: {locked:?}"
        );
        assert!(
            skips(&locked).iter().any(|s| s.contains("bid 0.0730 >= ask 0.0730")),
            "a locked book must say it is locked: {locked:?}"
        );

        let (mut cx, fees, bb, mut q) = books(vec![lvl("0.0720", "305")], phantom_asks());
        let tight = run(&mut q, &mut cx, &fees, &bb);
        assert!(
            any_place(&tight),
            "a one-tick spread is a normal book: {tight:?}"
        );
    }

    /// Attribution (audit C5 item 4): "the venue's book is crossed" must not
    /// look like "there is no edge". A crossed book emits a skip naming the
    /// venue, the market and the inversion; a merely unprofitable book is
    /// silent, which is what the operator sees today for a no-edge market.
    #[test]
    fn the_crossed_skip_is_distinguishable_from_a_no_edge_skip() {
        let (mut cx, fees, bb, mut q) = books(vec![lvl("0.1770", "305")], phantom_asks());
        let crossed = run(&mut q, &mut cx, &fees, &bb);
        let reasons = skips(&crossed);
        assert!(!reasons.is_empty(), "six hours of corruption must log something: {crossed:?}");
        for r in &reasons {
            assert_eq!(
                r,
                &format!("crossed book kalshi {KMKT} bid 0.1770 >= ask 0.0730"),
                "the skip must name venue, market and both prices"
            );
        }

        // No-edge control: a sane Kalshi book that funds nothing (hedging NO
        // costs 1 - 0.0100 = 0.99, so no maker bid clears fees) and offers
        // nothing worth an ask. Silence, and crucially no crossed-book reason.
        let (mut cx, fees, bb, mut q) =
            books(vec![lvl("0.0100", "305")], vec![lvl("0.9900", "305")]);
        let no_edge = run(&mut q, &mut cx, &fees, &bb);
        assert!(
            !skips(&no_edge).iter().any(|s| s.contains("crossed book")),
            "a no-edge book must not be reported as crossed: {no_edge:?}"
        );
    }

    /// Fail closed on a book that goes bad UNDER a resting quote: we can no
    /// longer verify the hedge, so the quote comes off. The crossed check sits
    /// ahead of pricing precisely so it falls into the existing cancel path.
    #[test]
    fn a_book_that_crosses_under_a_resting_quote_pulls_it() {
        let (mut cx, fees, mut bb, mut q) = books(vec![lvl("0.0720", "305")], phantom_asks());
        let placed = run(&mut q, &mut cx, &fees, &bb);
        assert!(any_place(&placed), "{placed:?}");

        // the 2026-07-28 corruption arrives: a bid above the standing ask
        bb.apply_snapshot(Venue::Kalshi, KMKT, vec![lvl("0.1770", "305")], phantom_asks(),
                          2, 2_000_000_000, None);
        let mut oid = 99u64;
        let mut intents = Vec::new();
        q.on_book(&mut cx, &fees, &bb, 2.0, &mut oid, &mut intents);
        assert!(
            any_cancel(&intents),
            "a quote resting on a corrupt book must be pulled: {intents:?}"
        );
        assert!(
            !any_place(&intents),
            "and nothing may be re-posted: {intents:?}"
        );
    }

    /// `hedge_anchor` (bins/arb-trader/src/engine.rs) is the OTHER blind
    /// `asks.first()` read in C5. It lives outside this crate, so what is
    /// pinned here is the primitive its fix uses and the reason the fix works:
    /// on the crossed book `asks.first()` IS the phantom, and filtering on
    /// `is_crossed()` is what stops the anchor from being minted at 0.073 —
    /// a price the hedge could then never reach inside `max_slip 0.01`.
    ///
    /// The engine-side one-liner (owned by the engine batch, see the B4a
    /// report) is:
    ///   let book = books.get(hedge.venue, &hedge.market_id).filter(|b| !b.is_crossed())?;
    #[test]
    fn the_anchor_primitive_refuses_a_phantom_asks_first() {
        let (_cx, _fees, bb, _q) = books(vec![lvl("0.1770", "305")], phantom_asks());
        let book = bb.get(Venue::Kalshi, KMKT).expect("book");

        // what engine.rs does today
        assert_eq!(book.asks.first().map(|l| l.price.as_str()), Some("0.0730"),
                   "the phantom is best-ask, as asks sort ascending");
        // what the one-liner does
        assert!(Some(book).filter(|b| !b.is_crossed()).is_none(),
                "the filter must refuse the crossed book");

        // and on the true book (bid 0.1760 / ask 0.1820) it anchors normally
        let (_cx, _fees, bb, _q) =
            books(vec![lvl("0.1760", "305")], vec![lvl("0.1820", "305")]);
        let book = bb.get(Venue::Kalshi, KMKT).expect("book");
        assert_eq!(
            Some(book)
                .filter(|b| !b.is_crossed())
                .and_then(|b| b.asks.first())
                .map(|l| l.price.as_str()),
            Some("0.1820")
        );
    }
}
