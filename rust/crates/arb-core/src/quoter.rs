//! Maker-quoter policy — port of src/arbbot/exec/quoter.py under the
//! intent-parity gate (scripts/intent_replay.py is the Python reference).
//!
//! Harness conventions (both sides identical): jitter knobs 0 (exact-match
//! deadband, no random), clip 5, min_requote_s 15.0 of TAPE time, default
//! Market metadata (tick 0.01), risk always allows, kill switch off, mock
//! gateway with globally sequential ids "m1","m2",... Canonical intent lines
//! are serde_json objects (BTreeMap => sorted keys, compact separators,
//! integral floats printed with ".0" — matching Python json.dumps).

use crate::book::BookBuilder;
use crate::fees::FeeSchedule;
use crate::model::Venue;
use crate::scan::{maker_ask_quote, maker_quote, Cx, MarketMeta, Rel, RelType, D};
use serde_json::json;
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
    pub markets: HashMap<String, HashMap<String, f64>>,
}

impl Toxgate {
    fn score(&self, market_id: &str, side: &str, now: f64) -> Option<f64> {
        if now - self.ts > TOXGATE_MAX_AGE {
            return None; // stale — fail open
        }
        self.markets.get(market_id)?.get(side).copied()
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
    suppress: HashSet<(String, &'static str)>,
    /// Toxicity gate feed; None => gate off (exact original behavior).
    toxgate: Option<Arc<Toxgate>>,
    risk: Option<Arc<dyn RiskGate>>,
    resting: HashMap<(usize, &'static str), RestingQuote>,
    last_quote_ts: HashMap<(usize, &'static str), f64>,
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

    pub fn set_suppress(&mut self, pairs: HashSet<(String, &'static str)>) {
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
        side: &'static str,
    ) -> Option<D> {
        let leg = &self.rel.legs[i];
        let book = books.get(leg.venue, &leg.market_id)?;
        let rq = self.resting.get(&(i, side));
        let levels = if side == "bid" { &book.bids } else { &book.asks };
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
        side: &'static str,
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
        if side == "bid" {
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

    fn hedge_has_depth(
        &self,
        cx: &mut Cx,
        books: &BookBuilder,
        i: usize,
        side: &'static str,
    ) -> bool {
        let hedge_leg = &self.rel.legs[1 - i];
        let Some(b) = books.get(hedge_leg.venue, &hedge_leg.market_id) else {
            return false;
        };
        let lvl = if side == "bid" { b.bids.first() } else { b.asks.first() };
        match lvl {
            Some(l) => {
                let Some(sz) = cx.parse(&l.size) else { return false };
                let clip = cx.from_i64(self.clip);
                cx.cmp(sz, clip) != Ordering::Less
            }
            None => false,
        }
    }

    fn venue_str(v: Venue) -> &'static str {
        v.as_str()
    }

    /// Emit price exactly like Python str(Decimal) of the quantized value.
    fn px(cx: &mut Cx, p: D) -> String {
        let _ = cx;
        p.to_standard_notation_string()
    }

    /// Cancel every resting quote (kill switch / shutdown), emitting cancel
    /// intents in deterministic (leg, side) order. Mirrors Python cancel_all.
    pub fn cancel_all(&mut self, cx: &mut Cx, now: f64, intents: &mut Vec<String>) {
        let mut keys: Vec<(usize, &'static str)> = self.resting.keys().copied().collect();
        keys.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(b.1)));
        for key in keys {
            let curq = self.resting.remove(&key).expect("key from keys()");
            let leg = &self.rel.legs[key.0];
            intents.push(
                json!({"ts": now, "cancel": leg.market_id.clone(),
                        "venue": Self::venue_str(leg.venue), "side": key.1,
                        "price": Self::px(cx, curq.price), "order_id": curq.order_id})
                .to_string(),
            );
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
        intents: &mut Vec<String>,
    ) {
        let leg_indices = maker_leg_indices(self.rel.rtype);
        for i in leg_indices {
            for side in ["bid", "ask"] {
                let key = (i, side);
                let mut target = self.target(cx, fees, books, i, side);
                if target.is_some() && !self.hedge_has_depth(cx, books, i, side) {
                    target = None;
                }
                let leg_venue = Self::venue_str(self.rel.legs[i].venue);
                let leg_market = self.rel.legs[i].market_id.clone();
                // toxgate (card 059ce700): a toxic (market, side) is unviable —
                // emit the skip record, cancel any resting quote, rest nothing.
                // Advisory + fail-open: missing/stale feed never blocks.
                if target.is_some() {
                    if let Some(tox) =
                        self.toxgate.as_ref().and_then(|t| t.score(&leg_market, side, now))
                    {
                        if tox > TOXGATE_MAX {
                            intents.push(
                                json!({"ts": now,
                                       "skip": [format!("toxgate {side} {tox:.3} > {TOXGATE_MAX}")]})
                                .to_string(),
                            );
                            target = None;
                        }
                    }
                }
                let Some(target) = target else {
                    if let Some(curq) = self.resting.remove(&key) {
                        intents.push(
                            json!({"ts": now, "cancel": leg_market, "venue": leg_venue,
                                    "side": side, "price": Self::px(cx, curq.price),
                                    "order_id": curq.order_id})
                            .to_string(),
                        );
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
                        intents.push(json!({"ts": now, "skip": v.reasons}).to_string());
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
                let mut evt = json!({"ts": now, "place": leg_market, "venue": leg_venue,
                                     "side": side, "price": price_s, "count": self.clip,
                                     "order_id": oid});
                if let Some((roid, oldp)) = &replaced {
                    evt["replaces"] = json!(roid);
                    evt["old_price"] = json!(oldp);
                }
                intents.push(evt.to_string());
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
        assert!(intents[0].contains(r#""place":"P""#), "{}", intents[0]);
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
        assert!(intents.iter().any(|i| i.contains(r#""cancel":"P""#)),
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
        assert!(intents.iter().any(|i| i.contains(r#""place":"P""#)),
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
        assert!(intents[0].contains(r#""place":"P""#));
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
        assert!(intents[0].contains(r#""skip""#), "{}", intents[0]);
        assert!(intents[0].contains("tail cap"), "{}", intents[0]);
        assert!(!intents[0].contains("place"), "nothing may be placed: {}", intents[0]);
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
        assert!(intents[0].contains(r#""place":"P""#), "{}", intents[0]);

        // now risk refuses, and the book moves enough to want a reprice
        q.set_risk(Some(std::sync::Arc::new(Always(false))));
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);

        assert!(
            intents.iter().any(|i| i.contains(r#""skip""#)),
            "expected a skip: {intents:?}"
        );
        assert!(
            !intents.iter().any(|i| i.contains(r#""cancel""#)),
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
        assert!(intents[0].contains(r#""place""#));

        q.set_risk(Some(std::sync::Arc::new(Always(false))));
        intents.clear();
        // K collapses => the PM quote is unviable
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.30", "500")],
                          vec![lvl("0.99", "1")], 2, 131_000_000_000, None);
        q.on_book(&mut cx, &fees, &bb, 131.0, &mut oid, &mut intents);

        assert!(
            intents.iter().any(|i| i.contains(r#""cancel":"P""#)),
            "an unviable quote must still cancel: {intents:?}"
        );
    }
}
