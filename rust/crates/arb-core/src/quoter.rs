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

/// Mirror of quoter.py TOXGATE_MAX / TOXGATE_MAX_AGE. Public because the
/// binary that loads the feed decides whether it is usable BEFORE installing
/// it, and both halves must answer to the same numbers.
pub const TOXGATE_MAX: f64 = 0.03;
pub const TOXGATE_MAX_AGE: f64 = 120.0;

/// How far AHEAD of the reader's clock a feed's `ts` may sit before it stops
/// being clock jitter and starts being a corrupt timestamp. Both processes
/// share a machine, so a real skew here is seconds at most; the bound exists
/// because the failure it catches is unbounded in the permissive direction.
pub const TOXGATE_MAX_SKEW: f64 = 5.0;

/// How long a resting quote may want a DIFFERENT price, continuously, while the
/// risk gate refuses the replacement, before the quote is pulled instead of
/// frozen. Same clock as `min_requote_s` (the book event's `ts_local_ns` live,
/// tape time in replay).
///
/// 2x `min_requote_s`. The quoter's own unit of "a price that has settled" is
/// one requote window, so a refusal that outlives TWO of them is not a blip
/// between a fill and its offset — it is the state of the account. Below that
/// the old rule holds and the quote stays: a cap refusal is not, on its own,
/// evidence that the resting price is wrong.
///
/// BE CLEAR ABOUT WHAT THIS BUYS. It is NOT the anti-churn bound — that comes
/// from `resting.remove` at the pull and the single site that refills
/// `resting`, and holds identically at 0s. What the delay buys is not going
/// flat on a cap breach that lasts one event, and what it COSTS is up to 30s of
/// a live quote at a price this quoter has already rejected. That is the whole
/// trade; 30s is a judgement about which side of it hurts more, not a
/// measurement.
const CAP_REFUSAL_PULL_S: f64 = 30.0;

/// The research toxicity shadow feed (data/exec/toxgate.json), loaded by the
/// caller (file I/O stays outside arb-core). Mirrors quoter.py `_toxgate`
/// EXCEPT that a stale feed (doc ts older than TOXGATE_MAX_AGE vs `now`) no
/// longer fails open — see [`ToxVerdict::Untrusted`].
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
    ///
    /// `rests_on` is the (market, side) the order would REST on, and an ALLOWED
    /// check RESERVES `notional` against it. Without that this call is a pure
    /// read: the quoter consults the gate once per (leg, side) and then rests
    /// the order, and every other resting quote in the process consults the
    /// same unchanged number — so a cap with headroom for one clip authorises
    /// one clip per quoting side, all live at the venue at once. `None` is an
    /// order that does NOT rest (a marketable IOC), which reserves nothing.
    ///
    /// A slot that already holds a reservation does not count against itself:
    /// an amend replaces the quote on its own slot rather than adding to it.
    fn check(
        &self,
        rel: &Rel,
        venue: Venue,
        notional: i64,
        rests_on: Option<(&str, BookSide)>,
    ) -> RiskVerdict;

    /// The quote reserved on this (market, side) is no longer resting, so give
    /// its capital back. A slot holding no reservation is a no-op, which is
    /// what makes this safe to call from every path a quote can leave by — a
    /// reservation that leaks is a cap that ratchets shut and silently stops
    /// all trading, which is worse than the bug it fixes.
    fn release(&self, rel_id: &str, market_id: &str, side: BookSide);
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

/// What the toxicity feed says about one (market, side) — including the case
/// where it can say nothing at all.
#[derive(Debug, PartialEq)]
pub enum ToxVerdict {
    /// Scored at or under `TOXGATE_MAX`, or carried by a current feed that
    /// simply does not list this market. Absence from a CURRENT feed is not
    /// evidence of toxicity; absence of the feed is a different answer.
    Clear,
    /// Expected adverse cost per contract, above `TOXGATE_MAX`.
    Toxic(f64),
    /// This side IS scored by the model, and the score in hand is `age_s`
    /// behind the caller's clock.
    ///
    /// Stale used to be `None` — indistinguishable from "not listed", i.e. a
    /// free pass. `Toxgate` carries ONE `ts`, captured when the file was read,
    /// so a gate nothing reloads goes permanently open TOXGATE_MAX_AGE after
    /// startup: two minutes of gating and then silence. The caller that owns
    /// the file owns reloading it; what this type owes is refusing to pretend
    /// a stale opinion is a current one.
    ///
    /// Reversing Python's fail-open (`quoter.py:61`, deliberate and
    /// documented) is a POLICY change and is meant as one. What is not a
    /// policy change is the scope: only a side the model actually covers is
    /// withheld. See `verdict` for why that distinction carries the whole
    /// weight here.
    Untrusted { age_s: f64 },
}

impl Toxgate {
    /// Parse `data/exec/toxgate.json`. `None` is a document this gate cannot
    /// read at all — including one with no numeric `ts`, because a feed that
    /// cannot say how old it is cannot be trusted to be current.
    pub fn from_json(doc: &str) -> Option<Toxgate> {
        let v: serde_json::Value = serde_json::from_str(doc).ok()?;
        let ts = v.get("ts")?.as_f64()?;
        let mut markets: HashMap<String, HashMap<String, f64>> = HashMap::new();
        if let Some(ms) = v.get("markets").and_then(|x| x.as_object()) {
            for (mid, sides) in ms {
                let mut by_side = HashMap::new();
                if let Some(so) = sides.as_object() {
                    for (side, score) in so {
                        if let Some(f) = score.as_f64() {
                            by_side.insert(side.clone(), f);
                        }
                    }
                }
                markets.insert(mid.clone(), by_side);
            }
        }
        Some(Toxgate { ts, markets })
    }

    /// One consultation. `now` is the caller's clock in the feed's own units
    /// (epoch seconds).
    ///
    /// THE LOOKUP COMES FIRST, and that ordering is the whole proportionality
    /// of this gate. Checking the age first makes a stale feed refuse every
    /// side in the book, including every side the model has never had an
    /// opinion about — and it never will have one about most of them, because
    /// this feed is Kalshi-only by construction. Measured on the live registry:
    /// of the 80 legs across the 40 quoting relationships, 42 are Kalshi (35
    /// scored) and 38 are Polymarket / PM-US (0 scored, ever). Age-first
    /// withholds those 38 for no safety gain at all.
    ///
    /// So: a (market, side) the document does not carry is `Clear` whatever
    /// the age — silence from a model is not a refusal by it. A side the model
    /// DOES cover is only ever quoted on a CURRENT opinion.
    ///
    /// A feed stamped SLIGHTLY in the future stays usable — the research
    /// writer and this engine share a machine, so a fractionally-ahead `ts` is
    /// float rounding, and withholding scored sides over it is the wrong
    /// failure. Past `TOXGATE_MAX_SKEW` it is not rounding, and the direction
    /// matters: an unbounded future `ts` is PERMANENTLY current, which
    /// silently disables `tox_tick`, the staleness gauge and every `Untrusted`
    /// verdict at once. That is a gate frozen open — the exact defect this
    /// type exists to prevent, arrived at from the other side. A writer that
    /// starts emitting milliseconds is all it takes.
    pub fn verdict(&self, market_id: &str, side: BookSide, now: f64) -> ToxVerdict {
        let Some(score) = self.markets.get(market_id).and_then(|m| m.get(side.as_str())).copied()
        else {
            return ToxVerdict::Clear; // not covered by the model
        };
        let age_s = now - self.ts;
        if !(-TOXGATE_MAX_SKEW..=TOXGATE_MAX_AGE).contains(&age_s) {
            return ToxVerdict::Untrusted { age_s };
        }
        if score > TOXGATE_MAX {
            return ToxVerdict::Toxic(score);
        }
        ToxVerdict::Clear
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
    /// When this (leg, side) FIRST wanted a price the risk gate would not let it
    /// post. Set on the refusal, cleared the moment either half stops being
    /// true — the gate allows again, or the book comes back to the resting
    /// price — so it measures one continuous run of "mispriced AND refused";
    /// see [`CAP_REFUSAL_PULL_S`].
    ///
    /// Also cleared wherever the QUOTE is destroyed (this pull, the unviable
    /// cancel, `cancel_all`), because it times a quote and an entry outliving
    /// its quote is a lie about how long the next one has been wrong. Nothing
    /// today could act on such an entry — only a place refills `resting`, and a
    /// place clears this — but a reconciliation path that adopted a live venue
    /// order into `resting` would pull it on its first refusal, having never
    /// been mispriced for a second.
    refused_since: HashMap<(usize, BookSide), f64>,
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
            refused_since: HashMap::new(),
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
            // RELEASE. This has exactly two production callers — the kill
            // switch and the feed-stale pull — and capital reserved for a quote
            // either of them is pulling is capital nothing will ever spend. NOT
            // the shutdown sweep, which is venue-level and reaches no quoter;
            // there the process is exiting and the map goes with it.
            if let Some(gate) = &self.risk {
                gate.release(&self.rel.id, &self.rel.legs[key.0].market_id, key.1);
            }
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
            self.refused_since.remove(&key);
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
            // (Contrast the risk gate below, which cancels only on DURATION —
            // a cap refusal is a reason not to add exposure, and on its own is
            // not evidence that the resting price is wrong.)
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
                // A COVERED side whose score has gone stale gets the same
                // answer, for the same reason: it is a side the model has an
                // opinion about and we no longer know what that opinion is.
                // A side the model does not cover is unaffected either way
                // (`verdict`), and no gate installed at all is still off,
                // which is what the golden and intent replays pin.
                //
                // Both reasons are emitted per event for as long as they hold,
                // which is the `crossed book` precedent above: a persistent
                // fault that produced no record is how six hours of corruption
                // went unnoticed.
                if target.is_some() {
                    let why = match self.toxgate.as_ref() {
                        None => None,
                        // `as_str`, not `{side:?}`: these reason strings reach
                        // the intents file and the digest, and Debug would
                        // spell it `Bid`.
                        Some(t) => match t.verdict(&leg_market, side, now) {
                            ToxVerdict::Clear => None,
                            ToxVerdict::Toxic(v) => {
                                Some(format!("toxgate {} {v:.3} > {TOXGATE_MAX}", side.as_str()))
                            }
                            ToxVerdict::Untrusted { age_s } => Some(format!(
                                "toxgate feed {age_s:.0}s old > {TOXGATE_MAX_AGE} — \
                                 cannot score {}",
                                side.as_str()
                            )),
                        },
                    };
                    if let Some(why) = why {
                        intents.push(Intent::Skip(intent::Skip { skip: vec![why], ts: now }));
                        target = None;
                    }
                }
                let Some(target) = target else {
                    if let Some(curq) = self.resting.remove(&key) {
                        // RELEASE. This is the ordinary way a quote goes away —
                        // the book moved, the hedge thinned, the side went
                        // toxic — and it is the release path everything else
                        // funnels through, `cancel_all` included.
                        if let Some(gate) = &self.risk {
                            gate.release(&self.rel.id, &leg_market, side);
                        }
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
                        // `refused_since` is NOT kept: it times a quote, and
                        // this one no longer exists.
                        self.refused_since.remove(&key);
                    }
                    continue;
                };
                // hysteresis: exact-match hold (deadband 0)
                if let Some(curq) = self.resting.get(&key) {
                    if cx.cmp(curq.price, target) == Ordering::Equal {
                        // The book agrees with what we rest, so this quote is
                        // not stale whatever risk thinks — the pull clock
                        // restarts from the next time it drifts.
                        self.refused_since.remove(&key);
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
                //
                // ...but only for CAP_REFUSAL_PULL_S. Reaching this line means
                // the quoter wants to MOVE this price (hysteresis and throttle
                // are both behind us), and a refusal that persists leaves the
                // old price resting and fillable forever: the refusal never
                // touches `last_quote_ts`, so every later book event lands here
                // too.
                //
                // Live on the armed engine, measured 2026-07-29T09:53 (the file
                // is still growing): 354,961 `class cap` skips over 11h55m,
                // longest UNBROKEN run 152,914 (00:53:50 -> 02:44:15), last
                // place 00:53:50, last cancel 22:05:00 — 11h48m in which the
                // engine refused continuously and cancelled nothing. The runs
                // are broken only by other skips, 27,038 of them `crossed book`
                // on KXRATECUT-26DEC31; that path (`crossed` above) forces
                // `target` None and cancels unconditionally, which is why the
                // live blast radius is smaller than the skip count suggests —
                // the market generating most of the interleaving is one where
                // nothing was left resting anyway.
                //
                // WHAT THIS FIXES, precisely: a quote resting at a price the
                // quoter has REJECTED. It does not, and cannot, stop a fill on
                // a correctly-priced quote from carrying exposure past a
                // breached cap — such a quote is held by the hysteresis check
                // above and never reaches this line. Pulling those too would be
                // cancel-all-on-cap-breach, a different and much larger policy.
                //
                // "The cap refuses more exposure" and "this resting price is
                // stale" are different facts. The first is not grounds to
                // cancel and never becomes so; the second is, and it is what
                // the duration measures. A cancel is a REDUCTION of exposure,
                // which no cap has an interest in refusing.
                if let Some(gate) = &self.risk {
                    let v = gate.check(
                        &self.rel,
                        self.rel.legs[i].venue,
                        self.clip,
                        // What this quote will rest on, so the gate can hold
                        // the capital until it fills or comes off.
                        Some((&leg_market, side)),
                    );
                    if !v.allowed {
                        intents.push(Intent::Skip(intent::Skip { skip: v.reasons, ts: now }));
                        let since = *self.refused_since.entry(key).or_insert(now);
                        // At most ONE cancel per resting quote, and it is
                        // `resting.remove` that bounds it, not the delay above:
                        // only a place refills `resting`, and that needs the
                        // gate to ALLOW (clearing the clock below). So a jam
                        // that never lifts costs one cancel per side however
                        // many events it spans.
                        if now - since >= CAP_REFUSAL_PULL_S {
                            if let Some(curq) = self.resting.remove(&key) {
                                // RELEASE, and this is the release path that
                                // matters most: the quote is being pulled
                                // BECAUSE the caps are jammed, so its capital
                                // is the headroom the next quote needs. The
                                // refusal above reserved nothing (only an
                                // ALLOWED check does), so what comes back here
                                // is the reservation the PLACE took.
                                gate.release(&self.rel.id, &leg_market, side);
                                intents.push(Intent::Cancel(intent::Cancel {
                                    cancel: leg_market,
                                    order_id: curq.order_id,
                                    price: Self::px(cx, curq.price),
                                    side,
                                    ts: now,
                                    venue: leg_venue,
                                }));
                                // `last_quote_ts` is left alone, but do NOT
                                // read that as a re-entry throttle the way the
                                // unviable-quote cancel above gets one. That
                                // cancel sits BEFORE the throttle; this one is
                                // past it, so reaching here already proved
                                // `now >= last + min_requote_s` and the pull is
                                // a further CAP_REFUSAL_PULL_S after that. Once
                                // the cap lifts this side re-posts on the next
                                // book event, which is what we want: the quote
                                // came off for want of headroom, not because
                                // the venue was churning us.
                                self.refused_since.remove(&key);
                            }
                        }
                        continue;
                    }
                    self.refused_since.remove(&key);
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

    /// A feed with a score for this side, `age` seconds behind `now`.
    fn aged_gate(age: f64, score: f64) -> Arc<Toxgate> {
        Arc::new(Toxgate {
            ts: 100.0 - age,
            markets: HashMap::from([(
                "P".to_string(),
                HashMap::from([("bid".to_string(), score)]),
            )]),
        })
    }

    /// **A stale opinion on a covered side is not a permit.**
    ///
    /// `Toxgate` carries one `ts`, captured when the file was read. Nothing
    /// reloaded it, and `score` answered a feed older than TOXGATE_MAX_AGE with
    /// `None` — the same answer as "this market is not listed", i.e. quote
    /// freely. So an installed gate gated for exactly two minutes and then
    /// silently stopped, forever.
    ///
    /// The score here is 0.001, WELL under `TOXGATE_MAX`: what is refused is
    /// not this number but our right to still be relying on it.
    #[test]
    fn a_stale_score_blocks_instead_of_permitting() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        q.set_toxgate(Some(aged_gate(TOXGATE_MAX_AGE + 1.0, 0.001)));
        let mut oid = 0u64;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);

        assert!(!any_place(&intents), "a stale opinion must not permit: {intents:?}");
        assert!(
            skips(&intents).iter().any(|s| s.contains("toxgate feed 121s old")),
            "and it must say the AGE is what refused: {intents:?}"
        );
    }

    /// The boundary, both directions, same side and same score: one second
    /// inside TOXGATE_MAX_AGE quotes normally, one second outside does not. So
    /// the refusal above is the age and nothing else.
    #[test]
    fn a_score_inside_the_max_age_still_quotes() {
        for (age, want_place) in [(TOXGATE_MAX_AGE - 1.0, true), (TOXGATE_MAX_AGE + 1.0, false)] {
            let (mut cx, fees, mut bb, mut q) = fixture();
            q.set_toxgate(Some(aged_gate(age, 0.001)));
            let mut oid = 0u64;
            let mut intents = Vec::new();
            pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
            q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
            assert_eq!(any_place(&intents), want_place, "age {age}: {intents:?}");
        }
    }

    /// **A `ts` in the future is a gate frozen OPEN.**
    ///
    /// `now - ts` was tested one-sidedly, so a forward-corrupted timestamp — a
    /// writer that starts emitting milliseconds is enough — makes the feed
    /// permanently current: `tox_tick` never reports it, the staleness gauge
    /// never sets, and every scored side is waved through on a number nobody
    /// is updating. That is this PR's own defect reached from the other side,
    /// which is why the bound is two-sided and why the slack is seconds.
    #[test]
    fn a_feed_stamped_far_in_the_future_is_untrusted_not_permanently_current() {
        // Milliseconds mistaken for seconds: ts ~1000x `now`.
        let ms = Toxgate {
            ts: 100.0 * 1000.0,
            markets: HashMap::from([(
                "P".to_string(),
                HashMap::from([("bid".to_string(), 0.001)]),
            )]),
        };
        assert!(
            matches!(ms.verdict("P", BookSide::Bid, 100.0), ToxVerdict::Untrusted { .. }),
            "a far-future ts must not read as current"
        );
        // ...but ordinary jitter between two processes on one machine does not
        // pull the book: inside the skew slack it is still a usable score.
        let jitter = Toxgate {
            ts: 100.0 + TOXGATE_MAX_SKEW / 2.0,
            markets: HashMap::from([(
                "P".to_string(),
                HashMap::from([("bid".to_string(), 0.001)]),
            )]),
        };
        assert_eq!(jitter.verdict("P", BookSide::Bid, 100.0), ToxVerdict::Clear);
    }

    /// **PROPORTIONALITY — the side the model never covered keeps quoting.**
    ///
    /// The age check used to come first, which made a stale feed withhold
    /// every side in the book. But this feed is Kalshi-only by construction:
    /// across the 40 quoting relationships' 80 legs, 42 are Kalshi (35 scored)
    /// and 38 are Polymarket / PM-US — 0 of which this model has EVER scored.
    /// Age-first withheld those 38 for no safety gain whatsoever.
    ///
    /// The fixture quotes market `P`, which the document below does not carry.
    /// Same three-day-stale feed as the test above; opposite answer, because
    /// silence from a model is not a refusal by it.
    #[test]
    fn a_stale_feed_does_not_withhold_a_side_it_never_scored() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        q.set_toxgate(Some(Arc::new(Toxgate {
            ts: 100.0 - 3.0 * 86_400.0,
            markets: HashMap::from([(
                // a Kalshi ticker, the only venue this feed carries
                "KXALIENS-27".to_string(),
                HashMap::from([("bid".to_string(), 0.0584)]),
            )]),
        })));
        let mut oid = 0u64;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);

        assert!(any_place(&intents), "an unscored side must keep quoting: {intents:?}");
        assert!(skips(&intents).is_empty(), "and raise nothing: {intents:?}");
    }

    /// The parser both binaries load the research file through. It was written
    /// out longhand in `arb-intent` with `ts` defaulting to 0.0 on a document
    /// that has none — which, now that age is what refuses, would read as a
    /// feed from 1970 rather than as an unreadable one. Here it is `None`, and
    /// the caller decides what an unreadable feed means.
    #[test]
    fn the_feed_parser_refuses_a_document_with_no_timestamp() {
        let good = Toxgate::from_json(
            r#"{"ts": 1785079574.72, "markets": {"K1": {"bid": 0.0822, "ask": 0.01}}}"#,
        )
        .expect("a well-formed feed");
        assert_eq!(good.ts, 1785079574.72);
        assert_eq!(
            good.verdict("K1", BookSide::Bid, 1785079574.72),
            ToxVerdict::Toxic(0.0822)
        );
        assert_eq!(good.verdict("K1", BookSide::Ask, 1785079574.72), ToxVerdict::Clear);
        // An uncovered market is Clear even three days on — the lookup runs
        // before the age check, deliberately.
        let old = 1785079574.72 + 3.0 * 86_400.0;
        assert_eq!(good.verdict("nope", BookSide::Bid, old), ToxVerdict::Clear);
        assert!(matches!(
            good.verdict("K1", BookSide::Bid, old),
            ToxVerdict::Untrusted { .. }
        ));

        assert!(Toxgate::from_json(r#"{"markets": {}}"#).is_none(), "no ts");
        assert!(Toxgate::from_json("not json").is_none());
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

    /// Allows or refuses unconditionally, and RECORDS what was reserved and
    /// released against it — the quoter owns both calls, so this is where the
    /// pairing is pinned.
    struct Always {
        allow: std::sync::atomic::AtomicBool,
        slots: std::sync::Mutex<Vec<String>>,
    }

    impl Always {
        fn new(allow: bool) -> std::sync::Arc<Always> {
            std::sync::Arc::new(Always {
                allow: std::sync::atomic::AtomicBool::new(allow),
                slots: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn allows(&self) -> bool {
            self.allow.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Flip the verdict on a gate the quoter already holds — the caps
        /// filling up under a quote that is already resting, which is the only
        /// way to reserve first and refuse afterwards.
        fn set(&self, allow: bool) {
            self.allow.store(allow, std::sync::atomic::Ordering::Relaxed);
        }

        fn log(&self) -> Vec<String> {
            self.slots.lock().expect("slots").clone()
        }
    }

    impl RiskGate for Always {
        fn check(
            &self,
            _rel: &Rel,
            _venue: Venue,
            _notional: i64,
            rests_on: Option<(&str, BookSide)>,
        ) -> RiskVerdict {
            let allow = self.allows();
            if allow {
                if let Some((m, s)) = rests_on {
                    self.slots.lock().expect("slots").push(format!("+{m}/{}", s.as_str()));
                }
            }
            RiskVerdict {
                allowed: allow,
                reasons: if allow { vec![] } else { vec!["per-relationship tail cap".into()] },
            }
        }

        fn release(&self, _rel_id: &str, market_id: &str, side: BookSide) {
            self.slots.lock().expect("slots").push(format!("-{market_id}/{}", side.as_str()));
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
        q.set_risk(Some(Always::new(false)));
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
        q.set_risk(Some(Always::new(false)));
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);

        assert!(!skips(&intents).is_empty(), "expected a skip: {intents:?}");
        assert!(
            cancel_on(&intents, "P").is_none(),
            "the resting quote must NOT be pulled: {intents:?}"
        );
    }

    /// **A cap that stays breached must PULL the quote it has frozen.**
    ///
    /// The refusal above `continue`s past the cancel/replace block, and it is
    /// reached exactly when the quoter has decided the resting price is WRONG
    /// (hysteresis and throttle are both behind it). Nothing there touches
    /// `last_quote_ts`, so once the cap is breached every later book event lands
    /// on the same `continue` and the old price rests forever. Live on the armed
    /// engine, measured 2026-07-29T09:53: 354,961 `class cap` skips over 11h55m,
    /// longest unbroken run 152,914, last place 00:53:50, last cancel 22:05:00 —
    /// 11h48m refusing continuously and cancelling nothing. A counterparty
    /// hitting one of those quotes buys a basket whose hedge has moved, and the
    /// fill takes exposure past the cap that was refusing every new order.
    ///
    /// The first refusal still holds the quote (that is the test below this
    /// one's job); what changes is that the refusal is no longer allowed to
    /// hold it indefinitely.
    #[test]
    fn a_cap_refusal_that_outlives_the_pull_delay_pulls_the_stale_quote() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        let (roid, rpx) = {
            let p = place_on(&intents, "P").expect("the entry place");
            (p.order_id.clone(), p.price.clone())
        };

        // capital is now fully deployed: the class cap refuses everything
        q.set_risk(Some(Always::new(false)));

        // the hedge drifts and the resting price stops clearing the edge. The
        // FIRST refusal could still be a blip between a fill and its offset, so
        // the quote stays — see the anti-churn test below.
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);
        assert!(!skips(&intents).is_empty(), "expected the cap skip: {intents:?}");
        assert!(
            cancel_on(&intents, "P").is_none(),
            "a single refusal must not pull: {intents:?}"
        );

        // ...the same refusal one pull-delay later is not a blip.
        intents.clear();
        pm_bid(&mut bb, "0.34", 3, 160_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0 + CAP_REFUSAL_PULL_S, &mut oid, &mut intents);
        let c = cancel_on(&intents, "P").expect("the frozen quote must be pulled");
        assert_eq!(c.order_id, roid, "and it must name the resting order: {intents:?}");
        assert_eq!(c.price, rpx);
        assert!(!any_place(&intents), "the cap still refuses a replacement: {intents:?}");
    }

    /// **The cap refusing MORE exposure is not evidence this price is stale.**
    ///
    /// The two facts the old code conflated. The clock the test above runs is a
    /// clock on being MISPRICED under refusal, so a book that comes back to what
    /// we rest restarts it: at that moment the quote is exactly the quote the
    /// quoter would post, and pulling it would be pure churn.
    #[test]
    fn a_book_that_agrees_with_the_resting_price_restarts_the_pull_clock() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");
        q.set_risk(Some(Always::new(false)));

        // mispriced and refused: the clock starts here
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_none(), "{intents:?}");

        // INSIDE the delay the book returns to the price we rest. The cap is
        // still refusing, and this quote is still the right one.
        intents.clear();
        pm_bid(&mut bb, "0.30", 3, 155_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 155.0, &mut oid, &mut intents);
        assert!(intents.is_empty(), "a correctly-priced quote is not even asked about: {intents:?}");

        // it drifts away again PAST the original deadline — but this run of
        // being mispriced is one event old, so the quote stays.
        intents.clear();
        pm_bid(&mut bb, "0.34", 4, 165_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 165.0, &mut oid, &mut intents);
        assert!(
            cancel_on(&intents, "P").is_none(),
            "the clock must restart from the new mispricing: {intents:?}"
        );

        // ...and a full delay from THAT point, it goes.
        intents.clear();
        pm_bid(&mut bb, "0.34", 5, 196_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 165.0 + CAP_REFUSAL_PULL_S, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_some(), "{intents:?}");
    }

    /// **THE ANTI-CHURN BOUND: one cancel per resting quote, not one per event.**
    ///
    /// Pulling on every book event of a jam is its own failure — the venue sees
    /// churn and the maker loses queue position permanently. The bound is
    /// structural rather than a second timer: the pull empties `resting`, and
    /// only a PLACE refills it, which needs the gate to allow (clearing the
    /// clock) and to clear `min_requote_s`. So the ceiling under a cap that
    /// never lifts is exactly one cancel per (leg, side) for the whole jam,
    /// however many events it spans.
    ///
    /// The skips keep coming for all of them: a persistent refusal that produced
    /// no record is how eleven hours of this went unnoticed.
    #[test]
    fn a_cap_that_never_lifts_pulls_a_side_once_and_not_once_per_event() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");
        q.set_risk(Some(Always::new(false)));

        // 500 book events over 5000s of jam, all wanting the same wrong price
        intents.clear();
        let n = 500;
        for k in 0..n {
            let now = 130.0 + 10.0 * k as f64;
            pm_bid(&mut bb, "0.34", 2 + k as u64, (now * 1e9) as i64);
            q.on_book(&mut cx, &fees, &bb, now, &mut oid, &mut intents);
        }
        let cancels = intents.iter().filter(|i| matches!(i, Intent::Cancel(_))).count();
        assert_eq!(cancels, 1, "the jam must cost ONE cancel, not {cancels}");
        assert!(!any_place(&intents), "and place nothing under the cap: {intents:?}");
        assert_eq!(skips(&intents).len(), n, "every refusal must still be recorded");
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

        q.set_risk(Some(Always::new(false)));
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

    // ---- the two release paths the QUOTER owns ----
    //
    // A quote reserves capital when it is allowed and rests. Everything below
    // is about giving it back: a reservation that leaks is a cap that ratchets
    // shut and silently stops all trading, which is worse than the overshoot it
    // exists to bound.

    /// RELEASE ON CANCEL. The book stops funding the quote, the quoter pulls
    /// it, and the capital it was holding goes back — reserved and released
    /// name the same slot.
    #[test]
    fn a_cancelled_quote_releases_the_slot_it_reserved() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let gate = Always::new(true);
        q.set_risk(Some(gate.clone()));
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(gate.log(), vec!["+P/bid"], "the resting quote reserves its slot");

        // K collapses => the PM quote is unviable => prompt cancel
        intents.clear();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.30", "500")],
                          vec![lvl("0.99", "1")], 2, 131_000_000_000, None);
        q.on_book(&mut cx, &fees, &bb, 131.0, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_some(), "{intents:?}");
        assert_eq!(gate.log(), vec!["+P/bid", "-P/bid"], "and gives it back");
    }

    /// RELEASE ON THE HALT PATHS. `cancel_all` has exactly two production
    /// callers — the kill switch and the feed-stale pull — and both pull quotes
    /// the engine will not re-place until the condition clears. Holding their
    /// capital would leave a killed engine unable to quote once the kill was
    /// lifted. (The shutdown sweep is NOT one of them: it is venue-level and
    /// touches no quoter.)
    #[test]
    fn cancel_all_releases_every_slot_it_pulls() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let gate = Always::new(true);
        q.set_risk(Some(gate.clone()));
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(gate.log(), vec!["+P/bid"]);

        intents.clear();
        q.cancel_all(&mut cx, 200.0, &mut intents);
        assert!(cancel_on(&intents, "P").is_some(), "{intents:?}");
        assert_eq!(gate.log(), vec!["+P/bid", "-P/bid"]);
    }

    /// RELEASE ON REPRICE, and the one release path that is really a rewrite:
    /// an amend names the SAME slot, so the gate replaces that quote's
    /// reservation instead of stacking a second one.
    ///
    /// This is also what makes a place the VENUE REJECTED self-heal. The engine
    /// is never told about a rejection (`exec.rs` counts `exec_failed` and emits
    /// nothing), so the quoter goes on believing it rests — and the reservation
    /// lasts exactly as long as that belief, until the next book event on the
    /// market reprices the slot or cancels it.
    #[test]
    fn a_reprice_names_the_slot_it_already_reserved() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let gate = Always::new(true);
        q.set_risk(Some(gate.clone()));
        let mut oid = 0;
        let mut intents = Vec::new();

        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(place_on(&intents, "P").is_some(), "{intents:?}");

        // past min_requote_s, with a book that wants a different price
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);
        let reprice = place_on(&intents, "P").expect("the reprice");
        assert!(reprice.replaces.is_some(), "{intents:?}");
        assert_eq!(
            gate.log(),
            vec!["+P/bid", "+P/bid"],
            "the same slot twice — never a cancel of the old and a second holding"
        );
    }

    /// RELEASE ON THE STALE-CAP PULL, and this is the release that matters
    /// most.
    ///
    /// `CAP_REFUSAL_PULL_S` (#35) pulls a quote whose price the quoter has
    /// rejected while the caps refuse the replacement — so the quote is coming
    /// off precisely BECAUSE capital is scarce, and its reservation is the
    /// headroom the next quote needs. Leaking it there would be the worst
    /// possible place to leak: the cap ratchets shut exactly when it is already
    /// jammed, and nothing short of a kill or a feed-stale pull reopens it.
    ///
    /// This path did not exist when the reservation was written; it arrived on
    /// main in #35 and is reachable only by reserving first and refusing after.
    #[test]
    fn a_quote_pulled_for_a_stale_cap_refusal_gives_its_capital_back() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let gate = Always::new(true);
        q.set_risk(Some(gate.clone()));
        let mut oid = 0;
        let mut intents = Vec::new();

        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert_eq!(gate.log(), vec!["+P/bid"], "the quote rests holding its clip");

        // the caps fill up under the resting quote, and the book moves enough
        // to want a reprice the gate will not allow
        gate.set(false);
        intents.clear();
        pm_bid(&mut bb, "0.34", 2, 130_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 130.0, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_none(), "the FIRST refusal holds it: {intents:?}");
        assert_eq!(gate.log(), vec!["+P/bid"], "and releases nothing");

        // ...past CAP_REFUSAL_PULL_S the stale quote comes off
        intents.clear();
        pm_bid(&mut bb, "0.34", 3, 200_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 200.0, &mut oid, &mut intents);
        assert!(cancel_on(&intents, "P").is_some(), "expected the pull: {intents:?}");
        assert_eq!(
            gate.log(),
            vec!["+P/bid", "-P/bid"],
            "a quote pulled BECAUSE capital is scarce must hand its capital back"
        );
    }

    /// A REFUSED quote reserves nothing, so a cap that is already full does not
    /// fill itself further every time it says no.
    #[test]
    fn a_refused_quote_reserves_nothing() {
        let (mut cx, fees, mut bb, mut q) = fixture();
        let gate = Always::new(false);
        q.set_risk(Some(gate.clone()));
        let mut oid = 0;
        let mut intents = Vec::new();
        pm_bid(&mut bb, "0.30", 1, 2_000_000_000);
        q.on_book(&mut cx, &fees, &bb, 100.0, &mut oid, &mut intents);
        assert!(gate.log().is_empty(), "{:?}", gate.log());
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


    /// The order `cancel_all` emits is ASK before BID, because it has always
    /// sorted by the wire spelling and `"ask" < "bid"`.
    ///
    /// This is here because nothing else can catch it. `cancel_all` runs on the
    /// kill switch and the feed-stale pull, and the bench replay that produces
    /// the golden digest fires neither — so a reversal is invisible to the one
    /// gate the campaign trusts. It was invisible to all 402 tests too: adding
    /// `#[derive(Ord)]` to `BookSide` and letting `keys.sort()` use it reverses
    /// this order (declaration order puts `Bid` first) with the whole suite
    /// green. The missing derive and the comment above `sort_by` are the guard;
    /// this is what makes the guard fail loudly instead of silently.
    #[test]
    fn cancel_all_emits_the_ask_before_the_bid() {
        let (mut cx, _fees, _bb, mut q) = tests_support::fixture();
        let px = cx.parse_exact("0.42");
        for side in [BookSide::Bid, BookSide::Ask] {
            q.resting.insert(
                (0, side),
                RestingQuote { order_id: format!("m-{}", side.as_str()), price: px, count: 1 },
            );
        }
        let mut intents = Vec::new();
        q.cancel_all(&mut cx, 1.0, &mut intents);
        let sides: Vec<&str> = intents
            .iter()
            .filter_map(|i| match i {
                Intent::Cancel(c) => Some(c.side.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(sides, vec!["ask", "bid"], "cancel_all must emit ask before bid");
    }
}
